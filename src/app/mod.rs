use crate::app::{
    command::{Command, ParsedCommand},
    protocol::{RespDecoder, RespValue},
    pubsub::PubSubHub,
    replication::ReplicationState,
    store::Store,
    wait::WaiterRegistry,
    watch::WatchRegistry,
};
use crate::config::Config;
use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio_util::codec::Framed;
use tracing::{error, info};

pub mod command;
pub mod error;
pub mod geo;
pub mod handlers;
pub mod protocol;
pub mod pubsub;
pub mod rdb;
pub mod replication;
pub mod sorted_set;
pub mod store;
pub mod stream;
pub mod wait;
pub mod watch;

static NEXT_CLIENT_ID: AtomicU64 = AtomicU64::new(1);

pub struct AppContext {
    pub store: Arc<Store>,
    pub config: Arc<Config>,
    pub replication: Arc<ReplicationState>,
    pub pubsub: Arc<PubSubHub>,
    pub waiters: Arc<WaiterRegistry>,
    /// Held while a write runs and is sent to replicas, so replicas receive
    /// writes in the order this server applied them.
    pub write_order: tokio::sync::Mutex<()>,
    pub watches: WatchRegistry,
}

impl AppContext {
    pub fn new(
        store: Arc<Store>,
        config: Arc<Config>,
        replication: Arc<ReplicationState>,
        pubsub: Arc<PubSubHub>,
        waiters: Arc<WaiterRegistry>,
    ) -> Self {
        Self {
            store,
            config,
            replication,
            pubsub,
            waiters,
            write_order: tokio::sync::Mutex::new(()),
            watches: WatchRegistry::new(),
        }
    }
}

pub enum ConnectionState {
    Normal,
    InTransaction,
    Subscribed {
        client_id: u64,
        subscriptions: HashSet<Bytes>,
        rx: mpsc::Receiver<RespValue>,
    },
}

pub async fn handle_connection(stream: TcpStream, ctx: Arc<AppContext>) {
    let client_id = NEXT_CLIENT_ID.fetch_add(1, Ordering::Relaxed);
    serve_client(stream, client_id, &ctx).await;
    ctx.watches.unwatch(client_id);
}

/// Serves a client until it disconnects or becomes a replica.
async fn serve_client(stream: TcpStream, client_id: u64, ctx: &Arc<AppContext>) {
    let mut framed = Framed::new(stream, RespDecoder);
    let mut state = ConnectionState::Normal;
    let mut command_queue: Vec<ParsedCommand> = Vec::new();

    loop {
        if let ConnectionState::Subscribed {
            client_id,
            ref mut subscriptions,
            ref mut rx,
        } = state
        {
            let exit = handlers::pubsub::subscribed_loop(
                &mut framed,
                subscriptions,
                rx,
                &ctx.pubsub,
                client_id,
            )
            .await;
            ctx.pubsub.remove_client(client_id);
            if exit == handlers::pubsub::SubscribedExit::Disconnected {
                info!("Client {client_id} disconnected from subscribed mode.");
                return;
            }
            state = ConnectionState::Normal;
            continue;
        }

        match framed.next().await {
            Some(Ok(value)) => {
                let parsed_command = match ParsedCommand::from_resp(value) {
                    Ok(cmd) => cmd,
                    Err(e) => {
                        if let Err(e) = framed.send(e.into()).await {
                            error!("failed to send error response: {e}");
                        }
                        continue;
                    }
                };

                if parsed_command.command() == Command::Subscribe {
                    let rx = ctx.pubsub.register(client_id);
                    let mut subscriptions = HashSet::new();
                    let replies = handlers::pubsub::subscribe(
                        &parsed_command,
                        &ctx.pubsub,
                        client_id,
                        &mut subscriptions,
                    );
                    if let Err(e) = handlers::pubsub::send_all(&mut framed, replies).await {
                        error!("failed to send subscribe response: {e}");
                        ctx.pubsub.remove_client(client_id);
                        return;
                    }
                    if subscriptions.is_empty() {
                        ctx.pubsub.remove_client(client_id);
                    } else {
                        state = ConnectionState::Subscribed {
                            client_id,
                            subscriptions,
                            rx,
                        };
                    }
                    continue;
                }

                if parsed_command.command() == Command::Unsubscribe {
                    // Not subscribed to anything; Redis still replies per channel.
                    let replies = handlers::pubsub::unsubscribe(
                        &parsed_command,
                        &ctx.pubsub,
                        client_id,
                        &mut HashSet::new(),
                    );
                    if let Err(e) = handlers::pubsub::send_all(&mut framed, replies).await {
                        error!("failed to send unsubscribe response: {e}");
                        return;
                    }
                    continue;
                }

                if parsed_command.command() == Command::PSync {
                    info!("Replica requested PSYNC, starting full resync.");
                    let response = RespValue::SimpleString(Bytes::from(format!(
                        "FULLRESYNC {} 0",
                        ctx.replication.master_replid()
                    )));
                    if let Err(e) = framed.send(response).await {
                        error!("Failed to send FULLRESYNC response: {e}");
                        return;
                    }
                    let rdb_bytes = rdb::empty_rdb_bytes();
                    if let Err(e) = framed.get_mut().write_all(&rdb_bytes).await {
                        error!("Failed to send RDB file: {e}");
                        return;
                    }

                    let (tx, rx) = mpsc::unbounded_channel();
                    let ack_offset = ctx.replication.add_replica(tx);
                    tokio::spawn(replication::serve_replica(
                        rx,
                        framed,
                        ack_offset,
                        Arc::clone(&ctx.replication),
                    ));
                    info!("Connection transitioned to replica serving mode, breaking main loop.");
                    return;
                }

                let response = handle_command(
                    parsed_command,
                    ctx,
                    client_id,
                    &mut state,
                    &mut command_queue,
                )
                .await;

                if let Err(e) = framed.send(response).await {
                    error!("failed to send response: {e}");
                    return;
                }
            }
            Some(Err(e)) => {
                error!("error reading from stream: {e}");
                return;
            }
            None => return,
        }
    }
}

/// Runs a write received from the master on a replica.
pub async fn apply_command(ctx: &Arc<AppContext>, parsed: ParsedCommand) -> RespValue {
    let _order = ctx.write_order.lock().await;
    let written_key = parsed.written_key().cloned();
    let response = handlers::dispatch(ctx, parsed).await;
    if let (Some(key), false) = (written_key, matches!(response, RespValue::Error(_))) {
        ctx.watches.touch(&key);
    }
    response
}

/// The reply to a write sent by a client to a replica.
pub(crate) fn read_only_error() -> RespValue {
    RespValue::Error(Bytes::from_static(
        b"READONLY You can't write against a read only replica.",
    ))
}

pub async fn handle_command(
    parsed: ParsedCommand,
    ctx: &Arc<AppContext>,
    client_id: u64,
    state: &mut ConnectionState,
    queue: &mut Vec<ParsedCommand>,
) -> RespValue {
    if let Some(response) =
        handlers::transaction::handle_state(&parsed, client_id, state, queue, ctx).await
    {
        return response;
    }

    if matches!(state, ConnectionState::InTransaction) {
        queue.push(parsed);
        return RespValue::SimpleString(Bytes::from_static(b"QUEUED"));
    }

    let command = parsed.command();
    if command.is_write() && ctx.replication.role() == replication::Role::Replica {
        return read_only_error();
    }

    // BLPOP may block, so it can't hold the write-order lock while it waits;
    // it takes the lock around each pop and propagates the pop itself.
    if !command.is_write() || command == Command::BLPop {
        return handlers::dispatch(ctx, parsed).await;
    }

    let _order = ctx.write_order.lock().await;
    apply_write(ctx, parsed).await
}

/// Runs a write and, if it succeeds, marks its key changed for WATCH, sends
/// it to replicas and serves clients blocked on a list it pushed to. The
/// caller holds the write-order lock.
pub(crate) async fn apply_write(ctx: &Arc<AppContext>, parsed: ParsedCommand) -> RespValue {
    let fed_list = matches!(parsed.command(), Command::LPush | Command::RPush)
        .then(|| parsed.first().cloned())
        .flatten();
    let written_key = parsed.written_key().cloned();
    let response = handlers::dispatch(ctx, parsed.clone()).await;
    if let Some(propagated) = propagated_form(parsed, &response) {
        if let Some(key) = written_key {
            ctx.watches.touch(&key);
        }
        ctx.replication.propagate(propagated);
        if let Some(key) = fed_list {
            handlers::list::serve_blocked_clients(ctx, &key);
        }
    }
    response
}

/// The command replicas apply for a write that replied `response`, or `None`
/// if the write failed and changed nothing.
fn propagated_form(parsed: ParsedCommand, response: &RespValue) -> Option<RespValue> {
    match response {
        RespValue::Error(_) => None,
        // An ID generated here (`*` or `<ms>-*`) must be the same on replicas.
        RespValue::BulkString(id) if parsed.command() == Command::XAdd => {
            Some(parsed.with_arg(1, id.clone()).into_resp_array())
        }
        _ => Some(parsed.into_resp_array()),
    }
}

/// Builds a command from its parts, for unit tests.
#[cfg(test)]
pub(crate) fn test_command(parts: &[&str]) -> ParsedCommand {
    ParsedCommand::from_resp(RespValue::Array(
        parts
            .iter()
            .map(|p| RespValue::BulkString(Bytes::copy_from_slice(p.as_bytes())))
            .collect(),
    ))
    .expect("valid command")
}

/// Runs a command as a client connection would, for unit tests.
#[cfg(test)]
pub(crate) async fn run_test(parts: &[&str], ctx: &Arc<AppContext>) -> RespValue {
    handle_command(
        test_command(parts),
        ctx,
        0,
        &mut ConnectionState::Normal,
        &mut vec![],
    )
    .await
}

/// A master's context with default config, for unit tests.
#[cfg(test)]
pub(crate) fn test_context() -> Arc<AppContext> {
    use clap::Parser;
    let config = Arc::new(Config::parse_from(["redis"]));
    Arc::new(AppContext::new(
        Arc::new(Store::new()),
        Arc::clone(&config),
        Arc::new(ReplicationState::new_from_config(&config)),
        Arc::new(PubSubHub::new()),
        Arc::new(WaiterRegistry::new()),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::{run_test as run, test_command as command};

    /// A master context with one connected replica, whose channel is returned.
    fn master_with_replica() -> (Arc<AppContext>, mpsc::UnboundedReceiver<RespValue>) {
        let ctx = test_context();
        let (tx, rx) = mpsc::unbounded_channel();
        ctx.replication.add_replica(tx);
        (ctx, rx)
    }

    fn received(rx: &mut mpsc::UnboundedReceiver<RespValue>) -> Vec<RespValue> {
        std::iter::from_fn(|| rx.try_recv().ok()).collect()
    }

    #[tokio::test]
    async fn sends_only_successful_writes_in_their_applied_form() {
        let (ctx, mut rx) = master_with_replica();
        run(&["SET", "a", "1"], &ctx).await;
        run(&["GET", "a"], &ctx).await;
        run(&["LPUSH", "a", "x"], &ctx).await; // WRONGTYPE: not sent
        let RespValue::BulkString(id) = run(&["XADD", "s", "*", "f", "v"], &ctx).await else {
            panic!("XADD failed");
        };
        run(&["RPUSH", "q", "x"], &ctx).await;
        run(&["BLPOP", "q", "0"], &ctx).await;

        let id = String::from_utf8(id.to_vec()).expect("utf8 id");
        let expected: Vec<RespValue> = [
            command(&["SET", "a", "1"]),
            command(&["XADD", "s", &id, "f", "v"]),
            command(&["RPUSH", "q", "x"]),
            command(&["LPOP", "q"]),
        ]
        .into_iter()
        .map(ParsedCommand::into_resp_array)
        .collect();
        let sent = received(&mut rx);
        assert_eq!(sent, expected);

        let sent_bytes: usize = sent.iter().map(|c| c.encode_to_bytes().len()).sum();
        assert_eq!(ctx.replication.master_repl_offset(), sent_bytes);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_writes_reach_replicas_in_applied_order() {
        let (ctx, mut rx) = master_with_replica();
        let tasks: Vec<_> = (0..50)
            .map(|i| {
                let ctx = Arc::clone(&ctx);
                tokio::spawn(async move { run(&["SET", "k", &i.to_string()], &ctx).await })
            })
            .collect();
        for task in tasks {
            task.await.expect("task");
        }

        let sent = received(&mut rx);
        assert_eq!(sent.len(), 50);
        let RespValue::BulkString(final_value) = run(&["GET", "k"], &ctx).await else {
            panic!("GET failed");
        };
        let last_sent = sent.last().cloned().expect("a write was sent");
        assert_eq!(
            last_sent,
            command(&["SET", "k", &String::from_utf8_lossy(&final_value)]).into_resp_array()
        );
    }

    #[tokio::test]
    async fn replicas_reject_writes_from_clients() {
        let config = Arc::new(<Config as clap::Parser>::parse_from([
            "redis",
            "--replicaof",
            "localhost 6379",
        ]));
        let ctx = Arc::new(AppContext::new(
            Arc::new(Store::new()),
            Arc::clone(&config),
            Arc::new(ReplicationState::new_from_config(&config)),
            Arc::new(PubSubHub::new()),
            Arc::new(WaiterRegistry::new()),
        ));
        assert!(matches!(
            run(&["BLPOP", "q", "0"], &ctx).await,
            RespValue::Error(_)
        ));
        assert!(matches!(
            run(&["SET", "a", "1"], &ctx).await,
            RespValue::Error(_)
        ));
        assert_eq!(run(&["GET", "a"], &ctx).await, RespValue::NullBulkString);
    }

    #[tokio::test]
    async fn a_push_that_serves_a_blocked_client_sends_the_push_then_the_pop() {
        let (ctx, mut rx) = master_with_replica();
        let mut blocked = Box::pin(run(&["BLPOP", "q", "0"], &ctx));
        assert!(futures::poll!(&mut blocked).is_pending());

        run(&["RPUSH", "q", "a"], &ctx).await;
        assert_eq!(
            blocked.await,
            RespValue::Array(vec![
                RespValue::BulkString(Bytes::from_static(b"q")),
                RespValue::BulkString(Bytes::from_static(b"a")),
            ])
        );
        assert_eq!(
            received(&mut rx),
            vec![
                command(&["RPUSH", "q", "a"]).into_resp_array(),
                command(&["LPOP", "q"]).into_resp_array(),
            ]
        );
        assert_eq!(
            run(&["TYPE", "q"], &ctx).await,
            RespValue::SimpleString(Bytes::from_static(b"none"))
        );
    }

    #[tokio::test]
    async fn commands_handled_before_dispatch_are_unknown_when_queued() {
        let ctx = test_context();
        let mut state = ConnectionState::Normal;
        let mut queue = vec![];
        for parts in [&["MULTI"][..], &["SUBSCRIBE", "a"], &["PSYNC", "?", "-1"]] {
            handle_command(command(parts), &ctx, 1, &mut state, &mut queue).await;
        }
        assert_eq!(
            handle_command(command(&["EXEC"]), &ctx, 1, &mut state, &mut queue).await,
            RespValue::Array(vec![
                RespValue::Error(Bytes::from_static(b"ERR unknown command 'SUBSCRIBE'")),
                RespValue::Error(Bytes::from_static(b"ERR unknown command 'PSYNC'")),
            ])
        );
    }
}
