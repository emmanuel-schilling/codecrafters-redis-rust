use crate::app::{
    command::{Command, CommandHandler, ParsedCommand},
    protocol::{RespDecoder, RespValue},
    pubsub::PubSubHub,
    replication::ReplicationState,
    store::Store,
    wait::WaiterRegistry,
};
use crate::config::Config;
use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use std::collections::{HashMap, HashSet};
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

static NEXT_CLIENT_ID: AtomicU64 = AtomicU64::new(1);

pub type CommandRegistry = HashMap<Command, Arc<dyn CommandHandler>>;

pub struct AppContext {
    pub store: Arc<Store>,
    pub config: Arc<Config>,
    pub replication: Arc<ReplicationState>,
    pub pubsub: Arc<PubSubHub>,
    pub waiters: Arc<WaiterRegistry>,
    pub registry: CommandRegistry,
    /// Held while a write runs and is sent to replicas, so replicas receive
    /// writes in the order this server applied them.
    pub write_order: tokio::sync::Mutex<()>,
}

impl AppContext {
    pub fn new(
        store: Arc<Store>,
        config: Arc<Config>,
        replication: Arc<ReplicationState>,
        pubsub: Arc<PubSubHub>,
        waiters: Arc<WaiterRegistry>,
    ) -> Self {
        let mut registry: CommandRegistry = HashMap::new();

        let connection_handler = Arc::new(handlers::ConnectionHandler);
        registry.insert(Command::Ping, connection_handler.clone());
        registry.insert(Command::Echo, connection_handler);

        let string_handler = Arc::new(handlers::StringHandler);
        registry.insert(Command::Set, string_handler.clone());
        registry.insert(Command::Get, string_handler.clone());
        registry.insert(Command::Incr, string_handler);

        let server_handler = Arc::new(handlers::ServerHandler);
        registry.insert(Command::Config, server_handler.clone());
        registry.insert(Command::Keys, server_handler.clone());
        registry.insert(Command::Type, server_handler);

        let replication_handler = Arc::new(handlers::ReplicationHandler);
        registry.insert(Command::Info, replication_handler.clone());
        registry.insert(Command::ReplConf, replication_handler.clone());
        registry.insert(Command::Wait, replication_handler);

        let list_handler = Arc::new(handlers::ListHandler);
        registry.insert(Command::LPush, list_handler.clone());
        registry.insert(Command::RPush, list_handler.clone());
        registry.insert(Command::LPop, list_handler.clone());
        registry.insert(Command::LLen, list_handler.clone());
        registry.insert(Command::LRange, list_handler.clone());
        registry.insert(Command::BLPop, list_handler);

        let stream_handler = Arc::new(handlers::StreamHandler);
        registry.insert(Command::XAdd, stream_handler.clone());
        registry.insert(Command::XRange, stream_handler.clone());
        registry.insert(Command::XRead, stream_handler);

        let sorted_set_handler = Arc::new(handlers::SortedSetHandler);
        registry.insert(Command::ZAdd, sorted_set_handler.clone());
        registry.insert(Command::ZCard, sorted_set_handler.clone());
        registry.insert(Command::ZScore, sorted_set_handler.clone());
        registry.insert(Command::ZRank, sorted_set_handler.clone());
        registry.insert(Command::ZRange, sorted_set_handler.clone());
        registry.insert(Command::ZRem, sorted_set_handler);

        let geo_handler = Arc::new(handlers::GeoHandler);
        registry.insert(Command::GeoAdd, geo_handler.clone());
        registry.insert(Command::GeoPos, geo_handler.clone());
        registry.insert(Command::GeoDist, geo_handler.clone());
        registry.insert(Command::GeoSearch, geo_handler);

        let pubsub_handler = Arc::new(handlers::PubSubHandler);
        registry.insert(Command::Publish, pubsub_handler);

        Self {
            store,
            config,
            replication,
            pubsub,
            waiters,
            registry,
            write_order: tokio::sync::Mutex::new(()),
        }
    }
}

pub enum ConnectionState {
    Normal,
    InTransaction,
    Subscribed {
        client_id: u64,
        subscriptions: HashSet<Bytes>,
        tx: mpsc::Sender<RespValue>,
        rx: mpsc::Receiver<RespValue>,
    },
}

pub async fn handle_connection(stream: TcpStream, ctx: Arc<AppContext>) {
    let client_id = NEXT_CLIENT_ID.fetch_add(1, Ordering::Relaxed);
    let mut framed = Framed::new(stream, RespDecoder);
    let mut state = ConnectionState::Normal;
    let mut command_queue: Vec<ParsedCommand> = Vec::new();

    loop {
        if let ConnectionState::Subscribed {
            client_id,
            ref mut subscriptions,
            ref tx,
            ref mut rx,
        } = state
        {
            handlers::pubsub::subscribed_loop(
                &mut framed,
                subscriptions,
                tx,
                rx,
                &ctx.pubsub,
                client_id,
            )
            .await;
            info!("Client {client_id} disconnected from subscribed mode.");
            ctx.pubsub.unsubscribe_all(client_id, subscriptions);
            return;
        }

        match framed.next().await {
            Some(Ok(value)) => {
                let parsed_command = match ParsedCommand::from_resp(value) {
                    Ok(cmd) => cmd,
                    Err(e) => {
                        if let Err(e) = framed
                            .send(RespValue::Error(Bytes::from(e.to_string())))
                            .await
                        {
                            error!("failed to send error response: {e}");
                        }
                        continue;
                    }
                };

                if parsed_command.command() == Command::Subscribe {
                    let (tx, rx) = mpsc::channel(128);
                    let (response, new_subs) = handlers::pubsub::subscribe(
                        parsed_command,
                        &ctx.pubsub,
                        client_id,
                        tx.clone(),
                        &HashSet::new(),
                    );
                    if let Err(e) = framed.send(response).await {
                        error!("failed to send subscribe response: {e}");
                        return;
                    }
                    state = ConnectionState::Subscribed {
                        client_id,
                        subscriptions: new_subs,
                        tx,
                        rx,
                    };
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

                let response =
                    handle_command(parsed_command, &ctx, &mut state, &mut command_queue).await;

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

/// Runs a command received from the master on a replica.
pub async fn apply_command(ctx: &Arc<AppContext>, parsed: ParsedCommand) -> RespValue {
    match ctx.registry.get(&parsed.command()) {
        Some(handler) => handler.handle(ctx, parsed).await,
        None => unknown_command(&parsed),
    }
}

pub async fn handle_command(
    parsed: ParsedCommand,
    ctx: &Arc<AppContext>,
    state: &mut ConnectionState,
    queue: &mut Vec<ParsedCommand>,
) -> RespValue {
    if let Some(response) =
        handlers::transaction::handle_state(parsed.clone(), state, queue, ctx).await
    {
        return response;
    }

    if matches!(state, ConnectionState::InTransaction) {
        queue.push(parsed);
        return RespValue::SimpleString(Bytes::from_static(b"QUEUED"));
    }

    let command = parsed.command();
    if command.is_write() && ctx.replication.role() == replication::Role::Replica {
        return RespValue::Error(Bytes::from_static(
            b"READONLY You can't write against a read only replica.",
        ));
    }

    let Some(handler) = ctx.registry.get(&command) else {
        return unknown_command(&parsed);
    };

    // BLPOP may block, so it can't hold the write-order lock while it waits;
    // it takes the lock around each pop and propagates the pop itself.
    if !command.is_write() || command == Command::BLPop {
        return handler.handle(ctx, parsed).await;
    }

    let _order = ctx.write_order.lock().await;
    let response = handler.handle(ctx, parsed.clone()).await;
    if let Some(propagated) = propagated_form(parsed, &response) {
        ctx.replication.propagate(propagated);
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

fn unknown_command(parsed: &ParsedCommand) -> RespValue {
    RespValue::Error(Bytes::from(format!(
        "ERR unknown command '{}'",
        parsed.command()
    )))
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

    fn command(parts: &[&str]) -> ParsedCommand {
        ParsedCommand::from_resp(RespValue::Array(
            parts
                .iter()
                .map(|p| RespValue::BulkString(Bytes::copy_from_slice(p.as_bytes())))
                .collect(),
        ))
        .expect("valid command")
    }

    async fn run(parts: &[&str], ctx: &Arc<AppContext>) -> RespValue {
        handle_command(
            command(parts),
            ctx,
            &mut ConnectionState::Normal,
            &mut vec![],
        )
        .await
    }

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
}
