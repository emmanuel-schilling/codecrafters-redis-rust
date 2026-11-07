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
use tracing::{error, info, warn};

pub mod aof;
pub mod command;
pub mod error;
pub mod exec;
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
            watches: WatchRegistry::new(),
        }
    }
}

pub enum ConnectionState {
    Normal,
    /// Queuing a transaction's commands. `aborted` is set when a command is
    /// rejected while queuing, which makes EXEC fail, as in Redis.
    InTransaction {
        aborted: bool,
    },
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
                        handlers::transaction::reject_while_queuing(&mut state);
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

/// Applies writes received from the master on a replica, together: a
/// transaction's writes arrive wrapped in MULTI and EXEC, and no client sees
/// the keyspace between them.
pub fn apply_from_master(ctx: &AppContext, commands: &[ParsedCommand]) {
    exec::locked_for_master(ctx, |ks, outbox| {
        for parsed in commands {
            let reply = match parsed.check_arity() {
                Ok(()) => exec::run(ctx, ks, parsed, outbox),
                Err(e) => e.into(),
            };
            if let RespValue::Error(e) = reply {
                warn!(
                    "Replica failed to apply write command {:?}: {}",
                    parsed.command(),
                    String::from_utf8_lossy(&e)
                );
            }
        }
    });
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
    if let Err(e) = parsed.check_arity() {
        handlers::transaction::reject_while_queuing(state);
        return e.into();
    }

    if let Some(response) =
        handlers::transaction::handle_state(&parsed, client_id, state, queue, ctx)
    {
        return response;
    }

    if matches!(state, ConnectionState::InTransaction { .. }) {
        queue.push(parsed);
        return RespValue::SimpleString(Bytes::from_static(b"QUEUED"));
    }

    if parsed.command().is_write() && ctx.replication.role() == replication::Role::Replica {
        return read_only_error();
    }

    // These can wait, so they lock the keyspace only while they look at it.
    match parsed.command() {
        Command::BLPop => handlers::list::blpop(&parsed, ctx).await,
        Command::XRead => handlers::stream::xread(&parsed, ctx).await,
        Command::Wait => handlers::replication::wait(&parsed, &ctx.replication)
            .await
            .unwrap_or_else(RespValue::from),
        _ => exec::locked(ctx, |ks, outbox| exec::run(ctx, ks, &parsed, outbox)),
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

    /// Runs `parts` as one client's transaction and returns EXEC's reply.
    async fn transaction(ctx: &Arc<AppContext>, client_id: u64, parts: &[&[&str]]) -> RespValue {
        let (mut state, mut queue) = (ConnectionState::Normal, vec![]);
        handle_command(command(&["MULTI"]), ctx, client_id, &mut state, &mut queue).await;
        for p in parts {
            handle_command(command(p), ctx, client_id, &mut state, &mut queue).await;
        }
        handle_command(command(&["EXEC"]), ctx, client_id, &mut state, &mut queue).await
    }

    #[tokio::test]
    async fn writes_that_change_nothing_are_not_sent_and_do_not_fail_watchers() {
        let (ctx, mut rx) = master_with_replica();
        run(&["ZADD", "z", "1", "a"], &ctx).await;
        run(&["SETBIT", "b", "3", "1"], &ctx).await;
        received(&mut rx);

        let (mut state, mut queue) = (ConnectionState::Normal, vec![]);
        handle_command(
            command(&["WATCH", "z", "b", "l", "d"]),
            &ctx,
            1,
            &mut state,
            &mut queue,
        )
        .await;
        for parts in [
            &["LPOP", "l"][..],
            &["LPOP", "l", "2"],
            &["ZREM", "z", "missing"],
            &["ZADD", "z", "1", "a"],
            &["SETBIT", "b", "3", "1"],
            &["SETBIT", "b", "0", "0"],
            &["BITOP", "AND", "d", "missing"],
            &["BLPOP", "l", "0.001"],
        ] {
            run(parts, &ctx).await;
        }
        assert_eq!(received(&mut rx), vec![]);
        assert_eq!(transaction(&ctx, 1, &[]).await, RespValue::Array(vec![]));
    }

    #[tokio::test]
    async fn a_transaction_reaches_replicas_wrapped_in_multi_and_exec() {
        let (ctx, mut rx) = master_with_replica();
        transaction(
            &ctx,
            1,
            &[&["SET", "a", "1"], &["GET", "a"], &["INCR", "a"]],
        )
        .await;
        // A single write needs no wrapping.
        transaction(&ctx, 1, &[&["SET", "b", "1"], &["GET", "b"]]).await;
        let expected: Vec<RespValue> = [
            &["MULTI"][..],
            &["SET", "a", "1"],
            &["INCR", "a"],
            &["EXEC"],
            &["SET", "b", "1"],
        ]
        .into_iter()
        .map(|parts| command(parts).into_resp_array())
        .collect();
        assert_eq!(received(&mut rx), expected);
    }

    #[tokio::test]
    async fn wait_in_a_transaction_replies_at_once() {
        let (ctx, _rx) = master_with_replica();
        run(&["SET", "a", "1"], &ctx).await;
        assert_eq!(
            transaction(&ctx, 1, &[&["WAIT", "1", "0"]]).await,
            RespValue::Array(vec![RespValue::Integer(0)])
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn no_command_sees_a_transaction_half_done() {
        let ctx = test_context();
        // Each transaction creates both keys or deletes both.
        let writer = {
            let ctx = Arc::clone(&ctx);
            tokio::spawn(async move {
                for _ in 0..500 {
                    transaction(&ctx, 1, &[&["RPUSH", "a", "x"], &["RPUSH", "b", "x"]]).await;
                    transaction(&ctx, 1, &[&["LPOP", "a"], &["LPOP", "b"]]).await;
                }
            })
        };
        while !writer.is_finished() {
            let RespValue::Array(keys) = run(&["KEYS", "*"], &ctx).await else {
                panic!("KEYS failed");
            };
            assert!(keys.len() != 1, "saw {keys:?}");
        }
        writer.await.expect("writer");
    }

    /// A Unix time in milliseconds, `offset_ms` from now, as a SET PXAT argument.
    fn unix_ms(offset_ms: i64) -> String {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_millis() as i64;
        (now + offset_ms).to_string()
    }

    #[tokio::test]
    async fn a_master_sends_del_for_a_key_it_finds_expired() {
        let (ctx, mut rx) = master_with_replica();
        let past = unix_ms(-1000);
        run(&["SET", "a", "1", "PXAT", &past], &ctx).await;
        run(&["SET", "b", "1", "PXAT", &past], &ctx).await;
        received(&mut rx);

        assert_eq!(run(&["GET", "a"], &ctx).await, RespValue::NullBulkString);
        assert_eq!(run(&["INCR", "b"], &ctx).await, RespValue::Integer(1));
        assert_eq!(
            received(&mut rx),
            [&["DEL", "a"][..], &["DEL", "b"], &["INCR", "b"]]
                .into_iter()
                .map(|parts| command(parts).into_resp_array())
                .collect::<Vec<_>>()
        );
    }

    #[tokio::test]
    async fn del_deletes_the_keys_that_exist() {
        let (ctx, mut rx) = master_with_replica();
        run(&["SET", "a", "1"], &ctx).await;
        run(&["RPUSH", "b", "x"], &ctx).await;
        received(&mut rx);
        assert_eq!(
            run(&["DEL", "a", "b", "c"], &ctx).await,
            RespValue::Integer(2)
        );
        assert_eq!(run(&["DEL", "a"], &ctx).await, RespValue::Integer(0));
        assert_eq!(
            received(&mut rx),
            vec![command(&["DEL", "a", "b", "c"]).into_resp_array()]
        );
        assert_eq!(run(&["KEYS", "*"], &ctx).await, RespValue::Array(vec![]));
    }

    #[tokio::test]
    async fn a_replica_keeps_expired_keys_until_its_master_deletes_them() {
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
        apply_from_master(
            &ctx,
            &[command(&["SET", "n", "41", "PXAT", &unix_ms(-1000)])],
        );

        // Its clients don't see the key.
        assert_eq!(run(&["GET", "n"], &ctx).await, RespValue::NullBulkString);
        assert_eq!(run(&["KEYS", "*"], &ctx).await, RespValue::Array(vec![]));
        // The master's writes still do, until it sends DEL.
        apply_from_master(&ctx, &[command(&["INCR", "n"])]);
        let stored = |ctx: &AppContext| {
            ctx.store.with_keyspace(|ks| {
                ks.set_expiry(store::Expiry::Ignore);
                ks.get(&Bytes::from_static(b"n"))
                    .and_then(|data| data.as_string().cloned())
            })
        };
        assert_eq!(stored(&ctx), Some(Bytes::from_static(b"42")));
        apply_from_master(&ctx, &[command(&["DEL", "n"])]);
        assert_eq!(stored(&ctx), None);
    }
}
