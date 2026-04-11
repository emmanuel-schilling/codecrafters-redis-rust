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
use tokio::sync::{mpsc, Notify};
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
    let wait_notify = Arc::new(Notify::new());

    loop {
        if let ConnectionState::Subscribed {
            client_id,
            ref mut subscriptions,
            ref tx,
            ref mut rx,
        } = state
        {
            if handlers::pubsub::subscribed_loop(
                &mut framed,
                subscriptions,
                tx,
                rx,
                &ctx.pubsub,
                client_id,
            )
            .await
            .is_err()
            {
                info!("Client {client_id} disconnected from subscribed mode.");
                ctx.pubsub.unsubscribe_all(client_id, subscriptions);
                return;
            }
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

                    let (tx, rx) = mpsc::channel(128);
                    let ack_offset = ctx.replication.add_replica(tx).await;
                    tokio::spawn(replication::serve_replica(
                        rx,
                        framed,
                        ack_offset,
                        Arc::clone(&wait_notify),
                    ));
                    info!("Connection transitioned to replica serving mode, breaking main loop.");
                    return;
                }

                let response = handle_command(
                    parsed_command,
                    &ctx,
                    &mut state,
                    &mut command_queue,
                    &wait_notify,
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

pub async fn apply_command(ctx: &Arc<AppContext>, parsed: ParsedCommand) -> RespValue {
    let command = parsed.command();
    let wait_notify = Arc::new(Notify::new()); // Dummy notify for non-connection-based apply
    if let Some(handler) = ctx.registry.get(&command) {
        handler.handle(ctx, parsed, &wait_notify).await
    } else {
        RespValue::Error(Bytes::from(format!(
            "ERR unknown command '{}'",
            parsed.command()
        )))
    }
}

pub async fn handle_command(
    parsed: ParsedCommand,
    ctx: &Arc<AppContext>,
    state: &mut ConnectionState,
    queue: &mut Vec<ParsedCommand>,
    wait_notify: &Arc<Notify>,
) -> RespValue {
    if let Some(response) =
        handlers::transaction::handle_state(parsed.clone(), state, queue, ctx, wait_notify).await
    {
        return response;
    }

    if matches!(state, ConnectionState::InTransaction) {
        queue.push(parsed);
        return RespValue::SimpleString(Bytes::from_static(b"QUEUED"));
    }

    let command = parsed.command();

    let is_write_command = matches!(
        command,
        Command::Set
            | Command::LPush
            | Command::RPush
            | Command::LPop
            | Command::Incr
            | Command::XAdd
            | Command::ZAdd
            | Command::ZRem
            | Command::GeoAdd
    );
    if is_write_command {
        if ctx.replication.role() == replication::Role::Replica {
            return RespValue::Error(Bytes::from_static(
                b"READONLY You can't write against a read only replica.",
            ));
        }
        Arc::clone(&ctx.replication).propagate(parsed.clone());
    }

    if let Some(handler) = ctx.registry.get(&command) {
        handler.handle(ctx, parsed, wait_notify).await
    } else {
        RespValue::Error(Bytes::from(format!(
            "ERR unknown command '{}'",
            parsed.command()
        )))
    }
}
