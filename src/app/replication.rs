use crate::app::{
    command::{Command, ParsedCommand},
    protocol::{RespDecoder, RespValue},
    AppContext,
};
use anyhow::Result;
use bytes::{Buf, Bytes};
use futures::{SinkExt, StreamExt};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use tokio::io::AsyncReadExt;
use tokio::net::TcpStream;
use tokio::sync::{mpsc, Mutex, Notify};
use tokio_util::codec::{Framed, FramedParts};
use tracing::{debug, error, info, warn};

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum Role {
    Master,
    Replica,
}

type ReplicaSender = mpsc::Sender<RespValue>;

#[derive(Debug)]
struct ReplicaInfo {
    sender: ReplicaSender,
    acknowledged_offset: Arc<AtomicUsize>,
}

#[derive(Debug)]
pub struct ReplicationState {
    role: Role,
    master_replid: String,
    master_repl_offset: AtomicUsize,
    replicas: Mutex<Vec<ReplicaInfo>>,
    pub(crate) wait_waiters: crate::app::wait::WaiterRegistry, // Using our BLPOP waiter for WAIT as well
}

impl ReplicationState {
    pub fn new_from_config(config: &crate::config::Config) -> Self {
        let role = if config.replicaof.is_some() {
            Role::Replica
        } else {
            Role::Master
        };

        Self {
            role,
            master_replid: "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb".to_string(),
            master_repl_offset: AtomicUsize::new(0),
            replicas: Mutex::new(Vec::new()),
            wait_waiters: crate::app::wait::WaiterRegistry::new(),
        }
    }

    pub fn role(&self) -> Role {
        self.role
    }
    pub fn master_replid(&self) -> &str {
        &self.master_replid
    }
    pub fn master_repl_offset(&self) -> usize {
        self.master_repl_offset.load(Ordering::Relaxed)
    }

    pub async fn replica_count(&self) -> usize {
        self.replicas.lock().await.len()
    }

    pub fn info_string(&self) -> String {
        format!(
            "role:{}\r\nmaster_replid:{}\r\nmaster_repl_offset:{}",
            if self.role == Role::Master {
                "master"
            } else {
                "slave"
            },
            self.master_replid,
            self.master_repl_offset()
        )
    }

    pub async fn add_replica(&self, tx: ReplicaSender) -> Arc<AtomicUsize> {
        let mut replicas = self.replicas.lock().await;
        let ack_offset = Arc::new(AtomicUsize::new(0));
        replicas.push(ReplicaInfo {
            sender: tx,
            acknowledged_offset: Arc::clone(&ack_offset),
        });
        ack_offset
    }

    pub fn propagate(self: Arc<Self>, cmd: ParsedCommand) {
        tokio::spawn(async move {
            let resp_array = cmd.into_resp_array();
            let bytes = resp_array.encode_to_bytes();
            self.master_repl_offset
                .fetch_add(bytes.len(), Ordering::Relaxed);

            let replicas = self.replicas.lock().await;
            for replica_info in replicas.iter() {
                if let Err(e) = replica_info.sender.send(resp_array.clone()).await {
                    error!("Failed to propagate command to replica: {}", e);
                }
            }
            // After propagating, notify any waiting WAIT commands.
            self.wait_waiters.notify_all();
        });
    }

    pub async fn broadcast_getack(&self) {
        let getack_cmd = RespValue::Array(vec![
            RespValue::BulkString(Bytes::from_static(b"REPLCONF")),
            RespValue::BulkString(Bytes::from_static(b"GETACK")),
            RespValue::BulkString(Bytes::from_static(b"*")),
        ]);
        let replicas = self.replicas.lock().await;
        for replica_info in replicas.iter() {
            if let Err(e) = replica_info.sender.send(getack_cmd.clone()).await {
                error!("Failed to send GETACK to replica: {}", e);
            }
        }
    }

    pub async fn count_acks(&self, target_offset: usize) -> usize {
        let replicas = self.replicas.lock().await;
        replicas
            .iter()
            .filter(|r| r.acknowledged_offset.load(Ordering::Relaxed) >= target_offset)
            .count()
    }
}

pub async fn start_replica_mode(master_addr: String, listening_port: u16, ctx: Arc<AppContext>) {
    let corrected_addr = master_addr.replace(' ', ":");
    info!("Attempting to connect to master at {}", corrected_addr);

    match TcpStream::connect(&corrected_addr).await {
        Ok(stream) => {
            info!("Successfully connected to master.");
            if let Err(e) = perform_handshake(stream, listening_port, ctx).await {
                error!("Handshake with master failed: {e}");
            }
        }
        Err(e) => {
            error!("Failed to connect to master: {e}");
        }
    }
}

async fn perform_handshake(
    stream: TcpStream,
    listening_port: u16,
    ctx: Arc<AppContext>,
) -> Result<()> {
    let mut framed = Framed::new(stream, RespDecoder);

    framed
        .send(RespValue::Array(vec![RespValue::BulkString(
            Bytes::from_static(b"PING"),
        )]))
        .await?;
    let _ = framed.next().await.unwrap()?;

    framed
        .send(RespValue::Array(vec![
            RespValue::BulkString(Bytes::from_static(b"REPLCONF")),
            RespValue::BulkString(Bytes::from_static(b"listening-port")),
            RespValue::BulkString(Bytes::from(listening_port.to_string())),
        ]))
        .await?;
    let _ = framed.next().await.unwrap()?;

    framed
        .send(RespValue::Array(vec![
            RespValue::BulkString(Bytes::from_static(b"REPLCONF")),
            RespValue::BulkString(Bytes::from_static(b"capa")),
            RespValue::BulkString(Bytes::from_static(b"psync2")),
        ]))
        .await?;
    let _ = framed.next().await.unwrap()?;

    framed
        .send(RespValue::Array(vec![
            RespValue::BulkString(Bytes::from_static(b"PSYNC")),
            RespValue::BulkString(Bytes::from_static(b"?")),
            RespValue::BulkString(Bytes::from_static(b"-1")),
        ]))
        .await?;
    let _ = framed.next().await.unwrap()?;

    info!("Handshake complete. Receiving RDB file from master.");

    let parts = framed.into_parts();
    let mut stream = parts.io;
    let mut read_buf = parts.read_buf;

    let mut line_buffer = Vec::new();
    loop {
        if read_buf.is_empty() && stream.read_buf(&mut read_buf).await? == 0 {
            break;
        }
        let byte = read_buf.get_u8();
        line_buffer.push(byte);
        if line_buffer.ends_with(b"\r\n") {
            break;
        }
    }
    let len_str = std::str::from_utf8(&line_buffer[1..line_buffer.len() - 2])?;
    let rdb_len = len_str.parse::<usize>()?;

    while read_buf.len() < rdb_len {
        stream.read_buf(&mut read_buf).await?;
    }
    read_buf.advance(rdb_len);

    let mut new_parts = FramedParts::new(stream, parts.codec);
    new_parts.read_buf = read_buf;
    let mut framed = Framed::from_parts(new_parts);

    info!("RDB file consumed. Listening for propagated commands.");

    let mut offset = 0usize;
    loop {
        match framed.next().await {
            Some(Ok(value)) => {
                debug!("Replica received value from master: {value:?}");
                let raw_bytes = value.encode_to_bytes();
                let parsed_command = ParsedCommand::from_resp(value)?;

                let cmd = parsed_command.command();
                let is_write = matches!(
                    cmd,
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

                if is_write {
                    let response = crate::app::apply_command(&ctx, parsed_command.clone()).await;
                    if let RespValue::Error(e) = response {
                        warn!(
                            "Replica failed to apply write command {:?}: {}",
                            cmd,
                            String::from_utf8_lossy(&e)
                        );
                    }
                }

                if parsed_command.command() == Command::ReplConf
                    && parsed_command
                        .arg(0)
                        .is_some_and(|a| a.eq_ignore_ascii_case(b"getack"))
                {
                    let ack_response = RespValue::Array(vec![
                        RespValue::BulkString(Bytes::from_static(b"REPLCONF")),
                        RespValue::BulkString(Bytes::from_static(b"ACK")),
                        RespValue::BulkString(Bytes::from(offset.to_string())),
                    ]);
                    framed.send(ack_response).await?;
                }
                offset += raw_bytes.len();
            }
            Some(Err(e)) => {
                error!("Error receiving from master: {e}");
                break;
            }
            None => {
                info!("Connection to master closed.");
                break;
            }
        }
    }

    Ok(())
}

pub async fn serve_replica(
    mut rx: mpsc::Receiver<RespValue>,
    mut framed: Framed<TcpStream, RespDecoder>,
    ack_offset: Arc<AtomicUsize>,
    wait_notify: Arc<Notify>,
) {
    info!("New replica serving task started.");
    loop {
        tokio::select! {
            Some(cmd) = rx.recv() => {
                if let Err(e) = framed.send(cmd).await {
                    error!("Failed to propagate command to replica: {}. Closing connection.", e);
                    break;
                }
            }
            result = framed.next() => {
                match result {
                    Some(Ok(value)) => {
                        let parsed = match ParsedCommand::from_resp(value) {
                            Ok(p) => p,
                            Err(e) => { warn!("Error parsing command from replica: {e}"); continue; },
                        };
                        if parsed.command() == Command::ReplConf && parsed.arg(0).is_some_and(|a| a.eq_ignore_ascii_case(b"ack")) {
                           if let Some(offset_bytes) = parsed.arg(1) {
                               if let Ok(offset_str) = std::str::from_utf8(offset_bytes) {
                                   if let Ok(offset) = offset_str.parse::<usize>() {
                                       ack_offset.store(offset, Ordering::Relaxed);
                                       wait_notify.notify_one();
                                   }
                               }
                           }
                        } else {
                            warn!("Unexpected command from replica: {:?}", parsed.command());
                        }
                    }
                    Some(Err(e)) => { error!("Error reading from replica: {e}. Closing connection."); break; }
                    None => { info!("Replica connection closed."); break; }
                }
            }
        }
    }
    info!("Replica serving task finished.");
}
