use crate::app::{
    command::{Command, ParsedCommand},
    protocol::{RespDecoder, RespValue},
    AppContext,
};
use anyhow::{bail, Context, Result};
use bytes::{Buf, Bytes};
use futures::{SinkExt, StreamExt};
use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hasher};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex, MutexGuard, PoisonError,
};
use std::time::Duration;
use tokio::io::AsyncReadExt;
use tokio::net::TcpStream;
use tokio::sync::{mpsc, Notify};
use tokio_util::codec::{Framed, FramedParts};
use tracing::{debug, error, info, warn};

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum Role {
    Master,
    Replica,
}

type ReplicaSender = mpsc::UnboundedSender<RespValue>;

#[derive(Debug)]
struct ReplicaInfo {
    sender: ReplicaSender,
    acknowledged_offset: Arc<AtomicUsize>,
}

/// The connected replicas and the replication offset, changed together so the
/// offset always matches what has been sent.
#[derive(Debug, Default)]
struct Replicas {
    connected: Vec<ReplicaInfo>,
    offset: usize,
}

#[derive(Debug)]
pub struct ReplicationState {
    role: Role,
    master_replid: String,
    replicas: Mutex<Replicas>,
    /// Wakes WAIT commands whenever a replica acknowledges an offset.
    acks_changed: Notify,
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
            master_replid: generate_replid(),
            replicas: Mutex::new(Replicas::default()),
            acks_changed: Notify::new(),
        }
    }

    pub fn role(&self) -> Role {
        self.role
    }
    pub fn master_replid(&self) -> &str {
        &self.master_replid
    }
    pub fn master_repl_offset(&self) -> usize {
        self.replicas().offset
    }

    fn replicas(&self) -> MutexGuard<'_, Replicas> {
        self.replicas.lock().unwrap_or_else(PoisonError::into_inner)
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

    /// Registers a replica that receives everything sent from now on. Returns
    /// the offset it has acknowledged, which its serving task keeps up to date.
    pub fn add_replica(&self, sender: ReplicaSender) -> Arc<AtomicUsize> {
        let acknowledged_offset = Arc::new(AtomicUsize::new(0));
        self.replicas().connected.push(ReplicaInfo {
            sender,
            acknowledged_offset: Arc::clone(&acknowledged_offset),
        });
        acknowledged_offset
    }

    /// Sends a write to every replica and advances the replication offset.
    /// Replicas receive writes in the order of the calls; callers make that
    /// the order the writes were applied in. Disconnected replicas are dropped.
    pub fn propagate(&self, command: RespValue) {
        let len = command.encode_to_bytes().len();
        let mut replicas = self.replicas();
        replicas.offset += len;
        replicas
            .connected
            .retain(|replica| replica.sender.send(command.clone()).is_ok());
    }

    /// Asks every replica to report the offset it has processed.
    pub fn broadcast_getack(&self) {
        let getack_cmd = RespValue::Array(vec![
            RespValue::BulkString(Bytes::from_static(b"REPLCONF")),
            RespValue::BulkString(Bytes::from_static(b"GETACK")),
            RespValue::BulkString(Bytes::from_static(b"*")),
        ]);
        self.replicas()
            .connected
            .retain(|replica| replica.sender.send(getack_cmd.clone()).is_ok());
    }

    /// The number of replicas that have acknowledged at least `target_offset`.
    pub fn count_acks(&self, target_offset: usize) -> usize {
        self.replicas()
            .connected
            .iter()
            .filter(|r| r.acknowledged_offset.load(Ordering::Acquire) >= target_offset)
            .count()
    }

    /// Waits until `wanted` replicas have acknowledged `target_offset`, or until
    /// `timeout` passes (`None` waits indefinitely), and returns how many have.
    pub async fn wait_for_acks(
        &self,
        target_offset: usize,
        wanted: usize,
        timeout: Option<Duration>,
    ) -> usize {
        // No deadline either for no timeout or for one too far ahead to represent.
        let deadline = timeout.and_then(|timeout| tokio::time::Instant::now().checked_add(timeout));
        loop {
            // Register for the next ACK before counting, so one that arrives
            // between the count and the wait still wakes this task.
            let acked = self.acks_changed.notified();
            tokio::pin!(acked);
            acked.as_mut().enable();

            let count = self.count_acks(target_offset);
            if count >= wanted {
                return count;
            }
            match deadline {
                Some(deadline) => {
                    if tokio::time::timeout_at(deadline, acked).await.is_err() {
                        return self.count_acks(target_offset);
                    }
                }
                None => acked.await,
            }
        }
    }

    fn record_ack(&self, acknowledged_offset: &AtomicUsize, offset: usize) {
        acknowledged_offset.store(offset, Ordering::Release);
        self.acks_changed.notify_waiters();
    }
}

/// A new 40-character hexadecimal replication ID, as Redis generates at
/// startup. It only has to differ between runs; it isn't a secret.
fn generate_replid() -> String {
    // Each `RandomState` is seeded randomly by the standard library.
    let mut id: String = (0..3)
        .map(|_| format!("{:016x}", RandomState::new().build_hasher().finish()))
        .collect();
    id.truncate(40);
    id
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

/// Sends one handshake command and waits for the master's reply.
async fn handshake_step(
    framed: &mut Framed<TcpStream, RespDecoder>,
    parts: &[&[u8]],
) -> Result<RespValue> {
    let command = parts
        .iter()
        .map(|part| RespValue::BulkString(Bytes::copy_from_slice(part)))
        .collect();
    framed.send(RespValue::Array(command)).await?;
    Ok(framed
        .next()
        .await
        .context("master closed the connection during the handshake")??)
}

async fn perform_handshake(
    stream: TcpStream,
    listening_port: u16,
    ctx: Arc<AppContext>,
) -> Result<()> {
    let mut framed = Framed::new(stream, RespDecoder);
    let port = listening_port.to_string();

    handshake_step(&mut framed, &[b"PING"]).await?;
    handshake_step(
        &mut framed,
        &[b"REPLCONF", b"listening-port", port.as_bytes()],
    )
    .await?;
    handshake_step(&mut framed, &[b"REPLCONF", b"capa", b"psync2"]).await?;
    handshake_step(&mut framed, &[b"PSYNC", b"?", b"-1"]).await?;

    info!("Handshake complete. Receiving RDB file from master.");

    // The RDB file arrives as `$<len>\r\n` and `len` raw bytes with no trailing CRLF.
    let parts = framed.into_parts();
    let mut stream = parts.io;
    let mut read_buf = parts.read_buf;

    let mut line = Vec::new();
    while !line.ends_with(b"\r\n") {
        if read_buf.is_empty() && stream.read_buf(&mut read_buf).await? == 0 {
            bail!("master closed the connection before sending the RDB file");
        }
        line.push(read_buf.get_u8());
    }
    let rdb_len: usize = line
        .strip_prefix(b"$")
        .and_then(|l| l.strip_suffix(b"\r\n"))
        .and_then(|l| std::str::from_utf8(l).ok())
        .and_then(|l| l.parse().ok())
        .context("invalid RDB length line from master")?;

    while read_buf.len() < rdb_len {
        if stream.read_buf(&mut read_buf).await? == 0 {
            bail!("master closed the connection while sending the RDB file");
        }
    }
    read_buf.advance(rdb_len);

    let mut new_parts = FramedParts::new(stream, parts.codec);
    new_parts.read_buf = read_buf;
    let mut framed = Framed::from_parts(new_parts);

    info!("RDB file consumed. Listening for propagated commands.");

    let mut offset = 0usize;
    // The writes of a transaction from the master, between its MULTI and EXEC.
    let mut transaction: Option<Vec<ParsedCommand>> = None;
    loop {
        match framed.next().await {
            Some(Ok(value)) => {
                debug!("Replica received value from master: {value:?}");
                let raw_len = value.encode_to_bytes().len();
                match ParsedCommand::from_resp(value) {
                    Ok(parsed_command) => {
                        let cmd = parsed_command.command();
                        if cmd == Command::Multi {
                            transaction = Some(Vec::new());
                        } else if cmd == Command::Exec {
                            if let Some(writes) = transaction.take() {
                                crate::app::apply_from_master(&ctx, &writes);
                            }
                        } else if cmd.is_write() {
                            match &mut transaction {
                                Some(writes) => writes.push(parsed_command),
                                None => crate::app::apply_from_master(&ctx, &[parsed_command]),
                            }
                        } else if cmd == Command::ReplConf
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
                    }
                    Err(e) => warn!("Replica ignored a command from the master: {e}"),
                }
                offset += raw_len;
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
    mut rx: mpsc::UnboundedReceiver<RespValue>,
    mut framed: Framed<TcpStream, RespDecoder>,
    acknowledged_offset: Arc<AtomicUsize>,
    replication: Arc<ReplicationState>,
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
                        let is_ack = parsed.command() == Command::ReplConf
                            && parsed.arg(0).is_some_and(|a| a.eq_ignore_ascii_case(b"ack"));
                        let offset = parsed
                            .arg(1)
                            .and_then(|o| std::str::from_utf8(o).ok())
                            .and_then(|o| o.parse::<usize>().ok());
                        match (is_ack, offset) {
                            (true, Some(offset)) => replication.record_ack(&acknowledged_offset, offset),
                            _ => warn!("Unexpected command from replica: {:?}", parsed.command()),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::store::DataType;
    use tokio::io::AsyncWriteExt;
    use tokio::net::TcpListener;

    fn master() -> Arc<ReplicationState> {
        let config = <crate::config::Config as clap::Parser>::parse_from(["redis"]);
        Arc::new(ReplicationState::new_from_config(&config))
    }

    fn set(key: &'static [u8]) -> RespValue {
        RespValue::Array(vec![
            RespValue::BulkString(Bytes::from_static(b"SET")),
            RespValue::BulkString(Bytes::from_static(key)),
            RespValue::BulkString(Bytes::from_static(b"v")),
        ])
    }

    #[test]
    fn propagate_drops_disconnected_replicas() {
        let replication = master();
        let (tx1, mut rx1) = mpsc::unbounded_channel();
        let (tx2, rx2) = mpsc::unbounded_channel();
        replication.add_replica(tx1);
        replication.add_replica(tx2);
        assert_eq!(replication.count_acks(0), 2);

        drop(rx2);
        replication.propagate(set(b"a"));
        assert_eq!(replication.count_acks(0), 1);
        assert_eq!(rx1.try_recv().ok(), Some(set(b"a")));
        assert_eq!(
            replication.master_repl_offset(),
            set(b"a").encode_to_bytes().len()
        );
    }

    #[tokio::test]
    async fn wait_for_acks_wakes_when_a_replica_acknowledges() {
        let replication = master();
        let (tx, _rx) = mpsc::unbounded_channel();
        let acked = replication.add_replica(tx);
        replication.propagate(set(b"a"));
        let target = replication.master_repl_offset();

        let acker = {
            let replication = Arc::clone(&replication);
            tokio::spawn(async move { replication.record_ack(&acked, target) })
        };
        // The timeout only bounds a failure; a correct wait returns on the ACK.
        let count = replication
            .wait_for_acks(target, 1, Some(Duration::from_secs(60)))
            .await;
        acker.await.expect("acker");
        assert_eq!(count, 1);
    }

    #[tokio::test]
    async fn wait_for_acks_returns_the_count_at_the_timeout() {
        let replication = master();
        let (tx, _rx) = mpsc::unbounded_channel();
        replication.add_replica(tx);
        replication.propagate(set(b"a"));
        let target = replication.master_repl_offset();
        assert_eq!(
            replication
                .wait_for_acks(target, 1, Some(Duration::ZERO))
                .await,
            0
        );
    }

    /// Serves the replica handshake, then writes `after_handshake` and closes.
    async fn fake_master(after_handshake: Vec<u8>) -> TcpStream {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept");
            let mut framed = Framed::new(stream, RespDecoder);
            for reply in [
                "PONG",
                "OK",
                "OK",
                "FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0",
            ] {
                framed.next().await.expect("request").expect("decode");
                framed
                    .send(RespValue::SimpleString(Bytes::from(reply)))
                    .await
                    .expect("reply");
            }
            let mut stream = framed.into_inner();
            stream.write_all(&after_handshake).await.expect("write");
        });
        TcpStream::connect(addr).await.expect("connect")
    }

    #[tokio::test]
    async fn handshake_fails_when_the_master_disconnects_mid_rdb() {
        let stream = fake_master(b"$100\r\nREDIS0011".to_vec()).await;
        let result = tokio::time::timeout(
            Duration::from_secs(60),
            perform_handshake(stream, 6380, crate::app::test_context()),
        )
        .await
        .expect("handshake returns instead of spinning");
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn replica_skips_unknown_commands_and_applies_later_writes() {
        let mut stream_bytes = crate::app::rdb::empty_rdb_bytes();
        stream_bytes.extend_from_slice(b"*1\r\n$7\r\nUNKNOWN\r\n");
        stream_bytes.extend_from_slice(&set(b"k").encode_to_bytes());
        let stream = fake_master(stream_bytes).await;

        let ctx = crate::app::test_context();
        perform_handshake(stream, 6380, Arc::clone(&ctx))
            .await
            .expect("replication ends cleanly when the master closes");
        let value = ctx.store.with_keyspace(|ks| {
            ks.get_as(&Bytes::from_static(b"k"), DataType::as_string)
                .ok()
                .flatten()
                .cloned()
        });
        assert_eq!(value, Some(Bytes::from_static(b"v")));
    }

    #[tokio::test]
    async fn wait_for_acks_without_a_timeout_waits_for_the_ack() {
        let replication = master();
        let (tx, _rx) = mpsc::unbounded_channel();
        let acked = replication.add_replica(tx);
        replication.propagate(set(b"a"));
        let target = replication.master_repl_offset();

        let mut waiting = Box::pin(replication.wait_for_acks(target, 1, None));
        assert!(futures::poll!(&mut waiting).is_pending());
        replication.record_ack(&acked, target);
        assert_eq!(waiting.await, 1);
    }

    #[test]
    fn replication_ids_are_40_hex_characters_and_differ() {
        let (first, second) = (generate_replid(), generate_replid());
        for id in [&first, &second] {
            assert_eq!(id.len(), 40);
            assert!(id.chars().all(|c| c.is_ascii_hexdigit()));
        }
        assert_ne!(first, second);
    }
}
