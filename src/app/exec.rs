//! Runs commands against the locked keyspace, and carries out what their
//! writes imply: clients watching the keys see them changed, replicas receive
//! the writes, and clients blocked on the keys are served.
//!
//! Everything happens while the keyspace is locked, so commands, the writes
//! replicas receive, and the clients they serve all follow one order.

use crate::app::{
    command::{Command, ParsedCommand},
    handlers,
    protocol::RespValue,
    replication::Role,
    store::{DataType, Expiry, Keyspace},
    AppContext,
};
use bytes::Bytes;
use tracing::warn;

/// What one command changed, recorded by its handler.
#[derive(Debug, Default)]
pub struct Changes {
    /// The keys it wrote, in order.
    keys: Vec<Bytes>,
    /// What replicas apply instead of the command as received, if it differs
    /// (an XADD's generated ID, BLPOP as LPOP).
    replicated_as: Option<Vec<RespValue>>,
}

impl Changes {
    /// Records that the command changed `key`.
    pub fn wrote(&mut self, key: &Bytes) {
        self.keys.push(key.clone());
    }

    /// Sends replicas `commands` instead of the command as received.
    pub fn replicate_as(&mut self, commands: Vec<RespValue>) {
        self.replicated_as = Some(commands);
    }
}

/// Commands for replicas and the AOF, in the order they were applied here.
pub type Outbox = Vec<RespValue>;

/// Runs `f` for a client with the keyspace locked, then sends replicas what
/// it applied, still holding the lock so they receive writes in the order applied.
pub fn locked<R>(ctx: &AppContext, f: impl FnOnce(&mut Keyspace, &mut Outbox) -> R) -> R {
    let expiry = match ctx.replication.role() {
        Role::Master => Expiry::Delete,
        Role::Replica => Expiry::Hide,
    };
    locked_with(ctx, expiry, f)
}

/// Runs `f` with the keyspace locked for writes a replica receives from its
/// master, which see keys the master hasn't deleted as live.
pub fn locked_for_master<R>(
    ctx: &AppContext,
    f: impl FnOnce(&mut Keyspace, &mut Outbox) -> R,
) -> R {
    locked_with(ctx, Expiry::Ignore, f)
}

fn locked_with<R>(
    ctx: &AppContext,
    expiry: Expiry,
    f: impl FnOnce(&mut Keyspace, &mut Outbox) -> R,
) -> R {
    ctx.store.with_keyspace(|ks| {
        ks.set_expiry(expiry);
        let mut outbox = Outbox::new();
        let result = f(ks, &mut outbox);
        send_expired(ks, &mut outbox);
        // Appended before the client is replied to, so with `appendfsync
        // always` an acknowledged write is already on disk.
        if let Some(aof) = &ctx.aof {
            aof.append(&outbox);
        }
        // A replica's writes come from its master; it has no replicas of its own.
        if ctx.replication.role() == Role::Master {
            for command in outbox {
                ctx.replication.propagate(command);
            }
        }
        result
    })
}

/// Applies the commands read back from the AOF at startup, before any client
/// connects. As in Redis, keys don't expire while the file is replayed, and
/// nothing replayed is written back to the AOF or sent to replicas. A
/// transaction's MULTI and EXEC are skipped: the replay runs in one locked
/// section anyway.
pub fn replay(ctx: &AppContext, commands: &[ParsedCommand]) {
    ctx.store.with_keyspace(|ks| {
        ks.set_expiry(Expiry::Ignore);
        let mut discarded = Outbox::new();
        for parsed in commands {
            if matches!(parsed.command(), Command::Multi | Command::Exec) {
                continue;
            }
            let reply = match parsed.check_arity() {
                Ok(()) => run(ctx, ks, parsed, &mut discarded),
                Err(e) => e.into(),
            };
            if let RespValue::Error(e) = reply {
                warn!(
                    "failed to replay {:?} from the AOF: {}",
                    parsed.command(),
                    String::from_utf8_lossy(&e)
                );
            }
            discarded.clear();
        }
    });
}

/// Runs a command that doesn't wait (blocking commands reply at once) and
/// carries out its writes: watchers of the written keys see them changed,
/// the writes go into `outbox`, and clients blocked on the keys are served.
pub fn run(
    ctx: &AppContext,
    ks: &mut Keyspace,
    parsed: &ParsedCommand,
    outbox: &mut Outbox,
) -> RespValue {
    let mut changes = Changes::default();
    let reply = handlers::execute(ctx, ks, &mut changes, parsed);
    // Keys the command found expired were deleted before it changed anything.
    send_expired(ks, outbox);
    if changes.keys.is_empty() {
        return reply;
    }
    for key in &changes.keys {
        ctx.watches.touch(key);
    }
    match changes.replicated_as {
        Some(commands) => outbox.extend(commands),
        None => outbox.push(parsed.clone().into_resp_array()),
    }
    for key in &changes.keys {
        serve_blocked_clients(ctx, ks, key, outbox);
        ctx.waiters.stream_changed(key);
    }
    reply
}

/// Sends replicas a DEL for each key deleted because it expired.
fn send_expired(ks: &mut Keyspace, outbox: &mut Outbox) {
    for key in ks.take_expired() {
        outbox.push(RespValue::Array(vec![
            RespValue::BulkString(Bytes::from_static(b"DEL")),
            RespValue::BulkString(key),
        ]));
    }
}

/// Hands elements of the list at `key` to clients blocked on it in BLPOP,
/// sending each pop to replicas as LPOP.
fn serve_blocked_clients(ctx: &AppContext, ks: &mut Keyspace, key: &Bytes, outbox: &mut Outbox) {
    if !ctx.waiters.blocked_on_list(key) {
        return;
    }
    let Ok(Some(list)) = ks.get_as(key, DataType::as_list) else {
        return;
    };
    let served = ctx.waiters.serve_list(key, list);
    if served == 0 {
        return;
    }
    ks.remove_if_empty(key);
    for _ in 0..served {
        outbox.push(lpop(key));
    }
}

/// The LPOP replicas apply for an element popped from `key` by BLPOP.
pub fn lpop(key: &Bytes) -> RespValue {
    RespValue::Array(vec![
        RespValue::BulkString(Bytes::from_static(b"LPOP")),
        RespValue::BulkString(key.clone()),
    ])
}

/// `commands` as replicas apply a transaction: wrapped in MULTI and EXEC when
/// there's more than one, so they apply them together.
pub fn as_transaction(commands: Outbox) -> Outbox {
    if commands.len() < 2 {
        return commands;
    }
    let word =
        |w: &'static [u8]| RespValue::Array(vec![RespValue::BulkString(Bytes::from_static(w))]);
    let mut wrapped = Vec::with_capacity(commands.len() + 2);
    wrapped.push(word(b"MULTI"));
    wrapped.extend(commands);
    wrapped.push(word(b"EXEC"));
    wrapped
}
