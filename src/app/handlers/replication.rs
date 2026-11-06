use crate::app::{
    command::ParsedCommand, handlers::Reply, protocol::RespValue, replication::ReplicationState,
};
use bytes::Bytes;
use std::time::Duration;

pub fn info(parsed: &ParsedCommand, replication: &ReplicationState) -> Reply {
    let section = parsed.required(0)?;
    Ok(if section.eq_ignore_ascii_case(b"replication") {
        RespValue::BulkString(Bytes::from(replication.info_string()))
    } else {
        RespValue::BulkString(Bytes::from_static(b""))
    })
}

pub fn replconf() -> Reply {
    Ok(RespValue::SimpleString(Bytes::from_static(b"OK")))
}

/// WAIT inside a transaction, which doesn't wait, as in Redis: the number of
/// replicas that have acknowledged every write so far.
pub fn wait_now(parsed: &ParsedCommand, replication: &ReplicationState) -> Reply {
    let _: usize = parsed.integer(0)?;
    let _: u64 = parsed.integer(1)?;
    let synced = replication.count_acks(replication.master_repl_offset());
    Ok(RespValue::Integer(synced as i64))
}

/// WAIT: blocks until enough replicas acknowledge every write so far, or until
/// the timeout; a timeout of 0 waits indefinitely.
pub async fn wait(parsed: &ParsedCommand, replication: &ReplicationState) -> Reply {
    let num_replicas: usize = parsed.integer(0)?;
    let timeout_ms: u64 = parsed.integer(1)?;

    let target_offset = replication.master_repl_offset();
    let already_synced = replication.count_acks(target_offset);
    if already_synced >= num_replicas {
        return Ok(RespValue::Integer(already_synced as i64));
    }

    replication.broadcast_getack();
    let timeout = (timeout_ms > 0).then(|| Duration::from_millis(timeout_ms));
    let synced = replication
        .wait_for_acks(target_offset, num_replicas, timeout)
        .await;
    Ok(RespValue::Integer(synced as i64))
}
