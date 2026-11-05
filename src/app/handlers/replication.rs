use crate::app::command::Command;
use crate::app::{command::ParsedCommand, protocol::RespValue, replication::ReplicationState};
use bytes::Bytes;
use std::sync::Arc;
use std::time::Duration;

pub async fn handle(parsed: ParsedCommand, replication: &Arc<ReplicationState>) -> RespValue {
    match parsed.command() {
        Command::Info => handle_info(parsed, replication),
        Command::ReplConf => handle_replconf(),
        Command::Wait => handle_wait(parsed, replication).await,
        _ => RespValue::Error(Bytes::from_static(b"ERR unknown replication command")),
    }
}
fn handle_info(parsed: ParsedCommand, replication: &ReplicationState) -> RespValue {
    let Some(section) = parsed.first() else {
        return RespValue::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'info' command",
        ));
    };
    if section.eq_ignore_ascii_case(b"replication") {
        RespValue::BulkString(Bytes::from(replication.info_string()))
    } else {
        RespValue::BulkString(Bytes::from_static(b""))
    }
}

fn handle_replconf() -> RespValue {
    RespValue::SimpleString(Bytes::from_static(b"OK"))
}

async fn handle_wait(parsed: ParsedCommand, replication: &ReplicationState) -> RespValue {
    let (Some(num_replicas_str), Some(timeout_str)) = (parsed.arg(0), parsed.arg(1)) else {
        return RespValue::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'wait' command",
        ));
    };
    let num_replicas = std::str::from_utf8(num_replicas_str)
        .ok()
        .and_then(|s| s.parse::<usize>().ok());
    let timeout_ms = std::str::from_utf8(timeout_str)
        .ok()
        .and_then(|s| s.parse::<u64>().ok());
    let (Some(num_replicas), Some(timeout_ms)) = (num_replicas, timeout_ms) else {
        return RespValue::Error(Bytes::from_static(
            b"ERR value is not an integer or out of range",
        ));
    };

    let target_offset = replication.master_repl_offset();
    let already_synced = replication.count_acks(target_offset);
    if already_synced >= num_replicas {
        return RespValue::Integer(already_synced as i64);
    }

    replication.broadcast_getack();
    let synced = replication
        .wait_for_acks(
            target_offset,
            num_replicas,
            Duration::from_millis(timeout_ms),
        )
        .await;
    RespValue::Integer(synced as i64)
}
