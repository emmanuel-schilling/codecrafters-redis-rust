use crate::app::{
    command::{Command, ParsedCommand},
    protocol::RespValue,
    store::Store,
};
use crate::config::Config;
use bytes::Bytes;
use std::sync::Arc;
use tracing::warn;

pub fn handle(parsed: ParsedCommand, config: &Arc<Config>, store: &Arc<Store>) -> RespValue {
    match parsed.command() {
        Command::Config => handle_config(parsed, config),
        Command::Keys => handle_keys(parsed, store),
        Command::Type => handle_type(parsed, store),
        _ => RespValue::Error(Bytes::from_static(b"ERR unknown server command")),
    }
}

// Handler for the TYPE command.
fn handle_type(parsed: ParsedCommand, store: &Store) -> RespValue {
    let Some(key) = parsed.first() else {
        return RespValue::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'type' command",
        ));
    };
    let type_name = store.with_keyspace(|ks| ks.type_name(key));
    RespValue::SimpleString(Bytes::from_static(type_name.as_bytes()))
}

fn handle_config(parsed: ParsedCommand, config: &Config) -> RespValue {
    let Some(verb) = parsed.arg(0) else {
        return RespValue::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'config' command",
        ));
    };
    let Some(key_bytes) = parsed.arg(1) else {
        return RespValue::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'config' command",
        ));
    };

    if !verb.eq_ignore_ascii_case(b"get") {
        return RespValue::Error(Bytes::from_static(b"ERR CONFIG only supports GET"));
    }

    let key = match std::str::from_utf8(key_bytes) {
        Ok(s) => s,
        Err(_) => return RespValue::Error(Bytes::from_static(b"ERR invalid config key")),
    };

    match key.to_lowercase().as_str() {
        "dir" => RespValue::Array(vec![
            RespValue::BulkString(Bytes::from_static(b"dir")),
            RespValue::BulkString(Bytes::from(config.dir.clone())),
        ]),
        "dbfilename" => RespValue::Array(vec![
            RespValue::BulkString(Bytes::from_static(b"dbfilename")),
            RespValue::BulkString(Bytes::from(config.dbfilename.clone())),
        ]),
        _ => RespValue::Array(vec![]),
    }
}

fn handle_keys(parsed: ParsedCommand, store: &Store) -> RespValue {
    let Some(pattern) = parsed.first() else {
        return RespValue::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'keys' command",
        ));
    };

    if &pattern[..] != b"*" {
        warn!("Received KEYS command with non-'*' pattern, which is not supported.");
        return RespValue::Array(vec![]);
    }

    let keys = store.with_keyspace(|ks| ks.keys());
    RespValue::Array(keys.into_iter().map(RespValue::BulkString).collect())
}
