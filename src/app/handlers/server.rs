use crate::app::{
    command::ParsedCommand, error::AppError, handlers::Reply, protocol::RespValue, store::Store,
};
use crate::config::Config;
use bytes::Bytes;
use tracing::warn;

/// TYPE: the type of the value at a key.
pub fn key_type(parsed: &ParsedCommand, store: &Store) -> Reply {
    let key = parsed.required(0)?;
    let type_name = store.with_keyspace(|ks| ks.type_name(key));
    Ok(RespValue::SimpleString(Bytes::from_static(
        type_name.as_bytes(),
    )))
}

pub fn config(parsed: &ParsedCommand, config: &Config) -> Reply {
    let verb = parsed.required(0)?;
    let key_bytes = parsed.required(1)?;
    if !verb.eq_ignore_ascii_case(b"get") {
        return Err(AppError::ValueError("CONFIG only supports GET".into()));
    }
    let key = std::str::from_utf8(key_bytes)
        .map_err(|_| AppError::ValueError("invalid config key".into()))?;

    let value = match key.to_lowercase().as_str() {
        "dir" => config.dir.to_string_lossy().into_owned(),
        "dbfilename" => config.dbfilename.clone(),
        _ => return Ok(RespValue::Array(vec![])),
    };
    Ok(RespValue::Array(vec![
        RespValue::BulkString(Bytes::from(key.to_lowercase())),
        RespValue::BulkString(Bytes::from(value)),
    ]))
}

pub fn keys(parsed: &ParsedCommand, store: &Store) -> Reply {
    let pattern = parsed.required(0)?;
    if &pattern[..] != b"*" {
        warn!("Received KEYS command with non-'*' pattern, which is not supported.");
        return Ok(RespValue::Array(vec![]));
    }
    let keys = store.with_keyspace(|ks| ks.keys());
    Ok(RespValue::Array(
        keys.into_iter().map(RespValue::BulkString).collect(),
    ))
}
