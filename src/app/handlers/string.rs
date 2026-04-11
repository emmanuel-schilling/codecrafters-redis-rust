use crate::app::{
    command::{Command, ParsedCommand},
    protocol::RespValue,
    store::{DataType, Store, StoreValue},
};
use bytes::Bytes;
use std::sync::Arc;
use std::time::{Duration, Instant};

pub fn handle(parsed: ParsedCommand, store: &Arc<Store>) -> RespValue {
    match parsed.command() {
        Command::Set => handle_set(parsed, store),
        Command::Get => handle_get(parsed, store),
        Command::Incr => handle_incr(parsed, store),
        _ => RespValue::Error(Bytes::from_static(b"ERR unknown string command")),
    }
}

fn handle_set(parsed: ParsedCommand, store: &Store) -> RespValue {
    let (Some(key), Some(value)) = (parsed.arg(0), parsed.arg(1)) else {
        return RespValue::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'set' command",
        ));
    };

    let mut expiry = None;
    if let Some(option) = parsed.arg(2) {
        if option.eq_ignore_ascii_case(b"px") {
            if let Some(millis_str) = parsed.arg(3) {
                if let Some(millis) = std::str::from_utf8(millis_str)
                    .ok()
                    .and_then(|s| s.parse::<u64>().ok())
                {
                    expiry = Some(Duration::from_millis(millis));
                } else {
                    return RespValue::Error(Bytes::from_static(
                        b"ERR value is not an integer or out of range",
                    ));
                }
            } else {
                return RespValue::Error(Bytes::from_static(b"ERR syntax error"));
            }
        } else {
            return RespValue::Error(Bytes::from_static(b"ERR syntax error"));
        }
    }

    let expires_at = expiry.and_then(|d| Instant::now().checked_add(d));
    let store_value = StoreValue {
        data: DataType::String(value.clone()),
        expires_at,
    };

    store.db().insert(key.clone(), store_value);
    RespValue::SimpleString(Bytes::from_static(b"OK"))
}

fn handle_get(parsed: ParsedCommand, store: &Store) -> RespValue {
    let Some(key) = parsed.first() else {
        return RespValue::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'get' command",
        ));
    };

    match store.db().get(key) {
        Some(entry) => {
            if store.is_expired(&entry) {
                drop(entry);
                store.db().remove(key);
                RespValue::NullBulkString
            } else {
                match &entry.data {
                    DataType::String(s) => RespValue::BulkString(s.clone()),
                    _ => RespValue::Error(Bytes::from_static(
                        b"WRONGTYPE Operation against a key holding the wrong kind of value",
                    )),
                }
            }
        }
        None => RespValue::NullBulkString,
    }
}

fn handle_incr(parsed: ParsedCommand, store: &Store) -> RespValue {
    let Some(key) = parsed.first() else {
        return RespValue::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'incr' command",
        ));
    };

    let mut entry = store.db().entry(key.clone()).or_insert_with(|| StoreValue {
        data: DataType::String(Bytes::from_static(b"0")),
        expires_at: None,
    });

    match &mut entry.value_mut().data {
        DataType::String(bytes) => {
            let current_val = match std::str::from_utf8(bytes)
                .ok()
                .and_then(|s| s.parse::<i64>().ok())
            {
                Some(v) => v,
                None => {
                    return RespValue::Error(Bytes::from_static(
                        b"ERR value is not an integer or out of range",
                    ))
                }
            };

            let new_val = current_val + 1;
            *bytes = Bytes::from(new_val.to_string());
            RespValue::Integer(new_val)
        }
        _ => RespValue::Error(Bytes::from_static(
            b"WRONGTYPE Operation against a key holding the wrong kind of value",
        )),
    }
}
