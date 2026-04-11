use crate::app::command::Command;
use crate::app::{
    command::ParsedCommand,
    protocol::RespValue,
    store::{DataType, Store, StoreValue},
    wait::WaiterRegistry,
};
use bytes::Bytes;
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

pub async fn handle(
    parsed: ParsedCommand,
    store: &Arc<Store>,
    waiters: &Arc<WaiterRegistry>,
) -> RespValue {
    match parsed.command() {
        Command::LPush => handle_push(parsed, store, waiters, true),
        Command::RPush => handle_push(parsed, store, waiters, false),
        Command::LPop => handle_lpop(parsed, store),
        Command::LLen => handle_llen(parsed, store),
        Command::LRange => handle_lrange(parsed, store),
        Command::BLPop => handle_blpop(parsed, store, waiters).await,
        _ => RespValue::Error(Bytes::from_static(b"ERR unknown list command")),
    }
}

fn handle_push(
    parsed: ParsedCommand,
    store: &Store,
    waiters: &WaiterRegistry,
    left: bool,
) -> RespValue {
    let Some(key) = parsed.first() else {
        return RespValue::Error(Bytes::from_static(b"ERR wrong number of arguments"));
    };
    let values = parsed.args_from(1);
    if values.is_empty() {
        return RespValue::Error(Bytes::from_static(b"ERR wrong number of arguments"));
    }

    let mut entry = store.db().entry(key.clone()).or_insert_with(|| StoreValue {
        data: DataType::List(VecDeque::new()),
        expires_at: None,
    });

    match &mut entry.value_mut().data {
        DataType::List(list) => {
            for value in values {
                if left {
                    list.push_front(value.clone());
                } else {
                    list.push_back(value.clone());
                }
            }
            let len = list.len();
            if len > 0 {
                waiters.notify_one(key);
            }
            RespValue::Integer(len as i64)
        }
        _ => RespValue::Error(Bytes::from_static(
            b"WRONGTYPE Operation against a key holding the wrong kind of value",
        )),
    }
}

fn handle_lpop(parsed: ParsedCommand, store: &Store) -> RespValue {
    let Some(key) = parsed.first() else {
        return RespValue::Error(Bytes::from_static(b"ERR wrong number of arguments"));
    };
    let count = parsed
        .arg(1)
        .and_then(|s| std::str::from_utf8(s).ok())
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(1);

    let mut entry = match store.db().get_mut(key) {
        Some(entry) => entry,
        None => return RespValue::NullBulkString,
    };

    if store.is_expired(&entry) {
        drop(entry);
        store.db().remove(key);
        return RespValue::NullBulkString;
    }

    match &mut entry.value_mut().data {
        DataType::List(list) => {
            if list.is_empty() {
                return RespValue::NullBulkString;
            }
            let mut popped = Vec::with_capacity(count);
            for _ in 0..count {
                if let Some(val) = list.pop_front() {
                    popped.push(val);
                } else {
                    break;
                }
            }
            if popped.len() == 1 && count == 1 {
                RespValue::BulkString(popped.into_iter().next().unwrap())
            } else {
                RespValue::Array(popped.into_iter().map(RespValue::BulkString).collect())
            }
        }
        _ => RespValue::Error(Bytes::from_static(
            b"WRONGTYPE Operation against a key holding the wrong kind of value",
        )),
    }
}

async fn handle_blpop(parsed: ParsedCommand, store: &Store, waiters: &WaiterRegistry) -> RespValue {
    let Some((keys, timeout_str)) = parsed
        .args_from(0)
        .split_last()
        .map(|(last, elements)| (elements, last))
    else {
        return RespValue::Error(Bytes::from_static(b"ERR wrong number of arguments"));
    };

    let timeout_secs = match std::str::from_utf8(timeout_str)
        .ok()
        .and_then(|s| s.parse::<f64>().ok())
    {
        Some(t) => t,
        None => {
            return RespValue::Error(Bytes::from_static(
                b"ERR timeout is not a float or out of range",
            ))
        }
    };

    let try_pop = |store: &Store, keys: &[Bytes]| {
        for key in keys {
            let mut entry = match store.db().get_mut(key) {
                Some(entry) => entry,
                None => continue,
            };

            if store.is_expired(&entry) {
                drop(entry);
                store.db().remove(key);
                continue;
            }

            if let DataType::List(list) = &mut entry.value_mut().data {
                if let Some(value) = list.pop_front() {
                    return Some((key.clone(), value));
                }
            }
        }
        None
    };

    if let Some((key, value)) = try_pop(store, keys) {
        return RespValue::Array(vec![
            RespValue::BulkString(key),
            RespValue::BulkString(value),
        ]);
    }

    let timeout = if timeout_secs == 0.0 {
        None
    } else {
        Some(Duration::from_secs_f64(timeout_secs))
    };

    waiters.wait_for_any(keys, timeout).await;

    if let Some((key, value)) = try_pop(store, keys) {
        RespValue::Array(vec![
            RespValue::BulkString(key),
            RespValue::BulkString(value),
        ])
    } else {
        RespValue::NullArray
    }
}

fn handle_llen(parsed: ParsedCommand, store: &Store) -> RespValue {
    let Some(key) = parsed.first() else {
        return RespValue::Error(Bytes::from_static(b"ERR wrong number of arguments"));
    };
    match store.db().get(key) {
        Some(entry) if !store.is_expired(&entry) => match &entry.data {
            DataType::List(list) => RespValue::Integer(list.len() as i64),
            _ => RespValue::Error(Bytes::from_static(
                b"WRONGTYPE Operation against a key holding the wrong kind of value",
            )),
        },
        _ => RespValue::Integer(0),
    }
}

fn handle_lrange(parsed: ParsedCommand, store: &Store) -> RespValue {
    let (Some(key), Some(start_str), Some(stop_str)) =
        (parsed.arg(0), parsed.arg(1), parsed.arg(2))
    else {
        return RespValue::Error(Bytes::from_static(b"ERR wrong number of arguments"));
    };

    let start = match std::str::from_utf8(start_str)
        .ok()
        .and_then(|s| s.parse::<i64>().ok())
    {
        Some(i) => i,
        None => {
            return RespValue::Error(Bytes::from_static(
                b"ERR value is not an integer or out of range",
            ))
        }
    };
    let stop = match std::str::from_utf8(stop_str)
        .ok()
        .and_then(|s| s.parse::<i64>().ok())
    {
        Some(i) => i,
        None => {
            return RespValue::Error(Bytes::from_static(
                b"ERR value is not an integer or out of range",
            ))
        }
    };

    match store.db().get(key) {
        Some(entry) if !store.is_expired(&entry) => match &entry.data {
            DataType::List(list) => {
                let len = list.len() as i64;
                let start_idx = store.normalize_index(start, len);
                let stop_idx = store.normalize_index(stop, len);

                if start_idx > stop_idx {
                    return RespValue::Array(vec![]);
                }

                let items = list
                    .iter()
                    .skip(start_idx)
                    .take(stop_idx - start_idx + 1)
                    .cloned()
                    .map(RespValue::BulkString)
                    .collect();
                RespValue::Array(items)
            }
            _ => RespValue::Error(Bytes::from_static(
                b"WRONGTYPE Operation against a key holding the wrong kind of value",
            )),
        },
        _ => RespValue::Array(vec![]),
    }
}
