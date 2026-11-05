use crate::app::command::Command;
use crate::app::{
    command::ParsedCommand,
    error::AppError,
    protocol::RespValue,
    store::{resolve_range, DataType, Store},
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

    let pushed = store.with_keyspace(|ks| {
        let list =
            ks.get_or_create_as(key, DataType::as_list, || DataType::List(VecDeque::new()))?;
        for value in values {
            if left {
                list.push_front(value.clone());
            } else {
                list.push_back(value.clone());
            }
        }
        Ok::<_, AppError>(list.len())
    });

    match pushed {
        Ok(len) => {
            waiters.notify_one(key);
            RespValue::Integer(len as i64)
        }
        Err(e) => e.into(),
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

    store.with_keyspace(|ks| {
        let list = match ks.get_as(key, DataType::as_list) {
            Ok(Some(list)) => list,
            Ok(None) => return RespValue::NullBulkString,
            Err(e) => return e.into(),
        };
        let mut popped: Vec<Bytes> = list.drain(..count.min(list.len())).collect();
        ks.remove_if_empty(key);
        if count == 1 && popped.len() == 1 {
            RespValue::BulkString(popped.swap_remove(0))
        } else {
            RespValue::Array(popped.into_iter().map(RespValue::BulkString).collect())
        }
    })
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

    // Pops from the first key holding a non-empty list; keys of other types are skipped.
    let try_pop = || {
        store.with_keyspace(|ks| {
            for key in keys {
                if let Ok(Some(list)) = ks.get_as(key, DataType::as_list) {
                    if let Some(value) = list.pop_front() {
                        ks.remove_if_empty(key);
                        return Some((key.clone(), value));
                    }
                }
            }
            None
        })
    };

    if let Some((key, value)) = try_pop() {
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

    if let Some((key, value)) = try_pop() {
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
    store.with_keyspace(|ks| match ks.get_as(key, DataType::as_list) {
        Ok(Some(list)) => RespValue::Integer(list.len() as i64),
        Ok(None) => RespValue::Integer(0),
        Err(e) => e.into(),
    })
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

    store.with_keyspace(|ks| match ks.get_as(key, DataType::as_list) {
        Ok(Some(list)) => {
            let items = resolve_range(start, stop, list.len())
                .map(|(first, last)| {
                    list.range(first..=last)
                        .cloned()
                        .map(RespValue::BulkString)
                        .collect()
                })
                .unwrap_or_default();
            RespValue::Array(items)
        }
        Ok(None) => RespValue::Array(vec![]),
        Err(e) => e.into(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn run(parts: &[&str], store: &Arc<Store>) -> RespValue {
        let command = ParsedCommand::from_resp(RespValue::Array(
            parts
                .iter()
                .map(|p| RespValue::BulkString(Bytes::copy_from_slice(p.as_bytes())))
                .collect(),
        ))
        .expect("valid command");
        handle(command, store, &Arc::new(WaiterRegistry::new())).await
    }

    fn bulks(items: &[&str]) -> RespValue {
        RespValue::Array(
            items
                .iter()
                .map(|s| RespValue::BulkString(Bytes::copy_from_slice(s.as_bytes())))
                .collect(),
        )
    }

    #[tokio::test]
    async fn lrange_resolves_negative_and_out_of_range_indexes() {
        let store = Arc::new(Store::new());
        run(&["RPUSH", "l", "a", "b", "c"], &store).await;
        let cases: [(&str, &str, &[&str]); 7] = [
            ("0", "-1", &["a", "b", "c"]),
            ("-2", "-1", &["b", "c"]),
            ("1", "100", &["b", "c"]),
            ("-100", "0", &["a"]),
            ("0", "-100", &[]),
            ("3", "5", &[]),
            ("2", "1", &[]),
        ];
        for (start, stop, expected) in cases {
            assert_eq!(
                run(&["LRANGE", "l", start, stop], &store).await,
                bulks(expected),
                "LRANGE l {start} {stop}"
            );
        }
    }

    #[tokio::test]
    async fn popping_the_last_element_deletes_the_list() {
        let store = Arc::new(Store::new());
        run(&["RPUSH", "l", "a", "b"], &store).await;
        assert_eq!(run(&["LPOP", "l", "2"], &store).await, bulks(&["a", "b"]));
        assert_eq!(
            store.with_keyspace(|ks| ks.type_name(&Bytes::from_static(b"l"))),
            "none"
        );

        run(&["RPUSH", "m", "x"], &store).await;
        assert_eq!(run(&["BLPOP", "m", "0"], &store).await, bulks(&["m", "x"]));
        assert_eq!(
            store.with_keyspace(|ks| ks.type_name(&Bytes::from_static(b"m"))),
            "none"
        );
    }

    #[tokio::test]
    async fn push_replaces_an_expired_key_of_another_type() {
        let store = Arc::new(Store::new());
        let past = std::time::Instant::now().checked_sub(Duration::from_secs(1));
        store.with_keyspace(|ks| {
            ks.set(
                Bytes::from_static(b"k"),
                DataType::String(Bytes::from_static(b"old")),
                past,
            )
        });
        assert_eq!(
            run(&["RPUSH", "k", "x"], &store).await,
            RespValue::Integer(1)
        );
        assert_eq!(
            run(&["LRANGE", "k", "0", "-1"], &store).await,
            bulks(&["x"])
        );
    }
}
