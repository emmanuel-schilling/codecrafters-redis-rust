use crate::app::command::Command;
use crate::app::{
    command::ParsedCommand,
    error::AppError,
    protocol::RespValue,
    store::{resolve_range, DataType, Store},
    wait::Popped,
    AppContext,
};
use bytes::Bytes;
use std::collections::VecDeque;
use std::time::Duration;

pub async fn handle(parsed: ParsedCommand, ctx: &AppContext) -> RespValue {
    let store = &ctx.store;
    match parsed.command() {
        Command::LPush => handle_push(parsed, store, true),
        Command::RPush => handle_push(parsed, store, false),
        Command::LPop => handle_lpop(parsed, store),
        Command::LLen => handle_llen(parsed, store),
        Command::LRange => handle_lrange(parsed, store),
        Command::BLPop => handle_blpop(parsed, ctx).await,
        _ => RespValue::Error(Bytes::from_static(b"ERR unknown list command")),
    }
}

fn handle_push(parsed: ParsedCommand, store: &Store, left: bool) -> RespValue {
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
        Ok(len) => RespValue::Integer(len as i64),
        Err(e) => e.into(),
    }
}

fn handle_lpop(parsed: ParsedCommand, store: &Store) -> RespValue {
    let Some(key) = parsed.first() else {
        return RespValue::Error(Bytes::from_static(b"ERR wrong number of arguments"));
    };
    // With a count, LPOP always replies with an array (or a null array).
    let count = match parsed.arg(1) {
        None => None,
        Some(arg) => match std::str::from_utf8(arg)
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
        {
            Some(count) => Some(count),
            None => {
                return RespValue::Error(Bytes::from_static(
                    b"ERR value is out of range, must be positive",
                ))
            }
        },
    };

    store.with_keyspace(|ks| {
        let list = match ks.get_as(key, DataType::as_list) {
            Ok(Some(list)) => list,
            Ok(None) if count.is_some() => return RespValue::NullArray,
            Ok(None) => return RespValue::NullBulkString,
            Err(e) => return e.into(),
        };
        let reply = match count {
            None => list
                .pop_front()
                .map_or(RespValue::NullBulkString, RespValue::BulkString),
            Some(count) => RespValue::Array(
                list.drain(..count.min(list.len()))
                    .map(RespValue::BulkString)
                    .collect(),
            ),
        };
        ks.remove_if_empty(key);
        reply
    })
}

async fn handle_blpop(parsed: ParsedCommand, ctx: &AppContext) -> RespValue {
    let Some((keys, timeout_str)) = parsed
        .args_from(0)
        .split_last()
        .map(|(last, elements)| (elements, last))
    else {
        return RespValue::Error(Bytes::from_static(b"ERR wrong number of arguments"));
    };

    let invalid_timeout = || {
        RespValue::Error(Bytes::from_static(
            b"ERR timeout is not a float or out of range",
        ))
    };
    let timeout = match std::str::from_utf8(timeout_str)
        .ok()
        .and_then(|s| s.parse::<f64>().ok())
    {
        None => return invalid_timeout(),
        Some(secs) if secs < 0.0 => {
            return RespValue::Error(Bytes::from_static(b"ERR timeout is negative"))
        }
        Some(0.0) => None,
        Some(secs) => match Duration::try_from_secs_f64(secs) {
            Ok(timeout) => Some(timeout),
            Err(_) => return invalid_timeout(),
        },
    };

    // Check and register under the write-order lock, which every push also
    // holds: a push either lands first (and is popped here) or after (and
    // hands its element to this client).
    let mut wait = {
        let _order = ctx.write_order.lock().await;
        if let Some(popped) = pop_first(ctx, keys) {
            return blpop_reply(popped);
        }
        ctx.waiters.block_on_lists(keys)
    };

    let delivered = match timeout {
        None => (&mut wait.delivery).await.ok(),
        Some(timeout) => match tokio::time::timeout(timeout, &mut wait.delivery).await {
            Ok(delivered) => delivered.ok(),
            Err(_) if wait.waiter.cancel() => None,
            // An element was handed over just as the timeout expired: take it.
            Err(_) => (&mut wait.delivery).await.ok(),
        },
    };
    delivered.map_or(RespValue::NullArray, blpop_reply)
}

fn blpop_reply((key, value): Popped) -> RespValue {
    RespValue::Array(vec![
        RespValue::BulkString(key),
        RespValue::BulkString(value),
    ])
}

/// The LPOP that replicas apply for an element popped from `key` by BLPOP.
fn lpop_for_replicas(key: &Bytes) -> RespValue {
    RespValue::Array(vec![
        RespValue::BulkString(Bytes::from_static(b"LPOP")),
        RespValue::BulkString(key.clone()),
    ])
}

/// Pops from the first key holding a non-empty list (keys of other types are
/// skipped) and sends the pop to replicas as LPOP, which never blocks there.
/// The caller holds the write-order lock.
fn pop_first(ctx: &AppContext, keys: &[Bytes]) -> Option<Popped> {
    let popped = ctx.store.with_keyspace(|ks| {
        for key in keys {
            if let Ok(Some(list)) = ks.get_as(key, DataType::as_list) {
                if let Some(value) = list.pop_front() {
                    ks.remove_if_empty(key);
                    return Some((key.clone(), value));
                }
            }
        }
        None
    });
    if let Some((key, _)) = &popped {
        ctx.replication.propagate(lpop_for_replicas(key));
    }
    popped
}

/// Hands elements of `key` to clients blocked on it, oldest first, as Redis
/// does right after a command that adds to a list. Each hand-over is sent to
/// replicas as LPOP. The caller holds the write-order lock and has already
/// propagated the command that added the elements.
pub fn serve_blocked_clients(ctx: &AppContext, key: &Bytes) {
    let served = ctx.store.with_keyspace(|ks| {
        let Ok(Some(list)) = ks.get_as(key, DataType::as_list) else {
            return 0;
        };
        let served = ctx.waiters.serve_list(key, list);
        ks.remove_if_empty(key);
        served
    });
    for _ in 0..served {
        ctx.replication.propagate(lpop_for_replicas(key));
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
    use std::sync::Arc;

    async fn run(parts: &[&str], ctx: &Arc<AppContext>) -> RespValue {
        let command = ParsedCommand::from_resp(RespValue::Array(
            parts
                .iter()
                .map(|p| RespValue::BulkString(Bytes::copy_from_slice(p.as_bytes())))
                .collect(),
        ))
        .expect("valid command");
        handle(command, ctx).await
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
        let ctx = crate::app::test_context();
        run(&["RPUSH", "l", "a", "b", "c"], &ctx).await;
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
                run(&["LRANGE", "l", start, stop], &ctx).await,
                bulks(expected),
                "LRANGE l {start} {stop}"
            );
        }
    }

    #[tokio::test]
    async fn popping_the_last_element_deletes_the_list() {
        let ctx = crate::app::test_context();
        run(&["RPUSH", "l", "a", "b"], &ctx).await;
        assert_eq!(run(&["LPOP", "l", "2"], &ctx).await, bulks(&["a", "b"]));
        assert_eq!(
            ctx.store
                .with_keyspace(|ks| ks.type_name(&Bytes::from_static(b"l"))),
            "none"
        );

        run(&["RPUSH", "m", "x"], &ctx).await;
        assert_eq!(run(&["BLPOP", "m", "0"], &ctx).await, bulks(&["m", "x"]));
        assert_eq!(
            ctx.store
                .with_keyspace(|ks| ks.type_name(&Bytes::from_static(b"m"))),
            "none"
        );
    }

    #[tokio::test]
    async fn push_replaces_an_expired_key_of_another_type() {
        let ctx = crate::app::test_context();
        let past = std::time::Instant::now().checked_sub(Duration::from_secs(1));
        ctx.store.with_keyspace(|ks| {
            ks.set(
                Bytes::from_static(b"k"),
                DataType::String(Bytes::from_static(b"old")),
                past,
            )
        });
        assert_eq!(run(&["RPUSH", "k", "x"], &ctx).await, RespValue::Integer(1));
        assert_eq!(run(&["LRANGE", "k", "0", "-1"], &ctx).await, bulks(&["x"]));
    }

    #[tokio::test]
    async fn blocked_clients_are_served_in_arrival_order() {
        let ctx = crate::app::test_context();
        let mut first = Box::pin(run_command(&["BLPOP", "q", "0"], &ctx));
        let mut second = Box::pin(run_command(&["BLPOP", "q", "0"], &ctx));
        assert!(futures::poll!(&mut first).is_pending());
        assert!(futures::poll!(&mut second).is_pending());

        run_command(&["RPUSH", "q", "a", "b", "c"], &ctx).await;
        assert_eq!(first.await, bulks(&["q", "a"]));
        assert_eq!(second.await, bulks(&["q", "b"]));
        assert_eq!(run(&["LRANGE", "q", "0", "-1"], &ctx).await, bulks(&["c"]));
    }

    #[tokio::test]
    async fn blpop_rejects_a_negative_timeout() {
        let ctx = crate::app::test_context();
        assert_eq!(
            run(&["BLPOP", "q", "-1"], &ctx).await,
            RespValue::Error(Bytes::from_static(b"ERR timeout is negative"))
        );
    }

    #[tokio::test]
    async fn a_timed_out_blpop_leaves_no_registration() {
        let ctx = crate::app::test_context();
        assert_eq!(
            run(&["BLPOP", "q", "0.01"], &ctx).await,
            RespValue::NullArray
        );
        run_command(&["RPUSH", "q", "x"], &ctx).await;
        assert_eq!(run(&["LLEN", "q"], &ctx).await, RespValue::Integer(1));
    }

    /// Runs a command through the full write path, as a client connection does.
    async fn run_command(parts: &[&str], ctx: &Arc<AppContext>) -> RespValue {
        let command = ParsedCommand::from_resp(RespValue::Array(
            parts
                .iter()
                .map(|p| RespValue::BulkString(Bytes::copy_from_slice(p.as_bytes())))
                .collect(),
        ))
        .expect("valid command");
        crate::app::handle_command(
            command,
            ctx,
            &mut crate::app::ConnectionState::Normal,
            &mut vec![],
        )
        .await
    }

    #[tokio::test]
    async fn lpop_with_a_count_always_replies_with_an_array() {
        let ctx = crate::app::test_context();
        run(&["RPUSH", "l", "a", "b"], &ctx).await;
        assert_eq!(run(&["LPOP", "l", "1"], &ctx).await, bulks(&["a"]));
        assert_eq!(
            run(&["LPOP", "l"], &ctx).await,
            RespValue::BulkString(Bytes::from_static(b"b"))
        );
        assert_eq!(run(&["LPOP", "l", "2"], &ctx).await, RespValue::NullArray);
        assert_eq!(run(&["LPOP", "l"], &ctx).await, RespValue::NullBulkString);
        assert_eq!(
            run(&["LPOP", "l", "-1"], &ctx).await,
            RespValue::Error(Bytes::from_static(
                b"ERR value is out of range, must be positive"
            ))
        );
    }
}
