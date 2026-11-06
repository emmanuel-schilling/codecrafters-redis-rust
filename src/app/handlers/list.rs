use crate::app::{
    command::{parse, Command, ParsedCommand},
    error::AppError,
    handlers::Reply,
    protocol::RespValue,
    store::{resolve_range, DataType, Store},
    wait::Popped,
    AppContext,
};
use bytes::Bytes;
use std::collections::VecDeque;
use std::time::Duration;

/// The end of a list that LPUSH or RPUSH adds to.
pub enum End {
    Front,
    Back,
}

pub fn push(parsed: &ParsedCommand, store: &Store, end: End) -> Reply {
    let key = parsed.required(0)?;
    parsed.required(1)?;
    let values = parsed.args_from(1);
    store.with_keyspace(|ks| {
        let list =
            ks.get_or_create_as(key, DataType::as_list, || DataType::List(VecDeque::new()))?;
        for value in values {
            match end {
                End::Front => list.push_front(value.clone()),
                End::Back => list.push_back(value.clone()),
            }
        }
        Ok(RespValue::Integer(list.len() as i64))
    })
}

pub fn lpop(parsed: &ParsedCommand, store: &Store) -> Reply {
    let key = parsed.required(0)?;
    // With a count, LPOP always replies with an array (or a null array).
    let count = match parsed.arg(1) {
        None => None,
        Some(arg) => Some(parse::<usize>(arg).ok_or_else(|| {
            AppError::ValueError("value is out of range, must be positive".into())
        })?),
    };

    store.with_keyspace(|ks| {
        let Some(list) = ks.get_as(key, DataType::as_list)? else {
            return Ok(match count {
                Some(_) => RespValue::NullArray,
                None => RespValue::NullBulkString,
            });
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
        Ok(reply)
    })
}

pub async fn blpop(parsed: &ParsedCommand, ctx: &AppContext) -> Reply {
    let (keys, timeout) = blpop_args(parsed)?;

    // Check and register under the write-order lock, which every push also
    // holds: a push either lands first (and is popped here) or after (and
    // hands its element to this client).
    let mut wait = {
        let _order = ctx.write_order.lock().await;
        if let Some(popped) = pop_first(ctx, keys) {
            return Ok(blpop_reply(popped));
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
    Ok(delivered.map_or(RespValue::NullArray, blpop_reply))
}

/// BLPOP inside a transaction, which doesn't block, as in Redis: it pops at
/// once or replies with a null array. The caller holds the write-order lock.
pub fn blpop_now(parsed: &ParsedCommand, ctx: &AppContext) -> Reply {
    let (keys, _) = blpop_args(parsed)?;
    Ok(pop_first(ctx, keys).map_or(RespValue::NullArray, blpop_reply))
}

/// BLPOP's keys and timeout (`None` to wait forever).
fn blpop_args(parsed: &ParsedCommand) -> Result<(&[Bytes], Option<Duration>), AppError> {
    let Some((timeout_str, keys)) = parsed.args_from(0).split_last() else {
        return Err(AppError::WrongArity(Command::BLPop));
    };
    if keys.is_empty() {
        return Err(AppError::WrongArity(Command::BLPop));
    }

    let invalid_timeout = || AppError::ValueError("timeout is not a float or out of range".into());
    let timeout = match parse::<f64>(timeout_str) {
        None => return Err(invalid_timeout()),
        Some(secs) if secs < 0.0 => return Err(AppError::ValueError("timeout is negative".into())),
        Some(0.0) => None,
        Some(secs) => Some(Duration::try_from_secs_f64(secs).map_err(|_| invalid_timeout())?),
    };
    Ok((keys, timeout))
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
/// skipped), marks the key changed for WATCH, and sends the pop to replicas
/// as LPOP, which never blocks there.
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
        ctx.watches.touch(key);
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

pub fn llen(parsed: &ParsedCommand, store: &Store) -> Reply {
    let key = parsed.required(0)?;
    store.with_keyspace(|ks| {
        let len = ks
            .get_as(key, DataType::as_list)?
            .map_or(0, |list| list.len());
        Ok(RespValue::Integer(len as i64))
    })
}

pub fn lrange(parsed: &ParsedCommand, store: &Store) -> Reply {
    let key = parsed.required(0)?;
    parsed.required(2)?;
    let start: i64 = parsed.integer(1)?;
    let stop: i64 = parsed.integer(2)?;

    store.with_keyspace(|ks| {
        let items = match ks.get_as(key, DataType::as_list)? {
            Some(list) => resolve_range(start, stop, list.len())
                .map(|(first, last)| {
                    list.range(first..=last)
                        .cloned()
                        .map(RespValue::BulkString)
                        .collect()
                })
                .unwrap_or_default(),
            None => vec![],
        };
        Ok(RespValue::Array(items))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    async fn run(parts: &[&str], ctx: &Arc<AppContext>) -> RespValue {
        crate::app::run_test(parts, ctx).await
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
        let mut first = Box::pin(run(&["BLPOP", "q", "0"], &ctx));
        let mut second = Box::pin(run(&["BLPOP", "q", "0"], &ctx));
        assert!(futures::poll!(&mut first).is_pending());
        assert!(futures::poll!(&mut second).is_pending());

        run(&["RPUSH", "q", "a", "b", "c"], &ctx).await;
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
        run(&["RPUSH", "q", "x"], &ctx).await;
        assert_eq!(run(&["LLEN", "q"], &ctx).await, RespValue::Integer(1));
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
