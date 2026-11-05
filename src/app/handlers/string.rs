use crate::app::{
    command::{parse, ParsedCommand},
    error::AppError,
    handlers::Reply,
    protocol::RespValue,
    store::{DataType, Store},
};
use bytes::Bytes;
use std::time::{Duration, Instant};

pub fn set(parsed: &ParsedCommand, store: &Store) -> Reply {
    let key = parsed.required(0)?;
    let value = parsed.required(1)?;
    let expires_at = match parsed.arg(2) {
        None => None,
        Some(option) if option.eq_ignore_ascii_case(b"px") => {
            if parsed.arg(3).is_none() {
                return Err(AppError::Syntax);
            }
            let millis: u64 = parsed.integer(3)?;
            Instant::now().checked_add(Duration::from_millis(millis))
        }
        Some(_) => return Err(AppError::Syntax),
    };
    store.with_keyspace(|ks| ks.set(key.clone(), DataType::String(value.clone()), expires_at));
    Ok(RespValue::SimpleString(Bytes::from_static(b"OK")))
}

pub fn get(parsed: &ParsedCommand, store: &Store) -> Reply {
    let key = parsed.required(0)?;
    store.with_keyspace(|ks| {
        Ok(match ks.get_as(key, DataType::as_string)? {
            Some(value) => RespValue::BulkString(value.clone()),
            None => RespValue::NullBulkString,
        })
    })
}

pub fn incr(parsed: &ParsedCommand, store: &Store) -> Reply {
    let key = parsed.required(0)?;
    store.with_keyspace(|ks| {
        let value = ks.get_or_create_as(key, DataType::as_string, || {
            DataType::String(Bytes::from_static(b"0"))
        })?;
        let new_val = parse::<i64>(value)
            .and_then(|n| n.checked_add(1))
            .ok_or(AppError::NotAnInteger)?;
        *value = Bytes::from(new_val.to_string());
        Ok(RespValue::Integer(new_val))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::AppContext;
    use std::sync::Arc;

    async fn run(parts: &[&str], ctx: &Arc<AppContext>) -> RespValue {
        crate::app::run_test(parts, ctx).await
    }

    #[tokio::test]
    async fn incr_rejects_overflow_and_keeps_the_value() {
        let ctx = crate::app::test_context();
        run(&["SET", "n", &i64::MAX.to_string()], &ctx).await;
        assert_eq!(
            run(&["INCR", "n"], &ctx).await,
            RespValue::Error(Bytes::from_static(
                b"ERR value is not an integer or out of range"
            ))
        );
        assert_eq!(
            run(&["GET", "n"], &ctx).await,
            RespValue::BulkString(Bytes::from(i64::MAX.to_string()))
        );
    }

    #[tokio::test]
    async fn incr_starts_an_expired_key_from_zero() {
        let ctx = crate::app::test_context();
        let past = Instant::now().checked_sub(Duration::from_secs(1));
        ctx.store.with_keyspace(|ks| {
            ks.set(
                Bytes::from_static(b"n"),
                DataType::String(Bytes::from_static(b"41")),
                past,
            )
        });
        assert_eq!(run(&["INCR", "n"], &ctx).await, RespValue::Integer(1));
    }
}
