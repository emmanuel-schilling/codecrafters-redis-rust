use crate::app::{
    command::{parse, ParsedCommand},
    error::AppError,
    exec::Changes,
    handlers::Reply,
    protocol::RespValue,
    store::{instant_at, DataType, Keyspace},
};
use bytes::Bytes;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// SET key value [PX ms | PXAT unix-ms]. A PX expiry becomes a Unix time in
/// milliseconds, which replicas receive as PXAT: the key expires at the same
/// moment here and there, however late they apply it.
pub fn set(parsed: &ParsedCommand, ks: &mut Keyspace, changes: &mut Changes) -> Reply {
    let key = parsed.required(0)?;
    let value = parsed.required(1)?;
    let mut expires_at = None;
    if let Some(option) = parsed.arg(2) {
        if parsed.arg(3).is_none() {
            return Err(AppError::Syntax);
        }
        let millis = Duration::from_millis(parsed.integer(3)?);
        if option.eq_ignore_ascii_case(b"px") {
            let at = SystemTime::now()
                .checked_add(millis)
                .and_then(|at| at.duration_since(UNIX_EPOCH).ok())
                .and_then(|at| u64::try_from(at.as_millis()).ok());
            match at {
                Some(at) => {
                    expires_at = instant_at(Duration::from_millis(at));
                    let pxat = parsed
                        .clone()
                        .with_arg(2, Bytes::from_static(b"PXAT"))
                        .with_arg(3, Bytes::from(at.to_string()));
                    changes.replicate_as(vec![pxat.into_resp_array()]);
                }
                // An expiry too far ahead to represent is sent as received.
                None => expires_at = Instant::now().checked_add(millis),
            }
        } else if option.eq_ignore_ascii_case(b"pxat") {
            expires_at = instant_at(millis);
        } else {
            return Err(AppError::Syntax);
        }
    }
    ks.set(key.clone(), DataType::String(value.clone()), expires_at);
    changes.wrote(key);
    Ok(RespValue::SimpleString(Bytes::from_static(b"OK")))
}

pub fn get(parsed: &ParsedCommand, ks: &mut Keyspace) -> Reply {
    let key = parsed.required(0)?;
    Ok(match ks.get_as(key, DataType::as_string)? {
        Some(value) => RespValue::BulkString(value.clone()),
        None => RespValue::NullBulkString,
    })
}

/// STRLEN key: the length of a string in bytes; 0 for a missing key.
pub fn strlen(parsed: &ParsedCommand, ks: &mut Keyspace) -> Reply {
    let key = parsed.required(0)?;
    let len = ks
        .get_as(key, DataType::as_string)?
        .map_or(0, |value| value.len());
    Ok(RespValue::Integer(len as i64))
}

pub fn incr(parsed: &ParsedCommand, ks: &mut Keyspace, changes: &mut Changes) -> Reply {
    let key = parsed.required(0)?;
    let value = ks.get_or_create_as(key, DataType::as_string, || {
        DataType::String(Bytes::from_static(b"0"))
    })?;
    let new_val = parse::<i64>(value)
        .and_then(|n| n.checked_add(1))
        .ok_or(AppError::NotAnInteger)?;
    *value = Bytes::from(new_val.to_string());
    changes.wrote(key);
    Ok(RespValue::Integer(new_val))
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

    #[tokio::test]
    async fn set_px_reaches_replicas_as_pxat() {
        let ctx = crate::app::test_context();
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        ctx.replication.add_replica(tx);
        let before = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_millis();
        run(&["SET", "k", "v", "px", "60000"], &ctx).await;
        let after = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_millis();

        let RespValue::Array(parts) = rx.try_recv().expect("sent") else {
            panic!("not a command");
        };
        let bulk = |s: &'static str| RespValue::BulkString(Bytes::from_static(s.as_bytes()));
        assert_eq!(
            parts[..4],
            [bulk("SET"), bulk("k"), bulk("v"), bulk("PXAT")]
        );
        let RespValue::BulkString(at) = &parts[4] else {
            panic!("no time");
        };
        let at: u128 = parse(at).expect("unix milliseconds");
        assert!((before + 60000..=after + 60000).contains(&at), "{at}");
    }

    #[tokio::test]
    async fn set_pxat_expires_at_the_given_unix_time() {
        let ctx = crate::app::test_context();
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_millis();
        run(
            &["SET", "live", "v", "PXAT", &(now + 60000).to_string()],
            &ctx,
        )
        .await;
        run(
            &["SET", "gone", "v", "pxat", &(now - 1000).to_string()],
            &ctx,
        )
        .await;
        assert_eq!(
            run(&["GET", "live"], &ctx).await,
            RespValue::BulkString(Bytes::from_static(b"v"))
        );
        assert_eq!(run(&["GET", "gone"], &ctx).await, RespValue::NullBulkString);
        assert_eq!(
            run(&["SET", "k", "v", "EX", "10"], &ctx).await,
            RespValue::Error(Bytes::from_static(b"ERR syntax error"))
        );
    }
}
