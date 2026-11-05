use crate::app::{
    command::{Command, ParsedCommand},
    protocol::RespValue,
    store::{DataType, Store},
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
    store.with_keyspace(|ks| ks.set(key.clone(), DataType::String(value.clone()), expires_at));
    RespValue::SimpleString(Bytes::from_static(b"OK"))
}

fn handle_get(parsed: ParsedCommand, store: &Store) -> RespValue {
    let Some(key) = parsed.first() else {
        return RespValue::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'get' command",
        ));
    };

    store.with_keyspace(|ks| match ks.get_as(key, DataType::as_string) {
        Ok(Some(value)) => RespValue::BulkString(value.clone()),
        Ok(None) => RespValue::NullBulkString,
        Err(e) => e.into(),
    })
}

fn handle_incr(parsed: ParsedCommand, store: &Store) -> RespValue {
    let Some(key) = parsed.first() else {
        return RespValue::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'incr' command",
        ));
    };

    store.with_keyspace(|ks| {
        let value = match ks.get_or_create_as(key, DataType::as_string, || {
            DataType::String(Bytes::from_static(b"0"))
        }) {
            Ok(value) => value,
            Err(e) => return e.into(),
        };
        let Some(new_val) = std::str::from_utf8(value)
            .ok()
            .and_then(|s| s.parse::<i64>().ok())
            .and_then(|n| n.checked_add(1))
        else {
            return RespValue::Error(Bytes::from_static(
                b"ERR value is not an integer or out of range",
            ));
        };
        *value = Bytes::from(new_val.to_string());
        RespValue::Integer(new_val)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run(parts: &[&str], store: &Arc<Store>) -> RespValue {
        let command = ParsedCommand::from_resp(RespValue::Array(
            parts
                .iter()
                .map(|p| RespValue::BulkString(Bytes::copy_from_slice(p.as_bytes())))
                .collect(),
        ))
        .expect("valid command");
        handle(command, store)
    }

    #[test]
    fn incr_rejects_overflow_and_keeps_the_value() {
        let store = Arc::new(Store::new());
        run(&["SET", "n", &i64::MAX.to_string()], &store);
        assert_eq!(
            run(&["INCR", "n"], &store),
            RespValue::Error(Bytes::from_static(
                b"ERR value is not an integer or out of range"
            ))
        );
        assert_eq!(
            run(&["GET", "n"], &store),
            RespValue::BulkString(Bytes::from(i64::MAX.to_string()))
        );
    }

    #[test]
    fn incr_starts_an_expired_key_from_zero() {
        let store = Arc::new(Store::new());
        let past = Instant::now().checked_sub(Duration::from_secs(1));
        store.with_keyspace(|ks| {
            ks.set(
                Bytes::from_static(b"n"),
                DataType::String(Bytes::from_static(b"41")),
                past,
            )
        });
        assert_eq!(run(&["INCR", "n"], &store), RespValue::Integer(1));
    }
}
