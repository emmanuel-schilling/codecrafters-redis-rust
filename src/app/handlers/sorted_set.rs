use crate::app::{
    command::{Command, ParsedCommand},
    error::AppError, // Import AppError
    protocol::RespValue,
    sorted_set::SortedSet,
    store::{DataType, Store, StoreValue},
};
use bytes::Bytes;
use std::sync::{Arc, RwLock};

// This function now acts as a dispatcher that handles the Result from internal handlers
// and always returns a RespValue suitable for sending to the client.
pub fn handle(parsed: ParsedCommand, store: &Arc<Store>) -> RespValue {
    // Call the appropriate internal handler which returns Result<RespValue, AppError>
    let handler_result = match parsed.command() {
        Command::ZAdd => handle_zadd(parsed, store),
        Command::ZCard => handle_zcard(parsed, store),
        Command::ZScore => handle_zscore(parsed, store),
        Command::ZRank => handle_zrank(parsed, store),
        Command::ZRange => handle_zrange(parsed, store),
        Command::ZRem => handle_zrem(parsed, store),
        // For unknown commands, we still return a RespValue::Error directly.
        // This is typically not an AppError that needs propagation to the server loop,
        // but rather a RESP-level error to send back to the client.
        _ => Ok(RespValue::Error(Bytes::from_static(
            b"ERR unknown sorted set command",
        ))),
    };

    // Process the Result:
    // - If Ok(resp_value), return resp_value (this could be a successful RESP value or a RESP error).
    // - If Err(app_error), convert the internal AppError to a generic RESP error.
    match handler_result {
        Ok(resp_value) => resp_value, // Successfully obtained a RespValue (either success or RESP error).
        Err(app_error) => {
            // This is an internal AppError (e.g., lock poisoning).
            // Convert it to a standard RESP error string.
            RespValue::Error(Bytes::from(format!("ERR internal error: {}", app_error)))
        }
    }
}

// Internal handlers now return Result<RespValue, AppError> to allow detailed error reporting.
// AppError::ValueError("Mutex poisoned".to_string()) is used for RwLock poisoning.

fn handle_zadd(parsed: ParsedCommand, store: &Arc<Store>) -> Result<RespValue, AppError> {
    let (Some(key), Some(score_bytes), Some(member)) =
        (parsed.arg(0), parsed.arg(1), parsed.arg(2))
    else {
        // Argument errors are RESP errors, not internal AppErrors.
        return Ok(RespValue::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'zadd' command",
        )));
    };

    let score = match std::str::from_utf8(score_bytes)
        .ok()
        .and_then(|s| s.parse::<f64>().ok())
    {
        Some(f) => f,
        None => {
            return Ok(RespValue::Error(Bytes::from_static(
                b"ERR value is not a valid float",
            )))
        }
    };

    // Ensure the key exists or is created with the correct type.
    let entry = store.db().entry(key.clone()).or_insert_with(|| StoreValue {
        data: DataType::SortedSet(RwLock::new(SortedSet::new())),
        expires_at: None,
    });

    match &entry.value().data {
        DataType::SortedSet(zset_lock) => {
            // Replace .unwrap() with error propagation, mapping lock poisoning to ValueError.
            let mut zset = zset_lock
                .write()
                .map_err(|_| AppError::ValueError("Mutex poisoned".to_string()))?;
            Ok(RespValue::Integer(zset.add(score, member.clone()) as i64))
        }
        _ => Ok(RespValue::Error(Bytes::from_static(
            b"WRONGTYPE Operation against a key holding the wrong kind of value",
        ))),
    }
}

fn handle_zcard(parsed: ParsedCommand, store: &Arc<Store>) -> Result<RespValue, AppError> {
    let Some(key) = parsed.first() else {
        return Ok(RespValue::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'zcard' command",
        )));
    };

    match store.db().get(key) {
        Some(entry) if !store.is_expired(&entry) => match &entry.data {
            DataType::SortedSet(zset_lock) => {
                // Replace .unwrap() with error propagation.
                let zset = zset_lock
                    .read()
                    .map_err(|_| AppError::ValueError("Mutex poisoned".to_string()))?;
                Ok(RespValue::Integer(zset.cardinality() as i64))
            }
            _ => Ok(RespValue::Error(Bytes::from_static(
                b"WRONGTYPE Operation against a key holding the wrong kind of value",
            ))),
        },
        _ => Ok(RespValue::Integer(0)), // Key doesn't exist, return 0.
    }
}

fn handle_zscore(parsed: ParsedCommand, store: &Arc<Store>) -> Result<RespValue, AppError> {
    let (Some(key), Some(member)) = (parsed.arg(0), parsed.arg(1)) else {
        return Ok(RespValue::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'zscore' command",
        )));
    };

    match store.db().get(key) {
        Some(entry) if !store.is_expired(&entry) => match &entry.data {
            DataType::SortedSet(zset_lock) => {
                // Replace .unwrap() with error propagation.
                let zset = zset_lock
                    .read()
                    .map_err(|_| AppError::ValueError("Mutex poisoned".to_string()))?;
                match zset.score_of(member) {
                    Some(score) => Ok(RespValue::BulkString(Bytes::from(score.to_string()))),
                    None => Ok(RespValue::NullBulkString),
                }
            }
            _ => Ok(RespValue::Error(Bytes::from_static(
                b"WRONGTYPE Operation against a key holding the wrong kind of value",
            ))),
        },
        _ => Ok(RespValue::NullBulkString), // Key doesn't exist.
    }
}

fn handle_zrank(parsed: ParsedCommand, store: &Arc<Store>) -> Result<RespValue, AppError> {
    let (Some(key), Some(member)) = (parsed.arg(0), parsed.arg(1)) else {
        return Ok(RespValue::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'zrank' command",
        )));
    };

    match store.db().get(key) {
        Some(entry) if !store.is_expired(&entry) => match &entry.data {
            DataType::SortedSet(zset_lock) => {
                // Replace .unwrap() with error propagation.
                let zset = zset_lock
                    .read()
                    .map_err(|_| AppError::ValueError("Mutex poisoned".to_string()))?;
                match zset.rank_of(member) {
                    Some(rank) => Ok(RespValue::Integer(rank as i64)),
                    None => Ok(RespValue::NullBulkString),
                }
            }
            _ => Ok(RespValue::Error(Bytes::from_static(
                b"WRONGTYPE Operation against a key holding the wrong kind of value",
            ))),
        },
        _ => Ok(RespValue::NullBulkString), // Key doesn't exist.
    }
}

fn handle_zrange(parsed: ParsedCommand, store: &Arc<Store>) -> Result<RespValue, AppError> {
    let (Some(key), Some(start_bytes), Some(stop_bytes)) =
        (parsed.arg(0), parsed.arg(1), parsed.arg(2))
    else {
        return Ok(RespValue::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'zrange' command",
        )));
    };

    let start = match std::str::from_utf8(start_bytes)
        .ok()
        .and_then(|s| s.parse::<i64>().ok())
    {
        Some(i) => i,
        None => {
            return Ok(RespValue::Error(Bytes::from_static(
                b"ERR value is not an integer or out of range",
            )))
        }
    };
    let stop = match std::str::from_utf8(stop_bytes)
        .ok()
        .and_then(|s| s.parse::<i64>().ok())
    {
        Some(i) => i,
        None => {
            return Ok(RespValue::Error(Bytes::from_static(
                b"ERR value is not an integer or out of range",
            )))
        }
    };

    match store.db().get(key) {
        Some(entry) if !store.is_expired(&entry) => match &entry.data {
            DataType::SortedSet(zset_lock) => {
                // Replace .unwrap() with error propagation.
                let zset = zset_lock
                    .read()
                    .map_err(|_| AppError::ValueError("Mutex poisoned".to_string()))?;
                let len = zset.cardinality() as i64;
                let start_idx = store.normalize_index(start, len);
                let stop_idx = store.normalize_index(stop, len);

                if start_idx > stop_idx {
                    return Ok(RespValue::Array(vec![]));
                }
                let members = zset.get_range(start_idx, stop_idx);
                Ok(RespValue::Array(
                    members.into_iter().map(RespValue::BulkString).collect(),
                ))
            }
            _ => Ok(RespValue::Error(Bytes::from_static(
                b"WRONGTYPE Operation against a key holding the wrong kind of value",
            ))),
        },
        _ => Ok(RespValue::Array(vec![])), // Key doesn't exist.
    }
}

fn handle_zrem(parsed: ParsedCommand, store: &Arc<Store>) -> Result<RespValue, AppError> {
    let (Some(key), Some(member)) = (parsed.arg(0), parsed.arg(1)) else {
        return Ok(RespValue::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'zrem' command",
        )));
    };

    match store.db().get(key) {
        Some(entry) if !store.is_expired(&entry) => match &entry.data {
            DataType::SortedSet(zset_lock) => {
                // Replace .unwrap() with error propagation.
                let mut zset = zset_lock
                    .write()
                    .map_err(|_| AppError::ValueError("Mutex poisoned".to_string()))?;
                Ok(RespValue::Integer(zset.remove(member) as i64))
            }
            _ => Ok(RespValue::Error(Bytes::from_static(
                b"WRONGTYPE Operation against a key holding the wrong kind of value",
            ))),
        },
        _ => Ok(RespValue::Integer(0)), // Key doesn't exist.
    }
}
