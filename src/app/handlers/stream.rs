use crate::app::command::Command;
use crate::app::{
    command::ParsedCommand,
    protocol::RespValue,
    store::{DataType, Store, StoreValue},
    stream::{Stream, XReadResult},
    wait::WaiterRegistry,
};
use bytes::Bytes;
use std::sync::Arc;
use std::time::Duration;

pub async fn handle(
    parsed: ParsedCommand,
    store: &Arc<Store>,
    waiters: &Arc<WaiterRegistry>,
) -> RespValue {
    match parsed.command() {
        Command::XAdd => handle_xadd(parsed, store, waiters),
        Command::XRange => handle_xrange(parsed, store),
        Command::XRead => handle_xread(parsed, store, waiters).await,
        _ => RespValue::Error(Bytes::from_static(b"ERR unknown stream command")),
    }
}

fn handle_xadd(
    parsed: ParsedCommand,
    store: &Arc<Store>,
    waiters: &Arc<WaiterRegistry>,
) -> RespValue {
    let (Some(key), Some(id_spec_bytes)) = (parsed.arg(0), parsed.arg(1)) else {
        return RespValue::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'xadd' command",
        ));
    };
    let Ok(id_spec) = std::str::from_utf8(id_spec_bytes) else {
        return RespValue::Error(Bytes::from_static(
            b"ERR invalid stream ID specified in XADD",
        ));
    };

    let fields_and_values = parsed.args_from(2);
    if fields_and_values.is_empty() || fields_and_values.len() % 2 != 0 {
        return RespValue::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'xadd' command",
        ));
    }

    let fields: Vec<(Bytes, Bytes)> = fields_and_values
        .chunks_exact(2)
        .map(|chunk| (chunk[0].clone(), chunk[1].clone()))
        .collect();

    let mut entry = store.db().entry(key.clone()).or_insert_with(|| StoreValue {
        data: DataType::Stream(Stream::new()),
        expires_at: None,
    });

    match &mut entry.value_mut().data {
        DataType::Stream(stream) => match stream.add(id_spec, fields) {
            Ok(id) => {
                waiters.notify_one(key);
                RespValue::BulkString(Bytes::from(id.to_string()))
            }
            Err(e) => RespValue::Error(Bytes::from(e.to_string())),
        },
        _ => RespValue::Error(Bytes::from_static(
            b"WRONGTYPE Operation against a key holding the wrong kind of value",
        )),
    }
}

fn handle_xrange(parsed: ParsedCommand, store: &Arc<Store>) -> RespValue {
    let (Some(key), Some(start_bytes), Some(end_bytes)) =
        (parsed.arg(0), parsed.arg(1), parsed.arg(2))
    else {
        return RespValue::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'xrange' command",
        ));
    };
    let Ok(start_spec) = std::str::from_utf8(start_bytes) else {
        return RespValue::Error(Bytes::from_static(
            b"ERR invalid start ID for 'xrange' command",
        ));
    };
    let Ok(end_spec) = std::str::from_utf8(end_bytes) else {
        return RespValue::Error(Bytes::from_static(
            b"ERR invalid end ID for 'xrange' command",
        ));
    };

    match store.db().get(key) {
        Some(entry) if !store.is_expired(&entry) => match &entry.data {
            DataType::Stream(stream) => match stream.range(start_spec, end_spec) {
                Ok(entries) => {
                    let result_array = entries
                        .into_iter()
                        .map(|(id, fields)| {
                            let mut field_array = Vec::with_capacity(fields.len() * 2);
                            for (field, value) in fields {
                                field_array.push(RespValue::BulkString(field));
                                field_array.push(RespValue::BulkString(value));
                            }
                            RespValue::Array(vec![
                                RespValue::BulkString(Bytes::from(id.to_string())),
                                RespValue::Array(field_array),
                            ])
                        })
                        .collect();
                    RespValue::Array(result_array)
                }
                Err(e) => RespValue::Error(Bytes::from(e.to_string())),
            },
            _ => RespValue::Error(Bytes::from_static(
                b"WRONGTYPE Operation against a key holding the wrong kind of value",
            )),
        },
        _ => RespValue::Array(vec![]),
    }
}

async fn handle_xread(
    parsed: ParsedCommand,
    store: &Arc<Store>,
    waiters: &Arc<WaiterRegistry>,
) -> RespValue {
    let mut args = parsed.args_from(0);
    let mut block_timeout = None;

    if args
        .first()
        .is_some_and(|a| a.eq_ignore_ascii_case(b"block"))
    {
        if args.len() < 3 {
            return RespValue::Error(Bytes::from_static(b"ERR syntax error"));
        }
        let Ok(timeout_ms_str) = std::str::from_utf8(&args[1]) else {
            return RespValue::Error(Bytes::from_static(
                b"ERR invalid timeout in 'xread' command",
            ));
        };
        let Ok(timeout_ms) = timeout_ms_str.parse::<u64>() else {
            return RespValue::Error(Bytes::from_static(
                b"ERR value is not an integer or out of range",
            ));
        };

        block_timeout = if timeout_ms == 0 {
            None
        } else {
            Some(Duration::from_millis(timeout_ms))
        };
        args = &args[2..];
    }

    if args
        .first()
        .is_some_and(|a| a.eq_ignore_ascii_case(b"streams"))
    {
        args = &args[1..];
    } else {
        return RespValue::Error(Bytes::from_static(b"ERR syntax error"));
    }

    let num_keys = args.len() / 2;
    if num_keys == 0 || args.len() % 2 != 0 {
        return RespValue::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'xread' command",
        ));
    }

    let keys = &args[..num_keys];
    let id_specs_bytes = &args[num_keys..];

    let resolved_ids: Vec<String> = id_specs_bytes
        .iter()
        .zip(keys.iter())
        .map(|(id_bytes, key)| {
            if &id_bytes[..] == b"$" {
                match store.db().get(key) {
                    Some(entry) if !store.is_expired(&entry) => {
                        if let DataType::Stream(s) = &entry.data {
                            s.last_id()
                                .map(|id| id.to_string())
                                .unwrap_or_else(|| "0-0".to_string())
                        } else {
                            "0-0".to_string()
                        }
                    }
                    _ => "0-0".to_string(),
                }
            } else {
                String::from_utf8_lossy(id_bytes).to_string()
            }
        })
        .collect();

    let keys_and_ids: Vec<(&Bytes, &str)> = keys
        .iter()
        .zip(resolved_ids.iter())
        .map(|(k, id)| (k, id.as_str()))
        .collect();

    let perform_read = |store: &Store, keys_and_ids: &[(&Bytes, &str)]| {
        let mut results = Vec::new();
        for (key, id_spec) in keys_and_ids {
            let stream_data = match store.db().get(*key) {
                Some(entry) if !store.is_expired(&entry) => match &entry.data {
                    DataType::Stream(stream) => match stream.read_from(id_spec) {
                        Ok(entries) if !entries.is_empty() => Some(((*key).clone(), entries)),
                        _ => None,
                    },
                    _ => return Err(crate::app::error::WRONGTYPE_ERROR),
                },
                _ => None,
            };
            if let Some(data) = stream_data {
                results.push(data);
            }
        }
        if results.is_empty() {
            Ok(None)
        } else {
            Ok(Some(results))
        }
    };

    if let Ok(Some(results)) = perform_read(store, &keys_and_ids) {
        return format_xread_response(results);
    } else if let Err(e) = perform_read(store, &keys_and_ids) {
        return RespValue::Error(Bytes::from(e.to_string()));
    }

    if block_timeout.is_none()
        && parsed
            .arg(0)
            .is_none_or(|a| !a.eq_ignore_ascii_case(b"block"))
    {
        return RespValue::NullBulkString;
    }

    waiters.wait_for_any(keys, block_timeout).await;

    match perform_read(store, &keys_and_ids) {
        Ok(Some(results)) => format_xread_response(results),
        Ok(None) => RespValue::NullArray,
        Err(e) => RespValue::Error(Bytes::from(e.to_string())),
    }
}

fn format_xread_response(results: XReadResult) -> RespValue {
    let result_array = results
        .into_iter()
        .map(|(key, entries)| {
            let entry_array = entries
                .into_iter()
                .map(|(id, fields)| {
                    let mut field_array = Vec::with_capacity(fields.len() * 2);
                    for (field, value) in fields {
                        field_array.push(RespValue::BulkString(field));
                        field_array.push(RespValue::BulkString(value));
                    }
                    RespValue::Array(vec![
                        RespValue::BulkString(Bytes::from(id.to_string())),
                        RespValue::Array(field_array),
                    ])
                })
                .collect();
            RespValue::Array(vec![
                RespValue::BulkString(key),
                RespValue::Array(entry_array),
            ])
        })
        .collect();
    RespValue::Array(result_array)
}
