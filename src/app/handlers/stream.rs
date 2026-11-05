use crate::app::command::Command;
use crate::app::{
    command::ParsedCommand,
    error::AppError,
    protocol::RespValue,
    store::{DataType, Store},
    stream::{Stream, StreamEntry, XReadResult},
    wait::WaiterRegistry,
};
use bytes::Bytes;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::{self, Instant};

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
    if fields_and_values.is_empty() || !fields_and_values.len().is_multiple_of(2) {
        return RespValue::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'xadd' command",
        ));
    }

    let fields: Vec<(Bytes, Bytes)> = fields_and_values
        .as_chunks::<2>()
        .0
        .iter()
        .map(|[field, value]| (field.clone(), value.clone()))
        .collect();

    let added = store.with_keyspace(|ks| match ks.get_as(key, DataType::as_stream)? {
        Some(stream) => stream.add(id_spec, fields),
        // Create the stream only once the entry is accepted, so a rejected ID leaves no key.
        None => {
            let mut stream = Stream::new();
            let id = stream.add(id_spec, fields)?;
            ks.set(key.clone(), DataType::Stream(stream), None);
            Ok(id)
        }
    });
    match added {
        Ok(id) => {
            waiters.stream_changed(key);
            RespValue::BulkString(Bytes::from(id.to_string()))
        }
        Err(e) => e.into(),
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

    store.with_keyspace(|ks| match ks.get_as(key, DataType::as_stream) {
        Ok(Some(stream)) => match stream.range(start_spec, end_spec) {
            Ok(entries) => format_entries(entries),
            Err(e) => e.into(),
        },
        Ok(None) => RespValue::Array(vec![]),
        Err(e) => e.into(),
    })
}

async fn handle_xread(
    parsed: ParsedCommand,
    store: &Arc<Store>,
    waiters: &Arc<WaiterRegistry>,
) -> RespValue {
    let mut args = parsed.args_from(0);
    let mut blocking = false;
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

        blocking = true;
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
    if num_keys == 0 || !args.len().is_multiple_of(2) {
        return RespValue::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'xread' command",
        ));
    }

    let keys = &args[..num_keys];
    let id_specs_bytes = &args[num_keys..];

    // Resolve `$` to each stream's current last ID before reading or blocking.
    let resolved_ids: Vec<String> = store.with_keyspace(|ks| {
        id_specs_bytes
            .iter()
            .zip(keys.iter())
            .map(|(id_bytes, key)| {
                if &id_bytes[..] == b"$" {
                    match ks.get_as(key, DataType::as_stream) {
                        Ok(Some(stream)) => stream
                            .last_id()
                            .map(|id| id.to_string())
                            .unwrap_or_else(|| "0-0".to_string()),
                        _ => "0-0".to_string(),
                    }
                } else {
                    String::from_utf8_lossy(id_bytes).to_string()
                }
            })
            .collect()
    });

    let read = || {
        store.with_keyspace(|ks| {
            let mut results: XReadResult = Vec::new();
            for (key, id_spec) in keys.iter().zip(&resolved_ids) {
                if let Some(stream) = ks.get_as(key, DataType::as_stream)? {
                    if let Ok(entries) = stream.read_from(id_spec) {
                        if !entries.is_empty() {
                            results.push((key.clone(), entries));
                        }
                    }
                }
            }
            Ok::<_, AppError>((!results.is_empty()).then_some(results))
        })
    };

    // Register before the first read, so an XADD that lands between the read
    // and the wait still wakes this client.
    let watch = blocking.then(|| waiters.watch_streams(keys));

    match read() {
        Ok(Some(results)) => return format_xread_response(results),
        Ok(None) => {}
        Err(e) => return e.into(),
    }

    let Some(watch) = watch else {
        return RespValue::NullArray;
    };
    // No deadline for BLOCK 0, or for one too far ahead to represent.
    let deadline = block_timeout.and_then(|timeout| Instant::now().checked_add(timeout));
    loop {
        match deadline {
            Some(deadline) => {
                if time::timeout_at(deadline, watch.changed.notified())
                    .await
                    .is_err()
                {
                    return RespValue::NullArray;
                }
            }
            None => watch.changed.notified().await,
        }
        match read() {
            Ok(Some(results)) => return format_xread_response(results),
            Ok(None) => {}
            Err(e) => return e.into(),
        }
    }
}

/// Formats stream entries as XRANGE replies them: `[id, [field, value, ...]]` each.
fn format_entries(entries: Vec<StreamEntry>) -> RespValue {
    RespValue::Array(
        entries
            .into_iter()
            .map(|(id, fields)| {
                let field_array = fields
                    .into_iter()
                    .flat_map(|(field, value)| {
                        [RespValue::BulkString(field), RespValue::BulkString(value)]
                    })
                    .collect();
                RespValue::Array(vec![
                    RespValue::BulkString(Bytes::from(id.to_string())),
                    RespValue::Array(field_array),
                ])
            })
            .collect(),
    )
}

fn format_xread_response(results: XReadResult) -> RespValue {
    RespValue::Array(
        results
            .into_iter()
            .map(|(key, entries)| {
                RespValue::Array(vec![RespValue::BulkString(key), format_entries(entries)])
            })
            .collect(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bulk(s: &str) -> RespValue {
        RespValue::BulkString(Bytes::copy_from_slice(s.as_bytes()))
    }

    fn command(parts: &[&str]) -> ParsedCommand {
        ParsedCommand::from_resp(RespValue::Array(parts.iter().map(|p| bulk(p)).collect()))
            .expect("valid command")
    }

    async fn run(parts: &[&str], store: &Arc<Store>) -> RespValue {
        handle(command(parts), store, &Arc::new(WaiterRegistry::new())).await
    }

    #[tokio::test]
    async fn xadd_stores_field_value_pairs_in_order() {
        let store = Arc::new(Store::new());
        assert_eq!(
            run(&["XADD", "s", "1-1", "a", "1", "b", "2"], &store).await,
            bulk("1-1")
        );
        assert_eq!(
            run(&["XRANGE", "s", "-", "+"], &store).await,
            RespValue::Array(vec![RespValue::Array(vec![
                bulk("1-1"),
                RespValue::Array(vec![bulk("a"), bulk("1"), bulk("b"), bulk("2")]),
            ])])
        );
    }

    #[tokio::test]
    async fn xadd_rejects_odd_field_value_count() {
        let store = Arc::new(Store::new());
        assert_eq!(
            run(&["XADD", "s", "1-1", "a", "1", "b"], &store).await,
            RespValue::Error(Bytes::from_static(
                b"ERR wrong number of arguments for 'xadd' command"
            ))
        );
        assert_eq!(
            run(&["XRANGE", "s", "-", "+"], &store).await,
            RespValue::Array(vec![])
        );
    }

    #[tokio::test]
    async fn xread_rejects_keys_without_matching_ids() {
        let store = Arc::new(Store::new());
        assert_eq!(
            run(&["XREAD", "STREAMS", "s1", "s2", "0-0"], &store).await,
            RespValue::Error(Bytes::from_static(
                b"ERR wrong number of arguments for 'xread' command"
            ))
        );
    }

    #[tokio::test]
    async fn xread_reads_every_requested_stream() {
        let store = Arc::new(Store::new());
        run(&["XADD", "s1", "1-1", "a", "1"], &store).await;
        run(&["XADD", "s2", "2-1", "b", "2"], &store).await;
        assert_eq!(
            run(&["XREAD", "STREAMS", "s1", "s2", "0-0", "0-0"], &store).await,
            RespValue::Array(vec![
                RespValue::Array(vec![
                    bulk("s1"),
                    RespValue::Array(vec![RespValue::Array(vec![
                        bulk("1-1"),
                        RespValue::Array(vec![bulk("a"), bulk("1")]),
                    ])]),
                ]),
                RespValue::Array(vec![
                    bulk("s2"),
                    RespValue::Array(vec![RespValue::Array(vec![
                        bulk("2-1"),
                        RespValue::Array(vec![bulk("b"), bulk("2")]),
                    ])]),
                ]),
            ])
        );
    }

    #[tokio::test]
    async fn xadd_wakes_every_blocked_reader() {
        let store = Arc::new(Store::new());
        let waiters = Arc::new(WaiterRegistry::new());
        let read = || {
            handle(
                command(&["XREAD", "BLOCK", "0", "STREAMS", "s", "$"]),
                &store,
                &waiters,
            )
        };
        let mut first = Box::pin(read());
        let mut second = Box::pin(read());
        assert!(futures::poll!(&mut first).is_pending());
        assert!(futures::poll!(&mut second).is_pending());

        handle(command(&["XADD", "s", "1-1", "a", "1"]), &store, &waiters).await;
        let expected = RespValue::Array(vec![RespValue::Array(vec![
            bulk("s"),
            RespValue::Array(vec![RespValue::Array(vec![
                bulk("1-1"),
                RespValue::Array(vec![bulk("a"), bulk("1")]),
            ])]),
        ])]);
        assert_eq!(first.await, expected);
        assert_eq!(second.await, expected);
    }

    #[tokio::test]
    async fn a_rejected_xadd_creates_no_key() {
        let store = Arc::new(Store::new());
        assert!(matches!(
            run(&["XADD", "s", "0-0", "a", "1"], &store).await,
            RespValue::Error(_)
        ));
        assert_eq!(
            store.with_keyspace(|ks| ks.type_name(&Bytes::from_static(b"s"))),
            "none"
        );
    }

    #[tokio::test]
    async fn xread_without_block_replies_null_array_when_empty() {
        let store = Arc::new(Store::new());
        run(&["XADD", "s", "1-1", "a", "1"], &store).await;
        assert_eq!(
            run(&["XREAD", "STREAMS", "s", "1-1"], &store).await,
            RespValue::NullArray
        );
    }
}
