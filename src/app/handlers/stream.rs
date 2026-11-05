use crate::app::{
    command::{Command, ParsedCommand},
    error::AppError,
    handlers::Reply,
    protocol::RespValue,
    store::{DataType, Store},
    stream::{Stream, StreamEntry, XReadResult},
    wait::WaiterRegistry,
};
use bytes::Bytes;
use std::time::Duration;
use tokio::time::{self, Instant};

/// Argument `index` as UTF-8 text, or `invalid` as the error.
fn text<'a>(parsed: &'a ParsedCommand, index: usize, invalid: &str) -> Result<&'a str, AppError> {
    std::str::from_utf8(parsed.required(index)?).map_err(|_| AppError::ValueError(invalid.into()))
}

pub fn xadd(parsed: &ParsedCommand, store: &Store, waiters: &WaiterRegistry) -> Reply {
    let key = parsed.required(0)?;
    let id_spec = text(parsed, 1, "invalid stream ID specified in XADD")?;
    let fields_and_values = parsed.args_from(2);
    if fields_and_values.is_empty() || !fields_and_values.len().is_multiple_of(2) {
        return Err(AppError::WrongArity(Command::XAdd));
    }
    let fields: Vec<(Bytes, Bytes)> = fields_and_values
        .as_chunks::<2>()
        .0
        .iter()
        .map(|[field, value]| (field.clone(), value.clone()))
        .collect();

    let id = store.with_keyspace(|ks| match ks.get_as(key, DataType::as_stream)? {
        Some(stream) => stream.add(id_spec, fields),
        // Create the stream only once the entry is accepted, so a rejected ID leaves no key.
        None => {
            let mut stream = Stream::new();
            let id = stream.add(id_spec, fields)?;
            ks.set(key.clone(), DataType::Stream(stream), None);
            Ok(id)
        }
    })?;
    waiters.stream_changed(key);
    Ok(RespValue::BulkString(Bytes::from(id.to_string())))
}

pub fn xrange(parsed: &ParsedCommand, store: &Store) -> Reply {
    let key = parsed.required(0)?;
    parsed.required(2)?;
    let start_spec = text(parsed, 1, "invalid start ID for 'xrange' command")?;
    let end_spec = text(parsed, 2, "invalid end ID for 'xrange' command")?;

    store.with_keyspace(|ks| match ks.get_as(key, DataType::as_stream)? {
        Some(stream) => Ok(format_entries(stream.range(start_spec, end_spec)?)),
        None => Ok(RespValue::Array(vec![])),
    })
}

pub async fn xread(parsed: &ParsedCommand, store: &Store, waiters: &WaiterRegistry) -> Reply {
    let mut args = parsed.args_from(0);
    let mut blocking = false;
    let mut block_timeout = None;

    if args
        .first()
        .is_some_and(|a| a.eq_ignore_ascii_case(b"block"))
    {
        if args.len() < 3 {
            return Err(AppError::Syntax);
        }
        let timeout_ms: u64 = parsed.integer(1)?;
        blocking = true;
        block_timeout = (timeout_ms > 0).then(|| Duration::from_millis(timeout_ms));
        args = &args[2..];
    }

    if args
        .first()
        .is_some_and(|a| a.eq_ignore_ascii_case(b"streams"))
    {
        args = &args[1..];
    } else {
        return Err(AppError::Syntax);
    }

    let num_keys = args.len() / 2;
    if num_keys == 0 || !args.len().is_multiple_of(2) {
        return Err(AppError::WrongArity(Command::XRead));
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

    if let Some(results) = read()? {
        return Ok(format_xread_response(results));
    }
    let Some(watch) = watch else {
        return Ok(RespValue::NullArray);
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
                    return Ok(RespValue::NullArray);
                }
            }
            None => watch.changed.notified().await,
        }
        if let Some(results) = read()? {
            return Ok(format_xread_response(results));
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
    use crate::app::AppContext;
    use std::sync::Arc;

    fn bulk(s: &str) -> RespValue {
        RespValue::BulkString(Bytes::copy_from_slice(s.as_bytes()))
    }

    async fn run(parts: &[&str], ctx: &Arc<AppContext>) -> RespValue {
        crate::app::run_test(parts, ctx).await
    }

    #[tokio::test]
    async fn xadd_stores_field_value_pairs_in_order() {
        let ctx = crate::app::test_context();
        assert_eq!(
            run(&["XADD", "s", "1-1", "a", "1", "b", "2"], &ctx).await,
            bulk("1-1")
        );
        assert_eq!(
            run(&["XRANGE", "s", "-", "+"], &ctx).await,
            RespValue::Array(vec![RespValue::Array(vec![
                bulk("1-1"),
                RespValue::Array(vec![bulk("a"), bulk("1"), bulk("b"), bulk("2")]),
            ])])
        );
    }

    #[tokio::test]
    async fn xadd_rejects_odd_field_value_count() {
        let ctx = crate::app::test_context();
        assert_eq!(
            run(&["XADD", "s", "1-1", "a", "1", "b"], &ctx).await,
            RespValue::Error(Bytes::from_static(
                b"ERR wrong number of arguments for 'xadd' command"
            ))
        );
        assert_eq!(
            run(&["XRANGE", "s", "-", "+"], &ctx).await,
            RespValue::Array(vec![])
        );
    }

    #[tokio::test]
    async fn xread_rejects_keys_without_matching_ids() {
        let ctx = crate::app::test_context();
        assert_eq!(
            run(&["XREAD", "STREAMS", "s1", "s2", "0-0"], &ctx).await,
            RespValue::Error(Bytes::from_static(
                b"ERR wrong number of arguments for 'xread' command"
            ))
        );
    }

    #[tokio::test]
    async fn xread_reads_every_requested_stream() {
        let ctx = crate::app::test_context();
        run(&["XADD", "s1", "1-1", "a", "1"], &ctx).await;
        run(&["XADD", "s2", "2-1", "b", "2"], &ctx).await;
        assert_eq!(
            run(&["XREAD", "STREAMS", "s1", "s2", "0-0", "0-0"], &ctx).await,
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
        let ctx = crate::app::test_context();
        let read = ["XREAD", "BLOCK", "0", "STREAMS", "s", "$"];
        let mut first = Box::pin(run(&read, &ctx));
        let mut second = Box::pin(run(&read, &ctx));
        assert!(futures::poll!(&mut first).is_pending());
        assert!(futures::poll!(&mut second).is_pending());

        run(&["XADD", "s", "1-1", "a", "1"], &ctx).await;
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
        let ctx = crate::app::test_context();
        assert!(matches!(
            run(&["XADD", "s", "0-0", "a", "1"], &ctx).await,
            RespValue::Error(_)
        ));
        assert_eq!(
            ctx.store
                .with_keyspace(|ks| ks.type_name(&Bytes::from_static(b"s"))),
            "none"
        );
    }

    #[tokio::test]
    async fn xread_without_block_replies_null_array_when_empty() {
        let ctx = crate::app::test_context();
        run(&["XADD", "s", "1-1", "a", "1"], &ctx).await;
        assert_eq!(
            run(&["XREAD", "STREAMS", "s", "1-1"], &ctx).await,
            RespValue::NullArray
        );
    }
}
