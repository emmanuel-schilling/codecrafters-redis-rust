use crate::app::{
    command::{Command, ParsedCommand},
    error::AppError,
    exec::{self, Changes},
    handlers::Reply,
    protocol::RespValue,
    store::{DataType, Keyspace},
    stream::{Stream, StreamEntry, XReadResult},
    AppContext,
};
use bytes::Bytes;
use std::time::Duration;
use tokio::time::{self, Instant};

/// Argument `index` as UTF-8 text, or `invalid` as the error.
fn text<'a>(parsed: &'a ParsedCommand, index: usize, invalid: &str) -> Result<&'a str, AppError> {
    std::str::from_utf8(parsed.required(index)?).map_err(|_| AppError::ValueError(invalid.into()))
}

pub fn xadd(parsed: &ParsedCommand, ks: &mut Keyspace, changes: &mut Changes) -> Reply {
    let key = parsed.required(0)?;
    let id_spec = text(parsed, 1, "invalid stream ID specified in XADD")?;
    let fields_and_values = parsed.args_from(2);
    if !fields_and_values.len().is_multiple_of(2) {
        return Err(AppError::WrongArity(Command::XAdd));
    }
    let fields: Vec<(Bytes, Bytes)> = fields_and_values
        .as_chunks::<2>()
        .0
        .iter()
        .map(|[field, value]| (field.clone(), value.clone()))
        .collect();

    let id = match ks.get_as(key, DataType::as_stream)? {
        Some(stream) => stream.add(id_spec, fields)?,
        // Create the stream only once the entry is accepted, so a rejected ID leaves no key.
        None => {
            let mut stream = Stream::new();
            let id = stream.add(id_spec, fields)?;
            ks.set(key.clone(), DataType::Stream(stream), None);
            id
        }
    };
    let id = Bytes::from(id.to_string());
    changes.wrote(key);
    // An ID generated here (`*` or `<ms>-*`) must be the same on replicas.
    changes.replicate_as(vec![parsed
        .clone()
        .with_arg(1, id.clone())
        .into_resp_array()]);
    Ok(RespValue::BulkString(id))
}

pub fn xrange(parsed: &ParsedCommand, ks: &mut Keyspace) -> Reply {
    let key = parsed.required(0)?;
    let start_spec = text(parsed, 1, "invalid start ID for 'xrange' command")?;
    let end_spec = text(parsed, 2, "invalid end ID for 'xrange' command")?;

    match ks.get_as(key, DataType::as_stream)? {
        Some(stream) => Ok(format_entries(stream.range(start_spec, end_spec)?)),
        None => Ok(RespValue::Array(vec![])),
    }
}

/// XREAD's arguments.
struct XRead<'a> {
    /// With BLOCK, how long to wait: `Some(None)` waits forever.
    block: Option<Option<Duration>>,
    keys: &'a [Bytes],
    ids: &'a [Bytes],
}

/// Parses XREAD [BLOCK ms] STREAMS key [key ...] id [id ...].
fn xread_args(parsed: &ParsedCommand) -> Result<XRead<'_>, AppError> {
    let mut args = parsed.args_from(0);
    let mut block = None;

    if args
        .first()
        .is_some_and(|a| a.eq_ignore_ascii_case(b"block"))
    {
        if args.len() < 3 {
            return Err(AppError::Syntax);
        }
        let timeout_ms: u64 = parsed.integer(1)?;
        block = Some((timeout_ms > 0).then(|| Duration::from_millis(timeout_ms)));
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
    let (keys, ids) = args.split_at(num_keys);
    Ok(XRead { block, keys, ids })
}

/// The IDs to read after, with `$` resolved to each stream's current last ID.
fn resolve_ids(ks: &mut Keyspace, args: &XRead<'_>) -> Vec<String> {
    args.ids
        .iter()
        .zip(args.keys)
        .map(|(id, key)| {
            if &id[..] == b"$" {
                match ks.get_as(key, DataType::as_stream) {
                    Ok(Some(stream)) => stream
                        .last_id()
                        .map(|id| id.to_string())
                        .unwrap_or_else(|| "0-0".to_string()),
                    _ => "0-0".to_string(),
                }
            } else {
                String::from_utf8_lossy(id).to_string()
            }
        })
        .collect()
}

/// The entries after `ids` in each stream, or `None` if there are none.
fn read(ks: &mut Keyspace, keys: &[Bytes], ids: &[String]) -> Result<Option<RespValue>, AppError> {
    let mut results: XReadResult = Vec::new();
    for (key, id_spec) in keys.iter().zip(ids) {
        if let Some(stream) = ks.get_as(key, DataType::as_stream)? {
            if let Ok(entries) = stream.read_from(id_spec) {
                if !entries.is_empty() {
                    results.push((key.clone(), entries));
                }
            }
        }
    }
    Ok((!results.is_empty()).then(|| format_xread_response(results)))
}

/// XREAD without waiting, as inside a transaction: BLOCK is ignored, and
/// with no new entries the reply is a null array.
pub fn xread_now(parsed: &ParsedCommand, ks: &mut Keyspace) -> Reply {
    let args = xread_args(parsed)?;
    let ids = resolve_ids(ks, &args);
    Ok(read(ks, args.keys, &ids)?.unwrap_or(RespValue::NullArray))
}

/// XREAD [BLOCK ms] STREAMS ...: with BLOCK, waits for new entries until the
/// timeout (0 waits forever).
pub async fn xread(parsed: &ParsedCommand, ctx: &AppContext) -> RespValue {
    let args = match xread_args(parsed) {
        Ok(args) => args,
        Err(e) => return e.into(),
    };
    let Some(timeout) = args.block else {
        return exec::locked(ctx, |ks, outbox| exec::run(ctx, ks, parsed, outbox));
    };

    // Resolve `$`, read and register in one locked section: every XADD runs
    // locked too, and wakes registered readers after adding.
    let first = exec::locked(ctx, |ks, _| {
        let ids = resolve_ids(ks, &args);
        match read(ks, args.keys, &ids) {
            Ok(None) => Err((ctx.waiters.watch_streams(args.keys), ids)),
            Ok(Some(reply)) => Ok(reply),
            Err(e) => Ok(e.into()),
        }
    });
    let (watch, ids) = match first {
        Ok(reply) => return reply,
        Err(waiting) => waiting,
    };

    // No deadline for BLOCK 0, or for one too far ahead to represent.
    let deadline = timeout.and_then(|timeout| Instant::now().checked_add(timeout));
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
        match exec::locked(ctx, |ks, _| read(ks, args.keys, &ids)) {
            Ok(Some(reply)) => return reply,
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
