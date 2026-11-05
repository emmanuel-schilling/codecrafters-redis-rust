use crate::app::{
    command::ParsedCommand,
    handlers::Reply,
    protocol::RespValue,
    sorted_set::SortedSet,
    store::{resolve_range, DataType, Store},
};
use bytes::Bytes;

pub fn zadd(parsed: &ParsedCommand, store: &Store) -> Reply {
    let key = parsed.required(0)?;
    let member = parsed.required(2)?;
    let score = parsed.float(1)?;
    store.with_keyspace(|ks| {
        let zset = ks.get_or_create_as(key, DataType::as_sorted_set, || {
            DataType::SortedSet(SortedSet::new())
        })?;
        Ok(RespValue::Integer(zset.add(score, member.clone()) as i64))
    })
}

pub fn zcard(parsed: &ParsedCommand, store: &Store) -> Reply {
    let key = parsed.required(0)?;
    store.with_keyspace(|ks| {
        let count = ks
            .get_as(key, DataType::as_sorted_set)?
            .map_or(0, |zset| zset.cardinality());
        Ok(RespValue::Integer(count as i64))
    })
}

pub fn zscore(parsed: &ParsedCommand, store: &Store) -> Reply {
    let key = parsed.required(0)?;
    let member = parsed.required(1)?;
    store.with_keyspace(|ks| {
        Ok(
            match ks
                .get_as(key, DataType::as_sorted_set)?
                .and_then(|zset| zset.score_of(member))
            {
                Some(score) => RespValue::BulkString(Bytes::from(score.to_string())),
                None => RespValue::NullBulkString,
            },
        )
    })
}

pub fn zrank(parsed: &ParsedCommand, store: &Store) -> Reply {
    let key = parsed.required(0)?;
    let member = parsed.required(1)?;
    store.with_keyspace(|ks| {
        Ok(
            match ks
                .get_as(key, DataType::as_sorted_set)?
                .and_then(|zset| zset.rank_of(member))
            {
                Some(rank) => RespValue::Integer(rank as i64),
                None => RespValue::NullBulkString,
            },
        )
    })
}

pub fn zrange(parsed: &ParsedCommand, store: &Store) -> Reply {
    let key = parsed.required(0)?;
    parsed.required(2)?;
    let start: i64 = parsed.integer(1)?;
    let stop: i64 = parsed.integer(2)?;
    store.with_keyspace(|ks| {
        let members = match ks.get_as(key, DataType::as_sorted_set)? {
            Some(zset) => resolve_range(start, stop, zset.cardinality())
                .map(|(first, last)| zset.get_range(first, last))
                .unwrap_or_default(),
            None => vec![],
        };
        Ok(RespValue::Array(
            members.into_iter().map(RespValue::BulkString).collect(),
        ))
    })
}

pub fn zrem(parsed: &ParsedCommand, store: &Store) -> Reply {
    let key = parsed.required(0)?;
    let member = parsed.required(1)?;
    store.with_keyspace(|ks| {
        let removed = match ks.get_as(key, DataType::as_sorted_set)? {
            Some(zset) => zset.remove(member),
            None => 0,
        };
        ks.remove_if_empty(key);
        Ok(RespValue::Integer(removed as i64))
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

    fn bulks(items: &[&str]) -> RespValue {
        RespValue::Array(
            items
                .iter()
                .map(|s| RespValue::BulkString(Bytes::copy_from_slice(s.as_bytes())))
                .collect(),
        )
    }

    #[tokio::test]
    async fn zrange_resolves_negative_and_out_of_range_indexes() {
        let ctx = crate::app::test_context();
        for (score, member) in [("1", "a"), ("2", "b"), ("3", "c")] {
            run(&["ZADD", "z", score, member], &ctx).await;
        }
        assert_eq!(
            run(&["ZRANGE", "z", "0", "-1"], &ctx).await,
            bulks(&["a", "b", "c"])
        );
        assert_eq!(
            run(&["ZRANGE", "z", "-2", "10"], &ctx).await,
            bulks(&["b", "c"])
        );
        assert_eq!(run(&["ZRANGE", "z", "0", "-100"], &ctx).await, bulks(&[]));
    }

    #[tokio::test]
    async fn removing_the_last_member_deletes_the_set() {
        let ctx = crate::app::test_context();
        run(&["ZADD", "z", "1", "a"], &ctx).await;
        assert_eq!(run(&["ZREM", "z", "a"], &ctx).await, RespValue::Integer(1));
        assert_eq!(run(&["ZCARD", "z"], &ctx).await, RespValue::Integer(0));
        assert_eq!(
            ctx.store
                .with_keyspace(|ks| ks.type_name(&Bytes::from_static(b"z"))),
            "none"
        );
    }

    #[tokio::test]
    async fn reports_wrongtype_for_a_key_of_another_type() {
        let ctx = crate::app::test_context();
        ctx.store.with_keyspace(|ks| {
            ks.set(
                Bytes::from_static(b"s"),
                DataType::String(Bytes::from_static(b"x")),
                None,
            )
        });
        assert_eq!(
            run(&["ZADD", "s", "1", "a"], &ctx).await,
            RespValue::Error(Bytes::from_static(
                b"WRONGTYPE Operation against a key holding the wrong kind of value"
            ))
        );
    }
}
