use crate::app::{
    command::{Command, ParsedCommand},
    protocol::RespValue,
    sorted_set::SortedSet,
    store::{resolve_range, DataType, Store},
};
use bytes::Bytes;
use std::sync::Arc;

pub fn handle(parsed: ParsedCommand, store: &Arc<Store>) -> RespValue {
    match parsed.command() {
        Command::ZAdd => handle_zadd(parsed, store),
        Command::ZCard => handle_zcard(parsed, store),
        Command::ZScore => handle_zscore(parsed, store),
        Command::ZRank => handle_zrank(parsed, store),
        Command::ZRange => handle_zrange(parsed, store),
        Command::ZRem => handle_zrem(parsed, store),
        _ => RespValue::Error(Bytes::from_static(b"ERR unknown sorted set command")),
    }
}

fn handle_zadd(parsed: ParsedCommand, store: &Store) -> RespValue {
    let (Some(key), Some(score_bytes), Some(member)) =
        (parsed.arg(0), parsed.arg(1), parsed.arg(2))
    else {
        return RespValue::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'zadd' command",
        ));
    };

    let Some(score) = std::str::from_utf8(score_bytes)
        .ok()
        .and_then(|s| s.parse::<f64>().ok())
    else {
        return RespValue::Error(Bytes::from_static(b"ERR value is not a valid float"));
    };

    store.with_keyspace(|ks| {
        match ks.get_or_create_as(key, DataType::as_sorted_set, || {
            DataType::SortedSet(SortedSet::new())
        }) {
            Ok(zset) => RespValue::Integer(zset.add(score, member.clone()) as i64),
            Err(e) => e.into(),
        }
    })
}

fn handle_zcard(parsed: ParsedCommand, store: &Store) -> RespValue {
    let Some(key) = parsed.first() else {
        return RespValue::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'zcard' command",
        ));
    };

    store.with_keyspace(|ks| match ks.get_as(key, DataType::as_sorted_set) {
        Ok(Some(zset)) => RespValue::Integer(zset.cardinality() as i64),
        Ok(None) => RespValue::Integer(0),
        Err(e) => e.into(),
    })
}

fn handle_zscore(parsed: ParsedCommand, store: &Store) -> RespValue {
    let (Some(key), Some(member)) = (parsed.arg(0), parsed.arg(1)) else {
        return RespValue::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'zscore' command",
        ));
    };

    store.with_keyspace(|ks| match ks.get_as(key, DataType::as_sorted_set) {
        Ok(zset) => match zset.and_then(|zset| zset.score_of(member)) {
            Some(score) => RespValue::BulkString(Bytes::from(score.to_string())),
            None => RespValue::NullBulkString,
        },
        Err(e) => e.into(),
    })
}

fn handle_zrank(parsed: ParsedCommand, store: &Store) -> RespValue {
    let (Some(key), Some(member)) = (parsed.arg(0), parsed.arg(1)) else {
        return RespValue::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'zrank' command",
        ));
    };

    store.with_keyspace(|ks| match ks.get_as(key, DataType::as_sorted_set) {
        Ok(zset) => match zset.and_then(|zset| zset.rank_of(member)) {
            Some(rank) => RespValue::Integer(rank as i64),
            None => RespValue::NullBulkString,
        },
        Err(e) => e.into(),
    })
}

fn handle_zrange(parsed: ParsedCommand, store: &Store) -> RespValue {
    let (Some(key), Some(start_bytes), Some(stop_bytes)) =
        (parsed.arg(0), parsed.arg(1), parsed.arg(2))
    else {
        return RespValue::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'zrange' command",
        ));
    };

    let parse = |bytes: &Bytes| {
        std::str::from_utf8(bytes)
            .ok()
            .and_then(|s| s.parse::<i64>().ok())
    };
    let (Some(start), Some(stop)) = (parse(start_bytes), parse(stop_bytes)) else {
        return RespValue::Error(Bytes::from_static(
            b"ERR value is not an integer or out of range",
        ));
    };

    store.with_keyspace(|ks| match ks.get_as(key, DataType::as_sorted_set) {
        Ok(Some(zset)) => {
            let members = resolve_range(start, stop, zset.cardinality())
                .map(|(first, last)| zset.get_range(first, last))
                .unwrap_or_default();
            RespValue::Array(members.into_iter().map(RespValue::BulkString).collect())
        }
        Ok(None) => RespValue::Array(vec![]),
        Err(e) => e.into(),
    })
}

fn handle_zrem(parsed: ParsedCommand, store: &Store) -> RespValue {
    let (Some(key), Some(member)) = (parsed.arg(0), parsed.arg(1)) else {
        return RespValue::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'zrem' command",
        ));
    };

    store.with_keyspace(|ks| match ks.get_as(key, DataType::as_sorted_set) {
        Ok(Some(zset)) => {
            let removed = zset.remove(member);
            ks.remove_if_empty(key);
            RespValue::Integer(removed as i64)
        }
        Ok(None) => RespValue::Integer(0),
        Err(e) => e.into(),
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

    fn bulks(items: &[&str]) -> RespValue {
        RespValue::Array(
            items
                .iter()
                .map(|s| RespValue::BulkString(Bytes::copy_from_slice(s.as_bytes())))
                .collect(),
        )
    }

    #[test]
    fn zrange_resolves_negative_and_out_of_range_indexes() {
        let store = Arc::new(Store::new());
        for (score, member) in [("1", "a"), ("2", "b"), ("3", "c")] {
            run(&["ZADD", "z", score, member], &store);
        }
        assert_eq!(
            run(&["ZRANGE", "z", "0", "-1"], &store),
            bulks(&["a", "b", "c"])
        );
        assert_eq!(
            run(&["ZRANGE", "z", "-2", "10"], &store),
            bulks(&["b", "c"])
        );
        assert_eq!(run(&["ZRANGE", "z", "0", "-100"], &store), bulks(&[]));
    }

    #[test]
    fn removing_the_last_member_deletes_the_set() {
        let store = Arc::new(Store::new());
        run(&["ZADD", "z", "1", "a"], &store);
        assert_eq!(run(&["ZREM", "z", "a"], &store), RespValue::Integer(1));
        assert_eq!(run(&["ZCARD", "z"], &store), RespValue::Integer(0));
        assert_eq!(
            store.with_keyspace(|ks| ks.type_name(&Bytes::from_static(b"z"))),
            "none"
        );
    }

    #[test]
    fn reports_wrongtype_for_a_key_of_another_type() {
        let store = Arc::new(Store::new());
        store.with_keyspace(|ks| {
            ks.set(
                Bytes::from_static(b"s"),
                DataType::String(Bytes::from_static(b"x")),
                None,
            )
        });
        assert_eq!(
            run(&["ZADD", "s", "1", "a"], &store),
            RespValue::Error(Bytes::from_static(
                b"WRONGTYPE Operation against a key holding the wrong kind of value"
            ))
        );
    }
}
