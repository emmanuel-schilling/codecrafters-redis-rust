//! Bitmaps: string values read and written one bit at a time. Bit 0 is the
//! most significant bit of the first byte, as in Redis.

use crate::app::{
    command::{parse, ParsedCommand},
    error::AppError,
    handlers::Reply,
    protocol::RespValue,
    store::{resolve_range, DataType, Store},
};
use bytes::{Bytes, BytesMut};

/// The largest bit offset, as in Redis: bitmaps are limited to 512 MiB.
const MAX_BIT_OFFSET: u64 = (1 << 32) - 1;

/// Argument `index` as a bit offset.
fn bit_offset(parsed: &ParsedCommand, index: usize) -> Result<usize, AppError> {
    parse::<u64>(parsed.required(index)?)
        .filter(|&offset| offset <= MAX_BIT_OFFSET)
        .and_then(|offset| usize::try_from(offset).ok())
        .ok_or_else(|| AppError::ValueError("bit offset is not an integer or out of range".into()))
}

/// The bit at `offset`; bits past the end of `bytes` are 0.
fn bit_at(bytes: &[u8], offset: usize) -> bool {
    bytes
        .get(offset / 8)
        .is_some_and(|byte| byte & mask(offset) != 0)
}

/// The mask that selects bit `offset` within its byte.
fn mask(offset: usize) -> u8 {
    0x80 >> (offset % 8)
}

/// SETBIT key offset 0|1: sets or clears a bit, growing the string with zero
/// bytes as needed, and replies with the bit's previous value.
pub fn setbit(parsed: &ParsedCommand, store: &Store) -> Reply {
    let key = parsed.required(0)?;
    let bit = match &parsed.required(2)?[..] {
        b"0" => false,
        b"1" => true,
        _ => {
            return Err(AppError::ValueError(
                "bit is not an integer or out of range".into(),
            ))
        }
    };
    let offset = bit_offset(parsed, 1)?;

    store.with_keyspace(|ks| {
        let value =
            ks.get_or_create_as(key, DataType::as_string, || DataType::String(Bytes::new()))?;
        // Reuses the buffer unless the value is shared (e.g. by a reply still
        // being sent), in which case it's copied.
        let mut bytes = std::mem::take(value)
            .try_into_mut()
            .unwrap_or_else(|shared| BytesMut::from(&shared[..]));
        let index = offset / 8;
        if bytes.len() <= index {
            bytes.resize(index + 1, 0);
        }
        let previous = bit_at(&bytes, offset);
        if bit {
            bytes[index] |= mask(offset);
        } else {
            bytes[index] &= !mask(offset);
        }
        *value = bytes.freeze();
        Ok(RespValue::Integer(i64::from(previous)))
    })
}

/// GETBIT key offset: the bit at `offset`; 0 for a missing key or an offset
/// past the end of the string.
pub fn getbit(parsed: &ParsedCommand, store: &Store) -> Reply {
    let key = parsed.required(0)?;
    let offset = bit_offset(parsed, 1)?;
    store.with_keyspace(|ks| {
        let bit = ks
            .get_as(key, DataType::as_string)?
            .is_some_and(|value| bit_at(value, offset));
        Ok(RespValue::Integer(i64::from(bit)))
    })
}

/// BITCOUNT key [start end]: the number of set bits, in the whole string or
/// in the inclusive byte range `start..=end`. Indexes follow Redis's range
/// rules: an end past the string is clamped, and an empty range counts 0.
pub fn bitcount(parsed: &ParsedCommand, store: &Store) -> Reply {
    let key = parsed.required(0)?;
    let range = match parsed.args_from(1).len() {
        0 => None,
        2 => Some((parsed.integer::<i64>(1)?, parsed.integer::<i64>(2)?)),
        // A start without an end, or the BYTE/BIT option, which isn't supported.
        _ => return Err(AppError::Syntax),
    };
    store.with_keyspace(|ks| {
        let Some(value) = ks.get_as(key, DataType::as_string)? else {
            return Ok(RespValue::Integer(0));
        };
        let bytes = match range {
            None => &value[..],
            Some((start, end)) => match resolve_range(start, end, value.len()) {
                Some((first, last)) => &value[first..=last],
                None => &[],
            },
        };
        let count: u64 = bytes.iter().map(|byte| u64::from(byte.count_ones())).sum();
        Ok(RespValue::Integer(count as i64))
    })
}

#[cfg(test)]
mod tests {
    use crate::app::{run_test as run, test_context, RespValue};
    use bytes::Bytes;

    fn int(n: i64) -> RespValue {
        RespValue::Integer(n)
    }

    fn error(message: &'static str) -> RespValue {
        RespValue::Error(Bytes::from_static(message.as_bytes()))
    }

    #[tokio::test]
    async fn setbit_creates_a_string_with_bit_0_as_the_most_significant_bit() {
        let ctx = test_context();
        assert_eq!(run(&["SETBIT", "k", "1", "1"], &ctx).await, int(0));
        assert_eq!(
            run(&["GET", "k"], &ctx).await,
            RespValue::BulkString(Bytes::from_static(&[0b0100_0000]))
        );
        assert_eq!(run(&["SETBIT", "k", "9", "1"], &ctx).await, int(0));
        assert_eq!(
            run(&["GET", "k"], &ctx).await,
            RespValue::BulkString(Bytes::from_static(&[0b0100_0000, 0b0100_0000]))
        );
    }

    #[tokio::test]
    async fn setbit_replies_with_the_previous_bit() {
        let ctx = test_context();
        assert_eq!(run(&["SETBIT", "k", "3", "1"], &ctx).await, int(0));
        assert_eq!(run(&["SETBIT", "k", "3", "1"], &ctx).await, int(1));
        assert_eq!(run(&["SETBIT", "k", "3", "0"], &ctx).await, int(1));
        assert_eq!(run(&["GETBIT", "k", "3"], &ctx).await, int(0));
        assert_eq!(
            run(&["GET", "k"], &ctx).await,
            RespValue::BulkString(Bytes::from_static(&[0]))
        );
    }

    #[tokio::test]
    async fn getbit_reads_set_unset_out_of_range_and_missing_bits() {
        let ctx = test_context();
        run(&["SETBIT", "bit_key", "2", "1"], &ctx).await;
        assert_eq!(run(&["GETBIT", "bit_key", "2"], &ctx).await, int(1));
        assert_eq!(run(&["GETBIT", "bit_key", "3"], &ctx).await, int(0));
        assert_eq!(run(&["GETBIT", "bit_key", "1000"], &ctx).await, int(0));
        assert_eq!(run(&["GETBIT", "new_key", "2"], &ctx).await, int(0));
    }

    #[tokio::test]
    async fn bits_of_a_string_set_with_set_are_readable() {
        let ctx = test_context();
        run(&["SET", "s", "a"], &ctx).await; // 'a' is 0b0110_0001
        let mut bits = Vec::new();
        for offset in 0..8 {
            bits.push(run(&["GETBIT", "s", &offset.to_string()], &ctx).await);
        }
        assert_eq!(bits, [0, 1, 1, 0, 0, 0, 0, 1].map(int));
        assert_eq!(run(&["SETBIT", "s", "6", "1"], &ctx).await, int(0));
        assert_eq!(
            run(&["GET", "s"], &ctx).await,
            RespValue::BulkString(Bytes::from_static(b"c"))
        );
    }

    #[tokio::test]
    async fn rejects_bad_arguments_and_other_types() {
        let ctx = test_context();
        run(&["RPUSH", "list", "x"], &ctx).await;
        let bad_offset = error("ERR bit offset is not an integer or out of range");
        let cases: [(&[&str], RespValue); 7] = [
            (&["SETBIT", "k", "-1", "1"], bad_offset.clone()),
            (&["SETBIT", "k", "4294967296", "1"], bad_offset.clone()),
            (&["GETBIT", "k", "x"], bad_offset),
            (
                &["SETBIT", "k", "0", "2"],
                error("ERR bit is not an integer or out of range"),
            ),
            (
                &["SETBIT", "k", "0"],
                error("ERR wrong number of arguments for 'setbit' command"),
            ),
            (
                &["SETBIT", "list", "0", "1"],
                error("WRONGTYPE Operation against a key holding the wrong kind of value"),
            ),
            (
                &["GETBIT", "list", "0"],
                error("WRONGTYPE Operation against a key holding the wrong kind of value"),
            ),
        ];
        for (parts, expected) in cases {
            assert_eq!(run(parts, &ctx).await, expected, "{parts:?}");
        }
        assert_eq!(run(&["GET", "k"], &ctx).await, RespValue::NullBulkString);
    }

    #[tokio::test]
    async fn setbit_grows_the_string_and_strlen_counts_bytes() {
        let ctx = test_context();
        assert_eq!(run(&["STRLEN", "bitmap_key"], &ctx).await, int(0));
        run(&["SETBIT", "bitmap_key", "1", "1"], &ctx).await;
        assert_eq!(run(&["STRLEN", "bitmap_key"], &ctx).await, int(1));
        assert_eq!(
            run(&["SETBIT", "bitmap_key", "10", "1"], &ctx).await,
            int(0)
        );
        assert_eq!(run(&["GETBIT", "bitmap_key", "10"], &ctx).await, int(1));
        assert_eq!(run(&["STRLEN", "bitmap_key"], &ctx).await, int(2));
        assert_eq!(
            run(&["GET", "bitmap_key"], &ctx).await,
            RespValue::BulkString(Bytes::from_static(&[0b0100_0000, 0b0010_0000]))
        );
    }

    #[tokio::test]
    async fn bitcount_counts_the_whole_string_or_a_byte_range() {
        let ctx = test_context();
        run(&["SETBIT", "bitmap_key", "1", "1"], &ctx).await;
        run(&["SETBIT", "bitmap_key", "10", "1"], &ctx).await;
        let cases: [(&[&str], i64); 9] = [
            (&[], 2),
            (&["0", "1"], 2),
            (&["0", "0"], 1),
            (&["1", "1"], 1),
            (&["0", "100"], 2),
            (&["2", "5"], 0),
            (&["1", "0"], 0),
            (&["-1", "-1"], 1),
            (&["-100", "0"], 1),
        ];
        for (range, expected) in cases {
            let mut parts = vec!["BITCOUNT", "bitmap_key"];
            parts.extend_from_slice(range);
            assert_eq!(run(&parts, &ctx).await, int(expected), "{parts:?}");
        }
        assert_eq!(run(&["BITCOUNT", "missing"], &ctx).await, int(0));
        assert_eq!(run(&["BITCOUNT", "missing", "0", "5"], &ctx).await, int(0));
    }

    #[tokio::test]
    async fn bitcount_and_strlen_reject_bad_arguments_and_other_types() {
        let ctx = test_context();
        run(&["SET", "s", "a"], &ctx).await;
        run(&["RPUSH", "list", "x"], &ctx).await;
        let wrongtype = error("WRONGTYPE Operation against a key holding the wrong kind of value");
        let cases: [(&[&str], RespValue); 6] = [
            (&["BITCOUNT", "s", "0"], error("ERR syntax error")),
            (
                &["BITCOUNT", "s", "0", "1", "BIT"],
                error("ERR syntax error"),
            ),
            (
                &["BITCOUNT", "s", "x", "1"],
                error("ERR value is not an integer or out of range"),
            ),
            (&["BITCOUNT", "list"], wrongtype.clone()),
            (&["STRLEN", "list"], wrongtype),
            (
                &["STRLEN"],
                error("ERR wrong number of arguments for 'strlen' command"),
            ),
        ];
        for (parts, expected) in cases {
            assert_eq!(run(parts, &ctx).await, expected, "{parts:?}");
        }
        assert_eq!(run(&["BITCOUNT", "s"], &ctx).await, int(3)); // 'a' is 0b0110_0001
    }
}
