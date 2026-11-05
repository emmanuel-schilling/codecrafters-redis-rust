use crate::app::error::AppError;
use bytes::{Buf, Bytes, BytesMut};
use tokio_util::codec::{Decoder, Encoder};

/// Represents any valid RESP value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RespValue {
    SimpleString(Bytes),
    Error(Bytes),
    Integer(i64),
    BulkString(Bytes),
    NullBulkString,
    Array(Vec<RespValue>),
    NullArray,
}

impl RespValue {
    /// Encodes the `RespValue` into a `Bytes` object using our Encoder implementation.
    pub fn encode_to_bytes(&self) -> Bytes {
        let mut buffer = BytesMut::new();
        let mut encoder = RespDecoder; // Our Encoder is part of the RespDecoder struct

        // The encode method should not fail for our well-defined types.
        // If it does, it indicates an internal logic error.
        if let Err(e) = encoder.encode(self.clone(), &mut buffer) {
            // For safety, return a RESP error string. In a real-world scenario, this would be logged.
            return Bytes::from(format!("-ERR Internal encoding error: {}\r\n", e));
        }

        buffer.freeze()
    }
}

//--- Decoder (Parser) ---

/// Longest bulk string accepted: Redis's default `proto-max-bulk-len` (512 MiB).
const MAX_BULK_LEN: usize = 512 * 1024 * 1024;
/// Longest line buffered while waiting for its CRLF: Redis's inline limit (64 KiB).
/// Stops a client that never sends CRLF from growing the buffer without bound.
const MAX_LINE_LEN: usize = 64 * 1024;
/// Most elements accepted in one array; Redis requires the count to fit in an `i32`.
const MAX_ARRAY_LEN: usize = i32::MAX as usize;
/// Deepest array nesting accepted, which bounds recursion on hostile input.
const MAX_DEPTH: usize = 32;
/// Array elements allocated up front. Larger arrays grow as their elements
/// arrive, so an untrusted length can't force a huge allocation.
const MAX_PREALLOC: usize = 1024;

/// A `tokio-util` codec for decoding RESP messages from a byte stream.
pub struct RespDecoder;

impl Decoder for RespDecoder {
    type Item = RespValue;
    type Error = AppError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let Some((value, len)) = parse_value(src, 0, 0)? else {
            return Ok(None);
        };
        src.advance(len);
        Ok(Some(value))
    }
}

/// Parses the RESP value that starts at `buf[pos]`.
///
/// Returns the value and the position just past it, or `None` when `buf` does
/// not yet hold the whole value; the codec retries once more bytes arrive.
fn parse_value(
    buf: &[u8],
    pos: usize,
    depth: usize,
) -> Result<Option<(RespValue, usize)>, AppError> {
    let Some(&type_byte) = buf.get(pos) else {
        return Ok(None);
    };
    let Some((line, mut pos)) = read_line(buf, pos + 1)? else {
        return Ok(None);
    };
    let value = match type_byte {
        b'+' => RespValue::SimpleString(Bytes::copy_from_slice(line)),
        b'-' => RespValue::Error(Bytes::copy_from_slice(line)),
        b':' => RespValue::Integer(parse_int(line)?),
        b'$' => match parse_len(line, MAX_BULK_LEN)? {
            None => RespValue::NullBulkString,
            Some(len) => {
                let end = pos + len;
                let Some(terminator) = buf.get(end..end + 2) else {
                    return Ok(None);
                };
                if terminator != b"\r\n" {
                    return Err(protocol_error("bulk string not terminated by CRLF"));
                }
                let value = RespValue::BulkString(Bytes::copy_from_slice(&buf[pos..end]));
                pos = end + 2;
                value
            }
        },
        b'*' => match parse_len(line, MAX_ARRAY_LEN)? {
            None => RespValue::NullArray,
            Some(_) if depth == MAX_DEPTH => {
                return Err(protocol_error("arrays nested too deeply"));
            }
            Some(len) => {
                let mut items = Vec::with_capacity(len.min(MAX_PREALLOC));
                for _ in 0..len {
                    let Some((item, next)) = parse_value(buf, pos, depth + 1)? else {
                        return Ok(None);
                    };
                    items.push(item);
                    pos = next;
                }
                RespValue::Array(items)
            }
        },
        other => {
            return Err(protocol_error(&format!(
                "unexpected type byte {:?}",
                char::from(other)
            )))
        }
    };
    Ok(Some((value, pos)))
}

/// Reads the line that starts at `buf[start]`. Returns it without its CRLF,
/// plus the position after the CRLF, or `None` if the CRLF hasn't arrived yet.
fn read_line(buf: &[u8], start: usize) -> Result<Option<(&[u8], usize)>, AppError> {
    let rest = &buf[start..];
    match rest.iter().position(|&b| b == b'\n') {
        Some(lf) if lf > 0 && rest[lf - 1] == b'\r' => Ok(Some((&rest[..lf - 1], start + lf + 1))),
        Some(_) => Err(protocol_error("line terminated by LF without CR")),
        None if rest.len() > MAX_LINE_LEN => Err(protocol_error("line too long")),
        None => Ok(None),
    }
}

fn parse_int(line: &[u8]) -> Result<i64, AppError> {
    std::str::from_utf8(line)
        .ok()
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| protocol_error("invalid integer"))
}

/// Parses a bulk-string or array length; RESP's null length, -1, gives `None`.
fn parse_len(line: &[u8], max: usize) -> Result<Option<usize>, AppError> {
    match parse_int(line)? {
        -1 => Ok(None),
        n => usize::try_from(n)
            .ok()
            .filter(|&n| n <= max)
            .map(Some)
            .ok_or_else(|| protocol_error("invalid length")),
    }
}

fn protocol_error(message: &str) -> AppError {
    AppError::ParseError(message.to_string())
}

//--- Encoder (Serializer) ---

impl Encoder<RespValue> for RespDecoder {
    type Error = AppError;

    fn encode(&mut self, item: RespValue, dst: &mut BytesMut) -> Result<(), Self::Error> {
        match item {
            RespValue::SimpleString(s) => {
                dst.extend_from_slice(b"+");
                dst.extend_from_slice(&s);
                dst.extend_from_slice(b"\r\n");
            }
            RespValue::Error(s) => {
                dst.extend_from_slice(b"-");
                dst.extend_from_slice(&s);
                dst.extend_from_slice(b"\r\n");
            }
            RespValue::Integer(i) => {
                dst.extend_from_slice(b":");
                dst.extend_from_slice(i.to_string().as_bytes());
                dst.extend_from_slice(b"\r\n");
            }
            RespValue::BulkString(s) => {
                dst.extend_from_slice(b"$");
                dst.extend_from_slice(s.len().to_string().as_bytes());
                dst.extend_from_slice(b"\r\n");
                dst.extend_from_slice(&s);
                dst.extend_from_slice(b"\r\n");
            }
            RespValue::NullBulkString => {
                dst.extend_from_slice(b"$-1\r\n");
            }
            RespValue::Array(items) => {
                dst.extend_from_slice(b"*");
                dst.extend_from_slice(items.len().to_string().as_bytes());
                dst.extend_from_slice(b"\r\n");
                for item in items {
                    // The encode method itself can return an error, so we propagate it.
                    self.encode(item, dst)?;
                }
            }
            RespValue::NullArray => {
                dst.extend_from_slice(b"*-1\r\n");
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn decode_all(bytes: &[u8]) -> Result<Option<RespValue>, AppError> {
        RespDecoder.decode(&mut BytesMut::from(bytes))
    }

    #[test]
    fn encodes_every_type_with_crlf_terminators() {
        let value = RespValue::Array(vec![
            RespValue::SimpleString(Bytes::from_static(b"OK")),
            RespValue::Error(Bytes::from_static(b"ERR x")),
            RespValue::Integer(-7),
            RespValue::BulkString(Bytes::from_static(b"hi")),
            RespValue::NullBulkString,
            RespValue::NullArray,
        ]);
        assert_eq!(
            &value.encode_to_bytes()[..],
            b"*6\r\n+OK\r\n-ERR x\r\n:-7\r\n$2\r\nhi\r\n$-1\r\n*-1\r\n"
        );
    }

    #[test]
    fn decodes_a_crlf_framed_command() {
        assert_eq!(
            decode_all(b"*2\r\n$4\r\nECHO\r\n$2\r\nhi\r\n").expect("decode"),
            Some(RespValue::Array(vec![
                RespValue::BulkString(Bytes::from_static(b"ECHO")),
                RespValue::BulkString(Bytes::from_static(b"hi")),
            ]))
        );
    }

    #[test]
    fn round_trips_through_encode_and_decode() {
        let value = RespValue::Array(vec![
            RespValue::SimpleString(Bytes::from_static(b"OK")),
            RespValue::Integer(-7),
            RespValue::BulkString(Bytes::from_static(b"a\r\nb")),
            RespValue::NullBulkString,
            RespValue::NullArray,
        ]);
        assert_eq!(
            decode_all(&value.encode_to_bytes()).expect("decode"),
            Some(value)
        );
    }

    #[test]
    fn rejects_lf_only_framing() {
        assert!(decode_all(b"+OK\n").is_err());
    }

    fn bulk(s: &[u8]) -> RespValue {
        RespValue::BulkString(Bytes::copy_from_slice(s))
    }

    #[test]
    fn waits_for_a_command_delivered_one_byte_at_a_time() {
        let frame = b"*2\r\n$4\r\nECHO\r\n$5\r\na\r\nbc\r\n";
        let mut buf = BytesMut::new();
        for (i, &byte) in frame.iter().enumerate() {
            buf.extend_from_slice(&[byte]);
            let decoded = RespDecoder.decode(&mut buf).expect("decode");
            if i + 1 < frame.len() {
                assert_eq!(decoded, None, "decoded early after {} bytes", i + 1);
            } else {
                assert_eq!(
                    decoded,
                    Some(RespValue::Array(vec![bulk(b"ECHO"), bulk(b"a\r\nbc")]))
                );
            }
        }
        assert!(buf.is_empty());
    }

    #[test]
    fn decodes_pipelined_commands_in_order_and_keeps_a_partial_tail() {
        let mut buf = BytesMut::from(
            &b"*1\r\n$4\r\nPING\r\n*2\r\n$4\r\nECHO\r\n$2\r\nhi\r\n*1\r\n$4\r\nPI"[..],
        );
        assert_eq!(
            RespDecoder.decode(&mut buf).expect("decode"),
            Some(RespValue::Array(vec![bulk(b"PING")]))
        );
        assert_eq!(
            RespDecoder.decode(&mut buf).expect("decode"),
            Some(RespValue::Array(vec![bulk(b"ECHO"), bulk(b"hi")]))
        );
        assert_eq!(RespDecoder.decode(&mut buf).expect("decode"), None);
        assert_eq!(&buf[..], b"*1\r\n$4\r\nPI");
    }

    #[test]
    fn decodes_scalars_and_nulls() {
        let cases: [(&[u8], RespValue); 7] = [
            (
                b"+OK\r\n",
                RespValue::SimpleString(Bytes::from_static(b"OK")),
            ),
            (b"+\r\n", RespValue::SimpleString(Bytes::new())),
            (
                b"-ERR bad\r\n",
                RespValue::Error(Bytes::from_static(b"ERR bad")),
            ),
            (b":-42\r\n", RespValue::Integer(-42)),
            (b"$0\r\n\r\n", bulk(b"")),
            (b"$-1\r\n", RespValue::NullBulkString),
            (b"*-1\r\n", RespValue::NullArray),
        ];
        for (input, expected) in cases {
            assert_eq!(decode_all(input).expect("decode"), Some(expected));
        }
    }

    #[test]
    fn rejects_malformed_frames() {
        let too_long = format!("${}\r\n", MAX_BULK_LEN + 1);
        let cases: [&[u8]; 8] = [
            b"$-2\r\n",
            b"*-5\r\n",
            b"$abc\r\n",
            b":12x\r\n",
            b"$2\r\nhiXY",
            b"?what\r\n",
            b"*1\r\n:1\n",
            too_long.as_bytes(),
        ];
        for input in cases {
            assert!(
                decode_all(input).is_err(),
                "accepted {:?}",
                String::from_utf8_lossy(input)
            );
        }
    }

    #[test]
    fn rejects_arrays_nested_deeper_than_the_limit() {
        let mut ok = b"*1\r\n".repeat(MAX_DEPTH);
        ok.extend_from_slice(b":1\r\n");
        assert!(decode_all(&ok).expect("decode").is_some());

        let mut too_deep = b"*1\r\n".repeat(MAX_DEPTH + 1);
        too_deep.extend_from_slice(b":1\r\n");
        assert!(decode_all(&too_deep).is_err());
    }

    #[test]
    fn rejects_an_unterminated_line_once_it_exceeds_the_limit() {
        let mut line = vec![b'+'];
        line.resize(MAX_LINE_LEN + 1, b'a');
        assert_eq!(decode_all(&line).expect("decode"), None);
        line.push(b'a');
        assert!(decode_all(&line).is_err());
    }

    #[test]
    fn waits_on_a_huge_declared_array_without_allocating_for_it() {
        assert_eq!(
            decode_all(format!("*{MAX_ARRAY_LEN}\r\n:1\r\n").as_bytes()).expect("decode"),
            None
        );
        assert!(decode_all(format!("*{}\r\n", MAX_ARRAY_LEN + 1).as_bytes()).is_err());
    }
}
