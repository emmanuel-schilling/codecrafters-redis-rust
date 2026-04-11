use crate::app::error::AppError;
use bytes::{Buf, Bytes, BytesMut};
use nom::{
    branch::alt,
    bytes::complete::{tag, take, take_until},
    character::complete::{crlf, digit1},
    combinator::{map, map_res, opt, recognize},
    IResult,
};
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
            return Bytes::from(format!("-ERR Internal encoding error: {}
", e));
        }

        buffer.freeze()
    }
}

//--- Decoder (Parser) ---

/// A `tokio-util` codec for decoding RESP messages from a byte stream.
pub struct RespDecoder;

impl Decoder for RespDecoder {
    type Item = RespValue;
    type Error = AppError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.is_empty() {
            return Ok(None);
        }
        // parse_message returns Result<(&[u8], RespValue), nom::Err<...>>
        // We need to convert nom::Err to AppError.
        match parse_message(src.as_ref()) {
            Ok((remaining, value)) => {
                let consumed = src.len() - remaining.len();
                src.advance(consumed);
                Ok(Some(value))
            }
            Err(nom::Err::Incomplete(_)) => Ok(None), // Not enough data, wait for more.
            Err(e) => {
                // Convert nom error to AppError::ParseError
                Err(AppError::ParseError(e.to_string()))
            }
        }
    }
}

/// Parses a full RESP message using `nom`.
/// Returns a Result containing the remaining input and the parsed value, or a nom error.
fn parse_message(input: &[u8]) -> Result<(&[u8], RespValue), nom::Err<nom::error::Error<&[u8]>>> {
    alt((
        parse_simple_string,
        parse_error,
        parse_integer,
        parse_bulk_string,
        parse_array,
    ))(input)
}

fn parse_simple_string(input: &[u8]) -> IResult<&[u8], RespValue> {
    map(
        nom::sequence::preceded(
            tag("+"),
            nom::sequence::terminated(take_until("
"), crlf),
        ),
        |s: &[u8]| RespValue::SimpleString(Bytes::copy_from_slice(s)),
    )(input)
}

fn parse_error(input: &[u8]) -> IResult<&[u8], RespValue> {
    map(
        nom::sequence::preceded(
            tag("-"),
            nom::sequence::terminated(take_until("
"), crlf),
        ),
        |s: &[u8]| RespValue::Error(Bytes::copy_from_slice(s)),
    )(input)
}

fn parse_integer(input: &[u8]) -> IResult<&[u8], RespValue> {
    map(
        nom::sequence::preceded(
            tag(":"),
            nom::sequence::terminated(
                // Use map_res to handle potential UTF-8 conversion or parse errors from i64
                map_res(
                    recognize(nom::sequence::pair(opt(tag("-")), digit1)),
                    |s: &[u8]| {
                        std::str::from_utf8(s)
                            .map_err(|_| ()) // Convert UTF-8 error to nom error type
                            .and_then(|s| s.parse::<i64>().map_err(|_| ())) // Convert parse error to nom error type
                    },
                ),
                crlf,
            ),
        ),
        RespValue::Integer,
    )(input)
}

/// Safely parses a string slice into an i64, returning a nom error on failure.
/// This function now consistently returns a nom error type for all failures.
fn parse_i64(input: &[u8]) -> Result<i64, nom::Err<nom::error::Error<&[u8]>>> {
    std::str::from_utf8(input)
        .map_err(|_| nom::Err::Failure(nom::error::Error::new(input, nom::error::ErrorKind::Char))) // Error if not valid UTF-8
        .and_then(|s| {
            s.parse::<i64>().map_err(|_| {
                nom::Err::Failure(nom::error::Error::new(input, nom::error::ErrorKind::Digit)) // Error if parsing to i64 fails
            })
        })
}

fn parse_bulk_string(input: &[u8]) -> IResult<&[u8], RespValue> {
    let (input, len_str) = nom::sequence::preceded(
        tag("$"),
        nom::sequence::terminated(take_until("
"), crlf),
    )(input)?;

    // parse_i64 now returns Result, so we need to handle its potential error.
    let len = parse_i64(len_str)?;

    if len == -1 {
        return Ok((input, RespValue::NullBulkString));
    }

    if len < 0 {
        // This case should ideally be caught by parse_i64 returning an error,
        // but as a safeguard, we ensure len is non-negative.
        return Err(nom::Err::Failure(nom::error::Error::new(
            len_str,
            nom::error::ErrorKind::Digit,
        )));
    }

    // The `take` and `crlf` are expected to succeed if len is valid.
    let (input, content) = nom::sequence::terminated(take(len as usize), crlf)(input)?;
    Ok((
        input,
        RespValue::BulkString(Bytes::copy_from_slice(content)),
    ))
}

fn parse_array(input: &[u8]) -> IResult<&[u8], RespValue> {
    let (input, count_str) = nom::sequence::preceded(
        tag("*"),
        nom::sequence::terminated(take_until("
"), crlf),
    )(input)?;

    // parse_i64 now returns Result, so we need to handle its potential error.
    let count = parse_i64(count_str)?;

    if count == -1 {
        return Ok((input, RespValue::NullArray));
    }

    if count < 0 {
        // Safeguard against negative counts.
        return Err(nom::Err::Failure(nom::error::Error::new(
            count_str,
            nom::error::ErrorKind::Digit,
        )));
    }

    // `nom::multi::count` handles parsing the specified number of items.
    // If any item fails to parse, `count` will return an error.
    let (input, items) = nom::multi::count(parse_message, count as usize)(input)?;
    Ok((input, RespValue::Array(items)))
}

//--- Encoder (Serializer) ---

impl Encoder<RespValue> for RespDecoder {
    type Error = AppError;

    fn encode(&mut self, item: RespValue, dst: &mut BytesMut) -> Result<(), Self::Error> {
        match item {
            RespValue::SimpleString(s) => {
                dst.extend_from_slice(b"+");
                dst.extend_from_slice(&s);
                dst.extend_from_slice(b"
");
            }
            RespValue::Error(s) => {
                dst.extend_from_slice(b"-");
                dst.extend_from_slice(&s);
                dst.extend_from_slice(b"
");
            }
            RespValue::Integer(i) => {
                dst.extend_from_slice(b":");
                dst.extend_from_slice(i.to_string().as_bytes());
                dst.extend_from_slice(b"
");
            }
            RespValue::BulkString(s) => {
                dst.extend_from_slice(b"$");
                dst.extend_from_slice(s.len().to_string().as_bytes());
                dst.extend_from_slice(b"
");
                dst.extend_from_slice(&s);
                dst.extend_from_slice(b"
");
            }
            RespValue::NullBulkString => {
                dst.extend_from_slice(b"$-1
");
            }
            RespValue::Array(items) => {
                dst.extend_from_slice(b"*");
                dst.extend_from_slice(items.len().to_string().as_bytes());
                dst.extend_from_slice(b"
");
                for item in items {
                    // The encode method itself can return an error, so we propagate it.
                    self.encode(item, dst)?;
                }
            }
            RespValue::NullArray => {
                dst.extend_from_slice(b"*-1
");
            }
        }
        Ok(())
    }
}
