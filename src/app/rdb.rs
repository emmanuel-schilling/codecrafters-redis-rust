use crate::app::store::{self, DataType, StoreValue};
use crate::config::Config;
use anyhow::{bail, Context, Result};
use bytes::Bytes;
use std::collections::HashMap;
use std::fs;
use std::time::Duration;
use tracing::debug;

/// A minimal RDB file (version 11) holding no keys.
const EMPTY_RDB: &[u8] =
    b"REDIS0011\xfa\x09redis-ver\x057.2.0\xfa\x0aredis-bits\xc0\x40\xfe\x00\xff";

/// Returns an empty RDB file framed as a RESP bulk string without the trailing
/// CRLF, the form a master sends to a replica after FULLRESYNC.
pub fn empty_rdb_bytes() -> Vec<u8> {
    let mut response = format!("${}\r\n", EMPTY_RDB.len()).into_bytes();
    response.extend_from_slice(EMPTY_RDB);
    response
}

/// Loads the RDB file named by the config. A missing file gives an empty keyspace.
pub fn load(config: &Config) -> Result<HashMap<Bytes, StoreValue>> {
    let path = config.rdb_file_path();
    if !path.exists() {
        return Ok(HashMap::new());
    }
    parse(&fs::read(path)?)
}

fn parse(file: &[u8]) -> Result<HashMap<Bytes, StoreValue>> {
    let mut reader = Reader { buf: file };
    if reader.take(5)? != b"REDIS" {
        bail!("not an RDB file: missing REDIS header");
    }
    reader.take(4)?; // Version number.

    let mut db = HashMap::new();
    let mut expires_at = None;
    while let Some(opcode) = reader.next_byte() {
        match opcode {
            0xFA => {
                // Auxiliary field: name and value.
                reader.string()?;
                reader.string()?;
            }
            0xFB => {
                // Hash table size hints for keys and for expiries.
                reader.length()?;
                reader.length()?;
            }
            0xFE => {
                // Database selector.
                reader.length()?;
            }
            0xFD => {
                let seconds = u32::from_le_bytes(reader.array()?);
                expires_at = store::instant_at(Duration::from_secs(u64::from(seconds)));
            }
            0xFC => {
                let millis = u64::from_le_bytes(reader.array()?);
                expires_at = store::instant_at(Duration::from_millis(millis));
            }
            0xFF => break, // End of file; a checksum follows.
            0x00 => {
                let key = reader.string()?;
                let value = reader.string()?;
                db.insert(
                    key,
                    StoreValue {
                        data: DataType::String(value),
                        expires_at: expires_at.take(),
                    },
                );
            }
            value_type => bail!("unsupported RDB value type {value_type}"),
        }
    }
    debug!("RDB file parsed: {} keys", db.len());
    Ok(db)
}

/// The first field of a length-encoded value.
enum Encoded {
    Length(usize),
    /// A string stored in a special format; the payload is the format number.
    Special(u8),
}

/// Reads an RDB file front to back; every read fails cleanly at end of input.
struct Reader<'a> {
    buf: &'a [u8],
}

impl<'a> Reader<'a> {
    fn next_byte(&mut self) -> Option<u8> {
        let (&byte, rest) = self.buf.split_first()?;
        self.buf = rest;
        Some(byte)
    }

    fn byte(&mut self) -> Result<u8> {
        self.next_byte().context("RDB file truncated")
    }

    fn take(&mut self, len: usize) -> Result<&'a [u8]> {
        let Some((head, rest)) = self.buf.split_at_checked(len) else {
            bail!("RDB file truncated");
        };
        self.buf = rest;
        Ok(head)
    }

    fn array<const N: usize>(&mut self) -> Result<[u8; N]> {
        let (head, rest) = self
            .buf
            .split_first_chunk::<N>()
            .context("RDB file truncated")?;
        self.buf = rest;
        Ok(*head)
    }

    fn encoded(&mut self) -> Result<Encoded> {
        let first = self.byte()?;
        let low_bits = usize::from(first & 0x3F);
        Ok(match first >> 6 {
            0b00 => Encoded::Length(low_bits),
            0b01 => Encoded::Length(low_bits << 8 | usize::from(self.byte()?)),
            0b10 => match first {
                0x80 => Encoded::Length(usize::try_from(u32::from_be_bytes(self.array()?))?),
                0x81 => Encoded::Length(usize::try_from(u64::from_be_bytes(self.array()?))?),
                _ => bail!("invalid RDB length byte {first:#04x}"),
            },
            _ => Encoded::Special(first & 0x3F),
        })
    }

    fn length(&mut self) -> Result<usize> {
        match self.encoded()? {
            Encoded::Length(len) => Ok(len),
            Encoded::Special(format) => {
                bail!("expected an RDB length, found string format {format}")
            }
        }
    }

    /// Reads a string; integer-encoded strings come back as their decimal text.
    fn string(&mut self) -> Result<Bytes> {
        let number = match self.encoded()? {
            Encoded::Length(len) => return Ok(Bytes::copy_from_slice(self.take(len)?)),
            Encoded::Special(0) => i64::from(i8::from_le_bytes(self.array()?)),
            Encoded::Special(1) => i64::from(i16::from_le_bytes(self.array()?)),
            Encoded::Special(2) => i64::from(i32::from_le_bytes(self.array()?)),
            Encoded::Special(3) => bail!("LZF-compressed RDB strings are not supported"),
            Encoded::Special(format) => bail!("unknown RDB string format {format}"),
        };
        Ok(Bytes::from(number.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Instant, SystemTime, UNIX_EPOCH};

    /// Wraps database opcodes in an RDB header, end marker and (unchecked) checksum.
    fn rdb(body: &[u8]) -> Vec<u8> {
        let mut file = b"REDIS0011".to_vec();
        file.extend_from_slice(body);
        file.push(0xFF);
        file.extend_from_slice(&[0; 8]);
        file
    }

    fn string_value(db: &HashMap<Bytes, StoreValue>, key: &[u8]) -> (Bytes, Option<Instant>) {
        let value = db.get(key).expect("key loaded");
        match &value.data {
            DataType::String(s) => (s.clone(), value.expires_at),
            other => panic!("expected a string, found {other:?}"),
        }
    }

    fn unix_millis(offset_secs: i64) -> u64 {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock after 1970")
            .as_millis() as i64;
        (now + offset_secs * 1000) as u64
    }

    #[test]
    fn empty_rdb_matches_the_previous_hex_and_has_no_keys() {
        const PREVIOUS_HEX: &str = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fe00ff";
        let decoded: Vec<u8> = (0..PREVIOUS_HEX.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&PREVIOUS_HEX[i..i + 2], 16).expect("hex"))
            .collect();
        assert_eq!(EMPTY_RDB, &decoded[..]);
        assert!(parse(EMPTY_RDB).expect("parse").is_empty());

        let framed = empty_rdb_bytes();
        let header = format!("${}\r\n", EMPTY_RDB.len());
        assert_eq!(&framed[..header.len()], header.as_bytes());
        assert_eq!(&framed[header.len()..], EMPTY_RDB);
    }

    #[test]
    fn loads_string_keys_with_their_expiries() {
        let mut body = b"\xfa\x09redis-ver\x057.2.0\xfe\x00\xfb\x03\x02".to_vec();
        body.extend_from_slice(b"\x00\x03foo\x03bar");
        body.push(0xFC);
        body.extend_from_slice(&unix_millis(3600).to_le_bytes());
        body.extend_from_slice(b"\x00\x03baz\x03qux");
        body.push(0xFD);
        body.extend_from_slice(&((unix_millis(-3600) / 1000) as u32).to_le_bytes());
        body.extend_from_slice(b"\x00\x03old\x03val");

        let db = parse(&rdb(&body)).expect("parse");
        assert_eq!(db.len(), 3);
        assert_eq!(
            string_value(&db, b"foo"),
            (Bytes::from_static(b"bar"), None)
        );

        let now = Instant::now();
        let (value, expiry) = string_value(&db, b"baz");
        assert_eq!(value, Bytes::from_static(b"qux"));
        assert!(expiry.is_some_and(|t| t > now));
        let (value, expiry) = string_value(&db, b"old");
        assert_eq!(value, Bytes::from_static(b"val"));
        assert!(expiry.is_some_and(|t| t < now));
    }

    #[test]
    fn decodes_integer_encoded_strings_as_decimal_text() {
        let mut body = b"\x00\xc0\x07\xc0\x80".to_vec(); // key 7, value -128
        body.extend_from_slice(b"\x00\x01a\xc1");
        body.extend_from_slice(&12345i16.to_le_bytes());
        body.extend_from_slice(b"\x00\x01b\xc2");
        body.extend_from_slice(&(-100_000i32).to_le_bytes());

        let db = parse(&rdb(&body)).expect("parse");
        assert_eq!(string_value(&db, b"7").0, Bytes::from_static(b"-128"));
        assert_eq!(string_value(&db, b"a").0, Bytes::from_static(b"12345"));
        assert_eq!(string_value(&db, b"b").0, Bytes::from_static(b"-100000"));
    }

    #[test]
    fn reads_14_bit_and_32_bit_lengths() {
        let long = vec![b'x'; 300];
        let mut body = b"\x00\x01a".to_vec();
        body.extend_from_slice(&[0x40 | (300 >> 8) as u8, (300 & 0xFF) as u8]);
        body.extend_from_slice(&long);
        body.extend_from_slice(b"\x00\x01b\x80");
        body.extend_from_slice(&70u32.to_be_bytes());
        body.extend_from_slice(&[b'y'; 70]);

        let db = parse(&rdb(&body)).expect("parse");
        assert_eq!(string_value(&db, b"a").0, Bytes::from(long));
        assert_eq!(string_value(&db, b"b").0, Bytes::from(vec![b'y'; 70]));
    }

    #[test]
    fn rejects_files_it_cannot_read() {
        let cases: [(&str, Vec<u8>); 5] = [
            ("bad header", b"RXDIS0011\xff".to_vec()),
            ("truncated string", b"REDIS0011\x00\x03fo".to_vec()),
            ("truncated expiry", b"REDIS0011\xfc\x01\x02".to_vec()),
            ("list value", rdb(b"\x01\x01k\x01\x01a")),
            ("LZF string", rdb(b"\x00\x01k\xc3\x01\x01a")),
        ];
        for (name, file) in cases {
            assert!(parse(&file).is_err(), "accepted {name}");
        }
    }

    #[test]
    fn handles_the_largest_expiry_without_panicking() {
        // How far ahead an `Instant` can reach differs by platform, so this
        // gives either "never expires" or a future time.
        let expiry = store::instant_at(Duration::from_millis(u64::MAX));
        assert!(expiry.is_none_or(|t| t > Instant::now()));
    }
}
