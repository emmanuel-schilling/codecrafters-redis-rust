use crate::app::{command::ParsedCommand, handlers::Reply, protocol::RespValue};
use bytes::Bytes;

pub fn ping() -> Reply {
    Ok(RespValue::SimpleString(Bytes::from_static(b"PONG")))
}

pub fn echo(parsed: &ParsedCommand) -> Reply {
    Ok(RespValue::BulkString(parsed.required(0)?.clone()))
}
