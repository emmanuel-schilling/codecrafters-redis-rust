use crate::app::{error::AppError, protocol::RespValue, AppContext};
use async_trait::async_trait;
use bytes::Bytes;
use std::str::FromStr;
use std::sync::Arc;
use strum_macros::{Display, EnumString};
use tokio::sync::Notify;

#[async_trait]
pub trait CommandHandler: Send + Sync {
    async fn handle(
        &self,
        ctx: &Arc<AppContext>,
        parsed: ParsedCommand,
        wait_notify: &Arc<Notify>,
    ) -> RespValue;
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Copy, Display, EnumString)]
#[strum(serialize_all = "UPPERCASE", ascii_case_insensitive)]
pub enum Command {
    Ping,
    Echo,
    Set,
    Get,
    Info,
    ReplConf,
    PSync,
    Wait,
    Config,
    Keys,
    Type,
    XAdd,
    XRange,
    XRead,
    Incr,
    Multi,
    Exec,
    Discard,
    RPush,
    LPush,
    LRange,
    LLen,
    LPop,
    BLPop,
    Subscribe,
    Publish,
    Unsubscribe,
    // Sorted Set Commands
    ZAdd,
    ZCard,
    ZScore,
    ZRank,
    ZRange,
    ZRem,
    // Geospatial Commands
    GeoAdd,
    GeoPos,
    GeoDist,
    GeoSearch,
}

#[derive(Debug, Clone)]
pub struct ParsedCommand {
    command: Command,
    args: Vec<Bytes>,
}

impl ParsedCommand {
    pub fn from_resp(value: RespValue) -> Result<Self, AppError> {
        match value {
            RespValue::Array(values) => {
                let mut iter = values.into_iter();
                let Some(RespValue::BulkString(command_bytes)) = iter.next() else {
                    return Err(AppError::ParseError("Command must be a bulk string".into()));
                };

                let command_str = std::str::from_utf8(&command_bytes)
                    .map_err(|_| AppError::ParseError("Command contains invalid UTF-8".into()))?;

                let command = Command::from_str(command_str)
                    .map_err(|_| AppError::UnknownCommand(command_str.to_string()))?;

                let args = iter
                    .map(|val| match val {
                        RespValue::BulkString(bytes) => Ok(bytes),
                        _ => Err(AppError::ParseError(
                            "Argument must be a bulk string".into(),
                        )),
                    })
                    .collect::<Result<Vec<Bytes>, _>>()?;

                Ok(ParsedCommand { command, args })
            }
            _ => Err(AppError::ParseError("Command must be a RESP array".into())),
        }
    }

    pub fn command(&self) -> Command {
        self.command
    }

    pub fn arg(&self, index: usize) -> Option<&Bytes> {
        self.args.get(index)
    }

    pub fn first(&self) -> Option<&Bytes> {
        self.args.first()
    }

    pub fn args_from(&self, start_index: usize) -> &[Bytes] {
        if start_index >= self.args.len() {
            &[]
        } else {
            &self.args[start_index..]
        }
    }

    pub fn into_resp_array(self) -> RespValue {
        let mut parts = Vec::with_capacity(self.args.len() + 1);
        parts.push(RespValue::BulkString(Bytes::from(self.command.to_string())));
        for arg in self.args {
            parts.push(RespValue::BulkString(arg));
        }
        RespValue::Array(parts)
    }
}
