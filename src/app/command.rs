use crate::app::{error::AppError, protocol::RespValue};
use bytes::Bytes;
use std::str::FromStr;
use strum_macros::{Display, EnumString};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Copy, Display, EnumString)]
#[strum(serialize_all = "UPPERCASE", ascii_case_insensitive)]
pub enum Command {
    Ping,
    Echo,
    Set,
    Get,
    StrLen,
    Del,
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
    Watch,
    Unwatch,
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
    // Bitmap Commands
    SetBit,
    GetBit,
    BitCount,
    BitOp,
    // Access Control
    Acl,
    Auth,
}

impl Command {
    /// The command's name as Redis writes it in error messages, e.g. `blpop`.
    pub fn name(self) -> String {
        self.to_string().to_lowercase()
    }

    /// The number of parts the command takes, counting its name, as Redis
    /// gives it: `n` means exactly `n`, and `-n` means at least `n`.
    fn arity(self) -> i32 {
        match self {
            Command::Multi | Command::Exec | Command::Discard | Command::Unwatch => 1,
            Command::Echo
            | Command::Get
            | Command::StrLen
            | Command::Incr
            | Command::Keys
            | Command::Type
            | Command::LLen
            | Command::ZCard => 2,
            Command::Wait | Command::Publish | Command::ZScore | Command::GetBit => 3,
            Command::LRange | Command::SetBit => 4,
            Command::Auth => 3,
            Command::Ping | Command::Info | Command::ReplConf | Command::Unsubscribe => -1,
            Command::Config
            | Command::Acl
            | Command::Watch
            | Command::Del
            | Command::LPop
            | Command::Subscribe
            | Command::GeoPos
            | Command::BitCount => -2,
            Command::Set
            | Command::PSync
            | Command::RPush
            | Command::LPush
            | Command::BLPop
            | Command::ZRank
            | Command::ZRem => -3,
            Command::XRange
            | Command::XRead
            | Command::ZAdd
            | Command::ZRange
            | Command::GeoDist
            | Command::BitOp => -4,
            Command::XAdd | Command::GeoAdd => -5,
            Command::GeoSearch => -7,
        }
    }

    /// Whether the command can change the keyspace. Replicas reject these from
    /// clients, and a master sends them to its replicas once they succeed.
    pub fn is_write(self) -> bool {
        matches!(
            self,
            Command::Set
                | Command::Del
                | Command::Incr
                | Command::LPush
                | Command::RPush
                | Command::LPop
                | Command::BLPop
                | Command::XAdd
                | Command::ZAdd
                | Command::ZRem
                | Command::GeoAdd
                | Command::SetBit
                | Command::BitOp
        )
    }
}

/// Parses UTF-8 `bytes` as a `T`, as Redis parses numeric arguments.
pub fn parse<T: FromStr>(bytes: &[u8]) -> Option<T> {
    std::str::from_utf8(bytes).ok()?.parse().ok()
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

    /// Checks the number of arguments against the command's arity.
    pub fn check_arity(&self) -> Result<(), AppError> {
        let parts = self.args.len() + 1;
        let arity = self.command.arity();
        let needed = arity.unsigned_abs() as usize;
        let fits = if arity < 0 {
            parts >= needed
        } else {
            parts == needed
        };
        if fits {
            Ok(())
        } else {
            Err(AppError::WrongArity(self.command))
        }
    }

    pub fn arg(&self, index: usize) -> Option<&Bytes> {
        self.args.get(index)
    }

    pub fn first(&self) -> Option<&Bytes> {
        self.args.first()
    }

    /// Argument `index`, or a wrong-number-of-arguments error if it's missing.
    pub fn required(&self, index: usize) -> Result<&Bytes, AppError> {
        self.args
            .get(index)
            .ok_or(AppError::WrongArity(self.command))
    }

    /// Argument `index` parsed as an integer type.
    pub fn integer<T: FromStr>(&self, index: usize) -> Result<T, AppError> {
        parse(self.required(index)?).ok_or(AppError::NotAnInteger)
    }

    /// Argument `index` parsed as a float.
    pub fn float(&self, index: usize) -> Result<f64, AppError> {
        parse(self.required(index)?).ok_or(AppError::NotAFloat)
    }

    pub fn args_from(&self, start_index: usize) -> &[Bytes] {
        if start_index >= self.args.len() {
            &[]
        } else {
            &self.args[start_index..]
        }
    }

    /// Replaces argument `index`, if the command has one.
    pub fn with_arg(mut self, index: usize, value: Bytes) -> Self {
        if let Some(arg) = self.args.get_mut(index) {
            *arg = value;
        }
        self
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unknown_commands_get_an_err_reply() {
        let unknown = RespValue::Array(vec![
            RespValue::BulkString(Bytes::from_static(b"NOSUCHCOMMAND")),
            RespValue::BulkString(Bytes::from_static(b"ARG")),
        ]);
        let error = ParsedCommand::from_resp(unknown).expect_err("NOSUCHCOMMAND is unknown");
        assert_eq!(
            RespValue::from(error),
            RespValue::Error(Bytes::from_static(b"ERR unknown command 'NOSUCHCOMMAND'"))
        );
    }

    fn command(parts: &[&str]) -> ParsedCommand {
        ParsedCommand::from_resp(RespValue::Array(
            parts
                .iter()
                .map(|p| RespValue::BulkString(Bytes::copy_from_slice(p.as_bytes())))
                .collect(),
        ))
        .expect("valid command")
    }

    fn reply(result: Result<impl std::fmt::Debug, AppError>) -> String {
        result.map_or_else(|e| e.to_string(), |v| format!("{v:?}"))
    }

    #[test]
    fn argument_helpers_return_redis_errors() {
        let parsed = command(&["BLPOP", "q", "1.5", "x"]);
        assert_eq!(reply(parsed.required(0)), "b\"q\"");
        assert_eq!(
            reply(parsed.required(3)),
            "ERR wrong number of arguments for 'blpop' command"
        );
        assert_eq!(reply(parsed.float(1)), "1.5");
        assert_eq!(
            reply(parsed.integer::<i64>(1)),
            "ERR value is not an integer or out of range"
        );
        assert_eq!(reply(parsed.float(2)), "ERR value is not a valid float");
        assert_eq!(
            reply(parsed.integer::<i64>(9)),
            "ERR wrong number of arguments for 'blpop' command"
        );
    }

    #[test]
    fn arity_counts_the_name_and_allows_more_for_negative_arities() {
        let arity_error = |parts: &[&str]| command(parts).check_arity().map_err(|e| e.to_string());
        let wrong = |name: &str| {
            Err(format!(
                "ERR wrong number of arguments for '{name}' command"
            ))
        };
        assert_eq!(arity_error(&["GET", "k"]), Ok(()));
        assert_eq!(arity_error(&["GET"]), wrong("get"));
        assert_eq!(arity_error(&["GET", "k", "extra"]), wrong("get"));
        assert_eq!(arity_error(&["MULTI", "x"]), wrong("multi"));
        assert_eq!(arity_error(&["PING"]), Ok(()));
        assert_eq!(arity_error(&["PING", "hi"]), Ok(()));
        assert_eq!(arity_error(&["RPUSH", "k", "a", "b", "c"]), Ok(()));
        assert_eq!(arity_error(&["RPUSH", "k"]), wrong("rpush"));
    }
}
