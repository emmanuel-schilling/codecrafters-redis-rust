use crate::app::command::Command;
use thiserror::Error;

/// Represents errors that can occur within the application logic.
#[derive(Error, Debug)]
pub enum AppError {
    /// An error encountered during protocol parsing.
    #[error("ERR Protocol error: {0}")]
    ParseError(String),

    /// An unknown command was received.
    #[error("ERR unknown command '{0}'")]
    UnknownCommand(String),

    /// An I/O error from the underlying transport.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// Operation against a key holding the wrong kind of value.
    #[error("WRONGTYPE Operation against a key holding the wrong kind of value")]
    WrongType,

    /// A value could not be parsed as the required type (e.g., for INCR).
    #[error("ERR {0}")]
    ValueError(String),

    /// A command was given too few arguments.
    #[error("ERR wrong number of arguments for '{}' command", .0.name())]
    WrongArity(Command),

    /// An argument that must be an integer isn't one, or is out of range.
    #[error("ERR value is not an integer or out of range")]
    NotAnInteger,

    /// An argument that must be a number isn't one.
    #[error("ERR value is not a valid float")]
    NotAFloat,

    /// Arguments that don't form a valid command.
    #[error("ERR syntax error")]
    Syntax,
}
