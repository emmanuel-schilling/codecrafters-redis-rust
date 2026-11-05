use thiserror::Error;

/// Represents errors that can occur within the application logic.
#[derive(Error, Debug)]
pub enum AppError {
    /// Used when a client connection provides an incomplete command.
    #[error("incomplete command")]
    Incomplete,

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
}
