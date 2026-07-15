use std::fmt;

use rusqlite::types::{FromSqlError, Type};

/// An error returned by Multilite's SQLite-facing API.
#[derive(Debug)]
pub enum Error {
    /// SQLite rejected an operation. The original error is retained intact.
    Sqlite(rusqlite::Error),
    /// V1 prepared statements are read-only; writes must use `execute`.
    PreparedWrite,
    /// A borrowed SQLite value could not be copied into its owned form.
    ValueConversion(FromSqlError),
    /// A value did not have the required SQLite storage class.
    UnexpectedValueType {
        /// Storage class required by the operation.
        expected: Type,
        /// Storage class carried by the value.
        actual: Type,
    },
    /// A SQLite hook observed a state that violates its capture contract.
    CaptureInvariant(&'static str),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Sqlite(error) => write!(f, "sqlite error: {error}"),
            Self::PreparedWrite => {
                f.write_str("prepared statements are read-only; use execute for writes")
            }
            Self::ValueConversion(error) => {
                write!(f, "could not copy SQLite value: {error}")
            }
            Self::UnexpectedValueType { expected, actual } => {
                write!(
                    f,
                    "unexpected SQLite value type: expected {expected}, got {actual}"
                )
            }
            Self::CaptureInvariant(message) => {
                write!(f, "SQLite capture invariant failed: {message}")
            }
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Sqlite(error) => Some(error),
            Self::PreparedWrite => None,
            Self::ValueConversion(error) => Some(error),
            Self::UnexpectedValueType { .. } => None,
            Self::CaptureInvariant(_) => None,
        }
    }
}

impl From<rusqlite::Error> for Error {
    fn from(error: rusqlite::Error) -> Self {
        Self::Sqlite(error)
    }
}

pub type Result<T> = std::result::Result<T, Error>;
