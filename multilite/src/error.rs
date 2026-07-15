use std::fmt;

/// An error returned by Multilite's SQLite-facing API.
#[derive(Debug)]
pub enum Error {
    /// SQLite rejected an operation. The original error is retained intact.
    Sqlite(rusqlite::Error),
    /// V1 prepared statements are read-only; writes must use `execute`.
    PreparedWrite,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Sqlite(error) => write!(f, "sqlite error: {error}"),
            Self::PreparedWrite => {
                f.write_str("prepared statements are read-only; use execute for writes")
            }
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Sqlite(error) => Some(error),
            Self::PreparedWrite => None,
        }
    }
}

impl From<rusqlite::Error> for Error {
    fn from(error: rusqlite::Error) -> Self {
        Self::Sqlite(error)
    }
}

pub type Result<T> = std::result::Result<T, Error>;
