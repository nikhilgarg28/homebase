use std::fmt;

use homebase_client::ClientError;
use homebase_core::storage::StorageError;
use rusqlite::types::{FromSqlError, Type};

/// An error returned by Multilite's SQLite-facing API.
#[derive(Debug)]
pub enum Error {
    /// SQLite rejected an operation. The original error is retained intact.
    Sqlite(rusqlite::Error),
    /// V1 prepared statements are read-only; writes must use `execute`.
    PreparedWrite,
    /// The statement uses SQL outside Multilite's current public surface.
    UnsupportedSql(&'static str),
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
    /// Durable Homebase metadata storage failed.
    Storage(StorageError),
    /// The file violates the one-space Multilite schema or metadata contract.
    InvalidDatabase(&'static str),
    /// The caller supplied an invitation for a different database.
    DatabaseIdMismatch {
        /// Space identity supplied by the caller.
        expected: [u8; 16],
        /// Space identity stored in the file.
        actual: [u8; 16],
    },
    /// Replica onboarding material has an unknown or malformed encoding.
    InvalidReplicaInvitation,
    /// The local V1 schema was created by a newer unsupported implementation.
    UnsupportedV1SchemaVersion {
        /// Version stored in the local V1 schema ledger.
        found: u64,
        /// Newest V1 schema version understood by this implementation.
        supported: u64,
    },
    /// Operating-system randomness was unavailable while minting identity.
    Entropy(String),
    /// A Multilite operation was malformed or contradicted its SQL.
    InvalidMultiliteOp(String),
    /// Fetched admissions cannot be rebased over speculative local work.
    RebasePendingSubmissions,
    /// Local submit or admit cursors changed during rebase preparation.
    RebaseStateChanged,
    /// A push rejection belongs to another replica or an obsolete submit window.
    StalePushRejection,
    /// The embedded Homebase client failed to initialize.
    Client(ClientError),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Sqlite(error) => write!(f, "sqlite error: {error}"),
            Self::PreparedWrite => {
                f.write_str("prepared statements are read-only; use execute for writes")
            }
            Self::UnsupportedSql(message) => write!(f, "unsupported SQL: {message}"),
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
            Self::Storage(error) => write!(f, "metadata storage error: {error}"),
            Self::InvalidDatabase(message) => write!(f, "invalid Multilite database: {message}"),
            Self::DatabaseIdMismatch { expected, actual } => write!(
                f,
                "database id mismatch: expected {}, found {}",
                hex_id(expected),
                hex_id(actual)
            ),
            Self::InvalidReplicaInvitation => f.write_str("invalid Multilite replica invitation"),
            Self::UnsupportedV1SchemaVersion { found, supported } => write!(
                f,
                "V1 schema version {found} is newer than supported version {supported}"
            ),
            Self::Entropy(message) => write!(f, "could not mint database identity: {message}"),
            Self::InvalidMultiliteOp(message) => {
                write!(f, "invalid Multilite operation: {message}")
            }
            Self::RebasePendingSubmissions => {
                f.write_str("rebase requires the local submit log to be empty")
            }
            Self::RebaseStateChanged => {
                f.write_str("submit or admit state changed while preparing rebase")
            }
            Self::StalePushRejection => {
                f.write_str("push rejection does not match the current submit window")
            }
            Self::Client(error) => write!(f, "homebase client error: {error}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Sqlite(error) => Some(error),
            Self::PreparedWrite | Self::UnsupportedSql(_) => None,
            Self::ValueConversion(error) => Some(error),
            Self::UnexpectedValueType { .. } => None,
            Self::CaptureInvariant(_) => None,
            Self::Storage(error) => Some(error),
            Self::InvalidDatabase(_)
            | Self::DatabaseIdMismatch { .. }
            | Self::InvalidReplicaInvitation
            | Self::UnsupportedV1SchemaVersion { .. }
            | Self::Entropy(_)
            | Self::InvalidMultiliteOp(_)
            | Self::RebasePendingSubmissions
            | Self::RebaseStateChanged
            | Self::StalePushRejection => None,
            Self::Client(error) => Some(error),
        }
    }
}

impl From<rusqlite::Error> for Error {
    fn from(error: rusqlite::Error) -> Self {
        Self::Sqlite(error)
    }
}

impl From<StorageError> for Error {
    fn from(error: StorageError) -> Self {
        Self::Storage(error)
    }
}

impl From<ClientError> for Error {
    fn from(error: ClientError) -> Self {
        Self::Client(error)
    }
}

fn hex_id(id: &[u8; 16]) -> String {
    id.iter().map(|byte| format!("{byte:02x}")).collect()
}

pub type Result<T> = std::result::Result<T, Error>;
