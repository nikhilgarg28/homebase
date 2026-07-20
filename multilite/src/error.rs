use std::fmt;

use homebase_client::ClientError;
use homebase_core::messages::KernelError;
use homebase_core::storage::StorageError;

use crate::database::PushRejection;

/// An error returned by Multilite's SQLite-facing API.
#[derive(Debug)]
pub enum Error {
    /// SQLite rejected an operation. The original error is retained intact.
    Sqlite(rusqlite::Error),
    /// Prepared statements are read-only; writes must use `execute`.
    PreparedWrite,
    /// The statement uses SQL outside Multilite's current public surface.
    UnsupportedSql(&'static str),
    /// The selected open-time policy cannot operate without authority.
    AuthorityRequired(&'static str),
    /// Authority definitively rejected a remote write and local effects were undone.
    AuthorityRejected(KernelError),
    /// A read refresh could not drain a definitively rejected local submission.
    RefreshPushRejected(PushRejection),
    /// An internal background worker could not be started.
    BackgroundWorker(String),
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
    /// Operating-system randomness was unavailable while minting identity.
    Entropy(String),
    /// A Multilite operation was malformed or contradicted its SQL.
    InvalidMultiliteOp(String),
    /// A transaction manifest or its lowered Homebase batch was malformed.
    InvalidMultiliteTransaction(String),
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
            Self::AuthorityRequired(policy) => {
                write!(f, "{policy} requires an authority server")
            }
            Self::AuthorityRejected(error) => {
                write!(f, "authority rejected write: {error}")
            }
            Self::RefreshPushRejected(rejection) => write!(
                f,
                "read refresh push was rejected at device sequence {}: {}",
                rejection.failed_sequence(),
                rejection.error()
            ),
            Self::BackgroundWorker(message) => {
                write!(f, "could not start Multilite background worker: {message}")
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
            Self::Entropy(message) => write!(f, "could not mint database identity: {message}"),
            Self::InvalidMultiliteOp(message) => {
                write!(f, "invalid Multilite operation: {message}")
            }
            Self::InvalidMultiliteTransaction(message) => {
                write!(f, "invalid Multilite transaction: {message}")
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
            Self::PreparedWrite
            | Self::UnsupportedSql(_)
            | Self::AuthorityRequired(_)
            | Self::AuthorityRejected(_)
            | Self::RefreshPushRejected(_)
            | Self::BackgroundWorker(_) => None,
            Self::CaptureInvariant(_) => None,
            Self::Storage(error) => Some(error),
            Self::InvalidDatabase(_)
            | Self::DatabaseIdMismatch { .. }
            | Self::InvalidReplicaInvitation
            | Self::Entropy(_)
            | Self::InvalidMultiliteOp(_)
            | Self::InvalidMultiliteTransaction(_)
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
