//! The server-side error type: semantic kernel rejections or storage faults.

use crate::storage::StorageError;
use homebase_core::messages::KernelError;
use homebase_core::space::SpaceError;
use std::fmt;

/// Either the kernel said no (an invariant refused to bend — report to the
/// client as-is) or the storage backend failed (an infrastructure fault —
/// retriable, alertable, never the client's fault).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Error {
    Kernel(KernelError),
    Storage(StorageError),
}

impl From<KernelError> for Error {
    fn from(e: KernelError) -> Self {
        Self::Kernel(e)
    }
}

impl From<StorageError> for Error {
    fn from(e: StorageError) -> Self {
        Self::Storage(e)
    }
}

/// The client-facing projection: kernel rejections pass through verbatim,
/// storage faults collapse to [`SpaceError::Unavailable`] (infrastructure
/// detail is the operator's business, not the client's).
impl From<Error> for SpaceError {
    fn from(e: Error) -> Self {
        match e {
            Error::Kernel(e) => SpaceError::Kernel(e),
            Error::Storage(e) => SpaceError::unavailable(e.to_string()),
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Kernel(e) => write!(f, "{e}"),
            Self::Storage(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for Error {}
