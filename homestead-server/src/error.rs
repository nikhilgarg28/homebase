//! The server-side error type: semantic kernel rejections or storage faults.

use crate::storage::StorageError;
use homestead_core::messages::KernelError;
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

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Kernel(e) => write!(f, "{e}"),
            Self::Storage(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for Error {}
