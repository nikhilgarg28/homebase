//! Storage for the server: the shared [`OrderedStore`] vocabulary
//! (re-exported from `homebase-core`, where client and sim share it) plus
//! the production slatedb backend, which stays here — it is the server's
//! deployment concern, and its dependency weight must never reach core.

#[cfg(feature = "slatedb")]
mod slate;

pub use homebase_core::storage::{
    MemoryStore, Op, OrderedStore, ScanIter, StorageError, WriteBatch, collect_scan, conformance,
    prefix_successor,
};

#[cfg(feature = "slatedb")]
pub use slate::{SlateOpenOptions, SlateStore, local_object_store};
