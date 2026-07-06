//! Compatibility aliases for the old engine name.
//!
//! The state machine now lives in [`crate::replica`] because it owns one
//! space envelope/cipher and commits encrypted data at ingest.

pub use crate::replica::{
    Acquired, LeaseState, PushOutcome, Replica as Engine, ReplicaError as EngineError, lease_margin,
};
