//! The homebase client SDK, rebuilt in atomic batches — see DESIGN.md,
//! "Client build sequence": interlocking contracts, deep implementation.
//!
//! Landed so far:
//!
//! - [`server`] — **contract 1 of 3**: [`ServerHandle`], the client's view
//!   of a server — the seven verbs, space-qualified, exactly the wire
//!   shape. In-process closures and the uninhabited [`Offline`] implement
//!   it today; the HTTP adapter lands later, gated on
//!   [`server::conformance`].
//!
//! - [`meta`] — **contract 2 of 3**: [`MetaStore`](meta::MetaStore), a
//!   device's durable truth (identity, one shared seq stream, one oplog,
//!   per-space range watermarks/leases/codec cache) expressed as the transition
//!   vocabulary itself — every method one atomic, durable, async step.
//!   [`OrderedMetaStore`](meta::OrderedMetaStore) is the reference
//!   implementation over any
//!   [`OrderedStore`](homebase_core::storage::OrderedStore); multilite
//!   implements the trait natively as legible SQLite system tables. The
//!   recomputation oracle ([`certify`](meta::certify) /
//!   [`audit`](meta::audit)) and [`meta::conformance`] gate every
//!   implementation.
//!
//! - [`replica`] — the one-space driver over both contracts (plus an injected
//!   [`Clock`](homebase_core::clock::Clock)): no mirror — durable
//!   collections are read from the store on demand — two-clock lease
//!   discipline with explicit renewal and idempotent acquire, and the
//!   adaptive pusher — FIFO groups on the wire, recovery reconstructed
//!   entirely from kernel rejections (seq collision → trim, group
//!   rejection → solo probes, solo rejection → conviction, fork →
//!   fatal). It owns the space envelope/cipher and encrypts at ingest.
//!
//! - [`cipher`] — the privacy boundary: `SpaceEnvelope`, space-id
//!   commitment, deterministic name pseudonyms, and value envelopes.
//!
//! - [`engine`] — compatibility aliases for the old state-machine name.
//!
//! Next: client identity and envelope discovery.

pub mod cipher;
pub mod engine;
pub mod meta;
pub mod replica;
pub mod server;

pub use replica::{Acquired, PushOutcome, Replica, ReplicaError};
pub use server::{Offline, ServerHandle};
