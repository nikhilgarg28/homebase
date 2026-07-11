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
//!   device's durable truth (global identity and clock tripwire; per-space
//!   oplog cursors, version high-water, range watermarks, leases, and codec cache) expressed as the transition
//!   vocabulary itself — every method one atomic, durable, async step.
//!
//! - [`client`] — the device-scoped coordinator: one store, one device
//!   identity, and persisted per-space oplogs — over many [`Space`] drivers.
//!
//! - [`space`] — per-space commit, pull, and lease operations, reached
//!   through [`Client::attach`] and [`Client::space`].
//!
//! - [`cipher`] — the privacy boundary: `SpaceEnvelope`, space-id
//!   commitment, deterministic name pseudonyms, and value envelopes.

pub mod cipher;
pub mod client;
mod coordination;
pub mod meta;
pub mod server;
pub mod space;

pub use client::{Client, ClientError, open_offline};
pub use server::{Offline, ServerHandle};
pub use space::{
    Acquired, DEFAULT_PUSH_CAP, LeaseState, PushOutcome, Space, SpaceDriverError, lease_margin,
};
