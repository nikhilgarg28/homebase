//! The homebase client SDK, rebuilt in atomic batches — see DESIGN.md,
//! "Client build sequence": interlocking contracts, deep implementation.
//!
//! Landed so far:
//!
//! - [`server`] — **contract 1 of 3**: [`ServerHandle`], the client's view
//!   of a server — the kernel verbs, space-qualified, exactly the wire
//!   shape. In-process closures and the uninhabited [`Offline`] implement
//!   it today; the HTTP adapter lands later, gated on
//!   [`server::conformance`].
//!
//! - [`meta`] — **contract 2 of 3**: [`MetaStore`](meta::MetaStore), a
//!   device's durable truth (global identity and clock tripwire; per-space
//!   submit/admit cursors and logs, version high-water, leases, and codec cache) expressed as the transition
//!   vocabulary itself — every method one atomic, durable, async step.
//!
//! - [`client`] — the device-scoped coordinator: one store, one device
//!   identity, and persisted per-space oplogs — over many [`Space`] drivers.
//!
//! - [`space`] — per-space submit/push, dense admit-log pull/application,
//!   stateless range fetch, and lease operations, reached through
//!   [`Client::attach`] and [`Client::space`].
//!
//! - [`cipher`] — the privacy boundary: `SpaceEnvelope`, space-id
//!   commitment, deterministic name pseudonyms, and detached value seals.

pub mod cipher;
pub mod client;
mod coordination;
pub mod meta;
pub mod server;
pub mod space;

pub use client::{Client, ClientError, open_offline};
pub use homebase_core::Mutation;
pub use server::{Offline, ServerHandle};
pub use space::{
    Admits, DEFAULT_PULL_CAP, DEFAULT_PUSH_CAP, FetchedRange, LeaseState, PushOutcome, PushReceipt,
    RepairedLeases, Space, SpaceDriverError, Submission, lease_margin,
};
