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
//!   per-space watermark/leases/codec cache) expressed as the transition
//!   vocabulary itself — every method one atomic, durable, async step.
//!   [`OrderedMetaStore`](meta::OrderedMetaStore) is the reference
//!   implementation over any
//!   [`OrderedStore`](homebase_core::storage::OrderedStore); multilite
//!   implements the trait natively as legible SQLite system tables. The
//!   recomputation oracle ([`certify`](meta::certify) /
//!   [`audit`](meta::audit)) and [`meta::conformance`] gate every
//!   implementation.
//!
//! Next: the write-through engine over both contracts.

pub mod meta;
pub mod server;

pub use server::{Offline, ServerHandle};
