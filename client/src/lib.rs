//! The homebase client SDK, rebuilt in atomic batches — see DESIGN.md,
//! "Client build sequence": interlocking contracts, deep implementation.
//!
//! Landed so far:
//!
//! - [`server`] — **contract 1 of 3**: [`ServerHandle`], the client's view
//!   of a server — the seven verbs, space-qualified, exactly the wire
//!   shape. In-process closures and the uninhabited [`Offline`] implement
//!   it today; the gRPC adapter lands later, gated on
//!   [`server::conformance`].
//!
//! Next: the client meta schema (contract 2: durable truth over
//! [`OrderedStore`](homebase_core::storage::OrderedStore)), then the
//! write-through engine.

pub mod server;

pub use server::{Offline, ServerHandle};
