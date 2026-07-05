//! The homebase client SDK: session and lease-lifetime machinery over the
//! seven kernel verbs.
//!
//! The client speaks [`homebase_core::space::Space`] — the transport-neutral
//! verb contract. In-process today (tests, the sim, embedded use); the wire
//! client implements the same trait later and nothing here changes.
//!
//! What lives here:
//!
//! - [`lease::HeldLease`] — a grant bound to its *local* deadline: the
//!   client half of asymmetric expiry. TTLs count from request send on the
//!   client's own clock, so the local window always closes before the
//!   server's.
//! - [`session::Session`] — one device's connection to one space:
//!   `device_seq` discipline, lease bookkeeping and coverage, heartbeat
//!   renewal, and the retry contract over `Unavailable` (the replay fence
//!   turns a retried-but-admitted batch into
//!   [`session::PutOutcome::AlreadyApplied`]).
//!
//! Mechanism, not policy — and the DST contract holds throughout: nothing
//! here spawns tasks, sleeps, or reads a wall clock. Time is an injected
//! [`Clock`](homebase_core::clock::Clock); heartbeats fire when the caller
//! cranks them; a tokio-driven policy loop layers above.
//!
//! Later batches: read cursors / shape catch-up (the acquire barrier's
//! consumer), and the E2EE codec.

pub mod lease;
pub mod session;

pub use lease::HeldLease;
pub use session::{Acquired, HeartbeatReport, PutError, PutOutcome, Session};
