//! Deterministic simulation testing for the homestead kernel.
//!
//! Everything here exists to make a whole kernel run — actors, storage,
//! clients, time — a **pure function of a seed**. Same seed, same
//! execution, same bug; a failing seed is a replay artifact, not a flaky
//! test.
//!
//! The pieces:
//!
//! - [`exec::SimExecutor`] — a single-threaded stepper. Among the tasks
//!   that are ready to run, a seeded RNG picks which one is polled next:
//!   every legal interleaving of actors and clients is reachable, and each
//!   is replayable.
//! - [`store::SimStore`] — an [`OrderedStore`] with seeded fault injection:
//!   operations yield a random number of times (interleaving points),
//!   reads and writes can fail with injected [`StorageError`]s (always
//!   *before* mutating — batch atomicity is never faked), and durability is
//!   modeled explicitly: applied batches sit in volatile state until a
//!   randomized flush, and [`crash`](store::SimStore::crash) rolls back to
//!   the last flush — always a *prefix* of applied batches, like a WAL.
//! - [`check`] — brute-force invariant oracles run over a (recovered)
//!   store: changelog ⇔ data agreement, dense admission seqs, per-prefix
//!   aggregates, lease index agreement, counter monotonicity.
//!
//! The actors and state machines under test are the production types from
//! `homestead-server`, unmodified — that is the point. The kernel takes
//! `now` explicitly, the actor loop is runtime-agnostic, and all IO sits
//! behind `OrderedStore`, so this crate supplies environments, never
//! forks.
//!
//! [`OrderedStore`]: homestead_server::storage::OrderedStore
//! [`StorageError`]: homestead_server::storage::StorageError

pub mod check;
pub mod exec;
pub mod store;
pub mod torture;
