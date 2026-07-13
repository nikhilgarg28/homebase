//! Deterministic simulation testing for the homebase kernel.
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
//! - [`store::SimStore`] — Layer 1 [`OrderedStore`]: seeded fault injection,
//!   explicit durability boundary, [`store::SimStore::crash`].
//! - [`slate_shard::SlateShard`] — Layer 3: real slatedb over a fault-injecting
//!   local object store with checkpointed power loss.
//! - [`crash`] — parameterized crash-restart torture shared by Layer 1 and 3.
//! - [`check`] — brute-force invariant oracles run over a (recovered) store.
//!
//! Seed counts: [`seeds::torture_seed_count`] (default 1000, env
//! `HOMEBASE_TORTURE_SEEDS`).
//!
//! [`OrderedStore`]: homebase::storage::OrderedStore

pub mod check;
pub mod crash;
pub mod exec;
pub mod seeds;
pub mod store;
pub mod torture;

#[cfg(feature = "slatedb")]
pub mod fault_object_store;
#[cfg(feature = "slatedb")]
pub mod fault_slate;
#[cfg(feature = "slatedb")]
pub mod slate_shard;
