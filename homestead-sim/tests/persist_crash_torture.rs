//! Layer 3 crash-restart torture: slatedb + fault-injecting object store.
//!
//! Same harness as Layer 1 ([`homestead_sim::crash`]) but crash = object-store
//! checkpoint rollback + db reopen.
//!
//! Layer 3 uses tokio task scheduling (not [`SimExecutor`]), so replay
//! determinism is not asserted here — structural oracles per seed suffice.
//!
//! Run: `cargo test -p homestead-sim persist_crash`
//! Skip: `cargo test -p homestead-sim --no-default-features` (slate tests omitted)

#![cfg(feature = "slatedb")]

use homestead_sim::crash::slate;
use homestead_sim::seeds;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn persist_crash_torture_seeds_hold_invariants() {
    for seed in seeds::persist_torture_seeds() {
        let (trace, _coverage) = slate::run_seed(seed).await;
        assert!(
            !trace.is_empty(),
            "seed {seed} produced no acks after persist torture"
        );
    }
    println!(
        "persist crash torture: {} seeds clean",
        seeds::persist_torture_seed_count()
    );
}
