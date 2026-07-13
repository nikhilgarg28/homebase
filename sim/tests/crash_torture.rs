//! Layer 1 crash-restart torture ([`SimStore`] + [`SimExecutor`]).
//!
//! Layer 3 (slatedb + fault object store) lives in `persist_crash_torture.rs`.

use homebase::actor::SpaceActor;
use homebase_core::clock::{ManualClock, Timestamp};
use homebase_core::messages::GetRequest;
use homebase_core::space::Space as _;
use homebase_sim::crash::{self, SPACE, sim, user_key};
use homebase_sim::exec::SimExecutor;
use homebase_sim::seeds;
use homebase_sim::store::{FaultConfig, SimStore};
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;

#[test]
fn crash_torture_seeds_hold_invariants() {
    let mut total = crash::Coverage::default();
    for seed in seeds::torture_seeds() {
        let (_, coverage) = sim::run_seed(seed);
        total.lease_invalid += coverage.lease_invalid;
        total.seq_regression += coverage.seq_regression;
        total.acked_writes_lost += coverage.acked_writes_lost;
        total.unavailable += coverage.unavailable;
    }
    println!(
        "coverage across {} seeds: {total:?}",
        seeds::torture_seed_count()
    );
    if !seeds::torture_coverage_enforced() {
        return;
    }
    assert!(total.seq_regression > 0, "no replay-fence hits: {total:?}");
    assert!(
        total.acked_writes_lost > 0,
        "no acked-write loss: {total:?}"
    );
    assert!(
        total.unavailable > 0,
        "no unavailability observed: {total:?}"
    );
}

#[test]
fn identical_seeds_replay_identically() {
    for seed in [0, 7, 42] {
        assert_eq!(
            sim::run_seed(seed).0,
            sim::run_seed(seed).0,
            "seed {seed} diverged on replay"
        );
    }
}

#[test]
fn recovered_space_still_serves_reads() {
    sim::run_seed(1);
    let store = SimStore::new(99, FaultConfig::NONE);
    let clock = Arc::new(ManualClock::new(Timestamp(0)));
    let mut exec = SimExecutor::new(0);
    let (actor, handle) = SpaceActor::new(SPACE, Arc::new(store), clock);
    exec.spawn(actor.run());

    let result = Rc::new(RefCell::new(None));
    let out = Rc::clone(&result);
    exec.spawn(async move {
        let got = handle
            .get(GetRequest {
                keys: vec![user_key(0, 1)],
            })
            .await
            .unwrap();
        *out.borrow_mut() = Some(got.entries[0].is_none());
    });
    exec.run_until_stalled();
    assert_eq!(*result.borrow(), Some(true), "fresh store has no data");
}
