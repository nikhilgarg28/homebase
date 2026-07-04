//! First slice of the kernel torture sim: crash-restart under seeded
//! schedules and storage faults.
//!
//! Per seed: one space actor over a [`SimStore`], two simulated client
//! devices writing unique keys through real [`SpaceHandle`]s. Phases end in
//! a simulated power loss — actor task killed mid-whatever, store rolled
//! back to its last flush — then a fresh actor recovers over the surviving
//! bytes and the clients (holding possibly-stale lease refs and device
//! seqs) carry on.
//!
//! Oracles after every crash and at the end:
//!
//! - [`check::audit`]: every structural invariant (changelog ⇔ data,
//!   counters, lease indexes, aggregates, device high waters) holds on the
//!   recovered store;
//! - **prefix durability**: an acknowledged batch survives iff its
//!   admission seq is ≤ the recovered high water — acked writes are lost
//!   only as a suffix, never torn, never reordered;
//! - **whole-run determinism**: replaying a seed reproduces the identical
//!   ack trace.
//!
//! Client recovery exercises real protocol paths, not test scaffolding:
//! orphaned grants from a dead incarnation are taken back with
//! `stealable + steal` (the single-active-device pattern), lost lease
//! records surface as `LeaseInvalid` → re-acquire, and replays of
//! applied-but-unacked batches are absorbed by the `device_seq` fence.

use homestead_core::clock::{ManualClock, Timestamp};
use homestead_core::key::Key;
use homestead_core::lease::{LeaseMode, LeaseRef};
use homestead_core::messages::{
    AcquireRequest, GetRequest, KernelError, LeaseSpec, PutBatchRequest, PutEntry,
};
use homestead_core::space::{Space as _, SpaceError, SpaceId};
use homestead_core::tag::{DeviceId, DeviceSeq, Value, Ver};
use homestead_server::actor::{SpaceActor, SpaceHandle};
use homestead_sim::check;
use homestead_sim::exec::SimExecutor;
use homestead_sim::store::{FaultConfig, SimStore};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::cell::{Cell, RefCell};
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

const SPACE: SpaceId = SpaceId([3; 16]);
const DEVICES: u8 = 2;
const PHASES: usize = 4;
const PUTS_PER_PHASE: u64 = 8;

const FAULTS: FaultConfig = FaultConfig {
    error_rate: 0.02,
    flush_rate: 0.25,
    max_latency_yields: 3,
};

fn dev(d: u8) -> DeviceId {
    DeviceId([d + 1; 16])
}

fn prefix(d: u8) -> Key {
    Key::from_bytes([format!("d{d}").into_bytes()]).unwrap()
}

fn user_key(d: u8, seq: u64) -> Key {
    Key::from_bytes([format!("d{d}").into_bytes(), format!("k{seq:06}").into_bytes()]).unwrap()
}

fn value(d: u8, seq: u64) -> Vec<u8> {
    format!("v{d}-{seq}").into_bytes()
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Ack {
    device: u8,
    device_seq: u64,
    admission_seq: u64,
}

/// Which recovery paths a run actually exercised — the seeds must
/// collectively prove the sim tortures, not merely passes.
#[derive(Clone, Copy, Debug, Default)]
struct Coverage {
    /// Lease record lost in a crash → `LeaseInvalid` → re-acquire.
    lease_invalid: u32,
    /// Applied-but-unacked batch survived → `DeviceSeqRegression` fence.
    seq_regression: u32,
    /// Acked batches lost to a crash (pruned by the prefix oracle).
    acked_writes_lost: u32,
    /// Injected storage faults / dead actors observed as `Unavailable`.
    unavailable: u32,
}

/// Per-device state that survives crashes — the client's "disk".
#[derive(Clone)]
struct DeviceState {
    lease: Rc<RefCell<Option<LeaseRef>>>,
    next_seq: Rc<Cell<u64>>,
}

/// One client incarnation: writes unique keys under its own prefix until
/// its budget or the space dies. Every error path is a real protocol path.
async fn client(
    handle: SpaceHandle,
    d: u8,
    state: DeviceState,
    acks: Rc<RefCell<Vec<Ack>>>,
    coverage: Rc<RefCell<Coverage>>,
) {
    let mut completed = 0u64;
    let mut attempts = 0u32;
    while completed < PUTS_PER_PHASE && attempts < 10 * PUTS_PER_PHASE as u32 {
        attempts += 1;

        if state.lease.borrow().is_none() {
            // Steal back our own orphaned grant, if a previous incarnation's
            // lease record survived the crash (single-active-device pattern).
            let req = AcquireRequest {
                device: dev(d),
                steal: true,
                specs: vec![LeaseSpec {
                    prefix: prefix(d),
                    mode: LeaseMode::Write,
                    ttl: Duration::from_secs(60),
                    stealable: true,
                }],
            };
            match handle.acquire(req).await {
                Ok(resp) => {
                    *state.lease.borrow_mut() = Some(LeaseRef {
                        id: resp.leases[0].id,
                        epoch: resp.leases[0].epoch,
                    });
                }
                Err(SpaceError::Unavailable { .. }) => {
                    coverage.borrow_mut().unavailable += 1;
                    return;
                }
                Err(SpaceError::Kernel(err)) => {
                    panic!("stealable self-handoff can never contend: {err:?}")
                }
            }
            continue;
        }

        let seq = state.next_seq.get();
        let lease = state.lease.borrow().unwrap();
        let req = PutBatchRequest {
            device: dev(d),
            device_seq: DeviceSeq(seq),
            leases: vec![lease],
            entries: vec![PutEntry {
                key: user_key(d, seq),
                value: Value::Present(value(d, seq)),
                ver: Ver(1),
            }],
        };
        match handle.put_batch(req).await {
            Ok(resp) => {
                acks.borrow_mut().push(Ack {
                    device: d,
                    device_seq: seq,
                    admission_seq: resp.admission_seq.0,
                });
                state.next_seq.set(seq + 1);
                completed += 1;
            }
            // Our lease record died with the crash (or was stolen by our
            // own next incarnation — not in this workload): re-acquire.
            Err(SpaceError::Kernel(KernelError::LeaseInvalid { .. })) => {
                coverage.borrow_mut().lease_invalid += 1;
                *state.lease.borrow_mut() = None;
            }
            // A previous incarnation's batch was applied but never acked;
            // the replay fence tells us where the server actually is.
            Err(SpaceError::Kernel(KernelError::DeviceSeqRegression { current, .. })) => {
                coverage.borrow_mut().seq_regression += 1;
                state.next_seq.set(current.0 + 1);
            }
            Err(SpaceError::Unavailable { .. }) => {
                coverage.borrow_mut().unavailable += 1;
                return;
            }
            Err(SpaceError::Kernel(err)) => panic!("unexpected kernel rejection: {err:?}"),
        }
    }
}

/// Runs one seeded life: PHASES-1 crash-terminated phases, one clean one.
/// Returns the full ack trace (the behavioral fingerprint for determinism)
/// and the recovery-path coverage it hit.
fn run_seed(seed: u64) -> (Vec<Ack>, Coverage) {
    let mut master = StdRng::seed_from_u64(seed);
    let store = SimStore::new(master.random(), FAULTS);
    let clock = Arc::new(ManualClock::new(Timestamp(0)));
    let acks: Rc<RefCell<Vec<Ack>>> = Rc::new(RefCell::new(Vec::new()));
    let coverage = Rc::new(RefCell::new(Coverage::default()));
    let devices: Vec<DeviceState> = (0..DEVICES)
        .map(|_| DeviceState {
            lease: Rc::new(RefCell::new(None)),
            next_seq: Rc::new(Cell::new(1)),
        })
        .collect();

    for phase in 0..PHASES {
        store.set_config(FAULTS);
        let mut exec = SimExecutor::new(master.random());
        let (actor, handle) = SpaceActor::new(
            SPACE,
            Arc::new(store.clone()),
            Arc::clone(&clock),
        );
        let actor_task = exec.spawn(actor.run());
        let client_tasks: Vec<_> = (0..DEVICES)
            .map(|d| {
                exec.spawn(client(
                    handle.clone(),
                    d,
                    devices[d as usize].clone(),
                    Rc::clone(&acks),
                    Rc::clone(&coverage),
                ))
            })
            .collect();
        drop(handle);

        let crash = phase != PHASES - 1;
        if crash {
            let steps = master.random_range(30..400);
            for _ in 0..steps {
                if !exec.step() {
                    break;
                }
                if master.random_bool(0.05) {
                    clock.advance(Duration::from_millis(master.random_range(1..10)));
                }
            }
            // Power loss kills everything mid-flight: the actor, and the
            // clients' in-flight calls — a reply the server sent but the
            // device never processed is exactly how applied-but-unacked
            // batches (and thus the replay fence) happen. Client *state*
            // (lease slot, next seq, ack log) survives: that is the
            // device's disk.
            exec.cancel(actor_task);
            for task in client_tasks {
                exec.cancel(task);
            }
            store.crash();
            exec.run_until_stalled();
        } else {
            exec.run_until_stalled();
        }

        // -- oracles over the surviving state --------------------------------
        store.set_config(FaultConfig::NONE);
        let audit = check::audit(SPACE, &store);

        // Prefix durability: acked ≤ high-water survived intact; acked
        // above it was lost whole. Lost acks are pruned (their seqs may be
        // reused by the recovered counter).
        let high = audit.max_admission_seq;
        acks.borrow_mut().retain(|ack| {
            let record = audit.data.get(&user_key(ack.device, ack.device_seq));
            if ack.admission_seq <= high {
                let record = record.unwrap_or_else(|| {
                    panic!("acked batch below high water lost: {ack:?} (seed {seed})")
                });
                assert_eq!(
                    record.value,
                    Value::Present(value(ack.device, ack.device_seq)),
                    "acked value corrupted: {ack:?} (seed {seed})"
                );
                true
            } else {
                assert!(
                    record.is_none(),
                    "batch above high water partially survived: {ack:?} (seed {seed})"
                );
                coverage.borrow_mut().acked_writes_lost += 1;
                false
            }
        });
    }

    let trace = acks.borrow().clone();
    assert!(
        !trace.is_empty(),
        "seed {seed} made no progress at all — faults drowned the workload"
    );
    (trace, *coverage.borrow())
}

#[test]
fn crash_torture_seeds_hold_invariants() {
    let mut total = Coverage::default();
    for seed in 0..100 {
        let (_, coverage) = run_seed(seed);
        total.lease_invalid += coverage.lease_invalid;
        total.seq_regression += coverage.seq_regression;
        total.acked_writes_lost += coverage.acked_writes_lost;
        total.unavailable += coverage.unavailable;
    }
    println!("coverage across seeds: {total:?}");
    // The swarm must actually torture: every recovery path fires somewhere
    // across the seeds, or the workload has gone soft.
    assert!(total.lease_invalid > 0, "no lost-lease recoveries: {total:?}");
    assert!(total.seq_regression > 0, "no replay-fence hits: {total:?}");
    assert!(total.acked_writes_lost > 0, "no acked-write loss: {total:?}");
    assert!(total.unavailable > 0, "no unavailability observed: {total:?}");
}

#[test]
fn identical_seeds_replay_identically() {
    for seed in [0, 7, 42] {
        assert_eq!(
            run_seed(seed).0,
            run_seed(seed).0,
            "seed {seed} diverged on replay"
        );
    }
}

/// The sim runs production types end to end: after all the torture, a
/// plain read through the handle still works.
#[test]
fn recovered_space_still_serves_reads() {
    run_seed(1); // sanity that the harness itself is healthy
    let store = SimStore::new(99, FaultConfig::NONE);
    let clock = Arc::new(ManualClock::new(Timestamp(0)));
    let mut exec = SimExecutor::new(0);
    let (actor, handle) = SpaceActor::new(SPACE, Arc::new(store), clock);
    exec.spawn(actor.run());

    let result = Rc::new(RefCell::new(None));
    let out = Rc::clone(&result);
    exec.spawn(async move {
        let got = handle
            .get(GetRequest { keys: vec![user_key(0, 1)] })
            .await
            .unwrap();
        *out.borrow_mut() = Some(got.entries[0].is_none());
    });
    exec.run_until_stalled();
    assert_eq!(*result.borrow(), Some(true), "fresh store has no data");
}
