//! Steal races, contended handoff, and zombie writers on one shared prefix
//! — under seeded schedules, storage faults, lease expiry, and crashes.
//!
//! Three devices fight over a single stealable write lease (the
//! single-active-device pattern) guarding one counter key. A holder
//! increments the counter read-modify-write style; rivals either steal the
//! lease outright or ask politely and get `Contended`; holders sometimes
//! release voluntarily (clean handoff) and sometimes just lose the lease —
//! to a steal, to TTL expiry (the clock is cranked hard enough to expire
//! tenures), or to a crash. Every stale writer must be *fenced*, never
//! admitted.
//!
//! **The mutual-exclusion oracle:** each increment writes `read value + 1`
//! while exclusively holding the lease, so across every device and every
//! crash, acknowledged counter values must be strictly increasing in
//! admission order. A single lost update — two writers basing on the same
//! read — shows up as a duplicate or regressing value. Ver regressions and
//! epoch fences firing where mutual exclusion should have prevented them
//! panic outright.

use homebase_core::clock::{ManualClock, Timestamp};
use homebase_core::key::Key;
use homebase_core::lease::{LeaseMode, LeaseRef};
use homebase_core::messages::{
    AcquireRequest, GetRequest, KernelError, LeaseSpec, PutBatchRequest, PutEntry, ReleaseRequest,
};
use homebase_core::space::{Space as _, SpaceError, SpaceId};
use homebase_core::tag::{DeviceId, DeviceSeq, Value, Ver};
use homebase_server::actor::{SpaceActor, SpaceHandle};
use homebase_sim::check;
use homebase_sim::exec::SimExecutor;
use homebase_sim::seeds;
use homebase_sim::store::{FaultConfig, SimStore};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::cell::{Cell, RefCell};
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

const SPACE: SpaceId = SpaceId([4; 16]);
const DEVICES: u8 = 3;
const PHASES: usize = 4;
const ATTEMPTS_PER_PHASE: u32 = 60;

const FAULTS: FaultConfig = FaultConfig {
    error_rate: 0.01,
    flush_rate: 0.25,
    max_latency_yields: 3,
};

fn dev(d: u8) -> DeviceId {
    DeviceId([d + 1; 16])
}

fn shared_prefix() -> Key {
    Key::from_bytes([b"acct".to_vec()]).unwrap()
}

fn counter_key() -> Key {
    Key::from_bytes([b"acct".to_vec(), b"n".to_vec()]).unwrap()
}

fn encode(n: u64) -> Vec<u8> {
    n.to_be_bytes().to_vec()
}

fn decode(bytes: &[u8]) -> u64 {
    u64::from_be_bytes(bytes.try_into().expect("counter width"))
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Ack {
    device: u8,
    value: u64,
    admission_seq: u64,
}

#[derive(Clone, Copy, Debug, Default)]
struct Coverage {
    steals: u32,
    contended: u32,
    /// Stale holder rejected (stolen, expired, or crash-lost lease record).
    fenced: u32,
    released: u32,
}

/// Per-device state that survives crashes.
#[derive(Clone)]
struct DeviceState {
    lease: Rc<RefCell<Option<LeaseRef>>>,
    next_seq: Rc<Cell<u64>>,
    rng_seed: u64,
}

async fn client(
    handle: SpaceHandle,
    d: u8,
    state: DeviceState,
    acks: Rc<RefCell<Vec<Ack>>>,
    coverage: Rc<RefCell<Coverage>>,
) {
    let mut rng = StdRng::seed_from_u64(state.rng_seed);
    for _ in 0..ATTEMPTS_PER_PHASE {
        let held = *state.lease.borrow();
        let Some(lease) = held else {
            // Rival move: usually a steal, sometimes a polite ask that
            // exercises the Contended path.
            let steal = rng.random_bool(0.7);
            let req = AcquireRequest {
                device: dev(d),
                steal,
                specs: vec![LeaseSpec {
                    prefix: shared_prefix(),
                    mode: LeaseMode::Write,
                    ttl: Duration::from_millis(200),
                    stealable: true,
                }],
            };
            match handle.acquire(req).await {
                Ok(resp) => {
                    *state.lease.borrow_mut() = Some(LeaseRef {
                        id: resp.leases[0].id,
                        epoch: resp.leases[0].epoch,
                    });
                    if steal {
                        coverage.borrow_mut().steals += 1;
                    }
                }
                Err(SpaceError::Kernel(KernelError::Contended { .. })) => {
                    assert!(!steal, "steal over all-stealable blockers can never contend");
                    coverage.borrow_mut().contended += 1;
                }
                Err(SpaceError::Unavailable { .. }) => return,
                Err(err) => panic!("unexpected acquire failure: {err:?}"),
            }
            continue;
        };

        // Holder move: read-modify-write the counter under the lease.
        let current = match handle.get(GetRequest { keys: vec![counter_key()] }).await {
            Ok(resp) => resp.entries[0]
                .as_ref()
                .map(|e| match &e.value {
                    Value::Present(v) => (decode(v), e.tag.ver.0),
                    Value::Absent => panic!("tombstone leaked out of get"),
                })
                .unwrap_or((0, 0)),
            Err(SpaceError::Unavailable { .. }) => return,
            Err(err) => panic!("unexpected get failure: {err:?}"),
        };

        let seq = state.next_seq.get();
        let req = PutBatchRequest {
            device: dev(d),
            device_seq: DeviceSeq(seq),
            leases: vec![lease],
            entries: vec![PutEntry {
                key: counter_key(),
                value: Value::Present(encode(current.0 + 1)),
                ver: Ver(current.1 + 1),
            }],
        };
        match handle.put_batch(req).await {
            Ok(resp) => {
                acks.borrow_mut().push(Ack {
                    device: d,
                    value: current.0 + 1,
                    admission_seq: resp.admission_seq.0,
                });
                state.next_seq.set(seq + 1);

                // Sometimes hand the lease off cleanly.
                if rng.random_bool(0.25) {
                    match handle
                        .release(ReleaseRequest { device: dev(d), leases: vec![lease.id] })
                        .await
                    {
                        Ok(_) => {
                            *state.lease.borrow_mut() = None;
                            coverage.borrow_mut().released += 1;
                        }
                        Err(SpaceError::Unavailable { .. }) => return,
                        Err(err) => panic!("unexpected release failure: {err:?}"),
                    }
                }
            }
            // The zombie path: our lease was stolen, expired, or its record
            // died with a crash. The kernel must fence us — and does.
            Err(SpaceError::Kernel(KernelError::LeaseInvalid { .. })) => {
                coverage.borrow_mut().fenced += 1;
                *state.lease.borrow_mut() = None;
            }
            // An earlier incarnation's batch landed without an ack.
            Err(SpaceError::Kernel(KernelError::DeviceSeqRegression { current, .. })) => {
                state.next_seq.set(current.0 + 1);
            }
            Err(SpaceError::Unavailable { .. }) => return,
            // Under correct mutual exclusion these are unreachable: a ver
            // regression means two holders based on one read; a stale-epoch
            // fence means an id survived a steal.
            Err(SpaceError::Kernel(err)) => {
                panic!("mutual exclusion breached: {err:?} at device {d}")
            }
        }
    }
}

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
            rng_seed: master.random(),
        })
        .collect();

    for phase in 0..PHASES {
        store.set_config(FAULTS);
        let mut exec = SimExecutor::new(master.random());
        let (actor, handle) = SpaceActor::new(SPACE, Arc::new(store.clone()), Arc::clone(&clock));
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

        if phase != PHASES - 1 {
            let steps = master.random_range(50..600);
            for _ in 0..steps {
                if !exec.step() {
                    break;
                }
                // Cranked hard enough that 200ms tenures really expire.
                if master.random_bool(0.1) {
                    clock.advance(Duration::from_millis(master.random_range(1..30)));
                }
            }
            exec.cancel(actor_task);
            for task in client_tasks {
                exec.cancel(task);
            }
            store.crash();
            exec.run_until_stalled();
        } else {
            exec.run_until_stalled();
        }

        // -- oracles ---------------------------------------------------------
        store.set_config(FaultConfig::NONE);
        let audit = check::audit(SPACE, &store);
        let high = audit.max_admission_seq;

        // Prefix durability on an overwritten key: the surviving record must
        // be at least as new as the newest surviving ack, and agree with it
        // when they coincide.
        acks.borrow_mut().retain(|ack| ack.admission_seq <= high);
        if let Some(best) = acks.borrow().iter().max_by_key(|a| a.admission_seq) {
            let record = audit
                .data
                .get(&counter_key())
                .unwrap_or_else(|| panic!("acked counter vanished (seed {seed})"));
            assert!(
                record.tag.admission_seq.0 >= best.admission_seq,
                "counter record older than a surviving ack (seed {seed})"
            );
            if record.tag.admission_seq.0 == best.admission_seq {
                assert_eq!(
                    record.value,
                    Value::Present(encode(best.value)),
                    "acked counter value corrupted (seed {seed})"
                );
            }
        }

        // Mutual exclusion: acked values strictly increase in admission
        // order, across devices, steals, expiries, and crashes.
        let acks = acks.borrow();
        let mut ordered: Vec<&Ack> = acks.iter().collect();
        ordered.sort_by_key(|a| a.admission_seq);
        for pair in ordered.windows(2) {
            assert!(
                pair[0].value < pair[1].value,
                "lost update: {:?} then {:?} (seed {seed})",
                pair[0],
                pair[1]
            );
        }
    }

    let trace = acks.borrow().clone();
    assert!(!trace.is_empty(), "seed {seed} made no progress");
    (trace, *coverage.borrow())
}

#[test]
fn steal_torture_seeds_hold_mutual_exclusion() {
    let mut total = Coverage::default();
    for seed in seeds::torture_seeds() {
        let (_, coverage) = run_seed(seed);
        total.steals += coverage.steals;
        total.contended += coverage.contended;
        total.fenced += coverage.fenced;
        total.released += coverage.released;
    }
    println!("coverage across seeds: {total:?}");
    assert!(total.steals > 0, "no steals happened: {total:?}");
    assert!(total.contended > 0, "no contention observed: {total:?}");
    assert!(total.fenced > 0, "no zombie writer was fenced: {total:?}");
    assert!(total.released > 0, "no voluntary handoff: {total:?}");
}

#[test]
fn steal_torture_replays_identically() {
    for seed in [3, 11] {
        assert_eq!(
            run_seed(seed).0,
            run_seed(seed).0,
            "seed {seed} diverged on replay"
        );
    }
}
