//! The client `Session` under torture: local-deadline discipline, engine
//! bookkeeping, and the device_seq replay fence driving the real kernel
//! under seeded schedules, storage faults, lease expiry, contention,
//! crashes, and dropped acks.
//!
//! Same battlefield as `steal_torture` — devices fighting over one write
//! lease guarding one read-modify-write counter — but the devices now speak
//! through [`Session`] instead of hand-rolled bookkeeping, so what's on
//! trial is the *client* half of the protocol. Two rigs:
//!
//! 1. **Crashy** ([`session_torture_holds_exclusion_across_crashes`]):
//!    phases end in `kill -9` (tasks dropped mid-await, store loses its
//!    unflushed suffix). Sessions run with a zero retry budget and resume
//!    each phase from a mirror updated only on acknowledgment — so a batch
//!    admitted right before the crash forces the resumed session through
//!    the `DeviceSeqRegression` resync path. Oracle: acked counter values
//!    strictly increase in admission order, across every device, expiry,
//!    resync, and crash — one lost update shows up as a duplicate.
//!
//! 2. **Replay fence** ([`replay_fence_is_exactly_once`]): no crashes, but
//!    a transport that drops put acks *after* admission. Sessions retry
//!    blindly; the fence must convert every replayed batch into
//!    `AlreadyApplied` rather than a second application. Oracle is sharp:
//!    under mutual exclusion every admitted batch increments by exactly
//!    one, so the final counter must equal the admission high water, and
//!    every `Admitted` ack's value must equal its admission seq.

use homebase::session::{PutError, PutOutcome, Session};
use homebase_core::clock::{ManualClock, Timestamp};
use homebase_core::key::Key;
use homebase_core::lease::{LeaseId, LeaseMode};
use homebase_core::messages::{
    AcquireRequest, AcquireResponse, GetRequest, GetResponse, KernelError, LeaseSpec, ListRequest,
    ListResponse, PutBatchRequest, PutBatchResponse, ReadAtRequest, ReadAtResponse,
    ReleaseRequest, ReleaseResponse, RenewRequest, RenewResponse,
};
use homebase_core::space::{Space, SpaceError, SpaceId};
use homebase_core::tag::{DeviceId, DeviceSeq, Value, Ver};
use homebase_server::actor::{SpaceActor, SpaceHandle};
use homebase_sim::check;
use homebase_sim::exec::SimExecutor;
use homebase_sim::seeds;
use homebase_sim::store::{FaultConfig, SimStore};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::cell::{Cell, RefCell};
use std::collections::BTreeSet;
use std::rc::Rc;
use std::sync::{Arc, Mutex};
use std::time::Duration;

const SPACE: SpaceId = SpaceId([6; 16]);
const DEVICES: u8 = 3;
const PHASES: usize = 4;
const ATTEMPTS_PER_PHASE: u32 = 60;
const TTL: Duration = Duration::from_millis(200);

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

fn write_spec() -> LeaseSpec {
    LeaseSpec {
        prefix: shared_prefix(),
        mode: LeaseMode::Write,
        ttl: TTL,
        stealable: false,
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Ack {
    device: u8,
    value: u64,
    /// `None` for `AlreadyApplied`: the batch landed but its admission
    /// point was lost with the ack.
    admission_seq: Option<u64>,
}

#[derive(Clone, Copy, Debug, Default)]
struct Coverage {
    contended: u32,
    renewed: u32,
    /// Heartbeat reported a lease the server no longer holds.
    expired: u32,
    /// Put refused locally: the deadline fired between acquire and use.
    local_expiry: u32,
    /// Put presented a locally-live ref the server had already expired
    /// (the in-flight window) — rejected safely, hold dropped.
    lease_lost: u32,
    /// A prior incarnation's batch landed unacked; the session resynced.
    resynced: u32,
    /// The replay fence converted a retried batch into `AlreadyApplied`.
    already_applied: u32,
    released: u32,
}

/// Per-device state that survives crashes: the durable seq mirror.
#[derive(Clone)]
struct DeviceState {
    /// Updated on acknowledgment (post-ack persistence), so a crash after
    /// admission but before the ack leaves the mirror stale — exactly the
    /// hole the resync path exists for.
    next_seq: Rc<Cell<u64>>,
    rng_seed: u64,
}

/// One device's life within a phase: acquire → heartbeat/increment →
/// sometimes release; give up on a final `Unavailable` (the phase is
/// dying). Everything stateful goes through the session.
async fn device_loop<S: Space>(
    mut session: Session<S, ManualClock>,
    d: u8,
    state: DeviceState,
    acks: Rc<RefCell<Vec<Ack>>>,
    coverage: Rc<RefCell<Coverage>>,
) {
    let mut rng = StdRng::seed_from_u64(state.rng_seed);
    let mut held: Option<LeaseId> = None;
    for _ in 0..ATTEMPTS_PER_PHASE {
        // Forget holds the session has already dropped (heartbeat-invalid
        // or refused on a put).
        if let Some(id) = held
            && session.lease(id).is_none()
        {
            held = None;
        }

        let Some(id) = held else {
            match session.acquire(vec![write_spec()], false).await {
                Ok(acq) => held = Some(acq.leases[0]),
                Err(SpaceError::Kernel(KernelError::Contended { .. })) => {
                    coverage.borrow_mut().contended += 1;
                }
                Err(SpaceError::Unavailable { .. }) => return,
                Err(err) => panic!("unexpected acquire failure: {err:?}"),
            }
            continue;
        };

        // Crank the renewal mechanism sometimes — the caller-driven
        // heartbeat keeping tenures alive across the clock cranks.
        if rng.random_bool(0.3) {
            match session.heartbeat().await {
                Ok(report) => {
                    let mut cov = coverage.borrow_mut();
                    cov.renewed += report.renewed.len() as u32;
                    cov.expired += report.invalid.len() as u32;
                }
                Err(SpaceError::Unavailable { .. }) => return,
                Err(err) => panic!("unexpected renew failure: {err:?}"),
            }
            continue;
        }

        // Read-modify-write the counter under the lease.
        let current = match session.get(vec![counter_key()]).await {
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

        let result = session
            .put(vec![homebase_core::messages::PutEntry {
                key: counter_key(),
                value: Value::Present(encode(current.0 + 1)),
                ver: Ver(current.1 + 1),
            }])
            .await;
        // Mirror the seq on every acknowledged outcome (including resync):
        // this is the "persist after ack" discipline whose gap the crashy
        // rig deliberately exploits.
        state.next_seq.set(session.next_seq().0);
        match result {
            Ok(PutOutcome::Admitted(seq)) => {
                acks.borrow_mut().push(Ack {
                    device: d,
                    value: current.0 + 1,
                    admission_seq: Some(seq.0),
                });
                // Demand-driven handoff sometimes.
                if rng.random_bool(0.2) {
                    match session.release(&[id]).await {
                        Ok(()) => {
                            coverage.borrow_mut().released += 1;
                            held = None;
                        }
                        Err(SpaceError::Unavailable { .. }) => return,
                        Err(err) => panic!("unexpected release failure: {err:?}"),
                    }
                }
            }
            Ok(PutOutcome::AlreadyApplied) => {
                acks.borrow_mut().push(Ack {
                    device: d,
                    value: current.0 + 1,
                    admission_seq: None,
                });
                coverage.borrow_mut().already_applied += 1;
            }
            Err(PutError::NotCovered { .. }) => {
                // The local deadline fired mid-loop (the clock cranked
                // between acquire and use). Heartbeat decides whether the
                // hold resurrects or is reported dead.
                coverage.borrow_mut().local_expiry += 1;
                match session.heartbeat().await {
                    Ok(report) => {
                        let mut cov = coverage.borrow_mut();
                        cov.renewed += report.renewed.len() as u32;
                        cov.expired += report.invalid.len() as u32;
                    }
                    Err(SpaceError::Unavailable { .. }) => return,
                    Err(err) => panic!("unexpected renew failure: {err:?}"),
                }
            }
            Err(PutError::Space(SpaceError::Kernel(KernelError::LeaseInvalid { .. }))) => {
                // Locally live at send, dead at dequeue — rejected safely.
                coverage.borrow_mut().lease_lost += 1;
            }
            Err(PutError::Space(SpaceError::Kernel(
                KernelError::DeviceSeqRegression { .. },
            ))) => {
                // A dead incarnation's batch landed without its ack; the
                // session already resynced the stream.
                coverage.borrow_mut().resynced += 1;
            }
            Err(PutError::Space(SpaceError::Unavailable { .. })) => return,
            // Under session discipline everything else is a breached
            // invariant: Fenced (an id survived a re-grant), VerRegression
            // (two holders based on one read), or a server-side NotCovered
            // (the session presented a non-covering ref).
            Err(err) => panic!("mutual exclusion breached at device {d}: {err}"),
        }
    }
}

fn accumulate(total: &mut Coverage, phase: Coverage) {
    total.contended += phase.contended;
    total.renewed += phase.renewed;
    total.expired += phase.expired;
    total.local_expiry += phase.local_expiry;
    total.lease_lost += phase.lease_lost;
    total.resynced += phase.resynced;
    total.already_applied += phase.already_applied;
    total.released += phase.released;
}

// ---------------------------------------------------------------------------
// Rig 1: crashy phases

fn run_crashy_seed(seed: u64) -> (Vec<Ack>, Coverage) {
    let mut master = StdRng::seed_from_u64(seed);
    let store = SimStore::new(master.random(), FAULTS);
    let clock = Arc::new(ManualClock::new(Timestamp(0)));
    let acks: Rc<RefCell<Vec<Ack>>> = Rc::new(RefCell::new(Vec::new()));
    let coverage = Rc::new(RefCell::new(Coverage::default()));
    let devices: Vec<DeviceState> = (0..DEVICES)
        .map(|_| DeviceState {
            next_seq: Rc::new(Cell::new(1)),
            rng_seed: master.random(),
        })
        .collect();

    for phase in 0..PHASES {
        store.set_config(FAULTS);
        let mut exec = SimExecutor::new(master.random());
        let (actor, handle) = SpaceActor::new(SPACE, Arc::new(store.clone()), Arc::clone(&clock));
        exec.spawn(actor.run());
        let client_tasks: Vec<_> = (0..DEVICES)
            .map(|d| {
                let state = devices[d as usize].clone();
                // Zero retry budget: every Unavailable surfaces, so
                // AlreadyApplied is unreachable and admitted-unacked
                // batches must flow through the resync path instead.
                let session = Session::resume(
                    handle.clone(),
                    Arc::clone(&clock),
                    dev(d),
                    DeviceSeq(state.next_seq.get()),
                )
                .with_retry_budget(0);
                exec.spawn(device_loop(
                    session,
                    d,
                    state,
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
            // Clients die first; the server outlives them and drains
            // whatever they had in flight — those batches are admitted
            // with nobody left to hear the ack, which is exactly the
            // orphan the resync path exists for. (The actor task ends on
            // its own once the dead sessions' handles drop.)
            for task in client_tasks {
                exec.cancel(task);
            }
            exec.run_until_stalled();
            // Half the crashes hit after the tail reached durability,
            // half lose an unflushed suffix — both recoveries stay covered.
            if master.random_bool(0.5) {
                store.flush();
            }
            store.crash();
        } else {
            // The final phase runs to completion, but time must keep
            // moving: leases here are non-stealable, so a fault-killed
            // holder's tenure frees up only by TTL expiry — on a frozen
            // clock the survivors would spin Contended forever.
            while exec.step() {
                if master.random_bool(0.1) {
                    clock.advance(Duration::from_millis(master.random_range(1..30)));
                }
            }
        }

        // -- oracles ---------------------------------------------------------
        store.set_config(FaultConfig::NONE);
        let audit = check::audit(SPACE, &store);
        let high = audit.max_admission_seq;

        // Zero-budget sessions cannot produce AlreadyApplied, so every ack
        // carries its admission point; durability-filter against the
        // surviving prefix.
        acks.borrow_mut().retain(|ack| {
            ack.admission_seq
                .expect("zero-retry rig cannot produce AlreadyApplied")
                <= high
        });

        // Prefix durability on the overwritten counter: the surviving
        // record is at least as new as the newest surviving ack, and
        // agrees with it when they coincide.
        if let Some(best) = acks
            .borrow()
            .iter()
            .max_by_key(|a| a.admission_seq.unwrap())
        {
            let record = audit
                .data
                .get(&counter_key())
                .unwrap_or_else(|| panic!("acked counter vanished (seed {seed})"));
            assert!(
                record.tag.admission_seq.0 >= best.admission_seq.unwrap(),
                "counter record older than a surviving ack (seed {seed})"
            );
            if record.tag.admission_seq.0 == best.admission_seq.unwrap() {
                assert_eq!(
                    record.value,
                    Value::Present(encode(best.value)),
                    "acked counter value corrupted (seed {seed})"
                );
            }
        }

        // Mutual exclusion under session discipline: acked values strictly
        // increase in admission order, across devices, expiries, resyncs,
        // and crashes.
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
fn session_torture_holds_exclusion_across_crashes() {
    let mut total = Coverage::default();
    for seed in seeds::torture_seeds() {
        let (_, coverage) = run_crashy_seed(seed);
        accumulate(&mut total, coverage);
    }
    println!("crashy coverage across seeds: {total:?}");
    assert!(total.contended > 0, "no contention observed: {total:?}");
    assert!(total.renewed > 0, "no heartbeat renewed anything: {total:?}");
    assert!(total.released > 0, "no voluntary handoff: {total:?}");
    assert!(total.expired > 0, "no server-side expiry reported: {total:?}");
    assert!(total.local_expiry > 0, "local deadline never gated a write: {total:?}");
    if seeds::torture_coverage_enforced() {
        assert!(
            total.resynced > 0,
            "no crash-orphaned batch exercised the resync path: {total:?}"
        );
    }
}

#[test]
fn session_torture_replays_identically() {
    for seed in [3, 11] {
        assert_eq!(
            run_crashy_seed(seed).0,
            run_crashy_seed(seed).0,
            "seed {seed} diverged on replay"
        );
    }
}

// ---------------------------------------------------------------------------
// Rig 2: dropped acks, exactly-once

/// Transport that admits the batch and then loses the reply — the network
/// failure the replay fence exists for. Reads and lease verbs pass through.
#[derive(Clone)]
struct AckDrop {
    inner: SpaceHandle,
    rng: Arc<Mutex<StdRng>>,
    drop_rate: f64,
}

impl AckDrop {
    fn drop_this_ack(&self) -> bool {
        self.rng.lock().unwrap().random_bool(self.drop_rate)
    }
}

impl Space for AckDrop {
    fn acquire(
        &self,
        req: AcquireRequest,
    ) -> impl Future<Output = Result<AcquireResponse, SpaceError>> + Send {
        self.inner.acquire(req)
    }

    fn renew(
        &self,
        req: RenewRequest,
    ) -> impl Future<Output = Result<RenewResponse, SpaceError>> + Send {
        self.inner.renew(req)
    }

    fn release(
        &self,
        req: ReleaseRequest,
    ) -> impl Future<Output = Result<ReleaseResponse, SpaceError>> + Send {
        self.inner.release(req)
    }

    async fn put_batch(&self, req: PutBatchRequest) -> Result<PutBatchResponse, SpaceError> {
        let resp = self.inner.put_batch(req).await?;
        if self.drop_this_ack() {
            return Err(SpaceError::unavailable("injected: ack dropped post-admission"));
        }
        Ok(resp)
    }

    fn get(
        &self,
        req: GetRequest,
    ) -> impl Future<Output = Result<GetResponse, SpaceError>> + Send {
        self.inner.get(req)
    }

    fn list(
        &self,
        req: ListRequest,
    ) -> impl Future<Output = Result<ListResponse, SpaceError>> + Send {
        self.inner.list(req)
    }

    fn read_at(
        &self,
        req: ReadAtRequest,
    ) -> impl Future<Output = Result<ReadAtResponse, SpaceError>> + Send {
        self.inner.read_at(req)
    }
}

fn run_replay_seed(seed: u64) -> (Vec<Ack>, Coverage) {
    let mut master = StdRng::seed_from_u64(seed);
    // Fault-free store: the only injected failure is the lost ack, so
    // every Unavailable a session sees hides an admitted batch.
    let store = SimStore::new(master.random(), FaultConfig::NONE);
    let clock = Arc::new(ManualClock::new(Timestamp(0)));
    let acks: Rc<RefCell<Vec<Ack>>> = Rc::new(RefCell::new(Vec::new()));
    let coverage = Rc::new(RefCell::new(Coverage::default()));

    let mut exec = SimExecutor::new(master.random());
    let (actor, handle) = SpaceActor::new(SPACE, Arc::new(store.clone()), Arc::clone(&clock));
    exec.spawn(actor.run());
    let transport = AckDrop {
        inner: handle,
        rng: Arc::new(Mutex::new(StdRng::seed_from_u64(master.random()))),
        drop_rate: 0.2,
    };
    for d in 0..DEVICES {
        let state = DeviceState {
            next_seq: Rc::new(Cell::new(1)),
            rng_seed: master.random(),
        };
        let session = Session::new(transport.clone(), Arc::clone(&clock), dev(d));
        exec.spawn(device_loop(
            session,
            d,
            state,
            Rc::clone(&acks),
            Rc::clone(&coverage),
        ));
    }
    drop(transport);

    while exec.step() {
        if master.random_bool(0.1) {
            clock.advance(Duration::from_millis(master.random_range(1..30)));
        }
    }

    // -- oracles ---------------------------------------------------------
    let audit = check::audit(SPACE, &store);
    let final_value = audit
        .data
        .get(&counter_key())
        .map(|record| match &record.value {
            Value::Present(v) => decode(v),
            Value::Absent => panic!("counter tombstoned (seed {seed})"),
        })
        .unwrap_or(0);

    // Exactly-once, sharply: every admitted batch is one increment, so a
    // replayed batch applied twice would push the counter past the
    // admission high water (and a lost one would leave it short).
    assert_eq!(
        final_value, audit.max_admission_seq,
        "counter diverged from admitted batch count (seed {seed})"
    );

    let trace = acks.borrow().clone();
    let mut seen = BTreeSet::new();
    for ack in &trace {
        assert!(
            ack.value <= final_value,
            "acked value {} beyond final counter {final_value} (seed {seed})",
            ack.value
        );
        assert!(
            seen.insert(ack.value),
            "value {} acked twice — a double-apply or lost update (seed {seed})",
            ack.value
        );
        // Value k is written by admission k: the RMW chain under mutual
        // exclusion. Holds for every ack whose admission point survived
        // the dropped-ack fog.
        if let Some(seq) = ack.admission_seq {
            assert_eq!(
                ack.value, seq,
                "value diverged from its admission point (seed {seed})"
            );
        }
    }
    assert!(!trace.is_empty(), "seed {seed} made no progress");
    (trace, *coverage.borrow())
}

#[test]
fn replay_fence_is_exactly_once() {
    let mut total = Coverage::default();
    for seed in seeds::torture_seeds() {
        let (_, coverage) = run_replay_seed(seed);
        accumulate(&mut total, coverage);
    }
    println!("replay-fence coverage across seeds: {total:?}");
    assert!(
        total.already_applied > 0,
        "no dropped ack was resolved by the replay fence: {total:?}"
    );
    assert!(total.contended > 0, "no contention observed: {total:?}");
    assert!(total.renewed > 0, "no heartbeat renewed anything: {total:?}");
}
