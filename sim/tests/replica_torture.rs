//! Replica reconstruction under crashes: a reader that knows *more* than a
//! crashed authority must detect it and resync.
//!
//! One writer churns a small key set under its prefix — overwrites and
//! tombstones, so exact admission-log delta semantics actually work
//! for a living — while a reader maintains a replica purely from `read_at`
//! (snapshot once, deltas after). Crashes roll the authority back to its
//! last flush; a replica that had already synced past that point now holds
//! acknowledged-but-lost state. The reader detects it by the only signal
//! the protocol gives: the cut regressing below its cursor (`at < since`),
//! and answers with a full resync.
//!
//! Oracles:
//! - every delta is well-formed: ascending `(admission_seq, key)`, each key
//!   at most once, everything strictly after the cursor;
//! - after each phase settles, one offline sync round must make the
//!   replica *exactly* equal the recovered authority's live state
//!   (tombstones invisible, overwrites at final values);
//! - the usual: full structural audit per phase, seeded replayability.

use homebase_core::clock::{HybridTimestamp, ManualClock, Timestamp};
use homebase_core::key::Key;
use homebase_core::lease::{LeaseId, LeaseMode};
use homebase_core::messages::{
    AcquireRequest, AdmissionBatch, AdmissionRequest, KernelError, LeaseSpec, Range, RangeCursor,
    RangeCut, ReadAtRequest,
};
use homebase_core::seal::Seal;
use homebase_core::space::{Space as _, SpaceError, SpaceId};
use homebase_core::tag::{
    AdmissionSeq, CipherEpoch, DeviceChecksum, DeviceEntry, DeviceId, DeviceSeq, DeviceTag,
    Mutation, OpaqueValue, Ver,
};
use homebase_server::actor::{SpaceActor, SpaceHandle};
use homebase_sim::check;
use homebase_sim::exec::SimExecutor;
use homebase_sim::seeds;
use homebase_sim::store::{FaultConfig, SimStore};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::cell::{Cell, RefCell};
use std::collections::BTreeMap;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

const SPACE: SpaceId = SpaceId([5; 16]);
const PHASES: usize = 4;
const KEY_POOL: u64 = 6;
const WRITER_ATTEMPTS: u32 = 40;
const READER_SYNCS: u32 = 15;

const FAULTS: FaultConfig = FaultConfig {
    error_rate: 0.01,
    flush_rate: 0.25,
    max_latency_yields: 3,
};

const WRITER: DeviceId = DeviceId([1; 16]);

fn prefix() -> Key {
    Key::from_bytes([b"db".to_vec()]).unwrap()
}

fn pool_key(i: u64) -> Key {
    Key::from_bytes([b"db".to_vec(), format!("k{i}").into_bytes()]).unwrap()
}

#[derive(Clone, Copy, Debug, Default)]
struct Coverage {
    tombstones: u32,
    overwrites: u32,
    ver_refreshes: u32,
    snapshots: u32,
    deltas: u32,
    replica_resets: u32,
}

/// Writer state surviving crashes.
#[derive(Clone)]
struct WriterState {
    next_seq: Rc<Cell<u64>>,
    checksum: Rc<Cell<DeviceChecksum>>,
    /// Last ver this client believes each pool key has. Refreshed from
    /// `VerRegression` when an unacked write survived a crash.
    vers: Rc<RefCell<BTreeMap<u64, u64>>>,
    lease: Rc<RefCell<Option<LeaseId>>>,
    stamp: Rc<Cell<u64>>,
    rng_seed: u64,
}

async fn writer(handle: SpaceHandle, state: WriterState, coverage: Rc<RefCell<Coverage>>) {
    let mut rng = StdRng::seed_from_u64(state.rng_seed);
    for _ in 0..WRITER_ATTEMPTS {
        if state.lease.borrow().is_none() {
            let req = AcquireRequest {
                device: WRITER,
                requested_at: HybridTimestamp::ZERO,
                specs: vec![LeaseSpec {
                    prefix: prefix(),
                    mode: LeaseMode::Write,
                    ttl: Duration::from_secs(60),
                }],
            };
            match handle.acquire(req).await {
                Ok(resp) => {
                    *state.lease.borrow_mut() = Some(resp.leases[0].id);
                }
                Err(SpaceError::Unavailable { .. }) => return,
                Err(err) => panic!("unexpected acquire failure: {err:?}"),
            }
            continue;
        }

        let key_index = rng.random_range(0..KEY_POOL);
        let tombstone = rng.random_bool(0.3);
        let ver = state.vers.borrow().get(&key_index).copied().unwrap_or(0) + 1;
        let seq = state.next_seq.get();
        let mutation = if tombstone {
            Mutation::Delete {
                key: pool_key(key_index),
            }
        } else {
            state.stamp.set(state.stamp.get() + 1);
            Mutation::Set {
                key: pool_key(key_index),
                value: OpaqueValue(format!("s{}", state.stamp.get()).into_bytes()),
            }
        };

        let req = AdmissionRequest {
            device: WRITER,
            expected_checksum: state.checksum.get(),
            evidence: vec![state.lease.borrow().unwrap()],
            batches: vec![AdmissionBatch {
                device_seq: DeviceSeq(seq),
                range_asserts: vec![],
                entries: vec![DeviceEntry {
                    mutation,
                    tag: DeviceTag {
                        device: WRITER,
                        device_seq: DeviceSeq(seq),
                        ver: Ver(ver),
                        cipher_epoch: CipherEpoch(0),
                    },
                    seal: Seal::empty_aead_v1(),
                }],
            }],
        };
        match handle.admit(req).await {
            Ok(response) => {
                state.next_seq.set(seq + 1);
                state.checksum.set(response.checksum);
                state.vers.borrow_mut().insert(key_index, ver);
                let mut cov = coverage.borrow_mut();
                if tombstone {
                    cov.tombstones += 1;
                } else if ver > 1 {
                    cov.overwrites += 1;
                }
            }
            Err(SpaceError::Kernel(KernelError::LeaseInvalid { .. })) => {
                *state.lease.borrow_mut() = None;
            }
            Err(SpaceError::Kernel(KernelError::DeviceSeqRegression { current, .. })) => {
                state.next_seq.set(current.0 + 1);
            }
            Err(SpaceError::Kernel(KernelError::DeviceChecksumMismatch { .. })) => return,
            // An unacked write to this key survived a crash; the rejection
            // itself tells us the authoritative ver.
            Err(SpaceError::Kernel(KernelError::VerRegression { current, .. })) => {
                state.vers.borrow_mut().insert(key_index, current.0);
                coverage.borrow_mut().ver_refreshes += 1;
            }
            Err(SpaceError::Unavailable { .. }) => return,
            Err(SpaceError::Kernel(err)) => panic!("unexpected rejection: {err:?}"),
        }
    }
}

/// Replica state surviving crashes — the reader's disk.
#[derive(Clone, Default)]
struct Replica {
    cursor: Rc<Cell<Option<u64>>>,
    state: Rc<RefCell<BTreeMap<Key, Vec<u8>>>>,
}

/// One sync round. Returns `false` on Unavailable.
async fn sync_once(
    handle: &SpaceHandle,
    replica: &Replica,
    coverage: &Rc<RefCell<Coverage>>,
) -> bool {
    let since = replica.cursor.get().map(AdmissionSeq);
    let resp = match handle
        .read_at(ReadAtRequest {
            ranges: vec![RangeCursor {
                range: Range::Prefix(prefix()),
                since,
            }],
        })
        .await
    {
        Ok(resp) => resp,
        Err(SpaceError::Unavailable { .. }) => return false,
        Err(err) => panic!("unexpected read_at failure: {err:?}"),
    };

    // The authority regressed below our cursor: it crashed and lost state
    // we already applied. The only sound move is a full resync.
    if let Some(since) = since {
        if resp.at < since {
            replica.cursor.set(None);
            replica.state.borrow_mut().clear();
            coverage.borrow_mut().replica_resets += 1;
            return true;
        }
    }

    match (&since, &resp.ranges[0]) {
        (None, RangeCut::Snapshot(entries)) => {
            coverage.borrow_mut().snapshots += 1;
            *replica.state.borrow_mut() = entries
                .iter()
                .map(|e| match &e.device_entry.mutation {
                    Mutation::Set { key, value } => (key.clone(), value.0.clone()),
                    Mutation::Delete { .. } => panic!("tombstone in snapshot"),
                    Mutation::DeleteRange { .. } => panic!("range delete in DR1 snapshot"),
                })
                .collect();
        }
        (Some(since), RangeCut::Delta(entries)) => {
            coverage.borrow_mut().deltas += 1;
            let positions: Vec<(u64, &Key)> = entries
                .iter()
                .map(|e| (e.admission.admission_seq.0, e.key()))
                .collect();
            assert!(
                positions.windows(2).all(|w| w[0] < w[1]),
                "delta order broken"
            );
            assert!(
                entries.iter().all(|e| e.admission.admission_seq > *since),
                "delta leaked entries at or before the cursor"
            );
            let mut state = replica.state.borrow_mut();
            for e in entries {
                match &e.device_entry.mutation {
                    Mutation::Set { key, value } => {
                        state.insert(key.clone(), value.0.clone());
                    }
                    Mutation::Delete { key } => {
                        state.remove(key);
                    }
                    Mutation::DeleteRange { .. } => {
                        unreachable!("server rejects DeleteRange during DR1")
                    }
                }
            }
        }
        (since, cut) => panic!("cursor {since:?} answered with wrong cut variant {cut:?}"),
    }
    replica.cursor.set(Some(resp.at.0));
    true
}

async fn reader(handle: SpaceHandle, replica: Replica, coverage: Rc<RefCell<Coverage>>) {
    for _ in 0..READER_SYNCS {
        if !sync_once(&handle, &replica, &coverage).await {
            return;
        }
    }
}

fn run_seed(seed: u64) -> (Vec<(Key, Vec<u8>)>, Coverage) {
    let mut master = StdRng::seed_from_u64(seed);
    let store = SimStore::new(master.random(), FAULTS);
    let clock = Arc::new(ManualClock::new(Timestamp(0)));
    let coverage = Rc::new(RefCell::new(Coverage::default()));
    let writer_state = WriterState {
        next_seq: Rc::new(Cell::new(1)),
        checksum: Rc::new(Cell::new(DeviceChecksum::EMPTY)),
        vers: Rc::new(RefCell::new(BTreeMap::new())),
        lease: Rc::new(RefCell::new(None)),
        stamp: Rc::new(Cell::new(0)),
        rng_seed: master.random(),
    };
    let replica = Replica::default();

    for phase in 0..PHASES {
        store.set_config(FAULTS);
        let mut exec = SimExecutor::new(master.random());
        let (actor, handle) = SpaceActor::new(SPACE, Arc::new(store.clone()), Arc::clone(&clock));
        let actor_task = exec.spawn(actor.run());
        let writer_task = exec.spawn(writer(
            handle.clone(),
            writer_state.clone(),
            Rc::clone(&coverage),
        ));
        let reader_task = exec.spawn(reader(
            handle.clone(),
            replica.clone(),
            Rc::clone(&coverage),
        ));
        drop(handle);

        if phase != PHASES - 1 {
            let steps = master.random_range(50..600);
            for _ in 0..steps {
                if !exec.step() {
                    break;
                }
            }
            exec.cancel(actor_task);
            exec.cancel(writer_task);
            exec.cancel(reader_task);
            store.crash();
            exec.run_until_stalled();
        } else {
            exec.run_until_stalled();
        }

        // -- oracles: audit, then offline convergence ------------------------
        store.set_config(FaultConfig::NONE);
        let audit = check::audit(SPACE, &store);

        // With the world quiet, at most two rounds settle the replica: one
        // to detect a cursor regression, one to resnapshot.
        let mut settle = SimExecutor::new(master.random());
        let (actor, handle) = SpaceActor::new(SPACE, Arc::new(store.clone()), Arc::clone(&clock));
        settle.spawn(actor.run());
        {
            let replica = replica.clone();
            let coverage = Rc::clone(&coverage);
            settle.spawn(async move {
                assert!(sync_once(&handle, &replica, &coverage).await);
                assert!(sync_once(&handle, &replica, &coverage).await);
            });
        }
        settle.run_until_stalled();

        let expected: BTreeMap<Key, Vec<u8>> = audit
            .data
            .iter()
            .filter_map(|(k, rec)| match &rec.entry.device_entry.mutation {
                Mutation::Set { value, .. } => Some((k.clone(), value.0.clone())),
                Mutation::Delete { .. } => None,
                Mutation::DeleteRange { .. } => {
                    unreachable!("server rejects DeleteRange during DR1")
                }
            })
            .collect();
        assert_eq!(
            *replica.state.borrow(),
            expected,
            "replica diverged from recovered authority (seed {seed}, phase {phase})"
        );
    }

    let final_state: Vec<(Key, Vec<u8>)> = replica.state.borrow().clone().into_iter().collect();
    (final_state, *coverage.borrow())
}

#[test]
fn replica_torture_seeds_reconverge() {
    let mut total = Coverage::default();
    for seed in seeds::torture_seeds() {
        let (_, coverage) = run_seed(seed);
        total.tombstones += coverage.tombstones;
        total.overwrites += coverage.overwrites;
        total.ver_refreshes += coverage.ver_refreshes;
        total.snapshots += coverage.snapshots;
        total.deltas += coverage.deltas;
        total.replica_resets += coverage.replica_resets;
    }
    println!("coverage across seeds: {total:?}");
    assert!(total.tombstones > 0, "no tombstones written: {total:?}");
    assert!(total.overwrites > 0, "no overwrites: {total:?}");
    assert!(total.snapshots > 0, "no snapshots served: {total:?}");
    assert!(total.deltas > 0, "no deltas served: {total:?}");
    assert!(
        total.replica_resets > 0,
        "no crash outran a replica: {total:?}"
    );
}

#[test]
fn replica_torture_replays_identically() {
    for seed in [5, 21] {
        assert_eq!(
            run_seed(seed).0,
            run_seed(seed).0,
            "seed {seed} diverged on replay"
        );
    }
}
