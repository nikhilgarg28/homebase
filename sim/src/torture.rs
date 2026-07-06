//! Shared torture harness: actors, clients, and oracles.
//!
//! Scenario tests import this module; crash torture lives in [`crate::crash`].

use crate::check;
use crate::exec::SimExecutor;
use crate::store::{FaultConfig, SimStore};
use homebase_core::clock::{ManualClock, Timestamp};
use homebase_core::key::Key;
use homebase_core::lease::{LeaseMode, LeaseRef};
use homebase_core::messages::{
    AcquireRequest, KernelError, LeaseSpec, PutBatchRequest, PutEntry, Range, RangeCursor,
    ReadAtRequest, ReleaseRequest,
};
use homebase_core::space::{Space as _, SpaceError, SpaceId};
use homebase_core::tag::{AdmissionSeq, DeviceId, DeviceSeq, Value, Ver};
use homebase_server::actor::{SpaceActor, SpaceHandle};
use homebase_server::storage::OrderedStore;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

pub const SPACE: SpaceId = SpaceId([3; 16]);

pub fn dev(d: u8) -> DeviceId {
    DeviceId([d + 1; 16])
}

pub fn key(parts: &[&[u8]]) -> Key {
    Key::from_bytes(parts.iter().copied()).unwrap()
}

pub fn write_lease_req(device: u8, prefix: &Key, ttl_ms: u64, stealable: bool) -> AcquireRequest {
    AcquireRequest {
        device: dev(device),
        steal: false,
        specs: vec![LeaseSpec {
            prefix: prefix.clone(),
            mode: LeaseMode::Write,
            ttl: Duration::from_millis(ttl_ms),
            stealable,
        }],
    }
}

pub fn put_one(
    device: u8,
    seq: u64,
    lease: LeaseRef,
    k: &Key,
    v: &[u8],
    ver: u64,
) -> PutBatchRequest {
    PutBatchRequest {
        device: dev(device),
        device_seq: DeviceSeq(seq),
        leases: vec![lease],
        entries: vec![PutEntry {
            key: k.clone(),
            value: Value::Present(v.to_vec()),
            ver: Ver(ver),
        }],
    }
}

/// Run one actor until stalled; returns the handle (keep clones alive).
pub fn run_actor<S>(exec: &mut SimExecutor, store: Arc<S>, clock: Arc<ManualClock>) -> SpaceHandle
where
    S: OrderedStore + Send + Sync + 'static,
{
    let (actor, handle) = SpaceActor::new(SPACE, store, clock);
    exec.spawn(actor.run());
    handle
}

/// Audit with faults disabled (SimStore only; no-op for other stores).
pub fn audit_sim_store(store: &SimStore) -> check::StoreAudit {
    store.set_config(FaultConfig::NONE);
    check::audit(SPACE, store)
}

pub fn audit_space<S: OrderedStore>(store: &S) -> check::StoreAudit {
    check::audit(SPACE, store)
}

/// A read replica driven purely by `read_at`.
#[derive(Clone, Default)]
pub struct Replica {
    pub cursor: Option<AdmissionSeq>,
    pub live: BTreeMap<Key, Vec<u8>>,
}

impl Replica {
    pub async fn sync(
        &mut self,
        handle: &SpaceHandle,
        prefix: &Key,
        high_water: u64,
    ) -> Result<(), SpaceError> {
        let resp = handle
            .read_at(ReadAtRequest {
                ranges: vec![RangeCursor {
                    range: Range::Prefix(prefix.clone()),
                    since: self.cursor,
                }],
            })
            .await?;
        assert_eq!(resp.at, AdmissionSeq(high_water));
        match (&self.cursor, &resp.ranges[0]) {
            (None, homebase_core::messages::RangeCut::Snapshot(entries)) => {
                self.live = entries
                    .iter()
                    .filter_map(|e| {
                        let Value::Present(v) = &e.value else {
                            return None;
                        };
                        Some((e.key.clone(), v.clone()))
                    })
                    .collect();
            }
            (Some(_), homebase_core::messages::RangeCut::Delta(entries)) => {
                for e in entries {
                    match &e.value {
                        Value::Present(v) => {
                            self.live.insert(e.key.clone(), v.clone());
                        }
                        Value::Absent => {
                            self.live.remove(&e.key);
                        }
                    }
                }
            }
            (c, cut) => panic!("cursor {c:?} vs cut {cut:?}"),
        }
        self.cursor = Some(resp.at);
        Ok(())
    }
}

/// Steal race: two devices on one shared stealable prefix; exactly one
/// writer is live at a time.
pub fn run_steal_race(seed: u64) {
    let mut rng = StdRng::seed_from_u64(seed);
    let store = Arc::new(SimStore::new(seed, FaultConfig::NONE));
    let clock = Arc::new(ManualClock::new(Timestamp(0)));
    let mut exec = SimExecutor::new(rng.random());
    let handle = run_actor(&mut exec, Arc::clone(&store), Arc::clone(&clock));

    let shared = key(&[b"account"]);
    let results: Rc<RefCell<Vec<Result<(), SpaceError>>>> = Rc::new(RefCell::new(Vec::new()));
    for device in [1u8, 2] {
        let h = handle.clone();
        let p = shared.clone();
        let out = Rc::clone(&results);
        exec.spawn(async move {
            let req = AcquireRequest {
                device: dev(device),
                steal: true,
                specs: vec![LeaseSpec {
                    prefix: p,
                    mode: LeaseMode::Write,
                    ttl: Duration::from_secs(60),
                    stealable: true,
                }],
            };
            let r = h.acquire(req).await.map(|_| ());
            out.borrow_mut().push(r);
        });
        exec.run_until_stalled();
    }

    let ok = results.borrow().iter().filter(|r| r.is_ok()).count();
    assert_eq!(ok, 2, "both steals must succeed sequentially");
    let audit = audit_sim_store(&store);
    assert_eq!(audit.leases.len(), 1, "only one live lease on the prefix");
}

/// Contended handoff: non-stealable holder blocks; after release + time, the
/// waiter acquires.
pub fn run_contended_handoff(seed: u64) {
    let store = Arc::new(SimStore::new(seed, FaultConfig::NONE));
    let clock = Arc::new(ManualClock::new(Timestamp(0)));
    let mut exec = SimExecutor::new(seed);
    let handle = run_actor(&mut exec, Arc::clone(&store), Arc::clone(&clock));
    let p = key(&[b"db"]);
    let lease_id = Rc::new(RefCell::new(None));

    let h1 = handle.clone();
    let p1 = p.clone();
    let lid = Rc::clone(&lease_id);
    exec.spawn(async move {
        let resp = h1
            .acquire(write_lease_req(1, &p1, 500, false))
            .await
            .unwrap();
        *lid.borrow_mut() = Some(resp.leases[0].id);
    });
    exec.run_until_stalled();

    let h2 = handle.clone();
    let p2 = p.clone();
    let denied: Rc<RefCell<Option<SpaceError>>> = Rc::new(RefCell::new(None));
    let d = Rc::clone(&denied);
    exec.spawn(async move {
        *d.borrow_mut() = Some(
            h2.acquire(write_lease_req(2, &p2, 500, false))
                .await
                .unwrap_err(),
        );
    });
    exec.run_until_stalled();
    assert!(matches!(
        denied.borrow().as_ref(),
        Some(SpaceError::Kernel(KernelError::Contended { .. }))
    ));

    let h1 = handle.clone();
    let lid = *lease_id.borrow();
    exec.spawn(async move {
        h1.release(ReleaseRequest {
            device: dev(1),
            leases: vec![lid.unwrap()],
        })
        .await
        .unwrap();
    });
    exec.run_until_stalled();

    let h2 = handle.clone();
    let p2 = p.clone();
    exec.spawn(async move {
        h2.acquire(write_lease_req(2, &p2, 500, false))
            .await
            .unwrap();
    });
    exec.run_until_stalled();

    let audit = audit_sim_store(&store);
    assert_eq!(audit.leases.len(), 1);
    assert_eq!(audit.leases.values().next().unwrap().device, dev(2));
}

/// Zombie writer: client keeps a stale lease ref past expiry; puts are
/// rejected.
pub fn run_zombie_writer(seed: u64) {
    let store = Arc::new(SimStore::new(seed, FaultConfig::NONE));
    let clock = Arc::new(ManualClock::new(Timestamp(0)));
    let mut exec = SimExecutor::new(seed);
    let handle = run_actor(&mut exec, Arc::clone(&store), Arc::clone(&clock));
    let p = key(&[b"db"]);
    let k = key(&[b"db", b"row"]);

    let lease: Rc<RefCell<Option<LeaseRef>>> = Rc::new(RefCell::new(None));
    let l = Rc::clone(&lease);
    let h = handle.clone();
    exec.spawn(async move {
        let resp = h.acquire(write_lease_req(1, &p, 100, false)).await.unwrap();
        *l.borrow_mut() = Some(LeaseRef {
            id: resp.leases[0].id,
            epoch: resp.leases[0].epoch,
        });
    });
    exec.run_until_stalled();

    clock.advance(Duration::from_millis(100));

    let h = handle.clone();
    let l = *lease.borrow();
    let err: Rc<RefCell<Option<SpaceError>>> = Rc::new(RefCell::new(None));
    let e = Rc::clone(&err);
    exec.spawn(async move {
        *e.borrow_mut() = Some(
            h.put_batch(put_one(1, 1, l.unwrap(), &k, b"zombie", 1))
                .await
                .unwrap_err(),
        );
    });
    exec.run_until_stalled();

    assert!(matches!(
        err.borrow().as_ref(),
        Some(SpaceError::Kernel(KernelError::LeaseInvalid { .. }))
    ));
    audit_sim_store(&store);
}

/// Replica tracks live keys via `read_at` through interleaved writes.
pub fn run_replica_sync(seed: u64) {
    let store = Arc::new(SimStore::new(seed, FaultConfig::NONE));
    let clock = Arc::new(ManualClock::new(Timestamp(0)));
    let mut exec = SimExecutor::new(seed);
    let handle = run_actor(&mut exec, Arc::clone(&store), Arc::clone(&clock));
    let prefix = key(&[b"d0"]);
    let replica = Rc::new(RefCell::new(Replica::default()));

    let granted: Rc<RefCell<Option<LeaseRef>>> = Rc::new(RefCell::new(None));
    let g = Rc::clone(&granted);
    let h = handle.clone();
    let p0 = prefix.clone();
    exec.spawn(async move {
        let resp = h
            .acquire(write_lease_req(0, &p0, 60_000, false))
            .await
            .unwrap();
        *g.borrow_mut() = Some(LeaseRef {
            id: resp.leases[0].id,
            epoch: resp.leases[0].epoch,
        });
    });
    exec.run_until_stalled();
    let lease = *granted.borrow();

    for seq in 1..=5u64 {
        let h = handle.clone();
        let k = key(&[b"d0", format!("k{seq}").as_bytes()]);
        let l = lease.unwrap();
        exec.spawn(async move {
            h.put_batch(put_one(0, seq, l, &k, format!("v{seq}").as_bytes(), 1))
                .await
                .unwrap();
        });
        exec.run_until_stalled();

        let hw = audit_sim_store(&store).max_admission_seq;
        let r = Rc::clone(&replica);
        let h = handle.clone();
        let p = prefix.clone();
        exec.spawn(async move {
            r.borrow_mut().sync(&h, &p, hw).await.unwrap();
        });
        exec.run_until_stalled();
    }

    let replica = replica.borrow().clone();

    let audit = audit_sim_store(&store);
    assert_eq!(
        replica.live.len(),
        audit.data.values().filter(|r| r.value.is_present()).count()
    );
}
