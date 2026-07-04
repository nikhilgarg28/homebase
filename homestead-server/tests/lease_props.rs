//! Model-based property tests for the lease state machine.
//!
//! A shadow model (plain Vec, brute-force overlap checks) is the oracle:
//! random command sequences run against both the real `LeaseManager` and the
//! model, asserting decisions and grants agree. After every command the real
//! store must satisfy the kernel invariants:
//!
//! 1. no two live leases overlap with incompatible modes;
//! 2. the by-id and by-prefix indexes hold identical record sets;
//! 3. live records in the store are exactly the model's live leases;
//! 4. lease ids and epochs strictly increase and never recur.
//!
//! Steal semantics ride the same oracle: an acquire with `steal = true`
//! succeeds against blockers that are all stealable (which the model then
//! marks gone) and contends if any blocker is not.

use homestead_core::clock::Timestamp;
use homestead_core::key::Key;
use homestead_core::lease::{LeaseId, LeaseMode};
use homestead_core::messages::{AcquireRequest, LeaseSpec, ReleaseRequest, RenewRequest};
use homestead_core::space::SpaceId;
use homestead_core::tag::DeviceId;
use homestead_server::space::lease::LeaseManager;
use homestead_server::schema::{LeaseRecord, lease_by_id_scan, lease_by_prefix_scan_all};
use homestead_server::storage::{MemoryStore, OrderedStore, collect_scan};
use pollster::block_on;
use proptest::prelude::*;
use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

const SPACE: SpaceId = SpaceId([7; 16]);

// ---------------------------------------------------------------------------
// commands

type Prefix = Vec<Vec<u8>>;

/// `(prefix, mode, ttl_ms, stealable)`
type Spec = (Prefix, LeaseMode, u64, bool);

#[derive(Clone, Debug)]
enum Cmd {
    Acquire { device: u8, specs: Vec<Spec>, steal: bool },
    RenewAll { device: u8 },
    ReleaseLive { device: u8 },
    Advance { ms: u64 },
}

fn arb_prefix() -> impl Strategy<Value = Prefix> {
    prop::collection::vec(
        prop::sample::select(vec![b"a".to_vec(), b"b".to_vec()]),
        1..=3,
    )
}

fn arb_cmd() -> impl Strategy<Value = Cmd> {
    let mode = prop::sample::select(vec![LeaseMode::Read, LeaseMode::Write]);
    let spec = (arb_prefix(), mode, 1u64..=40, prop::bool::weighted(0.5));
    prop_oneof![
        3 => (0u8..3, prop::collection::vec(spec, 1..=2), prop::bool::weighted(0.3))
            .prop_map(|(device, specs, steal)| Cmd::Acquire { device, specs, steal }),
        1 => (0u8..3).prop_map(|device| Cmd::RenewAll { device }),
        1 => (0u8..3).prop_map(|device| Cmd::ReleaseLive { device }),
        2 => (1u64..=25).prop_map(|ms| Cmd::Advance { ms }),
    ]
}

// ---------------------------------------------------------------------------
// the shadow model

#[derive(Clone, Debug)]
struct MLease {
    id: u64,
    epoch: u64,
    prefix: Prefix,
    mode: LeaseMode,
    device: u8,
    deadline: u64,
    ttl: u64,
    stealable: bool,
    gone: bool,
}

#[derive(Default)]
struct Model {
    now: u64,
    next_id: u64,
    next_epoch: u64,
    leases: Vec<MLease>,
}

fn overlaps(a: &Prefix, b: &Prefix) -> bool {
    let n = a.len().min(b.len());
    a[..n] == b[..n]
}

impl Model {
    fn live(&self) -> impl Iterator<Item = &MLease> {
        self.leases
            .iter()
            .filter(|l| !l.gone && l.deadline > self.now)
    }

    /// Brute-force oracle for the acquire decision. A blocker only blocks
    /// when it is not stealable or the request is not stealing.
    fn acquire_would_conflict(&self, steal: bool, specs: &[Spec]) -> bool {
        let store_conflict = specs.iter().any(|(prefix, mode, _, _)| {
            self.live().any(|l| {
                overlaps(prefix, &l.prefix)
                    && !mode.compatible_with(l.mode)
                    && !(steal && l.stealable)
            })
        });
        let intra_conflict = specs.iter().enumerate().any(|(i, (pa, ma, _, _))| {
            specs[i + 1..]
                .iter()
                .any(|(pb, mb, _, _)| overlaps(pa, pb) && !ma.compatible_with(*mb))
        });
        store_conflict || intra_conflict
    }

    /// Ids of live leases an admitted acquire displaces (empty unless the
    /// request stole; on a granted request every incompatible overlap must
    /// have been stealable).
    fn stolen_by(&self, specs: &[Spec]) -> Vec<u64> {
        self.live()
            .filter(|l| {
                specs.iter().any(|(prefix, mode, _, _)| {
                    overlaps(prefix, &l.prefix) && !mode.compatible_with(l.mode)
                })
            })
            .map(|l| l.id)
            .collect()
    }
}

// ---------------------------------------------------------------------------
// invariant checks against the real store

fn store_records(store: &MemoryStore) -> (Vec<LeaseRecord>, Vec<LeaseRecord>) {
    let by_id: Vec<LeaseRecord> = block_on(collect_scan(store.scan_prefix(&lease_by_id_scan(SPACE))))
        .unwrap()
        .into_iter()
        .map(|(_, v)| LeaseRecord::decode(&v).unwrap())
        .collect();
    let by_prefix: Vec<LeaseRecord> =
        block_on(collect_scan(store.scan_prefix(&lease_by_prefix_scan_all(SPACE))))
            .unwrap()
            .into_iter()
            .map(|(_, v)| LeaseRecord::decode(&v).unwrap())
            .collect();
    (by_id, by_prefix)
}

fn check_invariants(store: &MemoryStore, model: &Model) -> Result<(), TestCaseError> {
    let now = Timestamp(model.now);
    let (by_id, by_prefix) = store_records(store);

    // 2: indexes agree.
    let id_set: BTreeSet<u64> = by_id.iter().map(|r| r.id.0).collect();
    let prefix_set: BTreeSet<u64> = by_prefix.iter().map(|r| r.id.0).collect();
    prop_assert_eq!(&id_set, &prefix_set, "index sets diverged");

    let live: Vec<&LeaseRecord> = by_id.iter().filter(|r| r.is_live(now)).collect();

    // 1: no incompatible overlap among live leases.
    for (i, a) in live.iter().enumerate() {
        for b in &live[i + 1..] {
            let overlap = a.prefix.starts_with(&b.prefix) || b.prefix.starts_with(&a.prefix);
            prop_assert!(
                !(overlap && !a.mode.compatible_with(b.mode)),
                "incompatible overlap: {a:?} vs {b:?}"
            );
        }
    }

    // 3: live store records == model live leases, field by field.
    let store_live: BTreeMap<u64, &LeaseRecord> =
        live.iter().map(|r| (r.id.0, *r)).collect();
    let model_live: BTreeMap<u64, &MLease> =
        model.live().map(|l| (l.id, l)).collect();
    prop_assert_eq!(
        store_live.keys().collect::<Vec<_>>(),
        model_live.keys().collect::<Vec<_>>(),
        "live lease id sets diverged"
    );
    for (id, rec) in &store_live {
        let m = model_live[id];
        prop_assert_eq!(rec.epoch.0, m.epoch);
        prop_assert_eq!(rec.mode, m.mode);
        prop_assert_eq!(rec.stealable, m.stealable);
        prop_assert_eq!(rec.device, DeviceId([m.device; 16]));
        prop_assert_eq!(rec.deadline.0, m.deadline);
        let rec_prefix: Prefix = rec
            .prefix
            .components()
            .iter()
            .map(|c| c.as_bytes().to_vec())
            .collect();
        prop_assert_eq!(&rec_prefix, &m.prefix);
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// the run loop

fn dev(n: u8) -> DeviceId {
    DeviceId([n; 16])
}

fn to_key(prefix: &Prefix) -> Key {
    Key::from_bytes(prefix.clone()).unwrap()
}

proptest! {
    #[test]
    fn lease_state_machine_matches_model(cmds in prop::collection::vec(arb_cmd(), 1..=40)) {
        let mut store = MemoryStore::new();
        let mut mgr = LeaseManager::new(SPACE);
        let mut model = Model::default();

        for cmd in cmds {
            match &cmd {
                Cmd::Acquire { device, specs, steal } => {
                    let req = AcquireRequest {
                        device: dev(*device),
                        steal: *steal,
                        specs: specs
                            .iter()
                            .map(|(prefix, mode, ttl, stealable)| LeaseSpec {
                                prefix: to_key(prefix),
                                mode: *mode,
                                ttl: Duration::from_millis(*ttl),
                                stealable: *stealable,
                            })
                            .collect(),
                    };
                    let result = block_on(mgr.acquire(&mut store, Timestamp(model.now), &req));
                    let expected_conflict = model.acquire_would_conflict(*steal, specs);
                    prop_assert_eq!(
                        result.is_err(),
                        expected_conflict,
                        "decision diverged from oracle on {:?}", cmd
                    );
                    if let Ok((leases, _barrier)) = result {
                        // Victims first: a granted steal displaces every
                        // incompatible live overlap (all stealable, or the
                        // oracle would have contended).
                        let stolen = model.stolen_by(specs);
                        for l in &mut model.leases {
                            if stolen.contains(&l.id) {
                                l.gone = true;
                            }
                        }

                        prop_assert_eq!(leases.len(), specs.len());
                        for (lease, (prefix, mode, ttl, stealable)) in leases.iter().zip(specs) {
                            // 4: ids and epochs strictly increase, never recur.
                            prop_assert_eq!(lease.id, LeaseId(model.next_id));
                            prop_assert_eq!(lease.epoch.0, model.next_epoch);
                            prop_assert_eq!(lease.mode, *mode);
                            prop_assert_eq!(lease.stealable, *stealable);
                            model.leases.push(MLease {
                                id: model.next_id,
                                epoch: model.next_epoch,
                                prefix: prefix.clone(),
                                mode: *mode,
                                device: *device,
                                deadline: model.now + ttl,
                                ttl: *ttl,
                                stealable: *stealable,
                                gone: false,
                            });
                            model.next_id += 1;
                            model.next_epoch += 1;
                        }
                    }
                }
                Cmd::RenewAll { device } => {
                    // Renew everything ever granted to this device, including
                    // dead and released ids, to exercise the invalid path.
                    let ids: Vec<LeaseId> = model
                        .leases
                        .iter()
                        .filter(|l| l.device == *device)
                        .map(|l| LeaseId(l.id))
                        .collect();
                    let resp = block_on(mgr.renew(
                        &mut store,
                        Timestamp(model.now),
                        &RenewRequest { device: dev(*device), leases: ids.clone() },
                    ))
                    .unwrap();

                    let expected_granted: BTreeSet<u64> = model
                        .live()
                        .filter(|l| l.device == *device)
                        .map(|l| l.id)
                        .collect();
                    let granted: BTreeSet<u64> =
                        resp.granted.iter().map(|g| g.id.0).collect();
                    let invalid: BTreeSet<u64> =
                        resp.invalid.iter().map(|i| i.0).collect();
                    let all: BTreeSet<u64> = ids.iter().map(|i| i.0).collect();
                    prop_assert_eq!(&granted, &expected_granted);
                    prop_assert_eq!(
                        invalid,
                        all.difference(&granted).copied().collect::<BTreeSet<u64>>()
                    );

                    let now = model.now;
                    for l in &mut model.leases {
                        if granted.contains(&l.id) {
                            l.deadline = now + l.ttl;
                        }
                    }
                }
                Cmd::ReleaseLive { device } => {
                    let ids: Vec<LeaseId> = model
                        .live()
                        .filter(|l| l.device == *device)
                        .map(|l| LeaseId(l.id))
                        .collect();
                    block_on(mgr.release(
                        &mut store,
                        Timestamp(model.now),
                        &ReleaseRequest { device: dev(*device), leases: ids.clone() },
                    ))
                    .unwrap();
                    for l in &mut model.leases {
                        if ids.iter().any(|id| id.0 == l.id) {
                            l.gone = true;
                        }
                    }
                }
                Cmd::Advance { ms } => {
                    model.now += ms;
                }
            }

            check_invariants(&store, &model)?;
        }
    }
}
