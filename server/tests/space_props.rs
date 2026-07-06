//! Model-based property tests for the data plane.
//!
//! Two devices hold disjoint write leases (`("d0",)`, `("d1",)`) and run a
//! random interleaving of valid puts, ver regressions, device-seq replays,
//! reads, and replica syncs against both the real [`Space`] and a
//! brute-force shadow model. Invariants checked throughout:
//!
//! 1. **admission decisions agree** — the kernel admits exactly the batches
//!    the model would, with dense, strictly increasing admission seqs;
//! 2. **rejected batches leave no trace** — data, device seq, and high water
//!    all unchanged;
//! 3. **reads match** — `get` and `list` (including pagination) agree with
//!    the model's live state, tombstones hidden;
//! 4. **replica reconstruction** — a snapshot plus any sequence of later
//!    deltas equals current live state, with deltas ordered by
//!    `(admission_seq, key)` and each key appearing at most once;
//! 5. **aggregate coherence** — after every admitted batch, the stored
//!    per-prefix aggregates (max admission seq, live count) equal a
//!    brute-force recomputation from the data records.

use homebase_core::clock::Timestamp;
use homebase_core::key::Key;
use homebase_core::lease::{LeaseMode, LeaseRef};
use homebase_core::messages::{
    AcquireRequest, GetRequest, KernelError, LeaseSpec, ListRequest, PrefixCursor, PutBatchRequest,
    PutEntry, RangeCut, ReadAtRequest,
};
use homebase_core::space::SpaceId;
use homebase_core::tag::{AdmissionSeq, DeviceId, DeviceSeq, Value, Ver};
use homebase_server::error::Error;
use homebase_server::schema::{
    DataRecord, PrefixMetaRecord, data_scan_all, prefix_meta_key, prefix_meta_scan_all,
    user_key_from_data,
};
use homebase_server::space::Space;
use homebase_server::storage::{MemoryStore, OrderedStore, collect_scan};
use pollster::block_on;
use proptest::prelude::*;
use std::collections::BTreeMap;
use std::time::Duration;

const SPACE: SpaceId = SpaceId([11; 16]);
const DEVICES: usize = 2;
const SUFFIXES: &[&[u8]] = &[b"a", b"b", b"c", b"d", b"e", b"f"];

fn dev(d: usize) -> DeviceId {
    DeviceId([d as u8 + 1; 16])
}

fn dev_prefix(d: usize) -> Key {
    Key::from_bytes([format!("d{d}").into_bytes()]).unwrap()
}

fn user_key(d: usize, suffix: usize) -> Key {
    Key::from_bytes([format!("d{d}").into_bytes(), SUFFIXES[suffix].to_vec()]).unwrap()
}

// ---------------------------------------------------------------------------
// commands

#[derive(Clone, Debug)]
enum Cmd {
    /// Valid batch: fresh device_seq, strictly increasing vers.
    /// `(suffix, tombstone)` — duplicate suffixes exercise intra-batch order.
    Put {
        device: usize,
        writes: Vec<(usize, bool)>,
    },
    /// Reuses the stored ver for a written key → whole batch rejected.
    PutVerRegression { device: usize, suffix: usize },
    /// Replays the device's last device_seq → whole batch rejected.
    PutReplay { device: usize, suffix: usize },
    /// Advance the device's replica: snapshot if fresh, else delta.
    Sync { device: usize },
    /// Drop the replica: the next sync takes a fresh snapshot.
    ResetReplica { device: usize },
    /// Compare get + list (with pagination) against the model.
    CheckReads { device: usize },
}

fn arb_cmd() -> impl Strategy<Value = Cmd> {
    let device = 0..DEVICES;
    let write = (0..SUFFIXES.len(), prop::bool::weighted(0.3));
    prop_oneof![
        4 => (device.clone(), prop::collection::vec(write, 1..=3))
            .prop_map(|(device, writes)| Cmd::Put { device, writes }),
        1 => (device.clone(), 0..SUFFIXES.len())
            .prop_map(|(device, suffix)| Cmd::PutVerRegression { device, suffix }),
        1 => (device.clone(), 0..SUFFIXES.len())
            .prop_map(|(device, suffix)| Cmd::PutReplay { device, suffix }),
        3 => device.clone().prop_map(|device| Cmd::Sync { device }),
        1 => device.clone().prop_map(|device| Cmd::ResetReplica { device }),
        2 => device.prop_map(|device| Cmd::CheckReads { device }),
    ]
}

// ---------------------------------------------------------------------------
// the shadow model

#[derive(Clone, Debug, PartialEq)]
struct MEntry {
    value: Option<Vec<u8>>, // None = tombstone
    ver: u64,
}

#[derive(Default)]
struct Model {
    data: BTreeMap<(usize, usize), MEntry>,
    device_seq: [u64; DEVICES],
    high_water: u64,
    value_counter: u64,
}

impl Model {
    fn live(&self, device: usize) -> BTreeMap<usize, Vec<u8>> {
        self.data
            .iter()
            .filter(|((d, _), e)| *d == device && e.value.is_some())
            .map(|((_, s), e)| (*s, e.value.clone().unwrap()))
            .collect()
    }

    fn fresh_value(&mut self) -> Vec<u8> {
        self.value_counter += 1;
        format!("v{}", self.value_counter).into_bytes()
    }
}

/// One device's replica: a cursor into the admission order plus the state
/// reconstructed purely from `read_at` responses.
#[derive(Default)]
struct Replica {
    cursor: Option<AdmissionSeq>,
    state: BTreeMap<Key, Vec<u8>>,
}

// ---------------------------------------------------------------------------
// the run loop

struct Harness {
    space: Space,
    store: MemoryStore,
    leases: Vec<LeaseRef>,
}

impl Harness {
    fn new() -> Self {
        let mut space = Space::new(SPACE);
        let store = MemoryStore::new();
        let mut leases = Vec::new();
        for d in 0..DEVICES {
            let resp = block_on(space.acquire(
                &store,
                Timestamp(0),
                &AcquireRequest {
                    device: dev(d),
                    steal: false,
                    specs: vec![LeaseSpec {
                        prefix: dev_prefix(d),
                        mode: LeaseMode::Write,
                        stealable: false,
                        ttl: Duration::from_secs(1 << 30),
                    }],
                },
            ))
            .unwrap();
            leases.push(LeaseRef {
                id: resp.leases[0].id,
                epoch: resp.leases[0].epoch,
            });
        }
        Self {
            space,
            store,
            leases,
        }
    }

    fn put(
        &mut self,
        device: usize,
        device_seq: u64,
        entries: Vec<PutEntry>,
    ) -> Result<AdmissionSeq, Error> {
        block_on(self.space.put_batch(
            &self.store,
            Timestamp(1),
            &PutBatchRequest {
                device: dev(device),
                device_seq: DeviceSeq(device_seq),
                leases: vec![self.leases[device]],
                entries,
            },
        ))
        .map(|resp| resp.admission_seq)
    }
}

fn check_reads(h: &Harness, model: &Model, device: usize) -> Result<(), TestCaseError> {
    let live = model.live(device);

    // get: every suffix, written or not.
    let keys: Vec<Key> = (0..SUFFIXES.len()).map(|s| user_key(device, s)).collect();
    let got = block_on(h.space.get(&h.store, &GetRequest { keys: keys.clone() })).unwrap();
    for (s, entry) in got.entries.iter().enumerate() {
        match live.get(&s) {
            Some(value) => {
                let entry = entry.as_ref().expect("live key must be present in get");
                prop_assert_eq!(&entry.value, &Value::Present(value.clone()));
                prop_assert_eq!(entry.tag.ver, Ver(model.data[&(device, s)].ver));
            }
            None => prop_assert!(entry.is_none(), "tombstoned/unwritten key surfaced in get"),
        }
    }

    // list: full scan agrees with the model, in key order.
    let full = block_on(h.space.list(
        &h.store,
        &ListRequest {
            prefix: dev_prefix(device),
            start_after: None,
            limit: None,
        },
    ))
    .unwrap();
    prop_assert!(!full.truncated);
    let listed: Vec<(Key, Vec<u8>)> = full
        .entries
        .iter()
        .map(|e| {
            let Value::Present(v) = &e.value else {
                panic!("tombstone in list")
            };
            (e.key.clone(), v.clone())
        })
        .collect();
    let expected: Vec<(Key, Vec<u8>)> = live
        .iter()
        .map(|(s, v)| (user_key(device, *s), v.clone()))
        .collect();
    prop_assert_eq!(&listed, &expected);

    // list pagination: limit-1 pages walk to the same result.
    let mut paged = Vec::new();
    let mut cursor: Option<Key> = None;
    loop {
        let page = block_on(h.space.list(
            &h.store,
            &ListRequest {
                prefix: dev_prefix(device),
                start_after: cursor.clone(),
                limit: Some(1),
            },
        ))
        .unwrap();
        prop_assert!(page.entries.len() <= 1);
        match page.entries.into_iter().next() {
            Some(e) => {
                cursor = Some(e.key.clone());
                let Value::Present(v) = e.value else {
                    panic!("tombstone in list")
                };
                paged.push((e.key, v));
            }
            None => {
                prop_assert!(!page.truncated);
                break;
            }
        }
    }
    prop_assert_eq!(&paged, &expected);
    Ok(())
}

/// Invariant 5: stored per-prefix aggregates equal a brute-force
/// recomputation over every data record (tombstones count toward the max
/// seq, only present values toward the live count). This also pins the
/// "written prefixes keep their record forever" shape: the two maps must
/// have identical key sets.
fn check_aggregates(store: &MemoryStore) -> Result<(), TestCaseError> {
    let mut expected: BTreeMap<Vec<u8>, (u64, u64)> = BTreeMap::new();
    for (k, v) in block_on(collect_scan(store.scan_prefix(&data_scan_all(SPACE)))).unwrap() {
        let key = user_key_from_data(&k).unwrap();
        let rec = DataRecord::decode(&v).unwrap();
        let components = key.components();
        for depth in 1..=components.len() {
            let slot = expected
                .entry(prefix_meta_key(SPACE, &components[..depth]))
                .or_insert((0, 0));
            slot.0 = slot.0.max(rec.tag.admission_seq.0);
            slot.1 += rec.value.is_present() as u64;
        }
    }

    let stored: BTreeMap<Vec<u8>, (u64, u64)> = block_on(collect_scan(
        store.scan_prefix(&prefix_meta_scan_all(SPACE)),
    ))
    .unwrap()
    .into_iter()
    .map(|(k, v)| {
        let rec = PrefixMetaRecord::decode(&v).unwrap();
        (k, (rec.max_admission_seq, rec.live_count))
    })
    .collect();
    prop_assert_eq!(&stored, &expected, "aggregates diverged from data records");
    Ok(())
}

fn sync_replica(
    h: &Harness,
    model: &Model,
    device: usize,
    replica: &mut Replica,
) -> Result<(), TestCaseError> {
    let resp = block_on(h.space.read_at(
        &h.store,
        &ReadAtRequest {
            ranges: vec![PrefixCursor {
                prefix: dev_prefix(device),
                since: replica.cursor,
            }],
        },
    ))
    .unwrap();
    prop_assert_eq!(
        resp.at,
        AdmissionSeq(model.high_water),
        "cut is the high water"
    );

    match (&replica.cursor, &resp.ranges[0]) {
        (None, RangeCut::Snapshot(entries)) => {
            replica.state = entries
                .iter()
                .map(|e| {
                    let Value::Present(v) = &e.value else {
                        panic!("tombstone in snapshot")
                    };
                    (e.key.clone(), v.clone())
                })
                .collect();
        }
        (Some(since), RangeCut::Delta(entries)) => {
            // Ordered by (admission_seq, key), each key at most once, all
            // strictly after the cursor.
            let positions: Vec<(u64, &Key)> = entries
                .iter()
                .map(|e| (e.tag.admission_seq.0, &e.key))
                .collect();
            prop_assert!(positions.windows(2).all(|w| w[0] < w[1]), "delta order");
            prop_assert!(entries.iter().all(|e| e.tag.admission_seq > *since));
            let mut keys: Vec<&Key> = entries.iter().map(|e| &e.key).collect();
            keys.sort();
            keys.dedup();
            prop_assert_eq!(keys.len(), entries.len(), "each key at most once");

            for e in entries {
                match &e.value {
                    Value::Present(v) => {
                        replica.state.insert(e.key.clone(), v.clone());
                    }
                    Value::Absent => {
                        replica.state.remove(&e.key);
                    }
                }
            }
        }
        (cursor, cut) => {
            return Err(TestCaseError::fail(format!(
                "cursor {cursor:?} produced wrong cut variant {cut:?}"
            )));
        }
    }
    replica.cursor = Some(resp.at);

    // The reconstruction invariant: replica state == model live state.
    let expected: BTreeMap<Key, Vec<u8>> = model
        .live(device)
        .into_iter()
        .map(|(s, v)| (user_key(device, s), v))
        .collect();
    prop_assert_eq!(&replica.state, &expected, "replica diverged from authority");
    Ok(())
}

proptest! {
    #[test]
    fn data_plane_matches_model(cmds in prop::collection::vec(arb_cmd(), 1..=60)) {
        let mut h = Harness::new();
        let mut model = Model::default();
        let mut replicas: Vec<Replica> = (0..DEVICES).map(|_| Replica::default()).collect();

        for cmd in cmds {
            match cmd {
                Cmd::Put { device, writes } => {
                    // Build entries with strictly increasing vers, tracking
                    // intra-batch duplicates the way the kernel must.
                    let mut vers: BTreeMap<usize, u64> = BTreeMap::new();
                    let mut entries = Vec::new();
                    let mut staged: Vec<(usize, Option<Vec<u8>>, u64)> = Vec::new();
                    for (suffix, tombstone) in writes {
                        let current = vers.get(&suffix).copied().unwrap_or_else(|| {
                            model.data.get(&(device, suffix)).map_or(0, |e| e.ver)
                        });
                        let ver = current + 1;
                        vers.insert(suffix, ver);
                        let value = if tombstone { None } else { Some(model.fresh_value()) };
                        entries.push(PutEntry {
                            key: user_key(device, suffix),
                            value: match &value {
                                Some(v) => Value::Present(v.clone()),
                                None => Value::Absent,
                            },
                            ver: Ver(ver),
                        });
                        staged.push((suffix, value, ver));
                    }

                    let seq = model.device_seq[device] + 1;
                    let admitted = h.put(device, seq, entries).unwrap();
                    prop_assert_eq!(admitted, AdmissionSeq(model.high_water + 1), "dense seqs");

                    model.high_water += 1;
                    model.device_seq[device] = seq;
                    for (suffix, value, ver) in staged {
                        model.data.insert((device, suffix), MEntry { value, ver });
                    }
                    check_aggregates(&h.store)?;
                }
                Cmd::PutVerRegression { device, suffix } => {
                    // Only meaningful once the key exists.
                    let Some(current) = model.data.get(&(device, suffix)).map(|e| e.ver) else {
                        continue;
                    };
                    let entries = vec![PutEntry {
                        key: user_key(device, suffix),
                        value: Value::Present(b"never-lands".to_vec()),
                        ver: Ver(current), // equal, not greater → regression
                    }];
                    let err = h.put(device, model.device_seq[device] + 1, entries).unwrap_err();
                    prop_assert!(
                        matches!(err, Error::Kernel(KernelError::VerRegression { .. })),
                        "expected ver regression, got {err:?}"
                    );
                    // Rejected: device_seq must still be free.
                }
                Cmd::PutReplay { device, suffix } => {
                    // Only meaningful once the device has admitted something.
                    if model.device_seq[device] == 0 {
                        continue;
                    }
                    let ver = model.data.get(&(device, suffix)).map_or(0, |e| e.ver) + 1;
                    let entries = vec![PutEntry {
                        key: user_key(device, suffix),
                        value: Value::Present(b"never-lands".to_vec()),
                        ver: Ver(ver),
                    }];
                    let err = h.put(device, model.device_seq[device], entries).unwrap_err();
                    prop_assert!(
                        matches!(err, Error::Kernel(KernelError::DeviceSeqRegression { .. })),
                        "expected device_seq regression, got {err:?}"
                    );
                }
                Cmd::Sync { device } => {
                    sync_replica(&h, &model, device, &mut replicas[device])?;
                }
                Cmd::ResetReplica { device } => {
                    replicas[device] = Replica::default();
                }
                Cmd::CheckReads { device } => {
                    check_reads(&h, &model, device)?;
                }
            }
        }

        // Final full audit: aggregates, reads, every replica (after one
        // last sync).
        check_aggregates(&h.store)?;
        for device in 0..DEVICES {
            check_reads(&h, &model, device)?;
            sync_replica(&h, &model, device, &mut replicas[device])?;
        }
    }
}
