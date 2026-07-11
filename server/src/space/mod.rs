//! One space: the complete verb state machine, and (eventually) the
//! client-facing facade.
//!
//! [`Space`] composes the lease table ([`lease::LeaseManager`]) with the
//! data plane ([`data`]) over one [`OrderedStore`]. Every verb takes an
//! explicit `now` and applies at most one atomic write batch, so the whole
//! machine is deterministic under the sim and its counters/leases/data
//! commit or vanish together.
//!
//! The module is laid out for its future shape: `lease` and `data` stay
//! deterministic internals (explicit `now`, store passed in, verbs one at a
//! time), while this file is where `Space` will grow ownership of a store, a
//! [`Clock`](homebase_core::clock::Clock), and request serialization, and
//! implement the async [`Space` trait](homebase_core::space::Space). The
//! struct and the trait deliberately share a name: this is *the*
//! implementation of that contract.

pub mod lease;

mod data;

use crate::error::Error;
use crate::storage::OrderedStore;
use homebase_core::clock::Timestamp;
use homebase_core::messages::{
    AcquireRequest, AcquireResponse, GetRequest, GetResponse, ListLeasesRequest,
    ListLeasesResponse, ListRequest, ListResponse, PutBatchRequest, PutBatchResponse,
    ReadAtRequest, ReadAtResponse, ReleaseRequest, ReleaseResponse, RenewRequest, RenewResponse,
};
use homebase_core::space::SpaceId;
use lease::LeaseManager;

/// The deterministic verb state machine for one space.
pub struct Space {
    id: SpaceId,
    leases: LeaseManager,
}

impl Space {
    pub fn new(id: SpaceId) -> Self {
        Self {
            id,
            leases: LeaseManager::new(id),
        }
    }

    // -- lease verbs -----------------------------------------------------------

    pub async fn acquire<S: OrderedStore>(
        &mut self,
        store: &S,
        now: Timestamp,
        req: &AcquireRequest,
    ) -> Result<AcquireResponse, Error> {
        let leases = self.leases.acquire(store, now, req).await?;
        Ok(AcquireResponse { leases })
    }

    pub async fn renew<S: OrderedStore>(
        &mut self,
        store: &S,
        now: Timestamp,
        req: &RenewRequest,
    ) -> Result<RenewResponse, Error> {
        self.leases.renew(store, now, req).await
    }

    pub async fn release<S: OrderedStore>(
        &mut self,
        store: &S,
        now: Timestamp,
        req: &ReleaseRequest,
    ) -> Result<ReleaseResponse, Error> {
        self.leases.release(store, now, req).await?;
        Ok(ReleaseResponse {})
    }

    pub async fn list_leases<S: OrderedStore>(
        &mut self,
        store: &S,
        now: Timestamp,
        req: &ListLeasesRequest,
    ) -> Result<ListLeasesResponse, Error> {
        self.leases.list_leases(store, now, req).await
    }

    // -- data verbs ------------------------------------------------------------

    pub async fn put_batch<S: OrderedStore>(
        &mut self,
        store: &S,
        now: Timestamp,
        req: &PutBatchRequest,
    ) -> Result<PutBatchResponse, Error> {
        data::put_batch(self.id, &self.leases, store, now, req).await
    }

    pub async fn get<S: OrderedStore>(
        &self,
        store: &S,
        req: &GetRequest,
    ) -> Result<GetResponse, Error> {
        data::get(self.id, store, req).await
    }

    pub async fn list<S: OrderedStore>(
        &self,
        store: &S,
        req: &ListRequest,
    ) -> Result<ListResponse, Error> {
        data::list(self.id, store, req).await
    }

    pub async fn read_at<S: OrderedStore>(
        &self,
        store: &S,
        req: &ReadAtRequest,
    ) -> Result<ReadAtResponse, Error> {
        data::read_at(self.id, store, req).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{PrefixMetaRecord, prefix_meta_key};
    use crate::storage::MemoryStore;
    use homebase_core::clock::HybridTimestamp;
    use homebase_core::key::Key;
    use homebase_core::lease::{LeaseId, LeaseMode};
    use homebase_core::messages::{
        BatchOp, KernelError, LeaseSpec, PutBatch, PutBatchResult, PutEntry, Range, RangeAssert,
        RangeCursor, RangeCut,
    };
    use homebase_core::seal::Seal;
    use homebase_core::tag::{AdmissionSeq, DeviceId, DeviceSeq, Value, Ver};
    use pollster::block_on;
    use std::time::Duration;

    const SPACE: SpaceId = SpaceId([5; 16]);

    fn dev(n: u8) -> DeviceId {
        DeviceId([n; 16])
    }

    fn key(components: &[&[u8]]) -> Key {
        Key::from_bytes(components.iter().copied()).unwrap()
    }

    fn put(k: &Key, value: &[u8], ver: u64) -> BatchOp {
        PutEntry {
            key: k.clone(),
            value: Value::Present(value.to_vec()),
            ver: Ver(ver),
        }
        .into()
    }

    fn del(k: &Key, ver: u64) -> BatchOp {
        PutEntry {
            key: k.clone(),
            value: Value::Absent,
            ver: Ver(ver),
        }
        .into()
    }

    fn prefix_meta(device: DeviceId, seq: u64, live_count: u64) -> PrefixMetaRecord {
        let mut record = PrefixMetaRecord::empty();
        record.observe(device, AdmissionSeq(seq));
        record.live_count = live_count;
        record
    }

    /// Space + store + a write lease over `("db",)` held by device 1.
    fn setup() -> (Space, MemoryStore, LeaseId) {
        let mut space = Space::new(SPACE);
        let store = MemoryStore::new();
        let resp = block_on(space.acquire(
            &store,
            Timestamp(0),
            &AcquireRequest {
                device: dev(1),
                requested_at: HybridTimestamp::ZERO,
                specs: vec![LeaseSpec {
                    prefix: key(&[b"db"]),
                    mode: LeaseMode::Write,
                    ttl: Duration::from_secs(3600),
                }],
            },
        ))
        .unwrap();
        let lease = resp.leases[0].id;
        (space, store, lease)
    }

    fn put_batch(
        space: &mut Space,
        store: &MemoryStore,
        lease: LeaseId,
        device_seq: u64,
        ops: Vec<BatchOp>,
    ) -> Result<PutBatchResponse, Error> {
        put_batch_asserting(space, store, lease, device_seq, Vec::new(), ops)
    }

    fn put_batch_asserting(
        space: &mut Space,
        store: &MemoryStore,
        lease: LeaseId,
        device_seq: u64,
        range_asserts: Vec<RangeAssert>,
        ops: Vec<BatchOp>,
    ) -> Result<PutBatchResponse, Error> {
        block_on(space.put_batch(
            store,
            Timestamp(1),
            &PutBatchRequest {
                device: dev(1),
                evidence: vec![lease],
                batches: vec![PutBatch {
                    device_seq: DeviceSeq(device_seq),
                    range_asserts,
                    ops,
                }],
            },
        ))
    }

    fn put_batch_as(
        space: &mut Space,
        store: &MemoryStore,
        device: DeviceId,
        device_seq: u64,
        range_asserts: Vec<RangeAssert>,
        ops: Vec<BatchOp>,
    ) -> PutBatchResponse {
        block_on(space.put_batch(
            store,
            Timestamp(1),
            &PutBatchRequest {
                device,
                evidence: vec![],
                batches: vec![PutBatch {
                    device_seq: DeviceSeq(device_seq),
                    range_asserts,
                    ops,
                }],
            },
        ))
        .unwrap()
    }

    #[test]
    fn put_then_get_roundtrips_with_tags() {
        let (mut space, store, lease) = setup();
        let k = key(&[b"db", b"t", b"r1"]);
        let resp = put_batch(&mut space, &store, lease, 1, vec![put(&k, b"v", 1)]).unwrap();
        assert_eq!(resp.applied_admission_seq(0), Some(AdmissionSeq(1)));

        let got = block_on(space.get(
            &store,
            &GetRequest {
                keys: vec![k.clone()],
            },
        ))
        .unwrap();
        let entry = got.entries[0].as_ref().unwrap();
        assert_eq!(entry.value, Value::Present(b"v".to_vec()));
        assert_eq!(entry.tag.admission_seq, AdmissionSeq(1));
        assert_eq!(entry.tag.device, dev(1));
        assert_eq!(entry.tag.ver, Ver(1));

        // Unwritten key reads as None.
        let missing = key(&[b"db", b"t", b"nope"]);
        let got = block_on(space.get(
            &store,
            &GetRequest {
                keys: vec![missing],
            },
        ))
        .unwrap();
        assert!(got.entries[0].is_none());
    }

    #[test]
    fn scheme_zero_rejects_non_empty_seal_payload() {
        let (mut space, store, lease) = setup();
        let k = key(&[b"db", b"payload"]);
        let mut seal = Seal::empty_aead_v1();
        seal.payload.push(1);

        let err = put_batch(
            &mut space,
            &store,
            lease,
            1,
            vec![BatchOp::Set {
                key: k,
                ver: Ver(1),
                seal,
                ciphertext: b"v".to_vec(),
            }],
        )
        .unwrap_err();

        assert!(matches!(
            err,
            Error::Kernel(KernelError::InvalidSeal { .. })
        ));
    }

    #[test]
    fn put_batch_coalesces_successive_client_batches_atomically() {
        let (mut space, store, lease) = setup();
        let k1 = key(&[b"db", b"t", b"r1"]);
        let k2 = key(&[b"db", b"t", b"r2"]);
        let resp = block_on(space.put_batch(
            &store,
            Timestamp(1),
            &PutBatchRequest {
                device: dev(1),
                evidence: vec![lease],
                batches: vec![
                    PutBatch {
                        device_seq: DeviceSeq(1),
                        range_asserts: vec![],
                        ops: vec![put(&k1, b"v1", 1)],
                    },
                    PutBatch {
                        device_seq: DeviceSeq(2),
                        range_asserts: vec![],
                        ops: vec![put(&k2, b"v2", 2)],
                    },
                ],
            },
        ))
        .unwrap();
        assert_eq!(resp.results.len(), 2);
        assert_eq!(resp.applied_admission_seq(0), Some(AdmissionSeq(1)));
        assert_eq!(resp.applied_admission_seq(1), Some(AdmissionSeq(2)));

        let got = block_on(space.get(&store, &GetRequest { keys: vec![k1, k2] })).unwrap();
        let first = got.entries[0].as_ref().unwrap();
        let second = got.entries[1].as_ref().unwrap();
        assert_eq!(first.tag.device_seq, DeviceSeq(1));
        assert_eq!(first.tag.admission_seq, AdmissionSeq(1));
        assert_eq!(second.tag.device_seq, DeviceSeq(2));
        assert_eq!(second.tag.admission_seq, AdmissionSeq(2));
    }

    #[test]
    fn range_asserts_allow_earlier_same_device_batches_at_one_offline_cut() {
        let (mut space, store, lease) = setup();
        let root = key(&[b"db"]);
        let k1 = key(&[b"db", b"t", b"r1"]);
        let k2 = key(&[b"db", b"t", b"r2"]);

        let resp = block_on(space.put_batch(
            &store,
            Timestamp(1),
            &PutBatchRequest {
                device: dev(1),
                evidence: vec![lease],
                batches: vec![
                    PutBatch {
                        device_seq: DeviceSeq(1),
                        range_asserts: vec![],
                        ops: vec![put(&k1, b"v1", 1)],
                    },
                    PutBatch {
                        device_seq: DeviceSeq(2),
                        range_asserts: vec![RangeAssert {
                            prefix: root,
                            upto: AdmissionSeq(0),
                        }],
                        ops: vec![put(&k2, b"v2", 1)],
                    },
                ],
            },
        ))
        .unwrap();

        assert_eq!(resp.applied_admission_seq(0), Some(AdmissionSeq(1)));
        assert_eq!(resp.applied_admission_seq(1), Some(AdmissionSeq(2)));
    }

    #[test]
    fn foreign_range_assert_failure_reports_all_batches_and_applies_nothing() {
        let mut space = Space::new(SPACE);
        let store = MemoryStore::new();
        let root = key(&[b"db"]);
        let k1 = key(&[b"db", b"t", b"r1"]);
        let k2 = key(&[b"db", b"t", b"r2"]);

        block_on(space.put_batch(
            &store,
            Timestamp(1),
            &PutBatchRequest {
                device: dev(1),
                evidence: vec![],
                batches: vec![PutBatch {
                    device_seq: DeviceSeq(1),
                    range_asserts: vec![],
                    ops: vec![put(&k1, b"v1", 1)],
                }],
            },
        ))
        .unwrap();

        let resp = block_on(space.put_batch(
            &store,
            Timestamp(1),
            &PutBatchRequest {
                device: dev(2),
                evidence: vec![],
                batches: vec![
                    PutBatch {
                        device_seq: DeviceSeq(1),
                        range_asserts: vec![RangeAssert {
                            prefix: root.clone(),
                            upto: AdmissionSeq(0),
                        }],
                        ops: vec![put(&k2, b"v2", 1)],
                    },
                    PutBatch {
                        device_seq: DeviceSeq(2),
                        range_asserts: vec![RangeAssert {
                            prefix: root.clone(),
                            upto: AdmissionSeq(0),
                        }],
                        ops: vec![],
                    },
                ],
            },
        ))
        .unwrap();

        assert_eq!(resp.results.len(), 2);
        for result in &resp.results {
            let PutBatchResult::Failed {
                error: KernelError::RangeAssertFailed { failures },
            } = result
            else {
                panic!("expected range assert failure, got {result:?}");
            };
            assert_eq!(
                failures,
                &vec![homebase_core::messages::RangeAssertFailure {
                    prefix: root.clone(),
                    upto: AdmissionSeq(0),
                    actual: AdmissionSeq(1),
                }]
            );
        }

        let got = block_on(space.get(
            &store,
            &GetRequest {
                keys: vec![k1.clone()],
            },
        ))
        .unwrap();
        assert_eq!(
            got.entries[0].as_ref().unwrap().value,
            Value::Present(b"v1".to_vec())
        );
        let got = block_on(space.get(&store, &GetRequest { keys: vec![k2] })).unwrap();
        assert!(got.entries[0].is_none());
    }

    #[test]
    fn foreign_history_survives_a_later_own_delete() {
        let mut space = Space::new(SPACE);
        let store = MemoryStore::new();
        let root = key(&[b"db"]);
        let row = key(&[b"db", b"row"]);

        put_batch_as(
            &mut space,
            &store,
            dev(1),
            1,
            vec![],
            vec![put(&row, b"one", 1)],
        );
        put_batch_as(
            &mut space,
            &store,
            dev(2),
            1,
            vec![],
            vec![put(&row, b"foreign", 2)],
        );
        put_batch_as(&mut space, &store, dev(1), 2, vec![], vec![del(&row, 3)]);

        let meta = block_on(store.get(&prefix_meta_key(SPACE, root.components())))
            .unwrap()
            .map(|bytes| PrefixMetaRecord::decode(&bytes).unwrap())
            .unwrap();
        assert_eq!(meta.first.device, dev(1));
        assert_eq!(meta.first.admission_seq, AdmissionSeq(3));
        assert_eq!(meta.second.device, dev(2));
        assert_eq!(meta.second.admission_seq, AdmissionSeq(2));

        let response = put_batch_as(
            &mut space,
            &store,
            dev(1),
            3,
            vec![RangeAssert {
                prefix: root.clone(),
                upto: AdmissionSeq(1),
            }],
            vec![],
        );
        assert!(matches!(
            &response.results[0],
            PutBatchResult::Failed {
                error: KernelError::RangeAssertFailed { failures }
            } if failures == &vec![homebase_core::messages::RangeAssertFailure {
                prefix: root,
                upto: AdmissionSeq(1),
                actual: AdmissionSeq(2),
            }]
        ));
    }

    #[test]
    fn aggregates_track_max_seq_and_live_count() {
        use crate::schema::{PrefixMetaRecord, prefix_meta_key};
        let meta = |store: &MemoryStore, prefix: &Key| -> Option<PrefixMetaRecord> {
            block_on(store.get(&prefix_meta_key(SPACE, prefix.components())))
                .unwrap()
                .map(|bytes| PrefixMetaRecord::decode(&bytes).unwrap())
        };

        let (mut space, store, lease) = setup();
        let k1 = key(&[b"db", b"t", b"r1"]);
        let k2 = key(&[b"db", b"t", b"r2"]);
        let root = key(&[b"db"]);
        let table = key(&[b"db", b"t"]);

        // Never-written prefix: no record at all.
        assert_eq!(meta(&store, &root), None);

        // Two live keys: every ancestor counts both, at seq 2.
        put_batch(&mut space, &store, lease, 1, vec![put(&k1, b"v", 1)]).unwrap();
        put_batch(&mut space, &store, lease, 2, vec![put(&k2, b"v", 1)]).unwrap();
        let expect = prefix_meta(dev(1), 2, 2);
        assert_eq!(meta(&store, &root), Some(expect));
        assert_eq!(meta(&store, &table), Some(expect));
        assert_eq!(
            meta(&store, &k1),
            Some(prefix_meta(dev(1), 1, 1)),
            "leaf prefix untouched by the sibling's write"
        );

        // Overwrite: max seq advances, live count doesn't.
        put_batch(&mut space, &store, lease, 3, vec![put(&k1, b"v2", 2)]).unwrap();
        assert_eq!(meta(&store, &root), Some(prefix_meta(dev(1), 3, 2)));

        // Tombstone: count drops; the record persists at live_count 0.
        put_batch(&mut space, &store, lease, 4, vec![del(&k1, 3)]).unwrap();
        put_batch(&mut space, &store, lease, 5, vec![del(&k2, 2)]).unwrap();
        assert_eq!(meta(&store, &root), Some(prefix_meta(dev(1), 5, 0)));
        assert_eq!(meta(&store, &k1), Some(prefix_meta(dev(1), 4, 0)));

        // Intra-batch create+tombstone of a fresh key nets zero.
        let k3 = key(&[b"db", b"t", b"r3"]);
        put_batch(
            &mut space,
            &store,
            lease,
            6,
            vec![put(&k3, b"blip", 1), del(&k3, 2)],
        )
        .unwrap();
        assert_eq!(meta(&store, &root), Some(prefix_meta(dev(1), 6, 0)));

        // Repeated delete advances high water but keeps live count stable.
        put_batch(&mut space, &store, lease, 7, vec![del(&k1, 4)]).unwrap();
        assert_eq!(meta(&store, &root), Some(prefix_meta(dev(1), 7, 0)));

        // Delete -> present and a zero-length present both count as live.
        let k4 = key(&[b"db", b"t", b"r4"]);
        put_batch(&mut space, &store, lease, 8, vec![put(&k1, b"back", 5)]).unwrap();
        put_batch(&mut space, &store, lease, 9, vec![put(&k4, b"", 1)]).unwrap();
        assert_eq!(meta(&store, &root), Some(prefix_meta(dev(1), 9, 2)));
    }

    #[test]
    fn noop_batch_advances_admission_without_touching_aggregates() {
        use crate::schema::{PrefixMetaRecord, prefix_meta_key};
        let meta = |store: &MemoryStore, prefix: &Key| -> Option<PrefixMetaRecord> {
            block_on(store.get(&prefix_meta_key(SPACE, prefix.components())))
                .unwrap()
                .map(|bytes| PrefixMetaRecord::decode(&bytes).unwrap())
        };

        let (mut space, store, lease) = setup();
        let root = key(&[b"db"]);
        let k = key(&[b"db", b"t", b"r1"]);

        let resp = put_batch(&mut space, &store, lease, 1, vec![BatchOp::NoOp]).unwrap();
        assert_eq!(resp.applied_admission_seq(0), Some(AdmissionSeq(1)));
        assert_eq!(meta(&store, &root), None);

        let resp = put_batch(&mut space, &store, lease, 2, vec![put(&k, b"v", 1)]).unwrap();
        assert_eq!(resp.applied_admission_seq(0), Some(AdmissionSeq(2)));
    }

    #[test]
    fn batch_is_atomic_on_rejection() {
        let (mut space, store, lease) = setup();
        let k1 = key(&[b"db", b"a"]);
        let k2 = key(&[b"db", b"b"]);
        put_batch(&mut space, &store, lease, 1, vec![put(&k1, b"v", 5)]).unwrap();

        // Second batch: valid write to k2, then a ver regression on k1.
        let err = put_batch(
            &mut space,
            &store,
            lease,
            2,
            vec![put(&k2, b"v", 1), put(&k1, b"v", 5)],
        )
        .unwrap_err();
        assert!(matches!(
            err,
            Error::Kernel(KernelError::VerRegression { .. })
        ));

        // Nothing from the rejected batch landed — k2 unwritten, device seq unmoved.
        let got = block_on(space.get(
            &store,
            &GetRequest {
                keys: vec![k2.clone()],
            },
        ))
        .unwrap();
        assert!(got.entries[0].is_none());
        put_batch(&mut space, &store, lease, 2, vec![put(&k2, b"v", 1)])
            .expect("device_seq 2 still available after rejected batch");
    }

    #[test]
    fn device_seq_replays_rejected() {
        let (mut space, store, lease) = setup();
        let k = key(&[b"db", b"k"]);
        put_batch(&mut space, &store, lease, 3, vec![put(&k, b"v", 1)]).unwrap();

        for replayed in [3, 2] {
            let err =
                put_batch(&mut space, &store, lease, replayed, vec![put(&k, b"v", 2)]).unwrap_err();
            assert!(matches!(
                err,
                Error::Kernel(KernelError::DeviceSeqRegression {
                    current: DeviceSeq(3),
                    ..
                })
            ));
        }
    }

    #[test]
    fn intra_batch_ver_checks_are_sequential() {
        let (mut space, store, lease) = setup();
        let k = key(&[b"db", b"k"]);
        // Same key twice in one batch: second must exceed the first…
        put_batch(
            &mut space,
            &store,
            lease,
            1,
            vec![put(&k, b"v1", 1), put(&k, b"v2", 2)],
        )
        .unwrap();
        let got = block_on(space.get(
            &store,
            &GetRequest {
                keys: vec![k.clone()],
            },
        ))
        .unwrap();
        assert_eq!(
            got.entries[0].as_ref().unwrap().value,
            Value::Present(b"v2".to_vec())
        );

        // …and an equal ver within the batch is a regression.
        let err = put_batch(
            &mut space,
            &store,
            lease,
            2,
            vec![put(&k, b"v3", 3), put(&k, b"v4", 3)],
        )
        .unwrap_err();
        assert!(matches!(
            err,
            Error::Kernel(KernelError::VerRegression { .. })
        ));
    }

    #[test]
    fn list_hides_tombstones_and_paginates() {
        let (mut space, store, lease) = setup();
        let keys: Vec<Key> = [b"a" as &[u8], b"b", b"c", b"d"]
            .iter()
            .map(|s| key(&[b"db", s]))
            .collect();
        put_batch(
            &mut space,
            &store,
            lease,
            1,
            keys.iter().map(|k| put(k, b"v", 1)).collect(),
        )
        .unwrap();
        put_batch(&mut space, &store, lease, 2, vec![del(&keys[1], 2)]).unwrap();

        // Tombstoned "b" is hidden.
        let all = block_on(space.list(
            &store,
            &ListRequest {
                prefix: key(&[b"db"]),
                start_after: None,
                limit: None,
            },
        ))
        .unwrap();
        assert_eq!(
            all.entries.iter().map(|e| &e.key).collect::<Vec<_>>(),
            vec![&keys[0], &keys[2], &keys[3]]
        );
        assert!(!all.truncated);

        // Page of 2, then resume strictly after the last returned key.
        let page1 = block_on(space.list(
            &store,
            &ListRequest {
                prefix: key(&[b"db"]),
                start_after: None,
                limit: Some(2),
            },
        ))
        .unwrap();
        assert_eq!(page1.entries.len(), 2);
        assert!(page1.truncated);
        let page2 = block_on(space.list(
            &store,
            &ListRequest {
                prefix: key(&[b"db"]),
                start_after: Some(page1.entries[1].key.clone()),
                limit: Some(2),
            },
        ))
        .unwrap();
        assert_eq!(
            page2.entries.iter().map(|e| &e.key).collect::<Vec<_>>(),
            vec![&keys[3]]
        );
        assert!(!page2.truncated);

        // Exact-limit page: no phantom truncation flag.
        let exact = block_on(space.list(
            &store,
            &ListRequest {
                prefix: key(&[b"db"]),
                start_after: None,
                limit: Some(3),
            },
        ))
        .unwrap();
        assert_eq!(exact.entries.len(), 3);
        assert!(!exact.truncated);
    }

    #[test]
    fn read_at_snapshot_then_delta_reconstructs() {
        let (mut space, store, lease) = setup();
        let ka = key(&[b"db", b"a"]);
        let kb = key(&[b"db", b"b"]);
        put_batch(
            &mut space,
            &store,
            lease,
            1,
            vec![put(&ka, b"a1", 1), put(&kb, b"b1", 1)],
        )
        .unwrap();

        // Snapshot at seq 1.
        let snap = block_on(space.read_at(
            &store,
            &ReadAtRequest {
                ranges: vec![RangeCursor {
                    range: Range::Prefix(key(&[b"db"])),
                    since: None,
                }],
            },
        ))
        .unwrap();
        assert_eq!(snap.at, AdmissionSeq(1));
        let RangeCut::Snapshot(state) = &snap.ranges[0] else {
            panic!("expected snapshot")
        };
        assert_eq!(state.len(), 2);

        // Two more batches: overwrite a, tombstone b.
        put_batch(&mut space, &store, lease, 2, vec![put(&ka, b"a2", 2)]).unwrap();
        put_batch(&mut space, &store, lease, 3, vec![del(&kb, 2)]).unwrap();

        // Delta since the snapshot: each key exactly once, at final state,
        // tombstone visible.
        let delta = block_on(space.read_at(
            &store,
            &ReadAtRequest {
                ranges: vec![RangeCursor {
                    range: Range::Prefix(key(&[b"db"])),
                    since: Some(snap.at),
                }],
            },
        ))
        .unwrap();
        assert_eq!(delta.at, AdmissionSeq(3));
        let RangeCut::Delta(changes) = &delta.ranges[0] else {
            panic!("expected delta")
        };
        assert_eq!(changes.len(), 2);
        assert_eq!(changes[0].key, ka);
        assert_eq!(changes[0].value, Value::Present(b"a2".to_vec()));
        assert_eq!(changes[1].key, kb);
        assert_eq!(changes[1].value, Value::Absent);

        // A key changed twice appears once, at its latest admission seq.
        assert_eq!(changes[0].tag.admission_seq, AdmissionSeq(2));

        // Caught-up cursor: empty delta.
        let empty = block_on(space.read_at(
            &store,
            &ReadAtRequest {
                ranges: vec![RangeCursor {
                    range: Range::Prefix(key(&[b"db"])),
                    since: Some(delta.at),
                }],
            },
        ))
        .unwrap();
        let RangeCut::Delta(changes) = &empty.ranges[0] else {
            panic!("expected delta")
        };
        assert!(changes.is_empty());
    }

    #[test]
    fn read_at_delta_filters_by_prefix() {
        let (mut space, store, lease) = setup();
        let ka = key(&[b"db", b"t1", b"r"]);
        let kb = key(&[b"db", b"t2", b"r"]);
        put_batch(
            &mut space,
            &store,
            lease,
            1,
            vec![put(&ka, b"a", 1), put(&kb, b"b", 1)],
        )
        .unwrap();

        let resp = block_on(space.read_at(
            &store,
            &ReadAtRequest {
                ranges: vec![
                    RangeCursor {
                        range: Range::Prefix(key(&[b"db", b"t1"])),
                        since: Some(AdmissionSeq(0)),
                    },
                    RangeCursor {
                        range: Range::Prefix(key(&[b"db", b"t2"])),
                        since: Some(AdmissionSeq(0)),
                    },
                ],
            },
        ))
        .unwrap();
        let RangeCut::Delta(d1) = &resp.ranges[0] else {
            panic!()
        };
        let RangeCut::Delta(d2) = &resp.ranges[1] else {
            panic!()
        };
        assert_eq!(d1.iter().map(|e| &e.key).collect::<Vec<_>>(), vec![&ka]);
        assert_eq!(d2.iter().map(|e| &e.key).collect::<Vec<_>>(), vec![&kb]);
    }

    #[test]
    fn acquire_barrier_tracks_admissions() {
        let (mut space, store, lease) = setup();
        put_batch(
            &mut space,
            &store,
            lease,
            1,
            vec![put(&key(&[b"db", b"k"]), b"v", 1)],
        )
        .unwrap();

        let resp = block_on(space.acquire(
            &store,
            Timestamp(2),
            &AcquireRequest {
                device: dev(2),
                requested_at: HybridTimestamp::ZERO,
                specs: vec![LeaseSpec {
                    prefix: key(&[b"other"]),
                    mode: LeaseMode::Write,
                    ttl: Duration::from_secs(60),
                }],
            },
        ))
        .unwrap();
        assert_eq!(
            resp.leases[0].barrier,
            AdmissionSeq(1),
            "barrier = admission high water"
        );
    }

    #[test]
    fn writes_without_covering_lease_allowed_when_unreserved() {
        let (mut space, store, lease) = setup();
        // Key outside the leased prefix.
        let outside = key(&[b"elsewhere"]);
        put_batch(&mut space, &store, lease, 1, vec![put(&outside, b"v", 1)]).unwrap();
    }
}
