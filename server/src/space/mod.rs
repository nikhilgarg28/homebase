//! One space: the complete server verb state machine.
//!
//! [`Space`] composes the lease table ([`lease::LeaseManager`]) with the
//! deterministic data plane over one [`OrderedStore`]. Every verb takes an
//! explicit `now` and applies at most one atomic write batch, so the whole
//! machine is deterministic under the sim and its counters/leases/data
//! commit or vanish together.
//!
//! The `lease` and `data` modules remain deterministic internals (explicit
//! `now`, store passed in, verbs one at a time), while this facade composes
//! them into the actor-owned space state machine.

pub mod lease;

mod data;

use crate::error::Error;
use crate::storage::OrderedStore;
use homebase_core::clock::Timestamp;
use homebase_core::messages::{
    AcquireRequest, AcquireResponse, AdmissionRequest, AdmissionResponse, GetRequest, GetResponse,
    ListLeasesRequest, ListLeasesResponse, ListRequest, ListResponse, PullRequest, PullResponse,
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

    pub async fn admit<S: OrderedStore>(
        &mut self,
        store: &S,
        now: Timestamp,
        req: &AdmissionRequest,
    ) -> Result<AdmissionResponse, Error> {
        data::admit(self.id, &self.leases, store, now, req).await
    }

    pub async fn pull<S: OrderedStore>(
        &self,
        store: &S,
        req: &PullRequest,
    ) -> Result<PullResponse, Error> {
        data::pull(self.id, store, req).await
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
    mod randomized;
    mod reference;

    use super::*;
    use crate::schema::{
        DeviceRecord, PrefixMetaRecord, RangeDeleteRecord, device_key, prefix_meta_key,
        range_delete_key, root_meta_key,
    };
    use crate::storage::{MemoryStore, OrderedStore, ScanIter, StorageError, WriteBatch};
    use homebase_core::clock::HybridTimestamp;
    use homebase_core::key::Key;
    use homebase_core::lease::{LeaseId, LeaseMode};
    use homebase_core::messages::{
        AdmissionBatch, AdmissionResult, KernelError, LeaseSpec, PullRequest, Range, RangeAssert,
        RangeCursor, RangeCut,
    };
    use homebase_core::seal::{SEAL_AEAD_TAG_LEN, SEAL_NONCE_LEN, Seal, SealScheme};
    use homebase_core::tag::{
        AdmissionSeq, AdmittedEntry, CipherEpoch, DeviceEntry, DeviceId, DeviceSeq, DeviceTag,
        Mutation, OpaqueValue, Ver,
    };
    use pollster::block_on;
    use reference::ReferenceModel;
    use std::collections::BTreeMap;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;

    const SPACE: SpaceId = SpaceId([5; 16]);

    fn dev(n: u8) -> DeviceId {
        DeviceId([n; 16])
    }

    fn key(components: &[&[u8]]) -> Key {
        Key::from_bytes(components.iter().copied()).unwrap()
    }

    fn put(k: &Key, value: &[u8], ver: u64) -> (Mutation<OpaqueValue>, u64) {
        (
            Mutation::Set {
                key: k.clone(),
                value: OpaqueValue(value.to_vec()),
            },
            ver,
        )
    }

    fn del(k: &Key, ver: u64) -> (Mutation<OpaqueValue>, u64) {
        (Mutation::Delete { key: k.clone() }, ver)
    }

    fn entries_with_vers(
        device: DeviceId,
        device_seq: u64,
        mutations: Vec<(Mutation<OpaqueValue>, u64)>,
    ) -> Vec<DeviceEntry> {
        mutations
            .into_iter()
            .map(|(mutation, ver)| DeviceEntry {
                mutation,
                tag: DeviceTag {
                    device,
                    device_seq: DeviceSeq(device_seq),
                    ver: Ver(ver),
                    cipher_epoch: CipherEpoch(0),
                },
                seal: Seal::empty_aead_v1(),
            })
            .collect()
    }

    fn entry_with_seal(
        device: DeviceId,
        device_seq: u64,
        mutation: Mutation<OpaqueValue>,
        ver: u64,
        seal: Seal,
    ) -> DeviceEntry {
        DeviceEntry {
            mutation,
            tag: DeviceTag {
                device,
                device_seq: DeviceSeq(device_seq),
                ver: Ver(ver),
                cipher_epoch: CipherEpoch(0),
            },
            seal,
        }
    }

    fn seal(n: u8) -> Seal {
        Seal {
            scheme: SealScheme::AeadV1,
            nonce: [n; SEAL_NONCE_LEN],
            aead: [n.wrapping_add(1); SEAL_AEAD_TAG_LEN],
            payload: Vec::new(),
        }
    }

    fn prefix_meta(
        device: DeviceId,
        seq: u64,
        max_ver: u64,
        live_count: u64,
        op_index: u32,
    ) -> PrefixMetaRecord {
        let mut record = PrefixMetaRecord::empty();
        record.observe(
            device,
            AdmissionSeq(seq),
            Ver(max_ver),
            homebase_core::tag::AdmissionOrder {
                admission_seq: AdmissionSeq(seq),
                op_index,
            },
        );
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

    #[derive(Default)]
    struct FailApplyStore {
        inner: MemoryStore,
        fail_next_apply: AtomicBool,
    }

    impl FailApplyStore {
        fn fail_next_apply(&self) {
            self.fail_next_apply.store(true, Ordering::SeqCst);
        }
    }

    impl OrderedStore for FailApplyStore {
        fn get(
            &self,
            key: &[u8],
        ) -> impl Future<Output = Result<Option<Vec<u8>>, StorageError>> + Send {
            self.inner.get(key)
        }

        fn scan(&self, start: Vec<u8>, end: Option<Vec<u8>>) -> impl ScanIter {
            self.inner.scan(start, end)
        }

        async fn apply(&self, batch: WriteBatch) -> Result<(), StorageError> {
            if self.fail_next_apply.swap(false, Ordering::SeqCst) {
                return Err(StorageError("injected apply failure".into()));
            }
            self.inner.apply(batch).await
        }
    }

    fn admit(
        space: &mut Space,
        store: &MemoryStore,
        lease: LeaseId,
        device_seq: u64,
        mutations: Vec<(Mutation<OpaqueValue>, u64)>,
    ) -> Result<AdmissionResponse, Error> {
        admit_asserting(space, store, lease, device_seq, Vec::new(), mutations)
    }

    fn admit_asserting(
        space: &mut Space,
        store: &MemoryStore,
        lease: LeaseId,
        device_seq: u64,
        range_asserts: Vec<RangeAssert>,
        mutations: Vec<(Mutation<OpaqueValue>, u64)>,
    ) -> Result<AdmissionResponse, Error> {
        admit_entries(
            space,
            store,
            lease,
            device_seq,
            range_asserts,
            entries_with_vers(dev(1), device_seq, mutations),
        )
    }

    fn admit_entries(
        space: &mut Space,
        store: &MemoryStore,
        lease: LeaseId,
        device_seq: u64,
        range_asserts: Vec<RangeAssert>,
        entries: Vec<DeviceEntry>,
    ) -> Result<AdmissionResponse, Error> {
        let expected_checksum = block_on(store.get(&device_key(SPACE, dev(1))))
            .unwrap()
            .map(|bytes| DeviceRecord::decode(&bytes).unwrap().checksum)
            .unwrap_or_default();
        block_on(space.admit(
            store,
            Timestamp(1),
            &AdmissionRequest {
                device: dev(1),
                expected_checksum,
                evidence: vec![lease],
                batches: vec![AdmissionBatch {
                    device_seq: DeviceSeq(device_seq),
                    range_asserts,
                    entries,
                }],
            },
        ))
    }

    fn admit_range_entries(
        space: &mut Space,
        store: &MemoryStore,
        lease: LeaseId,
        device_seq: u64,
        entries: Vec<DeviceEntry>,
    ) -> Result<AdmissionResponse, Error> {
        let expected_checksum = block_on(store.get(&device_key(SPACE, dev(1))))
            .unwrap()
            .map(|bytes| DeviceRecord::decode(&bytes).unwrap().checksum)
            .unwrap_or_default();
        block_on(space.admit(
            store,
            Timestamp(1),
            &AdmissionRequest {
                device: dev(1),
                expected_checksum,
                evidence: vec![lease],
                batches: vec![AdmissionBatch {
                    device_seq: DeviceSeq(device_seq),
                    range_asserts: vec![],
                    entries,
                }],
            },
        ))
    }

    fn assert_effective_counts(store: &impl OrderedStore, expected: &[(Range, u64)]) {
        for (range, count) in expected {
            assert_eq!(
                block_on(data::effective_live_count(SPACE, store, range)).unwrap(),
                *count,
                "wrong effective live count for {range:?}"
            );
        }
    }

    fn plaintext_entry(entry: &AdmittedEntry) -> AdmittedEntry<Vec<u8>> {
        let mutation = match &entry.device_entry.mutation {
            Mutation::Set { key, value } => Mutation::Set {
                key: key.clone(),
                value: value.0.clone(),
            },
            Mutation::Delete { key } => Mutation::Delete { key: key.clone() },
            Mutation::DeleteRange { range } => Mutation::DeleteRange {
                range: range.clone(),
            },
        };
        AdmittedEntry {
            device_entry: DeviceEntry {
                mutation,
                tag: entry.device_entry.tag,
                seal: entry.device_entry.seal.clone(),
            },
            admission: entry.admission,
        }
    }

    fn read_delta(
        space: &Space,
        store: &impl OrderedStore,
        range: Range,
        since: AdmissionSeq,
    ) -> (AdmissionSeq, Vec<AdmittedEntry>) {
        let response = block_on(space.read_at(
            store,
            &ReadAtRequest {
                ranges: vec![RangeCursor {
                    range,
                    since: Some(since),
                }],
            },
        ))
        .unwrap();
        let RangeCut::Delta(entries) = response.ranges.into_iter().next().unwrap() else {
            panic!("expected delta")
        };
        (response.at, entries)
    }

    fn admit_as(
        space: &mut Space,
        store: &MemoryStore,
        device: DeviceId,
        device_seq: u64,
        range_asserts: Vec<RangeAssert>,
        mutations: Vec<(Mutation<OpaqueValue>, u64)>,
    ) -> AdmissionResponse {
        let expected_checksum = block_on(store.get(&device_key(SPACE, device)))
            .unwrap()
            .map(|bytes| DeviceRecord::decode(&bytes).unwrap().checksum)
            .unwrap_or_default();
        block_on(space.admit(
            store,
            Timestamp(1),
            &AdmissionRequest {
                device,
                expected_checksum,
                evidence: vec![],
                batches: vec![AdmissionBatch {
                    device_seq: DeviceSeq(device_seq),
                    range_asserts,
                    entries: entries_with_vers(device, device_seq, mutations),
                }],
            },
        ))
        .unwrap()
    }

    #[test]
    fn put_then_get_roundtrips_with_tags() {
        let (mut space, store, lease) = setup();
        let k = key(&[b"db", b"t", b"r1"]);
        let resp = admit(&mut space, &store, lease, 1, vec![put(&k, b"v", 1)]).unwrap();
        assert_eq!(resp.applied_admission_seq(0), Some(AdmissionSeq(1)));

        let got = block_on(space.get(
            &store,
            &GetRequest {
                keys: vec![k.clone()],
            },
        ))
        .unwrap();
        let entry = got.entries[0].as_ref().unwrap();
        assert_eq!(
            entry.device_entry.mutation,
            Mutation::Set {
                key: k,
                value: OpaqueValue(b"v".to_vec())
            }
        );
        assert_eq!(entry.admission.admission_seq, AdmissionSeq(1));
        assert_eq!(entry.admission.op_index, 0);
        assert_eq!(entry.device_entry.tag.device, dev(1));
        assert_eq!(entry.device_entry.tag.ver, Ver(1));

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

        let err = admit_entries(
            &mut space,
            &store,
            lease,
            1,
            vec![],
            vec![entry_with_seal(
                dev(1),
                1,
                Mutation::Set {
                    key: k,
                    value: OpaqueValue(b"v".to_vec()),
                },
                1,
                seal,
            )],
        )
        .unwrap_err();

        assert!(matches!(
            err,
            Error::Kernel(KernelError::InvalidSeal { .. })
        ));
    }

    #[test]
    fn covering_range_delete_lookup_chooses_newest_order_not_deepest_prefix() {
        let store = MemoryStore::new();
        let target = Range::Prefix(key(&[b"db", b"row", b"child"]));
        let record = |range: Range, seq: u64, op_index: u32| {
            RangeDeleteRecord::new(AdmittedEntry {
                device_entry: DeviceEntry {
                    mutation: Mutation::DeleteRange { range },
                    tag: DeviceTag {
                        device: dev(1),
                        device_seq: DeviceSeq(seq),
                        ver: Ver(seq),
                        cipher_epoch: CipherEpoch(0),
                    },
                    seal: Seal::empty_aead_v1(),
                },
                admission: homebase_core::tag::AdmissionTag {
                    admission_seq: AdmissionSeq(seq),
                    op_index,
                },
            })
        };
        let full = record(Range::Full, 5, 0);
        let parent_range = Range::Prefix(key(&[b"db"]));
        let parent = record(parent_range.clone(), 9, 0);
        let exact_range = Range::Prefix(key(&[b"db", b"row"]));
        let exact = record(exact_range.clone(), 8, 0);
        let sibling_range = Range::Prefix(key(&[b"db", b"other"]));
        let sibling = record(sibling_range.clone(), 12, 0);
        let mut batch = WriteBatch::new();
        for (range, record) in [
            (Range::Full, &full),
            (parent_range, &parent),
            (exact_range.clone(), &exact),
            (sibling_range, &sibling),
        ] {
            batch.put(range_delete_key(SPACE, &range), record.encode());
        }
        block_on(store.apply(batch)).unwrap();

        let found = block_on(data::covering_range_delete(SPACE, &store, &target))
            .unwrap()
            .unwrap();
        assert_eq!(found, parent, "newest ancestor beats deepest ancestor");

        let same_batch_later = record(exact_range.clone(), 9, 1);
        let mut batch = WriteBatch::new();
        batch.put(
            range_delete_key(SPACE, &exact_range),
            same_batch_later.encode(),
        );
        block_on(store.apply(batch)).unwrap();
        let found = block_on(data::covering_range_delete(SPACE, &store, &target))
            .unwrap()
            .unwrap();
        assert_eq!(found, same_batch_later);

        let unrelated = Range::Prefix(key(&[b"outside"]));
        let found = block_on(data::covering_range_delete(SPACE, &store, &unrelated))
            .unwrap()
            .unwrap();
        assert_eq!(found, full, "only Full covers an unrelated prefix");
    }

    #[test]
    fn effective_history_merges_descendants_and_covering_range_events() {
        let store = MemoryStore::new();
        let query_prefix = key(&[b"db", b"row"]);
        let query = Range::Prefix(query_prefix.clone());
        let mut aggregate = PrefixMetaRecord::empty();
        aggregate.observe(
            dev(1),
            AdmissionSeq(7),
            Ver(70),
            homebase_core::tag::AdmissionOrder {
                admission_seq: AdmissionSeq(7),
                op_index: 0,
            },
        );

        let range_entry = |range: Range, device: DeviceId, seq: u64, ver: u64| AdmittedEntry {
            device_entry: DeviceEntry {
                mutation: Mutation::DeleteRange { range },
                tag: DeviceTag {
                    device,
                    device_seq: DeviceSeq(seq),
                    ver: Ver(ver),
                    cipher_epoch: CipherEpoch(0),
                },
                seal: Seal::empty_aead_v1(),
            },
            admission: homebase_core::tag::AdmissionTag {
                admission_seq: AdmissionSeq(seq),
                op_index: 0,
            },
        };
        let parent_range = Range::Prefix(key(&[b"db"]));
        let mut parent = RangeDeleteRecord::new(range_entry(parent_range.clone(), dev(1), 8, 80));
        parent.observe(range_entry(parent_range.clone(), dev(2), 9, 90));
        let full = RangeDeleteRecord::new(range_entry(Range::Full, dev(3), 6, 60));

        let mut batch = WriteBatch::new();
        batch.put(
            prefix_meta_key(SPACE, query_prefix.components()),
            aggregate.encode(),
        );
        batch.put(range_delete_key(SPACE, &parent_range), parent.encode());
        batch.put(range_delete_key(SPACE, &Range::Full), full.encode());
        block_on(store.apply(batch)).unwrap();

        let effective = block_on(data::effective_history(SPACE, &store, &query)).unwrap();
        assert_eq!(effective.history.max_admission_seq(), AdmissionSeq(9));
        assert_eq!(effective.max_excluding(dev(2)), AdmissionSeq(8));
        assert_eq!(effective.max_ver, Ver(90));
    }

    #[test]
    fn delete_range_is_publicly_admitted_with_dense_state() {
        let (mut space, store, lease) = setup();
        let deleted = admit(
            &mut space,
            &store,
            lease,
            1,
            vec![(
                Mutation::DeleteRange {
                    range: Range::Prefix(key(&[b"db"])),
                },
                1,
            )],
        )
        .unwrap();
        assert_eq!(deleted.applied_admission_seq(0), Some(AdmissionSeq(1)));

        let k = key(&[b"db", b"after-range"]);
        let response = admit(&mut space, &store, lease, 2, vec![put(&k, b"v", 2)]).unwrap();
        assert_eq!(response.applied_admission_seq(0), Some(AdmissionSeq(2)));
        assert!(
            block_on(space.get(&store, &GetRequest { keys: vec![k] }))
                .unwrap()
                .entries[0]
                .is_some()
        );
    }

    #[test]
    fn public_mixed_pairs_refine_reference_visibility_and_exact_log_order() {
        fn mutation(kind: u8, value: u8) -> Mutation<OpaqueValue> {
            match kind {
                0 => Mutation::Set {
                    key: key(&[b"db", b"a"]),
                    value: OpaqueValue(vec![value]),
                },
                1 => Mutation::Set {
                    key: key(&[b"db", b"b"]),
                    value: OpaqueValue(vec![value]),
                },
                2 => Mutation::Delete {
                    key: key(&[b"db", b"a"]),
                },
                3 => Mutation::DeleteRange {
                    range: Range::Prefix(key(&[b"db"])),
                },
                4 => Mutation::DeleteRange {
                    range: Range::Prefix(key(&[b"db", b"a"])),
                },
                5 => Mutation::DeleteRange { range: Range::Full },
                _ => unreachable!(),
            }
        }
        fn plaintext(mutation: Mutation<OpaqueValue>) -> Mutation<Vec<u8>> {
            match mutation {
                Mutation::Set { key, value } => Mutation::Set {
                    key,
                    value: value.0,
                },
                Mutation::Delete { key } => Mutation::Delete { key },
                Mutation::DeleteRange { range } => Mutation::DeleteRange { range },
            }
        }
        fn visible(model: &ReferenceModel) -> BTreeMap<Key, Vec<u8>> {
            model
                .list_at(&Range::Full, model.high_water())
                .into_iter()
                .map(|entry| match entry.device_entry.mutation {
                    Mutation::Set { key, value } => (key, value),
                    Mutation::Delete { .. } | Mutation::DeleteRange { .. } => unreachable!(),
                })
                .collect()
        }

        for first in 0..6 {
            for second in 0..6 {
                let (mut space, store, lease) = setup();
                let mutations = [mutation(first, 1), mutation(second, 2)];
                let entries = entries_with_vers(
                    dev(1),
                    1,
                    vec![(mutations[0].clone(), 1), (mutations[1].clone(), 2)],
                );
                admit_range_entries(&mut space, &store, lease, 1, entries.clone()).unwrap();

                let mut model = ReferenceModel::default();
                model.append_batch(
                    dev(1),
                    DeviceSeq(1),
                    vec![
                        (plaintext(mutations[0].clone()), Ver(1)),
                        (plaintext(mutations[1].clone()), Ver(2)),
                    ],
                );
                let listed = block_on(space.list(
                    &store,
                    &ListRequest {
                        prefix: key(&[b"db"]),
                        start_after: None,
                        limit: None,
                    },
                ))
                .unwrap()
                .entries
                .into_iter()
                .map(|entry| match entry.device_entry.mutation {
                    Mutation::Set { key, value } => (key, value.0),
                    Mutation::Delete { .. } | Mutation::DeleteRange { .. } => unreachable!(),
                })
                .collect::<BTreeMap<_, _>>();
                assert_eq!(listed, visible(&model), "ordered pair {first}, {second}");
                for range in [
                    Range::Full,
                    Range::Prefix(key(&[b"db"])),
                    Range::Prefix(key(&[b"db", b"a"])),
                    Range::Prefix(key(&[b"db", b"b"])),
                ] {
                    assert_eq!(
                        block_on(data::effective_live_count(SPACE, &store, &range)).unwrap(),
                        model.live_count(&range, model.high_water()),
                        "ordered pair {first}, {second}, range {range:?}"
                    );
                }

                let pulled = block_on(space.pull(
                    &store,
                    &PullRequest {
                        after: AdmissionSeq(0),
                        max_batches: None,
                    },
                ))
                .unwrap();
                assert_eq!(pulled.batches.len(), 1);
                assert_eq!(
                    pulled.batches[0]
                        .entries
                        .iter()
                        .map(|entry| &entry.device_entry.mutation)
                        .collect::<Vec<_>>(),
                    entries
                        .iter()
                        .map(|entry| &entry.mutation)
                        .collect::<Vec<_>>()
                );
            }
        }
    }

    #[test]
    fn lazy_counts_handle_nested_resets_revivals_and_hidden_point_deletes() {
        let (mut space, store, lease) = setup();
        let db = Range::Prefix(key(&[b"db"]));
        let child = Range::Prefix(key(&[b"db", b"child"]));
        let outside = Range::Prefix(key(&[b"outside"]));
        let a = key(&[b"db", b"a"]);
        let b = key(&[b"db", b"child", b"b"]);
        let c = key(&[b"outside", b"c"]);

        admit_range_entries(
            &mut space,
            &store,
            lease,
            1,
            entries_with_vers(
                dev(1),
                1,
                vec![put(&a, b"a", 1), put(&b, b"b", 2), put(&c, b"c", 3)],
            ),
        )
        .unwrap();
        assert_effective_counts(
            &store,
            &[(Range::Full, 3), (db.clone(), 2), (child.clone(), 1)],
        );

        for (seq, ver) in [(2, 4), (3, 5)] {
            admit_range_entries(
                &mut space,
                &store,
                lease,
                seq,
                entries_with_vers(
                    dev(1),
                    seq,
                    vec![(
                        Mutation::DeleteRange {
                            range: child.clone(),
                        },
                        ver,
                    )],
                ),
            )
            .unwrap();
            assert_effective_counts(
                &store,
                &[(Range::Full, 2), (db.clone(), 1), (child.clone(), 0)],
            );
        }

        admit_range_entries(
            &mut space,
            &store,
            lease,
            4,
            entries_with_vers(dev(1), 4, vec![put(&b, b"revived", 6)]),
        )
        .unwrap();
        assert_effective_counts(
            &store,
            &[(Range::Full, 3), (db.clone(), 2), (child.clone(), 1)],
        );

        admit_range_entries(
            &mut space,
            &store,
            lease,
            5,
            entries_with_vers(
                dev(1),
                5,
                vec![(Mutation::DeleteRange { range: db.clone() }, 7)],
            ),
        )
        .unwrap();
        assert_effective_counts(
            &store,
            &[
                (Range::Full, 1),
                (db.clone(), 0),
                (child.clone(), 0),
                (outside, 1),
            ],
        );

        // The retained point Set is already hidden, so a later point Delete
        // advances history without decrementing any count.
        admit_range_entries(
            &mut space,
            &store,
            lease,
            6,
            entries_with_vers(dev(1), 6, vec![del(&b, 8)]),
        )
        .unwrap();
        assert_effective_counts(
            &store,
            &[(Range::Full, 1), (db.clone(), 0), (child.clone(), 0)],
        );

        let revived = key(&[b"db", b"child", b"new"]);
        admit_range_entries(
            &mut space,
            &store,
            lease,
            7,
            entries_with_vers(dev(1), 7, vec![put(&revived, b"new", 9)]),
        )
        .unwrap();
        assert_effective_counts(
            &store,
            &[(Range::Full, 2), (db.clone(), 1), (child.clone(), 1)],
        );

        admit_range_entries(
            &mut space,
            &store,
            lease,
            8,
            entries_with_vers(
                dev(1),
                8,
                vec![(Mutation::DeleteRange { range: Range::Full }, 10)],
            ),
        )
        .unwrap();
        assert_effective_counts(&store, &[(Range::Full, 0), (db, 0), (child, 0)]);
    }

    #[test]
    fn failed_range_reset_exposes_neither_count_nor_tombstone() {
        let mut space = Space::new(SPACE);
        let store = FailApplyStore::default();
        let row = key(&[b"db", b"row"]);
        let first = block_on(space.admit(
            &store,
            Timestamp(1),
            &AdmissionRequest {
                device: dev(1),
                expected_checksum: homebase_core::DeviceChecksum::EMPTY,
                evidence: vec![],
                batches: vec![AdmissionBatch {
                    device_seq: DeviceSeq(1),
                    range_asserts: vec![],
                    entries: entries_with_vers(dev(1), 1, vec![put(&row, b"v", 1)]),
                }],
            },
        ))
        .unwrap();
        let db = Range::Prefix(key(&[b"db"]));
        assert_effective_counts(&store, &[(Range::Full, 1), (db.clone(), 1)]);

        store.fail_next_apply();
        let failed = block_on(space.admit(
            &store,
            Timestamp(1),
            &AdmissionRequest {
                device: dev(1),
                expected_checksum: first.checksum,
                evidence: vec![],
                batches: vec![AdmissionBatch {
                    device_seq: DeviceSeq(2),
                    range_asserts: vec![],
                    entries: entries_with_vers(
                        dev(1),
                        2,
                        vec![(Mutation::DeleteRange { range: db.clone() }, 2)],
                    ),
                }],
            },
        ));
        assert!(matches!(failed, Err(Error::Storage(_))));
        assert_effective_counts(&store, &[(Range::Full, 1), (db.clone(), 1)]);
        assert!(
            block_on(store.get(&range_delete_key(SPACE, &db)))
                .unwrap()
                .is_none()
        );

        let pulled = block_on(space.pull(
            &store,
            &PullRequest {
                after: AdmissionSeq(0),
                max_batches: None,
            },
        ))
        .unwrap();
        assert_eq!(pulled.through, AdmissionSeq(1));
        assert_eq!(pulled.batches.len(), 1);
    }

    #[test]
    fn lazy_counts_preserve_order_across_coalesced_client_batches() {
        let mut space = Space::new(SPACE);
        let store = MemoryStore::new();
        let db = Range::Prefix(key(&[b"db"]));
        let a = key(&[b"db", b"a"]);
        let b = key(&[b"db", b"b"]);
        let mutations = [
            Mutation::Set {
                key: a.clone(),
                value: OpaqueValue(b"a".to_vec()),
            },
            Mutation::DeleteRange { range: db.clone() },
            Mutation::Set {
                key: b.clone(),
                value: OpaqueValue(b"b".to_vec()),
            },
        ];
        let batches = mutations
            .iter()
            .cloned()
            .enumerate()
            .map(|(index, mutation)| {
                let seq = u64::try_from(index + 1).unwrap();
                AdmissionBatch {
                    device_seq: DeviceSeq(seq),
                    range_asserts: vec![],
                    entries: entries_with_vers(dev(1), seq, vec![(mutation, seq)]),
                }
            })
            .collect();
        let response = block_on(space.admit(
            &store,
            Timestamp(1),
            &AdmissionRequest {
                device: dev(1),
                expected_checksum: homebase_core::DeviceChecksum::EMPTY,
                evidence: vec![],
                batches,
            },
        ))
        .unwrap();

        assert_eq!(
            (0..3)
                .map(|index| response.applied_admission_seq(index))
                .collect::<Vec<_>>(),
            vec![
                Some(AdmissionSeq(1)),
                Some(AdmissionSeq(2)),
                Some(AdmissionSeq(3)),
            ]
        );
        assert_effective_counts(&store, &[(Range::Full, 1), (db, 1)]);
        let got = block_on(space.get(&store, &GetRequest { keys: vec![a, b] })).unwrap();
        assert!(got.entries[0].is_none());
        assert!(got.entries[1].is_some());
    }

    #[test]
    #[should_panic(expected = "live count underflow: range count exceeds ancestor count")]
    fn range_count_underflow_is_detected_as_corruption() {
        let (mut space, store, lease) = setup();
        let db_key = key(&[b"db"]);
        let mut child = PrefixMetaRecord::empty();
        child.live_count = 1;
        let mut corrupt = WriteBatch::new();
        corrupt.put(root_meta_key(SPACE), PrefixMetaRecord::empty().encode());
        corrupt.put(prefix_meta_key(SPACE, db_key.components()), child.encode());
        block_on(store.apply(corrupt)).unwrap();

        admit_range_entries(
            &mut space,
            &store,
            lease,
            1,
            entries_with_vers(
                dev(1),
                1,
                vec![(
                    Mutation::DeleteRange {
                        range: Range::Prefix(db_key),
                    },
                    1,
                )],
            ),
        )
        .unwrap();
    }

    #[test]
    fn public_range_pull_is_dense_bounded_and_exact() {
        let mut space = Space::new(SPACE);
        let store = MemoryStore::new();
        let db = Range::Prefix(key(&[b"db"]));
        let row = key(&[b"db", b"row"]);
        let mutations = [
            vec![
                put(&row, b"v", 1),
                (Mutation::DeleteRange { range: db.clone() }, 2),
            ],
            vec![],
            vec![(Mutation::DeleteRange { range: Range::Full }, 3)],
        ];
        let batches = mutations
            .into_iter()
            .enumerate()
            .map(|(index, mutations)| {
                let seq = u64::try_from(index + 1).unwrap();
                AdmissionBatch {
                    device_seq: DeviceSeq(seq),
                    range_asserts: vec![],
                    entries: entries_with_vers(dev(1), seq, mutations),
                }
            })
            .collect();
        block_on(space.admit(
            &store,
            Timestamp(1),
            &AdmissionRequest {
                device: dev(1),
                expected_checksum: homebase_core::DeviceChecksum::EMPTY,
                evidence: vec![],
                batches,
            },
        ))
        .unwrap();

        let first = block_on(space.pull(
            &store,
            &PullRequest {
                after: AdmissionSeq(0),
                max_batches: Some(2),
            },
        ))
        .unwrap();
        assert_eq!(first.through, AdmissionSeq(2));
        assert_eq!(first.batches.len(), 2);
        assert_eq!(first.batches[0].entries.len(), 2);
        assert!(matches!(
            &first.batches[0].entries[1].device_entry.mutation,
            Mutation::DeleteRange { range } if range == &db
        ));
        assert!(first.batches[1].entries.is_empty());

        let second = block_on(space.pull(
            &store,
            &PullRequest {
                after: first.through,
                max_batches: Some(2),
            },
        ))
        .unwrap();
        assert_eq!(second.through, AdmissionSeq(3));
        assert_eq!(second.batches.len(), 1);
        assert!(matches!(
            &second.batches[0].entries[0].device_entry.mutation,
            Mutation::DeleteRange { range: Range::Full }
        ));
    }

    #[test]
    fn scoped_range_deltas_match_reference_for_ancestors_descendants_and_siblings() {
        let (mut space, store, lease) = setup();
        let parent = Range::Prefix(key(&[b"db"]));
        let child = Range::Prefix(key(&[b"db", b"child"]));
        let sibling = Range::Prefix(key(&[b"db", b"sibling"]));
        let child_key = key(&[b"db", b"child", b"row"]);
        let sibling_key = key(&[b"db", b"sibling", b"row"]);
        let commands = vec![
            vec![put(&child_key, b"old", 1)],
            vec![(
                Mutation::DeleteRange {
                    range: parent.clone(),
                },
                2,
            )],
            vec![put(&child_key, b"new", 3)],
            vec![(
                Mutation::DeleteRange {
                    range: child.clone(),
                },
                4,
            )],
            vec![put(&sibling_key, b"sibling", 5)],
        ];
        let mut model = ReferenceModel::default();
        for (index, mutations) in commands.into_iter().enumerate() {
            let seq = u64::try_from(index + 1).unwrap();
            let entries = entries_with_vers(dev(1), seq, mutations.clone());
            admit_range_entries(&mut space, &store, lease, seq, entries).unwrap();
            model.append_batch(
                dev(1),
                DeviceSeq(seq),
                mutations
                    .into_iter()
                    .map(|(mutation, ver)| {
                        let mutation = match mutation {
                            Mutation::Set { key, value } => Mutation::Set {
                                key,
                                value: value.0,
                            },
                            Mutation::Delete { key } => Mutation::Delete { key },
                            Mutation::DeleteRange { range } => Mutation::DeleteRange { range },
                        };
                        (mutation, Ver(ver))
                    })
                    .collect(),
            );
        }

        for query in [
            parent.clone(),
            child.clone(),
            sibling.clone(),
            Range::Prefix(child_key.clone()),
            Range::Full,
        ] {
            let (at, actual) = read_delta(&space, &store, query.clone(), AdmissionSeq(0));
            let expected = model.read(&query, Some(AdmissionSeq(0)));
            assert_eq!(at, expected.at, "cut for {query:?}");
            let RangeCut::Delta(expected) = expected.cut else {
                unreachable!()
            };
            assert_eq!(
                actual.iter().map(plaintext_entry).collect::<Vec<_>>(),
                expected,
                "sources for {query:?}"
            );
        }

        let (_, child_after_first) = read_delta(&space, &store, child.clone(), AdmissionSeq(1));
        assert_eq!(
            child_after_first
                .iter()
                .map(|entry| entry.admission.admission_seq)
                .collect::<Vec<_>>(),
            vec![AdmissionSeq(2), AdmissionSeq(3), AdmissionSeq(4)]
        );
        assert!(matches!(
            &child_after_first[0].device_entry.mutation,
            Mutation::DeleteRange { range } if range == &parent
        ));

        let (at, empty) = read_delta(&space, &store, child, AdmissionSeq(5));
        assert_eq!(at, AdmissionSeq(5));
        assert!(empty.is_empty());
    }

    #[test]
    fn public_range_and_point_version_fences_reject_atomically() {
        let (mut space, store, lease) = setup();
        let row = key(&[b"db", b"row"]);
        admit(&mut space, &store, lease, 1, vec![put(&row, b"old", 10)]).unwrap();

        let stale_range = entries_with_vers(
            dev(1),
            2,
            vec![(
                Mutation::DeleteRange {
                    range: Range::Prefix(key(&[b"db"])),
                },
                10,
            )],
        );
        assert!(matches!(
            admit_range_entries(&mut space, &store, lease, 2, stale_range),
            Err(Error::Kernel(KernelError::RangeVerRegression {
                current: Ver(10),
                attempted: Ver(10),
                ..
            }))
        ));

        let valid_range = entries_with_vers(
            dev(1),
            2,
            vec![(
                Mutation::DeleteRange {
                    range: Range::Prefix(key(&[b"db"])),
                },
                11,
            )],
        );
        admit_range_entries(&mut space, &store, lease, 2, valid_range).unwrap();
        assert!(
            block_on(space.get(
                &store,
                &GetRequest {
                    keys: vec![row.clone()]
                }
            ))
            .unwrap()
            .entries[0]
                .is_none()
        );

        let stale_revive = entries_with_vers(
            dev(1),
            3,
            vec![(
                Mutation::Set {
                    key: row.clone(),
                    value: OpaqueValue(b"stale".to_vec()),
                },
                11,
            )],
        );
        assert!(matches!(
            admit_range_entries(&mut space, &store, lease, 3, stale_revive),
            Err(Error::Kernel(KernelError::VerRegression {
                current: Ver(11),
                attempted: Ver(11),
                ..
            }))
        ));
        let valid_revive = entries_with_vers(
            dev(1),
            3,
            vec![(
                Mutation::Set {
                    key: row.clone(),
                    value: OpaqueValue(b"new".to_vec()),
                },
                12,
            )],
        );
        admit_range_entries(&mut space, &store, lease, 3, valid_revive).unwrap();
        assert!(
            block_on(space.get(&store, &GetRequest { keys: vec![row] }))
                .unwrap()
                .entries[0]
                .is_some()
        );
    }

    #[test]
    fn public_mixed_rejection_discards_the_entire_staged_prefix() {
        let (mut space, store, lease) = setup();
        let row = key(&[b"db", b"row"]);
        let rejected = entries_with_vers(
            dev(1),
            1,
            vec![
                (
                    Mutation::Set {
                        key: row.clone(),
                        value: OpaqueValue(b"staged".to_vec()),
                    },
                    2,
                ),
                (
                    Mutation::DeleteRange {
                        range: Range::Prefix(key(&[b"db"])),
                    },
                    1,
                ),
            ],
        );
        assert!(matches!(
            admit_range_entries(&mut space, &store, lease, 1, rejected),
            Err(Error::Kernel(KernelError::RangeVerRegression {
                current: Ver(2),
                attempted: Ver(1),
                ..
            }))
        ));
        assert!(
            block_on(space.get(
                &store,
                &GetRequest {
                    keys: vec![row.clone()]
                }
            ))
            .unwrap()
            .entries[0]
                .is_none()
        );
        let pull = block_on(space.pull(
            &store,
            &PullRequest {
                after: AdmissionSeq(0),
                max_batches: None,
            },
        ))
        .unwrap();
        assert_eq!(pull.through, AdmissionSeq(0));
        assert!(pull.batches.is_empty());

        let accepted = entries_with_vers(
            dev(1),
            1,
            vec![
                (
                    Mutation::Set {
                        key: row,
                        value: OpaqueValue(b"staged".to_vec()),
                    },
                    2,
                ),
                (
                    Mutation::DeleteRange {
                        range: Range::Prefix(key(&[b"db"])),
                    },
                    3,
                ),
            ],
        );
        let response = admit_range_entries(&mut space, &store, lease, 1, accepted).unwrap();
        assert_eq!(response.applied_admission_seq(0), Some(AdmissionSeq(1)));
    }

    #[test]
    fn publicly_admitted_range_history_invalidates_parent_and_child_assertions() {
        let mut space = Space::new(SPACE);
        let store = MemoryStore::new();
        let deleted = Range::Prefix(key(&[b"db", b"child"]));
        let request = AdmissionRequest {
            device: dev(2),
            expected_checksum: homebase_core::DeviceChecksum::EMPTY,
            evidence: vec![],
            batches: vec![AdmissionBatch {
                device_seq: DeviceSeq(1),
                range_asserts: vec![],
                entries: entries_with_vers(
                    dev(2),
                    1,
                    vec![(
                        Mutation::DeleteRange {
                            range: deleted.clone(),
                        },
                        1,
                    )],
                ),
            }],
        };
        block_on(space.admit(&store, Timestamp(1), &request)).unwrap();

        let response = block_on(space.admit(
            &store,
            Timestamp(1),
            &AdmissionRequest {
                device: dev(1),
                expected_checksum: homebase_core::DeviceChecksum::EMPTY,
                evidence: vec![],
                batches: vec![AdmissionBatch {
                    device_seq: DeviceSeq(1),
                    range_asserts: vec![
                        RangeAssert {
                            prefix: key(&[b"db"]),
                            upto: AdmissionSeq(0),
                        },
                        RangeAssert {
                            prefix: key(&[b"db", b"child", b"grandchild"]),
                            upto: AdmissionSeq(0),
                        },
                    ],
                    entries: vec![],
                }],
            },
        ))
        .unwrap();
        let AdmissionResult::Failed {
            error: KernelError::RangeAssertFailed { failures },
        } = &response.results[0]
        else {
            panic!("range assertions unexpectedly passed")
        };
        assert_eq!(failures.len(), 2);
        assert!(
            failures
                .iter()
                .all(|failure| failure.actual == AdmissionSeq(1))
        );
    }

    #[test]
    fn admit_coalesces_successive_client_batches_atomically() {
        let (mut space, store, lease) = setup();
        let k1 = key(&[b"db", b"t", b"r1"]);
        let k2 = key(&[b"db", b"t", b"r2"]);
        let resp = block_on(space.admit(
            &store,
            Timestamp(1),
            &AdmissionRequest {
                device: dev(1),
                expected_checksum: homebase_core::DeviceChecksum::EMPTY,
                evidence: vec![lease],
                batches: vec![
                    AdmissionBatch {
                        device_seq: DeviceSeq(1),
                        range_asserts: vec![],
                        entries: entries_with_vers(dev(1), 1, vec![put(&k1, b"v1", 1)]),
                    },
                    AdmissionBatch {
                        device_seq: DeviceSeq(2),
                        range_asserts: vec![],
                        entries: entries_with_vers(dev(1), 2, vec![put(&k2, b"v2", 2)]),
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
        assert_eq!(first.device_entry.tag.device_seq, DeviceSeq(1));
        assert_eq!(first.admission.admission_seq, AdmissionSeq(1));
        assert_eq!(first.admission.op_index, 0);
        assert_eq!(second.device_entry.tag.device_seq, DeviceSeq(2));
        assert_eq!(second.admission.admission_seq, AdmissionSeq(2));
        assert_eq!(second.admission.op_index, 0);
    }

    #[test]
    fn range_asserts_allow_earlier_same_device_batches_at_one_offline_cut() {
        let (mut space, store, lease) = setup();
        let root = key(&[b"db"]);
        let k1 = key(&[b"db", b"t", b"r1"]);
        let k2 = key(&[b"db", b"t", b"r2"]);

        let resp = block_on(space.admit(
            &store,
            Timestamp(1),
            &AdmissionRequest {
                device: dev(1),
                expected_checksum: homebase_core::DeviceChecksum::EMPTY,
                evidence: vec![lease],
                batches: vec![
                    AdmissionBatch {
                        device_seq: DeviceSeq(1),
                        range_asserts: vec![],
                        entries: entries_with_vers(dev(1), 1, vec![put(&k1, b"v1", 1)]),
                    },
                    AdmissionBatch {
                        device_seq: DeviceSeq(2),
                        range_asserts: vec![RangeAssert {
                            prefix: root,
                            upto: AdmissionSeq(0),
                        }],
                        entries: entries_with_vers(dev(1), 2, vec![put(&k2, b"v2", 1)]),
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

        block_on(space.admit(
            &store,
            Timestamp(1),
            &AdmissionRequest {
                device: dev(1),
                expected_checksum: homebase_core::DeviceChecksum::EMPTY,
                evidence: vec![],
                batches: vec![AdmissionBatch {
                    device_seq: DeviceSeq(1),
                    range_asserts: vec![],
                    entries: entries_with_vers(dev(1), 1, vec![put(&k1, b"v1", 1)]),
                }],
            },
        ))
        .unwrap();

        let resp = block_on(space.admit(
            &store,
            Timestamp(1),
            &AdmissionRequest {
                device: dev(2),
                expected_checksum: homebase_core::DeviceChecksum::EMPTY,
                evidence: vec![],
                batches: vec![
                    AdmissionBatch {
                        device_seq: DeviceSeq(1),
                        range_asserts: vec![RangeAssert {
                            prefix: root.clone(),
                            upto: AdmissionSeq(0),
                        }],
                        entries: entries_with_vers(dev(2), 1, vec![put(&k2, b"v2", 1)]),
                    },
                    AdmissionBatch {
                        device_seq: DeviceSeq(2),
                        range_asserts: vec![RangeAssert {
                            prefix: root.clone(),
                            upto: AdmissionSeq(0),
                        }],
                        entries: vec![],
                    },
                ],
            },
        ))
        .unwrap();

        assert_eq!(resp.results.len(), 2);
        for result in &resp.results {
            let AdmissionResult::Failed {
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
            got.entries[0].as_ref().unwrap().device_entry.mutation,
            Mutation::Set {
                key: k1.clone(),
                value: OpaqueValue(b"v1".to_vec())
            }
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

        admit_as(
            &mut space,
            &store,
            dev(1),
            1,
            vec![],
            vec![put(&row, b"one", 1)],
        );
        admit_as(
            &mut space,
            &store,
            dev(2),
            1,
            vec![],
            vec![put(&row, b"foreign", 2)],
        );
        admit_as(&mut space, &store, dev(1), 2, vec![], vec![del(&row, 3)]);

        let meta = block_on(store.get(&prefix_meta_key(SPACE, root.components())))
            .unwrap()
            .map(|bytes| PrefixMetaRecord::decode(&bytes).unwrap())
            .unwrap();
        assert_eq!(meta.history.first.device, dev(1));
        assert_eq!(meta.history.first.admission_seq, AdmissionSeq(3));
        assert_eq!(meta.history.second.device, dev(2));
        assert_eq!(meta.history.second.admission_seq, AdmissionSeq(2));

        let response = admit_as(
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
            AdmissionResult::Failed {
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
        let root_meta = |store: &MemoryStore| -> Option<PrefixMetaRecord> {
            block_on(store.get(&root_meta_key(SPACE)))
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
        assert_eq!(root_meta(&store), None);

        // Two live keys: every ancestor counts both, at seq 2.
        admit(&mut space, &store, lease, 1, vec![put(&k1, b"v", 1)]).unwrap();
        admit(&mut space, &store, lease, 2, vec![put(&k2, b"v", 1)]).unwrap();
        let expect = prefix_meta(dev(1), 2, 1, 2, 0);
        assert_eq!(meta(&store, &root), Some(expect));
        assert_eq!(root_meta(&store), Some(expect));
        assert_eq!(meta(&store, &table), Some(expect));
        assert_eq!(
            meta(&store, &k1),
            Some(prefix_meta(dev(1), 1, 1, 1, 0)),
            "leaf prefix untouched by the sibling's write"
        );

        // Overwrite: max seq advances, live count doesn't.
        admit(&mut space, &store, lease, 3, vec![put(&k1, b"v2", 2)]).unwrap();
        assert_eq!(meta(&store, &root), Some(prefix_meta(dev(1), 3, 2, 2, 0)));

        // Tombstone: count drops; the record persists at live_count 0.
        admit(&mut space, &store, lease, 4, vec![del(&k1, 3)]).unwrap();
        admit(&mut space, &store, lease, 5, vec![del(&k2, 2)]).unwrap();
        assert_eq!(meta(&store, &root), Some(prefix_meta(dev(1), 5, 3, 0, 0)));
        assert_eq!(meta(&store, &k1), Some(prefix_meta(dev(1), 4, 3, 0, 0)));

        // Intra-batch create+tombstone of a fresh key nets zero.
        let k3 = key(&[b"db", b"t", b"r3"]);
        admit(
            &mut space,
            &store,
            lease,
            6,
            vec![put(&k3, b"blip", 1), del(&k3, 2)],
        )
        .unwrap();
        assert_eq!(meta(&store, &root), Some(prefix_meta(dev(1), 6, 3, 0, 1)));

        // Repeated delete advances high water but keeps live count stable.
        admit(&mut space, &store, lease, 7, vec![del(&k1, 4)]).unwrap();
        assert_eq!(meta(&store, &root), Some(prefix_meta(dev(1), 7, 4, 0, 0)));

        // Delete -> present and a zero-length present both count as live.
        let k4 = key(&[b"db", b"t", b"r4"]);
        admit(&mut space, &store, lease, 8, vec![put(&k1, b"back", 5)]).unwrap();
        admit(&mut space, &store, lease, 9, vec![put(&k4, b"", 1)]).unwrap();
        assert_eq!(meta(&store, &root), Some(prefix_meta(dev(1), 9, 5, 2, 0)));
        assert_eq!(root_meta(&store), meta(&store, &root));
    }

    #[test]
    fn empty_batch_advances_admission_without_touching_aggregates() {
        use crate::schema::{PrefixMetaRecord, prefix_meta_key};
        let meta = |store: &MemoryStore, prefix: &Key| -> Option<PrefixMetaRecord> {
            block_on(store.get(&prefix_meta_key(SPACE, prefix.components())))
                .unwrap()
                .map(|bytes| PrefixMetaRecord::decode(&bytes).unwrap())
        };

        let (mut space, store, lease) = setup();
        let root = key(&[b"db"]);
        let k = key(&[b"db", b"t", b"r1"]);

        let resp = admit(&mut space, &store, lease, 1, vec![]).unwrap();
        assert_eq!(resp.applied_admission_seq(0), Some(AdmissionSeq(1)));
        assert_eq!(meta(&store, &root), None);

        let resp = admit(&mut space, &store, lease, 2, vec![put(&k, b"v", 1)]).unwrap();
        assert_eq!(resp.applied_admission_seq(0), Some(AdmissionSeq(2)));
    }

    #[test]
    fn batch_is_atomic_on_rejection() {
        let (mut space, store, lease) = setup();
        let k1 = key(&[b"db", b"a"]);
        let k2 = key(&[b"db", b"b"]);
        admit(&mut space, &store, lease, 1, vec![put(&k1, b"v", 5)]).unwrap();

        // Second batch: valid write to k2, then a ver regression on k1.
        let err = admit(
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
        admit(&mut space, &store, lease, 2, vec![put(&k2, b"v", 1)])
            .expect("device_seq 2 still available after rejected batch");
    }

    #[test]
    fn device_seq_replays_rejected() {
        let (mut space, store, lease) = setup();
        let k = key(&[b"db", b"k"]);
        admit(&mut space, &store, lease, 3, vec![put(&k, b"v", 1)]).unwrap();

        for replayed in [3, 2] {
            let err =
                admit(&mut space, &store, lease, replayed, vec![put(&k, b"v", 2)]).unwrap_err();
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
    fn checksum_mismatch_rejects_before_applying_or_advancing() {
        let (mut space, store, lease) = setup();
        let first = key(&[b"db", b"first"]);
        let second = key(&[b"db", b"second"]);
        let applied = admit(&mut space, &store, lease, 1, vec![put(&first, b"one", 1)]).unwrap();
        assert_ne!(applied.checksum, homebase_core::DeviceChecksum::EMPTY);

        let err = block_on(space.admit(
            &store,
            Timestamp(1),
            &AdmissionRequest {
                device: dev(1),
                expected_checksum: homebase_core::DeviceChecksum::EMPTY,
                evidence: vec![lease],
                batches: vec![AdmissionBatch {
                    device_seq: DeviceSeq(2),
                    range_asserts: vec![],
                    entries: entries_with_vers(dev(1), 2, vec![put(&second, b"two", 2)]),
                }],
            },
        ))
        .unwrap_err();
        assert!(matches!(
            err,
            Error::Kernel(KernelError::DeviceChecksumMismatch {
                current_seq: DeviceSeq(1),
                current,
            }) if current == applied.checksum
        ));
        assert!(
            block_on(space.get(
                &store,
                &GetRequest {
                    keys: vec![second.clone()]
                }
            ))
            .unwrap()
            .entries[0]
                .is_none()
        );

        admit(&mut space, &store, lease, 2, vec![put(&second, b"two", 2)])
            .expect("mismatch must not consume device_seq 2");
    }

    #[test]
    fn rolled_back_server_device_checksum_is_detected() {
        let (mut space, store, lease) = setup();
        let first = key(&[b"db", b"first"]);
        let second = key(&[b"db", b"second"]);
        let applied = admit(&mut space, &store, lease, 1, vec![put(&first, b"one", 1)]).unwrap();

        let mut rollback = WriteBatch::new();
        rollback.delete(device_key(SPACE, dev(1)));
        block_on(store.apply(rollback)).unwrap();

        let err = block_on(space.admit(
            &store,
            Timestamp(1),
            &AdmissionRequest {
                device: dev(1),
                expected_checksum: applied.checksum,
                evidence: vec![lease],
                batches: vec![AdmissionBatch {
                    device_seq: DeviceSeq(2),
                    range_asserts: vec![],
                    entries: entries_with_vers(dev(1), 2, vec![put(&second, b"two", 2)]),
                }],
            },
        ))
        .unwrap_err();
        assert!(matches!(
            err,
            Error::Kernel(KernelError::DeviceChecksumMismatch {
                current_seq: DeviceSeq(0),
                current: homebase_core::DeviceChecksum::EMPTY,
            })
        ));
    }

    #[test]
    fn intra_batch_ver_checks_are_sequential() {
        let (mut space, store, lease) = setup();
        let k = key(&[b"db", b"k"]);
        // Same key twice in one batch: second must exceed the first…
        admit(
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
            got.entries[0].as_ref().unwrap().device_entry.mutation,
            Mutation::Set {
                key: k.clone(),
                value: OpaqueValue(b"v2".to_vec())
            }
        );
        assert_eq!(got.entries[0].as_ref().unwrap().admission.op_index, 1);

        // …and an equal ver within the batch is a regression.
        let err = admit(
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
    fn admission_log_retains_repeated_keys_and_empty_batches() {
        let (mut space, store, lease) = setup();
        let k = key(&[b"db", b"row"]);
        let first = admit(
            &mut space,
            &store,
            lease,
            1,
            vec![put(&k, b"v1", 1), put(&k, b"v2", 2)],
        )
        .unwrap();
        let second = admit(&mut space, &store, lease, 2, vec![]).unwrap();

        let page = block_on(space.pull(
            &store,
            &PullRequest {
                after: AdmissionSeq(0),
                max_batches: None,
            },
        ))
        .unwrap();
        assert_eq!(page.through, AdmissionSeq(2));
        assert_eq!(page.batches.len(), 2);
        assert_eq!(page.batches[0].entries.len(), 2);
        assert_eq!(page.batches[0].entries[0].admission.op_index, 0);
        assert_eq!(page.batches[0].entries[1].admission.op_index, 1);
        assert_eq!(page.batches[0].checksum, first.checksum);
        assert_eq!(page.batches[1].device_seq, DeviceSeq(2));
        assert_eq!(page.batches[1].checksum, second.checksum);
        assert!(page.batches[1].entries.is_empty());

        let current = block_on(space.get(
            &store,
            &GetRequest {
                keys: vec![k.clone()],
            },
        ))
        .unwrap();
        assert!(matches!(
            &current.entries[0].as_ref().unwrap().device_entry.mutation,
            Mutation::Set { key, value } if key == &k && value.0 == b"v2"
        ));

        let history = block_on(space.read_at(
            &store,
            &ReadAtRequest {
                ranges: vec![RangeCursor {
                    range: Range::Prefix(key(&[b"db"])),
                    since: Some(AdmissionSeq(0)),
                }],
            },
        ))
        .unwrap();
        let RangeCut::Delta(entries) = &history.ranges[0] else {
            panic!("expected delta")
        };
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].admission.op_index, 0);
        assert_eq!(entries[1].admission.op_index, 1);
    }

    #[test]
    fn pull_is_dense_and_batch_bounded() {
        let (mut space, store, lease) = setup();
        for seq in 1..=3 {
            let k = key(&[b"db", &[seq as u8]]);
            admit(&mut space, &store, lease, seq, vec![put(&k, b"v", seq)]).unwrap();
        }

        let first = block_on(space.pull(
            &store,
            &PullRequest {
                after: AdmissionSeq(0),
                max_batches: Some(2),
            },
        ))
        .unwrap();
        assert_eq!(first.through, AdmissionSeq(2));
        assert_eq!(
            first
                .batches
                .iter()
                .map(|batch| batch.admission_seq)
                .collect::<Vec<_>>(),
            vec![AdmissionSeq(1), AdmissionSeq(2)]
        );

        let second = block_on(space.pull(
            &store,
            &PullRequest {
                after: first.through,
                max_batches: Some(2),
            },
        ))
        .unwrap();
        assert_eq!(second.through, AdmissionSeq(3));
        assert_eq!(second.batches[0].admission_seq, AdmissionSeq(3));

        let empty = block_on(space.pull(
            &store,
            &PullRequest {
                after: second.through,
                max_batches: Some(2),
            },
        ))
        .unwrap();
        assert_eq!(empty.through, AdmissionSeq(3));
        assert!(empty.batches.is_empty());

        let ahead = block_on(space.pull(
            &store,
            &PullRequest {
                after: AdmissionSeq(4),
                max_batches: None,
            },
        ));
        assert!(matches!(
            ahead,
            Err(Error::Kernel(KernelError::AdmissionCursorAhead {
                after: AdmissionSeq(4),
                high_water: AdmissionSeq(3),
            }))
        ));
    }

    #[test]
    fn read_at_delta_is_exact_sparse_history_at_the_atomic_cut() {
        let (mut space, store, lease) = setup();
        let a = key(&[b"db", b"a", b"row"]);
        let b = key(&[b"db", b"b", b"row"]);
        admit(&mut space, &store, lease, 1, vec![put(&a, b"a1", 1)]).unwrap();
        admit(&mut space, &store, lease, 2, vec![put(&b, b"b1", 7)]).unwrap();
        admit(&mut space, &store, lease, 3, vec![del(&a, 3)]).unwrap();

        let fetched = block_on(space.read_at(
            &store,
            &ReadAtRequest {
                ranges: vec![RangeCursor {
                    range: Range::Prefix(key(&[b"db", b"a"])),
                    since: Some(AdmissionSeq(0)),
                }],
            },
        ))
        .unwrap();
        assert_eq!(fetched.at, AdmissionSeq(3));
        let RangeCut::Delta(operations) = &fetched.ranges[0] else {
            panic!("expected delta")
        };
        assert_eq!(
            operations
                .iter()
                .map(|entry| entry.admission.admission_seq)
                .collect::<Vec<_>>(),
            vec![AdmissionSeq(1), AdmissionSeq(3)]
        );

        let empty = block_on(space.read_at(
            &store,
            &ReadAtRequest {
                ranges: vec![RangeCursor {
                    range: Range::Prefix(key(&[b"db", b"missing"])),
                    since: Some(AdmissionSeq(0)),
                }],
            },
        ))
        .unwrap();
        assert_eq!(empty.at, AdmissionSeq(3));
        assert!(matches!(&empty.ranges[0], RangeCut::Delta(entries) if entries.is_empty()));
    }

    #[test]
    fn failed_atomic_apply_exposes_neither_log_nor_materialization() {
        let mut space = Space::new(SPACE);
        let store = FailApplyStore::default();
        let k = key(&[b"db", b"row"]);
        let request = AdmissionRequest {
            device: dev(1),
            expected_checksum: homebase_core::DeviceChecksum::EMPTY,
            evidence: vec![],
            batches: vec![AdmissionBatch {
                device_seq: DeviceSeq(1),
                range_asserts: vec![],
                entries: entries_with_vers(dev(1), 1, vec![put(&k, b"v", 1)]),
            }],
        };

        store.fail_next_apply();
        assert!(block_on(space.admit(&store, Timestamp(1), &request)).is_err());
        let page = block_on(space.pull(
            &store,
            &PullRequest {
                after: AdmissionSeq(0),
                max_batches: None,
            },
        ))
        .unwrap();
        assert_eq!(page.through, AdmissionSeq(0));
        assert!(page.batches.is_empty());
        assert!(
            block_on(space.get(&store, &GetRequest { keys: vec![k] }))
                .unwrap()
                .entries[0]
                .is_none()
        );

        let retried = block_on(space.admit(&store, Timestamp(1), &request)).unwrap();
        assert_eq!(retried.applied_admission_seq(0), Some(AdmissionSeq(1)));
    }

    #[test]
    fn list_hides_tombstones_and_paginates() {
        let (mut space, store, lease) = setup();
        let keys: Vec<Key> = [b"a" as &[u8], b"b", b"c", b"d"]
            .iter()
            .map(|s| key(&[b"db", s]))
            .collect();
        admit(
            &mut space,
            &store,
            lease,
            1,
            keys.iter().map(|k| put(k, b"v", 1)).collect(),
        )
        .unwrap();
        admit(&mut space, &store, lease, 2, vec![del(&keys[1], 2)]).unwrap();

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
            all.entries.iter().map(|e| e.key()).collect::<Vec<_>>(),
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
                start_after: Some(page1.entries[1].key().clone()),
                limit: Some(2),
            },
        ))
        .unwrap();
        assert_eq!(
            page2.entries.iter().map(|e| e.key()).collect::<Vec<_>>(),
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
        admit(
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
        admit(&mut space, &store, lease, 2, vec![put(&ka, b"a2", 2)]).unwrap();
        admit(&mut space, &store, lease, 3, vec![del(&kb, 2)]).unwrap();

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
        assert_eq!(changes[0].key(), &ka);
        assert!(matches!(
            &changes[0].device_entry.mutation,
            Mutation::Set { value, .. } if value.0 == b"a2"
        ));
        assert_eq!(changes[1].key(), &kb);
        assert!(changes[1].device_entry.mutation.is_delete());

        // A key changed twice appears once, at its latest admission seq.
        assert_eq!(changes[0].admission.admission_seq, AdmissionSeq(2));

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
    fn seal_survives_set_get_and_delete_delta() {
        let (mut space, store, lease) = setup();
        let row = key(&[b"db", b"sealed"]);
        let set_seal = seal(7);
        let delete_seal = seal(9);

        admit_entries(
            &mut space,
            &store,
            lease,
            1,
            vec![],
            vec![entry_with_seal(
                dev(1),
                1,
                Mutation::Set {
                    key: row.clone(),
                    value: OpaqueValue(b"ciphertext".to_vec()),
                },
                1,
                set_seal.clone(),
            )],
        )
        .unwrap();
        let stored = block_on(space.get(
            &store,
            &GetRequest {
                keys: vec![row.clone()],
            },
        ))
        .unwrap()
        .entries
        .remove(0)
        .unwrap();
        assert_eq!(stored.device_entry.seal, set_seal);

        admit_entries(
            &mut space,
            &store,
            lease,
            2,
            vec![],
            vec![entry_with_seal(
                dev(1),
                2,
                Mutation::Delete { key: row.clone() },
                2,
                delete_seal.clone(),
            )],
        )
        .unwrap();
        let delta = block_on(space.read_at(
            &store,
            &ReadAtRequest {
                ranges: vec![RangeCursor {
                    range: Range::Prefix(key(&[b"db"])),
                    since: Some(AdmissionSeq(1)),
                }],
            },
        ))
        .unwrap();
        let RangeCut::Delta(changes) = &delta.ranges[0] else {
            panic!("expected delta")
        };
        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].key(), &row);
        assert!(changes[0].device_entry.mutation.is_delete());
        assert_eq!(changes[0].device_entry.seal, delete_seal);
    }

    #[test]
    fn read_at_delta_filters_by_prefix() {
        let (mut space, store, lease) = setup();
        let ka = key(&[b"db", b"t1", b"r"]);
        let kb = key(&[b"db", b"t2", b"r"]);
        admit(
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
        assert_eq!(d1.iter().map(|e| e.key()).collect::<Vec<_>>(), vec![&ka]);
        assert_eq!(d2.iter().map(|e| e.key()).collect::<Vec<_>>(), vec![&kb]);
    }

    #[test]
    fn acquire_barrier_tracks_admissions() {
        let (mut space, store, lease) = setup();
        admit(
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
        admit(&mut space, &store, lease, 1, vec![put(&outside, b"v", 1)]).unwrap();
    }
}
