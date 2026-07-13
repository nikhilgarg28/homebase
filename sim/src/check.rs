//! Brute-force invariant oracles over a (possibly crash-recovered) store.
//!
//! [`audit`] rebuilds every kernel structure the slow way — full scans,
//! recomputation from first principles — and panics on any disagreement
//! with what the write path maintained incrementally. Run it against a
//! fault-free view (disable injection first): it verifies *state*, not IO.
//!
//! Checked here, for any store the kernel produced (including one that
//! just lost an unflushed suffix of batches):
//!
//! 1. admission log ⇔ materialization: headers and operation indexes are
//!    dense, replay produces exact point records and exact range tombstones;
//! 2. counters: `admission_high_water` equals the exact log tail (counters
//!    commit atomically with the batch they describe), and the lease id
//!    counter strictly exceeds every surviving lease record's id;
//! 3. lease indexes: by-id and by-prefix hold identical record sets, ids
//!    unique;
//! 4. per-prefix and Full-root aggregates equal recomputation from data;
//! 5. per-device high waters and checksums equal the latest admitted header.

use homebase::schema::{
    AdmissionHeaderRecord, AdmissionTarget, CountersRecord, DataRecord, DeviceRecord, LeaseRecord,
    PrefixMetaRecord, RangeDeleteRecord, admission_header_key, admission_log_scan_all,
    admission_op_parts, admission_op_scan, counters_key, data_scan_all, device_key,
    lease_by_id_scan, lease_by_prefix_scan_all, prefix_meta_key, prefix_meta_scan_all,
    range_delete_parts, range_delete_scan_all, root_meta_key, user_key_from_data,
};
use homebase::storage::{OrderedStore, collect_scan};
use homebase_core::key::Key;
use homebase_core::messages::Range;
use homebase_core::space::SpaceId;
use homebase_core::tag::{
    AdmissionOrder, AdmissionSeq, DeviceChecksum, DeviceId, DeviceSeq, Mutation,
};
use pollster::block_on;
use std::collections::BTreeMap;

/// What the audit saw — handed back so workload-specific oracles (ack
/// prefix checks, expected values) can run against the same view.
pub struct StoreAudit {
    /// Every data record, tombstones included, by user key.
    pub data: BTreeMap<Key, DataRecord>,
    /// Latest exact materialized range tombstones.
    pub range_deletes: BTreeMap<Range, RangeDeleteRecord>,
    /// Durable admission high water (0 when nothing was admitted).
    pub max_admission_seq: u64,
    /// Surviving lease records by id.
    pub leases: BTreeMap<u64, LeaseRecord>,
}

fn scan_all<S: OrderedStore>(store: &S, prefix: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)> {
    block_on(collect_scan(store.scan_prefix(prefix))).expect("audit scans must not fault")
}

/// Full-store invariant audit for one space. Panics with context on any
/// violation. Disable fault injection before calling.
pub fn audit<S: OrderedStore>(space: SpaceId, store: &S) -> StoreAudit {
    // -- materialized data ---------------------------------------------------
    let data: BTreeMap<Key, DataRecord> = scan_all(store, &data_scan_all(space))
        .into_iter()
        .map(|(k, v)| {
            let key = user_key_from_data(&k).expect("undecodable data key");
            let record = DataRecord::decode(key.clone(), &v).expect("undecodable data record");
            (key, record)
        })
        .collect();

    // -- counters --------------------------------------------------------------
    let counters = block_on(store.get(&counters_key(space)))
        .unwrap()
        .map(|bytes| CountersRecord::decode(&bytes).expect("undecodable counters"))
        .unwrap_or(CountersRecord {
            next_lease_id: 0,
            admission_high_water: 0,
        });

    // -- exact admission log ---------------------------------------------------
    let mut replayed = BTreeMap::new();
    let mut history = Vec::new();
    let mut device_heads: BTreeMap<DeviceId, (DeviceSeq, DeviceChecksum)> = BTreeMap::new();
    let mut expected_record_count = 0usize;
    for raw_seq in 1..=counters.admission_high_water {
        let admission_seq = AdmissionSeq(raw_seq);
        let header = block_on(store.get(&admission_header_key(space, admission_seq)))
            .unwrap()
            .and_then(|bytes| AdmissionHeaderRecord::decode(&bytes))
            .unwrap_or_else(|| panic!("missing admission header at {admission_seq:?}"));
        expected_record_count += 1;

        let mut operation_count = 0u32;
        for (storage_key, bytes) in scan_all(store, &admission_op_scan(space, admission_seq)) {
            let (stored_seq, op_index, target) =
                admission_op_parts(&storage_key).expect("undecodable admission operation key");
            assert_eq!(stored_seq, admission_seq, "operation under wrong header");
            assert_eq!(op_index, operation_count, "admission operation gap");
            let entry = match target {
                AdmissionTarget::Point(key) => {
                    let record = DataRecord::decode(key.clone(), &bytes)
                        .expect("undecodable point admission operation record");
                    replayed.insert(key, record.clone());
                    record.entry
                }
                AdmissionTarget::Range(range) => {
                    RangeDeleteRecord::decode(range, &bytes)
                        .expect("undecodable range admission operation record")
                        .entry
                }
            };
            assert_eq!(
                entry.admission.order(),
                AdmissionOrder {
                    admission_seq,
                    op_index,
                },
                "operation tag diverges from log position"
            );
            assert_eq!(entry.device_entry.tag.device, header.device);
            assert_eq!(entry.device_entry.tag.device_seq, header.device_seq);
            history.push(entry);
            operation_count += 1;
            expected_record_count += 1;
        }
        assert_eq!(operation_count, header.operation_count);
        if let Some((previous, _)) = device_heads.get(&header.device) {
            assert!(*previous < header.device_seq, "device history regressed");
        }
        device_heads.insert(header.device, (header.device_seq, header.checksum));
    }
    assert_eq!(
        scan_all(store, &admission_log_scan_all(space)).len(),
        expected_record_count,
        "admission log contains records outside its dense headers"
    );
    assert_eq!(
        replayed, data,
        "exact log replay diverges from materialized data"
    );

    // -- range tombstones + aggregate replay ---------------------------------
    let mut replay_points: BTreeMap<Key, DataRecord> = BTreeMap::new();
    let mut replay_ranges: BTreeMap<Range, RangeDeleteRecord> = BTreeMap::new();
    let mut expected_aggregates: BTreeMap<Vec<u8>, PrefixMetaRecord> = BTreeMap::new();
    for entry in &history {
        match &entry.device_entry.mutation {
            Mutation::Set { key, .. } | Mutation::Delete { key } => {
                replay_points.insert(
                    key.clone(),
                    DataRecord {
                        entry: entry.clone(),
                    },
                );
            }
            Mutation::DeleteRange { range } => {
                if let Some(record) = replay_ranges.get_mut(range) {
                    record.observe(entry.clone());
                } else {
                    replay_ranges.insert(range.clone(), RangeDeleteRecord::new(entry.clone()));
                }
            }
        }

        for (target, key) in aggregate_path(space, &entry.device_entry.mutation) {
            let record = expected_aggregates
                .entry(key)
                .or_insert_with(PrefixMetaRecord::empty);
            record.observe_history(
                entry.device_entry.tag.device,
                entry.admission.admission_seq,
                entry.ver(),
            );
            record.materialize_count_at(entry.admission.order());
            record.live_count = visible_count(&target, &replay_points, &replay_ranges);
        }
    }

    let stored_ranges: BTreeMap<Range, RangeDeleteRecord> =
        scan_all(store, &range_delete_scan_all(space))
            .into_iter()
            .map(|(key, bytes)| {
                let (stored_space, range) =
                    range_delete_parts(&key).expect("undecodable range-delete key");
                assert_eq!(stored_space, space, "range tombstone under wrong space");
                let record = RangeDeleteRecord::decode(range.clone(), &bytes)
                    .expect("undecodable range-delete record");
                (range, record)
            })
            .collect();
    assert_eq!(
        stored_ranges, replay_ranges,
        "range tombstones diverged from exact log replay"
    );

    // -- lease indexes -----------------------------------------------------------
    let by_id: BTreeMap<u64, LeaseRecord> = scan_all(store, &lease_by_id_scan(space))
        .into_iter()
        .map(|(_, v)| {
            let rec = LeaseRecord::decode(&v).expect("undecodable lease record");
            (rec.id.0, rec)
        })
        .collect();
    let by_prefix: BTreeMap<u64, LeaseRecord> = scan_all(store, &lease_by_prefix_scan_all(space))
        .into_iter()
        .map(|(_, v)| {
            let rec = LeaseRecord::decode(&v).expect("undecodable lease record");
            (rec.id.0, rec)
        })
        .collect();
    assert_eq!(by_id, by_prefix, "lease indexes diverged");

    for rec in by_id.values() {
        assert!(
            rec.id.0 < counters.next_lease_id,
            "lease id {:?} not below counter {}",
            rec.id,
            counters.next_lease_id
        );
    }

    // -- per-prefix and Full-root aggregates ---------------------------------
    let mut stored_aggregates: BTreeMap<Vec<u8>, PrefixMetaRecord> =
        scan_all(store, &prefix_meta_scan_all(space))
            .into_iter()
            .map(|(k, v)| {
                let rec = PrefixMetaRecord::decode(&v).expect("undecodable prefix meta");
                (k, rec)
            })
            .collect();
    if let Some(root) = block_on(store.get(&root_meta_key(space))).unwrap() {
        stored_aggregates.insert(
            root_meta_key(space),
            PrefixMetaRecord::decode(&root).expect("undecodable root aggregate"),
        );
    }
    assert_eq!(
        stored_aggregates, expected_aggregates,
        "historical/count aggregates diverged from log recomputation"
    );

    // -- device high waters ------------------------------------------------------
    for (device, (device_seq, checksum)) in device_heads {
        let record = block_on(store.get(&device_key(space, device)))
            .unwrap()
            .map(|bytes| DeviceRecord::decode(&bytes).expect("undecodable device record"))
            .unwrap_or_else(|| panic!("admissions from {device:?} but no device record"));
        assert_eq!(record.last_seq, device_seq, "device high water diverged");
        assert_eq!(record.checksum, checksum, "device checksum diverged");
    }

    StoreAudit {
        data,
        range_deletes: stored_ranges,
        max_admission_seq: counters.admission_high_water,
        leases: by_id,
    }
}

fn aggregate_path(
    space: SpaceId,
    mutation: &Mutation<homebase_core::tag::OpaqueValue>,
) -> Vec<(Range, Vec<u8>)> {
    let target = match mutation {
        Mutation::Set { key, .. } | Mutation::Delete { key } => Range::Prefix(key.clone()),
        Mutation::DeleteRange { range } => range.clone(),
    };
    let mut path = vec![(Range::Full, root_meta_key(space))];
    let Range::Prefix(prefix) = target else {
        return path;
    };
    let components = prefix.components();
    for depth in 1..=components.len() {
        let node = Range::Prefix(
            Key::new(components[..depth].to_vec()).expect("prefix of valid key is valid"),
        );
        path.push((node, prefix_meta_key(space, &components[..depth])));
    }
    path
}

fn visible_count(
    target: &Range,
    points: &BTreeMap<Key, DataRecord>,
    ranges: &BTreeMap<Range, RangeDeleteRecord>,
) -> u64 {
    points
        .iter()
        .filter(|(key, record)| {
            if !target.covers_key(key) || !record.entry.device_entry.mutation.is_set() {
                return false;
            }
            let newest_covering = ranges
                .iter()
                .filter(|(range, _)| range.covers_key(key))
                .map(|(_, record)| record.entry.admission.order())
                .max();
            newest_covering.is_none_or(|reset| record.entry.admission.order() > reset)
        })
        .count() as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use homebase::schema::{range_delete_key, root_meta_key};
    use homebase::space::Space;
    use homebase::storage::{MemoryStore, WriteBatch};
    use homebase_core::clock::Timestamp;
    use homebase_core::messages::{AdmissionBatch, AdmissionRequest};
    use homebase_core::seal::Seal;
    use homebase_core::tag::{CipherEpoch, DeviceEntry, DeviceTag, OpaqueValue, Ver};
    use std::panic::{AssertUnwindSafe, catch_unwind};

    const SPACE: SpaceId = SpaceId([8; 16]);
    const DEVICE: DeviceId = DeviceId([1; 16]);

    fn key(parts: &[&[u8]]) -> Key {
        Key::from_bytes(parts.iter().copied()).unwrap()
    }

    fn entry(device_seq: u64, ver: u64, mutation: Mutation<OpaqueValue>) -> DeviceEntry {
        DeviceEntry {
            mutation,
            tag: DeviceTag {
                device: DEVICE,
                device_seq: DeviceSeq(device_seq),
                ver: Ver(ver),
                cipher_epoch: CipherEpoch(0),
            },
            seal: Seal::empty_aead_v1(),
        }
    }

    fn set(device_seq: u64, ver: u64, key: Key) -> DeviceEntry {
        entry(
            device_seq,
            ver,
            Mutation::Set {
                key,
                value: OpaqueValue(vec![ver as u8]),
            },
        )
    }

    fn delete_range(device_seq: u64, ver: u64, range: Range) -> DeviceEntry {
        entry(device_seq, ver, Mutation::DeleteRange { range })
    }

    fn admit(
        space: &mut Space,
        store: &MemoryStore,
        device_seq: u64,
        checksum: DeviceChecksum,
        entries: Vec<DeviceEntry>,
    ) -> DeviceChecksum {
        pollster::block_on(space.admit(
            store,
            Timestamp(device_seq),
            &AdmissionRequest {
                device: DEVICE,
                expected_checksum: checksum,
                evidence: vec![],
                batches: vec![AdmissionBatch {
                    device_seq: DeviceSeq(device_seq),
                    range_asserts: vec![],
                    entries,
                }],
            },
        ))
        .unwrap()
        .checksum
    }

    fn mixed_range_store() -> (MemoryStore, Range) {
        let store = MemoryStore::new();
        let mut space = Space::new(SPACE);
        let db = Range::Prefix(key(&[b"db"]));
        let child = Range::Prefix(key(&[b"db", b"child"]));
        let a = key(&[b"db", b"a"]);
        let b = key(&[b"db", b"child", b"b"]);
        let mut checksum = DeviceChecksum::EMPTY;

        checksum = admit(
            &mut space,
            &store,
            1,
            checksum,
            vec![set(1, 1, a), set(1, 2, b.clone())],
        );
        checksum = admit(
            &mut space,
            &store,
            2,
            checksum,
            vec![delete_range(2, 3, child.clone())],
        );
        checksum = admit(&mut space, &store, 3, checksum, vec![set(3, 4, b)]);
        checksum = admit(
            &mut space,
            &store,
            4,
            checksum,
            vec![delete_range(4, 5, db)],
        );
        let _ = admit(
            &mut space,
            &store,
            5,
            checksum,
            vec![delete_range(5, 6, Range::Full)],
        );
        (store, child)
    }

    #[test]
    fn audit_recomputes_mixed_range_materialization() {
        let (store, child) = mixed_range_store();
        let report = audit(SPACE, &store);
        assert_eq!(report.max_admission_seq, 5);
        assert_eq!(report.data.len(), 2);
        assert_eq!(report.range_deletes.len(), 3);
        assert!(report.range_deletes.contains_key(&child));
    }

    #[test]
    fn audit_detects_range_tombstone_divergence() {
        let (store, child) = mixed_range_store();
        let storage_key = range_delete_key(SPACE, &child);
        let mut record = pollster::block_on(store.get(&storage_key))
            .unwrap()
            .and_then(|bytes| RangeDeleteRecord::decode(child, &bytes))
            .unwrap();
        record.max_ver = Ver(999);
        let mut batch = WriteBatch::new();
        batch.put(storage_key, record.encode());
        pollster::block_on(store.apply(batch)).unwrap();

        assert!(catch_unwind(AssertUnwindSafe(|| audit(SPACE, &store))).is_err());
    }

    #[test]
    fn audit_detects_lazy_aggregate_divergence() {
        let (store, _) = mixed_range_store();
        let storage_key = root_meta_key(SPACE);
        let mut record = pollster::block_on(store.get(&storage_key))
            .unwrap()
            .and_then(|bytes| PrefixMetaRecord::decode(&bytes))
            .unwrap();
        record.live_count += 1;
        let mut batch = WriteBatch::new();
        batch.put(storage_key, record.encode());
        pollster::block_on(store.apply(batch)).unwrap();

        assert!(catch_unwind(AssertUnwindSafe(|| audit(SPACE, &store))).is_err());
    }
}
