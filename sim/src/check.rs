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
//! 1. admission log ⇔ data: headers are dense through the high water,
//!    operation indexes are dense within each header, and replaying the exact
//!    log produces the materialized data records;
//! 2. counters: `admission_high_water` equals the exact log tail (counters
//!    commit atomically with the batch they describe), and the lease id
//!    counter strictly exceeds every surviving lease record's id;
//! 3. lease indexes: by-id and by-prefix hold identical record sets, ids
//!    unique;
//! 4. per-prefix aggregates equal recomputation from the data records;
//! 5. per-device high waters and checksums equal the latest admitted header.

use homebase_core::key::Key;
use homebase_core::space::SpaceId;
use homebase_core::tag::{AdmissionOrder, AdmissionSeq, DeviceChecksum, DeviceId, DeviceSeq};
use homebase_server::schema::{
    AdmissionHeaderRecord, CountersRecord, DataRecord, DeviceRecord, LeaseRecord, PrefixMetaRecord,
    admission_header_key, admission_log_scan_all, admission_op_parts, admission_op_scan,
    counters_key, data_scan_all, device_key, lease_by_id_scan, lease_by_prefix_scan_all,
    prefix_meta_key, prefix_meta_scan_all, user_key_from_data,
};
use homebase_server::storage::{OrderedStore, collect_scan};
use pollster::block_on;
use std::collections::BTreeMap;

/// What the audit saw — handed back so workload-specific oracles (ack
/// prefix checks, expected values) can run against the same view.
pub struct StoreAudit {
    /// Every data record, tombstones included, by user key.
    pub data: BTreeMap<Key, DataRecord>,
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
            let (stored_seq, op_index, key) =
                admission_op_parts(&storage_key).expect("undecodable admission operation key");
            assert_eq!(stored_seq, admission_seq, "operation under wrong header");
            assert_eq!(op_index, operation_count, "admission operation gap");
            let record = DataRecord::decode(key.clone(), &bytes)
                .expect("undecodable admission operation record");
            assert_eq!(
                record.entry.admission.order(),
                AdmissionOrder {
                    admission_seq,
                    op_index,
                },
                "operation tag diverges from log position"
            );
            assert_eq!(record.entry.device_entry.tag.device, header.device);
            assert_eq!(record.entry.device_entry.tag.device_seq, header.device_seq);
            replayed.insert(key, record);
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

    // -- per-prefix aggregates ------------------------------------------------
    let mut expected: BTreeMap<Vec<u8>, (u64, u64)> = BTreeMap::new();
    for (key, record) in &data {
        let components = key.components();
        for depth in 1..=components.len() {
            let slot = expected
                .entry(prefix_meta_key(space, &components[..depth]))
                .or_insert((0, 0));
            slot.0 = slot.0.max(record.entry.admission.admission_seq.0);
            slot.1 += record.entry.device_entry.mutation.is_set() as u64;
        }
    }
    let stored: BTreeMap<Vec<u8>, (u64, u64)> = scan_all(store, &prefix_meta_scan_all(space))
        .into_iter()
        .map(|(k, v)| {
            let rec = PrefixMetaRecord::decode(&v).expect("undecodable prefix meta");
            (k, (rec.max_admission_seq().0, rec.live_count))
        })
        .collect();
    assert_eq!(stored, expected, "aggregates diverged from recomputation");

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
        max_admission_seq: counters.admission_high_water,
        leases: by_id,
    }
}
