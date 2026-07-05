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
//! 1. changelog ⇔ data: same key sets, identical record bytes, changelog
//!    seq equals the record's admission seq — the compacted-changelog
//!    contract;
//! 2. counters: `admission_high_water` equals the max changelog seq
//!    (counters commit atomically with the batch they describe, so a
//!    surviving prefix is always self-consistent), and lease id/epoch
//!    counters strictly exceed every surviving lease record's;
//! 3. lease indexes: by-id and by-prefix hold identical record sets, ids
//!    and epochs unique;
//! 4. per-prefix aggregates equal recomputation from the data records;
//! 5. per-device high waters bound every surviving data tag.

use homebase_core::key::{Key, decode_components};
use homebase_core::space::SpaceId;
use homebase_core::tag::DeviceId;
use homebase_server::schema::{
    CountersRecord, DataRecord, DeviceRecord, LeaseRecord, PrefixMetaRecord, changelog_scan_all,
    counters_key, data_scan_all, device_key, lease_by_id_scan, lease_by_prefix_scan_all,
    prefix_meta_key, prefix_meta_scan_all, user_key_from_changelog, user_key_from_data,
};
use homebase_server::storage::{OrderedStore, collect_scan};
use pollster::block_on;
use std::collections::{BTreeMap, BTreeSet};

/// What the audit saw — handed back so workload-specific oracles (ack
/// prefix checks, expected values) can run against the same view.
pub struct StoreAudit {
    /// Every data record, tombstones included, by user key.
    pub data: BTreeMap<Key, DataRecord>,
    /// Max surviving admission seq (0 when no data survived).
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
    // -- data + changelog ----------------------------------------------------
    let data: BTreeMap<Key, DataRecord> = scan_all(store, &data_scan_all(space))
        .into_iter()
        .map(|(k, v)| {
            let key = user_key_from_data(&k).expect("undecodable data key");
            let record = DataRecord::decode(&v).expect("undecodable data record");
            (key, record)
        })
        .collect();

    let mut changelog_keys = BTreeSet::new();
    let mut max_seq = 0u64;
    for (storage_key, bytes) in scan_all(store, &changelog_scan_all(space)) {
        let key = user_key_from_changelog(&storage_key).expect("undecodable changelog key");
        let components = decode_components(&storage_key).unwrap();
        let seq = u64::from_be_bytes(components[2].as_bytes().try_into().expect("seq width"));
        let record = DataRecord::decode(&bytes).expect("undecodable changelog record");

        assert!(
            changelog_keys.insert(key.clone()),
            "changelog must hold exactly one entry per key, second at {key:?}"
        );
        let data_record = data
            .get(&key)
            .unwrap_or_else(|| panic!("changelog entry without data record: {key:?}"));
        assert_eq!(&record, data_record, "changelog bytes diverge from data at {key:?}");
        assert_eq!(
            seq, record.tag.admission_seq.0,
            "changelog position diverges from record tag at {key:?}"
        );
        max_seq = max_seq.max(seq);
    }
    assert_eq!(
        changelog_keys.len(),
        data.len(),
        "every data record must have exactly one changelog entry"
    );

    // -- counters --------------------------------------------------------------
    let counters = block_on(store.get(&counters_key(space)))
        .unwrap()
        .map(|bytes| CountersRecord::decode(&bytes).expect("undecodable counters"))
        .unwrap_or(CountersRecord {
            next_lease_id: 0,
            next_epoch: 0,
            admission_high_water: 0,
        });
    assert_eq!(
        counters.admission_high_water, max_seq,
        "high water must equal the max surviving admission seq (atomic commit)"
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

    let mut epochs = BTreeSet::new();
    for rec in by_id.values() {
        assert!(epochs.insert(rec.epoch.0), "duplicate epoch {:?}", rec.epoch);
        assert!(
            rec.id.0 < counters.next_lease_id,
            "lease id {:?} not below counter {}",
            rec.id,
            counters.next_lease_id
        );
        assert!(
            rec.epoch.0 < counters.next_epoch,
            "epoch {:?} not below counter {}",
            rec.epoch,
            counters.next_epoch
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
            slot.0 = slot.0.max(record.tag.admission_seq.0);
            slot.1 += record.value.is_present() as u64;
        }
    }
    let stored: BTreeMap<Vec<u8>, (u64, u64)> = scan_all(store, &prefix_meta_scan_all(space))
        .into_iter()
        .map(|(k, v)| {
            let rec = PrefixMetaRecord::decode(&v).expect("undecodable prefix meta");
            (k, (rec.max_admission_seq, rec.live_count))
        })
        .collect();
    assert_eq!(stored, expected, "aggregates diverged from recomputation");

    // -- device high waters ------------------------------------------------------
    let devices: BTreeSet<DeviceId> = data.values().map(|r| r.tag.device).collect();
    for device in devices {
        let record = block_on(store.get(&device_key(space, device)))
            .unwrap()
            .map(|bytes| DeviceRecord::decode(&bytes).expect("undecodable device record"))
            .unwrap_or_else(|| panic!("data from {device:?} but no device record"));
        let max_tag = data
            .values()
            .filter(|r| r.tag.device == device)
            .map(|r| r.tag.device_seq)
            .max()
            .unwrap();
        assert!(
            record.last_seq >= max_tag,
            "device high water {:?} below surviving tag {max_tag:?}",
            record.last_seq
        );
    }

    StoreAudit {
        data,
        max_admission_seq: max_seq,
        leases: by_id,
    }
}
