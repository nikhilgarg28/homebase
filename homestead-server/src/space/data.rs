//! The data plane: `put_batch` admission and the three read verbs.
//!
//! Free functions over `(space, store)` — the data plane keeps no in-memory
//! state of its own; everything lives in the ordered store. [`super::Space`]
//! is the only caller.
//!
//! # Admission (`put_batch`)
//!
//! A batch admits if and only if, in this order:
//!
//! 1. every presented lease is live, owned, and epoch-exact, and every key
//!    is covered by a presented **write** lease
//!    ([`LeaseManager::validate_put`]);
//! 2. `device_seq` strictly exceeds the device's stored high water (replay
//!    and out-of-order rejection);
//! 3. every entry's `ver` strictly exceeds the stored ver for its key
//!    (within a batch, later entries for the same key check against earlier
//!    ones — the batch behaves like a sequence).
//!
//! On admission the batch takes the next admission seq and writes, atomically:
//! data records, changelog moves (delete the key's old changelog entry,
//! insert the new one), the device high water, and the counters.
//!
//! # Reads
//!
//! `get` and `list` serve current state and hide tombstones. `read_at`
//! evaluates all requested ranges at the current admission high water —
//! trivially untorn, because verbs execute serially — returning either a
//! snapshot (cursor `None`) or the changes since the cursor, tombstones
//! included. The one-record-per-key changelog (see [`crate::schema`]) makes
//! a delta a single seq-ordered scan; each changed key appears exactly once,
//! already at its final state. Delta scans currently walk the whole
//! changelog past the cursor and filter by prefix — the augmented range-max
//! tree that skips untouched prefixes is a later optimization.

use super::lease::LeaseManager;
use crate::error::Error;
use crate::schema::{
    CountersRecord, DataRecord, DeviceRecord, changelog_key, changelog_scan_after,
    changelog_scan_all, counters_key, data_key, device_key, user_key_from_changelog,
    user_key_from_data,
};
use crate::storage::{OrderedStore, ScanIter, StorageError, WriteBatch, prefix_successor};
use homestead_core::clock::Timestamp;
use homestead_core::key::Key;
use homestead_core::messages::{
    GetRequest, GetResponse, KernelError, ListRequest, ListResponse, PutBatchRequest,
    PutBatchResponse, RangeCut, ReadAtRequest, ReadAtResponse,
};
use homestead_core::space::SpaceId;
use homestead_core::tag::{AdmissionSeq, DeviceId, Entry, Tag};
use std::collections::BTreeMap;

pub async fn put_batch<S: OrderedStore>(
    space: SpaceId,
    leases: &LeaseManager,
    store: &mut S,
    now: Timestamp,
    req: &PutBatchRequest,
) -> Result<PutBatchResponse, Error> {
    // 1. Lease coverage: per-key covering epochs, parallel to entries.
    let keys: Vec<Key> = req.entries.iter().map(|e| e.key.clone()).collect();
    let epochs = leases
        .validate_put(store, now, req.device, &req.leases, &keys)
        .await?;

    // 2. Device replay fence.
    if let Some(rec) = device(space, store, req.device).await? {
        if req.device_seq <= rec.last_seq {
            return Err(KernelError::DeviceSeqRegression {
                current: rec.last_seq,
                attempted: req.device_seq,
            }
            .into());
        }
    }

    // 3. Ver monotonicity + changelog moves. `staged` folds the batch in
    // order so a later entry for the same key checks against the earlier
    // one; `old_seqs` remembers each stored key's current changelog slot.
    let mut counters = counters(space, store).await?;
    let seq = AdmissionSeq(counters.admission_high_water + 1);
    counters.admission_high_water = seq.0;

    let mut staged: BTreeMap<Key, DataRecord> = BTreeMap::new();
    let mut old_seqs: BTreeMap<Key, AdmissionSeq> = BTreeMap::new();
    for (entry, epoch) in req.entries.iter().zip(&epochs) {
        let current_ver = match staged.get(&entry.key) {
            Some(rec) => Some(rec.tag.ver),
            None => {
                let stored = data(space, store, &entry.key).await?;
                if let Some(rec) = &stored {
                    old_seqs.insert(entry.key.clone(), rec.tag.admission_seq);
                }
                stored.map(|rec| rec.tag.ver)
            }
        };
        if let Some(current) = current_ver {
            if entry.ver <= current {
                return Err(KernelError::VerRegression {
                    key: entry.key.clone(),
                    current,
                    attempted: entry.ver,
                }
                .into());
            }
        }
        staged.insert(
            entry.key.clone(),
            DataRecord {
                tag: Tag {
                    device: req.device,
                    device_seq: req.device_seq,
                    epoch: *epoch,
                    ver: entry.ver,
                    admission_seq: seq,
                },
                value: entry.value.clone(),
            },
        );
    }

    // Admitted: one atomic batch for data, changelog, device, counters.
    let mut batch = WriteBatch::new();
    for (key, record) in &staged {
        let bytes = record.encode();
        batch.put(data_key(space, key), bytes.clone());
        if let Some(old) = old_seqs.get(key) {
            batch.delete(changelog_key(space, *old, key));
        }
        batch.put(changelog_key(space, seq, key), bytes);
    }
    batch.put(
        device_key(space, req.device),
        DeviceRecord { last_seq: req.device_seq }.encode(),
    );
    batch.put(counters_key(space), counters.encode());
    store.apply(batch).await?;

    Ok(PutBatchResponse { admission_seq: seq })
}

pub async fn get<S: OrderedStore>(
    space: SpaceId,
    store: &S,
    req: &GetRequest,
) -> Result<GetResponse, Error> {
    let mut entries = Vec::with_capacity(req.keys.len());
    for key in &req.keys {
        let entry = data(space, store, key).await?.and_then(|rec| {
            rec.value.is_present().then(|| Entry {
                key: key.clone(),
                value: rec.value,
                tag: rec.tag,
            })
        });
        entries.push(entry);
    }
    Ok(GetResponse { entries })
}

pub async fn list<S: OrderedStore>(
    space: SpaceId,
    store: &S,
    req: &ListRequest,
) -> Result<ListResponse, Error> {
    let base = data_key(space, &req.prefix);
    // "Strictly after `start_after`" = the smallest byte string above its
    // encoding; clamp to the prefix range for degenerate cursors.
    let start = match &req.start_after {
        Some(after) => {
            let mut s = data_key(space, after);
            s.push(0x00);
            s.max(base.clone())
        }
        None => base.clone(),
    };

    let mut entries = Vec::new();
    let mut truncated = false;
    let mut iter = store.scan(start, prefix_successor(&base));
    while let Some((storage_key, bytes)) = iter.next().await? {
        let rec = DataRecord::decode(&bytes).expect("corrupt data record");
        if rec.value.is_absent() {
            continue;
        }
        if req.limit.is_some_and(|limit| entries.len() >= limit) {
            truncated = true;
            break;
        }
        let key = user_key_from_data(&storage_key).expect("corrupt data key");
        entries.push(Entry { key, value: rec.value, tag: rec.tag });
    }
    Ok(ListResponse { entries, truncated })
}

pub async fn read_at<S: OrderedStore>(
    space: SpaceId,
    store: &S,
    req: &ReadAtRequest,
) -> Result<ReadAtResponse, Error> {
    let at = AdmissionSeq(counters(space, store).await?.admission_high_water);
    let mut ranges = Vec::with_capacity(req.ranges.len());
    for range in &req.ranges {
        let cut = match range.since {
            None => RangeCut::Snapshot(snapshot(space, store, &range.prefix).await?),
            Some(since) => RangeCut::Delta(delta(space, store, &range.prefix, since).await?),
        };
        ranges.push(cut);
    }
    Ok(ReadAtResponse { at, ranges })
}

/// Live entries under `prefix`, key order, tombstones hidden.
async fn snapshot<S: OrderedStore>(
    space: SpaceId,
    store: &S,
    prefix: &Key,
) -> Result<Vec<Entry>, StorageError> {
    let base = data_key(space, prefix);
    let mut entries = Vec::new();
    let mut iter = store.scan_prefix(&base);
    while let Some((storage_key, bytes)) = iter.next().await? {
        let rec = DataRecord::decode(&bytes).expect("corrupt data record");
        if rec.value.is_absent() {
            continue;
        }
        let key = user_key_from_data(&storage_key).expect("corrupt data key");
        entries.push(Entry { key, value: rec.value, tag: rec.tag });
    }
    Ok(entries)
}

/// Changes under `prefix` since `since` (exclusive), ascending
/// `(admission_seq, key)`, tombstones included. Each changed key appears
/// exactly once, at its current state.
async fn delta<S: OrderedStore>(
    space: SpaceId,
    store: &S,
    prefix: &Key,
    since: AdmissionSeq,
) -> Result<Vec<Entry>, StorageError> {
    let start = changelog_scan_after(space, since);
    let end = prefix_successor(&changelog_scan_all(space));
    let mut entries = Vec::new();
    let mut iter = store.scan(start, end);
    while let Some((storage_key, bytes)) = iter.next().await? {
        let key = user_key_from_changelog(&storage_key).expect("corrupt changelog key");
        if !key.starts_with(prefix) {
            continue;
        }
        let rec = DataRecord::decode(&bytes).expect("corrupt data record");
        entries.push(Entry { key, value: rec.value, tag: rec.tag });
    }
    Ok(entries)
}

// -- record accessors ---------------------------------------------------------

async fn data<S: OrderedStore>(
    space: SpaceId,
    store: &S,
    key: &Key,
) -> Result<Option<DataRecord>, StorageError> {
    Ok(store
        .get(&data_key(space, key))
        .await?
        .map(|bytes| DataRecord::decode(&bytes).expect("corrupt data record")))
}

async fn device<S: OrderedStore>(
    space: SpaceId,
    store: &S,
    device: DeviceId,
) -> Result<Option<DeviceRecord>, StorageError> {
    Ok(store
        .get(&device_key(space, device))
        .await?
        .map(|bytes| DeviceRecord::decode(&bytes).expect("corrupt device record")))
}

/// Duplicated (privately) in the lease submodule: both subdomains read and
/// write the shared [`CountersRecord`], which is safe only because verbs
/// execute one at a time.
async fn counters<S: OrderedStore>(
    space: SpaceId,
    store: &S,
) -> Result<CountersRecord, StorageError> {
    Ok(store
        .get(&counters_key(space))
        .await?
        .map(|bytes| CountersRecord::decode(&bytes).expect("corrupt counters record"))
        .unwrap_or_default())
}
