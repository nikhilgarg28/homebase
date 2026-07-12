//! The data plane: `admit` admission and the three read verbs.
//!
//! Free functions over `(space, store)` — the data plane keeps no in-memory
//! state of its own; everything lives in the ordered store. [`super::Space`]
//! is the only caller.
//!
//! # Admission (`admit`)
//!
//! A batch admits if and only if, in this order:
//!
//! 1. every key has no live foreign lease reservation conflict
//!    ([`LeaseManager::validate_put`]); presented lease ids are diagnostic
//!    evidence only, not admission authority;
//! 2. each client batch's `device_seq` strictly follows the device's stored
//!    high water and the preceding batch in the request (replay and
//!    out-of-order rejection);
//! 3. each batch's range asserts bound the greatest historical admission
//!    under the prefix from devices other than the submitter; earlier
//!    coalesced or previously admitted batches from this device are expected;
//! 4. every Set/Delete has a valid seal and its `ver` strictly exceeds the
//!    stored ver for its key (within a batch, later ops for the same key
//!    check against earlier ones — the batch behaves like a sequence).
//!
//! On admission each client batch takes the next admission seq and the request
//! writes, atomically:
//! immutable admission headers and operations, data records for Set/Delete
//! ops, per-prefix aggregates along every written key's prefix path (two
//! distinct-device historical heads + live-key delta; see
//! [`PrefixMetaRecord`]), the device high water, and the counters.
//!
//! # Reads
//!
//! `pull` replays a dense interval of immutable complete batches. `get` and
//! `list` serve current state and hide tombstones. `read_at`
//! evaluates all requested ranges at the current admission high water —
//! trivially untorn, because verbs execute serially — returning either a
//! snapshot (cursor `None`) or the changes since the cursor, tombstones
//! included in exact `AdmissionOrder`. The per-prefix aggregates short-circuit
//! both read shapes: a delta whose prefix has `max_admission_seq() ≤ cursor`
//! and a snapshot whose prefix has `live_count == 0` return empty without
//! scanning.

use super::lease::LeaseManager;
use crate::error::Error;
use crate::schema::{
    AdmissionHeaderRecord, CountersRecord, DataRecord, DeviceRecord, PrefixMetaRecord,
    admission_header_key, admission_op_key, admission_op_parts, admission_op_scan, counters_key,
    data_key, data_scan_all, device_key, prefix_meta_key, user_key_from_data,
};
use crate::storage::{OrderedStore, ScanIter, StorageError, WriteBatch, prefix_successor};
use homebase_core::clock::Timestamp;
use homebase_core::key::Key;
use homebase_core::messages::{
    AdmissionRequest, AdmissionResponse, AdmissionResult, AdmittedBatch, GetRequest, GetResponse,
    KernelError, ListRequest, ListResponse, PullRequest, PullResponse, Range, RangeAssertFailure,
    RangeCut, ReadAtRequest, ReadAtResponse,
};
use homebase_core::space::SpaceId;
use homebase_core::tag::{
    AdmissionOrder, AdmissionSeq, AdmissionTag, AdmittedEntry, DeviceChecksum, DeviceId, DeviceSeq,
};
use std::collections::BTreeMap;

pub async fn admit<S: OrderedStore>(
    space: SpaceId,
    leases: &LeaseManager,
    store: &S,
    now: Timestamp,
    req: &AdmissionRequest,
) -> Result<AdmissionResponse, Error> {
    // 1. Confirm the exact device history before validating a new suffix.
    // A lost acknowledgement must remain reconcilable even if leases or
    // reservation conflicts changed after the already-admitted write.
    let device_record = device(space, store, req.device).await?;
    let mut last_device_seq = device_record.map_or(DeviceSeq(0), |record| record.last_seq);
    let current_checksum = device_record.map_or(DeviceChecksum::EMPTY, |record| record.checksum);
    if req.expected_checksum != current_checksum {
        return Err(KernelError::DeviceChecksumMismatch {
            current_seq: last_device_seq,
            current: current_checksum,
        }
        .into());
    }
    let mut next_checksum = current_checksum;
    let mut first = true;
    for client_batch in &req.batches {
        if client_batch.device_seq <= last_device_seq {
            return Err(KernelError::DeviceSeqRegression {
                current: last_device_seq,
                attempted: client_batch.device_seq,
            }
            .into());
        }
        if !first && client_batch.device_seq.0 != last_device_seq.0 + 1 {
            return Err(KernelError::DeviceSeqRegression {
                current: last_device_seq,
                attempted: client_batch.device_seq,
            }
            .into());
        }
        first = false;
        last_device_seq = client_batch.device_seq;
    }

    if req
        .batches
        .iter()
        .flat_map(|batch| &batch.entries)
        .any(|entry| entry.mutation.is_delete_range())
    {
        return Err(KernelError::DeleteRangeUnsupported.into());
    }

    // 2. Reservation conflicts over data entries; an empty batch is a no-op.
    let keys: Vec<Key> = req
        .batches
        .iter()
        .flat_map(|batch| batch.entries.iter().map(|entry| entry.key().clone()))
        .collect();
    leases
        .validate_put(store, now, req.device, &req.evidence, &keys)
        .await?;

    // 3. Ver monotonicity + exact log construction. `staged` folds the batch
    // in order so a later entry for the same key checks against the earlier
    // one. Log writes accumulate directly in `batch`; any rejection drops it
    // before the single atomic apply.
    let mut counters = counters(space, store).await?;
    let mut next_admission_seq = AdmissionSeq(counters.admission_high_water + 1);
    let mut results = Vec::with_capacity(req.batches.len());
    let mut batch = WriteBatch::new();

    let mut staged: BTreeMap<Key, DataRecord> = BTreeMap::new();
    let mut was_live: BTreeMap<Key, bool> = BTreeMap::new();
    let mut touched_prefix_high: BTreeMap<Vec<u8>, AdmissionSeq> = BTreeMap::new();
    for client_batch in &req.batches {
        let failures = range_assert_failures(space, store, req.device, client_batch).await?;
        if !failures.is_empty() {
            return Ok(failed_response(
                req.batches.len(),
                KernelError::RangeAssertFailed { failures },
                current_checksum,
            ));
        }

        next_checksum = client_batch.checksum(next_checksum, space, req.device);
        let operation_count =
            u32::try_from(client_batch.entries.len()).map_err(|_| KernelError::InvalidSeal {
                reason: "admission batch has too many entries".into(),
            })?;

        let seq = next_admission_seq;
        results.push(AdmissionResult::Applied { admission_seq: seq });
        next_admission_seq = AdmissionSeq(seq.0 + 1);
        let mut touched_prefixes = Vec::new();
        for (op_index, entry) in client_batch.entries.iter().enumerate() {
            let op_index = u32::try_from(op_index).map_err(|_| KernelError::InvalidSeal {
                reason: "admission batch has too many entries".into(),
            })?;
            if entry.tag.device != req.device || entry.tag.device_seq != client_batch.device_seq {
                return Err(KernelError::InvalidSeal {
                    reason: "device entry tag does not match its admission batch".into(),
                }
                .into());
            }
            entry
                .seal
                .validate_payload()
                .map_err(|err| KernelError::InvalidSeal {
                    reason: err.to_string(),
                })?;
            let key = entry.key();
            let ver = entry.ver();
            let current_ver = match staged.get(key) {
                Some(rec) => Some(rec.entry.ver()),
                None => {
                    let stored = data(space, store, key).await?;
                    was_live.insert(
                        key.clone(),
                        stored
                            .as_ref()
                            .is_some_and(|rec| rec.entry.device_entry.mutation.is_set()),
                    );
                    stored.map(|rec| rec.entry.ver())
                }
            };
            if let Some(current) = current_ver {
                if ver <= current {
                    return Err(KernelError::VerRegression {
                        key: key.clone(),
                        current,
                        attempted: ver,
                    }
                    .into());
                }
            }
            let admitted = AdmittedEntry {
                device_entry: entry.clone(),
                admission: AdmissionTag {
                    admission_seq: seq,
                    op_index,
                },
            };
            staged.insert(
                key.clone(),
                DataRecord {
                    entry: admitted.clone(),
                },
            );
            batch.put(
                admission_op_key(space, seq, op_index, key),
                DataRecord { entry: admitted }.encode(),
            );
            touched_prefixes.extend(prefix_meta_keys_for_key(space, key));
        }
        for meta_key in touched_prefixes {
            touched_prefix_high
                .entry(meta_key)
                .and_modify(|at| *at = (*at).max(seq))
                .or_insert(seq);
        }
        batch.put(
            admission_header_key(space, seq),
            AdmissionHeaderRecord {
                device: req.device,
                device_seq: client_batch.device_seq,
                checksum: next_checksum,
                operation_count,
            }
            .encode(),
        );
    }
    counters.admission_high_water = next_admission_seq.0 - 1;

    // Admitted: one atomic batch for exact log, materialized data,
    // aggregates, device state, checksum, and counters.
    let mut live_deltas: BTreeMap<Vec<u8>, i64> = BTreeMap::new();
    for (key, record) in &staged {
        batch.put(data_key(space, key), record.encode());

        // Aggregate updates along the key's prefix path: every ancestor sees
        // the new max seq; live counts move by the key's net transition
        // across the whole batch (absent→present +1, present→absent −1).
        let delta = (record.entry.device_entry.mutation.is_set() as i64) - (was_live[key] as i64);
        let components = key.components();
        for depth in 1..=components.len() {
            *live_deltas
                .entry(prefix_meta_key(space, &components[..depth]))
                .or_insert(0) += delta;
        }
    }
    for (meta_key, delta) in live_deltas {
        let mut updated = match store.get(&meta_key).await? {
            Some(bytes) => PrefixMetaRecord::decode(&bytes).expect("corrupt prefix meta record"),
            None => PrefixMetaRecord::empty(),
        };
        updated.observe(req.device, touched_prefix_high[&meta_key]);
        updated.live_count = updated
            .live_count
            .checked_add_signed(delta)
            .expect("live count underflow: aggregates diverged from data records");
        batch.put(meta_key, updated.encode());
    }
    batch.put(
        device_key(space, req.device),
        DeviceRecord {
            last_seq: last_device_seq,
            checksum: next_checksum,
        }
        .encode(),
    );
    batch.put(counters_key(space), counters.encode());
    store.apply(batch).await?;

    Ok(AdmissionResponse {
        checksum: next_checksum,
        results,
    })
}

async fn range_assert_failures<S: OrderedStore>(
    space: SpaceId,
    store: &S,
    device: DeviceId,
    batch: &homebase_core::messages::AdmissionBatch,
) -> Result<Vec<RangeAssertFailure>, Error> {
    let mut failures = Vec::new();
    for assert in &batch.range_asserts {
        let meta_key = prefix_meta_key(space, assert.prefix.components());
        let actual = match store.get(&meta_key).await? {
            Some(bytes) => PrefixMetaRecord::decode(&bytes)
                .expect("corrupt prefix meta record")
                .max_excluding(device),
            None => AdmissionSeq(0),
        };
        if actual > assert.upto {
            failures.push(RangeAssertFailure {
                prefix: assert.prefix.clone(),
                upto: assert.upto,
                actual,
            });
        }
    }
    Ok(failures)
}

fn failed_response(
    count: usize,
    error: KernelError,
    checksum: DeviceChecksum,
) -> AdmissionResponse {
    AdmissionResponse {
        checksum,
        results: (0..count)
            .map(|_| AdmissionResult::Failed {
                error: error.clone(),
            })
            .collect(),
    }
}

fn prefix_meta_keys_for_key(space: SpaceId, key: &Key) -> Vec<Vec<u8>> {
    let components = key.components();
    (1..=components.len())
        .map(|depth| prefix_meta_key(space, &components[..depth]))
        .collect()
}

pub async fn get<S: OrderedStore>(
    space: SpaceId,
    store: &S,
    req: &GetRequest,
) -> Result<GetResponse, Error> {
    let mut entries = Vec::with_capacity(req.keys.len());
    for key in &req.keys {
        let entry = data(space, store, key).await?.and_then(|rec| {
            rec.entry
                .device_entry
                .mutation
                .is_set()
                .then_some(rec.entry)
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
        let key = user_key_from_data(&storage_key).expect("corrupt data key");
        let rec = DataRecord::decode(key, &bytes).expect("corrupt data record");
        if rec.entry.device_entry.mutation.is_delete() {
            continue;
        }
        if req.limit.is_some_and(|limit| entries.len() >= limit) {
            truncated = true;
            break;
        }
        entries.push(rec.entry);
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
            None => RangeCut::Snapshot(snapshot(space, store, &range.range).await?),
            Some(since) => RangeCut::Delta(delta(space, store, &range.range, since, at).await?),
        };
        ranges.push(cut);
    }
    Ok(ReadAtResponse { at, ranges })
}

/// Dense full-space replay over complete admitted batches.
pub async fn pull<S: OrderedStore>(
    space: SpaceId,
    store: &S,
    req: &PullRequest,
) -> Result<PullResponse, Error> {
    let high_water = AdmissionSeq(counters(space, store).await?.admission_high_water);
    if req.after > high_water {
        return Err(KernelError::AdmissionCursorAhead {
            after: req.after,
            high_water,
        }
        .into());
    }
    let available = high_water.0.saturating_sub(req.after.0);
    let limit = req
        .max_batches
        .map(|limit| u64::try_from(limit).unwrap_or(u64::MAX))
        .unwrap_or(u64::MAX);
    let through = AdmissionSeq(req.after.0 + available.min(limit));
    let batches = read_admission_interval(space, store, req.after, through).await?;
    let response = PullResponse {
        after: req.after,
        through,
        batches,
    };
    response
        .validate_dense()
        .expect("server constructed a malformed dense pull");
    Ok(response)
}

async fn read_admission_interval<S: OrderedStore>(
    space: SpaceId,
    store: &S,
    after: AdmissionSeq,
    through: AdmissionSeq,
) -> Result<Vec<AdmittedBatch>, StorageError> {
    let mut batches = Vec::with_capacity(through.0.saturating_sub(after.0) as usize);
    for raw_seq in after.0.saturating_add(1)..=through.0 {
        let admission_seq = AdmissionSeq(raw_seq);
        let header = store
            .get(&admission_header_key(space, admission_seq))
            .await?
            .and_then(|bytes| AdmissionHeaderRecord::decode(&bytes))
            .expect("missing or corrupt admission header below high water");
        let mut entries = Vec::with_capacity(header.operation_count as usize);
        let op_prefix = admission_op_scan(space, admission_seq);
        let mut iter = store.scan_prefix(&op_prefix);
        while let Some((storage_key, bytes)) = iter.next().await? {
            let (stored_seq, op_index, key) =
                admission_op_parts(&storage_key).expect("corrupt admission operation key");
            assert_eq!(
                stored_seq, admission_seq,
                "admission operation in wrong batch"
            );
            assert_eq!(op_index as usize, entries.len(), "admission operation gap");
            let record = DataRecord::decode(key, &bytes).expect("corrupt admission operation");
            assert_eq!(
                record.entry.device_entry.tag.device, header.device,
                "admission operation device disagrees with its header"
            );
            assert_eq!(
                record.entry.device_entry.tag.device_seq, header.device_seq,
                "admission operation device seq disagrees with its header"
            );
            assert_eq!(
                record.entry.admission.order(),
                AdmissionOrder {
                    admission_seq,
                    op_index,
                },
                "admission operation tag disagrees with its storage key"
            );
            entries.push(record.entry);
        }
        assert_eq!(
            entries.len(),
            header.operation_count as usize,
            "admission header operation count mismatch"
        );
        batches.push(AdmittedBatch {
            admission_seq,
            device: header.device,
            device_seq: header.device_seq,
            checksum: header.checksum,
            entries,
        });
    }
    Ok(batches)
}

/// Live entries under `prefix`, key order, tombstones hidden.
async fn snapshot<S: OrderedStore>(
    space: SpaceId,
    store: &S,
    range: &Range,
) -> Result<Vec<AdmittedEntry>, StorageError> {
    let Range::Prefix(prefix) = range else {
        let base = data_scan_all(space);
        let mut entries = Vec::new();
        let mut iter = store.scan_prefix(&base);
        while let Some((storage_key, bytes)) = iter.next().await? {
            let key = user_key_from_data(&storage_key).expect("corrupt data key");
            let rec = DataRecord::decode(key, &bytes).expect("corrupt data record");
            if rec.entry.device_entry.mutation.is_delete() {
                continue;
            }
            entries.push(rec.entry);
        }
        return Ok(entries);
    };
    // Aggregate short-circuit: never-written prefix or all-tombstones.
    match prefix_meta(space, store, prefix).await? {
        None => return Ok(Vec::new()),
        Some(meta) if meta.live_count == 0 => return Ok(Vec::new()),
        Some(_) => {}
    }
    let base = data_key(space, prefix);
    let mut entries = Vec::new();
    let mut iter = store.scan_prefix(&base);
    while let Some((storage_key, bytes)) = iter.next().await? {
        let key = user_key_from_data(&storage_key).expect("corrupt data key");
        let rec = DataRecord::decode(key, &bytes).expect("corrupt data record");
        if rec.entry.device_entry.mutation.is_delete() {
            continue;
        }
        entries.push(rec.entry);
    }
    Ok(entries)
}

/// Exact admitted operations under `range` in `(since, at]`, preserving full
/// `AdmissionOrder` and tombstones. Repeated writes remain repeated.
async fn delta<S: OrderedStore>(
    space: SpaceId,
    store: &S,
    range: &Range,
    since: AdmissionSeq,
    at: AdmissionSeq,
) -> Result<Vec<AdmittedEntry>, StorageError> {
    if let Range::Prefix(prefix) = range {
        // Aggregate short-circuit: nothing under this prefix since the cursor.
        match prefix_meta(space, store, prefix).await? {
            None => return Ok(Vec::new()),
            Some(meta) if meta.max_admission_seq() <= since => return Ok(Vec::new()),
            Some(_) => {}
        }
    }
    let mut entries = Vec::new();
    for batch in read_admission_interval(space, store, since, at).await? {
        entries.extend(
            batch
                .entries
                .into_iter()
                .filter(|entry| range.covers_key(entry.key())),
        );
    }
    Ok(entries)
}

// -- record accessors ---------------------------------------------------------

async fn prefix_meta<S: OrderedStore>(
    space: SpaceId,
    store: &S,
    prefix: &Key,
) -> Result<Option<PrefixMetaRecord>, StorageError> {
    Ok(store
        .get(&prefix_meta_key(space, prefix.components()))
        .await?
        .map(|bytes| PrefixMetaRecord::decode(&bytes).expect("corrupt prefix meta record")))
}

async fn data<S: OrderedStore>(
    space: SpaceId,
    store: &S,
    key: &Key,
) -> Result<Option<DataRecord>, StorageError> {
    Ok(store
        .get(&data_key(space, key))
        .await?
        .map(|bytes| DataRecord::decode(key.clone(), &bytes).expect("corrupt data record")))
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
