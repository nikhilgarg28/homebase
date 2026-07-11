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
//! 1. every key has no live foreign lease reservation conflict
//!    ([`LeaseManager::validate_put`]); presented lease ids are diagnostic
//!    evidence only, not admission authority;
//! 2. each client batch's `device_seq` strictly follows the device's stored
//!    high water and the preceding batch in the request (replay and
//!    out-of-order rejection);
//! 3. each batch's range asserts match the scratch prefix high-water as of
//!    that batch (including earlier coalesced batches, excluding this batch);
//! 4. every Set/Delete has a valid seal and its `ver` strictly exceeds the
//!    stored ver for its key (within a batch, later ops for the same key
//!    check against earlier ones — the batch behaves like a sequence).
//!
//! On admission each client batch takes the next admission seq and the request
//! writes, atomically:
//! data records for Set/Delete ops, changelog moves (delete the key's old changelog entry,
//! insert the new one), per-prefix aggregates along every written key's
//! prefix path (max admission seq + live-key delta; see
//! [`PrefixMetaRecord`]), the device high water, and the counters.
//!
//! # Reads
//!
//! `get` and `list` serve current state and hide tombstones. `read_at`
//! evaluates all requested ranges at the current admission high water —
//! trivially untorn, because verbs execute serially — returning either a
//! snapshot (cursor `None`) or the changes since the cursor, tombstones
//! included. The one-record-per-key changelog (see [`crate::schema`]) makes
//! a delta a single seq-ordered scan; each changed key appears exactly once,
//! already at its final state. The per-prefix aggregates short-circuit both
//! read shapes: a delta whose prefix has `max_admission_seq ≤ cursor` and a
//! snapshot whose prefix has `live_count == 0` return empty without
//! scanning. (A delta that does have news still walks the whole changelog
//! past the cursor and filters by prefix; descending the aggregate tree to
//! skip within the scan is a later refinement.)

use super::lease::LeaseManager;
use crate::error::Error;
use crate::schema::{
    CountersRecord, DataRecord, DeviceRecord, PrefixMetaRecord, changelog_key,
    changelog_scan_after, changelog_scan_all, counters_key, data_key, data_scan_all, device_key,
    prefix_meta_key, user_key_from_changelog, user_key_from_data,
};
use crate::storage::{OrderedStore, ScanIter, StorageError, WriteBatch, prefix_successor};
use homebase_core::clock::Timestamp;
use homebase_core::key::Key;
use homebase_core::messages::{
    BatchOp, GetRequest, GetResponse, KernelError, ListRequest, ListResponse, PutBatchRequest,
    PutBatchResponse, PutBatchResult, Range, RangeAssertFailure, RangeCut, ReadAtRequest,
    ReadAtResponse,
};
use homebase_core::space::SpaceId;
use homebase_core::tag::{AdmissionSeq, DeviceId, Entry, Epoch, Tag, Value, Ver};
use std::collections::BTreeMap;

pub async fn put_batch<S: OrderedStore>(
    space: SpaceId,
    leases: &LeaseManager,
    store: &S,
    now: Timestamp,
    req: &PutBatchRequest,
) -> Result<PutBatchResponse, Error> {
    // 1. Reservation conflicts over mutating ops; NoOp carries no key.
    let keys: Vec<Key> = req
        .batches
        .iter()
        .flat_map(|batch| batch.ops.iter().filter_map(op_key).cloned())
        .collect();
    leases
        .validate_put(store, now, req.device, &req.evidence, &keys)
        .await?;

    // 2. Device replay fence.
    let mut last_device_seq = device(space, store, req.device)
        .await?
        .map_or(homebase_core::tag::DeviceSeq(0), |rec| rec.last_seq);
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

    // 3. Ver monotonicity + changelog moves. `staged` folds the batch in
    // order so a later entry for the same key checks against the earlier
    // one; `old_seqs` remembers each stored key's current changelog slot.
    let mut counters = counters(space, store).await?;
    let mut next_admission_seq = AdmissionSeq(counters.admission_high_water + 1);
    let mut results = Vec::with_capacity(req.batches.len());

    let mut staged: BTreeMap<Key, DataRecord> = BTreeMap::new();
    let mut old_seqs: BTreeMap<Key, AdmissionSeq> = BTreeMap::new();
    let mut was_live: BTreeMap<Key, bool> = BTreeMap::new();
    let mut scratch_prefix_high: BTreeMap<Vec<u8>, AdmissionSeq> = BTreeMap::new();
    for client_batch in &req.batches {
        let failures =
            range_assert_failures(space, store, &scratch_prefix_high, client_batch).await?;
        if !failures.is_empty() {
            return Ok(failed_response(
                req.batches.len(),
                KernelError::RangeAssertFailed { failures },
            ));
        }

        let seq = next_admission_seq;
        results.push(PutBatchResult::Applied { admission_seq: seq });
        next_admission_seq = AdmissionSeq(seq.0 + 1);
        let mut touched_prefixes = Vec::new();
        for op in &client_batch.ops {
            let Some((key, ver, value)) = op_write(op)? else {
                continue;
            };
            let current_ver = match staged.get(key) {
                Some(rec) => Some(rec.tag.ver),
                None => {
                    let stored = data(space, store, key).await?;
                    if let Some(rec) = &stored {
                        old_seqs.insert(key.clone(), rec.tag.admission_seq);
                    }
                    was_live.insert(
                        key.clone(),
                        stored.as_ref().is_some_and(|rec| rec.value.is_present()),
                    );
                    stored.map(|rec| rec.tag.ver)
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
            staged.insert(
                key.clone(),
                DataRecord {
                    tag: Tag {
                        device: req.device,
                        device_seq: client_batch.device_seq,
                        epoch: Epoch(0),
                        ver,
                        admission_seq: seq,
                    },
                    value,
                },
            );
            touched_prefixes.extend(prefix_meta_keys_for_key(space, key));
        }
        for meta_key in touched_prefixes {
            scratch_prefix_high
                .entry(meta_key)
                .and_modify(|at| *at = (*at).max(seq))
                .or_insert(seq);
        }
    }
    counters.admission_high_water = next_admission_seq.0 - 1;

    // Admitted: one atomic batch for data, changelog, aggregates, device,
    // counters.
    let mut batch = WriteBatch::new();
    let mut live_deltas: BTreeMap<Vec<u8>, i64> = BTreeMap::new();
    for (key, record) in &staged {
        let bytes = record.encode();
        batch.put(data_key(space, key), bytes.clone());
        if let Some(old) = old_seqs.get(key) {
            batch.delete(changelog_key(space, *old, key));
        }
        batch.put(changelog_key(space, record.tag.admission_seq, key), bytes);

        // Aggregate updates along the key's prefix path: every ancestor sees
        // the new max seq; live counts move by the key's net transition
        // across the whole batch (absent→present +1, present→absent −1).
        let delta = (record.value.is_present() as i64) - (was_live[key] as i64);
        let components = key.components();
        for depth in 1..=components.len() {
            *live_deltas
                .entry(prefix_meta_key(space, &components[..depth]))
                .or_insert(0) += delta;
        }
    }
    for (meta_key, delta) in live_deltas {
        let current = match store.get(&meta_key).await? {
            Some(bytes) => PrefixMetaRecord::decode(&bytes).expect("corrupt prefix meta record"),
            None => PrefixMetaRecord {
                max_admission_seq: 0,
                live_count: 0,
            },
        };
        let updated = PrefixMetaRecord {
            max_admission_seq: counters.admission_high_water,
            live_count: current
                .live_count
                .checked_add_signed(delta)
                .expect("live count underflow: aggregates diverged from data records"),
        };
        batch.put(meta_key, updated.encode());
    }
    batch.put(
        device_key(space, req.device),
        DeviceRecord {
            last_seq: last_device_seq,
        }
        .encode(),
    );
    batch.put(counters_key(space), counters.encode());
    store.apply(batch).await?;

    Ok(PutBatchResponse { results })
}

async fn range_assert_failures<S: OrderedStore>(
    space: SpaceId,
    store: &S,
    scratch_prefix_high: &BTreeMap<Vec<u8>, AdmissionSeq>,
    batch: &homebase_core::messages::PutBatch,
) -> Result<Vec<RangeAssertFailure>, Error> {
    let mut failures = Vec::new();
    for assert in &batch.range_asserts {
        let meta_key = prefix_meta_key(space, assert.prefix.components());
        let actual = match scratch_prefix_high.get(&meta_key) {
            Some(at) => *at,
            None => {
                let max = match store.get(&meta_key).await? {
                    Some(bytes) => {
                        PrefixMetaRecord::decode(&bytes)
                            .expect("corrupt prefix meta record")
                            .max_admission_seq
                    }
                    None => 0,
                };
                AdmissionSeq(max)
            }
        };
        if actual != assert.at {
            failures.push(RangeAssertFailure {
                prefix: assert.prefix.clone(),
                expected: assert.at,
                actual,
            });
        }
    }
    Ok(failures)
}

fn failed_response(count: usize, error: KernelError) -> PutBatchResponse {
    PutBatchResponse {
        results: (0..count)
            .map(|_| PutBatchResult::Failed {
                error: error.clone(),
            })
            .collect(),
    }
}

fn op_key(op: &BatchOp) -> Option<&Key> {
    match op {
        BatchOp::Set { key, .. } | BatchOp::Delete { key, .. } => Some(key),
        BatchOp::NoOp => None,
    }
}

fn prefix_meta_keys_for_key(space: SpaceId, key: &Key) -> Vec<Vec<u8>> {
    let components = key.components();
    (1..=components.len())
        .map(|depth| prefix_meta_key(space, &components[..depth]))
        .collect()
}

fn op_write(op: &BatchOp) -> Result<Option<(&Key, Ver, Value)>, Error> {
    match op {
        BatchOp::Set {
            key,
            ver,
            seal,
            ciphertext,
        } => {
            seal.validate_payload()
                .map_err(|err| KernelError::InvalidSeal {
                    reason: err.to_string(),
                })?;
            Ok(Some((key, *ver, Value::Present(ciphertext.clone()))))
        }
        BatchOp::Delete { key, ver, seal } => {
            seal.validate_payload()
                .map_err(|err| KernelError::InvalidSeal {
                    reason: err.to_string(),
                })?;
            Ok(Some((key, *ver, Value::Absent)))
        }
        BatchOp::NoOp => Ok(None),
    }
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
        entries.push(Entry {
            key,
            value: rec.value,
            tag: rec.tag,
        });
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
            Some(since) => RangeCut::Delta(delta(space, store, &range.range, since).await?),
        };
        ranges.push(cut);
    }
    Ok(ReadAtResponse { at, ranges })
}

/// Live entries under `prefix`, key order, tombstones hidden.
async fn snapshot<S: OrderedStore>(
    space: SpaceId,
    store: &S,
    range: &Range,
) -> Result<Vec<Entry>, StorageError> {
    let Range::Prefix(prefix) = range else {
        let base = data_scan_all(space);
        let mut entries = Vec::new();
        let mut iter = store.scan_prefix(&base);
        while let Some((storage_key, bytes)) = iter.next().await? {
            let rec = DataRecord::decode(&bytes).expect("corrupt data record");
            if rec.value.is_absent() {
                continue;
            }
            let key = user_key_from_data(&storage_key).expect("corrupt data key");
            entries.push(Entry {
                key,
                value: rec.value,
                tag: rec.tag,
            });
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
        let rec = DataRecord::decode(&bytes).expect("corrupt data record");
        if rec.value.is_absent() {
            continue;
        }
        let key = user_key_from_data(&storage_key).expect("corrupt data key");
        entries.push(Entry {
            key,
            value: rec.value,
            tag: rec.tag,
        });
    }
    Ok(entries)
}

/// Changes under `prefix` since `since` (exclusive), ascending
/// `(admission_seq, key)`, tombstones included. Each changed key appears
/// exactly once, at its current state.
async fn delta<S: OrderedStore>(
    space: SpaceId,
    store: &S,
    range: &Range,
    since: AdmissionSeq,
) -> Result<Vec<Entry>, StorageError> {
    if let Range::Prefix(prefix) = range {
        // Aggregate short-circuit: nothing under this prefix since the cursor.
        match prefix_meta(space, store, prefix).await? {
            None => return Ok(Vec::new()),
            Some(meta) if meta.max_admission_seq <= since.0 => return Ok(Vec::new()),
            Some(_) => {}
        }
    }
    let start = changelog_scan_after(space, since);
    let end = prefix_successor(&changelog_scan_all(space));
    let mut entries = Vec::new();
    let mut iter = store.scan(start, end);
    while let Some((storage_key, bytes)) = iter.next().await? {
        let key = user_key_from_changelog(&storage_key).expect("corrupt changelog key");
        if !range.covers_key(&key) {
            continue;
        }
        let rec = DataRecord::decode(&bytes).expect("corrupt data record");
        entries.push(Entry {
            key,
            value: rec.value,
            tag: rec.tag,
        });
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
