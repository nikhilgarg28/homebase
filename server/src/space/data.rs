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
//! 1. every point key has no covering live foreign lease and every range has
//!    no live foreign lease overlapping in either direction
//!    ([`LeaseManager::validate_writes`]); presented lease ids are diagnostic
//!    evidence only, not admission authority;
//! 2. each client batch's `device_seq` strictly follows the device's stored
//!    high water and the preceding batch in the request (replay and
//!    out-of-order rejection);
//! 3. each batch's range asserts bound the greatest historical admission
//!    under the prefix from devices other than the submitter; earlier
//!    coalesced or previously admitted batches from this device are expected;
//! 4. every mutation has a valid seal; point `ver` exceeds its retained point
//!    and covering range floors, while DeleteRange `ver` exceeds every
//!    relevant descendant or covering event. Mixed batches are evaluated in
//!    exact `AdmissionOrder`.
//!
//! On admission each client batch takes the next admission seq and the request
//! writes, atomically:
//! immutable admission headers and point/range operations, point data, exact
//! range tombstones, the Full root plus per-prefix historical/count
//! aggregates, the device high water, and counters. Lazy range count resets
//! are implemented in the private engine; the public entrypoint remains gated
//! until replay and client handling are complete.
//!
//! # Reads
//!
//! `pull` replays a dense interval of immutable complete batches. `get` and
//! `list` serve current state and hide tombstones. `read_at`
//! evaluates all requested ranges at the current admission high water —
//! trivially untorn, because verbs execute serially — returning either a
//! snapshot (cursor `None`) or the changes since the cursor, tombstones
//! included in exact `AdmissionOrder`. Historical aggregates may short-circuit
//! a delta whose prefix has `max_admission_seq() ≤ cursor`.

use super::lease::LeaseManager;
use crate::error::Error;
use crate::schema::{
    AdmissionHeaderRecord, AdmissionTarget, CountersRecord, DataRecord, DeviceRecord,
    HistoricalHeads, PrefixMetaRecord, RangeDeleteRecord, admission_header_key, admission_op_key,
    admission_op_parts, admission_op_scan, admission_range_op_key, counters_key,
    covering_range_delete_keys, data_key, data_scan_all, device_key, prefix_meta_key,
    range_delete_parts, root_meta_key, user_key_from_data,
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
    Mutation, Ver,
};
use std::collections::BTreeMap;

pub async fn admit<S: OrderedStore>(
    space: SpaceId,
    leases: &LeaseManager,
    store: &S,
    now: Timestamp,
    req: &AdmissionRequest,
) -> Result<AdmissionResponse, Error> {
    admit_impl(space, leases, store, now, req, false).await
}

#[cfg(test)]
pub(crate) async fn admit_internal<S: OrderedStore>(
    space: SpaceId,
    leases: &LeaseManager,
    store: &S,
    now: Timestamp,
    req: &AdmissionRequest,
) -> Result<AdmissionResponse, Error> {
    admit_impl(space, leases, store, now, req, true).await
}

async fn admit_impl<S: OrderedStore>(
    space: SpaceId,
    leases: &LeaseManager,
    store: &S,
    now: Timestamp,
    req: &AdmissionRequest,
    allow_delete_range: bool,
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

    if !allow_delete_range
        && req
            .batches
            .iter()
            .flat_map(|batch| &batch.entries)
            .any(|entry| entry.mutation.is_delete_range())
    {
        return Err(KernelError::DeleteRangeUnsupported.into());
    }

    // 2. Reservation conflicts over data entries; an empty batch is a no-op.
    let mut keys = Vec::new();
    let mut ranges = Vec::new();
    for entry in req.batches.iter().flat_map(|batch| &batch.entries) {
        match &entry.mutation {
            homebase_core::tag::Mutation::Set { key, .. }
            | homebase_core::tag::Mutation::Delete { key } => keys.push(key.clone()),
            homebase_core::tag::Mutation::DeleteRange { range } => ranges.push(range.clone()),
        }
    }
    leases
        .validate_writes(store, now, req.device, &req.evidence, &keys, &ranges)
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
    let mut staged_ranges: BTreeMap<Range, RangeDeleteRecord> = BTreeMap::new();
    let mut staged_events: Vec<AdmittedEntry> = Vec::new();
    let mut staged_aggregates: BTreeMap<Vec<u8>, PrefixMetaRecord> = BTreeMap::new();
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
            let ver = entry.ver();
            let order = AdmissionOrder {
                admission_seq: seq,
                op_index,
            };
            let admitted = AdmittedEntry {
                device_entry: entry.clone(),
                admission: AdmissionTag {
                    admission_seq: seq,
                    op_index,
                },
            };
            match &entry.mutation {
                Mutation::Set { key, .. } | Mutation::Delete { key } => {
                    let stored = if staged.contains_key(key) {
                        None
                    } else {
                        data(space, store, key).await?
                    };
                    let prior_live = point_record_visible_with_events(
                        space,
                        store,
                        staged.get(key).or(stored.as_ref()),
                        &staged_events,
                    )
                    .await?;
                    let mut current_ver = staged
                        .get(key)
                        .map(|record| record.entry.ver())
                        .or_else(|| stored.as_ref().map(|record| record.entry.ver()));
                    current_ver = max_ver(
                        current_ver,
                        covering_range_max_ver(space, store, key).await?,
                    );
                    current_ver = max_ver(
                        current_ver,
                        staged_events
                            .iter()
                            .filter(|event| match &event.device_entry.mutation {
                                Mutation::DeleteRange { range } => range.covers_key(key),
                                Mutation::Set { .. } | Mutation::Delete { .. } => false,
                            })
                            .map(AdmittedEntry::ver)
                            .max(),
                    );
                    if current_ver.is_some_and(|current| ver <= current) {
                        let current = current_ver.unwrap();
                        return Err(KernelError::VerRegression {
                            key: key.clone(),
                            current,
                            attempted: ver,
                        }
                        .into());
                    }
                    apply_point_count(
                        space,
                        store,
                        &mut staged_aggregates,
                        &staged_events,
                        req.device,
                        key,
                        seq,
                        ver,
                        order,
                        (entry.mutation.is_set() as i64) - (prior_live as i64),
                    )
                    .await?;
                    staged.insert(
                        key.clone(),
                        DataRecord {
                            entry: admitted.clone(),
                        },
                    );
                    batch.put(
                        admission_op_key(space, seq, op_index, key),
                        DataRecord {
                            entry: admitted.clone(),
                        }
                        .encode(),
                    );
                }
                Mutation::DeleteRange { range } => {
                    let effective = effective_history(space, store, range).await?;
                    let mut current_ver =
                        (effective.history.max_admission_seq().0 != 0).then_some(effective.max_ver);
                    current_ver = max_ver(
                        current_ver,
                        staged_events
                            .iter()
                            .filter(|event| mutation_relevant(range, &event.device_entry.mutation))
                            .map(AdmittedEntry::ver)
                            .max(),
                    );
                    if let Some(current) = current_ver
                        && ver <= current
                    {
                        return Err(KernelError::RangeVerRegression {
                            range: range.clone(),
                            current,
                            attempted: ver,
                        }
                        .into());
                    }
                    apply_range_count(
                        space,
                        store,
                        &mut staged_aggregates,
                        &staged_events,
                        req.device,
                        range,
                        seq,
                        ver,
                        order,
                    )
                    .await?;
                    if let Some(record) = staged_ranges.get_mut(range) {
                        record.observe(admitted.clone());
                    } else {
                        let mut record = range_delete(space, store, range)
                            .await?
                            .unwrap_or_else(|| RangeDeleteRecord::new(admitted.clone()));
                        if record.entry.admission.order() != admitted.admission.order() {
                            record.observe(admitted.clone());
                        }
                        staged_ranges.insert(range.clone(), record);
                    }
                    batch.put(
                        admission_range_op_key(space, seq, op_index, range),
                        RangeDeleteRecord::new(admitted.clone()).encode(),
                    );
                }
            }
            staged_events.push(admitted);
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
    for (key, record) in &staged {
        batch.put(data_key(space, key), record.encode());
    }
    for (range, record) in &staged_ranges {
        batch.put(
            crate::schema::range_delete_key(space, range),
            record.encode(),
        );
    }
    for (meta_key, record) in staged_aggregates {
        batch.put(meta_key, record.encode());
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
        let actual = effective_history(space, store, &Range::Prefix(assert.prefix.clone()))
            .await?
            .max_excluding(device);
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

fn aggregate_path(space: SpaceId, range: &Range) -> Vec<(Range, Vec<u8>)> {
    let mut path = vec![(Range::Full, root_meta_key(space))];
    let Range::Prefix(prefix) = range else {
        return path;
    };
    let components = prefix.components();
    path.extend((1..=components.len()).map(|depth| {
        let node = Range::Prefix(
            Key::from_bytes(
                components[..depth]
                    .iter()
                    .map(|component| component.as_bytes().to_vec()),
            )
            .expect("a prefix of a valid key is valid"),
        );
        let key = prefix_meta_key(space, &components[..depth]);
        (node, key)
    }));
    path
}

async fn load_aggregate<S: OrderedStore>(
    store: &S,
    staged: &mut BTreeMap<Vec<u8>, PrefixMetaRecord>,
    key: &[u8],
) -> Result<(), StorageError> {
    if !staged.contains_key(key) {
        let record = match store.get(key).await? {
            Some(bytes) => PrefixMetaRecord::decode(&bytes).expect("corrupt prefix meta record"),
            None => PrefixMetaRecord::empty(),
        };
        staged.insert(key.to_vec(), record);
    }
    Ok(())
}

async fn newest_covering_reset<S: OrderedStore>(
    space: SpaceId,
    store: &S,
    target: &Range,
    staged_events: &[AdmittedEntry],
) -> Result<Option<AdmissionOrder>, StorageError> {
    let stored = covering_range_delete(space, store, target)
        .await?
        .map(|record| record.entry.admission.order());
    Ok(staged_events
        .iter()
        .filter_map(|entry| match &entry.device_entry.mutation {
            Mutation::DeleteRange { range } if range.covers_range(target) => {
                Some(entry.admission.order())
            }
            Mutation::Set { .. } | Mutation::Delete { .. } | Mutation::DeleteRange { .. } => None,
        })
        .fold(stored, |newest, order| {
            Some(newest.map_or(order, |current| current.max(order)))
        }))
}

async fn materialize_aggregate<S: OrderedStore>(
    space: SpaceId,
    store: &S,
    staged: &mut BTreeMap<Vec<u8>, PrefixMetaRecord>,
    staged_events: &[AdmittedEntry],
    target: &Range,
    key: &[u8],
) -> Result<(), StorageError> {
    load_aggregate(store, staged, key).await?;
    if let Some(reset) = newest_covering_reset(space, store, target, staged_events).await? {
        let record = staged.get_mut(key).expect("aggregate was loaded");
        if record.count_epoch < reset {
            record.live_count = 0;
            record.count_epoch = reset;
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn apply_point_count<S: OrderedStore>(
    space: SpaceId,
    store: &S,
    staged: &mut BTreeMap<Vec<u8>, PrefixMetaRecord>,
    staged_events: &[AdmittedEntry],
    device: DeviceId,
    key: &Key,
    seq: AdmissionSeq,
    ver: Ver,
    order: AdmissionOrder,
    delta: i64,
) -> Result<(), StorageError> {
    for (target, meta_key) in aggregate_path(space, &Range::Prefix(key.clone())) {
        materialize_aggregate(space, store, staged, staged_events, &target, &meta_key).await?;
        let record = staged.get_mut(&meta_key).expect("aggregate was loaded");
        record.observe_history(device, seq, ver);
        record.live_count = record
            .live_count
            .checked_add_signed(delta)
            .expect("live count underflow: aggregates diverged from visible point state");
        record.materialize_count_at(order);
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn apply_range_count<S: OrderedStore>(
    space: SpaceId,
    store: &S,
    staged: &mut BTreeMap<Vec<u8>, PrefixMetaRecord>,
    staged_events: &[AdmittedEntry],
    device: DeviceId,
    range: &Range,
    seq: AdmissionSeq,
    ver: Ver,
    order: AdmissionOrder,
) -> Result<(), StorageError> {
    let path = aggregate_path(space, range);
    for (target, meta_key) in &path {
        materialize_aggregate(space, store, staged, staged_events, target, meta_key).await?;
    }
    let target_key = &path.last().expect("aggregate path has Full root").1;
    let removed = staged
        .get(target_key)
        .expect("target aggregate was loaded")
        .live_count;
    for (index, (_, meta_key)) in path.iter().enumerate() {
        let record = staged.get_mut(meta_key).expect("aggregate was loaded");
        record.observe_history(device, seq, ver);
        if index + 1 == path.len() {
            record.live_count = 0;
            record.count_epoch = order;
        } else {
            record.live_count = record
                .live_count
                .checked_sub(removed)
                .expect("live count underflow: range count exceeds ancestor count");
            record.materialize_count_at(order);
        }
    }
    Ok(())
}

fn mutation_relevant<T>(query: &Range, mutation: &Mutation<T>) -> bool {
    match mutation {
        Mutation::Set { key, .. } | Mutation::Delete { key } => query.covers_key(key),
        Mutation::DeleteRange { range } => query.overlaps(range),
    }
}

fn max_ver(left: Option<Ver>, right: Option<Ver>) -> Option<Ver> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.max(right)),
        (Some(ver), None) | (None, Some(ver)) => Some(ver),
        (None, None) => None,
    }
}

pub async fn get<S: OrderedStore>(
    space: SpaceId,
    store: &S,
    req: &GetRequest,
) -> Result<GetResponse, Error> {
    let mut entries = Vec::with_capacity(req.keys.len());
    for key in &req.keys {
        let entry = match data(space, store, key).await? {
            Some(record) if point_record_visible(space, store, &record).await? => {
                Some(record.entry)
            }
            Some(_) | None => None,
        };
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
        if !point_record_visible(space, store, &rec).await? {
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
            let (stored_seq, op_index, target) =
                admission_op_parts(&storage_key).expect("corrupt admission operation key");
            assert_eq!(
                stored_seq, admission_seq,
                "admission operation in wrong batch"
            );
            assert_eq!(op_index as usize, entries.len(), "admission operation gap");
            let entry = match target {
                AdmissionTarget::Point(key) => {
                    DataRecord::decode(key, &bytes)
                        .expect("corrupt point admission operation")
                        .entry
                }
                AdmissionTarget::Range(range) => {
                    RangeDeleteRecord::decode(range, &bytes)
                        .expect("corrupt range admission operation")
                        .entry
                }
            };
            assert_eq!(
                entry.device_entry.tag.device, header.device,
                "admission operation device disagrees with its header"
            );
            assert_eq!(
                entry.device_entry.tag.device_seq, header.device_seq,
                "admission operation device seq disagrees with its header"
            );
            assert_eq!(
                entry.admission.order(),
                AdmissionOrder {
                    admission_seq,
                    op_index,
                },
                "admission operation tag disagrees with its storage key"
            );
            entries.push(entry);
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
    if effective_live_count(space, store, range).await? == 0 {
        return Ok(Vec::new());
    }
    let Range::Prefix(prefix) = range else {
        let base = data_scan_all(space);
        let mut entries = Vec::new();
        let mut iter = store.scan_prefix(&base);
        while let Some((storage_key, bytes)) = iter.next().await? {
            let key = user_key_from_data(&storage_key).expect("corrupt data key");
            let rec = DataRecord::decode(key, &bytes).expect("corrupt data record");
            if !point_record_visible(space, store, &rec).await? {
                continue;
            }
            entries.push(rec.entry);
        }
        return Ok(entries);
    };
    let base = data_key(space, prefix);
    let mut entries = Vec::new();
    let mut iter = store.scan_prefix(&base);
    while let Some((storage_key, bytes)) = iter.next().await? {
        let key = user_key_from_data(&storage_key).expect("corrupt data key");
        let rec = DataRecord::decode(key, &bytes).expect("corrupt data record");
        if !point_record_visible(space, store, &rec).await? {
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
    // Historical aggregates below the query plus exact covering tombstones
    // prove whether any point or range source can be relevant after `since`.
    if effective_history(space, store, range)
        .await?
        .history
        .max_admission_seq()
        <= since
    {
        return Ok(Vec::new());
    }
    let mut entries = Vec::new();
    for batch in read_admission_interval(space, store, since, at).await? {
        entries.extend(
            batch
                .entries
                .into_iter()
                .filter(|entry| mutation_relevant(range, &entry.device_entry.mutation)),
        );
    }
    Ok(entries)
}

// -- record accessors ---------------------------------------------------------

/// Newest exact tombstone whose Full/Prefix target covers `target`.
/// The lookup is bounded by user-key depth: one Full read plus one read per
/// component-wise ancestor, with no descendant scan.
pub(crate) async fn covering_range_delete<S: OrderedStore>(
    space: SpaceId,
    store: &S,
    target: &Range,
) -> Result<Option<RangeDeleteRecord>, StorageError> {
    let mut newest: Option<RangeDeleteRecord> = None;
    for storage_key in covering_range_delete_keys(space, target) {
        let Some(bytes) = store.get(&storage_key).await? else {
            continue;
        };
        let (_, range) = range_delete_parts(&storage_key).expect("corrupt range-delete key");
        let record = RangeDeleteRecord::decode(range, &bytes).expect("corrupt range-delete record");
        if newest
            .as_ref()
            .is_none_or(|current| record.entry.admission.order() > current.entry.admission.order())
        {
            newest = Some(record);
        }
    }
    Ok(newest)
}

async fn covering_range_max_ver<S: OrderedStore>(
    space: SpaceId,
    store: &S,
    key: &Key,
) -> Result<Option<Ver>, StorageError> {
    let mut max = None;
    let target = Range::Prefix(key.clone());
    for storage_key in covering_range_delete_keys(space, &target) {
        let Some(bytes) = store.get(&storage_key).await? else {
            continue;
        };
        let (_, range) = range_delete_parts(&storage_key).expect("corrupt range-delete key");
        let record = RangeDeleteRecord::decode(range, &bytes).expect("corrupt range-delete record");
        max = max_ver(max, Some(record.max_ver));
    }
    Ok(max)
}

async fn point_record_visible<S: OrderedStore>(
    space: SpaceId,
    store: &S,
    record: &DataRecord,
) -> Result<bool, StorageError> {
    if !record.entry.device_entry.mutation.is_set() {
        return Ok(false);
    }
    let key = record.entry.key();
    let covering = covering_range_delete(space, store, &Range::Prefix(key.clone())).await?;
    Ok(covering
        .is_none_or(|delete| record.entry.admission.order() > delete.entry.admission.order()))
}

async fn point_record_visible_with_events<S: OrderedStore>(
    space: SpaceId,
    store: &S,
    record: Option<&DataRecord>,
    staged_events: &[AdmittedEntry],
) -> Result<bool, StorageError> {
    let Some(record) = record else {
        return Ok(false);
    };
    if !record.entry.device_entry.mutation.is_set() {
        return Ok(false);
    }
    let target = Range::Prefix(record.entry.key().clone());
    let newest_reset = newest_covering_reset(space, store, &target, staged_events).await?;
    Ok(newest_reset.is_none_or(|reset| record.entry.admission.order() > reset))
}

/// Historical facts relevant to one range: all events below it from the
/// aggregate plus exact ancestor tombstones that cover it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct EffectiveHistory {
    pub history: HistoricalHeads,
    pub max_ver: homebase_core::tag::Ver,
}

impl EffectiveHistory {
    pub fn max_excluding(self, device: DeviceId) -> AdmissionSeq {
        self.history.max_excluding(device)
    }
}

pub(crate) async fn effective_history<S: OrderedStore>(
    space: SpaceId,
    store: &S,
    target: &Range,
) -> Result<EffectiveHistory, StorageError> {
    let aggregate_key = match target {
        Range::Full => root_meta_key(space),
        Range::Prefix(prefix) => prefix_meta_key(space, prefix.components()),
    };
    let aggregate = match store.get(&aggregate_key).await? {
        Some(bytes) => PrefixMetaRecord::decode(&bytes).expect("corrupt aggregate record"),
        None => PrefixMetaRecord::empty(),
    };
    let mut effective = EffectiveHistory {
        history: aggregate.history,
        max_ver: aggregate.max_ver,
    };
    for storage_key in covering_range_delete_keys(space, target) {
        let Some(bytes) = store.get(&storage_key).await? else {
            continue;
        };
        let (_, range) = range_delete_parts(&storage_key).expect("corrupt range-delete key");
        let record = RangeDeleteRecord::decode(range, &bytes).expect("corrupt range-delete record");
        effective.history.merge(record.history);
        effective.max_ver = effective.max_ver.max(record.max_ver);
    }
    Ok(effective)
}

/// Current logical live count under `target`. A descendant aggregate may
/// remain physically stale after an ancestor DeleteRange; its count is zero
/// until a later point mutation materializes it beyond that reset.
pub(crate) async fn effective_live_count<S: OrderedStore>(
    space: SpaceId,
    store: &S,
    target: &Range,
) -> Result<u64, StorageError> {
    let aggregate_key = match target {
        Range::Full => root_meta_key(space),
        Range::Prefix(prefix) => prefix_meta_key(space, prefix.components()),
    };
    let aggregate = match store.get(&aggregate_key).await? {
        Some(bytes) => PrefixMetaRecord::decode(&bytes).expect("corrupt aggregate record"),
        None => PrefixMetaRecord::empty(),
    };
    let reset = covering_range_delete(space, store, target)
        .await?
        .map(|record| record.entry.admission.order());
    Ok(
        if reset.is_some_and(|reset| aggregate.count_epoch < reset) {
            0
        } else {
            aggregate.live_count
        },
    )
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

async fn range_delete<S: OrderedStore>(
    space: SpaceId,
    store: &S,
    range: &Range,
) -> Result<Option<RangeDeleteRecord>, StorageError> {
    Ok(store
        .get(&crate::schema::range_delete_key(space, range))
        .await?
        .map(|bytes| {
            RangeDeleteRecord::decode(range.clone(), &bytes).expect("corrupt range-delete record")
        }))
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
