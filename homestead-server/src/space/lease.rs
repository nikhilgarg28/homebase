//! Lease management: the state machine behind `acquire` / `renew` /
//! `release`, plus the coverage check `put_batch` admission will use.
//!
//! All lease state lives in the ordered store (nothing special-cased in
//! memory except the advisory contention-demand set): a grant writes both
//! index records and the counter update in one atomic batch, so epochs and
//! the lease table survive crashes exactly as committed.
//!
//! Expiry is strict and local: a record whose deadline has passed is dead
//! the moment `now` reaches it, regardless of whether it is still on disk.
//! Dead records are purged lazily by whichever operation touches them.
//!
//! Stealable leases (see [`homestead_core::lease`]) are preempted inside
//! `acquire`: the victims' records are deleted in the same atomic batch
//! that writes the new grant, whose fresh epoch fences them.

use crate::error::Error;
use crate::schema::{
    CountersRecord, LeaseRecord, counters_key, lease_by_id_key, lease_by_prefix_key,
    lease_by_prefix_scan,
};
use crate::storage::{OrderedStore, ScanIter, StorageError, WriteBatch};
use homestead_core::clock::Timestamp;
use homestead_core::key::{Key, MAX_COMPONENTS};
use homestead_core::lease::{Lease, LeaseId, LeaseRef};
use homestead_core::messages::{
    AcquireRequest, KernelError, ReleaseRequest, RenewGrant, RenewRequest, RenewResponse,
};
use homestead_core::space::SpaceId;
use homestead_core::tag::{AdmissionSeq, DeviceId, Epoch};
use std::collections::BTreeSet;

/// Lease verbs for one space, over any [`OrderedStore`].
///
/// The demand set (who wants a contended prefix) is deliberately in-memory:
/// it is advisory availability state, not correctness state — losing it on
/// restart merely delays the "please release" hint until the next failed
/// acquire re-registers it.
pub struct LeaseManager {
    space: SpaceId,
    demand: BTreeSet<LeaseId>,
}

impl LeaseManager {
    pub fn new(space: SpaceId) -> Self {
        Self {
            space,
            demand: BTreeSet::new(),
        }
    }

    /// All-or-nothing batch acquire. On conflict, nothing is granted, demand
    /// is registered on every blocking lease, and the first conflicting
    /// prefix is reported with the blockers' worst-case remaining TTL.
    ///
    /// With `steal = true`, a spec whose incompatible live blockers are
    /// *all* stealable preempts them: the victims are purged in the grant's
    /// atomic batch and fenced by the fresh epochs. One non-stealable
    /// blocker contends the whole request, as usual.
    pub async fn acquire<S: OrderedStore>(
        &mut self,
        store: &S,
        now: Timestamp,
        req: &AcquireRequest,
    ) -> Result<(Vec<Lease>, AdmissionSeq), Error> {
        let mut purge = WriteBatch::new();
        let mut stolen: Vec<LeaseRecord> = Vec::new();

        // Conflicts against live leases in the store.
        for spec in &req.specs {
            let (live, expired) = self.overlapping(store, now, &spec.prefix).await?;
            self.purge_records(&mut purge, &expired);

            let blockers: Vec<&LeaseRecord> = live
                .iter()
                .filter(|rec| !spec.mode.compatible_with(rec.mode))
                .collect();
            if blockers.is_empty() {
                continue;
            }
            if req.steal && blockers.iter().all(|rec| rec.stealable) {
                stolen.extend(blockers.into_iter().cloned());
                continue;
            }
            let worst = blockers.iter().map(|rec| rec.deadline.0).max().unwrap();
            self.demand.extend(blockers.iter().map(|rec| rec.id));
            store.apply(purge).await?;
            return Err(KernelError::Contended {
                prefix: spec.prefix.clone(),
                retry_after: Some(std::time::Duration::from_millis(
                    worst.saturating_sub(now.0),
                )),
            }
            .into());
        }

        // Conflicts within the batch itself (a request may not self-overlap
        // incompatibly; the compiler derives disjoint spec sets).
        for (i, a) in req.specs.iter().enumerate() {
            for b in &req.specs[i + 1..] {
                let overlap =
                    a.prefix.starts_with(&b.prefix) || b.prefix.starts_with(&a.prefix);
                if overlap && !a.mode.compatible_with(b.mode) {
                    store.apply(purge).await?;
                    return Err(KernelError::Contended {
                        prefix: b.prefix.clone(),
                        retry_after: None,
                    }
                    .into());
                }
            }
        }

        // Grant: victim purge + records + counters in one atomic batch (with
        // the expiry purge). A stolen lease vanishes at the same instant the
        // new grant appears — there is no in-between state to crash into.
        let mut counters = self.counters(store).await?;
        let barrier = AdmissionSeq(counters.admission_high_water);
        let mut batch = purge;
        self.purge_records(&mut batch, &stolen);
        for rec in &stolen {
            self.demand.remove(&rec.id);
        }
        let mut leases = Vec::with_capacity(req.specs.len());
        for spec in &req.specs {
            let record = LeaseRecord {
                id: LeaseId(counters.next_lease_id),
                prefix: spec.prefix.clone(),
                mode: spec.mode,
                device: req.device,
                epoch: Epoch(counters.next_epoch),
                deadline: now.saturating_add(spec.ttl),
                ttl: spec.ttl,
                stealable: spec.stealable,
            };
            counters.next_lease_id += 1;
            counters.next_epoch += 1;
            self.put_records(&mut batch, &record);
            leases.push(Lease {
                id: record.id,
                prefix: record.prefix.clone(),
                mode: record.mode,
                epoch: record.epoch,
                ttl: record.ttl,
                stealable: record.stealable,
            });
        }
        batch.put(counters_key(self.space), counters.encode());
        store.apply(batch).await?;

        Ok((leases, barrier))
    }

    /// Per-lease renewal: live-and-owned leases get a fresh deadline (same
    /// TTL); everything else lands in `invalid`. Contention demand
    /// piggybacks on the grants.
    pub async fn renew<S: OrderedStore>(
        &mut self,
        store: &S,
        now: Timestamp,
        req: &RenewRequest,
    ) -> Result<RenewResponse, Error> {
        let mut batch = WriteBatch::new();
        let mut granted = Vec::new();
        let mut invalid = Vec::new();

        for &id in &req.leases {
            match self.lease_by_id(store, id).await? {
                Some(rec) if rec.is_live(now) && rec.device == req.device => {
                    let renewed = LeaseRecord {
                        deadline: now.saturating_add(rec.ttl),
                        ..rec
                    };
                    self.put_records(&mut batch, &renewed);
                    granted.push(RenewGrant {
                        id,
                        ttl: renewed.ttl,
                        contended: self.demand.contains(&id),
                    });
                }
                Some(rec) => {
                    if !rec.is_live(now) {
                        self.purge_records(&mut batch, std::slice::from_ref(&rec));
                    }
                    invalid.push(id);
                }
                None => invalid.push(id),
            }
        }

        store.apply(batch).await?;
        Ok(RenewResponse { granted, invalid })
    }

    /// Idempotent release. Only the holder releases a live lease; expired
    /// leases are purged regardless of who asks.
    pub async fn release<S: OrderedStore>(
        &mut self,
        store: &S,
        now: Timestamp,
        req: &ReleaseRequest,
    ) -> Result<(), Error> {
        let mut batch = WriteBatch::new();
        for &id in &req.leases {
            if let Some(rec) = self.lease_by_id(store, id).await? {
                if !rec.is_live(now) || rec.device == req.device {
                    self.purge_records(&mut batch, std::slice::from_ref(&rec));
                    self.demand.remove(&id);
                }
            }
        }
        store.apply(batch).await?;
        Ok(())
    }

    /// The coverage check `put_batch` admission runs: every presented ref
    /// must be live, owned, and epoch-exact; every key must be covered by a
    /// presented **write** lease. Returns the covering lease's epoch per
    /// key, for tag construction.
    pub async fn validate_put<S: OrderedStore>(
        &self,
        store: &S,
        now: Timestamp,
        device: DeviceId,
        refs: &[LeaseRef],
        keys: &[Key],
    ) -> Result<Vec<Epoch>, Error> {
        let mut resolved = Vec::with_capacity(refs.len());
        for r in refs {
            let rec = self
                .lease_by_id(store, r.id)
                .await?
                .filter(|rec| rec.is_live(now) && rec.device == device)
                .ok_or(KernelError::LeaseInvalid { lease: r.id })?;
            if rec.epoch != r.epoch {
                return Err(KernelError::Fenced { lease: r.id }.into());
            }
            resolved.push(rec);
        }

        let mut epochs = Vec::with_capacity(keys.len());
        for key in keys {
            let covering = resolved.iter().find(|rec| {
                rec.mode == homestead_core::lease::LeaseMode::Write
                    && key.starts_with(&rec.prefix)
            });
            match covering {
                Some(rec) => epochs.push(rec.epoch),
                None => return Err(KernelError::NotCovered { key: key.clone() }.into()),
            }
        }
        Ok(epochs)
    }

    /// All lease records whose prefix overlaps `prefix` (ancestor, exact, or
    /// descendant), split into (live, dead). Index-served: at most
    /// [`MAX_COMPONENTS`] component-aligned scans, disjoint by depth.
    async fn overlapping<S: OrderedStore>(
        &self,
        store: &S,
        now: Timestamp,
        prefix: &Key,
    ) -> Result<(Vec<LeaseRecord>, Vec<LeaseRecord>), StorageError> {
        let components = prefix.components();
        let mut live = Vec::new();
        let mut dead = Vec::new();

        for depth in 1..=MAX_COMPONENTS {
            // Ancestors of `prefix` need exact-depth matches on a shortened
            // head; the prefix itself and its descendants share the full head.
            let head = &components[..depth.min(components.len())];
            let scan = lease_by_prefix_scan(self.space, depth, head);
            let mut iter = store.scan_prefix(&scan);
            while let Some((_, value)) = iter.next().await? {
                let rec = LeaseRecord::decode(&value).expect("corrupt lease record");
                if rec.is_live(now) {
                    live.push(rec);
                } else {
                    dead.push(rec);
                }
            }
        }
        Ok((live, dead))
    }

    async fn lease_by_id<S: OrderedStore>(
        &self,
        store: &S,
        id: LeaseId,
    ) -> Result<Option<LeaseRecord>, StorageError> {
        Ok(store
            .get(&lease_by_id_key(self.space, id))
            .await?
            .map(|value| LeaseRecord::decode(&value).expect("corrupt lease record")))
    }

    async fn counters<S: OrderedStore>(
        &self,
        store: &S,
    ) -> Result<CountersRecord, StorageError> {
        Ok(store
            .get(&counters_key(self.space))
            .await?
            .map(|value| CountersRecord::decode(&value).expect("corrupt counters record"))
            .unwrap_or_default())
    }

    fn put_records(&self, batch: &mut WriteBatch, rec: &LeaseRecord) {
        let value = rec.encode();
        batch.put(lease_by_id_key(self.space, rec.id), value.clone());
        batch.put(lease_by_prefix_key(self.space, &rec.prefix, rec.id), value);
    }

    fn purge_records(&self, batch: &mut WriteBatch, records: &[LeaseRecord]) {
        for rec in records {
            batch.delete(lease_by_id_key(self.space, rec.id));
            batch.delete(lease_by_prefix_key(self.space, &rec.prefix, rec.id));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::lease_by_id_scan;
    use crate::storage::{MemoryStore, collect_scan};
    use homestead_core::lease::LeaseMode;
    use homestead_core::messages::LeaseSpec;
    use pollster::block_on;
    use std::time::Duration;

    const SPACE: SpaceId = SpaceId([9; 16]);

    fn dev(n: u8) -> DeviceId {
        DeviceId([n; 16])
    }

    fn key(components: &[&[u8]]) -> Key {
        Key::from_bytes(components.iter().copied()).unwrap()
    }

    fn spec(prefix: &Key, mode: LeaseMode, ttl_ms: u64) -> LeaseSpec {
        LeaseSpec {
            prefix: prefix.clone(),
            mode,
            ttl: Duration::from_millis(ttl_ms),
            stealable: false,
        }
    }

    fn acquire_one(
        mgr: &mut LeaseManager,
        store: &MemoryStore,
        now: u64,
        device: u8,
        prefix: &Key,
        mode: LeaseMode,
        ttl_ms: u64,
    ) -> Result<Lease, Error> {
        let req = AcquireRequest {
            device: dev(device),
            specs: vec![spec(prefix, mode, ttl_ms)],
            steal: false,
        };
        block_on(mgr.acquire(store, Timestamp(now), &req))
            .map(|(mut leases, _)| leases.remove(0))
    }

    /// Single-spec acquire with explicit stealable/steal flags.
    #[allow(clippy::too_many_arguments)]
    fn acquire_flags(
        mgr: &mut LeaseManager,
        store: &MemoryStore,
        now: u64,
        device: u8,
        prefix: &Key,
        mode: LeaseMode,
        stealable: bool,
        steal: bool,
    ) -> Result<Lease, Error> {
        let req = AcquireRequest {
            device: dev(device),
            specs: vec![LeaseSpec {
                prefix: prefix.clone(),
                mode,
                ttl: Duration::from_millis(100),
                stealable,
            }],
            steal,
        };
        block_on(mgr.acquire(store, Timestamp(now), &req))
            .map(|(mut leases, _)| leases.remove(0))
    }

    fn live_record_count(store: &MemoryStore, now: u64) -> usize {
        block_on(collect_scan(store.scan_prefix(&lease_by_id_scan(SPACE))))
            .unwrap()
            .into_iter()
            .map(|(_, v)| LeaseRecord::decode(&v).unwrap())
            .filter(|rec| rec.is_live(Timestamp(now)))
            .count()
    }

    #[test]
    fn write_excludes_read_shares() {
        let (mut mgr, store) = (LeaseManager::new(SPACE), MemoryStore::new());
        let parent = key(&[b"db"]);
        let child = key(&[b"db", b"t1"]);
        let sibling = key(&[b"db2"]);

        acquire_one(&mut mgr, &store, 0, 1, &parent, LeaseMode::Read, 100).unwrap();
        // Read shares with read, exact and nested.
        acquire_one(&mut mgr, &store, 0, 2, &parent, LeaseMode::Read, 100).unwrap();
        acquire_one(&mut mgr, &store, 0, 3, &child, LeaseMode::Read, 100).unwrap();
        // Read blocks write, at ancestor and descendant.
        assert!(matches!(
            acquire_one(&mut mgr, &store, 0, 4, &child, LeaseMode::Write, 100),
            Err(Error::Kernel(KernelError::Contended { .. }))
        ));
        // Unrelated prefix is free.
        acquire_one(&mut mgr, &store, 0, 4, &sibling, LeaseMode::Write, 100).unwrap();
        // Write blocks read and write below it.
        assert!(matches!(
            acquire_one(&mut mgr, &store, 0, 5, &sibling, LeaseMode::Read, 100),
            Err(Error::Kernel(KernelError::Contended { .. }))
        ));
    }

    #[test]
    fn no_upgrade_even_for_own_device() {
        let (mut mgr, store) = (LeaseManager::new(SPACE), MemoryStore::new());
        let p = key(&[b"db"]);
        acquire_one(&mut mgr, &store, 0, 1, &p, LeaseMode::Read, 100).unwrap();
        assert!(matches!(
            acquire_one(&mut mgr, &store, 0, 1, &p, LeaseMode::Write, 100),
            Err(Error::Kernel(KernelError::Contended { .. }))
        ));
    }

    #[test]
    fn steal_denied_pre_deadline_allowed_after_with_fresh_epoch() {
        let (mut mgr, store) = (LeaseManager::new(SPACE), MemoryStore::new());
        let p = key(&[b"db"]);
        let first = acquire_one(&mut mgr, &store, 0, 1, &p, LeaseMode::Write, 50).unwrap();

        let denied = acquire_one(&mut mgr, &store, 49, 2, &p, LeaseMode::Write, 50);
        match denied {
            Err(Error::Kernel(KernelError::Contended { retry_after, .. })) => {
                assert_eq!(retry_after, Some(Duration::from_millis(1)));
            }
            other => panic!("expected contended, got {other:?}"),
        }

        // Strict local expiry: dead exactly at the deadline.
        let stolen = acquire_one(&mut mgr, &store, 50, 2, &p, LeaseMode::Write, 50).unwrap();
        assert!(stolen.epoch > first.epoch);
        assert!(stolen.id > first.id);
        // The expired record was purged when touched.
        assert_eq!(live_record_count(&store, 50), 1);
    }

    #[test]
    fn steal_preempts_stealable_and_fences_the_victim() {
        let (mut mgr, store) = (LeaseManager::new(SPACE), MemoryStore::new());
        let p = key(&[b"account"]);
        let first =
            acquire_flags(&mut mgr, &store, 0, 1, &p, LeaseMode::Write, true, false).unwrap();

        // Pre-deadline steal succeeds with a fresh id and epoch.
        let second =
            acquire_flags(&mut mgr, &store, 10, 2, &p, LeaseMode::Write, true, true).unwrap();
        assert!(second.epoch > first.epoch);
        assert!(second.id > first.id);
        assert_eq!(live_record_count(&store, 10), 1, "victim purged");

        // The victim is fenced: renew reports invalid, puts fail.
        let resp = block_on(mgr.renew(
            &store,
            Timestamp(10),
            &RenewRequest { device: dev(1), leases: vec![first.id] },
        ))
        .unwrap();
        assert_eq!(resp.invalid, vec![first.id]);
        let wref = LeaseRef { id: first.id, epoch: first.epoch };
        assert!(matches!(
            block_on(mgr.validate_put(&store, Timestamp(10), dev(1), &[wref], std::slice::from_ref(&p))),
            Err(Error::Kernel(KernelError::LeaseInvalid { .. }))
        ));
    }

    #[test]
    fn steal_denied_by_any_non_stealable_blocker() {
        let (mut mgr, store) = (LeaseManager::new(SPACE), MemoryStore::new());
        let p = key(&[b"account"]);

        // Non-stealable holder: steal is just a normal contended acquire.
        acquire_flags(&mut mgr, &store, 0, 1, &p, LeaseMode::Write, false, false).unwrap();
        assert!(matches!(
            acquire_flags(&mut mgr, &store, 10, 2, &p, LeaseMode::Write, true, true),
            Err(Error::Kernel(KernelError::Contended { .. }))
        ));

        // Mixed blockers: one stealable read + one non-stealable read block
        // a stealing write.
        let q = key(&[b"docs"]);
        acquire_flags(&mut mgr, &store, 0, 3, &q, LeaseMode::Read, true, false).unwrap();
        acquire_flags(&mut mgr, &store, 0, 4, &q, LeaseMode::Read, false, false).unwrap();
        assert!(matches!(
            acquire_flags(&mut mgr, &store, 10, 2, &q, LeaseMode::Write, true, true),
            Err(Error::Kernel(KernelError::Contended { .. }))
        ));
        assert_eq!(live_record_count(&store, 10), 3, "nothing stolen on denial");
    }

    #[test]
    fn stealable_without_steal_flag_still_contends() {
        let (mut mgr, store) = (LeaseManager::new(SPACE), MemoryStore::new());
        let p = key(&[b"account"]);
        acquire_flags(&mut mgr, &store, 0, 1, &p, LeaseMode::Write, true, false).unwrap();
        assert!(matches!(
            acquire_flags(&mut mgr, &store, 10, 2, &p, LeaseMode::Write, false, false),
            Err(Error::Kernel(KernelError::Contended { .. }))
        ));
    }

    #[test]
    fn steal_flag_without_blockers_is_a_plain_grant() {
        let (mut mgr, store) = (LeaseManager::new(SPACE), MemoryStore::new());
        let p = key(&[b"free"]);
        acquire_flags(&mut mgr, &store, 0, 1, &p, LeaseMode::Write, false, true).unwrap();
        assert_eq!(live_record_count(&store, 0), 1);
    }

    #[test]
    fn steal_preempts_multiple_stealable_readers() {
        let (mut mgr, store) = (LeaseManager::new(SPACE), MemoryStore::new());
        let parent = key(&[b"docs"]);
        let child = key(&[b"docs", b"a"]);
        acquire_flags(&mut mgr, &store, 0, 1, &parent, LeaseMode::Read, true, false).unwrap();
        acquire_flags(&mut mgr, &store, 0, 2, &child, LeaseMode::Read, true, false).unwrap();

        // A stealing write on the parent takes out both readers at once.
        acquire_flags(&mut mgr, &store, 10, 3, &parent, LeaseMode::Write, false, true)
            .unwrap();
        assert_eq!(live_record_count(&store, 10), 1);
    }

    #[test]
    fn intra_batch_self_conflict_denied() {
        let (mut mgr, store) = (LeaseManager::new(SPACE), MemoryStore::new());
        let req = AcquireRequest {
            device: dev(1),
            specs: vec![
                spec(&key(&[b"db"]), LeaseMode::Write, 100),
                spec(&key(&[b"db", b"t1"]), LeaseMode::Write, 100),
            ],
            steal: false,
        };
        assert!(matches!(
            block_on(mgr.acquire(&store, Timestamp(0), &req)),
            Err(Error::Kernel(KernelError::Contended { .. }))
        ));
        assert_eq!(live_record_count(&store, 0), 0, "all-or-nothing");
    }

    #[test]
    fn renew_extends_and_piggybacks_contention() {
        let (mut mgr, store) = (LeaseManager::new(SPACE), MemoryStore::new());
        let p = key(&[b"db"]);
        let lease = acquire_one(&mut mgr, &store, 0, 1, &p, LeaseMode::Write, 50).unwrap();

        // No demand yet.
        let resp = block_on(mgr.renew(
            &store,
            Timestamp(40),
            &RenewRequest { device: dev(1), leases: vec![lease.id] },
        ))
        .unwrap();
        assert_eq!(resp.granted.len(), 1);
        assert!(!resp.granted[0].contended);

        // A failed acquire registers demand; the holder hears about it.
        let _ = acquire_one(&mut mgr, &store, 60, 2, &p, LeaseMode::Write, 50);
        let resp = block_on(mgr.renew(
            &store,
            Timestamp(80), // would be past the original deadline (50) without renewal
            &RenewRequest { device: dev(1), leases: vec![lease.id] },
        ))
        .unwrap();
        assert_eq!(resp.granted.len(), 1, "renewal at t=40 extended to t=90");
        assert!(resp.granted[0].contended);
    }

    #[test]
    fn renew_rejects_expired_foreign_and_unknown() {
        let (mut mgr, store) = (LeaseManager::new(SPACE), MemoryStore::new());
        let p = key(&[b"db"]);
        let lease = acquire_one(&mut mgr, &store, 0, 1, &p, LeaseMode::Write, 50).unwrap();

        // Foreign device.
        let resp = block_on(mgr.renew(
            &store,
            Timestamp(10),
            &RenewRequest { device: dev(2), leases: vec![lease.id] },
        ))
        .unwrap();
        assert_eq!(resp.invalid, vec![lease.id]);

        // Expired (and gets purged), plus an unknown id.
        let resp = block_on(mgr.renew(
            &store,
            Timestamp(50),
            &RenewRequest { device: dev(1), leases: vec![lease.id, LeaseId(999)] },
        ))
        .unwrap();
        assert_eq!(resp.invalid, vec![lease.id, LeaseId(999)]);
        assert_eq!(live_record_count(&store, 50), 0);
    }

    #[test]
    fn release_is_idempotent_and_owner_only() {
        let (mut mgr, store) = (LeaseManager::new(SPACE), MemoryStore::new());
        let p = key(&[b"db"]);
        let lease = acquire_one(&mut mgr, &store, 0, 1, &p, LeaseMode::Write, 100).unwrap();

        // Foreign release of a live lease is a no-op.
        block_on(mgr.release(
            &store,
            Timestamp(10),
            &ReleaseRequest { device: dev(2), leases: vec![lease.id] },
        ))
        .unwrap();
        assert_eq!(live_record_count(&store, 10), 1);

        // Owner release frees the prefix; releasing again is fine.
        let req = ReleaseRequest { device: dev(1), leases: vec![lease.id] };
        block_on(mgr.release(&store, Timestamp(10), &req)).unwrap();
        block_on(mgr.release(&store, Timestamp(10), &req)).unwrap();
        acquire_one(&mut mgr, &store, 10, 2, &p, LeaseMode::Write, 100).unwrap();
    }

    #[test]
    fn validate_put_enforces_all_invariants() {
        let (mut mgr, store) = (LeaseManager::new(SPACE), MemoryStore::new());
        let table = key(&[b"db", b"t1"]);
        let row = key(&[b"db", b"t1", b"r1"]);
        let other_row = key(&[b"db", b"t2", b"r1"]);

        let write = acquire_one(&mut mgr, &store, 0, 1, &table, LeaseMode::Write, 50).unwrap();
        let read = acquire_one(&mut mgr, &store, 0, 1, &other_row, LeaseMode::Read, 50).unwrap();
        let wref = LeaseRef { id: write.id, epoch: write.epoch };
        let rref = LeaseRef { id: read.id, epoch: read.epoch };

        // Happy path returns the covering epoch.
        let epochs = block_on(mgr.validate_put(
            &store, Timestamp(10), dev(1), &[wref], std::slice::from_ref(&row),
        ))
        .unwrap();
        assert_eq!(epochs, vec![write.epoch]);

        // Read leases never authorize writes.
        assert!(matches!(
            block_on(mgr.validate_put(&store, Timestamp(10), dev(1), &[rref], std::slice::from_ref(&other_row))),
            Err(Error::Kernel(KernelError::NotCovered { .. }))
        ));

        // Uncovered key.
        assert!(matches!(
            block_on(mgr.validate_put(&store, Timestamp(10), dev(1), &[wref], std::slice::from_ref(&other_row))),
            Err(Error::Kernel(KernelError::NotCovered { .. }))
        ));

        // Stale epoch fences.
        let stale = LeaseRef { id: write.id, epoch: Epoch(write.epoch.0 + 1) };
        assert!(matches!(
            block_on(mgr.validate_put(&store, Timestamp(10), dev(1), &[stale], std::slice::from_ref(&row))),
            Err(Error::Kernel(KernelError::Fenced { .. }))
        ));

        // Foreign device.
        assert!(matches!(
            block_on(mgr.validate_put(&store, Timestamp(10), dev(2), &[wref], std::slice::from_ref(&row))),
            Err(Error::Kernel(KernelError::LeaseInvalid { .. }))
        ));

        // Expired.
        assert!(matches!(
            block_on(mgr.validate_put(&store, Timestamp(50), dev(1), &[wref], std::slice::from_ref(&row))),
            Err(Error::Kernel(KernelError::LeaseInvalid { .. }))
        ));
    }

    #[test]
    fn acquire_reports_admission_barrier() {
        let (mut mgr, store) = (LeaseManager::new(SPACE), MemoryStore::new());
        let req = AcquireRequest {
            device: dev(1),
            specs: vec![spec(&key(&[b"db"]), LeaseMode::Write, 100)],
            steal: false,
        };
        let (_, barrier) = block_on(mgr.acquire(&store, Timestamp(0), &req)).unwrap();
        assert_eq!(barrier, AdmissionSeq(0), "nothing admitted yet");
    }
}
