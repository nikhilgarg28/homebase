//! Lease management: the state machine behind `acquire` / `renew` /
//! `release` / `list_leases`, plus the reservation check `put_batch`
//! admission will use.
//!
//! All lease state lives in the ordered store (nothing special-cased in
//! memory except the advisory contention-demand set): a grant writes both
//! index records and the counter update in one atomic batch, so the lease
//! table survives crashes exactly as committed.
//!
//! Expiry is strict and local: a record whose deadline has passed is dead
//! the moment `now` reaches it, regardless of whether it is still on disk.
//! Dead records are purged lazily by whichever operation touches them.

use crate::error::Error;
use crate::schema::{
    CountersRecord, LeaseRecord, counters_key, lease_by_id_key, lease_by_id_scan,
    lease_by_prefix_key, lease_by_prefix_scan,
};
use crate::storage::{OrderedStore, ScanIter, StorageError, WriteBatch};
use homebase_core::clock::Timestamp;
use homebase_core::key::{Key, MAX_COMPONENTS};
use homebase_core::lease::{Lease, LeaseId};
use homebase_core::messages::{
    AcquireRequest, KernelError, ListLeasesRequest, ListLeasesResponse, ReleaseRequest, RenewGrant,
    RenewRequest, RenewResponse,
};
use homebase_core::space::SpaceId;
use homebase_core::tag::{AdmissionSeq, DeviceId, Epoch};
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
    pub async fn acquire<S: OrderedStore>(
        &mut self,
        store: &S,
        now: Timestamp,
        req: &AcquireRequest,
    ) -> Result<Vec<Lease>, Error> {
        let mut purge = WriteBatch::new();

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
                let overlap = a.prefix.starts_with(&b.prefix) || b.prefix.starts_with(&a.prefix);
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

        // Grant: records + counters in one atomic batch with the expiry purge.
        let mut counters = self.counters(store).await?;
        let barrier = AdmissionSeq(counters.admission_high_water);
        let mut batch = purge;
        let mut leases = Vec::with_capacity(req.specs.len());
        for spec in &req.specs {
            let record = LeaseRecord {
                id: LeaseId(counters.next_lease_id),
                prefix: spec.prefix.clone(),
                mode: spec.mode,
                device: req.device,
                requested_at: req.requested_at,
                granted_at: now,
                barrier,
                epoch: Epoch(counters.next_epoch),
                deadline: now.saturating_add(spec.ttl),
                ttl: spec.ttl,
            };
            counters.next_lease_id += 1;
            counters.next_epoch += 1;
            self.put_records(&mut batch, &record);
            leases.push(public_lease(&record));
        }
        batch.put(counters_key(self.space), counters.encode());
        store.apply(batch).await?;

        Ok(leases)
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
                        granted_at: now,
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

    /// Returns the live leases currently held by the requesting device.
    /// Expired records encountered during the scan are purged.
    pub async fn list_leases<S: OrderedStore>(
        &mut self,
        store: &S,
        now: Timestamp,
        req: &ListLeasesRequest,
    ) -> Result<ListLeasesResponse, Error> {
        let mut batch = WriteBatch::new();
        let mut leases = Vec::new();
        let scan = lease_by_id_scan(self.space);
        let mut iter = store.scan_prefix(&scan);
        while let Some((_, value)) = iter.next().await? {
            let rec = LeaseRecord::decode(&value).expect("corrupt lease record");
            if rec.is_live(now) {
                if rec.device == req.device {
                    leases.push(public_lease(&rec));
                }
            } else {
                self.purge_records(&mut batch, std::slice::from_ref(&rec));
            }
        }
        store.apply(batch).await?;
        Ok(ListLeasesResponse { leases })
    }

    /// The reservation check `put_batch` admission runs. Presented ids are
    /// diagnostic evidence only; they never authorize or reject admission. A
    /// key may be written when no live foreign lease overlaps it. Returns one
    /// epoch per key for the legacy tag field; writes use epoch 0 until the
    /// tag model is revised.
    pub async fn validate_put<S: OrderedStore>(
        &self,
        store: &S,
        now: Timestamp,
        device: DeviceId,
        _evidence: &[LeaseId],
        keys: &[Key],
    ) -> Result<Vec<Epoch>, Error> {
        let mut epochs = Vec::with_capacity(keys.len());
        for key in keys {
            let (live, _) = self.overlapping(store, now, key).await?;
            if live.iter().any(|rec| rec.device != device) {
                return Err(KernelError::Contended {
                    prefix: key.clone(),
                    retry_after: None,
                }
                .into());
            }
            epochs.push(Epoch(0));
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

    async fn counters<S: OrderedStore>(&self, store: &S) -> Result<CountersRecord, StorageError> {
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

fn public_lease(record: &LeaseRecord) -> Lease {
    Lease {
        id: record.id,
        prefix: record.prefix.clone(),
        mode: record.mode,
        requested_at: record.requested_at,
        granted_at: record.granted_at,
        ttl: record.ttl,
        barrier: record.barrier,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::lease_by_id_scan;
    use crate::storage::{MemoryStore, collect_scan};
    use homebase_core::clock::HybridTimestamp;
    use homebase_core::lease::LeaseMode;
    use homebase_core::messages::LeaseSpec;
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
            requested_at: HybridTimestamp::ZERO,
            specs: vec![spec(prefix, mode, ttl_ms)],
        };
        block_on(mgr.acquire(store, Timestamp(now), &req)).map(|mut leases| leases.remove(0))
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
    fn conflict_denied_pre_deadline_allowed_after_expiry_with_fresh_id() {
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
        let second = acquire_one(&mut mgr, &store, 50, 2, &p, LeaseMode::Write, 50).unwrap();
        assert!(second.id > first.id);
        // The expired record was purged when touched.
        assert_eq!(live_record_count(&store, 50), 1);
    }

    #[test]
    fn intra_batch_self_conflict_denied() {
        let (mut mgr, store) = (LeaseManager::new(SPACE), MemoryStore::new());
        let req = AcquireRequest {
            device: dev(1),
            requested_at: HybridTimestamp::ZERO,
            specs: vec![
                spec(&key(&[b"db"]), LeaseMode::Write, 100),
                spec(&key(&[b"db", b"t1"]), LeaseMode::Write, 100),
            ],
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
            &RenewRequest {
                device: dev(1),
                leases: vec![lease.id],
            },
        ))
        .unwrap();
        assert_eq!(resp.granted.len(), 1);
        assert!(!resp.granted[0].contended);

        // A failed acquire registers demand; the holder hears about it.
        let _ = acquire_one(&mut mgr, &store, 60, 2, &p, LeaseMode::Write, 50);
        let resp = block_on(mgr.renew(
            &store,
            Timestamp(80), // would be past the original deadline (50) without renewal
            &RenewRequest {
                device: dev(1),
                leases: vec![lease.id],
            },
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
            &RenewRequest {
                device: dev(2),
                leases: vec![lease.id],
            },
        ))
        .unwrap();
        assert_eq!(resp.invalid, vec![lease.id]);

        // Expired (and gets purged), plus an unknown id.
        let resp = block_on(mgr.renew(
            &store,
            Timestamp(50),
            &RenewRequest {
                device: dev(1),
                leases: vec![lease.id, LeaseId(999)],
            },
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
            &ReleaseRequest {
                device: dev(2),
                leases: vec![lease.id],
            },
        ))
        .unwrap();
        assert_eq!(live_record_count(&store, 10), 1);

        // Owner release frees the prefix; releasing again is fine.
        let req = ReleaseRequest {
            device: dev(1),
            leases: vec![lease.id],
        };
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
        let foreign = acquire_one(&mut mgr, &store, 0, 2, &other_row, LeaseMode::Read, 50).unwrap();

        // Own reservations do not block writes; evidence remains diagnostic.
        let epochs = block_on(mgr.validate_put(
            &store,
            Timestamp(10),
            dev(1),
            &[write.id],
            std::slice::from_ref(&row),
        ))
        .unwrap();
        assert_eq!(epochs.len(), 1);

        // No covering evidence is fine when no foreign lease overlaps.
        let free = key(&[b"free", b"row"]);
        assert_eq!(
            block_on(mgr.validate_put(
                &store,
                Timestamp(10),
                dev(1),
                &[],
                std::slice::from_ref(&free)
            ))
            .unwrap(),
            vec![Epoch(0)]
        );

        // Foreign reservations block uncovered writes.
        assert!(matches!(
            block_on(mgr.validate_put(
                &store,
                Timestamp(10),
                dev(1),
                &[],
                std::slice::from_ref(&other_row)
            )),
            Err(Error::Kernel(KernelError::Contended { .. }))
        ));

        // Evidence is diagnostic only: foreign, unknown, and expired ids do
        // not reject an otherwise unreserved write.
        assert_eq!(
            block_on(mgr.validate_put(
                &store,
                Timestamp(10),
                dev(1),
                &[foreign.id, LeaseId(999), write.id],
                std::slice::from_ref(&free)
            ))
            .unwrap(),
            vec![Epoch(0)]
        );

        // Foreign reservations block even when the request presents the
        // foreign lease as evidence.
        assert!(matches!(
            block_on(mgr.validate_put(
                &store,
                Timestamp(10),
                dev(1),
                &[foreign.id],
                std::slice::from_ref(&other_row)
            )),
            Err(Error::Kernel(KernelError::Contended { .. }))
        ));
    }

    #[test]
    fn acquire_reports_admission_barrier() {
        let (mut mgr, store) = (LeaseManager::new(SPACE), MemoryStore::new());
        let req = AcquireRequest {
            device: dev(1),
            requested_at: HybridTimestamp::ZERO,
            specs: vec![spec(&key(&[b"db"]), LeaseMode::Write, 100)],
        };
        let leases = block_on(mgr.acquire(&store, Timestamp(0), &req)).unwrap();
        assert_eq!(leases[0].barrier, AdmissionSeq(0), "nothing admitted yet");
        assert_eq!(leases[0].requested_at, HybridTimestamp::ZERO);
        assert_eq!(leases[0].granted_at, Timestamp(0));
    }
}
