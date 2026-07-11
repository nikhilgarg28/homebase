//! Per-space submit, pull, and lease operations for one [`SpaceId`],
//! reached through [`Client::attach`](crate::client::Client::attach) and
//! [`Client::space`](crate::client::Client::space).
//!
//! Data mutation has two local-only entry points. [`Space::submit_checked`]
//! requires every supplied range assertion to be backed by a live covering
//! lease and a local coverage watermark greater than or equal to `upto`;
//! [`Space::submit_unchecked`]
//! skips that preflight. Both durably append to this space's oplog and return
//! a [`Submission`]. Neither method performs network admission.

use crate::cipher::{CipherError, NonceSource, SpaceCipher, SystemNonceSource};
use crate::client::Client;
use crate::meta::{Committed, HeldLease, MetaStore};
use crate::server::ServerHandle;
use homebase_core::clock::{HybridClock, HybridTimestamp};
use homebase_core::key::Key;
use homebase_core::lease::{Lease, LeaseId, LeaseMode};
use homebase_core::messages::{
    AcquireRequest, KernelError, LeaseSpec, Range, RangeAssert, RangeCursor, RangeCut,
    ReadAtRequest, ReadAtResponse, ReleaseRequest, RenewRequest, RenewResponse,
};
use homebase_core::space::{SpaceError, SpaceId};
use homebase_core::storage::StorageError;
use homebase_core::tag::{AdmissionSeq, DeviceId, DeviceSeq, Value, Ver};
use std::fmt;
use std::time::Duration;

pub const DEFAULT_PUSH_CAP: usize = 256;

pub fn lease_margin(ttl: Duration) -> Duration {
    ttl / 1_000
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SpaceDriverError {
    Storage(StorageError),
    Cipher(CipherError),
    Nonce {
        reason: String,
    },
    Unavailable {
        reason: String,
    },
    Rejected(KernelError),
    Fork {
        admitted: DeviceSeq,
    },
    RangeAssertAuthority {
        prefix: Key,
    },
    RangeAssertAhead {
        prefix: Key,
        upto: AdmissionSeq,
        local: AdmissionSeq,
    },
    ReleaseBlocked {
        lease: LeaseId,
        at: DeviceSeq,
    },
}

impl From<StorageError> for SpaceDriverError {
    fn from(err: StorageError) -> Self {
        Self::Storage(err)
    }
}

impl From<CipherError> for SpaceDriverError {
    fn from(err: CipherError) -> Self {
        Self::Cipher(err)
    }
}

impl From<SpaceError> for SpaceDriverError {
    fn from(err: SpaceError) -> Self {
        match err {
            SpaceError::Kernel(err) => Self::Rejected(err),
            SpaceError::Unavailable { reason } => Self::Unavailable { reason },
        }
    }
}

impl fmt::Display for SpaceDriverError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Storage(err) => write!(f, "{err}"),
            Self::Cipher(err) => write!(f, "{err}"),
            Self::Nonce { reason } => write!(f, "nonce generation failed: {reason}"),
            Self::Unavailable { reason } => write!(f, "server unavailable: {reason}"),
            Self::Rejected(err) => write!(f, "server rejected: {err}"),
            Self::Fork { admitted } => write!(
                f,
                "device fork: the server admitted {admitted:?}, which this store never sent"
            ),
            Self::RangeAssertAuthority { prefix } => {
                write!(f, "no active local lease for asserted prefix {prefix:?}")
            }
            Self::RangeAssertAhead {
                prefix,
                upto,
                local,
            } => write!(
                f,
                "range assertion for {prefix:?} is upto {upto:?}, local coverage watermark is only {local:?}"
            ),
            Self::ReleaseBlocked { lease, at } => {
                write!(f, "lease {lease:?} still covers queued write {at:?}")
            }
        }
    }
}

impl std::error::Error for SpaceDriverError {}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Acquired {
    pub leases: Vec<Lease>,
    pub barrier: Option<AdmissionSeq>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PushOutcome {
    Drained {
        acked_through: Option<DeviceSeq>,
    },
    Stalled {
        at: DeviceSeq,
        error: KernelError,
        acked_through: Option<DeviceSeq>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LeaseState {
    pub held: HeldLease,
    pub live: bool,
}

/// A data batch durably appended to one space's local oplog.
///
/// The sequence identifies this exact local submission. Remote admission is
/// a separate push operation; per-submission push sugar lands with B13.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Submission {
    pub seq: DeviceSeq,
}

/// One logical client mutation. Version, nonce, seal, and ciphertext are
/// assigned internally when the mutation is appended to the oplog.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Mutation {
    Set { key: Key, value: Vec<u8> },
    Delete { key: Key },
}

impl From<(Key, Value)> for Mutation {
    fn from((key, value): (Key, Value)) -> Self {
        match value {
            Value::Present(value) => Self::Set { key, value },
            Value::Absent => Self::Delete { key },
        }
    }
}

/// A handle to one space within a [`Client`].
pub struct Space<'a, M, H, C, N = SystemNonceSource> {
    client: &'a Client<M, H, C, N>,
    id: SpaceId,
}

impl<'a, M: MetaStore, H: ServerHandle, C: HybridClock, N: NonceSource + Send + 'static>
    Space<'a, M, H, C, N>
{
    pub(crate) fn new(client: &'a Client<M, H, C, N>, id: SpaceId) -> Self {
        Self { client, id }
    }

    pub fn space_id(&self) -> SpaceId {
        self.id
    }

    pub fn device(&self) -> DeviceId {
        self.client.device()
    }

    pub fn cipher(&self) -> SpaceCipher {
        self.client.cipher(self.id)
    }

    pub async fn leases(&self, prefixes: &[Key]) -> Result<Vec<LeaseState>, SpaceDriverError> {
        let _permit = self.enter().await?;
        let now = self.client.clock().stamp();
        let prefixes = self.encode_keys(prefixes)?;
        let mut out = Vec::new();
        for held in self
            .client
            .store()
            .leases_covering(self.id, &prefixes)
            .await?
        {
            let live = self.lease_usable_for_held(&held, &now).await?;
            out.push(LeaseState { held, live });
        }
        Ok(out)
    }

    /// Append a local data batch after substantiating every range assertion
    /// from active local lease state and a sufficient local coverage cut.
    pub async fn submit_checked<I, T>(
        &self,
        mutations: I,
        range_asserts: Vec<RangeAssert>,
    ) -> Result<Submission, SpaceDriverError>
    where
        I: IntoIterator<Item = T>,
        T: Into<Mutation>,
    {
        let _permit = self.enter().await?;
        let mutations = mutations.into_iter().map(Into::into).collect();
        let (encoded, range_asserts) = self.encode_submission(mutations, range_asserts).await?;
        self.check_range_asserts(&range_asserts).await?;
        let committed = self.persist_submission(encoded, range_asserts).await?;
        Ok(Submission { seq: committed.seq })
    }

    /// Append a local data batch without checking lease-backed range
    /// assertions. The server still evaluates every assertion on push.
    pub async fn submit_unchecked<I, T>(
        &self,
        mutations: I,
        range_asserts: Vec<RangeAssert>,
    ) -> Result<Submission, SpaceDriverError>
    where
        I: IntoIterator<Item = T>,
        T: Into<Mutation>,
    {
        let _permit = self.enter().await?;
        let mutations = mutations.into_iter().map(Into::into).collect();
        let (encoded, range_asserts) = self.encode_submission(mutations, range_asserts).await?;
        let committed = self.persist_submission(encoded, range_asserts).await?;
        Ok(Submission { seq: committed.seq })
    }

    async fn encode_submission(
        &self,
        mutations: Vec<Mutation>,
        range_asserts: Vec<RangeAssert>,
    ) -> Result<(Vec<(Key, Value)>, Vec<RangeAssert>), SpaceDriverError> {
        let name_cipher = self.cipher();
        self.client
            .run_blocking(move || -> Result<_, CipherError> {
                let entries = mutations
                    .into_iter()
                    .map(|mutation| match mutation {
                        Mutation::Set { key, value } => {
                            Ok((name_cipher.encode_key(&key)?, Value::Present(value)))
                        }
                        Mutation::Delete { key } => {
                            Ok((name_cipher.encode_key(&key)?, Value::Absent))
                        }
                    })
                    .collect::<Result<Vec<_>, CipherError>>()?;
                let range_asserts = range_asserts
                    .into_iter()
                    .map(|assert| {
                        Ok(RangeAssert {
                            prefix: name_cipher.encode_key(&assert.prefix)?,
                            upto: assert.upto,
                        })
                    })
                    .collect::<Result<Vec<_>, CipherError>>()?;
                Ok((entries, range_asserts))
            })
            .await
            .map_err(coordination_unavailable)?
            .map_err(Into::into)
    }

    async fn persist_submission(
        &self,
        encoded: Vec<(Key, Value)>,
        range_asserts: Vec<RangeAssert>,
    ) -> Result<Committed, SpaceDriverError> {
        let cipher = self.cipher();
        let device = self.device();
        let reserved = self
            .client
            .store()
            .reserve_commit(self.id, encoded, range_asserts)
            .await?;
        let nonces = (0..reserved.record.ops().len())
            .map(|_| {
                self.client
                    .next_nonce()
                    .map_err(|reason| SpaceDriverError::Nonce { reason })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let reserved = self
            .client
            .run_blocking(move || {
                let mut reserved = reserved;
                for (op, nonce) in reserved.record.ops_mut().iter_mut().zip(nonces) {
                    let entry = match op {
                        homebase_core::messages::BatchOp::Set {
                            key,
                            ver,
                            ciphertext,
                            ..
                        } => homebase_core::messages::PutEntry {
                            key: key.clone(),
                            value: Value::Present(ciphertext.clone()),
                            ver: *ver,
                        },
                        homebase_core::messages::BatchOp::Delete { key, ver, .. } => {
                            homebase_core::messages::PutEntry {
                                key: key.clone(),
                                value: Value::Absent,
                                ver: *ver,
                            }
                        }
                        homebase_core::messages::BatchOp::NoOp => continue,
                    };
                    *op = cipher.encode_batch_op(device, reserved.seq, &entry, nonce)?;
                }
                Ok::<_, CipherError>(reserved)
            })
            .await
            .map_err(coordination_unavailable)??;
        let committed = self.client.store().commit(self.id, reserved).await?;
        Ok(committed)
    }

    pub async fn acquire(&self, specs: Vec<LeaseSpec>) -> Result<Acquired, SpaceDriverError> {
        let _permit = self.enter().await?;
        self.acquire_inner(specs).await
    }

    async fn acquire_inner(&self, specs: Vec<LeaseSpec>) -> Result<Acquired, SpaceDriverError> {
        let specs = self.encode_specs(specs)?;
        let space = self.id;
        if specs.is_empty() {
            return Ok(Acquired {
                leases: vec![],
                barrier: None,
            });
        }

        let queried: Vec<Key> = specs.iter().map(|spec| spec.prefix.clone()).collect();
        let now = self.client.clock().stamp();
        let held = self.client.store().leases_covering(space, &queried).await?;
        let mut revive: Vec<LeaseId> = Vec::new();
        for spec in &specs {
            if self.usable_covering(&held, spec, &now).await?.is_some() {
                continue;
            }
            if let Some(id) = held
                .iter()
                .find(|h| !h.retiring && covers(h, spec) && h.barrier.is_none())
                .map(|h| h.lease.id)
            {
                revive.push(id);
            }
        }
        revive.sort_unstable();
        revive.dedup();
        if !revive.is_empty() {
            self.renew_ids(space, &revive, &held).await?;
        }

        let send = self.client.clock().stamp();
        let held = self.client.store().leases_covering(space, &queried).await?;
        let mut satisfied: Vec<Option<Lease>> = Vec::with_capacity(specs.len());
        for spec in &specs {
            satisfied.push(
                self.usable_covering(&held, spec, &send)
                    .await?
                    .or_else(|| pending_barrier_covering(&held, spec, &send))
                    .cloned(),
            );
        }
        let missing: Vec<LeaseSpec> = specs
            .iter()
            .zip(&satisfied)
            .filter(|(_, slot)| slot.is_none())
            .map(|(spec, _)| spec.clone())
            .collect();
        if missing.is_empty() {
            let leases = satisfied.into_iter().flatten().collect();
            return Ok(Acquired {
                leases,
                barrier: self.max_pending_barrier(&held, &specs, &send).await?,
            });
        }

        let response = self
            .client
            .server()
            .acquire(
                &space,
                AcquireRequest {
                    device: self.device(),
                    requested_at: send,
                    specs: missing,
                },
            )
            .await?;
        let mut fresh = Vec::with_capacity(response.leases.len());
        for lease in &response.leases {
            let watermark = self
                .client
                .store()
                .watermark(space, &Range::Prefix(lease.prefix.clone()))
                .await?;
            fresh.push(HeldLease {
                lease: lease.clone(),
                deadline: send.saturating_add(lease.ttl),
                barrier: pending_barrier(lease.barrier, watermark),
                retiring: false,
            });
        }
        self.client.store().record_clock(send.wall).await?;
        self.client.store().record_leases(space, &fresh).await?;

        let mut granted = response.leases.into_iter();
        for slot in &mut satisfied {
            if slot.is_none() {
                *slot = Some(granted.next().expect("grants parallel to missing specs"));
            }
        }
        let existing_barrier = self.max_pending_barrier(&held, &specs, &send).await?;
        let fresh_barrier = fresh.iter().filter_map(|held| held.barrier).max();
        Ok(Acquired {
            leases: satisfied.into_iter().flatten().collect(),
            barrier: existing_barrier.max(fresh_barrier),
        })
    }

    pub async fn ensure(&self, specs: Vec<LeaseSpec>) -> Result<Acquired, SpaceDriverError> {
        let _permit = self.enter().await?;
        let acquired = self.acquire_inner(specs).await?;
        if acquired.barrier.is_none() {
            return Ok(acquired);
        }

        let mut pulled = Vec::<Key>::new();
        for lease in &acquired.leases {
            let held = self.held_lease_by_id(lease.id).await?;
            if held.barrier.is_some()
                && !self
                    .lease_usable_for_held(&held, &self.client.clock().stamp())
                    .await?
                && !pulled.iter().any(|prefix| prefix == &lease.prefix)
            {
                self.pull_encoded(Range::Prefix(lease.prefix.clone()))
                    .await?;
                pulled.push(lease.prefix.clone());
            }
        }

        Ok(Acquired {
            leases: acquired.leases,
            barrier: None,
        })
    }

    pub async fn renew(&self, prefixes: &[Key]) -> Result<RenewResponse, SpaceDriverError> {
        let _permit = self.enter().await?;
        let prefixes = self.encode_keys(prefixes)?;
        let held = if prefixes.is_empty() {
            Vec::new()
        } else {
            self.client
                .store()
                .leases_covering(self.id, &prefixes)
                .await?
        };
        let ids: Vec<LeaseId> = held
            .iter()
            .filter(|held| !held.retiring)
            .map(|h| h.lease.id)
            .collect();
        self.renew_ids(self.id, &ids, &held).await
    }

    pub async fn release(&self, ids: &[LeaseId]) -> Result<(), SpaceDriverError> {
        let _permit = self.enter().await?;
        if ids.is_empty() {
            return Ok(());
        }
        self.reject_release_if_queued_writes(ids).await?;
        self.client.store().retire_leases(self.id, ids).await?;
        self.client
            .server()
            .release(
                &self.id,
                ReleaseRequest {
                    device: self.device(),
                    leases: ids.to_vec(),
                },
            )
            .await?;
        self.client.store().drop_leases(self.id, ids).await?;
        Ok(())
    }

    pub async fn pull(&self, range: Range) -> Result<ReadAtResponse, SpaceDriverError> {
        let _permit = self.enter().await?;
        let range = self.cipher().encode_range(&range)?;
        self.pull_encoded(range).await
    }

    async fn enter(
        &self,
    ) -> Result<crate::coordination::SpacePermit<crate::client::ClientSession<N>>, SpaceDriverError>
    {
        self.client
            .enter_space(self.id)
            .await
            .map_err(|error| SpaceDriverError::Unavailable {
                reason: error.to_string(),
            })
    }

    async fn pull_encoded(&self, range: Range) -> Result<ReadAtResponse, SpaceDriverError> {
        let space = self.id;
        let cipher = self.cipher();
        let since = self.client.store().watermark(space, &range).await?;
        let mut response = self
            .client
            .server()
            .read_at(
                &space,
                ReadAtRequest {
                    ranges: vec![RangeCursor {
                        range: range.clone(),
                        since,
                    }],
                },
            )
            .await?;
        let ver_seen = response
            .ranges
            .iter()
            .flat_map(|range| {
                let (RangeCut::Snapshot(entries) | RangeCut::Delta(entries)) = range;
                entries.iter().map(|entry| entry.tag.ver)
            })
            .max()
            .unwrap_or(Ver(0));
        self.client
            .store()
            .advance_watermark(space, &range, response.at, ver_seen)
            .await?;
        self.client
            .run_blocking(move || {
                for cut in &mut response.ranges {
                    let entries = match cut {
                        RangeCut::Snapshot(entries) | RangeCut::Delta(entries) => entries,
                    };
                    for entry in entries {
                        entry.value = cipher.decode_entry_value(entry)?;
                    }
                }
                Ok::<_, CipherError>(response)
            })
            .await
            .map_err(coordination_unavailable)?
            .map_err(Into::into)
    }

    fn lease_live(&self, held: &HeldLease, now: &HybridTimestamp) -> bool {
        !held.deadline.expired(now, lease_margin(held.lease.ttl))
    }

    async fn renew_ids(
        &self,
        space: SpaceId,
        ids: &[LeaseId],
        held: &[HeldLease],
    ) -> Result<RenewResponse, SpaceDriverError> {
        if ids.is_empty() {
            return Ok(RenewResponse {
                granted: vec![],
                invalid: vec![],
            });
        }
        let send = self.client.clock().stamp();
        let response = self
            .client
            .server()
            .renew(
                &space,
                RenewRequest {
                    device: self.device(),
                    leases: ids.to_vec(),
                },
            )
            .await?;
        if !response.granted.is_empty() {
            let refreshed: Vec<HeldLease> = response
                .granted
                .iter()
                .map(|grant| {
                    let mut held = held
                        .iter()
                        .find(|h| h.lease.id == grant.id)
                        .expect("server renewed a lease the store does not hold")
                        .clone();
                    held.lease.ttl = grant.ttl;
                    held.deadline = send.saturating_add(grant.ttl);
                    held
                })
                .collect();
            self.client.store().record_clock(send.wall).await?;
            self.client.store().record_leases(space, &refreshed).await?;
        }
        if !response.invalid.is_empty() {
            self.client
                .store()
                .drop_leases(space, &response.invalid)
                .await?;
        }
        Ok(response)
    }

    async fn reject_release_if_queued_writes(
        &self,
        ids: &[LeaseId],
    ) -> Result<(), SpaceDriverError> {
        let state = self.client.store().load().await?;
        let Some(space_state) = state.spaces.get(&self.id) else {
            return Ok(());
        };
        let releasing: Vec<&HeldLease> = ids
            .iter()
            .filter_map(|id| space_state.leases.get(id))
            .collect();
        if releasing.is_empty() {
            return Ok(());
        }
        for (seq, record) in space_state.active_oplog() {
            for held in &releasing {
                if record.ops().iter().any(|op| {
                    op.key()
                        .is_some_and(|key| key.starts_with(&held.lease.prefix))
                }) {
                    return Err(SpaceDriverError::ReleaseBlocked {
                        lease: held.lease.id,
                        at: *seq,
                    });
                }
            }
        }
        Ok(())
    }

    async fn held_lease_by_id(&self, id: LeaseId) -> Result<HeldLease, SpaceDriverError> {
        let state = self.client.store().load().await?;
        Ok(state
            .spaces
            .get(&self.id)
            .and_then(|space_state| space_state.leases.get(&id))
            .cloned()
            .expect("acquire returned a lease that is not durably held"))
    }

    async fn check_range_asserts(
        &self,
        range_asserts: &[RangeAssert],
    ) -> Result<(), SpaceDriverError> {
        if range_asserts.is_empty() {
            return Ok(());
        }
        let prefixes: Vec<_> = range_asserts
            .iter()
            .map(|assert| assert.prefix.clone())
            .collect();
        let held = self
            .client
            .store()
            .leases_covering(self.id, &prefixes)
            .await?;
        let now = self.client.clock().stamp();
        for assert in range_asserts {
            let mut covered = false;
            for lease in &held {
                if assert.prefix.starts_with(&lease.lease.prefix)
                    && self.lease_usable_for_held(lease, &now).await?
                {
                    covered = true;
                    break;
                }
            }
            if !covered {
                return Err(SpaceDriverError::RangeAssertAuthority {
                    prefix: assert.prefix.clone(),
                });
            }
            let local = self
                .client
                .store()
                .watermark(self.id, &Range::Prefix(assert.prefix.clone()))
                .await?
                .unwrap_or(AdmissionSeq(0));
            if local < assert.upto {
                return Err(SpaceDriverError::RangeAssertAhead {
                    prefix: assert.prefix.clone(),
                    upto: assert.upto,
                    local,
                });
            }
        }
        Ok(())
    }

    fn encode_keys(&self, keys: &[Key]) -> Result<Vec<Key>, SpaceDriverError> {
        keys.iter()
            .map(|key| Ok(self.cipher().encode_key(key)?))
            .collect()
    }

    fn encode_specs(&self, specs: Vec<LeaseSpec>) -> Result<Vec<LeaseSpec>, SpaceDriverError> {
        specs
            .into_iter()
            .map(|spec| {
                Ok(LeaseSpec {
                    prefix: self.cipher().encode_key(&spec.prefix)?,
                    mode: spec.mode,
                    ttl: spec.ttl,
                })
            })
            .collect()
    }

    fn lease_usable(
        &self,
        held: &HeldLease,
        now: &HybridTimestamp,
        watermark: Option<AdmissionSeq>,
    ) -> bool {
        !held.retiring && self.lease_live(held, now) && barrier_satisfied(held.barrier, watermark)
    }

    async fn lease_usable_for_held(
        &self,
        held: &HeldLease,
        now: &HybridTimestamp,
    ) -> Result<bool, SpaceDriverError> {
        let watermark = self
            .client
            .store()
            .watermark(self.id, &Range::Prefix(held.lease.prefix.clone()))
            .await?;
        Ok(self.lease_usable(held, now, watermark))
    }

    async fn usable_covering<'b>(
        &self,
        held: &'b [HeldLease],
        spec: &LeaseSpec,
        now: &HybridTimestamp,
    ) -> Result<Option<&'b Lease>, SpaceDriverError> {
        for h in held {
            if covers(h, spec) && self.lease_usable_for_held(h, now).await? {
                return Ok(Some(&h.lease));
            }
        }
        Ok(None)
    }

    async fn max_pending_barrier(
        &self,
        held: &[HeldLease],
        specs: &[LeaseSpec],
        now: &HybridTimestamp,
    ) -> Result<Option<AdmissionSeq>, SpaceDriverError> {
        let mut out = None;
        for spec in specs {
            for held in held {
                if held.retiring
                    || held.barrier.is_none()
                    || held.deadline.expired(now, lease_margin(held.lease.ttl))
                    || !covers(held, spec)
                {
                    continue;
                }
                let watermark = self
                    .client
                    .store()
                    .watermark(self.id, &Range::Prefix(held.lease.prefix.clone()))
                    .await?;
                if let Some(barrier) = held.barrier {
                    if !barrier_satisfied(Some(barrier), watermark) {
                        out =
                            Some(out.map_or(barrier, |current: AdmissionSeq| current.max(barrier)));
                    }
                }
            }
        }
        Ok(out)
    }
}

pub(crate) async fn live_write_leases<M: MetaStore, C: HybridClock>(
    store: &M,
    clock: &C,
    space: SpaceId,
    keys: &[Key],
) -> Result<Vec<LeaseId>, SpaceDriverError> {
    let now = clock.stamp();
    let mut out = Vec::new();
    for held in store.leases_covering(space, keys).await? {
        if held.lease.mode != LeaseMode::Write {
            continue;
        }
        let watermark = store
            .watermark(space, &Range::Prefix(held.lease.prefix.clone()))
            .await?;
        if !held.retiring
            && !held.deadline.expired(&now, lease_margin(held.lease.ttl))
            && barrier_satisfied(held.barrier, watermark)
        {
            out.push(held.lease.id);
        }
    }
    Ok(out)
}

fn barrier_satisfied(barrier: Option<AdmissionSeq>, watermark: Option<AdmissionSeq>) -> bool {
    barrier.is_none_or(|barrier| watermark.unwrap_or(AdmissionSeq(0)) >= barrier)
}

fn pending_barrier(barrier: AdmissionSeq, watermark: Option<AdmissionSeq>) -> Option<AdmissionSeq> {
    (!barrier_satisfied(Some(barrier), watermark)).then_some(barrier)
}

fn pending_barrier_covering<'a>(
    held: &'a [HeldLease],
    spec: &LeaseSpec,
    now: &HybridTimestamp,
) -> Option<&'a Lease> {
    held.iter()
        .filter(|held| !held.retiring && held.barrier.is_some())
        .filter(|held| !held.deadline.expired(now, lease_margin(held.lease.ttl)))
        .find(|held| covers(held, spec))
        .map(|held| &held.lease)
}

fn covers(held: &HeldLease, spec: &LeaseSpec) -> bool {
    spec.prefix.starts_with(&held.lease.prefix) && mode_covers(held.lease.mode, spec.mode)
}

fn mode_covers(held: LeaseMode, want: LeaseMode) -> bool {
    matches!(
        (held, want),
        (LeaseMode::Write, _) | (LeaseMode::Read, LeaseMode::Read)
    )
}

fn coordination_unavailable(error: crate::coordination::CoordinationError) -> SpaceDriverError {
    SpaceDriverError::Unavailable {
        reason: error.to_string(),
    }
}
