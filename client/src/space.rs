//! Per-space submit, pull, and lease operations for one [`SpaceId`],
//! reached through [`Client::attach`](crate::client::Client::attach) and
//! [`Client::space`](crate::client::Client::space).
//!
//! Data mutation has two local-only entry points. [`Space::submit_checked`]
//! requires every supplied range assertion to be backed by a live covering
//! lease and a local coverage watermark greater than or equal to `upto`;
//! [`Space::submit_unchecked`]
//! skips that preflight. Both durably append to this space's oplog and return
//! a [`Submission`]. Neither method performs network admission. The persisted
//! submit mode also lets [`Space::release_checked`] preserve reservation
//! coverage for checked assertions; [`Space::release_unchecked`] deliberately
//! skips that potentially expensive local scan. [`Space::push`] drains only
//! this space, [`Space::push_until`] stops at a chosen local sequence, and
//! [`Submission::push`] is attribution sugar for the latter.
//!
//! Inbound operations have a separate durable admit log. [`Space::pull`]
//! captures and authenticates the dense full-space server suffix but never
//! claims that the application applied it. [`Space::admits`] exposes pending
//! batches and the explicit application/trim cursor transitions. By contrast,
//! [`Space::fetch`] is a stateless range observation and changes no client
//! replication, version, or lease state.

use crate::cipher::{CipherError, NonceSource, SpaceCipher, SystemNonceSource};
use crate::client::Client;
use crate::meta::{AdmitCursors, Committed, HeldLease, MetaStore, SubmitMode};
use crate::server::ServerHandle;
use homebase_core::clock::{HybridClock, HybridTimestamp};
use homebase_core::key::Key;
use homebase_core::lease::{Lease, LeaseId, LeaseMode};
use homebase_core::messages::{
    AcquireRequest, AdmittedBatch, KernelError, LeaseSpec, ListLeasesRequest, PullRequest, Range,
    RangeAssert, RangeCursor, RangeCut, ReadAtRequest, ReadAtResponse, ReleaseRequest,
    RenewRequest, RenewResponse,
};
use homebase_core::space::{SpaceError, SpaceId};
use homebase_core::storage::StorageError;
use homebase_core::tag::{
    AdmissionSeq, CipherEpoch, DeviceId, DeviceSeq, DeviceTag, Mutation, Ver,
};
use std::fmt;
use std::time::Duration;

pub const DEFAULT_PUSH_CAP: usize = 256;
pub const DEFAULT_PULL_CAP: usize = 256;
const MIN_LEASE_MARGIN: Duration = Duration::from_millis(10);

pub fn lease_margin(ttl: Duration) -> Duration {
    (ttl / 1_000).max(MIN_LEASE_MARGIN)
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
    SubmissionNotPending {
        seq: DeviceSeq,
    },
    MalformedResponse {
        reason: String,
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
                write!(
                    f,
                    "releasing lease {lease:?} would leave checked submission {at:?} unreserved"
                )
            }
            Self::SubmissionNotPending { seq } => {
                write!(f, "submission {seq:?} is no longer pending")
            }
            Self::MalformedResponse { reason } => write!(f, "malformed server response: {reason}"),
        }
    }
}

impl std::error::Error for SpaceDriverError {}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Ensured {
    pub leases: Vec<Lease>,
    pub barrier: Option<AdmissionSeq>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RepairedLeases {
    pub active: Vec<Lease>,
    pub forgotten: Vec<LeaseId>,
}

/// One authenticated stateless range observation and its next cursor.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FetchedRange<T = Vec<u8>> {
    pub range: Range,
    pub at: AdmissionSeq,
    pub cut: RangeCut<T>,
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

/// The disposition of the exact submission passed to [`Submission::push`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PushReceipt {
    Applied {
        seq: DeviceSeq,
        /// Absent when a retry discovers that the server had already applied
        /// the submission but its original response was lost.
        admission_seq: Option<AdmissionSeq>,
    },
    Failed {
        seq: DeviceSeq,
        error: KernelError,
    },
    Blocked {
        seq: DeviceSeq,
        at: DeviceSeq,
        error: KernelError,
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
/// a separate push operation. Retaining this handle permits retrying an
/// ambiguous [`Submission::push`] without another durable state machine.
pub struct Submission<'a, M, H, C, N = SystemNonceSource> {
    pub seq: DeviceSeq,
    client: &'a Client<M, H, C, N>,
    space: SpaceId,
}

impl<M, H, C, N> fmt::Debug for Submission<'_, M, H, C, N> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Submission")
            .field("seq", &self.seq)
            .field("space", &self.space)
            .finish_non_exhaustive()
    }
}

impl<'a, M: MetaStore, H: ServerHandle, C: HybridClock, N: NonceSource + Send + 'static>
    Submission<'a, M, H, C, N>
{
    /// Push this space through this submission and return its exact outcome.
    pub async fn push(&self) -> Result<PushReceipt, crate::client::ClientError> {
        let run = self.client.push_space(self.space, Some(self.seq)).await?;
        match run.outcome {
            PushOutcome::Drained { .. } => Ok(PushReceipt::Applied {
                seq: self.seq,
                admission_seq: run.target_admission,
            }),
            PushOutcome::Stalled {
                at,
                error,
                acked_through: _,
            } if at == self.seq => Ok(PushReceipt::Failed {
                seq: self.seq,
                error,
            }),
            PushOutcome::Stalled {
                at,
                error,
                acked_through: _,
            } => Ok(PushReceipt::Blocked {
                seq: self.seq,
                at,
                error,
            }),
        }
    }
}

/// A handle to one space within a [`Client`].
pub struct Space<'a, M, H, C, N = SystemNonceSource> {
    client: &'a Client<M, H, C, N>,
    id: SpaceId,
}

/// Application-facing access to one space's durable inbound admission log.
pub struct Admits<'space, 'client, M, H, C, N = SystemNonceSource> {
    space: &'space Space<'client, M, H, C, N>,
}

impl<M, H, C, N> fmt::Debug for Admits<'_, '_, M, H, C, N> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Admits")
            .field("space", &self.space.id)
            .finish_non_exhaustive()
    }
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

    pub fn admits(&self) -> Admits<'_, 'a, M, H, C, N> {
        Admits { space: self }
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
    ) -> Result<Submission<'_, M, H, C, N>, SpaceDriverError>
    where
        I: IntoIterator<Item = T>,
        T: Into<Mutation>,
    {
        let _permit = self.enter().await?;
        let mutations = mutations.into_iter().map(Into::into).collect();
        let (encoded, range_asserts) = self.encode_submission(mutations, range_asserts).await?;
        self.check_range_asserts(&range_asserts).await?;
        let committed = self
            .persist_submission(encoded, range_asserts, SubmitMode::Checked)
            .await?;
        Ok(Submission {
            seq: committed.seq,
            client: self.client,
            space: self.id,
        })
    }

    /// Append a local data batch without checking lease-backed range
    /// assertions. The server still evaluates every assertion on push.
    pub async fn submit_unchecked<I, T>(
        &self,
        mutations: I,
        range_asserts: Vec<RangeAssert>,
    ) -> Result<Submission<'_, M, H, C, N>, SpaceDriverError>
    where
        I: IntoIterator<Item = T>,
        T: Into<Mutation>,
    {
        let _permit = self.enter().await?;
        let mutations = mutations.into_iter().map(Into::into).collect();
        let (encoded, range_asserts) = self.encode_submission(mutations, range_asserts).await?;
        let committed = self
            .persist_submission(encoded, range_asserts, SubmitMode::Unchecked)
            .await?;
        Ok(Submission {
            seq: committed.seq,
            client: self.client,
            space: self.id,
        })
    }

    /// Push this space's active oplog as far as possible.
    pub async fn push(&self) -> Result<PushOutcome, crate::client::ClientError> {
        Ok(self.client.push_space(self.id, None).await?.outcome)
    }

    /// Push this space only through `seq`; later submissions are not sent.
    pub async fn push_until(
        &self,
        seq: DeviceSeq,
    ) -> Result<PushOutcome, crate::client::ClientError> {
        Ok(self.client.push_space(self.id, Some(seq)).await?.outcome)
    }

    async fn encode_submission(
        &self,
        mutations: Vec<Mutation>,
        range_asserts: Vec<RangeAssert>,
    ) -> Result<(Vec<Mutation>, Vec<RangeAssert>), SpaceDriverError> {
        let name_cipher = self.cipher();
        self.client
            .run_blocking(move || -> Result<_, CipherError> {
                let mutations = mutations
                    .into_iter()
                    .map(|mutation| match mutation {
                        Mutation::Set { key, value } => Ok(Mutation::Set {
                            key: name_cipher.encode_key(&key)?,
                            value,
                        }),
                        Mutation::Delete { key } => Ok(Mutation::Delete {
                            key: name_cipher.encode_key(&key)?,
                        }),
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
                Ok((mutations, range_asserts))
            })
            .await
            .map_err(coordination_unavailable)?
            .map_err(Into::into)
    }

    async fn persist_submission(
        &self,
        encoded: Vec<Mutation>,
        range_asserts: Vec<RangeAssert>,
        submit_mode: SubmitMode,
    ) -> Result<Committed, SpaceDriverError> {
        let cipher = self.cipher();
        let device = self.device();
        let reserved = self
            .client
            .store()
            .reserve_commit(self.id, encoded.len(), range_asserts, submit_mode)
            .await?;
        let nonces = (0..encoded.len())
            .map(|_| {
                self.client
                    .next_nonce()
                    .map_err(|reason| SpaceDriverError::Nonce { reason })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let seq = reserved.seq;
        let versions = reserved.versions.clone();
        let entries = self
            .client
            .run_blocking(move || {
                encoded
                    .into_iter()
                    .zip(versions)
                    .zip(nonces)
                    .map(|((mutation, ver), nonce)| {
                        cipher.encode_device_entry(
                            mutation,
                            DeviceTag {
                                device,
                                device_seq: seq,
                                ver,
                                cipher_epoch: CipherEpoch(crate::cipher::V1_CIPHER_EPOCH),
                            },
                            nonce,
                        )
                    })
                    .collect::<Result<Vec<_>, CipherError>>()
            })
            .await
            .map_err(coordination_unavailable)??;
        let committed = self
            .client
            .store()
            .commit(self.id, reserved, entries)
            .await?;
        Ok(committed)
    }

    async fn ensure_inner(&self, specs: Vec<LeaseSpec>) -> Result<Ensured, SpaceDriverError> {
        let specs = self.encode_specs(specs)?;
        let space = self.id;
        if specs.is_empty() {
            return Ok(Ensured {
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
                .find(|h| !h.forgotten && covers(h, spec) && !self.lease_live(h, &now))
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
            return Ok(Ensured {
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
                forgotten: false,
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
        Ok(Ensured {
            leases: satisfied.into_iter().flatten().collect(),
            barrier: existing_barrier.max(fresh_barrier),
        })
    }

    pub async fn ensure(&self, specs: Vec<LeaseSpec>) -> Result<Ensured, SpaceDriverError> {
        let _permit = self.enter().await?;
        let acquired = self.ensure_inner(specs).await?;
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

        Ok(Ensured {
            leases: acquired.leases,
            barrier: None,
        })
    }

    /// Rebuild local lease state from the server's complete live view for
    /// this device. Reconciled grants enter local state before barrier pulls,
    /// but remain unusable until their individual barriers are satisfied.
    pub async fn repair_leases(&self) -> Result<RepairedLeases, SpaceDriverError> {
        let _permit = self.enter().await?;
        let response = self
            .client
            .server()
            .list_leases(
                &self.id,
                ListLeasesRequest {
                    device: self.device(),
                },
            )
            .await?;
        let state = self.client.store().load().await?;
        let local = state.spaces.get(&self.id);
        let mut reconciled = Vec::with_capacity(response.leases.len());
        for lease in response.leases {
            let forgotten = local
                .and_then(|space| space.leases.get(&lease.id))
                .is_some_and(|held| held.forgotten);
            let watermark = self
                .client
                .store()
                .watermark(self.id, &Range::Prefix(lease.prefix.clone()))
                .await?;
            reconciled.push(HeldLease {
                deadline: lease.requested_at.saturating_add(lease.ttl),
                barrier: pending_barrier(lease.barrier, watermark),
                lease,
                forgotten,
            });
        }
        self.client
            .store()
            .reconcile_leases(self.id, &reconciled)
            .await?;

        let mut pulled = Vec::<Key>::new();
        for held in &reconciled {
            if held.forgotten || held.barrier.is_none() {
                continue;
            }
            let now = self.client.clock().stamp();
            if self.lease_live(held, &now)
                && !self.lease_usable_for_held(held, &now).await?
                && !pulled.iter().any(|prefix| prefix == &held.lease.prefix)
            {
                self.pull_encoded(Range::Prefix(held.lease.prefix.clone()))
                    .await?;
                pulled.push(held.lease.prefix.clone());
            }
        }

        let now = self.client.clock().stamp();
        let mut active = Vec::new();
        let mut forgotten = Vec::new();
        for held in &reconciled {
            if held.forgotten {
                forgotten.push(held.lease.id);
            } else if self.lease_usable_for_held(held, &now).await? {
                active.push(held.lease.clone());
            }
        }
        Ok(RepairedLeases { active, forgotten })
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
            .filter(|held| !held.forgotten)
            .map(|h| h.lease.id)
            .collect();
        self.renew_ids(self.id, &ids, &held).await
    }

    /// Release leases only if every checked, unpushed range assertion keeps
    /// another live covering reservation after the whole release set is removed.
    pub async fn release_checked(&self, ids: &[LeaseId]) -> Result<(), SpaceDriverError> {
        let _permit = self.enter().await?;
        if ids.is_empty() {
            return Ok(());
        }
        self.reject_release_if_checked_assertions_lose_coverage(ids)
            .await?;
        self.release_inner(ids).await
    }

    /// Release leases without preserving reservation coverage for queued
    /// checked assertions. Server admission still evaluates those assertions.
    pub async fn release_unchecked(&self, ids: &[LeaseId]) -> Result<(), SpaceDriverError> {
        let _permit = self.enter().await?;
        if ids.is_empty() {
            return Ok(());
        }
        self.release_inner(ids).await
    }

    async fn release_inner(&self, ids: &[LeaseId]) -> Result<(), SpaceDriverError> {
        if ids.is_empty() {
            return Ok(());
        }
        self.client.store().forget_leases(self.id, ids).await?;
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

    /// Capture all currently available complete server batches into the
    /// durable admit log. Each bounded page is authenticated before append.
    /// Returns the last captured server admission sequence.
    pub async fn pull(&self) -> Result<AdmissionSeq, SpaceDriverError> {
        let _permit = self.enter().await?;
        loop {
            let cursors = self.client.store().admit_cursors(self.id).await?;
            let after = AdmissionSeq(cursors.tail.0.checked_sub(1).ok_or_else(|| {
                SpaceDriverError::MalformedResponse {
                    reason: "local admit tail cannot be zero".into(),
                }
            })?);
            let response = self
                .client
                .server()
                .pull(
                    &self.id,
                    PullRequest {
                        after,
                        max_batches: Some(DEFAULT_PULL_CAP),
                    },
                )
                .await?;
            if response.after != after {
                return Err(SpaceDriverError::MalformedResponse {
                    reason: format!(
                        "response starts after {:?}, request was after {:?}",
                        response.after, after
                    ),
                });
            }
            response
                .validate_dense()
                .map_err(|error| SpaceDriverError::MalformedResponse {
                    reason: error.to_string(),
                })?;
            if response.batches.len() > DEFAULT_PULL_CAP {
                return Err(SpaceDriverError::MalformedResponse {
                    reason: format!(
                        "response contains {} batches, limit was {DEFAULT_PULL_CAP}",
                        response.batches.len()
                    ),
                });
            }
            let page_len = response.batches.len();
            let cipher = self.cipher();
            let response = self
                .client
                .run_blocking(move || {
                    for batch in &response.batches {
                        for entry in &batch.entries {
                            cipher.open_admitted_entry(entry)?;
                        }
                    }
                    Ok::<_, CipherError>(response)
                })
                .await
                .map_err(coordination_unavailable)??;
            let through = response.through;
            self.client
                .store()
                .append_admits(self.id, &response)
                .await?;
            if page_len < DEFAULT_PULL_CAP {
                return Ok(through);
            }
        }
    }

    /// Read one authenticated range delta after `after` without changing any
    /// client replication, version, or lease state.
    pub async fn fetch(
        &self,
        range: Range,
        after: AdmissionSeq,
    ) -> Result<FetchedRange, SpaceDriverError> {
        let _permit = self.enter().await?;
        let encoded_range = self.cipher().encode_range(&range)?;
        let response = self
            .client
            .server()
            .read_at(
                &self.id,
                ReadAtRequest {
                    ranges: vec![RangeCursor {
                        range: encoded_range,
                        since: Some(after),
                    }],
                },
            )
            .await?;
        if response.ranges.len() != 1 {
            return Err(SpaceDriverError::MalformedResponse {
                reason: format!(
                    "one-range fetch returned {} range cuts",
                    response.ranges.len()
                ),
            });
        }
        let at = response.at;
        let cipher = self.cipher();
        let cut = self
            .client
            .run_blocking(move || {
                open_range_cut(&cipher, response.ranges.into_iter().next().unwrap())
            })
            .await
            .map_err(coordination_unavailable)?
            .map_err(SpaceDriverError::from)?;
        Ok(FetchedRange { range, at, cut })
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

    async fn pull_encoded(
        &self,
        range: Range,
    ) -> Result<ReadAtResponse<Vec<u8>>, SpaceDriverError> {
        let space = self.id;
        let cipher = self.cipher();
        let since = self.client.store().watermark(space, &range).await?;
        let response = self
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
                entries.iter().map(|entry| entry.ver())
            })
            .max()
            .unwrap_or(Ver(0));
        self.client
            .store()
            .advance_watermark(space, &range, response.at, ver_seen)
            .await?;
        self.client
            .run_blocking(move || {
                let ranges = response
                    .ranges
                    .into_iter()
                    .map(|cut| match cut {
                        RangeCut::Snapshot(entries) => entries
                            .iter()
                            .map(|entry| cipher.open_admitted_entry(entry))
                            .collect::<Result<Vec<_>, _>>()
                            .map(RangeCut::Snapshot),
                        RangeCut::Delta(entries) => entries
                            .iter()
                            .map(|entry| cipher.open_admitted_entry(entry))
                            .collect::<Result<Vec<_>, _>>()
                            .map(RangeCut::Delta),
                    })
                    .collect::<Result<Vec<_>, CipherError>>()?;
                Ok::<_, CipherError>(ReadAtResponse {
                    at: response.at,
                    ranges,
                })
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
                    requested_at: send,
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
                    held.lease.requested_at = send;
                    held.lease.granted_at = grant.granted_at;
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

    async fn reject_release_if_checked_assertions_lose_coverage(
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
        let now = self.client.clock().stamp();
        for (seq, record) in space_state.active_oplog() {
            if record.submit_mode() != Some(SubmitMode::Checked) {
                continue;
            }
            for assertion in record.range_asserts() {
                let mut released_guard = None;
                for held in &releasing {
                    if assertion.prefix.starts_with(&held.lease.prefix)
                        && self.lease_usable_for_held(held, &now).await?
                    {
                        released_guard = Some(held.lease.id);
                        break;
                    }
                }
                let Some(lease) = released_guard else {
                    continue;
                };
                let mut replacement = false;
                for held in space_state.leases.values() {
                    if ids.contains(&held.lease.id)
                        || !assertion.prefix.starts_with(&held.lease.prefix)
                    {
                        continue;
                    }
                    if self.lease_usable_for_held(held, &now).await? {
                        replacement = true;
                        break;
                    }
                }
                if !replacement {
                    return Err(SpaceDriverError::ReleaseBlocked { lease, at: *seq });
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
        !held.forgotten && self.lease_live(held, now) && barrier_satisfied(held.barrier, watermark)
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
                if held.forgotten
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

impl<M: MetaStore, H: ServerHandle, C: HybridClock, N: NonceSource + Send + 'static>
    Admits<'_, '_, M, H, C, N>
{
    pub async fn cursors(&self) -> Result<AdmitCursors, SpaceDriverError> {
        let _permit = self.space.enter().await?;
        Ok(self
            .space
            .client
            .store()
            .admit_cursors(self.space.id)
            .await?)
    }

    /// Return all retained unapplied batches in server admission order,
    /// authenticating and opening their operations for application use.
    pub async fn iter_from_neck(&self) -> Result<Vec<AdmittedBatch<Vec<u8>>>, SpaceDriverError> {
        let _permit = self.space.enter().await?;
        let cursors = self
            .space
            .client
            .store()
            .admit_cursors(self.space.id)
            .await?;
        if cursors.neck == cursors.tail {
            return Ok(Vec::new());
        }
        let through = AdmissionSeq(cursors.tail.0.checked_sub(1).ok_or_else(|| {
            SpaceDriverError::MalformedResponse {
                reason: "local admit tail cannot be zero".into(),
            }
        })?);
        let batches = self
            .space
            .client
            .store()
            .admitted_batches(self.space.id, cursors.neck, through)
            .await?;
        let cipher = self.space.cipher();
        self.space
            .client
            .run_blocking(move || {
                batches
                    .into_iter()
                    .map(|batch| {
                        let entries = batch
                            .entries
                            .iter()
                            .map(|entry| cipher.open_admitted_entry(entry))
                            .collect::<Result<Vec<_>, CipherError>>()?;
                        Ok(AdmittedBatch {
                            admission_seq: batch.admission_seq,
                            device: batch.device,
                            device_seq: batch.device_seq,
                            checksum: batch.checksum,
                            entries,
                        })
                    })
                    .collect::<Result<Vec<_>, CipherError>>()
            })
            .await
            .map_err(coordination_unavailable)?
            .map_err(Into::into)
    }

    /// Acknowledge application of the dense interval up to exclusive `to`.
    pub async fn mark_applied(&self, to: AdmissionSeq) -> Result<(), SpaceDriverError> {
        let _permit = self.space.enter().await?;
        self.space
            .client
            .store()
            .mark_admits_applied(self.space.id, to)
            .await?;
        Ok(())
    }

    /// Reclaim retained admitted batches below exclusive `to`.
    pub async fn trim(&self, to: AdmissionSeq) -> Result<(), SpaceDriverError> {
        let _permit = self.space.enter().await?;
        self.space
            .client
            .store()
            .trim_admits(self.space.id, to)
            .await?;
        Ok(())
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
        if !held.forgotten
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
        .filter(|held| !held.forgotten && held.barrier.is_some())
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

fn open_range_cut(cipher: &SpaceCipher, cut: RangeCut) -> Result<RangeCut<Vec<u8>>, CipherError> {
    match cut {
        RangeCut::Snapshot(entries) => entries
            .iter()
            .map(|entry| cipher.open_admitted_entry(entry))
            .collect::<Result<Vec<_>, _>>()
            .map(RangeCut::Snapshot),
        RangeCut::Delta(entries) => entries
            .iter()
            .map(|entry| cipher.open_admitted_entry(entry))
            .collect::<Result<Vec<_>, _>>()
            .map(RangeCut::Delta),
    }
}

fn coordination_unavailable(error: crate::coordination::CoordinationError) -> SpaceDriverError {
    SpaceDriverError::Unavailable {
        reason: error.to_string(),
    }
}
