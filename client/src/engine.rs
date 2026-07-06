//! The engine — the driver over both contracts: one [`MetaStore`] (durable
//! truth), one [`ServerHandle`] (the seven verbs), one injected
//! [`HybridClock`] (the DST discipline: the engine never reads an
//! ambient clock).
//!
//! # No mirror, single driver
//!
//! [`Engine::open`] loads durable state once — to certify it (the audit
//! posture) and to adopt the constant-shape facts: the device identity
//! and the seq counter. Everything else — leases, range watermarks, the queue —
//! stays in the store and is **read on demand** through the [`MetaStore`]
//! point reads: local-disk cheap, and the store is free to buffer. The
//! engine holds no copy of any collection, so there is nothing to drift —
//! just a handful of scalars: the identity, the seq-counter shadow, the
//! queue-scan bound.
//!
//! Methods take `&mut self`: one engine drives one store, transitions are
//! serialized by the borrow checker itself. If a storage transition
//! faults, drop the engine and reopen — the store is the truth.
//!
//! # Leases: hybrid stamps, a margin, and a poison tripwire
//!
//! Every lease-bearing request stamps `send = clock.stamp()` *before*
//! the wire; a grant's deadline is `send + granted TTL` — a
//! [`HybridTimestamp`]: wall time, monotonic time, and the lineage of
//! the monotonic ruler. Expiry uses each ruler where it is trustworthy
//! ([`HybridTimestamp::expired`]): the incarnation that stamped a lease
//! judges it by its own monotonic clock — precise, step-immune, **no
//! margin**, the full window — with the wall alongside for the one case
//! monotonic cannot see (suspend); a *successor* incarnation falls back
//! to the wall reading shaved by a margin of **0.1% of the lease's
//! TTL** ([`lease_margin`]). The margin scales with the TTL because the
//! grant itself bounds the error: a granted lease proves a server round
//! trip at send time, so whatever wall drift a successor inherits
//! accrued over at most one TTL — and 0.1% (1000ppm) is still ~20×
//! real oscillator drift. The server counts TTL from receipt on its own
//! clock, so the local window expires strictly earlier either way;
//! epochs remain the correctness backstop for writes.
//!
//! The wall fallback is what keeps a *restarted* client's offline
//! authority: a process that dies and returns five minutes into a
//! day-long lease still holds it, no round trip required. The wall
//! clock's one lie is the backward step. In-process,
//! [`WallClock`](homebase_core::clock::WallClock) refuses to follow one
//! (it self-checks against a monotonic ruler) — and in-lineage expiry
//! doesn't consult it anyway. Across death, the store's clock
//! high-water is the tripwire: an [`open`](Engine::open) that reads a
//! wall clock *behind* the recorded high-water zero-stamps every stored
//! lease — structurally dead until renewed — and re-anchors the
//! high-water on the new timeline. Renewal always cures: stamps written
//! after a backward step end *earlier* than the server's window, the
//! conservative direction.
//!
//! There is deliberately no background heartbeat — renewal is a decision
//! the caller makes. [`acquire`](Engine::acquire) is idempotent: a spec
//! already covered by a live held lease is satisfied without the wire —
//! only the genuinely missing specs go to the server. "Ensure I hold
//! this" is the question call sites actually ask, so that is what the
//! verb answers.
//!
//! # The pusher
//!
//! [`push`](Engine::push) drains the queue FIFO, reading head windows
//! from the store as it goes. Consecutive commits to the same space merge
//! into one wire batch (up to [`with_push_cap`](Engine::with_push_cap)
//! entries) and ship under the group's **last** seq — the earlier seqs
//! are skipped, which the kernel permits (strictly increasing, gaps
//! legal). Grouping is a wire-time choice, never durable state; recovery
//! reconstructs everything it needs from the kernel's own rejections:
//!
//! - **Seq collision** (`DeviceSeqRegression` naming a seq that is still
//!   in our queue): the admitted set is always a *prefix* of the queue —
//!   pushes are FIFO, groups are contiguous, admission is atomic — so
//!   `current` *is* the admitted extent: trim through it and continue.
//!   This is exactly-once for free: a dead incarnation's send is
//!   discovered, never replayed.
//! - **Fork** (`current` at or past the mint counter, or naming a seq
//!   not in the queue): the server admitted something this store never
//!   minted or never sent — proof another store is writing under our
//!   device id (a file copy come alive). Fatal for now:
//!   [`EngineError::Fork`]. (Re-mint-and-requeue recovery is a later
//!   batch; the ver chains are the deeper tripwire for the forks this
//!   check cannot see.)
//! - **Any other kernel rejection of a merged group**: the group hides
//!   the culprit, so degrade to shipping the solo head and walk forward —
//!   healthy commits admit one by one until the faulty one stands alone.
//! - **A kernel rejection of a solo commit** convicts it:
//!   [`PushOutcome::Stalled`] names the seq and the error — the outcome
//!   *is* the record; re-pushing re-derives the same verdict — and the
//!   queue holds. A `VerRegression`
//!   means the commit itself is bad (written blind past a foreign write —
//!   the missing-pull mistake); the resolution is
//!   [`discard_from`](Engine::discard_from), the rollback. Lease-plane
//!   rejections (`NotCovered`, `LeaseInvalid`, `Fenced`) mean coverage
//!   lapsed; the resolution is re-acquire/renew and push again — the
//!   commit is innocent.
//!
//! Transport failures ([`EngineError::Unavailable`]) abort the pass with
//! the queue intact: retry when the server is back.

use crate::meta::{Committed, HeldLease, MetaStore, certify};
use crate::server::ServerHandle;
use homebase_core::clock::{HybridClock, HybridTimestamp};
use homebase_core::key::Key;
use homebase_core::lease::{Lease, LeaseId, LeaseMode, LeaseRef};
use homebase_core::messages::{
    AcquireRequest, KernelError, LeaseSpec, PutBatchRequest, Range, RangeCursor, RangeCut,
    ReadAtRequest, ReadAtResponse, ReleaseRequest, RenewRequest, RenewResponse,
};
use homebase_core::space::{SpaceError, SpaceId};
use homebase_core::storage::StorageError;
use homebase_core::tag::{AdmissionSeq, DeviceId, DeviceSeq, Value, Ver};
use std::fmt;
use std::time::Duration;

/// Default cap on entries per wire batch — the grouping limit, not a
/// correctness bound (a single oversized commit still ships alone).
pub const DEFAULT_PUSH_CAP: usize = 256;

/// The safety margin for the cross-incarnation wall fallback: 0.1% of
/// the lease's TTL. Proportional on purpose — the grant proves a
/// server round trip at send time, so the wall error a successor
/// inherits accrued over at most one TTL, and 0.1% (1000ppm) is still
/// ~20× real oscillator drift (~50ppm). Never applied in-lineage: the
/// stamping process judges its own leases by the precise monotonic
/// ruler.
pub fn lease_margin(ttl: Duration) -> Duration {
    ttl / 1_000
}

/// What the engine can fail with. Kernel rejections *inside a push* are
/// not errors — they are the protocol talking, and surface as
/// [`PushOutcome`]; this enum is for the faults that end a call.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EngineError {
    /// The meta store faulted mid-transition. The store remains the
    /// truth; drop the engine and reopen.
    Storage(StorageError),
    /// Transport-plane failure: nothing about the request was judged.
    /// Retry when the server is reachable.
    Unavailable { reason: String },
    /// The server rejected a non-push verb (acquire contention, above
    /// all). The request was judged and refused; the caller decides.
    Rejected(KernelError),
    /// The server has admitted a seq this store never minted or never
    /// sent: another store is writing under our device id — a file copy
    /// come alive. Fatal until fork recovery (re-mint, resync, requeue)
    /// lands in a later batch.
    Fork { admitted: DeviceSeq },
    /// The client refused to enqueue a local write because no usable
    /// local write lease covered the key. Usable means live, not
    /// retiring, and past its acquire barrier in the lease-prefix range.
    LocalAuthority { key: Key },
    /// Releasing this lease would strand queued local writes that still
    /// need to be pushed under it.
    ReleaseBlocked { lease: LeaseId, at: DeviceSeq },
}

impl From<StorageError> for EngineError {
    fn from(err: StorageError) -> Self {
        Self::Storage(err)
    }
}

impl From<SpaceError> for EngineError {
    fn from(err: SpaceError) -> Self {
        match err {
            SpaceError::Kernel(err) => Self::Rejected(err),
            SpaceError::Unavailable { reason } => Self::Unavailable { reason },
        }
    }
}

impl fmt::Display for EngineError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Storage(err) => write!(f, "{err}"),
            Self::Unavailable { reason } => write!(f, "server unavailable: {reason}"),
            Self::Rejected(err) => write!(f, "server rejected: {err}"),
            Self::Fork { admitted } => write!(
                f,
                "device fork: the server admitted {admitted:?}, which this store never sent"
            ),
            Self::LocalAuthority { key } => write!(f, "no local write authority for {key:?}"),
            Self::ReleaseBlocked { lease, at } => {
                write!(f, "lease {lease:?} still covers queued write {at:?}")
            }
        }
    }
}

impl std::error::Error for EngineError {}

/// What [`Engine::acquire`] hands back: the leases satisfying each spec,
/// parallel to the request.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Acquired {
    /// One lease per spec, in spec order — freshly granted or already
    /// held.
    pub leases: Vec<Lease>,
    /// Present iff the server granted anything new: the catch-up
    /// obligation — [`pull`](Engine::pull) the acquired prefixes to at
    /// least this point before trusting local state. `None` means every
    /// spec was satisfied by leases held continuously since their own
    /// grants, so no new obligation exists.
    pub barrier: Option<AdmissionSeq>,
}

/// How a push pass ended. `acked_through` is the highest seq *this call*
/// confirmed admitted (and trimmed) — `None` when the pass made no
/// progress.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PushOutcome {
    /// The queue is empty; everything committed has been admitted.
    Drained { acked_through: Option<DeviceSeq> },
    /// The queue head was convicted by a solo rejection and the pass
    /// stopped. See the module docs for the two resolutions: rollback
    /// ([`Engine::discard_from`]) for a faulty commit, re-acquire and
    /// push again for lapsed coverage.
    Stalled {
        at: DeviceSeq,
        error: KernelError,
        acked_through: Option<DeviceSeq>,
    },
}

/// A held lease as the engine reads it: the durable record joined with
/// its liveness verdict at read time ([`HybridTimestamp::expired`]). A
/// zero-stamped record (a poisoned open's leftovers) is never live.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LeaseState {
    pub held: HeldLease,
    pub live: bool,
}

/// The driver. See the module docs for the doctrine; the type parameters
/// are the three injected worlds: durable truth, the server, time.
pub struct Engine<M, H, C> {
    store: M,
    server: H,
    clock: C,
    device: DeviceId,
    /// The seq the next commit gets — the fork discriminant. A scalar
    /// shadow of the store's counter, maintained from commit returns.
    next_seq: DeviceSeq,
    /// A lower bound on the queue head: nothing is queued below it. The
    /// pusher walks the queue in `[scan_from, next_seq)` windows;
    /// incarnation-local, tightened by every ack.
    scan_from: DeviceSeq,
    push_cap: usize,
}

impl<M: MetaStore, H: ServerHandle, C: HybridClock> Engine<M, H, C> {
    /// Load, certify, and adopt (or mint) identity. `fresh` is used only
    /// when the store has no device record yet — the caller supplies the
    /// randomness (real callers mint a UUID, the sim derives one from its
    /// seed; the engine itself never touches an entropy source).
    ///
    /// Panics if the loaded state fails [`certify`] — the audit posture:
    /// corrupted truth is a stop, not an error to route around.
    pub async fn open(store: M, server: H, clock: C, fresh: DeviceId) -> Result<Self, EngineError> {
        let state = store.load().await?;
        certify(&state);
        let device = match state.device {
            Some(id) => id,
            None => {
                store.record_device(fresh).await?;
                fresh
            }
        };

        // The poison tripwire: a wall clock behind the recorded
        // high-water regressed while we were dead, so every stored
        // stamp is suspect — kill them structurally (renewals
        // re-stamp), then re-anchor the high-water on whatever timeline
        // we woke up on. Crash-safe in this order: an interrupted
        // cleanse re-detects.
        let now = clock.stamp();
        if state.clock_high.is_some_and(|high| now.wall < high) {
            for (space, space_state) in &state.spaces {
                let dead: Vec<HeldLease> = space_state
                    .leases
                    .values()
                    .map(|held| HeldLease {
                        lease: held.lease.clone(),
                        deadline: HybridTimestamp::ZERO,
                        barrier: held.barrier,
                        retiring: held.retiring,
                    })
                    .collect();
                if !dead.is_empty() {
                    store.record_leases(*space, &dead).await?;
                }
            }
        }
        store.record_clock(now.wall).await?;

        let next_seq = state.next_seq.unwrap_or(DeviceSeq(1));
        let scan_from = state.oplog.keys().next().copied().unwrap_or(next_seq);
        Ok(Self {
            store,
            server,
            clock,
            device,
            next_seq,
            scan_from,
            push_cap: DEFAULT_PUSH_CAP,
        })
    }

    /// Replace the grouping cap (entries per wire batch).
    pub fn with_push_cap(mut self, cap: usize) -> Self {
        assert!(cap > 0, "a zero cap would ship nothing");
        self.push_cap = cap;
        self
    }

    pub fn device(&self) -> DeviceId {
        self.device
    }

    /// Whether a held lease may be presented right now — the hybrid
    /// expiry rule, with the TTL-proportional margin on the wall
    /// fallback. Zero stamps (a poisoned open's leftovers) are never
    /// live.
    fn lease_live(&self, held: &HeldLease, now: &HybridTimestamp) -> bool {
        !held.deadline.expired(now, lease_margin(held.lease.ttl))
    }

    /// The held leases covering `prefixes`, judged against the clock —
    /// what a caller inspects to decide what to renew.
    pub async fn leases(
        &self,
        space: SpaceId,
        prefixes: &[Key],
    ) -> Result<Vec<LeaseState>, EngineError> {
        let now = self.clock.stamp();
        let mut out = Vec::new();
        for held in self.store.leases_covering(space, prefixes).await? {
            let live = self.lease_usable_for_held(space, &held, &now).await?;
            out.push(LeaseState { held, live });
        }
        Ok(out)
    }

    /// A local commit: durable in the store (seq and vers assigned
    /// there), queued for push. Entirely offline.
    pub async fn commit(
        &mut self,
        space: SpaceId,
        entries: Vec<(Key, Value)>,
    ) -> Result<Committed, EngineError> {
        self.check_local_write_authority(space, &entries).await?;
        let committed = self.store.commit(space, entries).await?;
        self.next_seq = DeviceSeq(committed.seq.0 + 1);
        Ok(committed)
    }

    /// Ensure the leases — the idempotent verb call sites actually want.
    /// Three tiers, cheapest first:
    ///
    /// 1. a spec covered by a live held lease (prefix covers, mode
    ///    adequate — a held write satisfies a read spec) is answered
    ///    from the store, no wire;
    /// 2. a spec covered by a held lease that is *not* live (expired,
    ///    inside the margin, or zero-stamped by a poisoned open) is
    ///    revived by renewal — same lease, same fence; the kernel treats
    ///    a same-device re-acquire as contention, so renewal is the only
    ///    correct revival;
    /// 3. only genuinely uncovered specs go to wire acquire (leases the
    ///    renewal reported invalid land here too).
    ///
    /// An empty `specs` never touches the wire.
    pub async fn acquire(
        &mut self,
        space: SpaceId,
        specs: Vec<LeaseSpec>,
        steal: bool,
    ) -> Result<Acquired, EngineError> {
        if specs.is_empty() {
            return Ok(Acquired {
                leases: vec![],
                barrier: None,
            });
        }

        // Tier 2 first: revive coverage that renewal can restore.
        let queried: Vec<Key> = specs.iter().map(|spec| spec.prefix.clone()).collect();
        let now = self.clock.stamp();
        let held = self.store.leases_covering(space, &queried).await?;
        let mut revive: Vec<LeaseId> = Vec::new();
        for spec in &specs {
            if self
                .usable_covering(space, &held, spec, &now)
                .await?
                .is_some()
            {
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

        // Tiers 1 and 3 against refreshed truth.
        let send = self.clock.stamp();
        let held = self.store.leases_covering(space, &queried).await?;
        let mut satisfied: Vec<Option<Lease>> = Vec::with_capacity(specs.len());
        for spec in &specs {
            satisfied.push(
                self.usable_covering(space, &held, spec, &send)
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
                barrier: self
                    .max_pending_barrier(space, &held, &specs, &send)
                    .await?,
            });
        }

        let response = self
            .server
            .acquire(
                &space,
                AcquireRequest {
                    device: self.device,
                    specs: missing,
                    steal,
                },
            )
            .await?;
        let mut fresh = Vec::with_capacity(response.leases.len());
        for lease in &response.leases {
            let watermark = self
                .store
                .watermark(space, &Range::Prefix(lease.prefix.clone()))
                .await?;
            fresh.push(HeldLease {
                lease: lease.clone(),
                deadline: send.saturating_add(lease.ttl),
                barrier: pending_barrier(response.barrier, watermark),
                retiring: false,
            });
        }
        self.store.record_clock(send.wall).await?;
        self.store.record_leases(space, &fresh).await?;

        let mut granted = response.leases.into_iter();
        for slot in &mut satisfied {
            if slot.is_none() {
                *slot = Some(granted.next().expect("grants parallel to missing specs"));
            }
        }
        let existing_barrier = self
            .max_pending_barrier(space, &held, &specs, &send)
            .await?;
        let fresh_barrier = fresh.iter().filter_map(|held| held.barrier).max();
        Ok(Acquired {
            leases: satisfied.into_iter().flatten().collect(),
            barrier: existing_barrier.max(fresh_barrier),
        })
    }

    /// Ensure the requested leases are locally usable before returning.
    ///
    /// This is the ergonomic acquire path: it obtains or renews coverage,
    /// then satisfies any pending acquire barriers by pulling each returned
    /// lease's own prefix. The lower-level [`Engine::acquire`] remains
    /// available for callers that want to schedule catch-up themselves.
    pub async fn ensure(
        &mut self,
        space: SpaceId,
        specs: Vec<LeaseSpec>,
        steal: bool,
    ) -> Result<Acquired, EngineError> {
        let acquired = self.acquire(space, specs, steal).await?;
        if acquired.barrier.is_none() {
            return Ok(acquired);
        }

        let mut pulled = Vec::<Key>::new();
        for lease in &acquired.leases {
            let held = self.held_lease_by_id(space, lease.id).await?;
            if held.barrier.is_some()
                && !self
                    .lease_usable_for_held(space, &held, &self.clock.stamp())
                    .await?
                && !pulled.iter().any(|prefix| prefix == &lease.prefix)
            {
                self.pull(space, Range::Prefix(lease.prefix.clone()))
                    .await?;
                pulled.push(lease.prefix.clone());
            }
        }

        Ok(Acquired {
            leases: acquired.leases,
            barrier: None,
        })
    }

    /// Renew the held leases covering `prefixes` — the explicit act that
    /// confirms them (there is no background heartbeat). Grants get
    /// fresh local deadlines from this call's send time and are written
    /// through; leases the server reports invalid are dropped
    /// everywhere. `contended` flags ride the response for the caller's
    /// release policy. Empty `prefixes` — or prefixes nothing covers —
    /// never touch the wire.
    pub async fn renew(
        &mut self,
        space: SpaceId,
        prefixes: &[Key],
    ) -> Result<RenewResponse, EngineError> {
        let held = if prefixes.is_empty() {
            Vec::new()
        } else {
            self.store.leases_covering(space, prefixes).await?
        };
        let ids: Vec<LeaseId> = held
            .iter()
            .filter(|held| !held.retiring)
            .map(|h| h.lease.id)
            .collect();
        self.renew_ids(space, &ids, &held).await
    }

    /// The renewal engine room: `held` must contain a record for every
    /// id (panics otherwise — the callers just read them).
    async fn renew_ids(
        &mut self,
        space: SpaceId,
        ids: &[LeaseId],
        held: &[HeldLease],
    ) -> Result<RenewResponse, EngineError> {
        if ids.is_empty() {
            return Ok(RenewResponse {
                granted: vec![],
                invalid: vec![],
            });
        }
        let send = self.clock.stamp();
        let response = self
            .server
            .renew(
                &space,
                RenewRequest {
                    device: self.device,
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
            self.store.record_clock(send.wall).await?;
            self.store.record_leases(space, &refreshed).await?;
        }
        if !response.invalid.is_empty() {
            self.store.drop_leases(space, &response.invalid).await?;
        }
        Ok(response)
    }

    /// Release leases: first mark them retiring locally, then tell the
    /// server (idempotent there), then forget. Queued writes covered by a
    /// lease block release; callers should push or discard before releasing.
    /// An empty `ids` never touches the wire.
    pub async fn release(&mut self, space: SpaceId, ids: &[LeaseId]) -> Result<(), EngineError> {
        if ids.is_empty() {
            return Ok(());
        }
        self.reject_release_if_queued_writes(space, ids).await?;
        self.store.retire_leases(space, ids).await?;
        self.server
            .release(
                &space,
                ReleaseRequest {
                    device: self.device,
                    leases: ids.to_vec(),
                },
            )
            .await?;
        self.store.drop_leases(space, ids).await?;
        Ok(())
    }

    async fn reject_release_if_queued_writes(
        &self,
        space: SpaceId,
        ids: &[LeaseId],
    ) -> Result<(), EngineError> {
        let state = self.store.load().await?;
        let Some(space_state) = state.spaces.get(&space) else {
            return Ok(());
        };
        let releasing: Vec<&HeldLease> = ids
            .iter()
            .filter_map(|id| space_state.leases.get(id))
            .collect();
        if releasing.is_empty() {
            return Ok(());
        }
        for (seq, record) in &state.oplog {
            if record.space != space {
                continue;
            }
            for held in &releasing {
                if record
                    .entries
                    .iter()
                    .any(|entry| entry.key.starts_with(&held.lease.prefix))
                {
                    return Err(EngineError::ReleaseBlocked {
                        lease: held.lease.id,
                        at: *seq,
                    });
                }
            }
        }
        Ok(())
    }

    async fn held_lease_by_id(
        &self,
        space: SpaceId,
        id: LeaseId,
    ) -> Result<HeldLease, EngineError> {
        let state = self.store.load().await?;
        Ok(state
            .spaces
            .get(&space)
            .and_then(|space_state| space_state.leases.get(&id))
            .cloned()
            .expect("acquire returned a lease that is not durably held"))
    }

    /// Pull one range since its effective watermark (`None` → snapshot), then
    /// record the returned cut for that exact range. Pulling
    /// [`Range::Prefix`] never advances [`Range::Full`]; descendants only see
    /// ancestor progress through the read-time max in [`MetaStore::watermark`].
    pub async fn pull(
        &mut self,
        space: SpaceId,
        range: Range,
    ) -> Result<ReadAtResponse, EngineError> {
        let since = self.store.watermark(space, &range).await?;
        let response = self
            .server
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
        self.store
            .advance_watermark(space, &range, response.at, ver_seen)
            .await?;
        Ok(response)
    }

    /// Drain the queue. One pass: ships groups FIFO until the queue is
    /// empty ([`PushOutcome::Drained`]), a solo head is convicted
    /// ([`PushOutcome::Stalled`]), the transport drops
    /// ([`EngineError::Unavailable`], queue intact), or a fork is proven
    /// ([`EngineError::Fork`]). See the module docs for the recovery
    /// algebra.
    pub async fn push(&mut self) -> Result<PushOutcome, EngineError> {
        let mut acked = None;
        // After a merged group is rejected, ship solo heads until one is
        // admitted (or convicted) — the adaptive probe.
        let mut probe = false;
        loop {
            // The queue lives in [scan_from, next_seq); walk it one
            // cap-sized seq window at a time. An empty window is a legal
            // gap (a discard's shadow), not the end.
            if self.scan_from >= self.next_seq {
                return Ok(PushOutcome::Drained {
                    acked_through: acked,
                });
            }
            let until = DeviceSeq(
                self.scan_from
                    .0
                    .saturating_add(self.push_cap as u64 - 1)
                    .min(self.next_seq.0 - 1),
            );
            let window = self.store.oplog(self.scan_from, until).await?;
            let Some((head, head_record)) = window.first() else {
                self.scan_from = DeviceSeq(until.0 + 1);
                continue;
            };
            let head = *head;
            self.scan_from = head; // nothing below was queued in the window
            let space = head_record.space;
            let mut last = head;
            let mut entries = head_record.entries.clone();
            if !probe {
                for (seq, record) in &window[1..] {
                    if seq.0 != last.0 + 1
                        || record.space != space
                        || entries.len() + record.entries.len() > self.push_cap
                    {
                        break;
                    }
                    entries.extend(record.entries.iter().cloned());
                    last = *seq;
                }
            }
            let keys: Vec<Key> = entries.iter().map(|entry| entry.key.clone()).collect();
            let request = PutBatchRequest {
                device: self.device,
                device_seq: last,
                leases: self.live_write_leases(space, &keys).await?,
                entries,
            };
            match self.server.put_batch(&space, request).await {
                Ok(_) => {
                    self.ack(last).await?;
                    acked = Some(last);
                    probe = false;
                }
                Err(SpaceError::Kernel(KernelError::DeviceSeqRegression { current, .. })) => {
                    // A legitimate collision names our own earlier send,
                    // and an admitted-but-untrimmed seq is necessarily
                    // still queued. Anything else — a seq we never
                    // minted, or one we minted but never sent — is
                    // another store wearing our identity.
                    let ours = current < self.next_seq
                        && !self.store.oplog(current, current).await?.is_empty();
                    if !ours {
                        return Err(EngineError::Fork { admitted: current });
                    }
                    self.ack(current).await?;
                    acked = Some(current);
                    probe = false;
                }
                Err(SpaceError::Kernel(error)) => {
                    if last > head {
                        probe = true;
                        continue;
                    }
                    return Ok(PushOutcome::Stalled {
                        at: head,
                        error,
                        acked_through: acked,
                    });
                }
                Err(SpaceError::Unavailable { reason }) => {
                    return Err(EngineError::Unavailable { reason });
                }
            }
        }
    }

    /// Rollback: drop every queued commit with seq ≥ `from` — the
    /// resolution for a convicted head the caller chooses not to repair.
    /// Later commits fall with it (they may have read what it wrote).
    /// The seq counter never rewinds, so the queue may carry a gap
    /// afterwards — legal everywhere.
    pub async fn discard_from(&mut self, from: DeviceSeq) -> Result<(), EngineError> {
        self.store.discard_from(from).await?;
        Ok(())
    }

    /// Acknowledged through `through`: trim durably, advance the scan
    /// bound.
    async fn ack(&mut self, through: DeviceSeq) -> Result<(), EngineError> {
        self.store.trim_oplog(through).await?;
        self.scan_from = DeviceSeq(self.scan_from.0.max(through.0 + 1));
        Ok(())
    }

    async fn check_local_write_authority(
        &self,
        space: SpaceId,
        entries: &[(Key, Value)],
    ) -> Result<(), EngineError> {
        if entries.is_empty() {
            return Ok(());
        }
        let keys: Vec<Key> = entries.iter().map(|(key, _)| key.clone()).collect();
        let held = self.store.leases_covering(space, &keys).await?;
        let now = self.clock.stamp();
        for key in keys {
            let mut covered = false;
            for held in &held {
                if held.lease.mode == LeaseMode::Write
                    && key.starts_with(&held.lease.prefix)
                    && self.lease_usable_for_held(space, held, &now).await?
                {
                    covered = true;
                    break;
                }
            }
            if !covered {
                return Err(EngineError::LocalAuthority { key });
            }
        }
        Ok(())
    }

    /// Whether a held lease is usable as local authority right now:
    /// unretired, live under the hybrid expiry rule, and past its
    /// whole-space acquire barrier.
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
        space: SpaceId,
        held: &HeldLease,
        now: &HybridTimestamp,
    ) -> Result<bool, EngineError> {
        let watermark = self
            .store
            .watermark(space, &Range::Prefix(held.lease.prefix.clone()))
            .await?;
        Ok(self.lease_usable(held, now, watermark))
    }

    /// The held lease (if any) that satisfies `spec` right now:
    /// covering, live, unretired, and barrier-satisfied.
    async fn usable_covering<'a>(
        &self,
        space: SpaceId,
        held: &'a [HeldLease],
        spec: &LeaseSpec,
        now: &HybridTimestamp,
    ) -> Result<Option<&'a Lease>, EngineError> {
        for h in held {
            if covers(h, spec) && self.lease_usable_for_held(space, h, now).await? {
                return Ok(Some(&h.lease));
            }
        }
        Ok(None)
    }

    async fn max_pending_barrier(
        &self,
        space: SpaceId,
        held: &[HeldLease],
        specs: &[LeaseSpec],
        now: &HybridTimestamp,
    ) -> Result<Option<AdmissionSeq>, EngineError> {
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
                    .store
                    .watermark(space, &Range::Prefix(held.lease.prefix.clone()))
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

    /// The lease refs a put over `keys` may present *right now*:
    /// covering, write mode, deadline clear of the margin. Presenting
    /// nothing when coverage lapsed is deliberate — the kernel's
    /// `NotCovered` is the signal to re-acquire, and an expired or
    /// margin-dead lease must never back a write (the two-clock rule).
    async fn live_write_leases(
        &self,
        space: SpaceId,
        keys: &[Key],
    ) -> Result<Vec<LeaseRef>, EngineError> {
        let now = self.clock.stamp();
        let mut out = Vec::new();
        for held in self.store.leases_covering(space, keys).await? {
            if held.lease.mode == LeaseMode::Write
                && self.lease_usable_for_held(space, &held, &now).await?
            {
                out.push(LeaseRef {
                    id: held.lease.id,
                    epoch: held.lease.epoch,
                });
            }
        }
        Ok(out)
    }
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

/// Whether a held lease answers a spec: its prefix covers the requested
/// one and its mode is adequate.
fn covers(held: &HeldLease, spec: &LeaseSpec) -> bool {
    spec.prefix.starts_with(&held.lease.prefix) && mode_covers(held.lease.mode, spec.mode)
}

/// Whether a held lease's mode satisfies a requested one: write covers
/// everything it excludes others from; read covers only read.
fn mode_covers(held: LeaseMode, want: LeaseMode) -> bool {
    matches!(
        (held, want),
        (LeaseMode::Write, _) | (LeaseMode::Read, LeaseMode::Read)
    )
}
