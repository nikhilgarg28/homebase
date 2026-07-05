//! One device's session with one space: the client-side verb discipline.
//!
//! [`Session`] owns everything the kernel holds a device accountable for:
//!
//! - **Lease bookkeeping** — grants are wrapped in [`HeldLease`]s with
//!   local deadlines (asymmetric expiry), renewed via [`heartbeat`]
//!   (mechanism, not policy: the caller cranks it; nothing here spawns or
//!   sleeps — the DST contract), and dropped the moment the server refuses
//!   them.
//! - **Write coverage** — [`put`] refuses locally, before anything crosses
//!   the wire, unless every key is under a held, locally-live *write*
//!   lease. This strictness is the point: a session that respects its
//!   local deadlines never has a write admitted after a re-grant, which is
//!   what keeps epochs a backstop instead of the first line of defense.
//! - **`device_seq` discipline** — one strictly-increasing sequence per
//!   device, and the retry contract built on it (see [`Space`] docs): an
//!   `Unavailable` reply says nothing about admission, so [`put`] resends
//!   the *identical* batch, and a [`DeviceSeqRegression`] on such a retry
//!   proves the earlier attempt landed — reported as
//!   [`PutOutcome::AlreadyApplied`], never re-applied.
//!
//! # The sole-writer contract
//!
//! A `DeviceId` is one logical write stream. At most one live `Session`
//! may write for a device at a time, and a session resumed after a crash
//! must be constructed ([`Session::resume`]) with a `device_seq` strictly
//! above anything the previous incarnation *may have sent* — persist the
//! intent before sending (gaps are legal; reuse is not). Both
//! `AlreadyApplied` detection and seq resynchronization lean on this.
//!
//! [`heartbeat`]: Session::heartbeat
//! [`put`]: Session::put
//! [`DeviceSeqRegression`]: KernelError::DeviceSeqRegression

use crate::lease::HeldLease;
use homebase_core::clock::Clock;
use homebase_core::key::Key;
use homebase_core::lease::{LeaseId, LeaseMode};
use homebase_core::messages::{
    AcquireRequest, GetRequest, GetResponse, KernelError, LeaseSpec, ListRequest, ListResponse,
    PrefixCursor, PutBatchRequest, PutEntry, ReadAtRequest, ReadAtResponse, ReleaseRequest,
    RenewRequest,
};
use homebase_core::space::{Space, SpaceError};
use homebase_core::tag::{AdmissionSeq, DeviceId, DeviceSeq};
use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

/// How many times a verb is *re*-sent after an `Unavailable` reply before
/// the error surfaces. Semantic (kernel) rejections never retry.
pub const DEFAULT_RETRY_BUDGET: u32 = 3;

/// A successful batch acquire, leases parallel to the requested specs.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Acquired {
    /// Ids of the granted leases, now tracked by the session.
    pub leases: Vec<LeaseId>,
    /// The acquire barrier: catch every acquired prefix up to this
    /// admission point (`read_at`) before trusting local state — lease +
    /// barrier = serializability, not just mutual exclusion.
    pub barrier: AdmissionSeq,
}

/// How a [`Session::put`] resolved.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PutOutcome {
    /// Admitted by this call, at this admission point.
    Admitted(AdmissionSeq),
    /// A retry hit the device_seq replay fence: an earlier attempt of this
    /// same batch was admitted and only its ack was lost. Applied exactly
    /// once; the admission point is unknown.
    AlreadyApplied,
}

/// Why a [`Session::put`] failed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PutError {
    /// Local refusal: no held, locally-live **write** lease covers `key`.
    /// Nothing was sent. Read leases never authorize writes, and a hold
    /// past its local deadline no longer counts (heartbeat may resurrect
    /// it).
    NotCovered { key: Key },
    /// The space rejected or failed the batch. `Unavailable` here means the
    /// retry budget ran out with the outcome *unknown* — the batch may have
    /// been admitted; the seq was not advanced, so the ambiguity resolves
    /// on the next put (admitted-before shows up as a
    /// [`KernelError::DeviceSeqRegression`], after which the session has
    /// already resynchronized).
    Space(SpaceError),
}

impl fmt::Display for PutError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotCovered { key } => {
                write!(f, "no held live write lease covers {key:?}; nothing was sent")
            }
            Self::Space(err) => write!(f, "{err}"),
        }
    }
}

impl std::error::Error for PutError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::NotCovered { .. } => None,
            Self::Space(err) => Some(err),
        }
    }
}

impl From<SpaceError> for PutError {
    fn from(err: SpaceError) -> Self {
        Self::Space(err)
    }
}

/// What one [`Session::heartbeat`] accomplished.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct HeartbeatReport {
    /// Renewed: local deadlines re-armed from this heartbeat's send time.
    pub renewed: Vec<LeaseId>,
    /// Subset of `renewed` another device is waiting on (demand-driven
    /// stickiness: release once past min-hold and convenient).
    pub contended: Vec<LeaseId>,
    /// No longer live on the server (expired, released, or stolen);
    /// dropped from the session.
    pub invalid: Vec<LeaseId>,
}

/// One device's connection to one space, over any [`Space`] transport —
/// the in-process actor handle today, the wire client later, unchanged.
///
/// Runtime-agnostic by construction: no spawning, no sleeping, no wall
/// clock. Time comes from the injected [`Clock`] (sampled at request send —
/// the client half of asymmetric expiry), renewal happens when the caller
/// cranks [`heartbeat`](Session::heartbeat), and a policy loop (cadence,
/// auto-release) layers above, in the runtime that owns the session.
pub struct Session<S, C> {
    space: S,
    clock: Arc<C>,
    device: DeviceId,
    next_seq: u64,
    held: BTreeMap<LeaseId, HeldLease>,
    retry_budget: u32,
}

impl<S: Space, C: Clock> Session<S, C> {
    /// A fresh session for a device that has never written to this space.
    pub fn new(space: S, clock: Arc<C>, device: DeviceId) -> Self {
        Self::resume(space, clock, device, DeviceSeq(1))
    }

    /// Resumes a device's write stream. `next_seq` must be strictly above
    /// anything a previous incarnation may have sent (the sole-writer
    /// contract, module docs) — persist the intent before sending, resume
    /// past it. Leases are not resumed: grants a dead incarnation held
    /// simply expire by TTL; re-acquire.
    pub fn resume(space: S, clock: Arc<C>, device: DeviceId, next_seq: DeviceSeq) -> Self {
        Self {
            space,
            clock,
            device,
            next_seq: next_seq.0,
            held: BTreeMap::new(),
            retry_budget: DEFAULT_RETRY_BUDGET,
        }
    }

    /// Overrides [`DEFAULT_RETRY_BUDGET`]. Zero disables blind retries
    /// entirely (every `Unavailable` surfaces, and
    /// [`PutOutcome::AlreadyApplied`] becomes unreachable).
    pub fn with_retry_budget(mut self, budget: u32) -> Self {
        self.retry_budget = budget;
        self
    }

    pub fn device(&self) -> DeviceId {
        self.device
    }

    /// The seq the next `put` will carry. This is what an incarnation
    /// persists (before sending) so a successor can [`resume`](Self::resume)
    /// safely.
    pub fn next_seq(&self) -> DeviceSeq {
        DeviceSeq(self.next_seq)
    }

    /// All held leases, live or locally expired, in id order.
    pub fn held(&self) -> impl Iterator<Item = &HeldLease> {
        self.held.values()
    }

    /// A held lease by id — `None` once the server has refused it (via
    /// heartbeat or a rejected put) or it was released.
    pub fn lease(&self, id: LeaseId) -> Option<&HeldLease> {
        self.held.get(&id)
    }

    /// Batch lease acquisition (all-or-nothing, like the verb). Granted
    /// leases enter the session's held table with local deadlines counted
    /// from this request's send time.
    ///
    /// Retrying a lost acquire is safe but not free: if the grant was
    /// admitted and only the ack lost, the retry contends against our own
    /// orphan until its TTL expires — surfaced as `Contended`, an
    /// availability delay, never a correctness problem.
    pub async fn acquire(
        &mut self,
        specs: Vec<LeaseSpec>,
        steal: bool,
    ) -> Result<Acquired, SpaceError> {
        let req = AcquireRequest { device: self.device, specs, steal };
        let mut retries = self.retry_budget;
        loop {
            let sent_at = self.clock.now();
            match self.space.acquire(req.clone()).await {
                Ok(resp) => {
                    let leases = resp.leases.iter().map(|l| l.id).collect();
                    for lease in resp.leases {
                        self.held.insert(lease.id, HeldLease::grant(lease, sent_at));
                    }
                    return Ok(Acquired { leases, barrier: resp.barrier });
                }
                Err(SpaceError::Unavailable { .. }) if retries > 0 => retries -= 1,
                Err(err) => return Err(err),
            }
        }
    }

    /// Renews every held lease in one batch — the renewal *mechanism*; the
    /// caller decides the cadence. Locally-expired holds are renewed too:
    /// if the server still holds them live (its window closes later),
    /// they're safely resurrected with a fresh local deadline. Whatever the
    /// server refuses is dropped and reported.
    ///
    /// Safe to retry blindly: renewal only ever extends, and the deadlines
    /// re-arm from the send time of the attempt that succeeded.
    pub async fn heartbeat(&mut self) -> Result<HeartbeatReport, SpaceError> {
        let ids: Vec<LeaseId> = self.held.keys().copied().collect();
        if ids.is_empty() {
            return Ok(HeartbeatReport::default());
        }
        let req = RenewRequest { device: self.device, leases: ids };
        let mut retries = self.retry_budget;
        loop {
            let sent_at = self.clock.now();
            match self.space.renew(req.clone()).await {
                Ok(resp) => {
                    let mut report = HeartbeatReport::default();
                    for grant in &resp.granted {
                        let Some(held) = self.held.get_mut(&grant.id) else { continue };
                        held.renewed(sent_at, grant);
                        report.renewed.push(grant.id);
                        if grant.contended {
                            report.contended.push(grant.id);
                        }
                    }
                    for id in &resp.invalid {
                        self.held.remove(id);
                    }
                    report.invalid = resp.invalid;
                    return Ok(report);
                }
                Err(SpaceError::Unavailable { .. }) if retries > 0 => retries -= 1,
                Err(err) => return Err(err),
            }
        }
    }

    /// Voluntary release. The holds are dropped locally up front — whatever
    /// the wire does, this session stops using them now — so a wire failure
    /// costs only availability (the server side expires by TTL), never
    /// correctness. The verb is idempotent, hence retried blindly.
    pub async fn release(&mut self, leases: &[LeaseId]) -> Result<(), SpaceError> {
        for id in leases {
            self.held.remove(id);
        }
        let req = ReleaseRequest { device: self.device, leases: leases.to_vec() };
        let mut retries = self.retry_budget;
        loop {
            match self.space.release(req.clone()).await {
                Ok(_) => return Ok(()),
                Err(SpaceError::Unavailable { .. }) if retries > 0 => retries -= 1,
                Err(err) => return Err(err),
            }
        }
    }

    /// Atomic write batch under the session's held leases.
    ///
    /// Coverage is checked locally first: every key must sit under a held,
    /// locally-live write lease, or the whole batch is refused with
    /// [`PutError::NotCovered`] before anything is sent. The covering
    /// refs and the device seq are then fixed for the lifetime of the call:
    /// retries after `Unavailable` resend the *identical* batch, which is
    /// what makes the replay fence's verdict meaningful (see
    /// [`PutOutcome::AlreadyApplied`]).
    ///
    /// A lease the server refuses (`LeaseInvalid` — e.g. it expired in
    /// flight — or `Fenced`) is dropped from the session before the error
    /// surfaces.
    pub async fn put(&mut self, entries: Vec<PutEntry>) -> Result<PutOutcome, PutError> {
        let now = self.clock.now();
        let mut cover = BTreeMap::new();
        for entry in &entries {
            let covering = self.held.values().find(|h| {
                h.mode() == LeaseMode::Write && h.is_live(now) && h.lease().covers(&entry.key)
            });
            match covering.map(|h| (h.id(), h.lease_ref(now).expect("checked live"))) {
                Some((id, lease_ref)) => {
                    cover.insert(id, lease_ref);
                }
                None => return Err(PutError::NotCovered { key: entry.key.clone() }),
            }
        }

        let seq = DeviceSeq(self.next_seq);
        let req = PutBatchRequest {
            device: self.device,
            device_seq: seq,
            leases: cover.into_values().collect(),
            entries,
        };

        let mut retries = self.retry_budget;
        // Whether an attempt of THIS batch may have been admitted without
        // an ack (its reply was lost to an Unavailable).
        let mut maybe_admitted = false;
        loop {
            match self.space.put_batch(req.clone()).await {
                Ok(resp) => {
                    self.next_seq = seq.0 + 1;
                    return Ok(PutOutcome::Admitted(resp.admission_seq));
                }
                Err(SpaceError::Unavailable { .. }) if retries > 0 => {
                    retries -= 1;
                    maybe_admitted = true;
                }
                Err(SpaceError::Kernel(KernelError::DeviceSeqRegression {
                    current,
                    attempted,
                })) => {
                    // The server's high water is truth either way.
                    self.next_seq = current.0 + 1;
                    if maybe_admitted {
                        // The replay fence caught our own earlier attempt:
                        // the batch is in. Sound under the sole-writer
                        // contract — nobody else advances this device's seq.
                        return Ok(PutOutcome::AlreadyApplied);
                    }
                    // A fresh regression is a real desync (a lost
                    // incarnation's batch landed unacked). Surface it; the
                    // resync above makes the *next* put well-formed.
                    return Err(PutError::Space(SpaceError::Kernel(
                        KernelError::DeviceSeqRegression { current, attempted },
                    )));
                }
                Err(err) => {
                    if let SpaceError::Kernel(
                        KernelError::LeaseInvalid { lease } | KernelError::Fenced { lease },
                    ) = &err
                    {
                        self.held.remove(lease);
                    }
                    return Err(PutError::Space(err));
                }
            }
        }
    }

    /// Batched point reads. Reads carry no session state and may be
    /// retried blindly (per the [`Space`] contract).
    pub async fn get(&self, keys: Vec<Key>) -> Result<GetResponse, SpaceError> {
        let req = GetRequest { keys };
        let mut retries = self.retry_budget;
        loop {
            match self.space.get(req.clone()).await {
                Ok(resp) => return Ok(resp),
                Err(SpaceError::Unavailable { .. }) if retries > 0 => retries -= 1,
                Err(err) => return Err(err),
            }
        }
    }

    /// Ordered prefix scan, blindly retried like [`get`](Self::get).
    pub async fn list(&self, req: ListRequest) -> Result<ListResponse, SpaceError> {
        let mut retries = self.retry_budget;
        loop {
            match self.space.list(req.clone()).await {
                Ok(resp) => return Ok(resp),
                Err(SpaceError::Unavailable { .. }) if retries > 0 => retries -= 1,
                Err(err) => return Err(err),
            }
        }
    }

    /// Atomic consistent cut over `ranges`, blindly retried like
    /// [`get`](Self::get). Cursors are the caller's state; batch 1 carries
    /// no cursor bookkeeping (that's the shape layer's job, later).
    pub async fn read_at(&self, ranges: Vec<PrefixCursor>) -> Result<ReadAtResponse, SpaceError> {
        let req = ReadAtRequest { ranges };
        let mut retries = self.retry_budget;
        loop {
            match self.space.read_at(req.clone()).await {
                Ok(resp) => return Ok(resp),
                Err(SpaceError::Unavailable { .. }) if retries > 0 => retries -= 1,
                Err(err) => return Err(err),
            }
        }
    }
}
