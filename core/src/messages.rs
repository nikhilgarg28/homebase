//! Request/response messages for the seven kernel verbs, plus [`KernelError`].
//!
//! These are the canonical, transport-neutral forms. The wire layer (likely
//! tonic/gRPC) defines its own generated DTOs and converts to and from these
//! at the boundary — which is also where prefix-scoped token enforcement
//! lives — so transport details never leak into kernel semantics or
//! validated types like [`Key`].
//!
//! Value bytes are opaque throughout: by the time a [`Value::Present`]
//! reaches these messages it is already ciphertext (the E2EE codec is a
//! client-side layer), and the kernel could not interpret it if it wanted to.
//!
//! The verb contract itself is [`Space`](crate::space::Space). A request
//! executes within exactly one space, selected by the authenticated
//! connection/token and never named in request bodies. Prefix-scoped token
//! enforcement on reads and writes is likewise a wire-layer concern; these
//! types describe kernel semantics for a caller already inside a space.

use crate::key::Key;
use crate::lease::{Lease, LeaseId, LeaseMode, LeaseRef};
use crate::tag::{AdmissionSeq, DeviceId, DeviceSeq, Entry, Value, Ver};
use std::fmt;
use std::time::Duration;

// ---------------------------------------------------------------------------
// acquire

/// One requested lease within a batch acquire.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LeaseSpec {
    pub prefix: Key,
    /// Read shares with read; write excludes everything (see
    /// [`LeaseMode::compatible_with`]).
    pub mode: LeaseMode,
    /// Requested TTL; the grant may be shorter (kernel cap → class default
    /// → app pin).
    pub ttl: Duration,
    /// Opt in to pre-deadline preemption by a later `steal = true` acquire
    /// (see [`crate::lease`] module docs).
    pub stealable: bool,
}

/// Batch lease acquisition. **All-or-nothing**: if any spec conflicts with a
/// live lease under the mode rules, nothing is granted and the first
/// conflict is reported via [`KernelError::Contended`]. Callers (the witness
/// compiler) acquire in sorted order, so batch atomicity cannot deadlock.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AcquireRequest {
    pub device: DeviceId,
    pub specs: Vec<LeaseSpec>,
    /// Preempt stealable blockers pre-deadline. A spec still contends if
    /// *any* of its incompatible live blockers is not stealable; a
    /// successful steal purges the victims, and the fresh epochs fence them.
    pub steal: bool,
}

/// Successful batch grant.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AcquireResponse {
    /// Granted leases, parallel to `specs`.
    pub leases: Vec<Lease>,
    /// The acquire barrier: the admission high-water mark at grant time.
    /// The caller must catch up (`read_at`) every acquired prefix to at
    /// least this point before trusting local state — lease + barrier =
    /// serializability, not just mutual exclusion.
    pub barrier: AdmissionSeq,
}

// ---------------------------------------------------------------------------
// renew / release

/// Heartbeat renewal for a batch of held leases.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RenewRequest {
    pub device: DeviceId,
    pub leases: Vec<LeaseId>,
}

/// Per-lease renewal outcome. Contention piggybacks here: there is no push
/// channel, so `contended` is how a holder learns someone is waiting.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RenewGrant {
    pub id: LeaseId,
    /// Fresh TTL, counted from this request's send time (asymmetric expiry).
    pub ttl: Duration,
    /// Another device wants an overlapping prefix. Demand-driven stickiness:
    /// the holder should release once past its min-hold and convenient;
    /// until the deadline the steal stays denied.
    pub contended: bool,
}

/// Renewal never fails as a batch; each lease renews or is reported invalid.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RenewResponse {
    pub granted: Vec<RenewGrant>,
    /// Leases the server no longer holds live: expired (strict local
    /// expiry), released, or unknown. The holder must stop writing under
    /// these immediately.
    pub invalid: Vec<LeaseId>,
}

/// Voluntary release. Idempotent: releasing an expired or unknown lease is
/// not an error.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReleaseRequest {
    pub device: DeviceId,
    pub leases: Vec<LeaseId>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReleaseResponse {}

// ---------------------------------------------------------------------------
// put_batch

/// One write within a batch. Deletes are explicit: writing
/// [`Value::Absent`] stores a tombstone.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PutEntry {
    pub key: Key,
    pub value: Value,
    /// Must be strictly greater than the stored `Ver` for this key.
    pub ver: Ver,
}

/// One client commit within an atomic put request.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PutBatch {
    /// Client-assigned, strictly increasing per device; one per client
    /// commit. A request may coalesce successive client commits without
    /// erasing their individual seq identity.
    pub device_seq: DeviceSeq,
    pub entries: Vec<PutEntry>,
}

/// Atomic write request (request = transaction; torn requests impossible).
///
/// Admission requires: every entry's key covered by some presented **write**
/// lease whose id and epoch are both live (epoch-fenced), and per-key `Ver`
/// monotonicity. Read leases never authorize writes. Any violation rejects
/// the whole batch.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PutBatchRequest {
    pub device: DeviceId,
    pub leases: Vec<LeaseRef>,
    /// Successive client commits to admit atomically in one server turn.
    pub batches: Vec<PutBatch>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PutBatchResponse {
    /// Admission points assigned to each input batch, in order.
    pub admission_seqs: Vec<AdmissionSeq>,
}

// ---------------------------------------------------------------------------
// get / list

/// Batched point reads.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GetRequest {
    pub keys: Vec<Key>,
}

/// Results parallel to the requested keys. `None` means no live value —
/// never-written and tombstoned are indistinguishable here (tombstones
/// surface only in `read_at` deltas).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GetResponse {
    pub entries: Vec<Option<Entry>>,
}

/// Ordered scan of live entries under a prefix.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ListRequest {
    pub prefix: Key,
    /// Resume point for pagination: strictly-after this key.
    pub start_after: Option<Key>,
    pub limit: Option<usize>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ListResponse {
    /// Live entries in key order (tombstones excluded).
    pub entries: Vec<Entry>,
    /// True when the scan stopped at `limit` with more remaining.
    pub truncated: bool,
}

// ---------------------------------------------------------------------------
// read_at

/// A read range within one space.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Range {
    /// The whole space: every live data key for snapshots, every changed
    /// key for deltas.
    Full,
    /// All keys under this component-wise prefix.
    Prefix(Key),
}

impl Range {
    pub fn covers_key(&self, key: &Key) -> bool {
        match self {
            Self::Full => true,
            Self::Prefix(prefix) => key.starts_with(prefix),
        }
    }

    pub fn covers_range(&self, other: &Range) -> bool {
        match (self, other) {
            (Self::Full, _) => true,
            (Self::Prefix(_), Self::Full) => false,
            (Self::Prefix(a), Self::Prefix(b)) => b.starts_with(a),
        }
    }
}

/// A range with the caller's cursor position. `since: None` requests a full
/// snapshot; `Some` requests the changes since that admission point.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RangeCursor {
    pub range: Range,
    pub since: Option<AdmissionSeq>,
}

/// Atomic consistent cut: all ranges evaluated at one admission point.
///
/// This is also the replication feed that drives shapes — each shape polls
/// its own ranges at its own cursors, and the augmented range-max tree
/// answers "anything new under this prefix since my cursor?" without a
/// scan. Cursors are client-owned state.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReadAtRequest {
    pub ranges: Vec<RangeCursor>,
}

/// Per-range result of a consistent cut: `(S, Δ)` in the design's terms.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RangeCut {
    /// Full state of the range at the cut (cursor was `None`); live entries
    /// only, key order.
    Snapshot(Vec<Entry>),
    /// Changes since the caller's cursor, tombstones included, ascending
    /// `(admission_seq, key)`.
    Delta(Vec<Entry>),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReadAtResponse {
    /// The single admission point at which every range was evaluated —
    /// never a torn multi-range read. Also the caller's next cursor for
    /// all requested ranges.
    pub at: AdmissionSeq,
    /// Parallel to the requested ranges.
    pub ranges: Vec<RangeCut>,
}

// ---------------------------------------------------------------------------
// errors

/// Kernel-level rejections. Each variant is one invariant refusing to bend.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum KernelError {
    /// Acquire denied: a requested lease overlaps a live lease in an
    /// incompatible mode (write conflicts with everything; read conflicts
    /// with write). Steals are denied until the holder's deadline unless
    /// the blocker was granted stealable and the acquire passes `steal`.
    Contended {
        prefix: Key,
        /// Hint: remaining TTL of the blocking lease, if the server chooses
        /// to reveal it. Purely advisory backoff guidance.
        retry_after: Option<Duration>,
    },
    /// The presented lease id is not live: expired (strict local expiry),
    /// released, or never granted.
    LeaseInvalid { lease: LeaseId },
    /// The lease id is live but the presented epoch is stale — a fenced
    /// zombie writer.
    Fenced { lease: LeaseId },
    /// A `put_batch` entry's key is not covered by any presented **write**
    /// lease (read leases guard read sets; they never authorize writes).
    NotCovered { key: Key },
    /// Per-key version monotonicity violated.
    VerRegression {
        key: Key,
        current: Ver,
        attempted: Ver,
    },
    /// Per-device batch sequence monotonicity violated: replay or
    /// out-of-order submission (client-assigned, strictly increasing).
    DeviceSeqRegression {
        current: DeviceSeq,
        attempted: DeviceSeq,
    },
    /// Outside the token's prefix scope (enforced on reads AND writes).
    /// Reserved for the wire layer; the in-process kernel never emits it.
    Unauthorized { prefix: Key },
}

impl fmt::Display for KernelError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Contended {
                prefix,
                retry_after,
            } => match retry_after {
                Some(d) => write!(f, "prefix {prefix:?} is contended (retry in ~{d:?})"),
                None => write!(f, "prefix {prefix:?} is contended"),
            },
            Self::LeaseInvalid { lease } => write!(f, "lease {lease:?} is not live"),
            Self::Fenced { lease } => write!(f, "stale epoch presented for lease {lease:?}"),
            Self::NotCovered { key } => write!(f, "key {key:?} not covered by any presented lease"),
            Self::VerRegression {
                key,
                current,
                attempted,
            } => write!(
                f,
                "ver regression on {key:?}: attempted {attempted:?} ≤ current {current:?}"
            ),
            Self::DeviceSeqRegression { current, attempted } => write!(
                f,
                "device_seq regression: attempted {attempted:?} ≤ current {current:?}"
            ),
            Self::Unauthorized { prefix } => {
                write!(f, "token does not cover prefix {prefix:?}")
            }
        }
    }
}

impl std::error::Error for KernelError {}
