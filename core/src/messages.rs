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

use crate::clock::HybridTimestamp;
use crate::key::Key;
use crate::lease::{Lease, LeaseId, LeaseMode};
use crate::seal::Seal;
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
}

/// Batch lease acquisition. **All-or-nothing**: if any spec conflicts with a
/// live lease under the mode rules, nothing is granted and the first
/// conflict is reported via [`KernelError::Contended`]. Callers (the witness
/// compiler) acquire in sorted order, so batch atomicity cannot deadlock.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AcquireRequest {
    pub device: DeviceId,
    /// Client-minted timestamp from the sending clock domain.
    pub requested_at: HybridTimestamp,
    pub specs: Vec<LeaseSpec>,
}

/// Successful batch grant.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AcquireResponse {
    /// Granted leases, parallel to `specs`.
    pub leases: Vec<Lease>,
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
    /// until release or expiry the waiter stays contended.
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
// list_leases

/// Repair/inspection request for active leases held by one device.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ListLeasesRequest {
    pub device: DeviceId,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ListLeasesResponse {
    pub leases: Vec<Lease>,
}

// ---------------------------------------------------------------------------
// put_batch

/// Equality assertion for the server-visible max admission sequence under a
/// component-wise prefix.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RangeAssert {
    pub prefix: Key,
    pub at: AdmissionSeq,
}

/// One v2 data operation within a client batch.
///
/// Set ciphertext is separate from the AEAD tag stored in [`Seal`]. To avoid
/// leaking empty logical values as empty ciphertexts, clients should encrypt a
/// non-empty Set plaintext frame even when the application value is empty.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BatchOp {
    Set {
        key: Key,
        ver: Ver,
        seal: Seal,
        ciphertext: Vec<u8>,
    },
    Delete {
        key: Key,
        ver: Ver,
        seal: Seal,
    },
    NoOp,
}

/// Legacy client-local write shape. The server wire format uses [`BatchOp`];
/// this remains the bridge for the current client oplog encoding.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PutEntry {
    pub key: Key,
    pub value: Value,
    /// Must be strictly greater than the stored `Ver` for this key.
    pub ver: Ver,
}

impl From<PutEntry> for BatchOp {
    fn from(entry: PutEntry) -> Self {
        match entry.value {
            Value::Present(ciphertext) => Self::Set {
                key: entry.key,
                ver: entry.ver,
                seal: Seal::empty_aead_v1(),
                ciphertext,
            },
            Value::Absent => Self::Delete {
                key: entry.key,
                ver: entry.ver,
                seal: Seal::empty_aead_v1(),
            },
        }
    }
}

/// One client commit within an atomic put request.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PutBatch {
    /// Client-assigned, strictly increasing per device; one per client
    /// commit. A request may coalesce successive client commits without
    /// erasing their individual seq identity.
    pub device_seq: DeviceSeq,
    pub ops: Vec<BatchOp>,
}

/// Atomic write request (request = transaction; torn requests impossible).
///
/// Admission requires: no Set/Delete key overlaps a live foreign lease
/// reservation, every Set/Delete has a valid [`Seal`], and every Set/Delete
/// satisfies per-key `Ver` monotonicity.
/// Presented lease ids are diagnostic evidence only. Any violation rejects
/// the whole batch.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PutBatchRequest {
    pub device: DeviceId,
    /// Diagnostic lease evidence only; never admission authority.
    pub evidence: Vec<LeaseId>,
    /// Successive client commits to admit atomically in one server turn.
    pub batches: Vec<PutBatch>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PutBatchResponse {
    /// One result per input batch, in order. Current in-process admission is
    /// still all-or-nothing, so a successful response contains only
    /// [`PutBatchResult::Applied`]; failed elements are the wire shape for
    /// richer per-batch reporting.
    pub results: Vec<PutBatchResult>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PutBatchResult {
    Applied { admission_seq: AdmissionSeq },
    Failed { error: KernelError },
}

impl PutBatchResponse {
    pub fn applied_admission_seq(&self, index: usize) -> Option<AdmissionSeq> {
        match self.results.get(index) {
            Some(PutBatchResult::Applied { admission_seq }) => Some(*admission_seq),
            _ => None,
        }
    }
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
    /// with write).
    Contended {
        prefix: Key,
        /// Hint: remaining TTL of the blocking lease, if the server chooses
        /// to reveal it. Purely advisory backoff guidance.
        retry_after: Option<Duration>,
    },
    /// The presented lease id is not live: expired, released, or never
    /// granted. Retained for lease verbs and diagnostics; put admission does
    /// not treat evidence ids as authority.
    LeaseInvalid { lease: LeaseId },
    /// Legacy coverage error. Reservation conflicts now use [`Contended`].
    NotCovered { key: Key },
    /// A Set/Delete seal is malformed for its declared scheme.
    InvalidSeal { reason: String },
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
            Self::NotCovered { key } => write!(f, "key {key:?} not covered by any presented lease"),
            Self::InvalidSeal { reason } => write!(f, "invalid seal: {reason}"),
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

#[cfg(test)]
mod tests {
    use super::*;

    fn key(parts: &[&[u8]]) -> Key {
        Key::from_bytes(parts.iter().copied()).unwrap()
    }

    #[test]
    fn put_batch_response_reports_one_result_per_batch() {
        let response = PutBatchResponse {
            results: vec![
                PutBatchResult::Applied {
                    admission_seq: AdmissionSeq(7),
                },
                PutBatchResult::Failed {
                    error: KernelError::VerRegression {
                        key: key(&[b"db", b"row"]),
                        current: Ver(3),
                        attempted: Ver(3),
                    },
                },
            ],
        };

        assert_eq!(response.results.len(), 2);
        assert_eq!(response.applied_admission_seq(0), Some(AdmissionSeq(7)));
        assert_eq!(response.applied_admission_seq(1), None);
    }
}
