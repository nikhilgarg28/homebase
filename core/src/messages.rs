//! Request/response messages for the kernel verbs, plus [`KernelError`].
//!
//! These are the canonical, transport-neutral forms. The wire layer (likely
//! tonic/gRPC) defines its own generated DTOs and converts to and from these
//! at the boundary — which is also where prefix-scoped token enforcement
//! lives — so transport details never leak into kernel semantics or
//! validated types like [`Key`].
//!
//! Device-entry values are opaque throughout. Encrypted clients place
//! ciphertext in them, but the kernel does not classify or interpret the
//! bytes.
//!
//! The verb contract itself is [`Space`](crate::space::Space). A request
//! executes within exactly one space, selected by the authenticated
//! connection/token and never named in request bodies. Prefix-scoped token
//! enforcement on reads and writes is likewise a wire-layer concern; these
//! types describe kernel semantics for a caller already inside a space.

use crate::clock::{HybridTimestamp, Timestamp};
use crate::key::Key;
use crate::lease::{Lease, LeaseId, LeaseMode};
pub use crate::range::Range;
use crate::space::SpaceId;
use crate::tag::{
    AdmissionSeq, AdmittedEntry, DeviceChecksum, DeviceEntry, DeviceId, DeviceSeq, Mutation,
    OpaqueValue, Ver,
};
use sha2::Digest;
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
    /// Client-domain send stamp used to reconstruct the renewed deadline
    /// after local lease-state repair.
    pub requested_at: HybridTimestamp,
    pub leases: Vec<LeaseId>,
}

/// Per-lease renewal outcome. Contention piggybacks here: there is no push
/// channel, so `contended` is how a holder learns someone is waiting.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RenewGrant {
    pub id: LeaseId,
    /// Server-domain grant timestamp for this refresh.
    pub granted_at: Timestamp,
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
// admission

/// Inclusive upper bound on foreign-device admissions under a prefix.
///
/// For a batch submitted by device `D`, this asserts that every historical
/// write under `prefix` from a device other than `D` was admitted at or
/// before `upto`. Earlier writes from `D` are ordered by its `DeviceSeq`
/// stream and do not invalidate the assertion.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RangeAssert {
    pub prefix: Key,
    pub upto: AdmissionSeq,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RangeAssertFailure {
    pub prefix: Key,
    pub upto: AdmissionSeq,
    pub actual: AdmissionSeq,
}

/// One device-sequenced unit within an atomic admission request. An empty
/// `entries` vector is the wire no-op used for a local rollback marker.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AdmissionBatch {
    /// Client-assigned, strictly increasing per device; one per client
    /// commit. A request may coalesce successive client commits without
    /// erasing their individual seq identity.
    pub device_seq: DeviceSeq,
    /// Assertions evaluated against the scratch prefix state immediately
    /// before this client batch is applied.
    pub range_asserts: Vec<RangeAssert>,
    pub entries: Vec<DeviceEntry>,
}

impl AdmissionBatch {
    /// Extend a device's cumulative checksum with this exact canonical batch.
    pub fn checksum(
        &self,
        previous: DeviceChecksum,
        space: SpaceId,
        device: DeviceId,
    ) -> DeviceChecksum {
        let mut hash = previous.hasher();
        hash.update(space.0);
        hash.update(device.0);
        hash.update(self.device_seq.0.to_be_bytes());
        hash.update((self.range_asserts.len() as u64).to_be_bytes());
        for assertion in &self.range_asserts {
            hash_bytes(&mut hash, &assertion.prefix.encode());
            hash.update(assertion.upto.0.to_be_bytes());
        }
        hash.update((self.entries.len() as u64).to_be_bytes());
        for entry in &self.entries {
            match &entry.mutation {
                Mutation::Set { key, value } => {
                    hash.update([0]);
                    hash_bytes(&mut hash, &key.encode());
                    hash_bytes(&mut hash, &value.0);
                }
                Mutation::Delete { key } => {
                    hash.update([1]);
                    hash_bytes(&mut hash, &key.encode());
                }
                Mutation::DeleteRange { range } => {
                    hash.update([2]);
                    hash_bytes(&mut hash, &range.encode());
                }
            }
            hash.update(entry.tag.device.0);
            hash.update(entry.tag.device_seq.0.to_be_bytes());
            hash.update(entry.tag.ver.0.to_be_bytes());
            hash.update(entry.tag.cipher_epoch.0.to_be_bytes());
            hash_bytes(&mut hash, &entry.seal.encode());
        }
        DeviceChecksum(hash.finalize().into())
    }
}

fn hash_bytes(hash: &mut sha2::Sha256, bytes: &[u8]) {
    hash.update((bytes.len() as u64).to_be_bytes());
    hash.update(bytes);
}

/// Atomic admission request (request = transaction; torn requests impossible).
///
/// Admission requires: no Set/Delete key overlaps a live foreign lease
/// reservation, every Set/Delete has a valid [`Seal`], and every Set/Delete
/// satisfies per-key `Ver` monotonicity.
/// Presented lease ids are diagnostic evidence only. Any violation rejects
/// the whole batch.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AdmissionRequest {
    pub device: DeviceId,
    /// The checksum the client last confirmed for this device in this space.
    pub expected_checksum: DeviceChecksum,
    /// Diagnostic lease evidence only; never admission authority.
    pub evidence: Vec<LeaseId>,
    /// Successive client commits to admit atomically in one server turn.
    pub batches: Vec<AdmissionBatch>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AdmissionResponse {
    /// Final checksum after all applied batches, or the unchanged checksum
    /// when this response reports failed batches.
    pub checksum: DeviceChecksum,
    /// One result per input batch, in order. Current in-process admission is
    /// still all-or-nothing, so a successful response contains only
    /// [`AdmissionResult::Applied`]; failed elements are the wire shape for
    /// richer per-batch reporting.
    pub results: Vec<AdmissionResult>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AdmissionResult {
    Applied { admission_seq: AdmissionSeq },
    Failed { error: KernelError },
}

impl AdmissionResponse {
    pub fn applied_admission_seq(&self, index: usize) -> Option<AdmissionSeq> {
        match self.results.get(index) {
            Some(AdmissionResult::Applied { admission_seq }) => Some(*admission_seq),
            _ => None,
        }
    }
}

// ---------------------------------------------------------------------------
// admission log

/// One durably admitted client batch in the server's exact space history.
///
/// `checksum` is the submitting device's cumulative checksum after this
/// batch. Empty rollback batches have an empty `entries` vector but retain
/// the rest of their identity here.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AdmittedBatch<T = OpaqueValue> {
    pub admission_seq: AdmissionSeq,
    pub device: DeviceId,
    pub device_seq: DeviceSeq,
    pub checksum: DeviceChecksum,
    pub entries: Vec<AdmittedEntry<T>>,
}

/// Reads a dense prefix of the retained server admission log after `after`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PullRequest {
    /// Must not exceed the server's current admission high water.
    pub after: AdmissionSeq,
    /// Maximum number of complete admitted batches to return. `None` means
    /// no protocol-level batch limit; `Some(0)` returns an empty page.
    pub max_batches: Option<usize>,
}

/// A complete dense server-log interval `(after, through]`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PullResponse {
    pub after: AdmissionSeq,
    pub through: AdmissionSeq,
    pub batches: Vec<AdmittedBatch>,
}

impl<T> AdmittedBatch<T> {
    /// Validates all redundant batch/entry ordering and device metadata.
    pub fn validate(&self) -> Result<(), PullDensityError> {
        u32::try_from(self.entries.len()).map_err(|_| PullDensityError::OperationCountOverflow)?;
        for (expected_index, entry) in self.entries.iter().enumerate() {
            let expected_index = u32::try_from(expected_index)
                .map_err(|_| PullDensityError::OperationCountOverflow)?;
            if entry.admission.admission_seq != self.admission_seq {
                return Err(PullDensityError::EntryAdmissionMismatch {
                    batch: self.admission_seq,
                    entry: entry.admission.admission_seq,
                });
            }
            if entry.admission.op_index != expected_index {
                return Err(PullDensityError::OperationIndexMismatch {
                    admission_seq: self.admission_seq,
                    expected: expected_index,
                    actual: entry.admission.op_index,
                });
            }
            if entry.device_entry.tag.device != self.device
                || entry.device_entry.tag.device_seq != self.device_seq
            {
                return Err(PullDensityError::EntryDeviceMismatch {
                    admission_seq: self.admission_seq,
                });
            }
        }
        Ok(())
    }
}

impl PullResponse {
    /// Validates that this response contains exactly the complete dense
    /// interval `(after, through]`, including headers for empty batches.
    pub fn validate_dense(&self) -> Result<(), PullDensityError> {
        if self.through < self.after {
            return Err(PullDensityError::CursorRegression {
                after: self.after,
                through: self.through,
            });
        }
        let expected_len = self.through.0 - self.after.0;
        if u64::try_from(self.batches.len()).ok() != Some(expected_len) {
            return Err(PullDensityError::BatchCountMismatch {
                expected: expected_len,
                actual: self.batches.len(),
            });
        }
        for (offset, batch) in self.batches.iter().enumerate() {
            let offset =
                u64::try_from(offset).map_err(|_| PullDensityError::AdmissionSeqOverflow)?;
            let expected = AdmissionSeq(
                self.after
                    .0
                    .checked_add(
                        offset
                            .checked_add(1)
                            .ok_or(PullDensityError::AdmissionSeqOverflow)?,
                    )
                    .ok_or(PullDensityError::AdmissionSeqOverflow)?,
            );
            if batch.admission_seq != expected {
                return Err(PullDensityError::BatchSequenceMismatch {
                    expected,
                    actual: batch.admission_seq,
                });
            }
            batch.validate()?;
        }
        Ok(())
    }
}

/// A malformed supposedly dense server pull.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PullDensityError {
    CursorRegression {
        after: AdmissionSeq,
        through: AdmissionSeq,
    },
    BatchCountMismatch {
        expected: u64,
        actual: usize,
    },
    BatchSequenceMismatch {
        expected: AdmissionSeq,
        actual: AdmissionSeq,
    },
    EntryAdmissionMismatch {
        batch: AdmissionSeq,
        entry: AdmissionSeq,
    },
    OperationIndexMismatch {
        admission_seq: AdmissionSeq,
        expected: u32,
        actual: u32,
    },
    EntryDeviceMismatch {
        admission_seq: AdmissionSeq,
    },
    AdmissionSeqOverflow,
    OperationCountOverflow,
}

impl fmt::Display for PullDensityError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "malformed dense pull: {self:?}")
    }
}

impl std::error::Error for PullDensityError {}

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
pub struct GetResponse<T = OpaqueValue> {
    pub entries: Vec<Option<AdmittedEntry<T>>>,
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
pub struct ListResponse<T = OpaqueValue> {
    /// Live entries in key order (tombstones excluded).
    pub entries: Vec<AdmittedEntry<T>>,
    /// True when the scan stopped at `limit` with more remaining.
    pub truncated: bool,
}

// ---------------------------------------------------------------------------
// read_at

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
pub enum RangeCut<T = OpaqueValue> {
    /// Full state of the range at the cut (cursor was `None`); live entries
    /// only, key order.
    Snapshot(Vec<AdmittedEntry<T>>),
    /// Relevant source operations since the caller's cursor, tombstones
    /// included, in ascending [`AdmissionOrder`](crate::tag::AdmissionOrder).
    Delta(Vec<AdmittedEntry<T>>),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReadAtResponse<T = OpaqueValue> {
    /// The single admission point at which every range was evaluated —
    /// never a torn multi-range read. Also the caller's next cursor for
    /// all requested ranges.
    pub at: AdmissionSeq,
    /// Parallel to the requested ranges.
    pub ranges: Vec<RangeCut<T>>,
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
    /// A range write overlaps a live foreign lease reservation.
    RangeContended {
        range: Range,
        retry_after: Option<Duration>,
    },
    /// The presented lease id is not live: expired, released, or never
    /// granted. Retained for lease verbs and diagnostics; put admission does
    /// not treat evidence ids as authority.
    LeaseInvalid { lease: LeaseId },
    /// Legacy coverage error. Reservation conflicts now use [`Contended`].
    NotCovered { key: Key },
    /// A mutation seal is malformed for its declared scheme.
    InvalidSeal { reason: String },
    /// Range deletes are understood by the protocol but not yet admitted by
    /// this server implementation.
    /// One or more range watermarks did not match the server-visible prefix
    /// high water.
    RangeAssertFailed { failures: Vec<RangeAssertFailure> },
    /// Per-key version monotonicity violated.
    VerRegression {
        key: Key,
        current: Ver,
        attempted: Ver,
    },
    /// A range mutation did not exceed every version affecting its target.
    RangeVerRegression {
        range: Range,
        current: Ver,
        attempted: Ver,
    },
    /// Per-device batch sequence monotonicity violated: replay or
    /// out-of-order submission (client-assigned, strictly increasing).
    DeviceSeqRegression {
        current: DeviceSeq,
        attempted: DeviceSeq,
    },
    /// The request was based on a different device-stream history.
    DeviceChecksumMismatch {
        current_seq: DeviceSeq,
        current: DeviceChecksum,
    },
    /// A read cursor names history the space has not admitted.
    AdmissionCursorAhead {
        after: AdmissionSeq,
        high_water: AdmissionSeq,
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
            Self::RangeContended { range, retry_after } => match retry_after {
                Some(d) => write!(f, "range {range:?} is contended (retry in ~{d:?})"),
                None => write!(f, "range {range:?} is contended"),
            },
            Self::LeaseInvalid { lease } => write!(f, "lease {lease:?} is not live"),
            Self::NotCovered { key } => write!(f, "key {key:?} not covered by any presented lease"),
            Self::InvalidSeal { reason } => write!(f, "invalid seal: {reason}"),
            Self::RangeAssertFailed { failures } => {
                write!(f, "{} range assert(s) failed", failures.len())
            }
            Self::VerRegression {
                key,
                current,
                attempted,
            } => write!(
                f,
                "ver regression on {key:?}: attempted {attempted:?} ≤ current {current:?}"
            ),
            Self::RangeVerRegression {
                range,
                current,
                attempted,
            } => write!(
                f,
                "ver regression on {range:?}: attempted {attempted:?} ≤ current {current:?}"
            ),
            Self::DeviceSeqRegression { current, attempted } => write!(
                f,
                "device_seq regression: attempted {attempted:?} ≤ current {current:?}"
            ),
            Self::DeviceChecksumMismatch {
                current_seq,
                current,
            } => write!(
                f,
                "device checksum mismatch at {current_seq:?}: server has {current:?}"
            ),
            Self::AdmissionCursorAhead { after, high_water } => write!(
                f,
                "admission cursor {after:?} is ahead of server high water {high_water:?}"
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
    use crate::seal::Seal;
    use crate::tag::{CipherEpoch, DeviceTag};

    fn key(parts: &[&[u8]]) -> Key {
        Key::from_bytes(parts.iter().copied()).unwrap()
    }

    #[test]
    fn admit_response_reports_one_result_per_batch() {
        let response = AdmissionResponse {
            checksum: DeviceChecksum::EMPTY,
            results: vec![
                AdmissionResult::Applied {
                    admission_seq: AdmissionSeq(7),
                },
                AdmissionResult::Failed {
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

    fn entry(device: DeviceId, seq: DeviceSeq, name: &[u8]) -> DeviceEntry {
        DeviceEntry {
            mutation: Mutation::Set {
                key: key(&[b"db", name]),
                value: OpaqueValue(name.to_vec()),
            },
            tag: DeviceTag {
                device,
                device_seq: seq,
                ver: Ver(seq.0),
                cipher_epoch: CipherEpoch(0),
            },
            seal: Seal::empty_aead_v1(),
        }
    }

    #[test]
    fn cumulative_checksum_commits_to_order_content_and_scope() {
        let space = SpaceId([1; 16]);
        let device = DeviceId([2; 16]);
        let first = AdmissionBatch {
            device_seq: DeviceSeq(1),
            range_asserts: vec![],
            entries: vec![entry(device, DeviceSeq(1), b"one")],
        };
        let second = AdmissionBatch {
            device_seq: DeviceSeq(2),
            range_asserts: vec![],
            entries: vec![entry(device, DeviceSeq(2), b"two")],
        };
        let first_checksum = first.checksum(DeviceChecksum::EMPTY, space, device);
        let ordered = second.checksum(first_checksum, space, device);

        let omitted = second.checksum(DeviceChecksum::EMPTY, space, device);
        let reordered = first.checksum(
            second.checksum(DeviceChecksum::EMPTY, space, device),
            space,
            device,
        );
        let mut altered = second.clone();
        altered.entries[0].seal.nonce[0] ^= 1;

        assert_ne!(ordered, omitted);
        assert_ne!(ordered, reordered);
        assert_ne!(ordered, altered.checksum(first_checksum, space, device));
        assert_ne!(
            ordered,
            second.checksum(first_checksum, SpaceId([9; 16]), device)
        );
        assert_ne!(
            ordered,
            second.checksum(first_checksum, space, DeviceId([9; 16]))
        );
    }

    #[test]
    fn empty_rollback_batch_still_extends_checksum() {
        let batch = AdmissionBatch {
            device_seq: DeviceSeq(7),
            range_asserts: vec![],
            entries: vec![],
        };
        assert_ne!(
            batch.checksum(DeviceChecksum::EMPTY, SpaceId([1; 16]), DeviceId([2; 16])),
            DeviceChecksum::EMPTY
        );
    }

    #[test]
    fn checksum_commits_to_delete_range_kind_and_target() {
        let space = SpaceId([1; 16]);
        let device = DeviceId([2; 16]);
        let range_entry = |range| DeviceEntry {
            mutation: Mutation::DeleteRange { range },
            tag: DeviceTag {
                device,
                device_seq: DeviceSeq(1),
                ver: Ver(1),
                cipher_epoch: CipherEpoch(0),
            },
            seal: Seal::empty_aead_v1(),
        };
        let checksum = |entry| {
            AdmissionBatch {
                device_seq: DeviceSeq(1),
                range_asserts: vec![],
                entries: vec![entry],
            }
            .checksum(DeviceChecksum::EMPTY, space, device)
        };

        let prefix = key(&[b"db"]);
        let range = checksum(range_entry(Range::Prefix(prefix.clone())));
        let full = checksum(range_entry(Range::Full));
        let point = checksum(DeviceEntry {
            mutation: Mutation::Delete { key: prefix },
            tag: DeviceTag {
                device,
                device_seq: DeviceSeq(1),
                ver: Ver(1),
                cipher_epoch: CipherEpoch(0),
            },
            seal: Seal::empty_aead_v1(),
        });

        assert_ne!(range, full);
        assert_ne!(range, point);
    }

    fn admitted_batch(admission: u64, device_seq: u64, op_count: u32) -> AdmittedBatch {
        let device = DeviceId([2; 16]);
        AdmittedBatch {
            admission_seq: AdmissionSeq(admission),
            device,
            device_seq: DeviceSeq(device_seq),
            checksum: DeviceChecksum([admission as u8; 32]),
            entries: (0..op_count)
                .map(|op_index| AdmittedEntry {
                    device_entry: entry(device, DeviceSeq(device_seq), &[op_index as u8 + 1]),
                    admission: crate::tag::AdmissionTag {
                        admission_seq: AdmissionSeq(admission),
                        op_index,
                    },
                })
                .collect(),
        }
    }

    #[test]
    fn pull_density_accepts_complete_batches_and_empty_headers() {
        let response = PullResponse {
            after: AdmissionSeq(4),
            through: AdmissionSeq(6),
            batches: vec![admitted_batch(5, 8, 2), admitted_batch(6, 9, 0)],
        };
        assert_eq!(response.validate_dense(), Ok(()));
    }

    #[test]
    fn pull_density_rejects_batch_and_operation_gaps() {
        let mut missing_batch = PullResponse {
            after: AdmissionSeq(4),
            through: AdmissionSeq(6),
            batches: vec![admitted_batch(5, 8, 1)],
        };
        assert!(matches!(
            missing_batch.validate_dense(),
            Err(PullDensityError::BatchCountMismatch { .. })
        ));

        missing_batch.through = AdmissionSeq(5);
        missing_batch.batches[0].entries[0].admission.op_index = 1;
        assert!(matches!(
            missing_batch.validate_dense(),
            Err(PullDensityError::OperationIndexMismatch { .. })
        ));

        let mut wrong_sequence = PullResponse {
            after: AdmissionSeq(4),
            through: AdmissionSeq(5),
            batches: vec![admitted_batch(6, 8, 1)],
        };
        assert!(matches!(
            wrong_sequence.validate_dense(),
            Err(PullDensityError::BatchSequenceMismatch { .. })
        ));

        wrong_sequence.batches[0] = admitted_batch(5, 8, 1);
        wrong_sequence.batches[0].entries[0].admission.admission_seq = AdmissionSeq(4);
        assert!(matches!(
            wrong_sequence.validate_dense(),
            Err(PullDensityError::EntryAdmissionMismatch { .. })
        ));
    }
}
