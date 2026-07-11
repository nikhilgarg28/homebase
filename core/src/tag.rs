//! Value tags and entries: what the kernel stores alongside every value.
//!
//! Every stored value is tagged `(device, device_seq, epoch, ver,
//! admission_seq)`. The server never interprets values; tags are the only
//! server-legible metadata, and they exist for fencing, replication cursors,
//! and fork detection — not for querying.

use crate::key::Key;
use crate::seal::Seal;
use std::fmt;

/// Identifies a writing device: 16 opaque bytes, UUID-shaped.
///
/// Assigned by the platform and carried as a token claim; the kernel never
/// generates or interprets one.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DeviceId(pub [u8; 16]);

impl fmt::Debug for DeviceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "device:")?;
        for b in self.0 {
            write!(f, "{b:02x}")?;
        }
        Ok(())
    }
}

/// Client-assigned batch sequence number, strictly increasing per device.
///
/// One `put_batch` = one `DeviceSeq`. Replicas use it to detect gaps in a
/// device's stream; the codec binds it into AEAD associated data.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DeviceSeq(pub u64);

/// Lease fencing token, server-assigned at acquire.
///
/// Epochs are correctness, timestamps are availability: any write admitted
/// under an old epoch after a re-grant is rejected regardless of clocks.
/// This is also the fencing token handed downstream by leader-election users.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Epoch(pub u64);

/// Server-assigned admission sequence: the total order of admitted batches
/// within a space.
///
/// All entries of one batch share one `AdmissionSeq` (batch = transaction).
/// Cursors and `read_at` cuts are positions in this order; the augmented
/// tree maintains its max under any prefix.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct AdmissionSeq(pub u64);

/// Client-computed per-key version.
///
/// The server enforces strict monotonicity per key (a put must carry a `Ver`
/// strictly greater than the stored one) but never generates versions —
/// clients own the chain, which keeps it meaningful under E2EE.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Ver(pub u64);

/// The tag attached to every stored value.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Tag {
    pub device: DeviceId,
    pub device_seq: DeviceSeq,
    /// Epoch of the lease that covered this key at admission.
    pub epoch: Epoch,
    pub ver: Ver,
    pub admission_seq: AdmissionSeq,
}

/// A value as written and stored. Deletes are explicit: a tombstone is a
/// first-class `Absent` value carrying a tag, not a missing row.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Value {
    /// Opaque bytes — ciphertext by the time the kernel sees them.
    Present(Vec<u8>),
    /// A tombstone.
    Absent,
}

impl Value {
    pub fn is_present(&self) -> bool {
        matches!(self, Self::Present(_))
    }

    pub fn is_absent(&self) -> bool {
        matches!(self, Self::Absent)
    }
}

/// A stored entry as returned by reads.
///
/// Tombstones (`Value::Absent`) appear in `read_at` deltas — replicas must
/// observe deletes — while `get` and `list` return live entries only, so
/// their entries are always `Present`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Entry {
    pub key: Key,
    pub value: Value,
    pub seal: Seal,
    pub tag: Tag,
}
