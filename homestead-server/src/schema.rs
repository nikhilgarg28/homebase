//! The shard keyspace: how spaces, leases, data, and metadata map onto the
//! ordered store.
//!
//! Every storage key is a tuple encoded with the core order-preserving
//! encoding. Space records lead with the 16-byte space id, so one space is
//! one contiguous key range (space deletion = one range delete). Layout:
//!
//! ```text
//! (space, Data,          k1, k2, …)             → data record
//! (space, Changelog,     seq_be, k1, k2, …)     → ∅
//! (space, LeaseByPrefix, depth, p1…pd, id_be)   → LeaseRecord
//! (space, LeaseById,     id_be)                 → LeaseRecord
//! (space, Meta,          "counters")            → CountersRecord
//! (space, Device,        device_id)             → device record
//! ```
//!
//! The by-prefix index carries an explicit **depth** component (number of
//! prefix components). That makes both conflict-check queries ordinary
//! component-aligned prefix scans:
//!
//! - leases at *exactly* prefix A: scan `(space, LeaseByPrefix, len(A), A…)`
//!   — depth pins the interpretation, so nothing deeper matches;
//! - leases at or under prefix P: for each depth d in `len(P)..=MAX`, scan
//!   `(space, LeaseByPrefix, d, P…)` — prefix correspondence guarantees
//!   each scan returns exactly the depth-d prefixes extending P.
//!
//! Storage tuples may exceed the user-facing 16-component key limit (they
//! add space id, kind, and suffix components), which is why they encode via
//! [`encode_components`] rather than [`Key::encode`].

use homestead_core::clock::Timestamp;
use homestead_core::key::{Key, KeyComponent, encode_components};
use homestead_core::lease::{LeaseId, LeaseMode};
use homestead_core::space::SpaceId;
use homestead_core::tag::{DeviceId, Epoch};
use std::time::Duration;

/// Record kind: the second component of every space-scoped storage key.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum RecordKind {
    Data = 0,
    Changelog = 1,
    LeaseByPrefix = 2,
    LeaseById = 3,
    Meta = 4,
    Device = 5,
}

impl RecordKind {
    fn component(self) -> KeyComponent {
        KeyComponent::new(vec![self as u8]).expect("single byte component")
    }
}

fn space_component(space: SpaceId) -> KeyComponent {
    KeyComponent::new(space.0.to_vec()).expect("16-byte component")
}

fn u64_component(v: u64) -> KeyComponent {
    KeyComponent::new(v.to_be_bytes().to_vec()).expect("8-byte component")
}

/// `(space, LeaseById, id_be)`
pub fn lease_by_id_key(space: SpaceId, id: LeaseId) -> Vec<u8> {
    encode_components(&[
        space_component(space),
        RecordKind::LeaseById.component(),
        u64_component(id.0),
    ])
}

/// Scan prefix for all by-id lease records of a space.
pub fn lease_by_id_scan(space: SpaceId) -> Vec<u8> {
    encode_components(&[space_component(space), RecordKind::LeaseById.component()])
}

/// `(space, LeaseByPrefix, depth, p1…pd, id_be)`
pub fn lease_by_prefix_key(space: SpaceId, prefix: &Key, id: LeaseId) -> Vec<u8> {
    let mut components = vec![
        space_component(space),
        RecordKind::LeaseByPrefix.component(),
        KeyComponent::new(vec![prefix.components().len() as u8]).expect("depth byte"),
    ];
    components.extend(prefix.components().iter().cloned());
    components.push(u64_component(id.0));
    encode_components(&components)
}

/// Scan prefix for depth-`depth` index entries whose lease prefix starts
/// with the first `head_len` components of `head`. With `head_len ==
/// depth` this is the exact-at-prefix query; with `head_len < depth` it is
/// the descendants-at-depth query.
pub fn lease_by_prefix_scan(
    space: SpaceId,
    depth: usize,
    head: &[KeyComponent],
) -> Vec<u8> {
    let mut components = vec![
        space_component(space),
        RecordKind::LeaseByPrefix.component(),
        KeyComponent::new(vec![depth as u8]).expect("depth byte"),
    ];
    components.extend(head.iter().cloned());
    encode_components(&components)
}

/// Scan prefix for the whole by-prefix lease index of a space.
pub fn lease_by_prefix_scan_all(space: SpaceId) -> Vec<u8> {
    encode_components(&[
        space_component(space),
        RecordKind::LeaseByPrefix.component(),
    ])
}

/// `(space, Meta, "counters")`
pub fn counters_key(space: SpaceId) -> Vec<u8> {
    encode_components(&[
        space_component(space),
        RecordKind::Meta.component(),
        KeyComponent::new(b"counters".to_vec()).expect("literal component"),
    ])
}

// ---------------------------------------------------------------------------
// record values

const LEASE_RECORD_VERSION: u8 = 1;
const COUNTERS_RECORD_VERSION: u8 = 1;

/// The full server-side state of one lease grant. Stored identically under
/// both lease keys so the conflict check never needs a second lookup.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LeaseRecord {
    pub id: LeaseId,
    pub prefix: Key,
    pub mode: LeaseMode,
    pub device: DeviceId,
    pub epoch: Epoch,
    /// Server-side deadline: the lease is live strictly before this instant
    /// (strict local expiry — at the deadline it is gone).
    pub deadline: Timestamp,
    /// The granted TTL, reused on renewal.
    pub ttl: Duration,
}

impl LeaseRecord {
    pub fn is_live(&self, now: Timestamp) -> bool {
        now < self.deadline
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(1 + 1 + 16 + 8 * 4 + 32);
        out.push(LEASE_RECORD_VERSION);
        out.extend_from_slice(&self.id.0.to_be_bytes());
        out.push(match self.mode {
            LeaseMode::Read => 0,
            LeaseMode::Write => 1,
        });
        out.extend_from_slice(&self.device.0);
        out.extend_from_slice(&self.epoch.0.to_be_bytes());
        out.extend_from_slice(&self.deadline.0.to_be_bytes());
        let ttl_ms = self.ttl.as_millis().min(u64::MAX as u128) as u64;
        out.extend_from_slice(&ttl_ms.to_be_bytes());
        out.extend_from_slice(&self.prefix.encode());
        out
    }

    pub fn decode(bytes: &[u8]) -> Option<Self> {
        let mut r = Reader::new(bytes);
        if r.u8()? != LEASE_RECORD_VERSION {
            return None;
        }
        let id = LeaseId(r.u64()?);
        let mode = match r.u8()? {
            0 => LeaseMode::Read,
            1 => LeaseMode::Write,
            _ => return None,
        };
        let device = DeviceId(r.bytes16()?);
        let epoch = Epoch(r.u64()?);
        let deadline = Timestamp(r.u64()?);
        let ttl = Duration::from_millis(r.u64()?);
        let prefix = Key::decode(r.rest()).ok()?;
        Some(Self { id, prefix, mode, device, epoch, deadline, ttl })
    }
}

/// Per-space counters, updated in the same atomic batch as the operation
/// that consumes them — monotonicity survives crashes with no extra
/// machinery.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CountersRecord {
    pub next_lease_id: u64,
    pub next_epoch: u64,
    /// Last admitted batch's admission seq (0 = nothing admitted yet).
    /// Incremented by `put_batch`; read by `acquire` as the barrier.
    pub admission_high_water: u64,
}

impl CountersRecord {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(1 + 8 * 3);
        out.push(COUNTERS_RECORD_VERSION);
        out.extend_from_slice(&self.next_lease_id.to_be_bytes());
        out.extend_from_slice(&self.next_epoch.to_be_bytes());
        out.extend_from_slice(&self.admission_high_water.to_be_bytes());
        out
    }

    pub fn decode(bytes: &[u8]) -> Option<Self> {
        let mut r = Reader::new(bytes);
        if r.u8()? != COUNTERS_RECORD_VERSION {
            return None;
        }
        Some(Self {
            next_lease_id: r.u64()?,
            next_epoch: r.u64()?,
            admission_high_water: r.u64()?,
        })
    }
}

struct Reader<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, pos: 0 }
    }

    fn u8(&mut self) -> Option<u8> {
        let b = *self.bytes.get(self.pos)?;
        self.pos += 1;
        Some(b)
    }

    fn u64(&mut self) -> Option<u64> {
        let slice = self.bytes.get(self.pos..self.pos + 8)?;
        self.pos += 8;
        Some(u64::from_be_bytes(slice.try_into().unwrap()))
    }

    fn bytes16(&mut self) -> Option<[u8; 16]> {
        let slice = self.bytes.get(self.pos..self.pos + 16)?;
        self.pos += 16;
        Some(slice.try_into().unwrap())
    }

    fn rest(&self) -> &'a [u8] {
        &self.bytes[self.pos..]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_lease() -> LeaseRecord {
        LeaseRecord {
            id: LeaseId(42),
            prefix: Key::from_bytes([&b"db"[..], &b"pay"[..]]).unwrap(),
            mode: LeaseMode::Write,
            device: DeviceId([7; 16]),
            epoch: Epoch(9),
            deadline: Timestamp(12345),
            ttl: Duration::from_secs(300),
        }
    }

    #[test]
    fn lease_record_roundtrips() {
        let rec = sample_lease();
        assert_eq!(LeaseRecord::decode(&rec.encode()), Some(rec));
    }

    #[test]
    fn counters_record_roundtrips() {
        let rec = CountersRecord {
            next_lease_id: 1,
            next_epoch: 2,
            admission_high_water: 3,
        };
        assert_eq!(CountersRecord::decode(&rec.encode()), Some(rec));
    }

    #[test]
    fn by_prefix_keys_group_by_depth_then_prefix() {
        let space = SpaceId([1; 16]);
        let prefix = Key::from_bytes([&b"db"[..], &b"pay"[..]]).unwrap();
        let key = lease_by_prefix_key(space, &prefix, LeaseId(1));

        // Exact-at-prefix scan (depth == component count) matches.
        let exact = lease_by_prefix_scan(space, 2, prefix.components());
        assert!(key.starts_with(&exact));

        // Descendant scan at depth 2 from the 1-component head matches.
        let descendants = lease_by_prefix_scan(space, 2, &prefix.components()[..1]);
        assert!(key.starts_with(&descendants));

        // A depth-1 scan must not match a depth-2 index entry.
        let wrong_depth = lease_by_prefix_scan(space, 1, &prefix.components()[..1]);
        assert!(!key.starts_with(&wrong_depth));

        // Sibling prefix ("payroll") must not match a "pay" scan.
        let payroll = Key::from_bytes([&b"db"[..], &b"payroll"[..]]).unwrap();
        let sibling_key = lease_by_prefix_key(space, &payroll, LeaseId(2));
        assert!(!sibling_key.starts_with(&exact));
    }
}
