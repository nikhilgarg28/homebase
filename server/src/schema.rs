//! The shard keyspace: how spaces, leases, data, and metadata map onto the
//! ordered store.
//!
//! Every storage key is a tuple encoded with the core order-preserving
//! encoding. Space records lead with the 16-byte space id, so one space is
//! one contiguous key range (space deletion = one range delete). Layout:
//!
//! ```text
//! (space, Data,          k1, k2, …)             → DataRecord (tag + value)
//! (space, LeaseByPrefix, depth, p1…pd, id_be)   → LeaseRecord
//! (space, LeaseById,     id_be)                 → LeaseRecord
//! (space, Meta,          "counters")            → CountersRecord
//! (space, Device,        device_id)             → DeviceRecord
//! (space, PrefixMeta,    depth, p1…pd)          → PrefixMetaRecord
//! (space, AdmissionLog,  seq_be, Header)         → AdmissionHeaderRecord
//! (space, AdmissionLog,  seq_be, Op, i_be, k…)   → DataRecord
//! ```
//!
//! **AdmissionLog is the immutable replay history.** Every admitted batch has
//! one header, including empty rollback batches, and every admitted operation
//! has one entry in stable `(admission_seq, op_index)` order. Materialized
//! Data and PrefixMeta records may be replaced; AdmissionLog records are never
//! moved or replaced.
//!
//! **PrefixMeta is the durable augmented tree**: per `(depth, prefix)`, the
//! two greatest historical admission points from distinct devices plus the
//! live-key count, updated along every written key's full prefix path in the
//! same atomic batch as the write. Two heads preserve the global maximum for
//! reads and the maximum excluding one submitting device for causal range
//! assertions, even after overwrite or delete.
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

use homebase_core::clock::{HybridTimestamp, Lineage, Timestamp};
use homebase_core::key::{Key, KeyComponent, decode_components, encode_components};
use homebase_core::lease::{LeaseId, LeaseMode};
use homebase_core::seal::Seal;
use homebase_core::space::SpaceId;
use homebase_core::tag::{
    AdmissionSeq, AdmissionTag, AdmittedEntry, CipherEpoch, DeviceChecksum, DeviceEntry, DeviceId,
    DeviceSeq, DeviceTag, Mutation, OpaqueValue, Ver,
};
use std::time::Duration;

/// Record kind: the second component of every space-scoped storage key.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum RecordKind {
    Data = 0,
    LeaseByPrefix = 2,
    LeaseById = 3,
    Meta = 4,
    Device = 5,
    PrefixMeta = 6,
    AdmissionLog = 7,
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

fn u32_component(v: u32) -> KeyComponent {
    KeyComponent::new(v.to_be_bytes().to_vec()).expect("4-byte component")
}

/// `(space, Data, k1, k2, …)`. Also the scan prefix for data at or under a
/// user prefix — by prefix correspondence they are the same bytes.
pub fn data_key(space: SpaceId, key: &Key) -> Vec<u8> {
    let mut components = vec![space_component(space), RecordKind::Data.component()];
    components.extend(key.components().iter().cloned());
    encode_components(&components)
}

/// Recovers the user key from a Data storage key.
pub fn user_key_from_data(storage_key: &[u8]) -> Option<Key> {
    let components = decode_components(storage_key).ok()?;
    Key::new(components.get(2..)?.to_vec()).ok()
}

/// Byte prefix of a space's whole Data keyspace.
pub fn data_scan_all(space: SpaceId) -> Vec<u8> {
    encode_components(&[space_component(space), RecordKind::Data.component()])
}

/// `(space, PrefixMeta, depth, p1…pd)` for the first `depth` components of
/// `head`.
pub fn prefix_meta_key(space: SpaceId, head: &[KeyComponent]) -> Vec<u8> {
    let mut components = vec![
        space_component(space),
        RecordKind::PrefixMeta.component(),
        KeyComponent::new(vec![head.len() as u8]).expect("depth byte"),
    ];
    components.extend(head.iter().cloned());
    encode_components(&components)
}

/// Byte prefix of a space's whole PrefixMeta keyspace.
pub fn prefix_meta_scan_all(space: SpaceId) -> Vec<u8> {
    encode_components(&[space_component(space), RecordKind::PrefixMeta.component()])
}

const ADMISSION_HEADER_COMPONENT: u8 = 0;
const ADMISSION_OP_COMPONENT: u8 = 1;

fn admission_component(kind: u8) -> KeyComponent {
    KeyComponent::new(vec![kind]).expect("single-byte admission component")
}

/// Byte prefix of a space's complete immutable admission log.
pub fn admission_log_scan_all(space: SpaceId) -> Vec<u8> {
    encode_components(&[space_component(space), RecordKind::AdmissionLog.component()])
}

/// `(space, AdmissionLog, admission_seq, Header)`.
pub fn admission_header_key(space: SpaceId, seq: AdmissionSeq) -> Vec<u8> {
    encode_components(&[
        space_component(space),
        RecordKind::AdmissionLog.component(),
        u64_component(seq.0),
        admission_component(ADMISSION_HEADER_COMPONENT),
    ])
}

/// `(space, AdmissionLog, admission_seq, Op, op_index, k1, k2, …)`.
pub fn admission_op_key(space: SpaceId, seq: AdmissionSeq, op_index: u32, key: &Key) -> Vec<u8> {
    let mut components = vec![
        space_component(space),
        RecordKind::AdmissionLog.component(),
        u64_component(seq.0),
        admission_component(ADMISSION_OP_COMPONENT),
        u32_component(op_index),
    ];
    components.extend(key.components().iter().cloned());
    encode_components(&components)
}

/// Scan prefix for one admitted batch's operations.
pub fn admission_op_scan(space: SpaceId, seq: AdmissionSeq) -> Vec<u8> {
    encode_components(&[
        space_component(space),
        RecordKind::AdmissionLog.component(),
        u64_component(seq.0),
        admission_component(ADMISSION_OP_COMPONENT),
    ])
}

/// Recovers `(admission_seq, op_index, user_key)` from an admission op key.
pub fn admission_op_parts(storage_key: &[u8]) -> Option<(AdmissionSeq, u32, Key)> {
    let components = decode_components(storage_key).ok()?;
    if components.get(3)?.as_bytes() != [ADMISSION_OP_COMPONENT] {
        return None;
    }
    let seq = AdmissionSeq(u64::from_be_bytes(
        components.get(2)?.as_bytes().try_into().ok()?,
    ));
    let op_index = u32::from_be_bytes(components.get(4)?.as_bytes().try_into().ok()?);
    let key = Key::new(components.get(5..)?.to_vec()).ok()?;
    Some((seq, op_index, key))
}

/// `(space, Device, device_id)`
pub fn device_key(space: SpaceId, device: DeviceId) -> Vec<u8> {
    encode_components(&[
        space_component(space),
        RecordKind::Device.component(),
        KeyComponent::new(device.0.to_vec()).expect("16-byte component"),
    ])
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
pub fn lease_by_prefix_scan(space: SpaceId, depth: usize, head: &[KeyComponent]) -> Vec<u8> {
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

const LEASE_RECORD_VERSION: u8 = 2;
const COUNTERS_RECORD_VERSION: u8 = 1;
const DATA_RECORD_VERSION: u8 = 1;
const DEVICE_RECORD_VERSION: u8 = 1;
const PREFIX_META_RECORD_VERSION: u8 = 1;
const ADMISSION_HEADER_RECORD_VERSION: u8 = 1;

/// Durable identity and completeness check for one admitted client batch.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AdmissionHeaderRecord {
    pub device: DeviceId,
    pub device_seq: DeviceSeq,
    pub checksum: DeviceChecksum,
    pub operation_count: u32,
}

impl AdmissionHeaderRecord {
    pub fn encode(self) -> Vec<u8> {
        let mut out = Vec::with_capacity(1 + 16 + 8 + 32 + 4);
        out.push(ADMISSION_HEADER_RECORD_VERSION);
        out.extend_from_slice(&self.device.0);
        out.extend_from_slice(&self.device_seq.0.to_be_bytes());
        out.extend_from_slice(&self.checksum.0);
        out.extend_from_slice(&self.operation_count.to_be_bytes());
        out
    }

    pub fn decode(bytes: &[u8]) -> Option<Self> {
        let mut r = Reader::new(bytes);
        if r.u8()? != ADMISSION_HEADER_RECORD_VERSION {
            return None;
        }
        let record = Self {
            device: DeviceId(r.bytes16()?),
            device_seq: DeviceSeq(r.u64()?),
            checksum: DeviceChecksum(r.take(32)?.try_into().ok()?),
            operation_count: r.u32()?,
        };
        r.rest().is_empty().then_some(record)
    }
}

/// The full server-side state of one lease grant. Stored identically under
/// both lease keys so the conflict check never needs a second lookup.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LeaseRecord {
    pub id: LeaseId,
    pub prefix: Key,
    pub mode: LeaseMode,
    pub device: DeviceId,
    /// Client clock stamp supplied with the acquire request that granted this lease.
    pub requested_at: HybridTimestamp,
    /// Server clock stamp at grant/renewal.
    pub granted_at: Timestamp,
    /// Prefix-scoped admission barrier captured at grant.
    pub barrier: AdmissionSeq,
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
        let mut out = Vec::with_capacity(1 + 8 + 1 + 16 + 16 + 8 * 7 + 32);
        out.push(LEASE_RECORD_VERSION);
        out.extend_from_slice(&self.id.0.to_be_bytes());
        out.push(match self.mode {
            LeaseMode::Read => 0,
            LeaseMode::Write => 1,
        });
        out.extend_from_slice(&self.device.0);
        out.extend_from_slice(&self.requested_at.wall.0.to_be_bytes());
        out.extend_from_slice(&self.requested_at.mono.0.to_be_bytes());
        out.extend_from_slice(&self.requested_at.lineage.0);
        out.extend_from_slice(&self.granted_at.0.to_be_bytes());
        out.extend_from_slice(&self.barrier.0.to_be_bytes());
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
        let requested_at = HybridTimestamp {
            wall: Timestamp(r.u64()?),
            mono: Timestamp(r.u64()?),
            lineage: Lineage(r.bytes16()?),
        };
        let granted_at = Timestamp(r.u64()?);
        let barrier = AdmissionSeq(r.u64()?);
        let deadline = Timestamp(r.u64()?);
        let ttl = Duration::from_millis(r.u64()?);
        let prefix = Key::decode(r.rest()).ok()?;
        Some(Self {
            id,
            prefix,
            mode,
            device,
            requested_at,
            granted_at,
            barrier,
            deadline,
            ttl,
        })
    }
}

/// Per-space counters, updated in the same atomic batch as the operation
/// that consumes them — monotonicity survives crashes with no extra
/// machinery.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CountersRecord {
    pub next_lease_id: u64,
    /// Last admitted batch's admission seq (0 = nothing admitted yet).
    /// Incremented by `admit`; read by `acquire` as the barrier.
    pub admission_high_water: u64,
}

impl CountersRecord {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(1 + 8 * 2);
        out.push(COUNTERS_RECORD_VERSION);
        out.extend_from_slice(&self.next_lease_id.to_be_bytes());
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
            admission_high_water: r.u64()?,
        })
    }
}

/// One stored key's tag and value. The same bytes live under the Data key
/// (current state) and the key's single Changelog entry (delta feed), so a
/// delta scan never needs a second lookup.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DataRecord {
    pub entry: AdmittedEntry,
}

impl DataRecord {
    pub fn encode(&self) -> Vec<u8> {
        let value_len = match &self.entry.device_entry.mutation {
            Mutation::Set { value, .. } => value.0.len(),
            Mutation::Delete { .. } => 0,
            Mutation::DeleteRange { .. } => {
                panic!("unsupported DeleteRange cannot be stored as point data")
            }
        };
        let tag = self.entry.device_entry.tag;
        let seal = self.entry.device_entry.seal.encode();
        let mut out = Vec::with_capacity(1 + 16 + 8 * 4 + 4 * 2 + seal.len() + 1 + value_len);
        out.push(DATA_RECORD_VERSION);
        out.extend_from_slice(&tag.device.0);
        out.extend_from_slice(&tag.device_seq.0.to_be_bytes());
        out.extend_from_slice(&tag.ver.0.to_be_bytes());
        out.extend_from_slice(&tag.cipher_epoch.0.to_be_bytes());
        out.extend_from_slice(&self.entry.admission.admission_seq.0.to_be_bytes());
        out.extend_from_slice(&self.entry.admission.op_index.to_be_bytes());
        out.extend_from_slice(&(seal.len() as u32).to_be_bytes());
        out.extend_from_slice(&seal);
        match &self.entry.device_entry.mutation {
            Mutation::Delete { .. } => out.push(0),
            Mutation::Set { value, .. } => {
                out.push(1);
                out.extend_from_slice(&value.0);
            }
            Mutation::DeleteRange { .. } => {
                unreachable!("DeleteRange was rejected before point-data encoding")
            }
        }
        out
    }

    pub fn decode(key: Key, bytes: &[u8]) -> Option<Self> {
        let mut r = Reader::new(bytes);
        if r.u8()? != DATA_RECORD_VERSION {
            return None;
        }
        let tag = DeviceTag {
            device: DeviceId(r.bytes16()?),
            device_seq: DeviceSeq(r.u64()?),
            ver: Ver(r.u64()?),
            cipher_epoch: CipherEpoch(r.u64()?),
        };
        let admission = AdmissionTag {
            admission_seq: AdmissionSeq(r.u64()?),
            op_index: r.u32()?,
        };
        let seal_len = r.u32()? as usize;
        let seal = Seal::decode(r.take(seal_len)?).ok()?;
        let mutation = match r.u8()? {
            0 => Mutation::Delete { key },
            1 => Mutation::Set {
                key,
                value: OpaqueValue(r.rest().to_vec()),
            },
            _ => return None,
        };
        Some(Self {
            entry: AdmittedEntry {
                device_entry: DeviceEntry {
                    mutation,
                    tag,
                    seal,
                },
                admission,
            },
        })
    }
}

/// One device's latest historical admission under a prefix.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DeviceAdmission {
    pub device: DeviceId,
    pub admission_seq: AdmissionSeq,
}

impl DeviceAdmission {
    const EMPTY: Self = Self {
        device: DeviceId([0; 16]),
        admission_seq: AdmissionSeq(0),
    };
}

/// Write-time aggregates for one `(depth, prefix)`: the durable form of the
/// augmented range-max tree. `first` and `second` are the greatest historical
/// admissions from distinct devices. They never regress or disappear after
/// overwrite/delete, while `live_count` may return to zero.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PrefixMetaRecord {
    pub first: DeviceAdmission,
    pub second: DeviceAdmission,
    /// Number of live (non-tombstoned) keys under this prefix.
    pub live_count: u64,
}

impl PrefixMetaRecord {
    pub const fn empty() -> Self {
        Self {
            first: DeviceAdmission::EMPTY,
            second: DeviceAdmission::EMPTY,
            live_count: 0,
        }
    }

    pub fn max_admission_seq(self) -> AdmissionSeq {
        self.first.admission_seq.max(self.second.admission_seq)
    }

    pub fn max_excluding(self, device: DeviceId) -> AdmissionSeq {
        if self.first.device != device {
            self.first.admission_seq
        } else {
            self.second.admission_seq
        }
    }

    pub fn observe(&mut self, device: DeviceId, admission_seq: AdmissionSeq) {
        let observed = DeviceAdmission {
            device,
            admission_seq,
        };
        if self.first.admission_seq.0 == 0 {
            self.first = observed;
        } else if self.first.device == device {
            self.first.admission_seq = self.first.admission_seq.max(admission_seq);
        } else if self.second.admission_seq.0 == 0 {
            self.second = observed;
        } else if self.second.device == device {
            self.second.admission_seq = self.second.admission_seq.max(admission_seq);
        } else if admission_seq > self.second.admission_seq {
            self.second = observed;
        }
        if self.second.admission_seq > self.first.admission_seq {
            std::mem::swap(&mut self.first, &mut self.second);
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(1 + (16 + 8) * 2 + 8);
        out.push(PREFIX_META_RECORD_VERSION);
        out.extend_from_slice(&self.first.device.0);
        out.extend_from_slice(&self.first.admission_seq.0.to_be_bytes());
        out.extend_from_slice(&self.second.device.0);
        out.extend_from_slice(&self.second.admission_seq.0.to_be_bytes());
        out.extend_from_slice(&self.live_count.to_be_bytes());
        out
    }

    pub fn decode(bytes: &[u8]) -> Option<Self> {
        let mut r = Reader::new(bytes);
        if r.u8()? != PREFIX_META_RECORD_VERSION {
            return None;
        }
        Some(Self {
            first: DeviceAdmission {
                device: DeviceId(r.bytes16()?),
                admission_seq: AdmissionSeq(r.u64()?),
            },
            second: DeviceAdmission {
                device: DeviceId(r.bytes16()?),
                admission_seq: AdmissionSeq(r.u64()?),
            },
            live_count: r.u64()?,
        })
    }
}

/// Per-device admission state: the high-water `device_seq`, for replay and
/// out-of-order rejection.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DeviceRecord {
    pub last_seq: DeviceSeq,
    pub checksum: DeviceChecksum,
}

impl DeviceRecord {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(1 + 8 + 32);
        out.push(DEVICE_RECORD_VERSION);
        out.extend_from_slice(&self.last_seq.0.to_be_bytes());
        out.extend_from_slice(&self.checksum.0);
        out
    }

    pub fn decode(bytes: &[u8]) -> Option<Self> {
        let mut r = Reader::new(bytes);
        if r.u8()? != DEVICE_RECORD_VERSION {
            return None;
        }
        Some(Self {
            last_seq: DeviceSeq(r.u64()?),
            checksum: DeviceChecksum(r.take(32)?.try_into().ok()?),
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

    fn u32(&mut self) -> Option<u32> {
        let slice = self.bytes.get(self.pos..self.pos + 4)?;
        self.pos += 4;
        Some(u32::from_be_bytes(slice.try_into().unwrap()))
    }

    fn bytes16(&mut self) -> Option<[u8; 16]> {
        let slice = self.bytes.get(self.pos..self.pos + 16)?;
        self.pos += 16;
        Some(slice.try_into().unwrap())
    }

    fn take(&mut self, len: usize) -> Option<&'a [u8]> {
        let slice = self.bytes.get(self.pos..self.pos + len)?;
        self.pos += len;
        Some(slice)
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
            requested_at: HybridTimestamp::ZERO,
            granted_at: Timestamp(1000),
            barrier: AdmissionSeq(3),
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
    fn prefix_meta_record_roundtrips() {
        let mut rec = PrefixMetaRecord::empty();
        rec.observe(DeviceId([1; 16]), AdmissionSeq(17));
        rec.observe(DeviceId([2; 16]), AdmissionSeq(11));
        rec.live_count = 4;
        assert_eq!(PrefixMetaRecord::decode(&rec.encode()), Some(rec));
        assert_eq!(rec.max_excluding(DeviceId([1; 16])), AdmissionSeq(11));
        assert_eq!(rec.max_excluding(DeviceId([3; 16])), AdmissionSeq(17));
    }

    #[test]
    fn prefix_meta_keeps_two_distinct_device_heads() {
        let mut rec = PrefixMetaRecord::empty();
        rec.observe(DeviceId([1; 16]), AdmissionSeq(1));
        rec.observe(DeviceId([2; 16]), AdmissionSeq(2));
        rec.observe(DeviceId([3; 16]), AdmissionSeq(3));
        assert_eq!(rec.first.device, DeviceId([3; 16]));
        assert_eq!(rec.second.device, DeviceId([2; 16]));

        rec.observe(DeviceId([1; 16]), AdmissionSeq(4));
        assert_eq!(rec.first.device, DeviceId([1; 16]));
        assert_eq!(rec.second.device, DeviceId([3; 16]));
        assert_eq!(rec.max_excluding(DeviceId([1; 16])), AdmissionSeq(3));
        assert_eq!(rec.max_excluding(DeviceId([3; 16])), AdmissionSeq(4));

        rec.observe(DeviceId([1; 16]), AdmissionSeq(2));
        assert_eq!(rec.first.admission_seq, AdmissionSeq(4));
    }

    #[test]
    fn prefix_meta_keys_group_by_depth() {
        let space = SpaceId([1; 16]);
        let key = Key::from_bytes([&b"db"[..], &b"pay"[..]]).unwrap();
        let deep = prefix_meta_key(space, key.components());
        let shallow = prefix_meta_key(space, &key.components()[..1]);
        assert_ne!(deep, shallow);
        // Depth pins the interpretation: a depth-1 key is never a byte
        // prefix of a depth-2 key.
        assert!(!deep.starts_with(&shallow));
        assert!(deep.starts_with(&prefix_meta_scan_all(space)));
        assert!(shallow.starts_with(&prefix_meta_scan_all(space)));
    }

    #[test]
    fn counters_record_roundtrips() {
        let rec = CountersRecord {
            next_lease_id: 1,
            admission_high_water: 3,
        };
        assert_eq!(CountersRecord::decode(&rec.encode()), Some(rec));
    }

    #[test]
    fn data_record_roundtrips() {
        let key = Key::from_bytes([&b"db"[..], &b"row"[..]]).unwrap();
        let tag = DeviceTag {
            device: DeviceId([3; 16]),
            device_seq: DeviceSeq(7),
            ver: Ver(11),
            cipher_epoch: CipherEpoch(2),
        };
        let present = DataRecord {
            entry: AdmittedEntry {
                device_entry: DeviceEntry {
                    mutation: Mutation::Set {
                        key: key.clone(),
                        value: OpaqueValue(b"ct".to_vec()),
                    },
                    tag,
                    seal: Seal::empty_aead_v1(),
                },
                admission: AdmissionTag {
                    admission_seq: AdmissionSeq(99),
                    op_index: 3,
                },
            },
        };
        assert_eq!(
            DataRecord::decode(key.clone(), &present.encode()),
            Some(present)
        );
        let tombstone = DataRecord {
            entry: AdmittedEntry {
                device_entry: DeviceEntry {
                    mutation: Mutation::Delete { key: key.clone() },
                    tag,
                    seal: Seal::empty_aead_v1(),
                },
                admission: AdmissionTag {
                    admission_seq: AdmissionSeq(99),
                    op_index: 4,
                },
            },
        };
        assert_eq!(
            DataRecord::decode(key, &tombstone.encode()),
            Some(tombstone)
        );
    }

    #[test]
    fn admission_header_and_operation_keys_roundtrip() {
        let space = SpaceId([8; 16]);
        let seq = AdmissionSeq(23);
        let header = AdmissionHeaderRecord {
            device: DeviceId([4; 16]),
            device_seq: DeviceSeq(17),
            checksum: DeviceChecksum([5; 32]),
            operation_count: 2,
        };
        assert_eq!(
            AdmissionHeaderRecord::decode(&header.encode()),
            Some(header)
        );

        let key = Key::from_bytes([&b"db"[..], &b"row"[..]]).unwrap();
        let storage_key = admission_op_key(space, seq, 1, &key);
        assert_eq!(admission_op_parts(&storage_key), Some((seq, 1, key)));
        assert!(storage_key.starts_with(&admission_op_scan(space, seq)));
    }

    #[test]
    fn device_record_roundtrips() {
        let rec = DeviceRecord {
            last_seq: DeviceSeq(41),
            checksum: DeviceChecksum([7; 32]),
        };
        assert_eq!(DeviceRecord::decode(&rec.encode()), Some(rec));
    }

    #[test]
    fn data_keys_recover_user_keys_and_scan_by_prefix() {
        let space = SpaceId([1; 16]);
        let key = Key::from_bytes([&b"db"[..], &b"pay"[..], &b"r1"[..]]).unwrap();
        let storage = data_key(space, &key);
        assert_eq!(user_key_from_data(&storage), Some(key.clone()));

        // Data scan for a user prefix is the same bytes as the prefix's key.
        let prefix = Key::from_bytes([&b"db"[..], &b"pay"[..]]).unwrap();
        assert!(storage.starts_with(&data_key(space, &prefix)));
        let sibling = Key::from_bytes([&b"db"[..], &b"payroll"[..]]).unwrap();
        assert!(!data_key(space, &sibling).starts_with(&data_key(space, &prefix)));
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
