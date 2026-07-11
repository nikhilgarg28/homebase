//! Leases and lease-time primitives.
//!
//! Leases come in two modes: **write** leases are exclusive against
//! everything, **read** leases coexist with other read leases and exclude
//! write leases. Two leases are in conflict when their prefixes overlap
//! (one is a component-wise prefix of the other) and their modes are
//! incompatible. There is no read→write upgrade, ever — release and
//! re-acquire instead.
//!
//! Leases are reservations, not write capabilities: data admission may
//! proceed without a held lease when no active incompatible lease conflicts
//! with the write. Held lease ids may still travel as diagnostic evidence.
//!
//! # Clock domains
//!
//! A lease records both the client-minted request timestamp and the server
//! grant timestamp. The server judges expiry by `granted_at + ttl`; the
//! client judges local authority conservatively by `requested_at + ttl`
//! minus its local safety margin. The two timestamps are never compared to
//! each other.

use crate::clock::{HybridTimestamp, Timestamp};
use crate::key::Key;
use crate::tag::AdmissionSeq;
use std::time::Duration;

/// Lease mode. Prefix overlap alone is not a conflict; the modes decide.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum LeaseMode {
    /// Shared: any number of devices may hold overlapping read leases.
    /// Guards a read set — nothing under the prefix can change while held.
    /// Never authorizes writes, and never upgrades to `Write`.
    Read,
    /// Exclusive: overlaps nothing, read or write. Required for every key
    /// admitted by `admit`.
    Write,
}

impl LeaseMode {
    /// Whether two leases with overlapping prefixes may coexist.
    pub fn compatible_with(self, other: LeaseMode) -> bool {
        matches!((self, other), (LeaseMode::Read, LeaseMode::Read))
    }
}

/// Unique identifier of a lease grant. Never reused within a space.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct LeaseId(pub u64);

/// A granted lease, as returned to the client by `acquire`.
///
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Lease {
    pub id: LeaseId,
    /// The covered prefix: every key that starts (component-wise) with it.
    pub prefix: Key,
    pub mode: LeaseMode,
    /// Client clock stamp from the acquire/refresh request that created this lease.
    pub requested_at: HybridTimestamp,
    /// Server clock stamp at grant/refresh.
    pub granted_at: Timestamp,
    /// Granted TTL. May be shorter than requested (kernel cap → class
    /// default → app pin).
    pub ttl: Duration,
    /// Prefix-scoped admission barrier: `effective_prefix_max(prefix)` at grant.
    pub barrier: AdmissionSeq,
}

impl Lease {
    /// True when this lease's prefix covers `key`.
    pub fn covers(&self, key: &Key) -> bool {
        key.starts_with(&self.prefix)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn covers_is_component_wise() {
        let lease = Lease {
            id: LeaseId(1),
            prefix: Key::from_bytes([&b"db"[..], &b"pay"[..]]).unwrap(),
            mode: LeaseMode::Write,
            requested_at: HybridTimestamp::ZERO,
            granted_at: Timestamp::ZERO,
            ttl: Duration::from_secs(300),
            barrier: AdmissionSeq(0),
        };
        let covered = Key::from_bytes([&b"db"[..], &b"pay"[..], &b"row1"[..]]).unwrap();
        let sibling = Key::from_bytes([&b"db"[..], &b"payroll"[..]]).unwrap();
        assert!(lease.covers(&covered));
        assert!(!lease.covers(&sibling));
    }

    #[test]
    fn only_read_read_is_compatible() {
        use LeaseMode::{Read, Write};
        assert!(Read.compatible_with(Read));
        assert!(!Read.compatible_with(Write));
        assert!(!Write.compatible_with(Read));
        assert!(!Write.compatible_with(Write));
    }
}
