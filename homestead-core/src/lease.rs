//! Leases and lease-time primitives.
//!
//! Leases come in two modes: **write** leases are exclusive against
//! everything, **read** leases coexist with other read leases and exclude
//! write leases. Two leases are in conflict when their prefixes overlap
//! (one is a component-wise prefix of the other) and their modes are
//! incompatible. There is no read→write upgrade, ever — release and
//! re-acquire instead. The server clock exists in exactly one code path —
//! lease deadlines — timed via [`crate::clock`].
//!
//! # Stealable leases
//!
//! A lease may opt in to preemption at acquire time (`stealable`). An
//! acquire with `steal = true` takes a prefix *pre-deadline* if every
//! incompatible live blocker is stealable: the blockers are purged and the
//! new grant's fresh epoch fences their holders — a victim's next put or
//! renew fails, so correctness never depends on it noticing. Leases not
//! marked stealable keep strict pre-deadline denial. This is the
//! single-active-device primitive: one stealable write lease on the account
//! prefix, and activating a new device steals it.
//!
//! # Asymmetric expiry
//!
//! Grants carry a TTL *duration*, never an absolute deadline. The client
//! counts the TTL from request-send on its monotonic clock; the server
//! counts from receipt. The client's window therefore starts strictly
//! earlier and expires strictly earlier — a client that respects its local
//! deadline can never write after the server has re-granted the prefix.
//! Epochs remain the correctness backstop; timestamps are availability.

use crate::key::Key;
use crate::tag::Epoch;
use std::time::Duration;

/// Lease mode. Prefix overlap alone is not a conflict; the modes decide.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum LeaseMode {
    /// Shared: any number of devices may hold overlapping read leases.
    /// Guards a read set — nothing under the prefix can change while held.
    /// Never authorizes writes, and never upgrades to `Write`.
    Read,
    /// Exclusive: overlaps nothing, read or write. Required for every key
    /// admitted by `put_batch`.
    Write,
}

impl LeaseMode {
    /// Whether two leases with overlapping prefixes may coexist.
    pub fn compatible_with(self, other: LeaseMode) -> bool {
        matches!((self, other), (LeaseMode::Read, LeaseMode::Read))
    }
}

/// Unique identifier of a lease grant. Never reused within a space; a
/// re-grant of the same prefix is a new `LeaseId` and a new [`Epoch`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct LeaseId(pub u64);

/// A granted lease, as returned to the client by `acquire`.
///
/// Deliberately carries no absolute deadline — see the module docs on
/// asymmetric expiry. The server keeps its own deadline bookkeeping
/// internally.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Lease {
    pub id: LeaseId,
    /// The covered prefix: every key that starts (component-wise) with it.
    pub prefix: Key,
    pub mode: LeaseMode,
    pub epoch: Epoch,
    /// Granted TTL. May be shorter than requested (kernel cap → class
    /// default → app pin).
    pub ttl: Duration,
    /// Whether this lease may be preempted pre-deadline by an
    /// `acquire(steal = true)` (see the module docs).
    pub stealable: bool,
}

impl Lease {
    /// True when this lease's prefix covers `key`.
    pub fn covers(&self, key: &Key) -> bool {
        key.starts_with(&self.prefix)
    }
}

/// Proof-of-lease presented with `put_batch`.
///
/// Both fields must match the server's live lease table: the id proves the
/// grant still exists (strict local expiry — expired is gone), the epoch
/// fences a zombie holder of a superseded grant.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct LeaseRef {
    pub id: LeaseId,
    pub epoch: Epoch,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tag::Epoch;

    #[test]
    fn covers_is_component_wise() {
        let lease = Lease {
            id: LeaseId(1),
            prefix: Key::from_bytes([&b"db"[..], &b"pay"[..]]).unwrap(),
            mode: LeaseMode::Write,
            epoch: Epoch(7),
            ttl: Duration::from_secs(300),
            stealable: false,
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
