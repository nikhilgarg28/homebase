//! Client-side lease lifetime: the local half of asymmetric expiry.
//!
//! A grant carries a TTL *duration*, never an absolute deadline (see
//! [`homebase_core::lease`]). [`HeldLease`] pins that TTL to the client's
//! own timeline: the deadline counts from the instant the acquire (or
//! renew) request was *sent*, sampled on the client's monotonic clock. The
//! server counts the same TTL from receipt, which is never earlier — so the
//! local deadline always falls due first, and a holder that stops
//! presenting the lease at its local deadline can never have a write
//! admitted after the server re-grants the prefix. Epochs remain the
//! correctness backstop; this discipline is what keeps them a backstop
//! rather than the first line of defense.

use homebase_core::clock::Timestamp;
use homebase_core::key::Key;
use homebase_core::lease::{Lease, LeaseId, LeaseMode, LeaseRef};
use homebase_core::messages::RenewGrant;

/// A granted lease bound to its local deadline.
///
/// Expiry is strict and mirrors the server's: the lease is dead the moment
/// `now` reaches the deadline. [`lease_ref`](HeldLease::lease_ref) is the
/// only way to obtain proof-of-lease from a hold, so an expired hold cannot
/// authorize anything by construction.
///
/// A locally-expired hold is not necessarily gone on the server (the
/// server's window closes later), so it stays renewable: a successful renew
/// re-arms the deadline from that renew's send time, and the safety
/// argument holds unchanged. Only [`Session`](crate::session::Session)
/// constructs these — a `HeldLease` always corresponds to a real grant.
#[derive(Clone, Debug)]
pub struct HeldLease {
    lease: Lease,
    /// Local deadline: send time of the granting (or last renewing) request
    /// plus the granted TTL, on the client's timeline.
    deadline: Timestamp,
    contended: bool,
}

impl HeldLease {
    /// Wraps a fresh grant. `sent_at` must be sampled *before* the acquire
    /// request left — sampling after receipt would claim time the server
    /// never promised.
    pub(crate) fn grant(lease: Lease, sent_at: Timestamp) -> Self {
        let deadline = sent_at.saturating_add(lease.ttl);
        Self {
            lease,
            deadline,
            contended: false,
        }
    }

    /// Applies a renewal: fresh deadline from the renew request's send
    /// time, plus the piggybacked contention signal (overwritten, not
    /// latched — demand that got served stops being reported).
    pub(crate) fn renewed(&mut self, sent_at: Timestamp, grant: &RenewGrant) {
        debug_assert_eq!(self.lease.id, grant.id);
        self.deadline = sent_at.saturating_add(grant.ttl);
        self.contended = grant.contended;
    }

    /// Live strictly before the deadline; dead exactly at it, matching the
    /// server's own strict expiry.
    pub fn is_live(&self, now: Timestamp) -> bool {
        now < self.deadline
    }

    /// Proof-of-lease for a request sent at `now` — `None` once the local
    /// deadline has passed. Strict local expiry: this gate is what keeps a
    /// slow holder from writing into a re-granted prefix.
    pub fn lease_ref(&self, now: Timestamp) -> Option<LeaseRef> {
        self.is_live(now).then_some(LeaseRef {
            id: self.lease.id,
            epoch: self.lease.epoch,
        })
    }

    pub fn id(&self) -> LeaseId {
        self.lease.id
    }

    pub fn mode(&self) -> LeaseMode {
        self.lease.mode
    }

    pub fn prefix(&self) -> &Key {
        &self.lease.prefix
    }

    /// The underlying grant.
    pub fn lease(&self) -> &Lease {
        &self.lease
    }

    /// Local deadline, on the client's timeline.
    pub fn deadline(&self) -> Timestamp {
        self.deadline
    }

    /// The latest renewal's contention signal: another device wants an
    /// overlapping prefix. Demand-driven stickiness — release once past
    /// min-hold and convenient.
    pub fn contended(&self) -> bool {
        self.contended
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use homebase_core::tag::Epoch;
    use std::time::Duration;

    fn lease(ttl_ms: u64) -> Lease {
        Lease {
            id: LeaseId(7),
            prefix: Key::from_bytes([&b"db"[..]]).unwrap(),
            mode: LeaseMode::Write,
            epoch: Epoch(3),
            ttl: Duration::from_millis(ttl_ms),
            stealable: false,
        }
    }

    #[test]
    fn deadline_counts_from_send_and_expiry_is_strict() {
        let held = HeldLease::grant(lease(100), Timestamp(40));
        assert_eq!(held.deadline(), Timestamp(140));
        assert!(held.is_live(Timestamp(139)));
        assert!(!held.is_live(Timestamp(140)), "dead exactly at the deadline");

        let r = held.lease_ref(Timestamp(139)).unwrap();
        assert_eq!(r, LeaseRef { id: LeaseId(7), epoch: Epoch(3) });
        assert_eq!(held.lease_ref(Timestamp(140)), None);
    }

    #[test]
    fn renewal_rearms_from_its_own_send_time() {
        let mut held = HeldLease::grant(lease(100), Timestamp(0));
        held.renewed(
            Timestamp(60),
            &RenewGrant { id: LeaseId(7), ttl: Duration::from_millis(100), contended: true },
        );
        assert_eq!(held.deadline(), Timestamp(160));
        assert!(held.contended());

        // A locally-expired hold is still renewable — resurrection is safe
        // because the server's window closes later than the local one.
        let mut held = HeldLease::grant(lease(100), Timestamp(0));
        assert!(!held.is_live(Timestamp(100)));
        held.renewed(
            Timestamp(100),
            &RenewGrant { id: LeaseId(7), ttl: Duration::from_millis(100), contended: false },
        );
        assert!(held.is_live(Timestamp(150)));
        assert!(!held.contended(), "contention is overwritten, not latched");
    }
}
