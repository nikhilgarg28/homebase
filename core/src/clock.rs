//! Monotonic millisecond clocks.
//!
//! The kernel's state machine never reads a clock: verbs receive
//! `now: Timestamp` from the layer above, which samples a [`Clock`] at
//! request receipt. The deterministic sim substitutes [`ManualClock`] and
//! cranks time by hand — same code path, virtual time.
//!
//! Clients use the same abstraction for their own expiry timelines, counted
//! from request-send. The two timelines are never compared; that asymmetry
//! is what makes TTL expiry safe without clock synchronization.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// Milliseconds on a monotonic clock.
///
/// Timestamps never cross the wire: grants carry TTL durations only, and
/// server and client each keep their own timeline (asymmetric expiry). The
/// server's state machine takes `now: Timestamp` explicitly rather than
/// reading a clock, so it stays deterministic — this is what makes the
/// torture-sim milestone tractable.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Timestamp(pub u64);

impl Timestamp {
    pub const ZERO: Self = Self(0);

    pub fn saturating_add(self, d: Duration) -> Self {
        let millis = d.as_millis().min(u64::MAX as u128) as u64;
        Self(self.0.saturating_add(millis))
    }
}

pub trait Clock {
    /// Current position on this clock's timeline.
    fn now(&self) -> Timestamp;
}

/// A shared reference to a clock is a clock — `now` already takes `&self`.
/// Tests hold the `ManualClock` and hand out `&clock` to crank it mid-run.
impl<C: Clock + ?Sized> Clock for &C {
    fn now(&self) -> Timestamp {
        (**self).now()
    }
}

/// Real monotonic clock: milliseconds elapsed since construction.
pub struct MonotonicClock {
    origin: Instant,
}

impl MonotonicClock {
    pub fn new() -> Self {
        Self {
            origin: Instant::now(),
        }
    }
}

impl Default for MonotonicClock {
    fn default() -> Self {
        Self::new()
    }
}

impl Clock for MonotonicClock {
    fn now(&self) -> Timestamp {
        Timestamp(self.origin.elapsed().as_millis().min(u64::MAX as u128) as u64)
    }
}

/// Hand-cranked clock for tests and the deterministic sim: time moves only
/// when told to.
pub struct ManualClock {
    now_ms: AtomicU64,
}

impl ManualClock {
    pub fn new(start: Timestamp) -> Self {
        Self {
            now_ms: AtomicU64::new(start.0),
        }
    }

    pub fn advance(&self, by: Duration) {
        let ms = by.as_millis().min(u64::MAX as u128) as u64;
        self.now_ms.fetch_add(ms, Ordering::SeqCst);
    }

    pub fn set(&self, to: Timestamp) {
        self.now_ms.store(to.0, Ordering::SeqCst);
    }
}

impl Clock for ManualClock {
    fn now(&self) -> Timestamp {
        Timestamp(self.now_ms.load(Ordering::SeqCst))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manual_clock_moves_only_when_cranked() {
        let clock = ManualClock::new(Timestamp(100));
        assert_eq!(clock.now(), Timestamp(100));
        assert_eq!(clock.now(), Timestamp(100));

        clock.advance(Duration::from_millis(50));
        assert_eq!(clock.now(), Timestamp(150));

        clock.set(Timestamp(1000));
        assert_eq!(clock.now(), Timestamp(1000));
    }

    #[test]
    fn monotonic_clock_never_goes_backwards() {
        let clock = MonotonicClock::new();
        let a = clock.now();
        let b = clock.now();
        assert!(a <= b);
    }

    #[test]
    fn timestamp_add_saturates() {
        assert_eq!(
            Timestamp(10).saturating_add(Duration::from_millis(5)),
            Timestamp(15)
        );
        assert_eq!(
            Timestamp(u64::MAX).saturating_add(Duration::from_millis(1)),
            Timestamp(u64::MAX)
        );
    }
}
