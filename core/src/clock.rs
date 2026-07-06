//! Millisecond clocks.
//!
//! The kernel's state machine never reads a clock: verbs receive
//! `now: Timestamp` from the layer above, which samples a [`Clock`] at
//! request receipt. The deterministic sim substitutes [`ManualClock`] and
//! cranks time by hand — same code path, virtual time.
//!
//! Clients use the same abstraction for their own expiry timelines, counted
//! from request-send. The two timelines are never compared; that asymmetry
//! is what makes TTL expiry safe without clock synchronization.
//!
//! Two production rulers:
//!
//! - [`MonotonicClock`] — milliseconds since construction, never
//!   backward, but its origin dies with the process: readings are
//!   meaningless to any other incarnation.
//! - [`WallClock`] — the device's wall clock, the one timeline that
//!   survives process death and suspend, which is what lets a client
//!   trust stored lease deadlines across restarts. Its price: wall
//!   clocks can be stepped. The dangerous direction is *backward* (it
//!   silently extends an expiry window; forward only shrinks one), so
//!   `WallClock` cross-checks itself against a monotonic ruler and,
//!   when the wall regresses, continues on the monotonic timeline
//!   instead of following the lie — in-process readings never ride a
//!   backward step. Detection *across* process death is the storage
//!   layer's job (a persisted high-water mark; see the client crate).
//!
//! # The hybrid clock — lease math only
//!
//! Lease expiry wants both rulers at once, so lease code uses
//! [`HybridClock`] / [`HybridTimestamp`]: every stamp carries a wall
//! reading, a monotonic reading, and the [`Lineage`] of the monotonic
//! timeline (one process = one lineage). The expiry rule
//! ([`HybridTimestamp::expired`]) uses each ruler exactly where it is
//! trustworthy: **same lineage** → either ruler expires it, no margin —
//! monotonic is precise and step-immune, wall sees what monotonic
//! cannot (suspend); **different lineage** → the monotonic reading is
//! meaningless, the wall decides, shaved by a safety margin. Everything
//! else (kernel admission, the sim's virtual time) keeps the plain
//! scalar [`Clock`].

use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

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

/// The wall clock, self-checked: milliseconds since the Unix epoch,
/// guaranteed never to ride a backward step within one process.
///
/// Every reading samples both the wall clock and a monotonic ruler. The
/// monotonic continuation of the last reading is the floor: a wall
/// regression beyond `slop` marks the clock [`poisoned`](Self::poisoned)
/// (sticky, informational — stamps stayed safe because the reading never
/// followed the step) and time continues on the monotonic ruler until
/// the wall catches back up. Forward jumps are accepted as-is: they only
/// shrink expiry windows, which is the safe direction.
pub struct WallClock {
    slop_ms: u64,
    state: Mutex<WallState>,
}

struct WallState {
    /// The last reading handed out, and the monotonic instant it was
    /// taken — together, the floor every later reading must respect.
    last: Timestamp,
    at: Instant,
    poisoned: bool,
}

impl WallClock {
    /// Tolerance for wall-vs-monotonic disagreement before a regression
    /// counts as a step (NTP slew and scheduling jitter live under it).
    pub const DEFAULT_SLOP: Duration = Duration::from_secs(1);

    pub fn new() -> Self {
        Self::with_slop(Self::DEFAULT_SLOP)
    }

    pub fn with_slop(slop: Duration) -> Self {
        Self {
            slop_ms: slop.as_millis().min(u64::MAX as u128) as u64,
            state: Mutex::new(WallState {
                last: unix_now(),
                at: Instant::now(),
                poisoned: false,
            }),
        }
    }

    /// Whether the wall clock has stepped backward during this process's
    /// life. Readings never followed the step, so stamps stayed sound;
    /// this is the signal a policy layer may want anyway.
    pub fn poisoned(&self) -> bool {
        self.state.lock().unwrap().poisoned
    }
}

impl Default for WallClock {
    fn default() -> Self {
        Self::new()
    }
}

impl Clock for WallClock {
    fn now(&self) -> Timestamp {
        let mut state = self.state.lock().unwrap();
        let at = Instant::now();
        let wall = unix_now();
        let elapsed = at
            .duration_since(state.at)
            .as_millis()
            .min(u64::MAX as u128) as u64;
        let floor = Timestamp(state.last.0.saturating_add(elapsed));
        if wall.0.saturating_add(self.slop_ms) < floor.0 {
            state.poisoned = true;
        }
        let reading = wall.max(floor);
        state.last = reading;
        state.at = at;
        reading
    }
}

fn unix_now() -> Timestamp {
    let since_epoch = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO);
    Timestamp(since_epoch.as_millis().min(u64::MAX as u128) as u64)
}

/// Hand-cranked clock for tests and the deterministic sim: time moves
/// only when told to. Doubles as the simulated [`HybridClock`]: its
/// [`stamp`](HybridClock::stamp) reads `wall = now + wall skew`,
/// `mono = now`, under a settable lineage — so a test can play process
/// death ([`set_lineage`](Self::set_lineage)), suspend
/// ([`skew_wall`](Self::skew_wall): wall advances, monotonic does not),
/// and backward steps ([`set`](Self::set)), all deterministically.
pub struct ManualClock {
    now_ms: AtomicU64,
    wall_skew_ms: AtomicU64,
    lineage: Mutex<Lineage>,
}

impl ManualClock {
    pub fn new(start: Timestamp) -> Self {
        Self {
            now_ms: AtomicU64::new(start.0),
            wall_skew_ms: AtomicU64::new(0),
            lineage: Mutex::new(Lineage([1; 16])),
        }
    }

    pub fn advance(&self, by: Duration) {
        let ms = by.as_millis().min(u64::MAX as u128) as u64;
        self.now_ms.fetch_add(ms, Ordering::SeqCst);
    }

    pub fn set(&self, to: Timestamp) {
        self.now_ms.store(to.0, Ordering::SeqCst);
    }

    /// Advance the wall ruler only — simulated suspend: real time
    /// passes, the process's monotonic ruler does not see it.
    pub fn skew_wall(&self, by: Duration) {
        let ms = by.as_millis().min(u64::MAX as u128) as u64;
        self.wall_skew_ms.fetch_add(ms, Ordering::SeqCst);
    }

    /// Start a new monotonic timeline — simulated process death.
    pub fn set_lineage(&self, lineage: Lineage) {
        *self.lineage.lock().unwrap() = lineage;
    }
}

impl Clock for ManualClock {
    fn now(&self) -> Timestamp {
        Timestamp(self.now_ms.load(Ordering::SeqCst))
    }
}

impl HybridClock for ManualClock {
    fn stamp(&self) -> HybridTimestamp {
        let now = self.now_ms.load(Ordering::SeqCst);
        let skew = self.wall_skew_ms.load(Ordering::SeqCst);
        HybridTimestamp {
            wall: Timestamp(now.saturating_add(skew)),
            mono: Timestamp(now),
            lineage: *self.lineage.lock().unwrap(),
        }
    }
}

// ---------------------------------------------------------------------------
// the hybrid clock — lease math only

/// An opaque identifier of one monotonic timeline — in production, one
/// process's clock. Minted at clock construction; the caller supplies
/// the randomness (same doctrine as device ids — the clock itself never
/// touches an entropy source).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Lineage(pub [u8; 16]);

impl Lineage {
    /// The lineage of nothing: used by zeroed stamps, matched by no
    /// real clock.
    pub const NONE: Self = Self([0; 16]);
}

/// A reading of both rulers, plus the lineage of the monotonic one.
/// The currency of lease math: deadlines are stamps, expiry is
/// [`expired`](Self::expired).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HybridTimestamp {
    pub wall: Timestamp,
    pub mono: Timestamp,
    pub lineage: Lineage,
}

impl HybridTimestamp {
    /// The universally-expired stamp: both rulers at zero, no lineage.
    /// What a poisoned open leaves behind.
    pub const ZERO: Self = Self {
        wall: Timestamp::ZERO,
        mono: Timestamp::ZERO,
        lineage: Lineage::NONE,
    };

    /// Push both rulers forward — deadline arithmetic (`send + ttl`).
    pub fn saturating_add(self, d: Duration) -> Self {
        Self {
            wall: self.wall.saturating_add(d),
            mono: self.mono.saturating_add(d),
            lineage: self.lineage,
        }
    }

    /// Whether this deadline has passed at reading `now` — each ruler
    /// used exactly where it is trustworthy:
    ///
    /// - **Same lineage** (the process that stamped it is judging it):
    ///   *either* ruler expires it, and no margin applies — monotonic is
    ///   precise and immune to wall steps; wall sees the one thing
    ///   monotonic cannot (suspend). The earlier of two independently
    ///   erring rulers is conservative by construction.
    /// - **Different lineage** (a successor judging a dead process's
    ///   stamp): the monotonic reading is meaningless, so the wall
    ///   decides, shaved by `margin` — the slack for clock error.
    pub fn expired(&self, now: &HybridTimestamp, margin: Duration) -> bool {
        if self.lineage == now.lineage {
            now.mono >= self.mono || now.wall >= self.wall
        } else {
            now.wall.saturating_add(margin) >= self.wall
        }
    }
}

/// The clock lease math reads: both rulers in one stamp. Everything
/// that is not a lease deadline keeps the scalar [`Clock`].
pub trait HybridClock {
    fn stamp(&self) -> HybridTimestamp;
}

/// A shared reference to a hybrid clock is a hybrid clock.
impl<C: HybridClock + ?Sized> HybridClock for &C {
    fn stamp(&self) -> HybridTimestamp {
        (**self).stamp()
    }
}

/// The production hybrid clock: the self-checked [`WallClock`] for the
/// wall ruler, an [`Instant`] origin for the monotonic one, and the
/// caller-minted lineage naming this process's timeline.
pub struct SystemHybridClock {
    lineage: Lineage,
    origin: Instant,
    wall: WallClock,
}

impl SystemHybridClock {
    pub fn new(lineage: Lineage) -> Self {
        Self {
            lineage,
            origin: Instant::now(),
            wall: WallClock::new(),
        }
    }

    /// Whether the wall ruler has stepped backward this process life
    /// (see [`WallClock::poisoned`]).
    pub fn poisoned(&self) -> bool {
        self.wall.poisoned()
    }
}

impl HybridClock for SystemHybridClock {
    fn stamp(&self) -> HybridTimestamp {
        HybridTimestamp {
            wall: self.wall.now(),
            mono: Timestamp(self.origin.elapsed().as_millis().min(u64::MAX as u128) as u64),
            lineage: self.lineage,
        }
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
    fn wall_clock_reads_monotone_and_clean() {
        let clock = WallClock::new();
        let a = clock.now();
        let b = clock.now();
        assert!(a <= b, "the floor forbids regression");
        assert!(!clock.poisoned(), "an untouched wall clock is trusted");
        // Sanity: the reading is a plausible Unix-epoch time (after
        // 2020-01-01, i.e. not a process-origin ruler).
        assert!(a.0 > 1_577_836_800_000);
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

    #[test]
    fn hybrid_expiry_uses_each_ruler_where_it_is_trustworthy() {
        let us = Lineage([1; 16]);
        let them = Lineage([2; 16]);
        let stamp = |wall: u64, mono: u64, lineage| HybridTimestamp {
            wall: Timestamp(wall),
            mono: Timestamp(mono),
            lineage,
        };
        let margin = Duration::from_secs(5);
        let deadline = stamp(60_000, 60_000, us);

        // Same lineage: monotonic is precise — no margin shaves it.
        assert!(!deadline.expired(&stamp(59_999, 59_999, us), margin));
        assert!(deadline.expired(&stamp(60_000, 60_000, us), margin));

        // Same lineage, suspended: the wall ruler advanced while the
        // monotonic one slept — either ruler expires the stamp.
        assert!(deadline.expired(&stamp(90_000, 10_000, us), margin));

        // Same lineage, wall stepped back: monotonic still expires it.
        assert!(deadline.expired(&stamp(1_000, 60_000, us), margin));

        // Different lineage: monotonic is meaningless; the wall decides,
        // shaved by the margin.
        assert!(!deadline.expired(&stamp(54_999, 0, them), margin));
        assert!(deadline.expired(&stamp(55_000, 0, them), margin));

        // The zeroed stamp is expired for everyone, forever.
        assert!(HybridTimestamp::ZERO.expired(&stamp(0, 0, us), margin));
        assert!(HybridTimestamp::ZERO.expired(&stamp(0, 0, Lineage::NONE), margin));
    }

    #[test]
    fn manual_clock_plays_all_three_timelines() {
        let clock = ManualClock::new(Timestamp(100));
        let a = clock.stamp();
        assert_eq!((a.wall, a.mono), (Timestamp(100), Timestamp(100)));

        clock.skew_wall(Duration::from_millis(50)); // suspend
        let b = clock.stamp();
        assert_eq!((b.wall, b.mono), (Timestamp(150), Timestamp(100)));
        assert_eq!(a.lineage, b.lineage);

        clock.set_lineage(Lineage([7; 16])); // process death
        assert_ne!(clock.stamp().lineage, a.lineage);
    }
}
