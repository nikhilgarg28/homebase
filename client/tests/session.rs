//! Session discipline against the real in-process kernel: a `SpaceActor`
//! over a `MemoryStore`, driven on a `LocalPool` — the same rig the server's
//! own tests use, with the session doing the client-side bookkeeping.
//!
//! The wire between session and actor is a fault-injecting [`Space`]
//! wrapper ([`Flaky`]) so the two halves of the `Unavailable` retry
//! contract are separable: a request lost *before* admission (retry just
//! works) versus an ack lost *after* admission (the device_seq replay
//! fence reports the batch as already applied — exactly once, never twice).
//!
//! The two-clock tests are the ones nothing server-side can express:
//! client and server each own a `ManualClock`, and the client's local
//! deadline must gate writes strictly before the server's window closes.

use futures::executor::LocalPool;
use futures::task::LocalSpawnExt;
use homebase::session::DEFAULT_RETRY_BUDGET;
use homebase::{PutError, PutOutcome, Session};
use homebase_core::clock::{Clock, ManualClock, Timestamp};
use homebase_core::key::Key;
use homebase_core::lease::{LeaseMode, LeaseRef};
use homebase_core::messages::{
    GetRequest, GetResponse, KernelError, LeaseSpec, ListRequest, ListResponse, PutBatchRequest,
    PutBatchResponse, PutEntry, RangeCut, ReadAtRequest, ReadAtResponse, ReleaseRequest,
    ReleaseResponse, RenewRequest, RenewResponse, AcquireRequest, AcquireResponse, PrefixCursor,
};
use homebase_core::space::{Space, SpaceError, SpaceId};
use homebase_core::tag::{AdmissionSeq, DeviceId, DeviceSeq, Value, Ver};
use homebase_server::actor::SpaceActor;
use homebase_server::storage::MemoryStore;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

const SPACE: SpaceId = SpaceId([3; 16]);

fn dev(n: u8) -> DeviceId {
    DeviceId([n; 16])
}

fn key(components: &[&[u8]]) -> Key {
    Key::from_bytes(components.iter().copied()).unwrap()
}

fn wspec(prefix: &Key, ttl_ms: u64) -> LeaseSpec {
    LeaseSpec {
        prefix: prefix.clone(),
        mode: LeaseMode::Write,
        ttl: Duration::from_millis(ttl_ms),
        stealable: false,
    }
}

fn entry(k: &Key, v: &[u8], ver: u64) -> PutEntry {
    PutEntry {
        key: k.clone(),
        value: Value::Present(v.to_vec()),
        ver: Ver(ver),
    }
}

/// Decrements `counter` if positive; true when a credit was consumed.
fn take(counter: &AtomicU32) -> bool {
    counter
        .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
        .is_ok()
}

/// Fault-injecting transport: delegates to the real actor handle, with
/// countdown knobs that model the two distinct network failures a client
/// must distinguish — a request that never arrived (`fail_puts`: error
/// *before* delegation, nothing admitted) and a reply that never returned
/// (`drop_acks`: error *after* delegation, batch admitted).
#[derive(Clone)]
struct Flaky<S> {
    inner: S,
    puts_seen: Arc<AtomicU32>,
    fail_puts: Arc<AtomicU32>,
    drop_acks: Arc<AtomicU32>,
    fail_reads: Arc<AtomicU32>,
}

impl<S> Flaky<S> {
    fn new(inner: S) -> Self {
        Self {
            inner,
            puts_seen: Arc::new(AtomicU32::new(0)),
            fail_puts: Arc::new(AtomicU32::new(0)),
            drop_acks: Arc::new(AtomicU32::new(0)),
            fail_reads: Arc::new(AtomicU32::new(0)),
        }
    }

    fn puts_seen(&self) -> u32 {
        self.puts_seen.load(Ordering::SeqCst)
    }
}

impl<S: Space + Sync> Space for Flaky<S> {
    fn acquire(
        &self,
        req: AcquireRequest,
    ) -> impl Future<Output = Result<AcquireResponse, SpaceError>> + Send {
        self.inner.acquire(req)
    }

    fn renew(
        &self,
        req: RenewRequest,
    ) -> impl Future<Output = Result<RenewResponse, SpaceError>> + Send {
        self.inner.renew(req)
    }

    fn release(
        &self,
        req: ReleaseRequest,
    ) -> impl Future<Output = Result<ReleaseResponse, SpaceError>> + Send {
        self.inner.release(req)
    }

    async fn put_batch(&self, req: PutBatchRequest) -> Result<PutBatchResponse, SpaceError> {
        self.puts_seen.fetch_add(1, Ordering::SeqCst);
        if take(&self.fail_puts) {
            return Err(SpaceError::unavailable("injected: request lost"));
        }
        let resp = self.inner.put_batch(req).await?;
        if take(&self.drop_acks) {
            return Err(SpaceError::unavailable("injected: ack lost"));
        }
        Ok(resp)
    }

    async fn get(&self, req: GetRequest) -> Result<GetResponse, SpaceError> {
        if take(&self.fail_reads) {
            return Err(SpaceError::unavailable("injected: read lost"));
        }
        self.inner.get(req).await
    }

    fn list(
        &self,
        req: ListRequest,
    ) -> impl Future<Output = Result<ListResponse, SpaceError>> + Send {
        self.inner.list(req)
    }

    fn read_at(
        &self,
        req: ReadAtRequest,
    ) -> impl Future<Output = Result<ReadAtResponse, SpaceError>> + Send {
        self.inner.read_at(req)
    }
}

type Rig = (LocalPool, Flaky<homebase_server::actor::SpaceHandle>, Arc<ManualClock>);

/// Actor over a fresh store, running on the pool; returns the flaky
/// transport in front of its handle plus the *server's* clock.
fn rig() -> Rig {
    let pool = LocalPool::new();
    let server_clock = Arc::new(ManualClock::new(Timestamp(0)));
    let store = Arc::new(MemoryStore::new());
    let (actor, handle) = SpaceActor::new(SPACE, store, Arc::clone(&server_clock));
    pool.spawner().spawn_local(actor.run()).unwrap();
    (pool, Flaky::new(handle), server_clock)
}

fn value_of(resp: &GetResponse, i: usize) -> Option<Vec<u8>> {
    resp.entries[i].as_ref().map(|e| match &e.value {
        Value::Present(v) => v.clone(),
        Value::Absent => panic!("tombstone leaked out of get"),
    })
}

#[test]
fn put_writes_under_an_acquired_lease() {
    let (mut pool, flaky, clock) = rig();
    let mut session = Session::new(flaky, clock, dev(1));
    pool.run_until(async move {
        let db = key(&[b"db"]);
        let acq = session.acquire(vec![wspec(&db, 1000)], false).await.unwrap();
        assert_eq!(acq.barrier, AdmissionSeq(0), "nothing admitted yet");
        assert_eq!(acq.leases.len(), 1);

        let row = key(&[b"db", b"row"]);
        let outcome = session.put(vec![entry(&row, b"v1", 1)]).await.unwrap();
        assert_eq!(outcome, PutOutcome::Admitted(AdmissionSeq(1)));
        assert_eq!(session.next_seq(), DeviceSeq(2), "seq advances per batch");

        let got = session.get(vec![row]).await.unwrap();
        assert_eq!(value_of(&got, 0), Some(b"v1".to_vec()));
    });
}

#[test]
fn coverage_is_local_and_write_only() {
    let (mut pool, flaky, clock) = rig();
    let probe = flaky.clone();
    let mut session = Session::new(flaky, clock, dev(1));
    pool.run_until(async move {
        let row = key(&[b"db", b"row"]);

        // No lease at all: refused locally, nothing crosses the wire.
        let err = session.put(vec![entry(&row, b"v", 1)]).await.unwrap_err();
        assert!(matches!(err, PutError::NotCovered { .. }));
        assert_eq!(probe.puts_seen(), 0, "local refusal must not touch the wire");

        // A read lease guards a read set; it never authorizes writes.
        let read_spec = LeaseSpec {
            prefix: key(&[b"db"]),
            mode: LeaseMode::Read,
            ttl: Duration::from_millis(1000),
            stealable: false,
        };
        session.acquire(vec![read_spec], false).await.unwrap();
        let err = session.put(vec![entry(&row, b"v", 1)]).await.unwrap_err();
        assert!(matches!(err, PutError::NotCovered { .. }));
        assert_eq!(probe.puts_seen(), 0);
    });
}

/// The two-clock property in miniature: the session's deadline runs on the
/// *client's* clock from request send, so it closes strictly before the
/// server's window. Past the local deadline the session refuses to write —
/// even though the server, whose clock hasn't moved, would still admit the
/// very same ref.
#[test]
fn local_deadline_gates_before_the_servers_window_closes() {
    let (mut pool, flaky, _server_clock) = rig();
    let client_clock = Arc::new(ManualClock::new(Timestamp(0)));
    let probe = flaky.clone();
    let mut session = Session::new(flaky, Arc::clone(&client_clock), dev(1));
    pool.run_until(async move {
        let db = key(&[b"db"]);
        let row = key(&[b"db", b"row"]);
        let acq = session.acquire(vec![wspec(&db, 100)], false).await.unwrap();
        let held = session.lease(acq.leases[0]).unwrap().lease().clone();

        // Client timeline reaches the local deadline; server timeline is
        // still at 0 and its grant is fully live.
        client_clock.advance(Duration::from_millis(100));
        let err = session.put(vec![entry(&row, b"v", 1)]).await.unwrap_err();
        assert!(matches!(err, PutError::NotCovered { .. }));
        assert_eq!(probe.puts_seen(), 0, "locally expired means nothing is sent");

        // Prove the asymmetry is real: presenting the same ref raw, outside
        // the session's discipline, is still admitted by the server.
        let raw = probe
            .inner
            .put_batch(PutBatchRequest {
                device: dev(1),
                device_seq: DeviceSeq(1),
                leases: vec![LeaseRef { id: held.id, epoch: held.epoch }],
                entries: vec![entry(&row, b"zombie", 1)],
            })
            .await
            .unwrap();
        assert_eq!(raw.admission_seq, AdmissionSeq(1));
    });
}

#[test]
fn heartbeat_rearms_the_local_deadline() {
    let (mut pool, flaky, clock) = rig();
    // One clock on both sides: client and server timelines move together.
    let mut session = Session::new(flaky, Arc::clone(&clock), dev(1));
    pool.run_until(async move {
        let db = key(&[b"db"]);
        let row = key(&[b"db", b"row"]);
        let acq = session.acquire(vec![wspec(&db, 100)], false).await.unwrap();
        let id = acq.leases[0];

        clock.advance(Duration::from_millis(60));
        let report = session.heartbeat().await.unwrap();
        assert_eq!(report.renewed, vec![id]);
        assert!(report.invalid.is_empty());
        assert_eq!(session.lease(id).unwrap().deadline(), Timestamp(160));

        // Past the original deadline, alive thanks to the renewal.
        clock.advance(Duration::from_millis(60));
        let outcome = session.put(vec![entry(&row, b"v", 1)]).await.unwrap();
        assert!(matches!(outcome, PutOutcome::Admitted(_)));

        // At the renewed deadline: strictly expired again.
        clock.advance(Duration::from_millis(40));
        let err = session.put(vec![entry(&row, b"v2", 2)]).await.unwrap_err();
        assert!(matches!(err, PutError::NotCovered { .. }));
    });
}

#[test]
fn heartbeat_drops_and_reports_server_side_expiry() {
    let (mut pool, flaky, clock) = rig();
    let mut session = Session::new(flaky, Arc::clone(&clock), dev(1));
    pool.run_until(async move {
        let db = key(&[b"db"]);
        let acq = session.acquire(vec![wspec(&db, 100)], false).await.unwrap();
        let id = acq.leases[0];

        // Both windows closed: the server reports the lease invalid, and
        // the session forgets it.
        clock.advance(Duration::from_millis(100));
        let report = session.heartbeat().await.unwrap();
        assert!(report.renewed.is_empty());
        assert_eq!(report.invalid, vec![id]);
        assert!(session.lease(id).is_none());
        assert_eq!(session.held().count(), 0);

        // The prefix is free again.
        session.acquire(vec![wspec(&db, 100)], false).await.unwrap();
    });
}

#[test]
fn heartbeat_surfaces_contention_from_a_waiting_device() {
    let (mut pool, flaky, clock) = rig();
    let mut holder = Session::new(flaky.clone(), Arc::clone(&clock), dev(1));
    let mut rival = Session::new(flaky, clock, dev(2));
    pool.run_until(async move {
        let db = key(&[b"db"]);
        let acq = holder.acquire(vec![wspec(&db, 1000)], false).await.unwrap();
        let id = acq.leases[0];

        let denied = rival.acquire(vec![wspec(&db, 1000)], false).await.unwrap_err();
        assert!(matches!(denied, SpaceError::Kernel(KernelError::Contended { .. })));

        // The demand piggybacks on the holder's next renewal.
        let report = holder.heartbeat().await.unwrap();
        assert_eq!(report.contended, vec![id]);
        assert!(holder.lease(id).unwrap().contended());
    });
}

#[test]
fn release_frees_the_prefix_and_forgets_the_hold() {
    let (mut pool, flaky, clock) = rig();
    let mut holder = Session::new(flaky.clone(), Arc::clone(&clock), dev(1));
    let mut rival = Session::new(flaky, clock, dev(2));
    pool.run_until(async move {
        let db = key(&[b"db"]);
        let acq = holder.acquire(vec![wspec(&db, 1000)], false).await.unwrap();
        holder.release(&acq.leases).await.unwrap();
        assert_eq!(holder.held().count(), 0);

        rival
            .acquire(vec![wspec(&db, 1000)], false)
            .await
            .expect("released prefix must be acquirable");
    });
}

#[test]
fn unavailable_puts_retry_within_budget() {
    let (mut pool, flaky, clock) = rig();
    let probe = flaky.clone();
    let mut session = Session::new(flaky, clock, dev(1)).with_retry_budget(3);
    pool.run_until(async move {
        let db = key(&[b"db"]);
        session.acquire(vec![wspec(&db, 1000)], false).await.unwrap();

        // Two lost requests, then delivery: the identical batch lands once.
        probe.fail_puts.store(2, Ordering::SeqCst);
        let row = key(&[b"db", b"row"]);
        let outcome = session.put(vec![entry(&row, b"v", 1)]).await.unwrap();
        assert_eq!(outcome, PutOutcome::Admitted(AdmissionSeq(1)));
        assert_eq!(probe.puts_seen(), 3, "one send plus two retries");
        assert_eq!(session.next_seq(), DeviceSeq(2));
    });
}

#[test]
fn exhausted_budget_surfaces_unavailable_and_the_seq_is_not_burned() {
    let (mut pool, flaky, clock) = rig();
    let probe = flaky.clone();
    let mut session = Session::new(flaky, clock, dev(1)).with_retry_budget(1);
    pool.run_until(async move {
        let db = key(&[b"db"]);
        session.acquire(vec![wspec(&db, 1000)], false).await.unwrap();

        probe.fail_puts.store(3, Ordering::SeqCst);
        let row = key(&[b"db", b"row"]);
        let err = session.put(vec![entry(&row, b"v", 1)]).await.unwrap_err();
        assert!(matches!(err, PutError::Space(SpaceError::Unavailable { .. })));
        assert_eq!(probe.puts_seen(), 2, "one send plus one retry");
        assert_eq!(session.next_seq(), DeviceSeq(1), "unknown outcome: seq not advanced");

        // The wire heals; the same seq is still admittable because nothing
        // ever reached the server.
        probe.fail_puts.store(0, Ordering::SeqCst);
        let outcome = session.put(vec![entry(&row, b"v", 1)]).await.unwrap();
        assert_eq!(outcome, PutOutcome::Admitted(AdmissionSeq(1)));
    });
}

/// The replay-fence contract end to end: the batch is admitted, the ack is
/// lost, the blind retry trips `DeviceSeqRegression`, and the session
/// reports the batch applied — exactly once, with the seq resynced.
#[test]
fn dropped_ack_resolves_to_already_applied() {
    let (mut pool, flaky, clock) = rig();
    let probe = flaky.clone();
    let mut session = Session::new(flaky, clock, dev(1)).with_retry_budget(DEFAULT_RETRY_BUDGET);
    pool.run_until(async move {
        let db = key(&[b"db"]);
        session.acquire(vec![wspec(&db, 1000)], false).await.unwrap();

        probe.drop_acks.store(1, Ordering::SeqCst);
        let row = key(&[b"db", b"row"]);
        let outcome = session.put(vec![entry(&row, b"once", 1)]).await.unwrap();
        assert_eq!(outcome, PutOutcome::AlreadyApplied);
        assert_eq!(session.next_seq(), DeviceSeq(2), "resynced past the admitted batch");

        // Applied exactly once, and the stream continues cleanly.
        let got = session.get(vec![row.clone()]).await.unwrap();
        assert_eq!(value_of(&got, 0), Some(b"once".to_vec()));
        let outcome = session.put(vec![entry(&row, b"twice", 2)]).await.unwrap();
        assert_eq!(outcome, PutOutcome::Admitted(AdmissionSeq(2)));
    });
}

/// A resume below the device's true high water is the *caller* violating
/// the sole-writer contract; the session surfaces it as an error rather
/// than guessing, but resyncs so the stream recovers on the next put.
#[test]
fn stale_resume_surfaces_regression_then_resyncs() {
    let (mut pool, flaky, clock) = rig();
    let mut first = Session::new(flaky.clone(), Arc::clone(&clock), dev(1));
    pool.run_until(async move {
        let db = key(&[b"db"]);
        let acq = first.acquire(vec![wspec(&db, 1000)], false).await.unwrap();
        let row = key(&[b"db", b"a"]);
        first.put(vec![entry(&row, b"v", 1)]).await.unwrap();
        first.release(&acq.leases).await.unwrap();

        // Same device resumed at a seq the server has already seen.
        let mut second = Session::resume(flaky, clock, dev(1), DeviceSeq(1));
        second.acquire(vec![wspec(&db, 1000)], false).await.unwrap();
        let other = key(&[b"db", b"b"]);
        let err = second.put(vec![entry(&other, b"w", 1)]).await.unwrap_err();
        assert!(matches!(
            err,
            PutError::Space(SpaceError::Kernel(KernelError::DeviceSeqRegression { .. }))
        ));
        assert_eq!(second.next_seq(), DeviceSeq(2), "resynced from the server's truth");

        let outcome = second.put(vec![entry(&other, b"w", 1)]).await.unwrap();
        assert_eq!(outcome, PutOutcome::Admitted(AdmissionSeq(2)));
    });
}

/// Losing a lease server-side (here: a steal of a stealable grant) shows up
/// on the next write; the session drops the hold so coverage stops
/// trusting it.
#[test]
fn server_side_lease_loss_is_dropped_on_put_failure() {
    let (mut pool, flaky, clock) = rig();
    let mut victim = Session::new(flaky.clone(), Arc::clone(&clock), dev(1));
    let mut thief = Session::new(flaky, clock, dev(2));
    pool.run_until(async move {
        let acct = key(&[b"acct"]);
        let spec = LeaseSpec {
            prefix: acct.clone(),
            mode: LeaseMode::Write,
            ttl: Duration::from_millis(1000),
            stealable: true,
        };
        let acq = victim.acquire(vec![spec.clone()], false).await.unwrap();
        let id = acq.leases[0];

        thief.acquire(vec![spec], true).await.expect("steal of a stealable grant");

        let row = key(&[b"acct", b"row"]);
        let err = victim.put(vec![entry(&row, b"v", 1)]).await.unwrap_err();
        assert!(matches!(
            err,
            PutError::Space(SpaceError::Kernel(KernelError::LeaseInvalid { .. }))
        ));
        assert!(victim.lease(id).is_none(), "refused lease must be forgotten");
    });
}

#[test]
fn reads_retry_blindly() {
    let (mut pool, flaky, clock) = rig();
    let probe = flaky.clone();
    let mut session = Session::new(flaky, clock, dev(1)).with_retry_budget(3);
    pool.run_until(async move {
        let db = key(&[b"db"]);
        session.acquire(vec![wspec(&db, 1000)], false).await.unwrap();
        let row = key(&[b"db", b"row"]);
        session.put(vec![entry(&row, b"v", 1)]).await.unwrap();

        probe.fail_reads.store(2, Ordering::SeqCst);
        let got = session.get(vec![row]).await.unwrap();
        assert_eq!(value_of(&got, 0), Some(b"v".to_vec()));
    });
}

#[test]
fn read_at_smoke_snapshot_then_delta() {
    let (mut pool, flaky, clock) = rig();
    let mut session = Session::new(flaky, clock, dev(1));
    pool.run_until(async move {
        let db = key(&[b"db"]);
        session.acquire(vec![wspec(&db, 1000)], false).await.unwrap();
        session.put(vec![entry(&key(&[b"db", b"a"]), b"1", 1)]).await.unwrap();
        session.put(vec![entry(&key(&[b"db", b"b"]), b"2", 1)]).await.unwrap();

        let cut = session
            .read_at(vec![PrefixCursor { prefix: db.clone(), since: None }])
            .await
            .unwrap();
        assert_eq!(cut.at, AdmissionSeq(2));
        match &cut.ranges[0] {
            RangeCut::Snapshot(entries) => assert_eq!(entries.len(), 2),
            RangeCut::Delta(_) => panic!("cursorless read must snapshot"),
        }

        session.put(vec![entry(&key(&[b"db", b"c"]), b"3", 1)]).await.unwrap();
        let next = session
            .read_at(vec![PrefixCursor { prefix: db, since: Some(cut.at) }])
            .await
            .unwrap();
        match &next.ranges[0] {
            RangeCut::Delta(entries) => assert_eq!(entries.len(), 1),
            RangeCut::Snapshot(_) => panic!("cursor must yield a delta"),
        }
    });
}

/// `Clock` is used generically by the session; this pins that a real
/// monotonic clock satisfies the same bounds (compile-time, mostly).
#[test]
fn session_accepts_a_real_clock() {
    let (mut pool, flaky, _server_clock) = rig();
    let clock = Arc::new(homebase_core::clock::MonotonicClock::new());
    let _ = clock.now();
    let mut session = Session::new(flaky, clock, dev(1));
    pool.run_until(async move {
        session
            .acquire(vec![wspec(&key(&[b"db"]), 60_000)], false)
            .await
            .unwrap();
        session
            .put(vec![entry(&key(&[b"db", b"k"]), b"v", 1)])
            .await
            .unwrap();
    });
}
