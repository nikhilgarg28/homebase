//! Engine tortures against a real in-process server: two hand-cranked
//! clocks (client and server timelines never compared), a shared
//! `MemoryStore` playing the client's disk so crashes are a drop-and-
//! reopen, and dead incarnations simulated by hand-shipping what they
//! would have sent. Every recovery path in the pusher's algebra gets a
//! deterministic run.

use homebase::engine::{Engine, EngineError, PushOutcome};
use homebase::meta::{MetaStore, OrderedMetaStore, audit};
use homebase::server::ServerHandle;
use homebase_core::clock::{HybridTimestamp, Lineage, ManualClock, Timestamp};
use homebase_core::key::Key;
use homebase_core::lease::{LeaseMode, LeaseRef};
use homebase_core::messages::{
    AcquireRequest, GetRequest, KernelError, LeaseSpec, PutBatchRequest, PutEntry, RangeCut,
    ReleaseRequest,
};
use homebase_core::space::SpaceId;
use homebase_core::storage::{MemoryStore, OrderedStore, WriteBatch, collect_scan};
use homebase_core::tag::{AdmissionSeq, DeviceId, DeviceSeq, Entry, Value, Ver};
use homebase_server::Server;
use homebase_server::actor::{SpaceHandle, Spawner};
use pollster::block_on;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

const SPACE: SpaceId = SpaceId([1; 16]);
const LINK: SpaceId = SpaceId([2; 16]);

/// A `Sync` spawner: each space actor gets a thread (same as the
/// ServerHandle conformance driver).
struct ThreadSpawner;

impl Spawner for ThreadSpawner {
    fn spawn(&self, task: Pin<Box<dyn Future<Output = ()> + Send + 'static>>) {
        std::thread::spawn(move || pollster::block_on(task));
    }
}

fn dev(n: u8) -> DeviceId {
    DeviceId([n; 16])
}

fn key(components: &[&[u8]]) -> Key {
    Key::from_bytes(components.iter().copied()).unwrap()
}

fn val(bytes: &[u8]) -> Value {
    Value::Present(bytes.to_vec())
}

fn wspec(prefix: &Key, secs: u64) -> LeaseSpec {
    LeaseSpec {
        prefix: prefix.clone(),
        mode: LeaseMode::Write,
        ttl: Duration::from_secs(secs),
        stealable: false,
    }
}

/// A real server behind the canonical closure handle. The closure owns
/// the `Arc<Server>`, so the server lives exactly as long as the handle.
fn spawn_server(
    clock: Arc<ManualClock>,
    spaces: &[SpaceId],
) -> impl Fn(&SpaceId) -> Option<SpaceHandle> + Sync {
    let server = Arc::new(Server::new(
        Arc::new(MemoryStore::new()),
        clock,
        ThreadSpawner,
    ));
    for space in spaces {
        assert!(server.create_space(*space));
    }
    move |id: &SpaceId| server.space(id)
}

async fn fetch(handle: &impl ServerHandle, space: SpaceId, k: &Key) -> Option<Entry> {
    handle
        .get(
            &space,
            GetRequest {
                keys: vec![k.clone()],
            },
        )
        .await
        .unwrap()
        .entries
        .remove(0)
}

/// What a *different* device does out of band: acquire, write, release.
async fn foreign_put(
    handle: &impl ServerHandle,
    space: SpaceId,
    device: DeviceId,
    prefix: &Key,
    entries: Vec<PutEntry>,
    seq: DeviceSeq,
) {
    let granted = handle
        .acquire(
            &space,
            AcquireRequest {
                device,
                specs: vec![wspec(prefix, 60)],
                steal: false,
            },
        )
        .await
        .expect("foreign acquire");
    let lease = LeaseRef {
        id: granted.leases[0].id,
        epoch: granted.leases[0].epoch,
    };
    handle
        .put_batch(
            &space,
            PutBatchRequest {
                device,
                device_seq: seq,
                leases: vec![lease],
                entries,
            },
        )
        .await
        .expect("foreign put");
    handle
        .release(
            &space,
            ReleaseRequest {
                device,
                leases: vec![lease.id],
            },
        )
        .await
        .expect("foreign release");
}

/// A hybrid stamp with both rulers at `ms`, on lineage `lin`.
/// `ManualClock` starts on lineage `[1; 16]`.
fn hstamp(ms: u64, lin: u8) -> HybridTimestamp {
    HybridTimestamp {
        wall: Timestamp(ms),
        mono: Timestamp(ms),
        lineage: Lineage([lin; 16]),
    }
}

/// The queue length, read from durable truth (the engine keeps no copy).
async fn queued(mem: &MemoryStore) -> usize {
    audit(&OrderedMetaStore::new(mem)).await.oplog.len()
}

/// Byte-for-byte copy of a store: the file-copy fork, made literal.
async fn clone_store(src: &MemoryStore) -> MemoryStore {
    let out = MemoryStore::new();
    let mut batch = WriteBatch::new();
    for (k, v) in collect_scan(src.scan(Vec::new(), None)).await.unwrap() {
        batch.put(k, v);
    }
    out.apply(batch).await.unwrap();
    out
}

#[test]
fn open_mints_identity_once() {
    block_on(async {
        let mem = MemoryStore::new();
        let clock = ManualClock::new(Timestamp(0));
        let handle = |_: &SpaceId| Option::<SpaceHandle>::None;

        let engine = Engine::open(OrderedMetaStore::new(&mem), &handle, &clock, dev(1))
            .await
            .unwrap();
        assert_eq!(engine.device(), dev(1));
        let _ = engine;

        // A later incarnation offers different randomness; the store wins.
        let engine = Engine::open(OrderedMetaStore::new(&mem), &handle, &clock, dev(2))
            .await
            .unwrap();
        assert_eq!(engine.device(), dev(1));
    });
}

#[test]
fn push_drains_and_groups_by_space() {
    block_on(async {
        let mem = MemoryStore::new();
        let clock = ManualClock::new(Timestamp(0));
        let handle = spawn_server(Arc::new(ManualClock::new(Timestamp(0))), &[SPACE, LINK]);
        let mut engine = Engine::open(OrderedMetaStore::new(&mem), &handle, &clock, dev(1))
            .await
            .unwrap();

        let db = key(&[b"db"]);
        let dir = key(&[b"dir"]);
        engine
            .acquire(SPACE, vec![wspec(&db, 60)], false)
            .await
            .unwrap();
        engine
            .acquire(LINK, vec![wspec(&dir, 60)], false)
            .await
            .unwrap();

        let (a1, a2, a3) = (
            key(&[b"db", b"a1"]),
            key(&[b"db", b"a2"]),
            key(&[b"db", b"a3"]),
        );
        let d = key(&[b"dir", b"d"]);
        engine
            .commit(SPACE, vec![(a1.clone(), val(b"1"))])
            .await
            .unwrap();
        engine
            .commit(SPACE, vec![(a2.clone(), val(b"2"))])
            .await
            .unwrap();
        engine
            .commit(LINK, vec![(d.clone(), val(b"3"))])
            .await
            .unwrap();
        engine
            .commit(SPACE, vec![(a3.clone(), val(b"4"))])
            .await
            .unwrap();
        assert_eq!(queued(&mem).await, 4);

        let outcome = engine.push().await.unwrap();
        assert_eq!(
            outcome,
            PushOutcome::Drained {
                acked_through: Some(DeviceSeq(4))
            }
        );
        assert_eq!(queued(&mem).await, 0);

        // Same-space neighbors merged and shipped under the group's LAST
        // seq; the space boundary split the stream.
        assert_eq!(
            fetch(&handle, SPACE, &a1).await.unwrap().tag.device_seq,
            DeviceSeq(2)
        );
        assert_eq!(
            fetch(&handle, SPACE, &a2).await.unwrap().tag.device_seq,
            DeviceSeq(2)
        );
        assert_eq!(
            fetch(&handle, LINK, &d).await.unwrap().tag.device_seq,
            DeviceSeq(3)
        );
        assert_eq!(
            fetch(&handle, SPACE, &a3).await.unwrap().tag.device_seq,
            DeviceSeq(4)
        );

        // The trim is durable, and a drained queue pushes as a no-op.
        let state = audit(&OrderedMetaStore::new(&mem)).await;
        assert!(state.oplog.is_empty());
        assert_eq!(state.next_seq, Some(DeviceSeq(5)));
        assert_eq!(
            engine.push().await.unwrap(),
            PushOutcome::Drained {
                acked_through: None
            }
        );
    });
}

#[test]
fn push_cap_splits_groups() {
    block_on(async {
        let mem = MemoryStore::new();
        let clock = ManualClock::new(Timestamp(0));
        let handle = spawn_server(Arc::new(ManualClock::new(Timestamp(0))), &[SPACE]);
        let mut engine = Engine::open(OrderedMetaStore::new(&mem), &handle, &clock, dev(1))
            .await
            .unwrap()
            .with_push_cap(1);

        let db = key(&[b"db"]);
        engine
            .acquire(SPACE, vec![wspec(&db, 60)], false)
            .await
            .unwrap();
        let (k1, k2) = (key(&[b"db", b"k1"]), key(&[b"db", b"k2"]));
        engine
            .commit(SPACE, vec![(k1.clone(), val(b"1"))])
            .await
            .unwrap();
        engine
            .commit(SPACE, vec![(k2.clone(), val(b"2"))])
            .await
            .unwrap();

        engine.push().await.unwrap();
        // At cap 1 nothing merges: each commit ships under its own seq.
        assert_eq!(
            fetch(&handle, SPACE, &k1).await.unwrap().tag.device_seq,
            DeviceSeq(1)
        );
        assert_eq!(
            fetch(&handle, SPACE, &k2).await.unwrap().tag.device_seq,
            DeviceSeq(2)
        );
    });
}

#[test]
fn acquire_satisfies_covered_specs_locally() {
    block_on(async {
        let mem = MemoryStore::new();
        let clock = ManualClock::new(Timestamp(0));
        let handle = spawn_server(Arc::new(ManualClock::new(Timestamp(0))), &[SPACE]);
        let mut engine = Engine::open(OrderedMetaStore::new(&mem), &handle, &clock, dev(1))
            .await
            .unwrap();

        let db = key(&[b"db"]);
        let first = engine
            .acquire(SPACE, vec![wspec(&db, 60)], false)
            .await
            .unwrap();
        assert_eq!(
            first.barrier, None,
            "no admitted writes means no catch-up barrier"
        );
        let lease = first.leases[0].clone();

        // Asking again changes nothing: same lease, same epoch, no wire
        // grant — and so no new catch-up obligation.
        let again = engine
            .acquire(SPACE, vec![wspec(&db, 60)], false)
            .await
            .unwrap();
        assert_eq!(again.leases, vec![lease.clone()]);
        assert_eq!(again.barrier, None);

        // A held write on the covering prefix satisfies a narrower read
        // spec too.
        let read_spec = LeaseSpec {
            prefix: key(&[b"db", b"sub"]),
            mode: LeaseMode::Read,
            ttl: Duration::from_secs(60),
            stealable: false,
        };
        let covered = engine.acquire(SPACE, vec![read_spec], false).await.unwrap();
        assert_eq!(covered.leases, vec![lease.clone()]);
        assert_eq!(covered.barrier, None);

        // Mixed: one satisfied spec, one genuinely new — only the new
        // one is acquired, and the answer stays parallel to the specs.
        let other = key(&[b"other"]);
        let mixed = engine
            .acquire(SPACE, vec![wspec(&db, 60), wspec(&other, 60)], false)
            .await
            .unwrap();
        assert_eq!(mixed.leases[0], lease);
        assert_eq!(mixed.leases[1].prefix, other);
        assert_ne!(mixed.leases[1].id, lease.id);
        assert_eq!(
            mixed.barrier, None,
            "the fresh half is on an empty timeline too"
        );

        // Local expiry doesn't force a re-grant: the engine revives the
        // held lease with a renewal — same lease, same fence, fresh
        // local window — because the kernel treats a same-device
        // re-acquire of a live lease as contention.
        clock.advance(Duration::from_secs(60));
        let revived = engine
            .acquire(SPACE, vec![wspec(&db, 60)], false)
            .await
            .unwrap();
        assert_eq!(revived.barrier, None);
        assert_eq!(revived.leases[0].id, lease.id, "renewed, not re-granted");
        assert_eq!(revived.leases[0].epoch, lease.epoch, "the fence stands");

        // And the revived lease actually backs writes again.
        engine
            .commit(SPACE, vec![(key(&[b"db", b"w"]), val(b"v"))])
            .await
            .unwrap();
        assert!(matches!(
            engine.push().await.unwrap(),
            PushOutcome::Drained { .. }
        ));
    });
}

#[test]
fn resume_keeps_wall_clock_authority() {
    block_on(async {
        let mem = MemoryStore::new();
        let clock = ManualClock::new(Timestamp(1_000));
        let handle = spawn_server(Arc::new(ManualClock::new(Timestamp(0))), &[SPACE]);

        let db = key(&[b"db"]);
        let k = key(&[b"db", b"k"]);
        let lease_id = {
            let mut engine = Engine::open(OrderedMetaStore::new(&mem), &handle, &clock, dev(1))
                .await
                .unwrap();
            let granted = engine
                .acquire(SPACE, vec![wspec(&db, 3_600)], false)
                .await
                .unwrap();
            engine
                .commit(SPACE, vec![(k.clone(), val(b"v"))])
                .await
                .unwrap();
            granted.leases[0].id
            // crash: the engine drops here, the store survives
        };

        // The grant was written through with its send-stamped deadline,
        // and the wall send stamp advanced the clock high-water.
        let state = audit(&OrderedMetaStore::new(&mem)).await;
        assert_eq!(
            state.spaces[&SPACE].leases[&lease_id].deadline,
            hstamp(1_000 + 3_600_000, 1)
        );
        assert_eq!(state.clock_high, Some(Timestamp(1_000)));

        // Five minutes later, a NEW process (new lineage) on the same
        // wall timeline: the monotonic component of the stamp is now
        // foreign, so liveness falls back to the wall reading — and the
        // restarted engine still holds the lease, pushing without any
        // renewal round trip. Offline authority survives restarts.
        clock.advance(Duration::from_secs(300));
        clock.set_lineage(Lineage([2; 16]));
        let mut engine = Engine::open(OrderedMetaStore::new(&mem), &handle, &clock, dev(9))
            .await
            .unwrap();
        assert_eq!(engine.device(), dev(1));
        let leases = engine
            .leases(SPACE, std::slice::from_ref(&db))
            .await
            .unwrap();
        assert_eq!(leases.len(), 1);
        assert!(leases[0].live, "the wall fallback outlives the process");
        assert_eq!(
            engine.push().await.unwrap(),
            PushOutcome::Drained {
                acked_through: Some(DeviceSeq(1))
            }
        );
        assert_eq!(fetch(&handle, SPACE, &k).await.unwrap().value, val(b"v"));

        // Real expiry still ends it: past the deadline the engine
        // refuses coverage, and renewal is the cure.
        clock.advance(Duration::from_secs(3600));
        assert!(matches!(
            engine
                .commit(SPACE, vec![(k.clone(), val(b"later"))])
                .await
                .unwrap_err(),
            EngineError::LocalAuthority { .. }
        ));
        let renewed = engine
            .renew(SPACE, std::slice::from_ref(&db))
            .await
            .unwrap();
        assert_eq!(renewed.granted.len(), 1);
        engine
            .commit(SPACE, vec![(k.clone(), val(b"later"))])
            .await
            .unwrap();
        assert_eq!(
            engine.push().await.unwrap(),
            PushOutcome::Drained {
                acked_through: Some(DeviceSeq(2))
            }
        );
    });
}

#[test]
fn margin_applies_only_across_incarnations() {
    block_on(async {
        let mem = MemoryStore::new();
        let clock = ManualClock::new(Timestamp(0));
        let handle = spawn_server(Arc::new(ManualClock::new(Timestamp(0))), &[SPACE]);

        let db = key(&[b"db"]);
        {
            let mut engine = Engine::open(OrderedMetaStore::new(&mem), &handle, &clock, dev(1))
                .await
                .unwrap();
            engine
                .acquire(SPACE, vec![wspec(&db, 60)], false)
                .await
                .unwrap();
            engine
                .commit(SPACE, vec![(key(&[b"db", b"k"]), val(b"v"))])
                .await
                .unwrap();

            // Two milliseconds shy of the deadline, judged by the
            // process that stamped it: the monotonic ruler is precise,
            // no margin shaves it — the full window is usable.
            clock.set(Timestamp(60_000 - 2));
            assert_eq!(
                engine.push().await.unwrap(),
                PushOutcome::Drained {
                    acked_through: Some(DeviceSeq(1))
                }
            );
        }

        // The same reading judged by a NEW incarnation: the monotonic
        // component is foreign, the wall fallback applies its margin —
        // 0.1% of the 60s TTL, 60ms — and two milliseconds shy is
        // already retired.
        clock.set_lineage(Lineage([2; 16]));
        let mut engine = Engine::open(OrderedMetaStore::new(&mem), &handle, &clock, dev(9))
            .await
            .unwrap();
        assert!(
            !engine
                .leases(SPACE, std::slice::from_ref(&db))
                .await
                .unwrap()[0]
                .live
        );
        // The exact boundary: live until deadline − 60ms, not after.
        clock.set(Timestamp(59_939));
        assert!(
            engine
                .leases(SPACE, std::slice::from_ref(&db))
                .await
                .unwrap()[0]
                .live
        );
        clock.set(Timestamp(59_940));
        assert!(
            !engine
                .leases(SPACE, std::slice::from_ref(&db))
                .await
                .unwrap()[0]
                .live
        );
        clock.set(Timestamp(60_000 - 2));
        assert!(matches!(
            engine
                .commit(SPACE, vec![(key(&[b"db", b"k2"]), val(b"v2"))])
                .await
                .unwrap_err(),
            EngineError::LocalAuthority { .. }
        ));

        engine
            .renew(SPACE, std::slice::from_ref(&db))
            .await
            .unwrap();
        engine
            .commit(SPACE, vec![(key(&[b"db", b"k2"]), val(b"v2"))])
            .await
            .unwrap();
        assert_eq!(
            engine.push().await.unwrap(),
            PushOutcome::Drained {
                acked_through: Some(DeviceSeq(2))
            }
        );
    });
}

#[test]
fn suspend_expires_leases_within_a_lineage() {
    block_on(async {
        let mem = MemoryStore::new();
        let clock = ManualClock::new(Timestamp(0));
        let handle = spawn_server(Arc::new(ManualClock::new(Timestamp(0))), &[SPACE]);
        let mut engine = Engine::open(OrderedMetaStore::new(&mem), &handle, &clock, dev(1))
            .await
            .unwrap();

        let db = key(&[b"db"]);
        engine
            .acquire(SPACE, vec![wspec(&db, 60)], false)
            .await
            .unwrap();
        engine
            .commit(SPACE, vec![(key(&[b"db", b"k"]), val(b"v"))])
            .await
            .unwrap();

        // The laptop sleeps for an hour: real time passes, the process's
        // monotonic ruler does not see it. Same lineage — but expiry
        // takes the earlier verdict of the two rulers, and the wall one
        // knows the lease is long gone.
        clock.skew_wall(Duration::from_secs(3_600));
        assert!(
            !engine
                .leases(SPACE, std::slice::from_ref(&db))
                .await
                .unwrap()[0]
                .live
        );
        let outcome = engine.push().await.unwrap();
        assert!(
            matches!(
                &outcome,
                PushOutcome::Stalled {
                    error: KernelError::NotCovered { .. },
                    ..
                }
            ),
            "a slept-through lease must not back a write, got {outcome:?}"
        );

        engine
            .renew(SPACE, std::slice::from_ref(&db))
            .await
            .unwrap();
        assert_eq!(
            engine.push().await.unwrap(),
            PushOutcome::Drained {
                acked_through: Some(DeviceSeq(1))
            }
        );
    });
}

#[test]
fn backward_clock_step_poisons_stored_stamps() {
    block_on(async {
        let mem = MemoryStore::new();
        let clock = ManualClock::new(Timestamp(10_000));
        let handle = spawn_server(Arc::new(ManualClock::new(Timestamp(0))), &[SPACE]);

        let db = key(&[b"db"]);
        let k = key(&[b"db", b"k"]);
        {
            let mut engine = Engine::open(OrderedMetaStore::new(&mem), &handle, &clock, dev(1))
                .await
                .unwrap();
            engine
                .acquire(SPACE, vec![wspec(&db, 60)], false)
                .await
                .unwrap();
            engine
                .commit(SPACE, vec![(k.clone(), val(b"v"))])
                .await
                .unwrap();
        }

        // The wall clock is set BACK while the process is dead. The
        // reopened engine (a new lineage) reads a wall behind the
        // recorded high-water: every stored stamp predates a step it
        // cannot size, so all of them die structurally, and the
        // high-water re-anchors.
        clock.set(Timestamp(2_000));
        clock.set_lineage(Lineage([2; 16]));
        let mut engine = Engine::open(OrderedMetaStore::new(&mem), &handle, &clock, dev(9))
            .await
            .unwrap();
        let state = audit(&OrderedMetaStore::new(&mem)).await;
        assert_eq!(
            state.spaces[&SPACE]
                .leases
                .values()
                .next()
                .unwrap()
                .deadline,
            HybridTimestamp::ZERO
        );
        assert_eq!(state.clock_high, Some(Timestamp(2_000)));
        assert!(
            !engine
                .leases(SPACE, std::slice::from_ref(&db))
                .await
                .unwrap()[0]
                .live
        );

        // Zero-stamped means no authority: the push stalls. Renewal
        // re-stamps on the new timeline — conservative by construction —
        // and authority resumes.
        let outcome = engine.push().await.unwrap();
        assert!(
            matches!(
                &outcome,
                PushOutcome::Stalled {
                    error: KernelError::NotCovered { .. },
                    ..
                }
            ),
            "poisoned stamps must not back writes, got {outcome:?}"
        );
        engine
            .renew(SPACE, std::slice::from_ref(&db))
            .await
            .unwrap();
        assert_eq!(
            audit(&OrderedMetaStore::new(&mem)).await.spaces[&SPACE]
                .leases
                .values()
                .next()
                .unwrap()
                .deadline,
            hstamp(2_000 + 60_000, 2)
        );
        assert_eq!(
            engine.push().await.unwrap(),
            PushOutcome::Drained {
                acked_through: Some(DeviceSeq(1))
            }
        );
        let _ = engine;

        // Poison does not linger: a later open on the healed timeline
        // keeps the renewed stamp alive.
        clock.advance(Duration::from_secs(10));
        let engine = Engine::open(OrderedMetaStore::new(&mem), &handle, &clock, dev(9))
            .await
            .unwrap();
        assert!(
            engine
                .leases(SPACE, std::slice::from_ref(&db))
                .await
                .unwrap()[0]
                .live
        );
    });
}

#[test]
fn local_expiry_gates_writes_before_the_server_does() {
    block_on(async {
        let mem = MemoryStore::new();
        let clock = ManualClock::new(Timestamp(0));
        let server_clock = Arc::new(ManualClock::new(Timestamp(0)));
        let handle = spawn_server(Arc::clone(&server_clock), &[SPACE]);
        let mut engine = Engine::open(OrderedMetaStore::new(&mem), &handle, &clock, dev(1))
            .await
            .unwrap();

        let db = key(&[b"db"]);
        let k = key(&[b"db", b"k"]);
        let granted = engine
            .acquire(SPACE, vec![wspec(&db, 60)], false)
            .await
            .unwrap();
        let _lease_id = granted.leases[0].id;
        engine
            .commit(SPACE, vec![(k.clone(), val(b"v"))])
            .await
            .unwrap();

        // Only the CLIENT clock reaches the deadline; the server still
        // holds the lease live. The engine must refuse first — that
        // asymmetry is the whole two-clock rule.
        clock.advance(Duration::from_secs(60));
        let outcome = engine.push().await.unwrap();
        assert!(
            matches!(
                &outcome,
                PushOutcome::Stalled {
                    error: KernelError::NotCovered { .. },
                    ..
                }
            ),
            "a locally-expired lease must never back a write, got {outcome:?}"
        );

        // Renewal restarts the local window from this send.
        engine
            .renew(SPACE, std::slice::from_ref(&db))
            .await
            .unwrap();
        let leases = engine
            .leases(SPACE, std::slice::from_ref(&db))
            .await
            .unwrap();
        assert_eq!(leases[0].held.deadline, hstamp(60_000 + 60_000, 1));
        assert_eq!(
            engine.push().await.unwrap(),
            PushOutcome::Drained {
                acked_through: Some(DeviceSeq(1))
            }
        );
    });
}

#[test]
fn seq_collision_recovers_a_dead_incarnations_send_exactly_once() {
    block_on(async {
        let mem = MemoryStore::new();
        let clock = ManualClock::new(Timestamp(0));
        let handle = spawn_server(Arc::new(ManualClock::new(Timestamp(0))), &[SPACE]);
        let mut engine = Engine::open(OrderedMetaStore::new(&mem), &handle, &clock, dev(1))
            .await
            .unwrap();

        let db = key(&[b"db"]);
        let (k1, k2) = (key(&[b"db", b"k1"]), key(&[b"db", b"k2"]));
        let granted = engine
            .acquire(SPACE, vec![wspec(&db, 60)], false)
            .await
            .unwrap();
        let lease = LeaseRef {
            id: granted.leases[0].id,
            epoch: granted.leases[0].epoch,
        };
        engine
            .commit(SPACE, vec![(k1.clone(), val(b"one"))])
            .await
            .unwrap();
        engine
            .commit(SPACE, vec![(k2.clone(), val(b"two"))])
            .await
            .unwrap();

        // The dead incarnation's send: the same group, shipped under the
        // group's last seq, admitted — and then the crash ate the trim.
        handle
            .put_batch(
                &SPACE,
                PutBatchRequest {
                    device: engine.device(),
                    device_seq: DeviceSeq(2),
                    leases: vec![lease],
                    entries: vec![
                        PutEntry {
                            key: k1.clone(),
                            value: val(b"one"),
                            ver: Ver(1),
                        },
                        PutEntry {
                            key: k2.clone(),
                            value: val(b"two"),
                            ver: Ver(2),
                        },
                    ],
                },
            )
            .await
            .expect("the dead incarnation's send was admitted");

        // The resend collides, the collision names the admitted extent,
        // the trim happens, and nothing is applied twice.
        assert_eq!(
            engine.push().await.unwrap(),
            PushOutcome::Drained {
                acked_through: Some(DeviceSeq(2))
            }
        );
        assert_eq!(queued(&mem).await, 0);
        let entry = fetch(&handle, SPACE, &k1).await.unwrap();
        assert_eq!(entry.value, val(b"one"));
        assert_eq!(entry.tag.ver, Ver(1), "admitted exactly once, no replay");
        assert!(audit(&OrderedMetaStore::new(&mem)).await.oplog.is_empty());
    });
}

#[test]
fn group_rejection_probes_to_the_faulty_commit() {
    block_on(async {
        let mem = MemoryStore::new();
        let clock = ManualClock::new(Timestamp(0));
        let handle = spawn_server(Arc::new(ManualClock::new(Timestamp(0))), &[SPACE]);

        // A foreign device already wrote k at ver 100 …
        let db = key(&[b"db"]);
        let (x, k, y) = (
            key(&[b"db", b"x"]),
            key(&[b"db", b"k"]),
            key(&[b"db", b"y"]),
        );
        foreign_put(
            &handle,
            SPACE,
            dev(2),
            &db,
            vec![PutEntry {
                key: k.clone(),
                value: val(b"foreign"),
                ver: Ver(100),
            }],
            DeviceSeq(1),
        )
        .await;

        // … and this engine commits against k blindly (it never pulled):
        // the middle commit of three is genuinely faulty.
        let mut engine = Engine::open(OrderedMetaStore::new(&mem), &handle, &clock, dev(1))
            .await
            .unwrap();
        engine
            .acquire(SPACE, vec![wspec(&db, 60)], false)
            .await
            .unwrap();
        // Simulate a buggy caller that marks the acquire barrier satisfied
        // without importing the foreign value's ver. The pusher still
        // degrades a group rejection into the faulty solo commit.
        OrderedMetaStore::new(&mem)
            .advance_watermark(SPACE, AdmissionSeq(1), Ver(0))
            .await
            .unwrap();
        engine
            .commit(SPACE, vec![(x.clone(), val(b"ok"))])
            .await
            .unwrap();
        engine
            .commit(SPACE, vec![(k.clone(), val(b"stale"))])
            .await
            .unwrap();
        engine
            .commit(SPACE, vec![(y.clone(), val(b"after"))])
            .await
            .unwrap();

        // The merged group bounces; solo probes admit the healthy head
        // and convict exactly the faulty seq.
        let outcome = engine.push().await.unwrap();
        match &outcome {
            PushOutcome::Stalled {
                at,
                error,
                acked_through,
            } => {
                assert_eq!(*at, DeviceSeq(2));
                assert!(
                    matches!(error, KernelError::VerRegression { .. }),
                    "expected a ver conviction, got {error:?}"
                );
                assert_eq!(*acked_through, Some(DeviceSeq(1)));
            }
            other => panic!("expected a conviction, got {other:?}"),
        }
        // The rollback: the convicted commit falls, and everything after
        // it falls too (it may have read what the fault wrote).
        engine.discard_from(DeviceSeq(2)).await.unwrap();
        assert_eq!(queued(&mem).await, 0);
        assert_eq!(
            engine.push().await.unwrap(),
            PushOutcome::Drained {
                acked_through: None
            }
        );

        assert_eq!(fetch(&handle, SPACE, &x).await.unwrap().value, val(b"ok"));
        let foreign = fetch(&handle, SPACE, &k).await.unwrap();
        assert_eq!(foreign.value, val(b"foreign"));
        assert_eq!(foreign.tag.ver, Ver(100), "the stale write never landed");
        assert!(
            fetch(&handle, SPACE, &y).await.is_none(),
            "the suffix rolled back"
        );
        assert!(audit(&OrderedMetaStore::new(&mem)).await.oplog.is_empty());
    });
}

#[test]
fn a_forked_store_is_fatal() {
    block_on(async {
        let mem = MemoryStore::new();
        let clock = ManualClock::new(Timestamp(0));
        let handle = spawn_server(Arc::new(ManualClock::new(Timestamp(0))), &[SPACE]);

        let db = key(&[b"db"]);
        let (k1, k2) = (key(&[b"db", b"k1"]), key(&[b"db", b"k2"]));
        let mut engine = Engine::open(OrderedMetaStore::new(&mem), &handle, &clock, dev(1))
            .await
            .unwrap();
        let granted = engine
            .acquire(SPACE, vec![wspec(&db, 60)], false)
            .await
            .unwrap();
        let _lease_id = granted.leases[0].id;
        engine
            .commit(SPACE, vec![(k1.clone(), val(b"a"))])
            .await
            .unwrap();

        // The file copy comes alive: a twin loads the same identity —
        // and, on the shared wall timeline, the same live authority.
        // Nothing distinguishes it until the seqs collide.
        let twin_mem = clone_store(&mem).await;
        let mut twin = Engine::open(OrderedMetaStore::new(&twin_mem), &handle, &clock, dev(7))
            .await
            .unwrap();
        assert_eq!(twin.device(), dev(1), "the copy carries the identity");
        twin.commit(SPACE, vec![(k2.clone(), val(b"twin"))])
            .await
            .unwrap();
        assert_eq!(
            twin.push().await.unwrap(),
            PushOutcome::Drained {
                acked_through: Some(DeviceSeq(2))
            }
        );

        // The original's push collides with a seq PAST its own mint
        // counter — proof it isn't looking at its own past. Fatal, and
        // nothing is destroyed.
        assert_eq!(
            engine.push().await.unwrap_err(),
            EngineError::Fork {
                admitted: DeviceSeq(2)
            }
        );
        assert_eq!(queued(&mem).await, 1, "a fork verdict destroys nothing");
    });
}

#[test]
fn pull_advances_the_watermark_and_dominates_foreign_vers() {
    block_on(async {
        let mem = MemoryStore::new();
        let clock = ManualClock::new(Timestamp(0));
        let handle = spawn_server(Arc::new(ManualClock::new(Timestamp(0))), &[SPACE]);

        let db = key(&[b"db"]);
        let k = key(&[b"db", b"k"]);
        foreign_put(
            &handle,
            SPACE,
            dev(2),
            &db,
            vec![PutEntry {
                key: k.clone(),
                value: val(b"foreign"),
                ver: Ver(7),
            }],
            DeviceSeq(1),
        )
        .await;

        let mut engine = Engine::open(OrderedMetaStore::new(&mem), &handle, &clock, dev(1))
            .await
            .unwrap();
        let granted = engine
            .acquire(SPACE, vec![wspec(&db, 60)], false)
            .await
            .unwrap();
        assert!(matches!(
            engine
                .commit(SPACE, vec![(k.clone(), val(b"too-soon"))])
                .await
                .unwrap_err(),
            EngineError::LocalAuthority { .. }
        ));

        // The acquire-barrier discipline: pull to the barrier before
        // trusting local state. The pull is a snapshot (no cursor yet)
        // and raises the ver high-water past everything it saw.
        let pulled = engine.pull(SPACE, std::slice::from_ref(&db)).await.unwrap();
        assert!(pulled.at >= granted.barrier.unwrap());
        assert!(matches!(&pulled.ranges[0], RangeCut::Snapshot(entries) if entries.len() == 1));
        assert_eq!(
            audit(&OrderedMetaStore::new(&mem)).await.spaces[&SPACE].watermark,
            Some(pulled.at)
        );

        // Now the same key can be overwritten: the commit stamps above
        // the pulled ver, so the server's chain accepts it.
        engine
            .commit(SPACE, vec![(k.clone(), val(b"mine"))])
            .await
            .unwrap();
        assert_eq!(
            engine.push().await.unwrap(),
            PushOutcome::Drained {
                acked_through: Some(DeviceSeq(1))
            }
        );
        let entry = fetch(&handle, SPACE, &k).await.unwrap();
        assert_eq!(entry.value, val(b"mine"));
        assert_eq!(entry.tag.ver, Ver(8), "stamped past the foreign chain");

        // The next pull is a delta from the stored cursor and carries
        // exactly our own admitted write.
        let pulled = engine.pull(SPACE, std::slice::from_ref(&db)).await.unwrap();
        assert!(matches!(&pulled.ranges[0], RangeCut::Delta(entries) if entries.len() == 1));

        // The cursor is durable: a resumed incarnation pulls deltas, not
        // snapshots.
        let state = audit(&OrderedMetaStore::new(&mem)).await;
        assert_eq!(state.spaces[&SPACE].watermark, Some(pulled.at));
    });
}

#[test]
fn pending_release_blocks_writes_and_finishes_on_reopen() {
    block_on(async {
        let mem = MemoryStore::new();
        let clock = ManualClock::new(Timestamp(0));
        let handle = spawn_server(Arc::new(ManualClock::new(Timestamp(0))), &[SPACE]);

        let db = key(&[b"db"]);
        let lease_id = {
            let mut engine = Engine::open(OrderedMetaStore::new(&mem), &handle, &clock, dev(1))
                .await
                .unwrap();
            let granted = engine
                .acquire(SPACE, vec![wspec(&db, 60)], false)
                .await
                .unwrap();
            granted.leases[0].id
        };

        let offline = |_: &SpaceId| Option::<SpaceHandle>::None;
        let mut engine = Engine::open(OrderedMetaStore::new(&mem), &offline, &clock, dev(1))
            .await
            .unwrap();
        assert!(matches!(
            engine.release(SPACE, &[lease_id]).await.unwrap_err(),
            EngineError::Unavailable { .. }
        ));
        let state = audit(&OrderedMetaStore::new(&mem)).await;
        assert!(state.spaces[&SPACE].leases[&lease_id].retiring);
        assert!(matches!(
            engine
                .commit(SPACE, vec![(key(&[b"db", b"k"]), val(b"v"))])
                .await
                .unwrap_err(),
            EngineError::LocalAuthority { .. }
        ));

        // Reopening with a server resumes the release saga and drops the
        // local record; a new device can acquire immediately.
        let engine = Engine::open(OrderedMetaStore::new(&mem), &handle, &clock, dev(1))
            .await
            .unwrap();
        assert!(
            engine
                .leases(SPACE, std::slice::from_ref(&db))
                .await
                .unwrap()
                .is_empty()
        );
        let other = handle
            .acquire(
                &SPACE,
                AcquireRequest {
                    device: dev(2),
                    specs: vec![wspec(&db, 60)],
                    steal: false,
                },
            )
            .await
            .unwrap();
        assert_eq!(other.leases[0].prefix, db);
    });
}

#[test]
fn unavailable_leaves_the_queue_intact() {
    block_on(async {
        let mem = MemoryStore::new();
        let clock = ManualClock::new(Timestamp(0));
        let served = spawn_server(Arc::new(ManualClock::new(Timestamp(0))), &[SPACE]);
        let mut engine = Engine::open(OrderedMetaStore::new(&mem), &served, &clock, dev(1))
            .await
            .unwrap();
        let db = key(&[b"db"]);
        engine
            .acquire(SPACE, vec![wspec(&db, 60)], false)
            .await
            .unwrap();
        drop(engine);

        let handle = |_: &SpaceId| Option::<SpaceHandle>::None;
        let mut engine = Engine::open(OrderedMetaStore::new(&mem), &handle, &clock, dev(1))
            .await
            .unwrap();

        engine
            .commit(SPACE, vec![(key(&[b"db", b"k"]), val(b"v"))])
            .await
            .unwrap();
        assert!(matches!(
            engine.push().await.unwrap_err(),
            EngineError::Unavailable { .. }
        ));
        assert_eq!(queued(&mem).await, 1, "transport failure judges nothing");
    });
}

#[test]
fn renew_reports_invalid_and_forgets() {
    block_on(async {
        let mem = MemoryStore::new();
        let clock = ManualClock::new(Timestamp(0));
        let server_clock = Arc::new(ManualClock::new(Timestamp(0)));
        let handle = spawn_server(Arc::clone(&server_clock), &[SPACE]);
        let mut engine = Engine::open(OrderedMetaStore::new(&mem), &handle, &clock, dev(1))
            .await
            .unwrap();

        let db = key(&[b"db"]);
        let granted = engine
            .acquire(SPACE, vec![wspec(&db, 60)], false)
            .await
            .unwrap();
        let lease_id = granted.leases[0].id;

        // The SERVER's clock passes the deadline: strict local expiry,
        // the lease is gone there. Renewal is how this side finds out.
        server_clock.advance(Duration::from_secs(120));
        let renewed = engine
            .renew(SPACE, std::slice::from_ref(&db))
            .await
            .unwrap();
        assert_eq!(renewed.invalid, vec![lease_id]);
        assert!(renewed.granted.is_empty());

        // Forgotten everywhere: memory and the durable record.
        assert!(
            engine
                .leases(SPACE, std::slice::from_ref(&db))
                .await
                .unwrap()
                .is_empty()
        );
        let state = audit(&OrderedMetaStore::new(&mem)).await;
        assert!(state.spaces.get(&SPACE).is_none_or(|s| s.leases.is_empty()));
    });
}
