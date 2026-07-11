//! Engine tortures against a real in-process server: two hand-cranked
//! clocks (client and server timelines never compared), a shared
//! `MemoryStore` playing the client's disk so crashes are a drop-and-
//! reopen, and dead incarnations simulated by hand-shipping what they
//! would have sent. Every recovery path in the pusher's algebra gets a
//! deterministic run.

use homebase::cipher::{SpaceEnvelope, SystemNonceSource};
use homebase::meta::{MetaStore, OrderedMetaStore, audit};
use homebase::server::ServerHandle;
use homebase::{Client, ClientError, PushOutcome, SpaceDriverError};
use homebase_core::clock::{HybridClock, HybridTimestamp, Lineage, ManualClock, Timestamp};
use homebase_core::key::Key;
use homebase_core::lease::LeaseMode;
use homebase_core::messages::{
    AcquireRequest, GetRequest, KernelError, LeaseSpec, PutBatch, PutBatchRequest, PutEntry, Range,
    RangeAssert, RangeAssertFailure, RangeCut, ReleaseRequest,
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
const OTHER_SPACE: SpaceId = SpaceId([2; 16]);

async fn open_client<M, H, C>(
    store: M,
    server: H,
    clock: C,
    fresh: DeviceId,
) -> Result<Client<M, H, C, SystemNonceSource>, ClientError>
where
    M: MetaStore,
    H: ServerHandle,
    C: HybridClock,
{
    Client::open(store, server, clock, fresh, SystemNonceSource).await
}

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
    }
}

fn rspec(prefix: &Key, secs: u64) -> LeaseSpec {
    LeaseSpec {
        prefix: prefix.clone(),
        mode: LeaseMode::Read,
        ttl: Duration::from_secs(secs),
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
                requested_at: HybridTimestamp::ZERO,
                specs: vec![wspec(prefix, 60)],
            },
        )
        .await
        .expect("foreign acquire");
    let lease = granted.leases[0].id;
    handle
        .put_batch(
            &space,
            PutBatchRequest {
                device,
                evidence: vec![lease],
                batches: vec![PutBatch {
                    device_seq: seq,
                    range_asserts: vec![],
                    ops: entries.into_iter().map(Into::into).collect(),
                }],
            },
        )
        .await
        .expect("foreign put");
    handle
        .release(
            &space,
            ReleaseRequest {
                device,
                leases: vec![lease],
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
    audit(&OrderedMetaStore::new(mem))
        .await
        .spaces
        .values()
        .map(|space| space.oplog.len())
        .sum()
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

        let client = open_client(OrderedMetaStore::new(&mem), &handle, &clock, dev(1))
            .await
            .unwrap();
        assert_eq!(client.device(), dev(1));
        let _ = client;

        // A later incarnation offers different randomness; the store wins.
        let client = open_client(OrderedMetaStore::new(&mem), &handle, &clock, dev(2))
            .await
            .unwrap();
        assert_eq!(client.device(), dev(1));
    });
}

#[test]
fn checked_submit_persists_lease_backed_range_assert() {
    block_on(async {
        let mem = MemoryStore::new();
        let clock = ManualClock::new(Timestamp(0));
        let handle = spawn_server(Arc::new(ManualClock::new(Timestamp(0))), &[SPACE]);
        let db = key(&[b"db"]);
        foreign_put(
            &handle,
            SPACE,
            dev(2),
            &db,
            vec![PutEntry {
                key: key(&[b"db", b"sibling"]),
                value: val(b"foreign"),
                ver: Ver(1),
            }],
            DeviceSeq(1),
        )
        .await;
        let client = open_client(OrderedMetaStore::new(&mem), &handle, &clock, dev(1))
            .await
            .unwrap();
        client
            .attach(&SpaceEnvelope::plaintext(SPACE))
            .await
            .unwrap();
        let space = client.space(SPACE).await.unwrap();
        space.ensure(vec![rspec(&db, 60)]).await.unwrap();
        let target = key(&[b"db", b"target"]);
        assert_eq!(
            OrderedMetaStore::new(&mem)
                .watermark(SPACE, &Range::Prefix(target.clone()))
                .await
                .unwrap(),
            Some(AdmissionSeq(1)),
            "the parent pull covers the child through the global cut"
        );

        let submission = space
            .submit_checked(
                vec![(key(&[b"db", b"target", b"row"]), val(b"value"))],
                vec![RangeAssert {
                    prefix: target.clone(),
                    upto: AdmissionSeq(0),
                }],
            )
            .await
            .unwrap();

        assert_eq!(submission.seq, DeviceSeq(1));
        let state = audit(&OrderedMetaStore::new(&mem)).await;
        let record = &state.spaces[&SPACE].oplog[&submission.seq];
        assert_eq!(record.range_asserts().len(), 1);
        assert_eq!(record.range_asserts()[0].prefix, target);
        assert_eq!(record.ops()[0].ver(), Some(Ver(2)));
        assert_eq!(
            client.push().await.unwrap(),
            PushOutcome::Drained {
                acked_through: Some(DeviceSeq(1))
            }
        );
    });
}

#[test]
fn dependent_asserting_submissions_push_across_request_boundaries() {
    block_on(async {
        let mem = MemoryStore::new();
        let clock = ManualClock::new(Timestamp(0));
        let handle = spawn_server(Arc::new(ManualClock::new(Timestamp(0))), &[SPACE]);
        let client = open_client(OrderedMetaStore::new(&mem), &handle, &clock, dev(1))
            .await
            .unwrap();
        client.with_push_cap(1);
        client
            .attach(&SpaceEnvelope::plaintext(SPACE))
            .await
            .unwrap();
        let space = client.space(SPACE).await.unwrap();
        let db = key(&[b"db"]);
        space.ensure(vec![rspec(&db, 60)]).await.unwrap();

        for name in [&b"one"[..], &b"two"[..]] {
            space
                .submit_checked(
                    vec![(key(&[b"db", name]), val(name))],
                    vec![RangeAssert {
                        prefix: db.clone(),
                        upto: AdmissionSeq(0),
                    }],
                )
                .await
                .unwrap();
        }

        assert_eq!(
            client.push().await.unwrap(),
            PushOutcome::Drained {
                acked_through: Some(DeviceSeq(2))
            }
        );
    });
}

#[test]
fn dependent_asserting_submissions_push_when_coalesced() {
    block_on(async {
        let mem = MemoryStore::new();
        let clock = ManualClock::new(Timestamp(0));
        let handle = spawn_server(Arc::new(ManualClock::new(Timestamp(0))), &[SPACE]);
        let client = open_client(OrderedMetaStore::new(&mem), &handle, &clock, dev(1))
            .await
            .unwrap();
        client
            .attach(&SpaceEnvelope::plaintext(SPACE))
            .await
            .unwrap();
        let space = client.space(SPACE).await.unwrap();
        let db = key(&[b"db"]);
        space.ensure(vec![rspec(&db, 60)]).await.unwrap();

        for name in [&b"one"[..], &b"two"[..]] {
            space
                .submit_checked(
                    vec![(key(&[b"db", name]), val(name))],
                    vec![RangeAssert {
                        prefix: db.clone(),
                        upto: AdmissionSeq(0),
                    }],
                )
                .await
                .unwrap();
        }

        assert_eq!(
            client.push().await.unwrap(),
            PushOutcome::Drained {
                acked_through: Some(DeviceSeq(2))
            }
        );
    });
}

#[test]
fn foreign_write_after_upto_stalls_and_keeps_submission_queued() {
    block_on(async {
        let mem = MemoryStore::new();
        let clock = ManualClock::new(Timestamp(0));
        let server_clock = Arc::new(ManualClock::new(Timestamp(0)));
        let handle = spawn_server(Arc::clone(&server_clock), &[SPACE]);
        let client = open_client(OrderedMetaStore::new(&mem), &handle, &clock, dev(1))
            .await
            .unwrap();
        client
            .attach(&SpaceEnvelope::plaintext(SPACE))
            .await
            .unwrap();
        let space = client.space(SPACE).await.unwrap();
        let db = key(&[b"db"]);
        space.ensure(vec![rspec(&db, 60)]).await.unwrap();
        space
            .submit_checked(
                vec![(key(&[b"db", b"mine"]), val(b"mine"))],
                vec![RangeAssert {
                    prefix: db.clone(),
                    upto: AdmissionSeq(0),
                }],
            )
            .await
            .unwrap();

        server_clock.advance(Duration::from_secs(61));
        foreign_put(
            &handle,
            SPACE,
            dev(2),
            &db,
            vec![PutEntry {
                key: key(&[b"db", b"foreign"]),
                value: val(b"foreign"),
                ver: Ver(1),
            }],
            DeviceSeq(1),
        )
        .await;

        assert_eq!(
            client.push().await.unwrap(),
            PushOutcome::Stalled {
                at: DeviceSeq(1),
                error: KernelError::RangeAssertFailed {
                    failures: vec![RangeAssertFailure {
                        prefix: db,
                        upto: AdmissionSeq(0),
                        actual: AdmissionSeq(1),
                    }],
                },
                acked_through: None,
            }
        );
        assert_eq!(queued(&mem).await, 1);
    });
}

#[test]
fn child_or_sibling_lease_does_not_cover_range_assert() {
    block_on(async {
        let mem = MemoryStore::new();
        let clock = ManualClock::new(Timestamp(0));
        let handle = spawn_server(Arc::new(ManualClock::new(Timestamp(0))), &[SPACE]);
        let client = open_client(OrderedMetaStore::new(&mem), &handle, &clock, dev(1))
            .await
            .unwrap();
        client
            .attach(&SpaceEnvelope::plaintext(SPACE))
            .await
            .unwrap();
        let space = client.space(SPACE).await.unwrap();
        let db = key(&[b"db"]);
        let child = key(&[b"db", b"left"]);
        space.ensure(vec![rspec(&child, 60)]).await.unwrap();

        for prefix in [db, key(&[b"db", b"right"])] {
            let error = space
                .submit_checked(
                    Vec::<homebase::Mutation>::new(),
                    vec![RangeAssert {
                        prefix: prefix.clone(),
                        upto: AdmissionSeq(0),
                    }],
                )
                .await
                .unwrap_err();
            assert_eq!(error, SpaceDriverError::RangeAssertAuthority { prefix });
        }
        assert_eq!(queued(&mem).await, 0);
    });
}

#[test]
fn checked_submit_rejects_assert_without_active_lease() {
    block_on(async {
        let mem = MemoryStore::new();
        let clock = ManualClock::new(Timestamp(0));
        let handle = |_: &SpaceId| Option::<SpaceHandle>::None;
        let client = open_client(OrderedMetaStore::new(&mem), &handle, &clock, dev(1))
            .await
            .unwrap();
        client
            .attach(&SpaceEnvelope::plaintext(SPACE))
            .await
            .unwrap();
        let space = client.space(SPACE).await.unwrap();
        let db = key(&[b"db"]);

        let error = space
            .submit_checked(
                vec![(key(&[b"db", b"row"]), val(b"value"))],
                vec![RangeAssert {
                    prefix: db.clone(),
                    upto: AdmissionSeq(0),
                }],
            )
            .await
            .unwrap_err();

        assert_eq!(error, SpaceDriverError::RangeAssertAuthority { prefix: db });
        assert_eq!(queued(&mem).await, 0);
    });
}

#[test]
fn checked_submit_rejects_assert_beyond_local_watermark() {
    block_on(async {
        let mem = MemoryStore::new();
        let clock = ManualClock::new(Timestamp(0));
        let handle = spawn_server(Arc::new(ManualClock::new(Timestamp(0))), &[SPACE]);
        let client = open_client(OrderedMetaStore::new(&mem), &handle, &clock, dev(1))
            .await
            .unwrap();
        client
            .attach(&SpaceEnvelope::plaintext(SPACE))
            .await
            .unwrap();
        let space = client.space(SPACE).await.unwrap();
        let db = key(&[b"db"]);
        space.ensure(vec![wspec(&db, 60)]).await.unwrap();

        let error = space
            .submit_checked(
                vec![(key(&[b"db", b"row"]), val(b"value"))],
                vec![RangeAssert {
                    prefix: db.clone(),
                    upto: AdmissionSeq(7),
                }],
            )
            .await
            .unwrap_err();

        assert_eq!(
            error,
            SpaceDriverError::RangeAssertAhead {
                prefix: db,
                upto: AdmissionSeq(7),
                local: AdmissionSeq(0),
            }
        );
        assert_eq!(queued(&mem).await, 0);
    });
}

#[test]
fn unchecked_submit_bypasses_local_assert_gate() {
    block_on(async {
        let mem = MemoryStore::new();
        let clock = ManualClock::new(Timestamp(0));
        let handle = |_: &SpaceId| Option::<SpaceHandle>::None;
        let client = open_client(OrderedMetaStore::new(&mem), &handle, &clock, dev(1))
            .await
            .unwrap();
        client
            .attach(&SpaceEnvelope::plaintext(SPACE))
            .await
            .unwrap();
        let space = client.space(SPACE).await.unwrap();

        let submission = space
            .submit_unchecked(
                vec![(key(&[b"db", b"row"]), val(b"value"))],
                vec![RangeAssert {
                    prefix: key(&[b"db"]),
                    upto: AdmissionSeq(99),
                }],
            )
            .await
            .unwrap();

        let state = audit(&OrderedMetaStore::new(&mem)).await;
        assert_eq!(
            state.spaces[&SPACE].oplog[&submission.seq].range_asserts()[0].upto,
            AdmissionSeq(99)
        );
    });
}

#[test]
fn submissions_stamp_versions_from_each_spaces_high_water() {
    block_on(async {
        let mem = MemoryStore::new();
        let clock = ManualClock::new(Timestamp(0));
        let handle = |_: &SpaceId| Option::<SpaceHandle>::None;
        let client = open_client(OrderedMetaStore::new(&mem), &handle, &clock, dev(1))
            .await
            .unwrap();
        client
            .attach(&SpaceEnvelope::plaintext(SPACE))
            .await
            .unwrap();
        client
            .attach(&SpaceEnvelope::plaintext(OTHER_SPACE))
            .await
            .unwrap();
        let first = client.space(SPACE).await.unwrap();
        let second = client.space(OTHER_SPACE).await.unwrap();

        first
            .submit_unchecked(
                vec![
                    (key(&[b"db", b"one"]), val(b"1")),
                    (key(&[b"db", b"two"]), val(b"2")),
                ],
                vec![],
            )
            .await
            .unwrap();
        second
            .submit_unchecked(vec![(key(&[b"db", b"one"]), val(b"1"))], vec![])
            .await
            .unwrap();

        let state = audit(&OrderedMetaStore::new(&mem)).await;
        assert_eq!(state.spaces[&SPACE].ver_high, Some(Ver(2)));
        assert_eq!(state.spaces[&OTHER_SPACE].ver_high, Some(Ver(1)));
    });
}

#[test]
fn push_drains_and_groups_same_space_neighbors() {
    block_on(async {
        let mem = MemoryStore::new();
        let clock = ManualClock::new(Timestamp(0));
        let handle = spawn_server(Arc::new(ManualClock::new(Timestamp(0))), &[SPACE]);
        let client = open_client(OrderedMetaStore::new(&mem), &handle, &clock, dev(1))
            .await
            .unwrap();

        client
            .attach(&SpaceEnvelope::plaintext(SPACE))
            .await
            .unwrap();

        let space = client.space(SPACE).await.unwrap();

        let db = key(&[b"db"]);
        space.acquire(vec![wspec(&db, 60)]).await.unwrap();

        let (a1, a2, a3) = (
            key(&[b"db", b"a1"]),
            key(&[b"db", b"a2"]),
            key(&[b"db", b"a3"]),
        );
        space
            .submit_checked(vec![(a1.clone(), val(b"1"))], vec![])
            .await
            .unwrap();
        space
            .submit_checked(vec![(a2.clone(), val(b"2"))], vec![])
            .await
            .unwrap();
        space
            .submit_checked(vec![(a3.clone(), val(b"4"))], vec![])
            .await
            .unwrap();
        assert_eq!(queued(&mem).await, 3);

        let outcome = client.push().await.unwrap();
        assert_eq!(
            outcome,
            PushOutcome::Drained {
                acked_through: Some(DeviceSeq(3))
            }
        );
        assert_eq!(queued(&mem).await, 0);

        // Same-space neighbors merge into one request while preserving each
        // client commit's seq identity in stored tags.
        assert_eq!(
            fetch(&handle, SPACE, &a1).await.unwrap().tag.device_seq,
            DeviceSeq(1)
        );
        assert_eq!(
            fetch(&handle, SPACE, &a2).await.unwrap().tag.device_seq,
            DeviceSeq(2)
        );
        assert_eq!(
            fetch(&handle, SPACE, &a3).await.unwrap().tag.device_seq,
            DeviceSeq(3)
        );

        // The trim is durable, and a drained queue pushes as a no-op.
        let state = audit(&OrderedMetaStore::new(&mem)).await;
        assert!(state.spaces[&SPACE].oplog.is_empty());
        assert_eq!(state.spaces[&SPACE].cursors.tail, DeviceSeq(4));
        assert_eq!(
            client.push().await.unwrap(),
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
        let client = open_client(OrderedMetaStore::new(&mem), &handle, &clock, dev(1))
            .await
            .unwrap();
        client.with_push_cap(1);
        client
            .attach(&SpaceEnvelope::plaintext(SPACE))
            .await
            .unwrap();
        let space = client.space(SPACE).await.unwrap();

        let db = key(&[b"db"]);
        space.acquire(vec![wspec(&db, 60)]).await.unwrap();
        let (k1, k2) = (key(&[b"db", b"k1"]), key(&[b"db", b"k2"]));
        space
            .submit_checked(vec![(k1.clone(), val(b"1"))], vec![])
            .await
            .unwrap();
        space
            .submit_checked(vec![(k2.clone(), val(b"2"))], vec![])
            .await
            .unwrap();

        client.push().await.unwrap();
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
        let client = open_client(OrderedMetaStore::new(&mem), &handle, &clock, dev(1))
            .await
            .unwrap();

        client
            .attach(&SpaceEnvelope::plaintext(SPACE))
            .await
            .unwrap();

        let space = client.space(SPACE).await.unwrap();

        let db = key(&[b"db"]);
        let first = space.acquire(vec![wspec(&db, 60)]).await.unwrap();
        assert_eq!(
            first.barrier, None,
            "no admitted writes means no catch-up barrier"
        );
        let lease = first.leases[0].clone();

        // Asking again changes nothing: same lease, same epoch, no wire
        // grant — and so no new catch-up obligation.
        let again = space.acquire(vec![wspec(&db, 60)]).await.unwrap();
        assert_eq!(again.leases, vec![lease.clone()]);
        assert_eq!(again.barrier, None);

        // A held write on the covering prefix satisfies a narrower read
        // spec too.
        let read_spec = LeaseSpec {
            prefix: key(&[b"db", b"sub"]),
            mode: LeaseMode::Read,
            ttl: Duration::from_secs(60),
        };
        let covered = space.acquire(vec![read_spec]).await.unwrap();
        assert_eq!(covered.leases, vec![lease.clone()]);
        assert_eq!(covered.barrier, None);

        // Mixed: one satisfied spec, one genuinely new — only the new
        // one is acquired, and the answer stays parallel to the specs.
        let other = key(&[b"other"]);
        let mixed = space
            .acquire(vec![wspec(&db, 60), wspec(&other, 60)])
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
        let revived = space.acquire(vec![wspec(&db, 60)]).await.unwrap();
        assert_eq!(revived.barrier, None);
        assert_eq!(revived.leases[0].id, lease.id, "renewed, not re-granted");

        // And the revived lease actually backs writes again.
        space
            .submit_checked(vec![(key(&[b"db", b"w"]), val(b"v"))], vec![])
            .await
            .unwrap();
        assert!(matches!(
            client.push().await.unwrap(),
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
            let client = open_client(OrderedMetaStore::new(&mem), &handle, &clock, dev(1))
                .await
                .unwrap();

            client
                .attach(&SpaceEnvelope::plaintext(SPACE))
                .await
                .unwrap();

            let space = client.space(SPACE).await.unwrap();
            let granted = space.acquire(vec![wspec(&db, 3_600)]).await.unwrap();
            space
                .submit_checked(vec![(k.clone(), val(b"v"))], vec![])
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
        let client = open_client(OrderedMetaStore::new(&mem), &handle, &clock, dev(9))
            .await
            .unwrap();

        client
            .attach(&SpaceEnvelope::plaintext(SPACE))
            .await
            .unwrap();

        let space = client.space(SPACE).await.unwrap();
        assert_eq!(client.device(), dev(1));
        let leases = space.leases(std::slice::from_ref(&db)).await.unwrap();
        assert_eq!(leases.len(), 1);
        assert!(leases[0].live, "the wall fallback outlives the process");
        assert_eq!(
            client.push().await.unwrap(),
            PushOutcome::Drained {
                acked_through: Some(DeviceSeq(1))
            }
        );
        assert_eq!(fetch(&handle, SPACE, &k).await.unwrap().value, val(b"v"));

        // Real expiry still ends it: past the deadline the engine
        // refuses assertion coverage, and renewal is the cure.
        clock.advance(Duration::from_secs(3600));
        assert!(matches!(
            space
                .submit_checked(
                    vec![(k.clone(), val(b"later"))],
                    vec![RangeAssert {
                        prefix: db.clone(),
                        upto: AdmissionSeq(0),
                    }],
                )
                .await
                .unwrap_err(),
            SpaceDriverError::RangeAssertAuthority { .. }
        ));
        let renewed = space.renew(std::slice::from_ref(&db)).await.unwrap();
        assert_eq!(renewed.granted.len(), 1);
        space
            .submit_checked(vec![(k.clone(), val(b"later"))], vec![])
            .await
            .unwrap();
        assert_eq!(
            client.push().await.unwrap(),
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
            let client = open_client(OrderedMetaStore::new(&mem), &handle, &clock, dev(1))
                .await
                .unwrap();

            client
                .attach(&SpaceEnvelope::plaintext(SPACE))
                .await
                .unwrap();

            let space = client.space(SPACE).await.unwrap();
            space.acquire(vec![wspec(&db, 60)]).await.unwrap();
            space
                .submit_checked(vec![(key(&[b"db", b"k"]), val(b"v"))], vec![])
                .await
                .unwrap();

            // Two milliseconds shy of the deadline, judged by the
            // process that stamped it: the monotonic ruler is precise,
            // no margin shaves it — the full window is usable.
            clock.set(Timestamp(60_000 - 2));
            assert_eq!(
                client.push().await.unwrap(),
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
        let client = open_client(OrderedMetaStore::new(&mem), &handle, &clock, dev(9))
            .await
            .unwrap();

        client
            .attach(&SpaceEnvelope::plaintext(SPACE))
            .await
            .unwrap();

        let space = client.space(SPACE).await.unwrap();
        assert!(!space.leases(std::slice::from_ref(&db)).await.unwrap()[0].live);
        // The exact boundary: live until deadline − 60ms, not after.
        clock.set(Timestamp(59_939));
        assert!(space.leases(std::slice::from_ref(&db)).await.unwrap()[0].live);
        clock.set(Timestamp(59_940));
        assert!(!space.leases(std::slice::from_ref(&db)).await.unwrap()[0].live);
        clock.set(Timestamp(60_000 - 2));
        assert!(matches!(
            space
                .submit_checked(
                    vec![(key(&[b"db", b"k2"]), val(b"v2"))],
                    vec![RangeAssert {
                        prefix: db.clone(),
                        upto: AdmissionSeq(0),
                    }],
                )
                .await
                .unwrap_err(),
            SpaceDriverError::RangeAssertAuthority { .. }
        ));

        space.renew(std::slice::from_ref(&db)).await.unwrap();
        space
            .submit_checked(vec![(key(&[b"db", b"k2"]), val(b"v2"))], vec![])
            .await
            .unwrap();
        assert_eq!(
            client.push().await.unwrap(),
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
        let client = open_client(OrderedMetaStore::new(&mem), &handle, &clock, dev(1))
            .await
            .unwrap();

        client
            .attach(&SpaceEnvelope::plaintext(SPACE))
            .await
            .unwrap();

        let space = client.space(SPACE).await.unwrap();

        let db = key(&[b"db"]);
        space.acquire(vec![wspec(&db, 60)]).await.unwrap();
        space
            .submit_checked(vec![(key(&[b"db", b"k"]), val(b"v"))], vec![])
            .await
            .unwrap();

        // The laptop sleeps for an hour: real time passes, the process's
        // monotonic ruler does not see it. Same lineage — but expiry
        // takes the earlier verdict of the two rulers, and the wall one
        // knows the lease is long gone.
        clock.skew_wall(Duration::from_secs(3_600));
        assert!(!space.leases(std::slice::from_ref(&db)).await.unwrap()[0].live);
        assert_eq!(
            client.push().await.unwrap(),
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
            let client = open_client(OrderedMetaStore::new(&mem), &handle, &clock, dev(1))
                .await
                .unwrap();

            client
                .attach(&SpaceEnvelope::plaintext(SPACE))
                .await
                .unwrap();

            let space = client.space(SPACE).await.unwrap();
            space.acquire(vec![wspec(&db, 60)]).await.unwrap();
            space
                .submit_checked(vec![(k.clone(), val(b"v"))], vec![])
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
        let client = open_client(OrderedMetaStore::new(&mem), &handle, &clock, dev(9))
            .await
            .unwrap();

        client
            .attach(&SpaceEnvelope::plaintext(SPACE))
            .await
            .unwrap();

        let space = client.space(SPACE).await.unwrap();
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
        assert!(!space.leases(std::slice::from_ref(&db)).await.unwrap()[0].live);

        // Zero-stamped means no lease authority, but an unreserved write may
        // still admit without lease evidence.
        assert_eq!(
            client.push().await.unwrap(),
            PushOutcome::Drained {
                acked_through: Some(DeviceSeq(1))
            }
        );
        space.renew(std::slice::from_ref(&db)).await.unwrap();
        assert_eq!(
            audit(&OrderedMetaStore::new(&mem)).await.spaces[&SPACE]
                .leases
                .values()
                .next()
                .unwrap()
                .deadline,
            hstamp(2_000 + 60_000, 2)
        );
        let _ = client;

        // Poison does not linger: a later open on the healed timeline
        // keeps the renewed stamp alive.
        clock.advance(Duration::from_secs(10));
        let client = open_client(OrderedMetaStore::new(&mem), &handle, &clock, dev(9))
            .await
            .unwrap();
        client
            .attach(&SpaceEnvelope::plaintext(SPACE))
            .await
            .unwrap();
        let space = client.space(SPACE).await.unwrap();
        assert!(space.leases(std::slice::from_ref(&db)).await.unwrap()[0].live);
    });
}

#[test]
fn expired_local_lease_does_not_block_unasserted_submission() {
    block_on(async {
        let mem = MemoryStore::new();
        let clock = ManualClock::new(Timestamp(0));
        let server_clock = Arc::new(ManualClock::new(Timestamp(0)));
        let handle = spawn_server(Arc::clone(&server_clock), &[SPACE]);
        let client = open_client(OrderedMetaStore::new(&mem), &handle, &clock, dev(1))
            .await
            .unwrap();

        client
            .attach(&SpaceEnvelope::plaintext(SPACE))
            .await
            .unwrap();

        let space = client.space(SPACE).await.unwrap();

        let db = key(&[b"db"]);
        let k = key(&[b"db", b"k"]);
        let granted = space.acquire(vec![wspec(&db, 60)]).await.unwrap();
        let _lease_id = granted.leases[0].id;
        space
            .submit_checked(vec![(k.clone(), val(b"v"))], vec![])
            .await
            .unwrap();

        // Only the CLIENT clock reaches the deadline; locally the lease is
        // not authority, but the unreserved write may still be admitted.
        clock.advance(Duration::from_secs(60));
        assert_eq!(
            client.push().await.unwrap(),
            PushOutcome::Drained {
                acked_through: Some(DeviceSeq(1))
            }
        );

        // Renewal restarts the local window from this send.
        space.renew(std::slice::from_ref(&db)).await.unwrap();
        let leases = space.leases(std::slice::from_ref(&db)).await.unwrap();
        assert_eq!(leases[0].held.deadline, hstamp(60_000 + 60_000, 1));
    });
}

#[test]
fn seq_collision_recovers_a_dead_incarnations_send_exactly_once() {
    block_on(async {
        let mem = MemoryStore::new();
        let clock = ManualClock::new(Timestamp(0));
        let handle = spawn_server(Arc::new(ManualClock::new(Timestamp(0))), &[SPACE]);
        let client = open_client(OrderedMetaStore::new(&mem), &handle, &clock, dev(1))
            .await
            .unwrap();

        client
            .attach(&SpaceEnvelope::plaintext(SPACE))
            .await
            .unwrap();

        let space = client.space(SPACE).await.unwrap();

        let db = key(&[b"db"]);
        let (k1, k2) = (key(&[b"db", b"k1"]), key(&[b"db", b"k2"]));
        let granted = space.acquire(vec![wspec(&db, 60)]).await.unwrap();
        let lease = granted.leases[0].id;
        space
            .submit_checked(vec![(k1.clone(), val(b"one"))], vec![])
            .await
            .unwrap();
        space
            .submit_checked(vec![(k2.clone(), val(b"two"))], vec![])
            .await
            .unwrap();

        // The dead incarnation's send: the same group, coalesced as two
        // client batches, admitted — and then the crash ate the trim.
        handle
            .put_batch(
                &SPACE,
                PutBatchRequest {
                    device: client.device(),
                    evidence: vec![lease],
                    batches: vec![
                        PutBatch {
                            device_seq: DeviceSeq(1),
                            range_asserts: vec![],
                            ops: vec![
                                PutEntry {
                                    key: k1.clone(),
                                    value: val(b"one"),
                                    ver: Ver(1),
                                }
                                .into(),
                            ],
                        },
                        PutBatch {
                            device_seq: DeviceSeq(2),
                            range_asserts: vec![],
                            ops: vec![
                                PutEntry {
                                    key: k2.clone(),
                                    value: val(b"two"),
                                    ver: Ver(2),
                                }
                                .into(),
                            ],
                        },
                    ],
                },
            )
            .await
            .expect("the dead incarnation's send was admitted");

        // The resend collides, the collision names the admitted extent,
        // the trim happens, and nothing is applied twice.
        assert_eq!(
            client.push().await.unwrap(),
            PushOutcome::Drained {
                acked_through: Some(DeviceSeq(2))
            }
        );
        assert_eq!(queued(&mem).await, 0);
        let entry = fetch(&handle, SPACE, &k1).await.unwrap();
        assert_eq!(entry.value, val(b"one"));
        assert_eq!(entry.tag.ver, Ver(1), "admitted exactly once, no replay");
        assert!(
            audit(&OrderedMetaStore::new(&mem)).await.spaces[&SPACE]
                .oplog
                .is_empty()
        );
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

        // ...and this engine submits against k blindly (it never pulled):
        // the middle commit of three is genuinely faulty.
        let client = open_client(OrderedMetaStore::new(&mem), &handle, &clock, dev(1))
            .await
            .unwrap();

        client
            .attach(&SpaceEnvelope::plaintext(SPACE))
            .await
            .unwrap();

        let space = client.space(SPACE).await.unwrap();
        space.acquire(vec![wspec(&db, 60)]).await.unwrap();
        // Simulate a buggy caller that marks the acquire barrier satisfied
        // without importing the foreign value's ver. The pusher still
        // degrades a group rejection into the faulty solo commit.
        OrderedMetaStore::new(&mem)
            .advance_watermark(SPACE, &Range::Prefix(db.clone()), AdmissionSeq(1), Ver(0))
            .await
            .unwrap();
        space
            .submit_checked(vec![(x.clone(), val(b"ok"))], vec![])
            .await
            .unwrap();
        space
            .submit_checked(vec![(k.clone(), val(b"stale"))], vec![])
            .await
            .unwrap();
        space
            .submit_checked(vec![(y.clone(), val(b"after"))], vec![])
            .await
            .unwrap();

        // The merged group bounces; solo probes admit the healthy head
        // and convict exactly the faulty seq.
        let outcome = client.push().await.unwrap();
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
        client.rollback(SPACE, DeviceSeq(2)).await.unwrap();
        assert_eq!(queued(&mem).await, 3, "dead suffix plus rollback marker");
        assert_eq!(
            client.push().await.unwrap(),
            PushOutcome::Drained {
                acked_through: Some(DeviceSeq(4))
            }
        );
        assert_eq!(queued(&mem).await, 0);

        assert_eq!(fetch(&handle, SPACE, &x).await.unwrap().value, val(b"ok"));
        let foreign = fetch(&handle, SPACE, &k).await.unwrap();
        assert_eq!(foreign.value, val(b"foreign"));
        assert_eq!(foreign.tag.ver, Ver(100), "the stale write never landed");
        assert!(
            fetch(&handle, SPACE, &y).await.is_none(),
            "the suffix rolled back"
        );
        assert!(
            audit(&OrderedMetaStore::new(&mem)).await.spaces[&SPACE]
                .oplog
                .is_empty()
        );
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
        let client = open_client(OrderedMetaStore::new(&mem), &handle, &clock, dev(1))
            .await
            .unwrap();

        client
            .attach(&SpaceEnvelope::plaintext(SPACE))
            .await
            .unwrap();

        let space = client.space(SPACE).await.unwrap();
        let granted = space.acquire(vec![wspec(&db, 60)]).await.unwrap();
        let _lease_id = granted.leases[0].id;
        space
            .submit_checked(vec![(k1.clone(), val(b"a"))], vec![])
            .await
            .unwrap();

        // The file copy comes alive: a twin loads the same identity —
        // and, on the shared wall timeline, the same live authority.
        // Nothing distinguishes it until the seqs collide.
        let twin_mem = clone_store(&mem).await;
        let twin_client = open_client(OrderedMetaStore::new(&twin_mem), &handle, &clock, dev(7))
            .await
            .unwrap();

        twin_client
            .attach(&SpaceEnvelope::plaintext(SPACE))
            .await
            .unwrap();

        let twin_space = twin_client.space(SPACE).await.unwrap();
        assert_eq!(
            twin_client.device(),
            dev(1),
            "the copy carries the identity"
        );
        twin_space
            .submit_checked(vec![(k2.clone(), val(b"twin"))], vec![])
            .await
            .unwrap();
        assert_eq!(
            twin_client.push().await.unwrap(),
            PushOutcome::Drained {
                acked_through: Some(DeviceSeq(2))
            }
        );

        // The original's push collides with a seq PAST its own mint
        // counter — proof it isn't looking at its own past. Fatal, and
        // nothing is destroyed.
        assert_eq!(
            client.push().await.unwrap_err(),
            ClientError::Space(SpaceDriverError::Fork {
                admitted: DeviceSeq(2)
            })
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

        let client = open_client(OrderedMetaStore::new(&mem), &handle, &clock, dev(1))
            .await
            .unwrap();

        client
            .attach(&SpaceEnvelope::plaintext(SPACE))
            .await
            .unwrap();

        let space = client.space(SPACE).await.unwrap();
        let granted = space.acquire(vec![wspec(&db, 60)]).await.unwrap();
        assert!(matches!(
            space
                .submit_checked(
                    vec![(k.clone(), val(b"too-soon"))],
                    vec![RangeAssert {
                        prefix: db.clone(),
                        upto: AdmissionSeq(0),
                    }],
                )
                .await
                .unwrap_err(),
            SpaceDriverError::RangeAssertAuthority { .. }
        ));

        // The acquire-barrier discipline: pull to the barrier before
        // trusting local state. The pull is a snapshot (no cursor yet)
        // and raises the ver high-water past everything it saw.
        let pulled = space.pull(Range::Prefix(db.clone())).await.unwrap();
        assert!(pulled.at >= granted.barrier.unwrap());
        assert!(matches!(&pulled.ranges[0], RangeCut::Snapshot(entries) if entries.len() == 1));
        assert_eq!(
            audit(&OrderedMetaStore::new(&mem)).await.spaces[&SPACE].watermarks
                [&Range::Prefix(db.clone())],
            pulled.at
        );

        // Now the same key can be overwritten: the commit stamps above
        // the pulled ver, so the server's chain accepts it.
        space
            .submit_checked(
                vec![(k.clone(), val(b"mine"))],
                vec![RangeAssert {
                    prefix: db.clone(),
                    upto: AdmissionSeq(1),
                }],
            )
            .await
            .unwrap();
        assert_eq!(
            client.push().await.unwrap(),
            PushOutcome::Drained {
                acked_through: Some(DeviceSeq(1))
            }
        );
        let entry = fetch(&handle, SPACE, &k).await.unwrap();
        assert_eq!(entry.value, val(b"mine"));
        assert_eq!(entry.tag.ver, Ver(8), "stamped past the foreign chain");

        // The next pull is a delta from the stored cursor and carries
        // exactly our own admitted write.
        let pulled = space.pull(Range::Prefix(db.clone())).await.unwrap();
        assert!(matches!(&pulled.ranges[0], RangeCut::Delta(entries) if entries.len() == 1));

        // The cursor is durable: a resumed incarnation pulls deltas, not
        // snapshots.
        let state = audit(&OrderedMetaStore::new(&mem)).await;
        assert_eq!(
            state.spaces[&SPACE].watermarks[&Range::Prefix(db.clone())],
            pulled.at
        );
    });
}

#[test]
fn ensure_acquires_pulls_and_makes_assertions_locally_authorized() {
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

        let client = open_client(OrderedMetaStore::new(&mem), &handle, &clock, dev(1))
            .await
            .unwrap();

        client
            .attach(&SpaceEnvelope::plaintext(SPACE))
            .await
            .unwrap();

        let space = client.space(SPACE).await.unwrap();
        let acquired = space.ensure(vec![wspec(&db, 60)]).await.unwrap();
        assert_eq!(acquired.barrier, None);
        assert_eq!(acquired.leases.len(), 1);
        assert_eq!(
            audit(&OrderedMetaStore::new(&mem)).await.spaces[&SPACE].watermarks
                [&Range::Prefix(db.clone())],
            AdmissionSeq(1)
        );

        space
            .submit_checked(vec![(k.clone(), val(b"mine"))], vec![])
            .await
            .unwrap();
        assert_eq!(
            client.push().await.unwrap(),
            PushOutcome::Drained {
                acked_through: Some(DeviceSeq(1))
            }
        );
        let entry = fetch(&handle, SPACE, &k).await.unwrap();
        assert_eq!(entry.value, val(b"mine"));
        assert_eq!(entry.tag.ver, Ver(8));
    });
}

#[test]
fn ensure_satisfies_read_lease_barriers_too() {
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
                key: k,
                value: val(b"foreign"),
                ver: Ver(7),
            }],
            DeviceSeq(1),
        )
        .await;

        let client = open_client(OrderedMetaStore::new(&mem), &handle, &clock, dev(1))
            .await
            .unwrap();

        client
            .attach(&SpaceEnvelope::plaintext(SPACE))
            .await
            .unwrap();

        let space = client.space(SPACE).await.unwrap();
        let acquired = space.ensure(vec![rspec(&db, 60)]).await.unwrap();
        assert_eq!(acquired.barrier, None);
        assert_eq!(acquired.leases[0].mode, LeaseMode::Read);
        assert_eq!(
            audit(&OrderedMetaStore::new(&mem)).await.spaces[&SPACE].watermarks
                [&Range::Prefix(db.clone())],
            AdmissionSeq(1)
        );
    });
}

#[test]
fn pending_release_blocks_checked_assertions_and_retries_explicitly() {
    block_on(async {
        let mem = MemoryStore::new();
        let clock = ManualClock::new(Timestamp(0));
        let handle = spawn_server(Arc::new(ManualClock::new(Timestamp(0))), &[SPACE]);

        let db = key(&[b"db"]);
        let lease_id = {
            let client = open_client(OrderedMetaStore::new(&mem), &handle, &clock, dev(1))
                .await
                .unwrap();

            client
                .attach(&SpaceEnvelope::plaintext(SPACE))
                .await
                .unwrap();

            let space = client.space(SPACE).await.unwrap();
            let granted = space.acquire(vec![wspec(&db, 60)]).await.unwrap();
            granted.leases[0].id
        };

        let offline = |_: &SpaceId| Option::<SpaceHandle>::None;
        let client = open_client(OrderedMetaStore::new(&mem), &offline, &clock, dev(1))
            .await
            .unwrap();

        client
            .attach(&SpaceEnvelope::plaintext(SPACE))
            .await
            .unwrap();

        let space = client.space(SPACE).await.unwrap();
        assert!(matches!(
            space.release(&[lease_id]).await.unwrap_err(),
            SpaceDriverError::Unavailable { .. }
        ));
        let state = audit(&OrderedMetaStore::new(&mem)).await;
        assert!(state.spaces[&SPACE].leases[&lease_id].retiring);
        assert!(matches!(
            space
                .submit_checked(
                    vec![(key(&[b"db", b"k"]), val(b"v"))],
                    vec![RangeAssert {
                        prefix: db.clone(),
                        upto: AdmissionSeq(0),
                    }],
                )
                .await
                .unwrap_err(),
            SpaceDriverError::RangeAssertAuthority { .. }
        ));

        // Reopening does not do hidden server work. The retiring lease is
        // still remembered but not usable.
        let client = open_client(OrderedMetaStore::new(&mem), &handle, &clock, dev(1))
            .await
            .unwrap();

        client
            .attach(&SpaceEnvelope::plaintext(SPACE))
            .await
            .unwrap();

        let space = client.space(SPACE).await.unwrap();
        let leases = space.leases(std::slice::from_ref(&db)).await.unwrap();
        assert_eq!(leases.len(), 1);
        assert!(!leases[0].live);

        // An explicit release retry finishes the saga and drops the local
        // record; a new device can acquire immediately.
        space.release(&[lease_id]).await.unwrap();
        let other = handle
            .acquire(
                &SPACE,
                AcquireRequest {
                    device: dev(2),
                    requested_at: HybridTimestamp::ZERO,
                    specs: vec![wspec(&db, 60)],
                },
            )
            .await
            .unwrap();
        assert_eq!(other.leases[0].prefix, db);
    });
}

#[test]
fn release_rejects_when_queued_writes_are_covered() {
    block_on(async {
        let mem = MemoryStore::new();
        let clock = ManualClock::new(Timestamp(0));
        let handle = spawn_server(Arc::new(ManualClock::new(Timestamp(0))), &[SPACE]);
        let db = key(&[b"db"]);
        let k = key(&[b"db", b"k"]);

        let client = open_client(OrderedMetaStore::new(&mem), &handle, &clock, dev(1))
            .await
            .unwrap();

        client
            .attach(&SpaceEnvelope::plaintext(SPACE))
            .await
            .unwrap();

        let space = client.space(SPACE).await.unwrap();
        let granted = space.acquire(vec![wspec(&db, 60)]).await.unwrap();
        space
            .submit_checked(vec![(k, val(b"queued"))], vec![])
            .await
            .unwrap();

        assert!(matches!(
            space.release(&[granted.leases[0].id]).await.unwrap_err(),
            SpaceDriverError::ReleaseBlocked {
                lease,
                at: DeviceSeq(1)
            } if lease == granted.leases[0].id
        ));
        assert!(
            !audit(&OrderedMetaStore::new(&mem)).await.spaces[&SPACE].leases[&granted.leases[0].id]
                .retiring
        );

        client.rollback(SPACE, DeviceSeq(1)).await.unwrap();
        space.release(&[granted.leases[0].id]).await.unwrap();
        assert!(
            audit(&OrderedMetaStore::new(&mem)).await.spaces[&SPACE]
                .leases
                .is_empty(),
            "retired writes below neck no longer block lease release"
        );
    });
}

#[test]
fn unavailable_leaves_the_queue_intact() {
    block_on(async {
        let mem = MemoryStore::new();
        let clock = ManualClock::new(Timestamp(0));
        let served = spawn_server(Arc::new(ManualClock::new(Timestamp(0))), &[SPACE]);
        let client = open_client(OrderedMetaStore::new(&mem), &served, &clock, dev(1))
            .await
            .unwrap();

        client
            .attach(&SpaceEnvelope::plaintext(SPACE))
            .await
            .unwrap();

        let space = client.space(SPACE).await.unwrap();
        let db = key(&[b"db"]);
        space.acquire(vec![wspec(&db, 60)]).await.unwrap();
        drop(client);

        let handle = |_: &SpaceId| Option::<SpaceHandle>::None;
        let client = open_client(OrderedMetaStore::new(&mem), &handle, &clock, dev(1))
            .await
            .unwrap();

        client
            .attach(&SpaceEnvelope::plaintext(SPACE))
            .await
            .unwrap();

        let space = client.space(SPACE).await.unwrap();

        space
            .submit_checked(vec![(key(&[b"db", b"k"]), val(b"v"))], vec![])
            .await
            .unwrap();
        assert!(matches!(
            client.push().await.unwrap_err(),
            ClientError::Space(SpaceDriverError::Unavailable { .. })
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
        let client = open_client(OrderedMetaStore::new(&mem), &handle, &clock, dev(1))
            .await
            .unwrap();

        client
            .attach(&SpaceEnvelope::plaintext(SPACE))
            .await
            .unwrap();

        let space = client.space(SPACE).await.unwrap();

        let db = key(&[b"db"]);
        let granted = space.acquire(vec![wspec(&db, 60)]).await.unwrap();
        let lease_id = granted.leases[0].id;

        // The SERVER's clock passes the deadline: strict local expiry,
        // the lease is gone there. Renewal is how this side finds out.
        server_clock.advance(Duration::from_secs(120));
        let renewed = space.renew(std::slice::from_ref(&db)).await.unwrap();
        assert_eq!(renewed.invalid, vec![lease_id]);
        assert!(renewed.granted.is_empty());

        // Forgotten everywhere: memory and the durable record.
        assert!(
            space
                .leases(std::slice::from_ref(&db))
                .await
                .unwrap()
                .is_empty()
        );
        let state = audit(&OrderedMetaStore::new(&mem)).await;
        assert!(state.spaces.get(&SPACE).is_none_or(|s| s.leases.is_empty()));
    });
}
