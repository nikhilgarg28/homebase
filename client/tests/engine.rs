//! Engine tortures against a real in-process server: two hand-cranked
//! clocks (client and server timelines never compared), a shared
//! `MemoryStore` playing the client's disk so crashes are a drop-and-
//! reopen, and dead incarnations simulated by hand-shipping what they
//! would have sent. Every recovery path in the pusher's algebra gets a
//! deterministic run.

use homebase::Server;
use homebase::actor::{SpaceHandle, Spawner};
use homebase_client::cipher::{SpaceEnvelope, SystemNonceSource};
use homebase_client::meta::{HeldLease, MetaStore, OrderedMetaStore, SubmitMode, audit};
use homebase_client::server::ServerHandle;
use homebase_client::{
    Client, ClientError, PushOutcome, PushReceipt, SpaceDriverError, lease_margin,
};
use homebase_core::clock::{HybridClock, HybridTimestamp, Lineage, ManualClock, Timestamp};
use homebase_core::key::Key;
use homebase_core::lease::LeaseMode;
use homebase_core::messages::{
    AcquireRequest, AdmissionBatch, AdmissionRequest, GetRequest, KernelError, LeaseSpec, Range,
    RangeAssert, RangeAssertFailure, RangeCut, ReleaseRequest, RenewRequest,
};
use homebase_core::seal::Seal;
use homebase_core::space::SpaceId;
use homebase_core::storage::{MemoryStore, OrderedStore, WriteBatch, collect_scan};
use homebase_core::tag::{
    AdmissionSeq, AdmittedEntry, CipherEpoch, DeviceEntry, DeviceId, DeviceSeq, DeviceTag,
    Mutation, OpaqueValue, Ver,
};
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

fn val(bytes: &[u8]) -> Vec<u8> {
    bytes.to_vec()
}

fn set(key: Key, bytes: &[u8]) -> Mutation {
    Mutation::Set {
        key,
        value: val(bytes),
    }
}

#[derive(Clone)]
struct PendingEntry {
    key: Key,
    value: Vec<u8>,
    ver: Ver,
}

fn wire_entry(device: DeviceId, device_seq: DeviceSeq, entry: PendingEntry) -> DeviceEntry {
    DeviceEntry {
        mutation: Mutation::Set {
            key: entry.key,
            value: OpaqueValue(entry.value),
        },
        tag: DeviceTag {
            device,
            device_seq,
            ver: entry.ver,
            cipher_epoch: CipherEpoch(0),
        },
        seal: Seal::empty_aead_v1(),
    }
}

fn entry_value(entry: &AdmittedEntry) -> &[u8] {
    match &entry.device_entry.mutation {
        Mutation::Set { value, .. } => &value.0,
        Mutation::Delete { .. } => panic!("expected live value"),
        Mutation::DeleteRange { .. } => panic!("unexpected range delete"),
    }
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

async fn fetch(handle: &impl ServerHandle, space: SpaceId, k: &Key) -> Option<AdmittedEntry> {
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
    entries: Vec<PendingEntry>,
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
        .admit(
            &space,
            AdmissionRequest {
                device,
                expected_checksum: homebase_core::DeviceChecksum::EMPTY,
                evidence: vec![lease],
                batches: vec![AdmissionBatch {
                    device_seq: seq,
                    range_asserts: vec![],
                    entries: entries
                        .into_iter()
                        .map(|entry| wire_entry(device, seq, entry))
                        .collect(),
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
            vec![PendingEntry {
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
        space.lease(vec![rspec(&db, 60)]).await.unwrap();
        let target = key(&[b"db", b"target"]);
        assert_eq!(
            space.admits().cursors().await.unwrap(),
            homebase_client::meta::AdmitCursors {
                head: AdmissionSeq(1),
                neck: AdmissionSeq(1),
                tail: AdmissionSeq(2),
            }
        );
        assert!(matches!(
            space
                .submit_checked(
                    vec![set(key(&[b"db", b"target", b"blocked"]), b"value")],
                    vec![RangeAssert {
                        prefix: target.clone(),
                        upto: AdmissionSeq(0),
                    }],
                )
                .await
                .unwrap_err(),
            SpaceDriverError::RangeAssertAuthority { .. }
        ));
        space.admits().mark_applied(AdmissionSeq(2)).await.unwrap();

        let submission = space
            .submit_checked(
                vec![set(key(&[b"db", b"target", b"row"]), b"value")],
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
        assert_eq!(record.submit_mode(), Some(SubmitMode::Checked));
        assert_eq!(record.range_asserts()[0].prefix, target);
        assert_eq!(record.entries()[0].ver(), Ver(2));
        assert_eq!(
            client.space(SPACE).await.unwrap().push().await.unwrap(),
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
        space.lease(vec![rspec(&db, 60)]).await.unwrap();

        for name in [&b"one"[..], &b"two"[..]] {
            space
                .submit_checked(
                    vec![set(key(&[b"db", name]), name)],
                    vec![RangeAssert {
                        prefix: db.clone(),
                        upto: AdmissionSeq(0),
                    }],
                )
                .await
                .unwrap();
        }

        assert_eq!(
            client.space(SPACE).await.unwrap().push().await.unwrap(),
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
        space.lease(vec![rspec(&db, 60)]).await.unwrap();

        for name in [&b"one"[..], &b"two"[..]] {
            space
                .submit_checked(
                    vec![set(key(&[b"db", name]), name)],
                    vec![RangeAssert {
                        prefix: db.clone(),
                        upto: AdmissionSeq(0),
                    }],
                )
                .await
                .unwrap();
        }

        assert_eq!(
            client.space(SPACE).await.unwrap().push().await.unwrap(),
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
        space.lease(vec![rspec(&db, 60)]).await.unwrap();
        space
            .submit_checked(
                vec![set(key(&[b"db", b"mine"]), b"mine")],
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
            vec![PendingEntry {
                key: key(&[b"db", b"foreign"]),
                value: val(b"foreign"),
                ver: Ver(1),
            }],
            DeviceSeq(1),
        )
        .await;

        assert_eq!(
            client.space(SPACE).await.unwrap().push().await.unwrap(),
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
        space.lease(vec![rspec(&child, 60)]).await.unwrap();

        for prefix in [db, key(&[b"db", b"right"])] {
            let error = space
                .submit_checked(
                    Vec::<homebase_client::Mutation>::new(),
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
                vec![set(key(&[b"db", b"row"]), b"value")],
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
fn checked_submit_rejects_assert_beyond_applied_neck() {
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
        space.lease(vec![wspec(&db, 60)]).await.unwrap();

        let error = space
            .submit_checked(
                vec![set(key(&[b"db", b"row"]), b"value")],
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
                vec![set(key(&[b"db", b"row"]), b"value")],
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
fn rebase_analysis_reports_assertion_conflicts_without_moving_either_log() {
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
        space
            .submit_unchecked(
                vec![set(key(&[b"db", b"local"]), b"local")],
                vec![RangeAssert {
                    prefix: db.clone(),
                    upto: AdmissionSeq(0),
                }],
            )
            .await
            .unwrap();
        space
            .submit_unchecked(vec![set(key(&[b"db", b"unasserted"]), b"local")], vec![])
            .await
            .unwrap();

        foreign_put(
            &handle,
            SPACE,
            dev(2),
            &db,
            vec![PendingEntry {
                key: key(&[b"db", b"foreign"]),
                value: val(b"foreign"),
                ver: Ver(1),
            }],
            DeviceSeq(1),
        )
        .await;
        assert_eq!(space.pull().await.unwrap(), AdmissionSeq(1));

        let before = audit(&OrderedMetaStore::new(&mem)).await;
        let before_space = &before.spaces[&SPACE];
        let analyzed_range = before_space.admit_cursors.neck..before_space.admit_cursors.tail;
        let analysis = space.analyze_rebase(analyzed_range.clone()).await.unwrap();
        assert_eq!(analysis.submit_cursors, before_space.cursors);
        assert_eq!(analysis.admit_cursors, before_space.admit_cursors);
        assert_eq!(analysis.admit_range, analyzed_range);
        assert_eq!(
            analysis.conflicts,
            vec![homebase_client::RebaseConflict {
                device_seq: DeviceSeq(1),
                failures: vec![RangeAssertFailure {
                    prefix: db,
                    upto: AdmissionSeq(0),
                    actual: AdmissionSeq(1),
                }],
            }]
        );

        let empty = space
            .analyze_rebase(before_space.admit_cursors.tail..before_space.admit_cursors.tail)
            .await
            .unwrap();
        assert!(empty.is_clean());
        assert!(matches!(
            space
                .analyze_rebase(
                    before_space.admit_cursors.head
                        ..AdmissionSeq(before_space.admit_cursors.tail.0 + 1),
                )
                .await,
            Err(SpaceDriverError::RebaseRangeUnavailable { .. })
        ));

        let after = audit(&OrderedMetaStore::new(&mem)).await;
        assert_eq!(after, before);
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
                    set(key(&[b"db", b"one"]), b"1"),
                    set(key(&[b"db", b"two"]), b"2"),
                ],
                vec![],
            )
            .await
            .unwrap();
        second
            .submit_unchecked(vec![set(key(&[b"db", b"one"]), b"1")], vec![])
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
        space.lease(vec![wspec(&db, 60)]).await.unwrap();

        let (a1, a2, a3) = (
            key(&[b"db", b"a1"]),
            key(&[b"db", b"a2"]),
            key(&[b"db", b"a3"]),
        );
        space
            .submit_checked(vec![set(a1.clone(), b"1")], vec![])
            .await
            .unwrap();
        space
            .submit_checked(vec![set(a2.clone(), b"2")], vec![])
            .await
            .unwrap();
        space
            .submit_checked(vec![set(a3.clone(), b"4")], vec![])
            .await
            .unwrap();
        assert_eq!(queued(&mem).await, 3);

        let outcome = client.space(SPACE).await.unwrap().push().await.unwrap();
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
            fetch(&handle, SPACE, &a1)
                .await
                .unwrap()
                .device_entry
                .tag
                .device_seq,
            DeviceSeq(1)
        );
        assert_eq!(
            fetch(&handle, SPACE, &a2)
                .await
                .unwrap()
                .device_entry
                .tag
                .device_seq,
            DeviceSeq(2)
        );
        assert_eq!(
            fetch(&handle, SPACE, &a3)
                .await
                .unwrap()
                .device_entry
                .tag
                .device_seq,
            DeviceSeq(3)
        );

        // The trim is durable, and a drained queue pushes as a no-op.
        let state = audit(&OrderedMetaStore::new(&mem)).await;
        assert!(state.spaces[&SPACE].oplog.is_empty());
        assert_eq!(state.spaces[&SPACE].cursors.tail, DeviceSeq(4));
        assert_eq!(
            client.space(SPACE).await.unwrap().push().await.unwrap(),
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
        space.lease(vec![wspec(&db, 60)]).await.unwrap();
        let (k1, k2) = (key(&[b"db", b"k1"]), key(&[b"db", b"k2"]));
        space
            .submit_checked(vec![set(k1.clone(), b"1")], vec![])
            .await
            .unwrap();
        space
            .submit_checked(vec![set(k2.clone(), b"2")], vec![])
            .await
            .unwrap();

        client.space(SPACE).await.unwrap().push().await.unwrap();
        // At cap 1 nothing merges: each commit ships under its own seq.
        assert_eq!(
            fetch(&handle, SPACE, &k1)
                .await
                .unwrap()
                .device_entry
                .tag
                .device_seq,
            DeviceSeq(1)
        );
        assert_eq!(
            fetch(&handle, SPACE, &k2)
                .await
                .unwrap()
                .device_entry
                .tag
                .device_seq,
            DeviceSeq(2)
        );
    });
}

#[test]
fn lease_satisfies_covered_specs_locally() {
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
        let first = space.lease(vec![wspec(&db, 60)]).await.unwrap();
        assert_eq!(first[0].barrier, AdmissionSeq(0));
        assert!(space.leases(std::slice::from_ref(&db)).await.unwrap()[0].usable);
        let lease = first[0].clone();

        // Asking again changes nothing: same lease id, no wire
        // grant — and so no new catch-up obligation.
        let again = space.lease(vec![wspec(&db, 60)]).await.unwrap();
        assert_eq!(again, vec![lease.clone()]);

        // A held write on the covering prefix satisfies a narrower read
        // spec too.
        let read_spec = LeaseSpec {
            prefix: key(&[b"db", b"sub"]),
            mode: LeaseMode::Read,
            ttl: Duration::from_secs(60),
        };
        let covered = space.lease(vec![read_spec]).await.unwrap();
        assert_eq!(covered, vec![lease.clone()]);

        // Mixed: one satisfied spec, one genuinely new — only the new
        // one is acquired, and the answer stays parallel to the specs.
        let other = key(&[b"other"]);
        let mixed = space
            .lease(vec![wspec(&db, 60), wspec(&other, 60)])
            .await
            .unwrap();
        assert_eq!(mixed[0], lease);
        assert_eq!(mixed[1].prefix, other);
        assert_ne!(mixed[1].id, lease.id);
        assert_eq!(mixed[1].barrier, AdmissionSeq(0));

        // Local expiry doesn't force a re-grant: the engine revives the
        // held lease with a renewal — same lease, same fence, fresh
        // local window — because the kernel treats a same-device
        // re-acquire of a live lease as contention.
        clock.advance(Duration::from_secs(60));
        let revived = space.lease(vec![wspec(&db, 60)]).await.unwrap();
        assert_eq!(revived[0].id, lease.id, "renewed, not re-granted");

        // And the revived lease actually backs writes again.
        space
            .submit_checked(vec![set(key(&[b"db", b"w"]), b"v")], vec![])
            .await
            .unwrap();
        assert!(matches!(
            client.space(SPACE).await.unwrap().push().await.unwrap(),
            PushOutcome::Drained { .. }
        ));
    });
}

#[test]
fn lease_does_not_push_a_nonempty_oplog() {
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
        space
            .submit_unchecked(vec![set(key(&[b"queued", b"k"]), b"v")], Vec::new())
            .await
            .unwrap();

        space
            .lease(vec![rspec(&key(&[b"reserved"]), 60)])
            .await
            .unwrap();
        assert_eq!(queued(&mem).await, 1);
        assert!(
            fetch(&handle, SPACE, &key(&[b"queued", b"k"]))
                .await
                .is_none()
        );
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
            let granted = space.lease(vec![wspec(&db, 3_600)]).await.unwrap();
            space
                .submit_checked(vec![set(k.clone(), b"v")], vec![])
                .await
                .unwrap();
            granted[0].id
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
        assert!(leases[0].usable, "the wall fallback outlives the process");
        assert_eq!(
            client.space(SPACE).await.unwrap().push().await.unwrap(),
            PushOutcome::Drained {
                acked_through: Some(DeviceSeq(1))
            }
        );
        assert_eq!(entry_value(&fetch(&handle, SPACE, &k).await.unwrap()), b"v");

        // Real expiry still ends it: past the deadline the engine
        // refuses assertion coverage, and renewal is the cure.
        clock.advance(Duration::from_secs(3600));
        assert!(matches!(
            space
                .submit_checked(
                    vec![set(k.clone(), b"later")],
                    vec![RangeAssert {
                        prefix: db.clone(),
                        upto: AdmissionSeq(0),
                    }],
                )
                .await
                .unwrap_err(),
            SpaceDriverError::RangeAssertAuthority { .. }
        ));
        let renewed = space.lease(vec![wspec(&db, 3_600)]).await.unwrap();
        assert_eq!(renewed.len(), 1);
        space
            .submit_checked(vec![set(k.clone(), b"later")], vec![])
            .await
            .unwrap();
        assert_eq!(
            client.space(SPACE).await.unwrap().push().await.unwrap(),
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
            space.lease(vec![wspec(&db, 60)]).await.unwrap();
            space
                .submit_checked(vec![set(key(&[b"db", b"k"]), b"v")], vec![])
                .await
                .unwrap();

            // Two milliseconds shy of the deadline, judged by the
            // process that stamped it: the monotonic ruler is precise,
            // no margin shaves it — the full window is usable.
            clock.set(Timestamp(60_000 - 2));
            assert_eq!(
                client.space(SPACE).await.unwrap().push().await.unwrap(),
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
        assert!(!space.leases(std::slice::from_ref(&db)).await.unwrap()[0].usable);
        // The exact boundary: live until deadline − 60ms, not after.
        clock.set(Timestamp(59_939));
        assert!(space.leases(std::slice::from_ref(&db)).await.unwrap()[0].usable);
        clock.set(Timestamp(59_940));
        assert!(!space.leases(std::slice::from_ref(&db)).await.unwrap()[0].usable);
        clock.set(Timestamp(60_000 - 2));
        assert!(matches!(
            space
                .submit_checked(
                    vec![set(key(&[b"db", b"k2"]), b"v2")],
                    vec![RangeAssert {
                        prefix: db.clone(),
                        upto: AdmissionSeq(0),
                    }],
                )
                .await
                .unwrap_err(),
            SpaceDriverError::RangeAssertAuthority { .. }
        ));

        space.lease(vec![wspec(&db, 60)]).await.unwrap();
        space
            .submit_checked(vec![set(key(&[b"db", b"k2"]), b"v2")], vec![])
            .await
            .unwrap();
        assert_eq!(
            client.space(SPACE).await.unwrap().push().await.unwrap(),
            PushOutcome::Drained {
                acked_through: Some(DeviceSeq(2))
            }
        );
    });
}

#[test]
fn lease_margin_has_ten_millisecond_floor() {
    assert_eq!(
        lease_margin(Duration::from_millis(1)),
        Duration::from_millis(10)
    );
    assert_eq!(
        lease_margin(Duration::from_secs(1)),
        Duration::from_millis(10)
    );
    assert_eq!(
        lease_margin(Duration::from_secs(60)),
        Duration::from_millis(60)
    );
}

#[test]
fn lease_renews_live_reservation_or_reacquires_with_fresh_barrier() {
    block_on(async {
        let mem = MemoryStore::new();
        let clock = ManualClock::new(Timestamp(0));
        let server_clock = Arc::new(ManualClock::new(Timestamp(0)));
        let handle = spawn_server(Arc::clone(&server_clock), &[SPACE]);
        let db = key(&[b"db"]);
        foreign_put(
            &handle,
            SPACE,
            dev(2),
            &db,
            vec![PendingEntry {
                key: key(&[b"db", b"first"]),
                value: val(b"one"),
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
        let first = space.lease(vec![wspec(&db, 60)]).await.unwrap()[0].clone();
        assert_eq!(first.barrier, AdmissionSeq(1));

        // The new incarnation's conservative margin expires locally while
        // the server reservation remains live. Lease renews the same grant,
        // so no new barrier is needed.
        clock.set_lineage(Lineage([2; 16]));
        clock.set(Timestamp(59_950));
        let renewed = space.lease(vec![wspec(&db, 60)]).await.unwrap()[0].clone();
        assert_eq!(renewed.id, first.id);
        assert_eq!(renewed.barrier, first.barrier);

        // Once the server grant also expires, another device may write.
        // Lease observes the invalid renewal and acquires a new grant at the
        // authority's current barrier.
        server_clock.advance(Duration::from_secs(60));
        foreign_put(
            &handle,
            SPACE,
            dev(3),
            &db,
            vec![PendingEntry {
                key: key(&[b"db", b"second"]),
                value: val(b"two"),
                ver: Ver(2),
            }],
            DeviceSeq(1),
        )
        .await;
        clock.set(Timestamp(120_000));
        let reacquired = space.lease(vec![wspec(&db, 60)]).await.unwrap()[0].clone();
        assert_ne!(reacquired.id, first.id);
        assert_eq!(reacquired.barrier, AdmissionSeq(2));
        let cursors = space.admits().cursors().await.unwrap();
        assert_eq!(cursors.tail, AdmissionSeq(3));
        assert_eq!(cursors.neck, AdmissionSeq(1));
        assert!(!space.leases(std::slice::from_ref(&db)).await.unwrap()[0].usable);
    });
}

#[test]
fn repair_leases_reconciles_and_preserves_forgotten_intent() {
    block_on(async {
        let mem = MemoryStore::new();
        let clock = ManualClock::new(Timestamp(1_000));
        let handle = spawn_server(Arc::new(ManualClock::new(Timestamp(0))), &[SPACE]);
        let db = key(&[b"db"]);
        foreign_put(
            &handle,
            SPACE,
            dev(2),
            &db,
            vec![PendingEntry {
                key: key(&[b"db", b"existing"]),
                value: val(b"foreign"),
                ver: Ver(1),
            }],
            DeviceSeq(1),
        )
        .await;
        let requested_at = clock.stamp();
        let mut remote = handle
            .acquire(
                &SPACE,
                AcquireRequest {
                    device: dev(1),
                    requested_at,
                    specs: vec![rspec(&db, 60)],
                },
            )
            .await
            .unwrap()
            .leases
            .remove(0);
        clock.advance(Duration::from_millis(100));
        let renewed_at = clock.stamp();
        let renewal = handle
            .renew(
                &SPACE,
                RenewRequest {
                    device: dev(1),
                    requested_at: renewed_at,
                    leases: vec![remote.id],
                },
            )
            .await
            .unwrap();
        remote.requested_at = renewed_at;
        remote.granted_at = renewal.granted[0].granted_at;
        remote.ttl = renewal.granted[0].ttl;

        let stale = HeldLease {
            lease: homebase_core::lease::Lease {
                id: homebase_core::lease::LeaseId(999),
                prefix: key(&[b"stale"]),
                mode: LeaseMode::Read,
                requested_at,
                granted_at: Timestamp(0),
                ttl: Duration::from_secs(60),
                barrier: AdmissionSeq(0),
            },
            deadline: requested_at.saturating_add(Duration::from_secs(60)),
            forgotten: false,
        };
        OrderedMetaStore::new(&mem)
            .record_leases(SPACE, &[stale])
            .await
            .unwrap();

        let client = open_client(OrderedMetaStore::new(&mem), &handle, &clock, dev(1))
            .await
            .unwrap();
        client
            .attach(&SpaceEnvelope::plaintext(SPACE))
            .await
            .unwrap();
        let space = client.space(SPACE).await.unwrap();
        space
            .submit_unchecked(Vec::<Mutation>::new(), Vec::new())
            .await
            .unwrap();
        let repaired = space.repair_leases().await.unwrap();
        assert!(
            repaired.active.is_empty(),
            "repair captures the barrier but cannot claim application"
        );
        assert!(repaired.forgotten.is_empty());
        let state = audit(&OrderedMetaStore::new(&mem)).await;
        assert_eq!(state.spaces[&SPACE].oplog.len(), 1);
        assert_eq!(state.spaces[&SPACE].leases.len(), 1);
        let held = &state.spaces[&SPACE].leases[&remote.id];
        assert_eq!(held.deadline, renewed_at.saturating_add(remote.ttl));
        assert_eq!(state.spaces[&SPACE].admit_cursors.tail, AdmissionSeq(2));
        assert_eq!(state.spaces[&SPACE].admit_cursors.neck, AdmissionSeq(1));
        space.admits().mark_applied(AdmissionSeq(2)).await.unwrap();
        assert!(space.leases(std::slice::from_ref(&db)).await.unwrap()[0].usable);
        drop(space);
        drop(client);

        let offline = |_: &SpaceId| Option::<SpaceHandle>::None;
        let client = open_client(OrderedMetaStore::new(&mem), &offline, &clock, dev(1))
            .await
            .unwrap();
        client
            .attach(&SpaceEnvelope::plaintext(SPACE))
            .await
            .unwrap();
        assert!(matches!(
            client
                .space(SPACE)
                .await
                .unwrap()
                .unlease_unchecked(&[remote.id])
                .await,
            Err(SpaceDriverError::Unavailable { .. })
        ));
        drop(client);

        let client = open_client(OrderedMetaStore::new(&mem), &handle, &clock, dev(1))
            .await
            .unwrap();
        client
            .attach(&SpaceEnvelope::plaintext(SPACE))
            .await
            .unwrap();
        let space = client.space(SPACE).await.unwrap();
        let repaired = space.repair_leases().await.unwrap();
        assert!(repaired.active.is_empty());
        assert_eq!(repaired.forgotten, vec![remote.id]);

        handle
            .release(
                &SPACE,
                ReleaseRequest {
                    device: dev(1),
                    leases: vec![remote.id],
                },
            )
            .await
            .unwrap();
        let repaired = space.repair_leases().await.unwrap();
        assert!(repaired.active.is_empty());
        assert!(repaired.forgotten.is_empty());
        assert!(
            audit(&OrderedMetaStore::new(&mem)).await.spaces[&SPACE]
                .leases
                .is_empty()
        );
    });
}

#[test]
fn unavailable_repair_preserves_existing_local_authority() {
    block_on(async {
        let mem = MemoryStore::new();
        let clock = ManualClock::new(Timestamp(0));
        let handle = spawn_server(Arc::new(ManualClock::new(Timestamp(0))), &[SPACE]);
        let db = key(&[b"db"]);
        let client = open_client(OrderedMetaStore::new(&mem), &handle, &clock, dev(1))
            .await
            .unwrap();
        client
            .attach(&SpaceEnvelope::plaintext(SPACE))
            .await
            .unwrap();
        client
            .space(SPACE)
            .await
            .unwrap()
            .lease(vec![rspec(&db, 60)])
            .await
            .unwrap();
        drop(client);

        let offline = |_: &SpaceId| Option::<SpaceHandle>::None;
        let client = open_client(OrderedMetaStore::new(&mem), &offline, &clock, dev(1))
            .await
            .unwrap();
        client
            .attach(&SpaceEnvelope::plaintext(SPACE))
            .await
            .unwrap();
        assert!(matches!(
            client.space(SPACE).await.unwrap().repair_leases().await,
            Err(SpaceDriverError::Unavailable { .. })
        ));
        let state = audit(&OrderedMetaStore::new(&mem)).await;
        assert_eq!(state.spaces[&SPACE].leases.len(), 1);
        assert!(
            !state.spaces[&SPACE]
                .leases
                .values()
                .next()
                .unwrap()
                .forgotten
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
        space.lease(vec![wspec(&db, 60)]).await.unwrap();
        space
            .submit_checked(vec![set(key(&[b"db", b"k"]), b"v")], vec![])
            .await
            .unwrap();

        // The laptop sleeps for an hour: real time passes, the process's
        // monotonic ruler does not see it. Same lineage — but expiry
        // takes the earlier verdict of the two rulers, and the wall one
        // knows the lease is long gone.
        clock.skew_wall(Duration::from_secs(3_600));
        assert!(!space.leases(std::slice::from_ref(&db)).await.unwrap()[0].usable);
        assert_eq!(
            client.space(SPACE).await.unwrap().push().await.unwrap(),
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
            space.lease(vec![wspec(&db, 60)]).await.unwrap();
            space
                .submit_checked(vec![set(k.clone(), b"v")], vec![])
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
        assert!(!space.leases(std::slice::from_ref(&db)).await.unwrap()[0].usable);

        // Zero-stamped means no lease authority, but an unreserved write may
        // still admit without lease evidence.
        assert_eq!(
            client.space(SPACE).await.unwrap().push().await.unwrap(),
            PushOutcome::Drained {
                acked_through: Some(DeviceSeq(1))
            }
        );
        space.lease(vec![wspec(&db, 60)]).await.unwrap();
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
        assert!(space.leases(std::slice::from_ref(&db)).await.unwrap()[0].usable);
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
        let granted = space.lease(vec![wspec(&db, 60)]).await.unwrap();
        let _lease_id = granted[0].id;
        space
            .submit_checked(vec![set(k.clone(), b"v")], vec![])
            .await
            .unwrap();

        // Only the CLIENT clock reaches the deadline; locally the lease is
        // not authority, but the unreserved write may still be admitted.
        clock.advance(Duration::from_secs(60));
        assert_eq!(
            client.space(SPACE).await.unwrap().push().await.unwrap(),
            PushOutcome::Drained {
                acked_through: Some(DeviceSeq(1))
            }
        );

        // Renewal restarts the local window from this send.
        space.lease(vec![wspec(&db, 60)]).await.unwrap();
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
        let granted = space.lease(vec![wspec(&db, 60)]).await.unwrap();
        let lease = granted[0].id;
        space
            .submit_checked(vec![set(k1.clone(), b"one")], vec![])
            .await
            .unwrap();
        let target = space
            .submit_checked(vec![set(k2.clone(), b"two")], vec![])
            .await
            .unwrap();

        // The dead incarnation's send: the same group, coalesced as two
        // client batches, admitted — and then the crash ate the trim.
        handle
            .admit(
                &SPACE,
                AdmissionRequest {
                    device: client.device(),
                    expected_checksum: homebase_core::DeviceChecksum::EMPTY,
                    evidence: vec![lease],
                    batches: vec![
                        AdmissionBatch {
                            device_seq: DeviceSeq(1),
                            range_asserts: vec![],
                            entries: vec![wire_entry(
                                client.device(),
                                DeviceSeq(1),
                                PendingEntry {
                                    key: k1.clone(),
                                    value: val(b"one"),
                                    ver: Ver(1),
                                },
                            )],
                        },
                        AdmissionBatch {
                            device_seq: DeviceSeq(2),
                            range_asserts: vec![],
                            entries: vec![wire_entry(
                                client.device(),
                                DeviceSeq(2),
                                PendingEntry {
                                    key: k2.clone(),
                                    value: val(b"two"),
                                    ver: Ver(2),
                                },
                            )],
                        },
                    ],
                },
            )
            .await
            .expect("the dead incarnation's send was admitted");

        // Admission history is checked before current reservation state.
        // Even though a foreign device reserves the range after the lost
        // response, retry still validates and trims the already-applied send.
        handle
            .release(
                &SPACE,
                ReleaseRequest {
                    device: client.device(),
                    leases: vec![lease],
                },
            )
            .await
            .unwrap();
        handle
            .acquire(
                &SPACE,
                AcquireRequest {
                    device: dev(2),
                    requested_at: clock.stamp(),
                    specs: vec![wspec(&db, 60)],
                },
            )
            .await
            .unwrap();

        // The checksum mismatch names and commits to the admitted extent;
        // matching the retained chain trims it without applying twice.
        assert_eq!(
            target.push().await.unwrap(),
            PushReceipt::Applied {
                seq: DeviceSeq(2),
                admission_seq: None,
            }
        );
        assert_eq!(queued(&mem).await, 0);
        let entry = fetch(&handle, SPACE, &k1).await.unwrap();
        assert_eq!(entry_value(&entry), b"one");
        assert_eq!(
            entry.device_entry.tag.ver,
            Ver(1),
            "admitted exactly once, no replay"
        );
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
            vec![PendingEntry {
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
        let requested_at = clock.stamp();
        let lease = handle
            .acquire(
                &SPACE,
                AcquireRequest {
                    device: dev(1),
                    requested_at,
                    specs: vec![wspec(&db, 60)],
                },
            )
            .await
            .unwrap()
            .leases
            .remove(0);
        // Simulate a buggy caller that marks the acquire barrier satisfied
        // without importing the foreign value's ver. The pusher still
        // degrades a group rejection into the faulty solo commit.
        OrderedMetaStore::new(&mem)
            .record_leases(
                SPACE,
                &[HeldLease {
                    deadline: requested_at.saturating_add(lease.ttl),
                    lease,
                    forgotten: false,
                }],
            )
            .await
            .unwrap();
        space
            .submit_checked(vec![set(x.clone(), b"ok")], vec![])
            .await
            .unwrap();
        space
            .submit_checked(vec![set(k.clone(), b"stale")], vec![])
            .await
            .unwrap();
        space
            .submit_checked(vec![set(y.clone(), b"after")], vec![])
            .await
            .unwrap();

        // The merged group bounces; solo probes admit the healthy head
        // and convict exactly the faulty seq.
        let outcome = client.space(SPACE).await.unwrap().push().await.unwrap();
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
            client.space(SPACE).await.unwrap().push().await.unwrap(),
            PushOutcome::Drained {
                acked_through: Some(DeviceSeq(4))
            }
        );
        assert_eq!(queued(&mem).await, 0);

        assert_eq!(
            entry_value(&fetch(&handle, SPACE, &x).await.unwrap()),
            b"ok"
        );
        let foreign = fetch(&handle, SPACE, &k).await.unwrap();
        assert_eq!(entry_value(&foreign), b"foreign");
        assert_eq!(
            foreign.device_entry.tag.ver,
            Ver(100),
            "the stale write never landed"
        );
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
fn stale_delete_range_rolls_back_without_poisoning_later_submissions() {
    block_on(async {
        let mem = MemoryStore::new();
        let clock = ManualClock::new(Timestamp(0));
        let handle = spawn_server(Arc::new(ManualClock::new(Timestamp(0))), &[SPACE]);
        let db = key(&[b"db"]);
        let foreign = key(&[b"db", b"foreign"]);
        foreign_put(
            &handle,
            SPACE,
            dev(2),
            &db,
            vec![PendingEntry {
                key: foreign.clone(),
                value: val(b"foreign"),
                ver: Ver(100),
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
        let stale = space
            .submit_unchecked(
                vec![Mutation::DeleteRange {
                    range: Range::Prefix(db),
                }],
                vec![],
            )
            .await
            .unwrap();
        assert!(matches!(
            stale.push().await.unwrap(),
            PushReceipt::Failed {
                seq: DeviceSeq(1),
                error: KernelError::RangeVerRegression {
                    current: Ver(100),
                    attempted: Ver(1),
                    ..
                },
            }
        ));

        client.rollback(SPACE, DeviceSeq(1)).await.unwrap();
        assert_eq!(
            space.push().await.unwrap(),
            PushOutcome::Drained {
                acked_through: Some(DeviceSeq(2))
            }
        );
        let outside = key(&[b"outside", b"row"]);
        let accepted = space
            .submit_unchecked(vec![set(outside.clone(), b"ok")], vec![])
            .await
            .unwrap();
        assert!(matches!(
            accepted.push().await.unwrap(),
            PushReceipt::Applied {
                seq: DeviceSeq(3),
                admission_seq: Some(_),
            }
        ));
        assert_eq!(
            entry_value(&fetch(&handle, SPACE, &foreign).await.unwrap()),
            b"foreign"
        );
        assert_eq!(
            entry_value(&fetch(&handle, SPACE, &outside).await.unwrap()),
            b"ok"
        );
    });
}

#[test]
fn submission_push_stops_at_target_and_attributes_later_failure() {
    block_on(async {
        let mem = MemoryStore::new();
        let clock = ManualClock::new(Timestamp(0));
        let handle = spawn_server(Arc::new(ManualClock::new(Timestamp(0))), &[SPACE]);
        let db = key(&[b"db"]);
        let (first, stale, last) = (
            key(&[b"db", b"first"]),
            key(&[b"db", b"stale"]),
            key(&[b"db", b"last"]),
        );
        foreign_put(
            &handle,
            SPACE,
            dev(2),
            &db,
            vec![PendingEntry {
                key: stale.clone(),
                value: val(b"foreign"),
                ver: Ver(100),
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
        let target = space
            .submit_unchecked(vec![set(first.clone(), b"first")], vec![])
            .await
            .unwrap();
        let faulty = space
            .submit_unchecked(vec![set(stale.clone(), b"stale")], vec![])
            .await
            .unwrap();
        let dependent = space
            .submit_unchecked(vec![set(last.clone(), b"last")], vec![])
            .await
            .unwrap();

        assert_eq!(
            target.push().await.unwrap(),
            PushReceipt::Applied {
                seq: DeviceSeq(1),
                admission_seq: Some(AdmissionSeq(2)),
            }
        );
        assert_eq!(queued(&mem).await, 2, "later submissions stay local");
        assert_eq!(
            entry_value(&fetch(&handle, SPACE, &first).await.unwrap()),
            b"first"
        );
        assert!(fetch(&handle, SPACE, &last).await.is_none());

        assert!(matches!(
            dependent.push().await.unwrap(),
            PushReceipt::Blocked {
                seq: DeviceSeq(3),
                at: DeviceSeq(2),
                error: KernelError::VerRegression { .. },
            }
        ));
        assert!(matches!(
            faulty.push().await.unwrap(),
            PushReceipt::Failed {
                seq: DeviceSeq(2),
                error: KernelError::VerRegression { .. },
            }
        ));
        assert_eq!(queued(&mem).await, 2, "a stall never trims the suffix");
    });
}

#[test]
fn push_until_rejects_a_sequence_outside_the_active_oplog() {
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

        assert_eq!(
            space.push_until(DeviceSeq(1)).await.unwrap_err(),
            ClientError::Space(SpaceDriverError::SubmissionNotPending { seq: DeviceSeq(1) })
        );
    });
}

#[test]
fn push_until_leaves_later_submission_pending() {
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
        let keys = [
            key(&[b"db", b"one"]),
            key(&[b"db", b"two"]),
            key(&[b"db", b"three"]),
        ];
        for (index, key) in keys.iter().enumerate() {
            space
                .submit_unchecked(vec![set(key.clone(), &[index as u8])], vec![])
                .await
                .unwrap();
        }

        assert_eq!(
            space.push_until(DeviceSeq(2)).await.unwrap(),
            PushOutcome::Drained {
                acked_through: Some(DeviceSeq(2))
            }
        );
        assert!(fetch(&handle, SPACE, &keys[0]).await.is_some());
        assert!(fetch(&handle, SPACE, &keys[1]).await.is_some());
        assert!(fetch(&handle, SPACE, &keys[2]).await.is_none());
        let state = audit(&OrderedMetaStore::new(&mem)).await;
        assert_eq!(
            state.spaces[&SPACE]
                .active_oplog()
                .map(|(seq, _)| *seq)
                .collect::<Vec<_>>(),
            vec![DeviceSeq(3)]
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
        let granted = space.lease(vec![wspec(&db, 60)]).await.unwrap();
        let _lease_id = granted[0].id;
        space
            .submit_checked(vec![set(k1.clone(), b"a")], vec![])
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
            .submit_checked(vec![set(k2.clone(), b"twin")], vec![])
            .await
            .unwrap();
        assert_eq!(
            twin_client
                .space(SPACE)
                .await
                .unwrap()
                .push()
                .await
                .unwrap(),
            PushOutcome::Drained {
                acked_through: Some(DeviceSeq(2))
            }
        );

        // The original's push collides with a seq PAST its own mint
        // counter — proof it isn't looking at its own past. Fatal, and
        // nothing is destroyed.
        assert_eq!(
            client.space(SPACE).await.unwrap().push().await.unwrap_err(),
            ClientError::Space(SpaceDriverError::Fork {
                admitted: DeviceSeq(2)
            })
        );
        assert_eq!(queued(&mem).await, 1, "a fork verdict destroys nothing");
    });
}

#[test]
fn divergent_content_at_the_same_device_seq_is_a_fork() {
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

        let twin_mem = clone_store(&mem).await;
        let original = client.space(SPACE).await.unwrap();
        original
            .submit_unchecked(vec![set(key(&[b"db", b"row"]), b"original")], vec![])
            .await
            .unwrap();

        let twin = open_client(OrderedMetaStore::new(&twin_mem), &handle, &clock, dev(9))
            .await
            .unwrap();
        let twin_space = twin.space(SPACE).await.unwrap();
        twin_space
            .submit_unchecked(vec![set(key(&[b"db", b"row"]), b"twin")], vec![])
            .await
            .unwrap();
        twin_space.push().await.unwrap();

        assert_eq!(
            original.push().await.unwrap_err(),
            ClientError::Space(SpaceDriverError::Fork {
                admitted: DeviceSeq(1)
            })
        );
        assert_eq!(queued(&mem).await, 1, "fork detection must not trim");
    });
}

#[test]
fn lease_captures_the_barrier_and_dominates_foreign_vers() {
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
            vec![PendingEntry {
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
        let leased = space.lease(vec![wspec(&db, 60)]).await.unwrap();
        assert_eq!(leased[0].barrier, AdmissionSeq(1));
        // Lease capture raises tail and the version high-water, but does not
        // claim that the application has applied the captured admission.
        let state = audit(&OrderedMetaStore::new(&mem)).await;
        assert_eq!(
            state.spaces[&SPACE].admit_cursors,
            homebase_client::meta::AdmitCursors {
                head: AdmissionSeq(1),
                neck: AdmissionSeq(1),
                tail: AdmissionSeq(2),
            }
        );
        assert_eq!(state.spaces[&SPACE].ver_high, Some(Ver(7)));

        assert!(matches!(
            space
                .submit_checked(
                    vec![set(k.clone(), b"too-early")],
                    vec![RangeAssert {
                        prefix: db.clone(),
                        upto: AdmissionSeq(1),
                    }],
                )
                .await
                .unwrap_err(),
            SpaceDriverError::RangeAssertAuthority { .. }
        ));
        space.admits().mark_applied(AdmissionSeq(2)).await.unwrap();

        // Now the same key can be overwritten: the commit stamps above
        // the pulled ver, so the server's chain accepts it.
        space
            .submit_checked(
                vec![set(k.clone(), b"mine")],
                vec![RangeAssert {
                    prefix: db.clone(),
                    upto: AdmissionSeq(1),
                }],
            )
            .await
            .unwrap();
        assert_eq!(
            client.space(SPACE).await.unwrap().push().await.unwrap(),
            PushOutcome::Drained {
                acked_through: Some(DeviceSeq(1))
            }
        );
        let entry = fetch(&handle, SPACE, &k).await.unwrap();
        assert_eq!(entry_value(&entry), b"mine");
        assert_eq!(
            entry.device_entry.tag.ver,
            Ver(8),
            "stamped past the foreign chain"
        );

        // Stateless fetch starts after the barrier cut and carries exactly
        // our own admitted write.
        let pulled = space
            .fetch(Range::Prefix(db.clone()), AdmissionSeq(1))
            .await
            .unwrap();
        assert!(matches!(&pulled.cut, RangeCut::Delta(entries) if entries.len() == 1));

        // Fetch does not alter either admit-log cursor.
        let state = audit(&OrderedMetaStore::new(&mem)).await;
        assert_eq!(state.spaces[&SPACE].admit_cursors.neck, AdmissionSeq(2));
    });
}

#[test]
fn lease_acquires_pulls_and_makes_assertions_locally_authorized() {
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
            vec![PendingEntry {
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
        let acquired = space.lease(vec![wspec(&db, 60)]).await.unwrap();
        assert_eq!(acquired[0].barrier, AdmissionSeq(1));
        assert_eq!(acquired.len(), 1);
        let cursors = space.admits().cursors().await.unwrap();
        assert_eq!(cursors.tail, AdmissionSeq(2));
        assert_eq!(cursors.neck, AdmissionSeq(1));

        space
            .submit_checked(vec![set(k.clone(), b"mine")], vec![])
            .await
            .unwrap();
        assert_eq!(
            client.space(SPACE).await.unwrap().push().await.unwrap(),
            PushOutcome::Drained {
                acked_through: Some(DeviceSeq(1))
            }
        );
        let entry = fetch(&handle, SPACE, &k).await.unwrap();
        assert_eq!(entry_value(&entry), b"mine");
        assert_eq!(entry.device_entry.tag.ver, Ver(8));
    });
}

#[test]
fn lease_satisfies_read_lease_barriers_too() {
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
            vec![PendingEntry {
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
        let acquired = space.lease(vec![rspec(&db, 60)]).await.unwrap();
        assert_eq!(acquired[0].barrier, AdmissionSeq(1));
        assert_eq!(acquired[0].mode, LeaseMode::Read);
        assert_eq!(
            space.admits().cursors().await.unwrap().tail,
            AdmissionSeq(2)
        );
        assert_eq!(
            space.admits().cursors().await.unwrap().neck,
            AdmissionSeq(1)
        );
        assert!(!space.leases(std::slice::from_ref(&db)).await.unwrap()[0].usable);
        space.admits().mark_applied(AdmissionSeq(2)).await.unwrap();
        assert!(space.leases(std::slice::from_ref(&db)).await.unwrap()[0].usable);
    });
}

#[test]
fn captured_lease_stays_unusable_across_reopen_until_neck_crosses_barrier() {
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
            vec![PendingEntry {
                key: key(&[b"db", b"row"]),
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
        let lease = space.lease(vec![wspec(&db, 60)]).await.unwrap()[0].clone();
        assert_eq!(lease.barrier, AdmissionSeq(1));
        assert_eq!(
            space.admits().cursors().await.unwrap().tail,
            AdmissionSeq(2)
        );
        drop(space);
        drop(client);

        let client = open_client(OrderedMetaStore::new(&mem), &handle, &clock, dev(1))
            .await
            .unwrap();
        client
            .attach(&SpaceEnvelope::plaintext(SPACE))
            .await
            .unwrap();
        let space = client.space(SPACE).await.unwrap();
        assert!(!space.leases(std::slice::from_ref(&db)).await.unwrap()[0].usable);
        assert!(matches!(
            space
                .submit_checked(
                    Vec::<Mutation>::new(),
                    vec![RangeAssert {
                        prefix: db.clone(),
                        upto: AdmissionSeq(0),
                    }],
                )
                .await
                .unwrap_err(),
            SpaceDriverError::RangeAssertAuthority { .. }
        ));

        space.admits().mark_applied(AdmissionSeq(2)).await.unwrap();
        assert!(space.leases(std::slice::from_ref(&db)).await.unwrap()[0].usable);
        space
            .submit_checked(
                Vec::<Mutation>::new(),
                vec![RangeAssert {
                    prefix: db,
                    upto: AdmissionSeq(1),
                }],
            )
            .await
            .unwrap();
    });
}

#[test]
fn application_does_not_resurrect_expired_or_unleased_reservations() {
    block_on(async {
        let mem = MemoryStore::new();
        let clock = ManualClock::new(Timestamp(0));
        let handle = spawn_server(Arc::new(ManualClock::new(Timestamp(0))), &[SPACE]);
        let db = key(&[b"db"]);
        let other = key(&[b"other"]);
        foreign_put(
            &handle,
            SPACE,
            dev(2),
            &db,
            vec![PendingEntry {
                key: key(&[b"db", b"row"]),
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
        let leases = space
            .lease(vec![wspec(&db, 1), rspec(&other, 60)])
            .await
            .unwrap();
        let other_id = leases
            .iter()
            .find(|lease| lease.prefix == other)
            .unwrap()
            .id;
        space.unlease_unchecked(&[other_id]).await.unwrap();
        clock.advance(Duration::from_secs(2));

        space.admits().mark_applied(AdmissionSeq(2)).await.unwrap();
        assert!(!space.leases(std::slice::from_ref(&db)).await.unwrap()[0].usable);
        assert!(
            space
                .leases(std::slice::from_ref(&other))
                .await
                .unwrap()
                .is_empty()
        );
    });
}

#[test]
fn pending_unlease_blocks_checked_assertions_and_retries_explicitly() {
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
            let granted = space.lease(vec![wspec(&db, 60)]).await.unwrap();
            granted[0].id
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
            space.unlease_unchecked(&[lease_id]).await.unwrap_err(),
            SpaceDriverError::Unavailable { .. }
        ));
        let state = audit(&OrderedMetaStore::new(&mem)).await;
        assert!(state.spaces[&SPACE].leases[&lease_id].forgotten);
        assert!(matches!(
            space
                .submit_checked(
                    vec![set(key(&[b"db", b"k"]), b"v")],
                    vec![RangeAssert {
                        prefix: db.clone(),
                        upto: AdmissionSeq(0),
                    }],
                )
                .await
                .unwrap_err(),
            SpaceDriverError::RangeAssertAuthority { .. }
        ));

        // Reopening does not do hidden server work. The forgotten lease is
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
        assert!(!leases[0].usable);

        // An explicit unlease retry finishes the saga and drops the local
        // record; a new device can acquire immediately.
        space.unlease_unchecked(&[lease_id]).await.unwrap();
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
fn checked_unlease_preserves_checked_assertion_reservations() {
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
        let granted = space.lease(vec![wspec(&db, 60)]).await.unwrap();
        space
            .submit_checked(
                vec![set(k, b"queued")],
                vec![RangeAssert {
                    prefix: db.clone(),
                    upto: AdmissionSeq(0),
                }],
            )
            .await
            .unwrap();

        assert!(matches!(
            space
                .unlease_checked(&[granted[0].id])
                .await
                .unwrap_err(),
            SpaceDriverError::UnleaseBlocked {
                lease,
                at: DeviceSeq(1)
            } if lease == granted[0].id
        ));
        assert!(
            !audit(&OrderedMetaStore::new(&mem)).await.spaces[&SPACE].leases[&granted[0].id]
                .forgotten
        );

        client.rollback(SPACE, DeviceSeq(1)).await.unwrap();
        space.unlease_checked(&[granted[0].id]).await.unwrap();
        assert!(
            audit(&OrderedMetaStore::new(&mem)).await.spaces[&SPACE]
                .leases
                .is_empty(),
            "retired assertions below neck no longer block checked unlease"
        );
    });
}

#[test]
fn checked_unlease_ignores_unchecked_assertions() {
    block_on(async {
        let mem = MemoryStore::new();
        let clock = ManualClock::new(Timestamp(0));
        let handle = spawn_server(Arc::new(ManualClock::new(Timestamp(0))), &[SPACE]);
        let db = key(&[b"db"]);
        let client = open_client(OrderedMetaStore::new(&mem), &handle, &clock, dev(1))
            .await
            .unwrap();
        client
            .attach(&SpaceEnvelope::plaintext(SPACE))
            .await
            .unwrap();
        let space = client.space(SPACE).await.unwrap();
        let granted = space.lease(vec![rspec(&db, 60)]).await.unwrap();
        let submission = space
            .submit_unchecked(
                Vec::<Mutation>::new(),
                vec![RangeAssert {
                    prefix: db,
                    upto: AdmissionSeq(0),
                }],
            )
            .await
            .unwrap();
        let state = audit(&OrderedMetaStore::new(&mem)).await;
        assert_eq!(
            state.spaces[&SPACE].oplog[&submission.seq].submit_mode(),
            Some(SubmitMode::Unchecked)
        );

        space.unlease_checked(&[granted[0].id]).await.unwrap();
    });
}

#[test]
fn checked_unlease_accepts_replacement_coverage() {
    block_on(async {
        let mem = MemoryStore::new();
        let clock = ManualClock::new(Timestamp(0));
        let handle = spawn_server(Arc::new(ManualClock::new(Timestamp(0))), &[SPACE]);
        let db = key(&[b"db"]);
        let child = key(&[b"db", b"child"]);
        let client = open_client(OrderedMetaStore::new(&mem), &handle, &clock, dev(1))
            .await
            .unwrap();
        client
            .attach(&SpaceEnvelope::plaintext(SPACE))
            .await
            .unwrap();
        let space = client.space(SPACE).await.unwrap();
        let parent = space.lease(vec![rspec(&db, 60)]).await.unwrap()[0].clone();
        space
            .submit_checked(
                Vec::<Mutation>::new(),
                vec![RangeAssert {
                    prefix: child.clone(),
                    upto: AdmissionSeq(0),
                }],
            )
            .await
            .unwrap();

        handle
            .admit(
                &SPACE,
                AdmissionRequest {
                    device: dev(2),
                    expected_checksum: homebase_core::DeviceChecksum::EMPTY,
                    evidence: vec![],
                    batches: vec![AdmissionBatch {
                        device_seq: DeviceSeq(1),
                        range_asserts: vec![],
                        entries: vec![],
                    }],
                },
            )
            .await
            .unwrap();
        let replacement = handle
            .acquire(
                &SPACE,
                AcquireRequest {
                    device: dev(1),
                    requested_at: hstamp(0, 1),
                    specs: vec![rspec(&child, 60)],
                },
            )
            .await
            .unwrap()
            .leases
            .remove(0);
        assert_eq!(replacement.barrier, AdmissionSeq(1));

        space.repair_leases().await.unwrap();
        assert_eq!(
            space.admits().cursors().await.unwrap().neck,
            AdmissionSeq(1)
        );
        let replacement_state = space
            .leases(std::slice::from_ref(&child))
            .await
            .unwrap()
            .into_iter()
            .find(|state| state.held.lease.id == replacement.id)
            .unwrap();
        assert!(!replacement_state.usable);

        space.unlease_checked(&[parent.id]).await.unwrap();
        let state = audit(&OrderedMetaStore::new(&mem)).await;
        assert!(state.spaces[&SPACE].leases.contains_key(&replacement.id));
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
        space.lease(vec![wspec(&db, 60)]).await.unwrap();
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
            .submit_checked(vec![set(key(&[b"db", b"k"]), b"v")], vec![])
            .await
            .unwrap();
        assert!(matches!(
            client.space(SPACE).await.unwrap().push().await.unwrap_err(),
            ClientError::Space(SpaceDriverError::Unavailable { .. })
        ));
        assert_eq!(queued(&mem).await, 1, "transport failure judges nothing");
    });
}

#[test]
fn lease_reacquires_when_renewal_reports_invalid() {
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
        let granted = space.lease(vec![wspec(&db, 60)]).await.unwrap();
        let lease_id = granted[0].id;

        // The SERVER's clock passes the deadline: strict local expiry,
        // the lease is gone there. Renewal is how this side finds out.
        server_clock.advance(Duration::from_secs(120));
        clock.advance(Duration::from_secs(120));
        let reacquired = space.lease(vec![wspec(&db, 60)]).await.unwrap();
        assert_eq!(reacquired.len(), 1);
        assert_ne!(reacquired[0].id, lease_id);

        // The invalid record was removed and replaced everywhere.
        assert_eq!(
            space.leases(std::slice::from_ref(&db)).await.unwrap().len(),
            1
        );
        let state = audit(&OrderedMetaStore::new(&mem)).await;
        assert_eq!(state.spaces[&SPACE].leases.len(), 1);
        assert!(state.spaces[&SPACE].leases.contains_key(&reacquired[0].id));
    });
}
