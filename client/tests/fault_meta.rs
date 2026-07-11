//! Client meta over the sim's fault-injecting [`SimStore`]: durability
//! boundary, crash/reopen, and ack-drop recovery.

use homebase::cipher::{SpaceEnvelope, SystemNonceSource};
use homebase::meta::{MetaStore, OrderedMetaStore, audit, conformance};
use homebase::server::ServerHandle;
use homebase::{Client, PushOutcome};
use homebase_core::clock::{ManualClock, Timestamp};
use homebase_core::key::Key;
use homebase_core::lease::LeaseMode;
use homebase_core::messages::{GetRequest, LeaseSpec, PutBatch, PutBatchRequest, PutEntry};
use homebase_core::space::SpaceId;
use homebase_core::storage::MemoryStore;
use homebase_core::tag::{DeviceId, DeviceSeq, Value, Ver};
use homebase_server::Server;
use homebase_server::actor::{SpaceHandle, Spawner};
use homebase_sim::store::{FaultConfig, SimStore};
use pollster::block_on;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

const SPACE: SpaceId = SpaceId([4; 16]);
const SEEDS: u64 = 10;

struct ThreadSpawner;

impl Spawner for ThreadSpawner {
    fn spawn(&self, task: Pin<Box<dyn Future<Output = ()> + Send + 'static>>) {
        std::thread::spawn(move || pollster::block_on(task));
    }
}

fn dev(n: u8) -> DeviceId {
    DeviceId([n; 16])
}

fn key(parts: &[&[u8]]) -> Key {
    Key::from_bytes(parts.iter().copied()).unwrap()
}

fn val(bytes: &[u8]) -> Value {
    Value::Present(bytes.to_vec())
}

fn wspec(prefix: &Key) -> LeaseSpec {
    LeaseSpec {
        prefix: prefix.clone(),
        mode: LeaseMode::Write,
        ttl: Duration::from_secs(60),
    }
}

fn spawn_server() -> impl Fn(&SpaceId) -> Option<SpaceHandle> + Sync {
    let server = Arc::new(Server::new(
        Arc::new(MemoryStore::new()),
        Arc::new(ManualClock::new(Timestamp(0))),
        ThreadSpawner,
    ));
    assert!(server.create_space(SPACE));
    move |id: &SpaceId| server.space(id)
}

async fn fetch(handle: &impl ServerHandle, k: &Key) -> Value {
    handle
        .get(
            &SPACE,
            GetRequest {
                keys: vec![k.clone()],
            },
        )
        .await
        .unwrap()
        .entries
        .remove(0)
        .unwrap()
        .value
}

#[test]
fn simstore_meta_passes_conformance() {
    block_on(async {
        let store = SimStore::new(0, FaultConfig::NONE);
        store.flush();
        let meta = OrderedMetaStore::new(store);
        conformance::run_all(&meta).await;
    });
}

#[test]
fn flushed_commit_survives_crash_and_pushes_after_reopen() {
    for seed in 0..SEEDS {
        block_on(run_flushed_crash_seed(seed));
    }
}

async fn run_flushed_crash_seed(seed: u64) {
    let sim = SimStore::new(seed, FaultConfig::NONE);
    let mem = OrderedMetaStore::new(sim.clone());
    let clock = ManualClock::new(Timestamp(0));
    let handle = spawn_server();
    let db = key(&[b"db"]);
    let row = key(&[b"db", b"k"]);

    {
        let client = Client::open(mem, &handle, &clock, dev(1), SystemNonceSource)
            .await
            .unwrap();
        client
            .attach(&SpaceEnvelope::plaintext(SPACE))
            .await
            .unwrap();
        let space = client.space(SPACE).await.unwrap();
        space.acquire(vec![wspec(&db)]).await.unwrap();
        space
            .commit(vec![(row.clone(), val(b"survived"))])
            .await
            .unwrap();
        sim.flush();
    }

    sim.crash();

    let mem = OrderedMetaStore::new(sim);
    let client = Client::open(mem, &handle, &clock, dev(1), SystemNonceSource)
        .await
        .unwrap();
    let space = client.space(SPACE).await.unwrap();
    space.ensure(vec![wspec(&db)]).await.unwrap();
    assert_eq!(
        client.push().await.unwrap(),
        PushOutcome::Drained {
            acked_through: Some(DeviceSeq(1))
        }
    );
    assert_eq!(fetch(&handle, &row).await, val(b"survived"));
}

#[test]
fn unflushed_commit_is_lost_on_crash() {
    block_on(async {
        let sim = SimStore::new(
            42,
            FaultConfig {
                error_rate: 0.0,
                flush_rate: 0.0,
                max_latency_yields: 0,
            },
        );
        let mem = OrderedMetaStore::new(sim.clone());
        let clock = ManualClock::new(Timestamp(0));
        let handle = spawn_server();
        let db = key(&[b"db"]);
        let row = key(&[b"db", b"k"]);

        {
            let client = Client::open(mem, &handle, &clock, dev(1), SystemNonceSource)
                .await
                .unwrap();
            client
                .attach(&SpaceEnvelope::plaintext(SPACE))
                .await
                .unwrap();
            let space = client.space(SPACE).await.unwrap();
            space.acquire(vec![wspec(&db)]).await.unwrap();
            space
                .commit(vec![(row.clone(), val(b"volatile"))])
                .await
                .unwrap();
        }

        sim.crash();
        let state = audit(&OrderedMetaStore::new(sim)).await;
        assert!(
            state.spaces.values().all(|space| space.oplog.is_empty()),
            "unflushed commit must not survive"
        );
    });
}

#[test]
fn flushed_rollback_survives_crash_as_one_cursor_marker_transition() {
    block_on(async {
        let sim = SimStore::new(43, FaultConfig::NONE);
        let mem = OrderedMetaStore::new(sim.clone());
        let clock = ManualClock::new(Timestamp(0));
        let handle = spawn_server();
        let db = key(&[b"db"]);

        let client = Client::open(mem, &handle, &clock, dev(1), SystemNonceSource)
            .await
            .unwrap();
        client
            .attach(&SpaceEnvelope::plaintext(SPACE))
            .await
            .unwrap();
        let space = client.space(SPACE).await.unwrap();
        space.acquire(vec![wspec(&db)]).await.unwrap();
        space
            .commit(vec![(key(&[b"db", b"a"]), val(b"one"))])
            .await
            .unwrap();
        space
            .commit(vec![(key(&[b"db", b"b"]), val(b"two"))])
            .await
            .unwrap();
        client.rollback(SPACE, DeviceSeq(2)).await.unwrap();
        sim.flush();
        drop(client);

        sim.crash();
        let state = audit(&OrderedMetaStore::new(sim.clone())).await;
        assert_eq!(state.spaces[&SPACE].cursors.head, DeviceSeq(1));
        assert_eq!(state.spaces[&SPACE].cursors.neck, DeviceSeq(3));
        assert_eq!(state.spaces[&SPACE].cursors.tail, DeviceSeq(4));
        assert_eq!(state.spaces[&SPACE].oplog.len(), 3);

        let reopened = Client::open(
            OrderedMetaStore::new(sim),
            &handle,
            &clock,
            dev(1),
            SystemNonceSource,
        )
        .await
        .unwrap();
        assert_eq!(
            reopened.push().await.unwrap(),
            PushOutcome::Drained {
                acked_through: Some(DeviceSeq(3))
            }
        );
    });
}

#[test]
fn unflushed_rollback_crash_recovers_the_complete_pre_transition_state() {
    block_on(async {
        let sim = SimStore::new(
            44,
            FaultConfig {
                error_rate: 0.0,
                flush_rate: 0.0,
                max_latency_yields: 0,
            },
        );
        let meta = OrderedMetaStore::new(sim.clone());
        for name in [b"a".as_slice(), b"b".as_slice()] {
            let reserved = meta
                .reserve_commit(
                    SPACE,
                    vec![(key(&[b"db", name]), Value::Present(name.to_vec()))],
                )
                .await
                .unwrap();
            meta.commit(SPACE, reserved).await.unwrap();
        }
        sim.flush();

        meta.rollback(SPACE, DeviceSeq(2)).await.unwrap();
        sim.crash();

        let state = audit(&OrderedMetaStore::new(sim)).await;
        assert_eq!(state.spaces[&SPACE].cursors.head, DeviceSeq(1));
        assert_eq!(state.spaces[&SPACE].cursors.neck, DeviceSeq(1));
        assert_eq!(state.spaces[&SPACE].cursors.tail, DeviceSeq(3));
        assert_eq!(
            state.spaces[&SPACE]
                .oplog
                .keys()
                .copied()
                .collect::<Vec<_>>(),
            vec![DeviceSeq(1), DeviceSeq(2)],
            "neither the volatile marker nor its cursor update survived"
        );
    });
}

#[test]
fn rollback_marker_ack_drop_recovers_by_trimming_dead_history() {
    block_on(async {
        let sim = SimStore::new(45, FaultConfig::NONE);
        let meta = OrderedMetaStore::new(sim.clone());
        let clock = ManualClock::new(Timestamp(0));
        let handle = spawn_server();
        let client = Client::open(meta, &handle, &clock, dev(1), SystemNonceSource)
            .await
            .unwrap();
        client
            .attach(&SpaceEnvelope::plaintext(SPACE))
            .await
            .unwrap();
        let space = client.space(SPACE).await.unwrap();
        space.acquire(vec![wspec(&key(&[b"db"]))]).await.unwrap();
        space
            .commit(vec![(key(&[b"db", b"a"]), val(b"one"))])
            .await
            .unwrap();
        space
            .commit(vec![(key(&[b"db", b"b"]), val(b"two"))])
            .await
            .unwrap();
        client.rollback(SPACE, DeviceSeq(2)).await.unwrap();
        sim.flush();

        // Admit only the rollback marker and drop the response before the
        // client can trim its retained dead prefix.
        handle
            .put_batch(
                &SPACE,
                PutBatchRequest {
                    device: client.device(),
                    evidence: vec![],
                    batches: vec![PutBatch {
                        device_seq: DeviceSeq(3),
                        range_asserts: vec![],
                        ops: vec![],
                    }],
                },
            )
            .await
            .unwrap();
        drop(client);
        sim.crash();

        let reopened = Client::open(
            OrderedMetaStore::new(sim.clone()),
            &handle,
            &clock,
            dev(1),
            SystemNonceSource,
        )
        .await
        .unwrap();
        assert_eq!(
            reopened.push().await.unwrap(),
            PushOutcome::Drained {
                acked_through: Some(DeviceSeq(3))
            }
        );
        assert!(
            audit(&OrderedMetaStore::new(sim)).await.spaces[&SPACE]
                .oplog
                .is_empty()
        );
    });
}

#[test]
fn ack_drop_trims_after_server_admitted() {
    block_on(async {
        let sim = SimStore::new(7, FaultConfig::NONE);
        let mem = OrderedMetaStore::new(sim.clone());
        let clock = ManualClock::new(Timestamp(0));
        let handle = spawn_server();
        let db = key(&[b"db"]);
        let k1 = key(&[b"db", b"k1"]);
        let k2 = key(&[b"db", b"k2"]);

        let client = Client::open(mem, &handle, &clock, dev(1), SystemNonceSource)
            .await
            .unwrap();
        client
            .attach(&SpaceEnvelope::plaintext(SPACE))
            .await
            .unwrap();
        let space = client.space(SPACE).await.unwrap();
        let granted = space.acquire(vec![wspec(&db)]).await.unwrap();
        let lease = granted.leases[0].id;
        space.commit(vec![(k1.clone(), val(b"one"))]).await.unwrap();
        space.commit(vec![(k2.clone(), val(b"two"))]).await.unwrap();
        sim.flush();

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
            .unwrap();

        assert_eq!(
            client.push().await.unwrap(),
            PushOutcome::Drained {
                acked_through: Some(DeviceSeq(2))
            }
        );
        assert!(
            audit(&OrderedMetaStore::new(sim)).await.spaces[&SPACE]
                .oplog
                .is_empty()
        );
    });
}
