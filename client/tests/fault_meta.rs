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
        let mut space = client.space(SPACE).await.unwrap();
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
    let mut space = client.space(SPACE).await.unwrap();
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
            let mut space = client.space(SPACE).await.unwrap();
            space.acquire(vec![wspec(&db)]).await.unwrap();
            space
                .commit(vec![(row.clone(), val(b"volatile"))])
                .await
                .unwrap();
        }

        sim.crash();
        let state = audit(&OrderedMetaStore::new(sim)).await;
        assert!(state.oplog.is_empty(), "unflushed commit must not survive");
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
        let mut space = client.space(SPACE).await.unwrap();
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
        assert!(audit(&OrderedMetaStore::new(sim)).await.oplog.is_empty());
    });
}
