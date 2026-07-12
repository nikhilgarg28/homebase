//! Client meta over the sim's fault-injecting [`SimStore`]: durability
//! boundary, crash/reopen, and ack-drop recovery.

use homebase::cipher::{SpaceEnvelope, SystemNonceSource};
use homebase::meta::{MetaStore, OrderedMetaStore, SubmitMode, audit, conformance};
use homebase::server::ServerHandle;
use homebase::{Client, PushOutcome};
use homebase_core::clock::{ManualClock, Timestamp};
use homebase_core::key::Key;
use homebase_core::lease::LeaseMode;
use homebase_core::messages::{
    AdmissionBatch, AdmissionRequest, AdmittedBatch, GetRequest, LeaseSpec, PullResponse,
};
use homebase_core::seal::Seal;
use homebase_core::space::SpaceId;
use homebase_core::storage::MemoryStore;
use homebase_core::tag::{
    AdmissionSeq, AdmissionTag, AdmittedEntry, CipherEpoch, DeviceChecksum, DeviceEntry, DeviceId,
    DeviceSeq, DeviceTag, Mutation, OpaqueValue, Ver,
};
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

fn val(bytes: &[u8]) -> Vec<u8> {
    bytes.to_vec()
}

fn set(key: Key, bytes: &[u8]) -> Mutation {
    Mutation::Set {
        key,
        value: val(bytes),
    }
}

fn wire_entry(device: DeviceId, seq: DeviceSeq, key: Key, value: &[u8], ver: Ver) -> DeviceEntry {
    DeviceEntry {
        mutation: Mutation::Set {
            key,
            value: OpaqueValue(val(value)),
        },
        tag: DeviceTag {
            device,
            device_seq: seq,
            ver,
            cipher_epoch: CipherEpoch(0),
        },
        seal: Seal::empty_aead_v1(),
    }
}

fn pulled() -> PullResponse {
    let device = dev(2);
    PullResponse {
        after: AdmissionSeq(0),
        through: AdmissionSeq(2),
        batches: vec![
            AdmittedBatch {
                admission_seq: AdmissionSeq(1),
                device,
                device_seq: DeviceSeq(1),
                checksum: DeviceChecksum([1; 32]),
                entries: vec![AdmittedEntry {
                    device_entry: wire_entry(
                        device,
                        DeviceSeq(1),
                        key(&[b"db", b"remote"]),
                        b"value",
                        Ver(1),
                    ),
                    admission: AdmissionTag {
                        admission_seq: AdmissionSeq(1),
                        op_index: 0,
                    },
                }],
            },
            AdmittedBatch {
                admission_seq: AdmissionSeq(2),
                device,
                device_seq: DeviceSeq(2),
                checksum: DeviceChecksum([2; 32]),
                entries: vec![],
            },
        ],
    }
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

async fn fetch(handle: &impl ServerHandle, k: &Key) -> Vec<u8> {
    let entry = handle
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
        .unwrap();
    match entry.device_entry.mutation {
        Mutation::Set { value, .. } => value.0,
        Mutation::Delete { .. } => panic!("expected live value"),
    }
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
fn admit_log_transitions_are_atomic_across_crash_and_reopen() {
    block_on(async {
        let sim = SimStore::new(
            123,
            FaultConfig {
                error_rate: 0.0,
                flush_rate: 0.0,
                max_latency_yields: 0,
            },
        );
        sim.flush();
        let response = pulled();

        let meta = OrderedMetaStore::new(sim.clone());
        meta.append_admits(SPACE, &response).await.unwrap();
        sim.crash();
        let meta = OrderedMetaStore::new(sim.clone());
        assert_eq!(
            meta.admit_cursors(SPACE).await.unwrap(),
            homebase::meta::AdmitCursors::default(),
            "an unflushed append vanishes completely"
        );
        assert_eq!(
            audit(&meta)
                .await
                .spaces
                .get(&SPACE)
                .and_then(|s| s.ver_high),
            None
        );

        meta.append_admits(SPACE, &response).await.unwrap();
        sim.flush();
        sim.crash();
        let meta = OrderedMetaStore::new(sim.clone());
        let state = audit(&meta).await;
        assert_eq!(state.spaces[&SPACE].admit_cursors.tail, AdmissionSeq(3));
        assert_eq!(state.spaces[&SPACE].admits.len(), 2);
        assert_eq!(state.spaces[&SPACE].ver_high, Some(Ver(1)));

        meta.mark_admits_applied(SPACE, AdmissionSeq(3))
            .await
            .unwrap();
        sim.crash();
        let meta = OrderedMetaStore::new(sim.clone());
        assert_eq!(
            audit(&meta).await.spaces[&SPACE].admit_cursors.neck,
            AdmissionSeq(1),
            "an unflushed mark leaves application coverage unchanged"
        );

        meta.mark_admits_applied(SPACE, AdmissionSeq(3))
            .await
            .unwrap();
        sim.flush();
        sim.crash();
        let meta = OrderedMetaStore::new(sim.clone());
        assert_eq!(
            audit(&meta).await.spaces[&SPACE].admit_cursors.neck,
            AdmissionSeq(3)
        );

        meta.trim_admits(SPACE, AdmissionSeq(2)).await.unwrap();
        sim.crash();
        let meta = OrderedMetaStore::new(sim.clone());
        let state = audit(&meta).await;
        assert_eq!(state.spaces[&SPACE].admit_cursors.head, AdmissionSeq(1));
        assert_eq!(state.spaces[&SPACE].admits.len(), 2);

        meta.trim_admits(SPACE, AdmissionSeq(2)).await.unwrap();
        sim.flush();
        sim.crash();
        let meta = OrderedMetaStore::new(sim);
        let state = audit(&meta).await;
        assert_eq!(state.spaces[&SPACE].admit_cursors.head, AdmissionSeq(2));
        assert_eq!(
            state.spaces[&SPACE]
                .admits
                .keys()
                .copied()
                .collect::<Vec<_>>(),
            vec![AdmissionSeq(2)]
        );
    });
}

#[test]
fn trim_and_confirmed_checksum_are_one_atomic_transition() {
    block_on(async {
        let sim = SimStore::new(99, FaultConfig::NONE);
        let meta = OrderedMetaStore::new(sim.clone());
        let reserved = meta
            .reserve_commit(SPACE, 1, vec![], SubmitMode::Unchecked)
            .await
            .unwrap();
        meta.commit(
            SPACE,
            reserved.clone(),
            vec![wire_entry(
                dev(1),
                reserved.seq,
                key(&[b"db", b"row"]),
                b"value",
                Ver(1),
            )],
        )
        .await
        .unwrap();
        sim.flush();

        sim.set_config(FaultConfig {
            error_rate: 1.0,
            flush_rate: 1.0,
            max_latency_yields: 0,
        });
        let confirmed = DeviceChecksum([7; 32]);
        assert!(
            meta.trim_oplog(SPACE, reserved.seq, confirmed)
                .await
                .is_err()
        );
        sim.set_config(FaultConfig::NONE);
        let unchanged = audit(&meta).await;
        assert_eq!(unchanged.spaces[&SPACE].checksum, DeviceChecksum::EMPTY);
        assert_eq!(unchanged.spaces[&SPACE].cursors.neck, DeviceSeq(1));
        assert!(unchanged.spaces[&SPACE].oplog.contains_key(&reserved.seq));

        meta.trim_oplog(SPACE, reserved.seq, confirmed)
            .await
            .unwrap();
        sim.flush();
        sim.crash();
        let advanced = audit(&meta).await;
        assert_eq!(advanced.spaces[&SPACE].checksum, confirmed);
        assert_eq!(advanced.spaces[&SPACE].cursors.neck, DeviceSeq(2));
        assert!(advanced.spaces[&SPACE].oplog.is_empty());

        assert!(
            meta.trim_oplog(SPACE, reserved.seq, DeviceChecksum([8; 32]))
                .await
                .is_err(),
            "an idempotent re-ack cannot rewrite confirmed history"
        );
        assert_eq!(audit(&meta).await.spaces[&SPACE].checksum, confirmed);
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
        space.lease(vec![wspec(&db)]).await.unwrap();
        space
            .submit_checked(vec![set(row.clone(), b"survived")], vec![])
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
    space.lease(vec![wspec(&db)]).await.unwrap();
    assert_eq!(
        client.space(SPACE).await.unwrap().push().await.unwrap(),
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
            space.lease(vec![wspec(&db)]).await.unwrap();
            space
                .submit_checked(vec![set(row.clone(), b"volatile")], vec![])
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
        space.lease(vec![wspec(&db)]).await.unwrap();
        space
            .submit_checked(vec![set(key(&[b"db", b"a"]), b"one")], vec![])
            .await
            .unwrap();
        space
            .submit_checked(vec![set(key(&[b"db", b"b"]), b"two")], vec![])
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
            reopened.space(SPACE).await.unwrap().push().await.unwrap(),
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
                .reserve_commit(SPACE, 1, Vec::new(), SubmitMode::Unchecked)
                .await
                .unwrap();
            let entry = wire_entry(
                dev(1),
                reserved.seq,
                key(&[b"db", name]),
                name,
                reserved.versions[0],
            );
            meta.commit(SPACE, reserved, vec![entry]).await.unwrap();
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
        space.lease(vec![wspec(&key(&[b"db"]))]).await.unwrap();
        space
            .submit_checked(vec![set(key(&[b"db", b"a"]), b"one")], vec![])
            .await
            .unwrap();
        space
            .submit_checked(vec![set(key(&[b"db", b"b"]), b"two")], vec![])
            .await
            .unwrap();
        client.rollback(SPACE, DeviceSeq(2)).await.unwrap();
        sim.flush();

        // Admit only the rollback marker and drop the response before the
        // client can trim its retained dead prefix.
        handle
            .admit(
                &SPACE,
                AdmissionRequest {
                    device: client.device(),
                    expected_checksum: homebase_core::DeviceChecksum::EMPTY,
                    evidence: vec![],
                    batches: vec![AdmissionBatch {
                        device_seq: DeviceSeq(3),
                        range_asserts: vec![],
                        entries: vec![],
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
            reopened.space(SPACE).await.unwrap().push().await.unwrap(),
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
        let granted = space.lease(vec![wspec(&db)]).await.unwrap();
        let lease = granted[0].id;
        space
            .submit_checked(vec![set(k1.clone(), b"one")], vec![])
            .await
            .unwrap();
        space
            .submit_checked(vec![set(k2.clone(), b"two")], vec![])
            .await
            .unwrap();
        sim.flush();

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
                                k1.clone(),
                                b"one",
                                Ver(1),
                            )],
                        },
                        AdmissionBatch {
                            device_seq: DeviceSeq(2),
                            range_asserts: vec![],
                            entries: vec![wire_entry(
                                client.device(),
                                DeviceSeq(2),
                                k2.clone(),
                                b"two",
                                Ver(2),
                            )],
                        },
                    ],
                },
            )
            .await
            .unwrap();

        assert_eq!(
            client.space(SPACE).await.unwrap().push().await.unwrap(),
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
