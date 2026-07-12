//! Encrypted-space crash-resume and ack-drop tortures.

use homebase::cipher::{
    NameKey, NonceSource, SpaceEnvelope, SpaceKey, SystemNonceSource, ValueNonce,
};
use homebase::meta::{OrderedMetaStore, audit};
use homebase::server::ServerHandle;
use homebase::{Client, Mutation, PushOutcome};
use homebase_core::clock::{HybridTimestamp, Lineage, ManualClock, Timestamp};
use homebase_core::key::Key;
use homebase_core::lease::LeaseMode;
use homebase_core::messages::{
    AdmissionBatch, AdmissionRequest, GetRequest, LeaseSpec, Range, RangeCut,
};
use homebase_core::space::SpaceId;
use homebase_core::storage::MemoryStore;
use homebase_core::tag::{AdmissionSeq, AdmittedEntry, DeviceId, DeviceSeq};
use homebase_server::Server;
use homebase_server::actor::{SpaceHandle, Spawner};
use pollster::block_on;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

struct ThreadSpawner;

impl Spawner for ThreadSpawner {
    fn spawn(&self, task: Pin<Box<dyn Future<Output = ()> + Send + 'static>>) {
        std::thread::spawn(move || pollster::block_on(task));
    }
}

#[derive(Clone)]
struct TestNonceSource {
    next: u8,
}

impl TestNonceSource {
    fn new(first: u8) -> Self {
        Self { next: first }
    }
}

impl NonceSource for TestNonceSource {
    fn next_nonce(&mut self) -> Result<ValueNonce, String> {
        let nonce = ValueNonce([self.next; 24]);
        self.next = self.next.wrapping_add(1);
        Ok(nonce)
    }
}

fn dev(n: u8) -> DeviceId {
    DeviceId([n; 16])
}

fn key(parts: &[&[u8]]) -> Key {
    Key::from_bytes(parts.iter().copied()).unwrap()
}

fn set(key: Key, bytes: &[u8]) -> Mutation {
    Mutation::Set {
        key,
        value: bytes.to_vec(),
    }
}

fn value(entry: &AdmittedEntry<Vec<u8>>) -> &[u8] {
    match &entry.device_entry.mutation {
        Mutation::Set { value, .. } => value,
        Mutation::Delete { .. } => panic!("expected set"),
    }
}

fn wspec(prefix: &Key, secs: u64) -> LeaseSpec {
    LeaseSpec {
        prefix: prefix.clone(),
        mode: LeaseMode::Write,
        ttl: Duration::from_secs(secs),
    }
}

fn hstamp(ms: u64, lin: u8) -> HybridTimestamp {
    HybridTimestamp {
        wall: Timestamp(ms),
        mono: Timestamp(ms),
        lineage: Lineage([lin; 16]),
    }
}

fn spawn_server(
    clock: Arc<ManualClock>,
    space: SpaceId,
) -> impl Fn(&SpaceId) -> Option<SpaceHandle> + Sync {
    let server = Arc::new(Server::new(
        Arc::new(MemoryStore::new()),
        clock,
        ThreadSpawner,
    ));
    assert!(server.create_space(space));
    move |id: &SpaceId| server.space(id)
}

async fn fetch_cipher(
    handle: &impl ServerHandle,
    space: SpaceId,
    envelope: &SpaceEnvelope,
    logical: &Key,
) -> AdmittedEntry<Vec<u8>> {
    let cipher = envelope.open().unwrap();
    let encoded = cipher.encode_key(logical).unwrap();
    let stored = handle
        .get(
            &space,
            GetRequest {
                keys: vec![encoded.clone()],
            },
        )
        .await
        .unwrap()
        .entries
        .remove(0)
        .unwrap();
    cipher.open_admitted_entry(&stored).unwrap()
}

async fn queued(mem: &MemoryStore) -> usize {
    audit(&OrderedMetaStore::new(mem))
        .await
        .spaces
        .values()
        .map(|space| space.oplog.len())
        .sum()
}

#[test]
fn encrypted_empty_set_and_delete_roundtrip_through_pull() {
    block_on(async {
        let envelope = SpaceEnvelope::mint(NameKey([18; 32]), SpaceKey([19; 32]));
        let space = envelope.space_id();
        let mem = MemoryStore::new();
        let clock = ManualClock::new(Timestamp(0));
        let handle = spawn_server(Arc::new(ManualClock::new(Timestamp(0))), space);
        let client = Client::open(
            OrderedMetaStore::new(&mem),
            &handle,
            &clock,
            dev(1),
            TestNonceSource::new(1),
        )
        .await
        .unwrap();
        client.attach(&envelope).await.unwrap();
        let space_handle = client.space(space).await.unwrap();
        let db = key(&[b"db"]);
        let row = key(&[b"db", b"empty"]);
        space_handle.lease(vec![wspec(&db, 60)]).await.unwrap();

        space_handle
            .submit_checked(
                vec![Mutation::Set {
                    key: row.clone(),
                    value: Vec::new(),
                }],
                vec![],
            )
            .await
            .unwrap();
        space_handle.push().await.unwrap();
        let first = space_handle
            .fetch(Range::Prefix(db.clone()), AdmissionSeq(0))
            .await
            .unwrap();
        let RangeCut::Delta(entries) = &first.cut else {
            panic!("expected delta")
        };
        assert_eq!(entries.len(), 1);
        assert!(
            matches!(&entries[0].device_entry.mutation, Mutation::Set { value, .. } if value.is_empty())
        );

        space_handle
            .submit_checked(vec![Mutation::Delete { key: row.clone() }], vec![])
            .await
            .unwrap();
        space_handle.push().await.unwrap();
        let second = space_handle
            .fetch(Range::Prefix(db), first.at)
            .await
            .unwrap();
        let RangeCut::Delta(entries) = &second.cut else {
            panic!("expected delta")
        };
        assert_eq!(entries.len(), 1);
        assert_eq!(
            entries[0].key(),
            &envelope.open().unwrap().encode_key(&row).unwrap()
        );
        assert!(matches!(
            entries[0].device_entry.mutation,
            Mutation::Delete { .. }
        ));
    });
}

#[test]
fn encrypted_resume_keeps_wall_clock_authority() {
    block_on(async {
        let envelope = SpaceEnvelope::mint(NameKey([20; 32]), SpaceKey([21; 32]));
        let space = envelope.space_id();
        let mem = MemoryStore::new();
        let clock = ManualClock::new(Timestamp(1_000));
        let handle = spawn_server(Arc::new(ManualClock::new(Timestamp(0))), space);

        let db = key(&[b"db"]);
        let row = key(&[b"db", b"k"]);
        let lease_id = {
            let client = Client::open(
                OrderedMetaStore::new(&mem),
                &handle,
                &clock,
                dev(1),
                TestNonceSource::new(1),
            )
            .await
            .unwrap();
            client.attach(&envelope).await.unwrap();
            let space_handle = client.space(space).await.unwrap();
            let granted = space_handle.lease(vec![wspec(&db, 3_600)]).await.unwrap();
            space_handle
                .submit_checked(vec![set(row.clone(), b"secret")], vec![])
                .await
                .unwrap();
            granted[0].id
        };

        let state = audit(&OrderedMetaStore::new(&mem)).await;
        assert_eq!(
            state.spaces[&space].leases[&lease_id].deadline,
            hstamp(1_000 + 3_600_000, 1)
        );

        clock.advance(Duration::from_secs(300));
        clock.set_lineage(Lineage([2; 16]));
        let client = Client::open(
            OrderedMetaStore::new(&mem),
            &handle,
            &clock,
            dev(9),
            TestNonceSource::new(99),
        )
        .await
        .unwrap();
        client.attach(&envelope).await.unwrap();
        let space_handle = client.space(space).await.unwrap();
        assert_eq!(client.device(), dev(1));

        space_handle
            .submit_checked(vec![set(row.clone(), b"after-resume")], vec![])
            .await
            .unwrap();
        space_handle.push().await.unwrap();

        let entry = fetch_cipher(&handle, space, &envelope, &row).await;
        assert_eq!(value(&entry), b"after-resume");
    });
}

#[test]
fn encrypted_ack_drop_recovers_without_double_apply() {
    block_on(async {
        let envelope = SpaceEnvelope::mint(NameKey([22; 32]), SpaceKey([23; 32]));
        let space = envelope.space_id();
        let mem = MemoryStore::new();
        let clock = ManualClock::new(Timestamp(0));
        let handle = spawn_server(Arc::new(ManualClock::new(Timestamp(0))), space);

        let client = Client::open(
            OrderedMetaStore::new(&mem),
            &handle,
            &clock,
            dev(1),
            TestNonceSource::new(1),
        )
        .await
        .unwrap();
        client.attach(&envelope).await.unwrap();
        let space_handle = client.space(space).await.unwrap();

        let db = key(&[b"db"]);
        let (k1, k2) = (key(&[b"db", b"k1"]), key(&[b"db", b"k2"]));
        let granted = space_handle.lease(vec![wspec(&db, 60)]).await.unwrap();
        let lease = granted[0].id;
        space_handle
            .submit_checked(vec![set(k1.clone(), b"one")], vec![])
            .await
            .unwrap();
        space_handle
            .submit_checked(vec![set(k2.clone(), b"two")], vec![])
            .await
            .unwrap();

        let cipher = envelope.open().unwrap();
        let encoded_k1 = cipher.encode_key(&k1).unwrap();
        let state = audit(&OrderedMetaStore::new(&mem)).await;
        let oplog = &state.spaces[&space].oplog;
        let seq1 = *oplog.keys().next().unwrap();
        let seq2 = DeviceSeq(seq1.0 + 1);

        handle
            .admit(
                &space,
                AdmissionRequest {
                    device: client.device(),
                    expected_checksum: homebase_core::DeviceChecksum::EMPTY,
                    evidence: vec![lease],
                    batches: vec![
                        AdmissionBatch {
                            device_seq: seq1,
                            range_asserts: vec![],
                            entries: oplog[&seq1].entries().to_vec(),
                        },
                        AdmissionBatch {
                            device_seq: seq2,
                            range_asserts: vec![],
                            entries: oplog[&seq2].entries().to_vec(),
                        },
                    ],
                },
            )
            .await
            .expect("dead incarnation admitted ciphertext");

        assert_eq!(
            space_handle.push().await.unwrap(),
            PushOutcome::Drained {
                acked_through: Some(seq2)
            }
        );
        assert_eq!(queued(&mem).await, 0);

        assert_eq!(
            value(&fetch_cipher(&handle, space, &envelope, &k1).await),
            b"one"
        );
        assert_eq!(
            value(&fetch_cipher(&handle, space, &envelope, &k2).await),
            b"two"
        );
        assert!(
            handle
                .get(
                    &space,
                    GetRequest {
                        keys: vec![k1.clone()],
                    },
                )
                .await
                .unwrap()
                .entries
                .into_iter()
                .next()
                .flatten()
                .is_none()
        );
        assert!(
            handle
                .get(
                    &space,
                    GetRequest {
                        keys: vec![encoded_k1],
                    },
                )
                .await
                .unwrap()
                .entries
                .into_iter()
                .next()
                .flatten()
                .is_some()
        );
    });
}

#[test]
fn push_stall_recovers_via_lease() {
    block_on(async {
        let envelope = SpaceEnvelope::mint(NameKey([24; 32]), SpaceKey([25; 32]));
        let space = envelope.space_id();
        let mem = MemoryStore::new();
        let clock = ManualClock::new(Timestamp(0));
        let handle = spawn_server(Arc::new(ManualClock::new(Timestamp(0))), space);

        let client = Client::open(
            OrderedMetaStore::new(&mem),
            &handle,
            &clock,
            dev(1),
            SystemNonceSource,
        )
        .await
        .unwrap();
        client.attach(&envelope).await.unwrap();
        let space_handle = client.space(space).await.unwrap();

        let db = key(&[b"db"]);
        let row = key(&[b"db", b"k"]);
        space_handle.lease(vec![wspec(&db, 60)]).await.unwrap();
        space_handle
            .submit_checked(vec![set(row.clone(), b"v")], vec![])
            .await
            .unwrap();

        clock.skew_wall(Duration::from_secs(3_600));
        assert_eq!(
            space_handle.push().await.unwrap(),
            PushOutcome::Drained {
                acked_through: Some(DeviceSeq(1))
            }
        );
        assert_eq!(
            value(&fetch_cipher(&handle, space, &envelope, &row).await),
            b"v"
        );
    });
}
