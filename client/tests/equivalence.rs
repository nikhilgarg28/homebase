//! pull ⊕ replay(unshipped oplog) ≡ server plaintext after push.

use homebase::cipher::{SpaceEnvelope, SystemNonceSource};
use homebase::meta::{OrderedMetaStore, audit};
use homebase::server::ServerHandle;
use homebase::{Client, PushOutcome};
use homebase_core::clock::{ManualClock, Timestamp};
use homebase_core::key::Key;
use homebase_core::lease::LeaseMode;
use homebase_core::messages::{BatchOp, GetRequest, LeaseSpec, Range, RangeCut};
use homebase_core::space::SpaceId;
use homebase_core::storage::MemoryStore;
use homebase_core::tag::{AdmissionSeq, DeviceId, DeviceSeq, Entry, Epoch, Tag, Value};
use homebase_server::Server;
use homebase_server::actor::{SpaceHandle, Spawner};
use pollster::block_on;
use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

const SPACE: SpaceId = SpaceId([1; 16]);

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

async fn fetch(handle: &impl ServerHandle, k: &Key) -> Option<Entry> {
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
}

fn snapshot_from_pull(entries: &[homebase_core::tag::Entry]) -> BTreeMap<Key, Value> {
    entries
        .iter()
        .map(|e| (e.key.clone(), e.value.clone()))
        .collect()
}

fn replay_oplog_plaintext(
    mut view: BTreeMap<Key, Value>,
    state: &homebase::meta::ClientState,
    space: SpaceId,
    device: DeviceId,
) -> BTreeMap<Key, Value> {
    let Some(space_state) = state.spaces.get(&space) else {
        return view;
    };
    for (seq, record) in &space_state.oplog {
        for op in record.ops() {
            match op {
                BatchOp::Set {
                    key, ciphertext, ..
                } => {
                    view.insert(key.clone(), Value::Present(ciphertext.clone()));
                }
                BatchOp::Delete { key, .. } => {
                    view.insert(key.clone(), Value::Absent);
                }
                BatchOp::NoOp => {}
            }
            let _ = (seq, device);
        }
    }
    view
}

#[test]
fn pull_plus_unshipped_oplog_matches_server_after_push() {
    block_on(async {
        let mem = MemoryStore::new();
        let clock = ManualClock::new(Timestamp(0));
        let handle = spawn_server();
        let db = key(&[b"db"]);
        let k1 = key(&[b"db", b"k1"]);
        let k2 = key(&[b"db", b"k2"]);

        let client = Client::open(
            OrderedMetaStore::new(&mem),
            &handle,
            &clock,
            dev(1),
            SystemNonceSource,
        )
        .await
        .unwrap();
        client
            .attach(&SpaceEnvelope::plaintext(SPACE))
            .await
            .unwrap();
        let space = client.space(SPACE).await.unwrap();
        space.acquire(vec![wspec(&db)]).await.unwrap();

        space
            .submit_checked(vec![(k1.clone(), val(b"one"))], vec![])
            .await
            .unwrap();
        client.push().await.unwrap();

        space
            .submit_checked(vec![(k2.clone(), val(b"two"))], vec![])
            .await
            .unwrap();
        assert_eq!(
            audit(&OrderedMetaStore::new(&mem)).await.spaces[&SPACE]
                .oplog
                .len(),
            1
        );

        let pulled = space.pull(Range::Prefix(db.clone())).await.unwrap();
        let RangeCut::Snapshot(entries) = &pulled.ranges[0] else {
            panic!("expected snapshot")
        };
        let mut expected = snapshot_from_pull(entries);
        expected = replay_oplog_plaintext(
            expected,
            &audit(&OrderedMetaStore::new(&mem)).await,
            SPACE,
            client.device(),
        );

        assert_eq!(
            client.push().await.unwrap(),
            PushOutcome::Drained {
                acked_through: Some(DeviceSeq(2))
            }
        );

        for (k, v) in &expected {
            assert_eq!(fetch(&handle, k).await.unwrap().value, *v);
        }
    });
}

#[test]
fn encrypted_pull_plus_oplog_matches_server_after_push() {
    block_on(async {
        use homebase::cipher::{NameKey, NonceSource, SpaceEnvelope, SpaceKey, ValueNonce};

        #[derive(Clone)]
        struct TestNonceSource {
            next: u8,
        }
        impl NonceSource for TestNonceSource {
            fn next_nonce(&mut self) -> Result<ValueNonce, String> {
                let n = ValueNonce([self.next; 24]);
                self.next = self.next.wrapping_add(1);
                Ok(n)
            }
        }

        let envelope = SpaceEnvelope::mint(NameKey([11; 32]), SpaceKey([12; 32]));
        let space_id = envelope.space_id();
        let mem = MemoryStore::new();
        let clock = ManualClock::new(Timestamp(0));
        let server = Arc::new(Server::new(
            Arc::new(MemoryStore::new()),
            Arc::new(ManualClock::new(Timestamp(0))),
            ThreadSpawner,
        ));
        assert!(server.create_space(space_id));
        let handle = move |id: &SpaceId| server.space(id);

        let db = key(&[b"db"]);
        let k1 = key(&[b"db", b"k1"]);
        let k2 = key(&[b"db", b"k2"]);

        let client = Client::open(
            OrderedMetaStore::new(&mem),
            &handle,
            &clock,
            dev(1),
            TestNonceSource { next: 1 },
        )
        .await
        .unwrap();
        client.attach(&envelope).await.unwrap();
        let space = client.space(space_id).await.unwrap();
        space.acquire(vec![wspec(&db)]).await.unwrap();

        space
            .submit_checked(vec![(k1.clone(), val(b"one"))], vec![])
            .await
            .unwrap();
        client.push().await.unwrap();
        space
            .submit_checked(vec![(k2.clone(), val(b"two"))], vec![])
            .await
            .unwrap();

        let pulled = space.pull(Range::Prefix(db.clone())).await.unwrap();
        let RangeCut::Snapshot(entries) = &pulled.ranges[0] else {
            panic!("expected snapshot")
        };
        let mut expected: BTreeMap<Key, Value> = entries
            .iter()
            .map(|e| (e.key.clone(), e.value.clone()))
            .collect();

        let state = audit(&OrderedMetaStore::new(&mem)).await;
        let cipher = envelope.open().unwrap();
        let encoded_k2 = cipher.encode_key(&k2).unwrap();
        for (seq, record) in &state.spaces[&space_id].oplog {
            for op in record.ops() {
                if op.key() != Some(&encoded_k2) {
                    continue;
                }
                let entry = Entry {
                    key: encoded_k2.clone(),
                    value: match op {
                        BatchOp::Set { ciphertext, .. } => Value::Present(ciphertext.clone()),
                        BatchOp::Delete { .. } => Value::Absent,
                        BatchOp::NoOp => continue,
                    },
                    seal: op.seal().unwrap().clone(),
                    tag: Tag {
                        device: client.device(),
                        device_seq: *seq,
                        epoch: Epoch(0),
                        ver: op.ver().unwrap(),
                        admission_seq: AdmissionSeq(0),
                    },
                };
                let plain = cipher.decode_entry_value(&entry).unwrap();
                expected.insert(encoded_k2.clone(), plain);
            }
        }

        client.push().await.unwrap();

        for (encoded, plain) in &expected {
            let stored = handle
                .get(
                    &space_id,
                    GetRequest {
                        keys: vec![encoded.clone()],
                    },
                )
                .await
                .unwrap()
                .entries
                .remove(0)
                .unwrap();
            let decoded = cipher.decode_entry_value(&stored).unwrap();
            assert_eq!(decoded, *plain);
        }
    });
}
