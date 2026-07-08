//! pull ⊕ replay(unshipped oplog) ≡ server plaintext after push.

use homebase::cipher::{SpaceEnvelope, SystemNonceSource, ValueContext};
use homebase::meta::{MetaStore, OrderedMetaStore, audit};
use homebase::server::ServerHandle;
use homebase::{Client, PushOutcome};
use homebase_core::clock::{ManualClock, Timestamp};
use homebase_core::key::Key;
use homebase_core::lease::LeaseMode;
use homebase_core::messages::{GetRequest, LeaseSpec, Range, RangeCut};
use homebase_core::space::SpaceId;
use homebase_core::storage::MemoryStore;
use homebase_core::tag::{DeviceId, DeviceSeq, Entry, Value};
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
    for (seq, record) in &state.oplog {
        if record.space().unwrap() != space {
            continue;
        }
        for entry in record.entries() {
            view.insert(entry.key.clone(), entry.value.clone());
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
        let mut space = client.space(SPACE).await.unwrap();
        space.acquire(vec![wspec(&db)]).await.unwrap();

        space.commit(vec![(k1.clone(), val(b"one"))]).await.unwrap();
        client.push().await.unwrap();

        space.commit(vec![(k2.clone(), val(b"two"))]).await.unwrap();
        assert_eq!(audit(&OrderedMetaStore::new(&mem)).await.oplog.len(), 1);

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
        let mut space = client.space(space_id).await.unwrap();
        space.acquire(vec![wspec(&db)]).await.unwrap();

        space.commit(vec![(k1.clone(), val(b"one"))]).await.unwrap();
        client.push().await.unwrap();
        space.commit(vec![(k2.clone(), val(b"two"))]).await.unwrap();

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
        for (seq, record) in &state.oplog {
            for entry in record.entries() {
                if entry.key != encoded_k2 {
                    continue;
                }
                let plain = cipher
                    .decode_value(
                        &entry.key,
                        &entry.value,
                        ValueContext {
                            device: client.device(),
                            device_seq: *seq,
                            ver: entry.ver,
                        },
                    )
                    .unwrap();
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
            let decoded = cipher
                .decode_value(encoded, &stored.value, ValueContext::from_tag(&stored.tag))
                .unwrap();
            assert_eq!(decoded, *plain);
        }
    });
}
