//! pull ⊕ replay(unshipped oplog) ≡ server plaintext after push.

use homebase::cipher::{SpaceEnvelope, SystemNonceSource};
use homebase::meta::{OrderedMetaStore, audit};
use homebase::server::ServerHandle;
use homebase::{Client, PushOutcome};
use homebase_core::clock::{ManualClock, Timestamp};
use homebase_core::key::Key;
use homebase_core::lease::LeaseMode;
use homebase_core::messages::{GetRequest, LeaseSpec, Range, RangeCut};
use homebase_core::space::SpaceId;
use homebase_core::storage::MemoryStore;
use homebase_core::tag::{
    AdmissionSeq, AdmissionTag, AdmittedEntry, DeviceId, DeviceSeq, Mutation,
};
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

fn set(key: Key, bytes: &[u8]) -> Mutation {
    Mutation::Set {
        key,
        value: bytes.to_vec(),
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

async fn fetch(handle: &impl ServerHandle, k: &Key) -> Option<AdmittedEntry> {
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

fn snapshot_from_pull(entries: &[AdmittedEntry<Vec<u8>>]) -> BTreeMap<Key, Option<Vec<u8>>> {
    entries
        .iter()
        .map(|e| match &e.device_entry.mutation {
            Mutation::Set { key, value } => (key.clone(), Some(value.clone())),
            Mutation::Delete { key } => (key.clone(), None),
            Mutation::DeleteRange { .. } => unreachable!("DR1 server rejects range deletes"),
        })
        .collect()
}

fn replay_oplog_plaintext(
    mut view: BTreeMap<Key, Option<Vec<u8>>>,
    state: &homebase::meta::ClientState,
    space: SpaceId,
    device: DeviceId,
) -> BTreeMap<Key, Option<Vec<u8>>> {
    let Some(space_state) = state.spaces.get(&space) else {
        return view;
    };
    for (seq, record) in &space_state.oplog {
        for entry in record.entries() {
            match &entry.mutation {
                Mutation::Set { key, value } => {
                    view.insert(key.clone(), Some(value.0.clone()));
                }
                Mutation::Delete { key } => {
                    view.insert(key.clone(), None);
                }
                Mutation::DeleteRange { .. } => unreachable!("test does not submit ranges"),
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
        space.lease(vec![wspec(&db)]).await.unwrap();

        space
            .submit_checked(vec![set(k1.clone(), b"one")], vec![])
            .await
            .unwrap();
        space.push().await.unwrap();

        space
            .submit_checked(vec![set(k2.clone(), b"two")], vec![])
            .await
            .unwrap();
        assert_eq!(
            audit(&OrderedMetaStore::new(&mem)).await.spaces[&SPACE]
                .oplog
                .len(),
            1
        );

        let pulled = space
            .fetch(Range::Prefix(db.clone()), AdmissionSeq(0))
            .await
            .unwrap();
        let RangeCut::Delta(entries) = &pulled.cut else {
            panic!("expected delta")
        };
        let mut expected = snapshot_from_pull(entries);
        expected = replay_oplog_plaintext(
            expected,
            &audit(&OrderedMetaStore::new(&mem)).await,
            SPACE,
            client.device(),
        );

        assert_eq!(
            client.space(SPACE).await.unwrap().push().await.unwrap(),
            PushOutcome::Drained {
                acked_through: Some(DeviceSeq(2))
            }
        );

        for (k, v) in &expected {
            let fetched = fetch(&handle, k).await;
            match (v, fetched) {
                (None, None) => {}
                (Some(expected), Some(entry)) => match entry.device_entry.mutation {
                    Mutation::Set { value, .. } => assert_eq!(value.0, *expected),
                    Mutation::Delete { .. } => panic!("get returned tombstone"),
                    Mutation::DeleteRange { .. } => panic!("get returned range delete"),
                },
                other => panic!("server state mismatch: {other:?}"),
            }
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
        space.lease(vec![wspec(&db)]).await.unwrap();

        space
            .submit_checked(vec![set(k1.clone(), b"one")], vec![])
            .await
            .unwrap();
        space.push().await.unwrap();
        space
            .submit_checked(vec![set(k2.clone(), b"two")], vec![])
            .await
            .unwrap();

        let pulled = space
            .fetch(Range::Prefix(db.clone()), AdmissionSeq(0))
            .await
            .unwrap();
        let RangeCut::Delta(entries) = &pulled.cut else {
            panic!("expected delta")
        };
        let mut expected = snapshot_from_pull(entries);

        let state = audit(&OrderedMetaStore::new(&mem)).await;
        let cipher = envelope.open().unwrap();
        let encoded_k2 = cipher.encode_key(&k2).unwrap();
        for (seq, record) in &state.spaces[&space_id].oplog {
            for device_entry in record.entries() {
                if device_entry.key() != &encoded_k2 {
                    continue;
                }
                let entry = AdmittedEntry {
                    device_entry: device_entry.clone(),
                    admission: AdmissionTag {
                        admission_seq: AdmissionSeq(0),
                        op_index: 0,
                    },
                };
                let plain = cipher.open_admitted_entry(&entry).unwrap();
                let value = match plain.device_entry.mutation {
                    Mutation::Set { value, .. } => Some(value),
                    Mutation::Delete { .. } => None,
                    Mutation::DeleteRange { .. } => {
                        unreachable!("DR1 server rejects range deletes")
                    }
                };
                expected.insert(encoded_k2.clone(), value);
                let _ = seq;
            }
        }

        space.push().await.unwrap();

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
            let decoded = cipher.open_admitted_entry(&stored).unwrap();
            match (&decoded.device_entry.mutation, plain) {
                (Mutation::Set { value, .. }, Some(expected)) => assert_eq!(value, expected),
                (Mutation::Delete { .. }, None) => {}
                other => panic!("decoded state mismatch: {other:?}"),
            }
        }
    });
}
