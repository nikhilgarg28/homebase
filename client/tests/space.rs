use homebase::Client;
use homebase::cipher::{
    NameKey, NonceSource, SpaceEnvelope, SpaceKey, SystemNonceSource, ValueNonce,
};
use homebase::meta::OrderedMetaStore;
use homebase::server::ServerHandle;
use homebase_core::clock::{ManualClock, Timestamp};
use homebase_core::key::Key;
use homebase_core::lease::LeaseMode;
use homebase_core::messages::{GetRequest, LeaseSpec, Range, RangeCut};
use homebase_core::space::SpaceId;
use homebase_core::storage::MemoryStore;
use homebase_core::tag::{AdmittedEntry, DeviceId, Mutation};
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

fn dev(n: u8) -> DeviceId {
    DeviceId([n; 16])
}

fn key(components: &[&[u8]]) -> Key {
    Key::from_bytes(components.iter().copied()).unwrap()
}

fn set(key: Key, bytes: &[u8]) -> Mutation {
    Mutation::Set {
        key,
        value: bytes.to_vec(),
    }
}

fn nonce(n: u8) -> ValueNonce {
    ValueNonce([n; 24])
}

#[derive(Clone, Debug)]
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
        let nonce = nonce(self.next);
        self.next = self.next.wrapping_add(1);
        Ok(nonce)
    }
}

fn wspec(prefix: &Key, secs: u64) -> LeaseSpec {
    LeaseSpec {
        prefix: prefix.clone(),
        mode: LeaseMode::Write,
        ttl: Duration::from_secs(secs),
    }
}

fn encrypted_envelope() -> SpaceEnvelope {
    SpaceEnvelope::encrypted(NameKey([1; 32]), SpaceKey([2; 32]))
}

fn spawn_server(
    clock: Arc<ManualClock>,
    spaces: &[SpaceId],
) -> impl Fn(&SpaceId) -> Option<SpaceHandle> + Sync + use<> {
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

#[test]
fn encrypted_space_writes_ciphertext_under_encoded_keys() {
    block_on(async {
        let envelope = encrypted_envelope();
        let space = envelope.space_id();
        let spaces = [space];
        let handle = spawn_server(Arc::new(ManualClock::new(Timestamp(0))), &spaces);
        let clock = ManualClock::new(Timestamp(0));
        let client = Client::open(
            OrderedMetaStore::new(MemoryStore::new()),
            &handle,
            &clock,
            dev(1),
            TestNonceSource::new(7),
        )
        .await
        .unwrap();
        client.attach(&envelope).await.unwrap();
        let space_handle = client.space(space).await.unwrap();

        let db = key(&[b"db"]);
        let row = key(&[b"db", b"k"]);
        space_handle.ensure(vec![wspec(&db, 60)]).await.unwrap();
        space_handle
            .submit_checked(vec![set(row.clone(), b"secret")], vec![])
            .await
            .unwrap();
        client.space(space).await.unwrap().push().await.unwrap();

        let cipher = envelope.open().unwrap();
        let encoded_row = cipher.encode_key(&row).unwrap();
        assert!(fetch(&handle, space, &row).await.is_none());
        let stored = fetch(&handle, space, &encoded_row).await.unwrap();
        assert!(
            matches!(&stored.device_entry.mutation, Mutation::Set { value, .. } if value.0 != b"secret")
        );
        assert_eq!(
            cipher
                .open_admitted_entry(&stored)
                .unwrap()
                .device_entry
                .mutation,
            set(encoded_row, b"secret")
        );
    });
}

#[test]
fn encrypted_push_preserves_per_commit_device_seq_aad() {
    block_on(async {
        let envelope = encrypted_envelope();
        let space = envelope.space_id();
        let spaces = [space];
        let handle = spawn_server(Arc::new(ManualClock::new(Timestamp(0))), &spaces);
        let db = key(&[b"db"]);
        let row1 = key(&[b"db", b"k1"]);
        let row2 = key(&[b"db", b"k2"]);

        let writer_clock = ManualClock::new(Timestamp(0));
        let writer = Client::open(
            OrderedMetaStore::new(MemoryStore::new()),
            &handle,
            &writer_clock,
            dev(1),
            TestNonceSource::new(1),
        )
        .await
        .unwrap();
        writer.attach(&envelope).await.unwrap();
        let writer_space = writer.space(space).await.unwrap();
        writer_space.ensure(vec![wspec(&db, 60)]).await.unwrap();
        writer_space
            .submit_checked(vec![set(row1.clone(), b"one")], vec![])
            .await
            .unwrap();
        writer_space
            .submit_checked(vec![set(row2.clone(), b"two")], vec![])
            .await
            .unwrap();
        writer.space(space).await.unwrap().push().await.unwrap();

        let reader_clock = ManualClock::new(Timestamp(0));
        let reader = Client::open(
            OrderedMetaStore::new(MemoryStore::new()),
            &handle,
            &reader_clock,
            dev(2),
            SystemNonceSource,
        )
        .await
        .unwrap();
        reader.attach(&envelope).await.unwrap();
        let reader_space = reader.space(space).await.unwrap();
        let pulled = reader_space.pull(Range::Prefix(db)).await.unwrap();

        let RangeCut::Snapshot(entries) = &pulled.ranges[0] else {
            panic!("initial pull should snapshot")
        };
        assert_eq!(entries.len(), 2);
        assert!(entries.iter().any(|entry| matches!(&entry.device_entry.mutation, Mutation::Set { value, .. } if value == b"one")));
        assert!(entries.iter().any(|entry| matches!(&entry.device_entry.mutation, Mutation::Set { value, .. } if value == b"two")));
        let mut device_seqs: Vec<_> = entries
            .iter()
            .map(|entry| entry.device_entry.tag.device_seq.0)
            .collect();
        device_seqs.sort_unstable();
        assert_eq!(device_seqs, vec![1, 2]);
    });
}

#[test]
fn envelope_can_be_reused_after_linking_without_changing_space() {
    block_on(async {
        let local_envelope = encrypted_envelope();
        let space = local_envelope.space_id();
        let spaces = [space];
        let handle = spawn_server(Arc::new(ManualClock::new(Timestamp(0))), &spaces);
        let db = key(&[b"db"]);
        let row = key(&[b"db", b"k"]);

        let writer_clock = ManualClock::new(Timestamp(0));
        let writer = Client::open(
            OrderedMetaStore::new(MemoryStore::new()),
            &handle,
            &writer_clock,
            dev(1),
            TestNonceSource::new(1),
        )
        .await
        .unwrap();
        writer.attach(&local_envelope).await.unwrap();
        let writer_space = writer.space(space).await.unwrap();
        writer_space.ensure(vec![wspec(&db, 60)]).await.unwrap();
        writer_space
            .submit_checked(vec![set(row, b"before-link")], vec![])
            .await
            .unwrap();
        writer.space(space).await.unwrap().push().await.unwrap();

        let linked_envelope = local_envelope.clone();
        assert_eq!(linked_envelope.space_id(), space);
        let reader_clock = ManualClock::new(Timestamp(0));
        let reader = Client::open(
            OrderedMetaStore::new(MemoryStore::new()),
            &handle,
            &reader_clock,
            dev(2),
            SystemNonceSource,
        )
        .await
        .unwrap();
        reader.attach(&linked_envelope).await.unwrap();
        let reader_space = reader.space(space).await.unwrap();
        let pulled = reader_space.pull(Range::Prefix(db)).await.unwrap();

        assert!(matches!(
            &pulled.ranges[0],
            RangeCut::Snapshot(entries)
                if entries.len() == 1 && matches!(&entries[0].device_entry.mutation, Mutation::Set { value, .. } if value == b"before-link")
        ));
    });
}
