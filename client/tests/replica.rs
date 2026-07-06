use homebase::cipher::{NameKey, NonceSource, SpaceEnvelope, SpaceKey, ValueNonce};
use homebase::meta::OrderedMetaStore;
use homebase::replica::Replica;
use homebase::server::ServerHandle;
use homebase_core::clock::{ManualClock, Timestamp};
use homebase_core::key::Key;
use homebase_core::lease::LeaseMode;
use homebase_core::messages::{GetRequest, LeaseSpec, Range, RangeCut};
use homebase_core::space::SpaceId;
use homebase_core::storage::MemoryStore;
use homebase_core::tag::{DeviceId, Entry, Value};
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

fn val(bytes: &[u8]) -> Value {
    Value::Present(bytes.to_vec())
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
        stealable: false,
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

#[test]
fn encrypted_replica_writes_ciphertext_under_encoded_keys() {
    block_on(async {
        let envelope = encrypted_envelope();
        let space = envelope.space_id();
        let handle = spawn_server(Arc::new(ManualClock::new(Timestamp(0))), &[space]);
        let clock = ManualClock::new(Timestamp(0));
        let mut replica = Replica::open_with_nonce_source(
            OrderedMetaStore::new(MemoryStore::new()),
            &handle,
            &clock,
            dev(1),
            &envelope,
            TestNonceSource::new(7),
        )
        .await
        .unwrap();

        let db = key(&[b"db"]);
        let row = key(&[b"db", b"k"]);
        replica.ensure(vec![wspec(&db, 60)], false).await.unwrap();
        replica
            .commit(vec![(row.clone(), val(b"secret"))])
            .await
            .unwrap();
        replica.push().await.unwrap();

        let cipher = envelope.open().unwrap();
        let encoded_row = cipher.encode_key(&row).unwrap();
        assert!(fetch(&handle, space, &row).await.is_none());
        let stored = fetch(&handle, space, &encoded_row).await.unwrap();
        assert_ne!(stored.value, val(b"secret"));
        assert_eq!(
            cipher.decode_value(&encoded_row, &stored.value).unwrap(),
            val(b"secret")
        );
    });
}

#[test]
fn envelope_can_be_reused_after_linking_without_changing_space() {
    block_on(async {
        // Local genesis: the device has an envelope before any link/account
        // directory exists.
        let local_envelope = encrypted_envelope();
        let space = local_envelope.space_id();
        let handle = spawn_server(Arc::new(ManualClock::new(Timestamp(0))), &[space]);
        let db = key(&[b"db"]);
        let row = key(&[b"db", b"k"]);

        let writer_clock = ManualClock::new(Timestamp(0));
        let mut writer = Replica::open_with_nonce_source(
            OrderedMetaStore::new(MemoryStore::new()),
            &handle,
            &writer_clock,
            dev(1),
            &local_envelope,
            TestNonceSource::new(1),
        )
        .await
        .unwrap();
        writer.ensure(vec![wspec(&db, 60)], false).await.unwrap();
        writer
            .commit(vec![(row, val(b"before-link"))])
            .await
            .unwrap();
        writer.push().await.unwrap();

        // Linking later publishes the same envelope through a directory.
        // Opening from that discovered envelope reaches the same space and
        // decrypts existing ciphertext.
        let linked_envelope = local_envelope.clone();
        assert_eq!(linked_envelope.space_id(), space);
        let reader_clock = ManualClock::new(Timestamp(0));
        let mut reader = Replica::open(
            OrderedMetaStore::new(MemoryStore::new()),
            &handle,
            &reader_clock,
            dev(2),
            &linked_envelope,
        )
        .await
        .unwrap();
        let pulled = reader.pull(Range::Prefix(db)).await.unwrap();

        assert!(matches!(
            &pulled.ranges[0],
            RangeCut::Snapshot(entries)
                if entries.len() == 1 && entries[0].value == val(b"before-link")
        ));
    });
}
