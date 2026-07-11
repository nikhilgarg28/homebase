//! Client-layer integration: multi-space coordination, offline physics,
//! per-space sequencing, and codec-cache resume.

use homebase::cipher::{NameKey, SpaceEnvelope, SpaceKey, SystemNonceSource, ValueContext};
use homebase::meta::{OrderedMetaStore, audit};
use homebase::server::ServerHandle;
use homebase::{Client, open_offline};
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

fn wspec(prefix: &Key, secs: u64) -> LeaseSpec {
    LeaseSpec {
        prefix: prefix.clone(),
        mode: LeaseMode::Write,
        ttl: Duration::from_secs(secs),
    }
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
fn device_minted_once_across_incarnations() {
    block_on(async {
        let mem = MemoryStore::new();
        let clock = ManualClock::new(Timestamp(0));
        let handle = |_: &SpaceId| Option::<SpaceHandle>::None;

        let client = Client::open(
            OrderedMetaStore::new(&mem),
            &handle,
            &clock,
            dev(1),
            SystemNonceSource,
        )
        .await
        .unwrap();
        assert_eq!(client.device(), dev(1));

        let client = Client::open(
            OrderedMetaStore::new(&mem),
            &handle,
            &clock,
            dev(2),
            SystemNonceSource,
        )
        .await
        .unwrap();
        assert_eq!(client.device(), dev(1));
    });
}

#[test]
fn multi_space_uses_independent_seq_streams_with_distinct_ciphertext() {
    block_on(async {
        let envelope_a = SpaceEnvelope::mint(NameKey([1; 32]), SpaceKey([2; 32]));
        let envelope_b = SpaceEnvelope::mint(NameKey([3; 32]), SpaceKey([4; 32]));
        let id_a = envelope_a.space_id();
        let id_b = envelope_b.space_id();
        let mem = MemoryStore::new();
        let clock = ManualClock::new(Timestamp(0));
        let spaces = [id_a, id_b];
        let handle = spawn_server(Arc::new(ManualClock::new(Timestamp(0))), &spaces);

        let client = Client::open(
            OrderedMetaStore::new(&mem),
            &handle,
            &clock,
            dev(1),
            SystemNonceSource,
        )
        .await
        .unwrap();
        client.attach(&envelope_a).await.unwrap();
        client.attach(&envelope_b).await.unwrap();

        let db_a = key(&[b"a", b"db"]);
        let row_a = key(&[b"a", b"db", b"k"]);
        let db_b = key(&[b"b", b"db"]);
        let row_b = key(&[b"b", b"db", b"k"]);

        let space_a = client.space(id_a).await.unwrap();
        let space_b = client.space(id_b).await.unwrap();
        space_a.ensure(vec![wspec(&db_a, 60)]).await.unwrap();
        space_b.ensure(vec![wspec(&db_b, 60)]).await.unwrap();

        space_a
            .commit(vec![(row_a.clone(), val(b"alpha"))])
            .await
            .unwrap();
        space_b
            .commit(vec![(row_b.clone(), val(b"beta"))])
            .await
            .unwrap();
        space_a
            .commit(vec![(row_a.clone(), val(b"alpha2"))])
            .await
            .unwrap();

        let state = audit(&OrderedMetaStore::new(&mem)).await;
        assert_eq!(state.spaces[&id_a].oplog.len(), 2);
        assert_eq!(state.spaces[&id_b].oplog.len(), 1);
        assert_eq!(
            state.spaces[&id_a]
                .oplog
                .keys()
                .copied()
                .collect::<Vec<_>>(),
            vec![DeviceSeq(1), DeviceSeq(2)]
        );
        assert_eq!(
            state.spaces[&id_b]
                .oplog
                .keys()
                .copied()
                .collect::<Vec<_>>(),
            vec![DeviceSeq(1)]
        );

        client.push().await.unwrap();

        let cipher_a = envelope_a.open().unwrap();
        let cipher_b = envelope_b.open().unwrap();
        let encoded_a = cipher_a.encode_key(&row_a).unwrap();
        let encoded_b = cipher_b.encode_key(&row_b).unwrap();
        assert_ne!(encoded_a, encoded_b);

        let stored_a = fetch(&handle, id_a, &encoded_a).await.unwrap();
        let stored_b = fetch(&handle, id_b, &encoded_b).await.unwrap();
        assert_ne!(stored_a.value, val(b"beta"));
        assert_ne!(stored_b.value, val(b"alpha2"));
        assert_eq!(
            cipher_a
                .decode_value(
                    &encoded_a,
                    &stored_a.value,
                    ValueContext::from_tag(&stored_a.tag)
                )
                .unwrap(),
            val(b"alpha2")
        );
        assert_eq!(
            cipher_b
                .decode_value(
                    &encoded_b,
                    &stored_b.value,
                    ValueContext::from_tag(&stored_b.tag)
                )
                .unwrap(),
            val(b"beta")
        );
    });
}

#[test]
fn global_push_compatibility_drains_independent_space_streams() {
    block_on(async {
        let id_a = SpaceId([10; 16]);
        let id_b = SpaceId([11; 16]);
        let mem = MemoryStore::new();
        let clock = ManualClock::new(Timestamp(0));
        let spaces = [id_a, id_b];
        let handle = spawn_server(Arc::new(ManualClock::new(Timestamp(0))), &spaces);

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
            .attach(&SpaceEnvelope::plaintext(id_a))
            .await
            .unwrap();
        client
            .attach(&SpaceEnvelope::plaintext(id_b))
            .await
            .unwrap();

        let db_a = key(&[b"a", b"db"]);
        let db_b = key(&[b"b", b"db"]);
        let row_a1 = key(&[b"a", b"db", b"k1"]);
        let row_b1 = key(&[b"b", b"db", b"k1"]);
        let row_a2 = key(&[b"a", b"db", b"k2"]);

        let space_a = client.space(id_a).await.unwrap();
        let space_b = client.space(id_b).await.unwrap();
        space_a.ensure(vec![wspec(&db_a, 60)]).await.unwrap();
        space_b.ensure(vec![wspec(&db_b, 60)]).await.unwrap();

        space_a
            .commit(vec![(row_a1.clone(), val(b"a1"))])
            .await
            .unwrap();
        space_b
            .commit(vec![(row_b1.clone(), val(b"b1"))])
            .await
            .unwrap();
        space_a
            .commit(vec![(row_a2.clone(), val(b"a2"))])
            .await
            .unwrap();

        client.push().await.unwrap();

        let mut seqs = Vec::new();
        for (id, row) in [(id_a, row_a1), (id_b, row_b1), (id_a, row_a2)] {
            let entry = fetch(&handle, id, &row).await.unwrap();
            seqs.push(entry.tag.device_seq);
        }
        assert_eq!(seqs, vec![DeviceSeq(1), DeviceSeq(1), DeviceSeq(2)]);
    });
}

#[test]
fn offline_commit_survives_until_online_push() {
    block_on(async {
        let envelope = SpaceEnvelope::mint(NameKey([5; 32]), SpaceKey([6; 32]));
        let space = envelope.space_id();
        let mem = MemoryStore::new();
        let clock = ManualClock::new(Timestamp(0));
        let spaces = [space];
        let handle = spawn_server(Arc::new(ManualClock::new(Timestamp(0))), &spaces);
        let db = key(&[b"db"]);
        let row = key(&[b"db", b"k"]);

        {
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
            let online_space = client.space(space).await.unwrap();
            online_space.ensure(vec![wspec(&db, 60)]).await.unwrap();
        }

        let offline = open_offline(
            OrderedMetaStore::new(&mem),
            &clock,
            dev(1),
            SystemNonceSource,
        )
        .await
        .unwrap();
        {
            let offline_space = offline.space(space).await.unwrap();
            offline_space
                .commit(vec![(row.clone(), val(b"offline"))])
                .await
                .unwrap();
        }
        assert_eq!(
            audit(&OrderedMetaStore::new(&mem)).await.spaces[&space]
                .oplog
                .len(),
            1
        );

        let online = Client::open(
            OrderedMetaStore::new(&mem),
            &handle,
            &clock,
            dev(1),
            SystemNonceSource,
        )
        .await
        .unwrap();
        online.push().await.unwrap();

        let cipher = envelope.open().unwrap();
        let encoded = cipher.encode_key(&row).unwrap();
        let stored = fetch(&handle, space, &encoded).await.unwrap();
        assert_eq!(
            cipher
                .decode_value(&encoded, &stored.value, ValueContext::from_tag(&stored.tag))
                .unwrap(),
            val(b"offline")
        );
    });
}

#[test]
fn resume_from_codec_cache_decrypts_without_envelope() {
    block_on(async {
        let envelope = SpaceEnvelope::mint(NameKey([7; 32]), SpaceKey([8; 32]));
        let space = envelope.space_id();
        let mem = MemoryStore::new();
        let clock = ManualClock::new(Timestamp(0));
        let spaces = [space];
        let handle = spawn_server(Arc::new(ManualClock::new(Timestamp(0))), &spaces);

        {
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
            let db = key(&[b"db"]);
            let row = key(&[b"db", b"k"]);
            let space = client.space(space).await.unwrap();
            space.ensure(vec![wspec(&db, 60)]).await.unwrap();
            space
                .commit(vec![(row.clone(), val(b"cached"))])
                .await
                .unwrap();
            client.push().await.unwrap();
        }

        let client = Client::open(
            OrderedMetaStore::new(&mem),
            &handle,
            &clock,
            dev(1),
            SystemNonceSource,
        )
        .await
        .unwrap();
        let db = key(&[b"db"]);
        let space = client.space(space).await.unwrap();
        let pulled = space.pull(Range::Prefix(db)).await.unwrap();
        assert!(matches!(
            &pulled.ranges[0],
            RangeCut::Snapshot(entries) if entries.len() == 1 && entries[0].value == val(b"cached")
        ));
    });
}

#[test]
fn encrypted_init_roundtrips_through_push_and_pull() {
    block_on(async {
        let envelope = SpaceEnvelope::mint(NameKey([9; 32]), SpaceKey([10; 32]));
        let space = envelope.space_id();
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
            SystemNonceSource,
        )
        .await
        .unwrap();
        writer.attach(&envelope).await.unwrap();
        {
            let writer_space = writer.space(space).await.unwrap();
            writer_space.ensure(vec![wspec(&db, 60)]).await.unwrap();
            writer_space
                .commit(vec![(row.clone(), val(b"roundtrip"))])
                .await
                .unwrap();
        }
        writer.push().await.unwrap();

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
        assert!(matches!(
            &pulled.ranges[0],
            RangeCut::Snapshot(entries)
                if entries.len() == 1 && entries[0].value == val(b"roundtrip")
        ));
    });
}
