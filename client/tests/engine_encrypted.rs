//! Encrypted-space crash-resume and ack-drop tortures.

use homebase::cipher::{NameKey, NonceSource, SpaceEnvelope, SpaceKey, SystemNonceSource, ValueContext, ValueNonce};
use homebase::meta::{MetaStore, OrderedMetaStore, audit};
use homebase::server::ServerHandle;
use homebase::{Client, PushOutcome};
use homebase_core::clock::{HybridTimestamp, Lineage, ManualClock, Timestamp};
use homebase_core::key::Key;
use homebase_core::lease::{LeaseMode, LeaseRef};
use homebase_core::messages::{GetRequest, KernelError, LeaseSpec, PutBatch, PutBatchRequest};
use homebase_core::space::SpaceId;
use homebase_core::storage::MemoryStore;
use homebase_core::tag::{DeviceId, DeviceSeq, Entry, Value, Ver};
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

fn val(bytes: &[u8]) -> Value {
    Value::Present(bytes.to_vec())
}

fn wspec(prefix: &Key, secs: u64) -> LeaseSpec {
    LeaseSpec {
        prefix: prefix.clone(),
        mode: LeaseMode::Write,
        ttl: Duration::from_secs(secs),
        stealable: false,
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
) -> Entry {
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
    Entry {
        key: logical.clone(),
        value: cipher
            .decode_value(
                &encoded,
                &stored.value,
                ValueContext::from_tag(&stored.tag),
            )
            .unwrap(),
        tag: stored.tag,
    }
}

async fn queued(mem: &MemoryStore) -> usize {
    audit(&OrderedMetaStore::new(mem)).await.oplog.len()
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
            let mut space_handle = client.space(space).await.unwrap();
            let granted = space_handle
                .acquire(vec![wspec(&db, 3_600)], false)
                .await
                .unwrap();
            space_handle
                .commit(vec![(row.clone(), val(b"secret"))])
                .await
                .unwrap();
            granted.leases[0].id
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
        let mut space_handle = client.space(space).await.unwrap();
        assert_eq!(client.device(), dev(1));

        space_handle
            .commit(vec![(row.clone(), val(b"after-resume"))])
            .await
            .unwrap();
        client.push().await.unwrap();

        let entry = fetch_cipher(&handle, space, &envelope, &row).await;
        assert_eq!(entry.value, val(b"after-resume"));
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
        let mut space_handle = client.space(space).await.unwrap();

        let db = key(&[b"db"]);
        let (k1, k2) = (key(&[b"db", b"k1"]), key(&[b"db", b"k2"]));
        let granted = space_handle
            .acquire(vec![wspec(&db, 60)], false)
            .await
            .unwrap();
        let lease = LeaseRef {
            id: granted.leases[0].id,
            epoch: granted.leases[0].epoch,
        };
        space_handle
            .commit(vec![(k1.clone(), val(b"one"))])
            .await
            .unwrap();
        space_handle
            .commit(vec![(k2.clone(), val(b"two"))])
            .await
            .unwrap();

        let cipher = envelope.open().unwrap();
        let encoded_k1 = cipher.encode_key(&k1).unwrap();
        let state = audit(&OrderedMetaStore::new(&mem)).await;
        let seq1 = *state.oplog.keys().next().unwrap();
        let seq2 = DeviceSeq(seq1.0 + 1);

        handle
            .put_batch(
                &space,
                PutBatchRequest {
                    device: client.device(),
                    leases: vec![lease],
                    batches: vec![
                        PutBatch {
                            device_seq: seq1,
                            entries: state.oplog[&seq1].entries.clone(),
                        },
                        PutBatch {
                            device_seq: seq2,
                            entries: state.oplog[&seq2].entries.clone(),
                        },
                    ],
                },
            )
            .await
            .expect("dead incarnation admitted ciphertext");

        assert_eq!(
            client.push().await.unwrap(),
            PushOutcome::Drained {
                acked_through: Some(seq2)
            }
        );
        assert_eq!(queued(&mem).await, 0);

        assert_eq!(
            fetch_cipher(&handle, space, &envelope, &k1).await.value,
            val(b"one")
        );
        assert_eq!(
            fetch_cipher(&handle, space, &envelope, &k2).await.value,
            val(b"two")
        );
        assert!(handle
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
            .is_none());
        assert!(handle
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
            .is_some());
    });
}

#[test]
fn push_stall_recovers_via_ensure() {
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
        let mut space_handle = client.space(space).await.unwrap();

        let db = key(&[b"db"]);
        let row = key(&[b"db", b"k"]);
        space_handle
            .acquire(vec![wspec(&db, 60)], false)
            .await
            .unwrap();
        space_handle
            .commit(vec![(row.clone(), val(b"v"))])
            .await
            .unwrap();

        clock.skew_wall(Duration::from_secs(3_600));
        let outcome = client.push().await.unwrap();
        assert!(
            matches!(
                outcome,
                PushOutcome::Stalled {
                    error: KernelError::NotCovered { .. },
                    ..
                }
            ),
            "expired lease must stall, got {outcome:?}"
        );

        space_handle.ensure(vec![wspec(&db, 60)], false).await.unwrap();
        assert_eq!(
            client.push().await.unwrap(),
            PushOutcome::Drained {
                acked_through: Some(DeviceSeq(1))
            }
        );
        assert_eq!(
            fetch_cipher(&handle, space, &envelope, &row).await.value,
            val(b"v")
        );
    });
}
