use homebase::cipher::{
    NameKey, NonceSource, SpaceEnvelope, SpaceKey, SystemNonceSource, ValueNonce,
};
use homebase::meta::{OrderedMetaStore, audit};
use homebase::server::ServerHandle;
use homebase::{Client, PushReceipt};
use homebase_core::clock::{ManualClock, Timestamp};
use homebase_core::key::Key;
use homebase_core::lease::LeaseMode;
use homebase_core::messages::{
    AcquireRequest, AcquireResponse, AdmissionRequest, AdmissionResponse, GetRequest, GetResponse,
    LeaseSpec, ListLeasesRequest, ListLeasesResponse, ListRequest, ListResponse, PullRequest,
    PullResponse, Range, RangeCut, ReadAtRequest, ReadAtResponse, ReleaseRequest, ReleaseResponse,
    RenewRequest, RenewResponse,
};
use homebase_core::space::{SpaceError, SpaceId};
use homebase_core::storage::MemoryStore;
use homebase_core::tag::{AdmissionSeq, AdmittedEntry, DeviceId, Mutation, Ver};
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

struct TamperPull<H> {
    inner: H,
}

impl<H: ServerHandle + Sync> ServerHandle for TamperPull<H> {
    async fn acquire(
        &self,
        space: &SpaceId,
        req: AcquireRequest,
    ) -> Result<AcquireResponse, SpaceError> {
        self.inner.acquire(space, req).await
    }

    async fn renew(&self, space: &SpaceId, req: RenewRequest) -> Result<RenewResponse, SpaceError> {
        self.inner.renew(space, req).await
    }

    async fn release(
        &self,
        space: &SpaceId,
        req: ReleaseRequest,
    ) -> Result<ReleaseResponse, SpaceError> {
        self.inner.release(space, req).await
    }

    async fn list_leases(
        &self,
        space: &SpaceId,
        req: ListLeasesRequest,
    ) -> Result<ListLeasesResponse, SpaceError> {
        self.inner.list_leases(space, req).await
    }

    async fn admit(
        &self,
        space: &SpaceId,
        req: AdmissionRequest,
    ) -> Result<AdmissionResponse, SpaceError> {
        self.inner.admit(space, req).await
    }

    async fn pull(&self, space: &SpaceId, req: PullRequest) -> Result<PullResponse, SpaceError> {
        let mut response = self.inner.pull(space, req).await?;
        let range_entry = response
            .batches
            .iter_mut()
            .flat_map(|batch| &mut batch.entries)
            .find(|entry| entry.device_entry.mutation.is_delete_range());
        if let Some(entry) = range_entry {
            entry.device_entry.seal.aead[0] ^= 1;
        }
        Ok(response)
    }

    async fn get(&self, space: &SpaceId, req: GetRequest) -> Result<GetResponse, SpaceError> {
        self.inner.get(space, req).await
    }

    async fn list(&self, space: &SpaceId, req: ListRequest) -> Result<ListResponse, SpaceError> {
        self.inner.list(space, req).await
    }

    async fn read_at(
        &self,
        space: &SpaceId,
        req: ReadAtRequest,
    ) -> Result<ReadAtResponse, SpaceError> {
        self.inner.read_at(space, req).await
    }
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
        space_handle.lease(vec![wspec(&db, 60)]).await.unwrap();
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
        writer_space.lease(vec![wspec(&db, 60)]).await.unwrap();
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
        let pulled = reader_space
            .fetch(Range::Prefix(db), AdmissionSeq(0))
            .await
            .unwrap();

        let RangeCut::Delta(entries) = &pulled.cut else {
            panic!("fetch should return a delta")
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
        writer_space.lease(vec![wspec(&db, 60)]).await.unwrap();
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
        let pulled = reader_space
            .fetch(Range::Prefix(db), AdmissionSeq(0))
            .await
            .unwrap();

        assert!(matches!(
            &pulled.cut,
            RangeCut::Delta(entries)
                if entries.len() == 1 && matches!(&entries[0].device_entry.mutation, Mutation::Set { value, .. } if value == b"before-link")
        ));
    });
}

#[test]
fn dense_pull_pages_into_admit_log_and_application_moves_only_neck() {
    block_on(async {
        let space = SpaceId([42; 16]);
        let envelope = SpaceEnvelope::plaintext(space);
        let handle = spawn_server(Arc::new(ManualClock::new(Timestamp(0))), &[space]);

        let writer_store = MemoryStore::new();
        let writer_clock = ManualClock::new(Timestamp(0));
        let writer = Client::open(
            OrderedMetaStore::new(&writer_store),
            &handle,
            &writer_clock,
            dev(1),
            TestNonceSource::new(0),
        )
        .await
        .unwrap();
        writer.attach(&envelope).await.unwrap();
        let writer_space = writer.space(space).await.unwrap();
        for n in 0..257_u16 {
            writer_space
                .submit_unchecked(
                    [set(key(&[b"db", &n.to_be_bytes()]), &n.to_be_bytes())],
                    vec![],
                )
                .await
                .unwrap();
        }
        writer_space.push().await.unwrap();

        let reader_store = MemoryStore::new();
        let reader_clock = ManualClock::new(Timestamp(0));
        let reader = Client::open(
            OrderedMetaStore::new(&reader_store),
            &handle,
            &reader_clock,
            dev(2),
            SystemNonceSource,
        )
        .await
        .unwrap();
        reader.attach(&envelope).await.unwrap();
        let reader_space = reader.space(space).await.unwrap();

        assert_eq!(reader_space.pull().await.unwrap(), AdmissionSeq(257));
        let state = audit(&OrderedMetaStore::new(&reader_store)).await;
        let state = &state.spaces[&space];
        assert_eq!(state.admit_cursors.head, AdmissionSeq(1));
        assert_eq!(state.admit_cursors.neck, AdmissionSeq(1));
        assert_eq!(state.admit_cursors.tail, AdmissionSeq(258));
        assert_eq!(state.ver_high, Some(Ver(257)));
        assert_eq!(state.admits.len(), 257);

        let pending = reader_space.admits().iter_from_neck().await.unwrap();
        assert_eq!(pending.len(), 257);
        for (offset, batch) in pending.iter().enumerate() {
            let expected = AdmissionSeq(offset as u64 + 1);
            assert_eq!(batch.admission_seq, expected);
            assert!(batch.entries.iter().all(|entry| {
                entry.admission.admission_seq == expected
                    && entry.device_entry.tag.device == batch.device
                    && entry.device_entry.tag.device_seq == batch.device_seq
            }));
        }

        reader_space
            .admits()
            .mark_applied(AdmissionSeq(3))
            .await
            .unwrap();
        reader_space.admits().trim(AdmissionSeq(2)).await.unwrap();
        let cursors = reader_space.admits().cursors().await.unwrap();
        assert_eq!(cursors.head, AdmissionSeq(2));
        assert_eq!(cursors.neck, AdmissionSeq(3));
        assert_eq!(cursors.tail, AdmissionSeq(258));

        assert_eq!(reader_space.pull().await.unwrap(), AdmissionSeq(257));
        assert_eq!(reader_space.admits().cursors().await.unwrap(), cursors);
    });
}

#[test]
fn stateless_fetch_leaves_all_client_state_unchanged() {
    block_on(async {
        let space = SpaceId([43; 16]);
        let envelope = SpaceEnvelope::plaintext(space);
        let handle = spawn_server(Arc::new(ManualClock::new(Timestamp(0))), &[space]);
        let store = MemoryStore::new();
        let clock = ManualClock::new(Timestamp(0));
        let client = Client::open(
            OrderedMetaStore::new(&store),
            &handle,
            &clock,
            dev(1),
            SystemNonceSource,
        )
        .await
        .unwrap();
        client.attach(&envelope).await.unwrap();
        let space_handle = client.space(space).await.unwrap();
        space_handle
            .submit_unchecked([set(key(&[b"db", b"row"]), b"value")], vec![])
            .await
            .unwrap();
        space_handle.push().await.unwrap();

        let before = audit(&OrderedMetaStore::new(&store)).await;
        let fetched = space_handle
            .fetch(Range::Prefix(key(&[b"db"])), AdmissionSeq(0))
            .await
            .unwrap();
        assert!(matches!(fetched.cut, RangeCut::Delta(ref entries) if entries.len() == 1));
        let after = audit(&OrderedMetaStore::new(&store)).await;
        assert_eq!(after, before);
    });
}

#[test]
fn delete_range_roundtrips_submit_fetch_pull_and_reopen() {
    block_on(async {
        let envelope = encrypted_envelope();
        let space = envelope.space_id();
        let handle = spawn_server(Arc::new(ManualClock::new(Timestamp(0))), &[space]);
        let db = key(&[b"db"]);
        let child = key(&[b"db", b"child"]);
        let row = key(&[b"db", b"child", b"row"]);

        let writer_store = MemoryStore::new();
        let writer_clock = ManualClock::new(Timestamp(0));
        let writer = Client::open(
            OrderedMetaStore::new(&writer_store),
            &handle,
            &writer_clock,
            dev(1),
            TestNonceSource::new(1),
        )
        .await
        .unwrap();
        writer.attach(&envelope).await.unwrap();
        let writer_space = writer.space(space).await.unwrap();
        let set_receipt = writer_space
            .submit_unchecked([set(row, b"value")], vec![])
            .await
            .unwrap()
            .push()
            .await
            .unwrap();
        assert!(matches!(
            set_receipt,
            PushReceipt::Applied {
                admission_seq: Some(AdmissionSeq(1)),
                ..
            }
        ));
        let range_receipt = writer_space
            .submit_unchecked(
                [homebase::Mutation::DeleteRange {
                    range: Range::Prefix(db.clone()),
                }],
                vec![],
            )
            .await
            .unwrap()
            .push()
            .await
            .unwrap();
        assert!(matches!(
            range_receipt,
            PushReceipt::Applied {
                admission_seq: Some(AdmissionSeq(2)),
                ..
            }
        ));

        let reader_store = MemoryStore::new();
        {
            let reader_clock = ManualClock::new(Timestamp(0));
            let reader = Client::open(
                OrderedMetaStore::new(&reader_store),
                &handle,
                &reader_clock,
                dev(2),
                SystemNonceSource,
            )
            .await
            .unwrap();
            reader.attach(&envelope).await.unwrap();
            let reader_space = reader.space(space).await.unwrap();

            let before_fetch = audit(&OrderedMetaStore::new(&reader_store)).await;
            let fetched = reader_space
                .fetch(Range::Prefix(child.clone()), AdmissionSeq(0))
                .await
                .unwrap();
            let RangeCut::Delta(entries) = &fetched.cut else {
                panic!("expected range delta")
            };
            let source = entries
                .iter()
                .find_map(|entry| entry.device_entry.mutation.range())
                .expect("ancestor range source");
            let encoded_child = envelope
                .open()
                .unwrap()
                .encode_range(&Range::Prefix(child))
                .unwrap();
            assert_eq!(fetched.delete_range_effect(source), Some(encoded_child));
            assert_eq!(
                audit(&OrderedMetaStore::new(&reader_store)).await,
                before_fetch,
                "stateless fetch must not advance ver or admit cursors"
            );

            assert_eq!(reader_space.pull().await.unwrap(), AdmissionSeq(2));
            let pulled_state = audit(&OrderedMetaStore::new(&reader_store)).await;
            let pulled_space = &pulled_state.spaces[&space];
            assert_eq!(pulled_space.ver_high, Some(Ver(2)));
            assert_eq!(pulled_space.admit_cursors.tail, AdmissionSeq(3));
            let pending = reader_space.admits().iter_from_neck().await.unwrap();
            assert_eq!(pending.len(), 2);
            assert!(matches!(
                &pending[1].entries[0].device_entry.mutation,
                homebase::Mutation::DeleteRange { .. }
            ));
        }

        let reopened_clock = ManualClock::new(Timestamp(0));
        let reopened = Client::open(
            OrderedMetaStore::new(&reader_store),
            &handle,
            &reopened_clock,
            dev(9),
            SystemNonceSource,
        )
        .await
        .unwrap();
        reopened.attach(&envelope).await.unwrap();
        let reopened_space = reopened.space(space).await.unwrap();
        let pending = reopened_space.admits().iter_from_neck().await.unwrap();
        assert_eq!(pending.len(), 2);
        assert!(matches!(
            &pending[1].entries[0].device_entry.mutation,
            homebase::Mutation::DeleteRange { .. }
        ));
    });
}

#[test]
fn pull_authenticates_the_complete_page_before_appending_any_batch() {
    block_on(async {
        let envelope = encrypted_envelope();
        let space = envelope.space_id();
        let handle = spawn_server(Arc::new(ManualClock::new(Timestamp(0))), &[space]);
        let writer_store = MemoryStore::new();
        let writer_clock = ManualClock::new(Timestamp(0));
        let writer = Client::open(
            OrderedMetaStore::new(&writer_store),
            &handle,
            &writer_clock,
            dev(1),
            TestNonceSource::new(1),
        )
        .await
        .unwrap();
        writer.attach(&envelope).await.unwrap();
        let writer_space = writer.space(space).await.unwrap();
        writer_space
            .submit_unchecked([set(key(&[b"db", b"row"]), b"secret")], vec![])
            .await
            .unwrap();
        writer_space
            .submit_unchecked(
                [homebase::Mutation::DeleteRange {
                    range: Range::Prefix(key(&[b"db"])),
                }],
                vec![],
            )
            .await
            .unwrap();
        writer_space.push().await.unwrap();
        drop(writer_space);
        drop(writer);

        let tampered = TamperPull { inner: handle };
        let reader_store = MemoryStore::new();
        let reader_clock = ManualClock::new(Timestamp(0));
        let reader = Client::open(
            OrderedMetaStore::new(&reader_store),
            tampered,
            &reader_clock,
            dev(2),
            SystemNonceSource,
        )
        .await
        .unwrap();
        reader.attach(&envelope).await.unwrap();
        let error = reader.space(space).await.unwrap().pull().await.unwrap_err();
        assert!(matches!(error, homebase::SpaceDriverError::Cipher(_)));

        let state = audit(&OrderedMetaStore::new(&reader_store)).await;
        let state = &state.spaces[&space];
        assert_eq!(state.admit_cursors, homebase::meta::AdmitCursors::default());
        assert!(state.admits.is_empty());
        assert_eq!(state.ver_high, None);
    });
}
