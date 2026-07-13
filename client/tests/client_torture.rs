//! Client-layer deterministic simulation: [`Client`] + [`Space`] over
//! fault-injecting meta, crash/reopen, and multi-task interleaving.

use homebase::actor::{SpaceActor, SpaceHandle};
use homebase_client::cipher::{SpaceEnvelope, SystemNonceSource};
use homebase_client::meta::{MetaStore, OrderedMetaStore, audit, certify};
use homebase_client::server::ServerHandle;
use homebase_client::{Client, ClientError, PushOutcome, SpaceDriverError};
use homebase_core::clock::{HybridClock, ManualClock, Timestamp};
use homebase_core::key::Key;
use homebase_core::lease::LeaseMode;
use homebase_core::messages::{
    AcquireRequest, AdmissionRequest, AdmissionResponse, AdmissionResult, GetRequest, KernelError,
    LeaseSpec, Range, RangeCut,
};
use homebase_core::space::{Space as _, SpaceId};
use homebase_core::storage::MemoryStore;
use homebase_core::tag::{AdmissionSeq, DeviceId, Mutation};
use homebase_sim::seeds;
use homebase_sim::store::{FaultConfig, SimStore};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

fn spawn_actor_thread(
    store: Arc<MemoryStore>,
    clock: Arc<ManualClock>,
) -> (SpaceHandle, JoinHandle<()>) {
    let (actor, handle) = SpaceActor::new(SPACE, store, clock);
    let join = thread::spawn(move || pollster::block_on(actor.run()));
    (handle, join)
}

const SPACE: SpaceId = SpaceId([6; 16]);
const DEVICE: DeviceId = DeviceId([1; 16]);
const PHASES: usize = 4;
const KEY_POOL: u64 = 6;
const WRITER_ATTEMPTS: u32 = 35;
const PUSHER_ROUNDS: u32 = 25;

const FAULTS: FaultConfig = FaultConfig {
    error_rate: 0.01,
    flush_rate: 0.25,
    max_latency_yields: 3,
};

#[derive(Clone, Debug, PartialEq, Eq)]
enum ModelValue {
    Present(Vec<u8>),
    Absent,
}

#[derive(Clone, Debug)]
struct Ack {
    key_index: u64,
    value: ModelValue,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct Coverage {
    tombstones: u32,
    range_deletes: u32,
    overwrites: u32,
    storage_errors: u32,
    push_stalls: u32,
    commits: u32,
    pushes: u32,
    pulls: u32,
    admit_marks: u32,
    admit_trims: u32,
}

#[derive(Clone)]
struct ClientTestServer {
    space: SpaceId,
    handle: SpaceHandle,
    acks: Arc<Mutex<Vec<Ack>>>,
}

impl ClientTestServer {
    fn new(space: SpaceId, handle: SpaceHandle, acks: Arc<Mutex<Vec<Ack>>>) -> Self {
        Self {
            space,
            handle,
            acks,
        }
    }
}

impl ServerHandle for ClientTestServer {
    async fn acquire(
        &self,
        space: &SpaceId,
        req: AcquireRequest,
    ) -> Result<homebase_core::messages::AcquireResponse, homebase_core::space::SpaceError> {
        if *space != self.space {
            return Err(homebase_core::space::SpaceError::unavailable(
                "space not served",
            ));
        }
        self.handle.acquire(req).await
    }

    async fn renew(
        &self,
        space: &SpaceId,
        req: homebase_core::messages::RenewRequest,
    ) -> Result<homebase_core::messages::RenewResponse, homebase_core::space::SpaceError> {
        if *space != self.space {
            return Err(homebase_core::space::SpaceError::unavailable(
                "space not served",
            ));
        }
        self.handle.renew(req).await
    }

    async fn release(
        &self,
        space: &SpaceId,
        req: homebase_core::messages::ReleaseRequest,
    ) -> Result<homebase_core::messages::ReleaseResponse, homebase_core::space::SpaceError> {
        if *space != self.space {
            return Err(homebase_core::space::SpaceError::unavailable(
                "space not served",
            ));
        }
        self.handle.release(req).await
    }

    async fn list_leases(
        &self,
        space: &SpaceId,
        req: homebase_core::messages::ListLeasesRequest,
    ) -> Result<homebase_core::messages::ListLeasesResponse, homebase_core::space::SpaceError> {
        if *space != self.space {
            return Err(homebase_core::space::SpaceError::unavailable(
                "space not served",
            ));
        }
        self.handle.list_leases(req).await
    }

    async fn admit(
        &self,
        space: &SpaceId,
        req: AdmissionRequest,
    ) -> Result<AdmissionResponse, homebase_core::space::SpaceError> {
        if *space != self.space {
            return Err(homebase_core::space::SpaceError::unavailable(
                "space not served",
            ));
        }
        let resp = self.handle.admit(req.clone()).await?;
        let mut acks = self.acks.lock().unwrap();
        for (batch, result) in req.batches.iter().zip(&resp.results) {
            if matches!(result, AdmissionResult::Applied { .. }) {
                for op in &batch.entries {
                    apply_ack(&mut acks, &op.mutation);
                }
            }
        }
        Ok(resp)
    }

    async fn get(
        &self,
        space: &SpaceId,
        req: GetRequest,
    ) -> Result<homebase_core::messages::GetResponse, homebase_core::space::SpaceError> {
        if *space != self.space {
            return Err(homebase_core::space::SpaceError::unavailable(
                "space not served",
            ));
        }
        self.handle.get(req).await
    }

    async fn pull(
        &self,
        space: &SpaceId,
        req: homebase_core::messages::PullRequest,
    ) -> Result<homebase_core::messages::PullResponse, homebase_core::space::SpaceError> {
        if *space != self.space {
            return Err(homebase_core::space::SpaceError::unavailable(
                "space not served",
            ));
        }
        self.handle.pull(req).await
    }

    async fn list(
        &self,
        space: &SpaceId,
        req: homebase_core::messages::ListRequest,
    ) -> Result<homebase_core::messages::ListResponse, homebase_core::space::SpaceError> {
        if *space != self.space {
            return Err(homebase_core::space::SpaceError::unavailable(
                "space not served",
            ));
        }
        self.handle.list(req).await
    }

    async fn read_at(
        &self,
        space: &SpaceId,
        req: homebase_core::messages::ReadAtRequest,
    ) -> Result<homebase_core::messages::ReadAtResponse, homebase_core::space::SpaceError> {
        if *space != self.space {
            return Err(homebase_core::space::SpaceError::unavailable(
                "space not served",
            ));
        }
        self.handle.read_at(req).await
    }
}

fn apply_ack(acks: &mut Vec<Ack>, mutation: &Mutation<homebase_core::OpaqueValue>) {
    match mutation {
        Mutation::Set { key, value } => {
            set_ack(acks, pool_index(key), ModelValue::Present(value.0.clone()));
        }
        Mutation::Delete { key } => set_ack(acks, pool_index(key), ModelValue::Absent),
        Mutation::DeleteRange { range } => {
            for key_index in 0..KEY_POOL {
                if range.covers_key(&pool_key(key_index)) {
                    set_ack(acks, key_index, ModelValue::Absent);
                }
            }
        }
    }
}

fn set_ack(acks: &mut Vec<Ack>, key_index: u64, value: ModelValue) {
    acks.retain(|ack| ack.key_index != key_index);
    acks.push(Ack { key_index, value });
}

fn apply_mutation<T: AsRef<[u8]>>(view: &mut BTreeMap<Key, ModelValue>, mutation: &Mutation<T>) {
    match mutation {
        Mutation::Set { key, value } => {
            view.insert(key.clone(), ModelValue::Present(value.as_ref().to_vec()));
        }
        Mutation::Delete { key } => {
            view.insert(key.clone(), ModelValue::Absent);
        }
        Mutation::DeleteRange { range } => {
            for (key, value) in view.iter_mut() {
                if range.covers_key(key) {
                    *value = ModelValue::Absent;
                }
            }
        }
    }
}

#[derive(Clone)]
struct SharedClock(Arc<ManualClock>);

impl HybridClock for SharedClock {
    fn stamp(&self) -> homebase_core::clock::HybridTimestamp {
        self.0.stamp()
    }
}

type TestClient =
    Client<OrderedMetaStore<SimStore>, ClientTestServer, SharedClock, SystemNonceSource>;
type ClientSlot = Rc<RefCell<Option<TestClient>>>;

fn prefix() -> Key {
    Key::from_bytes([b"db".to_vec()]).unwrap()
}

fn pool_key(i: u64) -> Key {
    Key::from_bytes([b"db".to_vec(), format!("k{i}").into_bytes()]).unwrap()
}

fn pool_index(key: &Key) -> u64 {
    let comps = key.components();
    let s = std::str::from_utf8(comps[1].as_bytes()).unwrap();
    s.strip_prefix('k').unwrap().parse().unwrap()
}

fn wspec() -> LeaseSpec {
    LeaseSpec {
        prefix: prefix(),
        mode: LeaseMode::Write,
        ttl: Duration::from_secs(60),
    }
}

struct ClientGuard<'a> {
    slot: &'a ClientSlot,
    client: Option<TestClient>,
}

impl Drop for ClientGuard<'_> {
    fn drop(&mut self) {
        if let Some(client) = self.client.take() {
            *self.slot.borrow_mut() = Some(client);
        }
    }
}

fn take_client(slot: &ClientSlot) -> ClientGuard<'_> {
    ClientGuard {
        slot,
        client: slot.borrow_mut().take(),
    }
}

fn finish_client(slot: &ClientSlot, mut guard: ClientGuard<'_>) {
    let client = guard.client.take().expect("client lost");
    *slot.borrow_mut() = Some(client);
    std::mem::forget(guard);
}

#[derive(Clone)]
struct DriverState {
    vers: Rc<RefCell<BTreeMap<u64, u64>>>,
    rng_seed: u64,
}

const DRIVER_STEPS: u32 = WRITER_ATTEMPTS + PUSHER_ROUNDS;

async fn driver(
    slot: ClientSlot,
    state: DriverState,
    coverage: Rc<RefCell<Coverage>>,
    sim: SimStore,
    server: ClientTestServer,
    clock: SharedClock,
) {
    let mut rng = StdRng::seed_from_u64(state.rng_seed);
    for _ in 0..DRIVER_STEPS {
        let mut guard = take_client(&slot);
        let client = guard.client.as_mut().expect("client slot empty");

        if rng.random_bool(0.35) {
            match client.space(SPACE).await.unwrap().push().await {
                Ok(PushOutcome::Stalled { .. }) => coverage.borrow_mut().push_stalls += 1,
                Ok(PushOutcome::Drained { .. }) => {}
                Err(ClientError::Store(_))
                | Err(ClientError::Space(SpaceDriverError::Storage(_))) => {
                    coverage.borrow_mut().storage_errors += 1;
                }
                Err(ClientError::Space(SpaceDriverError::Fork { admitted })) => {
                    guard.client.take();
                    OrderedMetaStore::new(sim.clone())
                        .trim_oplog(SPACE, admitted, homebase_core::DeviceChecksum::EMPTY)
                        .await
                        .expect("trim after fork");
                    *slot.borrow_mut() =
                        Some(open_attached(sim.clone(), server.clone(), clock.clone()).await);
                    std::mem::forget(guard);
                    coverage.borrow_mut().pushes += 1;
                    continue;
                }
                Err(err) => panic!("unexpected push failure: {err:?}"),
            }
            coverage.borrow_mut().pushes += 1;
            finish_client(&slot, guard);
            continue;
        }

        if rng.random_bool(0.2) {
            let space = match client.space(SPACE).await {
                Ok(space) => space,
                Err(ClientError::Store(_)) => {
                    coverage.borrow_mut().storage_errors += 1;
                    finish_client(&slot, guard);
                    continue;
                }
                Err(err) => panic!("unexpected space open: {err:?}"),
            };
            match space.pull().await {
                Ok(_) => {
                    coverage.borrow_mut().pulls += 1;
                    match space.admits().cursors().await {
                        Ok(cursors) if cursors.neck < cursors.tail => {
                            match space.admits().iter_from_neck().await {
                                Ok(batches) => {
                                    assert_eq!(
                                        batches.first().map(|batch| batch.admission_seq),
                                        Some(cursors.neck)
                                    );
                                    assert_eq!(
                                        batches.last().map(|batch| batch.admission_seq.0 + 1),
                                        Some(cursors.tail.0)
                                    );
                                    match space.admits().mark_applied(cursors.tail).await {
                                        Ok(()) => {
                                            coverage.borrow_mut().admit_marks += 1;
                                            match space.admits().trim(cursors.tail).await {
                                                Ok(()) => {
                                                    coverage.borrow_mut().admit_trims += 1;
                                                }
                                                Err(SpaceDriverError::Storage(_)) => {
                                                    coverage.borrow_mut().storage_errors += 1;
                                                }
                                                Err(err) => {
                                                    panic!("unexpected admit trim failure: {err:?}")
                                                }
                                            }
                                        }
                                        Err(SpaceDriverError::Storage(_)) => {
                                            coverage.borrow_mut().storage_errors += 1;
                                        }
                                        Err(err) => {
                                            panic!("unexpected admit mark failure: {err:?}")
                                        }
                                    }
                                }
                                Err(SpaceDriverError::Storage(_)) => {
                                    coverage.borrow_mut().storage_errors += 1;
                                }
                                Err(err) => panic!("unexpected admit read failure: {err:?}"),
                            }
                        }
                        Ok(_) => {}
                        Err(SpaceDriverError::Storage(_)) => {
                            coverage.borrow_mut().storage_errors += 1;
                        }
                        Err(err) => panic!("unexpected admit cursor failure: {err:?}"),
                    }
                }
                Err(SpaceDriverError::Storage(_)) => {
                    coverage.borrow_mut().storage_errors += 1;
                }
                Err(err) => panic!("unexpected pull failure: {err:?}"),
            }
            finish_client(&slot, guard);
            continue;
        }

        let key_index = rng.random_range(0..KEY_POOL);
        let range_delete = rng.random_bool(0.2);
        let tombstone = !range_delete && rng.random_bool(0.3);
        let ver = state.vers.borrow().get(&key_index).copied().unwrap_or(0) + 1;
        let stamp = rng.random::<u32>();
        let value = if tombstone {
            ModelValue::Absent
        } else {
            ModelValue::Present(format!("s{stamp}").into_bytes())
        };

        let space = match client.space(SPACE).await {
            Ok(s) => s,
            Err(ClientError::Store(_)) => {
                coverage.borrow_mut().storage_errors += 1;
                finish_client(&slot, guard);
                continue;
            }
            Err(err) => panic!("unexpected space open: {err:?}"),
        };
        if let Err(err) = space.lease(vec![wspec()]).await {
            match err {
                SpaceDriverError::Storage(_) => coverage.borrow_mut().storage_errors += 1,
                SpaceDriverError::Rejected(KernelError::Contended { .. }) => {}
                other => panic!("unexpected lease failure: {other:?}"),
            }
            finish_client(&slot, guard);
            continue;
        }
        let mutation = if range_delete {
            let range = if rng.random_bool(0.5) {
                Range::Prefix(prefix())
            } else {
                Range::Prefix(pool_key(key_index))
            };
            Mutation::DeleteRange { range }
        } else {
            match value {
                ModelValue::Present(value) => Mutation::Set {
                    key: pool_key(key_index),
                    value,
                },
                ModelValue::Absent => Mutation::Delete {
                    key: pool_key(key_index),
                },
            }
        };
        match space.submit_checked(vec![mutation], vec![]).await {
            Ok(_) => {
                state.vers.borrow_mut().insert(key_index, ver);
                let mut cov = coverage.borrow_mut();
                cov.commits += 1;
                if range_delete {
                    cov.range_deletes += 1;
                } else if tombstone {
                    cov.tombstones += 1;
                } else if ver > 1 {
                    cov.overwrites += 1;
                }
            }
            Err(SpaceDriverError::Storage(_)) => coverage.borrow_mut().storage_errors += 1,
            Err(err) => panic!("unexpected submission failure: {err:?}"),
        }
        finish_client(&slot, guard);
    }
}

async fn open_attached(sim: SimStore, server: ClientTestServer, clock: SharedClock) -> TestClient {
    let client = Client::open(
        OrderedMetaStore::new(sim),
        server,
        clock,
        DEVICE,
        SystemNonceSource,
    )
    .await
    .expect("client open");
    client
        .attach(&SpaceEnvelope::plaintext(SPACE))
        .await
        .expect("attach");
    client
}

async fn drain_push(
    slot: &ClientSlot,
    coverage: &Rc<RefCell<Coverage>>,
    sim: &SimStore,
    server: &ClientTestServer,
    clock: &SharedClock,
) {
    for attempt in 0..500 {
        let mut guard = take_client(slot);
        let client = guard.client.as_mut().expect("client slot empty");
        let outcome = client.space(SPACE).await.unwrap().push().await;
        finish_client(slot, guard);

        match outcome {
            Ok(PushOutcome::Drained { .. }) => return,
            Ok(PushOutcome::Stalled { at, error, .. }) => {
                coverage.borrow_mut().push_stalls += 1;
                if matches!(
                    error,
                    KernelError::VerRegression { .. } | KernelError::RangeVerRegression { .. }
                ) {
                    let mut guard = take_client(slot);
                    let client = guard.client.as_mut().unwrap();
                    match client.rollback(SPACE, at).await {
                        Ok(()) => {}
                        Err(ClientError::Store(_)) => {
                            coverage.borrow_mut().storage_errors += 1;
                        }
                        Err(err) => panic!("rollback failed during settle: {err:?}"),
                    }
                    finish_client(slot, guard);
                    continue;
                }
                if matches!(
                    error,
                    KernelError::NotCovered { .. } | KernelError::LeaseInvalid { .. }
                ) {
                    let mut guard = take_client(slot);
                    let client = guard.client.as_mut().unwrap();
                    let space = client.space(SPACE).await.unwrap();
                    let _ = space.lease(vec![wspec()]).await;
                    finish_client(slot, guard);
                }
            }
            Err(ClientError::Space(SpaceDriverError::Fork { admitted })) => {
                OrderedMetaStore::new(sim.clone())
                    .trim_oplog(SPACE, admitted, homebase_core::DeviceChecksum::EMPTY)
                    .await
                    .expect("trim after fork");
                *slot.borrow_mut() =
                    Some(open_attached(sim.clone(), server.clone(), clock.clone()).await);
            }
            Err(ClientError::Store(_)) | Err(ClientError::Space(SpaceDriverError::Storage(_))) => {
                coverage.borrow_mut().storage_errors += 1;
            }
            Err(err) => panic!("push failed during settle: {err:?}"),
        }
        assert!(
            attempt + 1 < 500,
            "drain_push stuck after 500 attempts (oplog may not drain)"
        );
    }
}

fn replay_oplog(
    mut view: BTreeMap<Key, ModelValue>,
    state: &homebase_client::meta::ClientState,
) -> BTreeMap<Key, ModelValue> {
    let Some(space) = state.spaces.get(&SPACE) else {
        return view;
    };
    for record in space.oplog.values() {
        for entry in record.entries() {
            apply_mutation(&mut view, &entry.mutation);
        }
    }
    view
}

async fn read_equivalence(
    slot: &ClientSlot,
    server: &ClientTestServer,
    meta: &OrderedMetaStore<SimStore>,
    seed: u64,
    phase: usize,
) {
    let state = audit(meta).await;
    let mut guard = take_client(slot);
    let client = guard.client.as_mut().expect("client slot empty");
    let space = client.space(SPACE).await.unwrap();
    let pulled = space
        .fetch(Range::Prefix(prefix()), AdmissionSeq(0))
        .await
        .unwrap();
    finish_client(slot, guard);

    let entries = match &pulled.cut {
        RangeCut::Snapshot(entries) | RangeCut::Delta(entries) => entries,
    };
    let mut expected = BTreeMap::new();
    for entry in entries {
        apply_mutation(&mut expected, &entry.device_entry.mutation);
    }
    expected = replay_oplog(expected, &state);

    for key_index in 0..KEY_POOL {
        let key = pool_key(key_index);
        let expected_value = expected.get(&key).unwrap_or(&ModelValue::Absent);
        let got = server
            .get(
                &SPACE,
                GetRequest {
                    keys: vec![key.clone()],
                },
            )
            .await
            .unwrap()
            .entries
            .remove(0);
        let got = match got {
            None => ModelValue::Absent,
            Some(entry) => match entry.device_entry.mutation {
                Mutation::Set { value, .. } => ModelValue::Present(value.0),
                Mutation::Delete { .. } => ModelValue::Absent,
                Mutation::DeleteRange { .. } => {
                    panic!("point get returned a range mutation")
                }
            },
        };
        assert_eq!(
            got, *expected_value,
            "pull ⊕ oplog diverged at {key:?} (seed {seed}, phase {phase})"
        );
    }
}

fn phase_oracle(
    server: &ClientTestServer,
    acks: &Arc<Mutex<Vec<Ack>>>,
    meta: &OrderedMetaStore<SimStore>,
    seed: u64,
    phase: usize,
) {
    let state = pollster::block_on(audit(meta));
    certify(&state);

    for ack in acks.lock().unwrap().iter() {
        let key = pool_key(ack.key_index);
        let entry = pollster::block_on(server.get(
            &SPACE,
            GetRequest {
                keys: vec![key.clone()],
            },
        ))
        .unwrap()
        .entries
        .remove(0);
        match (&ack.value, entry) {
            (ModelValue::Absent, None) => {}
            (_, Some(entry))
                if match &entry.device_entry.mutation {
                    Mutation::Set { value, .. } => ModelValue::Present(value.0.clone()),
                    Mutation::Delete { .. } => ModelValue::Absent,
                    Mutation::DeleteRange { .. } => panic!("point get returned a range mutation"),
                } == ack.value => {}
            (_, got) => {
                panic!("acked value corrupted: {ack:?} got {got:?} (seed {seed}, phase {phase})")
            }
        }
    }
}

fn run_seed(seed: u64) -> Coverage {
    let mut master = StdRng::seed_from_u64(seed);
    let meta = SimStore::new(master.random(), FAULTS);
    let server_store = Arc::new(MemoryStore::new());
    let clock = Arc::new(ManualClock::new(Timestamp(0)));
    let shared = SharedClock(Arc::clone(&clock));
    let acks = Arc::new(Mutex::new(Vec::<Ack>::new()));
    let coverage = Rc::new(RefCell::new(Coverage::default()));
    let driver_state = DriverState {
        vers: Rc::new(RefCell::new(BTreeMap::new())),
        rng_seed: master.random(),
    };

    let (handle, actor_join) = spawn_actor_thread(Arc::clone(&server_store), Arc::clone(&clock));
    let server = ClientTestServer::new(SPACE, handle, Arc::clone(&acks));

    for phase in 0..PHASES {
        meta.set_config(FAULTS);
        acks.lock().unwrap().clear();

        meta.set_config(FaultConfig::NONE);
        let client = Rc::new(RefCell::new(Some(pollster::block_on(open_attached(
            meta.clone(),
            server.clone(),
            shared.clone(),
        )))));
        meta.set_config(FAULTS);

        pollster::block_on(driver(
            Rc::clone(&client),
            driver_state.clone(),
            Rc::clone(&coverage),
            meta.clone(),
            server.clone(),
            shared.clone(),
        ));

        if phase != PHASES - 1 {
            meta.flush();
            meta.crash();
        }

        meta.set_config(FaultConfig::NONE);
        *client.borrow_mut() = Some(pollster::block_on(open_attached(
            meta.clone(),
            server.clone(),
            shared.clone(),
        )));
        pollster::block_on(drain_push(&client, &coverage, &meta, &server, &shared));
        phase_oracle(
            &server,
            &acks,
            &OrderedMetaStore::new(meta.clone()),
            seed,
            phase,
        );
        pollster::block_on(read_equivalence(
            &client,
            &server,
            &OrderedMetaStore::new(meta.clone()),
            seed,
            phase,
        ));
    }

    drop(server);
    drop(actor_join);
    *coverage.borrow()
}

#[test]
fn client_torture_seeds_hold_invariants() {
    let mut total = Coverage::default();
    for seed in seeds::torture_seeds() {
        let cov = run_seed(seed);
        total.tombstones += cov.tombstones;
        total.range_deletes += cov.range_deletes;
        total.overwrites += cov.overwrites;
        total.storage_errors += cov.storage_errors;
        total.push_stalls += cov.push_stalls;
        total.commits += cov.commits;
        total.pushes += cov.pushes;
        total.pulls += cov.pulls;
        total.admit_marks += cov.admit_marks;
        total.admit_trims += cov.admit_trims;
    }
    println!(
        "client torture across {} seeds: {total:?}",
        seeds::torture_seed_count()
    );
    if !seeds::torture_coverage_enforced() {
        return;
    }
    assert!(total.commits > 0, "no commits: {total:?}");
    assert!(total.pushes > 0, "no pushes: {total:?}");
    assert!(
        total.storage_errors > 0,
        "no storage faults exercised: {total:?}"
    );
    assert!(total.tombstones > 0, "no tombstones: {total:?}");
    assert!(total.range_deletes > 0, "no range deletes: {total:?}");
    assert!(total.overwrites > 0, "no overwrites: {total:?}");
    assert!(total.pulls > 0, "no pulls: {total:?}");
    assert!(total.admit_marks > 0, "no admit marks: {total:?}");
    assert!(total.admit_trims > 0, "no admit trims: {total:?}");
}

#[test]
fn client_torture_replays_identically() {
    for seed in [0, 11, 42] {
        let a = run_seed(seed);
        let b = run_seed(seed);
        assert_eq!(a, b, "seed {seed} diverged on replay");
    }
}
