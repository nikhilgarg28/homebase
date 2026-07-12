//! The homebase kernel server.
//!
//! Layering, outermost in:
//!
//! - [`Server`] — owns one shard: the shared [`storage::OrderedStore`], the
//!   [`Clock`](homebase_core::clock::Clock), and the `SpaceId` →
//!   [`actor::SpaceHandle`] table. Routes requests and spawns space actors
//!   lazily on first touch. Token verification (token → `SpaceId` + prefix
//!   scope) sits at the wire layer above this.
//! - [`actor::SpaceActor`] / [`actor::SpaceHandle`] — the runtime shell:
//!   one mailbox per space, verbs dequeued one at a time with `now` stamped
//!   at dequeue. The handle implements the core `Space` trait; the run loop
//!   is runtime-agnostic (tokio in production, the deterministic sim's
//!   stepper in torture tests) and reaches its executor only through
//!   [`actor::Spawner`].
//! - [`space::Space`] — one space's complete verb state machine (lease
//!   table + data plane), deterministic: explicit `now: Timestamp`, verbs
//!   executed one at a time, proptested and torture-simmed directly.
//! - [`storage::OrderedStore`] — the async ordered map underneath (slatedb
//!   in prod, [`storage::MemoryStore`] in tests); determinism holds because
//!   verbs never interleave and the test store resolves futures immediately.

pub mod actor;
pub mod error;
pub mod schema;
pub mod space;
pub mod storage;

use actor::{SpaceActor, SpaceHandle, Spawner};
use homebase_core::clock::Clock;
use homebase_core::space::SpaceId;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use storage::OrderedStore;

/// One shard: many spaces behind one store, one clock, one endpoint.
///
/// Spaces are fully isolated (no verb spans two spaces), so this layer is
/// pure routing plus lifecycle:
///
/// - **Registration** ([`create_space`](Server::create_space)) marks a space
///   as existing — a directory action. It is deliberately *not* one of the
///   seven data-plane verbs: it's a tenant-plane operation (token-authorized
///   and quota-checked at the layer above), and it costs the shard nothing.
/// - **Actors are lazy** ([`space`](Server::space)): the first touch of a
///   registered space builds its [`SpaceActor`] over the shared store and
///   hands the run loop to the [`Spawner`]. All state lives in the store, so
///   an actor is pure runtime machinery — nothing is lost if a future
///   version parks idle actors and respawns them.
///
/// The server holds a handle clone per live actor, so actors run until the
/// server itself is dropped (idle parking is a later optimization).
pub struct Server<S, C, P> {
    store: Arc<S>,
    clock: Arc<C>,
    spawner: P,
    /// Registered spaces; `None` until first touch spawns the actor.
    spaces: Mutex<BTreeMap<SpaceId, Option<SpaceHandle>>>,
}

impl<S, C, P> Server<S, C, P>
where
    S: OrderedStore + Send + Sync + 'static,
    C: Clock + Send + Sync + 'static,
    P: Spawner,
{
    pub fn new(store: Arc<S>, clock: Arc<C>, spawner: P) -> Self {
        Self {
            store,
            clock,
            spawner,
            spaces: Mutex::new(BTreeMap::new()),
        }
    }

    /// Registers a space id. Returns `false` (and changes nothing) if the
    /// id is already registered. Spawns nothing.
    pub fn create_space(&self, id: SpaceId) -> bool {
        let mut spaces = self.spaces.lock().unwrap();
        match spaces.entry(id) {
            std::collections::btree_map::Entry::Occupied(_) => false,
            std::collections::btree_map::Entry::Vacant(v) => {
                v.insert(None);
                true
            }
        }
    }

    /// Looks up a space for request routing, spawning its actor on first
    /// touch. `None` for unregistered ids.
    pub fn space(&self, id: &SpaceId) -> Option<SpaceHandle> {
        let mut spaces = self.spaces.lock().unwrap();
        let slot = spaces.get_mut(id)?;
        if let Some(handle) = slot {
            return Some(handle.clone());
        }
        let (space_actor, handle) =
            SpaceActor::new(*id, Arc::clone(&self.store), Arc::clone(&self.clock));
        self.spawner.spawn(Box::pin(space_actor.run()));
        *slot = Some(handle.clone());
        Some(handle)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::executor::{LocalPool, LocalSpawner};
    use futures::task::SpawnExt;
    use homebase_core::clock::{HybridTimestamp, ManualClock, Timestamp};
    use homebase_core::key::Key;
    use homebase_core::lease::LeaseMode;
    use homebase_core::messages::{
        AcquireRequest, AdmissionBatch, AdmissionRequest, GetRequest, LeaseSpec,
    };
    use homebase_core::seal::Seal;
    use homebase_core::space::Space as _;
    use homebase_core::tag::{
        AdmissionSeq, CipherEpoch, Ciphertext, DeviceEntry, DeviceId, DeviceSeq, DeviceTag,
        Mutation, Ver,
    };
    use std::cell::Cell;
    use std::pin::Pin;
    use std::rc::Rc;
    use std::time::Duration;
    use storage::MemoryStore;

    /// Test spawner: counts spawns, runs tasks on the pool.
    #[derive(Clone)]
    struct CountingSpawner {
        inner: LocalSpawner,
        count: Rc<Cell<usize>>,
    }

    impl Spawner for CountingSpawner {
        fn spawn(&self, task: Pin<Box<dyn Future<Output = ()> + Send + 'static>>) {
            self.count.set(self.count.get() + 1);
            self.inner.spawn(task).unwrap();
        }
    }

    fn server(
        pool: &LocalPool,
    ) -> (
        Server<MemoryStore, ManualClock, CountingSpawner>,
        Rc<Cell<usize>>,
    ) {
        let count = Rc::new(Cell::new(0));
        let spawner = CountingSpawner {
            inner: pool.spawner(),
            count: Rc::clone(&count),
        };
        let server = Server::new(
            Arc::new(MemoryStore::new()),
            Arc::new(ManualClock::new(Timestamp(0))),
            spawner,
        );
        (server, count)
    }

    fn dev(n: u8) -> DeviceId {
        DeviceId([n; 16])
    }

    fn key(components: &[&[u8]]) -> Key {
        Key::from_bytes(components.iter().copied()).unwrap()
    }

    /// Acquire a write lease on `("db",)` and put one key through a handle.
    async fn write_marker(handle: &SpaceHandle, marker: &[u8]) -> AdmissionSeq {
        let granted = handle
            .acquire(AcquireRequest {
                device: dev(1),
                requested_at: HybridTimestamp::ZERO,
                specs: vec![LeaseSpec {
                    prefix: key(&[b"db"]),
                    mode: LeaseMode::Write,
                    ttl: Duration::from_secs(60),
                }],
            })
            .await
            .unwrap();
        let lease = granted.leases[0].id;
        handle
            .admit(AdmissionRequest {
                device: dev(1),
                expected_checksum: homebase_core::DeviceChecksum::EMPTY,
                evidence: vec![lease],
                batches: vec![AdmissionBatch {
                    device_seq: DeviceSeq(1),
                    range_asserts: vec![],
                    entries: vec![DeviceEntry {
                        mutation: Mutation::Set {
                            key: key(&[b"db", b"marker"]),
                            value: Ciphertext(marker.to_vec()),
                        },
                        tag: DeviceTag {
                            device: dev(1),
                            device_seq: DeviceSeq(1),
                            ver: Ver(1),
                            cipher_epoch: CipherEpoch(0),
                        },
                        seal: Seal::empty_aead_v1(),
                    }],
                }],
            })
            .await
            .unwrap()
            .applied_admission_seq(0)
            .unwrap()
    }

    async fn read_marker(handle: &SpaceHandle) -> Option<Vec<u8>> {
        let got = handle
            .get(GetRequest {
                keys: vec![key(&[b"db", b"marker"])],
            })
            .await
            .unwrap();
        got.entries[0]
            .as_ref()
            .map(|e| match &e.device_entry.mutation {
                Mutation::Set { value, .. } => value.0.clone(),
                Mutation::Delete { .. } => panic!("tombstone in get"),
            })
    }

    #[test]
    fn registration_is_idempotent_and_routing_is_exact() {
        let pool = LocalPool::new();
        let (server, _count) = server(&pool);
        let a = SpaceId([1; 16]);
        let b = SpaceId([2; 16]);

        assert!(server.create_space(a));
        assert!(!server.create_space(a), "duplicate id must be rejected");

        assert!(server.space(&a).is_some());
        assert!(
            server.space(&b).is_none(),
            "unregistered space must not route"
        );
    }

    #[test]
    fn actors_spawn_lazily_and_exactly_once() {
        let pool = LocalPool::new();
        let (server, count) = server(&pool);
        let a = SpaceId([1; 16]);

        server.create_space(a);
        assert_eq!(count.get(), 0, "registration must spawn nothing");

        server.space(&a);
        assert_eq!(count.get(), 1, "first touch spawns the actor");
        server.space(&a);
        assert_eq!(count.get(), 1, "second touch reuses it");
    }

    #[test]
    fn spaces_are_isolated_end_to_end() {
        let mut pool = LocalPool::new();
        let (server, _count) = server(&pool);
        let a = SpaceId([1; 16]);
        let b = SpaceId([2; 16]);
        server.create_space(a);
        server.create_space(b);

        let ha = server.space(&a).unwrap();
        let hb = server.space(&b).unwrap();

        pool.run_until(async move {
            // Same key, same device, both spaces on one shared store: each
            // space sees only its own write, and admission seqs are
            // per-space (both batches are seq 1).
            let seq_a = write_marker(&ha, b"from-a").await;
            let seq_b = write_marker(&hb, b"from-b").await;
            assert_eq!(seq_a, AdmissionSeq(1));
            assert_eq!(
                seq_b,
                AdmissionSeq(1),
                "admission sequences must not couple"
            );

            assert_eq!(read_marker(&ha).await, Some(b"from-a".to_vec()));
            assert_eq!(read_marker(&hb).await, Some(b"from-b".to_vec()));
        });
    }
}
