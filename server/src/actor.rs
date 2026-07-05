//! The space actor: the runtime shell that makes one [`Space`] machine
//! concurrent-safe by refusing concurrency.
//!
//! One actor per space, one mailbox, one logical thread. Verbs are dequeued
//! and run strictly one at a time; `now` is stamped from the actor's
//! [`Clock`] at dequeue — that instant is the linearization point every
//! admission seq and epoch hangs off. [`SpaceHandle`] is the cheap clonable
//! sender side, and is *the* implementation of the client-facing
//! [`Space` trait](homebase_core::space::Space).
//!
//! # Runtime-agnostic by design (the DST contract)
//!
//! [`SpaceActor::run`] is a plain future over channel primitives; nothing
//! here spawns, sleeps, or reads a wall clock. Production hands `run()` to
//! tokio; the deterministic sim hands the *identical* code to its seeded
//! single-threaded stepper and cranks a [`ManualClock`]. Keeping this file
//! executor-free is a standing invariant, not an accident.
//!
//! The mailbox is currently unbounded: backpressure belongs to the wire
//! layer (bound the number of in-flight requests per connection), not to
//! the actor — revisit if a shared server ever fronts untrusted request
//! floods directly.
//!
//! [`ManualClock`]: homebase_core::clock::ManualClock

use crate::space::Space;
use crate::storage::OrderedStore;
use futures_channel::{mpsc, oneshot};
use futures_core::Stream;
use homebase_core::clock::Clock;
use homebase_core::messages::{
    AcquireRequest, AcquireResponse, GetRequest, GetResponse, ListRequest, ListResponse,
    PutBatchRequest, PutBatchResponse, ReadAtRequest, ReadAtResponse, ReleaseRequest,
    ReleaseResponse, RenewRequest, RenewResponse,
};
use homebase_core::space::{Space as SpaceApi, SpaceError, SpaceId};
use std::pin::Pin;
use std::sync::Arc;

/// The one hook through which anything in this crate reaches an executor.
///
/// The library never spawns on its own (the DST contract): production hands
/// this to tokio (`handle.spawn(task)`), tests to a `LocalPool`, the sim to
/// its seeded stepper. Futures are boxed `Send` — [`SpaceActor::run`] is
/// `Send` by construction (asserted in tests).
pub trait Spawner {
    fn spawn(&self, task: Pin<Box<dyn Future<Output = ()> + Send + 'static>>);
}

type Reply<T> = oneshot::Sender<Result<T, SpaceError>>;

/// One queued verb: the request plus the reply slot. The oneshot is the
/// whole calling convention — dropping it (client gave up) is fine, and a
/// dropped *actor* cancels it, which the handle reports as `Unavailable`.
enum Command {
    Acquire(AcquireRequest, Reply<AcquireResponse>),
    Renew(RenewRequest, Reply<RenewResponse>),
    Release(ReleaseRequest, Reply<ReleaseResponse>),
    PutBatch(PutBatchRequest, Reply<PutBatchResponse>),
    Get(GetRequest, Reply<GetResponse>),
    List(ListRequest, Reply<ListResponse>),
    ReadAt(ReadAtRequest, Reply<ReadAtResponse>),
}

/// Owns one space's state machine and drains its mailbox. Constructed
/// together with its [`SpaceHandle`]; the owner decides where `run()`
/// executes (tokio task, sim stepper, test pool).
pub struct SpaceActor<S, C> {
    machine: Space,
    store: Arc<S>,
    clock: Arc<C>,
    inbox: mpsc::UnboundedReceiver<Command>,
}

impl<S: OrderedStore, C: Clock> SpaceActor<S, C> {
    /// A fresh actor over a (possibly shared) store and clock, plus the
    /// handle that reaches it. Clone the handle freely; drop all clones and
    /// `run()` returns.
    pub fn new(id: SpaceId, store: Arc<S>, clock: Arc<C>) -> (Self, SpaceHandle) {
        let (outbox, inbox) = mpsc::unbounded();
        let actor = Self {
            machine: Space::new(id),
            store,
            clock,
            inbox,
        };
        (actor, SpaceHandle { outbox })
    }

    /// Drains the mailbox until every handle is gone. One command at a
    /// time, `now` stamped at dequeue: the linearization loop.
    pub async fn run(mut self) {
        while let Some(cmd) = next_command(&mut self.inbox).await {
            let now = self.clock.now();
            let store = Arc::clone(&self.store);
            match cmd {
                Command::Acquire(req, reply) => {
                    let result = self.machine.acquire(&*store, now, &req).await;
                    let _ = reply.send(result.map_err(Into::into));
                }
                Command::Renew(req, reply) => {
                    let result = self.machine.renew(&*store, now, &req).await;
                    let _ = reply.send(result.map_err(Into::into));
                }
                Command::Release(req, reply) => {
                    let result = self.machine.release(&*store, now, &req).await;
                    let _ = reply.send(result.map_err(Into::into));
                }
                Command::PutBatch(req, reply) => {
                    let result = self.machine.put_batch(&*store, now, &req).await;
                    let _ = reply.send(result.map_err(Into::into));
                }
                Command::Get(req, reply) => {
                    let result = self.machine.get(&*store, &req).await;
                    let _ = reply.send(result.map_err(Into::into));
                }
                Command::List(req, reply) => {
                    let result = self.machine.list(&*store, &req).await;
                    let _ = reply.send(result.map_err(Into::into));
                }
                Command::ReadAt(req, reply) => {
                    let result = self.machine.read_at(&*store, &req).await;
                    let _ = reply.send(result.map_err(Into::into));
                }
            }
        }
    }
}

/// Awaits the next command without pulling in an executor or stream
/// combinators: `poll_fn` over the receiver's `Stream` impl.
async fn next_command(inbox: &mut mpsc::UnboundedReceiver<Command>) -> Option<Command> {
    std::future::poll_fn(|cx| Pin::new(&mut *inbox).poll_next(cx)).await
}

/// The client-facing side of one space actor. Cheap to clone; every clone
/// feeds the same mailbox, so all callers see one linearized space.
#[derive(Clone)]
pub struct SpaceHandle {
    outbox: mpsc::UnboundedSender<Command>,
}

impl SpaceHandle {
    async fn call<T>(&self, wrap: impl FnOnce(Reply<T>) -> Command) -> Result<T, SpaceError> {
        let (reply, response) = oneshot::channel();
        self.outbox
            .unbounded_send(wrap(reply))
            .map_err(|_| SpaceError::unavailable("space actor has shut down"))?;
        match response.await {
            Ok(result) => result,
            Err(_cancelled) => Err(SpaceError::unavailable("space actor dropped mid-request")),
        }
    }
}

impl SpaceApi for SpaceHandle {
    fn acquire(
        &self,
        req: AcquireRequest,
    ) -> impl Future<Output = Result<AcquireResponse, SpaceError>> + Send {
        self.call(move |reply| Command::Acquire(req, reply))
    }

    fn renew(
        &self,
        req: RenewRequest,
    ) -> impl Future<Output = Result<RenewResponse, SpaceError>> + Send {
        self.call(move |reply| Command::Renew(req, reply))
    }

    fn release(
        &self,
        req: ReleaseRequest,
    ) -> impl Future<Output = Result<ReleaseResponse, SpaceError>> + Send {
        self.call(move |reply| Command::Release(req, reply))
    }

    fn put_batch(
        &self,
        req: PutBatchRequest,
    ) -> impl Future<Output = Result<PutBatchResponse, SpaceError>> + Send {
        self.call(move |reply| Command::PutBatch(req, reply))
    }

    fn get(
        &self,
        req: GetRequest,
    ) -> impl Future<Output = Result<GetResponse, SpaceError>> + Send {
        self.call(move |reply| Command::Get(req, reply))
    }

    fn list(
        &self,
        req: ListRequest,
    ) -> impl Future<Output = Result<ListResponse, SpaceError>> + Send {
        self.call(move |reply| Command::List(req, reply))
    }

    fn read_at(
        &self,
        req: ReadAtRequest,
    ) -> impl Future<Output = Result<ReadAtResponse, SpaceError>> + Send {
        self.call(move |reply| Command::ReadAt(req, reply))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::MemoryStore;
    use futures::executor::LocalPool;
    use futures::task::LocalSpawnExt;
    use homebase_core::clock::{ManualClock, Timestamp};
    use homebase_core::key::Key;
    use homebase_core::lease::{LeaseMode, LeaseRef};
    use homebase_core::messages::{KernelError, LeaseSpec, PutEntry};
    use homebase_core::tag::{AdmissionSeq, DeviceId, DeviceSeq, Value, Ver};
    use std::time::Duration;

    const SPACE: SpaceId = SpaceId([9; 16]);

    fn dev(n: u8) -> DeviceId {
        DeviceId([n; 16])
    }

    fn key(components: &[&[u8]]) -> Key {
        Key::from_bytes(components.iter().copied()).unwrap()
    }

    fn acquire_req(device: u8, prefix: &Key, ttl_ms: u64) -> AcquireRequest {
        AcquireRequest {
            device: dev(device),
            steal: false,
            specs: vec![LeaseSpec {
                prefix: prefix.clone(),
                mode: LeaseMode::Write,
                ttl: Duration::from_millis(ttl_ms),
                stealable: false,
            }],
        }
    }

    fn put_req(device: u8, seq: u64, lease: LeaseRef, k: &Key, v: &[u8], ver: u64) -> PutBatchRequest {
        PutBatchRequest {
            device: dev(device),
            device_seq: DeviceSeq(seq),
            leases: vec![lease],
            entries: vec![PutEntry {
                key: k.clone(),
                value: Value::Present(v.to_vec()),
                ver: Ver(ver),
            }],
        }
    }

    /// Pool with a running actor plus its handle and clock.
    fn setup() -> (LocalPool, SpaceHandle, Arc<ManualClock>) {
        let pool = LocalPool::new();
        let clock = Arc::new(ManualClock::new(Timestamp(0)));
        let store = Arc::new(MemoryStore::new());
        let (actor, handle) = SpaceActor::new(SPACE, store, Arc::clone(&clock));
        pool.spawner().spawn_local(actor.run()).unwrap();
        (pool, handle, clock)
    }

    #[test]
    fn verbs_flow_through_the_handle() {
        let (mut pool, handle, _clock) = setup();
        pool.run_until(async move {
            let prefix = key(&[b"db"]);
            let granted = handle.acquire(acquire_req(1, &prefix, 1000)).await.unwrap();
            let lease = LeaseRef {
                id: granted.leases[0].id,
                epoch: granted.leases[0].epoch,
            };

            let k = key(&[b"db", b"row"]);
            let put = handle.put_batch(put_req(1, 1, lease, &k, b"v", 1)).await.unwrap();
            assert_eq!(put.admission_seq, AdmissionSeq(1));

            let got = handle.get(GetRequest { keys: vec![k.clone()] }).await.unwrap();
            let entry = got.entries[0].as_ref().unwrap();
            assert_eq!(entry.value, Value::Present(b"v".to_vec()));

            let listed = handle
                .list(ListRequest { prefix, start_after: None, limit: None })
                .await
                .unwrap();
            assert_eq!(listed.entries.len(), 1);
        });
    }

    #[test]
    fn now_is_stamped_from_the_actor_clock() {
        let (mut pool, handle, clock) = setup();
        pool.run_until(async move {
            let prefix = key(&[b"db"]);
            handle.acquire(acquire_req(1, &prefix, 100)).await.unwrap();

            // Still live: a second device contends.
            let denied = handle.acquire(acquire_req(2, &prefix, 100)).await;
            assert!(matches!(
                denied,
                Err(SpaceError::Kernel(KernelError::Contended { .. }))
            ));

            // Crank the shared clock past the deadline: the very next
            // dequeue sees the lease expired.
            clock.advance(Duration::from_millis(100));
            handle
                .acquire(acquire_req(2, &prefix, 100))
                .await
                .expect("expired lease must be re-grantable");
        });
    }

    #[test]
    fn cloned_handles_share_one_linearized_space() {
        let (mut pool, handle, _clock) = setup();
        let other = handle.clone();

        // Two clients interleave on one actor; admission seqs must come out
        // dense — the mailbox is the serialization point.
        let client = |handle: SpaceHandle, device: u8| async move {
            let prefix = key(&[format!("d{device}").as_bytes()]);
            let granted = handle.acquire(acquire_req(device, &prefix, 1000)).await.unwrap();
            let lease = LeaseRef {
                id: granted.leases[0].id,
                epoch: granted.leases[0].epoch,
            };
            let mut seqs = Vec::new();
            for i in 1..=5u64 {
                let k = key(&[format!("d{device}").as_bytes(), format!("k{i}").as_bytes()]);
                let resp = handle
                    .put_batch(put_req(device, i, lease, &k, b"v", 1))
                    .await
                    .unwrap();
                seqs.push(resp.admission_seq.0);
            }
            seqs
        };

        let (a, b) = pool.run_until(futures::future::join(client(handle, 1), client(other, 2)));
        let mut all: Vec<u64> = a.into_iter().chain(b).collect();
        all.sort_unstable();
        assert_eq!(all, (1..=10).collect::<Vec<u64>>(), "dense, no gaps, no duplicates");
    }

    #[test]
    fn dead_actor_reports_unavailable() {
        let (actor, handle) = SpaceActor::new(
            SPACE,
            Arc::new(MemoryStore::new()),
            Arc::new(ManualClock::new(Timestamp(0))),
        );
        drop(actor);

        let result = pollster::block_on(handle.get(GetRequest { keys: vec![] }));
        assert!(matches!(result, Err(SpaceError::Unavailable { .. })));
    }

    /// The run future must stay `Send`: production spawns it on tokio, and
    /// this assertion is the cheapest way to keep that true forever.
    #[test]
    fn actor_run_future_is_send() {
        fn assert_send<T: Send>(_: &T) {}
        let (actor, _handle) = SpaceActor::new(
            SPACE,
            Arc::new(MemoryStore::new()),
            Arc::new(ManualClock::new(Timestamp(0))),
        );
        assert_send(&actor.run());
    }
}
