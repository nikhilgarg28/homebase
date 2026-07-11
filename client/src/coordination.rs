//! Runtime-independent client coordination loop.
//!
//! The loop owns fast session state and the right to run one workflow per
//! space. It never awaits storage, crypto, network, or timers: callers hold
//! a [`SpacePermit`] while doing that work in their own task or worker and
//! return to the loop only for short state transitions. One OS thread keeps
//! this boundary independent of whichever async executor embeds the SDK.

use futures_channel::oneshot;
use homebase_core::space::SpaceId;
use std::collections::{BTreeMap, VecDeque};
use std::fmt;
use std::sync::{Arc, Mutex, mpsc};

type Waiter = oneshot::Sender<()>;

trait SessionJob<S>: Send {
    fn run(self: Box<Self>, state: &mut S);
}

struct Call<S, F, R> {
    call: Option<F>,
    reply: mpsc::SyncSender<R>,
    _state: std::marker::PhantomData<fn(&mut S)>,
}

impl<S, F, R> SessionJob<S> for Call<S, F, R>
where
    F: FnOnce(&mut S) -> R + Send + 'static,
    R: Send + 'static,
{
    fn run(mut self: Box<Self>, state: &mut S) {
        let result = self.call.take().expect("session call runs once")(state);
        let _ = self.reply.send(result);
    }
}

enum Command<S> {
    Call(Box<dyn SessionJob<S>>),
    Enter(SpaceId, Waiter),
    Leave(SpaceId),
}

/// Sending side of one single-owner coordination loop.
pub(crate) struct Coordinator<S> {
    outbox: mpsc::Sender<Command<S>>,
}

impl<S: Send + 'static> Coordinator<S> {
    pub(crate) fn new(state: S) -> Result<Self, CoordinationError> {
        let (outbox, inbox) = mpsc::channel();
        std::thread::Builder::new()
            .name("homebase-client-coordinator".into())
            .spawn(move || run(state, inbox))
            .map_err(|error| CoordinationError(error.to_string()))?;
        Ok(Self { outbox })
    }

    /// Runs a short session-state transition on the loop and waits for its
    /// value. Calls must not perform IO, crypto, sleeps, or await work.
    pub(crate) fn call<F, R>(&self, call: F) -> R
    where
        F: FnOnce(&mut S) -> R + Send + 'static,
        R: Send + 'static,
    {
        let (reply, response) = mpsc::sync_channel(1);
        self.outbox
            .send(Command::Call(Box::new(Call {
                call: Some(call),
                reply,
                _state: std::marker::PhantomData,
            })))
            .expect("client coordination loop must outlive its handle");
        response
            .recv()
            .expect("client coordination loop dropped a session reply")
    }

    /// Waits for this space's FIFO workflow grant. Different spaces may
    /// hold grants concurrently; the loop remains free while work runs.
    pub(crate) async fn enter(&self, space: SpaceId) -> Result<SpacePermit<S>, CoordinationError> {
        let (reply, granted) = oneshot::channel();
        self.outbox
            .send(Command::Enter(space, reply))
            .map_err(|_| CoordinationError("client coordination loop has shut down".into()))?;
        granted
            .await
            .map_err(|_| CoordinationError("client coordination grant was cancelled".into()))?;
        Ok(SpacePermit {
            space,
            outbox: Some(self.outbox.clone()),
        })
    }
}

/// Exclusive right to perform one space's coordination workflow. Dropping
/// it always releases the next waiter, including error and cancellation paths.
pub(crate) struct SpacePermit<S> {
    space: SpaceId,
    outbox: Option<mpsc::Sender<Command<S>>>,
}

impl<S> Drop for SpacePermit<S> {
    fn drop(&mut self) {
        if let Some(outbox) = self.outbox.take() {
            let _ = outbox.send(Command::Leave(self.space));
        }
    }
}

fn run<S>(mut state: S, inbox: mpsc::Receiver<Command<S>>) {
    let mut active = BTreeMap::<SpaceId, VecDeque<Waiter>>::new();
    while let Ok(command) = inbox.recv() {
        match command {
            Command::Call(call) => call.run(&mut state),
            Command::Enter(space, waiter) => match active.get_mut(&space) {
                Some(waiters) => waiters.push_back(waiter),
                None => {
                    if waiter.send(()).is_ok() {
                        active.insert(space, VecDeque::new());
                    }
                }
            },
            Command::Leave(space) => grant_next(&mut active, space),
        }
    }
}

fn grant_next(active: &mut BTreeMap<SpaceId, VecDeque<Waiter>>, space: SpaceId) {
    let Some(mut waiters) = active.remove(&space) else {
        return;
    };
    while let Some(waiter) = waiters.pop_front() {
        if waiter.send(()).is_ok() {
            active.insert(space, waiters);
            return;
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct CoordinationError(pub(crate) String);

impl fmt::Display for CoordinationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "client coordination error: {}", self.0)
    }
}

impl std::error::Error for CoordinationError {}

trait BlockingJob: Send {
    fn run(self: Box<Self>);
}

struct Work<F, R> {
    work: Option<F>,
    reply: oneshot::Sender<R>,
}

impl<F, R> BlockingJob for Work<F, R>
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    fn run(mut self: Box<Self>) {
        let result = self.work.take().expect("blocking work runs once")();
        let _ = self.reply.send(result);
    }
}

/// Small runtime-independent pool for SQLite adapters and bulk crypto.
/// Results return through oneshots; applying them remains the caller's
/// coordinated responsibility.
pub(crate) struct BlockingPool {
    outbox: mpsc::Sender<Box<dyn BlockingJob>>,
}

impl BlockingPool {
    pub(crate) fn new(threads: usize) -> Result<Self, CoordinationError> {
        assert!(threads > 0, "blocking pool needs at least one worker");
        let (outbox, inbox) = mpsc::channel::<Box<dyn BlockingJob>>();
        let inbox = Arc::new(Mutex::new(inbox));
        for index in 0..threads {
            let inbox = Arc::clone(&inbox);
            std::thread::Builder::new()
                .name(format!("homebase-client-worker-{index}"))
                .spawn(move || {
                    loop {
                        let job = inbox.lock().expect("worker inbox poisoned").recv();
                        match job {
                            Ok(job) => job.run(),
                            Err(_) => return,
                        }
                    }
                })
                .map_err(|error| CoordinationError(error.to_string()))?;
        }
        Ok(Self { outbox })
    }

    pub(crate) async fn run<F, R>(&self, work: F) -> Result<R, CoordinationError>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        let (reply, result) = oneshot::channel();
        self.outbox
            .send(Box::new(Work {
                work: Some(work),
                reply,
            }))
            .map_err(|_| CoordinationError("client blocking pool has shut down".into()))?;
        result
            .await
            .map_err(|_| CoordinationError("client blocking worker dropped its result".into()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pollster::block_on;
    use std::sync::Arc;

    const A: SpaceId = SpaceId([1; 16]);
    const B: SpaceId = SpaceId([2; 16]);

    #[test]
    fn all_session_transitions_run_on_one_owner_thread() {
        let coordinator = Arc::new(Coordinator::new(Vec::new()).unwrap());
        let callers: Vec<_> = (0..8)
            .map(|value| {
                let coordinator = Arc::clone(&coordinator);
                std::thread::spawn(move || {
                    coordinator.call(move |seen| {
                        seen.push(value);
                        std::thread::current().id()
                    })
                })
            })
            .collect();
        let owners: Vec<_> = callers
            .into_iter()
            .map(|join| join.join().unwrap())
            .collect();
        assert!(owners.windows(2).all(|pair| pair[0] == pair[1]));
        assert_eq!(coordinator.call(|seen| seen.len()), 8);
    }

    #[test]
    fn same_space_waiters_are_fifo_but_other_spaces_progress() {
        let coordinator = Coordinator::new(()).unwrap();
        let first = block_on(coordinator.enter(A)).unwrap();
        let (second_reply, second) = oneshot::channel();
        let (third_reply, third) = oneshot::channel();
        coordinator
            .outbox
            .send(Command::Enter(A, second_reply))
            .unwrap();
        coordinator
            .outbox
            .send(Command::Enter(A, third_reply))
            .unwrap();

        let other = block_on(coordinator.enter(B)).unwrap();
        drop(other);
        drop(first);
        block_on(second).unwrap();
        coordinator.outbox.send(Command::Leave(A)).unwrap();
        block_on(third).unwrap();
        coordinator.outbox.send(Command::Leave(A)).unwrap();
    }

    #[test]
    fn cancelled_waiter_does_not_strand_the_space() {
        let coordinator = Coordinator::new(()).unwrap();
        let first = block_on(coordinator.enter(A)).unwrap();

        let (reply, cancelled) = oneshot::channel();
        coordinator.outbox.send(Command::Enter(A, reply)).unwrap();
        drop(cancelled);
        drop(first);

        let next = block_on(coordinator.enter(A)).unwrap();
        drop(next);
    }

    #[test]
    fn blocking_work_runs_off_loop_and_returns_by_channel() {
        let coordinator = Coordinator::new(()).unwrap();
        let owner = coordinator.call(|_| std::thread::current().id());
        let workers = BlockingPool::new(2).unwrap();
        let worker = block_on(workers.run(|| std::thread::current().id())).unwrap();
        assert_ne!(owner, worker);
        assert_eq!(coordinator.call(|_| 42), 42, "loop remains responsive");
    }

    #[test]
    fn out_of_order_worker_results_return_to_the_right_call() {
        let workers = Arc::new(BlockingPool::new(2).unwrap());
        let (started_tx, started_rx) = mpsc::sync_channel(1);
        let (release_tx, release_rx) = mpsc::sync_channel(1);
        let slow_workers = Arc::clone(&workers);
        let slow = std::thread::spawn(move || {
            block_on(slow_workers.run(move || {
                started_tx.send(()).unwrap();
                release_rx.recv().unwrap();
                "slow"
            }))
            .unwrap()
        });

        started_rx.recv().unwrap();
        let fast = block_on(workers.run(|| "fast")).unwrap();
        release_tx.send(()).unwrap();

        assert_eq!(fast, "fast");
        assert_eq!(slow.join().unwrap(), "slow");
    }
}
