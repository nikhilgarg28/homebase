//! Runtime-neutral bounded execution for filesystem and SQLite work.

use std::cell::Cell;
use std::panic::{AssertUnwindSafe, catch_unwind, resume_unwind};
use std::sync::OnceLock;

use async_channel::{Receiver, Sender};
use futures_channel::oneshot;

use crate::{Error, Result};

const INBOX_CAPACITY: usize = 256;
const MAX_WORKERS: usize = 16;

type Job = Box<dyn FnOnce() + Send + 'static>;

#[derive(Clone)]
struct BlockingPool {
    outbox: Sender<Job>,
}

thread_local! {
    static ON_BLOCKING_WORKER: Cell<bool> = const { Cell::new(false) };
}

static POOL: OnceLock<std::result::Result<BlockingPool, String>> = OnceLock::new();

/// Run one owned blocking operation without occupying the caller's executor.
///
/// Once accepted by the bounded queue, the operation completes even if its
/// returned future is dropped.
pub async fn run<T>(operation: impl FnOnce() -> Result<T> + Send + 'static) -> Result<T>
where
    T: Send + 'static,
{
    let pool = pool()?;
    run_on(pool, operation).await
}

async fn run_on<T>(
    pool: BlockingPool,
    operation: impl FnOnce() -> Result<T> + Send + 'static,
) -> Result<T>
where
    T: Send + 'static,
{
    if ON_BLOCKING_WORKER.get() {
        return operation();
    }
    let (reply, response) = oneshot::channel();
    pool.outbox
        .send(Box::new(move || {
            let outcome = catch_unwind(AssertUnwindSafe(operation));
            let _ = reply.send(outcome);
        }))
        .await
        .map_err(|_| unavailable())?;
    match response.await.map_err(|_| unavailable())? {
        Ok(result) => result,
        Err(panic) => resume_unwind(panic),
    }
}

fn pool() -> Result<BlockingPool> {
    POOL.get_or_init(BlockingPool::start)
        .as_ref()
        .cloned()
        .map_err(|message| Error::BackgroundWorker(message.clone()))
}

impl BlockingPool {
    fn start() -> std::result::Result<Self, String> {
        let workers = std::thread::available_parallelism()
            .map(usize::from)
            .unwrap_or(1)
            .clamp(1, MAX_WORKERS);
        Self::start_with_workers(workers)
    }

    fn start_with_workers(workers: usize) -> std::result::Result<Self, String> {
        let (outbox, inbox) = async_channel::bounded(INBOX_CAPACITY);
        for index in 0..workers {
            let inbox = inbox.clone();
            std::thread::Builder::new()
                .name(format!("multilite-blocking-{index}"))
                .spawn(move || worker(inbox))
                .map_err(|error| format!("could not start blocking worker: {error}"))?;
        }
        Ok(Self { outbox })
    }
}

fn worker(inbox: Receiver<Job>) {
    ON_BLOCKING_WORKER.set(true);
    while let Ok(job) = inbox.recv_blocking() {
        job();
    }
}

fn unavailable() -> Error {
    Error::BackgroundWorker("blocking executor is unavailable".into())
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::sync::{Arc, Barrier, mpsc};
    use std::task::{Context, Poll, Waker};
    use std::time::Duration;

    use super::*;

    #[test]
    fn jobs_run_off_thread_and_return_owned_results() {
        let caller = std::thread::current().id();
        let worker = pollster::block_on(run(move || Ok(std::thread::current().id()))).unwrap();
        assert_ne!(worker, caller);
    }

    #[test]
    fn accepted_jobs_finish_after_their_future_is_dropped() {
        let (started, observed_start) = mpsc::channel();
        let (release, wait_for_release) = mpsc::channel();
        let (finished, observed_finish) = mpsc::channel();
        let future = run(move || {
            started.send(()).unwrap();
            wait_for_release.recv().unwrap();
            finished.send(()).unwrap();
            Ok(())
        });
        let mut future = Box::pin(future);
        let mut context = Context::from_waker(Waker::noop());
        assert!(matches!(future.as_mut().poll(&mut context), Poll::Pending));
        observed_start.recv_timeout(Duration::from_secs(1)).unwrap();

        drop(future);
        release.send(()).unwrap();
        observed_finish
            .recv_timeout(Duration::from_secs(1))
            .unwrap();
    }

    #[test]
    fn panics_propagate_without_stopping_workers() {
        assert!(
            std::panic::catch_unwind(|| {
                pollster::block_on(run::<()>(|| panic!("injected blocking panic"))).unwrap()
            })
            .is_err()
        );
        assert_eq!(pollster::block_on(run(|| Ok(7))).unwrap(), 7);
    }

    #[test]
    fn saturated_workers_execute_nested_jobs_inline() {
        const WORKERS: usize = 4;

        let pool = BlockingPool::start_with_workers(WORKERS).unwrap();
        let barrier = Arc::new(Barrier::new(WORKERS));
        let (finished, completions) = mpsc::channel();
        let mut callers = Vec::new();
        for value in 0..WORKERS {
            let pool = pool.clone();
            let nested_pool = pool.clone();
            let barrier = Arc::clone(&barrier);
            let finished = finished.clone();
            callers.push(std::thread::spawn(move || {
                let result = pollster::block_on(run_on(pool, move || {
                    barrier.wait();
                    pollster::block_on(run_on(nested_pool, move || Ok(value)))
                }));
                finished.send(result).unwrap();
            }));
        }
        drop(finished);

        let mut values = (0..WORKERS)
            .map(|_| {
                completions
                    .recv_timeout(Duration::from_secs(1))
                    .expect("nested blocking work deadlocked")
                    .unwrap()
            })
            .collect::<Vec<_>>();
        values.sort_unstable();
        assert_eq!(values, (0..WORKERS).collect::<Vec<_>>());
        for caller in callers {
            caller.join().unwrap();
        }
    }
}
