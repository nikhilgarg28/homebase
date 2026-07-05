//! The seeded single-threaded stepper.
//!
//! A task is a `Future<Output = ()>` plus a woken flag. Each step collects
//! every woken live task, asks the seeded RNG which to poll, and polls it
//! exactly once. Wakes happen synchronously during polls (channels wake
//! their peers inline), so the whole schedule is a deterministic function
//! of the seed — and *only* the seed.
//!
//! [`cancel`](SimExecutor::cancel) models a process death: the future is
//! dropped on the spot, mid-await state and all. Kernel futures mutate the
//! store at a single synchronous point per operation, so cancellation
//! never tears a write — exactly the contract a real crash has.

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Wake, Waker};

/// Identifies a spawned task for [`SimExecutor::cancel`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TaskId(usize);

struct TaskState {
    woken: AtomicBool,
}

impl Wake for TaskState {
    fn wake(self: Arc<Self>) {
        self.woken.store(true, Ordering::SeqCst);
    }
}

struct Task {
    future: Pin<Box<dyn Future<Output = ()>>>,
    state: Arc<TaskState>,
}

/// The deterministic executor. Not `Send`, not fair, not fast — seeded.
pub struct SimExecutor {
    tasks: Vec<Option<Task>>,
    rng: StdRng,
}

impl SimExecutor {
    pub fn new(seed: u64) -> Self {
        Self {
            tasks: Vec::new(),
            rng: StdRng::seed_from_u64(seed),
        }
    }

    /// Adds a task; it starts woken. `Send` is not required — the sim is
    /// single-threaded by definition.
    pub fn spawn(&mut self, future: impl Future<Output = ()> + 'static) -> TaskId {
        let task = Task {
            future: Box::pin(future),
            state: Arc::new(TaskState {
                woken: AtomicBool::new(true),
            }),
        };
        self.tasks.push(Some(task));
        TaskId(self.tasks.len() - 1)
    }

    /// Drops the task's future immediately — the sim's `kill -9`. No-op if
    /// already finished or cancelled.
    pub fn cancel(&mut self, id: TaskId) {
        self.tasks[id.0] = None;
    }

    /// Polls one runnable task, chosen by the seed. Returns `false` when no
    /// task is runnable (all finished, cancelled, or waiting on a wake).
    pub fn step(&mut self) -> bool {
        let runnable: Vec<usize> = self
            .tasks
            .iter()
            .enumerate()
            .filter_map(|(i, slot)| {
                let task = slot.as_ref()?;
                task.state.woken.load(Ordering::SeqCst).then_some(i)
            })
            .collect();
        let Some(&index) = runnable.get(self.rng.random_range(0..runnable.len().max(1))) else {
            return false;
        };

        let task = self.tasks[index].as_mut().unwrap();
        task.state.woken.store(false, Ordering::SeqCst);
        let waker = Waker::from(Arc::clone(&task.state));
        let mut cx = Context::from_waker(&waker);
        if task.future.as_mut().poll(&mut cx).is_ready() {
            self.tasks[index] = None;
        }
        true
    }

    /// Steps until nothing is runnable.
    pub fn run_until_stalled(&mut self) {
        while self.step() {}
    }

    /// Whether any spawned task has not finished or been cancelled.
    pub fn has_live_tasks(&self) -> bool {
        self.tasks.iter().any(|slot| slot.is_some())
    }

    /// Like [`run_until_stalled`], but yields to tokio between steps so
    /// real async IO (slatedb) can complete while preserving seeded
    /// interleaving among sim tasks.
    #[cfg(feature = "slatedb")]
    pub async fn run_until_stalled_async(&mut self) {
        let mut idle = 0u32;
        loop {
            if self.step() {
                idle = 0;
                continue;
            }
            if !self.has_live_tasks() {
                break;
            }
            tokio::task::yield_now().await;
            idle += 1;
            assert!(
                idle < 10_000,
                "sim executor stuck waiting on IO (>10k yields)"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;
    use std::rc::Rc;
    use std::task::Poll;

    /// Yields once (self-waking), so a task stays interleavable.
    async fn yield_once() {
        let mut yielded = false;
        std::future::poll_fn(move |cx| {
            if yielded {
                Poll::Ready(())
            } else {
                yielded = true;
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        })
        .await
    }

    #[test]
    fn interleaving_is_seeded_and_reproducible() {
        let trace = |seed: u64| {
            let mut exec = SimExecutor::new(seed);
            let log = Rc::new(std::cell::RefCell::new(Vec::new()));
            for id in 0..3u8 {
                let log = Rc::clone(&log);
                exec.spawn(async move {
                    for round in 0..3u8 {
                        log.borrow_mut().push((id, round));
                        yield_once().await;
                    }
                });
            }
            exec.run_until_stalled();
            log.borrow().clone()
        };

        let a = trace(42);
        assert_eq!(a, trace(42), "same seed, same schedule");
        assert_ne!(a, trace(43), "different seed, different schedule");
        assert_eq!(a.len(), 9, "every task ran to completion");
    }

    #[test]
    fn cancel_drops_a_task_mid_flight() {
        let mut exec = SimExecutor::new(0);
        let progressed = Rc::new(Cell::new(0u32));
        let p = Rc::clone(&progressed);
        let id = exec.spawn(async move {
            loop {
                p.set(p.get() + 1);
                yield_once().await;
            }
        });

        exec.step();
        exec.step();
        let before = progressed.get();
        assert!(before > 0);
        exec.cancel(id);
        exec.run_until_stalled();
        assert_eq!(progressed.get(), before, "cancelled task never runs again");
    }

    #[test]
    fn stalls_when_no_task_is_woken() {
        let mut exec = SimExecutor::new(0);
        exec.spawn(std::future::pending::<()>());
        exec.step(); // polls once (starts woken), parks forever
        assert!(!exec.step(), "pending task without wake is not runnable");
    }
}
