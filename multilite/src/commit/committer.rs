//! FIFO serialization for canonical SQLite commits and authority workflows.

use std::collections::BTreeMap;
use std::fmt;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;
use std::sync::mpsc;

use async_channel::{Receiver, Sender};
use futures_channel::oneshot;
use parking_lot::Mutex;

use crate::commit::snapshot::CommitSeq;

const COMMAND_CAPACITY: usize = 64;

/// In-process writable branches that still need OCC history after their cut.
#[derive(Clone, Default)]
pub struct HistoryPins {
    inner: Arc<Mutex<BTreeMap<CommitSeq, usize>>>,
}

impl HistoryPins {
    pub fn register(&self, commit_seq: CommitSeq) -> HistoryPin {
        *self.inner.lock().entry(commit_seq).or_default() += 1;
        HistoryPin {
            commit_seq,
            pins: self.clone(),
        }
    }

    pub fn oldest(&self) -> Option<CommitSeq> {
        self.inner.lock().first_key_value().map(|(seq, _)| *seq)
    }
}

/// Registration held until one writable branch has received its disposition.
pub struct HistoryPin {
    commit_seq: CommitSeq,
    pins: HistoryPins,
}

impl Drop for HistoryPin {
    fn drop(&mut self) {
        let mut pins = self.pins.inner.lock();
        let count = pins
            .get_mut(&self.commit_seq)
            .expect("history pin remains registered");
        *count -= 1;
        if *count == 0 {
            pins.remove(&self.commit_seq);
        }
    }
}

trait ActorJob: Send {
    fn run(self: Box<Self>);
}

struct Work<F, R> {
    operation: Option<F>,
    reply: oneshot::Sender<std::result::Result<R, CommitterError>>,
}

impl<F, R> ActorJob for Work<F, R>
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    fn run(mut self: Box<Self>) {
        let operation = self.operation.take().expect("actor work runs once");
        let result = catch_unwind(AssertUnwindSafe(operation))
            .map_err(|_| CommitterError::OperationPanicked);
        let _ = self.reply.send(result);
    }
}

enum Command {
    Run(Box<dyn ActorJob>),
    Enter(oneshot::Sender<mpsc::SyncSender<()>>),
}

/// Sending side of one bounded serial database executor.
#[derive(Clone)]
pub struct Committer {
    outbox: Sender<Command>,
}

impl Committer {
    pub fn new() -> std::result::Result<Self, CommitterError> {
        Self::with_capacity(COMMAND_CAPACITY)
    }

    fn with_capacity(capacity: usize) -> std::result::Result<Self, CommitterError> {
        let (outbox, inbox) = async_channel::bounded(capacity);
        std::thread::Builder::new()
            .name("multilite-database".into())
            .spawn(move || run(inbox))
            .map_err(|error| CommitterError::Startup(error.to_string()))?;
        Ok(Self { outbox })
    }

    /// Run owned work in FIFO order on the database executor.
    ///
    /// Once accepted by the channel, work runs even if the response future is
    /// dropped. This prevents caller cancellation from retracting a commit.
    pub async fn call<F, R>(&self, operation: F) -> std::result::Result<R, CommitterError>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        let (reply, response) = oneshot::channel();
        self.outbox
            .send(Command::Run(Box::new(Work {
                operation: Some(operation),
                reply,
            })))
            .await
            .map_err(|_| CommitterError::Unavailable)?;
        response.await.map_err(|_| CommitterError::Unavailable)?
    }

    pub fn call_blocking<F, R>(&self, operation: F) -> std::result::Result<R, CommitterError>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        pollster::block_on(self.call(operation))
    }

    /// Acquire the FIFO execution turn for work that borrows caller state.
    ///
    /// The work remains on the caller thread, but every actor job and every
    /// other borrowed scope waits until this permit is dropped.
    pub async fn enter(&self) -> std::result::Result<CommitPermit, CommitterError> {
        let (reply, granted) = oneshot::channel();
        self.outbox
            .send(Command::Enter(reply))
            .await
            .map_err(|_| CommitterError::Unavailable)?;
        let release = granted.await.map_err(|_| CommitterError::Unavailable)?;
        Ok(CommitPermit {
            release: Some(release),
        })
    }

    pub fn enter_blocking(&self) -> std::result::Result<CommitPermit, CommitterError> {
        pollster::block_on(self.enter())
    }
}

/// Exclusive execution turn for one borrowed database workflow.
pub struct CommitPermit {
    release: Option<mpsc::SyncSender<()>>,
}

impl Drop for CommitPermit {
    fn drop(&mut self) {
        if let Some(release) = self.release.take() {
            let _ = release.send(());
        }
    }
}

fn run(inbox: Receiver<Command>) {
    while let Ok(command) = inbox.recv_blocking() {
        match command {
            Command::Run(job) => job.run(),
            Command::Enter(reply) => {
                let (release, released) = mpsc::sync_channel(1);
                if reply.send(release).is_ok() {
                    let _ = released.recv();
                }
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CommitterError {
    Startup(String),
    Unavailable,
    OperationPanicked,
}

impl fmt::Display for CommitterError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Startup(message) => write!(f, "could not start committer: {message}"),
            Self::Unavailable => f.write_str("committer is unavailable"),
            Self::OperationPanicked => f.write_str("committer operation panicked"),
        }
    }
}

impl std::error::Error for CommitterError {}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::mpsc;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use super::*;

    #[test]
    fn history_frontier_tracks_the_oldest_live_commit_sequence() {
        let pins = HistoryPins::default();
        let later = pins.register(CommitSeq(9));
        let first = pins.register(CommitSeq(3));
        let second = pins.register(CommitSeq(3));
        assert_eq!(pins.oldest(), Some(CommitSeq(3)));
        drop(first);
        assert_eq!(pins.oldest(), Some(CommitSeq(3)));
        drop(second);
        assert_eq!(pins.oldest(), Some(CommitSeq(9)));
        drop(later);
        assert_eq!(pins.oldest(), None);
    }

    #[test]
    fn borrowed_permit_blocks_owned_work_until_drop() {
        let actor = Committer::new().unwrap();
        let permit = actor.enter_blocking().unwrap();
        let (finished, completion) = mpsc::channel();
        let worker = actor.clone();
        let join = std::thread::spawn(move || {
            worker
                .call_blocking(move || finished.send(7).unwrap())
                .unwrap();
        });

        assert!(completion.recv_timeout(Duration::from_millis(20)).is_err());
        drop(permit);
        assert_eq!(completion.recv_timeout(Duration::from_secs(1)).unwrap(), 7);
        join.join().unwrap();
    }

    #[test]
    fn cancelled_waiter_does_not_strand_following_work() {
        let actor = Committer::new().unwrap();
        let permit = actor.enter_blocking().unwrap();
        let (cancelled_reply, cancelled) = oneshot::channel();
        actor
            .outbox
            .send_blocking(Command::Enter(cancelled_reply))
            .unwrap();
        drop(cancelled);

        let next = actor.clone();
        let join = std::thread::spawn(move || {
            let permit = next.enter_blocking().unwrap();
            drop(permit);
        });
        drop(permit);
        join.join().unwrap();
    }

    #[test]
    fn active_borrowed_scope_preserves_bounded_backpressure() {
        let actor = Committer::with_capacity(1).unwrap();
        let permit = actor.enter_blocking().unwrap();
        let (reply, _response) = oneshot::channel();
        actor
            .outbox
            .try_send(Command::Run(Box::new(Work {
                operation: Some(|| ()),
                reply,
            })))
            .unwrap();
        let (reply, _response) = oneshot::channel();
        assert!(matches!(
            actor.outbox.try_send(Command::Run(Box::new(Work {
                operation: Some(|| ()),
                reply,
            }))),
            Err(async_channel::TrySendError::Full(_))
        ));
        drop(permit);
    }

    #[test]
    fn owned_jobs_keep_fifo_channel_order() {
        let actor = Committer::new().unwrap();
        let permit = actor.enter_blocking().unwrap();
        let order = Arc::new(Mutex::new(Vec::new()));
        let mut responses = Vec::new();
        for value in [1, 2] {
            let order = Arc::clone(&order);
            let (reply, response) = oneshot::channel();
            actor
                .outbox
                .send_blocking(Command::Run(Box::new(Work {
                    operation: Some(move || order.lock().unwrap().push(value)),
                    reply,
                })))
                .unwrap();
            responses.push(response);
        }

        drop(permit);
        for response in responses {
            pollster::block_on(response).unwrap().unwrap();
        }
        assert_eq!(*order.lock().unwrap(), [1, 2]);
    }

    #[test]
    fn dropped_reply_does_not_cancel_accepted_work() {
        let actor = Committer::new().unwrap();
        let completed = Arc::new(AtomicUsize::new(0));
        let counted = Arc::clone(&completed);
        let (reply, response) = oneshot::channel();
        actor
            .outbox
            .send_blocking(Command::Run(Box::new(Work {
                operation: Some(move || counted.store(1, Ordering::Release)),
                reply,
            })))
            .unwrap();
        drop(response);

        actor.call_blocking(|| ()).unwrap();
        assert_eq!(completed.load(Ordering::Acquire), 1);
    }

    #[test]
    fn panicking_job_does_not_kill_the_actor() {
        let actor = Committer::new().unwrap();
        assert_eq!(
            actor.call_blocking(|| panic!("injected")),
            Err(CommitterError::OperationPanicked)
        );
        assert_eq!(actor.call_blocking(|| 9).unwrap(), 9);
    }
}
