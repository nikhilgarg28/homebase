//! Open-time synchronization intent plus delayed push scheduling.

use std::sync::mpsc::{self, Receiver, RecvTimeoutError, Sender};
use std::sync::{Mutex, MutexGuard, Weak};
use std::time::{Duration, Instant};

use homebase_client::ServerHandle;

use super::Database;
use crate::{Error, Result};

/// How locally materialized SQLite state interacts with authority.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum SyncPolicy {
    /// Buffer writes durably and serve reads without contacting authority.
    #[default]
    LocalOnly,
    /// Buffer writes locally, schedule pushes, and refresh stale reads on demand.
    LocalFirst {
        /// Maximum time before a buffered write is scheduled for push.
        write_delay: Duration,
        /// Maximum age of the authority state served by a read.
        read_staleness: Duration,
    },
    /// Admit writes before returning and observe authority before every read.
    Remote,
}

pub struct PolicyState {
    policy: SyncPolicy,
    last_pull: Mutex<Option<Instant>>,
    last_refresh: Mutex<Option<Instant>>,
}

impl PolicyState {
    pub fn new(policy: SyncPolicy) -> Self {
        Self {
            policy,
            last_pull: Mutex::new(None),
            last_refresh: Mutex::new(None),
        }
    }

    pub fn policy(&self) -> SyncPolicy {
        self.policy
    }

    pub fn write_delay(&self) -> Option<Duration> {
        match self.policy {
            SyncPolicy::LocalFirst { write_delay, .. } => Some(write_delay),
            SyncPolicy::LocalOnly | SyncPolicy::Remote => None,
        }
    }

    pub fn read_requires_refresh(&self) -> bool {
        match self.policy {
            SyncPolicy::LocalOnly => false,
            SyncPolicy::Remote => true,
            SyncPolicy::LocalFirst { read_staleness, .. } => {
                read_staleness.is_zero()
                    || lock(&self.last_refresh)
                        .is_none_or(|refreshed| refreshed.elapsed() > read_staleness)
            }
        }
    }

    pub fn mark_pulled(&self) {
        *lock(&self.last_pull) = Some(Instant::now());
    }

    pub fn mark_rebased(&self) {
        if let Some(pulled) = lock(&self.last_pull).take() {
            *lock(&self.last_refresh) = Some(pulled);
        }
    }
}

pub struct PushScheduler {
    sender: Sender<SchedulerCommand>,
    receiver: Mutex<Option<Receiver<SchedulerCommand>>>,
}

enum SchedulerCommand {
    Schedule(Instant),
    Stop,
}

impl PushScheduler {
    pub fn new() -> Self {
        let (sender, receiver) = mpsc::channel();
        Self {
            sender,
            receiver: Mutex::new(Some(receiver)),
        }
    }

    pub fn start<H>(&self, database: Weak<Database<H>>) -> Result<()>
    where
        H: ServerHandle + Send + Sync + 'static,
    {
        let Some(receiver) = lock(&self.receiver).take() else {
            return Ok(());
        };
        std::thread::Builder::new()
            .name("multilite-push".into())
            .spawn(move || run_scheduler(receiver, database))
            .map_err(|error| Error::BackgroundWorker(error.to_string()))?;
        Ok(())
    }

    pub fn schedule(&self, delay: Duration) {
        let now = Instant::now();
        let deadline = now.checked_add(delay).unwrap_or(now);
        let _ = self.sender.send(SchedulerCommand::Schedule(deadline));
    }
}

impl Drop for PushScheduler {
    fn drop(&mut self) {
        let _ = self.sender.send(SchedulerCommand::Stop);
    }
}

fn run_scheduler<H>(receiver: Receiver<SchedulerCommand>, database: Weak<Database<H>>)
where
    H: ServerHandle + Send + Sync + 'static,
{
    let mut deadline: Option<Instant> = None;
    loop {
        let received = match deadline {
            Some(at) if at <= Instant::now() => Err(RecvTimeoutError::Timeout),
            Some(at) => receiver.recv_timeout(at.saturating_duration_since(Instant::now())),
            None => receiver.recv().map_err(|_| RecvTimeoutError::Disconnected),
        };
        match received {
            Ok(SchedulerCommand::Schedule(candidate)) => {
                deadline = Some(deadline.map_or(candidate, |current| current.min(candidate)));
            }
            Ok(SchedulerCommand::Stop) | Err(RecvTimeoutError::Disconnected) => return,
            Err(RecvTimeoutError::Timeout) => {
                deadline = None;
                let Some(database) = database.upgrade() else {
                    return;
                };
                // A rejection remains in the submit log so explicit
                // push/rollback can surface and repair it; transient failures
                // are retried by later work.
                let _ = database.push();
            }
        }
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}
