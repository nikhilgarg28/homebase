//! Open-time synchronization intent and its single-owner policy actor.

use std::collections::VecDeque;
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, Sender};
use std::sync::{Mutex, MutexGuard, Weak};
use std::time::{Duration, Instant};

use futures_channel::oneshot;
use homebase_client::ServerHandle;

use super::Database;
use crate::commit::snapshot::CommitSeq;
use crate::{Error, Result};

const INITIAL_PUSH_RETRY: Duration = Duration::from_millis(100);
const MAX_PUSH_RETRY: Duration = Duration::from_secs(30);

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

/// Mailbox handle for freshness, refresh serialization, and delayed pushes.
pub struct PolicyActor {
    policy: SyncPolicy,
    sender: Sender<Command>,
    receiver: Mutex<Option<Receiver<Command>>>,
}

enum Command {
    Acquire {
        mode: AcquireMode,
        requested_at: Instant,
        reply: oneshot::Sender<AcquireReply>,
    },
    Complete {
        token: u64,
        transition: RefreshTransition,
    },
    Schedule(Instant),
    ScheduleGroup {
        group: CommitSeq,
        deadline: Instant,
    },
    PushComplete(Result<()>),
    Stop,
}

#[derive(Clone, Copy)]
enum AcquireMode {
    IfStale,
    Exclusive,
}

enum AcquireReply {
    Fresh,
    Lease(PolicyLease),
}

struct Waiter {
    mode: AcquireMode,
    requested_at: Instant,
    reply: oneshot::Sender<AcquireReply>,
}

/// State transition published when one serialized authority workflow ends.
#[derive(Clone, Copy)]
pub enum RefreshTransition {
    Unchanged,
    Pulled { pulled_at: Instant },
    Rebased { pulled_at: Option<Instant> },
}

/// Exclusive authority-refresh lease. Dropping it always releases the actor.
pub struct PolicyLease {
    token: u64,
    sender: Sender<Command>,
    completed: bool,
}

impl PolicyLease {
    pub fn complete(mut self, transition: RefreshTransition) {
        self.completed = true;
        let _ = self.sender.send(Command::Complete {
            token: self.token,
            transition,
        });
    }
}

impl Drop for PolicyLease {
    fn drop(&mut self) {
        if !self.completed {
            let _ = self.sender.send(Command::Complete {
                token: self.token,
                transition: RefreshTransition::Unchanged,
            });
        }
    }
}

impl PolicyActor {
    pub fn new(policy: SyncPolicy) -> Self {
        let (sender, receiver) = mpsc::channel();
        Self {
            policy,
            sender,
            receiver: Mutex::new(Some(receiver)),
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

    pub fn start<H>(&self, database: Weak<Database<H>>) -> Result<()>
    where
        H: ServerHandle + Send + Sync + 'static,
    {
        let Some(receiver) = lock(&self.receiver).take() else {
            return Ok(());
        };
        let sender = self.sender.clone();
        let policy = self.policy;
        let (pushes, push_jobs) = mpsc::channel();
        let push_sender = sender.clone();
        let push_database = database.clone();
        std::thread::Builder::new()
            .name("multilite-policy-workflow".into())
            .spawn(move || run_push_worker(push_jobs, push_sender, push_database))
            .map_err(|error| Error::BackgroundWorker(error.to_string()))?;
        std::thread::Builder::new()
            .name("multilite-policy".into())
            .spawn(move || run_actor(receiver, sender, pushes, policy))
            .map_err(|error| Error::BackgroundWorker(error.to_string()))?;
        Ok(())
    }

    /// Coalesce a stale read behind one complete push/pull/rebase workflow.
    pub async fn refresh_if_needed(&self, requested_at: Instant) -> Result<Option<PolicyLease>> {
        if self.policy == SyncPolicy::LocalOnly {
            return Ok(None);
        }
        self.acquire(AcquireMode::IfStale, requested_at).await
    }

    /// Serialize an explicit pull or rebase with every other refresh workflow.
    pub async fn enter_workflow(&self) -> Result<PolicyLease> {
        self.acquire(AcquireMode::Exclusive, Instant::now())
            .await?
            .ok_or_else(unavailable)
    }

    async fn acquire(
        &self,
        mode: AcquireMode,
        requested_at: Instant,
    ) -> Result<Option<PolicyLease>> {
        let (reply, response) = oneshot::channel();
        self.sender
            .send(Command::Acquire {
                mode,
                requested_at,
                reply,
            })
            .map_err(|_| unavailable())?;
        match response.await.map_err(|_| unavailable())? {
            AcquireReply::Fresh => Ok(None),
            AcquireReply::Lease(lease) => Ok(Some(lease)),
        }
    }

    pub fn schedule(&self, delay: Duration) {
        let now = Instant::now();
        let deadline = now.checked_add(delay).unwrap_or(now);
        let _ = self.sender.send(Command::Schedule(deadline));
    }

    pub fn schedule_group(&self, group: CommitSeq, delay: Duration) {
        let now = Instant::now();
        let deadline = now.checked_add(delay).unwrap_or(now);
        let _ = self.sender.send(Command::ScheduleGroup { group, deadline });
    }
}

impl Drop for PolicyActor {
    fn drop(&mut self) {
        let _ = self.sender.send(Command::Stop);
    }
}

struct PolicyMachine {
    policy: SyncPolicy,
    last_refresh: Option<Instant>,
    unapplied_pull: Option<Instant>,
    active: Option<u64>,
    next_token: u64,
    waiters: VecDeque<Waiter>,
    deadline: Option<Instant>,
    retry_delay: Duration,
    last_group: Option<CommitSeq>,
    push_pending: bool,
    push_in_flight: bool,
}

impl PolicyMachine {
    fn new(policy: SyncPolicy) -> Self {
        Self {
            policy,
            last_refresh: None,
            unapplied_pull: None,
            active: None,
            next_token: 1,
            waiters: VecDeque::new(),
            deadline: None,
            retry_delay: INITIAL_PUSH_RETRY,
            last_group: None,
            push_pending: false,
            push_in_flight: false,
        }
    }

    fn enqueue(&mut self, waiter: Waiter, sender: &Sender<Command>) {
        self.waiters.push_back(waiter);
        self.grant_next(sender);
    }

    fn complete(&mut self, token: u64, transition: RefreshTransition) {
        if self.active != Some(token) {
            return;
        }
        match transition {
            RefreshTransition::Unchanged => {}
            RefreshTransition::Pulled { pulled_at } => {
                self.last_refresh = None;
                self.unapplied_pull = Some(pulled_at);
            }
            RefreshTransition::Rebased { pulled_at } => {
                if let Some(pulled_at) = pulled_at.or_else(|| self.unapplied_pull.take()) {
                    self.last_refresh = Some(pulled_at);
                }
                if pulled_at.is_some() {
                    self.unapplied_pull = None;
                }
            }
        }
        self.active = None;
    }

    fn grant_next(&mut self, sender: &Sender<Command>) {
        if self.active.is_some() || self.push_pending || self.push_in_flight {
            return;
        }
        while let Some(waiter) = self.waiters.pop_front() {
            if matches!(waiter.mode, AcquireMode::IfStale)
                && !self.requires_refresh(waiter.requested_at)
            {
                let _ = waiter.reply.send(AcquireReply::Fresh);
                continue;
            }
            let token = self.next_token;
            self.next_token = self.next_token.wrapping_add(1).max(1);
            self.active = Some(token);
            let lease = PolicyLease {
                token,
                sender: sender.clone(),
                completed: false,
            };
            if waiter.reply.send(AcquireReply::Lease(lease)).is_ok() {
                return;
            }
            self.active = None;
        }
    }

    fn requires_refresh(&self, requested_at: Instant) -> bool {
        match self.policy {
            SyncPolicy::LocalOnly => false,
            SyncPolicy::Remote => self
                .last_refresh
                .is_none_or(|refreshed| refreshed < requested_at),
            SyncPolicy::LocalFirst { read_staleness, .. } => {
                if read_staleness.is_zero() {
                    self.last_refresh
                        .is_none_or(|refreshed| refreshed < requested_at)
                } else {
                    self.last_refresh.is_none_or(|refreshed| {
                        Instant::now().saturating_duration_since(refreshed) > read_staleness
                    })
                }
            }
        }
    }

    fn schedule(&mut self, candidate: Instant) {
        self.deadline = Some(
            self.deadline
                .map_or(candidate, |current| current.min(candidate)),
        );
        self.retry_delay = INITIAL_PUSH_RETRY;
    }

    fn schedule_group(&mut self, group: CommitSeq, deadline: Instant) {
        if self.last_group.is_some_and(|scheduled| scheduled >= group) {
            return;
        }
        self.last_group = Some(group);
        self.schedule(deadline);
    }

    fn push_failed(&mut self) {
        let now = Instant::now();
        self.deadline = Some(now.checked_add(self.retry_delay).unwrap_or(now));
        self.retry_delay = self
            .retry_delay
            .checked_mul(2)
            .unwrap_or(MAX_PUSH_RETRY)
            .min(MAX_PUSH_RETRY);
    }

    fn mark_push_due(&mut self) {
        self.deadline = None;
        self.push_pending = true;
    }

    fn take_push_job(&mut self) -> bool {
        if !self.push_pending || self.push_in_flight || self.active.is_some() {
            return false;
        }
        self.push_pending = false;
        self.push_in_flight = true;
        true
    }

    fn complete_push(&mut self, result: &Result<()>) {
        if !self.push_in_flight {
            return;
        }
        self.push_in_flight = false;
        if result.is_ok() {
            self.retry_delay = INITIAL_PUSH_RETRY;
        } else {
            // One retry deadline covers every write observed while this failed
            // attempt was in flight; otherwise zero-delay schedules could spin
            // against an unavailable authority without honoring backoff.
            self.push_pending = false;
            self.push_failed();
        }
    }
}

fn run_actor(
    receiver: Receiver<Command>,
    sender: Sender<Command>,
    pushes: Sender<()>,
    policy: SyncPolicy,
) {
    let mut state = PolicyMachine::new(policy);
    loop {
        let received = match state.deadline {
            Some(at) if at <= Instant::now() => Err(RecvTimeoutError::Timeout),
            Some(at) => receiver.recv_timeout(at.saturating_duration_since(Instant::now())),
            None => receiver.recv().map_err(|_| RecvTimeoutError::Disconnected),
        };
        match received {
            Ok(Command::Acquire {
                mode,
                requested_at,
                reply,
            }) => state.enqueue(
                Waiter {
                    mode,
                    requested_at,
                    reply,
                },
                &sender,
            ),
            Ok(Command::Complete { token, transition }) => {
                state.complete(token, transition);
                drive(&mut state, &sender, &pushes);
            }
            Ok(Command::Schedule(deadline)) => state.schedule(deadline),
            Ok(Command::ScheduleGroup { group, deadline }) => state.schedule_group(group, deadline),
            Ok(Command::PushComplete(result)) => {
                state.complete_push(&result);
                drive(&mut state, &sender, &pushes);
            }
            Ok(Command::Stop) | Err(RecvTimeoutError::Disconnected) => return,
            Err(RecvTimeoutError::Timeout) => {
                state.mark_push_due();
                drive(&mut state, &sender, &pushes);
            }
        }
    }
}

fn drive(state: &mut PolicyMachine, sender: &Sender<Command>, pushes: &Sender<()>) {
    if state.take_push_job() {
        if pushes.send(()).is_err() {
            state.push_in_flight = false;
            state.push_failed();
        }
    } else {
        state.grant_next(sender);
    }
}

fn run_push_worker<H>(jobs: Receiver<()>, sender: Sender<Command>, database: Weak<Database<H>>)
where
    H: ServerHandle + Send + Sync + 'static,
{
    while jobs.recv().is_ok() {
        let Some(database) = database.upgrade() else {
            return;
        };
        let result = database.run_scheduled_push();
        if sender.send(Command::PushComplete(result)).is_err() {
            return;
        }
    }
}

fn unavailable() -> Error {
    Error::BackgroundWorker("policy actor is unavailable".into())
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::database::OfflineServer;

    #[test]
    fn one_local_first_schedule_is_retained_per_commit_group() {
        let mut state = PolicyMachine::new(SyncPolicy::LocalFirst {
            write_delay: Duration::ZERO,
            read_staleness: Duration::ZERO,
        });
        let first = Instant::now() + Duration::from_secs(3);
        state.schedule_group(CommitSeq(7), first);
        state.schedule_group(CommitSeq(7), first - Duration::from_secs(1));
        state.schedule_group(CommitSeq(6), first - Duration::from_secs(2));
        assert_eq!(state.deadline, Some(first));

        let next = first - Duration::from_secs(1);
        state.schedule_group(CommitSeq(8), next);
        assert_eq!(state.deadline, Some(next));
    }

    #[test]
    fn remote_waiters_coalesce_behind_one_successful_refresh() {
        let (sender, _receiver) = mpsc::channel();
        let mut state = PolicyMachine::new(SyncPolicy::Remote);
        let requested_at = Instant::now();
        let (first_reply, first_response) = oneshot::channel();
        state.enqueue(
            Waiter {
                mode: AcquireMode::IfStale,
                requested_at,
                reply: first_reply,
            },
            &sender,
        );
        let AcquireReply::Lease(mut first) = pollster::block_on(first_response).unwrap() else {
            panic!("first stale read did not receive the refresh lease")
        };
        let (second_reply, mut second_response) = oneshot::channel();
        state.enqueue(
            Waiter {
                mode: AcquireMode::IfStale,
                requested_at,
                reply: second_reply,
            },
            &sender,
        );
        assert!(second_response.try_recv().unwrap().is_none());

        state.complete(
            first.token,
            RefreshTransition::Rebased {
                pulled_at: Some(Instant::now()),
            },
        );
        state.grant_next(&sender);
        assert!(matches!(
            pollster::block_on(second_response).unwrap(),
            AcquireReply::Fresh
        ));
        first.completed = true;
    }

    #[test]
    fn failed_refresh_hands_the_lease_to_the_next_waiter() {
        let (sender, _receiver) = mpsc::channel();
        let mut state = PolicyMachine::new(SyncPolicy::Remote);
        let requested_at = Instant::now();
        let (first_reply, first_response) = oneshot::channel();
        state.enqueue(
            Waiter {
                mode: AcquireMode::IfStale,
                requested_at,
                reply: first_reply,
            },
            &sender,
        );
        let AcquireReply::Lease(mut first) = pollster::block_on(first_response).unwrap() else {
            unreachable!()
        };
        let (second_reply, second_response) = oneshot::channel();
        state.enqueue(
            Waiter {
                mode: AcquireMode::IfStale,
                requested_at,
                reply: second_reply,
            },
            &sender,
        );

        state.complete(first.token, RefreshTransition::Unchanged);
        state.grant_next(&sender);
        assert!(matches!(
            pollster::block_on(second_response).unwrap(),
            AcquireReply::Lease(_)
        ));
        first.completed = true;
    }

    #[test]
    fn a_due_push_waits_for_refresh_and_precedes_the_next_waiter() {
        let (sender, _receiver) = mpsc::channel();
        let mut state = PolicyMachine::new(SyncPolicy::Remote);
        let requested_at = Instant::now();
        let (first_reply, first_response) = oneshot::channel();
        state.enqueue(
            Waiter {
                mode: AcquireMode::IfStale,
                requested_at,
                reply: first_reply,
            },
            &sender,
        );
        let AcquireReply::Lease(mut first) = pollster::block_on(first_response).unwrap() else {
            unreachable!()
        };
        let (second_reply, mut second_response) = oneshot::channel();
        state.enqueue(
            Waiter {
                mode: AcquireMode::IfStale,
                requested_at,
                reply: second_reply,
            },
            &sender,
        );

        state.mark_push_due();
        assert!(!state.take_push_job());
        state.complete(first.token, RefreshTransition::Unchanged);
        assert!(state.take_push_job());
        state.grant_next(&sender);
        assert!(second_response.try_recv().unwrap().is_none());

        state.complete_push(&Ok(()));
        state.grant_next(&sender);
        assert!(matches!(
            pollster::block_on(second_response).unwrap(),
            AcquireReply::Lease(_)
        ));
        first.completed = true;
    }

    #[test]
    fn a_failed_push_rearms_with_bounded_backoff() {
        let mut state = PolicyMachine::new(SyncPolicy::LocalFirst {
            write_delay: Duration::ZERO,
            read_staleness: Duration::ZERO,
        });
        state.mark_push_due();
        assert!(state.take_push_job());
        state.complete_push(&Err(Error::BackgroundWorker("injected".into())));

        let deadline = state.deadline.expect("failed push is rescheduled");
        assert!(deadline > Instant::now());
        assert_eq!(state.retry_delay, INITIAL_PUSH_RETRY * 2);
        assert!(!state.push_in_flight);
    }

    #[test]
    fn explicit_pull_then_rebase_advances_one_atomic_freshness_state() {
        let mut state = PolicyMachine::new(SyncPolicy::Remote);
        let pulled_at = Instant::now();

        state.active = Some(1);
        state.complete(1, RefreshTransition::Pulled { pulled_at });
        assert!(state.last_refresh.is_none());
        assert_eq!(state.unapplied_pull, Some(pulled_at));

        state.active = Some(2);
        state.complete(2, RefreshTransition::Rebased { pulled_at: None });
        assert_eq!(state.last_refresh, Some(pulled_at));
        assert!(state.unapplied_pull.is_none());
        assert!(!state.requires_refresh(pulled_at));
    }

    #[test]
    fn dropping_a_refresh_lease_releases_the_next_waiter() {
        let actor = PolicyActor::new(SyncPolicy::Remote);
        actor
            .start::<OfflineServer>(Weak::new())
            .expect("policy actor starts");

        let first = pollster::block_on(actor.refresh_if_needed(Instant::now()))
            .unwrap()
            .expect("first stale read owns the workflow");
        drop(first);

        let second = pollster::block_on(actor.refresh_if_needed(Instant::now()))
            .unwrap()
            .expect("cancelled workflow passes ownership onward");
        second.complete(RefreshTransition::Unchanged);
    }
}
