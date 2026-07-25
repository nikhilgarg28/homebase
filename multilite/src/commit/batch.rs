//! Bounded FIFO staging for owned commit proposals.

use std::collections::VecDeque;
use std::sync::{Mutex, MutexGuard};

use futures_channel::oneshot;

use super::proposal::{CommitProposal, CommitReceipt};
use crate::{Error, Result};

const QUEUE_CAPACITY: usize = 256;
const GROUP_CAPACITY: usize = 32;

/// Proposal inbox shared by application threads and the serial committer.
pub struct CommitQueue {
    capacity: usize,
    group_capacity: usize,
    state: Mutex<QueueState>,
}

#[derive(Default)]
struct QueueState {
    scheduled: bool,
    pending: VecDeque<QueuedCommit>,
}

impl CommitQueue {
    pub fn new() -> Self {
        Self {
            capacity: QUEUE_CAPACITY,
            group_capacity: GROUP_CAPACITY,
            state: Mutex::new(QueueState::default()),
        }
    }

    /// Enqueue one proposal and report whether a committer drain must be scheduled.
    pub fn enqueue(&self, proposal: CommitProposal) -> Result<CommitTicket> {
        let (reply, response) = oneshot::channel();
        let mut state = lock(&self.state);
        if state.pending.len() >= self.capacity {
            return Err(Error::Committer("commit proposal queue is full".into()));
        }
        state.pending.push_back(QueuedCommit { proposal, reply });
        let schedule = !state.scheduled;
        state.scheduled = true;
        Ok(CommitTicket { schedule, response })
    }

    /// Take the next bounded FIFO group, or end the current drain turn.
    pub fn take_group(&self) -> Option<Vec<QueuedCommit>> {
        let mut state = lock(&self.state);
        if state.pending.is_empty() {
            state.scheduled = false;
            return None;
        }
        let count = state.pending.len().min(self.group_capacity);
        Some(state.pending.drain(..count).collect())
    }

    /// Fail every request retained after a drain could not be scheduled.
    pub fn fail_all(&self, message: String) {
        let pending = {
            let mut state = lock(&self.state);
            state.scheduled = false;
            state.pending.drain(..).collect::<Vec<_>>()
        };
        for queued in pending {
            queued.reply(Err(Error::Committer(message.clone())));
        }
    }

    #[cfg(test)]
    pub(crate) fn pending_len(&self) -> usize {
        lock(&self.state).pending.len()
    }

    #[cfg(test)]
    fn with_limits(capacity: usize, group_capacity: usize) -> Self {
        Self {
            capacity,
            group_capacity,
            state: Mutex::new(QueueState::default()),
        }
    }
}

impl Default for CommitQueue {
    fn default() -> Self {
        Self::new()
    }
}

/// Caller-side wait handle for one queued proposal.
pub struct CommitTicket {
    schedule: bool,
    response: oneshot::Receiver<Result<CommitReceipt>>,
}

impl CommitTicket {
    pub fn should_schedule(&self) -> bool {
        self.schedule
    }

    pub fn wait(self) -> Result<CommitReceipt> {
        pollster::block_on(self.response)
            .map_err(|_| Error::Committer("commit proposal reply was dropped".into()))?
    }
}

/// One proposal and its reply channel, owned by a committer drain.
pub struct QueuedCommit {
    proposal: CommitProposal,
    reply: oneshot::Sender<Result<CommitReceipt>>,
}

impl QueuedCommit {
    pub fn proposal(&self) -> &CommitProposal {
        &self.proposal
    }

    pub fn reply(self, result: Result<CommitReceipt>) {
        let _ = self.reply.send(result);
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[cfg(test)]
mod tests {
    use homebase_client::meta::OplogCursors;
    use homebase_core::tag::AdmissionSeq;

    use super::*;
    use crate::commit::snapshot::{CommitSeq, SnapshotDescriptor};
    use crate::database::isolation::IsolationLevel;
    use crate::database::operation::MultiliteOp;
    use crate::database::schema::{
        CreateColumn, CreateTable, CreateTableSpec, DeclaredType, SqlName,
    };
    use crate::database::transaction::MultiliteTransaction;

    fn proposal(name: &str) -> CommitProposal {
        let transaction =
            MultiliteTransaction::new(vec![MultiliteOp::CreateTable(CreateTable::new(
                &format!("CREATE TABLE {name} (id INTEGER PRIMARY KEY)"),
                CreateTableSpec {
                    name: SqlName::new(name.into()),
                    columns: vec![CreateColumn {
                        name: SqlName::new("id".into()),
                        declared_type: DeclaredType::Integer,
                        not_null: false,
                        primary_key: true,
                    }],
                },
            ))])
            .unwrap();
        CommitProposal::from_transaction(
            SnapshotDescriptor {
                commit_seq: CommitSeq(0),
                authority_applied_through: AdmissionSeq(0),
                submit_cursors: OplogCursors::default(),
            },
            IsolationLevel::Snapshot,
            transaction,
            std::iter::empty(),
        )
        .unwrap()
    }

    #[test]
    fn one_schedule_drains_bounded_fifo_groups() {
        let queue = CommitQueue::with_limits(4, 2);
        let first = queue.enqueue(proposal("one")).unwrap();
        let second = queue.enqueue(proposal("two")).unwrap();
        let third = queue.enqueue(proposal("three")).unwrap();
        assert!(first.should_schedule());
        assert!(!second.should_schedule());
        assert!(!third.should_schedule());

        let group = queue.take_group().unwrap();
        assert_eq!(
            group
                .iter()
                .map(|queued| {
                    let crate::database::operation::MultiliteOp::CreateTable(created) =
                        &queued.proposal().transaction().operations()[0]
                    else {
                        unreachable!()
                    };
                    created.table_name().to_owned()
                })
                .collect::<Vec<_>>(),
            ["one", "two"]
        );
        for queued in group {
            queued.reply(Err(Error::Committer("test".into())));
        }
        let group = queue.take_group().unwrap();
        assert_eq!(group.len(), 1);
        group
            .into_iter()
            .next()
            .unwrap()
            .reply(Err(Error::Committer("test".into())));
        assert!(queue.take_group().is_none());
        assert!(matches!(first.wait(), Err(Error::Committer(_))));
        assert!(matches!(second.wait(), Err(Error::Committer(_))));
        assert!(matches!(third.wait(), Err(Error::Committer(_))));
    }

    #[test]
    fn capacity_is_explicit_and_a_finished_turn_can_schedule_again() {
        let queue = CommitQueue::with_limits(1, 1);
        let first = queue.enqueue(proposal("one")).unwrap();
        assert!(matches!(
            queue.enqueue(proposal("two")),
            Err(Error::Committer(message)) if message.contains("full")
        ));
        queue
            .take_group()
            .unwrap()
            .pop()
            .unwrap()
            .reply(Err(Error::Committer("test".into())));
        assert!(queue.take_group().is_none());
        assert!(matches!(first.wait(), Err(Error::Committer(_))));
        assert!(queue.enqueue(proposal("two")).unwrap().should_schedule());
    }
}
