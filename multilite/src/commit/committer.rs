//! Typed FIFO serialization for canonical proposals and snapshot capture.

use std::collections::BTreeMap;
use std::fmt;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;

use async_channel::{Receiver, Sender, TryRecvError, WeakSender};
use futures_channel::oneshot;
use parking_lot::Mutex;
use rusqlite::Connection;

use crate::branch::snapshot::PinnedSnapshot;
use crate::commit::history::{self, PreparedRecord};
use crate::commit::proposal::{CommitProposal, CommitReceipt};
use crate::commit::snapshot::{CommitSeq, SnapshotDescriptor};
use crate::{Error, Result};

const INBOX_CAPACITY: usize = 256;
const GROUP_CAPACITY: usize = 32;

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

/// Shared owner of canonical sequence advancement, OCC retention, and pins.
#[derive(Clone, Default)]
pub struct CommitHistory {
    pins: HistoryPins,
}

impl CommitHistory {
    /// Current canonical visibility sequence.
    pub fn current(&self, connection: &Connection) -> Result<CommitSeq> {
        history::current(connection)
    }

    /// Pin retained OCC evidence for one writable branch.
    pub fn pin(&self, commit_seq: CommitSeq) -> HistoryPin {
        self.pins.register(commit_seq)
    }

    /// Publish one proposal-granular canonical transition.
    pub fn record_group(
        &self,
        connection: &Connection,
        records: Vec<PreparedRecord>,
    ) -> Result<CommitSeq> {
        history::record_group(connection, records)
    }

    /// Prune evidence no branch needs while retaining the newest receipts.
    ///
    /// The committer sends every reply before it can process the next group.
    /// Keeping the current sequence therefore covers immediate retry and
    /// reply-delivery ambiguity without a second receipt-retention frontier.
    pub fn prune(&self, connection: &Connection) -> Result<usize> {
        let current = history::current(connection)?;
        let before_current = CommitSeq(current.0.saturating_sub(1));
        let through = self
            .pins
            .oldest()
            .map_or(before_current, |oldest| oldest.min(before_current));
        history::prune(connection, through)
    }
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

/// One physical and logical database snapshot captured at a queue boundary.
pub struct CommitSnapshot {
    pub(crate) physical: PinnedSnapshot,
    pub(crate) logical: SnapshotDescriptor,
    pub(crate) history_pin: Option<HistoryPin>,
}

/// Canonical database work owned by the committer thread.
pub trait CommitBackend: Send + Sync + 'static {
    /// Prepare and atomically finalize one bounded FIFO proposal group.
    fn commit_group(&self, proposals: &[&CommitProposal]) -> Result<Vec<Result<CommitReceipt>>>;

    /// Capture one physical and logical snapshot at this exact queue position.
    fn capture_snapshot(&self, writable: bool) -> Result<CommitSnapshot>;
}

enum Request {
    Propose {
        proposal: CommitProposal,
        reply: oneshot::Sender<Result<CommitReceipt>>,
    },
    CaptureSnapshot {
        writable: bool,
        reply: oneshot::Sender<Result<CommitSnapshot>>,
    },
}

/// Sending side of one typed serial database executor.
pub struct Committer {
    outbox: Sender<Request>,
}

impl Clone for Committer {
    fn clone(&self) -> Self {
        Self {
            outbox: self.outbox.clone(),
        }
    }
}

/// Non-owning route used by components retained by the committer backend.
pub struct WeakCommitter {
    outbox: WeakSender<Request>,
}

impl Clone for WeakCommitter {
    fn clone(&self) -> Self {
        Self {
            outbox: self.outbox.clone(),
        }
    }
}

impl Committer {
    pub fn new<B>(backend: Arc<B>) -> std::result::Result<Self, CommitterError>
    where
        B: CommitBackend,
    {
        Self::with_capacity(backend, INBOX_CAPACITY)
    }

    fn with_capacity<B>(
        backend: Arc<B>,
        capacity: usize,
    ) -> std::result::Result<Self, CommitterError>
    where
        B: CommitBackend,
    {
        let (outbox, inbox) = async_channel::bounded(capacity);
        std::thread::Builder::new()
            .name("multilite-committer".into())
            .spawn(move || run(inbox, backend))
            .map_err(|error| CommitterError::Startup(error.to_string()))?;
        Ok(Self { outbox })
    }

    /// Enqueue one owned canonical transition.
    pub async fn propose(&self, proposal: CommitProposal) -> Result<CommitReceipt> {
        let (reply, response) = oneshot::channel();
        self.outbox
            .send(Request::Propose { proposal, reply })
            .await
            .map_err(|_| Error::Committer(CommitterError::Unavailable.to_string()))?;
        response
            .await
            .map_err(|_| Error::Committer(CommitterError::Unavailable.to_string()))?
    }

    pub fn propose_blocking(&self, proposal: CommitProposal) -> Result<CommitReceipt> {
        pollster::block_on(self.propose(proposal))
    }

    /// Return a route that does not keep the committer worker alive.
    pub fn downgrade(&self) -> WeakCommitter {
        WeakCommitter {
            outbox: self.outbox.downgrade(),
        }
    }

    /// Capture one snapshot after every preceding proposal and before every
    /// following proposal.
    pub async fn capture_snapshot(&self, writable: bool) -> Result<CommitSnapshot> {
        let (reply, response) = oneshot::channel();
        self.outbox
            .send(Request::CaptureSnapshot { writable, reply })
            .await
            .map_err(|_| Error::Committer(CommitterError::Unavailable.to_string()))?;
        response
            .await
            .map_err(|_| Error::Committer(CommitterError::Unavailable.to_string()))?
    }

    pub fn capture_snapshot_blocking(&self, writable: bool) -> Result<CommitSnapshot> {
        pollster::block_on(self.capture_snapshot(writable))
    }
}

impl WeakCommitter {
    pub fn propose_blocking(&self, proposal: CommitProposal) -> Result<CommitReceipt> {
        let outbox = self
            .outbox
            .upgrade()
            .ok_or_else(|| Error::Committer(CommitterError::Unavailable.to_string()))?;
        let (reply, response) = oneshot::channel();
        outbox
            .send_blocking(Request::Propose { proposal, reply })
            .map_err(|_| Error::Committer(CommitterError::Unavailable.to_string()))?;
        pollster::block_on(response)
            .map_err(|_| Error::Committer(CommitterError::Unavailable.to_string()))?
    }
}

fn run<B: CommitBackend>(inbox: Receiver<Request>, backend: Arc<B>) {
    let mut deferred = None;
    loop {
        let request = match deferred.take() {
            Some(request) => request,
            None => match inbox.recv_blocking() {
                Ok(request) => request,
                Err(_) => return,
            },
        };
        match request {
            Request::Propose { proposal, reply } => {
                let mut group = vec![(proposal, reply)];
                while group.len() < GROUP_CAPACITY {
                    match inbox.try_recv() {
                        Ok(Request::Propose { proposal, reply }) => {
                            group.push((proposal, reply));
                        }
                        Ok(snapshot @ Request::CaptureSnapshot { .. }) => {
                            deferred = Some(snapshot);
                            break;
                        }
                        Err(TryRecvError::Empty) => break,
                        Err(TryRecvError::Closed) => break,
                    }
                }
                let proposals = group
                    .iter()
                    .map(|(proposal, _)| proposal)
                    .collect::<Vec<_>>();
                let results = catch_unwind(AssertUnwindSafe(|| backend.commit_group(&proposals)));
                match results {
                    Ok(Ok(results)) if results.len() == group.len() => {
                        for ((_, reply), result) in group.into_iter().zip(results) {
                            let _ = reply.send(result);
                        }
                    }
                    Ok(Ok(_)) => {
                        fail_group(group, "committer returned the wrong result count");
                    }
                    Ok(Err(error)) => {
                        fail_group(group, &format!("commit group aborted: {error}"));
                    }
                    Err(_) => fail_group(group, "commit group panicked"),
                }
            }
            Request::CaptureSnapshot { writable, reply } => {
                let result = catch_unwind(AssertUnwindSafe(|| backend.capture_snapshot(writable)))
                    .unwrap_or_else(|_| Err(Error::Committer("snapshot capture panicked".into())));
                let _ = reply.send(result);
            }
        }
    }
}

fn fail_group(group: Vec<(CommitProposal, oneshot::Sender<Result<CommitReceipt>>)>, message: &str) {
    for (_, reply) in group {
        let _ = reply.send(Err(Error::Committer(message.to_owned())));
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CommitterError {
    Startup(String),
    Unavailable,
}

impl fmt::Display for CommitterError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Startup(message) => write!(f, "could not start committer: {message}"),
            Self::Unavailable => f.write_str("committer is unavailable"),
        }
    }
}

impl std::error::Error for CommitterError {}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    use homebase_client::meta::OplogCursors;
    use homebase_core::tag::AdmissionSeq;

    use super::*;
    use crate::commit::snapshot::SnapshotDescriptor;
    use crate::database::isolation::IsolationLevel;
    use crate::database::operation::MultiliteOp;
    use crate::database::schema::{
        CreateColumn, CreateTable, CreateTableSpec, SqlName, TypeDeclaration,
    };
    use crate::database::transaction::MultiliteTransaction;

    struct TestBackend {
        groups: Mutex<Vec<Vec<String>>>,
        snapshot: AtomicU64,
        _directory: tempfile::TempDir,
        database_path: PathBuf,
        wal_path: PathBuf,
    }

    impl CommitBackend for TestBackend {
        fn commit_group(
            &self,
            proposals: &[&CommitProposal],
        ) -> Result<Vec<Result<CommitReceipt>>> {
            self.groups.lock().push(
                proposals
                    .iter()
                    .map(|proposal| {
                        let MultiliteOp::CreateTable(created) =
                            &proposal.transaction().operations()[0]
                        else {
                            unreachable!()
                        };
                        created.table_name().to_owned()
                    })
                    .collect(),
            );
            let commit_seq = CommitSeq(self.snapshot.fetch_add(1, Ordering::SeqCst) + 1);
            Ok(proposals
                .iter()
                .map(|_| {
                    Ok(CommitReceipt {
                        commit_seq,
                        disposition: crate::commit::proposal::CommitDisposition::Applied,
                        submitted: None,
                    })
                })
                .collect())
        }

        fn capture_snapshot(&self, _writable: bool) -> Result<CommitSnapshot> {
            let commit_seq = CommitSeq(self.snapshot.load(Ordering::SeqCst));
            Ok(CommitSnapshot {
                physical: PinnedSnapshot::capture(&self.database_path, &self.wal_path)
                    .map_err(|error| Error::Branch(error.to_string()))?,
                logical: SnapshotDescriptor {
                    commit_seq,
                    authority_applied_through: AdmissionSeq(0),
                    submit_cursors: OplogCursors::default(),
                },
                history_pin: None,
            })
        }
    }

    fn backend() -> Arc<TestBackend> {
        let directory = tempfile::tempdir().unwrap();
        let database_path = directory.path().join("committer.sqlite");
        let wal_path = directory.path().join("committer.sqlite-wal");
        let connection = Connection::open(&database_path).unwrap();
        connection
            .execute_batch("CREATE TABLE snapshot_fixture(id INTEGER PRIMARY KEY)")
            .unwrap();
        drop(connection);
        Arc::new(TestBackend {
            groups: Mutex::new(Vec::new()),
            snapshot: AtomicU64::new(0),
            _directory: directory,
            database_path,
            wal_path,
        })
    }

    fn proposal(name: &str) -> CommitProposal {
        let transaction =
            MultiliteTransaction::new(vec![MultiliteOp::CreateTable(CreateTable::new(
                &format!("CREATE TABLE {name} (id INTEGER PRIMARY KEY)"),
                CreateTableSpec {
                    name: SqlName::new(name.into()),
                    mode: Default::default(),
                    columns: vec![CreateColumn {
                        name: SqlName::new("id".into()),
                        declared_type: TypeDeclaration::integer(),
                        not_null: false,
                        primary_key: true,
                    }],
                    unique_constraints: Vec::new(),
                },
            ))])
            .unwrap();
        let (_, footprint) = transaction.to_homebase().unwrap().into_parts();
        CommitProposal::from_transaction(
            SnapshotDescriptor {
                commit_seq: CommitSeq(0),
                authority_applied_through: AdmissionSeq(0),
                submit_cursors: OplogCursors::default(),
            },
            IsolationLevel::Snapshot,
            transaction,
            footprint,
        )
        .unwrap()
    }

    #[test]
    fn proposals_batch_and_snapshot_capture_is_an_ordered_boundary() {
        let backend = backend();
        let committer = Committer::new(Arc::clone(&backend)).unwrap();

        let first = {
            let committer = committer.clone();
            std::thread::spawn(move || committer.propose_blocking(proposal("one")).unwrap())
        };
        let second = {
            let committer = committer.clone();
            std::thread::spawn(move || committer.propose_blocking(proposal("two")).unwrap())
        };
        first.join().unwrap();
        second.join().unwrap();

        let captured = committer.capture_snapshot_blocking(false).unwrap();
        assert!(captured.logical.commit_seq >= CommitSeq(1));
        let mut committed = backend
            .groups
            .lock()
            .iter()
            .flatten()
            .cloned()
            .collect::<Vec<_>>();
        committed.sort();
        assert_eq!(committed, ["one", "two"]);
    }

    #[test]
    fn history_frontier_tracks_the_oldest_live_commit_sequence() {
        let history = CommitHistory::default();
        let later = history.pin(CommitSeq(5));
        let earliest = history.pin(CommitSeq(2));
        let same = history.pin(CommitSeq(2));
        assert_eq!(history.pins.oldest(), Some(CommitSeq(2)));
        drop(earliest);
        assert_eq!(history.pins.oldest(), Some(CommitSeq(2)));
        drop(same);
        assert_eq!(history.pins.oldest(), Some(CommitSeq(5)));
        drop(later);
        assert_eq!(history.pins.oldest(), None);
    }

    #[test]
    fn pruning_keeps_the_current_receipts_and_respects_branch_pins() {
        fn record(byte: u8) -> PreparedRecord {
            let mut proposal_id = [byte; 16];
            proposal_id[6] = (proposal_id[6] & 0x0f) | 0x40;
            proposal_id[8] = (proposal_id[8] & 0x3f) | 0x80;
            PreparedRecord {
                proposal_id,
                proposal_hash: [byte; 32],
                submitted: None,
                writes: vec![history::WriteRegion::Point(
                    homebase_core::key::Key::from_bytes([b"rows".as_slice(), [byte].as_slice()])
                        .unwrap(),
                )],
            }
        }

        let connection = Connection::open_in_memory().unwrap();
        history::initialize(&connection).unwrap();
        let commits = CommitHistory::default();

        commits.record_group(&connection, vec![record(1)]).unwrap();
        assert_eq!(commits.prune(&connection).unwrap(), 0);

        let oldest = commits.pin(CommitSeq(0));
        commits.record_group(&connection, vec![record(2)]).unwrap();
        assert_eq!(commits.prune(&connection).unwrap(), 0);
        assert_eq!(
            history::history_after(&connection, CommitSeq(0))
                .unwrap()
                .into_iter()
                .map(|record| record.commit_seq)
                .collect::<Vec<_>>(),
            [CommitSeq(1), CommitSeq(2)]
        );

        drop(oldest);
        assert_eq!(commits.prune(&connection).unwrap(), 1);
        assert_eq!(
            history::history_after(&connection, CommitSeq(0))
                .unwrap()
                .into_iter()
                .map(|record| record.commit_seq)
                .collect::<Vec<_>>(),
            [CommitSeq(2)]
        );
    }
}
