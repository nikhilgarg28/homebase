//! Canonical SQLite commit execution for one Multilite database.

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::sync::Arc;

use homebase_client::meta::{MetaStore, OrderedMetaStore};
use homebase_client::{ClientError, ServerHandle};
use homebase_core::tag::{AdmissionSeq, DeviceSeq};
use parking_lot::Mutex;
use pollster::block_on;
use rusqlite::Connection;

use super::{DatabaseClient, DatabaseId, authorize_public, pending};
use crate::branch::snapshot::{PinnedReader, SnapshotCache};
use crate::commit::checkpoint::CheckpointPolicy;
use crate::commit::committer::{CommitBackend, CommitHistory, CommitSnapshot};
use crate::commit::history::{self, WriteRegion};
use crate::commit::proposal::{
    self, CommitDisposition, CommitProposal, CommitReceipt, PrepareOutcome, ProposalBody,
};
use crate::commit::snapshot::SnapshotDescriptor;
use crate::connection::ConnectionOwner;
use crate::metastore::{SqliteOrderedStore, SqliteSnapshotStore};
use crate::rowid;
use crate::{Error, Result};

pub(super) struct DatabaseCommitBackend<H: ServerHandle> {
    owner: ConnectionOwner,
    path: PathBuf,
    wal_path: PathBuf,
    database_id: DatabaseId,
    client: Arc<DatabaseClient<H>>,
    commit_history: CommitHistory,
    snapshot_cache: Mutex<SnapshotCache>,
    checkpoint: Mutex<CheckpointPolicy>,
}

enum GroupSlot {
    Prepared { submitted: Option<DeviceSeq> },
    DuplicatePrepared { submitted: Option<DeviceSeq> },
    Complete(Result<CommitReceipt>),
}

struct SeenGroupProposal {
    encoded: Vec<u8>,
    receipt: Option<CommitReceipt>,
    submitted: Option<DeviceSeq>,
}

impl<H: ServerHandle + Send + Sync + 'static> DatabaseCommitBackend<H> {
    pub(super) fn new(
        owner: ConnectionOwner,
        path: PathBuf,
        wal_path: PathBuf,
        database_id: DatabaseId,
        client: Arc<DatabaseClient<H>>,
        commit_history: CommitHistory,
    ) -> Self {
        Self {
            owner,
            path,
            wal_path,
            database_id,
            client,
            commit_history,
            snapshot_cache: Mutex::new(SnapshotCache::new()),
            checkpoint: Mutex::new(CheckpointPolicy::default()),
        }
    }

    /// Direct metadata access for work already owned by the canonical committer.
    ///
    /// Routing these writes through `DatabaseMetaStore` would propose them back
    /// to this same committer and deadlock. Keep the bypass centralized here.
    fn committer_metadata(&self) -> OrderedMetaStore<SqliteOrderedStore> {
        OrderedMetaStore::new(SqliteOrderedStore::new(self.owner.clone()))
    }

    #[cfg(test)]
    pub(super) fn capture_snapshot_inner(
        &self,
        track_for_commit: bool,
        after_physical: impl FnOnce() -> Result<()>,
    ) -> Result<CommitSnapshot> {
        self.capture_snapshot_with(track_for_commit, after_physical)
    }

    fn capture_snapshot_with(
        &self,
        track_for_commit: bool,
        after_physical: impl FnOnce() -> Result<()>,
    ) -> Result<CommitSnapshot> {
        let physical = self
            .snapshot_cache
            .lock()
            .capture(&self.path, &self.wal_path)
            .map_err(|error| Error::Branch(error.to_string()))?;
        after_physical()?;
        let (commit_seq, cursors) = physical.with_reader(|connection| {
            let commit_seq = self.commit_history.current(connection)?;
            let metadata = OrderedMetaStore::new(SqliteSnapshotStore::new(connection));
            let cursors = block_on(metadata.cursor_snapshot(self.database_id.space_id()))?;
            Ok::<_, Error>((commit_seq, cursors))
        })?;
        let authority_applied_through = AdmissionSeq(
            cursors
                .admits
                .neck
                .0
                .checked_sub(1)
                .ok_or(Error::InvalidDatabase("admit neck cannot be zero"))?,
        );
        Ok(CommitSnapshot {
            physical,
            logical: SnapshotDescriptor {
                commit_seq,
                authority_applied_through,
                submit_cursors: cursors.oplog,
            },
            history_pin: track_for_commit.then(|| self.commit_history.pin(commit_seq)),
        })
    }

    #[cfg(test)]
    pub(super) fn commit_history(&self) -> &CommitHistory {
        &self.commit_history
    }

    fn commit_proposal_group(
        &self,
        proposals: &[&CommitProposal],
    ) -> Result<Vec<Result<CommitReceipt>>> {
        self.owner
            .with_savepoint("__multilite__commit_group", |connection| {
                let mut accepted_writes = BTreeSet::new();
                let mut prepared = Vec::new();
                let mut slots = Vec::with_capacity(proposals.len());
                let mut seen: BTreeMap<_, SeenGroupProposal> = BTreeMap::new();

                for proposal in proposals {
                    let encoded = match proposal.encode() {
                        Ok(encoded) => encoded,
                        Err(error) => {
                            slots.push(GroupSlot::Complete(Err(error)));
                            continue;
                        }
                    };
                    if let Some(previous) = seen.get(&proposal.id()) {
                        if previous.encoded != encoded {
                            slots.push(GroupSlot::Complete(Err(Error::InvalidCommitProposal(
                                "one proposal id names different commit payloads".into(),
                            ))));
                        } else {
                            slots.push(match previous.receipt {
                                Some(receipt) => GroupSlot::Complete(Ok(receipt)),
                                None => GroupSlot::DuplicatePrepared {
                                    submitted: previous.submitted,
                                },
                            });
                        }
                        continue;
                    }

                    let outcome =
                        self.owner
                            .with_savepoint("__multilite__commit_member", |connection| {
                                self.prepare_commit_proposal(connection, proposal, &accepted_writes)
                            });

                    match outcome {
                        Ok(PrepareOutcome::Prepared(commit)) => {
                            accepted_writes.extend(commit.writes().iter().cloned());
                            let submitted = commit.submitted();
                            seen.insert(
                                proposal.id(),
                                SeenGroupProposal {
                                    encoded,
                                    receipt: None,
                                    submitted,
                                },
                            );
                            slots.push(GroupSlot::Prepared { submitted });
                            prepared.push(commit);
                        }
                        Ok(PrepareOutcome::AlreadyCommitted(receipt)) => {
                            seen.insert(
                                proposal.id(),
                                SeenGroupProposal {
                                    encoded,
                                    receipt: Some(receipt),
                                    submitted: receipt.submitted,
                                },
                            );
                            slots.push(GroupSlot::Complete(Ok(receipt)));
                        }
                        Err(error) => slots.push(GroupSlot::Complete(Err(error))),
                    }
                }

                let commit_seq = if prepared.is_empty() {
                    None
                } else {
                    Some(proposal::finalize_group(
                        connection,
                        &self.commit_history,
                        &prepared,
                    )?)
                };
                let results = slots
                    .into_iter()
                    .map(|slot| match slot {
                        GroupSlot::Prepared { submitted } => Ok(CommitReceipt {
                            commit_seq: commit_seq.expect("prepared group has a commit sequence"),
                            disposition: CommitDisposition::Applied,
                            submitted,
                        }),
                        GroupSlot::DuplicatePrepared { submitted } => Ok(CommitReceipt {
                            commit_seq: commit_seq.expect("prepared group has a commit sequence"),
                            disposition: CommitDisposition::AlreadyCommitted,
                            submitted,
                        }),
                        GroupSlot::Complete(result) => result,
                    })
                    .collect();

                self.commit_history.prune(connection)?;
                Ok(results)
            })
    }

    fn prepare_commit_proposal(
        &self,
        connection: &Connection,
        proposal: &CommitProposal,
        accepted_writes: &BTreeSet<WriteRegion>,
    ) -> Result<PrepareOutcome> {
        match proposal.body() {
            ProposalBody::Transaction(transaction) => {
                let outcome = proposal::prepare(connection, proposal, accepted_writes)?;
                let PrepareOutcome::Prepared(commit) = outcome else {
                    return Ok(outcome);
                };
                let (mutations, assertions) = proposal.to_homebase()?;
                let sequence = block_on(async {
                    let space = self.client.space(self.database_id.space_id()).await?;
                    let prepared = space
                        .prepare_unchecked(mutations, assertions)
                        .await
                        .map_err(ClientError::from)?;
                    space
                        .commit_prepared(prepared)
                        .await
                        .map_err(ClientError::from)
                        .map_err(Error::from)
                })?;
                pending::insert(connection, sequence, transaction.transaction())?;
                Ok(PrepareOutcome::Prepared(commit.with_submission(sequence)))
            }
            ProposalBody::ApplyAdmissions(apply) => {
                if let Some(receipt) = proposal.committed_receipt(connection)? {
                    return Ok(PrepareOutcome::AlreadyCommitted(receipt));
                }
                proposal.validate()?;
                let store = self.committer_metadata();
                let space = self.database_id.space_id();
                let current_submit = block_on(store.oplog_cursors(space))?;
                let current_admits = block_on(store.admit_cursors(space))?;
                if current_submit != apply.expected_submit()
                    || current_admits != apply.expected_admits()
                {
                    return Err(Error::RebaseStateChanged);
                }
                if current_submit.neck != current_submit.tail {
                    return Err(Error::RebasePendingSubmissions);
                }

                let mut writes = BTreeSet::new();
                for admitted in apply.transactions() {
                    if admitted.device == apply.local_device() {
                        continue;
                    }
                    let compiled = admitted.transaction.clone().compile()?;
                    compiled.logical().apply(connection)?;
                    writes.extend(history::writes_from_mutations(
                        compiled.homebase().mutations(),
                    ));
                }
                block_on(store.mark_admits_applied(space, apply.through()))?;
                Ok(PrepareOutcome::Prepared(
                    proposal.prepare_receipt(writes.into_iter().collect())?,
                ))
            }
            ProposalBody::RejectSubmissions(reject) => {
                if let Some(receipt) = proposal.committed_receipt(connection)? {
                    return Ok(PrepareOutcome::AlreadyCommitted(receipt));
                }
                proposal.validate()?;
                let store = self.committer_metadata();
                let space = self.database_id.space_id();
                let current = block_on(store.oplog_cursors(space))?;
                if current != reject.expected_submit() {
                    return Err(Error::StalePushRejection);
                }
                let through = DeviceSeq(
                    current
                        .tail
                        .0
                        .checked_sub(1)
                        .ok_or(Error::InvalidDatabase("submit tail cannot be zero"))?,
                );
                let active = block_on(store.oplog(space, current.neck, through))?;
                let repair = pending::prepare_rejection(connection, &active)?
                    .ok_or(Error::StalePushRejection)?;
                let writes = repair.writes().to_vec();
                repair.apply(connection)?;
                block_on(store.rollback_if_unchanged(
                    space,
                    reject.failed_at(),
                    reject.expected_submit(),
                ))?;
                Ok(PrepareOutcome::Prepared(proposal.prepare_receipt(writes)?))
            }
            ProposalBody::AcceptSubmissions(accept) => {
                if let Some(receipt) = proposal.committed_receipt(connection)? {
                    return Ok(PrepareOutcome::AlreadyCommitted(receipt));
                }
                proposal.validate()?;
                let store = self.committer_metadata();
                let space = self.database_id.space_id();
                let current = block_on(store.oplog_cursors(space))?;
                let expected = accept.expected_submit();
                if current.head != expected.head
                    || current.neck != expected.neck
                    || current.tail < expected.tail
                {
                    return Err(Error::CommitConflict(
                        "submit window changed before acknowledgement".into(),
                    ));
                }
                block_on(store.trim_oplog(space, accept.through(), accept.checksum()))?;
                pending::accept_through(connection, accept.through())?;
                Ok(PrepareOutcome::Prepared(
                    proposal.prepare_receipt(Vec::new())?,
                ))
            }
            ProposalBody::AppendAdmissions(append) => {
                if let Some(receipt) = proposal.committed_receipt(connection)? {
                    return Ok(PrepareOutcome::AlreadyCommitted(receipt));
                }
                proposal.validate()?;
                let store = self.committer_metadata();
                let space = self.database_id.space_id();
                if block_on(store.admit_cursors(space))? != append.expected_admits() {
                    return Err(Error::CommitConflict(
                        "admit window changed before pull capture".into(),
                    ));
                }
                block_on(store.append_admits(space, append.response()))?;
                Ok(PrepareOutcome::Prepared(
                    proposal.prepare_receipt(Vec::new())?,
                ))
            }
        }
    }
}

impl<H: ServerHandle + Send + Sync + 'static> CommitBackend for DatabaseCommitBackend<H> {
    fn commit_group(&self, proposals: &[&CommitProposal]) -> Result<Vec<Result<CommitReceipt>>> {
        let result = self.commit_proposal_group(proposals);
        if result.is_ok() {
            self.snapshot_cache.lock().invalidate_readers();
            self.owner.with_connection(|connection| {
                self.checkpoint
                    .lock()
                    .after_commit(connection, &self.wal_path)
            });
        }
        result
    }

    fn capture_snapshot(&self, writable: bool) -> Result<CommitSnapshot> {
        self.owner.with_connection(|connection| {
            self.checkpoint
                .lock()
                .before_snapshot(connection, &self.wal_path)
        })?;
        self.capture_snapshot_with(writable, || Ok(()))
    }

    fn capture_view(&self) -> Result<PinnedReader> {
        self.owner.with_connection(|connection| {
            self.checkpoint
                .lock()
                .before_snapshot(connection, &self.wal_path)
        })?;
        let reader = self
            .snapshot_cache
            .lock()
            .capture_reader(&self.path)
            .map_err(|error| Error::Branch(error.to_string()))?;
        reader.with_reader(|connection| connection.authorizer(Some(authorize_public)))?;
        Ok(reader)
    }

    fn lease_rowids(&self) -> Result<rowid::RowidLease> {
        self.owner
            .with_savepoint("__multilite__rowid_lease", rowid::lease)
    }
}
