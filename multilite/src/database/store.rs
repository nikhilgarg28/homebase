//! Homebase metadata transitions joined to Multilite's local disposition log.

use std::path::PathBuf;

use homebase_client::meta::{
    AdmitCursors, ClientState, CodecRecord, Committed, DeviceOp, HeldLease, MetaStore,
    OplogCursors, OrderedMetaStore, ReservedCommit, SubmitMode,
};
use homebase_core::clock::Timestamp;
use homebase_core::key::Key;
use homebase_core::lease::LeaseId;
use homebase_core::messages::{AdmittedBatch, PullResponse, RangeAssert};
use homebase_core::space::SpaceId;
use homebase_core::storage::StorageError;
use homebase_core::tag::{DeviceChecksum, DeviceEntry, DeviceId, DeviceSeq};

use super::pending;
use crate::Error;
use crate::branch::snapshot::PinnedSnapshot;
use crate::branch::{OverlayOptions, WritableBranch};
use crate::commit::committer::CommitHistory;
use crate::connection::ConnectionOwner;
use crate::metastore::SqliteOrderedStore;

/// Homebase metadata whose acknowledged submit trim also finalizes Multilite.
pub struct DatabaseMetaStore {
    owner: ConnectionOwner,
    inner: OrderedMetaStore<SqliteOrderedStore>,
    commit_history: CommitHistory,
    branch_files: Option<(PathBuf, PathBuf)>,
}

impl DatabaseMetaStore {
    #[cfg(test)]
    pub fn new(owner: ConnectionOwner) -> Self {
        Self::with_history(owner, CommitHistory::default())
    }

    pub fn with_history(owner: ConnectionOwner, commit_history: CommitHistory) -> Self {
        Self {
            inner: OrderedMetaStore::new(SqliteOrderedStore::new(owner.clone())),
            owner,
            commit_history,
            branch_files: None,
        }
    }

    pub fn with_database(
        owner: ConnectionOwner,
        commit_history: CommitHistory,
        database_path: PathBuf,
        wal_path: PathBuf,
    ) -> Self {
        Self {
            inner: OrderedMetaStore::new(SqliteOrderedStore::new(owner.clone())),
            owner,
            commit_history,
            branch_files: Some((database_path, wal_path)),
        }
    }
}

impl MetaStore for DatabaseMetaStore {
    async fn load(&self) -> Result<ClientState, StorageError> {
        self.inner.load().await
    }

    async fn oplog(
        &self,
        space: SpaceId,
        from: DeviceSeq,
        through: DeviceSeq,
    ) -> Result<Vec<(DeviceSeq, DeviceOp)>, StorageError> {
        self.inner.oplog(space, from, through).await
    }

    async fn oplog_cursors(&self, space: SpaceId) -> Result<OplogCursors, StorageError> {
        self.inner.oplog_cursors(space).await
    }

    async fn admit_cursors(&self, space: SpaceId) -> Result<AdmitCursors, StorageError> {
        self.inner.admit_cursors(space).await
    }

    async fn admitted_batches(
        &self,
        space: SpaceId,
        from: homebase_core::tag::AdmissionSeq,
        through: homebase_core::tag::AdmissionSeq,
    ) -> Result<Vec<AdmittedBatch>, StorageError> {
        self.inner.admitted_batches(space, from, through).await
    }

    async fn leases_covering(
        &self,
        space: SpaceId,
        prefixes: &[Key],
    ) -> Result<Vec<HeldLease>, StorageError> {
        self.inner.leases_covering(space, prefixes).await
    }

    async fn record_device(&self, id: DeviceId) -> Result<(), StorageError> {
        self.inner.record_device(id).await
    }

    async fn reserve_commit(
        &self,
        space: SpaceId,
        mutation_count: usize,
        range_asserts: Vec<RangeAssert>,
        submit_mode: SubmitMode,
    ) -> Result<ReservedCommit, StorageError> {
        self.inner
            .reserve_commit(space, mutation_count, range_asserts, submit_mode)
            .await
    }

    async fn commit(
        &self,
        space: SpaceId,
        reserved: ReservedCommit,
        entries: Vec<DeviceEntry>,
    ) -> Result<Committed, StorageError> {
        self.inner.commit(space, reserved, entries).await
    }

    async fn trim_oplog(
        &self,
        space: SpaceId,
        through: DeviceSeq,
        checksum: DeviceChecksum,
    ) -> Result<(), StorageError> {
        self.owner
            .with_savepoint("__multilite__ack", |connection| {
                pollster::block_on(self.inner.trim_oplog(space, through, checksum))?;
                pending::accept_through(connection, through)?;
                Ok(())
            })
            .map_err(storage_error)
    }

    async fn rollback(&self, space: SpaceId, to: DeviceSeq) -> Result<(), StorageError> {
        self.inner.rollback(space, to).await
    }

    async fn rollback_if_unchanged(
        &self,
        space: SpaceId,
        to: DeviceSeq,
        expected: OplogCursors,
    ) -> Result<(), StorageError> {
        let current = self.inner.oplog_cursors(space).await?;
        let repair = if current == expected {
            let through = DeviceSeq(
                expected
                    .tail
                    .0
                    .checked_sub(1)
                    .ok_or_else(|| StorageError("submit tail cannot be zero".into()))?,
            );
            let active = self.inner.oplog(space, expected.neck, through).await?;
            self.owner
                .with_connection(|connection| pending::prepare_rejection(connection, &active))
                .map_err(storage_error)?
        } else {
            None
        };

        if let (Some(repair), Some((database_path, wal_path))) =
            (&repair, self.branch_files.as_ref())
        {
            let snapshot = PinnedSnapshot::capture(database_path, wal_path)
                .map_err(|error| StorageError(error.to_string()))?;
            let branch = WritableBranch::open(snapshot, OverlayOptions::default())
                .map_err(|error| StorageError(error.to_string()))?;
            repair.apply(branch.connection()).map_err(storage_error)?;
        }

        self.owner
            .with_savepoint("__multilite__rollback", |connection| {
                let current = pollster::block_on(self.inner.oplog_cursors(space))?;
                if current == expected
                    && let Some(repair) = &repair
                {
                    repair.apply(connection)?;
                    self.commit_history
                        .record(connection, repair.writes().to_vec())?;
                }
                pollster::block_on(self.inner.rollback_if_unchanged(space, to, expected))?;
                Ok(())
            })
            .map_err(storage_error)
    }

    async fn append_admits(
        &self,
        space: SpaceId,
        response: &PullResponse,
    ) -> Result<(), StorageError> {
        self.inner.append_admits(space, response).await
    }

    async fn mark_admits_applied(
        &self,
        space: SpaceId,
        to: homebase_core::tag::AdmissionSeq,
    ) -> Result<(), StorageError> {
        self.inner.mark_admits_applied(space, to).await
    }

    async fn trim_admits(
        &self,
        space: SpaceId,
        to: homebase_core::tag::AdmissionSeq,
    ) -> Result<(), StorageError> {
        self.inner.trim_admits(space, to).await
    }

    async fn record_clock(&self, high: Timestamp) -> Result<(), StorageError> {
        self.inner.record_clock(high).await
    }

    async fn record_leases(
        &self,
        space: SpaceId,
        leases: &[HeldLease],
    ) -> Result<(), StorageError> {
        self.inner.record_leases(space, leases).await
    }

    async fn reconcile_leases(
        &self,
        space: SpaceId,
        leases: &[HeldLease],
    ) -> Result<(), StorageError> {
        self.inner.reconcile_leases(space, leases).await
    }

    async fn forget_leases(&self, space: SpaceId, ids: &[LeaseId]) -> Result<(), StorageError> {
        self.inner.forget_leases(space, ids).await
    }

    async fn drop_leases(&self, space: SpaceId, ids: &[LeaseId]) -> Result<(), StorageError> {
        self.inner.drop_leases(space, ids).await
    }

    async fn record_codec(&self, space: SpaceId, record: &CodecRecord) -> Result<(), StorageError> {
        self.inner.record_codec(space, record).await
    }
}

fn storage_error(error: Error) -> StorageError {
    match error {
        Error::Storage(error) => error,
        other => StorageError(format!("Multilite metadata transition: {other}")),
    }
}

#[cfg(test)]
mod tests {
    use homebase_client::meta::conformance;

    use super::*;

    #[test]
    fn joined_store_passes_homebase_conformance() {
        let owner = ConnectionOwner::open_in_memory().unwrap();
        SqliteOrderedStore::initialize(&owner).unwrap();
        owner.with_connection(pending::initialize).unwrap();

        pollster::block_on(conformance::run_all(&DatabaseMetaStore::new(owner)));
    }
}
