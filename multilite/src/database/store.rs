//! Homebase metadata transitions joined to Multilite's local disposition log.

use std::sync::{Arc, OnceLock};

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
use crate::commit::proposal::CommitProposal;
use crate::connection::ConnectionOwner;
use crate::metastore::SqliteOrderedStore;
use crate::{Error, Result as MultiliteResult};

/// Homebase metadata whose acknowledged submit trim also finalizes Multilite.
pub struct DatabaseMetaStore {
    owner: ConnectionOwner,
    inner: OrderedMetaStore<SqliteOrderedStore>,
    canonical: CanonicalRouter,
}

pub trait CanonicalMetaSink: Send + Sync + 'static {
    fn propose(&self, proposal: CommitProposal) -> MultiliteResult<()>;
}

#[derive(Clone, Default)]
pub struct CanonicalRouter {
    sink: Arc<OnceLock<Arc<dyn CanonicalMetaSink>>>,
}

impl CanonicalRouter {
    pub fn install(&self, sink: Arc<dyn CanonicalMetaSink>) -> MultiliteResult<()> {
        self.sink
            .set(sink)
            .map_err(|_| Error::Committer("canonical metadata sink is already installed".into()))
    }

    fn sink(&self) -> Option<&Arc<dyn CanonicalMetaSink>> {
        self.sink.get()
    }
}

impl DatabaseMetaStore {
    #[cfg(test)]
    pub fn new(owner: ConnectionOwner) -> Self {
        Self::read_only(owner)
    }

    pub fn read_only(owner: ConnectionOwner) -> Self {
        Self {
            inner: OrderedMetaStore::new(SqliteOrderedStore::new(owner.clone())),
            owner,
            canonical: CanonicalRouter::default(),
        }
    }

    pub fn with_database(owner: ConnectionOwner, canonical: CanonicalRouter) -> Self {
        Self {
            inner: OrderedMetaStore::new(SqliteOrderedStore::new(owner.clone())),
            owner,
            canonical,
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
        if let Some(sink) = self.canonical.sink() {
            let expected = self.inner.oplog_cursors(space).await?;
            let proposal = CommitProposal::accept_submissions(expected, through, checksum)
                .map_err(storage_error)?;
            return sink.propose(proposal).map_err(storage_error);
        }
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
        if let Some(sink) = self.canonical.sink() {
            let proposal =
                CommitProposal::reject_submissions(to, expected).map_err(storage_error)?;
            return sink.propose(proposal).map_err(storage_error);
        }
        self.inner.rollback_if_unchanged(space, to, expected).await
    }

    async fn append_admits(
        &self,
        space: SpaceId,
        response: &PullResponse,
    ) -> Result<(), StorageError> {
        if let Some(sink) = self.canonical.sink() {
            let expected = self.inner.admit_cursors(space).await?;
            let proposal = CommitProposal::append_admissions(expected, response.clone())
                .map_err(storage_error)?;
            return sink.propose(proposal).map_err(storage_error);
        }
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
