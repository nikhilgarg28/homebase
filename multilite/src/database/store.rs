//! Homebase metadata transitions joined to Multilite's local disposition log.

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
use crate::connection::ConnectionOwner;
use crate::metastore::SqliteOrderedStore;

/// Homebase metadata whose acknowledged submit trim also finalizes Multilite.
pub struct DatabaseMetaStore {
    owner: ConnectionOwner,
    inner: OrderedMetaStore<SqliteOrderedStore>,
}

impl DatabaseMetaStore {
    pub fn new(owner: ConnectionOwner) -> Self {
        Self {
            inner: OrderedMetaStore::new(SqliteOrderedStore::new(owner.clone())),
            owner,
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
        self.owner
            .with_savepoint("__multilite__rollback", |connection| {
                let current = pollster::block_on(self.inner.oplog_cursors(space))?;
                if current == expected {
                    let through = DeviceSeq(
                        expected
                            .tail
                            .0
                            .checked_sub(1)
                            .ok_or(Error::InvalidDatabase("submit tail cannot be zero"))?,
                    );
                    let active =
                        pollster::block_on(self.inner.oplog(space, expected.neck, through))?;
                    pending::reject_active(connection, &active)?;
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
