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
    joined_dispositions: bool,
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
            joined_dispositions: false,
        }
    }

    pub fn with_database(owner: ConnectionOwner, canonical: CanonicalRouter) -> Self {
        Self {
            inner: OrderedMetaStore::new(SqliteOrderedStore::new(owner.clone())),
            owner,
            canonical,
            joined_dispositions: true,
        }
    }

    fn rollback_without_sink(
        &self,
        space: SpaceId,
        to: DeviceSeq,
        expected: Option<OplogCursors>,
    ) -> MultiliteResult<()> {
        self.owner
            .with_savepoint("__multilite__reject", |connection| {
                let current = pollster::block_on(self.inner.oplog_cursors(space))?;
                let already_completed = expected
                    .map(completed_rollback_cursors)
                    .transpose()?
                    .is_some_and(|completed| current == completed);
                if let Some(expected) = expected
                    && current != expected
                    && !already_completed
                {
                    return Err(Error::StalePushRejection);
                }

                if to >= current.neck && to < current.tail {
                    let through = DeviceSeq(
                        current
                            .tail
                            .0
                            .checked_sub(1)
                            .ok_or(Error::InvalidDatabase("submit tail cannot be zero"))?,
                    );
                    let active =
                        pollster::block_on(self.inner.oplog(space, current.neck, through))?;
                    if let Some(repair) = pending::prepare_rejection(connection, &active)? {
                        repair.apply(connection)?;
                    }
                }

                match expected {
                    Some(expected) => {
                        pollster::block_on(self.inner.rollback_if_unchanged(space, to, expected))?
                    }
                    None => pollster::block_on(self.inner.rollback(space, to))?,
                }
                let after = pollster::block_on(self.inner.oplog_cursors(space))?;
                pending::validate_active_from(connection, after.neck)
            })
    }
}

fn completed_rollback_cursors(expected: OplogCursors) -> MultiliteResult<OplogCursors> {
    Ok(OplogCursors {
        head: expected.head,
        neck: expected.tail,
        tail: DeviceSeq(
            expected
                .tail
                .0
                .checked_add(1)
                .ok_or_else(|| Error::CommitConflict("submit tail is exhausted".into()))?,
        ),
    })
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
        if !self.joined_dispositions {
            return self.inner.rollback(space, to).await;
        }
        if let Some(sink) = self.canonical.sink() {
            let expected = self.inner.oplog_cursors(space).await?;
            let proposal =
                CommitProposal::reject_submissions(to, expected).map_err(storage_error)?;
            return sink.propose(proposal).map_err(storage_error);
        }
        self.rollback_without_sink(space, to, None)
            .map_err(storage_error)
    }

    async fn rollback_if_unchanged(
        &self,
        space: SpaceId,
        to: DeviceSeq,
        expected: OplogCursors,
    ) -> Result<(), StorageError> {
        if !self.joined_dispositions {
            return self.inner.rollback_if_unchanged(space, to, expected).await;
        }
        if let Some(sink) = self.canonical.sink() {
            let proposal =
                CommitProposal::reject_submissions(to, expected).map_err(storage_error)?;
            return sink.propose(proposal).map_err(storage_error);
        }
        self.rollback_without_sink(space, to, Some(expected))
            .map_err(storage_error)
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
    use homebase_client::meta::{SubmitMode, conformance};
    use homebase_core::seal::Seal;
    use homebase_core::space::SpaceId;
    use homebase_core::tag::{CipherEpoch, DeviceEntry, DeviceId, DeviceTag, OpaqueValue};

    use super::*;
    use crate::database::catalog;
    use crate::database::operation::MultiliteOp;
    use crate::database::schema::CreateTable;
    use crate::database::sql::ValidatedExecute;
    use crate::database::transaction::MultiliteTransaction;

    #[test]
    fn joined_store_passes_homebase_conformance() {
        let owner = ConnectionOwner::open_in_memory().unwrap();
        SqliteOrderedStore::initialize(&owner).unwrap();
        owner.with_connection(pending::initialize).unwrap();

        pollster::block_on(conformance::run_all(&DatabaseMetaStore::new(owner)));
    }

    #[test]
    fn no_sink_rollbacks_join_inverse_effects_pending_rows_and_cursors() {
        for guarded in [false, true] {
            let owner = ConnectionOwner::open_in_memory().unwrap();
            SqliteOrderedStore::initialize(&owner).unwrap();
            owner.with_connection(pending::initialize).unwrap();
            owner.with_connection(catalog::initialize).unwrap();
            let canonical = CanonicalRouter::default();
            let store = DatabaseMetaStore::with_database(owner.clone(), canonical);
            let space = SpaceId([7; 16]);
            let sql = "CREATE TABLE notes (id INTEGER PRIMARY KEY)";
            let ValidatedExecute::CreateTable(spec) =
                super::super::sql::validate_execute(sql).unwrap()
            else {
                unreachable!()
            };
            let created = CreateTable::new(sql, spec);
            let transaction =
                MultiliteTransaction::new(vec![MultiliteOp::CreateTable(created.clone())]).unwrap();
            let (mutations, _) = transaction.to_homebase().unwrap().into_parts();
            let reserved = pollster::block_on(store.reserve_commit(
                space,
                mutations.len(),
                Vec::new(),
                SubmitMode::Checked,
            ))
            .unwrap();
            let entries = mutations
                .into_iter()
                .zip(&reserved.versions)
                .map(|(mutation, ver)| DeviceEntry {
                    mutation: match mutation {
                        homebase_core::tag::Mutation::Set { key, value } => {
                            homebase_core::tag::Mutation::Set {
                                key,
                                value: OpaqueValue(value),
                            }
                        }
                        homebase_core::tag::Mutation::Delete { key } => {
                            homebase_core::tag::Mutation::Delete { key }
                        }
                        homebase_core::tag::Mutation::DeleteRange { range } => {
                            homebase_core::tag::Mutation::DeleteRange { range }
                        }
                    },
                    tag: DeviceTag {
                        device: DeviceId([3; 16]),
                        device_seq: reserved.seq,
                        ver: *ver,
                        cipher_epoch: CipherEpoch(0),
                    },
                    seal: Seal::empty_aead_v1(),
                })
                .collect();
            let committed = pollster::block_on(store.commit(space, reserved, entries)).unwrap();
            owner
                .with_savepoint("__multilite__speculate", |connection| {
                    connection.execute(&created.materialization_sql(connection)?, ())?;
                    catalog::insert(connection, &created)?;
                    pending::insert(connection, committed.seq, &transaction)
                })
                .unwrap();
            let expected = pollster::block_on(store.oplog_cursors(space)).unwrap();

            if guarded {
                pollster::block_on(store.rollback_if_unchanged(space, committed.seq, expected))
                    .unwrap();
                pollster::block_on(store.rollback_if_unchanged(space, committed.seq, expected))
                    .unwrap();
            } else {
                pollster::block_on(store.rollback(space, committed.seq)).unwrap();
                pollster::block_on(store.rollback(space, committed.seq)).unwrap();
            }

            owner
                .with_connection(|connection| {
                    assert!(pending::load(connection)?.is_empty());
                    assert!(
                        !catalog::is_initialized(connection)? || {
                            catalog::by_name(connection, "notes")?.is_none()
                        }
                    );
                    let exists = connection.query_row(
                        "SELECT EXISTS(
                            SELECT 1 FROM sqlite_schema
                            WHERE type = 'table' AND name = 'notes'
                        )",
                        (),
                        |row| row.get::<_, bool>(0),
                    )?;
                    assert!(!exists);
                    Ok::<(), Error>(())
                })
                .unwrap();
            let after = pollster::block_on(store.oplog_cursors(space)).unwrap();
            assert_eq!(after.neck, expected.tail);
            assert_eq!(after.tail, DeviceSeq(expected.tail.0 + 1));
        }
    }
}
