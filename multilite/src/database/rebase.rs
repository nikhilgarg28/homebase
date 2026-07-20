//! Atomic application of fetched Multilite operations after local push drains.

use homebase_client::meta::MetaStore;
use homebase_client::{ClientError, ServerHandle};
use pollster::block_on;
use rusqlite::Connection;

use super::catalog;
use super::operation::MultiliteOp;
use super::store::DatabaseMetaStore;
use super::{Database, DatabaseRuntime};
use crate::runtime::ExecutionMode;
use crate::{Error, Result};

impl<H: ServerHandle + Send + Sync + 'static> Database<H> {
    pub(crate) fn rebase(&self, runtime: &DatabaseRuntime) -> Result<()> {
        let _operation = super::lock(&self.operation);
        self.rebase_locked(runtime)?;
        self.policy.mark_rebased();
        Ok(())
    }

    pub fn rebase_locked(&self, runtime: &DatabaseRuntime) -> Result<()> {
        self.rebase_inner(runtime, || Ok(()))
    }

    #[cfg(test)]
    pub(super) fn rebase_after_snapshot(
        &self,
        runtime: &DatabaseRuntime,
        after_snapshot: impl FnOnce() -> Result<()>,
    ) -> Result<()> {
        let _operation = super::lock(&self.operation);
        self.rebase_inner(runtime, after_snapshot)
    }

    fn rebase_inner(
        &self,
        runtime: &DatabaseRuntime,
        after_snapshot: impl FnOnce() -> Result<()>,
    ) -> Result<()> {
        let space_id = self.database_id.space_id();
        let store = DatabaseMetaStore::new(self.owner.clone());
        let (initial_submit, initial_admits) = block_on(async {
            let submit = store.oplog_cursors(space_id).await?;
            let admits = store.admit_cursors(space_id).await?;
            Ok::<_, homebase_core::storage::StorageError>((submit, admits))
        })?;
        if initial_submit.neck != initial_submit.tail {
            return Err(Error::RebasePendingSubmissions);
        }

        let admit_range = initial_admits.neck..initial_admits.tail;
        let batches = block_on(async {
            let space = self.client.space(self.database_id.space_id()).await?;
            space
                .admits()
                .iter(admit_range.clone())
                .await
                .map_err(ClientError::from)
        })?;

        let operations = batches
            .into_iter()
            .map(|batch| {
                if batch.entries.is_empty() {
                    return Ok(None);
                }
                let operation = MultiliteOp::from_homebase(&batch)?;
                Ok(Some((batch.device, operation)))
            })
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();

        after_snapshot()?;

        let apply_to = admit_range.end;
        let local_device = self.client.device();
        runtime.run(ExecutionMode::RemoteApply, |connection| {
            runtime.with_internal_metadata(|| {
                let current_submit = block_on(store.oplog_cursors(space_id))?;
                let current_admits = block_on(store.admit_cursors(space_id))?;
                if current_submit != initial_submit || current_admits != initial_admits {
                    return Err(Error::RebaseStateChanged);
                }
                Ok(())
            })?;

            for (device, operation) in &operations {
                apply_operation(connection, operation, *device == local_device)?;
            }

            runtime.with_internal_metadata(|| {
                block_on(store.mark_admits_applied(space_id, apply_to))?;
                Ok(())
            })?;
            Ok(())
        })?;
        Ok(())
    }
}

fn apply_operation(
    connection: &Connection,
    operation: &MultiliteOp,
    originated_locally: bool,
) -> Result<()> {
    // Local operations were materialized atomically before their successful
    // push. Rebase requires an empty submit log, so their SQLite effects are
    // already durable and must not be replayed.
    if originated_locally {
        return Ok(());
    }

    match operation {
        MultiliteOp::CreateTable(created) => {
            connection.execute(created.sql(), ())?;
            catalog::insert(connection, created)?;
            Ok(())
        }
        MultiliteOp::InsertRows(inserted) => inserted.apply(connection),
    }
}
