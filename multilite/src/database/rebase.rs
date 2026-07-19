//! Atomic application of fetched Multilite operations after local push drains.

use homebase_client::meta::MetaStore;
use homebase_client::{ClientError, ServerHandle};
use pollster::block_on;
use rusqlite::Connection;

use super::operation::MultiliteOp;
use super::store::DatabaseMetaStore;
use super::{Database, DatabaseRuntime};
use crate::runtime::{ExecutionMode, HookPolicy};
use crate::{Error, Result};

impl<H: ServerHandle> Database<H> {
    pub(crate) fn rebase<P: HookPolicy>(&self, runtime: &DatabaseRuntime<P>) -> Result<()> {
        self.rebase_inner(runtime, || Ok(()))
    }

    #[cfg(test)]
    pub(super) fn rebase_after_snapshot<P: HookPolicy>(
        &self,
        runtime: &DatabaseRuntime<P>,
        after_snapshot: impl FnOnce() -> Result<()>,
    ) -> Result<()> {
        self.rebase_inner(runtime, after_snapshot)
    }

    fn rebase_inner<P: HookPolicy>(
        &self,
        runtime: &DatabaseRuntime<P>,
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
                let operation = MultiliteOp::from_homebase(&batch)?;
                Ok((batch.device, operation))
            })
            .collect::<Result<Vec<_>>>()?;

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
            Ok(())
        }
    }
}
