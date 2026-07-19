//! Snapshot validation and atomic application of fetched Multilite operations.

use std::ops::Range as SeqRange;

use homebase_client::meta::{AdmitCursors, MetaStore, OplogCursors};
use homebase_client::{ClientError, RebaseConflict as HomebaseRebaseConflict, ServerHandle};
use homebase_core::tag::{AdmissionSeq, DeviceId};
use pollster::block_on;
use rusqlite::{Connection, OptionalExtension};

use super::operation::MultiliteOp;
use super::store::DatabaseMetaStore;
use super::{Database, DatabaseId, DatabaseRuntime};
use crate::runtime::{ExecutionMode, HookPolicy};
use crate::{Error, Result};

/// Opaque record of conflicts against one observed submit/admit snapshot.
///
/// A later rollback will validate this identity and both cursor snapshots
/// before changing local state. Receiving the handle performs no repair.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RebaseConflict {
    pub(super) database_id: DatabaseId,
    pub(super) device_id: DeviceId,
    pub(super) submit_cursors: OplogCursors,
    pub(super) admit_cursors: AdmitCursors,
    pub(super) admit_range: SeqRange<AdmissionSeq>,
    conflicts: Vec<HomebaseRebaseConflict>,
}

impl RebaseConflict {
    /// Per-submission range-assert failures found by Homebase.
    pub fn conflicts(&self) -> &[HomebaseRebaseConflict] {
        &self.conflicts
    }

    /// First admission sequence included in this analysis.
    pub fn admitted_from(&self) -> u64 {
        self.admit_range.start.0
    }

    /// Exclusive end of the analyzed admission interval.
    pub fn admitted_to_exclusive(&self) -> u64 {
        self.admit_range.end.0
    }
}

impl<H: ServerHandle> Database<H> {
    pub(crate) fn rebase<P: HookPolicy>(&self, runtime: &DatabaseRuntime<P>) -> Result<()> {
        self.rebase_inner(runtime, || Ok(()))
    }

    #[cfg(test)]
    pub(super) fn rebase_after_plan<P: HookPolicy>(
        &self,
        runtime: &DatabaseRuntime<P>,
        after_plan: impl FnOnce() -> Result<()>,
    ) -> Result<()> {
        self.rebase_inner(runtime, after_plan)
    }

    fn rebase_inner<P: HookPolicy>(
        &self,
        runtime: &DatabaseRuntime<P>,
        after_plan: impl FnOnce() -> Result<()>,
    ) -> Result<()> {
        let (initial_admits, admit_range, batches, analysis) = block_on(async {
            let space = self.client.space(self.database_id.space_id()).await?;
            let initial_admits = space.admits().cursors().await.map_err(ClientError::from)?;
            let admit_range = initial_admits.neck..initial_admits.tail;
            let batches = space
                .admits()
                .iter(admit_range.clone())
                .await
                .map_err(ClientError::from)?;
            let analysis = space
                .analyze_rebase(admit_range.clone())
                .await
                .map_err(ClientError::from)?;
            Ok::<_, ClientError>((initial_admits, admit_range, batches, analysis))
        })?;

        if analysis.admit_cursors != initial_admits || analysis.admit_range != admit_range {
            return Err(Error::RebaseStateChanged);
        }

        let operations = batches
            .into_iter()
            .map(|batch| {
                let operation = MultiliteOp::from_homebase(&batch)?;
                Ok((batch.device, operation))
            })
            .collect::<Result<Vec<_>>>()?;

        if !analysis.is_clean() {
            return Err(Error::RebaseConflict(RebaseConflict {
                database_id: self.database_id,
                device_id: self.client.device(),
                submit_cursors: analysis.submit_cursors,
                admit_cursors: analysis.admit_cursors,
                admit_range: analysis.admit_range,
                conflicts: analysis.conflicts,
            }));
        }

        after_plan()?;

        let expected_submit = analysis.submit_cursors;
        let expected_admits = analysis.admit_cursors;
        let apply_to = admit_range.end;
        let space_id = self.database_id.space_id();
        let local_device = self.client.device();
        let store = DatabaseMetaStore::new(self.owner.clone());
        runtime.run(ExecutionMode::RemoteApply, |connection| {
            runtime.with_internal_metadata(|| {
                let current_submit = block_on(store.oplog_cursors(space_id))?;
                let current_admits = block_on(store.admit_cursors(space_id))?;
                if current_submit != expected_submit || current_admits != expected_admits {
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
    match operation {
        MultiliteOp::CreateTable(created) if originated_locally => {
            let materialized = connection
                .query_row(
                    "SELECT sql FROM main.sqlite_schema
                     WHERE type = 'table' AND name = ?1 COLLATE NOCASE",
                    [created.table_name()],
                    |row| row.get::<_, String>(0),
                )
                .optional()?;
            if !materialized.is_some_and(|sql| created.matches_sql(&sql)) {
                return Err(Error::InvalidDatabase(
                    "accepted local CREATE TABLE does not match SQLite schema",
                ));
            }
            Ok(())
        }
        MultiliteOp::CreateTable(created) => {
            connection.execute(created.sql(), ())?;
            Ok(())
        }
    }
}
