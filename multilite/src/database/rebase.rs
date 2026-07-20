//! Atomic application of fetched Multilite operations after local push drains.

use homebase_client::meta::MetaStore;
use homebase_client::{ClientError, ServerHandle};
use pollster::block_on;
use rusqlite::Connection;

use super::catalog;
use super::operation::MultiliteOp;
use super::store::DatabaseMetaStore;
use super::transaction::MultiliteTransaction;
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

        let transactions = batches
            .into_iter()
            .map(|batch| {
                if batch.entries.is_empty() {
                    return Ok(None);
                }
                let transaction = MultiliteTransaction::from_homebase(&batch)?;
                Ok(Some((batch.device, transaction)))
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

            for (device, transaction) in &transactions {
                apply_transaction(connection, transaction, *device == local_device)?;
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

fn apply_transaction(
    connection: &Connection,
    transaction: &MultiliteTransaction,
    originated_locally: bool,
) -> Result<()> {
    // Local operations were materialized atomically before their successful
    // push. Rebase requires an empty submit log, so their SQLite effects are
    // already durable and must not be replayed.
    if originated_locally {
        return Ok(());
    }

    for operation in transaction.operations() {
        apply_operation(connection, operation)?;
    }
    Ok(())
}

fn apply_operation(connection: &Connection, operation: &MultiliteOp) -> Result<()> {
    match operation {
        MultiliteOp::CreateTable(created) => {
            connection.execute(created.sql(), ())?;
            catalog::insert(connection, created)?;
            Ok(())
        }
        MultiliteOp::InsertRows(inserted) => inserted.apply(connection),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::database::row::{CapturedRow, InsertRows, StoredValue};
    use crate::database::schema::{CreateColumn, CreateTableSpec, DeclaredType, SqlName};

    #[test]
    fn foreign_mixed_transaction_applies_operations_in_manifest_order() {
        let created = MultiliteOp::create_table(
            "CREATE TABLE notes (id INTEGER PRIMARY KEY)",
            CreateTableSpec {
                name: SqlName::new("notes".into()),
                columns: vec![CreateColumn {
                    name: SqlName::new("id".into()),
                    declared_type: DeclaredType::Integer,
                    not_null: false,
                    primary_key: true,
                }],
            },
        );
        let MultiliteOp::CreateTable(table) = &created else {
            unreachable!()
        };
        let source = Connection::open_in_memory().unwrap();
        catalog::initialize(&source).unwrap();
        source.execute(table.sql(), ()).unwrap();
        catalog::insert(&source, table).unwrap();
        let inserted = InsertRows::from_captured(
            &source,
            &[CapturedRow {
                table: "notes".into(),
                values: vec![StoredValue::Integer(7)],
            }],
        )
        .unwrap()
        .unwrap();
        let transaction =
            MultiliteTransaction::new(vec![created, MultiliteOp::InsertRows(inserted)]).unwrap();

        let target = Connection::open_in_memory().unwrap();
        catalog::initialize(&target).unwrap();
        apply_transaction(&target, &transaction, false).unwrap();

        assert_eq!(
            target
                .query_row("SELECT id FROM notes", (), |row| row.get::<_, i64>(0))
                .unwrap(),
            7
        );
        assert!(catalog::by_name(&target, "notes").unwrap().is_some());
    }
}
