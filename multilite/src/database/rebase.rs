//! Atomic application of fetched Multilite operations after local push drains.

use std::sync::Arc;

#[cfg(test)]
use homebase_client::ClientError;
use homebase_client::ServerHandle;
#[cfg(test)]
use homebase_client::meta::MetaStore;
use pollster::block_on;
#[cfg(test)]
use rusqlite::Connection;

#[cfg(test)]
use super::store::DatabaseMetaStore;
use super::{Database, DatabaseRuntime};
#[cfg(test)]
use crate::Error;
use crate::Result;
#[cfg(test)]
use crate::commit::proposal::{AdmittedTransaction, CommitProposal};
#[cfg(test)]
use crate::logical::transaction::MultiliteTransaction;

impl<H: ServerHandle + Send + Sync + 'static> Database<H> {
    pub(crate) fn rebase(self: &Arc<Self>, runtime: &DatabaseRuntime) -> Result<()> {
        let _ = runtime;
        block_on(self.rebase_async())
    }

    #[cfg(test)]
    pub(super) fn rebase_after_snapshot(
        self: &Arc<Self>,
        runtime: &DatabaseRuntime,
        after_snapshot: impl FnOnce() -> Result<()>,
    ) -> Result<()> {
        self.rebase_inner(runtime, after_snapshot)
    }

    #[cfg(test)]
    fn rebase_inner(
        self: &Arc<Self>,
        _runtime: &DatabaseRuntime,
        after_snapshot: impl FnOnce() -> Result<()>,
    ) -> Result<()> {
        let space_id = self.database_id.space_id();
        let store = DatabaseMetaStore::read_only(self.owner.clone());
        let (initial_submit, initial_admits) = block_on(async {
            let submit = store.oplog_cursors(space_id).await?;
            let admits = store.admit_cursors(space_id).await?;
            Ok::<_, homebase_core::storage::StorageError>((submit, admits))
        })?;
        if initial_submit.neck != initial_submit.tail {
            return Err(Error::RebasePendingSubmissions);
        }

        let admit_range = initial_admits.neck..initial_admits.tail;
        if admit_range.is_empty() {
            return Ok(());
        }
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
                Ok(Some(AdmittedTransaction {
                    device: batch.device,
                    transaction,
                }))
            })
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();

        after_snapshot()?;

        let proposal = CommitProposal::apply_admissions(
            initial_submit,
            initial_admits,
            admit_range.end,
            self.client.device(),
            transactions,
        )?;
        self.commit_proposal(proposal)?;
        Ok(())
    }
}

#[cfg(test)]
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

    transaction.apply(connection)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog;
    use crate::logical::operation::MultiliteOp;
    use crate::logical::row::{CapturedRow, InsertRows, StoredValue};
    use crate::logical::schema::{CreateColumn, CreateTableSpec, SqlName, TypeDeclaration};

    #[test]
    fn foreign_mixed_transaction_applies_operations_in_manifest_order() {
        let created = MultiliteOp::create_table(
            "CREATE TABLE notes (id INTEGER PRIMARY KEY)",
            CreateTableSpec {
                name: SqlName::new("notes".into()),
                mode: Default::default(),
                storage: crate::logical::schema::TableStorage::Rowid,
                columns: vec![CreateColumn {
                    name: SqlName::new("id".into()),
                    declared_type: TypeDeclaration::integer(),
                    not_null: false,
                    not_null_name: None,
                    default: None,
                    primary_key: Some(0),
                }],
                unique_constraints: Vec::new(),
                foreign_keys: Vec::new(),
                primary_key_name: None,
                checks: Vec::new(),
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
                rowid: 7,
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
