//! Managed read snapshots and transaction-bound prepared statements.

use homebase_client::ServerHandle;
use rusqlite::{Connection, Row};

use super::{Database, DatabaseRuntime, sql};
use crate::branch::snapshot::PinnedReader;
use crate::{Error, Params, Result};

/// One managed, read-only SQLite snapshot.
pub struct ViewTransaction<'a> {
    connection: &'a Connection,
}

impl<'a> ViewTransaction<'a> {
    fn new(connection: &'a Connection) -> Self {
        Self { connection }
    }

    /// Execute a read-only statement and eagerly map every result row.
    pub fn query<T, P, F>(&self, sql: &str, params: P, map: F) -> Result<Vec<T>>
    where
        P: Params,
        F: FnMut(&Row<'_>) -> rusqlite::Result<T>,
    {
        let mut statement = self.prepare(sql)?;
        statement.query_map(params, map)
    }

    pub(super) fn query_prevalidated<T, P, F>(&self, sql: &str, params: P, map: F) -> Result<Vec<T>>
    where
        P: Params,
        F: FnMut(&Row<'_>) -> rusqlite::Result<T>,
    {
        let mut statement = TransactionStatement::new_prevalidated(self.connection, sql)?;
        statement.query_map(params, map)
    }

    /// Alias matching rusqlite's mapped-query vocabulary.
    pub fn query_map<T, P, F>(&self, sql: &str, params: P, map: F) -> Result<Vec<T>>
    where
        P: Params,
        F: FnMut(&Row<'_>) -> rusqlite::Result<T>,
    {
        self.query(sql, params, map)
    }

    /// Prepare one read-only statement bound to this managed snapshot.
    pub fn prepare(&self, sql: &str) -> Result<TransactionStatement<'a>> {
        TransactionStatement::new(self.connection, sql)
    }
}

/// A read-only prepared statement that cannot outlive its managed transaction.
pub struct TransactionStatement<'a> {
    statement: rusqlite::Statement<'a>,
}

impl<'a> TransactionStatement<'a> {
    pub(crate) fn new(connection: &'a Connection, sql: &str) -> Result<Self> {
        sql::validate_read_statement(sql)?;
        Self::new_prevalidated(connection, sql)
    }

    pub(super) fn new_prevalidated(connection: &'a Connection, sql: &str) -> Result<Self> {
        let statement = connection.prepare(sql)?;
        if !statement.readonly() {
            return Err(Error::PreparedWrite);
        }
        Ok(Self { statement })
    }

    /// Execute the query and eagerly map every row in the managed snapshot.
    pub fn query_map<T, P, F>(&mut self, params: P, map: F) -> Result<Vec<T>>
    where
        P: Params,
        F: FnMut(&Row<'_>) -> rusqlite::Result<T>,
    {
        self.statement
            .query_map(params, map)?
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(Into::into)
    }
}

pub(super) fn run_branch_view<T>(
    snapshot: PinnedReader,
    operation: impl FnOnce(&ViewTransaction<'_>) -> Result<T>,
) -> Result<T> {
    snapshot.with_reader(|reader| operation(&ViewTransaction::new(reader)))
}

impl<H: ServerHandle + Send + Sync + 'static> Database<H> {
    /// Refresh once, then run a closure inside one read-only SQLite snapshot.
    pub fn view<T>(
        self: &std::sync::Arc<Self>,
        runtime: &DatabaseRuntime,
        operation: impl FnOnce(&ViewTransaction<'_>) -> Result<T>,
    ) -> Result<T> {
        self.refresh_read(runtime)?;
        let snapshot = self.issue_view_snapshot()?;
        run_branch_view(snapshot, operation)
    }
}
