//! Managed read snapshots and transaction-bound prepared statements.

use homebase_client::ServerHandle;
use rusqlite::{Connection, Row};

use super::{Database, DatabaseRuntime, QueryTable, sql};
use crate::branch::snapshot::PinnedReader;
use crate::{Error, Params, Result, Value};

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

    pub(super) fn query_table_prevalidated<P: Params>(
        &self,
        sql: &str,
        params: P,
    ) -> Result<QueryTable> {
        let mut statement = TransactionStatement::new_prevalidated(self.connection, sql)?;
        statement.query_table(params)
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
            return Err(Error::StatementModeMismatch);
        }
        Ok(Self { statement })
    }

    /// Number of result columns for this prepared statement.
    pub fn column_count(&self) -> usize {
        self.statement.column_count()
    }

    /// Result-column names in declaration order.
    ///
    /// Anonymous expressions use SQLite's empty name, replaced with `?`.
    pub fn column_names(&self) -> Vec<String> {
        (0..self.column_count())
            .map(|index| match self.statement.column_name(index) {
                Ok(name) if !name.is_empty() => name.to_owned(),
                _ => "?".to_owned(),
            })
            .collect()
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

    /// Execute the query and return owned column names plus `Value` rows.
    pub fn query_table<P: Params>(&mut self, params: P) -> Result<QueryTable> {
        let columns = self.column_names();
        let width = columns.len();
        let rows = self.query_map(params, |row| {
            (0..width)
                .map(|index| row.get::<_, Value>(index))
                .collect::<rusqlite::Result<Vec<_>>>()
        })?;
        Ok(QueryTable { columns, rows })
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
