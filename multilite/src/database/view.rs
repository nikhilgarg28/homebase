//! Managed read snapshots and transaction-bound prepared statements.

use homebase_client::ServerHandle;
use rusqlite::{Connection, Row};

use super::{Database, DatabaseRuntime, lock, pin_snapshot, sql};
use crate::runtime::ExecutionMode;
use crate::{Error, Params, Result};

/// One managed, read-only SQLite snapshot.
pub struct ViewTransaction<'a> {
    runtime: &'a DatabaseRuntime,
    connection: &'a Connection,
}

impl<'a> ViewTransaction<'a> {
    fn new(runtime: &'a DatabaseRuntime, connection: &'a Connection) -> Self {
        Self {
            runtime,
            connection,
        }
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
        TransactionStatement::new(self.runtime, self.connection, sql)
    }
}

/// A read-only prepared statement that cannot outlive its managed transaction.
pub struct TransactionStatement<'a> {
    runtime: &'a DatabaseRuntime,
    connection: &'a Connection,
    sql: String,
}

impl<'a> TransactionStatement<'a> {
    pub(super) fn new(
        runtime: &'a DatabaseRuntime,
        connection: &'a Connection,
        sql: &str,
    ) -> Result<Self> {
        sql::validate_managed_statement(sql)?;
        let ((), events) = runtime.run(ExecutionMode::Public, |connection| {
            let statement = connection.prepare(sql)?;
            if statement.readonly() {
                Ok(())
            } else {
                Err(Error::PreparedWrite)
            }
        })?;
        if !events.is_empty() {
            return Err(Error::CaptureInvariant(
                "preparing a read-only statement captured row changes",
            ));
        }
        Ok(Self {
            runtime,
            connection,
            sql: sql.to_owned(),
        })
    }

    /// Execute the query and eagerly map every row in the managed snapshot.
    pub fn query_map<T, P, F>(&mut self, params: P, map: F) -> Result<Vec<T>>
    where
        P: Params,
        F: FnMut(&Row<'_>) -> rusqlite::Result<T>,
    {
        let sql = &self.sql;
        let expected_connection = self.connection;
        let (rows, events) = self.runtime.run(ExecutionMode::Public, |connection| {
            debug_assert!(std::ptr::eq(connection, expected_connection));
            let mut statement = connection.prepare(sql)?;
            if !statement.readonly() {
                return Err(Error::PreparedWrite);
            }
            statement
                .query_map(params, map)?
                .collect::<rusqlite::Result<Vec<_>>>()
                .map_err(Into::into)
        })?;
        if !events.is_empty() {
            return Err(Error::CaptureInvariant(
                "executing a read-only statement captured row changes",
            ));
        }
        Ok(rows)
    }
}

impl<H: ServerHandle + Send + Sync + 'static> Database<H> {
    /// Refresh once, then run a closure inside one read-only SQLite snapshot.
    pub fn view<T>(
        &self,
        runtime: &DatabaseRuntime,
        operation: impl FnOnce(&ViewTransaction<'_>) -> Result<T>,
    ) -> Result<T> {
        let _operation = lock(&self.operation);
        self.refresh_read_locked(runtime)?;
        self.owner
            .with_savepoint("__multilite__view", |connection| {
                pin_snapshot(connection)?;
                operation(&ViewTransaction::new(runtime, connection))
            })
    }
}
