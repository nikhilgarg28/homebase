//! Managed read snapshots and transaction-bound prepared statements.

use homebase_client::ServerHandle;
use rusqlite::{Connection, Row};

use super::isolation::ReadTrace;
use super::sql::VTabReadPlan;
use super::{Database, DatabaseRuntime, sql, vtab};
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
        TransactionStatement::new(self.runtime, self.connection, sql, None)
    }
}

/// A read-only prepared statement that cannot outlive its managed transaction.
pub struct TransactionStatement<'a> {
    runtime: &'a DatabaseRuntime,
    connection: &'a Connection,
    read_trace: Option<ReadTrace>,
    vtab_read_plan: Option<VTabReadPlan>,
    sql: String,
}

impl<'a> TransactionStatement<'a> {
    pub(super) fn new(
        runtime: &'a DatabaseRuntime,
        connection: &'a Connection,
        sql: &str,
        read_trace: Option<ReadTrace>,
    ) -> Result<Self> {
        sql::validate_managed_statement(sql)?;
        let vtab_read_plan = read_trace
            .as_ref()
            .map(|_| sql::plan_vtab_read(sql))
            .transpose()?
            .flatten();
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
            read_trace,
            vtab_read_plan,
            sql: sql.to_owned(),
        })
    }

    /// Execute the query and eagerly map every row in the managed snapshot.
    pub fn query_map<T, P, F>(&mut self, params: P, map: F) -> Result<Vec<T>>
    where
        P: Params,
        F: FnMut(&Row<'_>) -> rusqlite::Result<T>,
    {
        let (sql, mode) = match &self.vtab_read_plan {
            Some(plan) => {
                let trace = self
                    .read_trace
                    .as_ref()
                    .expect("read plans carry an update trace")
                    .clone();
                let ((), events) = self
                    .runtime
                    .run(ExecutionMode::InternalMetadata, |connection| {
                        vtab::install(connection, plan, trace)
                    })?;
                ensure_read_only_events(events)?;
                (&plan.rewritten_sql, ExecutionMode::InternalMetadata)
            }
            None => (&self.sql, ExecutionMode::Public),
        };
        let expected_connection = self.connection;
        let (rows, events) = self.runtime.run(mode, |connection| {
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
        ensure_read_only_events(events)?;
        Ok(rows)
    }
}

fn ensure_read_only_events(events: Vec<super::row::CapturedRow>) -> Result<()> {
    if events.is_empty() {
        Ok(())
    } else {
        Err(Error::CaptureInvariant(
            "executing a read-only statement captured row changes",
        ))
    }
}

impl<H: ServerHandle + Send + Sync + 'static> Database<H> {
    /// Refresh once, then run a closure inside one read-only SQLite snapshot.
    pub fn view<T>(
        &self,
        runtime: &DatabaseRuntime,
        operation: impl FnOnce(&ViewTransaction<'_>) -> Result<T>,
    ) -> Result<T> {
        let _operation = self.enter_operation()?;
        self.refresh_read_serial(runtime)?;
        self.owner
            .with_savepoint("__multilite__view", |connection| {
                operation(&ViewTransaction::new(runtime, connection))
            })
    }
}
