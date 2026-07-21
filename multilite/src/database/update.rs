//! Managed serialized local update execution.

use homebase_client::{ClientError, ServerHandle};
use homebase_core::tag::AdmissionSeq;
use pollster::block_on;
use rusqlite::{Connection, Row};

use super::operation::MultiliteOp;
use super::row::InsertRows;
use super::sql::ValidatedExecute;
use super::transaction::MultiliteTransaction;
use super::view::TransactionStatement;
use super::{
    Database, DatabaseRuntime, IsolationLevel, SyncPolicy, UpdateOptions, catalog, lock, pending,
    pin_snapshot,
};
use crate::runtime::ExecutionMode;
use crate::{Error, Params, Result};

/// One serialized SQLite update accumulating a single durable transaction.
///
/// The database operation lock and outer SQLite savepoint are owned by
/// `Database::update`. Individual statements use nested runtime
/// savepoints only to attribute hook events; no Homebase state is submitted
/// until the complete operation list has succeeded.
pub struct UpdateTransaction<'a, H: ServerHandle> {
    database: &'a Database<H>,
    runtime: &'a DatabaseRuntime,
    connection: &'a Connection,
    authority_frontier: AdmissionSeq,
    isolation: IsolationLevel,
    operations: Vec<MultiliteOp>,
}

impl<'a, H: ServerHandle + Send + Sync + 'static> UpdateTransaction<'a, H> {
    fn new(
        database: &'a Database<H>,
        runtime: &'a DatabaseRuntime,
        connection: &'a Connection,
        authority_frontier: AdmissionSeq,
        isolation: IsolationLevel,
    ) -> Self {
        Self {
            database,
            runtime,
            connection,
            authority_frontier,
            isolation,
            operations: Vec::new(),
        }
    }

    /// Isolation level selected for this managed update.
    pub fn isolation_level(&self) -> IsolationLevel {
        self.isolation
    }

    /// Execute one supported mutating statement inside this update.
    pub fn execute<Q: Params>(&mut self, sql: &str, params: Q) -> Result<usize> {
        super::sql::validate_managed_statement(sql)?;
        let validated = super::sql::validate_execute(sql)?;
        self.execute_validated(sql, params, validated)
    }

    /// Execute a read-only statement against this update's current snapshot.
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

    /// Prepare one read-only statement bound to this managed update.
    pub fn prepare(&self, sql: &str) -> Result<TransactionStatement<'a>> {
        TransactionStatement::new(self.runtime, self.connection, sql)
    }

    /// Execute one statement validated before the outer update began.
    pub(super) fn execute_validated<Q: Params>(
        &mut self,
        sql: &str,
        params: Q,
        validated: ValidatedExecute,
    ) -> Result<usize> {
        match validated {
            ValidatedExecute::CreateTable(table) => self.execute_create_table(sql, params, table),
            ValidatedExecute::Insert => self.execute_insert(sql, params),
        }
    }

    fn execute_create_table<Q: Params>(
        &mut self,
        sql: &str,
        params: Q,
        table: super::schema::CreateTableSpec,
    ) -> Result<usize> {
        let operation = MultiliteOp::create_table(sql, table);
        let MultiliteOp::CreateTable(created) = &operation else {
            unreachable!("create-table constructor returned another operation")
        };
        let (changed, _) = self.runtime.run(ExecutionMode::Public, |connection| {
            let changed = connection.execute(sql, params)?;
            self.runtime
                .with_internal_metadata(|| catalog::insert(connection, created))?;
            Ok(changed)
        })?;
        self.operations.push(operation);
        Ok(changed)
    }

    fn execute_insert<Q: Params>(&mut self, sql: &str, params: Q) -> Result<usize> {
        let (changed, events) = self.runtime.run(ExecutionMode::Public, |connection| {
            Ok(connection.execute(sql, params)?)
        })?;
        let Some(inserted) = InsertRows::from_captured(self.connection, &events)? else {
            if events.is_empty() {
                return Ok(changed);
            }
            return Err(Error::UnsupportedSql(
                "INSERT target has no synchronized schema identity",
            ));
        };
        self.operations.push(MultiliteOp::InsertRows(inserted));
        Ok(changed)
    }

    fn finalize(self) -> Result<()> {
        if self.operations.is_empty() {
            return Ok(());
        }
        let transaction = MultiliteTransaction::new(self.operations)?;
        let (mutations, assertions) = transaction
            .to_homebase()?
            .plan(self.isolation, self.authority_frontier);
        self.runtime.with_internal_metadata(|| {
            let sequence = block_on(async {
                let space = self
                    .database
                    .client
                    .space(self.database.database_id.space_id())
                    .await?;
                let submission = space
                    .submit_unchecked(mutations, assertions)
                    .await
                    .map_err(ClientError::from)?;
                Ok::<_, Error>(submission.seq)
            })?;
            pending::insert(self.connection, sequence, &transaction)
        })
    }
}

impl<H: ServerHandle + Send + Sync + 'static> Database<H> {
    /// Run a complete serialized update in one SQLite and Homebase atomic unit.
    pub fn update<T>(
        &self,
        runtime: &DatabaseRuntime,
        operation: impl FnOnce(&mut UpdateTransaction<'_, H>) -> Result<T>,
    ) -> Result<T> {
        self.update_with(runtime, UpdateOptions::new(self.isolation_level), operation)
    }

    /// Run one managed update with an explicit isolation override.
    pub fn update_with<T>(
        &self,
        runtime: &DatabaseRuntime,
        options: UpdateOptions,
        operation: impl FnOnce(&mut UpdateTransaction<'_, H>) -> Result<T>,
    ) -> Result<T> {
        let _operation = lock(&self.operation);
        self.refresh_read_locked(runtime)?;
        let authority_frontier = self.authority_frontier()?;
        let value = self
            .owner
            .with_savepoint("__multilite__serialized_update", |connection| {
                pin_snapshot(connection)?;
                let mut update = UpdateTransaction::new(
                    self,
                    runtime,
                    connection,
                    authority_frontier,
                    options.isolation_level(),
                );
                let value = operation(&mut update)?;
                update.finalize()?;
                Ok(value)
            })?;

        match self.policy.policy() {
            SyncPolicy::LocalOnly => {}
            SyncPolicy::LocalFirst { write_delay, .. } => self.scheduler.schedule(write_delay),
            SyncPolicy::Remote => self.finish_remote_write()?,
        }
        Ok(value)
    }

    fn authority_frontier(&self) -> Result<AdmissionSeq> {
        block_on(async {
            let space = self.client.space(self.database_id.space_id()).await?;
            let cursors = space.admits().cursors().await.map_err(ClientError::from)?;
            Ok(AdmissionSeq(cursors.neck.0.checked_sub(1).ok_or(
                Error::InvalidDatabase("admit neck cannot be zero"),
            )?))
        })
    }
}
