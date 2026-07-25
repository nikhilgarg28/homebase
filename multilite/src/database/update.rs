//! Managed local update execution.

use homebase_client::{ClientError, ServerHandle};
use homebase_core::tag::AdmissionSeq;
use pollster::block_on;
use rusqlite::{Connection, Row};

use super::isolation::ReadTrace;
use super::operation::MultiliteOp;
use super::row::InsertRows;
use super::sql::ValidatedExecute;
use super::transaction::MultiliteTransaction;
use super::view::TransactionStatement;
use super::{
    BranchSnapshot, Database, DatabaseRuntime, IsolationLevel, SyncPolicy, UpdateOptions, catalog,
    pending, pin_snapshot,
};
use crate::branch::changeset::ChangesetCapture;
use crate::branch::{OverlayOptions, WritableBranch};
use crate::commit::proposal::CommitProposal;
use crate::runtime::ExecutionMode;
use crate::{Error, Params, Result};

enum UpdateBackend<'a, H: ServerHandle> {
    Branch {
        connection: &'a Connection,
    },
    Serialized {
        database: &'a Database<H>,
        runtime: &'a DatabaseRuntime,
        connection: &'a Connection,
        authority_frontier: AdmissionSeq,
        read_trace: ReadTrace,
        operations: Vec<MultiliteOp>,
    },
}

/// One managed update accumulating a single durable transaction.
///
/// Snapshot-isolated DML executes on a private SQLite branch. Serializable
/// work and the temporary DDL path retain the canonical serialized runtime
/// until native branch read tracing and stop-the-world DDL are complete.
pub struct UpdateTransaction<'a, H: ServerHandle> {
    isolation: IsolationLevel,
    backend: UpdateBackend<'a, H>,
}

impl<'a, H: ServerHandle + Send + Sync + 'static> UpdateTransaction<'a, H> {
    fn branch(connection: &'a Connection, isolation: IsolationLevel) -> Self {
        Self {
            isolation,
            backend: UpdateBackend::Branch { connection },
        }
    }

    fn serialized(
        database: &'a Database<H>,
        runtime: &'a DatabaseRuntime,
        connection: &'a Connection,
        authority_frontier: AdmissionSeq,
        isolation: IsolationLevel,
    ) -> Self {
        Self {
            isolation,
            backend: UpdateBackend::Serialized {
                database,
                runtime,
                connection,
                authority_frontier,
                read_trace: ReadTrace::new(),
                operations: Vec::new(),
            },
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
        match &self.backend {
            UpdateBackend::Branch { connection } => {
                TransactionStatement::new_direct(connection, sql)
            }
            UpdateBackend::Serialized {
                runtime,
                connection,
                read_trace,
                ..
            } => {
                let trace =
                    (self.isolation == IsolationLevel::Serializable).then(|| read_trace.clone());
                TransactionStatement::new(runtime, connection, sql, trace)
            }
        }
    }

    /// Execute one statement validated before the transaction began.
    pub(super) fn execute_validated<Q: Params>(
        &mut self,
        sql: &str,
        params: Q,
        validated: ValidatedExecute,
    ) -> Result<usize> {
        match &mut self.backend {
            UpdateBackend::Branch { connection } => match validated {
                ValidatedExecute::Insert => Ok(connection.execute(sql, params)?),
                ValidatedExecute::CreateTable(_) => Err(Error::UnsupportedSql(
                    "DDL inside snapshot updates is not supported; execute it directly",
                )),
            },
            UpdateBackend::Serialized { .. } => match validated {
                ValidatedExecute::CreateTable(table) => {
                    self.execute_serialized_create_table(sql, params, table)
                }
                ValidatedExecute::Insert => self.execute_serialized_insert(sql, params),
            },
        }
    }

    fn execute_serialized_create_table<Q: Params>(
        &mut self,
        sql: &str,
        params: Q,
        table: super::schema::CreateTableSpec,
    ) -> Result<usize> {
        let UpdateBackend::Serialized {
            runtime,
            operations,
            ..
        } = &mut self.backend
        else {
            unreachable!("serialized CREATE TABLE used on a branch")
        };
        let operation = MultiliteOp::create_table(sql, table);
        let MultiliteOp::CreateTable(created) = &operation else {
            unreachable!("create-table constructor returned another operation")
        };
        let (changed, _) = runtime.run(ExecutionMode::Public, |connection| {
            let changed = connection.execute(sql, params)?;
            runtime.with_internal_metadata(|| catalog::insert(connection, created))?;
            Ok(changed)
        })?;
        operations.push(operation);
        Ok(changed)
    }

    fn execute_serialized_insert<Q: Params>(&mut self, sql: &str, params: Q) -> Result<usize> {
        let UpdateBackend::Serialized {
            runtime,
            connection,
            operations,
            ..
        } = &mut self.backend
        else {
            unreachable!("serialized INSERT used on a branch")
        };
        let (changed, events) = runtime.run(ExecutionMode::Public, |connection| {
            Ok(connection.execute(sql, params)?)
        })?;
        let Some(inserted) = InsertRows::from_captured(connection, &events)? else {
            if events.is_empty() {
                return Ok(changed);
            }
            return Err(Error::UnsupportedSql(
                "INSERT target has no synchronized schema identity",
            ));
        };
        operations.push(MultiliteOp::InsertRows(inserted));
        Ok(changed)
    }

    fn finalize_serialized(self) -> Result<bool> {
        let UpdateBackend::Serialized {
            database,
            runtime,
            connection,
            authority_frontier,
            read_trace,
            operations,
        } = self.backend
        else {
            return Ok(false);
        };
        if operations.is_empty() {
            return Ok(false);
        }
        let transaction = MultiliteTransaction::new(operations)?;
        let mut homebase = transaction.to_homebase()?;
        homebase.include_read_trace(&read_trace);
        let (mutations, assertions) = homebase.plan(self.isolation, authority_frontier);
        runtime.with_internal_metadata(|| {
            let sequence = block_on(async {
                let space = database
                    .client
                    .space(database.database_id.space_id())
                    .await?;
                let submission = space
                    .submit_unchecked(mutations, assertions)
                    .await
                    .map_err(ClientError::from)?;
                Ok::<_, Error>(submission.seq)
            })?;
            pending::insert(connection, sequence, &transaction)
        })?;
        Ok(true)
    }
}

impl<H: ServerHandle + Send + Sync + 'static> Database<H> {
    /// Run one complete update using the database's default isolation.
    pub fn update<T>(
        self: &std::sync::Arc<Self>,
        runtime: &DatabaseRuntime,
        operation: impl FnOnce(&mut UpdateTransaction<'_, H>) -> Result<T>,
    ) -> Result<T> {
        self.update_with(runtime, UpdateOptions::new(self.isolation_level), operation)
    }

    /// Run one managed update with an explicit isolation override.
    pub fn update_with<T>(
        self: &std::sync::Arc<Self>,
        runtime: &DatabaseRuntime,
        options: UpdateOptions,
        operation: impl FnOnce(&mut UpdateTransaction<'_, H>) -> Result<T>,
    ) -> Result<T> {
        match options.isolation_level() {
            IsolationLevel::Snapshot => self.update_on_branch(runtime, options, operation),
            IsolationLevel::Serializable => self.update_serialized(runtime, options, operation),
        }
    }

    pub(super) fn execute_serialized<T>(
        self: &std::sync::Arc<Self>,
        runtime: &DatabaseRuntime,
        operation: impl FnOnce(&mut UpdateTransaction<'_, H>) -> Result<T>,
    ) -> Result<T> {
        self.update_serialized(runtime, UpdateOptions::new(self.isolation_level), operation)
    }

    fn update_on_branch<T>(
        self: &std::sync::Arc<Self>,
        runtime: &DatabaseRuntime,
        options: UpdateOptions,
        operation: impl FnOnce(&mut UpdateTransaction<'_, H>) -> Result<T>,
    ) -> Result<T> {
        {
            let _operation = self.enter_operation()?;
            self.refresh_read_serial(runtime)?;
        }
        let BranchSnapshot {
            physical,
            logical,
            tables,
            history_pin: _history_pin,
        } = self.issue_branch_snapshot(true)?;
        let branch = WritableBranch::open(physical, OverlayOptions::default())
            .map_err(|error| Error::Branch(error.to_string()))?;
        let table_refs = tables.iter().map(String::as_str).collect::<Vec<_>>();
        let capture = ChangesetCapture::start(&branch, &table_refs)
            .map_err(|error| Error::Branch(error.to_string()))?;
        let mut update = UpdateTransaction::branch(branch.connection(), options.isolation_level());
        let value = operation(&mut update)?;
        drop(update);
        let changeset = capture
            .finish()
            .map_err(|error| Error::Branch(error.to_string()))?;
        let proposal = CommitProposal::from_captured(
            logical,
            options.isolation_level(),
            changeset,
            branch.connection(),
            std::iter::empty(),
        )?;
        if let Some(proposal) = proposal {
            self.commit_proposal(proposal)?;
            self.finish_branch_write()?;
        }
        Ok(value)
    }

    fn update_serialized<T>(
        self: &std::sync::Arc<Self>,
        runtime: &DatabaseRuntime,
        options: UpdateOptions,
        operation: impl FnOnce(&mut UpdateTransaction<'_, H>) -> Result<T>,
    ) -> Result<T> {
        let _operation = self.enter_operation()?;
        self.refresh_read_serial(runtime)?;
        let authority_frontier = self.authority_frontier()?;
        let value = self
            .owner
            .with_savepoint("__multilite__serialized_update", |connection| {
                pin_snapshot(connection)?;
                let mut update = UpdateTransaction::serialized(
                    self,
                    runtime,
                    connection,
                    authority_frontier,
                    options.isolation_level(),
                );
                let value = operation(&mut update)?;
                if update.finalize_serialized()? {
                    crate::commit::proposal::advance_commit_seq(connection)?;
                }
                Ok(value)
            })?;

        match self.policy.policy() {
            SyncPolicy::LocalOnly => {}
            SyncPolicy::LocalFirst { write_delay, .. } => self.scheduler.schedule(write_delay),
            SyncPolicy::Remote => self.finish_remote_write()?,
        }
        Ok(value)
    }

    fn finish_branch_write(self: &std::sync::Arc<Self>) -> Result<()> {
        match self.policy.policy() {
            SyncPolicy::LocalOnly => Ok(()),
            SyncPolicy::LocalFirst { write_delay, .. } => {
                self.scheduler.schedule(write_delay);
                Ok(())
            }
            SyncPolicy::Remote => match self.push()? {
                super::PushOutcome::Drained => Ok(()),
                super::PushOutcome::Rejected(rejection) => {
                    let error = rejection.error.clone();
                    self.rollback(&rejection)?;
                    let _ = self.push();
                    Err(Error::AuthorityRejected(error))
                }
            },
        }
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
