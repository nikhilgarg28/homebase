//! Managed local update execution.

use std::sync::{Arc, Mutex, MutexGuard};

use homebase_client::{ClientError, ServerHandle};
use homebase_core::tag::AdmissionSeq;
use pollster::block_on;
use rusqlite::hooks::PreUpdateCase;
use rusqlite::{Connection, Row};

use super::operation::MultiliteOp;
use super::row::InsertRows;
use super::sql::ValidatedExecute;
use super::transaction::MultiliteTransaction;
use super::view::TransactionStatement;
use super::{
    BranchSnapshot, Database, DatabaseRuntime, IsolationLevel, SyncPolicy, UpdateOptions, catalog,
    pending, pin_snapshot,
};
use crate::branch::{OverlayOptions, WritableBranch};
use crate::commit::footprint::ReadTrace;
use crate::commit::history::{self, WriteRegion};
use crate::commit::proposal::CommitProposal;
use crate::runtime::ExecutionMode;
use crate::{Error, Params, Result};

enum UpdateBackend<'a, H: ServerHandle> {
    Branch {
        connection: &'a Connection,
        capture: BranchCapture<'a>,
        operations: Vec<MultiliteOp>,
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
/// managed updates retain the canonical serialized runtime until native branch
/// read tracing is complete.
pub struct UpdateTransaction<'a, H: ServerHandle> {
    isolation: IsolationLevel,
    backend: UpdateBackend<'a, H>,
}

impl<'a, H: ServerHandle + Send + Sync + 'static> UpdateTransaction<'a, H> {
    fn branch(connection: &'a Connection, isolation: IsolationLevel) -> Result<Self> {
        Ok(Self {
            isolation,
            backend: UpdateBackend::Branch {
                connection,
                capture: BranchCapture::install(connection)?,
                operations: Vec::new(),
            },
        })
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
            UpdateBackend::Branch { connection, .. } => {
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
            UpdateBackend::Branch {
                connection,
                capture,
                operations,
            } => match validated {
                ValidatedExecute::CreateTable(table) => {
                    let operation = MultiliteOp::create_table(sql, table);
                    let MultiliteOp::CreateTable(created) = &operation else {
                        unreachable!("create-table constructor returned another operation")
                    };
                    let (changed, events) = capture.run(
                        || {
                            let changed = connection.execute(sql, params)?;
                            catalog::insert(connection, created)?;
                            Ok(changed)
                        },
                        |_| Ok(()),
                    )?;
                    if !events.is_empty() {
                        return Err(Error::CaptureInvariant(
                            "CREATE TABLE captured application rows",
                        ));
                    }
                    operations.push(operation);
                    Ok(changed)
                }
                ValidatedExecute::Insert => {
                    let (changed, events) = capture.run(
                        || Ok(connection.execute(sql, params)?),
                        |events| super::row::normalize_hidden_rowids(connection, events),
                    )?;
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
        let (changed, events) = runtime.run_captured(
            ExecutionMode::Public,
            |connection| Ok(connection.execute(sql, params)?),
            |connection, events| super::row::normalize_hidden_rowids(connection, events),
        )?;
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

    fn finalize_serialized(self) -> Result<Option<Vec<WriteRegion>>> {
        let UpdateBackend::Serialized {
            database,
            runtime,
            connection,
            authority_frontier,
            read_trace,
            operations,
        } = self.backend
        else {
            return Ok(None);
        };
        if operations.is_empty() {
            return Ok(None);
        }
        let transaction = MultiliteTransaction::new(operations)?;
        let mut homebase = transaction.to_homebase()?;
        homebase.include_read_trace(&read_trace);
        let (mutations, footprint) = homebase.into_parts();
        let writes = history::writes_from_mutations(&mutations);
        let assertions = footprint.plan(self.isolation, authority_frontier);
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
        Ok(Some(writes))
    }

    fn into_branch_operations(self) -> Vec<MultiliteOp> {
        match self.backend {
            UpdateBackend::Branch { operations, .. } => operations,
            UpdateBackend::Serialized { .. } => Vec::new(),
        }
    }
}

#[derive(Default)]
struct BranchCaptureState {
    events: Vec<super::row::CapturedRow>,
    error: Option<Error>,
    suppress: bool,
}

struct BranchCapture<'connection> {
    connection: &'connection Connection,
    state: Arc<Mutex<BranchCaptureState>>,
}

impl<'connection> BranchCapture<'connection> {
    fn install(connection: &'connection Connection) -> Result<Self> {
        let state = Arc::new(Mutex::new(BranchCaptureState::default()));
        let callback = Arc::clone(&state);
        connection.preupdate_hook(Some(
            move |_action, database: &str, table: &str, update: &PreUpdateCase| {
                let mut state = lock(&callback);
                if state.suppress || state.error.is_some() {
                    return;
                }
                match super::capture_insert(ExecutionMode::Public, database, table, update) {
                    Ok(Some(event)) => state.events.push(event),
                    Ok(None) => {}
                    Err(error) => state.error = Some(error),
                }
            },
        ))?;
        Ok(Self { connection, state })
    }

    fn run<T>(
        &self,
        operation: impl FnOnce() -> Result<T>,
        finalize: impl FnOnce(&mut Vec<super::row::CapturedRow>) -> Result<()>,
    ) -> Result<(T, Vec<super::row::CapturedRow>)> {
        let checkpoint = lock(&self.state).events.len();
        self.connection
            .execute_batch("SAVEPOINT __multilite__branch_statement")?;
        let result = operation();
        let callback_error = lock(&self.state).error.take();
        match result.and_then(|value| callback_error.map_or(Ok(value), Err)) {
            Ok(value) => {
                let mut events = lock(&self.state).events.split_off(checkpoint);
                lock(&self.state).suppress = true;
                let finalized = finalize(&mut events);
                lock(&self.state).suppress = false;
                if let Err(error) = finalized {
                    self.connection.execute_batch(
                        "ROLLBACK TO __multilite__branch_statement;
                         RELEASE __multilite__branch_statement",
                    )?;
                    return Err(error);
                }
                self.connection
                    .execute_batch("RELEASE __multilite__branch_statement")?;
                Ok((value, events))
            }
            Err(error) => {
                lock(&self.state).events.truncate(checkpoint);
                self.connection.execute_batch(
                    "ROLLBACK TO __multilite__branch_statement;
                     RELEASE __multilite__branch_statement",
                )?;
                Err(error)
            }
        }
    }
}

impl Drop for BranchCapture<'_> {
    fn drop(&mut self) {
        let _ = self
            .connection
            .preupdate_hook::<fn(rusqlite::hooks::Action, &str, &str, &PreUpdateCase)>(None);
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
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
            history_pin: _history_pin,
        } = self.issue_branch_snapshot(true)?;
        let branch = WritableBranch::open(physical, OverlayOptions::default())
            .map_err(|error| Error::Branch(error.to_string()))?;
        let mut update = UpdateTransaction::branch(branch.connection(), options.isolation_level())?;
        let value = operation(&mut update)?;
        let operations = update.into_branch_operations();
        if !operations.is_empty() {
            let transaction = MultiliteTransaction::new(operations)?;
            let proposal = CommitProposal::from_transaction(
                logical,
                options.isolation_level(),
                transaction,
                std::iter::empty(),
            )?;
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
                if let Some(writes) = update.finalize_serialized()? {
                    self.commit_history.record(connection, writes)?;
                }
                Ok(value)
            })?;
        self.prune_commit_history()?;

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
