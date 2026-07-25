//! Managed local update execution.

use std::collections::BTreeSet;
use std::marker::PhantomData;
use std::sync::{Arc, Mutex, MutexGuard};

use homebase_client::ServerHandle;
use homebase_core::key::Key;
use rusqlite::hooks::{AuthAction, AuthContext, Authorization, PreUpdateCase};
use rusqlite::{Connection, Row};

use super::operation::MultiliteOp;
use super::row::InsertRows;
use super::schema::table_prefix;
use super::sql::ValidatedExecute;
use super::transaction::MultiliteTransaction;
use super::view::TransactionStatement;
use super::{Database, DatabaseRuntime, IsolationLevel, SyncPolicy, UpdateOptions, catalog};
use crate::branch::{OverlayOptions, WritableBranch};
use crate::commit::committer::CommitSnapshot;
use crate::commit::proposal::CommitProposal;
use crate::runtime::ExecutionMode;
use crate::{Error, Params, Result};

/// One managed update accumulating a single durable transaction.
///
/// Every isolation level executes on a private SQLite branch. Serializable
/// updates additionally retain coarse table-level read dependencies.
pub struct UpdateTransaction<'a, H: ServerHandle> {
    isolation: IsolationLevel,
    connection: &'a Connection,
    hooks: BranchHooks<'a>,
    operations: Vec<MultiliteOp>,
    _server: PhantomData<fn() -> H>,
}

impl<'a, H: ServerHandle + Send + Sync + 'static> UpdateTransaction<'a, H> {
    fn branch(connection: &'a Connection, isolation: IsolationLevel) -> Result<Self> {
        Ok(Self {
            isolation,
            connection,
            hooks: BranchHooks::install(connection, isolation == IsolationLevel::Serializable)?,
            operations: Vec::new(),
            _server: PhantomData,
        })
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
        TransactionStatement::new(self.connection, sql)
    }

    /// Execute one statement validated before the transaction began.
    pub(super) fn execute_validated<Q: Params>(
        &mut self,
        sql: &str,
        params: Q,
        validated: ValidatedExecute,
    ) -> Result<usize> {
        match validated {
            ValidatedExecute::CreateTable(table) => {
                let operation = MultiliteOp::create_table(sql, table);
                let MultiliteOp::CreateTable(created) = &operation else {
                    unreachable!("create-table constructor returned another operation")
                };
                let (changed, events) = self.hooks.run(
                    || {
                        let changed = self.connection.execute(sql, params)?;
                        self.hooks
                            .with_internal(|| catalog::insert(self.connection, created))?;
                        Ok(changed)
                    },
                    |_| Ok(()),
                )?;
                if !events.is_empty() {
                    return Err(Error::CaptureInvariant(
                        "CREATE TABLE captured application rows",
                    ));
                }
                self.operations.push(operation);
                Ok(changed)
            }
            ValidatedExecute::Insert => {
                let (changed, events) = self.hooks.run(
                    || Ok(self.connection.execute(sql, params)?),
                    |events| super::row::normalize_hidden_rowids(self.connection, events),
                )?;
                let inserted = self
                    .hooks
                    .with_internal(|| InsertRows::from_captured(self.connection, &events))?;
                let Some(inserted) = inserted else {
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
        }
    }

    fn into_branch_parts(self) -> Result<(Vec<MultiliteOp>, Vec<Key>)> {
        let reads = self.hooks.read_prefixes(self.connection)?;
        Ok((self.operations, reads))
    }
}

#[derive(Default)]
struct BranchHookState {
    events: Vec<super::row::CapturedRow>,
    error: Option<Error>,
    internal_depth: usize,
    trace_reads: bool,
    read_tables: BTreeSet<String>,
}

struct BranchHooks<'connection> {
    connection: &'connection Connection,
    state: Arc<Mutex<BranchHookState>>,
}

impl<'connection> BranchHooks<'connection> {
    fn install(connection: &'connection Connection, trace_reads: bool) -> Result<Self> {
        let state = Arc::new(Mutex::new(BranchHookState {
            trace_reads,
            ..BranchHookState::default()
        }));

        let authorizer_state = Arc::clone(&state);
        connection.authorizer(Some(move |context: AuthContext<'_>| {
            let mut state = lock(&authorizer_state);
            let mode = if state.internal_depth == 0 {
                ExecutionMode::Public
            } else {
                ExecutionMode::InternalMetadata
            };
            let authorization = super::authorize_database(mode, &context);
            if state.trace_reads
                && mode == ExecutionMode::Public
                && authorization == Authorization::Allow
                && let AuthAction::Read { table_name, .. } = context.action
                && super::is_main(context.database_name)
                && !super::is_schema_table(table_name)
                && !super::has_multilite_prefix(table_name)
            {
                let mut canonical = table_name.to_owned();
                canonical.make_ascii_lowercase();
                state.read_tables.insert(canonical);
            }
            authorization
        }))?;

        let callback = Arc::clone(&state);
        connection.preupdate_hook(Some(
            move |_action, database: &str, table: &str, update: &PreUpdateCase| {
                let mut state = lock(&callback);
                if state.internal_depth != 0 || state.error.is_some() {
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
        self.with_internal(|| {
            self.connection
                .execute_batch("SAVEPOINT __multilite__branch_statement")?;
            Ok(())
        })?;
        let result = operation();
        let callback_error = lock(&self.state).error.take();
        match result.and_then(|value| callback_error.map_or(Ok(value), Err)) {
            Ok(value) => {
                let mut events = lock(&self.state).events.split_off(checkpoint);
                let finalized = self.with_internal(|| finalize(&mut events));
                if let Err(error) = finalized {
                    self.rollback_statement()?;
                    return Err(error);
                }
                self.with_internal(|| {
                    self.connection
                        .execute_batch("RELEASE __multilite__branch_statement")?;
                    Ok(())
                })?;
                Ok((value, events))
            }
            Err(error) => {
                lock(&self.state).events.truncate(checkpoint);
                self.rollback_statement()?;
                Err(error)
            }
        }
    }

    fn with_internal<T>(&self, operation: impl FnOnce() -> Result<T>) -> Result<T> {
        let _guard = BranchInternalGuard::enter(Arc::clone(&self.state));
        operation()
    }

    fn rollback_statement(&self) -> Result<()> {
        self.with_internal(|| {
            self.connection.execute_batch(
                "ROLLBACK TO __multilite__branch_statement;
                 RELEASE __multilite__branch_statement",
            )?;
            Ok(())
        })
    }

    fn read_prefixes(&self, connection: &Connection) -> Result<Vec<Key>> {
        let tables = lock(&self.state).read_tables.clone();
        self.with_internal(|| {
            tables
                .into_iter()
                .map(|table| {
                    let created =
                        catalog::by_name(connection, &table)?.ok_or(Error::UnsupportedSql(
                            "serializable reads require synchronized table identities",
                        ))?;
                    Ok(table_prefix(created.table_id()))
                })
                .collect()
        })
    }
}

struct BranchInternalGuard {
    state: Arc<Mutex<BranchHookState>>,
}

impl BranchInternalGuard {
    fn enter(state: Arc<Mutex<BranchHookState>>) -> Self {
        lock(&state).internal_depth += 1;
        Self { state }
    }
}

impl Drop for BranchInternalGuard {
    fn drop(&mut self) {
        let mut state = lock(&self.state);
        state.internal_depth = state
            .internal_depth
            .checked_sub(1)
            .expect("branch internal hook depth is balanced");
    }
}

impl Drop for BranchHooks<'_> {
    fn drop(&mut self) {
        let _ = self
            .connection
            .authorizer(None::<fn(AuthContext<'_>) -> Authorization>);
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
        self.update_on_branch(runtime, options, operation)
    }

    fn update_on_branch<T>(
        self: &std::sync::Arc<Self>,
        runtime: &DatabaseRuntime,
        options: UpdateOptions,
        operation: impl FnOnce(&mut UpdateTransaction<'_, H>) -> Result<T>,
    ) -> Result<T> {
        self.refresh_read(runtime)?;
        let CommitSnapshot {
            physical,
            logical,
            history_pin: _history_pin,
        } = self.issue_branch_snapshot(true)?;
        let branch = WritableBranch::open(physical, OverlayOptions::default())
            .map_err(|error| Error::Branch(error.to_string()))?;
        let mut update = UpdateTransaction::branch(branch.connection(), options.isolation_level())?;
        let value = operation(&mut update)?;
        let (operations, reads) = update.into_branch_parts()?;
        if !operations.is_empty() {
            let transaction = MultiliteTransaction::new(operations)?;
            let proposal = CommitProposal::from_transaction(
                logical,
                options.isolation_level(),
                transaction,
                reads,
            )?;
            let receipt = self.commit_proposal(proposal)?;
            self.finish_branch_write(receipt)?;
        }
        Ok(value)
    }

    fn finish_branch_write(
        self: &std::sync::Arc<Self>,
        receipt: crate::commit::proposal::CommitReceipt,
    ) -> Result<()> {
        let sequence = receipt.submitted.ok_or(Error::CaptureInvariant(
            "transaction commit receipt has no Homebase sequence",
        ))?;
        match self.policy.policy() {
            SyncPolicy::LocalOnly => Ok(()),
            SyncPolicy::LocalFirst { write_delay, .. } => {
                self.scheduler
                    .schedule_group(receipt.commit_seq, write_delay);
                Ok(())
            }
            SyncPolicy::Remote => match self.push_submission(sequence)? {
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
}
