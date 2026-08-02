//! Managed local update execution.

use std::collections::BTreeSet;
use std::marker::PhantomData;
use std::sync::{Arc, Mutex, MutexGuard};

use homebase_client::ServerHandle;
use homebase_core::key::Key;
use rusqlite::hooks::{AuthAction, AuthContext, Authorization, PreUpdateCase};
use rusqlite::{Connection, OptionalExtension, Row};

use super::view::TransactionStatement;
use super::{Database, DatabaseRuntime, IsolationLevel, UpdateOptions, catalog};
use crate::branch::{OverlayOptions, WritableBranch};
use crate::catalog::CatalogSnapshot;
use crate::commit::committer::{CommitSnapshot, HistoryPin};
use crate::commit::footprint::ConflictFootprint;
use crate::commit::proposal::CommitProposal;
use crate::logical::alter::AlterTableOperation;
use crate::logical::drop_table::DropTableOperation;
use crate::logical::guard::{GuardPlan, GuardReason, OperationFamily};
use crate::logical::index::IndexOperation;
use crate::logical::operation::{CompiledOperation, MultiliteOp};
use crate::logical::row::{CaptureBudget, CapturedChange, RowChanges};
use crate::logical::schema::{
    CreateTable, SqlName, TableId, schema_object_name_scope_key, table_prefix,
};
use crate::logical::transaction::MultiliteTransaction;
use crate::runtime::ExecutionMode;
use crate::sql::{StatementOutput, ValidatedExecute, ValidatedStatement};
use crate::{Error, Params, Result};

/// One managed update accumulating a single durable transaction.
///
/// Every isolation level executes on a private SQLite branch. Serializable
/// updates additionally retain coarse table-level read dependencies.
pub struct UpdateTransaction<'a, H: ServerHandle> {
    isolation: IsolationLevel,
    connection: &'a Connection,
    hooks: BranchHooks<'a>,
    operations: Vec<CompiledOperation>,
    footprint: ConflictFootprint,
    _server: PhantomData<fn() -> H>,
}

impl<'a, H: ServerHandle + Send + Sync + 'static> UpdateTransaction<'a, H> {
    fn branch(connection: &'a Connection, isolation: IsolationLevel) -> Result<Self> {
        Self::branch_with_capture_budget(connection, isolation, CaptureBudget::default())
    }

    fn branch_with_capture_budget(
        connection: &'a Connection,
        isolation: IsolationLevel,
        capture_budget: CaptureBudget,
    ) -> Result<Self> {
        Ok(Self {
            isolation,
            connection,
            hooks: BranchHooks::install_with_budget(
                connection,
                isolation == IsolationLevel::Serializable,
                capture_budget,
            )?,
            operations: Vec::new(),
            footprint: ConflictFootprint::new(),
            _server: PhantomData,
        })
    }

    /// Isolation level selected for this managed update.
    pub fn isolation_level(&self) -> IsolationLevel {
        self.isolation
    }

    /// Execute one supported mutating statement inside this update.
    pub fn execute<Q: Params>(&mut self, sql: &str, params: Q) -> Result<usize> {
        crate::sql::validate_managed_statement(sql)?;
        let validated = crate::sql::validate_execute(sql)?;
        self.execute_validated(sql, params, validated)
    }

    /// Execute one row-producing statement against this update's current state.
    ///
    /// Both reads and DML with `RETURNING` are accepted. A returning write is
    /// captured into the same managed transaction as calls to [`Self::execute`].
    pub fn query<T, P, F>(&mut self, sql: &str, params: P, map: F) -> Result<Vec<T>>
    where
        P: Params,
        F: FnMut(&Row<'_>) -> rusqlite::Result<T>,
    {
        let validated = crate::sql::validate_statement(sql)?;
        if validated.output() != StatementOutput::Rows {
            return Err(Error::StatementModeMismatch);
        }
        match validated {
            ValidatedStatement::Read => {
                let mut statement = TransactionStatement::new_prevalidated(self.connection, sql)?;
                statement.query_map(params, map)
            }
            ValidatedStatement::Write(validated) => {
                self.query_validated(sql, params, map, *validated)
            }
        }
    }

    /// Alias matching rusqlite's mapped-query vocabulary.
    pub fn query_map<T, P, F>(&mut self, sql: &str, params: P, map: F) -> Result<Vec<T>>
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
            ValidatedExecute::RenameTable(spec) => {
                let operation = self.hooks.with_internal(|| {
                    AlterTableOperation::prepare_rename_table(self.connection, sql, &spec)
                })?;
                self.execute_alter(sql, params, operation)
            }
            ValidatedExecute::RenameColumn(spec) => {
                let operation = self.hooks.with_internal(|| {
                    AlterTableOperation::prepare_rename_column(self.connection, sql, &spec)
                })?;
                self.execute_alter(sql, params, operation)
            }
            ValidatedExecute::AddColumn(spec) => {
                let operation = self.hooks.with_internal(|| {
                    AlterTableOperation::prepare_add_column(self.connection, sql, &spec)
                })?;
                self.execute_alter(sql, params, operation)
            }
            ValidatedExecute::DropColumn(spec) => {
                let operation = self.hooks.with_internal(|| {
                    AlterTableOperation::prepare_drop_column(self.connection, sql, &spec)
                })?;
                self.execute_alter(sql, params, operation)
            }
            ValidatedExecute::CreateTable(table) => self.execute_create_table(sql, params, table),
            ValidatedExecute::CreateTableIfNotExists(table) => {
                let is_noop = self
                    .hooks
                    .with_internal(|| self.create_table_is_noop(&table.name))?;
                if is_noop {
                    self.execute_conditional_schema_noop(
                        sql,
                        params,
                        &table.name,
                        "CREATE TABLE captured application rows",
                    )
                } else {
                    self.execute_create_table(sql, params, table)
                }
            }
            ValidatedExecute::DropTable(spec) => self.execute_drop_table(sql, params, spec),
            ValidatedExecute::DropTableIfExists(spec) => {
                let is_noop = self
                    .hooks
                    .with_internal(|| self.drop_table_is_noop(&spec.name))?;
                if is_noop {
                    self.execute_conditional_schema_noop(
                        sql,
                        params,
                        &spec.name,
                        "DROP TABLE captured application rows",
                    )
                } else {
                    self.execute_drop_table(sql, params, spec)
                }
            }
            ValidatedExecute::CreateIndex(spec) => self.execute_create_index(sql, params, spec),
            ValidatedExecute::CreateIndexIfNotExists(spec) => {
                let is_noop = self
                    .hooks
                    .with_internal(|| self.create_index_is_noop(&spec.name))?;
                if is_noop {
                    self.execute_conditional_schema_noop(
                        sql,
                        params,
                        &spec.name,
                        "CREATE INDEX captured application rows",
                    )
                } else {
                    self.execute_create_index(sql, params, spec)
                }
            }
            ValidatedExecute::DropIndex(spec) => self.execute_drop_index(sql, params, spec),
            ValidatedExecute::DropIndexIfExists(spec) => {
                let is_noop = self
                    .hooks
                    .with_internal(|| self.drop_index_is_noop(&spec.name))?;
                if is_noop {
                    self.execute_conditional_schema_noop(
                        sql,
                        params,
                        &spec.name,
                        "DROP INDEX captured application rows",
                    )
                } else {
                    self.execute_drop_index(sql, params, spec)
                }
            }
            ValidatedExecute::Insert(StatementOutput::Changes)
            | ValidatedExecute::Delete(StatementOutput::Changes)
            | ValidatedExecute::Update(StatementOutput::Changes) => {
                self.execute_captured(sql, params, compile_row_changes)
            }
            ValidatedExecute::Insert(StatementOutput::Rows)
            | ValidatedExecute::Delete(StatementOutput::Rows)
            | ValidatedExecute::Update(StatementOutput::Rows) => Err(Error::StatementModeMismatch),
        }
    }

    /// Execute one prevalidated write that produces mapped result rows.
    pub(super) fn query_validated<T, P, F>(
        &mut self,
        sql: &str,
        params: P,
        map: F,
        validated: ValidatedExecute,
    ) -> Result<Vec<T>>
    where
        P: Params,
        F: FnMut(&Row<'_>) -> rusqlite::Result<T>,
    {
        match validated {
            ValidatedExecute::Insert(StatementOutput::Rows)
            | ValidatedExecute::Delete(StatementOutput::Rows)
            | ValidatedExecute::Update(StatementOutput::Rows) => {
                self.query_captured(sql, params, map, compile_row_changes)
            }
            _ => Err(Error::StatementModeMismatch),
        }
    }

    fn execute_create_table<Q: Params>(
        &mut self,
        sql: &str,
        params: Q,
        table: crate::logical::schema::CreateTableSpec,
    ) -> Result<usize> {
        let operation = MultiliteOp::CreateTable(
            self.hooks
                .with_internal(|| CreateTable::prepare(self.connection, sql, table))?,
        );
        let operation = operation.compile()?;
        let MultiliteOp::CreateTable(created) = operation.logical() else {
            unreachable!("create-table constructor returned another operation")
        };
        let materialization_sql = self
            .hooks
            .with_internal(|| created.materialization_sql(self.connection))?;
        let (changed, events) = self.hooks.run_schema(
            || {
                let changed = self.connection.execute(&materialization_sql, params)?;
                self.hooks
                    .with_internal(|| catalog::insert(self.connection, created))?;
                Ok(changed)
            },
            |_| Ok(()),
        )?;
        ensure_no_schema_rows(&events, "CREATE TABLE captured application rows")?;
        self.record_operation(operation);
        Ok(changed)
    }

    fn execute_drop_table<Q: Params>(
        &mut self,
        sql: &str,
        params: Q,
        spec: crate::sql::DropTableSpec,
    ) -> Result<usize> {
        let operation = self
            .hooks
            .with_internal(|| DropTableOperation::prepare(self.connection, sql, &spec))?;
        let compiled = MultiliteOp::DropTable(operation.clone()).compile()?;
        let (changed, events) = self.hooks.run_schema(
            || {
                let changed = self.connection.execute(sql, params)?;
                self.hooks.with_internal(|| {
                    catalog::remove_by_id(self.connection, operation.table_id())
                })?;
                Ok(changed)
            },
            |_| Ok(()),
        )?;
        ensure_no_schema_rows(&events, "DROP TABLE captured application rows")?;
        self.record_operation(compiled);
        Ok(changed)
    }

    fn execute_create_index<Q: Params>(
        &mut self,
        sql: &str,
        params: Q,
        spec: crate::sql::CreateIndexSpec,
    ) -> Result<usize> {
        let mut captured_operation = None;
        let (changed, events) = self.hooks.run_schema(
            || {
                let changed = self.connection.execute(sql, params)?;
                let operation = self.hooks.with_internal(|| {
                    IndexOperation::prepare_create(self.connection, sql, &spec)
                })?;
                self.hooks
                    .with_internal(|| operation.record_catalog(self.connection))?;
                captured_operation = Some(MultiliteOp::Index(operation).compile()?);
                Ok(changed)
            },
            |_| Ok(()),
        )?;
        ensure_no_schema_rows(&events, "CREATE INDEX captured application rows")?;
        let operation =
            captured_operation.expect("successful index creation compiled an operation");
        self.record_operation(operation);
        Ok(changed)
    }

    fn execute_drop_index<Q: Params>(
        &mut self,
        sql: &str,
        params: Q,
        spec: crate::sql::DropIndexSpec,
    ) -> Result<usize> {
        let operation = self
            .hooks
            .with_internal(|| IndexOperation::prepare_drop(self.connection, sql, &spec))?;
        let compiled = MultiliteOp::Index(operation.clone()).compile()?;
        let (changed, events) = self.hooks.run_schema(
            || {
                let changed = self.connection.execute(sql, params)?;
                self.hooks
                    .with_internal(|| operation.record_catalog(self.connection))?;
                Ok(changed)
            },
            |_| Ok(()),
        )?;
        ensure_no_schema_rows(&events, "DROP INDEX captured application rows")?;
        self.record_operation(compiled);
        Ok(changed)
    }

    fn execute_schema_noop<Q: Params>(
        &self,
        sql: &str,
        params: Q,
        capture_error: &'static str,
    ) -> Result<usize> {
        let (changed, events) = self
            .hooks
            .run_schema(|| Ok(self.connection.execute(sql, params)?), |_| Ok(()))?;
        ensure_no_schema_rows(&events, capture_error)?;
        Ok(changed)
    }

    fn execute_conditional_schema_noop<Q: Params>(
        &mut self,
        sql: &str,
        params: Q,
        name: &SqlName,
        capture_error: &'static str,
    ) -> Result<usize> {
        let changed = self.execute_schema_noop(sql, params, capture_error)?;
        let mut reads = GuardPlan::for_operation(OperationFamily::TransactionRead);
        reads.serializable_read(
            schema_object_name_scope_key(name),
            GuardReason::SerializableRead,
        )?;
        self.footprint.extend(reads.footprint());
        Ok(changed)
    }

    fn create_table_is_noop(&self, name: &crate::logical::schema::SqlName) -> Result<bool> {
        match schema_object_state(
            self.connection,
            name,
            "table",
            catalog::by_name(self.connection, name.value())?,
            "CREATE TABLE IF NOT EXISTS found an untracked SQLite table",
            "schema catalog table is missing from SQLite",
        )? {
            SchemaObjectState::Present(table) => {
                crate::physical::verify_table(self.connection, table.table_id())?;
                Ok(true)
            }
            SchemaObjectState::Missing | SchemaObjectState::DifferentKind => Ok(false),
        }
    }

    fn create_index_is_noop(&self, name: &crate::logical::schema::SqlName) -> Result<bool> {
        match schema_object_state(
            self.connection,
            name,
            "index",
            catalog::index_by_name(self.connection, name)?,
            "CREATE INDEX IF NOT EXISTS found an untracked SQLite index",
            "schema catalog index is missing from SQLite",
        )? {
            SchemaObjectState::Present((table, _)) => {
                crate::physical::verify_table(self.connection, table.table_id())?;
                Ok(true)
            }
            SchemaObjectState::Missing | SchemaObjectState::DifferentKind => Ok(false),
        }
    }

    fn drop_index_is_noop(&self, name: &crate::logical::schema::SqlName) -> Result<bool> {
        match schema_object_state(
            self.connection,
            name,
            "index",
            catalog::index_by_name(self.connection, name)?,
            "DROP INDEX IF EXISTS found an untracked SQLite index",
            "schema catalog index is missing from SQLite",
        )? {
            SchemaObjectState::Present(_) => Ok(false),
            SchemaObjectState::Missing | SchemaObjectState::DifferentKind => Ok(true),
        }
    }

    fn drop_table_is_noop(&self, name: &SqlName) -> Result<bool> {
        match schema_object_state(
            self.connection,
            name,
            "table",
            catalog::by_name(self.connection, name.value())?,
            "DROP TABLE IF EXISTS found an untracked SQLite table",
            "schema catalog table is missing from SQLite",
        )? {
            SchemaObjectState::Present(_) => Ok(false),
            SchemaObjectState::Missing | SchemaObjectState::DifferentKind => Ok(true),
        }
    }

    fn execute_captured<Q: Params>(
        &mut self,
        sql: &str,
        params: Q,
        compile: impl FnOnce(&CatalogSnapshot, Vec<CapturedChange>) -> Result<Option<CompiledOperation>>,
    ) -> Result<usize> {
        self.run_captured(|connection| Ok(connection.execute(sql, params)?), compile)
    }

    fn query_captured<T, P, F>(
        &mut self,
        sql: &str,
        params: P,
        map: F,
        compile: impl FnOnce(&CatalogSnapshot, Vec<CapturedChange>) -> Result<Option<CompiledOperation>>,
    ) -> Result<Vec<T>>
    where
        P: Params,
        F: FnMut(&Row<'_>) -> rusqlite::Result<T>,
    {
        let hook_state = Arc::clone(&self.hooks.state);
        let mut map = map;
        self.run_captured(
            move |connection| {
                let mut statement = connection.prepare(sql)?;
                if statement.readonly() || statement.column_count() == 0 {
                    return Err(Error::StatementModeMismatch);
                }
                let mut rows = statement.query(params)?;
                let mut mapped = Vec::new();
                while let Some(row) = rows.next()? {
                    if let Some(error) = lock(&hook_state).error.clone() {
                        return Err(error);
                    }
                    mapped.push(map(row)?);
                }
                Ok(mapped)
            },
            compile,
        )
    }

    fn run_captured<T>(
        &mut self,
        run: impl FnOnce(&Connection) -> Result<T>,
        compile: impl FnOnce(&CatalogSnapshot, Vec<CapturedChange>) -> Result<Option<CompiledOperation>>,
    ) -> Result<T> {
        let mut captured_operation = None;
        let (value, _) = self.hooks.run(
            || run(self.connection),
            |events| {
                captured_operation = self
                    .hooks
                    .with_catalog(|catalog| compile(catalog, std::mem::take(events)))?;
                Ok(())
            },
        )?;
        if let Some(operation) = captured_operation {
            self.record_operation(operation);
        }
        Ok(value)
    }

    fn execute_alter<Q: Params>(
        &mut self,
        sql: &str,
        params: Q,
        operation: AlterTableOperation,
    ) -> Result<usize> {
        let logical = MultiliteOp::AlterTable(operation.clone()).compile()?;
        let (changed, events) = self.hooks.run_schema(
            || {
                if operation.materializes_internally() {
                    let mut statement = self.connection.prepare(sql)?;
                    params.__bind_in(&mut statement)?;
                    drop(statement);
                    self.hooks
                        .with_internal(|| operation.apply(self.connection))?;
                    Ok(0)
                } else {
                    let changed = self.connection.execute(sql, params)?;
                    self.hooks
                        .with_internal(|| operation.record_catalog(self.connection))?;
                    Ok(changed)
                }
            },
            |_| Ok(()),
        )?;
        if !events.is_empty() {
            return Err(Error::CaptureInvariant(
                "ALTER TABLE captured application rows",
            ));
        }
        self.record_operation(logical);
        Ok(changed)
    }

    fn record_operation(&mut self, operation: CompiledOperation) {
        self.footprint
            .extend(operation.homebase().guards().footprint());
        self.operations.push(operation);
    }

    fn into_branch_parts(mut self) -> Result<(Vec<CompiledOperation>, ConflictFootprint)> {
        let reads = self.hooks.read_prefixes()?;
        let mut read_guards = GuardPlan::for_operation(OperationFamily::TransactionRead);
        for read in reads {
            read_guards.serializable_read(read, GuardReason::SerializableRead)?;
        }
        self.footprint.extend(read_guards.footprint());
        Ok((self.operations, self.footprint))
    }
}

fn ensure_no_schema_rows(events: &[CapturedChange], message: &'static str) -> Result<()> {
    if events.is_empty() {
        Ok(())
    } else {
        Err(Error::CaptureInvariant(message))
    }
}

fn schema_object_kind(connection: &Connection, name: &str) -> Result<Option<String>> {
    Ok(connection
        .query_row(
            "SELECT type FROM main.sqlite_schema WHERE name = ?1 COLLATE NOCASE",
            [name],
            |row| row.get(0),
        )
        .optional()?)
}

enum SchemaObjectState<T> {
    Missing,
    Present(T),
    DifferentKind,
}

fn schema_object_state<T>(
    connection: &Connection,
    name: &SqlName,
    expected_kind: &'static str,
    catalog: Option<T>,
    untracked_error: &'static str,
    missing_error: &'static str,
) -> Result<SchemaObjectState<T>> {
    let physical = schema_object_kind(connection, name.value())?;
    match (physical.as_deref(), catalog) {
        (Some(kind), Some(catalog)) if kind == expected_kind => {
            Ok(SchemaObjectState::Present(catalog))
        }
        (Some(kind), None) if kind == expected_kind => Err(Error::InvalidDatabase(untracked_error)),
        (_, Some(_)) => Err(Error::InvalidDatabase(missing_error)),
        (Some(_), None) => Ok(SchemaObjectState::DifferentKind),
        (None, None) => Ok(SchemaObjectState::Missing),
    }
}

fn compile_row_changes(
    catalog: &CatalogSnapshot,
    events: Vec<CapturedChange>,
) -> Result<Option<CompiledOperation>> {
    RowChanges::from_catalog(catalog, events)?
        .map(|changes| MultiliteOp::ChangeRows(changes).compile())
        .transpose()
}

struct BranchHookState {
    events: Vec<CapturedChange>,
    capture_budget: CaptureBudget,
    error: Option<Error>,
    internal_depth: usize,
    read_trace_suppression_depth: usize,
    trace_reads: bool,
    catalog: CatalogSnapshot,
    read_tables: BTreeSet<[u8; 16]>,
    unresolved_read_tables: BTreeSet<String>,
}

struct BranchHooks<'connection> {
    connection: &'connection Connection,
    state: Arc<Mutex<BranchHookState>>,
}

impl<'connection> BranchHooks<'connection> {
    #[cfg(test)]
    fn install(connection: &'connection Connection, trace_reads: bool) -> Result<Self> {
        Self::install_with_budget(connection, trace_reads, CaptureBudget::default())
    }

    fn install_with_budget(
        connection: &'connection Connection,
        trace_reads: bool,
        capture_budget: CaptureBudget,
    ) -> Result<Self> {
        let catalog = CatalogSnapshot::load(connection)?;
        let state = Arc::new(Mutex::new(BranchHookState {
            events: Vec::new(),
            capture_budget,
            error: None,
            internal_depth: 0,
            read_trace_suppression_depth: 0,
            trace_reads,
            catalog,
            read_tables: BTreeSet::new(),
            unresolved_read_tables: BTreeSet::new(),
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
                && state.read_trace_suppression_depth == 0
                && authorization == Authorization::Allow
                && let AuthAction::Read { table_name, .. } = context.action
                && super::is_main(context.database_name)
                && !super::is_schema_table(table_name)
                && !crate::sql::has_multilite_prefix(table_name)
            {
                let mut canonical = table_name.to_owned();
                canonical.make_ascii_lowercase();
                if let Some(table) = state.catalog.table_id_by_name(&canonical) {
                    state.read_tables.insert(table.as_bytes());
                } else {
                    state.unresolved_read_tables.insert(canonical);
                }
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
                match super::capture_change(ExecutionMode::Public, database, table, update) {
                    Ok(Some(event)) => match state.capture_budget.record(&event) {
                        Ok(()) => state.events.push(event),
                        Err(error) => state.error = Some(error),
                    },
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
        finalize: impl FnOnce(&mut Vec<CapturedChange>) -> Result<()>,
    ) -> Result<(T, Vec<CapturedChange>)> {
        self.run_inner(operation, finalize, false)
    }

    fn run_schema<T>(
        &self,
        operation: impl FnOnce() -> Result<T>,
        finalize: impl FnOnce(&mut Vec<CapturedChange>) -> Result<()>,
    ) -> Result<(T, Vec<CapturedChange>)> {
        let _guard = BranchReadTraceGuard::enter(Arc::clone(&self.state));
        self.run_inner(operation, finalize, true)
    }

    fn run_inner<T>(
        &self,
        operation: impl FnOnce() -> Result<T>,
        finalize: impl FnOnce(&mut Vec<CapturedChange>) -> Result<()>,
        refresh_bindings: bool,
    ) -> Result<(T, Vec<CapturedChange>)> {
        let checkpoint = {
            let mut state = lock(&self.state);
            state.capture_budget.reset();
            state.events.len()
        };
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
                let finalized = self.with_internal(|| finalize(&mut events)).and_then(|_| {
                    if refresh_bindings {
                        self.with_internal(|| self.refresh_catalog())
                    } else {
                        Ok(())
                    }
                });
                if let Err(error) = finalized {
                    return Err(self.rollback_statement_error(error));
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
                Err(self.rollback_statement_error(error))
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

    fn rollback_statement_error(&self, statement: Error) -> Error {
        preserve_statement_error(statement, self.rollback_statement())
    }

    fn refresh_catalog(&self) -> Result<()> {
        let catalog = CatalogSnapshot::load(self.connection)?;
        lock(&self.state).catalog = catalog;
        Ok(())
    }

    fn with_catalog<T>(&self, operation: impl FnOnce(&CatalogSnapshot) -> Result<T>) -> Result<T> {
        operation(&lock(&self.state).catalog)
    }

    fn read_prefixes(&self) -> Result<Vec<Key>> {
        let state = lock(&self.state);
        if !state.unresolved_read_tables.is_empty() {
            return Err(Error::UnsupportedSql(
                "serializable reads require synchronized table identities",
            ));
        }
        Ok(state
            .read_tables
            .iter()
            .copied()
            .map(TableId::from_bytes)
            .map(table_prefix)
            .collect())
    }
}

fn preserve_statement_error(statement: Error, rollback: Result<()>) -> Error {
    match rollback {
        Ok(()) => statement,
        Err(rollback) => Error::StatementRollback {
            statement: Box::new(statement),
            rollback: Box::new(rollback),
        },
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

struct BranchReadTraceGuard {
    state: Arc<Mutex<BranchHookState>>,
}

impl BranchReadTraceGuard {
    fn enter(state: Arc<Mutex<BranchHookState>>) -> Self {
        lock(&state).read_trace_suppression_depth += 1;
        Self { state }
    }
}

impl Drop for BranchReadTraceGuard {
    fn drop(&mut self) {
        let mut state = lock(&self.state);
        state.read_trace_suppression_depth = state
            .read_trace_suppression_depth
            .checked_sub(1)
            .expect("branch read-trace suppression depth is balanced");
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

pub(super) struct BranchUpdate<T> {
    pub value: T,
    pub proposal: Option<CommitProposal>,
    pub history_pin: Option<HistoryPin>,
}

pub(super) fn run_branch_update<H, T>(
    snapshot: CommitSnapshot,
    rowid_allocator: crate::rowid::RowidAllocator,
    isolation: IsolationLevel,
    operation: impl FnOnce(&mut UpdateTransaction<'_, H>) -> Result<T>,
) -> Result<BranchUpdate<T>>
where
    H: ServerHandle + Send + Sync + 'static,
{
    let CommitSnapshot {
        physical,
        logical,
        history_pin,
    } = snapshot;
    let branch = WritableBranch::open(physical, OverlayOptions::default())
        .map_err(|error| Error::Branch(error.to_string()))?;
    crate::rowid::install(branch.connection(), rowid_allocator)?;
    let mut update = UpdateTransaction::branch(branch.connection(), isolation)?;
    let value = operation(&mut update)?;
    let (operations, footprint) = update.into_branch_parts()?;
    let proposal = if operations.is_empty() {
        None
    } else {
        let transaction = MultiliteTransaction::from_compiled_operations(operations)?;
        Some(CommitProposal::from_compiled_transaction(
            logical,
            isolation,
            transaction,
            footprint,
        )?)
    };
    Ok(BranchUpdate {
        value,
        proposal,
        history_pin,
    })
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
        let snapshot = self.issue_branch_snapshot(true)?;
        let BranchUpdate {
            value,
            proposal,
            history_pin: _history_pin,
        } = run_branch_update(
            snapshot,
            self.rowid_allocator.clone(),
            options.isolation_level(),
            operation,
        )?;
        if let Some(proposal) = proposal {
            let receipt = self.commit_proposal(proposal)?;
            self.finish_branch_write(receipt)?;
        }
        Ok(value)
    }

    fn finish_branch_write(
        self: &std::sync::Arc<Self>,
        receipt: crate::commit::proposal::CommitReceipt,
    ) -> Result<()> {
        pollster::block_on(self.finish_branch_write_async(receipt))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logical::row::CapturedRow;
    use crate::value::StoredValue;

    fn captured_account(id: i64, email: &str, body: &str) -> CapturedRow {
        CapturedRow {
            table: "accounts".into(),
            rowid: id,
            values: vec![
                StoredValue::Integer(id),
                StoredValue::Text(email.as_bytes().to_vec()),
                StoredValue::Text(body.as_bytes().to_vec()),
            ],
        }
    }

    #[test]
    fn upsert_hook_stream_reports_only_sqlites_actual_row_effects_in_order() {
        let connection = Connection::open_in_memory().unwrap();
        crate::catalog::initialize(&connection).unwrap();
        connection
            .execute_batch(
                "CREATE TABLE accounts (
                    id INTEGER PRIMARY KEY,
                    email TEXT NOT NULL UNIQUE,
                    body TEXT NOT NULL
                );
                INSERT INTO accounts VALUES (1, 'one', 'start')",
            )
            .unwrap();
        let hooks = BranchHooks::install(&connection, false).unwrap();

        let (changed, events) = hooks
            .run(
                || {
                    Ok(connection.execute(
                        "INSERT INTO accounts VALUES
                            (2, 'two', 'inserted'),
                            (3, 'two', 'second-touch'),
                            (9, 'one', 'updated-existing'),
                            (10, 'one', 'where-false')
                         ON CONFLICT(email) DO UPDATE SET body = excluded.body
                         WHERE excluded.id <> 10",
                        (),
                    )?)
                },
                |_| Ok(()),
            )
            .unwrap();

        assert_eq!(changed, 3);
        assert_eq!(
            events,
            vec![
                CapturedChange::Insert(captured_account(2, "two", "inserted")),
                CapturedChange::Update {
                    before: captured_account(2, "two", "inserted"),
                    after: captured_account(2, "two", "second-touch"),
                },
                CapturedChange::Update {
                    before: captured_account(1, "one", "start"),
                    after: captured_account(1, "one", "updated-existing"),
                },
            ]
        );
    }

    #[test]
    fn replacement_hook_stream_reports_every_implicit_victim() {
        for insert in ["INSERT OR REPLACE", "REPLACE"] {
            let connection = Connection::open_in_memory().unwrap();
            crate::catalog::initialize(&connection).unwrap();
            connection
                .execute_batch(
                    "CREATE TABLE accounts (
                        id INTEGER PRIMARY KEY,
                        email TEXT NOT NULL UNIQUE,
                        handle TEXT NOT NULL UNIQUE,
                        body TEXT NOT NULL
                    );
                    INSERT INTO accounts VALUES
                        (1, 'one@example.com', 'one', 'first'),
                        (2, 'two@example.com', 'two', 'second')",
                )
                .unwrap();
            let hooks = BranchHooks::install(&connection, false).unwrap();

            let (_, events) = hooks
                .run(
                    || {
                        Ok(connection.execute(
                            &format!(
                                "{insert} INTO accounts VALUES
                                 (3, 'one@example.com', 'two', 'replacement')"
                            ),
                            (),
                        )?)
                    },
                    |_| Ok(()),
                )
                .unwrap();

            let mut deleted = events
                .iter()
                .filter_map(|event| match event {
                    CapturedChange::Delete(row) => match row.values[0] {
                        StoredValue::Integer(id) => Some(id),
                        _ => None,
                    },
                    _ => None,
                })
                .collect::<Vec<_>>();
            deleted.sort_unstable();
            assert_eq!(deleted, [1, 2]);
            assert_eq!(
                events.last(),
                Some(&CapturedChange::Insert(CapturedRow {
                    table: "accounts".into(),
                    rowid: 3,
                    values: vec![
                        StoredValue::Integer(3),
                        StoredValue::Text(b"one@example.com".to_vec()),
                        StoredValue::Text(b"two".to_vec()),
                        StoredValue::Text(b"replacement".to_vec()),
                    ],
                }))
            );
            assert_eq!(events.len(), 3);
        }
    }

    #[test]
    fn update_or_replace_hook_stream_reports_deleted_victim_and_survivor_update() {
        let connection = Connection::open_in_memory().unwrap();
        crate::catalog::initialize(&connection).unwrap();
        connection
            .execute_batch(
                "CREATE TABLE accounts (
                    id INTEGER PRIMARY KEY,
                    email TEXT NOT NULL UNIQUE,
                    body TEXT NOT NULL
                );
                INSERT INTO accounts VALUES
                    (1, 'one@example.com', 'first'),
                    (2, 'two@example.com', 'second')",
            )
            .unwrap();
        let hooks = BranchHooks::install(&connection, false).unwrap();

        let (_, events) = hooks
            .run(
                || {
                    Ok(connection.execute(
                        "UPDATE OR REPLACE accounts
                         SET email = 'two@example.com', body = 'replaced'
                         WHERE id = 1",
                        (),
                    )?)
                },
                |_| Ok(()),
            )
            .unwrap();

        assert_eq!(events.len(), 2);
        assert!(events.iter().any(|event| {
            matches!(
                event,
                CapturedChange::Delete(row)
                    if row.values[0] == StoredValue::Integer(2)
            )
        }));
        assert!(events.iter().any(|event| {
            matches!(
                event,
                CapturedChange::Update { before, after }
                    if before.values[0] == StoredValue::Integer(1)
                        && after.values[0] == StoredValue::Integer(1)
                        && after.values[1]
                            == StoredValue::Text(b"two@example.com".to_vec())
            )
        }));
    }

    #[test]
    fn replacement_victims_count_toward_the_atomic_capture_limit() {
        let connection = Connection::open_in_memory().unwrap();
        crate::catalog::initialize(&connection).unwrap();
        connection
            .execute_batch(
                "CREATE TABLE accounts (
                    id INTEGER PRIMARY KEY,
                    email TEXT NOT NULL UNIQUE,
                    handle TEXT NOT NULL UNIQUE
                );
                INSERT INTO accounts VALUES
                    (1, 'one@example.com', 'one'),
                    (2, 'two@example.com', 'two')",
            )
            .unwrap();
        let hooks = BranchHooks::install_with_budget(
            &connection,
            false,
            CaptureBudget::with_limits(2, usize::MAX),
        )
        .unwrap();

        assert!(matches!(
            hooks.run(
                || {
                    connection.execute(
                        "REPLACE INTO accounts VALUES
                            (3, 'one@example.com', 'two')",
                        (),
                    )?;
                    Ok(())
                },
                |_| Ok(()),
            ),
            Err(Error::CaptureLimitExceeded {
                resource: "row-change count",
                limit: 2,
            })
        ));
        assert_eq!(
            connection
                .prepare("SELECT id, email, handle FROM accounts ORDER BY id")
                .unwrap()
                .query_map((), |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                    ))
                })
                .unwrap()
                .collect::<rusqlite::Result<Vec<_>>>()
                .unwrap(),
            [
                (1, "one@example.com".into(), "one".into()),
                (2, "two@example.com".into(), "two".into()),
            ]
        );
    }

    #[test]
    fn cascade_victims_count_toward_the_atomic_capture_limit() {
        let connection = Connection::open_in_memory().unwrap();
        crate::catalog::initialize(&connection).unwrap();
        connection
            .execute_batch(
                "PRAGMA foreign_keys = ON;
                 CREATE TABLE parents (id INTEGER PRIMARY KEY);
                 CREATE TABLE children (
                    id INTEGER PRIMARY KEY,
                    parent INTEGER REFERENCES parents(id) ON DELETE CASCADE
                 );
                 INSERT INTO parents VALUES (1);
                 INSERT INTO children VALUES (10, 1), (11, 1)",
            )
            .unwrap();
        let hooks = BranchHooks::install_with_budget(
            &connection,
            false,
            CaptureBudget::with_limits(2, usize::MAX),
        )
        .unwrap();

        assert!(matches!(
            hooks.run(
                || {
                    connection.execute("DELETE FROM parents WHERE id = 1", ())?;
                    Ok(())
                },
                |_| Ok(()),
            ),
            Err(Error::CaptureLimitExceeded {
                resource: "row-change count",
                limit: 2,
            })
        ));
        assert_eq!(
            connection
                .query_row("SELECT count(*) FROM parents", (), |row| row
                    .get::<_, i64>(0))
                .unwrap(),
            1
        );
        assert_eq!(
            connection
                .query_row("SELECT count(*) FROM children", (), |row| row
                    .get::<_, i64>(0))
                .unwrap(),
            2
        );
    }

    #[test]
    fn update_cascade_victims_count_toward_the_atomic_capture_limit() {
        let connection = Connection::open_in_memory().unwrap();
        crate::catalog::initialize(&connection).unwrap();
        connection
            .execute_batch(
                "PRAGMA foreign_keys = ON;
                 CREATE TABLE parents (id INTEGER PRIMARY KEY);
                 CREATE TABLE children (
                    id INTEGER PRIMARY KEY,
                    parent INTEGER REFERENCES parents(id) ON UPDATE CASCADE
                 );
                 INSERT INTO parents VALUES (1);
                 INSERT INTO children VALUES (10, 1), (11, 1)",
            )
            .unwrap();
        let hooks = BranchHooks::install_with_budget(
            &connection,
            false,
            CaptureBudget::with_limits(2, usize::MAX),
        )
        .unwrap();

        assert!(matches!(
            hooks.run(
                || {
                    connection.execute("UPDATE parents SET id = 2 WHERE id = 1", ())?;
                    Ok(())
                },
                |_| Ok(()),
            ),
            Err(Error::CaptureLimitExceeded {
                resource: "row-change count",
                limit: 2,
            })
        ));
        assert_eq!(
            connection
                .query_row("SELECT id FROM parents", (), |row| row.get::<_, i64>(0))
                .unwrap(),
            1
        );
        assert_eq!(
            connection
                .prepare("SELECT parent FROM children ORDER BY id")
                .unwrap()
                .query_map((), |row| row.get::<_, i64>(0))
                .unwrap()
                .collect::<rusqlite::Result<Vec<_>>>()
                .unwrap(),
            [1, 1]
        );
    }

    #[test]
    fn capture_limit_rolls_back_the_complete_sqlite_statement() {
        let connection = Connection::open_in_memory().unwrap();
        crate::catalog::initialize(&connection).unwrap();
        connection
            .execute_batch("CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)")
            .unwrap();
        let hooks = BranchHooks::install_with_budget(
            &connection,
            false,
            CaptureBudget::with_limits(1, usize::MAX),
        )
        .unwrap();

        assert!(matches!(
            hooks.run(
                || {
                    connection.execute("INSERT INTO notes VALUES (1, 'one'), (2, 'two')", ())?;
                    Ok(())
                },
                |_| Ok(()),
            ),
            Err(Error::CaptureLimitExceeded {
                resource: "row-change count",
                limit: 1,
            })
        ));
        assert_eq!(
            connection
                .query_row("SELECT count(*) FROM notes", (), |row| row.get::<_, i64>(0))
                .unwrap(),
            0
        );
    }

    #[test]
    fn returning_capture_limit_stops_mapping_and_leaves_hooks_reusable() {
        let connection = Connection::open_in_memory().unwrap();
        crate::catalog::initialize(&connection).unwrap();
        connection
            .execute_batch("CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)")
            .unwrap();
        let mut update =
            UpdateTransaction::<crate::database::OfflineServer>::branch_with_capture_budget(
                &connection,
                IsolationLevel::Snapshot,
                CaptureBudget::with_limits(2, usize::MAX),
            )
            .unwrap();
        let mut mapped = 0;

        assert!(matches!(
            update.query_captured(
                "INSERT INTO notes VALUES
                    (1, 'one'), (2, 'two'), (3, 'three'), (4, 'four')
                 RETURNING id",
                (),
                |row| {
                    mapped += 1;
                    row.get::<_, i64>(0)
                },
                |_, _| Ok(None),
            ),
            Err(Error::CaptureLimitExceeded {
                resource: "row-change count",
                limit: 2,
            })
        ));
        assert_eq!(mapped, 0);
        assert_eq!(
            connection
                .query_row("SELECT count(*) FROM notes", (), |row| row.get::<_, i64>(0))
                .unwrap(),
            0
        );

        assert_eq!(
            update
                .query_captured(
                    "INSERT INTO notes VALUES (5, 'five') RETURNING id",
                    (),
                    |row| row.get::<_, i64>(0),
                    |_, _| Ok(None),
                )
                .unwrap(),
            [5]
        );
    }

    #[test]
    fn rollback_failure_preserves_both_errors() {
        let error = preserve_statement_error(
            Error::CaptureInvariant("statement failed"),
            Err(Error::CaptureInvariant("rollback failed")),
        );

        let Error::StatementRollback {
            statement,
            rollback,
        } = error
        else {
            panic!("expected a compound rollback error");
        };
        assert!(matches!(
            *statement,
            Error::CaptureInvariant("statement failed")
        ));
        assert!(matches!(
            *rollback,
            Error::CaptureInvariant("rollback failed")
        ));
    }

    #[test]
    fn successful_rollback_returns_the_original_error_unchanged() {
        assert!(matches!(
            preserve_statement_error(Error::CommitConflict("stale".into()), Ok(())),
            Error::CommitConflict(message) if message == "stale"
        ));
    }
}
