//! General Multilite database identity and Homebase lifecycle.

mod async_api;
mod authority;
mod commit_backend;
mod connection;
mod pending;
mod policy;
mod rebase;
mod store;
mod update;
mod view;

use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use homebase_client::cipher::{SpaceEnvelope, SystemNonceSource};
use homebase_client::meta::{MetaStore, OplogCursors};
use homebase_client::server::UnreachableSpace;
use homebase_client::{Client, ServerHandle};
use homebase_core::clock::{Lineage, SystemHybridClock};
use homebase_core::messages::KernelError;
use homebase_core::space::SpaceId;
use homebase_core::tag::{AdmissionSeq, DeviceId, DeviceSeq};
use pollster::block_on;
use rusqlite::hooks::{AuthAction, AuthContext, Authorization, PreUpdateCase};
use rusqlite::{Connection as SqliteConnection, Row};

use crate::commit::committer::{CommitHistory, CommitSnapshot, Committer, WeakCommitter};
use crate::commit::history;
use crate::commit::proposal::{CommitProposal, CommitReceipt};
use crate::connection::ConnectionOwner;
use crate::logical::row::{CapturedChange, CapturedRow, StoredValue};
use crate::metastore::SqliteOrderedStore;
use crate::rowid;
use crate::runtime::{ExecutionMode, HookPolicy, RuntimeConnection};
use crate::{Error, Params, Result};
use crate::{catalog, sql};

use self::authority::Authority;
use self::commit_backend::DatabaseCommitBackend;
use self::policy::PolicyActor;
use self::store::{CanonicalMetaSink, CanonicalRouter, DatabaseMetaStore};

pub use self::connection::Connection;
pub use self::policy::SyncPolicy;
pub use self::update::UpdateTransaction;
pub use self::view::{TransactionStatement, ViewTransaction};
pub use crate::logical::isolation::{IsolationLevel, UpdateOptions};

const REPLICA_INVITATION_VERSION: u8 = 1;

/// Result of fetching this database's available server admissions.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PullOutcome {
    through: AdmissionSeq,
}

impl PullOutcome {
    /// Last server admission sequence durably captured by this database.
    ///
    /// Capturing an admission does not imply that it has been rebased or
    /// applied to SQLite.
    pub fn captured_through(&self) -> u64 {
        self.through.0
    }
}

/// Result of pushing this database's active local submission window.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PushOutcome {
    /// Every currently active submission was admitted.
    Drained,
    /// Admission stopped at a kernel rejection.
    Rejected(PushRejection),
}

/// Opaque record of a rejection against one observed local submission window.
///
/// A later rollback will validate this identity and window before changing
/// local state. Merely receiving the handle never performs repair.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PushRejection {
    database_id: DatabaseId,
    device_id: DeviceId,
    failed_at: DeviceSeq,
    submit_cursors: OplogCursors,
    error: KernelError,
}

impl PushRejection {
    /// Homebase sequence of the first rejected local submission.
    pub fn failed_sequence(&self) -> u64 {
        self.failed_at.0
    }

    /// Kernel invariant that rejected the submission.
    pub fn error(&self) -> &KernelError {
        &self.error
    }
}

/// Public identity shared by every replica of a Multilite database.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct DatabaseId {
    space_id: [u8; 16],
}

impl DatabaseId {
    /// Reconstruct an id from its complete plaintext representation.
    pub const fn from_bytes(space_id: [u8; 16]) -> Self {
        Self { space_id }
    }

    /// Return the complete plaintext representation.
    pub const fn to_bytes(self) -> [u8; 16] {
        self.space_id
    }

    fn space_id(self) -> SpaceId {
        SpaceId(self.space_id)
    }
}

/// Opaque, versioned material used to initialize another local replica.
///
/// The current format carries only the public database identity. A future
/// encrypted format can carry or unlock the space envelope without changing
/// the open API.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReplicaInvitation {
    database_id: DatabaseId,
}

impl ReplicaInvitation {
    /// Public identity named by this invitation.
    pub const fn database_id(&self) -> DatabaseId {
        self.database_id
    }

    /// Encode the invitation for transport to another replica.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(17);
        bytes.push(REPLICA_INVITATION_VERSION);
        bytes.extend_from_slice(&self.database_id.to_bytes());
        bytes
    }

    /// Decode one complete invitation, rejecting unknown or malformed forms.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        let [version, id @ ..] = bytes else {
            return Err(Error::InvalidReplicaInvitation);
        };
        if *version != REPLICA_INVITATION_VERSION || id.len() != 16 {
            return Err(Error::InvalidReplicaInvitation);
        }
        let space_id = id.try_into().map_err(|_| Error::InvalidReplicaInvitation)?;
        Ok(Self {
            database_id: DatabaseId::from_bytes(space_id),
        })
    }

    fn new(database_id: DatabaseId) -> Self {
        Self { database_id }
    }
}

/// Default endpoint type for a database opened without a server handle.
pub type OfflineServer = fn(&SpaceId) -> Option<UnreachableSpace>;

/// Optional identity and server configuration for opening a database.
pub struct OpenOptions<H = OfflineServer>
where
    H: ServerHandle,
{
    invitation: Option<ReplicaInvitation>,
    server: H,
    authority: bool,
    sync_policy: SyncPolicy,
    isolation_level: IsolationLevel,
}

impl Default for OpenOptions<OfflineServer> {
    fn default() -> Self {
        Self::new()
    }
}

impl OpenOptions<OfflineServer> {
    /// Default to a locally initialized database and an offline server route.
    pub fn new() -> Self {
        Self {
            invitation: None,
            server: offline_server,
            authority: false,
            sync_policy: SyncPolicy::default(),
            isolation_level: IsolationLevel::default(),
        }
    }
}

impl<H: ServerHandle> OpenOptions<H> {
    /// Initialize from, or verify against, a replica invitation.
    pub fn invitation(mut self, invitation: ReplicaInvitation) -> Self {
        self.invitation = Some(invitation);
        self
    }

    /// Select how local reads and writes interact with authority.
    pub fn sync_policy(mut self, policy: SyncPolicy) -> Self {
        self.sync_policy = policy;
        self
    }

    /// Select the default isolation level for managed updates.
    pub fn isolation_level(mut self, isolation_level: IsolationLevel) -> Self {
        self.isolation_level = isolation_level;
        self
    }

    /// Replace the server route while retaining all other options.
    pub fn server<S: ServerHandle>(self, server: S) -> OpenOptions<S> {
        OpenOptions {
            invitation: self.invitation,
            server,
            authority: true,
            sync_policy: self.sync_policy,
            isolation_level: self.isolation_level,
        }
    }

    fn validate(&self) -> Result<()> {
        if !self.authority {
            match self.sync_policy {
                SyncPolicy::LocalOnly => {}
                SyncPolicy::LocalFirst { .. } => {
                    return Err(Error::AuthorityRequired("local-first policy"));
                }
                SyncPolicy::Remote => return Err(Error::AuthorityRequired("remote policy")),
            }
        }
        Ok(())
    }
}

pub(crate) type DatabaseClient<H> =
    Client<DatabaseMetaStore, H, SystemHybridClock, SystemNonceSource>;

pub(crate) struct DatabaseRuntime {
    inner: RuntimeConnection<DatabaseHooks>,
}

impl DatabaseRuntime {
    fn new(owner: ConnectionOwner) -> Result<Self> {
        Ok(Self {
            inner: RuntimeConnection::new(owner, DatabaseHooks)?,
        })
    }
}

impl Deref for DatabaseRuntime {
    type Target = RuntimeConnection<DatabaseHooks>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

pub(crate) struct DatabaseHooks;

impl HookPolicy for DatabaseHooks {
    type Event = CapturedChange;

    fn authorize(&mut self, mode: ExecutionMode, context: AuthContext<'_>) -> Authorization {
        authorize_database(mode, &context)
    }

    fn preupdate(
        &mut self,
        mode: ExecutionMode,
        database: &str,
        table: &str,
        update: &PreUpdateCase,
    ) -> Result<Option<Self::Event>> {
        capture_change(mode, database, table, update)
    }
}

fn capture_change(
    mode: ExecutionMode,
    database: &str,
    table: &str,
    update: &PreUpdateCase,
) -> Result<Option<CapturedChange>> {
    if mode != ExecutionMode::Public
        || database != "main"
        || is_schema_table(table)
        || sql::has_multilite_prefix(table)
    {
        return Ok(None);
    }
    let event = match update {
        PreUpdateCase::Insert(values) => {
            let captured = (0..values.get_column_count())
                .map(|index| {
                    values
                        .get_new_column_value(index)
                        .map(StoredValue::capture)
                        .map_err(Error::from)
                })
                .collect::<Result<Vec<_>>>()?;
            CapturedChange::Insert(CapturedRow {
                table: table.to_owned(),
                rowid: values.get_new_row_id(),
                values: captured,
            })
        }
        PreUpdateCase::Delete(values) => {
            let captured = (0..values.get_column_count())
                .map(|index| {
                    values
                        .get_old_column_value(index)
                        .map(StoredValue::capture)
                        .map_err(Error::from)
                })
                .collect::<Result<Vec<_>>>()?;
            CapturedChange::Delete(CapturedRow {
                table: table.to_owned(),
                rowid: values.get_old_row_id(),
                values: captured,
            })
        }
        PreUpdateCase::Update {
            old_value_accessor: old,
            new_value_accessor: new,
        } => {
            if old.get_column_count() != new.get_column_count() {
                return Err(Error::CaptureInvariant(
                    "UPDATE before and after row widths differ",
                ));
            }
            let before = (0..old.get_column_count())
                .map(|index| {
                    old.get_old_column_value(index)
                        .map(StoredValue::capture)
                        .map_err(Error::from)
                })
                .collect::<Result<Vec<_>>>()?;
            let after = (0..new.get_column_count())
                .map(|index| {
                    new.get_new_column_value(index)
                        .map(StoredValue::capture)
                        .map_err(Error::from)
                })
                .collect::<Result<Vec<_>>>()?;
            CapturedChange::Update {
                before: CapturedRow {
                    table: table.to_owned(),
                    rowid: old.get_old_row_id(),
                    values: before,
                },
                after: CapturedRow {
                    table: table.to_owned(),
                    rowid: new.get_new_row_id(),
                    values: after,
                },
            }
        }
        PreUpdateCase::Unknown => {
            return Err(Error::CaptureInvariant(
                "public table mutation kind is unknown",
            ));
        }
    };
    Ok(Some(event))
}

/// An opened general Multilite database.
pub(crate) struct Database<H: ServerHandle> {
    owner: ConnectionOwner,
    database_id: DatabaseId,
    client: Arc<DatabaseClient<H>>,
    policy: PolicyActor,
    isolation_level: IsolationLevel,
    committer: Committer,
    rowid_allocator: rowid::RowidAllocator,
    authority: Authority,
}

impl Database<OfflineServer> {
    pub(crate) fn open(path: impl AsRef<Path>) -> Result<Arc<Self>> {
        Self::open_with(path, OpenOptions::new())
    }
}

impl<H: ServerHandle + Send + Sync + 'static> Database<H> {
    pub(crate) fn open_with(path: impl AsRef<Path>, options: OpenOptions<H>) -> Result<Arc<Self>> {
        options.validate()?;
        let path = path.as_ref().to_owned();
        let owner = ConnectionOwner::open(&path)?;
        owner.with_connection(|connection| {
            connection.pragma_update(None, "journal_mode", "WAL")?;
            connection.pragma_update(None, "wal_autocheckpoint", 0)?;
            connection.pragma_update(None, "foreign_keys", true)?;
            Ok::<_, rusqlite::Error>(())
        })?;
        let database = Arc::new(open_on(owner, path, options)?);
        database.start_policy_actor()?;
        Ok(database)
    }

    pub(crate) fn start_policy_actor(self: &Arc<Self>) -> Result<()> {
        self.policy.start(Arc::downgrade(self))?;
        if self.policy.write_delay().is_some() {
            let cursors = self.submit_cursors()?;
            if cursors.neck < cursors.tail {
                self.policy.schedule(std::time::Duration::ZERO);
            }
        }
        Ok(())
    }

    pub(crate) fn sync_policy(&self) -> SyncPolicy {
        self.policy.policy()
    }

    pub(crate) fn isolation_level(&self) -> IsolationLevel {
        self.isolation_level
    }

    pub(crate) fn database_id(&self) -> DatabaseId {
        self.database_id
    }

    pub(crate) fn replica_invitation(&self) -> ReplicaInvitation {
        ReplicaInvitation::new(self.database_id)
    }

    pub(crate) fn device_id(&self) -> [u8; 16] {
        self.client.device().0
    }

    pub(crate) fn runtime(&self) -> Result<DatabaseRuntime> {
        DatabaseRuntime::new(self.owner.clone())
    }

    pub(crate) fn execute<Q: Params>(
        self: &Arc<Self>,
        runtime: &DatabaseRuntime,
        sql: &str,
        params: Q,
    ) -> Result<usize> {
        let validated = sql::validate_execute(sql)?;
        self.execute_validated(runtime, sql, params, validated)
    }

    fn execute_validated<Q: Params>(
        self: &Arc<Self>,
        runtime: &DatabaseRuntime,
        sql: &str,
        params: Q,
        validated: sql::ValidatedExecute,
    ) -> Result<usize> {
        self.update(runtime, |update| {
            update.execute_validated(sql, params, validated)
        })
    }

    pub(crate) fn query<T, P, F>(
        self: &Arc<Self>,
        runtime: &DatabaseRuntime,
        sql: &str,
        params: P,
        map: F,
    ) -> Result<Vec<T>>
    where
        P: Params,
        F: FnMut(&Row<'_>) -> rusqlite::Result<T>,
    {
        let validated = sql::validate_statement(sql)?;
        if validated.output() != sql::StatementOutput::Rows {
            return Err(Error::StatementModeMismatch);
        }
        match validated {
            sql::ValidatedStatement::Read => {
                self.view(runtime, |view| view.query_prevalidated(sql, params, map))
            }
            sql::ValidatedStatement::Write(validated) => {
                self.query_write_validated(runtime, sql, params, map, *validated)
            }
        }
    }

    fn query_write_validated<T, P, F>(
        self: &Arc<Self>,
        runtime: &DatabaseRuntime,
        sql: &str,
        params: P,
        map: F,
        validated: sql::ValidatedExecute,
    ) -> Result<Vec<T>>
    where
        P: Params,
        F: FnMut(&Row<'_>) -> rusqlite::Result<T>,
    {
        self.update(runtime, |update| {
            update.query_validated(sql, params, map, validated)
        })
    }

    pub(crate) fn push(self: &Arc<Self>) -> Result<PushOutcome> {
        block_on(self.push_async())
    }

    /// Undo the speculative SQLite effects covered by one definitive push
    /// rejection and retire that exact active submit window.
    pub(crate) fn rollback(self: &Arc<Self>, rejection: &PushRejection) -> Result<()> {
        block_on(self.rollback_async(rejection.clone()))
    }

    pub(crate) fn pull(self: &Arc<Self>) -> Result<PullOutcome> {
        block_on(self.pull_async())
    }

    pub(crate) fn prepare(
        self: &Arc<Self>,
        runtime: &Arc<DatabaseRuntime>,
        sql: &str,
    ) -> Result<Statement<H>> {
        let _ = runtime;
        let validated = sql::validate_statement(sql)?;
        Ok(Statement {
            database: Arc::clone(self),
            runtime: Arc::clone(runtime),
            sql: sql.to_owned(),
            validated,
        })
    }

    #[cfg(test)]
    pub(crate) fn with_connection<T>(&self, operation: impl FnOnce(&SqliteConnection) -> T) -> T {
        self.owner.with_connection(operation)
    }

    fn submit_cursors(&self) -> Result<OplogCursors> {
        let store = DatabaseMetaStore::read_only(self.owner.clone());
        Ok(block_on(store.oplog_cursors(self.database_id.space_id()))?)
    }

    fn issue_branch_snapshot(self: &Arc<Self>, track_for_commit: bool) -> Result<CommitSnapshot> {
        self.committer.capture_snapshot_blocking(track_for_commit)
    }

    fn issue_view_snapshot(self: &Arc<Self>) -> Result<crate::branch::snapshot::PinnedReader> {
        self.committer.capture_view_blocking()
    }

    fn commit_proposal(self: &Arc<Self>, proposal: CommitProposal) -> Result<CommitReceipt> {
        self.committer.propose_blocking(proposal)
    }

    fn refresh_read(self: &Arc<Self>, runtime: &DatabaseRuntime) -> Result<()> {
        let _ = runtime;
        block_on(self.refresh_read_async())
    }
}

fn authorize_database(mode: ExecutionMode, context: &AuthContext<'_>) -> Authorization {
    if mode != ExecutionMode::Public {
        return Authorization::Allow;
    }

    match context.action {
        AuthAction::Select | AuthAction::Function { .. } | AuthAction::Recursive => {
            Authorization::Allow
        }
        AuthAction::Read { table_name, .. } => authorize_read(context.database_name, table_name),
        AuthAction::CreateTable { table_name } => {
            authorize_user_table(context.database_name, table_name)
        }
        AuthAction::DropTable { table_name } => {
            authorize_user_table(context.database_name, table_name)
        }
        AuthAction::AlterTable {
            database_name,
            table_name,
        } => authorize_user_table(Some(database_name), table_name),
        AuthAction::CreateIndex {
            index_name,
            table_name,
        } if index_name.starts_with("sqlite_autoindex_") => {
            authorize_user_table(context.database_name, table_name)
        }
        AuthAction::CreateIndex {
            index_name,
            table_name,
        }
        | AuthAction::DropIndex {
            index_name,
            table_name,
        } if !sql::has_multilite_prefix(index_name)
            && !sql::is_sqlite_internal_table(index_name) =>
        {
            authorize_user_table(context.database_name, table_name)
        }
        AuthAction::Reindex { index_name }
            if !sql::has_multilite_prefix(index_name)
                && !sql::is_sqlite_internal_table(index_name) =>
        {
            authorize_main(context.database_name)
        }
        AuthAction::Pragma {
            pragma_name,
            pragma_value: Some(table_name),
        } if pragma_name.eq_ignore_ascii_case("quick_check") => {
            authorize_user_table(Some("main"), table_name)
        }
        AuthAction::Insert { table_name } if is_schema_table(table_name) => {
            authorize_schema(context.database_name)
        }
        AuthAction::Update { table_name, .. } if is_schema_table(table_name) => {
            authorize_schema(context.database_name)
        }
        AuthAction::Delete { table_name } if is_schema_table(table_name) => {
            authorize_schema(context.database_name)
        }
        AuthAction::Insert { table_name } => {
            authorize_user_table(context.database_name, table_name)
        }
        AuthAction::Delete { table_name } => {
            authorize_user_table(context.database_name, table_name)
        }
        AuthAction::Update { table_name, .. } => {
            authorize_user_table(context.database_name, table_name)
        }
        _ => Authorization::Deny,
    }
}

fn authorize_public(context: AuthContext<'_>) -> Authorization {
    authorize_database(ExecutionMode::Public, &context)
}

fn authorize_read(database: Option<&str>, table: &str) -> Authorization {
    if is_schema_table(table) {
        if is_main(database) || database == Some("temp") {
            Authorization::Allow
        } else {
            Authorization::Deny
        }
    } else {
        authorize_user_table(database, table)
    }
}

fn authorize_user_table(database: Option<&str>, table: &str) -> Authorization {
    if is_main(database)
        && !sql::has_multilite_prefix(table)
        && !sql::is_sqlite_internal_table(table)
    {
        Authorization::Allow
    } else {
        Authorization::Deny
    }
}

fn authorize_main(database: Option<&str>) -> Authorization {
    if is_main(database) {
        Authorization::Allow
    } else {
        Authorization::Deny
    }
}

fn authorize_schema(database: Option<&str>) -> Authorization {
    if is_main(database) || database == Some("temp") {
        Authorization::Allow
    } else {
        Authorization::Deny
    }
}

fn is_main(database: Option<&str>) -> bool {
    matches!(database, None | Some("main"))
}

fn is_schema_table(table: &str) -> bool {
    table.eq_ignore_ascii_case("sqlite_master")
        || table.eq_ignore_ascii_case("sqlite_schema")
        || table.eq_ignore_ascii_case("sqlite_temp_master")
        || table.eq_ignore_ascii_case("sqlite_temp_schema")
}

/// A validated prepared statement owned by a Multilite database.
pub struct Statement<H = OfflineServer>
where
    H: ServerHandle + Send + Sync + 'static,
{
    database: Arc<Database<H>>,
    runtime: Arc<DatabaseRuntime>,
    sql: String,
    validated: sql::ValidatedStatement,
}

impl<H: ServerHandle + Send + Sync + 'static> Statement<H> {
    /// Whether this statement is read-only.
    pub fn readonly(&self) -> bool {
        self.validated.access() == sql::StatementAccess::Read
    }

    /// Execute a prepared rowless mutating statement.
    pub fn execute<P: Params>(&mut self, params: P) -> Result<usize> {
        let sql::ValidatedStatement::Write(validated) = &self.validated else {
            return Err(Error::StatementModeMismatch);
        };
        if validated.output() == sql::StatementOutput::Rows {
            return Err(rusqlite::Error::ExecuteReturnedResults.into());
        }
        self.database
            .execute_validated(&self.runtime, &self.sql, params, (**validated).clone())
    }

    /// Asynchronously execute a prepared rowless mutating statement.
    pub async fn execute_async<P>(&self, params: P) -> Result<usize>
    where
        P: Params + Send + 'static,
    {
        let sql::ValidatedStatement::Write(validated) = &self.validated else {
            return Err(Error::StatementModeMismatch);
        };
        if validated.output() == sql::StatementOutput::Rows {
            return Err(rusqlite::Error::ExecuteReturnedResults.into());
        }
        self.database
            .execute_validated_async(self.sql.clone(), params, (**validated).clone())
            .await
    }

    /// Execute the query and eagerly map every row.
    pub fn query_map<T, P, F>(&mut self, params: P, map: F) -> Result<Vec<T>>
    where
        P: Params,
        F: FnMut(&Row<'_>) -> rusqlite::Result<T>,
    {
        if self.validated.output() != sql::StatementOutput::Rows {
            return Err(Error::StatementModeMismatch);
        }
        match &self.validated {
            sql::ValidatedStatement::Read => self.database.view(&self.runtime, |view| {
                view.query_prevalidated(&self.sql, params, map)
            }),
            sql::ValidatedStatement::Write(validated) => self.database.query_write_validated(
                &self.runtime,
                &self.sql,
                params,
                map,
                (**validated).clone(),
            ),
        }
    }

    /// Asynchronously execute this statement and return owned mapped values.
    pub async fn query_map_async<T, P, F>(&self, params: P, map: F) -> Result<Vec<T>>
    where
        T: Send + 'static,
        P: Params + Send + 'static,
        F: Send + 'static + for<'a> FnMut(&Row<'a>) -> rusqlite::Result<T>,
    {
        let sql = self.sql.clone();
        if self.validated.output() != sql::StatementOutput::Rows {
            return Err(Error::StatementModeMismatch);
        }
        match &self.validated {
            sql::ValidatedStatement::Read => {
                self.database
                    .view_async(move |view| view.query_prevalidated(&sql, params, map))
                    .await
            }
            sql::ValidatedStatement::Write(validated) => {
                self.database
                    .query_write_validated_async(sql, params, map, (**validated).clone())
                    .await
            }
        }
    }

    /// Async alias matching the connection's direct-query vocabulary.
    pub async fn query_async<T, P, F>(&self, params: P, map: F) -> Result<Vec<T>>
    where
        T: Send + 'static,
        P: Params + Send + 'static,
        F: Send + 'static + for<'a> FnMut(&Row<'a>) -> rusqlite::Result<T>,
    {
        self.query_map_async(params, map).await
    }
}

fn open_on<H: ServerHandle + Send + Sync + 'static>(
    owner: ConnectionOwner,
    path: PathBuf,
    options: OpenOptions<H>,
) -> Result<Database<H>> {
    let OpenOptions {
        invitation,
        server,
        authority: _,
        sync_policy,
        isolation_level,
    } = options;
    let lineage = Lineage(mint_id()?);
    let commit_history = CommitHistory::default();
    let canonical = CanonicalRouter::default();
    let wal_path = wal_path_for(&path);
    owner.with_connection(crate::repair::register)?;
    let (database_id, client) =
        owner.with_savepoint("__multilite__database_open", |connection| {
            validate_user_table_shapes(connection)?;
            match classify(connection)? {
                DatabaseState::Fresh => {
                    initialize(&owner, invitation, server, lineage, canonical.clone())
                }
                DatabaseState::Initialized => reopen(
                    &owner,
                    invitation.as_ref(),
                    server,
                    lineage,
                    commit_history.clone(),
                    canonical.clone(),
                ),
            }
        })?;
    let client = Arc::new(client);
    let commit_backend = Arc::new(DatabaseCommitBackend::new(
        owner.clone(),
        path,
        wal_path,
        database_id,
        Arc::clone(&client),
        commit_history.clone(),
    ));
    let committer = Committer::new(Arc::clone(&commit_backend)).map_err(committer_error)?;
    let rowid_allocator = rowid::RowidAllocator::new(committer.clone());
    canonical.install(Arc::new(CommitterMetaSink {
        committer: committer.downgrade(),
    }))?;
    let authority = Authority::new(Arc::clone(&client), database_id.space_id())
        .map_err(|error| Error::BackgroundWorker(error.to_string()))?;
    Ok(Database {
        owner,
        database_id,
        client,
        policy: PolicyActor::new(sync_policy),
        isolation_level,
        committer,
        rowid_allocator,
        authority,
    })
}

struct CommitterMetaSink {
    committer: WeakCommitter,
}

impl CanonicalMetaSink for CommitterMetaSink {
    fn propose(&self, proposal: CommitProposal) -> Result<()> {
        self.committer.propose_blocking(proposal)?;
        Ok(())
    }
}

fn wal_path_for(path: &Path) -> PathBuf {
    let mut wal = path.as_os_str().to_owned();
    wal.push("-wal");
    wal.into()
}

fn initialize<H: ServerHandle>(
    owner: &ConnectionOwner,
    invitation: Option<ReplicaInvitation>,
    server: H,
    lineage: Lineage,
    canonical: CanonicalRouter,
) -> Result<(DatabaseId, DatabaseClient<H>)> {
    let database_id = match invitation {
        Some(invitation) => invitation.database_id,
        None => DatabaseId::from_bytes(mint_id()?),
    };
    SqliteOrderedStore::initialize(owner)?;
    owner.with_connection(pending::initialize)?;
    owner.with_connection(crate::repair::initialize)?;
    owner.with_connection(catalog::initialize)?;
    owner.with_connection(history::initialize)?;
    let store = DatabaseMetaStore::with_database(owner.clone(), canonical);
    let client = block_on(Client::open(
        store,
        server,
        SystemHybridClock::new(lineage),
        DeviceId(mint_id()?),
        SystemNonceSource,
    ))?;
    owner.with_connection(|connection| rowid::initialize(connection, client.device()))?;
    block_on(client.attach(&SpaceEnvelope::plaintext(database_id.space_id())))?;
    Ok((database_id, client))
}

fn reopen<H: ServerHandle>(
    owner: &ConnectionOwner,
    invitation: Option<&ReplicaInvitation>,
    server: H,
    lineage: Lineage,
    commit_history: CommitHistory,
    canonical: CanonicalRouter,
) -> Result<(DatabaseId, DatabaseClient<H>)> {
    let store = DatabaseMetaStore::with_database(owner.clone(), canonical);
    let state = block_on(store.load())?;
    if state.device.is_none() {
        return Err(Error::InvalidDatabase("device identity is missing"));
    }
    if state.spaces.len() != 1 {
        return Err(Error::InvalidDatabase(
            "file must contain exactly one Homebase space",
        ));
    }
    let (space_id, space) = state
        .spaces
        .first_key_value()
        .expect("length checked above");
    let codec = space
        .codec
        .as_ref()
        .ok_or(Error::InvalidDatabase("space envelope is missing"))?;
    let envelope =
        SpaceEnvelope::decode(&codec.sealed).map_err(homebase_client::ClientError::from)?;
    if envelope != SpaceEnvelope::plaintext(*space_id) {
        return Err(Error::InvalidDatabase(
            "database requires a plaintext envelope matching its stored space",
        ));
    }
    let database_id = DatabaseId::from_bytes(space_id.0);
    if let Some(invitation) = invitation
        && invitation.database_id != database_id
    {
        return Err(Error::DatabaseIdMismatch {
            expected: invitation.database_id.to_bytes(),
            actual: database_id.to_bytes(),
        });
    }

    owner.with_connection(|connection| {
        catalog::validate(connection)?;
        history::validate(connection)?;
        rowid::validate(connection)?;
        crate::repair::validate(connection)?;
        pending::validate_active_from(connection, space.cursors.neck)?;
        commit_history.prune(connection)?;
        Ok::<_, Error>(())
    })?;

    let client = block_on(Client::open(
        store,
        server,
        SystemHybridClock::new(lineage),
        DeviceId(mint_id()?),
        SystemNonceSource,
    ))?;
    owner.with_connection(|connection| rowid::validate_device(connection, client.device()))?;
    block_on(client.attach(&envelope))?;
    Ok((database_id, client))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DatabaseState {
    Fresh,
    Initialized,
}

fn classify(connection: &SqliteConnection) -> Result<DatabaseState> {
    let metadata = SqliteOrderedStore::is_initialized(connection)?;
    let pending = pending::is_initialized(connection)?;
    let repair = crate::repair::is_initialized(connection)?;
    let catalog = catalog::is_initialized(connection)?;
    let history = history::is_initialized(connection)?;
    let rowids = rowid::is_initialized(connection)?;
    match (metadata, pending, repair, catalog, history, rowids) {
        (false, false, false, false, false, false) => Ok(DatabaseState::Fresh),
        (true, true, true, true, true, true) => {
            SqliteOrderedStore::validate(connection)?;
            pending::validate(connection)?;
            crate::repair::validate(connection)?;
            catalog::validate(connection)?;
            rowid::validate(connection)?;
            Ok(DatabaseState::Initialized)
        }
        _ => Err(Error::InvalidDatabase(
            "general metadata tables are only partially initialized",
        )),
    }
}

fn validate_user_table_shapes(connection: &SqliteConnection) -> Result<()> {
    let mut tables = connection.prepare(
        "SELECT name, type, wr
         FROM pragma_table_list
         WHERE schema = 'main'
           AND type IN ('table', 'virtual', 'shadow')
         ORDER BY name",
    )?;
    let tables = tables
        .query_map((), |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, bool>(2)?,
            ))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    for (table, kind, without_rowid) in tables {
        if table.starts_with("sqlite_") || sql::has_multilite_prefix(&table) {
            continue;
        }
        if kind != "table" {
            return Err(Error::InvalidDatabase(
                "user virtual and shadow tables are not supported",
            ));
        }
        let mut columns = connection.prepare(
            "SELECT type, pk
             FROM pragma_table_xinfo(?1)
             WHERE pk > 0
             ORDER BY pk",
        )?;
        let primary = columns
            .query_map([&table], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, u32>(1)?))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        if primary.is_empty() {
            return Err(Error::InvalidDatabase(
                "every user table must declare a primary key",
            ));
        }
        if without_rowid {
            continue;
        }
        let primary_index: bool = connection.query_row(
            "SELECT EXISTS(
                SELECT 1 FROM pragma_index_list(?1) WHERE origin = 'pk'
             )",
            [&table],
            |row| row.get(0),
        )?;
        if primary.len() != 1 || !primary[0].0.eq_ignore_ascii_case("INTEGER") || primary_index {
            return Err(Error::InvalidDatabase(
                "rowid tables require a single INTEGER PRIMARY KEY alias",
            ));
        }
    }
    Ok(())
}

fn mint_id() -> Result<[u8; 16]> {
    let mut id = [0; 16];
    getrandom::fill(&mut id).map_err(|error| Error::Entropy(error.to_string()))?;
    Ok(id)
}

fn offline_server(_: &SpaceId) -> Option<UnreachableSpace> {
    None
}

fn committer_error(error: crate::commit::committer::CommitterError) -> Error {
    Error::Committer(error.to_string())
}

#[cfg(test)]
mod tests;
