//! General Multilite database identity and Homebase lifecycle.

mod actor;
mod catalog;
mod codes;
mod connection;
mod isolation;
mod operation;
mod pending;
mod policy;
mod proposal;
mod rebase;
mod row;
mod schema;
mod sql;
mod store;
mod transaction;
mod update;
mod view;
mod vtab;

use std::ops::Deref;
use std::path::Path;
use std::sync::Arc;

use homebase_client::cipher::{SpaceEnvelope, SystemNonceSource};
use homebase_client::meta::{MetaStore, OplogCursors};
use homebase_client::server::UnreachableSpace;
use homebase_client::{Client, ClientError, PushOutcome as HomebasePushOutcome, ServerHandle};
use homebase_core::clock::{Lineage, SystemHybridClock};
use homebase_core::messages::KernelError;
use homebase_core::space::SpaceId;
use homebase_core::tag::{AdmissionSeq, DeviceId, DeviceSeq};
use pollster::block_on;
use rusqlite::hooks::{AuthAction, AuthContext, Authorization, PreUpdateCase};
use rusqlite::{Connection as SqliteConnection, Row};

use crate::connection::ConnectionOwner;
use crate::metastore::SqliteOrderedStore;
use crate::runtime::{ExecutionMode, HookPolicy, RuntimeConnection};
use crate::{Error, Params, Result};

use self::actor::{ActorPermit, SerialActor};
use self::policy::{PolicyState, PushScheduler};
use self::row::{CapturedRow, StoredValue};
use self::store::DatabaseMetaStore;

pub use self::connection::Connection;
pub use self::isolation::{IsolationLevel, UpdateOptions};
pub use self::policy::SyncPolicy;
pub use self::update::UpdateTransaction;
pub use self::view::{TransactionStatement, ViewTransaction};

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
    vtabs: vtab::Registry,
}

impl DatabaseRuntime {
    fn new(owner: ConnectionOwner) -> Result<Self> {
        Ok(Self {
            inner: RuntimeConnection::new(owner, DatabaseHooks)?,
            vtabs: vtab::Registry::default(),
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
    type Event = CapturedRow;

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
        capture_insert(mode, database, table, update)
    }
}

fn capture_insert(
    mode: ExecutionMode,
    database: &str,
    table: &str,
    update: &PreUpdateCase,
) -> Result<Option<CapturedRow>> {
    if mode != ExecutionMode::Public
        || database != "main"
        || is_schema_table(table)
        || has_multilite_prefix(table)
    {
        return Ok(None);
    }
    let PreUpdateCase::Insert(values) = update else {
        return Err(Error::CaptureInvariant(
            "public table mutation was not an insert",
        ));
    };
    if values.get_query_depth() != 0 {
        return Err(Error::CaptureInvariant(
            "writes caused by triggers are not supported",
        ));
    }
    let values = (0..values.get_column_count())
        .map(|index| {
            values
                .get_new_column_value(index)
                .map(StoredValue::capture)
                .map_err(Error::from)
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Some(CapturedRow {
        table: table.to_owned(),
        values,
    }))
}

/// An opened general Multilite database.
pub(crate) struct Database<H: ServerHandle> {
    owner: ConnectionOwner,
    database_id: DatabaseId,
    client: DatabaseClient<H>,
    policy: PolicyState,
    isolation_level: IsolationLevel,
    actor: SerialActor,
    scheduler: PushScheduler,
}

impl Database<OfflineServer> {
    pub(crate) fn open(path: impl AsRef<Path>) -> Result<Arc<Self>> {
        Self::open_with(path, OpenOptions::new())
    }
}

impl<H: ServerHandle + Send + Sync + 'static> Database<H> {
    pub(crate) fn open_with(path: impl AsRef<Path>, options: OpenOptions<H>) -> Result<Arc<Self>> {
        options.validate()?;
        let owner = ConnectionOwner::open(path)?;
        let database = open_on(owner, options)?;
        Ok(Arc::new(database))
    }

    pub(crate) fn start_background_push(self: &Arc<Self>) -> Result<()> {
        if self.policy.write_delay().is_some() {
            self.scheduler.start(Arc::downgrade(self))?;
            let cursors = self.submit_cursors()?;
            if cursors.neck < cursors.tail {
                self.scheduler.schedule(std::time::Duration::ZERO);
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
        &self,
        runtime: &DatabaseRuntime,
        sql: &str,
        params: Q,
    ) -> Result<usize> {
        let validated = sql::validate_execute(sql)?;
        self.update(runtime, |update| {
            update.execute_validated(sql, params, validated)
        })
    }

    pub(crate) fn push(self: &Arc<Self>) -> Result<PushOutcome> {
        let database = Arc::clone(self);
        self.actor
            .call_blocking(move || database.push_serial())
            .map_err(actor_error)?
    }

    fn push_serial(&self) -> Result<PushOutcome> {
        let pushed = block_on(async {
            self.client
                .space(self.database_id.space_id())
                .await?
                .push()
                .await
        })?;
        match pushed {
            HomebasePushOutcome::Drained { .. } => Ok(PushOutcome::Drained),
            HomebasePushOutcome::Stalled { at, error, .. } => {
                let cursors = self.submit_cursors()?;
                Ok(PushOutcome::Rejected(PushRejection {
                    database_id: self.database_id,
                    device_id: self.client.device(),
                    failed_at: at,
                    submit_cursors: cursors,
                    error,
                }))
            }
        }
    }

    /// Undo the speculative SQLite effects covered by one definitive push
    /// rejection and retire that exact active submit window.
    pub(crate) fn rollback(self: &Arc<Self>, rejection: &PushRejection) -> Result<()> {
        let database = Arc::clone(self);
        let rejection = rejection.clone();
        self.actor
            .call_blocking(move || database.rollback_serial(&rejection))
            .map_err(actor_error)?
    }

    fn rollback_serial(&self, rejection: &PushRejection) -> Result<()> {
        if rejection.database_id != self.database_id
            || rejection.device_id != self.client.device()
            || rejection.failed_at != rejection.submit_cursors.neck
        {
            return Err(Error::StalePushRejection);
        }

        match block_on(self.client.rollback_if_unchanged(
            self.database_id.space_id(),
            rejection.failed_at,
            rejection.submit_cursors,
        )) {
            Ok(()) => Ok(()),
            Err(ClientError::RollbackWindowChanged) => Err(Error::StalePushRejection),
            Err(error) => Err(error.into()),
        }
    }

    pub(crate) fn pull(self: &Arc<Self>) -> Result<PullOutcome> {
        let database = Arc::clone(self);
        self.actor
            .call_blocking(move || database.pull_serial())
            .map_err(actor_error)?
    }

    fn pull_serial(&self) -> Result<PullOutcome> {
        let through = block_on(async {
            let space = self.client.space(self.database_id.space_id()).await?;
            space.pull().await.map_err(ClientError::from)
        })?;
        self.policy.mark_pulled();
        Ok(PullOutcome { through })
    }

    fn finish_remote_write(&self) -> Result<()> {
        match self.push_serial()? {
            PushOutcome::Drained => Ok(()),
            PushOutcome::Rejected(rejection) => self.repair_remote_rejection(rejection),
        }
    }

    fn repair_remote_rejection(&self, rejection: PushRejection) -> Result<()> {
        let error = rejection.error.clone();
        self.rollback_serial(&rejection)?;
        // Retire the rollback marker when authority remains reachable. If this
        // best-effort push becomes unavailable, the marker remains durable and
        // the next remote operation drains it before doing new work.
        let _ = self.push_serial();
        Err(Error::AuthorityRejected(error))
    }

    pub(crate) fn prepare(
        self: &Arc<Self>,
        runtime: &Arc<DatabaseRuntime>,
        sql: &str,
    ) -> Result<Statement<H>> {
        sql::validate_managed_statement(sql)?;
        let _operation = self.enter_operation()?;
        runtime.run(ExecutionMode::Public, |connection| {
            let statement = connection.prepare(sql)?;
            if statement.readonly() {
                Ok(())
            } else {
                Err(Error::PreparedWrite)
            }
        })?;
        Ok(Statement {
            database: Arc::clone(self),
            runtime: Arc::clone(runtime),
            sql: sql.to_owned(),
        })
    }

    #[cfg(test)]
    pub(crate) fn with_connection<T>(&self, operation: impl FnOnce(&SqliteConnection) -> T) -> T {
        self.owner.with_connection(operation)
    }

    fn submit_cursors(&self) -> Result<OplogCursors> {
        let store = DatabaseMetaStore::new(self.owner.clone());
        Ok(block_on(store.oplog_cursors(self.database_id.space_id()))?)
    }

    fn refresh_read_serial(&self, runtime: &DatabaseRuntime) -> Result<()> {
        if !self.policy.read_requires_refresh() {
            return Ok(());
        }
        let submit = self.submit_cursors()?;
        if submit.neck < submit.tail {
            match self.push_serial()? {
                PushOutcome::Drained => {}
                PushOutcome::Rejected(rejection) => {
                    return Err(Error::RefreshPushRejected(rejection));
                }
            }
        }
        self.pull_serial()?;
        self.rebase_serial(runtime)?;
        self.policy.mark_rebased();
        Ok(())
    }

    fn enter_operation(&self) -> Result<ActorPermit> {
        self.actor.enter_blocking().map_err(actor_error)
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
        AuthAction::CreateIndex {
            index_name,
            table_name,
        } if index_name.starts_with("sqlite_autoindex_") => {
            authorize_user_table(context.database_name, table_name)
        }
        AuthAction::Insert { table_name } if is_schema_table(table_name) => {
            authorize_main(context.database_name)
        }
        AuthAction::Update { table_name, .. } if is_schema_table(table_name) => {
            authorize_main(context.database_name)
        }
        AuthAction::Insert { table_name } => {
            authorize_user_table(context.database_name, table_name)
        }
        _ => Authorization::Deny,
    }
}

fn authorize_read(database: Option<&str>, table: &str) -> Authorization {
    if is_schema_table(table) {
        authorize_main(database)
    } else {
        authorize_user_table(database, table)
    }
}

fn authorize_user_table(database: Option<&str>, table: &str) -> Authorization {
    if is_main(database) && !has_multilite_prefix(table) {
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

fn is_main(database: Option<&str>) -> bool {
    matches!(database, None | Some("main"))
}

fn is_schema_table(table: &str) -> bool {
    table.eq_ignore_ascii_case("sqlite_master") || table.eq_ignore_ascii_case("sqlite_schema")
}

fn has_multilite_prefix(table: &str) -> bool {
    table
        .get(.."__multilite__".len())
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("__multilite__"))
}

/// A read-only prepared statement owned by a Multilite database.
pub struct Statement<H = OfflineServer>
where
    H: ServerHandle + Send + Sync + 'static,
{
    database: Arc<Database<H>>,
    runtime: Arc<DatabaseRuntime>,
    sql: String,
}

impl<H: ServerHandle + Send + Sync + 'static> Statement<H> {
    /// Execute the query and eagerly map every row.
    pub fn query_map<T, P, F>(&mut self, params: P, map: F) -> Result<Vec<T>>
    where
        P: Params,
        F: FnMut(&Row<'_>) -> rusqlite::Result<T>,
    {
        let _operation = self.database.enter_operation()?;
        self.database.refresh_read_serial(&self.runtime)?;
        self.database
            .owner
            .with_savepoint("__multilite__statement_view", |connection| {
                let mut statement = connection.prepare(&self.sql)?;
                if !statement.readonly() {
                    return Err(Error::PreparedWrite);
                }
                statement
                    .query_map(params, map)?
                    .collect::<rusqlite::Result<Vec<_>>>()
                    .map_err(Into::into)
            })
    }
}

fn pin_snapshot(connection: &SqliteConnection) -> Result<()> {
    let _: i64 = connection.query_row("SELECT count(*) FROM main.sqlite_schema", (), |row| {
        row.get(0)
    })?;
    Ok(())
}

fn open_on<H: ServerHandle + Send + Sync + 'static>(
    owner: ConnectionOwner,
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
    let (database_id, client) =
        owner.with_savepoint("__multilite__database_open", |connection| {
            match classify(connection)? {
                DatabaseState::Fresh => initialize(&owner, invitation, server, lineage),
                DatabaseState::Initialized => reopen(&owner, invitation.as_ref(), server, lineage),
            }
        })?;
    Ok(Database {
        owner,
        database_id,
        client,
        policy: PolicyState::new(sync_policy),
        isolation_level,
        actor: SerialActor::new().map_err(actor_error)?,
        scheduler: PushScheduler::new(),
    })
}

fn initialize<H: ServerHandle>(
    owner: &ConnectionOwner,
    invitation: Option<ReplicaInvitation>,
    server: H,
    lineage: Lineage,
) -> Result<(DatabaseId, DatabaseClient<H>)> {
    let database_id = match invitation {
        Some(invitation) => invitation.database_id,
        None => DatabaseId::from_bytes(mint_id()?),
    };
    SqliteOrderedStore::initialize(owner)?;
    owner.with_connection(pending::initialize)?;
    owner.with_connection(catalog::initialize)?;
    owner.with_connection(proposal::initialize)?;
    let store = DatabaseMetaStore::new(owner.clone());
    let client = block_on(Client::open(
        store,
        server,
        SystemHybridClock::new(lineage),
        DeviceId(mint_id()?),
        SystemNonceSource,
    ))?;
    block_on(client.attach(&SpaceEnvelope::plaintext(database_id.space_id())))?;
    Ok((database_id, client))
}

fn reopen<H: ServerHandle>(
    owner: &ConnectionOwner,
    invitation: Option<&ReplicaInvitation>,
    server: H,
    lineage: Lineage,
) -> Result<(DatabaseId, DatabaseClient<H>)> {
    let store = DatabaseMetaStore::new(owner.clone());
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
        proposal::validate(connection)?;
        pending::validate_active_from(connection, space.cursors.neck)
    })?;

    let client = block_on(Client::open(
        store,
        server,
        SystemHybridClock::new(lineage),
        DeviceId(mint_id()?),
        SystemNonceSource,
    ))?;
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
    let catalog = catalog::is_initialized(connection)?;
    let commits = proposal::is_initialized(connection)?;
    match (metadata, pending, catalog, commits) {
        (false, false, false, false) => Ok(DatabaseState::Fresh),
        (true, true, true, true) => {
            SqliteOrderedStore::validate(connection)?;
            pending::validate(connection)?;
            catalog::validate(connection)?;
            Ok(DatabaseState::Initialized)
        }
        _ => Err(Error::InvalidDatabase(
            "general metadata tables are only partially initialized",
        )),
    }
}

fn mint_id() -> Result<[u8; 16]> {
    let mut id = [0; 16];
    getrandom::fill(&mut id).map_err(|error| Error::Entropy(error.to_string()))?;
    Ok(id)
}

fn offline_server(_: &SpaceId) -> Option<UnreachableSpace> {
    None
}

fn actor_error(error: actor::ActorError) -> Error {
    Error::DatabaseActor(error.to_string())
}

#[cfg(test)]
mod tests;
