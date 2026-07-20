//! General Multilite database identity and Homebase lifecycle.

mod catalog;
mod codes;
mod connection;
mod operation;
mod pending;
mod policy;
mod rebase;
mod row;
mod schema;
mod sql;
mod store;
mod transaction;

use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard};

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

use self::operation::MultiliteOp;
use self::policy::{PolicyState, PushScheduler};
use self::row::{CapturedRow, InsertRows, StoredValue};
use self::store::DatabaseMetaStore;
use self::transaction::MultiliteTransaction;

pub use self::connection::Connection;
pub use self::policy::SyncPolicy;

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

    /// Replace the server route while retaining all other options.
    pub fn server<S: ServerHandle>(self, server: S) -> OpenOptions<S> {
        OpenOptions {
            invitation: self.invitation,
            server,
            authority: true,
            sync_policy: self.sync_policy,
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
pub(crate) type DatabaseRuntime = RuntimeConnection<DatabaseHooks>;

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
    operation: Arc<Mutex<()>>,
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
        RuntimeConnection::new(self.owner.clone(), DatabaseHooks)
    }

    pub(crate) fn execute<Q: Params>(
        &self,
        runtime: &DatabaseRuntime,
        sql: &str,
        params: Q,
    ) -> Result<usize> {
        let validated = sql::validate_execute(sql)?;
        let _operation = lock(&self.operation);
        let result = match validated {
            sql::ValidatedExecute::Insert => self.execute_insert(runtime, sql, params),
            sql::ValidatedExecute::CreateTable(table) => {
                if self.policy.policy() == SyncPolicy::Remote {
                    self.drain_remote_queue()?;
                }
                self.execute_create_table(runtime, sql, params, table)
            }
        };

        let result = result?;
        match self.policy.policy() {
            SyncPolicy::LocalOnly => {}
            SyncPolicy::LocalFirst { write_delay, .. } => self.scheduler.schedule(write_delay),
            SyncPolicy::Remote => self.finish_remote_write()?,
        }
        Ok(result)
    }

    pub(crate) fn push(&self) -> Result<PushOutcome> {
        let _operation = lock(&self.operation);
        self.push_locked()
    }

    fn push_locked(&self) -> Result<PushOutcome> {
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
    pub(crate) fn rollback(&self, rejection: &PushRejection) -> Result<()> {
        let _operation = lock(&self.operation);
        self.rollback_locked(rejection)
    }

    fn rollback_locked(&self, rejection: &PushRejection) -> Result<()> {
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

    pub(crate) fn pull(&self) -> Result<PullOutcome> {
        let _operation = lock(&self.operation);
        self.pull_locked()
    }

    fn pull_locked(&self) -> Result<PullOutcome> {
        let through = block_on(async {
            let space = self.client.space(self.database_id.space_id()).await?;
            space.pull().await.map_err(ClientError::from)
        })?;
        self.policy.mark_pulled();
        Ok(PullOutcome { through })
    }

    fn drain_remote_queue(&self) -> Result<()> {
        match self.push_locked()? {
            PushOutcome::Drained => Ok(()),
            PushOutcome::Rejected(rejection) => self.repair_remote_rejection(rejection),
        }
    }

    fn finish_remote_write(&self) -> Result<()> {
        match self.push_locked()? {
            PushOutcome::Drained => Ok(()),
            PushOutcome::Rejected(rejection) => self.repair_remote_rejection(rejection),
        }
    }

    fn repair_remote_rejection(&self, rejection: PushRejection) -> Result<()> {
        let error = rejection.error.clone();
        self.rollback_locked(&rejection)?;
        // Retire the rollback marker when authority remains reachable. If this
        // best-effort push becomes unavailable, the marker remains durable and
        // the next remote operation drains it before doing new work.
        let _ = self.push_locked();
        Err(Error::AuthorityRejected(error))
    }

    fn execute_create_table<Q: Params>(
        &self,
        runtime: &DatabaseRuntime,
        sql: &str,
        params: Q,
        table: schema::CreateTableSpec,
    ) -> Result<usize> {
        let transaction = MultiliteTransaction::one(MultiliteOp::create_table(sql, table));
        let MultiliteOp::CreateTable(created) = &transaction.operations()[0] else {
            unreachable!("create-table constructor returned another operation")
        };
        let (space, upto) = block_on(async {
            let space = self.client.space(self.database_id.space_id()).await?;
            let cursors = space.admits().cursors().await.map_err(ClientError::from)?;
            let upto = AdmissionSeq(
                cursors
                    .neck
                    .0
                    .checked_sub(1)
                    .ok_or(Error::InvalidDatabase("admit neck cannot be zero"))?,
            );
            Ok::<_, Error>((space, upto))
        })?;
        let (mutations, assertions) = transaction.to_homebase()?.at(upto);

        let (changed, _) = runtime.run(ExecutionMode::Public, |connection| {
            let changed = connection.execute(sql, params)?;
            runtime.with_internal_metadata(|| {
                catalog::insert(connection, created)?;
                let submission = block_on(space.submit_unchecked(mutations, assertions))
                    .map_err(ClientError::from)?;
                pending::insert(connection, submission.seq, &transaction)?;
                Ok(())
            })?;
            Ok(changed)
        })?;
        Ok(changed)
    }

    fn execute_insert<Q: Params>(
        &self,
        runtime: &DatabaseRuntime,
        sql: &str,
        params: Q,
    ) -> Result<usize> {
        let (space, upto) = block_on(async {
            let space = self.client.space(self.database_id.space_id()).await?;
            let cursors = space.admits().cursors().await.map_err(ClientError::from)?;
            let upto = AdmissionSeq(
                cursors
                    .neck
                    .0
                    .checked_sub(1)
                    .ok_or(Error::InvalidDatabase("admit neck cannot be zero"))?,
            );
            Ok::<_, Error>((space, upto))
        })?;

        let (changed, _) = runtime.run_captured(
            ExecutionMode::Public,
            |connection| Ok(connection.execute(sql, params)?),
            |connection, events| {
                let Some(inserted) = InsertRows::from_captured(connection, events)? else {
                    if events.is_empty() {
                        return Ok(());
                    }
                    return Err(Error::UnsupportedSql(
                        "INSERT target has no synchronized schema identity",
                    ));
                };
                let transaction = MultiliteTransaction::one(MultiliteOp::InsertRows(inserted));
                let (mutations, assertions) = transaction.to_homebase()?.at(upto);
                runtime.with_internal_metadata(|| {
                    let submission = block_on(space.submit_unchecked(mutations, assertions))
                        .map_err(ClientError::from)?;
                    pending::insert(connection, submission.seq, &transaction)?;
                    Ok(())
                })
            },
        )?;
        Ok(changed)
    }

    pub(crate) fn prepare(
        self: &Arc<Self>,
        runtime: &Arc<DatabaseRuntime>,
        sql: &str,
    ) -> Result<Statement> {
        let _operation = lock(&self.operation);
        runtime.run(ExecutionMode::Public, |connection| {
            let statement = connection.prepare(sql)?;
            if statement.readonly() {
                Ok(())
            } else {
                Err(Error::PreparedWrite)
            }
        })?;
        let database = Arc::clone(self);
        let runtime = Arc::clone(runtime);
        Ok(Statement {
            owner: self.owner.clone(),
            operation: Arc::clone(&self.operation),
            refresh: Arc::new(move || database.refresh_read_locked(&runtime)),
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

    fn refresh_read_locked(&self, runtime: &DatabaseRuntime) -> Result<()> {
        if !self.policy.read_requires_refresh() {
            return Ok(());
        }
        let submit = self.submit_cursors()?;
        if submit.neck < submit.tail {
            match self.push_locked()? {
                PushOutcome::Drained => {}
                PushOutcome::Rejected(rejection) => {
                    return Err(Error::RefreshPushRejected(rejection));
                }
            }
        }
        self.pull_locked()?;
        self.rebase_locked(runtime)?;
        self.policy.mark_rebased();
        Ok(())
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
pub struct Statement {
    owner: ConnectionOwner,
    operation: Arc<Mutex<()>>,
    refresh: Arc<dyn Fn() -> Result<()> + Send + Sync>,
    sql: String,
}

impl Statement {
    /// Execute the query and eagerly map every row.
    pub fn query_map<T, P, F>(&mut self, params: P, map: F) -> Result<Vec<T>>
    where
        P: Params,
        F: FnMut(&Row<'_>) -> rusqlite::Result<T>,
    {
        let _operation = lock(&self.operation);
        (self.refresh)()?;
        self.owner.with_connection(|connection| {
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

fn open_on<H: ServerHandle + Send + Sync + 'static>(
    owner: ConnectionOwner,
    options: OpenOptions<H>,
) -> Result<Database<H>> {
    let OpenOptions {
        invitation,
        server,
        authority: _,
        sync_policy,
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
        operation: Arc::new(Mutex::new(())),
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
    match (metadata, pending, catalog) {
        (false, false, false) => Ok(DatabaseState::Fresh),
        (true, true, true) => {
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

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{Duration, Instant};

    use homebase::Server;
    use homebase::actor::{SpaceHandle, Spawner};
    use homebase::storage::MemoryStore;
    use homebase_client::meta::{AdmitCursors, ClientState, DeviceOp, SubmitMode};
    use homebase_client::server::offline_router;
    use homebase_core::clock::{ManualClock, Timestamp};
    use homebase_core::key::{Key, MAX_COMPONENT_LEN};
    use homebase_core::tag::{DeviceSeq, Mutation};
    use rusqlite::OptionalExtension;
    use rusqlite::hooks::{AuthAction, AuthContext, Authorization};

    use super::*;

    struct ThreadSpawner;

    impl Spawner for ThreadSpawner {
        fn spawn(&self, task: Pin<Box<dyn Future<Output = ()> + Send + 'static>>) {
            std::thread::spawn(move || pollster::block_on(task));
        }
    }

    type TestServer = Server<MemoryStore, ManualClock, ThreadSpawner>;

    fn server() -> Arc<TestServer> {
        Arc::new(Server::new(
            Arc::new(MemoryStore::new()),
            Arc::new(ManualClock::new(Timestamp(0))),
            ThreadSpawner,
        ))
    }

    fn router(server: Arc<TestServer>) -> impl Fn(&SpaceId) -> Option<SpaceHandle> + Sync {
        move |space| server.space(space)
    }

    fn client_state<H: ServerHandle + Send + Sync + 'static>(
        database: &Database<H>,
    ) -> ClientState {
        let store = DatabaseMetaStore::new(database.owner.clone());
        block_on(store.load()).unwrap()
    }

    fn table_exists<H: ServerHandle + Send + Sync + 'static>(
        database: &Database<H>,
        table: &str,
    ) -> bool {
        database.with_connection(|connection| {
            connection
                .query_row(
                    "SELECT EXISTS(
                         SELECT 1 FROM sqlite_schema
                         WHERE type = 'table' AND name = ?1 COLLATE NOCASE
                     )",
                    [table],
                    |row| row.get(0),
                )
                .unwrap()
        })
    }

    fn pending_ops<H: ServerHandle + Send + Sync + 'static>(
        database: &Database<H>,
    ) -> Vec<pending::PendingTransaction> {
        database.with_connection(pending::load).unwrap()
    }

    fn table_sql<H: ServerHandle + Send + Sync + 'static>(
        database: &Database<H>,
        table: &str,
    ) -> Option<String> {
        database.with_connection(|connection| {
            connection
                .query_row(
                    "SELECT sql FROM sqlite_schema
                     WHERE type = 'table' AND name = ?1 COLLATE NOCASE",
                    [table],
                    |row| row.get(0),
                )
                .optional()
                .unwrap()
        })
    }

    fn stock_user_schema(path: &Path) -> Vec<(String, String)> {
        let connection = SqliteConnection::open(path).unwrap();
        let mut statement = connection
            .prepare(
                "SELECT name, sql FROM sqlite_schema
                 WHERE type = 'table' ORDER BY name COLLATE NOCASE",
            )
            .unwrap();
        statement
            .query_map((), |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap()
            .into_iter()
            .filter(|(name, _)| {
                let name = name.to_ascii_lowercase();
                !name.starts_with("__multilite__") && !name.starts_with("sqlite_")
            })
            .collect()
    }

    fn create_operation(name: &str) -> MultiliteOp {
        MultiliteOp::create_table(
            &format!("CREATE TABLE {name} (id INTEGER PRIMARY KEY)"),
            schema::CreateTableSpec {
                name: schema::SqlName::new(name.into()),
                columns: vec![schema::CreateColumn {
                    name: schema::SqlName::new("id".into()),
                    declared_type: schema::DeclaredType::Integer,
                    not_null: false,
                    primary_key: true,
                }],
            },
        )
    }

    fn submit_direct<H: ServerHandle + Send + Sync + 'static>(
        database: &Database<H>,
        operation: &MultiliteOp,
    ) {
        let (mutations, assertions) = MultiliteTransaction::one(operation.clone())
            .to_homebase()
            .unwrap()
            .at(AdmissionSeq(0));
        block_on(async {
            database
                .client
                .space(database.database_id().space_id())
                .await
                .unwrap()
                .submit_unchecked(mutations, assertions)
                .await
                .unwrap();
        });
    }

    fn wait_until(mut predicate: impl FnMut() -> bool) {
        let deadline = Instant::now() + Duration::from_secs(2);
        while !predicate() {
            assert!(Instant::now() < deadline, "condition did not become true");
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    #[test]
    fn policy_defaults_are_local_and_authority_requirements_fail_before_open() {
        let directory = tempfile::tempdir().unwrap();
        let local_path = directory.path().join("local.sqlite");
        let local = Database::open(&local_path).unwrap();
        assert_eq!(local.sync_policy(), SyncPolicy::LocalOnly);

        let remote_path = directory.path().join("remote.sqlite");
        assert!(matches!(
            Database::open_with(
                &remote_path,
                OpenOptions::new().sync_policy(SyncPolicy::Remote),
            ),
            Err(Error::AuthorityRequired("remote policy"))
        ));
        assert!(!remote_path.exists());

        let local_first_path = directory.path().join("local-first.sqlite");
        assert!(matches!(
            Database::open_with(
                &local_first_path,
                OpenOptions::new().sync_policy(SyncPolicy::LocalFirst {
                    write_delay: Duration::ZERO,
                    read_staleness: Duration::from_secs(1),
                }),
            ),
            Err(Error::AuthorityRequired("local-first policy"))
        ));
        assert!(!local_first_path.exists());
    }

    #[test]
    fn local_first_zero_schedules_push_without_waiting_in_execute() {
        let directory = tempfile::tempdir().unwrap();
        let server = server();
        let database = Database::open_with(
            directory.path().join("local-first.sqlite"),
            OpenOptions::new()
                .sync_policy(SyncPolicy::LocalFirst {
                    write_delay: Duration::ZERO,
                    read_staleness: Duration::from_secs(60),
                })
                .server(router(Arc::clone(&server))),
        )
        .unwrap();
        assert!(server.create_space(database.database_id().space_id()));
        let runtime = database.runtime().unwrap();
        database.start_background_push().unwrap();

        database
            .execute(&runtime, "CREATE TABLE notes (id INTEGER PRIMARY KEY)", ())
            .unwrap();
        wait_until(|| pending_ops(&database).is_empty());

        let state = client_state(&database);
        let cursors = state.spaces[&database.database_id().space_id()].cursors;
        assert_eq!(cursors.neck, cursors.tail);
        assert!(table_exists(&database, "notes"));
    }

    #[test]
    fn remote_write_returns_only_after_admission_and_pending_cleanup() {
        let directory = tempfile::tempdir().unwrap();
        let server = server();
        let database = Database::open_with(
            directory.path().join("remote.sqlite"),
            OpenOptions::new()
                .sync_policy(SyncPolicy::Remote)
                .server(router(Arc::clone(&server))),
        )
        .unwrap();
        assert!(server.create_space(database.database_id().space_id()));
        let runtime = database.runtime().unwrap();

        database
            .execute(&runtime, "CREATE TABLE notes (id INTEGER PRIMARY KEY)", ())
            .unwrap();

        assert!(pending_ops(&database).is_empty());
        let state = client_state(&database);
        let cursors = state.spaces[&database.database_id().space_id()].cursors;
        assert_eq!(cursors.neck, cursors.tail);
        assert!(table_exists(&database, "notes"));
    }

    #[test]
    fn remote_rejection_undoes_sqlite_before_returning_the_error() {
        let directory = tempfile::tempdir().unwrap();
        let server = server();
        let first = Database::open_with(
            directory.path().join("winner.sqlite"),
            OpenOptions::new().server(router(Arc::clone(&server))),
        )
        .unwrap();
        assert!(server.create_space(first.database_id().space_id()));
        let first_runtime = first.runtime().unwrap();
        first
            .execute(
                &first_runtime,
                "CREATE TABLE notes (id INTEGER PRIMARY KEY)",
                (),
            )
            .unwrap();
        assert_eq!(first.push().unwrap(), PushOutcome::Drained);

        let second = Database::open_with(
            directory.path().join("loser.sqlite"),
            OpenOptions::new()
                .invitation(first.replica_invitation())
                .sync_policy(SyncPolicy::Remote)
                .server(router(Arc::clone(&server))),
        )
        .unwrap();
        let second_runtime = second.runtime().unwrap();
        let error = second
            .execute(
                &second_runtime,
                "CREATE TABLE NOTES (id INTEGER PRIMARY KEY, payload BLOB)",
                (),
            )
            .unwrap_err();

        assert!(matches!(
            error,
            Error::AuthorityRejected(KernelError::RangeAssertFailed { .. })
        ));
        assert!(!table_exists(&second, "notes"));
        assert!(pending_ops(&second).is_empty());
        let state = client_state(&second);
        let cursors = state.spaces[&second.database_id().space_id()].cursors;
        assert_eq!(cursors.neck, cursors.tail);
    }

    #[test]
    fn remote_write_first_drains_history_buffered_under_local_only() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("policy-change.sqlite");
        let server = server();
        let database = Database::open_with(
            &path,
            OpenOptions::new().server(router(Arc::clone(&server))),
        )
        .unwrap();
        assert!(server.create_space(database.database_id().space_id()));
        let runtime = database.runtime().unwrap();
        database
            .execute(
                &runtime,
                "CREATE TABLE buffered (id INTEGER PRIMARY KEY)",
                (),
            )
            .unwrap();
        assert_eq!(pending_ops(&database).len(), 1);
        drop(runtime);
        drop(database);

        let database = Database::open_with(
            &path,
            OpenOptions::new()
                .sync_policy(SyncPolicy::Remote)
                .server(router(Arc::clone(&server))),
        )
        .unwrap();
        let runtime = database.runtime().unwrap();
        database
            .execute(
                &runtime,
                "CREATE TABLE admitted (id INTEGER PRIMARY KEY)",
                (),
            )
            .unwrap();

        assert!(pending_ops(&database).is_empty());
        let state = client_state(&database);
        let cursors = state.spaces[&database.database_id().space_id()].cursors;
        assert_eq!(cursors.neck, cursors.tail);
    }

    #[test]
    fn remote_read_pulls_and_rebases_before_running_a_prepared_query() {
        let directory = tempfile::tempdir().unwrap();
        let server = server();
        let source = Database::open_with(
            directory.path().join("read-source.sqlite"),
            OpenOptions::new()
                .sync_policy(SyncPolicy::Remote)
                .server(router(Arc::clone(&server))),
        )
        .unwrap();
        assert!(server.create_space(source.database_id().space_id()));
        let source_runtime = source.runtime().unwrap();
        source
            .execute(
                &source_runtime,
                "CREATE TABLE notes (id INTEGER PRIMARY KEY)",
                (),
            )
            .unwrap();

        let replica = Database::open_with(
            directory.path().join("read-replica.sqlite"),
            OpenOptions::new()
                .invitation(source.replica_invitation())
                .sync_policy(SyncPolicy::Remote)
                .server(router(Arc::clone(&server))),
        )
        .unwrap();
        let runtime = Arc::new(replica.runtime().unwrap());
        let mut statement = replica
            .prepare(
                &runtime,
                "SELECT name FROM sqlite_schema WHERE type = 'table' AND name = 'notes'",
            )
            .unwrap();

        assert_eq!(
            statement
                .query_map((), |row| row.get::<_, String>(0))
                .unwrap(),
            ["notes"]
        );
        assert!(table_exists(&replica, "notes"));
    }

    #[test]
    fn local_first_read_honors_its_staleness_window() {
        let directory = tempfile::tempdir().unwrap();
        let server = server();
        let source = Database::open_with(
            directory.path().join("stale-source.sqlite"),
            OpenOptions::new()
                .sync_policy(SyncPolicy::Remote)
                .server(router(Arc::clone(&server))),
        )
        .unwrap();
        assert!(server.create_space(source.database_id().space_id()));
        let source_runtime = source.runtime().unwrap();
        source
            .execute(
                &source_runtime,
                "CREATE TABLE first_table (id INTEGER PRIMARY KEY)",
                (),
            )
            .unwrap();

        let replica = Database::open_with(
            directory.path().join("stale-replica.sqlite"),
            OpenOptions::new()
                .invitation(source.replica_invitation())
                .sync_policy(SyncPolicy::LocalFirst {
                    write_delay: Duration::from_secs(60),
                    read_staleness: Duration::from_millis(100),
                })
                .server(router(Arc::clone(&server))),
        )
        .unwrap();
        let replica_runtime = Arc::new(replica.runtime().unwrap());
        let mut tables = replica
            .prepare(
                &replica_runtime,
                "SELECT name FROM sqlite_schema
                 WHERE type = 'table' AND name LIKE '%_table' ORDER BY name",
            )
            .unwrap();
        assert_eq!(
            tables.query_map((), |row| row.get::<_, String>(0)).unwrap(),
            ["first_table"]
        );

        source
            .execute(
                &source_runtime,
                "CREATE TABLE second_table (id INTEGER PRIMARY KEY)",
                (),
            )
            .unwrap();
        assert_eq!(
            tables.query_map((), |row| row.get::<_, String>(0)).unwrap(),
            ["first_table"],
            "fresh local state should not contact authority"
        );

        std::thread::sleep(Duration::from_millis(120));
        assert_eq!(
            tables.query_map((), |row| row.get::<_, String>(0)).unwrap(),
            ["first_table", "second_table"]
        );
    }

    #[test]
    fn authority_read_pushes_pending_local_submissions_before_rebase() {
        let directory = tempfile::tempdir().unwrap();
        let server = server();
        let path = directory.path().join("pending-read.sqlite");
        let database = Database::open_with(
            &path,
            OpenOptions::new().server(router(Arc::clone(&server))),
        )
        .unwrap();
        assert!(server.create_space(database.database_id().space_id()));
        let runtime = database.runtime().unwrap();
        database
            .execute(
                &runtime,
                "CREATE TABLE pending (id INTEGER PRIMARY KEY)",
                (),
            )
            .unwrap();
        assert_eq!(pending_ops(&database).len(), 1);
        drop(runtime);
        drop(database);

        let database = Database::open_with(
            &path,
            OpenOptions::new()
                .sync_policy(SyncPolicy::Remote)
                .server(router(Arc::clone(&server))),
        )
        .unwrap();
        let runtime = Arc::new(database.runtime().unwrap());
        let mut statement = database.prepare(&runtime, "SELECT 1").unwrap();

        assert_eq!(
            statement.query_map((), |row| row.get::<_, i64>(0)).unwrap(),
            [1]
        );
        assert!(table_exists(&database, "pending"));
        assert!(pending_ops(&database).is_empty());
        let state = client_state(&database);
        let space = &state.spaces[&database.database_id().space_id()];
        assert_eq!(space.cursors.neck, space.cursors.tail);
        assert_eq!(space.admit_cursors.neck, space.admit_cursors.tail);
    }

    #[test]
    fn authority_read_surfaces_push_rejection_without_implicit_rollback() {
        let directory = tempfile::tempdir().unwrap();
        let server = server();
        let first = Database::open_with(
            directory.path().join("read-winner.sqlite"),
            OpenOptions::new().server(router(Arc::clone(&server))),
        )
        .unwrap();
        assert!(server.create_space(first.database_id().space_id()));
        let first_runtime = first.runtime().unwrap();
        first
            .execute(
                &first_runtime,
                "CREATE TABLE notes (id INTEGER PRIMARY KEY)",
                (),
            )
            .unwrap();
        assert_eq!(first.push().unwrap(), PushOutcome::Drained);

        let second_path = directory.path().join("read-loser.sqlite");
        let second = Database::open_with(
            &second_path,
            OpenOptions::new()
                .invitation(first.replica_invitation())
                .server(router(Arc::clone(&server))),
        )
        .unwrap();
        let second_runtime = second.runtime().unwrap();
        second
            .execute(
                &second_runtime,
                "CREATE TABLE NOTES (id INTEGER PRIMARY KEY, payload BLOB)",
                (),
            )
            .unwrap();
        assert_eq!(pending_ops(&second).len(), 1);
        drop(second_runtime);
        drop(second);

        let second = Database::open_with(
            &second_path,
            OpenOptions::new()
                .sync_policy(SyncPolicy::Remote)
                .server(router(Arc::clone(&server))),
        )
        .unwrap();
        let second_runtime = Arc::new(second.runtime().unwrap());
        let mut statement = second.prepare(&second_runtime, "SELECT 1").unwrap();

        let error = statement
            .query_map((), |row| row.get::<_, i64>(0))
            .unwrap_err();
        let Error::RefreshPushRejected(rejection) = error else {
            panic!("remote read did not surface its push rejection")
        };
        assert!(matches!(
            rejection.error(),
            KernelError::RangeAssertFailed { .. }
        ));
        assert!(table_exists(&second, "notes"));
        assert_eq!(pending_ops(&second).len(), 1);

        second.rollback(&rejection).unwrap();
        assert!(!table_exists(&second, "notes"));
        assert!(pending_ops(&second).is_empty());
    }

    #[test]
    fn create_table_and_homebase_submission_commit_atomically_and_survive_reopen() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("captured-schema.sqlite");
        let database = Database::open(&path).unwrap();
        let database_id = database.database_id();
        let runtime = Arc::new(database.runtime().unwrap());

        database
            .execute(
                &runtime,
                "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT NOT NULL)",
                (),
            )
            .unwrap();
        assert!(table_exists(&database, "notes"));

        let state = client_state(&database);
        let space = state.spaces.get(&database_id.space_id()).unwrap();
        assert_eq!(space.cursors.tail, DeviceSeq(2));
        let DeviceOp::Commit {
            entries,
            range_asserts,
            submit_mode,
            ..
        } = space.oplog.get(&DeviceSeq(1)).unwrap()
        else {
            panic!("captured schema operation was not a commit")
        };
        assert_eq!(entries.len(), 7);
        assert_eq!(range_asserts.len(), 2);
        assert_eq!(*submit_mode, SubmitMode::Unchecked);
        assert!(
            range_asserts
                .iter()
                .all(|assertion| assertion.upto == AdmissionSeq(0))
        );
        assert_eq!(range_asserts[0].prefix, *entries[2].key());
        assert_eq!(range_asserts[1].prefix, *entries[6].key());
        let pending = pending_ops(&database);
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].seq, DeviceSeq(1));
        assert!(pending[0].on_accept.is_empty());
        assert_eq!(
            pending[0].on_reject,
            [pending::Effect::DropTable {
                name: "notes".into()
            }]
        );

        drop(runtime);
        drop(database);

        let reopened = Database::open(&path).unwrap();
        assert!(table_exists(&reopened, "notes"));
        let state = client_state(&reopened);
        let space = state.spaces.get(&database_id.space_id()).unwrap();
        assert_eq!(space.cursors.tail, DeviceSeq(2));
        assert!(matches!(
            space.oplog.get(&DeviceSeq(1)),
            Some(DeviceOp::Commit { .. })
        ));
        assert_eq!(pending_ops(&reopened), pending);
    }

    #[test]
    fn failed_schema_submission_rolls_back_the_created_table_and_oplog() {
        let directory = tempfile::tempdir().unwrap();
        let database = Database::open(directory.path().join("atomic-schema.sqlite")).unwrap();
        let database_id = database.database_id();
        let runtime = database.runtime().unwrap();
        database.with_connection(|connection| {
            connection
                .execute_batch(
                    "CREATE TRIGGER reject_schema_submission
                     BEFORE INSERT ON __multilite__meta
                     BEGIN SELECT RAISE(ABORT, 'injected metadata failure'); END",
                )
                .unwrap();
        });

        assert!(
            database
                .execute(
                    &runtime,
                    "CREATE TABLE rolled_back (id INTEGER PRIMARY KEY)",
                    (),
                )
                .is_err()
        );
        assert!(!table_exists(&database, "rolled_back"));

        let state = client_state(&database);
        let space = state.spaces.get(&database_id.space_id()).unwrap();
        assert_eq!(
            space.cursors,
            homebase_client::meta::OplogCursors::default()
        );
        assert!(space.oplog.is_empty());
        assert!(pending_ops(&database).is_empty());
    }

    #[test]
    fn failed_pending_insert_rolls_back_the_table_and_homebase_submission() {
        let directory = tempfile::tempdir().unwrap();
        let database = Database::open(directory.path().join("atomic-pending.sqlite")).unwrap();
        let database_id = database.database_id();
        let runtime = database.runtime().unwrap();
        database.with_connection(|connection| {
            connection
                .execute_batch(
                    "CREATE TRIGGER reject_pending_insert
                     BEFORE INSERT ON __multilite__pending
                     BEGIN SELECT RAISE(ABORT, 'injected pending failure'); END",
                )
                .unwrap();
        });

        assert!(
            database
                .execute(
                    &runtime,
                    "CREATE TABLE rolled_back_pending (id INTEGER PRIMARY KEY)",
                    (),
                )
                .is_err()
        );
        assert!(!table_exists(&database, "rolled_back_pending"));
        assert!(pending_ops(&database).is_empty());

        let state = client_state(&database);
        let space = state.spaces.get(&database_id.space_id()).unwrap();
        assert_eq!(
            space.cursors,
            homebase_client::meta::OplogCursors::default()
        );
        assert!(space.oplog.is_empty());
    }

    #[test]
    fn push_drains_and_retires_accepted_pending_operations() {
        let directory = tempfile::tempdir().unwrap();
        let server = server();
        let database = Database::open_with(
            directory.path().join("pushed.sqlite"),
            OpenOptions::new().server(router(Arc::clone(&server))),
        )
        .unwrap();
        assert!(server.create_space(database.database_id().space_id()));
        let runtime = database.runtime().unwrap();
        database
            .execute(&runtime, "CREATE TABLE notes (id INTEGER PRIMARY KEY)", ())
            .unwrap();

        assert_eq!(database.push().unwrap(), PushOutcome::Drained);
        assert!(pending_ops(&database).is_empty());
        assert!(table_exists(&database, "notes"));
        let state = client_state(&database);
        let space = state
            .spaces
            .get(&database.database_id().space_id())
            .unwrap();
        assert_eq!(space.cursors.neck, DeviceSeq(2));
        assert_eq!(space.cursors.tail, DeviceSeq(2));
    }

    #[test]
    fn pull_fetches_admissions_without_applying_them_and_survives_reopen() {
        let directory = tempfile::tempdir().unwrap();
        let source_path = directory.path().join("source.sqlite");
        let replica_path = directory.path().join("replica.sqlite");
        let server = server();
        let source = Database::open_with(
            &source_path,
            OpenOptions::new().server(router(Arc::clone(&server))),
        )
        .unwrap();
        assert!(server.create_space(source.database_id().space_id()));
        let replica = Database::open_with(
            &replica_path,
            OpenOptions::new()
                .invitation(source.replica_invitation())
                .server(router(Arc::clone(&server))),
        )
        .unwrap();
        let source_runtime = source.runtime().unwrap();

        source
            .execute(
                &source_runtime,
                "CREATE TABLE notes (id INTEGER PRIMARY KEY)",
                (),
            )
            .unwrap();
        assert_eq!(source.push().unwrap(), PushOutcome::Drained);
        assert!(!table_exists(&replica, "notes"));

        let outcome = replica.pull().unwrap();
        assert_eq!(outcome.captured_through(), 1);
        let after_first_pull = client_state(&replica);
        let space = after_first_pull
            .spaces
            .get(&replica.database_id().space_id())
            .unwrap();
        assert_eq!(
            space.admit_cursors,
            AdmitCursors {
                head: AdmissionSeq(1),
                neck: AdmissionSeq(1),
                tail: AdmissionSeq(2),
            }
        );
        assert_eq!(space.admits.len(), 1);
        assert_eq!(space.admits[&AdmissionSeq(1)].entries.len(), 7);
        assert!(!table_exists(&replica, "notes"));

        assert_eq!(replica.pull().unwrap(), outcome);
        assert_eq!(client_state(&replica), after_first_pull);
        assert!(!table_exists(&replica, "notes"));

        drop(replica);
        let reopened = Database::open_with(
            &replica_path,
            OpenOptions::new().server(router(Arc::clone(&server))),
        )
        .unwrap();
        let reopened_state = client_state(&reopened);
        let reopened_space = reopened_state
            .spaces
            .get(&reopened.database_id().space_id())
            .unwrap();
        assert_eq!(reopened_space.admit_cursors, space.admit_cursors);
        assert_eq!(reopened_space.admits, space.admits);
        assert!(!table_exists(&reopened, "notes"));
    }

    #[test]
    fn unavailable_pull_preserves_the_admit_log() {
        let directory = tempfile::tempdir().unwrap();
        let database = Database::open(directory.path().join("offline-pull.sqlite")).unwrap();
        let before = client_state(&database);

        assert!(database.pull().is_err());
        assert_eq!(client_state(&database), before);
    }

    #[test]
    fn empty_rebase_is_an_idempotent_local_noop() {
        let directory = tempfile::tempdir().unwrap();
        let database = Database::open(directory.path().join("empty-rebase.sqlite")).unwrap();
        let runtime = database.runtime().unwrap();
        let before = client_state(&database);

        database.rebase(&runtime).unwrap();
        assert_eq!(client_state(&database), before);
    }

    #[test]
    fn rebase_rejects_pending_submission_even_without_fetched_admissions() {
        let directory = tempfile::tempdir().unwrap();
        let database = Database::open(directory.path().join("pending-rebase.sqlite")).unwrap();
        let runtime = database.runtime().unwrap();
        database
            .execute(&runtime, "CREATE TABLE notes (id INTEGER PRIMARY KEY)", ())
            .unwrap();
        let before = client_state(&database);

        assert!(matches!(
            database.rebase(&runtime),
            Err(Error::RebasePendingSubmissions)
        ));
        assert_eq!(client_state(&database), before);
        assert!(table_exists(&database, "notes"));
        assert_eq!(pending_ops(&database).len(), 1);
    }

    #[test]
    fn rebase_rejects_cursor_changes_between_snapshot_and_apply() {
        let directory = tempfile::tempdir().unwrap();
        let server = server();
        let source = Database::open_with(
            directory.path().join("moving-source.sqlite"),
            OpenOptions::new().server(router(Arc::clone(&server))),
        )
        .unwrap();
        assert!(server.create_space(source.database_id().space_id()));
        let replica = Database::open_with(
            directory.path().join("moving-replica.sqlite"),
            OpenOptions::new()
                .invitation(source.replica_invitation())
                .server(router(Arc::clone(&server))),
        )
        .unwrap();
        let source_runtime = source.runtime().unwrap();
        let replica_runtime = replica.runtime().unwrap();
        source
            .execute(
                &source_runtime,
                "CREATE TABLE first_remote (id INTEGER PRIMARY KEY)",
                (),
            )
            .unwrap();
        assert_eq!(source.push().unwrap(), PushOutcome::Drained);
        replica.pull().unwrap();

        let error = replica
            .rebase_after_snapshot(&replica_runtime, || {
                source.execute(
                    &source_runtime,
                    "CREATE TABLE second_remote (id INTEGER PRIMARY KEY)",
                    (),
                )?;
                assert_eq!(source.push()?, PushOutcome::Drained);
                replica.pull_locked()?;
                Ok(())
            })
            .unwrap_err();

        assert!(matches!(error, Error::RebaseStateChanged));
        assert!(!table_exists(&replica, "first_remote"));
        assert!(!table_exists(&replica, "second_remote"));
        let state = client_state(&replica);
        let space = &state.spaces[&replica.database_id().space_id()];
        assert_eq!(space.admit_cursors.neck, AdmissionSeq(1));
        assert_eq!(space.admit_cursors.tail, AdmissionSeq(3));
        assert_eq!(space.admits.len(), 2);
    }

    #[test]
    fn rebase_applies_foreign_tables_and_preserves_own_tables_on_both_replicas() {
        let directory = tempfile::tempdir().unwrap();
        let first_path = directory.path().join("first-rebase.sqlite");
        let second_path = directory.path().join("second-rebase.sqlite");
        let server = server();
        let first = Database::open_with(
            &first_path,
            OpenOptions::new().server(router(Arc::clone(&server))),
        )
        .unwrap();
        assert!(server.create_space(first.database_id().space_id()));
        let second = Database::open_with(
            &second_path,
            OpenOptions::new()
                .invitation(first.replica_invitation())
                .server(router(Arc::clone(&server))),
        )
        .unwrap();
        let first_runtime = first.runtime().unwrap();
        let second_runtime = second.runtime().unwrap();

        first
            .execute(
                &first_runtime,
                "CREATE TABLE notes (id INTEGER PRIMARY KEY)",
                (),
            )
            .unwrap();
        second
            .execute(
                &second_runtime,
                "CREATE TABLE tasks (id INTEGER PRIMARY KEY)",
                (),
            )
            .unwrap();
        assert_eq!(first.push().unwrap(), PushOutcome::Drained);
        assert_eq!(second.push().unwrap(), PushOutcome::Drained);
        assert_eq!(first.pull().unwrap().captured_through(), 2);
        assert_eq!(second.pull().unwrap().captured_through(), 2);

        first.rebase(&first_runtime).unwrap();
        second.rebase(&second_runtime).unwrap();
        for database in [&first, &second] {
            assert!(table_exists(database, "notes"));
            assert!(table_exists(database, "tasks"));
            let state = client_state(database);
            let space = &state.spaces[&database.database_id().space_id()];
            assert_eq!(space.admit_cursors.neck, AdmissionSeq(3));
            assert_eq!(space.admit_cursors.tail, AdmissionSeq(3));
        }

        drop(first_runtime);
        drop(first);
        let reopened = Database::open_with(
            &first_path,
            OpenOptions::new().server(router(Arc::clone(&server))),
        )
        .unwrap();
        assert!(table_exists(&reopened, "notes"));
        assert!(table_exists(&reopened, "tasks"));
        let state = client_state(&reopened);
        assert_eq!(
            state.spaces[&reopened.database_id().space_id()]
                .admit_cursors
                .neck,
            AdmissionSeq(3)
        );
    }

    #[test]
    fn pull_before_push_conflict_recovers_across_restarts_and_converges() {
        let directory = tempfile::tempdir().unwrap();
        let server = server();
        let first_path = directory.path().join("winning-schema.sqlite");
        let second_path = directory.path().join("conflicting-schema.sqlite");
        let first = Database::open_with(
            &first_path,
            OpenOptions::new().server(router(Arc::clone(&server))),
        )
        .unwrap();
        assert!(server.create_space(first.database_id().space_id()));
        let invitation = first.replica_invitation();
        let second = Database::open_with(
            &second_path,
            OpenOptions::new()
                .invitation(invitation)
                .server(router(Arc::clone(&server))),
        )
        .unwrap();
        let first_runtime = first.runtime().unwrap();
        let second_runtime = second.runtime().unwrap();

        first
            .execute(
                &first_runtime,
                "CREATE TABLE notes (id INTEGER PRIMARY KEY)",
                (),
            )
            .unwrap();
        assert_eq!(first.push().unwrap(), PushOutcome::Drained);
        second
            .execute(
                &second_runtime,
                "CREATE TABLE NOTES (id INTEGER PRIMARY KEY, payload BLOB)",
                (),
            )
            .unwrap();
        second.pull().unwrap();
        let before = client_state(&second);
        let before_sql = table_sql(&second, "notes").unwrap();

        assert!(matches!(
            second.rebase(&second_runtime),
            Err(Error::RebasePendingSubmissions)
        ));
        assert_eq!(client_state(&second), before);
        assert_eq!(table_sql(&second, "notes").unwrap(), before_sql);
        assert_eq!(pending_ops(&second).len(), 1);

        let PushOutcome::Rejected(before_restart) = second.push().unwrap() else {
            panic!("same-name schema submission unexpectedly drained")
        };
        drop(second_runtime);
        drop(second);

        let second = Database::open_with(
            &second_path,
            OpenOptions::new().server(router(Arc::clone(&server))),
        )
        .unwrap();
        let second_runtime = second.runtime().unwrap();
        assert!(table_exists(&second, "NOTES"));
        assert_eq!(pending_ops(&second).len(), 1);
        let PushOutcome::Rejected(after_restart) = second.push().unwrap() else {
            panic!("re-probed schema submission unexpectedly drained")
        };
        assert_eq!(after_restart, before_restart);

        second.rollback(&after_restart).unwrap();
        assert!(pending_ops(&second).is_empty());
        assert!(!table_exists(&second, "notes"));
        drop(second_runtime);
        drop(second);

        let second = Database::open_with(
            &second_path,
            OpenOptions::new().server(router(Arc::clone(&server))),
        )
        .unwrap();
        let second_runtime = second.runtime().unwrap();
        let state = client_state(&second);
        let space = &state.spaces[&second.database_id().space_id()];
        assert_eq!(space.cursors.neck, DeviceSeq(2));
        assert_eq!(space.cursors.tail, DeviceSeq(3));
        assert_eq!(
            space.oplog[&DeviceSeq(2)],
            DeviceOp::Rollback {
                marker: DeviceSeq(1)
            }
        );
        assert!(matches!(
            second.rebase(&second_runtime),
            Err(Error::RebasePendingSubmissions)
        ));
        assert_eq!(second.push().unwrap(), PushOutcome::Drained);
        drop(second_runtime);
        drop(second);

        let second = Database::open_with(
            &second_path,
            OpenOptions::new().server(router(Arc::clone(&server))),
        )
        .unwrap();
        let second_runtime = second.runtime().unwrap();
        second.pull().unwrap();
        second.rebase(&second_runtime).unwrap();
        assert!(table_exists(&second, "notes"));

        first.pull().unwrap();
        first.rebase(&first_runtime).unwrap();
        assert_eq!(table_sql(&first, "notes"), table_sql(&second, "notes"));
        let first_state = client_state(&first);
        let second_state = client_state(&second);
        assert_eq!(
            first_state.spaces[&first.database_id().space_id()].admit_cursors,
            second_state.spaces[&second.database_id().space_id()].admit_cursors
        );

        drop(first_runtime);
        drop(second_runtime);
        drop(first);
        drop(second);
        assert_eq!(
            stock_user_schema(&first_path),
            stock_user_schema(&second_path)
        );
        assert_eq!(
            stock_user_schema(&first_path),
            [(
                String::from("notes"),
                String::from("CREATE TABLE notes (id INTEGER PRIMARY KEY)")
            )]
        );
    }

    #[test]
    fn malformed_admitted_transaction_fails_rebase_without_advancing_neck() {
        let directory = tempfile::tempdir().unwrap();
        let server = server();
        let source = Database::open_with(
            directory.path().join("malformed-source.sqlite"),
            OpenOptions::new().server(router(Arc::clone(&server))),
        )
        .unwrap();
        assert!(server.create_space(source.database_id().space_id()));
        let replica = Database::open_with(
            directory.path().join("malformed-replica.sqlite"),
            OpenOptions::new()
                .invitation(source.replica_invitation())
                .server(router(Arc::clone(&server))),
        )
        .unwrap();
        let replica_runtime = replica.runtime().unwrap();
        let malformed_key = Key::from_bytes([b"malformed".as_slice()]).unwrap();
        block_on(async {
            source
                .client
                .space(source.database_id().space_id())
                .await
                .unwrap()
                .submit_unchecked(
                    vec![Mutation::Set {
                        key: malformed_key,
                        value: vec![1, 2, 3],
                    }],
                    vec![],
                )
                .await
                .unwrap();
        });
        assert_eq!(source.push().unwrap(), PushOutcome::Drained);
        replica.pull().unwrap();
        let before = client_state(&replica);

        assert!(matches!(
            replica.rebase(&replica_runtime),
            Err(Error::InvalidMultiliteTransaction(_))
        ));
        assert_eq!(client_state(&replica), before);
    }

    #[test]
    fn failed_remote_ddl_rolls_back_prior_tables_and_admit_neck() {
        let directory = tempfile::tempdir().unwrap();
        let server = server();
        let source = Database::open_with(
            directory.path().join("atomic-source.sqlite"),
            OpenOptions::new().server(router(Arc::clone(&server))),
        )
        .unwrap();
        assert!(server.create_space(source.database_id().space_id()));
        let replica = Database::open_with(
            directory.path().join("atomic-replica.sqlite"),
            OpenOptions::new()
                .invitation(source.replica_invitation())
                .server(router(Arc::clone(&server))),
        )
        .unwrap();
        let replica_runtime = replica.runtime().unwrap();
        submit_direct(&source, &create_operation("first_remote"));
        submit_direct(&source, &create_operation("occupied"));
        assert_eq!(source.push().unwrap(), PushOutcome::Drained);
        replica.pull().unwrap();
        replica.with_connection(|connection| {
            connection
                .execute_batch("CREATE TABLE occupied (id INTEGER PRIMARY KEY, local BLOB)")
                .unwrap();
        });
        let before = client_state(&replica);

        assert!(matches!(
            replica.rebase(&replica_runtime),
            Err(Error::Sqlite(_))
        ));
        assert!(!table_exists(&replica, "first_remote"));
        assert!(table_exists(&replica, "occupied"));
        assert_eq!(client_state(&replica), before);
    }

    #[test]
    fn rollback_preserves_an_accepted_prefix_and_retires_only_the_rejected_suffix() {
        let directory = tempfile::tempdir().unwrap();
        let server = server();
        let first = Database::open_with(
            directory.path().join("first.sqlite"),
            OpenOptions::new().server(router(Arc::clone(&server))),
        )
        .unwrap();
        assert!(server.create_space(first.database_id().space_id()));
        let second = Database::open_with(
            directory.path().join("second.sqlite"),
            OpenOptions::new()
                .invitation(first.replica_invitation())
                .server(router(Arc::clone(&server))),
        )
        .unwrap();
        let first_runtime = first.runtime().unwrap();
        let second_runtime = second.runtime().unwrap();

        first
            .execute(
                &first_runtime,
                "CREATE TABLE notes (id INTEGER PRIMARY KEY)",
                (),
            )
            .unwrap();
        assert_eq!(first.push().unwrap(), PushOutcome::Drained);

        second
            .execute(
                &second_runtime,
                "CREATE TABLE tasks (id INTEGER PRIMARY KEY)",
                (),
            )
            .unwrap();
        second
            .execute(
                &second_runtime,
                "CREATE TABLE \"NOTES\" (id INTEGER PRIMARY KEY)",
                (),
            )
            .unwrap();

        let PushOutcome::Rejected(rejection) = second.push().unwrap() else {
            panic!("same-name schema submission unexpectedly drained")
        };
        assert_eq!(rejection.database_id, second.database_id());
        assert_eq!(rejection.device_id, second.client.device());
        assert_eq!(rejection.failed_sequence(), 2);
        assert_eq!(
            rejection.submit_cursors,
            OplogCursors {
                head: DeviceSeq(2),
                neck: DeviceSeq(2),
                tail: DeviceSeq(3),
            }
        );
        assert!(matches!(
            rejection.error(),
            KernelError::RangeAssertFailed { failures } if failures.len() == 1
        ));
        let pending = pending_ops(&second);
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].seq, DeviceSeq(2));
        assert!(table_exists(&second, "tasks"));
        assert!(table_exists(&second, "NOTES"));

        second.rollback(&rejection).unwrap();
        assert!(pending_ops(&second).is_empty());
        assert!(table_exists(&second, "tasks"));
        assert!(!table_exists(&second, "NOTES"));
        let after_rollback = client_state(&second);
        let space = &after_rollback.spaces[&second.database_id().space_id()];
        assert_eq!(
            space.cursors,
            OplogCursors {
                head: DeviceSeq(2),
                neck: DeviceSeq(3),
                tail: DeviceSeq(4),
            }
        );
        assert_eq!(
            space.oplog[&DeviceSeq(3)],
            DeviceOp::Rollback {
                marker: DeviceSeq(2)
            }
        );

        second.rollback(&rejection).unwrap();
        assert_eq!(client_state(&second), after_rollback);
        assert!(matches!(
            second.rebase(&second_runtime),
            Err(Error::RebasePendingSubmissions)
        ));

        assert_eq!(second.push().unwrap(), PushOutcome::Drained);
        second.pull().unwrap();
        second.rebase(&second_runtime).unwrap();
        assert!(table_exists(&second, "tasks"));
        assert!(table_exists(&second, "notes"));
    }

    #[test]
    fn rollback_rejects_foreign_or_stale_push_rejections_without_mutation() {
        let directory = tempfile::tempdir().unwrap();
        let server = server();
        let first = Database::open_with(
            directory.path().join("stale-first.sqlite"),
            OpenOptions::new().server(router(Arc::clone(&server))),
        )
        .unwrap();
        assert!(server.create_space(first.database_id().space_id()));
        let second = Database::open_with(
            directory.path().join("stale-second.sqlite"),
            OpenOptions::new()
                .invitation(first.replica_invitation())
                .server(router(Arc::clone(&server))),
        )
        .unwrap();
        let first_runtime = first.runtime().unwrap();
        let second_runtime = second.runtime().unwrap();

        first
            .execute(
                &first_runtime,
                "CREATE TABLE notes (id INTEGER PRIMARY KEY)",
                (),
            )
            .unwrap();
        assert_eq!(first.push().unwrap(), PushOutcome::Drained);
        second
            .execute(
                &second_runtime,
                "CREATE TABLE NOTES (id INTEGER PRIMARY KEY)",
                (),
            )
            .unwrap();
        let PushOutcome::Rejected(rejection) = second.push().unwrap() else {
            panic!("same-name schema submission unexpectedly drained")
        };

        let first_before = client_state(&first);
        assert!(matches!(
            first.rollback(&rejection),
            Err(Error::StalePushRejection)
        ));
        assert_eq!(client_state(&first), first_before);
        assert!(table_exists(&first, "notes"));

        second
            .execute(
                &second_runtime,
                "CREATE TABLE tasks (id INTEGER PRIMARY KEY)",
                (),
            )
            .unwrap();
        let second_before = client_state(&second);
        let pending_before = pending_ops(&second);
        assert!(matches!(
            second.rollback(&rejection),
            Err(Error::StalePushRejection)
        ));
        assert_eq!(client_state(&second), second_before);
        assert_eq!(pending_ops(&second), pending_before);
        assert!(table_exists(&second, "NOTES"));
        assert!(table_exists(&second, "tasks"));
    }

    #[test]
    fn rollback_failure_restores_sqlite_pending_and_homebase_state_before_retry() {
        let directory = tempfile::tempdir().unwrap();
        let server = server();
        let first = Database::open_with(
            directory.path().join("atomic-rollback-first.sqlite"),
            OpenOptions::new().server(router(Arc::clone(&server))),
        )
        .unwrap();
        assert!(server.create_space(first.database_id().space_id()));
        let second = Database::open_with(
            directory.path().join("atomic-rollback-second.sqlite"),
            OpenOptions::new()
                .invitation(first.replica_invitation())
                .server(router(Arc::clone(&server))),
        )
        .unwrap();
        let first_runtime = first.runtime().unwrap();
        let second_runtime = second.runtime().unwrap();

        first
            .execute(
                &first_runtime,
                "CREATE TABLE notes (id INTEGER PRIMARY KEY)",
                (),
            )
            .unwrap();
        assert_eq!(first.push().unwrap(), PushOutcome::Drained);
        second
            .execute(
                &second_runtime,
                "CREATE TABLE NOTES (id INTEGER PRIMARY KEY)",
                (),
            )
            .unwrap();
        let PushOutcome::Rejected(rejection) = second.push().unwrap() else {
            panic!("same-name schema submission unexpectedly drained")
        };
        second.with_connection(|connection| {
            connection
                .execute_batch(
                    "CREATE TRIGGER reject_pending_rollback
                     BEFORE DELETE ON __multilite__pending
                     BEGIN SELECT RAISE(ABORT, 'injected pending rollback failure'); END",
                )
                .unwrap();
        });
        let state_before = client_state(&second);
        let pending_before = pending_ops(&second);

        assert!(second.rollback(&rejection).is_err());
        assert_eq!(client_state(&second), state_before);
        assert_eq!(pending_ops(&second), pending_before);
        assert!(table_exists(&second, "NOTES"));

        second.with_connection(|connection| {
            connection
                .execute_batch("DROP TRIGGER reject_pending_rollback")
                .unwrap();
        });
        second.rollback(&rejection).unwrap();
        assert!(pending_ops(&second).is_empty());
        assert!(!table_exists(&second, "NOTES"));
        let state = client_state(&second);
        let space = &state.spaces[&second.database_id().space_id()];
        assert_eq!(space.cursors.neck, DeviceSeq(2));
        assert_eq!(space.cursors.tail, DeviceSeq(3));
        assert_eq!(
            space.oplog[&DeviceSeq(2)],
            DeviceOp::Rollback {
                marker: DeviceSeq(1)
            }
        );
    }

    #[test]
    fn unavailable_push_preserves_the_active_pending_window() {
        let directory = tempfile::tempdir().unwrap();
        let database = Database::open(directory.path().join("offline.sqlite")).unwrap();
        let runtime = database.runtime().unwrap();
        database
            .execute(&runtime, "CREATE TABLE notes (id INTEGER PRIMARY KEY)", ())
            .unwrap();

        assert!(database.push().is_err());
        assert_eq!(pending_ops(&database).len(), 1);
        let state = client_state(&database);
        let space = state
            .spaces
            .get(&database.database_id().space_id())
            .unwrap();
        assert_eq!(space.cursors.neck, DeviceSeq(1));
        assert_eq!(space.cursors.tail, DeviceSeq(2));
    }

    #[test]
    fn accepted_push_with_failed_local_trim_recovers_after_reopen() {
        let directory = tempfile::tempdir().unwrap();
        let server = server();
        let path = directory.path().join("atomic-accept.sqlite");
        let database = Database::open_with(
            &path,
            OpenOptions::new().server(router(Arc::clone(&server))),
        )
        .unwrap();
        assert!(server.create_space(database.database_id().space_id()));
        let runtime = database.runtime().unwrap();
        database
            .execute(&runtime, "CREATE TABLE notes (id INTEGER PRIMARY KEY)", ())
            .unwrap();
        database.with_connection(|connection| {
            connection
                .execute_batch(
                    "CREATE TRIGGER reject_pending_cleanup
                     BEFORE DELETE ON __multilite__pending
                     BEGIN SELECT RAISE(ABORT, 'injected pending cleanup failure'); END",
                )
                .unwrap();
        });

        assert!(database.push().is_err());
        assert_eq!(pending_ops(&database).len(), 1);
        assert_eq!(
            client_state(&database)
                .spaces
                .get(&database.database_id().space_id())
                .unwrap()
                .cursors
                .neck,
            DeviceSeq(1)
        );
        drop(runtime);
        drop(database);

        SqliteConnection::open(&path)
            .unwrap()
            .execute_batch("DROP TRIGGER reject_pending_cleanup")
            .unwrap();
        let database = Database::open_with(
            &path,
            OpenOptions::new().server(router(Arc::clone(&server))),
        )
        .unwrap();

        assert_eq!(database.push().unwrap(), PushOutcome::Drained);
        assert!(pending_ops(&database).is_empty());
        assert_eq!(
            client_state(&database)
                .spaces
                .get(&database.database_id().space_id())
                .unwrap()
                .cursors
                .neck,
            DeviceSeq(2)
        );
        assert!(table_exists(&database, "notes"));
        assert_eq!(database.pull().unwrap().captured_through(), 1);
        let state = client_state(&database);
        let space = &state.spaces[&database.database_id().space_id()];
        assert_eq!(space.admit_cursors.tail, AdmissionSeq(2));
        assert_eq!(space.admits.len(), 1, "the retry must not admit twice");
    }

    #[test]
    fn database_owns_the_public_sql_surface_independent_of_format_hooks() {
        let directory = tempfile::tempdir().unwrap();
        let database = Database::open(directory.path().join("sql-surface.sqlite")).unwrap();
        let runtime = Arc::new(database.runtime().unwrap());

        assert!(matches!(
            database.execute(
                &runtime,
                "CREATE TABLE rejected (id INTEGER PRIMARY KEY AUTOINCREMENT)",
                (),
            ),
            Err(Error::UnsupportedSql("AUTOINCREMENT is not supported"))
        ));
        database
            .execute(
                &runtime,
                "CREATE TABLE accepted (id INTEGER PRIMARY KEY, value TEXT)",
                (),
            )
            .unwrap();
        assert!(
            database
                .execute(
                    &runtime,
                    "CREATE TABLE __multilite__rejected (value TEXT)",
                    (),
                )
                .is_err()
        );
        assert!(
            database
                .prepare(&runtime, "SELECT value FROM __multilite__meta")
                .is_err()
        );
        assert!(matches!(
            database.execute(&runtime, "DELETE FROM accepted", ()),
            Err(Error::UnsupportedSql(_))
        ));
    }

    #[test]
    fn identity_invitation_and_device_rules_survive_reopen() {
        let directory = tempfile::tempdir().unwrap();
        let first_path = directory.path().join("first.sqlite");
        let replica_path = directory.path().join("replica.sqlite");

        let first = Database::open(&first_path).unwrap();
        let database_id = first.database_id();
        let device_id = first.device_id();
        let invitation = first.replica_invitation();
        drop(first);

        let reopened = Database::open(&first_path).unwrap();
        assert_eq!(reopened.database_id(), database_id);
        assert_eq!(reopened.device_id(), device_id);

        let replica =
            Database::open_with(&replica_path, OpenOptions::new().invitation(invitation)).unwrap();
        assert_eq!(replica.database_id(), database_id);
        assert_ne!(replica.device_id(), device_id);
    }

    #[test]
    fn invitation_roundtrips_and_conflicting_identity_is_rejected() {
        let directory = tempfile::tempdir().unwrap();
        let first_path = directory.path().join("first.sqlite");
        let second_path = directory.path().join("second.sqlite");
        let first = Database::open(&first_path).unwrap();
        let encoded = first.replica_invitation().to_bytes();
        let invitation = ReplicaInvitation::from_bytes(&encoded).unwrap();
        assert_eq!(invitation.database_id(), first.database_id());
        let conflicting = Database::open(&second_path).unwrap().replica_invitation();
        drop(first);

        assert!(matches!(
            Database::open_with(&first_path, OpenOptions::new().invitation(conflicting)),
            Err(Error::DatabaseIdMismatch { .. })
        ));
        for malformed in [&[][..], &[2][..], &[1, 0][..], &[1; 18][..]] {
            assert!(matches!(
                ReplicaInvitation::from_bytes(malformed),
                Err(Error::InvalidReplicaInvitation)
            ));
        }
    }

    #[test]
    fn general_open_adopts_an_existing_sqlite_schema() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("existing.sqlite");
        SqliteConnection::open(&path)
            .unwrap()
            .execute_batch("CREATE TABLE application_data (id INTEGER PRIMARY KEY)")
            .unwrap();

        let database = Database::open(&path).unwrap();
        assert_ne!(database.database_id().to_bytes(), [0; 16]);
        database.with_connection(|connection| {
            assert!(
                connection
                    .query_row(
                        "SELECT EXISTS(SELECT 1 FROM sqlite_schema
                         WHERE name = '__multilite__meta')",
                        (),
                        |row| row.get::<_, bool>(0),
                    )
                    .unwrap()
            );
            assert!(connection
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE name = 'application_data')",
                    (),
                    |row| row.get::<_, bool>(0),
                )
                .unwrap());
        });
    }

    #[test]
    fn general_open_rejects_unrecognized_metadata_namespace_tables() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("reserved.sqlite");
        SqliteConnection::open(&path)
            .unwrap()
            .execute_batch("CREATE TABLE __multilite__meta_future (value BLOB NOT NULL)")
            .unwrap();

        assert!(matches!(
            Database::open(&path),
            Err(Error::InvalidDatabase(
                "metadata table namespace contains unexpected tables"
            ))
        ));
    }

    #[test]
    fn multi_row_insert_is_one_durable_pending_operation() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("rows.sqlite");
        let database = Database::open(&path).unwrap();
        let runtime = database.runtime().unwrap();
        database
            .execute(
                &runtime,
                "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT, payload BLOB)",
                (),
            )
            .unwrap();
        database
            .execute(
                &runtime,
                "WITH input(body, payload) AS (
                    VALUES ('one', x'01'), ('two', NULL), ('three', x'0304')
                 )
                 INSERT INTO notes (body, payload)
                 SELECT body, payload FROM input ORDER BY body DESC",
                (),
            )
            .unwrap();

        let state = client_state(&database);
        let space = state.spaces.get(&database.database_id.space_id()).unwrap();
        assert_eq!(space.cursors.tail, DeviceSeq(3));
        let DeviceOp::Commit {
            entries,
            range_asserts,
            ..
        } = space.oplog.get(&DeviceSeq(2)).unwrap()
        else {
            panic!("captured INSERT was not one commit")
        };
        assert_eq!(entries.len(), 4);
        assert_eq!(range_asserts.len(), 5);
        assert!(entries[1..].iter().all(|entry| {
            let components = entry.key().components();
            components.len() == 6 && components[3].as_bytes() == b"rows"
        }));

        let pending = pending_ops(&database);
        assert_eq!(pending.len(), 2);
        assert!(matches!(
            pending[1].transaction.operations(),
            [MultiliteOp::InsertRows(_)]
        ));
        assert!(matches!(
            pending[1].on_reject.as_slice(),
            [pending::Effect::DeleteRows { .. }]
        ));
        database.with_connection(|connection| {
            assert_eq!(
                connection
                    .query_row("SELECT count(*) FROM notes", (), |row| row.get::<_, i64>(0))
                    .unwrap(),
                3
            );
            let mut statement = connection
                .prepare("SELECT id, body FROM notes ORDER BY id")
                .unwrap();
            let rows = statement
                .query_map((), |row| {
                    Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
                })
                .unwrap()
                .collect::<rusqlite::Result<Vec<_>>>()
                .unwrap();
            assert_eq!(
                rows,
                [(1, "two".into()), (2, "three".into()), (3, "one".into())]
            );
        });

        drop(runtime);
        drop(database);
        let reopened = Database::open(&path).unwrap();
        assert_eq!(pending_ops(&reopened).len(), 2);
    }

    #[test]
    fn long_primary_key_succeeds_and_oversized_key_rolls_back_before_submission() {
        let directory = tempfile::tempdir().unwrap();
        let database = Database::open(directory.path().join("large-key.sqlite")).unwrap();
        let runtime = database.runtime().unwrap();
        database
            .execute(
                &runtime,
                "CREATE TABLE notes (id TEXT NOT NULL PRIMARY KEY)",
                (),
            )
            .unwrap();
        let longest = "x".repeat(MAX_COMPONENT_LEN - 1);
        assert_eq!(
            database
                .execute(
                    &runtime,
                    "INSERT INTO notes VALUES (?1)",
                    rusqlite::params![longest],
                )
                .unwrap(),
            1
        );
        let pending_before = pending_ops(&database);
        let state_before = client_state(&database);

        assert!(matches!(
            database.execute(
                &runtime,
                "INSERT INTO notes VALUES (?1)",
                rusqlite::params!["y".repeat(MAX_COMPONENT_LEN)],
            ),
            Err(Error::InvalidMultiliteOp(_))
        ));
        assert_eq!(pending_ops(&database), pending_before);
        assert_eq!(client_state(&database), state_before);
        database.with_connection(|connection| {
            assert_eq!(
                connection
                    .query_row("SELECT length(id) FROM notes", (), |row| row
                        .get::<_, i64>(0))
                    .unwrap(),
                i64::try_from(MAX_COMPONENT_LEN - 1).unwrap()
            );
        });
    }

    #[test]
    fn two_replicas_converge_rows_and_reject_only_a_conflicting_insert() {
        let directory = tempfile::tempdir().unwrap();
        let server = server();
        let first = Database::open_with(
            directory.path().join("first.sqlite"),
            OpenOptions::new().server(router(Arc::clone(&server))),
        )
        .unwrap();
        assert!(server.create_space(first.database_id.space_id()));
        let second = Database::open_with(
            directory.path().join("second.sqlite"),
            OpenOptions::new()
                .invitation(first.replica_invitation())
                .server(router(Arc::clone(&server))),
        )
        .unwrap();
        let first_runtime = first.runtime().unwrap();
        let second_runtime = second.runtime().unwrap();

        first
            .execute(
                &first_runtime,
                "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT NOT NULL)",
                (),
            )
            .unwrap();
        assert_eq!(first.push().unwrap(), PushOutcome::Drained);
        second.pull().unwrap();
        second.rebase(&second_runtime).unwrap();

        first
            .execute(&first_runtime, "INSERT INTO notes VALUES (1, 'first')", ())
            .unwrap();
        second
            .execute(
                &second_runtime,
                "INSERT INTO notes VALUES (2, 'second')",
                (),
            )
            .unwrap();
        assert_eq!(first.push().unwrap(), PushOutcome::Drained);
        assert_eq!(second.push().unwrap(), PushOutcome::Drained);
        first.pull().unwrap();
        second.pull().unwrap();
        first.rebase(&first_runtime).unwrap();
        second.rebase(&second_runtime).unwrap();

        first
            .execute(&first_runtime, "INSERT INTO notes VALUES (7, 'winner')", ())
            .unwrap();
        second
            .execute(&second_runtime, "INSERT INTO notes VALUES (7, 'loser')", ())
            .unwrap();
        assert_eq!(first.push().unwrap(), PushOutcome::Drained);
        let PushOutcome::Rejected(rejection) = second.push().unwrap() else {
            panic!("same primary key was not rejected")
        };
        assert!(matches!(
            rejection.error(),
            KernelError::RangeAssertFailed { .. }
        ));
        second.rollback(&rejection).unwrap();
        assert!(pending_ops(&second).is_empty());
        assert_eq!(second.push().unwrap(), PushOutcome::Drained);

        first.pull().unwrap();
        second.pull().unwrap();
        first.rebase(&first_runtime).unwrap();
        second.rebase(&second_runtime).unwrap();

        let rows = |database: &Database<_>| {
            database.with_connection(|connection| {
                let mut statement = connection
                    .prepare("SELECT id, body FROM notes ORDER BY id")
                    .unwrap();
                statement
                    .query_map((), |row| {
                        Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
                    })
                    .unwrap()
                    .collect::<rusqlite::Result<Vec<_>>>()
                    .unwrap()
            })
        };
        let expected = vec![
            (1, String::from("first")),
            (2, String::from("second")),
            (7, String::from("winner")),
        ];
        assert_eq!(rows(&first), expected);
        assert_eq!(rows(&second), expected);
    }

    #[test]
    fn failed_general_bootstrap_rolls_back_all_metadata() {
        let owner = ConnectionOwner::open_in_memory().unwrap();
        let metadata_inserts = Arc::new(AtomicUsize::new(0));
        let counted = Arc::clone(&metadata_inserts);
        owner.with_connection(|connection| {
            connection
                .authorizer(Some(move |context: AuthContext<'_>| match context.action {
                    AuthAction::Insert {
                        table_name: "__multilite__meta",
                    } if counted.fetch_add(1, Ordering::Relaxed) == 1 => Authorization::Deny,
                    _ => Authorization::Allow,
                }))
                .unwrap();
        });

        let error = match open_on(owner.clone(), OpenOptions::new().server(offline_router())) {
            Ok(_) => panic!("bootstrap unexpectedly succeeded"),
            Err(error) => error,
        };
        assert!(matches!(
            error,
            Error::Client(homebase_client::ClientError::Store(_))
        ));
        assert_eq!(metadata_inserts.load(Ordering::Relaxed), 2);
        owner.with_connection(|connection| {
            let tables: i64 = connection
                .query_row(
                    "SELECT count(*) FROM sqlite_schema
                     WHERE type = 'table' AND name NOT LIKE 'sqlite_%'",
                    (),
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(tables, 0);
        });
    }
}
