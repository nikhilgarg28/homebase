//! General Multilite database identity and Homebase lifecycle.

mod operation;
mod pending;
mod rebase;
mod schema;
mod sql;
mod store;

use std::path::Path;

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
use rusqlite::{Connection, Row};

use crate::connection::ConnectionOwner;
use crate::metastore::SqliteOrderedStore;
use crate::runtime::{ExecutionMode, HookPolicy, RuntimeConnection};
use crate::{Error, Params, Result};

use self::operation::MultiliteOp;
use self::store::DatabaseMetaStore;

pub use self::rebase::RebaseConflict;

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
    observed_neck: DeviceSeq,
    observed_tail: DeviceSeq,
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
        }
    }
}

impl<H: ServerHandle> OpenOptions<H> {
    /// Initialize from, or verify against, a replica invitation.
    pub fn invitation(mut self, invitation: ReplicaInvitation) -> Self {
        self.invitation = Some(invitation);
        self
    }

    /// Replace the server route while retaining all other options.
    pub fn server<S: ServerHandle>(self, server: S) -> OpenOptions<S> {
        OpenOptions {
            invitation: self.invitation,
            server,
        }
    }
}

pub(crate) type DatabaseClient<H> =
    Client<DatabaseMetaStore, H, SystemHybridClock, SystemNonceSource>;
pub(crate) type DatabaseRuntime<P> = RuntimeConnection<DatabaseHooks<P>>;

pub(crate) struct DatabaseHooks<P> {
    format: P,
}

impl<P: HookPolicy> HookPolicy for DatabaseHooks<P> {
    type Event = P::Event;

    fn authorize(&mut self, mode: ExecutionMode, context: AuthContext<'_>) -> Authorization {
        match authorize_database(mode, &context) {
            Authorization::Allow => self.format.authorize(mode, context),
            decision => decision,
        }
    }

    fn preupdate(
        &mut self,
        mode: ExecutionMode,
        database: &str,
        table: &str,
        update: &PreUpdateCase,
    ) -> Result<Option<Self::Event>> {
        self.format.preupdate(mode, database, table, update)
    }
}

/// An opened general Multilite database, without a temporary format wrapper.
pub(crate) struct Database<H: ServerHandle> {
    owner: ConnectionOwner,
    database_id: DatabaseId,
    client: DatabaseClient<H>,
}

impl Database<OfflineServer> {
    pub(crate) fn open(path: impl AsRef<Path>) -> Result<Self> {
        Self::open_with(path, OpenOptions::new())
    }
}

impl<H: ServerHandle> Database<H> {
    pub(crate) fn open_with(path: impl AsRef<Path>, options: OpenOptions<H>) -> Result<Self> {
        let owner = ConnectionOwner::open(path)?;
        open_on(owner, options.invitation, options.server)
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

    pub(crate) fn runtime<P: HookPolicy>(&self, format: P) -> Result<DatabaseRuntime<P>> {
        RuntimeConnection::new(self.owner.clone(), DatabaseHooks { format })
    }

    pub(crate) fn execute<P: HookPolicy, Q: Params>(
        &self,
        runtime: &DatabaseRuntime<P>,
        sql: &str,
        params: Q,
    ) -> Result<(usize, Vec<P::Event>)> {
        match sql::validate_execute(sql)? {
            sql::ValidatedExecute::Insert => runtime.run(ExecutionMode::Public, |connection| {
                Ok(connection.execute(sql, params)?)
            }),
            sql::ValidatedExecute::CreateTable(table) => {
                self.execute_create_table(runtime, sql, params, table)
            }
        }
    }

    pub(crate) fn push(&self) -> Result<PushOutcome> {
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
                    observed_neck: cursors.neck,
                    observed_tail: cursors.tail,
                    error,
                }))
            }
        }
    }

    pub(crate) fn pull(&self) -> Result<PullOutcome> {
        let through = block_on(async {
            let space = self.client.space(self.database_id.space_id()).await?;
            space.pull().await.map_err(ClientError::from)
        })?;
        Ok(PullOutcome { through })
    }

    fn execute_create_table<P: HookPolicy, Q: Params>(
        &self,
        runtime: &DatabaseRuntime<P>,
        sql: &str,
        params: Q,
        table: schema::CreateTableSpec,
    ) -> Result<(usize, Vec<P::Event>)> {
        let operation = MultiliteOp::create_table(sql, table);
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
        let (mutations, assertions) = operation.to_homebase().at(upto);

        runtime.run(ExecutionMode::Public, |connection| {
            let changed = connection.execute(sql, params)?;
            runtime.with_internal_metadata(|| {
                let submission = block_on(space.submit_unchecked(mutations, assertions))
                    .map_err(ClientError::from)?;
                pending::insert(connection, submission.seq, &operation)?;
                Ok(())
            })?;
            Ok(changed)
        })
    }

    pub(crate) fn prepare<P: HookPolicy>(
        &self,
        runtime: &DatabaseRuntime<P>,
        sql: &str,
    ) -> Result<Statement> {
        runtime.run(ExecutionMode::Public, |connection| {
            let statement = connection.prepare(sql)?;
            if statement.readonly() {
                Ok(())
            } else {
                Err(Error::PreparedWrite)
            }
        })?;
        Ok(Statement {
            owner: self.owner.clone(),
            sql: sql.to_owned(),
        })
    }

    #[cfg(test)]
    pub(crate) fn with_connection<T>(&self, operation: impl FnOnce(&Connection) -> T) -> T {
        self.owner.with_connection(operation)
    }

    pub(crate) fn with_savepoint<T>(
        &self,
        prefix: &str,
        operation: impl FnOnce(&Connection) -> Result<T>,
    ) -> Result<T> {
        self.owner.with_savepoint(prefix, operation)
    }

    fn submit_cursors(&self) -> Result<OplogCursors> {
        let store = DatabaseMetaStore::new(self.owner.clone());
        Ok(block_on(store.oplog_cursors(self.database_id.space_id()))?)
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
    sql: String,
}

impl Statement {
    /// Execute the query and eagerly map every row.
    pub fn query_map<T, P, F>(&mut self, params: P, map: F) -> Result<Vec<T>>
    where
        P: Params,
        F: FnMut(&Row<'_>) -> rusqlite::Result<T>,
    {
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

fn open_on<H: ServerHandle>(
    owner: ConnectionOwner,
    invitation: Option<ReplicaInvitation>,
    server: H,
) -> Result<Database<H>> {
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

fn classify(connection: &Connection) -> Result<DatabaseState> {
    let metadata = SqliteOrderedStore::is_initialized(connection)?;
    let pending = pending::is_initialized(connection)?;
    match (metadata, pending) {
        (false, false) => Ok(DatabaseState::Fresh),
        (true, true) => {
            SqliteOrderedStore::validate(connection)?;
            pending::validate(connection)?;
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

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use homebase::Server;
    use homebase::actor::{SpaceHandle, Spawner};
    use homebase::storage::MemoryStore;
    use homebase_client::meta::{AdmitCursors, ClientState, DeviceOp, SubmitMode};
    use homebase_client::server::offline_router;
    use homebase_core::clock::{ManualClock, Timestamp};
    use homebase_core::key::Key;
    use homebase_core::tag::{DeviceSeq, Mutation};
    use rusqlite::OptionalExtension;
    use rusqlite::hooks::{AuthAction, AuthContext, Authorization};

    use super::*;

    struct NoopFormat;

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

    impl HookPolicy for NoopFormat {
        type Event = ();

        fn authorize(&mut self, _mode: ExecutionMode, _context: AuthContext<'_>) -> Authorization {
            Authorization::Allow
        }

        fn preupdate(
            &mut self,
            _mode: ExecutionMode,
            _database: &str,
            _table: &str,
            _update: &PreUpdateCase,
        ) -> Result<Option<Self::Event>> {
            Ok(None)
        }
    }

    fn client_state<H: ServerHandle>(database: &Database<H>) -> ClientState {
        let store = DatabaseMetaStore::new(database.owner.clone());
        block_on(store.load()).unwrap()
    }

    fn table_exists<H: ServerHandle>(database: &Database<H>, table: &str) -> bool {
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

    fn pending_ops<H: ServerHandle>(database: &Database<H>) -> Vec<pending::PendingOp> {
        database.with_connection(pending::load).unwrap()
    }

    fn table_sql<H: ServerHandle>(database: &Database<H>, table: &str) -> Option<String> {
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

    fn submit_direct<H: ServerHandle>(database: &Database<H>, operation: &MultiliteOp) {
        let (mutations, assertions) = operation.to_homebase().at(AdmissionSeq(0));
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

    #[test]
    fn create_table_and_homebase_submission_commit_atomically_and_survive_reopen() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("captured-schema.sqlite");
        let database = Database::open(&path).unwrap();
        let database_id = database.database_id();
        let runtime = database.runtime(NoopFormat).unwrap();

        let (_changed, captured) = database
            .execute(
                &runtime,
                "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT NOT NULL)",
                (),
            )
            .unwrap();
        assert!(captured.is_empty());
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
        assert_eq!(entries.len(), 3);
        assert_eq!(range_asserts.len(), 2);
        assert_eq!(*submit_mode, SubmitMode::Unchecked);
        assert!(
            range_asserts
                .iter()
                .all(|assertion| assertion.upto == AdmissionSeq(0))
        );
        assert_eq!(range_asserts[0].prefix, *entries[1].key());
        assert_eq!(range_asserts[1].prefix, *entries[2].key());
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
        let runtime = database.runtime(NoopFormat).unwrap();
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
        let runtime = database.runtime(NoopFormat).unwrap();
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
        let runtime = database.runtime(NoopFormat).unwrap();
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
        let source_runtime = source.runtime(NoopFormat).unwrap();

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
        assert_eq!(space.admits[&AdmissionSeq(1)].entries.len(), 3);
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
        let runtime = database.runtime(NoopFormat).unwrap();
        let before = client_state(&database);

        database.rebase(&runtime).unwrap();
        assert_eq!(client_state(&database), before);
    }

    #[test]
    fn rebase_rejects_cursor_changes_between_analysis_and_apply() {
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
        let source_runtime = source.runtime(NoopFormat).unwrap();
        let replica_runtime = replica.runtime(NoopFormat).unwrap();
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
            .rebase_after_plan(&replica_runtime, || {
                source.execute(
                    &source_runtime,
                    "CREATE TABLE second_remote (id INTEGER PRIMARY KEY)",
                    (),
                )?;
                assert_eq!(source.push()?, PushOutcome::Drained);
                replica.pull()?;
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
        let first_runtime = first.runtime(NoopFormat).unwrap();
        let second_runtime = second.runtime(NoopFormat).unwrap();

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
    fn rebase_conflict_returns_a_stable_handle_without_mutation() {
        let directory = tempfile::tempdir().unwrap();
        let server = server();
        let first = Database::open_with(
            directory.path().join("winning-schema.sqlite"),
            OpenOptions::new().server(router(Arc::clone(&server))),
        )
        .unwrap();
        assert!(server.create_space(first.database_id().space_id()));
        let second = Database::open_with(
            directory.path().join("conflicting-schema.sqlite"),
            OpenOptions::new()
                .invitation(first.replica_invitation())
                .server(router(Arc::clone(&server))),
        )
        .unwrap();
        let first_runtime = first.runtime(NoopFormat).unwrap();
        let second_runtime = second.runtime(NoopFormat).unwrap();

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

        let Error::RebaseConflict(conflict) = second.rebase(&second_runtime).unwrap_err() else {
            panic!("same-name rebase returned the wrong failure")
        };
        assert_eq!(conflict.database_id, second.database_id());
        assert_eq!(conflict.device_id, second.client.device());
        assert_eq!(conflict.admitted_from(), 1);
        assert_eq!(conflict.admitted_to_exclusive(), 2);
        assert_eq!(conflict.conflicts().len(), 1);
        assert_eq!(conflict.conflicts()[0].device_seq, DeviceSeq(1));
        assert_eq!(client_state(&second), before);
        assert_eq!(table_sql(&second, "notes").unwrap(), before_sql);
        assert_eq!(pending_ops(&second).len(), 1);
    }

    #[test]
    fn malformed_admitted_operation_fails_rebase_without_advancing_neck() {
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
        let replica_runtime = replica.runtime(NoopFormat).unwrap();
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
            Err(Error::InvalidMultiliteOp(_))
        ));
        assert_eq!(client_state(&replica), before);
    }

    #[test]
    fn own_admission_does_not_overwrite_newer_local_materialization() {
        let directory = tempfile::tempdir().unwrap();
        let server = server();
        let database = Database::open_with(
            directory.path().join("own-mismatch.sqlite"),
            OpenOptions::new().server(router(Arc::clone(&server))),
        )
        .unwrap();
        assert!(server.create_space(database.database_id().space_id()));
        let runtime = database.runtime(NoopFormat).unwrap();
        database
            .execute(&runtime, "CREATE TABLE notes (id INTEGER PRIMARY KEY)", ())
            .unwrap();
        assert_eq!(database.push().unwrap(), PushOutcome::Drained);
        database.pull().unwrap();
        // This stands in for a later speculative drop-and-recreate, which the
        // current restricted SQL surface does not support yet.
        database.with_connection(|connection| {
            connection
                .execute_batch(
                    "DROP TABLE notes;
                     CREATE TABLE notes (id INTEGER PRIMARY KEY, payload BLOB)",
                )
                .unwrap();
        });
        database.rebase(&runtime).unwrap();

        assert!(
            table_sql(&database, "notes")
                .unwrap()
                .contains("payload BLOB")
        );
        let state = client_state(&database);
        let cursors = state.spaces[&database.database_id().space_id()].admit_cursors;
        assert_eq!(cursors.neck, AdmissionSeq(2));
        assert_eq!(cursors.tail, AdmissionSeq(2));
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
        let replica_runtime = replica.runtime(NoopFormat).unwrap();
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
    fn push_finalizes_an_accepted_prefix_but_retains_the_rejected_suffix() {
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
        let first_runtime = first.runtime(NoopFormat).unwrap();
        let second_runtime = second.runtime(NoopFormat).unwrap();

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
        assert_eq!(rejection.observed_neck, DeviceSeq(2));
        assert_eq!(rejection.observed_tail, DeviceSeq(3));
        assert!(matches!(
            rejection.error(),
            KernelError::RangeAssertFailed { failures } if failures.len() == 1
        ));
        let pending = pending_ops(&second);
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].seq, DeviceSeq(2));
        assert!(table_exists(&second, "tasks"));
        assert!(table_exists(&second, "NOTES"));
    }

    #[test]
    fn unavailable_push_preserves_the_active_pending_window() {
        let directory = tempfile::tempdir().unwrap();
        let database = Database::open(directory.path().join("offline.sqlite")).unwrap();
        let runtime = database.runtime(NoopFormat).unwrap();
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
    fn pending_cleanup_failure_rolls_back_homebase_trim_and_retry_converges() {
        let directory = tempfile::tempdir().unwrap();
        let server = server();
        let database = Database::open_with(
            directory.path().join("atomic-accept.sqlite"),
            OpenOptions::new().server(router(Arc::clone(&server))),
        )
        .unwrap();
        assert!(server.create_space(database.database_id().space_id()));
        let runtime = database.runtime(NoopFormat).unwrap();
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
        database.with_connection(|connection| {
            connection
                .execute_batch("DROP TRIGGER reject_pending_cleanup")
                .unwrap();
        });

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
    }

    #[test]
    fn database_owns_the_public_sql_surface_independent_of_format_hooks() {
        let directory = tempfile::tempdir().unwrap();
        let database = Database::open(directory.path().join("sql-surface.sqlite")).unwrap();
        let runtime = database.runtime(NoopFormat).unwrap();

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
        Connection::open(&path)
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
        Connection::open(&path)
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

        let error = match open_on(owner.clone(), None, offline_router()) {
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
