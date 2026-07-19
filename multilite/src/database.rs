//! General Multilite database identity and Homebase lifecycle.

mod operation;
mod schema;
mod sql;

use std::path::Path;

use homebase_client::cipher::{SpaceEnvelope, SystemNonceSource};
use homebase_client::meta::{MetaStore, OrderedMetaStore};
use homebase_client::server::UnreachableSpace;
use homebase_client::{Client, ServerHandle};
use homebase_core::clock::{Lineage, SystemHybridClock};
use homebase_core::space::SpaceId;
use homebase_core::tag::DeviceId;
use pollster::block_on;
use rusqlite::hooks::{AuthAction, AuthContext, Authorization, PreUpdateCase};
use rusqlite::{Connection, Row};

use crate::connection::ConnectionOwner;
use crate::metastore::SqliteOrderedStore;
use crate::runtime::{ExecutionMode, HookPolicy, RuntimeConnection};
use crate::{Error, Params, Result};

const REPLICA_INVITATION_VERSION: u8 = 1;

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
    Client<OrderedMetaStore<SqliteOrderedStore>, H, SystemHybridClock, SystemNonceSource>;
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
        sql::validate_execute(sql)?;
        runtime.run(ExecutionMode::Public, |connection| {
            Ok(connection.execute(sql, params)?)
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
    let store = OrderedMetaStore::new(SqliteOrderedStore::new(owner.clone()));
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
    let store = OrderedMetaStore::new(SqliteOrderedStore::new(owner.clone()));
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
    if SqliteOrderedStore::is_initialized(connection)? {
        SqliteOrderedStore::validate(connection)?;
        Ok(DatabaseState::Initialized)
    } else {
        Ok(DatabaseState::Fresh)
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
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use homebase_client::server::offline_router;
    use rusqlite::hooks::{AuthAction, AuthContext, Authorization};

    use super::*;

    struct NoopFormat;

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
