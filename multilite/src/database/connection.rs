//! Public SQLite-shaped connection over the general Multilite database.

use std::path::Path;
use std::sync::Arc;

use homebase_client::ServerHandle;

use super::{
    Database, DatabaseId, DatabaseRuntime, IsolationLevel, OfflineServer, OpenOptions, PullOutcome,
    PushOutcome, PushRejection, ReplicaInvitation, Statement, UpdateOptions, UpdateTransaction,
    ViewTransaction,
};
use crate::{Params, Result};
use rusqlite::Row;

/// An opened Multilite database connection.
pub struct Connection<H = OfflineServer>
where
    H: ServerHandle + Send + Sync + 'static,
{
    database: Arc<Database<H>>,
    runtime: Arc<DatabaseRuntime>,
}

impl Connection<OfflineServer> {
    /// Open or initialize a local Multilite database.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let database = Database::open(path)?;
        Self::finish_open(database)
    }
}

impl<H: ServerHandle + Send + Sync + 'static> Connection<H> {
    /// Open with explicit identity, authority, and synchronization options.
    pub fn open_with(path: impl AsRef<Path>, options: OpenOptions<H>) -> Result<Self> {
        let database = Database::open_with(path, options)?;
        Self::finish_open(database)
    }

    fn finish_open(database: Arc<Database<H>>) -> Result<Self> {
        let runtime = Arc::new(database.runtime()?);
        database.start_background_push()?;
        Ok(Self { database, runtime })
    }

    /// Database identity shared by every replica of this file's space.
    pub fn database_id(&self) -> DatabaseId {
        self.database.database_id()
    }

    /// Versioned onboarding material for another local replica.
    pub fn replica_invitation(&self) -> ReplicaInvitation {
        self.database.replica_invitation()
    }

    /// Device identity unique to this local replica file.
    pub fn device_id(&self) -> [u8; 16] {
        self.database.device_id()
    }

    /// Synchronization behavior selected when this connection was opened.
    pub fn sync_policy(&self) -> super::SyncPolicy {
        self.database.sync_policy()
    }

    /// Default isolation level selected when this connection was opened.
    pub fn isolation_level(&self) -> IsolationLevel {
        self.database.isolation_level()
    }

    /// Push this database's active local submissions as far as possible.
    pub fn push(&self) -> Result<PushOutcome> {
        self.database.push()
    }

    /// Fetch all currently available admissions without applying them.
    pub fn pull(&self) -> Result<PullOutcome> {
        self.database.pull()
    }

    /// Undo and retire the exact speculative suffix named by a push rejection.
    pub fn rollback(&self, rejection: &PushRejection) -> Result<()> {
        self.database.rollback(rejection)
    }

    /// Reconcile the currently fetched admit interval with local SQLite state.
    pub fn rebase(&self) -> Result<()> {
        self.database.rebase(&self.runtime)
    }

    /// Execute one supported mutating SQLite statement.
    pub fn execute<P: Params>(&self, sql: &str, params: P) -> Result<usize> {
        self.database.execute(&self.runtime, sql, params)
    }

    /// Run a closure inside one refreshed, read-only SQLite snapshot.
    pub fn view<T>(&self, operation: impl FnOnce(&ViewTransaction<'_>) -> Result<T>) -> Result<T> {
        self.database.view(&self.runtime, operation)
    }

    /// Run a closure as one SQLite and Homebase transaction.
    pub fn update<T>(
        &self,
        operation: impl FnOnce(&mut UpdateTransaction<'_, H>) -> Result<T>,
    ) -> Result<T> {
        self.database.update(&self.runtime, operation)
    }

    /// Run one managed update with an explicit per-transaction override.
    pub fn update_with<T>(
        &self,
        options: UpdateOptions,
        operation: impl FnOnce(&mut UpdateTransaction<'_, H>) -> Result<T>,
    ) -> Result<T> {
        self.database.update_with(&self.runtime, options, operation)
    }

    /// Execute one read-only statement as an implicit managed view.
    pub fn query<T, P, F>(&self, sql: &str, params: P, map: F) -> Result<Vec<T>>
    where
        P: Params,
        F: FnMut(&Row<'_>) -> rusqlite::Result<T>,
    {
        self.view(|transaction| transaction.query(sql, params, map))
    }

    /// Alias matching rusqlite's mapped-query vocabulary.
    pub fn query_map<T, P, F>(&self, sql: &str, params: P, map: F) -> Result<Vec<T>>
    where
        P: Params,
        F: FnMut(&Row<'_>) -> rusqlite::Result<T>,
    {
        self.query(sql, params, map)
    }

    /// Prepare one read-only statement.
    pub fn prepare(&self, sql: &str) -> Result<Statement> {
        self.database.prepare(&self.runtime, sql)
    }
}
