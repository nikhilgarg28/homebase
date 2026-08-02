//! Public SQLite-shaped connection over the general Multilite database.

use std::future::Future;
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

    /// Asynchronously open without blocking the caller's executor thread.
    pub fn open_async(path: impl AsRef<Path>) -> impl Future<Output = Result<Self>> {
        let path = path.as_ref().to_owned();
        async move {
            crate::blocking::run(move || {
                let database = Database::open(path)?;
                Self::finish_open(database)
            })
            .await
        }
    }
}

impl<H: ServerHandle + Send + Sync + 'static> Connection<H> {
    /// Open with explicit identity, authority, and synchronization options.
    pub fn open_with(path: impl AsRef<Path>, options: OpenOptions<H>) -> Result<Self> {
        let database = Database::open_with(path, options)?;
        Self::finish_open(database)
    }

    /// Asynchronously open with explicit identity, authority, and policies.
    pub fn open_with_async(
        path: impl AsRef<Path>,
        options: OpenOptions<H>,
    ) -> impl Future<Output = Result<Self>> {
        let path = path.as_ref().to_owned();
        async move {
            crate::blocking::run(move || {
                let database = Database::open_with(path, options)?;
                Self::finish_open(database)
            })
            .await
        }
    }

    fn finish_open(database: Arc<Database<H>>) -> Result<Self> {
        let runtime = Arc::new(database.runtime()?);
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

    /// Asynchronously push local submissions as far as possible.
    pub async fn push_async(&self) -> Result<PushOutcome> {
        self.database.push_async().await
    }

    /// Fetch all currently available admissions without applying them.
    pub fn pull(&self) -> Result<PullOutcome> {
        self.database.pull()
    }

    /// Asynchronously fetch available admissions without applying them.
    pub async fn pull_async(&self) -> Result<PullOutcome> {
        self.database.pull_async().await
    }

    /// Undo and retire the exact speculative suffix named by a push rejection.
    pub fn rollback(&self, rejection: &PushRejection) -> Result<()> {
        self.database.rollback(rejection)
    }

    /// Asynchronously undo and retire one exact rejected speculative suffix.
    pub async fn rollback_async(&self, rejection: &PushRejection) -> Result<()> {
        self.database.rollback_async(rejection.clone()).await
    }

    /// Reconcile the currently fetched admit interval with local SQLite state.
    pub fn rebase(&self) -> Result<()> {
        self.database.rebase(&self.runtime)
    }

    /// Asynchronously apply the currently fetched admission interval.
    pub async fn rebase_async(&self) -> Result<()> {
        self.database.rebase_async().await
    }

    /// Execute one supported mutating SQLite statement.
    pub fn execute<P: Params>(&self, sql: &str, params: P) -> Result<usize> {
        self.database.execute(&self.runtime, sql, params)
    }

    /// Asynchronously execute one supported mutating statement.
    ///
    /// Parameters must be owned and sendable because SQLite executes on the
    /// bounded blocking pool. Tuples and arrays of `rusqlite::types::Value`
    /// work naturally for heterogeneous owned parameters.
    pub async fn execute_async<P>(&self, sql: impl Into<String>, params: P) -> Result<usize>
    where
        P: Params + Send + 'static,
    {
        self.database.execute_async(sql.into(), params).await
    }

    /// Run a closure inside one refreshed, read-only SQLite snapshot.
    pub fn view<T>(&self, operation: impl FnOnce(&ViewTransaction<'_>) -> Result<T>) -> Result<T> {
        self.database.view(&self.runtime, operation)
    }

    /// Asynchronously run an owned closure on one read-only SQLite snapshot.
    pub async fn view_async<T, F>(&self, operation: F) -> Result<T>
    where
        T: Send + 'static,
        F: Send + 'static + for<'a> FnOnce(&ViewTransaction<'a>) -> Result<T>,
    {
        self.database.view_async(operation).await
    }

    /// Run a closure as one SQLite and Homebase transaction.
    pub fn update<T>(
        &self,
        operation: impl FnOnce(&mut UpdateTransaction<'_, H>) -> Result<T>,
    ) -> Result<T> {
        self.database.update(&self.runtime, operation)
    }

    /// Asynchronously run one managed SQLite and Homebase transaction.
    pub async fn update_async<T, F>(&self, operation: F) -> Result<T>
    where
        T: Send + 'static,
        F: Send + 'static + for<'a> FnOnce(&mut UpdateTransaction<'a, H>) -> Result<T>,
    {
        self.database.update_async(operation).await
    }

    /// Run one managed update with an explicit per-transaction override.
    pub fn update_with<T>(
        &self,
        options: UpdateOptions,
        operation: impl FnOnce(&mut UpdateTransaction<'_, H>) -> Result<T>,
    ) -> Result<T> {
        self.database.update_with(&self.runtime, options, operation)
    }

    /// Asynchronously run one managed update with an isolation override.
    pub async fn update_with_async<T, F>(&self, options: UpdateOptions, operation: F) -> Result<T>
    where
        T: Send + 'static,
        F: Send + 'static + for<'a> FnOnce(&mut UpdateTransaction<'a, H>) -> Result<T>,
    {
        self.database.update_with_async(options, operation).await
    }

    /// Execute one row-producing statement and eagerly map its results.
    ///
    /// Reads run on a pinned view. `INSERT`, `UPDATE`, and `DELETE` with a
    /// `RETURNING` clause run as an implicit managed update. The mapper runs
    /// against that speculative branch; the mapped values are returned only
    /// after the branch commits under the configured sync policy.
    pub fn query<T, P, F>(&self, sql: &str, params: P, map: F) -> Result<Vec<T>>
    where
        P: Params,
        F: FnMut(&Row<'_>) -> rusqlite::Result<T>,
    {
        self.database.query(&self.runtime, sql, params, map)
    }

    /// Alias matching rusqlite's mapped-query vocabulary.
    pub fn query_map<T, P, F>(&self, sql: &str, params: P, map: F) -> Result<Vec<T>>
    where
        P: Params,
        F: FnMut(&Row<'_>) -> rusqlite::Result<T>,
    {
        self.query(sql, params, map)
    }

    /// Asynchronously execute one row-producing statement and return owned values.
    pub async fn query_async<T, P, F>(
        &self,
        sql: impl Into<String>,
        params: P,
        map: F,
    ) -> Result<Vec<T>>
    where
        T: Send + 'static,
        P: Params + Send + 'static,
        F: Send + 'static + for<'a> FnMut(&Row<'a>) -> rusqlite::Result<T>,
    {
        self.database.query_async(sql.into(), params, map).await
    }

    /// Async alias matching rusqlite's mapped-query vocabulary.
    pub async fn query_map_async<T, P, F>(
        &self,
        sql: impl Into<String>,
        params: P,
        map: F,
    ) -> Result<Vec<T>>
    where
        T: Send + 'static,
        P: Params + Send + 'static,
        F: Send + 'static + for<'a> FnMut(&Row<'a>) -> rusqlite::Result<T>,
    {
        self.query_async(sql, params, map).await
    }

    /// Validate and prepare one reusable read or write statement.
    pub fn prepare(&self, sql: &str) -> Result<Statement<H>> {
        self.database.prepare(&self.runtime, sql)
    }

    /// Asynchronously validate and prepare one reusable read or write statement.
    pub async fn prepare_async(&self, sql: impl Into<String>) -> Result<Statement<H>> {
        self.database
            .prepare_async(Arc::clone(&self.runtime), sql.into())
            .await
    }
}
