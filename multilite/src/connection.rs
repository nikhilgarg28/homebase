use crate::{Error, Params, Result};
use parking_lot::ReentrantMutex;
use rusqlite::{Connection, Row, Statement};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// Serialized ownership of Multilite's one SQLite connection.
///
/// Access is reentrant on the owning thread because Homebase metadata writes
/// must join an outer SQLite operation on this same connection. Other threads
/// remain serialized by the mutex.
#[derive(Clone)]
pub(crate) struct ConnectionOwner {
    inner: Arc<ConnectionState>,
}

struct ConnectionState {
    connection: ReentrantMutex<Connection>,
    next_savepoint: AtomicU64,
}

impl ConnectionOwner {
    pub(crate) fn open(path: impl AsRef<Path>) -> Result<Self> {
        Ok(Self::new(Connection::open(path)?))
    }

    #[cfg(test)]
    pub(crate) fn open_in_memory() -> Result<Self> {
        Ok(Self::new(Connection::open_in_memory()?))
    }

    pub(crate) fn new(connection: Connection) -> Self {
        Self {
            inner: Arc::new(ConnectionState {
                connection: ReentrantMutex::new(connection),
                next_savepoint: AtomicU64::new(0),
            }),
        }
    }

    pub(crate) fn with_connection<T>(&self, operation: impl FnOnce(&Connection) -> T) -> T {
        let connection = self.inner.connection.lock();
        operation(&connection)
    }

    pub(crate) fn next_savepoint_name(&self, prefix: &str) -> String {
        let next = self.inner.next_savepoint.fetch_add(1, Ordering::Relaxed);
        format!("{prefix}_{next}")
    }
}

/// A Multilite-owned SQLite connection.
///
/// V1 exposes writes through [`execute`](Self::execute) and read-only prepared
/// statements through [`prepare`](Self::prepare). Schema ownership, capture,
/// and synchronization are added by later batches.
pub struct MultiliteConnection {
    inner: Connection,
}

impl MultiliteConnection {
    /// Open or create the SQLite file at `path`.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Ok(Self {
            inner: Connection::open(path)?,
        })
    }

    /// Execute one SQLite statement directly.
    pub fn execute<P: Params>(&self, sql: &str, params: P) -> Result<usize> {
        self.inner.execute(sql, params).map_err(Into::into)
    }

    /// Prepare one read-only statement.
    ///
    /// Writes remain confined to [`execute`](Self::execute), which gives later
    /// capture batches one path through which every mutation must pass.
    pub fn prepare(&self, sql: &str) -> Result<MultiliteStatement<'_>> {
        let inner = self.inner.prepare(sql)?;
        if !inner.readonly() {
            return Err(Error::PreparedWrite);
        }
        Ok(MultiliteStatement { inner })
    }
}

/// A read-only prepared statement owned by a [`MultiliteConnection`].
pub struct MultiliteStatement<'connection> {
    inner: Statement<'connection>,
}

impl MultiliteStatement<'_> {
    /// Execute the query and eagerly map every row.
    ///
    /// Eager collection avoids exposing rusqlite's raw statement and its
    /// mutation-capable methods while preserving its parameter and row
    /// conversion behavior.
    pub fn query_map<T, P, F>(&mut self, params: P, map: F) -> Result<Vec<T>>
    where
        P: Params,
        F: FnMut(&Row<'_>) -> rusqlite::Result<T>,
    {
        self.inner
            .query_map(params, map)?
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(Into::into)
    }
}
