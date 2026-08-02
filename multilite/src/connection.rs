use crate::Result;
use parking_lot::ReentrantMutex;
use rusqlite::Connection;
use rusqlite::config::DbConfig;
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

    pub(crate) fn with_savepoint<T>(
        &self,
        prefix: &str,
        operation: impl FnOnce(&Connection) -> Result<T>,
    ) -> Result<T> {
        let name = self.next_savepoint_name(prefix);
        self.with_connection(|connection| {
            with_savepoint(connection, name, || operation(connection))
        })
    }
}

/// Run one operation inside a panic-safe SQLite savepoint.
pub(crate) fn with_savepoint<T, E>(
    connection: &Connection,
    name: impl Into<String>,
    operation: impl FnOnce() -> std::result::Result<T, E>,
) -> std::result::Result<T, E>
where
    E: From<rusqlite::Error>,
{
    let savepoint = ConnectionSavepoint::begin(connection, name.into())?;
    match operation() {
        Ok(value) => {
            savepoint.release()?;
            Ok(value)
        }
        Err(error) => match savepoint.rollback() {
            Ok(()) => Err(error),
            Err(rollback) => Err(rollback.into()),
        },
    }
}

/// Apply captured final state without re-running user triggers or intermediate
/// foreign-key checks, then validate the complete resulting database.
pub(crate) fn with_materialization_context<T, E>(
    connection: &Connection,
    operation: impl FnOnce() -> std::result::Result<T, E>,
    foreign_key_error: impl FnOnce() -> E,
) -> std::result::Result<T, E>
where
    E: From<rusqlite::Error>,
{
    let config = MaterializationConfig::suppress(connection)?;
    let value = operation()?;
    let foreign_key_violation = connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM pragma_foreign_key_check)",
        (),
        |row| row.get::<_, bool>(0),
    )?;
    if foreign_key_violation {
        return Err(foreign_key_error());
    }
    config.restore()?;
    Ok(value)
}

struct MaterializationConfig<'connection> {
    connection: &'connection Connection,
    triggers: bool,
    foreign_keys: bool,
    active: bool,
}

impl<'connection> MaterializationConfig<'connection> {
    fn suppress(connection: &'connection Connection) -> rusqlite::Result<Self> {
        let triggers = connection.db_config(DbConfig::SQLITE_DBCONFIG_ENABLE_TRIGGER)?;
        let foreign_keys = connection.db_config(DbConfig::SQLITE_DBCONFIG_ENABLE_FKEY)?;
        connection.set_db_config(DbConfig::SQLITE_DBCONFIG_ENABLE_TRIGGER, false)?;
        if let Err(error) = connection.set_db_config(DbConfig::SQLITE_DBCONFIG_ENABLE_FKEY, false) {
            let _ = connection.set_db_config(DbConfig::SQLITE_DBCONFIG_ENABLE_TRIGGER, triggers);
            return Err(error);
        }
        Ok(Self {
            connection,
            triggers,
            foreign_keys,
            active: true,
        })
    }

    fn restore(mut self) -> rusqlite::Result<()> {
        let triggers = self
            .connection
            .set_db_config(DbConfig::SQLITE_DBCONFIG_ENABLE_TRIGGER, self.triggers)
            .map(|_| ());
        let foreign_keys = self
            .connection
            .set_db_config(DbConfig::SQLITE_DBCONFIG_ENABLE_FKEY, self.foreign_keys)
            .map(|_| ());
        if triggers.is_ok() && foreign_keys.is_ok() {
            self.active = false;
        }
        triggers?;
        foreign_keys
    }
}

impl Drop for MaterializationConfig<'_> {
    fn drop(&mut self) {
        if self.active {
            let _ = self
                .connection
                .set_db_config(DbConfig::SQLITE_DBCONFIG_ENABLE_TRIGGER, self.triggers);
            let _ = self
                .connection
                .set_db_config(DbConfig::SQLITE_DBCONFIG_ENABLE_FKEY, self.foreign_keys);
        }
    }
}

pub(crate) struct ConnectionSavepoint<'connection> {
    connection: &'connection Connection,
    name: String,
    active: bool,
}

impl<'connection> ConnectionSavepoint<'connection> {
    pub(crate) fn begin(
        connection: &'connection Connection,
        name: String,
    ) -> rusqlite::Result<Self> {
        connection.execute_batch(&format!("SAVEPOINT {name}"))?;
        Ok(Self {
            connection,
            name,
            active: true,
        })
    }

    pub(crate) fn release(mut self) -> rusqlite::Result<()> {
        self.connection
            .execute_batch(&format!("RELEASE {}", self.name))?;
        self.active = false;
        Ok(())
    }

    pub(crate) fn rollback(mut self) -> rusqlite::Result<()> {
        self.connection
            .execute_batch(&format!("ROLLBACK TO {}; RELEASE {}", self.name, self.name))?;
        self.active = false;
        Ok(())
    }
}

impl Drop for ConnectionSavepoint<'_> {
    fn drop(&mut self) {
        if self.active {
            let _ = self
                .connection
                .execute_batch(&format!("ROLLBACK TO {}; RELEASE {}", self.name, self.name));
        }
    }
}

#[cfg(test)]
mod tests {
    use std::panic::{AssertUnwindSafe, catch_unwind};

    use super::*;
    use crate::Error;

    #[test]
    fn panic_rolls_back_and_closes_the_shared_savepoint() {
        let connection = Connection::open_in_memory().unwrap();
        connection
            .execute_batch("CREATE TABLE values_seen (value INTEGER NOT NULL)")
            .unwrap();

        let panic = catch_unwind(AssertUnwindSafe(|| {
            let _ = with_savepoint::<(), Error>(&connection, "panic_guard", || {
                connection.execute("INSERT INTO values_seen VALUES (1)", ())?;
                panic!("injected panic")
            });
        }));
        assert!(panic.is_err());
        assert_eq!(
            connection
                .query_row("SELECT count(*) FROM values_seen", (), |row| row
                    .get::<_, i64>(0))
                .unwrap(),
            0
        );

        with_savepoint::<_, Error>(&connection, "next_guard", || {
            connection.execute("INSERT INTO values_seen VALUES (2)", ())?;
            Ok(())
        })
        .unwrap();
        assert_eq!(
            connection
                .query_row("SELECT value FROM values_seen", (), |row| row
                    .get::<_, i64>(0))
                .unwrap(),
            2
        );
    }

    #[test]
    fn materialization_context_restores_connection_flags_after_a_panic() {
        let connection = Connection::open_in_memory().unwrap();
        connection
            .set_db_config(DbConfig::SQLITE_DBCONFIG_ENABLE_TRIGGER, true)
            .unwrap();
        connection
            .set_db_config(DbConfig::SQLITE_DBCONFIG_ENABLE_FKEY, true)
            .unwrap();

        let panic = catch_unwind(AssertUnwindSafe(|| {
            let _ = with_materialization_context::<(), Error>(
                &connection,
                || panic!("injected panic"),
                || Error::CommitConflict("foreign key".into()),
            );
        }));
        assert!(panic.is_err());
        assert!(
            connection
                .db_config(DbConfig::SQLITE_DBCONFIG_ENABLE_TRIGGER)
                .unwrap()
        );
        assert!(
            connection
                .db_config(DbConfig::SQLITE_DBCONFIG_ENABLE_FKEY)
                .unwrap()
        );
    }
}
