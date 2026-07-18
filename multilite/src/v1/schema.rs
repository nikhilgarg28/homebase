//! Resumable local schema migrations for the temporary V1 format.

use homebase_client::ServerHandle;
use rusqlite::Connection;

use crate::database::Database;
use crate::metastore::META_TABLE;
use crate::{Error, Result};

const VERSION_TABLE: &str = "__multilite__v1_schema";
const ITEMS_TABLE: &str = "items";
const CURRENT_VERSION: u64 = 1;

pub(crate) fn open<H: ServerHandle>(database: &Database<H>) -> Result<()> {
    database.with_savepoint("__multilite__v1_open", |connection| {
        if table_exists(connection, VERSION_TABLE)? {
            validate_version_table(connection)?;
            let version = read_version(connection)?;
            if version > CURRENT_VERSION {
                return Err(Error::UnsupportedV1SchemaVersion {
                    found: version,
                    supported: CURRENT_VERSION,
                });
            }
            if version != CURRENT_VERSION {
                return Err(Error::InvalidDatabase("unsupported V1 schema state"));
            }
            validate_items(connection)
        } else {
            migrate_from_zero(connection)
        }
    })
}

fn migrate_from_zero(connection: &Connection) -> Result<()> {
    let tables = user_tables(connection)?;
    if !tables.is_empty() {
        return Err(Error::InvalidDatabase(
            "V1 cannot initialize a database that already contains user tables",
        ));
    }
    connection.execute_batch(&format!(
        "CREATE TABLE {VERSION_TABLE} (
            singleton INTEGER PRIMARY KEY NOT NULL CHECK (singleton = 1),
            version INTEGER NOT NULL CHECK (version > 0)
        ) WITHOUT ROWID;
        CREATE TABLE {ITEMS_TABLE} (
            collection TEXT NOT NULL,
            id BLOB NOT NULL,
            payload BLOB NOT NULL,
            PRIMARY KEY (collection, id)
        );
        INSERT INTO {VERSION_TABLE} (singleton, version) VALUES (1, {CURRENT_VERSION});"
    ))?;
    Ok(())
}

fn validate_version_table(connection: &Connection) -> Result<()> {
    let columns = table_columns(connection, VERSION_TABLE)?;
    let expected = vec![
        (String::from("singleton"), String::from("INTEGER"), true, 1),
        (String::from("version"), String::from("INTEGER"), true, 0),
    ];
    if columns != expected {
        return Err(Error::InvalidDatabase(
            "V1 schema-version table does not match V1",
        ));
    }
    let schema_sql: String = connection.query_row(
        "SELECT sql FROM sqlite_schema WHERE type = 'table' AND name = ?1",
        [VERSION_TABLE],
        |row| row.get(0),
    )?;
    if !schema_sql.to_ascii_uppercase().contains("WITHOUT ROWID") {
        return Err(Error::InvalidDatabase(
            "V1 schema-version table must use WITHOUT ROWID",
        ));
    }
    Ok(())
}

fn read_version(connection: &Connection) -> Result<u64> {
    let mut statement = connection.prepare(&format!(
        "SELECT singleton, version FROM {VERSION_TABLE} ORDER BY singleton"
    ))?;
    let rows = statement
        .query_map((), |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    match rows.as_slice() {
        [(1, version)] => u64::try_from(*version)
            .map_err(|_| Error::InvalidDatabase("V1 schema version must be positive")),
        _ => Err(Error::InvalidDatabase(
            "V1 schema-version table must contain exactly one version row",
        )),
    }
}

fn validate_items(connection: &Connection) -> Result<()> {
    if !table_exists(connection, ITEMS_TABLE)? {
        return Err(Error::InvalidDatabase("V1 items table is missing"));
    }
    let columns = table_columns(connection, ITEMS_TABLE)?;
    let expected = vec![
        (String::from("collection"), String::from("TEXT"), true, 1),
        (String::from("id"), String::from("BLOB"), true, 2),
        (String::from("payload"), String::from("BLOB"), true, 0),
    ];
    if columns != expected {
        return Err(Error::InvalidDatabase("items schema does not match V1"));
    }
    Ok(())
}

fn table_columns(connection: &Connection, table: &str) -> Result<Vec<(String, String, bool, u32)>> {
    let mut statement = connection.prepare(&format!("PRAGMA table_info({table})"))?;
    Ok(statement
        .query_map((), |row| {
            Ok((
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, bool>(3)?,
                row.get::<_, u32>(5)?,
            ))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?)
}

fn table_exists(connection: &Connection, table: &str) -> Result<bool> {
    Ok(connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type = 'table' AND name = ?1)",
        [table],
        |row| row.get(0),
    )?)
}

fn user_tables(connection: &Connection) -> Result<Vec<String>> {
    let mut statement = connection.prepare(
        "SELECT name FROM sqlite_schema
         WHERE type = 'table'
           AND name NOT LIKE 'sqlite_%'
           AND name != ?1
         ORDER BY name",
    )?;
    Ok(statement
        .query_map([META_TABLE], |row| row.get(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    use rusqlite::hooks::{AuthAction, AuthContext, Authorization};

    use super::*;
    use crate::database::Database;

    #[test]
    fn version_zero_migrates_and_reopen_is_a_schema_noop() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("database.sqlite");
        let database = Database::open(&path).unwrap();
        open(&database).unwrap();
        let schema_version = database.with_connection(|connection| {
            connection
                .query_row("PRAGMA schema_version", (), |row| row.get::<_, i64>(0))
                .unwrap()
        });

        open(&database).unwrap();
        database.with_connection(|connection| {
            assert_eq!(read_version(connection).unwrap(), CURRENT_VERSION);
            validate_version_table(connection).unwrap();
            validate_items(connection).unwrap();
            assert_eq!(
                connection
                    .query_row("PRAGMA schema_version", (), |row| row.get::<_, i64>(0))
                    .unwrap(),
                schema_version
            );
        });
    }

    #[test]
    fn interrupted_v1_migration_preserves_base_and_retries_from_zero() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("database.sqlite");
        let database = Database::open(&path).unwrap();
        let database_id = database.database_id();
        let device_id = database.device_id();
        let denied = Arc::new(AtomicBool::new(false));
        let state = Arc::clone(&denied);
        database.with_connection(|connection| {
            connection
                .authorizer(Some(move |context: AuthContext<'_>| match context.action {
                    AuthAction::CreateTable {
                        table_name: "items",
                    } if !state.swap(true, Ordering::Relaxed) => Authorization::Deny,
                    _ => Authorization::Allow,
                }))
                .unwrap();
        });

        assert!(open(&database).is_err());
        database.with_connection(|connection| {
            assert!(table_exists(connection, META_TABLE).unwrap());
            assert!(!table_exists(connection, VERSION_TABLE).unwrap());
            assert!(!table_exists(connection, ITEMS_TABLE).unwrap());
        });
        drop(database);

        let reopened = Database::open(&path).unwrap();
        assert_eq!(reopened.database_id(), database_id);
        assert_eq!(reopened.device_id(), device_id);
        open(&reopened).unwrap();
        reopened.with_connection(|connection| {
            assert_eq!(read_version(connection).unwrap(), CURRENT_VERSION);
            validate_items(connection).unwrap();
        });
    }

    #[test]
    fn v1_rejects_existing_user_schema_after_general_adoption() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("existing.sqlite");
        Connection::open(&path)
            .unwrap()
            .execute_batch("CREATE TABLE application_data (id INTEGER PRIMARY KEY)")
            .unwrap();
        let database = Database::open(&path).unwrap();

        assert!(matches!(open(&database), Err(Error::InvalidDatabase(_))));
        database.with_connection(|connection| {
            assert!(table_exists(connection, META_TABLE).unwrap());
            assert!(!table_exists(connection, VERSION_TABLE).unwrap());
        });
    }

    #[test]
    fn newer_schema_versions_are_rejected() {
        let directory = tempfile::tempdir().unwrap();
        let database = Database::open(directory.path().join("newer.sqlite")).unwrap();
        database.with_connection(|connection| {
            connection
                .execute_batch(&format!(
                    "CREATE TABLE {VERSION_TABLE} (
                        singleton INTEGER PRIMARY KEY NOT NULL CHECK (singleton = 1),
                        version INTEGER NOT NULL CHECK (version > 0)
                    ) WITHOUT ROWID;
                    INSERT INTO {VERSION_TABLE} VALUES (1, 2);"
                ))
                .unwrap();
        });
        assert!(matches!(
            open(&database),
            Err(Error::UnsupportedV1SchemaVersion {
                found: 2,
                supported: 1,
            })
        ));
    }

    #[test]
    fn malformed_version_ledger_and_missing_items_are_rejected() {
        let directory = tempfile::tempdir().unwrap();
        let malformed = Database::open(directory.path().join("malformed.sqlite")).unwrap();
        malformed.with_connection(|connection| {
            connection
                .execute_batch(&format!(
                    "CREATE TABLE {VERSION_TABLE} (version TEXT NOT NULL);
                     INSERT INTO {VERSION_TABLE} VALUES ('one');"
                ))
                .unwrap();
        });
        assert!(matches!(open(&malformed), Err(Error::InvalidDatabase(_))));

        let missing = Database::open(directory.path().join("missing.sqlite")).unwrap();
        missing.with_connection(|connection| {
            connection
                .execute_batch(&format!(
                    "CREATE TABLE {VERSION_TABLE} (
                        singleton INTEGER PRIMARY KEY NOT NULL CHECK (singleton = 1),
                        version INTEGER NOT NULL CHECK (version > 0)
                    ) WITHOUT ROWID;
                    INSERT INTO {VERSION_TABLE} VALUES (1, {CURRENT_VERSION});"
                ))
                .unwrap();
        });
        assert!(matches!(
            open(&missing),
            Err(Error::InvalidDatabase("V1 items table is missing"))
        ));
    }
}
