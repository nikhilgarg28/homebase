//! Homebase metadata storage in Multilite's SQLite file.

#![cfg_attr(
    not(test),
    allow(
        dead_code,
        reason = "integrated into MultiliteConnection by later batches"
    )
)]

use homebase_core::storage::{Op, OrderedStore, ScanIter, StorageError, WriteBatch};
use rusqlite::{Connection, OptionalExtension, params, params_from_iter};
use std::future::{Future, ready};

use crate::Result as MultiliteResult;
use crate::connection::ConnectionOwner;

pub(crate) const META_TABLE: &str = "__multilite__meta";
const META_TABLE_PREFIX: &str = META_TABLE;
// Stay below SQLite's historical 999-variable default as well as modern
// bundled builds with substantially higher limits.
const MAX_BIND_PARAMETERS: usize = 900;
const MAX_PUTS_PER_STATEMENT: usize = MAX_BIND_PARAMETERS / 2;
type Entry = (Vec<u8>, Vec<u8>);
type StoreResult<T> = std::result::Result<T, StorageError>;

/// SQLite implementation of Homebase's ordered byte-map contract.
#[derive(Clone)]
pub(crate) struct SqliteOrderedStore {
    owner: ConnectionOwner,
}

impl SqliteOrderedStore {
    pub(crate) fn new(owner: ConnectionOwner) -> Self {
        Self { owner }
    }

    /// Create the ordered store's table as part of the caller's transaction.
    pub(crate) fn initialize(owner: &ConnectionOwner) -> MultiliteResult<()> {
        owner.with_connection(|connection| {
            connection.execute_batch(&format!(
                "CREATE TABLE IF NOT EXISTS {META_TABLE} (
                    key BLOB PRIMARY KEY NOT NULL,
                    value BLOB NOT NULL
                ) WITHOUT ROWID"
            ))
        })?;
        Ok(())
    }

    /// Return whether the metadata store exists, rejecting its reserved
    /// namespace if it contains anything other than the one known table.
    pub(crate) fn is_initialized(connection: &Connection) -> MultiliteResult<bool> {
        let tables = metadata_tables(connection)?;
        match tables.as_slice() {
            [] => Ok(false),
            [table] if table == META_TABLE => Ok(true),
            _ => Err(crate::Error::InvalidDatabase(
                "metadata table namespace contains unexpected tables",
            )),
        }
    }

    pub(crate) fn validate(connection: &Connection) -> MultiliteResult<()> {
        if !Self::is_initialized(connection)? {
            return Err(crate::Error::InvalidDatabase("metadata table is missing"));
        }
        let mut statement = connection.prepare(&format!("PRAGMA table_info({META_TABLE})"))?;
        let columns = statement
            .query_map((), |row| {
                Ok((
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, bool>(3)?,
                    row.get::<_, u32>(5)?,
                ))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        let expected = vec![
            (String::from("key"), String::from("BLOB"), true, 1),
            (String::from("value"), String::from("BLOB"), true, 0),
        ];
        if columns != expected {
            return Err(crate::Error::InvalidDatabase(
                "metadata table schema does not match the ordered store",
            ));
        }
        let schema_sql: String = connection.query_row(
            "SELECT sql FROM sqlite_schema WHERE type = 'table' AND name = ?1",
            [META_TABLE],
            |row| row.get(0),
        )?;
        if !schema_sql.to_ascii_uppercase().contains("WITHOUT ROWID") {
            return Err(crate::Error::InvalidDatabase(
                "metadata table must use WITHOUT ROWID",
            ));
        }
        Ok(())
    }

    #[cfg(test)]
    fn open_in_memory() -> MultiliteResult<Self> {
        let owner = ConnectionOwner::open_in_memory()?;
        Self::initialize(&owner)?;
        Ok(Self::new(owner))
    }

    fn get_now(&self, key: &[u8]) -> StoreResult<Option<Vec<u8>>> {
        self.owner
            .with_connection(|connection| {
                connection
                    .query_row(
                        &format!("SELECT value FROM {META_TABLE} WHERE key = ?1"),
                        [key],
                        |row| row.get(0),
                    )
                    .optional()
            })
            .map_err(storage_error)
    }

    fn scan_now(&self, start: &[u8], end: Option<&[u8]>) -> StoreResult<Vec<Entry>> {
        self.owner.with_connection(|connection| {
            let sql = match end {
                Some(_) => format!(
                    "SELECT key, value FROM {META_TABLE}
                     WHERE key >= ?1 AND key < ?2 ORDER BY key"
                ),
                None => format!(
                    "SELECT key, value FROM {META_TABLE}
                     WHERE key >= ?1 ORDER BY key"
                ),
            };
            let mut statement = connection.prepare(&sql).map_err(storage_error)?;
            let rows = match end {
                Some(end) => statement.query_map(params![start, end], read_entry),
                None => statement.query_map(params![start], read_entry),
            }
            .map_err(storage_error)?;
            rows.collect::<rusqlite::Result<Vec<_>>>()
                .map_err(storage_error)
        })
    }

    fn apply_now(&self, batch: WriteBatch) -> StoreResult<()> {
        if batch.is_empty() {
            return Ok(());
        }

        self.owner.with_connection(|connection| {
            let name = self.owner.next_savepoint_name("__multilite__store");
            connection
                .execute_batch(&format!("SAVEPOINT {name}"))
                .map_err(storage_error)?;

            match apply_ops(connection, batch) {
                Ok(()) => connection
                    .execute_batch(&format!("RELEASE {name}"))
                    .map_err(storage_error),
                Err(error) => {
                    let rollback = connection
                        .execute_batch(&format!("ROLLBACK TO {name}; RELEASE {name}"))
                        .map_err(storage_error);
                    rollback.and(Err(error))
                }
            }
        })
    }
}

fn metadata_tables(connection: &Connection) -> MultiliteResult<Vec<String>> {
    let mut statement = connection.prepare(
        "SELECT name FROM sqlite_schema
         WHERE type = 'table'
           AND substr(name, 1, length(?1)) = ?1 COLLATE NOCASE
         ORDER BY name",
    )?;
    Ok(statement
        .query_map([META_TABLE_PREFIX], |row| row.get(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?)
}

fn apply_ops(connection: &Connection, batch: WriteBatch) -> StoreResult<()> {
    let mut ops = batch.ops.into_iter().peekable();
    while let Some(op) = ops.next() {
        match op {
            Op::Put { key, value } => {
                let mut puts = vec![(key, value)];
                while puts.len() < MAX_PUTS_PER_STATEMENT
                    && matches!(ops.peek(), Some(Op::Put { .. }))
                {
                    let Some(Op::Put { key, value }) = ops.next() else {
                        unreachable!("peeked operation was a put")
                    };
                    puts.push((key, value));
                }
                apply_puts(connection, &puts)?;
            }
            Op::Delete { key } => {
                let mut keys = vec![key];
                while keys.len() < MAX_BIND_PARAMETERS
                    && matches!(ops.peek(), Some(Op::Delete { .. }))
                {
                    let Some(Op::Delete { key }) = ops.next() else {
                        unreachable!("peeked operation was a delete")
                    };
                    keys.push(key);
                }
                apply_deletes(connection, &keys)?;
            }
        }
    }
    Ok(())
}

fn apply_puts(connection: &Connection, puts: &[(Vec<u8>, Vec<u8>)]) -> StoreResult<()> {
    let mut sql = format!("INSERT INTO {META_TABLE} (key, value) VALUES ");
    for index in 0..puts.len() {
        if index > 0 {
            sql.push_str(", ");
        }
        sql.push_str("(?, ?)");
    }
    sql.push_str(" ON CONFLICT(key) DO UPDATE SET value = excluded.value");
    connection
        .execute(
            &sql,
            params_from_iter(
                puts.iter()
                    .flat_map(|(key, value)| [key.as_slice(), value.as_slice()]),
            ),
        )
        .map_err(storage_error)?;
    Ok(())
}

fn apply_deletes(connection: &Connection, keys: &[Vec<u8>]) -> StoreResult<()> {
    let placeholders = std::iter::repeat_n("?", keys.len())
        .collect::<Vec<_>>()
        .join(", ");
    connection
        .execute(
            &format!("DELETE FROM {META_TABLE} WHERE key IN ({placeholders})"),
            params_from_iter(keys.iter().map(Vec::as_slice)),
        )
        .map_err(storage_error)?;
    Ok(())
}

fn read_entry(row: &rusqlite::Row<'_>) -> rusqlite::Result<Entry> {
    Ok((row.get(0)?, row.get(1)?))
}

fn storage_error(error: rusqlite::Error) -> StorageError {
    StorageError(format!("SQLite metadata store: {error}"))
}

struct SqliteScan {
    result: std::vec::IntoIter<StoreResult<Entry>>,
}

impl ScanIter for SqliteScan {
    fn next(&mut self) -> impl Future<Output = StoreResult<Option<Entry>>> + Send {
        ready(self.result.next().transpose())
    }
}

impl OrderedStore for SqliteOrderedStore {
    fn get(&self, key: &[u8]) -> impl Future<Output = StoreResult<Option<Vec<u8>>>> + Send {
        ready(self.get_now(key))
    }

    fn scan(&self, start: Vec<u8>, end: Option<Vec<u8>>) -> impl ScanIter {
        let result = match self.scan_now(&start, end.as_deref()) {
            Ok(entries) => entries.into_iter().map(Ok).collect(),
            Err(error) => vec![Err(error)],
        };
        SqliteScan {
            result: result.into_iter(),
        }
    }

    fn apply(&self, batch: WriteBatch) -> impl Future<Output = StoreResult<()>> + Send {
        ready(self.apply_now(batch))
    }
}

#[cfg(test)]
mod tests {
    use homebase_client::meta::{
        MetaStore, OrderedMetaStore, audit, conformance as meta_conformance,
    };
    use homebase_core::clock::Timestamp;
    use homebase_core::storage::{self, OrderedStore};
    use homebase_core::tag::DeviceId;
    use pollster::block_on;
    use rusqlite::hooks::{AuthAction, AuthContext, Authorization, PreUpdateCase};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;
    use crate::runtime::{ExecutionMode, HookPolicy, RuntimeConnection};
    use crate::{Error, Result};

    struct NoopPolicy;

    impl HookPolicy for NoopPolicy {
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
    fn ordered_store_passes_conformance() {
        block_on(storage::conformance::run_all(|| {
            SqliteOrderedStore::open_in_memory().unwrap()
        }));
    }

    #[test]
    fn ordered_meta_store_passes_complete_conformance() {
        let store = SqliteOrderedStore::open_in_memory().unwrap();
        block_on(meta_conformance::run_all(&OrderedMetaStore::new(store)));
    }

    #[test]
    fn initialized_store_validates_its_schema_and_reserved_namespace() {
        let store = SqliteOrderedStore::open_in_memory().unwrap();
        store.owner.with_connection(|connection| {
            assert!(SqliteOrderedStore::is_initialized(connection).unwrap());
            SqliteOrderedStore::validate(connection).unwrap();

            connection
                .execute_batch("CREATE TABLE __MULTILITE__meta_future (value BLOB NOT NULL)")
                .unwrap();
            assert!(matches!(
                SqliteOrderedStore::validate(connection),
                Err(Error::InvalidDatabase(
                    "metadata table namespace contains unexpected tables"
                ))
            ));
        });
    }

    #[test]
    fn consecutive_mutations_are_grouped_into_bounded_statements() {
        let store = SqliteOrderedStore::open_in_memory().unwrap();
        let inserts = Arc::new(AtomicUsize::new(0));
        let deletes = Arc::new(AtomicUsize::new(0));
        let counted_inserts = Arc::clone(&inserts);
        let counted_deletes = Arc::clone(&deletes);
        store.owner.with_connection(|connection| {
            connection
                .authorizer(Some(move |context: AuthContext<'_>| {
                    match context.action {
                        AuthAction::Insert {
                            table_name: META_TABLE,
                        } => {
                            counted_inserts.fetch_add(1, Ordering::Relaxed);
                        }
                        AuthAction::Delete {
                            table_name: META_TABLE,
                        } => {
                            counted_deletes.fetch_add(1, Ordering::Relaxed);
                        }
                        _ => {}
                    }
                    Authorization::Allow
                }))
                .unwrap();
        });

        let entry_count = MAX_PUTS_PER_STATEMENT + 1;
        let mut puts = WriteBatch::new();
        for index in 0..entry_count {
            puts.put(index.to_be_bytes().to_vec(), b"value".to_vec());
        }
        block_on(store.apply(puts)).unwrap();
        assert_eq!(inserts.load(Ordering::Relaxed), 2);

        let mut deletes_batch = WriteBatch::new();
        for index in 0..entry_count {
            deletes_batch.delete(index.to_be_bytes().to_vec());
        }
        block_on(store.apply(deletes_batch)).unwrap();
        assert_eq!(deletes.load(Ordering::Relaxed), 1);
        assert!(
            block_on(storage::collect_scan(store.scan(Vec::new(), None)))
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn grouped_mutations_preserve_duplicate_key_and_mixed_run_order() {
        let store = SqliteOrderedStore::open_in_memory().unwrap();
        let mut batch = WriteBatch::new();
        batch.put(b"a".to_vec(), b"first".to_vec());
        batch.put(b"a".to_vec(), b"second".to_vec());
        batch.delete(b"a".to_vec());
        batch.put(b"a".to_vec(), b"last".to_vec());
        batch.put(b"b".to_vec(), b"temporary".to_vec());
        batch.delete(b"b".to_vec());
        block_on(store.apply(batch)).unwrap();

        assert_eq!(block_on(store.get(b"a")).unwrap(), Some(b"last".to_vec()));
        assert_eq!(block_on(store.get(b"b")).unwrap(), None);
    }

    #[test]
    fn metadata_and_domain_rows_commit_in_one_outer_unit() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("commit.sqlite");
        {
            let runtime = RuntimeConnection::open(&path, NoopPolicy).unwrap();
            let store = initialize_store(&runtime);
            let meta = OrderedMetaStore::new(store);

            runtime
                .run(ExecutionMode::InternalMetadata, |connection| {
                    connection.execute_batch("CREATE TABLE domain_rows (value TEXT NOT NULL)")?;
                    connection.execute("INSERT INTO domain_rows VALUES ('committed')", ())?;
                    block_on(meta.record_device(DeviceId([7; 16])))?;
                    block_on(meta.record_clock(Timestamp(700)))?;
                    Ok(())
                })
                .unwrap();

            assert_eq!(block_on(audit(&meta)).device, Some(DeviceId([7; 16])));
            assert_eq!(block_on(audit(&meta)).clock_high, Some(Timestamp(700)));
            assert_eq!(domain_row_count(&runtime), 1);
        }

        let runtime = RuntimeConnection::open(&path, NoopPolicy).unwrap();
        let meta = OrderedMetaStore::new(SqliteOrderedStore::new(runtime.owner()));
        assert_eq!(block_on(audit(&meta)).device, Some(DeviceId([7; 16])));
        assert_eq!(block_on(audit(&meta)).clock_high, Some(Timestamp(700)));
        assert_eq!(domain_row_count(&runtime), 1);
    }

    #[test]
    fn outer_rollback_removes_metadata_and_domain_rows() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("rollback.sqlite");
        {
            let runtime = RuntimeConnection::open(&path, NoopPolicy).unwrap();
            let store = initialize_store(&runtime);
            let meta = OrderedMetaStore::new(store);
            runtime
                .run(ExecutionMode::InternalMetadata, |connection| {
                    connection.execute_batch("CREATE TABLE domain_rows (value TEXT NOT NULL)")?;
                    Ok(())
                })
                .unwrap();

            let error = runtime
                .run(ExecutionMode::InternalMetadata, |connection| {
                    connection.execute("INSERT INTO domain_rows VALUES ('rolled back')", ())?;
                    block_on(meta.record_device(DeviceId([8; 16])))?;
                    block_on(meta.record_clock(Timestamp(800)))?;
                    Err::<(), _>(Error::CaptureInvariant("injected outer rollback"))
                })
                .unwrap_err();

            assert!(matches!(error, Error::CaptureInvariant(_)));
            assert_eq!(block_on(audit(&meta)).device, None);
            assert_eq!(block_on(audit(&meta)).clock_high, None);
            assert_eq!(domain_row_count(&runtime), 0);
        }

        let runtime = RuntimeConnection::open(&path, NoopPolicy).unwrap();
        let meta = OrderedMetaStore::new(SqliteOrderedStore::new(runtime.owner()));
        assert_eq!(block_on(audit(&meta)).device, None);
        assert_eq!(block_on(audit(&meta)).clock_high, None);
        assert_eq!(domain_row_count(&runtime), 0);
    }

    #[test]
    fn reopen_preserves_and_certifies_complete_metadata_state() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("metadata.sqlite");
        let before = {
            let owner = ConnectionOwner::open(&path).unwrap();
            SqliteOrderedStore::initialize(&owner).unwrap();
            let store = SqliteOrderedStore::new(owner);
            let meta = OrderedMetaStore::new(store);
            block_on(meta_conformance::run_all(&meta));
            block_on(meta.load()).unwrap()
        };

        let store = SqliteOrderedStore::new(ConnectionOwner::open(&path).unwrap());
        let after = block_on(audit(&OrderedMetaStore::new(store)));
        assert_eq!(after, before);
    }

    #[test]
    fn sqlite_error_rolls_back_the_entire_write_batch() {
        let store = SqliteOrderedStore::open_in_memory().unwrap();
        store.owner.with_connection(|connection| {
            connection
                .execute_batch(
                    "CREATE TRIGGER reject_bad_metadata
                     BEFORE INSERT ON __multilite__meta WHEN NEW.key = x'626164'
                     BEGIN SELECT RAISE(ABORT, 'injected'); END",
                )
                .unwrap();
        });
        let mut batch = WriteBatch::new();
        batch.put(b"good".to_vec(), b"value".to_vec());
        batch.put(b"bad".to_vec(), b"value".to_vec());

        assert!(block_on(store.apply(batch)).is_err());
        assert_eq!(block_on(store.get(b"good")).unwrap(), None);
        assert_eq!(block_on(store.get(b"bad")).unwrap(), None);
    }

    fn domain_row_count(runtime: &RuntimeConnection<NoopPolicy>) -> i64 {
        runtime
            .run(ExecutionMode::InternalMetadata, |connection| {
                Ok(connection
                    .query_row("SELECT count(*) FROM domain_rows", (), |row| row.get(0))?)
            })
            .unwrap()
            .0
    }

    fn initialize_store(runtime: &RuntimeConnection<NoopPolicy>) -> SqliteOrderedStore {
        let owner = runtime.owner();
        SqliteOrderedStore::initialize(&owner).unwrap();
        SqliteOrderedStore::new(owner)
    }
}
