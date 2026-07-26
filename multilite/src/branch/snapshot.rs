//! Pinned physical SQLite images used by private branches.

#![cfg_attr(
    not(test),
    allow(
        dead_code,
        reason = "consumed by managed branch transactions in batch 16"
    )
)]

use std::fmt;
use std::fs::File;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::Mutex;
use rusqlite::hooks::{AuthContext, Authorization};
use rusqlite::{Connection, OpenFlags};

use super::wal::{WalError, WalFrame, WalParser, WalSnapshot};

const MAX_CAPTURE_ATTEMPTS: usize = 8;
const MAX_IDLE_READERS: usize = 32;

/// One complete physical SQLite image, with or without committed WAL frames.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SqliteSnapshot {
    wal: Option<WalSnapshot>,
    page_count: u32,
    page_size: u32,
}

impl SqliteSnapshot {
    pub fn wal(&self) -> Option<&WalSnapshot> {
        self.wal.as_ref()
    }

    pub fn page_count(&self) -> u32 {
        self.page_count
    }

    pub fn page_size(&self) -> u32 {
        self.page_size
    }

    pub fn frame_for(&self, page: u32) -> Option<WalFrame> {
        self.wal
            .as_ref()
            .and_then(|wal| wal.page_map().get(&page).copied())
    }
}

/// Physical image plus the SQLite reader mark that keeps its files stable.
pub struct PinnedSnapshot {
    snapshot: SqliteSnapshot,
    database_path: PathBuf,
    wal_path: PathBuf,
    reader: PooledReader,
}

/// Native SQLite reader pinned at one ordered committer boundary.
pub struct PinnedReader {
    reader: PooledReader,
}

impl PinnedReader {
    pub(crate) fn with_reader<T>(&self, operation: impl FnOnce(&Connection) -> T) -> T {
        operation(self.reader.connection())
    }
}

impl PinnedSnapshot {
    /// Capture a WAL/base image and plant a companion reader at exactly that cut.
    pub fn capture(
        database_path: impl AsRef<Path>,
        wal_path: impl AsRef<Path>,
    ) -> Result<Self, SnapshotError> {
        SnapshotCache::new().capture(database_path, wal_path)
    }

    fn from_parts(
        snapshot: SqliteSnapshot,
        database_path: &Path,
        wal_path: &Path,
        reader: Connection,
        pool: Arc<ReaderPool>,
    ) -> Self {
        Self {
            snapshot,
            database_path: database_path.to_owned(),
            wal_path: wal_path.to_owned(),
            reader: PooledReader::new(reader, pool, ReaderDisposition::Physical),
        }
    }

    pub fn snapshot(&self) -> &SqliteSnapshot {
        &self.snapshot
    }

    pub fn database_path(&self) -> &Path {
        &self.database_path
    }

    pub fn wal_path(&self) -> &Path {
        &self.wal_path
    }

    /// Read metadata from the same SQLite transaction that pins this image.
    pub(crate) fn with_reader<T>(&self, operation: impl FnOnce(&Connection) -> T) -> T {
        operation(self.reader.connection())
    }

    pub fn into_snapshot_and_pin(self) -> (SqliteSnapshot, SnapshotPin) {
        let Self {
            snapshot,
            database_path: _,
            wal_path: _,
            reader,
        } = self;
        (
            snapshot,
            SnapshotPin {
                reader: Arc::new(Mutex::new(reader)),
            },
        )
    }
}

/// Stateful WAL-derived snapshot cache owned by one canonical committer.
pub struct SnapshotCache {
    parser: WalParser,
    readers: Arc<ReaderPool>,
}

impl Default for SnapshotCache {
    fn default() -> Self {
        Self::new()
    }
}

impl SnapshotCache {
    pub fn new() -> Self {
        Self {
            parser: WalParser::new(),
            readers: Arc::new(ReaderPool::new(MAX_IDLE_READERS)),
        }
    }

    /// Pin a native SQLite reader without deriving a branch page map.
    pub fn capture_reader(
        &self,
        database_path: impl AsRef<Path>,
    ) -> Result<PinnedReader, SnapshotError> {
        let (reader, generation, already_pinned) =
            self.readers.checkout_view(database_path.as_ref())?;
        if !already_pinned {
            reader.execute_batch("BEGIN")?;
            reader.query_row("PRAGMA page_count", (), |_| Ok(()))?;
        }
        Ok(PinnedReader {
            reader: PooledReader::new(
                reader,
                Arc::clone(&self.readers),
                ReaderDisposition::View(generation),
            ),
        })
    }

    /// Retire idle native views after one canonical visibility transition.
    pub fn invalidate_readers(&self) {
        self.readers.invalidate_views();
    }

    /// Capture a pinned image while parsing only WAL frames not seen previously.
    pub fn capture(
        &mut self,
        database_path: impl AsRef<Path>,
        wal_path: impl AsRef<Path>,
    ) -> Result<PinnedSnapshot, SnapshotError> {
        let database_path = database_path.as_ref();
        let wal_path = wal_path.as_ref();
        for _ in 0..MAX_CAPTURE_ATTEMPTS {
            let before = self.observe(wal_path)?;
            let reader = self.readers.checkout(database_path)?;
            reader.execute_batch("BEGIN")?;
            let page_count =
                reader.query_row("PRAGMA page_count", (), |row| row.get::<_, u32>(0))?;
            let page_size = reader.query_row("PRAGMA page_size", (), |row| row.get::<_, u32>(0))?;
            let after = self.observe(wal_path)?;
            if !same_tip(before.as_ref(), after.as_ref()) {
                self.readers.recycle(reader, ReaderDisposition::Physical);
                continue;
            }
            if let Some(wal) = &after
                && (wal.page_count() != page_count || wal.page_size() != page_size)
            {
                self.readers.recycle(reader, ReaderDisposition::Physical);
                continue;
            }
            return Ok(PinnedSnapshot::from_parts(
                SqliteSnapshot {
                    wal: after,
                    page_count,
                    page_size,
                },
                database_path,
                wal_path,
                reader,
                Arc::clone(&self.readers),
            ));
        }
        Err(SnapshotError::Unstable)
    }

    fn observe(&mut self, path: &Path) -> Result<Option<WalSnapshot>, SnapshotError> {
        let file = match File::open(path) {
            Ok(file) => file,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                self.parser.clear();
                return Ok(None);
            }
            Err(error) => return Err(error.into()),
        };
        if file.metadata()?.len() == 0 {
            self.parser.clear();
            return Ok(None);
        }
        Ok(self.parser.refresh_source(&file)?.snapshot)
    }
}

/// Reader ownership retained by a branch after its paths have been opened.
pub struct SnapshotPin {
    reader: Arc<Mutex<PooledReader>>,
}

impl Clone for SnapshotPin {
    fn clone(&self) -> Self {
        Self {
            reader: Arc::clone(&self.reader),
        }
    }
}

struct ReaderPool {
    idle: Mutex<Vec<IdleReader>>,
    max_idle: usize,
    view_generation: AtomicU64,
}

struct IdleReader {
    connection: Connection,
    view_generation: Option<u64>,
}

impl ReaderPool {
    fn new(max_idle: usize) -> Self {
        Self {
            idle: Mutex::new(Vec::new()),
            max_idle,
            view_generation: AtomicU64::new(0),
        }
    }

    fn checkout(&self, database_path: &Path) -> Result<Connection, SnapshotError> {
        let connection = match self.idle.lock().pop() {
            Some(idle) => {
                clear_authorizer(&idle.connection)?;
                if idle.view_generation.is_some() {
                    idle.connection.execute_batch("ROLLBACK")?;
                }
                idle.connection
            }
            None => open_reader(database_path)?,
        };
        connection.authorizer(None::<fn(AuthContext<'_>) -> Authorization>)?;
        Ok(connection)
    }

    fn checkout_view(
        &self,
        database_path: &Path,
    ) -> Result<(Connection, u64, bool), SnapshotError> {
        let generation = self.view_generation.load(Ordering::Acquire);
        let (connection, already_pinned) = match self.idle.lock().pop() {
            Some(idle) if idle.view_generation == Some(generation) => (idle.connection, true),
            Some(idle) => {
                clear_authorizer(&idle.connection)?;
                if idle.view_generation.is_some() {
                    idle.connection.execute_batch("ROLLBACK")?;
                }
                (idle.connection, false)
            }
            None => (open_reader(database_path)?, false),
        };
        connection.authorizer(None::<fn(AuthContext<'_>) -> Authorization>)?;
        Ok((connection, generation, already_pinned))
    }

    fn invalidate_views(&self) {
        self.view_generation.fetch_add(1, Ordering::AcqRel);
        let mut idle = self.idle.lock();
        idle.retain_mut(|reader| {
            if reader.view_generation.take().is_none() {
                return true;
            }
            if clear_authorizer(&reader.connection).is_err() {
                return false;
            }
            reader.connection.execute_batch("ROLLBACK").is_ok()
        });
    }

    fn recycle(&self, connection: Connection, disposition: ReaderDisposition) {
        let view_generation = match disposition {
            ReaderDisposition::Physical => {
                if clear_authorizer(&connection).is_err() {
                    return;
                }
                if connection.execute_batch("ROLLBACK").is_err() {
                    return;
                }
                None
            }
            ReaderDisposition::View(generation)
                if generation == self.view_generation.load(Ordering::Acquire) =>
            {
                Some(generation)
            }
            ReaderDisposition::View(_) => {
                if clear_authorizer(&connection).is_err() {
                    return;
                }
                if connection.execute_batch("ROLLBACK").is_err() {
                    return;
                }
                None
            }
        };
        let mut idle = self.idle.lock();
        if idle.len() < self.max_idle {
            idle.push(IdleReader {
                connection,
                view_generation,
            });
        }
    }
}

fn open_reader(database_path: &Path) -> Result<Connection, SnapshotError> {
    let connection = Connection::open_with_flags(
        database_path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )?;
    connection.pragma_update(None, "query_only", true)?;
    Ok(connection)
}

fn clear_authorizer(connection: &Connection) -> Result<(), SnapshotError> {
    connection.authorizer(None::<fn(AuthContext<'_>) -> Authorization>)?;
    Ok(())
}

#[derive(Clone, Copy)]
enum ReaderDisposition {
    Physical,
    View(u64),
}

struct PooledReader {
    connection: Option<Connection>,
    pool: Arc<ReaderPool>,
    disposition: ReaderDisposition,
}

impl PooledReader {
    fn new(connection: Connection, pool: Arc<ReaderPool>, disposition: ReaderDisposition) -> Self {
        Self {
            connection: Some(connection),
            pool,
            disposition,
        }
    }

    fn connection(&self) -> &Connection {
        self.connection
            .as_ref()
            .expect("pooled snapshot reader remains checked out")
    }
}

impl Drop for PooledReader {
    fn drop(&mut self) {
        let Some(connection) = self.connection.take() else {
            return;
        };
        self.pool.recycle(connection, self.disposition);
    }
}

fn same_tip(left: Option<&WalSnapshot>, right: Option<&WalSnapshot>) -> bool {
    match (left, right) {
        (None, None) => true,
        (Some(left), Some(right)) => {
            left.epoch() == right.epoch()
                && left.max_frame() == right.max_frame()
                && left.page_count() == right.page_count()
                && left.page_size() == right.page_size()
        }
        _ => false,
    }
}

#[derive(Debug)]
pub enum SnapshotError {
    Io(io::Error),
    Sqlite(rusqlite::Error),
    Wal(WalError),
    Unstable,
}

impl fmt::Display for SnapshotError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "snapshot file: {error}"),
            Self::Sqlite(error) => write!(formatter, "snapshot pin: {error}"),
            Self::Wal(error) => write!(formatter, "snapshot WAL: {error}"),
            Self::Unstable => formatter.write_str("database changed while pinning its snapshot"),
        }
    }
}

impl std::error::Error for SnapshotError {}

impl From<io::Error> for SnapshotError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<rusqlite::Error> for SnapshotError {
    fn from(error: rusqlite::Error) -> Self {
        Self::Sqlite(error)
    }
}

impl From<WalError> for SnapshotError {
    fn from(error: WalError) -> Self {
        Self::Wal(error)
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::thread;
    use std::time::Duration;

    use super::*;

    #[test]
    fn capture_supports_committed_empty_and_absent_wal_images() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("snapshot.sqlite");
        let wal_path = directory.path().join("snapshot.sqlite-wal");
        let writer = Connection::open(&path).unwrap();
        writer.pragma_update(None, "journal_mode", "WAL").unwrap();
        writer.pragma_update(None, "wal_autocheckpoint", 0).unwrap();
        writer
            .execute_batch("CREATE TABLE records(id INTEGER PRIMARY KEY)")
            .unwrap();

        let committed = PinnedSnapshot::capture(&path, &wal_path).unwrap();
        assert!(committed.snapshot().wal().is_some());
        assert_ne!(
            committed
                .snapshot()
                .wal()
                .expect("committed WAL")
                .epoch()
                .salts(),
            [0; 2]
        );
        assert!(committed.snapshot().page_count() > 0);
        drop(committed);

        writer
            .query_row("PRAGMA wal_checkpoint(TRUNCATE)", (), |_| Ok(()))
            .unwrap();
        let empty = PinnedSnapshot::capture(&path, &wal_path).unwrap();
        assert!(empty.snapshot().wal().is_none());
        assert!(empty.snapshot().page_count() > 0);
        drop(empty);

        drop(writer);
        if wal_path.exists() {
            fs::remove_file(&wal_path).unwrap();
        }
        let absent = PinnedSnapshot::capture(&path, &wal_path).unwrap();
        assert!(absent.snapshot().wal().is_none());
        assert!(absent.snapshot().page_count() > 0);
    }

    #[test]
    fn capture_stays_consistent_while_another_connection_commits_and_checkpoints() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("racing.sqlite");
        let wal_path = directory.path().join("racing.sqlite-wal");
        let writer = Connection::open(&path).unwrap();
        writer.pragma_update(None, "journal_mode", "WAL").unwrap();
        writer.pragma_update(None, "wal_autocheckpoint", 0).unwrap();
        writer
            .execute_batch(
                "CREATE TABLE state (
                    singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
                    left_value INTEGER NOT NULL,
                    right_value INTEGER NOT NULL
                 );
                 INSERT INTO state VALUES (1, 0, 0)",
            )
            .unwrap();
        drop(writer);

        let running = Arc::new(AtomicBool::new(true));
        let writer_running = Arc::clone(&running);
        let writer_path = path.clone();
        let worker = thread::spawn(move || {
            let writer = Connection::open(writer_path).unwrap();
            writer.pragma_update(None, "wal_autocheckpoint", 0).unwrap();
            for value in 1..=80 {
                writer
                    .execute(
                        "UPDATE state SET left_value = ?1, right_value = ?1",
                        [value],
                    )
                    .unwrap();
                if value % 7 == 0 {
                    writer
                        .query_row("PRAGMA wal_checkpoint(PASSIVE)", (), |_| Ok(()))
                        .unwrap();
                }
                thread::sleep(Duration::from_micros(250));
            }
            writer_running.store(false, Ordering::Release);
        });

        let mut cache = SnapshotCache::new();
        let mut captures = 0;
        while running.load(Ordering::Acquire) || captures < 12 {
            match cache.capture(&path, &wal_path) {
                Ok(snapshot) => {
                    let branch = super::super::ReadBranch::open(snapshot).unwrap();
                    let (left, right) = branch
                        .connection()
                        .query_row("SELECT left_value, right_value FROM state", (), |row| {
                            Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?))
                        })
                        .unwrap();
                    assert_eq!(left, right);
                    assert_eq!(
                        branch
                            .connection()
                            .query_row("PRAGMA integrity_check", (), |row| {
                                row.get::<_, String>(0)
                            })
                            .unwrap(),
                        "ok"
                    );
                    captures += 1;
                }
                Err(SnapshotError::Unstable) => {}
                Err(error) => panic!("snapshot capture failed: {error}"),
            }
        }
        worker.join().unwrap();
        assert!(captures >= 12);
    }

    #[test]
    fn cached_capture_tracks_appends_and_checkpoint_rotation() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("cached.sqlite");
        let wal_path = directory.path().join("cached.sqlite-wal");
        let writer = Connection::open(&path).unwrap();
        writer.pragma_update(None, "journal_mode", "WAL").unwrap();
        writer.pragma_update(None, "wal_autocheckpoint", 0).unwrap();
        writer
            .execute_batch("CREATE TABLE records(id INTEGER PRIMARY KEY)")
            .unwrap();

        let mut cache = SnapshotCache::new();
        let first = cache.capture(&path, &wal_path).unwrap();
        let first_epoch = first.snapshot().wal().unwrap().epoch();
        let first_frame = first.snapshot().wal().unwrap().max_frame();
        drop(first);

        writer
            .execute("INSERT INTO records VALUES (1)", ())
            .unwrap();
        let second = cache.capture(&path, &wal_path).unwrap();
        assert!(second.snapshot().wal().unwrap().max_frame() > first_frame);
        assert_eq!(
            second
                .with_reader(|reader| reader
                    .query_row("SELECT count(*) FROM records", (), |row| row
                        .get::<_, i64>(0)))
                .unwrap(),
            1
        );
        drop(second);

        writer
            .query_row("PRAGMA wal_checkpoint(TRUNCATE)", (), |_| Ok(()))
            .unwrap();
        let base_only = cache.capture(&path, &wal_path).unwrap();
        assert!(base_only.snapshot().wal().is_none());
        drop(base_only);

        writer
            .execute("INSERT INTO records VALUES (2)", ())
            .unwrap();
        let rotated = cache.capture(&path, &wal_path).unwrap();
        assert_ne!(rotated.snapshot().wal().unwrap().epoch(), first_epoch);
        assert_eq!(
            rotated
                .with_reader(|reader| reader
                    .query_row("SELECT count(*) FROM records", (), |row| row
                        .get::<_, i64>(0)))
                .unwrap(),
            2
        );
    }

    #[test]
    fn native_reader_cache_reuses_one_generation_and_invalidates_after_commit() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("reader-cache.sqlite");
        let writer = Connection::open(&path).unwrap();
        writer.pragma_update(None, "journal_mode", "WAL").unwrap();
        writer.pragma_update(None, "wal_autocheckpoint", 0).unwrap();
        writer
            .execute_batch(
                "CREATE TABLE records (id INTEGER PRIMARY KEY);
                 INSERT INTO records VALUES (1)",
            )
            .unwrap();
        let cache = SnapshotCache::new();

        let first = cache.capture_reader(&path).unwrap();
        assert_eq!(
            first.with_reader(|reader| {
                reader.query_row("SELECT count(*) FROM records", (), |row| {
                    row.get::<_, i64>(0)
                })
            }),
            Ok(1)
        );
        drop(first);

        writer
            .execute("INSERT INTO records VALUES (2)", ())
            .unwrap();
        let same_generation = cache.capture_reader(&path).unwrap();
        assert_eq!(
            same_generation.with_reader(|reader| {
                reader.query_row("SELECT count(*) FROM records", (), |row| {
                    row.get::<_, i64>(0)
                })
            }),
            Ok(1)
        );
        drop(same_generation);

        cache.invalidate_readers();
        let refreshed = cache.capture_reader(&path).unwrap();
        assert_eq!(
            refreshed.with_reader(|reader| {
                reader.query_row("SELECT count(*) FROM records", (), |row| {
                    row.get::<_, i64>(0)
                })
            }),
            Ok(2)
        );
    }
}
