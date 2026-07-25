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

use parking_lot::Mutex;
use rusqlite::{Connection, OpenFlags};

use super::wal::{WalError, WalFrame, WalParse, WalParser, WalSnapshot, read_observation};

const MAX_CAPTURE_ATTEMPTS: usize = 8;

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
    _reader: Connection,
}

impl PinnedSnapshot {
    /// Capture a WAL/base image and plant a companion reader at exactly that cut.
    pub fn capture(
        database_path: impl AsRef<Path>,
        wal_path: impl AsRef<Path>,
    ) -> Result<Self, SnapshotError> {
        let database_path = database_path.as_ref();
        let wal_path = wal_path.as_ref();
        for _ in 0..MAX_CAPTURE_ATTEMPTS {
            let before = read_snapshot(wal_path)?;
            let reader = Connection::open_with_flags(
                database_path,
                OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
            )?;
            reader.execute_batch("PRAGMA query_only = ON; BEGIN")?;
            reader.query_row("SELECT count(*) FROM main.sqlite_schema", (), |_| Ok(()))?;
            let page_count =
                reader.query_row("PRAGMA page_count", (), |row| row.get::<_, u32>(0))?;
            let page_size = reader.query_row("PRAGMA page_size", (), |row| row.get::<_, u32>(0))?;
            let after = read_snapshot(wal_path)?;
            if before != after {
                continue;
            }
            if let Some(wal) = &after
                && (wal.page_count() != page_count || wal.page_size() != page_size)
            {
                continue;
            }
            return Ok(Self {
                snapshot: SqliteSnapshot {
                    wal: after,
                    page_count,
                    page_size,
                },
                database_path: database_path.to_owned(),
                wal_path: wal_path.to_owned(),
                _reader: reader,
            });
        }
        Err(SnapshotError::Unstable)
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
        operation(&self._reader)
    }

    pub fn into_snapshot_and_pin(self) -> (SqliteSnapshot, SnapshotPin) {
        let Self {
            snapshot,
            database_path: _,
            wal_path: _,
            _reader,
        } = self;
        (
            snapshot,
            SnapshotPin {
                _reader: Arc::new(Mutex::new(_reader)),
            },
        )
    }
}

/// Reader ownership retained by a branch after its paths have been opened.
pub struct SnapshotPin {
    _reader: Arc<Mutex<Connection>>,
}

impl Clone for SnapshotPin {
    fn clone(&self) -> Self {
        Self {
            _reader: Arc::clone(&self._reader),
        }
    }
}

fn read_snapshot(path: &Path) -> Result<Option<WalSnapshot>, SnapshotError> {
    let file = match File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let bytes = read_observation(&file)?;
    if bytes.is_empty() {
        return Ok(None);
    }
    let WalParse { snapshot, .. } = WalParser::parse(&bytes)?;
    Ok(snapshot)
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

        let mut captures = 0;
        while running.load(Ordering::Acquire) || captures < 12 {
            match PinnedSnapshot::capture(&path, &wal_path) {
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
}
