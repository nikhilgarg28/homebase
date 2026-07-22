//! Transaction-start coordinates and their companion SQLite reader pin.

#![cfg_attr(
    not(test),
    allow(
        dead_code,
        reason = "consumed by managed branch transactions in batch 16"
    )
)]

use std::fmt;
use std::fs::File;
use std::path::{Path, PathBuf};

use homebase_client::meta::OplogCursors;
use homebase_core::tag::AdmissionSeq;
use rusqlite::{Connection, OpenFlags};

use super::wal::{WalError, WalSnapshot, read_observation};
use super::wal::{WalParse, WalParser};

const MAX_CAPTURE_ATTEMPTS: usize = 8;

/// Monotone canonical SQLite commit coordinate.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct LocalGeneration(pub u64);

/// Complete transaction-start cut across SQLite and Homebase state.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SnapshotDescriptor {
    pub image: WalSnapshot,
    pub local_generation: LocalGeneration,
    pub authority_applied_through: AdmissionSeq,
    pub submit_cursors: OplogCursors,
}

/// Descriptor plus the real SQLite reader mark that keeps its files stable.
pub struct PinnedSnapshot {
    descriptor: SnapshotDescriptor,
    database_path: PathBuf,
    wal_path: PathBuf,
    _reader: Connection,
}

impl PinnedSnapshot {
    /// Capture a WAL image and plant a companion reader at exactly that cut.
    pub fn capture(
        database_path: impl AsRef<Path>,
        wal_path: impl AsRef<Path>,
        local_generation: LocalGeneration,
        authority_applied_through: AdmissionSeq,
        submit_cursors: OplogCursors,
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
            let after = read_snapshot(wal_path)?;
            if before == after {
                return Ok(Self {
                    descriptor: SnapshotDescriptor {
                        image: after,
                        local_generation,
                        authority_applied_through,
                        submit_cursors,
                    },
                    database_path: database_path.to_owned(),
                    wal_path: wal_path.to_owned(),
                    _reader: reader,
                });
            }
        }
        Err(SnapshotError::Unstable)
    }

    pub fn descriptor(&self) -> &SnapshotDescriptor {
        &self.descriptor
    }

    pub fn database_path(&self) -> &Path {
        &self.database_path
    }

    pub fn wal_path(&self) -> &Path {
        &self.wal_path
    }

    pub fn into_descriptor_and_pin(self) -> (SnapshotDescriptor, SnapshotPin) {
        let Self {
            descriptor,
            database_path: _,
            wal_path: _,
            _reader,
        } = self;
        (descriptor, SnapshotPin { _reader })
    }
}

/// Reader ownership retained by a branch after its paths have been opened.
pub struct SnapshotPin {
    _reader: Connection,
}

fn read_snapshot(path: &Path) -> Result<WalSnapshot, SnapshotError> {
    let bytes = read_observation(&File::open(path)?)?;
    let WalParse { snapshot, .. } = WalParser::parse(&bytes)?;
    snapshot.ok_or(SnapshotError::NoCommittedWalSnapshot)
}

#[derive(Debug)]
pub enum SnapshotError {
    Io(std::io::Error),
    Sqlite(rusqlite::Error),
    Wal(WalError),
    NoCommittedWalSnapshot,
    Unstable,
}

impl fmt::Display for SnapshotError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "snapshot file: {error}"),
            Self::Sqlite(error) => write!(formatter, "snapshot pin: {error}"),
            Self::Wal(error) => write!(formatter, "snapshot WAL: {error}"),
            Self::NoCommittedWalSnapshot => {
                formatter.write_str("WAL contains no complete committed snapshot")
            }
            Self::Unstable => formatter.write_str("database changed while pinning its snapshot"),
        }
    }
}

impl std::error::Error for SnapshotError {}

impl From<std::io::Error> for SnapshotError {
    fn from(error: std::io::Error) -> Self {
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
    use homebase_core::tag::DeviceSeq;

    use super::*;

    #[test]
    fn descriptor_separates_sqlite_submit_and_authority_coordinates() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("snapshot.sqlite");
        let wal_path = directory.path().join("snapshot.sqlite-wal");
        let writer = Connection::open(&path).unwrap();
        writer.pragma_update(None, "journal_mode", "WAL").unwrap();
        writer.pragma_update(None, "wal_autocheckpoint", 0).unwrap();
        writer.execute_batch("CREATE TABLE records(id)").unwrap();
        let cursors = OplogCursors {
            head: DeviceSeq(3),
            neck: DeviceSeq(5),
            tail: DeviceSeq(8),
        };

        let snapshot = PinnedSnapshot::capture(
            &path,
            &wal_path,
            LocalGeneration(41),
            AdmissionSeq(17),
            cursors,
        )
        .unwrap();
        let descriptor = snapshot.descriptor();
        assert_eq!(descriptor.local_generation, LocalGeneration(41));
        assert_eq!(descriptor.authority_applied_through, AdmissionSeq(17));
        assert_eq!(descriptor.submit_cursors, cursors);
        assert!(descriptor.image.max_frame() > 0);
        assert_ne!(descriptor.image.epoch().salts(), [0; 2]);
    }
}
