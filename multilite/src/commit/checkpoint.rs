//! WAL growth control owned by the canonical committer.

use std::fs;
use std::path::Path;

use rusqlite::Connection;

use crate::{Error, Result};

const DEFAULT_SOFT_BYTES: u64 = 64 * 1024 * 1024;
const DEFAULT_HARD_BYTES: u64 = 256 * 1024 * 1024;
const DEFAULT_RETRY_GROWTH: u64 = 16 * 1024 * 1024;

/// Stateful, rate-limited checkpoint policy for one canonical WAL.
pub(crate) struct CheckpointPolicy {
    soft_bytes: u64,
    hard_bytes: u64,
    retry_growth: u64,
    next_attempt_bytes: u64,
    hard_blocked: bool,
    maintenance_error: Option<String>,
}

impl Default for CheckpointPolicy {
    fn default() -> Self {
        Self::new(DEFAULT_SOFT_BYTES, DEFAULT_HARD_BYTES, DEFAULT_RETRY_GROWTH)
    }
}

impl CheckpointPolicy {
    fn new(soft_bytes: u64, hard_bytes: u64, retry_growth: u64) -> Self {
        debug_assert!(soft_bytes > 0);
        debug_assert!(soft_bytes <= hard_bytes);
        debug_assert!(retry_growth > 0);
        Self {
            soft_bytes,
            hard_bytes,
            retry_growth,
            next_attempt_bytes: soft_bytes,
            hard_blocked: false,
            maintenance_error: None,
        }
    }

    /// Observe post-commit pressure without changing the commit's disposition.
    pub(crate) fn after_commit(&mut self, connection: &Connection, wal_path: &Path) {
        let result = self.maintain(connection, wal_path, false);
        self.maintenance_error = result.err().map(|error| error.to_string());
    }

    /// Establish that issuing another pinned snapshot cannot grow an already
    /// over-limit WAL indefinitely.
    pub(crate) fn before_snapshot(
        &mut self,
        connection: &Connection,
        wal_path: &Path,
    ) -> Result<()> {
        if !self.hard_blocked && self.maintenance_error.is_none() {
            return Ok(());
        }
        self.maintain(connection, wal_path, true)?;
        self.maintenance_error = None;
        if self.hard_blocked {
            return Err(Error::Checkpoint(
                "WAL remains above its hard limit while an older snapshot is pinned".into(),
            ));
        }
        Ok(())
    }

    fn maintain(&mut self, connection: &Connection, wal_path: &Path, force: bool) -> Result<()> {
        let bytes = wal_bytes(wal_path)?;
        if bytes < self.soft_bytes {
            self.hard_blocked = false;
            self.next_attempt_bytes = self.soft_bytes;
            return Ok(());
        }
        if !force && bytes < self.next_attempt_bytes && bytes < self.hard_bytes {
            return Ok(());
        }

        let passive = checkpoint(connection, "PASSIVE")?;
        let complete = passive.log_frames == passive.checkpointed_frames;
        if bytes < self.hard_bytes {
            self.hard_blocked = false;
            self.next_attempt_bytes = bytes.saturating_add(self.retry_growth);
            return Ok(());
        }

        let truncated = complete && checkpoint(connection, "TRUNCATE")?.busy == 0;
        self.hard_blocked = !truncated;
        self.next_attempt_bytes = if truncated {
            self.soft_bytes
        } else {
            bytes.saturating_add(self.retry_growth)
        };
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct CheckpointReport {
    busy: u32,
    log_frames: u32,
    checkpointed_frames: u32,
}

fn checkpoint(connection: &Connection, mode: &str) -> Result<CheckpointReport> {
    connection
        .query_row(&format!("PRAGMA wal_checkpoint({mode})"), (), |row| {
            Ok(CheckpointReport {
                busy: row.get(0)?,
                log_frames: row.get(1)?,
                checkpointed_frames: row.get(2)?,
            })
        })
        .map_err(Error::from)
}

fn wal_bytes(path: &Path) -> Result<u64> {
    match fs::metadata(path) {
        Ok(metadata) => Ok(metadata.len()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(0),
        Err(error) => Err(Error::Checkpoint(format!(
            "could not inspect WAL pressure: {error}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn open_wal(path: &Path) -> Connection {
        let connection = Connection::open(path).unwrap();
        connection
            .pragma_update(None, "journal_mode", "WAL")
            .unwrap();
        connection
            .pragma_update(None, "wal_autocheckpoint", 0)
            .unwrap();
        connection
            .execute_batch("CREATE TABLE records (id INTEGER PRIMARY KEY, value BLOB)")
            .unwrap();
        connection
    }

    #[test]
    fn soft_checkpoints_are_rate_limited_by_wal_growth() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("soft.sqlite");
        let wal_path = directory.path().join("soft.sqlite-wal");
        let writer = open_wal(&path);
        let initial = wal_bytes(&wal_path).unwrap();
        let mut policy = CheckpointPolicy::new(1, u64::MAX, initial);

        policy.after_commit(&writer, &wal_path);
        let next_attempt = policy.next_attempt_bytes;
        assert!(next_attempt > initial);
        policy.after_commit(&writer, &wal_path);
        assert_eq!(policy.next_attempt_bytes, next_attempt);
        assert!(!policy.hard_blocked);
    }

    #[test]
    fn hard_pressure_blocks_new_snapshots_until_the_oldest_reader_drains() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("hard.sqlite");
        let wal_path = directory.path().join("hard.sqlite-wal");
        let writer = open_wal(&path);
        let reader = Connection::open(&path).unwrap();
        reader.execute_batch("BEGIN").unwrap();
        reader
            .query_row("SELECT count(*) FROM records", (), |_| Ok(()))
            .unwrap();
        writer
            .execute("INSERT INTO records VALUES (1, ?1)", [vec![7_u8; 16_000]])
            .unwrap();

        let mut policy = CheckpointPolicy::new(1, 1, 1);
        policy.after_commit(&writer, &wal_path);
        assert!(policy.hard_blocked);
        assert!(matches!(
            policy.before_snapshot(&writer, &wal_path),
            Err(Error::Checkpoint(_))
        ));

        reader.execute_batch("ROLLBACK").unwrap();
        policy.before_snapshot(&writer, &wal_path).unwrap();
        assert!(!policy.hard_blocked);
        assert_eq!(wal_bytes(&wal_path).unwrap(), 0);
    }

    #[test]
    fn a_post_commit_checkpoint_error_is_reported_only_at_the_next_snapshot() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("error.sqlite");
        let wal_path = directory.path().join("missing").join("database-wal");
        let writer = open_wal(&path);
        let mut policy = CheckpointPolicy::new(1, 1, 1);

        policy.after_commit(&writer, &wal_path);
        assert!(policy.maintenance_error.is_none());

        let unreadable_wal = directory.path().join("wal-loop");
        std::os::unix::fs::symlink("wal-loop", &unreadable_wal).unwrap();
        policy.after_commit(&writer, &unreadable_wal);
        assert!(policy.maintenance_error.is_some());
        assert!(matches!(
            policy.before_snapshot(&writer, &unreadable_wal),
            Err(Error::Checkpoint(_))
        ));
    }
}
