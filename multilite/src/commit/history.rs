//! Durable canonical write history used for local optimistic validation.

use std::collections::BTreeSet;
use std::fmt;

use homebase_core::key::Key;
use homebase_core::range::Range;
use homebase_core::reader::Reader;
use homebase_core::tag::Mutation;
use homebase_core::writer::Writer;
use rusqlite::Connection;

use super::snapshot::CommitSeq;
use crate::{Error, Result};

const HISTORY_TABLE: &str = "__multilite__history";
const COMMIT_STATE_TABLE: &str = "__multilite__commit_state";
const WRITE_SET_VERSION: u8 = 2;
const LEGACY_WRITE_SET_VERSION: u8 = 1;
const POINT_WRITE: u8 = 1;
const RANGE_WRITE: u8 = 2;

/// One exact point write or range deletion in canonical logical state.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum WriteRegion {
    Point(Key),
    Range(Range),
}

impl WriteRegion {
    /// True when this write can invalidate an assertion over `prefix`.
    pub fn overlaps_prefix(&self, prefix: &Key) -> bool {
        match self {
            Self::Point(key) => key.starts_with(prefix),
            Self::Range(Range::Full) => true,
            Self::Range(Range::Prefix(written)) => {
                written.starts_with(prefix) || prefix.starts_with(written)
            }
        }
    }
}

/// Logical writes made by one canonical SQLite visibility transition.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommitRecord {
    pub commit_seq: CommitSeq,
    pub writes: Vec<WriteRegion>,
}

/// Build the canonical logical writes represented by lowered mutations.
pub fn writes_from_mutations<'a>(
    mutations: impl IntoIterator<Item = &'a Mutation>,
) -> Vec<WriteRegion> {
    let mut writes = BTreeSet::new();
    for mutation in mutations {
        match mutation {
            Mutation::Set { key, .. } | Mutation::Delete { key } => {
                writes.insert(WriteRegion::Point(key.clone()));
            }
            Mutation::DeleteRange { range } => {
                writes.insert(WriteRegion::Range(range.clone()));
            }
        }
    }
    writes.into_iter().collect()
}

/// Create the canonical-history tables as part of database bootstrap.
pub fn initialize(connection: &Connection) -> Result<()> {
    connection.execute_batch(&format!(
        "CREATE TABLE {HISTORY_TABLE} (
            commit_seq BLOB PRIMARY KEY NOT NULL CHECK(length(commit_seq) = 8),
            writes BLOB NOT NULL
        ) WITHOUT ROWID;
        CREATE TABLE {COMMIT_STATE_TABLE} (
            singleton INTEGER PRIMARY KEY NOT NULL CHECK(singleton = 1),
            commit_seq BLOB NOT NULL CHECK(length(commit_seq) = 8)
        ) WITHOUT ROWID;
        INSERT INTO {COMMIT_STATE_TABLE} VALUES (1, x'0000000000000000')"
    ))?;
    Ok(())
}

/// Check whether the complete canonical-history namespace is present.
pub fn is_initialized(connection: &Connection) -> Result<bool> {
    let history = table_initialized(connection, HISTORY_TABLE)?;
    let commit_state = table_initialized(connection, COMMIT_STATE_TABLE)?;
    match (history, commit_state) {
        (false, false) => Ok(false),
        (true, true) => Ok(true),
        _ => Err(Error::InvalidDatabase(
            "canonical history tables are only partially initialized",
        )),
    }
}

/// Validate table shape, the current sequence, and every retained write set.
pub fn validate(connection: &Connection) -> Result<()> {
    if !is_initialized(connection)? {
        return Err(Error::InvalidDatabase(
            "canonical commit history is missing",
        ));
    }
    validate_columns(
        connection,
        HISTORY_TABLE,
        &[("commit_seq", "BLOB", true, 1), ("writes", "BLOB", true, 0)],
        "canonical history table schema is invalid",
    )?;
    validate_without_rowid(
        connection,
        HISTORY_TABLE,
        "canonical history table must use WITHOUT ROWID",
    )?;
    validate_columns(
        connection,
        COMMIT_STATE_TABLE,
        &[
            ("singleton", "INTEGER", true, 1),
            ("commit_seq", "BLOB", true, 0),
        ],
        "commit state table schema is invalid",
    )?;
    validate_without_rowid(
        connection,
        COMMIT_STATE_TABLE,
        "commit state table must use WITHOUT ROWID",
    )?;

    let row = connection.query_row(
        &format!("SELECT singleton, commit_seq FROM {COMMIT_STATE_TABLE}"),
        (),
        |row| Ok((row.get::<_, i64>(0)?, row.get::<_, Vec<u8>>(1)?)),
    )?;
    if row.0 != 1 {
        return Err(Error::InvalidDatabase("commit state row is invalid"));
    }
    let current = decode_commit_seq(&row.1, true)?;
    if history_after(connection, CommitSeq(0))?
        .last()
        .is_some_and(|record| record.commit_seq > current)
    {
        return Err(Error::InvalidDatabase(
            "canonical history is newer than canonical SQLite",
        ));
    }
    Ok(())
}

/// Last canonical commit sequence, or zero before canonical state first changes.
pub fn current(connection: &Connection) -> Result<CommitSeq> {
    let encoded = connection.query_row(
        &format!("SELECT commit_seq FROM {COMMIT_STATE_TABLE} WHERE singleton = 1"),
        (),
        |row| row.get::<_, Vec<u8>>(0),
    )?;
    decode_commit_seq(&encoded, true)
}

/// Advance canonical state and retain its writes in the caller's transaction.
pub fn record(connection: &Connection, writes: Vec<WriteRegion>) -> Result<CommitSeq> {
    connection.execute_batch("SAVEPOINT __multilite__commit_history")?;
    let result = record_inner(connection, writes);
    match result {
        Ok(commit_seq) => {
            connection.execute_batch("RELEASE __multilite__commit_history")?;
            Ok(commit_seq)
        }
        Err(error) => {
            let rollback = connection.execute_batch(
                "ROLLBACK TO __multilite__commit_history;
                 RELEASE __multilite__commit_history",
            );
            match rollback {
                Ok(()) => Err(error),
                Err(rollback) => Err(rollback.into()),
            }
        }
    }
}

fn record_inner(connection: &Connection, writes: Vec<WriteRegion>) -> Result<CommitSeq> {
    if !canonical_writes(&writes) {
        return Err(Error::CaptureInvariant(
            "canonical commit writes are not sorted and unique",
        ));
    }
    let current = current(connection)?;
    let next = CommitSeq(
        current
            .0
            .checked_add(1)
            .ok_or_else(|| Error::CommitConflict("local commit sequence is exhausted".into()))?,
    );
    connection.execute(
        &format!("UPDATE {COMMIT_STATE_TABLE} SET commit_seq = ?1 WHERE singleton = 1"),
        [next.0.to_be_bytes().as_slice()],
    )?;
    if !writes.is_empty() {
        let encoded = encode_writes(&writes)?;
        connection.execute(
            &format!("INSERT INTO {HISTORY_TABLE} (commit_seq, writes) VALUES (?1, ?2)"),
            rusqlite::params![next.0.to_be_bytes().as_slice(), encoded],
        )?;
    }
    Ok(next)
}

/// Load retained canonical writes strictly newer than `commit_seq`.
pub fn history_after(connection: &Connection, commit_seq: CommitSeq) -> Result<Vec<CommitRecord>> {
    let mut statement = connection.prepare(&format!(
        "SELECT commit_seq, writes FROM {HISTORY_TABLE}
         WHERE commit_seq > ?1 ORDER BY commit_seq"
    ))?;
    let rows = statement.query_map([commit_seq.0.to_be_bytes().as_slice()], |row| {
        Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, Vec<u8>>(1)?))
    })?;
    rows.map(|row| {
        let (commit_seq, encoded_keys) = row?;
        Ok(CommitRecord {
            commit_seq: decode_commit_seq(&commit_seq, false)?,
            writes: decode_writes(&encoded_keys)
                .map_err(|_| Error::InvalidDatabase("canonical write record is malformed"))?,
        })
    })
    .collect()
}

/// Remove OCC evidence no newer than the oldest live writable snapshot.
pub fn prune(connection: &Connection, through: CommitSeq) -> Result<usize> {
    Ok(connection.execute(
        &format!("DELETE FROM {HISTORY_TABLE} WHERE commit_seq <= ?1"),
        [through.0.to_be_bytes().as_slice()],
    )?)
}

fn encode_writes(writes: &[WriteRegion]) -> Result<Vec<u8>> {
    let mut writer = Writer::new();
    writer.u8(WRITE_SET_VERSION);
    writer.u32(
        u32::try_from(writes.len())
            .map_err(|_| Error::CaptureInvariant("canonical write set is too large"))?,
    );
    for write in writes {
        let (kind, encoded) = match write {
            WriteRegion::Point(key) => (POINT_WRITE, key.encode()),
            WriteRegion::Range(range) => (RANGE_WRITE, range.encode()),
        };
        writer.u8(kind);
        writer.u32(
            u32::try_from(encoded.len())
                .map_err(|_| Error::CaptureInvariant("canonical write region is too large"))?,
        );
        writer.bytes(&encoded);
    }
    Ok(writer.finish())
}

fn decode_writes(frame: &[u8]) -> std::result::Result<Vec<WriteRegion>, HistoryCodecError> {
    let mut reader = Reader::new(frame);
    let version = reader.u8().ok_or(HistoryCodecError::Truncated)?;
    let tagged = match version {
        WRITE_SET_VERSION => true,
        LEGACY_WRITE_SET_VERSION => false,
        _ => return Err(HistoryCodecError::UnknownVersion),
    };
    let count = reader.u32().ok_or(HistoryCodecError::Truncated)?;
    if count == 0 {
        return Err(HistoryCodecError::Empty);
    }
    let mut writes =
        Vec::with_capacity(usize::try_from(count).map_err(|_| HistoryCodecError::InvalidLength)?);
    for _ in 0..count {
        let kind = tagged
            .then(|| reader.u8().ok_or(HistoryCodecError::Truncated))
            .transpose()?
            .unwrap_or(POINT_WRITE);
        let length = reader.u32().ok_or(HistoryCodecError::Truncated)?;
        let length = usize::try_from(length).map_err(|_| HistoryCodecError::InvalidLength)?;
        let encoded = reader.take(length).ok_or(HistoryCodecError::Truncated)?;
        let write = match kind {
            POINT_WRITE => {
                WriteRegion::Point(Key::decode(encoded).map_err(|_| HistoryCodecError::InvalidKey)?)
            }
            RANGE_WRITE => {
                WriteRegion::Range(Range::decode(encoded).ok_or(HistoryCodecError::InvalidRange)?)
            }
            _ => return Err(HistoryCodecError::UnknownKind),
        };
        writes.push(write);
    }
    if !canonical_writes(&writes) {
        return Err(HistoryCodecError::NonCanonical);
    }
    if reader.end().is_none() {
        return Err(HistoryCodecError::TrailingBytes);
    }
    Ok(writes)
}

fn canonical_writes(writes: &[WriteRegion]) -> bool {
    writes.windows(2).all(|pair| pair[0] < pair[1])
}

pub fn decode_commit_seq(bytes: &[u8], allow_zero: bool) -> Result<CommitSeq> {
    let bytes: [u8; 8] = bytes
        .try_into()
        .map_err(|_| Error::InvalidDatabase("commit sequence is malformed"))?;
    let commit_seq = u64::from_be_bytes(bytes);
    if commit_seq == 0 && !allow_zero {
        return Err(Error::InvalidDatabase(
            "committed sequence must be greater than zero",
        ));
    }
    Ok(CommitSeq(commit_seq))
}

fn table_initialized(connection: &Connection, table: &'static str) -> Result<bool> {
    let mut statement = connection.prepare(
        "SELECT name FROM sqlite_schema
         WHERE type = 'table'
           AND substr(name, 1, length(?1)) = ?1 COLLATE NOCASE
         ORDER BY name",
    )?;
    let tables = statement
        .query_map([table], |row| row.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    match tables.as_slice() {
        [] => Ok(false),
        [found] if found == table => Ok(true),
        _ => Err(Error::InvalidDatabase(
            "canonical history namespace contains unexpected tables",
        )),
    }
}

fn validate_columns(
    connection: &Connection,
    table: &str,
    expected: &[(&str, &str, bool, u32)],
    message: &'static str,
) -> Result<()> {
    let mut statement = connection.prepare(&format!("PRAGMA table_info({table})"))?;
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
    let expected = expected
        .iter()
        .map(|(name, kind, not_null, primary)| {
            ((*name).to_owned(), (*kind).to_owned(), *not_null, *primary)
        })
        .collect::<Vec<_>>();
    if columns != expected {
        return Err(Error::InvalidDatabase(message));
    }
    Ok(())
}

fn validate_without_rowid(
    connection: &Connection,
    table: &str,
    message: &'static str,
) -> Result<()> {
    let schema_sql: String = connection.query_row(
        "SELECT sql FROM sqlite_schema WHERE type = 'table' AND name = ?1",
        [table],
        |row| row.get(0),
    )?;
    if !schema_sql.to_ascii_uppercase().contains("WITHOUT ROWID") {
        return Err(Error::InvalidDatabase(message));
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum HistoryCodecError {
    UnknownVersion,
    Truncated,
    InvalidLength,
    InvalidKey,
    InvalidRange,
    UnknownKind,
    Empty,
    NonCanonical,
    TrailingBytes,
}

impl fmt::Display for HistoryCodecError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{self:?}")
    }
}

#[cfg(test)]
mod tests {
    use homebase_core::tag::Mutation;

    use super::*;

    fn key(parts: &[&[u8]]) -> Key {
        Key::from_bytes(parts.iter().copied()).unwrap()
    }

    #[test]
    fn records_roundtrip_as_sorted_write_regions() {
        let connection = Connection::open_in_memory().unwrap();
        initialize(&connection).unwrap();
        let writes = writes_from_mutations([
            &Mutation::Set {
                key: key(&[b"tables", b"one", b"rows", b"7"]),
                value: vec![],
            },
            &Mutation::Set {
                key: key(&[b"tables", b"one"]),
                value: vec![],
            },
            &Mutation::Set {
                key: key(&[b"tables", b"two"]),
                value: vec![],
            },
        ]);

        assert_eq!(record(&connection, writes).unwrap(), CommitSeq(1));
        assert_eq!(current(&connection).unwrap(), CommitSeq(1));
        assert_eq!(
            history_after(&connection, CommitSeq(0)).unwrap(),
            vec![CommitRecord {
                commit_seq: CommitSeq(1),
                writes: vec![
                    WriteRegion::Point(key(&[b"tables", b"one"])),
                    WriteRegion::Point(key(&[b"tables", b"one", b"rows", b"7"])),
                    WriteRegion::Point(key(&[b"tables", b"two"])),
                ],
            }]
        );
        validate(&connection).unwrap();
    }

    #[test]
    fn malformed_records_are_rejected_and_metadata_only_commits_advance() {
        let connection = Connection::open_in_memory().unwrap();
        initialize(&connection).unwrap();
        connection
            .execute(
                &format!("UPDATE {COMMIT_STATE_TABLE} SET commit_seq = ?1"),
                [1_u64.to_be_bytes().as_slice()],
            )
            .unwrap();
        connection
            .execute(
                &format!("INSERT INTO {HISTORY_TABLE} VALUES (?1, x'01')"),
                [1_u64.to_be_bytes().as_slice()],
            )
            .unwrap();
        assert!(matches!(
            history_after(&connection, CommitSeq(0)),
            Err(Error::InvalidDatabase(
                "canonical write record is malformed"
            ))
        ));
        let metadata = Connection::open_in_memory().unwrap();
        initialize(&metadata).unwrap();
        assert_eq!(record(&metadata, Vec::new()).unwrap(), CommitSeq(1));
        assert_eq!(current(&metadata).unwrap(), CommitSeq(1));
        assert!(history_after(&metadata, CommitSeq(0)).unwrap().is_empty());
    }

    #[test]
    fn range_mutations_roundtrip_and_overlap_prefixes_bidirectionally() {
        let connection = Connection::open_in_memory().unwrap();
        initialize(&connection).unwrap();
        let table = key(&[b"tables", b"one"]);
        let rows = key(&[b"tables", b"one", b"rows"]);
        let writes = writes_from_mutations([
            &Mutation::DeleteRange {
                range: Range::Prefix(rows.clone()),
            },
            &Mutation::Set {
                key: key(&[b"tables", b"two"]),
                value: Vec::new(),
            },
        ]);

        record(&connection, writes.clone()).unwrap();
        assert_eq!(
            history_after(&connection, CommitSeq(0)).unwrap()[0].writes,
            writes
        );
        let range = WriteRegion::Range(Range::Prefix(rows.clone()));
        assert!(range.overlaps_prefix(&table));
        assert!(range.overlaps_prefix(&rows));
        assert!(range.overlaps_prefix(&key(&[b"tables", b"one", b"rows", b"7"])));
        assert!(!range.overlaps_prefix(&key(&[b"tables", b"two"])));
    }

    #[test]
    fn record_failure_does_not_advance_canonical_sequence() {
        let connection = Connection::open_in_memory().unwrap();
        initialize(&connection).unwrap();
        connection
            .execute_batch(&format!(
                "CREATE TRIGGER reject_history
                 BEFORE INSERT ON {HISTORY_TABLE}
                 BEGIN SELECT RAISE(ABORT, 'injected'); END"
            ))
            .unwrap();

        assert!(
            record(
                &connection,
                vec![WriteRegion::Point(key(&[b"tables", b"one"]))]
            )
            .is_err()
        );
        assert_eq!(current(&connection).unwrap(), CommitSeq(0));
        assert!(history_after(&connection, CommitSeq(0)).unwrap().is_empty());
    }

    #[test]
    fn validation_requires_without_rowid_for_history_tables() {
        for (table, replacement, expected) in [
            (
                HISTORY_TABLE,
                format!(
                    "CREATE TABLE {HISTORY_TABLE} (
                        commit_seq BLOB PRIMARY KEY NOT NULL,
                        writes BLOB NOT NULL
                    )"
                ),
                "canonical history table must use WITHOUT ROWID",
            ),
            (
                COMMIT_STATE_TABLE,
                format!(
                    "CREATE TABLE {COMMIT_STATE_TABLE} (
                        singleton INTEGER PRIMARY KEY NOT NULL,
                        commit_seq BLOB NOT NULL
                    );
                    INSERT INTO {COMMIT_STATE_TABLE} VALUES (1, x'0000000000000000')"
                ),
                "commit state table must use WITHOUT ROWID",
            ),
        ] {
            let connection = Connection::open_in_memory().unwrap();
            initialize(&connection).unwrap();
            connection
                .execute_batch(&format!("DROP TABLE {table}; {replacement}"))
                .unwrap();
            assert!(matches!(
                validate(&connection),
                Err(Error::InvalidDatabase(message)) if message == expected
            ));
        }
    }

    #[test]
    fn pruning_retains_records_newer_than_the_oldest_snapshot() {
        let connection = Connection::open_in_memory().unwrap();
        initialize(&connection).unwrap();
        for value in [1_u8, 2, 3] {
            record(
                &connection,
                vec![WriteRegion::Point(key(&[b"rows", &[value]]))],
            )
            .unwrap();
        }

        assert_eq!(prune(&connection, CommitSeq(1)).unwrap(), 1);
        assert_eq!(
            history_after(&connection, CommitSeq(0))
                .unwrap()
                .into_iter()
                .map(|record| record.commit_seq)
                .collect::<Vec<_>>(),
            [CommitSeq(2), CommitSeq(3)]
        );
    }
}
