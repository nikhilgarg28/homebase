//! Durable canonical write history used for local optimistic validation.

use std::collections::BTreeSet;
use std::fmt;

use homebase_core::key::Key;
use homebase_core::range::Range;
use homebase_core::reader::Reader;
use homebase_core::tag::{DeviceSeq, Mutation};
use homebase_core::writer::Writer;
use rusqlite::{Connection, OptionalExtension, params};
use uuid::{Uuid, Variant, Version};

use super::snapshot::CommitSeq;
use crate::{Error, Result};

const COMMIT_LOG_TABLE: &str = "__multilite__commits";
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

/// One accepted proposal ready to be published in the canonical commit log.
pub struct PreparedRecord {
    pub proposal_id: [u8; 16],
    pub proposal_hash: [u8; 32],
    pub submitted: Option<DeviceSeq>,
    pub writes: Vec<WriteRegion>,
}

/// Durable identity retained for a recently committed proposal.
pub struct StoredReceipt {
    pub commit_seq: CommitSeq,
    pub proposal_hash: [u8; 32],
    pub submitted: Option<DeviceSeq>,
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

/// Create the canonical commit-log tables as part of database bootstrap.
pub fn initialize(connection: &Connection) -> Result<()> {
    connection.execute_batch(&format!(
        "CREATE TABLE {COMMIT_LOG_TABLE} (
            proposal_id BLOB NOT NULL CHECK(length(proposal_id) = 16),
            commit_seq BLOB NOT NULL CHECK(length(commit_seq) = 8),
            proposal_hash BLOB NOT NULL CHECK(length(proposal_hash) = 32),
            device_seq BLOB CHECK(device_seq IS NULL OR length(device_seq) = 8),
            writes BLOB NOT NULL,
            PRIMARY KEY (commit_seq, proposal_id),
            UNIQUE (proposal_id)
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
    let history = table_initialized(connection, COMMIT_LOG_TABLE)?;
    let commit_state = table_initialized(connection, COMMIT_STATE_TABLE)?;
    match (history, commit_state) {
        (false, false) => Ok(false),
        (true, true) => Ok(true),
        _ => Err(Error::InvalidDatabase(
            "canonical commit-log tables are only partially initialized",
        )),
    }
}

/// Validate table shape, the current sequence, and every retained commit row.
pub fn validate(connection: &Connection) -> Result<()> {
    if !is_initialized(connection)? {
        return Err(Error::InvalidDatabase("canonical commit log is missing"));
    }
    validate_columns(
        connection,
        COMMIT_LOG_TABLE,
        &[
            ("proposal_id", "BLOB", true, 2),
            ("commit_seq", "BLOB", true, 1),
            ("proposal_hash", "BLOB", true, 0),
            ("device_seq", "BLOB", false, 0),
            ("writes", "BLOB", true, 0),
        ],
        "canonical commit log schema is invalid",
    )?;
    validate_without_rowid(
        connection,
        COMMIT_LOG_TABLE,
        "canonical commit log must use WITHOUT ROWID",
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
        .iter()
        .any(|record| record.commit_seq > current)
    {
        return Err(Error::InvalidDatabase(
            "canonical commit log is newer than canonical SQLite",
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

/// Advance canonical state and retain one row per accepted proposal.
pub fn record_group(connection: &Connection, records: Vec<PreparedRecord>) -> Result<CommitSeq> {
    connection.execute_batch("SAVEPOINT __multilite__commit_history")?;
    let result = record_group_inner(connection, records);
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

fn record_group_inner(connection: &Connection, records: Vec<PreparedRecord>) -> Result<CommitSeq> {
    if records.is_empty() {
        return Err(Error::CaptureInvariant(
            "cannot record an empty canonical commit group",
        ));
    }
    let mut proposal_ids = BTreeSet::new();
    for record in &records {
        if !proposal_ids.insert(record.proposal_id) {
            return Err(Error::InvalidCommitProposal(
                "commit group contains a duplicate proposal id".into(),
            ));
        }
        if !valid_uuid(record.proposal_id) {
            return Err(Error::InvalidCommitProposal(
                "commit proposal id is not a UUID v4".into(),
            ));
        }
        if !canonical_writes(&record.writes) {
            return Err(Error::CaptureInvariant(
                "canonical commit writes are not sorted and unique",
            ));
        }
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
    for record in records {
        let encoded = encode_writes(&record.writes)?;
        connection.execute(
            &format!(
                "INSERT INTO {COMMIT_LOG_TABLE} (
                    proposal_id, commit_seq, proposal_hash, device_seq, writes
                 ) VALUES (?1, ?2, ?3, ?4, ?5)"
            ),
            params![
                record.proposal_id.as_slice(),
                next.0.to_be_bytes().as_slice(),
                record.proposal_hash.as_slice(),
                record
                    .submitted
                    .map(|sequence| sequence.0.to_be_bytes().to_vec()),
                encoded,
            ],
        )?;
    }
    Ok(next)
}

/// Load retained canonical writes strictly newer than `commit_seq`.
pub fn history_after(connection: &Connection, commit_seq: CommitSeq) -> Result<Vec<CommitRecord>> {
    let mut statement = connection.prepare(&format!(
        "SELECT proposal_id, commit_seq, device_seq, writes FROM {COMMIT_LOG_TABLE}
         WHERE commit_seq > ?1 ORDER BY commit_seq, proposal_id"
    ))?;
    let rows = statement.query_map([commit_seq.0.to_be_bytes().as_slice()], |row| {
        Ok((
            row.get::<_, Vec<u8>>(0)?,
            row.get::<_, Vec<u8>>(1)?,
            row.get::<_, Option<Vec<u8>>>(2)?,
            row.get::<_, Vec<u8>>(3)?,
        ))
    })?;
    rows.map(|row| {
        let (proposal_id, commit_seq, submitted, encoded_writes) = row?;
        decode_proposal_id(&proposal_id)?;
        submitted.as_deref().map(decode_device_seq).transpose()?;
        Ok(CommitRecord {
            commit_seq: decode_commit_seq(&commit_seq, false)?,
            writes: decode_writes(&encoded_writes)
                .map_err(|_| Error::InvalidDatabase("canonical commit log is malformed"))?,
        })
    })
    .collect()
}

/// Find a retained receipt by its stable proposal identity.
pub fn committed(connection: &Connection, proposal_id: [u8; 16]) -> Result<Option<StoredReceipt>> {
    let row = connection
        .query_row(
            &format!(
                "SELECT commit_seq, proposal_hash, device_seq
                 FROM {COMMIT_LOG_TABLE} WHERE proposal_id = ?1"
            ),
            [proposal_id.as_slice()],
            |row| {
                Ok((
                    row.get::<_, Vec<u8>>(0)?,
                    row.get::<_, Vec<u8>>(1)?,
                    row.get::<_, Option<Vec<u8>>>(2)?,
                ))
            },
        )
        .optional()?;
    row.map(|(commit_seq, proposal_hash, submitted)| {
        Ok(StoredReceipt {
            commit_seq: decode_commit_seq(&commit_seq, false)?,
            proposal_hash: proposal_hash
                .try_into()
                .map_err(|_| Error::InvalidDatabase("canonical commit log is malformed"))?,
            submitted: submitted.as_deref().map(decode_device_seq).transpose()?,
        })
    })
    .transpose()
}

/// Remove commit rows no newer than the supplied safe frontier.
pub fn prune(connection: &Connection, through: CommitSeq) -> Result<usize> {
    Ok(connection.execute(
        &format!("DELETE FROM {COMMIT_LOG_TABLE} WHERE commit_seq <= ?1"),
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

fn decode_proposal_id(bytes: &[u8]) -> Result<[u8; 16]> {
    let bytes: [u8; 16] = bytes
        .try_into()
        .map_err(|_| Error::InvalidDatabase("canonical proposal id is malformed"))?;
    if !valid_uuid(bytes) {
        return Err(Error::InvalidDatabase("canonical proposal id is malformed"));
    }
    Ok(bytes)
}

fn valid_uuid(bytes: [u8; 16]) -> bool {
    let uuid = Uuid::from_bytes(bytes);
    uuid.get_version() == Some(Version::Random) && uuid.get_variant() == Variant::RFC4122
}

fn decode_device_seq(bytes: &[u8]) -> Result<DeviceSeq> {
    let bytes: [u8; 8] = bytes
        .try_into()
        .map_err(|_| Error::InvalidDatabase("canonical device sequence is malformed"))?;
    let sequence = u64::from_be_bytes(bytes);
    if sequence == 0 {
        return Err(Error::InvalidDatabase(
            "canonical device sequence is malformed",
        ));
    }
    Ok(DeviceSeq(sequence))
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
            "canonical commit-log namespace contains unexpected tables",
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

    fn proposal_id(byte: u8) -> [u8; 16] {
        let mut id = [byte; 16];
        id[6] = (id[6] & 0x0f) | 0x40;
        id[8] = (id[8] & 0x3f) | 0x80;
        id
    }

    fn record(connection: &Connection, writes: Vec<WriteRegion>) -> Result<CommitSeq> {
        let byte = u8::try_from(current(connection)?.0 + 1)
            .map_err(|_| Error::CaptureInvariant("test commit sequence is too large"))?;
        record_group(
            connection,
            vec![PreparedRecord {
                proposal_id: proposal_id(byte),
                proposal_hash: [byte; 32],
                submitted: None,
                writes,
            }],
        )
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
                &format!(
                    "INSERT INTO {COMMIT_LOG_TABLE}
                     VALUES (?1, ?2, ?3, NULL, x'01')"
                ),
                params![
                    proposal_id(1).as_slice(),
                    1_u64.to_be_bytes().as_slice(),
                    [1_u8; 32].as_slice(),
                ],
            )
            .unwrap();
        assert!(matches!(
            history_after(&connection, CommitSeq(0)),
            Err(Error::InvalidDatabase("canonical commit log is malformed"))
        ));
        let metadata = Connection::open_in_memory().unwrap();
        initialize(&metadata).unwrap();
        assert_eq!(record(&metadata, Vec::new()).unwrap(), CommitSeq(1));
        assert_eq!(current(&metadata).unwrap(), CommitSeq(1));
        assert_eq!(
            history_after(&metadata, CommitSeq(0)).unwrap(),
            [CommitRecord {
                commit_seq: CommitSeq(1),
                writes: Vec::new(),
            }]
        );
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
                 BEFORE INSERT ON {COMMIT_LOG_TABLE}
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
                COMMIT_LOG_TABLE,
                format!(
                    "CREATE TABLE {COMMIT_LOG_TABLE} (
                        proposal_id BLOB NOT NULL,
                        commit_seq BLOB NOT NULL,
                        proposal_hash BLOB NOT NULL,
                        device_seq BLOB,
                        writes BLOB NOT NULL,
                        PRIMARY KEY (commit_seq, proposal_id),
                        UNIQUE (proposal_id)
                    )"
                ),
                "canonical commit log must use WITHOUT ROWID",
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
    fn validation_rejects_invalid_id_sequence_and_future_rows() {
        for mutation in [
            "UPDATE __multilite__commits SET proposal_id = zeroblob(16)",
            "UPDATE __multilite__commits SET device_seq = zeroblob(8)",
            "UPDATE __multilite__commits SET commit_seq = x'0000000000000002'",
        ] {
            let connection = Connection::open_in_memory().unwrap();
            initialize(&connection).unwrap();
            record(&connection, Vec::new()).unwrap();
            connection.execute(mutation, ()).unwrap();
            assert!(validate(&connection).is_err());
        }
    }

    #[test]
    fn commit_log_namespace_rejects_lookalike_tables() {
        let connection = Connection::open_in_memory().unwrap();
        initialize(&connection).unwrap();
        connection
            .execute_batch("CREATE TABLE __multilite__commits_future(value BLOB NOT NULL)")
            .unwrap();
        assert!(matches!(
            is_initialized(&connection),
            Err(Error::InvalidDatabase(
                "canonical commit-log namespace contains unexpected tables"
            ))
        ));
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

    #[test]
    fn grouped_proposals_share_a_sequence_but_keep_distinct_receipts_and_writes() {
        let connection = Connection::open_in_memory().unwrap();
        initialize(&connection).unwrap();
        let first = proposal_id(1);
        let second = proposal_id(2);
        let commit_seq = record_group(
            &connection,
            vec![
                PreparedRecord {
                    proposal_id: first,
                    proposal_hash: [1; 32],
                    submitted: Some(DeviceSeq(7)),
                    writes: vec![WriteRegion::Point(key(&[b"rows", b"one"]))],
                },
                PreparedRecord {
                    proposal_id: second,
                    proposal_hash: [2; 32],
                    submitted: None,
                    writes: Vec::new(),
                },
            ],
        )
        .unwrap();

        assert_eq!(commit_seq, CommitSeq(1));
        assert_eq!(
            history_after(&connection, CommitSeq(0)).unwrap(),
            [
                CommitRecord {
                    commit_seq,
                    writes: vec![WriteRegion::Point(key(&[b"rows", b"one"]))],
                },
                CommitRecord {
                    commit_seq,
                    writes: Vec::new(),
                },
            ]
        );
        let first_receipt = committed(&connection, first).unwrap().unwrap();
        assert_eq!(first_receipt.commit_seq, commit_seq);
        assert_eq!(first_receipt.proposal_hash, [1; 32]);
        assert_eq!(first_receipt.submitted, Some(DeviceSeq(7)));
        assert!(committed(&connection, second).unwrap().is_some());
    }
}
