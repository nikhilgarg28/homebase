//! Net SQLite changes captured from a private branch and replayed canonically.

#![cfg_attr(
    not(test),
    allow(
        dead_code,
        reason = "wired into branch commit proposals in the next batch"
    )
)]

use std::collections::BTreeMap;
use std::fmt;
use std::io::Cursor;

use fallible_streaming_iterator::FallibleStreamingIterator as _;
use rusqlite::config::DbConfig;
use rusqlite::hooks::Action;
use rusqlite::session::{ChangesetIter, Session};
use rusqlite::{Connection, OptionalExtension as _, params_from_iter};
use sha2::{Digest, Sha256};

use super::row::StoredValue;

/// Hash of the complete application schema against which a changeset ran.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SchemaFingerprint([u8; 32]);

/// A transaction's net changes over explicitly attached synchronized tables.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CapturedChangeset {
    schema: SchemaFingerprint,
    bytes: Vec<u8>,
}

impl CapturedChangeset {
    pub fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }

    pub fn schema(&self) -> SchemaFingerprint {
        self.schema
    }

    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// Replay the final logical changes without re-running triggers or FK actions.
    pub fn apply(&self, connection: &Connection) -> Result<(), ChangesetError> {
        if schema_fingerprint(connection)? != self.schema {
            return Err(ChangesetError::SchemaChanged);
        }
        let changes = decode_changeset(&self.bytes)?;
        apply_changes(connection, &changes)
    }

    #[cfg(test)]
    fn summary(&self) -> Result<ChangeSummary, ChangesetError> {
        let mut summary = ChangeSummary::default();
        for change in decode_changeset(&self.bytes)? {
            match change.kind {
                ChangeKind::Insert => summary.inserts += 1,
                ChangeKind::Update => summary.updates += 1,
                ChangeKind::Delete => summary.deletes += 1,
            }
            summary.indirect += usize::from(change.indirect);
        }
        Ok(summary)
    }
}

/// Active SQLite Session capture scoped to one branch transaction.
pub struct ChangesetCapture<'connection> {
    session: Session<'connection>,
    schema: SchemaFingerprint,
}

impl<'connection> ChangesetCapture<'connection> {
    pub fn start(
        connection: &'connection Connection,
        tables: &[&str],
    ) -> Result<Self, ChangesetError> {
        let schema = schema_fingerprint(connection)?;
        let mut session = Session::new(connection)?;
        for table in tables {
            let layout = table_layout(connection, table)?;
            if !layout.columns.iter().any(|column| column.primary_key > 0) {
                return Err(ChangesetError::TableWithoutPrimaryKey((*table).to_owned()));
            }
            session.attach(Some(*table))?;
        }
        Ok(Self { session, schema })
    }

    pub fn finish(mut self) -> Result<CapturedChangeset, ChangesetError> {
        let mut bytes = Vec::new();
        self.session.changeset_strm(&mut bytes)?;
        Ok(CapturedChangeset {
            schema: self.schema,
            bytes,
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ChangeKind {
    Insert,
    Update,
    Delete,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct NetChange {
    table: String,
    kind: ChangeKind,
    primary_key: Vec<bool>,
    old: Vec<Option<StoredValue>>,
    new: Vec<Option<StoredValue>>,
    indirect: bool,
}

#[derive(Clone, Debug)]
struct ColumnLayout {
    name: String,
    primary_key: u32,
    writable: bool,
}

#[derive(Clone, Debug)]
struct TableLayout {
    name: String,
    columns: Vec<ColumnLayout>,
}

#[derive(Clone, Debug)]
struct ReplayChange {
    table: TableLayout,
    old_key: Option<Vec<StoredValue>>,
    final_row: Option<Vec<StoredValue>>,
}

fn schema_fingerprint(connection: &Connection) -> Result<SchemaFingerprint, ChangesetError> {
    let mut statement = connection.prepare(
        "SELECT type, name, tbl_name, coalesce(sql, '')
         FROM main.sqlite_schema
         WHERE name NOT LIKE 'sqlite_%'
         ORDER BY type, name",
    )?;
    let rows = statement.query_map((), |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
            row.get::<_, String>(3)?,
        ))
    })?;
    let mut hash = Sha256::new();
    for row in rows {
        let (kind, name, table, sql) = row?;
        for field in [&kind, &name, &table, &sql] {
            let length = u64::try_from(field.len())
                .map_err(|_| ChangesetError::Malformed("schema field is too large"))?;
            hash.update(length.to_be_bytes());
            hash.update(field.as_bytes());
        }
    }
    Ok(SchemaFingerprint(hash.finalize().into()))
}

fn decode_changeset(bytes: &[u8]) -> Result<Vec<NetChange>, ChangesetError> {
    if bytes.is_empty() {
        return Ok(Vec::new());
    }
    let mut cursor = Cursor::new(bytes);
    let input: &mut dyn std::io::Read = &mut cursor;
    let mut iterator = ChangesetIter::start_strm(&input)?;
    let mut changes = Vec::new();
    while let Some(item) = iterator.next()? {
        let operation = item.op()?;
        let kind = match operation.code() {
            Action::SQLITE_INSERT => ChangeKind::Insert,
            Action::SQLITE_UPDATE => ChangeKind::Update,
            Action::SQLITE_DELETE => ChangeKind::Delete,
            _ => return Err(ChangesetError::Malformed("unknown changeset operation")),
        };
        let width = usize::try_from(operation.number_of_columns())
            .map_err(|_| ChangesetError::Malformed("negative changeset width"))?;
        let primary_key = item
            .pk()?
            .iter()
            .map(|value| *value != 0)
            .collect::<Vec<_>>();
        if primary_key.len() != width || !primary_key.iter().any(|part| *part) {
            return Err(ChangesetError::Malformed("invalid changeset primary key"));
        }
        let old = match kind {
            ChangeKind::Insert => vec![None; width],
            ChangeKind::Update | ChangeKind::Delete => (0..width)
                .map(|index| optional_value(item.old_value(index), index))
                .collect::<Result<Vec<_>, _>>()?,
        };
        let new = match kind {
            ChangeKind::Delete => vec![None; width],
            ChangeKind::Insert | ChangeKind::Update => (0..width)
                .map(|index| optional_value(item.new_value(index), index))
                .collect::<Result<Vec<_>, _>>()?,
        };
        changes.push(NetChange {
            table: operation.table_name().to_owned(),
            kind,
            primary_key,
            old,
            new,
            indirect: operation.indirect(),
        });
    }
    Ok(changes)
}

fn optional_value(
    value: rusqlite::Result<rusqlite::types::ValueRef<'_>>,
    index: usize,
) -> Result<Option<StoredValue>, ChangesetError> {
    match value {
        Ok(value) => Ok(Some(StoredValue::capture(value))),
        Err(rusqlite::Error::InvalidColumnIndex(actual)) if actual == index => Ok(None),
        Err(error) => Err(error.into()),
    }
}

fn apply_changes(connection: &Connection, changes: &[NetChange]) -> Result<(), ChangesetError> {
    if changes.is_empty() {
        return Ok(());
    }
    connection.execute_batch("SAVEPOINT __multilite_changeset_apply")?;
    let triggers = connection.db_config(DbConfig::SQLITE_DBCONFIG_ENABLE_TRIGGER)?;
    let foreign_keys = connection.db_config(DbConfig::SQLITE_DBCONFIG_ENABLE_FKEY)?;
    connection.set_db_config(DbConfig::SQLITE_DBCONFIG_ENABLE_TRIGGER, false)?;
    connection.set_db_config(DbConfig::SQLITE_DBCONFIG_ENABLE_FKEY, false)?;

    let result = (|| {
        let replay = prepare_replay(connection, changes)?;
        for change in &replay {
            if let Some(key) = &change.old_key {
                delete_old_row(connection, &change.table, key)?;
            }
        }
        for change in &replay {
            if let Some(row) = &change.final_row {
                insert_final_row(connection, &change.table, row)?;
            }
        }
        let foreign_key_violation = connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM pragma_foreign_key_check)",
            (),
            |row| row.get::<_, bool>(0),
        )?;
        if foreign_key_violation {
            return Err(ChangesetError::ForeignKeyViolation);
        }
        Ok(())
    })();

    if result.is_err() {
        let _ = connection.execute_batch(
            "ROLLBACK TO __multilite_changeset_apply;
             RELEASE __multilite_changeset_apply",
        );
    }
    let restore_triggers = connection
        .set_db_config(DbConfig::SQLITE_DBCONFIG_ENABLE_TRIGGER, triggers)
        .map(|_| ());
    let restore_foreign_keys = connection
        .set_db_config(DbConfig::SQLITE_DBCONFIG_ENABLE_FKEY, foreign_keys)
        .map(|_| ());
    result?;
    restore_triggers?;
    restore_foreign_keys?;
    connection.execute_batch("RELEASE __multilite_changeset_apply")?;
    Ok(())
}

fn prepare_replay(
    connection: &Connection,
    changes: &[NetChange],
) -> Result<Vec<ReplayChange>, ChangesetError> {
    let mut layouts: BTreeMap<String, TableLayout> = BTreeMap::new();
    let mut replay = Vec::with_capacity(changes.len());
    for change in changes {
        let layout = match layouts.get(&change.table) {
            Some(layout) => layout.clone(),
            None => {
                let layout = table_layout(connection, &change.table)?;
                layouts.insert(change.table.clone(), layout.clone());
                layout
            }
        };
        if layout.columns.len() != change.primary_key.len()
            || layout
                .columns
                .iter()
                .map(|column| column.primary_key > 0)
                .ne(change.primary_key.iter().copied())
        {
            return Err(ChangesetError::Malformed(
                "changeset columns contradict the current table",
            ));
        }

        let old_key = match change.kind {
            ChangeKind::Insert => None,
            ChangeKind::Update | ChangeKind::Delete => Some(primary_values(change, &change.old)?),
        };
        let final_row = match change.kind {
            ChangeKind::Delete => {
                let current = load_current_row(connection, &layout, old_key.as_ref().unwrap())?;
                verify_old_values(change, &current)?;
                None
            }
            ChangeKind::Update => {
                let mut current = load_current_row(connection, &layout, old_key.as_ref().unwrap())?;
                verify_old_values(change, &current)?;
                for (slot, value) in current.iter_mut().zip(&change.new) {
                    if let Some(value) = value {
                        *slot = value.clone();
                    }
                }
                Some(current)
            }
            ChangeKind::Insert => Some(
                change
                    .new
                    .iter()
                    .enumerate()
                    .map(|(index, value)| {
                        value
                            .clone()
                            .or_else(|| {
                                (!layout.columns[index].writable).then_some(StoredValue::Null)
                            })
                            .ok_or(ChangesetError::Malformed(
                                "insert is missing a writable column value",
                            ))
                    })
                    .collect::<Result<Vec<_>, _>>()?,
            ),
        };
        replay.push(ReplayChange {
            table: layout,
            old_key,
            final_row,
        });
    }
    Ok(replay)
}

fn primary_values(
    change: &NetChange,
    values: &[Option<StoredValue>],
) -> Result<Vec<StoredValue>, ChangesetError> {
    change
        .primary_key
        .iter()
        .zip(values)
        .filter_map(|(primary, value)| primary.then_some(value))
        .map(|value| {
            value.clone().ok_or(ChangesetError::Malformed(
                "changeset is missing a primary-key value",
            ))
        })
        .collect()
}

fn load_current_row(
    connection: &Connection,
    table: &TableLayout,
    key: &[StoredValue],
) -> Result<Vec<StoredValue>, ChangesetError> {
    let columns = table
        .columns
        .iter()
        .map(|column| quote_identifier(&column.name))
        .collect::<Vec<_>>()
        .join(", ");
    let predicate = table
        .columns
        .iter()
        .filter(|column| column.primary_key > 0)
        .map(|column| format!("{} IS ?", quote_identifier(&column.name)))
        .collect::<Vec<_>>()
        .join(" AND ");
    let sql = format!(
        "SELECT {columns} FROM {} WHERE {predicate}",
        quote_identifier(&table.name)
    );
    connection
        .query_row(&sql, params_from_iter(key), |row| {
            (0..table.columns.len())
                .map(|index| row.get_ref(index).map(StoredValue::capture))
                .collect::<rusqlite::Result<Vec<_>>>()
        })
        .optional()?
        .ok_or_else(|| ChangesetError::Conflict(format!("row missing from {}", table.name)))
}

fn verify_old_values(change: &NetChange, current: &[StoredValue]) -> Result<(), ChangesetError> {
    if change
        .old
        .iter()
        .zip(current)
        .any(|(expected, actual)| expected.as_ref().is_some_and(|value| value != actual))
    {
        return Err(ChangesetError::Conflict(format!(
            "row in {} changed since the branch snapshot",
            change.table
        )));
    }
    Ok(())
}

fn delete_old_row(
    connection: &Connection,
    table: &TableLayout,
    key: &[StoredValue],
) -> Result<(), ChangesetError> {
    let predicate = table
        .columns
        .iter()
        .filter(|column| column.primary_key > 0)
        .map(|column| format!("{} IS ?", quote_identifier(&column.name)))
        .collect::<Vec<_>>()
        .join(" AND ");
    let sql = format!(
        "DELETE FROM {} WHERE {predicate}",
        quote_identifier(&table.name)
    );
    if connection.execute(&sql, params_from_iter(key))? != 1 {
        return Err(ChangesetError::Conflict(format!(
            "old row in {} was not unique",
            table.name
        )));
    }
    Ok(())
}

fn insert_final_row(
    connection: &Connection,
    table: &TableLayout,
    row: &[StoredValue],
) -> Result<(), ChangesetError> {
    let writable = table
        .columns
        .iter()
        .enumerate()
        .filter(|(_, column)| column.writable)
        .collect::<Vec<_>>();
    let columns = writable
        .iter()
        .map(|(_, column)| quote_identifier(&column.name))
        .collect::<Vec<_>>()
        .join(", ");
    let placeholders = std::iter::repeat_n("?", writable.len())
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        "INSERT INTO {} ({columns}) VALUES ({placeholders})",
        quote_identifier(&table.name)
    );
    connection.execute(
        &sql,
        params_from_iter(writable.iter().map(|(index, _)| &row[*index])),
    )?;
    Ok(())
}

fn table_layout(connection: &Connection, table: &str) -> Result<TableLayout, ChangesetError> {
    let exists = connection.query_row(
        "SELECT EXISTS(
             SELECT 1 FROM main.sqlite_schema
             WHERE type = 'table' AND name = ?1 COLLATE NOCASE
         )",
        [table],
        |row| row.get::<_, bool>(0),
    )?;
    if !exists {
        return Err(ChangesetError::UnknownTable(table.to_owned()));
    }
    let mut statement = connection.prepare(&format!(
        "PRAGMA main.table_xinfo({})",
        quote_identifier(table)
    ))?;
    let columns = statement
        .query_map((), |row| {
            Ok((
                ColumnLayout {
                    name: row.get(1)?,
                    primary_key: row.get(5)?,
                    writable: true,
                },
                row.get::<_, u32>(6)?,
            ))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?
        .into_iter()
        .filter_map(|(column, hidden)| (hidden == 0).then_some(column))
        .collect::<Vec<_>>();
    if columns.is_empty() {
        return Err(ChangesetError::UnknownTable(table.to_owned()));
    }
    Ok(TableLayout {
        name: table.to_owned(),
        columns,
    })
}

fn quote_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

#[derive(Debug)]
pub enum ChangesetError {
    Sqlite(rusqlite::Error),
    SchemaChanged,
    UnknownTable(String),
    TableWithoutPrimaryKey(String),
    Malformed(&'static str),
    Conflict(String),
    ForeignKeyViolation,
}

impl fmt::Display for ChangesetError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Sqlite(error) => write!(formatter, "SQLite changeset error: {error}"),
            Self::SchemaChanged => {
                formatter.write_str("canonical schema differs from the branch schema")
            }
            Self::UnknownTable(table) => write!(formatter, "unknown changeset table {table:?}"),
            Self::TableWithoutPrimaryKey(table) => {
                write!(formatter, "changeset table {table:?} has no primary key")
            }
            Self::Malformed(message) => write!(formatter, "malformed SQLite changeset: {message}"),
            Self::Conflict(message) => write!(formatter, "changeset conflict: {message}"),
            Self::ForeignKeyViolation => {
                formatter.write_str("changeset leaves a foreign-key violation")
            }
        }
    }
}

impl std::error::Error for ChangesetError {}

impl From<rusqlite::Error> for ChangesetError {
    fn from(error: rusqlite::Error) -> Self {
        Self::Sqlite(error)
    }
}

#[cfg(test)]
#[derive(Debug, Default, PartialEq, Eq)]
struct ChangeSummary {
    inserts: usize,
    updates: usize,
    deletes: usize,
    indirect: usize,
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;

    use homebase_core::tag::AdmissionSeq;

    use super::*;
    use crate::database::branch::{OverlayOptions, WritableBranch};
    use crate::database::wal::{SnapshotDescriptor, WalParser};

    struct Fixture {
        directory: tempfile::TempDir,
        writer: Connection,
        generation: u64,
    }

    impl Fixture {
        fn new(schema: &str, seed: &str) -> Self {
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("changeset.sqlite");
            let writer = Connection::open(path).unwrap();
            writer.pragma_update(None, "journal_mode", "WAL").unwrap();
            writer.pragma_update(None, "wal_autocheckpoint", 0).unwrap();
            writer.execute_batch(schema).unwrap();
            if !seed.is_empty() {
                writer.execute_batch(seed).unwrap();
            }
            Self {
                directory,
                writer,
                generation: 1,
            }
        }

        fn path(&self) -> PathBuf {
            self.directory.path().join("changeset.sqlite")
        }

        fn wal_path(&self) -> PathBuf {
            self.directory.path().join("changeset.sqlite-wal")
        }

        fn snapshot(&self) -> SnapshotDescriptor {
            let parsed = WalParser::parse(&fs::read(self.wal_path()).unwrap()).unwrap();
            SnapshotDescriptor::from_wal(
                self.generation,
                AdmissionSeq(self.generation),
                &parsed.snapshot.unwrap(),
            )
        }

        fn branch(&self) -> WritableBranch {
            WritableBranch::open(
                self.path(),
                self.wal_path(),
                self.snapshot(),
                OverlayOptions::default(),
            )
            .unwrap()
        }
    }

    #[test]
    fn net_capture_replays_pk_unique_defaults_and_generated_values() {
        let fixture = Fixture::new(
            "CREATE TABLE items (
                id INTEGER PRIMARY KEY,
                label TEXT NOT NULL UNIQUE,
                base INTEGER NOT NULL DEFAULT 5 CHECK(base >= 0),
                doubled INTEGER GENERATED ALWAYS AS (base * 2) STORED
             )",
            "INSERT INTO items(id, label, base) VALUES
                (1, 'alpha', 10),
                (2, 'beta', 20)",
        );
        let branch = fixture.branch();
        let capture = ChangesetCapture::start(branch.connection(), &["items"]).unwrap();
        branch
            .connection()
            .execute_batch(
                "BEGIN;
                 UPDATE items SET base = 11 WHERE id = 1;
                 UPDATE items SET base = 12 WHERE id = 1;
                 UPDATE items SET label = 'temporary' WHERE id = 1;
                 UPDATE items SET label = 'alpha' WHERE id = 2;
                 UPDATE items SET label = 'beta', id = 10 WHERE id = 1;
                 INSERT INTO items(id, label) VALUES (3, 'discarded');
                 DELETE FROM items WHERE id = 3;
                 INSERT INTO items(id, label) VALUES (4, 'defaulted');
                 COMMIT",
            )
            .unwrap();
        let changeset = capture.finish().unwrap();

        assert!(!changeset.is_empty());
        assert!(!changeset.bytes().is_empty());
        assert_eq!(
            changeset.schema(),
            schema_fingerprint(&fixture.writer).unwrap()
        );
        assert_eq!(
            changeset.summary().unwrap(),
            ChangeSummary {
                inserts: 2,
                updates: 1,
                deletes: 1,
                indirect: 0,
            }
        );
        changeset.apply(&fixture.writer).unwrap();

        assert_eq!(dump_items(branch.connection()), dump_items(&fixture.writer));
        assert_eq!(
            dump_items(&fixture.writer),
            vec![
                (2, "alpha".into(), 20, 40),
                (4, "defaulted".into(), 5, 10),
                (10, "beta".into(), 12, 24),
            ]
        );
    }

    #[test]
    fn indirect_cascade_and_trigger_effects_are_replayed_exactly_once() {
        let fixture = Fixture::new(
            "PRAGMA foreign_keys = ON;
             CREATE TABLE parents (id INTEGER PRIMARY KEY);
             CREATE TABLE children (
                 id INTEGER PRIMARY KEY,
                 parent_id INTEGER NOT NULL REFERENCES parents(id) ON DELETE CASCADE
             );
             CREATE TABLE audit (id INTEGER PRIMARY KEY, child_id INTEGER NOT NULL);
             CREATE TRIGGER audit_child_delete AFTER DELETE ON children BEGIN
                 INSERT INTO audit(child_id) VALUES (OLD.id);
             END",
            "INSERT INTO parents VALUES (1);
             INSERT INTO children VALUES (7, 1)",
        );
        let branch = fixture.branch();
        branch
            .connection()
            .pragma_update(None, "foreign_keys", true)
            .unwrap();
        let capture =
            ChangesetCapture::start(branch.connection(), &["parents", "children", "audit"])
                .unwrap();
        branch
            .connection()
            .execute("DELETE FROM parents WHERE id = 1", ())
            .unwrap();
        let changeset = capture.finish().unwrap();
        let summary = changeset.summary().unwrap();

        assert_eq!(summary.inserts, 1);
        assert_eq!(summary.deletes, 2);
        assert!(summary.indirect >= 2);
        changeset.apply(&fixture.writer).unwrap();
        assert_eq!(row_count(&fixture.writer, "parents"), 0);
        assert_eq!(row_count(&fixture.writer, "children"), 0);
        assert_eq!(row_count(&fixture.writer, "audit"), 1);
        assert_eq!(
            fixture
                .writer
                .query_row("SELECT child_id FROM audit", (), |row| row.get::<_, i64>(0))
                .unwrap(),
            7
        );
    }

    #[test]
    fn replay_allows_finally_valid_foreign_key_cycles() {
        let fixture = Fixture::new(
            "PRAGMA foreign_keys = ON;
             CREATE TABLE left_nodes (
                 id INTEGER PRIMARY KEY,
                 right_id INTEGER REFERENCES right_nodes(id) DEFERRABLE INITIALLY DEFERRED
             );
             CREATE TABLE right_nodes (
                 id INTEGER PRIMARY KEY,
                 left_id INTEGER REFERENCES left_nodes(id) DEFERRABLE INITIALLY DEFERRED
             )",
            "BEGIN;
             INSERT INTO left_nodes VALUES (1, 1);
             INSERT INTO right_nodes VALUES (1, 1);
             COMMIT",
        );
        let branch = fixture.branch();
        branch
            .connection()
            .pragma_update(None, "foreign_keys", true)
            .unwrap();
        let capture =
            ChangesetCapture::start(branch.connection(), &["left_nodes", "right_nodes"]).unwrap();
        branch
            .connection()
            .execute_batch(
                "BEGIN;
                 DELETE FROM left_nodes;
                 DELETE FROM right_nodes;
                 COMMIT",
            )
            .unwrap();
        let changeset = capture.finish().unwrap();

        changeset.apply(&fixture.writer).unwrap();
        assert_eq!(row_count(&fixture.writer, "left_nodes"), 0);
        assert_eq!(row_count(&fixture.writer, "right_nodes"), 0);
    }

    #[test]
    fn replay_rejects_conflicts_and_restores_the_canonical_transaction() {
        let fixture = Fixture::new(
            "CREATE TABLE records (id INTEGER PRIMARY KEY, value TEXT NOT NULL)",
            "INSERT INTO records VALUES (1, 'base')",
        );
        let branch = fixture.branch();
        let capture = ChangesetCapture::start(branch.connection(), &["records"]).unwrap();
        branch
            .connection()
            .execute("UPDATE records SET value = 'branch' WHERE id = 1", ())
            .unwrap();
        let changeset = capture.finish().unwrap();
        fixture
            .writer
            .execute("UPDATE records SET value = 'foreign' WHERE id = 1", ())
            .unwrap();

        assert!(matches!(
            changeset.apply(&fixture.writer),
            Err(ChangesetError::Conflict(_))
        ));
        assert_eq!(record_value(&fixture.writer), "foreign");
    }

    #[test]
    fn replay_rejects_schema_changes_before_mutating_rows() {
        let fixture = Fixture::new(
            "CREATE TABLE records (id INTEGER PRIMARY KEY, value TEXT NOT NULL)",
            "INSERT INTO records VALUES (1, 'base')",
        );
        let branch = fixture.branch();
        let capture = ChangesetCapture::start(branch.connection(), &["records"]).unwrap();
        branch
            .connection()
            .execute("UPDATE records SET value = 'branch' WHERE id = 1", ())
            .unwrap();
        let changeset = capture.finish().unwrap();
        fixture
            .writer
            .execute("ALTER TABLE records ADD COLUMN note TEXT", ())
            .unwrap();

        assert!(matches!(
            changeset.apply(&fixture.writer),
            Err(ChangesetError::SchemaChanged)
        ));
        assert_eq!(record_value(&fixture.writer), "base");
    }

    #[test]
    fn final_foreign_key_validation_rolls_back_an_orphan() {
        let fixture = Fixture::new(
            "CREATE TABLE parents (id INTEGER PRIMARY KEY);
             CREATE TABLE children (
                 id INTEGER PRIMARY KEY,
                 parent_id INTEGER NOT NULL REFERENCES parents(id)
             )",
            "",
        );
        let branch = fixture.branch();
        branch
            .connection()
            .pragma_update(None, "foreign_keys", false)
            .unwrap();
        let capture = ChangesetCapture::start(branch.connection(), &["children"]).unwrap();
        branch
            .connection()
            .execute("INSERT INTO children VALUES (1, 99)", ())
            .unwrap();
        let changeset = capture.finish().unwrap();

        assert!(matches!(
            changeset.apply(&fixture.writer),
            Err(ChangesetError::ForeignKeyViolation)
        ));
        assert_eq!(row_count(&fixture.writer, "children"), 0);
    }

    #[test]
    fn capture_rejects_tables_without_stable_row_identity() {
        let connection = Connection::open_in_memory().unwrap();
        connection
            .execute("CREATE TABLE loose(value TEXT)", ())
            .unwrap();
        assert!(matches!(
            ChangesetCapture::start(&connection, &["loose"]),
            Err(ChangesetError::TableWithoutPrimaryKey(table)) if table == "loose"
        ));
    }

    fn dump_items(connection: &Connection) -> Vec<(i64, String, i64, i64)> {
        let mut statement = connection
            .prepare("SELECT id, label, base, doubled FROM items ORDER BY id")
            .unwrap();
        statement
            .query_map((), |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
            })
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap()
    }

    fn row_count(connection: &Connection, table: &str) -> i64 {
        connection
            .query_row(
                &format!("SELECT count(*) FROM {}", quote_identifier(table)),
                (),
                |row| row.get(0),
            )
            .unwrap()
    }

    fn record_value(connection: &Connection) -> String {
        connection
            .query_row("SELECT value FROM records WHERE id = 1", (), |row| {
                row.get(0)
            })
            .unwrap()
    }
}
