//! Net SQLite changes captured from a private branch and replayed canonically.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::io::Cursor;

use fallible_iterator::FallibleIterator as _;
use fallible_streaming_iterator::FallibleStreamingIterator as _;
use homebase_core::reader::Reader;
use homebase_core::writer::Writer;
use rusqlite::config::DbConfig;
use rusqlite::hooks::Action;
use rusqlite::session::{ChangesetIter, Session};
use rusqlite::{Connection, OptionalExtension as _, params_from_iter};
use sha2::{Digest, Sha256};
use sqlite3_parser::ast::{Cmd, ColumnConstraint, CreateTableBody, Stmt};
use sqlite3_parser::lexer::sql::Parser;

use super::WritableBranch;
use crate::value::StoredValue;

const CHANGESET_FRAME_VERSION: u8 = 2;
const TAG_TABLE_BINDING: u8 = 1;
const TAG_SQLITE_CHANGESET: u8 = 2;
const TABLE_BINDING_FRAME_VERSION: u8 = 1;
const TAG_TABLE_NAME: u8 = 1;
const TAG_TABLE_FINGERPRINT: u8 = 2;

/// Physical SQLite definition of one table touched by a changeset.
#[derive(Clone, Debug, PartialEq, Eq)]
struct TableBinding {
    name: String,
    fingerprint: [u8; 32],
}

/// A transaction's net changes over explicitly attached synchronized tables.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CapturedChangeset {
    tables: Vec<TableBinding>,
    sqlite: Vec<u8>,
}

/// One final inserted row projected from SQLite's net changeset.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CapturedInsert {
    pub table: String,
    pub rowid: i64,
    pub values: Vec<StoredValue>,
}

impl CapturedChangeset {
    pub fn is_empty(&self) -> bool {
        self.sqlite.is_empty()
    }

    pub fn sqlite_bytes(&self) -> &[u8] {
        &self.sqlite
    }

    /// Verify every touched table still has its captured physical definition.
    pub fn validate_tables(&self, connection: &Connection) -> Result<(), ChangesetError> {
        validate_table_bindings(connection, &self.tables)
    }

    /// Project the currently supported insert-only logical transaction.
    pub fn inserted_rows(&self) -> Result<Vec<CapturedInsert>, ChangesetError> {
        decode_changeset(&self.sqlite)?
            .into_iter()
            .map(|change| {
                if change.kind != ChangeKind::Insert {
                    return Err(ChangesetError::UnsupportedChange {
                        table: change.table,
                        operation: change.kind.name(),
                    });
                }
                let values = change
                    .new
                    .into_iter()
                    .map(|value| {
                        value.ok_or(ChangesetError::Malformed(
                            "insert changeset omits a final column value",
                        ))
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                let rowid = change
                    .primary_key
                    .iter()
                    .enumerate()
                    .filter_map(|(index, primary)| primary.then_some(index))
                    .collect::<Vec<_>>();
                let rowid = match rowid.as_slice() {
                    [index] => match values[*index] {
                        StoredValue::Integer(value) => value,
                        _ => 0,
                    },
                    _ => 0,
                };
                Ok(CapturedInsert {
                    table: change.table,
                    rowid,
                    values,
                })
            })
            .collect()
    }

    /// Encode the complete replay record, including every required sidecar.
    pub fn encode(&self) -> Result<Vec<u8>, ChangesetError> {
        let mut writer = Writer::new();
        writer.u8(CHANGESET_FRAME_VERSION);
        for table in &self.tables {
            writer
                .field(TAG_TABLE_BINDING, &encode_table_binding(table)?)
                .map_err(|_| ChangesetError::Malformed("captured changeset field is too large"))?;
        }
        writer
            .field(TAG_SQLITE_CHANGESET, &self.sqlite)
            .map_err(|_| ChangesetError::Malformed("captured changeset field is too large"))?;
        Ok(writer.finish())
    }

    /// Decode one complete replay record and validate its SQLite payload.
    pub fn decode(frame: &[u8]) -> Result<Self, ChangesetError> {
        let mut reader = Reader::new(frame);
        if reader.u8() != Some(CHANGESET_FRAME_VERSION) {
            return Err(ChangesetError::Malformed(
                "unknown captured changeset frame version",
            ));
        }
        let mut tables = Vec::new();
        let mut sqlite = None;
        while let Some((tag, value)) = reader
            .field()
            .map_err(|_| ChangesetError::Malformed("truncated captured changeset field"))?
        {
            match tag {
                TAG_TABLE_BINDING => tables.push(decode_table_binding(value)?),
                TAG_SQLITE_CHANGESET => set_once(&mut sqlite, value.to_vec())?,
                _ => {}
            }
        }
        let sqlite = sqlite.ok_or(ChangesetError::Malformed(
            "captured changeset is missing its SQLite payload",
        ))?;
        let changes = decode_changeset(&sqlite)?;
        let captured = Self { tables, sqlite };
        validate_binding_set(&captured.tables, &changes)?;
        Ok(captured)
    }

    /// Replay the final logical changes without re-running triggers or FK actions.
    pub fn apply(&self, connection: &Connection) -> Result<(), ChangesetError> {
        let changes = decode_changeset(&self.sqlite)?;
        validate_binding_set(&self.tables, &changes)?;
        apply_changes(connection, &self.tables, &changes)
    }

    #[cfg(test)]
    fn summary(&self) -> Result<ChangeSummary, ChangesetError> {
        let mut summary = ChangeSummary::default();
        for change in decode_changeset(&self.sqlite)? {
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
    tables: BTreeMap<Vec<u8>, TableBinding>,
}

impl<'connection> ChangesetCapture<'connection> {
    pub fn start(
        branch: &'connection WritableBranch,
        tables: &[&str],
    ) -> Result<Self, ChangesetError> {
        Self::start_between(branch.connection(), tables)
    }

    fn start_between(
        connection: &'connection Connection,
        tables: &[&str],
    ) -> Result<Self, ChangesetError> {
        let mut bindings = BTreeMap::new();
        let mut session = Session::new(connection)?;
        for table in tables {
            let layout = table_layout(connection, table)?;
            if !layout.columns.iter().any(|column| column.primary_key > 0) {
                return Err(ChangesetError::TableWithoutPrimaryKey((*table).to_owned()));
            }
            let binding = table_binding(connection, table)?
                .ok_or_else(|| ChangesetError::UnknownTable((*table).to_owned()))?;
            if bindings
                .insert(canonical_name(&binding.name), binding)
                .is_some()
            {
                return Err(ChangesetError::Malformed(
                    "duplicate attached changeset table",
                ));
            }
            session.attach(Some(*table))?;
        }
        Ok(Self {
            session,
            tables: bindings,
        })
    }

    pub fn finish(mut self) -> Result<CapturedChangeset, ChangesetError> {
        let mut bytes = Vec::new();
        self.session.changeset_strm(&mut bytes)?;
        let changes = decode_changeset(&bytes)?;
        let touched = changes
            .iter()
            .map(|change| canonical_name(&change.table))
            .collect::<BTreeSet<_>>();
        let tables = touched
            .into_iter()
            .map(|name| {
                self.tables.remove(&name).ok_or(ChangesetError::Malformed(
                    "changeset contains an unattached table",
                ))
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(CapturedChangeset {
            tables,
            sqlite: bytes,
        })
    }
}

fn set_once<T>(slot: &mut Option<T>, value: T) -> Result<(), ChangesetError> {
    if slot.replace(value).is_some() {
        Err(ChangesetError::Malformed(
            "duplicate captured changeset field",
        ))
    } else {
        Ok(())
    }
}

fn encode_table_binding(binding: &TableBinding) -> Result<Vec<u8>, ChangesetError> {
    let mut writer = Writer::new();
    writer.u8(TABLE_BINDING_FRAME_VERSION);
    writer
        .field(TAG_TABLE_NAME, binding.name.as_bytes())
        .map_err(|_| ChangesetError::Malformed("captured changeset field is too large"))?;
    writer
        .field(TAG_TABLE_FINGERPRINT, &binding.fingerprint)
        .map_err(|_| ChangesetError::Malformed("captured changeset field is too large"))?;
    Ok(writer.finish())
}

fn decode_table_binding(frame: &[u8]) -> Result<TableBinding, ChangesetError> {
    let mut reader = Reader::new(frame);
    if reader.u8() != Some(TABLE_BINDING_FRAME_VERSION) {
        return Err(ChangesetError::Malformed(
            "unknown changeset table-binding version",
        ));
    }
    let mut name = None;
    let mut fingerprint = None;
    while let Some((tag, value)) = reader
        .field()
        .map_err(|_| ChangesetError::Malformed("truncated captured changeset field"))?
    {
        match tag {
            TAG_TABLE_NAME => {
                let value = String::from_utf8(value.to_vec())
                    .map_err(|_| ChangesetError::Malformed("changeset table name is not UTF-8"))?;
                if value.is_empty() {
                    return Err(ChangesetError::Malformed("changeset table name is empty"));
                }
                set_once(&mut name, value)?;
            }
            TAG_TABLE_FINGERPRINT => {
                let value = value.try_into().map_err(|_| {
                    ChangesetError::Malformed("table fingerprint has the wrong length")
                })?;
                set_once(&mut fingerprint, value)?;
            }
            _ => {}
        }
    }
    Ok(TableBinding {
        name: name.ok_or(ChangesetError::Malformed(
            "changeset table binding is missing its name",
        ))?,
        fingerprint: fingerprint.ok_or(ChangesetError::Malformed(
            "changeset table binding is missing its fingerprint",
        ))?,
    })
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ChangeKind {
    Insert,
    Update,
    Delete,
}

impl ChangeKind {
    fn name(self) -> &'static str {
        match self {
            Self::Insert => "INSERT",
            Self::Update => "UPDATE",
            Self::Delete => "DELETE",
        }
    }
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
    declared_type: String,
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

fn table_binding(
    connection: &Connection,
    table: &str,
) -> Result<Option<TableBinding>, ChangesetError> {
    let mut statement = connection.prepare(
        "SELECT type, name, tbl_name, coalesce(sql, '')
         FROM main.sqlite_schema
         WHERE tbl_name = ?1 COLLATE NOCASE
           AND name NOT LIKE 'sqlite_%'
         ORDER BY type, name",
    )?;
    let rows = statement.query_map([table], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
            row.get::<_, String>(3)?,
        ))
    })?;
    let rows = rows.collect::<rusqlite::Result<Vec<_>>>()?;
    let Some(table_name) = rows
        .iter()
        .find(|(kind, name, owner, _)| kind == "table" && name == owner)
        .map(|(_, name, _, _)| name.clone())
    else {
        return Ok(None);
    };
    let mut hash = Sha256::new();
    for (kind, name, table, sql) in rows {
        for field in [&kind, &name, &table, &sql] {
            let length = u64::try_from(field.len())
                .map_err(|_| ChangesetError::Malformed("schema field is too large"))?;
            hash.update(length.to_be_bytes());
            hash.update(field.as_bytes());
        }
    }
    Ok(Some(TableBinding {
        name: table_name,
        fingerprint: hash.finalize().into(),
    }))
}

fn validate_table_bindings(
    connection: &Connection,
    expected: &[TableBinding],
) -> Result<(), ChangesetError> {
    for expected in expected {
        if table_binding(connection, &expected.name)?.as_ref() != Some(expected) {
            return Err(ChangesetError::SchemaChanged);
        }
    }
    Ok(())
}

fn validate_binding_set(
    bindings: &[TableBinding],
    changes: &[NetChange],
) -> Result<(), ChangesetError> {
    let expected = changes
        .iter()
        .map(|change| canonical_name(&change.table))
        .collect::<BTreeSet<_>>();
    let actual = bindings
        .iter()
        .map(|binding| canonical_name(&binding.name))
        .collect::<Vec<_>>();
    if actual.windows(2).any(|pair| pair[0] >= pair[1])
        || actual.into_iter().collect::<BTreeSet<_>>() != expected
    {
        return Err(ChangesetError::Malformed(
            "changeset table bindings do not match its changes",
        ));
    }
    Ok(())
}

fn canonical_name(name: &str) -> Vec<u8> {
    let mut canonical = name.as_bytes().to_vec();
    canonical.make_ascii_lowercase();
    canonical
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

fn apply_changes(
    connection: &Connection,
    expected_tables: &[TableBinding],
    changes: &[NetChange],
) -> Result<(), ChangesetError> {
    connection.execute_batch("SAVEPOINT __multilite_changeset_apply")?;
    let result = (|| {
        validate_table_bindings(connection, expected_tables)?;
        if changes.is_empty() {
            return Ok(());
        }

        let triggers = connection.db_config(DbConfig::SQLITE_DBCONFIG_ENABLE_TRIGGER)?;
        let foreign_keys = connection.db_config(DbConfig::SQLITE_DBCONFIG_ENABLE_FKEY)?;
        connection.set_db_config(DbConfig::SQLITE_DBCONFIG_ENABLE_TRIGGER, false)?;
        if let Err(error) = connection.set_db_config(DbConfig::SQLITE_DBCONFIG_ENABLE_FKEY, false) {
            let _ = connection.set_db_config(DbConfig::SQLITE_DBCONFIG_ENABLE_TRIGGER, triggers);
            return Err(error.into());
        }

        let replay_result = (|| {
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

        let restore_triggers = connection
            .set_db_config(DbConfig::SQLITE_DBCONFIG_ENABLE_TRIGGER, triggers)
            .map(|_| ());
        let restore_foreign_keys = connection
            .set_db_config(DbConfig::SQLITE_DBCONFIG_ENABLE_FKEY, foreign_keys)
            .map(|_| ());
        replay_result?;
        restore_triggers?;
        restore_foreign_keys?;
        Ok(())
    })();

    match result {
        Ok(()) => connection.execute_batch("RELEASE __multilite_changeset_apply")?,
        Err(error) => {
            connection.execute_batch(
                "ROLLBACK TO __multilite_changeset_apply;
                 RELEASE __multilite_changeset_apply",
            )?;
            return Err(error);
        }
    }
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
    let selected = table
        .columns
        .iter()
        .map(|column| quote_identifier(&column.name))
        .collect::<Vec<_>>();
    let columns = selected.join(", ");
    let predicate = primary_key_predicate(table);
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

fn primary_key_predicate(table: &TableLayout) -> String {
    table
        .columns
        .iter()
        .filter(|column| column.primary_key > 0)
        .map(|column| format!("{} IS ?", quote_identifier(&column.name)))
        .collect::<Vec<_>>()
        .join(" AND ")
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
    let schema_sql = connection
        .query_row(
            "SELECT sql FROM main.sqlite_schema
             WHERE type = 'table' AND name = ?1 COLLATE NOCASE",
            [table],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .ok_or_else(|| ChangesetError::UnknownTable(table.to_owned()))?;
    let (table_type, without_rowid) = connection
        .query_row(
            "SELECT type, wr FROM pragma_table_list
             WHERE schema = 'main' AND name = ?1 COLLATE NOCASE",
            [table],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, bool>(1)?)),
        )
        .optional()?
        .ok_or_else(|| ChangesetError::UnknownTable(table.to_owned()))?;
    if table_type == "virtual" {
        return Err(ChangesetError::UnsupportedTable {
            table: table.to_owned(),
            reason: "virtual tables are not yet supported",
        });
    }
    if table_type != "table" {
        return Err(ChangesetError::UnsupportedTable {
            table: table.to_owned(),
            reason: "only ordinary SQLite tables are supported",
        });
    }
    if schema_uses_autoincrement(&schema_sql)? {
        return Err(ChangesetError::UnsupportedTable {
            table: table.to_owned(),
            reason: "AUTOINCREMENT state is not yet captured",
        });
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
                    declared_type: row.get(2)?,
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

    let primary = columns
        .iter()
        .filter(|column| column.primary_key > 0)
        .collect::<Vec<_>>();
    if !without_rowid && !primary.is_empty() {
        let has_primary_index = connection.query_row(
            "SELECT EXISTS(
                SELECT 1 FROM pragma_index_list(?1) WHERE origin = 'pk'
             )",
            [table],
            |row| row.get::<_, bool>(0),
        )?;
        if primary.len() != 1
            || !primary[0].declared_type.eq_ignore_ascii_case("INTEGER")
            || has_primary_index
        {
            return Err(ChangesetError::UnsupportedTable {
                table: table.to_owned(),
                reason: "rowid tables require a single INTEGER PRIMARY KEY alias",
            });
        }
    }
    Ok(TableLayout {
        name: table.to_owned(),
        columns,
    })
}

fn schema_uses_autoincrement(sql: &str) -> Result<bool, ChangesetError> {
    let mut parser = Parser::new(sql.as_bytes());
    let command = parser
        .next()
        .map_err(|_| ChangesetError::Malformed("stored table SQL is not valid SQLite"))?
        .ok_or(ChangesetError::Malformed("stored table SQL is empty"))?;
    if parser
        .next()
        .map_err(|_| ChangesetError::Malformed("stored table SQL is not valid SQLite"))?
        .is_some()
    {
        return Err(ChangesetError::Malformed(
            "stored table SQL contains multiple statements",
        ));
    }
    let Cmd::Stmt(Stmt::CreateTable { body, .. }) = command else {
        return Err(ChangesetError::Malformed(
            "ordinary table does not have CREATE TABLE SQL",
        ));
    };
    let CreateTableBody::ColumnsAndConstraints { columns, .. } = body else {
        return Ok(false);
    };
    Ok(columns.into_values().any(|column| {
        column.constraints.into_iter().any(|constraint| {
            matches!(
                constraint.constraint,
                ColumnConstraint::PrimaryKey {
                    auto_increment: true,
                    ..
                }
            )
        })
    }))
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
    UnsupportedTable {
        table: String,
        reason: &'static str,
    },
    UnsupportedChange {
        table: String,
        operation: &'static str,
    },
    Malformed(&'static str),
    Conflict(String),
    ForeignKeyViolation,
}

impl fmt::Display for ChangesetError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Sqlite(error) => write!(formatter, "SQLite changeset error: {error}"),
            Self::SchemaChanged => {
                formatter.write_str("a touched table differs from the branch schema")
            }
            Self::UnknownTable(table) => write!(formatter, "unknown changeset table {table:?}"),
            Self::TableWithoutPrimaryKey(table) => {
                write!(formatter, "changeset table {table:?} has no primary key")
            }
            Self::UnsupportedTable { table, reason } => {
                write!(formatter, "unsupported changeset table {table:?}: {reason}")
            }
            Self::UnsupportedChange { table, operation } => {
                write!(
                    formatter,
                    "unsupported {operation} change in table {table:?}"
                )
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
    use std::path::PathBuf;

    use super::*;
    use crate::branch::snapshot::PinnedSnapshot;
    use crate::branch::{OverlayOptions, WritableBranch};

    struct Fixture {
        directory: tempfile::TempDir,
        writer: Connection,
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
            Self { directory, writer }
        }

        fn path(&self) -> PathBuf {
            self.directory.path().join("changeset.sqlite")
        }

        fn wal_path(&self) -> PathBuf {
            self.directory.path().join("changeset.sqlite-wal")
        }

        fn snapshot(&self) -> PinnedSnapshot {
            PinnedSnapshot::capture(self.path(), self.wal_path()).unwrap()
        }

        fn branch(&self) -> WritableBranch {
            WritableBranch::open(self.snapshot(), OverlayOptions::default()).unwrap()
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
        let capture = ChangesetCapture::start(&branch, &["items"]).unwrap();
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
        assert!(!changeset.sqlite_bytes().is_empty());
        assert_eq!(
            CapturedChangeset::decode(&changeset.encode().unwrap()).unwrap(),
            changeset
        );
        assert_eq!(
            changeset.tables,
            vec![table_binding(&fixture.writer, "items").unwrap().unwrap()]
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
    fn strict_without_rowid_tables_accept_richer_dml_grammar() {
        let fixture = Fixture::new(
            "CREATE TABLE metrics (
                org TEXT COLLATE NOCASE,
                slug TEXT,
                score REAL CHECK(score >= 0),
                note TEXT,
                PRIMARY KEY(org, slug)
             ) WITHOUT ROWID, STRICT;
             CREATE UNIQUE INDEX metrics_note
                ON metrics(lower(note)) WHERE note IS NOT NULL",
            "INSERT INTO metrics VALUES ('acme', 'one', 1.0, 'First')",
        );
        let branch = fixture.branch();
        let capture = ChangesetCapture::start(&branch, &["metrics"]).unwrap();
        branch
            .connection()
            .execute_batch(
                "WITH incoming(org, slug, score, note) AS (
                    VALUES ('acme', 'two', 2.0, 'Second')
                 )
                 INSERT INTO metrics SELECT * FROM incoming;
                 INSERT INTO metrics VALUES ('ACME', 'one', 3.0, 'First')
                 ON CONFLICT(org, slug) DO UPDATE SET score = excluded.score;
                 WITH patch(org, slug, score) AS (
                    VALUES ('acme', 'two', 4.5)
                 )
                 UPDATE metrics SET score = patch.score
                 FROM patch
                 WHERE metrics.org = patch.org AND metrics.slug = patch.slug",
            )
            .unwrap();
        let deleted = branch
            .connection()
            .query_row(
                "DELETE FROM metrics WHERE slug = 'one' RETURNING note",
                (),
                |row| row.get::<_, String>(0),
            )
            .unwrap();
        assert_eq!(deleted, "First");
        let changeset = capture.finish().unwrap();
        changeset.apply(&fixture.writer).unwrap();

        let dump = |connection: &Connection| {
            connection
                .query_row("SELECT org, slug, score, note FROM metrics", (), |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, f64>(2)?,
                        row.get::<_, String>(3)?,
                    ))
                })
                .unwrap()
        };
        assert_eq!(dump(branch.connection()), dump(&fixture.writer));
        assert_eq!(
            dump(&fixture.writer),
            ("acme".into(), "two".into(), 4.5, "Second".into())
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
        let capture = ChangesetCapture::start(&branch, &["parents", "children", "audit"]).unwrap();
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
        let capture = ChangesetCapture::start(&branch, &["left_nodes", "right_nodes"]).unwrap();
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
        let capture = ChangesetCapture::start(&branch, &["records"]).unwrap();
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
        assert!(
            fixture
                .writer
                .db_config(DbConfig::SQLITE_DBCONFIG_ENABLE_TRIGGER)
                .unwrap()
        );
        assert!(
            fixture
                .writer
                .db_config(DbConfig::SQLITE_DBCONFIG_ENABLE_FKEY)
                .unwrap()
        );
    }

    #[test]
    fn late_constraint_failure_rolls_back_only_the_replay_savepoint() {
        let fixture = Fixture::new(
            "CREATE TABLE records (
                id INTEGER PRIMARY KEY,
                email TEXT NOT NULL UNIQUE
             );
             CREATE TABLE surrounding (value TEXT NOT NULL)",
            "INSERT INTO records VALUES (1, 'old@example.com')",
        );
        let branch = fixture.branch();
        let capture = ChangesetCapture::start(&branch, &["records"]).unwrap();
        branch
            .connection()
            .execute(
                "UPDATE records SET email = 'claimed@example.com' WHERE id = 1",
                (),
            )
            .unwrap();
        let changeset = capture.finish().unwrap();

        fixture.writer.execute_batch("BEGIN").unwrap();
        fixture
            .writer
            .execute("INSERT INTO surrounding VALUES ('keep me')", ())
            .unwrap();
        fixture
            .writer
            .execute("INSERT INTO records VALUES (2, 'claimed@example.com')", ())
            .unwrap();
        assert!(matches!(
            changeset.apply(&fixture.writer),
            Err(ChangesetError::Sqlite(rusqlite::Error::SqliteFailure(_, _)))
        ));

        assert_eq!(row_count(&fixture.writer, "surrounding"), 1);
        assert_eq!(
            fixture
                .writer
                .query_row("SELECT email FROM records WHERE id = 1", (), |row| {
                    row.get::<_, String>(0)
                })
                .unwrap(),
            "old@example.com"
        );
        fixture.writer.execute_batch("COMMIT").unwrap();
        assert_eq!(row_count(&fixture.writer, "records"), 2);
    }

    #[test]
    fn replay_rejects_schema_changes_before_mutating_rows() {
        let fixture = Fixture::new(
            "CREATE TABLE records (id INTEGER PRIMARY KEY, value TEXT NOT NULL)",
            "INSERT INTO records VALUES (1, 'base')",
        );
        let branch = fixture.branch();
        let capture = ChangesetCapture::start(&branch, &["records"]).unwrap();
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
    fn replay_ignores_physical_schema_changes_to_untouched_tables() {
        let fixture = Fixture::new(
            "CREATE TABLE records (id INTEGER PRIMARY KEY, value TEXT NOT NULL);
             CREATE TABLE unrelated (id INTEGER PRIMARY KEY)",
            "INSERT INTO records VALUES (1, 'base')",
        );
        let branch = fixture.branch();
        let capture = ChangesetCapture::start(&branch, &["records", "unrelated"]).unwrap();
        branch
            .connection()
            .execute("UPDATE records SET value = 'branch' WHERE id = 1", ())
            .unwrap();
        let changeset = capture.finish().unwrap();
        assert_eq!(
            changeset
                .tables
                .iter()
                .map(|table| table.name.as_str())
                .collect::<Vec<_>>(),
            ["records"]
        );

        fixture
            .writer
            .execute("ALTER TABLE unrelated ADD COLUMN note TEXT", ())
            .unwrap();
        changeset.apply(&fixture.writer).unwrap();
        assert_eq!(record_value(&fixture.writer), "branch");
    }

    #[test]
    fn replay_rejects_index_changes_on_a_touched_table() {
        let fixture = Fixture::new(
            "CREATE TABLE records (id INTEGER PRIMARY KEY, value TEXT NOT NULL)",
            "INSERT INTO records VALUES (1, 'base')",
        );
        let branch = fixture.branch();
        let capture = ChangesetCapture::start(&branch, &["records"]).unwrap();
        branch
            .connection()
            .execute("UPDATE records SET value = 'branch' WHERE id = 1", ())
            .unwrap();
        let changeset = capture.finish().unwrap();
        fixture
            .writer
            .execute("CREATE INDEX records_value ON records(value)", ())
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
        let capture = ChangesetCapture::start(&branch, &["children"]).unwrap();
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
    fn complete_codec_rejects_truncation_duplicates_and_binding_mismatch() {
        let fixture = Fixture::new(
            "CREATE TABLE records (
                code TEXT PRIMARY KEY,
                value TEXT NOT NULL
             ) WITHOUT ROWID",
            "INSERT INTO records VALUES ('a', 'base')",
        );
        let branch = fixture.branch();
        let capture = ChangesetCapture::start(&branch, &["records"]).unwrap();
        branch
            .connection()
            .execute("UPDATE records SET value = 'branch' WHERE code = 'a'", ())
            .unwrap();
        let changeset = capture.finish().unwrap();
        let encoded = changeset.encode().unwrap();

        for length in 0..encoded.len() {
            assert!(CapturedChangeset::decode(&encoded[..length]).is_err());
        }
        let mut unknown_version = encoded.clone();
        unknown_version[0] = CHANGESET_FRAME_VERSION + 1;
        assert!(matches!(
            CapturedChangeset::decode(&unknown_version),
            Err(ChangesetError::Malformed(
                "unknown captured changeset frame version"
            ))
        ));

        let mut with_unknown = encoded.clone();
        with_unknown.extend_from_slice(&[99, 0, 0, 0, 2, 7, 8]);
        assert_eq!(CapturedChangeset::decode(&with_unknown).unwrap(), changeset);

        let mut duplicate_binding = encoded.clone();
        let mut writer = Writer::new();
        writer
            .field(
                TAG_TABLE_BINDING,
                &encode_table_binding(&changeset.tables[0]).unwrap(),
            )
            .unwrap();
        duplicate_binding.extend_from_slice(&writer.finish());
        assert!(matches!(
            CapturedChangeset::decode(&duplicate_binding),
            Err(ChangesetError::Malformed(
                "changeset table bindings do not match its changes"
            ))
        ));

        let missing_binding = CapturedChangeset {
            tables: Vec::new(),
            sqlite: changeset.sqlite,
        };
        assert!(matches!(
            CapturedChangeset::decode(&missing_binding.encode().unwrap()),
            Err(ChangesetError::Malformed(
                "changeset table bindings do not match its changes"
            ))
        ));
    }

    #[test]
    fn capture_rejects_tables_without_stable_row_identity() {
        let connection = Connection::open_in_memory().unwrap();
        connection
            .execute("CREATE TABLE loose(value TEXT)", ())
            .unwrap();
        assert!(matches!(
            ChangesetCapture::start_between(&connection, &["loose"]),
            Err(ChangesetError::TableWithoutPrimaryKey(table)) if table == "loose"
        ));
    }

    #[test]
    fn capture_explicitly_rejects_untracked_row_identity_state() {
        let connection = Connection::open_in_memory().unwrap();
        connection
            .execute_batch(
                "CREATE TABLE generated_ids (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    value TEXT
                 );
                 CREATE TABLE accepted (
                    id INTEGER PRIMARY KEY,
                    value TEXT
                 );
                 CREATE TABLE textual (
                    id TEXT PRIMARY KEY,
                    value TEXT
                 );
                 CREATE TABLE descending (
                    id INTEGER PRIMARY KEY DESC,
                    value TEXT
                 );
                 CREATE TABLE composite (
                    first INTEGER,
                    second INTEGER,
                    PRIMARY KEY (first, second)
                 );
                 CREATE TABLE shadowed (
                    rowid TEXT,
                    _rowid_ TEXT,
                    oid TEXT,
                    id TEXT PRIMARY KEY
                 )",
            )
            .unwrap();

        assert!(matches!(
            ChangesetCapture::start_between(&connection, &["generated_ids"]),
            Err(ChangesetError::UnsupportedTable { table, reason })
                if table == "generated_ids" && reason.contains("AUTOINCREMENT")
        ));
        ChangesetCapture::start_between(&connection, &["accepted"]).unwrap();
        for table in ["textual", "descending", "composite", "shadowed"] {
            assert!(matches!(
                ChangesetCapture::start_between(&connection, &[table]),
                Err(ChangesetError::UnsupportedTable {
                    table: rejected,
                    reason
                }) if rejected == table && reason.contains("INTEGER PRIMARY KEY")
            ));
        }
    }

    #[test]
    fn table_features_are_classified_structurally_not_by_schema_keywords() {
        let connection = Connection::open_in_memory().unwrap();
        connection
            .execute_batch(
                "CREATE TABLE \"AUTOINCREMENT ledger\" (
                    code TEXT PRIMARY KEY,
                    note TEXT DEFAULT 'AUTOINCREMENT WITHOUT ROWID'
                 ) WITHOUT ROWID;
                 CREATE VIRTUAL TABLE search_docs USING fts5(body)",
            )
            .unwrap();

        ChangesetCapture::start_between(&connection, &["AUTOINCREMENT ledger"]).unwrap();
        assert!(matches!(
            ChangesetCapture::start_between(&connection, &["search_docs"]),
            Err(ChangesetError::UnsupportedTable { table, reason })
                if table == "search_docs" && reason.contains("virtual")
        ));
    }

    #[test]
    fn randomized_net_replay_matches_the_private_branch() {
        for seed in 1_u64..=24 {
            let fixture = Fixture::new(
                "CREATE TABLE records (
                    code TEXT PRIMARY KEY,
                    value INTEGER NOT NULL CHECK(value >= 0),
                    marker TEXT NOT NULL UNIQUE,
                    payload BLOB NOT NULL,
                    doubled INTEGER GENERATED ALWAYS AS (value * 2) STORED
                 ) WITHOUT ROWID",
                "INSERT INTO records(code, value, marker, payload) VALUES
                    ('k0', 0, 'marker-k0', x'00'),
                    ('k1', 1, 'marker-k1', x'01'),
                    ('k2', 2, 'marker-k2', x'02'),
                    ('k3', 3, 'marker-k3', x'03'),
                    ('k4', 4, 'marker-k4', x'04'),
                    ('k5', 5, 'marker-k5', x'05')",
            );
            let branch = fixture.branch();
            let capture = ChangesetCapture::start(&branch, &["records"]).unwrap();
            let mut random = seed;
            for step in 0_u64..64 {
                random = random
                    .wrapping_mul(6_364_136_223_846_793_005)
                    .wrapping_add(1_442_695_040_888_963_407);
                let key = format!("k{}", (random >> 16) % 18);
                let value = i64::try_from(seed * 1000 + step).unwrap();
                match random % 4 {
                    0 | 1 => {
                        branch
                            .connection()
                            .execute(
                                "INSERT INTO records(code, value, marker, payload)
                                 VALUES (?1, ?2, 'marker-' || ?1, ?3)
                                 ON CONFLICT(code) DO UPDATE SET
                                    value = excluded.value,
                                    marker = excluded.marker,
                                    payload = excluded.payload",
                                rusqlite::params![
                                    key,
                                    value,
                                    vec![seed as u8, step as u8, (random >> 32) as u8]
                                ],
                            )
                            .unwrap();
                    }
                    2 => {
                        branch
                            .connection()
                            .execute(
                                "WITH patch(code, value) AS (VALUES (?1, ?2))
                                 UPDATE records SET value = patch.value
                                 FROM patch WHERE records.code = patch.code",
                                rusqlite::params![key, value],
                            )
                            .unwrap();
                    }
                    _ => {
                        let _ = branch
                            .connection()
                            .query_row(
                                "DELETE FROM records WHERE code = ?1 RETURNING value",
                                [key],
                                |row| row.get::<_, i64>(0),
                            )
                            .optional()
                            .unwrap();
                    }
                }
            }

            let changeset = capture.finish().unwrap();
            let decoded = CapturedChangeset::decode(&changeset.encode().unwrap()).unwrap();
            decoded.apply(&fixture.writer).unwrap();
            assert_eq!(
                dump_randomized(branch.connection()),
                dump_randomized(&fixture.writer),
                "seed {seed}"
            );
            assert_eq!(
                fixture
                    .writer
                    .query_row("PRAGMA integrity_check", (), |row| row.get::<_, String>(0))
                    .unwrap(),
                "ok",
                "seed {seed}"
            );
        }
    }

    fn dump_randomized(connection: &Connection) -> Vec<(String, i64, String, Vec<u8>, i64)> {
        let mut statement = connection
            .prepare(
                "SELECT code, value, marker, payload, doubled
                 FROM records ORDER BY code",
            )
            .unwrap();
        statement
            .query_map((), |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                ))
            })
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap()
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
