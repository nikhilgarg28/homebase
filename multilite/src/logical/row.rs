//! Captured SQLite rows and their durable Homebase representation.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use homebase_core::key::{Key, KeyError};
#[cfg(test)]
use homebase_core::messages::AdmittedBatch;
use homebase_core::range::Range;
use homebase_core::reader::Reader;
use homebase_core::tag::Mutation;
use homebase_core::writer::Writer;
use rusqlite::{Connection, OptionalExtension, ToSql, params_from_iter};
use uuid::{Uuid, Variant, Version};

#[cfg(test)]
use super::codes;
#[cfg(test)]
use super::guard::TargetFamily;
use super::guard::{GuardPlan, GuardReason, LogicalTarget, OperationFamily};
use super::schema::{
    Affinity, Column, ColumnId, CreateTable, ForeignKeyDefinition, ForeignKeyId, IndexId,
    NamedIndex, SchemaRevisionId, SqlName, StrictType, TableId, TableMode, TableStorage,
    active_primary_index_key, write_revision_key,
};
use crate::catalog;
use crate::catalog::CatalogSnapshot;
use crate::commit::footprint::ConflictFootprint;
use crate::sqlite::quote_identifier;
pub(crate) use crate::value::StoredValue;
use crate::{Error, Result};

const ROW_FRAME_VERSION: u8 = 6;
const ROW_IMAGE_FRAME_VERSION: u8 = 1;
const ROW_SET_FRAME_VERSION: u8 = 2;
const ROW_CHANGES_FRAME_VERSION: u8 = 1;
const TABLE_CHANGES_FRAME_VERSION: u8 = 1;
const ROW_DELTA_FRAME_VERSION: u8 = 1;
const TAG_SCHEMA_REVISION: u8 = 1;
const TAG_COLUMN_VALUE: u8 = 4;
const TAG_SET_TABLE: u8 = 1;
const TAG_SET_SCHEMA_REVISION: u8 = 2;
const TAG_SET_PRIMARY_INDEX: u8 = 3;
const TAG_SET_TABLE_STORAGE: u8 = 4;
const TAG_SET_KEY_PART: u8 = 5;
const TAG_SET_INDEX_RULES: u8 = 6;
const TAG_SET_FOREIGN_KEY: u8 = 7;
const TAG_SET_INCOMING_FOREIGN_KEY: u8 = 8;
const TAG_SET_ROW: u8 = 9;
const TAG_COLUMN_ID: u8 = 1;
const TAG_COLUMN_AFFINITY: u8 = 2;
const TAG_KEY_PART_FLAGS: u8 = 3;
const TAG_VALUE: u8 = 2;
const TAG_CHANGED_TABLE: u8 = 1;
const TAG_CHANGE_RULES: u8 = 1;
const TAG_CHANGE_ROW: u8 = 2;
const TAG_DELTA_BEFORE: u8 = 1;
const TAG_DELTA_AFTER: u8 = 2;
const TAG_RULES_INDEX_ID: u8 = 1;
const TAG_RULES_INDEX_PART: u8 = 2;
const TAG_FOREIGN_KEY_ID: u8 = 1;
const TAG_FOREIGN_KEY_PARENT_TABLE: u8 = 2;
const TAG_FOREIGN_KEY_PART: u8 = 4;
const TAG_FOREIGN_KEY_PARENT_INDEX: u8 = 5;
const TAG_FOREIGN_KEY_CHILD_COLUMN: u8 = 1;
const TAG_FOREIGN_KEY_PARENT_PART: u8 = 2;
const TAG_INCOMING_FOREIGN_KEY_ID: u8 = 1;
const TAG_INCOMING_CHILD_TABLE: u8 = 2;
const TAG_INCOMING_CHILD_PRIMARY_INDEX: u8 = 3;
const TAG_INCOMING_PARENT_INDEX: u8 = 5;
const TAG_INCOMING_PARENT_PART: u8 = 6;
const KEY_PART_ROWID_ALIAS: u8 = 1;

/// Maximum direct row events retained for one SQLite statement.
pub(crate) const MAX_CAPTURED_CHANGES: usize = 100_000;
/// Maximum encoded bytes retained by one logical row operation.
pub(crate) const MAX_ROW_OPERATION_BYTES: usize = 64 * 1024 * 1024;

/// One complete SQLite row image observed after affinity and generated values ran.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CapturedRow {
    pub table: String,
    pub rowid: i64,
    pub values: Vec<StoredValue>,
}

/// One application-row change observed by SQLite's preupdate hook, including
/// nested foreign-key and trigger effects.
///
/// SQLite selects and executes referential actions on the writable branch.
/// Multilite compiles the resulting direct and indirect row events; replay and
/// repair therefore never dispatch foreign-key actions a second time.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CapturedChange {
    Insert(CapturedRow),
    Delete(CapturedRow),
    Update {
        before: CapturedRow,
        after: CapturedRow,
    },
}

/// Per-statement memory fence applied inside the SQLite preupdate hook.
#[derive(Clone, Debug)]
pub(crate) struct CaptureBudget {
    changes: usize,
    bytes: usize,
    max_changes: usize,
    max_bytes: usize,
}

impl Default for CaptureBudget {
    fn default() -> Self {
        Self {
            changes: 0,
            bytes: 0,
            max_changes: MAX_CAPTURED_CHANGES,
            max_bytes: MAX_ROW_OPERATION_BYTES,
        }
    }
}

impl CaptureBudget {
    pub(crate) fn reset(&mut self) {
        self.changes = 0;
        self.bytes = 0;
    }

    pub(crate) fn record(&mut self, change: &CapturedChange) -> Result<()> {
        let changes = self
            .changes
            .checked_add(1)
            .ok_or_else(|| capture_limit("row-change count", self.max_changes))?;
        if changes > self.max_changes {
            return Err(capture_limit("row-change count", self.max_changes));
        }
        let bytes = self
            .bytes
            .checked_add(change.retained_bytes()?)
            .ok_or_else(|| capture_limit("row-capture bytes", self.max_bytes))?;
        if bytes > self.max_bytes {
            return Err(capture_limit("row-capture bytes", self.max_bytes));
        }
        self.changes = changes;
        self.bytes = bytes;
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn with_limits(max_changes: usize, max_bytes: usize) -> Self {
        Self {
            max_changes,
            max_bytes,
            ..Self::default()
        }
    }
}

impl CapturedChange {
    fn retained_bytes(&self) -> Result<usize> {
        match self {
            Self::Insert(row) | Self::Delete(row) => row.retained_bytes(),
            Self::Update { before, after } => before
                .retained_bytes()?
                .checked_add(after.retained_bytes()?)
                .ok_or_else(|| capture_limit("row-capture bytes", MAX_ROW_OPERATION_BYTES)),
        }
    }
}

impl CapturedRow {
    fn retained_bytes(&self) -> Result<usize> {
        self.values.iter().try_fold(
            self.table
                .len()
                .checked_add(std::mem::size_of::<i64>())
                .ok_or_else(|| capture_limit("row-capture bytes", MAX_ROW_OPERATION_BYTES))?,
            |bytes, value| {
                bytes
                    .checked_add(value.encoded_len())
                    .ok_or_else(|| capture_limit("row-capture bytes", MAX_ROW_OPERATION_BYTES))
            },
        )
    }
}

fn decode_stored_value(frame: &[u8]) -> std::result::Result<StoredValue, RowCodecError> {
    StoredValue::decode(frame).map_err(|error| match error {
        crate::value::StoredValueCodecError::Truncated => RowCodecError::Truncated,
        crate::value::StoredValueCodecError::InvalidLength => RowCodecError::InvalidLength,
        crate::value::StoredValueCodecError::InvalidKind(_) => RowCodecError::InvalidValue,
    })
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct KeyPartRules {
    pub column: ColumnId,
    pub affinity: Affinity,
    pub rowid_alias: bool,
}

/// Ordered comparison rules for one table-owned UNIQUE index.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IndexRules {
    index: IndexId,
    parts: Vec<KeyPartRules>,
}

/// One child-to-parent key mapping used for reverse-reference assertions.
#[derive(Clone, Debug, PartialEq, Eq)]
struct ForeignKeyRules {
    id: ForeignKeyId,
    parent_table: TableId,
    parent_index: IndexId,
    key_parts: Vec<ForeignKeyPartRules>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ForeignKeyPartRules {
    child_column: ColumnId,
    parent: KeyPartRules,
}

/// One child primary index that may prevent deletion of this table's rows.
#[derive(Clone, Debug, PartialEq, Eq)]
struct IncomingForeignKeyRules {
    id: ForeignKeyId,
    child_table: TableId,
    child_primary_index: IndexId,
    parent_index: IndexId,
    parent_key_parts: Vec<KeyPartRules>,
}

struct ForeignReference {
    key: Key,
    owner: Vec<u8>,
}

struct IndexEntry {
    key: Key,
    value: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Row {
    values: Vec<(ColumnId, StoredValue)>,
}

impl Row {
    fn value(&self, column: ColumnId) -> Option<&StoredValue> {
        self.values
            .iter()
            .find(|(candidate, _)| *candidate == column)
            .map(|(_, value)| value)
    }

    fn value_mut(&mut self, column: ColumnId) -> Option<&mut StoredValue> {
        self.values
            .iter_mut()
            .find(|(candidate, _)| *candidate == column)
            .map(|(_, value)| value)
    }
}

/// Immutable interpretation rules shared by every row in one operation.
#[derive(Clone, Debug, PartialEq, Eq)]
struct RowRules {
    table: TableId,
    schema_revision: SchemaRevisionId,
    primary_index: IndexId,
    storage: TableStorage,
    key_parts: Vec<KeyPartRules>,
    indexes: Vec<IndexRules>,
    foreign_keys: Vec<ForeignKeyRules>,
    incoming_foreign_keys: Vec<IncomingForeignKeyRules>,
}

/// Homogeneous row images interpreted under one table's stable rules.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RowSet {
    rules: RowRules,
    rows: Vec<Row>,
}

/// One logical multi-row DELETE carrying the complete removed row images.
#[cfg(test)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeletedRowsFixture {
    deleted: RowSet,
}

/// One logical multi-row UPDATE carrying complete before and after row images.
#[cfg(test)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UpdatedRowsFixture {
    before: RowSet,
    after: RowSet,
}

/// Net row effects of one SQLite statement after repeated touches are folded.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RowChanges {
    tables: CanonicalTableChanges,
}

/// Non-empty table transitions in stable table-id order.
#[derive(Clone, Debug, PartialEq, Eq)]
struct CanonicalTableChanges(Vec<TableChanges>);

/// One table's rules and row-lineage-preserving before/after images.
#[derive(Clone, Debug, PartialEq, Eq)]
struct TableChanges {
    rules: RowRules,
    rows: Vec<RowDelta>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct RowDelta {
    before: Option<Row>,
    after: Option<Row>,
}

impl CanonicalTableChanges {
    fn new(tables: Vec<TableChanges>) -> std::result::Result<Self, RowCodecError> {
        let canonical = Self(tables);
        canonical.validate()?;
        Ok(canonical)
    }

    fn singleton(table: TableChanges) -> Self {
        debug_assert!(table.validate_structure().is_ok());
        Self(vec![table])
    }

    fn validate(&self) -> std::result::Result<(), RowCodecError> {
        if self.0.is_empty()
            || self
                .0
                .windows(2)
                .any(|pair| pair[0].rules.table.as_bytes() >= pair[1].rules.table.as_bytes())
        {
            return Err(RowCodecError::InvalidRow);
        }
        for table in &self.0 {
            table.validate_structure()?;
        }
        Ok(())
    }
}

impl std::ops::Deref for CanonicalTableChanges {
    type Target = [TableChanges];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<'a> IntoIterator for &'a CanonicalTableChanges {
    type Item = &'a TableChanges;
    type IntoIter = std::slice::Iter<'a, TableChanges>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}

/// Homebase mutations and conflict footprint for one row operation.
pub struct RowHomebaseOp {
    pub mutations: Vec<Mutation>,
    pub footprint: ConflictFootprint,
    pub guards: GuardPlan,
}

/// One durable entry created while a new index scans existing rows.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IndexBackfillEntry {
    pub key: Key,
    pub value: Vec<u8>,
}

impl RowRules {
    fn from_table(catalog: &CatalogSnapshot, created: &CreateTable) -> Result<Self> {
        Ok(Self {
            table: created.table_id(),
            schema_revision: created.schema_revision_id(),
            primary_index: created.primary_index_id(),
            storage: created.storage(),
            key_parts: created
                .primary_key_columns()
                .map(|column| KeyPartRules {
                    column: column.id(),
                    affinity: column.affinity(created.mode()),
                    rowid_alias: created.is_rowid_alias(column.id()),
                })
                .collect(),
            indexes: index_rules(created),
            foreign_keys: foreign_key_rules(catalog, created)?,
            incoming_foreign_keys: incoming_foreign_key_rules(catalog, created)?,
        })
    }

    fn capture(&self, created: &CreateTable, captured: &CapturedRow) -> Result<Row> {
        if captured.values.len() != created.columns().len() {
            return Err(Error::CaptureInvariant(
                "captured row width does not match its schema catalog",
            ));
        }
        let values = created
            .columns()
            .iter()
            .zip(&captured.values)
            .map(|(column, value)| {
                (
                    column.id(),
                    normalize_captured_value(value, column.affinity(created.mode())),
                )
            })
            .collect::<Vec<_>>();
        if self.storage == TableStorage::Rowid {
            let alias = created
                .primary_key_columns()
                .next()
                .filter(|column| created.is_rowid_alias(column.id()))
                .ok_or(Error::CaptureInvariant(
                    "rowid table is missing its INTEGER PRIMARY KEY alias",
                ))?;
            if !values.iter().any(|(column, value)| {
                *column == alias.id() && *value == StoredValue::Integer(captured.rowid)
            }) {
                return Err(Error::CaptureInvariant(
                    "captured rowid contradicts its INTEGER PRIMARY KEY",
                ));
            }
        }
        let row = Row { values };
        if created.mode() == TableMode::Strict
            && created.columns().iter().any(|column| {
                row.value(column.id())
                    .is_some_and(|value| !strict_value_matches(column, value))
            })
        {
            return Err(Error::InvalidMultiliteOp(
                "row value has an invalid STRICT storage class".into(),
            ));
        }
        self.key_images(&row).map_err(|error| {
            Error::InvalidMultiliteOp(format!("invalid primary key image: {error}"))
        })?;
        self.index_entries(&row).map_err(|error| {
            Error::InvalidMultiliteOp(format!("invalid UNIQUE key image: {error}"))
        })?;
        self.foreign_references(&row).map_err(|error| {
            Error::InvalidMultiliteOp(format!("invalid foreign-key image: {error}"))
        })?;
        Ok(row)
    }

    fn encode(&self) -> Vec<u8> {
        let mut writer = Writer::new();
        self.encode_into(&mut writer);
        writer.finish()
    }

    fn encode_into(&self, writer: &mut Writer) {
        writer.u8(ROW_SET_FRAME_VERSION);
        writer
            .field(TAG_SET_TABLE, &self.table.as_bytes())
            .expect("row field length fits in u32");
        writer
            .field(TAG_SET_SCHEMA_REVISION, &self.schema_revision.as_bytes())
            .expect("row field length fits in u32");
        writer
            .field(TAG_SET_PRIMARY_INDEX, &self.primary_index.as_bytes())
            .expect("row field length fits in u32");
        writer
            .field(TAG_SET_TABLE_STORAGE, &[self.storage.to_u8()])
            .expect("row field length fits in u32");
        for part in &self.key_parts {
            writer
                .field(TAG_SET_KEY_PART, &encode_key_part(*part))
                .expect("row field length fits in u32");
        }
        for index in &self.indexes {
            writer
                .field(TAG_SET_INDEX_RULES, &encode_index_rules(index))
                .expect("row field length fits in u32");
        }
        for foreign_key in &self.foreign_keys {
            writer
                .field(TAG_SET_FOREIGN_KEY, &encode_foreign_key(foreign_key))
                .expect("row field length fits in u32");
        }
        for incoming in &self.incoming_foreign_keys {
            writer
                .field(
                    TAG_SET_INCOMING_FOREIGN_KEY,
                    &encode_incoming_foreign_key(incoming),
                )
                .expect("row field length fits in u32");
        }
    }

    fn decode(frame: &[u8]) -> std::result::Result<Self, RowCodecError> {
        let (rules, rows) = decode_row_rules(frame, false)?;
        if !rows.is_empty() {
            return Err(RowCodecError::InvalidRow);
        }
        Ok(rules)
    }

    fn with_rows(&self, rows: Vec<Row>) -> RowSet {
        RowSet {
            rules: self.clone(),
            rows,
        }
    }

    fn primary_values<'a>(&self, row: &'a Row) -> Result<Vec<&'a StoredValue>> {
        self.key_parts
            .iter()
            .map(|part| {
                row.value(part.column).ok_or(Error::InvalidDatabase(
                    "pending row is missing a primary-key value",
                ))
            })
            .collect()
    }

    fn validate(&self) -> std::result::Result<(), RowCodecError> {
        if self.key_parts.is_empty() {
            return Err(RowCodecError::InvalidRow);
        }
        if self.key_parts.iter().enumerate().any(|(index, part)| {
            self.key_parts[..index]
                .iter()
                .any(|seen| seen.column == part.column)
        }) || (self.key_parts.iter().any(|part| part.rowid_alias)
            && !matches!(
                self.key_parts.as_slice(),
                [KeyPartRules {
                    affinity: Affinity::Integer,
                    rowid_alias: true,
                    ..
                }]
            ))
        {
            return Err(RowCodecError::DuplicateField);
        }
        if self.indexes.iter().enumerate().any(|(position, index)| {
            index.parts.is_empty()
                || index.parts.iter().any(|part| part.rowid_alias)
                || self.indexes[..position]
                    .iter()
                    .any(|seen| seen.index == index.index)
                || index.parts.iter().enumerate().any(|(part_index, part)| {
                    index.parts[..part_index]
                        .iter()
                        .any(|seen| seen.column == part.column)
                })
        }) {
            return Err(RowCodecError::InvalidRow);
        }
        if self
            .foreign_keys
            .iter()
            .enumerate()
            .any(|(index, foreign_key)| {
                foreign_key.key_parts.is_empty()
                    || self.foreign_keys[..index]
                        .iter()
                        .any(|seen| seen.id == foreign_key.id)
                    || foreign_key
                        .key_parts
                        .iter()
                        .enumerate()
                        .any(|(part_index, part)| {
                            foreign_key.key_parts[..part_index]
                                .iter()
                                .any(|seen| seen.child_column == part.child_column)
                        })
            })
            || self
                .incoming_foreign_keys
                .iter()
                .enumerate()
                .any(|(index, incoming)| {
                    self.incoming_foreign_keys[..index]
                        .iter()
                        .any(|seen| seen.id == incoming.id)
                        || incoming.parent_key_parts.is_empty()
                        || incoming
                            .parent_key_parts
                            .iter()
                            .enumerate()
                            .any(|(part_index, part)| {
                                incoming.parent_key_parts[..part_index]
                                    .iter()
                                    .any(|seen| seen.column == part.column)
                            })
                })
        {
            return Err(RowCodecError::InvalidRow);
        }
        Ok(())
    }

    fn row_key(&self, row: &Row) -> std::result::Result<Key, RowCodecError> {
        row_prefix(self.table, self.primary_index, self.key_images(row)?)
    }

    fn key_images(&self, row: &Row) -> std::result::Result<Vec<Vec<u8>>, RowCodecError> {
        self.key_parts
            .iter()
            .map(|part| {
                row.value(part.column)
                    .ok_or(RowCodecError::InvalidRow)
                    .and_then(|value| key_image(value, *part))
            })
            .collect()
    }

    fn index_entries(&self, row: &Row) -> std::result::Result<Vec<IndexEntry>, RowCodecError> {
        let owner = self.row_key(row)?.encode();
        let mut entries = Vec::with_capacity(self.indexes.len());
        for index in &self.indexes {
            let values = index
                .parts
                .iter()
                .map(|part| row.value(part.column).ok_or(RowCodecError::InvalidRow))
                .collect::<std::result::Result<Vec<_>, _>>()?;
            if values
                .iter()
                .any(|value| matches!(value, StoredValue::Null))
            {
                continue;
            }
            let images = values
                .into_iter()
                .zip(&index.parts)
                .map(|(value, rules)| index_image(value, *rules))
                .collect::<std::result::Result<Vec<_>, _>>()?;
            entries.push(IndexEntry {
                key: unique_prefix(self.table, index.index, images)?,
                value: owner.clone(),
            });
        }
        Ok(entries)
    }

    fn foreign_references(
        &self,
        row: &Row,
    ) -> std::result::Result<Vec<ForeignReference>, RowCodecError> {
        let owner = self.row_key(row)?.encode();
        let child_images = self.key_images(row)?;
        let mut references = Vec::with_capacity(self.foreign_keys.len());
        for foreign_key in &self.foreign_keys {
            let values = foreign_key
                .key_parts
                .iter()
                .map(|part| {
                    row.value(part.child_column)
                        .ok_or(RowCodecError::InvalidRow)
                })
                .collect::<std::result::Result<Vec<_>, _>>()?;
            if values
                .iter()
                .any(|value| matches!(value, StoredValue::Null))
            {
                continue;
            }
            let images = values
                .into_iter()
                .zip(&foreign_key.key_parts)
                .map(|(value, part)| key_image(value, part.parent))
                .collect::<std::result::Result<Vec<_>, _>>()?;
            references.push(ForeignReference {
                key: foreign_reference_key(
                    foreign_key.parent_table,
                    foreign_key.id,
                    foreign_key.parent_index,
                    images,
                    self.primary_index,
                    child_images.clone(),
                )?,
                owner: owner.clone(),
            });
        }
        Ok(references)
    }

    fn incoming_reference_prefixes(
        &self,
        row: &Row,
    ) -> std::result::Result<Vec<Key>, RowCodecError> {
        let mut prefixes = Vec::with_capacity(self.incoming_foreign_keys.len());
        for incoming in &self.incoming_foreign_keys {
            let values = incoming
                .parent_key_parts
                .iter()
                .map(|part| row.value(part.column).ok_or(RowCodecError::InvalidRow))
                .collect::<std::result::Result<Vec<_>, _>>()?;
            if values
                .iter()
                .any(|value| matches!(value, StoredValue::Null))
            {
                continue;
            }
            let images = values
                .into_iter()
                .zip(&incoming.parent_key_parts)
                .map(|(value, rules)| key_image(value, *rules))
                .collect::<std::result::Result<Vec<_>, _>>()?;
            prefixes.push(foreign_reference_prefix(
                self.table,
                incoming.id,
                incoming.parent_index,
                images,
            )?);
        }
        Ok(prefixes)
    }

    fn encode_row(&self, row: &Row) -> Vec<u8> {
        let mut writer = Writer::new();
        writer.u8(ROW_FRAME_VERSION);
        writer
            .field(TAG_SCHEMA_REVISION, &self.schema_revision.as_bytes())
            .expect("row field length fits in u32");
        for (column, value) in &row.values {
            writer
                .field(TAG_COLUMN_VALUE, &encode_column_value(*column, value))
                .expect("row field length fits in u32");
        }
        writer.finish()
    }
}

fn decode_row_rules(
    frame: &[u8],
    allow_rows: bool,
) -> std::result::Result<(RowRules, Vec<Row>), RowCodecError> {
    let mut reader = Reader::new(frame);
    if reader.u8() != Some(ROW_SET_FRAME_VERSION) {
        return Err(RowCodecError::UnknownVersion);
    }
    let mut table = None;
    let mut schema_revision = None;
    let mut primary_index = None;
    let mut storage = None;
    let mut key_parts = Vec::new();
    let mut indexes = Vec::new();
    let mut foreign_keys = Vec::new();
    let mut incoming_foreign_keys = Vec::new();
    let mut rows = Vec::new();
    while let Some((tag, value)) = reader.field().map_err(|_| RowCodecError::Truncated)? {
        match tag {
            TAG_SET_TABLE => set_once(&mut table, TableId::from_bytes(uuid_bytes(value)?))?,
            TAG_SET_SCHEMA_REVISION => set_once(
                &mut schema_revision,
                SchemaRevisionId::from_bytes(uuid_bytes(value)?),
            )?,
            TAG_SET_PRIMARY_INDEX => {
                set_once(&mut primary_index, IndexId::from_bytes(uuid_bytes(value)?))?
            }
            TAG_SET_TABLE_STORAGE => {
                let [value] = value else {
                    return Err(RowCodecError::InvalidLength);
                };
                set_once(
                    &mut storage,
                    TableStorage::from_u8(*value).ok_or(RowCodecError::InvalidRow)?,
                )?;
            }
            TAG_SET_KEY_PART => key_parts.push(decode_key_part(value)?),
            TAG_SET_INDEX_RULES => indexes.push(decode_index_rules(value)?),
            TAG_SET_FOREIGN_KEY => foreign_keys.push(decode_foreign_key(value)?),
            TAG_SET_INCOMING_FOREIGN_KEY => {
                incoming_foreign_keys.push(decode_incoming_foreign_key(value)?)
            }
            TAG_SET_ROW if allow_rows => rows.push(decode_row_image(value)?),
            TAG_SET_ROW => return Err(RowCodecError::InvalidRow),
            _ => {}
        }
    }
    let rules = RowRules {
        table: table.ok_or(RowCodecError::MissingField(TAG_SET_TABLE))?,
        schema_revision: schema_revision
            .ok_or(RowCodecError::MissingField(TAG_SET_SCHEMA_REVISION))?,
        primary_index: primary_index.ok_or(RowCodecError::MissingField(TAG_SET_PRIMARY_INDEX))?,
        storage: storage.ok_or(RowCodecError::MissingField(TAG_SET_TABLE_STORAGE))?,
        key_parts,
        indexes,
        foreign_keys,
        incoming_foreign_keys,
    };
    rules.validate()?;
    Ok((rules, rows))
}

impl RowSet {
    pub fn from_captured(
        connection: &Connection,
        captured: &[CapturedRow],
    ) -> Result<Option<Self>> {
        let catalog = CatalogSnapshot::load(connection)?;
        Self::from_catalog(&catalog, captured)
    }

    pub(crate) fn from_catalog(
        catalog: &CatalogSnapshot,
        captured: &[CapturedRow],
    ) -> Result<Option<Self>> {
        let Some(first) = captured.first() else {
            return Ok(None);
        };
        if captured.iter().any(|row| row.table != first.table) {
            return Err(Error::CaptureInvariant(
                "one row operation changed more than one table",
            ));
        }
        let Some(created) = catalog.by_name(&first.table) else {
            return Ok(None);
        };
        let rules = RowRules::from_table(catalog, created)?;
        let rows = captured
            .iter()
            .map(|captured| rules.capture(created, captured))
            .collect::<Result<Vec<_>>>()?;
        let inserted = Self { rules, rows };
        inserted.validate_against_catalog(catalog, created)?;
        Ok(Some(inserted))
    }

    #[cfg(test)]
    pub fn to_homebase(&self) -> Result<RowHomebaseOp> {
        self.validate_structure()
            .map_err(|error| Error::InvalidMultiliteOp(error.to_string()))?;
        let mut mutations = Vec::with_capacity(
            self.rows.len() * (self.rules.indexes.len() + self.rules.foreign_keys.len() + 1),
        );
        let mut guards = GuardPlan::for_operation(OperationFamily::RowChanges);
        for row in &self.rows {
            let key = self
                .row_key(row)
                .map_err(|error| Error::InvalidMultiliteOp(error.to_string()))?;
            guards.invariant(key.clone(), GuardReason::RowIdentity)?;
            guards.write(key.clone(), GuardReason::RowIdentity)?;
            mutations.push(Mutation::Set {
                key,
                value: self.rules.encode_row(row),
            });
        }
        for row in &self.rows {
            for entry in self
                .index_entries(row)
                .map_err(|error| Error::InvalidMultiliteOp(error.to_string()))?
            {
                guards.invariant(entry.key.clone(), GuardReason::UniqueOwnership)?;
                guards.write(entry.key.clone(), GuardReason::UniqueOwnership)?;
                mutations.push(Mutation::Set {
                    key: entry.key,
                    value: entry.value,
                });
            }
            for reference in self
                .foreign_references(row)
                .map_err(|error| Error::InvalidMultiliteOp(error.to_string()))?
            {
                guards.invariant(reference.key.clone(), GuardReason::ForeignReference)?;
                guards.write(reference.key.clone(), GuardReason::ForeignReference)?;
                mutations.push(Mutation::Set {
                    key: reference.key,
                    value: reference.owner,
                });
            }
        }
        guards.invariant(
            active_primary_index_key(self.rules.table),
            GuardReason::PrimaryIndex,
        )?;
        guards.invariant(
            write_revision_key(self.rules.table),
            GuardReason::WriteContract,
        )?;
        let footprint = guards.footprint();
        Ok(RowHomebaseOp {
            mutations,
            footprint,
            guards,
        })
    }

    #[cfg(test)]
    fn validate_homebase(
        &self,
        batch: &AdmittedBatch<Vec<u8>>,
    ) -> std::result::Result<(), RowCodecError> {
        batch.validate().map_err(|_| RowCodecError::InvalidBatch)?;
        let expected = self
            .to_homebase()
            .map_err(|_| RowCodecError::InvalidBatch)?
            .mutations;
        if expected.len() != batch.entries.len()
            || expected
                .iter()
                .zip(&batch.entries)
                .any(|(expected, admitted)| expected != &admitted.device_entry.mutation)
        {
            return Err(RowCodecError::InvalidBatch);
        }
        Ok(())
    }

    #[cfg(test)]
    pub fn encode(&self) -> Vec<u8> {
        let mut writer = Writer::new();
        self.rules.encode_into(&mut writer);
        for row in &self.rows {
            writer
                .field(TAG_SET_ROW, &encode_row_image(row))
                .expect("row field length fits in u32");
        }
        writer.finish()
    }

    #[cfg(test)]
    pub fn decode(frame: &[u8]) -> std::result::Result<Self, RowCodecError> {
        let (rules, rows) = decode_row_rules(frame, true)?;
        if rows.is_empty() {
            return Err(RowCodecError::MissingField(TAG_SET_ROW));
        }
        let operation = Self { rules, rows };
        operation.validate_structure()?;
        Ok(operation)
    }

    fn without_rows(self) -> RowRules {
        self.rules
    }

    pub fn primary_values<'a>(&self, row: &'a Row) -> Result<Vec<&'a StoredValue>> {
        self.rules.primary_values(row)
    }

    pub fn apply(&self, connection: &Connection) -> Result<()> {
        let created = self.catalog_definition(connection)?;
        let table_name = materialized_table_name(connection, self.rules.table)?;
        let columns = created
            .columns()
            .iter()
            .filter(|column| self.rows[0].value(column.id()).is_some())
            .collect::<Vec<_>>();
        let column_names = columns
            .iter()
            .map(|column| materialized_column_name(connection, self.rules.table, column.id()))
            .collect::<Result<Vec<_>>>()?;
        let names = column_names
            .iter()
            .map(|name| quote_identifier(name.value()))
            .collect::<Vec<_>>()
            .join(", ");
        let placeholders = std::iter::repeat_n("?", columns.len())
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            "INSERT INTO {} ({names}) VALUES ({placeholders})",
            quote_identifier(&table_name)
        );
        let mut statement = connection.prepare(&sql)?;
        for row in &self.rows {
            let values = columns
                .iter()
                .map(|column| {
                    row.value(column.id()).ok_or(Error::InvalidMultiliteOp(
                        "row is missing a schema column".into(),
                    ))
                })
                .collect::<Result<Vec<_>>>()?;
            statement.execute(params_from_iter(values))?;
        }
        Ok(())
    }

    #[cfg(test)]
    pub fn delete_materialized(&self, connection: &Connection) -> Result<()> {
        self.delete_materialized_with(
            connection,
            "pending INSERT row no longer matches SQLite state",
        )
    }

    fn delete_materialized_with(
        &self,
        connection: &Connection,
        mismatch: &'static str,
    ) -> Result<()> {
        let created = self.catalog_definition(connection)?;
        let table_name = materialized_table_name(connection, self.rules.table)?;
        self.validate_materialized_against(connection, &created, mismatch)?;
        let primary = created.primary_key_columns().collect::<Vec<_>>();
        let predicates = primary
            .iter()
            .map(|column| materialized_column_name(connection, self.rules.table, column.id()))
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .map(|name| format!("{} = ?", quote_identifier(name.value())))
            .collect::<Vec<_>>();
        let sql = format!(
            "DELETE FROM {} WHERE {}",
            quote_identifier(&table_name),
            predicates.join(" AND ")
        );
        let mut delete = connection.prepare(&sql)?;
        for row in self.rows.iter().rev() {
            if delete.execute(params_from_iter(self.primary_values(row)?))? != 1 {
                return Err(Error::InvalidDatabase(mismatch));
            }
        }
        Ok(())
    }

    fn validate_materialized_against(
        &self,
        connection: &Connection,
        created: &CreateTable,
        mismatch: &'static str,
    ) -> Result<()> {
        self.materialized_rows(connection, created, mismatch)
            .map(|_| ())
    }

    #[cfg(debug_assertions)]
    fn validate_materialized_absent(
        &self,
        connection: &Connection,
        mismatch: &'static str,
    ) -> Result<()> {
        let created = self.catalog_definition(connection)?;
        let table_name = materialized_table_name(connection, self.rules.table)?;
        let predicates = created
            .primary_key_columns()
            .map(|column| materialized_column_name(connection, self.rules.table, column.id()))
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .map(|name| format!("{} = ?", quote_identifier(name.value())))
            .collect::<Vec<_>>()
            .join(" AND ");
        let sql = format!(
            "SELECT 1 FROM {} WHERE {predicates}",
            quote_identifier(&table_name)
        );
        let mut select = connection.prepare(&sql)?;
        for row in &self.rows {
            if select
                .query_row(params_from_iter(self.primary_values(row)?), |_| Ok(()))
                .optional()?
                .is_some()
            {
                return Err(Error::InvalidDatabase(mismatch));
            }
        }
        Ok(())
    }

    fn materialized_rows(
        &self,
        connection: &Connection,
        created: &CreateTable,
        mismatch: &'static str,
    ) -> Result<Vec<Row>> {
        let table_name = materialized_table_name(connection, self.rules.table)?;
        let columns = created.columns();
        let primary = created.primary_key_columns().collect::<Vec<_>>();
        let predicates = primary
            .iter()
            .map(|column| materialized_column_name(connection, self.rules.table, column.id()))
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .map(|name| format!("{} = ?", quote_identifier(name.value())))
            .collect::<Vec<_>>();
        let predicate = predicates.join(" AND ");
        let selected = catalog::column_names(connection, created)?
            .iter()
            .map(|name| quote_identifier(name.value()))
            .collect::<Vec<_>>();
        let select_sql = format!(
            "SELECT {} FROM {} WHERE {predicate}",
            selected.join(", "),
            quote_identifier(&table_name)
        );
        let mut select = connection.prepare(&select_sql)?;
        let mut materialized = Vec::with_capacity(self.rows.len());
        for row in self.rows.iter().rev() {
            let actual = select
                .query_row(params_from_iter(self.primary_values(row)?), |result| {
                    let values = (0..columns.len())
                        .zip(columns)
                        .map(|(index, column)| {
                            result
                                .get_ref(index)
                                .map(StoredValue::capture)
                                .map(|value| (column.id(), value))
                        })
                        .collect::<rusqlite::Result<Vec<_>>>()?;
                    Ok(Row { values })
                })
                .optional()?;
            let Some(actual) = actual else {
                return Err(Error::InvalidDatabase(mismatch));
            };
            if row.values.iter().any(|(column, expected)| {
                actual
                    .value(*column)
                    .is_some_and(|actual| actual != expected)
            }) {
                return Err(Error::InvalidDatabase(mismatch));
            }
            materialized.push(actual);
        }
        materialized.reverse();
        Ok(materialized)
    }

    fn catalog_definition(&self, connection: &Connection) -> Result<CreateTable> {
        let catalog = CatalogSnapshot::load(connection)?;
        let created = catalog
            .by_id(self.rules.table)
            .cloned()
            .ok_or(Error::InvalidDatabase(
                "row operation references an unknown table",
            ))?;
        self.validate_against_catalog(&catalog, &created)?;
        Ok(created)
    }

    fn for_current_rows(
        connection: &Connection,
        created: &CreateTable,
        rows: Vec<Row>,
    ) -> Result<Self> {
        let table = materialized_table_name(connection, created.table_id())?;
        let captured = rows
            .into_iter()
            .map(|row| {
                let values = created
                    .columns()
                    .iter()
                    .map(|column| {
                        row.value(column.id())
                            .cloned()
                            .ok_or(Error::InvalidDatabase(
                                "materialized row is missing a current schema column",
                            ))
                    })
                    .collect::<Result<Vec<_>>>()?;
                Ok(CapturedRow {
                    table: table.clone(),
                    rowid: rowid_from_declared_primary_key(created, &row)?,
                    values,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        Self::from_captured(connection, &captured)?.ok_or(Error::CaptureInvariant(
            "materialized row projection unexpectedly became empty",
        ))
    }

    fn validate_against_catalog(
        &self,
        catalog: &CatalogSnapshot,
        created: &CreateTable,
    ) -> Result<()> {
        self.validate_structure()
            .map_err(|error| Error::InvalidMultiliteOp(error.to_string()))?;
        let expected_key_parts = created
            .primary_key_columns()
            .map(|column| KeyPartRules {
                column: column.id(),
                affinity: column.affinity(created.mode()),
                rowid_alias: created.is_rowid_alias(column.id()),
            })
            .collect::<Vec<_>>();
        let expected_indexes = index_rules(created);
        let known_indexes = known_index_rules(created);
        let expected_foreign_keys = foreign_key_rules(catalog, created)?;
        let expected_incoming_foreign_keys = incoming_foreign_key_rules(catalog, created)?;
        if self.rules.table != created.table_id()
            || self.rules.primary_index != created.primary_index_id()
            || self.rules.storage != created.storage()
            || self.rules.key_parts != expected_key_parts
            || expected_indexes
                .iter()
                .any(|expected| !self.rules.indexes.contains(expected))
            || self
                .rules
                .indexes
                .iter()
                .any(|actual| !known_indexes.contains(actual))
            || self.rules.foreign_keys != expected_foreign_keys
            || self.rules.incoming_foreign_keys != expected_incoming_foreign_keys
        {
            return Err(Error::InvalidMultiliteOp(
                "row operation contradicts the local schema catalog".into(),
            ));
        }
        for row in &self.rows {
            if created.mode() == TableMode::Strict
                && created.columns().iter().any(|column| {
                    row.value(column.id())
                        .is_some_and(|value| !strict_value_matches(column, value))
                })
            {
                return Err(Error::InvalidMultiliteOp(
                    "row value has an invalid STRICT storage class".into(),
                ));
            }
            self.key_images(row).map_err(|error| {
                Error::InvalidMultiliteOp(format!("invalid primary key image: {error}"))
            })?;
            self.index_entries(row).map_err(|error| {
                Error::InvalidMultiliteOp(format!("invalid UNIQUE key image: {error}"))
            })?;
            self.foreign_references(row).map_err(|error| {
                Error::InvalidMultiliteOp(format!("invalid foreign-key image: {error}"))
            })?;
        }
        Ok(())
    }

    fn validate_structure(&self) -> std::result::Result<(), RowCodecError> {
        self.validate_rules()?;
        if self.rows.is_empty() {
            return Err(RowCodecError::InvalidRow);
        }
        let columns = self.rows[0]
            .values
            .iter()
            .map(|(column, _)| *column)
            .collect::<Vec<_>>();
        if self.rows.iter().any(|row| {
            row.values.len() != columns.len()
                || row
                    .values
                    .iter()
                    .zip(&columns)
                    .any(|((column, _), expected)| column != expected)
        }) {
            return Err(RowCodecError::InvalidRow);
        }
        let mut keys = BTreeSet::new();
        let mut indexes = BTreeSet::new();
        for row in &self.rows {
            if !keys.insert(self.row_key(row)?) {
                return Err(RowCodecError::DuplicateRow);
            }
            for entry in self.index_entries(row)? {
                if !indexes.insert(entry.key) {
                    return Err(RowCodecError::DuplicateUniqueKey);
                }
            }
            self.foreign_references(row)?;
        }
        Ok(())
    }

    fn validate_rules(&self) -> std::result::Result<(), RowCodecError> {
        self.rules.validate()
    }

    fn row_key(&self, row: &Row) -> std::result::Result<Key, RowCodecError> {
        self.rules.row_key(row)
    }

    fn key_images(&self, row: &Row) -> std::result::Result<Vec<Vec<u8>>, RowCodecError> {
        self.rules.key_images(row)
    }

    fn index_entries(&self, row: &Row) -> std::result::Result<Vec<IndexEntry>, RowCodecError> {
        self.rules.index_entries(row)
    }

    fn index_entry_map(&self) -> std::result::Result<BTreeMap<Key, Vec<u8>>, RowCodecError> {
        let mut entries = BTreeMap::new();
        for row in &self.rows {
            for entry in self.index_entries(row)? {
                if entries.insert(entry.key, entry.value).is_some() {
                    return Err(RowCodecError::DuplicateUniqueKey);
                }
            }
        }
        Ok(entries)
    }

    fn foreign_reference_map(&self) -> std::result::Result<BTreeMap<Key, Vec<u8>>, RowCodecError> {
        let mut references = BTreeMap::new();
        for row in &self.rows {
            for reference in self.foreign_references(row)? {
                if references.insert(reference.key, reference.owner).is_some() {
                    return Err(RowCodecError::InvalidRow);
                }
            }
        }
        Ok(references)
    }

    fn foreign_references(
        &self,
        row: &Row,
    ) -> std::result::Result<Vec<ForeignReference>, RowCodecError> {
        self.rules.foreign_references(row)
    }

    fn incoming_reference_prefixes(
        &self,
        row: &Row,
    ) -> std::result::Result<Vec<Key>, RowCodecError> {
        self.rules.incoming_reference_prefixes(row)
    }

    fn incoming_reference_prefix_set(&self) -> std::result::Result<BTreeSet<Key>, RowCodecError> {
        let mut prefixes = BTreeSet::new();
        for row in &self.rows {
            prefixes.extend(self.incoming_reference_prefixes(row)?);
        }
        Ok(prefixes)
    }
}

#[cfg(test)]
fn decode_stored_row(frame: &[u8]) -> std::result::Result<Row, RowCodecError> {
    let mut reader = Reader::new(frame);
    if reader.u8() != Some(ROW_FRAME_VERSION) {
        return Err(RowCodecError::UnknownVersion);
    }
    let mut schema_revision = None;
    let mut values = Vec::new();
    while let Some((tag, value)) = reader.field().map_err(|_| RowCodecError::Truncated)? {
        match tag {
            TAG_SCHEMA_REVISION => set_once(
                &mut schema_revision,
                SchemaRevisionId::from_bytes(uuid_bytes(value)?),
            )?,
            TAG_COLUMN_VALUE => values.push(decode_column_value(value)?),
            _ => {}
        }
    }
    schema_revision.ok_or(RowCodecError::MissingField(TAG_SCHEMA_REVISION))?;
    validate_row_values(values)
}

fn encode_row_image(row: &Row) -> Vec<u8> {
    let mut writer = Writer::new();
    writer.u8(ROW_IMAGE_FRAME_VERSION);
    for (column, value) in &row.values {
        writer
            .field(TAG_COLUMN_VALUE, &encode_column_value(*column, value))
            .expect("row field length fits in u32");
    }
    writer.finish()
}

fn decode_row_image(frame: &[u8]) -> std::result::Result<Row, RowCodecError> {
    let mut reader = Reader::new(frame);
    if reader.u8() != Some(ROW_IMAGE_FRAME_VERSION) {
        return Err(RowCodecError::UnknownVersion);
    }
    let mut values = Vec::new();
    while let Some((tag, value)) = reader.field().map_err(|_| RowCodecError::Truncated)? {
        if tag == TAG_COLUMN_VALUE {
            values.push(decode_column_value(value)?);
        }
    }
    validate_row_values(values)
}

fn encode_index_rules(index: &IndexRules) -> Vec<u8> {
    let mut writer = Writer::new();
    writer
        .field(TAG_RULES_INDEX_ID, &index.index.as_bytes())
        .expect("row field length fits in u32");
    for part in &index.parts {
        writer
            .field(TAG_RULES_INDEX_PART, &encode_key_part(*part))
            .expect("row field length fits in u32");
    }
    writer.finish()
}

fn decode_index_rules(frame: &[u8]) -> std::result::Result<IndexRules, RowCodecError> {
    let mut reader = Reader::new(frame);
    let mut index = None;
    let mut parts = Vec::new();
    while let Some((tag, value)) = reader.field().map_err(|_| RowCodecError::Truncated)? {
        match tag {
            TAG_RULES_INDEX_ID => set_once(&mut index, IndexId::from_bytes(uuid_bytes(value)?))?,
            TAG_RULES_INDEX_PART => parts.push(decode_key_part(value)?),
            _ => {}
        }
    }
    if parts.is_empty()
        || parts
            .iter()
            .enumerate()
            .any(|(index, part)| parts[..index].iter().any(|seen| seen.column == part.column))
    {
        return Err(RowCodecError::InvalidRow);
    }
    Ok(IndexRules {
        index: index.ok_or(RowCodecError::MissingField(TAG_RULES_INDEX_ID))?,
        parts,
    })
}

fn encode_foreign_key(foreign_key: &ForeignKeyRules) -> Vec<u8> {
    let mut writer = Writer::new();
    writer
        .field(TAG_FOREIGN_KEY_ID, &foreign_key.id.as_bytes())
        .expect("row field length fits in u32");
    writer
        .field(
            TAG_FOREIGN_KEY_PARENT_TABLE,
            &foreign_key.parent_table.as_bytes(),
        )
        .expect("row field length fits in u32");
    writer
        .field(
            TAG_FOREIGN_KEY_PARENT_INDEX,
            &foreign_key.parent_index.as_bytes(),
        )
        .expect("row field length fits in u32");
    for part in &foreign_key.key_parts {
        let mut encoded = Writer::new();
        encoded
            .field(TAG_FOREIGN_KEY_CHILD_COLUMN, &part.child_column.as_bytes())
            .expect("row field length fits in u32");
        encoded
            .field(TAG_FOREIGN_KEY_PARENT_PART, &encode_key_part(part.parent))
            .expect("row field length fits in u32");
        writer
            .field(TAG_FOREIGN_KEY_PART, &encoded.finish())
            .expect("row field length fits in u32");
    }
    writer.finish()
}

fn decode_foreign_key(frame: &[u8]) -> std::result::Result<ForeignKeyRules, RowCodecError> {
    let mut reader = Reader::new(frame);
    let mut id = None;
    let mut parent_table = None;
    let mut parent_index = None;
    let mut key_parts = Vec::new();
    while let Some((tag, value)) = reader.field().map_err(|_| RowCodecError::Truncated)? {
        match tag {
            TAG_FOREIGN_KEY_ID => set_once(&mut id, ForeignKeyId::from_bytes(uuid_bytes(value)?))?,
            TAG_FOREIGN_KEY_PARENT_TABLE => {
                set_once(&mut parent_table, TableId::from_bytes(uuid_bytes(value)?))?
            }
            TAG_FOREIGN_KEY_PARENT_INDEX => {
                set_once(&mut parent_index, IndexId::from_bytes(uuid_bytes(value)?))?
            }
            TAG_FOREIGN_KEY_PART => {
                let mut part_reader = Reader::new(value);
                let mut child_column = None;
                let mut parent = None;
                while let Some((part_tag, part_value)) =
                    part_reader.field().map_err(|_| RowCodecError::Truncated)?
                {
                    match part_tag {
                        TAG_FOREIGN_KEY_CHILD_COLUMN => set_once(
                            &mut child_column,
                            ColumnId::from_bytes(uuid_bytes(part_value)?),
                        )?,
                        TAG_FOREIGN_KEY_PARENT_PART => {
                            set_once(&mut parent, decode_key_part(part_value)?)?
                        }
                        _ => {}
                    }
                }
                key_parts.push(ForeignKeyPartRules {
                    child_column: child_column
                        .ok_or(RowCodecError::MissingField(TAG_FOREIGN_KEY_CHILD_COLUMN))?,
                    parent: parent
                        .ok_or(RowCodecError::MissingField(TAG_FOREIGN_KEY_PARENT_PART))?,
                });
            }
            _ => {}
        }
    }
    if key_parts.is_empty() {
        return Err(RowCodecError::InvalidRow);
    }
    Ok(ForeignKeyRules {
        id: id.ok_or(RowCodecError::MissingField(TAG_FOREIGN_KEY_ID))?,
        parent_table: parent_table
            .ok_or(RowCodecError::MissingField(TAG_FOREIGN_KEY_PARENT_TABLE))?,
        parent_index: parent_index
            .ok_or(RowCodecError::MissingField(TAG_FOREIGN_KEY_PARENT_INDEX))?,
        key_parts,
    })
}

fn encode_incoming_foreign_key(incoming: &IncomingForeignKeyRules) -> Vec<u8> {
    let mut writer = Writer::new();
    writer
        .field(TAG_INCOMING_FOREIGN_KEY_ID, &incoming.id.as_bytes())
        .expect("row field length fits in u32");
    writer
        .field(TAG_INCOMING_CHILD_TABLE, &incoming.child_table.as_bytes())
        .expect("row field length fits in u32");
    writer
        .field(
            TAG_INCOMING_CHILD_PRIMARY_INDEX,
            &incoming.child_primary_index.as_bytes(),
        )
        .expect("row field length fits in u32");
    writer
        .field(TAG_INCOMING_PARENT_INDEX, &incoming.parent_index.as_bytes())
        .expect("row field length fits in u32");
    for part in &incoming.parent_key_parts {
        writer
            .field(TAG_INCOMING_PARENT_PART, &encode_key_part(*part))
            .expect("row field length fits in u32");
    }
    writer.finish()
}

fn decode_incoming_foreign_key(
    frame: &[u8],
) -> std::result::Result<IncomingForeignKeyRules, RowCodecError> {
    let mut reader = Reader::new(frame);
    let mut id = None;
    let mut child_table = None;
    let mut child_primary_index = None;
    let mut parent_index = None;
    let mut parent_key_parts = Vec::new();
    while let Some((tag, value)) = reader.field().map_err(|_| RowCodecError::Truncated)? {
        match tag {
            TAG_INCOMING_FOREIGN_KEY_ID => {
                set_once(&mut id, ForeignKeyId::from_bytes(uuid_bytes(value)?))?
            }
            TAG_INCOMING_CHILD_TABLE => {
                set_once(&mut child_table, TableId::from_bytes(uuid_bytes(value)?))?
            }
            TAG_INCOMING_CHILD_PRIMARY_INDEX => set_once(
                &mut child_primary_index,
                IndexId::from_bytes(uuid_bytes(value)?),
            )?,
            TAG_INCOMING_PARENT_INDEX => {
                set_once(&mut parent_index, IndexId::from_bytes(uuid_bytes(value)?))?
            }
            TAG_INCOMING_PARENT_PART => parent_key_parts.push(decode_key_part(value)?),
            _ => {}
        }
    }
    if parent_key_parts.is_empty() {
        return Err(RowCodecError::InvalidRow);
    }
    Ok(IncomingForeignKeyRules {
        id: id.ok_or(RowCodecError::MissingField(TAG_INCOMING_FOREIGN_KEY_ID))?,
        child_table: child_table.ok_or(RowCodecError::MissingField(TAG_INCOMING_CHILD_TABLE))?,
        child_primary_index: child_primary_index.ok_or(RowCodecError::MissingField(
            TAG_INCOMING_CHILD_PRIMARY_INDEX,
        ))?,
        parent_index: parent_index.ok_or(RowCodecError::MissingField(TAG_INCOMING_PARENT_INDEX))?,
        parent_key_parts,
    })
}

struct TableChangesBuilder {
    rules: RowRules,
    rows: Vec<RowDelta>,
    current: BTreeMap<Key, usize>,
    tombstones: BTreeMap<Key, usize>,
}

impl RowChanges {
    pub(crate) fn inserted(inserted: RowSet) -> Self {
        let rules = inserted.clone().without_rows();
        Self {
            tables: CanonicalTableChanges::singleton(TableChanges {
                rules,
                rows: inserted
                    .rows
                    .into_iter()
                    .map(|after| RowDelta {
                        before: None,
                        after: Some(after),
                    })
                    .collect(),
            }),
        }
    }

    #[cfg(test)]
    pub(crate) fn deleted(deleted: DeletedRowsFixture) -> Self {
        let rules = deleted.deleted.clone().without_rows();
        Self {
            tables: CanonicalTableChanges::singleton(TableChanges {
                rules,
                rows: deleted
                    .deleted
                    .rows
                    .into_iter()
                    .map(|before| RowDelta {
                        before: Some(before),
                        after: None,
                    })
                    .collect(),
            }),
        }
    }

    #[cfg(test)]
    pub(crate) fn updated(updated: UpdatedRowsFixture) -> Self {
        let rules = updated.before.clone().without_rows();
        Self {
            tables: CanonicalTableChanges::singleton(TableChanges {
                rules,
                rows: updated
                    .before
                    .rows
                    .into_iter()
                    .zip(updated.after.rows)
                    .map(|(before, after)| RowDelta {
                        before: Some(before),
                        after: Some(after),
                    })
                    .collect(),
            }),
        }
    }

    /// Fold SQLite's ordered preupdate stream into one durable net statement effect.
    pub(crate) fn from_catalog(
        catalog: &CatalogSnapshot,
        events: Vec<CapturedChange>,
    ) -> Result<Option<Self>> {
        if events.is_empty() {
            return Ok(None);
        }
        let mut builders = BTreeMap::<[u8; 16], TableChangesBuilder>::new();
        for event in events {
            match event {
                CapturedChange::Insert(row) => {
                    let (rules, row, key) = captured_image(
                        catalog,
                        row,
                        "INSERT target has no synchronized schema identity",
                        None,
                    )?;
                    table_builder(&mut builders, rules)?.insert(key, row)?;
                }
                CapturedChange::Delete(row) => {
                    let (rules, row, key) = captured_image(
                        catalog,
                        row,
                        "DELETE target has no synchronized schema identity",
                        None,
                    )?;
                    table_builder(&mut builders, rules)?.delete(key, row)?;
                }
                CapturedChange::Update { before, after } => {
                    let missing = "UPDATE target has no synchronized schema identity";
                    let (before_rules, before, before_key) =
                        captured_image(catalog, before, missing, None)?;
                    let (after_rules, after, after_key) =
                        captured_image(catalog, after, missing, Some(&before_rules))?;
                    if before_rules != after_rules {
                        return Err(Error::CaptureInvariant(
                            "one row update crossed synchronized table identities",
                        ));
                    }
                    table_builder(&mut builders, before_rules)?
                        .update(before_key, before, after_key, after)?;
                }
            }
        }

        let tables = builders
            .into_values()
            .filter_map(|builder| {
                let rows = builder
                    .rows
                    .into_iter()
                    .filter(|change| change.before != change.after)
                    .collect::<Vec<_>>();
                (!rows.is_empty()).then_some(TableChanges {
                    rules: builder.rules,
                    rows,
                })
            })
            .collect::<Vec<_>>();
        if tables.is_empty() {
            return Ok(None);
        }
        let changes = Self {
            tables: CanonicalTableChanges::new(tables)
                .map_err(|error| Error::InvalidMultiliteOp(error.to_string()))?,
        };
        changes.validate_budget()?;
        Ok(Some(changes))
    }

    pub fn to_homebase(&self) -> Result<RowHomebaseOp> {
        self.validate_budget()?;
        self.validate_structure()
            .map_err(|error| Error::InvalidMultiliteOp(error.to_string()))?;
        let mut mutations = Vec::new();
        let mut guards = GuardPlan::for_operation(OperationFamily::RowChanges);
        for table in &self.tables {
            table.lower(&mut mutations, &mut guards)?;
        }
        prune_redundant_point_deletes(&mut mutations);
        let footprint = guards.footprint();
        Ok(RowHomebaseOp {
            mutations,
            footprint,
            guards,
        })
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut writer = Writer::new();
        writer.u8(ROW_CHANGES_FRAME_VERSION);
        for table in &self.tables {
            writer
                .field(TAG_CHANGED_TABLE, &table.encode())
                .expect("row-change field length fits in u32");
        }
        writer.finish()
    }

    pub fn decode(frame: &[u8]) -> std::result::Result<Self, RowCodecError> {
        if frame.len() > MAX_ROW_OPERATION_BYTES {
            return Err(RowCodecError::FrameTooLarge);
        }
        let mut reader = Reader::new(frame);
        if reader.u8() != Some(ROW_CHANGES_FRAME_VERSION) {
            return Err(RowCodecError::UnknownVersion);
        }
        let mut tables = Vec::new();
        while let Some((tag, value)) = reader.field().map_err(|_| RowCodecError::Truncated)? {
            if tag == TAG_CHANGED_TABLE {
                tables.push(TableChanges::decode(value)?);
            }
        }
        if tables.is_empty() {
            return Err(RowCodecError::MissingField(TAG_CHANGED_TABLE));
        }
        let changes = Self {
            tables: CanonicalTableChanges::new(tables)?,
        };
        changes.validate_budget_codec()?;
        Ok(changes)
    }

    fn validate_budget(&self) -> Result<()> {
        self.validate_budget_with(MAX_CAPTURED_CHANGES, MAX_ROW_OPERATION_BYTES)
            .map_err(|error| match error {
                RowCodecError::TooManyChanges => {
                    capture_limit("normalized row-change count", MAX_CAPTURED_CHANGES)
                }
                RowCodecError::FrameTooLarge | RowCodecError::InvalidLength => {
                    capture_limit("row-operation bytes", MAX_ROW_OPERATION_BYTES)
                }
                error => Error::InvalidMultiliteOp(error.to_string()),
            })
    }

    fn validate_budget_codec(&self) -> std::result::Result<(), RowCodecError> {
        self.validate_budget_with(MAX_CAPTURED_CHANGES, MAX_ROW_OPERATION_BYTES)
    }

    fn validate_budget_with(
        &self,
        max_changes: usize,
        max_bytes: usize,
    ) -> std::result::Result<(), RowCodecError> {
        let changes = self.tables.iter().try_fold(0usize, |count, table| {
            count
                .checked_add(table.rows.len())
                .ok_or(RowCodecError::InvalidLength)
        })?;
        if changes > max_changes {
            return Err(RowCodecError::TooManyChanges);
        }
        let bytes = self.tables.iter().try_fold(1usize, |bytes, table| {
            bytes
                .checked_add(field_len(table.encoded_len()?)?)
                .ok_or(RowCodecError::InvalidLength)
        })?;
        if bytes > max_bytes {
            return Err(RowCodecError::FrameTooLarge);
        }
        Ok(())
    }

    pub fn apply(&self, connection: &Connection) -> Result<()> {
        apply_final_row_state(connection, || {
            for table in &self.tables {
                table.apply(connection, false)?;
            }
            Ok(())
        })
    }

    pub fn restore_materialized(&self, connection: &Connection) -> Result<()> {
        apply_final_row_state(connection, || {
            for table in self.tables.iter().rev() {
                table.apply(connection, true)?;
            }
            Ok(())
        })
    }

    #[cfg(debug_assertions)]
    pub fn verify_materialized(&self, connection: &Connection) -> Result<()> {
        for table in &self.tables {
            if let Some(after) = table.after_rows() {
                let created = after.catalog_definition(connection)?;
                after.validate_materialized_against(
                    connection,
                    &created,
                    "canonical row changes diverged from captured after-images",
                )?;
            }
            let after_keys = table.keys(false)?;
            let retired = table
                .rows
                .iter()
                .filter_map(|change| change.before.as_ref())
                .filter(|row| {
                    table
                        .rules
                        .row_key(row)
                        .is_ok_and(|key| !after_keys.contains(&key))
                })
                .cloned()
                .collect::<Vec<_>>();
            if !retired.is_empty() {
                table
                    .rules
                    .with_rows(retired)
                    .validate_materialized_absent(
                        connection,
                        "canonical row changes retained a retired primary-key image",
                    )?;
            }
        }
        Ok(())
    }

    fn validate_structure(&self) -> std::result::Result<(), RowCodecError> {
        self.tables.validate()
    }
}

/// A parent-range retirement subsumes exact child-reference deletions captured
/// in the same statement. Keep their guards for OCC auditability, but avoid
/// sending redundant mutations to Homebase.
fn prune_redundant_point_deletes(mutations: &mut Vec<Mutation>) {
    let deleted_ranges = mutations
        .iter()
        .filter_map(|mutation| match mutation {
            Mutation::DeleteRange { range } => Some(range.clone()),
            Mutation::Set { .. } | Mutation::Delete { .. } => None,
        })
        .collect::<Vec<_>>();
    mutations.retain(|mutation| match mutation {
        Mutation::Delete { key } => !deleted_ranges.iter().any(|range| range.covers_key(key)),
        Mutation::Set { .. } | Mutation::DeleteRange { .. } => true,
    });
}

fn apply_final_row_state(
    connection: &Connection,
    operation: impl FnOnce() -> Result<()>,
) -> Result<()> {
    crate::connection::with_savepoint(connection, "__multilite__row_changes_apply", || {
        crate::connection::with_materialization_context(connection, operation, || {
            Error::CommitConflict("FOREIGN KEY constraint failed".into())
        })
    })
}

impl TableChangesBuilder {
    fn insert(&mut self, key: Key, row: Row) -> Result<()> {
        if self.current.contains_key(&key) {
            return Err(Error::CaptureInvariant(
                "SQLite inserted a row key already live in the statement delta",
            ));
        }
        let index = self
            .tombstones
            .remove(&key)
            .filter(|index| self.rows[*index].after.is_none())
            .unwrap_or_else(|| {
                let index = self.rows.len();
                self.rows.push(RowDelta {
                    before: None,
                    after: None,
                });
                index
            });
        self.rows[index].after = Some(row);
        self.current.insert(key, index);
        Ok(())
    }

    fn delete(&mut self, key: Key, row: Row) -> Result<()> {
        let index = match self.current.remove(&key) {
            Some(index) => {
                if self.rows[index].after.as_ref() != Some(&row) {
                    return Err(Error::CaptureInvariant(
                        "SQLite delete before-image contradicts the statement delta",
                    ));
                }
                index
            }
            None => {
                let index = self.rows.len();
                self.rows.push(RowDelta {
                    before: Some(row),
                    after: None,
                });
                index
            }
        };
        self.rows[index].after = None;
        self.tombstones.insert(key, index);
        Ok(())
    }

    fn update(&mut self, before_key: Key, before: Row, after_key: Key, after: Row) -> Result<()> {
        let index = match self.current.remove(&before_key) {
            Some(index) => {
                if self.rows[index].after.as_ref() != Some(&before) {
                    return Err(Error::CaptureInvariant(
                        "SQLite update before-image contradicts the statement delta",
                    ));
                }
                index
            }
            None => {
                let index = self.rows.len();
                self.rows.push(RowDelta {
                    before: Some(before),
                    after: None,
                });
                index
            }
        };
        if self
            .current
            .get(&after_key)
            .is_some_and(|other| *other != index)
        {
            return Err(Error::CaptureInvariant(
                "SQLite update produced a duplicate live row key",
            ));
        }
        self.rows[index].after = Some(after);
        if before_key != after_key {
            self.tombstones.insert(before_key, index);
        } else {
            self.tombstones.remove(&before_key);
        }
        self.current.insert(after_key, index);
        Ok(())
    }
}

fn captured_image(
    catalog: &CatalogSnapshot,
    captured: CapturedRow,
    missing_identity: &'static str,
    expected: Option<&RowRules>,
) -> Result<(RowRules, Row, Key)> {
    let created = catalog
        .by_name(&captured.table)
        .ok_or(Error::UnsupportedSql(missing_identity))?;
    let rules = if let Some(expected) = expected {
        if expected.table != created.table_id() {
            return Err(Error::CaptureInvariant(
                "one row statement changed more than one synchronized table",
            ));
        }
        expected.clone()
    } else {
        RowRules::from_table(catalog, created)?
    };
    let row = rules.capture(created, &captured)?;
    let key = rules
        .row_key(&row)
        .map_err(|error| Error::InvalidMultiliteOp(error.to_string()))?;
    Ok((rules, row, key))
}

fn table_builder(
    builders: &mut BTreeMap<[u8; 16], TableChangesBuilder>,
    rules: RowRules,
) -> Result<&mut TableChangesBuilder> {
    use std::collections::btree_map::Entry;

    let table = rules.table.as_bytes();
    match builders.entry(table) {
        Entry::Occupied(entry) => {
            if entry.get().rules != rules {
                return Err(Error::CaptureInvariant(
                    "one row statement used inconsistent rules for one synchronized table",
                ));
            }
            Ok(entry.into_mut())
        }
        Entry::Vacant(entry) => Ok(entry.insert(TableChangesBuilder {
            rules,
            rows: Vec::new(),
            current: BTreeMap::new(),
            tombstones: BTreeMap::new(),
        })),
    }
}

impl TableChanges {
    fn encoded_len(&self) -> std::result::Result<usize, RowCodecError> {
        let rules = self.rules.encode().len();
        let mut bytes = 1usize
            .checked_add(field_len(rules)?)
            .ok_or(RowCodecError::InvalidLength)?;
        for row in &self.rows {
            bytes = bytes
                .checked_add(field_len(row.encoded_len()?)?)
                .ok_or(RowCodecError::InvalidLength)?;
        }
        Ok(bytes)
    }

    fn before_rows(&self) -> Option<RowSet> {
        let rows = self
            .rows
            .iter()
            .filter_map(|change| change.before.clone())
            .collect::<Vec<_>>();
        (!rows.is_empty()).then(|| self.rules.with_rows(rows))
    }

    fn after_rows(&self) -> Option<RowSet> {
        let rows = self
            .rows
            .iter()
            .filter_map(|change| change.after.clone())
            .collect::<Vec<_>>();
        (!rows.is_empty()).then(|| self.rules.with_rows(rows))
    }

    #[cfg(debug_assertions)]
    fn keys(&self, before: bool) -> Result<BTreeSet<Key>> {
        self.rows
            .iter()
            .filter_map(|change| {
                if before {
                    change.before.as_ref()
                } else {
                    change.after.as_ref()
                }
            })
            .map(|row| {
                self.rules
                    .row_key(row)
                    .map_err(|error| Error::InvalidMultiliteOp(error.to_string()))
            })
            .collect()
    }

    fn lower(&self, mutations: &mut Vec<Mutation>, guards: &mut GuardPlan) -> Result<()> {
        let before = self.before_rows();
        let after = self.after_rows();
        let before_unique = optional_index_map(before.as_ref())?;
        let after_unique = optional_index_map(after.as_ref())?;
        let before_references = optional_reference_map(before.as_ref())?;
        let after_references = optional_reference_map(after.as_ref())?;

        let row_keys = self
            .rows
            .iter()
            .map(|change| {
                let before = change
                    .before
                    .as_ref()
                    .map(|row| self.rules.row_key(row))
                    .transpose()?;
                let after = change
                    .after
                    .as_ref()
                    .map(|row| self.rules.row_key(row))
                    .transpose()?;
                Ok((before, after))
            })
            .collect::<std::result::Result<Vec<_>, RowCodecError>>()
            .map_err(|error| Error::InvalidMultiliteOp(error.to_string()))?;

        // Retire every moved source before publishing any destination. This is
        // required when one row moves into another row's former primary key.
        for (before_key, after_key) in &row_keys {
            if let Some(key) = &before_key {
                guards.write(key.clone(), GuardReason::RowIdentity)?;
                if after_key.as_ref() != Some(key) {
                    mutations.push(Mutation::Delete { key: key.clone() });
                }
            }
        }
        for (change, (_, after_key)) in self.rows.iter().zip(row_keys) {
            if let (Some(row), Some(key)) = (&change.after, after_key) {
                guards.invariant(key.clone(), GuardReason::RowIdentity)?;
                guards.write(key.clone(), GuardReason::RowIdentity)?;
                mutations.push(Mutation::Set {
                    key,
                    value: self.rules.encode_row(row),
                });
            }
        }
        lower_map_delta(
            &before_unique,
            &after_unique,
            GuardReason::UniqueOwnership,
            mutations,
            guards,
        )?;
        lower_map_delta(
            &before_references,
            &after_references,
            GuardReason::ForeignReference,
            mutations,
            guards,
        )?;

        let before_incoming = optional_incoming_set(before.as_ref())?;
        let after_incoming = optional_incoming_set(after.as_ref())?;
        for prefix in before_incoming.difference(&after_incoming) {
            guards.invariant(prefix.clone(), GuardReason::ForeignChildren)?;
            guards.write(prefix.clone(), GuardReason::ForeignChildren)?;
            mutations.push(Mutation::DeleteRange {
                range: Range::Prefix(prefix.clone()),
            });
        }
        guards.invariant(
            active_primary_index_key(self.rules.table),
            GuardReason::PrimaryIndex,
        )?;
        guards.invariant(
            write_revision_key(self.rules.table),
            GuardReason::WriteContract,
        )?;
        Ok(())
    }

    fn encode(&self) -> Vec<u8> {
        let mut writer = Writer::new();
        writer.u8(TABLE_CHANGES_FRAME_VERSION);
        writer
            .field(TAG_CHANGE_RULES, &self.rules.encode())
            .expect("row-change rules fit in u32");
        for row in &self.rows {
            writer
                .field(TAG_CHANGE_ROW, &row.encode())
                .expect("row delta fits in u32");
        }
        writer.finish()
    }

    fn decode(frame: &[u8]) -> std::result::Result<Self, RowCodecError> {
        let mut reader = Reader::new(frame);
        if reader.u8() != Some(TABLE_CHANGES_FRAME_VERSION) {
            return Err(RowCodecError::UnknownVersion);
        }
        let mut rules = None;
        let mut rows = Vec::new();
        while let Some((tag, value)) = reader.field().map_err(|_| RowCodecError::Truncated)? {
            match tag {
                TAG_CHANGE_RULES => set_once(&mut rules, RowRules::decode(value)?)?,
                TAG_CHANGE_ROW => rows.push(RowDelta::decode(value)?),
                _ => {}
            }
        }
        let table = Self {
            rules: rules.ok_or(RowCodecError::MissingField(TAG_CHANGE_RULES))?,
            rows,
        };
        table.validate_structure()?;
        Ok(table)
    }

    fn validate_structure(&self) -> std::result::Result<(), RowCodecError> {
        self.rules.validate()?;
        if self.rows.is_empty()
            || self
                .rows
                .iter()
                .any(|row| row.before.is_none() && row.after.is_none() || row.before == row.after)
        {
            return Err(RowCodecError::InvalidRow);
        }
        if let Some(before) = self.before_rows() {
            before.validate_structure()?;
        }
        if let Some(after) = self.after_rows() {
            after.validate_structure()?;
        }
        Ok(())
    }

    fn apply(&self, connection: &Connection, reverse: bool) -> Result<()> {
        let pairs = self
            .rows
            .iter()
            .map(|change| {
                if reverse {
                    (change.after.clone(), change.before.clone())
                } else {
                    (change.before.clone(), change.after.clone())
                }
            })
            .collect::<Vec<_>>();
        let before = pairs
            .iter()
            .filter_map(|(before, _)| before.clone())
            .collect::<Vec<_>>();
        let after = pairs
            .iter()
            .filter_map(|(_, after)| after.clone())
            .collect::<Vec<_>>();
        let created = if !before.is_empty() {
            self.rules
                .with_rows(before.clone())
                .catalog_definition(connection)?
        } else {
            self.rules
                .with_rows(after.clone())
                .catalog_definition(connection)?
        };

        let before_set = self.rules.with_rows(before);
        let materialized = if before_set.rows.is_empty() {
            Vec::new()
        } else {
            before_set.materialized_rows(
                connection,
                &created,
                if reverse {
                    "pending row changes no longer match SQLite state"
                } else {
                    "row changes no longer match SQLite state"
                },
            )?
        };
        let mut materialized = materialized.into_iter();
        let mut stable_before = Vec::new();
        let mut stable_after = Vec::new();
        let mut removed = Vec::new();
        let mut inserted = Vec::new();
        for (before, after) in pairs {
            let current = before.as_ref().map(|_| {
                materialized
                    .next()
                    .expect("materialized before rows preserve row-delta order")
            });
            match (before, after, current) {
                (Some(before), Some(after), Some(current)) => {
                    let before_key = self
                        .rules
                        .row_key(&before)
                        .map_err(|error| Error::InvalidMultiliteOp(error.to_string()))?;
                    let after_key = self
                        .rules
                        .row_key(&after)
                        .map_err(|error| Error::InvalidMultiliteOp(error.to_string()))?;
                    if before_key == after_key {
                        stable_before.push(before);
                        stable_after.push(after);
                    } else {
                        let mut target = current.clone();
                        for (column, value) in after.values {
                            if let Some(slot) = target.value_mut(column) {
                                *slot = value;
                            }
                        }
                        removed.push(current);
                        inserted.push(target);
                    }
                }
                (Some(_), None, Some(current)) => removed.push(current),
                (None, Some(after), None) => inserted.push(after),
                _ => return Err(Error::CaptureInvariant("invalid row delta while applying")),
            }
        }
        if !removed.is_empty() {
            RowSet::for_current_rows(connection, &created, removed)?
                .delete_materialized_with(connection, "row changes no longer match SQLite state")?;
        }
        if !stable_before.is_empty() {
            let before = self.rules.with_rows(stable_before);
            let after = self.rules.with_rows(stable_after);
            let stable = (0..before.rows.len()).collect::<Vec<_>>();
            update_stable_rows(
                connection,
                &created,
                &before,
                &after,
                &stable,
                "row changes no longer match SQLite state",
            )?;
        }
        if !inserted.is_empty() {
            let rows = if inserted.iter().all(|row| {
                created
                    .columns()
                    .iter()
                    .all(|column| row.value(column.id()).is_some())
            }) {
                RowSet::for_current_rows(connection, &created, inserted)?
            } else {
                self.rules.with_rows(inserted)
            };
            rows.apply(connection)?;
        }
        Ok(())
    }
}

impl RowDelta {
    fn encoded_len(&self) -> std::result::Result<usize, RowCodecError> {
        let mut bytes = 1usize;
        for row in [&self.before, &self.after].into_iter().flatten() {
            bytes = bytes
                .checked_add(field_len(row_image_len(row)?)?)
                .ok_or(RowCodecError::InvalidLength)?;
        }
        Ok(bytes)
    }

    fn encode(&self) -> Vec<u8> {
        let mut writer = Writer::new();
        writer.u8(ROW_DELTA_FRAME_VERSION);
        if let Some(before) = &self.before {
            writer
                .field(TAG_DELTA_BEFORE, &encode_row_image(before))
                .expect("before image fits in u32");
        }
        if let Some(after) = &self.after {
            writer
                .field(TAG_DELTA_AFTER, &encode_row_image(after))
                .expect("after image fits in u32");
        }
        writer.finish()
    }

    fn decode(frame: &[u8]) -> std::result::Result<Self, RowCodecError> {
        let mut reader = Reader::new(frame);
        if reader.u8() != Some(ROW_DELTA_FRAME_VERSION) {
            return Err(RowCodecError::UnknownVersion);
        }
        let mut before = None;
        let mut after = None;
        while let Some((tag, value)) = reader.field().map_err(|_| RowCodecError::Truncated)? {
            match tag {
                TAG_DELTA_BEFORE => set_once(&mut before, decode_row_image(value)?)?,
                TAG_DELTA_AFTER => set_once(&mut after, decode_row_image(value)?)?,
                _ => {}
            }
        }
        Ok(Self { before, after })
    }
}

fn optional_index_map(rows: Option<&RowSet>) -> Result<BTreeMap<Key, Vec<u8>>> {
    rows.map(RowSet::index_entry_map)
        .transpose()
        .map_err(|error| Error::InvalidMultiliteOp(error.to_string()))
        .map(Option::unwrap_or_default)
}

fn optional_reference_map(rows: Option<&RowSet>) -> Result<BTreeMap<Key, Vec<u8>>> {
    rows.map(RowSet::foreign_reference_map)
        .transpose()
        .map_err(|error| Error::InvalidMultiliteOp(error.to_string()))
        .map(Option::unwrap_or_default)
}

fn optional_incoming_set(rows: Option<&RowSet>) -> Result<BTreeSet<Key>> {
    rows.map(RowSet::incoming_reference_prefix_set)
        .transpose()
        .map_err(|error| Error::InvalidMultiliteOp(error.to_string()))
        .map(Option::unwrap_or_default)
}

fn lower_map_delta(
    before: &BTreeMap<Key, Vec<u8>>,
    after: &BTreeMap<Key, Vec<u8>>,
    reason: GuardReason,
    mutations: &mut Vec<Mutation>,
    guards: &mut GuardPlan,
) -> Result<()> {
    for (key, owner) in before {
        if after.get(key) != Some(owner) {
            guards.write(key.clone(), reason)?;
            mutations.push(Mutation::Delete { key: key.clone() });
        }
    }
    for (key, owner) in after {
        if before.get(key) != Some(owner) {
            guards.invariant(key.clone(), reason)?;
            guards.write(key.clone(), reason)?;
            mutations.push(Mutation::Set {
                key: key.clone(),
                value: owner.clone(),
            });
        }
    }
    Ok(())
}

// These fixtures keep older focused row tests concise while delegating every
// codec, lowering, replay, and inverse path to the production statement delta.
#[cfg(test)]
impl DeletedRowsFixture {
    pub(crate) fn from_row_set(deleted: RowSet) -> Self {
        Self { deleted }
    }

    pub fn from_captured(
        connection: &Connection,
        captured: &[CapturedRow],
    ) -> Result<Option<Self>> {
        let catalog = CatalogSnapshot::load(connection)?;
        Self::from_catalog(&catalog, captured)
    }

    pub(crate) fn from_catalog(
        catalog: &CatalogSnapshot,
        captured: &[CapturedRow],
    ) -> Result<Option<Self>> {
        let events = captured
            .iter()
            .cloned()
            .map(CapturedChange::Delete)
            .collect();
        RowChanges::from_catalog(catalog, events)?
            .map(Self::from_changes)
            .transpose()
            .map_err(|error| Error::InvalidMultiliteOp(error.to_string()))
    }

    fn from_changes(changes: RowChanges) -> std::result::Result<Self, RowCodecError> {
        let [table] = changes
            .tables
            .0
            .try_into()
            .map_err(|_| RowCodecError::InvalidRow)?;
        let rows = table
            .rows
            .into_iter()
            .map(|delta| match (delta.before, delta.after) {
                (Some(before), None) => Ok(before),
                _ => Err(RowCodecError::InvalidRow),
            })
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(Self {
            deleted: table.rules.with_rows(rows),
        })
    }

    pub fn to_homebase(&self) -> Result<RowHomebaseOp> {
        RowChanges::deleted(self.clone()).to_homebase()
    }

    pub fn encode(&self) -> Vec<u8> {
        RowChanges::deleted(self.clone()).encode()
    }

    pub fn decode(frame: &[u8]) -> std::result::Result<Self, RowCodecError> {
        Self::from_changes(RowChanges::decode(frame)?)
    }

    pub fn apply(&self, connection: &Connection) -> Result<()> {
        RowChanges::deleted(self.clone()).apply(connection)
    }

    #[cfg(debug_assertions)]
    pub fn verify_materialized(&self, connection: &Connection) -> Result<()> {
        RowChanges::deleted(self.clone()).verify_materialized(connection)
    }

    pub fn restore_materialized(&self, connection: &Connection) -> Result<()> {
        RowChanges::deleted(self.clone()).restore_materialized(connection)
    }
}

#[cfg(test)]
impl UpdatedRowsFixture {
    pub fn from_captured(
        connection: &Connection,
        captured: &[(CapturedRow, CapturedRow)],
    ) -> Result<Option<Self>> {
        let catalog = CatalogSnapshot::load(connection)?;
        Self::from_catalog(&catalog, captured)
    }

    pub(crate) fn from_catalog(
        catalog: &CatalogSnapshot,
        captured: &[(CapturedRow, CapturedRow)],
    ) -> Result<Option<Self>> {
        let events = captured
            .iter()
            .cloned()
            .map(|(before, after)| CapturedChange::Update { before, after })
            .collect();
        RowChanges::from_catalog(catalog, events)?
            .map(Self::from_changes)
            .transpose()
            .map_err(|error| Error::InvalidMultiliteOp(error.to_string()))
    }

    fn from_changes(changes: RowChanges) -> std::result::Result<Self, RowCodecError> {
        let [table] = changes
            .tables
            .0
            .try_into()
            .map_err(|_| RowCodecError::InvalidRow)?;
        let mut before = Vec::with_capacity(table.rows.len());
        let mut after = Vec::with_capacity(table.rows.len());
        for delta in table.rows {
            let (Some(before_row), Some(after_row)) = (delta.before, delta.after) else {
                return Err(RowCodecError::InvalidRow);
            };
            before.push(before_row);
            after.push(after_row);
        }
        Ok(Self {
            before: table.rules.with_rows(before),
            after: table.rules.with_rows(after),
        })
    }

    pub fn to_homebase(&self) -> Result<RowHomebaseOp> {
        RowChanges::updated(self.clone()).to_homebase()
    }

    pub fn encode(&self) -> Vec<u8> {
        RowChanges::updated(self.clone()).encode()
    }

    pub fn decode(frame: &[u8]) -> std::result::Result<Self, RowCodecError> {
        Self::from_changes(RowChanges::decode(frame)?)
    }

    pub fn apply(&self, connection: &Connection) -> Result<()> {
        RowChanges::updated(self.clone()).apply(connection)
    }

    #[cfg(debug_assertions)]
    pub fn verify_materialized(&self, connection: &Connection) -> Result<()> {
        RowChanges::updated(self.clone()).verify_materialized(connection)
    }

    pub fn restore_materialized(&self, connection: &Connection) -> Result<()> {
        RowChanges::updated(self.clone()).restore_materialized(connection)
    }
}

fn update_stable_rows(
    connection: &Connection,
    created: &CreateTable,
    before: &RowSet,
    after: &RowSet,
    stable: &[usize],
    mismatch: &'static str,
) -> Result<()> {
    if stable.is_empty() {
        return Ok(());
    }
    let columns = created
        .columns()
        .iter()
        .filter(|column| {
            stable.iter().any(|index| {
                before.rows[*index].value(column.id()) != after.rows[*index].value(column.id())
            })
        })
        .collect::<Vec<_>>();
    if columns.is_empty() {
        return Ok(());
    }
    let assignments = columns
        .iter()
        .map(|column| {
            materialized_column_name(connection, created.table_id(), column.id())
                .map(|name| format!("{} = ?", quote_identifier(name.value())))
        })
        .collect::<Result<Vec<_>>>()?
        .join(", ");
    let predicates = created
        .primary_key_columns()
        .map(|column| materialized_column_name(connection, created.table_id(), column.id()))
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .map(|name| format!("{} = ?", quote_identifier(name.value())))
        .collect::<Vec<_>>();
    let sql = format!(
        "UPDATE {} SET {assignments} WHERE {}",
        quote_identifier(&materialized_table_name(connection, after.rules.table)?),
        predicates.join(" AND ")
    );
    let mut statement = connection.prepare(&sql)?;
    for index in stable {
        let before_row = &before.rows[*index];
        let after_row = &after.rows[*index];
        let values = columns
            .iter()
            .map(|column| {
                after_row
                    .value(column.id())
                    .ok_or(Error::InvalidMultiliteOp(
                        "row is missing a schema column".into(),
                    ))
            })
            .collect::<Result<Vec<_>>>()?;
        let primary = before.primary_values(before_row)?;
        let mut parameters = Vec::<&dyn ToSql>::with_capacity(values.len() + primary.len());
        parameters.extend(values.into_iter().map(|value| value as &dyn ToSql));
        parameters.extend(primary.into_iter().map(|value| value as &dyn ToSql));
        if statement.execute(params_from_iter(parameters))? != 1 {
            return Err(Error::InvalidDatabase(mismatch));
        }
    }
    Ok(())
}
/// Prefix covering every row encoded under a table's active primary index.
pub fn primary_index_prefix(created: &CreateTable) -> Key {
    row_prefix(created.table_id(), created.primary_index_id(), Vec::new())
        .expect("table row prefix is bounded and non-empty")
}

/// Exact row prefix produced by one complete primary-key value tuple.
#[cfg(test)]
pub fn primary_key_prefix(
    created: &CreateTable,
    values: &[StoredValue],
) -> std::result::Result<Key, RowCodecError> {
    let primary = created.primary_key_columns().collect::<Vec<_>>();
    if primary.len() != values.len() {
        return Err(RowCodecError::InvalidRow);
    }
    primary_key_equality_prefix(created, values)
}

/// Row prefix produced by equality values for consecutive leading PK parts.
#[cfg(test)]
pub fn primary_key_equality_prefix(
    created: &CreateTable,
    values: &[StoredValue],
) -> std::result::Result<Key, RowCodecError> {
    let primary = created.primary_key_columns().collect::<Vec<_>>();
    if values.len() > primary.len() {
        return Err(RowCodecError::InvalidRow);
    }
    let images = primary
        .into_iter()
        .take(values.len())
        .zip(values)
        .map(|(column, value)| {
            if matches!(
                (column.affinity(created.mode()), value),
                (
                    Affinity::Integer | Affinity::Real | Affinity::Numeric,
                    StoredValue::Text(_)
                )
            ) {
                return Err(RowCodecError::InvalidRow);
            }
            key_image(
                value,
                KeyPartRules {
                    column: column.id(),
                    affinity: column.affinity(created.mode()),
                    rowid_alias: created.is_rowid_alias(column.id()),
                },
            )
        })
        .collect::<std::result::Result<Vec<_>, _>>()?;
    row_prefix(created.table_id(), created.primary_index_id(), images)
}

fn index_rules(created: &CreateTable) -> Vec<IndexRules> {
    created
        .unique_constraints()
        .iter()
        .map(|unique| IndexRules {
            index: unique.index_id(),
            parts: index_parts(created, unique.columns()),
        })
        .chain(
            created
                .indexes()
                .iter()
                .filter(|index| index.is_active() && index.is_unique())
                .map(|index| IndexRules {
                    index: index.index_id(),
                    parts: index_parts(created, index.columns()),
                }),
        )
        .collect()
}

fn known_index_rules(created: &CreateTable) -> Vec<IndexRules> {
    created
        .unique_constraints()
        .iter()
        .map(|unique| IndexRules {
            index: unique.index_id(),
            parts: index_parts(created, unique.columns()),
        })
        .chain(
            created
                .indexes()
                .iter()
                .filter(|index| index.is_unique())
                .map(|index| IndexRules {
                    index: index.index_id(),
                    parts: index_parts(created, index.columns()),
                }),
        )
        .collect()
}

fn foreign_key_rules(
    catalog: &CatalogSnapshot,
    created: &CreateTable,
) -> Result<Vec<ForeignKeyRules>> {
    created
        .foreign_keys()
        .iter()
        .map(|foreign_key| foreign_key_rule(catalog, created, foreign_key))
        .collect()
}

fn foreign_key_rule(
    catalog: &CatalogSnapshot,
    child: &CreateTable,
    foreign_key: &ForeignKeyDefinition,
) -> Result<ForeignKeyRules> {
    let parent = catalog
        .by_id(foreign_key.referenced_table())
        .ok_or(Error::InvalidDatabase(
            "foreign key references an unknown parent table",
        ))?;
    let parent_columns = parent
        .foreign_key_target_columns(foreign_key.referenced_index())
        .ok_or(Error::InvalidDatabase(
            "foreign key target is no longer active in the parent schema",
        ))?;
    if parent_columns
        .iter()
        .copied()
        .map(Column::id)
        .ne(foreign_key.referenced_columns().iter().copied())
    {
        return Err(Error::InvalidDatabase(
            "foreign key parent identity contradicts the schema catalog",
        ));
    }
    let key_parts = foreign_key
        .columns()
        .iter()
        .copied()
        .zip(parent_columns)
        .map(|(child_column, parent_column)| {
            if !child
                .columns()
                .iter()
                .any(|column| column.id() == child_column)
            {
                return Err(Error::InvalidDatabase(
                    "foreign key references an unknown child column",
                ));
            }
            Ok(ForeignKeyPartRules {
                child_column,
                parent: KeyPartRules {
                    column: parent_column.id(),
                    affinity: parent_column.affinity(parent.mode()),
                    rowid_alias: parent.is_rowid_alias(parent_column.id()),
                },
            })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(ForeignKeyRules {
        id: foreign_key.id(),
        parent_table: parent.table_id(),
        parent_index: foreign_key.referenced_index(),
        key_parts,
    })
}

fn incoming_foreign_key_rules(
    catalog: &CatalogSnapshot,
    parent: &CreateTable,
) -> Result<Vec<IncomingForeignKeyRules>> {
    let mut incoming = Vec::new();
    for (child, foreign_key) in catalog.incoming_foreign_keys(parent.table_id()) {
        // Reuse full relationship validation so corrupt catalog links do not
        // silently weaken parent-side deletion guards.
        let _ = foreign_key_rule(catalog, child, foreign_key)?;
        let target = foreign_key.referenced_index();
        let parent_key_parts = parent
            .foreign_key_target_columns(target)
            .ok_or(Error::InvalidDatabase(
                "foreign key target is no longer active in the parent schema",
            ))?
            .into_iter()
            .map(|column| KeyPartRules {
                column: column.id(),
                affinity: column.affinity(parent.mode()),
                rowid_alias: target == parent.primary_index_id()
                    && parent.is_rowid_alias(column.id()),
            })
            .collect();
        incoming.push(IncomingForeignKeyRules {
            id: foreign_key.id(),
            child_table: child.table_id(),
            child_primary_index: child.primary_index_id(),
            parent_index: target,
            parent_key_parts,
        });
    }
    Ok(incoming)
}

fn index_parts(created: &CreateTable, columns: &[ColumnId]) -> Vec<KeyPartRules> {
    columns
        .iter()
        .map(|id| {
            let column = created
                .columns()
                .iter()
                .find(|column| column.id() == *id)
                .expect("validated UNIQUE column exists");
            KeyPartRules {
                column: column.id(),
                affinity: column.affinity(created.mode()),
                rowid_alias: false,
            }
        })
        .collect()
}

/// Scan the current table and encode ownership entries for one new UNIQUE index.
pub fn backfill_unique_index(
    connection: &Connection,
    created: &CreateTable,
    index: &NamedIndex,
) -> Result<Vec<IndexBackfillEntry>> {
    if !index.is_unique() {
        return Err(Error::CaptureInvariant(
            "ordinary secondary indexes do not have Homebase entries",
        ));
    }
    let captured = capture_table(connection, created)?;
    if captured.is_empty() {
        return Ok(Vec::new());
    }
    let mut inserted = RowSet::from_captured(connection, &captured)?.ok_or(
        Error::CaptureInvariant("index table is missing its schema catalog"),
    )?;
    inserted.rules.indexes = vec![IndexRules {
        index: index.index_id(),
        parts: index_parts(created, index.columns()),
    }];
    inserted
        .validate_structure()
        .map_err(|error| Error::InvalidMultiliteOp(error.to_string()))?;
    let mut entries = BTreeMap::new();
    for row in &inserted.rows {
        for entry in inserted
            .index_entries(row)
            .map_err(|error| Error::InvalidMultiliteOp(error.to_string()))?
        {
            if entries.insert(entry.key, entry.value).is_some() {
                return Err(Error::InvalidMultiliteOp(
                    "index backfill contains duplicate entries".into(),
                ));
            }
        }
    }
    Ok(entries
        .into_iter()
        .map(|(key, value)| IndexBackfillEntry { key, value })
        .collect())
}

fn capture_table(connection: &Connection, created: &CreateTable) -> Result<Vec<CapturedRow>> {
    let table_name = materialized_table_name(connection, created.table_id())?;
    let selected = catalog::column_names(connection, created)?
        .iter()
        .map(|name| quote_identifier(name.value()))
        .collect::<Vec<_>>();
    let sql = format!(
        "SELECT {} FROM {}",
        selected.join(", "),
        quote_identifier(&table_name)
    );
    let mut statement = connection.prepare(&sql)?;
    let rows = statement.query_map((), |row| {
        let values = (0..created.columns().len())
            .map(|index| row.get_ref(index).map(StoredValue::capture))
            .collect::<rusqlite::Result<Vec<_>>>()?;
        let rowid = if created.storage() == TableStorage::Rowid {
            let alias = created
                .primary_key_columns()
                .next()
                .filter(|column| created.is_rowid_alias(column.id()))
                .expect("rowid table has an INTEGER PRIMARY KEY");
            let position = created
                .columns()
                .iter()
                .position(|column| column.id() == alias.id())
                .expect("INTEGER PRIMARY KEY belongs to the table");
            match values[position] {
                StoredValue::Integer(value) => value,
                _ => return Err(rusqlite::Error::InvalidQuery),
            }
        } else {
            0
        };
        Ok(CapturedRow {
            table: table_name.clone(),
            rowid,
            values,
        })
    })?;
    rows.collect::<rusqlite::Result<Vec<_>>>()
        .map_err(Into::into)
}

#[cfg(test)]
pub(crate) fn expected_materialized_cells(
    connection: &Connection,
) -> Result<BTreeMap<Key, Vec<u8>>> {
    let catalog = CatalogSnapshot::load(connection)?;
    let mut expected = BTreeMap::new();
    for created in catalog.tables() {
        let captured = capture_table(connection, created)?;
        let Some(rows) = RowSet::from_catalog(&catalog, &captured)? else {
            continue;
        };
        for mutation in rows.to_homebase()?.mutations {
            let Mutation::Set { key, value } = mutation else {
                return Err(Error::CaptureInvariant(
                    "materialized rows lowered to a destructive mutation",
                ));
            };
            if expected.insert(key, value).is_some() {
                return Err(Error::CaptureInvariant(
                    "materialized rows produce duplicate authority cells",
                ));
            }
        }
    }
    Ok(expected)
}

#[cfg(test)]
pub(crate) fn validate_materialized_cells(
    expected: &BTreeMap<Key, Vec<u8>>,
    actual: &BTreeMap<Key, Vec<u8>>,
) -> Result<()> {
    let exact_family = |key: &Key| {
        matches!(
            TargetFamily::classify(key),
            Some(TargetFamily::UniqueOwner | TargetFamily::ForeignReference)
        )
    };
    let expected_exact = expected
        .iter()
        .filter(|(key, _)| exact_family(key))
        .collect::<BTreeMap<_, _>>();
    let actual_exact = actual
        .iter()
        .filter(|(key, _)| exact_family(key))
        .collect::<BTreeMap<_, _>>();
    if expected_exact != actual_exact {
        return Err(Error::CaptureInvariant(
            "authority UNIQUE or foreign-reference cells diverge from materialized rows",
        ));
    }

    let expected_rows = expected
        .iter()
        .filter(|(key, _)| TargetFamily::classify(key) == Some(TargetFamily::Row))
        .collect::<BTreeMap<_, _>>();
    let actual_rows = actual
        .iter()
        .filter(|(key, _)| TargetFamily::classify(key) == Some(TargetFamily::Row))
        .collect::<BTreeMap<_, _>>();
    if !expected_rows.keys().eq(actual_rows.keys()) {
        return Err(Error::CaptureInvariant(
            "authority row identities diverge from materialized rows",
        ));
    }
    for (key, expected_frame) in expected_rows {
        let expected_row = decode_stored_row(expected_frame)
            .map_err(|_| Error::CaptureInvariant("materialized row frame is malformed"))?;
        let actual_row = decode_stored_row(actual_rows[key])
            .map_err(|_| Error::CaptureInvariant("authority row frame is malformed"))?;
        if actual_row.values.iter().any(|(column, actual)| {
            expected_row
                .value(*column)
                .is_some_and(|expected| expected != actual)
        }) {
            return Err(Error::CaptureInvariant(
                "authority row values diverge from materialized rows",
            ));
        }
    }
    Ok(())
}

fn row_prefix(
    table: TableId,
    primary_index: IndexId,
    images: Vec<Vec<u8>>,
) -> std::result::Result<Key, RowCodecError> {
    LogicalTarget::Row {
        table,
        index: primary_index,
        images,
    }
    .render()
    .map_err(RowCodecError::InvalidKey)
}

fn unique_prefix(
    table: TableId,
    index: IndexId,
    images: Vec<Vec<u8>>,
) -> std::result::Result<Key, RowCodecError> {
    LogicalTarget::UniqueOwner {
        table,
        index,
        images,
    }
    .render()
    .map_err(RowCodecError::InvalidKey)
}

fn foreign_reference_prefix(
    parent: TableId,
    relationship: ForeignKeyId,
    parent_index: IndexId,
    parent_images: Vec<Vec<u8>>,
) -> std::result::Result<Key, RowCodecError> {
    LogicalTarget::ForeignReferencePrefix {
        parent,
        relationship,
        parent_index,
        parent_images,
    }
    .render()
    .map_err(RowCodecError::InvalidKey)
}

fn foreign_reference_key(
    parent: TableId,
    relationship: ForeignKeyId,
    parent_index: IndexId,
    parent_images: Vec<Vec<u8>>,
    child_primary_index: IndexId,
    child_images: Vec<Vec<u8>>,
) -> std::result::Result<Key, RowCodecError> {
    LogicalTarget::ForeignReference {
        parent,
        relationship,
        parent_index,
        parent_images,
        child_index: child_primary_index,
        child_images,
    }
    .render()
    .map_err(RowCodecError::InvalidKey)
}

fn key_image(
    value: &StoredValue,
    rules: KeyPartRules,
) -> std::result::Result<Vec<u8>, RowCodecError> {
    if rules.affinity == Affinity::Text
        && matches!(value, StoredValue::Integer(_) | StoredValue::Real(_))
    {
        return Err(RowCodecError::InvalidRow);
    }
    match value {
        StoredValue::Null => Err(RowCodecError::NullPrimaryKey),
        StoredValue::Integer(value) => {
            let mut image = vec![1];
            let ordered = (*value as u64) ^ (1_u64 << 63);
            image.extend_from_slice(&ordered.to_be_bytes());
            Ok(image)
        }
        StoredValue::Real(bits) => {
            let value = f64::from_bits(*bits);
            if value.is_finite()
                && value.fract() == 0.0
                && value >= i64::MIN as f64
                && value < -(i64::MIN as f64)
            {
                return key_image(&StoredValue::Integer(value as i64), rules);
            }
            let ordered = if bits & (1_u64 << 63) == 0 {
                bits ^ (1_u64 << 63)
            } else {
                !bits
            };
            let mut image = vec![2];
            image.extend_from_slice(&ordered.to_be_bytes());
            Ok(image)
        }
        StoredValue::Text(value) => {
            let mut image = Vec::with_capacity(value.len() + 1);
            image.push(3);
            image.extend_from_slice(value);
            Ok(image)
        }
        StoredValue::Blob(value) => {
            let mut image = Vec::with_capacity(value.len() + 1);
            image.push(4);
            image.extend_from_slice(value);
            Ok(image)
        }
    }
}

fn index_image(
    value: &StoredValue,
    rules: KeyPartRules,
) -> std::result::Result<Vec<u8>, RowCodecError> {
    if matches!(value, StoredValue::Null) {
        Ok(vec![0])
    } else {
        key_image(value, rules)
    }
}

fn normalize_captured_value(value: &StoredValue, affinity: Affinity) -> StoredValue {
    match (affinity, value) {
        (Affinity::Real, StoredValue::Integer(value)) => {
            StoredValue::Real((*value as f64).to_bits())
        }
        _ => value.clone(),
    }
}

fn strict_value_matches(column: &Column, value: &StoredValue) -> bool {
    if matches!(value, StoredValue::Null) {
        return !column.is_not_null();
    }
    matches!(
        (column.strict_type(), value),
        (Some(StrictType::Integer), StoredValue::Integer(_))
            | (Some(StrictType::Real), StoredValue::Real(_))
            | (Some(StrictType::Text), StoredValue::Text(_))
            | (Some(StrictType::Blob), StoredValue::Blob(_))
            | (Some(StrictType::Any), _)
    )
}

fn encode_key_part(part: KeyPartRules) -> Vec<u8> {
    let mut writer = Writer::new();
    writer
        .field(TAG_COLUMN_ID, &part.column.as_bytes())
        .expect("row field length fits in u32");
    writer
        .field(TAG_COLUMN_AFFINITY, &[part.affinity.to_u8()])
        .expect("row field length fits in u32");
    writer
        .field(
            TAG_KEY_PART_FLAGS,
            &[u8::from(part.rowid_alias) * KEY_PART_ROWID_ALIAS],
        )
        .expect("row field length fits in u32");
    writer.finish()
}

fn decode_key_part(frame: &[u8]) -> std::result::Result<KeyPartRules, RowCodecError> {
    let mut reader = Reader::new(frame);
    let mut column = None;
    let mut affinity = None;
    let mut flags = None;
    while let Some((tag, value)) = reader.field().map_err(|_| RowCodecError::Truncated)? {
        match tag {
            TAG_COLUMN_ID => set_once(&mut column, ColumnId::from_bytes(uuid_bytes(value)?))?,
            TAG_COLUMN_AFFINITY => {
                let [value] = value else {
                    return Err(RowCodecError::InvalidLength);
                };
                set_once(
                    &mut affinity,
                    Affinity::from_u8(*value).ok_or(RowCodecError::InvalidRow)?,
                )?;
            }
            TAG_KEY_PART_FLAGS => {
                let [value] = value else {
                    return Err(RowCodecError::InvalidLength);
                };
                if value & !KEY_PART_ROWID_ALIAS != 0 {
                    return Err(RowCodecError::InvalidRow);
                }
                set_once(&mut flags, *value)?;
            }
            _ => {}
        }
    }
    let part = KeyPartRules {
        column: column.ok_or(RowCodecError::MissingField(TAG_COLUMN_ID))?,
        affinity: affinity.ok_or(RowCodecError::MissingField(TAG_COLUMN_AFFINITY))?,
        rowid_alias: flags.ok_or(RowCodecError::MissingField(TAG_KEY_PART_FLAGS))?
            & KEY_PART_ROWID_ALIAS
            != 0,
    };
    if part.rowid_alias && part.affinity != Affinity::Integer {
        return Err(RowCodecError::InvalidRow);
    }
    Ok(part)
}

fn encode_column_value(column: ColumnId, value: &StoredValue) -> Vec<u8> {
    let mut writer = Writer::new();
    writer
        .field(TAG_COLUMN_ID, &column.as_bytes())
        .expect("row field length fits in u32");
    writer
        .field(TAG_VALUE, &value.encode())
        .expect("row field length fits in u32");
    writer.finish()
}

fn decode_column_value(
    frame: &[u8],
) -> std::result::Result<(ColumnId, StoredValue), RowCodecError> {
    let mut reader = Reader::new(frame);
    let mut column = None;
    let mut value = None;
    while let Some((tag, bytes)) = reader.field().map_err(|_| RowCodecError::Truncated)? {
        match tag {
            TAG_COLUMN_ID => set_once(&mut column, ColumnId::from_bytes(uuid_bytes(bytes)?))?,
            TAG_VALUE => set_once(&mut value, decode_stored_value(bytes)?)?,
            _ => {}
        }
    }
    Ok((
        column.ok_or(RowCodecError::MissingField(TAG_COLUMN_ID))?,
        value.ok_or(RowCodecError::MissingField(TAG_VALUE))?,
    ))
}

fn validate_row_values(
    values: Vec<(ColumnId, StoredValue)>,
) -> std::result::Result<Row, RowCodecError> {
    if values.is_empty() {
        return Err(RowCodecError::InvalidRow);
    }
    if values
        .iter()
        .enumerate()
        .any(|(index, (column, _))| values[..index].iter().any(|(seen, _)| seen == column))
    {
        return Err(RowCodecError::DuplicateField);
    }
    Ok(Row { values })
}

fn materialized_table_name(connection: &Connection, table: TableId) -> Result<String> {
    catalog::name_by_id(connection, table)?
        .map(|name| name.value().to_owned())
        .ok_or(Error::InvalidDatabase(
            "row operation references a table without a current name binding",
        ))
}

fn rowid_from_declared_primary_key(created: &CreateTable, row: &Row) -> Result<i64> {
    if created.storage() == TableStorage::WithoutRowid {
        return Ok(0);
    }
    let column = created
        .primary_key_columns()
        .find(|column| created.is_rowid_alias(column.id()))
        .ok_or(Error::InvalidDatabase(
            "rowid table has no declared INTEGER PRIMARY KEY alias",
        ))?;
    match row.value(column.id()) {
        Some(StoredValue::Integer(rowid)) => Ok(*rowid),
        _ => Err(Error::InvalidDatabase(
            "rowid table has a non-integer primary key value",
        )),
    }
}

fn materialized_column_name(
    connection: &Connection,
    table: TableId,
    column: ColumnId,
) -> Result<SqlName> {
    catalog::column_name_by_id(connection, table, column)?.ok_or(Error::InvalidDatabase(
        "row operation references a column without a current name binding",
    ))
}

fn set_once<T>(slot: &mut Option<T>, value: T) -> std::result::Result<(), RowCodecError> {
    if slot.replace(value).is_some() {
        Err(RowCodecError::DuplicateField)
    } else {
        Ok(())
    }
}

fn row_image_len(row: &Row) -> std::result::Result<usize, RowCodecError> {
    row.values.iter().try_fold(1usize, |bytes, (_, value)| {
        let column_value = field_len(16)?
            .checked_add(field_len(value.encoded_len())?)
            .ok_or(RowCodecError::InvalidLength)?;
        bytes
            .checked_add(field_len(column_value)?)
            .ok_or(RowCodecError::InvalidLength)
    })
}

fn field_len(payload: usize) -> std::result::Result<usize, RowCodecError> {
    u32::try_from(payload).map_err(|_| RowCodecError::InvalidLength)?;
    payload.checked_add(5).ok_or(RowCodecError::InvalidLength)
}

fn capture_limit(resource: &'static str, limit: usize) -> Error {
    Error::CaptureLimitExceeded { resource, limit }
}

fn uuid_bytes(value: &[u8]) -> std::result::Result<[u8; 16], RowCodecError> {
    let bytes = value.try_into().map_err(|_| RowCodecError::InvalidLength)?;
    let uuid = Uuid::from_bytes(bytes);
    if uuid.get_version() != Some(Version::Random) || uuid.get_variant() != Variant::RFC4122 {
        return Err(RowCodecError::InvalidUuid);
    }
    Ok(bytes)
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RowCodecError {
    UnknownVersion,
    Truncated,
    DuplicateField,
    MissingField(u8),
    InvalidLength,
    InvalidUuid,
    InvalidValue,
    InvalidRow,
    DuplicateRow,
    DuplicateUniqueKey,
    TooManyChanges,
    FrameTooLarge,
    NullPrimaryKey,
    InvalidKey(KeyError),
    #[cfg(test)]
    InvalidBatch,
}

impl fmt::Display for RowCodecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownVersion => f.write_str("unknown row frame version"),
            Self::Truncated => f.write_str("truncated row frame"),
            Self::DuplicateField => f.write_str("duplicate row field"),
            Self::MissingField(tag) => write!(f, "missing row field {tag}"),
            Self::InvalidLength => f.write_str("invalid row field length"),
            Self::InvalidUuid => f.write_str("row identity is not a UUID v4"),
            Self::InvalidValue => f.write_str("invalid stored SQLite value"),
            Self::InvalidRow => f.write_str("invalid row frame"),
            Self::DuplicateRow => f.write_str("row operation contains a duplicate logical row"),
            Self::DuplicateUniqueKey => {
                f.write_str("row operation contains a duplicate UNIQUE key")
            }
            Self::TooManyChanges => f.write_str("row operation contains too many changes"),
            Self::FrameTooLarge => f.write_str("row operation frame is too large"),
            Self::NullPrimaryKey => f.write_str("primary key value is NULL"),
            Self::InvalidKey(error) => write!(f, "invalid Homebase row key: {error}"),
            #[cfg(test)]
            Self::InvalidBatch => f.write_str("admitted row operation has an invalid envelope"),
        }
    }
}

#[cfg(test)]
mod tests {
    use homebase_core::seal::Seal;
    use homebase_core::tag::{
        AdmissionSeq, AdmissionTag, CipherEpoch, DeviceChecksum, DeviceEntry, DeviceId, DeviceSeq,
        DeviceTag, Ver,
    };
    use rusqlite::config::DbConfig;

    use super::*;
    use crate::commit::footprint::assert_explicit_range_assertions;
    use crate::logical::alter::AlterTableOperation;
    use crate::logical::guard::MutationKind;
    use crate::logical::index::IndexOperation;
    use crate::logical::schema::{
        CreateColumn, CreateTableSpec, CreateUnique, SqlName, TypeDeclaration,
    };

    #[test]
    fn range_retirements_prune_only_redundant_point_deletes() {
        let prefix = Key::from_bytes([b"foreign".as_slice(), b"parent".as_slice()]).unwrap();
        let covered = Key::from_bytes([
            b"foreign".as_slice(),
            b"parent".as_slice(),
            b"child".as_slice(),
        ])
        .unwrap();
        let sibling = Key::from_bytes([
            b"foreign".as_slice(),
            b"other".as_slice(),
            b"child".as_slice(),
        ])
        .unwrap();
        let mut mutations = vec![
            Mutation::Delete {
                key: covered.clone(),
            },
            Mutation::Set {
                key: covered.clone(),
                value: b"replacement".to_vec(),
            },
            Mutation::Delete {
                key: sibling.clone(),
            },
            Mutation::DeleteRange {
                range: Range::Prefix(prefix.clone()),
            },
        ];

        prune_redundant_point_deletes(&mut mutations);

        assert_eq!(
            mutations,
            vec![
                Mutation::Set {
                    key: covered,
                    value: b"replacement".to_vec(),
                },
                Mutation::Delete { key: sibling },
                Mutation::DeleteRange {
                    range: Range::Prefix(prefix),
                },
            ]
        );
    }

    fn definition() -> CreateTable {
        CreateTable::new(
            "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT, payload BLOB)",
            CreateTableSpec {
                name: SqlName::new("notes".into()),
                mode: Default::default(),
                storage: crate::logical::schema::TableStorage::Rowid,
                columns: vec![
                    CreateColumn {
                        name: SqlName::new("id".into()),
                        declared_type: TypeDeclaration::integer(),
                        not_null: false,
                        not_null_name: None,
                        default: None,
                        primary_key: Some(0),
                    },
                    CreateColumn {
                        name: SqlName::new("body".into()),
                        declared_type: TypeDeclaration::text(),
                        not_null: false,
                        not_null_name: None,
                        default: None,
                        primary_key: None,
                    },
                    CreateColumn {
                        name: SqlName::new("payload".into()),
                        declared_type: TypeDeclaration::blob(),
                        not_null: false,
                        not_null_name: None,
                        default: None,
                        primary_key: None,
                    },
                ],
                unique_constraints: Vec::new(),
                foreign_keys: Vec::new(),
                primary_key_name: None,
                checks: Vec::new(),
            },
        )
    }

    fn connection(created: &CreateTable) -> Connection {
        let connection = Connection::open_in_memory().unwrap();
        catalog::initialize(&connection).unwrap();
        connection.execute(created.sql(), ()).unwrap();
        catalog::insert(&connection, created).unwrap();
        connection
    }

    fn add_index(connection: &Connection, sql: &str) {
        let crate::sql::ValidatedExecute::CreateIndex(spec) =
            crate::sql::validate_execute(sql).unwrap()
        else {
            unreachable!()
        };
        connection.execute(sql, ()).unwrap();
        let operation = IndexOperation::prepare_create(connection, sql, &spec).unwrap();
        operation.record_catalog(connection).unwrap();
    }

    fn alter_table(connection: &Connection, sql: &str) {
        let operation = match crate::sql::validate_execute(sql).unwrap() {
            crate::sql::ValidatedExecute::AddColumn(spec) => {
                AlterTableOperation::prepare_add_column(connection, sql, &spec).unwrap()
            }
            crate::sql::ValidatedExecute::DropColumn(spec) => {
                AlterTableOperation::prepare_drop_column(connection, sql, &spec).unwrap()
            }
            _ => unreachable!(),
        };
        operation.apply(connection).unwrap();
    }

    fn without_rowid_definition() -> CreateTable {
        let sql = "CREATE TABLE memberships (
            tenant TEXT,
            member INTEGER,
            body TEXT,
            PRIMARY KEY (member, tenant)
        ) WITHOUT ROWID";
        let crate::sql::ValidatedExecute::CreateTable(spec) =
            crate::sql::validate_execute(sql).unwrap()
        else {
            unreachable!()
        };
        CreateTable::new(sql, spec)
    }

    fn unique_definition() -> CreateTable {
        CreateTable::new(
            "CREATE TABLE accounts (
                id INTEGER PRIMARY KEY,
                organization TEXT,
                email TEXT,
                UNIQUE (organization, email)
            )",
            CreateTableSpec {
                name: SqlName::new("accounts".into()),
                mode: Default::default(),
                storage: crate::logical::schema::TableStorage::Rowid,
                columns: vec![
                    CreateColumn {
                        name: SqlName::new("id".into()),
                        declared_type: TypeDeclaration::integer(),
                        not_null: false,
                        not_null_name: None,
                        default: None,
                        primary_key: Some(0),
                    },
                    CreateColumn {
                        name: SqlName::new("organization".into()),
                        declared_type: TypeDeclaration::text(),
                        not_null: false,
                        not_null_name: None,
                        default: None,
                        primary_key: None,
                    },
                    CreateColumn {
                        name: SqlName::new("email".into()),
                        declared_type: TypeDeclaration::text(),
                        not_null: false,
                        not_null_name: None,
                        default: None,
                        primary_key: None,
                    },
                ],
                unique_constraints: vec![CreateUnique {
                    name: None,
                    columns: vec![
                        SqlName::new("organization".into()),
                        SqlName::new("email".into()),
                    ],
                }],
                foreign_keys: Vec::new(),
                primary_key_name: None,
                checks: Vec::new(),
            },
        )
    }

    fn overlapping_unique_definition() -> CreateTable {
        CreateTable::new(
            "CREATE TABLE profiles (
                id INTEGER PRIMARY KEY,
                tenant TEXT,
                email TEXT CONSTRAINT email_key UNIQUE,
                username TEXT UNIQUE,
                CONSTRAINT tenant_email UNIQUE (tenant, email),
                CONSTRAINT tenant_username UNIQUE (tenant, username)
            )",
            CreateTableSpec {
                name: SqlName::new("profiles".into()),
                mode: Default::default(),
                storage: crate::logical::schema::TableStorage::Rowid,
                columns: vec![
                    CreateColumn {
                        name: SqlName::new("id".into()),
                        declared_type: TypeDeclaration::integer(),
                        not_null: false,
                        not_null_name: None,
                        default: None,
                        primary_key: Some(0),
                    },
                    CreateColumn {
                        name: SqlName::new("tenant".into()),
                        declared_type: TypeDeclaration::text(),
                        not_null: false,
                        not_null_name: None,
                        default: None,
                        primary_key: None,
                    },
                    CreateColumn {
                        name: SqlName::new("email".into()),
                        declared_type: TypeDeclaration::text(),
                        not_null: false,
                        not_null_name: None,
                        default: None,
                        primary_key: None,
                    },
                    CreateColumn {
                        name: SqlName::new("username".into()),
                        declared_type: TypeDeclaration::text(),
                        not_null: false,
                        not_null_name: None,
                        default: None,
                        primary_key: None,
                    },
                ],
                unique_constraints: vec![
                    CreateUnique {
                        name: Some(SqlName::new("email_key".into())),
                        columns: vec![SqlName::new("email".into())],
                    },
                    CreateUnique {
                        name: None,
                        columns: vec![SqlName::new("username".into())],
                    },
                    CreateUnique {
                        name: Some(SqlName::new("tenant_email".into())),
                        columns: vec![SqlName::new("tenant".into()), SqlName::new("email".into())],
                    },
                    CreateUnique {
                        name: Some(SqlName::new("tenant_username".into())),
                        columns: vec![
                            SqlName::new("tenant".into()),
                            SqlName::new("username".into()),
                        ],
                    },
                ],
                foreign_keys: Vec::new(),
                primary_key_name: None,
                checks: Vec::new(),
            },
        )
    }

    fn profile(id: i64, tenant: &str, email: &str, username: &str) -> CapturedRow {
        CapturedRow {
            table: "profiles".into(),
            rowid: id,
            values: vec![
                StoredValue::Integer(id),
                StoredValue::Text(tenant.as_bytes().to_vec()),
                StoredValue::Text(email.as_bytes().to_vec()),
                StoredValue::Text(username.as_bytes().to_vec()),
            ],
        }
    }

    fn inserted(connection: &Connection) -> RowSet {
        RowSet::from_captured(
            connection,
            &[
                CapturedRow {
                    table: "notes".into(),
                    rowid: 7,
                    values: vec![
                        StoredValue::Integer(7),
                        StoredValue::Text(b"hello".to_vec()),
                        StoredValue::Blob(vec![0, 1]),
                    ],
                },
                CapturedRow {
                    table: "notes".into(),
                    rowid: 9,
                    values: vec![
                        StoredValue::Integer(9),
                        StoredValue::Null,
                        StoredValue::Blob(Vec::new()),
                    ],
                },
            ],
        )
        .unwrap()
        .unwrap()
    }

    fn note(id: i64, body: &str) -> CapturedRow {
        CapturedRow {
            table: "notes".into(),
            rowid: id,
            values: vec![
                StoredValue::Integer(id),
                StoredValue::Text(body.as_bytes().to_vec()),
                StoredValue::Blob(Vec::new()),
            ],
        }
    }

    #[test]
    fn capture_budget_fails_deterministically_and_can_be_reused() {
        let event = CapturedChange::Insert(note(7, "hello"));
        let retained = event.retained_bytes().unwrap();

        let mut count = CaptureBudget::with_limits(1, usize::MAX);
        count.record(&event).unwrap();
        assert!(matches!(
            count.record(&event),
            Err(Error::CaptureLimitExceeded {
                resource: "row-change count",
                limit: 1,
            })
        ));
        count.reset();
        count.record(&event).unwrap();

        let mut bytes = CaptureBudget::with_limits(usize::MAX, retained - 1);
        assert!(matches!(
            bytes.record(&event),
            Err(Error::CaptureLimitExceeded {
                resource: "row-capture bytes",
                limit,
            }) if limit == retained - 1
        ));
    }

    #[test]
    fn normalized_row_changes_enforce_count_and_frame_budgets() {
        let created = definition();
        let connection = connection(&created);
        let catalog = CatalogSnapshot::load(&connection).unwrap();
        let changes =
            RowChanges::from_catalog(&catalog, vec![CapturedChange::Insert(note(7, "hello"))])
                .unwrap()
                .unwrap();

        assert_eq!(
            changes.validate_budget_with(0, usize::MAX),
            Err(RowCodecError::TooManyChanges)
        );
        assert_eq!(
            changes.validate_budget_with(usize::MAX, 1),
            Err(RowCodecError::FrameTooLarge)
        );
        changes
            .validate_budget_with(changes.tables[0].rows.len(), changes.encode().len())
            .unwrap();
    }

    #[test]
    fn statement_delta_folds_repeated_touches_and_cancels_transient_rows() {
        let created = definition();
        let connection = connection(&created);
        connection
            .execute("INSERT INTO notes VALUES (7, 'before', x'')", ())
            .unwrap();
        let before = note(7, "before");
        let middle = note(7, "middle");
        let after = note(7, "after");
        let transient = note(9, "transient");
        let catalog = CatalogSnapshot::load(&connection).unwrap();
        let transient_set = RowSet::from_catalog(&catalog, std::slice::from_ref(&transient))
            .unwrap()
            .unwrap();
        let transient_key = transient_set.row_key(&transient_set.rows[0]).unwrap();
        let changes = RowChanges::from_catalog(
            &catalog,
            vec![
                CapturedChange::Update {
                    before: before.clone(),
                    after: middle.clone(),
                },
                CapturedChange::Update {
                    before: middle,
                    after: after.clone(),
                },
                CapturedChange::Insert(transient.clone()),
                CapturedChange::Delete(transient),
            ],
        )
        .unwrap()
        .unwrap();

        assert_eq!(changes.tables[0].rows.len(), 1);
        assert_eq!(RowChanges::decode(&changes.encode()).unwrap(), changes);
        let lowered = changes.to_homebase().unwrap();
        assert!(matches!(
            lowered.mutations.as_slice(),
            [Mutation::Set { .. }]
        ));
        assert!(
            lowered
                .guards
                .entries()
                .iter()
                .all(|guard| guard.target() != &transient_key),
            "a canceled transient row leaked into the guard plan"
        );

        changes.apply(&connection).unwrap();
        assert_eq!(
            connection
                .query_row("SELECT body FROM notes WHERE id = 7", (), |row| {
                    row.get::<_, String>(0)
                })
                .unwrap(),
            "after"
        );
        changes.restore_materialized(&connection).unwrap();
        assert_eq!(
            connection
                .query_row("SELECT body FROM notes WHERE id = 7", (), |row| {
                    row.get::<_, String>(0)
                })
                .unwrap(),
            "before"
        );
    }

    #[test]
    fn statement_delta_folds_upsert_insert_then_update_into_one_insert() {
        let created = definition();
        let connection = connection(&created);
        let catalog = CatalogSnapshot::load(&connection).unwrap();
        let inserted = note(7, "inserted");
        let updated = note(7, "updated");
        let changes = RowChanges::from_catalog(
            &catalog,
            vec![
                CapturedChange::Insert(inserted.clone()),
                CapturedChange::Update {
                    before: inserted,
                    after: updated,
                },
            ],
        )
        .unwrap()
        .unwrap();

        assert_eq!(changes.tables[0].rows.len(), 1);
        assert!(changes.tables[0].rows[0].before.is_none());
        assert!(changes.tables[0].rows[0].after.is_some());
        assert!(matches!(
            changes.to_homebase().unwrap().mutations.as_slice(),
            [Mutation::Set { .. }]
        ));

        changes.apply(&connection).unwrap();
        assert_eq!(
            connection
                .query_row("SELECT body FROM notes WHERE id = 7", (), |row| {
                    row.get::<_, String>(0)
                })
                .unwrap(),
            "updated"
        );
        changes.restore_materialized(&connection).unwrap();
        assert_eq!(
            connection
                .query_row("SELECT count(*) FROM notes", (), |row| row.get::<_, i64>(0))
                .unwrap(),
            0
        );
    }

    #[test]
    fn statement_delta_discards_unchanged_and_transient_upsert_streams() {
        let created = definition();
        let connection = connection(&created);
        let catalog = CatalogSnapshot::load(&connection).unwrap();
        let unchanged = note(7, "same");
        assert!(
            RowChanges::from_catalog(
                &catalog,
                vec![CapturedChange::Update {
                    before: unchanged.clone(),
                    after: unchanged,
                }],
            )
            .unwrap()
            .is_none()
        );

        let transient = note(9, "transient");
        assert!(
            RowChanges::from_catalog(
                &catalog,
                vec![
                    CapturedChange::Insert(transient.clone()),
                    CapturedChange::Delete(transient),
                ],
            )
            .unwrap()
            .is_none()
        );
    }

    #[test]
    fn statement_delta_treats_delete_then_insert_as_one_replacement() {
        let created = definition();
        let connection = connection(&created);
        connection
            .execute("INSERT INTO notes VALUES (7, 'before', x'')", ())
            .unwrap();
        let catalog = CatalogSnapshot::load(&connection).unwrap();
        let changes = RowChanges::from_catalog(
            &catalog,
            vec![
                CapturedChange::Delete(note(7, "before")),
                CapturedChange::Insert(note(7, "after")),
            ],
        )
        .unwrap()
        .unwrap();

        assert_eq!(changes.tables[0].rows.len(), 1);
        assert!(changes.tables[0].rows[0].before.is_some());
        assert!(changes.tables[0].rows[0].after.is_some());
        changes.apply(&connection).unwrap();
        assert_eq!(
            connection
                .query_row("SELECT body FROM notes", (), |row| row.get::<_, String>(0))
                .unwrap(),
            "after"
        );
        changes.restore_materialized(&connection).unwrap();
        assert_eq!(
            connection
                .query_row("SELECT body FROM notes", (), |row| row.get::<_, String>(0))
                .unwrap(),
            "before"
        );
    }

    #[test]
    fn statement_delta_replaces_multiple_unique_victims_and_restores_them_exactly() {
        let created = overlapping_unique_definition();
        let connection = connection(&created);
        connection
            .execute(
                "INSERT INTO profiles VALUES
                    (1, 'acme', 'shared@example.com', 'alpha'),
                    (2, 'other', 'other@example.com', 'beta')",
                (),
            )
            .unwrap();
        let catalog = CatalogSnapshot::load(&connection).unwrap();
        let changes = RowChanges::from_catalog(
            &catalog,
            vec![
                CapturedChange::Delete(profile(1, "acme", "shared@example.com", "alpha")),
                CapturedChange::Delete(profile(2, "other", "other@example.com", "beta")),
                CapturedChange::Insert(profile(3, "acme", "shared@example.com", "beta")),
            ],
        )
        .unwrap()
        .unwrap();

        assert_eq!(changes.tables[0].rows.len(), 3);
        assert_eq!(RowChanges::decode(&changes.encode()).unwrap(), changes);
        let lowered = changes.to_homebase().unwrap();
        let mut asserted = lowered
            .mutations
            .iter()
            .filter_map(|mutation| match mutation {
                Mutation::Set { key, .. } => Some(key.clone()),
                Mutation::Delete { .. } | Mutation::DeleteRange { .. } => None,
            })
            .collect::<Vec<_>>();
        asserted.extend([
            active_primary_index_key(created.table_id()),
            write_revision_key(created.table_id()),
        ]);
        assert_explicit_range_assertions(&lowered.footprint, &asserted);
        assert!(
            lowered
                .mutations
                .iter()
                .all(|mutation| lowered.footprint.writes().contains(mutation.key()))
        );
        crate::logical::guard::validate_compiled_output(
            OperationFamily::RowChanges,
            &lowered.mutations,
            &lowered.guards,
        )
        .unwrap();
        assert!(
            lowered
                .mutations
                .iter()
                .any(|mutation| matches!(mutation, Mutation::Delete { .. }))
        );
        assert!(
            lowered
                .mutations
                .iter()
                .any(|mutation| matches!(mutation, Mutation::Set { .. }))
        );

        connection
            .execute_batch(
                "CREATE TABLE replay_audit (event TEXT NOT NULL);
                 CREATE TRIGGER audit_profile_insert AFTER INSERT ON profiles BEGIN
                     INSERT INTO replay_audit VALUES ('insert');
                 END;
                 CREATE TRIGGER audit_profile_delete AFTER DELETE ON profiles BEGIN
                     INSERT INTO replay_audit VALUES ('delete');
                 END",
            )
            .unwrap();
        connection
            .set_db_config(DbConfig::SQLITE_DBCONFIG_ENABLE_TRIGGER, true)
            .unwrap();
        connection
            .set_db_config(DbConfig::SQLITE_DBCONFIG_ENABLE_FKEY, true)
            .unwrap();
        changes.apply(&connection).unwrap();
        assert!(
            connection
                .db_config(DbConfig::SQLITE_DBCONFIG_ENABLE_TRIGGER)
                .unwrap()
        );
        assert!(
            connection
                .db_config(DbConfig::SQLITE_DBCONFIG_ENABLE_FKEY)
                .unwrap()
        );
        assert_eq!(
            connection
                .query_row("SELECT count(*) FROM replay_audit", (), |row| {
                    row.get::<_, i64>(0)
                })
                .unwrap(),
            0
        );
        assert_eq!(
            connection
                .prepare("SELECT id, tenant, email, username FROM profiles ORDER BY id")
                .unwrap()
                .query_map((), |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                    ))
                })
                .unwrap()
                .collect::<rusqlite::Result<Vec<_>>>()
                .unwrap(),
            [(3, "acme".into(), "shared@example.com".into(), "beta".into(),)]
        );

        changes.restore_materialized(&connection).unwrap();
        assert!(
            connection
                .db_config(DbConfig::SQLITE_DBCONFIG_ENABLE_TRIGGER)
                .unwrap()
        );
        assert!(
            connection
                .db_config(DbConfig::SQLITE_DBCONFIG_ENABLE_FKEY)
                .unwrap()
        );
        assert_eq!(
            connection
                .query_row("SELECT count(*) FROM replay_audit", (), |row| {
                    row.get::<_, i64>(0)
                })
                .unwrap(),
            0
        );
        assert_eq!(
            connection
                .prepare("SELECT id, tenant, email, username FROM profiles ORDER BY id")
                .unwrap()
                .query_map((), |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                    ))
                })
                .unwrap()
                .collect::<rusqlite::Result<Vec<_>>>()
                .unwrap(),
            [
                (
                    1,
                    "acme".into(),
                    "shared@example.com".into(),
                    "alpha".into(),
                ),
                (2, "other".into(), "other@example.com".into(), "beta".into(),),
            ]
        );
    }

    #[test]
    fn statement_delta_replay_rejects_an_invalid_final_foreign_key_state_atomically() {
        let (connection, _parent, _child) = foreign_key_tables();
        connection
            .set_db_config(DbConfig::SQLITE_DBCONFIG_ENABLE_TRIGGER, true)
            .unwrap();
        connection
            .set_db_config(DbConfig::SQLITE_DBCONFIG_ENABLE_FKEY, true)
            .unwrap();
        let catalog = CatalogSnapshot::load(&connection).unwrap();
        let changes = RowChanges::from_catalog(
            &catalog,
            vec![CapturedChange::Insert(CapturedRow {
                table: "children".into(),
                rowid: 10,
                values: vec![
                    StoredValue::Integer(10),
                    StoredValue::Integer(999),
                    StoredValue::Text(b"orphan".to_vec()),
                ],
            })],
        )
        .unwrap()
        .unwrap();

        assert!(matches!(
            changes.apply(&connection),
            Err(Error::CommitConflict(_))
        ));
        assert_eq!(
            connection
                .query_row("SELECT count(*) FROM children", (), |row| {
                    row.get::<_, i64>(0)
                })
                .unwrap(),
            0
        );
        assert!(
            connection
                .db_config(DbConfig::SQLITE_DBCONFIG_ENABLE_TRIGGER)
                .unwrap()
        );
        assert!(
            connection
                .db_config(DbConfig::SQLITE_DBCONFIG_ENABLE_FKEY)
                .unwrap()
        );
    }

    #[test]
    fn statement_delta_emits_exact_mandatory_guards_for_each_net_row_shape() {
        let created = definition();
        let connection = connection(&created);
        let before = note(7, "before");
        let after = note(7, "after");
        let catalog = CatalogSnapshot::load(&connection).unwrap();
        let rows = RowSet::from_catalog(&catalog, std::slice::from_ref(&after))
            .unwrap()
            .unwrap();
        let row_key = rows.row_key(&rows.rows[0]).unwrap();
        let required = [
            (
                active_primary_index_key(created.table_id()),
                crate::logical::guard::GuardClass::Invariant,
                GuardReason::PrimaryIndex,
            ),
            (
                write_revision_key(created.table_id()),
                crate::logical::guard::GuardClass::Invariant,
                GuardReason::WriteContract,
            ),
        ];

        for (event, mutation_kind, expected_row_classes) in [
            (
                CapturedChange::Insert(after.clone()),
                MutationKind::Set,
                &[
                    crate::logical::guard::GuardClass::Invariant,
                    crate::logical::guard::GuardClass::Write,
                ][..],
            ),
            (
                CapturedChange::Delete(before.clone()),
                MutationKind::Delete,
                &[crate::logical::guard::GuardClass::Write][..],
            ),
            (
                CapturedChange::Update { before, after },
                MutationKind::Set,
                &[
                    crate::logical::guard::GuardClass::Invariant,
                    crate::logical::guard::GuardClass::Write,
                ][..],
            ),
        ] {
            let lowered = RowChanges::from_catalog(&catalog, vec![event])
                .unwrap()
                .unwrap()
                .to_homebase()
                .unwrap();
            assert_eq!(lowered.mutations.len(), 1);
            assert_eq!(lowered.mutations[0].key(), &row_key);
            assert_eq!(
                match &lowered.mutations[0] {
                    Mutation::Set { .. } => MutationKind::Set,
                    Mutation::Delete { .. } => MutationKind::Delete,
                    Mutation::DeleteRange { .. } => MutationKind::DeletePrefix,
                },
                mutation_kind
            );
            for class in expected_row_classes {
                assert!(lowered.guards.entries().iter().any(|guard| {
                    guard.target() == &row_key
                        && guard.class() == *class
                        && guard.reason() == GuardReason::RowIdentity
                        && guard.family() == TargetFamily::Row
                }));
            }
            for (target, class, reason) in &required {
                assert!(lowered.guards.entries().iter().any(|guard| {
                    guard.target() == target && guard.class() == *class && guard.reason() == *reason
                }));
            }
            crate::logical::guard::validate_compiled_output(
                OperationFamily::RowChanges,
                &lowered.mutations,
                &lowered.guards,
            )
            .unwrap();
        }
    }

    #[test]
    fn statement_delta_codec_rejects_every_truncated_prefix() {
        let created = definition();
        let connection = connection(&created);
        let catalog = CatalogSnapshot::load(&connection).unwrap();
        let changes =
            RowChanges::from_catalog(&catalog, vec![CapturedChange::Insert(note(7, "hello"))])
                .unwrap()
                .unwrap();
        let encoded = changes.encode();

        for length in 0..encoded.len() {
            assert!(
                RowChanges::decode(&encoded[..length]).is_err(),
                "accepted truncated row-change frame at {length}"
            );
        }
    }

    #[test]
    fn statement_delta_codec_rejects_malformed_nested_envelopes() {
        let created = definition();
        let connection = connection(&created);
        let catalog = CatalogSnapshot::load(&connection).unwrap();
        let changes =
            RowChanges::from_catalog(&catalog, vec![CapturedChange::Insert(note(7, "hello"))])
                .unwrap()
                .unwrap();

        assert_eq!(
            RowChanges::decode(&[ROW_CHANGES_FRAME_VERSION + 1]),
            Err(RowCodecError::UnknownVersion)
        );
        assert_eq!(
            RowChanges::decode(&[ROW_CHANGES_FRAME_VERSION]),
            Err(RowCodecError::MissingField(TAG_CHANGED_TABLE))
        );

        let table = changes.tables[0].encode();
        let mut duplicate_table = Writer::new();
        duplicate_table.u8(ROW_CHANGES_FRAME_VERSION);
        duplicate_table.field(TAG_CHANGED_TABLE, &table).unwrap();
        duplicate_table.field(TAG_CHANGED_TABLE, &table).unwrap();
        assert_eq!(
            RowChanges::decode(&duplicate_table.finish()),
            Err(RowCodecError::InvalidRow)
        );

        let mut missing_rules = Writer::new();
        missing_rules.u8(TABLE_CHANGES_FRAME_VERSION);
        missing_rules
            .field(TAG_CHANGE_ROW, &changes.tables[0].rows[0].encode())
            .unwrap();
        assert_eq!(
            TableChanges::decode(&missing_rules.finish()),
            Err(RowCodecError::MissingField(TAG_CHANGE_RULES))
        );

        let rules = changes.tables[0].rules.encode();
        let mut duplicate_rules = Writer::new();
        duplicate_rules.u8(TABLE_CHANGES_FRAME_VERSION);
        duplicate_rules.field(TAG_CHANGE_RULES, &rules).unwrap();
        duplicate_rules.field(TAG_CHANGE_RULES, &rules).unwrap();
        duplicate_rules
            .field(TAG_CHANGE_ROW, &changes.tables[0].rows[0].encode())
            .unwrap();
        assert_eq!(
            TableChanges::decode(&duplicate_rules.finish()),
            Err(RowCodecError::DuplicateField)
        );

        let mut no_rows = Writer::new();
        no_rows.u8(TABLE_CHANGES_FRAME_VERSION);
        no_rows.field(TAG_CHANGE_RULES, &rules).unwrap();
        assert_eq!(
            TableChanges::decode(&no_rows.finish()),
            Err(RowCodecError::InvalidRow)
        );

        let mut empty_delta = Writer::new();
        empty_delta.u8(ROW_DELTA_FRAME_VERSION);
        let mut table_with_empty_delta = Writer::new();
        table_with_empty_delta.u8(TABLE_CHANGES_FRAME_VERSION);
        table_with_empty_delta
            .field(TAG_CHANGE_RULES, &rules)
            .unwrap();
        table_with_empty_delta
            .field(TAG_CHANGE_ROW, &empty_delta.finish())
            .unwrap();
        assert_eq!(
            TableChanges::decode(&table_with_empty_delta.finish()),
            Err(RowCodecError::InvalidRow)
        );

        let image = encode_row_image(
            changes.tables[0].rows[0]
                .after
                .as_ref()
                .expect("insert delta has an after-image"),
        );
        let mut duplicate_before = Writer::new();
        duplicate_before.u8(ROW_DELTA_FRAME_VERSION);
        duplicate_before.field(TAG_DELTA_BEFORE, &image).unwrap();
        duplicate_before.field(TAG_DELTA_BEFORE, &image).unwrap();
        assert_eq!(
            RowDelta::decode(&duplicate_before.finish()),
            Err(RowCodecError::DuplicateField)
        );

        let mut unchanged = Writer::new();
        unchanged.u8(ROW_DELTA_FRAME_VERSION);
        unchanged.field(TAG_DELTA_BEFORE, &image).unwrap();
        unchanged.field(TAG_DELTA_AFTER, &image).unwrap();
        let mut unchanged_table = Writer::new();
        unchanged_table.u8(TABLE_CHANGES_FRAME_VERSION);
        unchanged_table.field(TAG_CHANGE_RULES, &rules).unwrap();
        unchanged_table
            .field(TAG_CHANGE_ROW, &unchanged.finish())
            .unwrap();
        assert_eq!(
            TableChanges::decode(&unchanged_table.finish()),
            Err(RowCodecError::InvalidRow)
        );

        let mut rules_with_rows = Writer::new();
        changes.tables[0].rules.encode_into(&mut rules_with_rows);
        rules_with_rows.field(TAG_SET_ROW, &image).unwrap();
        assert_eq!(
            RowRules::decode(&rules_with_rows.finish()),
            Err(RowCodecError::InvalidRow)
        );
    }

    #[test]
    fn statement_delta_normalizes_lineage_and_groups_cross_table_streams() {
        let created = definition();
        let connection = connection(&created);
        let catalog = CatalogSnapshot::load(&connection).unwrap();
        let before = note(7, "before");
        let middle = note(7, "middle");
        let after = note(7, "after");
        let direct = RowChanges::from_catalog(
            &catalog,
            vec![CapturedChange::Update {
                before: before.clone(),
                after: after.clone(),
            }],
        )
        .unwrap()
        .unwrap();
        let chained = RowChanges::from_catalog(
            &catalog,
            vec![
                CapturedChange::Update {
                    before: before.clone(),
                    after: middle.clone(),
                },
                CapturedChange::Update {
                    before: middle,
                    after: after.clone(),
                },
            ],
        )
        .unwrap()
        .unwrap();
        assert_eq!(direct.encode(), chained.encode());
        assert_eq!(
            direct.to_homebase().unwrap().mutations,
            chained.to_homebase().unwrap().mutations
        );

        assert!(matches!(
            RowChanges::from_catalog(
                &catalog,
                vec![
                    CapturedChange::Update {
                        before: before.clone(),
                        after: note(7, "first"),
                    },
                    CapturedChange::Update { before, after },
                ],
            ),
            Err(Error::CaptureInvariant(
                "SQLite update before-image contradicts the statement delta"
            ))
        ));

        let sql = "CREATE TABLE tasks (id INTEGER PRIMARY KEY, body TEXT, payload BLOB)";
        let crate::sql::ValidatedExecute::CreateTable(spec) =
            crate::sql::validate_execute(sql).unwrap()
        else {
            unreachable!()
        };
        let tasks = CreateTable::new(sql, spec);
        connection.execute(sql, ()).unwrap();
        catalog::insert(&connection, &tasks).unwrap();
        let catalog = CatalogSnapshot::load(&connection).unwrap();
        let mut task = note(8, "task");
        task.table = "tasks".into();
        let note_insert = CapturedChange::Insert(note(7, "note"));
        let task_insert = CapturedChange::Insert(task);
        let changes =
            RowChanges::from_catalog(&catalog, vec![note_insert.clone(), task_insert.clone()])
                .unwrap()
                .unwrap();
        let reversed = RowChanges::from_catalog(&catalog, vec![task_insert, note_insert])
            .unwrap()
            .unwrap();
        assert_eq!(changes.tables.len(), 2);
        assert_eq!(changes.encode(), reversed.encode());
        assert_eq!(
            changes.to_homebase().unwrap().mutations,
            reversed.to_homebase().unwrap().mutations
        );
        assert_eq!(RowChanges::decode(&changes.encode()).unwrap(), changes);
        let lowered = changes.to_homebase().unwrap();
        for table in &changes.tables {
            assert!(
                lowered
                    .guards
                    .entries()
                    .iter()
                    .any(|guard| { guard.target() == &write_revision_key(table.rules.table) })
            );
        }
        changes.apply(&connection).unwrap();
        for table in ["notes", "tasks"] {
            assert_eq!(
                connection
                    .query_row(&format!("SELECT count(*) FROM {table}"), (), |row| {
                        row.get::<_, i64>(0)
                    })
                    .unwrap(),
                1
            );
        }
        changes.restore_materialized(&connection).unwrap();
        for table in ["notes", "tasks"] {
            assert_eq!(
                connection
                    .query_row(&format!("SELECT count(*) FROM {table}"), (), |row| {
                        row.get::<_, i64>(0)
                    })
                    .unwrap(),
                0
            );
        }

        let mut noncanonical = changes;
        noncanonical.tables.0.reverse();
        assert_eq!(
            RowChanges::decode(&noncanonical.encode()),
            Err(RowCodecError::InvalidRow)
        );
    }

    fn foreign_key_tables() -> (Connection, CreateTable, CreateTable) {
        let connection = Connection::open_in_memory().unwrap();
        catalog::initialize(&connection).unwrap();
        let parent_sql = "CREATE TABLE parents (id INTEGER PRIMARY KEY, body TEXT)";
        let crate::sql::ValidatedExecute::CreateTable(parent_spec) =
            crate::sql::validate_execute(parent_sql).unwrap()
        else {
            unreachable!()
        };
        let parent = CreateTable::new(parent_sql, parent_spec);
        connection.execute(parent.sql(), ()).unwrap();
        catalog::insert(&connection, &parent).unwrap();

        let child_sql = "CREATE TABLE children (
            id INTEGER PRIMARY KEY,
            parent INTEGER REFERENCES parents(id),
            body TEXT
        )";
        let crate::sql::ValidatedExecute::CreateTable(child_spec) =
            crate::sql::validate_execute(child_sql).unwrap()
        else {
            unreachable!()
        };
        let child = CreateTable::prepare(&connection, child_sql, child_spec).unwrap();
        connection.execute(child.sql(), ()).unwrap();
        catalog::insert(&connection, &child).unwrap();
        (connection, parent, child)
    }

    fn unique_foreign_key_tables() -> (Connection, CreateTable, CreateTable) {
        let connection = Connection::open_in_memory().unwrap();
        catalog::initialize(&connection).unwrap();
        let parent_sql = "CREATE TABLE accounts (
            id INTEGER PRIMARY KEY,
            tenant TEXT,
            email TEXT,
            UNIQUE (tenant, email)
        )";
        let crate::sql::ValidatedExecute::CreateTable(parent_spec) =
            crate::sql::validate_execute(parent_sql).unwrap()
        else {
            unreachable!()
        };
        let parent = CreateTable::new(parent_sql, parent_spec);
        connection.execute(parent.sql(), ()).unwrap();
        catalog::insert(&connection, &parent).unwrap();

        let child_sql = "CREATE TABLE messages (
            id INTEGER PRIMARY KEY,
            tenant TEXT,
            recipient TEXT,
            FOREIGN KEY (tenant, recipient) REFERENCES accounts (tenant, email)
        )";
        let crate::sql::ValidatedExecute::CreateTable(child_spec) =
            crate::sql::validate_execute(child_sql).unwrap()
        else {
            unreachable!()
        };
        let child = CreateTable::prepare(&connection, child_sql, child_spec).unwrap();
        connection.execute(child.sql(), ()).unwrap();
        catalog::insert(&connection, &child).unwrap();
        (connection, parent, child)
    }

    fn admit(mutations: Vec<Mutation>) -> AdmittedBatch<Vec<u8>> {
        let device = DeviceId([7; 16]);
        let device_seq = DeviceSeq(3);
        let admission_seq = AdmissionSeq(9);
        let entries = mutations
            .into_iter()
            .enumerate()
            .map(|(index, mutation)| homebase_core::tag::AdmittedEntry {
                device_entry: DeviceEntry {
                    mutation,
                    tag: DeviceTag {
                        device,
                        device_seq,
                        ver: Ver(index as u64 + 1),
                        cipher_epoch: CipherEpoch(0),
                    },
                    seal: Seal::empty_aead_v1(),
                },
                admission: AdmissionTag {
                    admission_seq,
                    op_index: index as u32,
                },
            })
            .collect();
        AdmittedBatch {
            admission_seq,
            device,
            device_seq,
            checksum: DeviceChecksum::EMPTY,
            entries,
        }
    }

    fn stored_row_frame(revision: SchemaRevisionId, row: &Row) -> Vec<u8> {
        let mut writer = Writer::new();
        writer.u8(ROW_FRAME_VERSION);
        writer
            .field(TAG_SCHEMA_REVISION, &revision.as_bytes())
            .unwrap();
        for (column, value) in &row.values {
            writer
                .field(TAG_COLUMN_VALUE, &encode_column_value(*column, value))
                .unwrap();
        }
        writer.finish()
    }

    #[test]
    fn insert_codec_and_homebase_envelope_roundtrip() {
        let created = definition();
        let connection = connection(&created);
        let inserted = inserted(&connection);

        assert_eq!(RowSet::decode(&inserted.encode()).unwrap(), inserted);
        let lowered = inserted.to_homebase().unwrap();
        assert_eq!(lowered.mutations.len(), 2);
        assert_eq!(lowered.footprint.writes().len(), 2);
        assert_eq!(lowered.footprint.constraints().len(), 4);
        let mut expected = lowered
            .mutations
            .iter()
            .map(|mutation| mutation.key().clone())
            .collect::<Vec<_>>();
        expected.extend([
            active_primary_index_key(created.table_id()),
            write_revision_key(created.table_id()),
        ]);
        assert_explicit_range_assertions(&lowered.footprint, &expected);
        assert_eq!(
            lowered.mutations[0].key(),
            &primary_key_prefix(&created, &[StoredValue::Integer(7)]).unwrap()
        );
        assert!(
            lowered
                .mutations
                .iter()
                .all(|mutation| lowered.footprint.constraints().contains(mutation.key()))
        );
        for (mutation, assertion) in lowered.mutations.iter().zip(lowered.footprint.writes()) {
            assert_eq!(mutation.key(), assertion);
            assert_eq!(mutation.key().components().len(), 6);
        }
        inserted
            .validate_homebase(&admit(lowered.mutations))
            .unwrap();
    }

    #[test]
    fn materialized_cell_audit_projects_historical_rows_and_rejects_corruption() {
        let created = definition();
        let connection = connection(&created);
        connection
            .execute("INSERT INTO notes VALUES (7, 'hello', x'0102')", ())
            .unwrap();
        let expected = expected_materialized_cells(&connection).unwrap();
        let row_key = expected
            .keys()
            .find(|key| TargetFamily::classify(key) == Some(TargetFamily::Row))
            .unwrap()
            .clone();
        let mut historical = decode_stored_row(&expected[&row_key]).unwrap();
        historical.values.pop();

        let mut actual = expected.clone();
        actual.insert(
            row_key.clone(),
            stored_row_frame(created.schema_revision_id(), &historical),
        );
        validate_materialized_cells(&expected, &actual).unwrap();

        let body = historical.values.get_mut(1).unwrap();
        body.1 = StoredValue::Text(b"corrupt".to_vec());
        actual.insert(
            row_key.clone(),
            stored_row_frame(created.schema_revision_id(), &historical),
        );
        assert!(matches!(
            validate_materialized_cells(&expected, &actual),
            Err(Error::CaptureInvariant(
                "authority row values diverge from materialized rows"
            ))
        ));

        actual = expected.clone();
        actual.remove(&row_key);
        assert!(matches!(
            validate_materialized_cells(&expected, &actual),
            Err(Error::CaptureInvariant(
                "authority row identities diverge from materialized rows"
            ))
        ));
    }

    #[test]
    fn materialized_cell_audit_rejects_corrupt_ownership_cells() {
        let created = unique_definition();
        let connection = connection(&created);
        connection
            .execute(
                "INSERT INTO accounts VALUES (7, 'acme', 'a@example.com')",
                (),
            )
            .unwrap();
        let expected = expected_materialized_cells(&connection).unwrap();
        let unique_key = expected
            .keys()
            .find(|key| TargetFamily::classify(key) == Some(TargetFamily::UniqueOwner))
            .unwrap()
            .clone();
        let mut actual = expected.clone();
        actual.insert(unique_key, b"another owner".to_vec());

        assert!(matches!(
            validate_materialized_cells(&expected, &actual),
            Err(Error::CaptureInvariant(
                "authority UNIQUE or foreign-reference cells diverge from materialized rows"
            ))
        ));
    }

    #[test]
    fn row_codecs_store_rules_once_and_keep_homebase_values_compact() {
        let created = definition();
        let connection = connection(&created);
        let inserted = inserted(&connection);
        let mut first_row = inserted.clone();
        first_row.rows.truncate(1);

        assert_eq!(
            inserted.encode().len(),
            first_row.encode().len() + 5 + encode_row_image(&inserted.rows[1]).len()
        );

        let lowered = inserted.to_homebase().unwrap();
        for (row, mutation) in inserted.rows.iter().zip(&lowered.mutations) {
            let Mutation::Set { value, .. } = mutation else {
                panic!("insert row mutation was not a set")
            };
            let encoded_values = row
                .values
                .iter()
                .map(|(column, value)| 5 + encode_column_value(*column, value).len())
                .sum::<usize>();
            assert_eq!(value.len(), 1 + 5 + 16 + encoded_values);
            assert_eq!(value, &inserted.rules.encode_row(row));
        }
    }

    #[test]
    fn row_set_codec_rejects_duplicate_common_fields() {
        let created = definition();
        let connection = connection(&created);
        let inserted = inserted(&connection);
        let mut malformed = Writer::new();
        malformed.u8(ROW_SET_FRAME_VERSION);
        malformed
            .field(TAG_SET_TABLE, &inserted.rules.table.as_bytes())
            .unwrap();
        malformed
            .field(TAG_SET_TABLE, &inserted.rules.table.as_bytes())
            .unwrap();

        assert_eq!(
            RowSet::decode(&malformed.finish()),
            Err(RowCodecError::DuplicateField)
        );
    }

    #[test]
    fn foreign_reference_cells_follow_child_rows_and_fence_parent_removal() {
        let (connection, parent, child) = foreign_key_tables();
        let child_row = CapturedRow {
            table: "children".into(),
            rowid: 10,
            values: vec![
                StoredValue::Integer(10),
                StoredValue::Integer(7),
                StoredValue::Text(b"body".to_vec()),
            ],
        };
        let inserted = RowSet::from_captured(&connection, std::slice::from_ref(&child_row))
            .unwrap()
            .unwrap();
        let child_key = inserted.row_key(&inserted.rows[0]).unwrap();
        let references = inserted.foreign_references(&inserted.rows[0]).unwrap();
        let reference = &references[0];
        let relationship = child.foreign_keys()[0].id();
        let parent_image = key_image(
            &StoredValue::Integer(7),
            KeyPartRules {
                column: parent.columns()[0].id(),
                affinity: Affinity::Integer,
                rowid_alias: true,
            },
        )
        .unwrap();
        let expected_prefix = foreign_reference_prefix(
            parent.table_id(),
            relationship,
            parent.primary_index_id(),
            vec![parent_image],
        )
        .unwrap();

        assert!(reference.key.starts_with(&expected_prefix));
        assert_eq!(
            reference.key.components().len(),
            codes::FOREIGN_REFERENCE_KEY_FIXED_COMPONENTS + 1 + 1
        );
        assert_eq!(reference.owner, child_key.encode());
        let lowered = inserted.to_homebase().unwrap();
        assert_eq!(lowered.mutations.len(), 2);
        assert_eq!(lowered.footprint.writes().len(), 2);
        assert_explicit_range_assertions(
            &lowered.footprint,
            &[
                child_key.clone(),
                reference.key.clone(),
                active_primary_index_key(child.table_id()),
                write_revision_key(child.table_id()),
            ],
        );
        assert!(lowered.footprint.writes().contains(&reference.key));
        assert!(lowered.footprint.constraints().contains(&reference.key));
        assert!(matches!(
            &lowered.mutations[1],
            Mutation::Set { key, value }
                if key == &reference.key && value == &child_key.encode()
        ));
        inserted
            .validate_homebase(&admit(lowered.mutations.clone()))
            .unwrap();
        let mut missing_reference = admit(lowered.mutations.clone());
        missing_reference.entries.pop();
        assert_eq!(
            inserted.validate_homebase(&missing_reference),
            Err(RowCodecError::InvalidBatch)
        );
        let mut corrupt_reference = admit(lowered.mutations);
        let Mutation::Set { value, .. } = &mut corrupt_reference.entries[1].device_entry.mutation
        else {
            unreachable!()
        };
        value.push(0);
        assert_eq!(
            inserted.validate_homebase(&corrupt_reference),
            Err(RowCodecError::InvalidBatch)
        );

        let deleted =
            DeletedRowsFixture::from_captured(&connection, std::slice::from_ref(&child_row))
                .unwrap()
                .unwrap()
                .to_homebase()
                .unwrap();
        assert!(matches!(
            &deleted.mutations[1],
            Mutation::Delete { key } if key == &reference.key
        ));

        let parent_delete = DeletedRowsFixture::from_captured(
            &connection,
            &[CapturedRow {
                table: "parents".into(),
                rowid: 7,
                values: vec![
                    StoredValue::Integer(7),
                    StoredValue::Text(b"parent".to_vec()),
                ],
            }],
        )
        .unwrap()
        .unwrap()
        .to_homebase()
        .unwrap();
        assert_explicit_range_assertions(
            &parent_delete.footprint,
            &[
                expected_prefix.clone(),
                active_primary_index_key(parent.table_id()),
                write_revision_key(parent.table_id()),
            ],
        );
        assert!(parent_delete.footprint.writes().contains(&expected_prefix));
        assert!(
            parent_delete
                .footprint
                .constraints()
                .contains(&expected_prefix)
        );
        assert!(parent_delete.mutations.iter().any(|mutation| {
            matches!(
                mutation,
                Mutation::DeleteRange {
                    range: Range::Prefix(prefix)
                } if prefix == &expected_prefix
            )
        }));
        assert!(!parent_delete.footprint.constraints().contains(
            &row_prefix(child.table_id(), child.primary_index_id(), Vec::new()).unwrap()
        ));

        let parent_move = UpdatedRowsFixture::from_captured(
            &connection,
            &[(
                CapturedRow {
                    table: "parents".into(),
                    rowid: 7,
                    values: vec![
                        StoredValue::Integer(7),
                        StoredValue::Text(b"parent".to_vec()),
                    ],
                },
                CapturedRow {
                    table: "parents".into(),
                    rowid: 8,
                    values: vec![
                        StoredValue::Integer(8),
                        StoredValue::Text(b"parent".to_vec()),
                    ],
                },
            )],
        )
        .unwrap()
        .unwrap()
        .to_homebase()
        .unwrap();
        assert_explicit_range_assertions(
            &parent_move.footprint,
            &[
                primary_key_prefix(&parent, &[StoredValue::Integer(8)]).unwrap(),
                expected_prefix.clone(),
                active_primary_index_key(parent.table_id()),
                write_revision_key(parent.table_id()),
            ],
        );
        assert!(parent_move.footprint.writes().contains(&expected_prefix));
        assert!(
            parent_move
                .footprint
                .constraints()
                .contains(&expected_prefix)
        );
        assert!(parent_move.mutations.iter().any(|mutation| {
            matches!(
                mutation,
                Mutation::DeleteRange {
                    range: Range::Prefix(prefix)
                } if prefix == &expected_prefix
            )
        }));

        let null_child = RowSet::from_captured(
            &connection,
            &[CapturedRow {
                table: "children".into(),
                rowid: 11,
                values: vec![
                    StoredValue::Integer(11),
                    StoredValue::Null,
                    StoredValue::Text(b"null".to_vec()),
                ],
            }],
        )
        .unwrap()
        .unwrap();
        assert!(
            null_child
                .foreign_references(&null_child.rows[0])
                .unwrap()
                .is_empty()
        );
        assert_eq!(null_child.to_homebase().unwrap().mutations.len(), 1);
    }

    #[test]
    fn unique_foreign_references_fence_parent_key_changes_by_reference_range() {
        let (connection, parent, child) = unique_foreign_key_tables();
        let target = parent.unique_constraints()[0].index_id();
        assert_eq!(child.foreign_keys()[0].referenced_index(), target);

        let child_row = CapturedRow {
            table: "messages".into(),
            rowid: 10,
            values: vec![
                StoredValue::Integer(10),
                StoredValue::Text(b"north".to_vec()),
                StoredValue::Text(b"user@example.com".to_vec()),
            ],
        };
        let inserted = RowSet::from_captured(&connection, &[child_row])
            .unwrap()
            .unwrap();
        let reference = inserted
            .foreign_references(&inserted.rows[0])
            .unwrap()
            .remove(0);
        let images = [
            StoredValue::Text(b"north".to_vec()),
            StoredValue::Text(b"user@example.com".to_vec()),
        ]
        .iter()
        .zip(parent.foreign_key_target_columns(target).unwrap())
        .map(|(value, column)| {
            key_image(
                value,
                KeyPartRules {
                    column: column.id(),
                    affinity: column.affinity(parent.mode()),
                    rowid_alias: false,
                },
            )
            .unwrap()
        })
        .collect::<Vec<_>>();
        let parent_ownership = unique_prefix(
            parent.table_id(),
            parent.unique_constraints()[0].index_id(),
            images.clone(),
        )
        .unwrap();
        let reference_prefix = foreign_reference_prefix(
            parent.table_id(),
            child.foreign_keys()[0].id(),
            target,
            images,
        )
        .unwrap();
        assert!(reference.key.starts_with(&reference_prefix));
        let lowered = inserted.to_homebase().unwrap();
        assert_explicit_range_assertions(
            &lowered.footprint,
            &[
                inserted.row_key(&inserted.rows[0]).unwrap(),
                reference.key.clone(),
                active_primary_index_key(child.table_id()),
                write_revision_key(child.table_id()),
            ],
        );
        assert!(lowered.footprint.writes().contains(&reference.key));
        assert!(lowered.footprint.constraints().contains(&reference.key));
        assert!(!lowered.footprint.constraints().contains(&parent_ownership));
        assert_eq!(RowSet::decode(&inserted.encode()).unwrap(), inserted);

        let before = CapturedRow {
            table: "accounts".into(),
            rowid: 1,
            values: vec![
                StoredValue::Integer(1),
                StoredValue::Text(b"north".to_vec()),
                StoredValue::Text(b"user@example.com".to_vec()),
            ],
        };
        let after = CapturedRow {
            values: vec![
                StoredValue::Integer(1),
                StoredValue::Text(b"north".to_vec()),
                StoredValue::Text(b"moved@example.com".to_vec()),
            ],
            ..before.clone()
        };
        let updated = UpdatedRowsFixture::from_captured(&connection, &[(before.clone(), after)])
            .unwrap()
            .unwrap();
        let old_reference = updated
            .before
            .incoming_reference_prefixes(&updated.before.rows[0])
            .unwrap()
            .remove(0);
        let lowered = updated.to_homebase().unwrap();
        assert_explicit_range_assertions(
            &lowered.footprint,
            &[
                old_reference.clone(),
                active_primary_index_key(parent.table_id()),
                write_revision_key(parent.table_id()),
            ],
        );
        assert!(lowered.footprint.writes().contains(&old_reference));
        assert!(lowered.footprint.constraints().contains(&old_reference));
        assert!(lowered.mutations.iter().any(|mutation| {
            matches!(
                mutation,
                Mutation::DeleteRange {
                    range: Range::Prefix(prefix)
                } if prefix == &old_reference
            )
        }));
    }

    #[test]
    fn multi_row_parent_key_moves_retire_only_operation_wide_removed_references() {
        let (connection, _, _) = foreign_key_tables();
        let before_two = CapturedRow {
            table: "parents".into(),
            rowid: 2,
            values: vec![StoredValue::Integer(2), StoredValue::Text(b"two".to_vec())],
        };
        let before_three = CapturedRow {
            table: "parents".into(),
            rowid: 3,
            values: vec![
                StoredValue::Integer(3),
                StoredValue::Text(b"three".to_vec()),
            ],
        };
        let after_one = CapturedRow {
            rowid: 1,
            values: vec![StoredValue::Integer(1), StoredValue::Text(b"two".to_vec())],
            ..before_two.clone()
        };
        let after_two = CapturedRow {
            rowid: 2,
            values: vec![
                StoredValue::Integer(2),
                StoredValue::Text(b"three".to_vec()),
            ],
            ..before_three.clone()
        };
        let updated = UpdatedRowsFixture::from_captured(
            &connection,
            &[(before_two, after_one), (before_three, after_two)],
        )
        .unwrap()
        .unwrap();
        let retained = updated
            .before
            .incoming_reference_prefixes(&updated.before.rows[0])
            .unwrap()
            .remove(0);
        let retired = updated
            .before
            .incoming_reference_prefixes(&updated.before.rows[1])
            .unwrap()
            .remove(0);

        let lowered = updated.to_homebase().unwrap();
        assert!(!lowered.mutations.iter().any(|mutation| {
            matches!(
                mutation,
                Mutation::DeleteRange {
                    range: Range::Prefix(prefix)
                } if prefix == &retained
            )
        }));
        assert!(lowered.mutations.iter().any(|mutation| {
            matches!(
                mutation,
                Mutation::DeleteRange {
                    range: Range::Prefix(prefix)
                } if prefix == &retired
            )
        }));
        assert!(!lowered.footprint.writes().contains(&retained));
        assert!(lowered.footprint.writes().contains(&retired));
        assert!(lowered.footprint.constraints().contains(&retired));
    }

    #[test]
    fn separate_foreign_keys_to_one_parent_keep_distinct_reference_cells() {
        let connection = Connection::open_in_memory().unwrap();
        catalog::initialize(&connection).unwrap();
        let parent_sql = "CREATE TABLE people (id INTEGER PRIMARY KEY)";
        let crate::sql::ValidatedExecute::CreateTable(parent_spec) =
            crate::sql::validate_execute(parent_sql).unwrap()
        else {
            unreachable!()
        };
        let parent = CreateTable::new(parent_sql, parent_spec);
        connection.execute(parent_sql, ()).unwrap();
        catalog::insert(&connection, &parent).unwrap();

        let child_sql = "CREATE TABLE families (
            id INTEGER PRIMARY KEY,
            mother INTEGER REFERENCES people(id),
            father INTEGER REFERENCES people(id)
        )";
        let crate::sql::ValidatedExecute::CreateTable(child_spec) =
            crate::sql::validate_execute(child_sql).unwrap()
        else {
            unreachable!()
        };
        let child = CreateTable::prepare(&connection, child_sql, child_spec).unwrap();
        connection.execute(child_sql, ()).unwrap();
        catalog::insert(&connection, &child).unwrap();

        let inserted = RowSet::from_captured(
            &connection,
            &[CapturedRow {
                table: "families".into(),
                rowid: 10,
                values: vec![
                    StoredValue::Integer(10),
                    StoredValue::Integer(1),
                    StoredValue::Integer(2),
                ],
            }],
        )
        .unwrap()
        .unwrap();
        let references = inserted.foreign_references(&inserted.rows[0]).unwrap();
        assert_eq!(references.len(), 2);
        assert_ne!(references[0].key, references[1].key);
        assert_eq!(references[0].owner, references[1].owner);

        let lowered = inserted.to_homebase().unwrap();
        assert_eq!(lowered.mutations.len(), 3);
        let mut expected = lowered
            .mutations
            .iter()
            .map(|mutation| mutation.key().clone())
            .collect::<Vec<_>>();
        expected.extend([
            active_primary_index_key(child.table_id()),
            write_revision_key(child.table_id()),
        ]);
        assert_explicit_range_assertions(&lowered.footprint, &expected);
        assert!(
            references
                .iter()
                .all(|reference| lowered.footprint.writes().contains(&reference.key))
        );
        assert!(
            references
                .iter()
                .all(|reference| lowered.footprint.constraints().contains(&reference.key))
        );
    }

    #[test]
    fn foreign_key_updates_move_reverse_reference_cells() {
        let (connection, _, _) = foreign_key_tables();
        let before = CapturedRow {
            table: "children".into(),
            rowid: 10,
            values: vec![
                StoredValue::Integer(10),
                StoredValue::Integer(7),
                StoredValue::Text(b"before".to_vec()),
            ],
        };
        let after = CapturedRow {
            table: "children".into(),
            rowid: 10,
            values: vec![
                StoredValue::Integer(10),
                StoredValue::Integer(8),
                StoredValue::Text(b"after".to_vec()),
            ],
        };
        let updated = UpdatedRowsFixture::from_captured(&connection, &[(before, after)])
            .unwrap()
            .unwrap();
        let before_reference = updated.before.foreign_reference_map().unwrap();
        let after_reference = updated.after.foreign_reference_map().unwrap();
        let lowered = updated.to_homebase().unwrap();
        let after_reference_key = after_reference.first_key_value().unwrap().0.clone();
        assert_explicit_range_assertions(
            &lowered.footprint,
            &[
                after_reference_key,
                active_primary_index_key(updated.after.rules.table),
                write_revision_key(updated.after.rules.table),
            ],
        );

        assert_eq!(before_reference.len(), 1);
        assert_eq!(after_reference.len(), 1);
        assert_ne!(
            before_reference.first_key_value().unwrap().0,
            after_reference.first_key_value().unwrap().0
        );
        assert!(lowered.mutations.iter().any(|mutation| {
            matches!(
                mutation,
                Mutation::Delete { key } if before_reference.contains_key(key)
            )
        }));
        assert!(lowered.mutations.iter().any(|mutation| {
            matches!(
                mutation,
                Mutation::Set { key, .. } if after_reference.contains_key(key)
            )
        }));
    }

    #[test]
    fn without_rowid_rows_discard_undefined_hook_rowids_and_keep_composite_order() {
        let created = without_rowid_definition();
        let connection = connection(&created);
        let captured = CapturedRow {
            table: "memberships".into(),
            rowid: 41,
            values: vec![
                StoredValue::Text(b"north".to_vec()),
                StoredValue::Integer(7),
                StoredValue::Text(b"body".to_vec()),
            ],
        };
        let inserted = RowSet::from_captured(&connection, std::slice::from_ref(&captured))
            .unwrap()
            .unwrap();

        assert_eq!(inserted.rules.storage, TableStorage::WithoutRowid);
        assert_eq!(
            inserted
                .rules
                .key_parts
                .iter()
                .map(|part| part.column)
                .collect::<Vec<_>>(),
            [created.columns()[1].id(), created.columns()[0].id(),]
        );
        assert_eq!(RowSet::decode(&inserted.encode()).unwrap(), inserted);
        let lowered = inserted.to_homebase().unwrap();
        assert_eq!(lowered.mutations[0].key().components().len(), 7);

        let mut same_row = captured.clone();
        same_row.rowid = 999;
        assert!(
            UpdatedRowsFixture::from_captured(&connection, &[(captured, same_row)])
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn composite_indexes_lower_per_part_and_skip_null_tuples() {
        let created = unique_definition();
        let connection = connection(&created);
        let captured = [
            CapturedRow {
                table: "accounts".into(),
                rowid: 1,
                values: vec![
                    StoredValue::Integer(1),
                    StoredValue::Text(b"acme".to_vec()),
                    StoredValue::Text(b"a@example.com".to_vec()),
                ],
            },
            CapturedRow {
                table: "accounts".into(),
                rowid: 2,
                values: vec![
                    StoredValue::Integer(2),
                    StoredValue::Text(b"other".to_vec()),
                    StoredValue::Text(b"a@example.com".to_vec()),
                ],
            },
            CapturedRow {
                table: "accounts".into(),
                rowid: 3,
                values: vec![
                    StoredValue::Integer(3),
                    StoredValue::Null,
                    StoredValue::Text(b"a@example.com".to_vec()),
                ],
            },
            CapturedRow {
                table: "accounts".into(),
                rowid: 4,
                values: vec![
                    StoredValue::Integer(4),
                    StoredValue::Null,
                    StoredValue::Text(b"a@example.com".to_vec()),
                ],
            },
        ];
        let inserted = RowSet::from_captured(&connection, &captured)
            .unwrap()
            .unwrap();

        assert_eq!(RowSet::decode(&inserted.encode()).unwrap(), inserted);
        let lowered = inserted.to_homebase().unwrap();
        assert_eq!(lowered.mutations.len(), 6);
        assert_eq!(lowered.footprint.writes().len(), 6);
        let mut expected = lowered
            .mutations
            .iter()
            .map(|mutation| mutation.key().clone())
            .collect::<Vec<_>>();
        expected.extend([
            active_primary_index_key(created.table_id()),
            write_revision_key(created.table_id()),
        ]);
        assert_explicit_range_assertions(&lowered.footprint, &expected);
        let unique = lowered
            .mutations
            .iter()
            .filter(|mutation| mutation.key().components()[3].as_bytes() == codes::UNIQUE)
            .collect::<Vec<_>>();
        assert_eq!(unique.len(), 2);
        assert!(
            unique
                .iter()
                .all(|mutation| mutation.key().components().len() == 7)
        );
        assert!(
            unique
                .iter()
                .all(|mutation| lowered.footprint.constraints().contains(mutation.key()))
        );
        inserted
            .validate_homebase(&admit(lowered.mutations))
            .unwrap();
        let deleted = DeletedRowsFixture::from_captured(&connection, &captured)
            .unwrap()
            .unwrap()
            .to_homebase()
            .unwrap();
        assert_eq!(deleted.mutations.len(), 6);
        assert!(
            deleted
                .mutations
                .iter()
                .all(|mutation| matches!(mutation, Mutation::Delete { .. }))
        );
    }

    #[test]
    fn secondary_indexes_do_not_change_row_lowering() {
        let created = definition();
        let connection = connection(&created);
        let captured = [
            CapturedRow {
                table: "notes".into(),
                rowid: 1,
                values: vec![
                    StoredValue::Integer(1),
                    StoredValue::Text(b"same".to_vec()),
                    StoredValue::Null,
                ],
            },
            CapturedRow {
                table: "notes".into(),
                rowid: 2,
                values: vec![
                    StoredValue::Integer(2),
                    StoredValue::Text(b"same".to_vec()),
                    StoredValue::Blob(vec![7]),
                ],
            },
        ];
        let baseline = RowSet::from_captured(&connection, &captured)
            .unwrap()
            .unwrap()
            .to_homebase()
            .unwrap();

        add_index(&connection, "CREATE INDEX notes_body ON notes (body)");
        add_index(
            &connection,
            "CREATE INDEX notes_payload_body ON notes (payload, body)",
        );
        let inserted = RowSet::from_captured(&connection, &captured)
            .unwrap()
            .unwrap();

        assert!(inserted.rules.indexes.is_empty());
        assert_eq!(RowSet::decode(&inserted.encode()).unwrap(), inserted);
        let lowered = inserted.to_homebase().unwrap();
        assert!(
            lowered
                .mutations
                .iter()
                .all(|mutation| mutation.key().components()[3].as_bytes() != codes::INDEXES)
        );
        assert_eq!(lowered.mutations.len(), baseline.mutations.len());
        assert_eq!(lowered.footprint, baseline.footprint);
        inserted
            .validate_homebase(&admit(lowered.mutations))
            .unwrap();

        let deleted = DeletedRowsFixture::from_captured(&connection, &captured)
            .unwrap()
            .unwrap()
            .to_homebase()
            .unwrap();
        assert!(
            deleted
                .mutations
                .iter()
                .all(|mutation| mutation.key().components()[3].as_bytes() != codes::INDEXES)
        );

        let updated = UpdatedRowsFixture::from_captured(
            &connection,
            &[(
                captured[0].clone(),
                CapturedRow {
                    table: "notes".into(),
                    rowid: 1,
                    values: vec![
                        StoredValue::Integer(1),
                        StoredValue::Text(b"changed".to_vec()),
                        StoredValue::Null,
                    ],
                },
            )],
        )
        .unwrap()
        .unwrap()
        .to_homebase()
        .unwrap();
        assert!(
            updated
                .mutations
                .iter()
                .all(|mutation| mutation.key().components()[3].as_bytes() != codes::INDEXES)
        );
    }

    #[test]
    fn overlapping_unique_indexes_lower_and_delete_independently() {
        let created = overlapping_unique_definition();
        let connection = connection(&created);
        let captured = profile(1, "acme", "same", "same");
        let inserted = RowSet::from_captured(&connection, std::slice::from_ref(&captured))
            .unwrap()
            .unwrap();

        assert_eq!(RowSet::decode(&inserted.encode()).unwrap(), inserted);
        let lowered = inserted.to_homebase().unwrap();
        assert_eq!(lowered.mutations.len(), 5);
        assert_eq!(lowered.footprint.writes().len(), 5);
        let unique = lowered
            .mutations
            .iter()
            .filter(|mutation| mutation.key().components()[3].as_bytes() == codes::UNIQUE)
            .collect::<Vec<_>>();
        assert_eq!(unique.len(), 4);
        assert_eq!(
            unique
                .iter()
                .map(|mutation| mutation.key().components()[4].as_bytes().to_vec())
                .collect::<BTreeSet<_>>()
                .len(),
            4
        );
        assert_eq!(
            unique
                .iter()
                .map(|mutation| mutation.key().components().len())
                .collect::<Vec<_>>(),
            [6, 6, 7, 7]
        );
        assert!(
            unique
                .iter()
                .all(|mutation| lowered.footprint.constraints().contains(mutation.key()))
        );
        inserted
            .validate_homebase(&admit(lowered.mutations.clone()))
            .unwrap();

        let deleted = DeletedRowsFixture::from_captured(&connection, &[captured])
            .unwrap()
            .unwrap()
            .to_homebase()
            .unwrap();
        assert_eq!(deleted.mutations.len(), lowered.mutations.len());
        assert_eq!(
            lowered
                .mutations
                .iter()
                .map(|mutation| mutation.key().clone())
                .collect::<BTreeSet<_>>(),
            deleted
                .mutations
                .iter()
                .map(|mutation| mutation.key().clone())
                .collect()
        );
        assert!(
            deleted
                .mutations
                .iter()
                .all(|mutation| matches!(mutation, Mutation::Delete { .. }))
        );
    }

    #[test]
    fn one_insert_operation_rejects_duplicates_in_any_unique_index() {
        let created = overlapping_unique_definition();
        let connection = connection(&created);
        let captured = [
            profile(1, "acme", "shared@example.com", "alpha"),
            profile(2, "other", "shared@example.com", "beta"),
            profile(3, "acme", "third@example.com", "alpha"),
        ];
        assert!(matches!(
            RowSet::from_captured(&connection, &captured),
            Err(Error::InvalidMultiliteOp(message))
                if message == "row operation contains a duplicate UNIQUE key"
        ));

        let mut inserted = RowSet::from_captured(&connection, std::slice::from_ref(&captured[0]))
            .unwrap()
            .unwrap();
        let duplicate = RowSet::from_captured(&connection, std::slice::from_ref(&captured[1]))
            .unwrap()
            .unwrap();
        inserted.rows.extend(duplicate.rows);

        assert_eq!(
            RowSet::decode(&inserted.encode()),
            Err(RowCodecError::DuplicateUniqueKey)
        );
        assert!(matches!(
            inserted.to_homebase(),
            Err(Error::InvalidMultiliteOp(message))
                if message == "row operation contains a duplicate UNIQUE key"
        ));
    }

    #[test]
    fn admitted_insert_rejects_missing_crossed_extra_and_corrupt_unique_cells() {
        let created = overlapping_unique_definition();
        let connection = connection(&created);
        let inserted =
            RowSet::from_captured(&connection, &[profile(1, "acme", "email", "username")])
                .unwrap()
                .unwrap();
        let lowered = inserted.to_homebase().unwrap().mutations;

        let mut missing = lowered.clone();
        missing.pop();
        assert_eq!(
            inserted.validate_homebase(&admit(missing)),
            Err(RowCodecError::InvalidBatch)
        );

        let mut crossed = lowered.clone();
        crossed.swap(1, 2);
        assert_eq!(
            inserted.validate_homebase(&admit(crossed)),
            Err(RowCodecError::InvalidBatch)
        );

        let mut extra = lowered.clone();
        extra.push(lowered.last().unwrap().clone());
        assert_eq!(
            inserted.validate_homebase(&admit(extra)),
            Err(RowCodecError::InvalidBatch)
        );

        let mut corrupt = lowered;
        let Mutation::Set { value, .. } = &mut corrupt[1] else {
            unreachable!()
        };
        value.push(0);
        assert_eq!(
            inserted.validate_homebase(&admit(corrupt)),
            Err(RowCodecError::InvalidBatch)
        );
    }

    #[test]
    fn delete_codec_lowers_exact_keys_and_restores_complete_rows() {
        let created = definition();
        let connection = connection(&created);
        let inserted = inserted(&connection);
        let deleted = DeletedRowsFixture {
            deleted: inserted.clone(),
        };

        assert_eq!(
            DeletedRowsFixture::decode(&deleted.encode()).unwrap(),
            deleted
        );
        let lowered = deleted.to_homebase().unwrap();
        assert_eq!(lowered.mutations.len(), 2);
        assert!(
            lowered
                .mutations
                .iter()
                .all(|mutation| matches!(mutation, Mutation::Delete { .. }))
        );
        assert_eq!(lowered.footprint.writes().len(), 2);
        assert_eq!(lowered.footprint.constraints().len(), 2);
        assert_explicit_range_assertions(
            &lowered.footprint,
            &[
                active_primary_index_key(created.table_id()),
                write_revision_key(created.table_id()),
            ],
        );
        assert_eq!(
            lowered.mutations[0].key(),
            &primary_key_prefix(&created, &[StoredValue::Integer(7)]).unwrap()
        );

        inserted.apply(&connection).unwrap();
        deleted.apply(&connection).unwrap();
        assert_eq!(
            connection
                .query_row("SELECT count(*) FROM notes", (), |row| row.get::<_, i64>(0))
                .unwrap(),
            0
        );
        deleted.restore_materialized(&connection).unwrap();
        assert_eq!(
            connection
                .prepare("SELECT id, body, payload FROM notes ORDER BY id")
                .unwrap()
                .query_map((), |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, Option<String>>(1)?,
                        row.get::<_, Vec<u8>>(2)?,
                    ))
                })
                .unwrap()
                .collect::<rusqlite::Result<Vec<_>>>()
                .unwrap(),
            [(7, Some("hello".into()), vec![0, 1]), (9, None, Vec::new()),]
        );
    }

    #[test]
    fn integer_affinity_does_not_imply_a_rowid_alias() {
        let sql = "CREATE TABLE aliases (id INTEGER PRIMARY KEY, body TEXT)";
        let crate::sql::ValidatedExecute::CreateTable(spec) =
            crate::sql::validate_execute(sql).unwrap()
        else {
            unreachable!()
        };
        let created = CreateTable::new(sql, spec);
        assert!(
            created
                .primary_key_columns()
                .all(|column| created.is_rowid_alias(column.id()))
        );

        assert!(matches!(
            crate::sql::validate_execute(
                "CREATE TABLE aliases (id INT NOT NULL PRIMARY KEY, body TEXT)"
            ),
            Err(Error::UnsupportedSql(_))
        ));
        crate::sql::validate_execute(
            "CREATE TABLE aliases (id INT NOT NULL PRIMARY KEY, body TEXT) WITHOUT ROWID",
        )
        .unwrap();
    }

    #[test]
    fn historical_inserts_project_through_added_and_dropped_columns() {
        let created = definition();
        let source = connection(&created);
        let inserted = RowSet::from_captured(
            &source,
            &[CapturedRow {
                table: "notes".into(),
                rowid: 7,
                values: vec![
                    StoredValue::Integer(7),
                    StoredValue::Text(b"body".to_vec()),
                    StoredValue::Blob(vec![1, 2]),
                ],
            }],
        )
        .unwrap()
        .unwrap();

        let added = connection(&created);
        alter_table(
            &added,
            "ALTER TABLE notes ADD COLUMN summary TEXT NOT NULL DEFAULT 'new'",
        );
        inserted.apply(&added).unwrap();
        assert_eq!(
            added
                .query_row(
                    "SELECT body, payload, summary FROM notes WHERE id = 7",
                    (),
                    |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, Vec<u8>>(1)?,
                            row.get::<_, String>(2)?,
                        ))
                    },
                )
                .unwrap(),
            ("body".into(), vec![1, 2], "new".into())
        );

        let dropped = connection(&created);
        alter_table(&dropped, "ALTER TABLE notes DROP COLUMN payload");
        inserted.apply(&dropped).unwrap();
        assert_eq!(
            dropped
                .query_row("SELECT id, body FROM notes", (), |row| {
                    Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
                })
                .unwrap(),
            (7, "body".into())
        );
    }

    #[test]
    fn historical_updates_preserve_columns_added_after_capture() {
        let created = definition();
        let source = connection(&created);
        let stable = UpdatedRowsFixture::from_captured(
            &source,
            &[(
                CapturedRow {
                    table: "notes".into(),
                    rowid: 1,
                    values: vec![
                        StoredValue::Integer(1),
                        StoredValue::Text(b"before".to_vec()),
                        StoredValue::Blob(vec![1]),
                    ],
                },
                CapturedRow {
                    table: "notes".into(),
                    rowid: 1,
                    values: vec![
                        StoredValue::Integer(1),
                        StoredValue::Text(b"after".to_vec()),
                        StoredValue::Blob(vec![1]),
                    ],
                },
            )],
        )
        .unwrap()
        .unwrap();
        let moved = UpdatedRowsFixture::from_captured(
            &source,
            &[(
                CapturedRow {
                    table: "notes".into(),
                    rowid: 1,
                    values: vec![
                        StoredValue::Integer(1),
                        StoredValue::Text(b"before".to_vec()),
                        StoredValue::Blob(vec![1]),
                    ],
                },
                CapturedRow {
                    table: "notes".into(),
                    rowid: 2,
                    values: vec![
                        StoredValue::Integer(2),
                        StoredValue::Text(b"moved".to_vec()),
                        StoredValue::Blob(vec![1]),
                    ],
                },
            )],
        )
        .unwrap()
        .unwrap();

        let stable_target = connection(&created);
        stable_target
            .execute("INSERT INTO notes VALUES (1, 'before', x'01')", ())
            .unwrap();
        alter_table(
            &stable_target,
            "ALTER TABLE notes ADD COLUMN summary TEXT DEFAULT 'default'",
        );
        stable_target
            .execute("UPDATE notes SET summary = 'preserved'", ())
            .unwrap();
        stable.apply(&stable_target).unwrap();
        assert_eq!(
            stable_target
                .query_row("SELECT body, summary FROM notes", (), |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
                })
                .unwrap(),
            ("after".into(), "preserved".into())
        );

        let moved_target = connection(&created);
        moved_target
            .execute("INSERT INTO notes VALUES (1, 'before', x'01')", ())
            .unwrap();
        alter_table(
            &moved_target,
            "ALTER TABLE notes ADD COLUMN summary TEXT DEFAULT 'default'",
        );
        moved_target
            .execute("UPDATE notes SET summary = 'preserved'", ())
            .unwrap();
        moved.apply(&moved_target).unwrap();
        assert_eq!(
            moved_target
                .query_row("SELECT id, body, summary FROM notes", (), |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                    ))
                })
                .unwrap(),
            (2, "moved".into(), "preserved".into())
        );
    }

    #[test]
    fn stable_update_codec_replaces_rows_and_restores_before_images() {
        let created = definition();
        let connection = connection(&created);
        let updated = UpdatedRowsFixture::from_captured(
            &connection,
            &[
                (
                    CapturedRow {
                        table: "notes".into(),
                        rowid: 7,
                        values: vec![
                            StoredValue::Integer(7),
                            StoredValue::Text(b"before".to_vec()),
                            StoredValue::Blob(vec![0]),
                        ],
                    },
                    CapturedRow {
                        table: "notes".into(),
                        rowid: 7,
                        values: vec![
                            StoredValue::Integer(7),
                            StoredValue::Text(b"after".to_vec()),
                            StoredValue::Blob(vec![1, 2]),
                        ],
                    },
                ),
                (
                    CapturedRow {
                        table: "notes".into(),
                        rowid: 9,
                        values: vec![
                            StoredValue::Integer(9),
                            StoredValue::Null,
                            StoredValue::Blob(Vec::new()),
                        ],
                    },
                    CapturedRow {
                        table: "notes".into(),
                        rowid: 9,
                        values: vec![
                            StoredValue::Integer(9),
                            StoredValue::Text(b"filled".to_vec()),
                            StoredValue::Blob(vec![3]),
                        ],
                    },
                ),
            ],
        )
        .unwrap()
        .unwrap();

        assert_eq!(
            UpdatedRowsFixture::decode(&updated.encode()).unwrap(),
            updated
        );
        let lowered = updated.to_homebase().unwrap();
        assert_eq!(lowered.mutations.len(), 2);
        assert!(
            lowered
                .mutations
                .iter()
                .all(|mutation| matches!(mutation, Mutation::Set { .. }))
        );
        assert_eq!(lowered.footprint.writes().len(), 2);
        assert_eq!(lowered.footprint.constraints().len(), 4);
        assert_explicit_range_assertions(
            &lowered.footprint,
            &[
                active_primary_index_key(created.table_id()),
                write_revision_key(created.table_id()),
                updated.before.row_key(&updated.before.rows[0]).unwrap(),
                updated.before.row_key(&updated.before.rows[1]).unwrap(),
            ],
        );

        updated.before.apply(&connection).unwrap();
        updated.apply(&connection).unwrap();
        assert_eq!(
            connection
                .prepare("SELECT id, body, payload FROM notes ORDER BY id")
                .unwrap()
                .query_map((), |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, Option<String>>(1)?,
                        row.get::<_, Vec<u8>>(2)?,
                    ))
                })
                .unwrap()
                .collect::<rusqlite::Result<Vec<_>>>()
                .unwrap(),
            [
                (7, Some("after".into()), vec![1, 2]),
                (9, Some("filled".into()), vec![3]),
            ]
        );

        updated.restore_materialized(&connection).unwrap();
        assert_eq!(
            connection
                .prepare("SELECT id, body, payload FROM notes ORDER BY id")
                .unwrap()
                .query_map((), |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, Option<String>>(1)?,
                        row.get::<_, Vec<u8>>(2)?,
                    ))
                })
                .unwrap()
                .collect::<rusqlite::Result<Vec<_>>>()
                .unwrap(),
            [(7, Some("before".into()), vec![0]), (9, None, Vec::new()),]
        );
    }

    #[test]
    fn update_replaces_changed_composite_unique_ownership() {
        let created = unique_definition();
        let connection = connection(&created);
        connection
            .execute(
                "INSERT INTO accounts VALUES (1, 'acme', 'before@example.com')",
                (),
            )
            .unwrap();
        let before = CapturedRow {
            table: "accounts".into(),
            rowid: 1,
            values: vec![
                StoredValue::Integer(1),
                StoredValue::Text(b"acme".to_vec()),
                StoredValue::Text(b"before@example.com".to_vec()),
            ],
        };
        let after = CapturedRow {
            table: "accounts".into(),
            rowid: 1,
            values: vec![
                StoredValue::Integer(1),
                StoredValue::Text(b"acme".to_vec()),
                StoredValue::Text(b"after@example.com".to_vec()),
            ],
        };
        let updated = UpdatedRowsFixture::from_captured(&connection, &[(before, after)])
            .unwrap()
            .unwrap();

        let lowered = updated.to_homebase().unwrap();
        assert_eq!(lowered.mutations.len(), 3);
        let claimed_unique = lowered
            .mutations
            .iter()
            .find(|mutation| {
                matches!(mutation, Mutation::Set { .. })
                    && mutation.key().components()[3].as_bytes() == codes::UNIQUE
            })
            .unwrap()
            .key()
            .clone();
        assert_explicit_range_assertions(
            &lowered.footprint,
            &[
                claimed_unique,
                active_primary_index_key(created.table_id()),
                write_revision_key(created.table_id()),
            ],
        );
        assert_eq!(
            lowered
                .mutations
                .iter()
                .filter(|mutation| mutation.key().components()[3].as_bytes() == codes::UNIQUE)
                .count(),
            2
        );
        assert!(
            lowered
                .mutations
                .iter()
                .filter(|mutation| {
                    matches!(mutation, Mutation::Set { .. })
                        && mutation.key().components()[3].as_bytes() == codes::UNIQUE
                })
                .all(|mutation| lowered.footprint.constraints().contains(mutation.key()))
        );
        updated.apply(&connection).unwrap();
        assert_eq!(
            connection
                .query_row("SELECT email FROM accounts WHERE id = 1", (), |row| {
                    row.get::<_, String>(0)
                })
                .unwrap(),
            "after@example.com"
        );
        updated.restore_materialized(&connection).unwrap();
        assert_eq!(
            connection
                .query_row("SELECT email FROM accounts WHERE id = 1", (), |row| {
                    row.get::<_, String>(0)
                })
                .unwrap(),
            "before@example.com"
        );
    }

    #[test]
    fn update_changes_only_the_affected_overlapping_indexes() {
        let created = overlapping_unique_definition();
        let connection = connection(&created);
        let before = profile(1, "acme", "same@example.com", "before");
        let after = profile(1, "other", "same@example.com", "after");
        let updated = UpdatedRowsFixture::from_captured(&connection, &[(before, after)])
            .unwrap()
            .unwrap();

        let lowered = updated.to_homebase().unwrap();
        assert_eq!(lowered.mutations.len(), 7);
        assert_eq!(lowered.footprint.writes().len(), 7);
        let counts = lowered
            .mutations
            .iter()
            .filter(|mutation| mutation.key().components()[3].as_bytes() == codes::UNIQUE)
            .fold(BTreeMap::<Vec<u8>, usize>::new(), |mut counts, mutation| {
                *counts
                    .entry(mutation.key().components()[4].as_bytes().to_vec())
                    .or_default() += 1;
                counts
            });
        assert_eq!(counts.len(), 3);
        assert!(counts.values().all(|count| *count == 2));
        assert!(
            !counts.contains_key(
                created.unique_constraints()[0]
                    .index_id()
                    .as_bytes()
                    .as_slice()
            )
        );
    }

    #[test]
    fn primary_key_moves_reassign_unchanged_unique_ownership() {
        let created = unique_definition();
        let connection = connection(&created);
        let before = CapturedRow {
            table: "accounts".into(),
            rowid: 1,
            values: vec![
                StoredValue::Integer(1),
                StoredValue::Text(b"acme".to_vec()),
                StoredValue::Text(b"owner@example.com".to_vec()),
            ],
        };
        let after = CapturedRow {
            table: "accounts".into(),
            rowid: 2,
            values: vec![
                StoredValue::Integer(2),
                StoredValue::Text(b"acme".to_vec()),
                StoredValue::Text(b"owner@example.com".to_vec()),
            ],
        };
        let updated = UpdatedRowsFixture::from_captured(&connection, &[(before, after)])
            .unwrap()
            .unwrap();

        let lowered = updated.to_homebase().unwrap();
        let unique = lowered
            .mutations
            .iter()
            .filter(|mutation| mutation.key().components()[3].as_bytes() == codes::UNIQUE)
            .collect::<Vec<_>>();
        assert!(matches!(
            unique.as_slice(),
            [
                Mutation::Delete { key: deleted },
                Mutation::Set {
                    key: inserted,
                    value: _
                }
            ] if deleted == inserted
        ));
        assert_eq!(lowered.footprint.writes().len(), 3);
    }

    #[test]
    fn null_transitions_remove_and_create_unique_ownership() {
        let created = unique_definition();
        let connection = connection(&created);
        let present = CapturedRow {
            table: "accounts".into(),
            rowid: 1,
            values: vec![
                StoredValue::Integer(1),
                StoredValue::Text(b"acme".to_vec()),
                StoredValue::Text(b"nullable@example.com".to_vec()),
            ],
        };
        let absent = CapturedRow {
            table: "accounts".into(),
            rowid: 1,
            values: vec![
                StoredValue::Integer(1),
                StoredValue::Null,
                StoredValue::Text(b"nullable@example.com".to_vec()),
            ],
        };

        let removed =
            UpdatedRowsFixture::from_captured(&connection, &[(present.clone(), absent.clone())])
                .unwrap()
                .unwrap()
                .to_homebase()
                .unwrap();
        assert_eq!(
            removed
                .mutations
                .iter()
                .filter(|mutation| mutation.key().components()[3].as_bytes() == codes::UNIQUE)
                .filter(|mutation| matches!(mutation, Mutation::Delete { .. }))
                .count(),
            1
        );

        let created = UpdatedRowsFixture::from_captured(&connection, &[(absent, present)])
            .unwrap()
            .unwrap()
            .to_homebase()
            .unwrap();
        assert_eq!(
            created
                .mutations
                .iter()
                .filter(|mutation| mutation.key().components()[3].as_bytes() == codes::UNIQUE)
                .filter(|mutation| matches!(mutation, Mutation::Set { .. }))
                .count(),
            1
        );
    }

    #[test]
    fn primary_key_update_moves_the_row_and_restores_the_before_image() {
        let created = definition();
        let notes_connection = connection(&created);
        let moved = UpdatedRowsFixture::from_captured(
            &notes_connection,
            &[(
                CapturedRow {
                    table: "notes".into(),
                    rowid: 1,
                    values: vec![
                        StoredValue::Integer(1),
                        StoredValue::Text(b"before".to_vec()),
                        StoredValue::Blob(Vec::new()),
                    ],
                },
                CapturedRow {
                    table: "notes".into(),
                    rowid: 2,
                    values: vec![
                        StoredValue::Integer(2),
                        StoredValue::Text(b"after".to_vec()),
                        StoredValue::Blob(Vec::new()),
                    ],
                },
            )],
        )
        .unwrap()
        .unwrap();
        let lowered = moved.to_homebase().unwrap();
        assert!(matches!(
            lowered.mutations.as_slice(),
            [Mutation::Delete { .. }, Mutation::Set { .. }]
        ));
        assert_eq!(lowered.footprint.writes().len(), 2);
        let destination = lowered.mutations[1].key().clone();
        assert_explicit_range_assertions(
            &lowered.footprint,
            &[
                destination,
                active_primary_index_key(created.table_id()),
                write_revision_key(created.table_id()),
            ],
        );

        moved.before.apply(&notes_connection).unwrap();
        moved.apply(&notes_connection).unwrap();
        assert_eq!(
            notes_connection
                .query_row("SELECT id, body FROM notes", (), |row| {
                    Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
                })
                .unwrap(),
            (2, "after".into())
        );
        moved.restore_materialized(&notes_connection).unwrap();
        assert_eq!(
            notes_connection
                .query_row("SELECT id, body FROM notes", (), |row| {
                    Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
                })
                .unwrap(),
            (1, "before".into())
        );

        assert!(matches!(
            UpdatedRowsFixture::from_captured(
                &notes_connection,
                &[(
                    CapturedRow {
                        table: "notes".into(),
                        rowid: 1,
                        values: vec![
                            StoredValue::Integer(1),
                            StoredValue::Null,
                            StoredValue::Blob(Vec::new()),
                        ],
                    },
                    CapturedRow {
                        table: "notes".into(),
                        rowid: 3,
                        values: vec![
                            StoredValue::Integer(2),
                            StoredValue::Null,
                            StoredValue::Blob(Vec::new()),
                        ],
                    },
                )],
            ),
            Err(Error::CaptureInvariant(
                "captured rowid contradicts its INTEGER PRIMARY KEY"
            ))
        ));
    }

    #[test]
    fn multi_row_key_moves_delete_every_source_before_setting_destinations() {
        let created = definition();
        let connection = connection(&created);
        let row = |id, body: &str| CapturedRow {
            table: "notes".into(),
            rowid: id,
            values: vec![
                StoredValue::Integer(id),
                StoredValue::Text(body.as_bytes().to_vec()),
                StoredValue::Blob(Vec::new()),
            ],
        };
        let updated = UpdatedRowsFixture::from_captured(
            &connection,
            &[
                (row(2, "two"), row(3, "two-moved")),
                (row(1, "one"), row(2, "one-moved")),
            ],
        )
        .unwrap()
        .unwrap();

        let lowered = updated.to_homebase().unwrap();
        let [
            Mutation::Delete { key: first_source },
            Mutation::Delete { key: second_source },
            Mutation::Set {
                key: first_destination,
                ..
            },
            Mutation::Set {
                key: second_destination,
                ..
            },
        ] = lowered.mutations.as_slice()
        else {
            panic!("key moves did not lower as deletes followed by sets")
        };
        assert_eq!(first_source, second_destination);
        assert_ne!(first_source, first_destination);
        assert_ne!(second_source, second_destination);

        updated.before.apply(&connection).unwrap();
        updated.apply(&connection).unwrap();
        #[cfg(debug_assertions)]
        updated.verify_materialized(&connection).unwrap();
        assert_eq!(
            connection
                .prepare("SELECT id, body FROM notes ORDER BY id")
                .unwrap()
                .query_map((), |row| {
                    Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
                })
                .unwrap()
                .collect::<rusqlite::Result<Vec<_>>>()
                .unwrap(),
            [(2, "one-moved".into()), (3, "two-moved".into())]
        );
        updated.restore_materialized(&connection).unwrap();
        assert_eq!(
            connection
                .prepare("SELECT id, body FROM notes ORDER BY id")
                .unwrap()
                .query_map((), |row| {
                    Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
                })
                .unwrap()
                .collect::<rusqlite::Result<Vec<_>>>()
                .unwrap(),
            [(1, "one".into()), (2, "two".into())]
        );
    }

    #[test]
    fn update_validates_every_before_image_before_changing_any_row() {
        let created = definition();
        let connection = connection(&created);
        let updated = UpdatedRowsFixture::from_captured(
            &connection,
            &[
                (
                    CapturedRow {
                        table: "notes".into(),
                        rowid: 7,
                        values: vec![
                            StoredValue::Integer(7),
                            StoredValue::Text(b"before-7".to_vec()),
                            StoredValue::Blob(vec![7]),
                        ],
                    },
                    CapturedRow {
                        table: "notes".into(),
                        rowid: 7,
                        values: vec![
                            StoredValue::Integer(7),
                            StoredValue::Text(b"after-7".to_vec()),
                            StoredValue::Blob(vec![70]),
                        ],
                    },
                ),
                (
                    CapturedRow {
                        table: "notes".into(),
                        rowid: 9,
                        values: vec![
                            StoredValue::Integer(9),
                            StoredValue::Text(b"before-9".to_vec()),
                            StoredValue::Blob(vec![9]),
                        ],
                    },
                    CapturedRow {
                        table: "notes".into(),
                        rowid: 9,
                        values: vec![
                            StoredValue::Integer(9),
                            StoredValue::Text(b"after-9".to_vec()),
                            StoredValue::Blob(vec![90]),
                        ],
                    },
                ),
            ],
        )
        .unwrap()
        .unwrap();
        updated.before.apply(&connection).unwrap();
        connection
            .execute("UPDATE notes SET body = 'diverged' WHERE id = 9", ())
            .unwrap();

        assert!(matches!(
            updated.apply(&connection),
            Err(Error::InvalidDatabase(
                "row changes no longer match SQLite state"
            ))
        ));
        assert_eq!(
            connection
                .prepare("SELECT id, body FROM notes ORDER BY id")
                .unwrap()
                .query_map((), |row| {
                    Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
                })
                .unwrap()
                .collect::<rusqlite::Result<Vec<_>>>()
                .unwrap(),
            [(7, "before-7".into()), (9, "diverged".into())]
        );
    }

    #[test]
    fn multi_table_apply_failure_rolls_back_earlier_tables_and_restores_flags() {
        let notes = definition();
        let connection = connection(&notes);
        let tasks_sql = "CREATE TABLE tasks (id INTEGER PRIMARY KEY, body TEXT, payload BLOB)";
        let crate::sql::ValidatedExecute::CreateTable(tasks_spec) =
            crate::sql::validate_execute(tasks_sql).unwrap()
        else {
            unreachable!()
        };
        let tasks = CreateTable::new(tasks_sql, tasks_spec);
        connection.execute(tasks_sql, ()).unwrap();
        catalog::insert(&connection, &tasks).unwrap();
        connection
            .execute("INSERT INTO notes VALUES (1, 'before', x'')", ())
            .unwrap();
        connection
            .execute("INSERT INTO tasks VALUES (1, 'before', x'')", ())
            .unwrap();
        connection
            .set_db_config(DbConfig::SQLITE_DBCONFIG_ENABLE_TRIGGER, true)
            .unwrap();
        connection
            .set_db_config(DbConfig::SQLITE_DBCONFIG_ENABLE_FKEY, true)
            .unwrap();

        let image = |table: &str, body: &str| CapturedRow {
            table: table.into(),
            rowid: 1,
            values: vec![
                StoredValue::Integer(1),
                StoredValue::Text(body.as_bytes().to_vec()),
                StoredValue::Blob(Vec::new()),
            ],
        };
        let catalog = CatalogSnapshot::load(&connection).unwrap();
        let changes = RowChanges::from_catalog(
            &catalog,
            vec![
                CapturedChange::Update {
                    before: image("notes", "before"),
                    after: image("notes", "after"),
                },
                CapturedChange::Update {
                    before: image("tasks", "before"),
                    after: image("tasks", "after"),
                },
            ],
        )
        .unwrap()
        .unwrap();
        let (first, second) = if notes.table_id().as_bytes() < tasks.table_id().as_bytes() {
            ("notes", "tasks")
        } else {
            ("tasks", "notes")
        };
        connection
            .execute(&format!("UPDATE {second} SET body = 'diverged'"), ())
            .unwrap();

        assert!(matches!(
            changes.apply(&connection),
            Err(Error::InvalidDatabase(
                "row changes no longer match SQLite state"
            ))
        ));
        assert_eq!(
            connection
                .query_row(&format!("SELECT body FROM {first}"), (), |row| {
                    row.get::<_, String>(0)
                })
                .unwrap(),
            "before"
        );
        assert_eq!(
            connection
                .query_row(&format!("SELECT body FROM {second}"), (), |row| {
                    row.get::<_, String>(0)
                })
                .unwrap(),
            "diverged"
        );
        assert!(
            connection
                .db_config(DbConfig::SQLITE_DBCONFIG_ENABLE_TRIGGER)
                .unwrap()
        );
        assert!(
            connection
                .db_config(DbConfig::SQLITE_DBCONFIG_ENABLE_FKEY)
                .unwrap()
        );
    }

    #[test]
    fn row_set_codec_rejects_duplicate_primary_key_images() {
        let created = definition();
        let connection = connection(&created);
        let mut inserted = inserted(&connection);
        inserted.rows.push(inserted.rows[0].clone());

        assert_eq!(
            RowSet::decode(&inserted.encode()),
            Err(RowCodecError::DuplicateRow)
        );
        assert!(matches!(
            inserted.to_homebase(),
            Err(Error::InvalidMultiliteOp(message))
                if message == "row operation contains a duplicate logical row"
        ));
    }

    #[test]
    fn delete_refuses_a_row_whose_non_key_values_diverged() {
        let created = definition();
        let connection = connection(&created);
        let inserted = inserted(&connection);
        let deleted = DeletedRowsFixture {
            deleted: inserted.clone(),
        };
        inserted.apply(&connection).unwrap();
        connection
            .execute("UPDATE notes SET body = 'changed' WHERE id = 7", ())
            .unwrap();

        assert!(matches!(
            deleted.apply(&connection),
            Err(Error::InvalidDatabase(
                "row changes no longer match SQLite state"
            ))
        ));
        assert_eq!(
            connection
                .query_row("SELECT count(*) FROM notes", (), |row| row.get::<_, i64>(0))
                .unwrap(),
            2
        );
    }

    #[test]
    fn apply_and_reject_effects_replay_exact_rows() {
        let created = definition();
        let connection = connection(&created);
        let inserted = inserted(&connection);

        inserted.apply(&connection).unwrap();
        assert_eq!(
            connection
                .query_row("SELECT count(*) FROM notes", (), |row| row.get::<_, i64>(0))
                .unwrap(),
            2
        );
        inserted.delete_materialized(&connection).unwrap();
        assert_eq!(
            connection
                .query_row("SELECT count(*) FROM notes", (), |row| row.get::<_, i64>(0))
                .unwrap(),
            0
        );
    }

    #[cfg(debug_assertions)]
    #[test]
    fn delete_verifier_rejects_a_reappeared_primary_key() {
        let created = definition();
        let connection = connection(&created);
        let inserted = inserted(&connection);
        let deleted = DeletedRowsFixture {
            deleted: inserted.clone(),
        };

        inserted.apply(&connection).unwrap();
        deleted.apply(&connection).unwrap();
        deleted.verify_materialized(&connection).unwrap();
        connection
            .execute("INSERT INTO notes VALUES (7, 'other', x'')", ())
            .unwrap();

        assert!(matches!(
            deleted.verify_materialized(&connection),
            Err(Error::InvalidDatabase(
                "canonical row changes retained a retired primary-key image"
            ))
        ));
    }

    #[test]
    fn stale_rows_follow_table_identity_after_rename_and_name_reuse() {
        let created = definition();
        let connection = connection(&created);
        let inserted = inserted(&connection);
        connection
            .execute("ALTER TABLE notes RENAME TO archived_notes", ())
            .unwrap();
        catalog::rename_binding(
            &connection,
            created.table_id(),
            created.table_name_identity(),
            &crate::logical::schema::SqlName::new("archived_notes".into()),
        )
        .unwrap();

        let replacement = definition();
        connection.execute(replacement.sql(), ()).unwrap();
        catalog::insert(&connection, &replacement).unwrap();
        inserted.apply(&connection).unwrap();

        assert_eq!(
            connection
                .query_row("SELECT count(*) FROM archived_notes", (), |row| {
                    row.get::<_, i64>(0)
                })
                .unwrap(),
            2
        );
        assert_eq!(
            connection
                .query_row("SELECT count(*) FROM notes", (), |row| row.get::<_, i64>(0))
                .unwrap(),
            0
        );
        inserted.delete_materialized(&connection).unwrap();
        assert_eq!(
            connection
                .query_row("SELECT count(*) FROM archived_notes", (), |row| {
                    row.get::<_, i64>(0)
                })
                .unwrap(),
            0
        );
        catalog::validate(&connection).unwrap();
    }

    #[test]
    fn key_images_normalize_equal_integer_and_real_values() {
        let part = KeyPartRules {
            column: ColumnId::from_bytes(Uuid::new_v4().into_bytes()),
            affinity: Affinity::Blob,
            rowid_alias: false,
        };
        assert_eq!(
            key_image(&StoredValue::Integer(1), part).unwrap(),
            key_image(&StoredValue::Real(1.0_f64.to_bits()), part).unwrap()
        );
        assert_eq!(
            key_image(&StoredValue::Integer(0), part).unwrap(),
            key_image(&StoredValue::Real((-0.0_f64).to_bits()), part).unwrap()
        );
    }

    #[test]
    fn key_rule_codec_roundtrips_every_affinity_and_the_rowid_marker() {
        let column = ColumnId::from_bytes(Uuid::new_v4().into_bytes());
        for affinity in [
            Affinity::Integer,
            Affinity::Real,
            Affinity::Text,
            Affinity::Blob,
            Affinity::Numeric,
        ] {
            let part = KeyPartRules {
                column,
                affinity,
                rowid_alias: affinity == Affinity::Integer,
            };
            assert_eq!(decode_key_part(&encode_key_part(part)).unwrap(), part);
        }

        let mut malformed = Writer::new();
        malformed
            .field(TAG_COLUMN_ID, &column.as_bytes())
            .expect("test field is bounded");
        malformed
            .field(TAG_COLUMN_AFFINITY, &[Affinity::Text.to_u8()])
            .expect("test field is bounded");
        malformed
            .field(TAG_KEY_PART_FLAGS, &[KEY_PART_ROWID_ALIAS])
            .expect("test field is bounded");
        assert_eq!(
            decode_key_part(&malformed.finish()),
            Err(RowCodecError::InvalidRow)
        );
    }

    #[test]
    fn sqlite_affinity_is_captured_before_row_and_unique_key_lowering() {
        let sql = "CREATE TABLE typed_values (
            id INTEGER PRIMARY KEY,
            amount DECIMAL(10, 2) UNIQUE,
            label VARCHAR(40),
            payload BLOB,
            ratio DOUBLE
        )";
        let crate::sql::ValidatedExecute::CreateTable(spec) =
            crate::sql::validate_execute(sql).unwrap()
        else {
            unreachable!()
        };
        let created = CreateTable::new(sql, spec);
        let connection = connection(&created);
        connection
            .execute("INSERT INTO typed_values VALUES (1, '7', 8, 9, '10.5')", ())
            .unwrap();
        let captured = connection
            .query_row(
                "SELECT rowid, id, amount, label, payload, ratio FROM typed_values",
                (),
                |row| {
                    Ok(CapturedRow {
                        table: "typed_values".into(),
                        rowid: row.get(0)?,
                        values: (1..=5)
                            .map(|index| row.get_ref(index).map(StoredValue::capture))
                            .collect::<rusqlite::Result<Vec<_>>>()?,
                    })
                },
            )
            .unwrap();
        assert_eq!(
            captured.values,
            [
                StoredValue::Integer(1),
                StoredValue::Integer(7),
                StoredValue::Text(b"8".to_vec()),
                StoredValue::Integer(9),
                StoredValue::Real(10.5_f64.to_bits()),
            ]
        );

        let inserted = RowSet::from_captured(&connection, &[captured])
            .unwrap()
            .unwrap();
        let lowered = inserted.to_homebase().unwrap();
        assert_eq!(lowered.mutations.len(), 2);
        assert_eq!(lowered.footprint.writes().len(), 2);
        assert_eq!(RowSet::decode(&inserted.encode()).unwrap(), inserted);
    }

    #[test]
    fn strict_rows_preserve_any_and_reject_invalid_admitted_storage_classes() {
        let sql = "CREATE TABLE strict_values (
            id INTEGER PRIMARY KEY,
            count INT,
            ratio REAL,
            label TEXT,
            payload BLOB,
            anything ANY UNIQUE
        ) STRICT";
        let crate::sql::ValidatedExecute::CreateTable(spec) =
            crate::sql::validate_execute(sql).unwrap()
        else {
            unreachable!()
        };
        let created = CreateTable::new(sql, spec);
        let connection = connection(&created);
        connection
            .execute(
                "INSERT INTO strict_values VALUES
                    (1, '7', 2, 3, x'04', '000123'),
                    (2, 8, 2.5, '4', x'05', 123)",
                (),
            )
            .unwrap();
        assert!(
            connection
                .execute(
                    "INSERT INTO strict_values VALUES
                        (3, 'not-an-integer', 1, 'x', x'00', 'bad')",
                    (),
                )
                .is_err()
        );

        let mut statement = connection
            .prepare(
                "SELECT rowid, id, count, ratio, label, payload, anything
                 FROM strict_values ORDER BY id",
            )
            .unwrap();
        let captured = statement
            .query_map((), |row| {
                Ok(CapturedRow {
                    table: "strict_values".into(),
                    rowid: row.get(0)?,
                    values: (1..=6)
                        .map(|index| row.get_ref(index).map(StoredValue::capture))
                        .collect::<rusqlite::Result<Vec<_>>>()?,
                })
            })
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap();
        assert_eq!(
            captured[0].values,
            [
                StoredValue::Integer(1),
                StoredValue::Integer(7),
                StoredValue::Real(2.0_f64.to_bits()),
                StoredValue::Text(b"3".to_vec()),
                StoredValue::Blob(vec![4]),
                StoredValue::Text(b"000123".to_vec()),
            ]
        );
        assert_eq!(captured[1].values[5], StoredValue::Integer(123));

        let inserted = RowSet::from_captured(&connection, &captured)
            .unwrap()
            .unwrap();
        assert_eq!(inserted.rules.indexes[0].parts[0].affinity, Affinity::Blob);
        assert_eq!(inserted.to_homebase().unwrap().mutations.len(), 4);

        let mut invalid = inserted;
        let count = created.columns()[1].id();
        invalid.rows[0]
            .values
            .iter_mut()
            .find(|(column, _)| *column == count)
            .unwrap()
            .1 = StoredValue::Text(b"invalid".to_vec());
        let catalog = CatalogSnapshot::load(&connection).unwrap();
        assert!(matches!(
            invalid.validate_against_catalog(&catalog, &created),
            Err(Error::InvalidMultiliteOp(message))
                if message == "row value has an invalid STRICT storage class"
        ));
    }

    #[test]
    fn stored_value_codec_roundtrips_every_sqlite_storage_class() {
        for value in [
            StoredValue::Null,
            StoredValue::Integer(i64::MIN),
            StoredValue::Real((-0.5_f64).to_bits()),
            StoredValue::Text(b"hello".to_vec()),
            StoredValue::Blob(vec![0, 1, 0xff]),
        ] {
            assert_eq!(StoredValue::decode(&value.encode()).unwrap(), value);
        }
        assert_eq!(
            StoredValue::decode(&[0, 1]),
            Err(crate::value::StoredValueCodecError::InvalidLength)
        );
        assert_eq!(
            StoredValue::decode(&[1, 0]),
            Err(crate::value::StoredValueCodecError::InvalidLength)
        );
    }
}
