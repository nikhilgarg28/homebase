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

use super::schema::{
    Affinity, Column, ColumnId, CreateTable, ForeignKeyDefinition, ForeignKeyId, IndexId,
    NamedIndex, SchemaRevisionId, StrictType, TableId, TableMode, TableStorage,
    active_primary_index_key, write_revision_key,
};
use super::{catalog, codes};
use crate::commit::footprint::ConflictFootprint;
pub(crate) use crate::value::StoredValue;
use crate::{Error, Result};

const ROW_FRAME_VERSION: u8 = 5;
const ROW_SET_FRAME_VERSION: u8 = 1;
const UPDATE_FRAME_VERSION: u8 = 1;
const TAG_SCHEMA_REVISION: u8 = 1;
const TAG_PRIMARY_INDEX: u8 = 2;
const TAG_KEY_PART: u8 = 3;
const TAG_COLUMN_VALUE: u8 = 4;
const TAG_ROWID: u8 = 5;
const TAG_INDEX_RULES: u8 = 6;
const TAG_TABLE_STORAGE: u8 = 7;
const TAG_FOREIGN_KEY: u8 = 8;
const TAG_INCOMING_FOREIGN_KEY: u8 = 9;
const TAG_TABLE: u8 = 1;
const TAG_ROW: u8 = 2;
const TAG_COLUMN_ID: u8 = 1;
const TAG_COLUMN_AFFINITY: u8 = 2;
const TAG_KEY_PART_FLAGS: u8 = 3;
const TAG_VALUE: u8 = 2;
const TAG_UPDATE_BEFORE: u8 = 1;
const TAG_UPDATE_AFTER: u8 = 2;
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

/// One complete SQLite row image observed after affinity and generated values ran.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CapturedRow {
    pub table: String,
    pub rowid: i64,
    pub values: Vec<StoredValue>,
}

/// One direct application-row change observed by SQLite's preupdate hook.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CapturedChange {
    Insert(CapturedRow),
    Delete(CapturedRow),
    Update {
        before: CapturedRow,
        after: CapturedRow,
    },
}

impl StoredValue {
    fn encode(&self) -> Vec<u8> {
        match self {
            Self::Null => vec![0],
            Self::Integer(value) => {
                let mut encoded = vec![1];
                encoded.extend_from_slice(&value.to_be_bytes());
                encoded
            }
            Self::Real(bits) => {
                let mut encoded = vec![2];
                encoded.extend_from_slice(&bits.to_be_bytes());
                encoded
            }
            Self::Text(value) => {
                let mut encoded = vec![3];
                encoded.extend_from_slice(value);
                encoded
            }
            Self::Blob(value) => {
                let mut encoded = vec![4];
                encoded.extend_from_slice(value);
                encoded
            }
        }
    }

    fn decode(frame: &[u8]) -> std::result::Result<Self, RowCodecError> {
        let mut reader = Reader::new(frame);
        let kind = reader.u8().ok_or(RowCodecError::Truncated)?;
        let value = match kind {
            0 => Self::Null,
            1 => {
                let bits = reader.u64().ok_or(RowCodecError::InvalidLength)?;
                Self::Integer(i64::from_be_bytes(bits.to_be_bytes()))
            }
            2 => Self::Real(reader.u64().ok_or(RowCodecError::InvalidLength)?),
            3 | 4 => {
                let remaining = reader.rest().len();
                let bytes = reader
                    .take(remaining)
                    .expect("remaining byte count came from this reader")
                    .to_vec();
                if kind == 3 {
                    Self::Text(bytes)
                } else {
                    Self::Blob(bytes)
                }
            }
            _ => return Err(RowCodecError::InvalidValue),
        };
        if reader.end().is_none() {
            return Err(RowCodecError::InvalidLength);
        }
        Ok(value)
    }
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
    rowid: Option<i64>,
    values: Vec<(ColumnId, StoredValue)>,
}

/// One logical multi-row INSERT captured from a single SQLite statement.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InsertRows {
    table: TableId,
    schema_revision: SchemaRevisionId,
    primary_index: IndexId,
    storage: TableStorage,
    key_parts: Vec<KeyPartRules>,
    indexes: Vec<IndexRules>,
    foreign_keys: Vec<ForeignKeyRules>,
    incoming_foreign_keys: Vec<IncomingForeignKeyRules>,
    rows: Vec<Row>,
}

/// One logical multi-row DELETE carrying the complete removed row images.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeleteRows {
    deleted: InsertRows,
}

/// One logical multi-row UPDATE carrying complete before and after row images.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UpdateRows {
    before: InsertRows,
    after: InsertRows,
}

/// Homebase mutations and conflict footprint for one row operation.
pub struct RowHomebaseOp {
    pub mutations: Vec<Mutation>,
    pub footprint: ConflictFootprint,
}

/// One durable entry created while a new index scans existing rows.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IndexBackfillEntry {
    pub key: Key,
    pub value: Vec<u8>,
}

impl InsertRows {
    pub fn from_captured(
        connection: &Connection,
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
        let Some(created) = catalog::by_name(connection, &first.table)? else {
            return Ok(None);
        };
        let columns = created.columns();
        if captured.iter().any(|row| row.values.len() != columns.len()) {
            return Err(Error::CaptureInvariant(
                "captured row width does not match its schema catalog",
            ));
        }
        let key_parts = created
            .primary_key_columns()
            .map(|column| KeyPartRules {
                column: column.id(),
                affinity: column.affinity(created.mode()),
                rowid_alias: created.is_rowid_alias(column.id()),
            })
            .collect::<Vec<_>>();
        let indexes = index_rules(&created);
        let foreign_keys = foreign_key_rules(connection, &created)?;
        let incoming_foreign_keys = incoming_foreign_key_rules(connection, &created)?;
        let rows = captured
            .iter()
            .map(|captured| Row {
                rowid: (created.storage() == TableStorage::Rowid).then_some(captured.rowid),
                values: columns
                    .iter()
                    .zip(&captured.values)
                    .map(|(column, value)| {
                        (
                            column.id(),
                            normalize_captured_value(value, column.affinity(created.mode())),
                        )
                    })
                    .collect(),
            })
            .collect();
        let inserted = Self {
            table: created.table_id(),
            schema_revision: created.schema_revision_id(),
            primary_index: created.primary_index_id(),
            storage: created.storage(),
            key_parts,
            indexes,
            foreign_keys,
            incoming_foreign_keys,
            rows,
        };
        inserted.validate_against(connection, &created)?;
        Ok(Some(inserted))
    }

    pub fn to_homebase(&self) -> Result<RowHomebaseOp> {
        self.validate_structure()
            .map_err(|error| Error::InvalidMultiliteOp(error.to_string()))?;
        let mut mutations = Vec::with_capacity(
            self.rows.len() * (self.indexes.len() + self.foreign_keys.len() + 1),
        );
        let mut footprint = ConflictFootprint::new();
        for row in &self.rows {
            let key = self
                .row_key(row)
                .map_err(|error| Error::InvalidMultiliteOp(error.to_string()))?;
            footprint.add_constraint(key.clone());
            footprint.add_write(key.clone());
            mutations.push(Mutation::Set {
                key,
                value: self.encode_row(row),
            });
        }
        for row in &self.rows {
            for entry in self
                .index_entries(row)
                .map_err(|error| Error::InvalidMultiliteOp(error.to_string()))?
            {
                footprint.add_constraint(entry.key.clone());
                footprint.add_write(entry.key.clone());
                mutations.push(Mutation::Set {
                    key: entry.key,
                    value: entry.value,
                });
            }
            for reference in self
                .foreign_references(row)
                .map_err(|error| Error::InvalidMultiliteOp(error.to_string()))?
            {
                footprint.add_constraint(reference.key.clone());
                footprint.add_write(reference.key.clone());
                mutations.push(Mutation::Set {
                    key: reference.key,
                    value: reference.owner,
                });
            }
        }
        footprint.add_constraint(active_primary_index_key(self.table));
        footprint.add_constraint(write_revision_key(self.table));
        Ok(RowHomebaseOp {
            mutations,
            footprint,
        })
    }

    #[cfg(test)]
    pub fn from_homebase(
        batch: &AdmittedBatch<Vec<u8>>,
    ) -> std::result::Result<Self, RowCodecError> {
        batch.validate().map_err(|_| RowCodecError::InvalidBatch)?;
        if batch.entries.is_empty() {
            return Err(RowCodecError::InvalidBatch);
        }
        let mut operation = None::<Self>;
        for entry in &batch.entries {
            let Mutation::Set { key, value } = &entry.device_entry.mutation else {
                return Err(RowCodecError::InvalidBatch);
            };
            let components = key.components();
            if components.len() < 4
                || components[0].as_bytes() != codes::ROOT
                || components[1].as_bytes() != codes::TABLES
            {
                return Err(RowCodecError::InvalidBatch);
            }
            if components[3].as_bytes() != codes::ROWS {
                continue;
            }
            if components.len() < 6 {
                return Err(RowCodecError::InvalidBatch);
            }
            let table = TableId::from_bytes(uuid_bytes(components[2].as_bytes())?);
            let primary_index = IndexId::from_bytes(uuid_bytes(components[4].as_bytes())?);
            let (
                schema_revision,
                encoded_primary_index,
                storage,
                key_parts,
                indexes,
                foreign_keys,
                incoming_foreign_keys,
                row,
            ) = decode_row(value)?;
            if encoded_primary_index != primary_index {
                return Err(RowCodecError::InvalidBatch);
            }
            let candidate = operation.get_or_insert_with(|| Self {
                table,
                schema_revision,
                primary_index,
                storage,
                key_parts: key_parts.clone(),
                indexes: indexes.clone(),
                foreign_keys: foreign_keys.clone(),
                incoming_foreign_keys: incoming_foreign_keys.clone(),
                rows: Vec::new(),
            });
            if candidate.table != table
                || candidate.schema_revision != schema_revision
                || candidate.primary_index != primary_index
                || candidate.storage != storage
                || candidate.key_parts != key_parts
                || candidate.indexes != indexes
                || candidate.foreign_keys != foreign_keys
                || candidate.incoming_foreign_keys != incoming_foreign_keys
            {
                return Err(RowCodecError::InvalidBatch);
            }
            let expected = candidate.key_images(&row)?;
            if components[5..]
                .iter()
                .map(|component| component.as_bytes())
                .ne(expected.iter().map(Vec::as_slice))
            {
                return Err(RowCodecError::InvalidBatch);
            }
            candidate.rows.push(row);
        }
        let operation = operation.ok_or(RowCodecError::InvalidBatch)?;
        operation.validate_structure()?;
        let expected = operation
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
        Ok(operation)
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut writer = Writer::new();
        writer.u8(ROW_SET_FRAME_VERSION);
        writer
            .field(TAG_TABLE, &self.table.as_bytes())
            .expect("row field length fits in u32");
        for row in &self.rows {
            writer
                .field(TAG_ROW, &self.encode_row(row))
                .expect("row field length fits in u32");
        }
        writer.finish()
    }

    pub fn decode(frame: &[u8]) -> std::result::Result<Self, RowCodecError> {
        let mut reader = Reader::new(frame);
        if reader.u8() != Some(ROW_SET_FRAME_VERSION) {
            return Err(RowCodecError::UnknownVersion);
        }
        let mut table = None;
        let mut operation = None::<Self>;
        while let Some((tag, value)) = reader.field().map_err(|_| RowCodecError::Truncated)? {
            match tag {
                TAG_TABLE => set_once(&mut table, TableId::from_bytes(uuid_bytes(value)?))?,
                TAG_ROW => {
                    let table = table.ok_or(RowCodecError::MissingField(TAG_TABLE))?;
                    let (
                        schema_revision,
                        primary_index,
                        storage,
                        key_parts,
                        indexes,
                        foreign_keys,
                        incoming_foreign_keys,
                        row,
                    ) = decode_row(value)?;
                    let candidate = operation.get_or_insert_with(|| Self {
                        table,
                        schema_revision,
                        primary_index,
                        storage,
                        key_parts: key_parts.clone(),
                        indexes: indexes.clone(),
                        foreign_keys: foreign_keys.clone(),
                        incoming_foreign_keys: incoming_foreign_keys.clone(),
                        rows: Vec::new(),
                    });
                    if candidate.table != table
                        || candidate.schema_revision != schema_revision
                        || candidate.primary_index != primary_index
                        || candidate.storage != storage
                        || candidate.key_parts != key_parts
                        || candidate.indexes != indexes
                        || candidate.foreign_keys != foreign_keys
                        || candidate.incoming_foreign_keys != incoming_foreign_keys
                    {
                        return Err(RowCodecError::InvalidBatch);
                    }
                    candidate.rows.push(row);
                }
                _ => {}
            }
        }
        let operation = operation.ok_or(RowCodecError::MissingField(TAG_ROW))?;
        if Some(operation.table) != table {
            return Err(RowCodecError::InvalidBatch);
        }
        operation.validate_structure()?;
        Ok(operation)
    }

    pub fn primary_values<'a>(&self, row: &'a Row) -> Result<Vec<&'a StoredValue>> {
        self.key_parts
            .iter()
            .map(|part| {
                row.values
                    .iter()
                    .find(|(column, _)| *column == part.column)
                    .map(|(_, value)| value)
                    .ok_or(Error::InvalidDatabase(
                        "pending row is missing a primary-key value",
                    ))
            })
            .collect()
    }

    pub fn apply(&self, connection: &Connection) -> Result<()> {
        let created = self.catalog_definition(connection)?;
        let table_name = materialized_table_name(connection, self.table)?;
        let columns = created.columns();
        let hidden_rowid = hidden_rowid_alias(&created)?;
        let mut names = columns
            .iter()
            .map(|column| quote_identifier(column.name().value()))
            .collect::<Vec<_>>();
        if let Some(alias) = hidden_rowid {
            names.insert(0, quote_identifier(alias));
        }
        let names = names.join(", ");
        let placeholders =
            std::iter::repeat_n("?", columns.len() + usize::from(hidden_rowid.is_some()))
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
                    row.values
                        .iter()
                        .find(|(id, _)| *id == column.id())
                        .map(|(_, value)| value)
                        .ok_or(Error::InvalidMultiliteOp(
                            "row is missing a schema column".into(),
                        ))
                })
                .collect::<Result<Vec<_>>>()?;
            let mut parameters = Vec::<&dyn ToSql>::with_capacity(
                values.len() + usize::from(hidden_rowid.is_some()),
            );
            if hidden_rowid.is_some() {
                parameters.push(row.rowid.as_ref().ok_or(Error::InvalidMultiliteOp(
                    "rowid table row is missing its rowid".into(),
                ))?);
            }
            parameters.extend(values.into_iter().map(|value| value as &dyn ToSql));
            statement.execute(params_from_iter(parameters))?;
        }
        Ok(())
    }

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
        let table_name = materialized_table_name(connection, self.table)?;
        self.validate_materialized_against(connection, &created, mismatch)?;
        let primary = created.primary_key_columns().collect::<Vec<_>>();
        let mut predicates = primary
            .iter()
            .map(|column| format!("{} = ?", quote_identifier(column.name().value())))
            .collect::<Vec<_>>();
        let hidden_rowid = hidden_rowid_alias(&created)?;
        if let Some(alias) = hidden_rowid {
            predicates.push(format!("{} = ?", quote_identifier(alias)));
        }
        let sql = format!(
            "DELETE FROM {} WHERE {}",
            quote_identifier(&table_name),
            predicates.join(" AND ")
        );
        let mut delete = connection.prepare(&sql)?;
        for row in self.rows.iter().rev() {
            let primary = self.primary_values(row)?;
            let mut parameters = Vec::<&dyn ToSql>::with_capacity(
                primary.len() + usize::from(hidden_rowid.is_some()),
            );
            parameters.extend(primary.into_iter().map(|value| value as &dyn ToSql));
            if hidden_rowid.is_some() {
                parameters.push(row.rowid.as_ref().ok_or(Error::InvalidMultiliteOp(
                    "rowid table row is missing its rowid".into(),
                ))?);
            }
            if delete.execute(params_from_iter(parameters))? != 1 {
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
        let table_name = materialized_table_name(connection, self.table)?;
        let columns = created.columns();
        let primary = created.primary_key_columns().collect::<Vec<_>>();
        let mut predicates = primary
            .iter()
            .map(|column| format!("{} = ?", quote_identifier(column.name().value())))
            .collect::<Vec<_>>();
        let hidden_rowid = hidden_rowid_alias(created)?;
        if let Some(alias) = hidden_rowid {
            predicates.push(format!("{} = ?", quote_identifier(alias)));
        }
        let predicate = predicates.join(" AND ");
        let mut selected = columns
            .iter()
            .map(|column| quote_identifier(column.name().value()))
            .collect::<Vec<_>>();
        if let Some(alias) = hidden_rowid {
            selected.push(quote_identifier(alias));
        }
        let select_sql = format!(
            "SELECT {} FROM {} WHERE {predicate}",
            selected.join(", "),
            quote_identifier(&table_name)
        );
        let mut select = connection.prepare(&select_sql)?;
        for row in self.rows.iter().rev() {
            let primary = self.primary_values(row)?;
            let mut parameters = Vec::<&dyn ToSql>::with_capacity(
                primary.len() + usize::from(hidden_rowid.is_some()),
            );
            parameters.extend(primary.into_iter().map(|value| value as &dyn ToSql));
            if hidden_rowid.is_some() {
                parameters.push(row.rowid.as_ref().ok_or(Error::InvalidMultiliteOp(
                    "rowid table row is missing its rowid".into(),
                ))?);
            }
            let actual = select
                .query_row(params_from_iter(parameters.iter().copied()), |result| {
                    let values = (0..columns.len())
                        .map(|index| result.get_ref(index).map(StoredValue::capture))
                        .collect::<rusqlite::Result<Vec<_>>>()?;
                    let rowid = hidden_rowid
                        .map(|_| result.get::<_, i64>(columns.len()))
                        .transpose()?;
                    Ok((values, rowid))
                })
                .optional()?;
            let expected = columns
                .iter()
                .map(|column| {
                    row.values
                        .iter()
                        .find(|(id, _)| *id == column.id())
                        .map(|(_, value)| value.clone())
                        .ok_or(Error::InvalidMultiliteOp(
                            "row is missing a schema column".into(),
                        ))
                })
                .collect::<Result<Vec<_>>>()?;
            if actual != Some((expected, hidden_rowid.and(row.rowid))) {
                return Err(Error::InvalidDatabase(mismatch));
            }
        }
        Ok(())
    }

    fn catalog_definition(&self, connection: &Connection) -> Result<CreateTable> {
        let created = catalog::by_id(connection, self.table)?.ok_or(Error::InvalidDatabase(
            "row operation references an unknown table",
        ))?;
        self.validate_against(connection, &created)?;
        Ok(created)
    }

    fn validate_against(&self, connection: &Connection, created: &CreateTable) -> Result<()> {
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
        let expected_foreign_keys = foreign_key_rules(connection, created)?;
        let expected_incoming_foreign_keys = incoming_foreign_key_rules(connection, created)?;
        if self.table != created.table_id()
            || self.primary_index != created.primary_index_id()
            || self.storage != created.storage()
            || self.key_parts != expected_key_parts
            || expected_indexes
                .iter()
                .any(|expected| !self.indexes.contains(expected))
            || self
                .indexes
                .iter()
                .any(|actual| !known_indexes.contains(actual))
            || self.foreign_keys != expected_foreign_keys
            || self.incoming_foreign_keys != expected_incoming_foreign_keys
        {
            return Err(Error::InvalidMultiliteOp(
                "row operation contradicts the local schema catalog".into(),
            ));
        }
        for row in &self.rows {
            if row.values.len() != created.columns().len()
                || created
                    .columns()
                    .iter()
                    .any(|column| !row.values.iter().any(|(id, _)| *id == column.id()))
            {
                return Err(Error::InvalidMultiliteOp(
                    "row values contradict the local schema catalog".into(),
                ));
            }
            if created.mode() == TableMode::Strict
                && created.columns().iter().any(|column| {
                    let value = row
                        .values
                        .iter()
                        .find(|(id, _)| *id == column.id())
                        .map(|(_, value)| value)
                        .expect("row width and column identities were validated");
                    !strict_value_matches(column, value)
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
        if self.key_parts.is_empty() || self.rows.is_empty() {
            return Err(RowCodecError::InvalidRow);
        }
        if self
            .rows
            .iter()
            .any(|row| row.rowid.is_some() != (self.storage == TableStorage::Rowid))
        {
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
        let mut keys = BTreeSet::new();
        let mut indexes = BTreeSet::new();
        for row in &self.rows {
            if let [
                KeyPartRules {
                    column,
                    rowid_alias: true,
                    ..
                },
            ] = self.key_parts.as_slice()
            {
                let rowid_matches = row.values.iter().any(|(id, value)| {
                    id == column
                        && row
                            .rowid
                            .is_some_and(|rowid| *value == StoredValue::Integer(rowid))
                });
                if !rowid_matches {
                    return Err(RowCodecError::InvalidRow);
                }
            }
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

    fn row_key(&self, row: &Row) -> std::result::Result<Key, RowCodecError> {
        let images = self.key_images(row)?;
        row_prefix(self.table, self.primary_index, images)
    }

    fn key_images(&self, row: &Row) -> std::result::Result<Vec<Vec<u8>>, RowCodecError> {
        self.key_parts
            .iter()
            .map(|part| {
                let value = row
                    .values
                    .iter()
                    .find(|(column, _)| *column == part.column)
                    .map(|(_, value)| value)
                    .ok_or(RowCodecError::InvalidRow)?;
                key_image(value, *part)
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
                .map(|part| {
                    row.values
                        .iter()
                        .find(|(column, _)| *column == part.column)
                        .map(|(_, value)| value)
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
        let owner = self.row_key(row)?.encode();
        let child_images = self.key_images(row)?;
        let mut references = Vec::with_capacity(self.foreign_keys.len());
        for foreign_key in &self.foreign_keys {
            let values = foreign_key
                .key_parts
                .iter()
                .map(|part| {
                    row.values
                        .iter()
                        .find(|(column, _)| *column == part.child_column)
                        .map(|(_, value)| value)
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
            let key = foreign_reference_key(
                foreign_key.parent_table,
                foreign_key.id,
                foreign_key.parent_index,
                images,
                self.primary_index,
                child_images.clone(),
            )?;
            references.push(ForeignReference {
                key,
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
                .map(|part| {
                    row.values
                        .iter()
                        .find(|(column, _)| *column == part.column)
                        .map(|(_, value)| value)
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
        writer
            .field(TAG_PRIMARY_INDEX, &self.primary_index.as_bytes())
            .expect("row field length fits in u32");
        writer
            .field(TAG_TABLE_STORAGE, &[self.storage.to_u8()])
            .expect("row field length fits in u32");
        if let Some(rowid) = row.rowid {
            writer
                .field(TAG_ROWID, &rowid.to_be_bytes())
                .expect("row field length fits in u32");
        }
        for part in &self.key_parts {
            writer
                .field(TAG_KEY_PART, &encode_key_part(*part))
                .expect("row field length fits in u32");
        }
        for index in &self.indexes {
            writer
                .field(TAG_INDEX_RULES, &encode_index_rules(index))
                .expect("row field length fits in u32");
        }
        for foreign_key in &self.foreign_keys {
            writer
                .field(TAG_FOREIGN_KEY, &encode_foreign_key(foreign_key))
                .expect("row field length fits in u32");
        }
        for incoming in &self.incoming_foreign_keys {
            writer
                .field(
                    TAG_INCOMING_FOREIGN_KEY,
                    &encode_incoming_foreign_key(incoming),
                )
                .expect("row field length fits in u32");
        }
        for (column, value) in &row.values {
            writer
                .field(TAG_COLUMN_VALUE, &encode_column_value(*column, value))
                .expect("row field length fits in u32");
        }
        writer.finish()
    }
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

impl DeleteRows {
    pub fn from_captured(
        connection: &Connection,
        captured: &[CapturedRow],
    ) -> Result<Option<Self>> {
        Ok(InsertRows::from_captured(connection, captured)?.map(|deleted| Self { deleted }))
    }

    pub fn to_homebase(&self) -> Result<RowHomebaseOp> {
        self.deleted
            .validate_structure()
            .map_err(|error| Error::InvalidMultiliteOp(error.to_string()))?;
        let mut mutations = Vec::with_capacity(
            self.deleted.rows.len()
                * (self.deleted.indexes.len() + self.deleted.foreign_keys.len() + 1),
        );
        let mut footprint = ConflictFootprint::new();
        for row in &self.deleted.rows {
            let key = self
                .deleted
                .row_key(row)
                .map_err(|error| Error::InvalidMultiliteOp(error.to_string()))?;
            footprint.add_write(key.clone());
            mutations.push(Mutation::Delete { key });
        }
        for row in &self.deleted.rows {
            for entry in self
                .deleted
                .index_entries(row)
                .map_err(|error| Error::InvalidMultiliteOp(error.to_string()))?
            {
                footprint.add_write(entry.key.clone());
                mutations.push(Mutation::Delete { key: entry.key });
            }
            for reference in self
                .deleted
                .foreign_references(row)
                .map_err(|error| Error::InvalidMultiliteOp(error.to_string()))?
            {
                footprint.add_write(reference.key.clone());
                mutations.push(Mutation::Delete { key: reference.key });
            }
        }
        for row in &self.deleted.rows {
            for reference_prefix in self
                .deleted
                .incoming_reference_prefixes(row)
                .map_err(|error| Error::InvalidMultiliteOp(error.to_string()))?
            {
                footprint.add_write(reference_prefix.clone());
                footprint.add_constraint(reference_prefix.clone());
                mutations.push(Mutation::DeleteRange {
                    range: Range::Prefix(reference_prefix),
                });
            }
        }
        footprint.add_constraint(active_primary_index_key(self.deleted.table));
        footprint.add_constraint(write_revision_key(self.deleted.table));
        Ok(RowHomebaseOp {
            mutations,
            footprint,
        })
    }

    pub fn encode(&self) -> Vec<u8> {
        self.deleted.encode()
    }

    pub fn decode(frame: &[u8]) -> std::result::Result<Self, RowCodecError> {
        InsertRows::decode(frame).map(|deleted| Self { deleted })
    }

    pub fn apply(&self, connection: &Connection) -> Result<()> {
        self.deleted
            .delete_materialized_with(connection, "DELETE row no longer matches SQLite state")
    }

    pub fn restore_materialized(&self, connection: &Connection) -> Result<()> {
        self.deleted.apply(connection)
    }
}

impl UpdateRows {
    pub fn from_captured(
        connection: &Connection,
        captured: &[(CapturedRow, CapturedRow)],
    ) -> Result<Option<Self>> {
        let Some((first_before, first_after)) = captured.first() else {
            return Ok(None);
        };
        if first_before.table != first_after.table {
            return Err(Error::CaptureInvariant(
                "one UPDATE changed rows from different tables",
            ));
        }
        let Some(created) = catalog::by_name(connection, &first_before.table)? else {
            return Ok(None);
        };
        let changed = captured
            .iter()
            .filter(|(before, after)| {
                before.table != after.table
                    || before.values != after.values
                    || (created.storage() == TableStorage::Rowid && before.rowid != after.rowid)
            })
            .collect::<Vec<_>>();
        if changed.is_empty() {
            return Ok(None);
        }
        if changed
            .iter()
            .any(|(before, after)| before.table != after.table)
        {
            return Err(Error::CaptureInvariant(
                "one UPDATE changed rows from different tables",
            ));
        }

        let before_rows = changed
            .iter()
            .map(|(before, _)| (*before).clone())
            .collect::<Vec<_>>();
        let after_rows = changed
            .iter()
            .map(|(_, after)| (*after).clone())
            .collect::<Vec<_>>();
        let Some(before) = InsertRows::from_captured(connection, &before_rows)? else {
            return Ok(None);
        };
        let after = InsertRows::from_captured(connection, &after_rows)?.ok_or(
            Error::CaptureInvariant("UPDATE before and after rows resolved differently"),
        )?;
        let updated = Self { before, after };
        if let Err(error) = updated.validate_structure() {
            return Err(match error {
                RowCodecError::RowidChanged => {
                    Error::UnsupportedSql("UPDATE of SQLite rowid is not supported")
                }
                error => Error::InvalidMultiliteOp(error.to_string()),
            });
        }
        Ok(Some(updated))
    }

    pub fn to_homebase(&self) -> Result<RowHomebaseOp> {
        self.validate_structure()
            .map_err(|error| Error::InvalidMultiliteOp(error.to_string()))?;
        let mut mutations = Vec::with_capacity(
            self.after.rows.len()
                * (self.after.indexes.len() * 2 + self.after.foreign_keys.len() * 2 + 2),
        );
        let mut footprint = ConflictFootprint::new();
        let keys = self
            .before
            .rows
            .iter()
            .zip(&self.after.rows)
            .map(|(before, after)| {
                Ok((
                    self.before
                        .row_key(before)
                        .map_err(|error| Error::InvalidMultiliteOp(error.to_string()))?,
                    self.after
                        .row_key(after)
                        .map_err(|error| Error::InvalidMultiliteOp(error.to_string()))?,
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        // Remove every moved source before publishing any destination. If one
        // row moves into another row's former key, the later Set must win.
        for (before, after) in &keys {
            footprint.add_write(before.clone());
            footprint.add_write(after.clone());
            if before != after {
                footprint.add_constraint(after.clone());
                mutations.push(Mutation::Delete {
                    key: before.clone(),
                });
            }
        }
        let before_unique = self
            .before
            .index_entry_map()
            .map_err(|error| Error::InvalidMultiliteOp(error.to_string()))?;
        let after_unique = self
            .after
            .index_entry_map()
            .map_err(|error| Error::InvalidMultiliteOp(error.to_string()))?;
        for (key, owner) in &before_unique {
            if after_unique.get(key) != Some(owner) {
                footprint.add_write(key.clone());
                mutations.push(Mutation::Delete { key: key.clone() });
            }
        }
        let before_references = self
            .before
            .foreign_reference_map()
            .map_err(|error| Error::InvalidMultiliteOp(error.to_string()))?;
        let after_references = self
            .after
            .foreign_reference_map()
            .map_err(|error| Error::InvalidMultiliteOp(error.to_string()))?;
        for (key, owner) in &before_references {
            if after_references.get(key) != Some(owner) {
                footprint.add_write(key.clone());
                mutations.push(Mutation::Delete { key: key.clone() });
            }
        }
        for (row, (_, key)) in self.after.rows.iter().zip(&keys) {
            mutations.push(Mutation::Set {
                key: key.clone(),
                value: self.after.encode_row(row),
            });
        }
        for (key, owner) in &after_unique {
            if before_unique.get(key) != Some(owner) {
                footprint.add_write(key.clone());
                if is_unique_entry_key(key) {
                    footprint.add_constraint(key.clone());
                }
                mutations.push(Mutation::Set {
                    key: key.clone(),
                    value: owner.clone(),
                });
            }
        }
        for (key, owner) in &after_references {
            if before_references.get(key) != Some(owner) {
                footprint.add_write(key.clone());
                footprint.add_constraint(key.clone());
                mutations.push(Mutation::Set {
                    key: key.clone(),
                    value: owner.clone(),
                });
            }
        }
        for (before, after) in self.before.rows.iter().zip(&self.after.rows) {
            let before_prefixes = self
                .before
                .incoming_reference_prefixes(before)
                .map_err(|error| Error::InvalidMultiliteOp(error.to_string()))?
                .into_iter()
                .collect::<BTreeSet<_>>();
            let after_prefixes = self
                .after
                .incoming_reference_prefixes(after)
                .map_err(|error| Error::InvalidMultiliteOp(error.to_string()))?
                .into_iter()
                .collect::<BTreeSet<_>>();
            for reference_prefix in before_prefixes.difference(&after_prefixes) {
                footprint.add_write(reference_prefix.clone());
                footprint.add_constraint(reference_prefix.clone());
                mutations.push(Mutation::DeleteRange {
                    range: Range::Prefix(reference_prefix.clone()),
                });
            }
        }
        footprint.add_constraint(active_primary_index_key(self.after.table));
        footprint.add_constraint(write_revision_key(self.after.table));
        Ok(RowHomebaseOp {
            mutations,
            footprint,
        })
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut writer = Writer::new();
        writer.u8(UPDATE_FRAME_VERSION);
        writer
            .field(TAG_UPDATE_BEFORE, &self.before.encode())
            .expect("row field length fits in u32");
        writer
            .field(TAG_UPDATE_AFTER, &self.after.encode())
            .expect("row field length fits in u32");
        writer.finish()
    }

    pub fn decode(frame: &[u8]) -> std::result::Result<Self, RowCodecError> {
        let mut reader = Reader::new(frame);
        if reader.u8() != Some(UPDATE_FRAME_VERSION) {
            return Err(RowCodecError::UnknownVersion);
        }
        let mut before = None;
        let mut after = None;
        while let Some((tag, value)) = reader.field().map_err(|_| RowCodecError::Truncated)? {
            match tag {
                TAG_UPDATE_BEFORE => set_once(&mut before, InsertRows::decode(value)?)?,
                TAG_UPDATE_AFTER => set_once(&mut after, InsertRows::decode(value)?)?,
                _ => {}
            }
        }
        let updated = Self {
            before: before.ok_or(RowCodecError::MissingField(TAG_UPDATE_BEFORE))?,
            after: after.ok_or(RowCodecError::MissingField(TAG_UPDATE_AFTER))?,
        };
        updated.validate_structure()?;
        Ok(updated)
    }

    pub fn apply(&self, connection: &Connection) -> Result<()> {
        let created = self.validate_against_catalog(connection)?;
        self.apply_direction(
            connection,
            &created,
            &self.before,
            &self.after,
            "UPDATE row no longer matches SQLite state",
        )
    }

    pub fn restore_materialized(&self, connection: &Connection) -> Result<()> {
        let created = self.validate_against_catalog(connection)?;
        self.apply_direction(
            connection,
            &created,
            &self.after,
            &self.before,
            "pending UPDATE row no longer matches SQLite state",
        )
    }

    fn apply_direction(
        &self,
        connection: &Connection,
        created: &CreateTable,
        before: &InsertRows,
        after: &InsertRows,
        mismatch: &'static str,
    ) -> Result<()> {
        before.validate_materialized_against(connection, created, mismatch)?;
        let mut moved_before = before.clone();
        moved_before.rows.clear();
        let mut moved_after = after.clone();
        moved_after.rows.clear();
        let mut stable = Vec::new();
        for (index, (before_row, after_row)) in before.rows.iter().zip(&after.rows).enumerate() {
            let before_key = before
                .row_key(before_row)
                .map_err(|error| Error::InvalidMultiliteOp(error.to_string()))?;
            let after_key = after
                .row_key(after_row)
                .map_err(|error| Error::InvalidMultiliteOp(error.to_string()))?;
            if before_key == after_key {
                stable.push(index);
            } else {
                moved_before.rows.push(before_row.clone());
                moved_after.rows.push(after_row.clone());
            }
        }

        if !moved_before.rows.is_empty() {
            moved_before.delete_materialized_with(connection, mismatch)?;
        }
        self.update_stable_rows(connection, created, before, after, &stable, mismatch)?;
        if !moved_after.rows.is_empty() {
            moved_after.apply(connection)?;
        }
        Ok(())
    }

    fn update_stable_rows(
        &self,
        connection: &Connection,
        created: &CreateTable,
        before: &InsertRows,
        after: &InsertRows,
        stable: &[usize],
        mismatch: &'static str,
    ) -> Result<()> {
        if stable.is_empty() {
            return Ok(());
        }
        let columns = created.columns();
        let assignments = columns
            .iter()
            .map(|column| format!("{} = ?", quote_identifier(column.name().value())))
            .collect::<Vec<_>>()
            .join(", ");
        let mut predicates = created
            .primary_key_columns()
            .map(|column| format!("{} = ?", quote_identifier(column.name().value())))
            .collect::<Vec<_>>();
        let hidden_rowid = hidden_rowid_alias(created)?;
        if let Some(alias) = hidden_rowid {
            predicates.push(format!("{} = ?", quote_identifier(alias)));
        }
        let sql = format!(
            "UPDATE {} SET {assignments} WHERE {}",
            quote_identifier(&materialized_table_name(connection, self.after.table)?),
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
                        .values
                        .iter()
                        .find(|(id, _)| *id == column.id())
                        .map(|(_, value)| value)
                        .ok_or(Error::InvalidMultiliteOp(
                            "row is missing a schema column".into(),
                        ))
                })
                .collect::<Result<Vec<_>>>()?;
            let primary = before.primary_values(before_row)?;
            let mut parameters = Vec::<&dyn ToSql>::with_capacity(
                values.len() + primary.len() + usize::from(hidden_rowid.is_some()),
            );
            parameters.extend(values.into_iter().map(|value| value as &dyn ToSql));
            parameters.extend(primary.into_iter().map(|value| value as &dyn ToSql));
            if hidden_rowid.is_some() {
                parameters.push(before_row.rowid.as_ref().ok_or(Error::InvalidMultiliteOp(
                    "rowid table row is missing its rowid".into(),
                ))?);
            }
            if statement.execute(params_from_iter(parameters))? != 1 {
                return Err(Error::InvalidDatabase(mismatch));
            }
        }
        Ok(())
    }

    fn validate_against_catalog(&self, connection: &Connection) -> Result<CreateTable> {
        let before = self.before.catalog_definition(connection)?;
        let after = self.after.catalog_definition(connection)?;
        if before != after {
            return Err(Error::InvalidMultiliteOp(
                "UPDATE before and after rows use different schemas".into(),
            ));
        }
        Ok(before)
    }

    fn validate_structure(&self) -> std::result::Result<(), RowCodecError> {
        self.before.validate_structure()?;
        self.after.validate_structure()?;
        if self.before.table != self.after.table
            || self.before.schema_revision != self.after.schema_revision
            || self.before.primary_index != self.after.primary_index
            || self.before.storage != self.after.storage
            || self.before.key_parts != self.after.key_parts
            || self.before.indexes != self.after.indexes
            || self.before.foreign_keys != self.after.foreign_keys
            || self.before.incoming_foreign_keys != self.after.incoming_foreign_keys
            || self.before.rows.len() != self.after.rows.len()
        {
            return Err(RowCodecError::InvalidRow);
        }
        let integer_primary_key = self.before.storage == TableStorage::Rowid
            && matches!(
                self.before.key_parts.as_slice(),
                [KeyPartRules {
                    rowid_alias: true,
                    ..
                }]
            );
        for (before, after) in self.before.rows.iter().zip(&self.after.rows) {
            if before.rowid != after.rowid && !integer_primary_key {
                return Err(RowCodecError::RowidChanged);
            }
        }
        Ok(())
    }
}

/// Prefix covering every row encoded under a table's active primary index.
#[cfg(test)]
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
    connection: &Connection,
    created: &CreateTable,
) -> Result<Vec<ForeignKeyRules>> {
    created
        .foreign_keys()
        .iter()
        .map(|foreign_key| foreign_key_rule(connection, created, foreign_key))
        .collect()
}

fn foreign_key_rule(
    connection: &Connection,
    child: &CreateTable,
    foreign_key: &ForeignKeyDefinition,
) -> Result<ForeignKeyRules> {
    let parent = catalog::by_id(connection, foreign_key.referenced_table())?.ok_or(
        Error::InvalidDatabase("foreign key references an unknown parent table"),
    )?;
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
    connection: &Connection,
    parent: &CreateTable,
) -> Result<Vec<IncomingForeignKeyRules>> {
    let mut incoming = Vec::new();
    for (child, foreign_key) in catalog::incoming_foreign_keys(connection, parent.table_id())? {
        // Reuse full relationship validation so corrupt catalog links do not
        // silently weaken parent-side deletion guards.
        let _ = foreign_key_rule(connection, &child, &foreign_key)?;
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
    let mut inserted = InsertRows::from_captured(connection, &captured)?.ok_or(
        Error::CaptureInvariant("index table is missing its schema catalog"),
    )?;
    inserted.indexes = vec![IndexRules {
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
    let hidden_rowid = hidden_rowid_alias(created)?;
    let mut selected = created
        .columns()
        .iter()
        .map(|column| quote_identifier(column.name().value()))
        .collect::<Vec<_>>();
    if let Some(alias) = hidden_rowid {
        selected.push(quote_identifier(alias));
    }
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
        let rowid = if let Some(_) = hidden_rowid {
            row.get(created.columns().len())?
        } else if created.storage() == TableStorage::Rowid {
            let alias = created
                .primary_key_columns()
                .next()
                .filter(|column| created.is_rowid_alias(column.id()))
                .expect("rowid table without a hidden alias has an INTEGER PRIMARY KEY");
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

fn row_prefix(
    table: TableId,
    primary_index: IndexId,
    images: Vec<Vec<u8>>,
) -> std::result::Result<Key, RowCodecError> {
    Key::from_bytes(
        [
            codes::ROOT.to_vec(),
            codes::TABLES.to_vec(),
            table.as_bytes().to_vec(),
            codes::ROWS.to_vec(),
            primary_index.as_bytes().to_vec(),
        ]
        .into_iter()
        .chain(images),
    )
    .map_err(RowCodecError::InvalidKey)
}

fn unique_prefix(
    table: TableId,
    index: IndexId,
    images: Vec<Vec<u8>>,
) -> std::result::Result<Key, RowCodecError> {
    Key::from_bytes(
        [
            codes::ROOT.to_vec(),
            codes::TABLES.to_vec(),
            table.as_bytes().to_vec(),
            codes::UNIQUE.to_vec(),
            index.as_bytes().to_vec(),
        ]
        .into_iter()
        .chain(images),
    )
    .map_err(RowCodecError::InvalidKey)
}

fn is_unique_entry_key(key: &Key) -> bool {
    key.components()
        .get(3)
        .is_some_and(|component| component.as_bytes() == codes::UNIQUE)
}

fn foreign_reference_prefix(
    parent: TableId,
    relationship: ForeignKeyId,
    parent_index: IndexId,
    parent_images: Vec<Vec<u8>>,
) -> std::result::Result<Key, RowCodecError> {
    Key::from_bytes(
        [
            codes::ROOT.to_vec(),
            codes::TABLES.to_vec(),
            parent.as_bytes().to_vec(),
            codes::FOREIGN_REFERENCES.to_vec(),
            relationship.as_bytes().to_vec(),
            parent_index.as_bytes().to_vec(),
        ]
        .into_iter()
        .chain(parent_images),
    )
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
    Key::from_bytes(
        [
            codes::ROOT.to_vec(),
            codes::TABLES.to_vec(),
            parent.as_bytes().to_vec(),
            codes::FOREIGN_REFERENCES.to_vec(),
            relationship.as_bytes().to_vec(),
            parent_index.as_bytes().to_vec(),
        ]
        .into_iter()
        .chain(parent_images)
        .chain([child_primary_index.as_bytes().to_vec()])
        .chain(child_images),
    )
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
    match (column.strict_type(), value) {
        (Some(StrictType::Integer), StoredValue::Integer(_))
        | (Some(StrictType::Real), StoredValue::Real(_))
        | (Some(StrictType::Text), StoredValue::Text(_))
        | (Some(StrictType::Blob), StoredValue::Blob(_))
        | (Some(StrictType::Any), _) => true,
        _ => false,
    }
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
            TAG_VALUE => set_once(&mut value, StoredValue::decode(bytes)?)?,
            _ => {}
        }
    }
    Ok((
        column.ok_or(RowCodecError::MissingField(TAG_COLUMN_ID))?,
        value.ok_or(RowCodecError::MissingField(TAG_VALUE))?,
    ))
}

fn decode_row(
    frame: &[u8],
) -> std::result::Result<
    (
        SchemaRevisionId,
        IndexId,
        TableStorage,
        Vec<KeyPartRules>,
        Vec<IndexRules>,
        Vec<ForeignKeyRules>,
        Vec<IncomingForeignKeyRules>,
        Row,
    ),
    RowCodecError,
> {
    let mut reader = Reader::new(frame);
    if reader.u8() != Some(ROW_FRAME_VERSION) {
        return Err(RowCodecError::UnknownVersion);
    }
    let mut schema_revision = None;
    let mut primary_index = None;
    let mut storage = None;
    let mut rowid = None;
    let mut key_parts = Vec::new();
    let mut indexes = Vec::new();
    let mut foreign_keys = Vec::new();
    let mut incoming_foreign_keys = Vec::new();
    let mut values = Vec::new();
    while let Some((tag, value)) = reader.field().map_err(|_| RowCodecError::Truncated)? {
        match tag {
            TAG_SCHEMA_REVISION => set_once(
                &mut schema_revision,
                SchemaRevisionId::from_bytes(uuid_bytes(value)?),
            )?,
            TAG_PRIMARY_INDEX => {
                set_once(&mut primary_index, IndexId::from_bytes(uuid_bytes(value)?))?
            }
            TAG_TABLE_STORAGE => {
                let [value] = value else {
                    return Err(RowCodecError::InvalidLength);
                };
                set_once(
                    &mut storage,
                    TableStorage::from_u8(*value).ok_or(RowCodecError::InvalidRow)?,
                )?;
            }
            TAG_ROWID => {
                let bytes = value.try_into().map_err(|_| RowCodecError::InvalidLength)?;
                set_once(&mut rowid, i64::from_be_bytes(bytes))?;
            }
            TAG_KEY_PART => key_parts.push(decode_key_part(value)?),
            TAG_INDEX_RULES => indexes.push(decode_index_rules(value)?),
            TAG_FOREIGN_KEY => foreign_keys.push(decode_foreign_key(value)?),
            TAG_INCOMING_FOREIGN_KEY => {
                incoming_foreign_keys.push(decode_incoming_foreign_key(value)?)
            }
            TAG_COLUMN_VALUE => values.push(decode_column_value(value)?),
            _ => {}
        }
    }
    if key_parts.is_empty() || values.is_empty() {
        return Err(RowCodecError::InvalidRow);
    }
    if values
        .iter()
        .enumerate()
        .any(|(index, (column, _))| values[..index].iter().any(|(seen, _)| seen == column))
    {
        return Err(RowCodecError::DuplicateField);
    }
    let storage = storage.ok_or(RowCodecError::MissingField(TAG_TABLE_STORAGE))?;
    if rowid.is_some() != (storage == TableStorage::Rowid) {
        return Err(RowCodecError::InvalidRow);
    }
    Ok((
        schema_revision.ok_or(RowCodecError::MissingField(TAG_SCHEMA_REVISION))?,
        primary_index.ok_or(RowCodecError::MissingField(TAG_PRIMARY_INDEX))?,
        storage,
        key_parts,
        indexes,
        foreign_keys,
        incoming_foreign_keys,
        Row { rowid, values },
    ))
}

/// Replace branch-local sequential hidden rowids with collision-resistant ids.
///
/// INTEGER PRIMARY KEY already supplies stable row identity and is left alone.
pub(super) fn normalize_insert_rowids(
    connection: &Connection,
    captured: &mut [CapturedChange],
) -> Result<()> {
    let Some(first) = captured.first().map(|change| match change {
        CapturedChange::Insert(row) => Ok(row),
        CapturedChange::Delete(_) | CapturedChange::Update { .. } => Err(Error::CaptureInvariant(
            "INSERT captured a deleted application row",
        )),
    }) else {
        return Ok(());
    };
    let first = first?;
    if captured.iter().any(|change| match change {
        CapturedChange::Insert(row) => row.table != first.table,
        CapturedChange::Delete(_) | CapturedChange::Update { .. } => true,
    }) {
        return Err(Error::CaptureInvariant(
            "INSERT captured an unexpected row change",
        ));
    }
    let Some(created) = catalog::by_name(connection, &first.table)? else {
        return Ok(());
    };
    let Some(alias) = hidden_rowid_alias(&created)? else {
        return Ok(());
    };
    let table = quote_identifier(&first.table);
    let alias = quote_identifier(alias);
    let exists_sql = format!("SELECT EXISTS(SELECT 1 FROM {table} WHERE {alias} = ?1)");
    let update_sql = format!("UPDATE {table} SET {alias} = ?1 WHERE {alias} = ?2");
    for change in captured {
        let CapturedChange::Insert(row) = change else {
            unreachable!("all captured changes were checked above")
        };
        let rowid = loop {
            let bytes = Uuid::new_v4().into_bytes();
            let candidate =
                i64::from_be_bytes(bytes[..8].try_into().expect("UUID has 16 bytes")) & i64::MAX;
            if candidate == 0 {
                continue;
            }
            let exists: bool =
                connection.query_row(&exists_sql, [candidate], |result| result.get(0))?;
            if !exists {
                break candidate;
            }
        };
        let changed = connection.execute(&update_sql, [rowid, row.rowid])?;
        if changed != 1 {
            return Err(Error::CaptureInvariant(
                "captured INSERT rowid no longer identifies exactly one row",
            ));
        }
        row.rowid = rowid;
    }
    Ok(())
}

fn hidden_rowid_alias(created: &CreateTable) -> Result<Option<&'static str>> {
    created
        .hidden_rowid_alias()
        .map(Some)
        .or_else(|| {
            (created.storage() == TableStorage::WithoutRowid
                || created
                    .primary_key_columns()
                    .any(|column| created.is_rowid_alias(column.id())))
            .then_some(None)
        })
        .ok_or(Error::InvalidDatabase(
            "tables with a non-integer primary key must leave one SQLite rowid alias unshadowed",
        ))
}

fn materialized_table_name(connection: &Connection, table: TableId) -> Result<String> {
    catalog::name_by_id(connection, table)?
        .map(|name| name.value().to_owned())
        .ok_or(Error::InvalidDatabase(
            "row operation references a table without a current name binding",
        ))
}

fn quote_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

fn set_once<T>(slot: &mut Option<T>, value: T) -> std::result::Result<(), RowCodecError> {
    if slot.replace(value).is_some() {
        Err(RowCodecError::DuplicateField)
    } else {
        Ok(())
    }
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
    RowidChanged,
    NullPrimaryKey,
    InvalidKey(KeyError),
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
            Self::RowidChanged => f.write_str("UPDATE of SQLite rowid is not supported"),
            Self::NullPrimaryKey => f.write_str("primary key value is NULL"),
            Self::InvalidKey(error) => write!(f, "invalid Homebase row key: {error}"),
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

    use super::*;
    use crate::commit::footprint::assert_explicit_range_assertions;
    use crate::database::index::IndexOperation;
    use crate::database::schema::{
        CreateColumn, CreateTableSpec, CreateUnique, SqlName, TypeDeclaration,
    };

    fn definition() -> CreateTable {
        CreateTable::new(
            "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT, payload BLOB)",
            CreateTableSpec {
                name: SqlName::new("notes".into()),
                mode: Default::default(),
                storage: crate::database::schema::TableStorage::Rowid,
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
        let super::super::sql::ValidatedExecute::CreateIndex(spec) =
            super::super::sql::validate_execute(sql).unwrap()
        else {
            unreachable!()
        };
        connection.execute(sql, ()).unwrap();
        let operation = IndexOperation::prepare_create(connection, sql, &spec).unwrap();
        operation.record_catalog(connection).unwrap();
    }

    fn without_rowid_definition() -> CreateTable {
        let sql = "CREATE TABLE memberships (
            tenant TEXT,
            member INTEGER,
            body TEXT,
            PRIMARY KEY (member, tenant)
        ) WITHOUT ROWID";
        let super::super::sql::ValidatedExecute::CreateTable(spec) =
            super::super::sql::validate_execute(sql).unwrap()
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
                storage: crate::database::schema::TableStorage::Rowid,
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
                storage: crate::database::schema::TableStorage::Rowid,
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

    fn inserted(connection: &Connection) -> InsertRows {
        InsertRows::from_captured(
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

    fn foreign_key_tables() -> (Connection, CreateTable, CreateTable) {
        let connection = Connection::open_in_memory().unwrap();
        catalog::initialize(&connection).unwrap();
        let parent_sql = "CREATE TABLE parents (id INTEGER PRIMARY KEY, body TEXT)";
        let super::super::sql::ValidatedExecute::CreateTable(parent_spec) =
            super::super::sql::validate_execute(parent_sql).unwrap()
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
        let super::super::sql::ValidatedExecute::CreateTable(child_spec) =
            super::super::sql::validate_execute(child_sql).unwrap()
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
        let super::super::sql::ValidatedExecute::CreateTable(parent_spec) =
            super::super::sql::validate_execute(parent_sql).unwrap()
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
        let super::super::sql::ValidatedExecute::CreateTable(child_spec) =
            super::super::sql::validate_execute(child_sql).unwrap()
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

    #[test]
    fn insert_codec_and_homebase_envelope_roundtrip() {
        let created = definition();
        let connection = connection(&created);
        let inserted = inserted(&connection);

        assert_eq!(InsertRows::decode(&inserted.encode()).unwrap(), inserted);
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
        assert_eq!(
            InsertRows::from_homebase(&admit(lowered.mutations)).unwrap(),
            inserted
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
        let inserted = InsertRows::from_captured(&connection, std::slice::from_ref(&child_row))
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
        assert_eq!(
            InsertRows::from_homebase(&admit(lowered.mutations.clone())).unwrap(),
            inserted
        );
        let mut missing_reference = admit(lowered.mutations.clone());
        missing_reference.entries.pop();
        assert_eq!(
            InsertRows::from_homebase(&missing_reference),
            Err(RowCodecError::InvalidBatch)
        );
        let mut corrupt_reference = admit(lowered.mutations);
        let Mutation::Set { value, .. } = &mut corrupt_reference.entries[1].device_entry.mutation
        else {
            unreachable!()
        };
        value.push(0);
        assert_eq!(
            InsertRows::from_homebase(&corrupt_reference),
            Err(RowCodecError::InvalidBatch)
        );

        let deleted = DeleteRows::from_captured(&connection, std::slice::from_ref(&child_row))
            .unwrap()
            .unwrap()
            .to_homebase()
            .unwrap();
        assert!(matches!(
            &deleted.mutations[1],
            Mutation::Delete { key } if key == &reference.key
        ));

        let parent_delete = DeleteRows::from_captured(
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

        let parent_move = UpdateRows::from_captured(
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

        let null_child = InsertRows::from_captured(
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
        let inserted = InsertRows::from_captured(&connection, &[child_row])
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
        assert_eq!(InsertRows::decode(&inserted.encode()).unwrap(), inserted);

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
        let updated = UpdateRows::from_captured(&connection, &[(before.clone(), after)])
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
    fn separate_foreign_keys_to_one_parent_keep_distinct_reference_cells() {
        let connection = Connection::open_in_memory().unwrap();
        catalog::initialize(&connection).unwrap();
        let parent_sql = "CREATE TABLE people (id INTEGER PRIMARY KEY)";
        let super::super::sql::ValidatedExecute::CreateTable(parent_spec) =
            super::super::sql::validate_execute(parent_sql).unwrap()
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
        let super::super::sql::ValidatedExecute::CreateTable(child_spec) =
            super::super::sql::validate_execute(child_sql).unwrap()
        else {
            unreachable!()
        };
        let child = CreateTable::prepare(&connection, child_sql, child_spec).unwrap();
        connection.execute(child_sql, ()).unwrap();
        catalog::insert(&connection, &child).unwrap();

        let inserted = InsertRows::from_captured(
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
        let updated = UpdateRows::from_captured(&connection, &[(before, after)])
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
                active_primary_index_key(updated.after.table),
                write_revision_key(updated.after.table),
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
        let inserted = InsertRows::from_captured(&connection, std::slice::from_ref(&captured))
            .unwrap()
            .unwrap();

        assert_eq!(inserted.storage, TableStorage::WithoutRowid);
        assert_eq!(inserted.rows[0].rowid, None);
        assert_eq!(
            inserted
                .key_parts
                .iter()
                .map(|part| part.column)
                .collect::<Vec<_>>(),
            [created.columns()[1].id(), created.columns()[0].id(),]
        );
        assert_eq!(InsertRows::decode(&inserted.encode()).unwrap(), inserted);
        let lowered = inserted.to_homebase().unwrap();
        assert_eq!(lowered.mutations[0].key().components().len(), 7);

        let mut same_row = captured.clone();
        same_row.rowid = 999;
        assert!(
            UpdateRows::from_captured(&connection, &[(captured, same_row)])
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
        let inserted = InsertRows::from_captured(&connection, &captured)
            .unwrap()
            .unwrap();

        assert_eq!(InsertRows::decode(&inserted.encode()).unwrap(), inserted);
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
        assert_eq!(
            InsertRows::from_homebase(&admit(lowered.mutations)).unwrap(),
            inserted
        );
        let deleted = DeleteRows::from_captured(&connection, &captured)
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
        let baseline = InsertRows::from_captured(&connection, &captured)
            .unwrap()
            .unwrap()
            .to_homebase()
            .unwrap();

        add_index(&connection, "CREATE INDEX notes_body ON notes (body)");
        add_index(
            &connection,
            "CREATE INDEX notes_payload_body ON notes (payload, body)",
        );
        let inserted = InsertRows::from_captured(&connection, &captured)
            .unwrap()
            .unwrap();

        assert!(inserted.indexes.is_empty());
        assert_eq!(InsertRows::decode(&inserted.encode()).unwrap(), inserted);
        let lowered = inserted.to_homebase().unwrap();
        assert!(
            lowered
                .mutations
                .iter()
                .all(|mutation| mutation.key().components()[3].as_bytes() != codes::INDEXES)
        );
        assert_eq!(lowered.mutations.len(), baseline.mutations.len());
        assert_eq!(lowered.footprint, baseline.footprint);
        assert_eq!(
            InsertRows::from_homebase(&admit(lowered.mutations)).unwrap(),
            inserted
        );

        let deleted = DeleteRows::from_captured(&connection, &captured)
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

        let updated = UpdateRows::from_captured(
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
        let inserted = InsertRows::from_captured(&connection, std::slice::from_ref(&captured))
            .unwrap()
            .unwrap();

        assert_eq!(InsertRows::decode(&inserted.encode()).unwrap(), inserted);
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
        assert_eq!(
            InsertRows::from_homebase(&admit(lowered.mutations.clone())).unwrap(),
            inserted
        );

        let deleted = DeleteRows::from_captured(&connection, &[captured])
            .unwrap()
            .unwrap()
            .to_homebase()
            .unwrap();
        assert_eq!(deleted.mutations.len(), lowered.mutations.len());
        for (inserted, deleted) in lowered.mutations.iter().zip(&deleted.mutations) {
            assert_eq!(inserted.key(), deleted.key());
            assert!(matches!(deleted, Mutation::Delete { .. }));
        }
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
            InsertRows::from_captured(&connection, &captured),
            Err(Error::InvalidMultiliteOp(message))
                if message == "row operation contains a duplicate UNIQUE key"
        ));

        let mut inserted =
            InsertRows::from_captured(&connection, std::slice::from_ref(&captured[0]))
                .unwrap()
                .unwrap();
        let duplicate = InsertRows::from_captured(&connection, std::slice::from_ref(&captured[1]))
            .unwrap()
            .unwrap();
        inserted.rows.extend(duplicate.rows);

        assert_eq!(
            InsertRows::decode(&inserted.encode()),
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
            InsertRows::from_captured(&connection, &[profile(1, "acme", "email", "username")])
                .unwrap()
                .unwrap();
        let lowered = inserted.to_homebase().unwrap().mutations;

        let mut missing = lowered.clone();
        missing.pop();
        assert_eq!(
            InsertRows::from_homebase(&admit(missing)),
            Err(RowCodecError::InvalidBatch)
        );

        let mut crossed = lowered.clone();
        crossed.swap(1, 2);
        assert_eq!(
            InsertRows::from_homebase(&admit(crossed)),
            Err(RowCodecError::InvalidBatch)
        );

        let mut extra = lowered.clone();
        extra.push(lowered.last().unwrap().clone());
        assert_eq!(
            InsertRows::from_homebase(&admit(extra)),
            Err(RowCodecError::InvalidBatch)
        );

        let mut corrupt = lowered;
        let Mutation::Set { value, .. } = &mut corrupt[1] else {
            unreachable!()
        };
        value.push(0);
        assert_eq!(
            InsertRows::from_homebase(&admit(corrupt)),
            Err(RowCodecError::InvalidBatch)
        );
    }

    #[test]
    fn delete_codec_lowers_exact_keys_and_restores_complete_rows() {
        let created = definition();
        let connection = connection(&created);
        let inserted = inserted(&connection);
        let deleted = DeleteRows {
            deleted: inserted.clone(),
        };

        assert_eq!(DeleteRows::decode(&deleted.encode()).unwrap(), deleted);
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
        for (declaration, expected_hidden) in [("INTEGER", false), ("INT NOT NULL", true)] {
            let sql = format!("CREATE TABLE aliases (id {declaration} PRIMARY KEY, body TEXT)");
            let super::super::sql::ValidatedExecute::CreateTable(spec) =
                super::super::sql::validate_execute(&sql).unwrap()
            else {
                unreachable!()
            };
            let created = CreateTable::new(&sql, spec);
            assert_eq!(
                hidden_rowid_alias(&created).unwrap().is_some(),
                expected_hidden,
                "{declaration}"
            );
            assert_eq!(
                created
                    .primary_key_columns()
                    .next()
                    .unwrap()
                    .affinity(created.mode()),
                Affinity::Integer
            );
        }
    }

    #[test]
    fn stable_update_codec_replaces_rows_and_restores_before_images() {
        let created = definition();
        let connection = connection(&created);
        let updated = UpdateRows::from_captured(
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

        assert_eq!(UpdateRows::decode(&updated.encode()).unwrap(), updated);
        let lowered = updated.to_homebase().unwrap();
        assert_eq!(lowered.mutations.len(), 2);
        assert!(
            lowered
                .mutations
                .iter()
                .all(|mutation| matches!(mutation, Mutation::Set { .. }))
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
        let updated = UpdateRows::from_captured(&connection, &[(before, after)])
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
        let updated = UpdateRows::from_captured(&connection, &[(before, after)])
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
        let updated = UpdateRows::from_captured(&connection, &[(before, after)])
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

        let removed = UpdateRows::from_captured(&connection, &[(present.clone(), absent.clone())])
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

        let created = UpdateRows::from_captured(&connection, &[(absent, present)])
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
    fn primary_key_update_moves_the_row_and_hidden_rowid_changes_stay_rejected() {
        let created = definition();
        let notes_connection = connection(&created);
        let moved = UpdateRows::from_captured(
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
            UpdateRows::from_captured(
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
            Err(Error::InvalidMultiliteOp(_))
        ));

        let documents = CreateTable::new(
            "CREATE TABLE documents (id TEXT NOT NULL PRIMARY KEY, body TEXT)",
            CreateTableSpec {
                name: SqlName::new("documents".into()),
                mode: Default::default(),
                storage: crate::database::schema::TableStorage::Rowid,
                columns: vec![
                    CreateColumn {
                        name: SqlName::new("id".into()),
                        declared_type: TypeDeclaration::text(),
                        not_null: true,
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
                ],
                unique_constraints: Vec::new(),
                foreign_keys: Vec::new(),
                primary_key_name: None,
                checks: Vec::new(),
            },
        );
        let documents_connection = connection(&documents);
        assert!(matches!(
            UpdateRows::from_captured(
                &documents_connection,
                &[(
                    CapturedRow {
                        table: "documents".into(),
                        rowid: 11,
                        values: vec![
                            StoredValue::Text(b"a".to_vec()),
                            StoredValue::Text(b"before".to_vec()),
                        ],
                    },
                    CapturedRow {
                        table: "documents".into(),
                        rowid: 12,
                        values: vec![
                            StoredValue::Text(b"a".to_vec()),
                            StoredValue::Text(b"after".to_vec()),
                        ],
                    },
                )],
            ),
            Err(Error::UnsupportedSql(
                "UPDATE of SQLite rowid is not supported"
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
        let updated = UpdateRows::from_captured(
            &connection,
            &[
                (row(1, "one"), row(2, "one-moved")),
                (row(2, "two"), row(3, "two-moved")),
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
        assert_eq!(second_source, first_destination);
        assert_ne!(first_source, first_destination);
        assert_ne!(second_source, second_destination);

        updated.before.apply(&connection).unwrap();
        updated.apply(&connection).unwrap();
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
        let updated = UpdateRows::from_captured(
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
                "UPDATE row no longer matches SQLite state"
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
    fn row_set_codec_rejects_duplicate_primary_key_images() {
        let created = definition();
        let connection = connection(&created);
        let mut inserted = inserted(&connection);
        inserted.rows.push(inserted.rows[0].clone());

        assert_eq!(
            InsertRows::decode(&inserted.encode()),
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
        let deleted = DeleteRows {
            deleted: inserted.clone(),
        };
        inserted.apply(&connection).unwrap();
        connection
            .execute("UPDATE notes SET body = 'changed' WHERE id = 7", ())
            .unwrap();

        assert!(matches!(
            deleted.apply(&connection),
            Err(Error::InvalidDatabase(
                "DELETE row no longer matches SQLite state"
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
            &super::super::schema::SqlName::new("archived_notes".into()),
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
        let super::super::sql::ValidatedExecute::CreateTable(spec) =
            super::super::sql::validate_execute(sql).unwrap()
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

        let inserted = InsertRows::from_captured(&connection, &[captured])
            .unwrap()
            .unwrap();
        let lowered = inserted.to_homebase().unwrap();
        assert_eq!(lowered.mutations.len(), 2);
        assert_eq!(lowered.footprint.writes().len(), 2);
        assert_eq!(InsertRows::decode(&inserted.encode()).unwrap(), inserted);
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
        let super::super::sql::ValidatedExecute::CreateTable(spec) =
            super::super::sql::validate_execute(sql).unwrap()
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

        let inserted = InsertRows::from_captured(&connection, &captured)
            .unwrap()
            .unwrap();
        assert_eq!(inserted.indexes[0].parts[0].affinity, Affinity::Blob);
        assert_eq!(inserted.to_homebase().unwrap().mutations.len(), 4);

        let mut invalid = inserted;
        let count = created.columns()[1].id();
        invalid.rows[0]
            .values
            .iter_mut()
            .find(|(column, _)| *column == count)
            .unwrap()
            .1 = StoredValue::Text(b"invalid".to_vec());
        assert!(matches!(
            invalid.validate_against(&connection, &created),
            Err(Error::InvalidMultiliteOp(message))
                if message == "row value has an invalid STRICT storage class"
        ));
    }

    #[test]
    fn non_integer_primary_keys_keep_collision_resistant_hidden_rowids() {
        let created = CreateTable::new(
            "CREATE TABLE documents (id TEXT NOT NULL PRIMARY KEY, body TEXT)",
            CreateTableSpec {
                name: SqlName::new("documents".into()),
                mode: Default::default(),
                storage: crate::database::schema::TableStorage::Rowid,
                columns: vec![
                    CreateColumn {
                        name: SqlName::new("id".into()),
                        declared_type: TypeDeclaration::text(),
                        not_null: true,
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
                ],
                unique_constraints: Vec::new(),
                foreign_keys: Vec::new(),
                primary_key_name: None,
                checks: Vec::new(),
            },
        );
        let source = connection(&created);
        source
            .execute("INSERT INTO documents VALUES ('a', 'one')", ())
            .unwrap();
        let mut changes = vec![CapturedChange::Insert(CapturedRow {
            table: "documents".into(),
            rowid: 1,
            values: vec![
                StoredValue::Text(b"a".to_vec()),
                StoredValue::Text(b"one".to_vec()),
            ],
        })];
        normalize_insert_rowids(&source, &mut changes).unwrap();
        let [CapturedChange::Insert(captured)] = changes.as_slice() else {
            unreachable!()
        };
        let captured = vec![captured.clone()];
        assert_ne!(captured[0].rowid, 1);

        let inserted = InsertRows::from_captured(&source, &captured)
            .unwrap()
            .unwrap();
        assert_eq!(InsertRows::decode(&inserted.encode()).unwrap(), inserted);

        let target = connection(&created);
        inserted.apply(&target).unwrap();
        assert_eq!(
            target
                .query_row("SELECT _rowid_, id, body FROM documents", (), |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                    ))
                },)
                .unwrap(),
            (captured[0].rowid, "a".into(), "one".into())
        );

        let replacement_rowid = if captured[0].rowid == i64::MAX {
            captured[0].rowid - 1
        } else {
            captured[0].rowid + 1
        };
        target
            .execute(
                "UPDATE documents SET _rowid_ = ?1 WHERE id = 'a'",
                [replacement_rowid],
            )
            .unwrap();
        assert!(matches!(
            inserted.delete_materialized(&target),
            Err(Error::InvalidDatabase(
                "pending INSERT row no longer matches SQLite state"
            ))
        ));
        assert_eq!(
            target
                .query_row("SELECT count(*) FROM documents", (), |row| row
                    .get::<_, i64>(0))
                .unwrap(),
            1
        );
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
            Err(RowCodecError::InvalidLength)
        );
        assert_eq!(
            StoredValue::decode(&[1, 0]),
            Err(RowCodecError::InvalidLength)
        );
    }
}
