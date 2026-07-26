//! Captured SQLite rows and their durable Homebase representation.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use homebase_core::key::{Key, KeyError};
#[cfg(test)]
use homebase_core::messages::AdmittedBatch;
use homebase_core::reader::Reader;
use homebase_core::tag::Mutation;
use homebase_core::writer::Writer;
use rusqlite::{Connection, OptionalExtension, ToSql, params_from_iter};
use uuid::{Uuid, Variant, Version};

use super::schema::{
    Affinity, Column, ColumnId, CreateTable, RowKeyspaceId, SchemaRevisionId, StrictType, TableId,
    TableMode, UniqueKeyspaceId, active_row_keyspace_key, write_revision_key,
};
use super::{catalog, codes};
use crate::commit::footprint::ConflictFootprint;
pub(crate) use crate::value::StoredValue;
use crate::{Error, Result};

const ROW_FRAME_VERSION: u8 = 2;
const ROW_SET_FRAME_VERSION: u8 = 1;
const UPDATE_FRAME_VERSION: u8 = 1;
const TAG_SCHEMA_REVISION: u8 = 1;
const TAG_ROW_KEYSPACE: u8 = 2;
const TAG_KEY_PART: u8 = 3;
const TAG_COLUMN_VALUE: u8 = 4;
const TAG_ROWID: u8 = 5;
const TAG_UNIQUE_KEY: u8 = 6;
const TAG_TABLE: u8 = 1;
const TAG_ROW: u8 = 2;
const TAG_COLUMN_ID: u8 = 1;
const TAG_COLUMN_AFFINITY: u8 = 2;
const TAG_KEY_PART_FLAGS: u8 = 3;
const TAG_VALUE: u8 = 2;
const TAG_UPDATE_BEFORE: u8 = 1;
const TAG_UPDATE_AFTER: u8 = 2;
const TAG_UNIQUE_KEYSPACE: u8 = 1;
const TAG_UNIQUE_KEY_PART: u8 = 2;
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

/// Ordered comparison rules for one table-owned UNIQUE keyspace.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UniqueKeyRules {
    keyspace: UniqueKeyspaceId,
    key_parts: Vec<KeyPartRules>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Row {
    rowid: i64,
    values: Vec<(ColumnId, StoredValue)>,
}

/// One logical multi-row INSERT captured from a single SQLite statement.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InsertRows {
    table: TableId,
    schema_revision: SchemaRevisionId,
    row_keyspace: RowKeyspaceId,
    key_parts: Vec<KeyPartRules>,
    unique_keys: Vec<UniqueKeyRules>,
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
                rowid_alias: column.is_rowid_alias(),
            })
            .collect::<Vec<_>>();
        let unique_keys = created
            .unique_constraints()
            .iter()
            .map(|unique| UniqueKeyRules {
                keyspace: unique.keyspace_id(),
                key_parts: unique
                    .columns()
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
                    .collect(),
            })
            .collect();
        let rows = captured
            .iter()
            .map(|captured| Row {
                rowid: captured.rowid,
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
            row_keyspace: created.row_keyspace_id(),
            key_parts,
            unique_keys,
            rows,
        };
        inserted.validate_against(&created)?;
        Ok(Some(inserted))
    }

    pub fn to_homebase(&self) -> Result<RowHomebaseOp> {
        self.validate_structure()
            .map_err(|error| Error::InvalidMultiliteOp(error.to_string()))?;
        let mut mutations = Vec::with_capacity(self.rows.len() * (self.unique_keys.len() + 1));
        let mut footprint = ConflictFootprint::new();
        for row in &self.rows {
            let key = self
                .row_key(row)
                .map_err(|error| Error::InvalidMultiliteOp(error.to_string()))?;
            footprint.add_write(key.clone());
            mutations.push(Mutation::Set {
                key,
                value: self.encode_row(row),
            });
        }
        for row in &self.rows {
            for (key, owner) in self
                .unique_entries(row)
                .map_err(|error| Error::InvalidMultiliteOp(error.to_string()))?
            {
                footprint.add_write(key.clone());
                mutations.push(Mutation::Set { key, value: owner });
            }
        }
        footprint.add_constraint(active_row_keyspace_key(self.table));
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
            let row_keyspace = RowKeyspaceId::from_bytes(uuid_bytes(components[4].as_bytes())?);
            let (schema_revision, encoded_keyspace, key_parts, unique_keys, row) =
                decode_row(value)?;
            if encoded_keyspace != row_keyspace {
                return Err(RowCodecError::InvalidBatch);
            }
            let candidate = operation.get_or_insert_with(|| Self {
                table,
                schema_revision,
                row_keyspace,
                key_parts: key_parts.clone(),
                unique_keys: unique_keys.clone(),
                rows: Vec::new(),
            });
            if candidate.table != table
                || candidate.schema_revision != schema_revision
                || candidate.row_keyspace != row_keyspace
                || candidate.key_parts != key_parts
                || candidate.unique_keys != unique_keys
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
                    let (schema_revision, row_keyspace, key_parts, unique_keys, row) =
                        decode_row(value)?;
                    let candidate = operation.get_or_insert_with(|| Self {
                        table,
                        schema_revision,
                        row_keyspace,
                        key_parts: key_parts.clone(),
                        unique_keys: unique_keys.clone(),
                        rows: Vec::new(),
                    });
                    if candidate.table != table
                        || candidate.schema_revision != schema_revision
                        || candidate.row_keyspace != row_keyspace
                        || candidate.key_parts != key_parts
                        || candidate.unique_keys != unique_keys
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
            quote_identifier(created.table_name())
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
                parameters.push(&row.rowid);
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
        let columns = created.columns();
        let primary = created.primary_key_columns().collect::<Vec<_>>();
        let mut predicates = primary
            .iter()
            .map(|column| format!("{} = ?", quote_identifier(column.name().value())))
            .collect::<Vec<_>>();
        let hidden_rowid = hidden_rowid_alias(&created)?;
        if let Some(alias) = hidden_rowid {
            predicates.push(format!("{} = ?", quote_identifier(alias)));
        }
        let predicate = predicates.join(" AND ");
        let sql = format!(
            "DELETE FROM {} WHERE {predicate}",
            quote_identifier(created.table_name())
        );
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
            quote_identifier(created.table_name())
        );
        let mut select = connection.prepare(&select_sql)?;
        for row in self.rows.iter().rev() {
            let primary = self.primary_values(row)?;
            let mut parameters = Vec::<&dyn ToSql>::with_capacity(
                primary.len() + usize::from(hidden_rowid.is_some()),
            );
            parameters.extend(primary.into_iter().map(|value| value as &dyn ToSql));
            if hidden_rowid.is_some() {
                parameters.push(&row.rowid);
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
            if actual != Some((expected, hidden_rowid.map(|_| row.rowid))) {
                return Err(Error::InvalidDatabase(mismatch));
            }
        }
        let mut delete = connection.prepare(&sql)?;
        for row in self.rows.iter().rev() {
            let primary = self.primary_values(row)?;
            let mut parameters = Vec::<&dyn ToSql>::with_capacity(
                primary.len() + usize::from(hidden_rowid.is_some()),
            );
            parameters.extend(primary.into_iter().map(|value| value as &dyn ToSql));
            if hidden_rowid.is_some() {
                parameters.push(&row.rowid);
            }
            if delete.execute(params_from_iter(parameters))? != 1 {
                return Err(Error::InvalidDatabase(mismatch));
            }
        }
        Ok(())
    }

    fn catalog_definition(&self, connection: &Connection) -> Result<CreateTable> {
        let created = catalog::by_id(connection, self.table)?.ok_or(Error::InvalidDatabase(
            "row operation references an unknown table",
        ))?;
        self.validate_against(&created)?;
        Ok(created)
    }

    fn validate_against(&self, created: &CreateTable) -> Result<()> {
        self.validate_structure()
            .map_err(|error| Error::InvalidMultiliteOp(error.to_string()))?;
        let expected_key_parts = created
            .primary_key_columns()
            .map(|column| KeyPartRules {
                column: column.id(),
                affinity: column.affinity(created.mode()),
                rowid_alias: column.is_rowid_alias(),
            })
            .collect::<Vec<_>>();
        let expected_unique_keys = unique_key_rules(created);
        if self.table != created.table_id()
            || self.schema_revision != created.schema_revision_id()
            || self.row_keyspace != created.row_keyspace_id()
            || self.key_parts != expected_key_parts
            || self.unique_keys != expected_unique_keys
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
            self.unique_entries(row).map_err(|error| {
                Error::InvalidMultiliteOp(format!("invalid UNIQUE key image: {error}"))
            })?;
        }
        Ok(())
    }

    fn validate_structure(&self) -> std::result::Result<(), RowCodecError> {
        if self.key_parts.is_empty() || self.rows.is_empty() {
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
        if self.unique_keys.iter().enumerate().any(|(index, unique)| {
            unique.key_parts.is_empty()
                || unique.key_parts.iter().any(|part| part.rowid_alias)
                || self.unique_keys[..index]
                    .iter()
                    .any(|seen| seen.keyspace == unique.keyspace)
                || unique
                    .key_parts
                    .iter()
                    .enumerate()
                    .any(|(part_index, part)| {
                        unique.key_parts[..part_index]
                            .iter()
                            .any(|seen| seen.column == part.column)
                    })
        }) {
            return Err(RowCodecError::InvalidRow);
        }
        let mut keys = BTreeSet::new();
        let mut unique_keys = BTreeSet::new();
        for row in &self.rows {
            if let [
                KeyPartRules {
                    column,
                    rowid_alias: true,
                    ..
                },
            ] = self.key_parts.as_slice()
            {
                let rowid_matches = row
                    .values
                    .iter()
                    .any(|(id, value)| id == column && *value == StoredValue::Integer(row.rowid));
                if !rowid_matches {
                    return Err(RowCodecError::InvalidRow);
                }
            }
            if !keys.insert(self.row_key(row)?) {
                return Err(RowCodecError::DuplicateRow);
            }
            for (key, _) in self.unique_entries(row)? {
                if !unique_keys.insert(key) {
                    return Err(RowCodecError::DuplicateUniqueKey);
                }
            }
        }
        Ok(())
    }

    fn row_key(&self, row: &Row) -> std::result::Result<Key, RowCodecError> {
        let images = self.key_images(row)?;
        row_prefix(self.table, self.row_keyspace, images)
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

    fn unique_entries(&self, row: &Row) -> std::result::Result<Vec<(Key, Vec<u8>)>, RowCodecError> {
        let owner = self.row_key(row)?.encode();
        let mut entries = Vec::with_capacity(self.unique_keys.len());
        for unique in &self.unique_keys {
            let values = unique
                .key_parts
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
                .zip(&unique.key_parts)
                .map(|(value, rules)| key_image(value, *rules))
                .collect::<std::result::Result<Vec<_>, _>>()?;
            entries.push((
                unique_prefix(self.table, unique.keyspace, images)?,
                owner.clone(),
            ));
        }
        Ok(entries)
    }

    fn unique_entry_map(&self) -> std::result::Result<BTreeMap<Key, Vec<u8>>, RowCodecError> {
        let mut entries = BTreeMap::new();
        for row in &self.rows {
            for (key, owner) in self.unique_entries(row)? {
                if entries.insert(key, owner).is_some() {
                    return Err(RowCodecError::DuplicateUniqueKey);
                }
            }
        }
        Ok(entries)
    }

    fn encode_row(&self, row: &Row) -> Vec<u8> {
        let mut writer = Writer::new();
        writer.u8(ROW_FRAME_VERSION);
        writer
            .field(TAG_SCHEMA_REVISION, &self.schema_revision.as_bytes())
            .expect("row field length fits in u32");
        writer
            .field(TAG_ROW_KEYSPACE, &self.row_keyspace.as_bytes())
            .expect("row field length fits in u32");
        writer
            .field(TAG_ROWID, &row.rowid.to_be_bytes())
            .expect("row field length fits in u32");
        for part in &self.key_parts {
            writer
                .field(TAG_KEY_PART, &encode_key_part(*part))
                .expect("row field length fits in u32");
        }
        for unique in &self.unique_keys {
            writer
                .field(TAG_UNIQUE_KEY, &encode_unique_key(unique))
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

fn encode_unique_key(unique: &UniqueKeyRules) -> Vec<u8> {
    let mut writer = Writer::new();
    writer
        .field(TAG_UNIQUE_KEYSPACE, &unique.keyspace.as_bytes())
        .expect("row field length fits in u32");
    for part in &unique.key_parts {
        writer
            .field(TAG_UNIQUE_KEY_PART, &encode_key_part(*part))
            .expect("row field length fits in u32");
    }
    writer.finish()
}

fn decode_unique_key(frame: &[u8]) -> std::result::Result<UniqueKeyRules, RowCodecError> {
    let mut reader = Reader::new(frame);
    let mut keyspace = None;
    let mut key_parts = Vec::new();
    while let Some((tag, value)) = reader.field().map_err(|_| RowCodecError::Truncated)? {
        match tag {
            TAG_UNIQUE_KEYSPACE => set_once(
                &mut keyspace,
                UniqueKeyspaceId::from_bytes(uuid_bytes(value)?),
            )?,
            TAG_UNIQUE_KEY_PART => key_parts.push(decode_key_part(value)?),
            _ => {}
        }
    }
    if key_parts.is_empty()
        || key_parts.iter().enumerate().any(|(index, part)| {
            key_parts[..index]
                .iter()
                .any(|seen| seen.column == part.column)
        })
    {
        return Err(RowCodecError::InvalidRow);
    }
    Ok(UniqueKeyRules {
        keyspace: keyspace.ok_or(RowCodecError::MissingField(TAG_UNIQUE_KEYSPACE))?,
        key_parts,
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
        let mut mutations =
            Vec::with_capacity(self.deleted.rows.len() * (self.deleted.unique_keys.len() + 1));
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
            for (key, _) in self
                .deleted
                .unique_entries(row)
                .map_err(|error| Error::InvalidMultiliteOp(error.to_string()))?
            {
                footprint.add_write(key.clone());
                mutations.push(Mutation::Delete { key });
            }
        }
        footprint.add_constraint(active_row_keyspace_key(self.deleted.table));
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
        let changed = captured
            .iter()
            .filter(|(before, after)| before != after)
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
        let mut mutations =
            Vec::with_capacity(self.after.rows.len() * (self.after.unique_keys.len() * 2 + 2));
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
                mutations.push(Mutation::Delete {
                    key: before.clone(),
                });
            }
        }
        let before_unique = self
            .before
            .unique_entry_map()
            .map_err(|error| Error::InvalidMultiliteOp(error.to_string()))?;
        let after_unique = self
            .after
            .unique_entry_map()
            .map_err(|error| Error::InvalidMultiliteOp(error.to_string()))?;
        for (key, owner) in &before_unique {
            if after_unique.get(key) != Some(owner) {
                footprint.add_write(key.clone());
                mutations.push(Mutation::Delete { key: key.clone() });
            }
        }
        for (row, (_, key)) in self.after.rows.iter().zip(keys) {
            mutations.push(Mutation::Set {
                key,
                value: self.after.encode_row(row),
            });
        }
        for (key, owner) in &after_unique {
            if before_unique.get(key) != Some(owner) {
                footprint.add_write(key.clone());
                mutations.push(Mutation::Set {
                    key: key.clone(),
                    value: owner.clone(),
                });
            }
        }
        footprint.add_constraint(active_row_keyspace_key(self.after.table));
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
        self.validate_against_catalog(connection)?;
        self.before
            .delete_materialized_with(connection, "UPDATE row no longer matches SQLite state")?;
        self.after.apply(connection)
    }

    pub fn restore_materialized(&self, connection: &Connection) -> Result<()> {
        self.validate_against_catalog(connection)?;
        self.after.delete_materialized_with(
            connection,
            "pending UPDATE row no longer matches SQLite state",
        )?;
        self.before.apply(connection)
    }

    fn validate_against_catalog(&self, connection: &Connection) -> Result<()> {
        let before = self.before.catalog_definition(connection)?;
        let after = self.after.catalog_definition(connection)?;
        if before != after {
            return Err(Error::InvalidMultiliteOp(
                "UPDATE before and after rows use different schemas".into(),
            ));
        }
        Ok(())
    }

    fn validate_structure(&self) -> std::result::Result<(), RowCodecError> {
        self.before.validate_structure()?;
        self.after.validate_structure()?;
        if self.before.table != self.after.table
            || self.before.schema_revision != self.after.schema_revision
            || self.before.row_keyspace != self.after.row_keyspace
            || self.before.key_parts != self.after.key_parts
            || self.before.unique_keys != self.after.unique_keys
            || self.before.rows.len() != self.after.rows.len()
        {
            return Err(RowCodecError::InvalidRow);
        }
        let integer_primary_key = matches!(
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

/// Prefix covering every row encoded under a table's active row keyspace.
#[cfg(test)]
pub fn row_keyspace_prefix(created: &CreateTable) -> Key {
    row_prefix(created.table_id(), created.row_keyspace_id(), Vec::new())
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
    let images = primary
        .into_iter()
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
                    rowid_alias: column.is_rowid_alias(),
                },
            )
        })
        .collect::<std::result::Result<Vec<_>, _>>()?;
    row_prefix(created.table_id(), created.row_keyspace_id(), images)
}

fn unique_key_rules(created: &CreateTable) -> Vec<UniqueKeyRules> {
    created
        .unique_constraints()
        .iter()
        .map(|unique| UniqueKeyRules {
            keyspace: unique.keyspace_id(),
            key_parts: unique
                .columns()
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
                .collect(),
        })
        .collect()
}

fn row_prefix(
    table: TableId,
    row_keyspace: RowKeyspaceId,
    images: Vec<Vec<u8>>,
) -> std::result::Result<Key, RowCodecError> {
    Key::from_bytes(
        [
            codes::ROOT.to_vec(),
            codes::TABLES.to_vec(),
            table.as_bytes().to_vec(),
            codes::ROWS.to_vec(),
            row_keyspace.as_bytes().to_vec(),
        ]
        .into_iter()
        .chain(images),
    )
    .map_err(RowCodecError::InvalidKey)
}

fn unique_prefix(
    table: TableId,
    keyspace: UniqueKeyspaceId,
    images: Vec<Vec<u8>>,
) -> std::result::Result<Key, RowCodecError> {
    Key::from_bytes(
        [
            codes::ROOT.to_vec(),
            codes::TABLES.to_vec(),
            table.as_bytes().to_vec(),
            codes::UNIQUE.to_vec(),
            keyspace.as_bytes().to_vec(),
        ]
        .into_iter()
        .chain(images),
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
        RowKeyspaceId,
        Vec<KeyPartRules>,
        Vec<UniqueKeyRules>,
        Row,
    ),
    RowCodecError,
> {
    let mut reader = Reader::new(frame);
    if reader.u8() != Some(ROW_FRAME_VERSION) {
        return Err(RowCodecError::UnknownVersion);
    }
    let mut schema_revision = None;
    let mut row_keyspace = None;
    let mut rowid = None;
    let mut key_parts = Vec::new();
    let mut unique_keys = Vec::new();
    let mut values = Vec::new();
    while let Some((tag, value)) = reader.field().map_err(|_| RowCodecError::Truncated)? {
        match tag {
            TAG_SCHEMA_REVISION => set_once(
                &mut schema_revision,
                SchemaRevisionId::from_bytes(uuid_bytes(value)?),
            )?,
            TAG_ROW_KEYSPACE => set_once(
                &mut row_keyspace,
                RowKeyspaceId::from_bytes(uuid_bytes(value)?),
            )?,
            TAG_ROWID => {
                let bytes = value.try_into().map_err(|_| RowCodecError::InvalidLength)?;
                set_once(&mut rowid, i64::from_be_bytes(bytes))?;
            }
            TAG_KEY_PART => key_parts.push(decode_key_part(value)?),
            TAG_UNIQUE_KEY => unique_keys.push(decode_unique_key(value)?),
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
    Ok((
        schema_revision.ok_or(RowCodecError::MissingField(TAG_SCHEMA_REVISION))?,
        row_keyspace.ok_or(RowCodecError::MissingField(TAG_ROW_KEYSPACE))?,
        key_parts,
        unique_keys,
        Row {
            rowid: rowid.ok_or(RowCodecError::MissingField(TAG_ROWID))?,
            values,
        },
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
    let table = quote_identifier(created.table_name());
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
    let primary = created.primary_key_columns().collect::<Vec<_>>();
    if primary.len() == 1 && primary[0].is_rowid_alias() {
        return Ok(None);
    }
    ["_rowid_", "rowid", "oid"]
        .into_iter()
        .find(|candidate| {
            created
                .columns()
                .iter()
                .all(|column| !column.name().value().eq_ignore_ascii_case(candidate))
        })
        .map(Some)
        .ok_or(Error::UnsupportedSql(
            "tables with a non-integer primary key must leave one SQLite rowid alias unshadowed",
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
    use crate::database::schema::{
        CreateColumn, CreateTableSpec, CreateUnique, SqlName, TypeDeclaration,
    };

    fn definition() -> CreateTable {
        CreateTable::new(
            "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT, payload BLOB)",
            CreateTableSpec {
                name: SqlName::new("notes".into()),
                mode: Default::default(),
                columns: vec![
                    CreateColumn {
                        name: SqlName::new("id".into()),
                        declared_type: TypeDeclaration::integer(),
                        not_null: false,
                        primary_key: true,
                    },
                    CreateColumn {
                        name: SqlName::new("body".into()),
                        declared_type: TypeDeclaration::text(),
                        not_null: false,
                        primary_key: false,
                    },
                    CreateColumn {
                        name: SqlName::new("payload".into()),
                        declared_type: TypeDeclaration::blob(),
                        not_null: false,
                        primary_key: false,
                    },
                ],
                unique_constraints: Vec::new(),
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
                columns: vec![
                    CreateColumn {
                        name: SqlName::new("id".into()),
                        declared_type: TypeDeclaration::integer(),
                        not_null: false,
                        primary_key: true,
                    },
                    CreateColumn {
                        name: SqlName::new("organization".into()),
                        declared_type: TypeDeclaration::text(),
                        not_null: false,
                        primary_key: false,
                    },
                    CreateColumn {
                        name: SqlName::new("email".into()),
                        declared_type: TypeDeclaration::text(),
                        not_null: false,
                        primary_key: false,
                    },
                ],
                unique_constraints: vec![CreateUnique {
                    name: None,
                    columns: vec![
                        SqlName::new("organization".into()),
                        SqlName::new("email".into()),
                    ],
                }],
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
                columns: vec![
                    CreateColumn {
                        name: SqlName::new("id".into()),
                        declared_type: TypeDeclaration::integer(),
                        not_null: false,
                        primary_key: true,
                    },
                    CreateColumn {
                        name: SqlName::new("tenant".into()),
                        declared_type: TypeDeclaration::text(),
                        not_null: false,
                        primary_key: false,
                    },
                    CreateColumn {
                        name: SqlName::new("email".into()),
                        declared_type: TypeDeclaration::text(),
                        not_null: false,
                        primary_key: false,
                    },
                    CreateColumn {
                        name: SqlName::new("username".into()),
                        declared_type: TypeDeclaration::text(),
                        not_null: false,
                        primary_key: false,
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
        assert_eq!(lowered.footprint.constraints().len(), 2);
        assert_eq!(
            lowered.mutations[0].key(),
            &primary_key_prefix(&created, &[StoredValue::Integer(7)]).unwrap()
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
    fn composite_unique_keys_lower_per_part_and_skip_null_tuples() {
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
    fn overlapping_unique_keyspaces_lower_and_delete_independently() {
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
    fn one_insert_operation_rejects_duplicates_in_any_unique_keyspace() {
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
        assert_eq!(
            lowered
                .mutations
                .iter()
                .filter(|mutation| mutation.key().components()[3].as_bytes() == codes::UNIQUE)
                .count(),
            2
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
    fn update_changes_only_the_affected_overlapping_unique_keyspaces() {
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
                    .keyspace_id()
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
                columns: vec![
                    CreateColumn {
                        name: SqlName::new("id".into()),
                        declared_type: TypeDeclaration::text(),
                        not_null: true,
                        primary_key: true,
                    },
                    CreateColumn {
                        name: SqlName::new("body".into()),
                        declared_type: TypeDeclaration::text(),
                        not_null: false,
                        primary_key: false,
                    },
                ],
                unique_constraints: Vec::new(),
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
        assert_eq!(
            inserted.unique_keys[0].key_parts[0].affinity,
            Affinity::Blob
        );
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
            invalid.validate_against(&created),
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
                columns: vec![
                    CreateColumn {
                        name: SqlName::new("id".into()),
                        declared_type: TypeDeclaration::text(),
                        not_null: true,
                        primary_key: true,
                    },
                    CreateColumn {
                        name: SqlName::new("body".into()),
                        declared_type: TypeDeclaration::text(),
                        not_null: false,
                        primary_key: false,
                    },
                ],
                unique_constraints: Vec::new(),
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
