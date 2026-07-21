//! Captured SQLite rows and their durable Homebase representation.

use std::fmt;

use homebase_core::key::{Key, KeyError};
#[cfg(test)]
use homebase_core::messages::AdmittedBatch;
use homebase_core::reader::Reader;
use homebase_core::tag::Mutation;
use homebase_core::writer::Writer;
use rusqlite::types::{ToSqlOutput, ValueRef};
use rusqlite::{Connection, ToSql, params_from_iter};
use uuid::{Uuid, Variant, Version};

use super::isolation::ConflictFootprint;
use super::schema::{
    ColumnId, CreateTable, DeclaredType, RowKeyspaceId, SchemaRevisionId, TableId,
    active_row_keyspace_key, write_revision_key,
};
use super::{catalog, codes};
use crate::{Error, Result};

const ROW_FRAME_VERSION: u8 = 1;
const INSERT_FRAME_VERSION: u8 = 1;
const TAG_SCHEMA_REVISION: u8 = 1;
const TAG_ROW_KEYSPACE: u8 = 2;
const TAG_KEY_PART: u8 = 3;
const TAG_COLUMN_VALUE: u8 = 4;
const TAG_TABLE: u8 = 1;
const TAG_ROW: u8 = 2;
const TAG_COLUMN_ID: u8 = 1;
const TAG_COLUMN_TYPE: u8 = 2;
const TAG_VALUE: u8 = 2;

/// One final SQLite row observed after affinity and generated values ran.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CapturedRow {
    pub table: String,
    pub values: Vec<StoredValue>,
}

/// Lossless SQLite storage-class value used by row frames.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StoredValue {
    Null,
    Integer(i64),
    Real(u64),
    Text(Vec<u8>),
    Blob(Vec<u8>),
}

impl StoredValue {
    pub fn capture(value: ValueRef<'_>) -> Self {
        match value {
            ValueRef::Null => Self::Null,
            ValueRef::Integer(value) => Self::Integer(value),
            ValueRef::Real(value) => Self::Real(value.to_bits()),
            ValueRef::Text(value) => Self::Text(value.to_vec()),
            ValueRef::Blob(value) => Self::Blob(value.to_vec()),
        }
    }

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

impl ToSql for StoredValue {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        Ok(ToSqlOutput::Borrowed(match self {
            Self::Null => ValueRef::Null,
            Self::Integer(value) => ValueRef::Integer(*value),
            Self::Real(bits) => ValueRef::Real(f64::from_bits(*bits)),
            Self::Text(value) => ValueRef::Text(value),
            Self::Blob(value) => ValueRef::Blob(value),
        }))
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct KeyPartRules {
    pub column: ColumnId,
    pub declared_type: DeclaredType,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Row {
    values: Vec<(ColumnId, StoredValue)>,
}

/// One logical multi-row INSERT captured from a single SQLite statement.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InsertRows {
    table: TableId,
    schema_revision: SchemaRevisionId,
    row_keyspace: RowKeyspaceId,
    key_parts: Vec<KeyPartRules>,
    rows: Vec<Row>,
}

/// Homebase mutations and conflict footprint for one row insertion.
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
                "one INSERT statement changed more than one table",
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
                declared_type: column.declared_type(),
            })
            .collect::<Vec<_>>();
        let rows = captured
            .iter()
            .map(|captured| Row {
                values: columns
                    .iter()
                    .zip(&captured.values)
                    .map(|(column, value)| (column.id(), value.clone()))
                    .collect(),
            })
            .collect();
        let inserted = Self {
            table: created.table_id(),
            schema_revision: created.schema_revision_id(),
            row_keyspace: created.row_keyspace_id(),
            key_parts,
            rows,
        };
        inserted.validate_against(&created)?;
        Ok(Some(inserted))
    }

    pub fn to_homebase(&self) -> Result<RowHomebaseOp> {
        let mut mutations = Vec::with_capacity(self.rows.len());
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
            if components.len() < 6
                || components[0].as_bytes() != codes::ROOT
                || components[1].as_bytes() != codes::TABLES
                || components[3].as_bytes() != codes::ROWS
            {
                return Err(RowCodecError::InvalidBatch);
            }
            let table = TableId::from_bytes(uuid_bytes(components[2].as_bytes())?);
            let row_keyspace = RowKeyspaceId::from_bytes(uuid_bytes(components[4].as_bytes())?);
            let (schema_revision, encoded_keyspace, key_parts, row) = decode_row(value)?;
            if encoded_keyspace != row_keyspace {
                return Err(RowCodecError::InvalidBatch);
            }
            let candidate = operation.get_or_insert_with(|| Self {
                table,
                schema_revision,
                row_keyspace,
                key_parts: key_parts.clone(),
                rows: Vec::new(),
            });
            if candidate.table != table
                || candidate.schema_revision != schema_revision
                || candidate.row_keyspace != row_keyspace
                || candidate.key_parts != key_parts
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
        operation.ok_or(RowCodecError::InvalidBatch)
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut writer = Writer::new();
        writer.u8(INSERT_FRAME_VERSION);
        put_field(&mut writer, TAG_TABLE, &self.table.as_bytes());
        for row in &self.rows {
            put_field(&mut writer, TAG_ROW, &self.encode_row(row));
        }
        writer.finish()
    }

    pub fn decode(frame: &[u8]) -> std::result::Result<Self, RowCodecError> {
        let mut reader = Reader::new(frame);
        if reader.u8() != Some(INSERT_FRAME_VERSION) {
            return Err(RowCodecError::UnknownVersion);
        }
        let mut table = None;
        let mut operation = None::<Self>;
        while let Some((tag, value)) = next_field(&mut reader)? {
            match tag {
                TAG_TABLE => set_once(&mut table, TableId::from_bytes(uuid_bytes(value)?))?,
                TAG_ROW => {
                    let table = table.ok_or(RowCodecError::MissingField(TAG_TABLE))?;
                    let (schema_revision, row_keyspace, key_parts, row) = decode_row(value)?;
                    let candidate = operation.get_or_insert_with(|| Self {
                        table,
                        schema_revision,
                        row_keyspace,
                        key_parts: key_parts.clone(),
                        rows: Vec::new(),
                    });
                    if candidate.table != table
                        || candidate.schema_revision != schema_revision
                        || candidate.row_keyspace != row_keyspace
                        || candidate.key_parts != key_parts
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
        let names = columns
            .iter()
            .map(|column| quote_identifier(column.name().value()))
            .collect::<Vec<_>>()
            .join(", ");
        let placeholders = std::iter::repeat_n("?", columns.len())
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
            statement.execute(params_from_iter(values))?;
        }
        Ok(())
    }

    pub fn delete_materialized(&self, connection: &Connection) -> Result<()> {
        let created = self.catalog_definition(connection)?;
        let primary = created.primary_key_columns().collect::<Vec<_>>();
        let predicate = primary
            .iter()
            .map(|column| format!("{} = ?", quote_identifier(column.name().value())))
            .collect::<Vec<_>>()
            .join(" AND ");
        let sql = format!(
            "DELETE FROM {} WHERE {predicate}",
            quote_identifier(created.table_name())
        );
        let mut statement = connection.prepare(&sql)?;
        for row in self.rows.iter().rev() {
            statement.execute(params_from_iter(self.primary_values(row)?))?;
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
        let expected_key_parts = created
            .primary_key_columns()
            .map(|column| KeyPartRules {
                column: column.id(),
                declared_type: column.declared_type(),
            })
            .collect::<Vec<_>>();
        if self.table != created.table_id()
            || self.schema_revision != created.schema_revision_id()
            || self.row_keyspace != created.row_keyspace_id()
            || self.key_parts != expected_key_parts
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
            self.key_images(row).map_err(|error| {
                Error::InvalidMultiliteOp(format!("invalid primary key image: {error}"))
            })?;
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

    fn encode_row(&self, row: &Row) -> Vec<u8> {
        let mut writer = Writer::new();
        writer.u8(ROW_FRAME_VERSION);
        put_field(
            &mut writer,
            TAG_SCHEMA_REVISION,
            &self.schema_revision.as_bytes(),
        );
        put_field(&mut writer, TAG_ROW_KEYSPACE, &self.row_keyspace.as_bytes());
        for part in &self.key_parts {
            put_field(&mut writer, TAG_KEY_PART, &encode_key_part(*part));
        }
        for (column, value) in &row.values {
            put_field(
                &mut writer,
                TAG_COLUMN_VALUE,
                &encode_column_value(*column, value),
            );
        }
        writer.finish()
    }
}

/// Prefix covering every row encoded under a table's active row keyspace.
pub fn row_keyspace_prefix(created: &CreateTable) -> Key {
    row_prefix(created.table_id(), created.row_keyspace_id(), Vec::new())
        .expect("table row prefix is bounded and non-empty")
}

/// Exact row prefix produced by one complete primary-key value tuple.
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
                (column.declared_type(), value),
                (
                    DeclaredType::Integer | DeclaredType::Real,
                    StoredValue::Text(_)
                )
            ) {
                return Err(RowCodecError::InvalidRow);
            }
            key_image(
                value,
                KeyPartRules {
                    column: column.id(),
                    declared_type: column.declared_type(),
                },
            )
        })
        .collect::<std::result::Result<Vec<_>, _>>()?;
    row_prefix(created.table_id(), created.row_keyspace_id(), images)
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

fn key_image(
    value: &StoredValue,
    rules: KeyPartRules,
) -> std::result::Result<Vec<u8>, RowCodecError> {
    if rules.declared_type == DeclaredType::Text
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

fn encode_key_part(part: KeyPartRules) -> Vec<u8> {
    let mut writer = Writer::new();
    put_field(&mut writer, TAG_COLUMN_ID, &part.column.as_bytes());
    put_field(&mut writer, TAG_COLUMN_TYPE, &[part.declared_type.to_u8()]);
    writer.finish()
}

fn decode_key_part(frame: &[u8]) -> std::result::Result<KeyPartRules, RowCodecError> {
    let mut reader = Reader::new(frame);
    let mut column = None;
    let mut declared_type = None;
    while let Some((tag, value)) = next_field(&mut reader)? {
        match tag {
            TAG_COLUMN_ID => set_once(&mut column, ColumnId::from_bytes(uuid_bytes(value)?))?,
            TAG_COLUMN_TYPE => {
                let [value] = value else {
                    return Err(RowCodecError::InvalidLength);
                };
                set_once(
                    &mut declared_type,
                    DeclaredType::from_u8(*value).map_err(|_| RowCodecError::InvalidRow)?,
                )?;
            }
            _ => {}
        }
    }
    Ok(KeyPartRules {
        column: column.ok_or(RowCodecError::MissingField(TAG_COLUMN_ID))?,
        declared_type: declared_type.ok_or(RowCodecError::MissingField(TAG_COLUMN_TYPE))?,
    })
}

fn encode_column_value(column: ColumnId, value: &StoredValue) -> Vec<u8> {
    let mut writer = Writer::new();
    put_field(&mut writer, TAG_COLUMN_ID, &column.as_bytes());
    put_field(&mut writer, TAG_VALUE, &value.encode());
    writer.finish()
}

fn decode_column_value(
    frame: &[u8],
) -> std::result::Result<(ColumnId, StoredValue), RowCodecError> {
    let mut reader = Reader::new(frame);
    let mut column = None;
    let mut value = None;
    while let Some((tag, bytes)) = next_field(&mut reader)? {
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
) -> std::result::Result<(SchemaRevisionId, RowKeyspaceId, Vec<KeyPartRules>, Row), RowCodecError> {
    let mut reader = Reader::new(frame);
    if reader.u8() != Some(ROW_FRAME_VERSION) {
        return Err(RowCodecError::UnknownVersion);
    }
    let mut schema_revision = None;
    let mut row_keyspace = None;
    let mut key_parts = Vec::new();
    let mut values = Vec::new();
    while let Some((tag, value)) = next_field(&mut reader)? {
        match tag {
            TAG_SCHEMA_REVISION => set_once(
                &mut schema_revision,
                SchemaRevisionId::from_bytes(uuid_bytes(value)?),
            )?,
            TAG_ROW_KEYSPACE => set_once(
                &mut row_keyspace,
                RowKeyspaceId::from_bytes(uuid_bytes(value)?),
            )?,
            TAG_KEY_PART => key_parts.push(decode_key_part(value)?),
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
        Row { values },
    ))
}

fn quote_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

fn put_field(writer: &mut Writer, tag: u8, value: &[u8]) {
    let len = u32::try_from(value.len()).expect("row field length fits in u32");
    writer.u8(tag);
    writer.u32(len);
    writer.bytes(value);
}

fn next_field<'a>(
    reader: &mut Reader<'a>,
) -> std::result::Result<Option<(u8, &'a [u8])>, RowCodecError> {
    if reader.end().is_some() {
        return Ok(None);
    }
    let tag = reader.u8().ok_or(RowCodecError::Truncated)?;
    let len = reader.u32().ok_or(RowCodecError::Truncated)?;
    let len = usize::try_from(len).map_err(|_| RowCodecError::InvalidLength)?;
    let value = reader.take(len).ok_or(RowCodecError::Truncated)?;
    Ok(Some((tag, value)))
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
    use crate::database::schema::{CreateColumn, CreateTableSpec, SqlName};

    fn definition() -> CreateTable {
        CreateTable::new(
            "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT, payload BLOB)",
            CreateTableSpec {
                name: SqlName::new("notes".into()),
                columns: vec![
                    CreateColumn {
                        name: SqlName::new("id".into()),
                        declared_type: DeclaredType::Integer,
                        not_null: false,
                        primary_key: true,
                    },
                    CreateColumn {
                        name: SqlName::new("body".into()),
                        declared_type: DeclaredType::Text,
                        not_null: false,
                        primary_key: false,
                    },
                    CreateColumn {
                        name: SqlName::new("payload".into()),
                        declared_type: DeclaredType::Blob,
                        not_null: false,
                        primary_key: false,
                    },
                ],
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

    fn inserted(connection: &Connection) -> InsertRows {
        InsertRows::from_captured(
            connection,
            &[
                CapturedRow {
                    table: "notes".into(),
                    values: vec![
                        StoredValue::Integer(7),
                        StoredValue::Text(b"hello".to_vec()),
                        StoredValue::Blob(vec![0, 1]),
                    ],
                },
                CapturedRow {
                    table: "notes".into(),
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
            declared_type: DeclaredType::Blob,
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
