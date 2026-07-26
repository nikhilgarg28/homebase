//! Durable schema identities, codecs, and Homebase coordination keys.
//!
//! A table creation lowers to an immutable UUID-keyed schema log entry plus
//! mutable revision cells. It can be reconstructed only from a complete,
//! self-consistent admitted envelope.

use std::fmt;

use homebase_core::key::Key;
#[cfg(test)]
use homebase_core::messages::AdmittedBatch;
use homebase_core::tag::Mutation;
use homebase_core::writer::Writer;
use sha2::{Digest, Sha256};
use uuid::{Uuid, Variant, Version};

use super::codes;
use crate::commit::footprint::ConflictFootprint;

const SCHEMA_FRAME_VERSION: u8 = 1;
const TAG_MUTATION_ID: u8 = 1;
const TAG_SQL: u8 = 2;
const TAG_CREATE_TABLE: u8 = 10;
const TAG_TABLE_ID: u8 = 1;
const TAG_TABLE_NAME: u8 = 2;
const TAG_COLUMN: u8 = 3;
const TAG_SCHEMA_REVISION_ID: u8 = 4;
const TAG_ROW_KEYSPACE_ID: u8 = 5;
const TAG_UNIQUE_CONSTRAINT: u8 = 6;
const TAG_COLUMN_ID: u8 = 1;
const TAG_COLUMN_NAME: u8 = 2;
const TAG_COLUMN_TYPE: u8 = 3;
const TAG_COLUMN_FLAGS: u8 = 4;
const TAG_UNIQUE_KEYSPACE_ID: u8 = 1;
const TAG_UNIQUE_NAME: u8 = 2;
const TAG_UNIQUE_COLUMN_ID: u8 = 3;
const COLUMN_NOT_NULL: u8 = 1;
const COLUMN_PRIMARY_KEY: u8 = 2;

const SHORT_NAME_LIMIT: usize = 250;
const TABLE_NAME_HASH_DOMAIN: &[u8] = b"multilite:table-name:v1\0";

/// SQLite identifier spelling plus its case-insensitive identity form.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SqlName {
    value: String,
    canonical: Vec<u8>,
}

impl SqlName {
    pub fn new(value: String) -> Self {
        let mut canonical = value.as_bytes().to_vec();
        canonical.make_ascii_lowercase();
        Self { value, canonical }
    }

    pub fn value(&self) -> &str {
        &self.value
    }

    pub fn canonical(&self) -> &[u8] {
        &self.canonical
    }
}

/// Declared SQL type accepted by the initial schema format.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DeclaredType {
    Integer,
    Real,
    Text,
    Blob,
}

impl DeclaredType {
    pub fn to_u8(self) -> u8 {
        match self {
            Self::Integer => 1,
            Self::Real => 2,
            Self::Text => 3,
            Self::Blob => 4,
        }
    }

    pub fn from_u8(value: u8) -> std::result::Result<Self, SchemaCodecError> {
        match value {
            1 => Ok(Self::Integer),
            2 => Ok(Self::Real),
            3 => Ok(Self::Text),
            4 => Ok(Self::Blob),
            _ => Err(SchemaCodecError::InvalidColumnType(value)),
        }
    }
}

/// One validated column in a restricted `CREATE TABLE` statement.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateColumn {
    pub name: SqlName,
    pub declared_type: DeclaredType,
    pub not_null: bool,
    pub primary_key: bool,
}

/// One validated inline or table-level UNIQUE declaration.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateUnique {
    pub name: Option<SqlName>,
    pub columns: Vec<SqlName>,
}

/// Structured result of validating a restricted `CREATE TABLE` statement.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateTableSpec {
    pub name: SqlName,
    pub columns: Vec<CreateColumn>,
    pub unique_constraints: Vec<CreateUnique>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MutationId([u8; 16]);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TableId([u8; 16]);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SchemaRevisionId([u8; 16]);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RowKeyspaceId([u8; 16]);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct UniqueKeyspaceId([u8; 16]);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ColumnId([u8; 16]);

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Column {
    id: ColumnId,
    name: SqlName,
    declared_type: DeclaredType,
    not_null: bool,
    primary_key: bool,
}

/// One durable UNIQUE key definition owned by a table schema revision.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UniqueConstraint {
    keyspace_id: UniqueKeyspaceId,
    name: Option<SqlName>,
    columns: Vec<ColumnId>,
}

/// Durable meaning of a restricted table creation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateTable {
    mutation_id: MutationId,
    sql: String,
    table_id: TableId,
    schema_revision_id: SchemaRevisionId,
    row_keyspace_id: RowKeyspaceId,
    name: SqlName,
    columns: Vec<Column>,
    unique_constraints: Vec<UniqueConstraint>,
}

/// Homebase mutations and conflict footprint for one schema change.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SchemaHomebaseOp {
    pub mutations: Vec<Mutation>,
    pub footprint: ConflictFootprint,
}

macro_rules! id_accessors {
    ($type:ty) => {
        impl $type {
            pub fn from_bytes(bytes: [u8; 16]) -> Self {
                Self(bytes)
            }

            pub fn as_bytes(self) -> [u8; 16] {
                self.0
            }
        }
    };
}

id_accessors!(TableId);
id_accessors!(SchemaRevisionId);
id_accessors!(RowKeyspaceId);
id_accessors!(UniqueKeyspaceId);
id_accessors!(ColumnId);

impl Column {
    pub fn id(&self) -> ColumnId {
        self.id
    }

    pub fn name(&self) -> &SqlName {
        &self.name
    }

    pub fn declared_type(&self) -> DeclaredType {
        self.declared_type
    }
}

impl UniqueConstraint {
    pub fn keyspace_id(&self) -> UniqueKeyspaceId {
        self.keyspace_id
    }

    pub fn columns(&self) -> &[ColumnId] {
        &self.columns
    }
}

impl CreateTable {
    /// Mint durable identities for one validated table creation.
    pub fn new(sql: &str, spec: CreateTableSpec) -> Self {
        build_create_table(sql, spec, || Uuid::new_v4().into_bytes())
    }

    /// Lower this schema change to its complete Homebase representation.
    pub fn to_homebase(&self) -> SchemaHomebaseOp {
        let log = log_key(self.mutation_id);
        let name_scope = table_name_scope_key(&self.name);
        let schema = table_schema_key(self.table_id, self.schema_revision_id);
        let active_row_keyspace = active_row_keyspace_key(self.table_id);
        let row_keyspace = row_keyspace_key(self.table_id, self.row_keyspace_id);
        let write_revision = write_revision_key(self.table_id);
        let mut footprint = ConflictFootprint::new();
        footprint.add_constraint(name_scope.clone());
        footprint.add_write(write_revision.clone());
        let mut mutations = vec![
            Mutation::Set {
                key: log,
                value: self.encode(),
            },
            Mutation::Set {
                key: name_scope.clone(),
                value: self.table_id.0.to_vec(),
            },
            Mutation::Set {
                key: schema,
                value: self.encode(),
            },
            Mutation::Set {
                key: active_row_keyspace,
                value: self.row_keyspace_id.0.to_vec(),
            },
            Mutation::Set {
                key: row_keyspace,
                value: encode_row_keyspace(self),
            },
        ];
        mutations.extend(self.unique_constraints.iter().map(|unique| Mutation::Set {
            key: unique_keyspace_key(self.table_id, unique.keyspace_id),
            value: encode_unique_constraint(unique),
        }));
        mutations.push(Mutation::Set {
            key: write_revision.clone(),
            value: self.mutation_id.0.to_vec(),
        });
        SchemaHomebaseOp {
            mutations,
            footprint,
        }
    }

    /// Raise one complete authenticated Homebase batch into a schema change.
    #[cfg(test)]
    pub fn from_homebase(
        batch: &AdmittedBatch<Vec<u8>>,
    ) -> std::result::Result<Self, SchemaCodecError> {
        from_homebase_inner(batch)
    }

    /// Encode this complete schema operation for local durable state.
    pub fn encode(&self) -> Vec<u8> {
        let mut writer = Writer::new();
        writer.u8(SCHEMA_FRAME_VERSION);
        writer
            .field(TAG_MUTATION_ID, &self.mutation_id.0)
            .expect("schema field length must fit in u32");
        writer
            .field(TAG_SQL, self.sql.as_bytes())
            .expect("schema field length must fit in u32");
        writer
            .field(TAG_CREATE_TABLE, &encode_create_table(self))
            .expect("schema field length must fit in u32");
        writer.finish()
    }

    /// Decode and validate one complete locally stored schema operation.
    pub fn decode(frame: &[u8]) -> std::result::Result<Self, SchemaCodecError> {
        let created = decode_frame(frame)?;
        validate_literal_sql(&created)?;
        Ok(created)
    }

    /// Return the exact SQLite spelling of the created table name.
    pub fn table_name(&self) -> &str {
        self.name.value()
    }

    /// Return the validated SQL used to materialize this table.
    pub fn sql(&self) -> &str {
        &self.sql
    }

    pub fn table_id(&self) -> TableId {
        self.table_id
    }

    pub fn schema_revision_id(&self) -> SchemaRevisionId {
        self.schema_revision_id
    }

    pub fn row_keyspace_id(&self) -> RowKeyspaceId {
        self.row_keyspace_id
    }

    pub fn table_name_identity(&self) -> &SqlName {
        &self.name
    }

    pub fn columns(&self) -> &[Column] {
        &self.columns
    }

    pub fn unique_constraints(&self) -> &[UniqueConstraint] {
        &self.unique_constraints
    }

    pub fn primary_key_columns(&self) -> impl Iterator<Item = &Column> {
        self.columns.iter().filter(|column| column.primary_key)
    }

    fn matches_spec(&self, spec: &CreateTableSpec) -> bool {
        self.name == spec.name
            && self.columns.len() == spec.columns.len()
            && self
                .columns
                .iter()
                .zip(&spec.columns)
                .all(|(encoded, parsed)| {
                    encoded.name == parsed.name
                        && encoded.declared_type == parsed.declared_type
                        && encoded.not_null == parsed.not_null
                        && encoded.primary_key == parsed.primary_key
                })
            && self.unique_constraints.len() == spec.unique_constraints.len()
            && self
                .unique_constraints
                .iter()
                .zip(&spec.unique_constraints)
                .all(|(encoded, parsed)| {
                    encoded.name == parsed.name
                        && encoded.columns.len() == parsed.columns.len()
                        && encoded.columns.iter().zip(&parsed.columns).all(
                            |(encoded_column, parsed_column)| {
                                self.columns.iter().any(|column| {
                                    column.id == *encoded_column
                                        && column.name.canonical() == parsed_column.canonical()
                                })
                            },
                        )
                })
    }
}

fn build_create_table(
    sql: &str,
    spec: CreateTableSpec,
    mut mint: impl FnMut() -> [u8; 16],
) -> CreateTable {
    let CreateTableSpec {
        name,
        columns: column_specs,
        unique_constraints: unique_specs,
    } = spec;
    let mutation_id = MutationId(mint());
    let table_id = TableId(mint());
    let schema_revision_id = SchemaRevisionId(mint());
    let row_keyspace_id = RowKeyspaceId(mint());
    let columns = column_specs
        .into_iter()
        .map(|column| Column {
            id: ColumnId(mint()),
            name: column.name,
            declared_type: column.declared_type,
            not_null: column.not_null,
            primary_key: column.primary_key,
        })
        .collect::<Vec<_>>();
    let unique_constraints = unique_specs
        .into_iter()
        .map(|unique| UniqueConstraint {
            keyspace_id: UniqueKeyspaceId(mint()),
            name: unique.name,
            columns: unique
                .columns
                .into_iter()
                .map(|name| {
                    columns
                        .iter()
                        .find(|column| column.name.canonical() == name.canonical())
                        .expect("validated UNIQUE column exists")
                        .id
                })
                .collect(),
        })
        .collect();
    CreateTable {
        mutation_id,
        sql: sql.to_owned(),
        table_id,
        schema_revision_id,
        row_keyspace_id,
        name,
        columns,
        unique_constraints,
    }
}

fn log_key(id: MutationId) -> Key {
    Key::from_bytes([codes::ROOT, codes::SCHEMA, codes::LOG, id.0.as_slice()])
        .expect("schema log components are bounded and non-empty")
}

fn table_name_scope_key(name: &SqlName) -> Key {
    let component = name_component(name.canonical());
    Key::from_bytes([
        codes::ROOT,
        codes::SCHEMA,
        codes::NAMES,
        codes::TABLES,
        codes::MAIN,
        component.as_slice(),
    ])
    .expect("table-name scope components are bounded and non-empty")
}

fn table_schema_key(table: TableId, revision: SchemaRevisionId) -> Key {
    Key::from_bytes([
        codes::ROOT,
        codes::TABLES,
        table.0.as_slice(),
        codes::SCHEMA,
        revision.0.as_slice(),
    ])
    .expect("table schema key is bounded")
}

/// Prefix covering every durable schema and row cell owned by one table.
pub fn table_prefix(table: TableId) -> Key {
    Key::from_bytes([codes::ROOT, codes::TABLES, table.0.as_slice()])
        .expect("table prefix is bounded")
}

pub fn active_row_keyspace_key(table: TableId) -> Key {
    Key::from_bytes([
        codes::ROOT,
        codes::TABLES,
        table.0.as_slice(),
        codes::ACTIVE_ROW_KEYSPACE,
    ])
    .expect("active row keyspace key is bounded")
}

fn row_keyspace_key(table: TableId, keyspace: RowKeyspaceId) -> Key {
    Key::from_bytes([
        codes::ROOT,
        codes::TABLES,
        table.0.as_slice(),
        codes::ROW_KEYSPACES,
        keyspace.0.as_slice(),
    ])
    .expect("row keyspace key is bounded")
}

fn unique_keyspace_key(table: TableId, keyspace: UniqueKeyspaceId) -> Key {
    Key::from_bytes([
        codes::ROOT,
        codes::TABLES,
        table.0.as_slice(),
        codes::UNIQUE_KEYSPACES,
        keyspace.0.as_slice(),
    ])
    .expect("unique keyspace key is bounded")
}

pub fn write_revision_key(table: TableId) -> Key {
    Key::from_bytes([
        codes::ROOT,
        codes::TABLES,
        table.0.as_slice(),
        codes::WRITE_REVISION,
    ])
    .expect("write revision key is bounded")
}

fn name_component(canonical: &[u8]) -> Vec<u8> {
    if canonical.len() <= SHORT_NAME_LIMIT {
        let mut component = Vec::with_capacity(5 + canonical.len());
        component.extend_from_slice(b"name-");
        component.extend_from_slice(canonical);
        component
    } else {
        let mut hash = Sha256::new();
        hash.update(TABLE_NAME_HASH_DOMAIN);
        hash.update(canonical);
        let mut component = Vec::with_capacity(5 + 32);
        component.extend_from_slice(b"hash-");
        component.extend_from_slice(&hash.finalize());
        component
    }
}

fn encode_create_table(table: &CreateTable) -> Vec<u8> {
    let mut writer = Writer::new();
    writer
        .field(TAG_TABLE_ID, &table.table_id.0)
        .expect("schema field length must fit in u32");
    writer
        .field(TAG_TABLE_NAME, table.name.value().as_bytes())
        .expect("schema field length must fit in u32");
    writer
        .field(TAG_SCHEMA_REVISION_ID, &table.schema_revision_id.0)
        .expect("schema field length must fit in u32");
    writer
        .field(TAG_ROW_KEYSPACE_ID, &table.row_keyspace_id.0)
        .expect("schema field length must fit in u32");
    for column in &table.columns {
        writer
            .field(TAG_COLUMN, &encode_column(column))
            .expect("schema field length must fit in u32");
    }
    for unique in &table.unique_constraints {
        writer
            .field(TAG_UNIQUE_CONSTRAINT, &encode_unique_constraint(unique))
            .expect("schema field length must fit in u32");
    }
    writer.finish()
}

fn encode_row_keyspace(table: &CreateTable) -> Vec<u8> {
    let primary = table.primary_key_columns().collect::<Vec<_>>();
    let mut writer = Writer::with_capacity(2 + primary.len() * 17);
    writer.u8(1);
    writer.u8(u8::try_from(primary.len()).expect("supported primary key count fits in u8"));
    for column in primary {
        writer.bytes16(&column.id.0);
        writer.u8(column.declared_type.to_u8());
    }
    writer.finish()
}

fn encode_column(column: &Column) -> Vec<u8> {
    let mut writer = Writer::new();
    writer
        .field(TAG_COLUMN_ID, &column.id.0)
        .expect("schema field length must fit in u32");
    writer
        .field(TAG_COLUMN_NAME, column.name.value().as_bytes())
        .expect("schema field length must fit in u32");
    writer
        .field(TAG_COLUMN_TYPE, &[column.declared_type.to_u8()])
        .expect("schema field length must fit in u32");
    let mut flags = 0;
    if column.not_null {
        flags |= COLUMN_NOT_NULL;
    }
    if column.primary_key {
        flags |= COLUMN_PRIMARY_KEY;
    }
    writer
        .field(TAG_COLUMN_FLAGS, &[flags])
        .expect("schema field length must fit in u32");
    writer.finish()
}

fn encode_unique_constraint(unique: &UniqueConstraint) -> Vec<u8> {
    let mut writer = Writer::new();
    writer
        .field(TAG_UNIQUE_KEYSPACE_ID, &unique.keyspace_id.0)
        .expect("schema field length must fit in u32");
    if let Some(name) = &unique.name {
        writer
            .field(TAG_UNIQUE_NAME, name.value().as_bytes())
            .expect("schema field length must fit in u32");
    }
    for column in &unique.columns {
        writer
            .field(TAG_UNIQUE_COLUMN_ID, &column.0)
            .expect("schema field length must fit in u32");
    }
    writer.finish()
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SchemaCodecError {
    UnknownVersion,
    Truncated,
    DuplicateField,
    MissingField(u8),
    InvalidLength,
    InvalidUtf8,
    InvalidColumnType(u8),
    InvalidColumnFlags(u8),
    InvalidSchema,
    InvalidUuid,
    InvalidSql,
    SqlMismatch,
    #[cfg(test)]
    InvalidBatch,
}

impl fmt::Display for SchemaCodecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownVersion => f.write_str("unknown schema frame version"),
            Self::Truncated => f.write_str("truncated schema frame"),
            Self::DuplicateField => f.write_str("duplicate schema field"),
            Self::MissingField(tag) => write!(f, "missing schema field {tag}"),
            Self::InvalidLength => f.write_str("invalid schema field length"),
            Self::InvalidUtf8 => f.write_str("schema name or SQL is not UTF-8"),
            Self::InvalidColumnType(value) => write!(f, "invalid column type {value}"),
            Self::InvalidColumnFlags(value) => write!(f, "invalid column flags {value}"),
            Self::InvalidSchema => f.write_str("invalid structured schema"),
            Self::InvalidUuid => f.write_str("schema id is not a UUID v4"),
            Self::InvalidSql => f.write_str("literal SQL is outside the supported grammar"),
            Self::SqlMismatch => f.write_str("literal SQL contradicts the structured schema"),
            #[cfg(test)]
            Self::InvalidBatch => f.write_str("admitted schema mutation has an invalid envelope"),
        }
    }
}

fn decode_frame(frame: &[u8]) -> std::result::Result<CreateTable, SchemaCodecError> {
    use homebase_core::reader::Reader;

    let mut reader = Reader::new(frame);
    if reader.u8() != Some(SCHEMA_FRAME_VERSION) {
        return Err(SchemaCodecError::UnknownVersion);
    }
    let mut mutation_id = None;
    let mut sql = None;
    let mut create_table = None;
    while let Some((tag, value)) = reader.field().map_err(|_| SchemaCodecError::Truncated)? {
        match tag {
            TAG_MUTATION_ID => {
                set_once(&mut mutation_id, MutationId(uuid_bytes(value)?))?;
            }
            TAG_SQL => set_once(
                &mut sql,
                String::from_utf8(value.to_vec()).map_err(|_| SchemaCodecError::InvalidUtf8)?,
            )?,
            TAG_CREATE_TABLE => {
                set_once(&mut create_table, decode_create_table(value)?)?;
            }
            _ => {}
        }
    }
    let mutation_id = mutation_id.ok_or(SchemaCodecError::MissingField(TAG_MUTATION_ID))?;
    let sql = sql.ok_or(SchemaCodecError::MissingField(TAG_SQL))?;
    let (table_id, schema_revision_id, row_keyspace_id, name, columns, unique_constraints) =
        create_table.ok_or(SchemaCodecError::MissingField(TAG_CREATE_TABLE))?;
    Ok(CreateTable {
        mutation_id,
        sql,
        table_id,
        schema_revision_id,
        row_keyspace_id,
        name,
        columns,
        unique_constraints,
    })
}

fn set_once<T>(slot: &mut Option<T>, value: T) -> std::result::Result<(), SchemaCodecError> {
    if slot.replace(value).is_some() {
        Err(SchemaCodecError::DuplicateField)
    } else {
        Ok(())
    }
}

fn uuid_bytes(value: &[u8]) -> std::result::Result<[u8; 16], SchemaCodecError> {
    let bytes = value
        .try_into()
        .map_err(|_| SchemaCodecError::InvalidLength)?;
    let uuid = Uuid::from_bytes(bytes);
    if uuid.get_version() != Some(Version::Random) || uuid.get_variant() != Variant::RFC4122 {
        return Err(SchemaCodecError::InvalidUuid);
    }
    Ok(bytes)
}

fn decode_create_table(
    frame: &[u8],
) -> std::result::Result<
    (
        TableId,
        SchemaRevisionId,
        RowKeyspaceId,
        SqlName,
        Vec<Column>,
        Vec<UniqueConstraint>,
    ),
    SchemaCodecError,
> {
    use homebase_core::reader::Reader;

    let mut reader = Reader::new(frame);
    let mut table_id = None;
    let mut schema_revision_id = None;
    let mut row_keyspace_id = None;
    let mut name = None;
    let mut columns = Vec::new();
    let mut unique_constraints = Vec::new();
    while let Some((tag, value)) = reader.field().map_err(|_| SchemaCodecError::Truncated)? {
        match tag {
            TAG_TABLE_ID => set_once(&mut table_id, TableId(uuid_bytes(value)?))?,
            TAG_TABLE_NAME => set_once(&mut name, decode_name(value)?)?,
            TAG_SCHEMA_REVISION_ID => set_once(
                &mut schema_revision_id,
                SchemaRevisionId(uuid_bytes(value)?),
            )?,
            TAG_ROW_KEYSPACE_ID => {
                set_once(&mut row_keyspace_id, RowKeyspaceId(uuid_bytes(value)?))?
            }
            TAG_COLUMN => columns.push(decode_column(value)?),
            TAG_UNIQUE_CONSTRAINT => unique_constraints.push(decode_unique_constraint(value)?),
            _ => {}
        }
    }
    let primary_keys = columns.iter().filter(|column| column.primary_key).count();
    if columns.is_empty()
        || primary_keys != 1
        || unique_constraints
            .iter()
            .enumerate()
            .any(|(index, unique)| {
                unique.columns.is_empty()
                    || unique_constraints[..index]
                        .iter()
                        .any(|seen| seen.keyspace_id == unique.keyspace_id)
                    || unique
                        .columns
                        .iter()
                        .enumerate()
                        .any(|(column_index, column)| {
                            unique.columns[..column_index].contains(column)
                                || !columns.iter().any(|candidate| candidate.id == *column)
                        })
            })
    {
        return Err(SchemaCodecError::InvalidSchema);
    }
    Ok((
        table_id.ok_or(SchemaCodecError::MissingField(TAG_TABLE_ID))?,
        schema_revision_id.ok_or(SchemaCodecError::MissingField(TAG_SCHEMA_REVISION_ID))?,
        row_keyspace_id.ok_or(SchemaCodecError::MissingField(TAG_ROW_KEYSPACE_ID))?,
        name.ok_or(SchemaCodecError::MissingField(TAG_TABLE_NAME))?,
        columns,
        unique_constraints,
    ))
}

fn decode_column(frame: &[u8]) -> std::result::Result<Column, SchemaCodecError> {
    use homebase_core::reader::Reader;

    let mut reader = Reader::new(frame);
    let mut id = None;
    let mut name = None;
    let mut declared_type = None;
    let mut flags = None;
    while let Some((tag, value)) = reader.field().map_err(|_| SchemaCodecError::Truncated)? {
        match tag {
            TAG_COLUMN_ID => set_once(&mut id, ColumnId(uuid_bytes(value)?))?,
            TAG_COLUMN_NAME => set_once(&mut name, decode_name(value)?)?,
            TAG_COLUMN_TYPE => {
                let [value] = value else {
                    return Err(SchemaCodecError::InvalidLength);
                };
                set_once(&mut declared_type, DeclaredType::from_u8(*value)?)?;
            }
            TAG_COLUMN_FLAGS => {
                let [value] = value else {
                    return Err(SchemaCodecError::InvalidLength);
                };
                if value & !(COLUMN_NOT_NULL | COLUMN_PRIMARY_KEY) != 0 {
                    return Err(SchemaCodecError::InvalidColumnFlags(*value));
                }
                set_once(&mut flags, *value)?;
            }
            _ => {}
        }
    }
    let flags = flags.ok_or(SchemaCodecError::MissingField(TAG_COLUMN_FLAGS))?;
    Ok(Column {
        id: id.ok_or(SchemaCodecError::MissingField(TAG_COLUMN_ID))?,
        name: name.ok_or(SchemaCodecError::MissingField(TAG_COLUMN_NAME))?,
        declared_type: declared_type.ok_or(SchemaCodecError::MissingField(TAG_COLUMN_TYPE))?,
        not_null: flags & COLUMN_NOT_NULL != 0,
        primary_key: flags & COLUMN_PRIMARY_KEY != 0,
    })
}

fn decode_unique_constraint(
    frame: &[u8],
) -> std::result::Result<UniqueConstraint, SchemaCodecError> {
    use homebase_core::reader::Reader;

    let mut reader = Reader::new(frame);
    let mut keyspace_id = None;
    let mut name = None;
    let mut columns = Vec::new();
    while let Some((tag, value)) = reader.field().map_err(|_| SchemaCodecError::Truncated)? {
        match tag {
            TAG_UNIQUE_KEYSPACE_ID => {
                set_once(&mut keyspace_id, UniqueKeyspaceId(uuid_bytes(value)?))?
            }
            TAG_UNIQUE_NAME => set_once(&mut name, decode_name(value)?)?,
            TAG_UNIQUE_COLUMN_ID => columns.push(ColumnId(uuid_bytes(value)?)),
            _ => {}
        }
    }
    if columns.is_empty()
        || columns
            .iter()
            .enumerate()
            .any(|(index, column)| columns[..index].contains(column))
    {
        return Err(SchemaCodecError::InvalidSchema);
    }
    Ok(UniqueConstraint {
        keyspace_id: keyspace_id.ok_or(SchemaCodecError::MissingField(TAG_UNIQUE_KEYSPACE_ID))?,
        name,
        columns,
    })
}

fn decode_name(value: &[u8]) -> std::result::Result<SqlName, SchemaCodecError> {
    let value = String::from_utf8(value.to_vec()).map_err(|_| SchemaCodecError::InvalidUtf8)?;
    Ok(SqlName::new(value))
}

#[cfg(test)]
fn from_homebase_inner(
    batch: &AdmittedBatch<Vec<u8>>,
) -> std::result::Result<CreateTable, SchemaCodecError> {
    batch
        .validate()
        .map_err(|_| SchemaCodecError::InvalidBatch)?;
    let log_entry = batch
        .entries
        .first()
        .ok_or(SchemaCodecError::InvalidBatch)?;
    let Mutation::Set {
        key: admitted_log_key,
        value: frame,
    } = &log_entry.device_entry.mutation
    else {
        return Err(SchemaCodecError::InvalidBatch);
    };
    let created = CreateTable::decode(frame)?;
    if admitted_log_key != &log_key(created.mutation_id) {
        return Err(SchemaCodecError::InvalidBatch);
    }
    let expected = created.to_homebase().mutations;
    if expected.len() != batch.entries.len()
        || expected
            .iter()
            .zip(&batch.entries)
            .any(|(expected, admitted)| expected != &admitted.device_entry.mutation)
    {
        return Err(SchemaCodecError::InvalidBatch);
    }
    Ok(created)
}

fn validate_literal_sql(created: &CreateTable) -> std::result::Result<(), SchemaCodecError> {
    let parsed = parse_create_table(&created.sql)?;
    if !created.matches_spec(&parsed) {
        return Err(SchemaCodecError::SqlMismatch);
    }
    Ok(())
}

fn parse_create_table(sql: &str) -> std::result::Result<CreateTableSpec, SchemaCodecError> {
    let super::sql::ValidatedExecute::CreateTable(parsed) =
        super::sql::validate_execute(sql).map_err(|_| SchemaCodecError::InvalidSql)?
    else {
        return Err(SchemaCodecError::InvalidSql);
    };
    Ok(parsed)
}

#[cfg(test)]
mod tests {
    use homebase_core::seal::Seal;
    use homebase_core::tag::{
        AdmissionSeq, AdmissionTag, CipherEpoch, DeviceChecksum, DeviceEntry, DeviceId, DeviceSeq,
        DeviceTag, Ver,
    };

    use super::*;

    fn definition(name: &str) -> CreateTableSpec {
        CreateTableSpec {
            name: SqlName::new(name.into()),
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
                    not_null: true,
                    primary_key: false,
                },
            ],
            unique_constraints: Vec::new(),
        }
    }

    fn deterministic_create(name: &str) -> CreateTable {
        let mut next = 1_u8;
        build_create_table(
            &format!("CREATE TABLE {name} (id INTEGER PRIMARY KEY, body TEXT NOT NULL)"),
            definition(name),
            || {
                let id = test_uuid(next);
                next += 1;
                id
            },
        )
    }

    fn deterministic_unique_create() -> CreateTable {
        let mut next = 1_u8;
        build_create_table(
            "CREATE TABLE accounts (
                id INTEGER PRIMARY KEY,
                organization TEXT,
                email TEXT,
                CONSTRAINT account_email UNIQUE (organization, email)
            )",
            CreateTableSpec {
                name: SqlName::new("accounts".into()),
                columns: vec![
                    CreateColumn {
                        name: SqlName::new("id".into()),
                        declared_type: DeclaredType::Integer,
                        not_null: false,
                        primary_key: true,
                    },
                    CreateColumn {
                        name: SqlName::new("organization".into()),
                        declared_type: DeclaredType::Text,
                        not_null: false,
                        primary_key: false,
                    },
                    CreateColumn {
                        name: SqlName::new("email".into()),
                        declared_type: DeclaredType::Text,
                        not_null: false,
                        primary_key: false,
                    },
                ],
                unique_constraints: vec![CreateUnique {
                    name: Some(SqlName::new("account_email".into())),
                    columns: vec![
                        SqlName::new("organization".into()),
                        SqlName::new("email".into()),
                    ],
                }],
            },
            || {
                let id = test_uuid(next);
                next += 1;
                id
            },
        )
    }

    fn deterministic_overlapping_unique_create() -> CreateTable {
        let mut next = 1_u8;
        build_create_table(
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
                columns: vec![
                    CreateColumn {
                        name: SqlName::new("id".into()),
                        declared_type: DeclaredType::Integer,
                        not_null: false,
                        primary_key: true,
                    },
                    CreateColumn {
                        name: SqlName::new("tenant".into()),
                        declared_type: DeclaredType::Text,
                        not_null: false,
                        primary_key: false,
                    },
                    CreateColumn {
                        name: SqlName::new("email".into()),
                        declared_type: DeclaredType::Text,
                        not_null: false,
                        primary_key: false,
                    },
                    CreateColumn {
                        name: SqlName::new("username".into()),
                        declared_type: DeclaredType::Text,
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
            || {
                let id = test_uuid(next);
                next += 1;
                id
            },
        )
    }

    fn test_uuid(byte: u8) -> [u8; 16] {
        let mut id = [byte; 16];
        id[6] = (id[6] & 0x0f) | 0x40;
        id[8] = (id[8] & 0x3f) | 0x80;
        id
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
    fn table_creation_lowers_to_log_and_revision_cells_and_raises_back() {
        let created = deterministic_create("Notes");
        let lowered = created.to_homebase();
        assert_eq!(lowered.mutations.len(), 6);
        assert_eq!(lowered.footprint.constraints().len(), 1);
        assert_eq!(lowered.footprint.writes().len(), 1);

        let Mutation::Set { key: log, value } = &lowered.mutations[0] else {
            panic!("schema log entry was not a set")
        };
        assert_eq!(log.components()[2].as_bytes(), b"log");
        assert_eq!(log.components()[3].as_bytes(), test_uuid(1));
        assert_eq!(decode_frame(value).unwrap(), created);
        assert!(
            lowered
                .footprint
                .constraints()
                .contains(lowered.mutations[1].key())
        );
        assert!(
            lowered
                .footprint
                .writes()
                .contains(lowered.mutations[5].key())
        );

        let admitted = admit(lowered.mutations);
        assert_eq!(CreateTable::from_homebase(&admitted).unwrap(), created);
    }

    #[test]
    fn composite_unique_constraints_roundtrip_with_their_own_keyspace() {
        let created = deterministic_unique_create();
        let unique = &created.unique_constraints[0];
        assert_eq!(unique.keyspace_id.0, test_uuid(8));
        assert_eq!(
            unique.columns,
            vec![created.columns[1].id, created.columns[2].id]
        );

        let decoded = CreateTable::decode(&created.encode()).unwrap();
        assert_eq!(decoded, created);
        let lowered = created.to_homebase();
        assert_eq!(lowered.mutations.len(), 7);
        assert_eq!(
            lowered.mutations[5].key(),
            &unique_keyspace_key(created.table_id, unique.keyspace_id)
        );
        assert_eq!(
            CreateTable::from_homebase(&admit(lowered.mutations)).unwrap(),
            created
        );
    }

    #[test]
    fn overlapping_unique_constraints_keep_distinct_ordered_keyspaces() {
        let created = deterministic_overlapping_unique_create();
        assert_eq!(created.unique_constraints.len(), 4);
        assert_eq!(
            created
                .unique_constraints
                .iter()
                .map(|unique| unique.keyspace_id.0)
                .collect::<Vec<_>>(),
            [test_uuid(9), test_uuid(10), test_uuid(11), test_uuid(12)]
        );
        assert_eq!(
            created
                .unique_constraints
                .iter()
                .map(|unique| unique.columns.clone())
                .collect::<Vec<_>>(),
            [
                vec![created.columns[2].id],
                vec![created.columns[3].id],
                vec![created.columns[1].id, created.columns[2].id],
                vec![created.columns[1].id, created.columns[3].id],
            ]
        );

        assert_eq!(CreateTable::decode(&created.encode()).unwrap(), created);
        let lowered = created.to_homebase();
        assert_eq!(lowered.mutations.len(), 10);
        for (mutation, unique) in lowered.mutations[5..9]
            .iter()
            .zip(&created.unique_constraints)
        {
            assert_eq!(
                mutation.key(),
                &unique_keyspace_key(created.table_id, unique.keyspace_id)
            );
        }
        assert_eq!(
            CreateTable::from_homebase(&admit(lowered.mutations)).unwrap(),
            created
        );
    }

    #[test]
    fn decoder_rejects_malformed_unique_definitions() {
        let mut duplicate_column = deterministic_unique_create();
        duplicate_column.unique_constraints[0]
            .columns
            .push(duplicate_column.columns[1].id);
        assert_eq!(
            CreateTable::decode(&duplicate_column.encode()),
            Err(SchemaCodecError::InvalidSchema)
        );

        let mut unknown_column = deterministic_unique_create();
        unknown_column.unique_constraints[0].columns[0] = ColumnId(test_uuid(99));
        assert_eq!(
            CreateTable::decode(&unknown_column.encode()),
            Err(SchemaCodecError::InvalidSchema)
        );

        let mut duplicate_keyspace = deterministic_overlapping_unique_create();
        duplicate_keyspace.unique_constraints[1].keyspace_id =
            duplicate_keyspace.unique_constraints[0].keyspace_id;
        assert_eq!(
            CreateTable::decode(&duplicate_keyspace.encode()),
            Err(SchemaCodecError::InvalidSchema)
        );
    }

    #[test]
    fn short_names_are_readable_and_long_names_are_hashed() {
        let short = name_component("A".repeat(250).as_bytes());
        assert!(short.starts_with(b"name-"));
        assert_eq!(short.len(), 255);

        let long = name_component("A".repeat(251).as_bytes());
        assert!(long.starts_with(b"hash-"));
        assert_eq!(long.len(), 37);
        assert_eq!(
            table_name_scope_key(&SqlName::new("Notes".into())),
            table_name_scope_key(&SqlName::new("nOtEs".into()))
        );
    }

    #[test]
    fn decoder_rejects_malformed_frames_and_invalid_uuids() {
        let created = deterministic_create("notes");
        let encoded = created.encode();
        assert_eq!(decode_frame(&encoded).unwrap(), created);
        assert_eq!(decode_frame(&[]), Err(SchemaCodecError::UnknownVersion));
        assert_eq!(
            decode_frame(&[SCHEMA_FRAME_VERSION]),
            Err(SchemaCodecError::MissingField(TAG_MUTATION_ID))
        );
        assert_eq!(
            decode_frame(&encoded[..encoded.len() - 1]),
            Err(SchemaCodecError::Truncated)
        );

        let mut invalid_uuid = encoded;
        invalid_uuid[6..22].fill(0);
        assert_eq!(
            decode_frame(&invalid_uuid),
            Err(SchemaCodecError::InvalidUuid)
        );
    }

    #[test]
    fn admitted_envelope_rejects_missing_or_corrupt_revision_cells() {
        let lowered = deterministic_create("notes").to_homebase();
        let mut missing = admit(lowered.mutations.clone());
        missing.entries.pop();
        assert_eq!(
            from_homebase_inner(&missing),
            Err(SchemaCodecError::InvalidBatch)
        );

        let mut corrupt = admit(lowered.mutations);
        let Mutation::Set { value, .. } = &mut corrupt.entries[1].device_entry.mutation else {
            unreachable!()
        };
        value[0] ^= 0xff;
        assert_eq!(
            from_homebase_inner(&corrupt),
            Err(SchemaCodecError::InvalidBatch)
        );
    }

    #[test]
    fn literal_sql_must_match_the_structured_schema() {
        let created = deterministic_create("notes");
        validate_literal_sql(&created).unwrap();

        let mut mismatch = created.clone();
        mismatch.sql = "CREATE TABLE notes (id INTEGER PRIMARY KEY, body BLOB NOT NULL)".into();
        assert_eq!(
            validate_literal_sql(&mismatch),
            Err(SchemaCodecError::SqlMismatch)
        );
    }

    #[test]
    fn minted_ids_are_uuid_v4_shaped() {
        let created = CreateTable::new(
            "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT NOT NULL)",
            definition("notes"),
        );
        for bytes in std::iter::once(created.mutation_id.0)
            .chain(std::iter::once(created.table_id.0))
            .chain(std::iter::once(created.schema_revision_id.0))
            .chain(std::iter::once(created.row_keyspace_id.0))
            .chain(created.columns.iter().map(|column| column.id.0))
        {
            let uuid = Uuid::from_bytes(bytes);
            assert_eq!(uuid.get_version(), Some(Version::Random));
            assert_eq!(uuid.get_variant(), Variant::RFC4122);
        }
    }
}
