//! Durable schema identities, codecs, and Homebase coordination keys.
//!
//! A table creation lowers to an immutable UUID-keyed schema log entry plus
//! mutable revision cells. It can be reconstructed only from a complete,
//! self-consistent admitted envelope.

use std::fmt;

use homebase_core::key::Key;
use homebase_core::messages::AdmittedBatch;
use homebase_core::tag::Mutation;
use sha2::{Digest, Sha256};
use uuid::{Uuid, Variant, Version};

use super::codes;

const SCHEMA_FRAME_VERSION: u8 = 1;
const TAG_MUTATION_ID: u8 = 1;
const TAG_SQL: u8 = 2;
const TAG_CREATE_TABLE: u8 = 10;
const TAG_TABLE_ID: u8 = 1;
const TAG_TABLE_NAME: u8 = 2;
const TAG_COLUMN: u8 = 3;
const TAG_SCHEMA_REVISION_ID: u8 = 4;
const TAG_ROW_KEYSPACE_ID: u8 = 5;
const TAG_COLUMN_ID: u8 = 1;
const TAG_COLUMN_NAME: u8 = 2;
const TAG_COLUMN_TYPE: u8 = 3;
const TAG_COLUMN_FLAGS: u8 = 4;
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

/// Structured result of validating a restricted `CREATE TABLE` statement.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateTableSpec {
    pub name: SqlName,
    pub columns: Vec<CreateColumn>,
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
pub struct ColumnId([u8; 16]);

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Column {
    id: ColumnId,
    name: SqlName,
    declared_type: DeclaredType,
    not_null: bool,
    primary_key: bool,
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
}

/// Homebase mutations and coordination scopes for one schema change.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SchemaHomebaseOp {
    pub mutations: Vec<Mutation>,
    pub asserted_scopes: Vec<Key>,
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
        SchemaHomebaseOp {
            mutations: vec![
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
                Mutation::Set {
                    key: write_revision.clone(),
                    value: self.mutation_id.0.to_vec(),
                },
            ],
            asserted_scopes: vec![name_scope, write_revision],
        }
    }

    /// Raise one complete authenticated Homebase batch into a schema change.
    pub fn from_homebase(
        batch: &AdmittedBatch<Vec<u8>>,
    ) -> std::result::Result<Self, SchemaCodecError> {
        from_homebase_inner(batch)
    }

    /// Encode this complete schema operation for local durable state.
    pub fn encode(&self) -> Vec<u8> {
        let mut frame = vec![SCHEMA_FRAME_VERSION];
        put_field(&mut frame, TAG_MUTATION_ID, &self.mutation_id.0);
        put_field(&mut frame, TAG_SQL, self.sql.as_bytes());
        put_field(&mut frame, TAG_CREATE_TABLE, &encode_create_table(self));
        frame
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
    }
}

fn build_create_table(
    sql: &str,
    spec: CreateTableSpec,
    mut mint: impl FnMut() -> [u8; 16],
) -> CreateTable {
    let mutation_id = MutationId(mint());
    let table_id = TableId(mint());
    let schema_revision_id = SchemaRevisionId(mint());
    let row_keyspace_id = RowKeyspaceId(mint());
    let columns = spec
        .columns
        .into_iter()
        .map(|column| Column {
            id: ColumnId(mint()),
            name: column.name,
            declared_type: column.declared_type,
            not_null: column.not_null,
            primary_key: column.primary_key,
        })
        .collect();
    CreateTable {
        mutation_id,
        sql: sql.to_owned(),
        table_id,
        schema_revision_id,
        row_keyspace_id,
        name: spec.name,
        columns,
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
    let mut frame = Vec::new();
    put_field(&mut frame, TAG_TABLE_ID, &table.table_id.0);
    put_field(&mut frame, TAG_TABLE_NAME, table.name.value().as_bytes());
    put_field(
        &mut frame,
        TAG_SCHEMA_REVISION_ID,
        &table.schema_revision_id.0,
    );
    put_field(&mut frame, TAG_ROW_KEYSPACE_ID, &table.row_keyspace_id.0);
    for column in &table.columns {
        put_field(&mut frame, TAG_COLUMN, &encode_column(column));
    }
    frame
}

fn encode_row_keyspace(table: &CreateTable) -> Vec<u8> {
    let primary = table.primary_key_columns().collect::<Vec<_>>();
    let mut frame = Vec::with_capacity(2 + primary.len() * 17);
    frame.push(1);
    frame.push(u8::try_from(primary.len()).expect("supported primary key count fits in u8"));
    for column in primary {
        frame.extend_from_slice(&column.id.0);
        frame.push(column.declared_type.to_u8());
    }
    frame
}

fn encode_column(column: &Column) -> Vec<u8> {
    let mut frame = Vec::new();
    put_field(&mut frame, TAG_COLUMN_ID, &column.id.0);
    put_field(&mut frame, TAG_COLUMN_NAME, column.name.value().as_bytes());
    put_field(&mut frame, TAG_COLUMN_TYPE, &[column.declared_type.to_u8()]);
    let mut flags = 0;
    if column.not_null {
        flags |= COLUMN_NOT_NULL;
    }
    if column.primary_key {
        flags |= COLUMN_PRIMARY_KEY;
    }
    put_field(&mut frame, TAG_COLUMN_FLAGS, &[flags]);
    frame
}

fn put_field(frame: &mut Vec<u8>, tag: u8, value: &[u8]) {
    frame.push(tag);
    let len = u32::try_from(value.len()).expect("schema field length must fit in u32");
    frame.extend_from_slice(&len.to_be_bytes());
    frame.extend_from_slice(value);
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
    while let Some((tag, value)) = next_field(&mut reader)? {
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
    let (table_id, schema_revision_id, row_keyspace_id, name, columns) =
        create_table.ok_or(SchemaCodecError::MissingField(TAG_CREATE_TABLE))?;
    Ok(CreateTable {
        mutation_id,
        sql,
        table_id,
        schema_revision_id,
        row_keyspace_id,
        name,
        columns,
    })
}

fn next_field<'a>(
    reader: &mut homebase_core::reader::Reader<'a>,
) -> std::result::Result<Option<(u8, &'a [u8])>, SchemaCodecError> {
    if reader.end().is_some() {
        return Ok(None);
    }
    let tag = reader.u8().ok_or(SchemaCodecError::Truncated)?;
    let len = reader.u32().ok_or(SchemaCodecError::Truncated)?;
    let len = usize::try_from(len).map_err(|_| SchemaCodecError::InvalidLength)?;
    let value = reader.take(len).ok_or(SchemaCodecError::Truncated)?;
    Ok(Some((tag, value)))
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
    while let Some((tag, value)) = next_field(&mut reader)? {
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
            _ => {}
        }
    }
    let primary_keys = columns.iter().filter(|column| column.primary_key).count();
    if columns.is_empty() || primary_keys != 1 {
        return Err(SchemaCodecError::InvalidSchema);
    }
    Ok((
        table_id.ok_or(SchemaCodecError::MissingField(TAG_TABLE_ID))?,
        schema_revision_id.ok_or(SchemaCodecError::MissingField(TAG_SCHEMA_REVISION_ID))?,
        row_keyspace_id.ok_or(SchemaCodecError::MissingField(TAG_ROW_KEYSPACE_ID))?,
        name.ok_or(SchemaCodecError::MissingField(TAG_TABLE_NAME))?,
        columns,
    ))
}

fn decode_column(frame: &[u8]) -> std::result::Result<Column, SchemaCodecError> {
    use homebase_core::reader::Reader;

    let mut reader = Reader::new(frame);
    let mut id = None;
    let mut name = None;
    let mut declared_type = None;
    let mut flags = None;
    while let Some((tag, value)) = next_field(&mut reader)? {
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

fn decode_name(value: &[u8]) -> std::result::Result<SqlName, SchemaCodecError> {
    let value = String::from_utf8(value.to_vec()).map_err(|_| SchemaCodecError::InvalidUtf8)?;
    Ok(SqlName::new(value))
}

fn from_homebase_inner(
    batch: &AdmittedBatch<Vec<u8>>,
) -> std::result::Result<CreateTable, SchemaCodecError> {
    batch
        .validate()
        .map_err(|_| SchemaCodecError::InvalidBatch)?;
    let [
        log_entry,
        name_entry,
        schema_entry,
        active_entry,
        keyspace_entry,
        write_entry,
    ] = batch.entries.as_slice()
    else {
        return Err(SchemaCodecError::InvalidBatch);
    };
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
    validate_set(
        name_entry,
        &table_name_scope_key(&created.name),
        &created.table_id.0,
    )?;
    validate_set(
        schema_entry,
        &table_schema_key(created.table_id, created.schema_revision_id),
        &created.encode(),
    )?;
    validate_set(
        active_entry,
        &active_row_keyspace_key(created.table_id),
        &created.row_keyspace_id.0,
    )?;
    validate_set(
        keyspace_entry,
        &row_keyspace_key(created.table_id, created.row_keyspace_id),
        &encode_row_keyspace(&created),
    )?;
    validate_set(
        write_entry,
        &write_revision_key(created.table_id),
        &created.mutation_id.0,
    )?;
    Ok(created)
}

fn validate_set(
    entry: &homebase_core::tag::AdmittedEntry<Vec<u8>>,
    expected_key: &Key,
    expected_value: &[u8],
) -> std::result::Result<(), SchemaCodecError> {
    let Mutation::Set { key, value } = &entry.device_entry.mutation else {
        return Err(SchemaCodecError::InvalidBatch);
    };
    if key != expected_key || value != expected_value {
        return Err(SchemaCodecError::InvalidBatch);
    }
    Ok(())
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
        assert_eq!(lowered.asserted_scopes.len(), 2);

        let Mutation::Set { key: log, value } = &lowered.mutations[0] else {
            panic!("schema log entry was not a set")
        };
        assert_eq!(log.components()[2].as_bytes(), b"log");
        assert_eq!(log.components()[3].as_bytes(), test_uuid(1));
        assert_eq!(decode_frame(value).unwrap(), created);
        assert_eq!(lowered.mutations[1].key(), &lowered.asserted_scopes[0]);
        assert_eq!(lowered.mutations[5].key(), &lowered.asserted_scopes[1]);

        let admitted = admit(lowered.mutations);
        assert_eq!(CreateTable::from_homebase(&admitted).unwrap(), created);
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
