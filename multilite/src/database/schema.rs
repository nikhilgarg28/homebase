//! Durable schema identities, codecs, and Homebase coordination keys.
//!
//! A table creation lowers to an immutable UUID-keyed schema log entry plus
//! mutable revision cells. It can be reconstructed only from a complete,
//! self-consistent admitted envelope.

#![allow(
    dead_code,
    reason = "schema translation is wired into local capture in the next batch"
)]

use std::fmt;

use homebase_core::key::Key;
use homebase_core::messages::AdmittedBatch;
use homebase_core::tag::Mutation;
use sha2::{Digest, Sha256};
use uuid::{Uuid, Variant, Version};

const SCHEMA_FRAME_VERSION: u8 = 1;
const TAG_MUTATION_ID: u8 = 1;
const TAG_SQL: u8 = 2;
const TAG_CREATE_TABLE: u8 = 10;
const TAG_TABLE_ID: u8 = 1;
const TAG_TABLE_NAME: u8 = 2;
const TAG_COLUMN: u8 = 3;
const TAG_COLUMN_ID: u8 = 1;
const TAG_COLUMN_NAME: u8 = 2;
const TAG_COLUMN_TYPE: u8 = 3;
const TAG_COLUMN_FLAGS: u8 = 4;
const COLUMN_NOT_NULL: u8 = 1;
const COLUMN_PRIMARY_KEY: u8 = 2;

const SCHEMA_ROOT: [&[u8]; 2] = [b"multilite", b"schema"];
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

    fn value(&self) -> &str {
        &self.value
    }

    fn canonical(&self) -> &[u8] {
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
    fn to_u8(self) -> u8 {
        match self {
            Self::Integer => 1,
            Self::Real => 2,
            Self::Text => 3,
            Self::Blob => 4,
        }
    }

    fn from_u8(value: u8) -> std::result::Result<Self, SchemaCodecError> {
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
struct MutationId([u8; 16]);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct TableId([u8; 16]);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ColumnId([u8; 16]);

#[derive(Clone, Debug, PartialEq, Eq)]
struct Column {
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
    name: SqlName,
    columns: Vec<Column>,
}

/// Homebase mutations and coordination scopes for one schema change.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SchemaHomebaseOp {
    pub mutations: Vec<Mutation>,
    pub asserted_scopes: Vec<Key>,
}

impl CreateTable {
    /// Mint durable identities for one validated table creation.
    pub fn new(sql: &str, spec: CreateTableSpec) -> Self {
        build_create_table(sql, spec, || Uuid::new_v4().into_bytes())
    }

    /// Lower this schema change to its complete Homebase representation.
    pub fn to_homebase(&self) -> SchemaHomebaseOp {
        let log = log_key(self.mutation_id);
        let table_scope = table_scope_key(self.table_id);
        let name_scope = table_name_scope_key(&self.name);
        let revision = self.mutation_id.0.to_vec();
        SchemaHomebaseOp {
            mutations: vec![
                Mutation::Set {
                    key: log,
                    value: self.encode(),
                },
                Mutation::Set {
                    key: table_scope.clone(),
                    value: revision.clone(),
                },
                Mutation::Set {
                    key: name_scope.clone(),
                    value: revision,
                },
            ],
            asserted_scopes: vec![table_scope, name_scope],
        }
    }

    /// Raise one complete authenticated Homebase batch into a schema change.
    pub fn from_homebase(
        batch: &AdmittedBatch<Vec<u8>>,
    ) -> std::result::Result<Self, SchemaCodecError> {
        from_homebase_inner(batch)
    }

    fn encode(&self) -> Vec<u8> {
        let mut frame = vec![SCHEMA_FRAME_VERSION];
        put_field(&mut frame, TAG_MUTATION_ID, &self.mutation_id.0);
        put_field(&mut frame, TAG_SQL, self.sql.as_bytes());
        put_field(&mut frame, TAG_CREATE_TABLE, &encode_create_table(self));
        frame
    }
}

fn build_create_table(
    sql: &str,
    spec: CreateTableSpec,
    mut mint: impl FnMut() -> [u8; 16],
) -> CreateTable {
    let mutation_id = MutationId(mint());
    let table_id = TableId(mint());
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
        name: spec.name,
        columns,
    }
}

fn log_key(id: MutationId) -> Key {
    Key::from_bytes([
        SCHEMA_ROOT[0],
        SCHEMA_ROOT[1],
        b"log".as_slice(),
        id.0.as_slice(),
    ])
    .expect("schema log components are bounded and non-empty")
}

fn table_scope_key(id: TableId) -> Key {
    Key::from_bytes([
        SCHEMA_ROOT[0],
        SCHEMA_ROOT[1],
        b"scopes".as_slice(),
        b"tables".as_slice(),
        id.0.as_slice(),
    ])
    .expect("table scope components are bounded and non-empty")
}

fn table_name_scope_key(name: &SqlName) -> Key {
    let component = name_component(name.canonical());
    Key::from_bytes([
        SCHEMA_ROOT[0],
        SCHEMA_ROOT[1],
        b"scopes".as_slice(),
        b"table-names".as_slice(),
        component.as_slice(),
    ])
    .expect("table-name scope components are bounded and non-empty")
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
    for column in &table.columns {
        put_field(&mut frame, TAG_COLUMN, &encode_column(column));
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
    let (table_id, name, columns) =
        create_table.ok_or(SchemaCodecError::MissingField(TAG_CREATE_TABLE))?;
    Ok(CreateTable {
        mutation_id,
        sql,
        table_id,
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
) -> std::result::Result<(TableId, SqlName, Vec<Column>), SchemaCodecError> {
    use homebase_core::reader::Reader;

    let mut reader = Reader::new(frame);
    let mut table_id = None;
    let mut name = None;
    let mut columns = Vec::new();
    while let Some((tag, value)) = next_field(&mut reader)? {
        match tag {
            TAG_TABLE_ID => set_once(&mut table_id, TableId(uuid_bytes(value)?))?,
            TAG_TABLE_NAME => set_once(&mut name, decode_name(value)?)?,
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
    let [log_entry, table_entry, name_entry] = batch.entries.as_slice() else {
        return Err(SchemaCodecError::InvalidBatch);
    };
    let Mutation::Set {
        key: admitted_log_key,
        value: frame,
    } = &log_entry.device_entry.mutation
    else {
        return Err(SchemaCodecError::InvalidBatch);
    };
    let created = decode_frame(frame)?;
    if admitted_log_key != &log_key(created.mutation_id) {
        return Err(SchemaCodecError::InvalidBatch);
    }
    validate_literal_sql(&created)?;
    validate_revision_entry(
        table_entry,
        &table_scope_key(created.table_id),
        created.mutation_id,
    )?;
    validate_revision_entry(
        name_entry,
        &table_name_scope_key(&created.name),
        created.mutation_id,
    )?;
    Ok(created)
}

fn validate_revision_entry(
    entry: &homebase_core::tag::AdmittedEntry<Vec<u8>>,
    expected_key: &Key,
    mutation_id: MutationId,
) -> std::result::Result<(), SchemaCodecError> {
    let Mutation::Set { key, value } = &entry.device_entry.mutation else {
        return Err(SchemaCodecError::InvalidBatch);
    };
    if key != expected_key || value.as_slice() != mutation_id.0 {
        return Err(SchemaCodecError::InvalidBatch);
    }
    Ok(())
}

fn validate_literal_sql(created: &CreateTable) -> std::result::Result<(), SchemaCodecError> {
    let super::sql::ValidatedExecute::CreateTable(parsed) =
        super::sql::validate_execute(&created.sql).map_err(|_| SchemaCodecError::InvalidSql)?
    else {
        return Err(SchemaCodecError::InvalidSql);
    };
    if parsed.name != created.name || parsed.columns.len() != created.columns.len() {
        return Err(SchemaCodecError::SqlMismatch);
    }
    for (parsed, encoded) in parsed.columns.iter().zip(&created.columns) {
        if parsed.name != encoded.name
            || parsed.declared_type != encoded.declared_type
            || parsed.not_null != encoded.not_null
            || parsed.primary_key != encoded.primary_key
        {
            return Err(SchemaCodecError::SqlMismatch);
        }
    }
    Ok(())
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
        assert_eq!(lowered.mutations.len(), 3);
        assert_eq!(lowered.asserted_scopes.len(), 2);

        let Mutation::Set { key: log, value } = &lowered.mutations[0] else {
            panic!("schema log entry was not a set")
        };
        assert_eq!(log.components()[2].as_bytes(), b"log");
        assert_eq!(log.components()[3].as_bytes(), test_uuid(1));
        assert_eq!(decode_frame(value).unwrap(), created);
        assert_eq!(lowered.mutations[1].key(), &lowered.asserted_scopes[0]);
        assert_eq!(lowered.mutations[2].key(), &lowered.asserted_scopes[1]);

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
            .chain(created.columns.iter().map(|column| column.id.0))
        {
            let uuid = Uuid::from_bytes(bytes);
            assert_eq!(uuid.get_version(), Some(Version::Random));
            assert_eq!(uuid.get_variant(), Variant::RFC4122);
        }
    }
}
