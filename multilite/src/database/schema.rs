//! Durable schema identities, codecs, and Homebase coordination keys.
//!
//! A table creation lowers to an immutable UUID-keyed schema log entry plus
//! mutable revision cells. It can be reconstructed only from a complete,
//! self-consistent admitted envelope.

use std::collections::BTreeSet;
use std::fmt;

use homebase_core::key::{Key, MAX_COMPONENTS};
#[cfg(test)]
use homebase_core::messages::AdmittedBatch;
use homebase_core::tag::Mutation;
use homebase_core::writer::Writer;
use sha2::{Digest, Sha256};
use uuid::{Uuid, Variant, Version};

use super::codes;
use crate::commit::footprint::ConflictFootprint;

const SCHEMA_FRAME_VERSION: u8 = 2;
const TAG_MUTATION_ID: u8 = 1;
const TAG_SQL: u8 = 2;
const TAG_CREATE_TABLE: u8 = 10;
const TAG_TABLE_ID: u8 = 1;
const TAG_TABLE_NAME: u8 = 2;
const TAG_COLUMN: u8 = 3;
const TAG_SCHEMA_REVISION_ID: u8 = 4;
const TAG_ROW_KEYSPACE_ID: u8 = 5;
const TAG_UNIQUE_CONSTRAINT: u8 = 6;
const TAG_TABLE_MODE: u8 = 7;
const TAG_PRIMARY_KEY: u8 = 8;
const TAG_TABLE_STORAGE: u8 = 9;
const TAG_INDEX_DEFINITION: u8 = 10;
const TAG_COLUMN_ID: u8 = 1;
const TAG_COLUMN_NAME: u8 = 2;
const TAG_COLUMN_TYPE: u8 = 3;
const TAG_COLUMN_FLAGS: u8 = 4;
const TYPE_DECLARATION_FRAME_VERSION: u8 = 1;
const TAG_TYPE_NAME: u8 = 1;
const TAG_TYPE_ARGUMENT: u8 = 2;
const TAG_UNIQUE_KEYSPACE_ID: u8 = 1;
const TAG_UNIQUE_NAME: u8 = 2;
const TAG_UNIQUE_COLUMN_ID: u8 = 3;
const TAG_INDEX_KEYSPACE_ID: u8 = 1;
const TAG_INDEX_NAME: u8 = 2;
const TAG_INDEX_UNIQUE: u8 = 3;
const TAG_INDEX_COLUMN_ID: u8 = 4;
const TAG_INDEX_SQL: u8 = 5;
const TAG_INDEX_ACTIVE: u8 = 6;
const TAG_PRIMARY_COLUMN_ID: u8 = 1;
const COLUMN_NOT_NULL: u8 = 1;
const TABLE_MODE_ORDINARY: u8 = 0;
const TABLE_MODE_STRICT: u8 = 1;
const TABLE_STORAGE_ROWID: u8 = 0;
const TABLE_STORAGE_WITHOUT_ROWID: u8 = 1;

const SHORT_NAME_LIMIT: usize = 250;
const TABLE_NAME_HASH_DOMAIN: &[u8] = b"multilite:table-name:v1\0";
const LOGICAL_KEY_PREFIX_COMPONENTS: usize = 5;
const SQLITE_ROWID_ALIASES: [&str; 3] = ["_rowid_", "rowid", "oid"];

/// Maximum number of value components in a row or UNIQUE Homebase key.
pub const MAX_KEY_PARTS: usize = MAX_COMPONENTS - LOGICAL_KEY_PREFIX_COMPONENTS;

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

pub fn available_hidden_rowid_alias<'a>(
    columns: impl IntoIterator<Item = &'a SqlName>,
) -> Option<&'static str> {
    let columns = columns
        .into_iter()
        .map(SqlName::canonical)
        .collect::<Vec<_>>();
    SQLITE_ROWID_ALIASES
        .into_iter()
        .find(|candidate| !columns.iter().any(|column| *column == candidate.as_bytes()))
}

/// SQLite's five ordinary-table type affinities.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Affinity {
    Integer,
    Real,
    Text,
    Blob,
    Numeric,
}

impl Affinity {
    pub fn to_u8(self) -> u8 {
        match self {
            Self::Integer => 1,
            Self::Real => 2,
            Self::Text => 3,
            Self::Blob => 4,
            Self::Numeric => 5,
        }
    }

    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            1 => Some(Self::Integer),
            2 => Some(Self::Real),
            3 => Some(Self::Text),
            4 => Some(Self::Blob),
            5 => Some(Self::Numeric),
            _ => None,
        }
    }
}

/// SQLite type enforcement selected for one table.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum TableMode {
    #[default]
    Ordinary,
    Strict,
}

/// Physical SQLite table storage selected by the schema.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum TableStorage {
    #[default]
    Rowid,
    WithoutRowid,
}

impl TableStorage {
    pub(crate) fn to_u8(self) -> u8 {
        match self {
            Self::Rowid => TABLE_STORAGE_ROWID,
            Self::WithoutRowid => TABLE_STORAGE_WITHOUT_ROWID,
        }
    }

    pub(crate) fn from_u8(value: u8) -> Option<Self> {
        match value {
            TABLE_STORAGE_ROWID => Some(Self::Rowid),
            TABLE_STORAGE_WITHOUT_ROWID => Some(Self::WithoutRowid),
            _ => None,
        }
    }
}

impl TableMode {
    fn to_u8(self) -> u8 {
        match self {
            Self::Ordinary => TABLE_MODE_ORDINARY,
            Self::Strict => TABLE_MODE_STRICT,
        }
    }

    fn from_u8(value: u8) -> Option<Self> {
        match value {
            TABLE_MODE_ORDINARY => Some(Self::Ordinary),
            TABLE_MODE_STRICT => Some(Self::Strict),
            _ => None,
        }
    }
}

/// Storage class required after SQLite coerces a STRICT-table value.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StrictType {
    Integer,
    Real,
    Text,
    Blob,
    Any,
}

/// Canonical SQLite type declaration plus its ignored size annotations.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TypeDeclaration {
    name: String,
    arguments: Vec<String>,
}

impl TypeDeclaration {
    pub fn new(name: String, arguments: Vec<String>) -> Self {
        let name = unquote_type_name(name.trim())
            .split_ascii_whitespace()
            .collect::<Vec<_>>()
            .join(" ")
            .to_ascii_uppercase();
        Self { name, arguments }
    }

    #[cfg(test)]
    pub fn integer() -> Self {
        Self::new("INTEGER".into(), Vec::new())
    }

    #[cfg(test)]
    pub fn text() -> Self {
        Self::new("TEXT".into(), Vec::new())
    }

    #[cfg(test)]
    pub fn blob() -> Self {
        Self::new("BLOB".into(), Vec::new())
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    #[cfg(test)]
    pub fn arguments(&self) -> &[String] {
        &self.arguments
    }

    pub fn affinity(&self) -> Affinity {
        if self.name.contains("INT") {
            Affinity::Integer
        } else if ["CHAR", "CLOB", "TEXT"]
            .into_iter()
            .any(|part| self.name.contains(part))
        {
            Affinity::Text
        } else if self.name.contains("BLOB") {
            Affinity::Blob
        } else if ["REAL", "FLOA", "DOUB"]
            .into_iter()
            .any(|part| self.name.contains(part))
        {
            Affinity::Real
        } else {
            Affinity::Numeric
        }
    }

    pub fn affinity_for(&self, mode: TableMode) -> Affinity {
        if mode == TableMode::Strict && self.strict_type() == Some(StrictType::Any) {
            Affinity::Blob
        } else {
            self.affinity()
        }
    }

    pub fn strict_type(&self) -> Option<StrictType> {
        if !self.arguments.is_empty() {
            return None;
        }
        match self.name.as_str() {
            "INT" | "INTEGER" => Some(StrictType::Integer),
            "REAL" => Some(StrictType::Real),
            "TEXT" => Some(StrictType::Text),
            "BLOB" => Some(StrictType::Blob),
            "ANY" => Some(StrictType::Any),
            _ => None,
        }
    }

    pub fn is_exact_integer(&self) -> bool {
        self.name == "INTEGER" && self.arguments.is_empty()
    }

    #[cfg(test)]
    pub fn to_sql(&self) -> String {
        if self.arguments.is_empty() {
            self.name.clone()
        } else {
            format!("{}({})", self.name, self.arguments.join(", "))
        }
    }
}

fn unquote_type_name(name: &str) -> String {
    let bytes = name.as_bytes();
    let Some((&open, middle_and_close)) = bytes.split_first() else {
        return String::new();
    };
    let Some((&close, middle)) = middle_and_close.split_last() else {
        return name.to_owned();
    };
    let escaped = match (open, close) {
        (b'"', b'"') | (b'`', b'`') | (b'\'', b'\'') => Some(open),
        (b'[', b']') => Some(close),
        _ => None,
    };
    let Some(escaped) = escaped else {
        return name.to_owned();
    };

    let mut value = Vec::with_capacity(middle.len());
    let mut index = 0;
    while index < middle.len() {
        value.push(middle[index]);
        if middle[index] == escaped && middle.get(index + 1) == Some(&escaped) {
            index += 1;
        }
        index += 1;
    }
    String::from_utf8(value).expect("SQLite type names originate in UTF-8 SQL")
}

/// One validated column in a restricted `CREATE TABLE` statement.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateColumn {
    pub name: SqlName,
    pub declared_type: TypeDeclaration,
    pub not_null: bool,
    /// Position in the table's ordered primary key, if any.
    pub primary_key: Option<usize>,
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
    pub mode: TableMode,
    pub storage: TableStorage,
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
    declared_type: TypeDeclaration,
    not_null: bool,
}

/// Ordered durable primary-key definition owned by a table.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PrimaryKey {
    columns: Vec<ColumnId>,
}

/// One durable UNIQUE key definition owned by a table schema revision.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UniqueConstraint {
    keyspace_id: UniqueKeyspaceId,
    name: Option<SqlName>,
    columns: Vec<ColumnId>,
}

/// An explicit index attached to a table schema.
///
/// CREATE INDEX is not admitted yet, but indexes live here rather than in a
/// parallel database-level registry when that grammar is added.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IndexDefinition {
    keyspace_id: UniqueKeyspaceId,
    name: SqlName,
    unique: bool,
    columns: Vec<ColumnId>,
    sql: String,
    active: bool,
}

/// A foreign-key definition attached to a table schema.
///
/// Foreign-key SQL remains unsupported; this establishes table ownership for
/// the definition before enforcement and codecs are added.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ForeignKeyDefinition {
    name: Option<SqlName>,
    columns: Vec<ColumnId>,
    referenced_table: SqlName,
    referenced_columns: Vec<SqlName>,
}

/// Complete schema known for one table revision.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TableSchema {
    mode: TableMode,
    storage: TableStorage,
    columns: Vec<Column>,
    primary_key: PrimaryKey,
    unique_constraints: Vec<UniqueConstraint>,
    indexes: Vec<IndexDefinition>,
    foreign_keys: Vec<ForeignKeyDefinition>,
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
    schema: TableSchema,
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
id_accessors!(MutationId);
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

    #[cfg(test)]
    pub fn declared_type(&self) -> &TypeDeclaration {
        &self.declared_type
    }

    pub fn affinity(&self, mode: TableMode) -> Affinity {
        self.declared_type.affinity_for(mode)
    }

    pub fn strict_type(&self) -> Option<StrictType> {
        self.declared_type.strict_type()
    }

    pub fn is_not_null(&self) -> bool {
        self.not_null
    }
}

impl PrimaryKey {
    pub fn columns(&self) -> &[ColumnId] {
        &self.columns
    }
}

impl TableSchema {
    pub fn mode(&self) -> TableMode {
        self.mode
    }

    pub fn storage(&self) -> TableStorage {
        self.storage
    }

    pub fn columns(&self) -> &[Column] {
        &self.columns
    }

    pub fn primary_key(&self) -> &PrimaryKey {
        &self.primary_key
    }

    pub fn unique_constraints(&self) -> &[UniqueConstraint] {
        &self.unique_constraints
    }

    pub fn indexes(&self) -> &[IndexDefinition] {
        &self.indexes
    }

    #[allow(dead_code, reason = "populated when FOREIGN KEY grammar is admitted")]
    pub fn foreign_keys(&self) -> &[ForeignKeyDefinition] {
        &self.foreign_keys
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

impl IndexDefinition {
    pub fn new_unique(sql: String, name: SqlName, columns: Vec<ColumnId>) -> Self {
        Self {
            keyspace_id: UniqueKeyspaceId(Uuid::new_v4().into_bytes()),
            name,
            unique: true,
            columns,
            sql,
            active: true,
        }
    }

    pub fn keyspace_id(&self) -> UniqueKeyspaceId {
        self.keyspace_id
    }

    pub fn name(&self) -> &SqlName {
        &self.name
    }

    pub fn is_unique(&self) -> bool {
        self.unique
    }

    pub fn columns(&self) -> &[ColumnId] {
        &self.columns
    }

    pub fn sql(&self) -> &str {
        &self.sql
    }

    pub fn is_active(&self) -> bool {
        self.active
    }

    pub fn retired(&self) -> Self {
        let mut retired = self.clone();
        retired.active = false;
        retired
    }

    pub fn encode(&self) -> Vec<u8> {
        encode_index_definition(self)
    }

    pub fn decode(frame: &[u8]) -> std::result::Result<Self, SchemaCodecError> {
        decode_index_definition(frame)
    }
}

impl CreateTable {
    /// Mint durable identities for one validated table creation.
    pub fn new(sql: &str, spec: CreateTableSpec) -> Self {
        build_create_table(sql, spec, || Uuid::new_v4().into_bytes())
    }

    /// Lower this schema change to its complete Homebase representation.
    pub fn to_homebase(&self) -> SchemaHomebaseOp {
        let log = schema_log_key(self.mutation_id);
        let name_scope = table_name_scope_key(&self.name);
        let schema = table_schema_key(self.table_id, self.schema_revision_id);
        let active_schema_revision = active_schema_revision_key(self.table_id);
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
                key: active_schema_revision,
                value: self.schema_revision_id.0.to_vec(),
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
        mutations.extend(
            self.schema
                .unique_constraints
                .iter()
                .map(|unique| Mutation::Set {
                    key: unique_keyspace_key(self.table_id, unique.keyspace_id),
                    value: encode_unique_constraint(unique),
                }),
        );
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
        validate_index_sql(&created)?;
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

    pub fn mode(&self) -> TableMode {
        self.schema.mode()
    }

    pub fn storage(&self) -> TableStorage {
        self.schema.storage()
    }

    #[allow(dead_code, reason = "used by future schema-changing operations")]
    pub fn schema(&self) -> &TableSchema {
        &self.schema
    }

    pub fn columns(&self) -> &[Column] {
        self.schema.columns()
    }

    pub fn unique_constraints(&self) -> &[UniqueConstraint] {
        self.schema.unique_constraints()
    }

    pub fn indexes(&self) -> &[IndexDefinition] {
        self.schema.indexes()
    }

    pub fn column_named(&self, name: &SqlName) -> Option<&Column> {
        self.columns()
            .iter()
            .find(|column| column.name.canonical() == name.canonical())
    }

    pub fn index_named(&self, name: &SqlName) -> Option<&IndexDefinition> {
        self.indexes()
            .iter()
            .find(|index| index.active && index.name.canonical() == name.canonical())
    }

    pub fn with_added_index(&self, revision: SchemaRevisionId, index: IndexDefinition) -> Self {
        let mut evolved = self.clone();
        evolved.schema_revision_id = revision;
        evolved.schema.indexes.push(index);
        evolved
    }

    pub fn with_retired_index(&self, revision: SchemaRevisionId, name: &SqlName) -> Option<Self> {
        let mut evolved = self.clone();
        let position = evolved
            .schema
            .indexes
            .iter()
            .position(|index| index.active && index.name.canonical() == name.canonical())?;
        evolved.schema_revision_id = revision;
        evolved.schema.indexes[position].active = false;
        Some(evolved)
    }

    pub fn primary_key_columns(&self) -> impl Iterator<Item = &Column> {
        self.schema.primary_key().columns().iter().map(|id| {
            self.schema
                .columns
                .iter()
                .find(|column| column.id == *id)
                .expect("validated primary-key column exists")
        })
    }

    pub fn is_rowid_alias(&self, column: ColumnId) -> bool {
        self.schema.storage == TableStorage::Rowid
            && self.schema.primary_key.columns.as_slice() == [column]
            && self
                .schema
                .columns
                .iter()
                .find(|candidate| candidate.id == column)
                .is_some_and(|candidate| candidate.declared_type.is_exact_integer())
    }

    pub fn hidden_rowid_alias(&self) -> Option<&'static str> {
        if self.storage() == TableStorage::WithoutRowid
            || self
                .primary_key_columns()
                .any(|column| self.is_rowid_alias(column.id()))
        {
            return None;
        }
        available_hidden_rowid_alias(self.columns().iter().map(Column::name))
    }

    fn matches_spec(&self, spec: &CreateTableSpec) -> bool {
        self.name == spec.name
            && self.schema.mode == spec.mode
            && self.schema.storage == spec.storage
            && self.schema.columns.len() == spec.columns.len()
            && self
                .schema
                .columns
                .iter()
                .zip(&spec.columns)
                .all(|(encoded, parsed)| {
                    encoded.name == parsed.name
                        && encoded.declared_type == parsed.declared_type
                        && encoded.not_null == parsed.not_null
                })
            && spec_primary_key_ids(spec, &self.schema.columns)
                .is_some_and(|ids| ids == self.schema.primary_key.columns)
            && self.schema.unique_constraints.len() == spec.unique_constraints.len()
            && self
                .schema
                .unique_constraints
                .iter()
                .zip(&spec.unique_constraints)
                .all(|(encoded, parsed)| {
                    encoded.name == parsed.name
                        && encoded.columns.len() == parsed.columns.len()
                        && encoded.columns.iter().zip(&parsed.columns).all(
                            |(encoded_column, parsed_column)| {
                                self.schema.columns.iter().any(|column| {
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
        mode,
        storage,
        columns: column_specs,
        unique_constraints: unique_specs,
    } = spec;
    let mutation_id = MutationId(mint());
    let table_id = TableId(mint());
    let schema_revision_id = SchemaRevisionId(mint());
    let row_keyspace_id = RowKeyspaceId(mint());
    let columns = column_specs
        .iter()
        .map(|column| Column {
            id: ColumnId(mint()),
            name: column.name.clone(),
            declared_type: column.declared_type.clone(),
            not_null: column.not_null,
        })
        .collect::<Vec<_>>();
    let primary_key = PrimaryKey {
        columns: spec_primary_key_ids_from_columns(&column_specs, &columns)
            .expect("validated PRIMARY KEY columns exist"),
    };
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
        schema: TableSchema {
            mode,
            storage,
            columns,
            primary_key,
            unique_constraints,
            indexes: Vec::new(),
            foreign_keys: Vec::new(),
        },
    }
}

fn spec_primary_key_ids(spec: &CreateTableSpec, columns: &[Column]) -> Option<Vec<ColumnId>> {
    spec_primary_key_ids_from_columns(&spec.columns, columns)
}

fn spec_primary_key_ids_from_columns(
    specs: &[CreateColumn],
    columns: &[Column],
) -> Option<Vec<ColumnId>> {
    let mut ordered = specs
        .iter()
        .enumerate()
        .filter_map(|(column_index, column)| {
            column
                .primary_key
                .map(|position| (position, columns[column_index].id))
        })
        .collect::<Vec<_>>();
    ordered.sort_by_key(|(position, _)| *position);
    if ordered.is_empty()
        || ordered
            .iter()
            .enumerate()
            .any(|(position, (actual, _))| *actual != position)
    {
        return None;
    }
    Some(ordered.into_iter().map(|(_, id)| id).collect())
}

pub fn schema_log_key(id: MutationId) -> Key {
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

pub fn table_schema_key(table: TableId, revision: SchemaRevisionId) -> Key {
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

pub fn active_schema_revision_key(table: TableId) -> Key {
    Key::from_bytes([
        codes::ROOT,
        codes::TABLES,
        table.0.as_slice(),
        codes::ACTIVE_SCHEMA_REVISION,
    ])
    .expect("active schema revision key is bounded")
}

pub fn index_name_scope_key(name: &SqlName) -> Key {
    let component = name_component(name.canonical());
    Key::from_bytes([
        codes::ROOT,
        codes::SCHEMA,
        codes::NAMES,
        codes::INDEXES,
        codes::MAIN,
        component.as_slice(),
    ])
    .expect("index-name scope components are bounded and non-empty")
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

pub fn unique_keyspace_key(table: TableId, keyspace: UniqueKeyspaceId) -> Key {
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
    writer
        .field(TAG_TABLE_MODE, &[table.schema.mode.to_u8()])
        .expect("schema field length must fit in u32");
    writer
        .field(TAG_TABLE_STORAGE, &[table.schema.storage.to_u8()])
        .expect("schema field length must fit in u32");
    writer
        .field(
            TAG_PRIMARY_KEY,
            &encode_primary_key(&table.schema.primary_key),
        )
        .expect("schema field length must fit in u32");
    for column in &table.schema.columns {
        writer
            .field(TAG_COLUMN, &encode_column(column))
            .expect("schema field length must fit in u32");
    }
    for unique in &table.schema.unique_constraints {
        writer
            .field(TAG_UNIQUE_CONSTRAINT, &encode_unique_constraint(unique))
            .expect("schema field length must fit in u32");
    }
    for index in &table.schema.indexes {
        writer
            .field(TAG_INDEX_DEFINITION, &encode_index_definition(index))
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
        writer.u8(column.affinity(table.mode()).to_u8());
    }
    writer.finish()
}

fn encode_type_declaration(declaration: &TypeDeclaration) -> Vec<u8> {
    let mut writer = Writer::new();
    writer.u8(TYPE_DECLARATION_FRAME_VERSION);
    writer
        .field(TAG_TYPE_NAME, declaration.name.as_bytes())
        .expect("type declaration field length must fit in u32");
    for argument in &declaration.arguments {
        writer
            .field(TAG_TYPE_ARGUMENT, argument.as_bytes())
            .expect("type declaration field length must fit in u32");
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
        .field(
            TAG_COLUMN_TYPE,
            &encode_type_declaration(&column.declared_type),
        )
        .expect("schema field length must fit in u32");
    let mut flags = 0;
    if column.not_null {
        flags |= COLUMN_NOT_NULL;
    }
    writer
        .field(TAG_COLUMN_FLAGS, &[flags])
        .expect("schema field length must fit in u32");
    writer.finish()
}

fn encode_primary_key(primary_key: &PrimaryKey) -> Vec<u8> {
    let mut writer = Writer::new();
    for column in &primary_key.columns {
        writer
            .field(TAG_PRIMARY_COLUMN_ID, &column.0)
            .expect("schema field length must fit in u32");
    }
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

fn encode_index_definition(index: &IndexDefinition) -> Vec<u8> {
    let mut writer = Writer::new();
    writer
        .field(TAG_INDEX_KEYSPACE_ID, &index.keyspace_id.0)
        .expect("index field length must fit in u32");
    writer
        .field(TAG_INDEX_NAME, index.name.value().as_bytes())
        .expect("index field length must fit in u32");
    writer
        .field(TAG_INDEX_UNIQUE, &[u8::from(index.unique)])
        .expect("index field length must fit in u32");
    writer
        .field(TAG_INDEX_SQL, index.sql.as_bytes())
        .expect("index field length must fit in u32");
    writer
        .field(TAG_INDEX_ACTIVE, &[u8::from(index.active)])
        .expect("index field length must fit in u32");
    for column in &index.columns {
        writer
            .field(TAG_INDEX_COLUMN_ID, &column.0)
            .expect("index field length must fit in u32");
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
    InvalidColumnType,
    InvalidColumnFlags(u8),
    InvalidTableMode(u8),
    InvalidTableStorage(u8),
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
            Self::InvalidColumnType => f.write_str("invalid column type declaration"),
            Self::InvalidColumnFlags(value) => write!(f, "invalid column flags {value}"),
            Self::InvalidTableMode(value) => write!(f, "invalid table mode {value}"),
            Self::InvalidTableStorage(value) => write!(f, "invalid table storage {value}"),
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
    let (table_id, schema_revision_id, row_keyspace_id, name, schema) =
        create_table.ok_or(SchemaCodecError::MissingField(TAG_CREATE_TABLE))?;
    if !schema_identities_are_unique(
        mutation_id,
        table_id,
        schema_revision_id,
        row_keyspace_id,
        &schema,
    ) {
        return Err(SchemaCodecError::InvalidSchema);
    }
    Ok(CreateTable {
        mutation_id,
        sql,
        table_id,
        schema_revision_id,
        row_keyspace_id,
        name,
        schema,
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
        TableSchema,
    ),
    SchemaCodecError,
> {
    use homebase_core::reader::Reader;

    let mut reader = Reader::new(frame);
    let mut table_id = None;
    let mut schema_revision_id = None;
    let mut row_keyspace_id = None;
    let mut name = None;
    let mut mode = None;
    let mut storage = None;
    let mut primary_key = None;
    let mut columns = Vec::new();
    let mut unique_constraints = Vec::new();
    let mut indexes = Vec::new();
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
            TAG_TABLE_MODE => {
                let [value] = value else {
                    return Err(SchemaCodecError::InvalidLength);
                };
                set_once(
                    &mut mode,
                    TableMode::from_u8(*value).ok_or(SchemaCodecError::InvalidTableMode(*value))?,
                )?;
            }
            TAG_TABLE_STORAGE => {
                let [value] = value else {
                    return Err(SchemaCodecError::InvalidLength);
                };
                set_once(
                    &mut storage,
                    TableStorage::from_u8(*value)
                        .ok_or(SchemaCodecError::InvalidTableStorage(*value))?,
                )?;
            }
            TAG_PRIMARY_KEY => set_once(&mut primary_key, decode_primary_key(value)?)?,
            TAG_COLUMN => columns.push(decode_column(value)?),
            TAG_UNIQUE_CONSTRAINT => unique_constraints.push(decode_unique_constraint(value)?),
            TAG_INDEX_DEFINITION => indexes.push(decode_index_definition(value)?),
            _ => {}
        }
    }
    let mode = mode.ok_or(SchemaCodecError::MissingField(TAG_TABLE_MODE))?;
    let storage = storage.ok_or(SchemaCodecError::MissingField(TAG_TABLE_STORAGE))?;
    let primary_key = primary_key.ok_or(SchemaCodecError::MissingField(TAG_PRIMARY_KEY))?;
    let rowid_alias = storage == TableStorage::Rowid
        && primary_key.columns.len() == 1
        && primary_key.columns.first().is_some_and(|id| {
            columns
                .iter()
                .find(|column| column.id == *id)
                .is_some_and(|column| column.declared_type.is_exact_integer())
        });
    if columns.is_empty()
        || columns.iter().enumerate().any(|(index, column)| {
            columns[..index]
                .iter()
                .any(|seen| seen.name.canonical() == column.name.canonical())
        })
        || primary_key.columns.is_empty()
        || primary_key.columns.len() > MAX_KEY_PARTS
        || primary_key
            .columns
            .iter()
            .enumerate()
            .any(|(index, column)| {
                primary_key.columns[..index].contains(column)
                    || !columns.iter().any(|candidate| candidate.id == *column)
            })
        || (mode == TableMode::Strict
            && columns.iter().any(|column| {
                column.strict_type().is_none()
                    || (primary_key.columns.contains(&column.id) && !column.not_null)
            }))
        || (storage == TableStorage::WithoutRowid
            && primary_key.columns.iter().any(|id| {
                columns
                    .iter()
                    .find(|column| column.id == *id)
                    .is_none_or(|column| !column.not_null)
            }))
        || (!rowid_alias
            && primary_key.columns.iter().any(|id| {
                columns
                    .iter()
                    .find(|column| column.id == *id)
                    .is_none_or(|column| !column.not_null)
            }))
        || unique_constraints
            .iter()
            .enumerate()
            .any(|(index, unique)| {
                unique.columns.is_empty()
                    || unique.columns.len() > MAX_KEY_PARTS
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
        || indexes.iter().enumerate().any(|(index, definition)| {
            !definition.unique
                || definition.columns.is_empty()
                || definition.columns.len() > MAX_KEY_PARTS
                || definition.sql.is_empty()
                || indexes[..index]
                    .iter()
                    .any(|seen| seen.keyspace_id == definition.keyspace_id)
                || (definition.active
                    && indexes[..index].iter().any(|seen| {
                        seen.active && seen.name.canonical() == definition.name.canonical()
                    }))
                || definition
                    .columns
                    .iter()
                    .enumerate()
                    .any(|(column_index, column)| {
                        definition.columns[..column_index].contains(column)
                            || !columns.iter().any(|candidate| candidate.id == *column)
                    })
        })
        || (storage == TableStorage::Rowid
            && !rowid_alias
            && available_hidden_rowid_alias(columns.iter().map(|column| &column.name)).is_none())
    {
        return Err(SchemaCodecError::InvalidSchema);
    }
    Ok((
        table_id.ok_or(SchemaCodecError::MissingField(TAG_TABLE_ID))?,
        schema_revision_id.ok_or(SchemaCodecError::MissingField(TAG_SCHEMA_REVISION_ID))?,
        row_keyspace_id.ok_or(SchemaCodecError::MissingField(TAG_ROW_KEYSPACE_ID))?,
        name.ok_or(SchemaCodecError::MissingField(TAG_TABLE_NAME))?,
        TableSchema {
            mode,
            storage,
            columns,
            primary_key,
            unique_constraints,
            indexes,
            foreign_keys: Vec::new(),
        },
    ))
}

fn schema_identities_are_unique(
    mutation: MutationId,
    table: TableId,
    schema_revision: SchemaRevisionId,
    row_keyspace: RowKeyspaceId,
    schema: &TableSchema,
) -> bool {
    let mut identities = BTreeSet::new();
    std::iter::once(mutation.0)
        .chain([table.0, schema_revision.0, row_keyspace.0])
        .chain(schema.columns.iter().map(|column| column.id.0))
        .chain(
            schema
                .unique_constraints
                .iter()
                .map(|unique| unique.keyspace_id.0),
        )
        .chain(schema.indexes.iter().map(|index| index.keyspace_id.0))
        .all(|identity| identities.insert(identity))
}

fn decode_index_definition(frame: &[u8]) -> std::result::Result<IndexDefinition, SchemaCodecError> {
    use homebase_core::reader::Reader;

    let mut reader = Reader::new(frame);
    let mut keyspace_id = None;
    let mut name = None;
    let mut unique = None;
    let mut sql = None;
    let mut active = None;
    let mut columns = Vec::new();
    while let Some((tag, value)) = reader.field().map_err(|_| SchemaCodecError::Truncated)? {
        match tag {
            TAG_INDEX_KEYSPACE_ID => {
                set_once(&mut keyspace_id, UniqueKeyspaceId(uuid_bytes(value)?))?
            }
            TAG_INDEX_NAME => set_once(&mut name, decode_name(value)?)?,
            TAG_INDEX_UNIQUE => {
                let [value] = value else {
                    return Err(SchemaCodecError::InvalidLength);
                };
                let value = match value {
                    0 => false,
                    1 => true,
                    _ => return Err(SchemaCodecError::InvalidSchema),
                };
                set_once(&mut unique, value)?;
            }
            TAG_INDEX_COLUMN_ID => columns.push(ColumnId(uuid_bytes(value)?)),
            TAG_INDEX_SQL => set_once(
                &mut sql,
                String::from_utf8(value.to_vec()).map_err(|_| SchemaCodecError::InvalidUtf8)?,
            )?,
            TAG_INDEX_ACTIVE => {
                let [value] = value else {
                    return Err(SchemaCodecError::InvalidLength);
                };
                let value = match value {
                    0 => false,
                    1 => true,
                    _ => return Err(SchemaCodecError::InvalidSchema),
                };
                set_once(&mut active, value)?;
            }
            _ => {}
        }
    }
    Ok(IndexDefinition {
        keyspace_id: keyspace_id.ok_or(SchemaCodecError::MissingField(TAG_INDEX_KEYSPACE_ID))?,
        name: name.ok_or(SchemaCodecError::MissingField(TAG_INDEX_NAME))?,
        unique: unique.ok_or(SchemaCodecError::MissingField(TAG_INDEX_UNIQUE))?,
        columns,
        sql: sql.ok_or(SchemaCodecError::MissingField(TAG_INDEX_SQL))?,
        active: active.ok_or(SchemaCodecError::MissingField(TAG_INDEX_ACTIVE))?,
    })
}

fn decode_primary_key(frame: &[u8]) -> std::result::Result<PrimaryKey, SchemaCodecError> {
    use homebase_core::reader::Reader;

    let mut reader = Reader::new(frame);
    let mut columns = Vec::new();
    while let Some((tag, value)) = reader.field().map_err(|_| SchemaCodecError::Truncated)? {
        if tag == TAG_PRIMARY_COLUMN_ID {
            columns.push(ColumnId(uuid_bytes(value)?));
        }
    }
    if columns.is_empty() {
        return Err(SchemaCodecError::InvalidSchema);
    }
    Ok(PrimaryKey { columns })
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
            TAG_COLUMN_TYPE => set_once(&mut declared_type, decode_type_declaration(value)?)?,
            TAG_COLUMN_FLAGS => {
                let [value] = value else {
                    return Err(SchemaCodecError::InvalidLength);
                };
                if value & !COLUMN_NOT_NULL != 0 {
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
    })
}

fn decode_type_declaration(frame: &[u8]) -> std::result::Result<TypeDeclaration, SchemaCodecError> {
    use homebase_core::reader::Reader;

    let mut reader = Reader::new(frame);
    if reader.u8() != Some(TYPE_DECLARATION_FRAME_VERSION) {
        return Err(SchemaCodecError::InvalidColumnType);
    }
    let mut name = None;
    let mut arguments = Vec::new();
    while let Some((tag, value)) = reader.field().map_err(|_| SchemaCodecError::Truncated)? {
        match tag {
            TAG_TYPE_NAME => set_once(
                &mut name,
                String::from_utf8(value.to_vec()).map_err(|_| SchemaCodecError::InvalidUtf8)?,
            )?,
            TAG_TYPE_ARGUMENT => arguments.push(
                String::from_utf8(value.to_vec()).map_err(|_| SchemaCodecError::InvalidUtf8)?,
            ),
            _ => {}
        }
    }
    let declaration = TypeDeclaration::new(
        name.ok_or(SchemaCodecError::MissingField(TAG_TYPE_NAME))?,
        arguments,
    );
    if declaration.name.is_empty()
        || declaration.arguments.len() > 2
        || declaration.arguments.iter().any(String::is_empty)
    {
        return Err(SchemaCodecError::InvalidColumnType);
    }
    Ok(declaration)
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
    if admitted_log_key != &schema_log_key(created.mutation_id) {
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

fn validate_index_sql(created: &CreateTable) -> std::result::Result<(), SchemaCodecError> {
    for index in created.indexes() {
        let super::sql::ValidatedExecute::CreateUniqueIndex(spec) =
            super::sql::validate_execute(index.sql()).map_err(|_| SchemaCodecError::InvalidSql)?
        else {
            return Err(SchemaCodecError::InvalidSql);
        };
        if !index.is_unique()
            || spec.name != *index.name()
            || spec.table != *created.table_name_identity()
            || spec.columns.len() != index.columns().len()
            || spec.columns.iter().zip(index.columns()).any(|(name, id)| {
                created
                    .column_named(name)
                    .is_none_or(|column| column.id() != *id)
            })
        {
            return Err(SchemaCodecError::SqlMismatch);
        }
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
            mode: TableMode::Ordinary,
            storage: TableStorage::Rowid,
            columns: vec![
                CreateColumn {
                    name: SqlName::new("id".into()),
                    declared_type: TypeDeclaration::integer(),
                    not_null: false,
                    primary_key: Some(0),
                },
                CreateColumn {
                    name: SqlName::new("body".into()),
                    declared_type: TypeDeclaration::text(),
                    not_null: true,
                    primary_key: None,
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
                mode: TableMode::Ordinary,
                storage: TableStorage::Rowid,
                columns: vec![
                    CreateColumn {
                        name: SqlName::new("id".into()),
                        declared_type: TypeDeclaration::integer(),
                        not_null: false,
                        primary_key: Some(0),
                    },
                    CreateColumn {
                        name: SqlName::new("organization".into()),
                        declared_type: TypeDeclaration::text(),
                        not_null: false,
                        primary_key: None,
                    },
                    CreateColumn {
                        name: SqlName::new("email".into()),
                        declared_type: TypeDeclaration::text(),
                        not_null: false,
                        primary_key: None,
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
                mode: TableMode::Ordinary,
                storage: TableStorage::Rowid,
                columns: vec![
                    CreateColumn {
                        name: SqlName::new("id".into()),
                        declared_type: TypeDeclaration::integer(),
                        not_null: false,
                        primary_key: Some(0),
                    },
                    CreateColumn {
                        name: SqlName::new("tenant".into()),
                        declared_type: TypeDeclaration::text(),
                        not_null: false,
                        primary_key: None,
                    },
                    CreateColumn {
                        name: SqlName::new("email".into()),
                        declared_type: TypeDeclaration::text(),
                        not_null: false,
                        primary_key: None,
                    },
                    CreateColumn {
                        name: SqlName::new("username".into()),
                        declared_type: TypeDeclaration::text(),
                        not_null: false,
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
        assert_eq!(lowered.mutations.len(), 7);
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
                .contains(lowered.mutations[6].key())
        );

        let admitted = admit(lowered.mutations);
        assert_eq!(CreateTable::from_homebase(&admitted).unwrap(), created);
    }

    #[test]
    fn composite_unique_constraints_roundtrip_with_their_own_keyspace() {
        let created = deterministic_unique_create();
        let unique = &created.schema.unique_constraints[0];
        assert_eq!(unique.keyspace_id.0, test_uuid(8));
        assert_eq!(
            unique.columns,
            vec![created.schema.columns[1].id, created.schema.columns[2].id]
        );

        let decoded = CreateTable::decode(&created.encode()).unwrap();
        assert_eq!(decoded, created);
        let lowered = created.to_homebase();
        assert_eq!(lowered.mutations.len(), 8);
        assert_eq!(
            lowered.mutations[6].key(),
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
        assert_eq!(created.schema.unique_constraints.len(), 4);
        assert_eq!(
            created
                .schema
                .unique_constraints
                .iter()
                .map(|unique| unique.keyspace_id.0)
                .collect::<Vec<_>>(),
            [test_uuid(9), test_uuid(10), test_uuid(11), test_uuid(12)]
        );
        assert_eq!(
            created
                .schema
                .unique_constraints
                .iter()
                .map(|unique| unique.columns.clone())
                .collect::<Vec<_>>(),
            [
                vec![created.schema.columns[2].id],
                vec![created.schema.columns[3].id],
                vec![created.schema.columns[1].id, created.schema.columns[2].id],
                vec![created.schema.columns[1].id, created.schema.columns[3].id],
            ]
        );

        assert_eq!(CreateTable::decode(&created.encode()).unwrap(), created);
        let lowered = created.to_homebase();
        assert_eq!(lowered.mutations.len(), 11);
        for (mutation, unique) in lowered.mutations[6..10]
            .iter()
            .zip(&created.schema.unique_constraints)
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
        duplicate_column.schema.unique_constraints[0]
            .columns
            .push(duplicate_column.schema.columns[1].id);
        assert_eq!(
            CreateTable::decode(&duplicate_column.encode()),
            Err(SchemaCodecError::InvalidSchema)
        );

        let mut unknown_column = deterministic_unique_create();
        unknown_column.schema.unique_constraints[0].columns[0] = ColumnId(test_uuid(99));
        assert_eq!(
            CreateTable::decode(&unknown_column.encode()),
            Err(SchemaCodecError::InvalidSchema)
        );

        let mut duplicate_keyspace = deterministic_overlapping_unique_create();
        duplicate_keyspace.schema.unique_constraints[1].keyspace_id =
            duplicate_keyspace.schema.unique_constraints[0].keyspace_id;
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
    fn type_declaration_codec_roundtrips_names_sizes_and_numeric_affinity() {
        let declaration = TypeDeclaration::new("decimal".into(), vec!["10".into(), "2".into()]);
        assert_eq!(
            decode_type_declaration(&encode_type_declaration(&declaration)).unwrap(),
            declaration
        );
        assert_eq!(declaration.name(), "DECIMAL");
        assert_eq!(declaration.arguments(), ["10", "2"]);
        assert_eq!(declaration.affinity(), Affinity::Numeric);
        assert_eq!(declaration.to_sql(), "DECIMAL(10, 2)");

        assert_eq!(
            decode_type_declaration(&[]),
            Err(SchemaCodecError::InvalidColumnType)
        );
        let mut missing_name = Writer::new();
        missing_name.u8(TYPE_DECLARATION_FRAME_VERSION);
        missing_name
            .field(TAG_TYPE_ARGUMENT, b"10")
            .expect("test field is bounded");
        assert_eq!(
            decode_type_declaration(&missing_name.finish()),
            Err(SchemaCodecError::MissingField(TAG_TYPE_NAME))
        );
    }

    #[test]
    fn complete_schema_codec_preserves_ordinary_sqlite_declarations() {
        let sql = "CREATE TABLE measurements (
            id INTEGER PRIMARY KEY,
            label VARCHAR(40),
            amount DECIMAL(10, 2),
            enabled BOOLEAN
        )";
        let super::super::sql::ValidatedExecute::CreateTable(spec) =
            super::super::sql::validate_execute(sql).unwrap()
        else {
            unreachable!()
        };
        let created = CreateTable::new(sql, spec);
        let decoded = CreateTable::decode(&created.encode()).unwrap();

        assert_eq!(decoded, created);
        assert_eq!(
            decoded
                .columns()
                .iter()
                .map(|column| (
                    column.declared_type().to_sql(),
                    column.affinity(decoded.mode()),
                    decoded.is_rowid_alias(column.id()),
                ))
                .collect::<Vec<_>>(),
            [
                ("INTEGER".into(), Affinity::Integer, true),
                ("VARCHAR(40)".into(), Affinity::Text, false),
                ("DECIMAL(10, 2)".into(), Affinity::Numeric, false),
                ("BOOLEAN".into(), Affinity::Numeric, false),
            ]
        );
    }

    #[test]
    fn strict_table_mode_roundtrips_and_is_part_of_schema_identity() {
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
        let decoded = CreateTable::decode(&created.encode()).unwrap();

        assert_eq!(decoded, created);
        assert_eq!(decoded.mode(), TableMode::Strict);
        assert!(decoded.primary_key_columns().next().unwrap().is_not_null());
        assert_eq!(
            decoded.schema.columns.last().unwrap().strict_type(),
            Some(StrictType::Any)
        );
        assert_eq!(
            decoded
                .schema
                .columns
                .last()
                .unwrap()
                .affinity(decoded.mode()),
            Affinity::Blob
        );

        let mut mode_mismatch = created.clone();
        mode_mismatch.schema.mode = TableMode::Ordinary;
        assert_eq!(
            CreateTable::decode(&mode_mismatch.encode()),
            Err(SchemaCodecError::SqlMismatch)
        );

        let mut invalid_type = created;
        invalid_type.schema.columns[1].declared_type =
            TypeDeclaration::new("DECIMAL".into(), Vec::new());
        assert_eq!(
            CreateTable::decode(&invalid_type.encode()),
            Err(SchemaCodecError::InvalidSchema)
        );
    }

    #[test]
    fn table_schema_owns_ordered_composite_primary_and_associated_schema() {
        let sql = "CREATE TABLE memberships (
            tenant TEXT,
            member INTEGER,
            body TEXT,
            UNIQUE (tenant, body),
            PRIMARY KEY (member, tenant)
        ) WITHOUT ROWID";
        let super::super::sql::ValidatedExecute::CreateTable(spec) =
            super::super::sql::validate_execute(sql).unwrap()
        else {
            unreachable!()
        };
        let created = CreateTable::new(sql, spec);
        let schema = created.schema();

        assert_eq!(schema.storage(), TableStorage::WithoutRowid);
        assert_eq!(
            created
                .primary_key_columns()
                .map(|column| column.name().value())
                .collect::<Vec<_>>(),
            ["member", "tenant"]
        );
        assert_eq!(schema.primary_key().columns().len(), 2);
        assert_eq!(schema.unique_constraints().len(), 1);
        assert!(schema.indexes().is_empty());
        assert!(schema.foreign_keys().is_empty());
        assert_eq!(CreateTable::decode(&created.encode()).unwrap(), created);

        let mut duplicate_primary = created;
        duplicate_primary.schema.primary_key.columns[1] =
            duplicate_primary.schema.primary_key.columns[0];
        assert_eq!(
            CreateTable::decode(&duplicate_primary.encode()),
            Err(SchemaCodecError::InvalidSchema)
        );
    }

    #[test]
    fn decoder_rejects_duplicate_names_and_reused_schema_identities() {
        let created = deterministic_create("notes");

        let mut duplicate_name = created.clone();
        duplicate_name.schema.columns[1].name = SqlName::new("ID".into());
        assert_eq!(
            CreateTable::decode(&duplicate_name.encode()),
            Err(SchemaCodecError::InvalidSchema)
        );

        let mut duplicate_column_id = created.clone();
        duplicate_column_id.schema.columns[1].id = duplicate_column_id.schema.columns[0].id;
        assert_eq!(
            CreateTable::decode(&duplicate_column_id.encode()),
            Err(SchemaCodecError::InvalidSchema)
        );

        let mut reused_identity = created;
        reused_identity.row_keyspace_id = RowKeyspaceId(reused_identity.table_id.0);
        assert_eq!(
            CreateTable::decode(&reused_identity.encode()),
            Err(SchemaCodecError::InvalidSchema)
        );
    }

    #[test]
    fn decoder_rejects_rowid_tables_without_a_stable_hidden_alias() {
        let mut spec = definition("shadowed");
        spec.columns[0] = CreateColumn {
            name: SqlName::new("key".into()),
            declared_type: TypeDeclaration::text(),
            not_null: true,
            primary_key: Some(0),
        };
        for name in ["rowid", "oid", "_rowid_"] {
            spec.columns.push(CreateColumn {
                name: SqlName::new(name.into()),
                declared_type: TypeDeclaration::text(),
                not_null: false,
                primary_key: None,
            });
        }
        let created = CreateTable::new(
            "CREATE TABLE shadowed (
                key TEXT NOT NULL PRIMARY KEY,
                body TEXT NOT NULL,
                rowid TEXT,
                oid TEXT,
                _rowid_ TEXT
            )",
            spec,
        );

        assert_eq!(
            CreateTable::decode(&created.encode()),
            Err(SchemaCodecError::InvalidSchema)
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
            .chain(created.schema.columns.iter().map(|column| column.id.0))
        {
            let uuid = Uuid::from_bytes(bytes);
            assert_eq!(uuid.get_version(), Some(Version::Random));
            assert_eq!(uuid.get_variant(), Variant::RFC4122);
        }
    }
}
