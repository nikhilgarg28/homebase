//! Explicit index schema operations and durable entry backfills.

use std::collections::BTreeMap;
use std::fmt;

use homebase_core::key::Key;
use homebase_core::reader::Reader;
use homebase_core::tag::Mutation;
use homebase_core::writer::Writer;
use rusqlite::Connection;
use uuid::{Uuid, Variant, Version};

use super::catalog;
use super::codes;
use super::row::{IndexBackfillEntry, backfill_unique_index, primary_index_prefix};
use super::schema::{
    CreateTable, IndexId, MutationId, NamedIndex, SchemaRevisionId, active_schema_revision_key,
    column_index_dependency_key, column_name_scope_key, index_definition_key, schema_log_key,
    schema_object_name_scope_key, table_schema_key, write_revision_key,
};
use super::sql::{CreateIndexSpec, CreateIndexTerm, DropIndexSpec, ValidatedExecute};
use crate::commit::footprint::ConflictFootprint;
use crate::{Error, Result};

const INDEX_OPERATION_VERSION: u8 = 2;
const TAG_MUTATION_ID: u8 = 1;
const TAG_SQL: u8 = 2;
const TAG_BEFORE: u8 = 3;
const TAG_AFTER: u8 = 4;
const TAG_ACTION: u8 = 5;
const TAG_INDEX: u8 = 6;
const TAG_BACKFILL: u8 = 7;
const CREATE: u8 = 1;
const DROP: u8 = 2;

const BACKFILL_VERSION: u8 = 1;
const TAG_BACKFILL_KEY: u8 = 1;
const TAG_BACKFILL_OWNER: u8 = 2;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum IndexAction {
    Create,
    Drop,
}

/// One self-contained explicit-index DDL operation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IndexOperation {
    mutation_id: MutationId,
    sql: String,
    before: CreateTable,
    after: CreateTable,
    action: IndexAction,
    index: NamedIndex,
    backfill: Vec<IndexBackfillEntry>,
}

/// Homebase mutations and conflict footprint for one explicit-index change.
pub struct IndexHomebaseOp {
    pub mutations: Vec<Mutation>,
    pub footprint: ConflictFootprint,
}

impl IndexOperation {
    /// Prepare a CREATE INDEX after SQLite has validated and built it.
    pub fn prepare_create(
        connection: &Connection,
        sql: &str,
        spec: &CreateIndexSpec,
    ) -> Result<Self> {
        if catalog::index_by_name(connection, &spec.name)?.is_some() {
            return Err(Error::InvalidDatabase(
                "schema catalog already contains this index",
            ));
        }
        let before = catalog::by_name(connection, spec.table.value())?.ok_or(
            Error::UnsupportedSql("CREATE INDEX target has no synchronized schema identity"),
        )?;
        let index = before.prepare_named_index(connection, sql, spec)?;
        let backfill = if index.is_unique() {
            backfill_unique_index(connection, &before, &index)?
        } else {
            Vec::new()
        };
        let after = before.with_added_index(
            SchemaRevisionId::from_bytes(Uuid::new_v4().into_bytes()),
            index.clone(),
        )?;
        let operation = Self {
            mutation_id: MutationId::from_bytes(Uuid::new_v4().into_bytes()),
            sql: sql.to_owned(),
            before,
            after,
            action: IndexAction::Create,
            index,
            backfill,
        };
        operation.validate().map_err(invalid_operation)?;
        Ok(operation)
    }

    /// Prepare a DROP INDEX before SQLite removes its physical index.
    pub fn prepare_drop(connection: &Connection, sql: &str, spec: &DropIndexSpec) -> Result<Self> {
        let (before, index) = catalog::index_by_name(connection, &spec.name)?.ok_or(
            Error::UnsupportedSql("DROP INDEX target has no synchronized schema identity"),
        )?;
        ensure_not_referenced(connection, &before, &index)?;
        let after = before
            .with_retired_index(
                SchemaRevisionId::from_bytes(Uuid::new_v4().into_bytes()),
                &spec.name,
            )?
            .expect("catalog lookup found the index");
        let operation = Self {
            mutation_id: MutationId::from_bytes(Uuid::new_v4().into_bytes()),
            sql: sql.to_owned(),
            before,
            after,
            action: IndexAction::Drop,
            index,
            backfill: Vec::new(),
        };
        operation.validate().map_err(invalid_operation)?;
        Ok(operation)
    }

    pub fn to_homebase(&self) -> Result<IndexHomebaseOp> {
        self.validate().map_err(invalid_operation)?;
        let name = schema_object_name_scope_key(self.index.name());
        let schema_head = active_schema_revision_key(self.after.table_id());
        let mut footprint = ConflictFootprint::new();
        footprint.add_constraint(name.clone());
        footprint.add_constraint(schema_head.clone());
        footprint.add_write(schema_head.clone());
        if self.action == IndexAction::Create {
            for dependency in self.create_dependencies()? {
                footprint.add_constraint(column_name_scope_key(self.after.table_id(), &dependency));
            }
        }
        let mut mutations = vec![Mutation::Set {
            key: schema_log_key(self.mutation_id),
            value: self.encode(),
        }];
        match self.action {
            IndexAction::Create => mutations.push(Mutation::Set {
                key: name,
                value: index_name_value(self.after.table_id().as_bytes(), self.index.index_id()),
            }),
            IndexAction::Drop => mutations.push(Mutation::Delete { key: name }),
        }
        mutations.push(Mutation::Set {
            key: table_schema_key(self.after.table_id(), self.after.schema_revision_id()),
            value: self.after.encode(),
        });
        mutations.push(Mutation::Set {
            key: schema_head,
            value: self.after.schema_revision_id().as_bytes().to_vec(),
        });

        for dependency in self.index.dependencies() {
            let key = column_index_dependency_key(
                self.after.table_id(),
                *dependency,
                self.index.index_id(),
            );
            footprint.add_constraint(key.clone());
            footprint.add_write(key.clone());
            mutations.push(match self.action {
                IndexAction::Create => Mutation::Set {
                    key,
                    value: self.index.index_id().as_bytes().to_vec(),
                },
                IndexAction::Drop => Mutation::Delete { key },
            });
        }

        if self.action == IndexAction::Create {
            mutations.push(Mutation::Set {
                key: index_definition_key(self.after.table_id(), self.index.index_id()),
                value: self.index.encode(),
            });
        }

        if self.action == IndexAction::Create && self.index.is_unique() {
            for entry in &self.backfill {
                mutations.push(Mutation::Set {
                    key: entry.key.clone(),
                    value: entry.value.clone(),
                });
            }
            let write_revision = write_revision_key(self.after.table_id());
            footprint.add_write(write_revision.clone());
            footprint.add_constraint(primary_index_prefix(&self.before));
            mutations.push(Mutation::Set {
                key: write_revision,
                value: self.mutation_id.as_bytes().to_vec(),
            });
        } else if self.action == IndexAction::Drop && self.index.is_unique() {
            footprint.add_constraint(write_revision_key(self.after.table_id()));
        }
        Ok(IndexHomebaseOp {
            mutations,
            footprint,
        })
    }

    pub fn apply(&self, connection: &Connection) -> Result<()> {
        self.validate().map_err(invalid_operation)?;
        let current = catalog::by_id(connection, self.before.table_id())?.ok_or(
            Error::InvalidDatabase("index operation references an unknown table identity"),
        )?;
        if self.action == IndexAction::Drop {
            if current.index_named(self.index.name()) != Some(&self.index) {
                return Err(Error::InvalidDatabase(
                    "index operation no longer matches the schema catalog",
                ));
            }
            ensure_not_referenced(connection, &current, &self.index)?;
        } else if current.index_named(self.index.name()).is_some() {
            return Err(Error::InvalidDatabase(
                "index operation no longer matches the schema catalog",
            ));
        }
        match self.action {
            IndexAction::Create => {
                let table = catalog::name_by_id(connection, self.before.table_id())?.ok_or(
                    Error::InvalidDatabase("index operation references an unknown table identity"),
                )?;
                connection.execute(
                    &self
                        .index
                        .materialization_sql(connection, &self.after, &table)?,
                    (),
                )?;
            }
            IndexAction::Drop => {
                connection.execute(&self.sql, ())?;
            }
        }
        self.record_catalog(connection)
    }

    pub fn record_catalog(&self, connection: &Connection) -> Result<()> {
        let current = catalog::by_id(connection, self.before.table_id())?.ok_or(
            Error::InvalidDatabase("index operation references an unknown table identity"),
        )?;
        let folded = match self.action {
            IndexAction::Create => current.fold_added_index(&self.index)?,
            IndexAction::Drop => current.fold_retired_index(&self.index)?,
        };
        catalog::replace(connection, &folded)
    }

    pub fn rollback(&self, connection: &Connection) -> Result<()> {
        self.validate().map_err(invalid_operation)?;
        let current = catalog::by_id(connection, self.after.table_id())?.ok_or(
            Error::InvalidDatabase("pending index operation references an unknown table identity"),
        )?;
        match self.action {
            IndexAction::Create => {
                if current.index_named(self.index.name()) != Some(&self.index) {
                    return Err(Error::InvalidDatabase(
                        "pending index operation no longer matches SQLite state",
                    ));
                }
                connection.execute_batch(&format!(
                    "DROP INDEX {}",
                    quote_identifier(self.index.name().value())
                ))?;
            }
            IndexAction::Drop => {
                if current.index_named(self.index.name()).is_some() {
                    return Err(Error::InvalidDatabase(
                        "pending index operation no longer matches SQLite state",
                    ));
                }
                let table = catalog::name_by_id(connection, self.after.table_id())?.ok_or(
                    Error::InvalidDatabase(
                        "pending index operation references an unknown table identity",
                    ),
                )?;
                connection.execute(
                    &self
                        .index
                        .materialization_sql(connection, &current, &table)?,
                    (),
                )?;
            }
        }
        let folded = match self.action {
            IndexAction::Create => current.fold_removed_index(&self.index)?,
            IndexAction::Drop => current.fold_restored_index(&self.index)?,
        };
        catalog::replace(connection, &folded)
    }

    fn create_dependencies(&self) -> Result<Vec<super::schema::SqlName>> {
        let ValidatedExecute::CreateIndex(spec) = super::sql::validate_execute(&self.sql)? else {
            return Err(Error::InvalidMultiliteOp(
                "CREATE index operation has invalid SQL provenance".into(),
            ));
        };
        let mut dependencies = BTreeMap::new();
        for term in spec.terms {
            match term {
                CreateIndexTerm::Column { name, .. } => {
                    dependencies.insert(name.canonical().to_vec(), name);
                }
                CreateIndexTerm::Expression { expression, .. } => {
                    for name in expression.referenced_columns() {
                        dependencies.insert(name.canonical().to_vec(), name);
                    }
                }
            }
        }
        if let Some(predicate) = spec.predicate {
            for name in predicate.referenced_columns() {
                dependencies.insert(name.canonical().to_vec(), name);
            }
        }
        Ok(dependencies.into_values().collect())
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut writer = Writer::new();
        writer.u8(INDEX_OPERATION_VERSION);
        writer
            .field(TAG_MUTATION_ID, &self.mutation_id.as_bytes())
            .expect("index operation field fits in u32");
        writer
            .field(TAG_SQL, self.sql.as_bytes())
            .expect("index operation field fits in u32");
        writer
            .field(TAG_BEFORE, &self.before.encode())
            .expect("index operation field fits in u32");
        writer
            .field(TAG_AFTER, &self.after.encode())
            .expect("index operation field fits in u32");
        writer
            .field(
                TAG_ACTION,
                &[match self.action {
                    IndexAction::Create => CREATE,
                    IndexAction::Drop => DROP,
                }],
            )
            .expect("index operation field fits in u32");
        writer
            .field(TAG_INDEX, &self.index.encode())
            .expect("index operation field fits in u32");
        for entry in &self.backfill {
            writer
                .field(TAG_BACKFILL, &encode_backfill(entry))
                .expect("index operation field fits in u32");
        }
        writer.finish()
    }

    pub fn decode(frame: &[u8]) -> std::result::Result<Self, IndexCodecError> {
        let mut reader = Reader::new(frame);
        if reader.u8() != Some(INDEX_OPERATION_VERSION) {
            return Err(IndexCodecError::UnknownVersion);
        }
        let mut mutation_id = None;
        let mut sql = None;
        let mut before = None;
        let mut after = None;
        let mut action = None;
        let mut index = None;
        let mut backfill = Vec::new();
        while let Some((tag, value)) = reader.field().map_err(|_| IndexCodecError::Truncated)? {
            match tag {
                TAG_MUTATION_ID => {
                    set_once(&mut mutation_id, MutationId::from_bytes(uuid_bytes(value)?))?
                }
                TAG_SQL => set_once(
                    &mut sql,
                    String::from_utf8(value.to_vec()).map_err(|_| IndexCodecError::InvalidUtf8)?,
                )?,
                TAG_BEFORE => set_once(
                    &mut before,
                    CreateTable::decode(value)
                        .map_err(|_| IndexCodecError::InvalidTableDefinition)?,
                )?,
                TAG_AFTER => set_once(
                    &mut after,
                    CreateTable::decode(value)
                        .map_err(|_| IndexCodecError::InvalidTableDefinition)?,
                )?,
                TAG_ACTION => {
                    let [value] = value else {
                        return Err(IndexCodecError::InvalidLength);
                    };
                    let value = match *value {
                        CREATE => IndexAction::Create,
                        DROP => IndexAction::Drop,
                        _ => return Err(IndexCodecError::InvalidAction),
                    };
                    set_once(&mut action, value)?;
                }
                TAG_INDEX => set_once(
                    &mut index,
                    NamedIndex::decode(value)
                        .map_err(|_| IndexCodecError::InvalidIndexDefinition)?,
                )?,
                TAG_BACKFILL => backfill.push(decode_backfill(value)?),
                _ => {}
            }
        }
        let operation = Self {
            mutation_id: mutation_id.ok_or(IndexCodecError::MissingField(TAG_MUTATION_ID))?,
            sql: sql.ok_or(IndexCodecError::MissingField(TAG_SQL))?,
            before: before.ok_or(IndexCodecError::MissingField(TAG_BEFORE))?,
            after: after.ok_or(IndexCodecError::MissingField(TAG_AFTER))?,
            action: action.ok_or(IndexCodecError::MissingField(TAG_ACTION))?,
            index: index.ok_or(IndexCodecError::MissingField(TAG_INDEX))?,
            backfill,
        };
        operation.validate()?;
        Ok(operation)
    }

    fn validate(&self) -> std::result::Result<(), IndexCodecError> {
        if self.before.table_id() != self.after.table_id()
            || self.before.table_name_identity() != self.after.table_name_identity()
            || self.before.primary_index_id() != self.after.primary_index_id()
        {
            return Err(IndexCodecError::InvalidEvolution);
        }
        match self.action {
            IndexAction::Create => {
                let ValidatedExecute::CreateIndex(spec) =
                    super::sql::validate_execute(&self.sql)
                        .map_err(|_| IndexCodecError::InvalidSql)?
                else {
                    return Err(IndexCodecError::InvalidSql);
                };
                let expected_after = self
                    .before
                    .with_added_index(self.after.schema_revision_id(), self.index.clone())
                    .map_err(|_| IndexCodecError::InvalidEvolution)?;
                if self.before.schema_revision_id() == self.after.schema_revision_id()
                    || expected_after != self.after
                    || !self.before.named_index_matches_spec(&self.index, &spec)
                    || self.before.index_named(self.index.name()).is_some()
                    || self.after.index_named(self.index.name()) != Some(&self.index)
                    || (!self.index.is_unique() && !self.backfill.is_empty())
                    || (self.index.is_unique()
                        && (self.backfill.iter().any(|entry| {
                            let owner = Key::decode(&entry.value);
                            !entry
                                .key
                                .starts_with(&unique_prefix(&self.after, &self.index))
                                || !owner.is_ok_and(|owner| {
                                    owner.starts_with(&primary_index_prefix(&self.after))
                                })
                        }) || self
                            .backfill
                            .windows(2)
                            .any(|entries| entries[0].key >= entries[1].key)))
                {
                    return Err(IndexCodecError::InvalidEvolution);
                }
            }
            IndexAction::Drop => {
                let ValidatedExecute::DropIndex(spec) = super::sql::validate_execute(&self.sql)
                    .map_err(|_| IndexCodecError::InvalidSql)?
                else {
                    return Err(IndexCodecError::InvalidSql);
                };
                let expected_after = self
                    .before
                    .with_retired_index(self.after.schema_revision_id(), self.index.name())
                    .map_err(|_| IndexCodecError::InvalidEvolution)?
                    .ok_or(IndexCodecError::InvalidEvolution)?;
                if self.before.schema_revision_id() == self.after.schema_revision_id()
                    || expected_after != self.after
                    || spec.name != *self.index.name()
                    || self.before.index_named(self.index.name()) != Some(&self.index)
                    || self.after.index_named(self.index.name()).is_some()
                    || !self.after.indexes().contains(&self.index.retired())
                    || !self.backfill.is_empty()
                {
                    return Err(IndexCodecError::InvalidEvolution);
                }
            }
        }
        Ok(())
    }
}

fn ensure_not_referenced(
    connection: &Connection,
    table: &CreateTable,
    index: &NamedIndex,
) -> Result<()> {
    if catalog::incoming_foreign_keys(connection, table.table_id())?
        .iter()
        .any(|(_, foreign_key)| {
            index.is_unique() && foreign_key.referenced_index() == index.index_id()
        })
    {
        return Err(Error::UnsupportedSql(
            "cannot drop a UNIQUE index referenced by a foreign key",
        ));
    }
    Ok(())
}

fn unique_prefix(table: &CreateTable, index: &NamedIndex) -> Key {
    debug_assert!(index.is_unique());
    Key::from_bytes([
        codes::ROOT,
        codes::TABLES,
        table.table_id().as_bytes().as_slice(),
        codes::UNIQUE,
        index.index_id().as_bytes().as_slice(),
    ])
    .expect("UNIQUE entry prefix is bounded")
}

fn index_name_value(table: [u8; 16], index: IndexId) -> Vec<u8> {
    let mut value = Vec::with_capacity(32);
    value.extend_from_slice(&table);
    value.extend_from_slice(&index.as_bytes());
    value
}

fn encode_backfill(entry: &IndexBackfillEntry) -> Vec<u8> {
    let mut writer = Writer::new();
    writer.u8(BACKFILL_VERSION);
    writer
        .field(TAG_BACKFILL_KEY, &entry.key.encode())
        .expect("backfill field fits in u32");
    writer
        .field(TAG_BACKFILL_OWNER, &entry.value)
        .expect("backfill field fits in u32");
    writer.finish()
}

fn decode_backfill(frame: &[u8]) -> std::result::Result<IndexBackfillEntry, IndexCodecError> {
    let mut reader = Reader::new(frame);
    if reader.u8() != Some(BACKFILL_VERSION) {
        return Err(IndexCodecError::UnknownVersion);
    }
    let mut key = None;
    let mut owner = None;
    while let Some((tag, value)) = reader.field().map_err(|_| IndexCodecError::Truncated)? {
        match tag {
            TAG_BACKFILL_KEY => set_once(
                &mut key,
                Key::decode(value).map_err(|_| IndexCodecError::InvalidKey)?,
            )?,
            TAG_BACKFILL_OWNER => set_once(&mut owner, value.to_vec())?,
            _ => {}
        }
    }
    Ok(IndexBackfillEntry {
        key: key.ok_or(IndexCodecError::MissingField(TAG_BACKFILL_KEY))?,
        value: owner.ok_or(IndexCodecError::MissingField(TAG_BACKFILL_OWNER))?,
    })
}

fn set_once<T>(slot: &mut Option<T>, value: T) -> std::result::Result<(), IndexCodecError> {
    if slot.replace(value).is_some() {
        Err(IndexCodecError::DuplicateField)
    } else {
        Ok(())
    }
}

fn uuid_bytes(value: &[u8]) -> std::result::Result<[u8; 16], IndexCodecError> {
    let bytes = value
        .try_into()
        .map_err(|_| IndexCodecError::InvalidLength)?;
    let uuid = Uuid::from_bytes(bytes);
    if uuid.get_version() != Some(Version::Random) || uuid.get_variant() != Variant::RFC4122 {
        return Err(IndexCodecError::InvalidUuid);
    }
    Ok(bytes)
}

fn invalid_operation(error: IndexCodecError) -> Error {
    Error::InvalidMultiliteOp(error.to_string())
}

fn quote_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IndexCodecError {
    UnknownVersion,
    Truncated,
    DuplicateField,
    MissingField(u8),
    InvalidLength,
    InvalidUtf8,
    InvalidUuid,
    InvalidAction,
    InvalidSql,
    InvalidTableDefinition,
    InvalidIndexDefinition,
    InvalidKey,
    InvalidEvolution,
}

impl fmt::Display for IndexCodecError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{self:?}")
    }
}

#[cfg(test)]
mod tests {
    use rusqlite::Connection;

    use super::*;
    use crate::commit::footprint::assert_explicit_range_assertions;
    use crate::database::schema::{
        CreateColumn, CreateTableSpec, IndexOrder, IndexTerm, TableStorage, TypeDeclaration,
    };
    use crate::database::sql::CreateIndexTerm;

    fn table() -> CreateTable {
        CreateTable::new(
            "CREATE TABLE notes (id INTEGER PRIMARY KEY, tenant TEXT, slug TEXT)",
            CreateTableSpec {
                name: super::super::schema::SqlName::new("notes".into()),
                mode: Default::default(),
                storage: TableStorage::Rowid,
                columns: vec![
                    CreateColumn {
                        name: super::super::schema::SqlName::new("id".into()),
                        declared_type: TypeDeclaration::integer(),
                        not_null: false,
                        not_null_name: None,
                        default: None,
                        primary_key: Some(0),
                    },
                    CreateColumn {
                        name: super::super::schema::SqlName::new("tenant".into()),
                        declared_type: TypeDeclaration::text(),
                        not_null: false,
                        not_null_name: None,
                        default: None,
                        primary_key: None,
                    },
                    CreateColumn {
                        name: super::super::schema::SqlName::new("slug".into()),
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
        )
    }

    fn connection() -> (Connection, CreateTable) {
        let connection = Connection::open_in_memory().unwrap();
        catalog::initialize(&connection).unwrap();
        let table = table();
        connection.execute(table.sql(), ()).unwrap();
        catalog::insert(&connection, &table).unwrap();
        connection
            .execute(
                "INSERT INTO notes VALUES
                 (1, 'one', 'same'), (2, 'two', 'same'), (3, NULL, 'ignored')",
                (),
            )
            .unwrap();
        (connection, table)
    }

    fn create_spec() -> CreateIndexSpec {
        CreateIndexSpec {
            unique: true,
            name: super::super::schema::SqlName::new("notes_tenant_slug".into()),
            table: super::super::schema::SqlName::new("notes".into()),
            terms: vec![
                CreateIndexTerm::Column {
                    name: super::super::schema::SqlName::new("tenant".into()),
                    collation: None,
                    order: None,
                },
                CreateIndexTerm::Column {
                    name: super::super::schema::SqlName::new("slug".into()),
                    collation: None,
                    order: None,
                },
            ],
            predicate: None,
        }
    }

    fn secondary_spec() -> CreateIndexSpec {
        CreateIndexSpec {
            unique: false,
            name: super::super::schema::SqlName::new("notes_tenant_slug_lookup".into()),
            table: super::super::schema::SqlName::new("notes".into()),
            terms: vec![
                CreateIndexTerm::Column {
                    name: super::super::schema::SqlName::new("tenant".into()),
                    collation: None,
                    order: None,
                },
                CreateIndexTerm::Column {
                    name: super::super::schema::SqlName::new("slug".into()),
                    collation: None,
                    order: None,
                },
            ],
            predicate: None,
        }
    }

    #[test]
    fn create_unique_index_roundtrips_backfills_and_reverts() {
        let (connection, before) = connection();
        let sql = "CREATE UNIQUE INDEX notes_tenant_slug ON notes (tenant, slug)";
        connection.execute(sql, ()).unwrap();
        let operation = IndexOperation::prepare_create(&connection, sql, &create_spec()).unwrap();

        assert_eq!(operation.backfill.len(), 2);
        assert_eq!(
            IndexOperation::decode(&operation.encode()).unwrap(),
            operation
        );
        let lowered = operation.to_homebase().unwrap();
        assert_explicit_range_assertions(
            &lowered.footprint,
            &[
                schema_object_name_scope_key(operation.index.name()),
                active_schema_revision_key(before.table_id()),
                primary_index_prefix(&before),
                column_name_scope_key(
                    before.table_id(),
                    &super::super::schema::SqlName::new("tenant".into()),
                ),
                column_name_scope_key(
                    before.table_id(),
                    &super::super::schema::SqlName::new("slug".into()),
                ),
                column_index_dependency_key(
                    before.table_id(),
                    before.columns()[1].id(),
                    operation.index.index_id(),
                ),
                column_index_dependency_key(
                    before.table_id(),
                    before.columns()[2].id(),
                    operation.index.index_id(),
                ),
            ],
        );
        assert!(
            lowered
                .footprint
                .writes()
                .contains(&active_schema_revision_key(before.table_id()))
        );
        assert!(
            lowered
                .footprint
                .constraints()
                .contains(&active_schema_revision_key(before.table_id()))
        );
        assert!(
            lowered
                .footprint
                .writes()
                .contains(&write_revision_key(before.table_id()))
        );
        assert_eq!(lowered.footprint.constraints().len(), 7);

        operation.record_catalog(&connection).unwrap();
        assert_eq!(
            catalog::by_name(&connection, "notes")
                .unwrap()
                .unwrap()
                .indexes()
                .len(),
            1
        );
        operation.rollback(&connection).unwrap();
        assert_eq!(
            catalog::by_name(&connection, "notes").unwrap(),
            Some(before)
        );
        assert_eq!(
            connection
                .query_row(
                    "SELECT count(*) FROM sqlite_schema
                     WHERE type = 'index' AND name = 'notes_tenant_slug'",
                    (),
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            0
        );
        operation.apply(&connection).unwrap();
    }

    #[test]
    fn create_secondary_index_tracks_ddl_without_row_projection() {
        let (connection, before) = connection();
        let sql = "CREATE INDEX notes_tenant_slug_lookup ON notes (tenant, slug)";
        connection.execute(sql, ()).unwrap();
        let operation =
            IndexOperation::prepare_create(&connection, sql, &secondary_spec()).unwrap();

        assert!(!operation.index.is_unique());
        assert!(operation.backfill.is_empty());
        assert_eq!(
            IndexOperation::decode(&operation.encode()).unwrap(),
            operation
        );

        let lowered = operation.to_homebase().unwrap();
        assert_explicit_range_assertions(
            &lowered.footprint,
            &[
                schema_object_name_scope_key(operation.index.name()),
                active_schema_revision_key(before.table_id()),
                column_name_scope_key(
                    before.table_id(),
                    &super::super::schema::SqlName::new("tenant".into()),
                ),
                column_name_scope_key(
                    before.table_id(),
                    &super::super::schema::SqlName::new("slug".into()),
                ),
                column_index_dependency_key(
                    before.table_id(),
                    before.columns()[1].id(),
                    operation.index.index_id(),
                ),
                column_index_dependency_key(
                    before.table_id(),
                    before.columns()[2].id(),
                    operation.index.index_id(),
                ),
            ],
        );
        assert_eq!(lowered.mutations.len(), 7);
        assert!(
            !lowered
                .footprint
                .constraints()
                .contains(&primary_index_prefix(&before))
        );
        assert!(
            !lowered
                .footprint
                .writes()
                .contains(&write_revision_key(before.table_id()))
        );
        assert!(
            !lowered
                .mutations
                .iter()
                .any(|mutation| mutation.key() == &write_revision_key(before.table_id()))
        );

        operation.record_catalog(&connection).unwrap();
        operation.rollback(&connection).unwrap();
        operation.apply(&connection).unwrap();
    }

    #[test]
    fn index_apply_and_restore_render_the_owners_current_name() {
        let (source, before) = connection();
        let sql = "CREATE INDEX notes_tenant_slug_lookup ON notes (tenant, slug)";
        source.execute(sql, ()).unwrap();
        let create = IndexOperation::prepare_create(&source, sql, &secondary_spec()).unwrap();

        let target = Connection::open_in_memory().unwrap();
        catalog::initialize(&target).unwrap();
        target.execute(before.sql(), ()).unwrap();
        catalog::insert(&target, &before).unwrap();
        target
            .execute("ALTER TABLE notes RENAME TO archived_notes", ())
            .unwrap();
        catalog::rename_binding(
            &target,
            before.table_id(),
            before.table_name_identity(),
            &super::super::schema::SqlName::new("archived_notes".into()),
        )
        .unwrap();

        create.apply(&target).unwrap();
        assert_eq!(
            target
                .query_row(
                    "SELECT tbl_name FROM sqlite_schema
                     WHERE type = 'index' AND name = 'notes_tenant_slug_lookup'",
                    (),
                    |row| row.get::<_, String>(0),
                )
                .unwrap(),
            "archived_notes"
        );
        assert_eq!(
            catalog::by_id(&target, before.table_id())
                .unwrap()
                .unwrap()
                .indexes()[0]
                .sql(),
            sql
        );

        let drop_sql = "DROP INDEX notes_tenant_slug_lookup";
        let drop = IndexOperation::prepare_drop(
            &target,
            drop_sql,
            &DropIndexSpec {
                name: super::super::schema::SqlName::new("notes_tenant_slug_lookup".into()),
            },
        )
        .unwrap();
        target.execute(drop_sql, ()).unwrap();
        drop.record_catalog(&target).unwrap();
        drop.rollback(&target).unwrap();
        assert_eq!(
            target
                .query_row(
                    "SELECT tbl_name FROM sqlite_schema
                     WHERE type = 'index' AND name = 'notes_tenant_slug_lookup'",
                    (),
                    |row| row.get::<_, String>(0),
                )
                .unwrap(),
            "archived_notes"
        );
        catalog::validate(&target).unwrap();
    }

    #[test]
    fn rich_secondary_index_definition_roundtrips_replays_and_rejects_sql_mismatch() {
        let (connection, before) = connection();
        let sql = "CREATE INDEX notes_search ON notes (
            tenant COLLATE NOCASE DESC,
            lower(slug) ASC,
            tenant
        ) WHERE tenant IS NOT NULL AND length(slug) > 0";
        let ValidatedExecute::CreateIndex(spec) = super::super::sql::validate_execute(sql).unwrap()
        else {
            unreachable!()
        };
        connection.execute(sql, ()).unwrap();
        let operation = IndexOperation::prepare_create(&connection, sql, &spec).unwrap();

        assert!(!operation.index.is_unique());
        assert!(operation.index.columns().is_empty());
        assert_eq!(operation.index.terms().len(), 3);
        assert_eq!(
            operation.index.terms()[0],
            IndexTerm::Column {
                column: before
                    .column_named(&super::super::schema::SqlName::new("tenant".into()))
                    .unwrap()
                    .id(),
                collation: Some(super::super::schema::SqlName::new("NOCASE".into())),
                order: Some(IndexOrder::Desc),
            }
        );
        assert!(matches!(
            operation.index.terms()[1],
            IndexTerm::Expression {
                order: Some(IndexOrder::Asc),
                ..
            }
        ));
        assert!(operation.index.predicate().is_some());
        assert_eq!(
            IndexOperation::decode(&operation.encode()).unwrap(),
            operation
        );
        let lowered = operation.to_homebase().unwrap();
        assert_explicit_range_assertions(
            &lowered.footprint,
            &[
                column_name_scope_key(
                    before.table_id(),
                    &super::super::schema::SqlName::new("tenant".into()),
                ),
                column_name_scope_key(
                    before.table_id(),
                    &super::super::schema::SqlName::new("slug".into()),
                ),
            ],
        );
        let definition = lowered
            .mutations
            .iter()
            .find_map(|mutation| match mutation {
                Mutation::Set { key, value }
                    if key
                        == &index_definition_key(
                            operation.after.table_id(),
                            operation.index.index_id(),
                        ) =>
                {
                    Some(value)
                }
                _ => None,
            })
            .expect("secondary index definition mutation is present");
        assert_eq!(
            NamedIndex::decode(definition).unwrap(),
            operation.index,
            "the independently fetched definition retains every physical term"
        );
        assert!(
            !lowered
                .mutations
                .iter()
                .any(|mutation| mutation.key() == &write_revision_key(operation.after.table_id()))
        );

        let mut mismatched = operation.clone();
        mismatched.sql =
            "CREATE INDEX notes_search ON notes (tenant, lower(tenant), tenant)".into();
        assert_eq!(
            mismatched.validate(),
            Err(IndexCodecError::InvalidEvolution)
        );

        operation.record_catalog(&connection).unwrap();
        let catalog_index = catalog::by_name(&connection, "notes")
            .unwrap()
            .unwrap()
            .index_named(&super::super::schema::SqlName::new("notes_search".into()))
            .unwrap()
            .clone();
        assert_eq!(catalog_index, operation.index);
        operation.rollback(&connection).unwrap();
        operation.apply(&connection).unwrap();
    }

    #[test]
    fn index_restoration_renders_current_column_bindings_from_typed_terms() {
        let (connection, _) = connection();
        let create_sql =
            "CREATE INDEX notes_search ON notes (lower(\"slug\")) WHERE \"slug\" IS NOT NULL";
        let ValidatedExecute::CreateIndex(create_spec) =
            super::super::sql::validate_execute(create_sql).unwrap()
        else {
            unreachable!()
        };
        connection.execute(create_sql, ()).unwrap();
        let create = IndexOperation::prepare_create(&connection, create_sql, &create_spec).unwrap();
        create.record_catalog(&connection).unwrap();

        let rename_sql = "ALTER TABLE notes RENAME COLUMN slug TO contents";
        let ValidatedExecute::RenameColumn(rename_spec) =
            super::super::sql::validate_execute(rename_sql).unwrap()
        else {
            unreachable!()
        };
        let rename = super::super::alter::AlterTableOperation::prepare_rename_column(
            &connection,
            rename_sql,
            &rename_spec,
        )
        .unwrap();
        connection.execute(rename_sql, ()).unwrap();
        rename.record_catalog(&connection).unwrap();

        let drop_sql = "DROP INDEX notes_search";
        let drop = IndexOperation::prepare_drop(
            &connection,
            drop_sql,
            &DropIndexSpec {
                name: super::super::schema::SqlName::new("notes_search".into()),
            },
        )
        .unwrap();
        connection.execute(drop_sql, ()).unwrap();
        drop.record_catalog(&connection).unwrap();
        drop.rollback(&connection).unwrap();

        let sql = connection
            .query_row(
                "SELECT sql FROM sqlite_schema WHERE name = 'notes_search'",
                (),
                |row| row.get::<_, String>(0),
            )
            .unwrap();
        assert!(sql.contains("\"contents\""), "{sql}");
        assert!(!sql.contains("\"slug\""), "{sql}");
        connection
            .execute(
                "INSERT INTO notes (id, tenant, contents) VALUES (7, 'tenant', 'restored')",
                (),
            )
            .unwrap();
        catalog::validate(&connection).unwrap();
    }

    #[test]
    fn dropping_a_secondary_index_does_not_touch_the_write_contract() {
        let (connection, before) = connection();
        let create_sql = "CREATE INDEX notes_tenant_slug_lookup ON notes (tenant, slug)";
        connection.execute(create_sql, ()).unwrap();
        let created =
            IndexOperation::prepare_create(&connection, create_sql, &secondary_spec()).unwrap();
        created.record_catalog(&connection).unwrap();

        let drop_sql = "DROP INDEX notes_tenant_slug_lookup";
        let drop = IndexOperation::prepare_drop(
            &connection,
            drop_sql,
            &DropIndexSpec {
                name: super::super::schema::SqlName::new("notes_tenant_slug_lookup".into()),
            },
        )
        .unwrap();
        connection.execute(drop_sql, ()).unwrap();

        let lowered = drop.to_homebase().unwrap();
        assert_explicit_range_assertions(
            &lowered.footprint,
            &[
                schema_object_name_scope_key(drop.index.name()),
                active_schema_revision_key(before.table_id()),
            ],
        );
        assert!(
            !lowered
                .footprint
                .constraints()
                .contains(&write_revision_key(before.table_id()))
        );
        assert!(
            !lowered
                .mutations
                .iter()
                .any(|mutation| mutation.key() == &write_revision_key(before.table_id()))
        );
    }

    #[test]
    fn drop_changes_schema_head_without_advancing_write_contract() {
        let (connection, _) = connection();
        let create_sql = "CREATE UNIQUE INDEX notes_tenant_slug ON notes (tenant, slug)";
        connection.execute(create_sql, ()).unwrap();
        let created =
            IndexOperation::prepare_create(&connection, create_sql, &create_spec()).unwrap();
        created.record_catalog(&connection).unwrap();

        let drop_sql = "DROP INDEX notes_tenant_slug";
        let drop = IndexOperation::prepare_drop(
            &connection,
            drop_sql,
            &DropIndexSpec {
                name: super::super::schema::SqlName::new("notes_tenant_slug".into()),
            },
        )
        .unwrap();
        connection.execute(drop_sql, ()).unwrap();
        drop.record_catalog(&connection).unwrap();

        assert_eq!(IndexOperation::decode(&drop.encode()).unwrap(), drop);
        let lowered = drop.to_homebase().unwrap();
        assert_explicit_range_assertions(
            &lowered.footprint,
            &[
                schema_object_name_scope_key(drop.index.name()),
                active_schema_revision_key(drop.after.table_id()),
                write_revision_key(drop.after.table_id()),
                column_index_dependency_key(
                    drop.after.table_id(),
                    drop.after.columns()[1].id(),
                    drop.index.index_id(),
                ),
                column_index_dependency_key(
                    drop.after.table_id(),
                    drop.after.columns()[2].id(),
                    drop.index.index_id(),
                ),
            ],
        );
        assert_eq!(lowered.footprint.writes().len(), 3);
        assert!(
            lowered
                .footprint
                .writes()
                .contains(&active_schema_revision_key(drop.after.table_id()))
        );
        assert!(
            !lowered
                .mutations
                .iter()
                .any(|mutation| mutation.key() == &write_revision_key(drop.after.table_id()))
        );

        drop.rollback(&connection).unwrap();
        assert_eq!(
            catalog::by_name(&connection, "notes")
                .unwrap()
                .unwrap()
                .indexes()
                .len(),
            1
        );
    }
}
