//! Explicit UNIQUE-index schema operations and ownership backfills.

use std::fmt;

use homebase_core::key::Key;
use homebase_core::reader::Reader;
use homebase_core::tag::Mutation;
use homebase_core::writer::Writer;
use rusqlite::Connection;
use uuid::{Uuid, Variant, Version};

use super::catalog;
use super::codes;
use super::row::{UniqueBackfillEntry, backfill_unique_index};
use super::schema::{
    CreateTable, IndexDefinition, MutationId, SchemaRevisionId, active_schema_revision_key,
    index_name_scope_key, schema_log_key, table_schema_key, unique_keyspace_key,
    write_revision_key,
};
use super::sql::{CreateUniqueIndexSpec, DropIndexSpec, ValidatedExecute};
use crate::commit::footprint::ConflictFootprint;
use crate::{Error, Result};

const INDEX_OPERATION_VERSION: u8 = 1;
const TAG_MUTATION_ID: u8 = 1;
const TAG_SQL: u8 = 2;
const TAG_BEFORE: u8 = 3;
const TAG_AFTER: u8 = 4;
const TAG_ACTION: u8 = 5;
const TAG_INDEX: u8 = 6;
const TAG_BACKFILL: u8 = 7;
const CREATE_UNIQUE: u8 = 1;
const DROP: u8 = 2;

const BACKFILL_VERSION: u8 = 1;
const TAG_BACKFILL_KEY: u8 = 1;
const TAG_BACKFILL_OWNER: u8 = 2;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum IndexAction {
    CreateUnique,
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
    index: IndexDefinition,
    backfill: Vec<UniqueBackfillEntry>,
}

/// Homebase mutations and conflict footprint for one explicit-index change.
pub struct IndexHomebaseOp {
    pub mutations: Vec<Mutation>,
    pub footprint: ConflictFootprint,
}

impl IndexOperation {
    /// Prepare a CREATE UNIQUE INDEX after SQLite has validated and built it.
    pub fn prepare_create(
        connection: &Connection,
        sql: &str,
        spec: &CreateUniqueIndexSpec,
    ) -> Result<Self> {
        if catalog::index_by_name(connection, &spec.name)?.is_some() {
            return Err(Error::InvalidDatabase(
                "schema catalog already contains this index",
            ));
        }
        let before = catalog::by_name(connection, spec.table.value())?.ok_or(
            Error::UnsupportedSql("CREATE UNIQUE INDEX target has no synchronized schema identity"),
        )?;
        let columns =
            spec.columns
                .iter()
                .map(|name| {
                    before.column_named(name).map(|column| column.id()).ok_or(
                        Error::UnsupportedSql("CREATE UNIQUE INDEX references an unknown column"),
                    )
                })
                .collect::<Result<Vec<_>>>()?;
        let index = IndexDefinition::new_unique(sql.to_owned(), spec.name.clone(), columns);
        let after = before.with_added_index(
            SchemaRevisionId::from_bytes(Uuid::new_v4().into_bytes()),
            index.clone(),
        );
        let backfill = backfill_unique_index(connection, &before, &index)?;
        let operation = Self {
            mutation_id: MutationId::from_bytes(Uuid::new_v4().into_bytes()),
            sql: sql.to_owned(),
            before,
            after,
            action: IndexAction::CreateUnique,
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
        let after = before
            .with_retired_index(
                SchemaRevisionId::from_bytes(Uuid::new_v4().into_bytes()),
                &spec.name,
            )
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
        let name = index_name_scope_key(self.index.name());
        let schema_head = active_schema_revision_key(self.after.table_id());
        let mut footprint = ConflictFootprint::new();
        footprint.add_constraint(name.clone());
        footprint.add_write(schema_head.clone());
        let mut mutations = vec![Mutation::Set {
            key: schema_log_key(self.mutation_id),
            value: self.encode(),
        }];
        match self.action {
            IndexAction::CreateUnique => mutations.push(Mutation::Set {
                key: name,
                value: index_name_value(self.after.table_id().as_bytes(), self.index.keyspace_id()),
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

        if self.action == IndexAction::CreateUnique {
            mutations.push(Mutation::Set {
                key: unique_keyspace_key(self.after.table_id(), self.index.keyspace_id()),
                value: self.index.encode(),
            });
            for entry in &self.backfill {
                mutations.push(Mutation::Set {
                    key: entry.key.clone(),
                    value: entry.owner.clone(),
                });
            }
            let write_revision = write_revision_key(self.after.table_id());
            footprint.add_write(write_revision.clone());
            footprint.add_constraint(row_keyspace_prefix(&self.before));
            mutations.push(Mutation::Set {
                key: write_revision,
                value: self.mutation_id.as_bytes().to_vec(),
            });
        }
        Ok(IndexHomebaseOp {
            mutations,
            footprint,
        })
    }

    pub fn apply(&self, connection: &Connection) -> Result<()> {
        self.validate().map_err(invalid_operation)?;
        if catalog::by_id(connection, self.before.table_id())?.as_ref() != Some(&self.before) {
            return Err(Error::InvalidDatabase(
                "index operation no longer matches the schema catalog",
            ));
        }
        connection.execute(&self.sql, ())?;
        catalog::replace(connection, &self.after)
    }

    pub fn record_catalog(&self, connection: &Connection) -> Result<()> {
        catalog::replace(connection, &self.after)
    }

    pub fn rollback(&self, connection: &Connection) -> Result<()> {
        self.validate().map_err(invalid_operation)?;
        if catalog::by_id(connection, self.after.table_id())?.as_ref() != Some(&self.after) {
            return Err(Error::InvalidDatabase(
                "pending index operation no longer matches SQLite state",
            ));
        }
        match self.action {
            IndexAction::CreateUnique => {
                connection.execute_batch(&format!(
                    "DROP INDEX {}",
                    quote_identifier(self.index.name().value())
                ))?;
            }
            IndexAction::Drop => {
                connection.execute(self.index.sql(), ())?;
            }
        }
        catalog::replace(connection, &self.before)
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
                    IndexAction::CreateUnique => CREATE_UNIQUE,
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
                        CREATE_UNIQUE => IndexAction::CreateUnique,
                        DROP => IndexAction::Drop,
                        _ => return Err(IndexCodecError::InvalidAction),
                    };
                    set_once(&mut action, value)?;
                }
                TAG_INDEX => set_once(
                    &mut index,
                    IndexDefinition::decode(value)
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
            || self.before.row_keyspace_id() != self.after.row_keyspace_id()
        {
            return Err(IndexCodecError::InvalidEvolution);
        }
        match self.action {
            IndexAction::CreateUnique => {
                let ValidatedExecute::CreateUniqueIndex(spec) =
                    super::sql::validate_execute(&self.sql)
                        .map_err(|_| IndexCodecError::InvalidSql)?
                else {
                    return Err(IndexCodecError::InvalidSql);
                };
                let expected_after = self
                    .before
                    .with_added_index(self.after.schema_revision_id(), self.index.clone());
                if self.before.schema_revision_id() == self.after.schema_revision_id()
                    || expected_after != self.after
                    || spec.name != *self.index.name()
                    || spec.table != *self.before.table_name_identity()
                    || spec.columns.len() != self.index.columns().len()
                    || spec
                        .columns
                        .iter()
                        .zip(self.index.columns())
                        .any(|(name, id)| {
                            self.before
                                .column_named(name)
                                .is_none_or(|column| column.id() != *id)
                        })
                    || self.before.index_named(self.index.name()).is_some()
                    || self.after.index_named(self.index.name()) != Some(&self.index)
                    || self.backfill.iter().any(|entry| {
                        let owner = Key::decode(&entry.owner);
                        !entry
                            .key
                            .starts_with(&unique_prefix(&self.after, &self.index))
                            || !owner.is_ok_and(|owner| {
                                owner.starts_with(&row_keyspace_prefix(&self.after))
                            })
                    })
                    || self
                        .backfill
                        .windows(2)
                        .any(|entries| entries[0].key >= entries[1].key)
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

fn row_keyspace_prefix(table: &CreateTable) -> Key {
    Key::from_bytes([
        codes::ROOT,
        codes::TABLES,
        table.table_id().as_bytes().as_slice(),
        codes::ROWS,
        table.row_keyspace_id().as_bytes().as_slice(),
    ])
    .expect("row keyspace prefix is bounded")
}

fn unique_prefix(table: &CreateTable, index: &IndexDefinition) -> Key {
    Key::from_bytes([
        codes::ROOT,
        codes::TABLES,
        table.table_id().as_bytes().as_slice(),
        codes::UNIQUE,
        index.keyspace_id().as_bytes().as_slice(),
    ])
    .expect("unique keyspace prefix is bounded")
}

fn index_name_value(table: [u8; 16], keyspace: super::schema::UniqueKeyspaceId) -> Vec<u8> {
    let mut value = Vec::with_capacity(32);
    value.extend_from_slice(&table);
    value.extend_from_slice(&keyspace.as_bytes());
    value
}

fn encode_backfill(entry: &UniqueBackfillEntry) -> Vec<u8> {
    let mut writer = Writer::new();
    writer.u8(BACKFILL_VERSION);
    writer
        .field(TAG_BACKFILL_KEY, &entry.key.encode())
        .expect("backfill field fits in u32");
    writer
        .field(TAG_BACKFILL_OWNER, &entry.owner)
        .expect("backfill field fits in u32");
    writer.finish()
}

fn decode_backfill(frame: &[u8]) -> std::result::Result<UniqueBackfillEntry, IndexCodecError> {
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
    Ok(UniqueBackfillEntry {
        key: key.ok_or(IndexCodecError::MissingField(TAG_BACKFILL_KEY))?,
        owner: owner.ok_or(IndexCodecError::MissingField(TAG_BACKFILL_OWNER))?,
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
    use crate::database::schema::{CreateColumn, CreateTableSpec, TableStorage, TypeDeclaration};

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

    fn create_spec() -> CreateUniqueIndexSpec {
        CreateUniqueIndexSpec {
            name: super::super::schema::SqlName::new("notes_tenant_slug".into()),
            table: super::super::schema::SqlName::new("notes".into()),
            columns: vec![
                super::super::schema::SqlName::new("tenant".into()),
                super::super::schema::SqlName::new("slug".into()),
            ],
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
        assert!(
            lowered
                .footprint
                .writes()
                .contains(&active_schema_revision_key(before.table_id()))
        );
        assert!(
            lowered
                .footprint
                .writes()
                .contains(&write_revision_key(before.table_id()))
        );
        assert_eq!(lowered.footprint.constraints().len(), 2);

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
        assert_eq!(lowered.footprint.writes().len(), 1);
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
