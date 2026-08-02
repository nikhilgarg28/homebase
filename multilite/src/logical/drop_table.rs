//! Metadata-only table destruction with local-only rejection repair.

use std::fmt;

use homebase_core::range::Range;
use homebase_core::reader::Reader;
use homebase_core::tag::Mutation;
use homebase_core::writer::Writer;
use rusqlite::{Connection, OptionalExtension};
use uuid::{Uuid, Variant, Version};

use super::guard::{GuardPlan, GuardReason, LogicalTarget, OperationFamily};
use super::schema::{
    CreateTable, MutationId, SqlName, TableId, constraint_reference_key, schema_log_key,
    schema_object_name_scope_key, table_prefix,
};
use super::view;
use crate::catalog::{self, TableState};
use crate::commit::footprint::ConflictFootprint;
use crate::repair;
use crate::sqlite::quote_identifier;
use crate::{Error, Result};

const VERSION: u8 = 1;
const TAG_MUTATION_ID: u8 = 1;
const TAG_SQL: u8 = 2;
const TAG_SOURCE_TABLE: u8 = 3;
const TAG_BEFORE: u8 = 4;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DropTableOperation {
    mutation_id: MutationId,
    sql: String,
    source_table: SqlName,
    before: CreateTable,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DropTableHomebaseOp {
    pub mutations: Vec<Mutation>,
    pub footprint: ConflictFootprint,
    pub guards: GuardPlan,
}

impl DropTableOperation {
    pub fn prepare(
        connection: &Connection,
        sql: &str,
        spec: &crate::sql::DropTableSpec,
    ) -> Result<Self> {
        let before = catalog::by_name(connection, spec.name.value())?.ok_or(
            Error::InvalidDatabase("DROP TABLE references an unknown table"),
        )?;
        let source_table = catalog::name_by_id(connection, before.table_id())?.ok_or(
            Error::InvalidDatabase("DROP TABLE identity has no current name binding"),
        )?;
        if source_table.canonical() != spec.name.canonical() {
            return Err(Error::InvalidDatabase(
                "DROP TABLE name contradicts the schema catalog",
            ));
        }
        if !catalog::incoming_foreign_keys(connection, before.table_id())?.is_empty() {
            return Err(Error::UnsupportedSql(
                "DROP TABLE of a referenced parent table is not supported",
            ));
        }
        view::ensure_table_not_referenced(connection, &source_table)?;
        let operation = Self {
            mutation_id: MutationId::from_bytes(Uuid::new_v4().into_bytes()),
            sql: sql.to_owned(),
            source_table,
            before,
        };
        operation.validate().map_err(|error| {
            Error::InvalidMultiliteOp(format!("invalid DROP TABLE operation: {error}"))
        })?;
        Ok(operation)
    }

    pub fn table_id(&self) -> TableId {
        self.before.table_id()
    }

    pub(crate) fn repair_spec(&self) -> repair::RepairSpec {
        repair::drop_table_spec(
            self.mutation_id.as_bytes(),
            self.before.primary_key_columns().count(),
            self.before.columns().len(),
        )
    }

    pub fn to_homebase(&self) -> Result<DropTableHomebaseOp> {
        self.validate().map_err(|error| {
            Error::InvalidMultiliteOp(format!("invalid DROP TABLE operation: {error}"))
        })?;
        let table = table_prefix(self.before.table_id());
        let table_name = schema_object_name_scope_key(&self.source_table);
        let mut guards = GuardPlan::for_operation(OperationFamily::DropTable);
        guards.invariant(table.clone(), GuardReason::TableExistence)?;
        guards.write(table.clone(), GuardReason::TableExistence)?;
        guards.invariant(table_name.clone(), GuardReason::SchemaObjectName)?;
        let mut mutations = vec![
            Mutation::Set {
                key: schema_log_key(self.mutation_id),
                value: self.encode(),
            },
            Mutation::Delete { key: table_name },
        ];
        for index in self
            .before
            .indexes()
            .iter()
            .filter(|index| index.is_active())
        {
            let name = schema_object_name_scope_key(index.name());
            guards.invariant(name.clone(), GuardReason::SchemaObjectName)?;
            mutations.push(Mutation::Delete { key: name });
        }
        mutations.push(Mutation::DeleteRange {
            range: Range::Prefix(table),
        });
        for foreign_key in self
            .before
            .foreign_keys()
            .iter()
            .filter(|foreign_key| foreign_key.is_active())
        {
            let reference = constraint_reference_key(
                foreign_key.referenced_table(),
                foreign_key.referenced_index().as_bytes(),
                foreign_key.id(),
            );
            guards.invariant(reference.clone(), GuardReason::ConstraintReference)?;
            guards.write(reference.clone(), GuardReason::ConstraintReference)?;
            mutations.push(Mutation::Delete { key: reference });
            let prefix = LogicalTarget::ForeignReferencePrefix {
                parent: foreign_key.referenced_table(),
                relationship: foreign_key.id(),
                parent_index: foreign_key.referenced_index(),
                parent_images: Vec::new(),
            }
            .render()
            .map_err(|_| Error::CaptureInvariant("DROP TABLE foreign-key prefix is invalid"))?;
            guards.invariant(prefix.clone(), GuardReason::ForeignReference)?;
            guards.write(prefix.clone(), GuardReason::ForeignReference)?;
            mutations.push(Mutation::DeleteRange {
                range: Range::Prefix(prefix),
            });
        }
        let footprint = guards.footprint();
        Ok(DropTableHomebaseOp {
            mutations,
            footprint,
            guards,
        })
    }

    pub fn apply(&self, connection: &Connection) -> Result<()> {
        self.validate_catalog_before(connection)?;
        connection.execute_batch(&format!(
            "DROP TABLE {}",
            quote_identifier(self.source_table.value())
        ))?;
        catalog::remove_by_id(connection, self.before.table_id())
    }

    #[cfg(debug_assertions)]
    pub(super) fn verify_materialized(&self, connection: &Connection) -> Result<()> {
        let physical = connection
            .query_row(
                "SELECT type FROM main.sqlite_schema WHERE name = ?1 COLLATE NOCASE",
                [self.source_table.value()],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        if physical.is_some()
            || catalog::by_id(connection, self.before.table_id())?.is_some()
            || catalog::by_name(connection, self.source_table.value())?.is_some()
        {
            return Err(Error::InvalidDatabase(
                "dropped table remains in SQLite or the schema catalog",
            ));
        }
        Ok(())
    }

    pub(crate) fn capture_local_repair(&self, connection: &Connection) -> Result<()> {
        self.validate_catalog_before(connection)?;
        let state = catalog::capture_table_state(connection, self.before.table_id())?;
        if state.name() != &self.source_table || state.definition() != &self.before {
            return Err(Error::InvalidDatabase(
                "DROP TABLE repair catalog state contradicts its operation",
            ));
        }
        let primary_key = self
            .before
            .primary_key_columns()
            .map(|column| {
                catalog::column_name_by_id(connection, self.before.table_id(), column.id())?
                    .map(|name| name.value().to_owned())
                    .ok_or(Error::InvalidDatabase(
                        "DROP TABLE primary-key column has no current name binding",
                    ))
            })
            .collect::<Result<Vec<_>>>()?;
        let columns = catalog::column_names(connection, &self.before)?
            .into_iter()
            .map(|name| name.value().to_owned())
            .collect::<Vec<_>>();
        repair::capture_drop_table(
            connection,
            self.mutation_id.as_bytes(),
            self.source_table.value(),
            &primary_key,
            &columns,
            &state.encode()?,
        )
    }

    pub fn rollback(&self, connection: &Connection) -> Result<()> {
        if catalog::by_id(connection, self.before.table_id())?.is_some()
            || catalog::by_name(connection, self.source_table.value())?.is_some()
        {
            return Err(Error::InvalidDatabase(
                "pending DROP TABLE identity or name is already materialized",
            ));
        }
        let spec = self.repair_spec();
        let state = TableState::decode(&repair::drop_table_metadata(connection, spec)?)?;
        if state.name() != &self.source_table || state.definition() != &self.before {
            return Err(Error::InvalidDatabase(
                "pending DROP TABLE catalog repair contradicts its operation",
            ));
        }
        catalog::restore_table_state(connection, &state)?;
        connection.execute(&self.before.materialization_sql(connection)?, ())?;
        let primary_key = self
            .before
            .primary_key_columns()
            .map(|column| {
                catalog::column_name_by_id(connection, self.before.table_id(), column.id())?
                    .map(|name| name.value().to_owned())
                    .ok_or(Error::InvalidDatabase(
                        "restored DROP TABLE primary-key column has no name binding",
                    ))
            })
            .collect::<Result<Vec<_>>>()?;
        let columns = catalog::column_names(connection, &self.before)?
            .into_iter()
            .map(|name| name.value().to_owned())
            .collect::<Vec<_>>();
        repair::restore_drop_table_rows(
            connection,
            spec,
            self.source_table.value(),
            &primary_key,
            &columns,
        )?;
        for index in self
            .before
            .indexes()
            .iter()
            .filter(|index| index.is_active())
        {
            connection.execute_batch(&index.materialization_sql(
                connection,
                &self.before,
                &self.source_table,
            )?)?;
        }
        let violations: bool = connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM pragma_foreign_key_check)",
            (),
            |row| row.get(0),
        )?;
        if violations {
            return Err(Error::InvalidDatabase(
                "DROP TABLE repair violates foreign-key integrity",
            ));
        }
        repair::retire(connection, self.mutation_id.as_bytes())
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut writer = Writer::new();
        writer.u8(VERSION);
        writer
            .field(TAG_MUTATION_ID, &self.mutation_id.as_bytes())
            .expect("DROP TABLE identity fits in u32");
        writer
            .field(TAG_SQL, self.sql.as_bytes())
            .expect("DROP TABLE SQL fits in u32");
        writer
            .field(TAG_SOURCE_TABLE, self.source_table.value().as_bytes())
            .expect("DROP TABLE name fits in u32");
        writer
            .field(TAG_BEFORE, &self.before.encode())
            .expect("DROP TABLE schema fits in u32");
        writer.finish()
    }

    pub fn decode(frame: &[u8]) -> std::result::Result<Self, DropTableCodecError> {
        let mut reader = Reader::new(frame);
        if reader.u8() != Some(VERSION) {
            return Err(DropTableCodecError::UnknownVersion);
        }
        let mut mutation_id = None;
        let mut sql = None;
        let mut source_table = None;
        let mut before = None;
        while let Some((tag, value)) = reader.field().map_err(|_| DropTableCodecError::Truncated)? {
            match tag {
                TAG_MUTATION_ID => {
                    set_once(&mut mutation_id, MutationId::from_bytes(uuid_bytes(value)?))?
                }
                TAG_SQL => set_once(&mut sql, decode_string(value)?)?,
                TAG_SOURCE_TABLE => {
                    set_once(&mut source_table, SqlName::new(decode_string(value)?))?
                }
                TAG_BEFORE => set_once(
                    &mut before,
                    CreateTable::decode(value).map_err(|_| DropTableCodecError::InvalidSchema)?,
                )?,
                _ => {}
            }
        }
        let operation = Self {
            mutation_id: mutation_id.ok_or(DropTableCodecError::MissingField(TAG_MUTATION_ID))?,
            sql: sql.ok_or(DropTableCodecError::MissingField(TAG_SQL))?,
            source_table: source_table
                .ok_or(DropTableCodecError::MissingField(TAG_SOURCE_TABLE))?,
            before: before.ok_or(DropTableCodecError::MissingField(TAG_BEFORE))?,
        };
        operation.validate()?;
        Ok(operation)
    }

    fn validate(&self) -> std::result::Result<(), DropTableCodecError> {
        let spec = match crate::sql::validate_execute(&self.sql)
            .map_err(|_| DropTableCodecError::InvalidSql)?
        {
            crate::sql::ValidatedExecute::DropTable(spec)
            | crate::sql::ValidatedExecute::DropTableIfExists(spec) => spec,
            _ => return Err(DropTableCodecError::InvalidSql),
        };
        if spec.name.canonical() != self.source_table.canonical() {
            return Err(DropTableCodecError::SqlMismatch);
        }
        self.before
            .validate_ir()
            .map_err(|_| DropTableCodecError::InvalidSchema)
    }

    fn validate_catalog_before(&self, connection: &Connection) -> Result<()> {
        if catalog::by_id(connection, self.before.table_id())?.as_ref() != Some(&self.before)
            || catalog::name_by_id(connection, self.before.table_id())?.as_ref()
                != Some(&self.source_table)
            || !catalog::incoming_foreign_keys(connection, self.before.table_id())?.is_empty()
        {
            return Err(Error::InvalidDatabase(
                "DROP TABLE no longer matches the schema catalog",
            ));
        }
        Ok(())
    }
}

fn set_once<T>(slot: &mut Option<T>, value: T) -> std::result::Result<(), DropTableCodecError> {
    if slot.replace(value).is_some() {
        Err(DropTableCodecError::DuplicateField)
    } else {
        Ok(())
    }
}

fn decode_string(value: &[u8]) -> std::result::Result<String, DropTableCodecError> {
    String::from_utf8(value.to_vec()).map_err(|_| DropTableCodecError::InvalidUtf8)
}

fn uuid_bytes(value: &[u8]) -> std::result::Result<[u8; 16], DropTableCodecError> {
    let bytes = value
        .try_into()
        .map_err(|_| DropTableCodecError::InvalidLength)?;
    let uuid = Uuid::from_bytes(bytes);
    if uuid.get_version() != Some(Version::Random) || uuid.get_variant() != Variant::RFC4122 {
        return Err(DropTableCodecError::InvalidUuid);
    }
    Ok(bytes)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DropTableCodecError {
    UnknownVersion,
    Truncated,
    DuplicateField,
    MissingField(u8),
    InvalidLength,
    InvalidUtf8,
    InvalidUuid,
    InvalidSql,
    SqlMismatch,
    InvalidSchema,
}

impl fmt::Display for DropTableCodecError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{self:?}")
    }
}

#[cfg(test)]
mod tests {
    use homebase_core::range::Range;

    use super::*;
    use crate::commit::footprint::assert_explicit_range_assertions;
    use crate::logical::schema::{schema_object_name_scope_key, table_prefix};

    fn operation() -> (Connection, DropTableOperation) {
        let connection = Connection::open_in_memory().unwrap();
        repair::register(&connection).unwrap();
        repair::initialize(&connection).unwrap();
        catalog::initialize(&connection).unwrap();
        let create_sql = "CREATE TABLE inventory (tenant TEXT, sku INTEGER, body BLOB, \
             PRIMARY KEY (tenant, sku)) WITHOUT ROWID";
        connection.execute(create_sql, ()).unwrap();
        let crate::sql::ValidatedExecute::CreateTable(spec) =
            crate::sql::validate_execute(create_sql).unwrap()
        else {
            unreachable!()
        };
        let definition = CreateTable::prepare(&connection, create_sql, spec).unwrap();
        catalog::insert(&connection, &definition).unwrap();
        connection
            .execute(
                "INSERT INTO inventory VALUES ('north', 1, x'0001'), ('south', 2, NULL)",
                (),
            )
            .unwrap();
        let sql = "DROP TABLE inventory";
        let crate::sql::ValidatedExecute::DropTable(spec) =
            crate::sql::validate_execute(sql).unwrap()
        else {
            unreachable!()
        };
        let operation = DropTableOperation::prepare(&connection, sql, &spec).unwrap();
        (connection, operation)
    }

    #[test]
    fn codec_and_lowering_are_metadata_only_and_guard_table_identity() {
        let (_, operation) = operation();
        let encoded = operation.encode();
        assert_eq!(DropTableOperation::decode(&encoded).unwrap(), operation);

        let lowered = operation.to_homebase().unwrap();
        let table = table_prefix(operation.table_id());
        let name = schema_object_name_scope_key(&operation.source_table);
        assert_explicit_range_assertions(&lowered.footprint, &[table.clone(), name]);
        assert_eq!(lowered.mutations.len(), 3);
        assert!(matches!(
            &lowered.mutations[2],
            Mutation::DeleteRange {
                range: Range::Prefix(prefix)
            } if prefix == &table
        ));
        assert!(
            !encoded
                .windows(b"north".len())
                .any(|bytes| bytes == b"north")
        );
    }

    #[test]
    fn dropping_a_child_table_retires_its_exact_constraint_reference_marker() {
        let connection = Connection::open_in_memory().unwrap();
        repair::register(&connection).unwrap();
        repair::initialize(&connection).unwrap();
        catalog::initialize(&connection).unwrap();

        let parent_sql = "CREATE TABLE parents (id INTEGER PRIMARY KEY)";
        connection.execute(parent_sql, ()).unwrap();
        let crate::sql::ValidatedExecute::CreateTable(parent_spec) =
            crate::sql::validate_execute(parent_sql).unwrap()
        else {
            unreachable!()
        };
        let parent = CreateTable::prepare(&connection, parent_sql, parent_spec).unwrap();
        catalog::insert(&connection, &parent).unwrap();

        let child_sql = "CREATE TABLE children (
            id INTEGER PRIMARY KEY,
            parent_id INTEGER,
            CONSTRAINT fk_parent FOREIGN KEY (parent_id) REFERENCES parents(id)
        )";
        connection.execute(child_sql, ()).unwrap();
        let crate::sql::ValidatedExecute::CreateTable(child_spec) =
            crate::sql::validate_execute(child_sql).unwrap()
        else {
            unreachable!()
        };
        let child = CreateTable::prepare(&connection, child_sql, child_spec).unwrap();
        catalog::insert(&connection, &child).unwrap();

        let drop_sql = "DROP TABLE children";
        let crate::sql::ValidatedExecute::DropTable(drop_spec) =
            crate::sql::validate_execute(drop_sql).unwrap()
        else {
            unreachable!()
        };
        let operation = DropTableOperation::prepare(&connection, drop_sql, &drop_spec).unwrap();
        let lowered = operation.to_homebase().unwrap();
        let foreign_key = &child.foreign_keys()[0];
        let reference = constraint_reference_key(
            parent.table_id(),
            foreign_key.referenced_index().as_bytes(),
            foreign_key.id(),
        );

        assert_explicit_range_assertions(&lowered.footprint, std::slice::from_ref(&reference));
        assert!(lowered.footprint.writes().contains(&reference));
        assert!(
            lowered
                .mutations
                .iter()
                .any(|mutation| matches!(mutation, Mutation::Delete { key } if key == &reference))
        );
    }

    #[test]
    fn local_sidecar_restores_composite_rows_and_is_not_part_of_the_wire_frame() {
        let (connection, operation) = operation();
        let encoded = operation.encode();
        operation.capture_local_repair(&connection).unwrap();
        assert_eq!(operation.encode(), encoded);
        repair::validate_expected(&connection, [operation.repair_spec()]).unwrap();

        operation.apply(&connection).unwrap();
        operation.verify_materialized(&connection).unwrap();
        operation.rollback(&connection).unwrap();
        assert_eq!(
            connection
                .prepare("SELECT tenant, sku, hex(body) FROM inventory ORDER BY tenant")
                .unwrap()
                .query_map((), |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, String>(2)?,
                    ))
                })
                .unwrap()
                .collect::<rusqlite::Result<Vec<_>>>()
                .unwrap(),
            [
                ("north".into(), 1, "0001".into()),
                ("south".into(), 2, String::new()),
            ]
        );
        repair::validate_expected(&connection, []).unwrap();
        crate::physical::verify_table(&connection, operation.table_id()).unwrap();
    }

    #[test]
    fn decoding_rejects_malformed_and_contradictory_provenance() {
        let (_, operation) = operation();
        assert_eq!(
            DropTableOperation::decode(&[]),
            Err(DropTableCodecError::UnknownVersion)
        );
        let encoded = operation.encode();
        assert_eq!(
            DropTableOperation::decode(&encoded[..encoded.len() - 1]),
            Err(DropTableCodecError::Truncated)
        );

        let mut contradictory = operation.clone();
        contradictory.sql = "DROP TABLE another_table".into();
        assert_eq!(
            DropTableOperation::decode(&contradictory.encode()),
            Err(DropTableCodecError::SqlMismatch)
        );

        let mut conditional = operation;
        conditional.sql = "DROP TABLE IF EXISTS inventory".into();
        assert_eq!(
            DropTableOperation::decode(&conditional.encode()).unwrap(),
            conditional
        );
    }
}
