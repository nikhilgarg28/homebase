//! Metadata-only synchronized SQLite views.

use std::fmt;

use homebase_core::reader::Reader;
use homebase_core::tag::Mutation;
use homebase_core::writer::Writer;
use rusqlite::{Connection, OptionalExtension};
use uuid::{Uuid, Variant, Version};

use super::guard::{
    GuardPlan, GuardReason, OperationFamily, column_view_dependency_key, view_dependency_key,
};
use super::schema::{
    ColumnId, MutationId, SqlName, TableId, column_name_scope_key, schema_log_key,
    schema_object_name_scope_key,
};
use crate::catalog;
use crate::commit::footprint::ConflictFootprint;
use crate::sql::{CreateViewSpec, DropViewSpec, ValidatedExecute};
use crate::sqlite::quote_identifier;
use crate::{Error, Result};

const VERSION: u8 = 1;
const TAG_MUTATION_ID: u8 = 1;
const TAG_ACTION: u8 = 2;
const TAG_NAME: u8 = 3;
const TAG_CREATE_SQL: u8 = 4;
const TAG_DEPENDENCY: u8 = 5;
const CREATE: u8 = 1;
const DROP: u8 = 2;

const DEPENDENCY_VERSION: u8 = 1;
const TAG_TABLE_ID: u8 = 1;
const TAG_TABLE_NAME: u8 = 2;
const TAG_COLUMN_DEPENDENCY: u8 = 3;

const COLUMN_DEPENDENCY_VERSION: u8 = 1;
const TAG_COLUMN_ID: u8 = 1;
const TAG_COLUMN_NAME: u8 = 2;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ViewAction {
    Create,
    Drop,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ViewDependency {
    table: TableId,
    name: SqlName,
    columns: Vec<ViewColumnDependency>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ViewColumnDependency {
    column: ColumnId,
    name: SqlName,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ViewOperation {
    mutation_id: MutationId,
    action: ViewAction,
    name: SqlName,
    create_sql: String,
    dependencies: Vec<ViewDependency>,
}

pub struct ViewHomebaseOp {
    pub mutations: Vec<Mutation>,
    pub footprint: ConflictFootprint,
    pub guards: GuardPlan,
}

impl ViewOperation {
    pub fn prepare_create(
        connection: &Connection,
        sql: &str,
        spec: &CreateViewSpec,
    ) -> Result<Self> {
        let dependencies = resolve_dependencies(connection, &spec.dependencies)?;
        let operation = Self {
            mutation_id: MutationId::from_bytes(Uuid::new_v4().into_bytes()),
            action: ViewAction::Create,
            name: spec.name.clone(),
            create_sql: sql.to_owned(),
            dependencies,
        };
        operation.validate().map_err(invalid_operation)?;
        Ok(operation)
    }

    pub fn prepare_drop(connection: &Connection, spec: &DropViewSpec) -> Result<Self> {
        let create_sql = connection
            .query_row(
                "SELECT sql FROM main.sqlite_schema WHERE type = 'view' AND name = ?1 COLLATE NOCASE",
                [spec.name.value()],
                |row| row.get::<_, String>(0),
            )
            .optional()?
            .ok_or(Error::UnsupportedSql("DROP VIEW target is not a synchronized view"))?;
        let ValidatedExecute::CreateView(created) = crate::sql::validate_execute(&create_sql)?
        else {
            return Err(Error::InvalidDatabase(
                "stored view SQL is outside the supported grammar",
            ));
        };
        if created.name.canonical() != spec.name.canonical() {
            return Err(Error::InvalidDatabase(
                "stored view name contradicts SQLite schema",
            ));
        }
        let operation = Self {
            mutation_id: MutationId::from_bytes(Uuid::new_v4().into_bytes()),
            action: ViewAction::Drop,
            name: spec.name.clone(),
            create_sql,
            dependencies: resolve_dependencies(connection, &created.dependencies)?,
        };
        operation.validate().map_err(invalid_operation)?;
        Ok(operation)
    }

    pub fn to_homebase(&self) -> Result<ViewHomebaseOp> {
        self.validate().map_err(invalid_operation)?;
        let family = match self.action {
            ViewAction::Create => OperationFamily::CreateView,
            ViewAction::Drop => OperationFamily::DropView,
        };
        let name = schema_object_name_scope_key(&self.name);
        let mut guards = GuardPlan::for_operation(family);
        guards.invariant(name.clone(), GuardReason::SchemaObjectName)?;
        let mut mutations = vec![Mutation::Set {
            key: schema_log_key(self.mutation_id),
            value: self.encode(),
        }];
        for dependency in &self.dependencies {
            let marker = view_dependency_key(dependency.table, &self.name);
            guards.invariant(marker.clone(), GuardReason::ViewDependency)?;
            mutations.push(match self.action {
                ViewAction::Create => Mutation::Set {
                    key: marker,
                    value: self.mutation_id.as_bytes().to_vec(),
                },
                ViewAction::Drop => Mutation::Delete { key: marker },
            });
            if self.action == ViewAction::Create {
                guards.invariant(
                    schema_object_name_scope_key(&dependency.name),
                    GuardReason::ViewDependency,
                )?;
            }
            for column in &dependency.columns {
                let marker =
                    column_view_dependency_key(dependency.table, column.column, &self.name);
                guards.invariant(marker.clone(), GuardReason::ColumnDependency)?;
                guards.write(marker.clone(), GuardReason::ColumnDependency)?;
                mutations.push(match self.action {
                    ViewAction::Create => Mutation::Set {
                        key: marker,
                        value: self.mutation_id.as_bytes().to_vec(),
                    },
                    ViewAction::Drop => Mutation::Delete { key: marker },
                });
                if self.action == ViewAction::Create {
                    guards.invariant(
                        column_name_scope_key(dependency.table, &column.name),
                        GuardReason::ViewDependency,
                    )?;
                }
            }
        }
        mutations.push(match self.action {
            ViewAction::Create => Mutation::Set {
                key: name,
                value: b"view".to_vec(),
            },
            ViewAction::Drop => Mutation::Delete { key: name },
        });
        let footprint = guards.footprint();
        Ok(ViewHomebaseOp {
            mutations,
            footprint,
            guards,
        })
    }

    pub fn apply(&self, connection: &Connection) -> Result<()> {
        self.validate().map_err(invalid_operation)?;
        match self.action {
            ViewAction::Create => {
                self.validate_dependencies(connection)?;
                connection.execute(&self.create_sql, ())?;
                validate_view_query(connection, &self.name)
            }
            ViewAction::Drop => {
                ensure_view(connection, &self.name, true)?;
                connection.execute_batch(&format!(
                    "DROP VIEW {}",
                    quote_identifier(self.name.value())
                ))?;
                Ok(())
            }
        }
    }

    pub fn validate_created(&self, connection: &Connection) -> Result<()> {
        if self.action != ViewAction::Create {
            return Err(Error::CaptureInvariant("DROP VIEW validated as a creation"));
        }
        ensure_view(connection, &self.name, true)?;
        self.validate_dependencies(connection)?;
        validate_view_query(connection, &self.name)
    }

    pub fn rollback(&self, connection: &Connection) -> Result<()> {
        match self.action {
            ViewAction::Create => {
                ensure_view(connection, &self.name, true)?;
                connection.execute_batch(&format!(
                    "DROP VIEW {}",
                    quote_identifier(self.name.value())
                ))?;
                Ok(())
            }
            ViewAction::Drop => {
                ensure_view(connection, &self.name, false)?;
                self.validate_dependencies(connection)?;
                connection.execute(&self.create_sql, ())?;
                validate_view_query(connection, &self.name)
            }
        }
    }

    #[cfg(debug_assertions)]
    pub fn verify_materialized(&self, connection: &Connection) -> Result<()> {
        ensure_view(connection, &self.name, self.action == ViewAction::Create)
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut writer = Writer::new();
        writer.u8(VERSION);
        writer
            .field(TAG_MUTATION_ID, &self.mutation_id.as_bytes())
            .unwrap();
        writer
            .field(
                TAG_ACTION,
                &[match self.action {
                    ViewAction::Create => CREATE,
                    ViewAction::Drop => DROP,
                }],
            )
            .unwrap();
        writer
            .field(TAG_NAME, self.name.value().as_bytes())
            .unwrap();
        writer
            .field(TAG_CREATE_SQL, self.create_sql.as_bytes())
            .unwrap();
        for dependency in &self.dependencies {
            writer
                .field(TAG_DEPENDENCY, &encode_dependency(dependency))
                .unwrap();
        }
        writer.finish()
    }

    pub fn decode(frame: &[u8]) -> std::result::Result<Self, ViewCodecError> {
        let mut reader = Reader::new(frame);
        if reader.u8() != Some(VERSION) {
            return Err(ViewCodecError::UnknownVersion);
        }
        let mut mutation_id = None;
        let mut action = None;
        let mut name = None;
        let mut create_sql = None;
        let mut dependencies = Vec::new();
        while let Some((tag, value)) = reader.field().map_err(|_| ViewCodecError::Truncated)? {
            match tag {
                TAG_MUTATION_ID => {
                    set_once(&mut mutation_id, MutationId::from_bytes(uuid_bytes(value)?))?
                }
                TAG_ACTION => {
                    let [value] = value else {
                        return Err(ViewCodecError::InvalidLength);
                    };
                    set_once(
                        &mut action,
                        match *value {
                            CREATE => ViewAction::Create,
                            DROP => ViewAction::Drop,
                            _ => return Err(ViewCodecError::InvalidAction),
                        },
                    )?;
                }
                TAG_NAME => set_once(&mut name, SqlName::new(decode_string(value)?))?,
                TAG_CREATE_SQL => set_once(&mut create_sql, decode_string(value)?)?,
                TAG_DEPENDENCY => dependencies.push(decode_dependency(value)?),
                _ => {}
            }
        }
        let operation = Self {
            mutation_id: mutation_id.ok_or(ViewCodecError::MissingField(TAG_MUTATION_ID))?,
            action: action.ok_or(ViewCodecError::MissingField(TAG_ACTION))?,
            name: name.ok_or(ViewCodecError::MissingField(TAG_NAME))?,
            create_sql: create_sql.ok_or(ViewCodecError::MissingField(TAG_CREATE_SQL))?,
            dependencies,
        };
        operation.validate()?;
        Ok(operation)
    }

    fn validate(&self) -> std::result::Result<(), ViewCodecError> {
        let ValidatedExecute::CreateView(spec) = crate::sql::validate_execute(&self.create_sql)
            .map_err(|_| ViewCodecError::InvalidSql)?
        else {
            return Err(ViewCodecError::InvalidSql);
        };
        if spec.name != self.name
            || spec.dependencies.len() != self.dependencies.len()
            || spec
                .dependencies
                .iter()
                .zip(&self.dependencies)
                .any(|(expected, actual)| expected != &actual.name)
        {
            return Err(ViewCodecError::SqlMismatch);
        }
        if self.dependencies.is_empty()
            || self
                .dependencies
                .iter()
                .enumerate()
                .any(|(index, dependency)| {
                    self.dependencies[..index].iter().any(|seen| {
                        seen.table == dependency.table
                            || seen.name.canonical() == dependency.name.canonical()
                    })
                })
        {
            return Err(ViewCodecError::InvalidDependencies);
        }
        if self.dependencies.iter().any(|dependency| {
            dependency.columns.is_empty()
                || dependency
                    .columns
                    .iter()
                    .enumerate()
                    .any(|(index, column)| {
                        dependency.columns[..index].iter().any(|seen| {
                            seen.column == column.column
                                || seen.name.canonical() == column.name.canonical()
                        })
                    })
        }) {
            return Err(ViewCodecError::InvalidDependencies);
        }
        Ok(())
    }

    fn validate_dependencies(&self, connection: &Connection) -> Result<()> {
        for dependency in &self.dependencies {
            let current = catalog::by_id(connection, dependency.table)?
                .ok_or(Error::InvalidDatabase("view dependency table is missing"))?;
            let name = catalog::name_by_id(connection, current.table_id())?.ok_or(
                Error::InvalidDatabase("view dependency has no name binding"),
            )?;
            if name.canonical() != dependency.name.canonical() {
                return Err(Error::InvalidDatabase(
                    "view dependency was renamed before materialization",
                ));
            }
            for column in &dependency.columns {
                let current =
                    catalog::column_name_by_id(connection, dependency.table, column.column)?
                        .ok_or(Error::InvalidDatabase("view dependency column is missing"))?;
                if current.canonical() != column.name.canonical() {
                    return Err(Error::InvalidDatabase(
                        "view dependency column was renamed before materialization",
                    ));
                }
            }
        }
        Ok(())
    }
}

/// Refuse DDL that would leave a synchronized view with a missing source.
pub(crate) fn ensure_table_not_referenced(connection: &Connection, table: &SqlName) -> Result<()> {
    let mut statement = connection.prepare(
        "SELECT sql FROM main.sqlite_schema
         WHERE type = 'view' AND sql IS NOT NULL
         ORDER BY lower(name), name",
    )?;
    let rows = statement.query_map((), |row| row.get::<_, String>(0))?;
    for row in rows {
        let sql = row?;
        let ValidatedExecute::CreateView(view) =
            crate::sql::validate_execute(&sql).map_err(|_| {
                Error::InvalidDatabase("stored view SQL is outside the supported grammar")
            })?
        else {
            return Err(Error::InvalidDatabase(
                "stored view SQL does not describe a view",
            ));
        };
        if view
            .dependencies
            .iter()
            .any(|dependency| dependency.canonical() == table.canonical())
        {
            return Err(Error::UnsupportedSql(
                "table is referenced by a synchronized view",
            ));
        }
    }
    Ok(())
}

fn resolve_dependencies(connection: &Connection, names: &[SqlName]) -> Result<Vec<ViewDependency>> {
    names
        .iter()
        .map(|name| {
            let table = catalog::by_name(connection, name.value())?.ok_or(
                Error::UnsupportedSql("views may reference only synchronized base tables"),
            )?;
            let columns = table
                .columns()
                .iter()
                .map(|column| {
                    Ok(ViewColumnDependency {
                        column: column.id(),
                        name: catalog::column_name_by_id(
                            connection,
                            table.table_id(),
                            column.id(),
                        )?
                        .ok_or(Error::InvalidDatabase(
                            "view dependency column has no name binding",
                        ))?,
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(ViewDependency {
                table: table.table_id(),
                name: name.clone(),
                columns,
            })
        })
        .collect()
}

fn validate_view_query(connection: &Connection, name: &SqlName) -> Result<()> {
    let sql = format!("SELECT * FROM {} LIMIT 0", quote_identifier(name.value()));
    connection.prepare(&sql)?;
    Ok(())
}

fn ensure_view(connection: &Connection, name: &SqlName, expected: bool) -> Result<()> {
    let actual = connection
        .query_row(
            "SELECT type FROM main.sqlite_schema WHERE name = ?1 COLLATE NOCASE",
            [name.value()],
            |row| row.get::<_, String>(0),
        )
        .optional()?;
    if actual.as_deref() == expected.then_some("view") {
        Ok(())
    } else {
        Err(Error::InvalidDatabase(
            "pending view no longer matches SQLite schema",
        ))
    }
}

fn encode_dependency(dependency: &ViewDependency) -> Vec<u8> {
    let mut writer = Writer::new();
    writer.u8(DEPENDENCY_VERSION);
    writer
        .field(TAG_TABLE_ID, &dependency.table.as_bytes())
        .unwrap();
    writer
        .field(TAG_TABLE_NAME, dependency.name.value().as_bytes())
        .unwrap();
    for column in &dependency.columns {
        writer
            .field(TAG_COLUMN_DEPENDENCY, &encode_column_dependency(column))
            .unwrap();
    }
    writer.finish()
}

fn decode_dependency(frame: &[u8]) -> std::result::Result<ViewDependency, ViewCodecError> {
    let mut reader = Reader::new(frame);
    if reader.u8() != Some(DEPENDENCY_VERSION) {
        return Err(ViewCodecError::UnknownVersion);
    }
    let mut table = None;
    let mut name = None;
    let mut columns = Vec::new();
    while let Some((tag, value)) = reader.field().map_err(|_| ViewCodecError::Truncated)? {
        match tag {
            TAG_TABLE_ID => set_once(&mut table, TableId::from_bytes(uuid_bytes(value)?))?,
            TAG_TABLE_NAME => set_once(&mut name, SqlName::new(decode_string(value)?))?,
            TAG_COLUMN_DEPENDENCY => columns.push(decode_column_dependency(value)?),
            _ => {}
        }
    }
    Ok(ViewDependency {
        table: table.ok_or(ViewCodecError::MissingField(TAG_TABLE_ID))?,
        name: name.ok_or(ViewCodecError::MissingField(TAG_TABLE_NAME))?,
        columns,
    })
}

fn encode_column_dependency(dependency: &ViewColumnDependency) -> Vec<u8> {
    let mut writer = Writer::new();
    writer.u8(COLUMN_DEPENDENCY_VERSION);
    writer
        .field(TAG_COLUMN_ID, &dependency.column.as_bytes())
        .unwrap();
    writer
        .field(TAG_COLUMN_NAME, dependency.name.value().as_bytes())
        .unwrap();
    writer.finish()
}

fn decode_column_dependency(
    frame: &[u8],
) -> std::result::Result<ViewColumnDependency, ViewCodecError> {
    let mut reader = Reader::new(frame);
    if reader.u8() != Some(COLUMN_DEPENDENCY_VERSION) {
        return Err(ViewCodecError::UnknownVersion);
    }
    let mut column = None;
    let mut name = None;
    while let Some((tag, value)) = reader.field().map_err(|_| ViewCodecError::Truncated)? {
        match tag {
            TAG_COLUMN_ID => set_once(&mut column, ColumnId::from_bytes(uuid_bytes(value)?))?,
            TAG_COLUMN_NAME => set_once(&mut name, SqlName::new(decode_string(value)?))?,
            _ => {}
        }
    }
    Ok(ViewColumnDependency {
        column: column.ok_or(ViewCodecError::MissingField(TAG_COLUMN_ID))?,
        name: name.ok_or(ViewCodecError::MissingField(TAG_COLUMN_NAME))?,
    })
}

fn set_once<T>(slot: &mut Option<T>, value: T) -> std::result::Result<(), ViewCodecError> {
    if slot.replace(value).is_some() {
        Err(ViewCodecError::DuplicateField)
    } else {
        Ok(())
    }
}

fn decode_string(value: &[u8]) -> std::result::Result<String, ViewCodecError> {
    String::from_utf8(value.to_vec()).map_err(|_| ViewCodecError::InvalidUtf8)
}

fn uuid_bytes(value: &[u8]) -> std::result::Result<[u8; 16], ViewCodecError> {
    let bytes = value
        .try_into()
        .map_err(|_| ViewCodecError::InvalidLength)?;
    let uuid = Uuid::from_bytes(bytes);
    if uuid.get_version() != Some(Version::Random) || uuid.get_variant() != Variant::RFC4122 {
        return Err(ViewCodecError::InvalidUuid);
    }
    Ok(bytes)
}

fn invalid_operation(error: ViewCodecError) -> Error {
    Error::InvalidMultiliteOp(error.to_string())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ViewCodecError {
    UnknownVersion,
    Truncated,
    DuplicateField,
    MissingField(u8),
    InvalidLength,
    InvalidUtf8,
    InvalidUuid,
    InvalidAction,
    InvalidSql,
    SqlMismatch,
    InvalidDependencies,
}

impl fmt::Display for ViewCodecError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{self:?}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logical::operation::MultiliteOp;
    use crate::logical::schema::CreateTable;

    fn connection() -> Connection {
        let connection = Connection::open_in_memory().unwrap();
        catalog::initialize(&connection).unwrap();
        let sql = "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)";
        let ValidatedExecute::CreateTable(spec) = crate::sql::validate_execute(sql).unwrap() else {
            unreachable!()
        };
        let table = CreateTable::prepare(&connection, sql, spec).unwrap();
        connection.execute(sql, ()).unwrap();
        catalog::insert(&connection, &table).unwrap();
        connection
    }

    #[test]
    fn create_drop_codec_lowering_apply_and_repair_are_symmetric() {
        let connection = connection();
        let sql = "CREATE VIEW note_bodies AS SELECT id, upper(body) AS body FROM notes";
        let ValidatedExecute::CreateView(spec) = crate::sql::validate_execute(sql).unwrap() else {
            unreachable!()
        };
        let created = ViewOperation::prepare_create(&connection, sql, &spec).unwrap();
        assert_eq!(ViewOperation::decode(&created.encode()).unwrap(), created);
        let logical = MultiliteOp::View(created.clone());
        assert_eq!(MultiliteOp::decode(&logical.encode()).unwrap(), logical);
        let lowered = created.to_homebase().unwrap();
        assert_eq!(lowered.mutations.len(), 5);
        assert_eq!(lowered.footprint.constraints().len(), 7);
        assert_eq!(lowered.footprint.writes().len(), 2);

        created.apply(&connection).unwrap();
        created.rollback(&connection).unwrap();
        created.apply(&connection).unwrap();
        let dropped = ViewOperation::prepare_drop(
            &connection,
            &DropViewSpec {
                name: SqlName::new("note_bodies".into()),
            },
        )
        .unwrap();
        assert_eq!(ViewOperation::decode(&dropped.encode()).unwrap(), dropped);
        dropped.apply(&connection).unwrap();
        dropped.rollback(&connection).unwrap();
        validate_view_query(&connection, &SqlName::new("note_bodies".into())).unwrap();
    }

    #[test]
    fn malformed_frames_and_invalid_views_fail_without_panics() {
        let connection = connection();
        let sql = "CREATE VIEW broken AS SELECT missing FROM notes";
        let ValidatedExecute::CreateView(spec) = crate::sql::validate_execute(sql).unwrap() else {
            unreachable!()
        };
        let operation = ViewOperation::prepare_create(&connection, sql, &spec).unwrap();
        let encoded = operation.encode();
        for length in 0..encoded.len() {
            assert!(ViewOperation::decode(&encoded[..length]).is_err());
        }
        let mut missing_column_contract = operation.clone();
        missing_column_contract.dependencies[0].columns.clear();
        assert_eq!(
            ViewOperation::decode(&missing_column_contract.encode()),
            Err(ViewCodecError::InvalidDependencies)
        );
        connection.execute(sql, ()).unwrap();
        assert!(operation.validate_created(&connection).is_err());
    }
}
