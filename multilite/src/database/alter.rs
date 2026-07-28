//! Identity-preserving table-name binding changes.

use std::fmt;

use homebase_core::reader::Reader;
use homebase_core::tag::Mutation;
use homebase_core::writer::Writer;
use rusqlite::Connection;
use uuid::{Uuid, Variant, Version};

use super::catalog;
use super::schema::{MutationId, SqlName, TableId, schema_log_key, table_name_scope_key};
use super::sql::{RenameTableSpec, ValidatedExecute};
use crate::commit::footprint::ConflictFootprint;
use crate::{Error, Result};

const ALTER_TABLE_VERSION: u8 = 1;
const TAG_MUTATION_ID: u8 = 1;
const TAG_SQL: u8 = 2;
const TAG_TABLE: u8 = 3;
const TAG_OLD_NAME: u8 = 4;
const TAG_NEW_NAME: u8 = 5;

/// One stable table identity moving between two mutable name bindings.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AlterTableOperation {
    mutation_id: MutationId,
    sql: String,
    table: TableId,
    old_name: SqlName,
    new_name: SqlName,
}

/// Homebase mutations and conflict footprint for one table alteration.
pub struct AlterTableHomebaseOp {
    pub mutations: Vec<Mutation>,
    pub footprint: ConflictFootprint,
}

impl AlterTableOperation {
    /// Resolve the SQL source name once, then retain only its stable identity.
    pub fn prepare_rename(
        connection: &Connection,
        sql: &str,
        spec: &RenameTableSpec,
    ) -> Result<Self> {
        let created = catalog::by_name(connection, spec.table.value())?.ok_or(
            Error::UnsupportedSql("ALTER TABLE target has no synchronized schema identity"),
        )?;
        if catalog::by_name(connection, spec.new_name.value())?.is_some() {
            return Err(Error::UnsupportedSql(
                "ALTER TABLE target name is already bound",
            ));
        }
        let operation = Self {
            mutation_id: MutationId::from_bytes(Uuid::new_v4().into_bytes()),
            sql: sql.to_owned(),
            table: created.table_id(),
            old_name: spec.table.clone(),
            new_name: spec.new_name.clone(),
        };
        operation.validate().map_err(invalid_operation)?;
        Ok(operation)
    }

    /// Move the name registry without evolving table-owned schema state.
    pub fn to_homebase(&self) -> Result<AlterTableHomebaseOp> {
        self.validate().map_err(invalid_operation)?;
        let old_name = table_name_scope_key(&self.old_name);
        let new_name = table_name_scope_key(&self.new_name);
        let mut footprint = ConflictFootprint::new();
        footprint.add_constraint(old_name.clone());
        footprint.add_constraint(new_name.clone());
        Ok(AlterTableHomebaseOp {
            mutations: vec![
                Mutation::Set {
                    key: schema_log_key(self.mutation_id),
                    value: self.encode(),
                },
                Mutation::Delete { key: old_name },
                Mutation::Set {
                    key: new_name,
                    value: self.table.as_bytes().to_vec(),
                },
            ],
            footprint,
        })
    }

    /// Apply an authenticated binding change to canonical SQLite.
    pub fn apply(&self, connection: &Connection) -> Result<()> {
        self.validate().map_err(invalid_operation)?;
        self.validate_catalog_before(connection)?;
        connection.execute_batch(&rename_sql(&self.old_name, &self.new_name))?;
        self.record_catalog(connection)
    }

    /// Record the binding change after a branch has executed the user's SQL.
    pub fn record_catalog(&self, connection: &Connection) -> Result<()> {
        self.validate_catalog_before(connection)?;
        catalog::rename_binding(connection, self.table, &self.old_name, &self.new_name)
    }

    /// Reverse one speculative binding change after authority rejects it.
    pub fn rollback(&self, connection: &Connection) -> Result<()> {
        self.validate().map_err(invalid_operation)?;
        self.validate_catalog_after(connection)?;
        connection.execute_batch(&rename_sql(&self.new_name, &self.old_name))?;
        catalog::rename_binding(connection, self.table, &self.new_name, &self.old_name)
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut writer = Writer::new();
        writer.u8(ALTER_TABLE_VERSION);
        writer
            .field(TAG_MUTATION_ID, &self.mutation_id.as_bytes())
            .expect("ALTER TABLE field fits in u32");
        writer
            .field(TAG_SQL, self.sql.as_bytes())
            .expect("ALTER TABLE field fits in u32");
        writer
            .field(TAG_TABLE, &self.table.as_bytes())
            .expect("ALTER TABLE field fits in u32");
        writer
            .field(TAG_OLD_NAME, self.old_name.value().as_bytes())
            .expect("ALTER TABLE field fits in u32");
        writer
            .field(TAG_NEW_NAME, self.new_name.value().as_bytes())
            .expect("ALTER TABLE field fits in u32");
        writer.finish()
    }

    pub fn decode(frame: &[u8]) -> std::result::Result<Self, AlterTableCodecError> {
        let mut reader = Reader::new(frame);
        if reader.u8() != Some(ALTER_TABLE_VERSION) {
            return Err(AlterTableCodecError::UnknownVersion);
        }
        let mut mutation_id = None;
        let mut sql = None;
        let mut table = None;
        let mut old_name = None;
        let mut new_name = None;
        while let Some((tag, value)) = reader
            .field()
            .map_err(|_| AlterTableCodecError::Truncated)?
        {
            match tag {
                TAG_MUTATION_ID => {
                    set_once(&mut mutation_id, MutationId::from_bytes(uuid_bytes(value)?))?
                }
                TAG_SQL => set_once(&mut sql, decode_string(value)?)?,
                TAG_TABLE => set_once(&mut table, TableId::from_bytes(uuid_bytes(value)?))?,
                TAG_OLD_NAME => set_once(&mut old_name, SqlName::new(decode_string(value)?))?,
                TAG_NEW_NAME => set_once(&mut new_name, SqlName::new(decode_string(value)?))?,
                _ => {}
            }
        }
        let operation = Self {
            mutation_id: mutation_id.ok_or(AlterTableCodecError::MissingField(TAG_MUTATION_ID))?,
            sql: sql.ok_or(AlterTableCodecError::MissingField(TAG_SQL))?,
            table: table.ok_or(AlterTableCodecError::MissingField(TAG_TABLE))?,
            old_name: old_name.ok_or(AlterTableCodecError::MissingField(TAG_OLD_NAME))?,
            new_name: new_name.ok_or(AlterTableCodecError::MissingField(TAG_NEW_NAME))?,
        };
        operation.validate()?;
        Ok(operation)
    }

    fn validate(&self) -> std::result::Result<(), AlterTableCodecError> {
        let ValidatedExecute::RenameTable(spec) = super::sql::validate_execute(&self.sql)
            .map_err(|_| AlterTableCodecError::InvalidSql)?
        else {
            return Err(AlterTableCodecError::InvalidSql);
        };
        if spec.table != self.old_name || spec.new_name != self.new_name {
            return Err(AlterTableCodecError::InvalidRename);
        }
        Ok(())
    }

    fn validate_catalog_before(&self, connection: &Connection) -> Result<()> {
        let current = catalog::name_by_id(connection, self.table)?.ok_or(
            Error::InvalidDatabase("table rename identity is missing from the schema catalog"),
        )?;
        if current.canonical() != self.old_name.canonical()
            || catalog::by_name(connection, self.new_name.value())?.is_some()
        {
            return Err(Error::InvalidDatabase(
                "table rename no longer matches the schema catalog",
            ));
        }
        Ok(())
    }

    fn validate_catalog_after(&self, connection: &Connection) -> Result<()> {
        let current = catalog::name_by_id(connection, self.table)?.ok_or(
            Error::InvalidDatabase("pending table rename identity is missing from the catalog"),
        )?;
        if current.canonical() != self.new_name.canonical()
            || catalog::by_name(connection, self.old_name.value())?.is_some()
        {
            return Err(Error::InvalidDatabase(
                "pending table rename no longer matches SQLite state",
            ));
        }
        Ok(())
    }
}

fn rename_sql(from: &SqlName, to: &SqlName) -> String {
    format!(
        "ALTER TABLE {} RENAME TO {}",
        quote_identifier(from.value()),
        quote_identifier(to.value())
    )
}

fn quote_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

fn decode_string(value: &[u8]) -> std::result::Result<String, AlterTableCodecError> {
    String::from_utf8(value.to_vec()).map_err(|_| AlterTableCodecError::InvalidUtf8)
}

fn set_once<T>(slot: &mut Option<T>, value: T) -> std::result::Result<(), AlterTableCodecError> {
    if slot.replace(value).is_some() {
        Err(AlterTableCodecError::DuplicateField)
    } else {
        Ok(())
    }
}

fn uuid_bytes(value: &[u8]) -> std::result::Result<[u8; 16], AlterTableCodecError> {
    let bytes = value
        .try_into()
        .map_err(|_| AlterTableCodecError::InvalidLength)?;
    let uuid = Uuid::from_bytes(bytes);
    if uuid.get_version() != Some(Version::Random) || uuid.get_variant() != Variant::RFC4122 {
        return Err(AlterTableCodecError::InvalidUuid);
    }
    Ok(bytes)
}

fn invalid_operation(error: AlterTableCodecError) -> Error {
    Error::InvalidMultiliteOp(error.to_string())
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AlterTableCodecError {
    UnknownVersion,
    Truncated,
    DuplicateField,
    MissingField(u8),
    InvalidLength,
    InvalidUtf8,
    InvalidUuid,
    InvalidSql,
    InvalidRename,
}

impl fmt::Display for AlterTableCodecError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownVersion => formatter.write_str("unknown ALTER TABLE frame version"),
            Self::Truncated => formatter.write_str("truncated ALTER TABLE frame"),
            Self::DuplicateField => formatter.write_str("duplicate ALTER TABLE field"),
            Self::MissingField(tag) => write!(formatter, "missing ALTER TABLE field {tag}"),
            Self::InvalidLength => formatter.write_str("invalid ALTER TABLE field length"),
            Self::InvalidUtf8 => formatter.write_str("ALTER TABLE text is not UTF-8"),
            Self::InvalidUuid => formatter.write_str("ALTER TABLE identity is not a UUID v4"),
            Self::InvalidSql => formatter.write_str("invalid ALTER TABLE SQL"),
            Self::InvalidRename => {
                formatter.write_str("ALTER TABLE SQL contradicts its stable binding change")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use homebase_core::tag::Mutation;

    use super::*;
    use crate::database::schema::{
        CreateColumn, CreateTable, CreateTableSpec, TableMode, TableStorage, TypeDeclaration,
    };

    fn connection() -> (Connection, CreateTable) {
        let connection = Connection::open_in_memory().unwrap();
        catalog::initialize(&connection).unwrap();
        let created = CreateTable::new(
            "CREATE TABLE notes (id INTEGER PRIMARY KEY)",
            CreateTableSpec {
                name: SqlName::new("notes".into()),
                mode: TableMode::Ordinary,
                storage: TableStorage::Rowid,
                columns: vec![CreateColumn {
                    name: SqlName::new("id".into()),
                    declared_type: TypeDeclaration::integer(),
                    not_null: false,
                    not_null_name: None,
                    default: None,
                    primary_key: Some(0),
                }],
                primary_key_name: None,
                unique_constraints: Vec::new(),
                foreign_keys: Vec::new(),
                checks: Vec::new(),
            },
        );
        connection.execute(created.sql(), ()).unwrap();
        catalog::insert(&connection, &created).unwrap();
        (connection, created)
    }

    fn operation(connection: &Connection) -> AlterTableOperation {
        let sql = "ALTER TABLE notes RENAME TO \"Archived Notes\"";
        let ValidatedExecute::RenameTable(spec) = super::super::sql::validate_execute(sql).unwrap()
        else {
            unreachable!()
        };
        AlterTableOperation::prepare_rename(connection, sql, &spec).unwrap()
    }

    #[test]
    fn codec_and_homebase_form_contain_only_identity_and_name_bindings() {
        let (connection, created) = connection();
        let operation = operation(&connection);
        assert_eq!(
            AlterTableOperation::decode(&operation.encode()).unwrap(),
            operation
        );

        let lowered = operation.to_homebase().unwrap();
        assert_eq!(lowered.mutations.len(), 3);
        assert_eq!(lowered.footprint.constraints().len(), 2);
        assert!(matches!(lowered.mutations[0], Mutation::Set { .. }));
        assert!(matches!(lowered.mutations[1], Mutation::Delete { .. }));
        let Mutation::Set { value, .. } = &lowered.mutations[2] else {
            panic!("new name registry entry was not set")
        };
        assert_eq!(value, &created.table_id().as_bytes());
    }

    #[test]
    fn apply_and_rollback_move_only_the_mutable_binding() {
        let (connection, created) = connection();
        let definition = created.encode();
        let operation = operation(&connection);

        operation.apply(&connection).unwrap();
        assert!(catalog::by_name(&connection, "notes").unwrap().is_none());
        assert_eq!(
            catalog::by_name(&connection, "archived notes")
                .unwrap()
                .unwrap()
                .encode(),
            definition
        );
        operation.rollback(&connection).unwrap();
        assert_eq!(
            catalog::by_name(&connection, "notes")
                .unwrap()
                .unwrap()
                .encode(),
            definition
        );
        catalog::validate(&connection).unwrap();
    }

    #[test]
    fn codec_rejects_truncation_and_sql_metadata_mismatch() {
        let (connection, _) = connection();
        let operation = operation(&connection);
        let encoded = operation.encode();
        assert_eq!(
            AlterTableOperation::decode(&[]),
            Err(AlterTableCodecError::UnknownVersion)
        );
        assert_eq!(
            AlterTableOperation::decode(&encoded[..encoded.len() - 1]),
            Err(AlterTableCodecError::Truncated)
        );

        let mut mismatched = operation;
        mismatched.new_name = SqlName::new("different".into());
        assert_eq!(
            AlterTableOperation::decode(&mismatched.encode()),
            Err(AlterTableCodecError::InvalidRename)
        );
    }
}
