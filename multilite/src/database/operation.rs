//! Logical Multilite operations and their durable representation.

use std::fmt;

use homebase_core::reader::Reader;
use homebase_core::tag::Mutation;
use homebase_core::writer::Writer;
use rusqlite::Connection;

use super::alter::{AlterTableHomebaseOp, AlterTableOperation};
use super::catalog;
use super::index::{IndexHomebaseOp, IndexOperation};
use super::row::{DeleteRows, InsertRows, RowHomebaseOp, UpdateRows};
use super::schema::CreateTable;
#[cfg(test)]
use super::schema::{CreateTableSpec, write_revision_key};
use crate::commit::footprint::ConflictFootprint;
use crate::{Error, Result};

const OPERATION_FRAME_VERSION: u8 = 1;
const CREATE_TABLE_OPERATION: u8 = 1;
const INSERT_ROWS_OPERATION: u8 = 2;
const DELETE_ROWS_OPERATION: u8 = 3;
const UPDATE_ROWS_OPERATION: u8 = 4;
const INDEX_OPERATION: u8 = 5;
const ALTER_TABLE_OPERATION: u8 = 6;

/// One logical Multilite operation, independent of its Homebase envelope.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MultiliteOp {
    AlterTable(AlterTableOperation),
    CreateTable(CreateTable),
    InsertRows(InsertRows),
    DeleteRows(DeleteRows),
    UpdateRows(UpdateRows),
    Index(IndexOperation),
}

/// Homebase mutations and conflict footprint for one [`MultiliteOp`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HomebaseOp {
    pub mutations: Vec<Mutation>,
    footprint: ConflictFootprint,
}

impl HomebaseOp {
    /// Split deterministic mutations from their logical conflict footprint.
    pub fn into_parts(self) -> (Vec<Mutation>, ConflictFootprint) {
        (self.mutations, self.footprint)
    }
}

impl MultiliteOp {
    /// Mint durable schema identities for one validated table creation.
    #[cfg(test)]
    pub fn create_table(sql: &str, spec: CreateTableSpec) -> Self {
        Self::CreateTable(CreateTable::new(sql, spec))
    }

    /// Encode one complete logical operation for transaction and pending frames.
    pub fn encode(&self) -> Vec<u8> {
        let mut writer = Writer::new();
        writer.u8(OPERATION_FRAME_VERSION);
        match self {
            Self::AlterTable(altered) => {
                writer.u8(ALTER_TABLE_OPERATION);
                writer.bytes(&altered.encode());
            }
            Self::CreateTable(created) => {
                writer.u8(CREATE_TABLE_OPERATION);
                writer.bytes(&created.encode());
            }
            Self::InsertRows(inserted) => {
                writer.u8(INSERT_ROWS_OPERATION);
                writer.bytes(&inserted.encode());
            }
            Self::DeleteRows(deleted) => {
                writer.u8(DELETE_ROWS_OPERATION);
                writer.bytes(&deleted.encode());
            }
            Self::UpdateRows(updated) => {
                writer.u8(UPDATE_ROWS_OPERATION);
                writer.bytes(&updated.encode());
            }
            Self::Index(index) => {
                writer.u8(INDEX_OPERATION);
                writer.bytes(&index.encode());
            }
        }
        writer.finish()
    }

    /// Decode and validate one complete logical operation.
    pub fn decode(frame: &[u8]) -> std::result::Result<Self, OperationCodecError> {
        let mut reader = Reader::new(frame);
        let version = reader.u8().ok_or(OperationCodecError::Truncated)?;
        if version != OPERATION_FRAME_VERSION {
            return Err(OperationCodecError::UnknownVersion(version));
        }
        match reader.u8().ok_or(OperationCodecError::Truncated)? {
            ALTER_TABLE_OPERATION => AlterTableOperation::decode(reader.rest())
                .map(Self::AlterTable)
                .map_err(|error| OperationCodecError::InvalidPayload(error.to_string())),
            CREATE_TABLE_OPERATION => CreateTable::decode(reader.rest())
                .map(Self::CreateTable)
                .map_err(|error| OperationCodecError::InvalidPayload(error.to_string())),
            INSERT_ROWS_OPERATION => InsertRows::decode(reader.rest())
                .map(Self::InsertRows)
                .map_err(|error| OperationCodecError::InvalidPayload(error.to_string())),
            DELETE_ROWS_OPERATION => DeleteRows::decode(reader.rest())
                .map(Self::DeleteRows)
                .map_err(|error| OperationCodecError::InvalidPayload(error.to_string())),
            UPDATE_ROWS_OPERATION => UpdateRows::decode(reader.rest())
                .map(Self::UpdateRows)
                .map_err(|error| OperationCodecError::InvalidPayload(error.to_string())),
            INDEX_OPERATION => IndexOperation::decode(reader.rest())
                .map(Self::Index)
                .map_err(|error| OperationCodecError::InvalidPayload(error.to_string())),
            kind => Err(OperationCodecError::UnknownKind(kind)),
        }
    }

    /// Lower this operation to its complete Homebase representation.
    pub fn to_homebase(&self) -> Result<HomebaseOp> {
        let (mutations, footprint) = match self {
            Self::AlterTable(altered) => {
                let AlterTableHomebaseOp {
                    mutations,
                    footprint,
                } = altered.to_homebase()?;
                (mutations, footprint)
            }
            Self::CreateTable(created) => {
                let schema = created.to_homebase();
                (schema.mutations, schema.footprint)
            }
            Self::InsertRows(inserted) => {
                let RowHomebaseOp {
                    mutations,
                    footprint,
                } = inserted
                    .to_homebase()
                    .map_err(|error| Error::InvalidMultiliteOp(error.to_string()))?;
                (mutations, footprint)
            }
            Self::DeleteRows(deleted) => {
                let RowHomebaseOp {
                    mutations,
                    footprint,
                } = deleted
                    .to_homebase()
                    .map_err(|error| Error::InvalidMultiliteOp(error.to_string()))?;
                (mutations, footprint)
            }
            Self::UpdateRows(updated) => {
                let RowHomebaseOp {
                    mutations,
                    footprint,
                } = updated
                    .to_homebase()
                    .map_err(|error| Error::InvalidMultiliteOp(error.to_string()))?;
                (mutations, footprint)
            }
            Self::Index(index) => {
                let IndexHomebaseOp {
                    mutations,
                    footprint,
                } = index.to_homebase()?;
                (mutations, footprint)
            }
        };
        Ok(HomebaseOp {
            mutations,
            footprint,
        })
    }

    /// Materialize this logical operation in canonical SQLite.
    pub fn apply(&self, connection: &Connection) -> Result<()> {
        match self {
            Self::AlterTable(altered) => altered.apply(connection),
            Self::CreateTable(created) => {
                created.validate_foreign_key_parents(connection)?;
                connection.execute(&created.materialization_sql(connection)?, ())?;
                catalog::insert(connection, created)
            }
            Self::InsertRows(inserted) => inserted.apply(connection),
            Self::DeleteRows(deleted) => deleted.apply(connection),
            Self::UpdateRows(updated) => updated.apply(connection),
            Self::Index(index) => index.apply(connection),
        }
    }
}

/// Failure to decode one logical operation frame.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OperationCodecError {
    UnknownVersion(u8),
    Truncated,
    UnknownKind(u8),
    InvalidPayload(String),
}

impl fmt::Display for OperationCodecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownVersion(version) => {
                write!(f, "unknown Multilite operation version {version}")
            }
            Self::Truncated => f.write_str("Multilite operation frame is truncated"),
            Self::UnknownKind(kind) => write!(f, "unknown Multilite operation kind {kind}"),
            Self::InvalidPayload(error) => write!(f, "invalid operation payload: {error}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use rusqlite::Connection;

    use super::*;
    use crate::database::catalog;
    use crate::database::row::{CapturedRow, DeleteRows, StoredValue, UpdateRows};
    use crate::database::schema::{CreateColumn, SqlName, TypeDeclaration};

    fn table() -> CreateTableSpec {
        CreateTableSpec {
            name: SqlName::new("notes".into()),
            mode: Default::default(),
            storage: crate::database::schema::TableStorage::Rowid,
            columns: vec![CreateColumn {
                name: SqlName::new("id".into()),
                declared_type: TypeDeclaration::integer(),
                not_null: false,
                not_null_name: None,
                default: None,
                primary_key: Some(0),
            }],
            unique_constraints: Vec::new(),
            foreign_keys: Vec::new(),
            primary_key_name: None,
            checks: Vec::new(),
        }
    }

    #[test]
    fn operation_dispatches_schema_translation_and_exposes_its_footprint() {
        let operation =
            MultiliteOp::create_table("CREATE TABLE notes (id INTEGER PRIMARY KEY)", table());
        let (mutations, footprint) = operation.to_homebase().unwrap().into_parts();

        assert_eq!(mutations.len(), 8);
        assert_eq!(footprint.constraints().len(), 1);
        assert!(footprint.constraints().contains(mutations[1].key()));
        assert_eq!(footprint.writes().len(), 1);
        assert!(
            footprint
                .writes()
                .contains(&write_revision_key(match &operation {
                    MultiliteOp::CreateTable(created) => created.table_id(),
                    _ => unreachable!(),
                }))
        );
        assert!(footprint.reads().is_empty());
    }

    #[test]
    fn operation_codec_roundtrips_insert_rows() {
        let connection = Connection::open_in_memory().unwrap();
        catalog::initialize(&connection).unwrap();
        let created = CreateTable::new("CREATE TABLE notes (id INTEGER PRIMARY KEY)", table());
        connection.execute(created.sql(), ()).unwrap();
        catalog::insert(&connection, &created).unwrap();
        let inserted = InsertRows::from_captured(
            &connection,
            &[CapturedRow {
                table: "notes".into(),
                rowid: 7,
                values: vec![StoredValue::Integer(7)],
            }],
        )
        .unwrap()
        .unwrap();
        let operation = MultiliteOp::InsertRows(inserted);

        assert_eq!(MultiliteOp::decode(&operation.encode()).unwrap(), operation);
        assert_eq!(
            MultiliteOp::decode(&[]),
            Err(OperationCodecError::Truncated)
        );
        assert_eq!(
            MultiliteOp::decode(&[2, CREATE_TABLE_OPERATION]),
            Err(OperationCodecError::UnknownVersion(2))
        );

        let deleted = MultiliteOp::DeleteRows(
            DeleteRows::from_captured(
                &connection,
                &[CapturedRow {
                    table: "notes".into(),
                    rowid: 7,
                    values: vec![StoredValue::Integer(7)],
                }],
            )
            .unwrap()
            .unwrap(),
        );
        assert_eq!(MultiliteOp::decode(&deleted.encode()).unwrap(), deleted);
        let (mutations, footprint) = deleted.to_homebase().unwrap().into_parts();
        assert!(matches!(mutations.as_slice(), [Mutation::Delete { .. }]));
        assert_eq!(footprint.writes().len(), 1);

        let update_spec = CreateTableSpec {
            name: SqlName::new("updates".into()),
            mode: Default::default(),
            storage: crate::database::schema::TableStorage::Rowid,
            columns: vec![
                CreateColumn {
                    name: SqlName::new("id".into()),
                    declared_type: TypeDeclaration::integer(),
                    not_null: false,
                    not_null_name: None,
                    default: None,
                    primary_key: Some(0),
                },
                CreateColumn {
                    name: SqlName::new("body".into()),
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
        };
        let update_table = CreateTable::new(
            "CREATE TABLE updates (id INTEGER PRIMARY KEY, body TEXT)",
            update_spec,
        );
        connection.execute(update_table.sql(), ()).unwrap();
        catalog::insert(&connection, &update_table).unwrap();
        let updated = MultiliteOp::UpdateRows(
            UpdateRows::from_captured(
                &connection,
                &[(
                    CapturedRow {
                        table: "updates".into(),
                        rowid: 7,
                        values: vec![
                            StoredValue::Integer(7),
                            StoredValue::Text(b"before".to_vec()),
                        ],
                    },
                    CapturedRow {
                        table: "updates".into(),
                        rowid: 7,
                        values: vec![
                            StoredValue::Integer(7),
                            StoredValue::Text(b"after".to_vec()),
                        ],
                    },
                )],
            )
            .unwrap()
            .unwrap(),
        );
        assert_eq!(MultiliteOp::decode(&updated.encode()).unwrap(), updated);
        let (mutations, footprint) = updated.to_homebase().unwrap().into_parts();
        assert!(matches!(mutations.as_slice(), [Mutation::Set { .. }]));
        assert_eq!(footprint.writes().len(), 1);
    }

    #[test]
    fn create_table_apply_renders_current_foreign_parent_names() {
        let source = Connection::open_in_memory().unwrap();
        catalog::initialize(&source).unwrap();
        let parent_sql = "CREATE TABLE parents (id INTEGER PRIMARY KEY)";
        let crate::database::sql::ValidatedExecute::CreateTable(parent_spec) =
            crate::database::sql::validate_execute(parent_sql).unwrap()
        else {
            unreachable!()
        };
        let parent = CreateTable::new(parent_sql, parent_spec);
        source.execute(parent.sql(), ()).unwrap();
        catalog::insert(&source, &parent).unwrap();
        let child_sql = "CREATE TABLE children (
            id INTEGER PRIMARY KEY,
            parent_id INTEGER REFERENCES parents(id)
        )";
        let crate::database::sql::ValidatedExecute::CreateTable(child_spec) =
            crate::database::sql::validate_execute(child_sql).unwrap()
        else {
            unreachable!()
        };
        let child = CreateTable::prepare(&source, child_sql, child_spec).unwrap();
        let encoded_child = child.encode();

        let target = Connection::open_in_memory().unwrap();
        target.execute_batch("PRAGMA foreign_keys = ON").unwrap();
        catalog::initialize(&target).unwrap();
        target.execute(parent.sql(), ()).unwrap();
        catalog::insert(&target, &parent).unwrap();
        target
            .execute("ALTER TABLE parents RENAME TO accounts", ())
            .unwrap();
        catalog::rename_binding(
            &target,
            parent.table_id(),
            parent.table_name_identity(),
            &SqlName::new("accounts".into()),
        )
        .unwrap();

        MultiliteOp::CreateTable(child.clone())
            .apply(&target)
            .unwrap();

        assert_eq!(
            target
                .query_row("PRAGMA foreign_key_list(children)", (), |row| {
                    row.get::<_, String>(2)
                })
                .unwrap(),
            "accounts"
        );
        assert_eq!(
            catalog::by_id(&target, child.table_id())
                .unwrap()
                .unwrap()
                .encode(),
            encoded_child
        );
        target
            .execute("INSERT INTO accounts VALUES (1)", ())
            .unwrap();
        target
            .execute("INSERT INTO children VALUES (1, 1)", ())
            .unwrap();
        catalog::validate(&target).unwrap();
    }
}
