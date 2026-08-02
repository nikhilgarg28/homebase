//! Logical Multilite operations and their durable representation.

use std::fmt;

use homebase_core::reader::Reader;
use homebase_core::tag::Mutation;
use homebase_core::writer::Writer;
use rusqlite::Connection;

use super::alter::{AlterTableHomebaseOp, AlterTableOperation};
use super::drop_table::{DropTableHomebaseOp, DropTableOperation};
use super::guard::{GuardPlan, RejectionKind, validate_compiled_output, validate_rejection};
use super::index::{IndexHomebaseOp, IndexOperation};
use super::row::{RowChanges, RowHomebaseOp};
use super::schema::CreateTable;
#[cfg(test)]
use super::schema::{CreateTableSpec, write_revision_key};
use super::user_version::{SetUserVersion, UserVersionHomebaseOp};
use super::view::{ViewHomebaseOp, ViewOperation};
use crate::Result;
use crate::catalog;
use crate::commit::footprint::ConflictFootprint;

const OPERATION_FRAME_VERSION: u8 = 1;
const CREATE_TABLE_OPERATION: u8 = 1;
const ROW_CHANGES_OPERATION: u8 = 2;
const INDEX_OPERATION: u8 = 5;
const ALTER_TABLE_OPERATION: u8 = 6;
const DROP_TABLE_OPERATION: u8 = 7;
const SET_USER_VERSION_OPERATION: u8 = 8;
const VIEW_OPERATION: u8 = 9;

/// One logical Multilite operation, independent of its Homebase envelope.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MultiliteOp {
    AlterTable(AlterTableOperation),
    CreateTable(CreateTable),
    DropTable(DropTableOperation),
    ChangeRows(RowChanges),
    Index(IndexOperation),
    SetUserVersion(SetUserVersion),
    View(ViewOperation),
}

/// Homebase mutations and conflict footprint for one [`MultiliteOp`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HomebaseOp {
    pub mutations: Vec<Mutation>,
    footprint: ConflictFootprint,
    guards: GuardPlan,
}

/// Local inverse selected while compiling one speculative operation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RejectionEffect {
    RevertAlterTable { operation: AlterTableOperation },
    RemoveCreatedTable { created: CreateTable },
    RestoreDroppedTable { operation: DropTableOperation },
    RestoreRowChanges { changes: RowChanges },
    RevertIndex { operation: IndexOperation },
    RestoreUserVersion { operation: SetUserVersion },
    RevertView { operation: ViewOperation },
}

/// One logical operation and every deterministic artifact derived from it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CompiledOperation {
    logical: MultiliteOp,
    homebase: HomebaseOp,
    rejection: RejectionEffect,
}

impl HomebaseOp {
    /// Split deterministic mutations from their logical conflict footprint.
    #[cfg(test)]
    pub fn into_parts(self) -> (Vec<Mutation>, ConflictFootprint) {
        (self.mutations, self.footprint)
    }

    pub fn guards(&self) -> &GuardPlan {
        &self.guards
    }

    pub(super) fn into_all_parts(self) -> (Vec<Mutation>, ConflictFootprint, GuardPlan) {
        (self.mutations, self.footprint, self.guards)
    }
}

impl CompiledOperation {
    pub fn logical(&self) -> &MultiliteOp {
        &self.logical
    }

    pub fn homebase(&self) -> &HomebaseOp {
        &self.homebase
    }

    pub(super) fn into_parts(self) -> (MultiliteOp, HomebaseOp, RejectionEffect) {
        (self.logical, self.homebase, self.rejection)
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
            Self::DropTable(dropped) => {
                writer.u8(DROP_TABLE_OPERATION);
                writer.bytes(&dropped.encode());
            }
            Self::ChangeRows(changes) => {
                writer.u8(ROW_CHANGES_OPERATION);
                writer.bytes(&changes.encode());
            }
            Self::Index(index) => {
                writer.u8(INDEX_OPERATION);
                writer.bytes(&index.encode());
            }
            Self::SetUserVersion(operation) => {
                writer.u8(SET_USER_VERSION_OPERATION);
                writer.bytes(&operation.encode());
            }
            Self::View(operation) => {
                writer.u8(VIEW_OPERATION);
                writer.bytes(&operation.encode());
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
            CREATE_TABLE_OPERATION => CreateTable::decode_operation(reader.rest())
                .map(Self::CreateTable)
                .map_err(|error| OperationCodecError::InvalidPayload(error.to_string())),
            DROP_TABLE_OPERATION => DropTableOperation::decode(reader.rest())
                .map(Self::DropTable)
                .map_err(|error| OperationCodecError::InvalidPayload(error.to_string())),
            ROW_CHANGES_OPERATION => RowChanges::decode(reader.rest())
                .map(Self::ChangeRows)
                .map_err(|error| OperationCodecError::InvalidPayload(error.to_string())),
            INDEX_OPERATION => IndexOperation::decode(reader.rest())
                .map(Self::Index)
                .map_err(|error| OperationCodecError::InvalidPayload(error.to_string())),
            SET_USER_VERSION_OPERATION => SetUserVersion::decode(reader.rest())
                .map(Self::SetUserVersion)
                .map_err(|error| OperationCodecError::InvalidPayload(error.to_string())),
            VIEW_OPERATION => ViewOperation::decode(reader.rest())
                .map(Self::View)
                .map_err(|error| OperationCodecError::InvalidPayload(error.to_string())),
            kind => Err(OperationCodecError::UnknownKind(kind)),
        }
    }

    /// Validate and derive every deterministic artifact for this operation.
    pub fn compile(self) -> Result<CompiledOperation> {
        let homebase = self.to_homebase()?;
        let (rejection, rejection_kind) = match &self {
            Self::AlterTable(operation) => (
                RejectionEffect::RevertAlterTable {
                    operation: operation.clone(),
                },
                RejectionKind::RevertAlterTable,
            ),
            Self::CreateTable(created) => (
                RejectionEffect::RemoveCreatedTable {
                    created: created.clone(),
                },
                RejectionKind::RemoveCreatedTable,
            ),
            Self::DropTable(operation) => (
                RejectionEffect::RestoreDroppedTable {
                    operation: operation.clone(),
                },
                RejectionKind::RestoreDroppedTable,
            ),
            Self::ChangeRows(changes) => (
                RejectionEffect::RestoreRowChanges {
                    changes: changes.clone(),
                },
                RejectionKind::RestoreRowChanges,
            ),
            Self::Index(operation) => (
                RejectionEffect::RevertIndex {
                    operation: operation.clone(),
                },
                RejectionKind::RevertIndex,
            ),
            Self::SetUserVersion(operation) => (
                RejectionEffect::RestoreUserVersion {
                    operation: operation.clone(),
                },
                RejectionKind::RestoreUserVersion,
            ),
            Self::View(operation) => (
                RejectionEffect::RevertView {
                    operation: operation.clone(),
                },
                RejectionKind::RevertView,
            ),
        };
        validate_rejection(
            homebase
                .guards
                .operation()
                .ok_or(crate::Error::CaptureInvariant(
                    "operation lowering produced an unscoped guard plan",
                ))?,
            rejection_kind,
        )?;
        Ok(CompiledOperation {
            logical: self,
            homebase,
            rejection,
        })
    }

    /// Lower this operation to its complete Homebase representation.
    pub fn to_homebase(&self) -> Result<HomebaseOp> {
        let (mutations, footprint, guards) = match self {
            Self::AlterTable(altered) => {
                let AlterTableHomebaseOp {
                    mutations,
                    footprint,
                    guards,
                } = altered.to_homebase()?;
                (mutations, footprint, guards)
            }
            Self::CreateTable(created) => {
                let schema = created.to_homebase()?;
                (schema.mutations, schema.footprint, schema.guards)
            }
            Self::DropTable(dropped) => {
                let DropTableHomebaseOp {
                    mutations,
                    footprint,
                    guards,
                } = dropped.to_homebase()?;
                (mutations, footprint, guards)
            }
            Self::ChangeRows(changes) => {
                let RowHomebaseOp {
                    mutations,
                    footprint,
                    guards,
                } = changes.to_homebase()?;
                (mutations, footprint, guards)
            }
            Self::Index(index) => {
                let IndexHomebaseOp {
                    mutations,
                    footprint,
                    guards,
                } = index.to_homebase()?;
                (mutations, footprint, guards)
            }
            Self::SetUserVersion(operation) => {
                let UserVersionHomebaseOp {
                    mutations,
                    footprint,
                    guards,
                } = operation.to_homebase()?;
                (mutations, footprint, guards)
            }
            Self::View(operation) => {
                let ViewHomebaseOp {
                    mutations,
                    footprint,
                    guards,
                } = operation.to_homebase()?;
                (mutations, footprint, guards)
            }
        };
        let operation = guards.operation().ok_or(crate::Error::CaptureInvariant(
            "operation lowering produced an unscoped guard plan",
        ))?;
        validate_compiled_output(operation, &mutations, &guards)?;
        if footprint != guards.footprint() {
            return Err(crate::Error::CaptureInvariant(
                "operation footprint contradicts its guard plan",
            ));
        }
        Ok(HomebaseOp {
            mutations,
            footprint,
            guards,
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
            Self::DropTable(dropped) => dropped.apply(connection),
            Self::ChangeRows(changes) => changes.apply(connection),
            Self::Index(index) => index.apply(connection),
            Self::SetUserVersion(operation) => operation.apply(connection),
            Self::View(operation) => operation.apply(connection),
        }
    }

    /// Capture any local-only inverse state required before speculative apply.
    pub(crate) fn capture_local_repair(&self, connection: &Connection) -> Result<()> {
        match self {
            Self::AlterTable(altered) => altered.capture_local_repair(connection),
            Self::DropTable(dropped) => dropped.capture_local_repair(connection),
            Self::CreateTable(_)
            | Self::ChangeRows(_)
            | Self::Index(_)
            | Self::SetUserVersion(_)
            | Self::View(_) => Ok(()),
        }
    }

    /// Sidecar identity required while this operation remains pending.
    #[cfg(test)]
    pub(crate) fn repair_id(&self) -> Option<crate::repair::RepairId> {
        match self {
            Self::AlterTable(altered) => altered.repair_id(),
            Self::DropTable(dropped) => Some(dropped.repair_spec().id),
            Self::CreateTable(_)
            | Self::ChangeRows(_)
            | Self::Index(_)
            | Self::SetUserVersion(_)
            | Self::View(_) => None,
        }
    }

    pub(crate) fn repair_spec(&self) -> Option<crate::repair::RepairSpec> {
        match self {
            Self::AlterTable(altered) => altered.repair_spec(),
            Self::DropTable(dropped) => Some(dropped.repair_spec()),
            Self::CreateTable(_)
            | Self::ChangeRows(_)
            | Self::Index(_)
            | Self::SetUserVersion(_)
            | Self::View(_) => None,
        }
    }

    /// Check the canonical row state against the branch-captured operation.
    #[cfg(debug_assertions)]
    pub(super) fn verify_materialized(&self, connection: &Connection) -> Result<()> {
        match self {
            Self::ChangeRows(changes) => changes.verify_materialized(connection),
            Self::AlterTable(altered) => {
                crate::physical::verify_table(connection, altered.table_id())
            }
            Self::CreateTable(created) => {
                crate::physical::verify_table(connection, created.table_id())
            }
            Self::DropTable(dropped) => dropped.verify_materialized(connection),
            Self::Index(index) => crate::physical::verify_table(connection, index.table_id()),
            Self::SetUserVersion(operation) => operation.verify_materialized(connection),
            Self::View(operation) => operation.verify_materialized(connection),
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
    use crate::catalog;
    use crate::logical::row::{
        CapturedRow, DeletedRowsFixture, RowSet, StoredValue, UpdatedRowsFixture,
    };
    use crate::logical::schema::{CreateColumn, SqlName, TypeDeclaration};

    fn table() -> CreateTableSpec {
        CreateTableSpec {
            name: SqlName::new("notes".into()),
            mode: Default::default(),
            storage: crate::logical::schema::TableStorage::Rowid,
            primary_key_conflict: Default::default(),
            columns: vec![CreateColumn {
                name: SqlName::new("id".into()),
                declared_type: TypeDeclaration::integer(),
                not_null: false,
                not_null_name: None,
                not_null_conflict: Default::default(),
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

        assert_eq!(mutations.len(), 9);
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
    fn compilation_binds_lowering_and_rejection_to_the_same_operation() {
        let operation =
            MultiliteOp::create_table("CREATE TABLE notes (id INTEGER PRIMARY KEY)", table());
        let expected = operation.to_homebase().unwrap();
        let compiled = operation.clone().compile().unwrap();

        assert_eq!(compiled.logical(), &operation);
        assert_eq!(compiled.homebase(), &expected);
        assert!(matches!(
            &compiled.rejection,
            RejectionEffect::RemoveCreatedTable { created } if created.table_name() == "notes"
        ));
    }

    #[test]
    fn operation_codec_roundtrips_insert_shaped_row_changes() {
        let connection = Connection::open_in_memory().unwrap();
        catalog::initialize(&connection).unwrap();
        let created = CreateTable::new("CREATE TABLE notes (id INTEGER PRIMARY KEY)", table());
        connection.execute(created.sql(), ()).unwrap();
        catalog::insert(&connection, &created).unwrap();
        let inserted = RowSet::from_captured(
            &connection,
            &[CapturedRow {
                table: "notes".into(),
                rowid: 7,
                values: vec![StoredValue::Integer(7)],
            }],
        )
        .unwrap()
        .unwrap();
        let operation = MultiliteOp::ChangeRows(RowChanges::inserted(inserted));

        assert_eq!(MultiliteOp::decode(&operation.encode()).unwrap(), operation);
        assert_eq!(
            MultiliteOp::decode(&[]),
            Err(OperationCodecError::Truncated)
        );
        assert_eq!(
            MultiliteOp::decode(&[2, CREATE_TABLE_OPERATION]),
            Err(OperationCodecError::UnknownVersion(2))
        );

        let deleted = MultiliteOp::ChangeRows(RowChanges::deleted(
            DeletedRowsFixture::from_captured(
                &connection,
                &[CapturedRow {
                    table: "notes".into(),
                    rowid: 7,
                    values: vec![StoredValue::Integer(7)],
                }],
            )
            .unwrap()
            .unwrap(),
        ));
        assert_eq!(MultiliteOp::decode(&deleted.encode()).unwrap(), deleted);
        let (mutations, footprint) = deleted.to_homebase().unwrap().into_parts();
        assert!(matches!(mutations.as_slice(), [Mutation::Delete { .. }]));
        assert_eq!(footprint.writes().len(), 1);

        let update_spec = CreateTableSpec {
            name: SqlName::new("updates".into()),
            mode: Default::default(),
            storage: crate::logical::schema::TableStorage::Rowid,
            primary_key_conflict: Default::default(),
            columns: vec![
                CreateColumn {
                    name: SqlName::new("id".into()),
                    declared_type: TypeDeclaration::integer(),
                    not_null: false,
                    not_null_name: None,
                    not_null_conflict: Default::default(),
                    default: None,
                    primary_key: Some(0),
                },
                CreateColumn {
                    name: SqlName::new("body".into()),
                    declared_type: TypeDeclaration::text(),
                    not_null: false,
                    not_null_name: None,
                    not_null_conflict: Default::default(),
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
        let updated = MultiliteOp::ChangeRows(RowChanges::updated(
            UpdatedRowsFixture::from_captured(
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
        ));
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
        let crate::sql::ValidatedExecute::CreateTable(parent_spec) =
            crate::sql::validate_execute(parent_sql).unwrap()
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
        let crate::sql::ValidatedExecute::CreateTable(child_spec) =
            crate::sql::validate_execute(child_sql).unwrap()
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
