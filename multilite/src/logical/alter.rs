//! Identity-resolved table and column schema deltas.

use std::fmt;

use homebase_core::range::Range;
use homebase_core::reader::Reader;
use homebase_core::tag::Mutation;
use homebase_core::writer::Writer;
use rusqlite::Connection;
use rusqlite::config::DbConfig;
use uuid::{Uuid, Variant, Version};

use super::guard::{GuardPlan, GuardReason, OperationFamily};
use super::row::primary_index_prefix;
use super::schema::{
    ColumnId, CreateTable, MutationId, SchemaRevisionId, SqlName, TableId,
    column_check_dependency_key, column_dependency_prefix, column_name_scope_key, schema_log_key,
    schema_object_name_scope_key, table_schema_key, write_revision_key,
};
use crate::catalog;
use crate::commit::footprint::ConflictFootprint;
use crate::repair;
use crate::sql::{AddColumnSpec, RenameColumnSpec, RenameTableSpec, ValidatedExecute};
use crate::sqlite::quote_identifier;
use crate::{Error, Result};

const ALTER_TABLE_VERSION: u8 = 5;
const TAG_MUTATION_ID: u8 = 1;
const TAG_SQL: u8 = 2;
const TAG_TABLE: u8 = 3;
const TAG_OLD_NAME: u8 = 4;
const TAG_NEW_NAME: u8 = 5;
const TAG_ACTION: u8 = 6;
const TAG_SOURCE_TABLE: u8 = 7;
const TAG_COLUMN: u8 = 8;
const TAG_BEFORE: u8 = 9;
const TAG_AFTER: u8 = 10;
const TAG_SCHEMA_REVISION: u8 = 12;
const TAG_PREDECESSOR: u8 = 13;
const RENAME_TABLE: u8 = 1;
const RENAME_COLUMN: u8 = 2;
const ADD_COLUMN: u8 = 3;
const DROP_COLUMN: u8 = 4;
#[derive(Clone, Debug, PartialEq, Eq)]
enum AlterTableDelta {
    RenameTable {
        old_name: SqlName,
        new_name: SqlName,
    },
    RenameColumn {
        column: ColumnId,
        old_name: SqlName,
        new_name: SqlName,
    },
    AddColumn {
        column: ColumnId,
        predecessor: ColumnId,
        name: SqlName,
        before: CreateTable,
        after: CreateTable,
    },
    DropColumn {
        column: ColumnId,
        name: SqlName,
        before: CreateTable,
        after: CreateTable,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RenameTarget {
    Table,
    Column,
}

/// One identity-resolved table-schema delta.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AlterTableOperation {
    mutation_id: MutationId,
    sql: String,
    table: TableId,
    schema_revision: SchemaRevisionId,
    source_table: SqlName,
    delta: AlterTableDelta,
}

/// Homebase mutations and conflict footprint for one table alteration.
pub struct AlterTableHomebaseOp {
    pub mutations: Vec<Mutation>,
    pub footprint: ConflictFootprint,
    pub guards: GuardPlan,
}

impl AlterTableOperation {
    #[cfg(debug_assertions)]
    pub(super) fn table_id(&self) -> TableId {
        self.table
    }

    /// Resolve the SQL source name once, then retain only its stable identity.
    pub fn prepare_rename_table(
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
            schema_revision: created.schema_revision_id(),
            source_table: spec.table.clone(),
            delta: AlterTableDelta::RenameTable {
                old_name: spec.table.clone(),
                new_name: spec.new_name.clone(),
            },
        };
        operation.validate().map_err(invalid_operation)?;
        Ok(operation)
    }

    /// Resolve the SQL column name once, then retain only its stable identity.
    pub fn prepare_rename_column(
        connection: &Connection,
        sql: &str,
        spec: &RenameColumnSpec,
    ) -> Result<Self> {
        let created = catalog::by_name(connection, spec.table.value())?.ok_or(
            Error::UnsupportedSql("ALTER TABLE target has no synchronized schema identity"),
        )?;
        let column = catalog::column_id_by_name(connection, created.table_id(), &spec.old_name)?
            .ok_or(Error::UnsupportedSql(
                "ALTER TABLE RENAME COLUMN references an unknown column",
            ))?;
        if catalog::column_id_by_name(connection, created.table_id(), &spec.new_name)?.is_some() {
            return Err(Error::UnsupportedSql(
                "ALTER TABLE target column name is already bound",
            ));
        }
        let operation = Self {
            mutation_id: MutationId::from_bytes(Uuid::new_v4().into_bytes()),
            sql: sql.to_owned(),
            table: created.table_id(),
            schema_revision: created.schema_revision_id(),
            source_table: spec.table.clone(),
            delta: AlterTableDelta::RenameColumn {
                column,
                old_name: spec.old_name.clone(),
                new_name: spec.new_name.clone(),
            },
        };
        operation.validate().map_err(invalid_operation)?;
        Ok(operation)
    }

    pub fn prepare_add_column(
        connection: &Connection,
        sql: &str,
        spec: &AddColumnSpec,
    ) -> Result<Self> {
        let before = catalog::by_name(connection, spec.table.value())?.ok_or(
            Error::UnsupportedSql("ALTER TABLE target has no synchronized schema identity"),
        )?;
        if catalog::column_id_by_name(connection, before.table_id(), &spec.column.name)?.is_some() {
            return Err(Error::UnsupportedSql(
                "ALTER TABLE target column name is already bound",
            ));
        }
        let (after, column) = before.with_added_column(&spec.column, &spec.checks)?;
        let predecessor = before
            .columns()
            .last()
            .expect("validated tables have a primary-key column")
            .id();
        let operation = Self {
            mutation_id: MutationId::from_bytes(Uuid::new_v4().into_bytes()),
            sql: sql.to_owned(),
            table: before.table_id(),
            schema_revision: before.schema_revision_id(),
            source_table: spec.table.clone(),
            delta: AlterTableDelta::AddColumn {
                column,
                predecessor,
                name: spec.column.name.clone(),
                before,
                after,
            },
        };
        operation.validate().map_err(invalid_operation)?;
        Ok(operation)
    }

    pub fn prepare_drop_column(
        connection: &Connection,
        sql: &str,
        spec: &crate::sql::DropColumnSpec,
    ) -> Result<Self> {
        let before = catalog::by_name(connection, spec.table.value())?.ok_or(
            Error::UnsupportedSql("ALTER TABLE target has no synchronized schema identity"),
        )?;
        let column = catalog::column_id_by_name(connection, before.table_id(), &spec.column)?
            .ok_or(Error::UnsupportedSql(
                "ALTER TABLE DROP COLUMN references an unknown column",
            ))?;
        if catalog::incoming_foreign_keys(connection, before.table_id())?
            .iter()
            .any(|(_, foreign_key)| foreign_key.referenced_columns().contains(&column))
        {
            return Err(Error::UnsupportedSql(
                "DROP COLUMN does not support referenced parent columns",
            ));
        }
        let after = before.with_removed_column(column)?;
        let operation = Self {
            mutation_id: MutationId::from_bytes(Uuid::new_v4().into_bytes()),
            sql: sql.to_owned(),
            table: before.table_id(),
            schema_revision: before.schema_revision_id(),
            source_table: spec.table.clone(),
            delta: AlterTableDelta::DropColumn {
                column,
                name: spec.column.clone(),
                before,
                after,
            },
        };
        operation.validate().map_err(invalid_operation)?;
        Ok(operation)
    }

    /// Move the name registry without evolving table-owned schema state.
    pub fn to_homebase(&self) -> Result<AlterTableHomebaseOp> {
        self.validate().map_err(invalid_operation)?;
        match &self.delta {
            AlterTableDelta::AddColumn {
                column,
                name,
                before,
                after,
                ..
            }
            | AlterTableDelta::DropColumn {
                column,
                name,
                before,
                after,
                ..
            } => {
                let name_key = column_name_scope_key(self.table, name);
                let changes_write_contract =
                    matches!(&self.delta, AlterTableDelta::AddColumn { .. })
                        && after.added_column_changes_write_contract(*column);
                let mut guards = GuardPlan::for_operation(match &self.delta {
                    AlterTableDelta::AddColumn { .. } => OperationFamily::AddColumn,
                    AlterTableDelta::DropColumn { .. } => OperationFamily::DropColumn,
                    _ => unreachable!(),
                });
                guards.invariant(name_key.clone(), GuardReason::ColumnNameBinding)?;
                let binding = match &self.delta {
                    AlterTableDelta::AddColumn { .. } => {
                        for dependency in after.added_column_dependencies(*column) {
                            guards.invariant(
                                column_name_scope_key(self.table, &dependency),
                                GuardReason::ColumnNameBinding,
                            )?;
                        }
                        Mutation::Set {
                            key: name_key,
                            value: column.as_bytes().to_vec(),
                        }
                    }
                    AlterTableDelta::DropColumn { .. } => Mutation::Delete { key: name_key },
                    _ => unreachable!(),
                };
                let mut mutations = vec![
                    Mutation::Set {
                        key: schema_log_key(self.mutation_id),
                        value: self.encode(),
                    },
                    binding,
                    Mutation::Set {
                        key: table_schema_key(self.table, after.schema_revision_id()),
                        value: after.encode(),
                    },
                ];
                match &self.delta {
                    AlterTableDelta::AddColumn { .. } => {
                        for dependency in after.column_check_dependencies(*column) {
                            let key = column_check_dependency_key(self.table, dependency, *column);
                            guards.invariant(key.clone(), GuardReason::ColumnDependency)?;
                            guards.write(key.clone(), GuardReason::ColumnDependency)?;
                            mutations.push(Mutation::Set {
                                key,
                                value: column.as_bytes().to_vec(),
                            });
                        }
                    }
                    AlterTableDelta::DropColumn { .. } => {
                        for dependency in before.column_check_dependencies(*column) {
                            let key = column_check_dependency_key(self.table, dependency, *column);
                            guards.invariant(key.clone(), GuardReason::ColumnDependency)?;
                            guards.write(key.clone(), GuardReason::ColumnDependency)?;
                            mutations.push(Mutation::Delete { key });
                        }
                        let dependents = column_dependency_prefix(self.table, *column);
                        guards.invariant(dependents.clone(), GuardReason::ColumnDependency)?;
                        guards.write(dependents.clone(), GuardReason::ColumnDependency)?;
                        mutations.push(Mutation::DeleteRange {
                            range: Range::Prefix(dependents),
                        });
                    }
                    _ => unreachable!(),
                }
                if changes_write_contract {
                    let write_revision = write_revision_key(self.table);
                    guards.write(write_revision.clone(), GuardReason::WriteContract)?;
                    guards.invariant(primary_index_prefix(before), GuardReason::ExistingRows)?;
                    mutations.push(Mutation::Set {
                        key: write_revision,
                        value: self.mutation_id.as_bytes().to_vec(),
                    });
                }
                let footprint = guards.footprint();
                Ok(AlterTableHomebaseOp {
                    mutations,
                    footprint,
                    guards,
                })
            }
            AlterTableDelta::RenameTable { old_name, new_name } => self.rename_homebase(
                schema_object_name_scope_key(old_name),
                schema_object_name_scope_key(new_name),
                self.table.as_bytes().to_vec(),
                OperationFamily::RenameTable,
                GuardReason::SchemaObjectName,
            ),
            AlterTableDelta::RenameColumn {
                column,
                old_name,
                new_name,
            } => self.rename_homebase(
                column_name_scope_key(self.table, old_name),
                column_name_scope_key(self.table, new_name),
                column.as_bytes().to_vec(),
                OperationFamily::RenameColumn,
                GuardReason::ColumnNameBinding,
            ),
        }
    }

    /// Apply an authenticated binding change to canonical SQLite.
    pub fn apply(&self, connection: &Connection) -> Result<()> {
        self.validate().map_err(invalid_operation)?;
        if matches!(self.delta, AlterTableDelta::DropColumn { .. }) {
            return self.record_catalog(connection);
        }
        self.validate_catalog_before(connection)?;
        let table = catalog::name_by_id(connection, self.table)?.ok_or(Error::InvalidDatabase(
            "ALTER TABLE identity is missing from the schema catalog",
        ))?;
        let sql = match &self.delta {
            AlterTableDelta::AddColumn { .. } => crate::sql::render_alter_table(&self.sql, &table)?,
            AlterTableDelta::DropColumn { .. } => {
                unreachable!("DROP COLUMN materializes through table rebuild")
            }
            AlterTableDelta::RenameTable { old_name, new_name } => {
                rename_sql(&table, RenameTarget::Table, old_name, new_name)
            }
            AlterTableDelta::RenameColumn {
                old_name, new_name, ..
            } => rename_sql(&table, RenameTarget::Column, old_name, new_name),
        };
        connection.execute_batch(&sql)?;
        self.record_catalog(connection)
    }

    pub fn materializes_internally(&self) -> bool {
        matches!(self.delta, AlterTableDelta::DropColumn { .. })
    }

    /// Local sidecar identity required while this destructive operation is pending.
    #[cfg(test)]
    pub(crate) fn repair_id(&self) -> Option<repair::RepairId> {
        matches!(self.delta, AlterTableDelta::DropColumn { .. })
            .then(|| self.mutation_id.as_bytes())
    }

    pub(crate) fn repair_spec(&self) -> Option<repair::RepairSpec> {
        let AlterTableDelta::DropColumn { before, .. } = &self.delta else {
            return None;
        };
        Some(repair::drop_column_spec(
            self.mutation_id.as_bytes(),
            before.primary_key_columns().count(),
        ))
    }

    /// Stream destroyed values into local durable storage before canonical apply.
    pub(crate) fn capture_local_repair(&self, connection: &Connection) -> Result<()> {
        let AlterTableDelta::DropColumn {
            column,
            name,
            before,
            ..
        } = &self.delta
        else {
            return Ok(());
        };
        self.validate_catalog_before(connection)?;
        let table = catalog::name_by_id(connection, self.table)?.ok_or(Error::InvalidDatabase(
            "DROP COLUMN table has no current name binding",
        ))?;
        let current_column = catalog::column_name_by_id(connection, self.table, *column)?.ok_or(
            Error::InvalidDatabase("DROP COLUMN column has no current name binding"),
        )?;
        if current_column.canonical() != name.canonical() {
            return Err(Error::InvalidDatabase(
                "DROP COLUMN repair no longer matches the current column binding",
            ));
        }
        let primary = primary_key_names(connection, before)?;
        repair::capture_drop_column(
            connection,
            self.mutation_id.as_bytes(),
            table.value(),
            &primary,
            current_column.value(),
        )
    }

    /// Record the binding change after a branch has executed the user's SQL.
    pub fn record_catalog(&self, connection: &Connection) -> Result<()> {
        with_savepoint(connection, "__multilite__record_alter", || {
            self.record_catalog_inner(connection)
        })
    }

    fn record_catalog_inner(&self, connection: &Connection) -> Result<()> {
        self.validate_catalog_before(connection)?;
        match &self.delta {
            AlterTableDelta::RenameTable { old_name, new_name } => {
                catalog::rename_binding(connection, self.table, old_name, new_name)
            }
            AlterTableDelta::RenameColumn {
                column,
                old_name,
                new_name,
            } => {
                catalog::rename_column_binding(
                    connection, self.table, *column, old_name, new_name,
                )?;
                let current = catalog::by_id(connection, self.table)?.ok_or(
                    Error::InvalidDatabase("renamed table is missing from the schema catalog"),
                )?;
                let folded =
                    current.fold_renamed_column_expressions(*column, old_name, new_name)?;
                catalog::replace(connection, &folded)
            }
            AlterTableDelta::AddColumn {
                column,
                predecessor,
                name,
                after,
                ..
            } => {
                let current = catalog::by_id(connection, self.table)?.ok_or(
                    Error::InvalidDatabase("ADD COLUMN table is missing from the schema catalog"),
                )?;
                catalog::insert_column_binding(
                    connection,
                    self.table,
                    *column,
                    *predecessor,
                    name,
                )?;
                let order = catalog::column_order(connection, self.table)?;
                let folded = current.fold_added_column(after, *column, &order)?;
                catalog::replace(connection, &folded)?;
                rebuild_table_if_needed(connection, &folded, false)
            }
            AlterTableDelta::DropColumn { column, .. } => {
                let current = catalog::by_id(connection, self.table)?.ok_or(
                    Error::InvalidDatabase("DROP COLUMN table is missing from the schema catalog"),
                )?;
                catalog::retire_column_binding(connection, self.table, *column)?;
                let order = catalog::column_order(connection, self.table)?;
                let folded = current.fold_removed_column(*column, &order)?;
                catalog::replace(connection, &folded)?;
                rebuild_table_if_needed(connection, &folded, false)
            }
        }
    }

    /// Reverse one speculative binding change after authority rejects it.
    pub fn rollback(&self, connection: &Connection) -> Result<()> {
        with_savepoint(connection, "__multilite__rollback_alter", || {
            self.rollback_inner(connection)
        })
    }

    fn rollback_inner(&self, connection: &Connection) -> Result<()> {
        self.validate().map_err(invalid_operation)?;
        self.validate_catalog_after(connection)?;
        let table = catalog::name_by_id(connection, self.table)?.ok_or(Error::InvalidDatabase(
            "pending ALTER TABLE identity is missing from the catalog",
        ))?;
        match &self.delta {
            AlterTableDelta::RenameTable { old_name, new_name } => {
                connection.execute_batch(&rename_sql(
                    &table,
                    RenameTarget::Table,
                    new_name,
                    old_name,
                ))?;
                catalog::rename_binding(connection, self.table, new_name, old_name)
            }
            AlterTableDelta::RenameColumn {
                column,
                old_name,
                new_name,
            } => {
                connection.execute_batch(&rename_sql(
                    &table,
                    RenameTarget::Column,
                    new_name,
                    old_name,
                ))?;
                catalog::rename_column_binding(
                    connection, self.table, *column, new_name, old_name,
                )?;
                let current = catalog::by_id(connection, self.table)?.ok_or(
                    Error::InvalidDatabase("renamed table is missing from the schema catalog"),
                )?;
                let folded =
                    current.fold_renamed_column_expressions(*column, new_name, old_name)?;
                catalog::replace(connection, &folded)
            }
            AlterTableDelta::AddColumn { column, name, .. } => {
                connection.execute_batch(&format!(
                    "ALTER TABLE {} DROP COLUMN {}",
                    quote_identifier(table.value()),
                    quote_identifier(name.value())
                ))?;
                let current =
                    catalog::by_id(connection, self.table)?.ok_or(Error::InvalidDatabase(
                        "pending ADD COLUMN table is missing from the schema catalog",
                    ))?;
                catalog::retire_column_binding(connection, self.table, *column)?;
                let order = catalog::column_order(connection, self.table)?;
                let folded = current.fold_removed_column(*column, &order)?;
                catalog::replace(connection, &folded)?;
                rebuild_table_if_needed(connection, &folded, false)
            }
            AlterTableDelta::DropColumn {
                column,
                name,
                before,
                ..
            } => {
                connection.execute_batch(&format!(
                    "ALTER TABLE {} ADD COLUMN {} BLOB",
                    quote_identifier(table.value()),
                    quote_identifier(name.value())
                ))?;
                let current =
                    catalog::by_id(connection, self.table)?.ok_or(Error::InvalidDatabase(
                        "pending DROP COLUMN table is missing from the schema catalog",
                    ))?;
                catalog::restore_column_binding(connection, self.table, *column, name)?;
                let order = catalog::column_order(connection, self.table)?;
                let folded = current.fold_added_column(before, *column, &order)?;
                catalog::replace(connection, &folded)?;
                let primary = primary_key_names(connection, &folded)?;
                repair::restore_drop_column(
                    connection,
                    self.mutation_id.as_bytes(),
                    table.value(),
                    &primary,
                    name.value(),
                )?;
                rebuild_table_if_needed(connection, &folded, true)?;
                repair::retire(connection, self.mutation_id.as_bytes())
            }
        }
    }

    fn rename_homebase(
        &self,
        old_name: homebase_core::key::Key,
        new_name: homebase_core::key::Key,
        value: Vec<u8>,
        operation: OperationFamily,
        reason: GuardReason,
    ) -> Result<AlterTableHomebaseOp> {
        let mut guards = GuardPlan::for_operation(operation);
        guards.invariant(old_name.clone(), reason)?;
        guards.invariant(new_name.clone(), reason)?;
        let mutations = vec![
            Mutation::Set {
                key: schema_log_key(self.mutation_id),
                value: self.encode(),
            },
            Mutation::Delete { key: old_name },
            Mutation::Set {
                key: new_name,
                value,
            },
        ];
        let footprint = guards.footprint();
        Ok(AlterTableHomebaseOp {
            mutations,
            footprint,
            guards,
        })
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
            .field(TAG_SCHEMA_REVISION, &self.schema_revision.as_bytes())
            .expect("ALTER TABLE field fits in u32");
        writer
            .field(TAG_SOURCE_TABLE, self.source_table.value().as_bytes())
            .expect("ALTER TABLE field fits in u32");
        match &self.delta {
            AlterTableDelta::RenameTable { old_name, new_name } => {
                put_action(&mut writer, RENAME_TABLE);
                put_name(&mut writer, TAG_OLD_NAME, old_name);
                put_name(&mut writer, TAG_NEW_NAME, new_name);
            }
            AlterTableDelta::RenameColumn {
                column,
                old_name,
                new_name,
            } => {
                put_action(&mut writer, RENAME_COLUMN);
                put_column(&mut writer, *column);
                put_name(&mut writer, TAG_OLD_NAME, old_name);
                put_name(&mut writer, TAG_NEW_NAME, new_name);
            }
            AlterTableDelta::AddColumn {
                column,
                predecessor,
                name,
                before,
                after,
            } => {
                put_action(&mut writer, ADD_COLUMN);
                put_column(&mut writer, *column);
                writer
                    .field(TAG_PREDECESSOR, &predecessor.as_bytes())
                    .expect("ALTER TABLE field fits in u32");
                put_name(&mut writer, TAG_NEW_NAME, name);
                writer
                    .field(TAG_BEFORE, &before.encode())
                    .expect("ALTER TABLE field fits in u32");
                writer
                    .field(TAG_AFTER, &after.encode())
                    .expect("ALTER TABLE field fits in u32");
            }
            AlterTableDelta::DropColumn {
                column,
                name,
                before,
                after,
            } => {
                put_action(&mut writer, DROP_COLUMN);
                put_column(&mut writer, *column);
                put_name(&mut writer, TAG_OLD_NAME, name);
                writer
                    .field(TAG_BEFORE, &before.encode())
                    .expect("ALTER TABLE field fits in u32");
                writer
                    .field(TAG_AFTER, &after.encode())
                    .expect("ALTER TABLE field fits in u32");
            }
        }
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
        let mut schema_revision = None;
        let mut source_table = None;
        let mut action = None;
        let mut column = None;
        let mut predecessor = None;
        let mut before = None;
        let mut after = None;
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
                TAG_SCHEMA_REVISION => set_once(
                    &mut schema_revision,
                    SchemaRevisionId::from_bytes(uuid_bytes(value)?),
                )?,
                TAG_SOURCE_TABLE => {
                    set_once(&mut source_table, SqlName::new(decode_string(value)?))?
                }
                TAG_ACTION => {
                    let [value] = value else {
                        return Err(AlterTableCodecError::InvalidLength);
                    };
                    set_once(&mut action, *value)?;
                }
                TAG_COLUMN => set_once(&mut column, ColumnId::from_bytes(uuid_bytes(value)?))?,
                TAG_PREDECESSOR => {
                    set_once(&mut predecessor, ColumnId::from_bytes(uuid_bytes(value)?))?
                }
                TAG_BEFORE => set_once(
                    &mut before,
                    CreateTable::decode(value).map_err(|_| AlterTableCodecError::InvalidSchema)?,
                )?,
                TAG_AFTER => set_once(
                    &mut after,
                    CreateTable::decode(value).map_err(|_| AlterTableCodecError::InvalidSchema)?,
                )?,
                TAG_OLD_NAME => set_once(&mut old_name, SqlName::new(decode_string(value)?))?,
                TAG_NEW_NAME => set_once(&mut new_name, SqlName::new(decode_string(value)?))?,
                _ => {}
            }
        }
        let action = action.ok_or(AlterTableCodecError::MissingField(TAG_ACTION))?;
        let delta = match action {
            RENAME_TABLE => AlterTableDelta::RenameTable {
                old_name: take_required(&mut old_name, TAG_OLD_NAME)?,
                new_name: take_required(&mut new_name, TAG_NEW_NAME)?,
            },
            RENAME_COLUMN => AlterTableDelta::RenameColumn {
                column: take_required(&mut column, TAG_COLUMN)?,
                old_name: take_required(&mut old_name, TAG_OLD_NAME)?,
                new_name: take_required(&mut new_name, TAG_NEW_NAME)?,
            },
            ADD_COLUMN => AlterTableDelta::AddColumn {
                column: take_required(&mut column, TAG_COLUMN)?,
                predecessor: take_required(&mut predecessor, TAG_PREDECESSOR)?,
                name: take_required(&mut new_name, TAG_NEW_NAME)?,
                before: take_required(&mut before, TAG_BEFORE)?,
                after: take_required(&mut after, TAG_AFTER)?,
            },
            DROP_COLUMN => AlterTableDelta::DropColumn {
                column: take_required(&mut column, TAG_COLUMN)?,
                name: take_required(&mut old_name, TAG_OLD_NAME)?,
                before: take_required(&mut before, TAG_BEFORE)?,
                after: take_required(&mut after, TAG_AFTER)?,
            },
            _ => return Err(AlterTableCodecError::InvalidAction),
        };
        if column.is_some()
            || predecessor.is_some()
            || before.is_some()
            || after.is_some()
            || old_name.is_some()
            || new_name.is_some()
        {
            return Err(AlterTableCodecError::InvalidAction);
        }
        let operation = Self {
            mutation_id: mutation_id.ok_or(AlterTableCodecError::MissingField(TAG_MUTATION_ID))?,
            sql: sql.ok_or(AlterTableCodecError::MissingField(TAG_SQL))?,
            table: table.ok_or(AlterTableCodecError::MissingField(TAG_TABLE))?,
            schema_revision: schema_revision
                .ok_or(AlterTableCodecError::MissingField(TAG_SCHEMA_REVISION))?,
            source_table: source_table
                .ok_or(AlterTableCodecError::MissingField(TAG_SOURCE_TABLE))?,
            delta,
        };
        operation.validate()?;
        Ok(operation)
    }

    fn validate(&self) -> std::result::Result<(), AlterTableCodecError> {
        let validated = crate::sql::validate_execute(&self.sql)
            .map_err(|_| AlterTableCodecError::InvalidSql)?;
        match (&self.delta, validated) {
            (
                AlterTableDelta::RenameTable { old_name, new_name },
                ValidatedExecute::RenameTable(spec),
            ) if spec.table == self.source_table
                && spec.table == *old_name
                && spec.new_name == *new_name => {}
            (
                AlterTableDelta::RenameColumn {
                    old_name, new_name, ..
                },
                ValidatedExecute::RenameColumn(spec),
            ) if spec.table == self.source_table
                && spec.old_name == *old_name
                && spec.new_name == *new_name => {}
            (
                AlterTableDelta::AddColumn {
                    column,
                    predecessor,
                    name,
                    before,
                    after,
                },
                ValidatedExecute::AddColumn(spec),
            ) if spec.table == self.source_table
                && spec.column.name == *name
                && before.table_id() == self.table
                && before.schema_revision_id() == self.schema_revision
                && before.schema_revision_id() != after.schema_revision_id()
                && before
                    .columns()
                    .last()
                    .is_some_and(|column| column.id() == *predecessor)
                && before
                    .with_added_column_identity(*column, &spec.column, &spec.checks)
                    .is_ok_and(|expected| expected == *after) => {}
            (
                AlterTableDelta::DropColumn {
                    column,
                    name,
                    before,
                    after,
                },
                ValidatedExecute::DropColumn(spec),
            ) if spec.table == self.source_table
                && spec.column == *name
                && before.table_id() == self.table
                && before.schema_revision_id() == self.schema_revision
                && before.schema_revision_id() != after.schema_revision_id()
                && before
                    .with_removed_column(*column)
                    .is_ok_and(|expected| expected == *after) => {}
            _ => return Err(AlterTableCodecError::InvalidRename),
        }
        Ok(())
    }

    fn validate_catalog_before(&self, connection: &Connection) -> Result<()> {
        match &self.delta {
            AlterTableDelta::RenameTable { old_name, new_name } => {
                let current =
                    catalog::name_by_id(connection, self.table)?.ok_or(Error::InvalidDatabase(
                        "table rename identity is missing from the schema catalog",
                    ))?;
                if current.canonical() != old_name.canonical()
                    || catalog::by_name(connection, new_name.value())?.is_some()
                {
                    return Err(Error::InvalidDatabase(
                        "table rename no longer matches the schema catalog",
                    ));
                }
            }
            AlterTableDelta::RenameColumn {
                column,
                old_name,
                new_name,
            } => {
                let current = catalog::column_name_by_id(connection, self.table, *column)?.ok_or(
                    Error::InvalidDatabase(
                        "column rename identity is missing from the schema catalog",
                    ),
                )?;
                if current.canonical() != old_name.canonical()
                    || catalog::column_id_by_name(connection, self.table, new_name)?.is_some()
                {
                    return Err(Error::InvalidDatabase(
                        "column rename no longer matches the schema catalog",
                    ));
                }
            }
            AlterTableDelta::AddColumn { column, name, .. } => {
                if catalog::by_id(connection, self.table)?.is_none()
                    || catalog::column_id_by_name(connection, self.table, name)?.is_some()
                    || catalog::column_name_by_id(connection, self.table, *column)?.is_some()
                {
                    return Err(Error::InvalidDatabase(
                        "ADD COLUMN no longer matches the schema catalog",
                    ));
                }
            }
            AlterTableDelta::DropColumn { column, name, .. } => {
                if catalog::by_id(connection, self.table)?.is_none()
                    || catalog::column_name_by_id(connection, self.table, *column)?
                        .is_none_or(|current| current.canonical() != name.canonical())
                {
                    return Err(Error::InvalidDatabase(
                        "DROP COLUMN no longer matches the schema catalog",
                    ));
                }
            }
        }
        Ok(())
    }

    fn validate_catalog_after(&self, connection: &Connection) -> Result<()> {
        match &self.delta {
            AlterTableDelta::RenameTable { old_name, new_name } => {
                let current =
                    catalog::name_by_id(connection, self.table)?.ok_or(Error::InvalidDatabase(
                        "pending table rename identity is missing from the catalog",
                    ))?;
                if current.canonical() != new_name.canonical()
                    || catalog::by_name(connection, old_name.value())?.is_some()
                {
                    return Err(Error::InvalidDatabase(
                        "pending table rename no longer matches SQLite state",
                    ));
                }
            }
            AlterTableDelta::RenameColumn {
                column,
                old_name,
                new_name,
            } => {
                let current = catalog::column_name_by_id(connection, self.table, *column)?.ok_or(
                    Error::InvalidDatabase(
                        "pending column rename identity is missing from the catalog",
                    ),
                )?;
                if current.canonical() != new_name.canonical()
                    || catalog::column_id_by_name(connection, self.table, old_name)?.is_some()
                {
                    return Err(Error::InvalidDatabase(
                        "pending column rename no longer matches SQLite state",
                    ));
                }
            }
            AlterTableDelta::AddColumn { column, name, .. } => {
                let current =
                    catalog::by_id(connection, self.table)?.ok_or(Error::InvalidDatabase(
                        "pending ADD COLUMN table is missing from the schema catalog",
                    ))?;
                if !current
                    .columns()
                    .iter()
                    .any(|candidate| candidate.id() == *column)
                    || catalog::column_name_by_id(connection, self.table, *column)?
                        .is_none_or(|current| current.canonical() != name.canonical())
                {
                    return Err(Error::InvalidDatabase(
                        "pending ADD COLUMN no longer matches SQLite state",
                    ));
                }
            }
            AlterTableDelta::DropColumn { column, .. } => {
                let current =
                    catalog::by_id(connection, self.table)?.ok_or(Error::InvalidDatabase(
                        "pending DROP COLUMN table is missing from the schema catalog",
                    ))?;
                if current
                    .columns()
                    .iter()
                    .any(|candidate| candidate.id() == *column)
                    || catalog::column_name_by_id(connection, self.table, *column)?.is_some()
                {
                    return Err(Error::InvalidDatabase(
                        "pending DROP COLUMN no longer matches SQLite state",
                    ));
                }
            }
        }
        Ok(())
    }
}

fn with_savepoint<T>(
    connection: &Connection,
    prefix: &str,
    operation: impl FnOnce() -> Result<T>,
) -> Result<T> {
    let name = format!("{prefix}_{}", Uuid::new_v4().simple());
    crate::connection::with_savepoint(connection, name, operation)
}

fn rebuild_table_if_needed(
    connection: &Connection,
    table: &CreateTable,
    force: bool,
) -> Result<()> {
    let table_name = catalog::name_by_id(connection, table.table_id())?.ok_or(
        Error::InvalidDatabase("rebuilt table has no current name binding"),
    )?;
    let desired = catalog::column_names(connection, table)?;
    let materialized = materialized_column_names(connection, &table_name)?;
    if !force
        && materialized.len() == desired.len()
        && materialized
            .iter()
            .zip(&desired)
            .all(|(actual, expected)| actual.eq_ignore_ascii_case(expected.value()))
    {
        return Ok(());
    }

    let source_sql = table.materialization_sql(connection)?;
    let dependent_sql = materialized_dependents(connection, &table_name)?;
    let temporary = SqlName::new(format!("__multilite__rebuild_{}", Uuid::new_v4().simple()));
    let create_sql = crate::sql::render_create_table_name(&source_sql, &temporary)?;

    let copied = desired
        .iter()
        .map(|name| quote_identifier(name.value()))
        .collect::<Vec<_>>()
        .join(", ");

    // Dropping the old parent name makes SQLite enforce incoming immediate
    // foreign keys before the replacement can claim that name. The connection
    // API can suspend enforcement within the surrounding savepoint; a complete
    // integrity check runs before enforcement and the savepoint are restored.
    let foreign_keys = ForeignKeyGuard::suspend(connection)?;
    let rebuild = (|| {
        connection.execute_batch(&create_sql)?;
        connection.execute_batch(&format!(
            "INSERT INTO {} ({copied}) SELECT {copied} FROM {}",
            quote_identifier(temporary.value()),
            quote_identifier(table_name.value())
        ))?;
        connection.execute_batch(&format!(
            "DROP TABLE {};
             ALTER TABLE {} RENAME TO {}",
            quote_identifier(table_name.value()),
            quote_identifier(temporary.value()),
            quote_identifier(table_name.value())
        ))?;
        for sql in dependent_sql {
            connection.execute_batch(&sql)?;
        }
        if connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM pragma_foreign_key_check)",
            (),
            |row| row.get::<_, bool>(0),
        )? {
            return Err(Error::InvalidDatabase(
                "table rebuild violates a foreign-key relationship",
            ));
        }
        Ok(())
    })();
    let restore = foreign_keys.restore();
    rebuild?;
    restore
}

struct ForeignKeyGuard<'connection> {
    connection: &'connection Connection,
    enabled: bool,
    active: bool,
}

impl<'connection> ForeignKeyGuard<'connection> {
    fn suspend(connection: &'connection Connection) -> Result<Self> {
        let enabled = connection.db_config(DbConfig::SQLITE_DBCONFIG_ENABLE_FKEY)?;
        connection.set_db_config(DbConfig::SQLITE_DBCONFIG_ENABLE_FKEY, false)?;
        Ok(Self {
            connection,
            enabled,
            active: true,
        })
    }

    fn restore(mut self) -> Result<()> {
        self.connection
            .set_db_config(DbConfig::SQLITE_DBCONFIG_ENABLE_FKEY, self.enabled)?;
        self.active = false;
        Ok(())
    }
}

impl Drop for ForeignKeyGuard<'_> {
    fn drop(&mut self) {
        if self.active {
            let _ = self
                .connection
                .set_db_config(DbConfig::SQLITE_DBCONFIG_ENABLE_FKEY, self.enabled);
        }
    }
}

fn materialized_column_names(connection: &Connection, table: &SqlName) -> Result<Vec<String>> {
    let mut statement = connection.prepare(&format!(
        "PRAGMA main.table_xinfo({})",
        quote_identifier(table.value())
    ))?;
    let columns = statement
        .query_map((), |row| {
            Ok((row.get::<_, String>(1)?, row.get::<_, i64>(6)?))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    if columns.iter().any(|(_, hidden)| *hidden != 0) {
        return Err(Error::InvalidDatabase(
            "materialized table contains unsupported hidden columns",
        ));
    }
    Ok(columns.into_iter().map(|(name, _)| name).collect())
}

fn materialized_dependents(connection: &Connection, table: &SqlName) -> Result<Vec<String>> {
    let mut statement = connection.prepare(
        "SELECT sql FROM main.sqlite_schema
         WHERE tbl_name = ?1 COLLATE NOCASE
           AND type IN ('index', 'trigger')
           AND sql IS NOT NULL
         ORDER BY type, name",
    )?;
    statement
        .query_map([table.value()], |row| row.get(0))?
        .collect::<rusqlite::Result<Vec<_>>>()
        .map_err(Into::into)
}

fn primary_key_names(connection: &Connection, table: &CreateTable) -> Result<Vec<String>> {
    table
        .primary_key_columns()
        .map(|column| {
            catalog::column_name_by_id(connection, table.table_id(), column.id())?.ok_or(
                Error::InvalidDatabase("DROP COLUMN primary key has no current name binding"),
            )
        })
        .map(|result| result.map(|name| name.value().to_owned()))
        .collect()
}

fn rename_sql(table: &SqlName, target: RenameTarget, from: &SqlName, to: &SqlName) -> String {
    match target {
        RenameTarget::Table => format!(
            "ALTER TABLE {} RENAME TO {}",
            quote_identifier(from.value()),
            quote_identifier(to.value())
        ),
        RenameTarget::Column => format!(
            "ALTER TABLE {} RENAME COLUMN {} TO {}",
            quote_identifier(table.value()),
            quote_identifier(from.value()),
            quote_identifier(to.value())
        ),
    }
}

fn decode_string(value: &[u8]) -> std::result::Result<String, AlterTableCodecError> {
    String::from_utf8(value.to_vec()).map_err(|_| AlterTableCodecError::InvalidUtf8)
}

fn put_action(writer: &mut Writer, action: u8) {
    writer
        .field(TAG_ACTION, &[action])
        .expect("ALTER TABLE field fits in u32");
}

fn put_column(writer: &mut Writer, column: ColumnId) {
    writer
        .field(TAG_COLUMN, &column.as_bytes())
        .expect("ALTER TABLE field fits in u32");
}

fn put_name(writer: &mut Writer, tag: u8, name: &SqlName) {
    writer
        .field(tag, name.value().as_bytes())
        .expect("ALTER TABLE field fits in u32");
}

fn take_required<T>(slot: &mut Option<T>, tag: u8) -> std::result::Result<T, AlterTableCodecError> {
    slot.take().ok_or(AlterTableCodecError::MissingField(tag))
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
    InvalidAction,
    InvalidSchema,
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
            Self::InvalidAction => formatter.write_str("invalid ALTER TABLE action"),
            Self::InvalidSchema => formatter.write_str("invalid ALTER TABLE schema evolution"),
        }
    }
}

#[cfg(test)]
mod tests {
    use homebase_core::tag::Mutation;

    use super::*;
    use crate::commit::footprint::assert_explicit_range_assertions;
    use crate::logical::schema::{
        CreateColumn, CreateTable, CreateTableSpec, TableMode, TableStorage, TypeDeclaration,
    };

    fn connection() -> (Connection, CreateTable) {
        let connection = Connection::open_in_memory().unwrap();
        repair::register(&connection).unwrap();
        repair::initialize(&connection).unwrap();
        catalog::initialize(&connection).unwrap();
        let created = CreateTable::new(
            "CREATE TABLE notes (id INTEGER PRIMARY KEY)",
            CreateTableSpec {
                name: SqlName::new("notes".into()),
                mode: TableMode::Ordinary,
                storage: TableStorage::Rowid,
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

    fn connection_with(sql: &str) -> (Connection, CreateTable) {
        let connection = Connection::open_in_memory().unwrap();
        repair::register(&connection).unwrap();
        repair::initialize(&connection).unwrap();
        catalog::initialize(&connection).unwrap();
        let ValidatedExecute::CreateTable(spec) = crate::sql::validate_execute(sql).unwrap() else {
            unreachable!()
        };
        let created = CreateTable::new(sql, spec);
        connection.execute(sql, ()).unwrap();
        catalog::insert(&connection, &created).unwrap();
        (connection, created)
    }

    fn prepare_drop(connection: &Connection, sql: &str) -> AlterTableOperation {
        let ValidatedExecute::DropColumn(spec) = crate::sql::validate_execute(sql).unwrap() else {
            unreachable!()
        };
        AlterTableOperation::prepare_drop_column(connection, sql, &spec).unwrap()
    }

    fn operation(connection: &Connection) -> AlterTableOperation {
        let sql = "ALTER TABLE notes RENAME TO \"Archived Notes\"";
        let ValidatedExecute::RenameTable(spec) = crate::sql::validate_execute(sql).unwrap() else {
            unreachable!()
        };
        AlterTableOperation::prepare_rename_table(connection, sql, &spec).unwrap()
    }

    fn column_operation(connection: &Connection) -> AlterTableOperation {
        let sql = "ALTER TABLE notes RENAME COLUMN id TO note_id";
        let ValidatedExecute::RenameColumn(spec) = crate::sql::validate_execute(sql).unwrap()
        else {
            unreachable!()
        };
        AlterTableOperation::prepare_rename_column(connection, sql, &spec).unwrap()
    }

    fn add_operation(connection: &Connection) -> AlterTableOperation {
        let sql = "ALTER TABLE notes ADD COLUMN body TEXT DEFAULT 'empty'
                   CHECK (id > 0 AND length(body) > 0)";
        let ValidatedExecute::AddColumn(spec) = crate::sql::validate_execute(sql).unwrap() else {
            unreachable!()
        };
        AlterTableOperation::prepare_add_column(connection, sql, &spec).unwrap()
    }

    fn simple_add_operation(connection: &Connection) -> AlterTableOperation {
        let sql = "ALTER TABLE notes ADD COLUMN body TEXT DEFAULT 'empty'";
        let ValidatedExecute::AddColumn(spec) = crate::sql::validate_execute(sql).unwrap() else {
            unreachable!()
        };
        AlterTableOperation::prepare_add_column(connection, sql, &spec).unwrap()
    }

    fn drop_operation(connection: &Connection) -> AlterTableOperation {
        let sql = "ALTER TABLE notes DROP COLUMN body";
        let ValidatedExecute::DropColumn(spec) = crate::sql::validate_execute(sql).unwrap() else {
            unreachable!()
        };
        AlterTableOperation::prepare_drop_column(connection, sql, &spec).unwrap()
    }

    fn apply_speculative_drop(connection: &Connection, operation: &AlterTableOperation) {
        operation.capture_local_repair(connection).unwrap();
        operation.apply(connection).unwrap();
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
        let (connection, created) = connection();
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
        let AlterTableDelta::RenameTable { new_name, .. } = &mut mismatched.delta else {
            unreachable!()
        };
        *new_name = SqlName::new("different".into());
        assert_eq!(
            AlterTableOperation::decode(&mismatched.encode()),
            Err(AlterTableCodecError::InvalidRename)
        );

        let mut crossed = encoded;
        let mut extra = Writer::new();
        extra
            .field(TAG_BEFORE, &created.encode())
            .expect("test schema fits in a field");
        crossed.extend_from_slice(&extra.finish());
        assert_eq!(
            AlterTableOperation::decode(&crossed),
            Err(AlterTableCodecError::InvalidAction)
        );
    }

    #[test]
    fn column_rename_moves_only_column_name_cells_and_rolls_back() {
        let (connection, created) = connection();
        let operation = column_operation(&connection);
        assert_eq!(
            AlterTableOperation::decode(&operation.encode()).unwrap(),
            operation
        );
        let lowered = operation.to_homebase().unwrap();
        assert_eq!(lowered.mutations.len(), 3);
        assert_eq!(lowered.footprint.constraints().len(), 2);
        assert!(lowered.footprint.writes().is_empty());
        assert!(lowered.mutations.iter().all(|mutation| {
            mutation.key() != &table_schema_key(created.table_id(), created.schema_revision_id())
        }));
        let Mutation::Set { value, .. } = &lowered.mutations[2] else {
            panic!("new column name registry entry was not set")
        };
        assert_eq!(value, &created.columns()[0].id().as_bytes());

        operation.apply(&connection).unwrap();
        assert_ne!(
            catalog::by_id(&connection, created.table_id())
                .unwrap()
                .unwrap()
                .schema_revision_id(),
            created.schema_revision_id()
        );
        assert_eq!(
            catalog::column_name_by_id(&connection, created.table_id(), created.columns()[0].id())
                .unwrap(),
            Some(SqlName::new("note_id".into()))
        );
        connection
            .execute("INSERT INTO notes (note_id) VALUES (1)", ())
            .unwrap();

        operation.rollback(&connection).unwrap();
        assert_eq!(
            catalog::column_name_by_id(&connection, created.table_id(), created.columns()[0].id())
                .unwrap(),
            Some(SqlName::new("id".into()))
        );
        assert_eq!(
            connection
                .query_row("SELECT id FROM notes", (), |row| row.get::<_, i64>(0))
                .unwrap(),
            1
        );
        catalog::validate(&connection).unwrap();
    }

    #[test]
    fn add_column_evolves_schema_and_write_contract_and_rolls_back() {
        let (compatible, compatible_created) = connection();
        let (connection, created) = connection();
        connection
            .execute("INSERT INTO notes VALUES (1)", ())
            .unwrap();
        let operation = add_operation(&connection);
        let expected_after = match &operation.delta {
            AlterTableDelta::AddColumn { after, .. } => after.clone(),
            _ => unreachable!(),
        };
        assert_eq!(
            AlterTableOperation::decode(&operation.encode()).unwrap(),
            operation
        );
        let lowered = operation.to_homebase().unwrap();
        let AlterTableDelta::AddColumn { column, after, .. } = &operation.delta else {
            unreachable!()
        };
        assert_explicit_range_assertions(
            &lowered.footprint,
            &[
                column_check_dependency_key(after.table_id(), created.columns()[0].id(), *column),
                column_check_dependency_key(after.table_id(), *column, *column),
                primary_index_prefix(&created),
            ],
        );
        assert_eq!(lowered.mutations.len(), 6);
        assert_eq!(lowered.footprint.constraints().len(), 5);
        assert_eq!(lowered.footprint.writes().len(), 3);

        let compatible = simple_add_operation(&compatible).to_homebase().unwrap();
        assert_eq!(compatible.mutations.len(), 3);
        assert!(compatible.footprint.writes().is_empty());
        assert!(
            compatible.mutations.iter().all(
                |mutation| mutation.key() != &write_revision_key(compatible_created.table_id())
            )
        );

        operation.apply(&connection).unwrap();
        let evolved = catalog::by_id(&connection, created.table_id())
            .unwrap()
            .unwrap();
        assert_eq!(evolved, expected_after);
        assert_eq!(evolved.columns().len(), 2);
        assert_ne!(evolved.schema_revision_id(), created.schema_revision_id());
        assert_eq!(
            connection
                .query_row("SELECT body FROM notes WHERE id = 1", (), |row| {
                    row.get::<_, String>(0)
                })
                .unwrap(),
            "empty"
        );

        operation.rollback(&connection).unwrap();
        assert_eq!(
            catalog::by_id(&connection, created.table_id()).unwrap(),
            Some(created)
        );
        assert_eq!(
            connection
                .query_row(
                    "SELECT count(*) FROM pragma_table_info('notes')",
                    (),
                    |row| row.get::<_, i64>(0)
                )
                .unwrap(),
            1
        );
        catalog::validate(&connection).unwrap();
    }

    #[test]
    fn drop_column_wire_is_metadata_only_and_sidecar_repairs_rejection() {
        let (connection, _) = connection();
        connection
            .execute("INSERT INTO notes VALUES (1)", ())
            .unwrap();
        let add = simple_add_operation(&connection);
        add.apply(&connection).unwrap();
        connection
            .execute("UPDATE notes SET body = 'custom' WHERE id = 1", ())
            .unwrap();
        connection
            .execute("INSERT INTO notes VALUES (2, NULL)", ())
            .unwrap();

        let drop = drop_operation(&connection);
        let repair_id = drop.repair_id().unwrap();
        assert!(!repair::contains(&connection, repair_id).unwrap());
        assert_eq!(AlterTableOperation::decode(&drop.encode()).unwrap(), drop);
        assert!(
            !drop
                .encode()
                .windows(b"custom".len())
                .any(|window| window == b"custom")
        );
        let lowered = drop.to_homebase().unwrap();
        let AlterTableDelta::DropColumn { column, after, .. } = &drop.delta else {
            unreachable!()
        };
        assert_explicit_range_assertions(
            &lowered.footprint,
            &[column_dependency_prefix(after.table_id(), *column)],
        );
        assert_eq!(lowered.mutations.len(), 4);
        assert_eq!(lowered.footprint.constraints().len(), 2);
        assert_eq!(lowered.footprint.writes().len(), 1);

        apply_speculative_drop(&connection, &drop);
        assert!(repair::contains(&connection, repair_id).unwrap());
        assert_eq!(
            connection
                .query_row(
                    "SELECT count(*) FROM pragma_table_info('notes')",
                    (),
                    |row| row.get::<_, i64>(0)
                )
                .unwrap(),
            1
        );
        drop.rollback(&connection).unwrap();
        assert!(!repair::contains(&connection, repair_id).unwrap());
        assert_eq!(
            connection
                .prepare("SELECT id, body FROM notes ORDER BY id")
                .unwrap()
                .query_map((), |row| {
                    Ok((row.get::<_, i64>(0)?, row.get::<_, Option<String>>(1)?))
                })
                .unwrap()
                .collect::<rusqlite::Result<Vec<_>>>()
                .unwrap(),
            [(1, Some("custom".into())), (2, None)]
        );
        catalog::validate(&connection).unwrap();
    }

    #[test]
    fn failed_speculative_drop_rolls_back_sidecar_schema_and_catalog_together() {
        let (connection, _) = connection();
        simple_add_operation(&connection)
            .apply(&connection)
            .unwrap();
        connection
            .execute("INSERT INTO notes VALUES (1, 'kept')", ())
            .unwrap();
        let drop = drop_operation(&connection);
        let repair_id = drop.repair_id().unwrap();

        let result: Result<()> =
            crate::connection::with_savepoint(&connection, "__multilite__test_failed_drop", || {
                drop.capture_local_repair(&connection)?;
                drop.apply(&connection)?;
                Err(Error::CaptureInvariant("injected after destructive apply"))
            });

        assert!(matches!(
            result,
            Err(Error::CaptureInvariant("injected after destructive apply"))
        ));
        assert!(!repair::contains(&connection, repair_id).unwrap());
        assert_eq!(
            connection
                .query_row("SELECT body FROM notes WHERE id = 1", (), |row| {
                    row.get::<_, String>(0)
                })
                .unwrap(),
            "kept"
        );
        assert_eq!(
            catalog::by_name(&connection, "notes")
                .unwrap()
                .unwrap()
                .columns()
                .len(),
            2
        );
    }

    #[test]
    fn drop_column_wire_size_is_independent_of_destroyed_row_count() {
        let (empty, _) = connection();
        simple_add_operation(&empty).apply(&empty).unwrap();
        let empty_drop = drop_operation(&empty);

        let (populated, _) = connection();
        simple_add_operation(&populated).apply(&populated).unwrap();
        for id in 1..=100 {
            populated
                .execute(
                    "INSERT INTO notes (id, body) VALUES (?1, printf('body-%d', ?1))",
                    [id],
                )
                .unwrap();
        }
        let populated_drop = drop_operation(&populated);

        assert_eq!(empty_drop.encode().len(), populated_drop.encode().len());
        assert!(!repair::contains(&populated, populated_drop.repair_id().unwrap()).unwrap());
    }

    #[test]
    fn admitted_drop_column_materializes_without_creating_local_repair() {
        let (connection, _) = connection();
        simple_add_operation(&connection)
            .apply(&connection)
            .unwrap();
        connection
            .execute("INSERT INTO notes VALUES (1, 'remote')", ())
            .unwrap();
        let drop = drop_operation(&connection);

        drop.apply(&connection).unwrap();

        assert!(!repair::contains(&connection, drop.repair_id().unwrap()).unwrap());
        assert_eq!(
            connection
                .query_row(
                    "SELECT count(*) FROM pragma_table_info('notes')",
                    (),
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            1
        );
    }

    #[test]
    fn middle_not_null_column_rollback_restores_definition_order_and_values() {
        let (connection, created) = connection_with(
            "CREATE TABLE notes (
                id INTEGER PRIMARY KEY,
                guarded TEXT CONSTRAINT guarded_nn NOT NULL DEFAULT 'seed'
                    CONSTRAINT guarded_nonempty CHECK (length(guarded) > 0),
                tail TEXT DEFAULT 'tail'
            )",
        );
        connection
            .execute("INSERT INTO notes VALUES (1, 'custom', 'end')", ())
            .unwrap();
        let drop = prepare_drop(&connection, "ALTER TABLE notes DROP COLUMN guarded");

        apply_speculative_drop(&connection, &drop);
        assert_eq!(
            connection
                .prepare("SELECT name FROM pragma_table_info('notes') ORDER BY cid")
                .unwrap()
                .query_map((), |row| row.get::<_, String>(0))
                .unwrap()
                .collect::<rusqlite::Result<Vec<_>>>()
                .unwrap(),
            ["id", "tail"]
        );
        drop.rollback(&connection).unwrap();

        let columns = connection
            .prepare(
                "SELECT name, \"notnull\", dflt_value
                 FROM pragma_table_info('notes') ORDER BY cid",
            )
            .unwrap()
            .query_map((), |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, bool>(1)?,
                    row.get::<_, Option<String>>(2)?,
                ))
            })
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap();
        assert_eq!(
            columns,
            [
                ("id".into(), false, None),
                ("guarded".into(), true, Some("'seed'".into())),
                ("tail".into(), false, Some("'tail'".into())),
            ]
        );
        assert_eq!(
            connection
                .query_row(
                    "SELECT guarded, tail FROM notes WHERE id = 1",
                    (),
                    |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
                )
                .unwrap(),
            ("custom".into(), "end".into())
        );
        connection
            .execute("INSERT INTO notes (id) VALUES (2)", ())
            .unwrap();
        assert_eq!(
            connection
                .query_row("SELECT guarded FROM notes WHERE id = 2", (), |row| {
                    row.get::<_, String>(0)
                })
                .unwrap(),
            "seed"
        );
        assert!(
            connection
                .execute("INSERT INTO notes VALUES (3, '', 'bad')", ())
                .is_err()
        );
        assert_eq!(
            catalog::by_id(&connection, created.table_id()).unwrap(),
            Some(created)
        );
        catalog::validate(&connection).unwrap();
    }

    #[test]
    fn table_rebuild_preserves_integer_primary_keys_indexes_and_triggers() {
        let (connection, _) = connection_with(
            "CREATE TABLE notes (
                id INTEGER PRIMARY KEY,
                key TEXT UNIQUE,
                middle TEXT,
                tail TEXT
            )",
        );
        connection
            .execute_batch(
                "CREATE INDEX notes_tail ON notes(tail);
                 CREATE TABLE audit (key TEXT);
                 CREATE TRIGGER notes_audit AFTER UPDATE OF tail ON notes
                 BEGIN INSERT INTO audit VALUES (NEW.key); END;
                 INSERT INTO notes(id, key, middle, tail)
                 VALUES (77, 'one', 'middle', 'tail')",
            )
            .unwrap();
        let drop = prepare_drop(&connection, "ALTER TABLE notes DROP COLUMN middle");

        apply_speculative_drop(&connection, &drop);
        drop.rollback(&connection).unwrap();

        assert_eq!(
            connection
                .query_row(
                    "SELECT id, middle FROM notes WHERE key = 'one'",
                    (),
                    |row| Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
                )
                .unwrap(),
            (77, "middle".into())
        );
        assert_eq!(
            connection
                .query_row(
                    "SELECT count(*) FROM sqlite_schema
                     WHERE type = 'index' AND name = 'notes_tail'",
                    (),
                    |row| row.get::<_, i64>(0)
                )
                .unwrap(),
            1
        );
        connection
            .execute("UPDATE notes SET tail = 'changed' WHERE key = 'one'", ())
            .unwrap();
        assert_eq!(
            connection
                .query_row("SELECT key FROM audit", (), |row| row.get::<_, String>(0))
                .unwrap(),
            "one"
        );
    }

    #[test]
    fn without_rowid_middle_column_rollback_preserves_composite_rows() {
        let (connection, _) = connection_with(
            "CREATE TABLE pairs (
                tenant TEXT,
                sequence INTEGER,
                middle BLOB,
                tail TEXT,
                PRIMARY KEY (tenant, sequence)
            ) WITHOUT ROWID",
        );
        connection
            .execute(
                "INSERT INTO pairs VALUES ('acme', 7, x'0001ff', 'tail')",
                (),
            )
            .unwrap();
        let drop = prepare_drop(&connection, "ALTER TABLE pairs DROP COLUMN middle");

        apply_speculative_drop(&connection, &drop);
        drop.rollback(&connection).unwrap();

        assert_eq!(
            connection
                .query_row(
                    "SELECT hex(middle), tail FROM pairs
                     WHERE tenant = 'acme' AND sequence = 7",
                    (),
                    |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
                )
                .unwrap(),
            ("0001FF".into(), "tail".into())
        );
        catalog::validate(&connection).unwrap();
    }

    #[test]
    fn table_rebuild_keeps_incoming_foreign_keys_valid() {
        let (connection, _) = connection_with(
            "CREATE TABLE parents (
                id INTEGER PRIMARY KEY,
                middle TEXT,
                tail TEXT
            )",
        );
        connection
            .pragma_update(None, "foreign_keys", true)
            .unwrap();
        let child_sql = "CREATE TABLE children (
            id INTEGER PRIMARY KEY,
            parent_id INTEGER,
            FOREIGN KEY (parent_id) REFERENCES parents(id)
                ON UPDATE NO ACTION ON DELETE NO ACTION
        )";
        let ValidatedExecute::CreateTable(spec) = crate::sql::validate_execute(child_sql).unwrap()
        else {
            unreachable!()
        };
        let child = CreateTable::prepare(&connection, child_sql, spec).unwrap();
        connection
            .execute(&child.materialization_sql(&connection).unwrap(), ())
            .unwrap();
        catalog::insert(&connection, &child).unwrap();
        connection
            .execute_batch(
                "INSERT INTO parents VALUES (1, 'middle', 'tail');
                 INSERT INTO children VALUES (7, 1)",
            )
            .unwrap();
        let drop = prepare_drop(&connection, "ALTER TABLE parents DROP COLUMN middle");

        apply_speculative_drop(&connection, &drop);
        drop.rollback(&connection).unwrap();

        assert_eq!(
            connection
                .query_row(
                    "SELECT parents.middle, children.parent_id
                     FROM parents JOIN children ON children.parent_id = parents.id",
                    (),
                    |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
                )
                .unwrap(),
            ("middle".into(), 1)
        );
        assert_eq!(
            connection
                .query_row("SELECT count(*) FROM pragma_foreign_key_check", (), |row| {
                    row.get::<_, i64>(0)
                })
                .unwrap(),
            0
        );
        assert!(
            connection
                .db_config(DbConfig::SQLITE_DBCONFIG_ENABLE_FKEY)
                .unwrap()
        );
        catalog::validate(&connection).unwrap();
    }
}
