//! Identity-preserving table-name binding changes.

use std::fmt;

use homebase_core::reader::Reader;
use homebase_core::tag::Mutation;
use homebase_core::writer::Writer;
use rusqlite::{Connection, ToSql, params_from_iter};
use uuid::{Uuid, Variant, Version};

use super::catalog;
use super::schema::{
    ColumnId, CreateTable, MutationId, SchemaRevisionId, SqlName, TableId, TableStorage,
    active_schema_revision_key, available_hidden_rowid_alias, column_name_scope_key,
    schema_log_key, table_name_scope_key, table_schema_key, write_revision_key,
};
use super::sql::{AddColumnSpec, RenameColumnSpec, RenameTableSpec, ValidatedExecute};
use crate::commit::footprint::ConflictFootprint;
use crate::value::StoredValue;
use crate::{Error, Result};

const ALTER_TABLE_VERSION: u8 = 2;
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
const TAG_COLUMN_SQL: u8 = 11;
const TAG_DROPPED_VALUE: u8 = 12;
const TAG_SCHEMA_REVISION: u8 = 13;
const RENAME_TABLE: u8 = 1;
const RENAME_COLUMN: u8 = 2;
const ADD_COLUMN: u8 = 3;
const DROP_COLUMN: u8 = 4;
const TAG_DROPPED_PRIMARY: u8 = 1;
const TAG_DROPPED_ROWID: u8 = 2;
const TAG_DROPPED_COLUMN: u8 = 3;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum AlterAction {
    RenameTable,
    RenameColumn(ColumnId),
    AddColumn(ColumnId),
    DropColumn(ColumnId),
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct DroppedValue {
    primary: Vec<StoredValue>,
    rowid: Option<StoredValue>,
    value: StoredValue,
}

/// One stable table identity moving between two mutable name bindings.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AlterTableOperation {
    mutation_id: MutationId,
    sql: String,
    table: TableId,
    schema_revision: SchemaRevisionId,
    source_table: SqlName,
    action: AlterAction,
    old_name: SqlName,
    new_name: SqlName,
    before: Option<CreateTable>,
    after: Option<CreateTable>,
    column_sql: Option<String>,
    dropped_values: Vec<DroppedValue>,
}

/// Homebase mutations and conflict footprint for one table alteration.
pub struct AlterTableHomebaseOp {
    pub mutations: Vec<Mutation>,
    pub footprint: ConflictFootprint,
}

impl AlterTableOperation {
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
            action: AlterAction::RenameTable,
            old_name: spec.table.clone(),
            new_name: spec.new_name.clone(),
            before: None,
            after: None,
            column_sql: None,
            dropped_values: Vec::new(),
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
        if created.storage() == TableStorage::Rowid
            && !created
                .primary_key_columns()
                .any(|column| created.is_rowid_alias(column.id()))
        {
            let mut names = catalog::column_names(connection, &created)?;
            let renamed = names
                .iter_mut()
                .find(|name| name.canonical() == spec.old_name.canonical())
                .expect("resolved column name has a catalog binding");
            *renamed = spec.new_name.clone();
            if available_hidden_rowid_alias(names.iter()).is_none() {
                return Err(Error::UnsupportedSql(
                    "RENAME COLUMN must leave one SQLite rowid alias unshadowed",
                ));
            }
        }
        let operation = Self {
            mutation_id: MutationId::from_bytes(Uuid::new_v4().into_bytes()),
            sql: sql.to_owned(),
            table: created.table_id(),
            schema_revision: created.schema_revision_id(),
            source_table: spec.table.clone(),
            action: AlterAction::RenameColumn(column),
            old_name: spec.old_name.clone(),
            new_name: spec.new_name.clone(),
            before: None,
            after: None,
            column_sql: None,
            dropped_values: Vec::new(),
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
        if before.storage() == TableStorage::Rowid
            && !before
                .primary_key_columns()
                .any(|column| before.is_rowid_alias(column.id()))
        {
            let mut names = catalog::column_names(connection, &before)?;
            names.push(spec.column.name.clone());
            if available_hidden_rowid_alias(names.iter()).is_none() {
                return Err(Error::UnsupportedSql(
                    "ADD COLUMN must leave one SQLite rowid alias unshadowed",
                ));
            }
        }
        let (after, column) = before.with_added_column(
            SchemaRevisionId::from_bytes(Uuid::new_v4().into_bytes()),
            &spec.column,
            &spec.checks,
        )?;
        let operation = Self {
            mutation_id: MutationId::from_bytes(Uuid::new_v4().into_bytes()),
            sql: sql.to_owned(),
            table: before.table_id(),
            schema_revision: before.schema_revision_id(),
            source_table: spec.table.clone(),
            action: AlterAction::AddColumn(column),
            old_name: spec.column.name.clone(),
            new_name: spec.column.name.clone(),
            before: Some(before),
            after: Some(after),
            column_sql: None,
            dropped_values: Vec::new(),
        };
        operation.validate().map_err(invalid_operation)?;
        Ok(operation)
    }

    pub fn prepare_drop_column(
        connection: &Connection,
        sql: &str,
        spec: &super::sql::DropColumnSpec,
    ) -> Result<Self> {
        let before = catalog::by_name(connection, spec.table.value())?.ok_or(
            Error::UnsupportedSql("ALTER TABLE target has no synchronized schema identity"),
        )?;
        let column = catalog::column_id_by_name(connection, before.table_id(), &spec.column)?
            .ok_or(Error::UnsupportedSql(
                "ALTER TABLE DROP COLUMN references an unknown column",
            ))?;
        let position = before
            .columns()
            .iter()
            .position(|candidate| candidate.id() == column)
            .expect("catalog binding belongs to its table");
        if position + 1 != before.columns().len() {
            return Err(Error::UnsupportedSql(
                "DROP COLUMN currently supports only the final table column",
            ));
        }
        if before.columns()[position].is_not_null() {
            return Err(Error::UnsupportedSql(
                "DROP COLUMN of NOT NULL columns requires table-rebuild rollback",
            ));
        }
        if catalog::incoming_foreign_keys(connection, before.table_id())?
            .iter()
            .any(|(_, foreign_key)| foreign_key.referenced_columns().contains(&column))
        {
            return Err(Error::UnsupportedSql(
                "DROP COLUMN does not support referenced parent columns",
            ));
        }
        let after = before.with_removed_column(
            SchemaRevisionId::from_bytes(Uuid::new_v4().into_bytes()),
            column,
        )?;
        let column_sql = before.reversible_column_sql(column, &spec.column)?;
        let dropped_values = capture_dropped_values(connection, &before, column, &spec.column)?;
        let operation = Self {
            mutation_id: MutationId::from_bytes(Uuid::new_v4().into_bytes()),
            sql: sql.to_owned(),
            table: before.table_id(),
            schema_revision: before.schema_revision_id(),
            source_table: spec.table.clone(),
            action: AlterAction::DropColumn(column),
            old_name: spec.column.clone(),
            new_name: spec.column.clone(),
            before: Some(before),
            after: Some(after),
            column_sql: Some(column_sql),
            dropped_values,
        };
        operation.validate().map_err(invalid_operation)?;
        Ok(operation)
    }

    /// Move the name registry without evolving table-owned schema state.
    pub fn to_homebase(&self) -> Result<AlterTableHomebaseOp> {
        self.validate().map_err(invalid_operation)?;
        if let AlterAction::AddColumn(column) | AlterAction::DropColumn(column) = self.action {
            let after = self
                .after
                .as_ref()
                .expect("validated ADD COLUMN has after schema");
            let name = column_name_scope_key(self.table, &self.new_name);
            let schema_head = active_schema_revision_key(self.table);
            let write_revision = write_revision_key(self.table);
            let mut footprint = ConflictFootprint::new();
            footprint.add_constraint(name.clone());
            footprint.add_constraint(schema_head.clone());
            footprint.add_write(schema_head.clone());
            footprint.add_write(write_revision.clone());
            let binding = match self.action {
                AlterAction::AddColumn(_) => Mutation::Set {
                    key: name,
                    value: column.as_bytes().to_vec(),
                },
                AlterAction::DropColumn(_) => Mutation::Delete { key: name },
                _ => unreachable!(),
            };
            return Ok(AlterTableHomebaseOp {
                mutations: vec![
                    Mutation::Set {
                        key: schema_log_key(self.mutation_id),
                        value: self.encode(),
                    },
                    binding,
                    Mutation::Set {
                        key: table_schema_key(self.table, after.schema_revision_id()),
                        value: after.encode(),
                    },
                    Mutation::Set {
                        key: schema_head,
                        value: after.schema_revision_id().as_bytes().to_vec(),
                    },
                    Mutation::Set {
                        key: write_revision,
                        value: self.mutation_id.as_bytes().to_vec(),
                    },
                ],
                footprint,
            });
        }
        let (old_name, new_name, value) = match self.action {
            AlterAction::RenameTable => (
                table_name_scope_key(&self.old_name),
                table_name_scope_key(&self.new_name),
                self.table.as_bytes().to_vec(),
            ),
            AlterAction::RenameColumn(column) => (
                column_name_scope_key(self.table, &self.old_name),
                column_name_scope_key(self.table, &self.new_name),
                column.as_bytes().to_vec(),
            ),
            AlterAction::AddColumn(_) => unreachable!(),
            AlterAction::DropColumn(_) => unreachable!(),
        };
        let mut footprint = ConflictFootprint::new();
        footprint.add_constraint(old_name.clone());
        footprint.add_constraint(new_name.clone());
        let schema_fence = matches!(self.action, AlterAction::RenameColumn(_)).then(|| {
            let key = active_schema_revision_key(self.table);
            footprint.add_write(key.clone());
            Mutation::Set {
                key,
                value: self.schema_revision.as_bytes().to_vec(),
            }
        });
        let mut mutations = vec![
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
        mutations.extend(schema_fence);
        Ok(AlterTableHomebaseOp {
            mutations,
            footprint,
        })
    }

    /// Apply an authenticated binding change to canonical SQLite.
    pub fn apply(&self, connection: &Connection) -> Result<()> {
        self.validate().map_err(invalid_operation)?;
        self.validate_catalog_before(connection)?;
        let table = catalog::name_by_id(connection, self.table)?.ok_or(Error::InvalidDatabase(
            "ALTER TABLE identity is missing from the schema catalog",
        ))?;
        let sql = match self.action {
            AlterAction::AddColumn(_) | AlterAction::DropColumn(_) => {
                super::sql::render_alter_table(&self.sql, &table)?
            }
            _ => rename_sql(&table, self.action, &self.old_name, &self.new_name),
        };
        connection.execute_batch(&sql)?;
        self.record_catalog(connection)
    }

    /// Record the binding change after a branch has executed the user's SQL.
    pub fn record_catalog(&self, connection: &Connection) -> Result<()> {
        self.validate_catalog_before(connection)?;
        match self.action {
            AlterAction::RenameTable => {
                catalog::rename_binding(connection, self.table, &self.old_name, &self.new_name)
            }
            AlterAction::RenameColumn(column) => catalog::rename_column_binding(
                connection,
                self.table,
                column,
                &self.old_name,
                &self.new_name,
            ),
            AlterAction::AddColumn(column) => {
                let before = self
                    .before
                    .as_ref()
                    .expect("validated ADD COLUMN has before schema");
                if catalog::by_id(connection, self.table)?.as_ref() != Some(before) {
                    return Err(Error::InvalidDatabase(
                        "ADD COLUMN no longer matches the schema catalog",
                    ));
                }
                catalog::insert_column_binding(connection, self.table, column, &self.new_name)?;
                catalog::replace(
                    connection,
                    self.after
                        .as_ref()
                        .expect("validated ADD COLUMN has after schema"),
                )
            }
            AlterAction::DropColumn(column) => {
                let before = self
                    .before
                    .as_ref()
                    .expect("validated DROP COLUMN has before schema");
                if catalog::by_id(connection, self.table)?.as_ref() != Some(before) {
                    return Err(Error::InvalidDatabase(
                        "DROP COLUMN no longer matches the schema catalog",
                    ));
                }
                catalog::remove_column_binding(connection, self.table, column)?;
                catalog::replace(
                    connection,
                    self.after
                        .as_ref()
                        .expect("validated DROP COLUMN has after schema"),
                )
            }
        }
    }

    /// Reverse one speculative binding change after authority rejects it.
    pub fn rollback(&self, connection: &Connection) -> Result<()> {
        self.validate().map_err(invalid_operation)?;
        self.validate_catalog_after(connection)?;
        let table = catalog::name_by_id(connection, self.table)?.ok_or(Error::InvalidDatabase(
            "pending ALTER TABLE identity is missing from the catalog",
        ))?;
        let sql = match self.action {
            AlterAction::AddColumn(_) => format!(
                "ALTER TABLE {} DROP COLUMN {}",
                quote_identifier(table.value()),
                quote_identifier(self.new_name.value())
            ),
            AlterAction::DropColumn(_) => format!(
                "ALTER TABLE {} ADD COLUMN {}",
                quote_identifier(table.value()),
                self.column_sql
                    .as_deref()
                    .expect("validated DROP COLUMN has a column definition")
            ),
            _ => rename_sql(&table, self.action, &self.new_name, &self.old_name),
        };
        connection.execute_batch(&sql)?;
        match self.action {
            AlterAction::RenameTable => {
                catalog::rename_binding(connection, self.table, &self.new_name, &self.old_name)
            }
            AlterAction::RenameColumn(column) => catalog::rename_column_binding(
                connection,
                self.table,
                column,
                &self.new_name,
                &self.old_name,
            ),
            AlterAction::AddColumn(column) => {
                catalog::remove_column_binding(connection, self.table, column)?;
                catalog::replace(
                    connection,
                    self.before
                        .as_ref()
                        .expect("validated ADD COLUMN has before schema"),
                )
            }
            AlterAction::DropColumn(column) => {
                let before = self
                    .before
                    .as_ref()
                    .expect("validated DROP COLUMN has before schema");
                restore_dropped_values(
                    connection,
                    before,
                    column,
                    &self.old_name,
                    &self.dropped_values,
                )?;
                catalog::insert_column_binding(connection, self.table, column, &self.old_name)?;
                catalog::replace(connection, before)
            }
        }
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
        writer
            .field(
                TAG_ACTION,
                &[match self.action {
                    AlterAction::RenameTable => RENAME_TABLE,
                    AlterAction::RenameColumn(_) => RENAME_COLUMN,
                    AlterAction::AddColumn(_) => ADD_COLUMN,
                    AlterAction::DropColumn(_) => DROP_COLUMN,
                }],
            )
            .expect("ALTER TABLE field fits in u32");
        if let AlterAction::RenameColumn(column)
        | AlterAction::AddColumn(column)
        | AlterAction::DropColumn(column) = self.action
        {
            writer
                .field(TAG_COLUMN, &column.as_bytes())
                .expect("ALTER TABLE field fits in u32");
        }
        if let Some(before) = &self.before {
            writer
                .field(TAG_BEFORE, &before.encode())
                .expect("ALTER TABLE field fits in u32");
        }
        if let Some(after) = &self.after {
            writer
                .field(TAG_AFTER, &after.encode())
                .expect("ALTER TABLE field fits in u32");
        }
        if let Some(column_sql) = &self.column_sql {
            writer
                .field(TAG_COLUMN_SQL, column_sql.as_bytes())
                .expect("ALTER TABLE field fits in u32");
        }
        for value in &self.dropped_values {
            writer
                .field(TAG_DROPPED_VALUE, &encode_dropped_value(value))
                .expect("ALTER TABLE field fits in u32");
        }
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
        let mut schema_revision = None;
        let mut source_table = None;
        let mut action = None;
        let mut column = None;
        let mut before = None;
        let mut after = None;
        let mut column_sql = None;
        let mut dropped_values = Vec::new();
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
                TAG_BEFORE => set_once(
                    &mut before,
                    CreateTable::decode(value).map_err(|_| AlterTableCodecError::InvalidSchema)?,
                )?,
                TAG_AFTER => set_once(
                    &mut after,
                    CreateTable::decode(value).map_err(|_| AlterTableCodecError::InvalidSchema)?,
                )?,
                TAG_COLUMN_SQL => set_once(&mut column_sql, decode_string(value)?)?,
                TAG_DROPPED_VALUE => dropped_values.push(decode_dropped_value(value)?),
                TAG_OLD_NAME => set_once(&mut old_name, SqlName::new(decode_string(value)?))?,
                TAG_NEW_NAME => set_once(&mut new_name, SqlName::new(decode_string(value)?))?,
                _ => {}
            }
        }
        let action = match (
            action.ok_or(AlterTableCodecError::MissingField(TAG_ACTION))?,
            column,
        ) {
            (RENAME_TABLE, None) => AlterAction::RenameTable,
            (RENAME_COLUMN, Some(column)) => AlterAction::RenameColumn(column),
            (ADD_COLUMN, Some(column)) => AlterAction::AddColumn(column),
            (DROP_COLUMN, Some(column)) => AlterAction::DropColumn(column),
            _ => return Err(AlterTableCodecError::InvalidAction),
        };
        let operation = Self {
            mutation_id: mutation_id.ok_or(AlterTableCodecError::MissingField(TAG_MUTATION_ID))?,
            sql: sql.ok_or(AlterTableCodecError::MissingField(TAG_SQL))?,
            table: table.ok_or(AlterTableCodecError::MissingField(TAG_TABLE))?,
            schema_revision: schema_revision
                .ok_or(AlterTableCodecError::MissingField(TAG_SCHEMA_REVISION))?,
            source_table: source_table
                .ok_or(AlterTableCodecError::MissingField(TAG_SOURCE_TABLE))?,
            action,
            old_name: old_name.ok_or(AlterTableCodecError::MissingField(TAG_OLD_NAME))?,
            new_name: new_name.ok_or(AlterTableCodecError::MissingField(TAG_NEW_NAME))?,
            before,
            after,
            column_sql,
            dropped_values,
        };
        operation.validate()?;
        Ok(operation)
    }

    fn validate(&self) -> std::result::Result<(), AlterTableCodecError> {
        match self.action {
            AlterAction::RenameTable | AlterAction::RenameColumn(_)
                if self.before.is_some()
                    || self.after.is_some()
                    || self.column_sql.is_some()
                    || !self.dropped_values.is_empty() =>
            {
                return Err(AlterTableCodecError::InvalidSchema);
            }
            AlterAction::AddColumn(_)
                if self.column_sql.is_some() || !self.dropped_values.is_empty() =>
            {
                return Err(AlterTableCodecError::InvalidSchema);
            }
            _ => {}
        }
        match (
            self.action,
            super::sql::validate_execute(&self.sql)
                .map_err(|_| AlterTableCodecError::InvalidSql)?,
        ) {
            (AlterAction::RenameTable, ValidatedExecute::RenameTable(spec))
                if spec.table == self.source_table
                    && spec.table == self.old_name
                    && spec.new_name == self.new_name => {}
            (AlterAction::RenameColumn(_), ValidatedExecute::RenameColumn(spec))
                if spec.table == self.source_table
                    && spec.old_name == self.old_name
                    && spec.new_name == self.new_name => {}
            (AlterAction::AddColumn(column), ValidatedExecute::AddColumn(spec))
                if spec.table == self.source_table
                    && spec.column.name == self.new_name
                    && self.old_name == self.new_name
                    && self.before.as_ref().is_some_and(|before| {
                        self.after.as_ref().is_some_and(|after| {
                            before.table_id() == self.table
                                && before.schema_revision_id() == self.schema_revision
                                && before.schema_revision_id() != after.schema_revision_id()
                                && before
                                    .with_added_column_identity(
                                        after.schema_revision_id(),
                                        column,
                                        &spec.column,
                                        &spec.checks,
                                    )
                                    .is_ok_and(|expected| expected == *after)
                        })
                    }) => {}
            (AlterAction::DropColumn(column), ValidatedExecute::DropColumn(spec))
                if spec.table == self.source_table
                    && spec.column == self.old_name
                    && self.old_name == self.new_name
                    && self.column_sql.as_ref().is_some_and(|sql| !sql.is_empty())
                    && self.before.as_ref().is_some_and(|before| {
                        self.after.as_ref().is_some_and(|after| {
                            before.table_id() == self.table
                                && before.schema_revision_id() == self.schema_revision
                                && before.schema_revision_id() != after.schema_revision_id()
                                && before
                                    .with_removed_column(after.schema_revision_id(), column)
                                    .is_ok_and(|expected| expected == *after)
                                && self.dropped_values.iter().all(|value| {
                                    value.primary.len() == before.primary_key_columns().count()
                                })
                        })
                    }) => {}
            _ => return Err(AlterTableCodecError::InvalidRename),
        }
        Ok(())
    }

    fn validate_catalog_before(&self, connection: &Connection) -> Result<()> {
        match self.action {
            AlterAction::RenameTable => {
                let current =
                    catalog::name_by_id(connection, self.table)?.ok_or(Error::InvalidDatabase(
                        "table rename identity is missing from the schema catalog",
                    ))?;
                if current.canonical() != self.old_name.canonical()
                    || catalog::by_name(connection, self.new_name.value())?.is_some()
                {
                    return Err(Error::InvalidDatabase(
                        "table rename no longer matches the schema catalog",
                    ));
                }
            }
            AlterAction::RenameColumn(column) => {
                let definition =
                    catalog::by_id(connection, self.table)?.ok_or(Error::InvalidDatabase(
                        "column rename table is missing from the schema catalog",
                    ))?;
                let current = catalog::column_name_by_id(connection, self.table, column)?.ok_or(
                    Error::InvalidDatabase(
                        "column rename identity is missing from the schema catalog",
                    ),
                )?;
                if definition.schema_revision_id() != self.schema_revision
                    || current.canonical() != self.old_name.canonical()
                    || catalog::column_id_by_name(connection, self.table, &self.new_name)?.is_some()
                {
                    return Err(Error::InvalidDatabase(
                        "column rename no longer matches the schema catalog",
                    ));
                }
            }
            AlterAction::AddColumn(column) => {
                let before = self
                    .before
                    .as_ref()
                    .expect("validated ADD COLUMN has before schema");
                if catalog::by_id(connection, self.table)?.as_ref() != Some(before)
                    || catalog::column_id_by_name(connection, self.table, &self.new_name)?.is_some()
                    || catalog::column_name_by_id(connection, self.table, column)?.is_some()
                {
                    return Err(Error::InvalidDatabase(
                        "ADD COLUMN no longer matches the schema catalog",
                    ));
                }
            }
            AlterAction::DropColumn(column) => {
                let before = self
                    .before
                    .as_ref()
                    .expect("validated DROP COLUMN has before schema");
                if catalog::by_id(connection, self.table)?.as_ref() != Some(before)
                    || catalog::column_name_by_id(connection, self.table, column)?
                        .is_none_or(|name| name.canonical() != self.old_name.canonical())
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
        match self.action {
            AlterAction::RenameTable => {
                let current =
                    catalog::name_by_id(connection, self.table)?.ok_or(Error::InvalidDatabase(
                        "pending table rename identity is missing from the catalog",
                    ))?;
                if current.canonical() != self.new_name.canonical()
                    || catalog::by_name(connection, self.old_name.value())?.is_some()
                {
                    return Err(Error::InvalidDatabase(
                        "pending table rename no longer matches SQLite state",
                    ));
                }
            }
            AlterAction::RenameColumn(column) => {
                let definition =
                    catalog::by_id(connection, self.table)?.ok_or(Error::InvalidDatabase(
                        "pending column rename table is missing from the schema catalog",
                    ))?;
                let current = catalog::column_name_by_id(connection, self.table, column)?.ok_or(
                    Error::InvalidDatabase(
                        "pending column rename identity is missing from the catalog",
                    ),
                )?;
                if definition.schema_revision_id() != self.schema_revision
                    || current.canonical() != self.new_name.canonical()
                    || catalog::column_id_by_name(connection, self.table, &self.old_name)?.is_some()
                {
                    return Err(Error::InvalidDatabase(
                        "pending column rename no longer matches SQLite state",
                    ));
                }
            }
            AlterAction::AddColumn(column) => {
                let after = self
                    .after
                    .as_ref()
                    .expect("validated ADD COLUMN has after schema");
                if catalog::by_id(connection, self.table)?.as_ref() != Some(after)
                    || catalog::column_name_by_id(connection, self.table, column)?
                        .is_none_or(|name| name.canonical() != self.new_name.canonical())
                {
                    return Err(Error::InvalidDatabase(
                        "pending ADD COLUMN no longer matches SQLite state",
                    ));
                }
            }
            AlterAction::DropColumn(column) => {
                let after = self
                    .after
                    .as_ref()
                    .expect("validated DROP COLUMN has after schema");
                if catalog::by_id(connection, self.table)?.as_ref() != Some(after)
                    || catalog::column_name_by_id(connection, self.table, column)?.is_some()
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

fn capture_dropped_values(
    connection: &Connection,
    table: &CreateTable,
    _column: ColumnId,
    column_name: &SqlName,
) -> Result<Vec<DroppedValue>> {
    let table_name = catalog::name_by_id(connection, table.table_id())?.ok_or(
        Error::InvalidDatabase("DROP COLUMN table has no current name binding"),
    )?;
    let primary = table
        .primary_key_columns()
        .map(|column| {
            catalog::column_name_by_id(connection, table.table_id(), column.id())?.ok_or(
                Error::InvalidDatabase("DROP COLUMN primary key has no current name binding"),
            )
        })
        .collect::<Result<Vec<_>>>()?;
    let rowid = current_hidden_rowid_alias(connection, table)?;
    let mut selected = primary
        .iter()
        .map(|name| quote_identifier(name.value()))
        .collect::<Vec<_>>();
    if let Some(rowid) = rowid {
        selected.push(quote_identifier(rowid));
    }
    selected.push(quote_identifier(column_name.value()));
    let sql = format!(
        "SELECT {} FROM {}",
        selected.join(", "),
        quote_identifier(table_name.value())
    );
    let primary_width = primary.len();
    let mut statement = connection.prepare(&sql)?;
    let rows = statement.query_map((), |row| {
        let primary = (0..primary_width)
            .map(|index| row.get_ref(index).map(StoredValue::capture))
            .collect::<rusqlite::Result<Vec<_>>>()?;
        let mut next = primary_width;
        let rowid = if rowid.is_some() {
            let value = StoredValue::capture(row.get_ref(next)?);
            next += 1;
            Some(value)
        } else {
            None
        };
        Ok(DroppedValue {
            primary,
            rowid,
            value: StoredValue::capture(row.get_ref(next)?),
        })
    })?;
    // TODO: enforce deterministic row/byte limits and spill large DDL repair
    // payloads before DROP COLUMN is widened beyond its initial restricted form.
    rows.collect::<rusqlite::Result<Vec<_>>>()
        .map_err(Into::into)
}

fn restore_dropped_values(
    connection: &Connection,
    table: &CreateTable,
    _column: ColumnId,
    column_name: &SqlName,
    values: &[DroppedValue],
) -> Result<()> {
    if values.is_empty() {
        return Ok(());
    }
    let table_name = catalog::name_by_id(connection, table.table_id())?.ok_or(
        Error::InvalidDatabase("pending DROP COLUMN table has no current name binding"),
    )?;
    let primary = table
        .primary_key_columns()
        .map(|column| {
            catalog::column_name_by_id(connection, table.table_id(), column.id())?.ok_or(
                Error::InvalidDatabase(
                    "pending DROP COLUMN primary key has no current name binding",
                ),
            )
        })
        .collect::<Result<Vec<_>>>()?;
    let rowid = current_hidden_rowid_alias(connection, table)?;
    let mut predicates = primary
        .iter()
        .map(|name| format!("{} IS ?", quote_identifier(name.value())))
        .collect::<Vec<_>>();
    if let Some(rowid) = rowid {
        predicates.push(format!("{} IS ?", quote_identifier(rowid)));
    }
    let sql = format!(
        "UPDATE {} SET {} = ? WHERE {}",
        quote_identifier(table_name.value()),
        quote_identifier(column_name.value()),
        predicates.join(" AND ")
    );
    let mut statement = connection.prepare(&sql)?;
    for value in values {
        let mut parameters = Vec::<&dyn ToSql>::with_capacity(
            1 + value.primary.len() + usize::from(value.rowid.is_some()),
        );
        parameters.push(&value.value);
        parameters.extend(value.primary.iter().map(|value| value as &dyn ToSql));
        if let Some(rowid) = &value.rowid {
            parameters.push(rowid);
        }
        if statement.execute(params_from_iter(parameters))? != 1 {
            return Err(Error::InvalidDatabase(
                "pending DROP COLUMN row no longer matches SQLite state",
            ));
        }
    }
    Ok(())
}

fn current_hidden_rowid_alias(
    connection: &Connection,
    table: &CreateTable,
) -> Result<Option<&'static str>> {
    if table.storage() == TableStorage::WithoutRowid
        || table
            .primary_key_columns()
            .any(|column| table.is_rowid_alias(column.id()))
    {
        return Ok(None);
    }
    let names = catalog::column_names(connection, table)?;
    available_hidden_rowid_alias(names.iter())
        .map(Some)
        .ok_or(Error::InvalidDatabase(
            "rowid table has no unshadowed hidden rowid alias",
        ))
}

fn encode_dropped_value(value: &DroppedValue) -> Vec<u8> {
    let mut writer = Writer::new();
    for primary in &value.primary {
        writer
            .field(TAG_DROPPED_PRIMARY, &encode_stored_value(primary))
            .expect("dropped value field fits in u32");
    }
    if let Some(rowid) = &value.rowid {
        writer
            .field(TAG_DROPPED_ROWID, &encode_stored_value(rowid))
            .expect("dropped value field fits in u32");
    }
    writer
        .field(TAG_DROPPED_COLUMN, &encode_stored_value(&value.value))
        .expect("dropped value field fits in u32");
    writer.finish()
}

fn decode_dropped_value(frame: &[u8]) -> std::result::Result<DroppedValue, AlterTableCodecError> {
    let mut reader = Reader::new(frame);
    let mut primary = Vec::new();
    let mut rowid = None;
    let mut value = None;
    while let Some((tag, bytes)) = reader
        .field()
        .map_err(|_| AlterTableCodecError::Truncated)?
    {
        match tag {
            TAG_DROPPED_PRIMARY => primary.push(decode_stored_value(bytes)?),
            TAG_DROPPED_ROWID => set_once(&mut rowid, decode_stored_value(bytes)?)?,
            TAG_DROPPED_COLUMN => set_once(&mut value, decode_stored_value(bytes)?)?,
            _ => {}
        }
    }
    Ok(DroppedValue {
        primary,
        rowid,
        value: value.ok_or(AlterTableCodecError::MissingField(TAG_DROPPED_COLUMN))?,
    })
}

fn encode_stored_value(value: &StoredValue) -> Vec<u8> {
    match value {
        StoredValue::Null => vec![0],
        StoredValue::Integer(value) => {
            let mut encoded = vec![1];
            encoded.extend_from_slice(&value.to_be_bytes());
            encoded
        }
        StoredValue::Real(value) => {
            let mut encoded = vec![2];
            encoded.extend_from_slice(&value.to_be_bytes());
            encoded
        }
        StoredValue::Text(value) => {
            let mut encoded = vec![3];
            encoded.extend_from_slice(value);
            encoded
        }
        StoredValue::Blob(value) => {
            let mut encoded = vec![4];
            encoded.extend_from_slice(value);
            encoded
        }
    }
}

fn decode_stored_value(frame: &[u8]) -> std::result::Result<StoredValue, AlterTableCodecError> {
    let Some((&kind, value)) = frame.split_first() else {
        return Err(AlterTableCodecError::InvalidValue);
    };
    match kind {
        0 if value.is_empty() => Ok(StoredValue::Null),
        1 => Ok(StoredValue::Integer(i64::from_be_bytes(
            value
                .try_into()
                .map_err(|_| AlterTableCodecError::InvalidValue)?,
        ))),
        2 => Ok(StoredValue::Real(u64::from_be_bytes(
            value
                .try_into()
                .map_err(|_| AlterTableCodecError::InvalidValue)?,
        ))),
        3 => Ok(StoredValue::Text(value.to_vec())),
        4 => Ok(StoredValue::Blob(value.to_vec())),
        _ => Err(AlterTableCodecError::InvalidValue),
    }
}

fn rename_sql(table: &SqlName, action: AlterAction, from: &SqlName, to: &SqlName) -> String {
    match action {
        AlterAction::RenameTable => format!(
            "ALTER TABLE {} RENAME TO {}",
            quote_identifier(from.value()),
            quote_identifier(to.value())
        ),
        AlterAction::RenameColumn(_) => format!(
            "ALTER TABLE {} RENAME COLUMN {} TO {}",
            quote_identifier(table.value()),
            quote_identifier(from.value()),
            quote_identifier(to.value())
        ),
        AlterAction::AddColumn(_) => unreachable!("ADD COLUMN uses its stored SQL"),
        AlterAction::DropColumn(_) => unreachable!("DROP COLUMN uses its stored SQL"),
    }
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
    InvalidAction,
    InvalidSchema,
    InvalidValue,
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
            Self::InvalidValue => formatter.write_str("invalid retained DROP COLUMN value"),
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
        AlterTableOperation::prepare_rename_table(connection, sql, &spec).unwrap()
    }

    fn column_operation(connection: &Connection) -> AlterTableOperation {
        let sql = "ALTER TABLE notes RENAME COLUMN id TO note_id";
        let ValidatedExecute::RenameColumn(spec) =
            super::super::sql::validate_execute(sql).unwrap()
        else {
            unreachable!()
        };
        AlterTableOperation::prepare_rename_column(connection, sql, &spec).unwrap()
    }

    fn add_operation(connection: &Connection) -> AlterTableOperation {
        let sql = "ALTER TABLE notes ADD COLUMN body TEXT DEFAULT 'empty' CHECK (length(body) > 0)";
        let ValidatedExecute::AddColumn(spec) = super::super::sql::validate_execute(sql).unwrap()
        else {
            unreachable!()
        };
        AlterTableOperation::prepare_add_column(connection, sql, &spec).unwrap()
    }

    fn simple_add_operation(connection: &Connection) -> AlterTableOperation {
        let sql = "ALTER TABLE notes ADD COLUMN body TEXT DEFAULT 'empty'";
        let ValidatedExecute::AddColumn(spec) = super::super::sql::validate_execute(sql).unwrap()
        else {
            unreachable!()
        };
        AlterTableOperation::prepare_add_column(connection, sql, &spec).unwrap()
    }

    fn drop_operation(connection: &Connection) -> AlterTableOperation {
        let sql = "ALTER TABLE notes DROP COLUMN body";
        let ValidatedExecute::DropColumn(spec) = super::super::sql::validate_execute(sql).unwrap()
        else {
            unreachable!()
        };
        AlterTableOperation::prepare_drop_column(connection, sql, &spec).unwrap()
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

    #[test]
    fn column_rename_moves_only_column_name_cells_and_rolls_back() {
        let (connection, created) = connection();
        let operation = column_operation(&connection);
        assert_eq!(
            AlterTableOperation::decode(&operation.encode()).unwrap(),
            operation
        );
        let lowered = operation.to_homebase().unwrap();
        assert_eq!(lowered.mutations.len(), 4);
        assert_eq!(lowered.footprint.constraints().len(), 2);
        assert_eq!(lowered.footprint.writes().len(), 1);
        assert!(
            lowered
                .footprint
                .writes()
                .contains(&active_schema_revision_key(created.table_id()))
        );
        let Mutation::Set { value, .. } = &lowered.mutations[2] else {
            panic!("new column name registry entry was not set")
        };
        assert_eq!(value, &created.columns()[0].id().as_bytes());
        let Mutation::Set { key, value } = &lowered.mutations[3] else {
            panic!("column rename schema fence was not set")
        };
        assert_eq!(key, &active_schema_revision_key(created.table_id()));
        assert_eq!(value, &created.schema_revision_id().as_bytes());

        operation.apply(&connection).unwrap();
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
        let (connection, created) = connection();
        connection
            .execute("INSERT INTO notes VALUES (1)", ())
            .unwrap();
        let operation = add_operation(&connection);
        assert_eq!(
            AlterTableOperation::decode(&operation.encode()).unwrap(),
            operation
        );
        let lowered = operation.to_homebase().unwrap();
        assert_eq!(lowered.mutations.len(), 5);
        assert_eq!(lowered.footprint.constraints().len(), 2);
        assert_eq!(lowered.footprint.writes().len(), 2);

        operation.apply(&connection).unwrap();
        let evolved = catalog::by_id(&connection, created.table_id())
            .unwrap()
            .unwrap();
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
    fn drop_column_retains_values_for_rejection_repair() {
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
        assert_eq!(AlterTableOperation::decode(&drop.encode()).unwrap(), drop);
        let lowered = drop.to_homebase().unwrap();
        assert_eq!(lowered.mutations.len(), 5);
        assert_eq!(lowered.footprint.constraints().len(), 2);
        assert_eq!(lowered.footprint.writes().len(), 2);

        drop.apply(&connection).unwrap();
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
}
