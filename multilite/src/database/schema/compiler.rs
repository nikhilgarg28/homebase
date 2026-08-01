//! Construction-time invariants for the validated schema IR.

use std::collections::BTreeSet;
use std::fmt;

use super::{
    Column, ColumnId, CreateTable, IndexKind, MAX_INDEX_COLUMNS, SqlExpression, TableId, TableMode,
    TableSchema, TableStorage, index_columns_supported, named_index_definition_is_valid,
    type_declaration_roundtrips,
};

/// One violated invariant while constructing a semantic schema value.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SchemaInvariantError {
    EmptyTable,
    DuplicateColumnName,
    InvalidNullability,
    InvalidColumnType,
    InvalidPrimaryKey,
    InvalidStrictColumn,
    NullablePrimaryKey,
    MissingRowidAlias,
    InvalidUniqueConstraint,
    InvalidNamedIndex,
    DuplicateIndexIdentity,
    DuplicateIndexName,
    InvalidForeignKey,
    SelfReferentialForeignKey,
    DuplicateForeignKeyIdentity,
    InvalidCheckConstraint,
    UnknownExpressionColumn,
    ReusedSchemaIdentity,
    InvalidSchemaRevision,
}

impl fmt::Display for SchemaInvariantError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::EmptyTable => "table schema has no columns",
            Self::DuplicateColumnName => "table schema reuses a column name",
            Self::InvalidNullability => "column nullability contradicts its named constraint",
            Self::InvalidColumnType => "column has an invalid type declaration",
            Self::InvalidPrimaryKey => "table schema has an invalid primary key",
            Self::InvalidStrictColumn => "STRICT table schema has an invalid column declaration",
            Self::NullablePrimaryKey => "table storage requires non-null primary-key columns",
            Self::MissingRowidAlias => "rowid table has no exact INTEGER PRIMARY KEY alias",
            Self::InvalidUniqueConstraint => "table schema has an invalid UNIQUE constraint",
            Self::InvalidNamedIndex => "table schema has an invalid named index",
            Self::DuplicateIndexIdentity => "table schema reuses an index identity",
            Self::DuplicateIndexName => "table schema reuses an active index name",
            Self::InvalidForeignKey => "table schema has an invalid foreign key",
            Self::SelfReferentialForeignKey => "self-referential foreign keys are not supported",
            Self::DuplicateForeignKeyIdentity => "table schema reuses a foreign-key identity",
            Self::InvalidCheckConstraint => "table schema has an invalid CHECK constraint",
            Self::UnknownExpressionColumn => "schema expression references an unknown table column",
            Self::ReusedSchemaIdentity => "schema operation reuses a stable identity",
            Self::InvalidSchemaRevision => {
                "schema revision does not authenticate the encoded table definition"
            }
        })
    }
}

pub(super) fn validate_table_schema(
    owner: TableId,
    schema: &TableSchema,
) -> Result<(), SchemaInvariantError> {
    let columns = &schema.columns;
    if columns.is_empty() {
        return Err(SchemaInvariantError::EmptyTable);
    }
    let mut column_names = BTreeSet::new();
    for column in columns {
        if !column_names.insert(column.name.canonical()) {
            return Err(SchemaInvariantError::DuplicateColumnName);
        }
        if !column.not_null && column.not_null_name.is_some() {
            return Err(SchemaInvariantError::InvalidNullability);
        }
        if column.declared_type.name.is_empty()
            || column.declared_type.arguments.len() > 2
            || column.declared_type.arguments.iter().any(String::is_empty)
            || !type_declaration_roundtrips(&column.declared_type)
        {
            return Err(SchemaInvariantError::InvalidColumnType);
        }
    }

    let primary = &schema.primary_key.index;
    if primary.kind != IndexKind::Primary
        || !valid_index_columns(primary.columns(), columns)
        || !index_columns_supported(IndexKind::Primary, primary.columns().len())
    {
        return Err(SchemaInvariantError::InvalidPrimaryKey);
    }
    if schema.mode == TableMode::Strict
        && columns.iter().any(|column| {
            column.strict_type().is_none()
                || (primary.columns().contains(&column.id) && !column.not_null)
        })
    {
        return Err(SchemaInvariantError::InvalidStrictColumn);
    }
    if schema.storage == TableStorage::WithoutRowid
        && primary.columns().iter().any(|id| {
            columns
                .iter()
                .find(|column| column.id == *id)
                .is_none_or(|column| !column.not_null)
        })
    {
        return Err(SchemaInvariantError::NullablePrimaryKey);
    }
    let rowid_alias = primary.columns().len() == 1
        && primary.columns().first().is_some_and(|id| {
            columns
                .iter()
                .find(|column| column.id == *id)
                .is_some_and(|column| column.declared_type.is_exact_integer())
        });
    if schema.storage == TableStorage::Rowid && !rowid_alias {
        return Err(SchemaInvariantError::MissingRowidAlias);
    }

    let mut index_ids = BTreeSet::from([primary.id.0]);
    for unique in &schema.unique_constraints {
        if unique.index.kind != IndexKind::Unique
            || !valid_index_columns(unique.columns(), columns)
            || !index_columns_supported(IndexKind::Unique, unique.columns().len())
        {
            return Err(SchemaInvariantError::InvalidUniqueConstraint);
        }
        if !index_ids.insert(unique.index.id.0) {
            return Err(SchemaInvariantError::DuplicateIndexIdentity);
        }
    }
    let mut active_index_names = BTreeSet::new();
    for index in &schema.indexes {
        if !matches!(index.index.kind, IndexKind::Unique | IndexKind::Secondary)
            || !named_index_definition_is_valid(index, columns)
        {
            return Err(SchemaInvariantError::InvalidNamedIndex);
        }
        if !index_ids.insert(index.index.id.0) {
            return Err(SchemaInvariantError::DuplicateIndexIdentity);
        }
        if index.active && !active_index_names.insert(index.name.canonical()) {
            return Err(SchemaInvariantError::DuplicateIndexName);
        }
    }

    let mut foreign_ids = BTreeSet::new();
    for foreign_key in &schema.foreign_keys {
        if foreign_key.referenced_table == owner {
            return Err(SchemaInvariantError::SelfReferentialForeignKey);
        }
        if foreign_key.columns.is_empty()
            || foreign_key.columns.len() > MAX_INDEX_COLUMNS
            || foreign_key.columns.len() != foreign_key.referenced_columns.len()
            || foreign_key.columns.len() != foreign_key.referenced_column_names.len()
            || !valid_column_list(&foreign_key.columns, columns)
            || has_duplicates(&foreign_key.referenced_columns)
        {
            return Err(SchemaInvariantError::InvalidForeignKey);
        }
        if !foreign_ids.insert(foreign_key.id.0) {
            return Err(SchemaInvariantError::DuplicateForeignKeyIdentity);
        }
    }

    for check in &schema.checks {
        if check
            .column
            .is_some_and(|id| !columns.iter().any(|column| column.id == id))
            || bind_expression(&check.expression, columns)? != check.dependencies
        {
            return Err(SchemaInvariantError::InvalidCheckConstraint);
        }
    }
    Ok(())
}

/// Bind every column spelling in one parsed SQLite expression to stable IDs.
///
/// Expressions remain an owned SQLite AST for deterministic rendering, while
/// this resolved identity list is the semantic input to DDL conflict planning.
pub(super) fn bind_expression(
    expression: &SqlExpression,
    columns: &[Column],
) -> Result<Vec<ColumnId>, SchemaInvariantError> {
    expression
        .referenced_columns()
        .into_iter()
        .map(|name| {
            columns
                .iter()
                .find(|column| column.name.canonical() == name.canonical())
                .map(|column| column.id)
                .ok_or(SchemaInvariantError::UnknownExpressionColumn)
        })
        .collect::<Result<BTreeSet<_>, _>>()
        .map(BTreeSet::into_iter)
        .map(Iterator::collect)
}

pub(super) fn validate_create_table(created: &CreateTable) -> Result<(), SchemaInvariantError> {
    let schema = &created.schema;
    let mut identities = BTreeSet::new();
    let unique = std::iter::once(created.mutation_id.0)
        .chain([
            created.table_id.0,
            created.schema_revision_id.0,
            schema.primary_key.index.id.0,
        ])
        .chain(schema.columns.iter().map(|column| column.id.0))
        .chain(
            schema
                .unique_constraints
                .iter()
                .map(|unique| unique.index.id.0),
        )
        .chain(schema.indexes.iter().map(|index| index.index.id.0))
        .chain(
            schema
                .foreign_keys
                .iter()
                .map(|foreign_key| foreign_key.id.0),
        )
        .all(|identity| identities.insert(identity));
    if !unique {
        return Err(SchemaInvariantError::ReusedSchemaIdentity);
    }
    if created.schema_revision_id != created.computed_schema_revision() {
        return Err(SchemaInvariantError::InvalidSchemaRevision);
    }
    Ok(())
}

fn valid_index_columns(columns: &[super::ColumnId], known: &[super::Column]) -> bool {
    !columns.is_empty() && valid_column_list(columns, known)
}

fn valid_column_list(columns: &[super::ColumnId], known: &[super::Column]) -> bool {
    columns.iter().enumerate().all(|(index, column)| {
        !columns[..index].contains(column) && known.iter().any(|known| known.id == *column)
    })
}

fn has_duplicates<T: PartialEq>(values: &[T]) -> bool {
    values
        .iter()
        .enumerate()
        .any(|(index, value)| values[..index].contains(value))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Error;
    use crate::database::schema::{CreateColumn, CreateTable, TypeDeclaration};
    use crate::database::sql::{ValidatedExecute, validate_execute};
    use uuid::Uuid;

    fn table() -> CreateTable {
        let sql = "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT NOT NULL)";
        let ValidatedExecute::CreateTable(spec) = validate_execute(sql).unwrap() else {
            unreachable!()
        };
        CreateTable::prepare(&rusqlite::Connection::open_in_memory().unwrap(), sql, spec).unwrap()
    }

    #[test]
    fn one_validator_guards_resolved_and_decoded_schema_values() {
        let created = table();
        created.validate_ir().unwrap();
        let decoded = CreateTable::decode(&created.encode()).unwrap();
        decoded.validate_ir().unwrap();
        assert_eq!(decoded, created);
    }

    #[test]
    fn invariant_failures_are_classified_before_encoding() {
        let mut empty = table();
        empty.schema.columns.clear();
        assert_eq!(empty.validate_ir(), Err(SchemaInvariantError::EmptyTable));

        let mut invalid_rowid = table();
        invalid_rowid.schema.columns[0].declared_type = TypeDeclaration::text();
        assert_eq!(
            invalid_rowid.validate_ir(),
            Err(SchemaInvariantError::MissingRowidAlias)
        );

        let mut reused = table();
        reused.schema.primary_key.index.id.0 = reused.table_id.0;
        assert_eq!(
            reused.validate_ir(),
            Err(SchemaInvariantError::ReusedSchemaIdentity)
        );
    }

    #[test]
    fn schema_evolution_cannot_construct_an_invalid_ir() {
        let created = table();
        let duplicate = CreateColumn {
            name: created.columns()[1].name().clone(),
            declared_type: TypeDeclaration::text(),
            not_null: false,
            not_null_name: None,
            default: None,
            primary_key: None,
        };

        assert!(matches!(
            created.with_added_column_identity(
                ColumnId::from_bytes(Uuid::new_v4().into_bytes()),
                &duplicate,
                &[],
            ),
            Err(Error::InvalidDatabase(
                "schema evolution produced an invalid table definition"
            ))
        ));
    }
}
