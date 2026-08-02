//! Debug and CI checks for catalog-to-SQLite structural equivalence.

use std::collections::BTreeMap;

use rusqlite::{Connection, OptionalExtension};

use crate::catalog;
use crate::logical::schema::{
    CreateCheckConstraint, CreateColumn, CreateForeignKey, CreateTableSpec, CreateUnique,
    DefaultDefinition, SqlName, TableId,
};
use crate::sql::{CreateIndexSpec, CreateIndexTerm, ValidatedExecute};
use crate::{Error, Result};

const TABLE_MISMATCH: &str = "canonical SQLite table diverges from the schema catalog";
const INDEX_MISMATCH: &str = "canonical SQLite indexes diverge from the schema catalog";

/// Verify one table and every active explicit index against the catalog IR.
pub(super) fn verify_table(connection: &Connection, table: TableId) -> Result<()> {
    let definition =
        catalog::by_id(connection, table)?.ok_or(Error::InvalidDatabase(TABLE_MISMATCH))?;
    let name =
        catalog::name_by_id(connection, table)?.ok_or(Error::InvalidDatabase(TABLE_MISMATCH))?;
    let actual_sql = connection
        .query_row(
            "SELECT sql FROM main.sqlite_schema
             WHERE type = 'table' AND name = ?1 COLLATE NOCASE",
            [name.value()],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .ok_or(Error::InvalidDatabase(TABLE_MISMATCH))?;
    let expected_sql = definition.materialization_sql(connection)?;
    let actual = parse_table(&actual_sql)?;
    let expected = parse_table(&expected_sql)?;
    if !same_table(&actual, &expected) {
        return Err(Error::InvalidDatabase(TABLE_MISMATCH));
    }

    let mut expected_indexes = BTreeMap::new();
    for index in definition
        .indexes()
        .iter()
        .filter(|index| index.is_active())
    {
        let sql = index.materialization_sql(connection, &definition, &name)?;
        let spec = parse_index(&sql)?;
        if expected_indexes
            .insert(index.name().canonical().to_vec(), spec)
            .is_some()
        {
            return Err(Error::InvalidDatabase(INDEX_MISMATCH));
        }
    }

    let mut statement = connection.prepare(
        "SELECT name, sql FROM main.sqlite_schema
         WHERE type = 'index' AND tbl_name = ?1 COLLATE NOCASE AND sql IS NOT NULL
         ORDER BY name",
    )?;
    let physical = statement
        .query_map([name.value()], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let mut actual_indexes = BTreeMap::new();
    for (name, sql) in physical {
        let name = SqlName::new(name);
        if actual_indexes
            .insert(name.canonical().to_vec(), parse_index(&sql)?)
            .is_some()
        {
            return Err(Error::InvalidDatabase(INDEX_MISMATCH));
        }
    }
    if expected_indexes.len() != actual_indexes.len()
        || expected_indexes.iter().any(|(name, expected)| {
            actual_indexes
                .get(name)
                .is_none_or(|actual| !same_index(actual, expected))
        })
    {
        return Err(Error::InvalidDatabase(INDEX_MISMATCH));
    }
    Ok(())
}

fn parse_table(sql: &str) -> Result<CreateTableSpec> {
    match crate::sql::validate_execute(sql) {
        Ok(ValidatedExecute::CreateTable(spec))
        | Ok(ValidatedExecute::CreateTableIfNotExists(spec)) => Ok(spec),
        _ => Err(Error::InvalidDatabase(TABLE_MISMATCH)),
    }
}

fn parse_index(sql: &str) -> Result<CreateIndexSpec> {
    match crate::sql::validate_execute(sql) {
        Ok(ValidatedExecute::CreateIndex(spec))
        | Ok(ValidatedExecute::CreateIndexIfNotExists(spec)) => Ok(spec),
        _ => Err(Error::InvalidDatabase(INDEX_MISMATCH)),
    }
}

fn same_table(actual: &CreateTableSpec, expected: &CreateTableSpec) -> bool {
    same_name(&actual.name, &expected.name)
        && actual.mode == expected.mode
        && actual.storage == expected.storage
        && same_optional_name(
            actual.primary_key_name.as_ref(),
            expected.primary_key_name.as_ref(),
        )
        && actual.columns.len() == expected.columns.len()
        && actual
            .columns
            .iter()
            .zip(&expected.columns)
            .all(|(actual, expected)| same_column(actual, expected))
        && actual.unique_constraints.len() == expected.unique_constraints.len()
        && actual
            .unique_constraints
            .iter()
            .zip(&expected.unique_constraints)
            .all(|(actual, expected)| same_unique(actual, expected))
        && actual.foreign_keys.len() == expected.foreign_keys.len()
        && actual
            .foreign_keys
            .iter()
            .zip(&expected.foreign_keys)
            .all(|(actual, expected)| same_foreign_key(actual, expected))
        && actual.checks.len() == expected.checks.len()
        && actual
            .checks
            .iter()
            .zip(&expected.checks)
            .all(|(actual, expected)| same_check(actual, expected))
}

fn same_column(actual: &CreateColumn, expected: &CreateColumn) -> bool {
    same_name(&actual.name, &expected.name)
        && actual.declared_type == expected.declared_type
        && actual.not_null == expected.not_null
        && same_optional_name(
            actual.not_null_name.as_ref(),
            expected.not_null_name.as_ref(),
        )
        && same_default(actual.default.as_ref(), expected.default.as_ref())
        && actual.primary_key == expected.primary_key
}

fn same_default(actual: Option<&DefaultDefinition>, expected: Option<&DefaultDefinition>) -> bool {
    match (actual, expected) {
        (None, None) => true,
        (Some(actual), Some(expected)) => {
            same_optional_name(actual.name.as_ref(), expected.name.as_ref())
                && actual.expression == expected.expression
        }
        _ => false,
    }
}

fn same_unique(actual: &CreateUnique, expected: &CreateUnique) -> bool {
    same_optional_name(actual.name.as_ref(), expected.name.as_ref())
        && same_names(&actual.columns, &expected.columns)
}

fn same_foreign_key(actual: &CreateForeignKey, expected: &CreateForeignKey) -> bool {
    same_optional_name(actual.name.as_ref(), expected.name.as_ref())
        && same_names(&actual.columns, &expected.columns)
        && same_name(&actual.referenced_table, &expected.referenced_table)
        && actual.on_delete == expected.on_delete
        && match (
            actual.referenced_columns.as_deref(),
            expected.referenced_columns.as_deref(),
        ) {
            (None, None) => true,
            (Some(actual), Some(expected)) => same_names(actual, expected),
            _ => false,
        }
}

fn same_check(actual: &CreateCheckConstraint, expected: &CreateCheckConstraint) -> bool {
    // SQLite may retain a CHECK inline after ADD COLUMN while the structural
    // renderer emits the same constraint at table scope. Placement is not part
    // of CHECK semantics; ownership and dependencies live in the catalog IR.
    same_optional_name(actual.name.as_ref(), expected.name.as_ref())
        && actual.expression == expected.expression
}

fn same_index(actual: &CreateIndexSpec, expected: &CreateIndexSpec) -> bool {
    actual.unique == expected.unique
        && same_name(&actual.name, &expected.name)
        && same_name(&actual.table, &expected.table)
        && actual.terms.len() == expected.terms.len()
        && actual
            .terms
            .iter()
            .zip(&expected.terms)
            .all(|(actual, expected)| same_index_term(actual, expected))
        && actual.predicate == expected.predicate
}

fn same_index_term(actual: &CreateIndexTerm, expected: &CreateIndexTerm) -> bool {
    match (actual, expected) {
        (
            CreateIndexTerm::Column {
                name: actual_name,
                collation: actual_collation,
                order: actual_order,
            },
            CreateIndexTerm::Column {
                name: expected_name,
                collation: expected_collation,
                order: expected_order,
            },
        ) => {
            same_name(actual_name, expected_name)
                && same_optional_name(actual_collation.as_ref(), expected_collation.as_ref())
                && actual_order == expected_order
        }
        (
            CreateIndexTerm::Expression {
                expression: actual_expression,
                order: actual_order,
            },
            CreateIndexTerm::Expression {
                expression: expected_expression,
                order: expected_order,
            },
        ) => actual_expression == expected_expression && actual_order == expected_order,
        _ => false,
    }
}

fn same_names(actual: &[SqlName], expected: &[SqlName]) -> bool {
    actual.len() == expected.len()
        && actual
            .iter()
            .zip(expected)
            .all(|(actual, expected)| same_name(actual, expected))
}

fn same_optional_name(actual: Option<&SqlName>, expected: Option<&SqlName>) -> bool {
    match (actual, expected) {
        (None, None) => true,
        (Some(actual), Some(expected)) => same_name(actual, expected),
        _ => false,
    }
}

fn same_name(actual: &SqlName, expected: &SqlName) -> bool {
    actual.canonical() == expected.canonical()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logical::index::IndexOperation;
    use crate::logical::schema::CreateTable;

    fn table_definition(connection: &Connection) -> CreateTable {
        let sql = "CREATE TABLE Notes (
            id INTEGER PRIMARY KEY,
            tenant TEXT NOT NULL,
            slug TEXT DEFAULT 'draft',
            CONSTRAINT tenant_slug UNIQUE (tenant, slug),
            CHECK (length(slug) > 0)
        ) STRICT";
        let ValidatedExecute::CreateTable(spec) = crate::sql::validate_execute(sql).unwrap() else {
            unreachable!()
        };
        CreateTable::prepare(connection, sql, spec).unwrap()
    }

    #[test]
    fn verifier_accepts_catalog_table_and_explicit_index_structure() {
        let connection = Connection::open_in_memory().unwrap();
        catalog::initialize(&connection).unwrap();
        let table = table_definition(&connection);
        connection
            .execute(&table.materialization_sql(&connection).unwrap(), ())
            .unwrap();
        catalog::insert(&connection, &table).unwrap();
        verify_table(&connection, table.table_id()).unwrap();

        let sql = "CREATE INDEX NotesSearch ON Notes (slug DESC) WHERE tenant <> ''";
        let ValidatedExecute::CreateIndex(spec) = crate::sql::validate_execute(sql).unwrap() else {
            unreachable!()
        };
        connection.execute(sql, ()).unwrap();
        let index = IndexOperation::prepare_create(&connection, sql, &spec).unwrap();
        index.record_catalog(&connection).unwrap();
        verify_table(&connection, table.table_id()).unwrap();
    }

    #[test]
    fn verifier_rejects_unrepresented_table_and_index_changes() {
        let connection = Connection::open_in_memory().unwrap();
        catalog::initialize(&connection).unwrap();
        let table = table_definition(&connection);
        connection
            .execute(&table.materialization_sql(&connection).unwrap(), ())
            .unwrap();
        catalog::insert(&connection, &table).unwrap();

        connection
            .execute("ALTER TABLE Notes ADD COLUMN foreign_column TEXT", ())
            .unwrap();
        assert!(matches!(
            verify_table(&connection, table.table_id()),
            Err(Error::InvalidDatabase(TABLE_MISMATCH))
        ));

        let clean = Connection::open_in_memory().unwrap();
        catalog::initialize(&clean).unwrap();
        let table = table_definition(&clean);
        clean
            .execute(&table.materialization_sql(&clean).unwrap(), ())
            .unwrap();
        catalog::insert(&clean, &table).unwrap();
        clean
            .execute("CREATE INDEX foreign_index ON Notes (slug)", ())
            .unwrap();
        assert!(matches!(
            verify_table(&clean, table.table_id()),
            Err(Error::InvalidDatabase(INDEX_MISMATCH))
        ));
    }
}
