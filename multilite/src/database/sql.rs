//! SQLite-AST checks for the database's current public SQL surface.

use fallible_iterator::FallibleIterator as _;
use sqlite3_parser::ast::{
    Cmd, ColumnConstraint, CreateTableBody, InsertBody, Stmt, TabFlags, TableConstraint,
};
use sqlite3_parser::lexer::sql::Parser;

use crate::{Error, Result};

pub(crate) fn validate_execute(sql: &str) -> Result<()> {
    match parse_one(sql)? {
        Stmt::CreateTable {
            temporary, body, ..
        } => {
            if temporary {
                return Err(Error::UnsupportedSql("temporary tables are not supported"));
            }
            validate_create_table(&body)
        }
        Stmt::Insert {
            or_conflict,
            body,
            returning,
            ..
        } => {
            let has_upsert = matches!(body, InsertBody::Select(_, Some(_)));
            if or_conflict.is_some() || has_upsert {
                return Err(Error::UnsupportedSql(
                    "INSERT conflict clauses and REPLACE are not supported",
                ));
            }
            if returning.is_some() {
                return Err(Error::UnsupportedSql("INSERT RETURNING is not supported"));
            }
            Ok(())
        }
        _ => Err(Error::UnsupportedSql(
            "execute accepts only CREATE TABLE and INSERT",
        )),
    }
}

fn parse_one(sql: &str) -> Result<Stmt> {
    let mut parser = Parser::new(sql.as_bytes());
    let first = parser
        .next()
        .map_err(|_| Error::UnsupportedSql("statement is not valid SQLite SQL"))?
        .ok_or(Error::UnsupportedSql("statement is empty"))?;
    if parser
        .next()
        .map_err(|_| Error::UnsupportedSql("statement is not valid SQLite SQL"))?
        .is_some()
    {
        return Err(Error::UnsupportedSql(
            "multiple statements are not supported",
        ));
    }
    match first {
        Cmd::Stmt(statement) => Ok(statement),
        Cmd::Explain(_) | Cmd::ExplainQueryPlan(_) => {
            Err(Error::UnsupportedSql("EXPLAIN is not supported"))
        }
    }
}

fn validate_create_table(body: &CreateTableBody) -> Result<()> {
    let CreateTableBody::ColumnsAndConstraints {
        columns,
        constraints,
        flags,
    } = body
    else {
        return Ok(());
    };

    let column_autoincrement = columns.values().any(|column| {
        column.constraints.iter().any(|constraint| {
            matches!(
                &constraint.constraint,
                ColumnConstraint::PrimaryKey {
                    auto_increment: true,
                    ..
                }
            )
        })
    });
    let table_autoincrement = constraints.as_ref().is_some_and(|constraints| {
        constraints.iter().any(|constraint| {
            matches!(
                &constraint.constraint,
                TableConstraint::PrimaryKey {
                    auto_increment: true,
                    ..
                }
            )
        })
    });
    if flags.contains(TabFlags::Autoincrement) || column_autoincrement || table_autoincrement {
        return Err(Error::UnsupportedSql("AUTOINCREMENT is not supported"));
    }

    let column_conflict = columns.values().any(|column| {
        column
            .constraints
            .iter()
            .any(|constraint| match &constraint.constraint {
                ColumnConstraint::PrimaryKey {
                    conflict_clause, ..
                }
                | ColumnConstraint::NotNull {
                    conflict_clause, ..
                } => conflict_clause.is_some(),
                ColumnConstraint::Unique(conflict_clause) => conflict_clause.is_some(),
                _ => false,
            })
    });
    let table_conflict = constraints.as_ref().is_some_and(|constraints| {
        constraints
            .iter()
            .any(|constraint| match &constraint.constraint {
                TableConstraint::PrimaryKey {
                    conflict_clause, ..
                }
                | TableConstraint::Unique {
                    conflict_clause, ..
                } => conflict_clause.is_some(),
                TableConstraint::Check(_, conflict_clause) => conflict_clause.is_some(),
                TableConstraint::ForeignKey { .. } => false,
            })
    });
    if column_conflict || table_conflict {
        return Err(Error::UnsupportedSql(
            "CREATE TABLE conflict clauses are not supported",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_unsupported(sql: &str) {
        assert!(
            matches!(validate_execute(sql), Err(Error::UnsupportedSql(_))),
            "statement was accepted: {sql}"
        );
    }

    #[test]
    fn accepts_plain_create_table_and_insert_forms() {
        for sql in [
            "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT UNIQUE)",
            "CREATE TABLE copy AS SELECT 1 AS value",
            "INSERT INTO notes VALUES (1, 'ON CONFLICT')",
            "INSERT INTO \"replace\" VALUES (1)",
            "WITH value(id) AS (SELECT 1) INSERT INTO notes SELECT id, 'x' FROM value",
        ] {
            validate_execute(sql).unwrap();
        }
    }

    #[test]
    fn rejects_replace_and_insert_conflict_forms() {
        for sql in [
            "REPLACE INTO notes VALUES (1)",
            "INSERT OR IGNORE INTO notes VALUES (1)",
            "INSERT OR REPLACE INTO notes VALUES (1)",
            "INSERT INTO notes VALUES (1) ON CONFLICT DO NOTHING",
            "INSERT INTO notes VALUES (1) ON CONFLICT(id) DO UPDATE SET id = 2",
            "WITH value(id) AS (SELECT 1) INSERT OR FAIL INTO notes SELECT id FROM value",
        ] {
            assert_unsupported(sql);
        }
    }

    #[test]
    fn rejects_autoincrement_and_schema_conflict_policies() {
        for sql in [
            "CREATE TABLE notes (id INTEGER PRIMARY KEY AUTOINCREMENT)",
            "CREATE TABLE notes (id INTEGER PRIMARY KEY ON CONFLICT REPLACE)",
            "CREATE TABLE notes (id INTEGER NOT NULL ON CONFLICT IGNORE)",
            "CREATE TABLE notes (id INTEGER UNIQUE ON CONFLICT FAIL)",
            "CREATE TABLE notes (id INTEGER, PRIMARY KEY (id) ON CONFLICT ABORT)",
            "CREATE TABLE notes (id INTEGER, UNIQUE (id) ON CONFLICT ROLLBACK)",
        ] {
            assert_unsupported(sql);
        }
    }

    #[test]
    fn rejects_every_other_statement_shape() {
        for sql in [
            "",
            "SELECT 1",
            "UPDATE notes SET id = 2",
            "BEGIN",
            "EXPLAIN SELECT 1",
            "INSERT INTO notes VALUES (1) RETURNING id",
            "CREATE TABLE one (id); CREATE TABLE two (id)",
        ] {
            assert_unsupported(sql);
        }
    }
}
