//! SQLite-AST checks for the database's current public SQL surface.

use fallible_iterator::FallibleIterator as _;
use sqlite3_parser::ast::{
    As, Cmd, ColumnConstraint, CreateTableBody, Expr, InsertBody, Name, OneSelect, Operator,
    ResultColumn, SelectTable, Stmt, TabFlags,
};
use sqlite3_parser::lexer::sql::Parser;

use super::schema::{CreateColumn, CreateTableSpec, DeclaredType, SqlName};
use crate::{Error, Result};

pub enum ValidatedExecute {
    CreateTable(CreateTableSpec),
    Insert,
}

/// One supported transaction read rewritten to the internal vtable facade.
pub struct VTabReadPlan {
    pub table_name: String,
    pub rewritten_sql: String,
}

/// Validate the initial transaction-read grammar and replace its one source.
pub fn plan_vtab_read(sql: &str) -> Result<Option<VTabReadPlan>> {
    let command = parse_one_command(sql)?;
    let Cmd::Stmt(Stmt::Select(mut select)) = command else {
        return Err(Error::UnsupportedSql(
            "managed update queries accept only SELECT",
        ));
    };
    if select.with.is_some() || select.body.compounds.is_some() {
        return Err(unsupported_transaction_read());
    }
    let OneSelect::Select {
        columns,
        from,
        where_clause,
        group_by,
        having,
        window_clause,
        ..
    } = &mut select.body.select
    else {
        return Err(unsupported_transaction_read());
    };
    validate_result_columns(columns)?;
    if group_by.is_some() || having.is_some() || window_clause.is_some() {
        return Err(unsupported_transaction_read());
    }
    if let Some(where_clause) = where_clause {
        validate_read_expression(where_clause)?;
    }
    if let Some(order_by) = &select.order_by {
        for column in order_by {
            validate_read_expression(&column.expr)?;
        }
    }
    if let Some(limit) = &select.limit {
        validate_read_expression(&limit.expr)?;
        if let Some(offset) = &limit.offset {
            validate_read_expression(offset)?;
        }
    }

    let Some(from) = from else {
        return Ok(None);
    };
    if from.joins.is_some() {
        return Err(unsupported_transaction_read());
    }
    let Some(source) = from.select.as_deref_mut() else {
        return Err(unsupported_transaction_read());
    };
    let SelectTable::Table(name, alias, indexed) = source else {
        return Err(unsupported_transaction_read());
    };
    if name.db_name.is_some() || name.alias.is_some() || indexed.is_some() {
        return Err(unsupported_transaction_read());
    }
    let table = identifier(&name.name)?;
    if super::is_schema_table(table.value()) {
        return Ok(None);
    }
    if super::has_multilite_prefix(table.value()) {
        return Err(Error::UnsupportedSql(
            "reserved Multilite tables are not supported",
        ));
    }
    if alias.is_none() {
        *alias = Some(As::As(name.name.clone()));
    }
    name.name = Name(super::vtab::MODULE_NAME.into());

    Ok(Some(VTabReadPlan {
        table_name: table.value().to_owned(),
        rewritten_sql: Cmd::Stmt(Stmt::Select(select)).to_string(),
    }))
}

fn validate_result_columns(columns: &[ResultColumn]) -> Result<()> {
    for column in columns {
        match column {
            ResultColumn::Star | ResultColumn::TableStar(_) => {}
            ResultColumn::Expr(expression, _) => validate_read_expression(expression)?,
        }
    }
    Ok(())
}

fn validate_read_expression(expression: &Expr) -> Result<()> {
    match expression {
        Expr::Id(_) | Expr::Name(_) | Expr::Qualified(_, _) | Expr::Variable(_) => Ok(()),
        Expr::Literal(_) => Ok(()),
        Expr::FunctionCallStar {
            name,
            filter_over: None,
        } if name.0.eq_ignore_ascii_case("count") => Ok(()),
        Expr::Binary(left, Operator::Equals | Operator::And, right) => {
            validate_read_expression(left)?;
            validate_read_expression(right)
        }
        Expr::Parenthesized(expressions) if expressions.len() == 1 => {
            validate_read_expression(&expressions[0])
        }
        _ => Err(unsupported_transaction_read()),
    }
}

fn unsupported_transaction_read() -> Error {
    Error::UnsupportedSql(
        "managed update SELECT supports one table with simple equality predicates",
    )
}

pub fn validate_execute(sql: &str) -> Result<ValidatedExecute> {
    match parse_one(sql)? {
        Stmt::CreateTable {
            temporary,
            if_not_exists,
            tbl_name,
            body,
        } => {
            if temporary {
                return Err(Error::UnsupportedSql("temporary tables are not supported"));
            }
            if if_not_exists {
                return Err(Error::UnsupportedSql(
                    "CREATE TABLE IF NOT EXISTS is not supported",
                ));
            }
            if tbl_name.db_name.is_some() || tbl_name.alias.is_some() {
                return Err(Error::UnsupportedSql(
                    "qualified CREATE TABLE names are not supported",
                ));
            }
            validate_create_table(identifier(&tbl_name.name)?, body)
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
            Ok(ValidatedExecute::Insert)
        }
        _ => Err(Error::UnsupportedSql(
            "execute accepts only CREATE TABLE and INSERT",
        )),
    }
}

/// Reject transaction lifecycle commands owned by a managed closure.
pub fn validate_managed_statement(sql: &str) -> Result<()> {
    let command = parse_one_command(sql)?;
    if matches!(
        command,
        Cmd::Stmt(
            Stmt::Begin(..)
                | Stmt::Commit(..)
                | Stmt::Rollback { .. }
                | Stmt::Savepoint(..)
                | Stmt::Release(..)
        )
    ) {
        return Err(Error::UnsupportedSql(
            "transaction control is owned by the managed closure",
        ));
    }
    Ok(())
}

fn parse_one(sql: &str) -> Result<Stmt> {
    match parse_one_command(sql)? {
        Cmd::Stmt(statement) => Ok(statement),
        Cmd::Explain(_) | Cmd::ExplainQueryPlan(_) => {
            Err(Error::UnsupportedSql("EXPLAIN is not supported"))
        }
    }
}

fn parse_one_command(sql: &str) -> Result<Cmd> {
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
    Ok(first)
}

fn validate_create_table(name: SqlName, body: CreateTableBody) -> Result<ValidatedExecute> {
    if super::has_multilite_prefix(name.value()) {
        return Err(Error::UnsupportedSql(
            "reserved Multilite table names are not supported",
        ));
    }
    let CreateTableBody::ColumnsAndConstraints {
        columns,
        constraints,
        flags,
    } = body
    else {
        return Err(Error::UnsupportedSql(
            "CREATE TABLE AS SELECT is not supported",
        ));
    };
    if flags.intersects(TabFlags::WithoutRowid | TabFlags::Strict) {
        return Err(Error::UnsupportedSql(
            "STRICT and WITHOUT ROWID tables are not supported",
        ));
    }
    if constraints
        .as_ref()
        .is_some_and(|constraints| !constraints.is_empty())
    {
        return Err(Error::UnsupportedSql("table constraints are not supported"));
    }

    let mut primary_keys = 0;
    let columns = columns
        .into_values()
        .map(|column| {
            let name = identifier(&column.col_name)?;
            let declared_type = column
                .col_type
                .ok_or(Error::UnsupportedSql("every column must declare a type"))?;
            if declared_type.size.is_some() {
                return Err(Error::UnsupportedSql(
                    "sized column types are not supported",
                ));
            }
            let declared_type = match declared_type.name.trim() {
                name if name.eq_ignore_ascii_case("INTEGER") => DeclaredType::Integer,
                name if name.eq_ignore_ascii_case("REAL") => DeclaredType::Real,
                name if name.eq_ignore_ascii_case("TEXT") => DeclaredType::Text,
                name if name.eq_ignore_ascii_case("BLOB") => DeclaredType::Blob,
                _ => {
                    return Err(Error::UnsupportedSql(
                        "column types must be INTEGER, REAL, TEXT, or BLOB",
                    ));
                }
            };
            let mut not_null = false;
            let mut primary_key = false;
            for constraint in column.constraints {
                if constraint.name.is_some() {
                    return Err(Error::UnsupportedSql(
                        "named column constraints are not supported",
                    ));
                }
                match constraint.constraint {
                    ColumnConstraint::PrimaryKey {
                        order,
                        conflict_clause,
                        auto_increment,
                    } => {
                        if auto_increment {
                            return Err(Error::UnsupportedSql("AUTOINCREMENT is not supported"));
                        }
                        if order.is_some() || conflict_clause.is_some() {
                            return Err(Error::UnsupportedSql(
                                "PRIMARY KEY ordering and conflict clauses are not supported",
                            ));
                        }
                        if primary_key {
                            return Err(Error::UnsupportedSql(
                                "duplicate PRIMARY KEY constraints are not supported",
                            ));
                        }
                        primary_key = true;
                        primary_keys += 1;
                    }
                    ColumnConstraint::NotNull {
                        nullable: false,
                        conflict_clause: None,
                    } => {
                        if not_null {
                            return Err(Error::UnsupportedSql(
                                "duplicate NOT NULL constraints are not supported",
                            ));
                        }
                        not_null = true;
                    }
                    ColumnConstraint::NotNull { .. } => {
                        return Err(Error::UnsupportedSql(
                            "NULL and NOT NULL conflict clauses are not supported",
                        ));
                    }
                    _ => {
                        return Err(Error::UnsupportedSql(
                            "only PRIMARY KEY and NOT NULL column constraints are supported",
                        ));
                    }
                }
            }
            if primary_key && declared_type != DeclaredType::Integer && !not_null {
                return Err(Error::UnsupportedSql(
                    "a non-INTEGER PRIMARY KEY must also be NOT NULL",
                ));
            }
            Ok(CreateColumn {
                name,
                declared_type,
                not_null,
                primary_key,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    if primary_keys != 1 {
        return Err(Error::UnsupportedSql(
            "CREATE TABLE requires exactly one inline PRIMARY KEY",
        ));
    }
    Ok(ValidatedExecute::CreateTable(CreateTableSpec {
        name,
        columns,
    }))
}

fn identifier(name: &Name) -> Result<SqlName> {
    let token = name.0.as_ref();
    let bytes = token.as_bytes();
    let value = match bytes {
        [b'"', middle @ .., b'"'] => unescape_identifier(middle, b'"'),
        [b'`', middle @ .., b'`'] => unescape_identifier(middle, b'`'),
        [b'[', middle @ .., b']'] => unescape_identifier(middle, b']'),
        [b'\'', middle @ .., b'\''] => unescape_identifier(middle, b'\''),
        _ => token.to_owned(),
    };
    if value.is_empty() {
        return Err(Error::UnsupportedSql("empty identifiers are not supported"));
    }
    Ok(SqlName::new(value))
}

fn unescape_identifier(bytes: &[u8], quote: u8) -> String {
    let mut value = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        value.push(bytes[index]);
        if bytes[index] == quote && bytes.get(index + 1) == Some(&quote) {
            index += 1;
        }
        index += 1;
    }
    String::from_utf8(value).expect("SQLite parser identifiers originate in UTF-8 SQL")
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
    fn accepts_restricted_create_table_and_insert_forms() {
        for sql in [
            "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT NOT NULL, payload BLOB)",
            "CREATE TABLE \"Case Sensitive\" (\"Primary Key\" TEXT NOT NULL PRIMARY KEY)",
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
    fn rejects_unnecessary_create_table_grammar() {
        for sql in [
            "CREATE TEMP TABLE notes (id INTEGER PRIMARY KEY)",
            "CREATE TABLE IF NOT EXISTS notes (id INTEGER PRIMARY KEY)",
            "CREATE TABLE main.notes (id INTEGER PRIMARY KEY)",
            "CREATE TABLE notes AS SELECT 1 AS id",
            "CREATE TABLE notes (id)",
            "CREATE TABLE notes (id VARCHAR PRIMARY KEY)",
            "CREATE TABLE notes (id TEXT PRIMARY KEY)",
            "CREATE TABLE notes (id TEXT NOT NULL PRIMARY KEY, body TEXT UNIQUE)",
            "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT DEFAULT 'x')",
            "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT CHECK(length(body) > 0))",
            "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT COLLATE nocase)",
            "CREATE TABLE notes (id INTEGER PRIMARY KEY, parent INTEGER REFERENCES other(id))",
            "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT GENERATED ALWAYS AS (id))",
            "CREATE TABLE notes (id INTEGER CONSTRAINT pk PRIMARY KEY)",
            "CREATE TABLE notes (id INTEGER, PRIMARY KEY (id))",
            "CREATE TABLE notes (id INTEGER PRIMARY KEY) STRICT",
            "CREATE TABLE notes (id TEXT NOT NULL PRIMARY KEY) WITHOUT ROWID",
            "CREATE TABLE notes (id INTEGER PRIMARY KEY AUTOINCREMENT)",
            "CREATE TABLE notes (id INTEGER PRIMARY KEY ON CONFLICT REPLACE)",
            "CREATE TABLE notes (id INTEGER NOT NULL ON CONFLICT IGNORE)",
            "CREATE TABLE notes (id INTEGER UNIQUE ON CONFLICT FAIL)",
            "CREATE TABLE notes (id INTEGER, PRIMARY KEY (id) ON CONFLICT ABORT)",
            "CREATE TABLE notes (id INTEGER, UNIQUE (id) ON CONFLICT ROLLBACK)",
            "CREATE TABLE __MULTILITE__future (id INTEGER PRIMARY KEY)",
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

    #[test]
    fn managed_statements_reject_outer_transaction_control_only() {
        for sql in [
            "BEGIN",
            "BEGIN IMMEDIATE",
            "COMMIT",
            "END",
            "ROLLBACK",
            "ROLLBACK TO nested",
            "SAVEPOINT nested",
            "RELEASE nested",
        ] {
            assert!(matches!(
                validate_managed_statement(sql),
                Err(Error::UnsupportedSql(
                    "transaction control is owned by the managed closure"
                ))
            ));
        }
        for sql in [
            "SELECT 1",
            "EXPLAIN QUERY PLAN SELECT 1",
            "CREATE TABLE notes (id INTEGER PRIMARY KEY)",
            "INSERT INTO notes VALUES (1)",
        ] {
            validate_managed_statement(sql).unwrap();
        }
    }

    #[test]
    fn transaction_reads_rewrite_one_table_and_leave_constant_selects_direct() {
        let plan = plan_vtab_read("SELECT count(*) FROM notes WHERE day = ?1 ORDER BY id")
            .unwrap()
            .unwrap();
        assert_eq!(plan.table_name, "notes");
        assert!(plan.rewritten_sql.contains(super::super::vtab::MODULE_NAME));
        assert!(plan.rewritten_sql.contains("notes"));
        assert!(plan_vtab_read("SELECT 1").unwrap().is_none());
    }

    #[test]
    fn transaction_reads_reject_sources_outside_the_initial_vtable_slice() {
        for sql in [
            "SELECT * FROM notes JOIN tasks USING (id)",
            "SELECT * FROM (SELECT * FROM notes)",
            "SELECT * FROM notes WHERE id = 1 OR id = 2",
            "SELECT EXISTS(SELECT 1 FROM notes)",
            "WITH values AS (SELECT 1) SELECT * FROM values",
            "SELECT * FROM __multilite__vtab",
        ] {
            assert!(
                matches!(plan_vtab_read(sql), Err(Error::UnsupportedSql(_))),
                "transaction read was accepted: {sql}"
            );
        }
    }
}
