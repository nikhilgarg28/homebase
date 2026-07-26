//! SQLite-AST checks for the database's current public SQL surface.

use fallible_iterator::FallibleIterator as _;
#[cfg(test)]
use sqlite3_parser::ast::{As, OneSelect, Operator, ResultColumn, SelectTable};
use sqlite3_parser::ast::{
    Cmd, ColumnConstraint, CreateTableBody, Expr, InsertBody, Literal, Name, Stmt, TabFlags,
    TableConstraint, Type, TypeSize, UnaryOperator,
};
use sqlite3_parser::lexer::sql::Parser;

use super::schema::{
    CreateColumn, CreateTableSpec, CreateUnique, SqlName, TableMode, TypeDeclaration,
};
use crate::{Error, Result};

pub enum ValidatedExecute {
    CreateTable(CreateTableSpec),
    Insert,
    Delete,
    Update,
}

/// Validate the initial transaction-read grammar and rewrite its sources.
#[cfg(test)]
pub fn rewrite_managed_read(
    sql: &str,
    mut resolve_source: impl FnMut(&str) -> Result<String>,
) -> Result<Option<String>> {
    let command = parse_one_command(sql)?;
    let Cmd::Stmt(Stmt::Select(mut select)) = command else {
        return Err(Error::UnsupportedSql(
            "managed update queries accept only SELECT",
        ));
    };
    if select.with.is_some() || select.body.compounds.is_some() {
        return Err(unsupported_managed_read());
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
        return Err(unsupported_managed_read());
    };
    validate_result_columns(columns)?;
    if group_by.is_some() || having.is_some() || window_clause.is_some() {
        return Err(unsupported_managed_read());
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
        return Err(unsupported_managed_read());
    }
    let Some(source) = from.select.as_deref_mut() else {
        return Err(unsupported_managed_read());
    };
    let SelectTable::Table(name, alias, indexed) = source else {
        return Err(unsupported_managed_read());
    };
    if name.db_name.is_some() || name.alias.is_some() || indexed.is_some() {
        return Err(unsupported_managed_read());
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
    name.name = Name(resolve_source(table.value())?.into());

    Ok(Some(Cmd::Stmt(Stmt::Select(select)).to_string()))
}

#[cfg(test)]
fn validate_result_columns(columns: &[ResultColumn]) -> Result<()> {
    for column in columns {
        match column {
            ResultColumn::Star | ResultColumn::TableStar(_) => {}
            ResultColumn::Expr(expression, _) => validate_read_expression(expression)?,
        }
    }
    Ok(())
}

#[cfg(test)]
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
        _ => Err(unsupported_managed_read()),
    }
}

#[cfg(test)]
fn unsupported_managed_read() -> Error {
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
        Stmt::Delete {
            with,
            tbl_name,
            indexed,
            returning,
            order_by,
            limit,
            ..
        } => {
            if with.is_some() {
                return Err(Error::UnsupportedSql("DELETE WITH is not supported"));
            }
            if tbl_name.db_name.is_some() || tbl_name.alias.is_some() {
                return Err(Error::UnsupportedSql(
                    "qualified and aliased DELETE targets are not supported",
                ));
            }
            let table = identifier(&tbl_name.name)?;
            if super::has_multilite_prefix(table.value())
                || super::is_sqlite_internal_table(table.value())
            {
                return Err(Error::UnsupportedSql(
                    "reserved SQLite and Multilite table names are not supported",
                ));
            }
            if indexed.is_some() {
                return Err(Error::UnsupportedSql(
                    "DELETE index selection is not supported",
                ));
            }
            if returning.is_some() {
                return Err(Error::UnsupportedSql("DELETE RETURNING is not supported"));
            }
            if order_by.is_some() || limit.is_some() {
                return Err(Error::UnsupportedSql(
                    "DELETE ORDER BY and LIMIT are not supported",
                ));
            }
            Ok(ValidatedExecute::Delete)
        }
        Stmt::Update {
            with,
            or_conflict,
            tbl_name,
            indexed,
            sets,
            from,
            returning,
            order_by,
            limit,
            ..
        } => {
            if with.is_some() {
                return Err(Error::UnsupportedSql("UPDATE WITH is not supported"));
            }
            if or_conflict.is_some() {
                return Err(Error::UnsupportedSql(
                    "UPDATE conflict clauses are not supported",
                ));
            }
            if tbl_name.db_name.is_some() || tbl_name.alias.is_some() {
                return Err(Error::UnsupportedSql(
                    "qualified and aliased UPDATE targets are not supported",
                ));
            }
            let table = identifier(&tbl_name.name)?;
            if super::has_multilite_prefix(table.value())
                || super::is_sqlite_internal_table(table.value())
            {
                return Err(Error::UnsupportedSql(
                    "reserved SQLite and Multilite table names are not supported",
                ));
            }
            if indexed.is_some() {
                return Err(Error::UnsupportedSql(
                    "UPDATE index selection is not supported",
                ));
            }
            if sets.iter().any(|set| set.col_names.len() != 1) {
                return Err(Error::UnsupportedSql(
                    "UPDATE tuple assignments are not supported",
                ));
            }
            if from.is_some() {
                return Err(Error::UnsupportedSql("UPDATE FROM is not supported"));
            }
            if returning.is_some() {
                return Err(Error::UnsupportedSql("UPDATE RETURNING is not supported"));
            }
            if order_by.is_some() || limit.is_some() {
                return Err(Error::UnsupportedSql(
                    "UPDATE ORDER BY and LIMIT are not supported",
                ));
            }
            Ok(ValidatedExecute::Update)
        }
        _ => Err(Error::UnsupportedSql(
            "execute accepts only CREATE TABLE, INSERT, DELETE, and UPDATE",
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
    if flags.contains(TabFlags::WithoutRowid) {
        return Err(Error::UnsupportedSql(
            "WITHOUT ROWID tables are not supported",
        ));
    }
    let mode = if flags.contains(TabFlags::Strict) {
        TableMode::Strict
    } else {
        TableMode::Ordinary
    };
    let mut primary_keys = 0;
    let mut unique_constraints = Vec::new();
    let columns = columns
        .into_values()
        .map(|column| {
            let name = identifier(&column.col_name)?;
            let declared_type = column
                .col_type
                .ok_or(Error::UnsupportedSql("every column must declare a type"))?;
            let declared_type = type_declaration(declared_type)?;
            if mode == TableMode::Strict && declared_type.strict_type().is_none() {
                return Err(Error::UnsupportedSql(
                    "STRICT columns must use INT, INTEGER, REAL, TEXT, BLOB, or ANY without size arguments",
                ));
            }
            let mut not_null = false;
            let mut primary_key = false;
            for constraint in column.constraints {
                match constraint.constraint {
                    ColumnConstraint::PrimaryKey {
                        order,
                        conflict_clause,
                        auto_increment,
                    } => {
                        if constraint.name.is_some() {
                            return Err(Error::UnsupportedSql(
                                "named PRIMARY KEY constraints are not supported",
                            ));
                        }
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
                        if constraint.name.is_some() {
                            return Err(Error::UnsupportedSql(
                                "named NOT NULL constraints are not supported",
                            ));
                        }
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
                    ColumnConstraint::Unique(conflict_clause) => {
                        if conflict_clause.is_some() {
                            return Err(Error::UnsupportedSql(
                                "UNIQUE conflict clauses are not supported",
                            ));
                        }
                        unique_constraints.push(CreateUnique {
                            name: constraint.name.map(|name| identifier(&name)).transpose()?,
                            columns: vec![name.clone()],
                        });
                    }
                    _ => {
                        return Err(Error::UnsupportedSql(
                            "only PRIMARY KEY, NOT NULL, and UNIQUE column constraints are supported",
                        ));
                    }
                }
            }
            if mode == TableMode::Strict && primary_key {
                not_null = true;
            } else if primary_key && !declared_type.is_exact_integer() && !not_null {
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
    for constraint in constraints.into_iter().flatten() {
        let TableConstraint::Unique {
            columns: unique_columns,
            conflict_clause,
        } = constraint.constraint
        else {
            return Err(Error::UnsupportedSql(
                "only UNIQUE table constraints are supported",
            ));
        };
        if conflict_clause.is_some() {
            return Err(Error::UnsupportedSql(
                "UNIQUE conflict clauses are not supported",
            ));
        }
        let unique_columns = unique_columns
            .into_iter()
            .map(|column| {
                if column.order.is_some() || column.nulls.is_some() {
                    return Err(Error::UnsupportedSql(
                        "UNIQUE ordering and NULLS placement are not supported",
                    ));
                }
                expression_identifier(column.expr)
            })
            .collect::<Result<Vec<_>>>()?;
        if unique_columns.is_empty()
            || unique_columns.iter().enumerate().any(|(index, column)| {
                unique_columns[..index]
                    .iter()
                    .any(|seen| seen.canonical() == column.canonical())
            })
            || unique_columns.iter().any(|unique| {
                !columns
                    .iter()
                    .any(|column| column.name.canonical() == unique.canonical())
            })
        {
            return Err(Error::UnsupportedSql(
                "UNIQUE constraints require distinct table columns",
            ));
        }
        unique_constraints.push(CreateUnique {
            name: constraint.name.map(|name| identifier(&name)).transpose()?,
            columns: unique_columns,
        });
    }
    if primary_keys != 1 {
        return Err(Error::UnsupportedSql(
            "CREATE TABLE requires exactly one inline PRIMARY KEY",
        ));
    }
    Ok(ValidatedExecute::CreateTable(CreateTableSpec {
        name,
        mode,
        columns,
        unique_constraints,
    }))
}

fn type_declaration(declared: Type) -> Result<TypeDeclaration> {
    let arguments = match declared.size {
        None => Vec::new(),
        Some(TypeSize::MaxSize(argument)) => vec![type_argument(*argument)?],
        Some(TypeSize::TypeSize(first, second)) => {
            vec![type_argument(*first)?, type_argument(*second)?]
        }
    };
    let declaration = TypeDeclaration::new(declared.name, arguments);
    if declaration.name().is_empty() {
        return Err(Error::UnsupportedSql(
            "empty column types are not supported",
        ));
    }
    Ok(declaration)
}

fn type_argument(argument: Expr) -> Result<String> {
    match argument {
        Expr::Literal(Literal::Numeric(value)) => Ok(value.into()),
        Expr::Unary(operator @ (UnaryOperator::Positive | UnaryOperator::Negative), value) => {
            let Expr::Literal(Literal::Numeric(value)) = *value else {
                return Err(Error::UnsupportedSql(
                    "column type sizes must be numeric literals",
                ));
            };
            let sign = match operator {
                UnaryOperator::Positive => "+",
                UnaryOperator::Negative => "-",
                _ => unreachable!(),
            };
            Ok(format!("{sign}{value}"))
        }
        _ => Err(Error::UnsupportedSql(
            "column type sizes must be numeric literals",
        )),
    }
}

fn expression_identifier(expression: Expr) -> Result<SqlName> {
    match expression {
        Expr::Id(id) => identifier(&Name(id.0)),
        Expr::Name(name) => identifier(&name),
        _ => Err(Error::UnsupportedSql(
            "UNIQUE expressions and collations are not supported",
        )),
    }
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
    use crate::database::schema::Affinity;

    fn assert_unsupported(sql: &str) {
        assert!(
            matches!(validate_execute(sql), Err(Error::UnsupportedSql(_))),
            "statement was accepted: {sql}"
        );
    }

    #[test]
    fn accepts_restricted_create_insert_delete_and_update_forms() {
        for sql in [
            "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT NOT NULL, payload BLOB)",
            "CREATE TABLE \"Case Sensitive\" (\"Primary Key\" TEXT NOT NULL PRIMARY KEY)",
            "CREATE TABLE accounts (
                id INTEGER PRIMARY KEY,
                organization TEXT,
                email TEXT,
                CONSTRAINT account_email UNIQUE (Organization, EMAIL)
            )",
            "CREATE TABLE users (
                id INTEGER PRIMARY KEY,
                email TEXT CONSTRAINT user_email UNIQUE
            )",
            "INSERT INTO notes VALUES (1, 'ON CONFLICT')",
            "INSERT INTO \"replace\" VALUES (1)",
            "WITH value(id) AS (SELECT 1) INSERT INTO notes SELECT id, 'x' FROM value",
            "DELETE FROM notes",
            "DELETE FROM notes WHERE id = ?1",
            "DELETE FROM notes WHERE id IN (SELECT id FROM archived)",
            "UPDATE notes SET body = upper(body) WHERE id = ?1",
            "UPDATE notes SET body = (SELECT body FROM archived WHERE archived.id = notes.id)",
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
            "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT DEFAULT 'x')",
            "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT CHECK(length(body) > 0))",
            "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT COLLATE nocase)",
            "CREATE TABLE notes (id INTEGER PRIMARY KEY, parent INTEGER REFERENCES other(id))",
            "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT GENERATED ALWAYS AS (id))",
            "CREATE TABLE notes (id INTEGER CONSTRAINT pk PRIMARY KEY)",
            "CREATE TABLE notes (id INTEGER, PRIMARY KEY (id))",
            "CREATE TABLE notes (id TEXT NOT NULL PRIMARY KEY) WITHOUT ROWID",
            "CREATE TABLE notes (id INTEGER PRIMARY KEY AUTOINCREMENT)",
            "CREATE TABLE notes (id INTEGER PRIMARY KEY ON CONFLICT REPLACE)",
            "CREATE TABLE notes (id INTEGER NOT NULL ON CONFLICT IGNORE)",
            "CREATE TABLE notes (id INTEGER UNIQUE ON CONFLICT FAIL)",
            "CREATE TABLE notes (id INTEGER, PRIMARY KEY (id) ON CONFLICT ABORT)",
            "CREATE TABLE notes (id INTEGER, UNIQUE (id) ON CONFLICT ROLLBACK)",
            "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT, UNIQUE (body COLLATE NOCASE))",
            "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT, UNIQUE (body DESC))",
            "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT, UNIQUE (lower(body)))",
            "CREATE TABLE notes (id INTEGER PRIMARY KEY, UNIQUE (missing))",
            "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT, UNIQUE (body, body))",
            "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT, UNIQUE (body, BODY))",
            "CREATE TABLE __MULTILITE__future (id INTEGER PRIMARY KEY)",
        ] {
            assert_unsupported(sql);
        }
    }

    #[test]
    fn normalizes_inline_and_composite_unique_constraints() {
        let ValidatedExecute::CreateTable(spec) = validate_execute(
            "CREATE TABLE accounts (
                id INTEGER PRIMARY KEY,
                email TEXT CONSTRAINT email_key UNIQUE,
                organization TEXT,
                CONSTRAINT account_key UNIQUE (organization, email)
            )",
        )
        .unwrap() else {
            unreachable!()
        };

        assert_eq!(spec.unique_constraints.len(), 2);
        assert_eq!(
            spec.unique_constraints[0].name.as_ref().map(SqlName::value),
            Some("email_key")
        );
        assert_eq!(
            spec.unique_constraints[0]
                .columns
                .iter()
                .map(SqlName::value)
                .collect::<Vec<_>>(),
            ["email"]
        );
        assert_eq!(
            spec.unique_constraints[1]
                .columns
                .iter()
                .map(SqlName::value)
                .collect::<Vec<_>>(),
            ["organization", "email"]
        );
    }

    #[test]
    fn retains_type_declarations_and_derives_sqlite_affinity_in_rule_order() {
        let ValidatedExecute::CreateTable(spec) = validate_execute(
            "CREATE TABLE values_by_type (
                id INTEGER PRIMARY KEY,
                short_name VARCHAR(40),
                amount DECIMAL(10, 2),
                flag BOOLEAN,
                payload BLOB,
                ratio DOUBLE PRECISION,
                surprising FLOATING POINT,
                label STRING,
                anything ANY
            )",
        )
        .unwrap() else {
            unreachable!()
        };

        let declarations = spec
            .columns
            .iter()
            .map(|column| {
                (
                    column.declared_type.name(),
                    column.declared_type.arguments().to_vec(),
                    column.declared_type.affinity(),
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(
            declarations,
            [
                ("INTEGER", vec![], Affinity::Integer),
                ("VARCHAR", vec!["40".to_owned()], Affinity::Text),
                (
                    "DECIMAL",
                    vec!["10".to_owned(), "2".to_owned()],
                    Affinity::Numeric,
                ),
                ("BOOLEAN", vec![], Affinity::Numeric),
                ("BLOB", vec![], Affinity::Blob),
                ("DOUBLE PRECISION", vec![], Affinity::Real),
                ("FLOATING POINT", vec![], Affinity::Integer),
                ("STRING", vec![], Affinity::Numeric),
                ("ANY", vec![], Affinity::Numeric),
            ]
        );
    }

    #[test]
    fn strict_tables_accept_only_strict_types_and_preserve_any_affinity() {
        let ValidatedExecute::CreateTable(spec) = validate_execute(
            "CREATE TABLE strict_values (
                id INT PRIMARY KEY,
                rowid_alias INTEGER UNIQUE,
                count INT,
                ratio REAL,
                label TEXT,
                payload BLOB,
                anything ANY UNIQUE
            ) STRICT",
        )
        .unwrap() else {
            unreachable!()
        };

        assert_eq!(spec.mode, TableMode::Strict);
        assert!(spec.columns[0].not_null);
        assert!(!spec.columns[0].declared_type.is_exact_integer());
        assert!(spec.columns[1].declared_type.is_exact_integer());
        assert_eq!(
            spec.columns
                .iter()
                .map(|column| column.declared_type.strict_type())
                .collect::<Vec<_>>(),
            [
                Some(super::super::schema::StrictType::Integer),
                Some(super::super::schema::StrictType::Integer),
                Some(super::super::schema::StrictType::Integer),
                Some(super::super::schema::StrictType::Real),
                Some(super::super::schema::StrictType::Text),
                Some(super::super::schema::StrictType::Blob),
                Some(super::super::schema::StrictType::Any),
            ]
        );
        assert_eq!(
            spec.columns[6].declared_type.affinity_for(spec.mode),
            super::super::schema::Affinity::Blob
        );
        assert_eq!(
            TypeDeclaration::new("ANY".into(), Vec::new()).affinity_for(TableMode::Ordinary),
            super::super::schema::Affinity::Numeric
        );
    }

    #[test]
    fn strict_tables_reject_ordinary_declarations_and_size_arguments() {
        for declaration in [
            "VARCHAR(40)",
            "DECIMAL(10, 2)",
            "BOOLEAN",
            "DOUBLE",
            "INTEGER(8)",
            "\"UNSIGNED BIG INT\"",
        ] {
            assert_unsupported(&format!(
                "CREATE TABLE strict_values (
                    id INTEGER PRIMARY KEY,
                    value {declaration}
                ) STRICT"
            ));
        }
    }

    #[test]
    fn only_exact_unsized_integer_primary_keys_alias_the_rowid() {
        for (declaration, rowid_alias) in [
            ("INTEGER", true),
            ("\"INTEGER\"", true),
            ("INT", false),
            ("INTEGER(8)", false),
            ("UNSIGNED BIG INT", false),
        ] {
            let not_null = if rowid_alias { "" } else { " NOT NULL" };
            let sql =
                format!("CREATE TABLE items (id {declaration}{not_null} PRIMARY KEY, body TEXT)");
            let ValidatedExecute::CreateTable(spec) = validate_execute(&sql).unwrap() else {
                unreachable!()
            };
            assert_eq!(
                spec.columns[0].declared_type.is_exact_integer(),
                rowid_alias,
                "{declaration}"
            );
            assert_eq!(spec.columns[0].declared_type.affinity(), Affinity::Integer);
        }
    }

    #[test]
    fn type_sizes_must_be_literal_numbers() {
        for sql in [
            "CREATE TABLE notes (id INTEGER PRIMARY KEY, body VARCHAR(length('x')))",
            "CREATE TABLE notes (id INTEGER PRIMARY KEY, amount DECIMAL(10, ?1))",
            "CREATE TABLE notes (id INTEGER PRIMARY KEY, amount DECIMAL('10', 2))",
        ] {
            assert_unsupported(sql);
        }
    }

    #[test]
    fn rejects_delete_extensions_outside_the_initial_slice() {
        for sql in [
            "WITH old AS (SELECT 1) DELETE FROM notes WHERE id IN old",
            "DELETE FROM main.notes",
            "DELETE FROM notes AS old",
            "DELETE FROM notes INDEXED BY notes_id",
            "DELETE FROM notes NOT INDEXED",
            "DELETE FROM notes RETURNING id",
            "DELETE FROM notes ORDER BY id LIMIT 1",
            "DELETE FROM __multilite__pending",
        ] {
            assert_unsupported(sql);
        }
    }

    #[test]
    fn rejects_update_extensions_outside_the_initial_slice() {
        for sql in [
            "WITH old AS (SELECT 1) UPDATE notes SET body = 'x'",
            "UPDATE OR REPLACE notes SET body = 'x'",
            "UPDATE main.notes SET body = 'x'",
            "UPDATE notes AS old SET body = 'x'",
            "UPDATE notes INDEXED BY notes_id SET body = 'x'",
            "UPDATE notes NOT INDEXED SET body = 'x'",
            "UPDATE notes SET (body, payload) = ('x', x'00')",
            "UPDATE notes SET body = archived.body FROM archived WHERE archived.id = notes.id",
            "UPDATE notes SET body = 'x' RETURNING id",
            "UPDATE notes SET body = 'x' ORDER BY id LIMIT 1",
            "UPDATE __multilite__pending SET record = x''",
        ] {
            assert_unsupported(sql);
        }
    }

    #[test]
    fn rejects_every_other_statement_shape() {
        for sql in [
            "",
            "SELECT 1",
            "ALTER TABLE notes ADD COLUMN body TEXT",
            "CREATE UNIQUE INDEX notes_body ON notes (body)",
            "DROP INDEX notes_body",
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
    fn managed_reads_rewrite_one_source_and_leave_constant_selects_direct() {
        let rewritten = rewrite_managed_read(
            "SELECT count(*) FROM notes WHERE day = ?1 ORDER BY id",
            |_| Ok("__multilite__source_test".into()),
        )
        .unwrap()
        .unwrap();
        assert!(rewritten.contains("__multilite__source_test"));
        assert!(rewritten.contains("notes"));
        assert!(
            rewrite_managed_read("SELECT 1", |_| unreachable!())
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn managed_reads_reject_sources_outside_the_initial_sql_slice() {
        for sql in [
            "SELECT * FROM notes JOIN tasks USING (id)",
            "SELECT * FROM (SELECT * FROM notes)",
            "SELECT * FROM notes WHERE id = 1 OR id = 2",
            "SELECT EXISTS(SELECT 1 FROM notes)",
            "WITH values AS (SELECT 1) SELECT * FROM values",
            "SELECT * FROM __multilite__vtab",
        ] {
            assert!(
                matches!(
                    rewrite_managed_read(sql, |_| Ok("__multilite__source_test".into())),
                    Err(Error::UnsupportedSql(_))
                ),
                "managed read was accepted: {sql}"
            );
        }
    }
}
