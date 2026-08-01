//! SQLite AST front end for the current public logical-operation compiler.

use fallible_iterator::FallibleIterator as _;
use sqlite3_parser::ast::{
    AlterTableBody, Cmd, ColumnConstraint, CreateTableBody, Expr, ForeignKeyClause, FrameBound,
    FromClause, FunctionCallOrder, FunctionTail, Indexed, IndexedColumn, InsertBody,
    JoinConstraint, Literal, Name, OneSelect, Over, RefAct, RefArg, ResultColumn, Select,
    SelectTable, SortedColumn, Stmt, TabFlags, TableConstraint, Type, TypeSize, UnaryOperator,
    Window, With,
};
use sqlite3_parser::lexer::sql::Parser;

use crate::logical::schema::{
    CreateCheckConstraint, CreateColumn, CreateForeignKey, CreateTableSpec, CreateUnique,
    DefaultDefinition, IndexOrder, MAX_INDEX_COLUMNS, SqlExpression, SqlName, TableMode,
    TableStorage, TypeDeclaration,
};
use crate::{Error, Result};

pub(crate) fn is_sqlite_internal_table(table: &str) -> bool {
    table
        .get(.."sqlite_".len())
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("sqlite_"))
}

pub(crate) fn has_multilite_prefix(table: &str) -> bool {
    table
        .get(.."__multilite__".len())
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("__multilite__"))
}

#[derive(Clone)]
pub enum ValidatedExecute {
    RenameTable(RenameTableSpec),
    RenameColumn(RenameColumnSpec),
    AddColumn(AddColumnSpec),
    DropColumn(DropColumnSpec),
    CreateTable(CreateTableSpec),
    CreateIndex(CreateIndexSpec),
    DropIndex(DropIndexSpec),
    Insert,
    Delete,
    Update,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RenameTableSpec {
    pub table: SqlName,
    pub new_name: SqlName,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RenameColumnSpec {
    pub table: SqlName,
    pub old_name: SqlName,
    pub new_name: SqlName,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AddColumnSpec {
    pub table: SqlName,
    pub column: CreateColumn,
    pub checks: Vec<CreateCheckConstraint>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DropColumnSpec {
    pub table: SqlName,
    pub column: SqlName,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateIndexSpec {
    pub unique: bool,
    pub name: SqlName,
    pub table: SqlName,
    pub terms: Vec<CreateIndexTerm>,
    pub predicate: Option<SqlExpression>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CreateIndexTerm {
    Column {
        name: SqlName,
        collation: Option<SqlName>,
        order: Option<IndexOrder>,
    },
    Expression {
        expression: SqlExpression,
        order: Option<IndexOrder>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DropIndexSpec {
    pub name: SqlName,
}

pub enum ValidatedStatement {
    Read,
    Execute(Box<ValidatedExecute>),
}

pub fn validate_execute(sql: &str) -> Result<ValidatedExecute> {
    validate_execute_statement(parse_one(sql)?)
}

fn validate_execute_statement(statement: Stmt) -> Result<ValidatedExecute> {
    match statement {
        Stmt::AlterTable(table, AlterTableBody::RenameTo(new_name)) => {
            if table.db_name.is_some() || table.alias.is_some() {
                return Err(Error::UnsupportedSql(
                    "qualified ALTER TABLE names are not supported",
                ));
            }
            let table = identifier(&table.name)?;
            let new_name = identifier(&new_name)?;
            if has_multilite_prefix(table.value())
                || is_sqlite_internal_table(table.value())
                || has_multilite_prefix(new_name.value())
                || is_sqlite_internal_table(new_name.value())
            {
                return Err(Error::UnsupportedSql(
                    "reserved SQLite and Multilite table names are not supported",
                ));
            }
            if table.canonical() == new_name.canonical() {
                return Err(Error::UnsupportedSql(
                    "ALTER TABLE RENAME TO must change the table's case-insensitive identity",
                ));
            }
            Ok(ValidatedExecute::RenameTable(RenameTableSpec {
                table,
                new_name,
            }))
        }
        Stmt::AlterTable(table, AlterTableBody::RenameColumn { old, new }) => {
            if table.db_name.is_some() || table.alias.is_some() {
                return Err(Error::UnsupportedSql(
                    "qualified ALTER TABLE names are not supported",
                ));
            }
            let table = identifier(&table.name)?;
            let old_name = identifier(&old)?;
            let new_name = identifier(&new)?;
            if has_multilite_prefix(table.value()) || is_sqlite_internal_table(table.value()) {
                return Err(Error::UnsupportedSql(
                    "reserved SQLite and Multilite table names are not supported",
                ));
            }
            if old_name.canonical() == new_name.canonical() {
                return Err(Error::UnsupportedSql(
                    "ALTER TABLE RENAME COLUMN must change the column's case-insensitive identity",
                ));
            }
            Ok(ValidatedExecute::RenameColumn(RenameColumnSpec {
                table,
                old_name,
                new_name,
            }))
        }
        Stmt::AlterTable(table, AlterTableBody::AddColumn(column)) => {
            if table.db_name.is_some() || table.alias.is_some() {
                return Err(Error::UnsupportedSql(
                    "qualified ALTER TABLE names are not supported",
                ));
            }
            let table = identifier(&table.name)?;
            if has_multilite_prefix(table.value()) || is_sqlite_internal_table(table.value()) {
                return Err(Error::UnsupportedSql(
                    "reserved SQLite and Multilite table names are not supported",
                ));
            }
            let name = identifier(&column.col_name)?;
            let declared_type = column
                .col_type
                .ok_or(Error::UnsupportedSql("every column must declare a type"))
                .and_then(type_declaration)?;
            let mut not_null = false;
            let mut not_null_name = None;
            let mut default = None;
            let mut checks = Vec::new();
            for constraint in column.constraints {
                let constraint_name = constraint.name.map(|name| identifier(&name)).transpose()?;
                match constraint.constraint {
                    ColumnConstraint::NotNull {
                        nullable: false,
                        conflict_clause: None,
                    } if !not_null => {
                        not_null = true;
                        not_null_name = constraint_name;
                    }
                    ColumnConstraint::Default(expression) if default.is_none() => {
                        default = Some(DefaultDefinition {
                            name: constraint_name,
                            expression: schema_expression(expression),
                        });
                    }
                    ColumnConstraint::Check(expression) => {
                        checks.push(CreateCheckConstraint {
                            column: Some(name.clone()),
                            name: constraint_name,
                            expression: schema_expression(expression),
                        });
                    }
                    ColumnConstraint::NotNull { .. } => {
                        return Err(Error::UnsupportedSql(
                            "duplicate, nullable, and conflict-clause NOT NULL forms are not supported",
                        ));
                    }
                    ColumnConstraint::Default(_) => {
                        return Err(Error::UnsupportedSql(
                            "duplicate DEFAULT constraints are not supported",
                        ));
                    }
                    _ => {
                        return Err(Error::UnsupportedSql(
                            "ADD COLUMN supports NOT NULL, DEFAULT, and CHECK constraints",
                        ));
                    }
                }
            }
            Ok(ValidatedExecute::AddColumn(AddColumnSpec {
                table,
                column: CreateColumn {
                    name,
                    declared_type,
                    not_null,
                    not_null_name,
                    default,
                    primary_key: None,
                },
                checks,
            }))
        }
        Stmt::AlterTable(table, AlterTableBody::DropColumn(column)) => {
            if table.db_name.is_some() || table.alias.is_some() {
                return Err(Error::UnsupportedSql(
                    "qualified ALTER TABLE names are not supported",
                ));
            }
            let table = identifier(&table.name)?;
            let column = identifier(&column)?;
            if has_multilite_prefix(table.value()) || is_sqlite_internal_table(table.value()) {
                return Err(Error::UnsupportedSql(
                    "reserved SQLite and Multilite table names are not supported",
                ));
            }
            Ok(ValidatedExecute::DropColumn(DropColumnSpec {
                table,
                column,
            }))
        }
        Stmt::AlterTable(..) => Err(Error::UnsupportedSql(
            "this ALTER TABLE form is not supported",
        )),
        Stmt::CreateIndex {
            unique,
            if_not_exists,
            idx_name,
            tbl_name,
            columns,
            where_clause,
        } => {
            if if_not_exists {
                return Err(Error::UnsupportedSql(
                    "CREATE INDEX IF NOT EXISTS is not supported",
                ));
            }
            if idx_name.db_name.is_some() || idx_name.alias.is_some() {
                return Err(Error::UnsupportedSql(
                    "qualified CREATE INDEX names are not supported",
                ));
            }
            if columns.is_empty() || columns.len() > MAX_INDEX_COLUMNS {
                return Err(Error::UnsupportedSql(
                    "index has an unsupported number of terms",
                ));
            }
            let name = identifier(&idx_name.name)?;
            let table = identifier(&tbl_name)?;
            if has_multilite_prefix(name.value())
                || is_sqlite_internal_table(name.value())
                || has_multilite_prefix(table.value())
                || is_sqlite_internal_table(table.value())
            {
                return Err(Error::UnsupportedSql(
                    "reserved SQLite and Multilite names are not supported",
                ));
            }
            let mut terms = Vec::with_capacity(columns.len());
            for column in columns {
                if column.nulls.is_some() {
                    return Err(Error::UnsupportedSql(
                        "index NULLS FIRST and NULLS LAST clauses are not supported",
                    ));
                }
                terms.push(create_index_term(column.expr, column.order)?);
            }
            let predicate = where_clause
                .map(|expression| {
                    validate_index_expression(&expression)?;
                    Ok::<_, Error>(schema_expression(expression))
                })
                .transpose()?;
            if unique {
                if predicate.is_some()
                    || terms.iter().any(|term| {
                        !matches!(
                            term,
                            CreateIndexTerm::Column {
                                collation: None,
                                order: None,
                                ..
                            }
                        )
                    })
                {
                    return Err(Error::UnsupportedSql(
                        "UNIQUE index expressions, collations, ordering, and predicates are not supported",
                    ));
                }
                let columns = terms
                    .iter()
                    .map(|term| match term {
                        CreateIndexTerm::Column { name, .. } => name,
                        CreateIndexTerm::Expression { .. } => unreachable!(),
                    })
                    .collect::<Vec<_>>();
                if columns.iter().enumerate().any(|(index, column)| {
                    columns[..index]
                        .iter()
                        .any(|seen| seen.canonical() == column.canonical())
                }) {
                    return Err(Error::UnsupportedSql(
                        "UNIQUE index columns must be distinct",
                    ));
                }
            }
            Ok(ValidatedExecute::CreateIndex(CreateIndexSpec {
                unique,
                name,
                table,
                terms,
                predicate,
            }))
        }
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
        Stmt::DropIndex {
            if_exists,
            idx_name,
        } => {
            if if_exists {
                return Err(Error::UnsupportedSql(
                    "DROP INDEX IF EXISTS is not supported",
                ));
            }
            if idx_name.db_name.is_some() || idx_name.alias.is_some() {
                return Err(Error::UnsupportedSql(
                    "qualified DROP INDEX names are not supported",
                ));
            }
            let name = identifier(&idx_name.name)?;
            if has_multilite_prefix(name.value()) || is_sqlite_internal_table(name.value()) {
                return Err(Error::UnsupportedSql(
                    "reserved SQLite and Multilite index names are not supported",
                ));
            }
            Ok(ValidatedExecute::DropIndex(DropIndexSpec { name }))
        }
        Stmt::Insert {
            with,
            or_conflict,
            tbl_name,
            columns: _,
            body,
            returning,
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
            if tbl_name.db_name.is_some() || tbl_name.alias.is_some() {
                return Err(Error::UnsupportedSql(
                    "qualified and aliased INSERT targets are not supported",
                ));
            }
            let table = identifier(&tbl_name.name)?;
            if has_multilite_prefix(table.value()) || is_sqlite_internal_table(table.value()) {
                return Err(Error::UnsupportedSql(
                    "reserved SQLite and Multilite table names are not supported",
                ));
            }
            let reads_reserved = with.as_ref().is_some_and(|with| {
                with.ctes
                    .iter()
                    .any(|cte| select_reads_reserved(&cte.select))
            }) || match &body {
                InsertBody::Select(select, _) => select_reads_reserved(select),
                InsertBody::DefaultValues => false,
            };
            if reads_reserved {
                return Err(Error::UnsupportedSql(
                    "INSERT cannot read reserved Multilite tables",
                ));
            }
            Ok(ValidatedExecute::Insert)
        }
        Stmt::Delete {
            with,
            tbl_name,
            indexed,
            where_clause,
            returning,
            order_by,
            limit,
        } => {
            if tbl_name.db_name.is_some() || tbl_name.alias.is_some() {
                return Err(Error::UnsupportedSql(
                    "qualified and aliased DELETE targets are not supported",
                ));
            }
            let table = identifier(&tbl_name.name)?;
            if has_multilite_prefix(table.value()) || is_sqlite_internal_table(table.value()) {
                return Err(Error::UnsupportedSql(
                    "reserved SQLite and Multilite table names are not supported",
                ));
            }
            validate_index_hint(indexed.as_ref())?;
            if returning.is_some() {
                return Err(Error::UnsupportedSql("DELETE RETURNING is not supported"));
            }
            if order_by.is_some() || limit.is_some() {
                return Err(Error::UnsupportedSql(
                    "DELETE ORDER BY and LIMIT are not supported",
                ));
            }
            if with_reads_reserved(with.as_ref())
                || where_clause.as_ref().is_some_and(expression_reads_reserved)
            {
                return Err(Error::UnsupportedSql(
                    "DELETE cannot read reserved Multilite tables",
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
            where_clause,
            returning,
            order_by,
            limit,
        } => {
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
            if has_multilite_prefix(table.value()) || is_sqlite_internal_table(table.value()) {
                return Err(Error::UnsupportedSql(
                    "reserved SQLite and Multilite table names are not supported",
                ));
            }
            validate_index_hint(indexed.as_ref())?;
            if returning.is_some() {
                return Err(Error::UnsupportedSql("UPDATE RETURNING is not supported"));
            }
            if order_by.is_some() || limit.is_some() {
                return Err(Error::UnsupportedSql(
                    "UPDATE ORDER BY and LIMIT are not supported",
                ));
            }
            if with_reads_reserved(with.as_ref())
                || sets.iter().any(|set| expression_reads_reserved(&set.expr))
                || from.as_ref().is_some_and(from_reads_reserved)
                || where_clause.as_ref().is_some_and(expression_reads_reserved)
            {
                return Err(Error::UnsupportedSql(
                    "UPDATE cannot read reserved Multilite tables",
                ));
            }
            Ok(ValidatedExecute::Update)
        }
        _ => Err(Error::UnsupportedSql(
            "execute accepts only supported schema changes, INSERT, DELETE, and UPDATE",
        )),
    }
}

/// Classify one public prepared statement with a single AST parse.
pub fn validate_statement(sql: &str) -> Result<ValidatedStatement> {
    let command = parse_one_command(sql)?;
    match command {
        Cmd::Stmt(Stmt::Select(select))
        | Cmd::Explain(Stmt::Select(select))
        | Cmd::ExplainQueryPlan(Stmt::Select(select)) => {
            if select_reads_reserved(&select) {
                Err(Error::UnsupportedSql(
                    "reserved Multilite tables are not readable",
                ))
            } else {
                Ok(ValidatedStatement::Read)
            }
        }
        Cmd::Stmt(Stmt::Pragma(_, None)) => Ok(ValidatedStatement::Read),
        Cmd::Stmt(
            Stmt::Begin(..)
            | Stmt::Commit(..)
            | Stmt::Rollback { .. }
            | Stmt::Savepoint(..)
            | Stmt::Release(..),
        ) => Err(Error::UnsupportedSql(
            "transaction control is owned by the managed closure",
        )),
        Cmd::Stmt(statement) => validate_execute_statement(statement)
            .map(Box::new)
            .map(ValidatedStatement::Execute),
        Cmd::Explain(_) | Cmd::ExplainQueryPlan(_) => Err(Error::UnsupportedSql(
            "EXPLAIN is supported only for SELECT",
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

/// Validate one statement intended for the public read-only surface.
pub fn validate_read_statement(sql: &str) -> Result<()> {
    let command = parse_one_command(sql)?;
    match command {
        Cmd::Stmt(Stmt::Select(select))
        | Cmd::Explain(Stmt::Select(select))
        | Cmd::ExplainQueryPlan(Stmt::Select(select))
            if !select_reads_reserved(&select) =>
        {
            Ok(())
        }
        Cmd::Stmt(Stmt::Pragma(_, None)) => Ok(()),
        Cmd::Stmt(
            Stmt::Begin(..)
            | Stmt::Commit(..)
            | Stmt::Rollback { .. }
            | Stmt::Savepoint(..)
            | Stmt::Release(..),
        ) => Err(Error::UnsupportedSql(
            "transaction control is owned by the managed closure",
        )),
        _ => Err(Error::PreparedWrite),
    }
}

fn select_reads_reserved(select: &Select) -> bool {
    with_reads_reserved(select.with.as_ref())
        || one_select_reads_reserved(&select.body.select)
        || select
            .body
            .compounds
            .iter()
            .flatten()
            .any(|compound| one_select_reads_reserved(&compound.select))
        || select
            .order_by
            .iter()
            .flatten()
            .any(sorted_column_reads_reserved)
        || select.limit.as_ref().is_some_and(|limit| {
            expression_reads_reserved(&limit.expr)
                || limit.offset.as_ref().is_some_and(expression_reads_reserved)
        })
}

fn one_select_reads_reserved(select: &OneSelect) -> bool {
    match select {
        OneSelect::Select {
            columns,
            from,
            where_clause,
            group_by,
            having,
            window_clause,
            ..
        } => {
            columns.iter().any(|column| match column {
                ResultColumn::Expr(expression, _) => expression_reads_reserved(expression),
                ResultColumn::Star | ResultColumn::TableStar(_) => false,
            }) || from.as_ref().is_some_and(from_reads_reserved)
                || where_clause
                    .as_ref()
                    .is_some_and(|expression| expression_reads_reserved(expression))
                || group_by.iter().flatten().any(expression_reads_reserved)
                || having
                    .as_ref()
                    .is_some_and(|expression| expression_reads_reserved(expression))
                || window_clause
                    .iter()
                    .flatten()
                    .any(|definition| window_reads_reserved(&definition.window))
        }
        OneSelect::Values(rows) => rows.iter().flatten().any(expression_reads_reserved),
    }
}

fn with_reads_reserved(with: Option<&With>) -> bool {
    with.iter()
        .flat_map(|with| &with.ctes)
        .any(|cte| select_reads_reserved(&cte.select))
}

fn from_reads_reserved(from: &FromClause) -> bool {
    from.select
        .iter()
        .any(|table| select_table_reads_reserved(table))
        || from.joins.iter().flatten().any(|join| {
            select_table_reads_reserved(&join.table)
                || matches!(
                    &join.constraint,
                    Some(JoinConstraint::On(expression))
                        if expression_reads_reserved(expression)
                )
        })
}

fn validate_index_hint(indexed: Option<&Indexed>) -> Result<()> {
    let Some(Indexed::IndexedBy(name)) = indexed else {
        return Ok(());
    };
    let name = identifier(name)?;
    if has_multilite_prefix(name.value()) || is_sqlite_internal_table(name.value()) {
        return Err(Error::UnsupportedSql(
            "reserved SQLite and Multilite index names are not supported",
        ));
    }
    Ok(())
}

fn select_table_reads_reserved(table: &SelectTable) -> bool {
    match table {
        SelectTable::Table(name, ..) => name_reads_reserved(&name.name),
        SelectTable::TableCall(name, arguments, ..) => {
            name_reads_reserved(&name.name)
                || arguments.iter().flatten().any(expression_reads_reserved)
        }
        SelectTable::Select(select, ..) => select_reads_reserved(select),
        SelectTable::Sub(from, ..) => {
            from.select
                .iter()
                .any(|table| select_table_reads_reserved(table))
                || from.joins.iter().flatten().any(|join| {
                    select_table_reads_reserved(&join.table)
                        || matches!(
                            &join.constraint,
                            Some(JoinConstraint::On(expression))
                                if expression_reads_reserved(expression)
                        )
                })
        }
    }
}

fn expression_reads_reserved(expression: &Expr) -> bool {
    match expression {
        Expr::Between {
            lhs, start, end, ..
        } => {
            expression_reads_reserved(lhs)
                || expression_reads_reserved(start)
                || expression_reads_reserved(end)
        }
        Expr::Binary(left, _, right) => {
            expression_reads_reserved(left) || expression_reads_reserved(right)
        }
        Expr::Case {
            base,
            when_then_pairs,
            else_expr,
        } => {
            base.as_ref()
                .is_some_and(|expression| expression_reads_reserved(expression))
                || when_then_pairs.iter().any(|(when, then)| {
                    expression_reads_reserved(when) || expression_reads_reserved(then)
                })
                || else_expr
                    .as_ref()
                    .is_some_and(|expression| expression_reads_reserved(expression))
        }
        Expr::Cast { expr, .. }
        | Expr::Collate(expr, _)
        | Expr::IsNull(expr)
        | Expr::NotNull(expr)
        | Expr::Unary(_, expr) => expression_reads_reserved(expr),
        Expr::Exists(select) | Expr::Subquery(select) => select_reads_reserved(select),
        Expr::FunctionCall {
            args,
            order_by,
            filter_over,
            ..
        } => {
            args.iter().flatten().any(expression_reads_reserved)
                || order_by.as_ref().is_some_and(function_order_reads_reserved)
                || filter_over
                    .as_ref()
                    .is_some_and(function_tail_reads_reserved)
        }
        Expr::FunctionCallStar { filter_over, .. } => filter_over
            .as_ref()
            .is_some_and(function_tail_reads_reserved),
        Expr::InList { lhs, rhs, .. } => {
            expression_reads_reserved(lhs) || rhs.iter().flatten().any(expression_reads_reserved)
        }
        Expr::InSelect { lhs, rhs, .. } => {
            expression_reads_reserved(lhs) || select_reads_reserved(rhs)
        }
        Expr::InTable { lhs, rhs, args, .. } => {
            expression_reads_reserved(lhs)
                || name_reads_reserved(&rhs.name)
                || args.iter().flatten().any(expression_reads_reserved)
        }
        Expr::Like {
            lhs, rhs, escape, ..
        } => {
            expression_reads_reserved(lhs)
                || expression_reads_reserved(rhs)
                || escape
                    .as_ref()
                    .is_some_and(|expression| expression_reads_reserved(expression))
        }
        Expr::Parenthesized(expressions) => expressions.iter().any(expression_reads_reserved),
        Expr::Raise(_, expression) => expression
            .as_ref()
            .is_some_and(|expression| expression_reads_reserved(expression)),
        Expr::DoublyQualified(..)
        | Expr::Id(_)
        | Expr::Literal(_)
        | Expr::Name(_)
        | Expr::Qualified(..)
        | Expr::Variable(_) => false,
    }
}

fn name_reads_reserved(name: &Name) -> bool {
    SqlName::from_sqlite_token(&name.0)
        .map(|name| has_multilite_prefix(name.value()))
        // The parser should only produce valid SQLite identifier tokens. If a
        // future parser shape violates that contract, reject conservatively.
        .unwrap_or(true)
}

fn sorted_column_reads_reserved(column: &SortedColumn) -> bool {
    expression_reads_reserved(&column.expr)
}

fn function_order_reads_reserved(order: &FunctionCallOrder) -> bool {
    match order {
        FunctionCallOrder::SortList(columns) => columns.iter().any(sorted_column_reads_reserved),
    }
}

fn function_tail_reads_reserved(tail: &FunctionTail) -> bool {
    tail.filter_clause
        .as_ref()
        .is_some_and(|expression| expression_reads_reserved(expression))
        || tail.over_clause.as_ref().is_some_and(|over| match &**over {
            Over::Window(window) => window_reads_reserved(window),
            Over::Name(_) => false,
        })
}

fn window_reads_reserved(window: &Window) -> bool {
    window
        .partition_by
        .iter()
        .flatten()
        .any(expression_reads_reserved)
        || window
            .order_by
            .iter()
            .flatten()
            .any(sorted_column_reads_reserved)
        || window.frame_clause.as_ref().is_some_and(|frame| {
            frame_bound_reads_reserved(&frame.start)
                || frame.end.as_ref().is_some_and(frame_bound_reads_reserved)
        })
}

fn frame_bound_reads_reserved(bound: &FrameBound) -> bool {
    match bound {
        FrameBound::Following(expression) | FrameBound::Preceding(expression) => {
            expression_reads_reserved(expression)
        }
        FrameBound::CurrentRow
        | FrameBound::UnboundedFollowing
        | FrameBound::UnboundedPreceding => false,
    }
}

/// Render an immutable ALTER TABLE statement for its owner's current name.
pub(super) fn render_alter_table(sql: &str, table: &SqlName) -> Result<String> {
    let mut statement = parse_one(sql)?;
    let Stmt::AlterTable(name, _) = &mut statement else {
        return Err(Error::InvalidMultiliteOp(
            "table alteration is not ALTER TABLE".into(),
        ));
    };
    name.db_name = None;
    name.name = quoted_name(table);
    name.alias = None;
    Ok(Cmd::Stmt(statement).to_string())
}

/// Render one complete CREATE TABLE definition under a temporary owner name.
pub(super) fn render_create_table_name(sql: &str, table: &SqlName) -> Result<String> {
    let mut statement = parse_one(sql)?;
    let Stmt::CreateTable { tbl_name, body, .. } = &mut statement else {
        return Err(Error::InvalidDatabase(
            "materialized table definition is not CREATE TABLE",
        ));
    };
    if !matches!(body, CreateTableBody::ColumnsAndConstraints { .. }) {
        return Err(Error::InvalidDatabase(
            "materialized table has no column definitions",
        ));
    }
    tbl_name.db_name = None;
    tbl_name.name = quoted_name(table);
    tbl_name.alias = None;
    Ok(Cmd::Stmt(statement).to_string())
}

/// Render immutable CREATE TABLE provenance with current foreign-parent names.
#[cfg(test)]
pub(super) fn render_create_table(sql: &str, parents: &[(SqlName, SqlName)]) -> Result<String> {
    if parents
        .iter()
        .all(|(source, current)| source.canonical() == current.canonical())
    {
        return Ok(sql.to_owned());
    }
    let mut statement = parse_one(sql)?;
    let Stmt::CreateTable { body, .. } = &mut statement else {
        return Err(Error::InvalidMultiliteOp(
            "table definition is not CREATE TABLE".into(),
        ));
    };
    let CreateTableBody::ColumnsAndConstraints {
        columns,
        constraints,
        ..
    } = body
    else {
        return Err(Error::InvalidMultiliteOp(
            "foreign-key table has no structural definition".into(),
        ));
    };

    for column in columns.values_mut() {
        for constraint in &mut column.constraints {
            if let ColumnConstraint::ForeignKey { clause, .. } = &mut constraint.constraint {
                rewrite_parent_clause(clause, parents)?;
            }
        }
    }
    for constraint in constraints.iter_mut().flatten() {
        if let TableConstraint::ForeignKey { clause, .. } = &mut constraint.constraint {
            rewrite_parent_clause(clause, parents)?;
        }
    }
    Ok(Cmd::Stmt(statement).to_string())
}

#[cfg(test)]
fn rewrite_parent_clause(
    clause: &mut ForeignKeyClause,
    parents: &[(SqlName, SqlName)],
) -> Result<()> {
    let original = identifier(&clause.tbl_name)?;
    let replacement = parents
        .iter()
        .find(|(source, _)| source.canonical() == original.canonical())
        .map(|(_, replacement)| replacement)
        .ok_or_else(|| {
            Error::InvalidMultiliteOp(
                "foreign-key SQL has no matching stable parent identity".into(),
            )
        })?;
    clause.tbl_name = quoted_name(replacement);
    Ok(())
}

fn quoted_name(name: &SqlName) -> Name {
    Name(format!("\"{}\"", name.value().replace('"', "\"\"")).into())
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
    if has_multilite_prefix(name.value()) {
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
    let mode = if flags.contains(TabFlags::Strict) {
        TableMode::Strict
    } else {
        TableMode::Ordinary
    };
    let storage = if flags.contains(TabFlags::WithoutRowid) {
        TableStorage::WithoutRowid
    } else {
        TableStorage::Rowid
    };
    let mut inline_primary_keys = 0;
    let mut primary_key_name = None;
    let mut unique_constraints = Vec::new();
    let mut foreign_keys = Vec::new();
    let mut checks = Vec::new();
    let mut columns = columns
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
            let mut not_null_name = None;
            let mut primary_key = None;
            let mut default = None;
            for constraint in column.constraints {
                let constraint_name = constraint
                    .name
                    .map(|name| identifier(&name))
                    .transpose()?;
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
                        if primary_key.is_some() {
                            return Err(Error::UnsupportedSql(
                                "duplicate PRIMARY KEY constraints are not supported",
                            ));
                        }
                        primary_key = Some(0);
                        inline_primary_keys += 1;
                        if let Some(name) = constraint_name {
                            primary_key_name = Some(name);
                        }
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
                        not_null_name = constraint_name;
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
                            name: constraint_name,
                            columns: vec![name.clone()],
                        });
                    }
                    ColumnConstraint::Default(expression) => {
                        if default.is_some() {
                            return Err(Error::UnsupportedSql(
                                "duplicate DEFAULT constraints are not supported",
                            ));
                        }
                        default = Some(DefaultDefinition {
                            name: constraint_name,
                            expression: schema_expression(expression),
                        });
                    }
                    ColumnConstraint::Check(expression) => {
                        checks.push(CreateCheckConstraint {
                            column: Some(name.clone()),
                            name: constraint_name,
                            expression: schema_expression(expression),
                        });
                    }
                    ColumnConstraint::ForeignKey {
                        clause,
                        defer_clause,
                    } => {
                        foreign_keys.push(validate_foreign_key(
                            constraint_name,
                            vec![name.clone()],
                            clause,
                            defer_clause,
                        )?);
                    }
                    _ => {
                        return Err(Error::UnsupportedSql(
                            "only PRIMARY KEY, NOT NULL, UNIQUE, DEFAULT, CHECK, and REFERENCES column constraints are supported",
                        ));
                    }
                }
            }
            Ok(CreateColumn {
                name,
                declared_type,
                not_null,
                not_null_name,
                default,
                primary_key,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let mut table_primary_key = None;
    if columns.iter().enumerate().any(|(index, column)| {
        columns[..index]
            .iter()
            .any(|seen| seen.name.canonical() == column.name.canonical())
    }) {
        return Err(Error::UnsupportedSql(
            "duplicate column names are not supported",
        ));
    }
    for constraint in constraints.into_iter().flatten() {
        match constraint.constraint {
            TableConstraint::Unique {
                columns: unique_columns,
                conflict_clause,
            } => {
                if conflict_clause.is_some() {
                    return Err(Error::UnsupportedSql(
                        "UNIQUE conflict clauses are not supported",
                    ));
                }
                let unique_columns = simple_key_columns(unique_columns, &columns, "UNIQUE")?;
                unique_constraints.push(CreateUnique {
                    name: constraint.name.map(|name| identifier(&name)).transpose()?,
                    columns: unique_columns,
                });
            }
            TableConstraint::PrimaryKey {
                columns: primary_columns,
                auto_increment,
                conflict_clause,
            } => {
                let constraint_name = constraint.name.map(|name| identifier(&name)).transpose()?;
                if auto_increment {
                    return Err(Error::UnsupportedSql("AUTOINCREMENT is not supported"));
                }
                if conflict_clause.is_some() {
                    return Err(Error::UnsupportedSql(
                        "PRIMARY KEY conflict clauses are not supported",
                    ));
                }
                if table_primary_key.is_some() {
                    return Err(Error::UnsupportedSql(
                        "duplicate PRIMARY KEY constraints are not supported",
                    ));
                }
                table_primary_key = Some(simple_key_columns(
                    primary_columns,
                    &columns,
                    "PRIMARY KEY",
                )?);
                primary_key_name = constraint_name;
            }
            TableConstraint::Check(expression, conflict_clause) => {
                if conflict_clause.is_some() {
                    return Err(Error::UnsupportedSql(
                        "CHECK conflict clauses are not supported",
                    ));
                }
                checks.push(CreateCheckConstraint {
                    column: None,
                    name: constraint.name.map(|name| identifier(&name)).transpose()?,
                    expression: schema_expression(expression),
                });
            }
            TableConstraint::ForeignKey {
                columns: child_columns,
                clause,
                defer_clause,
            } => {
                let child_columns = simple_indexed_columns(child_columns, &columns, "FOREIGN KEY")?;
                foreign_keys.push(validate_foreign_key(
                    constraint.name.map(|name| identifier(&name)).transpose()?,
                    child_columns,
                    clause,
                    defer_clause,
                )?);
            }
        }
    }
    if inline_primary_keys > 1 || (inline_primary_keys != 0 && table_primary_key.is_some()) {
        return Err(Error::UnsupportedSql(
            "CREATE TABLE requires exactly one PRIMARY KEY",
        ));
    }
    if let Some(primary) = table_primary_key {
        for column in &mut columns {
            column.primary_key = primary
                .iter()
                .position(|name| name.canonical() == column.name.canonical());
        }
    }
    let primary = columns
        .iter()
        .enumerate()
        .filter_map(|(index, column)| column.primary_key.map(|position| (position, index)))
        .collect::<Vec<_>>();
    if primary.is_empty() {
        return Err(Error::UnsupportedSql(
            "CREATE TABLE requires exactly one PRIMARY KEY",
        ));
    }
    let rowid_alias = storage == TableStorage::Rowid
        && primary.len() == 1
        && columns[primary[0].1].declared_type.is_exact_integer();
    if storage == TableStorage::Rowid && !rowid_alias {
        return Err(Error::UnsupportedSql(
            "rowid tables require a single INTEGER PRIMARY KEY; other primary keys require WITHOUT ROWID",
        ));
    }
    for (_, index) in primary {
        if mode == TableMode::Strict || storage == TableStorage::WithoutRowid {
            columns[index].not_null = true;
        }
    }
    if checks.iter().any(|check| {
        check.expression.referenced_columns().iter().any(|name| {
            !columns
                .iter()
                .any(|column| column.name.canonical() == name.canonical())
        })
    }) {
        return Err(Error::UnsupportedSql(
            "CHECK constraints reference unknown table columns",
        ));
    }
    Ok(ValidatedExecute::CreateTable(CreateTableSpec {
        name,
        mode,
        storage,
        columns,
        primary_key_name,
        unique_constraints,
        foreign_keys,
        checks,
    }))
}

fn schema_expression(expression: Expr) -> SqlExpression {
    SqlExpression::new(expression)
}

fn create_index_term(
    expression: Expr,
    order: Option<sqlite3_parser::ast::SortOrder>,
) -> Result<CreateIndexTerm> {
    let order = order.map(|order| match order {
        sqlite3_parser::ast::SortOrder::Asc => IndexOrder::Asc,
        sqlite3_parser::ast::SortOrder::Desc => IndexOrder::Desc,
    });
    match expression {
        Expr::Id(id) => Ok(CreateIndexTerm::Column {
            name: identifier(&Name(id.0))?,
            collation: None,
            order,
        }),
        Expr::Name(name) => Ok(CreateIndexTerm::Column {
            name: identifier(&name)?,
            collation: None,
            order,
        }),
        Expr::Collate(expression, collation) => match *expression {
            Expr::Id(id) => Ok(CreateIndexTerm::Column {
                name: identifier(&Name(id.0))?,
                collation: Some(identifier(&Name(collation))?),
                order,
            }),
            Expr::Name(name) => Ok(CreateIndexTerm::Column {
                name: identifier(&name)?,
                collation: Some(identifier(&Name(collation))?),
                order,
            }),
            expression => {
                let expression = Expr::Collate(Box::new(expression), collation);
                validate_index_expression(&expression)?;
                Ok(CreateIndexTerm::Expression {
                    expression: schema_expression(expression),
                    order,
                })
            }
        },
        expression => {
            validate_index_expression(&expression)?;
            Ok(CreateIndexTerm::Expression {
                expression: schema_expression(expression),
                order,
            })
        }
    }
}

fn validate_index_expression(expression: &Expr) -> Result<()> {
    match expression {
        Expr::Between {
            lhs, start, end, ..
        } => {
            validate_index_expression(lhs)?;
            validate_index_expression(start)?;
            validate_index_expression(end)
        }
        Expr::Binary(left, _, right) => {
            validate_index_expression(left)?;
            validate_index_expression(right)
        }
        Expr::Case {
            base,
            when_then_pairs,
            else_expr,
        } => {
            if let Some(base) = base {
                validate_index_expression(base)?;
            }
            for (when, then) in when_then_pairs {
                validate_index_expression(when)?;
                validate_index_expression(then)?;
            }
            if let Some(expression) = else_expr {
                validate_index_expression(expression)?;
            }
            Ok(())
        }
        Expr::Cast { expr, .. }
        | Expr::Collate(expr, _)
        | Expr::IsNull(expr)
        | Expr::NotNull(expr)
        | Expr::Unary(_, expr) => validate_index_expression(expr),
        Expr::FunctionCall {
            distinctness,
            args,
            order_by,
            filter_over,
            ..
        } => {
            if distinctness.is_some() || order_by.is_some() || filter_over.is_some() {
                return Err(unsupported_index_expression());
            }
            for argument in args.iter().flatten() {
                validate_index_expression(argument)?;
            }
            Ok(())
        }
        Expr::InList { lhs, rhs, .. } => {
            validate_index_expression(lhs)?;
            for value in rhs.iter().flatten() {
                validate_index_expression(value)?;
            }
            Ok(())
        }
        Expr::Like {
            lhs, rhs, escape, ..
        } => {
            validate_index_expression(lhs)?;
            validate_index_expression(rhs)?;
            if let Some(escape) = escape {
                validate_index_expression(escape)?;
            }
            Ok(())
        }
        Expr::Parenthesized(expressions) => {
            for expression in expressions {
                validate_index_expression(expression)?;
            }
            Ok(())
        }
        Expr::Id(_) | Expr::Literal(_) | Expr::Name(_) => Ok(()),
        Expr::DoublyQualified(_, _, _)
        | Expr::Exists(_)
        | Expr::FunctionCallStar { .. }
        | Expr::InSelect { .. }
        | Expr::InTable { .. }
        | Expr::Qualified(_, _)
        | Expr::Raise(_, _)
        | Expr::Subquery(_)
        | Expr::Variable(_) => Err(unsupported_index_expression()),
    }
}

fn unsupported_index_expression() -> Error {
    Error::UnsupportedSql(
        "index expressions cannot contain parameters, subqueries, qualified columns, aggregates, or window clauses",
    )
}

pub(super) fn parse_schema_expression(sql: &str) -> Result<SqlExpression> {
    let statement = parse_one(&format!("SELECT {sql}"))?;
    let Stmt::Select(select) = statement else {
        return Err(Error::UnsupportedSql(
            "stored schema expression is not valid SQLite SQL",
        ));
    };
    if select.with.is_some()
        || select.body.compounds.is_some()
        || select.order_by.is_some()
        || select.limit.is_some()
    {
        return Err(Error::UnsupportedSql(
            "stored schema expression is not valid SQLite SQL",
        ));
    }
    let OneSelect::Select {
        distinctness: None,
        mut columns,
        from: None,
        where_clause: None,
        group_by: None,
        having: None,
        window_clause: None,
    } = select.body.select
    else {
        return Err(Error::UnsupportedSql(
            "stored schema expression is not valid SQLite SQL",
        ));
    };
    let [ResultColumn::Expr(expression, None)] = columns.as_mut_slice() else {
        return Err(Error::UnsupportedSql(
            "stored schema expression is not valid SQLite SQL",
        ));
    };
    validate_index_expression(expression)?;
    Ok(schema_expression(expression.clone()))
}

fn validate_foreign_key(
    name: Option<SqlName>,
    columns: Vec<SqlName>,
    clause: ForeignKeyClause,
    defer_clause: Option<sqlite3_parser::ast::DeferSubclause>,
) -> Result<CreateForeignKey> {
    if defer_clause.is_some() {
        return Err(Error::UnsupportedSql(
            "deferred foreign keys are not supported",
        ));
    }
    let mut on_delete = false;
    let mut on_update = false;
    for argument in clause.args {
        match argument {
            RefArg::OnDelete(RefAct::NoAction) if !on_delete => on_delete = true,
            RefArg::OnUpdate(RefAct::NoAction) if !on_update => on_update = true,
            RefArg::OnDelete(_) | RefArg::OnUpdate(_) => {
                return Err(Error::UnsupportedSql(
                    "foreign-key actions other than NO ACTION are not supported",
                ));
            }
            RefArg::OnInsert(_) | RefArg::Match(_) => {
                return Err(Error::UnsupportedSql(
                    "foreign-key MATCH and ON INSERT clauses are not supported",
                ));
            }
        }
    }
    let referenced_columns = clause
        .columns
        .map(|columns| simple_indexed_column_names(columns, "REFERENCES"))
        .transpose()?;
    if referenced_columns
        .as_ref()
        .is_some_and(|referenced| referenced.len() != columns.len())
    {
        return Err(Error::UnsupportedSql(
            "foreign-key child and parent column counts must match",
        ));
    }
    Ok(CreateForeignKey {
        name,
        columns,
        referenced_table: identifier(&clause.tbl_name)?,
        referenced_columns,
    })
}

fn simple_indexed_columns(
    columns: Vec<IndexedColumn>,
    table_columns: &[CreateColumn],
    kind: &'static str,
) -> Result<Vec<SqlName>> {
    let names = simple_indexed_column_names(columns, kind)?;
    if names.iter().any(|key| {
        !table_columns
            .iter()
            .any(|column| column.name.canonical() == key.canonical())
    }) {
        return Err(Error::UnsupportedSql(
            "FOREIGN KEY constraints reference unknown child columns",
        ));
    }
    Ok(names)
}

fn simple_indexed_column_names(
    columns: Vec<IndexedColumn>,
    kind: &'static str,
) -> Result<Vec<SqlName>> {
    let columns = columns
        .into_iter()
        .map(|column| {
            if column.collation_name.is_some() || column.order.is_some() {
                return Err(Error::UnsupportedSql(
                    "foreign-key collations and ordering are not supported",
                ));
            }
            identifier(&column.col_name)
        })
        .collect::<Result<Vec<_>>>()?;
    if columns.is_empty()
        || columns.len() > MAX_INDEX_COLUMNS
        || columns.iter().enumerate().any(|(index, column)| {
            columns[..index]
                .iter()
                .any(|seen| seen.canonical() == column.canonical())
        })
    {
        return Err(Error::UnsupportedSql(match kind {
            "REFERENCES" => "REFERENCES requires distinct parent columns",
            _ => "FOREIGN KEY requires distinct child columns",
        }));
    }
    Ok(columns)
}

fn simple_key_columns(
    columns: Vec<sqlite3_parser::ast::SortedColumn>,
    table_columns: &[CreateColumn],
    kind: &'static str,
) -> Result<Vec<SqlName>> {
    let columns = columns
        .into_iter()
        .map(|column| {
            if column.order.is_some() || column.nulls.is_some() {
                return Err(Error::UnsupportedSql(match kind {
                    "PRIMARY KEY" => "PRIMARY KEY ordering and NULLS placement are not supported",
                    _ => "UNIQUE ordering and NULLS placement are not supported",
                }));
            }
            expression_identifier(column.expr)
        })
        .collect::<Result<Vec<_>>>()?;
    if columns.len() > MAX_INDEX_COLUMNS {
        return Err(Error::UnsupportedSql(match kind {
            "PRIMARY KEY" => "PRIMARY KEY has too many columns",
            _ => "UNIQUE constraint has too many columns",
        }));
    }
    if columns.is_empty()
        || columns.iter().enumerate().any(|(index, column)| {
            columns[..index]
                .iter()
                .any(|seen| seen.canonical() == column.canonical())
        })
        || columns.iter().any(|key| {
            !table_columns
                .iter()
                .any(|column| column.name.canonical() == key.canonical())
        })
    {
        return Err(Error::UnsupportedSql(match kind {
            "PRIMARY KEY" => "PRIMARY KEY constraints require distinct table columns",
            _ => "UNIQUE constraints require distinct table columns",
        }));
    }
    Ok(columns)
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
    SqlName::from_sqlite_token(&name.0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logical::schema::Affinity;

    fn assert_unsupported(sql: &str) {
        assert!(
            matches!(validate_execute(sql), Err(Error::UnsupportedSql(_))),
            "statement was accepted: {sql}"
        );
    }

    #[test]
    fn accepts_restricted_create_insert_delete_and_update_forms() {
        for sql in [
            "ALTER TABLE notes RENAME TO archived_notes",
            "ALTER TABLE notes RENAME COLUMN body TO contents",
            "ALTER TABLE notes ADD COLUMN extra TEXT DEFAULT 'x'",
            "ALTER TABLE notes DROP COLUMN extra",
            "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT NOT NULL, payload BLOB)",
            "CREATE TABLE \"Case Sensitive\" (\"Primary Key\" TEXT NOT NULL PRIMARY KEY) WITHOUT ROWID",
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
            "CREATE UNIQUE INDEX notes_body ON notes (body)",
            "CREATE UNIQUE INDEX notes_tenant_body ON notes (tenant, body)",
            "CREATE INDEX notes_body_lookup ON notes (body)",
            "CREATE INDEX notes_tenant_lookup ON notes (tenant, body)",
            "DROP INDEX notes_body",
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
    fn materialization_renderer_changes_only_uuid_resolved_table_names() {
        let rendered = render_create_table(
            "CREATE TABLE children (
                id INTEGER PRIMARY KEY,
                first INTEGER REFERENCES parents(id),
                second INTEGER,
                FOREIGN KEY (second) REFERENCES parents(id)
            )",
            &[(
                SqlName::new("parents".into()),
                SqlName::new("Archived Parents".into()),
            )],
        )
        .unwrap();
        let ValidatedExecute::CreateTable(spec) = validate_execute(&rendered).unwrap() else {
            panic!("rendered table parsed as another statement kind")
        };
        assert!(
            spec.foreign_keys
                .iter()
                .all(|foreign_key| foreign_key.referenced_table
                    == SqlName::new("Archived Parents".into()))
        );
    }

    #[test]
    fn alter_rename_preserves_case_insensitive_names_and_rejects_other_forms() {
        let ValidatedExecute::RenameTable(spec) =
            validate_execute("ALTER TABLE \"Old Notes\" RENAME TO `Archived Notes`").unwrap()
        else {
            panic!("table rename parsed as another statement kind")
        };
        assert_eq!(spec.table, SqlName::new("Old Notes".into()));
        assert_eq!(spec.new_name, SqlName::new("Archived Notes".into()));

        let ValidatedExecute::RenameColumn(spec) =
            validate_execute("ALTER TABLE notes RENAME COLUMN body TO text").unwrap()
        else {
            panic!("column rename parsed as another statement kind")
        };
        assert_eq!(spec.table, SqlName::new("notes".into()));
        assert_eq!(spec.old_name, SqlName::new("body".into()));
        assert_eq!(spec.new_name, SqlName::new("text".into()));

        let ValidatedExecute::AddColumn(spec) = validate_execute(
            "ALTER TABLE notes ADD COLUMN summary TEXT DEFAULT 'none' CHECK (length(summary) > 0)",
        )
        .unwrap() else {
            panic!("ADD COLUMN parsed as another statement kind")
        };
        assert_eq!(spec.table, SqlName::new("notes".into()));
        assert_eq!(spec.column.name, SqlName::new("summary".into()));
        assert!(spec.column.default.is_some());
        assert_eq!(spec.checks.len(), 1);

        let ValidatedExecute::DropColumn(spec) =
            validate_execute("ALTER TABLE notes DROP COLUMN summary").unwrap()
        else {
            panic!("DROP COLUMN parsed as another statement kind")
        };
        assert_eq!(spec.table, SqlName::new("notes".into()));
        assert_eq!(spec.column, SqlName::new("summary".into()));

        for sql in [
            "ALTER TABLE main.notes RENAME TO archived",
            "ALTER TABLE notes RENAME TO NOTES",
            "ALTER TABLE notes RENAME TO __multilite__notes",
            "ALTER TABLE notes RENAME TO sqlite_notes",
            "ALTER TABLE notes RENAME COLUMN body TO BODY",
            "ALTER TABLE notes ADD COLUMN extra TEXT UNIQUE",
            "ALTER TABLE notes ADD COLUMN extra TEXT REFERENCES parents(id)",
        ] {
            assert_unsupported(sql);
        }
    }

    #[test]
    fn create_index_preserves_its_semantic_kind_and_column_order() {
        for (sql, expected_unique) in [
            ("CREATE INDEX notes_lookup ON notes (tenant, body)", false),
            (
                "CREATE UNIQUE INDEX notes_identity ON notes (tenant, body)",
                true,
            ),
        ] {
            let ValidatedExecute::CreateIndex(spec) = validate_execute(sql).unwrap() else {
                panic!("CREATE INDEX parsed as another statement kind")
            };
            assert_eq!(spec.unique, expected_unique);
            assert_eq!(
                spec.name.value(),
                if expected_unique {
                    "notes_identity"
                } else {
                    "notes_lookup"
                }
            );
            assert_eq!(spec.table.value(), "notes");
            assert_eq!(
                spec.terms
                    .iter()
                    .map(|term| match term {
                        CreateIndexTerm::Column { name, .. } => name.value(),
                        CreateIndexTerm::Expression { .. } => panic!("unexpected expression"),
                    })
                    .collect::<Vec<_>>(),
                ["tenant", "body"]
            );
            assert!(spec.predicate.is_none());
        }
    }

    #[test]
    fn ordinary_indexes_preserve_order_collation_expressions_and_predicates() {
        let sql = "CREATE INDEX notes_search ON notes (
            tenant COLLATE NOCASE DESC,
            lower(trim(body)) ASC,
            tenant
        ) WHERE tenant IS NOT NULL AND length(body) > 0";
        let ValidatedExecute::CreateIndex(spec) = validate_execute(sql).unwrap() else {
            unreachable!()
        };
        assert!(!spec.unique);
        assert_eq!(spec.terms.len(), 3);
        assert_eq!(
            spec.terms[0],
            CreateIndexTerm::Column {
                name: SqlName::new("tenant".into()),
                collation: Some(SqlName::new("NOCASE".into())),
                order: Some(IndexOrder::Desc),
            }
        );
        let CreateIndexTerm::Expression { expression, order } = &spec.terms[1] else {
            panic!("function term was not retained as an expression")
        };
        assert_eq!(*order, Some(IndexOrder::Asc));
        assert_eq!(
            parse_schema_expression(&expression.to_string()).unwrap(),
            *expression
        );
        assert_eq!(
            spec.terms[2],
            CreateIndexTerm::Column {
                name: SqlName::new("tenant".into()),
                collation: None,
                order: None,
            }
        );
        let predicate = spec.predicate.unwrap();
        assert_eq!(
            parse_schema_expression(&predicate.to_string()).unwrap(),
            predicate
        );
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
    fn rejects_qualified_reserved_and_reserved_reading_inserts() {
        for sql in [
            "INSERT INTO main.notes VALUES (1)",
            "INSERT INTO notes AS target VALUES (1)",
            "INSERT INTO __multilite__meta VALUES (x'01', x'02')",
            "INSERT INTO sqlite_schema VALUES ('table', 'x', 'x', 1, '')",
            "WITH hidden AS (SELECT value FROM __multilite__meta)
             INSERT INTO notes SELECT value FROM hidden",
            "INSERT INTO notes SELECT (SELECT value FROM __multilite__meta)",
        ] {
            assert_unsupported(sql);
        }
    }

    #[test]
    fn public_reads_find_reserved_tables_in_every_subquery_expression() {
        for sql in [
            "SELECT (SELECT value FROM __multilite__meta)",
            "SELECT value FROM \"__multilite__meta\"",
            "SELECT value FROM [__multilite__meta]",
            "SELECT value FROM `__multilite__meta`",
            "SELECT 1 WHERE EXISTS (SELECT 1 FROM __multilite__meta)",
            "SELECT 1 WHERE 1 IN (SELECT length(value) FROM __multilite__meta)",
            "SELECT CASE WHEN EXISTS (SELECT 1 FROM __multilite__meta)
                         THEN 1 ELSE 0 END",
            "SELECT count(*) FILTER (
                WHERE EXISTS (SELECT 1 FROM __multilite__meta)
             ) FROM notes",
            "SELECT 1 ORDER BY (SELECT value FROM __multilite__meta) LIMIT 1",
            "SELECT 1 LIMIT (SELECT length(value) FROM __multilite__meta)",
        ] {
            assert!(
                matches!(validate_statement(sql), Err(Error::UnsupportedSql(_))),
                "prepared read gate accepted: {sql}"
            );
            assert!(
                matches!(validate_read_statement(sql), Err(Error::PreparedWrite)),
                "read-only gate accepted: {sql}"
            );
        }

        for sql in [
            "SELECT (SELECT body FROM notes LIMIT 1)",
            "SELECT 1 WHERE EXISTS (SELECT 1 FROM notes)",
            "SELECT 1 WHERE 1 IN (SELECT id FROM notes)",
        ] {
            assert!(matches!(
                validate_statement(sql),
                Ok(ValidatedStatement::Read)
            ));
            validate_read_statement(sql).unwrap();
        }
    }

    #[test]
    fn mutating_predicates_and_assignments_cannot_read_reserved_tables() {
        for sql in [
            "WITH hidden AS (SELECT value FROM __multilite__meta)
             DELETE FROM notes WHERE id IN (SELECT value FROM hidden)",
            "DELETE FROM notes WHERE EXISTS (
                SELECT 1 FROM __multilite__meta
             )",
            "DELETE FROM notes WHERE id IN (
                SELECT length(value) FROM \"__multilite__meta\"
             )",
            "UPDATE notes SET body = (
                SELECT value FROM [__multilite__meta] LIMIT 1
             )",
            "UPDATE notes SET body = 'x' WHERE EXISTS (
                SELECT 1 FROM `__multilite__meta`
             )",
            "WITH hidden AS (SELECT value FROM __multilite__meta)
             UPDATE notes SET body = (SELECT value FROM hidden LIMIT 1)",
            "UPDATE notes SET body = hidden.value
             FROM __multilite__meta AS hidden",
            "UPDATE notes INDEXED BY __multilite__internal SET body = 'x'",
            "DELETE FROM notes INDEXED BY sqlite_autoindex_notes_1",
        ] {
            assert_unsupported(sql);
        }

        for sql in [
            "DELETE FROM notes WHERE EXISTS (SELECT 1 FROM archived)",
            "WITH doomed AS (SELECT id FROM archived)
             DELETE FROM notes NOT INDEXED WHERE id IN (SELECT id FROM doomed)",
            "UPDATE notes SET body = (SELECT body FROM archived LIMIT 1)",
            "UPDATE notes SET body = 'x' WHERE id IN (SELECT id FROM archived)",
            "WITH replacement AS (SELECT id, body FROM archived)
             UPDATE notes INDEXED BY notes_id
             SET (body, payload) = (replacement.body, x'00')
             FROM replacement WHERE replacement.id = notes.id",
        ] {
            validate_execute(sql).unwrap();
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
            "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT COLLATE nocase)",
            "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT GENERATED ALWAYS AS (id))",
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
    fn normalizes_defaults_checks_and_named_constraints() {
        let ValidatedExecute::CreateTable(spec) = validate_execute(
            "CREATE TABLE accounts (
                id INTEGER CONSTRAINT account_pk PRIMARY KEY,
                state TEXT CONSTRAINT state_required NOT NULL
                    CONSTRAINT state_default DEFAULT ('new')
                    CONSTRAINT state_check CHECK (length(state) > 0),
                score REAL DEFAULT -1.5,
                CONSTRAINT score_check CHECK (score IS NULL OR score >= 0)
            )",
        )
        .unwrap() else {
            unreachable!()
        };

        assert_eq!(
            spec.primary_key_name,
            Some(SqlName::new("account_pk".into()))
        );
        assert_eq!(
            spec.columns[1].not_null_name,
            Some(SqlName::new("state_required".into()))
        );
        let state_default = spec.columns[1].default.as_ref().unwrap();
        assert_eq!(
            state_default.name,
            Some(SqlName::new("state_default".into()))
        );
        assert_eq!(state_default.expression.to_string(), "('new')");
        let score_default = spec.columns[2].default.as_ref().unwrap();
        assert_eq!(score_default.name, None);
        assert_eq!(score_default.expression.to_string(), "- 1.5");
        assert_eq!(spec.checks.len(), 2);
        assert_eq!(spec.checks[0].column, Some(SqlName::new("state".into())));
        assert_eq!(
            spec.checks[0].name,
            Some(SqlName::new("state_check".into()))
        );
        assert_eq!(spec.checks[0].expression.to_string(), "length (state) > 0");
        assert_eq!(spec.checks[1].column, None);
        assert_eq!(
            spec.checks[1].name,
            Some(SqlName::new("score_check".into()))
        );
        assert_eq!(
            spec.checks[1].expression.to_string(),
            "score IS NULL OR score >= 0"
        );
    }

    #[test]
    fn rejects_duplicate_defaults_and_check_conflict_clauses() {
        for sql in [
            "CREATE TABLE notes (
                id INTEGER PRIMARY KEY,
                body TEXT DEFAULT 'one' DEFAULT 'two'
            )",
            "CREATE TABLE notes (
                id INTEGER PRIMARY KEY,
                body TEXT,
                CHECK (length(body) > 0) ON CONFLICT IGNORE
            )",
        ] {
            assert_unsupported(sql);
        }
    }

    #[test]
    fn rejects_check_references_to_unknown_columns_without_panicking() {
        for sql in [
            "CREATE TABLE notes (id INTEGER PRIMARY KEY, CHECK (missing > 0))",
            "CREATE TABLE notes (
                id INTEGER PRIMARY KEY,
                body TEXT CHECK (length(unknown_body) > 0)
            )",
        ] {
            assert_unsupported(sql);
        }
    }

    #[test]
    fn normalizes_immediate_primary_key_foreign_references() {
        let ValidatedExecute::CreateTable(spec) = validate_execute(
            "CREATE TABLE children (
                tenant TEXT NOT NULL,
                child INTEGER PRIMARY KEY,
                parent INTEGER REFERENCES parents,
                CONSTRAINT composite_parent
                    FOREIGN KEY (tenant, parent)
                    REFERENCES memberships (tenant, member)
                    ON UPDATE NO ACTION ON DELETE NO ACTION
            )",
        )
        .unwrap() else {
            unreachable!()
        };

        assert_eq!(spec.foreign_keys.len(), 2);
        assert_eq!(spec.foreign_keys[0].columns[0].value(), "parent");
        assert_eq!(spec.foreign_keys[0].referenced_table.value(), "parents");
        assert!(spec.foreign_keys[0].referenced_columns.is_none());
        assert_eq!(
            spec.foreign_keys[1].name.as_ref().map(SqlName::value),
            Some("composite_parent")
        );
        assert_eq!(
            spec.foreign_keys[1]
                .columns
                .iter()
                .map(SqlName::value)
                .collect::<Vec<_>>(),
            ["tenant", "parent"]
        );
        assert_eq!(
            spec.foreign_keys[1]
                .referenced_columns
                .as_ref()
                .unwrap()
                .iter()
                .map(SqlName::value)
                .collect::<Vec<_>>(),
            ["tenant", "member"]
        );
    }

    #[test]
    fn rejects_foreign_key_extensions_outside_the_initial_slice() {
        for sql in [
            "CREATE TABLE child (id INTEGER PRIMARY KEY, parent INTEGER REFERENCES parents(id) ON DELETE CASCADE)",
            "CREATE TABLE child (id INTEGER PRIMARY KEY, parent INTEGER REFERENCES parents(id) ON UPDATE SET NULL)",
            "CREATE TABLE child (id INTEGER PRIMARY KEY, parent INTEGER REFERENCES parents(id) MATCH FULL)",
            "CREATE TABLE child (id INTEGER PRIMARY KEY, parent INTEGER REFERENCES parents(id) DEFERRABLE INITIALLY DEFERRED)",
            "CREATE TABLE child (id INTEGER PRIMARY KEY, parent INTEGER, FOREIGN KEY (parent) REFERENCES parents(id, tenant))",
            "CREATE TABLE child (id INTEGER PRIMARY KEY, parent INTEGER, FOREIGN KEY (missing) REFERENCES parents(id))",
            "CREATE TABLE child (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, FOREIGN KEY (a, b) REFERENCES parents(id, id))",
            "CREATE TABLE child (id INTEGER PRIMARY KEY, parent INTEGER, FOREIGN KEY (parent DESC) REFERENCES parents(id))",
        ] {
            assert_unsupported(sql);
        }
    }

    #[test]
    fn preserves_table_primary_key_order_and_without_rowid_storage() {
        let ValidatedExecute::CreateTable(spec) = validate_execute(
            "CREATE TABLE memberships (
                tenant TEXT,
                member INTEGER,
                body TEXT,
                PRIMARY KEY (member, tenant)
            ) WITHOUT ROWID",
        )
        .unwrap() else {
            unreachable!()
        };

        assert_eq!(spec.storage, TableStorage::WithoutRowid);
        assert_eq!(
            spec.columns
                .iter()
                .filter_map(|column| {
                    column
                        .primary_key
                        .map(|position| (position, column.name.value()))
                })
                .collect::<Vec<_>>(),
            [(1, "tenant"), (0, "member")]
        );
        assert!(spec.columns[0].not_null);
        assert!(spec.columns[1].not_null);

        let ValidatedExecute::CreateTable(rowid) =
            validate_execute("CREATE TABLE aliases (id INTEGER, PRIMARY KEY (id))").unwrap()
        else {
            unreachable!()
        };
        assert_eq!(rowid.storage, TableStorage::Rowid);
        assert!(!rowid.columns[0].not_null);
    }

    #[test]
    fn composite_primary_keys_require_stable_non_null_identity() {
        for sql in [
            "CREATE TABLE notes (tenant TEXT, id INTEGER, PRIMARY KEY (tenant, id))",
            "CREATE TABLE notes (a TEXT NOT NULL, b TEXT NOT NULL, PRIMARY KEY (a, a))",
            "CREATE TABLE notes (a TEXT NOT NULL, PRIMARY KEY (missing))",
            "CREATE TABLE notes (
                a TEXT NOT NULL,
                b TEXT NOT NULL,
                PRIMARY KEY (a DESC, b)
            )",
        ] {
            assert_unsupported(sql);
        }

        validate_execute(
            "CREATE TABLE notes (
                tenant TEXT NOT NULL,
                id INTEGER NOT NULL,
                PRIMARY KEY (tenant, id)
            ) WITHOUT ROWID",
        )
        .unwrap();
    }

    #[test]
    fn logical_keys_fit_within_homebase_component_limits() {
        let key_columns = (0..=MAX_INDEX_COLUMNS)
            .map(|index| format!("key_{index} INTEGER"))
            .collect::<Vec<_>>()
            .join(", ");
        let key_names = (0..=MAX_INDEX_COLUMNS)
            .map(|index| format!("key_{index}"))
            .collect::<Vec<_>>()
            .join(", ");

        assert_unsupported(&format!(
            "CREATE TABLE too_wide (
                {key_columns},
                PRIMARY KEY ({key_names})
            ) WITHOUT ROWID"
        ));
        assert_unsupported(&format!(
            "CREATE TABLE too_unique (
                id INTEGER PRIMARY KEY,
                {key_columns},
                UNIQUE ({key_names})
            )"
        ));
    }

    #[test]
    fn rowid_tables_require_a_declared_integer_primary_key_alias() {
        for sql in [
            "CREATE TABLE text_key (key TEXT NOT NULL PRIMARY KEY)",
            "CREATE TABLE composite_key (
                tenant TEXT NOT NULL,
                key INTEGER NOT NULL,
                PRIMARY KEY (tenant, key)
            )",
            "CREATE TABLE int_key (key INT NOT NULL PRIMARY KEY)",
        ] {
            assert_unsupported(sql);
        }
        validate_execute(
            "CREATE TABLE aliased (
                rowid INTEGER PRIMARY KEY,
                oid TEXT,
                _rowid_ TEXT
            )",
        )
        .unwrap();
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
            ) WITHOUT ROWID, STRICT",
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
                Some(crate::logical::schema::StrictType::Integer),
                Some(crate::logical::schema::StrictType::Integer),
                Some(crate::logical::schema::StrictType::Integer),
                Some(crate::logical::schema::StrictType::Real),
                Some(crate::logical::schema::StrictType::Text),
                Some(crate::logical::schema::StrictType::Blob),
                Some(crate::logical::schema::StrictType::Any),
            ]
        );
        assert_eq!(
            spec.columns[6].declared_type.affinity_for(spec.mode),
            crate::logical::schema::Affinity::Blob
        );
        assert_eq!(
            TypeDeclaration::new("ANY".into(), Vec::new()).affinity_for(TableMode::Ordinary),
            crate::logical::schema::Affinity::Numeric
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
            if rowid_alias {
                let ValidatedExecute::CreateTable(spec) = validate_execute(&sql).unwrap() else {
                    unreachable!()
                };
                assert!(spec.columns[0].declared_type.is_exact_integer());
                assert_eq!(spec.columns[0].declared_type.affinity(), Affinity::Integer);
            } else {
                assert_unsupported(&sql);
                validate_execute(&format!("{sql} WITHOUT ROWID")).unwrap();
            }
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
    fn accepts_captured_delete_and_update_extensions() {
        for sql in [
            "WITH old AS (SELECT 1 AS id) DELETE FROM notes WHERE id IN old",
            "DELETE FROM notes INDEXED BY notes_id WHERE id = 1",
            "DELETE FROM notes NOT INDEXED WHERE id = 1",
            "WITH replacement AS (SELECT 1 AS id, 'x' AS body)
             UPDATE notes SET (body, payload) = (replacement.body, x'00')
             FROM replacement WHERE replacement.id = notes.id",
            "UPDATE notes INDEXED BY notes_id SET body = 'x'",
            "UPDATE notes NOT INDEXED SET body = 'x'",
        ] {
            validate_execute(sql).unwrap();
        }
    }

    #[test]
    fn rejects_delete_extensions_without_owned_semantics() {
        for sql in [
            "DELETE FROM main.notes",
            "DELETE FROM notes AS old",
            "DELETE FROM notes INDEXED BY __multilite__internal",
            "DELETE FROM notes INDEXED BY sqlite_autoindex_notes_1",
            "DELETE FROM notes RETURNING id",
            "DELETE FROM notes ORDER BY id LIMIT 1",
            "DELETE FROM __multilite__pending",
        ] {
            assert_unsupported(sql);
        }
    }

    #[test]
    fn rejects_update_extensions_without_owned_semantics() {
        for sql in [
            "UPDATE OR REPLACE notes SET body = 'x'",
            "UPDATE main.notes SET body = 'x'",
            "UPDATE notes AS old SET body = 'x'",
            "UPDATE notes INDEXED BY __multilite__internal SET body = 'x'",
            "UPDATE notes INDEXED BY sqlite_autoindex_notes_1 SET body = 'x'",
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
            "BEGIN",
            "EXPLAIN SELECT 1",
            "INSERT INTO notes VALUES (1) RETURNING id",
            "CREATE TABLE one (id); CREATE TABLE two (id)",
        ] {
            assert_unsupported(sql);
        }
    }

    #[test]
    fn rejects_unsafe_or_semantically_unimplemented_index_extensions() {
        for sql in [
            "CREATE UNIQUE INDEX IF NOT EXISTS notes_body ON notes (body)",
            "CREATE INDEX IF NOT EXISTS notes_body ON notes (body)",
            "CREATE UNIQUE INDEX main.notes_body ON notes (body)",
            "CREATE INDEX main.notes_body ON notes (body)",
            "CREATE UNIQUE INDEX notes_body ON notes (body DESC)",
            "CREATE UNIQUE INDEX notes_body ON notes (body COLLATE NOCASE)",
            "CREATE UNIQUE INDEX notes_body ON notes (lower(body))",
            "CREATE UNIQUE INDEX notes_body ON notes (body) WHERE body IS NOT NULL",
            "CREATE UNIQUE INDEX notes_body ON notes (body, body)",
            "CREATE INDEX notes_body ON notes (body NULLS FIRST)",
            "CREATE INDEX notes_body ON notes ((SELECT body FROM notes))",
            "CREATE INDEX notes_body ON notes (?1)",
            "DROP INDEX IF EXISTS notes_body",
            "DROP INDEX main.notes_body",
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
}
