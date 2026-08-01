//! Durable schema identities, codecs, and Homebase coordination keys.
//!
//! A table creation lowers to an immutable UUID-keyed schema log entry plus
//! mutable revision cells. It can be reconstructed only from a complete,
//! self-consistent admitted envelope.

mod compiler;

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use homebase_core::key::{Key, MAX_COMPONENTS};
#[cfg(test)]
use homebase_core::messages::AdmittedBatch;
use homebase_core::tag::Mutation;
use homebase_core::writer::Writer;
use rusqlite::Connection;
use sha2::{Digest, Sha256};
use sqlite3_parser::ast::Expr;
use uuid::{Uuid, Variant, Version};

use super::guard::{GuardPlan, GuardReason, LogicalTarget, OperationFamily};
use super::{catalog, codes};
use crate::commit::footprint::ConflictFootprint;
use crate::sqlite::quote_identifier;
use crate::{Error, Result};

pub use self::compiler::SchemaInvariantError;

const SCHEMA_FRAME_VERSION: u8 = 6;
const TAG_MUTATION_ID: u8 = 1;
const TAG_SQL: u8 = 2;
const TAG_CREATE_TABLE: u8 = 10;
const TAG_TABLE_ID: u8 = 1;
const TAG_TABLE_NAME: u8 = 2;
const TAG_COLUMN: u8 = 3;
const TAG_SCHEMA_REVISION_ID: u8 = 4;
const TAG_UNIQUE_CONSTRAINT: u8 = 6;
const TAG_TABLE_MODE: u8 = 7;
const TAG_PRIMARY_KEY: u8 = 8;
const TAG_TABLE_STORAGE: u8 = 9;
const TAG_INDEX_DEFINITION: u8 = 10;
const TAG_FOREIGN_KEY_DEFINITION: u8 = 11;
const TAG_CHECK_DEFINITION: u8 = 12;
const TAG_COLUMN_ID: u8 = 1;
const TAG_COLUMN_NAME: u8 = 2;
const TAG_COLUMN_TYPE: u8 = 3;
const TAG_COLUMN_FLAGS: u8 = 4;
const TAG_COLUMN_NOT_NULL_NAME: u8 = 5;
const TAG_COLUMN_DEFAULT: u8 = 6;
const TAG_DEFAULT_NAME: u8 = 1;
const TAG_DEFAULT_EXPRESSION: u8 = 2;
const TAG_PRIMARY_INDEX: u8 = 1;
const TAG_PRIMARY_NAME: u8 = 2;
const TAG_CHECK_COLUMN: u8 = 1;
const TAG_CHECK_NAME: u8 = 2;
const TAG_CHECK_EXPRESSION: u8 = 3;
const TAG_CHECK_DEPENDENCY: u8 = 4;
const TYPE_DECLARATION_FRAME_VERSION: u8 = 1;
const TAG_TYPE_NAME: u8 = 1;
const TAG_TYPE_ARGUMENT: u8 = 2;
const TAG_UNIQUE_INDEX_DEFINITION: u8 = 1;
const TAG_UNIQUE_NAME: u8 = 2;
const TAG_NAMED_INDEX_DEFINITION: u8 = 1;
const TAG_INDEX_NAME: u8 = 2;
const TAG_INDEX_ACTIVE: u8 = 6;
const TAG_INDEX_TERM: u8 = 7;
const TAG_INDEX_PREDICATE: u8 = 8;
const TAG_INDEX_DEPENDENCY: u8 = 9;
const INDEX_TERM_FRAME_VERSION: u8 = 1;
const TAG_INDEX_TERM_COLUMN: u8 = 1;
const TAG_INDEX_TERM_COLLATION: u8 = 2;
const TAG_INDEX_TERM_EXPRESSION: u8 = 3;
const TAG_INDEX_TERM_ORDER: u8 = 4;
const INDEX_ORDER_ASC: u8 = 1;
const INDEX_ORDER_DESC: u8 = 2;
const TAG_FOREIGN_KEY_ID: u8 = 1;
const TAG_FOREIGN_KEY_NAME: u8 = 2;
const TAG_FOREIGN_KEY_COLUMN_ID: u8 = 3;
const TAG_FOREIGN_KEY_PARENT_TABLE_ID: u8 = 4;
const TAG_FOREIGN_KEY_PARENT_TABLE_NAME: u8 = 5;
const TAG_FOREIGN_KEY_PARENT_COLUMN_ID: u8 = 7;
const TAG_FOREIGN_KEY_PARENT_COLUMN_NAME: u8 = 8;
const TAG_FOREIGN_KEY_PARENT_INDEX_ID: u8 = 9;
const INDEX_DEFINITION_FRAME_VERSION: u8 = 1;
const TAG_INDEX_ID: u8 = 1;
const TAG_INDEX_KIND: u8 = 2;
const TAG_INDEX_COLUMN_ID: u8 = 3;
const INDEX_KIND_PRIMARY: u8 = 1;
const INDEX_KIND_UNIQUE: u8 = 2;
const INDEX_KIND_SECONDARY: u8 = 3;
const COLUMN_NOT_NULL: u8 = 1;
const TABLE_MODE_ORDINARY: u8 = 0;
const TABLE_MODE_STRICT: u8 = 1;
const TABLE_STORAGE_ROWID: u8 = 0;
const TABLE_STORAGE_WITHOUT_ROWID: u8 = 1;

/// Maximum number of columns in a single logical index definition.
pub const MAX_INDEX_COLUMNS: usize = MAX_COMPONENTS - codes::VALUE_KEY_PREFIX_COMPONENTS;

/// SQLite identifier spelling plus its case-insensitive identity form.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SqlName {
    value: String,
    canonical: Vec<u8>,
}

impl SqlName {
    pub fn new(value: String) -> Self {
        let mut canonical = value.as_bytes().to_vec();
        canonical.make_ascii_lowercase();
        Self { value, canonical }
    }

    pub fn value(&self) -> &str {
        &self.value
    }

    pub fn canonical(&self) -> &[u8] {
        &self.canonical
    }

    pub(super) fn from_sqlite_token(token: &str) -> Result<Self> {
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
        Ok(Self::new(value))
    }
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

/// SQLite's five ordinary-table type affinities.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Affinity {
    Integer,
    Real,
    Text,
    Blob,
    Numeric,
}

impl Affinity {
    pub fn to_u8(self) -> u8 {
        match self {
            Self::Integer => 1,
            Self::Real => 2,
            Self::Text => 3,
            Self::Blob => 4,
            Self::Numeric => 5,
        }
    }

    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            1 => Some(Self::Integer),
            2 => Some(Self::Real),
            3 => Some(Self::Text),
            4 => Some(Self::Blob),
            5 => Some(Self::Numeric),
            _ => None,
        }
    }
}

/// SQLite type enforcement selected for one table.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum TableMode {
    #[default]
    Ordinary,
    Strict,
}

/// Physical SQLite table storage selected by the schema.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum TableStorage {
    #[default]
    Rowid,
    WithoutRowid,
}

impl TableStorage {
    pub(crate) fn to_u8(self) -> u8 {
        match self {
            Self::Rowid => TABLE_STORAGE_ROWID,
            Self::WithoutRowid => TABLE_STORAGE_WITHOUT_ROWID,
        }
    }

    pub(crate) fn from_u8(value: u8) -> Option<Self> {
        match value {
            TABLE_STORAGE_ROWID => Some(Self::Rowid),
            TABLE_STORAGE_WITHOUT_ROWID => Some(Self::WithoutRowid),
            _ => None,
        }
    }
}

impl TableMode {
    fn to_u8(self) -> u8 {
        match self {
            Self::Ordinary => TABLE_MODE_ORDINARY,
            Self::Strict => TABLE_MODE_STRICT,
        }
    }

    fn from_u8(value: u8) -> Option<Self> {
        match value {
            TABLE_MODE_ORDINARY => Some(Self::Ordinary),
            TABLE_MODE_STRICT => Some(Self::Strict),
            _ => None,
        }
    }
}

/// Storage class required after SQLite coerces a STRICT-table value.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StrictType {
    Integer,
    Real,
    Text,
    Blob,
    Any,
}

/// Canonical SQLite type declaration plus its ignored size annotations.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TypeDeclaration {
    name: String,
    arguments: Vec<String>,
}

impl TypeDeclaration {
    pub fn new(name: String, arguments: Vec<String>) -> Self {
        let name = unquote_type_name(name.trim())
            .split_ascii_whitespace()
            .collect::<Vec<_>>()
            .join(" ")
            .to_ascii_uppercase();
        Self { name, arguments }
    }

    #[cfg(test)]
    pub fn integer() -> Self {
        Self::new("INTEGER".into(), Vec::new())
    }

    #[cfg(test)]
    pub fn text() -> Self {
        Self::new("TEXT".into(), Vec::new())
    }

    #[cfg(test)]
    pub fn blob() -> Self {
        Self::new("BLOB".into(), Vec::new())
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    #[cfg(test)]
    pub fn arguments(&self) -> &[String] {
        &self.arguments
    }

    pub fn affinity(&self) -> Affinity {
        if self.name.contains("INT") {
            Affinity::Integer
        } else if ["CHAR", "CLOB", "TEXT"]
            .into_iter()
            .any(|part| self.name.contains(part))
        {
            Affinity::Text
        } else if self.name.contains("BLOB") {
            Affinity::Blob
        } else if ["REAL", "FLOA", "DOUB"]
            .into_iter()
            .any(|part| self.name.contains(part))
        {
            Affinity::Real
        } else {
            Affinity::Numeric
        }
    }

    pub fn affinity_for(&self, mode: TableMode) -> Affinity {
        if mode == TableMode::Strict && self.strict_type() == Some(StrictType::Any) {
            Affinity::Blob
        } else {
            self.affinity()
        }
    }

    pub fn strict_type(&self) -> Option<StrictType> {
        if !self.arguments.is_empty() {
            return None;
        }
        match self.name.as_str() {
            "INT" | "INTEGER" => Some(StrictType::Integer),
            "REAL" => Some(StrictType::Real),
            "TEXT" => Some(StrictType::Text),
            "BLOB" => Some(StrictType::Blob),
            "ANY" => Some(StrictType::Any),
            _ => None,
        }
    }

    pub fn is_exact_integer(&self) -> bool {
        self.name == "INTEGER" && self.arguments.is_empty()
    }

    pub fn to_sql(&self) -> String {
        if self.arguments.is_empty() {
            self.name.clone()
        } else {
            format!("{}({})", self.name, self.arguments.join(", "))
        }
    }
}

fn unquote_type_name(name: &str) -> String {
    let bytes = name.as_bytes();
    let Some((&open, middle_and_close)) = bytes.split_first() else {
        return String::new();
    };
    let Some((&close, middle)) = middle_and_close.split_last() else {
        return name.to_owned();
    };
    let escaped = match (open, close) {
        (b'"', b'"') | (b'`', b'`') | (b'\'', b'\'') => Some(open),
        (b'[', b']') => Some(close),
        _ => None,
    };
    let Some(escaped) = escaped else {
        return name.to_owned();
    };

    let mut value = Vec::with_capacity(middle.len());
    let mut index = 0;
    while index < middle.len() {
        value.push(middle[index]);
        if middle[index] == escaped && middle.get(index + 1) == Some(&escaped) {
            index += 1;
        }
        index += 1;
    }
    String::from_utf8(value).expect("SQLite type names originate in UTF-8 SQL")
}

/// One validated column in a restricted `CREATE TABLE` statement.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateColumn {
    pub name: SqlName,
    pub declared_type: TypeDeclaration,
    pub not_null: bool,
    pub not_null_name: Option<SqlName>,
    pub default: Option<DefaultDefinition>,
    /// Position in the table's ordered primary key, if any.
    pub primary_key: Option<usize>,
}

/// One validated inline or table-level UNIQUE declaration.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateUnique {
    pub name: Option<SqlName>,
    pub columns: Vec<SqlName>,
}

/// One validated immediate `NO ACTION` foreign-key declaration.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateForeignKey {
    pub name: Option<SqlName>,
    pub columns: Vec<SqlName>,
    pub referenced_table: SqlName,
    pub referenced_columns: Option<Vec<SqlName>>,
}

/// One optional SQLite default owned by a column.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DefaultDefinition {
    pub name: Option<SqlName>,
    pub expression: SqlExpression,
}

/// An owned SQLite expression used only in the in-memory schema model.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SqlExpression(Box<Expr>);

impl SqlExpression {
    pub(super) fn new(expression: Expr) -> Self {
        Self(Box::new(expression))
    }

    pub(super) fn referenced_columns(&self) -> Vec<SqlName> {
        let mut columns = BTreeMap::new();
        collect_expression_columns(&self.0, &mut columns);
        columns.into_values().collect()
    }

    fn rename_column(&mut self, old_name: &SqlName, new_name: &SqlName) {
        rename_expression_column(&mut self.0, old_name, new_name);
    }
}

impl fmt::Display for SqlExpression {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&*self.0, formatter)
    }
}

fn collect_expression_columns(expression: &Expr, columns: &mut BTreeMap<Vec<u8>, SqlName>) {
    match expression {
        Expr::Between {
            lhs, start, end, ..
        } => {
            collect_expression_columns(lhs, columns);
            collect_expression_columns(start, columns);
            collect_expression_columns(end, columns);
        }
        Expr::Binary(left, _, right) => {
            collect_expression_columns(left, columns);
            collect_expression_columns(right, columns);
        }
        Expr::Case {
            base,
            when_then_pairs,
            else_expr,
        } => {
            if let Some(base) = base {
                collect_expression_columns(base, columns);
            }
            for (when, then) in when_then_pairs {
                collect_expression_columns(when, columns);
                collect_expression_columns(then, columns);
            }
            if let Some(else_expr) = else_expr {
                collect_expression_columns(else_expr, columns);
            }
        }
        Expr::Cast { expr, .. }
        | Expr::Collate(expr, _)
        | Expr::IsNull(expr)
        | Expr::NotNull(expr)
        | Expr::Unary(_, expr) => collect_expression_columns(expr, columns),
        Expr::DoublyQualified(_, _, column) | Expr::Qualified(_, column) => {
            record_expression_column(columns, &column.0)
        }
        Expr::FunctionCall {
            args,
            order_by,
            filter_over,
            ..
        } => {
            if let Some(args) = args {
                for argument in args {
                    collect_expression_columns(argument, columns);
                }
            }
            if let Some(sqlite3_parser::ast::FunctionCallOrder::SortList(order_by)) = order_by {
                for column in order_by {
                    collect_expression_columns(&column.expr, columns);
                }
            }
            if let Some(filter) = filter_over
                .as_ref()
                .and_then(|tail| tail.filter_clause.as_deref())
            {
                collect_expression_columns(filter, columns);
            }
        }
        Expr::FunctionCallStar { filter_over, .. } => {
            if let Some(filter) = filter_over
                .as_ref()
                .and_then(|tail| tail.filter_clause.as_deref())
            {
                collect_expression_columns(filter, columns);
            }
        }
        Expr::Id(identifier) => record_expression_column(columns, &identifier.0),
        Expr::InList { lhs, rhs, .. } => {
            collect_expression_columns(lhs, columns);
            if let Some(rhs) = rhs {
                for expression in rhs {
                    collect_expression_columns(expression, columns);
                }
            }
        }
        Expr::InSelect { lhs, .. } | Expr::InTable { lhs, .. } => {
            collect_expression_columns(lhs, columns);
        }
        Expr::Like {
            lhs, rhs, escape, ..
        } => {
            collect_expression_columns(lhs, columns);
            collect_expression_columns(rhs, columns);
            if let Some(escape) = escape {
                collect_expression_columns(escape, columns);
            }
        }
        Expr::Name(name) => record_expression_column(columns, &name.0),
        Expr::Parenthesized(expressions) => {
            for expression in expressions {
                collect_expression_columns(expression, columns);
            }
        }
        Expr::Raise(_, expression) => {
            if let Some(expression) = expression {
                collect_expression_columns(expression, columns);
            }
        }
        Expr::Exists(_) | Expr::Literal(_) | Expr::Subquery(_) | Expr::Variable(_) => {}
    }
}

fn record_expression_column(columns: &mut BTreeMap<Vec<u8>, SqlName>, value: &str) {
    // Preserve malformed spellings as unresolved names. The compiler will
    // classify them instead of allowing parser/compiler disagreement to panic.
    let name = SqlName::from_sqlite_token(value).unwrap_or_else(|_| SqlName::new(value.to_owned()));
    columns.entry(name.canonical().to_vec()).or_insert(name);
}

fn rename_expression_column(expression: &mut Expr, old_name: &SqlName, new_name: &SqlName) {
    match expression {
        Expr::Between {
            lhs, start, end, ..
        } => {
            rename_expression_column(lhs, old_name, new_name);
            rename_expression_column(start, old_name, new_name);
            rename_expression_column(end, old_name, new_name);
        }
        Expr::Binary(left, _, right) => {
            rename_expression_column(left, old_name, new_name);
            rename_expression_column(right, old_name, new_name);
        }
        Expr::Case {
            base,
            when_then_pairs,
            else_expr,
        } => {
            if let Some(base) = base {
                rename_expression_column(base, old_name, new_name);
            }
            for (when, then) in when_then_pairs {
                rename_expression_column(when, old_name, new_name);
                rename_expression_column(then, old_name, new_name);
            }
            if let Some(else_expr) = else_expr {
                rename_expression_column(else_expr, old_name, new_name);
            }
        }
        Expr::Cast { expr, .. }
        | Expr::Collate(expr, _)
        | Expr::IsNull(expr)
        | Expr::NotNull(expr)
        | Expr::Unary(_, expr) => rename_expression_column(expr, old_name, new_name),
        Expr::DoublyQualified(_, _, column) | Expr::Qualified(_, column) => {
            rename_expression_name(column, old_name, new_name)
        }
        Expr::FunctionCall {
            args,
            order_by,
            filter_over,
            ..
        } => {
            for argument in args.iter_mut().flatten() {
                rename_expression_column(argument, old_name, new_name);
            }
            if let Some(sqlite3_parser::ast::FunctionCallOrder::SortList(order_by)) = order_by {
                for column in order_by {
                    rename_expression_column(&mut column.expr, old_name, new_name);
                }
            }
            if let Some(filter) = filter_over
                .as_mut()
                .and_then(|tail| tail.filter_clause.as_deref_mut())
            {
                rename_expression_column(filter, old_name, new_name);
            }
        }
        Expr::FunctionCallStar { filter_over, .. } => {
            if let Some(filter) = filter_over
                .as_mut()
                .and_then(|tail| tail.filter_clause.as_deref_mut())
            {
                rename_expression_column(filter, old_name, new_name);
            }
        }
        Expr::Id(identifier) => {
            if expression_name_matches(&identifier.0, old_name) {
                identifier.0 = quoted_identifier(new_name);
            }
        }
        Expr::InList { lhs, rhs, .. } => {
            rename_expression_column(lhs, old_name, new_name);
            for expression in rhs.iter_mut().flatten() {
                rename_expression_column(expression, old_name, new_name);
            }
        }
        Expr::InSelect { lhs, .. } | Expr::InTable { lhs, .. } => {
            rename_expression_column(lhs, old_name, new_name);
        }
        Expr::Like {
            lhs, rhs, escape, ..
        } => {
            rename_expression_column(lhs, old_name, new_name);
            rename_expression_column(rhs, old_name, new_name);
            if let Some(escape) = escape {
                rename_expression_column(escape, old_name, new_name);
            }
        }
        Expr::Name(name) => rename_expression_name(name, old_name, new_name),
        Expr::Parenthesized(expressions) => {
            for expression in expressions {
                rename_expression_column(expression, old_name, new_name);
            }
        }
        Expr::Raise(_, expression) => {
            if let Some(expression) = expression {
                rename_expression_column(expression, old_name, new_name);
            }
        }
        Expr::Exists(_) | Expr::Literal(_) | Expr::Subquery(_) | Expr::Variable(_) => {}
    }
}

fn rename_expression_name(
    name: &mut sqlite3_parser::ast::Name,
    old_name: &SqlName,
    new_name: &SqlName,
) {
    if expression_name_matches(&name.0, old_name) {
        name.0 = quoted_identifier(new_name);
    }
}

fn expression_name_matches(token: &str, expected: &SqlName) -> bool {
    SqlName::from_sqlite_token(token).is_ok_and(|name| name.canonical() == expected.canonical())
}

fn quoted_identifier(name: &SqlName) -> Box<str> {
    format!("\"{}\"", name.value().replace('"', "\"\"")).into()
}

/// One validated CHECK declaration before column names become stable IDs.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateCheckConstraint {
    pub column: Option<SqlName>,
    pub name: Option<SqlName>,
    pub expression: SqlExpression,
}

/// Structured result of validating a restricted `CREATE TABLE` statement.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateTableSpec {
    pub name: SqlName,
    pub mode: TableMode,
    pub storage: TableStorage,
    pub columns: Vec<CreateColumn>,
    pub primary_key_name: Option<SqlName>,
    pub unique_constraints: Vec<CreateUnique>,
    pub foreign_keys: Vec<CreateForeignKey>,
    pub checks: Vec<CreateCheckConstraint>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MutationId([u8; 16]);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TableId([u8; 16]);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SchemaRevisionId([u8; 16]);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct IndexId([u8; 16]);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ForeignKeyId([u8; 16]);

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct ColumnId([u8; 16]);

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Column {
    id: ColumnId,
    name: SqlName,
    declared_type: TypeDeclaration,
    not_null: bool,
    not_null_name: Option<SqlName>,
    default: Option<DefaultDefinition>,
}

/// Ordered durable primary-key definition owned by a table.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PrimaryKey {
    name: Option<SqlName>,
    index: IndexDefinition,
}

/// One durable UNIQUE key definition owned by a table schema revision.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UniqueConstraint {
    name: Option<SqlName>,
    index: IndexDefinition,
}

/// Semantic kind of one table-owned logical index.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IndexKind {
    Primary,
    Unique,
    Secondary,
}

/// Explicit ordering attached to one ordinary secondary-index term.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IndexOrder {
    Asc,
    Desc,
}

/// One durable term in an ordinary secondary-index definition.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum IndexTerm {
    Column {
        column: ColumnId,
        collation: Option<SqlName>,
        order: Option<IndexOrder>,
    },
    Expression {
        expression: SqlExpression,
        order: Option<IndexOrder>,
    },
}

/// Stable identity and comparison columns for one logical table index.
///
/// Primary and UNIQUE indexes carry ordered comparison columns here. Ordinary
/// secondary indexes carry their physical terms on [`NamedIndex`] instead.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IndexDefinition {
    id: IndexId,
    kind: IndexKind,
    columns: Vec<ColumnId>,
}

/// Typed index state attached to a table schema.
///
/// The DDL operation owns and verifies its immutable SQL provenance. Catalog
/// snapshots retain only this executable IR, so table rebuilds cannot acquire a
/// second, unchecked SQL interpretation of the same index.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NamedIndex {
    name: SqlName,
    index: IndexDefinition,
    active: bool,
    terms: Vec<IndexTerm>,
    predicate: Option<SqlExpression>,
    dependencies: Vec<ColumnId>,
}

/// One stable immediate `NO ACTION` relationship owned by the child table.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ForeignKeyDefinition {
    id: ForeignKeyId,
    name: Option<SqlName>,
    columns: Vec<ColumnId>,
    referenced_table: TableId,
    referenced_table_name: SqlName,
    referenced_index: IndexId,
    referenced_columns: Vec<ColumnId>,
    referenced_column_names: Vec<SqlName>,
}

/// One SQLite CHECK declaration owned by a table schema.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CheckConstraint {
    column: Option<ColumnId>,
    name: Option<SqlName>,
    expression: SqlExpression,
    dependencies: Vec<ColumnId>,
}

/// Complete schema known for one table revision.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TableSchema {
    mode: TableMode,
    storage: TableStorage,
    columns: Vec<Column>,
    primary_key: PrimaryKey,
    unique_constraints: Vec<UniqueConstraint>,
    indexes: Vec<NamedIndex>,
    foreign_keys: Vec<ForeignKeyDefinition>,
    checks: Vec<CheckConstraint>,
}

/// Durable meaning of a restricted table creation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateTable {
    mutation_id: MutationId,
    sql: String,
    table_id: TableId,
    schema_revision_id: SchemaRevisionId,
    name: SqlName,
    schema: TableSchema,
}

/// Homebase mutations and conflict footprint for one schema change.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SchemaHomebaseOp {
    pub mutations: Vec<Mutation>,
    pub footprint: ConflictFootprint,
    pub guards: GuardPlan,
}

macro_rules! id_accessors {
    ($type:ty) => {
        impl $type {
            pub fn from_bytes(bytes: [u8; 16]) -> Self {
                Self(bytes)
            }

            pub fn as_bytes(self) -> [u8; 16] {
                self.0
            }
        }
    };
}

id_accessors!(TableId);
id_accessors!(MutationId);
id_accessors!(SchemaRevisionId);
id_accessors!(IndexId);
id_accessors!(ForeignKeyId);
id_accessors!(ColumnId);

impl Column {
    pub fn id(&self) -> ColumnId {
        self.id
    }

    pub fn name(&self) -> &SqlName {
        &self.name
    }

    pub fn declared_type(&self) -> &TypeDeclaration {
        &self.declared_type
    }

    pub fn affinity(&self, mode: TableMode) -> Affinity {
        self.declared_type.affinity_for(mode)
    }

    pub fn strict_type(&self) -> Option<StrictType> {
        self.declared_type.strict_type()
    }

    pub fn is_not_null(&self) -> bool {
        self.not_null
    }

    pub fn not_null_name(&self) -> Option<&SqlName> {
        self.not_null_name.as_ref()
    }

    pub fn default(&self) -> Option<&DefaultDefinition> {
        self.default.as_ref()
    }
}

impl PrimaryKey {
    pub fn name(&self) -> Option<&SqlName> {
        self.name.as_ref()
    }

    pub fn columns(&self) -> &[ColumnId] {
        self.index.columns()
    }

    pub fn index(&self) -> &IndexDefinition {
        &self.index
    }
}

impl TableSchema {
    #[allow(clippy::too_many_arguments)]
    fn try_from_parts(
        owner: TableId,
        mode: TableMode,
        storage: TableStorage,
        columns: Vec<Column>,
        primary_key: PrimaryKey,
        unique_constraints: Vec<UniqueConstraint>,
        indexes: Vec<NamedIndex>,
        foreign_keys: Vec<ForeignKeyDefinition>,
        checks: Vec<CheckConstraint>,
    ) -> std::result::Result<Self, SchemaInvariantError> {
        let schema = Self {
            mode,
            storage,
            columns,
            primary_key,
            unique_constraints,
            indexes,
            foreign_keys,
            checks,
        };
        compiler::validate_table_schema(owner, &schema)?;
        Ok(schema)
    }

    fn validate_for(&self, owner: TableId) -> std::result::Result<(), SchemaInvariantError> {
        compiler::validate_table_schema(owner, self)
    }

    pub fn mode(&self) -> TableMode {
        self.mode
    }

    pub fn storage(&self) -> TableStorage {
        self.storage
    }

    pub fn columns(&self) -> &[Column] {
        &self.columns
    }

    pub fn primary_key(&self) -> &PrimaryKey {
        &self.primary_key
    }

    pub fn unique_constraints(&self) -> &[UniqueConstraint] {
        &self.unique_constraints
    }

    pub fn indexes(&self) -> &[NamedIndex] {
        &self.indexes
    }

    #[allow(dead_code, reason = "populated when FOREIGN KEY grammar is admitted")]
    pub fn foreign_keys(&self) -> &[ForeignKeyDefinition] {
        &self.foreign_keys
    }

    pub fn checks(&self) -> &[CheckConstraint] {
        &self.checks
    }
}

impl UniqueConstraint {
    pub fn index_id(&self) -> IndexId {
        self.index.id()
    }

    pub fn columns(&self) -> &[ColumnId] {
        self.index.columns()
    }

    pub fn index(&self) -> &IndexDefinition {
        &self.index
    }
}

impl ForeignKeyDefinition {
    pub fn id(&self) -> ForeignKeyId {
        self.id
    }

    #[cfg(test)]
    pub fn name(&self) -> Option<&SqlName> {
        self.name.as_ref()
    }

    pub fn columns(&self) -> &[ColumnId] {
        &self.columns
    }

    pub fn referenced_table(&self) -> TableId {
        self.referenced_table
    }

    pub fn referenced_index(&self) -> IndexId {
        self.referenced_index
    }

    pub fn referenced_columns(&self) -> &[ColumnId] {
        &self.referenced_columns
    }

    #[cfg(test)]
    pub fn referenced_column_names(&self) -> &[SqlName] {
        &self.referenced_column_names
    }
}

impl IndexKind {
    fn to_u8(self) -> u8 {
        match self {
            Self::Primary => INDEX_KIND_PRIMARY,
            Self::Unique => INDEX_KIND_UNIQUE,
            Self::Secondary => INDEX_KIND_SECONDARY,
        }
    }

    fn from_u8(value: u8) -> Option<Self> {
        match value {
            INDEX_KIND_PRIMARY => Some(Self::Primary),
            INDEX_KIND_UNIQUE => Some(Self::Unique),
            INDEX_KIND_SECONDARY => Some(Self::Secondary),
            _ => None,
        }
    }
}

/// Whether an index definition fits the currently supported SQL and durable
/// representation. Ordinary secondary indexes have no per-row Homebase key.
fn index_columns_supported(kind: IndexKind, index_columns: usize) -> bool {
    match kind {
        IndexKind::Primary | IndexKind::Unique => codes::VALUE_KEY_PREFIX_COMPONENTS
            .checked_add(index_columns)
            .is_some_and(|components| components <= MAX_COMPONENTS),
        IndexKind::Secondary => index_columns == 0,
    }
}

impl IndexDefinition {
    pub fn id(&self) -> IndexId {
        self.id
    }

    pub fn kind(&self) -> IndexKind {
        self.kind
    }

    pub fn columns(&self) -> &[ColumnId] {
        &self.columns
    }

    pub fn encode(&self) -> Vec<u8> {
        encode_logical_index(self)
    }
}

impl NamedIndex {
    pub fn new_unique(name: SqlName, columns: Vec<ColumnId>) -> Self {
        let dependencies = columns.iter().copied().collect::<BTreeSet<_>>();
        Self {
            name,
            index: IndexDefinition {
                id: IndexId(Uuid::new_v4().into_bytes()),
                kind: IndexKind::Unique,
                columns,
            },
            active: true,
            terms: Vec::new(),
            predicate: None,
            dependencies: dependencies.into_iter().collect(),
        }
    }

    pub fn new_secondary(
        name: SqlName,
        terms: Vec<IndexTerm>,
        predicate: Option<SqlExpression>,
        dependencies: Vec<ColumnId>,
    ) -> Self {
        Self {
            name,
            index: IndexDefinition {
                id: IndexId(Uuid::new_v4().into_bytes()),
                kind: IndexKind::Secondary,
                columns: Vec::new(),
            },
            active: true,
            terms,
            predicate,
            dependencies,
        }
    }

    pub fn index_id(&self) -> IndexId {
        self.index.id()
    }

    pub fn name(&self) -> &SqlName {
        &self.name
    }

    pub fn is_unique(&self) -> bool {
        self.index.kind() == IndexKind::Unique
    }

    pub fn columns(&self) -> &[ColumnId] {
        self.index.columns()
    }

    pub fn terms(&self) -> &[IndexTerm] {
        &self.terms
    }

    pub fn predicate(&self) -> Option<&SqlExpression> {
        self.predicate.as_ref()
    }

    pub fn dependencies(&self) -> &[ColumnId] {
        &self.dependencies
    }

    pub fn index(&self) -> &IndexDefinition {
        &self.index
    }

    /// Render this typed definition against current table and column bindings.
    pub fn materialization_sql(
        &self,
        connection: &Connection,
        owner: &CreateTable,
        table: &SqlName,
    ) -> Result<String> {
        if owner.index_definition(self.index_id()) != Some(self.index()) {
            return Err(Error::InvalidDatabase(
                "materialized index is not owned by its table definition",
            ));
        }
        let column_name = |column| {
            catalog::column_name_by_id(connection, owner.table_id(), column)?.ok_or(
                Error::InvalidDatabase("materialized index column has no current name binding"),
            )
        };
        let terms = if self.is_unique() {
            self.columns()
                .iter()
                .map(|column| column_name(*column).map(|name| quote_identifier(name.value())))
                .collect::<Result<Vec<_>>>()?
        } else {
            self.terms
                .iter()
                .map(|term| match term {
                    IndexTerm::Column {
                        column,
                        collation,
                        order,
                    } => {
                        let mut term = quote_identifier(column_name(*column)?.value());
                        if let Some(collation) = collation {
                            term.push_str(" COLLATE ");
                            term.push_str(&quote_identifier(collation.value()));
                        }
                        push_index_order(&mut term, *order);
                        Ok(term)
                    }
                    IndexTerm::Expression { expression, order } => {
                        let mut term = expression.to_string();
                        push_index_order(&mut term, *order);
                        Ok(term)
                    }
                })
                .collect::<Result<Vec<_>>>()?
        };
        let mut sql = format!(
            "CREATE {}INDEX {} ON {} ({})",
            if self.is_unique() { "UNIQUE " } else { "" },
            quote_identifier(self.name.value()),
            quote_identifier(table.value()),
            terms.join(", ")
        );
        if let Some(predicate) = &self.predicate {
            sql.push_str(" WHERE ");
            sql.push_str(&predicate.to_string());
        }
        Ok(sql)
    }

    pub fn is_active(&self) -> bool {
        self.active
    }

    pub fn retired(&self) -> Self {
        let mut retired = self.clone();
        retired.active = false;
        retired
    }

    pub fn encode(&self) -> Vec<u8> {
        encode_named_index(self)
    }

    pub fn decode(frame: &[u8]) -> std::result::Result<Self, SchemaCodecError> {
        decode_named_index(frame)
    }
}

impl CreateTable {
    #[allow(clippy::too_many_arguments)]
    fn try_from_parts(
        mutation_id: MutationId,
        sql: String,
        table_id: TableId,
        schema_revision_id: SchemaRevisionId,
        name: SqlName,
        schema: TableSchema,
    ) -> std::result::Result<Self, SchemaInvariantError> {
        schema.validate_for(table_id)?;
        let created = Self {
            mutation_id,
            sql,
            table_id,
            schema_revision_id,
            name,
            schema,
        };
        compiler::validate_create_table(&created)?;
        Ok(created)
    }

    pub(super) fn validate_ir(&self) -> std::result::Result<(), SchemaInvariantError> {
        self.schema.validate_for(self.table_id)?;
        compiler::validate_create_table(self)
    }

    fn validate_operation(&self) -> Result<()> {
        self.validate_ir()
            .map_err(|error| Error::InvalidMultiliteOp(error.to_string()))?;
        validate_initial_provenance_sql(self)
            .map_err(|error| Error::InvalidMultiliteOp(error.to_string()))
    }

    /// Mint durable identities for one validated table creation.
    #[cfg(test)]
    pub fn new(sql: &str, spec: CreateTableSpec) -> Self {
        assert!(
            spec.foreign_keys.is_empty(),
            "foreign keys must be resolved against a schema catalog"
        );
        build_create_table(sql, spec, Vec::new(), || Uuid::new_v4().into_bytes())
            .expect("test CREATE TABLE specification is valid")
    }

    /// Resolve stable parent identities and mint one table definition.
    pub fn prepare(connection: &Connection, sql: &str, spec: CreateTableSpec) -> Result<Self> {
        let resolved = resolve_foreign_keys(connection, &spec)?;
        validate_foreign_reference_key_shapes(&spec, &resolved)?;
        build_create_table(sql, spec, resolved, || Uuid::new_v4().into_bytes())
    }

    /// Lower this schema change to its complete Homebase representation.
    pub fn to_homebase(&self) -> Result<SchemaHomebaseOp> {
        self.validate_operation()?;
        let log = schema_log_key(self.mutation_id);
        let name_scope = schema_object_name_scope_key(&self.name);
        let schema = table_schema_key(self.table_id, self.schema_revision_id);
        let active_schema_revision = active_schema_revision_key(self.table_id);
        let active_primary_index = active_primary_index_key(self.table_id);
        let primary_index = index_definition_key(self.table_id, self.primary_index_id());
        let write_revision = write_revision_key(self.table_id);
        let mut guards = GuardPlan::for_operation(OperationFamily::CreateTable);
        guards.invariant(name_scope.clone(), GuardReason::SchemaObjectName)?;
        guards.write(write_revision.clone(), GuardReason::WriteContract)?;
        let mut parent_write_revisions = BTreeSet::new();
        for foreign_key in &self.schema.foreign_keys {
            // The schema head guards the referenced logical index definition,
            // whether it is the primary or a UNIQUE index.
            guards.invariant(
                active_schema_revision_key(foreign_key.referenced_table),
                GuardReason::SchemaRevision,
            )?;
            let revision = write_revision_key(foreign_key.referenced_table);
            guards.write(revision.clone(), GuardReason::WriteContract)?;
            parent_write_revisions.insert(revision);
        }
        let mut mutations = vec![
            Mutation::Set {
                key: log,
                value: self.encode(),
            },
            Mutation::Set {
                key: name_scope.clone(),
                value: self.table_id.0.to_vec(),
            },
            Mutation::Set {
                key: schema,
                value: self.encode(),
            },
            Mutation::Set {
                key: active_schema_revision,
                value: self.schema_revision_id.0.to_vec(),
            },
            Mutation::Set {
                key: active_primary_index,
                value: self.primary_index_id().0.to_vec(),
            },
            Mutation::Set {
                key: primary_index,
                value: self.schema.primary_key.index.encode(),
            },
        ];
        mutations.extend(
            self.schema
                .unique_constraints
                .iter()
                .map(|unique| Mutation::Set {
                    key: index_definition_key(self.table_id, unique.index.id),
                    value: unique.index.encode(),
                }),
        );
        mutations.extend(self.schema.columns.iter().map(|column| Mutation::Set {
            key: column_name_scope_key(self.table_id, column.name()),
            value: column.id().as_bytes().to_vec(),
        }));
        mutations.push(Mutation::Set {
            key: write_revision.clone(),
            value: self.mutation_id.0.to_vec(),
        });
        mutations.extend(parent_write_revisions.into_iter().map(|key| Mutation::Set {
            key,
            value: self.mutation_id.0.to_vec(),
        }));
        let footprint = guards.footprint();
        Ok(SchemaHomebaseOp {
            mutations,
            footprint,
            guards,
        })
    }

    /// Raise one complete authenticated Homebase batch into a schema change.
    #[cfg(test)]
    pub fn from_homebase(
        batch: &AdmittedBatch<Vec<u8>>,
    ) -> std::result::Result<Self, SchemaCodecError> {
        from_homebase_inner(batch)
    }

    /// Encode this complete schema operation for local durable state.
    pub fn encode(&self) -> Vec<u8> {
        let mut writer = Writer::new();
        writer.u8(SCHEMA_FRAME_VERSION);
        writer
            .field(TAG_MUTATION_ID, &self.mutation_id.0)
            .expect("schema field length must fit in u32");
        writer
            .field(TAG_SQL, self.sql.as_bytes())
            .expect("schema field length must fit in u32");
        writer
            .field(TAG_CREATE_TABLE, &encode_create_table(self))
            .expect("schema field length must fit in u32");
        writer.finish()
    }

    /// Decode and validate one complete locally stored schema operation.
    pub fn decode(frame: &[u8]) -> std::result::Result<Self, SchemaCodecError> {
        let created = decode_frame(frame)?;
        validate_catalog_provenance_sql(&created)?;
        Ok(created)
    }

    /// Decode an initial table-creation operation and bind every SQL spelling
    /// back to its stable structured identity.
    pub fn decode_operation(frame: &[u8]) -> std::result::Result<Self, SchemaCodecError> {
        let created = decode_frame(frame)?;
        validate_initial_provenance_sql(&created)?;
        Ok(created)
    }

    /// Return the exact SQLite spelling of the created table name.
    #[cfg(test)]
    pub fn table_name(&self) -> &str {
        self.name.value()
    }

    /// Return the immutable SQL provenance for this table creation.
    #[cfg(test)]
    pub fn sql(&self) -> &str {
        &self.sql
    }

    /// Render immutable CREATE TABLE provenance against current parent bindings.
    pub fn materialization_sql(&self, connection: &Connection) -> Result<String> {
        self.validate_ir()
            .map_err(|error| Error::InvalidMultiliteOp(error.to_string()))?;
        render_structural_create_table(self, connection)
    }

    pub fn table_id(&self) -> TableId {
        self.table_id
    }

    pub fn schema_revision_id(&self) -> SchemaRevisionId {
        self.schema_revision_id
    }

    pub fn primary_index_id(&self) -> IndexId {
        self.schema.primary_key.index.id()
    }

    pub fn table_name_identity(&self) -> &SqlName {
        &self.name
    }

    pub fn mode(&self) -> TableMode {
        self.schema.mode()
    }

    pub fn storage(&self) -> TableStorage {
        self.schema.storage()
    }

    #[allow(dead_code, reason = "used by future schema-changing operations")]
    pub fn schema(&self) -> &TableSchema {
        &self.schema
    }

    pub fn columns(&self) -> &[Column] {
        self.schema.columns()
    }

    pub fn unique_constraints(&self) -> &[UniqueConstraint] {
        self.schema.unique_constraints()
    }

    pub fn indexes(&self) -> &[NamedIndex] {
        self.schema.indexes()
    }

    pub fn foreign_keys(&self) -> &[ForeignKeyDefinition] {
        self.schema.foreign_keys()
    }

    #[cfg(test)]
    pub fn column_named(&self, name: &SqlName) -> Option<&Column> {
        self.columns()
            .iter()
            .find(|column| column.name.canonical() == name.canonical())
    }

    pub fn prepare_named_index(
        &self,
        connection: &Connection,
        spec: &super::sql::CreateIndexSpec,
    ) -> Result<NamedIndex> {
        let (columns, terms, dependencies) = self.resolve_index_terms(connection, spec)?;
        if spec.unique {
            Ok(NamedIndex::new_unique(spec.name.clone(), columns))
        } else {
            Ok(NamedIndex::new_secondary(
                spec.name.clone(),
                terms,
                spec.predicate.clone(),
                dependencies,
            ))
        }
    }

    pub fn named_index_matches_spec(
        &self,
        index: &NamedIndex,
        spec: &super::sql::CreateIndexSpec,
    ) -> bool {
        if spec.name != *index.name() || spec.unique != index.is_unique() {
            return false;
        }
        if spec.unique {
            index.columns().len() == spec.terms.len()
                && index.terms().is_empty()
                && index.predicate().is_none()
                && spec.predicate.is_none()
                && index
                    .columns()
                    .iter()
                    .zip(&spec.terms)
                    .all(|(encoded, term)| {
                        let super::sql::CreateIndexTerm::Column {
                            name,
                            collation: None,
                            order: None,
                        } = term
                        else {
                            return false;
                        };
                        self.columns().iter().any(|column| {
                            column.id == *encoded && column.name.canonical() == name.canonical()
                        })
                    })
        } else {
            index.columns().is_empty()
                && index.terms().len() == spec.terms.len()
                && index.predicate() == spec.predicate.as_ref()
                && index
                    .terms()
                    .iter()
                    .zip(&spec.terms)
                    .all(|(encoded, parsed)| match (encoded, parsed) {
                        (
                            IndexTerm::Column {
                                column: encoded_column,
                                collation: encoded_collation,
                                order: encoded_order,
                            },
                            super::sql::CreateIndexTerm::Column {
                                name: parsed_name,
                                collation: parsed_collation,
                                order: parsed_order,
                            },
                        ) => {
                            encoded_collation == parsed_collation
                                && encoded_order == parsed_order
                                && self.columns().iter().any(|column| {
                                    column.id == *encoded_column
                                        && column.name.canonical() == parsed_name.canonical()
                                })
                        }
                        (
                            IndexTerm::Expression {
                                expression: encoded_expression,
                                order: encoded_order,
                            },
                            super::sql::CreateIndexTerm::Expression {
                                expression: parsed_expression,
                                order: parsed_order,
                            },
                        ) => {
                            encoded_expression == parsed_expression && encoded_order == parsed_order
                        }
                        _ => false,
                    })
        }
    }

    fn resolve_index_terms(
        &self,
        connection: &Connection,
        spec: &super::sql::CreateIndexSpec,
    ) -> Result<(Vec<ColumnId>, Vec<IndexTerm>, Vec<ColumnId>)> {
        let mut columns = Vec::new();
        let mut terms = Vec::new();
        let mut dependencies = BTreeSet::new();
        for term in &spec.terms {
            match term {
                super::sql::CreateIndexTerm::Column {
                    name,
                    collation,
                    order,
                } => {
                    let column_id = catalog::column_id_by_name(connection, self.table_id(), name)?
                        .ok_or(Error::UnsupportedSql(
                            "CREATE INDEX references an unknown column",
                        ))?;
                    dependencies.insert(column_id);
                    if spec.unique {
                        if collation.is_some() || order.is_some() {
                            return Err(Error::UnsupportedSql(
                                "UNIQUE index collations and ordering are not supported",
                            ));
                        }
                        columns.push(column_id);
                    } else {
                        terms.push(IndexTerm::Column {
                            column: column_id,
                            collation: collation.clone(),
                            order: *order,
                        });
                    }
                }
                super::sql::CreateIndexTerm::Expression { expression, order } => {
                    if spec.unique {
                        return Err(Error::UnsupportedSql(
                            "UNIQUE index expressions are not supported",
                        ));
                    }
                    terms.push(IndexTerm::Expression {
                        expression: expression.clone(),
                        order: *order,
                    });
                    dependencies
                        .extend(resolve_expression_dependencies(expression, self.columns())?);
                }
            }
        }
        if let Some(predicate) = &spec.predicate {
            dependencies.extend(resolve_expression_dependencies(predicate, self.columns())?);
        }
        Ok((columns, terms, dependencies.into_iter().collect()))
    }

    pub fn index_named(&self, name: &SqlName) -> Option<&NamedIndex> {
        self.indexes()
            .iter()
            .find(|index| index.active && index.name.canonical() == name.canonical())
    }

    pub fn with_added_index(&self, index: NamedIndex) -> Result<Self> {
        let mut evolved = self.clone();
        evolved.schema.indexes.push(index);
        evolved.refresh_and_validate_evolution()
    }

    pub fn with_retired_index(&self, name: &SqlName) -> Result<Option<Self>> {
        let mut evolved = self.clone();
        let position = evolved
            .schema
            .indexes
            .iter()
            .position(|index| index.active && index.name.canonical() == name.canonical());
        let Some(position) = position else {
            return Ok(None);
        };
        evolved.schema.indexes[position].active = false;
        evolved.refresh_and_validate_evolution().map(Some)
    }

    pub fn fold_added_index(&self, index: &NamedIndex) -> Result<Self> {
        if !index.is_active()
            || !named_index_definition_is_valid(index, self.columns())
            || self
                .schema
                .indexes
                .iter()
                .any(|current| current.index_id() == index.index_id())
            || self.index_named(index.name()).is_some()
        {
            return Err(Error::InvalidDatabase(
                "index addition contradicts the current table components",
            ));
        }
        let mut folded = self.clone();
        folded.schema.indexes.push(index.clone());
        folded.refresh_and_validate_evolution()
    }

    pub fn fold_retired_index(&self, index: &NamedIndex) -> Result<Self> {
        let mut folded = self.clone();
        let current = folded
            .schema
            .indexes
            .iter_mut()
            .find(|current| current.index_id() == index.index_id())
            .ok_or(Error::InvalidDatabase(
                "index retirement references an unknown identity",
            ))?;
        if current != index || !current.is_active() {
            return Err(Error::InvalidDatabase(
                "index retirement contradicts the current table components",
            ));
        }
        current.active = false;
        folded.refresh_and_validate_evolution()
    }

    pub fn fold_removed_index(&self, index: &NamedIndex) -> Result<Self> {
        let mut folded = self.clone();
        let position = folded
            .schema
            .indexes
            .iter()
            .position(|current| current == index)
            .ok_or(Error::InvalidDatabase(
                "index rollback references an unknown identity",
            ))?;
        folded.schema.indexes.remove(position);
        folded.refresh_and_validate_evolution()
    }

    pub fn fold_restored_index(&self, index: &NamedIndex) -> Result<Self> {
        let mut folded = self.clone();
        let current = folded
            .schema
            .indexes
            .iter_mut()
            .find(|current| current.index_id() == index.index_id())
            .ok_or(Error::InvalidDatabase(
                "index restoration references an unknown identity",
            ))?;
        if current != &index.retired() {
            return Err(Error::InvalidDatabase(
                "index restoration contradicts the current table components",
            ));
        }
        *current = index.clone();
        folded.refresh_and_validate_evolution()
    }

    pub fn with_added_column(
        &self,
        spec: &CreateColumn,
        checks: &[CreateCheckConstraint],
    ) -> Result<(Self, ColumnId)> {
        if self.mode() == TableMode::Strict && spec.declared_type.strict_type().is_none() {
            return Err(Error::UnsupportedSql(
                "STRICT columns must use INT, INTEGER, REAL, TEXT, BLOB, or ANY without size arguments",
            ));
        }
        let id = ColumnId::from_bytes(Uuid::new_v4().into_bytes());
        let evolved = self.with_added_column_identity(id, spec, checks)?;
        Ok((evolved, id))
    }

    pub fn with_added_column_identity(
        &self,
        id: ColumnId,
        spec: &CreateColumn,
        checks: &[CreateCheckConstraint],
    ) -> Result<Self> {
        let mut evolved = self.clone();
        evolved.schema.columns.push(Column {
            id,
            name: spec.name.clone(),
            declared_type: spec.declared_type.clone(),
            not_null: spec.not_null,
            not_null_name: spec.not_null_name.clone(),
            default: spec.default.clone(),
        });
        let checks = checks
            .iter()
            .map(|check| {
                Ok(CheckConstraint {
                    column: check.column.as_ref().map(|_| id),
                    name: check.name.clone(),
                    expression: check.expression.clone(),
                    dependencies: resolve_expression_dependencies(
                        &check.expression,
                        &evolved.schema.columns,
                    )?,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        evolved.schema.checks.extend(checks);
        evolved.refresh_and_validate_evolution()
    }

    /// Names read by constraints introduced with one column.
    ///
    /// These are SQL bindings rather than durable identities: a concurrent
    /// rename must conflict with DDL compiled against the old spelling.
    pub fn added_column_dependencies(&self, column: ColumnId) -> Vec<SqlName> {
        let mut dependencies = BTreeMap::new();
        for check in self
            .schema
            .checks
            .iter()
            .filter(|check| check.column == Some(column))
        {
            for name in check.expression.referenced_columns() {
                dependencies.insert(name.canonical().to_vec(), name);
            }
        }
        dependencies.into_values().collect()
    }

    pub fn column_check_dependencies(&self, column: ColumnId) -> Vec<ColumnId> {
        self.schema
            .checks
            .iter()
            .filter(|check| check.column == Some(column))
            .flat_map(|check| check.dependencies.iter().copied())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect()
    }

    /// Whether introducing this column changes which pre-existing row writes
    /// are valid. Defaults and nullability project deterministically; CHECK
    /// constraints and an unfilled NOT NULL column do not.
    pub fn added_column_changes_write_contract(&self, column: ColumnId) -> bool {
        let Some(column) = self
            .columns()
            .iter()
            .find(|candidate| candidate.id() == column)
        else {
            return true;
        };
        (column.is_not_null() && column.default().is_none())
            || self
                .schema
                .checks
                .iter()
                .any(|check| check.column == Some(column.id()))
    }

    pub fn with_removed_column(&self, column: ColumnId) -> Result<Self> {
        if self.schema.primary_key.columns().contains(&column)
            || self
                .schema
                .unique_constraints
                .iter()
                .any(|unique| unique.columns().contains(&column))
            || self
                .schema
                .indexes
                .iter()
                .any(|index| index.active && index.dependencies().contains(&column))
            || self
                .schema
                .foreign_keys
                .iter()
                .any(|foreign_key| foreign_key.columns().contains(&column))
            || self
                .schema
                .checks
                .iter()
                .any(|check| check.column != Some(column) && check.dependencies.contains(&column))
        {
            return Err(Error::UnsupportedSql(
                "DROP COLUMN does not support key, index, CHECK, or foreign-key dependencies",
            ));
        }
        let mut evolved = self.clone();
        let position = evolved
            .schema
            .columns
            .iter()
            .position(|candidate| candidate.id() == column)
            .ok_or(Error::UnsupportedSql(
                "ALTER TABLE DROP COLUMN references an unknown column",
            ))?;
        evolved.schema.columns.remove(position);
        evolved
            .schema
            .checks
            .retain(|check| check.column != Some(column));
        evolved.refresh_and_validate_evolution()
    }

    /// Fold one independently admitted column addition into the current table.
    pub fn fold_added_column(
        &self,
        source: &Self,
        column: ColumnId,
        order: &[ColumnId],
    ) -> Result<Self> {
        if self.table_id != source.table_id
            || self.mode() != source.mode()
            || self.storage() != source.storage()
            || self.schema.primary_key != source.schema.primary_key
            || self
                .columns()
                .iter()
                .any(|candidate| candidate.id() == column)
        {
            return Err(Error::InvalidDatabase(
                "column addition contradicts the current table components",
            ));
        }
        let added = source
            .columns()
            .iter()
            .find(|candidate| candidate.id() == column)
            .ok_or(Error::InvalidDatabase(
                "column addition is missing its column definition",
            ))?;
        let mut folded = self.clone();
        folded.schema.columns.push(added.clone());
        folded.schema.checks.extend(
            source
                .schema
                .checks
                .iter()
                .filter(|check| check.column == Some(column))
                .cloned(),
        );
        folded.reorder_columns(order)?;
        folded.refresh_and_validate_evolution()
    }

    /// Fold one independently admitted column removal into the current table.
    pub fn fold_removed_column(&self, column: ColumnId, order: &[ColumnId]) -> Result<Self> {
        let mut folded = self.with_removed_column(column)?;
        folded.reorder_columns(order)?;
        folded.refresh_and_validate_evolution()
    }

    /// Refresh expression spellings after one stable column binding moves.
    pub fn fold_renamed_column_expressions(
        &self,
        column: ColumnId,
        old_name: &SqlName,
        new_name: &SqlName,
    ) -> Result<Self> {
        if !self
            .columns()
            .iter()
            .any(|candidate| candidate.id() == column)
        {
            return Err(Error::InvalidDatabase(
                "column rename references an unknown table component",
            ));
        }
        let mut folded = self.clone();
        for check in &mut folded.schema.checks {
            check.expression.rename_column(old_name, new_name);
        }
        let renamed = folded
            .schema
            .columns
            .iter_mut()
            .find(|candidate| candidate.id() == column)
            .expect("column existence was checked");
        if renamed.name.canonical() != old_name.canonical() {
            return Err(Error::InvalidDatabase(
                "column rename contradicts the folded schema binding",
            ));
        }
        renamed.name = new_name.clone();
        for index in &mut folded.schema.indexes {
            for term in &mut index.terms {
                if let IndexTerm::Expression { expression, .. } = term {
                    expression.rename_column(old_name, new_name);
                }
            }
            if let Some(predicate) = &mut index.predicate {
                predicate.rename_column(old_name, new_name);
            }
        }
        // The authority conflict head and write contract remain unchanged,
        // but the local folded IR has different bytes and therefore receives
        // a different content-addressed revision.
        folded.refresh_and_validate_evolution()
    }

    fn reorder_columns(&mut self, order: &[ColumnId]) -> Result<()> {
        if order.len() != self.schema.columns.len() {
            return Err(Error::InvalidDatabase(
                "column order contradicts the active table definition",
            ));
        }
        let mut columns = std::mem::take(&mut self.schema.columns)
            .into_iter()
            .map(|column| (column.id(), column))
            .collect::<BTreeMap<_, _>>();
        self.schema.columns = order
            .iter()
            .map(|column| {
                columns.remove(column).ok_or(Error::InvalidDatabase(
                    "column order references an unknown definition",
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        if !columns.is_empty() {
            return Err(Error::InvalidDatabase(
                "column order omits an active definition",
            ));
        }
        Ok(())
    }

    pub(super) fn computed_schema_revision(&self) -> SchemaRevisionId {
        const DOMAIN: &[u8] = b"multilite:folded-schema:v1\0";

        let mut normalized = self.clone();
        normalized.schema_revision_id = SchemaRevisionId([0; 16]);
        let mut hash = Sha256::new();
        hash.update(DOMAIN);
        hash.update(encode_create_table(&normalized));
        let mut revision: [u8; 16] = hash.finalize()[..16]
            .try_into()
            .expect("SHA-256 prefix has a fixed length");
        revision[6] = (revision[6] & 0x0f) | 0x40;
        revision[8] = (revision[8] & 0x3f) | 0x80;
        SchemaRevisionId(revision)
    }

    fn refresh_schema_revision(&mut self) {
        self.schema_revision_id = self.computed_schema_revision();
    }

    fn validate_evolution(self) -> Result<Self> {
        self.validate_ir().map_err(|_| {
            Error::InvalidDatabase("schema evolution produced an invalid table definition")
        })?;
        Ok(self)
    }

    fn refresh_and_validate_evolution(mut self) -> Result<Self> {
        self.refresh_schema_revision();
        self.validate_evolution()
    }

    pub fn primary_key_columns(&self) -> impl Iterator<Item = &Column> {
        self.schema.primary_key().columns().iter().map(|id| {
            self.schema
                .columns
                .iter()
                .find(|column| column.id == *id)
                .expect("validated primary-key column exists")
        })
    }

    pub fn index_definition(&self, index: IndexId) -> Option<&IndexDefinition> {
        std::iter::once(self.schema.primary_key.index())
            .chain(
                self.schema
                    .unique_constraints
                    .iter()
                    .map(UniqueConstraint::index),
            )
            .chain(self.schema.indexes.iter().map(NamedIndex::index))
            .find(|definition| definition.id() == index)
    }

    pub fn foreign_key_target_columns(&self, index: IndexId) -> Option<Vec<&Column>> {
        let definition = self.index_definition(index)?;
        if definition.kind() == IndexKind::Secondary
            || (definition.kind() == IndexKind::Unique
                && self
                    .schema
                    .indexes
                    .iter()
                    .any(|index| index.index().id() == definition.id() && !index.is_active()))
        {
            return None;
        }
        let columns = definition.columns();
        columns
            .iter()
            .map(|id| self.schema.columns.iter().find(|column| column.id == *id))
            .collect()
    }

    fn resolve_foreign_key_target(
        &self,
        referenced: Option<&[ColumnId]>,
    ) -> Option<(IndexId, Vec<&Column>)> {
        let primary = self.primary_index_id();
        if referenced.is_none() {
            return Some((
                primary,
                self.foreign_key_target_columns(primary)
                    .expect("validated primary key exists"),
            ));
        }
        let referenced = referenced.expect("absence was handled above");
        let matches = |columns: &[ColumnId]| columns == referenced;
        if matches(self.schema.primary_key.columns()) {
            return Some((
                primary,
                self.foreign_key_target_columns(primary)
                    .expect("validated primary key exists"),
            ));
        }
        let index = self
            .schema
            .unique_constraints
            .iter()
            .find(|unique| matches(unique.columns()))
            .map(UniqueConstraint::index_id)
            .or_else(|| {
                self.schema
                    .indexes
                    .iter()
                    .find(|index| index.active && index.is_unique() && matches(index.columns()))
                    .map(NamedIndex::index_id)
            })?;
        Some((
            index,
            self.foreign_key_target_columns(index)
                .expect("selected UNIQUE target exists"),
        ))
    }

    pub fn is_rowid_alias(&self, column: ColumnId) -> bool {
        self.schema.storage == TableStorage::Rowid
            && self.schema.primary_key.columns() == [column]
            && self
                .schema
                .columns
                .iter()
                .find(|candidate| candidate.id == column)
                .is_some_and(|candidate| candidate.declared_type.is_exact_integer())
    }

    /// Ensure every stable parent identity still names the declared primary key.
    pub fn validate_foreign_key_parents(&self, connection: &Connection) -> Result<()> {
        for foreign_key in self.foreign_keys() {
            let parent = catalog::by_id(connection, foreign_key.referenced_table)?.ok_or(
                Error::InvalidDatabase("foreign key references an unknown parent table"),
            )?;
            validate_foreign_key_link(self, foreign_key, &parent)?;
        }
        Ok(())
    }
}

fn render_structural_create_table(table: &CreateTable, connection: &Connection) -> Result<String> {
    let owner = catalog::name_by_id(connection, table.table_id())?
        .unwrap_or_else(|| table.table_name_identity().clone());
    let column_names = if catalog::by_id(connection, table.table_id())?.is_some() {
        catalog::column_names(connection, table)?
    } else {
        table
            .columns()
            .iter()
            .map(|column| column.name().clone())
            .collect()
    };
    let name_for = |id: ColumnId| {
        table
            .columns()
            .iter()
            .position(|column| column.id() == id)
            .and_then(|position| column_names.get(position))
            .ok_or(Error::InvalidDatabase(
                "schema constraint references an unknown column identity",
            ))
    };

    let mut declarations = Vec::new();
    for (column, name) in table.columns().iter().zip(&column_names) {
        let mut declaration = format!(
            "{} {}",
            quote_identifier(name.value()),
            column.declared_type().to_sql()
        );
        if column.is_not_null() {
            push_constraint_name(&mut declaration, column.not_null_name());
            declaration.push_str(" NOT NULL");
        }
        if let Some(default) = column.default() {
            push_constraint_name(&mut declaration, default.name.as_ref());
            declaration.push_str(" DEFAULT ");
            declaration.push_str(&default.expression.to_string());
        }
        declarations.push(declaration);
    }

    let primary = table
        .schema
        .primary_key
        .columns()
        .iter()
        .map(|id| name_for(*id).map(|name| quote_identifier(name.value())))
        .collect::<Result<Vec<_>>>()?
        .join(", ");
    let mut declaration = String::new();
    push_constraint_name(&mut declaration, table.schema.primary_key.name());
    declaration.push_str(" PRIMARY KEY (");
    declaration.push_str(&primary);
    declaration.push(')');
    declarations.push(declaration.trim_start().to_owned());

    for unique in table.unique_constraints() {
        let columns = unique
            .columns()
            .iter()
            .map(|id| name_for(*id).map(|name| quote_identifier(name.value())))
            .collect::<Result<Vec<_>>>()?
            .join(", ");
        let mut declaration = String::new();
        push_constraint_name(&mut declaration, unique.name.as_ref());
        declaration.push_str(" UNIQUE (");
        declaration.push_str(&columns);
        declaration.push(')');
        declarations.push(declaration.trim_start().to_owned());
    }

    for foreign_key in table.foreign_keys() {
        let child = foreign_key
            .columns()
            .iter()
            .map(|id| name_for(*id).map(|name| quote_identifier(name.value())))
            .collect::<Result<Vec<_>>>()?
            .join(", ");
        let parent = catalog::by_id(connection, foreign_key.referenced_table())?.ok_or(
            Error::InvalidDatabase("foreign key references an unknown parent table"),
        )?;
        let parent_name = catalog::name_by_id(connection, foreign_key.referenced_table())?.ok_or(
            Error::InvalidDatabase("foreign key parent has no current name binding"),
        )?;
        let parent_columns = foreign_key
            .referenced_columns()
            .iter()
            .map(|id| {
                catalog::column_name_by_id(connection, parent.table_id(), *id)?.ok_or(
                    Error::InvalidDatabase("foreign key parent column has no current name binding"),
                )
            })
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .map(|name| quote_identifier(name.value()))
            .collect::<Vec<_>>()
            .join(", ");
        let mut declaration = String::new();
        push_constraint_name(&mut declaration, foreign_key.name.as_ref());
        declaration.push_str(" FOREIGN KEY (");
        declaration.push_str(&child);
        declaration.push_str(") REFERENCES ");
        declaration.push_str(&quote_identifier(parent_name.value()));
        declaration.push_str(" (");
        declaration.push_str(&parent_columns);
        declaration.push(')');
        declarations.push(declaration.trim_start().to_owned());
    }

    for check in table.schema.checks() {
        let mut declaration = String::new();
        push_constraint_name(&mut declaration, check.name.as_ref());
        declaration.push_str(" CHECK (");
        declaration.push_str(&check.expression.to_string());
        declaration.push(')');
        declarations.push(declaration.trim_start().to_owned());
    }

    let mut sql = format!(
        "CREATE TABLE {} ({})",
        quote_identifier(owner.value()),
        declarations.join(", ")
    );
    match (table.storage(), table.mode()) {
        (TableStorage::Rowid, TableMode::Ordinary) => {}
        (TableStorage::WithoutRowid, TableMode::Ordinary) => sql.push_str(" WITHOUT ROWID"),
        (TableStorage::Rowid, TableMode::Strict) => sql.push_str(" STRICT"),
        (TableStorage::WithoutRowid, TableMode::Strict) => sql.push_str(" WITHOUT ROWID, STRICT"),
    }
    Ok(sql)
}

fn push_constraint_name(sql: &mut String, name: Option<&SqlName>) {
    if let Some(name) = name {
        sql.push_str(" CONSTRAINT ");
        sql.push_str(&quote_identifier(name.value()));
    }
}

fn push_index_order(sql: &mut String, order: Option<IndexOrder>) {
    match order {
        Some(IndexOrder::Asc) => sql.push_str(" ASC"),
        Some(IndexOrder::Desc) => sql.push_str(" DESC"),
        None => {}
    }
}

struct ResolvedForeignKey {
    spec: CreateForeignKey,
    parent: CreateTable,
    target: IndexId,
}

fn resolve_foreign_keys(
    connection: &Connection,
    spec: &CreateTableSpec,
) -> Result<Vec<ResolvedForeignKey>> {
    spec.foreign_keys
        .iter()
        .cloned()
        .map(|foreign_key| {
            if foreign_key.referenced_table.canonical() == spec.name.canonical() {
                return Err(Error::UnsupportedSql(
                    "self-referential foreign keys are not supported",
                ));
            }
            let parent = catalog::by_name(connection, foreign_key.referenced_table.value())?
                .ok_or(Error::UnsupportedSql(
                    "foreign-key parent must already be a synchronized table",
                ))?;
            let referenced_columns = foreign_key
                .referenced_columns
                .as_ref()
                .map(|columns| {
                    columns
                        .iter()
                        .map(|name| {
                            catalog::column_id_by_name(connection, parent.table_id(), name)?.ok_or(
                                Error::UnsupportedSql(
                                    "foreign key references an unknown parent column",
                                ),
                            )
                        })
                        .collect::<Result<Vec<_>>>()
                })
                .transpose()?;
            let (target, parent_columns) = parent
                .resolve_foreign_key_target(referenced_columns.as_deref())
                .ok_or(Error::UnsupportedSql(
                    "foreign keys must reference a complete primary or UNIQUE key in order",
                ))?;
            if foreign_key.columns.len() != parent_columns.len() {
                return Err(Error::UnsupportedSql(
                    "foreign keys must reference a complete primary or UNIQUE key in order",
                ));
            }
            if foreign_key
                .columns
                .iter()
                .zip(&parent_columns)
                .any(|(child_name, parent_column)| {
                    spec.columns
                        .iter()
                        .find(|column| column.name.canonical() == child_name.canonical())
                        .is_none_or(|child_column| {
                            child_column.declared_type.affinity_for(spec.mode)
                                != parent_column.affinity(parent.mode())
                        })
                })
            {
                return Err(Error::UnsupportedSql(
                    "foreign-key child and parent columns must have matching affinities",
                ));
            }
            Ok(ResolvedForeignKey {
                spec: foreign_key,
                parent,
                target,
            })
        })
        .collect()
}

fn validate_foreign_reference_key_shapes(
    spec: &CreateTableSpec,
    resolved: &[ResolvedForeignKey],
) -> Result<()> {
    let child_key_parts = spec
        .columns
        .iter()
        .filter(|column| column.primary_key.is_some())
        .count();
    for foreign_key in resolved {
        let parent_key_parts = foreign_key
            .parent
            .foreign_key_target_columns(foreign_key.target)
            .expect("resolved foreign-key target exists")
            .len();
        if !foreign_reference_key_fits(parent_key_parts, child_key_parts) {
            return Err(Error::UnsupportedSql(
                "foreign-key reference key exceeds the Homebase component limit",
            ));
        }
    }
    Ok(())
}

fn validate_foreign_key_link(
    child: &CreateTable,
    foreign_key: &ForeignKeyDefinition,
    parent: &CreateTable,
) -> Result<()> {
    let parent_columns = parent
        .foreign_key_target_columns(foreign_key.referenced_index)
        .ok_or(Error::InvalidDatabase(
            "foreign key target is no longer active in the parent schema",
        ))?;
    if !foreign_reference_key_fits(parent_columns.len(), child.primary_key_columns().count()) {
        return Err(Error::InvalidDatabase(
            "foreign-key reference key exceeds the Homebase component limit",
        ));
    }
    if parent_columns
        .iter()
        .copied()
        .map(Column::id)
        .ne(foreign_key.referenced_columns.iter().copied())
        || parent_columns
            .iter()
            .copied()
            .map(Column::name)
            .ne(foreign_key.referenced_column_names.iter())
    {
        return Err(Error::InvalidDatabase(
            "foreign key parent identity contradicts the schema catalog",
        ));
    }
    let child_columns = foreign_key
        .columns
        .iter()
        .map(|id| child.columns().iter().find(|column| column.id() == *id));
    if child_columns
        .zip(parent_columns)
        .any(|(child_column, parent_column)| {
            child_column.is_none_or(|child_column| {
                child_column.affinity(child.mode()) != parent_column.affinity(parent.mode())
            })
        })
    {
        return Err(Error::InvalidDatabase(
            "foreign key child identity contradicts the schema catalog",
        ));
    }
    Ok(())
}

fn foreign_reference_key_fits(parent_key_parts: usize, child_key_parts: usize) -> bool {
    codes::FOREIGN_REFERENCE_KEY_FIXED_COMPONENTS
        .checked_add(parent_key_parts)
        .and_then(|components| components.checked_add(child_key_parts))
        .is_some_and(|components| components <= MAX_COMPONENTS)
}

/// Validate links whose correctness depends on more than one catalog row.
pub(super) fn validate_foreign_key_graph(tables: &[CreateTable]) -> Result<()> {
    let mut relationships = BTreeSet::new();
    for child in tables {
        for foreign_key in child.foreign_keys() {
            let parent = tables
                .iter()
                .find(|table| table.table_id() == foreign_key.referenced_table())
                .ok_or(Error::InvalidDatabase(
                    "foreign key references an unknown parent table",
                ))?;
            let identity = (
                foreign_key.referenced_table().as_bytes(),
                foreign_key.id().as_bytes(),
            );
            if !relationships.insert(identity) {
                return Err(Error::InvalidDatabase(
                    "schema catalog contains duplicate foreign-key identities",
                ));
            }
            validate_foreign_key_link(child, foreign_key, parent)?;
        }
    }
    Ok(())
}

fn build_create_table(
    sql: &str,
    spec: CreateTableSpec,
    resolved_foreign_keys: Vec<ResolvedForeignKey>,
    mut mint: impl FnMut() -> [u8; 16],
) -> Result<CreateTable> {
    let CreateTableSpec {
        name,
        mode,
        storage,
        columns: column_specs,
        primary_key_name,
        unique_constraints: unique_specs,
        foreign_keys: _,
        checks: check_specs,
    } = spec;
    let mutation_id = MutationId(mint());
    let table_id = TableId(mint());
    let schema_revision_id = SchemaRevisionId([0; 16]);
    let row_index_id = IndexId(mint());
    let columns = column_specs
        .iter()
        .map(|column| Column {
            id: ColumnId(mint()),
            name: column.name.clone(),
            declared_type: column.declared_type.clone(),
            not_null: column.not_null,
            not_null_name: column.not_null_name.clone(),
            default: column.default.clone(),
        })
        .collect::<Vec<_>>();
    let primary_columns = spec_primary_key_ids_from_columns(&column_specs, &columns).ok_or(
        Error::CaptureInvariant("validated PRIMARY KEY columns could not be resolved"),
    )?;
    let primary_key = PrimaryKey {
        name: primary_key_name,
        index: IndexDefinition {
            id: row_index_id,
            kind: IndexKind::Primary,
            columns: primary_columns,
        },
    };
    let checks = lower_checks(check_specs, &columns)?;
    let unique_constraints = unique_specs
        .into_iter()
        .map(|unique| {
            let resolved = unique
                .columns
                .into_iter()
                .map(|name| {
                    columns
                        .iter()
                        .find(|column| column.name.canonical() == name.canonical())
                        .map(Column::id)
                        .ok_or(Error::CaptureInvariant(
                            "validated UNIQUE column could not be resolved",
                        ))
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(UniqueConstraint {
                name: unique.name,
                index: IndexDefinition {
                    id: IndexId(mint()),
                    kind: IndexKind::Unique,
                    columns: resolved,
                },
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let foreign_keys = resolved_foreign_keys
        .into_iter()
        .map(|resolved| {
            let parent_columns = resolved
                .parent
                .foreign_key_target_columns(resolved.target)
                .ok_or(Error::CaptureInvariant(
                    "resolved foreign-key target disappeared during lowering",
                ))?;
            let child_columns = resolved
                .spec
                .columns
                .into_iter()
                .map(|name| {
                    columns
                        .iter()
                        .find(|column| column.name.canonical() == name.canonical())
                        .map(Column::id)
                        .ok_or(Error::CaptureInvariant(
                            "validated FOREIGN KEY child column could not be resolved",
                        ))
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(ForeignKeyDefinition {
                id: ForeignKeyId(mint()),
                name: resolved.spec.name,
                columns: child_columns,
                referenced_table: resolved.parent.table_id(),
                referenced_table_name: resolved.parent.table_name_identity().clone(),
                referenced_index: resolved.target,
                referenced_columns: parent_columns.iter().map(|column| column.id()).collect(),
                referenced_column_names: parent_columns
                    .iter()
                    .map(|column| column.name().clone())
                    .collect(),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let schema = TableSchema::try_from_parts(
        table_id,
        mode,
        storage,
        columns,
        primary_key,
        unique_constraints,
        Vec::new(),
        foreign_keys,
        checks,
    )
    .map_err(|error| Error::InvalidMultiliteOp(error.to_string()))?;
    let mut table = CreateTable {
        mutation_id,
        sql: sql.to_owned(),
        table_id,
        schema_revision_id,
        name,
        schema,
    };
    table.refresh_schema_revision();
    table
        .validate_ir()
        .map_err(|error| Error::InvalidMultiliteOp(error.to_string()))?;
    Ok(table)
}

fn lower_checks(
    checks: Vec<CreateCheckConstraint>,
    columns: &[Column],
) -> Result<Vec<CheckConstraint>> {
    checks
        .into_iter()
        .map(|check| {
            let column = check
                .column
                .as_ref()
                .map(|name| {
                    columns
                        .iter()
                        .find(|column| column.name.canonical() == name.canonical())
                        .map(Column::id)
                        .ok_or(Error::UnsupportedSql(
                            "CHECK constraint owner is not a table column",
                        ))
                })
                .transpose()?;
            let dependencies = resolve_expression_dependencies(&check.expression, columns)?;
            Ok(CheckConstraint {
                column,
                name: check.name,
                dependencies,
                expression: check.expression,
            })
        })
        .collect()
}

fn resolve_expression_dependencies(
    expression: &SqlExpression,
    columns: &[Column],
) -> Result<Vec<ColumnId>> {
    compiler::bind_expression(expression, columns).map_err(|error| match error {
        SchemaInvariantError::UnknownExpressionColumn => {
            Error::UnsupportedSql("schema expression references an unknown column")
        }
        error => Error::InvalidMultiliteOp(error.to_string()),
    })
}

fn spec_primary_key_ids_from_columns(
    specs: &[CreateColumn],
    columns: &[Column],
) -> Option<Vec<ColumnId>> {
    let mut ordered = specs
        .iter()
        .enumerate()
        .filter_map(|(column_index, column)| {
            column
                .primary_key
                .map(|position| (position, columns[column_index].id))
        })
        .collect::<Vec<_>>();
    ordered.sort_by_key(|(position, _)| *position);
    if ordered.is_empty()
        || ordered
            .iter()
            .enumerate()
            .any(|(position, (actual, _))| *actual != position)
    {
        return None;
    }
    Some(ordered.into_iter().map(|(_, id)| id).collect())
}

pub fn schema_log_key(id: MutationId) -> Key {
    LogicalTarget::SchemaLog { mutation: id }
        .render()
        .expect("schema log components are bounded and non-empty")
}

/// One SQLite schema-object name shared by tables, indexes, views, and triggers.
///
/// SQLite rejects duplicate names across these object kinds, so every
/// synchronized DDL operation must acquire and release this same cell.
pub fn schema_object_name_scope_key(name: &SqlName) -> Key {
    LogicalTarget::SchemaObjectName {
        canonical: name.canonical().to_vec(),
    }
    .render()
    .expect("schema-object name scope components are bounded and non-empty")
}

pub fn table_schema_key(table: TableId, revision: SchemaRevisionId) -> Key {
    LogicalTarget::TableSchema { table, revision }
        .render()
        .expect("table schema key is bounded")
}

pub fn column_name_scope_key(table: TableId, name: &SqlName) -> Key {
    LogicalTarget::ColumnName {
        table,
        canonical: name.canonical().to_vec(),
    }
    .render()
    .expect("column-name scope components are bounded and non-empty")
}

pub fn column_dependency_prefix(table: TableId, column: ColumnId) -> Key {
    LogicalTarget::ColumnDependencyPrefix { table, column }
        .render()
        .expect("column dependency prefix is bounded")
}

pub fn column_index_dependency_key(table: TableId, column: ColumnId, index: IndexId) -> Key {
    LogicalTarget::ColumnIndexDependency {
        table,
        column,
        index,
    }
    .render()
    .expect("column index dependency key is bounded")
}

pub fn column_check_dependency_key(table: TableId, column: ColumnId, owner: ColumnId) -> Key {
    LogicalTarget::ColumnCheckDependency {
        table,
        column,
        owner,
    }
    .render()
    .expect("column CHECK dependency key is bounded")
}

/// Prefix covering every durable schema and row cell owned by one table.
pub fn table_prefix(table: TableId) -> Key {
    LogicalTarget::TableRoot { table }
        .render()
        .expect("table prefix is bounded")
}

pub fn active_primary_index_key(table: TableId) -> Key {
    LogicalTarget::ActivePrimaryIndex { table }
        .render()
        .expect("active primary index key is bounded")
}

pub fn active_schema_revision_key(table: TableId) -> Key {
    LogicalTarget::ActiveSchemaRevision { table }
        .render()
        .expect("active schema revision key is bounded")
}

pub fn index_definition_key(table: TableId, index: IndexId) -> Key {
    LogicalTarget::IndexDefinition { table, index }
        .render()
        .expect("index definition key is bounded")
}

pub fn write_revision_key(table: TableId) -> Key {
    LogicalTarget::WriteRevision { table }
        .render()
        .expect("write revision key is bounded")
}

fn encode_create_table(table: &CreateTable) -> Vec<u8> {
    let mut writer = Writer::new();
    writer
        .field(TAG_TABLE_ID, &table.table_id.0)
        .expect("schema field length must fit in u32");
    writer
        .field(TAG_TABLE_NAME, table.name.value().as_bytes())
        .expect("schema field length must fit in u32");
    writer
        .field(TAG_SCHEMA_REVISION_ID, &table.schema_revision_id.0)
        .expect("schema field length must fit in u32");
    writer
        .field(TAG_TABLE_MODE, &[table.schema.mode.to_u8()])
        .expect("schema field length must fit in u32");
    writer
        .field(TAG_TABLE_STORAGE, &[table.schema.storage.to_u8()])
        .expect("schema field length must fit in u32");
    writer
        .field(
            TAG_PRIMARY_KEY,
            &encode_primary_key(&table.schema.primary_key),
        )
        .expect("schema field length must fit in u32");
    for column in &table.schema.columns {
        writer
            .field(TAG_COLUMN, &encode_column(column))
            .expect("schema field length must fit in u32");
    }
    for unique in &table.schema.unique_constraints {
        writer
            .field(TAG_UNIQUE_CONSTRAINT, &encode_unique_constraint(unique))
            .expect("schema field length must fit in u32");
    }
    for index in &table.schema.indexes {
        writer
            .field(TAG_INDEX_DEFINITION, &encode_named_index(index))
            .expect("schema field length must fit in u32");
    }
    for foreign_key in &table.schema.foreign_keys {
        writer
            .field(
                TAG_FOREIGN_KEY_DEFINITION,
                &encode_foreign_key_definition(foreign_key),
            )
            .expect("schema field length must fit in u32");
    }
    for check in &table.schema.checks {
        writer
            .field(TAG_CHECK_DEFINITION, &encode_check_constraint(check))
            .expect("schema field length must fit in u32");
    }
    writer.finish()
}

fn encode_type_declaration(declaration: &TypeDeclaration) -> Vec<u8> {
    let mut writer = Writer::new();
    writer.u8(TYPE_DECLARATION_FRAME_VERSION);
    writer
        .field(TAG_TYPE_NAME, declaration.name.as_bytes())
        .expect("type declaration field length must fit in u32");
    for argument in &declaration.arguments {
        writer
            .field(TAG_TYPE_ARGUMENT, argument.as_bytes())
            .expect("type declaration field length must fit in u32");
    }
    writer.finish()
}

fn encode_column(column: &Column) -> Vec<u8> {
    let mut writer = Writer::new();
    writer
        .field(TAG_COLUMN_ID, &column.id.0)
        .expect("schema field length must fit in u32");
    writer
        .field(TAG_COLUMN_NAME, column.name.value().as_bytes())
        .expect("schema field length must fit in u32");
    writer
        .field(
            TAG_COLUMN_TYPE,
            &encode_type_declaration(&column.declared_type),
        )
        .expect("schema field length must fit in u32");
    let mut flags = 0;
    if column.not_null {
        flags |= COLUMN_NOT_NULL;
    }
    writer
        .field(TAG_COLUMN_FLAGS, &[flags])
        .expect("schema field length must fit in u32");
    if let Some(name) = &column.not_null_name {
        writer
            .field(TAG_COLUMN_NOT_NULL_NAME, name.value().as_bytes())
            .expect("schema field length must fit in u32");
    }
    if let Some(default) = &column.default {
        writer
            .field(TAG_COLUMN_DEFAULT, &encode_default(default))
            .expect("schema field length must fit in u32");
    }
    writer.finish()
}

fn encode_primary_key(primary_key: &PrimaryKey) -> Vec<u8> {
    let mut writer = Writer::new();
    writer
        .field(TAG_PRIMARY_INDEX, &primary_key.index.encode())
        .expect("primary-key field length must fit in u32");
    if let Some(name) = &primary_key.name {
        writer
            .field(TAG_PRIMARY_NAME, name.value().as_bytes())
            .expect("primary-key field length must fit in u32");
    }
    writer.finish()
}

fn encode_default(default: &DefaultDefinition) -> Vec<u8> {
    let mut writer = Writer::new();
    if let Some(name) = &default.name {
        writer
            .field(TAG_DEFAULT_NAME, name.value().as_bytes())
            .expect("default field length must fit in u32");
    }
    writer
        .field(
            TAG_DEFAULT_EXPRESSION,
            default.expression.to_string().as_bytes(),
        )
        .expect("default field length must fit in u32");
    writer.finish()
}

fn encode_check_constraint(check: &CheckConstraint) -> Vec<u8> {
    let mut writer = Writer::new();
    if let Some(column) = check.column {
        writer
            .field(TAG_CHECK_COLUMN, &column.as_bytes())
            .expect("CHECK field length must fit in u32");
    }
    if let Some(name) = &check.name {
        writer
            .field(TAG_CHECK_NAME, name.value().as_bytes())
            .expect("CHECK field length must fit in u32");
    }
    writer
        .field(
            TAG_CHECK_EXPRESSION,
            check.expression.to_string().as_bytes(),
        )
        .expect("CHECK field length must fit in u32");
    for dependency in &check.dependencies {
        writer
            .field(TAG_CHECK_DEPENDENCY, &dependency.as_bytes())
            .expect("CHECK dependency field length must fit in u32");
    }
    writer.finish()
}

fn encode_unique_constraint(unique: &UniqueConstraint) -> Vec<u8> {
    let mut writer = Writer::new();
    writer
        .field(TAG_UNIQUE_INDEX_DEFINITION, &unique.index.encode())
        .expect("schema field length must fit in u32");
    if let Some(name) = &unique.name {
        writer
            .field(TAG_UNIQUE_NAME, name.value().as_bytes())
            .expect("schema field length must fit in u32");
    }
    writer.finish()
}

fn encode_logical_index(index: &IndexDefinition) -> Vec<u8> {
    let mut writer = Writer::new();
    writer.u8(INDEX_DEFINITION_FRAME_VERSION);
    writer
        .field(TAG_INDEX_ID, &index.id.0)
        .expect("index definition field length must fit in u32");
    writer
        .field(TAG_INDEX_KIND, &[index.kind.to_u8()])
        .expect("index definition field length must fit in u32");
    for column in &index.columns {
        writer
            .field(TAG_INDEX_COLUMN_ID, &column.0)
            .expect("index definition field length must fit in u32");
    }
    writer.finish()
}

fn encode_named_index(index: &NamedIndex) -> Vec<u8> {
    let mut writer = Writer::new();
    writer
        .field(TAG_NAMED_INDEX_DEFINITION, &index.index.encode())
        .expect("index field length must fit in u32");
    writer
        .field(TAG_INDEX_NAME, index.name.value().as_bytes())
        .expect("index field length must fit in u32");
    writer
        .field(TAG_INDEX_ACTIVE, &[u8::from(index.active)])
        .expect("index field length must fit in u32");
    for term in &index.terms {
        writer
            .field(TAG_INDEX_TERM, &encode_index_term(term))
            .expect("index term field length must fit in u32");
    }
    if let Some(predicate) = &index.predicate {
        writer
            .field(TAG_INDEX_PREDICATE, predicate.to_string().as_bytes())
            .expect("index predicate field length must fit in u32");
    }
    for dependency in &index.dependencies {
        writer
            .field(TAG_INDEX_DEPENDENCY, &dependency.as_bytes())
            .expect("index dependency field length must fit in u32");
    }
    writer.finish()
}

fn encode_index_term(term: &IndexTerm) -> Vec<u8> {
    let mut writer = Writer::new();
    writer.u8(INDEX_TERM_FRAME_VERSION);
    let order = match term {
        IndexTerm::Column { column, .. } => {
            writer
                .field(TAG_INDEX_TERM_COLUMN, &column.0)
                .expect("index column field length must fit in u32");
            if let IndexTerm::Column {
                collation: Some(collation),
                ..
            } = term
            {
                writer
                    .field(TAG_INDEX_TERM_COLLATION, collation.value().as_bytes())
                    .expect("index collation field length must fit in u32");
            }
            match term {
                IndexTerm::Column { order, .. } => *order,
                IndexTerm::Expression { .. } => unreachable!(),
            }
        }
        IndexTerm::Expression { expression, order } => {
            writer
                .field(TAG_INDEX_TERM_EXPRESSION, expression.to_string().as_bytes())
                .expect("index expression field length must fit in u32");
            *order
        }
    };
    if let Some(order) = order {
        writer
            .field(
                TAG_INDEX_TERM_ORDER,
                &[match order {
                    IndexOrder::Asc => INDEX_ORDER_ASC,
                    IndexOrder::Desc => INDEX_ORDER_DESC,
                }],
            )
            .expect("index order field length must fit in u32");
    }
    writer.finish()
}

fn encode_foreign_key_definition(foreign_key: &ForeignKeyDefinition) -> Vec<u8> {
    let mut writer = Writer::new();
    writer
        .field(TAG_FOREIGN_KEY_ID, &foreign_key.id.0)
        .expect("foreign-key field length must fit in u32");
    if let Some(name) = &foreign_key.name {
        writer
            .field(TAG_FOREIGN_KEY_NAME, name.value().as_bytes())
            .expect("foreign-key field length must fit in u32");
    }
    for column in &foreign_key.columns {
        writer
            .field(TAG_FOREIGN_KEY_COLUMN_ID, &column.0)
            .expect("foreign-key field length must fit in u32");
    }
    writer
        .field(
            TAG_FOREIGN_KEY_PARENT_TABLE_ID,
            &foreign_key.referenced_table.0,
        )
        .expect("foreign-key field length must fit in u32");
    writer
        .field(
            TAG_FOREIGN_KEY_PARENT_TABLE_NAME,
            foreign_key.referenced_table_name.value().as_bytes(),
        )
        .expect("foreign-key field length must fit in u32");
    writer
        .field(
            TAG_FOREIGN_KEY_PARENT_INDEX_ID,
            &foreign_key.referenced_index.as_bytes(),
        )
        .expect("foreign-key field length must fit in u32");
    for (column, name) in foreign_key
        .referenced_columns
        .iter()
        .zip(&foreign_key.referenced_column_names)
    {
        writer
            .field(TAG_FOREIGN_KEY_PARENT_COLUMN_ID, &column.0)
            .expect("foreign-key field length must fit in u32");
        writer
            .field(TAG_FOREIGN_KEY_PARENT_COLUMN_NAME, name.value().as_bytes())
            .expect("foreign-key field length must fit in u32");
    }
    writer.finish()
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SchemaCodecError {
    UnknownVersion,
    Truncated,
    DuplicateField,
    MissingField(u8),
    InvalidLength,
    InvalidUtf8,
    InvalidColumnType,
    InvalidColumnFlags(u8),
    InvalidTableMode(u8),
    InvalidTableStorage(u8),
    InvalidInvariant(SchemaInvariantError),
    InvalidSchema,
    InvalidUuid,
    InvalidSql,
    SqlMismatch,
    #[cfg(test)]
    InvalidBatch,
}

impl fmt::Display for SchemaCodecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownVersion => f.write_str("unknown schema frame version"),
            Self::Truncated => f.write_str("truncated schema frame"),
            Self::DuplicateField => f.write_str("duplicate schema field"),
            Self::MissingField(tag) => write!(f, "missing schema field {tag}"),
            Self::InvalidLength => f.write_str("invalid schema field length"),
            Self::InvalidUtf8 => f.write_str("schema name or SQL is not UTF-8"),
            Self::InvalidColumnType => f.write_str("invalid column type declaration"),
            Self::InvalidColumnFlags(value) => write!(f, "invalid column flags {value}"),
            Self::InvalidTableMode(value) => write!(f, "invalid table mode {value}"),
            Self::InvalidTableStorage(value) => write!(f, "invalid table storage {value}"),
            Self::InvalidInvariant(error) => write!(f, "invalid structured schema: {error}"),
            Self::InvalidSchema => f.write_str("invalid structured schema"),
            Self::InvalidUuid => f.write_str("schema id is not a UUID v4"),
            Self::InvalidSql => f.write_str("literal SQL is outside the supported grammar"),
            Self::SqlMismatch => f.write_str("literal SQL contradicts the structured schema"),
            #[cfg(test)]
            Self::InvalidBatch => f.write_str("admitted schema mutation has an invalid envelope"),
        }
    }
}

fn decode_frame(frame: &[u8]) -> std::result::Result<CreateTable, SchemaCodecError> {
    use homebase_core::reader::Reader;

    let mut reader = Reader::new(frame);
    if reader.u8() != Some(SCHEMA_FRAME_VERSION) {
        return Err(SchemaCodecError::UnknownVersion);
    }
    let mut mutation_id = None;
    let mut sql = None;
    let mut create_table = None;
    while let Some((tag, value)) = reader.field().map_err(|_| SchemaCodecError::Truncated)? {
        match tag {
            TAG_MUTATION_ID => {
                set_once(&mut mutation_id, MutationId(uuid_bytes(value)?))?;
            }
            TAG_SQL => set_once(
                &mut sql,
                String::from_utf8(value.to_vec()).map_err(|_| SchemaCodecError::InvalidUtf8)?,
            )?,
            TAG_CREATE_TABLE => {
                set_once(&mut create_table, decode_create_table(value)?)?;
            }
            _ => {}
        }
    }
    let mutation_id = mutation_id.ok_or(SchemaCodecError::MissingField(TAG_MUTATION_ID))?;
    let sql = sql.ok_or(SchemaCodecError::MissingField(TAG_SQL))?;
    let (table_id, schema_revision_id, name, schema) =
        create_table.ok_or(SchemaCodecError::MissingField(TAG_CREATE_TABLE))?;
    CreateTable::try_from_parts(mutation_id, sql, table_id, schema_revision_id, name, schema)
        .map_err(SchemaCodecError::InvalidInvariant)
}

fn set_once<T>(slot: &mut Option<T>, value: T) -> std::result::Result<(), SchemaCodecError> {
    if slot.replace(value).is_some() {
        Err(SchemaCodecError::DuplicateField)
    } else {
        Ok(())
    }
}

fn uuid_bytes(value: &[u8]) -> std::result::Result<[u8; 16], SchemaCodecError> {
    let bytes = value
        .try_into()
        .map_err(|_| SchemaCodecError::InvalidLength)?;
    let uuid = Uuid::from_bytes(bytes);
    if uuid.get_version() != Some(Version::Random) || uuid.get_variant() != Variant::RFC4122 {
        return Err(SchemaCodecError::InvalidUuid);
    }
    Ok(bytes)
}

fn named_index_definition_is_valid(index: &NamedIndex, columns: &[Column]) -> bool {
    let shape_is_valid = match index.index.kind {
        IndexKind::Unique => {
            !index.columns().is_empty()
                && index_columns_supported(IndexKind::Unique, index.columns().len())
                && index.terms().is_empty()
                && index.predicate().is_none()
                && !index
                    .columns()
                    .iter()
                    .enumerate()
                    .any(|(position, column)| {
                        index.columns()[..position].contains(column)
                            || !columns.iter().any(|candidate| candidate.id == *column)
                    })
        }
        IndexKind::Secondary => {
            index.columns().is_empty()
                && !index.terms().is_empty()
                && index.terms().len() <= MAX_INDEX_COLUMNS
                && index.terms().iter().all(|term| match term {
                    IndexTerm::Column { column, .. } => {
                        columns.iter().any(|candidate| candidate.id == *column)
                    }
                    IndexTerm::Expression { .. } => true,
                })
        }
        IndexKind::Primary => false,
    };
    shape_is_valid && index_dependencies_match(index, columns)
}

fn index_dependencies_match(index: &NamedIndex, columns: &[Column]) -> bool {
    let mut expected = BTreeSet::new();
    expected.extend(index.columns().iter().copied());
    for term in index.terms() {
        match term {
            IndexTerm::Column { column, .. } => {
                expected.insert(*column);
            }
            IndexTerm::Expression { expression, .. } => {
                let Ok(dependencies) = resolve_expression_dependencies(expression, columns) else {
                    return false;
                };
                expected.extend(dependencies);
            }
        }
    }
    if let Some(predicate) = index.predicate() {
        let Ok(dependencies) = resolve_expression_dependencies(predicate, columns) else {
            return false;
        };
        expected.extend(dependencies);
    }
    index.dependencies == expected.into_iter().collect::<Vec<_>>()
}

fn decode_create_table(
    frame: &[u8],
) -> std::result::Result<(TableId, SchemaRevisionId, SqlName, TableSchema), SchemaCodecError> {
    use homebase_core::reader::Reader;

    let mut reader = Reader::new(frame);
    let mut table_id = None;
    let mut schema_revision_id = None;
    let mut name = None;
    let mut mode = None;
    let mut storage = None;
    let mut primary_key = None;
    let mut columns = Vec::new();
    let mut unique_constraints = Vec::new();
    let mut indexes = Vec::new();
    let mut foreign_keys = Vec::new();
    let mut checks = Vec::new();
    while let Some((tag, value)) = reader.field().map_err(|_| SchemaCodecError::Truncated)? {
        match tag {
            TAG_TABLE_ID => set_once(&mut table_id, TableId(uuid_bytes(value)?))?,
            TAG_TABLE_NAME => set_once(&mut name, decode_name(value)?)?,
            TAG_SCHEMA_REVISION_ID => set_once(
                &mut schema_revision_id,
                SchemaRevisionId(uuid_bytes(value)?),
            )?,
            TAG_TABLE_MODE => {
                let [value] = value else {
                    return Err(SchemaCodecError::InvalidLength);
                };
                set_once(
                    &mut mode,
                    TableMode::from_u8(*value).ok_or(SchemaCodecError::InvalidTableMode(*value))?,
                )?;
            }
            TAG_TABLE_STORAGE => {
                let [value] = value else {
                    return Err(SchemaCodecError::InvalidLength);
                };
                set_once(
                    &mut storage,
                    TableStorage::from_u8(*value)
                        .ok_or(SchemaCodecError::InvalidTableStorage(*value))?,
                )?;
            }
            TAG_PRIMARY_KEY => set_once(&mut primary_key, decode_primary_key(value)?)?,
            TAG_COLUMN => columns.push(decode_column(value)?),
            TAG_UNIQUE_CONSTRAINT => unique_constraints.push(decode_unique_constraint(value)?),
            TAG_INDEX_DEFINITION => indexes.push(decode_named_index(value)?),
            TAG_FOREIGN_KEY_DEFINITION => foreign_keys.push(decode_foreign_key_definition(value)?),
            TAG_CHECK_DEFINITION => checks.push(decode_check_constraint(value)?),
            _ => {}
        }
    }
    let table_id = table_id.ok_or(SchemaCodecError::MissingField(TAG_TABLE_ID))?;
    let schema = TableSchema::try_from_parts(
        table_id,
        mode.ok_or(SchemaCodecError::MissingField(TAG_TABLE_MODE))?,
        storage.ok_or(SchemaCodecError::MissingField(TAG_TABLE_STORAGE))?,
        columns,
        primary_key.ok_or(SchemaCodecError::MissingField(TAG_PRIMARY_KEY))?,
        unique_constraints,
        indexes,
        foreign_keys,
        checks,
    )
    .map_err(SchemaCodecError::InvalidInvariant)?;
    Ok((
        table_id,
        schema_revision_id.ok_or(SchemaCodecError::MissingField(TAG_SCHEMA_REVISION_ID))?,
        name.ok_or(SchemaCodecError::MissingField(TAG_TABLE_NAME))?,
        schema,
    ))
}

fn decode_foreign_key_definition(
    frame: &[u8],
) -> std::result::Result<ForeignKeyDefinition, SchemaCodecError> {
    use homebase_core::reader::Reader;

    let mut reader = Reader::new(frame);
    let mut id = None;
    let mut name = None;
    let mut columns = Vec::new();
    let mut referenced_table = None;
    let mut referenced_table_name = None;
    let mut referenced_index = None;
    let mut referenced_columns = Vec::new();
    let mut referenced_column_names = Vec::new();
    while let Some((tag, value)) = reader.field().map_err(|_| SchemaCodecError::Truncated)? {
        match tag {
            TAG_FOREIGN_KEY_ID => set_once(&mut id, ForeignKeyId(uuid_bytes(value)?))?,
            TAG_FOREIGN_KEY_NAME => set_once(&mut name, decode_name(value)?)?,
            TAG_FOREIGN_KEY_COLUMN_ID => columns.push(ColumnId(uuid_bytes(value)?)),
            TAG_FOREIGN_KEY_PARENT_TABLE_ID => {
                set_once(&mut referenced_table, TableId(uuid_bytes(value)?))?
            }
            TAG_FOREIGN_KEY_PARENT_TABLE_NAME => {
                set_once(&mut referenced_table_name, decode_name(value)?)?
            }
            TAG_FOREIGN_KEY_PARENT_INDEX_ID => {
                set_once(&mut referenced_index, IndexId(uuid_bytes(value)?))?
            }
            TAG_FOREIGN_KEY_PARENT_COLUMN_ID => {
                referenced_columns.push(ColumnId(uuid_bytes(value)?))
            }
            TAG_FOREIGN_KEY_PARENT_COLUMN_NAME => referenced_column_names.push(decode_name(value)?),
            _ => {}
        }
    }
    if columns.is_empty()
        || columns.len() != referenced_columns.len()
        || columns.len() != referenced_column_names.len()
    {
        return Err(SchemaCodecError::InvalidSchema);
    }
    let referenced_index = referenced_index.ok_or(SchemaCodecError::MissingField(
        TAG_FOREIGN_KEY_PARENT_INDEX_ID,
    ))?;
    Ok(ForeignKeyDefinition {
        id: id.ok_or(SchemaCodecError::MissingField(TAG_FOREIGN_KEY_ID))?,
        name,
        columns,
        referenced_table: referenced_table.ok_or(SchemaCodecError::MissingField(
            TAG_FOREIGN_KEY_PARENT_TABLE_ID,
        ))?,
        referenced_table_name: referenced_table_name.ok_or(SchemaCodecError::MissingField(
            TAG_FOREIGN_KEY_PARENT_TABLE_NAME,
        ))?,
        referenced_index,
        referenced_columns,
        referenced_column_names,
    })
}

fn decode_named_index(frame: &[u8]) -> std::result::Result<NamedIndex, SchemaCodecError> {
    use homebase_core::reader::Reader;

    let mut reader = Reader::new(frame);
    let mut index = None;
    let mut name = None;
    let mut active = None;
    let mut terms = Vec::new();
    let mut predicate = None;
    let mut dependencies = Vec::new();
    while let Some((tag, value)) = reader.field().map_err(|_| SchemaCodecError::Truncated)? {
        match tag {
            TAG_NAMED_INDEX_DEFINITION => set_once(&mut index, decode_logical_index(value)?)?,
            TAG_INDEX_NAME => set_once(&mut name, decode_name(value)?)?,
            TAG_INDEX_ACTIVE => {
                let [value] = value else {
                    return Err(SchemaCodecError::InvalidLength);
                };
                let value = match value {
                    0 => false,
                    1 => true,
                    _ => return Err(SchemaCodecError::InvalidSchema),
                };
                set_once(&mut active, value)?;
            }
            TAG_INDEX_TERM => terms.push(decode_index_term(value)?),
            TAG_INDEX_PREDICATE => {
                let expression = std::str::from_utf8(value)
                    .map_err(|_| SchemaCodecError::InvalidUtf8)
                    .and_then(|expression| {
                        super::sql::parse_schema_expression(expression)
                            .map_err(|_| SchemaCodecError::InvalidSql)
                    })?;
                set_once(&mut predicate, expression)?;
            }
            TAG_INDEX_DEPENDENCY => {
                dependencies.push(ColumnId::from_bytes(uuid_bytes(value)?));
            }
            _ => {}
        }
    }
    Ok(NamedIndex {
        index: index.ok_or(SchemaCodecError::MissingField(TAG_NAMED_INDEX_DEFINITION))?,
        name: name.ok_or(SchemaCodecError::MissingField(TAG_INDEX_NAME))?,
        active: active.ok_or(SchemaCodecError::MissingField(TAG_INDEX_ACTIVE))?,
        terms,
        predicate,
        dependencies,
    })
}

fn decode_index_term(frame: &[u8]) -> std::result::Result<IndexTerm, SchemaCodecError> {
    use homebase_core::reader::Reader;

    let mut reader = Reader::new(frame);
    if reader.u8() != Some(INDEX_TERM_FRAME_VERSION) {
        return Err(SchemaCodecError::UnknownVersion);
    }
    let mut column = None;
    let mut collation = None;
    let mut expression = None;
    let mut order = None;
    while let Some((tag, value)) = reader.field().map_err(|_| SchemaCodecError::Truncated)? {
        match tag {
            TAG_INDEX_TERM_COLUMN => {
                set_once(&mut column, ColumnId(uuid_bytes(value)?))?;
            }
            TAG_INDEX_TERM_COLLATION => set_once(&mut collation, decode_name(value)?)?,
            TAG_INDEX_TERM_EXPRESSION => {
                let encoded =
                    std::str::from_utf8(value).map_err(|_| SchemaCodecError::InvalidUtf8)?;
                let parsed = super::sql::parse_schema_expression(encoded)
                    .map_err(|_| SchemaCodecError::InvalidSql)?;
                set_once(&mut expression, parsed)?;
            }
            TAG_INDEX_TERM_ORDER => {
                let [value] = value else {
                    return Err(SchemaCodecError::InvalidLength);
                };
                let decoded = match *value {
                    INDEX_ORDER_ASC => IndexOrder::Asc,
                    INDEX_ORDER_DESC => IndexOrder::Desc,
                    _ => return Err(SchemaCodecError::InvalidSchema),
                };
                set_once(&mut order, decoded)?;
            }
            _ => {}
        }
    }
    match (column, expression) {
        (Some(column), None) => Ok(IndexTerm::Column {
            column,
            collation,
            order,
        }),
        (None, Some(expression)) if collation.is_none() => {
            Ok(IndexTerm::Expression { expression, order })
        }
        _ => Err(SchemaCodecError::InvalidSchema),
    }
}

fn decode_primary_key(frame: &[u8]) -> std::result::Result<PrimaryKey, SchemaCodecError> {
    use homebase_core::reader::Reader;

    let mut reader = Reader::new(frame);
    let mut index = None;
    let mut name = None;
    while let Some((tag, value)) = reader.field().map_err(|_| SchemaCodecError::Truncated)? {
        match tag {
            TAG_PRIMARY_INDEX => set_once(&mut index, decode_logical_index(value)?)?,
            TAG_PRIMARY_NAME => set_once(&mut name, decode_name(value)?)?,
            _ => {}
        }
    }
    let index = index.ok_or(SchemaCodecError::MissingField(TAG_PRIMARY_INDEX))?;
    if index.kind != IndexKind::Primary {
        return Err(SchemaCodecError::InvalidSchema);
    }
    Ok(PrimaryKey { name, index })
}

fn decode_column(frame: &[u8]) -> std::result::Result<Column, SchemaCodecError> {
    use homebase_core::reader::Reader;

    let mut reader = Reader::new(frame);
    let mut id = None;
    let mut name = None;
    let mut declared_type = None;
    let mut flags = None;
    let mut not_null_name = None;
    let mut default = None;
    while let Some((tag, value)) = reader.field().map_err(|_| SchemaCodecError::Truncated)? {
        match tag {
            TAG_COLUMN_ID => set_once(&mut id, ColumnId(uuid_bytes(value)?))?,
            TAG_COLUMN_NAME => set_once(&mut name, decode_name(value)?)?,
            TAG_COLUMN_TYPE => set_once(&mut declared_type, decode_type_declaration(value)?)?,
            TAG_COLUMN_FLAGS => {
                let [value] = value else {
                    return Err(SchemaCodecError::InvalidLength);
                };
                if value & !COLUMN_NOT_NULL != 0 {
                    return Err(SchemaCodecError::InvalidColumnFlags(*value));
                }
                set_once(&mut flags, *value)?;
            }
            TAG_COLUMN_NOT_NULL_NAME => set_once(&mut not_null_name, decode_name(value)?)?,
            TAG_COLUMN_DEFAULT => set_once(&mut default, decode_default(value)?)?,
            _ => {}
        }
    }
    let flags = flags.ok_or(SchemaCodecError::MissingField(TAG_COLUMN_FLAGS))?;
    let not_null = flags & COLUMN_NOT_NULL != 0;
    Ok(Column {
        id: id.ok_or(SchemaCodecError::MissingField(TAG_COLUMN_ID))?,
        name: name.ok_or(SchemaCodecError::MissingField(TAG_COLUMN_NAME))?,
        declared_type: declared_type.ok_or(SchemaCodecError::MissingField(TAG_COLUMN_TYPE))?,
        not_null,
        not_null_name,
        default,
    })
}

fn decode_default(frame: &[u8]) -> std::result::Result<DefaultDefinition, SchemaCodecError> {
    use homebase_core::reader::Reader;

    let mut reader = Reader::new(frame);
    let mut name = None;
    let mut expression = None;
    while let Some((tag, value)) = reader.field().map_err(|_| SchemaCodecError::Truncated)? {
        match tag {
            TAG_DEFAULT_NAME => set_once(&mut name, decode_name(value)?)?,
            TAG_DEFAULT_EXPRESSION => {
                let encoded =
                    std::str::from_utf8(value).map_err(|_| SchemaCodecError::InvalidUtf8)?;
                set_once(
                    &mut expression,
                    super::sql::parse_schema_expression(encoded)
                        .map_err(|_| SchemaCodecError::InvalidSql)?,
                )?;
            }
            _ => {}
        }
    }
    Ok(DefaultDefinition {
        name,
        expression: expression.ok_or(SchemaCodecError::MissingField(TAG_DEFAULT_EXPRESSION))?,
    })
}

fn decode_check_constraint(frame: &[u8]) -> std::result::Result<CheckConstraint, SchemaCodecError> {
    use homebase_core::reader::Reader;

    let mut reader = Reader::new(frame);
    let mut column = None;
    let mut name = None;
    let mut expression = None;
    let mut dependencies = Vec::new();
    while let Some((tag, value)) = reader.field().map_err(|_| SchemaCodecError::Truncated)? {
        match tag {
            TAG_CHECK_COLUMN => set_once(&mut column, ColumnId::from_bytes(uuid_bytes(value)?))?,
            TAG_CHECK_NAME => set_once(&mut name, decode_name(value)?)?,
            TAG_CHECK_EXPRESSION => {
                let encoded =
                    std::str::from_utf8(value).map_err(|_| SchemaCodecError::InvalidUtf8)?;
                set_once(
                    &mut expression,
                    super::sql::parse_schema_expression(encoded)
                        .map_err(|_| SchemaCodecError::InvalidSql)?,
                )?;
            }
            TAG_CHECK_DEPENDENCY => {
                dependencies.push(ColumnId::from_bytes(uuid_bytes(value)?));
            }
            _ => {}
        }
    }
    Ok(CheckConstraint {
        column,
        name,
        expression: expression.ok_or(SchemaCodecError::MissingField(TAG_CHECK_EXPRESSION))?,
        dependencies,
    })
}

fn decode_type_declaration(frame: &[u8]) -> std::result::Result<TypeDeclaration, SchemaCodecError> {
    use homebase_core::reader::Reader;

    let mut reader = Reader::new(frame);
    if reader.u8() != Some(TYPE_DECLARATION_FRAME_VERSION) {
        return Err(SchemaCodecError::InvalidColumnType);
    }
    let mut name = None;
    let mut arguments = Vec::new();
    while let Some((tag, value)) = reader.field().map_err(|_| SchemaCodecError::Truncated)? {
        match tag {
            TAG_TYPE_NAME => set_once(
                &mut name,
                String::from_utf8(value.to_vec()).map_err(|_| SchemaCodecError::InvalidUtf8)?,
            )?,
            TAG_TYPE_ARGUMENT => arguments.push(
                String::from_utf8(value.to_vec()).map_err(|_| SchemaCodecError::InvalidUtf8)?,
            ),
            _ => {}
        }
    }
    let declaration = TypeDeclaration::new(
        name.ok_or(SchemaCodecError::MissingField(TAG_TYPE_NAME))?,
        arguments,
    );
    if declaration.name.is_empty()
        || declaration.arguments.len() > 2
        || declaration.arguments.iter().any(String::is_empty)
        || !type_declaration_roundtrips(&declaration)
    {
        return Err(SchemaCodecError::InvalidColumnType);
    }
    Ok(declaration)
}

fn type_declaration_roundtrips(declaration: &TypeDeclaration) -> bool {
    let sql = format!(
        "CREATE TABLE __multilite_type_probe (
            id INTEGER PRIMARY KEY,
            value {}
        )",
        declaration.to_sql()
    );
    let Ok(super::sql::ValidatedExecute::CreateTable(spec)) = super::sql::validate_execute(&sql)
    else {
        return false;
    };
    matches!(
        spec.columns.as_slice(),
        [_, value]
            if value.name == SqlName::new("value".into())
                && value.declared_type == *declaration
                && !value.not_null
                && value.not_null_name.is_none()
                && value.default.is_none()
                && value.primary_key.is_none()
                && spec.unique_constraints.is_empty()
                && spec.foreign_keys.is_empty()
                && spec.checks.is_empty()
    )
}

fn decode_unique_constraint(
    frame: &[u8],
) -> std::result::Result<UniqueConstraint, SchemaCodecError> {
    use homebase_core::reader::Reader;

    let mut reader = Reader::new(frame);
    let mut index = None;
    let mut name = None;
    while let Some((tag, value)) = reader.field().map_err(|_| SchemaCodecError::Truncated)? {
        match tag {
            TAG_UNIQUE_INDEX_DEFINITION => set_once(&mut index, decode_logical_index(value)?)?,
            TAG_UNIQUE_NAME => set_once(&mut name, decode_name(value)?)?,
            _ => {}
        }
    }
    let index = index.ok_or(SchemaCodecError::MissingField(TAG_UNIQUE_INDEX_DEFINITION))?;
    if index.kind != IndexKind::Unique {
        return Err(SchemaCodecError::InvalidSchema);
    }
    Ok(UniqueConstraint { name, index })
}

fn decode_logical_index(frame: &[u8]) -> std::result::Result<IndexDefinition, SchemaCodecError> {
    use homebase_core::reader::Reader;

    let mut reader = Reader::new(frame);
    if reader.u8() != Some(INDEX_DEFINITION_FRAME_VERSION) {
        return Err(SchemaCodecError::UnknownVersion);
    }
    let mut id = None;
    let mut kind = None;
    let mut columns = Vec::new();
    while let Some((tag, value)) = reader.field().map_err(|_| SchemaCodecError::Truncated)? {
        match tag {
            TAG_INDEX_ID => set_once(&mut id, IndexId(uuid_bytes(value)?))?,
            TAG_INDEX_KIND => {
                let [value] = value else {
                    return Err(SchemaCodecError::InvalidLength);
                };
                set_once(
                    &mut kind,
                    IndexKind::from_u8(*value).ok_or(SchemaCodecError::InvalidSchema)?,
                )?;
            }
            TAG_INDEX_COLUMN_ID => columns.push(ColumnId(uuid_bytes(value)?)),
            _ => {}
        }
    }
    let kind = kind.ok_or(SchemaCodecError::MissingField(TAG_INDEX_KIND))?;
    if (kind == IndexKind::Secondary && !columns.is_empty())
        || (kind != IndexKind::Secondary
            && (columns.is_empty()
                || columns
                    .iter()
                    .enumerate()
                    .any(|(position, column)| columns[..position].contains(column))))
    {
        return Err(SchemaCodecError::InvalidSchema);
    }
    Ok(IndexDefinition {
        id: id.ok_or(SchemaCodecError::MissingField(TAG_INDEX_ID))?,
        kind,
        columns,
    })
}

fn decode_name(value: &[u8]) -> std::result::Result<SqlName, SchemaCodecError> {
    let value = String::from_utf8(value.to_vec()).map_err(|_| SchemaCodecError::InvalidUtf8)?;
    Ok(SqlName::new(value))
}

#[cfg(test)]
fn from_homebase_inner(
    batch: &AdmittedBatch<Vec<u8>>,
) -> std::result::Result<CreateTable, SchemaCodecError> {
    batch
        .validate()
        .map_err(|_| SchemaCodecError::InvalidBatch)?;
    let log_entry = batch
        .entries
        .first()
        .ok_or(SchemaCodecError::InvalidBatch)?;
    let Mutation::Set {
        key: admitted_log_key,
        value: frame,
    } = &log_entry.device_entry.mutation
    else {
        return Err(SchemaCodecError::InvalidBatch);
    };
    let created = CreateTable::decode_operation(frame)?;
    if admitted_log_key != &schema_log_key(created.mutation_id) {
        return Err(SchemaCodecError::InvalidBatch);
    }
    let expected = created
        .to_homebase()
        .map_err(|_| SchemaCodecError::InvalidSchema)?
        .mutations;
    if expected.len() != batch.entries.len()
        || expected
            .iter()
            .zip(&batch.entries)
            .any(|(expected, admitted)| expected != &admitted.device_entry.mutation)
    {
        return Err(SchemaCodecError::InvalidBatch);
    }
    Ok(created)
}

/// Validate the immutable creation provenance retained by an evolved catalog.
///
/// Later DDL is verified by its own operation decoder and mutates only typed
/// IR. The initial SQL can therefore differ from the folded schema after
/// rename/add/drop operations, and is never used to materialize that fold.
fn validate_catalog_provenance_sql(
    created: &CreateTable,
) -> std::result::Result<(), SchemaCodecError> {
    let parsed = parse_create_table(&created.sql)?;
    if parsed.name != created.name {
        return Err(SchemaCodecError::SqlMismatch);
    }
    Ok(())
}

fn validate_initial_provenance_sql(
    created: &CreateTable,
) -> std::result::Result<(), SchemaCodecError> {
    let parsed = parse_create_table(&created.sql)?;
    if parsed.name != created.name
        || parsed.mode != created.schema.mode
        || parsed.storage != created.schema.storage
        || !created.schema.indexes.is_empty()
        || parsed.primary_key_name != created.schema.primary_key.name
        || parsed.columns.len() != created.schema.columns.len()
        || parsed.unique_constraints.len() != created.schema.unique_constraints.len()
        || parsed.foreign_keys.len() != created.schema.foreign_keys.len()
        || parsed.checks.len() != created.schema.checks.len()
    {
        return Err(SchemaCodecError::SqlMismatch);
    }

    for (parsed, encoded) in parsed.columns.iter().zip(&created.schema.columns) {
        let primary_key = created
            .schema
            .primary_key
            .columns()
            .iter()
            .position(|column| *column == encoded.id);
        if parsed.name != encoded.name
            || parsed.declared_type != encoded.declared_type
            || parsed.not_null != encoded.not_null
            || parsed.not_null_name != encoded.not_null_name
            || parsed.default != encoded.default
            || parsed.primary_key != primary_key
        {
            return Err(SchemaCodecError::SqlMismatch);
        }
    }

    for (parsed, encoded) in parsed
        .unique_constraints
        .iter()
        .zip(&created.schema.unique_constraints)
    {
        let columns = encoded
            .columns()
            .iter()
            .map(|column| column_name(created, *column))
            .collect::<Option<Vec<_>>>()
            .ok_or(SchemaCodecError::SqlMismatch)?;
        if parsed.name != encoded.name || parsed.columns.as_slice() != columns.as_slice() {
            return Err(SchemaCodecError::SqlMismatch);
        }
    }

    for (parsed, encoded) in parsed.foreign_keys.iter().zip(&created.schema.foreign_keys) {
        let columns = encoded
            .columns
            .iter()
            .map(|column| column_name(created, *column))
            .collect::<Option<Vec<_>>>()
            .ok_or(SchemaCodecError::SqlMismatch)?;
        if parsed.name != encoded.name
            || parsed.columns.as_slice() != columns.as_slice()
            || parsed.referenced_table.canonical() != encoded.referenced_table_name.canonical()
            || parsed.referenced_columns.as_ref().is_some_and(|columns| {
                columns.len() != encoded.referenced_column_names.len()
                    || columns
                        .iter()
                        .zip(&encoded.referenced_column_names)
                        .any(|(parsed, encoded)| parsed.canonical() != encoded.canonical())
            })
        {
            return Err(SchemaCodecError::SqlMismatch);
        }
    }

    for (parsed, encoded) in parsed.checks.iter().zip(&created.schema.checks) {
        let column = match encoded.column {
            Some(column) => {
                Some(column_name(created, column).ok_or(SchemaCodecError::SqlMismatch)?)
            }
            None => None,
        };
        if parsed.column.as_ref() != column.as_ref()
            || parsed.name != encoded.name
            || parsed.expression != encoded.expression
        {
            return Err(SchemaCodecError::SqlMismatch);
        }
    }
    Ok(())
}

fn column_name(created: &CreateTable, id: ColumnId) -> Option<SqlName> {
    created
        .schema
        .columns
        .iter()
        .find(|column| column.id == id)
        .map(|column| column.name.clone())
}

fn parse_create_table(sql: &str) -> std::result::Result<CreateTableSpec, SchemaCodecError> {
    let super::sql::ValidatedExecute::CreateTable(parsed) =
        super::sql::validate_execute(sql).map_err(|_| SchemaCodecError::InvalidSql)?
    else {
        return Err(SchemaCodecError::InvalidSql);
    };
    Ok(parsed)
}

#[cfg(test)]
mod tests {
    use homebase_core::seal::Seal;
    use homebase_core::tag::{
        AdmissionSeq, AdmissionTag, CipherEpoch, DeviceChecksum, DeviceEntry, DeviceId, DeviceSeq,
        DeviceTag, Ver,
    };

    use super::*;
    use crate::commit::footprint::assert_explicit_range_assertions;

    fn definition(name: &str) -> CreateTableSpec {
        CreateTableSpec {
            name: SqlName::new(name.into()),
            mode: TableMode::Ordinary,
            storage: TableStorage::Rowid,
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
                    not_null: true,
                    not_null_name: None,
                    default: None,
                    primary_key: None,
                },
            ],
            unique_constraints: Vec::new(),
            foreign_keys: Vec::new(),
            primary_key_name: None,
            checks: Vec::new(),
        }
    }

    fn deterministic_create(name: &str) -> CreateTable {
        let mut next = 1_u8;
        build_create_table(
            &format!("CREATE TABLE {name} (id INTEGER PRIMARY KEY, body TEXT NOT NULL)"),
            definition(name),
            Vec::new(),
            || {
                let id = test_uuid(next);
                next += 1;
                id
            },
        )
        .unwrap()
    }

    fn deterministic_unique_create() -> CreateTable {
        let mut next = 1_u8;
        build_create_table(
            "CREATE TABLE accounts (
                id INTEGER PRIMARY KEY,
                organization TEXT,
                email TEXT,
                CONSTRAINT account_email UNIQUE (organization, email)
            )",
            CreateTableSpec {
                name: SqlName::new("accounts".into()),
                mode: TableMode::Ordinary,
                storage: TableStorage::Rowid,
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
                        name: SqlName::new("organization".into()),
                        declared_type: TypeDeclaration::text(),
                        not_null: false,
                        not_null_name: None,
                        default: None,
                        primary_key: None,
                    },
                    CreateColumn {
                        name: SqlName::new("email".into()),
                        declared_type: TypeDeclaration::text(),
                        not_null: false,
                        not_null_name: None,
                        default: None,
                        primary_key: None,
                    },
                ],
                unique_constraints: vec![CreateUnique {
                    name: Some(SqlName::new("account_email".into())),
                    columns: vec![
                        SqlName::new("organization".into()),
                        SqlName::new("email".into()),
                    ],
                }],
                foreign_keys: Vec::new(),
                primary_key_name: None,
                checks: Vec::new(),
            },
            Vec::new(),
            || {
                let id = test_uuid(next);
                next += 1;
                id
            },
        )
        .unwrap()
    }

    fn deterministic_overlapping_unique_create() -> CreateTable {
        let mut next = 1_u8;
        build_create_table(
            "CREATE TABLE profiles (
                id INTEGER PRIMARY KEY,
                tenant TEXT,
                email TEXT CONSTRAINT email_key UNIQUE,
                username TEXT UNIQUE,
                CONSTRAINT tenant_email UNIQUE (tenant, email),
                CONSTRAINT tenant_username UNIQUE (tenant, username)
            )",
            CreateTableSpec {
                name: SqlName::new("profiles".into()),
                mode: TableMode::Ordinary,
                storage: TableStorage::Rowid,
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
                        name: SqlName::new("tenant".into()),
                        declared_type: TypeDeclaration::text(),
                        not_null: false,
                        not_null_name: None,
                        default: None,
                        primary_key: None,
                    },
                    CreateColumn {
                        name: SqlName::new("email".into()),
                        declared_type: TypeDeclaration::text(),
                        not_null: false,
                        not_null_name: None,
                        default: None,
                        primary_key: None,
                    },
                    CreateColumn {
                        name: SqlName::new("username".into()),
                        declared_type: TypeDeclaration::text(),
                        not_null: false,
                        not_null_name: None,
                        default: None,
                        primary_key: None,
                    },
                ],
                unique_constraints: vec![
                    CreateUnique {
                        name: Some(SqlName::new("email_key".into())),
                        columns: vec![SqlName::new("email".into())],
                    },
                    CreateUnique {
                        name: None,
                        columns: vec![SqlName::new("username".into())],
                    },
                    CreateUnique {
                        name: Some(SqlName::new("tenant_email".into())),
                        columns: vec![SqlName::new("tenant".into()), SqlName::new("email".into())],
                    },
                    CreateUnique {
                        name: Some(SqlName::new("tenant_username".into())),
                        columns: vec![
                            SqlName::new("tenant".into()),
                            SqlName::new("username".into()),
                        ],
                    },
                ],
                foreign_keys: Vec::new(),
                primary_key_name: None,
                checks: Vec::new(),
            },
            Vec::new(),
            || {
                let id = test_uuid(next);
                next += 1;
                id
            },
        )
        .unwrap()
    }

    fn test_uuid(byte: u8) -> [u8; 16] {
        let mut id = [byte; 16];
        id[6] = (id[6] & 0x0f) | 0x40;
        id[8] = (id[8] & 0x3f) | 0x80;
        id
    }

    fn admit(mutations: Vec<Mutation>) -> AdmittedBatch<Vec<u8>> {
        let device = DeviceId([7; 16]);
        let device_seq = DeviceSeq(3);
        let admission_seq = AdmissionSeq(9);
        let entries = mutations
            .into_iter()
            .enumerate()
            .map(|(index, mutation)| homebase_core::tag::AdmittedEntry {
                device_entry: DeviceEntry {
                    mutation,
                    tag: DeviceTag {
                        device,
                        device_seq,
                        ver: Ver(index as u64 + 1),
                        cipher_epoch: CipherEpoch(0),
                    },
                    seal: Seal::empty_aead_v1(),
                },
                admission: AdmissionTag {
                    admission_seq,
                    op_index: index as u32,
                },
            })
            .collect();
        AdmittedBatch {
            admission_seq,
            device,
            device_seq,
            checksum: DeviceChecksum::EMPTY,
            entries,
        }
    }

    #[test]
    fn independent_column_folds_have_one_definition_and_revision() {
        let base = deterministic_create("notes");
        let alpha = ColumnId(test_uuid(30));
        let beta = ColumnId(test_uuid(20));
        let added = |name: &str| CreateColumn {
            name: SqlName::new(name.into()),
            declared_type: TypeDeclaration::text(),
            not_null: false,
            not_null_name: None,
            default: None,
            primary_key: None,
        };
        let alpha_source = base
            .with_added_column_identity(alpha, &added("alpha"), &[])
            .unwrap();
        let beta_source = base
            .with_added_column_identity(beta, &added("beta"), &[])
            .unwrap();
        let base_order = base
            .columns()
            .iter()
            .map(|column| column.id())
            .collect::<Vec<_>>();
        let mut alpha_order = base_order.clone();
        alpha_order.push(alpha);
        let mut beta_order = base_order.clone();
        beta_order.push(beta);
        let mut final_order = base_order;
        final_order.extend([beta, alpha]);

        let alpha_then_beta = base
            .fold_added_column(&alpha_source, alpha, &alpha_order)
            .unwrap()
            .fold_added_column(&beta_source, beta, &final_order)
            .unwrap();
        let beta_then_alpha = base
            .fold_added_column(&beta_source, beta, &beta_order)
            .unwrap()
            .fold_added_column(&alpha_source, alpha, &final_order)
            .unwrap();

        assert_eq!(alpha_then_beta, beta_then_alpha);
        assert_eq!(
            alpha_then_beta
                .columns()
                .iter()
                .map(|column| column.name().value())
                .collect::<Vec<_>>(),
            ["id", "body", "beta", "alpha"]
        );
        assert_eq!(
            CreateTable::decode(&alpha_then_beta.encode()).unwrap(),
            alpha_then_beta
        );
    }

    #[test]
    fn independent_column_renames_derive_one_authenticated_catalog_revision() {
        let base = deterministic_create("notes");
        let id = base.columns()[0].id();
        let body = base.columns()[1].id();
        let id_name = SqlName::new("id".into());
        let body_name = SqlName::new("body".into());
        let note_id = SqlName::new("note_id".into());
        let contents = SqlName::new("contents".into());

        let id_then_body = base
            .fold_renamed_column_expressions(id, &id_name, &note_id)
            .unwrap()
            .fold_renamed_column_expressions(body, &body_name, &contents)
            .unwrap();
        let body_then_id = base
            .fold_renamed_column_expressions(body, &body_name, &contents)
            .unwrap()
            .fold_renamed_column_expressions(id, &id_name, &note_id)
            .unwrap();

        assert_eq!(id_then_body, body_then_id);
        assert_ne!(id_then_body.schema_revision_id(), base.schema_revision_id());
        assert_ne!(
            table_schema_key(base.table_id(), base.schema_revision_id()),
            table_schema_key(id_then_body.table_id(), id_then_body.schema_revision_id())
        );
        assert_eq!(
            CreateTable::decode(&id_then_body.encode()).unwrap(),
            id_then_body
        );
    }

    #[test]
    fn table_creation_lowers_to_log_and_revision_cells_and_raises_back() {
        let created = deterministic_create("Notes");
        let lowered = created.to_homebase().unwrap();
        assert_eq!(lowered.mutations.len(), 9);
        assert_eq!(lowered.footprint.constraints().len(), 1);
        assert_eq!(lowered.footprint.writes().len(), 1);
        assert_explicit_range_assertions(
            &lowered.footprint,
            &[schema_object_name_scope_key(&created.name)],
        );

        let Mutation::Set { key: log, value } = &lowered.mutations[0] else {
            panic!("schema log entry was not a set")
        };
        assert_eq!(log.components()[2].as_bytes(), b"log");
        assert_eq!(log.components()[3].as_bytes(), test_uuid(1));
        assert_eq!(decode_frame(value).unwrap(), created);
        assert!(
            lowered
                .footprint
                .constraints()
                .contains(lowered.mutations[1].key())
        );
        assert!(
            lowered
                .footprint
                .writes()
                .contains(&write_revision_key(created.table_id()))
        );

        let admitted = admit(lowered.mutations);
        assert_eq!(CreateTable::from_homebase(&admitted).unwrap(), created);
    }

    #[test]
    fn composite_unique_constraints_roundtrip_with_their_own_index() {
        let created = deterministic_unique_create();
        let unique = &created.schema.unique_constraints[0];
        assert_eq!(unique.index.id.0, test_uuid(7));
        assert_eq!(
            unique.columns(),
            vec![created.schema.columns[1].id, created.schema.columns[2].id]
        );

        let decoded = CreateTable::decode(&created.encode()).unwrap();
        assert_eq!(decoded, created);
        let lowered = created.to_homebase().unwrap();
        assert_eq!(lowered.mutations.len(), 11);
        assert_eq!(
            lowered.mutations[6].key(),
            &index_definition_key(created.table_id, unique.index.id)
        );
        assert_eq!(
            CreateTable::from_homebase(&admit(lowered.mutations)).unwrap(),
            created
        );
    }

    #[test]
    fn only_semantic_indexes_are_bounded_by_homebase_key_components() {
        assert!(index_columns_supported(
            IndexKind::Primary,
            MAX_INDEX_COLUMNS
        ));
        assert!(index_columns_supported(
            IndexKind::Unique,
            MAX_INDEX_COLUMNS
        ));
        assert!(index_columns_supported(IndexKind::Secondary, 0));
        assert!(!index_columns_supported(IndexKind::Secondary, 1));
    }

    #[test]
    fn defaults_checks_and_constraint_names_roundtrip_structurally() {
        let sql = "CREATE TABLE accounts (
            id INTEGER,
            state TEXT CONSTRAINT state_required NOT NULL
                CONSTRAINT state_default DEFAULT ('new')
                CONSTRAINT state_check CHECK (length(state) > 0),
            score REAL DEFAULT -1.5,
            CONSTRAINT account_pk PRIMARY KEY (id),
            CONSTRAINT score_check CHECK (score IS NULL OR score >= 0)
        )";
        let super::super::sql::ValidatedExecute::CreateTable(spec) =
            super::super::sql::validate_execute(sql).unwrap()
        else {
            unreachable!()
        };
        let created = CreateTable::new(sql, spec);
        let id = created.columns()[0].id();
        let state = created.columns()[1].id();

        assert_eq!(
            created.columns()[1].not_null_name(),
            Some(&SqlName::new("state_required".into()))
        );
        let state_default = created.columns()[1].default().unwrap();
        assert_eq!(
            state_default.name,
            Some(SqlName::new("state_default".into()))
        );
        assert_eq!(state_default.expression.to_string(), "('new')");
        let score_default = created.columns()[2].default().unwrap();
        assert_eq!(score_default.name, None);
        assert_eq!(score_default.expression.to_string(), "- 1.5");
        assert_eq!(
            created.schema().primary_key().name(),
            Some(&SqlName::new("account_pk".into()))
        );
        let checks = created.schema().checks();
        assert_eq!(checks.len(), 2);
        assert_eq!(checks[0].column, Some(state));
        assert_eq!(checks[0].name, Some(SqlName::new("state_check".into())));
        assert_eq!(checks[0].expression.to_string(), "length (state) > 0");
        assert_eq!(checks[1].column, None);
        assert_eq!(checks[1].name, Some(SqlName::new("score_check".into())));
        assert_eq!(
            checks[1].expression.to_string(),
            "score IS NULL OR score >= 0"
        );
        assert_eq!(created.primary_key_columns().next().unwrap().id(), id);
        assert_eq!(CreateTable::decode(&created.encode()).unwrap(), created);
    }

    #[test]
    fn decoder_rejects_expression_dependency_mismatches() {
        let sql = "CREATE TABLE notes (
            id INTEGER PRIMARY KEY,
            body TEXT,
            summary TEXT CHECK (body IS NULL OR length(body) > 0)
        )";
        let super::super::sql::ValidatedExecute::CreateTable(spec) =
            super::super::sql::validate_execute(sql).unwrap()
        else {
            unreachable!()
        };
        let created = CreateTable::new(sql, spec);
        let body = created.columns()[1].id();
        assert_eq!(created.schema.checks[0].dependencies, [body]);
        assert_eq!(CreateTable::decode(&created.encode()).unwrap(), created);

        let mut missing_check_dependency = created.clone();
        missing_check_dependency.schema.checks[0]
            .dependencies
            .clear();
        assert_eq!(
            CreateTable::decode(&missing_check_dependency.encode()),
            Err(SchemaCodecError::InvalidInvariant(
                SchemaInvariantError::InvalidCheckConstraint
            ))
        );

        let index = NamedIndex::new_secondary(
            SqlName::new("notes_body".into()),
            vec![IndexTerm::Column {
                column: body,
                collation: None,
                order: None,
            }],
            None,
            vec![body],
        );
        let indexed = created.with_added_index(index).unwrap();
        assert_eq!(CreateTable::decode(&indexed.encode()).unwrap(), indexed);

        let mut duplicate_index_dependency = indexed;
        duplicate_index_dependency.schema.indexes[0]
            .dependencies
            .push(body);
        assert_eq!(
            CreateTable::decode(&duplicate_index_dependency.encode()),
            Err(SchemaCodecError::InvalidInvariant(
                SchemaInvariantError::InvalidNamedIndex
            ))
        );
    }

    #[test]
    fn index_provenance_binds_sql_column_order_to_stable_ids() {
        let created = deterministic_create("notes");
        let super::super::sql::ValidatedExecute::CreateIndex(spec) =
            super::super::sql::validate_execute(
                "CREATE UNIQUE INDEX notes_identity ON notes (id, body)",
            )
            .unwrap()
        else {
            unreachable!()
        };
        let correct = NamedIndex::new_unique(
            spec.name.clone(),
            vec![created.columns()[0].id(), created.columns()[1].id()],
        );
        let crossed = NamedIndex::new_unique(
            spec.name.clone(),
            vec![created.columns()[1].id(), created.columns()[0].id()],
        );

        assert!(created.named_index_matches_spec(&correct, &spec));
        assert!(!created.named_index_matches_spec(&crossed, &spec));
    }

    #[test]
    fn defaults_checks_and_constraint_names_are_structurally_encoded() {
        let sql = "CREATE TABLE accounts (
            id INTEGER CONSTRAINT account_pk PRIMARY KEY,
            state TEXT CONSTRAINT state_required NOT NULL
                CONSTRAINT state_default DEFAULT 'new'
                CONSTRAINT state_check CHECK (length(state) > 0),
            score REAL,
            CONSTRAINT score_check CHECK (score >= 0)
        )";
        let super::super::sql::ValidatedExecute::CreateTable(spec) =
            super::super::sql::validate_execute(sql).unwrap()
        else {
            unreachable!()
        };
        let created = CreateTable::new(sql, spec);
        let encoded = created.encode();
        let raw = decode_frame(&encoded).unwrap();
        assert_eq!(raw, created);
        assert_eq!(CreateTable::decode(&encoded).unwrap(), created);

        let mut changed_sql = raw;
        changed_sql.sql = "CREATE TABLE accounts (
            id INTEGER CONSTRAINT renamed_pk PRIMARY KEY,
            state TEXT CONSTRAINT renamed_required NOT NULL
                CONSTRAINT renamed_default DEFAULT 'changed'
                CONSTRAINT renamed_check CHECK (length(state) > 1),
            score REAL,
            CONSTRAINT renamed_score_check CHECK (score < 10)
        )"
        .into();
        let hydrated = CreateTable::decode(&changed_sql.encode()).unwrap();
        assert_eq!(
            hydrated.schema.primary_key.name(),
            Some(&SqlName::new("account_pk".into()))
        );
        assert_eq!(
            hydrated.columns()[1].not_null_name(),
            Some(&SqlName::new("state_required".into()))
        );
        let default = hydrated.columns()[1].default().unwrap();
        assert_eq!(default.name, Some(SqlName::new("state_default".into())));
        assert_eq!(default.expression.to_string(), "'new'");
        assert_eq!(hydrated.schema.checks.len(), 2);
        assert_eq!(
            hydrated.schema.checks[0].expression.to_string(),
            "length (state) > 0"
        );
        assert_eq!(
            hydrated.schema.checks[1].expression.to_string(),
            "score >= 0"
        );
    }

    #[test]
    fn foreign_keys_roundtrip_stable_parent_identity_and_fence_parent_writes() {
        let connection = Connection::open_in_memory().unwrap();
        catalog::initialize(&connection).unwrap();
        let parent = deterministic_create("parents");
        catalog::insert(&connection, &parent).unwrap();
        let sql = "CREATE TABLE children (
            id INTEGER PRIMARY KEY,
            parent INTEGER,
            CONSTRAINT parent_fk FOREIGN KEY (parent) REFERENCES parents (id)
        )";
        let super::super::sql::ValidatedExecute::CreateTable(spec) =
            super::super::sql::validate_execute(sql).unwrap()
        else {
            unreachable!()
        };
        let child = CreateTable::prepare(&connection, sql, spec).unwrap();
        let foreign_key = &child.foreign_keys()[0];

        assert_eq!(foreign_key.name().map(SqlName::value), Some("parent_fk"));
        assert_eq!(foreign_key.referenced_table(), parent.table_id());
        assert_eq!(foreign_key.referenced_index(), parent.primary_index_id());
        assert_eq!(
            foreign_key
                .referenced_column_names()
                .iter()
                .map(SqlName::value)
                .collect::<Vec<_>>(),
            ["id"]
        );
        assert_eq!(CreateTable::decode(&child.encode()).unwrap(), child);

        let lowered = child.to_homebase().unwrap();
        assert_explicit_range_assertions(
            &lowered.footprint,
            &[
                schema_object_name_scope_key(&child.name),
                active_schema_revision_key(parent.table_id()),
            ],
        );
        assert!(
            lowered
                .footprint
                .writes()
                .contains(&write_revision_key(parent.table_id()))
        );
        assert_eq!(
            CreateTable::from_homebase(&admit(lowered.mutations)).unwrap(),
            child
        );

        let mut malformed = child;
        malformed.schema.foreign_keys[0]
            .referenced_columns
            .push(parent.columns()[0].id());
        malformed.schema.foreign_keys[0]
            .referenced_column_names
            .push(parent.columns()[0].name().clone());
        assert_eq!(
            CreateTable::decode(&malformed.encode()),
            Err(SchemaCodecError::InvalidSchema)
        );
    }

    #[test]
    fn foreign_keys_resolve_composite_unique_targets_with_stable_indexes() {
        let connection = Connection::open_in_memory().unwrap();
        catalog::initialize(&connection).unwrap();
        let parent_sql = "CREATE TABLE accounts (
            id INTEGER PRIMARY KEY,
            tenant TEXT NOT NULL,
            email TEXT NOT NULL,
            UNIQUE (tenant, email)
        )";
        let super::super::sql::ValidatedExecute::CreateTable(parent_spec) =
            super::super::sql::validate_execute(parent_sql).unwrap()
        else {
            unreachable!()
        };
        let parent = CreateTable::new(parent_sql, parent_spec);
        catalog::insert(&connection, &parent).unwrap();
        let child_sql = "CREATE TABLE messages (
            id INTEGER PRIMARY KEY,
            tenant TEXT,
            recipient TEXT,
            FOREIGN KEY (tenant, recipient) REFERENCES accounts (tenant, email)
        )";
        let super::super::sql::ValidatedExecute::CreateTable(child_spec) =
            super::super::sql::validate_execute(child_sql).unwrap()
        else {
            unreachable!()
        };
        let child = CreateTable::prepare(&connection, child_sql, child_spec).unwrap();
        let foreign_key = &child.foreign_keys()[0];
        let target = parent.unique_constraints()[0].index_id();

        assert_eq!(foreign_key.referenced_index(), target);
        assert_eq!(
            parent
                .foreign_key_target_columns(target)
                .unwrap()
                .into_iter()
                .map(|column| column.name().value())
                .collect::<Vec<_>>(),
            ["tenant", "email"]
        );
        assert_eq!(CreateTable::decode(&child.encode()).unwrap(), child);
        child.validate_foreign_key_parents(&connection).unwrap();

        let lowered = child.to_homebase().unwrap();
        assert_explicit_range_assertions(
            &lowered.footprint,
            &[
                schema_object_name_scope_key(&child.name),
                active_schema_revision_key(parent.table_id()),
            ],
        );
        assert!(
            lowered
                .footprint
                .writes()
                .contains(&write_revision_key(parent.table_id()))
        );

        let mut missing_target = child.clone();
        missing_target.schema.foreign_keys[0].referenced_index = IndexId(test_uuid(99));
        assert!(matches!(
            validate_foreign_key_graph(&[parent, missing_target]),
            Err(Error::InvalidDatabase(
                "foreign key target is no longer active in the parent schema"
            ))
        ));
    }

    #[test]
    fn foreign_reference_key_shape_is_rejected_at_schema_preparation() {
        fn child_spec(child_primary_parts: usize, parent_parts: usize) -> CreateTableSpec {
            let primary_columns = (0..child_primary_parts).map(|index| CreateColumn {
                name: SqlName::new(format!("child_key_{index}")),
                declared_type: TypeDeclaration::integer(),
                not_null: true,
                not_null_name: None,
                default: None,
                primary_key: Some(index),
            });
            let foreign_columns = (0..parent_parts).map(|index| CreateColumn {
                name: SqlName::new(format!("parent_key_{index}")),
                declared_type: TypeDeclaration::integer(),
                not_null: false,
                not_null_name: None,
                default: None,
                primary_key: None,
            });
            CreateTableSpec {
                name: SqlName::new("child".into()),
                mode: TableMode::Ordinary,
                storage: TableStorage::WithoutRowid,
                columns: primary_columns.chain(foreign_columns).collect(),
                unique_constraints: Vec::new(),
                foreign_keys: vec![CreateForeignKey {
                    name: None,
                    columns: (0..parent_parts)
                        .map(|index| SqlName::new(format!("parent_key_{index}")))
                        .collect(),
                    referenced_table: SqlName::new("parent".into()),
                    referenced_columns: Some(
                        (0..parent_parts)
                            .map(|index| SqlName::new(format!("key_{index}")))
                            .collect(),
                    ),
                }],
                primary_key_name: None,
                checks: Vec::new(),
            }
        }

        let connection = Connection::open_in_memory().unwrap();
        catalog::initialize(&connection).unwrap();
        let parent_columns = (0..125)
            .map(|index| format!("key_{index} INTEGER NOT NULL"))
            .collect::<Vec<_>>();
        let parent_primary_key = (0..125)
            .map(|index| format!("key_{index}"))
            .collect::<Vec<_>>()
            .join(", ");
        let parent_sql = format!(
            "CREATE TABLE parent ({}, PRIMARY KEY ({parent_primary_key})) WITHOUT ROWID",
            parent_columns.join(", ")
        );
        let super::super::sql::ValidatedExecute::CreateTable(parent_spec) =
            super::super::sql::validate_execute(&parent_sql).unwrap()
        else {
            unreachable!()
        };
        let parent = CreateTable::new(&parent_sql, parent_spec);
        catalog::insert(&connection, &parent).unwrap();

        CreateTable::prepare(
            &connection,
            "CREATE TABLE child (...) WITHOUT ROWID",
            child_spec(124, 125),
        )
        .unwrap();
        assert!(matches!(
            CreateTable::prepare(
                &connection,
                "CREATE TABLE child (...) WITHOUT ROWID",
                child_spec(125, 125),
            ),
            Err(Error::UnsupportedSql(
                "foreign-key reference key exceeds the Homebase component limit"
            ))
        ));
    }

    #[test]
    fn foreign_key_graph_rejects_missing_parents_and_duplicate_relationship_ids() {
        let connection = Connection::open_in_memory().unwrap();
        catalog::initialize(&connection).unwrap();
        let parent = deterministic_create("parents");
        catalog::insert(&connection, &parent).unwrap();

        let make_child = |name: &str| {
            let sql = format!(
                "CREATE TABLE {name} (
                    id INTEGER PRIMARY KEY,
                    parent INTEGER REFERENCES parents(id)
                )"
            );
            let super::super::sql::ValidatedExecute::CreateTable(spec) =
                super::super::sql::validate_execute(&sql).unwrap()
            else {
                unreachable!()
            };
            CreateTable::prepare(&connection, &sql, spec).unwrap()
        };
        let first = make_child("first_child");
        let mut second = make_child("second_child");

        validate_foreign_key_graph(&[parent.clone(), first.clone(), second.clone()]).unwrap();
        assert!(matches!(
            validate_foreign_key_graph(std::slice::from_ref(&first)),
            Err(Error::InvalidDatabase(
                "foreign key references an unknown parent table"
            ))
        ));

        second.schema.foreign_keys[0].id = first.schema.foreign_keys[0].id;
        assert!(matches!(
            validate_foreign_key_graph(&[parent, first, second]),
            Err(Error::InvalidDatabase(
                "schema catalog contains duplicate foreign-key identities"
            ))
        ));
    }

    #[test]
    fn overlapping_unique_constraints_keep_distinct_ordered_indexes() {
        let created = deterministic_overlapping_unique_create();
        assert_eq!(created.schema.unique_constraints.len(), 4);
        assert_eq!(
            created
                .schema
                .unique_constraints
                .iter()
                .map(|unique| unique.index.id.0)
                .collect::<Vec<_>>(),
            [test_uuid(8), test_uuid(9), test_uuid(10), test_uuid(11)]
        );
        assert_eq!(
            created
                .schema
                .unique_constraints
                .iter()
                .map(|unique| unique.columns().to_vec())
                .collect::<Vec<_>>(),
            [
                vec![created.schema.columns[2].id],
                vec![created.schema.columns[3].id],
                vec![created.schema.columns[1].id, created.schema.columns[2].id],
                vec![created.schema.columns[1].id, created.schema.columns[3].id],
            ]
        );

        assert_eq!(CreateTable::decode(&created.encode()).unwrap(), created);
        let lowered = created.to_homebase().unwrap();
        assert_eq!(lowered.mutations.len(), 15);
        for (mutation, unique) in lowered.mutations[6..10]
            .iter()
            .zip(&created.schema.unique_constraints)
        {
            assert_eq!(
                mutation.key(),
                &index_definition_key(created.table_id, unique.index.id)
            );
        }
        assert_eq!(
            CreateTable::from_homebase(&admit(lowered.mutations)).unwrap(),
            created
        );
    }

    #[test]
    fn decoder_rejects_malformed_unique_definitions() {
        let mut duplicate_column = deterministic_unique_create();
        duplicate_column.schema.unique_constraints[0]
            .index
            .columns
            .push(duplicate_column.schema.columns[1].id);
        assert_eq!(
            CreateTable::decode(&duplicate_column.encode()),
            Err(SchemaCodecError::InvalidSchema)
        );

        let mut unknown_column = deterministic_unique_create();
        unknown_column.schema.unique_constraints[0].index.columns[0] = ColumnId(test_uuid(99));
        assert_eq!(
            CreateTable::decode(&unknown_column.encode()),
            Err(SchemaCodecError::InvalidInvariant(
                SchemaInvariantError::InvalidUniqueConstraint
            ))
        );

        let mut duplicate_index = deterministic_overlapping_unique_create();
        duplicate_index.schema.unique_constraints[1].index.id =
            duplicate_index.schema.unique_constraints[0].index.id;
        assert_eq!(
            CreateTable::decode(&duplicate_index.encode()),
            Err(SchemaCodecError::InvalidInvariant(
                SchemaInvariantError::DuplicateIndexIdentity
            ))
        );
    }

    #[test]
    fn schema_object_names_are_case_insensitive() {
        assert_eq!(
            schema_object_name_scope_key(&SqlName::new("Notes".into())),
            schema_object_name_scope_key(&SqlName::new("nOtEs".into()))
        );
    }

    #[test]
    fn decoder_rejects_malformed_frames_and_invalid_uuids() {
        let created = deterministic_create("notes");
        let encoded = created.encode();
        assert_eq!(decode_frame(&encoded).unwrap(), created);
        assert_eq!(decode_frame(&[]), Err(SchemaCodecError::UnknownVersion));
        assert_eq!(
            decode_frame(&[SCHEMA_FRAME_VERSION]),
            Err(SchemaCodecError::MissingField(TAG_MUTATION_ID))
        );
        assert_eq!(
            decode_frame(&encoded[..encoded.len() - 1]),
            Err(SchemaCodecError::Truncated)
        );

        let mut invalid_uuid = encoded;
        invalid_uuid[6..22].fill(0);
        assert_eq!(
            decode_frame(&invalid_uuid),
            Err(SchemaCodecError::InvalidUuid)
        );
    }

    #[test]
    fn admitted_envelope_rejects_missing_or_corrupt_revision_cells() {
        let lowered = deterministic_create("notes").to_homebase().unwrap();
        let mut missing = admit(lowered.mutations.clone());
        missing.entries.pop();
        assert_eq!(
            from_homebase_inner(&missing),
            Err(SchemaCodecError::InvalidBatch)
        );

        let mut corrupt = admit(lowered.mutations);
        let Mutation::Set { value, .. } = &mut corrupt.entries[1].device_entry.mutation else {
            unreachable!()
        };
        value[0] ^= 0xff;
        assert_eq!(
            from_homebase_inner(&corrupt),
            Err(SchemaCodecError::InvalidBatch)
        );
    }

    #[test]
    fn structural_schema_is_self_contained_and_sql_remains_valid_provenance() {
        let created = deterministic_create("notes");
        assert_eq!(CreateTable::decode(&created.encode()).unwrap(), created);
        assert_eq!(
            CreateTable::decode_operation(&created.encode()).unwrap(),
            created
        );

        let mut mismatch = decode_frame(&created.encode()).unwrap();
        mismatch.sql = "CREATE TABLE notes (id INTEGER PRIMARY KEY, body BLOB NOT NULL)".into();
        assert_eq!(
            CreateTable::decode(&mismatch.encode()).unwrap().schema,
            created.schema
        );
        assert_eq!(
            CreateTable::decode_operation(&mismatch.encode()),
            Err(SchemaCodecError::SqlMismatch)
        );
        assert!(matches!(
            mismatch.to_homebase(),
            Err(Error::InvalidMultiliteOp(message)) if message.contains("contradicts")
        ));

        let mut invalid = decode_frame(&created.encode()).unwrap();
        invalid.sql = "CREATE TABLE".into();
        assert_eq!(
            CreateTable::decode(&invalid.encode()),
            Err(SchemaCodecError::InvalidSql)
        );
    }

    #[test]
    fn type_declaration_codec_roundtrips_names_sizes_and_numeric_affinity() {
        let declaration = TypeDeclaration::new("decimal".into(), vec!["10".into(), "2".into()]);
        assert_eq!(
            decode_type_declaration(&encode_type_declaration(&declaration)).unwrap(),
            declaration
        );
        assert_eq!(declaration.name(), "DECIMAL");
        assert_eq!(declaration.arguments(), ["10", "2"]);
        assert_eq!(declaration.affinity(), Affinity::Numeric);
        assert_eq!(declaration.to_sql(), "DECIMAL(10, 2)");

        assert_eq!(
            decode_type_declaration(&[]),
            Err(SchemaCodecError::InvalidColumnType)
        );
        let mut missing_name = Writer::new();
        missing_name.u8(TYPE_DECLARATION_FRAME_VERSION);
        missing_name
            .field(TAG_TYPE_ARGUMENT, b"10")
            .expect("test field is bounded");
        assert_eq!(
            decode_type_declaration(&missing_name.finish()),
            Err(SchemaCodecError::MissingField(TAG_TYPE_NAME))
        );

        let injected = TypeDeclaration::new("TEXT NOT NULL".into(), Vec::new());
        assert_eq!(
            decode_type_declaration(&encode_type_declaration(&injected)),
            Err(SchemaCodecError::InvalidColumnType)
        );
    }

    #[test]
    fn complete_schema_codec_preserves_ordinary_sqlite_declarations() {
        let sql = "CREATE TABLE measurements (
            id INTEGER PRIMARY KEY,
            label VARCHAR(40),
            amount DECIMAL(10, 2),
            enabled BOOLEAN
        )";
        let super::super::sql::ValidatedExecute::CreateTable(spec) =
            super::super::sql::validate_execute(sql).unwrap()
        else {
            unreachable!()
        };
        let created = CreateTable::new(sql, spec);
        let decoded = CreateTable::decode(&created.encode()).unwrap();

        assert_eq!(decoded, created);
        assert_eq!(
            decoded
                .columns()
                .iter()
                .map(|column| (
                    column.declared_type().to_sql(),
                    column.affinity(decoded.mode()),
                    decoded.is_rowid_alias(column.id()),
                ))
                .collect::<Vec<_>>(),
            [
                ("INTEGER".into(), Affinity::Integer, true),
                ("VARCHAR(40)".into(), Affinity::Text, false),
                ("DECIMAL(10, 2)".into(), Affinity::Numeric, false),
                ("BOOLEAN".into(), Affinity::Numeric, false),
            ]
        );
    }

    #[test]
    fn strict_table_mode_roundtrips_and_is_part_of_schema_identity() {
        let sql = "CREATE TABLE strict_values (
            id INTEGER PRIMARY KEY,
            count INT,
            ratio REAL,
            label TEXT,
            payload BLOB,
            anything ANY UNIQUE
        ) STRICT";
        let super::super::sql::ValidatedExecute::CreateTable(spec) =
            super::super::sql::validate_execute(sql).unwrap()
        else {
            unreachable!()
        };
        let created = CreateTable::new(sql, spec);
        let decoded = CreateTable::decode(&created.encode()).unwrap();

        assert_eq!(decoded, created);
        assert_eq!(decoded.mode(), TableMode::Strict);
        assert!(decoded.primary_key_columns().next().unwrap().is_not_null());
        assert_eq!(
            decoded.schema.columns.last().unwrap().strict_type(),
            Some(StrictType::Any)
        );
        assert_eq!(
            decoded
                .schema
                .columns
                .last()
                .unwrap()
                .affinity(decoded.mode()),
            Affinity::Blob
        );

        let mut mode_mismatch = created.clone();
        mode_mismatch.schema.mode = TableMode::Ordinary;
        assert_eq!(
            CreateTable::decode(&mode_mismatch.encode()),
            Err(SchemaCodecError::InvalidInvariant(
                SchemaInvariantError::InvalidSchemaRevision
            ))
        );

        let mut invalid_type = created;
        invalid_type.schema.columns[1].declared_type =
            TypeDeclaration::new("DECIMAL".into(), Vec::new());
        assert_eq!(
            CreateTable::decode(&invalid_type.encode()),
            Err(SchemaCodecError::InvalidInvariant(
                SchemaInvariantError::InvalidStrictColumn
            ))
        );
    }

    #[test]
    fn table_schema_owns_ordered_composite_primary_and_associated_schema() {
        let sql = "CREATE TABLE memberships (
            tenant TEXT,
            member INTEGER,
            body TEXT,
            UNIQUE (tenant, body),
            PRIMARY KEY (member, tenant)
        ) WITHOUT ROWID";
        let super::super::sql::ValidatedExecute::CreateTable(spec) =
            super::super::sql::validate_execute(sql).unwrap()
        else {
            unreachable!()
        };
        let created = CreateTable::new(sql, spec);
        let schema = created.schema();

        assert_eq!(schema.storage(), TableStorage::WithoutRowid);
        assert_eq!(
            created
                .primary_key_columns()
                .map(|column| column.name().value())
                .collect::<Vec<_>>(),
            ["member", "tenant"]
        );
        assert_eq!(schema.primary_key().columns().len(), 2);
        assert_eq!(schema.unique_constraints().len(), 1);
        assert!(schema.indexes().is_empty());
        assert!(schema.foreign_keys().is_empty());
        assert_eq!(CreateTable::decode(&created.encode()).unwrap(), created);

        let mut duplicate_primary = created;
        duplicate_primary.schema.primary_key.index.columns[1] =
            duplicate_primary.schema.primary_key.index.columns[0];
        assert_eq!(
            CreateTable::decode(&duplicate_primary.encode()),
            Err(SchemaCodecError::InvalidSchema)
        );
    }

    #[test]
    fn decoder_rejects_duplicate_names_and_reused_schema_identities() {
        let created = deterministic_create("notes");

        let mut duplicate_name = created.clone();
        duplicate_name.schema.columns[1].name = SqlName::new("ID".into());
        assert_eq!(
            CreateTable::decode(&duplicate_name.encode()),
            Err(SchemaCodecError::InvalidInvariant(
                SchemaInvariantError::DuplicateColumnName
            ))
        );

        let mut duplicate_column_id = created.clone();
        duplicate_column_id.schema.columns[1].id = duplicate_column_id.schema.columns[0].id;
        assert_eq!(
            CreateTable::decode(&duplicate_column_id.encode()),
            Err(SchemaCodecError::InvalidInvariant(
                SchemaInvariantError::ReusedSchemaIdentity
            ))
        );

        let mut reused_identity = created;
        reused_identity.schema.primary_key.index.id = IndexId(reused_identity.table_id.0);
        assert_eq!(
            CreateTable::decode(&reused_identity.encode()),
            Err(SchemaCodecError::InvalidInvariant(
                SchemaInvariantError::ReusedSchemaIdentity
            ))
        );
    }

    #[test]
    fn decoder_rejects_rowid_tables_without_an_integer_primary_key_alias() {
        let mut created = deterministic_create("shadowed");
        created.schema.columns[0].declared_type = TypeDeclaration::text();

        assert_eq!(
            CreateTable::decode(&created.encode()),
            Err(SchemaCodecError::InvalidInvariant(
                SchemaInvariantError::MissingRowidAlias
            ))
        );
    }

    #[test]
    fn minted_ids_are_uuid_v4_shaped() {
        let created = CreateTable::new(
            "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT NOT NULL)",
            definition("notes"),
        );
        for bytes in std::iter::once(created.mutation_id.0)
            .chain(std::iter::once(created.table_id.0))
            .chain(std::iter::once(created.schema_revision_id.0))
            .chain(std::iter::once(created.primary_index_id().0))
            .chain(created.schema.columns.iter().map(|column| column.id.0))
        {
            let uuid = Uuid::from_bytes(bytes);
            assert_eq!(uuid.get_version(), Some(Version::Random));
            assert_eq!(uuid.get_variant(), Variant::RFC4122);
        }
    }
}
