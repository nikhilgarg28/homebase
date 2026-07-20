use std::fmt;
use std::path::Path;

use multilite::MultiliteConnection;
use rusqlite::types::ValueRef;
use sqllogictest::{DBOutput, DefaultColumnType};

/// Errors surfaced through the SQL Logic Test runner.
#[derive(Debug)]
pub enum DriverError {
    Multilite(multilite::Error),
    Sqlite(rusqlite::Error),
    InvalidQueryShape(&'static str),
}

impl fmt::Display for DriverError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Multilite(error) => write!(f, "multilite: {error}"),
            Self::Sqlite(error) => write!(f, "sqlite: {error}"),
            Self::InvalidQueryShape(message) => f.write_str(message),
        }
    }
}

impl std::error::Error for DriverError {}

impl From<multilite::Error> for DriverError {
    fn from(error: multilite::Error) -> Self {
        Self::Multilite(error)
    }
}

impl From<rusqlite::Error> for DriverError {
    fn from(error: rusqlite::Error) -> Self {
        Self::Sqlite(error)
    }
}

pub type DriverResult<T> = std::result::Result<T, DriverError>;

/// Vanilla SQLite reference engine.
pub struct SqliteDriver {
    connection: rusqlite::Connection,
}

impl SqliteDriver {
    pub fn open(path: impl AsRef<Path>) -> DriverResult<Self> {
        Ok(Self {
            connection: rusqlite::Connection::open(path)?,
        })
    }
}

/// Multilite engine under test.
pub struct MultiliteDriver {
    connection: MultiliteConnection,
}

impl MultiliteDriver {
    pub fn open(path: impl AsRef<Path>) -> DriverResult<Self> {
        Ok(Self {
            connection: MultiliteConnection::open(path)?,
        })
    }
}

impl sqllogictest::DB for SqliteDriver {
    type ColumnType = DefaultColumnType;
    type Error = DriverError;

    fn run(&mut self, sql: &str) -> DriverResult<DBOutput<Self::ColumnType>> {
        run_sqlite(&self.connection, sql)
    }

    fn engine_name(&self) -> &str {
        "sqlite"
    }
}

impl sqllogictest::DB for MultiliteDriver {
    type ColumnType = DefaultColumnType;
    type Error = DriverError;

    fn run(&mut self, sql: &str) -> DriverResult<DBOutput<Self::ColumnType>> {
        match self.connection.prepare(sql) {
            Ok(_) => run_multilite_query(&self.connection, sql),
            Err(multilite::Error::PreparedWrite) => {
                let changed = self.connection.execute(sql, ())?;
                Ok(DBOutput::StatementComplete(changed as u64))
            }
            Err(error) => Err(error.into()),
        }
    }

    fn engine_name(&self) -> &str {
        "multilite"
    }
}

fn run_sqlite(
    connection: &rusqlite::Connection,
    sql: &str,
) -> DriverResult<DBOutput<DefaultColumnType>> {
    if connection.prepare(sql)?.readonly() {
        let mut statement = connection.prepare(sql)?;
        query_rows(&mut statement)
    } else {
        let changed = connection.execute(sql, ())?;
        Ok(DBOutput::StatementComplete(changed as u64))
    }
}

fn run_multilite_query(
    connection: &MultiliteConnection,
    sql: &str,
) -> DriverResult<DBOutput<DefaultColumnType>> {
    let mut statement = connection.prepare(sql)?;
    let rows = statement.query_map((), |row| {
        let mut values = Vec::with_capacity(row.as_ref().column_count());
        for index in 0..row.as_ref().column_count() {
            values.push(format_value(row.get_ref(index)?));
        }
        Ok(values)
    })?;
    Ok(DBOutput::Rows {
        types: vec![DefaultColumnType::Any; rows.first().map_or(0, Vec::len)],
        rows,
    })
}

fn query_rows(
    statement: &mut rusqlite::Statement<'_>,
) -> DriverResult<DBOutput<DefaultColumnType>> {
    let column_count = statement.column_count();
    let rows = statement
        .query_map((), |row| {
            let mut values = Vec::with_capacity(column_count);
            for index in 0..column_count {
                values.push(format_value(row.get_ref(index)?));
            }
            Ok(values)
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(DBOutput::Rows {
        types: vec![DefaultColumnType::Any; column_count],
        rows,
    })
}

fn format_value(value: ValueRef<'_>) -> String {
    match value {
        ValueRef::Null => String::from("NULL"),
        ValueRef::Integer(value) => value.to_string(),
        ValueRef::Real(value) => {
            let mut formatted = value.to_string();
            if !formatted.contains('.') && !formatted.contains('e') && !formatted.contains('E') {
                formatted.push_str(".0");
            }
            formatted
        }
        ValueRef::Text(value) => String::from_utf8_lossy(value).into_owned(),
        ValueRef::Blob(value) => {
            let mut formatted = String::with_capacity(value.len() * 2);
            for byte in value {
                formatted.push_str(&format!("{byte:02x}"));
            }
            formatted
        }
    }
}
