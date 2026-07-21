//! Transaction-local virtual tables and logical read-range tracing.

use std::ffi::c_int;
use std::sync::Arc;

use rusqlite::Connection;
use rusqlite::types::ValueRef;
use rusqlite::vtab::{
    Context, Filters, IndexConstraintOp, IndexInfo, VTab, VTabConnection, VTabCursor,
    eponymous_only_module, sqlite3_vtab, sqlite3_vtab_cursor,
};

use super::catalog;
use super::isolation::ReadTrace;
use super::row::{StoredValue, primary_key_prefix, row_keyspace_prefix};
use super::schema::{CreateTable, DeclaredType};
use super::sql::VTabReadPlan;
use crate::{Error, Result};

pub const MODULE_NAME: &str = "__multilite__vtab";
const FULL_SCAN: c_int = 0;
const PRIMARY_KEY_EQUALITY: c_int = 1;

/// Refresh the fixed vtable module with rows visible to this execution.
pub fn install(connection: &Connection, plan: &VTabReadPlan, trace: ReadTrace) -> Result<()> {
    let source = VTabReadSource::load(connection, &plan.table_name, trace)?;
    connection.create_module(
        MODULE_NAME,
        eponymous_only_module::<MultiliteVTab>(),
        Some(source),
    )?;
    Ok(())
}

#[derive(Clone)]
struct VTabReadSource {
    definition: CreateTable,
    rows: Arc<Vec<Vec<StoredValue>>>,
    primary_column: c_int,
    trace: ReadTrace,
}

impl VTabReadSource {
    fn load(connection: &Connection, table: &str, trace: ReadTrace) -> Result<Self> {
        let definition = catalog::by_name(connection, table)?.ok_or(Error::UnsupportedSql(
            "managed update SELECT requires a synchronized table",
        ))?;
        let columns = definition
            .columns()
            .iter()
            .map(|column| quote_identifier(column.name().value()))
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            "SELECT {columns} FROM main.{}",
            quote_identifier(definition.table_name())
        );
        let mut statement = connection.prepare(&sql)?;
        let width = definition.columns().len();
        let rows = statement
            .query_map((), |row| {
                (0..width)
                    .map(|index| row.get_ref(index).map(StoredValue::capture))
                    .collect::<rusqlite::Result<Vec<_>>>()
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        let primary = definition
            .primary_key_columns()
            .next()
            .expect("validated tables have one primary key")
            .id();
        let primary_column = definition
            .columns()
            .iter()
            .position(|column| column.id() == primary)
            .expect("primary key belongs to its table") as c_int;
        Ok(Self {
            definition,
            rows: Arc::new(rows),
            primary_column,
            trace,
        })
    }

    fn schema_sql(&self) -> String {
        let columns = self
            .definition
            .columns()
            .iter()
            .map(|column| {
                format!(
                    "{} {}",
                    quote_identifier(column.name().value()),
                    declared_type_sql(column.declared_type())
                )
            })
            .collect::<Vec<_>>()
            .join(", ");
        format!("CREATE TABLE x ({columns})")
    }

    fn trace_filter(&self, value: Option<ValueRef<'_>>) {
        let full = row_keyspace_prefix(&self.definition);
        let prefix = value
            .map(StoredValue::capture)
            .and_then(|value| primary_key_prefix(&self.definition, &[value]).ok())
            .unwrap_or(full);
        self.trace.record(prefix);
    }
}

#[repr(C)]
struct MultiliteVTab {
    base: sqlite3_vtab,
    source: VTabReadSource,
}

unsafe impl<'vtab> VTab<'vtab> for MultiliteVTab {
    type Aux = VTabReadSource;
    type Cursor = MultiliteCursor;

    fn connect(
        _connection: &mut VTabConnection,
        source: Option<&Self::Aux>,
        _args: &[&[u8]],
    ) -> rusqlite::Result<(String, Self)> {
        let source = source
            .ok_or_else(|| rusqlite::Error::ModuleError("read source is missing".into()))?
            .clone();
        Ok((
            source.schema_sql(),
            Self {
                base: sqlite3_vtab::default(),
                source,
            },
        ))
    }

    fn best_index(&self, info: &mut IndexInfo) -> rusqlite::Result<()> {
        let mut point_read = false;
        for (constraint, mut usage) in info.constraints_and_usages() {
            if constraint.is_usable()
                && constraint.column() == self.source.primary_column
                && constraint.operator() == IndexConstraintOp::SQLITE_INDEX_CONSTRAINT_EQ
            {
                usage.set_argv_index(1);
                point_read = true;
                break;
            }
        }
        info.set_idx_num(if point_read {
            PRIMARY_KEY_EQUALITY
        } else {
            FULL_SCAN
        });
        info.set_estimated_rows(if point_read {
            1
        } else {
            self.source.rows.len() as i64
        });
        Ok(())
    }

    fn open(&mut self) -> rusqlite::Result<Self::Cursor> {
        Ok(MultiliteCursor {
            base: sqlite3_vtab_cursor::default(),
            source: self.source.clone(),
            row: 0,
        })
    }
}

#[repr(C)]
struct MultiliteCursor {
    base: sqlite3_vtab_cursor,
    source: VTabReadSource,
    row: usize,
}

unsafe impl VTabCursor for MultiliteCursor {
    fn filter(
        &mut self,
        idx_num: c_int,
        _idx_str: Option<&str>,
        args: &Filters<'_>,
    ) -> rusqlite::Result<()> {
        self.row = 0;
        self.source.trace_filter(
            (idx_num == PRIMARY_KEY_EQUALITY)
                .then(|| args.iter().next())
                .flatten(),
        );
        Ok(())
    }

    fn next(&mut self) -> rusqlite::Result<()> {
        self.row += 1;
        Ok(())
    }

    fn eof(&self) -> bool {
        self.row >= self.source.rows.len()
    }

    fn column(&self, context: &mut Context, index: c_int) -> rusqlite::Result<()> {
        let value = self
            .source
            .rows
            .get(self.row)
            .and_then(|row| row.get(index as usize))
            .ok_or_else(|| rusqlite::Error::ModuleError("read cursor is out of bounds".into()))?;
        context.set_result(value)
    }

    fn rowid(&self) -> rusqlite::Result<i64> {
        i64::try_from(self.row + 1)
            .map_err(|_| rusqlite::Error::ModuleError("read rowid overflowed".into()))
    }
}

fn declared_type_sql(declared_type: DeclaredType) -> &'static str {
    match declared_type {
        DeclaredType::Integer => "INTEGER",
        DeclaredType::Real => "REAL",
        DeclaredType::Text => "TEXT",
        DeclaredType::Blob => "BLOB",
    }
}

fn quote_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use homebase_core::tag::AdmissionSeq;

    use super::*;
    use crate::database::IsolationLevel;
    use crate::database::schema::{CreateColumn, CreateTableSpec, SqlName};

    fn connection() -> (Connection, CreateTable) {
        let connection = Connection::open_in_memory().unwrap();
        catalog::initialize(&connection).unwrap();
        let created = CreateTable::new(
            "CREATE TABLE notes (id INTEGER PRIMARY KEY, day TEXT NOT NULL)",
            CreateTableSpec {
                name: SqlName::new("notes".into()),
                columns: vec![
                    CreateColumn {
                        name: SqlName::new("id".into()),
                        declared_type: DeclaredType::Integer,
                        not_null: false,
                        primary_key: true,
                    },
                    CreateColumn {
                        name: SqlName::new("day".into()),
                        declared_type: DeclaredType::Text,
                        not_null: true,
                        primary_key: false,
                    },
                ],
            },
        );
        connection.execute(created.sql(), ()).unwrap();
        catalog::insert(&connection, &created).unwrap();
        connection
            .execute("INSERT INTO notes VALUES (1, 'mon'), (2, 'tue')", ())
            .unwrap();
        (connection, created)
    }

    #[test]
    fn full_scan_matches_sqlite_and_records_the_row_keyspace() {
        let (connection, created) = connection();
        let plan =
            super::super::sql::plan_vtab_read("SELECT id FROM notes WHERE day = ?1 ORDER BY id")
                .unwrap()
                .unwrap();
        let trace = ReadTrace::new();
        install(&connection, &plan, trace.clone()).unwrap();

        let rows = connection
            .prepare(&plan.rewritten_sql)
            .unwrap()
            .query_map(["mon"], |row| row.get::<_, i64>(0))
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap();
        assert_eq!(rows, [1]);
        assert_eq!(
            trace.footprint().reads(),
            &BTreeSet::from([row_keyspace_prefix(&created)])
        );
    }

    #[test]
    fn primary_key_equality_records_one_exact_row_prefix() {
        let (connection, created) = connection();
        let plan = super::super::sql::plan_vtab_read("SELECT day FROM notes WHERE id = ?1")
            .unwrap()
            .unwrap();
        let trace = ReadTrace::new();
        install(&connection, &plan, trace.clone()).unwrap();

        let rows = connection
            .prepare(&plan.rewritten_sql)
            .unwrap()
            .query_map([2_i64], |row| row.get::<_, String>(0))
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap();
        assert_eq!(rows, ["tue"]);
        let exact = primary_key_prefix(&created, &[StoredValue::Integer(2)]).unwrap();
        assert_eq!(trace.footprint().reads(), &BTreeSet::from([exact.clone()]));
        assert_eq!(
            trace
                .footprint()
                .plan(IsolationLevel::Serializable, AdmissionSeq(7))[0]
                .prefix,
            exact
        );
    }

    #[test]
    fn point_reads_fall_back_to_the_table_prefix_when_affinity_is_ambiguous() {
        let (connection, created) = connection();
        let plan = super::super::sql::plan_vtab_read("SELECT day FROM notes WHERE id = ?1")
            .unwrap()
            .unwrap();
        let trace = ReadTrace::new();
        install(&connection, &plan, trace.clone()).unwrap();

        let rows = connection
            .prepare(&plan.rewritten_sql)
            .unwrap()
            .query_map(["2"], |row| row.get::<_, String>(0))
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap();
        assert_eq!(rows, ["tue"]);
        assert_eq!(
            trace.footprint().reads(),
            &BTreeSet::from([row_keyspace_prefix(&created)])
        );
    }
}
