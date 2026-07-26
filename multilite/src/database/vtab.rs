//! Transaction-local virtual tables and logical read-range tracing.

use std::collections::BTreeMap;
use std::ffi::c_int;
use std::sync::Arc;

use parking_lot::Mutex;
use rusqlite::Connection;
use rusqlite::types::ValueRef;
use rusqlite::vtab::{
    Context, Filters, IndexConstraintOp, IndexInfo, VTab, VTabConnection, VTabCursor,
    eponymous_only_module, sqlite3_vtab, sqlite3_vtab_cursor,
};

use super::catalog;
use super::row::{StoredValue, primary_key_prefix, row_keyspace_prefix};
use super::schema::{CreateTable, SchemaRevisionId, StrictType, TableId, TableMode};
use crate::commit::footprint::ReadTrace;
use crate::{Error, Result};

const MODULE_PREFIX: &str = "__multilite__vtab_";
const FULL_SCAN: c_int = 0;
const PRIMARY_KEY_EQUALITY: c_int = 1;

/// Lazy vtable registrations owned by one physical SQLite connection.
#[derive(Default)]
pub struct Registry {
    modules: Mutex<BTreeMap<String, Module>>,
}

impl Registry {
    /// Validate SQL and capture the versioned modules used by this statement.
    pub fn plan(&self, connection: &Connection, sql: &str) -> Result<Option<Plan>> {
        let mut modules = Vec::new();
        let rewritten_sql = super::sql::rewrite_managed_read(sql, |table| {
            let module = self.register(connection, table)?;
            let name = module.name.clone();
            if modules
                .iter()
                .all(|registered: &Module| registered.name != name)
            {
                modules.push(module);
            }
            Ok(name)
        })?;
        Ok(rewritten_sql.map(|rewritten_sql| Plan {
            rewritten_sql,
            modules,
        }))
    }

    fn register(&self, connection: &Connection, table: &str) -> Result<Module> {
        let definition = catalog::by_name(connection, table)?.ok_or(Error::UnsupportedSql(
            "managed update SELECT requires a synchronized table",
        ))?;
        let name = module_name(definition.table_id(), definition.schema_revision_id());
        let mut modules = self.modules.lock();
        if let Some(module) = modules.get(&name) {
            module.validate_definition(&definition)?;
            return Ok(module.clone());
        }

        let module = Module::new(name.clone(), definition);
        connection.create_module(
            name.as_str(),
            eponymous_only_module::<MultiliteVTab>(),
            Some(module.clone()),
        )?;
        modules.insert(name, module.clone());
        Ok(module)
    }

    #[cfg(test)]
    fn registered_names(&self) -> Vec<String> {
        self.modules.lock().keys().cloned().collect()
    }

    #[cfg(test)]
    fn bound_count(&self) -> usize {
        self.modules
            .lock()
            .values()
            .filter(|module| module.is_bound())
            .count()
    }
}

/// Rewritten SQL and versioned modules resolved when a statement is prepared.
pub struct Plan {
    rewritten_sql: String,
    modules: Vec<Module>,
}

impl Plan {
    pub fn sql(&self) -> &str {
        &self.rewritten_sql
    }

    /// Bind rows and read tracing for one execution of this plan.
    pub fn bind(&self, connection: &Connection, trace: ReadTrace) -> Result<BindingGuard> {
        let mut guard = BindingGuard {
            modules: Vec::with_capacity(self.modules.len()),
        };
        for module in &self.modules {
            let current = catalog::by_id(connection, module.definition.table_id())?.ok_or(
                Error::InvalidDatabase("vtable plan references a missing table identity"),
            )?;
            module.validate_definition(&current)?;
            module.bind(Arc::new(EagerSource::load(
                connection,
                module.definition.clone(),
                trace.clone(),
            )?));
            guard.modules.push(module.clone());
        }
        Ok(guard)
    }
}

/// Transaction-local source bindings released after one query execution.
pub struct BindingGuard {
    modules: Vec<Module>,
}

impl Drop for BindingGuard {
    fn drop(&mut self) {
        for module in &self.modules {
            module.clear();
        }
    }
}

fn module_name(table: TableId, revision: SchemaRevisionId) -> String {
    let mut name = String::with_capacity(MODULE_PREFIX.len() + 65);
    name.push_str(MODULE_PREFIX);
    for (index, id) in [table.as_bytes(), revision.as_bytes()].iter().enumerate() {
        if index > 0 {
            name.push('_');
        }
        for byte in id {
            use std::fmt::Write as _;
            write!(&mut name, "{byte:02x}").expect("writing to a string cannot fail");
        }
    }
    name
}

#[derive(Clone)]
struct Module {
    name: String,
    definition: CreateTable,
    primary_column: c_int,
    source: Arc<Mutex<Option<Arc<EagerSource>>>>,
}

impl Module {
    fn new(name: String, definition: CreateTable) -> Self {
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
        Self {
            name,
            definition,
            primary_column,
            source: Arc::new(Mutex::new(None)),
        }
    }

    fn validate_definition(&self, definition: &CreateTable) -> Result<()> {
        if &self.definition == definition {
            Ok(())
        } else {
            Err(Error::InvalidDatabase(
                "vtable plan's schema revision is no longer current",
            ))
        }
    }

    fn bind(&self, source: Arc<EagerSource>) {
        *self.source.lock() = Some(source);
    }

    fn clear(&self) {
        *self.source.lock() = None;
    }

    #[cfg(test)]
    fn is_bound(&self) -> bool {
        self.source.lock().is_some()
    }

    fn source(&self) -> rusqlite::Result<Arc<EagerSource>> {
        self.source.lock().clone().ok_or_else(|| {
            rusqlite::Error::ModuleError("vtable execution source is not bound".into())
        })
    }

    fn schema_sql(&self) -> String {
        let mode = self.definition.mode();
        let columns = self
            .definition
            .columns()
            .iter()
            .map(|column| {
                let declaration =
                    if mode == TableMode::Strict && column.strict_type() == Some(StrictType::Any) {
                        "BLOB".to_owned()
                    } else {
                        column.declared_type().to_sql()
                    };
                format!(
                    "{} {}",
                    quote_identifier(column.name().value()),
                    declaration
                )
            })
            .collect::<Vec<_>>()
            .join(", ");
        format!("CREATE TABLE x ({columns})")
    }
}

struct EagerSource {
    definition: CreateTable,
    rows: Vec<Vec<StoredValue>>,
    trace: ReadTrace,
}

impl EagerSource {
    fn load(connection: &Connection, definition: CreateTable, trace: ReadTrace) -> Result<Self> {
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
        Ok(Self {
            definition,
            rows,
            trace,
        })
    }

    fn estimated_rows(&self) -> i64 {
        self.rows.len() as i64
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
    module: Module,
}

unsafe impl<'vtab> VTab<'vtab> for MultiliteVTab {
    type Aux = Module;
    type Cursor = MultiliteCursor;

    fn connect(
        _connection: &mut VTabConnection,
        source: Option<&Self::Aux>,
        _args: &[&[u8]],
    ) -> rusqlite::Result<(String, Self)> {
        let module = source
            .ok_or_else(|| rusqlite::Error::ModuleError("vtable module is missing".into()))?
            .clone();
        Ok((
            module.schema_sql(),
            Self {
                base: sqlite3_vtab::default(),
                module,
            },
        ))
    }

    fn best_index(&self, info: &mut IndexInfo) -> rusqlite::Result<()> {
        // Later index SQL slices add catalog-backed keyspace candidates here;
        // SQLite's materialized index list is not durable replication meaning.
        let mut point_read = false;
        for (constraint, mut usage) in info.constraints_and_usages() {
            if constraint.is_usable()
                && constraint.column() == self.module.primary_column
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
        let source = self.module.source()?;
        info.set_estimated_rows(if point_read {
            1
        } else {
            source.estimated_rows()
        });
        Ok(())
    }

    fn open(&mut self) -> rusqlite::Result<Self::Cursor> {
        Ok(MultiliteCursor {
            base: sqlite3_vtab_cursor::default(),
            source: self.module.source()?,
            row: None,
        })
    }
}

#[repr(C)]
struct MultiliteCursor {
    base: sqlite3_vtab_cursor,
    source: Arc<EagerSource>,
    row: Option<usize>,
}

unsafe impl VTabCursor for MultiliteCursor {
    fn filter(
        &mut self,
        idx_num: c_int,
        _idx_str: Option<&str>,
        args: &Filters<'_>,
    ) -> rusqlite::Result<()> {
        self.source.trace_filter(
            (idx_num == PRIMARY_KEY_EQUALITY)
                .then(|| args.iter().next())
                .flatten(),
        );
        self.row = Some(0);
        Ok(())
    }

    fn next(&mut self) -> rusqlite::Result<()> {
        let row = self.row.as_mut().ok_or_else(cursor_not_filtered)?;
        *row += 1;
        Ok(())
    }

    fn eof(&self) -> bool {
        self.row.is_none_or(|row| row >= self.source.rows.len())
    }

    fn column(&self, context: &mut Context, index: c_int) -> rusqlite::Result<()> {
        let value = self
            .source
            .rows
            .get(self.row_index()?)
            .and_then(|row| row.get(index as usize))
            .ok_or_else(|| rusqlite::Error::ModuleError("vtable cursor is out of bounds".into()))?;
        context.set_result(value)
    }

    fn rowid(&self) -> rusqlite::Result<i64> {
        i64::try_from(self.row_index()? + 1)
            .map_err(|_| rusqlite::Error::ModuleError("vtable rowid overflowed".into()))
    }
}

impl MultiliteCursor {
    fn row_index(&self) -> rusqlite::Result<usize> {
        self.row.ok_or_else(cursor_not_filtered)
    }
}

fn cursor_not_filtered() -> rusqlite::Error {
    rusqlite::Error::ModuleError("vtable cursor is not filtered".into())
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
    use crate::database::schema::{CreateColumn, CreateTableSpec, SqlName, TypeDeclaration};

    fn connection() -> (Connection, CreateTable) {
        let connection = Connection::open_in_memory().unwrap();
        catalog::initialize(&connection).unwrap();
        let created = CreateTable::new(
            "CREATE TABLE notes (id INTEGER PRIMARY KEY, day TEXT NOT NULL)",
            CreateTableSpec {
                name: SqlName::new("notes".into()),
                mode: Default::default(),
                columns: vec![
                    CreateColumn {
                        name: SqlName::new("id".into()),
                        declared_type: TypeDeclaration::integer(),
                        not_null: false,
                        primary_key: true,
                    },
                    CreateColumn {
                        name: SqlName::new("day".into()),
                        declared_type: TypeDeclaration::text(),
                        not_null: true,
                        primary_key: false,
                    },
                ],
                unique_constraints: Vec::new(),
            },
        );
        connection.execute(created.sql(), ()).unwrap();
        catalog::insert(&connection, &created).unwrap();
        connection
            .execute("INSERT INTO notes VALUES (1, 'mon'), (2, 'tue')", ())
            .unwrap();
        (connection, created)
    }

    fn tasks(connection: &Connection) -> CreateTable {
        let created = CreateTable::new(
            "CREATE TABLE tasks (id INTEGER PRIMARY KEY, title TEXT, payload BLOB)",
            CreateTableSpec {
                name: SqlName::new("tasks".into()),
                mode: Default::default(),
                columns: vec![
                    CreateColumn {
                        name: SqlName::new("id".into()),
                        declared_type: TypeDeclaration::integer(),
                        not_null: false,
                        primary_key: true,
                    },
                    CreateColumn {
                        name: SqlName::new("title".into()),
                        declared_type: TypeDeclaration::text(),
                        not_null: false,
                        primary_key: false,
                    },
                    CreateColumn {
                        name: SqlName::new("payload".into()),
                        declared_type: TypeDeclaration::blob(),
                        not_null: false,
                        primary_key: false,
                    },
                ],
                unique_constraints: Vec::new(),
            },
        );
        connection.execute(created.sql(), ()).unwrap();
        catalog::insert(connection, &created).unwrap();
        connection
            .execute("INSERT INTO tasks VALUES (4, 'ship', x'0102')", ())
            .unwrap();
        created
    }

    #[test]
    fn full_scan_matches_sqlite_and_records_the_row_keyspace() {
        let (connection, created) = connection();
        let registry = Registry::default();
        let plan = registry
            .plan(
                &connection,
                "SELECT id FROM notes WHERE day = ?1 ORDER BY id",
            )
            .unwrap()
            .unwrap();
        let trace = ReadTrace::new();
        let _bindings = plan.bind(&connection, trace.clone()).unwrap();

        let rows = connection
            .prepare(plan.sql())
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
        let registry = Registry::default();
        let plan = registry
            .plan(&connection, "SELECT day FROM notes WHERE id = ?1")
            .unwrap()
            .unwrap();
        let trace = ReadTrace::new();
        let _bindings = plan.bind(&connection, trace.clone()).unwrap();

        let rows = connection
            .prepare(plan.sql())
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
        let registry = Registry::default();
        let plan = registry
            .plan(&connection, "SELECT day FROM notes WHERE id = ?1")
            .unwrap()
            .unwrap();
        let trace = ReadTrace::new();
        let _bindings = plan.bind(&connection, trace.clone()).unwrap();

        let rows = connection
            .prepare(plan.sql())
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

    #[test]
    fn strict_any_primary_keys_keep_exact_storage_classes_in_tracked_reads() {
        let connection = Connection::open_in_memory().unwrap();
        catalog::initialize(&connection).unwrap();
        let sql = "CREATE TABLE strict_values (
            id ANY PRIMARY KEY,
            body TEXT
        ) STRICT";
        let super::super::sql::ValidatedExecute::CreateTable(spec) =
            super::super::sql::validate_execute(sql).unwrap()
        else {
            unreachable!()
        };
        let created = CreateTable::new(sql, spec);
        connection.execute(sql, ()).unwrap();
        catalog::insert(&connection, &created).unwrap();
        connection
            .execute(
                "INSERT INTO strict_values VALUES ('000123', 'text'), (123, 'integer')",
                (),
            )
            .unwrap();

        let registry = Registry::default();
        let text_plan = registry
            .plan(&connection, "SELECT body FROM strict_values WHERE id = ?1")
            .unwrap()
            .unwrap();
        let text_trace = ReadTrace::new();
        let text_rows = {
            let _bindings = text_plan.bind(&connection, text_trace.clone()).unwrap();
            connection
                .prepare(text_plan.sql())
                .unwrap()
                .query_map(["000123"], |row| row.get::<_, String>(0))
                .unwrap()
                .collect::<rusqlite::Result<Vec<_>>>()
                .unwrap()
        };
        assert_eq!(text_rows, ["text"]);
        assert_eq!(
            text_trace.footprint().reads(),
            &BTreeSet::from([primary_key_prefix(
                &created,
                &[StoredValue::Text(b"000123".to_vec())]
            )
            .unwrap()])
        );

        let integer_plan = registry
            .plan(&connection, "SELECT body FROM strict_values WHERE id = ?1")
            .unwrap()
            .unwrap();
        let integer_trace = ReadTrace::new();
        let integer_rows = {
            let _bindings = integer_plan
                .bind(&connection, integer_trace.clone())
                .unwrap();
            connection
                .prepare(integer_plan.sql())
                .unwrap()
                .query_map([123], |row| row.get::<_, String>(0))
                .unwrap()
                .collect::<rusqlite::Result<Vec<_>>>()
                .unwrap()
        };
        assert_eq!(integer_rows, ["integer"]);
        assert_eq!(
            integer_trace.footprint().reads(),
            &BTreeSet::from([primary_key_prefix(&created, &[StoredValue::Integer(123)]).unwrap()])
        );
    }

    #[test]
    fn aliases_use_versioned_uuid_modules_without_schema_objects() {
        let (connection, created) = connection();
        let registry = Registry::default();
        let plan = registry
            .plan(&connection, "SELECT n.day FROM notes AS n WHERE n.id = ?1")
            .unwrap()
            .unwrap();
        assert_eq!(
            plan.modules[0].name,
            module_name(created.table_id(), created.schema_revision_id())
        );
        assert!(plan.sql().contains(" AS n"));

        let trace = ReadTrace::new();
        let _bindings = plan.bind(&connection, trace).unwrap();
        let rows = connection
            .prepare(plan.sql())
            .unwrap()
            .query_map([1_i64], |row| row.get::<_, String>(0))
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap();
        assert_eq!(rows, ["mon"]);

        for schema in ["main", "temp"] {
            let count = connection
                .query_row(
                    &format!(
                        "SELECT count(*) FROM {schema}.sqlite_schema WHERE name LIKE '__multilite__vtab_%'"
                    ),
                    (),
                    |row| row.get::<_, i64>(0),
                )
                .unwrap();
            assert_eq!(count, 0);
        }
    }

    #[test]
    fn module_identity_includes_the_schema_revision() {
        let table = TableId::from_bytes([1; 16]);
        let first = module_name(table, SchemaRevisionId::from_bytes([2; 16]));
        let second = module_name(table, SchemaRevisionId::from_bytes([3; 16]));

        assert_ne!(first, second);
        assert!(first.starts_with(MODULE_PREFIX));
        assert_eq!(first.len(), MODULE_PREFIX.len() + 65);
    }

    #[test]
    fn captured_plan_rejects_a_missing_catalog_identity() {
        let (connection, _) = connection();
        let registry = Registry::default();
        let plan = registry
            .plan(&connection, "SELECT day FROM notes WHERE id = 1")
            .unwrap()
            .unwrap();
        catalog::remove_by_name(&connection, "notes").unwrap();

        assert!(matches!(
            plan.bind(&connection, ReadTrace::new()),
            Err(Error::InvalidDatabase(
                "vtable plan references a missing table identity"
            ))
        ));
    }

    #[test]
    fn differently_shaped_tables_remain_registered_together() {
        let (connection, notes) = connection();
        let tasks = tasks(&connection);
        let registry = Registry::default();
        let notes_plan = registry
            .plan(&connection, "SELECT day FROM notes WHERE id = ?1")
            .unwrap()
            .unwrap();
        let tasks_plan = registry
            .plan(
                &connection,
                "SELECT title, payload FROM tasks WHERE id = ?1",
            )
            .unwrap()
            .unwrap();
        assert_eq!(
            registry
                .registered_names()
                .into_iter()
                .collect::<BTreeSet<_>>(),
            [
                module_name(notes.table_id(), notes.schema_revision_id()),
                module_name(tasks.table_id(), tasks.schema_revision_id())
            ]
            .into_iter()
            .collect::<BTreeSet<_>>()
        );

        let notes_trace = ReadTrace::new();
        let notes_rows = {
            let _bindings = notes_plan.bind(&connection, notes_trace.clone()).unwrap();
            connection
                .prepare(notes_plan.sql())
                .unwrap()
                .query_map([2_i64], |row| row.get::<_, String>(0))
                .unwrap()
                .collect::<rusqlite::Result<Vec<_>>>()
                .unwrap()
        };
        assert_eq!(notes_rows, ["tue"]);
        assert_eq!(registry.bound_count(), 0);

        let tasks_trace = ReadTrace::new();
        let _bindings = tasks_plan.bind(&connection, tasks_trace.clone()).unwrap();
        let task_rows = connection
            .prepare(tasks_plan.sql())
            .unwrap()
            .query_map([4_i64], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, Vec<u8>>(1)?))
            })
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap();
        assert_eq!(task_rows, [("ship".into(), vec![1, 2])]);
        assert_eq!(
            notes_trace.footprint().reads(),
            &BTreeSet::from([primary_key_prefix(&notes, &[StoredValue::Integer(2)]).unwrap()])
        );
        assert_eq!(
            tasks_trace.footprint().reads(),
            &BTreeSet::from([primary_key_prefix(&tasks, &[StoredValue::Integer(4)]).unwrap()])
        );
    }

    #[test]
    fn physical_connections_register_independently_with_the_same_stable_name() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("vtabs.sqlite");
        let first = Connection::open(&path).unwrap();
        catalog::initialize(&first).unwrap();
        let created = CreateTable::new(
            "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)",
            CreateTableSpec {
                name: SqlName::new("notes".into()),
                mode: Default::default(),
                columns: vec![
                    CreateColumn {
                        name: SqlName::new("id".into()),
                        declared_type: TypeDeclaration::integer(),
                        not_null: false,
                        primary_key: true,
                    },
                    CreateColumn {
                        name: SqlName::new("body".into()),
                        declared_type: TypeDeclaration::text(),
                        not_null: false,
                        primary_key: false,
                    },
                ],
                unique_constraints: Vec::new(),
            },
        );
        first.execute(created.sql(), ()).unwrap();
        catalog::insert(&first, &created).unwrap();
        first
            .execute("INSERT INTO notes VALUES (1, 'one')", ())
            .unwrap();
        let second = Connection::open(&path).unwrap();

        let first_registry = Registry::default();
        let second_registry = Registry::default();
        let first_plan = first_registry
            .plan(&first, "SELECT body FROM notes WHERE id = 1")
            .unwrap()
            .unwrap();
        let second_plan = second_registry
            .plan(&second, "SELECT body FROM notes WHERE id = 1")
            .unwrap()
            .unwrap();
        let stable = module_name(created.table_id(), created.schema_revision_id());
        let first_names = first_registry.registered_names();
        let second_names = second_registry.registered_names();
        assert_eq!(first_names.len(), 1);
        assert_eq!(second_names.len(), 1);
        assert_eq!(first_names[0], stable);
        assert_eq!(second_names[0], stable);

        let first_trace = ReadTrace::new();
        let _first_bindings = first_plan.bind(&first, first_trace).unwrap();
        let second_trace = ReadTrace::new();
        let _second_bindings = second_plan.bind(&second, second_trace).unwrap();
        for (connection, plan) in [(&first, &first_plan), (&second, &second_plan)] {
            assert_eq!(
                connection
                    .query_row(plan.sql(), (), |row| row.get::<_, String>(0))
                    .unwrap(),
                "one"
            );
        }

        drop(_first_bindings);
        drop(_second_bindings);
        drop(first);
        drop(second);

        let reopened = Connection::open(&path).unwrap();
        let reopened_registry = Registry::default();
        let reopened_plan = reopened_registry
            .plan(&reopened, "SELECT body FROM notes WHERE id = 1")
            .unwrap()
            .unwrap();
        assert_eq!(reopened_plan.modules[0].name, stable);
    }
}
