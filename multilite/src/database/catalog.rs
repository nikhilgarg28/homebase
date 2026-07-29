//! Local lookup index for durable schema identities.

use std::collections::BTreeSet;

use rusqlite::{Connection, OptionalExtension, params};

use super::schema::{
    ColumnId, CreateTable, ForeignKeyDefinition, NamedIndex, SqlName, TableId,
    validate_foreign_key_graph,
};
use crate::{Error, Result};

const TABLE: &str = "__multilite__schema";
const COLUMN_TABLE: &str = "__multilite__schema_columns";
const MAIN_SCHEMA: &str = "main";

pub fn initialize(connection: &Connection) -> Result<()> {
    connection.execute_batch(&format!(
        "CREATE TABLE {TABLE} (
            schema_name TEXT NOT NULL,
            table_name BLOB NOT NULL,
            table_id BLOB NOT NULL UNIQUE CHECK(length(table_id) = 16),
            definition BLOB NOT NULL,
            PRIMARY KEY (schema_name, table_name)
        ) WITHOUT ROWID;
        CREATE TABLE {COLUMN_TABLE} (
            table_id BLOB NOT NULL CHECK(length(table_id) = 16),
            column_id BLOB NOT NULL CHECK(length(column_id) = 16),
            predecessor_id BLOB CHECK(predecessor_id IS NULL OR length(predecessor_id) = 16),
            column_name BLOB,
            PRIMARY KEY (table_id, column_id),
            UNIQUE (table_id, column_name)
        ) WITHOUT ROWID"
    ))?;
    Ok(())
}

pub fn is_initialized(connection: &Connection) -> Result<bool> {
    let mut statement = connection.prepare(
        "SELECT name FROM sqlite_schema
         WHERE type = 'table'
           AND substr(name, 1, length(?1)) = ?1 COLLATE NOCASE
         ORDER BY name",
    )?;
    let tables = statement
        .query_map([TABLE], |row| row.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    match tables.as_slice() {
        [] => Ok(false),
        [table, columns] if table == TABLE && columns == COLUMN_TABLE => Ok(true),
        _ => Err(Error::InvalidDatabase(
            "schema catalog namespace contains unexpected tables",
        )),
    }
}

pub fn validate(connection: &Connection) -> Result<()> {
    if !is_initialized(connection)? {
        return Err(Error::InvalidDatabase("schema catalog is missing"));
    }
    let mut statement = connection.prepare(&format!("PRAGMA table_info({TABLE})"))?;
    let columns = statement
        .query_map((), |row| {
            Ok((
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, bool>(3)?,
                row.get::<_, u32>(5)?,
            ))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let expected = vec![
        (String::from("schema_name"), String::from("TEXT"), true, 1),
        (String::from("table_name"), String::from("BLOB"), true, 2),
        (String::from("table_id"), String::from("BLOB"), true, 0),
        (String::from("definition"), String::from("BLOB"), true, 0),
    ];
    if columns != expected {
        return Err(Error::InvalidDatabase("schema catalog layout is invalid"));
    }
    let schema_sql: String = connection.query_row(
        "SELECT sql FROM sqlite_schema WHERE type = 'table' AND name = ?1",
        [TABLE],
        |row| row.get(0),
    )?;
    if !schema_sql.to_ascii_uppercase().contains("WITHOUT ROWID") {
        return Err(Error::InvalidDatabase(
            "schema catalog must use WITHOUT ROWID",
        ));
    }
    let mut statement = connection.prepare(&format!("PRAGMA table_info({COLUMN_TABLE})"))?;
    let columns = statement
        .query_map((), |row| {
            Ok((
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, bool>(3)?,
                row.get::<_, u32>(5)?,
            ))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let expected = vec![
        (String::from("table_id"), String::from("BLOB"), true, 1),
        (String::from("column_id"), String::from("BLOB"), true, 2),
        (
            String::from("predecessor_id"),
            String::from("BLOB"),
            false,
            0,
        ),
        (String::from("column_name"), String::from("BLOB"), false, 0),
    ];
    if columns != expected {
        return Err(Error::InvalidDatabase(
            "schema column catalog layout is invalid",
        ));
    }
    let schema_sql: String = connection.query_row(
        "SELECT sql FROM sqlite_schema WHERE type = 'table' AND name = ?1",
        [COLUMN_TABLE],
        |row| row.get(0),
    )?;
    if !schema_sql.to_ascii_uppercase().contains("WITHOUT ROWID") {
        return Err(Error::InvalidDatabase(
            "schema column catalog must use WITHOUT ROWID",
        ));
    }

    let mut statement = connection.prepare(&format!(
        "SELECT schema_name, table_name, table_id, definition
         FROM {TABLE} ORDER BY schema_name, table_name"
    ))?;
    let rows = statement.query_map((), |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, Vec<u8>>(1)?,
            row.get::<_, Vec<u8>>(2)?,
            row.get::<_, Vec<u8>>(3)?,
        ))
    })?;
    let mut active_indexes = BTreeSet::new();
    let mut tables = Vec::new();
    for row in rows {
        let (schema_name, table_name, table_id, definition) = row?;
        let binding = decode_binding(&table_name)?;
        let created = decode_definition(&definition)?;
        if schema_name != MAIN_SCHEMA
            || binding.canonical() != table_name
            || table_id != created.table_id().as_bytes()
        {
            return Err(Error::InvalidDatabase(
                "schema catalog binding contradicts its definition",
            ));
        }
        if created
            .indexes()
            .iter()
            .filter(|index| index.is_active())
            .any(|index| !active_indexes.insert(index.name().canonical().to_vec()))
        {
            return Err(Error::InvalidDatabase(
                "schema catalog contains duplicate active index names",
            ));
        }
        let bindings = column_bindings(connection, created.table_id())?;
        if bindings.len() != created.columns().len()
            || created.columns().iter().any(|column| {
                !bindings.iter().any(|(id, name)| {
                    *id == column.id() && name.canonical() == column.name().canonical()
                })
            })
            || column_order(connection, created.table_id())?
                != created
                    .columns()
                    .iter()
                    .map(|column| column.id())
                    .collect::<Vec<_>>()
        {
            return Err(Error::InvalidDatabase(
                "schema column bindings contradict their definition",
            ));
        }
        tables.push(created);
    }
    let orphaned_columns: bool = connection.query_row(
        &format!(
            "SELECT EXISTS(
                SELECT 1 FROM {COLUMN_TABLE} AS columns
                LEFT JOIN {TABLE} AS tables ON tables.table_id = columns.table_id
                WHERE tables.table_id IS NULL
            )"
        ),
        (),
        |row| row.get(0),
    )?;
    if orphaned_columns {
        return Err(Error::InvalidDatabase(
            "schema column catalog contains an unknown table identity",
        ));
    }
    validate_foreign_key_graph(&tables)?;
    Ok(())
}

pub fn insert(connection: &Connection, created: &CreateTable) -> Result<()> {
    connection.execute(
        &format!(
            "INSERT INTO {TABLE} (schema_name, table_name, table_id, definition)
             VALUES (?1, ?2, ?3, ?4)"
        ),
        params![
            MAIN_SCHEMA,
            created.table_name_identity().canonical(),
            created.table_id().as_bytes().as_slice(),
            created.encode(),
        ],
    )?;
    let mut statement = connection.prepare(&format!(
        "INSERT INTO {COLUMN_TABLE} (
            table_id, column_id, predecessor_id, column_name
         ) VALUES (?1, ?2, ?3, ?4)"
    ))?;
    let mut predecessor = None::<ColumnId>;
    for column in created.columns() {
        let predecessor_bytes = predecessor.map(ColumnId::as_bytes);
        statement.execute(params![
            created.table_id().as_bytes().as_slice(),
            column.id().as_bytes().as_slice(),
            predecessor_bytes.as_ref().map(<[u8; 16]>::as_slice),
            column.name().canonical(),
        ])?;
        predecessor = Some(column.id());
    }
    Ok(())
}

pub fn replace(connection: &Connection, definition: &CreateTable) -> Result<()> {
    let changed = connection.execute(
        &format!(
            "UPDATE {TABLE} SET definition = ?1
             WHERE schema_name = ?2 AND table_id = ?3"
        ),
        params![
            definition.encode(),
            MAIN_SCHEMA,
            definition.table_id().as_bytes().as_slice(),
        ],
    )?;
    if changed != 1 {
        return Err(Error::InvalidDatabase(
            "schema catalog table changed during DDL",
        ));
    }
    Ok(())
}

#[cfg(test)]
pub fn remove_by_name(connection: &Connection, name: &str) -> Result<()> {
    let definition = by_name(connection, name)?;
    if let Some(definition) = definition {
        remove_by_id(connection, definition.table_id())?;
    }
    Ok(())
}

pub fn remove_by_id(connection: &Connection, table: TableId) -> Result<()> {
    connection.execute(
        &format!("DELETE FROM {COLUMN_TABLE} WHERE table_id = ?1"),
        [table.as_bytes().as_slice()],
    )?;
    let changed = connection.execute(
        &format!("DELETE FROM {TABLE} WHERE schema_name = ?1 AND table_id = ?2"),
        params![MAIN_SCHEMA, table.as_bytes().as_slice()],
    )?;
    if changed != 1 {
        return Err(Error::InvalidDatabase(
            "schema catalog table changed during removal",
        ));
    }
    Ok(())
}

/// Return the current SQLite spelling bound to one stable column identity.
pub fn column_name_by_id(
    connection: &Connection,
    table: TableId,
    column: ColumnId,
) -> Result<Option<SqlName>> {
    let name = connection
        .query_row(
            &format!(
                "SELECT column_name FROM {COLUMN_TABLE}
                 WHERE table_id = ?1 AND column_id = ?2 AND column_name IS NOT NULL"
            ),
            params![table.as_bytes().as_slice(), column.as_bytes().as_slice()],
            |row| row.get::<_, Vec<u8>>(0),
        )
        .optional()?;
    name.map(|name| decode_column_binding(&name)).transpose()
}

/// Resolve one current SQL column name to its stable identity.
pub fn column_id_by_name(
    connection: &Connection,
    table: TableId,
    name: &SqlName,
) -> Result<Option<ColumnId>> {
    let id = connection
        .query_row(
            &format!(
                "SELECT column_id FROM {COLUMN_TABLE}
                 WHERE table_id = ?1 AND column_name = ?2"
            ),
            params![table.as_bytes().as_slice(), name.canonical()],
            |row| row.get::<_, Vec<u8>>(0),
        )
        .optional()?;
    id.map(|id| decode_column_id(&id)).transpose()
}

/// Move only the mutable name binding for one stable column identity.
pub fn rename_column_binding(
    connection: &Connection,
    table: TableId,
    column: ColumnId,
    expected: &SqlName,
    replacement: &SqlName,
) -> Result<()> {
    let changed = connection.execute(
        &format!(
            "UPDATE {COLUMN_TABLE} SET column_name = ?1
             WHERE table_id = ?2 AND column_name = ?3 AND column_id = ?4"
        ),
        params![
            replacement.canonical(),
            table.as_bytes().as_slice(),
            expected.canonical(),
            column.as_bytes().as_slice(),
        ],
    )?;
    if changed != 1 {
        return Err(Error::InvalidDatabase(
            "schema catalog column changed during rename",
        ));
    }
    Ok(())
}

pub fn insert_column_binding(
    connection: &Connection,
    table: TableId,
    column: ColumnId,
    predecessor: ColumnId,
    name: &SqlName,
) -> Result<()> {
    connection.execute(
        &format!(
            "INSERT INTO {COLUMN_TABLE} (
                table_id, column_id, predecessor_id, column_name
             ) VALUES (?1, ?2, ?3, ?4)"
        ),
        params![
            table.as_bytes().as_slice(),
            column.as_bytes().as_slice(),
            predecessor.as_bytes().as_slice(),
            name.canonical(),
        ],
    )?;
    Ok(())
}

pub fn retire_column_binding(
    connection: &Connection,
    table: TableId,
    column: ColumnId,
) -> Result<()> {
    let changed = connection.execute(
        &format!(
            "UPDATE {COLUMN_TABLE} SET column_name = NULL
             WHERE table_id = ?1 AND column_id = ?2 AND column_name IS NOT NULL"
        ),
        params![table.as_bytes().as_slice(), column.as_bytes().as_slice()],
    )?;
    if changed != 1 {
        return Err(Error::InvalidDatabase(
            "schema catalog column changed during removal",
        ));
    }
    Ok(())
}

pub fn restore_column_binding(
    connection: &Connection,
    table: TableId,
    column: ColumnId,
    name: &SqlName,
) -> Result<()> {
    let changed = connection.execute(
        &format!(
            "UPDATE {COLUMN_TABLE} SET column_name = ?1
             WHERE table_id = ?2 AND column_id = ?3 AND column_name IS NULL"
        ),
        params![
            name.canonical(),
            table.as_bytes().as_slice(),
            column.as_bytes().as_slice(),
        ],
    )?;
    if changed != 1 {
        return Err(Error::InvalidDatabase(
            "schema catalog column changed during restoration",
        ));
    }
    Ok(())
}

/// Return every current column binding in immutable schema order.
pub fn column_names(connection: &Connection, definition: &CreateTable) -> Result<Vec<SqlName>> {
    definition
        .columns()
        .iter()
        .map(|column| {
            column_name_by_id(connection, definition.table_id(), column.id())?.ok_or(
                Error::InvalidDatabase("schema column identity has no current name binding"),
            )
        })
        .collect()
}

fn column_bindings(connection: &Connection, table: TableId) -> Result<Vec<(ColumnId, SqlName)>> {
    let mut statement = connection.prepare(&format!(
        "SELECT column_id, column_name FROM {COLUMN_TABLE}
         WHERE table_id = ?1 AND column_name IS NOT NULL ORDER BY column_name"
    ))?;
    statement
        .query_map([table.as_bytes().as_slice()], |row| {
            Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, Vec<u8>>(1)?))
        })?
        .map(|row| {
            let (id, name) = row?;
            Ok((decode_column_id(&id)?, decode_column_binding(&name)?))
        })
        .collect()
}

/// Linearize active column identities from their durable predecessor graph.
pub fn column_order(connection: &Connection, table: TableId) -> Result<Vec<ColumnId>> {
    let mut statement = connection.prepare(&format!(
        "SELECT column_id, predecessor_id, column_name
         FROM {COLUMN_TABLE} WHERE table_id = ?1 ORDER BY column_id"
    ))?;
    let rows = statement
        .query_map([table.as_bytes().as_slice()], |row| {
            Ok((
                row.get::<_, Vec<u8>>(0)?,
                row.get::<_, Option<Vec<u8>>>(1)?,
                row.get::<_, Option<Vec<u8>>>(2)?,
            ))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let mut nodes = std::collections::BTreeMap::new();
    for (column, predecessor, name) in rows {
        let column = decode_column_id(&column)?;
        let predecessor = predecessor.as_deref().map(decode_column_id).transpose()?;
        if nodes
            .insert(column, (predecessor, name.is_some()))
            .is_some()
        {
            return Err(Error::InvalidDatabase(
                "schema column catalog contains a duplicate identity",
            ));
        }
    }
    if nodes.is_empty() {
        return Err(Error::InvalidDatabase(
            "schema column catalog has no ordering root",
        ));
    }

    let mut children = std::collections::BTreeMap::<Option<ColumnId>, Vec<ColumnId>>::new();
    for (column, (predecessor, _)) in &nodes {
        if predecessor.is_some_and(|predecessor| !nodes.contains_key(&predecessor)) {
            return Err(Error::InvalidDatabase(
                "schema column predecessor is unknown",
            ));
        }
        children.entry(*predecessor).or_default().push(*column);
    }
    if children.get(&None).is_none_or(|roots| roots.len() != 1) {
        return Err(Error::InvalidDatabase(
            "schema column catalog has an invalid ordering root",
        ));
    }
    for siblings in children.values_mut() {
        siblings.sort_unstable();
    }

    fn visit(
        parent: Option<ColumnId>,
        nodes: &std::collections::BTreeMap<ColumnId, (Option<ColumnId>, bool)>,
        children: &std::collections::BTreeMap<Option<ColumnId>, Vec<ColumnId>>,
        visited: &mut BTreeSet<ColumnId>,
        active: &mut Vec<ColumnId>,
    ) -> Result<()> {
        for column in children.get(&parent).into_iter().flatten() {
            if !visited.insert(*column) {
                return Err(Error::InvalidDatabase(
                    "schema column predecessor graph contains a cycle",
                ));
            }
            if nodes[column].1 {
                active.push(*column);
            }
            visit(Some(*column), nodes, children, visited, active)?;
        }
        Ok(())
    }

    let mut visited = BTreeSet::new();
    let mut active = Vec::new();
    visit(None, &nodes, &children, &mut visited, &mut active)?;
    if visited.len() != nodes.len() || active.is_empty() {
        return Err(Error::InvalidDatabase(
            "schema column predecessor graph is disconnected",
        ));
    }
    Ok(active)
}

/// Return the current SQLite spelling bound to a stable table identity.
pub fn name_by_id(connection: &Connection, table: TableId) -> Result<Option<SqlName>> {
    let name = connection
        .query_row(
            &format!(
                "SELECT table_name FROM {TABLE}
                 WHERE schema_name = ?1 AND table_id = ?2"
            ),
            params![MAIN_SCHEMA, table.as_bytes().as_slice()],
            |row| row.get::<_, Vec<u8>>(0),
        )
        .optional()?;
    name.map(|name| decode_binding(&name)).transpose()
}

/// Move only the mutable name binding for one stable table identity.
#[allow(dead_code, reason = "used by the table-rename operation")]
pub fn rename_binding(
    connection: &Connection,
    table: TableId,
    expected: &SqlName,
    replacement: &SqlName,
) -> Result<()> {
    let changed = connection.execute(
        &format!(
            "UPDATE {TABLE} SET table_name = ?1
             WHERE schema_name = ?2 AND table_name = ?3 AND table_id = ?4"
        ),
        params![
            replacement.canonical(),
            MAIN_SCHEMA,
            expected.canonical(),
            table.as_bytes().as_slice(),
        ],
    )?;
    if changed != 1 {
        return Err(Error::InvalidDatabase(
            "schema catalog table changed during rename",
        ));
    }
    Ok(())
}

pub fn by_name(connection: &Connection, name: &str) -> Result<Option<CreateTable>> {
    let name = SqlName::new(name.to_owned());
    let definition = connection
        .query_row(
            &format!(
                "SELECT definition FROM {TABLE}
                 WHERE schema_name = ?1 AND table_name = ?2"
            ),
            params![MAIN_SCHEMA, name.canonical()],
            |row| row.get::<_, Vec<u8>>(0),
        )
        .optional()?;
    definition
        .map(|frame| decode_definition(&frame))
        .transpose()
}

pub fn by_id(connection: &Connection, table: TableId) -> Result<Option<CreateTable>> {
    let definition = connection
        .query_row(
            &format!("SELECT definition FROM {TABLE} WHERE table_id = ?1"),
            [table.as_bytes().as_slice()],
            |row| row.get::<_, Vec<u8>>(0),
        )
        .optional()?;
    definition
        .map(|frame| decode_definition(&frame))
        .transpose()
}

pub fn all(connection: &Connection) -> Result<Vec<CreateTable>> {
    let mut statement = connection.prepare(&format!(
        "SELECT definition FROM {TABLE} ORDER BY schema_name, table_name"
    ))?;
    statement
        .query_map((), |row| row.get::<_, Vec<u8>>(0))?
        .map(|row| decode_definition(&row?))
        .collect()
}

pub fn incoming_foreign_keys(
    connection: &Connection,
    parent: TableId,
) -> Result<Vec<(CreateTable, ForeignKeyDefinition)>> {
    let mut incoming = Vec::new();
    for child in all(connection)? {
        incoming.extend(
            child
                .foreign_keys()
                .iter()
                .filter(|foreign_key| foreign_key.referenced_table() == parent)
                .cloned()
                .map(|foreign_key| (child.clone(), foreign_key)),
        );
    }
    Ok(incoming)
}

pub fn index_by_name(
    connection: &Connection,
    name: &SqlName,
) -> Result<Option<(CreateTable, NamedIndex)>> {
    let mut statement = connection.prepare(&format!(
        "SELECT definition FROM {TABLE} ORDER BY schema_name, table_name"
    ))?;
    let definitions = statement
        .query_map((), |row| row.get::<_, Vec<u8>>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let mut found = None;
    for definition in definitions {
        let table = decode_definition(&definition)?;
        if let Some(index) = table.index_named(name) {
            if found.is_some() {
                return Err(Error::InvalidDatabase(
                    "schema catalog contains duplicate index names",
                ));
            }
            found = Some((table.clone(), index.clone()));
        }
    }
    Ok(found)
}

fn decode_definition(frame: &[u8]) -> Result<CreateTable> {
    CreateTable::decode(frame).map_err(|_| Error::InvalidDatabase("schema catalog is malformed"))
}

fn decode_binding(bytes: &[u8]) -> Result<SqlName> {
    let value = String::from_utf8(bytes.to_vec())
        .map_err(|_| Error::InvalidDatabase("schema catalog table name is not UTF-8"))?;
    Ok(SqlName::new(value))
}

fn decode_column_binding(bytes: &[u8]) -> Result<SqlName> {
    let value = String::from_utf8(bytes.to_vec())
        .map_err(|_| Error::InvalidDatabase("schema catalog column name is not UTF-8"))?;
    let name = SqlName::new(value);
    if name.canonical() != bytes {
        return Err(Error::InvalidDatabase(
            "schema catalog column name is not canonical",
        ));
    }
    Ok(name)
}

fn decode_column_id(bytes: &[u8]) -> Result<ColumnId> {
    let bytes = bytes
        .try_into()
        .map_err(|_| Error::InvalidDatabase("schema catalog column identity is malformed"))?;
    Ok(ColumnId::from_bytes(bytes))
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use super::*;
    use crate::database::schema::{CreateColumn, CreateTableSpec, TypeDeclaration};

    fn created() -> CreateTable {
        CreateTable::new(
            "CREATE TABLE Notes (id INTEGER PRIMARY KEY, body TEXT)",
            CreateTableSpec {
                name: SqlName::new("Notes".into()),
                mode: Default::default(),
                storage: crate::database::schema::TableStorage::Rowid,
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
                        not_null: false,
                        not_null_name: None,
                        default: None,
                        primary_key: None,
                    },
                ],
                unique_constraints: Vec::new(),
                foreign_keys: Vec::new(),
                primary_key_name: None,
                checks: Vec::new(),
            },
        )
    }

    #[test]
    fn catalog_roundtrips_by_case_insensitive_name_and_stable_id() {
        let connection = Connection::open_in_memory().unwrap();
        initialize(&connection).unwrap();
        let created = created();
        insert(&connection, &created).unwrap();

        assert_eq!(
            by_name(&connection, "nOtEs").unwrap(),
            Some(created.clone())
        );
        assert_eq!(
            by_id(&connection, created.table_id()).unwrap(),
            Some(created)
        );
        validate(&connection).unwrap();
    }

    #[test]
    fn mutable_name_bindings_do_not_rewrite_immutable_definitions() {
        let connection = Connection::open_in_memory().unwrap();
        initialize(&connection).unwrap();
        let created = created();
        let encoded = created.encode();
        insert(&connection, &created).unwrap();
        let replacement = SqlName::new("Archived Notes".into());

        rename_binding(
            &connection,
            created.table_id(),
            created.table_name_identity(),
            &replacement,
        )
        .unwrap();

        assert!(by_name(&connection, "notes").unwrap().is_none());
        assert_eq!(
            by_name(&connection, "ARCHIVED NOTES").unwrap(),
            Some(created.clone())
        );
        assert_eq!(
            name_by_id(&connection, created.table_id()).unwrap(),
            Some(SqlName::new("archived notes".into()))
        );
        assert_eq!(
            by_id(&connection, created.table_id())
                .unwrap()
                .unwrap()
                .encode(),
            encoded
        );
        validate(&connection).unwrap();
    }

    #[test]
    fn validation_rejects_missing_and_orphaned_column_bindings() {
        let missing = Connection::open_in_memory().unwrap();
        initialize(&missing).unwrap();
        let created = created();
        insert(&missing, &created).unwrap();
        missing
            .execute(
                &format!("DELETE FROM {COLUMN_TABLE} WHERE column_id = ?1"),
                [created.columns()[1].id().as_bytes().as_slice()],
            )
            .unwrap();
        assert!(matches!(
            validate(&missing),
            Err(Error::InvalidDatabase(
                "schema column bindings contradict their definition"
            ))
        ));

        let orphaned = Connection::open_in_memory().unwrap();
        initialize(&orphaned).unwrap();
        orphaned
            .execute(
                &format!(
                    "INSERT INTO {COLUMN_TABLE} (
                        table_id, column_id, predecessor_id, column_name
                     ) VALUES (?1, ?2, NULL, ?3)"
                ),
                params![[1_u8; 16].as_slice(), [2_u8; 16].as_slice(), b"body"],
            )
            .unwrap();
        assert!(matches!(
            validate(&orphaned),
            Err(Error::InvalidDatabase(
                "schema column catalog contains an unknown table identity"
            ))
        ));
    }

    #[test]
    fn tombstoned_predecessors_keep_live_descendants_in_order() {
        let connection = Connection::open_in_memory().unwrap();
        initialize(&connection).unwrap();
        let created = created();
        insert(&connection, &created).unwrap();
        let id = created.columns()[0].id();
        let body = created.columns()[1].id();
        let tail = ColumnId::from_bytes(Uuid::new_v4().into_bytes());
        insert_column_binding(
            &connection,
            created.table_id(),
            tail,
            body,
            &SqlName::new("tail".into()),
        )
        .unwrap();

        assert_eq!(
            column_order(&connection, created.table_id()).unwrap(),
            [id, body, tail]
        );
        retire_column_binding(&connection, created.table_id(), body).unwrap();
        assert_eq!(
            column_order(&connection, created.table_id()).unwrap(),
            [id, tail]
        );
        restore_column_binding(
            &connection,
            created.table_id(),
            body,
            &SqlName::new("body".into()),
        )
        .unwrap();
        assert_eq!(
            column_order(&connection, created.table_id()).unwrap(),
            [id, body, tail]
        );
    }

    #[test]
    fn column_order_rejects_unknown_and_disconnected_predecessors() {
        let connection = Connection::open_in_memory().unwrap();
        initialize(&connection).unwrap();
        let created = created();
        insert(&connection, &created).unwrap();
        let body = created.columns()[1].id();
        connection
            .execute(
                &format!(
                    "UPDATE {COLUMN_TABLE} SET predecessor_id = ?1
                     WHERE table_id = ?2 AND column_id = ?3"
                ),
                params![
                    Uuid::new_v4().into_bytes().as_slice(),
                    created.table_id().as_bytes().as_slice(),
                    body.as_bytes().as_slice(),
                ],
            )
            .unwrap();
        assert!(matches!(
            column_order(&connection, created.table_id()),
            Err(Error::InvalidDatabase(
                "schema column predecessor is unknown"
            ))
        ));

        let disconnected = Connection::open_in_memory().unwrap();
        initialize(&disconnected).unwrap();
        insert(&disconnected, &created).unwrap();
        disconnected
            .execute(
                &format!(
                    "UPDATE {COLUMN_TABLE} SET predecessor_id = column_id
                     WHERE table_id = ?1 AND column_id = ?2"
                ),
                params![
                    created.table_id().as_bytes().as_slice(),
                    body.as_bytes().as_slice(),
                ],
            )
            .unwrap();
        assert!(matches!(
            column_order(&disconnected, created.table_id()),
            Err(Error::InvalidDatabase(
                "schema column predecessor graph is disconnected"
            ))
        ));
    }

    #[test]
    fn catalog_validation_rejects_a_dangling_foreign_key_parent() {
        let connection = Connection::open_in_memory().unwrap();
        initialize(&connection).unwrap();
        let parent = created();
        insert(&connection, &parent).unwrap();
        let sql = "CREATE TABLE children (
            id INTEGER PRIMARY KEY,
            parent_id INTEGER REFERENCES Notes(id)
        )";
        let super::super::sql::ValidatedExecute::CreateTable(spec) =
            super::super::sql::validate_execute(sql).unwrap()
        else {
            unreachable!()
        };
        let child = CreateTable::prepare(&connection, sql, spec).unwrap();
        insert(&connection, &child).unwrap();
        validate(&connection).unwrap();

        remove_by_id(&connection, parent.table_id()).unwrap();
        assert!(matches!(
            validate(&connection),
            Err(Error::InvalidDatabase(
                "foreign key references an unknown parent table"
            ))
        ));
    }
}
