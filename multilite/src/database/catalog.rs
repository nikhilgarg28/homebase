//! Local lookup index for durable schema identities.

use rusqlite::{Connection, OptionalExtension, params};

use super::schema::{CreateTable, SqlName, TableId};
use crate::{Error, Result};

const TABLE: &str = "__multilite__schema";
const MAIN_SCHEMA: &str = "main";

pub fn initialize(connection: &Connection) -> Result<()> {
    connection.execute_batch(&format!(
        "CREATE TABLE {TABLE} (
            schema_name TEXT NOT NULL,
            table_name BLOB NOT NULL,
            table_id BLOB NOT NULL UNIQUE CHECK(length(table_id) = 16),
            definition BLOB NOT NULL,
            PRIMARY KEY (schema_name, table_name)
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
        [table] if table == TABLE => Ok(true),
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

    let mut statement = connection.prepare(&format!(
        "SELECT table_id, definition FROM {TABLE} ORDER BY schema_name, table_name"
    ))?;
    let rows = statement.query_map((), |row| {
        Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, Vec<u8>>(1)?))
    })?;
    for row in rows {
        let (table_id, definition) = row?;
        let created = decode_definition(&definition)?;
        if table_id != created.table_id().as_bytes() {
            return Err(Error::InvalidDatabase(
                "schema catalog table id contradicts its definition",
            ));
        }
    }
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
    Ok(())
}

pub fn remove_by_name(connection: &Connection, name: &str) -> Result<()> {
    let name = SqlName::new(name.to_owned());
    connection.execute(
        &format!("DELETE FROM {TABLE} WHERE schema_name = ?1 AND table_name = ?2"),
        params![MAIN_SCHEMA, name.canonical()],
    )?;
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

fn decode_definition(frame: &[u8]) -> Result<CreateTable> {
    CreateTable::decode(frame).map_err(|_| Error::InvalidDatabase("schema catalog is malformed"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::database::schema::{CreateColumn, CreateTableSpec, DeclaredType};

    fn created() -> CreateTable {
        CreateTable::new(
            "CREATE TABLE Notes (id INTEGER PRIMARY KEY, body TEXT)",
            CreateTableSpec {
                name: SqlName::new("Notes".into()),
                columns: vec![
                    CreateColumn {
                        name: SqlName::new("id".into()),
                        declared_type: DeclaredType::Integer,
                        not_null: false,
                        primary_key: true,
                    },
                    CreateColumn {
                        name: SqlName::new("body".into()),
                        declared_type: DeclaredType::Text,
                        not_null: false,
                        primary_key: false,
                    },
                ],
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
}
