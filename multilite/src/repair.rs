//! Local durable sidecars for repairing rejected destructive operations.
//!
//! Logical operations and pending frames contain only replicated metadata.
//! The originating replica keeps destroyed SQLite values here until authority
//! accepts the operation or rejection repair consumes them.

use std::collections::BTreeSet;
use std::fmt;

use homebase_core::reader::Reader;
use homebase_core::writer::Writer;
use rusqlite::functions::FunctionFlags;
use rusqlite::{Connection, ToSql, params, params_from_iter};
use uuid::{Uuid, Variant, Version};

use crate::connection::with_savepoint;
use crate::sqlite::quote_identifier;
use crate::value::StoredValue;
use crate::{Error, Result};

const JOBS_TABLE: &str = "__multilite__repair";
const ROWS_TABLE: &str = "__multilite__repair_rows";
const PACK_FUNCTION: &str = "__multilite__repair_pack";
const DROP_COLUMN_KIND: i64 = 1;
const TUPLE_VERSION: u8 = 1;
const TAG_VALUE: u8 = 1;

const MAX_DROP_COLUMN_REPAIR_ROWS: usize = 100_000;
const MAX_DROP_COLUMN_REPAIR_BYTES: usize = 64 * 1024 * 1024;

pub(crate) type RepairId = [u8; 16];

#[derive(Clone, Copy)]
struct CaptureBudget {
    rows: usize,
    bytes: usize,
}

const DEFAULT_CAPTURE_BUDGET: CaptureBudget = CaptureBudget {
    rows: MAX_DROP_COLUMN_REPAIR_ROWS,
    bytes: MAX_DROP_COLUMN_REPAIR_BYTES,
};

/// Attach the private lossless tuple encoder used by streaming sidecar capture.
pub(crate) fn register(connection: &Connection) -> Result<()> {
    connection.create_scalar_function(
        PACK_FUNCTION,
        -1,
        FunctionFlags::SQLITE_UTF8
            | FunctionFlags::SQLITE_DETERMINISTIC
            | FunctionFlags::SQLITE_DIRECTONLY,
        |context| {
            let mut writer = Writer::new();
            writer.u8(TUPLE_VERSION);
            for index in 0..context.len() {
                writer
                    .field(
                        TAG_VALUE,
                        &StoredValue::capture(context.get_raw(index)).encode(),
                    )
                    .map_err(user_function_error)?;
            }
            Ok(writer.finish())
        },
    )?;
    Ok(())
}

pub(crate) fn initialize(connection: &Connection) -> Result<()> {
    connection.execute_batch(&format!(
        "CREATE TABLE {JOBS_TABLE} (
            mutation_id BLOB PRIMARY KEY NOT NULL CHECK(length(mutation_id) = 16),
            kind INTEGER NOT NULL CHECK(kind = {DROP_COLUMN_KIND}),
            row_count INTEGER NOT NULL CHECK(row_count >= 0),
            payload_bytes INTEGER NOT NULL CHECK(payload_bytes >= 0)
        ) WITHOUT ROWID;
        CREATE TABLE {ROWS_TABLE} (
            mutation_id BLOB NOT NULL CHECK(length(mutation_id) = 16),
            primary_key BLOB NOT NULL,
            value BLOB NOT NULL,
            PRIMARY KEY (mutation_id, primary_key)
        ) WITHOUT ROWID"
    ))?;
    Ok(())
}

pub(crate) fn is_initialized(connection: &Connection) -> Result<bool> {
    let mut statement = connection.prepare(
        "SELECT name FROM sqlite_schema
         WHERE type = 'table'
           AND substr(name, 1, length(?1)) = ?1 COLLATE NOCASE
         ORDER BY name",
    )?;
    let tables = statement
        .query_map([JOBS_TABLE], |row| row.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    match tables.as_slice() {
        [] => Ok(false),
        [jobs, rows] if jobs == JOBS_TABLE && rows == ROWS_TABLE => Ok(true),
        _ => Err(Error::InvalidDatabase(
            "repair sidecar namespace contains unexpected tables",
        )),
    }
}

pub(crate) fn validate(connection: &Connection) -> Result<()> {
    if !is_initialized(connection)? {
        return Err(Error::InvalidDatabase("repair sidecar tables are missing"));
    }
    validate_columns(
        connection,
        JOBS_TABLE,
        &[
            ("mutation_id", "BLOB", true, 1),
            ("kind", "INTEGER", true, 0),
            ("row_count", "INTEGER", true, 0),
            ("payload_bytes", "INTEGER", true, 0),
        ],
    )?;
    validate_columns(
        connection,
        ROWS_TABLE,
        &[
            ("mutation_id", "BLOB", true, 1),
            ("primary_key", "BLOB", true, 2),
            ("value", "BLOB", true, 0),
        ],
    )?;
    validate_without_rowid(connection, JOBS_TABLE)?;
    validate_without_rowid(connection, ROWS_TABLE)?;

    let mut statement = connection.prepare(&format!(
        "SELECT job.mutation_id, job.kind, job.row_count, job.payload_bytes,
                count(row.primary_key),
                coalesce(sum(length(row.primary_key) + length(row.value)), 0)
         FROM {JOBS_TABLE} AS job
         LEFT JOIN {ROWS_TABLE} AS row USING (mutation_id)
         GROUP BY job.mutation_id, job.kind, job.row_count, job.payload_bytes
         ORDER BY job.mutation_id"
    ))?;
    let jobs = statement.query_map((), |row| {
        Ok((
            row.get::<_, Vec<u8>>(0)?,
            row.get::<_, i64>(1)?,
            row.get::<_, i64>(2)?,
            row.get::<_, i64>(3)?,
            row.get::<_, i64>(4)?,
            row.get::<_, i64>(5)?,
        ))
    })?;
    for job in jobs {
        let (id, kind, expected_rows, expected_bytes, actual_rows, actual_bytes) = job?;
        decode_repair_id(&id)?;
        if kind != DROP_COLUMN_KIND
            || expected_rows != actual_rows
            || expected_bytes != actual_bytes
        {
            return Err(Error::InvalidDatabase(
                "repair sidecar job does not match its retained rows",
            ));
        }
    }

    let orphaned: bool = connection.query_row(
        &format!(
            "SELECT EXISTS(
                SELECT 1 FROM {ROWS_TABLE} AS row
                LEFT JOIN {JOBS_TABLE} AS job USING (mutation_id)
                WHERE job.mutation_id IS NULL
            )"
        ),
        (),
        |row| row.get(0),
    )?;
    if orphaned {
        return Err(Error::InvalidDatabase(
            "repair sidecar contains rows without a job",
        ));
    }

    let mut rows = connection.prepare(&format!(
        "SELECT mutation_id, primary_key, value FROM {ROWS_TABLE} ORDER BY mutation_id, primary_key"
    ))?;
    let rows = rows.query_map((), |row| {
        Ok((
            row.get::<_, Vec<u8>>(0)?,
            row.get::<_, Vec<u8>>(1)?,
            row.get::<_, Vec<u8>>(2)?,
        ))
    })?;
    for row in rows {
        let (id, primary, value) = row?;
        decode_repair_id(&id)?;
        let primary = decode_tuple(&primary).map_err(|_| {
            Error::InvalidDatabase("repair sidecar contains a malformed retained value")
        })?;
        let value = decode_tuple(&value).map_err(|_| {
            Error::InvalidDatabase("repair sidecar contains a malformed retained value")
        })?;
        if primary.is_empty() || value.len() != 1 {
            return Err(Error::InvalidDatabase(
                "repair sidecar contains an invalid retained row shape",
            ));
        }
    }
    Ok(())
}

/// Capture one dropped column without retaining table data in Rust memory.
pub(crate) fn capture_drop_column(
    connection: &Connection,
    mutation_id: RepairId,
    table: &str,
    primary_key: &[String],
    column: &str,
) -> Result<()> {
    capture_drop_column_with_budget(
        connection,
        mutation_id,
        table,
        primary_key,
        column,
        DEFAULT_CAPTURE_BUDGET,
    )
}

fn capture_drop_column_with_budget(
    connection: &Connection,
    mutation_id: RepairId,
    table: &str,
    primary_key: &[String],
    column: &str,
    budget: CaptureBudget,
) -> Result<()> {
    if primary_key.is_empty() {
        return Err(Error::CaptureInvariant(
            "DROP COLUMN repair requires a declared primary key",
        ));
    }
    validate_uuid(mutation_id)?;
    let primary_pack = pack_expression(primary_key.iter().map(String::as_str));
    let value_pack = pack_expression([column]);
    let table = quote_identifier(table);
    let (row_count, payload_bytes) = connection.query_row(
        &format!(
            "SELECT count(*),
                    coalesce(sum(length({primary_pack}) + length({value_pack})), 0)
             FROM {table}"
        ),
        (),
        |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
    )?;
    let row_count = usize::try_from(row_count)
        .map_err(|_| Error::CaptureInvariant("DROP COLUMN repair row count is invalid"))?;
    let payload_bytes = usize::try_from(payload_bytes)
        .map_err(|_| Error::CaptureInvariant("DROP COLUMN repair byte count is invalid"))?;
    if row_count > budget.rows {
        return Err(Error::CaptureLimitExceeded {
            resource: "DROP COLUMN repair rows",
            limit: budget.rows,
        });
    }
    if payload_bytes > budget.bytes {
        return Err(Error::CaptureLimitExceeded {
            resource: "DROP COLUMN repair bytes",
            limit: budget.bytes,
        });
    }

    with_savepoint(connection, "__multilite__capture_repair", || {
        connection.execute(
            &format!(
                "INSERT INTO {JOBS_TABLE}
                    (mutation_id, kind, row_count, payload_bytes)
                 VALUES (?1, {DROP_COLUMN_KIND}, ?2, ?3)"
            ),
            params![
                mutation_id.as_slice(),
                i64::try_from(row_count).expect("row limit fits in i64"),
                i64::try_from(payload_bytes).expect("byte limit fits in i64"),
            ],
        )?;
        let inserted = connection.execute(
            &format!(
                "INSERT INTO {ROWS_TABLE} (mutation_id, primary_key, value)
                 SELECT ?1, {primary_pack}, {value_pack} FROM {table}"
            ),
            [mutation_id.as_slice()],
        )?;
        if inserted != row_count {
            return Err(Error::CaptureInvariant(
                "DROP COLUMN repair changed during capture",
            ));
        }
        Ok(())
    })
}

/// Restore a dropped value to every row identified by the captured primary key.
pub(crate) fn restore_drop_column(
    connection: &Connection,
    mutation_id: RepairId,
    table: &str,
    primary_key: &[String],
    column: &str,
) -> Result<()> {
    let job = load_job(connection, mutation_id)?.ok_or(Error::InvalidDatabase(
        "pending DROP COLUMN repair sidecar is missing",
    ))?;
    if job.kind != DROP_COLUMN_KIND {
        return Err(Error::InvalidDatabase(
            "pending DROP COLUMN repair sidecar has the wrong kind",
        ));
    }
    if primary_key.is_empty() {
        return Err(Error::InvalidDatabase(
            "pending DROP COLUMN repair has no primary key",
        ));
    }

    let predicates = primary_key
        .iter()
        .map(|name| format!("{} IS ?", quote_identifier(name)))
        .collect::<Vec<_>>()
        .join(" AND ");
    let sql = format!(
        "UPDATE {} SET {} = ? WHERE {predicates}",
        quote_identifier(table),
        quote_identifier(column),
    );
    let mut update = connection.prepare(&sql)?;
    let mut select = connection.prepare(&format!(
        "SELECT primary_key, value FROM {ROWS_TABLE}
         WHERE mutation_id = ?1 ORDER BY primary_key"
    ))?;
    let mut rows = select.query([mutation_id.as_slice()])?;
    let mut restored = 0usize;
    while let Some(row) = rows.next()? {
        let primary = decode_tuple(&row.get::<_, Vec<u8>>(0)?)
            .map_err(|_| Error::InvalidDatabase("DROP COLUMN repair primary key is malformed"))?;
        let value = decode_tuple(&row.get::<_, Vec<u8>>(1)?)
            .map_err(|_| Error::InvalidDatabase("DROP COLUMN repair value is malformed"))?;
        let [value] = value.as_slice() else {
            return Err(Error::InvalidDatabase(
                "DROP COLUMN repair value has an invalid shape",
            ));
        };
        if primary.len() != primary_key.len() {
            return Err(Error::InvalidDatabase(
                "DROP COLUMN repair primary key has an invalid shape",
            ));
        }
        let mut parameters = Vec::<&dyn ToSql>::with_capacity(primary.len() + 1);
        parameters.push(value);
        parameters.extend(primary.iter().map(|value| value as &dyn ToSql));
        if update.execute(params_from_iter(parameters))? != 1 {
            return Err(Error::InvalidDatabase(
                "pending DROP COLUMN row no longer matches SQLite state",
            ));
        }
        restored += 1;
    }
    if restored != job.row_count {
        return Err(Error::InvalidDatabase(
            "DROP COLUMN repair row count changed after validation",
        ));
    }
    Ok(())
}

/// Delete one repair job and all of its rows after acceptance or restoration.
pub(crate) fn retire(connection: &Connection, mutation_id: RepairId) -> Result<()> {
    connection.execute(
        &format!("DELETE FROM {ROWS_TABLE} WHERE mutation_id = ?1"),
        [mutation_id.as_slice()],
    )?;
    if connection.execute(
        &format!("DELETE FROM {JOBS_TABLE} WHERE mutation_id = ?1"),
        [mutation_id.as_slice()],
    )? != 1
    {
        return Err(Error::InvalidDatabase(
            "pending destructive operation has no repair sidecar",
        ));
    }
    Ok(())
}

pub(crate) fn contains(connection: &Connection, mutation_id: RepairId) -> Result<bool> {
    Ok(connection.query_row(
        &format!("SELECT EXISTS(SELECT 1 FROM {JOBS_TABLE} WHERE mutation_id = ?1)"),
        [mutation_id.as_slice()],
        |row| row.get(0),
    )?)
}

/// Ensure repair jobs correspond exactly to pending destructive operations.
pub(crate) fn validate_expected(
    connection: &Connection,
    expected: impl IntoIterator<Item = RepairId>,
) -> Result<()> {
    let expected = expected.into_iter().collect::<BTreeSet<_>>();
    let mut statement = connection.prepare(&format!(
        "SELECT mutation_id FROM {JOBS_TABLE} ORDER BY mutation_id"
    ))?;
    let actual = statement
        .query_map((), |row| row.get::<_, Vec<u8>>(0))?
        .map(|row| decode_repair_id(&row?))
        .collect::<Result<BTreeSet<_>>>()?;
    if actual != expected {
        return Err(Error::InvalidDatabase(
            "repair sidecars do not match pending destructive operations",
        ));
    }
    Ok(())
}

#[derive(Clone, Copy)]
struct RepairJob {
    kind: i64,
    row_count: usize,
}

fn load_job(connection: &Connection, mutation_id: RepairId) -> Result<Option<RepairJob>> {
    let mut statement = connection.prepare(&format!(
        "SELECT kind, row_count FROM {JOBS_TABLE} WHERE mutation_id = ?1"
    ))?;
    let mut rows = statement.query([mutation_id.as_slice()])?;
    let Some(row) = rows.next()? else {
        return Ok(None);
    };
    let row_count = usize::try_from(row.get::<_, i64>(1)?)
        .map_err(|_| Error::InvalidDatabase("repair sidecar row count is invalid"))?;
    Ok(Some(RepairJob {
        kind: row.get(0)?,
        row_count,
    }))
}

fn pack_expression<'a>(columns: impl IntoIterator<Item = &'a str>) -> String {
    let columns = columns
        .into_iter()
        .map(quote_identifier)
        .collect::<Vec<_>>()
        .join(", ");
    format!("{PACK_FUNCTION}({columns})")
}

fn decode_tuple(frame: &[u8]) -> std::result::Result<Vec<StoredValue>, RepairCodecError> {
    let mut reader = Reader::new(frame);
    if reader.u8() != Some(TUPLE_VERSION) {
        return Err(RepairCodecError::UnknownVersion);
    }
    let mut values = Vec::new();
    while let Some((tag, value)) = reader.field().map_err(|_| RepairCodecError::Truncated)? {
        if tag == TAG_VALUE {
            values.push(StoredValue::decode(value).map_err(|_| RepairCodecError::InvalidValue)?);
        }
    }
    Ok(values)
}

fn decode_repair_id(bytes: &[u8]) -> Result<RepairId> {
    let id = bytes
        .try_into()
        .map_err(|_| Error::InvalidDatabase("repair sidecar identity is malformed"))?;
    validate_uuid(id)?;
    Ok(id)
}

fn validate_uuid(id: RepairId) -> Result<()> {
    let uuid = Uuid::from_bytes(id);
    if uuid.get_version() != Some(Version::Random) || uuid.get_variant() != Variant::RFC4122 {
        return Err(Error::InvalidDatabase(
            "repair sidecar identity is not a UUID v4",
        ));
    }
    Ok(())
}

fn validate_columns(
    connection: &Connection,
    table: &str,
    expected: &[(&str, &str, bool, u32)],
) -> Result<()> {
    let mut statement = connection.prepare(&format!("PRAGMA table_info({table})"))?;
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
    let expected = expected
        .iter()
        .map(|(name, kind, not_null, primary)| {
            ((*name).to_owned(), (*kind).to_owned(), *not_null, *primary)
        })
        .collect::<Vec<_>>();
    if columns != expected {
        return Err(Error::InvalidDatabase(
            "repair sidecar table schema is invalid",
        ));
    }
    Ok(())
}

fn validate_without_rowid(connection: &Connection, table: &str) -> Result<()> {
    let sql: String = connection.query_row(
        "SELECT sql FROM sqlite_schema WHERE type = 'table' AND name = ?1",
        [table],
        |row| row.get(0),
    )?;
    if !sql.to_ascii_uppercase().contains("WITHOUT ROWID") {
        return Err(Error::InvalidDatabase(
            "repair sidecar tables must use WITHOUT ROWID",
        ));
    }
    Ok(())
}

fn user_function_error(error: impl fmt::Display) -> rusqlite::Error {
    rusqlite::Error::UserFunctionError(Box::new(std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        error.to_string(),
    )))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RepairCodecError {
    UnknownVersion,
    Truncated,
    InvalidValue,
}

impl fmt::Display for RepairCodecError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{self:?}")
    }
}

#[cfg(test)]
mod tests {
    use rusqlite::types::ValueRef;

    use super::*;

    fn id(byte: u8) -> RepairId {
        let mut id = [byte; 16];
        id[6] = (id[6] & 0x0f) | 0x40;
        id[8] = (id[8] & 0x3f) | 0x80;
        id
    }

    fn connection() -> Connection {
        let connection = Connection::open_in_memory().unwrap();
        register(&connection).unwrap();
        initialize(&connection).unwrap();
        connection
    }

    #[test]
    fn captures_and_restores_composite_primary_keys_losslessly() {
        let connection = connection();
        connection
            .execute_batch(
                "CREATE TABLE items (
                    tenant TEXT,
                    sequence INTEGER,
                    value BLOB,
                    PRIMARY KEY (tenant, sequence)
                 ) WITHOUT ROWID;
                 INSERT INTO items VALUES
                    ('blob', 1, x'0001'),
                    ('integer', 2, -7),
                    ('null', 3, NULL),
                    ('real', 4, -0.0),
                    ('text', 5, 'hello'),
                    ('text-bytes', 6, CAST(x'ff00' AS TEXT))",
            )
            .unwrap();
        capture_drop_column(
            &connection,
            id(1),
            "items",
            &["tenant".into(), "sequence".into()],
            "value",
        )
        .unwrap();
        connection
            .execute("UPDATE items SET value = x'ff'", ())
            .unwrap();
        restore_drop_column(
            &connection,
            id(1),
            "items",
            &["tenant".into(), "sequence".into()],
            "value",
        )
        .unwrap();

        let values = connection
            .prepare("SELECT value FROM items ORDER BY tenant")
            .unwrap()
            .query_map((), |row| Ok(StoredValue::capture(row.get_ref(0)?)))
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap();
        assert_eq!(
            values,
            [
                StoredValue::Blob(vec![0, 1]),
                StoredValue::Integer(-7),
                StoredValue::Null,
                StoredValue::Real((-0.0_f64).to_bits()),
                StoredValue::Text(b"hello".to_vec()),
                StoredValue::Text(vec![0xff, 0]),
            ]
        );
        retire(&connection, id(1)).unwrap();
        validate_expected(&connection, []).unwrap();
    }

    #[test]
    fn empty_tables_still_create_a_valid_job_marker() {
        let connection = connection();
        connection
            .execute_batch("CREATE TABLE items (id INTEGER PRIMARY KEY, value TEXT)")
            .unwrap();
        capture_drop_column(&connection, id(2), "items", &["id".into()], "value").unwrap();
        assert!(contains(&connection, id(2)).unwrap());
        validate(&connection).unwrap();
        validate_expected(&connection, [id(2)]).unwrap();
    }

    #[test]
    fn capture_limits_refuse_atomically_before_creating_a_job() {
        let connection = connection();
        connection
            .execute_batch(
                "CREATE TABLE items (id INTEGER PRIMARY KEY, value BLOB);
                 INSERT INTO items VALUES (1, zeroblob(32)), (2, zeroblob(32))",
            )
            .unwrap();
        let error = capture_drop_column_with_budget(
            &connection,
            id(3),
            "items",
            &["id".into()],
            "value",
            CaptureBudget { rows: 1, bytes: 1 },
        )
        .unwrap_err();
        assert!(matches!(
            error,
            Error::CaptureLimitExceeded {
                resource: "DROP COLUMN repair rows",
                limit: 1,
            }
        ));
        assert!(!contains(&connection, id(3)).unwrap());

        let error = capture_drop_column_with_budget(
            &connection,
            id(5),
            "items",
            &["id".into()],
            "value",
            CaptureBudget { rows: 10, bytes: 1 },
        )
        .unwrap_err();
        assert!(matches!(
            error,
            Error::CaptureLimitExceeded {
                resource: "DROP COLUMN repair bytes",
                limit: 1,
            }
        ));
        assert!(!contains(&connection, id(5)).unwrap());
    }

    #[test]
    fn tuple_codec_preserves_all_sqlite_storage_classes() {
        let values = [
            StoredValue::Null,
            StoredValue::Integer(i64::MIN),
            StoredValue::Real((-0.0_f64).to_bits()),
            StoredValue::Text(vec![0xff, 0]),
            StoredValue::Blob(vec![0, 1, 2]),
        ];
        let mut writer = Writer::new();
        writer.u8(TUPLE_VERSION);
        for value in &values {
            writer.field(TAG_VALUE, &value.encode()).unwrap();
        }
        assert_eq!(decode_tuple(&writer.finish()).unwrap(), values);
        assert_eq!(
            StoredValue::capture(ValueRef::Text(&[0xff])),
            StoredValue::Text(vec![0xff])
        );
    }

    #[test]
    fn validation_rejects_corrupt_rows_and_namespace_lookalikes() {
        let corrupt = connection();
        corrupt
            .execute_batch(
                "CREATE TABLE items (id INTEGER PRIMARY KEY, value TEXT);
                 INSERT INTO items VALUES (1, 'one')",
            )
            .unwrap();
        capture_drop_column(&corrupt, id(4), "items", &["id".into()], "value").unwrap();
        corrupt
            .execute(
                &format!(
                    "UPDATE {ROWS_TABLE}
                     SET value = zeroblob(length(value))
                     WHERE mutation_id = ?1"
                ),
                [id(4).as_slice()],
            )
            .unwrap();
        assert!(matches!(validate(&corrupt), Err(Error::InvalidDatabase(_))));

        let lookalike = connection();
        lookalike
            .execute_batch("CREATE TABLE __multilite__repair_future (value BLOB)")
            .unwrap();
        assert!(matches!(
            is_initialized(&lookalike),
            Err(Error::InvalidDatabase(
                "repair sidecar namespace contains unexpected tables"
            ))
        ));
    }
}
