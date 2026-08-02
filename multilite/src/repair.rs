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
const DROP_TABLE_KIND: i64 = 2;
const TUPLE_VERSION: u8 = 1;
const TAG_VALUE: u8 = 1;

const MAX_REPAIR_ROWS: usize = 100_000;
const MAX_REPAIR_BYTES: usize = 64 * 1024 * 1024;

pub(crate) type RepairId = [u8; 16];

/// Durable shape expected for one pending destructive operation.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct RepairSpec {
    pub id: RepairId,
    pub kind: RepairKind,
    pub key_parts: usize,
    pub value_parts: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum RepairKind {
    DropColumn,
    DropTable,
}

impl RepairKind {
    fn code(self) -> i64 {
        match self {
            Self::DropColumn => DROP_COLUMN_KIND,
            Self::DropTable => DROP_TABLE_KIND,
        }
    }

    fn from_code(code: i64) -> Option<Self> {
        match code {
            DROP_COLUMN_KIND => Some(Self::DropColumn),
            DROP_TABLE_KIND => Some(Self::DropTable),
            _ => None,
        }
    }
}

pub(crate) fn drop_column_spec(mutation_id: RepairId, key_parts: usize) -> RepairSpec {
    RepairSpec {
        id: mutation_id,
        kind: RepairKind::DropColumn,
        key_parts,
        value_parts: 1,
    }
}

pub(crate) fn drop_table_spec(
    mutation_id: RepairId,
    key_parts: usize,
    value_parts: usize,
) -> RepairSpec {
    RepairSpec {
        id: mutation_id,
        kind: RepairKind::DropTable,
        key_parts,
        value_parts,
    }
}

#[derive(Clone, Copy)]
struct CaptureBudget {
    rows: usize,
    bytes: usize,
}

const DEFAULT_CAPTURE_BUDGET: CaptureBudget = CaptureBudget {
    rows: MAX_REPAIR_ROWS,
    bytes: MAX_REPAIR_BYTES,
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
            kind INTEGER NOT NULL CHECK(kind IN ({DROP_COLUMN_KIND}, {DROP_TABLE_KIND})),
            key_parts INTEGER NOT NULL CHECK(key_parts > 0),
            value_parts INTEGER NOT NULL CHECK(value_parts > 0),
            row_count INTEGER NOT NULL CHECK(row_count >= 0),
            payload_bytes INTEGER NOT NULL CHECK(payload_bytes >= 0),
            metadata BLOB NOT NULL
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
            ("key_parts", "INTEGER", true, 0),
            ("value_parts", "INTEGER", true, 0),
            ("row_count", "INTEGER", true, 0),
            ("payload_bytes", "INTEGER", true, 0),
            ("metadata", "BLOB", true, 0),
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
        "SELECT job.mutation_id, job.kind, job.key_parts, job.value_parts,
                job.row_count, job.payload_bytes, job.metadata,
                count(row.primary_key),
                coalesce(sum(length(row.primary_key) + length(row.value)), 0)
         FROM {JOBS_TABLE} AS job
         LEFT JOIN {ROWS_TABLE} AS row USING (mutation_id)
         GROUP BY job.mutation_id, job.kind, job.key_parts, job.value_parts,
                  job.row_count, job.payload_bytes
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
            row.get::<_, Vec<u8>>(6)?,
            row.get::<_, i64>(7)?,
            row.get::<_, i64>(8)?,
        ))
    })?;
    for job in jobs {
        let (
            id,
            kind,
            key_parts,
            value_parts,
            expected_rows,
            expected_bytes,
            metadata,
            actual_rows,
            actual_bytes,
        ) = job?;
        decode_repair_id(&id)?;
        if RepairKind::from_code(kind).is_none()
            || key_parts <= 0
            || value_parts <= 0
            || expected_rows != actual_rows
            || expected_bytes != actual_bytes
        {
            return Err(Error::InvalidDatabase(
                "repair sidecar job does not match its retained rows",
            ));
        }
        match RepairKind::from_code(kind).expect("repair kind checked above") {
            RepairKind::DropColumn if !metadata.is_empty() => {
                return Err(Error::InvalidDatabase(
                    "DROP COLUMN repair sidecar has unexpected metadata",
                ));
            }
            RepairKind::DropTable => {
                let state = crate::catalog::TableState::decode(&metadata)?;
                if state.definition().primary_key_columns().count()
                    != usize::try_from(key_parts).unwrap_or(0)
                    || state.definition().columns().len()
                        != usize::try_from(value_parts).unwrap_or(0)
                {
                    return Err(Error::InvalidDatabase(
                        "DROP TABLE repair sidecar contradicts its catalog state",
                    ));
                }
            }
            RepairKind::DropColumn => {}
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
        "SELECT row.mutation_id, row.primary_key, row.value,
                job.key_parts, job.value_parts
         FROM {ROWS_TABLE} AS row JOIN {JOBS_TABLE} AS job USING (mutation_id)
         ORDER BY row.mutation_id, row.primary_key"
    ))?;
    let rows = rows.query_map((), |row| {
        Ok((
            row.get::<_, Vec<u8>>(0)?,
            row.get::<_, Vec<u8>>(1)?,
            row.get::<_, Vec<u8>>(2)?,
            row.get::<_, i64>(3)?,
            row.get::<_, i64>(4)?,
        ))
    })?;
    for row in rows {
        let (id, primary, value, key_parts, value_parts) = row?;
        decode_repair_id(&id)?;
        let primary = decode_tuple(&primary).map_err(|_| {
            Error::InvalidDatabase("repair sidecar contains a malformed retained value")
        })?;
        let value = decode_tuple(&value).map_err(|_| {
            Error::InvalidDatabase("repair sidecar contains a malformed retained value")
        })?;
        if primary.len() != usize::try_from(key_parts).unwrap_or(0)
            || value.len() != usize::try_from(value_parts).unwrap_or(0)
        {
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
    let spec = drop_column_spec(mutation_id, primary_key.len());
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
                    (mutation_id, kind, key_parts, value_parts,
                     row_count, payload_bytes, metadata)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, x'')"
            ),
            params![
                mutation_id.as_slice(),
                spec.kind.code(),
                i64::try_from(spec.key_parts).expect("key-part limit fits in i64"),
                i64::try_from(spec.value_parts).expect("value-part limit fits in i64"),
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

/// Stream every row and the complete local catalog state for a dropped table.
pub(crate) fn capture_drop_table(
    connection: &Connection,
    mutation_id: RepairId,
    table: &str,
    primary_key: &[String],
    columns: &[String],
    metadata: &[u8],
) -> Result<()> {
    capture_drop_table_with_budget(
        connection,
        mutation_id,
        table,
        primary_key,
        columns,
        metadata,
        DEFAULT_CAPTURE_BUDGET,
    )
}

fn capture_drop_table_with_budget(
    connection: &Connection,
    mutation_id: RepairId,
    table: &str,
    primary_key: &[String],
    columns: &[String],
    metadata: &[u8],
    budget: CaptureBudget,
) -> Result<()> {
    if primary_key.is_empty() || columns.is_empty() {
        return Err(Error::CaptureInvariant(
            "DROP TABLE repair requires declared columns and a primary key",
        ));
    }
    validate_uuid(mutation_id)?;
    let spec = drop_table_spec(mutation_id, primary_key.len(), columns.len());
    let state = crate::catalog::TableState::decode(metadata)?;
    if !state.name().value().eq_ignore_ascii_case(table)
        || state.definition().primary_key_columns().count() != primary_key.len()
        || state.definition().columns().len() != columns.len()
    {
        return Err(Error::CaptureInvariant(
            "DROP TABLE repair metadata contradicts its row shape",
        ));
    }
    let projection = primary_key
        .iter()
        .chain(columns)
        .map(|name| quote_identifier(name))
        .collect::<Vec<_>>()
        .join(", ");
    let table = quote_identifier(table);

    with_savepoint(connection, "__multilite__capture_repair", || {
        connection.execute(
            &format!(
                "INSERT INTO {JOBS_TABLE}
                    (mutation_id, kind, key_parts, value_parts,
                     row_count, payload_bytes, metadata)
                 VALUES (?1, ?2, ?3, ?4, 0, 0, ?5)"
            ),
            params![
                mutation_id.as_slice(),
                spec.kind.code(),
                i64::try_from(spec.key_parts).expect("key-part limit fits in i64"),
                i64::try_from(spec.value_parts).expect("value-part limit fits in i64"),
                metadata,
            ],
        )?;

        let mut select = connection.prepare(&format!("SELECT {projection} FROM {table}"))?;
        let mut rows = select.query(())?;
        let mut insert = connection.prepare(&format!(
            "INSERT INTO {ROWS_TABLE} (mutation_id, primary_key, value)
             VALUES (?1, ?2, ?3)"
        ))?;
        let mut row_count = 0usize;
        let mut payload_bytes = 0usize;
        while let Some(row) = rows.next()? {
            let primary = (0..primary_key.len())
                .map(|index| Ok(StoredValue::capture(row.get_ref(index)?)))
                .collect::<rusqlite::Result<Vec<_>>>()?;
            let values = (0..columns.len())
                .map(|index| {
                    Ok(StoredValue::capture(
                        row.get_ref(primary_key.len() + index)?,
                    ))
                })
                .collect::<rusqlite::Result<Vec<_>>>()?;
            let primary = encode_tuple(&primary)?;
            let values = encode_tuple(&values)?;
            row_count = row_count
                .checked_add(1)
                .ok_or(Error::CaptureLimitExceeded {
                    resource: "DROP TABLE repair rows",
                    limit: budget.rows,
                })?;
            payload_bytes = payload_bytes
                .checked_add(primary.len())
                .and_then(|bytes| bytes.checked_add(values.len()))
                .ok_or(Error::CaptureLimitExceeded {
                    resource: "DROP TABLE repair bytes",
                    limit: budget.bytes,
                })?;
            if row_count > budget.rows {
                return Err(Error::CaptureLimitExceeded {
                    resource: "DROP TABLE repair rows",
                    limit: budget.rows,
                });
            }
            if payload_bytes > budget.bytes {
                return Err(Error::CaptureLimitExceeded {
                    resource: "DROP TABLE repair bytes",
                    limit: budget.bytes,
                });
            }
            insert.execute(params![mutation_id.as_slice(), primary, values])?;
        }
        drop(rows);
        drop(select);
        drop(insert);
        if connection.execute(
            &format!(
                "UPDATE {JOBS_TABLE} SET row_count = ?1, payload_bytes = ?2
                 WHERE mutation_id = ?3"
            ),
            params![
                i64::try_from(row_count).expect("row limit fits in i64"),
                i64::try_from(payload_bytes).expect("byte limit fits in i64"),
                mutation_id.as_slice(),
            ],
        )? != 1
        {
            return Err(Error::CaptureInvariant(
                "DROP TABLE repair job changed during capture",
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
    if job.spec.kind != RepairKind::DropColumn {
        return Err(Error::InvalidDatabase(
            "pending DROP COLUMN repair sidecar has the wrong kind",
        ));
    }
    if primary_key.is_empty() {
        return Err(Error::InvalidDatabase(
            "pending DROP COLUMN repair has no primary key",
        ));
    }
    if job.spec.key_parts != primary_key.len() || job.spec.value_parts != 1 {
        return Err(Error::InvalidDatabase(
            "pending DROP COLUMN repair sidecar has the wrong shape",
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

/// Load the opaque local catalog snapshot retained for one dropped table.
pub(crate) fn drop_table_metadata(
    connection: &Connection,
    expected: RepairSpec,
) -> Result<Vec<u8>> {
    let job = load_job(connection, expected.id)?.ok_or(Error::InvalidDatabase(
        "pending DROP TABLE repair sidecar is missing",
    ))?;
    if job.spec != expected {
        return Err(Error::InvalidDatabase(
            "pending DROP TABLE repair sidecar has the wrong kind or shape",
        ));
    }
    Ok(job.metadata)
}

/// Stream retained full-row images into an already recreated table.
pub(crate) fn restore_drop_table_rows(
    connection: &Connection,
    expected: RepairSpec,
    table: &str,
    primary_key: &[String],
    columns: &[String],
) -> Result<()> {
    let job = load_job(connection, expected.id)?.ok_or(Error::InvalidDatabase(
        "pending DROP TABLE repair sidecar is missing",
    ))?;
    if job.spec != expected
        || primary_key.len() != expected.key_parts
        || columns.len() != expected.value_parts
    {
        return Err(Error::InvalidDatabase(
            "pending DROP TABLE repair sidecar has the wrong kind or shape",
        ));
    }
    let primary_positions = primary_key
        .iter()
        .map(|primary| {
            columns
                .iter()
                .position(|column| column.eq_ignore_ascii_case(primary))
                .ok_or(Error::InvalidDatabase(
                    "DROP TABLE repair primary key is absent from its row image",
                ))
        })
        .collect::<Result<Vec<_>>>()?;
    let names = columns
        .iter()
        .map(|name| quote_identifier(name))
        .collect::<Vec<_>>()
        .join(", ");
    let placeholders = std::iter::repeat_n("?", columns.len())
        .collect::<Vec<_>>()
        .join(", ");
    let mut insert = connection.prepare(&format!(
        "INSERT INTO {} ({names}) VALUES ({placeholders})",
        quote_identifier(table)
    ))?;
    let mut select = connection.prepare(&format!(
        "SELECT primary_key, value FROM {ROWS_TABLE}
         WHERE mutation_id = ?1 ORDER BY primary_key"
    ))?;
    let mut rows = select.query([expected.id.as_slice()])?;
    let mut restored = 0usize;
    while let Some(row) = rows.next()? {
        let primary = decode_tuple(&row.get::<_, Vec<u8>>(0)?)
            .map_err(|_| Error::InvalidDatabase("DROP TABLE repair primary key is malformed"))?;
        let values = decode_tuple(&row.get::<_, Vec<u8>>(1)?)
            .map_err(|_| Error::InvalidDatabase("DROP TABLE repair row is malformed"))?;
        if primary.len() != primary_positions.len()
            || values.len() != columns.len()
            || primary
                .iter()
                .zip(&primary_positions)
                .any(|(primary, position)| primary != &values[*position])
        {
            return Err(Error::InvalidDatabase(
                "DROP TABLE repair row has an invalid shape",
            ));
        }
        insert.execute(params_from_iter(&values))?;
        restored += 1;
    }
    if restored != job.row_count {
        return Err(Error::InvalidDatabase(
            "DROP TABLE repair row count changed after validation",
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
    expected: impl IntoIterator<Item = RepairSpec>,
) -> Result<()> {
    let expected = expected.into_iter().collect::<BTreeSet<_>>();
    let mut statement = connection.prepare(&format!(
        "SELECT mutation_id, kind, key_parts, value_parts
         FROM {JOBS_TABLE} ORDER BY mutation_id"
    ))?;
    let actual = statement
        .query_map((), |row| {
            Ok((
                row.get::<_, Vec<u8>>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, i64>(3)?,
            ))
        })?
        .map(|row| {
            let (id, kind, key_parts, value_parts) = row?;
            Ok(RepairSpec {
                id: decode_repair_id(&id)?,
                kind: RepairKind::from_code(kind).ok_or(Error::InvalidDatabase(
                    "repair sidecar job has an unknown kind",
                ))?,
                key_parts: usize::try_from(key_parts)
                    .map_err(|_| Error::InvalidDatabase("repair sidecar key width is invalid"))?,
                value_parts: usize::try_from(value_parts)
                    .map_err(|_| Error::InvalidDatabase("repair sidecar value width is invalid"))?,
            })
        })
        .collect::<Result<BTreeSet<_>>>()?;
    if actual != expected {
        return Err(Error::InvalidDatabase(
            "repair sidecars do not match pending destructive operations",
        ));
    }
    Ok(())
}

#[derive(Clone)]
struct RepairJob {
    spec: RepairSpec,
    row_count: usize,
    metadata: Vec<u8>,
}

fn load_job(connection: &Connection, mutation_id: RepairId) -> Result<Option<RepairJob>> {
    let mut statement = connection.prepare(&format!(
        "SELECT kind, key_parts, value_parts, row_count, metadata
         FROM {JOBS_TABLE} WHERE mutation_id = ?1"
    ))?;
    let mut rows = statement.query([mutation_id.as_slice()])?;
    let Some(row) = rows.next()? else {
        return Ok(None);
    };
    let kind = RepairKind::from_code(row.get(0)?).ok_or(Error::InvalidDatabase(
        "repair sidecar job has an unknown kind",
    ))?;
    let key_parts = usize::try_from(row.get::<_, i64>(1)?)
        .map_err(|_| Error::InvalidDatabase("repair sidecar key width is invalid"))?;
    let value_parts = usize::try_from(row.get::<_, i64>(2)?)
        .map_err(|_| Error::InvalidDatabase("repair sidecar value width is invalid"))?;
    let row_count = usize::try_from(row.get::<_, i64>(3)?)
        .map_err(|_| Error::InvalidDatabase("repair sidecar row count is invalid"))?;
    Ok(Some(RepairJob {
        spec: RepairSpec {
            id: mutation_id,
            kind,
            key_parts,
            value_parts,
        },
        row_count,
        metadata: row.get(4)?,
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

fn encode_tuple(values: &[StoredValue]) -> Result<Vec<u8>> {
    let mut writer = Writer::new();
    writer.u8(TUPLE_VERSION);
    for value in values {
        writer
            .field(TAG_VALUE, &value.encode())
            .map_err(|_| Error::CaptureInvariant("repair tuple field is too large"))?;
    }
    Ok(writer.finish())
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
    fn drop_table_capture_restores_full_composite_rows_and_metadata() {
        let connection = connection();
        crate::catalog::initialize(&connection).unwrap();
        let create_sql = "CREATE TABLE items (
                tenant TEXT,
                sequence INTEGER,
                value BLOB,
                PRIMARY KEY (tenant, sequence)
             ) WITHOUT ROWID";
        connection
            .execute_batch(&format!(
                "{create_sql};
                     INSERT INTO items VALUES
                    ('blob', 1, x'0001'),
                    ('integer', 2, -7),
                    ('null', 3, NULL),
                    ('real', 4, -0.0),
                    ('text', 5, CAST(x'ff00' AS TEXT))"
            ))
            .unwrap();
        let crate::sql::ValidatedExecute::CreateTable(specification) =
            crate::sql::validate_execute(create_sql).unwrap()
        else {
            unreachable!()
        };
        let definition =
            crate::logical::schema::CreateTable::prepare(&connection, create_sql, specification)
                .unwrap();
        crate::catalog::insert(&connection, &definition).unwrap();
        let metadata = crate::catalog::capture_table_state(&connection, definition.table_id())
            .unwrap()
            .encode()
            .unwrap();
        let primary = ["tenant".into(), "sequence".into()];
        let columns = ["tenant".into(), "sequence".into(), "value".into()];
        capture_drop_table(&connection, id(7), "items", &primary, &columns, &metadata).unwrap();
        let spec = drop_table_spec(id(7), 2, 3);
        assert_eq!(drop_table_metadata(&connection, spec).unwrap(), metadata);
        connection
            .execute_batch(
                "DROP TABLE items;
                 CREATE TABLE items (
                    tenant TEXT,
                    sequence INTEGER,
                    value BLOB,
                    PRIMARY KEY (tenant, sequence)
                 ) WITHOUT ROWID",
            )
            .unwrap();
        restore_drop_table_rows(&connection, spec, "items", &primary, &columns).unwrap();
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
                StoredValue::Text(vec![0xff, 0]),
            ]
        );
        validate(&connection).unwrap();
        validate_expected(&connection, [spec]).unwrap();
    }

    #[test]
    fn drop_table_streaming_limits_roll_back_rows_job_and_metadata() {
        let connection = connection();
        crate::catalog::initialize(&connection).unwrap();
        let create_sql = "CREATE TABLE items (id INTEGER PRIMARY KEY, value BLOB)";
        connection
            .execute_batch(&format!(
                "{create_sql};
                     INSERT INTO items VALUES (1, zeroblob(32)), (2, zeroblob(32))"
            ))
            .unwrap();
        let crate::sql::ValidatedExecute::CreateTable(specification) =
            crate::sql::validate_execute(create_sql).unwrap()
        else {
            unreachable!()
        };
        let definition =
            crate::logical::schema::CreateTable::prepare(&connection, create_sql, specification)
                .unwrap();
        crate::catalog::insert(&connection, &definition).unwrap();
        let metadata = crate::catalog::capture_table_state(&connection, definition.table_id())
            .unwrap()
            .encode()
            .unwrap();
        for (repair_id, budget, resource) in [
            (
                id(8),
                CaptureBudget {
                    rows: 1,
                    bytes: 1024,
                },
                "DROP TABLE repair rows",
            ),
            (
                id(9),
                CaptureBudget { rows: 10, bytes: 1 },
                "DROP TABLE repair bytes",
            ),
        ] {
            let error = capture_drop_table_with_budget(
                &connection,
                repair_id,
                "items",
                &["id".into()],
                &["id".into(), "value".into()],
                &metadata,
                budget,
            )
            .unwrap_err();
            assert!(matches!(
                error,
                Error::CaptureLimitExceeded {
                    resource: actual,
                    ..
                } if actual == resource
            ));
            assert!(!contains(&connection, repair_id).unwrap());
            assert_eq!(
                connection
                    .query_row(
                        &format!("SELECT count(*) FROM {ROWS_TABLE} WHERE mutation_id = ?1"),
                        [repair_id.as_slice()],
                        |row| row.get::<_, i64>(0),
                    )
                    .unwrap(),
                0
            );
        }
    }

    #[test]
    fn drop_table_validation_rejects_corrupted_shape_and_catalog_metadata() {
        let connection = connection();
        crate::catalog::initialize(&connection).unwrap();
        let create_sql = "CREATE TABLE items (id INTEGER PRIMARY KEY, value TEXT)";
        connection.execute(create_sql, ()).unwrap();
        let crate::sql::ValidatedExecute::CreateTable(specification) =
            crate::sql::validate_execute(create_sql).unwrap()
        else {
            unreachable!()
        };
        let definition =
            crate::logical::schema::CreateTable::prepare(&connection, create_sql, specification)
                .unwrap();
        crate::catalog::insert(&connection, &definition).unwrap();
        let metadata = crate::catalog::capture_table_state(&connection, definition.table_id())
            .unwrap()
            .encode()
            .unwrap();
        let spec = drop_table_spec(id(10), 1, 2);
        capture_drop_table(
            &connection,
            spec.id,
            "items",
            &["id".into()],
            &["id".into(), "value".into()],
            &metadata,
        )
        .unwrap();

        connection
            .execute(
                &format!("UPDATE {JOBS_TABLE} SET value_parts = 3 WHERE mutation_id = ?1"),
                [spec.id.as_slice()],
            )
            .unwrap();
        assert!(matches!(
            validate(&connection),
            Err(Error::InvalidDatabase(_))
        ));
        connection
            .execute(
                &format!(
                    "UPDATE {JOBS_TABLE} SET value_parts = 2, metadata = x'00' \
                     WHERE mutation_id = ?1"
                ),
                [spec.id.as_slice()],
            )
            .unwrap();
        assert!(matches!(
            validate(&connection),
            Err(Error::InvalidDatabase(_))
        ));
    }

    #[test]
    fn empty_drop_table_capture_still_records_exact_schema_shape() {
        let connection = connection();
        crate::catalog::initialize(&connection).unwrap();
        let create_sql =
            "CREATE TABLE items (tenant TEXT, id INTEGER, PRIMARY KEY (tenant, id)) WITHOUT ROWID";
        connection.execute(create_sql, ()).unwrap();
        let crate::sql::ValidatedExecute::CreateTable(specification) =
            crate::sql::validate_execute(create_sql).unwrap()
        else {
            unreachable!()
        };
        let definition =
            crate::logical::schema::CreateTable::prepare(&connection, create_sql, specification)
                .unwrap();
        crate::catalog::insert(&connection, &definition).unwrap();
        let metadata = crate::catalog::capture_table_state(&connection, definition.table_id())
            .unwrap()
            .encode()
            .unwrap();
        let spec = drop_table_spec(id(11), 2, 2);
        capture_drop_table(
            &connection,
            spec.id,
            "items",
            &["tenant".into(), "id".into()],
            &["tenant".into(), "id".into()],
            &metadata,
        )
        .unwrap();
        validate(&connection).unwrap();
        validate_expected(&connection, [spec]).unwrap();
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
        validate_expected(&connection, [drop_column_spec(id(2), 1)]).unwrap();
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
