//! Local accept/reject effects for speculative Multilite transactions.

use std::collections::BTreeSet;
use std::fmt;

use homebase_client::meta::DeviceOp;
use homebase_core::reader::Reader;
use homebase_core::tag::DeviceSeq;
use homebase_core::writer::Writer;
use rusqlite::{Connection, params};

use crate::catalog;
use crate::commit::history::{self, WriteRegion};
use crate::logical::operation::RejectionEffect;
use crate::logical::transaction::MultiliteTransaction;
use crate::sqlite::quote_identifier;
use crate::{Error, Result};

const TABLE: &str = "__multilite__pending";

const PENDING_FRAME_VERSION: u8 = 3;
const TAG_DEVICE_SEQ: u8 = 1;
const TAG_TRANSACTION: u8 = 2;

/// One speculative Multilite transaction keyed by its Homebase sequence.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PendingTransaction {
    pub seq: DeviceSeq,
    pub transaction: MultiliteTransaction,
}

/// Exact inverse of one authenticated active submit window.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RejectionRepair {
    pending: Vec<PendingTransaction>,
    writes: Vec<WriteRegion>,
    effects: Vec<RejectionEffect>,
}

impl RejectionRepair {
    /// Logical regions changed when this speculative window is removed.
    pub fn writes(&self) -> &[WriteRegion] {
        &self.writes
    }

    /// Apply the inverse effects and retire their pending rows.
    ///
    /// This runs first on a private branch and then in the guarded canonical
    /// rollback transaction. Re-loading makes the canonical application reject
    /// a plan prepared for any other pending window.
    pub fn apply(&self, connection: &Connection) -> Result<()> {
        if load(connection)? != self.pending {
            return Err(Error::StalePushRejection);
        }
        apply_rejection(connection, &self.effects)?;
        if !self.pending.is_empty() {
            connection.execute(&format!("DELETE FROM {TABLE}"), ())?;
        }
        Ok(())
    }
}

impl PendingTransaction {
    fn new(seq: DeviceSeq, transaction: MultiliteTransaction) -> Self {
        Self { seq, transaction }
    }

    #[cfg(test)]
    pub(super) fn rejection(&self) -> Result<Vec<RejectionEffect>> {
        Ok(self.transaction.clone().compile()?.rejection().to_vec())
    }
}

/// Versioned encoding for one complete pending disposition record.
struct PendingCodec;

impl PendingCodec {
    fn encode(pending: &PendingTransaction) -> Result<Vec<u8>> {
        let mut writer = Writer::new();
        writer.u8(PENDING_FRAME_VERSION);
        writer
            .field(TAG_DEVICE_SEQ, &pending.seq.0.to_be_bytes())
            .map_err(|_| pending_field_too_large())?;
        writer
            .field(TAG_TRANSACTION, &pending.transaction.encode()?)
            .map_err(|_| pending_field_too_large())?;
        Ok(writer.finish())
    }

    fn decode(frame: &[u8]) -> std::result::Result<PendingTransaction, PendingCodecError> {
        let mut reader = Reader::new(frame);
        let version = reader.u8().ok_or(PendingCodecError::Truncated)?;
        if version != PENDING_FRAME_VERSION {
            return Err(PendingCodecError::UnknownVersion(version));
        }

        let mut seq = None;
        let mut transaction = None;
        while let Some((tag, value)) = reader.field().map_err(|_| PendingCodecError::Truncated)? {
            match tag {
                TAG_DEVICE_SEQ => set_once(&mut seq, decode_seq(value)?)?,
                TAG_TRANSACTION => set_once(
                    &mut transaction,
                    MultiliteTransaction::decode(value).map_err(|error| {
                        PendingCodecError::InvalidTransaction(error.to_string())
                    })?,
                )?,
                _ => {}
            }
        }

        Ok(PendingTransaction {
            seq: seq.ok_or(PendingCodecError::MissingField(TAG_DEVICE_SEQ))?,
            transaction: transaction.ok_or(PendingCodecError::MissingField(TAG_TRANSACTION))?,
        })
    }
}

fn pending_field_too_large() -> Error {
    Error::InvalidMultiliteTransaction("pending record field is too large".into())
}

pub fn initialize(connection: &Connection) -> Result<()> {
    connection.execute_batch(&format!(
        "CREATE TABLE {TABLE} (
            device_seq BLOB PRIMARY KEY NOT NULL CHECK(length(device_seq) = 8),
            record BLOB NOT NULL
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
            "pending table namespace contains unexpected tables",
        )),
    }
}

pub fn validate(connection: &Connection) -> Result<()> {
    if !is_initialized(connection)? {
        return Err(Error::InvalidDatabase("pending effects table is missing"));
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
        (String::from("device_seq"), String::from("BLOB"), true, 1),
        (String::from("record"), String::from("BLOB"), true, 0),
    ];
    if columns != expected {
        return Err(Error::InvalidDatabase(
            "pending effects table schema is invalid",
        ));
    }
    let schema_sql: String = connection.query_row(
        "SELECT sql FROM sqlite_schema WHERE type = 'table' AND name = ?1",
        [TABLE],
        |row| row.get(0),
    )?;
    if !schema_sql.to_ascii_uppercase().contains("WITHOUT ROWID") {
        return Err(Error::InvalidDatabase(
            "pending effects table must use WITHOUT ROWID",
        ));
    }
    Ok(())
}

pub fn insert(
    connection: &Connection,
    seq: DeviceSeq,
    transaction: &MultiliteTransaction,
) -> Result<()> {
    let pending = PendingTransaction::new(seq, transaction.clone());
    connection.execute(
        &format!("INSERT INTO {TABLE} (device_seq, record) VALUES (?1, ?2)"),
        params![
            seq.0.to_be_bytes().as_slice(),
            PendingCodec::encode(&pending)?,
        ],
    )?;
    Ok(())
}

pub fn load(connection: &Connection) -> Result<Vec<PendingTransaction>> {
    let mut statement = connection.prepare(&format!(
        "SELECT device_seq, record FROM {TABLE} ORDER BY device_seq"
    ))?;
    let rows = statement.query_map((), |row| {
        Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, Vec<u8>>(1)?))
    })?;
    rows.map(|row| {
        let (seq, record) = row?;
        let seq = decode_seq(&seq).map_err(invalid_record)?;
        let pending = PendingCodec::decode(&record).map_err(invalid_record)?;
        if pending.seq != seq {
            return Err(Error::InvalidDatabase(
                "pending record sequence does not match its row key",
            ));
        }
        Ok(pending)
    })
    .collect()
}

/// Retire every definitively accepted pending transaction through `through`.
///
/// The database metadata adapter calls this inside the same SQLite savepoint
/// that advances Homebase's submit neck.
pub fn accept_through(connection: &Connection, through: DeviceSeq) -> Result<()> {
    let accepted = load(connection)?
        .into_iter()
        .take_while(|pending| pending.seq <= through)
        .collect::<Vec<_>>();
    if !accepted.is_empty() {
        connection.execute(
            &format!("DELETE FROM {TABLE} WHERE device_seq <= ?1"),
            [through.0.to_be_bytes().as_slice()],
        )?;
    }
    Ok(())
}

/// Authenticate and prepare the inverse of one exact active Homebase window.
pub fn prepare_rejection(
    connection: &Connection,
    active: &[(DeviceSeq, DeviceOp)],
) -> Result<Option<RejectionRepair>> {
    let expected = active
        .iter()
        .filter_map(|(seq, operation)| matches!(operation, DeviceOp::Commit { .. }).then_some(*seq))
        .collect::<Vec<_>>();
    let pending = load(connection)?;
    let actual = pending
        .iter()
        .map(|pending| pending.seq)
        .collect::<Vec<_>>();
    if actual != expected {
        return Err(Error::InvalidDatabase(
            "pending transactions do not match the active submit window",
        ));
    }

    let mut writes = BTreeSet::new();
    let mut compiled = Vec::with_capacity(pending.len());
    for pending in &pending {
        let transaction = pending.transaction.clone().compile()?;
        writes.extend(history::writes_from_mutations(
            transaction.homebase().mutations(),
        ));
        compiled.push(transaction);
    }
    let effects = compiled
        .into_iter()
        .rev()
        .flat_map(|transaction| transaction.rejection().to_vec())
        .collect();
    Ok((!pending.is_empty()).then(|| RejectionRepair {
        pending,
        writes: writes.into_iter().collect(),
        effects,
    }))
}

/// Prepare and apply a rejection directly in the caller's transaction.
///
/// Kept as the narrow metadata-store boundary; production validates the same
/// plan on a private branch before invoking it canonically.
#[cfg(test)]
pub fn reject_active(
    connection: &Connection,
    active: &[(DeviceSeq, DeviceOp)],
) -> Result<Option<Vec<WriteRegion>>> {
    let Some(repair) = prepare_rejection(connection, active)? else {
        return Ok(None);
    };
    repair.apply(connection)?;
    Ok(Some(repair.writes))
}

/// Verify that every pending transaction still belongs to the active submit log.
pub fn validate_active_from(connection: &Connection, neck: DeviceSeq) -> Result<()> {
    if load(connection)?
        .into_iter()
        .any(|pending| pending.seq < neck)
    {
        return Err(Error::InvalidDatabase(
            "accepted pending transaction was not finalized with its submit trim",
        ));
    }
    Ok(())
}

fn apply_rejection(connection: &Connection, effects: &[RejectionEffect]) -> Result<()> {
    for effect in effects {
        match effect {
            RejectionEffect::RevertAlterTable { operation } => operation.rollback(connection)?,
            RejectionEffect::DropTable { created } => {
                if catalog::by_id(connection, created.table_id())?.as_ref() != Some(created) {
                    return Err(Error::InvalidDatabase(
                        "pending CREATE TABLE no longer matches SQLite state",
                    ));
                }
                let name = catalog::name_by_id(connection, created.table_id())?.ok_or(
                    Error::InvalidDatabase("pending CREATE TABLE has no current name binding"),
                )?;
                connection
                    .execute_batch(&format!("DROP TABLE {}", quote_identifier(name.value())))?;
                catalog::remove_by_id(connection, created.table_id())?;
            }
            RejectionEffect::RestoreRowChanges { changes } => {
                changes.restore_materialized(connection)?
            }
            RejectionEffect::RevertIndex { operation } => operation.rollback(connection)?,
        }
    }
    Ok(())
}

fn set_once<T>(slot: &mut Option<T>, value: T) -> std::result::Result<(), PendingCodecError> {
    if slot.replace(value).is_some() {
        Err(PendingCodecError::DuplicateField)
    } else {
        Ok(())
    }
}

fn decode_seq(bytes: &[u8]) -> std::result::Result<DeviceSeq, PendingCodecError> {
    let bytes: [u8; 8] = bytes
        .try_into()
        .map_err(|_| PendingCodecError::InvalidLength)?;
    Ok(DeviceSeq(u64::from_be_bytes(bytes)))
}

fn invalid_record(_: PendingCodecError) -> Error {
    Error::InvalidDatabase("pending record is malformed")
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum PendingCodecError {
    UnknownVersion(u8),
    Truncated,
    DuplicateField,
    MissingField(u8),
    InvalidLength,
    InvalidTransaction(String),
}

impl fmt::Display for PendingCodecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownVersion(version) => write!(f, "unknown pending frame version {version}"),
            Self::Truncated => f.write_str("pending frame is truncated"),
            Self::DuplicateField => f.write_str("pending frame contains a duplicate field"),
            Self::MissingField(tag) => write!(f, "pending frame is missing field {tag}"),
            Self::InvalidLength => f.write_str("pending field has an invalid length"),
            Self::InvalidTransaction(error) => {
                write!(f, "invalid pending transaction: {error}")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use homebase_client::meta::{DeviceOp, SubmitMode};

    use super::*;
    use crate::logical::alter::AlterTableOperation;
    use crate::logical::operation::{MultiliteOp, RejectionEffect};
    use crate::logical::row::{
        CapturedRow, DeletedRowsFixture, RowChanges, RowSet, StoredValue, UpdatedRowsFixture,
    };
    use crate::logical::schema::{
        CreateColumn, CreateTable, CreateTableSpec, SqlName, TypeDeclaration,
    };

    fn operation(name: &str) -> MultiliteOp {
        MultiliteOp::create_table(
            &format!("CREATE TABLE {name} (id INTEGER PRIMARY KEY)"),
            CreateTableSpec {
                name: SqlName::new(name.into()),
                mode: Default::default(),
                storage: crate::logical::schema::TableStorage::Rowid,
                columns: vec![CreateColumn {
                    name: SqlName::new("id".into()),
                    declared_type: TypeDeclaration::integer(),
                    not_null: false,
                    not_null_name: None,
                    default: None,
                    primary_key: Some(0),
                }],
                unique_constraints: Vec::new(),
                foreign_keys: Vec::new(),
                primary_key_name: None,
                checks: Vec::new(),
            },
        )
    }

    fn insert_row_set() -> RowSet {
        let connection = Connection::open_in_memory().unwrap();
        catalog::initialize(&connection).unwrap();
        let MultiliteOp::CreateTable(created) = operation("notes") else {
            unreachable!()
        };
        connection.execute(created.sql(), ()).unwrap();
        catalog::insert(&connection, &created).unwrap();
        RowSet::from_captured(
            &connection,
            &[CapturedRow {
                table: "notes".into(),
                rowid: 7,
                values: vec![StoredValue::Integer(7)],
            }],
        )
        .unwrap()
        .unwrap()
    }

    fn insert_operation() -> MultiliteOp {
        MultiliteOp::ChangeRows(RowChanges::inserted(insert_row_set()))
    }

    fn delete_operation() -> MultiliteOp {
        let deleted = DeletedRowsFixture::from_row_set(insert_row_set());
        MultiliteOp::ChangeRows(RowChanges::deleted(deleted))
    }

    fn update_operation() -> MultiliteOp {
        let connection = Connection::open_in_memory().unwrap();
        catalog::initialize(&connection).unwrap();
        let created = CreateTable::new(
            "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)",
            CreateTableSpec {
                name: SqlName::new("notes".into()),
                mode: Default::default(),
                storage: crate::logical::schema::TableStorage::Rowid,
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
        );
        connection.execute(created.sql(), ()).unwrap();
        catalog::insert(&connection, &created).unwrap();
        MultiliteOp::ChangeRows(RowChanges::updated(
            UpdatedRowsFixture::from_captured(
                &connection,
                &[(
                    CapturedRow {
                        table: "notes".into(),
                        rowid: 7,
                        values: vec![
                            StoredValue::Integer(7),
                            StoredValue::Text(b"before".to_vec()),
                        ],
                    },
                    CapturedRow {
                        table: "notes".into(),
                        rowid: 8,
                        values: vec![
                            StoredValue::Integer(8),
                            StoredValue::Text(b"after".to_vec()),
                        ],
                    },
                )],
            )
            .unwrap()
            .unwrap(),
        ))
    }

    fn alter_operation() -> MultiliteOp {
        let connection = Connection::open_in_memory().unwrap();
        catalog::initialize(&connection).unwrap();
        let MultiliteOp::CreateTable(created) = operation("notes") else {
            unreachable!()
        };
        connection.execute(created.sql(), ()).unwrap();
        catalog::insert(&connection, &created).unwrap();
        let sql = "ALTER TABLE notes RENAME TO archived_notes";
        let crate::sql::ValidatedExecute::RenameTable(spec) =
            crate::sql::validate_execute(sql).unwrap()
        else {
            unreachable!()
        };
        MultiliteOp::AlterTable(
            AlterTableOperation::prepare_rename_table(&connection, sql, &spec).unwrap(),
        )
    }

    fn transaction(operation: MultiliteOp) -> MultiliteTransaction {
        MultiliteTransaction::new(vec![operation]).unwrap()
    }

    #[test]
    fn journal_roundtrips_transactions_in_sequence_order() {
        let connection = Connection::open_in_memory().unwrap();
        initialize(&connection).unwrap();
        let later = transaction(operation("tasks"));
        let earlier = transaction(operation("notes"));
        let expected_later = PendingTransaction::new(DeviceSeq(9), later.clone());
        let expected_earlier = PendingTransaction::new(DeviceSeq(3), earlier.clone());
        insert(&connection, DeviceSeq(9), &later).unwrap();
        insert(&connection, DeviceSeq(3), &earlier).unwrap();

        assert_eq!(
            load(&connection).unwrap(),
            vec![expected_earlier, expected_later]
        );
    }

    #[test]
    fn codec_roundtrips_and_rejects_unknown_or_truncated_versions() {
        let pending = PendingTransaction::new(DeviceSeq(7), transaction(operation("notes")));
        let encoded = PendingCodec::encode(&pending).unwrap();
        assert_eq!(PendingCodec::decode(&encoded).unwrap(), pending);
        assert_eq!(PendingCodec::decode(&[]), Err(PendingCodecError::Truncated));
        assert_eq!(
            PendingCodec::decode(&[2]),
            Err(PendingCodecError::UnknownVersion(2))
        );
        assert_eq!(
            PendingCodec::decode(&encoded[..encoded.len() - 1]),
            Err(PendingCodecError::Truncated)
        );
    }

    #[test]
    fn codec_and_journal_roundtrip_insert_shaped_changes_and_their_inverse() {
        let operation = insert_operation();
        let transaction = transaction(operation.clone());
        let pending = PendingTransaction::new(DeviceSeq(11), transaction.clone());
        let MultiliteOp::ChangeRows(changes) = &operation else {
            unreachable!()
        };
        assert_eq!(
            pending.rejection().unwrap(),
            vec![RejectionEffect::RestoreRowChanges {
                changes: changes.clone(),
            }]
        );
        assert_eq!(
            PendingCodec::decode(&PendingCodec::encode(&pending).unwrap()).unwrap(),
            pending
        );

        let connection = Connection::open_in_memory().unwrap();
        initialize(&connection).unwrap();
        insert(&connection, DeviceSeq(11), &transaction).unwrap();
        assert_eq!(load(&connection).unwrap(), [pending]);
    }

    #[test]
    fn codec_roundtrips_table_rename_and_its_inverse_binding_effect() {
        let operation = alter_operation();
        let transaction = transaction(operation.clone());
        let pending = PendingTransaction::new(DeviceSeq(12), transaction);
        let MultiliteOp::AlterTable(operation) = operation else {
            unreachable!()
        };
        assert_eq!(
            pending.rejection().unwrap(),
            [RejectionEffect::RevertAlterTable {
                operation: operation.clone(),
            }]
        );
        assert_eq!(
            PendingCodec::decode(&PendingCodec::encode(&pending).unwrap()).unwrap(),
            pending
        );
        assert_eq!(
            MultiliteOp::decode(&MultiliteOp::AlterTable(operation.clone()).encode()).unwrap(),
            MultiliteOp::AlterTable(operation)
        );
    }

    #[test]
    fn codec_and_journal_roundtrip_delete_shaped_changes_and_their_inverse() {
        let operation = delete_operation();
        let transaction = transaction(operation.clone());
        let pending = PendingTransaction::new(DeviceSeq(12), transaction.clone());
        let MultiliteOp::ChangeRows(changes) = &operation else {
            unreachable!()
        };
        assert_eq!(
            pending.rejection().unwrap(),
            vec![RejectionEffect::RestoreRowChanges {
                changes: changes.clone(),
            }]
        );
        assert_eq!(
            PendingCodec::decode(&PendingCodec::encode(&pending).unwrap()).unwrap(),
            pending
        );

        let connection = Connection::open_in_memory().unwrap();
        initialize(&connection).unwrap();
        insert(&connection, DeviceSeq(12), &transaction).unwrap();
        assert_eq!(load(&connection).unwrap(), [pending]);
    }

    #[test]
    fn codec_and_journal_roundtrip_update_shaped_changes_and_their_inverse() {
        let operation = update_operation();
        let transaction = transaction(operation.clone());
        let pending = PendingTransaction::new(DeviceSeq(13), transaction.clone());
        let MultiliteOp::ChangeRows(changes) = &operation else {
            unreachable!()
        };
        assert_eq!(
            pending.rejection().unwrap(),
            vec![RejectionEffect::RestoreRowChanges {
                changes: changes.clone(),
            }]
        );
        assert_eq!(
            PendingCodec::decode(&PendingCodec::encode(&pending).unwrap()).unwrap(),
            pending
        );

        let connection = Connection::open_in_memory().unwrap();
        initialize(&connection).unwrap();
        insert(&connection, DeviceSeq(13), &transaction).unwrap();
        assert_eq!(load(&connection).unwrap(), [pending]);
    }

    #[test]
    fn mixed_transaction_repair_runs_reject_effects_in_reverse_operation_order() {
        let connection = Connection::open_in_memory().unwrap();
        initialize(&connection).unwrap();
        catalog::initialize(&connection).unwrap();

        let created = operation("notes");
        let MultiliteOp::CreateTable(table) = &created else {
            unreachable!()
        };
        connection.execute(table.sql(), ()).unwrap();
        catalog::insert(&connection, table).unwrap();
        connection
            .execute("INSERT INTO notes VALUES (7)", ())
            .unwrap();
        let inserted = RowSet::from_captured(
            &connection,
            &[CapturedRow {
                table: "notes".into(),
                rowid: 7,
                values: vec![StoredValue::Integer(7)],
            }],
        )
        .unwrap()
        .unwrap();
        let transaction = MultiliteTransaction::new(vec![
            created,
            MultiliteOp::ChangeRows(RowChanges::inserted(inserted.clone())),
        ])
        .unwrap();
        insert(&connection, DeviceSeq(1), &transaction).unwrap();

        let pending = load(&connection).unwrap();
        assert!(matches!(
            pending[0].rejection().unwrap().as_slice(),
            [
                RejectionEffect::RestoreRowChanges { .. },
                RejectionEffect::DropTable { created }
            ] if created.table_name() == "notes"
        ));
        let active = vec![(
            DeviceSeq(1),
            DeviceOp::Commit {
                entries: Vec::new(),
                range_asserts: Vec::new(),
                evidence: Vec::new(),
                submit_mode: SubmitMode::Unchecked,
            },
        )];
        reject_active(&connection, &active).unwrap();

        assert!(
            !connection
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE name = 'notes')",
                    (),
                    |row| row.get::<_, bool>(0),
                )
                .unwrap()
        );
        assert!(catalog::by_name(&connection, "notes").unwrap().is_none());
        assert!(load(&connection).unwrap().is_empty());
    }

    #[test]
    fn drop_effect_refuses_a_recreated_table_with_the_same_name() {
        let connection = Connection::open_in_memory().unwrap();
        catalog::initialize(&connection).unwrap();
        let MultiliteOp::CreateTable(original) = operation("notes") else {
            unreachable!()
        };
        connection.execute(original.sql(), ()).unwrap();
        catalog::insert(&connection, &original).unwrap();

        connection.execute("DROP TABLE notes", ()).unwrap();
        catalog::remove_by_name(&connection, "notes").unwrap();
        let MultiliteOp::CreateTable(replacement) = operation("notes") else {
            unreachable!()
        };
        connection.execute(replacement.sql(), ()).unwrap();
        catalog::insert(&connection, &replacement).unwrap();

        assert!(matches!(
            apply_rejection(
                &connection,
                &[RejectionEffect::DropTable { created: original }],
            ),
            Err(Error::InvalidDatabase(
                "pending CREATE TABLE no longer matches SQLite state"
            ))
        ));
        assert_eq!(
            catalog::by_name(&connection, "notes").unwrap(),
            Some(replacement)
        );
    }

    #[test]
    fn drop_effect_follows_the_created_identity_after_rename_and_name_reuse() {
        let connection = Connection::open_in_memory().unwrap();
        catalog::initialize(&connection).unwrap();
        let MultiliteOp::CreateTable(original) = operation("notes") else {
            unreachable!()
        };
        connection.execute(original.sql(), ()).unwrap();
        catalog::insert(&connection, &original).unwrap();
        connection
            .execute("ALTER TABLE notes RENAME TO archived_notes", ())
            .unwrap();
        catalog::rename_binding(
            &connection,
            original.table_id(),
            original.table_name_identity(),
            &crate::logical::schema::SqlName::new("archived_notes".into()),
        )
        .unwrap();
        let MultiliteOp::CreateTable(replacement) = operation("notes") else {
            unreachable!()
        };
        connection.execute(replacement.sql(), ()).unwrap();
        catalog::insert(&connection, &replacement).unwrap();

        apply_rejection(
            &connection,
            &[RejectionEffect::DropTable {
                created: original.clone(),
            }],
        )
        .unwrap();

        assert!(
            catalog::by_id(&connection, original.table_id())
                .unwrap()
                .is_none()
        );
        assert_eq!(
            catalog::by_name(&connection, "notes").unwrap(),
            Some(replacement)
        );
        assert!(
            !connection
                .query_row(
                    "SELECT EXISTS(
                        SELECT 1 FROM sqlite_schema WHERE name = 'archived_notes'
                    )",
                    (),
                    |row| row.get::<_, bool>(0),
                )
                .unwrap()
        );
    }

    #[test]
    fn validation_accepts_the_created_table_and_rejects_lookalikes() {
        let connection = Connection::open_in_memory().unwrap();
        initialize(&connection).unwrap();
        assert!(is_initialized(&connection).unwrap());
        validate(&connection).unwrap();

        connection
            .execute_batch("CREATE TABLE __multilite__pending_future (value BLOB NOT NULL)")
            .unwrap();
        assert!(matches!(
            is_initialized(&connection),
            Err(Error::InvalidDatabase(
                "pending table namespace contains unexpected tables"
            ))
        ));
    }

    #[test]
    fn malformed_rows_are_rejected_when_loaded() {
        let connection = Connection::open_in_memory().unwrap();
        initialize(&connection).unwrap();
        connection
            .execute(
                &format!("INSERT INTO {TABLE} (device_seq, record) VALUES (?1, x'02')"),
                [DeviceSeq(1).0.to_be_bytes().as_slice()],
            )
            .unwrap();

        assert!(matches!(
            load(&connection),
            Err(Error::InvalidDatabase("pending record is malformed"))
        ));
    }

    #[test]
    fn pending_frame_contains_only_sequence_and_logical_transaction() {
        let pending = PendingTransaction::new(DeviceSeq(1), transaction(operation("notes")));
        let encoded = PendingCodec::encode(&pending).unwrap();
        let mut reader = Reader::new(&encoded);
        assert_eq!(reader.u8(), Some(PENDING_FRAME_VERSION));
        let mut tags = Vec::new();
        while let Some((tag, _)) = reader.field().unwrap() {
            tags.push(tag);
        }
        assert_eq!(tags, [TAG_DEVICE_SEQ, TAG_TRANSACTION]);
    }

    #[test]
    fn record_sequence_must_match_its_ordering_key() {
        let connection = Connection::open_in_memory().unwrap();
        initialize(&connection).unwrap();
        let record = PendingCodec::encode(&PendingTransaction::new(
            DeviceSeq(2),
            transaction(operation("notes")),
        ))
        .unwrap();
        connection
            .execute(
                &format!("INSERT INTO {TABLE} (device_seq, record) VALUES (?1, ?2)"),
                params![DeviceSeq(1).0.to_be_bytes().as_slice(), record],
            )
            .unwrap();

        assert!(matches!(
            load(&connection),
            Err(Error::InvalidDatabase(
                "pending record sequence does not match its row key"
            ))
        ));
    }

    #[test]
    fn acceptance_retires_only_the_acknowledged_prefix() {
        let connection = Connection::open_in_memory().unwrap();
        initialize(&connection).unwrap();
        let later = transaction(operation("tasks"));
        insert(&connection, DeviceSeq(3), &transaction(operation("notes"))).unwrap();
        insert(&connection, DeviceSeq(9), &later).unwrap();

        accept_through(&connection, DeviceSeq(3)).unwrap();

        assert_eq!(
            load(&connection).unwrap(),
            [PendingTransaction::new(DeviceSeq(9), later)]
        );
    }

    #[test]
    fn validation_rejects_pending_transactions_below_submit_neck() {
        let connection = Connection::open_in_memory().unwrap();
        initialize(&connection).unwrap();
        insert(&connection, DeviceSeq(3), &transaction(operation("notes"))).unwrap();

        validate_active_from(&connection, DeviceSeq(3)).unwrap();
        assert!(matches!(
            validate_active_from(&connection, DeviceSeq(4)),
            Err(Error::InvalidDatabase(
                "accepted pending transaction was not finalized with its submit trim"
            ))
        ));
    }
}
