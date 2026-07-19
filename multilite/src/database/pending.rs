//! Local accept/reject effects for speculative Multilite operations.

use std::fmt;

use homebase_core::reader::Reader;
use homebase_core::tag::DeviceSeq;
use rusqlite::{Connection, params};

use super::operation::MultiliteOp;
use super::schema::CreateTable;
use crate::{Error, Result};

const TABLE: &str = "__multilite__pending";

const PENDING_FRAME_VERSION: u8 = 1;
const TAG_DEVICE_SEQ: u8 = 1;
const TAG_OPERATION: u8 = 2;
const TAG_ACCEPT_EFFECT: u8 = 3;
const TAG_REJECT_EFFECT: u8 = 4;

const OPERATION_FRAME_VERSION: u8 = 1;
const CREATE_TABLE_OPERATION: u8 = 1;

const EFFECT_FRAME_VERSION: u8 = 1;
const DROP_TABLE_EFFECT: u8 = 1;

/// A local effect to run when a speculative operation gets its disposition.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Effect {
    DropTable { name: String },
}

/// One speculative Multilite operation keyed by its Homebase sequence.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PendingOp {
    pub seq: DeviceSeq,
    pub operation: MultiliteOp,
    pub on_accept: Vec<Effect>,
    pub on_reject: Vec<Effect>,
}

impl PendingOp {
    fn new(seq: DeviceSeq, operation: MultiliteOp) -> Self {
        let (on_accept, on_reject) = effects_for(&operation);
        Self {
            seq,
            operation,
            on_accept,
            on_reject,
        }
    }
}

/// Versioned encoding for one complete pending disposition record.
struct PendingCodec;

impl PendingCodec {
    fn encode(pending: &PendingOp) -> Vec<u8> {
        let mut frame = vec![PENDING_FRAME_VERSION];
        put_field(&mut frame, TAG_DEVICE_SEQ, &pending.seq.0.to_be_bytes());
        put_field(
            &mut frame,
            TAG_OPERATION,
            &Self::encode_operation(&pending.operation),
        );
        for effect in &pending.on_accept {
            put_field(&mut frame, TAG_ACCEPT_EFFECT, &Self::encode_effect(effect));
        }
        for effect in &pending.on_reject {
            put_field(&mut frame, TAG_REJECT_EFFECT, &Self::encode_effect(effect));
        }
        frame
    }

    fn decode(frame: &[u8]) -> std::result::Result<PendingOp, PendingCodecError> {
        let mut reader = Reader::new(frame);
        let version = reader.u8().ok_or(PendingCodecError::Truncated)?;
        if version != PENDING_FRAME_VERSION {
            return Err(PendingCodecError::UnknownVersion {
                frame: FrameKind::Pending,
                version,
            });
        }

        let mut seq = None;
        let mut operation = None;
        let mut on_accept = Vec::new();
        let mut on_reject = Vec::new();
        while let Some((tag, value)) = next_field(&mut reader)? {
            match tag {
                TAG_DEVICE_SEQ => set_once(&mut seq, decode_seq(value)?)?,
                TAG_OPERATION => set_once(&mut operation, Self::decode_operation(value)?)?,
                TAG_ACCEPT_EFFECT => on_accept.push(Self::decode_effect(value)?),
                TAG_REJECT_EFFECT => on_reject.push(Self::decode_effect(value)?),
                _ => {}
            }
        }

        let pending = PendingOp {
            seq: seq.ok_or(PendingCodecError::MissingField(TAG_DEVICE_SEQ))?,
            operation: operation.ok_or(PendingCodecError::MissingField(TAG_OPERATION))?,
            on_accept,
            on_reject,
        };
        let (expected_accept, expected_reject) = effects_for(&pending.operation);
        if pending.on_accept != expected_accept || pending.on_reject != expected_reject {
            return Err(PendingCodecError::EffectsMismatch);
        }
        Ok(pending)
    }

    fn encode_operation(operation: &MultiliteOp) -> Vec<u8> {
        let mut frame = vec![OPERATION_FRAME_VERSION];
        match operation {
            MultiliteOp::CreateTable(created) => {
                frame.push(CREATE_TABLE_OPERATION);
                frame.extend_from_slice(&created.encode());
            }
        }
        frame
    }

    fn decode_operation(frame: &[u8]) -> std::result::Result<MultiliteOp, PendingCodecError> {
        let mut reader = Reader::new(frame);
        let version = reader.u8().ok_or(PendingCodecError::Truncated)?;
        if version != OPERATION_FRAME_VERSION {
            return Err(PendingCodecError::UnknownVersion {
                frame: FrameKind::Operation,
                version,
            });
        }
        match reader.u8().ok_or(PendingCodecError::Truncated)? {
            CREATE_TABLE_OPERATION => CreateTable::decode(reader.rest())
                .map(MultiliteOp::CreateTable)
                .map_err(|error| PendingCodecError::InvalidOperation(error.to_string())),
            kind => Err(PendingCodecError::UnknownOperation(kind)),
        }
    }

    fn encode_effect(effect: &Effect) -> Vec<u8> {
        let mut frame = vec![EFFECT_FRAME_VERSION];
        match effect {
            Effect::DropTable { name } => {
                frame.push(DROP_TABLE_EFFECT);
                frame.extend_from_slice(name.as_bytes());
            }
        }
        frame
    }

    fn decode_effect(frame: &[u8]) -> std::result::Result<Effect, PendingCodecError> {
        let mut reader = Reader::new(frame);
        let version = reader.u8().ok_or(PendingCodecError::Truncated)?;
        if version != EFFECT_FRAME_VERSION {
            return Err(PendingCodecError::UnknownVersion {
                frame: FrameKind::Effect,
                version,
            });
        }
        match reader.u8().ok_or(PendingCodecError::Truncated)? {
            DROP_TABLE_EFFECT => Ok(Effect::DropTable {
                name: std::str::from_utf8(reader.rest())
                    .map_err(|_| PendingCodecError::InvalidUtf8)?
                    .to_owned(),
            }),
            kind => Err(PendingCodecError::UnknownEffect(kind)),
        }
    }
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

pub fn insert(connection: &Connection, seq: DeviceSeq, operation: &MultiliteOp) -> Result<()> {
    let pending = PendingOp::new(seq, operation.clone());
    connection.execute(
        &format!("INSERT INTO {TABLE} (device_seq, record) VALUES (?1, ?2)"),
        params![
            seq.0.to_be_bytes().as_slice(),
            PendingCodec::encode(&pending),
        ],
    )?;
    Ok(())
}

pub fn load(connection: &Connection) -> Result<Vec<PendingOp>> {
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

/// Run acceptance effects and retire every pending operation through `through`.
///
/// The database metadata adapter calls this inside the same SQLite savepoint
/// that advances Homebase's submit neck.
pub fn accept_through(connection: &Connection, through: DeviceSeq) -> Result<()> {
    let accepted = load(connection)?
        .into_iter()
        .take_while(|pending| pending.seq <= through)
        .collect::<Vec<_>>();
    for pending in &accepted {
        apply_effects(connection, &pending.on_accept)?;
    }
    if !accepted.is_empty() {
        connection.execute(
            &format!("DELETE FROM {TABLE} WHERE device_seq <= ?1"),
            [through.0.to_be_bytes().as_slice()],
        )?;
    }
    Ok(())
}

/// Verify that every pending operation still belongs to the active submit log.
pub fn validate_active_from(connection: &Connection, neck: DeviceSeq) -> Result<()> {
    if load(connection)?
        .into_iter()
        .any(|pending| pending.seq < neck)
    {
        return Err(Error::InvalidDatabase(
            "accepted pending operation was not finalized with its submit trim",
        ));
    }
    Ok(())
}

fn effects_for(operation: &MultiliteOp) -> (Vec<Effect>, Vec<Effect>) {
    match operation {
        MultiliteOp::CreateTable(created) => (
            Vec::new(),
            vec![Effect::DropTable {
                name: created.table_name().to_owned(),
            }],
        ),
    }
}

fn apply_effects(connection: &Connection, effects: &[Effect]) -> Result<()> {
    for effect in effects {
        match effect {
            Effect::DropTable { name } => {
                connection.execute_batch(&format!("DROP TABLE {}", quote_identifier(name)))?
            }
        }
    }
    Ok(())
}

fn quote_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

fn put_field(frame: &mut Vec<u8>, tag: u8, value: &[u8]) {
    frame.push(tag);
    let len = u32::try_from(value.len()).expect("pending field length must fit in u32");
    frame.extend_from_slice(&len.to_be_bytes());
    frame.extend_from_slice(value);
}

fn next_field<'a>(
    reader: &mut Reader<'a>,
) -> std::result::Result<Option<(u8, &'a [u8])>, PendingCodecError> {
    if reader.end().is_some() {
        return Ok(None);
    }
    let tag = reader.u8().ok_or(PendingCodecError::Truncated)?;
    let len = reader.u32().ok_or(PendingCodecError::Truncated)?;
    let len = usize::try_from(len).map_err(|_| PendingCodecError::InvalidLength)?;
    let value = reader.take(len).ok_or(PendingCodecError::Truncated)?;
    Ok(Some((tag, value)))
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FrameKind {
    Pending,
    Operation,
    Effect,
}

impl fmt::Display for FrameKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Pending => f.write_str("pending"),
            Self::Operation => f.write_str("pending operation"),
            Self::Effect => f.write_str("pending effect"),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum PendingCodecError {
    UnknownVersion { frame: FrameKind, version: u8 },
    Truncated,
    DuplicateField,
    MissingField(u8),
    InvalidLength,
    UnknownOperation(u8),
    InvalidOperation(String),
    UnknownEffect(u8),
    InvalidUtf8,
    EffectsMismatch,
}

impl fmt::Display for PendingCodecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownVersion { frame, version } => {
                write!(f, "unknown {frame} frame version {version}")
            }
            Self::Truncated => f.write_str("pending frame is truncated"),
            Self::DuplicateField => f.write_str("pending frame contains a duplicate field"),
            Self::MissingField(tag) => write!(f, "pending frame is missing field {tag}"),
            Self::InvalidLength => f.write_str("pending field has an invalid length"),
            Self::UnknownOperation(kind) => write!(f, "unknown pending operation {kind}"),
            Self::InvalidOperation(error) => write!(f, "invalid pending operation: {error}"),
            Self::UnknownEffect(kind) => write!(f, "unknown pending effect {kind}"),
            Self::InvalidUtf8 => f.write_str("pending effect contains invalid UTF-8"),
            Self::EffectsMismatch => {
                f.write_str("pending effects contradict their logical operation")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::database::schema::{CreateColumn, CreateTableSpec, DeclaredType, SqlName};

    fn operation(name: &str) -> MultiliteOp {
        MultiliteOp::create_table(
            &format!("CREATE TABLE {name} (id INTEGER PRIMARY KEY)"),
            CreateTableSpec {
                name: SqlName::new(name.into()),
                columns: vec![CreateColumn {
                    name: SqlName::new("id".into()),
                    declared_type: DeclaredType::Integer,
                    not_null: false,
                    primary_key: true,
                }],
            },
        )
    }

    #[test]
    fn journal_roundtrips_operations_and_effect_lists_in_sequence_order() {
        let connection = Connection::open_in_memory().unwrap();
        initialize(&connection).unwrap();
        let later = operation("tasks");
        let earlier = operation("notes");
        insert(&connection, DeviceSeq(9), &later).unwrap();
        insert(&connection, DeviceSeq(3), &earlier).unwrap();

        assert_eq!(
            load(&connection).unwrap(),
            vec![
                PendingOp {
                    seq: DeviceSeq(3),
                    operation: earlier,
                    on_accept: Vec::new(),
                    on_reject: vec![Effect::DropTable {
                        name: "notes".into()
                    }],
                },
                PendingOp {
                    seq: DeviceSeq(9),
                    operation: later,
                    on_accept: Vec::new(),
                    on_reject: vec![Effect::DropTable {
                        name: "tasks".into()
                    }],
                },
            ]
        );
    }

    #[test]
    fn codec_roundtrips_and_rejects_unknown_or_truncated_versions() {
        let pending = PendingOp::new(DeviceSeq(7), operation("notes"));
        let encoded = PendingCodec::encode(&pending);
        assert_eq!(PendingCodec::decode(&encoded).unwrap(), pending);
        assert_eq!(PendingCodec::decode(&[]), Err(PendingCodecError::Truncated));
        assert_eq!(
            PendingCodec::decode(&[2]),
            Err(PendingCodecError::UnknownVersion {
                frame: FrameKind::Pending,
                version: 2,
            })
        );
        assert_eq!(
            PendingCodec::decode(&encoded[..encoded.len() - 1]),
            Err(PendingCodecError::Truncated)
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
    fn effects_must_match_their_operation() {
        let pending = PendingOp {
            seq: DeviceSeq(1),
            operation: operation("notes"),
            on_accept: Vec::new(),
            on_reject: vec![Effect::DropTable {
                name: "tasks".into(),
            }],
        };

        assert_eq!(
            PendingCodec::decode(&PendingCodec::encode(&pending)),
            Err(PendingCodecError::EffectsMismatch)
        );
    }

    #[test]
    fn record_sequence_must_match_its_ordering_key() {
        let connection = Connection::open_in_memory().unwrap();
        initialize(&connection).unwrap();
        let record = PendingCodec::encode(&PendingOp::new(DeviceSeq(2), operation("notes")));
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
        let later = operation("tasks");
        insert(&connection, DeviceSeq(3), &operation("notes")).unwrap();
        insert(&connection, DeviceSeq(9), &later).unwrap();

        accept_through(&connection, DeviceSeq(3)).unwrap();

        assert_eq!(
            load(&connection).unwrap(),
            [PendingOp::new(DeviceSeq(9), later)]
        );
    }

    #[test]
    fn validation_rejects_pending_operations_below_submit_neck() {
        let connection = Connection::open_in_memory().unwrap();
        initialize(&connection).unwrap();
        insert(&connection, DeviceSeq(3), &operation("notes")).unwrap();

        validate_active_from(&connection, DeviceSeq(3)).unwrap();
        assert!(matches!(
            validate_active_from(&connection, DeviceSeq(4)),
            Err(Error::InvalidDatabase(
                "accepted pending operation was not finalized with its submit trim"
            ))
        ));
    }
}
