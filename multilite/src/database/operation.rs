//! Logical Multilite operations and their durable representation.

use std::fmt;

use homebase_core::key::Key;
use homebase_core::reader::Reader;
use homebase_core::tag::Mutation;
use homebase_core::writer::Writer;

use super::row::{InsertRows, RowHomebaseOp};
use super::schema::{CreateTable, CreateTableSpec};
use crate::{Error, Result};

const OPERATION_FRAME_VERSION: u8 = 1;
const CREATE_TABLE_OPERATION: u8 = 1;
const INSERT_ROWS_OPERATION: u8 = 2;

/// One logical Multilite operation, independent of its Homebase envelope.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MultiliteOp {
    CreateTable(CreateTable),
    InsertRows(InsertRows),
}

/// Homebase mutations and coordination scopes for one [`MultiliteOp`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HomebaseOp {
    pub mutations: Vec<Mutation>,
    asserted_scopes: Vec<Key>,
}

impl HomebaseOp {
    /// Split deterministic mutations from their unbound assertion scopes.
    pub fn into_parts(self) -> (Vec<Mutation>, Vec<Key>) {
        (self.mutations, self.asserted_scopes)
    }
}

impl MultiliteOp {
    /// Mint durable schema identities for one validated table creation.
    pub fn create_table(sql: &str, spec: CreateTableSpec) -> Self {
        Self::CreateTable(CreateTable::new(sql, spec))
    }

    /// Encode one complete logical operation for transaction and pending frames.
    pub fn encode(&self) -> Vec<u8> {
        let mut writer = Writer::new();
        writer.u8(OPERATION_FRAME_VERSION);
        match self {
            Self::CreateTable(created) => {
                writer.u8(CREATE_TABLE_OPERATION);
                writer.bytes(&created.encode());
            }
            Self::InsertRows(inserted) => {
                writer.u8(INSERT_ROWS_OPERATION);
                writer.bytes(&inserted.encode());
            }
        }
        writer.finish()
    }

    /// Decode and validate one complete logical operation.
    pub fn decode(frame: &[u8]) -> std::result::Result<Self, OperationCodecError> {
        let mut reader = Reader::new(frame);
        let version = reader.u8().ok_or(OperationCodecError::Truncated)?;
        if version != OPERATION_FRAME_VERSION {
            return Err(OperationCodecError::UnknownVersion(version));
        }
        match reader.u8().ok_or(OperationCodecError::Truncated)? {
            CREATE_TABLE_OPERATION => CreateTable::decode(reader.rest())
                .map(Self::CreateTable)
                .map_err(|error| OperationCodecError::InvalidPayload(error.to_string())),
            INSERT_ROWS_OPERATION => InsertRows::decode(reader.rest())
                .map(Self::InsertRows)
                .map_err(|error| OperationCodecError::InvalidPayload(error.to_string())),
            kind => Err(OperationCodecError::UnknownKind(kind)),
        }
    }

    /// Lower this operation to its complete Homebase representation.
    pub fn to_homebase(&self) -> Result<HomebaseOp> {
        let (mutations, asserted_scopes) = match self {
            Self::CreateTable(created) => {
                let schema = created.to_homebase();
                (schema.mutations, schema.asserted_scopes)
            }
            Self::InsertRows(inserted) => {
                let RowHomebaseOp {
                    mutations,
                    asserted_scopes,
                } = inserted
                    .to_homebase()
                    .map_err(|error| Error::InvalidMultiliteOp(error.to_string()))?;
                (mutations, asserted_scopes)
            }
        };
        Ok(HomebaseOp {
            mutations,
            asserted_scopes,
        })
    }
}

/// Failure to decode one logical operation frame.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OperationCodecError {
    UnknownVersion(u8),
    Truncated,
    UnknownKind(u8),
    InvalidPayload(String),
}

impl fmt::Display for OperationCodecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownVersion(version) => {
                write!(f, "unknown Multilite operation version {version}")
            }
            Self::Truncated => f.write_str("Multilite operation frame is truncated"),
            Self::UnknownKind(kind) => write!(f, "unknown Multilite operation kind {kind}"),
            Self::InvalidPayload(error) => write!(f, "invalid operation payload: {error}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use rusqlite::Connection;

    use super::*;
    use crate::database::catalog;
    use crate::database::row::{CapturedRow, StoredValue};
    use crate::database::schema::{CreateColumn, DeclaredType, SqlName};

    fn table() -> CreateTableSpec {
        CreateTableSpec {
            name: SqlName::new("notes".into()),
            columns: vec![CreateColumn {
                name: SqlName::new("id".into()),
                declared_type: DeclaredType::Integer,
                not_null: false,
                primary_key: true,
            }],
        }
    }

    #[test]
    fn operation_dispatches_schema_translation_and_exposes_asserted_scopes() {
        let operation =
            MultiliteOp::create_table("CREATE TABLE notes (id INTEGER PRIMARY KEY)", table());
        let (mutations, asserted_scopes) = operation.to_homebase().unwrap().into_parts();

        assert_eq!(mutations.len(), 6);
        assert_eq!(asserted_scopes.len(), 2);
        assert_eq!(asserted_scopes[0], *mutations[1].key());
        assert_eq!(asserted_scopes[1], *mutations[5].key());
    }

    #[test]
    fn operation_codec_roundtrips_insert_rows() {
        let connection = Connection::open_in_memory().unwrap();
        catalog::initialize(&connection).unwrap();
        let created = CreateTable::new("CREATE TABLE notes (id INTEGER PRIMARY KEY)", table());
        connection.execute(created.sql(), ()).unwrap();
        catalog::insert(&connection, &created).unwrap();
        let inserted = InsertRows::from_captured(
            &connection,
            &[CapturedRow {
                table: "notes".into(),
                values: vec![StoredValue::Integer(7)],
            }],
        )
        .unwrap()
        .unwrap();
        let operation = MultiliteOp::InsertRows(inserted);

        assert_eq!(MultiliteOp::decode(&operation.encode()).unwrap(), operation);
        assert_eq!(
            MultiliteOp::decode(&[]),
            Err(OperationCodecError::Truncated)
        );
        assert_eq!(
            MultiliteOp::decode(&[2, CREATE_TABLE_OPERATION]),
            Err(OperationCodecError::UnknownVersion(2))
        );
    }
}
