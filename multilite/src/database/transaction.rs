//! Durable ordered Multilite transactions and authenticated Homebase batches.

use std::fmt;

use homebase_core::key::Key;
use homebase_core::messages::{AdmittedBatch, RangeAssert};
use homebase_core::reader::Reader;
use homebase_core::tag::{AdmissionSeq, Mutation};
use homebase_core::writer::Writer;
use uuid::{Uuid, Variant, Version};

use super::codes;
use super::operation::MultiliteOp;
use crate::{Error, Result};

const TRANSACTION_FRAME_VERSION: u8 = 1;
const TAG_TRANSACTION_ID: u8 = 1;
const TAG_OPERATION: u8 = 2;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct TransactionId([u8; 16]);

/// One ordered unit of local materialization, Homebase submission, and repair.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MultiliteTransaction {
    id: TransactionId,
    operations: Vec<MultiliteOp>,
}

/// Homebase mutations and coordination scopes for one transaction.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HomebaseTransaction {
    pub mutations: Vec<Mutation>,
    asserted_scopes: Vec<Key>,
}

impl HomebaseTransaction {
    /// Bind every asserted scope to the transaction's authority snapshot.
    pub fn at(self, upto: AdmissionSeq) -> (Vec<Mutation>, Vec<RangeAssert>) {
        let assertions = self
            .asserted_scopes
            .into_iter()
            .map(|prefix| RangeAssert { prefix, upto })
            .collect();
        (self.mutations, assertions)
    }
}

impl MultiliteTransaction {
    /// Wrap the current one-statement write path as a one-operation transaction.
    pub fn one(operation: MultiliteOp) -> Self {
        Self {
            id: TransactionId(Uuid::new_v4().into_bytes()),
            operations: vec![operation],
        }
    }

    /// Mint one transaction containing the supplied ordered operations.
    #[allow(
        dead_code,
        reason = "used by the managed multi-statement transaction batch"
    )]
    pub fn new(operations: Vec<MultiliteOp>) -> Result<Self> {
        if operations.is_empty() {
            return Err(Error::InvalidMultiliteTransaction(
                "transaction contains no operations".into(),
            ));
        }
        Ok(Self {
            id: TransactionId(Uuid::new_v4().into_bytes()),
            operations,
        })
    }

    /// Operations in their SQLite apply order.
    pub fn operations(&self) -> &[MultiliteOp] {
        &self.operations
    }

    /// Encode the immutable transaction manifest.
    pub fn encode(&self) -> Vec<u8> {
        let mut writer = Writer::new();
        writer.u8(TRANSACTION_FRAME_VERSION);
        put_field(&mut writer, TAG_TRANSACTION_ID, &self.id.0);
        for operation in &self.operations {
            put_field(&mut writer, TAG_OPERATION, &operation.encode());
        }
        writer.finish()
    }

    /// Decode one complete immutable transaction manifest.
    pub fn decode(frame: &[u8]) -> std::result::Result<Self, TransactionCodecError> {
        let mut reader = Reader::new(frame);
        let version = reader.u8().ok_or(TransactionCodecError::Truncated)?;
        if version != TRANSACTION_FRAME_VERSION {
            return Err(TransactionCodecError::UnknownVersion(version));
        }

        let mut id = None;
        let mut operations = Vec::new();
        while let Some((tag, value)) = next_field(&mut reader)? {
            match tag {
                TAG_TRANSACTION_ID => {
                    if id.replace(TransactionId(uuid_bytes(value)?)).is_some() {
                        return Err(TransactionCodecError::DuplicateField(TAG_TRANSACTION_ID));
                    }
                }
                TAG_OPERATION => {
                    operations.push(MultiliteOp::decode(value).map_err(|error| {
                        TransactionCodecError::InvalidOperation(error.to_string())
                    })?)
                }
                _ => {}
            }
        }
        if operations.is_empty() {
            return Err(TransactionCodecError::Empty);
        }
        Ok(Self {
            id: id.ok_or(TransactionCodecError::MissingField(TAG_TRANSACTION_ID))?,
            operations,
        })
    }

    /// Lower the manifest followed by every operation's deterministic mutations.
    pub fn to_homebase(&self) -> Result<HomebaseTransaction> {
        let mut mutations = vec![Mutation::Set {
            key: transaction_key(self.id),
            value: self.encode(),
        }];
        let mut asserted_scopes = Vec::new();
        for operation in &self.operations {
            let (operation_mutations, operation_scopes) = operation.to_homebase()?.into_parts();
            mutations.extend(operation_mutations);
            asserted_scopes.extend(operation_scopes);
        }
        Ok(HomebaseTransaction {
            mutations,
            asserted_scopes,
        })
    }

    /// Raise and authenticate one complete admitted transaction batch.
    pub fn from_homebase(batch: &AdmittedBatch<Vec<u8>>) -> Result<Self> {
        Self::from_homebase_inner(batch)
            .map_err(|error| Error::InvalidMultiliteTransaction(error.to_string()))
    }

    fn from_homebase_inner(
        batch: &AdmittedBatch<Vec<u8>>,
    ) -> std::result::Result<Self, TransactionCodecError> {
        batch
            .validate()
            .map_err(|_| TransactionCodecError::InvalidBatch)?;
        let first = batch
            .entries
            .first()
            .ok_or(TransactionCodecError::InvalidBatch)?;
        let Mutation::Set { value, .. } = &first.device_entry.mutation else {
            return Err(TransactionCodecError::InvalidBatch);
        };
        let transaction = Self::decode(value)?;
        let lowered = transaction
            .to_homebase()
            .map_err(|error| TransactionCodecError::InvalidOperation(error.to_string()))?;
        if batch.entries.len() != lowered.mutations.len()
            || batch
                .entries
                .iter()
                .map(|entry| &entry.device_entry.mutation)
                .ne(lowered.mutations.iter())
        {
            return Err(TransactionCodecError::InvalidBatch);
        }
        Ok(transaction)
    }
}

fn transaction_key(id: TransactionId) -> Key {
    Key::from_bytes([
        codes::ROOT,
        codes::TRANSACTIONS,
        codes::LOG,
        id.0.as_slice(),
    ])
    .expect("transaction manifest key is bounded and non-empty")
}

fn put_field(writer: &mut Writer, tag: u8, value: &[u8]) {
    let len = u32::try_from(value.len()).expect("transaction field length fits in u32");
    writer.u8(tag);
    writer.u32(len);
    writer.bytes(value);
}

fn next_field<'a>(
    reader: &mut Reader<'a>,
) -> std::result::Result<Option<(u8, &'a [u8])>, TransactionCodecError> {
    if reader.end().is_some() {
        return Ok(None);
    }
    let tag = reader.u8().ok_or(TransactionCodecError::Truncated)?;
    let len = reader.u32().ok_or(TransactionCodecError::Truncated)?;
    let len = usize::try_from(len).map_err(|_| TransactionCodecError::InvalidLength)?;
    let value = reader.take(len).ok_or(TransactionCodecError::Truncated)?;
    Ok(Some((tag, value)))
}

fn uuid_bytes(value: &[u8]) -> std::result::Result<[u8; 16], TransactionCodecError> {
    let bytes = value
        .try_into()
        .map_err(|_| TransactionCodecError::InvalidLength)?;
    let uuid = Uuid::from_bytes(bytes);
    if uuid.get_version() != Some(Version::Random) || uuid.get_variant() != Variant::RFC4122 {
        return Err(TransactionCodecError::InvalidUuid);
    }
    Ok(bytes)
}

/// Failure to decode or authenticate one transaction envelope.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TransactionCodecError {
    UnknownVersion(u8),
    Truncated,
    DuplicateField(u8),
    MissingField(u8),
    InvalidLength,
    InvalidUuid,
    Empty,
    InvalidOperation(String),
    InvalidBatch,
}

impl fmt::Display for TransactionCodecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownVersion(version) => {
                write!(f, "unknown transaction manifest version {version}")
            }
            Self::Truncated => f.write_str("transaction manifest is truncated"),
            Self::DuplicateField(tag) => write!(f, "duplicate transaction field {tag}"),
            Self::MissingField(tag) => write!(f, "missing transaction field {tag}"),
            Self::InvalidLength => f.write_str("transaction field has an invalid length"),
            Self::InvalidUuid => f.write_str("transaction id is not a UUID v4"),
            Self::Empty => f.write_str("transaction contains no operations"),
            Self::InvalidOperation(error) => write!(f, "invalid transaction operation: {error}"),
            Self::InvalidBatch => f.write_str("admitted transaction does not match its manifest"),
        }
    }
}

#[cfg(test)]
mod tests {
    use homebase_core::seal::Seal;
    use homebase_core::tag::{
        AdmissionTag, CipherEpoch, DeviceChecksum, DeviceEntry, DeviceId, DeviceSeq, DeviceTag, Ver,
    };
    use rusqlite::Connection;

    use super::*;
    use crate::database::catalog;
    use crate::database::row::{CapturedRow, InsertRows, StoredValue};
    use crate::database::schema::{CreateColumn, CreateTableSpec, DeclaredType, SqlName};

    fn create_operation() -> MultiliteOp {
        MultiliteOp::create_table(
            "CREATE TABLE notes (id INTEGER PRIMARY KEY)",
            CreateTableSpec {
                name: SqlName::new("notes".into()),
                columns: vec![CreateColumn {
                    name: SqlName::new("id".into()),
                    declared_type: DeclaredType::Integer,
                    not_null: false,
                    primary_key: true,
                }],
            },
        )
    }

    fn mixed_transaction() -> MultiliteTransaction {
        let created = create_operation();
        let MultiliteOp::CreateTable(table) = &created else {
            unreachable!()
        };
        let connection = Connection::open_in_memory().unwrap();
        catalog::initialize(&connection).unwrap();
        connection.execute(table.sql(), ()).unwrap();
        catalog::insert(&connection, table).unwrap();
        let inserted = InsertRows::from_captured(
            &connection,
            &[CapturedRow {
                table: "notes".into(),
                values: vec![StoredValue::Integer(7)],
            }],
        )
        .unwrap()
        .unwrap();
        let mut transaction =
            MultiliteTransaction::new(vec![created, MultiliteOp::InsertRows(inserted)]).unwrap();
        transaction.id = TransactionId(test_uuid(1));
        transaction
    }

    fn admitted(mutations: Vec<Mutation>) -> AdmittedBatch<Vec<u8>> {
        let device = DeviceId([7; 16]);
        let device_seq = DeviceSeq(3);
        let admission_seq = AdmissionSeq(9);
        let entries = mutations
            .into_iter()
            .enumerate()
            .map(|(index, mutation)| homebase_core::tag::AdmittedEntry {
                device_entry: DeviceEntry {
                    mutation,
                    tag: DeviceTag {
                        device,
                        device_seq,
                        ver: Ver(index as u64 + 1),
                        cipher_epoch: CipherEpoch(0),
                    },
                    seal: Seal::empty_aead_v1(),
                },
                admission: AdmissionTag {
                    admission_seq,
                    op_index: index as u32,
                },
            })
            .collect();
        AdmittedBatch {
            admission_seq,
            device,
            device_seq,
            checksum: DeviceChecksum::EMPTY,
            entries,
        }
    }

    fn test_uuid(byte: u8) -> [u8; 16] {
        let mut id = [byte; 16];
        id[6] = (id[6] & 0x0f) | 0x40;
        id[8] = (id[8] & 0x3f) | 0x80;
        id
    }

    #[test]
    fn manifest_and_homebase_batch_roundtrip_ordered_operations() {
        let transaction = mixed_transaction();
        assert_eq!(
            MultiliteTransaction::decode(&transaction.encode()).unwrap(),
            transaction
        );

        let lowered = transaction.to_homebase().unwrap();
        assert_eq!(lowered.mutations.len(), 8);
        assert_eq!(lowered.asserted_scopes.len(), 5);
        let Mutation::Set { key, value } = &lowered.mutations[0] else {
            panic!("manifest was not a set")
        };
        assert_eq!(key.components()[1].as_bytes(), codes::TRANSACTIONS);
        assert_eq!(value, &transaction.encode());
        assert_eq!(
            MultiliteTransaction::from_homebase(&admitted(lowered.mutations)).unwrap(),
            transaction
        );
    }

    #[test]
    fn admitted_batch_rejects_missing_extra_and_crossed_operation_mutations() {
        let transaction = mixed_transaction();
        let lowered = transaction.to_homebase().unwrap().mutations;

        let mut missing = lowered.clone();
        missing.pop();
        assert!(matches!(
            MultiliteTransaction::from_homebase(&admitted(missing)),
            Err(Error::InvalidMultiliteTransaction(_))
        ));

        let mut extra = lowered.clone();
        extra.push(lowered.last().unwrap().clone());
        assert!(matches!(
            MultiliteTransaction::from_homebase(&admitted(extra)),
            Err(Error::InvalidMultiliteTransaction(_))
        ));

        let mut crossed = lowered;
        crossed.swap(1, 7);
        assert!(matches!(
            MultiliteTransaction::from_homebase(&admitted(crossed)),
            Err(Error::InvalidMultiliteTransaction(_))
        ));
    }

    #[test]
    fn manifest_rejects_empty_invalid_uuid_and_truncation() {
        assert_eq!(
            MultiliteTransaction::decode(&[]),
            Err(TransactionCodecError::Truncated)
        );
        let mut empty = Writer::new();
        empty.u8(TRANSACTION_FRAME_VERSION);
        put_field(&mut empty, TAG_TRANSACTION_ID, &test_uuid(1));
        assert_eq!(
            MultiliteTransaction::decode(&empty.finish()),
            Err(TransactionCodecError::Empty)
        );
        let mut invalid_id = Writer::new();
        invalid_id.u8(TRANSACTION_FRAME_VERSION);
        put_field(&mut invalid_id, TAG_TRANSACTION_ID, &[0; 16]);
        put_field(&mut invalid_id, TAG_OPERATION, &create_operation().encode());
        assert_eq!(
            MultiliteTransaction::decode(&invalid_id.finish()),
            Err(TransactionCodecError::InvalidUuid)
        );
    }
}
