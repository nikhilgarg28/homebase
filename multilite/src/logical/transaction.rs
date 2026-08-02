//! Durable ordered Multilite transactions and authenticated Homebase batches.

use std::fmt;

use homebase_core::key::Key;
use homebase_core::messages::{AdmittedBatch, RangeAssert};
use homebase_core::reader::Reader;
use homebase_core::tag::{AdmissionSeq, Mutation};
use homebase_core::writer::Writer;
use rusqlite::Connection;
use uuid::{Uuid, Variant, Version};

#[cfg(test)]
use super::codes;
use super::guard::{GuardPlan, LogicalTarget, OperationFamily, validate_mutations};
use super::isolation::IsolationLevel;
use super::operation::{CompiledOperation, MultiliteOp, RejectionEffect};
use crate::commit::footprint::ConflictFootprint;
use crate::{Error, Result};

const TRANSACTION_FRAME_VERSION: u8 = 1;
const TAG_TRANSACTION_ID: u8 = 1;
const TAG_OPERATION: u8 = 2;
const MAX_TRANSACTION_OPERATIONS: usize = 100_000;
const MAX_TRANSACTION_FRAME_BYTES: usize = 64 * 1024 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct TransactionId([u8; 16]);

/// One ordered unit of local materialization, Homebase submission, and repair.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MultiliteTransaction {
    id: TransactionId,
    operations: Vec<MultiliteOp>,
}

/// Homebase mutations and conflict footprint for one transaction.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HomebaseTransaction {
    pub mutations: Vec<Mutation>,
    footprint: ConflictFootprint,
    guards: Box<GuardPlan>,
}

/// One validated logical transaction and its single deterministic lowering.
///
/// Proposal validation, authority submission, and local history extraction all
/// consume this artifact so they cannot derive subtly different meanings from
/// the same operation manifest.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CompiledTransaction {
    logical: MultiliteTransaction,
    homebase: HomebaseTransaction,
    rejection: Vec<RejectionEffect>,
}

impl HomebaseTransaction {
    /// Split deterministic mutations from their typed conflict footprint.
    #[allow(dead_code, reason = "used by focused lowering tests and tooling")]
    pub fn into_parts(self) -> (Vec<Mutation>, ConflictFootprint) {
        (self.mutations, self.footprint)
    }

    pub fn mutations(&self) -> &[Mutation] {
        &self.mutations
    }

    pub fn footprint(&self) -> &ConflictFootprint {
        &self.footprint
    }

    pub fn guards(&self) -> &GuardPlan {
        &self.guards
    }

    /// Plan assertions for one isolation level and authority snapshot.
    #[allow(
        dead_code,
        reason = "kept as the direct transaction-to-authority planning boundary"
    )]
    pub fn plan(
        self,
        isolation: IsolationLevel,
        upto: AdmissionSeq,
    ) -> (Vec<Mutation>, Vec<RangeAssert>) {
        let assertions = self.footprint.plan(isolation, upto);
        (self.mutations, assertions)
    }
}

impl CompiledTransaction {
    pub fn logical(&self) -> &MultiliteTransaction {
        &self.logical
    }

    pub fn homebase(&self) -> &HomebaseTransaction {
        &self.homebase
    }

    /// Local inverses in the order required to reject the transaction.
    pub fn rejection(&self) -> &[RejectionEffect] {
        &self.rejection
    }
}

impl MultiliteTransaction {
    /// Mint one transaction containing the supplied ordered operations.
    #[cfg(test)]
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
    #[cfg(test)]
    pub fn operations(&self) -> &[MultiliteOp] {
        &self.operations
    }

    /// Validate and lower this manifest exactly once for a commit proposal.
    pub fn compile(self) -> Result<CompiledTransaction> {
        let Self { id, operations } = self;
        let operations = operations
            .into_iter()
            .map(MultiliteOp::compile)
            .collect::<Result<Vec<_>>>()?;
        Self::compile_operations(id, operations)
    }

    /// Mint and assemble one transaction from operations compiled during capture.
    pub fn from_compiled_operations(
        operations: Vec<CompiledOperation>,
    ) -> Result<CompiledTransaction> {
        if operations.is_empty() {
            return Err(Error::InvalidMultiliteTransaction(
                "transaction contains no operations".into(),
            ));
        }
        Self::compile_operations(TransactionId(Uuid::new_v4().into_bytes()), operations)
    }

    /// Encode the immutable transaction manifest.
    pub fn encode(&self) -> Result<Vec<u8>> {
        self.encode_with_limits(MAX_TRANSACTION_OPERATIONS, MAX_TRANSACTION_FRAME_BYTES)
    }

    fn encode_with_limits(&self, max_operations: usize, max_bytes: usize) -> Result<Vec<u8>> {
        if self.operations.len() > max_operations {
            return Err(Error::CaptureLimitExceeded {
                resource: "transaction operation count",
                limit: max_operations,
            });
        }
        let mut writer = Writer::new();
        writer.u8(TRANSACTION_FRAME_VERSION);
        writer
            .field(TAG_TRANSACTION_ID, &self.id.0)
            .map_err(|_| transaction_field_too_large())?;
        let mut encoded_bytes = 1 + 5 + self.id.0.len();
        for operation in &self.operations {
            let operation = operation.encode();
            encoded_bytes = encoded_bytes
                .checked_add(5)
                .and_then(|bytes| bytes.checked_add(operation.len()))
                .ok_or(Error::CaptureLimitExceeded {
                    resource: "transaction frame bytes",
                    limit: max_bytes,
                })?;
            if encoded_bytes > max_bytes {
                return Err(Error::CaptureLimitExceeded {
                    resource: "transaction frame bytes",
                    limit: max_bytes,
                });
            }
            writer
                .field(TAG_OPERATION, &operation)
                .map_err(|_| transaction_field_too_large())?;
        }
        Ok(writer.finish())
    }

    /// Decode one complete immutable transaction manifest.
    pub fn decode(frame: &[u8]) -> std::result::Result<Self, TransactionCodecError> {
        Self::decode_with_limits(
            frame,
            MAX_TRANSACTION_OPERATIONS,
            MAX_TRANSACTION_FRAME_BYTES,
        )
    }

    fn decode_with_limits(
        frame: &[u8],
        max_operations: usize,
        max_bytes: usize,
    ) -> std::result::Result<Self, TransactionCodecError> {
        if frame.len() > max_bytes {
            return Err(TransactionCodecError::FrameTooLarge);
        }
        let mut reader = Reader::new(frame);
        let version = reader.u8().ok_or(TransactionCodecError::Truncated)?;
        if version != TRANSACTION_FRAME_VERSION {
            return Err(TransactionCodecError::UnknownVersion(version));
        }

        let mut id = None;
        let mut operations = Vec::new();
        while let Some((tag, value)) = reader
            .field()
            .map_err(|_| TransactionCodecError::Truncated)?
        {
            match tag {
                TAG_TRANSACTION_ID => {
                    if id.replace(TransactionId(uuid_bytes(value)?)).is_some() {
                        return Err(TransactionCodecError::DuplicateField(TAG_TRANSACTION_ID));
                    }
                }
                TAG_OPERATION => {
                    if operations.len() == max_operations {
                        return Err(TransactionCodecError::TooManyOperations);
                    }
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
            value: self.encode()?,
        }];
        validate_mutations(OperationFamily::TransactionEnvelope, &mutations)?;
        let mut guards = GuardPlan::merged();
        for operation in &self.operations {
            let (operation_mutations, operation_footprint, operation_guards) =
                operation.to_homebase()?.into_all_parts();
            debug_assert_eq!(operation_footprint, operation_guards.footprint());
            mutations.extend(operation_mutations);
            guards.extend(operation_guards);
        }
        let footprint = guards.footprint();
        Ok(HomebaseTransaction {
            mutations,
            footprint,
            guards: Box::new(guards),
        })
    }

    fn compile_operations(
        id: TransactionId,
        operations: Vec<CompiledOperation>,
    ) -> Result<CompiledTransaction> {
        let mut logical_operations = Vec::with_capacity(operations.len());
        let mut operation_mutations = Vec::new();
        let mut guards = GuardPlan::merged();
        let mut rejection = Vec::with_capacity(operations.len());
        for operation in operations {
            let (logical, homebase, inverse) = operation.into_parts();
            let (mutations, operation_footprint, operation_guards) = homebase.into_all_parts();
            debug_assert_eq!(operation_footprint, operation_guards.footprint());
            logical_operations.push(logical);
            operation_mutations.extend(mutations);
            guards.extend(operation_guards);
            rejection.push(inverse);
        }
        rejection.reverse();

        let logical = MultiliteTransaction {
            id,
            operations: logical_operations,
        };
        let mut mutations = Vec::with_capacity(operation_mutations.len() + 1);
        mutations.push(Mutation::Set {
            key: transaction_key(id),
            value: logical.encode()?,
        });
        validate_mutations(OperationFamily::TransactionEnvelope, &mutations)?;
        mutations.extend(operation_mutations);
        let footprint = guards.footprint();
        Ok(CompiledTransaction {
            logical,
            homebase: HomebaseTransaction {
                mutations,
                footprint,
                guards: Box::new(guards),
            },
            rejection,
        })
    }

    /// Materialize every operation in manifest order.
    pub fn apply(&self, connection: &Connection) -> Result<()> {
        for operation in &self.operations {
            operation.apply(connection)?;
            #[cfg(debug_assertions)]
            operation.verify_materialized(connection)?;
        }
        Ok(())
    }

    /// Materialize a locally-originated transaction and retain inverse state.
    pub(crate) fn apply_speculative(&self, connection: &Connection) -> Result<()> {
        for operation in &self.operations {
            operation.capture_local_repair(connection)?;
            operation.apply(connection)?;
            #[cfg(debug_assertions)]
            operation.verify_materialized(connection)?;
        }
        Ok(())
    }

    /// Local repair jobs that must exist while this transaction is pending.
    #[cfg(test)]
    pub(crate) fn repair_ids(&self) -> impl Iterator<Item = crate::repair::RepairId> + '_ {
        self.operations.iter().filter_map(MultiliteOp::repair_id)
    }

    pub(crate) fn repair_specs(&self) -> impl Iterator<Item = crate::repair::RepairSpec> + '_ {
        self.operations.iter().filter_map(MultiliteOp::repair_spec)
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

fn transaction_field_too_large() -> Error {
    Error::InvalidMultiliteTransaction("transaction field is too large".into())
}

fn transaction_key(id: TransactionId) -> Key {
    LogicalTarget::TransactionLog { transaction: id.0 }
        .render()
        .expect("transaction manifest key is bounded and non-empty")
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
    TooManyOperations,
    FrameTooLarge,
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
            Self::TooManyOperations => f.write_str("transaction contains too many operations"),
            Self::FrameTooLarge => f.write_str("transaction manifest is too large"),
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
    use crate::catalog;
    use crate::logical::row::{CapturedRow, RowChanges, RowSet, StoredValue};
    use crate::logical::schema::{CreateColumn, CreateTableSpec, SqlName, TypeDeclaration};

    fn create_operation() -> MultiliteOp {
        MultiliteOp::create_table(
            "CREATE TABLE notes (id INTEGER PRIMARY KEY)",
            CreateTableSpec {
                name: SqlName::new("notes".into()),
                mode: Default::default(),
                storage: crate::logical::schema::TableStorage::Rowid,
                primary_key_conflict: Default::default(),
                columns: vec![CreateColumn {
                    name: SqlName::new("id".into()),
                    declared_type: TypeDeclaration::integer(),
                    not_null: false,
                    not_null_name: None,
                    not_null_conflict: Default::default(),
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

    fn mixed_transaction() -> MultiliteTransaction {
        let created = create_operation();
        let MultiliteOp::CreateTable(table) = &created else {
            unreachable!()
        };
        let connection = Connection::open_in_memory().unwrap();
        catalog::initialize(&connection).unwrap();
        connection.execute(table.sql(), ()).unwrap();
        catalog::insert(&connection, table).unwrap();
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
        let mut transaction = MultiliteTransaction::new(vec![
            created,
            MultiliteOp::ChangeRows(RowChanges::inserted(inserted)),
        ])
        .unwrap();
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
        let encoded = transaction.encode().unwrap();
        assert_eq!(MultiliteTransaction::decode(&encoded).unwrap(), transaction);

        let lowered = transaction.to_homebase().unwrap();
        let compiled = transaction.clone().compile().unwrap();
        assert_eq!(compiled.logical(), &transaction);
        assert_eq!(compiled.homebase(), &lowered);
        assert!(matches!(
            compiled.rejection(),
            [
                RejectionEffect::RestoreRowChanges { .. },
                RejectionEffect::RemoveCreatedTable { .. }
            ]
        ));
        assert_eq!(lowered.mutations.len(), 10);
        assert_eq!(lowered.footprint.writes().len(), 2);
        assert_eq!(lowered.footprint.constraints().len(), 4);
        assert!(lowered.footprint.reads().is_empty());
        let Mutation::Set { key, value } = &lowered.mutations[0] else {
            panic!("manifest was not a set")
        };
        assert_eq!(key.components()[1].as_bytes(), codes::TRANSACTIONS);
        assert_eq!(value, &encoded);
        assert_eq!(
            MultiliteTransaction::from_homebase(&admitted(lowered.mutations)).unwrap(),
            transaction
        );
    }

    #[test]
    fn manifest_codec_enforces_operation_and_byte_budgets() {
        let transaction = MultiliteTransaction::new(vec![create_operation()]).unwrap();
        assert!(matches!(
            transaction.encode_with_limits(0, usize::MAX),
            Err(Error::CaptureLimitExceeded {
                resource: "transaction operation count",
                limit: 0,
            })
        ));
        assert!(matches!(
            transaction.encode_with_limits(usize::MAX, 1),
            Err(Error::CaptureLimitExceeded {
                resource: "transaction frame bytes",
                limit: 1,
            })
        ));

        let encoded = transaction.encode().unwrap();
        assert_eq!(
            MultiliteTransaction::decode_with_limits(&encoded, 0, encoded.len()),
            Err(TransactionCodecError::TooManyOperations)
        );
        assert_eq!(
            MultiliteTransaction::decode_with_limits(&encoded, usize::MAX, encoded.len() - 1,),
            Err(TransactionCodecError::FrameTooLarge)
        );
    }

    #[test]
    fn current_operations_plan_identically_at_both_isolation_levels() {
        let transaction = mixed_transaction();
        let frontier = AdmissionSeq(23);
        let snapshot = transaction
            .to_homebase()
            .unwrap()
            .plan(IsolationLevel::Snapshot, frontier)
            .1;
        let serializable = transaction
            .to_homebase()
            .unwrap()
            .plan(IsolationLevel::Serializable, frontier)
            .1;

        assert_eq!(snapshot, serializable);
        assert_eq!(snapshot.len(), 4);
        assert!(snapshot.iter().all(|assertion| assertion.upto == frontier));
    }

    #[cfg(debug_assertions)]
    #[test]
    fn canonical_apply_suppresses_unrepresented_trigger_side_effects() {
        let created = create_operation();
        let MultiliteOp::CreateTable(table) = &created else {
            unreachable!()
        };
        let connection = Connection::open_in_memory().unwrap();
        catalog::initialize(&connection).unwrap();
        connection.execute(table.sql(), ()).unwrap();
        catalog::insert(&connection, table).unwrap();
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
        connection
            .execute_batch(
                "ALTER TABLE notes ADD COLUMN marker TEXT;
                 CREATE TRIGGER mutate_canonical_insert
                 AFTER INSERT ON notes
                 BEGIN
                     UPDATE notes SET id = id + 1 WHERE rowid = NEW.rowid;
                 END",
            )
            .unwrap();
        let transaction = MultiliteTransaction::new(vec![MultiliteOp::ChangeRows(
            RowChanges::inserted(inserted),
        )])
        .unwrap();

        transaction.apply(&connection).unwrap();
        assert_eq!(
            connection
                .query_row("SELECT id FROM notes", (), |row| row.get::<_, i64>(0))
                .unwrap(),
            7
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
        empty.field(TAG_TRANSACTION_ID, &test_uuid(1)).unwrap();
        assert_eq!(
            MultiliteTransaction::decode(&empty.finish()),
            Err(TransactionCodecError::Empty)
        );
        let mut invalid_id = Writer::new();
        invalid_id.u8(TRANSACTION_FRAME_VERSION);
        invalid_id.field(TAG_TRANSACTION_ID, &[0; 16]).unwrap();
        invalid_id
            .field(TAG_OPERATION, &create_operation().encode())
            .unwrap();
        assert_eq!(
            MultiliteTransaction::decode(&invalid_id.finish()),
            Err(TransactionCodecError::InvalidUuid)
        );
    }
}
