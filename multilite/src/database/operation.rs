//! Logical Multilite operations and their Homebase representation.

use homebase_core::key::Key;
use homebase_core::messages::{AdmittedBatch, RangeAssert};
use homebase_core::tag::{AdmissionSeq, Mutation};

use super::codes;
use super::row::{InsertRows, RowHomebaseOp};
use super::schema::{CreateTable, CreateTableSpec};
use crate::{Error, Result};

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
    /// Bind asserted scopes to the local admission cut used by submission.
    pub fn at(self, upto: AdmissionSeq) -> (Vec<Mutation>, Vec<RangeAssert>) {
        let assertions = self
            .asserted_scopes
            .into_iter()
            .map(|prefix| RangeAssert { prefix, upto })
            .collect();
        (self.mutations, assertions)
    }
}

impl MultiliteOp {
    /// Mint durable schema identities for one validated table creation.
    pub fn create_table(sql: &str, spec: CreateTableSpec) -> Self {
        Self::CreateTable(CreateTable::new(sql, spec))
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

    /// Raise one complete authenticated Homebase batch into a logical op.
    pub fn from_homebase(batch: &AdmittedBatch<Vec<u8>>) -> Result<Self> {
        let first = batch
            .entries
            .first()
            .ok_or_else(|| Error::InvalidMultiliteOp("operation batch is empty".into()))?;
        let components = first.key().components();
        if components.len() >= 3
            && components[0].as_bytes() == codes::ROOT
            && components[1].as_bytes() == codes::SCHEMA
            && components[2].as_bytes() == codes::LOG
        {
            CreateTable::from_homebase(batch)
                .map(Self::CreateTable)
                .map_err(|error| Error::InvalidMultiliteOp(error.to_string()))
        } else {
            InsertRows::from_homebase(batch)
                .map(Self::InsertRows)
                .map_err(|error| Error::InvalidMultiliteOp(error.to_string()))
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

    #[test]
    fn operation_dispatches_schema_translation_and_binds_the_submission_cut() {
        let operation =
            MultiliteOp::create_table("CREATE TABLE notes (id INTEGER PRIMARY KEY)", table());
        let (mutations, assertions) = operation.to_homebase().unwrap().at(AdmissionSeq(41));

        assert_eq!(mutations.len(), 6);
        assert_eq!(assertions.len(), 2);
        assert!(
            assertions
                .iter()
                .all(|assertion| assertion.upto == AdmissionSeq(41))
        );
        assert_eq!(assertions[0].prefix, *mutations[1].key());
        assert_eq!(assertions[1].prefix, *mutations[5].key());
    }

    #[test]
    fn operation_dispatches_insert_rows_through_a_homebase_roundtrip() {
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

        let lowered = operation.to_homebase().unwrap();
        assert_eq!(lowered.mutations.len(), 1);
        assert_eq!(
            MultiliteOp::from_homebase(&admitted(lowered.mutations)).unwrap(),
            operation
        );
    }
}
