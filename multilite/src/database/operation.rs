//! Logical Multilite operations and their Homebase representation.

#![allow(
    dead_code,
    reason = "operation translation is wired into local capture in the next batch"
)]

use homebase_core::key::Key;
use homebase_core::messages::{AdmittedBatch, RangeAssert};
use homebase_core::tag::{AdmissionSeq, Mutation};

use super::schema::{CreateTable, CreateTableSpec};
use crate::{Error, Result};

/// One logical Multilite operation, independent of its Homebase envelope.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MultiliteOp {
    CreateTable(CreateTable),
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
    pub fn to_homebase(&self) -> HomebaseOp {
        let schema = match self {
            Self::CreateTable(created) => created.to_homebase(),
        };
        HomebaseOp {
            mutations: schema.mutations,
            asserted_scopes: schema.asserted_scopes,
        }
    }

    /// Raise one complete authenticated Homebase batch into a logical op.
    pub fn from_homebase(batch: &AdmittedBatch<Vec<u8>>) -> Result<Self> {
        CreateTable::from_homebase(batch)
            .map(Self::CreateTable)
            .map_err(|error| Error::InvalidMultiliteOp(error.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
    fn operation_dispatches_schema_translation_and_binds_the_submission_cut() {
        let operation =
            MultiliteOp::create_table("CREATE TABLE notes (id INTEGER PRIMARY KEY)", table());
        let (mutations, assertions) = operation.to_homebase().at(AdmissionSeq(41));

        assert_eq!(mutations.len(), 3);
        assert_eq!(assertions.len(), 2);
        assert!(
            assertions
                .iter()
                .all(|assertion| assertion.upto == AdmissionSeq(41))
        );
        assert_eq!(assertions[0].prefix, *mutations[1].key());
        assert_eq!(assertions[1].prefix, *mutations[2].key());
    }
}
