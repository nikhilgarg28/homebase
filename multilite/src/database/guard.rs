//! Typed logical targets used by Homebase mutations and conflict guards.

use homebase_core::key::{Key, KeyError};
use sha2::{Digest, Sha256};

use super::codes;
use super::schema::{ColumnId, ForeignKeyId, IndexId, MutationId, SchemaRevisionId, TableId};

const SHORT_NAME_LIMIT: usize = 250;
const OBJECT_NAME_HASH_DOMAIN: &[u8] = b"multilite:table-name:v1\0";

/// Finite durable namespaces understood by the operation compiler.
#[allow(
    dead_code,
    reason = "consumed by the reasoned guard audit in the next compiler slice"
)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum TargetFamily {
    SchemaLog,
    SchemaObjectName,
    TableSchema,
    ColumnName,
    ColumnDependency,
    TableRoot,
    ActivePrimaryIndex,
    ActiveSchemaRevision,
    IndexDefinition,
    WriteRevision,
    Row,
    UniqueOwner,
    ForeignReference,
    TransactionLog,
}

/// One point or component-prefix address in Multilite's durable Homebase model.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LogicalTarget {
    SchemaLog {
        mutation: MutationId,
    },
    SchemaObjectName {
        canonical: Vec<u8>,
    },
    TableSchema {
        table: TableId,
        revision: SchemaRevisionId,
    },
    ColumnName {
        table: TableId,
        canonical: Vec<u8>,
    },
    ColumnDependencyPrefix {
        table: TableId,
        column: ColumnId,
    },
    ColumnIndexDependency {
        table: TableId,
        column: ColumnId,
        index: IndexId,
    },
    ColumnCheckDependency {
        table: TableId,
        column: ColumnId,
        owner: ColumnId,
    },
    TableRoot {
        table: TableId,
    },
    ActivePrimaryIndex {
        table: TableId,
    },
    ActiveSchemaRevision {
        table: TableId,
    },
    IndexDefinition {
        table: TableId,
        index: IndexId,
    },
    WriteRevision {
        table: TableId,
    },
    Row {
        table: TableId,
        index: IndexId,
        images: Vec<Vec<u8>>,
    },
    UniqueOwner {
        table: TableId,
        index: IndexId,
        images: Vec<Vec<u8>>,
    },
    ForeignReferencePrefix {
        parent: TableId,
        relationship: ForeignKeyId,
        parent_index: IndexId,
        parent_images: Vec<Vec<u8>>,
    },
    ForeignReference {
        parent: TableId,
        relationship: ForeignKeyId,
        parent_index: IndexId,
        parent_images: Vec<Vec<u8>>,
        child_index: IndexId,
        child_images: Vec<Vec<u8>>,
    },
    TransactionLog {
        transaction: [u8; 16],
    },
}

impl LogicalTarget {
    /// Durable namespace family used by diagnostics and audit generation.
    #[allow(
        dead_code,
        reason = "consumed by the reasoned guard audit in the next compiler slice"
    )]
    pub fn family(&self) -> TargetFamily {
        match self {
            Self::SchemaLog { .. } => TargetFamily::SchemaLog,
            Self::SchemaObjectName { .. } => TargetFamily::SchemaObjectName,
            Self::TableSchema { .. } => TargetFamily::TableSchema,
            Self::ColumnName { .. } => TargetFamily::ColumnName,
            Self::ColumnDependencyPrefix { .. }
            | Self::ColumnIndexDependency { .. }
            | Self::ColumnCheckDependency { .. } => TargetFamily::ColumnDependency,
            Self::TableRoot { .. } => TargetFamily::TableRoot,
            Self::ActivePrimaryIndex { .. } => TargetFamily::ActivePrimaryIndex,
            Self::ActiveSchemaRevision { .. } => TargetFamily::ActiveSchemaRevision,
            Self::IndexDefinition { .. } => TargetFamily::IndexDefinition,
            Self::WriteRevision { .. } => TargetFamily::WriteRevision,
            Self::Row { .. } => TargetFamily::Row,
            Self::UniqueOwner { .. } => TargetFamily::UniqueOwner,
            Self::ForeignReferencePrefix { .. } | Self::ForeignReference { .. } => {
                TargetFamily::ForeignReference
            }
            Self::TransactionLog { .. } => TargetFamily::TransactionLog,
        }
    }

    /// Render the canonical component layout consumed by Homebase.
    pub fn render(&self) -> Result<Key, KeyError> {
        let components = match self {
            Self::SchemaLog { mutation } => vec![
                codes::ROOT.to_vec(),
                codes::SCHEMA.to_vec(),
                codes::LOG.to_vec(),
                mutation.as_bytes().to_vec(),
            ],
            Self::SchemaObjectName { canonical } => vec![
                codes::ROOT.to_vec(),
                codes::SCHEMA.to_vec(),
                codes::NAMES.to_vec(),
                codes::MAIN.to_vec(),
                name_component(canonical),
            ],
            Self::TableSchema { table, revision } => vec![
                codes::ROOT.to_vec(),
                codes::TABLES.to_vec(),
                table.as_bytes().to_vec(),
                codes::SCHEMA.to_vec(),
                revision.as_bytes().to_vec(),
            ],
            Self::ColumnName { table, canonical } => vec![
                codes::ROOT.to_vec(),
                codes::TABLES.to_vec(),
                table.as_bytes().to_vec(),
                codes::NAMES.to_vec(),
                codes::COLUMNS.to_vec(),
                name_component(canonical),
            ],
            Self::ColumnDependencyPrefix { table, column } => vec![
                codes::ROOT.to_vec(),
                codes::TABLES.to_vec(),
                table.as_bytes().to_vec(),
                codes::COLUMN_DEPENDENCIES.to_vec(),
                column.as_bytes().to_vec(),
            ],
            Self::ColumnIndexDependency {
                table,
                column,
                index,
            } => vec![
                codes::ROOT.to_vec(),
                codes::TABLES.to_vec(),
                table.as_bytes().to_vec(),
                codes::COLUMN_DEPENDENCIES.to_vec(),
                column.as_bytes().to_vec(),
                codes::INDEXES.to_vec(),
                index.as_bytes().to_vec(),
            ],
            Self::ColumnCheckDependency {
                table,
                column,
                owner,
            } => vec![
                codes::ROOT.to_vec(),
                codes::TABLES.to_vec(),
                table.as_bytes().to_vec(),
                codes::COLUMN_DEPENDENCIES.to_vec(),
                column.as_bytes().to_vec(),
                codes::COLUMNS.to_vec(),
                owner.as_bytes().to_vec(),
            ],
            Self::TableRoot { table } => vec![
                codes::ROOT.to_vec(),
                codes::TABLES.to_vec(),
                table.as_bytes().to_vec(),
            ],
            Self::ActivePrimaryIndex { table } => vec![
                codes::ROOT.to_vec(),
                codes::TABLES.to_vec(),
                table.as_bytes().to_vec(),
                codes::ACTIVE_PRIMARY_INDEX.to_vec(),
            ],
            Self::ActiveSchemaRevision { table } => vec![
                codes::ROOT.to_vec(),
                codes::TABLES.to_vec(),
                table.as_bytes().to_vec(),
                codes::ACTIVE_SCHEMA_REVISION.to_vec(),
            ],
            Self::IndexDefinition { table, index } => vec![
                codes::ROOT.to_vec(),
                codes::TABLES.to_vec(),
                table.as_bytes().to_vec(),
                codes::INDEX_DEFINITIONS.to_vec(),
                index.as_bytes().to_vec(),
            ],
            Self::WriteRevision { table } => vec![
                codes::ROOT.to_vec(),
                codes::TABLES.to_vec(),
                table.as_bytes().to_vec(),
                codes::WRITE_REVISION.to_vec(),
            ],
            Self::Row {
                table,
                index,
                images,
            } => value_target(*table, codes::ROWS, *index, images),
            Self::UniqueOwner {
                table,
                index,
                images,
            } => value_target(*table, codes::UNIQUE, *index, images),
            Self::ForeignReferencePrefix {
                parent,
                relationship,
                parent_index,
                parent_images,
            } => foreign_reference_prefix(*parent, *relationship, *parent_index, parent_images),
            Self::ForeignReference {
                parent,
                relationship,
                parent_index,
                parent_images,
                child_index,
                child_images,
            } => foreign_reference_prefix(*parent, *relationship, *parent_index, parent_images)
                .into_iter()
                .chain([child_index.as_bytes().to_vec()])
                .chain(child_images.iter().cloned())
                .collect(),
            Self::TransactionLog { transaction } => vec![
                codes::ROOT.to_vec(),
                codes::TRANSACTIONS.to_vec(),
                codes::LOG.to_vec(),
                transaction.to_vec(),
            ],
        };
        Key::from_bytes(components)
    }
}

fn value_target(table: TableId, label: &[u8], index: IndexId, images: &[Vec<u8>]) -> Vec<Vec<u8>> {
    [
        codes::ROOT.to_vec(),
        codes::TABLES.to_vec(),
        table.as_bytes().to_vec(),
        label.to_vec(),
        index.as_bytes().to_vec(),
    ]
    .into_iter()
    .chain(images.iter().cloned())
    .collect()
}

fn foreign_reference_prefix(
    parent: TableId,
    relationship: ForeignKeyId,
    parent_index: IndexId,
    parent_images: &[Vec<u8>],
) -> Vec<Vec<u8>> {
    [
        codes::ROOT.to_vec(),
        codes::TABLES.to_vec(),
        parent.as_bytes().to_vec(),
        codes::FOREIGN_REFERENCES.to_vec(),
        relationship.as_bytes().to_vec(),
        parent_index.as_bytes().to_vec(),
    ]
    .into_iter()
    .chain(parent_images.iter().cloned())
    .collect()
}

pub(super) fn name_component(canonical: &[u8]) -> Vec<u8> {
    if canonical.len() <= SHORT_NAME_LIMIT {
        let mut component = Vec::with_capacity(5 + canonical.len());
        component.extend_from_slice(b"name-");
        component.extend_from_slice(canonical);
        component
    } else {
        let mut hash = Sha256::new();
        hash.update(OBJECT_NAME_HASH_DOMAIN);
        hash.update(canonical);
        let mut component = Vec::with_capacity(5 + 32);
        component.extend_from_slice(b"hash-");
        component.extend_from_slice(&hash.finalize());
        component
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id<T>(byte: u8, constructor: impl FnOnce([u8; 16]) -> T) -> T {
        constructor([byte; 16])
    }

    #[test]
    fn every_target_reports_its_stable_family_and_renders() {
        let table = id(1, TableId::from_bytes);
        let index = id(2, IndexId::from_bytes);
        let relationship = id(3, ForeignKeyId::from_bytes);
        let targets = [
            LogicalTarget::Row {
                table,
                index,
                images: vec![b"row".to_vec()],
            },
            LogicalTarget::UniqueOwner {
                table,
                index,
                images: vec![b"unique".to_vec()],
            },
            LogicalTarget::ForeignReference {
                parent: table,
                relationship,
                parent_index: index,
                parent_images: vec![b"parent".to_vec()],
                child_index: index,
                child_images: vec![b"child".to_vec()],
            },
        ];

        assert_eq!(targets[0].family(), TargetFamily::Row);
        assert_eq!(targets[1].family(), TargetFamily::UniqueOwner);
        assert_eq!(targets[2].family(), TargetFamily::ForeignReference);
        for target in targets {
            assert!(target.render().is_ok());
        }
    }

    #[test]
    fn names_preserve_readable_short_components_and_hash_long_ones() {
        let short = name_component("A".repeat(250).as_bytes());
        assert!(short.starts_with(b"name-"));
        assert_eq!(short.len(), 255);

        let long = name_component("A".repeat(251).as_bytes());
        assert!(long.starts_with(b"hash-"));
        assert_eq!(long.len(), 37);
    }
}
