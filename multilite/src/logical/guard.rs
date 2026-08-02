//! Typed logical targets used by Homebase mutations and conflict guards.

use homebase_core::key::{Key, KeyError};
use homebase_core::range::Range;
use homebase_core::tag::Mutation;
use sha2::{Digest, Sha256};

use super::codes;
use super::schema::{
    ColumnId, ForeignKeyId, IndexId, MutationId, SchemaRevisionId, SqlName, TableId,
};
use crate::commit::footprint::ConflictFootprint;
use crate::{Error, Result as MultiliteResult};

const SHORT_NAME_LIMIT: usize = 250;
const OBJECT_NAME_HASH_DOMAIN: &[u8] = b"multilite:table-name:v1\0";

/// Finite durable namespaces understood by the operation compiler.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum TargetFamily {
    SchemaLog,
    SchemaObjectName,
    TableSchema,
    ColumnName,
    ConstraintName,
    ActiveConstraint,
    ConstraintReference,
    ColumnDependency,
    TableRoot,
    ActivePrimaryIndex,
    ActiveSchemaRevision,
    IndexDefinition,
    WriteRevision,
    TableRows,
    Row,
    UniqueOwner,
    ForeignReference,
    TransactionLog,
    UserVersion,
    ViewDependency,
}

/// Logical operation family whose conflict contract emitted a guard.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum OperationFamily {
    TransactionEnvelope,
    CreateTable,
    DropTable,
    RowChanges,
    CreateIndex,
    DropIndex,
    RenameTable,
    RenameColumn,
    AddColumn,
    DropColumn,
    DropConstraint,
    TransactionRead,
    SetUserVersion,
    CreateView,
    DropView,
}

/// Mutation shape permitted against one logical target family.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum MutationKind {
    Set,
    Delete,
    DeletePrefix,
}

/// One permitted durable mutation shape in the checked compiler contract.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MutationContract {
    operation: OperationFamily,
    kind: MutationKind,
    target: TargetFamily,
}

macro_rules! mutation_contract {
    ($operation:ident, $kind:ident, $target:ident) => {
        MutationContract {
            operation: OperationFamily::$operation,
            kind: MutationKind::$kind,
            target: TargetFamily::$target,
        }
    };
}

/// Central allowlist for every Homebase mutation emitted by the compiler.
pub const MUTATION_CONTRACTS: &[MutationContract] = &[
    mutation_contract!(TransactionEnvelope, Set, TransactionLog),
    mutation_contract!(SetUserVersion, Set, UserVersion),
    mutation_contract!(CreateView, Set, SchemaLog),
    mutation_contract!(CreateView, Set, SchemaObjectName),
    mutation_contract!(CreateView, Set, ViewDependency),
    mutation_contract!(CreateView, Set, ColumnDependency),
    mutation_contract!(DropView, Set, SchemaLog),
    mutation_contract!(DropView, Delete, SchemaObjectName),
    mutation_contract!(DropView, Delete, ViewDependency),
    mutation_contract!(DropView, Delete, ColumnDependency),
    mutation_contract!(CreateTable, Set, SchemaLog),
    mutation_contract!(CreateTable, Set, SchemaObjectName),
    mutation_contract!(CreateTable, Set, TableSchema),
    mutation_contract!(CreateTable, Set, ActivePrimaryIndex),
    mutation_contract!(CreateTable, Set, ActiveSchemaRevision),
    mutation_contract!(CreateTable, Set, IndexDefinition),
    mutation_contract!(CreateTable, Set, ColumnName),
    mutation_contract!(CreateTable, Set, ConstraintName),
    mutation_contract!(CreateTable, Set, ActiveConstraint),
    mutation_contract!(CreateTable, Set, ConstraintReference),
    mutation_contract!(CreateTable, Set, WriteRevision),
    mutation_contract!(DropTable, Set, SchemaLog),
    mutation_contract!(DropTable, Delete, SchemaObjectName),
    mutation_contract!(DropTable, DeletePrefix, TableRoot),
    mutation_contract!(DropTable, DeletePrefix, ForeignReference),
    mutation_contract!(DropTable, Delete, ConstraintReference),
    mutation_contract!(RowChanges, Set, Row),
    mutation_contract!(RowChanges, Set, UniqueOwner),
    mutation_contract!(RowChanges, Set, ForeignReference),
    mutation_contract!(RowChanges, Delete, Row),
    mutation_contract!(RowChanges, Delete, UniqueOwner),
    mutation_contract!(RowChanges, Delete, ForeignReference),
    mutation_contract!(RowChanges, DeletePrefix, ForeignReference),
    mutation_contract!(CreateIndex, Set, SchemaLog),
    mutation_contract!(CreateIndex, Set, SchemaObjectName),
    mutation_contract!(CreateIndex, Set, TableSchema),
    mutation_contract!(CreateIndex, Set, ActiveSchemaRevision),
    mutation_contract!(CreateIndex, Set, ColumnDependency),
    mutation_contract!(CreateIndex, Set, IndexDefinition),
    mutation_contract!(CreateIndex, Set, ActiveConstraint),
    mutation_contract!(CreateIndex, Set, UniqueOwner),
    mutation_contract!(CreateIndex, Set, WriteRevision),
    mutation_contract!(DropIndex, Set, SchemaLog),
    mutation_contract!(DropIndex, Set, TableSchema),
    mutation_contract!(DropIndex, Set, ActiveSchemaRevision),
    mutation_contract!(DropIndex, Delete, SchemaObjectName),
    mutation_contract!(DropIndex, Delete, ColumnDependency),
    mutation_contract!(DropIndex, Delete, ActiveConstraint),
    mutation_contract!(DropIndex, DeletePrefix, ConstraintReference),
    mutation_contract!(RenameTable, Set, SchemaLog),
    mutation_contract!(RenameTable, Set, SchemaObjectName),
    mutation_contract!(RenameTable, Delete, SchemaObjectName),
    mutation_contract!(RenameColumn, Set, SchemaLog),
    mutation_contract!(RenameColumn, Set, ColumnName),
    mutation_contract!(RenameColumn, Delete, ColumnName),
    mutation_contract!(AddColumn, Set, SchemaLog),
    mutation_contract!(AddColumn, Set, ColumnName),
    mutation_contract!(AddColumn, Set, ConstraintName),
    mutation_contract!(AddColumn, Set, ConstraintReference),
    mutation_contract!(AddColumn, Set, TableSchema),
    mutation_contract!(AddColumn, Set, ColumnDependency),
    mutation_contract!(AddColumn, Set, WriteRevision),
    mutation_contract!(DropColumn, Set, SchemaLog),
    mutation_contract!(DropColumn, Set, TableSchema),
    mutation_contract!(DropColumn, Delete, ColumnName),
    mutation_contract!(DropColumn, Delete, ConstraintName),
    mutation_contract!(DropColumn, Delete, ColumnDependency),
    mutation_contract!(DropColumn, DeletePrefix, ColumnDependency),
    mutation_contract!(DropConstraint, Set, SchemaLog),
    mutation_contract!(DropConstraint, Set, TableSchema),
    mutation_contract!(DropConstraint, Delete, ConstraintName),
    mutation_contract!(DropConstraint, Delete, ActiveConstraint),
    mutation_contract!(DropConstraint, Delete, ConstraintReference),
    mutation_contract!(DropConstraint, DeletePrefix, ConstraintReference),
];

/// Local materialized-state repair selected when authority rejects an op.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum RejectionKind {
    RemoveCreatedTable,
    RestoreDroppedTable,
    RestoreRowChanges,
    RevertIndex,
    RevertAlterTable,
    RestoreUserVersion,
    RevertView,
}

/// One operation-to-repair mapping in the checked compiler contract.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RejectionContract {
    operation: OperationFamily,
    rejection: RejectionKind,
}

macro_rules! rejection_contract {
    ($operation:ident, $rejection:ident) => {
        RejectionContract {
            operation: OperationFamily::$operation,
            rejection: RejectionKind::$rejection,
        }
    };
}

/// Central allowlist for speculative rejection repair.
pub const REJECTION_CONTRACTS: &[RejectionContract] = &[
    rejection_contract!(CreateTable, RemoveCreatedTable),
    rejection_contract!(DropTable, RestoreDroppedTable),
    rejection_contract!(RowChanges, RestoreRowChanges),
    rejection_contract!(CreateIndex, RevertIndex),
    rejection_contract!(DropIndex, RevertIndex),
    rejection_contract!(RenameTable, RevertAlterTable),
    rejection_contract!(RenameColumn, RevertAlterTable),
    rejection_contract!(AddColumn, RevertAlterTable),
    rejection_contract!(DropColumn, RevertAlterTable),
    rejection_contract!(DropConstraint, RevertAlterTable),
    rejection_contract!(SetUserVersion, RestoreUserVersion),
    rejection_contract!(CreateView, RevertView),
    rejection_contract!(DropView, RevertView),
];

impl TargetFamily {
    /// Classify a rendered target back into the compiler's finite vocabulary.
    pub fn classify(key: &Key) -> Option<Self> {
        let parts = key.components();
        if parts.first()?.as_bytes() != codes::ROOT {
            return None;
        }
        let component = |index: usize| parts.get(index).map(|part| part.as_bytes());
        match component(1)? {
            value if value == codes::SCHEMA => match (component(2), parts.len()) {
                (Some(value), 4) if value == codes::LOG => Some(Self::SchemaLog),
                (Some(value), 5) if value == codes::NAMES && component(3) == Some(codes::MAIN) => {
                    Some(Self::SchemaObjectName)
                }
                _ => None,
            },
            value if value == codes::TABLES => {
                if parts.len() == 3 {
                    return Some(Self::TableRoot);
                }
                match component(3)? {
                    value if value == codes::SCHEMA && parts.len() == 5 => Some(Self::TableSchema),
                    value
                        if value == codes::NAMES
                            && component(4) == Some(codes::COLUMNS)
                            && parts.len() == 6 =>
                    {
                        Some(Self::ColumnName)
                    }
                    value
                        if value == codes::NAMES
                            && component(4) == Some(codes::CONSTRAINTS)
                            && parts.len() == 6 =>
                    {
                        Some(Self::ConstraintName)
                    }
                    value if value == codes::ACTIVE_CONSTRAINTS && parts.len() == 5 => {
                        Some(Self::ActiveConstraint)
                    }
                    value
                        if value == codes::CONSTRAINT_REFERENCES
                            && matches!(parts.len(), 5 | 6) =>
                    {
                        Some(Self::ConstraintReference)
                    }
                    value if value == codes::COLUMN_DEPENDENCIES && parts.len() >= 5 => {
                        Some(Self::ColumnDependency)
                    }
                    value if value == codes::ACTIVE_PRIMARY_INDEX && parts.len() == 4 => {
                        Some(Self::ActivePrimaryIndex)
                    }
                    value if value == codes::ACTIVE_SCHEMA_REVISION && parts.len() == 4 => {
                        Some(Self::ActiveSchemaRevision)
                    }
                    value if value == codes::INDEX_DEFINITIONS && parts.len() == 5 => {
                        Some(Self::IndexDefinition)
                    }
                    value if value == codes::WRITE_REVISION && parts.len() == 4 => {
                        Some(Self::WriteRevision)
                    }
                    value if value == codes::VIEW_DEPENDENCIES && matches!(parts.len(), 4 | 5) => {
                        Some(Self::ViewDependency)
                    }
                    value if value == codes::ROWS && parts.len() == 4 => Some(Self::TableRows),
                    value if value == codes::ROWS && parts.len() >= 5 => Some(Self::Row),
                    value if value == codes::UNIQUE && parts.len() >= 5 => Some(Self::UniqueOwner),
                    value if value == codes::FOREIGN_REFERENCES && parts.len() >= 6 => {
                        Some(Self::ForeignReference)
                    }
                    _ => None,
                }
            }
            value
                if value == codes::TRANSACTIONS
                    && component(2) == Some(codes::LOG)
                    && parts.len() == 4 =>
            {
                Some(Self::TransactionLog)
            }
            value
                if value == codes::METADATA
                    && component(2) == Some(codes::USER_VERSION)
                    && parts.len() == 3 =>
            {
                Some(Self::UserVersion)
            }
            _ => None,
        }
    }
}

/// Whether a guard is mandatory by invariant, selected for write conflicts, or
/// included only by serializable isolation.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum GuardClass {
    Invariant,
    Write,
    SerializableRead,
}

/// Semantic reason an operation depends on one logical target.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum GuardReason {
    SchemaObjectName,
    ColumnNameBinding,
    ConstraintNameBinding,
    ConstraintState,
    ConstraintReference,
    SchemaRevision,
    WriteContract,
    PrimaryIndex,
    RowIdentity,
    UniqueOwnership,
    ForeignReference,
    ForeignChildren,
    ColumnDependency,
    ExistingRows,
    TableExistence,
    SerializableRead,
    UserVersion,
    ViewDependency,
}

/// One permitted guard shape in the checked compiler contract.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GuardContract {
    operation: OperationFamily,
    class: GuardClass,
    reason: GuardReason,
    target: TargetFamily,
}

macro_rules! contract {
    ($operation:ident, $class:ident, $reason:ident, $target:ident) => {
        GuardContract {
            operation: OperationFamily::$operation,
            class: GuardClass::$class,
            reason: GuardReason::$reason,
            target: TargetFamily::$target,
        }
    };
}

/// Central allowlist used by lowering, tests, and the generated guard audit.
pub const GUARD_CONTRACTS: &[GuardContract] = &[
    contract!(CreateTable, Invariant, SchemaObjectName, SchemaObjectName),
    contract!(CreateTable, Invariant, SchemaRevision, ActiveSchemaRevision),
    contract!(CreateTable, Invariant, ConstraintState, ActiveConstraint),
    contract!(
        CreateTable,
        Invariant,
        ConstraintReference,
        ConstraintReference
    ),
    contract!(CreateTable, Write, ConstraintReference, ConstraintReference),
    contract!(CreateTable, Write, WriteContract, WriteRevision),
    contract!(DropTable, Invariant, TableExistence, TableRoot),
    contract!(DropTable, Write, TableExistence, TableRoot),
    contract!(DropTable, Invariant, SchemaObjectName, SchemaObjectName),
    contract!(DropTable, Invariant, ForeignReference, ForeignReference),
    contract!(DropTable, Write, ForeignReference, ForeignReference),
    contract!(
        DropTable,
        Invariant,
        ConstraintReference,
        ConstraintReference
    ),
    contract!(DropTable, Write, ConstraintReference, ConstraintReference),
    contract!(RowChanges, Invariant, RowIdentity, Row),
    contract!(RowChanges, Write, RowIdentity, Row),
    contract!(RowChanges, Invariant, UniqueOwnership, UniqueOwner),
    contract!(RowChanges, Write, UniqueOwnership, UniqueOwner),
    contract!(RowChanges, Invariant, ForeignReference, ForeignReference),
    contract!(RowChanges, Write, ForeignReference, ForeignReference),
    contract!(RowChanges, Invariant, ForeignChildren, ForeignReference),
    contract!(RowChanges, Write, ForeignChildren, ForeignReference),
    contract!(RowChanges, Invariant, PrimaryIndex, ActivePrimaryIndex),
    contract!(RowChanges, Invariant, WriteContract, WriteRevision),
    contract!(CreateIndex, Invariant, SchemaObjectName, SchemaObjectName),
    contract!(CreateIndex, Invariant, SchemaRevision, ActiveSchemaRevision),
    contract!(CreateIndex, Write, SchemaRevision, ActiveSchemaRevision),
    contract!(CreateIndex, Invariant, ColumnNameBinding, ColumnName),
    contract!(CreateIndex, Invariant, ColumnDependency, ColumnDependency),
    contract!(CreateIndex, Write, ColumnDependency, ColumnDependency),
    contract!(CreateIndex, Write, WriteContract, WriteRevision),
    contract!(CreateIndex, Invariant, ExistingRows, Row),
    contract!(CreateIndex, Invariant, ConstraintState, ActiveConstraint),
    contract!(CreateIndex, Write, ConstraintState, ActiveConstraint),
    contract!(DropIndex, Invariant, SchemaObjectName, SchemaObjectName),
    contract!(DropIndex, Invariant, SchemaRevision, ActiveSchemaRevision),
    contract!(DropIndex, Write, SchemaRevision, ActiveSchemaRevision),
    contract!(DropIndex, Invariant, ColumnDependency, ColumnDependency),
    contract!(DropIndex, Write, ColumnDependency, ColumnDependency),
    contract!(DropIndex, Invariant, WriteContract, WriteRevision),
    contract!(DropIndex, Invariant, ConstraintState, ActiveConstraint),
    contract!(DropIndex, Write, ConstraintState, ActiveConstraint),
    contract!(
        DropIndex,
        Invariant,
        ConstraintReference,
        ConstraintReference
    ),
    contract!(DropIndex, Write, ConstraintReference, ConstraintReference),
    contract!(RenameTable, Invariant, SchemaObjectName, SchemaObjectName),
    contract!(RenameColumn, Invariant, ColumnNameBinding, ColumnName),
    contract!(AddColumn, Invariant, ColumnNameBinding, ColumnName),
    contract!(AddColumn, Write, ColumnNameBinding, ColumnName),
    contract!(AddColumn, Invariant, ConstraintNameBinding, ConstraintName),
    contract!(AddColumn, Invariant, SchemaObjectName, SchemaObjectName),
    contract!(AddColumn, Invariant, SchemaRevision, ActiveSchemaRevision),
    contract!(AddColumn, Invariant, ConstraintState, ActiveConstraint),
    contract!(
        AddColumn,
        Invariant,
        ConstraintReference,
        ConstraintReference
    ),
    contract!(AddColumn, Write, ConstraintReference, ConstraintReference),
    contract!(AddColumn, Invariant, ExistingRows, TableRows),
    contract!(AddColumn, Invariant, ColumnDependency, ColumnDependency),
    contract!(AddColumn, Write, ColumnDependency, ColumnDependency),
    contract!(AddColumn, Write, WriteContract, WriteRevision),
    contract!(AddColumn, Invariant, ExistingRows, Row),
    contract!(DropColumn, Invariant, ColumnNameBinding, ColumnName),
    contract!(DropColumn, Invariant, ConstraintNameBinding, ConstraintName),
    contract!(DropColumn, Write, ConstraintNameBinding, ConstraintName),
    contract!(DropColumn, Invariant, ColumnDependency, ColumnDependency),
    contract!(DropColumn, Write, ColumnDependency, ColumnDependency),
    contract!(
        DropConstraint,
        Invariant,
        ConstraintNameBinding,
        ConstraintName
    ),
    contract!(DropConstraint, Write, ConstraintNameBinding, ConstraintName),
    contract!(DropConstraint, Invariant, ConstraintState, ActiveConstraint),
    contract!(DropConstraint, Write, ConstraintState, ActiveConstraint),
    contract!(
        DropConstraint,
        Invariant,
        ConstraintReference,
        ConstraintReference
    ),
    contract!(
        DropConstraint,
        Write,
        ConstraintReference,
        ConstraintReference
    ),
    contract!(
        TransactionRead,
        SerializableRead,
        SerializableRead,
        TableRoot
    ),
    contract!(
        TransactionRead,
        SerializableRead,
        SerializableRead,
        SchemaObjectName
    ),
    contract!(SetUserVersion, Write, UserVersion, UserVersion),
    contract!(CreateView, Invariant, SchemaObjectName, SchemaObjectName),
    contract!(CreateView, Invariant, ViewDependency, SchemaObjectName),
    contract!(CreateView, Invariant, ViewDependency, ColumnName),
    contract!(CreateView, Invariant, ViewDependency, ViewDependency),
    contract!(CreateView, Invariant, ColumnDependency, ColumnDependency),
    contract!(CreateView, Write, ColumnDependency, ColumnDependency),
    contract!(DropView, Invariant, SchemaObjectName, SchemaObjectName),
    contract!(DropView, Invariant, ViewDependency, SchemaObjectName),
    contract!(DropView, Invariant, ViewDependency, ColumnName),
    contract!(DropView, Invariant, ViewDependency, ViewDependency),
    contract!(DropView, Invariant, ColumnDependency, ColumnDependency),
    contract!(DropView, Write, ColumnDependency, ColumnDependency),
    contract!(DropColumn, Invariant, ViewDependency, ViewDependency),
    contract!(
        TransactionRead,
        SerializableRead,
        SerializableRead,
        UserVersion
    ),
];

/// Guards that every operation in a family must emit at least once.
///
/// Exact guards coupled to individual mutations are checked separately by
/// [`validate_compiled_output`]. These requirements cover dependencies which
/// have no one-to-one mutation, such as a row operation's active key contract.
pub const REQUIRED_GUARD_CONTRACTS: &[GuardContract] = &[
    contract!(CreateTable, Invariant, SchemaObjectName, SchemaObjectName),
    contract!(CreateTable, Write, WriteContract, WriteRevision),
    contract!(DropTable, Invariant, TableExistence, TableRoot),
    contract!(DropTable, Write, TableExistence, TableRoot),
    contract!(DropTable, Invariant, SchemaObjectName, SchemaObjectName),
    contract!(RowChanges, Invariant, PrimaryIndex, ActivePrimaryIndex),
    contract!(RowChanges, Invariant, WriteContract, WriteRevision),
    contract!(CreateIndex, Invariant, SchemaObjectName, SchemaObjectName),
    contract!(CreateIndex, Invariant, SchemaRevision, ActiveSchemaRevision),
    contract!(CreateIndex, Write, SchemaRevision, ActiveSchemaRevision),
    contract!(DropIndex, Invariant, SchemaObjectName, SchemaObjectName),
    contract!(DropIndex, Invariant, SchemaRevision, ActiveSchemaRevision),
    contract!(DropIndex, Write, SchemaRevision, ActiveSchemaRevision),
    contract!(RenameTable, Invariant, SchemaObjectName, SchemaObjectName),
    contract!(RenameColumn, Invariant, ColumnNameBinding, ColumnName),
    contract!(AddColumn, Invariant, ColumnNameBinding, ColumnName),
    contract!(DropColumn, Invariant, ColumnNameBinding, ColumnName),
    contract!(DropColumn, Invariant, ViewDependency, ViewDependency),
    contract!(
        DropConstraint,
        Invariant,
        ConstraintNameBinding,
        ConstraintName
    ),
    contract!(SetUserVersion, Write, UserVersion, UserVersion),
    contract!(CreateView, Invariant, SchemaObjectName, SchemaObjectName),
    contract!(DropView, Invariant, SchemaObjectName, SchemaObjectName),
];

/// One unpruned, reviewable conflict dependency emitted by the compiler.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Guard {
    operation: OperationFamily,
    target: Key,
    family: TargetFamily,
    class: GuardClass,
    reason: GuardReason,
}

impl Guard {
    pub fn operation(&self) -> OperationFamily {
        self.operation
    }

    pub fn target(&self) -> &Key {
        &self.target
    }

    pub fn family(&self) -> TargetFamily {
        self.family
    }

    pub fn class(&self) -> GuardClass {
        self.class
    }

    pub fn reason(&self) -> GuardReason {
        self.reason
    }
}

/// Unpruned compiler evidence from which the executable footprint is derived.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct GuardPlan {
    operation: Option<OperationFamily>,
    entries: Vec<Guard>,
}

impl GuardPlan {
    pub fn for_operation(operation: OperationFamily) -> Self {
        Self {
            operation: Some(operation),
            entries: Vec::new(),
        }
    }

    pub fn merged() -> Self {
        Self::default()
    }

    pub fn operation(&self) -> Option<OperationFamily> {
        self.operation
    }

    pub fn invariant(&mut self, target: Key, reason: GuardReason) -> MultiliteResult<()> {
        self.insert(target, GuardClass::Invariant, reason)
    }

    pub fn write(&mut self, target: Key, reason: GuardReason) -> MultiliteResult<()> {
        self.insert(target, GuardClass::Write, reason)
    }

    pub fn serializable_read(&mut self, target: Key, reason: GuardReason) -> MultiliteResult<()> {
        self.insert(target, GuardClass::SerializableRead, reason)
    }

    pub fn extend(&mut self, other: Self) {
        self.entries.extend(other.entries);
    }

    #[allow(
        dead_code,
        reason = "exposed to compiler audit tests and diagnostic tooling"
    )]
    pub fn entries(&self) -> &[Guard] {
        &self.entries
    }

    pub fn footprint(&self) -> ConflictFootprint {
        let mut footprint = ConflictFootprint::new();
        for guard in &self.entries {
            debug_assert!(GUARD_CONTRACTS.iter().any(|contract| {
                contract.operation == guard.operation()
                    && contract.class == guard.class()
                    && contract.reason == guard.reason()
                    && contract.target == guard.family()
            }));
            match guard.class() {
                GuardClass::Invariant => footprint.add_constraint(guard.target().clone()),
                GuardClass::Write => footprint.add_write(guard.target().clone()),
                GuardClass::SerializableRead => footprint.add_read(guard.target().clone()),
            }
        }
        footprint
    }

    fn insert(
        &mut self,
        target: Key,
        class: GuardClass,
        reason: GuardReason,
    ) -> MultiliteResult<()> {
        let family = TargetFamily::classify(&target).ok_or(Error::CaptureInvariant(
            "guard target is outside the logical target registry",
        ))?;
        let operation = self.operation.ok_or(Error::CaptureInvariant(
            "guards can only be added to an operation-scoped plan",
        ))?;
        if !GUARD_CONTRACTS.iter().any(|contract| {
            contract.operation == operation
                && contract.class == class
                && contract.reason == reason
                && contract.target == family
        }) {
            return Err(Error::CaptureInvariant(
                "guard is absent from the operation contract",
            ));
        }
        self.entries.push(Guard {
            operation,
            target,
            family,
            class,
            reason,
        });
        Ok(())
    }
}

/// Reject compiler output that writes outside its declared operation contract.
pub fn validate_mutations(
    operation: OperationFamily,
    mutations: &[Mutation],
) -> MultiliteResult<()> {
    for mutation in mutations {
        let (kind, key) = match mutation {
            Mutation::Set { key, .. } => (MutationKind::Set, key),
            Mutation::Delete { key } => (MutationKind::Delete, key),
            Mutation::DeleteRange {
                range: Range::Prefix(prefix),
            } => (MutationKind::DeletePrefix, prefix),
            Mutation::DeleteRange { range: Range::Full } => {
                return Err(Error::CaptureInvariant(
                    "Multilite operations cannot delete the full Homebase keyspace",
                ));
            }
        };
        let family = TargetFamily::classify(key).ok_or(Error::CaptureInvariant(
            "mutation target is outside the logical target registry",
        ))?;
        if !MUTATION_CONTRACTS.iter().any(|contract| {
            contract.operation == operation && contract.kind == kind && contract.target == family
        }) {
            return Err(Error::CaptureInvariant(
                "mutation is absent from the operation contract",
            ));
        }
    }
    Ok(())
}

/// Reject compiler output that omits a mandatory operation or mutation guard.
pub fn validate_compiled_output(
    operation: OperationFamily,
    mutations: &[Mutation],
    guards: &GuardPlan,
) -> MultiliteResult<()> {
    validate_mutations(operation, mutations)?;
    if guards.operation != Some(operation) {
        return Err(Error::CaptureInvariant(
            "compiled guard plan has the wrong operation family",
        ));
    }

    for required in REQUIRED_GUARD_CONTRACTS
        .iter()
        .filter(|required| required.operation == operation)
    {
        if !guards.entries.iter().any(|guard| {
            guard.class == required.class
                && guard.reason == required.reason
                && guard.family == required.target
        }) {
            return Err(Error::CaptureInvariant(
                "compiler omitted a required operation guard",
            ));
        }
    }

    for mutation in mutations {
        let (kind, key) = mutation_kind_and_key(mutation)?;
        let family = TargetFamily::classify(key).ok_or(Error::CaptureInvariant(
            "mutation target is outside the logical target registry",
        ))?;
        for (class, reason) in mutation_guard_requirements(operation, kind, family) {
            if !guards.entries.iter().any(|guard| {
                guard.target == *key && guard.class == *class && guard.reason == *reason
            }) {
                return Err(Error::CaptureInvariant(
                    "compiler omitted a guard required by a mutation",
                ));
            }
        }
    }
    Ok(())
}

fn mutation_kind_and_key(mutation: &Mutation) -> MultiliteResult<(MutationKind, &Key)> {
    match mutation {
        Mutation::Set { key, .. } => Ok((MutationKind::Set, key)),
        Mutation::Delete { key } => Ok((MutationKind::Delete, key)),
        Mutation::DeleteRange {
            range: Range::Prefix(prefix),
        } => Ok((MutationKind::DeletePrefix, prefix)),
        Mutation::DeleteRange { range: Range::Full } => Err(Error::CaptureInvariant(
            "Multilite operations cannot delete the full Homebase keyspace",
        )),
    }
}

fn mutation_guard_requirements(
    operation: OperationFamily,
    kind: MutationKind,
    family: TargetFamily,
) -> &'static [(GuardClass, GuardReason)] {
    use GuardClass::{Invariant, Write};
    use GuardReason::{
        ColumnDependency, ColumnNameBinding, ConstraintNameBinding, ConstraintReference,
        ConstraintState, ForeignChildren, ForeignReference, RowIdentity, SchemaObjectName,
        SchemaRevision, TableExistence, UniqueOwnership, UserVersion as UserVersionReason,
        ViewDependency as ViewDependencyReason, WriteContract,
    };
    use MutationKind::{Delete, DeletePrefix, Set};
    use OperationFamily::{
        AddColumn, CreateIndex, CreateTable, CreateView, DropColumn, DropConstraint, DropIndex,
        DropTable, DropView, RenameColumn, RenameTable, RowChanges, SetUserVersion,
    };
    use TargetFamily::{
        ActiveConstraint, ActiveSchemaRevision, ColumnDependency as ColumnDependencyTarget,
        ColumnName, ConstraintName, ConstraintReference as ConstraintReferenceTarget,
        ForeignReference as ForeignReferenceTarget, Row,
        SchemaObjectName as SchemaObjectNameTarget, TableRoot, UniqueOwner,
        UserVersion as UserVersionTarget, ViewDependency as ViewDependencyTarget, WriteRevision,
    };

    match (operation, kind, family) {
        (CreateTable, Set, SchemaObjectNameTarget)
        | (DropTable, Delete, SchemaObjectNameTarget)
        | (RenameTable, Set | Delete, SchemaObjectNameTarget)
        | (CreateIndex, Set, SchemaObjectNameTarget)
        | (DropIndex, Delete, SchemaObjectNameTarget)
        | (CreateView, Set, SchemaObjectNameTarget)
        | (DropView, Delete, SchemaObjectNameTarget) => &[(Invariant, SchemaObjectName)],
        (CreateView, Set, ViewDependencyTarget) | (DropView, Delete, ViewDependencyTarget) => {
            &[(Invariant, ViewDependencyReason)]
        }
        (CreateView, Set, ColumnDependencyTarget) | (DropView, Delete, ColumnDependencyTarget) => {
            &[(Invariant, ColumnDependency), (Write, ColumnDependency)]
        }
        (RenameColumn, Set | Delete, ColumnName) | (DropColumn, Delete, ColumnName) => {
            &[(Invariant, ColumnNameBinding)]
        }
        (AddColumn, Set, ColumnName) => &[(Invariant, ColumnNameBinding), (Write, ColumnNameBinding)],
        (AddColumn, Set, ConstraintName) => &[(Invariant, ConstraintNameBinding)],
        (DropColumn, Delete, ConstraintName) => &[
            (Invariant, ConstraintNameBinding),
            (Write, ConstraintNameBinding),
        ],
        (DropConstraint, Delete, ConstraintName) => &[
            (Invariant, ConstraintNameBinding),
            (Write, ConstraintNameBinding),
        ],
        (DropConstraint, Delete, ActiveConstraint) => {
            &[(Invariant, ConstraintState), (Write, ConstraintState)]
        }
        (CreateIndex, Set, ActiveConstraint) | (DropIndex, Delete, ActiveConstraint) => {
            &[(Invariant, ConstraintState), (Write, ConstraintState)]
        }
        (CreateTable | AddColumn, Set, ConstraintReferenceTarget)
        | (DropTable | DropConstraint, Delete, ConstraintReferenceTarget)
        | (DropIndex | DropConstraint, DeletePrefix, ConstraintReferenceTarget) => &[
            (Invariant, ConstraintReference),
            (Write, ConstraintReference),
        ],
        (CreateIndex | DropIndex, Set, ActiveSchemaRevision) => {
            &[(Invariant, SchemaRevision), (Write, SchemaRevision)]
        }
        (CreateTable | CreateIndex | AddColumn, Set, WriteRevision) => &[(Write, WriteContract)],
        (CreateIndex | AddColumn, Set, ColumnDependencyTarget)
        | (DropIndex | DropColumn, Delete | DeletePrefix, ColumnDependencyTarget) => {
            &[(Invariant, ColumnDependency), (Write, ColumnDependency)]
        }
        (RowChanges, Set, Row) => &[(Invariant, RowIdentity), (Write, RowIdentity)],
        (RowChanges, Delete, Row) => &[(Write, RowIdentity)],
        (RowChanges, Set, UniqueOwner) => &[(Invariant, UniqueOwnership), (Write, UniqueOwnership)],
        (RowChanges, Delete, UniqueOwner) => &[(Write, UniqueOwnership)],
        (RowChanges, Set, ForeignReferenceTarget) => {
            &[(Invariant, ForeignReference), (Write, ForeignReference)]
        }
        (RowChanges, Delete, ForeignReferenceTarget) => &[(Write, ForeignReference)],
        (RowChanges, DeletePrefix, ForeignReferenceTarget) => {
            &[(Invariant, ForeignChildren), (Write, ForeignChildren)]
        }
        (DropTable, DeletePrefix, TableRoot) => {
            &[(Invariant, TableExistence), (Write, TableExistence)]
        }
        (DropTable, DeletePrefix, ForeignReferenceTarget) => {
            &[(Invariant, ForeignReference), (Write, ForeignReference)]
        }
        (SetUserVersion, Set, UserVersionTarget) => &[(Write, UserVersionReason)],
        _ => &[],
    }
}

/// Reject an inverse that does not match its operation's declared repair.
pub fn validate_rejection(
    operation: OperationFamily,
    rejection: RejectionKind,
) -> MultiliteResult<()> {
    if REJECTION_CONTRACTS
        .iter()
        .any(|contract| contract.operation == operation && contract.rejection == rejection)
    {
        Ok(())
    } else {
        Err(Error::CaptureInvariant(
            "rejection repair is absent from the operation contract",
        ))
    }
}

#[cfg(test)]
fn audit_markdown() -> String {
    use std::fmt::Write as _;

    let mut audit = String::from(
        "# Multilite Operation Contracts\n\n\
         This file is generated from the checked contracts in `src/logical/guard.rs`. \
         The compiler rejects mutations, guards, and rejection repairs absent from these tables.\n\n\
         Guard classes have distinct semantics: `Invariant` is mandatory at every isolation level, \
         `Write` participates in write/write validation, and `SerializableRead` is added only for \
         serializable transactions. Repeated runtime guards are retained for auditability before \
         the executable footprint is prefix-pruned.\n\n\
         ## Mutations\n\n\
         | Operation | Mutation | Target family |\n\
         | --- | --- | --- |\n",
    );
    for contract in MUTATION_CONTRACTS {
        writeln!(
            audit,
            "| `{:?}` | `{:?}` | `{:?}` |",
            contract.operation, contract.kind, contract.target
        )
        .unwrap();
    }
    audit.push_str(
        "\n## Guards\n\n\
         | Operation | Class | Reason | Target family |\n\
         | --- | --- | --- | --- |\n",
    );
    for contract in GUARD_CONTRACTS {
        writeln!(
            audit,
            "| `{:?}` | `{:?}` | `{:?}` | `{:?}` |",
            contract.operation, contract.class, contract.reason, contract.target
        )
        .unwrap();
    }
    audit.push_str(
        "\n## Rejection Repair\n\n\
         | Operation | Local inverse |\n\
         | --- | --- |\n",
    );
    for contract in REJECTION_CONTRACTS {
        writeln!(
            audit,
            "| `{:?}` | `{:?}` |",
            contract.operation, contract.rejection
        )
        .unwrap();
    }
    audit.push_str(
        "\n## Required Guards\n\n\
         These family-level guards must occur at least once. In addition, the compiler requires \
         exact guards for every mutation whose safety depends on its rendered target.\n\n\
         | Operation | Class | Reason | Target family |\n\
         | --- | --- | --- | --- |\n",
    );
    for contract in REQUIRED_GUARD_CONTRACTS {
        writeln!(
            audit,
            "| `{:?}` | `{:?}` | `{:?}` | `{:?}` |",
            contract.operation, contract.class, contract.reason, contract.target
        )
        .unwrap();
    }
    audit
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
    ConstraintName {
        table: TableId,
        canonical: Vec<u8>,
    },
    ActiveConstraint {
        table: TableId,
        identity: [u8; 16],
    },
    ConstraintReferencePrefix {
        table: TableId,
        identity: [u8; 16],
    },
    ConstraintReference {
        table: TableId,
        identity: [u8; 16],
        relationship: ForeignKeyId,
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
    ColumnViewDependency {
        table: TableId,
        column: ColumnId,
        canonical: Vec<u8>,
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
    RowPrefix {
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
    UserVersion,
    ViewDependencyPrefix {
        table: TableId,
    },
    ViewDependency {
        table: TableId,
        canonical: Vec<u8>,
    },
}

impl LogicalTarget {
    /// Durable namespace family used by diagnostics and audit generation.
    pub fn family(&self) -> TargetFamily {
        match self {
            Self::SchemaLog { .. } => TargetFamily::SchemaLog,
            Self::SchemaObjectName { .. } => TargetFamily::SchemaObjectName,
            Self::TableSchema { .. } => TargetFamily::TableSchema,
            Self::ColumnName { .. } => TargetFamily::ColumnName,
            Self::ConstraintName { .. } => TargetFamily::ConstraintName,
            Self::ActiveConstraint { .. } => TargetFamily::ActiveConstraint,
            Self::ConstraintReferencePrefix { .. } | Self::ConstraintReference { .. } => {
                TargetFamily::ConstraintReference
            }
            Self::ColumnDependencyPrefix { .. }
            | Self::ColumnIndexDependency { .. }
            | Self::ColumnCheckDependency { .. }
            | Self::ColumnViewDependency { .. } => TargetFamily::ColumnDependency,
            Self::TableRoot { .. } => TargetFamily::TableRoot,
            Self::ActivePrimaryIndex { .. } => TargetFamily::ActivePrimaryIndex,
            Self::ActiveSchemaRevision { .. } => TargetFamily::ActiveSchemaRevision,
            Self::IndexDefinition { .. } => TargetFamily::IndexDefinition,
            Self::WriteRevision { .. } => TargetFamily::WriteRevision,
            Self::RowPrefix { .. } => TargetFamily::TableRows,
            Self::Row { .. } => TargetFamily::Row,
            Self::UniqueOwner { .. } => TargetFamily::UniqueOwner,
            Self::ForeignReferencePrefix { .. } | Self::ForeignReference { .. } => {
                TargetFamily::ForeignReference
            }
            Self::TransactionLog { .. } => TargetFamily::TransactionLog,
            Self::UserVersion => TargetFamily::UserVersion,
            Self::ViewDependencyPrefix { .. } | Self::ViewDependency { .. } => {
                TargetFamily::ViewDependency
            }
        }
    }

    /// Render the canonical component layout consumed by Homebase.
    pub fn render(&self) -> std::result::Result<Key, KeyError> {
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
            Self::ConstraintName { table, canonical } => vec![
                codes::ROOT.to_vec(),
                codes::TABLES.to_vec(),
                table.as_bytes().to_vec(),
                codes::NAMES.to_vec(),
                codes::CONSTRAINTS.to_vec(),
                name_component(canonical),
            ],
            Self::ActiveConstraint { table, identity } => vec![
                codes::ROOT.to_vec(),
                codes::TABLES.to_vec(),
                table.as_bytes().to_vec(),
                codes::ACTIVE_CONSTRAINTS.to_vec(),
                identity.to_vec(),
            ],
            Self::ConstraintReferencePrefix { table, identity } => vec![
                codes::ROOT.to_vec(),
                codes::TABLES.to_vec(),
                table.as_bytes().to_vec(),
                codes::CONSTRAINT_REFERENCES.to_vec(),
                identity.to_vec(),
            ],
            Self::ConstraintReference {
                table,
                identity,
                relationship,
            } => vec![
                codes::ROOT.to_vec(),
                codes::TABLES.to_vec(),
                table.as_bytes().to_vec(),
                codes::CONSTRAINT_REFERENCES.to_vec(),
                identity.to_vec(),
                relationship.as_bytes().to_vec(),
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
            Self::ColumnViewDependency {
                table,
                column,
                canonical,
            } => vec![
                codes::ROOT.to_vec(),
                codes::TABLES.to_vec(),
                table.as_bytes().to_vec(),
                codes::COLUMN_DEPENDENCIES.to_vec(),
                column.as_bytes().to_vec(),
                codes::VIEW_DEPENDENCIES.to_vec(),
                name_component(canonical),
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
            Self::RowPrefix { table } => vec![
                codes::ROOT.to_vec(),
                codes::TABLES.to_vec(),
                table.as_bytes().to_vec(),
                codes::ROWS.to_vec(),
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
            Self::UserVersion => vec![
                codes::ROOT.to_vec(),
                codes::METADATA.to_vec(),
                codes::USER_VERSION.to_vec(),
            ],
            Self::ViewDependencyPrefix { table } => vec![
                codes::ROOT.to_vec(),
                codes::TABLES.to_vec(),
                table.as_bytes().to_vec(),
                codes::VIEW_DEPENDENCIES.to_vec(),
            ],
            Self::ViewDependency { table, canonical } => vec![
                codes::ROOT.to_vec(),
                codes::TABLES.to_vec(),
                table.as_bytes().to_vec(),
                codes::VIEW_DEPENDENCIES.to_vec(),
                name_component(canonical),
            ],
        };
        let key = Key::from_bytes(components)?;
        debug_assert_eq!(TargetFamily::classify(&key), Some(self.family()));
        Ok(key)
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

pub fn view_dependency_prefix(table: TableId) -> Key {
    LogicalTarget::ViewDependencyPrefix { table }
        .render()
        .expect("view-dependency prefix is bounded")
}

pub fn view_dependency_key(table: TableId, view: &SqlName) -> Key {
    LogicalTarget::ViewDependency {
        table,
        canonical: view.canonical().to_vec(),
    }
    .render()
    .expect("view-dependency key is bounded")
}

pub fn column_view_dependency_key(table: TableId, column: ColumnId, view: &SqlName) -> Key {
    LogicalTarget::ColumnViewDependency {
        table,
        column,
        canonical: view.canonical().to_vec(),
    }
    .render()
    .expect("column view-dependency key is bounded")
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

    #[test]
    fn reasoned_plan_preserves_raw_evidence_and_derives_pruned_footprint() {
        let table = id(1, TableId::from_bytes);
        let index = id(2, IndexId::from_bytes);
        let row = LogicalTarget::Row {
            table,
            index,
            images: vec![b"row".to_vec()],
        }
        .render()
        .unwrap();
        let relationship = id(3, ForeignKeyId::from_bytes);
        let reference_prefix = LogicalTarget::ForeignReferencePrefix {
            parent: table,
            relationship,
            parent_index: index,
            parent_images: vec![b"parent".to_vec()],
        }
        .render()
        .unwrap();
        let reference = LogicalTarget::ForeignReference {
            parent: table,
            relationship,
            parent_index: index,
            parent_images: vec![b"parent".to_vec()],
            child_index: index,
            child_images: vec![b"child".to_vec()],
        }
        .render()
        .unwrap();
        let mut plan = GuardPlan::for_operation(OperationFamily::RowChanges);
        plan.write(row, GuardReason::RowIdentity).unwrap();
        plan.invariant(reference, GuardReason::ForeignChildren)
            .unwrap();
        plan.invariant(reference_prefix.clone(), GuardReason::ForeignChildren)
            .unwrap();
        let read = LogicalTarget::TableRoot {
            table: id(4, TableId::from_bytes),
        }
        .render()
        .unwrap();
        let mut reads = GuardPlan::for_operation(OperationFamily::TransactionRead);
        reads
            .serializable_read(read.clone(), GuardReason::SerializableRead)
            .unwrap();
        let mut merged = GuardPlan::merged();
        merged.extend(plan);
        merged.extend(reads);

        assert_eq!(merged.entries().len(), 4);
        assert_eq!(merged.entries()[0].operation(), OperationFamily::RowChanges);
        assert_eq!(merged.entries()[0].family(), TargetFamily::Row);
        assert_eq!(merged.entries()[1].class(), GuardClass::Invariant);
        assert_eq!(merged.entries()[2].reason(), GuardReason::ForeignChildren);
        assert_eq!(merged.entries()[3].target(), &read);
        let footprint = merged.footprint();
        assert_eq!(
            footprint.constraints(),
            &std::collections::BTreeSet::from([reference_prefix])
        );
        assert_eq!(footprint.writes().len(), 1);
        assert_eq!(footprint.reads(), &std::collections::BTreeSet::from([read]));
    }

    #[test]
    fn reason_vocabulary_is_finite_and_reviewable() {
        let reasons = [
            GuardReason::SchemaObjectName,
            GuardReason::ColumnNameBinding,
            GuardReason::SchemaRevision,
            GuardReason::WriteContract,
            GuardReason::PrimaryIndex,
            GuardReason::RowIdentity,
            GuardReason::UniqueOwnership,
            GuardReason::ForeignReference,
            GuardReason::ForeignChildren,
            GuardReason::ColumnDependency,
            GuardReason::ExistingRows,
            GuardReason::TableExistence,
            GuardReason::SerializableRead,
            GuardReason::UserVersion,
            GuardReason::ViewDependency,
        ];
        assert_eq!(reasons.len(), 15);
    }

    #[test]
    fn plans_reject_targets_outside_the_registry() {
        let mut plan = GuardPlan::for_operation(OperationFamily::RowChanges);
        let unknown = Key::from_bytes([b"other".as_slice(), b"key".as_slice()]).unwrap();
        assert!(matches!(
            plan.write(unknown, GuardReason::RowIdentity),
            Err(Error::CaptureInvariant(
                "guard target is outside the logical target registry"
            ))
        ));

        let registered = LogicalTarget::TableRoot {
            table: id(1, TableId::from_bytes),
        }
        .render()
        .unwrap();
        assert!(matches!(
            plan.write(registered, GuardReason::RowIdentity),
            Err(Error::CaptureInvariant(
                "guard is absent from the operation contract"
            ))
        ));
    }

    #[test]
    fn checked_guard_audit_matches_the_compiler_contracts() {
        assert_eq!(include_str!("../../GUARDS.md"), audit_markdown());
    }

    #[test]
    fn mutation_and_rejection_contracts_are_enforced() {
        let table = id(1, TableId::from_bytes);
        let index = id(2, IndexId::from_bytes);
        let row = LogicalTarget::Row {
            table,
            index,
            images: vec![b"row".to_vec()],
        }
        .render()
        .unwrap();
        validate_mutations(
            OperationFamily::RowChanges,
            &[Mutation::Set {
                key: row.clone(),
                value: Vec::new(),
            }],
        )
        .unwrap();
        assert!(matches!(
            validate_mutations(
                OperationFamily::CreateTable,
                &[Mutation::Delete { key: row.clone() }]
            ),
            Err(Error::CaptureInvariant(
                "mutation is absent from the operation contract"
            ))
        ));
        assert!(matches!(
            validate_mutations(
                OperationFamily::RowChanges,
                &[Mutation::DeleteRange { range: Range::Full }]
            ),
            Err(Error::CaptureInvariant(
                "Multilite operations cannot delete the full Homebase keyspace"
            ))
        ));
        validate_rejection(
            OperationFamily::RowChanges,
            RejectionKind::RestoreRowChanges,
        )
        .unwrap();
        assert!(
            validate_rejection(
                OperationFamily::RowChanges,
                RejectionKind::RemoveCreatedTable,
            )
            .is_err()
        );
    }

    #[test]
    fn compiled_output_rejects_missing_global_and_exact_mutation_guards() {
        let table = id(1, TableId::from_bytes);
        let index = id(2, IndexId::from_bytes);
        let row = LogicalTarget::Row {
            table,
            index,
            images: vec![b"row".to_vec()],
        }
        .render()
        .unwrap();
        let mutations = [Mutation::Set {
            key: row.clone(),
            value: Vec::new(),
        }];
        let mut guards = GuardPlan::for_operation(OperationFamily::RowChanges);

        assert!(matches!(
            validate_compiled_output(OperationFamily::RowChanges, &mutations, &guards),
            Err(Error::CaptureInvariant(
                "compiler omitted a required operation guard"
            ))
        ));

        guards
            .invariant(
                LogicalTarget::ActivePrimaryIndex { table }
                    .render()
                    .unwrap(),
                GuardReason::PrimaryIndex,
            )
            .unwrap();
        guards
            .invariant(
                LogicalTarget::WriteRevision { table }.render().unwrap(),
                GuardReason::WriteContract,
            )
            .unwrap();
        guards.write(row.clone(), GuardReason::RowIdentity).unwrap();
        assert!(matches!(
            validate_compiled_output(OperationFamily::RowChanges, &mutations, &guards),
            Err(Error::CaptureInvariant(
                "compiler omitted a guard required by a mutation"
            ))
        ));

        guards.invariant(row, GuardReason::RowIdentity).unwrap();
        validate_compiled_output(OperationFamily::RowChanges, &mutations, &guards).unwrap();
    }

    #[test]
    fn central_contract_tables_are_duplicate_free_and_cover_every_operation() {
        use std::collections::BTreeSet;

        let mutations = MUTATION_CONTRACTS
            .iter()
            .map(|contract| (contract.operation, contract.kind, contract.target))
            .collect::<BTreeSet<_>>();
        assert_eq!(mutations.len(), MUTATION_CONTRACTS.len());
        let guards = GUARD_CONTRACTS
            .iter()
            .map(|contract| {
                (
                    contract.operation,
                    contract.class,
                    contract.reason,
                    contract.target,
                )
            })
            .collect::<BTreeSet<_>>();
        assert_eq!(guards.len(), GUARD_CONTRACTS.len());
        assert!(
            REQUIRED_GUARD_CONTRACTS
                .iter()
                .all(|required| { GUARD_CONTRACTS.iter().any(|allowed| allowed == required) })
        );
        let rejections = REJECTION_CONTRACTS
            .iter()
            .map(|contract| (contract.operation, contract.rejection))
            .collect::<BTreeSet<_>>();
        assert_eq!(rejections.len(), REJECTION_CONTRACTS.len());

        let logical_operations = BTreeSet::from([
            OperationFamily::CreateTable,
            OperationFamily::DropTable,
            OperationFamily::RowChanges,
            OperationFamily::CreateIndex,
            OperationFamily::DropIndex,
            OperationFamily::RenameTable,
            OperationFamily::RenameColumn,
            OperationFamily::AddColumn,
            OperationFamily::DropColumn,
            OperationFamily::DropConstraint,
            OperationFamily::SetUserVersion,
            OperationFamily::CreateView,
            OperationFamily::DropView,
        ]);
        assert_eq!(
            REJECTION_CONTRACTS
                .iter()
                .map(|contract| contract.operation)
                .collect::<BTreeSet<_>>(),
            logical_operations
        );
        assert!(logical_operations.iter().all(|operation| {
            MUTATION_CONTRACTS
                .iter()
                .any(|contract| contract.operation == *operation)
        }));
        assert!(logical_operations.iter().all(|operation| {
            GUARD_CONTRACTS
                .iter()
                .any(|contract| contract.operation == *operation)
        }));
    }
}
