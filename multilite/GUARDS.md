# Multilite Operation Contracts

This file is generated from the checked contracts in `src/logical/guard.rs`. The compiler rejects mutations, guards, and rejection repairs absent from these tables.

Guard classes have distinct semantics: `Invariant` is mandatory at every isolation level, `Write` participates in write/write validation, and `SerializableRead` is added only for serializable transactions. Repeated runtime guards are retained for auditability before the executable footprint is prefix-pruned.

## Mutations

| Operation | Mutation | Target family |
| --- | --- | --- |
| `TransactionEnvelope` | `Set` | `TransactionLog` |
| `SetUserVersion` | `Set` | `UserVersion` |
| `CreateView` | `Set` | `SchemaLog` |
| `CreateView` | `Set` | `SchemaObjectName` |
| `CreateView` | `Set` | `ViewDependency` |
| `CreateView` | `Set` | `ColumnDependency` |
| `DropView` | `Set` | `SchemaLog` |
| `DropView` | `Delete` | `SchemaObjectName` |
| `DropView` | `Delete` | `ViewDependency` |
| `DropView` | `Delete` | `ColumnDependency` |
| `CreateTable` | `Set` | `SchemaLog` |
| `CreateTable` | `Set` | `SchemaObjectName` |
| `CreateTable` | `Set` | `TableSchema` |
| `CreateTable` | `Set` | `ActivePrimaryIndex` |
| `CreateTable` | `Set` | `ActiveSchemaRevision` |
| `CreateTable` | `Set` | `IndexDefinition` |
| `CreateTable` | `Set` | `ColumnName` |
| `CreateTable` | `Set` | `ConstraintName` |
| `CreateTable` | `Set` | `ActiveConstraint` |
| `CreateTable` | `Set` | `ConstraintReference` |
| `CreateTable` | `Set` | `WriteRevision` |
| `DropTable` | `Set` | `SchemaLog` |
| `DropTable` | `Delete` | `SchemaObjectName` |
| `DropTable` | `DeletePrefix` | `TableRoot` |
| `DropTable` | `DeletePrefix` | `ForeignReference` |
| `DropTable` | `Delete` | `ConstraintReference` |
| `RowChanges` | `Set` | `Row` |
| `RowChanges` | `Set` | `UniqueOwner` |
| `RowChanges` | `Set` | `ForeignReference` |
| `RowChanges` | `Delete` | `Row` |
| `RowChanges` | `Delete` | `UniqueOwner` |
| `RowChanges` | `Delete` | `ForeignReference` |
| `RowChanges` | `DeletePrefix` | `ForeignReference` |
| `CreateIndex` | `Set` | `SchemaLog` |
| `CreateIndex` | `Set` | `SchemaObjectName` |
| `CreateIndex` | `Set` | `TableSchema` |
| `CreateIndex` | `Set` | `ActiveSchemaRevision` |
| `CreateIndex` | `Set` | `ColumnDependency` |
| `CreateIndex` | `Set` | `IndexDefinition` |
| `CreateIndex` | `Set` | `ActiveConstraint` |
| `CreateIndex` | `Set` | `UniqueOwner` |
| `CreateIndex` | `Set` | `WriteRevision` |
| `DropIndex` | `Set` | `SchemaLog` |
| `DropIndex` | `Set` | `TableSchema` |
| `DropIndex` | `Set` | `ActiveSchemaRevision` |
| `DropIndex` | `Delete` | `SchemaObjectName` |
| `DropIndex` | `Delete` | `ColumnDependency` |
| `DropIndex` | `Delete` | `ActiveConstraint` |
| `DropIndex` | `DeletePrefix` | `ConstraintReference` |
| `RenameTable` | `Set` | `SchemaLog` |
| `RenameTable` | `Set` | `SchemaObjectName` |
| `RenameTable` | `Delete` | `SchemaObjectName` |
| `RenameColumn` | `Set` | `SchemaLog` |
| `RenameColumn` | `Set` | `ColumnName` |
| `RenameColumn` | `Delete` | `ColumnName` |
| `AddColumn` | `Set` | `SchemaLog` |
| `AddColumn` | `Set` | `ColumnName` |
| `AddColumn` | `Set` | `ConstraintName` |
| `AddColumn` | `Set` | `ConstraintReference` |
| `AddColumn` | `Set` | `TableSchema` |
| `AddColumn` | `Set` | `ColumnDependency` |
| `AddColumn` | `Set` | `WriteRevision` |
| `DropColumn` | `Set` | `SchemaLog` |
| `DropColumn` | `Set` | `TableSchema` |
| `DropColumn` | `Delete` | `ColumnName` |
| `DropColumn` | `Delete` | `ConstraintName` |
| `DropColumn` | `Delete` | `ColumnDependency` |
| `DropColumn` | `DeletePrefix` | `ColumnDependency` |
| `DropConstraint` | `Set` | `SchemaLog` |
| `DropConstraint` | `Set` | `TableSchema` |
| `DropConstraint` | `Delete` | `ConstraintName` |
| `DropConstraint` | `Delete` | `ActiveConstraint` |
| `DropConstraint` | `Delete` | `ConstraintReference` |
| `DropConstraint` | `DeletePrefix` | `ConstraintReference` |

## Guards

| Operation | Class | Reason | Target family |
| --- | --- | --- | --- |
| `CreateTable` | `Invariant` | `SchemaObjectName` | `SchemaObjectName` |
| `CreateTable` | `Invariant` | `SchemaRevision` | `ActiveSchemaRevision` |
| `CreateTable` | `Invariant` | `ConstraintState` | `ActiveConstraint` |
| `CreateTable` | `Invariant` | `ConstraintReference` | `ConstraintReference` |
| `CreateTable` | `Write` | `ConstraintReference` | `ConstraintReference` |
| `CreateTable` | `Write` | `WriteContract` | `WriteRevision` |
| `DropTable` | `Invariant` | `TableExistence` | `TableRoot` |
| `DropTable` | `Write` | `TableExistence` | `TableRoot` |
| `DropTable` | `Invariant` | `SchemaObjectName` | `SchemaObjectName` |
| `DropTable` | `Invariant` | `ForeignReference` | `ForeignReference` |
| `DropTable` | `Write` | `ForeignReference` | `ForeignReference` |
| `DropTable` | `Invariant` | `ConstraintReference` | `ConstraintReference` |
| `DropTable` | `Write` | `ConstraintReference` | `ConstraintReference` |
| `RowChanges` | `Invariant` | `RowIdentity` | `Row` |
| `RowChanges` | `Write` | `RowIdentity` | `Row` |
| `RowChanges` | `Invariant` | `UniqueOwnership` | `UniqueOwner` |
| `RowChanges` | `Write` | `UniqueOwnership` | `UniqueOwner` |
| `RowChanges` | `Invariant` | `ForeignReference` | `ForeignReference` |
| `RowChanges` | `Write` | `ForeignReference` | `ForeignReference` |
| `RowChanges` | `Invariant` | `ForeignChildren` | `ForeignReference` |
| `RowChanges` | `Write` | `ForeignChildren` | `ForeignReference` |
| `RowChanges` | `Invariant` | `PrimaryIndex` | `ActivePrimaryIndex` |
| `RowChanges` | `Invariant` | `WriteContract` | `WriteRevision` |
| `CreateIndex` | `Invariant` | `SchemaObjectName` | `SchemaObjectName` |
| `CreateIndex` | `Invariant` | `SchemaRevision` | `ActiveSchemaRevision` |
| `CreateIndex` | `Write` | `SchemaRevision` | `ActiveSchemaRevision` |
| `CreateIndex` | `Invariant` | `ColumnNameBinding` | `ColumnName` |
| `CreateIndex` | `Invariant` | `ColumnDependency` | `ColumnDependency` |
| `CreateIndex` | `Write` | `ColumnDependency` | `ColumnDependency` |
| `CreateIndex` | `Write` | `WriteContract` | `WriteRevision` |
| `CreateIndex` | `Invariant` | `ExistingRows` | `Row` |
| `CreateIndex` | `Invariant` | `ConstraintState` | `ActiveConstraint` |
| `CreateIndex` | `Write` | `ConstraintState` | `ActiveConstraint` |
| `DropIndex` | `Invariant` | `SchemaObjectName` | `SchemaObjectName` |
| `DropIndex` | `Invariant` | `SchemaRevision` | `ActiveSchemaRevision` |
| `DropIndex` | `Write` | `SchemaRevision` | `ActiveSchemaRevision` |
| `DropIndex` | `Invariant` | `ColumnDependency` | `ColumnDependency` |
| `DropIndex` | `Write` | `ColumnDependency` | `ColumnDependency` |
| `DropIndex` | `Invariant` | `WriteContract` | `WriteRevision` |
| `DropIndex` | `Invariant` | `ConstraintState` | `ActiveConstraint` |
| `DropIndex` | `Write` | `ConstraintState` | `ActiveConstraint` |
| `DropIndex` | `Invariant` | `ConstraintReference` | `ConstraintReference` |
| `DropIndex` | `Write` | `ConstraintReference` | `ConstraintReference` |
| `RenameTable` | `Invariant` | `SchemaObjectName` | `SchemaObjectName` |
| `RenameColumn` | `Invariant` | `ColumnNameBinding` | `ColumnName` |
| `AddColumn` | `Invariant` | `ColumnNameBinding` | `ColumnName` |
| `AddColumn` | `Write` | `ColumnNameBinding` | `ColumnName` |
| `AddColumn` | `Invariant` | `ConstraintNameBinding` | `ConstraintName` |
| `AddColumn` | `Invariant` | `SchemaObjectName` | `SchemaObjectName` |
| `AddColumn` | `Invariant` | `SchemaRevision` | `ActiveSchemaRevision` |
| `AddColumn` | `Invariant` | `ConstraintState` | `ActiveConstraint` |
| `AddColumn` | `Invariant` | `ConstraintReference` | `ConstraintReference` |
| `AddColumn` | `Write` | `ConstraintReference` | `ConstraintReference` |
| `AddColumn` | `Invariant` | `ExistingRows` | `TableRows` |
| `AddColumn` | `Invariant` | `ColumnDependency` | `ColumnDependency` |
| `AddColumn` | `Write` | `ColumnDependency` | `ColumnDependency` |
| `AddColumn` | `Write` | `WriteContract` | `WriteRevision` |
| `AddColumn` | `Invariant` | `ExistingRows` | `Row` |
| `DropColumn` | `Invariant` | `ColumnNameBinding` | `ColumnName` |
| `DropColumn` | `Invariant` | `ConstraintNameBinding` | `ConstraintName` |
| `DropColumn` | `Write` | `ConstraintNameBinding` | `ConstraintName` |
| `DropColumn` | `Invariant` | `ColumnDependency` | `ColumnDependency` |
| `DropColumn` | `Write` | `ColumnDependency` | `ColumnDependency` |
| `DropConstraint` | `Invariant` | `ConstraintNameBinding` | `ConstraintName` |
| `DropConstraint` | `Write` | `ConstraintNameBinding` | `ConstraintName` |
| `DropConstraint` | `Invariant` | `ConstraintState` | `ActiveConstraint` |
| `DropConstraint` | `Write` | `ConstraintState` | `ActiveConstraint` |
| `DropConstraint` | `Invariant` | `ConstraintReference` | `ConstraintReference` |
| `DropConstraint` | `Write` | `ConstraintReference` | `ConstraintReference` |
| `TransactionRead` | `SerializableRead` | `SerializableRead` | `TableRoot` |
| `TransactionRead` | `SerializableRead` | `SerializableRead` | `SchemaObjectName` |
| `SetUserVersion` | `Write` | `UserVersion` | `UserVersion` |
| `CreateView` | `Invariant` | `SchemaObjectName` | `SchemaObjectName` |
| `CreateView` | `Invariant` | `ViewDependency` | `SchemaObjectName` |
| `CreateView` | `Invariant` | `ViewDependency` | `ColumnName` |
| `CreateView` | `Invariant` | `ViewDependency` | `ViewDependency` |
| `CreateView` | `Invariant` | `ColumnDependency` | `ColumnDependency` |
| `CreateView` | `Write` | `ColumnDependency` | `ColumnDependency` |
| `DropView` | `Invariant` | `SchemaObjectName` | `SchemaObjectName` |
| `DropView` | `Invariant` | `ViewDependency` | `SchemaObjectName` |
| `DropView` | `Invariant` | `ViewDependency` | `ColumnName` |
| `DropView` | `Invariant` | `ViewDependency` | `ViewDependency` |
| `DropView` | `Invariant` | `ColumnDependency` | `ColumnDependency` |
| `DropView` | `Write` | `ColumnDependency` | `ColumnDependency` |
| `DropColumn` | `Invariant` | `ViewDependency` | `ViewDependency` |
| `TransactionRead` | `SerializableRead` | `SerializableRead` | `UserVersion` |

## Rejection Repair

| Operation | Local inverse |
| --- | --- |
| `CreateTable` | `RemoveCreatedTable` |
| `DropTable` | `RestoreDroppedTable` |
| `RowChanges` | `RestoreRowChanges` |
| `CreateIndex` | `RevertIndex` |
| `DropIndex` | `RevertIndex` |
| `RenameTable` | `RevertAlterTable` |
| `RenameColumn` | `RevertAlterTable` |
| `AddColumn` | `RevertAlterTable` |
| `DropColumn` | `RevertAlterTable` |
| `DropConstraint` | `RevertAlterTable` |
| `SetUserVersion` | `RestoreUserVersion` |
| `CreateView` | `RevertView` |
| `DropView` | `RevertView` |

## Required Guards

These family-level guards must occur at least once. In addition, the compiler requires exact guards for every mutation whose safety depends on its rendered target.

| Operation | Class | Reason | Target family |
| --- | --- | --- | --- |
| `CreateTable` | `Invariant` | `SchemaObjectName` | `SchemaObjectName` |
| `CreateTable` | `Write` | `WriteContract` | `WriteRevision` |
| `DropTable` | `Invariant` | `TableExistence` | `TableRoot` |
| `DropTable` | `Write` | `TableExistence` | `TableRoot` |
| `DropTable` | `Invariant` | `SchemaObjectName` | `SchemaObjectName` |
| `RowChanges` | `Invariant` | `PrimaryIndex` | `ActivePrimaryIndex` |
| `RowChanges` | `Invariant` | `WriteContract` | `WriteRevision` |
| `CreateIndex` | `Invariant` | `SchemaObjectName` | `SchemaObjectName` |
| `CreateIndex` | `Invariant` | `SchemaRevision` | `ActiveSchemaRevision` |
| `CreateIndex` | `Write` | `SchemaRevision` | `ActiveSchemaRevision` |
| `DropIndex` | `Invariant` | `SchemaObjectName` | `SchemaObjectName` |
| `DropIndex` | `Invariant` | `SchemaRevision` | `ActiveSchemaRevision` |
| `DropIndex` | `Write` | `SchemaRevision` | `ActiveSchemaRevision` |
| `RenameTable` | `Invariant` | `SchemaObjectName` | `SchemaObjectName` |
| `RenameColumn` | `Invariant` | `ColumnNameBinding` | `ColumnName` |
| `AddColumn` | `Invariant` | `ColumnNameBinding` | `ColumnName` |
| `DropColumn` | `Invariant` | `ColumnNameBinding` | `ColumnName` |
| `DropColumn` | `Invariant` | `ViewDependency` | `ViewDependency` |
| `DropConstraint` | `Invariant` | `ConstraintNameBinding` | `ConstraintName` |
| `SetUserVersion` | `Write` | `UserVersion` | `UserVersion` |
| `CreateView` | `Invariant` | `SchemaObjectName` | `SchemaObjectName` |
| `DropView` | `Invariant` | `SchemaObjectName` | `SchemaObjectName` |
