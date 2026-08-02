# Multilite Operation Contracts

This file is generated from the checked contracts in `src/logical/guard.rs`. The compiler rejects mutations, guards, and rejection repairs absent from these tables.

Guard classes have distinct semantics: `Invariant` is mandatory at every isolation level, `Write` participates in write/write validation, and `SerializableRead` is added only for serializable transactions. Repeated runtime guards are retained for auditability before the executable footprint is prefix-pruned.

## Mutations

| Operation | Mutation | Target family |
| --- | --- | --- |
| `TransactionEnvelope` | `Set` | `TransactionLog` |
| `CreateTable` | `Set` | `SchemaLog` |
| `CreateTable` | `Set` | `SchemaObjectName` |
| `CreateTable` | `Set` | `TableSchema` |
| `CreateTable` | `Set` | `ActivePrimaryIndex` |
| `CreateTable` | `Set` | `ActiveSchemaRevision` |
| `CreateTable` | `Set` | `IndexDefinition` |
| `CreateTable` | `Set` | `ColumnName` |
| `CreateTable` | `Set` | `WriteRevision` |
| `DropTable` | `Set` | `SchemaLog` |
| `DropTable` | `Delete` | `SchemaObjectName` |
| `DropTable` | `DeletePrefix` | `TableRoot` |
| `DropTable` | `DeletePrefix` | `ForeignReference` |
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
| `CreateIndex` | `Set` | `UniqueOwner` |
| `CreateIndex` | `Set` | `WriteRevision` |
| `DropIndex` | `Set` | `SchemaLog` |
| `DropIndex` | `Set` | `TableSchema` |
| `DropIndex` | `Set` | `ActiveSchemaRevision` |
| `DropIndex` | `Delete` | `SchemaObjectName` |
| `DropIndex` | `Delete` | `ColumnDependency` |
| `RenameTable` | `Set` | `SchemaLog` |
| `RenameTable` | `Set` | `SchemaObjectName` |
| `RenameTable` | `Delete` | `SchemaObjectName` |
| `RenameColumn` | `Set` | `SchemaLog` |
| `RenameColumn` | `Set` | `ColumnName` |
| `RenameColumn` | `Delete` | `ColumnName` |
| `AddColumn` | `Set` | `SchemaLog` |
| `AddColumn` | `Set` | `ColumnName` |
| `AddColumn` | `Set` | `TableSchema` |
| `AddColumn` | `Set` | `ColumnDependency` |
| `AddColumn` | `Set` | `WriteRevision` |
| `DropColumn` | `Set` | `SchemaLog` |
| `DropColumn` | `Set` | `TableSchema` |
| `DropColumn` | `Delete` | `ColumnName` |
| `DropColumn` | `Delete` | `ColumnDependency` |
| `DropColumn` | `DeletePrefix` | `ColumnDependency` |

## Guards

| Operation | Class | Reason | Target family |
| --- | --- | --- | --- |
| `CreateTable` | `Invariant` | `SchemaObjectName` | `SchemaObjectName` |
| `CreateTable` | `Invariant` | `SchemaRevision` | `ActiveSchemaRevision` |
| `CreateTable` | `Write` | `WriteContract` | `WriteRevision` |
| `DropTable` | `Invariant` | `TableExistence` | `TableRoot` |
| `DropTable` | `Write` | `TableExistence` | `TableRoot` |
| `DropTable` | `Invariant` | `SchemaObjectName` | `SchemaObjectName` |
| `DropTable` | `Invariant` | `ForeignReference` | `ForeignReference` |
| `DropTable` | `Write` | `ForeignReference` | `ForeignReference` |
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
| `DropIndex` | `Invariant` | `SchemaObjectName` | `SchemaObjectName` |
| `DropIndex` | `Invariant` | `SchemaRevision` | `ActiveSchemaRevision` |
| `DropIndex` | `Write` | `SchemaRevision` | `ActiveSchemaRevision` |
| `DropIndex` | `Invariant` | `ColumnDependency` | `ColumnDependency` |
| `DropIndex` | `Write` | `ColumnDependency` | `ColumnDependency` |
| `DropIndex` | `Invariant` | `WriteContract` | `WriteRevision` |
| `RenameTable` | `Invariant` | `SchemaObjectName` | `SchemaObjectName` |
| `RenameColumn` | `Invariant` | `ColumnNameBinding` | `ColumnName` |
| `AddColumn` | `Invariant` | `ColumnNameBinding` | `ColumnName` |
| `AddColumn` | `Invariant` | `ColumnDependency` | `ColumnDependency` |
| `AddColumn` | `Write` | `ColumnDependency` | `ColumnDependency` |
| `AddColumn` | `Write` | `WriteContract` | `WriteRevision` |
| `AddColumn` | `Invariant` | `ExistingRows` | `Row` |
| `DropColumn` | `Invariant` | `ColumnNameBinding` | `ColumnName` |
| `DropColumn` | `Invariant` | `ColumnDependency` | `ColumnDependency` |
| `DropColumn` | `Write` | `ColumnDependency` | `ColumnDependency` |
| `TransactionRead` | `SerializableRead` | `SerializableRead` | `TableRoot` |

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
