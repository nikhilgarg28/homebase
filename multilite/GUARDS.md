# Multilite Operation Contracts

This file is generated from the checked contracts in `src/database/guard.rs`. The compiler rejects mutations, guards, and rejection repairs absent from these tables.

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
| `InsertRows` | `Set` | `Row` |
| `InsertRows` | `Set` | `UniqueOwner` |
| `InsertRows` | `Set` | `ForeignReference` |
| `DeleteRows` | `Delete` | `Row` |
| `DeleteRows` | `Delete` | `UniqueOwner` |
| `DeleteRows` | `Delete` | `ForeignReference` |
| `DeleteRows` | `DeletePrefix` | `ForeignReference` |
| `UpdateRows` | `Set` | `Row` |
| `UpdateRows` | `Set` | `UniqueOwner` |
| `UpdateRows` | `Set` | `ForeignReference` |
| `UpdateRows` | `Delete` | `Row` |
| `UpdateRows` | `Delete` | `UniqueOwner` |
| `UpdateRows` | `Delete` | `ForeignReference` |
| `UpdateRows` | `DeletePrefix` | `ForeignReference` |
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
| `InsertRows` | `Invariant` | `RowIdentity` | `Row` |
| `InsertRows` | `Write` | `RowIdentity` | `Row` |
| `InsertRows` | `Invariant` | `UniqueOwnership` | `UniqueOwner` |
| `InsertRows` | `Write` | `UniqueOwnership` | `UniqueOwner` |
| `InsertRows` | `Invariant` | `ForeignReference` | `ForeignReference` |
| `InsertRows` | `Write` | `ForeignReference` | `ForeignReference` |
| `InsertRows` | `Invariant` | `PrimaryIndex` | `ActivePrimaryIndex` |
| `InsertRows` | `Invariant` | `WriteContract` | `WriteRevision` |
| `DeleteRows` | `Write` | `RowIdentity` | `Row` |
| `DeleteRows` | `Write` | `UniqueOwnership` | `UniqueOwner` |
| `DeleteRows` | `Write` | `ForeignReference` | `ForeignReference` |
| `DeleteRows` | `Invariant` | `ForeignChildren` | `ForeignReference` |
| `DeleteRows` | `Write` | `ForeignChildren` | `ForeignReference` |
| `DeleteRows` | `Invariant` | `PrimaryIndex` | `ActivePrimaryIndex` |
| `DeleteRows` | `Invariant` | `WriteContract` | `WriteRevision` |
| `UpdateRows` | `Invariant` | `RowIdentity` | `Row` |
| `UpdateRows` | `Write` | `RowIdentity` | `Row` |
| `UpdateRows` | `Invariant` | `UniqueOwnership` | `UniqueOwner` |
| `UpdateRows` | `Write` | `UniqueOwnership` | `UniqueOwner` |
| `UpdateRows` | `Invariant` | `ForeignReference` | `ForeignReference` |
| `UpdateRows` | `Write` | `ForeignReference` | `ForeignReference` |
| `UpdateRows` | `Invariant` | `ForeignChildren` | `ForeignReference` |
| `UpdateRows` | `Write` | `ForeignChildren` | `ForeignReference` |
| `UpdateRows` | `Invariant` | `PrimaryIndex` | `ActivePrimaryIndex` |
| `UpdateRows` | `Invariant` | `WriteContract` | `WriteRevision` |
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
| `CreateTable` | `DropTable` |
| `InsertRows` | `DeleteRows` |
| `DeleteRows` | `RestoreRows` |
| `UpdateRows` | `RestoreUpdatedRows` |
| `CreateIndex` | `RevertIndex` |
| `DropIndex` | `RevertIndex` |
| `RenameTable` | `RevertAlterTable` |
| `RenameColumn` | `RevertAlterTable` |
| `AddColumn` | `RevertAlterTable` |
| `DropColumn` | `RevertAlterTable` |
