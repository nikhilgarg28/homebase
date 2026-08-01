# UUID Schema Evolution

This note defines the boundary between immutable synchronized DDL and mutable
SQLite materialization state.

## Core Invariants

1. Every SQL identifier is resolved against one transaction snapshot. Once
   resolved, logical operations refer to table, column, index, relationship,
   keyspace, and schema objects by UUID.
2. The SQL accepted from the application is immutable provenance. It is parsed
   and cross-checked against the resolved UUID operation, but is never rewritten
   in an existing log record.
3. The local catalog is a derived materialization index. Its table-name column
   is the current `(schema, name) -> TableId` binding; its encoded definition is
   a validated fold of immutable DDL operations. Every folded encoding carries
   a recomputed content-addressed revision, including folds whose mutable
   spellings changed without advancing an authority conflict cell.
4. SQLite execution uses freshly rendered SQL. Rendering may substitute current
   names for UUID-resolved objects, but may not change expressions, ordering,
   conflict behavior, or any other semantic property of the authenticated
   operation.

`SchemaRevisionId` is a content fingerprint of one complete encoded table IR.
Decoding recomputes it, so a valid-looking structural edit cannot retain the
old revision. Homebase's table-schema namespace contains the authenticated
before/after snapshots published by individual DDL operations. The local
catalog may additionally derive a deterministic fold of several commuting
operations; that folded revision need not have its own Homebase snapshot cell.
The mutable active-schema cell is the distributed conflict frontier, not a
promise that every locally derived fold is independently fetchable by revision.
5. DML never re-resolves a stored SQL name. Row operations carry stable IDs and
   apply through the current catalog binding. Reusing an old name for a new table
   cannot retarget an older operation.
6. Pending state stores typed semantic inverses plus explicit preconditions, not
   arbitrary executable undo SQL. Inverses resolve current names by UUID and run
   in reverse operation order inside the canonical repair transaction.
7. A name-only table rename does not change row encoding, keyspaces, columns,
   indexes, relationships, or the write contract. It therefore does not
   invalidate stale DML.

## Current Grammar

### CREATE TABLE

The immutable operation records the original SQL and resolved UUID graph.
Canonical apply renders the table's own creation name and rewrites each
foreign-key parent reference from its stable `TableId` to that parent's current
catalog name. This lets a stale but otherwise compatible relationship creation
survive a parent rename.

Rejection drops the created table by `TableId`, after proving that the catalog
still contains the expected created definition. It never drops an object merely
because it reused the same text name.

### INSERT, UPDATE, DELETE

Captured rows already carry table and column UUIDs plus complete SQLite storage
values. Canonical apply and rejection repair resolve the current table name by
`TableId`, render column names from the compatible structural definition, and
never execute the original DML text.

Compatibility still depends on primary keyspace, mandatory UNIQUE ownership,
foreign-key bookkeeping, storage mode, and the table write contract. Rename is
absent from that contract.

### CREATE INDEX and DROP INDEX

The immutable operation records original SQL and binds it to an owner `TableId`,
index UUID, and structured terms/predicate. Catalog snapshots store only the
typed index IR. CREATE, DROP, and rejection render from that IR against current
table and column bindings; historical SQL is never a second executable source.

### ALTER TABLE RENAME TO

The logical operation needs only an operation UUID, target `TableId`, old and new
`SqlName`s, and optional original SQL provenance. Apply verifies the target's
current binding, lets SQLite perform the physical rename, and atomically moves
the catalog binding. Homebase changes the old and new name-registry cells and
records the immutable operation. It does not mutate table, index, or relationship
definitions and does not advance the row write contract.

Rejection performs the same operation in reverse after checking
`TableId -> new_name`. Multiple speculative renames unwind in reverse manifest
order.

### ALTER TABLE RENAME COLUMN

The operation resolves the source spelling once to a stable `ColumnId`, then
moves its catalog binding. Rows, primary keys, UNIQUE ownership, foreign keys,
CHECK dependencies, and index terms continue to refer to that identity. SQLite
DDL is rendered against current bindings. The folded catalog derives a new
content-addressed revision because its encoded IR changed, while the admitted
operation publishes only its old and new name cells plus immutable provenance.
That local revision is a fold fingerprint, not an authority conflict frontier.

### ALTER TABLE ADD COLUMN

SQLite's one-column ADD form is supported for declared types with optional
NOT NULL, DEFAULT, and CHECK constraints. The operation mints a `ColumnId`,
records its predecessor for ordered concurrent folding, and retains before and
after structural definitions for deterministic apply and repair. Historical
rows project a missing value through the admitted default or NULL rules. A
column that changes valid row lowering advances the write contract; compatible
additions use narrower schema and dependency cells.

### ALTER TABLE DROP COLUMN

DROP resolves and retires one `ColumnId`; identities are never reused. The
compiler rejects active primary-key, index, CHECK, relationship, or referenced
parent dependencies that make retirement unsafe. Materialized values are
captured for rejection repair, so a non-final physical column can be restored
at its original logical position. Historical row frames may retain the retired
value because current-schema projection ignores inactive identities.

## Conflict Rules

- Two renames of the same current table conflict through the old name cell.
- Two objects acquiring the same target name conflict through the new name cell.
- DML and name-only rename do not conflict.
- UUID-resolved index or relationship DDL may remain compatible with rename when
  its materialization renderer substitutes current names.
- Structural DDL must advance the appropriate active schema or write-contract
  cells. Rename does neither unless a later feature explicitly gives names row
  semantics.
- Serializable read tracking treats a resolved table UUID as the accessed
  object. Identifier lookup is not an application-data read. Multilite does not
  promise strict real-time namespace consistency to an offline stale device.

## Future Grammar

### DROP TABLE

Drop writes a table tombstone and advances the write contract so stale DML
cannot apply. Rejection requires retained rows/schema or delayed physical
destruction; recreating from CREATE SQL alone is insufficient. Reusing the old
name always mints a new `TableId`.

### Constraint and Relationship Evolution

Definitions remain UUID-owned and immutable. Additions mint identities and
advance the write contract whenever an older writer could omit required cells
or validation. Retirement leaves old namespaces inert until every relevant
device frontier permits garbage collection. Physical SQL is rendered from
current table and column bindings.

### Views, Triggers, and Generated Side Effects

These objects may embed multiple identifier references and executable behavior.
Support requires structured dependency IDs and exact logical capture of every
side effect. Until then, foreign/out-of-band instances remain a quarantine
boundary rather than being rewritten opportunistically.

## Required Regression Matrix

- Rename before and after stale INSERT, UPDATE, and DELETE under both isolation
  levels.
- Reuse the old name for a new `TableId`; stale DML must affect the renamed
  original only.
- CREATE/DROP INDEX and relationship creation on a stale snapshot before and
  after rename admission.
- Multiple renames in one transaction, including rejection after restart.
- Competing same-source and same-target renames.
- Rename with composite keys, WITHOUT ROWID, STRICT, UNIQUE ownership, incoming
  and outgoing foreign keys, and ordinary indexes.
- Pending repair after accepted prefixes, rejected suffixes, restart, and name
  reuse.
- Stock SQLite reopen, `integrity_check`, and `foreign_key_check` after every
  convergence scenario.
