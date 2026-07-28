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
   the latest immutable structural revision and may retain historical identifier
   spellings from the DDL that created each object.
4. SQLite execution uses freshly rendered SQL. Rendering may substitute current
   names for UUID-resolved objects, but may not change expressions, ordering,
   conflict behavior, or any other semantic property of the authenticated
   operation.
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

The immutable index definition records its owner `TableId`, index UUID,
structured terms/predicate, and original SQL. CREATE and DROP apply by index
identity/name, while CREATE and DROP rejection render a CREATE statement whose
target is the owner's current catalog name. Existing index definitions are not
rewritten when their table is renamed.

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

### Column Rename

Column names can follow the same binding rule only after row apply, index
rendering, CHECK/default/generated-expression handling, and foreign-key DDL all
resolve `ColumnId` to current names. A column rename must not change storage or
key images, but expressions that preserve identifier tokens require a structured
AST renderer rather than string substitution.

### ADD COLUMN

Adding a nullable column or a column with a deterministic captured default may
be compatible with older row frames if canonical apply projects a missing
`ColumnId` according to the admitted schema operation. NOT NULL, generated
columns, CHECK constraints, UNIQUE participation, and relationship participation
can require write-contract advancement and backfill.

### DROP COLUMN

Dropped column identities must remain retired rather than reused. Old row frames
may carry harmless extra values only if apply can prove that no active key,
constraint, generated expression, or relationship consumes the column.
Otherwise the write contract rejects them. Physical DROP needs a rejection
strategy based on a shadow table or complete logical restoration, not inverse
SQL alone.

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
