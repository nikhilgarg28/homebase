# Multilite SQL Grammar

Multilite's SQL grammar grows only through the completion gate below. The
logical-operation compiler, verifier, and guard registry are now the stable
path for admitting compatible syntax. The current executable families are
restricted `CREATE TABLE`, `ALTER TABLE` rename/add/drop forms,
`CREATE [UNIQUE] INDEX`, `DROP INDEX`, `SELECT`, `INSERT`, `DELETE`, and
`UPDATE`. Exact accepted forms are defined by tests and the parser; accepting a
SQLite statement does not imply that every SQLite spelling is supported.

Captured INSERT, UPDATE, and DELETE syntax compiles through one statement-delta
operation that folds repeated touches into deterministic before/after row
images. `INSERT [OR ABORT|OR IGNORE]`, `UPDATE [OR ABORT|OR IGNORE]`, and UPSERT
chains containing `DO NOTHING` and/or `DO UPDATE` are supported. UPSERT stores
SQLite's captured net row transition rather than replaying conflict selection
on replicas. `REPLACE`, `OR FAIL`, `OR ROLLBACK`, triggers, mutating foreign-key
actions, `RETURNING`, and write `ORDER BY` / `LIMIT` remain outside the managed
surface.

Captured UPDATE and DELETE syntax also includes CTE prefixes, `UPDATE ...
FROM`, tuple assignments, and `INDEXED BY` / `NOT INDEXED`. These spellings
produce the same statement-delta operation as simpler DML.

Each statement may capture at most 100,000 direct row events and 64 MiB of row
images. The normalized row operation and enclosing transaction frame enforce
the same 64 MiB durable boundary on encode and decode. Crossing a limit rolls
back the complete SQLite statement with a typed error.

## Completion Gate

A new statement family or syntax extension is incomplete until all of these are
true:

1. SQL parses and resolves to a validated logical operation with no unresolved
   identifiers or unchecked semantic strings.
2. Its durable operation and transaction codecs round-trip and reject malformed
   frames without panicking.
3. Stored SQL provenance projects to the same resolved operation.
4. Homebase lowering is deterministic and admitted batches are authenticated by
   decoding and exact re-lowering.
5. Every mutation, mandatory invariant, write guard, serializable read, and
   rejection inverse is declared in the checked [`GUARDS.md`](./GUARDS.md)
   contract; any serializable read contribution is conservative and complete.
6. Local branch execution and canonical logical application produce the same
   touched rows and catalog state.
7. Pending rejection effects restore the before-state atomically.
8. Local, remote, rejection, and two-replica convergence/conflict tests cover
   the operation.
9. Any unsupported boundary fails before leaving durable SQLite, submit-log, or
   pending-journal state.

Serializable read precision is an optimization after correctness. A statement
may conservatively assert a stable table root, but it may never omit a logical
dependency merely because precise predicate tracing is unavailable.
