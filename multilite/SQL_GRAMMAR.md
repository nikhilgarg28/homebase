# Multilite SQL Grammar

Multilite's SQL grammar is intentionally frozen while the logical-operation
compiler and verifier are consolidated. The current executable families are
restricted `CREATE TABLE`, `ALTER TABLE` rename/add/drop forms,
`CREATE [UNIQUE] INDEX`, `DROP INDEX`, `SELECT`, `INSERT`, `DELETE`, and
`UPDATE`. Exact accepted forms are defined by tests and the parser; accepting a
SQLite statement does not imply that every SQLite spelling is supported.

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
5. Snapshot-isolation writes and mandatory constraints are explicit; any
   serializable read contribution is conservative and complete.
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
