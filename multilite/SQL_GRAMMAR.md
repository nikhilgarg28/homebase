# Multilite SQL Grammar

Multilite's SQL grammar grows only through the completion gate below. The
logical-operation compiler, verifier, and guard registry are now the stable
path for admitting compatible syntax. The current executable families are
restricted `CREATE TABLE`, `ALTER TABLE` rename/add/drop forms,
`DROP TABLE`, `CREATE [UNIQUE] INDEX`, `DROP INDEX`, `SELECT`, `INSERT`, `DELETE`, and
`UPDATE`. Exact accepted forms are defined by tests and the parser; accepting a
SQLite statement does not imply that every SQLite spelling is supported.

Captured INSERT, UPDATE, and DELETE syntax compiles through one statement-delta
operation that folds repeated touches into deterministic before/after row
images. `INSERT [OR ABORT|OR IGNORE|OR REPLACE]`, `REPLACE INTO`,
`UPDATE [OR ABORT|OR IGNORE|OR REPLACE]`, and UPSERT chains containing
`DO NOTHING` and/or `DO UPDATE` are supported. UPSERT and replacement store
SQLite's complete captured net row transition rather than replaying conflict
selection on replicas. Replacement includes every implicitly deleted conflict
victim. `ON DELETE CASCADE`, `SET NULL`, and `SET DEFAULT` capture their
complete multi-table transition; the same five actions are supported for `ON
UPDATE`, and `RESTRICT` preserves SQLite's immediate statement behavior. `OR
FAIL`, `OR ROLLBACK`, public trigger creation, and write `ORDER BY` / `LIMIT`
remain outside the managed surface.

`INSERT`, `UPDATE`, and `DELETE` accept SQLite `RETURNING` result columns.
Statement validation classifies access (`read` or `write`) independently from
output (`rows` or change count): `query` and prepared `query_map` route a
returning DML statement through a writable capture branch, while `execute`
returns SQLite's `ExecuteReturnedResults` error. Returned rows are transient
API output and never enter the logical operation, pending frame, or replicated
wire format. A mapper or statement error rolls back the complete statement.
Mapping necessarily runs on the speculative branch; mapper side effects are
therefore not transactional. Under `Remote` policy, the outer `query` or
`update` call returns mapped values only after authority accepts the write.
`ViewTransaction` remains read-only, while `UpdateTransaction::query` can mix
reads and returning writes in one managed transaction.

Serializable tracing remains deliberately conservative. SQLite's authorizer
reports target columns projected directly by `RETURNING` as reads, so the
current table-root tracer may make a serializable returning write conflict
with another write to the same table even when their row keys are disjoint.
Snapshot isolation is unaffected. Reads performed by subqueries inside a
`RETURNING` expression are genuine dependencies and are always retained;
future predicate tracing may distinguish those from output-only projections.

Captured UPDATE and DELETE syntax also includes CTE prefixes, `UPDATE ...
FROM`, tuple assignments, and `INDEXED BY` / `NOT INDEXED`. These spellings
produce the same statement-delta operation as simpler DML.

Each statement may capture at most 100,000 direct row events and 64 MiB of row
images. The normalized row operation and enclosing transaction frame enforce
the same 64 MiB durable boundary on encode and decode. Crossing a limit rolls
back the complete SQLite statement with a typed error. For `RETURNING`, capture
poisoning is checked before invoking the row mapper, so application mapping
stops as soon as SQLite reports the bounded failure.

`DROP COLUMN` and `DROP TABLE` replicate metadata only. Their originating
replica streams dropped values or complete row images into a local repair
sidecar, currently bounded at 100,000 rows and 64 MiB. `DROP TABLE` supports
declared composite primary keys and restores explicit indexes, but rejects a
table still targeted by a synchronized foreign key. Crossing either limit
refuses the statement before schema, pending-journal, or sidecar state becomes
durable.

`CREATE TABLE IF NOT EXISTS`, `CREATE INDEX IF NOT EXISTS`, `DROP INDEX IF
EXISTS`, and `DROP TABLE IF EXISTS` are supported. If SQLite and the catalog
prove the requested disposition is already true in the branch snapshot, the
statement contributes no logical operation and does not advance local commit
or submit state. Its schema-name observation still joins a larger serializable
transaction's read footprint. Otherwise it lowers to the same operation,
codec, and guards as the clause-free spelling. Cross-kind names retain
SQLite's shared-namespace behavior.

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
