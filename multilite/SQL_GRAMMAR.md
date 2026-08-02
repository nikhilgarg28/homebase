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
FAIL`, `OR ROLLBACK`, and public trigger creation remain outside the managed
surface.

`UPDATE` and `DELETE` accept SQLite's native `ORDER BY`, `LIMIT`, and `OFFSET`
forms, including `LIMIT offset, count`; `ORDER BY` requires `LIMIT`, matching
SQLite. SQLite chooses the affected rows once against the writable branch
snapshot. Multilite replicates the resulting `RowChanges`, not the selection
query, so remote apply and rejection repair never rerun a top-N decision.
Selection expressions are ordinary reads: snapshot isolation retains mandatory
row and constraint guards, while serializable isolation adds conservative
table-root prefixes for every synchronized table SQLite reads, including tables
consulted by scalar subqueries in `ORDER BY`, `LIMIT`, or `OFFSET`. Invalid
runtime bounds roll back before a logical operation, commit sequence, or pending
record is created. `RETURNING` may be combined with these clauses, but its
output order is not promised by SQLite.

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
FROM`, tuple assignments, `INDEXED BY` / `NOT INDEXED`, and native limited-write
selection. These spellings produce the same statement-delta operation as
simpler DML.

`INSERT INTO table AS alias`, `UPDATE table AS alias`, and `DELETE FROM table
AS alias` use SQLite's native alias scoping, including UPSERT, `UPDATE ...
FROM`, subqueries, limited writes, and `RETURNING`. Aliases affect only local
name resolution; captured row changes retain the synchronized table identity
and produce the same wire operations and guards as unaliased statements.
Qualified targets such as `main.table` remain unsupported.

Schema constraints accept atomic `ON CONFLICT ABORT`, `IGNORE`, and `REPLACE`
policies on primary keys, UNIQUE declarations, and NOT NULL declarations.
Absence and explicit `ABORT` normalize to the same durable policy. The policy
is part of the stable schema IR and authenticated schema frame, so later table
rebuilds retain it. SQLite resolves the policy once on the writable branch;
Multilite replicates the resulting net row transition, including replacement
victims, rather than selecting conflicts again on replicas. Statement-level
`INSERT OR ...` / `UPDATE OR ...` keeps SQLite's normal precedence over the
schema policy. `ALTER TABLE ... ADD COLUMN ... NOT NULL ON CONFLICT ...` uses
the same policy model. `FAIL` and `ROLLBACK` remain fenced because their
partial-statement effects do not fit the atomic statement-delta contract;
CHECK conflict clauses remain unsupported because SQLite does not use them as
an enforcement policy.

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

`ALTER TABLE ... ADD COLUMN ... REFERENCES` supports one inline, immediate
foreign key on the new column. The parent must already be synchronized and the
reference must resolve to one complete single-column PRIMARY KEY or UNIQUE
key with matching affinity. Named constraints and all five `ON DELETE` / `ON
UPDATE` actions are retained. Introducing the relationship advances the child
and parent write contracts and explicitly guards both tables' row namespaces,
so stale child writes and stale parent writes conflict under both isolation
levels. This is intentionally conservative: it keeps pre-relationship row
operations from being replayed against a future relationship envelope. Existing
child rows project `NULL` through SQLite's native ADD COLUMN semantics. Self
references, composite targets from a single added column, `MATCH`, and deferred
constraints remain fenced.

`ALTER TABLE table DROP CONSTRAINT name` is a Multilite extension for active,
named table-level `UNIQUE`, `FOREIGN KEY`, and `CHECK` constraints. SQLite has
no native spelling for this operation, so Multilite resolves the name to the
constraint's stable schema identity, retires it in the immutable before/after
schema IR, and rebuilds the physical table internally. The replicated frame is
metadata-only and rejection repair restores the constraint from that same IR;
no row values enter a sidecar or the wire format. Constraint-name cells,
active UNIQUE identities, and exact FK-to-parent relationship markers make
same-constraint changes and FK-target retirement conflict symmetrically. The
table write revision does not advance, so historical row operations carrying
the stricter retired UNIQUE/FK bookkeeping remain valid and can commute with
the drop. A UNIQUE constraint or unique index still cannot be retired while an
active FK targets it; drop the referencing FK first. PRIMARY KEY, NOT NULL,
DEFAULT, unnamed constraints, `ADD CONSTRAINT`, and type changes remain
outside this slice. Retired constraint names remain reserved within the table's
retained schema history.

`PRAGMA [main.]user_version` is supported for reads and signed 32-bit decimal
literal assignments (hex and string forms are rejected). A changing assignment
is one replicated metadata operation with write conflict detection under both
isolation levels and local rejection repair; assigning the current value is a
logical no-op with no fence. Public read-only
schema introspection also supports `table_info`, `table_xinfo`, `index_list`,
`index_info`, `index_xinfo`, and `foreign_key_list` with literal object names.
Serializable updates retain name and owning-table dependencies for those
observations. Other PRAGMAs are explicitly rejected: physical or
connection-local settings are not silently replicated as logical database
state.

`CREATE VIEW name [(columns...)] AS SELECT ...` and `DROP VIEW name` are
supported for views whose complete dependency graph resolves to synchronized
base tables. View operations are metadata-only. CTEs, joins, compounds, and
nested SELECT expressions are walked structurally; each base table's stable
identity and the stable identity plus historical binding of every current
column are authenticated guards. Table-owned and column-owned dependency
markers make concurrent source-table or source-column destruction conflict in
either admission order. Consequently row DML and ADD COLUMN commute with view
DDL, while stale destructive DDL conflicts before replay can create a broken
view. The current DROP COLUMN rebuild requires dependent views to be dropped
first, even when the selected column is not referenced.
The created view is prepared in its statement savepoint so SQLite's otherwise
deferred unknown-column errors become atomic failures. Views over views,
table-valued functions, TEMP/qualified views, and conditional view DDL remain
outside this first slice pending a folded view-identity catalog.

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
