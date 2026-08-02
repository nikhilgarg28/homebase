# Grammar Expansion Review Notes

This file records boundaries, surprises, and consciously deferred work from
the grammar expansion following write `ORDER BY` / `LIMIT`. Atlas remains the
project tracker; this document is the compact review trail for this batch.

## DML target aliases

Status: complete.

- `INSERT`, `UPDATE`, and `DELETE` accept native SQLite target aliases.
- Qualified targets remain fenced because multi-schema identity is not part of
  this batch.
- Aliases remain syntax only. Preupdate capture, logical row operations, guard
  planning, replay, and rejection repair continue to use stable table IDs.
- No unexpected distributed invariant was required. The important hardening
  boundary is alias scope inside UPSERT, `UPDATE ... FROM`, predicates, and
  `RETURNING`; local, conformance, two-replica, both-isolation, and rejection
  tests cover those paths.

## Schema conflict policies

Status: complete.

- PRIMARY KEY, UNIQUE, and NOT NULL retain an explicit ABORT/IGNORE/REPLACE
  policy in the validated schema IR, durable codec, provenance projection, and
  structural materialization SQL. Absence is canonical ABORT.
- SQLite chooses the conflict result on the branch. Replicas apply the captured
  row transition, so no conflict selection is replayed remotely.
- Statement-level conflict algorithms retain SQLite's precedence over schema
  defaults. Replacement victims and ignored rows therefore use the same
  `RowChanges` path already covered for `OR REPLACE` and UPSERT.
- `ADD COLUMN ... NOT NULL ON CONFLICT ...` shares the same IR and survives
  subsequent rebuilds and renames.
- `FAIL` and `ROLLBACK` remain intentionally fenced because they can preserve a
  partial statement before returning an error. CHECK conflict clauses remain
  fenced.
- The schema frame changes in place because Multilite has no compatibility
  deployment yet; there is deliberately no legacy decoder for an unpublished
  frame.

## PRAGMA policy

Status: complete.

- `PRAGMA [main.]user_version` is a replicated application-metadata cell, not
  a device-local setting. Signed 32-bit literal assignments compile to one
  durable operation with exact write guards and rejection repair; same-value
  assignments are true logical no-ops.
- Reads admit only `user_version` and deterministic schema introspection:
  `table_info`, `table_xinfo`, `index_list`, `index_info`, `index_xinfo`, and
  `foreign_key_list`. Everything else fails at the SQL gate instead of later
  surfacing as raw `SQLITE_AUTH`.
- Serializable managed updates record the exact user-version cell or the
  schema-object name plus owning table root inspected by a PRAGMA, including
  through `prepare()` as well as `query()`. Snapshot isolation retains only
  mandatory write guards. Same-value `PRAGMA user_version = N` assignments are
  true logical no-ops with no fence: they neither write nor read the cell.
  Assignments accept signed decimal literals only (not hex).
- Physical, connection-local, or environment-sensitive PRAGMAs such as
  `journal_mode`, `data_version`, `schema_version`, and `page_count` remain
  fenced. Supporting operational configuration belongs on open options rather
  than in replicated SQL.

## Views

Status: complete for synchronized base-table views.

- `CREATE VIEW` and `DROP VIEW` replicate metadata only. Their operation frame
  carries canonical CREATE SQL plus stable IDs and historical names for every
  base table and all of its current columns; no rows or repair sidecar are
  involved.
- CREATE writes table-owned and stable-column-owned dependency markers; DROP
  removes the same markers. `DROP TABLE` retires/asserts the table prefix, while
  `DROP COLUMN` retires/asserts the selected column-dependency prefix and the
  table-wide view-dependency prefix (so any synchronized view on the table
  blocks the drop, matching the local rebuild rule). CREATE and DROP both assert
  every captured table and column name binding so a source rename that admits
  first conflicts with DROP VIEW; rejection repair can then re-execute historical
  CREATE SQL against unchanged names. Drop-then-rename still admits (the view is
  already gone) without a shared schema head that would serialize unrelated
  column DDL.
- The SELECT dependency walker covers CTEs, joins, compounds, scalar/EXISTS/IN
  subqueries, function/window expressions, ordering, limits, and offsets.
  Every dependency contributes mandatory table/column name-binding guards and
  conservative all-column markers, so DML and ADD COLUMN commute while a stale
  DROP COLUMN cannot leave an admitted view unmaterializable.
- CREATE is validated by preparing `SELECT * FROM view LIMIT 0` inside the same
  branch savepoint. SQLite's permissive creation of a view with unknown columns
  therefore becomes an atomic typed failure.
- Views over views, table-valued functions, TEMP/qualified views, and
  `IF [NOT] EXISTS` remain fenced. A folded stable-ID view catalog is required
  before view-of-view dependencies and foreign/pre-existing view provenance
  can be distinguished cleanly; this is the principal follow-up risk from the
  story.
- The current DROP COLUMN materializer rebuilds its table. Any synchronized
  view over that table must therefore be dropped first, even when it does not
  mention the removed column. Stable per-column provenance can later relax
  that conservative local rule together with a view-aware rebuild.

## ADD COLUMN REFERENCES

Status: complete for one inline relationship on the added column.

- Parent table, target key, columns, affinities, names, actions, and optional
  constraint name resolve into the same stable-ID FK IR used by CREATE TABLE.
  Decode authenticates the SQL projection and canonical apply revalidates every
  current parent binding before materializing it.
- The operation advances both child and parent write revisions. Mandatory
  guards cover the child primary-index generation, the parent's complete row
  namespace, parent active schema revision, parent schema-name binding, and each
  referenced parent column name binding. Parent column name cells are also
  touched (same-value Set + Write) so a concurrent parent column rename
  conflicts in either admission order; invariant-only asserts would miss the
  AddColumn-first case. Stale child/parent writes likewise reject under Snapshot
  and Serializable isolation; no historical row operation is replayed against a
  relationship envelope it did not encode.
- Rejection removes both the column and the relationship from physical SQLite,
  name bindings, and the folded catalog. Admission on another device folds the
  stable relationship and preserves it across reopen and later table rebuilds.
- Coverage includes all action plumbing, named constraints, UNIQUE parents,
  STRICT tables, composite-primary-key WITHOUT ROWID children, malformed
  provenance, both admission orders and isolation levels, winning and losing
  DDL repair, and atomic refusal of SQLite-invalid non-NULL defaults.
- Deliberate boundaries: one added column cannot establish a composite FK;
  parent and target must already exist; self references, MATCH, and deferred
  constraints remain fenced. The table-wide parent guard is conservative and
  can be narrowed only together with historical relationship-aware row replay.

## Dropping named constraints

Status: complete for named table-level UNIQUE, FOREIGN KEY, and CHECK.

- `ALTER TABLE table DROP CONSTRAINT name` is intentionally a Multilite SQL
  extension: sqlite3-parser recognizes the spelling, while canonical physical
  apply uses the existing stable-ID table rebuild rather than asking stock
  SQLite to execute unsupported syntax.
- The logical operation carries metadata only: mutation identity, source SQL,
  stable table identity, constraint name, and complete before/after schema IR.
  UNIQUE, FK, and CHECK definitions remain in the IR with `active = false`, so
  historical row operations can still authenticate and replay their stricter
  owner/reference bookkeeping. No repair sidecar is needed because no row
  value is destroyed.
- Named-constraint cells provide the shared SQLite-name fence. UNIQUE
  constraints additionally delete and guard their active identity plus the
  complete incoming relationship-marker prefix. FK retirement deletes and
  guards its exact parent relationship marker. CHECK retirement needs only the
  name binding because it has no external ownership namespace. None advances
  the table write revision; stale DML therefore commutes when its historical
  rules remain semantically safe.
- Under the current coarse serializable read tracer, a stale child write
  compiled before FK retirement rejects when the retirement admits first, but
  the reverse order admits both: the write's old-schema table read is real in
  that serialization order, while the metadata-only drop does not semantically
  read row values. Snapshot isolation admits both orders.
- A UNIQUE target with an active incoming FK is refused before branch mutation.
  The FK must be retired first. Pair coverage verifies the converse race: a
  concurrent FK addition and UNIQUE retirement conflict in either admission
  order through the relationship marker, not through local SQLite replay.
- Coverage includes codec round trips and malformed active flags, guard audit,
  named UNIQUE/CHECK/FK behavior, quoted case-insensitive names, duplicate
  names, parameter refusal, STRICT and composite-PK WITHOUT ROWID schemas,
  managed-update rollback, reopen, rejection repair, both isolation levels,
  both admission orders, and converged schema/row state.
- Deliberate boundaries: PRIMARY KEY, NOT NULL, DEFAULT, and unnamed constraints
  cannot be dropped; `ADD CONSTRAINT` and type changes remain unsupported.
  CHECK constraints currently use their canonical name as retirement identity,
  so all retired constraint names remain permanently reserved within the
  table's retained IR. DROP COLUMN therefore refuses columns that still own a
  retired CHECK (active column-owned CHECKs continue to drop with the column).
  A future CheckId can relax that boundary. Retired definitions, active-state
  tombstones, and UNIQUE-owner cells need the general frontier-aware schema GC
  protocol. FK addition still rewrites the parent/child write revisions, so
  independent relationship additions are conservatively serialized; relaxing
  that requires a monotonic per-contract registry on both sides, not an ad hoc
  exception in this operation.
