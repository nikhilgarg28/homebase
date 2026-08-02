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
  schema-object name plus owning table root inspected by a PRAGMA. Snapshot
  isolation retains only mandatory write guards.
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
  `DROP COLUMN` retires/asserts the selected column-dependency prefix. CREATE
  asserts every captured table and column name binding. This makes destructive
  DDL conflicts symmetric in either admission order without a shared schema
  head that would serialize unrelated column DDL.
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

Status: pending.

## Dropping named constraints

Status: pending.
