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

Status: pending.

## Views

Status: pending.

## ADD COLUMN REFERENCES

Status: pending.

## Dropping named constraints

Status: pending.
