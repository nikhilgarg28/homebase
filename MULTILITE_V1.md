# multilite V1: append-only collections

Working design note. This is the deliberately small first executable shape for
multilite: enough real SQLite, local metadata, optimistic sync, and repair
machinery to prove the loop, without taking on general SQLite replication yet.

## V1 shape

V1 is a normal SQLite file with one built-in synced table shape, the existing
homebase kernel and client, and optimistic append-only sync. There is no user
DDL. multilite owns one logical table:

```sql
CREATE TABLE items (
  collection TEXT NOT NULL,
  id         BLOB NOT NULL,
  payload    BLOB NOT NULL,
  PRIMARY KEY (collection, id)
);
```

Applications treat `collection` as the table or namespace name and store opaque
application data in `payload` (JSON, msgpack, protobuf, etc.). The only
supported write is `INSERT` of a new `(collection, id, payload)` row. Reads are
ordinary local SQLite `SELECT`s through a rusqlite-like API. There are no
updates, deletes, transactions, secondary indexes, foreign keys, triggers,
migrations, encryption, partial replication, leases, device fencing, or user
schema management.

All local inserts are accepted optimistically and appended to homebase's local
submit log as unchecked submits. Each insert internally carries an exact-key
range assertion at the SQLite file's applied admission cut. On push, homebase
orders accepted inserts and rejects a stale exact-key assertion when another
device has already won the same `(collection, id)`, in addition to its normal
device stream and fork checks.

`push()` never repairs or discards local state. A definitive rejection is
returned to the application. The application may then explicitly call repair,
whose only V1 resolution is to roll back the complete active local suffix and
restore the SQLite file to admitted server state. Ambiguous or unavailable
pushes remain retryable and are never repairable.

V1 proves:

- a rusqlite-like public API can sit in front of SQLite;
- the SQLite file remains readable by stock SQLite;
- local rows and local sync metadata commit atomically in one file;
- supported inserts are captured into the durable homebase submit log;
- unpushed work survives restart;
- push, pull, apply, idempotent retry, conflict rejection, and repair converge.

V1 does not need UUID schema tags, column-tagged row frames, public range
asserts, vtable machinery, or SQL DDL interception yet. Exact-key range asserts
are an internal OCC mechanism, not part of the Multilite API. V1 still uses
versioned logical records and future-compatible names where cheap.

## Layering and trust boundary

multilite does not define a second oplog, cursor model, checksum, rollback
protocol, or server admission path. It uses `homebase-client` for submit, push,
pull, admit-log progress, device checksums, versions, and rollback, and uses the
`homebase` server unchanged.

Multilite does need one narrow addition to `homebase-client`: a read-only view
of the active submit window decoded back to plaintext mutations. Apply uses it
to distinguish an own already-materialized admission from a foreign collision;
repair uses it to enumerate the suffix it will roll back. This is a view over
the existing `MetaStore`, not another durable log or cursor.

The append-only guarantee is enforced among conforming Multilite clients. An
authorized raw homebase client can bypass Multilite's exact-key assertion and
issue an ordinary `Set`; server-side space policies against such clients are
post-V1 work.

One Multilite file is exactly one homebase space. Creating a file mints its
space id and device id. Reopening preserves both. Joining another replica uses
a small database descriptor containing the plaintext space id and mints a new
device id for the new file. V1 uses a plaintext `SpaceEnvelope`; encryption and
credential delivery remain post-V1.

## Item wire identity

SQLite permits empty and arbitrarily large `collection` and `id` values, while
homebase keys have bounded, non-empty components. V1 therefore does not embed
either field directly in key components. It encodes a versioned,
length-delimited `(collection UTF-8 bytes, id bytes)` frame and hashes it with a
domain-separated SHA-256 digest. The homebase key is a fixed namespace plus
that digest, and item keys are terminal: no other protocol key may be nested
under one.

The `Set` value is a separate versioned `ItemInsert` frame containing the
original collection, id, and payload. Pull and repair decode this frame before
touching SQLite. The full hashed key is also the prefix of the internal range
assert. Cryptographic hash collision resistance is part of the V1 key-identity
assumption.

If admit `neck = N`, SQLite has applied every server admission below `N`. A
locally absent item is submitted with `RangeAssert { prefix: item_key, upto:
N - 1 }` through `submit_unchecked`. A foreign winner admitted after that cut
makes the assertion fail. A winner admitted before the cut would already have
been present in SQLite, assuming the file invariants hold.

At every committed boundary, `items` is the materialization of the applied
admit prefix plus the active speculative local inserts. Applying an admitted
batch and moving admit `neck` are atomic; creating a speculative row and its
homebase submission are atomic.

## Local metadata table

All multilite-owned tables in the SQLite file use the `_mt_meta_` prefix. The
prefix is reserved; user SQL may not read, create, alter, or write these tables
through the public multilite connection.

V1 uses the complete existing homebase `MetaStore` state rather than a reduced
parallel schema. The first implementation is an SQLite-backed `OrderedStore`
over one table:

```sql
CREATE TABLE _mt_meta_kv (
  key   BLOB PRIMARY KEY,
  value BLOB NOT NULL
) WITHOUT ROWID;
```

`SqliteOrderedStore` snapshots scans into owned rows and applies every
`WriteBatch` in a savepoint. `OrderedMetaStore<SqliteOrderedStore>` remains the
sole homebase durable truth and retains the full submit/admit cursors, logs,
device checksum, version high-water, rollback markers, device identity, and
codec record. It must pass the existing `OrderedStore` and `MetaStore`
conformance suites.

Store methods must be savepoint-joining: inside an ambient statement or repair
transaction they participate in that atomic unit; outside one they still run
as their own atomic SQLite step. Native relational metadata tables may replace
the KV implementation later behind `MetaStore`; V1 does not duplicate its
state into convenience tables.

## V1 verification target

When V1 is complete, these tests should pass:

- Create a fresh multilite DB, insert rows into multiple collections, and verify
  local `SELECT`s return normal SQLite results.
- Push inserts from device A, pull/apply on device B, and verify both SQLite
  files return identical rows.
- Let devices A and B insert different ids into the same collection
  concurrently; verify both pushes succeed and both replicas converge.
- Let devices A and B insert the same `(collection, id)` concurrently; verify
  one push succeeds, the other rejects without changing local state, and the
  losing device repairs only after an explicit call that rolls back its active
  suffix.
- Let device B pull device A's winning duplicate before B pushes; verify pull
  captures the admission but apply stops before the conflict until B explicitly
  pushes and repairs.
- Restart a client with unpushed local inserts and verify the local submit log
  resumes correctly.
- Simulate lost acknowledgements and verify re-push is idempotent.
- Kill/restart during local insert capture, push, pull, apply, and repair;
  verify there is no cursor-ahead-of-rows, row-without-log, or log-without-row.
- Verify unavailable and ambiguous pushes never enable rollback or remove
  speculative rows.
- Attempt unsupported SQL (`UPDATE`, `DELETE`, DDL, explicit transactions,
  `CREATE INDEX`, write PRAGMAs, `ATTACH`) and verify clean errors with no
  mutation.
- Open the file with stock SQLite and verify `items` is readable and
  `PRAGMA integrity_check` passes.

## Work batches

Each batch should be independently testable and commit-ready.

### Batch 1: V1 contract

Land this V1 contract as the executable scope: fixed `items`, one file per
homebase space, internal exact-key assertions, explicit rollback repair,
supported and rejected SQL, metadata prefix, and extensions.

Tests: documentation acceptance checklist only.

### Batch 2: public connection API

Create a `multilite` connection wrapper shaped close to rusqlite:

```rust
let db = MultiliteConnection::open(path)?;
db.execute(sql, params)?;
let mut stmt = db.prepare(sql)?;
let rows = stmt.query_map(params, |row| { ... })?;
```

Initially it delegates to rusqlite with no sync behavior. `prepare()` accepts
read-only statements only; writes enter through `execute()` so no prepared
statement path can bypass capture. The wrapper does not expose the raw
rusqlite connection or statement. It may eagerly collect `query_map` results in
V1 if shared SQLite ownership makes a borrowed lazy iterator unsound.

Tests:

- open a file through the wrapper;
- execute and query through the wrapper;
- prepare and `query_map`;
- reject a write passed to `prepare`;
- bind parameters;
- convert SQLite errors into multilite errors without losing useful detail.

### Batch 3: adopt the rusqlite type surface

Use rusqlite's value and conversion ecosystem rather than inventing a competing
public value system:

```rust
pub use rusqlite::{params, Params, ToSql};
pub use rusqlite::types::{FromSql, Type, Value, ValueRef};
```

Define internal helpers for copying and validating V1 values:

```rust
fn owned_value(v: ValueRef<'_>) -> Result<Value>;
fn require_text(v: &Value) -> Result<&str>;
fn require_blob(v: &Value) -> Result<&[u8]>;
```

Define the V1 logical structs:

```rust
struct ItemKey {
  collection: String,
  id: Vec<u8>,
}

struct ItemInsert {
  key: ItemKey,
  payload: Vec<u8>,
}
```

Define versioned canonical codecs for `ItemKey` and `ItemInsert`. Derive the
fixed homebase item key by hashing the canonical `ItemKey` frame with a domain
separator. Decoding always verifies the frame version, lengths, and trailing
bytes.

Tests:

- `ValueRef -> Value` copying preserves null, integer, real, text, and blob for
  supported cases;
- V1 extraction accepts `collection TEXT`, `id BLOB`, and `payload BLOB`;
- wrong storage classes are rejected for V1 inserts;
- empty and large collection/id values map to valid fixed-size homebase keys;
- distinct logical keys have distinct canonical frames and pinned digest
  vectors;
- `ItemInsert` round-trips and rejects malformed or unknown-version frames;
- the public API still accepts normal rusqlite params and `FromSql`
  conversions.

### Batch 4: SQLite runtime hooks spike

Prove the low-level SQLite machinery V1 needs:

- authorizer callback;
- preupdate hook or update hook path;
- savepoints;
- nested connection mode distinguishing public capture, internal metadata,
  remote apply, and repair.

Tests:

- authorizer denies a selected unsupported statement;
- hook observes an `items` insert and captures values as rusqlite `Value`s;
- savepoint rollback removes the row;
- internal metadata, remote apply, and repair are not captured or rejected by
  the public authorizer.

If preupdate support is unavailable in the selected SQLite build, explicit
wrapper logging can be the V1 fallback, but the spike should make that choice
explicit.

### Batch 5: SQLite ordered store and homebase metadata

Implement `SqliteOrderedStore` over `_mt_meta_kv` on the same SQLite connection
used for `items`, then compose it with the existing `OrderedMetaStore`. Reads
and writes execute synchronously while holding the connection owner; returned
futures are ready, and scans own a snapshot rather than retaining a SQLite
statement or lock.

Every `WriteBatch` uses a savepoint so it composes with an ambient statement or
repair transaction. Do not add a second Multilite oplog, pushed bit, pull
cursor, or admit log.

Tests:

- the existing `OrderedStore` conformance suite passes;
- the existing complete `MetaStore` conformance suite passes;
- metadata writes join an outer savepoint/transaction;
- rollback of the outer unit removes all metadata mutations;
- reopen loads and certifies the complete homebase client state;
- injected SQLite errors leave no partial `WriteBatch`.

### Batch 6: file bootstrap schema

Define the one-file/one-space lifecycle and initialize `items` plus
`_mt_meta_kv`:

- `create` mints a plaintext space descriptor and a device identity;
- `join` creates another file for an existing descriptor and mints a distinct
  device identity;
- `open` reopens an initialized file and rejects a conflicting descriptor;
- each connection is wired to the existing homebase client and server handle.

Bootstrap is one SQLite transaction and is idempotent after success.

Tests:

- fresh open creates all required tables;
- create, reopen, and join preserve the space/device identity rules;
- reopen is a schema no-op;
- the metadata store works after bootstrap;
- stock SQLite reads `items`;
- `PRAGMA integrity_check` passes.

### Batch 7: SQL surface gate

Allow only:

- `SELECT`;
- `INSERT INTO items`.

Reject:

- `UPDATE`;
- `DELETE`;
- DDL;
- explicit `BEGIN`/`COMMIT`/`ROLLBACK`;
- write PRAGMAs;
- `ATTACH`;
- writes to `_mt_meta_` tables through the public connection.

Prepared statements are read-only. Public execution also rejects conflict
clauses (`OR REPLACE`, `OR IGNORE`, and UPSERT), `RETURNING`, multi-row insert,
`INSERT ... SELECT`, multiple statements, TEMP writes, `VACUUM`, `ANALYZE`,
and `REINDEX`. Internal metadata/apply/repair statements run under the internal
connection mode and remain allowed.

Use SQLite authorizer first. Add minimal wrapper classification only where the
authorizer cannot express a V1 rule clearly.

Tests:

- accepted `SELECT` and `INSERT` work;
- unsupported SQL matrix returns clean errors;
- metadata cannot be read or written through the public API;
- rejected statements leave no mutation behind.

### Batch 8: capture INSERT to homebase submit log

Wrap each accepted insert:

```text
SAVEPOINT multilite_stmt
execute INSERT
hook captures exactly one items insert
read admit neck N and verify the captured key is locally new
encode ItemInsert and derive its hashed homebase key
homebase submit_unchecked(Set, exact-key assert upto N - 1)
RELEASE
```

The homebase `MetaStore::commit` joins the statement savepoint through
`SqliteOrderedStore`. If capture validation, encoding, reservation, or commit
fails, roll back the row and metadata together. No network work occurs inside
the statement savepoint.

Tests:

- one insert creates one row and one homebase submit-log entry atomically;
- the entry is a versioned `ItemInsert` Set with an exact-key assertion at the
  applied admit cut;
- duplicate primary key writes no submission;
- multi-row insert is rejected and rolled back;
- reopen sees the unpushed submit log;
- injected failures cannot produce row-without-log or log-without-row.

### Batch 9: homebase admission integration

Use the existing homebase server unchanged. Verify Multilite's append-only OCC
mapping end to end:

- the hashed item key is the Set key and exact range-assert prefix;
- disjoint exact-key assertions do not conflict;
- a foreign winner after the submitter's applied cut fails the stale
  assertion;
- Homebase device sequence and checksum retry remain the sole idempotency
  mechanism.

Add the narrow `homebase-client` active-submission view needed by Multilite.
It returns the current active window in device-sequence order, preserves commit
versus rollback-marker shape, and decodes commit entries through the space
cipher without moving any cursor.

Tests:

- disjoint inserts admit;
- concurrent duplicate logical keys reject one conforming Multilite client;
- retry of the same device sequence is idempotent;
- admission order is stable;
- the active-submission view returns plaintext commits and rollback markers
  without changing durable state;
- a raw assertion-free Homebase Set can bypass the append-only convention,
  documenting the V1 trust boundary.

### Batch 10: push

Delegate to the existing homebase push path. Successful acknowledgement trims
the admitted submit-log prefix and advances its checksum. The expected
exact-key `RangeAssertFailed` at the stalled active head is wrapped as a
`PushRejection`; it does not mutate SQLite rows or retire the rejected suffix.
Other kernel stalls remain non-repairable errors. Unavailable or ambiguous
results remain ordinary retryable errors and never produce a repairable
rejection. `PushRejection` binds the active `head` and exclusive `tail` it
observed so later local submissions make the handle stale rather than silently
expanding what repair will discard.

Tests:

- empty push is a no-op;
- successful push trims entries through the acknowledged prefix;
- lost acknowledgement followed by retry is safe;
- an exact-key rejection reports the first failed local sequence and leaves
  every active row and submission unchanged;
- unrelated kernel stalls cannot be converted into a `PushRejection`;
- dropping a `PushRejection` has no side effect;
- submitting after rejection makes that rejection handle stale;
- unavailable and ambiguous outcomes cannot be passed to repair.

### Batch 11: pull and apply

Use homebase `pull` to append complete dense batches to the durable admit log,
then apply plaintext `ItemInsert` records from admit `neck` in admission and
operation order under internal capture suppression. Each applied batch and
the corresponding `mark_applied` transition commit in the same SQLite
transaction.

An existing row is idempotent only when the admitted entry is this file's own
already-materialized `(device, device_seq)` submission, as in a lost-ack path.
If a foreign admitted item collides with any active local submission, apply
stops before that admission, leaves admit `neck` unchanged, and reports that
push/explicit repair is required. Admit `tail` may continue to capture later
batches without claiming they are applied.

Tests:

- device B pulls device A's rows;
- repeated pull is idempotent;
- crash during apply resumes safely;
- apply does not echo into the homebase submit log;
- pulling an own admitted row after a lost acknowledgement is idempotent;
- pulling a foreign winner over a speculative local duplicate blocks before
  advancing admit neck;
- disjoint foreign rows still apply while local submissions are pending.

### Batch 12: explicit rejected-tail rollback repair

`push()` itself never repairs. Given a current definitive `PushRejection`, the
application explicitly calls `repair(rejection)`. V1 offers no keep/rebase or
selective retry strategy: repair rolls back the complete active submit-log
window and restores `items` to admitted state.

Repair first validates that the rejection still identifies the active head and
pulls all currently available admitted history. It decodes the active suffix
for the return value. Then one SQLite transaction:

```text
delete every speculative item represented by the active suffix
apply unapplied admitted batches in exact order
homebase rollback the active submit-log window
advance admit neck through the applied batches
commit
```

The returned repair outcome contains the rolled-back `ItemInsert` values for
application inspection or later manual resubmission, but rollback is the only
V1 state transition. If validation no longer matches, repair refuses and the
caller must push again. A crash before the transaction leaves the rejection
retryable; a crash during it exposes either the entire old state or the entire
repaired state.

For append-only V1:

- if the server has a winner for the key, keep/apply the winner;
- otherwise delete the local speculative row;
- retire the active homebase suffix with its normal rollback marker and cursor
  transition.

Tests:

- two devices insert the same key; one wins and the loser repairs;
- entries after the rejected sequence are also discarded;
- push rejection alone leaves all rows and metadata unchanged;
- repair returns every rolled-back logical insert in device-sequence order;
- pull-before-push collision becomes applicable after explicit repair;
- a stale or mismatched rejection handle cannot roll back newer work;
- unavailable/ambiguous outcomes cannot trigger repair;
- after a crash before repair, re-push produces a fresh rejection and repair
  converges; after a committed repair, the old handle is stale and harmless.

### Batch 13: end-to-end fault and fidelity matrix

Run the full V1 scenario suite over two or more clients.

Tests:

- concurrent disjoint inserts converge;
- concurrent duplicate inserts converge after loser repair;
- restart between insert, push, pull, apply, and repair phases;
- compare SQLite query output across replicas;
- unsupported SQL matrix remains stable;
- stock SQLite can inspect the file.

## Post-V1 extensions

Each extension should relax one V1 constraint or add one clear user-facing
capability.

1. **User-created collections/tables.** Let apps create named logical tables
   instead of using only `items.collection`.
2. **General `CREATE TABLE`.** Support ordinary SQLite rowid tables.
3. **UUID schema tags and name-to-tag catalogs.** Decouple stable storage
   identity from SQL names.
4. **Column-tagged row frames.** Make row values robust to rename/add/drop
   column evolution.
5. **Multi-column scalar row values.** Sync normal SQLite columns rather than
   one opaque payload.
6. **Additional primary key shapes.** Add text, integer, rowid/IPK, and
   composite key support.
7. **Transactions and atomic multi-row batches.** Let several writes sync as
   one atomic admission batch.
8. **`UPDATE`.** Capture and ship after-images for changing rows.
9. **`DELETE` and tombstones.** Remove rows and preserve delete history.
10. **Secondary indexes.** Maintain derived index keys and local/index sync
    metadata.
11. **Unique constraints and witness keys.** Enforce uniqueness across devices.
12. **Foreign keys.** Add dependency tracking and parent/child validation.
13. **CHECK, NOT NULL, defaults, generated columns.** Expand SQLite constraint
    fidelity.
14. **ALTER TABLE migrations.** Support additive and rename schema evolution.
15. **DROP TABLE / DROP COLUMN.** Use range delete, schema-history
    materialization, or row rewrite.
16. **Named/idempotent migrations.** Let app startup migrations run safely on
    multiple devices.
17. **Existing SQLite adoption.** Turn supported vanilla SQLite files into
    multilite files.
18. **Large database snapshotting.** Avoid doubling large files during
    adoption.
19. **Encryption.** Encrypt keys, schema records, and values so the server sees
    only opaque data.
20. **Formal leases.** Add explicit lease acquisition/renewal semantics.
21. **Device fencing / single-active-writer lifecycle.** Add WhatsApp-like
    takeover for one active writer at a time.
22. **Coarse range asserts.** Validate broad data/schema prefixes at admission.
23. **Table-level multi-writer mode.** Allow concurrent writers on independent
    tables.
24. **Row/prefix optimistic multi-writer mode.** Allow concurrent writers on
    disjoint rows or prefixes.
25. **Precise read/predicate capture.** Capture reads and write predicates for
    finer serializability.
26. **Rollback repair for rejected speculative tails.** Generalize V1 repair
    beyond append-only inserts.
27. **Partial replication / shapes.** Sync only selected collections, tables, or
    query shapes.
28. **Online index builds.** Add `Building -> Ready` index state and resumable
    backfill.
29. **Views.** Support schema-only views and eventually dependency-aware view
    writes where applicable.
30. **Triggers.** Capture trigger side effects and assert trigger dependencies.
31. **Expression and partial indexes.** Add deterministic expression/predicate
    evaluation.
32. **Virtual table / FTS support.** Add module-specific support for derived or
    local-only virtual tables.
33. **Foreign-write detection and recovery.** Detect plain SQLite writes after
    adoption and provide repair/re-adoption flows.
34. **Sync-commit durability mode.** Let selected commits wait for remote
    admission before returning.
35. **Fork detection / anchors / per-prefix hashes.** Strengthen history and
    anti-entropy verification.
36. **SQLite compatibility harness.** Run sqllogictest or SQLite's test suite
    through passthrough and V1 modes with an expected-failure manifest, then
    ratchet exclusions down as extensions land.
