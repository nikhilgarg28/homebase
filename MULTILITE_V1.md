# multilite V1: append-only collections

Working design note. This is the deliberately small first executable shape for
multilite: enough real SQLite, local metadata, optimistic sync, and repair
machinery to prove the loop, without taking on general SQLite replication yet.

V1 is a pre-release implementation boundary, not a compatibility promise.
There are no released Multilite files and no external users; its schema and
encodings may be replaced freely before the first supported release. Code tied
to this design lives under `multilite/src/v1/` so it can evolve or disappear
without muddying the generic SQLite-facing machinery.

## V1 shape

V1 is a normal SQLite file with one built-in synced table shape, the existing
homebase kernel and client, and optimistic append-only sync. The general
database layer now accepts a deliberately restricted `CREATE TABLE`, and its
operation layer defines the Homebase representation; the temporary V1 row-sync
layer still owns one logical table:

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
migrations, encryption, partial replication, leases, device fencing, or schema
evolution beyond initial table creation.

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

Schema mutations and their table/column identities now use UUIDs, while row
frames remain the next layer. Range assertions are still internal OCC
machinery rather than part of the Multilite SQL API.

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

One Multilite file is exactly one homebase space. Opening a missing or empty
file mints its space id and device id; reopening preserves both. Another
replica opens a fresh file with an opaque, versioned `ReplicaInvitation` and
mints a new device id. V1 invitations carry the plaintext database id and use a
plaintext `SpaceEnvelope`; encryption and credential delivery remain post-V1.

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

All multilite-owned tables in the SQLite file use the `__multilite__` prefix,
which is reserved from public SQL. General Homebase state uses
`__multilite__meta`; temporary V1 format state uses
`__multilite__v1_schema`. The metadata adapter additionally reserves every
table name beginning with `__multilite__meta` and rejects unknown entries in
that subnamespace. User SQL may not read, create, alter, or write any
`__multilite__` table through the public connection.

V1 uses the complete existing homebase `MetaStore` state rather than a reduced
parallel schema. The first implementation is an SQLite-backed `OrderedStore`
over one table:

```sql
CREATE TABLE __multilite__meta (
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

### Batch 3: SQLite type and value machinery

Use rusqlite's value and conversion ecosystem rather than inventing a competing
public value system:

```rust
pub use rusqlite::{params, Params, ToSql};
pub use rusqlite::types::{FromSql, Type, Value, ValueRef};
```

Define generic internal helpers for copying owned SQLite values and requiring a
particular SQLite storage class:

```rust
fn owned_value(v: ValueRef<'_>) -> Result<Value>;
fn require_text(v: &Value) -> Result<&str>;
fn require_blob(v: &Value) -> Result<&[u8]>;
```

These helpers know about SQLite values and Multilite errors, but not about the
V1 `items` schema, collections, item identities, or homebase keys.

Tests:

- `ValueRef -> Value` copying preserves null, integer, real, text, and blob;
- text and blob validation accepts the matching storage class and rejects all
  other storage classes with useful expected/actual type details;
- the public API accepts normal rusqlite params and `FromSql` conversions;
- rusqlite conversion errors retain their original detail.

### Batch 4: V1 item identity and codec

Build the V1 item model on top of the generic SQLite value machinery.

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

- V1 extraction accepts `collection TEXT`, `id BLOB`, and `payload BLOB`;
- wrong storage classes are rejected for V1 inserts;
- empty and large collection/id values map to valid fixed-size homebase keys;
- distinct logical keys have distinct canonical frames and pinned digest
  vectors;
- `ItemInsert` round-trips and rejects malformed or unknown-version frames.

### Batch 5: SQLite runtime hooks spike

Prove the low-level SQLite machinery V1 needs:

- authorizer callback;
- preupdate hook or update hook path;
- savepoints;
- nested connection mode distinguishing public capture, internal metadata,
  remote apply, and repair.

Tests:

- database authorizer denies a selected unsupported statement;
- hook observes an `items` insert and captures values as rusqlite `Value`s;
- savepoint rollback removes the row;
- internal metadata, remote apply, and repair bypass public authorization and
  are not captured.

If preupdate support is unavailable in the selected SQLite build, explicit
wrapper logging can be the V1 fallback, but the spike should make that choice
explicit.

Spike result: the bundled SQLite build supports preupdate capture. The general
database owns public authorization, while V1's format hook only captures
`items` inserts. The reusable runtime rolls hook failures, operation errors,
and panics back through an internal savepoint while discarding their captured
events. Enabling the rusqlite feature currently requires libclang at build
time through `libsqlite3-sys`; this is accepted for the pre-release
implementation and must be reconsidered as part of distribution work.

### Batch 6: SQLite ordered store and homebase metadata

Implement `SqliteOrderedStore` over `__multilite__meta` on the same SQLite
connection used for `items`, then compose it with the existing
`OrderedMetaStore`. Reads and writes execute synchronously while holding the
connection owner; returned futures are ready, and scans own a snapshot rather
than retaining a SQLite statement or lock.

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

Implementation result: `ConnectionOwner` serializes the one rusqlite
connection with a thread-reentrant mutex, which satisfies Homebase's `Sync`
store boundary while allowing ready metadata futures to re-enter an ambient
runtime operation on the same thread. `SqliteOrderedStore` keeps schema
initialization explicit for Batch 7, snapshots scans eagerly, and wraps every
non-empty `WriteBatch` in a uniquely named savepoint. Consecutive puts and
deletes are grouped into bounded multi-row statements without reordering mixed
runs. Both conformance suites pass; file-backed tests also prove that a domain
row and metadata transition commit or roll back together and retain that
result after reopen.

### Batch 7: file bootstrap schema

Define the one-file/one-space lifecycle and layer temporary V1 over a general
Multilite database:

- general database open initializes or validates `__multilite__meta`, identity,
  and the Homebase client, then commits;
- the V1 connection wrapper runs its local schema migration before public open
  returns;
- a fresh open without options mints a database id and device identity;
- a `ReplicaInvitation` initializes another file with the same database id and
  a distinct device identity;
- an invitation supplied for initialized general state must match its identity;
- each database is wired to the existing homebase client and server handle.

General open and each V1 migration are separate SQLite transactions. General
open is complete before V1 begins; the intermediate base-only state is valid,
durable, and not exposed to application SQL because the wrapper has not yet
returned.

Tests:

- general genesis, reopen, and invited replica opens preserve identity rules;
- interrupted general initialization rolls back completely;
- interrupted V1 initialization preserves general identity and retries from
  version zero;
- invitations round-trip and conflicting invitations are rejected;
- the general database can adopt an ordinary SQLite user schema without V1
  knowledge;
- V1 rejects incompatible user schemas after general adoption;
- V1 reopen is a schema no-op and malformed or newer versions are rejected;
- stock SQLite reads `items` and `PRAGMA integrity_check` passes.

Implementation result: root `database` machinery owns `DatabaseId`,
`ReplicaInvitation`, `OpenOptions`, general classification,
`__multilite__meta`, the
Homebase client, and identity tests. It can add general metadata alongside an
ordinary SQLite user schema without knowing any V1 tables. `v1::Connection` is
a thin temporary wrapper: after general open commits, `v1::schema` reads its
local `__multilite__v1_schema` ledger and applies the required migration in a
second savepoint. Absence of the ledger means version zero; migration `0 -> 1`
creates the ledger, its singleton version row, and `items` atomically. Version
1 always validates both table shapes, malformed states are rejected, and a
version newer than the library is reported explicitly.

If the process dies or V1 migration fails after general open,
`__multilite__meta`,
the database id, device id, clock state, and plaintext space envelope remain
valid. The next open reloads them and retries V1; it never remints identity. A
V1 failure against an existing ordinary user schema likewise leaves only the
general adoption committed. Public SQL, the metadata store, and the wrapper
share one thread-reentrant `ConnectionOwner`; prepared reads retain SQL and
eagerly reprepare under that owner instead of borrowing a second connection.
As the general SQL implementation grows, behavior moves from the V1 wrapper
into the root database until the wrapper and `__multilite__v1_schema` can be
deleted.

#### Opening and identity evolution

The `open(path)` shape is intended to survive the transition from plaintext V1
to encrypted synchronization:

1. In V1, a fresh open mints a random `DatabaseId` and stores a plaintext
   `SpaceEnvelope`. The invitation contains only that public id.
2. Before a supported encrypted release, a fresh open will instead mint the
   final Homebase `NameKey` and initial `SpaceKey`, derive `DatabaseId` from the
   name key, and store the envelope locally. Local submit-log entries will use
   that final cipher even when no server is configured.
3. The versioned `ReplicaInvitation` will grow to carry or unlock the same
   envelope for another device. `DatabaseId` remains public and is not by
   itself sufficient to initialize an encrypted replica.
4. `OpenOptions` can add a key provider, wrapped-envelope source, server route,
   credentials, and SQLite opening flags without changing the default
   `open(path)` call. Supplying options to an initialized file verifies or
   unlocks its stored identity; it never silently replaces it.
5. A configurable server router may later allow an already-open, offline
   connection to attach synchronization. Until then, reopening with a server
   option preserves the durable submit log and all file identity.

The database must not use a placeholder name key or space id. `SpaceId` scopes
all Homebase metadata and participates in the device checksum, while the name
key determines anonymized mutation targets. Changing either after local writes
requires a full atomic migration and is not part of ordinary linking. Value-key
rotation instead adds a cipher epoch and retains old keys for old entries.

### Batch 8: SQL surface gate

Allow only:

- persistent `CREATE TABLE` in the main database;
- `SELECT`;
- `INSERT` into any non-reserved main-database table, including multi-row and
  `INSERT ... SELECT` forms.

Reject:

- `UPDATE`;
- `DELETE`;
- DDL other than `CREATE TABLE`;
- `AUTOINCREMENT` and schema-level `ON CONFLICT` policies;
- explicit `BEGIN`/`COMMIT`/`ROLLBACK`;
- explicit savepoints;
- all PRAGMAs;
- `ATTACH`/`DETACH` and temporary objects;
- reads or writes of `__multilite__` tables through the public connection.

Prepared statements are read-only. Public execution rejects `REPLACE`, every
`INSERT OR ...` conflict policy, every `ON CONFLICT` UPSERT form, multiple
statements, `VACUUM`, `ANALYZE`, and `REINDEX`. A conflict form is rejected
explicitly rather than interpreted as an insert, because it may update,
delete, or silently suppress a row. Internal metadata/apply/repair statements
run under the internal connection mode and remain allowed.

Use the SQLite authorizer for runtime object/action access. Parse each public
execution with SQLite's grammar-derived AST to enforce statement shape and
syntax-level rules the authorizer cannot distinguish.

Tests:

- accepted `CREATE TABLE`, `SELECT`, and single- or multi-row `INSERT` work on
  arbitrary user tables;
- `AUTOINCREMENT` and schema-level conflict policies are rejected before any
  schema mutation;
- unsupported SQL matrix returns clean errors;
- metadata cannot be read or written through the public API;
- rejected statements leave no mutation behind.

Implementation result: `Database` wraps every format hook in its own mandatory
public policy. `Database::execute` applies the grammar-derived AST gate, and
the database authorizer admits only the action graph needed for the three
verbs, including SQLite's internal catalog writes and implicit indexes for
`CREATE TABLE`; the `__multilite__` namespace is reserved case-insensitively.
The AST gate admits only one restricted `CREATE TABLE` or `INSERT` execution
and rejects `AUTOINCREMENT`, schema conflict policies, `REPLACE`, `INSERT OR ...`,
UPSERT, and `RETURNING` before SQLite mutates the file. Prepared SQL is
authorized as public and must be read-only before a reusable statement handle
is returned. V1 adds only its local migration ledger, validation of `items`,
and preupdate capture of `items` inserts; it does not own the public SQL
surface.

### Batch 9a: restricted schema operations

The first synchronized schema slice accepts only an unqualified persistent
table with explicit `INTEGER`, `REAL`, `TEXT`, or `BLOB` columns, exactly one
inline primary key, and optional `NOT NULL`. A non-`INTEGER` primary key must
also be `NOT NULL`. It rejects `IF NOT EXISTS`, `AS SELECT`, table constraints,
named constraints, `UNIQUE`, `CHECK`, defaults, collations, generated columns,
foreign keys, sized or custom type names, ordering/conflict clauses, `STRICT`,
`WITHOUT ROWID`, and `AUTOINCREMENT`. Later batches can add each omitted
semantic deliberately.

`MultiliteOp::CreateTable` carries UUID-shaped mutation, table, and column ids.
Its tagged frame stores the exact SQL plus the structured table definition.
Lowering one operation produces:

```text
(multilite, schema, log, mutation_uuid)                       -> mutation frame
(multilite, schema, scopes, tables, table_uuid)               -> mutation_uuid
(multilite, schema, scopes, table-names, encoded_name)        -> mutation_uuid
```

The immutable UUID log and mutable revision cells are separate layers. The
Homebase admission log supplies total replay order; revision cells identify
the latest mutation touching a coordination scope. Short canonical names use
`name-` followed by at most 250 UTF-8 bytes. Longer names use `hash-` followed
by a raw, domain-separated SHA-256 digest. The complete spelling remains in
the mutation value and is protected whenever the space uses encryption.

The table and name revision cells become exact `RangeAssert` prefixes when the
lowered operation is bound to a local applied cut. Independently minted tables
with the same canonical name will therefore race on the name cell, while
disjoint names can admit independently once capture is wired in.

The reverse translation accepts only a complete authenticated three-entry
Homebase envelope. It verifies UUID-v4 ids, log and revision keys and values,
and that the literal SQL projects to the same structured `MultiliteOp`. This
batch does not submit or apply the operation. Tests pin tagged-frame
roundtrips, malformed frames and envelopes, key shape, short/long names, and
SQL/structure agreement.

### Batch 9b: atomically capture CREATE TABLE

Executing a validated table creation first mints its `MultiliteOp`, reads the
current applied admit cut `N - 1`, and lowers the operation with table-id and
canonical-name assertions at that cut. One SQLite savepoint then performs both
the local DDL and `homebase submit_unchecked`. The `SqliteOrderedStore` joins
the active savepoint, so the SQLite schema and submit log cannot diverge. No
network work occurs inside this unit.

Tests prove that the table and three-entry submission survive reopen together,
that the two revision assertions use the applied admit cut, and that an
injected metadata failure rolls back both the DDL and submit-log transition.

### Batch 9c: pending CREATE TABLE effects

Add a local Multilite pending-effects journal keyed by the Homebase
`DeviceSeq`. It records the logical operation plus explicit accept and reject
behavior without adding another operation log. For `CreateTable`, acceptance
only retires the pending record; rejection removes the speculative table.
Suffix rollback executes reject effects in reverse device order. Journal
updates join the same SQLite transaction as submission and materialization.

Implementation result: `__multilite__pending` stores the assigned sequence and
one opaque, versioned record frame containing the local `MultiliteOp` plus
repeated accept and reject effects. The frame repeats its `DeviceSeq`; loading
verifies that it matches the fixed-width big-endian row key, which preserves
numeric order without constraining Homebase's `u64` domain to SQLite's signed
integer range. The initial acceptance list is empty and the rejection list is
`[DropTable(exact_name)]`. General database bootstrap creates and validates
this table with the Homebase metastore; partial initialization is invalid.
Tests cover codec and ordered roundtrip, malformed rows, mismatched sequence
keys, namespace lookalikes, reopen recovery, and failure after Homebase
submission but before pending insertion.

### Batch 10: CREATE TABLE push

Expose Multilite `push` as a thin policy layer over Homebase push. A definitive
acknowledged prefix finalizes its pending effects immediately at push time. A
stalled schema range assertion returns a rejection bound to the observed active
submit window but performs no rollback. Unavailable or ambiguous outcomes are
retryable and never authorize repair.

Implementation result: `MultiliteConnection::push()` runs network admission
without holding a SQLite transaction. After an acknowledgement, Multilite's
`MetaStore` decorator wraps Homebase's submit-log trim, submit `neck` movement,
acceptance effects, and pending-row deletion in one outer SQLite savepoint.
There is therefore no committed state in which `neck` has advanced but its
accepted pending record remains; reopen treats such a state as corruption. A
stall returns an opaque `PushRejection` carrying the database, device, failed
sequence, exact observed submit window, and kernel error for later rollback
validation. It does not change the rejected operation or suffix. An
unavailable push returns an error and retains every still-active pending row;
any earlier prefix acknowledged during the same push attempt was already
trimmed and finalized atomically.

Tests cover an empty offline push, full drain, an accepted prefix followed by
a same-name range-assert rejection, unavailable transport, complete Homebase
`MetaStore` conformance, and an injected pending-cleanup failure that rolls
back `neck` movement before a server-ahead retry converges.

### Batch 11: Homebase rebase analysis

Add a read-only Homebase client operation that compares unapplied admitted
history with the active local submission window. It uses admitted keys and the
submissions' existing range assertions to identify which local device
sequences can no longer be replayed over the new admit cut. It moves no cursor
and does not interpret Multilite operations.

Implementation result: `Space::analyze_rebase(from..to)` point-reads both
cursor triples, scans the active submit window and exactly that retained admit
interval, and returns the interval and observed cursors with
per-device-sequence `RangeAssertFailure`s. Range assertions are the sole
dependency declaration. Foreign Set/Delete descendants and overlapping
DeleteRanges invalidate them; own-device history, admissions at or below
`upto`, and unasserted submissions do not. If the same local sequence is
already present in the selected interval, its admission caps the history
relevant to replay. Analysis performs no network I/O, decryption, or cursor
transition and assigns no implicit meaning to admit `neck`; callers own
interval continuity and application policy. The `MetaStore` contract now has a symmetric
`oplog_cursors(space)` point read, and Multilite uses it instead of loading the
entire client state when recording a push rejection.

Tests cover point and range overlap, sibling exclusion, inclusive `upto`, all
failures and their order, own-device exclusion, the already-admitted retry
case, unasserted submissions, explicit empty and unavailable intervals,
encrypted name-domain agreement, cursor immutability, and both reference and
joined-store conformance.

### Batch 12: CREATE TABLE pull

Multilite `pull` is fetch-only. It asks Homebase to append complete dense server
batches to the durable admit log and does not modify SQLite user schema or move
admit `neck`. Applications may therefore fetch while deferring reconciliation.

Implementation result: `MultiliteConnection::pull()` synchronously delegates
to Homebase's paged pull and returns the last server admission sequence durably
captured. It performs no SQLite schema application and gives the captured
admissions no rebase or applied meaning. Each complete authenticated page is
appended atomically by the joined metadata store; a later-page failure can
retain earlier pages but cannot append the failed page. Repeated pulls are
idempotent.

Tests cover a remote three-entry `CREATE TABLE` admission, unchanged SQLite
schema and admit `neck`, advancing admit `tail`, repeated pull without metadata
churn, persistence across reopen, and an unavailable first page leaving the
admit log unchanged.

### Batch 13: CREATE TABLE rebase

Multilite `rebase` snapshots `[admit.neck, admit.tail)`, decodes and validates
those admitted operations, asks Homebase to analyze that exact interval, and
returns an error without mutation when a conflict exists. Otherwise it applies
foreign table creations in admission order, verifies already-materialized own
admissions, rechecks the returned submit/admit cursor snapshots, and advances
admit `neck` in the same SQLite transaction. Internal apply mode suppresses
local capture.

### Batch 14: explicit CREATE TABLE rollback

Given a current rejection or rebase conflict, explicit `rollback` validates
that the pending and Homebase windows have not changed. In one SQLite
transaction it runs reject effects for the speculative suffix in reverse
order, applies the admitted schema history in forward order, records the
Homebase rollback marker, advances admit `neck`, and retires the corresponding
pending records. Rollback is the only V1 conflict-repair strategy; `push` and
`rebase` never invoke it implicitly.

### Batch 15: CREATE TABLE fault and convergence matrix

Run two-device tests for disjoint table creation, same-name rejection,
pull-before-push conflict, explicit rollback, lost acknowledgement, stale
rejection handles, restart between every phase, and injected transaction
failures. Both replicas must eventually expose identical `sqlite_schema`
results, while stock SQLite remains able to inspect each file.

### Batch 16 and later: INSERT pipeline

Only after CREATE TABLE converges end to end, add row identity and operation
encoding, preupdate-hook capture, atomic row submission, push, pull, rebase,
and explicit rollback. Multi-row and `INSERT ... SELECT` must preserve SQLite
row order in one Homebase commit. Duplicate local primary keys produce no
submission, exact-key assertions enforce append-only OCC, and pending row
effects follow the same accept/reject protocol established for schema.

## Post-V1 extensions

Each extension should relax one V1 constraint or add one clear user-facing
capability.

1. **Transactions and atomic multi-statement batches.** Let several statements sync as
   one atomic admission batch.
2. **`UPDATE`.** Capture and ship after-images for changing rows.
3. **`DELETE` and tombstones.** Remove rows and preserve delete history.
4. **Secondary indexes.** Maintain derived index keys and local/index sync
    metadata.
5. **Unique constraints and witness keys.** Enforce uniqueness across devices.
6. **Foreign keys.** Add dependency tracking and parent/child validation.
7. **CHECK, NOT NULL, defaults, generated columns.** Expand SQLite constraint
    fidelity.
8. **ALTER TABLE migrations.** Support additive and rename schema evolution.
9. **DROP TABLE / DROP COLUMN.** Use range delete, schema-history
    materialization, or row rewrite.
10. **Named/idempotent migrations.** Let app startup migrations run safely on
    multiple devices.
11. **Existing SQLite adoption.** Turn supported vanilla SQLite files into
    multilite files.
12. **Large database snapshotting.** Avoid doubling large files during
    adoption.
13. **Encryption.** Encrypt keys, schema records, and values so the server sees
    only opaque data.
14. **Formal leases.** Add explicit lease acquisition/renewal semantics.
15. **Device fencing / single-active-writer lifecycle.** Add WhatsApp-like
    takeover for one active writer at a time.
16. **Coarse range asserts.** Validate broad data/schema prefixes at admission.
17. **Table-level multi-writer mode.** Allow concurrent writers on independent
    tables.
18. **Row/prefix optimistic multi-writer mode.** Allow concurrent writers on
    disjoint rows or prefixes.
19. **Precise read/predicate capture.** Capture reads and write predicates for
    finer serializability.
20. **Rollback repair for rejected speculative tails.** Generalize V1 repair
    beyond append-only inserts.
21. **Partial replication / shapes.** Sync only selected collections, tables, or
    query shapes.
22. **Online index builds.** Add `Building -> Ready` index state and resumable
    backfill.
23. **Views.** Support schema-only views and eventually dependency-aware view
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
