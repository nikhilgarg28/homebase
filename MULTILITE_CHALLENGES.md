# multilite — vtable architecture: design sketch + challenge inventory

*Working document · July 2026 · Companion to [DESIGN.md](./DESIGN.md) (single-file doctrine,
witness compiler, launch claims) and the admission/rollback v2 kernel plan.*

**Purpose.** Before implementing the SQL layer, inventory every known complexity of the
vtable-based interception architecture. We refine this document until each item is either
**satisfied by the design** or **explicitly unsupported**, then implement.

**Status legend:** `open` (undecided) · `leaning: …` (tentative) · `decided: …` (settled,
implement as stated) · `unsupported-v1` (rejected at adoption/DDL time with a clear error).

---

## Architecture sketch (current thinking)

1. **Real tables keep their user names in `main`** — reads run unmodified at native speed
   against real b-trees and indexes.
2. **Per-table shadow vtables live in `temp`** under mangled names (e.g. `temp._w_orders`),
   created per connection at open from the catalog. Temp schema never persists — the file
   stays pristine for detach.
3. **A wrapper-level rewriter** retargets only *write statements* (`INSERT`/`UPDATE`/
   `DELETE`) at the shadow; reads inside those statements still hit real tables.
4. **`xBestIndex`/`xFilter`** forward the write statement's row scan to the real table and
   capture WHERE constraints in structured form → range asserts / lease derivation.
5. **`xUpdate`** forwards each write to the real table eagerly (read-your-writes) and
   appends the logical op to an in-memory per-transaction buffer.
6. **At COMMIT** the wrapper runs the lease fixpoint over the buffer (validate coverage,
   acquire missing, barrier); on success the buffer materializes into `hb_oplog` rows in
   the same SQLite transaction (single fsync domain); on failure the whole transaction
   rolls back — real-table writes vanish with it.
7. **Metadata** (`hb_*` system tables in `main`): oplog, leases, cursors, device identity,
   codec caches, adoption record — the `MetaStore` trait implemented natively over SQLite,
   gated by the existing conformance suite in `client/src/meta.rs`.

---

## Group A — Adoption and genesis

**A1. First-load adoption transaction.** Opening a vanilla SQLite file under multilite
creates the system tables (`hb_meta`, `hb_oplog`, `hb_leases`, …) and records the adoption
(device id, adoption timestamp, schema snapshot hash, per-table max rowids). Must be one
atomic transaction so a crash mid-adoption leaves either a vanilla file or a fully adopted
one — never half.
`Status: decided — single-transaction adoption; snapshot ship is a separate resumable saga.`

**A2. System-table naming.** `_multilite_metadata` vs the `hb_*` / `_hb_*` family.
DESIGN.md already commits to `hb_*` (`hb_oplog`, `hb_leases`) and the SLT exclusion
manifest mentions `_hb_*`. One prefix, everywhere, decided before first byte is written —
it is part of the detach contract and the conformance exclusion manifest.
`Status: leaning — hb_* per DESIGN.md; reconcile the _hb_ vs hb_ inconsistency there.`

**A3. Snapshot-into-oplog vs virtual snapshot cursor.** Two ways to make pre-adoption data
shippable:
- **(a) Materialize:** every existing row becomes an oplog `Set` at adoption. Simple,
  uniform push path; ~2× file size until synced + pruned (10 GB db → 20 GB file).
- **(b) Virtual snapshot:** record only a snapshot boundary at adoption; at first sync,
  scan user tables directly and upload as generation 0 (matches LAUNCH_CHECKLIST "attach =
  snapshot upload as generation 0"); only post-adoption writes go through the oplog.
Option (b) avoids the blowup but raises A4.
`Status: open — user preference is (a) for v1 simplicity; (b) likely required for large
files. Possibly (a) with a size threshold, or (a) first then (b) as an optimization.`

**A4. Snapshot/delta convergence (if A3-b).** Rows written after adoption but before the
snapshot scan appear in both the scan and the oplog. Server-side convergence needs the
oplog entries' vers to dominate the scanned rows' vers — ver assignment order between scan
stamping and commit stamping must be pinned, or the scan must run inside one read
transaction at a known cut.
`Status: open — only matters if A3 lands on (b).`

**A5. Unsupported-schema scan at adoption.** Adoption must walk `sqlite_master` and reject
(or flag) features the v1 design cannot intercept: triggers, cascading FKs (C1), composite
`WITHOUT ROWID` PKs (B5), custom collations (D2), pre-existing virtual tables (A7), views
with `INSTEAD OF` triggers (C3). Decide per feature: hard-reject adoption vs adopt with the
table marked local-only/unsynced.
`Status: leaning — hard-reject at adoption with a precise error listing offenders; a
local-only escape hatch dilutes the sync contract.`

**A6. Rowid headroom scan.** Adoption records each table's `max(rowid)` and starts the
device's rowid block allocation above it (F1). Also validates no table is near rowid
exhaustion given block sizing. DESIGN.md already anticipates this scan in the conformance
matrix ("a vanilla-produced file is adopted mid-suite").
`Status: decided — part of the adoption transaction.`

**A7. Pre-existing virtual tables in the adopted file** (fts5, rtree, …). Cannot shadow a
vtable with a vtable; their content is derived data anyway.
`Status: leaning — allow read-only passthrough, never synced, rebuilt locally; writes to
them outside a recognized "derived from synced tables" pattern are the app's problem.
Needs a decision on whether fts5 shadow tables (real b-trees) confuse the adoption scan.`

**A8. Re-open and idempotency.** Opening an already-adopted file must be a no-op fast path
(detect `hb_meta`, verify device identity, run foreign-write check — Group B). Re-adoption
after detach + foreign writes is Group B's problem, not a second adoption.
`Status: decided.`

---

## Group B — Foreign-write protection

Threat: writes to the file by plain SQLite — while a multilite connection is live, or
between multilite sessions — silently invalidate oplog/cursor/ver bookkeeping. Required
behavior: **detect and warn** ("file not valid to open under multilite"); prevention is
opt-in policy.

**B1. Offline detection (between sessions).** Every multilite commit already writes
`hb_*` rows in the same transaction, so multilite state is always consistent with data at
our commits; a foreign commit changes data without touching `hb_*`. Candidate mechanisms:
- **Header change counter** (file header offset 24): store its value in `hb_meta` at every
  multilite commit (cheap: it's our own transaction); on open, compare header vs stored.
  Foreign rollback-journal commits bump the header but not our stored copy → mismatch.
  **WAL caveat:** in WAL mode the header counter moves on checkpoint, not per commit —
  detection must also inspect `-wal` file state (salts, frame count) and we must checkpoint
  + record on clean close.
- **Schema cookie** (`PRAGMA schema_version`): same trick, catches foreign DDL specifically.
- **Content CRC** (per-table or per-space, already in TODO.md): definitive but O(data);
  optional deep check behind a flag or on suspicion.
`Status: leaning — change counter + schema cookie snapshot in hb_meta as the always-on
check; CRC as an explicit deep-verify mode. Exact WAL-mode story open.`

**B2. Live detection (concurrent foreign connection).** `PRAGMA data_version` changes when
*another* connection commits — poll it per statement or per transaction on the multilite
connection; a change we didn't cause → warn/fail.
`Status: leaning — check data_version at each commit boundary; per-statement is overkill.`

**B3. Prevention (opt-in).** `PRAGMA locking_mode=EXCLUSIVE` on the multilite connection
blocks all other writers (and in WAL mode, other connections entirely). Strongest
protection, breaks legitimate read-only observers.
`Status: leaning — off by default, exposed as a connection-string option.`

**B4. Response to detection.** Warning vs hard-fail vs recovery. A foreign write may be
benign (VACUUM by a backup tool?) or corrupting (rows changed under synced vers).
Recovery = re-adoption: diff current content against last-known state (needs B1-CRC or
shadow tags), or full re-snapshot as a new generation / fresh device.
`Status: open — v1: hard warning + refuse to sync (local reads still work); recovery
flow designed later. Note VACUUM rewrites the file but preserves content and bumps
counters — decide whether to whitelist content-preserving operations via CRC.`

**B5 (cross-cutting note).** Detach is a *feature* (DESIGN.md) — the contract is
one-directional: detach freely, but a multilite re-open after foreign writes is not
guaranteed. This group implements exactly that guarantee boundary.

---

## Group C — Constraints and side-effect writes

**C1. FK cascade bypass.** `xUpdate` forwards a DELETE to the real parent table; the
engine fires `ON DELETE CASCADE` on the real child table directly — invisible to the
shadow layer. Options: (a) v1-reject cascading FKs at adoption/DDL (`RESTRICT`/`NO ACTION`
are fine — they check, never write); (b) supplementary capture via preupdate hook or
session extension on real tables; (c) multilite re-implements constraint semantics.
(c) is rejected — it kills "it *is* SQLite executing".
`Status: leaning — (a) for v1; (b) is the upgrade path and, note honestly, if preupdate
ends up present anyway the vtable layer's remaining value is asserts capture + veto timing.`

**C2. Triggers.** Cannot be created on virtual tables at all; triggers on real tables fire
on forwarded writes and their bodies write real tables directly — same bypass as C1.
`Status: leaning — unsupported-v1, rejected at adoption/DDL scan.`

**C3. Views with INSTEAD OF triggers.** Allowed by SQLite on views; the trigger body
writes real tables — a side-effect channel that doesn't even involve our shadows.
`Status: leaning — unsupported-v1 (plain views are fine and read-only).`

**C4. Deferred FK enforcement.** `PRAGMA defer_foreign_keys` / `DEFERRABLE INITIALLY
DEFERRED` moves FK checks to COMMIT — after our fixpoint has validated the buffer. A
transaction can pass lease checks then fail FK at commit; must be handled as a normal
rollback, not a torn state.
`Status: open — likely fine (whole SQLite txn rolls back, buffer discarded), verify in
spike.`

**C5. CHECK / NOT NULL / UNIQUE on real tables.** Enforced by the engine on forwarded
writes — errors surface through `xUpdate`'s return. Uniqueness additionally needs
*distributed* enforcement via witness keys (D5): local UNIQUE catches local conflicts
only; cross-device uniqueness is the kernel's ver/witness machinery.
`Status: decided — engine enforces locally; witness keys carry the distributed claim.`

---

## Group D — Data encoding and collation

Local storage stays plain SQLite rows. The codec below governs only the **oplog/wire
representation** (kernel keys/values) — independent of local at-rest encryption.

**D1. Key codec.** Row → `(User, table′, pk-components…)` using the kernel's
order-preserving tuple encoding; index entry → one kernel key per index. Table identity:
name vs stable table id surviving RENAME (see I3).
`Status: leaning — stable numeric table id minted at CREATE/adoption, stored in hb_meta;
survives rename without rewriting keys.`

**D2. Collations in keys.** Keys must encode *collated* values — a `NOCASE` unique column
must map `'Foo'` and `'foo'` to one witness key. Built-ins (`BINARY`, `NOCASE`, `RTRIM`)
are implementable in the codec; custom collations (app-registered C callbacks) cannot be
reproduced under E2EE pseudonymization.
`Status: leaning — support the three built-ins; custom collations unsupported-v1
(rejected at adoption/DDL when used in an indexed/PK position).`

**D3. Numeric key affinity.** SQLite compares INTEGER and REAL cross-type in one column
(`1 == 1.0`); an order-preserving byte encoding must place 1 and 1.0 identically for PK
and witness purposes. Needs a numeric normalization rule in the codec (à la SQLite's own
record format semantics).
`Status: open — codec design detail; must be pinned before any key is written.`

**D4. Value codec.** After-image only: column count + typed values with exact fidelity
(NULL / INTEGER / REAL / TEXT / BLOB, text encoding pinned to UTF-8). `xUpdate` provides
no old column values — before-images would cost a read of the real table.
`Status: leaning — after-image only; delete = key + Delete seal (v2 Seal model). Frame
format versioned from day one.`

**D5. Index entries and witness keys.** Computed client-side from schema knowledge (we
know the indexes), not observed from the engine. Expression indexes: the expression must
be evaluated by us to compute the witness key — deterministic SQL expressions only.
`Status: leaning — plain and prefix-of-column indexes v1; expression/partial indexes
escalate to table-level protection or unsupported-v1 (decide per SLT corpus impact).`

**D6. Bucketing/padding before encryption** (TODO.md item): key components and values
leak length; padding policy interacts with the codec frame.
`Status: open — orthogonal to v1 correctness; reserve frame bytes for it.`

**D7. Partition components** (DESIGN.md `@partition` directive): partition dims prepend to
storage keys of rows *and* index entries. Not v1-critical but the key codec must reserve
the layout so adding partitions later isn't a key migration.
`Status: open — layout reservation decision only.`

---

## Group E — Read capture and range asserts

**E1. Pure SELECTs are invisible.** Reads go straight to real tables — no capture. A write
transaction's read dependencies (`SELECT balance` then `UPDATE orders`) are unguarded.
Options: (a) wrapper-level authorizer (`sqlite3_set_authorizer`) for table-granularity
read sets on statements inside write transactions; (b) EQP + bound-parameter analysis for
approximate index ranges (heuristic — parses plan output); (c) rewrite read statements
*inside write transactions* to route through read-shadowing vtables — precise structured
capture via `xBestIndex`, with vtable-forwarding overhead confined to write transactions
(pure SELECTs stay native, and serializability only needs write-transaction read sets);
or (d) document reads as unasserted in v1. Note this item is identical under a
preupdate-hook architecture — hooks capture writes only; read capture is a separate
channel in every design.
`Status: leaning — (a) table-level asserts for v1; (c) is the precision upgrade path,
preferred over (b) because it reuses the assert mapping (E2) instead of plan scraping.`

**E2. xBestIndex → range assert mapping.** The write statement's own scan hands us
structured constraints `(column, op, value)` at `xFilter` time (bindings resolved). Pin
the mapping table: EQ on PK/prefix → point/prefix assert or row lease; GT/LT/GE/LE on an
ordered prefix → interval assert; OR terms, `LIKE`, `GLOB`, `IN` → decide each (likely:
`IN` = union of point asserts; `LIKE 'abc%'` = prefix when left-anchored; everything else
escalates).
`Status: open — write the full op table as a spike deliverable.`

**E3. Escalation path.** Any unclassifiable scan → table-level assert/lease. This is the
correctness safety net; precision is optimization.
`Status: decided — escalation always available, per DESIGN.md.`

**E4. Assert vs read-lease split.** Optimistic scan dependencies → range asserts (cheap,
fail at admission); stability across the acquire barrier (FK parent existence) → read
leases. Maps directly onto v2's `RangeAssert` and lease kinds.
`Status: leaning — asserts by default; read leases only where the fixpoint needs
cross-barrier stability.`

**E5. Assert timing vs snapshot.** Constraints captured at `xFilter` describe what the
statement *scanned*; the assert must pin the admission point the local replica was at —
tie-in with per-prefix cursors and `effective_prefix_max` equality semantics from v2.
`Status: open — needs a worked example against the v2 assert definition.`

---

## Group F — Rowids and primary keys

**F1. Per-device rowid block allocation.** Concurrent writers colliding on `max(rowid)+1`
is the known problem; DESIGN commits to block allocation. Decide block size, allocator
state location (`hb_meta`), refill protocol, and interaction with `AUTOINCREMENT`
(`sqlite_sequence` is engine-owned — either override at INSERT-forward time or reject
AUTOINCREMENT at adoption).
`Status: open — block size and AUTOINCREMENT stance; allocator-in-hb_meta is decided.`

**F2. Dense-rowid assumptions break.** Block allocation makes rowids sparse; SLT corpus
families asserting dense rowids go in the exclusion manifest (already anticipated in
DESIGN.md).
`Status: decided — exclusion manifest entry.`

**F3. `INTEGER PRIMARY KEY` alias.** IPK *is* the rowid; user-supplied IPK values must be
honored (no reallocation), which means user-chosen IPKs can collide across devices —
that's a normal distributed uniqueness conflict resolved by witness/ver machinery, not
block allocation.
`Status: decided — user-supplied IPK = user data; blocks only govern auto-assigned.`

**F4. Composite-PK WITHOUT ROWID tables.** Writable vtables require a single-column PK for
WITHOUT ROWID shadowing; composite-PK tables can't be shadowed.
`Status: leaning — unsupported-v1 at adoption/DDL; revisit via rowid-shadow mapping
table if demand appears.`

---

## Group G — Vtable machinery gaps (dialect fidelity)

**G1. UPSERT.** `ON CONFLICT DO UPDATE/NOTHING` does not work on virtual tables. The
rewriter must translate upserts into engine-equivalent statement sequences (semantics are
subtle: `excluded.` references, multiple conflict targets) or v1 forbids them.
`Status: open — attempt rewriter translation in spike; forbid if semantics don't hold.`

**G2. Omitted column vs explicit NULL.** vtable INSERT can't distinguish them, so DEFAULT
values can't be applied inside `xUpdate`. The rewriter (which sees the column list) must
expand defaults into the statement — including `CURRENT_TIMESTAMP` and expression
defaults, evaluated per-row at the right time.
`Status: leaning — rewriter expands defaults; this single item pushes the rewriter from
"tokenizer" to "real parser". Acknowledge that cost once, here.`

**G3. Conflict modes (`OR REPLACE`/`OR IGNORE`/`OR ABORT`…).** Delivered to `xUpdate` via
`sqlite3_vtab_on_conflict`; our forward must reproduce the semantics on the real table
(e.g. forward with the same `OR` clause) *and* capture the implied deletes of `OR
REPLACE` (which the engine performs on the real table — do we see them? No: same bypass
class as C1 unless the forward itself is what performs them).
`Status: open — spike item; likely: forward carries the OR clause, and OR REPLACE's
implied delete is detected by a pre-read in xUpdate (one extra point query).`

**G4. RETURNING.** Interaction with vtable forwarding (values must reflect the real-table
result, e.g. assigned rowid) — verify support and fidelity.
`Status: open — spike item.`

**G5. Generated columns.** Stored/virtual generated columns are computed by the engine on
the real table; the shadow's `declare_vtab` schema and the oplog row image must agree on
whether generated values are captured (stored: yes from a post-forward read; virtual:
never stored, recompute on apply side?).
`Status: open — lean: capture stored generated values via the forwarded row's re-read;
virtual generated columns excluded from the value codec.`

**G6. Savepoint fidelity of the op buffer.** `xSavepoint`/`xRelease`/`xRollbackTo` must
shear the in-memory buffer exactly in step with the engine, or oplog diverges from the
file. Statement-level aborts (implicit savepoints) included.
`Status: decided — implement the three methods; conformance-test buffer vs file.`

**G7. Multi-row statement atomicity.** One statement updating N rows = N `xUpdate` calls;
if the Kth fails, the engine rolls back the statement (implicit savepoint) — buffer must
follow (G6). Also pins that oplog granularity is the *transaction*, not the statement.
`Status: decided — transaction-granular oplog records (matches kernel batch =
transaction).`

---

## Group H — Interception and rewriting

**H1. Shadow naming and resolution.** Temp schema wins unqualified resolution — shadows
must NOT share user table names (that would capture reads). Mangled temp names + rewriter
retargeting write statements only.
`Status: decided — per architecture sketch.`

**H2. Rewriter scope.** Target-table renaming is shallow; G1 (upsert translation) and G2
(default expansion) are not. Decide the parsing substrate once: hand-rolled tokenizer vs a
real SQLite-dialect parser (and its round-trip fidelity), vs preparing the statement and
using SQLite's own analysis to locate the target.
`Status: open — biggest implementation-cost fork in the layer.`

**H3. Statements that bypass the rewriter.** `ATTACH`ed databases, `PRAGMA`s with side
effects (`journal_mode`, `wal_checkpoint`, `writable_schema`!), `VACUUM`, `REINDEX`,
`ANALYZE`. Each needs a stance: passthrough / intercepted / forbidden.
`Status: open — inventory pass needed; writable_schema and VACUUM INTO at minimum are
dangerous.`

**H4. Multiple connections from the app.** Two multilite connections to one file: temp
shadows are per-connection (fine) but the op buffer/fixpoint assume one writer.
`Status: leaning — v1: one write connection per file enforced at open (matches SQLite's
own single-writer reality); read-only companion connections allowed.`

**H5. Prepared-statement lifecycle.** Apps re-prepare rarely; if DDL regenerates a shadow
(I2), outstanding prepared statements against the old shadow must error cleanly
(`SQLITE_SCHEMA` behavior should handle it — verify).
`Status: open — verify, likely free.`

---

## Group I — Schema management and DDL

**I1. DDL interception.** First-token dispatch at the wrapper (like the planned `LEASE`
family): require db-level write lease, apply to the real table, regenerate the temp
shadow, append a schema oplog record.
`Status: decided — shape; details below open.`

**I2. Shadow regeneration.** After `ALTER TABLE` (add column, rename, drop column) the
shadow's declared schema is stale; drop + recreate in the same wrapper operation; see H5
for prepared statements.
`Status: decided.`

**I3. Schema replication representation.** What ships in the oplog for DDL: the literal
SQL text (replayed on apply — matches "schema propagates via sqlite_master") vs a
structured schema delta. Text is simple and DESIGN-aligned; interacts with D1 (stable
table ids must survive replay) and version skew between multilite builds.
`Status: leaning — literal SQL text + minted table-id record alongside.`

**I4. Schema drift detection on apply.** A replica applying a remote DDL record while its
local schema diverged (should be impossible under db-lease DDL — but crashes/foreign
writes happen). Cheap guard: schema cookie / hash carried in the DDL record, mismatch →
resync.
`Status: leaning — carry schema hash in DDL records.`

**I5. `sqlite_master` visibility.** Shadows live in temp (invisible in the file — good);
`hb_*` tables are visible by contract (exclusion manifest). Nothing else may leak.
`Status: decided — CI-enforced by the detach fidelity job (DESIGN.md).`

---

## Group J — Transactions, sync, and the apply path

**J1. Commit fixpoint protocol.** Validate buffer coverage → acquire missing leases
(network, barrier) → barrier may deliver remote rows → current transaction read stale data
→ must abort + retry, not patch. Retry = re-run the app's transaction (app-visible retry
or wrapper auto-retry with budget, per DESIGN's retry budget).
`Status: decided in shape (optimistic, commit-time), retry surface open (auto vs
SQLITE_BUSY to app).`

**J2. Apply-path re-entrancy.** Applying remote ops writes real tables on the same file;
those writes must NOT re-enter capture (no oplog echo). With capture confined to shadow
vtables, the apply path simply writes real tables directly — structurally immune. Foreign-
write detection (B2) must likewise not fire on our own apply writes (`data_version` only
moves for *other* connections — verify the apply path shares the connection or is
whitelisted).
`Status: leaning — apply on the same connection (shares the transaction domain with
cursor updates); B2 check suspended during apply.`

**J3. Apply vs open user transaction.** Remote ops arriving while the app holds an open
write transaction: applying mid-transaction changes the app's snapshot. Queue applies
until the transaction closes.
`Status: decided — applies queue behind user transactions.`

**J4. Journal mode.** WAL strongly preferred (readers during sync applies, checkpoint
control for B1); rollback-journal files must be migrated at adoption or supported for
detection.
`Status: leaning — force WAL at adoption (it's a persistent property); document as an
adoption-time file change. Revisit if it breaks the "file stays pristine" story for
detach (WAL mode survives detach fine — vanilla SQLite reads WAL files).`

**J5. Durability watermark surface.** `last_acked_seq` API (launch checklist) comes from
`hb_*` cursors; the remote-sync-commit pragma (DESIGN) turns the commit fixpoint into
1-RTT. Both ride this layer — no new machinery, but the fixpoint must expose hooks.
`Status: decided — design the fixpoint API with both consumers in mind.`

---

## Group K — File fidelity (detach / attach contract)

**K1. Detach cleanliness.** After any workload: file opens under stock libsqlite3, passes
`integrity_check`, byte-identical query results, no additions beyond `hb_*`. Already a
CI-enforced conformance job in DESIGN.md; the vtable architecture makes it easy (shadows
in temp, nothing engine-invisible in the file).
`Status: decided.`

**K2. Attach = adoption.** Group A. The conformance matrix adopts a vanilla file
mid-suite and continues — adoption must be cheap enough to run in CI constantly.
`Status: decided — adoption is the same code path, exercised by CI.`

**K3. What breaks fidelity.** Forced WAL (J4), `hb_*` tables (contract), sparse rowids
(F2). Keep this list exhaustive and in the exclusion manifest.
`Status: open — maintain as a living list here until the manifest exists.`

---

## Group L — Isolation semantics and replica completeness

**L1. Offered isolation level(s).** DESIGN.md's contract: **serializable writes,
three-tier reads** — local (0 RTT, snapshot freshness), owned (0 RTT, authoritative under
held leases), consistent (1 RTT, true `read_at` cut). Decide what multilite v1 actually
exposes and how: default = local-snapshot reads + optimistic serializable write
transactions (abort on assert/lease failure at the commit fixpoint); `BEGIN IMMEDIATE`
escalates to pessimistic (acquire before executing, DESIGN's documented hole for write
skew on *undeclared* invariants at default isolation); the strong-read pragma forces the
consistent tier. Each tier must be nameable in the physics doc — no tier the docs can't
explain.
`Status: leaning — three tiers as above; v1 ships local + owned with commit-time
serializability, consistent tier behind the pragma.`

**L2. Conflict-detection granularity (how coarse is isolation).** With v1's table-level
read asserts (E1), two write transactions touching the same *table* conflict even on
disjoint rows — spurious aborts under concurrency, correct but coarse. Writes are already
row-precise (row leases from the write set); reads are the coarse side. Decide the
acceptable spurious-abort posture for v1 (single-writer default lease makes this moot
initially — coarseness only bites once multi-writer contention exists) and the refinement
trigger (E1-c precise read capture) when it does.
`Status: leaning — accept table-level read coarseness for v1; revisit with E1-c when
multi-writer workloads land. Document the asymmetry: row-precise writes, table-coarse
reads.`

**L3. Replica completeness model.** Does the local db hold a **complete prefix of the
space's admission history** (full replica, one cursor: every table fully formed as of
admission seq N), or a **patchwork of prefix-granular pulls** (shape/partition cursors at
different points)? A patchwork breaks naive SQL: a table scan cannot distinguish "row not
replicated" from "row absent", so any unscoped read over a partially-cached table is
silently wrong; asserts also become ambiguous (which cursor does an assert pin — E5).
Full replica makes every local read well-defined at one cut and gives asserts a single
anchor.
`Status: leaning — v1 assumes full-space replica with a single cursor; partial
replication (shapes, per-prefix cursors) arrives only with query-shape coverage checking
(unindexed reads → subscribe-table fallback, per DESIGN.md). This assumption should be
stated as an invariant in hb_meta so shapes can't be half-adopted later.`

**L4. Read-your-writes vs rejected batches (local rollback repair).** Local reads see
committed-but-unacked writes — they live in the real tables. If the server rejects a
pushed batch (assert failure, lease loss), the v2 kernel answer is caller-driven
`rollback(to)` on the oplog; but multilite must also repair the **materialized SQLite
state** — un-apply the rejected writes from real tables. With an after-image-only value
codec (D4) there is nothing local to restore from: repair = re-pull authoritative state
for the affected keys (or ranges) from the server. Decide: re-pull repair (needs
connectivity — acceptable, rejection already implies connectivity) vs keeping local
before-images for offline-capable undo (reopens D4). Also decide the app-visible surface:
which error, on which call, and what "your last N transactions were undone" looks like.
`Status: open — leaning re-pull repair keyed off the rollback marker; D4 stays
after-image-only. The app-visible contract needs design (ties to the durability
watermark API, J5).`

**L5. Read-transaction stability under applies.** A long-running local read transaction
must keep a stable snapshot while sync applies remote ops. WAL mode gives readers
snapshot isolation natively, and applies queue behind write transactions (J3) — read
snapshots come free as long as applies run as ordinary write transactions.
`Status: decided — WAL reader snapshots (J4) + apply queuing (J3); no new machinery.`

---

## Resolution workflow

1. Walk one group at a time (brainstorm → decide → update statuses here).
2. An item leaves the document only as `decided` or `unsupported-v1` (with its
   adoption/DDL-time rejection specified).
3. Spike deliverables referenced above (E2 op table, G1/G3/G4 probes, H2 parser fork)
   are the first implementation artifacts — they inform decisions, not follow them.
4. Implementation begins when no group has an `open` item that blocks the wedge:
   adoption (A) → foreign-write guard (B) → single-table write path (G/H) → fixpoint (J).
