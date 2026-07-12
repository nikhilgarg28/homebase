# multilite — vtable architecture: design sketch + challenge inventory

*Working document · July 2026 · Companion to [DESIGN.md](./DESIGN.md) (single-file doctrine,
witness compiler, launch claims) and the admission/rollback v2 kernel plan.*

**Purpose.** Before implementing the SQL layer, inventory every known complexity of the
vtable-based interception architecture. We refine this document until each item is either
**satisfied by the design** or **explicitly unsupported**, then implement.

**Status legend:** `open` (undecided) · `leaning: …` (tentative) · `decided: …` (settled,
implement as stated) · `unsupported-v1` (rejected at adoption/DDL time with a clear error).

---

## Architecture sketch (current thinking — revised for hook-authoritative capture, C6)

1. **Real tables keep their user names in `main`** — reads run unmodified at native speed
   against real b-trees and indexes.
2. **Write statements also run natively** against the real tables — no write rewriting,
   so UPSERT, DEFAULTs, conflict modes, RETURNING, triggers, and cascades are all
   engine-native behavior.
3. **The preupdate hook is the authoritative write capture** (C6-b): every write that
   lands in the b-trees — including cascade and trigger effects — enters the
   per-transaction op buffer with old + new row values.
4. **Read-shadow vtables live in `temp`** under mangled names, created per connection at
   open. The rewriter retargets *reads inside write transactions* at them;
   `xBestIndex`/`xFilter` forward the scan to the real table and record structured WHERE
   constraints → range asserts. FK/constraint read dependencies (parent existence,
   uniqueness witnesses) are schema-derived from the captured write set — no capture
   layer sees them, in any architecture.
5. **Optional eager-lease mode** (N6): when the open mode says so, the assert-recording
   layer acquires leases at scan time instead of deferring everything to commit —
   pessimistic concurrency for apps that prefer blocking to aborting.
6. **At COMMIT** the wrapper runs the lease fixpoint over the buffer (validate coverage,
   acquire missing, barrier); on success the buffer materializes into `hb_oplog` rows in
   the same SQLite transaction (single fsync domain); on failure the whole transaction
   rolls back — real-table writes vanish with it.
7. **Metadata** (`hb_*` system tables in `main`): oplog, leases, cursors, device identity,
   codec caches, adoption record — the `MetaStore` trait implemented natively over SQLite,
   gated by the existing conformance suite in `client/src/meta.rs`.
8. **Fallback preserved:** the earlier write-vtable design (writes rewritten to shadows,
   `xUpdate` forwarding) remains the fallback if the C6 spike verifications fail
   (WITHOUT ROWID hook coverage, truncate-DELETE behavior). Group G's dialect gaps apply
   only in that fallback.

---

## Group A — Adoption and genesis

**A1. First-load adoption transaction.** Opening a vanilla SQLite file under multilite
creates the system tables (`hb_meta`, `hb_oplog`, `hb_leases`, …) and records the adoption
(device id, adoption timestamp, schema snapshot hash, per-table max rowids). Must be one
atomic transaction so a crash mid-adoption leaves either a vanilla file or a fully adopted
one — never half.
`Status: decided — single-transaction adoption; snapshot ship is a separate resumable saga.`

**A2. System-table naming and schema placement.** Naming: `_multilite_metadata` vs the
`hb_*` / `_hb_*` family — DESIGN.md already commits to `hb_*` (`hb_oplog`, `hb_leases`);
one prefix, everywhere, decided before first byte is written (part of the detach
contract and the exclusion manifest). **Placement: `main`, forced** — a "separate
multilite schema" means an ATTACHed database file, and **multi-database transactions
are not atomic under WAL** (the super-journal mechanism is rollback-journal-only; a
power loss mid-COMMIT can land one file's changes without the other's). J4 forces WAL
and the single-file doctrine requires rows + oplog in one atomic commit, so metadata in
an attached file would reintroduce exactly the torn state the architecture prevents.
In-file prefix namespacing is the only namespacing SQLite offers.
`Status: decided — hb_* prefix in main (reconcile _hb_ vs hb_ in DESIGN.md); attached-
file metadata rejected on WAL atomicity grounds.`

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
our commits; a foreign commit changes data without touching `hb_*`. **No single SQLite
property covers every write across journal modes persistently** — `data_version` is
live-only and volatile; the header change counter (offset 24) bumps per commit only in
rollback-journal mode (WAL: on checkpoint only); WAL salts/frame-count reflect every
WAL write but are transient (reset on clean close); schema cookie is DDL-only. The
composite that works:

- **Clean-close ritual (counter fixpoint).** The circularity — recording the counter is
  itself a write that changes it — resolves by making the final write deterministic:
  checkpoint (TRUNCATE) → switch journal mode to DELETE for the ritual → raw-read
  header counter `C` (plain file IO) → commit one final transaction writing
  `hb_meta.expected = C + 1` (rollback-mode commits bump exactly once — the file
  satisfies its own prediction; WAL file gone).
- **Open-time assertion:** raw-read counter; require `counter == hb_meta.expected` AND
  no/empty `-wal` file. Any interim foreign write either bumped the counter (rollback
  mode, or their checkpoint) or left WAL frames — both trip it.
- **Unclean close (crash):** the fixpoint never ran — fall back to WAL-consistency
  evidence + M2's `certify_sql` (oplog tail vs materialized rows) instead of a counter
  compare; "our crash" vs "our crash + foreign write" is not cheaply distinguishable
  here — the deep-verify CRC exists for when it matters.
- **Schema cookie** snapshot: cheap additional tripwire for foreign DDL specifically.
- **Content CRC** (per-table or per-space, TODO.md): definitive but O(data); explicit
  deep-verify mode.

Threat model note: this defends against *accidental* foreign writes (someone opens the
file with the sqlite3 CLI) — not an adversary who restores counters after tampering;
adversarial file tampering is the sync layer's domain (E2EE, ver chains, anchors).
`Status: leaning — clean-close ritual + open assertion + certify fallback as designed;
verify the exact counter-increment semantics of the ritual commit in the spike.`

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

**C6. Dual capture: preupdate hook alongside the vtable layer.** A preupdate hook on the
real tables sees *every* write that lands in the b-trees — forwarded writes, FK cascades,
trigger bodies, `OR REPLACE` implied deletes. Spectrum of roles:
- **(a) Verifier:** diff hook-observed writes against the vtable buffer at commit;
  mismatch = unanticipated bypass → hard error. Lifts no restrictions but converts
  capture completeness from faith into an invariant. Cheap; always-on in debug/CI.
- **(b) Authoritative write capture:** the oplog write set comes from the hook; the
  vtable layer is scoped to scan capture (E1-c/E2) and veto timing. **C1–C3 restrictions
  dissolve** — cascades and triggers enter the buffer as ordinary row ops and the commit
  fixpoint derives their leases like any other write. Bonus: the hook provides old column
  values (unlike xUpdate) — before-images nearly free (see L4).
- FK *read* dependencies (parent-existence lookups) are invisible to both layers and are
  schema-derived by the compiler in every architecture — capture never provides them.
Costs: `SQLITE_ENABLE_PREUPDATE_HOOK` compile flag (we vendor anyway); hook fires for our
own sync applies on the same connection → J2's re-entrancy guard becomes a hook-suppress
flag. Spike verifications: hook coverage of WITHOUT ROWID tables in the pinned SQLite;
hook presence disabling the truncate-optimized DELETE (which skips per-row callbacks).
`Status: decided (direction) — (b): hook-authoritative writes + vtable scoped to
read/scan capture and optional eager lease acquisition (N6), with the (a) diff kept as a
permanent debug/CI invariant. Conditional on the two spike verifications above; the
write-vtable design is the fallback if they fail. C1–C3 flip to supported under this
model (cascades/triggers are captured writes).`

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

**D8. Interception-layer type system.** The value representation shuttled between
`sqlite3_value` (engine boundary), forwarded statements, the op buffer, and the codecs.
Rule: mirror SQLite's five storage classes exactly — `Null | Int(i64) | Real(f64) |
Text | Blob` — and nothing richer; affinity is a *column* property the engine applies,
and the layer stays affinity-transparent. Load-bearing invariants:
- **Post-affinity capture:** buffer what the engine *stored*, not what the statement
  said. The preupdate hook guarantees this; the vtable path only if shadow column
  declarations match the real table exactly. Pre-affinity capture ⇒ replica divergence.
- **Fidelity:** capture → oplog → apply is bit-identical on every replica (the
  conformance matrix's byte-identical claim depends on it). Floats never round-trip
  through text.
- **Pinned edge cases, each a conformance test:** NaN stores as NULL (frame never
  carries NaN); `1` vs `1.0` equal in index comparisons ⇒ witness/key encoding needs
  numeric normalization across the int/real boundary (concretizes D3); `-0.0` vs `0.0`;
  i64 range vs f64 precision; TEXT ≠ BLOB (`'a'` ≠ `x'61'`) — storage class preserved,
  never coerced; UTF-8 pinned (reject/convert UTF-16 dbs at adoption).
- **Two codecs, separate:** value codec = versioned serial-type-tagged frame (fidelity);
  key codec = order-preserving tuple encoding with numeric normalization + collation
  transforms applied first (comparison semantics live here only).
- **FFI discipline:** copy `sqlite3_value`s into owned values at the capture boundary;
  no protected/unprotected lifetime cleverness.
`Status: leaning — as stated; the numeric-normalization rule for keys is the remaining
design detail (shared with D3).`

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
`Status: decided — (c) per C6: reads inside write transactions route through
read-shadows for structured capture; (a) authorizer table-level asserts remain the
escalation for anything the rewriter can't classify.`

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

**E6. Write-predicate capture under native writes (phantom protection).** The honest
cost of C6-b: `UPDATE`/`DELETE` statements run natively, so their WHERE scans no longer
pass through a vtable — the predicate *range* is uncaptured even though the touched rows
are (via the hook). Serializability against phantoms needs the range, not just the rows
(`UPDATE … WHERE status='pending'` depends on which rows were pending, including ones
that weren't). Options: (a) authorizer table-level assert on the written table — coarse,
correct, the v1 default; (b) companion probe-SELECT through the read-shadow reproducing
the write's WHERE clause — precise, costs a second scan and risks semantic drift from
the real statement; (c) opt-in write-vtable mode for hot tables that need precision.
`Status: decided (v1) — (a): write statements forward natively with a table-level
assert on the written table; finer predicate capture ((b)/(c), with E2's mapping table)
is a post-v1 refinement triggered by multi-writer contention, not a wedge requirement.`

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

*Under the revised sketch (C6-b: native writes), G1–G4 are moot — statements run
engine-native. They re-activate only if the write-vtable fallback is needed. G5–G7 apply
regardless (the op buffer exists in both models).*

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

**H1. Shadow naming and resolution.** Shadows live in an **attached in-memory schema**
(`ATTACH ':memory:' AS hb` at open — per-connection, never touches the file) under
**identity names**: `hb.orders` shadows `main.orders`. Safe because unqualified name
resolution runs **temp → main → attached**: attached-last means identity names never
capture reads accidentally (temp-schema-same-name would — temp wins resolution — which
is why that variant stays rejected). Rewriting becomes schema *qualification*
(`FROM orders` → `FROM hb.orders`): locating the target reference is the same parser
work as renaming, but the bookkeeping disappears — no mangled-name map, no collision
risk with user names, sane error messages and introspection. Shadows cannot live in
`main` (their CREATE VIRTUAL TABLE entries would persist in sqlite_master as
unknown-module junk — K1 violation). The `hb` schema name is reserved (H3).
`Status: decided — attached-memory schema + identity names + qualification rewriting.`

**H2. Rewriter scope.** Target-table renaming is shallow; G1 (upsert translation) and G2
(default expansion) are not. Decide the parsing substrate once: hand-rolled tokenizer vs a
real SQLite-dialect parser (and its round-trip fidelity), vs preparing the statement and
using SQLite's own analysis to locate the target.
`Status: open — biggest implementation-cost fork in the layer.`

**H3. Statements that bypass the rewriter.** `ATTACH`ed databases, `PRAGMA`s with side
effects (`journal_mode`, `wal_checkpoint`, `writable_schema`!), `VACUUM`, `REINDEX`,
`ANALYZE`. Each needs a stance: passthrough / intercepted / forbidden. User `ATTACH …
AS hb` is rejected — the shadow schema name is reserved (H1).
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

**I6. DDL and the rollback window.** Surgical DDL undo is real machinery (see I7 — it
is *tractable*, not free), so v1 avoids needing it at all. Two regimes:

- **Forever db-level W lease held (single-writer default): DDL allowed offline**, enters
  the oplog like any write. Justification: under forever-W whole-db coverage, no
  surgical rollback scenario exists *for anything* — no assert can fail, no contention,
  no ver regression; the only push rejections are **fence** (takeover) and **fork**,
  both wholesale events where the entire unacked tail is forfeit and recovery is
  re-bootstrap, never `rollback(to)`-and-continue. Offline DDL therefore carries no
  incremental risk over offline data writes, which the lifecycle already accepts. The
  discarded-tail export covers the human side — DDL exports as literal SQL text.
- **Anything less** (bounded lease — offline expiry makes rejection *routine*, not
  exceptional; partial-prefix coverage; OCC/`submit_unchecked` multi-writer):
  **sync-barrier DDL** — empty unacked oplog (push first) + synchronous push before
  returning; the v2 kernel's Sync-mode `PendingOps` discipline applied one level up.
  Offline DDL errors. Refinement that eliminates DDL undo here too: **push the schema
  record first, execute locally on ack** — concurrent DDL loses the ver race on the
  schema key and fails cleanly to the app with nothing local to unwind; crash between
  ack and execute = resumable saga (schema cursor vs oplog).

Either way v1's `hb_undo` (L4) never needs schema pre-images, and authorization is lease
coverage, not asserts — a held db-level W lease is strictly stronger than any assert.
Escape hatch for pathological cases: re-bootstrap from server snapshot (M1 path).
I7's invertible DDL, when built, relaxes the sync-barrier regime (offline DDL for
bounded/partial-lease tiers too).
`Status: leaning — two-regime policy as stated for v1; the regime check is a
lease-coverage inspection at DDL time. Error surface for the sync-barrier regime still
to design.`

**I7. Invertible DDL (post-wedge upgrade).** Every DDL kind has a practical inverse
under **reverse-order undo** (each statement undone against the schema state it
created):
- `CREATE TABLE` → drop (its row inserts are undone first). `CREATE INDEX`/`DROP INDEX`,
  views, trigger DDL → SQL text both ways (index data derivable; rebuild on undo).
- `ADD COLUMN` → `DROP COLUMN` (3.35+); later DDLs that would block the drop are undone
  first by reverse order. `RENAME` → rename back.
- `DROP COLUMN` → `ADD COLUMN` + restore from a `(rowid, value)` snapshot captured at
  drop time into `hb_undo` — O(table), same order as the drop itself.
- `DROP TABLE` → **rename-defer**: rename to an `hb_trash_*` name at drop time (O(1)),
  rename back on undo, real drop on ack.
Known complications, all bounded: **(1) index/trigger names are schema-global** — a
trashed table's indexes/triggers keep their names and would collide with a same-name
recreate, breaking *forward* behavior; fix: drop them at rename time, record their SQL,
rebuild on undo. **(2) FK fidelity** — a real DROP TABLE on a parent with live child
rows errors; a rename instead rewrites children's FK clauses and succeeds; multilite
must pre-check and raise the drop's error. **(3) Trash GC** — real drop on ack; crash
between ack and drop needs an idempotent `hb_trash_*` sweep at open; trash lives in the
`hb_` namespace so K1 detach fidelity and sync exclusion hold by contract.
Payoff: relaxes I6's sync-barrier — offline DDL for bounded/partial-lease tiers, not
just forever-W holders. Staging: sync-barrier everywhere first; rename-defer DROP TABLE
next (biggest win, O(1)); remaining kinds after.
`Status: leaning — design as stated, sequenced post-wedge; not a v1 requirement.`

**I8. DDL data-plane effects (v1 walkthrough discoveries).** Mechanics per I1/I3/I6:
first-token intercept; **DDL standalone-transaction-only** (never mixed with DML in a
user BEGIN — v1 restriction); execute natively; record as a `Set` on a **reserved
schema key** (`(schema-prefix, counter++)` → SQL text, encrypted, ver-chained); replay
on replicas is deterministic (SQLite itself forbids non-constant ADD COLUMN defaults).
**Admission-time drift guard:** every data batch carries a range assert on the schema
prefix at the local schema cursor — stale-schema writers auto-reject at admission
(upgrades I4's apply-time hash check). Two data-plane consequences:
- **DROP TABLE leaves kernel garbage.** Replicas replay the drop locally, but the
  kernel still holds every row as live KV — permanent garbage, and fresh bootstraps
  download rows with no table to land in. Resolved by kernel **DeleteRange** (in
  progress, will land): DROP TABLE emits DeleteRange over the table prefix in the same
  batch. Mass tombstones remain only as a contingency if DeleteRange slips.
- **Schema evolution vs stored frames.** After destructive DDL (DROP/RENAME COLUMN),
  live replicas are fine but kernel value frames keep the old layout — a fresh
  bootstrap must decode old frames under new schema. Options: (a) re-emit all rows
  after destructive DDL (eager rewrite, O(table)); (b) schema-versioned frames decoded
  against the DDL chain (materializer becomes history-dependent); (c) **v1 additive-only
  DDL** — CREATE TABLE / ADD COLUMN / CREATE+DROP INDEX supported; destructive DDL
  rejected or sync-barrier + re-emit.
`Status: leaning — (c) additive-only for v1 with (a) as escape hatch; schema-prefix
assert on every batch decided; DeleteRange landing in kernel (unblocks DROP TABLE's
data plane — the frame-evolution question for DROP/RENAME COLUMN remains the reason
for additive-only). Namespace/lease placement of the schema prefix (outside (User,)?
covered by what?) still open.`

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

**J6. MetaStore-on-SQLite: connection and transaction discipline.** The oplog append
must join the *user's open transaction* (single-file doctrine: rows + oplog entry = one
commit), while engine transitions (trim on ack, watermark, lease churn) run standalone.
Resolution: the store holds the shared connection and **every trait method wraps its
work in a SAVEPOINT** — context-adaptive: outside a transaction it is its own atomic
transaction (RELEASE commits); inside one it nests. The kernel client never manages
transactions — joining the ambient user transaction is multilite's commit choreography,
invisible through the trait. Consequences: engine transitions queue behind open user
transactions (same discipline as J3 applies) on the single write connection (H4); a
second hb_*-only connection is the later concurrency upgrade (B2 whitelist cost). The
impl must pass the existing MetaStore conformance suite + fault-injection tests.
`Status: decided — savepoint-per-method; single connection with queued transitions for
v1.`

**J7. Durability policy: WAL + synchronous=NORMAL + fsync-barrier-before-push.** Never
fsync manually — SQLite owns durability via `PRAGMA synchronous`. Defaults: rollback
journal + FULL ≈ 2 fsyncs/commit; WAL + FULL = 1; **WAL + NORMAL (production standard)
fsyncs only at checkpoints** — app crash loses nothing; power loss loses recent commits
but never corrupts. Why NORMAL is correct here: **losing unpushed tail commits is safe
by construction** — user rows and oplog entries vanish *together* (one transaction), the
file stays consistent, and the server never saw those seqs (reuse harmless; it is the
documented 0-RTT local durability contract). **Losing a pushed seq is fatal**: oplog
tail regression → device seq reuse → server `DeviceSeqRegression` → false **fork**. The
one hard rule: **the push loop fsyncs the WAL through the batch before sending it** —
one fsync amortized per push, write-ahead-of-push not write-ahead-of-return. Per-commit
FULL is the opt-in durability tier (N-group connection-string option), not a correctness
requirement.
`Status: decided — as stated; implement the pre-push barrier as a store-level primitive
(sync_through(seq)) so the push loop cannot forget it.`

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

**L2a. The v1 isolation claim, precisely.** With table-level asserts on both the read
set (E1-a authorizer) and write set (E6-a) of every write transaction, validated in
admission order, v1 multi-writer is **serializable via backward-validation OCC at table
granularity** — *stronger than snapshot isolation*: read-set validation kills write
skew (SI admits it; our second txn's read-table assert fails), and table coarseness
subsumes phantoms (a whole-table assert covers every predicate). The price is spurious
aborts (L2), never anomalies. Qualifications: (1) read-only / local-tier transactions
are **serializable-but-stale** — they order at their snapshot's admission cut; strict
serializability (recency) is the consistent tier's job (1-RTT read_at, N4); (2) the
claim attaches to the **admitted global history** — locally-committed-but-unacked
transactions are visible locally and may later roll back (L4); per-device views stay
self-consistent. Single-writer default is trivially serializable (one writer + replay).
Note the inversion: DESIGN.md's documented write-skew hole belongs to the finer-grained
witness-compiler model with incomplete read capture; v1's assert-everything coarseness
*closes* it — the precision roadmap (E1-c/E6-b) must reduce aborts without reopening it.
`Status: decided — this is the claim the physics doc states for v1; conformance/torture
tests should encode write-skew and phantom kill cases explicitly.`

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
pushed batch (assert failure, lease loss, fenced takeover), the v2 kernel answer is
caller-driven `rollback(to)` on the oplog; multilite must also repair the
**materialized SQLite state** — un-apply the rejected writes from real tables.

**Design: logical undo log (`hb_undo`).** The post-image already exists (the oplog
entry); only the pre-image is new, and the preupdate hook provides old column values for
free (C6). `hb_undo` rows — pre-images keyed by `(device_seq, key)` — are written in the
same transaction as user rows + oplog entry (one fsync domain) and pruned on ack: the
undo window is exactly the unacked window, since `rollback(to)` only targets
`[neck, tail)` and acked batches never roll back. Restore-to-`to` walks the rolled-back
suffix; per key, the pre-image from the *earliest* rolled-back batch wins. Repair writes
bypass capture via the apply-path suppression flag (J2). Works offline (no re-pull
dependency) and directly enables LAUNCH_CHECKLIST's "optional export of discarded tail"
in the fenced-takeover flow. D4 stays after-image-only on the wire — pre-images are
local-only rows, never shipped.

Fallbacks: re-pull affected ranges (needs connectivity) or full re-bootstrap (M1 path).

**v1 decision: no undo log in either variant.** Variant A: surgical rollback never
fires (fence/fork → re-bootstrap; the discarded-tail export needs only oplog
post-images, not pre-images). Variant B (OCC): repair is required but **re-pull repair
suffices** — rejection happens at push = online by definition; repair = for every key
in the discarded suffix, server point-read → overwrite or delete locally under apply
suppression; inputs are oplog post-images + server reads, no pre-images. `hb_undo`'s
unique additions — zero-RTT repair and **offline-initiated abandon** ("discard my
unsynced changes" without connectivity) — are the fast-follow triggers.

**Layer placement (revised): split ownership, client-level per-key pre-images.**
Pre-images are KV-native, and **restore reuses the existing apply path** — the client
replays stored pre-image entries through the same pipeline that materializes server
deltas (decode frame → suppressed row write). Property gained: *any consumer that can
sync can roll back*, with zero consumer-specific restore machinery. Split: **multilite
captures + encodes** (preupdate old values → value frames, supplied with the batch at
commit); **client stores + orchestrates** (pre-image records in MetaStore, atomic with
the oplog record via the J6 savepoint — physically still an hb_ table; the layering
question is API ownership, not byte placement); **restore = client emits pre-images
through the consumer's apply path**. Plaintext frames are fine locally (N1). Kernel
cost when built: MetaStore trait pre-image extension + conformance coverage (v2
backlog item).

**DeleteRange is the leak in per-key undo** — its pre-image is every live pair in the
range (O(table) capture; no rename exists at the KV layer). Resolution: **barrier
discipline** — destructive DDL is sync-barrier'd (I8 additive-only v1), and
sync-barrier ops never sit in the unacked window, which *is* the undo window — so
DeleteRange never needs undo in v1. If offline destructive DDL ever lands (I7), the
DeleteRange undo record becomes a **reference to the consumer's artifact** (the
rename-deferred trash table is the O(1) pre-image) — inherently consumer-assisted;
ranges are where pure-KV undo genuinely leaks.

**MetaStore under rollback: atomicity, not undo.** MetaStore state is designed to move
forward only — oplog rollback is a marker append + cursor advance (v2, dead rows until
trim); `ver_high` never rewinds (Lamport: gaps legal, rewind invites reuse bugs); pull
cursors/watermarks track server state, untouched; schema state never participates
(barrier discipline, I6/I8); consumed pre-images are deleted as cleanup. Requirement:
marker + cursors + restored rows + pre-image cleanup = **one SQLite transaction** (J6
savepoints), restore writes under J2 suppression; crash mid-rollback = transition never
happened, re-detected and re-run idempotently off the marker. **Flagged for the kernel
plan:** discarded `Release` ops — v2 retires leases locally at commit; a rolled-back
release never shipped, leaving the lease live server-side but locally forgotten (TTL
reclaims bounded; same-device refresh recovers forever leases) — needs an explicit
story, not an accident that works.

DDL undo is avoided in v1 by regime (I6) and arrives later via invertible DDL (I7).
`Status: decided — v1 ships re-pull repair; hb_undo fast-follow in the multilite layer.
The app-visible contract ("your last N transactions were undone": which error, on which
call) still needs design (ties to J5/N5).`

**L5. Read-transaction stability under applies.** A long-running local read transaction
must keep a stable snapshot while sync applies remote ops. WAL mode gives readers
snapshot isolation natively, and applies queue behind write transactions (J3) — read
snapshots come free as long as applies run as ordinary write transactions.
`Status: decided — WAL reader snapshots (J4) + apply queuing (J3); no new machinery.`

---

## Group M — Local corruption and self-consistency

Distinct from Group B (foreign writes are *well-formed* SQLite commits by someone else;
this group is about the file or our own bookkeeping going bad).

**M1. SQLite-level file corruption.** Torn pages, bad checksums, filesystem damage.
Detection: `PRAGMA quick_check` at open (cheap) vs full `integrity_check` (O(db), behind
the deep-verify flag alongside B1's CRC). Response: corrupted file → refuse sync; local
reads at the app's own risk; recovery = re-bootstrap from server snapshot + tail (the
replica-restore path from LAUNCH_CHECKLIST), which multilite gets for free once bootstrap
exists.
`Status: leaning — quick_check at open, integrity_check in deep-verify mode; recovery =
re-bootstrap, never in-place repair.`

**M2. hb_* metadata vs data divergence.** The single-transaction invariant (user rows +
oplog entry commit together) can still be violated by bugs: buffer/savepoint shear errors
(G6), apply-path mistakes, rollback-repair (L4) partial application. Needs a
recomputation oracle for the SQL layer — the `certify` discipline extended: cursors
within bounds, oplog vers consistent with `ver_high`, spot-check that oplog entries
re-encode from current rows where they should (sampled, not exhaustive). Run at open;
full mode in CI/torture.
`Status: open — design certify_sql alongside the existing meta::certify; decide the
sampled vs exhaustive split.`

**M3. Local vs server divergence.** Local materialized state disagrees with what the
server holds under our own acked seqs (bug, M1 damage that slipped through, bad rollback
repair). TODO.md's per-space/per-client CRC idea is the detection primitive: server
maintains cheap rolling checksums per space (or per prefix), client compares at sync
checkpoints; also catches the re-pushed-older-seq case. Costs a kernel feature — decide
whether it's a multilite requirement or deferred hardening.
`Status: open — leaning deferred to post-v1 hardening; until then divergence is caught
only by ver-monotonicity rejections and the sim/torture rigs.`

**M4. Response policy and observability.** One coherent story across B (foreign writes),
M1–M3: a health state on the connection (`ok / suspect / invalid`), surfaced via an API
and connection-string policy for what `suspect` does (warn-and-continue vs read-only vs
refuse-open). Every detector above feeds the same state machine rather than each
inventing its own error.
`Status: leaning — single health state machine in hb_meta + open-time report; exact
policy knobs open.`

---

## Group N — Deployment and policy option space

The knobs an app (or connection string) can set. Each needs a default that matches the
"drop-in SQLite" claim: working defaults, escalations opt-in.

**N1. Local file: plaintext vs encrypted at rest.** E2EE (DESIGN) covers the wire and
server; the local file is a separate decision. Options: plaintext file (default —
detach-with-stock-sqlite works, C-ABI claim intact); OS-level disk encryption (free,
recommended posture); SQLCipher-style page encryption (breaks detach with stock
libsqlite3, complicates the C-ABI tier, third-party dependency); store-level wrapper
(TODO.md's revisit item). At-rest encryption also interacts with B/M detection (header
counters move differently under page encryption).
`Status: leaning — plaintext file + documented OS-encryption posture for v1; page-level
at-rest encryption explicitly out of scope until after launch, revisited as a wrapper.`

**N2. Space key delivery at open.** `multilite.open(url, key)` — what is `key`? Options:
raw space key material; a serialized `SpaceEnvelope` (name key + space keys — the
DESIGN-committed shape); a keystore/enclave callback (key never in argv); nothing
(plaintext spaces via `SpaceEnvelope::Plaintext`). Interacts with the identity-spec
reconciliation (TODO.md): `Client::open`'s enclave param becomes the envelope/keystore
source. Also decide: key in the connection string is forbidden (logs) or allowed for dev.
`Status: open — leaning SpaceEnvelope-or-callback as the two blessed forms; raw-key-in-
URL dev-only behind an explicit unsafe flag.`

**N3. Pull cadence and staleness bounds.** How does the local replica advance, and how
stale may it get before reads are affected? Dimensions: pull trigger (live subscription /
interval poll / on-demand only); staleness enforcement (none — always readable, staleness
observable; warn; refuse reads beyond bound T); scope (per connection vs per table).
Refusing stale reads trades away offline availability — the whatsapp-lifecycle default
must stay always-readable, with strict staleness an opt-in pragma for apps that prefer
unavailability to stale answers. Note the interplay: an enforced staleness bound is a
*freshness lease on reads* and needs a clock story offline.
`Status: leaning — default: continuous pull while connected, always-readable offline,
staleness observable via API (N5); optional max-staleness pragma refusing reads with
SQLITE_BUSY-offline semantics. Bound semantics (wall clock vs admission-seq lag) open.`

**N4. Freshness-on-demand APIs.** Can a read be *made* fresh? Surfaces: the strong-read
pragma (route through the consistent tier — a real read_at cut, L1); an explicit
`refresh()` / sync-barrier call (pull until local cursor ≥ server head as of the call,
return the achieved seq); per-query escalation (attach freshness to one statement rather
than the connection). All three are policy over the same pull machinery — decide which
ship v1 and their blocking/timeout semantics offline.
`Status: leaning — refresh() barrier + connection-level strong-read pragma for v1;
per-statement escalation later. Offline: both fail fast with the offline error, never
block indefinitely by default.`

**N5. Staleness observability.** The read-side counterpart of the durability watermark
(J5): expose last-synced admission seq and wall-clock age (`hb_status()`: local cursor,
server head if known, lag, health state from M4). Cheap, ships v1, and is what makes
N3's "always readable" default honest.
`Status: decided — status API alongside the watermark API.`

**N6. Lease acquisition mode.** When does the fixpoint acquire? **Optimistic (default):**
everything deferred to COMMIT — validate the buffer, acquire missing, abort + retry on
conflict; zero network inside the transaction. **Eager (opt-in, open-mode or per-
transaction via `BEGIN IMMEDIATE`):** the assert-recording layer acquires leases at scan
time — blocking beats aborting, at the cost of network RTTs mid-transaction (holding the
SQLite write lock across them) and longer lease hold times. Interacts with L1 (this *is*
the pessimistic escalation) and J1 (eager mode shrinks the commit fixpoint to
validation). Barrier handling in eager mode needs care: an acquire's barrier may demand
applying remote ops while the user transaction is open — likely: eager mode acquires
but defers barrier catch-up rows to the retry path, same as optimistic.
`Status: open — leaning optimistic default + eager as connection-string option mapped
onto BEGIN IMMEDIATE; the eager-mode barrier question is the design detail to resolve.`

**N7. Two-method query API: `query_read` vs `query`.** `query_read` **fails on any write
statement** and in exchange forwards everything immediately to the real tables — zero
interception cost, no rewriter, no buffer, a declared-intent contract. `query` (default)
handles the general case. Mechanism: `sqlite3_stmt_readonly()` after prepare — no SQL
parsing. The same primitive is the default path's rewriter-engagement rule: readonly
statements outside a write transaction pass through untouched, so interception cost is
paid only by writes and by reads inside open write transactions. Edge cases to pin:
`stmt_readonly` returns true for BEGIN/COMMIT/SAVEPOINT (accept in query_read, or reject
for strictness); write-flavored PRAGMAs; temp-table writes. **Connection semantics
decision:** query_read on the writer's connection sees uncommitted local transaction
state (read-your-writes); on a dedicated read-only WAL connection it gets snapshot
isolation and never blocks behind the writer, but must be whitelisted by B2's
data_version detector and reopens H4. Ties to L1 (query_read = the local tier surface)
and N3/N4 (freshness options naturally attach to query_read).
`Status: leaning — ship both methods v1; stmt_readonly as the gate; same-connection
semantics for v1 (least surprise, no H4 complications); dedicated read connection as a
later concurrency upgrade.`

---

## Group O — Conformance-first testing (build against the real SQLite suite)

Strategy: stand up the test harness *before* the engine, and let the suite drive
development as a fidelity ratchet. One refinement over "start failing, make tests pass":
start **passthrough-green**, not red — a pure passthrough build should pass nearly
everything on day one, and every interception layer added must *keep* it green. Green→
green catches regressions the moment a layer breaks dialect fidelity; red→green can't
distinguish "not built yet" from "built wrong".

**O1. Walking skeleton + SLT harness first.** Build the minimal public API — `open(path)`
returning a connection, `execute`/`query` — as a *pure passthrough* to vendored SQLite
(no adoption, no capture, no vtables). Implement the `sqllogictest` crate's driver trait
over it and run the vendored, pinned SQL Logic Test corpus. Deliverables: the harness in
CI, the day-one pass/fail baseline, and the first draft of the exclusion manifest
(families that fail even on passthrough — build/flag artifacts, not our physics).
`Status: decided — this is the first implementation artifact of multilite, before any
interception code.`

**O2. Layer-by-layer ratchet.** Insert layers one at a time, each behind a build/open
flag, re-running the full corpus after each: (1) adoption (A) — hb_* tables exist, suite
must stay green; (2) preupdate capture + op buffer (C6/G6) — capture-only, no sync,
green + the C6-a hook-vs-buffer diff and M2 certify_sql run after every script; (3)
read-shadow rewriting (E1-c) — the layer most likely to break fidelity, watched by the
same green bar; (4) fixpoint + local-only lease stub; (5) real sync against an
in-process homebase server. A family that goes red names its layer immediately.
`Status: decided — flag-gated layers; corpus green is the merge bar per layer.`

**O3. Exclusion manifest as a living deliverable.** Every excluded family carries a
one-line reason tagged **physics** (documented behavior: dense-rowid asserts under F2,
`hb_*` visible in schema dumps, journal-mode pragmas we own, forced WAL under J4) or
**debt** (should pass, doesn't yet — must trend to zero). The manifest starts at O1 and
ships with the launch claims (DESIGN.md already commits to publishing it).
`Status: decided — manifest format: family, tag, reason, owner item in this doc.`

**O4. The matrix comes later.** The full conformance matrix (topology × encryption ×
durability, byte-identical across cells, mid-suite adoption of a vanilla file, detach
fidelity job, testfixture build) is the launch gate, not the development loop. v1 CI
runs three cells: passthrough, adopted+capturing (no server), synced (in-process
server). Cross-cell byte-identity checks start when cell 3 exists.
`Status: decided — three cells during development; full matrix per LAUNCH_CHECKLIST.`

**O5. What SLT does not cover.** The corpus is read-heavy single-connection SQL — it
exercises dialect fidelity, not sync. It will not catch: rollback repair (L4), foreign-
write detection (B), staleness policy (N3), crash recovery (M), multi-writer contention
(L2). Those get the kernel's own disciplines: the differential harness (script ± LEASE
lines), crash-injection torture reusing the DST rigs, and dedicated integration tests.
Do not let a green SLT bar masquerade as sync correctness.
`Status: decided — SLT = dialect fidelity only; sync correctness has its own rigs.`

---

## Appendix — v1 flows (table-level asserts) and build list

Two lease postures, same flows except where marked:
- **Variant A (DESIGN default):** forever-W db lease auto-acquired at open; asserts never
  fail in steady state; rejection = fence/fork = re-bootstrap (I6/L4).
- **Variant B (pure OCC, no lease):** asserts carry all correctness, shipped via the
  kernel's **`submit_unchecked` mode** (no lease check on writes — already exists).
  Assert failure is *routine* → `hb_undo`, rollback repair, and the app retry surface
  move **into the wedge**.

**Open:** connection (WAL+NORMAL, busy_timeout) → `ATTACH ':memory:' AS hb` → register
authorizer + preupdate hook + commit interception → adoption (A1) or B1 assertion + M4
health → `SqliteMetaStore` → kernel `Client::open` (certify) → push/pull loops.

**Read:** prepare → `stmt_readonly` → execute natively. No capture, no asserts; local
tier, serializable-but-stale (L2a). Overhead ≈ one readonly check.

**Write:** (statement time) authorizer records read-set tables at prepare; preupdate
hook appends `(table, op, rowid, old, new)` to the op buffer; savepoint shear (G6/G7).
(COMMIT, wrapper-intercepted, fully local) fixpoint: encode rows (D8/D4, keys D1–D3),
stamp vers from meta high-water, build table-level `RangeAssert`s for read∪write tables
at the replica cursor → `MetaStore::commit` joins the txn (J6) (+ `hb_undo` pre-images
in variant B) → real COMMIT: rows + oplog one fsync domain (J7). 0-RTT return.
(push loop) `sync_through(seq)` barrier → unacked oplog coalesced as one multi-batch
`PutBatchRequest` → server: seq fence, asserts (`effective_prefix_max == at`), vers
(+ lease in A), all-or-nothing apply → ack: trim/watermark/cursors (queued behind user
txns). Rejection: A = fence/fork → export tail, re-bootstrap; B = routine → hb_undo
repair + app retry surface (L4).

**Pull/apply:** `read_at` deltas → decode → apply txn queued behind user txns (J3) with
hook suppression (J2), cursor in same txn; B2 whitelists own applies.

**DDL:** first-token intercept (I1); regime per I6 (A: offline OK; B: sync-barrier).

**Build order:** (1) wrapper: classification, query/query_read, txn state machine +
COMMIT intercept; (2) hooks + buffer + savepoint shear; (3) codecs; (4) SqliteMetaStore
passing the existing conformance suite; (5) adoption + B1 ritual; (6) fixpoint wired to
kernel push/ack (v2 machinery); (7) apply path; (8) hb_status. Variant B adds (9)
hb_undo + repair + retry, plus the kernel admission-mode decision.
**Not in v1:** rewriter, vtables/read-shadows, eager leases, LEASE statements, witness
compiler, partitions, invertible DDL, shapes.

**Subtleties:** (i) **own-pipeline asserts — handled by the kernel**: range-assert
evaluation excludes the asserting device's own writes (foreign seqnums only), so
successive commits touching the same table never self-collide; no coalescing or
single-outstanding-push constraint needed. (ii) **Variant B couples abort rate to pull
freshness** — N3's cadence becomes the OCC abort-rate dial, not a UX knob.

---

## v1 corner-cut register

Every accepted v1 simplification, by dimension. Three kinds: **precision** cuts (safe,
degrade concurrency/freshness), **scope** cuts (loud rejection at adoption/DDL),
**contract** cuts (documented weaker promises). None is a silent-corruption risk —
defend that property as implementation pressure mounts.

**Consistency & isolation (precision):** recency — local reads serializable-but-stale,
strictness only via refresh()/strong-read (L2a/N4) · revocable reads — unacked writes
visible locally, guarantee attaches to admitted history (L2a) · table-coarse conflicts →
spurious aborts (L2) · no per-statement freshness (N4). Note: isolation *level* is not
cut — v1 is serializable, stronger than SI (L2a).

**Replication scope (precision):** full-space replica, single cursor — no shapes/sparse
ranges (L3) · simple pull cadence, no subscriptions (N3).

**SQL surface (scope):** additive-only DDL (I8) · DDL standalone + sync-barrier outside
forever-W (I6/I8) · custom collations rejected in indexed positions (D2) · UTF-16
rejected (D8) · expression/partial indexes escalate/reject (D5) · virtual generated
columns excluded from frames (G5) · composite WITHOUT ROWID pending hook spike (F4/C6) ·
pre-existing vtables passthrough-unsynced (A7) · AUTOINCREMENT open (F1).

**Concurrency machinery (precision):** one write connection (H4) · applies queue behind
open txns (J3) · optimistic only, no eager leases (N6) · no LEASE
statements/partitions/RBAC (D7) · **no witness keys — safe only while asserts are
table-coarse; the E1-c precision upgrade and witness machinery must land together**
(C5/D5) · no read-shadows (E1-c/E6).

**Durability (contract):** WAL+NORMAL — power loss may eat unpushed commits
(safe-by-construction, documented) (J7) · per-commit durability and remote-sync commit
are later opt-ins.

**Repair & recovery (contract):** re-pull repair only — no hb_undo, no offline abandon
(L4) · no invertible DDL (I7) · foreign write → warn + refuse sync, no recovery (B4) ·
divergence CRC deferred (M3) · app-visible rollback contract undesigned (L4 open).

**Security posture (contract):** plaintext local file (N1) · no key rotation (epoch 0)
· no padding — lengths leak (D6) · key delivery: envelope-or-callback only (N2).

**Packaging & adoption (scope):** Rust crate only — no C-ABI/Python · three CI topology
cells (O4) · adoption hard-rejects unsupported schemas (A5) · forced WAL at adoption
(J4) · ~2× file size until first sync if A3 = materialize.

---

## Resolution workflow

1. Walk one group at a time (brainstorm → decide → update statuses here).
2. An item leaves the document only as `decided` or `unsupported-v1` (with its
   adoption/DDL-time rejection specified).
3. Spike deliverables referenced above (E2 op table, G1/G3/G4 probes, H2 parser fork)
   are the first implementation artifacts — they inform decisions, not follow them.
4. Implementation begins with the O1 walking skeleton (passthrough + SLT harness), then
   proceeds when no group has an `open` item that blocks the wedge: adoption (A) →
   foreign-write guard (B) → hook capture + buffer (C6/G6) → read-shadow asserts (E) →
   fixpoint (J), each layer ratcheted per O2.
