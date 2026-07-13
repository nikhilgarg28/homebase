# DeleteRange Design

Status: proposed design, no implementation yet.

This document defines scalable prefix deletion for the homebase kernel. It is
deliberately separate from the current Set/Delete implementation because a
range delete is not merely a compact list of point deletes. It changes point
visibility, range history, lease overlap, replication, version floors,
aggregates, compaction, and client recovery.

The first implementation targets tuple-prefix ranges and the full space. It
does not target arbitrary half-open key intervals.

## Goals

- Delete every currently visible key under a prefix in one admitted mutation.
- Avoid enumerating every covered key during admission.
- Preserve ordered Set/Delete/DeleteRange semantics within one batch.
- Make point reads, list, and exact admission-log replay agree exactly.
- Preserve causal range assertions and lease barriers.
- Keep `live_count` exact without eagerly rewriting every descendant record.
- Preserve per-key version monotonicity after old values become hidden.
- Keep `DeviceChecksum` sensitive to the complete ordered mutation stream.
- Keep the homebase client ops-only; materialized replica state belongs to its
  consumer.
- Replicate the complete dense admission log per client space while retaining
  arbitrary stateless server range reads.

## Non-goals

- Arbitrary byte intervals in the first version.
- Physically erasing covered ciphertext during admission.
- Immediate garbage collection of hidden point records.
- Hiding range-delete events from raw changefeed consumers.
- Applying pulled mutations to a consumer-owned database inside homebase.
- Treating a stateless range read as client replication progress.
- Byzantine-server fork consistency without an external witness.

## Terminology

`DeleteRange(R)` admits a range tombstone at admission sequence `S`.

An admitted mutation is ordered by
`AdmissionOrder { admission_seq, op_index }`, where `admission_seq` identifies
the admitted client batch and `op_index` is the mutation's
zero-based position inside that batch. Public barriers and range assertions
continue to use the batch-level `AdmissionSeq`; point/range visibility and
admission-log ordering use the full order.

For prefix ranges, two ranges overlap if either prefix covers the other:

```text
overlaps(A, B) = A covers B || B covers A
```

An ancestor tombstone covers a queried descendant. A descendant tombstone is
a historical change beneath every queried ancestor.

The full-space range covers every key and prefix. Empty user key components
remain invalid; full-space deletion is represented explicitly rather than by
an empty `Key`.

## Public Mutation Shape

The intended low-level mutation vocabulary is:

```rust
pub enum Mutation<T = Vec<u8>> {
    Set { key: Key, value: T },
    Delete { key: Key },
    DeleteRange { range: Range }, // Range::Prefix or Range::Full
}
```

`DeleteRange` is a data mutation, not a rollback marker and not a control
verb. It receives a normal `DeviceTag`, a `Seal`, a `DeviceSeq`, and an
admission sequence. Its seal authenticates empty plaintext with a distinct
operation kind and binds the anonymized range, device, device sequence, ver,
and cipher epoch in AAD.

Methods that currently assume every mutation has a point key must become
explicit about their target internally:

```rust
pub enum MutationTarget<'a> {
    Key(&'a Key),
    Range(&'a Range),
}
```

`MutationTarget` is an implementation helper, not another public operation
vocabulary. There should be no fake representative key for a range mutation.

## Core Visibility Rule

For key `K`, let `covering_delete(K)` be the newest admitted range tombstone
whose range covers `K`.

A stored point Set is visible exactly when:

```text
point is Set
and
point.admission_order > covering_delete(K).admission_order
```

A point Delete is never visible. A later Set revives a key after a range
delete. Hidden point records remain stored until a separate compaction rule
proves they can be removed safely.

This rule is evaluated at the actor's current atomic cut for materialized
`get` and `list` reads.

## Ordered Admission Semantics

Mutation order is observable:

```text
DeleteRange(db), Set(db/a)  => db/a is present
Set(db/a), DeleteRange(db)  => db/a is absent
```

The same applies across coalesced `AdmissionBatch` values. Range assertions
for a batch are evaluated immediately before that batch, after all preceding
batches in the request.

All mutations in one batch currently share an `AdmissionSeq`, so sequence
alone cannot encode `DeleteRange, Set` versus `Set, DeleteRange`. Every
admitted point and range record therefore carries `op_index`; admission-log keys
sort by `(admission_seq, op_index, target)`. Using `ver` as a tie-break is not
valid because the server does not require unrelated targets to have globally
ordered vers.

The admission scratch model must therefore retain ordered point and range
events. It cannot reduce the request to a `BTreeMap<Key, final_point_state>`
before evaluating visibility and count deltas.

DR5 implements this as a gated mixed-mutation engine. It retains staged point
records, exact range tombstones, and the ordered admitted events needed for
point and range version fences. It writes materialization, immutable log
entries, historical aggregates, device state, checksum, and counters in one
batch. Public admission still rejects DeleteRange because DR6 has not yet
made range-driven `live_count` resets exact.

Required ordered cases include:

- Multiple overlapping DeleteRange mutations.
- DeleteRange followed by point Set/Delete under it.
- Point Set/Delete followed by a covering DeleteRange.
- Parent DeleteRange followed by child DeleteRange and the reverse.
- Repeated DeleteRange over an already empty range.
- Empty-entry rollback batches, which remain distinct wire no-ops.

## Device Checksum

`DeleteRange` participates in the cumulative `DeviceChecksum` canonical
encoding. The encoding includes:

- A distinct mutation discriminant.
- Range kind (`Prefix` or `Full`).
- Canonical encoded prefix when present.
- Full `DeviceTag`.
- Full `Seal` encoding.

Diagnostic lease evidence and server-assigned admission sequence remain
excluded, as they are for point mutations. `op_index` is already committed by
the canonical ordered entry list and is derived from that position; it is not
an independently mutable checksum field.

## Range History And Assertions

The historical maximum for query prefix `P` has two sources:

1. Events at or below `P`: point mutations and descendant range tombstones.
2. Ancestor range tombstones that cover `P`.

Descendant range tombstones update the normal aggregate path upward through
all ancestors. Ancestor range tombstones are not fanned into every possible
descendant; reads walk upward to find them.

For submitting device `D`, the effective foreign maximum is:

```text
max(
    aggregate_below(P).max_excluding(D),
    covering_range_deletes(P).max_excluding(D),
)
```

The existing two-greatest-distinct-device representation applies to both
sources. The same effective maximum must be used by:

- `RangeAssert` evaluation.
- Lease grant barriers.
- Stateless `read_at` cuts and short-circuit checks.
- Any future public range-version API.

Earlier range deletes from the submitting device do not invalidate its own
assertion. Their order and identity are supplied by `DeviceSeq` and
`DeviceChecksum`, just like earlier point mutations.

`Range::Full` has a real root aggregate record. Every point and prefix-range
mutation updates it. Full-space history, barriers, `max_ver`, and `live_count`
must not be inferred from a nonexistent empty key or by scanning all depth-one
prefixes.

## Version Monotonicity

Range deletion hides old point records but does not erase their versions.
Without another mechanism, a client replaying past a range delete can miss a
hidden `Ver(100)` and later submit `Ver(5)`.

The proposed solution is a monotonic `max_ver` aggregate:

- Every prefix aggregate records the greatest point or range-mutation ver
  ever admitted beneath it.
- A DeleteRange carries one normal `DeviceTag.ver`.
- Admission requires the DeleteRange ver to exceed the effective `max_ver`
  under its target range.
- A later point mutation must exceed both its stored point ver and the newest
  covering range tombstone ver.
- Stateful full-log pull raises the client's space-local `ver_high` from every
  captured operation. Stateless range reads are observational only and do not
  establish version authority or mutate `ver_high`.

As with admission history, a child query combines its aggregate below the
query with covering ancestor tombstones:

```text
effective_max_ver(P) = max(
    aggregate_below(P).max_ver,
    covering_range_deletes(P).max_ver,
)
```

This makes the range tombstone itself a version floor above every point it
hides. `max_ver` is historical and never resets when data is deleted.

Unchecked submission skips only the local lease/range-assert preflight. It
does not skip authoritative server version checks.

## Current-State Counts

Historical admission maxima and `max_ver` are monotonic. `live_count` is not:
it represents current visible state and must support subtree reset.

An eager implementation could enumerate every covered key and descendant
aggregate, but that would make DeleteRange proportional to the deleted data.
The scalable design uses lazy subtree resets.

Each prefix aggregate gains count-generation metadata conceptually equivalent
to:

```text
PrefixMeta {
    historical_heads: top two DeviceAdmission values,
    max_ver: Ver,
    live_count: u64,
    count_epoch: AdmissionOrder,
    exact_range_delete: optional latest tombstone metadata,
}
```

The persisted DR4 shape factors the historical heads into a reusable
`HistoricalHeads { first, second }`. Prefix and Full-root aggregates store
those heads together with monotonic `max_ver`, current `live_count`, and the
`AdmissionOrder` through which that count is materialized. Each exact range
tombstone record stores its current authenticated event plus its own
`HistoricalHeads` and monotonic exact-target `max_ver`, so replacing a newest
tombstone never erases the foreign head needed by range assertions.

The exact storage split may differ, but these facts must be recoverable.

### Effective count

For prefix `P`, find the newest covering range tombstone `T`. If `P`'s count
state was last materialized before `T`, its effective count is zero. If it was
materialized at or after `T`, its stored count contains only post-reset state.

### Applying DeleteRange(P)

1. Materialize `P` against any newer covering ancestor reset.
2. Read `removed = effective_live_count(P)`.
3. Set `P.live_count = 0` and `P.count_epoch = admission_order`.
4. Record the exact range tombstone at `P`.
5. Materialize every strict ancestor against its own covering reset, then
   subtract `removed` from its count.
6. Leave descendant count records physically untouched; they are logically
   stale until read or written after the reset.

### Applying a later point mutation

For each node on the point key's prefix path:

1. Find the newest covering reset relevant to that node.
2. If the node's `count_epoch` is older, materialize it at zero in the new
   epoch.
3. Apply the point's net visibility delta.

DR6 implements these transitions in admission-order scratch state rather than
as an end-of-request net delta. Each touched aggregate is loaded at most once
into the scratch map, logically materialized against the newest persisted or
earlier-in-request covering tombstone, and updated immediately. DeleteRange
sets its exact target to zero and subtracts that target's effective count from
strict ancestors with checked arithmetic; descendants remain physically
untouched. The scratch aggregate records are committed atomically with point
materialization, range tombstones, the exact log, device state, checksum, and
counters. Snapshot reads use the same effective-count rule for their empty
subtree short circuit.

The implementation remained behind the public unsupported gate through DR7.
DR8 opens the shared admission path after client submission, authenticated
pull/fetch, durable admit-log, recovery, and scoped-effect handling all support
range operations.

The point's previous visibility is computed against covering tombstones, not
only from whether its retained point record is a Set.

### Count edge cases

- Repeated DeleteRange over an empty range removes zero keys but still
  advances history, checksum, and tombstone state.
- DeleteRange(child) after DeleteRange(parent) removes zero unless child keys
  were revived after the parent reset.
- Set after DeleteRange increments every materialized ancestor exactly once.
- Point Delete of a key already hidden by a range tombstone has zero count
  delta.
- Parent DeleteRange after child revival removes the revived keys once.
- Count subtraction must be checked and underflow is corruption.

The lazy-reset algorithm needs a model-based property suite before production
storage implementation.

## Storage And Admission Log

The server stores both materialized current state and an exact append-only
admission log:

```text
(space, Data, key) -> latest point record
(space, RangeDelete, 0) -> latest Full tombstone
(space, RangeDelete, depth, prefix) -> latest exact Prefix tombstone
(space, Meta, "root") -> full-space aggregate
(space, AdmissionLog, admission_seq, Header) -> admitted batch header
(space, AdmissionLog, admission_seq, Op, op_index) -> admitted operation
```

Depth zero is reserved for `Range::Full`; prefix records require a valid,
non-empty key whose component count exactly matches `depth`. Covering-ancestor
lookup reads only the Full key and each component-wise ancestor key, then
chooses the greatest `AdmissionOrder` rather than assuming the deepest record
is newest. The Full aggregate reuses the prefix aggregate value codec at its
dedicated `Meta/root` key. DR3 reserves and validates these storage shapes;
admission begins maintaining them only in the later semantic batches.

Every admitted Set, Delete, and DeleteRange is appended once in
`AdmissionOrder`; old entries are not moved or replaced when the same target
is written again. The materialized records make `get`, `list`, conflict
checks, and aggregates efficient. The admission log is the replication and
audit source of truth. The batch header preserves device sequence/checksum
history and gives accepted empty rollback batches a durable log record even
though they contain no operations.

Admission of a batch atomically updates point/range materialization,
aggregates, device state, counters, checksum, and every corresponding log
entry. A crash exposes either all of them or none of them.

The first implementation retains the server admission log indefinitely.
Snapshot replacement, log truncation, cursor floors, and compaction proofs are
future protocol additions rather than partially supported fallback behavior.

## Reads

### Get

`get(K)` reads the retained point record and newest covering range tombstone.
It returns `None` unless the point Set is newer than the tombstone.

The public result continues to hide the distinction among never written,
point deleted, and range deleted.

### List

`list(P)` scans candidate point records and filters each through its newest
covering tombstone. `live_count == 0` remains a valid empty short-circuit only
after evaluating lazy reset inheritance.

Pagination operates over visible keys. Hidden records must not consume the
requested visible-result limit.

### Full admission-log pull

Client replication is exact full-space log replay, not snapshot/delta state
transfer. The server verb reads a contiguous interval after one
`AdmissionSeq` and returns every admitted batch, including accepted empty
rollback batches:

```rust
pub struct PullResponse {
    pub after: AdmissionSeq,
    pub through: AdmissionSeq,
    pub batches: Vec<AdmittedBatch>,
}
```

The batches are dense over `(after, through]`; each batch's mutations are
ordered by `op_index`. Bootstrap starts at `AdmissionSeq(0)` and replays all
retained history. A bounded page may stop before the current server high
water, but it must contain the complete dense interval it claims. There is no
snapshot shortcut in the initial protocol.

### Arbitrary range reads

The existing `read_at` verb provides stateless range reads. For requested
range `P` and cursor `after`, its delta path scans the admission log through
one atomic cut and returns relevant source operations in `RangeCut::Delta`.
The response's `at` field is the cut, including when the delta is empty.

```rust
read_at([RangeCursor { range: P, since: Some(after) }])
    -> ReadAtResponse { at, ranges: [RangeCut::Delta(operations)] }
```

A point operation is relevant when `P` covers its key. A DeleteRange `R` is
relevant when:

```text
R covers P || P covers R
```

DR7 implements this predicate directly over each immutable log mutation.
DeleteRange sources are returned unchanged, including their original Full or
Prefix target and `AdmissionOrder`; the requested range scopes how the caller
applies the response, not how the server rewrites authenticated operations.
The delta empty-short-circuit uses effective history: descendant aggregate
history plus exact covering ancestor tombstones. This prevents an ancestor
DeleteRange from being skipped merely because the requested child aggregate
was left physically untouched by the lazy reset.

Dense `pull` uses the same exact log decoder and returns range operations once
in their original batch, while retaining empty batches and bounded-page
density. DR8 removes the former private/public admission split after client
submission and admit-log handling become range-aware.

The first case clears all of the requested local range. The second clears a
nested part of it.

The operation remains byte-for-byte the original authenticated admission.
The request carries the scope `P`. A caller that materializes only `P` applies
a relevant DeleteRange over the intersection:

```text
effect(P, R) = intersection(P, R)
```

This scope rule is mandatory. If a range-read caller reading `db/a` applied an
ancestor `DeleteRange(db)` outside its captured scope, it could corrupt
caller-owned `db/b` state. Homebase does not manufacture a projected
mutation or new seal; it exposes the authenticated source operation and the
scope under which it was captured.

Point and range events are returned in `AdmissionOrder`. Ordering is required
to distinguish:

```text
DeleteRange(db) at 5, Set(db/a) at 6
Set(db/a) at 5, DeleteRange(db) at 6
```

`at` advances even when `operations` is empty. The server's global
`AdmissionSeq` remains dense, but a range read naturally observes gaps
because irrelevant batches are filtered. Callers order returned mutations by
full `AdmissionOrder`, never by assuming the filtered result is dense.

Range reads are deliberately outside client replication state. Calling one does
not append the client admit log or change `head`, `neck`, `tail`, `ver_high`,
lease usability, or checked-assertion readiness. Only a full admission-log
pull may advance those facts.

### Atomic cut

Point records, range tombstones, aggregate metadata, and admission-log events are
read at one actor-serialized admission cut. A pull must not combine a new
tombstone with old aggregate metadata or vice versa.

## Leases And Authorization

DeleteRange is a write over an entire prefix. It conflicts with every live
foreign lease whose prefix overlaps in either direction:

```text
lease.prefix covers delete.prefix
||
delete.prefix covers lease.prefix
```

This differs from a point write, which only needs to find leases covering one
key. `LeaseManager` needs an explicit range-write overlap validator rather
than a representative-key workaround.

Presented lease ids remain diagnostic evidence, not admission authority.
Writes without a lease remain admissible only when no live foreign
reservation overlaps.

The DR5 kernel validator distinguishes point coverage from range overlap: a
point write checks only lease prefixes that cover that exact key, while a
range write checks ancestors and descendants. Full checks every live lease.
Prefix-scoped capability authorization remains at the wire layer above the
in-process kernel, where connection/token scope is available; the private DR5
admission path does not manufacture a representative key for Full.

A capability/token must authorize the entire deleted range. Authorization of
one contained key is insufficient.

## Client Encryption And Local State

Tuple-prefix encryption preserves component boundaries, so an encoded prefix
can be authenticated and transported without revealing plaintext names.

The value cipher adds a distinct DeleteRange operation kind. It authenticates
empty plaintext and emits no ciphertext, like point Delete, but binds a Range
rather than a Key.

The client submit log stores DeleteRange in normal `DeviceOp::Commit` entries.
Rollback behavior is unchanged: a rejected commit and its dependent suffix
are retired, while the eventual empty rollback batch remains a wire no-op.

Local equivalence and recovery logic must replay pending point and range
mutations in order. A local database implementation may execute its own range
delete immediately, but its submit-log representation must still be sufficient to
reconstruct the exact server mutation stream and `DeviceChecksum`.

## Client Storage Boundary

The homebase client is not a materialized KV replica. Its `MetaStore` contains
coordination and recovery state:

- Device identity and clock high water.
- Per-space submit-log records and `head/neck/tail` cursors.
- Per-space admit-log records and independent `head/neck/tail` cursors.
- Confirmed `DeviceChecksum` and space-local `ver_high`.
- Lease state and forgotten release intents.
- Codec/envelope material.

It does not contain the current admitted KV map and cannot answer application
point or range reads locally. The reference `OrderedMetaStore` reserves a
separate `Data` namespace for cohabitants but never reads or writes it.

MetaStore has no range-watermark records or transitions. Stateful replication
progress exists only in the dense admit log; stateless `fetch` callers own
their cursors outside MetaStore. This keeps capture and application separate
without introducing a managed replica.

## Ops-Only API

### Mutation submission

The ordinary submit API accepts DeleteRange alongside Set and Delete:

```rust
let submission = space
    .submit_checked(
        [Mutation::DeleteRange {
            range: Range::Prefix(prefix),
        }],
        assertions,
    )
    .await?;

submission.push().await?;
```

No DeleteRange-specific builder is needed initially. Keeping it as an ordinary
`Mutation` preserves the existing submit/push vocabulary and avoids a second
API that must duplicate checked/unchecked semantics.

### Admit log

Each space owns a durable client-side admit log that is an exact retained
replica of the server admission log. Client records use the original
`AdmissionSeq` and contain the complete admitted batch; there is no local
admit sequence or scoped wrapper.

The log stores verified opaque values rather than decrypted values. Iteration
authenticates them again while opening operations, minimizing secret-bearing
durable state and checking integrity at the application boundary.

The submit and admit logs use the same cursor geometry:

```text
submit: [head, neck) acknowledged/retained; [neck, tail) awaiting push
admit:  [head, neck) applied/retained;      [neck, tail) awaiting application
```

All cursors are exclusive durable positions and satisfy
`head <= neck <= tail`. Empty logs initialize to the same valid position,
`AdmissionSeq(1) / AdmissionSeq(1) / AdmissionSeq(1)`. The meanings are:

- `head`: first server admission still retained locally.
- `neck`: next server admission not yet acknowledged as applied.
- `tail`: next server admission not yet captured locally.

Each bounded `pull()` page performs one atomic client transition:

1. Read a dense full-space `PullResponse` after `tail - 1`.
2. Decrypt and authenticate every returned operation and validate batch/order
   density.
3. Append every complete admitted batch at its server `AdmissionSeq`.
4. Advance `tail` to `through + 1` and raise `ver_high` from observed entries.

Pull never advances `neck`. Accepted empty rollback batches are retained as
headers, so `[head, tail)` stays a dense replica even when a batch has no data
operation. Each bounded page may advance `tail` independently after its whole
dense interval has been authenticated and durably appended.

The application-facing API is deliberately log-shaped:

```rust
space.pull().await?;

for batch in space.admits().iter_from_neck().await? {
    application.apply_admitted_batch_atomically(&batch)?;
    space.admits().mark_applied(batch.admission_seq.next()).await?;
}

space.admits().trim(to).await?;
```

`mark_applied(to)` advances admit `neck` over `[old_neck, to)`. In the same
MetaStore transition it records that the complete server log prefix through
`to - 1` was applied. It requires `neck <= to <= tail` and trusts the caller's
claim that every crossed batch was durably applied in order.

`trim(to)` deletes records in `[head, to)` and moves `head`; it requires
`head <= to <= neck`. It never changes application coverage. This makes trim
isomorphic across submit and admit logs: logical completion moves `neck`,
physical reclamation moves `head`.

The durable admit log makes crash behavior explicit:

- Crash before append: fetched bytes are forgotten and pulled again.
- Crash after append: the record remains pending at or beyond `neck`.
- Crash after application but before `mark_applied`: the record replays, so
  application must be idempotent or persist the server `AdmissionSeq` with its
  own update.
- Crash after `mark_applied`: `neck` durably includes the applied prefix.
- Crash during trim: the old or new retained prefix is visible atomically.

Pending records cannot be trimmed. A future discard operation, if added, must
atomically delete `[neck, tail)` and rewind `tail = neck` so the dense suffix is
replayed from retained server history. Pulling must apply backpressure when
`[neck, tail)` exceeds configured record or byte limits.

### Stateless range read

`fetch(range, after)` is client convenience sugar over a one-range `read_at`
delta. It returns `FetchedRange { range, at, cut }`: authenticated source
operations, the requested scope, and the `at` cursor required for the next
fetch. It performs no MetaStore transition. In particular, it cannot advance admit
`tail` or `neck`, `ver_high`, leases, or checked-submission readiness.

For DeleteRange, a fetch caller that materializes only the requested range
applies the intersection of source range and requested range. The client
admit-log consumer sees the complete space log and therefore applies the
original DeleteRange directly.

DR8 validates every fetched source against the encoded request scope before
returning it. [`FetchedRange::delete_range_effect`] computes the deterministic
intersection in the same encoded name domain as returned operations while the
original authenticated source remains unchanged. Fetch performs no MetaStore
transition; dense pull authenticates the complete page before atomically
appending range-bearing batches and raising the space-local `ver_high`.

[`FetchedRange::delete_range_effect`]: client/src/space.rs

This decision makes a space the minimum replication and client-log authority
unit. Future subspace-only replication would require separate subscription
logs or another cursor model; arbitrary transient range read alone does not
change that boundary.

### Leasing and admission position

`lease()` acquires, renews, or reuses server reservations and performs a full
log pull until `tail > lease.barrier`. The immutable prefix-scoped server
barrier is already in the same `AdmissionSeq` domain as the client admit-log
cursors; no translated local cursor is stored.

A lease is usable exactly when:

```text
locally held
&& live under the conservative client deadline
&& admit_log.neck > lease.barrier
```

No pending bit, barrier object, `cross()`, or managed application path is
needed. `submit_checked` requires `neck > range_assert.upto`, proving that the
application has processed the complete server log through that assertion cut.
Fetched `tail` is never application authority. `ver_high`, by contrast, may
advance at append time because it means "do not reuse an observed version,"
not "the application has installed this state."

The application owns one essential range-assert invariant. At
`submit_checked`, `RangeAssert { prefix, upto }` must be true in the
application state represented by the applied log prefix `[1, neck)`: the
maximum relevant foreign admission affecting `prefix` is at most `upto`.
Usually the application can use `upto = neck - 1`; if it uses an older
optimistic read cut, it must establish that no relevant applied operation in
`(upto, neck)` invalidated that read. It must also assert every range whose
foreign state influenced the submitted decision.

Once that predicate is true and a covering lease is live, it remains true
while the lease continuously reserves the range: own-device and unrelated
admissions may advance the space sequence, but conflicting foreign admissions
cannot enter the range. Lease expiry or unlease does not itself falsify the
predicate; it permits a later relevant foreign admission that may do so. The
server therefore re-evaluates the literal range predicate at admission and
rejects any such race. Homebase can verify `neck > upto` and
`neck > lease.barrier`, but cannot verify that the application's operation was
actually derived from all and only the ranges it declared.

The client methods are `unlease_checked()` and `unlease_unchecked()`; the wire
verb remains `release`. Checked unlease preserves a live covering reservation
for every range assertion in queued checked submissions. Such replacement
coverage need not have crossed its own barrier because it is preserving
server-side exclusion for work already checked. Its barrier need not be behind
neck for that purpose, though it cannot authorize a new local check until it
is. Unchecked unlease skips that guard.

An unapplied lease may be unleased. Later admit-log application still advances
coverage, but must never recreate or return a released, forgotten, or expired
lease. `repair_leases()` reconciles server reservations and performs the same
full-log pull needed to capture their barriers. No path gives Homebase
ownership of consumer data.

`unlease_checked()` is a conservative local preflight like
`submit_checked()`, not a permanent server guarantee: it refuses to knowingly
remove the last locally live covering reservation needed by a queued checked
submission. A lease may still expire or race the release RPC; authoritative
range assertions remain the correctness check at admission.

### Irreducible consumer complexity

An admit-log consumer that materializes state must understand DeleteRange; a
stateless range-read consumer must additionally respect its requested scope.
Expanding a range tombstone into point deletes would destroy scalability.
Higher-level libraries can hide this from their own application users, while
Homebase remains an oplog service.

## Systematic Correctness Method

DeleteRange optimizations are accepted only by refinement against a small,
append-only reference model. The model stores every admitted mutation in
`AdmissionOrder` and derives visibility, history, versions, counts, conflicts,
full-log replay, and arbitrary range reads directly. It deliberately has no
prefix aggregates, lazy count epochs, or materialized-record shortcuts.

The test-only server `ReferenceModel` is that executable specification. Its
only state is a dense vector of exact admitted batches. Queries at an explicit
cut rescan that history, including empty batches, same-batch `op_index` order,
covering ancestor tombstones, and overlapping descendant tombstones. It must
remain structurally independent of production schema and admission code so a
shared implementation bug cannot make differential tests agree falsely.

The production implementation is exercised after every randomly generated
command and compared with that model. Commands include multiple devices,
mixed batches, Set/Delete/DeleteRange at parent/child/full ranges, assertions,
lease acquire/renew/unlease/expiry, pull/fetch/mark/trim, crashes, and reopen.

DR9a strengthens the simulation store audit before adding those workloads. It
replays the immutable admission log to reconstruct exact point records and
exact-target range tombstones. For aggregates it deliberately does not repeat
the lazy subtraction algorithm: after each source operation it brute-force
scans replayed visibility for only the aggregate nodes that operation touches,
thereby reconstructing historical heads, `max_ver`, physical `live_count`, and
`count_epoch`, including intentionally stale descendant records. Stored point,
range, prefix, and Full-root materialization must match this reconstruction.
Dedicated corruption tests prove that independently altering a tombstone or a
lazy aggregate is detected.

The following laws are permanent tests rather than one-time examples:

1. **Read agreement:** `get`, `list`, server log replay, and model visibility
   agree for every key and prefix; `live_count` equals visible cardinality.
2. **Server replay:** replaying the exact admission log through cut `T`
   produces the same visible state, historical heads, `max_ver`, and counts as
   server materialization at `T`.
3. **Client replica:** `[head, tail)` contains exactly the retained server
   batches at those `AdmissionSeq` values; full pull never creates a gap,
   duplicate, or translated cursor.
4. **Range-read composition:** fetching `A..B` then `B..C` for one range is
   state-equivalent to fetching `A..C`; stateless fetch changes no client
   cursor, `ver_high`, lease, or checked-submission state.
5. **Range projection:** applying a range read for `P` never changes caller
   state outside `P`, including when the source tombstone is an ancestor.
6. **Batch order:** Set/DeleteRange permutations in one batch match full-log
   replay; splitting a mutation sequence across batches preserves final
   state when assertions and lease timing are held constant.
7. **History:** effective foreign maximum and lease barriers equal the model's
   maximum over descendant events plus covering ancestor tombstones.
8. **Version floor:** effective `max_ver` dominates every visible or hidden
   point/range mutation affecting the query, including full-space resets.
9. **Server crash atomicity:** every injected storage failure exposes either the
   complete old state or complete new state, never mixed data/tombstone/meta/
   admission-log/device/checksum state.
10. **Client cursor geometry:** every client transition preserves
   `head <= neck <= tail`; append changes only `tail`, mark-applied changes only
   `neck`, and trim changes only `head` plus retained bytes.
11. **Capture/application separation:** `ver_high` never gets ahead of durable
    admit-log `tail`; lease and checked-assertion usability never get ahead of
    admit-log `neck`.
12. **Replay after crash:** faults before/after append, external application,
    mark-applied, and trim cause only refetch, idempotent replay, or durable
    progress, never a missing operation.

In addition to randomized property tests, an exhaustive bounded-state test
enumerates short histories over a tiny three-level key tree and two devices.
This is especially valuable for equal-`AdmissionSeq` operation ordering,
nested lazy resets, dense-page boundaries, and pull/fetch/mark/unlease/expiry
interleavings that random tests may rarely align. Storage `certify`/audit code
should recompute aggregates and tombstone invariants from records in test and
verification builds.

Every future optimization must name its refinement law and fallback. Aggregate
short-circuiting owns read agreement; paged full pulls own client-replica
density; range indexes own range-read composition; and future server-log GC
owns replay for every supported cursor. An optimization without such an
oracle stays out.

### Correctness boundary

These invariants cover honest protocol participants, crashes, retries,
concurrency, storage faults surfaced through `OrderedStore`, and corrupted or
misbound ciphertext detected by AEAD. AEAD authenticates the original
mutation and range; any stateless range-read effect is the deterministic
intersection of that range with the requested range.

They do not prove that a Byzantine server returned a complete admission log,
truthful `max_ver`, or correct admission order. `DeviceChecksum` protects one
device's submitted history, not the completeness of reads observed by another
device. Detecting server omission or equivocation requires the deferred
external witness/Merkle/space-checksum layer and must not be implied by the
DeleteRange API.

## Failure And Recovery

- Admission is atomic across point data, range tombstones, aggregates,
  admission log, counters, device sequence, and `DeviceChecksum`.
- Client submit-log trim and confirmed checksum remain atomic.
- A lost admission response is recovered through the existing retained-batch
  checksum chain, including DeleteRange canonical bytes.
- A malformed or unknown DeleteRange seal rejects the whole request.
- A range-delete conflict or version regression leaves all counts and
  tombstones unchanged.
- Admit-log append advances `ver_high` atomically with `tail`; mark-applied
  advances the applied global prefix atomically with `neck`.
- A consumer that cannot make replay idempotent must persist `AdmissionSeq` in
  its own application transaction. Homebase cannot repair a non-idempotent
  external apply after a crash between application and `mark_applied`.

## Retention And Garbage Collection

The initial server retains every admission-log entry indefinitely. The client
may trim only records below admit `neck`; keeping `[head, neck)` is optional
local history and dropping it does not affect `neck` or `tail`.

Server log GC requires a new protocol: durable cursor floors, an authenticated
checkpoint or snapshot representation, and an explicit stale-cursor response.
Until that exists, deleting old server admissions would silently make replay
incomplete. Physical deletion of hidden materialized point records likewise
requires preserving their version floors. Both are deferred.

## Required Edge-Case Matrix

### Visibility

- Parent delete hides all descendants but no sibling.
- Child delete does not hide parent or sibling keys.
- Set after covering delete revives.
- Point Delete after covering delete remains absent with zero count delta.
- Parent delete after child revival hides the revival.
- Full-space delete followed by a Set leaves only the new Set visible.

### Ordered batches

- Set then DeleteRange in one batch.
- DeleteRange then Set in one batch.
- Both orders carry distinct `op_index` values despite sharing one
  `AdmissionSeq`, and admission-log scans preserve them.
- Multiple same-batch range resets compare full `AdmissionOrder` count epochs,
  not only their shared sequence.
- The same cases split across batches in one request.
- The same cases split across separate pushes.
- Overlapping parent/child deletes in both orders.

### History and assertions

- Ancestor range delete invalidates a child assertion from another device.
- Descendant range delete invalidates a parent assertion.
- Same-device delete and dependent Set can queue offline at one `upto` cut.
- Lease barrier includes a covering ancestor tombstone.

### Reads and replication

- `get`, `list`, full server-log replay, and the reference model agree.
- Full client pull reproduces every server batch and empty header at the same
  `AdmissionSeq` without translation.
- Child range read receives the authenticated ancestor tombstone with child
  request scope; caller application leaves siblings unchanged.
- Parent range read receives descendant tombstones.
- Filtered range read preserves delete-before-set and set-before-delete order.
- A range read with no relevant operations still reports its `at` cut
  but changes no client state.
- Appending a pull changes admit `tail` but never `neck`; marking applied
  changes only `neck` atomically.
- `trim(to)` accepts exactly `head <= to <= neck` and changes neither `neck`
  nor `tail`.
- Reopen at every append/mark/trim crash point preserves
  `head <= neck <= tail` and never loses pending admissions.
- Pagination skips hidden records without reducing the visible limit.
- Bounded full-log pages remain dense and raise `ver_high` only when appended.
- Arbitrary range read before/after pull leaves all three cursors,
  `ver_high`, leases, and checked readiness unchanged.

### Counts

- Delete populated, empty, and already deleted ranges.
- Nested resets with later revivals.
- Repeated point deletes under a range tombstone.
- Counts remain exact at every ancestor and never underflow.
- Crash before/after the atomic reset transition.
- The full-space root aggregate agrees with the total visible cardinality.

### Versions

- Hidden high-ver point cannot cause a later surprising regression after pull.
- DeleteRange with stale ver rejects atomically.
- Later Set must exceed the covering tombstone ver.
- Parent and child `max_ver` queries include the correct events.

### Coordination and integrity

- Foreign ancestor and descendant leases both conflict.
- Same-device lease evidence remains diagnostic only.
- DeleteRange changes `DeviceChecksum`.
- Altered range kind/prefix/seal fails checksum catch-up.
- Rollback of a rejected DeleteRange commit emits only the existing empty
  rollback batch.
- A lease remains unusable while `neck <= barrier`, including after reopen,
  renewal, repair, or a pull that captured but did not apply the barrier.
- Expiring, forgetting, or unleasing a lease before application cannot make
  later neck advancement resurrect it.

## Proposed Implementation Batches

The admit-log substrate lands first for existing Set/Delete behavior. Range
deletion then builds on an already durable replay path. Every row below is one
reviewable local commit and includes its own tests and documentation change.

| Batch | Scope | Tests | Documentation |
|---|---|---|---|
| AL1 Admission order | Add `AdmissionOrder { admission_seq, op_index }`; persist `op_index` in admitted point records and assign it from batch entry order. No DeleteRange yet. | Core/schema roundtrips; same-batch point order; workspace migration tests. | Core tag/schema docs distinguish batch barriers from operation order. |
| AL2 Exact server log | Add append-only server `AdmissionLog` records for existing Set/Delete and write them atomically with materialized data, aggregates, counters, device state, and checksum. | Exact log order; repeated-key history retained; empty admitted batch; fault injection proves log/data atomicity. | Server schema/data docs describe materialization versus replay log. |
| AL3 Full-log and range-read verbs | Add dense bounded `pull` for full replay and make `read_at` deltas scan exact sparse history for arbitrary Prefix/Full ranges. Remove the superseded compacted changelog. Existing Set/Delete only. | Dense bounded pulls; empty batch headers; range-filtered gaps; empty delta with advancing `at`; both reads share one atomic cut. | Wire docs separate stateful full replication from stateless range observation. |
| AL4 Client admit MetaStore | Add per-space admitted batches keyed by server `AdmissionSeq`, independent `head/neck/tail` in the same domain, and atomic append/mark-applied/trim transitions. Do not route network pulls through it yet. | Shared `PullResponse` density validation; MetaStore conformance; exact server-seq keys; reopen; illegal gaps/moves; trim-above-neck rejection; empty `1/1/1`; fault injection for every transition. | Meta docs specify both logs, exclusive cursors, and dense replica invariant. |
| AL5 Client full pull and stateless fetch | Route Set/Delete `pull()` through dense server pulls: authenticate before append, raise `ver_high` with `tail`, expose iteration, move `neck` on mark-applied, and trim below neck. Expose `fetch(range, after)` as one-range `read_at` sugar without any MetaStore mutation. | Pull retry/paging; crash before/after append/apply/mark/trim; server/client seq identity; ciphertext tamper never appends; range read leaves cursors/ver/leases byte-identical. | Client API docs distinguish `pull` from `fetch`. |
| AL6 Lease/admit integration | Replace public `ensure` with `lease`; rename release methods to `unlease_checked`/`unlease_unchecked`; make checked submit and lease usability require `neck > barrier`; route acquisition/repair through full pull until `tail > barrier`. | New/reused/renewed lease with captured-but-unapplied barrier; genesis barrier; expiry/unlease before apply; repair/reopen; checked submit blocked until neck passes both barrier and `upto`. | Lease/client docs for one shared `AdmissionSeq` domain. |
| AL7 Remove old pull model | Delete temporary pull-barrier abstractions and obsolete replication watermark transitions; retain `read_at` solely as stateless range observation. | Workspace tests plus grep/schema checks proving one replication path remains. | Final admit-log API and storage-format cleanup. |
| DR1 Core DeleteRange | Add `Mutation::DeleteRange`, range target helpers, admitted range operation encoding, distinct seal AAD kind, and `DeviceChecksum` encoding. Server admission rejects it as unsupported for now. | Core/cipher roundtrips; empty ciphertext; key/range/op-kind tamper; checksum order sensitivity; Full/Prefix encoding. | Core mutation/seal docs. |
| DR2 Reference model | Build an append-only plaintext oracle for Set/Delete/DeleteRange over a tiny prefix tree, including `AdmissionOrder`, visibility, history, `max_ver`, counts, conflicts, full replay, and stateless range reads. | Exhaustive short histories and initial randomized commands; no optimized server behavior yet. | Model invariants and refinement-law section. |
| DR3 Tombstone/root storage | Add exact range-tombstone records, covering-ancestor lookup, dedicated full-space root aggregate, and schema codecs. Keep public DeleteRange rejected. | Schema roundtrips; ancestor lookup; Full root; malformed records; storage ordering. | Server schema docs and key layouts. |
| DR4 Historical metadata and versions | Extend prefix/root metadata with monotonic `max_ver`, top-two history for range events, and `AdmissionOrder` count epochs; migrate existing Set/Delete updates first. | Existing point behavior unchanged; effective ancestor/descendant history; max excluding device; root max/count invariants. | Aggregate docs define historical versus current fields. |
| DR5 Ordered internal range admission | Implement internal scratch application for mixed point/range mutations, range `ver` fence checks, current tombstone writes, point visibility, bidirectional lease conflicts, authorization checks, and exact server-log append. Keep the public gate closed until counts are correct. | Model differential tests for all same-batch orders; stale ver; parent/child/full overlap; atomic rejection; conflict matrix. | Admission-order and range-conflict docs. |
| DR6 Lazy live count | Implement lazy subtree resets/materialization and exact ancestor subtraction. Keep the public unsupported gate closed until server replay and client handling also exist. | Model-based count properties; nested resets/revivals; repeated empty deletes; underflow corruption; crash atomicity. | Count-generation algorithm and gated behavior. |
| DR7 Full replay and range reads | Include DeleteRange unchanged in dense pulls; include relevant authenticated sources in stateless `read_at` deltas when either range covers the other. | Full replica gets exact source once; child read gets ancestor; parent read gets descendant; no sibling point leakage; empty delta at a later cut; both paths equal model. | Full-replay and scoped-read DeleteRange contracts. |
| DR8 Client DeleteRange integration and enablement | Add outgoing sealing/submission, incoming full-log verification, admit-log persistence/iteration, stateless `read_at` decoding/intersection helper, rollback/recovery, and `ver_high` updates from pull only; then remove the server unsupported gate. | Submit/push/pull roundtrip at identical server seq; fetched range effect never escapes request; fetch leaves client state unchanged; crash/reopen; tampered source rejects before append; public end-to-end admission. | Client cipher/admit-log/fetch docs and public examples. |
| DR9 System torture and audit | Run full randomized server/materialization/log/client differential testing with faults, leases, assertions, duplicate devices, acknowledgement loss, and trim retention. Add recomputation audits for tombstones, metadata, counts, and log order. | Workspace, simulation, equivalence, torture, bounded-history, and seeded fault suites. | Final design conformance notes and deferred GC/checkpoint limits. |

The public DeleteRange path remains hard-rejected through DR7, so no commit
exposes a mutation whose visibility or counts are only partially implemented.
Because nothing is deployed, each storage and wire batch may replace record
versions directly instead of carrying compatibility decoders.

## Open Decisions

1. Final `PullResponse` and `read_at` response byte/operation limits.
2. Exact persisted fields and split for prefix/root aggregate count
   generations introduced by the lazy-count implementation.
3. Whether DeleteRange ver regression uses a new range-specific error or a
   generalized target-aware `VerRegression`.
4. Whether `mark_applied(to)` remains the only neck-advance API or receives
   per-record convenience sugar.
5. Admit-log byte/record backpressure defaults and the future semantics of
   abandoning pending captures.
6. Retention tripwires that would justify checkpoints, Merkle consistency
   proofs, or whole-space checksums for anti-entropy and checkpoint proofs.

Item 1 should be resolved before transport adapters; the others may be settled
in their owning batch. Visibility, ordering, cursor geometry,
version-floor, and atomicity invariants are not optional API details.
