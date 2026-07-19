# Homebase Physics

This document states the observable semantics of the current Homebase kernel
and client. It separates three kinds of claim:

- **Semantics** describe what an operation means.
- **Invariants** are properties enforced directly by Homebase transitions.
- **Emergent guarantees** follow only when stated application obligations also
  hold.

This is a contract for the landed system, not a description of a future
managed SQLite replica. Homebase stores, admits, encrypts, and replays
operations. It does not apply those operations to application state.

The implementation details and algorithms live in [DESIGN.md](./DESIGN.md)
and [DELETE_RANGE_DESIGN.md](./DELETE_RANGE_DESIGN.md). When those documents
describe future work, this document is the narrower authority for current
behavior.

## 1. System boundary

A **space** is the unit of ordering, atomic admission, leases, materialized
server state, and exact server history. Nothing is atomic or ordered across
spaces.

A **device** is a durable client identity. One device may use many spaces. For
each `(space, device)` pair, the client and server maintain an independent
outgoing sequence and cumulative checksum.

The client has two durable logs per space:

- The **submit log** contains locally accepted outgoing work, keyed by
  `DeviceSeq`.
- The **admit log** contains the exact incoming server history, keyed by
  `AdmissionSeq`.

The sequence domains are deliberately different and are never compared with
each other. A `DeviceSeq(7)` and an `AdmissionSeq(7)` have no relationship.

Homebase also exposes a materialized server view through `get` and `list`, and
a stateless historical range observation through `read_at` / client `fetch`.
Neither is the client's application database.

## 2. Keys, ranges, and mutations

A key is a non-empty tuple of non-empty byte components. Prefix relations are
component-wise, not byte-string prefix relations. For example, `(db, pay)` is
a prefix of `(db, pay, row)` but not of `(db, payroll)`.

A `Range` is either:

- `Full`, covering the whole space; or
- `Prefix(key)`, covering that key and every component-wise descendant.

There are three mutation kinds:

- `Set { key, value }` makes a point value present.
- `Delete { key }` makes one point absent.
- `DeleteRange { range }` makes every point in a range absent as of that
  operation's position in admission order.

Set with an empty logical value is distinct from Delete. Delete and
DeleteRange carry no value ciphertext; an empty Set encrypts a non-empty
framing byte and therefore has non-empty ciphertext.

### Range visibility

Server visibility is ordered by `AdmissionOrder { admission_seq, op_index }`.
Within one admitted batch, operation order is therefore significant.

For a point key, the current visible state is determined by its newest point
mutation relative to the newest admitted DeleteRange that covers it:

- a later Set revives a point hidden by a range delete;
- a later point Delete keeps it absent;
- a later covering DeleteRange hides it again.

Set then DeleteRange in one batch differs from DeleteRange then Set. Replay,
materialized reads, counts, and stateless range reads preserve that order.

A DeleteRange is retained as one range operation. It is never expanded into a
set of point deletes. Descendant aggregate records may consequently be
physically stale after an ancestor reset, but effective visibility and
`live_count` account for the covering reset and remain exact.

`get` returns only a currently live Set; never-written, point-deleted, and
range-hidden keys are all `None`. `list` returns only live points in key order.
Deleted history remains observable through exact pull and stateless range
history.

## 3. Ordering domains

### Device sequence

`DeviceSeq` is client-minted, strictly increasing within one `(space,
device)` stream, and assigned when a local submission becomes durable.
Different spaces have independent streams.

Device sequences need not be dense. Rollback retires local sequence numbers
without reusing them, so a later valid server admission may jump over a gap.

### Admission sequence

`AdmissionSeq` is server-minted and globally ordered within one space.
Successful batches receive consecutive values beginning at 1. Every batch in
a successful multi-batch request receives its own admission sequence. An
empty rollback-marker batch also consumes an admission sequence.

All operations in one batch share its `AdmissionSeq` and receive dense
`op_index` values beginning at zero. The pair is the total `AdmissionOrder`.

### Versions

`Ver` is a client-minted Lamport-style version. The client keeps one durable
high-water per space and assigns consecutive versions to mutations in entry
order. The high-water advances atomically with local submission and also
advances when a pulled page is durably appended.

Versions are not dense per key. The server enforces strict monotonicity:

- a point mutation must exceed the point's current version and every covering
  range-delete version;
- a DeleteRange must exceed every point or range version that affects its
  target.

This preserves hidden version floors. A stale writer cannot revive data
merely because the newer value was deleted or hidden by an ancestor range
delete.

## 4. Atomic server admission

An `AdmissionRequest` contains an ordered vector of successive device batches.
The current server policy is all-or-nothing across the entire request.

Before admission, the server checks:

1. the client's expected `DeviceChecksum` against server state;
2. device-sequence progression;
3. live foreign lease conflicts for every mutation target;
4. every range assertion against historical foreign admissions;
5. mutation and tag shape, including supported Seal payload shape;
6. point and range version floors in batch order.

The request is evaluated against scratch state. A later batch and later
operation in the same request observe earlier operations in that request.

On success, one atomic storage transition publishes all of the following:

- exact admission-log headers and operations;
- point materialization and range tombstones;
- prefix/root history, version, and live-count aggregates;
- the device sequence and cumulative checksum;
- the space admission high-water.

On semantic failure, none of them change. The response contains one result
per requested batch. Today all results are Applied or all are Failed, although
the vector shape permits a future policy with per-batch outcomes.

On a storage or process failure, observers see either the complete old state
or the complete new state, assuming the `OrderedStore` fulfills its atomic
`apply` contract.

## 5. Submit and push

### Local submission

`submit_checked` and `submit_unchecked` perform no network admission. They:

1. encode names into the server-visible namespace;
2. reserve a `DeviceSeq` and versions without making that reservation durable;
3. seal mutations off the coordination loop;
4. atomically advance counters and append one `DeviceOp::Commit` to the submit
   log.

A crash before step 4 leaves no durable reservation or sequence hole. A crash
after step 4 leaves a complete queued submission. The returned `Submission {
seq }` proves local durability only.

`submit_checked` differs from `submit_unchecked` only in local preflight and
the persisted submit mode used by checked unlease. The server evaluates the
same range assertions either way. Checked submission does not require leases
for the mutation targets themselves.

### Submit-log cursors

The submit log has exclusive cursors satisfying:

```text
head <= neck <= tail
```

- `[neck, tail)` is the active FIFO window eligible for push.
- `[head, neck)` is retained inactive history, normally empty after an
  acknowledged trim but possibly populated by rollback.
- `tail` is the next sequence to mint.

The canonical empty state is `{ head: 1, neck: 1, tail: 1 }`.

### Push

`space.push()` pushes only that space. There is no client-wide push.
`push_until(seq)` does not send later submissions. `Submission::push()` is
attribution sugar over `push_until` and reports whether that exact submission
was Applied, Failed itself, or Blocked by an earlier failure.

Push is FIFO. Adjacent local batches may be grouped into one atomic server
request, but grouping is not durable state and does not erase batch identity.
If a grouped request fails, the client probes smaller prefixes to locate the
first rejected submission.

A semantic rejection stalls at the rejected sequence. Earlier successfully
admitted work may already have been trimmed, but the rejected submission and
its active suffix remain queued. Homebase does not automatically roll them
back or rewrite them.

An unavailable response is ambiguous: the server may or may not have admitted
the request. The queue is preserved and the caller must retry before deciding
to roll back.

## 6. Device checksum and retry

Each `(space, device)` stream has a cumulative `DeviceChecksum`:

```text
C[n] = H(domain, C[n-1], space, device, device_seq, canonical_batch)
```

The canonical batch includes ordered range assertions, mutations, device
tags, ciphertext, and Seals. Diagnostic lease evidence is excluded.

The server accepts new work only when the presented checksum equals its
confirmed checksum for that device. The client advances its confirmed
checksum in the same atomic transition that trims the acknowledged submit-log
prefix.

If the server admitted a request but its response was lost, retry observes the
server ahead. The client recomputes the checksum through the retained local
batches and trims only if it reaches the server's exact `(DeviceSeq,
DeviceChecksum)` pair. This yields exactly-once admission under response loss
without a separate submission id.

If the server is ahead and the retained local history cannot reproduce its
checksum, the client reports a fatal fork. It does not silently remint device
identity, discard history, or guess which branch is correct.

The checksum commits to one device's canonical submission stream under the
honest-server model. It is not a keyed authenticator. It does not prove that a
server returned every other device's admission, assigned truthful global
admission order, or gave two clients the same view.

## 7. Rollback

Rollback is caller-directed recovery after a definitive rejection. It must
not be used to resolve an ambiguous unavailable response.

`rollback(space, to)` atomically:

- appends `DeviceOp::Rollback { marker: to }` at the old submit tail;
- moves submit `neck` to that marker;
- advances `tail` past the marker;
- leaves `head` and retired rows intact.

This retires the entire active window, not merely one encoded mutation. The
marker later pushes as an empty admission batch, preserving forward-only
device sequence and checksum history while allowing gaps over retired rows.

A crash exposes either the complete pre-rollback or complete post-rollback
cursor/marker state. Retrying the exact completed rollback is idempotent while
that exact post-state remains current; a later append makes the old target
stale.

## 8. Leases

Leases are prefix reservations, not write capabilities and not data-log
operations. Acquire, renew, release, and list/repair are synchronous control
verbs absent from both the submit and admission logs.

### Compatibility

For overlapping prefixes:

- Read is compatible with Read.
- Write conflicts with Read and Write.

Acquire is all-or-nothing for its requested set. Incompatible self-overlap in
one acquire request is also rejected. There is no read-to-write upgrade, lease
stealing, fencing epoch, or pre-deadline takeover.

A point write conflicts with a live foreign lease whose prefix covers that
point. A range write conflicts in both directions: a foreign ancestor or
descendant reservation overlaps it. The submitting device's own leases do not
block its writes.

A write with no lease is admissible when no live foreign reservation
conflicts. Lease ids sent as admission evidence are diagnostics only and are
not checked as authority.

### Barrier and usability

Every newly acquired lease records the space's **global admission high-water**
at grant as its immutable `barrier`. This is conservative for a prefix lease:
unrelated admissions can force extra pulling, but no admission at or before
the grant can lie beyond the barrier. This global definition is the current
implementation and test contract; it is not a prefix-local maximum.

The client `lease()` method acquires, renews, or reuses reservations and pulls
until admit `tail > barrier`. Capture is not application authority. A locally
held lease is usable only when all are true:

```text
not forgotten
and locally live
and admit neck > lease.barrier
```

Thus a crash after recording a lease but before applying its barrier leaves a
durable but unusable lease. Advancing admit `tail` alone never makes it usable.

Renewal preserves the lease id and barrier while refreshing timestamps and
TTL. If a lease expired and must be acquired again, the new lease has a new id
and a fresh barrier.

### Clock domains

The request carries a client-minted `requested_at`; the server stores its own
`granted_at` and the granted TTL.

- The server expires at `granted_at + TTL` in the server clock domain.
- The originating client process uses its hybrid wall/monotonic deadline.
- A successor process cannot reuse the old monotonic ruler, so it uses the
  stored client wall deadline with an early-expiry margin of
  `max(TTL / 1000, 10ms)`.

Under the hybrid-clock regression checks and configured successor margin,
clock uncertainty can reduce local availability without extending local
authority beyond the server deadline. Local usable lease state is intended to
remain a subset of server authority; an undetected wall-clock error larger
than the assumed margin is outside that guarantee.

### Unlease and repair

Unlease first marks leases Forgotten durably, then performs remote release,
then deletes the local records after acknowledgement. Forgotten leases are
treated as nonexistent authority but remain retry state.

The intended `unlease_checked` contract is to reject removal when a queued
checked submission would lose its last live covering reservation. This
relationship is recomputed from all active checked submissions; submissions
are not permanently tied to a particular lease id. A live replacement
reservation is sufficient to preserve exclusion even if its barrier is not
yet application-usable.

**Review issue in the current implementation:** the scan only treats a lease
being removed as a guard when that lease is itself application-usable. This
leaves an edge case: an old usable lease can be released in favor of a live
replacement whose barrier is not yet applied, after which checked removal of
that replacement is not blocked. The queued assertion still receives full
server validation, so this can cause rejection rather than unauthorized
admission, but the stronger “checked unlease always preserves exclusion” law
is not currently enforced for that path.

`unlease_unchecked` skips this guard. It does not change server evaluation of
the queued assertions.

`repair_leases` obtains the server's complete live lease set for the device,
reconciles local records, preserves Forgotten intent, clears leases absent at
the authority, and pulls live barriers. Failed repair network I/O does not
first revoke otherwise valid local records.

## 9. Range assertions

A range assertion is:

```text
RangeAssert { prefix: P, upto: U }
```

For submitting device `D`, define `foreign_max(P, D)` as the greatest prior
`AdmissionSeq` of any mutation relevant to `P` authored by a device other
than `D`. Relevant history includes descendant point/range mutations and
ancestor DeleteRanges that cover `P`.

The server accepts the assertion exactly when:

```text
foreign_max(P, D) <= U
```

`upto` is inclusive. The server evaluates assertions against scratch state
immediately before each batch. Earlier batches from the same device are
excluded from `foreign_max`, which permits a device to queue dependent offline
submissions without forcing an admission boundary between them.

### Checked preflight

For `submit_checked`, the client additionally requires:

- a live, unforgotten Read or Write lease whose prefix covers `P`;
- admit `neck > lease.barrier`; and
- admit `neck > U`.

Equivalently, the locally applied frontier `N = neck - 1` must satisfy both
`N >= barrier` and `N >= U`.

The usual safe construction is:

1. acquire/reuse the covering lease;
2. pull and apply through its barrier;
3. validate the application invariant against state applied through `N`;
4. set `upto = N` and submit the assertion before the lease expires.

While that lease remains live, no foreign conflicting mutation can be
admitted under the reserved prefix. If the lease expires before admission, a
racing foreign mutation either remains absent or raises `foreign_max` and
causes the assertion to fail.

Homebase cannot verify step 3. It does not own application state and may no
longer retain all locally applied batches. The application must:

- make the assertion truthful for the state it actually inspected;
- declare every range whose foreign history can affect its invariant; and
- include its own queued same-device effects in that reasoning.

Checked submission proves local lease/cursor authority, not business-logic
correctness. Unchecked submission skips even that local proof.

## 10. Pull and the admit log

Server pull returns complete admitted batches for a dense interval:

```text
(after, through]
```

Empty admitted batches are included. Every response validates batch density,
entry `op_index`, redundant device metadata, and sequence agreement.

Client `pull()` pages from its durable admit tail. For each page it:

1. validates the complete dense response;
2. authenticates every encrypted entry before storing any batch in the page;
3. atomically appends the page, advances only admit `tail`, and raises the
   space version high-water.

A crash between pages leaves a valid shorter dense prefix. Retry resumes from
the durable tail. A malformed or unauthenticated page appends nothing.

### Admit-log cursors

The admit log has exclusive cursors satisfying:

```text
head <= neck <= tail
```

- `[head, neck)` is applied history still retained locally.
- `[neck, tail)` is captured history awaiting application.
- `tail` is the next server admission not captured.

The cursors are exact server `AdmissionSeq` positions. The canonical empty
state is `{ head: 1, neck: 1, tail: 1 }` while server high-water is 0.

`iter(from..to)` authenticates and returns exactly one retained dense interval
in server order; `iter_from_neck()` is convenience for the current
`[neck, tail)`. `mark_applied(to)` advances only `neck` to the exclusive
position `to`. `trim(to)` reclaims records below `to` and advances only `head`;
it may not trim beyond `neck`.

Pulling does not move `neck`. Marking does not trim. Trimming does not claim
new application progress.

### Multilite rebase

Multilite `rebase()` reconciles only the fetched interval that existed when it
started. It first snapshots both cursor triples and refuses to proceed unless
`submit.neck == submit.tail`; all range assertions are evaluated by the server
during the push that establishes this empty window. Rebase then authenticates
and decodes exactly `[admit.neck, admit.tail)`. Any malformed Multilite
operation fails before local application.

Multilite opens one SQLite savepoint, verifies that both submit and admit cursor
snapshots are unchanged, applies foreign operations in admission order, and
skips materialization for authenticated operations from the local device.
Those operations were materialized atomically before their successful push.
Only then does rebase advance admit `neck` to the snapshotted `tail` in that
same savepoint. Internal apply mode bypasses public SQL capture. A DDL,
metadata, or commit failure rolls back both application changes and cursor
movement. `rebase()` performs no pull, push, or implicit rollback.

### Application obligation

To avoid missing an operation, an application must never mark a batch applied
before its own corresponding state transition is durable.

The strongest pattern is a higher-level MetaStore integration that places the
application changes and `mark_applied(next_admission_seq)` in one transaction.
The reference ordered-store adapter cannot make an unrelated application
store participate automatically. If co-transactional application is not
possible, application must be idempotent:

- crash after apply but before mark may replay the batch;
- crash after mark but before durable apply removes the batch from the pending
  interval; it may later be trimmed and violates replica correctness.

Homebase cannot repair a non-idempotent external apply performed outside its
MetaStore transition.

## 11. Stateless fetch

`fetch(range, after)` asks for authenticated history relevant to one range in
`(after, at]`, where `at` is one atomic server cut. It does not change submit
cursors, admit cursors, version high-water, leases, or checked-submission
authority.

The cut is sparse with respect to the requested range: `at` may advance even
when no returned operation is relevant. Repeated point writes remain repeated
history rather than being compacted to final state.

A relevant DeleteRange source is returned when it is inside the requested
range or covers the requested range from an ancestor. Its effect for the
caller is the intersection of source and requested ranges. The authenticated
source operation itself is not rewritten; `FetchedRange::delete_range_effect`
computes the projection.

Separate fetch calls are separate cuts. Fetch is not a durable replication
cursor and does not make a lease usable.

## 12. Encryption and integrity

The encrypted namespace preserves component equality and prefix structure
needed by the server while hiding plaintext components. The server still
observes encoded structure, operation kinds, ciphertext sizes, devices,
timing, and access patterns.

Every mutation carries a `Seal` with scheme, 24-byte nonce, 16-byte AEAD tag,
and reserved payload. Scheme 0 requires an empty payload.

AEAD binds the mutation to:

- sealing scheme and operation kind;
- encoded key or range target;
- device id and device sequence;
- version and cipher epoch.

The server has no value key and cannot cryptographically verify AEAD. It
validates structural Seal rules and stores entries opaquely. Clients verify
AEAD before accepting pulled or fetched entries. Target, operation-kind,
version, sequence, ciphertext, nonce, or tag tampering therefore fails client
authentication.

Server-assigned `AdmissionTag` is not in AEAD because it is assigned after
encryption. The per-device checksum commits to the device stream, but global
admission completeness and order still rely on the honest-server boundary.

After opening an encrypted admitted entry, value plaintext is available but
key and range names remain in their deterministic encoded namespace; name
encoding is intentionally not reversible.

Anyone holding the shared space value key can mint valid ciphertext. AEAD is
not per-device signature or non-repudiation. Cipher epoch 0 also provides no
cryptographic member revocation.

## 13. Concurrency and isolation

The server serializes all verbs for one space through one actor. Successful
admission, lease transitions, reads, and barriers therefore have one
space-local linearization order. Different spaces may progress concurrently.

The client serializes public coordination workflows per space and permits
different spaces to progress concurrently. Slow storage, crypto, network, and
timer work is performed outside the coordinator's fast state owner, but a
same-space workflow currently holds its permit across that work. This gives a
single correctness owner at the cost of possible same-space head-of-line
blocking.

There is no cross-space transaction, ordering guarantee, or automatic
two-phase commit. Applications needing those semantics require a higher-level
protocol.

## 14. Derived guarantees

The following guarantees emerge from the preceding laws.

### Exactly-once server admission under retry

If one durable client store exclusively owns a `(space, device)` stream, the
server is honest, and retained batches remain available, ambiguous response
loss can be retried without double admission. `DeviceSeq` prevents replay and
`DeviceChecksum` distinguishes the exact already-admitted history from a fork.

This does not mean application side effects are exactly once; admit-log apply
must follow the application obligation in section 10.

### No stale resurrection

If clients allocate versions through the space MetaStore and first capture the
required history, a stale point Set cannot resurrect a value hidden by a newer
point or range mutation. Server version floors include hidden events.

### Race-free checked invariants

If an application truthfully validates every declared input range at its
applied frontier, holds covering live leases through admission, and submits
matching `upto` assertions, then no foreign conflicting admission can slip
between observation and the admitted dependent write. Lease exclusion prevents
the race while live; the server assertion detects a race after expiry.

Omitting an input range or validating against state newer/older than the
declared frontier voids this guarantee.

### Gap-free captured history

Under an honest server with retained history, the admit log is an exact dense
prefix of server batches. Page loss causes refetch, not a hole. Application
state is gap-free only if mark/apply obligations also hold.

### Conservative local lease authority

Remote-first acquire, local early expiry, Forgotten-before-release, barrier
application, and authoritative repair make usable local lease state a subset
of server authority. Failures may unnecessarily remove or delay local
authority, but should not manufacture it.

### Materialization/replay agreement

Under uncorrupted ordered storage, replaying the exact admission log in
`AdmissionOrder` produces the same point visibility, range visibility,
version floors, and effective counts as the server materialization.

## 15. Explicit non-guarantees

Homebase does **not** currently guarantee:

- that a locally returned `Submission` was remotely admitted;
- that pulled operations were applied to application state;
- that holding a lease alone means application data is current;
- that `submit_checked` verifies the application's asserted invariant;
- that a write needs or is authorized by its own lease;
- automatic rollback, merge, conflict repair, or retry policy;
- per-key dense versions or proof that a server omitted no key version;
- cross-space atomicity or ordering;
- downstream fencing for side effects outside Homebase;
- availability during partition, storage failure, or lease contention;
- Byzantine-server completeness, non-equivocation, or truthful global order;
- a whole-space cross-device checksum or authenticated checkpoint;
- server admission-log garbage collection or stale-cursor recovery;
- managed SQLite/KV storage or application of admitted operations;
- per-device signatures, non-repudiation, or cryptographic member revocation;
- historical MVCC snapshots merely because exact replay history exists.

## 16. Crash matrix

| Failure point | Durable result | Required recovery |
|---|---|---|
| Before local submission commit | No submission or consumed durable sequence | Retry submit if desired |
| After local submission commit | Complete queued submission | Push later |
| During server admission | Complete old or complete new server state | Retry push |
| After admission, before response | Server may be ahead | Retry; reconcile checksum |
| Definitive semantic rejection | Rejected batch and suffix remain queued | Repair, retry, or caller rollback |
| Before/after rollback transition | Complete old or complete marker/cursor state | Retry exact rollback if needed |
| After remote acquire, before local record | Server may hold an unknown local lease | `repair_leases` |
| After local lease record, before barrier apply | Lease is durable but unusable | Pull, apply, mark |
| After local Forgotten, before remote release | Lease is non-authoritative retry state | Retry unlease or repair |
| During pull page append | Complete old or complete extended dense prefix | Pull again |
| After application apply, before mark | Batch may replay | Application must be idempotent or atomic with mark |
| After mark, before application durability | Pending history can be skipped | Forbidden application ordering |
| During admit trim | Complete old or complete reclaimed prefix | Retry trim |

## 17. Retention and future proofs

The server currently retains every exact admission indefinitely. Client submit
history is trimmed after acknowledged checksum advancement. Client admit
history may be trimmed only below its applied neck.

Server-log garbage collection needs a new protocol with an authenticated
checkpoint, explicit stale-cursor response, and version floors sufficient to
preserve future regression checks. A whole-space checksum is useful only when
a client can receive every intervening canonical batch or verify a compact
proof. Neither property should be inferred from the present per-device
checksum.

Any optimization to range aggregates, replay, paging, or retention must
refine the simple append-only model. In particular:

- materialized reads must agree with replay;
- full pull must remain dense;
- scoped fetches must compose across adjacent cuts;
- projected range effects must never escape the requested range;
- crashes must expose one complete side of every atomic transition.
