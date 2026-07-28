- Make DST work seamlessly with SlateDB (including local disk cache)
- Client disk store: pick and implement the directory-tied `OrderedStore` backend (`client/src/client.rs::DiskStore`; redb is the candidate). Revisit local at-rest encryption as a store-level wrapper while at it.
- Serverless-born clients attaching a transport later: how does an oplog accumulated without leases acquire authority and ship? (Flagged in `client/src/client.rs` docs; device-scoped-prefix/ledger pattern may make it safe.)
- Rotation × push pipeline (deferred rotation tier — v1 has no cryptographic revocation): can a mutable-SQL generation boundary (eager re-key) occur while unshipped oplog entries still sit under the old key_epoch? Define the ordering (likely: drain the oplog before the generation rewrite; lazy prefixes unaffected).
- Threat-model doc (launch artifact): crypto-design section is largely written (DESIGN.md honest-but-dumb + key-hierarchy paragraphs, `client/src/client.rs` § Encryption); must include the minter-needs-name-key tension (hosted auth can mint partition-scoped claims only if the tenant shares the name key).
- Complete the crates.io name transition now reflected by the workspace: repurpose `homebase` from the old client SDK to the server package, publish the client as `homebase-client`, and retire or deprecate the old `homebase-server` and `homebased` packages as appropriate.
- Client interface reconciliation with the identity spec: `crypto.rs` Enclave/KeyBundle/bootstrap-record model → `SpaceEnvelope` + `homebase_client::identity` module (core modules never import it); `SystemRecord::Bootstrap` → `Envelope`; delete `derive_space_id` (ids are `HKDF(name_key)` commitments); drop `Replica::rotate_secret`/`rotate_space_key` from the v1 surface (keep `KeyEpoch`, permanently 0, reserved); `Client::open`'s enclave param becomes the envelope/keystore source.
- Device vs. account key layers: decide whether devices need their own keypairs beneath the Link (per-device keys would enable device-granular revocation without password rotation, device-to-device pairing without password entry, and per-device wrap entries in the envelope — vs. v1's single link_priv shared by all of a user's devices via the password-derived KEK). Related naming question: rename `Link` → `Account`? (Link was chosen for its neutrality — person, tenant, fleet, agent pool — where Account connotes only the person/tenant cases; but Account may communicate better. Decide before batch 11 freezes the vocabulary.)
- Device identity vs file copies (direction set in DESIGN.md — random-in-file id + unexpected-DeviceSeqRegression-as-fork-proof → re-mint & resync; ratify with the engine batch): remaining bits — whether to add early-warning heuristics (inode/host, per-device incarnation lease), and whether device-scoped ledger prefixes under a retired id migrate or just coexist.
- Client push/lease recovery cleanup: make push stalls distinguish lease-plane recoverable failures (`NotCovered`, `LeaseInvalid`, `Fenced`) from semantic write failures (`VerRegression`); add helper or retry path using `lease` for queued head keys, while keeping rollback manual for bad commits.
- Make `unlease_checked` cheaper. It currently scans the active oplog and re-evaluates usable lease coverage for every checked range assertion. Maintain local metadata indexing checked assertions by covering lease/prefix so unlease cost is proportional to affected guards rather than the full queue; preserve correctness across lease refresh, repair, rollback, and crash recovery.
- Fix checked-unlease replacement coverage. Today `unlease_checked` can remove a usable lease in favor of a live replacement whose barrier has not yet been applied, then permit that replacement to be removed because it is not usable. Preserve a live, usable covering reservation for every range assertion in queued checked submissions throughout replacement, refresh, repair, expiry, and crash recovery; add regression tests for the two-step removal sequence.
- Resolve lease-barrier scope and align code, tests, and documentation. The server currently records the space-global admission high water at grant time, while older design text describes a prefix-local barrier. Decide whether barriers are intentionally global or should become prefix-local, document the resulting semantics, and remove the contradictory contract everywhere.
- Evaluate a whole-space cumulative checksum as a sync/snapshot integrity layer. Unlike the per-device checksum used for push recovery, clients can validate a cross-device checksum only when they receive every intervening canonical batch or a compact proof; design it with changelog retention, snapshot manifests, and the existing per-prefix Merkle-hash idea rather than folding it into device admission.
multilite
- Add a randomized, two-replica integrity-audit simulation. Generate parent
  and child inserts, retargets, primary-key moves, and deletes; vary
  push/pull/rebase/restart order; then require converged SQLite rows, a valid
  schema relationship graph, an empty `PRAGMA foreign_key_check`, and exact
  agreement between materialized rows and reverse-reference cells. Generalize
  the resulting checker into a broader explicit database integrity audit.

- Exact reverse-reference assertions, parent prefix range fences, and parent
  targets backed by primary or explicit UNIQUE indexes have landed. A
  referenced explicit index currently cannot be dropped. Before widening the
  grammar, define retirement and GC for dropped relationships, then support
  durable retargeting/removal, affinity/collation coercion, self-references and
  cycles, deferred constraints, mutating actions, and add/drop relationship
  evolution.
  Existence remains established by SQLite against the branch snapshot;
  Homebase assertions certify the child/reference handshake, not an independent
  boolean witness.

- Foreign-writer tolerance after the SI Branch VFS is stable. The normal mode
  remains one cooperating Multilite committer per file. The WAL-derived map,
  cold-parse == incremental-parse invariant, and per-snapshot companion SQLite
  reader pins have landed. Remaining work is to fence committer apply against an
  unexpected WAL tip while holding SQLite's real write lock, poison stale
  writable branches, and rebuild after salt rotation. Treat another Multilite
  writer as a retryable all-range conflict. Detect stock-SQLite commits with a
  dedicated `PRAGMA data_version` observer, WAL epoch/tip comparison, schema
  cookie, and Multilite commit marker; enter an explicit externally-modified or
  quarantine state until a logical import/diff exists, since repairing the page
  map alone cannot create the missing Homebase operation. Include divergent
  physical-schema tests where an out-of-band writer installs a trigger or
  foreign-key action on a synchronized table: remote materialization and
  rejection repair must not silently produce unrepresented side-effect writes.
  Today this is principally a foreign/legacy-schema hazard because Multilite
  rejects trigger-generated public writes and cannot create those schema
  features. If Multilite later supports triggers or mutating FK actions, captured
  logical effects must still be materialized exactly once, either by suppressing
  trigger/FK execution during replay or by proving an equivalent centralized
  materialization invariant.

- Centralize local physical-schema validation under that foreign-writer and
  corruption boundary. Logical `MultiliteOp`s now completely describe canonical
  materialization, so proposals no longer carry touched-table fingerprints or a
  SQLite changeset. Have the committer maintain and validate one physical
  schema/catalog generation at snapshot issue and canonical apply. A mismatch
  should detect an out-of-band SQLite schema change and enter the same
  externally-modified/quarantine path; schema revision and index IDs plus
  range assertions remain the distributed DDL/DML conflict mechanism.

- Bound speculative DELETE capture deterministically. A DELETE currently buffers
  every complete old row image in memory and retains restoration data for
  rejection repair, so enforce explicit per-statement row and encoded-byte limits
  with atomic rollback and stable errors before claiming support for unbounded
  bulk deletes. Later evaluate spilling restoration images to disk, transaction-
  preserving chunking, and lowering eligible predicates to Homebase
  `DeleteRange`; range lowering must preserve precise local rollback data and
  correct snapshot/serializable conflict footprints.

- Bound `CREATE UNIQUE INDEX` backfills by deterministic row and encoded-byte
  limits before accepting unbounded tables. Later add spill/chunk support that
  preserves one logical schema operation and one atomic SQLite transaction.
  Retired unique-index definitions and ownership cells intentionally remain
  inert: `DROP INDEX` advances the schema head but not the write-contract
  revision, so operations compiled against the old index may still arrive and
  perform harmless superset bookkeeping. Garbage collection therefore needs an
  explicit device/frontier retirement protocol; never delete those index-owned
  namespaces merely because the local submit window is empty. Also extend the
  admitted grammar only alongside exact key-image support for collations, sort rules,
  and partial/expression UNIQUE indexes.

- Ordinary secondary-index DDL now converges without row-level Homebase index
  cells or `write-revision` churn. Keep serializable read tracing coarse until
  measurements justify an optional secondary-index conflict projection. If
  that projection is added, define its activation and backfill lifecycle so
  snapshot-isolated writes do not pay permanent mutation overhead merely
  because a SQLite access-path index exists.

- Continue reducing private-transaction startup overhead. The committer now
  incrementally streams appended WAL frames into its snapshot cache, native
  views reuse generation-pinned canonical readers, the WAL map uses
  structurally shared 256-page chunks, and the SQLite Session baseline
  connection is opt-in rather than part of every writable branch. The
  committer now attempts passive checkpoints above 64 MiB and refuses new
  snapshots above 256 MiB when live readers prevent cleanup. Next benchmark
  large databases and long-lived snapshots, replace per-branch VFS
  registration with one process-wide routing VFS, cache decoded catalog/schema
  metadata, and measure concurrent group-commit throughput. Reusing a writable
  branch SQLite handle itself still requires a proven reset/rebind protocol for
  schema caches, hooks, temp state, overlays, and snapshot identity.

- Extend the landed per-database authority actor beyond FIFO push/pull. Exact
  per-submission targeting and typed replies have landed; add transport request
  coalescing, retry/backoff, and cancellation-independent completion. Preserve
  the acyclic ownership rule: authority
  workflows may submit checked completion proposals and wait for the committer,
  while the committer may apply owned local Homebase transitions but must never
  wait for the authority actor or acquire its workflow permit. Completion
  proposals carry deterministic ids and expected cursors so stale responses are
  rejected or retried idempotently.

- client should run slatedb in single threaded tokio
- add more kinds of leases - forever lease, oneshot lease?
- Clock - track lineage so that we can track incarnation key from process restart
- Should client be renamed to be device or all device machinery (ID, seqnum etc) should be mapped to client (i.e ClientID)

key ver today is global lamport - make it lamport per hash bucket 2^16

support Device fencing

codec for smuggle admission seq, keep 64 random seqs, use trailing 0s to decide etc.
ensure key components can not be empty

Ensure that prefix can be empty but keys can not be

many responses should return global seqnum or return ops when range assert fails

Add bucketing/padding to key components & values before encrypting

admit log level checksum?

use uuid indirection for key components - better rotation

Migrate legacy core/client/server codecs to `homebase_core::writer::Writer` in a
separate mechanical commit, retaining byte-for-byte fixtures for every stable
format so the cleanup cannot accidentally change durable or wire encodings.

Handle multi-schema / attach etc

Async-first Multilite public API landed in batch 19: runtime-neutral futures
cover open, prepare, execute, query/view, update, push, pull, rebase, and
rollback. Owned SQLite work runs on a bounded process-wide blocking pool and
only owned mapped query results cross its boundary. The remaining follow-ups
are configurable worker sizing/shutdown, streaming owned-row APIs if demanded
by adapters, and cancellation/stress telemetry. Managed async transaction
closures intentionally remain synchronous while they borrow one thread-bound
SQLite connection.

Live queries, add db.watch api

Conformance suite

Store a single metadata table and put everything in it as triples of namespace, key, value


Change name of column from name-{name} to just <name> or <name>...[hash]

Take gem name for multilite

idea - split i64 in 8 components to make range locking / asserts finer


Modes
======
1. Keep regular sqlite, just add concurrent transactions + watch queries

2. Same as (1) but ability to sync to remote server

3.
