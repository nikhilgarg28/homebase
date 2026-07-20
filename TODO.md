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
- maintain some metadata about the last sqllite wal multilite saw - makes it easy
to detect that multilite db was written to from sqlite later

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

Have Writer class like Reader

Explroe if we can support concurrent write transactions, maybe via separate redb file for transactions

Handle multi-schema / attach etc

Async-first Multilite/database actor:
- Replace the current operation mutex, direct `ConnectionOwner` access,
  scheduler calls, and statement refresh callback with one database actor that
  owns the SQLite connection, Homebase client, pending effects, sync-policy
  state, and complete submit/push/pull/rebase/rollback workflows. Do not add a
  Homebase-only worker: local SQLite changes and Homebase metadata must remain
  in the same transaction.
- Send owned commands over a bounded channel and return results through oneshot
  replies. Define cancellation, backpressure, shutdown, and durable-side-effect
  semantics explicitly; dropping a caller must not cancel an operation after
  it may have committed.
- Make the internal and primary API async. Keep SQLite-compatible synchronous
  methods as thin blocking wrappers over the same command/reply path so the two
  surfaces cannot acquire different behavior.
- Return owned query rows and column metadata from the actor because
  `rusqlite::Row` borrows its statement and connection and cannot cross the
  channel. Perform public mapping and `FromSql` conversion on the caller side.
- Represent transactions with logical handles or atomic batch commands rather
  than borrowed `rusqlite::Transaction` values. Start with sequential actor
  execution; add concurrent in-flight authority work only through an explicit
  state machine that preserves SQLite/Homebase transaction boundaries.

Live queries

Conformance suite

Store a single metadata table and put everything in it as triples of namespace, key, value

use db.view, db.update, db.watch api

Change name of column from name-{name} to just <name> or <name>...[hash]