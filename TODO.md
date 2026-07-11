- Make DST work seamlessly with SlateDB (including local disk cache)
- Client disk store: pick and implement the directory-tied `OrderedStore` backend (`client/src/client.rs::DiskStore`; redb is the candidate). Revisit local at-rest encryption as a store-level wrapper while at it.
- Serverless-born clients attaching a transport later: how does an oplog accumulated without leases acquire authority and ship? (Flagged in `client/src/client.rs` docs; device-scoped-prefix/ledger pattern may make it safe.)
- Rotation × push pipeline (deferred rotation tier — v1 has no cryptographic revocation): can a mutable-SQL generation boundary (eager re-key) occur while unshipped oplog entries still sit under the old key_epoch? Define the ordering (likely: drain the oplog before the generation rewrite; lazy prefixes unaffected).
- Threat-model doc (launch artifact): crypto-design section is largely written (DESIGN.md honest-but-dumb + key-hierarchy paragraphs, `client/src/client.rs` § Encryption); must include the minter-needs-name-key tension (hosted auth can mint partition-scoped claims only if the tenant shares the name key).
- Crate rename batch (per DESIGN.md "Naming, repo, and crate layout"): server crate + binary become `homebase` (subcommands: serve, resolver); the SDK becomes `homebase-client`. Sort the crates.io shuffle — `homebase` is currently published as the client SDK (0.1.1) and must be repurposed for the server; publish `homebase-client`; retire `homebase-server`; decide whether the just-created `homebased` placeholder is dropped or kept parked. Repo moves to `github.com/multilite/multilite`.
- Client interface reconciliation with the identity spec: `crypto.rs` Enclave/KeyBundle/bootstrap-record model → `SpaceEnvelope` + `homebase::identity` module (core modules never import it); `SystemRecord::Bootstrap` → `Envelope`; delete `derive_space_id` (ids are `HKDF(name_key)` commitments); drop `Replica::rotate_secret`/`rotate_space_key` from the v1 surface (keep `KeyEpoch`, permanently 0, reserved); `Client::open`'s enclave param becomes the envelope/keystore source.
- Device vs. account key layers: decide whether devices need their own keypairs beneath the Link (per-device keys would enable device-granular revocation without password rotation, device-to-device pairing without password entry, and per-device wrap entries in the envelope — vs. v1's single link_priv shared by all of a user's devices via the password-derived KEK). Related naming question: rename `Link` → `Account`? (Link was chosen for its neutrality — person, tenant, fleet, agent pool — where Account connotes only the person/tenant cases; but Account may communicate better. Decide before batch 11 freezes the vocabulary.)
- Device identity vs file copies (direction set in DESIGN.md — random-in-file id + unexpected-DeviceSeqRegression-as-fork-proof → re-mint & resync; ratify with the engine batch): remaining bits — whether to add early-warning heuristics (inode/host, per-device incarnation lease), and whether device-scoped ledger prefixes under a retired id migrate or just coexist.
- Client push/lease recovery cleanup: make push stalls distinguish lease-plane recoverable failures (`NotCovered`, `LeaseInvalid`, `Fenced`) from semantic write failures (`VerRegression`); add helper or retry path using `ensure` for queued head keys, while keeping rollback manual for bad commits.
- Make `release_checked` cheaper. It currently scans the active oplog and re-evaluates live lease coverage for every checked range assertion. Maintain local metadata indexing checked assertions by covering lease/prefix so release cost is proportional to affected guards rather than the full queue; preserve correctness across lease refresh, repair, rollback, and crash recovery.

multilite
- maintain versions of keys as of last sync point for rollback
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

Store crc for each client as well as global space? Use that to identify 
divergences + optinally identify when older seqnum is pushed and see if it's 
identical or different
