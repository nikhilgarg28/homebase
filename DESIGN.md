# homestead + homebase — a Leased KV Kernel and Multi-Writer SQLite on Top

*One-pager · July 2026 · **homestead** = the OSS kernel (Laravel-collision only, legally clean; crate free) · **homebase** = the SQL layer / commercial-facing product (live "HOMEBASE" mark, Homebase Inc., Classes 9/42 scoped to HR/labor — clearance opinion before public launch; crate free).*

## What it is

**Unmodified SQLite, multiple concurrent writers, constraints intact, coordinated by a small lease service instead of a consensus cluster — with E2EE nearly free because the server never interprets values.**

Every writer holds a full or partial local replica; reads are function calls, writes are locally-committed SQLite transactions (microseconds) validated authoritatively via held leases, shipped asynchronously as logical row changes. **The architecture in one line: a coherent write-back cache protocol for relational data — MESI for databases.** Everyone solved read-side caching (Turso, PowerSync, ElectricSQL replicate the authority's reads down); write-side caching is unsolved except via consensus RTTs or optimism-with-merges. Sticky ownership is the third option. Turso runs SQL at the center and replicates outward; homebase runs SQL at the edge and coordinates upward.

**Why E2EE is a moat, not a feature:** server-side query execution requires server-side plaintext (uniqueness needs equality, WHERE needs reading, indexes need order). Therefore E2EE ⇒ client-side execution ⇒ client-side enforcement ⇒ something lease-shaped. Authority-model systems cannot follow even in principle.

**Honest scope:** online-preferred with explicit bounded offline for mutable SQL (pin API: declared-intent long leases; sticky residue ≥ TTL−heartbeat for surprise drops); offline-first for append-shaped data (ledger pattern). Conceded openly: hot-contention rows resolve in seconds (lease migration) vs. Postgres's ~1ms lock queue — queue-like workloads are documented anti-patterns; partial replication alone is table stakes (others have it); durability is device-then-space (Litestream contract, per-table sync-commit opt-out, watermark API exposed).

**ACID label:** atomic (batch = transaction; torn batches impossible); consistent for all declared constraints globally (write skew on undeclared invariants at default isolation — same hole as Postgres defaults; `BEGIN IMMEDIATE` escalates); **serializable writes, three-tier reads** — local (0 RTT, per-shape freshness), owned (0 RTT, authoritative), consistent (1 RTT, true snapshot cut); durable per watermark.

## homestead — the kernel (frozen shape)

**The kernel in one line: an opaque KV server with read/write prefix leases.** One ordered map per space — tuple keys (`Vec<Bytes>`, component-wise prefix, order-preserving flat encoding: 0x00-escape + two-byte terminator, so tuple-prefix ⟺ encoded-byte-prefix and prefix scans are plain byte-range scans) → opaque values, tagged `(device, device_seq, epoch, ver, admission_seq)` — plus a lease table, registered subscriptions, and an **augmented tree maintaining max-admission-seq under any prefix**. Seven verbs:

```
acquire/renew/release   # mode read|write. TTL, batch acquire → barrier seq,
                        # contention piggybacks on renewal, steal denied pre-deadline
put_batch               # atomic; every key covered by valid write lease; epoch-fenced;
                        # per-key ver monotonicity enforced; tombstone deletes
get / list
read_at(ranges, cursors) -> (S, Δ)   # atomic consistent cut at one admission point;
                                     # also the delta feed that drives shapes
```

Prefix-scoped tokens enforced on reads AND writes. **Two lease modes:** write excludes everything; read coexists with read and excludes write. Read leases guard read sets (FK parents take read; child inserts take write on per-child guard keys; ledger appends = long write on device-scoped prefixes). Invariants: no write admission without covering valid write lease · no incompatible overlap · no read→write upgrade, ever · epochs are correctness, timestamps availability (asymmetric expiry) · strict local expiry · barrier = serializability · demand-driven stickiness, min-hold ≈ 2× heartbeat · TTL policy: kernel cap → class defaults (witness ~10s non-sticky, row ~5m, db ~1h) → app pin.

**Honest-but-dumb:** SIV per-component key pseudonymization (prefix relations survive at component boundaries); XChaCha20-Poly1305 values with coordination tags bound in the AEAD associated data — client-known fields only (device, device_seq, epoch, ver); admission_seq is server-assigned post-encryption, so admission-order splice protection comes from anchors, not AD; client-computed per-key ver chains (server enforces monotonicity); subtree anchors at generation boundaries for fork detection (full fork-consistency explicitly out of scope). Server clock exists in one code path (lease deadlines); server trust in clients: zero.

## homebase — the SQL layer (where all the smarts live)

**State = shape cache ⊕ oplog.** Shapes: SQL-vocabulary subscriptions compiled to index-prefix ranges, each at its cursor; eviction is shape-granular. Oplog: unshipped committed ops, persisted inside the local SQLite file (one fsync domain), drained eagerly. Owned regions are authoritative; unowned are snapshots.

**Witness compiler (the novel contribution):** speculative execute in savepoint → concrete write set via preupdate hook (catches triggers/cascades) → derive leases (row write; uniqueness witness write on collated value; FK parent read) → sorted batch-acquire (no deadlocks) → barrier → re-execute → coverage check → retry budget → table escalation. Handles collations, expression/partial indexes, read-beyond-row triggers (static scan → escalate), rowids (per-device block allocation). Query-side twin compiles reads to range fetches (PK/index point-gets, FK closure; unindexed → subscribe-table fallback). Compat: swap the open call; same dialect (it *is* SQLite); `SQLITE_BUSY` for contention/offline; DDL requires db-level write lease.

**What's novel vs. not:** leased-KV is Chubby lineage. The deltas: (1) mandatory enforcement fused with storage + barrier (vs. advisory locks); (2) the witness compiler — externalizing key-range locking onto a lease service so unmodified SQLite keeps constraints multi-writer (cr-sqlite/Ditto abandon them for CRDTs; no prior art found); (3) coordination over ciphertext.

## What else the kernel serves (homestead as a primitive)

Each is a thin client library over the same seven verbs — no kernel changes:

- **Job queue (SQS-lite)** — write lease on the job key = claim; TTL = visibility timeout; completion put, then release; crash = expiry reclaim; epoch = zombie-worker fencing. *Adopted as rung 2.5 dogfood consumer: a few hundred lines, no SQLite, exercises TTL/contention/fencing under crash injection before the compiler exists.*
- **Distributed cron** — write lease on `("cron", job, slot)` + idempotent fired-marker = exactly-once firing across N schedulers.
- **Leader election / service registry** — long write lease = leadership; epoch = the fencing token handed downstream; the Chubby use cases, E2EE-compatible.
- **Kafka-lite** — admission seq = total order; cursors = consumer offsets; consumer groups = write-lease claims on partition prefixes. Encrypted event streaming at team scale.
- **Config / feature-flag distribution** — admin holds write lease, fleets poll `read_at`: consistent multi-key config cuts, never torn flag combinations; range seq = free config version.
- **Dropbox-lite** — content-addressed chunks at `("cas", hash)` + manifest keys; E2EE file sync. (The consumer that forces the deferred blob/value-size decision.)
- **CRDT sync backend** — per-device write prefixes carrying Automerge/Yjs ops; drop-in encrypted relay for the existing local-first ecosystem. Adoption wedge.
- **Secrets vault, IoT state, document store** — as previously mapped.

## Trust & platform (post-proof-point)

App code never authenticates itself — roots are WebPKI + user password. Hosted auth (v1 cut: email/password, refresh rotation, verification, reset), split Argon2id derivation (auth_key ∥ KEK; OPAQUE later), DEK wrap/rewrap, recovery = escrow-or-code tenant knob. Per-tenant Ed25519 tokens with (space, prefix, device, exp); BYO backend = same claims, one verifier. Signup spam = friction economics (rate limits, verification-gated provisioning, quotas, App Attest knob). Directory/inbox/invites/ACLs = app-layer patterns over prefix tokens; Homestead collapsed into this — residual deep add-on is cert-chain key verification for the operator-hostile tier.

**OSS/commercial:** Apache-2.0 for kernel + full SQL layer *including compiler* + codec + single binary + BYO verification + reference auth; commercial = hosted auth, membership/token control plane, quotas/abuse, dashboards. Line = single-operator vs. multi-principal — architectural, not a gate.

## Milestones — kernel first, by design

The server is no longer trivially dumb (admission serialization, augmented range-max tree, atomic read_at cuts, lease overlap checks, fencing) — which *strengthens* kernel-first sequencing: it's now genuinely novel infrastructure deserving its own validation, and it's pure distributed-systems Rust with zero SQLite dependency.

1. **Kernel, in-memory** — client + server crates, all seven verbs, tuple encoding, augmented tree. *Exit: proptest invariant suite (overlap/mode compatibility, fencing, barrier, expiry asymmetry, ver monotonicity, read_at cut correctness). The crown jewel.*
2. **Kernel torture** — deterministic sim: virtual time, partitions, steal races, zombie writers, contended handoff. *Exit: 10k seeds clean.*
2.5. **Job-queue dogfood** — first real consumer, kernel-only. *Exit: crash-injected workers, zero double-completions, zero lost jobs.*
3. **Persistence + crypto codec** — storage trait (redb first); SIV/AEAD/HKDF codec. *Exit: kill -9 mid-batch → invariants hold; server dump = ciphertext only.*
4. **Single-writer SQL, logical** — one write lease on db prefix; hook capture; custom changesets; dumb-applier replicas; DDL-as-text; generation snapshots. *Exit: unmodified rusqlite app runs; checksum-matched replicas over randomized DML/DDL; ~1 RTT handoff.* (4.5 optional: hand-wired witnesses on a toy schema — falsify the mechanism cheaply.)
5. **Witness compiler + fixpoint + shapes.** *Exit: constraint gauntlet — concurrent colliding writers, zero silent violations.*
6. **Proof point** — 3 nodes, one DB, real Axum app; latency/throughput vs. Postgres; E2EE demo. Write-up leads with the compiler (mini-paper shape).

## Deliberate exclusions

No read→write lease upgrade, ever (release and re-acquire). No max_puts one-shot leases (dropped; claims are write-lease + marker patterns — revisit if the job-queue dogfood earns it back). No separate weak `sync` verb (read_at subsumes it: one admission sequence makes the consistent cut ~free; revisit only if incremental catch-up or long-poll delta feeds demand it). No server-side search/aggregation (permanent). No cross-lease-domain transactions. No optimistic CAS / range-CAS (range-version machinery exists via admission seqs; conditional writes noted as the future mechanism if an optimistic profile ever earns in — not built). No MVCC/server history (consistent reads are single-shot; pinned read transactions would need a retention window — deferred). No merge engine/CRDTs. No push channel (renewal piggyback suffices). Fork-consistency out of scope (anchors bound the window).

## Open decisions

Wire protocol (lean tonic) · changeset format custom vs. sessions (lean custom) · rung 4.5 in/out · repo public-from-spec vs. private-until-rung-4 · multi-process lease broker (post-v1) · homebase clearance (homestead needs none) · key limits (16 comps × 256B proposed) · read-tier API naming · LWW table class (deferred; would layer above kernel, HLC server-side max-wins if ever built).