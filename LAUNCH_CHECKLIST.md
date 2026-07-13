# multilite — Launch Readiness Checklist

Gate for the public announcement ("SQLite with end-to-end encrypted sync", single-writer,
multi-writer explicitly roadmap). Governing rule: **every sentence in the announcement must
have a reproducible artifact behind it.** Work top to bottom; nothing ships while a claimed
item is unchecked.

Product ships as **multilite** (SQLite built on [homebase](./DESIGN.md)). This repo holds
the homebase kernel; multilite gets its own repo at launch.

## 1. Functionality floor (rung 4 + polish)

- [ ] Drop-in `open()` with explicit multilite lease policy via connection-string options
- [ ] Live replicas: sub-second lag while connected
- [ ] Graceful writer handoff through checked unlease + reacquire; document that v1 has
      no pre-deadline stealing or forced takeover
- [ ] E2EE on by default; key from config; XChaCha20-Poly1305 values + SIV key components
- [ ] Deployment trio all working:
  - [ ] Embedded mode (in-process kernel, cargo feature)
  - [ ] Single-binary server
  - [ ] S3-backed shard (slatedb), manifest-CAS failover
- [ ] kill -9 anywhere → clean recovery (client and server)
- [ ] Bootstrap/restore a fresh replica by dense exact-log replay; define authenticated
      checkpoints before introducing server-log GC
- [ ] **Local-first genesis + attach**: mint and persist the envelope, register the space,
      and upload initial application state through ordinary submissions or a future
      authenticated checkpoint protocol; attach itself neither uploads data nor acquires a lease
- [ ] Durability API: `Submission::push()` disposition plus durable submit/admit cursors
- [ ] Linux + macOS support
- [ ] Rust client (rusqlite-compatible wrapper)
- [ ] C-ABI shim (`libmultilite` returning real `sqlite3*`) — proves "unmodified apps" claim
  - [ ] Python via the shim as the demo consumer
- [ ] **Acid test: an existing rusqlite app runs unmodified except `open`**
      (someone on the thread will try this within an hour)

## 2. Correctness evidence

- [ ] Proptest invariant suite public, in CI, badged (I1–I15 from spec)
- [ ] Deterministic simulator: seed-reproducible failures, 10k-seed CI run, badged
      (DST-as-identity — make it a named, documented artifact)
- [ ] Differential conformance harness: same script minus LEASE lines → identical results
      vs vanilla SQLite
- [ ] Crash-injection matrix (client mid-submit/apply/mark, server mid-batch, S3 mid-flush)
- [ ] **Money demo (public script):** two writers + partition → checked range assertion
      prevents a stale invariant-dependent write; contrast with an uncoordinated replica
- [ ] Threat-model doc: exact ciphers, key derivation, and **what the server sees**
      (pseudonymized key structure, sizes, timing)
- [ ] "Not yet externally audited" stated plainly in the crypto docs

## 3. Benchmarks (public scripts, disclosed hardware, losses included)

- [ ] Overhead vs vanilla SQLite: local read/write throughput + latency
      — reads must be ~single-digit-% overhead or it's a fix-before-launch item
- [ ] Replication characteristics: submit-to-admit lag, admit-log apply lag, bootstrap time per GB,
      codec throughput on an ARM core (phone-ingest number)
- [ ] Vs neighbors: replication lag + restore vs Litestream; local-write latency
      vs Turso embedded (0-RTT vs RTT chart)
- [ ] Honest weaknesses rows: cold-acquire latency, S3-tier ack times, failover time

## 4. Docs

- [ ] Five-minute quickstart that actually takes five minutes
- [ ] **Physics contract page**: durability tiers, freshness tiers, what BUSY now means,
      offline budget, storage overhead (application file + submit/admit logs locally;
      exact admission log + materialized state on the server until checkpoint/GC exists)
- [ ] Comparison table incl. weaknesses column
- [ ] Kernel spec published (v0.2)
- [ ] Related-work write-up published (mini-paper framing)
- [ ] Pre-written FAQ (one paragraph each):
  - [ ] Why not CRDTs
  - [ ] vs Litestream / vs LiteFS
  - [ ] vs Turso
  - [ ] vs cr-sqlite / Ditto
  - [ ] vs PowerSync / ElectricSQL
  - [ ] vs rqlite / dqlite
  - [ ] What does the server see
  - [ ] What happens when my lease expires offline
  - [ ] Is this a SQLite fork (no; trademark-respecting naming posture)
  - [ ] Where's multi-writer (roadmap; constraint-preservation thesis teased)
  - [ ] Can I self-host (yes: one binary, or embedded, or S3-only)

## 5. Website (minimal by design)

- [ ] Landing page: tagline + two-line stack + killer-demo GIF/terminal-cast
      (two devices submitting and pulling encrypted operations; server dump stays opaque)
- [ ] Quickstart link + GitHub link + CI badges
- [ ] No overproduced marketing (inverse credibility on zero-mile projects)

## 6. Launch discipline

- [ ] Announcement scope excludes managed SQLite apply, partitions, shapes, RBAC, and LEASE grammar
      (rung-5-era surface; half-built dilutes the claim) — roadmap mentions only
- [ ] Venue sequencing: r/rust first → HN/r/programming ~a week later with
      r/rust reception as social proof
- [ ] Every announcement sentence cross-checked against its artifact above

## Blocking prerequisite

- [ ] **Spec v0.2 written and reviewed** — remaining decisions (multilite application,
      partitions + LEASE grammar, lifecycle presets, durability tiers, authenticated
      checkpoints/log retention, S3-HA, metering counters, open/packaging shape, tuple-key
      limits, …). The physics doc and threat model both derive from it.
