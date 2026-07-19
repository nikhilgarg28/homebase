# multilite

Rust library for multi-writer SQLite with end-to-end encrypted sync, built on
the Homebase coordination kernel.

**Not ready for production use.** The current surface is a small,
rusqlite-shaped connection wrapper with one-file/one-space bootstrap and
Homebase metadata. Public SQL currently permits a restricted persistent
`CREATE TABLE`, read-only prepared `SELECT`, and `INSERT` against non-reserved
tables. A table must use the initial four declared types and exactly one inline
primary key; richer constraints and schema forms remain rejected. Other verbs,
caller-owned transactions, conflict clauses, attached databases, and
`AUTOINCREMENT` are rejected, and the `__multilite__` namespace is reserved.
The internal operation layer translates restricted table creation to and from
its complete Homebase log-and-revision-cell envelope. Local `CREATE TABLE` and
its Homebase submission now commit in one SQLite savepoint together with a
pending-effects row keyed by the assigned device sequence. Push, pull, rebase,
and rollback complete the schema synchronization loop before INSERT is
connected to synchronization. `push()` now admits the active Homebase stream,
then atomically advances its local submit cursor and retires every definitively
accepted pending prefix in one SQLite savepoint. It returns an opaque rejection
handle without repairing a stalled suffix. Explicit `rollback(&rejection)`
atomically runs the remaining reject effects in reverse order, retires the
pending suffix, and appends Homebase's empty rollback marker. That marker must
be pushed before rebase. `pull()` may capture admissions at any time, but
`rebase()` applies them only after the submit log is empty and treats admitted
empty markers as materialization no-ops. Range-assert conflicts are decided
exclusively by the server during push.
The general `Database` owns this SQL gate and reserved namespace. The
temporary V1 layer only initializes and validates its `items` representation
and captures inserts into that table.

Each translated table creation contains immutable UUID identities and exact
SQL. Its Homebase form contains an immutable operation record plus table and
canonical-name revision cells, with those cells also serving as range-assert
scopes. The inverse translation verifies the complete envelope and checks that
the stored SQL projects to the same structured operation.

`MultiliteConnection::open` is the single file-lifecycle verb. Internally it
first opens and commits a general Multilite database containing identity and
Homebase metadata, then a temporary V1 wrapper initializes or validates its
own local schema in a separate resumable transaction. A crash or V1 migration
failure can leave valid general state without V1 tables; the next open retains
the same database and device identities and retries V1 from its recorded
version. The wrapper is intended to disappear as general Multilite absorbs the
supported SQL surface.

A new database without options mints a public `DatabaseId` and local device
identity. Another replica is initialized by passing the first file's opaque,
versioned `ReplicaInvitation` through `OpenOptions`; an invitation supplied for
existing general state is an identity constraint and can never replace its
identity. Each database owns a Homebase client and uses an offline endpoint by
default; `OpenOptions::server` supplies an explicit `ServerHandle`.

`OpenOptions` also carries one `SyncPolicy`, defaulting to `LocalOnly`.
Local-only writes still commit atomically to SQLite, the Homebase submit log,
and the pending-effects log, but reads and writes perform no automatic network
work. Reopening with an authority under `LocalFirst` or `Remote` can therefore
deliver that buffered history. `LocalFirst { write_delay, read_staleness }`
schedules authority push no later than the oldest buffered write's deadline
and refreshes reads whose last applied authority observation is too old.
`Remote` waits for each write's admission and refreshes before every prepared
query. Both synchronized policies require authority at open.

A required refresh first pushes a nonempty submit log, then pulls and
atomically rebases the available admissions. A definitive push rejection fails
the read and returns a rejection handle without implicitly rolling back
speculative SQLite state. A remote write does undo its own local SQLite effects
before returning a definitive rejection. Transport failure is not rejection:
durable local submissions remain available for retry because admission may be
ambiguous. Freshness is session-local and starts stale after every open. Until
the general row-operation batch lands, synchronized policy modes reject
`INSERT` instead of presenting a local-only row as admitted.

V1 invitations and space envelopes are plaintext scaffolding. The stable API
is designed for a later encrypted default: a fresh open will mint the final
Homebase name and value keys, derive `DatabaseId` from the name key, and retain
the envelope locally. The invitation format can then carry or unlock that
envelope without changing `open` or `OpenOptions`. See the V1 plan's opening
and identity evolution section for the intended key-provider and sync path.

Multilite re-exports rusqlite's `params`, `Params`, `ToSql`, `FromSql`, `Type`,
`Value`, and `ValueRef` interfaces. Applications can therefore use the normal
SQLite parameter and conversion ecosystem rather than translating through a
Multilite-specific value model.

V1 item identities use a versioned, length-delimited canonical frame. Their
Homebase keys are fixed namespace components plus a domain-separated SHA-256
digest, so empty or large SQLite keys do not inherit Homebase component limits.

V1 uses SQLite's preupdate hook to capture inserted values before a statement
commits. Rusqlite enables that API through build-time bindings, so the current
Rust build requires libclang; packaging may revisit that tradeoff before the
first supported release.

Homebase client state is stored in the same SQLite file under
`__multilite__meta`.
Speculative Multilite operations and their explicit acceptance/rejection
effects are stored under `__multilite__pending`; this is a local disposition
journal, not a second operation log. Its versioned record codec stores repeated
effect lists: the initial CREATE TABLE acceptance list is empty, while its
rejection list drops the speculative table.
The ordered-store adapter executes synchronously under a serialized,
thread-reentrant connection owner: other threads cannot use the connection
concurrently, while metadata operations can join the outer SQLite savepoint
that is already running on the owning thread. Range scans eagerly own their
snapshot and retain neither a SQLite statement nor the connection lock.
Consecutive metadata puts and deletes are issued as bounded multi-row SQL
statements while preserving the original `WriteBatch` operation order.
V1 separately owns `__multilite__v1_schema`, a one-row local migration ledger.
Absence of that table means V1 version zero. Migration `0 -> 1` creates both
the ledger and `items` atomically; a committed general database therefore
remains safely retryable if V1 initialization has not happened yet. The
`__multilite__` table namespace is reserved for library-owned state.

See the [monorepo README](../README.md) and
[V1 plan](../MULTILITE_V1.md) for the current architecture and build sequence.
