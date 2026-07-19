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
The internal operation layer can translate restricted table creation to and
from its complete Homebase log-and-revision-cell envelope. Local capture,
submission, pull, rebase, and rollback land in subsequent batches.
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
