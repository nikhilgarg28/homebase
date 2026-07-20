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
The internal operation layer translates restricted table creation and captured
row insertion to and from complete Homebase envelopes. Local `CREATE TABLE` or
multi-row `INSERT`, its Homebase submission, and its pending-effects row commit
in one SQLite savepoint. Push, pull, rebase, and rollback cover both schema and
row operations. `push()` now admits the active Homebase stream,
then atomically advances its local submit cursor and retires every definitively
accepted pending prefix in one SQLite savepoint. It returns an opaque rejection
handle without repairing a stalled suffix. Explicit `rollback(&rejection)`
atomically runs the remaining reject effects in reverse order, retires the
pending suffix, and appends Homebase's empty rollback marker. That marker must
be pushed before rebase. `pull()` may capture admissions at any time, but
`rebase()` applies them only after the submit log is empty and treats admitted
empty markers as materialization no-ops. Range-assert conflicts are decided
exclusively by the server during push.
The general database owns this SQL gate, reserved namespace, schema catalog,
and row capture. Multilite does not create or require a built-in user table.

Each translated table creation contains immutable UUID identities for its
table, schema revision, row keyspace, and columns, plus the exact SQL. Its
Homebase form records the immutable schema operation, canonical name lookup,
schema revision, row-keyspace definition, active row keyspace, and one mutable
`write-revision` cell whose value is the UUID of the latest DDL operation that
changed valid row lowering. The inverse translation verifies the complete
envelope and checks that stored SQL projects to the same structured operation.

SQLite's preupdate hook captures final inserted values after affinity has run.
One SQL statement becomes one `InsertRows` operation even when it inserts many
rows. Row frames identify their schema revision and carry column-UUID/value
pairs using lossless SQLite storage classes. Primary-key values become separate
Homebase key components under the table and row-keyspace UUID. Submissions
assert every exact row key plus the table's active-row-keyspace and
write-revision cells. Accepted foreign rows replay by stable IDs through the
local schema catalog; rejected local rows are deleted by the pending journal in
the same transaction that rolls back the Homebase submit window.

`Connection::open` is the single file-lifecycle verb;
`MultiliteConnection` remains an alias for compatibility. Open initializes or
validates database identity, Homebase metadata, the pending-effects journal,
and the schema catalog in one general implementation path. Existing SQLite
user tables are preserved and remain readable. Inserts into an adopted table
are rejected until that table has a synchronized schema identity.

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
ambiguous. Freshness is session-local and starts stale after every open. Inserts
into tables created through Multilite participate in every synchronization
policy; adopted tables without durable schema identities are rejected by the
row pipeline.

Current invitations and space envelopes are plaintext scaffolding. The API
is designed for a later encrypted default: a fresh open will mint the final
Homebase name and value keys, derive `DatabaseId` from the name key, and retain
the envelope locally. The invitation format can then carry or unlock that
envelope without changing `open` or `OpenOptions`.

Multilite re-exports rusqlite's `params`, `Params`, `ToSql`, `FromSql`, `Type`,
`Value`, and `ValueRef` interfaces. Applications can therefore use the normal
SQLite parameter and conversion ecosystem rather than translating through a
Multilite-specific value model.

Multilite uses SQLite's preupdate hook to capture inserted values before a
statement commits. Rusqlite enables that API through build-time bindings, so
the current Rust build requires libclang; packaging may revisit that tradeoff
before the first supported release.

Homebase client state is stored in the same SQLite file under
`__multilite__meta`.
Speculative Multilite operations and their explicit acceptance/rejection
effects are stored under `__multilite__pending`; this is a local disposition
journal, not a second operation log. Its versioned record codec stores repeated
effect lists: CREATE TABLE rejection drops the speculative table and catalog
entry, while INSERT rejection removes the exact speculative rows.
`__multilite__schema` is the local lookup index from SQLite names and stable
table UUIDs to authenticated schema definitions.
The ordered-store adapter executes synchronously under a serialized,
thread-reentrant connection owner: other threads cannot use the connection
concurrently, while metadata operations can join the outer SQLite savepoint
that is already running on the owning thread. Range scans eagerly own their
snapshot and retain neither a SQLite statement nor the connection lock.
Consecutive metadata puts and deletes are issued as bounded multi-row SQL
statements while preserving the original `WriteBatch` operation order.
The `__multilite__` table namespace is reserved for library-owned state.

See the [monorepo README](../README.md) and
[design notes](../DESIGN.md) for the current architecture and build sequence.
