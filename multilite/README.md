# multilite

Rust library for multi-writer SQLite with end-to-end encrypted sync, built on
the Homebase coordination kernel.

The documentation has one source for each kind of contract:

- [`SQL_GRAMMAR.md`](./SQL_GRAMMAR.md) defines the accepted SQL surface and the
  completion gate for every future grammar addition.
- [`SCHEMA_EVOLUTION.md`](./SCHEMA_EVOLUTION.md) defines stable identities,
  materialization, repair, and DDL compatibility.
- [`GUARDS.md`](./GUARDS.md) is the generated audit of every logical target,
  mutation, assertion, and rejection effect.
- [`../DESIGN.md`](../DESIGN.md) is the system architecture. The root
  `MULTILITE_CHALLENGES.md` is explicitly a historical vtable-era record.

**Not ready for production use.** The current surface is a small,
rusqlite-shaped connection wrapper with one-file/one-space bootstrap and
Homebase metadata. Public SQL currently permits restricted persistent
`CREATE TABLE`; table/column rename and column add/drop forms of `ALTER TABLE`;
`CREATE [UNIQUE] INDEX` and `DROP INDEX`; read-only `SELECT`; and captured
`INSERT`, `DELETE`, and `UPDATE` against non-reserved tables. Ordinary and
`STRICT` tables are supported. A rowid table must expose one exact `INTEGER
PRIMARY KEY` alias; richer and composite primary keys use `WITHOUT ROWID`.
Defaults, CHECK constraints, named constraints, ordered multi-column UNIQUE
keys, and immediate `NO ACTION` foreign keys participate in the synchronized
schema and guard model. Other verbs, caller-owned transactions, conflict
clauses, attached databases, `AUTOINCREMENT`, deferred or cascading foreign
keys, and unsupported expression/collation forms are rejected. The
`__multilite__` namespace is reserved. Exact boundaries live in the grammar
spec and executable parser tests rather than this overview.
The internal operation layer translates restricted table creation and captured
row insertion/deletion/replacement into lean logical operations. `Connection::update(|tx| ...)`
accumulates multiple statements into one UUID-keyed `MultiliteTransaction`,
while `Connection::view(|tx| ...)` owns one read-only SQLite snapshot. Their
transaction values provide SQLite-shaped `query`, `query_map`, and `prepare`
methods; updates additionally provide `execute`. The manifest is the first
Homebase mutation and carries the ordered operation frames.
Admission decoding re-lowers those frames and requires every following mutation
to match exactly. All local statement effects, their one Homebase submission,
and their one pending-effects row commit in one outer SQLite savepoint. Push,
pull, rebase, and rollback cover both schema and row operations. `push()` admits the active
Homebase stream, then atomically advances its local submit cursor and retires
every definitively accepted pending prefix in one SQLite savepoint. It returns
an opaque rejection handle without repairing a stalled suffix. Explicit
`rollback(&rejection)`
atomically runs the remaining reject effects in reverse order, retires the
pending suffix, and appends Homebase's empty rollback marker. That marker must
be pushed before rebase. `pull()` may capture admissions at any time, but
`rebase()` applies them only after the submit log is empty and treats admitted
empty markers as materialization no-ops. Range-assert conflicts are decided
exclusively by the server during push.
The general database owns this SQL gate, reserved namespace, schema catalog,
and row capture. Multilite does not create or require a built-in user table.

Each translated table creation contains immutable UUID identities for its
table, schema revision, primary index, columns, and logical indexes, plus the
exact SQL. Its
Homebase form records the immutable schema operation, canonical name lookup,
schema revision, index definitions, active primary index, and one mutable
`write-revision` cell whose value is the UUID of the latest DDL operation that
changed valid row lowering. The inverse translation verifies the complete
envelope and checks that stored SQL projects to the same structured operation.

SQLite's preupdate hook captures final inserted, deleted, and updated values
after affinity and conflict handling have run. One SQL statement becomes one
`RowChanges` operation: its ordered hook events are folded into deterministic
net before/after images for one synchronized table, while immutable table,
index, and relationship rules are stored once. `OR ABORT`, `OR IGNORE`, and
UPSERT chains containing only `DO NOTHING` therefore lower only the rows SQLite
actually changed. Row frames identify their schema revision and carry
column-UUID/value pairs using lossless SQLite storage classes. Primary-key
values become separate
Homebase key components under the table and primary-index UUID. Submissions
assert every exact row key, every non-NULL unique tuple, and the table's
active-primary-index and write-revision cells. Composite unique values occupy
one Homebase component per key part under their immutable index UUID;
their value identifies the owning row. Tuples containing NULL emit no ownership
cell, matching SQLite's distinct-NULL behavior. Accepted foreign row deltas
replay by stable IDs through the local schema catalog; the pending journal
restores the exact net before-images in the same transaction that rolls back
the Homebase submit window. Capture is fenced at 100,000 direct row events and
64 MiB per row operation and transaction; an oversized statement rolls back
with a typed error rather than retaining unbounded memory or reaching a codec
length panic.

Ordinary secondary indexes are synchronized schema and physical SQLite access
paths only. Their names and definitions converge across replicas, but row
operations emit no secondary-index Homebase cells and creating or dropping one
does not advance `write-revision`. Serializable read tracking therefore remains
coarse rather than charging snapshot-isolated writes for speculative future
index precision. Ordinary definitions may contain repeated column terms,
`ASC`/`DESC`, explicit collations, scalar expression terms, and a partial-index
predicate. These richer forms remain rejected for `UNIQUE` indexes until
Multilite can reproduce their exact comparison semantics in Homebase key
images. Tables and indexes acquire names through one shared schema-object
registry, matching SQLite's single namespace and making cross-kind collisions
ordinary admission conflicts rather than apply-time failures.

Table rename preserves the table, column, primary-index, UNIQUE-index,
secondary-index, foreign-key, and schema-revision UUIDs. It moves only the
canonical table-name registry cell; immutable DDL records keep their historical
SQL and stable references. When an operation must execute SQLite DDL, Multilite
renders its physical SQL from the current UUID-to-name bindings. A rename
therefore advances neither the authority schema head nor `write-revision`, and
stale row, index, or incoming-relationship operations compiled under the old
spelling remain valid. The local catalog updates its validated structural fold
and derives a new content fingerprint for those changed IR bytes, without
advancing the authority schema head or row write contract. Rejection runs the inverse binding change and physical
rename in the same canonical transaction.

Foreign-key declarations retain stable parent table, target index, and
ordered parent-column identities. Non-NULL child tuples write and assert an
exact reverse-reference cell keyed by the relationship UUID, parent target
image, and child row identity. Child deletes remove that cell; foreign-key or
child-primary-key updates move it. Parent deletes and changes to a referenced
key assert and delete only the exact relationship/parent prefixes they retire.
The range deletion fences stale child submissions in the opposite admission
order, while changes to unrelated parent columns remain independent. Creating
an incoming relationship also advances the parent's write contract so a parent
write compiled against the older catalog cannot slip through. A referenced
explicit UNIQUE index cannot be dropped until relationship evolution can
durably retarget or remove the relationship. SQLite continues to establish
immediate local existence and `MATCH SIMPLE` NULL behavior against the branch
snapshot.

`DELETE` currently accepts one unqualified, unaliased user table with an
optional `WITH` clause, SQLite predicate, and ordinary `INDEXED BY` or `NOT
INDEXED` hint; `RETURNING`, `ORDER BY`, and `LIMIT` remain rejected. SQLite
evaluates the predicate and the preupdate hook captures every complete old row
image. `RowChanges` lowers those rows to exact Homebase point deletes with the
same primary-index and write-revision guards as inserts. Remote apply verifies
the complete current row before deleting it, and rejection restores values
plus the hidden SQLite rowid when one exists. Zero-row deletes create no
logical transaction.

`UPDATE` uses the same target boundary as `DELETE`, while allowing `WITH`,
`UPDATE ... FROM`, tuple assignments, ordinary SQLite expressions and
subqueries, and ordinary index hints. SQLite supplies complete before/after row
images, so these forms use the same lowering rather than a parallel expression
evaluator. Stable-key updates lower to a point Set; primary-key moves lower to
a Delete of the old key followed by a Set of the new key, with both keys in the
conflict footprint. Across a multi-row operation every retired source is
deleted before any destination is set, so one row may move into another row's
former key. Rejection restores the old image. Integer primary-key moves
follow SQLite's rowid alias, while non-integer primary-key moves preserve their
hidden rowid; direct hidden-rowid changes remain unsupported.

`Connection::open` is the single file-lifecycle verb;
`MultiliteConnection` remains an alias for compatibility. Open initializes or
validates database identity, Homebase metadata, the pending-effects journal,
and the schema catalog in one general implementation path. Existing SQLite
user tables are preserved and remain readable. Inserts, deletes, and updates
against an adopted table are rejected until that table has a synchronized
schema identity.

A new database without options mints a public `DatabaseId` and local device
identity. Another replica is initialized by passing the first file's opaque,
versioned `ReplicaInvitation` through `OpenOptions`; an invitation supplied for
existing general state is an identity constraint and can never replace its
identity. Each database owns a Homebase client and uses an offline endpoint by
default; `OpenOptions::server` supplies an explicit `ServerHandle`.

`OpenOptions` also carries one `SyncPolicy`, defaulting to `LocalOnly`.
Local-only updates still commit atomically to SQLite, the Homebase submit log,
and the pending-effects log, but reads and writes perform no automatic network
work. Reopening with an authority under `LocalFirst` or `Remote` can therefore
deliver that buffered history. `LocalFirst { write_delay, read_staleness }`
schedules authority push no later than the oldest buffered write's deadline
and refreshes reads whose last applied authority observation is too old.
`Remote` waits for each update's admission and refreshes before every managed
view or update. Both synchronized policies require authority at open. A managed
closure refreshes at most once before its SQLite savepoint begins. Updates pin
their base snapshot before user code runs; views establish their snapshot on
their first query and retain it for every later query in the closure.

Isolation is configured independently through `IsolationLevel`, defaulting to
`Snapshot`. `OpenOptions::isolation_level` selects the connection default,
while `update_with(UpdateOptions::new(...), |tx| ...)` overrides one managed
update. Every operation contributes typed write and constraint prefixes to one
transaction footprint, which eagerly prunes redundant descendants as prefixes
arrive within each typed set. The final planner merges the selected antichains,
prunes cross-category overlap, and binds every range assertion to the
transaction's authority frontier. Snapshot isolation always includes writes
and mandatory constraints and executes reads directly against SQLite.
Serializable isolation also includes traced application reads. Production
tracking currently uses SQLite's authorizer to resolve every application table
read to its stable table UUID and records that table's complete Homebase root.
This deliberately coarse prefix covers joins, subqueries, trigger reads, and
write predicates without changing native SQLite execution. Exact primary-key
and index-prefix tracing remain future precision work; unsupported precision
always widens to the table root rather than omitting a dependency.

`tests/operation_pairs.rs` is the executable compatibility table for operation
pairs. Each row names two concrete logical operation families, their target
relationship, and the expected result under both isolation levels and both
admission orders. The harness requires the first submission to admit, checks
whether the second must admit or reject, performs rejection repair, converges
both replicas, runs SQLite and foreign-key integrity checks, and compares a
normalized schema-and-row observation. Pairs declared commutative must produce
the same observation in either order. A registry meta-test covers every current
logical operation family and requires examples of commutative, conflicting,
isolation-dependent, and directional relationships.

`BEGIN`, `COMMIT`, `ROLLBACK`, `SAVEPOINT`, and `RELEASE` are rejected inside
managed closures because the closure owns its outer lifecycle. Returning an
error or unwinding a panic rolls back the complete local update before any
Homebase submission survives. Direct `execute` and `query` remain convenient
one-statement managed transactions, and the existing reusable read-only
`prepare` surface retains its SQLite-shaped parameter and conversion behavior.

Every connection lifecycle and data operation also has a runtime-neutral async
form: `open_async`, `open_with_async`, `execute_async`, `query_async`,
`prepare_async`, `view_async`, `update_async`, `push_async`, `pull_async`,
`rebase_async`, and `rollback_async`. These futures wait asynchronously on the
typed committer and authority channels. Filesystem work, branch creation,
SQLite execution, and row mapping run on a bounded process-wide blocking pool,
so they do not occupy an async executor worker. Async parameters and returned
values must be owned and `Send`; reusable statement queries eagerly map rows
into owned values before crossing the worker boundary.

The closure passed to `view_async` or `update_async` is synchronous. It runs on
one blocking worker while borrowing a thread-bound SQLite transaction, and may
execute any number of currently supported statements without blocking the
caller's executor. Awaiting arbitrary application futures inside that closure
is deliberately unsupported. Once blocking or canonical work has entered its
bounded queue, dropping the caller's future does not cancel a possibly durable
side effect. The synchronous APIs remain available for rusqlite-compatible
borrowed parameters and closures and share the same branch, proposal, policy,
and authority machinery.

A required refresh first pushes a nonempty submit log, then pulls and
atomically rebases the available admissions. A definitive push rejection fails
the read and returns a rejection handle without implicitly rolling back
speculative SQLite state. A remote write does undo its own local SQLite effects
before returning a definitive rejection. Transport failure is not rejection:
durable local submissions remain available for retry because admission may be
ambiguous. Freshness is session-local and starts stale after every open. Inserts
and deletes against tables created through Multilite participate in every
synchronization policy; adopted tables without durable schema identities are
rejected by the row pipeline.

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
Speculative Multilite transaction manifests are stored under
`__multilite__pending`; this is a local disposition journal, not a second
operation log. The journal does not duplicate derived effects. The operation
compiler selects each inverse from the authenticated manifest, and rejection
unwinds those operations in reverse. CREATE TABLE rejection drops
the speculative table and catalog entry, ALTER TABLE rename rejection restores
the old physical and catalog names, INSERT rejection removes the exact
speculative rows, DELETE rejection restores each complete old row, and UPDATE
rejection restores its complete before images.
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
