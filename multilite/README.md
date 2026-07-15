# multilite

Rust library for multi-writer SQLite with end-to-end encrypted sync, built on
the Homebase coordination kernel.

**Not ready for production use.** The current surface is a small,
rusqlite-shaped connection wrapper. Schema ownership, mutation capture,
metadata, and synchronization land in subsequent V1 batches.

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

Homebase client state is stored in the same SQLite file under `_mt_meta_kv`.
The ordered-store adapter executes synchronously under a serialized,
thread-reentrant connection owner: other threads cannot use the connection
concurrently, while metadata operations can join the outer SQLite savepoint
that is already running on the owning thread. Range scans eagerly own their
snapshot and retain neither a SQLite statement nor the connection lock.
Consecutive metadata puts and deletes are issued as bounded multi-row SQL
statements while preserving the original `WriteBatch` operation order.

See the [monorepo README](../README.md) and
[V1 plan](../MULTILITE_V1.md) for the current architecture and build sequence.
