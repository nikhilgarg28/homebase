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

See the [monorepo README](../README.md) and
[V1 plan](../MULTILITE_V1.md) for the current architecture and build sequence.
