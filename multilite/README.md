# multilite

Rust library for multi-writer SQLite with end-to-end encrypted sync, built on
the Homebase coordination kernel.

**Not ready for production use.** The current surface is a small,
rusqlite-shaped connection wrapper. Schema ownership, mutation capture,
metadata, and synchronization land in subsequent V1 batches.

See the [monorepo README](../README.md) and
[V1 plan](../MULTILITE_V1.md) for the current architecture and build sequence.
