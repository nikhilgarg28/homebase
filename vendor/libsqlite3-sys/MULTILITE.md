# Multilite SQLite Patch

This directory vendors `libsqlite3-sys` 0.37.0 and its bundled SQLite 3.51.3
amalgamation. It is selected by the workspace-level `[patch.crates-io]`.

Multilite adds one connection-local C API:

```c
sqlite3_multilite_set_rowid_allocator(...)
```

The callback is consulted only when an ordinary table insert would otherwise
run SQLite's `OP_NewRowid`. SQLite still validates that each returned positive
rowid is unused. Explicit rowids, `WITHOUT ROWID`, virtual, `AUTOINCREMENT`, and
internal ephemeral tables retain upstream behavior.

When upgrading SQLite or `libsqlite3-sys`, reapply the changes marked by
`multilite` in `sqlite3/sqlite3.c` and `sqlite3/sqlite3.h`, then run
`cargo test -p multilite --test sqlite_rowid_allocator` before the full
workspace suite.
