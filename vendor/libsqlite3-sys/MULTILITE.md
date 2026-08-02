# Multilite SQLite Patch

This directory vendors `libsqlite3-sys` 0.37.0 and its bundled SQLite 3.51.3
amalgamation. It is selected by the workspace-level `[patch.crates-io]`.

Multilite adds one connection-local C API:

```c
sqlite3_multilite_set_rowid_allocator(...)
```

The standard bundled amalgamation is also generated with SQLite's
`SQLITE_UDL_CAPABLE_PARSER` marker and compiled with
`SQLITE_ENABLE_UPDATE_DELETE_LIMIT`. This keeps write-row selection inside
SQLite for `UPDATE` and `DELETE` statements using `ORDER BY`, `LIMIT`, or
`OFFSET`. The SQLCipher amalgamation is unchanged.

The bundled build script checks the amalgamation for that parser marker before
enabling the compile option. An upstream refresh using the ordinary parser now
fails at build time instead of producing a library that advertises a feature
its parser cannot accept.

The callback is consulted only when an ordinary table insert would otherwise
run SQLite's `OP_NewRowid`. SQLite still validates that each returned positive
rowid is unused. Explicit rowids, `WITHOUT ROWID`, virtual, `AUTOINCREMENT`, and
internal ephemeral tables retain upstream behavior.

When upgrading SQLite or `libsqlite3-sys`:

1. Generate the standard amalgamation from the matching SQLite source tree
   with `make OPTIONS=-DSQLITE_ENABLE_UPDATE_DELETE_LIMIT sqlite3.c`. The
   resulting `sqlite3.c` must define `SQLITE_UDL_CAPABLE_PARSER`.
2. Reapply the changes marked by `multilite` in `sqlite3/sqlite3.c` and
   `sqlite3/sqlite3.h`.
3. Run `cargo test -p multilite --test sqlite_features` and
   `cargo test -p multilite --test sqlite_rowid_allocator` before the full
   workspace suite.
