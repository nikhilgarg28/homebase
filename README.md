# multilite

Multi-writer SQLite with end-to-end encrypted sync, built on the homebase coordination kernel.

**Not ready for production use.** APIs and durable formats are unstable. This
monorepo contains the Homebase kernel and an executable Multilite SQL layer for
a deliberately restricted SQLite grammar. See [`multilite/README.md`](./multilite/README.md)
for the currently implemented surface and limitations.

## Layout

| Path | Package | Purpose |
|------|---------|---------|
| `multilite/` | [`multilite`](https://crates.io/crates/multilite) | Rust SQL layer |
| `server/` | [`homebase`](https://crates.io/crates/homebase) | Kernel server library and binary |
| `client/` | [`homebase-client`](https://crates.io/crates/homebase-client) | Kernel client SDK |
| `core/` | [`homebase-core`](https://crates.io/crates/homebase-core) | Shared protocol vocabulary |
| `sim/` | `homebase-sim` | Deterministic simulation and torture rig |
| `npm/` | [`multilite`](https://www.npmjs.com/package/multilite) | JavaScript/TypeScript skin (currently empty) |
| `python/` | `multilite` | Python skin (currently empty) |

## Docs

- [DESIGN.md](./DESIGN.md) - architecture one-pager
- [physics.md](./physics.md) - current semantics, invariants, and guarantees
- [LAUNCH_CHECKLIST.md](./LAUNCH_CHECKLIST.md) - launch gates

## Current Multilite Surface

Multilite exposes a rusqlite-shaped connection wrapper plus managed snapshot
and update transactions:

```rust
use multilite::MultiliteConnection;

let db = MultiliteConnection::open("example.sqlite")?;
db.execute("CREATE TABLE example (id INTEGER PRIMARY KEY, value TEXT)", ())?;
let rows = db.query(
    "INSERT INTO example (value) VALUES ('hello') RETURNING id, value",
    (),
    |row| Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?)),
)?;
# Ok::<(), multilite::Error>(())
```

Prepared statements are classified as reads or writes from the parsed SQL;
`query` eagerly collects rows from either SELECT or DML `RETURNING`.
Managed updates execute native SQLite SQL on private branches, then send owned
logical proposals through one canonical committer. The current grammar includes
restricted table and index DDL plus INSERT, DELETE, and UPDATE. Homebase push,
pull, rebase, and rejection repair are implemented for that surface.

## Publish (maintainers)

```bash
# Rust
cargo publish -p homebase-core
cargo publish -p homebase        # after core is indexed
cargo publish -p homebase-client # after homebase and core are indexed
cargo publish -p multilite

# npm
cd npm && npm publish --access public

# PyPI
cd python
python -m venv .venv && source .venv/bin/activate
pip install build twine
python -m build && twine upload dist/*
```
