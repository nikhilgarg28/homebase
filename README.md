# multilite

Multi-writer SQLite with end-to-end encrypted sync, built on the homebase coordination kernel.

**Not ready for production use.** APIs are unstable. This monorepo currently contains the Homebase kernel and reserved Multilite package shells; the SQL layer has not landed yet.

## Layout

| Path | Package | Purpose |
|------|---------|---------|
| `src/` | [`multilite`](https://crates.io/crates/multilite) | Rust SQL layer (currently empty) |
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
