# homebase

Leased KV kernel: prefix leases, seven verbs, E2EE-friendly coordination layer for [multilite](https://github.com/nikhilgarg28/multilite) and other clients.

**Not ready for production use.** API unstable.

## Crates

| Directory | crates.io |
|-----------|-----------|
| `core/` | [`homebase-core`](https://crates.io/crates/homebase-core) |
| `server/` | [`homebase-server`](https://crates.io/crates/homebase-server) |
| `client/` | [`homebase`](https://crates.io/crates/homebase) |
| `sim/` | (workspace only — not published) |

## Docs

- [DESIGN.md](./DESIGN.md) — architecture one-pager
- [LAUNCH_CHECKLIST.md](./LAUNCH_CHECKLIST.md) — multilite launch gates

## Publish (maintainers)

```bash
cargo publish -p homebase-core
cargo publish -p homebase-server   # after core is indexed
cargo publish -p homebase
```
