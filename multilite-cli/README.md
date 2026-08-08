# multilite CLI

sqlite3-style LocalOnly REPL over the [`multilite`](../multilite) library crate.

```bash
cargo run -p multilite-cli -- /tmp/demo.db
```

```
mlite> CREATE TABLE notes (
   ...>   id INTEGER PRIMARY KEY,
   ...>   body TEXT NOT NULL
   ...> );
rows changed: 0
mlite> INSERT INTO notes VALUES (1, 'hello');
rows changed: 1
mlite> SELECT * FROM notes;
id | body
---+------
1  | hello
1 row
```

## v0 scope

- Required database path
- `SyncPolicy::LocalOnly` (no network sync)
- One Multilite statement at a time, terminated by `;`
- Interactive TTY uses rustyline; piped stdin uses the batch script runner
- `.help` / `.quit`

## Tests

```bash
cargo test -p multilite-cli
```

Library unit tests cover statement parsing, formatting, `execute_sql`, and
`run_script`. `tests/batch_shell.rs` spawns the `multilite` binary with piped
SQL to assert end-to-end stdout.

See atlas roadmap card **r63** / launch item **l20** for follow-ups (`:memory:`,
sync verbs, richer dot commands, Homebrew).
