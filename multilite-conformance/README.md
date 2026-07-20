# Multilite Conformance

This crate is the SQL compatibility harness for Multilite. It is built on the
Rust `sqllogictest` runner so that SQLite/DuckDB-style SQL Logic Test files can
be used directly instead of inventing a local test format.

Current scope:

- run one or more `.slt`/`.test` files against vanilla SQLite;
- run the same file shape against Multilite;
- emit a small JSON report suitable for later CI artifacts;
- keep the crate out of the workspace default members so normal workspace tests
  do not have to run a large external corpus.

The original SQLite SQL Logic Test corpus is value-wise by default, so this
harness starts each file with `resultmode valuewise`. Modern row-wise SLT files
can opt in explicitly with `resultmode rowwise`.

Example:

```sh
cargo run -p multilite-conformance -- \
  --engine multilite \
  --output target/conformance/basic.json \
  multilite-conformance/tests/slt/basic.slt
```

Run the local SQL Logic Test corpus checkout:

```sh
cargo run -p multilite-conformance -- \
  --engine sqlite \
  --output target/conformance/sql-logic-test-sqlite.json \
  --corpus third_party/sql-logic-test/src/main/resources/test
```

Compare SQLite and Multilite on the same corpus:

```sh
cargo run -p multilite-conformance -- \
  --engine both \
  --output target/conformance/sql-logic-test-both.json \
  --corpus third_party/sql-logic-test/src/main/resources/test
```

The corpus checkout is intentionally ignored by git. Recreate it with:

```sh
multilite-conformance/scripts/fetch-sql-logic-test.sh
```

Observed local corpus size after a shallow clone:

```text
checkout: 1.1G
test files: 622
```

First SQLite-reference baseline on this branch:

```text
files: 622
records: 10,655,429
passed: 10,645,113
failed: 10,316
```

The remaining SQLite-reference failures are currently harness/corpus dialect
debt, not Multilite failures. The first class seen is legacy SQLite SLT
type-rendering behavior, for example aggregate queries declared as `query I`
where `rusqlite` naturally returns `0.5` or `1.0` but the historical script
expects integer-ish rendering.

Near-term next steps:

- classify unsupported records separately from actual failures;
- add progress output and sharding for long corpus runs;
- normalize or classify legacy SQLite SLT type-rendering differences;
- add a Tcl extractor only after the sqllogictest corpus is useful.
