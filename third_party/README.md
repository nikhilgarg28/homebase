# Third-Party Test Corpora

This directory is for large local conformance corpora. The corpora themselves
are not committed; only small lock files and fetch scripts are.

## SQL Logic Test

Fetch the current pinned corpus with:

```sh
multilite-conformance/scripts/fetch-sql-logic-test.sh
```

The checkout is expected at:

```text
third_party/sql-logic-test
```

The SQL Logic Test files used by the harness live under:

```text
third_party/sql-logic-test/src/main/resources/test
```

Current local checkout size after a shallow clone is about `1.1G`.
