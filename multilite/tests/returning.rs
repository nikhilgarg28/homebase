use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;

use homebase_core::space::SpaceId;
use multilite::{Error, MultiliteConnection, OpenOptions, PushOutcome, Value};

mod common;

use common::{router, server};

fn sqlite_execute_returned_results(error: &Error) -> bool {
    matches!(
        error,
        Error::Sqlite(error)
            if matches!(error.as_ref(), rusqlite::Error::ExecuteReturnedResults)
    )
}

fn row_count(database: &MultiliteConnection, table: &str) -> i64 {
    database
        .query(&format!("SELECT count(*) FROM {table}"), (), |row| {
            row.get::<_, i64>(0)
        })
        .unwrap()[0]
}

fn first_i64(row: &rusqlite::Row<'_>) -> rusqlite::Result<i64> {
    row.get(0)
}

fn first_string(row: &rusqlite::Row<'_>) -> rusqlite::Result<String> {
    row.get(0)
}

#[derive(Debug, PartialEq, Eq)]
struct LocalWriteState {
    pending: i64,
    commit_seq: String,
    last_device_seq: Option<String>,
}

fn local_write_state(path: &std::path::Path) -> LocalWriteState {
    let connection = rusqlite::Connection::open(path).unwrap();
    LocalWriteState {
        pending: connection
            .query_row("SELECT count(*) FROM __multilite__pending", (), |row| {
                row.get(0)
            })
            .unwrap(),
        commit_seq: connection
            .query_row(
                "SELECT hex(commit_seq) FROM __multilite__commit_state WHERE singleton = 1",
                (),
                |row| row.get(0),
            )
            .unwrap(),
        last_device_seq: connection
            .query_row(
                "SELECT max(hex(device_seq)) FROM __multilite__pending",
                (),
                |row| row.get(0),
            )
            .unwrap(),
    }
}

#[test]
fn direct_and_prepared_queries_execute_every_returning_dml_verb() {
    let directory = tempfile::tempdir().unwrap();
    let database = MultiliteConnection::open(directory.path().join("returning.sqlite")).unwrap();
    database
        .execute(
            "CREATE TABLE notes (
                id INTEGER PRIMARY KEY,
                body TEXT NOT NULL,
                revision INTEGER NOT NULL DEFAULT 1
            )",
            (),
        )
        .unwrap();

    let mut inserted = database
        .query(
            "INSERT INTO notes (body) VALUES ('one'), ('two')
             RETURNING id, body, revision",
            (),
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                ))
            },
        )
        .unwrap();
    inserted.sort_by(|left, right| left.1.cmp(&right.1));
    assert_eq!(
        inserted
            .iter()
            .map(|(_, body, revision)| (body.as_str(), *revision))
            .collect::<Vec<_>>(),
        [("one", 1), ("two", 1)]
    );
    assert!(inserted.iter().all(|(id, _, _)| *id >= 1_i64 << 47));

    let mut insert = database
        .prepare("INSERT INTO notes (body) VALUES (?1) RETURNING id, upper(body)")
        .unwrap();
    assert!(!insert.readonly());
    let prepared = insert
        .query_map(["three"], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
        })
        .unwrap();
    assert_eq!(prepared.len(), 1);
    assert_eq!(prepared[0].1, "THREE");

    let updated = database
        .query(
            "UPDATE notes
             SET body = upper(body), revision = revision + 1
             WHERE body <> 'two'
             RETURNING body, revision",
            (),
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?)),
        )
        .unwrap();
    assert_eq!(
        updated
            .into_iter()
            .collect::<std::collections::BTreeSet<_>>(),
        [(String::from("ONE"), 2), (String::from("THREE"), 2)].into()
    );

    let deleted = database
        .query(
            "DELETE FROM notes WHERE revision = 1 RETURNING body",
            (),
            |row| row.get::<_, String>(0),
        )
        .unwrap();
    assert_eq!(deleted, ["two"]);
    assert_eq!(row_count(&database, "notes"), 2);
}

#[test]
fn limited_writes_return_only_rows_selected_by_native_sqlite() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("limited-returning.sqlite");
    let database = MultiliteConnection::open(&path).unwrap();
    database
        .execute(
            "CREATE TABLE notes (
                id INTEGER PRIMARY KEY,
                score INTEGER NOT NULL,
                body TEXT NOT NULL
            )",
            (),
        )
        .unwrap();
    database
        .execute(
            "INSERT INTO notes VALUES
                (1, 10, 'one'), (2, 40, 'two'),
                (3, 30, 'three'), (4, 20, 'four')",
            (),
        )
        .unwrap();

    let mut update = database
        .prepare(
            "UPDATE notes SET body = upper(body)
             RETURNING id, body ORDER BY score DESC, id LIMIT ?1 OFFSET ?2",
        )
        .unwrap();
    let mut updated = update
        .query_map((2, 1), |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
        })
        .unwrap();
    updated.sort_by_key(|(id, _)| *id);
    assert_eq!(updated, [(3, "THREE".into()), (4, "FOUR".into())]);

    let mut deleted = database
        .query(
            "DELETE FROM notes RETURNING id ORDER BY score, id LIMIT 2",
            (),
            |row| row.get::<_, i64>(0),
        )
        .unwrap();
    deleted.sort_unstable();
    assert_eq!(deleted, [1, 4]);
    assert_eq!(
        database
            .query("SELECT id, body FROM notes ORDER BY id", (), |row| {
                Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
            })
            .unwrap(),
        [(2, "two".into()), (3, "THREE".into())]
    );

    let before = local_write_state(&path);
    assert!(
        database
            .query(
                "UPDATE notes SET body = 'never'
                 RETURNING id ORDER BY id LIMIT 0",
                (),
                first_i64,
            )
            .unwrap()
            .is_empty()
    );
    assert_eq!(local_write_state(&path), before);
}

#[test]
fn query_and_execute_enforce_rows_vs_changes_without_mutating() {
    let directory = tempfile::tempdir().unwrap();
    let database =
        MultiliteConnection::open(directory.path().join("returning-modes.sqlite")).unwrap();
    database
        .execute("CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)", ())
        .unwrap();

    let error = database
        .execute("INSERT INTO notes VALUES (1, 'execute') RETURNING id", ())
        .unwrap_err();
    assert!(sqlite_execute_returned_results(&error));

    assert!(matches!(
        database.query("INSERT INTO notes VALUES (2, 'query')", (), |_| Ok(())),
        Err(Error::StatementModeMismatch)
    ));

    assert!(matches!(
        database.view(|view| {
            view.query(
                "INSERT INTO notes VALUES (3, 'view') RETURNING id",
                (),
                |row| row.get::<_, i64>(0),
            )
        }),
        Err(Error::StatementModeMismatch)
    ));

    let mut returning = database
        .prepare("INSERT INTO notes VALUES (4, 'prepared execute') RETURNING id")
        .unwrap();
    assert!(sqlite_execute_returned_results(
        &returning.execute(()).unwrap_err()
    ));

    let mut rowless = database
        .prepare("INSERT INTO notes VALUES (5, 'prepared query')")
        .unwrap();
    assert!(matches!(
        rowless.query_map((), |_| Ok(())),
        Err(Error::StatementModeMismatch)
    ));
    assert_eq!(row_count(&database, "notes"), 0);
}

#[test]
fn managed_update_can_mix_returning_writes_and_snapshot_reads_atomically() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("returning-update.sqlite");
    let database = MultiliteConnection::open(&path).unwrap();
    database
        .execute("CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)", ())
        .unwrap();
    let pending_before = rusqlite::Connection::open(&path)
        .unwrap()
        .query_row("SELECT count(*) FROM __multilite__pending", (), |row| {
            row.get::<_, i64>(0)
        })
        .unwrap();

    let result = database
        .update(|update| {
            let inserted = update.query(
                "INSERT INTO notes VALUES (1, 'one'), (2, 'two') RETURNING id",
                (),
                |row| row.get::<_, i64>(0),
            )?;
            let visible = update.query("SELECT id FROM notes ORDER BY id", (), |row| {
                row.get::<_, i64>(0)
            })?;
            let updated = update.query(
                "UPDATE notes SET body = upper(body) WHERE id = 2 RETURNING body",
                (),
                |row| row.get::<_, String>(0),
            )?;
            Ok((inserted, visible, updated))
        })
        .unwrap();

    assert_eq!(result, (vec![1, 2], vec![1, 2], vec![String::from("TWO")]));
    assert_eq!(
        database
            .query("SELECT id, body FROM notes ORDER BY id", (), |row| {
                Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
            })
            .unwrap(),
        [(1, "one".into()), (2, "TWO".into())]
    );
    let pending_after = rusqlite::Connection::open(path)
        .unwrap()
        .query_row("SELECT count(*) FROM __multilite__pending", (), |row| {
            row.get::<_, i64>(0)
        })
        .unwrap();
    assert_eq!(pending_after, pending_before + 1);
}

#[test]
fn returning_mapper_failure_rolls_back_the_complete_statement() {
    let directory = tempfile::tempdir().unwrap();
    let database =
        MultiliteConnection::open(directory.path().join("returning-map-error.sqlite")).unwrap();
    database
        .execute("CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)", ())
        .unwrap();

    let mut mapped = 0;
    let result = database.query(
        "INSERT INTO notes VALUES (1, 'one'), (2, 'two'), (3, 'three') RETURNING id",
        (),
        |row| {
            mapped += 1;
            if mapped == 2 {
                Err(rusqlite::Error::InvalidQuery)
            } else {
                row.get::<_, i64>(0)
            }
        },
    );
    assert!(matches!(result, Err(Error::Sqlite(_))));
    assert_eq!(mapped, 2);
    assert_eq!(row_count(&database, "notes"), 0);
}

#[test]
fn caught_mapper_failure_rolls_back_only_its_statement_inside_an_update() {
    let directory = tempfile::tempdir().unwrap();
    let database =
        MultiliteConnection::open(directory.path().join("returning-caught-error.sqlite")).unwrap();
    database
        .execute("CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)", ())
        .unwrap();

    database
        .update(|update| {
            update.execute("INSERT INTO notes VALUES (1, 'before')", ())?;
            let mut mapped = 0;
            let failed = update.query(
                "INSERT INTO notes VALUES (2, 'rolled back'), (3, 'also rolled back')
                 RETURNING id",
                (),
                |row| {
                    mapped += 1;
                    if mapped == 2 {
                        Err(rusqlite::Error::InvalidQuery)
                    } else {
                        row.get::<_, i64>(0)
                    }
                },
            );
            assert!(matches!(failed, Err(Error::Sqlite(_))));
            assert_eq!(mapped, 2);
            update.execute("INSERT INTO notes VALUES (4, 'after')", ())?;
            Ok(())
        })
        .unwrap();

    assert_eq!(
        database
            .query("SELECT id, body FROM notes ORDER BY id", (), |row| {
                Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
            })
            .unwrap(),
        [(1, "before".into()), (4, "after".into())]
    );
}

#[test]
fn mapper_panic_discards_the_complete_managed_update() {
    let directory = tempfile::tempdir().unwrap();
    let database =
        MultiliteConnection::open(directory.path().join("returning-map-panic.sqlite")).unwrap();
    database
        .execute("CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)", ())
        .unwrap();

    let panic = catch_unwind(AssertUnwindSafe(|| {
        let _ = database.update(|update| -> multilite::Result<()> {
            update.execute("INSERT INTO notes VALUES (1, 'before')", ())?;
            update.query(
                "INSERT INTO notes VALUES (2, 'panic') RETURNING id",
                (),
                |_| -> rusqlite::Result<i64> { panic!("injected RETURNING mapper panic") },
            )?;
            Ok(())
        });
    }));
    assert!(panic.is_err());
    assert_eq!(row_count(&database, "notes"), 0);

    database
        .query(
            "INSERT INTO notes VALUES (3, 'usable') RETURNING id",
            (),
            |row| row.get::<_, i64>(0),
        )
        .unwrap();
    assert_eq!(row_count(&database, "notes"), 1);
}

#[test]
fn zero_effect_returning_dml_does_not_advance_local_write_state() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("returning-noop-state.sqlite");
    let database = MultiliteConnection::open(&path).unwrap();
    database
        .execute(
            "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT UNIQUE)",
            (),
        )
        .unwrap();
    database
        .execute("INSERT INTO notes VALUES (1, 'one')", ())
        .unwrap();
    let before = local_write_state(&path);

    assert!(
        database
            .query(
                "UPDATE notes SET body = upper(body) WHERE id = 99 RETURNING id",
                (),
                |row| row.get::<_, i64>(0),
            )
            .unwrap()
            .is_empty()
    );
    assert!(
        database
            .query(
                "DELETE FROM notes WHERE id = 99 RETURNING id",
                (),
                first_i64
            )
            .unwrap()
            .is_empty()
    );
    assert!(
        database
            .query(
                "INSERT INTO notes VALUES (2, 'one')
                 ON CONFLICT(body) DO NOTHING RETURNING id",
                (),
                |row| row.get::<_, i64>(0),
            )
            .unwrap()
            .is_empty()
    );

    assert_eq!(local_write_state(&path), before);
    assert_eq!(row_count(&database, "notes"), 1);
}

#[test]
fn prepared_returning_statement_can_be_reused_after_mapper_failure() {
    let directory = tempfile::tempdir().unwrap();
    let database =
        MultiliteConnection::open(directory.path().join("returning-reuse.sqlite")).unwrap();
    database
        .execute("CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)", ())
        .unwrap();
    let mut statement = database
        .prepare("INSERT INTO notes VALUES (?1, ?2) RETURNING id")
        .unwrap();

    assert_eq!(
        statement
            .query_map((1, "one"), |row| row.get::<_, i64>(0))
            .unwrap(),
        [1]
    );
    assert!(matches!(
        statement.query_map((2, "two"), |_| Err::<i64, _>(rusqlite::Error::InvalidQuery)),
        Err(Error::Sqlite(_))
    ));
    assert_eq!(
        statement
            .query_map((3, "three"), |row| row.get::<_, i64>(0))
            .unwrap(),
        [3]
    );
    assert_eq!(
        database
            .query("SELECT id FROM notes ORDER BY id", (), |row| {
                row.get::<_, i64>(0)
            })
            .unwrap(),
        [1, 3]
    );
}

#[test]
fn returning_uses_schema_changes_made_earlier_in_the_same_update() {
    let directory = tempfile::tempdir().unwrap();
    let database =
        MultiliteConnection::open(directory.path().join("returning-schema.sqlite")).unwrap();

    let observed = database
        .update(|update| {
            update.execute(
                "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT NOT NULL)",
                (),
            )?;
            let inserted = update.query(
                "INSERT INTO notes VALUES (1, 'one') RETURNING id, body",
                (),
                |row| Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?)),
            )?;
            update.execute(
                "ALTER TABLE notes ADD COLUMN revision INTEGER NOT NULL DEFAULT 1",
                (),
            )?;
            let updated = update.query(
                "UPDATE notes SET revision = revision + 1 RETURNING body, revision",
                (),
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?)),
            )?;
            Ok((inserted, updated))
        })
        .unwrap();

    assert_eq!(observed, (vec![(1, "one".into())], vec![("one".into(), 2)]));
    assert_eq!(
        database
            .query("SELECT id, body, revision FROM notes", (), |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                ))
            })
            .unwrap(),
        [(1, "one".into(), 2)]
    );
}

#[test]
fn returning_handles_zero_rows_and_strict_composite_without_rowid_tables() {
    let directory = tempfile::tempdir().unwrap();
    let database =
        MultiliteConnection::open(directory.path().join("returning-strict.sqlite")).unwrap();
    database
        .execute(
            "CREATE TABLE inventory (
                tenant TEXT NOT NULL,
                sku INTEGER NOT NULL,
                payload ANY,
                PRIMARY KEY (tenant, sku)
            ) WITHOUT ROWID, STRICT",
            (),
        )
        .unwrap();

    assert_eq!(
        database
            .query(
                "INSERT INTO inventory VALUES ('north', 7, X'00FF')
                 RETURNING tenant, sku, typeof(payload), hex(payload)",
                (),
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                    ))
                },
            )
            .unwrap(),
        [("north".into(), 7, "blob".into(), "00FF".into())]
    );
    assert!(
        database
            .query(
                "UPDATE inventory SET payload = 9 WHERE tenant = 'missing' RETURNING payload",
                (),
                |row| row.get::<_, i64>(0),
            )
            .unwrap()
            .is_empty()
    );
    assert!(
        database
            .query(
                "DELETE FROM inventory WHERE sku = 99 RETURNING tenant, sku",
                (),
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?)),
            )
            .unwrap()
            .is_empty()
    );
    assert_eq!(row_count(&database, "inventory"), 1);
}

#[test]
fn returning_preserves_sqlite_upsert_replace_and_cascade_results() {
    let directory = tempfile::tempdir().unwrap();
    let database =
        MultiliteConnection::open(directory.path().join("returning-effects.sqlite")).unwrap();
    database
        .execute(
            "CREATE TABLE accounts (
                id INTEGER PRIMARY KEY,
                email TEXT NOT NULL UNIQUE,
                body TEXT NOT NULL
            )",
            (),
        )
        .unwrap();
    database
        .execute(
            "CREATE TABLE children (
                id INTEGER PRIMARY KEY,
                account_id INTEGER NOT NULL,
                FOREIGN KEY (account_id) REFERENCES accounts(id) ON DELETE CASCADE
            )",
            (),
        )
        .unwrap();
    database
        .execute(
            "INSERT INTO accounts VALUES (1, 'a@example.com', 'old')",
            (),
        )
        .unwrap();

    let ignored = database
        .query(
            "INSERT INTO accounts VALUES (2, 'a@example.com', 'ignored')
             ON CONFLICT(email) DO NOTHING RETURNING id",
            (),
            |row| row.get::<_, i64>(0),
        )
        .unwrap();
    assert!(ignored.is_empty());

    let upserted = database
        .query(
            "INSERT INTO accounts VALUES (1, 'a@example.com', 'new')
             ON CONFLICT(id) DO UPDATE SET body = excluded.body
             RETURNING id, body",
            (),
            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?)),
        )
        .unwrap();
    assert_eq!(upserted, [(1, "new".into())]);

    let replaced = database
        .query(
            "INSERT OR REPLACE INTO accounts VALUES (3, 'a@example.com', 'replacement')
             RETURNING id, body",
            (),
            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?)),
        )
        .unwrap();
    assert_eq!(replaced, [(3, "replacement".into())]);
    assert_eq!(row_count(&database, "accounts"), 1);
    database
        .execute("INSERT INTO children VALUES (10, 3), (11, 3)", ())
        .unwrap();
    assert_eq!(row_count(&database, "children"), 2);
    let deleted = database
        .query(
            "DELETE FROM accounts WHERE id = 3 RETURNING id, body",
            (),
            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?)),
        )
        .unwrap();
    assert_eq!(deleted, [(3, "replacement".into())]);
    assert_eq!(row_count(&database, "children"), 0);
}

#[test]
fn async_direct_and_prepared_returning_queries_use_the_write_path() {
    pollster::block_on(async {
        let directory = tempfile::tempdir().unwrap();
        let database =
            MultiliteConnection::open_async(directory.path().join("returning-async.sqlite"))
                .await
                .unwrap();
        database
            .execute_async("CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)", ())
            .await
            .unwrap();

        assert_eq!(
            database
                .query_async(
                    "INSERT INTO notes VALUES (?1, ?2) RETURNING id, body",
                    (Value::Integer(1), Value::Text("one".into())),
                    |row| Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?)),
                )
                .await
                .unwrap(),
            [(1, "one".into())]
        );

        let update = database
            .prepare_async("UPDATE notes SET body = ?1 WHERE id = ?2 RETURNING body")
            .await
            .unwrap();
        assert!(!update.readonly());
        assert_eq!(
            update
                .query_async((Value::Text("ONE".into()), Value::Integer(1)), first_string,)
                .await
                .unwrap(),
            ["ONE"]
        );

        let error = database
            .execute_async("DELETE FROM notes RETURNING id", ())
            .await
            .unwrap_err();
        assert!(sqlite_execute_returned_results(&error));
        assert_eq!(row_count(&database, "notes"), 1);
    });
}

#[test]
fn returning_writes_replicate_as_ordinary_logical_row_changes() {
    let directory = tempfile::tempdir().unwrap();
    let server = server();
    let first = MultiliteConnection::open_with(
        directory.path().join("returning-first.sqlite"),
        OpenOptions::new().server(router(Arc::clone(&server))),
    )
    .unwrap();
    assert!(server.create_space(SpaceId(first.database_id().to_bytes())));
    let second = MultiliteConnection::open_with(
        directory.path().join("returning-second.sqlite"),
        OpenOptions::new()
            .invitation(first.replica_invitation())
            .server(router(Arc::clone(&server))),
    )
    .unwrap();

    first
        .execute("CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)", ())
        .unwrap();
    assert_eq!(first.push().unwrap(), PushOutcome::Drained);
    second.pull().unwrap();
    second.rebase().unwrap();

    assert_eq!(
        first
            .query(
                "INSERT INTO notes VALUES (1, 'one'), (2, 'two') RETURNING id",
                (),
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        [1, 2]
    );
    assert_eq!(first.push().unwrap(), PushOutcome::Drained);
    second.pull().unwrap();
    second.rebase().unwrap();

    assert_eq!(
        second
            .query(
                "UPDATE notes SET body = upper(body) RETURNING id, body",
                (),
                |row| Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?)),
            )
            .unwrap(),
        [(1, "ONE".into()), (2, "TWO".into())]
    );
    assert_eq!(second.push().unwrap(), PushOutcome::Drained);
    first.pull().unwrap();
    second.pull().unwrap();
    first.rebase().unwrap();
    second.rebase().unwrap();

    let expected = [(1, String::from("ONE")), (2, String::from("TWO"))];
    for database in [&first, &second] {
        assert_eq!(
            database
                .query("SELECT id, body FROM notes ORDER BY id", (), |row| {
                    Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
                })
                .unwrap(),
            expected
        );
    }
}
