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
        Err(Error::PreparedWrite)
    ));

    assert!(matches!(
        database.view(|view| {
            view.query(
                "INSERT INTO notes VALUES (3, 'view') RETURNING id",
                (),
                |row| row.get::<_, i64>(0),
            )
        }),
        Err(Error::PreparedWrite)
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
        Err(Error::PreparedWrite)
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
                .query_async((Value::Text("ONE".into()), Value::Integer(1)), |row| row
                    .get::<_, String>(
                    0
                ),)
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
