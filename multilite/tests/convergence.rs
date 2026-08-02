use std::sync::Arc;
use std::time::Duration;

use homebase_client::ServerHandle;
use homebase_core::space::SpaceId;
use multilite::{Error, IsolationLevel, MultiliteConnection, OpenOptions, PushOutcome};

mod common;

use common::{router, server};

fn tables<H>(database: &MultiliteConnection<H>) -> Vec<(String, String)>
where
    H: ServerHandle + Send + Sync + 'static,
{
    let mut statement = database
        .prepare(
            "SELECT name, sql FROM sqlite_schema
             WHERE type = 'table' AND name NOT LIKE '__multilite__%'
             ORDER BY name",
        )
        .unwrap();
    statement
        .query_map((), |row| Ok((row.get(0)?, row.get(1)?)))
        .unwrap()
}

fn rows<H>(database: &MultiliteConnection<H>) -> Vec<(i64, String)>
where
    H: ServerHandle + Send + Sync + 'static,
{
    let mut statement = database
        .prepare("SELECT id, body FROM notes ORDER BY id")
        .unwrap();
    statement
        .query_map((), |row| Ok((row.get(0)?, row.get(1)?)))
        .unwrap()
}

fn membership_rows<H>(database: &MultiliteConnection<H>) -> Vec<(String, i64, String)>
where
    H: ServerHandle + Send + Sync + 'static,
{
    database
        .query(
            "SELECT tenant, member, body
             FROM memberships ORDER BY member, tenant",
            (),
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, String>(2)?,
                ))
            },
        )
        .unwrap()
}

fn index_names<H>(database: &MultiliteConnection<H>) -> Vec<String>
where
    H: ServerHandle + Send + Sync + 'static,
{
    database
        .query(
            "SELECT name FROM sqlite_schema
             WHERE type = 'index' AND name NOT LIKE 'sqlite_autoindex_%'
             ORDER BY name",
            (),
            |row| row.get(0),
        )
        .unwrap()
}

fn user_version<H>(database: &MultiliteConnection<H>) -> i32
where
    H: ServerHandle + Send + Sync + 'static,
{
    database
        .query("PRAGMA user_version", (), |row| row.get(0))
        .unwrap()[0]
}

#[test]
fn concurrent_user_versions_reject_repair_and_converge_at_both_isolation_levels() {
    for isolation in [IsolationLevel::Snapshot, IsolationLevel::Serializable] {
        let directory = tempfile::tempdir().unwrap();
        let authority = server();
        let second_path = directory
            .path()
            .join(format!("pragma-second-{isolation:?}.sqlite"));
        let first = MultiliteConnection::open_with(
            directory
                .path()
                .join(format!("pragma-first-{isolation:?}.sqlite")),
            OpenOptions::new()
                .isolation_level(isolation)
                .server(router(Arc::clone(&authority))),
        )
        .unwrap();
        assert!(authority.create_space(SpaceId(first.database_id().to_bytes())));
        let second = MultiliteConnection::open_with(
            &second_path,
            OpenOptions::new()
                .isolation_level(isolation)
                .invitation(first.replica_invitation())
                .server(router(Arc::clone(&authority))),
        )
        .unwrap();

        first.execute("PRAGMA user_version = 11", ()).unwrap();
        second.execute("PRAGMA user_version = 12", ()).unwrap();
        assert_eq!(first.push().unwrap(), PushOutcome::Drained);
        let PushOutcome::Rejected(rejection) = second.push().unwrap() else {
            panic!("concurrent user_version unexpectedly admitted")
        };
        assert_eq!(user_version(&second), 12);
        drop(second);

        let second = MultiliteConnection::open_with(
            &second_path,
            OpenOptions::new()
                .isolation_level(isolation)
                .invitation(first.replica_invitation())
                .server(router(Arc::clone(&authority))),
        )
        .unwrap();
        assert_eq!(user_version(&second), 12);
        second.rollback(&rejection).unwrap();
        assert_eq!(user_version(&second), 0);
        assert_eq!(second.push().unwrap(), PushOutcome::Drained);
        second.pull().unwrap();
        second.rebase().unwrap();
        assert_eq!(user_version(&first), 11);
        assert_eq!(user_version(&second), 11);
    }
}

#[test]
fn pragma_reads_join_only_serializable_conflict_footprints() {
    for isolation in [IsolationLevel::Snapshot, IsolationLevel::Serializable] {
        let directory = tempfile::tempdir().unwrap();
        let authority = server();
        let first = MultiliteConnection::open_with(
            directory
                .path()
                .join(format!("pragma-read-first-{isolation:?}.sqlite")),
            OpenOptions::new()
                .isolation_level(isolation)
                .server(router(Arc::clone(&authority))),
        )
        .unwrap();
        assert!(authority.create_space(SpaceId(first.database_id().to_bytes())));
        let second = MultiliteConnection::open_with(
            directory
                .path()
                .join(format!("pragma-read-second-{isolation:?}.sqlite")),
            OpenOptions::new()
                .isolation_level(isolation)
                .invitation(first.replica_invitation())
                .server(router(Arc::clone(&authority))),
        )
        .unwrap();
        first
            .execute("CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)", ())
            .unwrap();
        assert_eq!(first.push().unwrap(), PushOutcome::Drained);
        second.pull().unwrap();
        second.rebase().unwrap();

        first
            .update(|transaction| {
                let columns = transaction.query("PRAGMA table_info(notes)", (), |row| {
                    row.get::<_, String>(1)
                })?;
                assert_eq!(columns, ["id", "body"]);
                transaction.execute("INSERT INTO notes VALUES (3, 'three')", ())?;
                Ok(())
            })
            .unwrap();
        second
            .execute("CREATE INDEX notes_body ON notes(body)", ())
            .unwrap();
        assert_eq!(second.push().unwrap(), PushOutcome::Drained);
        match (isolation, first.push().unwrap()) {
            (IsolationLevel::Snapshot, PushOutcome::Drained) => {}
            (IsolationLevel::Serializable, PushOutcome::Rejected(rejection)) => {
                first.rollback(&rejection).unwrap();
                assert_eq!(first.push().unwrap(), PushOutcome::Drained);
            }
            (_, outcome) => panic!("unexpected PRAGMA read disposition: {outcome:?}"),
        }
    }
}

#[test]
fn public_sql_create_and_insert_converge_across_two_replicas() {
    let directory = tempfile::tempdir().unwrap();
    let server = server();
    let first = MultiliteConnection::open_with(
        directory.path().join("first.sqlite"),
        OpenOptions::new().server(router(Arc::clone(&server))),
    )
    .unwrap();
    assert!(server.create_space(SpaceId(first.database_id().to_bytes())));
    let second = MultiliteConnection::open_with(
        directory.path().join("second.sqlite"),
        OpenOptions::new()
            .invitation(first.replica_invitation())
            .server(router(Arc::clone(&server))),
    )
    .unwrap();

    first
        .execute(
            "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT NOT NULL)",
            (),
        )
        .unwrap();
    first
        .execute(
            "CREATE TABLE documents (
                id TEXT NOT NULL PRIMARY KEY,
                body TEXT NOT NULL
            ) WITHOUT ROWID",
            (),
        )
        .unwrap();
    assert_eq!(first.push().unwrap(), PushOutcome::Drained);
    second.pull().unwrap();
    second.rebase().unwrap();

    first
        .execute("INSERT INTO notes VALUES (1, 'first')", ())
        .unwrap();
    let long_key = "long-key".repeat(512);
    first
        .execute("INSERT INTO documents VALUES (?1, 'large')", [&long_key])
        .unwrap();
    second
        .execute("INSERT INTO notes VALUES (2, 'second')", ())
        .unwrap();
    assert_eq!(first.push().unwrap(), PushOutcome::Drained);
    assert_eq!(second.push().unwrap(), PushOutcome::Drained);
    first.pull().unwrap();
    second.pull().unwrap();
    first.rebase().unwrap();
    second.rebase().unwrap();

    first
        .execute("INSERT INTO notes VALUES (7, 'winner')", ())
        .unwrap();
    second
        .execute("INSERT INTO notes VALUES (7, 'loser')", ())
        .unwrap();
    assert_eq!(first.push().unwrap(), PushOutcome::Drained);
    let PushOutcome::Rejected(rejection) = second.push().unwrap() else {
        panic!("same primary key was not rejected")
    };
    second.rollback(&rejection).unwrap();
    assert_eq!(second.push().unwrap(), PushOutcome::Drained);

    first.pull().unwrap();
    second.pull().unwrap();
    first.rebase().unwrap();
    second.rebase().unwrap();

    assert_eq!(tables(&first), tables(&second));
    let expected = vec![
        (1, String::from("first")),
        (2, String::from("second")),
        (7, String::from("winner")),
    ];
    assert_eq!(rows(&first), expected);
    assert_eq!(rows(&second), expected);
    for database in [&first, &second] {
        let mut statement = database
            .prepare("SELECT length(id), body FROM documents")
            .unwrap();
        assert_eq!(
            statement
                .query_map((), |row| Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, String>(1)?
                )))
                .unwrap(),
            [(
                i64::try_from(long_key.len()).unwrap(),
                String::from("large")
            )]
        );
    }
}

#[test]
fn aliased_dml_replays_repairs_and_converges_at_both_isolation_levels() {
    for isolation in [IsolationLevel::Snapshot, IsolationLevel::Serializable] {
        let directory = tempfile::tempdir().unwrap();
        let authority = server();
        let first = MultiliteConnection::open_with(
            directory
                .path()
                .join(format!("alias-first-{isolation:?}.sqlite")),
            OpenOptions::new()
                .isolation_level(isolation)
                .server(router(Arc::clone(&authority))),
        )
        .unwrap();
        assert!(authority.create_space(SpaceId(first.database_id().to_bytes())));
        let second = MultiliteConnection::open_with(
            directory
                .path()
                .join(format!("alias-second-{isolation:?}.sqlite")),
            OpenOptions::new()
                .isolation_level(isolation)
                .invitation(first.replica_invitation())
                .server(router(Arc::clone(&authority))),
        )
        .unwrap();

        first
            .update(|transaction| {
                transaction.execute(
                    "CREATE TABLE notes (
                        id INTEGER PRIMARY KEY,
                        body TEXT NOT NULL
                    )",
                    (),
                )?;
                transaction.execute("INSERT INTO notes VALUES (1, 'one'), (2, 'two')", ())?;
                Ok(())
            })
            .unwrap();
        assert_eq!(first.push().unwrap(), PushOutcome::Drained);
        second.pull().unwrap();
        second.rebase().unwrap();

        first
            .execute(
                "INSERT INTO notes AS target VALUES (1, 'upserted')
                 ON CONFLICT(id) DO UPDATE SET body = excluded.body",
                (),
            )
            .unwrap();
        assert_eq!(first.push().unwrap(), PushOutcome::Drained);
        second.pull().unwrap();
        second.rebase().unwrap();

        second
            .execute(
                "UPDATE notes AS target SET body = 'updated' WHERE target.id = 2",
                (),
            )
            .unwrap();
        assert_eq!(second.push().unwrap(), PushOutcome::Drained);
        first.pull().unwrap();
        first.rebase().unwrap();

        first
            .execute(
                "UPDATE notes AS target SET body = 'winner' WHERE target.id = 1",
                (),
            )
            .unwrap();
        second
            .execute(
                "UPDATE notes AS target SET body = 'loser' WHERE target.id = 1",
                (),
            )
            .unwrap();
        assert_eq!(first.push().unwrap(), PushOutcome::Drained);
        let PushOutcome::Rejected(rejection) = second.push().unwrap() else {
            panic!("aliased same-row update was not rejected")
        };
        second.rollback(&rejection).unwrap();
        assert_eq!(second.push().unwrap(), PushOutcome::Drained);

        first.pull().unwrap();
        second.pull().unwrap();
        first.rebase().unwrap();
        second.rebase().unwrap();
        second
            .execute("DELETE FROM notes AS target WHERE target.id = 2", ())
            .unwrap();
        assert_eq!(second.push().unwrap(), PushOutcome::Drained);
        first.pull().unwrap();
        second.pull().unwrap();
        first.rebase().unwrap();
        second.rebase().unwrap();

        let expected = vec![(1, String::from("winner"))];
        assert_eq!(rows(&first), expected);
        assert_eq!(rows(&second), expected);
    }
}

#[test]
fn idempotent_ddl_lowers_missing_objects_and_repeats_as_convergent_noops() {
    let directory = tempfile::tempdir().unwrap();
    let server = server();
    let first = MultiliteConnection::open_with(
        directory.path().join("idempotent-first.sqlite"),
        OpenOptions::new().server(router(Arc::clone(&server))),
    )
    .unwrap();
    assert!(server.create_space(SpaceId(first.database_id().to_bytes())));
    let second = MultiliteConnection::open_with(
        directory.path().join("idempotent-second.sqlite"),
        OpenOptions::new()
            .invitation(first.replica_invitation())
            .server(router(Arc::clone(&server))),
    )
    .unwrap();

    first
        .execute(
            "CREATE TABLE IF NOT EXISTS notes (id INTEGER PRIMARY KEY, body TEXT)",
            (),
        )
        .unwrap();
    first
        .execute("CREATE INDEX IF NOT EXISTS notes_body ON notes(body)", ())
        .unwrap();
    assert_eq!(first.push().unwrap(), PushOutcome::Drained);
    second.pull().unwrap();
    second.rebase().unwrap();

    second
        .execute(
            "CREATE TABLE IF NOT EXISTS NOTES (id INTEGER PRIMARY KEY, other BLOB)",
            (),
        )
        .unwrap();
    second
        .execute(
            "CREATE INDEX IF NOT EXISTS NOTES_BODY ON notes(missing)",
            (),
        )
        .unwrap();
    assert_eq!(second.push().unwrap(), PushOutcome::Drained);

    first
        .execute("DROP INDEX IF EXISTS notes_body", ())
        .unwrap();
    assert_eq!(first.push().unwrap(), PushOutcome::Drained);
    second.pull().unwrap();
    second.rebase().unwrap();
    second
        .execute("DROP INDEX IF EXISTS NOTES_BODY", ())
        .unwrap();
    assert_eq!(second.push().unwrap(), PushOutcome::Drained);

    assert_eq!(tables(&first), tables(&second));
    assert!(index_names(&first).is_empty());
    assert_eq!(index_names(&first), index_names(&second));
}

#[test]
fn concurrent_conditional_table_drops_repair_and_retry_as_a_noop() {
    for isolation in [IsolationLevel::Snapshot, IsolationLevel::Serializable] {
        let directory = tempfile::tempdir().unwrap();
        let authority = server();
        let second_path = directory
            .path()
            .join(format!("conditional-drop-second-{isolation:?}.sqlite"));
        let first = MultiliteConnection::open_with(
            directory
                .path()
                .join(format!("conditional-drop-first-{isolation:?}.sqlite")),
            OpenOptions::new()
                .isolation_level(isolation)
                .server(router(Arc::clone(&authority))),
        )
        .unwrap();
        assert!(authority.create_space(SpaceId(first.database_id().to_bytes())));
        let second = MultiliteConnection::open_with(
            &second_path,
            OpenOptions::new()
                .isolation_level(isolation)
                .invitation(first.replica_invitation())
                .server(router(Arc::clone(&authority))),
        )
        .unwrap();

        first
            .update(|transaction| {
                transaction.execute(
                    "CREATE TABLE disposable (
                        tenant TEXT,
                        id INTEGER,
                        body TEXT NOT NULL UNIQUE,
                        PRIMARY KEY (tenant, id)
                    ) WITHOUT ROWID",
                    (),
                )?;
                transaction.execute(
                    "CREATE INDEX disposable_body ON disposable(lower(body))",
                    (),
                )?;
                transaction.execute("INSERT INTO disposable VALUES ('north', 1, 'one')", ())?;
                Ok(())
            })
            .unwrap();
        assert_eq!(first.push().unwrap(), PushOutcome::Drained);
        second.pull().unwrap();
        second.rebase().unwrap();

        first
            .execute("DROP TABLE IF EXISTS disposable", ())
            .unwrap();
        second
            .execute("DROP TABLE IF EXISTS DISPOSABLE", ())
            .unwrap();
        assert_eq!(first.push().unwrap(), PushOutcome::Drained);
        let PushOutcome::Rejected(rejection) = second.push().unwrap() else {
            panic!("concurrent conditional table drops both admitted")
        };
        drop(second);

        let second = MultiliteConnection::open_with(
            &second_path,
            OpenOptions::new()
                .isolation_level(isolation)
                .server(router(Arc::clone(&authority))),
        )
        .unwrap();
        second.rollback(&rejection).unwrap();
        assert_eq!(second.push().unwrap(), PushOutcome::Drained);
        second.pull().unwrap();
        second.rebase().unwrap();

        second
            .execute("DROP TABLE IF EXISTS disposable", ())
            .unwrap();
        assert_eq!(second.push().unwrap(), PushOutcome::Drained);
        first.pull().unwrap();
        second.pull().unwrap();
        first.rebase().unwrap();
        second.rebase().unwrap();
        for database in [&first, &second] {
            assert!(
                database
                    .query("SELECT * FROM disposable", (), |_| Ok(()))
                    .is_err()
            );
        }
    }
}

#[test]
fn ignored_conflicts_compile_only_the_rows_sqlite_changed_and_converge() {
    for isolation in [IsolationLevel::Snapshot, IsolationLevel::Serializable] {
        let directory = tempfile::tempdir().unwrap();
        let server = server();
        let first = MultiliteConnection::open_with(
            directory.path().join("ignore-first.sqlite"),
            OpenOptions::new()
                .isolation_level(isolation)
                .server(router(Arc::clone(&server))),
        )
        .unwrap();
        assert!(server.create_space(SpaceId(first.database_id().to_bytes())));
        let second = MultiliteConnection::open_with(
            directory.path().join("ignore-second.sqlite"),
            OpenOptions::new()
                .isolation_level(isolation)
                .invitation(first.replica_invitation())
                .server(router(Arc::clone(&server))),
        )
        .unwrap();

        first
            .execute(
                "CREATE TABLE accounts (
                    id INTEGER PRIMARY KEY,
                    email TEXT NOT NULL UNIQUE
                )",
                (),
            )
            .unwrap();
        first
            .execute("INSERT INTO accounts VALUES (1, 'shared')", ())
            .unwrap();
        assert_eq!(first.push().unwrap(), PushOutcome::Drained);
        second.pull().unwrap();
        second.rebase().unwrap();

        assert_eq!(
            first
                .execute(
                    "INSERT OR IGNORE INTO accounts VALUES
                        (2, 'shared'), (3, 'first'),
                        (6, 'third'), (7, 'fourth')",
                    (),
                )
                .unwrap(),
            3
        );
        assert_eq!(
            first
                .execute(
                    "UPDATE OR IGNORE accounts
                     SET email = CASE id
                         WHEN 6 THEN 'shared'
                         ELSE email || '-updated'
                     END
                     WHERE id IN (6, 7)",
                    (),
                )
                .unwrap(),
            1
        );
        assert_eq!(first.push().unwrap(), PushOutcome::Drained);
        second.pull().unwrap();
        second.rebase().unwrap();

        assert_eq!(
            second
                .execute(
                    "INSERT INTO accounts VALUES
                        (4, 'shared'), (5, 'second')
                     ON CONFLICT(email) DO NOTHING
                     ON CONFLICT DO NOTHING",
                    (),
                )
                .unwrap(),
            1
        );
        assert_eq!(second.push().unwrap(), PushOutcome::Drained);
        first.pull().unwrap();
        second.pull().unwrap();
        first.rebase().unwrap();
        second.rebase().unwrap();

        for database in [&first, &second] {
            assert_eq!(
                database
                    .query("SELECT id, email FROM accounts ORDER BY id", (), |row| Ok(
                        (row.get::<_, i64>(0)?, row.get::<_, String>(1)?)
                    ),)
                    .unwrap(),
                [
                    (1, "shared".into()),
                    (3, "first".into()),
                    (5, "second".into()),
                    (6, "third".into()),
                    (7, "fourth-updated".into()),
                ]
            );
        }
    }
}

#[test]
fn upsert_do_update_rejection_reopens_repairs_exact_net_effects_and_converges() {
    for isolation in [IsolationLevel::Snapshot, IsolationLevel::Serializable] {
        let directory = tempfile::tempdir().unwrap();
        let server = server();
        let first_path = directory.path().join("upsert-first.sqlite");
        let second_path = directory.path().join("upsert-second.sqlite");
        let first = MultiliteConnection::open_with(
            &first_path,
            OpenOptions::new()
                .isolation_level(isolation)
                .server(router(Arc::clone(&server))),
        )
        .unwrap();
        assert!(server.create_space(SpaceId(first.database_id().to_bytes())));
        let invitation = first.replica_invitation();
        let second_options = || {
            OpenOptions::new()
                .isolation_level(isolation)
                .server(router(Arc::clone(&server)))
        };
        let second =
            MultiliteConnection::open_with(&second_path, second_options().invitation(invitation))
                .unwrap();

        first
            .update(|transaction| {
                transaction.execute(
                    "CREATE TABLE accounts (
                        id INTEGER PRIMARY KEY,
                        email TEXT NOT NULL UNIQUE,
                        body TEXT NOT NULL,
                        revision INTEGER NOT NULL
                    )",
                    (),
                )?;
                transaction.execute(
                    "INSERT INTO accounts VALUES
                        (1, 'shared', 'base', 0),
                        (5, 'five', 'five', 0)",
                    (),
                )?;
                Ok(())
            })
            .unwrap();
        assert_eq!(first.push().unwrap(), PushOutcome::Drained);
        second.pull().unwrap();
        second.rebase().unwrap();

        first
            .execute(
                "UPDATE accounts
                 SET body = 'winner', revision = revision + 1
                 WHERE id = 1",
                (),
            )
            .unwrap();
        assert_eq!(
            second
                .execute(
                    "INSERT INTO accounts VALUES
                        (2, 'new', 'speculative-new', 0),
                        (3, 'new', 'speculative-newer', 7),
                        (9, 'shared', 'speculative-shared', 8),
                        (10, 'shared', 'where-false', 9)
                     ON CONFLICT(email) DO UPDATE SET
                        body = excluded.body,
                        revision = accounts.revision + 1
                     WHERE excluded.id <> 10",
                    (),
                )
                .unwrap(),
            3
        );
        assert_eq!(first.push().unwrap(), PushOutcome::Drained);
        let PushOutcome::Rejected(rejection) = second.push().unwrap() else {
            panic!("conflicting UPSERT DO UPDATE was admitted under {isolation:?}")
        };

        drop(second);
        let second = MultiliteConnection::open_with(&second_path, second_options()).unwrap();
        assert_eq!(
            second
                .query(
                    "SELECT id, email, body, revision FROM accounts ORDER BY id",
                    (),
                    |row| {
                        Ok((
                            row.get::<_, i64>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, String>(2)?,
                            row.get::<_, i64>(3)?,
                        ))
                    },
                )
                .unwrap(),
            [
                (1, "shared".into(), "speculative-shared".into(), 1),
                (2, "new".into(), "speculative-newer".into(), 1),
                (5, "five".into(), "five".into(), 0),
            ]
        );

        second.rollback(&rejection).unwrap();
        assert_eq!(
            second
                .query(
                    "SELECT id, email, body, revision FROM accounts ORDER BY id",
                    (),
                    |row| {
                        Ok((
                            row.get::<_, i64>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, String>(2)?,
                            row.get::<_, i64>(3)?,
                        ))
                    },
                )
                .unwrap(),
            [
                (1, "shared".into(), "base".into(), 0),
                (5, "five".into(), "five".into(), 0),
            ]
        );
        assert_eq!(second.push().unwrap(), PushOutcome::Drained);
        first.pull().unwrap();
        second.pull().unwrap();
        first.rebase().unwrap();
        second.rebase().unwrap();

        let expected = [
            (1, String::from("shared"), String::from("winner"), 1),
            (5, String::from("five"), String::from("five"), 0),
        ];
        for database in [&first, &second] {
            assert_eq!(
                database
                    .query(
                        "SELECT id, email, body, revision FROM accounts ORDER BY id",
                        (),
                        |row| {
                            Ok((
                                row.get::<_, i64>(0)?,
                                row.get::<_, String>(1)?,
                                row.get::<_, String>(2)?,
                                row.get::<_, i64>(3)?,
                            ))
                        },
                    )
                    .unwrap(),
                expected
            );
        }

        drop(first);
        drop(second);
        for path in [&first_path, &second_path] {
            let connection = rusqlite::Connection::open(path).unwrap();
            assert_eq!(
                connection
                    .query_row("SELECT count(*) FROM __multilite__pending", (), |row| {
                        row.get::<_, i64>(0)
                    })
                    .unwrap(),
                0
            );
            assert_eq!(
                connection
                    .query_row("PRAGMA integrity_check", (), |row| row.get::<_, String>(0))
                    .unwrap(),
                "ok"
            );
        }
    }
}

#[test]
fn replacement_rejection_reopens_restores_every_victim_and_converges() {
    for isolation in [IsolationLevel::Snapshot, IsolationLevel::Serializable] {
        let directory = tempfile::tempdir().unwrap();
        let server = server();
        let first_path = directory.path().join("replace-first.sqlite");
        let second_path = directory.path().join("replace-second.sqlite");
        let first = MultiliteConnection::open_with(
            &first_path,
            OpenOptions::new()
                .isolation_level(isolation)
                .server(router(Arc::clone(&server))),
        )
        .unwrap();
        assert!(server.create_space(SpaceId(first.database_id().to_bytes())));
        let invitation = first.replica_invitation();
        let second_options = || {
            OpenOptions::new()
                .isolation_level(isolation)
                .server(router(Arc::clone(&server)))
        };
        let second =
            MultiliteConnection::open_with(&second_path, second_options().invitation(invitation))
                .unwrap();

        first
            .update(|transaction| {
                transaction.execute(
                    "CREATE TABLE profiles (
                        id INTEGER PRIMARY KEY,
                        email TEXT NOT NULL UNIQUE,
                        handle TEXT NOT NULL UNIQUE,
                        body TEXT NOT NULL
                    )",
                    (),
                )?;
                transaction.execute(
                    "INSERT INTO profiles VALUES
                        (1, 'one@example.com', 'one', 'first'),
                        (2, 'two@example.com', 'two', 'second'),
                        (5, 'five@example.com', 'five', 'untouched')",
                    (),
                )?;
                Ok(())
            })
            .unwrap();
        assert_eq!(first.push().unwrap(), PushOutcome::Drained);
        second.pull().unwrap();
        second.rebase().unwrap();

        first
            .execute("UPDATE profiles SET body = 'winner' WHERE id = 1", ())
            .unwrap();
        second
            .execute(
                "REPLACE INTO profiles VALUES
                    (3, 'one@example.com', 'two', 'speculative')",
                (),
            )
            .unwrap();
        assert_eq!(first.push().unwrap(), PushOutcome::Drained);
        let PushOutcome::Rejected(rejection) = second.push().unwrap() else {
            panic!("conflicting replacement was admitted under {isolation:?}")
        };

        drop(second);
        let second = MultiliteConnection::open_with(&second_path, second_options()).unwrap();
        let profile_rows = |database: &MultiliteConnection<_>| {
            database
                .query(
                    "SELECT id, email, handle, body FROM profiles ORDER BY id",
                    (),
                    |row| {
                        Ok((
                            row.get::<_, i64>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, String>(2)?,
                            row.get::<_, String>(3)?,
                        ))
                    },
                )
                .unwrap()
        };
        assert_eq!(
            profile_rows(&second),
            [
                (
                    3,
                    "one@example.com".into(),
                    "two".into(),
                    "speculative".into(),
                ),
                (
                    5,
                    "five@example.com".into(),
                    "five".into(),
                    "untouched".into(),
                ),
            ]
        );

        second.rollback(&rejection).unwrap();
        assert_eq!(
            profile_rows(&second),
            [
                (1, "one@example.com".into(), "one".into(), "first".into(),),
                (2, "two@example.com".into(), "two".into(), "second".into(),),
                (
                    5,
                    "five@example.com".into(),
                    "five".into(),
                    "untouched".into(),
                ),
            ]
        );
        assert_eq!(second.push().unwrap(), PushOutcome::Drained);
        first.pull().unwrap();
        second.pull().unwrap();
        first.rebase().unwrap();
        second.rebase().unwrap();

        let expected = [
            (
                1,
                String::from("one@example.com"),
                String::from("one"),
                String::from("winner"),
            ),
            (
                2,
                String::from("two@example.com"),
                String::from("two"),
                String::from("second"),
            ),
            (
                5,
                String::from("five@example.com"),
                String::from("five"),
                String::from("untouched"),
            ),
        ];
        assert_eq!(profile_rows(&first), expected);
        assert_eq!(profile_rows(&second), expected);
    }
}

#[test]
fn conflict_mode_rejection_repair_survives_reopen_and_restores_only_net_changes() {
    for isolation in [IsolationLevel::Snapshot, IsolationLevel::Serializable] {
        let directory = tempfile::tempdir().unwrap();
        let server = server();
        let first_path = directory.path().join("repair-first.sqlite");
        let second_path = directory.path().join("repair-second.sqlite");
        let first = MultiliteConnection::open_with(
            &first_path,
            OpenOptions::new()
                .isolation_level(isolation)
                .server(router(Arc::clone(&server))),
        )
        .unwrap();
        assert!(server.create_space(SpaceId(first.database_id().to_bytes())));
        let invitation = first.replica_invitation();
        let second_options = || {
            OpenOptions::new()
                .isolation_level(isolation)
                .server(router(Arc::clone(&server)))
        };
        let second =
            MultiliteConnection::open_with(&second_path, second_options().invitation(invitation))
                .unwrap();

        first
            .update(|transaction| {
                transaction.execute(
                    "CREATE TABLE accounts (
                        id INTEGER PRIMARY KEY,
                        email TEXT NOT NULL UNIQUE
                    )",
                    (),
                )?;
                transaction.execute(
                    "INSERT INTO accounts VALUES
                        (1, 'shared'), (5, 'five'), (6, 'six')",
                    (),
                )?;
                Ok(())
            })
            .unwrap();
        assert_eq!(first.push().unwrap(), PushOutcome::Drained);
        second.pull().unwrap();
        second.rebase().unwrap();

        first
            .execute("INSERT INTO accounts VALUES (3, 'winner')", ())
            .unwrap();
        assert_eq!(
            second
                .execute(
                    "INSERT OR IGNORE INTO accounts VALUES
                        (1, 'ignored'), (3, 'loser'), (4, 'also speculative')",
                    (),
                )
                .unwrap(),
            2
        );
        assert_eq!(first.push().unwrap(), PushOutcome::Drained);
        let PushOutcome::Rejected(rejection) = second.push().unwrap() else {
            panic!("conflicting INSERT OR IGNORE was admitted under {isolation:?}")
        };

        drop(second);
        let second = MultiliteConnection::open_with(&second_path, second_options()).unwrap();
        assert_eq!(
            second
                .query("SELECT id, email FROM accounts ORDER BY id", (), |row| Ok(
                    (row.get::<_, i64>(0)?, row.get::<_, String>(1)?)
                ),)
                .unwrap(),
            [
                (1, "shared".into()),
                (3, "loser".into()),
                (4, "also speculative".into()),
                (5, "five".into()),
                (6, "six".into()),
            ]
        );
        second.rollback(&rejection).unwrap();
        assert_eq!(second.push().unwrap(), PushOutcome::Drained);
        second.pull().unwrap();
        second.rebase().unwrap();
        assert_eq!(
            second
                .query("SELECT id, email FROM accounts ORDER BY id", (), |row| Ok(
                    (row.get::<_, i64>(0)?, row.get::<_, String>(1)?)
                ),)
                .unwrap(),
            [
                (1, "shared".into()),
                (3, "winner".into()),
                (5, "five".into()),
                (6, "six".into()),
            ]
        );

        first
            .execute("UPDATE accounts SET email = 'winner-six' WHERE id = 6", ())
            .unwrap();
        assert_eq!(
            second
                .execute(
                    "UPDATE OR IGNORE accounts
                     SET email = CASE id WHEN 5 THEN 'shared' ELSE 'loser-six' END
                     WHERE id IN (5, 6)",
                    (),
                )
                .unwrap(),
            1
        );
        assert_eq!(first.push().unwrap(), PushOutcome::Drained);
        let PushOutcome::Rejected(rejection) = second.push().unwrap() else {
            panic!("conflicting UPDATE OR IGNORE was admitted under {isolation:?}")
        };
        second.rollback(&rejection).unwrap();
        assert_eq!(second.push().unwrap(), PushOutcome::Drained);
        first.pull().unwrap();
        second.pull().unwrap();
        first.rebase().unwrap();
        second.rebase().unwrap();

        let expected = [
            (1, String::from("shared")),
            (3, String::from("winner")),
            (5, String::from("five")),
            (6, String::from("winner-six")),
        ];
        for database in [&first, &second] {
            assert_eq!(
                database
                    .query("SELECT id, email FROM accounts ORDER BY id", (), |row| Ok(
                        (row.get::<_, i64>(0)?, row.get::<_, String>(1)?)
                    ),)
                    .unwrap(),
                expected
            );
        }

        drop(first);
        drop(second);
        for path in [&first_path, &second_path] {
            let connection = rusqlite::Connection::open(path).unwrap();
            assert_eq!(
                connection
                    .query_row("SELECT count(*) FROM __multilite__pending", (), |row| {
                        row.get::<_, i64>(0)
                    })
                    .unwrap(),
                0
            );
        }
    }
}

#[test]
fn richer_captured_dml_repairs_conflicts_and_converges() {
    for isolation in [IsolationLevel::Snapshot, IsolationLevel::Serializable] {
        let directory = tempfile::tempdir().unwrap();
        let server = server();
        let first = MultiliteConnection::open_with(
            directory.path().join("richer-first.sqlite"),
            OpenOptions::new()
                .isolation_level(isolation)
                .server(router(Arc::clone(&server))),
        )
        .unwrap();
        assert!(server.create_space(SpaceId(first.database_id().to_bytes())));
        let second = MultiliteConnection::open_with(
            directory.path().join("richer-second.sqlite"),
            OpenOptions::new()
                .isolation_level(isolation)
                .invitation(first.replica_invitation())
                .server(router(Arc::clone(&server))),
        )
        .unwrap();

        first
            .update(|transaction| {
                transaction.execute(
                    "CREATE TABLE notes (
                        id INTEGER PRIMARY KEY,
                        body TEXT NOT NULL,
                        score INTEGER NOT NULL
                    )",
                    (),
                )?;
                transaction.execute("CREATE INDEX notes_by_body ON notes (body)", ())?;
                transaction.execute(
                    "CREATE TABLE replacements (
                        id INTEGER PRIMARY KEY,
                        body TEXT NOT NULL,
                        score INTEGER NOT NULL
                    )",
                    (),
                )?;
                transaction.execute(
                    "INSERT INTO notes VALUES (1, 'one', 10), (2, 'two', 20)",
                    (),
                )?;
                transaction.execute(
                    "INSERT INTO replacements VALUES (1, 'winner', 11), (2, 'gone', 22)",
                    (),
                )?;
                Ok(())
            })
            .unwrap();
        assert_eq!(first.push().unwrap(), PushOutcome::Drained);
        second.pull().unwrap();
        second.rebase().unwrap();

        first
            .execute(
                "WITH selected AS (
                    SELECT id, body, score FROM replacements WHERE id = 1
                 )
                 UPDATE notes INDEXED BY notes_by_body
                 SET (body, score) = (selected.body, selected.score)
                 FROM selected WHERE selected.id = notes.id",
                (),
            )
            .unwrap();
        second
            .execute(
                "WITH doomed AS (SELECT 1 AS id)
                 DELETE FROM notes NOT INDEXED
                 WHERE id IN (SELECT id FROM doomed)",
                (),
            )
            .unwrap();
        assert_eq!(first.push().unwrap(), PushOutcome::Drained);
        let PushOutcome::Rejected(rejection) = second.push().unwrap() else {
            panic!("same-row rich DML did not conflict under {isolation:?}")
        };
        second.rollback(&rejection).unwrap();
        assert_eq!(second.push().unwrap(), PushOutcome::Drained);
        first.pull().unwrap();
        second.pull().unwrap();
        first.rebase().unwrap();
        second.rebase().unwrap();

        second
            .execute(
                "WITH doomed AS (SELECT id FROM replacements WHERE id = 2)
                 DELETE FROM notes WHERE id IN (SELECT id FROM doomed)",
                (),
            )
            .unwrap();
        assert_eq!(second.push().unwrap(), PushOutcome::Drained);
        first.pull().unwrap();
        first.rebase().unwrap();

        let expected = [(1, String::from("winner"), 11)];
        for database in [&first, &second] {
            assert_eq!(
                database
                    .query("SELECT id, body, score FROM notes ORDER BY id", (), |row| {
                        Ok((
                            row.get::<_, i64>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, i64>(2)?,
                        ))
                    },)
                    .unwrap(),
                expected
            );
        }
    }
}

#[test]
fn captured_defaults_and_checks_converge_without_remote_reevaluation() {
    let directory = tempfile::tempdir().unwrap();
    let server = server();
    let first = MultiliteConnection::open_with(
        directory.path().join("defaults-first.sqlite"),
        OpenOptions::new().server(router(Arc::clone(&server))),
    )
    .unwrap();
    assert!(server.create_space(SpaceId(first.database_id().to_bytes())));
    let second = MultiliteConnection::open_with(
        directory.path().join("defaults-second.sqlite"),
        OpenOptions::new()
            .invitation(first.replica_invitation())
            .server(router(Arc::clone(&server))),
    )
    .unwrap();

    first
        .update(|transaction| {
            transaction.execute(
                "CREATE TABLE events (
                    id INTEGER CONSTRAINT event_pk PRIMARY KEY,
                    state TEXT CONSTRAINT state_required NOT NULL
                        CONSTRAINT state_default DEFAULT ('new')
                        CONSTRAINT state_nonempty CHECK (length(state) > 0),
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    score INTEGER DEFAULT 0,
                    CONSTRAINT score_nonnegative CHECK (score >= 0)
                )",
                (),
            )?;
            transaction.execute("INSERT INTO events (id) VALUES (1)", ())?;
            Ok(())
        })
        .unwrap();
    let origin_created_at = first
        .query("SELECT created_at FROM events WHERE id = 1", (), |row| {
            row.get::<_, String>(0)
        })
        .unwrap()
        .remove(0);

    std::thread::sleep(Duration::from_millis(1_100));
    assert_eq!(first.push().unwrap(), PushOutcome::Drained);
    second.pull().unwrap();
    second.rebase().unwrap();
    assert_eq!(
        second
            .query("SELECT created_at FROM events WHERE id = 1", (), |row| {
                row.get::<_, String>(0)
            },)
            .unwrap(),
        [origin_created_at]
    );

    assert!(matches!(
        first.execute("INSERT INTO events (id, score) VALUES (9, -1)", ()),
        Err(multilite::Error::Sqlite(_))
    ));
    first
        .execute("INSERT INTO events (id, score) VALUES (2, 2)", ())
        .unwrap();
    second
        .execute("INSERT INTO events (id, state) VALUES (3, 'ready')", ())
        .unwrap();
    assert_eq!(first.push().unwrap(), PushOutcome::Drained);
    assert_eq!(second.push().unwrap(), PushOutcome::Drained);
    first.pull().unwrap();
    second.pull().unwrap();
    first.rebase().unwrap();
    second.rebase().unwrap();

    for database in [&first, &second] {
        assert_eq!(
            database
                .query(
                    "SELECT id, state, score FROM events ORDER BY id",
                    (),
                    |row| {
                        Ok((
                            row.get::<_, i64>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, i64>(2)?,
                        ))
                    },
                )
                .unwrap(),
            [
                (1, "new".into(), 0),
                (2, "new".into(), 2),
                (3, "ready".into(), 0),
            ]
        );
        assert_eq!(
            database
                .query("SELECT count(*) FROM events WHERE id = 9", (), |row| {
                    row.get::<_, i64>(0)
                })
                .unwrap(),
            [0]
        );
    }
}

#[test]
fn parent_delete_and_child_insert_conflict_in_either_admission_order() {
    let directory = tempfile::tempdir().unwrap();
    let server = server();
    let first = MultiliteConnection::open_with(
        directory.path().join("foreign-key-first.sqlite"),
        OpenOptions::new().server(router(Arc::clone(&server))),
    )
    .unwrap();
    assert!(server.create_space(SpaceId(first.database_id().to_bytes())));
    let second = MultiliteConnection::open_with(
        directory.path().join("foreign-key-second.sqlite"),
        OpenOptions::new()
            .invitation(first.replica_invitation())
            .server(router(Arc::clone(&server))),
    )
    .unwrap();

    first
        .execute(
            "CREATE TABLE parents (id INTEGER PRIMARY KEY, body TEXT)",
            (),
        )
        .unwrap();
    first
        .execute(
            "CREATE TABLE children (
                id INTEGER PRIMARY KEY,
                parent INTEGER REFERENCES parents(id),
                body TEXT
            )",
            (),
        )
        .unwrap();
    first
        .execute("INSERT INTO parents VALUES (1, 'first'), (2, 'second')", ())
        .unwrap();
    assert_eq!(first.push().unwrap(), PushOutcome::Drained);
    second.pull().unwrap();
    second.rebase().unwrap();

    first
        .execute("DELETE FROM parents WHERE id = 1", ())
        .unwrap();
    second
        .execute("INSERT INTO children VALUES (10, 1, 'loser')", ())
        .unwrap();
    assert_eq!(first.push().unwrap(), PushOutcome::Drained);
    let PushOutcome::Rejected(rejection) = second.push().unwrap() else {
        panic!("child insert did not conflict with admitted parent deletion")
    };
    second.rollback(&rejection).unwrap();
    assert_eq!(second.push().unwrap(), PushOutcome::Drained);
    first.pull().unwrap();
    second.pull().unwrap();
    first.rebase().unwrap();
    second.rebase().unwrap();

    second
        .execute("INSERT INTO children VALUES (20, 2, 'winner')", ())
        .unwrap();
    first
        .execute("DELETE FROM parents WHERE id = 2", ())
        .unwrap();
    assert_eq!(second.push().unwrap(), PushOutcome::Drained);
    let PushOutcome::Rejected(rejection) = first.push().unwrap() else {
        panic!("parent deletion did not conflict with admitted child insert")
    };
    first.rollback(&rejection).unwrap();
    assert_eq!(first.push().unwrap(), PushOutcome::Drained);
    first.pull().unwrap();
    second.pull().unwrap();
    first.rebase().unwrap();
    second.rebase().unwrap();

    for database in [&first, &second] {
        assert_eq!(
            database
                .query("SELECT id FROM parents ORDER BY id", (), |row| {
                    row.get::<_, i64>(0)
                })
                .unwrap(),
            [2]
        );
        assert_eq!(
            database
                .query("SELECT id, parent FROM children ORDER BY id", (), |row| Ok(
                    (row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)
                ),)
                .unwrap(),
            [(20, 2)]
        );
    }

    first
        .execute("DELETE FROM children WHERE id = 20", ())
        .unwrap();
    first
        .execute("DELETE FROM parents WHERE id = 2", ())
        .unwrap();
    assert_eq!(first.push().unwrap(), PushOutcome::Drained);
    first.pull().unwrap();
    second.pull().unwrap();
    first.rebase().unwrap();
    second.rebase().unwrap();
    for database in [&first, &second] {
        assert_eq!(
            database
                .query("SELECT count(*) FROM parents", (), |row| {
                    row.get::<_, i64>(0)
                })
                .unwrap(),
            [0]
        );
        assert_eq!(
            database
                .query("SELECT count(*) FROM children", (), |row| {
                    row.get::<_, i64>(0)
                })
                .unwrap(),
            [0]
        );
    }
}

#[test]
fn multi_row_parent_key_shift_with_a_reused_value_converges() {
    let directory = tempfile::tempdir().unwrap();
    let server = server();
    let first = MultiliteConnection::open_with(
        directory.path().join("parent-shift-first.sqlite"),
        OpenOptions::new().server(router(Arc::clone(&server))),
    )
    .unwrap();
    assert!(server.create_space(SpaceId(first.database_id().to_bytes())));
    let second = MultiliteConnection::open_with(
        directory.path().join("parent-shift-second.sqlite"),
        OpenOptions::new()
            .invitation(first.replica_invitation())
            .server(router(Arc::clone(&server))),
    )
    .unwrap();

    first
        .update(|transaction| {
            transaction.execute("CREATE TABLE parents (id INTEGER PRIMARY KEY)", ())?;
            transaction.execute(
                "CREATE TABLE children (
                    id INTEGER PRIMARY KEY,
                    parent INTEGER REFERENCES parents(id) ON UPDATE CASCADE
                )",
                (),
            )?;
            transaction.execute("INSERT INTO parents VALUES (2), (3)", ())?;
            transaction.execute("INSERT INTO children VALUES (20, 2), (30, 3)", ())?;
            Ok(())
        })
        .unwrap();
    assert_eq!(first.push().unwrap(), PushOutcome::Drained);
    second.pull().unwrap();
    second.rebase().unwrap();

    first
        .execute("UPDATE parents SET id = id - 1 WHERE id IN (2, 3)", ())
        .unwrap();
    assert_eq!(first.push().unwrap(), PushOutcome::Drained);
    first.pull().unwrap();
    second.pull().unwrap();
    first.rebase().unwrap();
    second.rebase().unwrap();

    for database in [&first, &second] {
        assert_eq!(
            database
                .query("SELECT id FROM parents ORDER BY id", (), |row| {
                    row.get::<_, i64>(0)
                })
                .unwrap(),
            [1, 2]
        );
        assert_eq!(
            database
                .query("SELECT id, parent FROM children ORDER BY id", (), |row| {
                    Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?))
                })
                .unwrap(),
            [(20, 1), (30, 2)]
        );
    }
}

#[test]
fn rejected_cascade_restores_every_table_then_converges_when_retried() {
    for isolation in [IsolationLevel::Snapshot, IsolationLevel::Serializable] {
        let directory = tempfile::tempdir().unwrap();
        let server = server();
        let first_path = directory.path().join("cascade-first.sqlite");
        let first_options = || {
            OpenOptions::new()
                .isolation_level(isolation)
                .server(router(Arc::clone(&server)))
        };
        let first = MultiliteConnection::open_with(&first_path, first_options()).unwrap();
        assert!(server.create_space(SpaceId(first.database_id().to_bytes())));
        let second = MultiliteConnection::open_with(
            directory.path().join("cascade-second.sqlite"),
            OpenOptions::new()
                .isolation_level(isolation)
                .invitation(first.replica_invitation())
                .server(router(Arc::clone(&server))),
        )
        .unwrap();

        first
            .update(|transaction| {
                transaction.execute("CREATE TABLE parents (id INTEGER PRIMARY KEY)", ())?;
                transaction.execute(
                    "CREATE TABLE children (
                        id INTEGER PRIMARY KEY,
                        parent INTEGER REFERENCES parents(id) ON DELETE CASCADE
                    )",
                    (),
                )?;
                transaction.execute(
                    "CREATE TABLE grandchildren (
                        id INTEGER PRIMARY KEY,
                        child INTEGER REFERENCES children(id) ON DELETE CASCADE
                    )",
                    (),
                )?;
                transaction.execute(
                    "CREATE TABLE labels (
                        id INTEGER PRIMARY KEY,
                        parent INTEGER REFERENCES parents(id) ON DELETE SET NULL
                    )",
                    (),
                )?;
                transaction.execute("INSERT INTO parents VALUES (1)", ())?;
                transaction.execute("INSERT INTO children VALUES (10, 1)", ())?;
                transaction.execute("INSERT INTO grandchildren VALUES (100, 10)", ())?;
                transaction.execute("INSERT INTO labels VALUES (1000, 1)", ())?;
                Ok(())
            })
            .unwrap();
        assert_eq!(first.push().unwrap(), PushOutcome::Drained);
        second.pull().unwrap();
        second.rebase().unwrap();

        first
            .execute("DELETE FROM parents WHERE id = 1", ())
            .unwrap();
        second
            .execute("INSERT INTO children VALUES (11, 1)", ())
            .unwrap();
        assert_eq!(second.push().unwrap(), PushOutcome::Drained);
        let PushOutcome::Rejected(rejection) = first.push().unwrap() else {
            panic!("cascade did not conflict with an admitted child insert")
        };
        drop(first);
        let first = MultiliteConnection::open_with(&first_path, first_options()).unwrap();
        first.rollback(&rejection).unwrap();
        assert_eq!(first.push().unwrap(), PushOutcome::Drained);

        first.pull().unwrap();
        second.pull().unwrap();
        first.rebase().unwrap();
        second.rebase().unwrap();
        for database in [&first, &second] {
            assert_eq!(
                database
                    .query("SELECT id FROM parents", (), |row| row.get::<_, i64>(0))
                    .unwrap(),
                [1]
            );
            assert_eq!(
                database
                    .query("SELECT id FROM children ORDER BY id", (), |row| {
                        row.get::<_, i64>(0)
                    })
                    .unwrap(),
                [10, 11]
            );
            assert_eq!(
                database
                    .query("SELECT id FROM grandchildren", (), |row| row
                        .get::<_, i64>(0))
                    .unwrap(),
                [100]
            );
            assert_eq!(
                database
                    .query("SELECT id, parent FROM labels", (), |row| {
                        Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?))
                    })
                    .unwrap(),
                [(1000, 1)]
            );
        }

        first
            .execute("DELETE FROM parents WHERE id = 1", ())
            .unwrap();
        assert_eq!(first.push().unwrap(), PushOutcome::Drained);
        first.pull().unwrap();
        second.pull().unwrap();
        first.rebase().unwrap();
        second.rebase().unwrap();
        for database in [&first, &second] {
            assert_eq!(
                database
                    .query("SELECT count(*) FROM parents", (), |row| row
                        .get::<_, i64>(0))
                    .unwrap(),
                [0]
            );
            assert_eq!(
                database
                    .query("SELECT count(*) FROM children", (), |row| row
                        .get::<_, i64>(0))
                    .unwrap(),
                [0]
            );
            assert_eq!(
                database
                    .query("SELECT count(*) FROM grandchildren", (), |row| {
                        row.get::<_, i64>(0)
                    })
                    .unwrap(),
                [0]
            );
            assert_eq!(
                database
                    .query("SELECT id, parent FROM labels", (), |row| {
                        Ok((row.get::<_, i64>(0)?, row.get::<_, Option<i64>>(1)?))
                    })
                    .unwrap(),
                [(1000, None)]
            );
        }
    }
}

#[test]
fn set_default_and_restrict_delete_races_repair_and_converge() {
    for isolation in [IsolationLevel::Snapshot, IsolationLevel::Serializable] {
        for (name, action, has_initial_child) in [
            ("set-default", "SET DEFAULT", true),
            ("restrict", "RESTRICT", false),
        ] {
            let directory = tempfile::tempdir().unwrap();
            let server = server();
            let first_path = directory.path().join(format!("{name}-first.sqlite"));
            let first_options = || {
                OpenOptions::new()
                    .isolation_level(isolation)
                    .server(router(Arc::clone(&server)))
            };
            let first = MultiliteConnection::open_with(&first_path, first_options()).unwrap();
            assert!(server.create_space(SpaceId(first.database_id().to_bytes())));
            let second = MultiliteConnection::open_with(
                directory.path().join(format!("{name}-second.sqlite")),
                OpenOptions::new()
                    .isolation_level(isolation)
                    .invitation(first.replica_invitation())
                    .server(router(Arc::clone(&server))),
            )
            .unwrap();

            first
                .update(|transaction| {
                    transaction.execute(
                        "CREATE TABLE parents (id INTEGER PRIMARY KEY, body TEXT NOT NULL)",
                        (),
                    )?;
                    transaction.execute(
                        &format!(
                            "CREATE TABLE children (
                                id INTEGER PRIMARY KEY,
                                parent INTEGER NOT NULL DEFAULT 0
                                    REFERENCES parents(id) ON DELETE {action},
                                body TEXT NOT NULL
                            ) STRICT"
                        ),
                        (),
                    )?;
                    transaction.execute(
                        "INSERT INTO parents VALUES (0, 'fallback'), (1, 'target')",
                        (),
                    )?;
                    if has_initial_child {
                        transaction
                            .execute("INSERT INTO children VALUES (10, 1, 'existing')", ())?;
                    }
                    Ok(())
                })
                .unwrap();
            assert_eq!(first.push().unwrap(), PushOutcome::Drained);
            second.pull().unwrap();
            second.rebase().unwrap();

            first
                .execute("DELETE FROM parents WHERE id = 1", ())
                .unwrap();
            second
                .execute("INSERT INTO children VALUES (11, 1, 'concurrent')", ())
                .unwrap();
            assert_eq!(second.push().unwrap(), PushOutcome::Drained);
            let PushOutcome::Rejected(rejection) = first.push().unwrap() else {
                panic!("{action} parent delete did not conflict with a concurrent child")
            };

            drop(first);
            let first = MultiliteConnection::open_with(&first_path, first_options()).unwrap();
            first.rollback(&rejection).unwrap();
            assert_eq!(first.push().unwrap(), PushOutcome::Drained);
            first.pull().unwrap();
            second.pull().unwrap();
            first.rebase().unwrap();
            second.rebase().unwrap();

            let expected = if has_initial_child {
                vec![
                    (10, 1, "existing".to_owned()),
                    (11, 1, "concurrent".to_owned()),
                ]
            } else {
                vec![(11, 1, "concurrent".to_owned())]
            };
            for database in [&first, &second] {
                assert_eq!(
                    database
                        .query("SELECT id FROM parents ORDER BY id", (), |row| {
                            row.get::<_, i64>(0)
                        })
                        .unwrap(),
                    [0, 1]
                );
                assert_eq!(
                    database
                        .query(
                            "SELECT id, parent, body FROM children ORDER BY id",
                            (),
                            |row| {
                                Ok((
                                    row.get::<_, i64>(0)?,
                                    row.get::<_, i64>(1)?,
                                    row.get::<_, String>(2)?,
                                ))
                            },
                        )
                        .unwrap(),
                    expected
                );
            }

            if action == "RESTRICT" {
                assert!(matches!(
                    first.execute("DELETE FROM parents WHERE id = 1", ()),
                    Err(Error::Sqlite(_))
                ));
                first
                    .execute("DELETE FROM children WHERE id = 11", ())
                    .unwrap();
            }
            first
                .execute("DELETE FROM parents WHERE id = 1", ())
                .unwrap();
            assert_eq!(first.push().unwrap(), PushOutcome::Drained);
            first.pull().unwrap();
            second.pull().unwrap();
            first.rebase().unwrap();
            second.rebase().unwrap();
            for database in [&first, &second] {
                assert_eq!(
                    database
                        .query("SELECT id FROM parents", (), |row| row.get::<_, i64>(0))
                        .unwrap(),
                    [0]
                );
                let expected_children = if action == "SET DEFAULT" {
                    vec![(10, 0), (11, 0)]
                } else {
                    Vec::new()
                };
                assert_eq!(
                    database
                        .query("SELECT id, parent FROM children ORDER BY id", (), |row| {
                            Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?))
                        })
                        .unwrap(),
                    expected_children
                );
            }
        }
    }
}

#[test]
fn mutating_update_actions_repair_races_and_converge() {
    for isolation in [IsolationLevel::Snapshot, IsolationLevel::Serializable] {
        for (name, action, has_initial_child, final_parent) in [
            ("cascade", "CASCADE", true, Some(2)),
            ("set-null", "SET NULL", true, None),
            ("set-default", "SET DEFAULT", true, Some(0)),
            ("restrict", "RESTRICT", false, None),
        ] {
            let directory = tempfile::tempdir().unwrap();
            let server = server();
            let first_path = directory.path().join(format!("update-{name}-first.sqlite"));
            let first_options = || {
                OpenOptions::new()
                    .isolation_level(isolation)
                    .server(router(Arc::clone(&server)))
            };
            let first = MultiliteConnection::open_with(&first_path, first_options()).unwrap();
            assert!(server.create_space(SpaceId(first.database_id().to_bytes())));
            let second = MultiliteConnection::open_with(
                directory
                    .path()
                    .join(format!("update-{name}-second.sqlite")),
                OpenOptions::new()
                    .isolation_level(isolation)
                    .invitation(first.replica_invitation())
                    .server(router(Arc::clone(&server))),
            )
            .unwrap();

            first
                .update(|transaction| {
                    transaction.execute(
                        "CREATE TABLE parents (id INTEGER PRIMARY KEY, body TEXT NOT NULL)",
                        (),
                    )?;
                    transaction.execute(
                        &format!(
                            "CREATE TABLE children (
                                id INTEGER PRIMARY KEY,
                                parent INTEGER DEFAULT 0
                                    REFERENCES parents(id) ON UPDATE {action},
                                body TEXT NOT NULL
                            ) STRICT"
                        ),
                        (),
                    )?;
                    transaction.execute(
                        "INSERT INTO parents VALUES (0, 'fallback'), (1, 'target')",
                        (),
                    )?;
                    if has_initial_child {
                        transaction
                            .execute("INSERT INTO children VALUES (10, 1, 'existing')", ())?;
                    }
                    Ok(())
                })
                .unwrap();
            assert_eq!(first.push().unwrap(), PushOutcome::Drained);
            second.pull().unwrap();
            second.rebase().unwrap();

            first
                .execute("UPDATE parents SET id = 2 WHERE id = 1", ())
                .unwrap();
            second
                .execute("INSERT INTO children VALUES (11, 1, 'concurrent')", ())
                .unwrap();
            assert_eq!(second.push().unwrap(), PushOutcome::Drained);
            let PushOutcome::Rejected(rejection) = first.push().unwrap() else {
                panic!("ON UPDATE {action} did not conflict with a concurrent child")
            };

            drop(first);
            let first = MultiliteConnection::open_with(&first_path, first_options()).unwrap();
            first.rollback(&rejection).unwrap();
            assert_eq!(first.push().unwrap(), PushOutcome::Drained);
            first.pull().unwrap();
            second.pull().unwrap();
            first.rebase().unwrap();
            second.rebase().unwrap();

            let restored = if has_initial_child {
                vec![(10, Some(1)), (11, Some(1))]
            } else {
                vec![(11, Some(1))]
            };
            for database in [&first, &second] {
                assert_eq!(
                    database
                        .query("SELECT id FROM parents ORDER BY id", (), |row| {
                            row.get::<_, i64>(0)
                        })
                        .unwrap(),
                    [0, 1]
                );
                assert_eq!(
                    database
                        .query("SELECT id, parent FROM children ORDER BY id", (), |row| {
                            Ok((row.get::<_, i64>(0)?, row.get::<_, Option<i64>>(1)?))
                        })
                        .unwrap(),
                    restored
                );
            }

            if action == "RESTRICT" {
                assert!(matches!(
                    first.execute("UPDATE parents SET id = 2 WHERE id = 1", ()),
                    Err(Error::Sqlite(_))
                ));
                first
                    .execute("DELETE FROM children WHERE id = 11", ())
                    .unwrap();
            }
            first
                .execute("UPDATE parents SET id = 2 WHERE id = 1", ())
                .unwrap();
            assert_eq!(first.push().unwrap(), PushOutcome::Drained);
            first.pull().unwrap();
            second.pull().unwrap();
            first.rebase().unwrap();
            second.rebase().unwrap();

            for database in [&first, &second] {
                assert_eq!(
                    database
                        .query("SELECT id FROM parents ORDER BY id", (), |row| {
                            row.get::<_, i64>(0)
                        })
                        .unwrap(),
                    [0, 2]
                );
                let expected = if action == "RESTRICT" {
                    Vec::new()
                } else {
                    vec![(10, final_parent), (11, final_parent)]
                };
                assert_eq!(
                    database
                        .query("SELECT id, parent FROM children ORDER BY id", (), |row| {
                            Ok((row.get::<_, i64>(0)?, row.get::<_, Option<i64>>(1)?))
                        })
                        .unwrap(),
                    expected
                );
            }
        }
    }
}

#[test]
fn unique_parent_relationships_conflict_symmetrically_and_allow_sibling_inserts() {
    let directory = tempfile::tempdir().unwrap();
    let server = server();
    let first = MultiliteConnection::open_with(
        directory.path().join("unique-foreign-first.sqlite"),
        OpenOptions::new().server(router(Arc::clone(&server))),
    )
    .unwrap();
    assert!(server.create_space(SpaceId(first.database_id().to_bytes())));
    let second = MultiliteConnection::open_with(
        directory.path().join("unique-foreign-second.sqlite"),
        OpenOptions::new()
            .invitation(first.replica_invitation())
            .server(router(Arc::clone(&server))),
    )
    .unwrap();

    first
        .execute(
            "CREATE TABLE accounts (
                id INTEGER PRIMARY KEY,
                tenant TEXT NOT NULL,
                email TEXT NOT NULL,
                UNIQUE (tenant, email)
            )",
            (),
        )
        .unwrap();
    first
        .execute(
            "CREATE TABLE messages (
                id INTEGER PRIMARY KEY,
                tenant TEXT,
                recipient TEXT,
                FOREIGN KEY (tenant, recipient)
                    REFERENCES accounts (tenant, email)
            )",
            (),
        )
        .unwrap();
    first
        .execute(
            "INSERT INTO accounts VALUES
                (1, 'acme', 'one@example.com'),
                (2, 'acme', 'two@example.com'),
                (3, 'acme', 'shared@example.com')",
            (),
        )
        .unwrap();
    assert_eq!(first.push().unwrap(), PushOutcome::Drained);
    second.pull().unwrap();
    second.rebase().unwrap();

    first
        .execute("DELETE FROM accounts WHERE id = 1", ())
        .unwrap();
    second
        .execute(
            "INSERT INTO messages VALUES (10, 'acme', 'one@example.com')",
            (),
        )
        .unwrap();
    assert_eq!(first.push().unwrap(), PushOutcome::Drained);
    let PushOutcome::Rejected(rejection) = second.push().unwrap() else {
        panic!("UNIQUE-backed child insert did not conflict with parent deletion")
    };
    second.rollback(&rejection).unwrap();
    assert_eq!(second.push().unwrap(), PushOutcome::Drained);
    first.pull().unwrap();
    second.pull().unwrap();
    first.rebase().unwrap();
    second.rebase().unwrap();

    second
        .execute(
            "INSERT INTO messages VALUES (20, 'acme', 'two@example.com')",
            (),
        )
        .unwrap();
    first
        .execute("DELETE FROM accounts WHERE id = 2", ())
        .unwrap();
    assert_eq!(second.push().unwrap(), PushOutcome::Drained);
    let PushOutcome::Rejected(rejection) = first.push().unwrap() else {
        panic!("UNIQUE-backed parent deletion did not conflict with child insert")
    };
    first.rollback(&rejection).unwrap();
    assert_eq!(first.push().unwrap(), PushOutcome::Drained);
    first.pull().unwrap();
    second.pull().unwrap();
    first.rebase().unwrap();
    second.rebase().unwrap();

    first
        .execute(
            "INSERT INTO messages VALUES (30, 'acme', 'shared@example.com')",
            (),
        )
        .unwrap();
    second
        .execute(
            "INSERT INTO messages VALUES (31, 'acme', 'shared@example.com')",
            (),
        )
        .unwrap();
    assert_eq!(first.push().unwrap(), PushOutcome::Drained);
    assert_eq!(second.push().unwrap(), PushOutcome::Drained);
    first.pull().unwrap();
    second.pull().unwrap();
    first.rebase().unwrap();
    second.rebase().unwrap();

    for database in [&first, &second] {
        assert_eq!(
            database
                .query("SELECT id FROM accounts ORDER BY id", (), |row| {
                    row.get::<_, i64>(0)
                })
                .unwrap(),
            [2, 3]
        );
        assert_eq!(
            database
                .query("SELECT id FROM messages ORDER BY id", (), |row| {
                    row.get::<_, i64>(0)
                })
                .unwrap(),
            [20, 30, 31]
        );
    }
}

#[test]
fn foreign_key_creation_and_unique_index_drop_conflict_in_either_order() {
    let directory = tempfile::tempdir().unwrap();
    let server = server();
    let first = MultiliteConnection::open_with(
        directory.path().join("foreign-index-first.sqlite"),
        OpenOptions::new().server(router(Arc::clone(&server))),
    )
    .unwrap();
    assert!(server.create_space(SpaceId(first.database_id().to_bytes())));
    let second = MultiliteConnection::open_with(
        directory.path().join("foreign-index-second.sqlite"),
        OpenOptions::new()
            .invitation(first.replica_invitation())
            .server(router(Arc::clone(&server))),
    )
    .unwrap();

    first
        .update(|transaction| {
            transaction.execute(
                "CREATE TABLE first_parents (
                    id INTEGER PRIMARY KEY,
                    tenant TEXT NOT NULL,
                    code TEXT NOT NULL
                )",
                (),
            )?;
            transaction.execute(
                "CREATE UNIQUE INDEX first_parent_key
                 ON first_parents (tenant, code)",
                (),
            )?;
            transaction.execute(
                "CREATE TABLE second_parents (
                    id INTEGER PRIMARY KEY,
                    tenant TEXT NOT NULL,
                    code TEXT NOT NULL
                )",
                (),
            )?;
            transaction.execute(
                "CREATE UNIQUE INDEX second_parent_key
                 ON second_parents (tenant, code)",
                (),
            )?;
            Ok(())
        })
        .unwrap();
    assert_eq!(first.push().unwrap(), PushOutcome::Drained);
    second.pull().unwrap();
    second.rebase().unwrap();

    first
        .execute(
            "CREATE TABLE first_children (
                id INTEGER PRIMARY KEY,
                tenant TEXT,
                code TEXT,
                FOREIGN KEY (tenant, code)
                    REFERENCES first_parents (tenant, code)
            )",
            (),
        )
        .unwrap();
    second.execute("DROP INDEX first_parent_key", ()).unwrap();
    assert_eq!(first.push().unwrap(), PushOutcome::Drained);
    let PushOutcome::Rejected(rejection) = second.push().unwrap() else {
        panic!("index drop did not conflict with admitted foreign-key creation")
    };
    second.rollback(&rejection).unwrap();
    assert_eq!(second.push().unwrap(), PushOutcome::Drained);
    first.pull().unwrap();
    second.pull().unwrap();
    first.rebase().unwrap();
    second.rebase().unwrap();

    first.execute("DROP INDEX second_parent_key", ()).unwrap();
    second
        .execute(
            "CREATE TABLE second_children (
                id INTEGER PRIMARY KEY,
                tenant TEXT,
                code TEXT,
                FOREIGN KEY (tenant, code)
                    REFERENCES second_parents (tenant, code)
            )",
            (),
        )
        .unwrap();
    assert_eq!(first.push().unwrap(), PushOutcome::Drained);
    let PushOutcome::Rejected(rejection) = second.push().unwrap() else {
        panic!("foreign-key creation did not conflict with admitted index drop")
    };
    second.rollback(&rejection).unwrap();
    assert_eq!(second.push().unwrap(), PushOutcome::Drained);
    first.pull().unwrap();
    second.pull().unwrap();
    first.rebase().unwrap();
    second.rebase().unwrap();

    for database in [&first, &second] {
        assert_eq!(
            database
                .query(
                    "SELECT name FROM sqlite_schema
                     WHERE name IN (
                        'first_parent_key',
                        'first_children',
                        'second_parent_key',
                        'second_children'
                     )
                     ORDER BY name",
                    (),
                    |row| row.get::<_, String>(0),
                )
                .unwrap(),
            ["first_children", "first_parent_key"]
        );
    }
}

#[test]
fn unique_parent_key_updates_conflict_with_child_inserts_in_either_order() {
    let directory = tempfile::tempdir().unwrap();
    let server = server();
    let first = MultiliteConnection::open_with(
        directory.path().join("unique-update-first.sqlite"),
        OpenOptions::new().server(router(Arc::clone(&server))),
    )
    .unwrap();
    assert!(server.create_space(SpaceId(first.database_id().to_bytes())));
    let second = MultiliteConnection::open_with(
        directory.path().join("unique-update-second.sqlite"),
        OpenOptions::new()
            .invitation(first.replica_invitation())
            .server(router(Arc::clone(&server))),
    )
    .unwrap();

    first
        .execute(
            "CREATE TABLE accounts (
                id INTEGER PRIMARY KEY,
                email TEXT UNIQUE
            )",
            (),
        )
        .unwrap();
    first
        .execute(
            "CREATE TABLE messages (
                id INTEGER PRIMARY KEY,
                recipient TEXT REFERENCES accounts (email)
            )",
            (),
        )
        .unwrap();
    first
        .execute(
            "INSERT INTO accounts VALUES
                (1, 'one@example.com'),
                (2, 'two@example.com')",
            (),
        )
        .unwrap();
    assert_eq!(first.push().unwrap(), PushOutcome::Drained);
    second.pull().unwrap();
    second.rebase().unwrap();

    first
        .execute(
            "UPDATE accounts SET email = 'moved@example.com' WHERE id = 1",
            (),
        )
        .unwrap();
    second
        .execute("INSERT INTO messages VALUES (10, 'one@example.com')", ())
        .unwrap();
    assert_eq!(first.push().unwrap(), PushOutcome::Drained);
    let PushOutcome::Rejected(rejection) = second.push().unwrap() else {
        panic!("child insert did not conflict with an admitted UNIQUE-key update")
    };
    second.rollback(&rejection).unwrap();
    assert_eq!(second.push().unwrap(), PushOutcome::Drained);
    first.pull().unwrap();
    second.pull().unwrap();
    first.rebase().unwrap();
    second.rebase().unwrap();

    second
        .execute("INSERT INTO messages VALUES (20, 'two@example.com')", ())
        .unwrap();
    first
        .execute(
            "UPDATE accounts SET email = 'other@example.com' WHERE id = 2",
            (),
        )
        .unwrap();
    assert_eq!(second.push().unwrap(), PushOutcome::Drained);
    let PushOutcome::Rejected(rejection) = first.push().unwrap() else {
        panic!("UNIQUE-key update did not conflict with an admitted child insert")
    };
    first.rollback(&rejection).unwrap();
    assert_eq!(first.push().unwrap(), PushOutcome::Drained);
    first.pull().unwrap();
    second.pull().unwrap();
    first.rebase().unwrap();
    second.rebase().unwrap();

    for database in [&first, &second] {
        assert_eq!(
            database
                .query("SELECT id, email FROM accounts ORDER BY id", (), |row| {
                    Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
                })
                .unwrap(),
            [
                (1, "moved@example.com".into()),
                (2, "two@example.com".into()),
            ]
        );
        assert_eq!(
            database
                .query("SELECT id, recipient FROM messages", (), |row| {
                    Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
                })
                .unwrap(),
            [(20, "two@example.com".into())]
        );
    }
}

#[test]
fn composite_foreign_reference_conflicts_with_the_exact_parent_delete() {
    let directory = tempfile::tempdir().unwrap();
    let server = server();
    let first = MultiliteConnection::open_with(
        directory.path().join("composite-foreign-first.sqlite"),
        OpenOptions::new().server(router(Arc::clone(&server))),
    )
    .unwrap();
    assert!(server.create_space(SpaceId(first.database_id().to_bytes())));
    let second = MultiliteConnection::open_with(
        directory.path().join("composite-foreign-second.sqlite"),
        OpenOptions::new()
            .invitation(first.replica_invitation())
            .server(router(Arc::clone(&server))),
    )
    .unwrap();

    first
        .update(|transaction| {
            transaction.execute(
                "CREATE TABLE parents (
                    tenant TEXT NOT NULL,
                    id INTEGER NOT NULL,
                    body TEXT,
                    PRIMARY KEY (tenant, id)
                ) WITHOUT ROWID",
                (),
            )?;
            transaction.execute(
                "CREATE TABLE children (
                    id INTEGER PRIMARY KEY,
                    parent_tenant TEXT,
                    parent_id INTEGER,
                    FOREIGN KEY (parent_tenant, parent_id)
                        REFERENCES parents (tenant, id)
                )",
                (),
            )?;
            transaction.execute("INSERT INTO parents VALUES ('acme', 7, 'parent')", ())?;
            Ok(())
        })
        .unwrap();
    assert_eq!(first.push().unwrap(), PushOutcome::Drained);
    second.pull().unwrap();
    second.rebase().unwrap();

    first
        .execute("DELETE FROM parents WHERE tenant = 'acme' AND id = 7", ())
        .unwrap();
    second
        .execute("INSERT INTO children VALUES (10, 'acme', 7)", ())
        .unwrap();
    assert_eq!(first.push().unwrap(), PushOutcome::Drained);
    let PushOutcome::Rejected(rejection) = second.push().unwrap() else {
        panic!("composite child reference did not conflict with its parent deletion")
    };
    second.rollback(&rejection).unwrap();
    assert_eq!(second.push().unwrap(), PushOutcome::Drained);

    first.pull().unwrap();
    second.pull().unwrap();
    first.rebase().unwrap();
    second.rebase().unwrap();
    for database in [&first, &second] {
        assert_eq!(
            database
                .query("SELECT count(*) FROM parents", (), |row| {
                    row.get::<_, i64>(0)
                })
                .unwrap(),
            [0]
        );
        assert_eq!(
            database
                .query("SELECT count(*) FROM children", (), |row| {
                    row.get::<_, i64>(0)
                })
                .unwrap(),
            [0]
        );
    }
}

#[test]
fn child_retarget_conflicts_with_deleting_its_new_parent() {
    let directory = tempfile::tempdir().unwrap();
    let server = server();
    let first = MultiliteConnection::open_with(
        directory.path().join("retarget-first.sqlite"),
        OpenOptions::new().server(router(Arc::clone(&server))),
    )
    .unwrap();
    assert!(server.create_space(SpaceId(first.database_id().to_bytes())));
    let second = MultiliteConnection::open_with(
        directory.path().join("retarget-second.sqlite"),
        OpenOptions::new()
            .invitation(first.replica_invitation())
            .server(router(Arc::clone(&server))),
    )
    .unwrap();

    first
        .update(|transaction| {
            transaction.execute("CREATE TABLE parents (id INTEGER PRIMARY KEY)", ())?;
            transaction.execute(
                "CREATE TABLE children (
                    id INTEGER PRIMARY KEY,
                    parent INTEGER REFERENCES parents(id)
                )",
                (),
            )?;
            transaction.execute("INSERT INTO parents VALUES (1), (2)", ())?;
            transaction.execute("INSERT INTO children VALUES (10, 1)", ())?;
            Ok(())
        })
        .unwrap();
    assert_eq!(first.push().unwrap(), PushOutcome::Drained);
    second.pull().unwrap();
    second.rebase().unwrap();

    first
        .execute("UPDATE children SET parent = 2 WHERE id = 10", ())
        .unwrap();
    second
        .execute("DELETE FROM parents WHERE id = 2", ())
        .unwrap();
    assert_eq!(first.push().unwrap(), PushOutcome::Drained);
    let PushOutcome::Rejected(rejection) = second.push().unwrap() else {
        panic!("parent deletion did not conflict with an admitted child retarget")
    };
    second.rollback(&rejection).unwrap();
    assert_eq!(second.push().unwrap(), PushOutcome::Drained);

    first.pull().unwrap();
    second.pull().unwrap();
    first.rebase().unwrap();
    second.rebase().unwrap();
    for database in [&first, &second] {
        assert_eq!(
            database
                .query("SELECT id, parent FROM children", (), |row| {
                    Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?))
                })
                .unwrap(),
            [(10, 2)]
        );
        assert_eq!(
            database
                .query("SELECT id FROM parents ORDER BY id", (), |row| {
                    row.get::<_, i64>(0)
                })
                .unwrap(),
            [1, 2]
        );
    }
}

#[test]
fn foreign_references_do_not_conflict_across_disjoint_parent_rows() {
    let directory = tempfile::tempdir().unwrap();
    let server = server();
    let first = MultiliteConnection::open_with(
        directory.path().join("foreign-key-precise-first.sqlite"),
        OpenOptions::new().server(router(Arc::clone(&server))),
    )
    .unwrap();
    assert!(server.create_space(SpaceId(first.database_id().to_bytes())));
    let second = MultiliteConnection::open_with(
        directory.path().join("foreign-key-precise-second.sqlite"),
        OpenOptions::new()
            .invitation(first.replica_invitation())
            .server(router(Arc::clone(&server))),
    )
    .unwrap();

    first
        .execute(
            "CREATE TABLE parents (id INTEGER PRIMARY KEY, body TEXT)",
            (),
        )
        .unwrap();
    first
        .execute(
            "CREATE TABLE children (
                id INTEGER PRIMARY KEY,
                parent INTEGER REFERENCES parents(id)
            )",
            (),
        )
        .unwrap();
    first
        .execute("INSERT INTO parents VALUES (1, 'one'), (2, 'two')", ())
        .unwrap();
    assert_eq!(first.push().unwrap(), PushOutcome::Drained);
    second.pull().unwrap();
    second.rebase().unwrap();

    first
        .execute("DELETE FROM parents WHERE id = 1", ())
        .unwrap();
    second
        .execute("INSERT INTO children VALUES (20, 2)", ())
        .unwrap();
    assert_eq!(second.push().unwrap(), PushOutcome::Drained);
    assert_eq!(first.push().unwrap(), PushOutcome::Drained);
    first.pull().unwrap();
    second.pull().unwrap();
    first.rebase().unwrap();
    second.rebase().unwrap();

    for database in [&first, &second] {
        assert_eq!(
            database
                .query("SELECT id FROM parents ORDER BY id", (), |row| {
                    row.get::<_, i64>(0)
                })
                .unwrap(),
            [2]
        );
        assert_eq!(
            database
                .query("SELECT id, parent FROM children", (), |row| {
                    Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?))
                })
                .unwrap(),
            [(20, 2)]
        );
    }
}

#[test]
fn unrelated_parent_updates_and_child_inserts_admit_for_both_target_kinds() {
    let directory = tempfile::tempdir().unwrap();
    let server = server();
    let first = MultiliteConnection::open_with(
        directory.path().join("foreign-key-body-first.sqlite"),
        OpenOptions::new().server(router(Arc::clone(&server))),
    )
    .unwrap();
    assert!(server.create_space(SpaceId(first.database_id().to_bytes())));
    let second = MultiliteConnection::open_with(
        directory.path().join("foreign-key-body-second.sqlite"),
        OpenOptions::new()
            .invitation(first.replica_invitation())
            .server(router(Arc::clone(&server))),
    )
    .unwrap();

    first
        .update(|transaction| {
            transaction.execute(
                "CREATE TABLE parents (
                    id INTEGER PRIMARY KEY,
                    email TEXT UNIQUE,
                    body TEXT
                )",
                (),
            )?;
            transaction.execute(
                "CREATE TABLE primary_children (
                    id INTEGER PRIMARY KEY,
                    parent INTEGER REFERENCES parents(id)
                )",
                (),
            )?;
            transaction.execute(
                "CREATE TABLE unique_children (
                    id INTEGER PRIMARY KEY,
                    parent_email TEXT REFERENCES parents(email)
                )",
                (),
            )?;
            transaction.execute(
                "INSERT INTO parents VALUES
                    (1, 'one@example.com', 'one'),
                    (2, 'two@example.com', 'two')",
                (),
            )?;
            Ok(())
        })
        .unwrap();
    assert_eq!(first.push().unwrap(), PushOutcome::Drained);
    second.pull().unwrap();
    second.rebase().unwrap();

    first
        .execute("UPDATE parents SET body = 'one-updated' WHERE id = 1", ())
        .unwrap();
    second
        .update(|transaction| {
            transaction.execute("INSERT INTO primary_children VALUES (10, 1)", ())?;
            transaction.execute(
                "INSERT INTO unique_children VALUES (10, 'one@example.com')",
                (),
            )?;
            Ok(())
        })
        .unwrap();
    assert_eq!(first.push().unwrap(), PushOutcome::Drained);
    assert_eq!(
        second.push().unwrap(),
        PushOutcome::Drained,
        "an unrelated parent update must not invalidate child references"
    );
    first.pull().unwrap();
    second.pull().unwrap();
    first.rebase().unwrap();
    second.rebase().unwrap();

    first
        .update(|transaction| {
            transaction.execute("INSERT INTO primary_children VALUES (20, 2)", ())?;
            transaction.execute(
                "INSERT INTO unique_children VALUES (20, 'two@example.com')",
                (),
            )?;
            Ok(())
        })
        .unwrap();
    second
        .execute("UPDATE parents SET body = 'two-updated' WHERE id = 2", ())
        .unwrap();
    assert_eq!(first.push().unwrap(), PushOutcome::Drained);
    assert_eq!(
        second.push().unwrap(),
        PushOutcome::Drained,
        "a child reference must not invalidate an unrelated parent update"
    );
    first.pull().unwrap();
    second.pull().unwrap();
    first.rebase().unwrap();
    second.rebase().unwrap();

    for database in [&first, &second] {
        assert_eq!(
            database
                .query("SELECT id, body FROM parents ORDER BY id", (), |row| {
                    Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
                })
                .unwrap(),
            [(1, "one-updated".into()), (2, "two-updated".into())]
        );
        assert_eq!(
            database
                .query(
                    "SELECT id, parent FROM primary_children ORDER BY id",
                    (),
                    |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
                )
                .unwrap(),
            [(10, 1), (20, 2)]
        );
        assert_eq!(
            database
                .query(
                    "SELECT id, parent_email FROM unique_children ORDER BY id",
                    (),
                    |row| Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?)),
                )
                .unwrap(),
            [
                (10, "one@example.com".into()),
                (20, "two@example.com".into()),
            ]
        );
    }
}

#[test]
fn creating_an_incoming_foreign_key_fences_stale_parent_writes() {
    let directory = tempfile::tempdir().unwrap();
    let server = server();
    let first = MultiliteConnection::open_with(
        directory.path().join("foreign-key-ddl-first.sqlite"),
        OpenOptions::new().server(router(Arc::clone(&server))),
    )
    .unwrap();
    assert!(server.create_space(SpaceId(first.database_id().to_bytes())));
    let second = MultiliteConnection::open_with(
        directory.path().join("foreign-key-ddl-second.sqlite"),
        OpenOptions::new()
            .invitation(first.replica_invitation())
            .server(router(Arc::clone(&server))),
    )
    .unwrap();

    first
        .execute(
            "CREATE TABLE parents (id INTEGER PRIMARY KEY, body TEXT)",
            (),
        )
        .unwrap();
    first
        .execute("INSERT INTO parents VALUES (1, 'keep')", ())
        .unwrap();
    assert_eq!(first.push().unwrap(), PushOutcome::Drained);
    second.pull().unwrap();
    second.rebase().unwrap();

    first
        .execute("DELETE FROM parents WHERE id = 1", ())
        .unwrap();
    second
        .execute(
            "CREATE TABLE children (
                id INTEGER PRIMARY KEY,
                parent INTEGER REFERENCES parents(id)
            )",
            (),
        )
        .unwrap();
    assert_eq!(second.push().unwrap(), PushOutcome::Drained);
    let PushOutcome::Rejected(rejection) = first.push().unwrap() else {
        panic!("parent write compiled before the relationship was admitted")
    };
    first.rollback(&rejection).unwrap();
    assert_eq!(first.push().unwrap(), PushOutcome::Drained);
    first.pull().unwrap();
    second.pull().unwrap();
    first.rebase().unwrap();
    second.rebase().unwrap();

    for database in [&first, &second] {
        assert_eq!(
            database
                .query("SELECT id, body FROM parents", (), |row| {
                    Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
                })
                .unwrap(),
            [(1, "keep".into())]
        );
        assert_eq!(
            database
                .query(
                    "SELECT count(*) FROM sqlite_schema
                     WHERE type = 'table' AND name = 'children'",
                    (),
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            [1]
        );
    }
}

#[test]
fn adding_unique_index_fences_rows_compiled_against_the_old_write_contract() {
    let directory = tempfile::tempdir().unwrap();
    let server = server();
    let first = MultiliteConnection::open_with(
        directory.path().join("add-index-first.sqlite"),
        OpenOptions::new().server(router(Arc::clone(&server))),
    )
    .unwrap();
    assert!(server.create_space(SpaceId(first.database_id().to_bytes())));
    let second = MultiliteConnection::open_with(
        directory.path().join("add-index-second.sqlite"),
        OpenOptions::new()
            .invitation(first.replica_invitation())
            .server(router(Arc::clone(&server))),
    )
    .unwrap();

    first
        .execute(
            "CREATE TABLE notes (
                id INTEGER PRIMARY KEY,
                tenant TEXT,
                slug TEXT,
                body TEXT
            )",
            (),
        )
        .unwrap();
    first
        .execute("INSERT INTO notes VALUES (1, 'one', 'home', 'seed')", ())
        .unwrap();
    assert_eq!(first.push().unwrap(), PushOutcome::Drained);
    second.pull().unwrap();
    second.rebase().unwrap();

    second
        .execute("INSERT INTO notes VALUES (2, 'two', 'queued', 'stale')", ())
        .unwrap();
    first
        .execute(
            "CREATE UNIQUE INDEX notes_tenant_slug ON notes (tenant, slug)",
            (),
        )
        .unwrap();
    assert_eq!(first.push().unwrap(), PushOutcome::Drained);

    let PushOutcome::Rejected(rejection) = second.push().unwrap() else {
        panic!("row compiled before CREATE UNIQUE INDEX was admitted")
    };
    second.rollback(&rejection).unwrap();
    assert_eq!(second.push().unwrap(), PushOutcome::Drained);
    second.pull().unwrap();
    second.rebase().unwrap();
    first.pull().unwrap();
    first.rebase().unwrap();

    assert_eq!(index_names(&first), ["notes_tenant_slug"]);
    assert_eq!(index_names(&second), ["notes_tenant_slug"]);
    assert_eq!(
        first
            .query("SELECT id FROM notes ORDER BY id", (), |row| row
                .get::<_, i64>(0))
            .unwrap(),
        [1]
    );
    assert_eq!(
        second
            .query("SELECT id FROM notes ORDER BY id", (), |row| row
                .get::<_, i64>(0))
            .unwrap(),
        [1]
    );

    first
        .execute(
            "INSERT INTO notes VALUES (3, 'shared', 'slug', 'winner')",
            (),
        )
        .unwrap();
    second
        .execute(
            "INSERT INTO notes VALUES (4, 'shared', 'slug', 'loser')",
            (),
        )
        .unwrap();
    assert_eq!(first.push().unwrap(), PushOutcome::Drained);
    let PushOutcome::Rejected(rejection) = second.push().unwrap() else {
        panic!("new UNIQUE index did not coordinate ownership")
    };
    second.rollback(&rejection).unwrap();
}

#[test]
fn secondary_index_lifecycle_converges_without_conflicting_duplicate_values() {
    let directory = tempfile::tempdir().unwrap();
    let server = server();
    let first = MultiliteConnection::open_with(
        directory.path().join("secondary-index-first.sqlite"),
        OpenOptions::new().server(router(Arc::clone(&server))),
    )
    .unwrap();
    assert!(server.create_space(SpaceId(first.database_id().to_bytes())));
    let second = MultiliteConnection::open_with(
        directory.path().join("secondary-index-second.sqlite"),
        OpenOptions::new()
            .invitation(first.replica_invitation())
            .server(router(Arc::clone(&server))),
    )
    .unwrap();

    first
        .execute(
            "CREATE TABLE notes (
                id INTEGER PRIMARY KEY,
                tenant TEXT,
                category TEXT,
                body TEXT
            )",
            (),
        )
        .unwrap();
    first
        .execute(
            "INSERT INTO notes VALUES (1, 'seed', 'shared', 'initial')",
            (),
        )
        .unwrap();
    assert_eq!(first.push().unwrap(), PushOutcome::Drained);
    second.pull().unwrap();
    second.rebase().unwrap();

    second
        .execute(
            "INSERT INTO notes VALUES (2, 'stale', 'shared', 'queued')",
            (),
        )
        .unwrap();
    first
        .execute(
            "CREATE INDEX notes_category_tenant ON notes (
                category COLLATE NOCASE DESC,
                lower(tenant) ASC
            ) WHERE category IS NOT NULL",
            (),
        )
        .unwrap();
    assert_eq!(first.push().unwrap(), PushOutcome::Drained);
    assert_eq!(
        second.push().unwrap(),
        PushOutcome::Drained,
        "ordinary index creation must not reject a stale row operation"
    );
    first.pull().unwrap();
    second.pull().unwrap();
    first.rebase().unwrap();
    second.rebase().unwrap();

    first
        .execute(
            "INSERT INTO notes VALUES (6, 'before-ddl', 'shared', 'existing')",
            (),
        )
        .unwrap();
    second
        .execute(
            "CREATE INDEX notes_tenant ON notes (
                tenant COLLATE RTRIM,
                length(body) DESC,
                tenant
            ) WHERE tenant IS NOT NULL",
            (),
        )
        .unwrap();
    assert_eq!(first.push().unwrap(), PushOutcome::Drained);
    assert_eq!(
        second.push().unwrap(),
        PushOutcome::Drained,
        "ordinary index creation must admit after a concurrent row operation"
    );
    first.pull().unwrap();
    second.pull().unwrap();
    first.rebase().unwrap();
    second.rebase().unwrap();

    first
        .execute("INSERT INTO notes VALUES (3, 'first', 'shared', 'one')", ())
        .unwrap();
    second
        .execute(
            "INSERT INTO notes VALUES (4, 'second', 'shared', 'two')",
            (),
        )
        .unwrap();
    assert_eq!(first.push().unwrap(), PushOutcome::Drained);
    assert_eq!(
        second.push().unwrap(),
        PushOutcome::Drained,
        "duplicate secondary values must coexist through their primary suffixes"
    );
    first.pull().unwrap();
    second.pull().unwrap();
    first.rebase().unwrap();
    second.rebase().unwrap();

    first
        .execute(
            "UPDATE notes SET category = NULL, body = 'changed' WHERE id = 3",
            (),
        )
        .unwrap();
    second
        .execute("DELETE FROM notes WHERE id = 4", ())
        .unwrap();
    assert_eq!(first.push().unwrap(), PushOutcome::Drained);
    assert_eq!(second.push().unwrap(), PushOutcome::Drained);
    first.pull().unwrap();
    second.pull().unwrap();
    first.rebase().unwrap();
    second.rebase().unwrap();

    second
        .execute(
            "INSERT INTO notes VALUES (5, NULL, 'shared', 'after-drop')",
            (),
        )
        .unwrap();
    first
        .execute("DROP INDEX notes_category_tenant", ())
        .unwrap();
    first.execute("DROP INDEX notes_tenant", ()).unwrap();
    assert_eq!(first.push().unwrap(), PushOutcome::Drained);
    assert_eq!(
        second.push().unwrap(),
        PushOutcome::Drained,
        "ordinary index removal and stale rows must remain compatible"
    );
    first.pull().unwrap();
    second.pull().unwrap();
    first.rebase().unwrap();
    second.rebase().unwrap();

    for database in [&first, &second] {
        assert!(index_names(database).is_empty());
        assert_eq!(
            database
                .query(
                    "SELECT id, tenant, category, body FROM notes ORDER BY id",
                    (),
                    |row| {
                        Ok((
                            row.get::<_, i64>(0)?,
                            row.get::<_, Option<String>>(1)?,
                            row.get::<_, Option<String>>(2)?,
                            row.get::<_, String>(3)?,
                        ))
                    },
                )
                .unwrap(),
            [
                (
                    1,
                    Some("seed".into()),
                    Some("shared".into()),
                    "initial".into(),
                ),
                (
                    2,
                    Some("stale".into()),
                    Some("shared".into()),
                    "queued".into(),
                ),
                (3, Some("first".into()), None, "changed".into()),
                (5, None, Some("shared".into()), "after-drop".into(),),
                (
                    6,
                    Some("before-ddl".into()),
                    Some("shared".into()),
                    "existing".into(),
                ),
            ]
        );
    }
}

#[test]
fn dropping_unique_index_keeps_stale_superset_row_operations_compatible() {
    let directory = tempfile::tempdir().unwrap();
    let server = server();
    let first = MultiliteConnection::open_with(
        directory.path().join("drop-index-first.sqlite"),
        OpenOptions::new().server(router(Arc::clone(&server))),
    )
    .unwrap();
    assert!(server.create_space(SpaceId(first.database_id().to_bytes())));
    let second = MultiliteConnection::open_with(
        directory.path().join("drop-index-second.sqlite"),
        OpenOptions::new()
            .invitation(first.replica_invitation())
            .server(router(Arc::clone(&server))),
    )
    .unwrap();

    first
        .update(|transaction| {
            transaction.execute(
                "CREATE TABLE notes (
                    id INTEGER PRIMARY KEY,
                    tenant TEXT,
                    slug TEXT,
                    body TEXT
                )",
                (),
            )?;
            transaction.execute(
                "CREATE UNIQUE INDEX notes_tenant_slug ON notes (tenant, slug)",
                (),
            )?;
            transaction.execute("INSERT INTO notes VALUES (1, 'seed', 'one', 'initial')", ())?;
            Ok(())
        })
        .unwrap();
    assert_eq!(first.push().unwrap(), PushOutcome::Drained);
    second.pull().unwrap();
    second.rebase().unwrap();

    second
        .execute(
            "INSERT INTO notes VALUES (2, 'stale', 'owner', 'queued')",
            (),
        )
        .unwrap();
    first.execute("DROP INDEX notes_tenant_slug", ()).unwrap();
    assert_eq!(first.push().unwrap(), PushOutcome::Drained);
    assert_eq!(
        second.push().unwrap(),
        PushOutcome::Drained,
        "DROP INDEX must not fence a stale writer doing superset bookkeeping"
    );

    first.pull().unwrap();
    second.pull().unwrap();
    first.rebase().unwrap();
    second.rebase().unwrap();
    assert!(index_names(&first).is_empty());
    assert!(index_names(&second).is_empty());

    first
        .execute(
            "INSERT INTO notes VALUES (3, 'duplicate', 'allowed', 'first')",
            (),
        )
        .unwrap();
    second
        .execute(
            "INSERT INTO notes VALUES (4, 'duplicate', 'allowed', 'second')",
            (),
        )
        .unwrap();
    assert_eq!(first.push().unwrap(), PushOutcome::Drained);
    assert_eq!(second.push().unwrap(), PushOutcome::Drained);
    first.pull().unwrap();
    second.pull().unwrap();
    first.rebase().unwrap();
    second.rebase().unwrap();

    let expected = [
        (1, "seed".to_owned(), "one".to_owned()),
        (2, "stale".to_owned(), "owner".to_owned()),
        (3, "duplicate".to_owned(), "allowed".to_owned()),
        (4, "duplicate".to_owned(), "allowed".to_owned()),
    ];
    for database in [&first, &second] {
        assert_eq!(
            database
                .query(
                    "SELECT id, tenant, slug FROM notes ORDER BY id",
                    (),
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )
                .unwrap(),
            expected
        );
    }
}

#[test]
fn conflicting_index_ddl_repairs_local_sqlite_before_converging() {
    let directory = tempfile::tempdir().unwrap();
    let server = server();
    let first = MultiliteConnection::open_with(
        directory.path().join("index-ddl-first.sqlite"),
        OpenOptions::new().server(router(Arc::clone(&server))),
    )
    .unwrap();
    assert!(server.create_space(SpaceId(first.database_id().to_bytes())));
    let second = MultiliteConnection::open_with(
        directory.path().join("index-ddl-second.sqlite"),
        OpenOptions::new()
            .invitation(first.replica_invitation())
            .server(router(Arc::clone(&server))),
    )
    .unwrap();

    first
        .execute(
            "CREATE TABLE notes (
                id INTEGER PRIMARY KEY,
                tenant TEXT,
                slug TEXT,
                body TEXT
            )",
            (),
        )
        .unwrap();
    assert_eq!(first.push().unwrap(), PushOutcome::Drained);
    second.pull().unwrap();
    second.rebase().unwrap();

    first
        .execute(
            "CREATE INDEX notes_identity ON notes (
                tenant COLLATE NOCASE DESC,
                lower(slug)
            ) WHERE tenant IS NOT NULL",
            (),
        )
        .unwrap();
    second
        .execute(
            "CREATE INDEX notes_identity ON notes (
                upper(slug) ASC,
                body COLLATE RTRIM
            ) WHERE body IS NOT NULL",
            (),
        )
        .unwrap();
    assert_eq!(first.push().unwrap(), PushOutcome::Drained);
    let PushOutcome::Rejected(rejection) = second.push().unwrap() else {
        panic!("conflicting index-name ownership was admitted")
    };
    second.rollback(&rejection).unwrap();
    assert!(
        index_names(&second).is_empty(),
        "CREATE rejection must drop its speculative physical index"
    );
    assert_eq!(second.push().unwrap(), PushOutcome::Drained);
    second.pull().unwrap();
    second.rebase().unwrap();
    assert_eq!(index_names(&second), ["notes_identity"]);
    assert_eq!(
        first
            .query(
                "SELECT sql FROM sqlite_schema WHERE name = 'notes_identity'",
                (),
                |row| row.get::<_, String>(0),
            )
            .unwrap(),
        second
            .query(
                "SELECT sql FROM sqlite_schema WHERE name = 'notes_identity'",
                (),
                |row| row.get::<_, String>(0),
            )
            .unwrap()
    );

    first.execute("DROP INDEX notes_identity", ()).unwrap();
    second.execute("DROP INDEX notes_identity", ()).unwrap();
    assert_eq!(first.push().unwrap(), PushOutcome::Drained);
    let PushOutcome::Rejected(rejection) = second.push().unwrap() else {
        panic!("concurrent DROP INDEX operations were both admitted")
    };
    second.rollback(&rejection).unwrap();
    assert_eq!(
        index_names(&second),
        ["notes_identity"],
        "DROP rejection must recreate the physical index before returning"
    );
    assert_eq!(second.push().unwrap(), PushOutcome::Drained);
    second.pull().unwrap();
    second.rebase().unwrap();
    assert!(index_names(&second).is_empty());
}

#[test]
fn tables_and_indexes_contend_for_one_sqlite_schema_name() {
    for index_wins in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let server = server();
        let first = MultiliteConnection::open_with(
            directory
                .path()
                .join(format!("schema-name-{index_wins}-first.sqlite")),
            OpenOptions::new().server(router(Arc::clone(&server))),
        )
        .unwrap();
        assert!(server.create_space(SpaceId(first.database_id().to_bytes())));
        let second = MultiliteConnection::open_with(
            directory
                .path()
                .join(format!("schema-name-{index_wins}-second.sqlite")),
            OpenOptions::new()
                .invitation(first.replica_invitation())
                .server(router(Arc::clone(&server))),
        )
        .unwrap();

        first
            .update(|transaction| {
                transaction.execute("CREATE TABLE notes (id INTEGER PRIMARY KEY)", ())?;
                transaction.execute("CREATE TABLE tasks (id INTEGER PRIMARY KEY)", ())?;
                Ok(())
            })
            .unwrap();
        assert_eq!(first.push().unwrap(), PushOutcome::Drained);
        second.pull().unwrap();
        second.rebase().unwrap();

        first
            .execute("ALTER TABLE notes RENAME TO collision", ())
            .unwrap();
        second
            .execute("CREATE INDEX collision ON tasks (id)", ())
            .unwrap();

        let (winner, loser) = if index_wins {
            (&second, &first)
        } else {
            (&first, &second)
        };
        assert_eq!(winner.push().unwrap(), PushOutcome::Drained);
        let PushOutcome::Rejected(rejection) = loser.push().unwrap() else {
            panic!("a table and index acquired the same SQLite schema name")
        };
        loser.rollback(&rejection).unwrap();
        assert_eq!(loser.push().unwrap(), PushOutcome::Drained);

        first.pull().unwrap();
        second.pull().unwrap();
        first.rebase().unwrap();
        second.rebase().unwrap();

        assert_eq!(tables(&first), tables(&second));
        assert_eq!(index_names(&first), index_names(&second));
        if index_wins {
            assert_eq!(
                tables(&first)
                    .into_iter()
                    .map(|(name, _)| name)
                    .collect::<Vec<_>>(),
                ["notes", "tasks"]
            );
            assert_eq!(index_names(&first), ["collision"]);
        } else {
            assert_eq!(
                tables(&first)
                    .into_iter()
                    .map(|(name, _)| name)
                    .collect::<Vec<_>>(),
                ["collision", "tasks"]
            );
            assert!(index_names(&first).is_empty());
        }
    }
}

#[test]
fn conflicting_table_renames_repair_physical_names_and_converge() {
    let directory = tempfile::tempdir().unwrap();
    let second_path = directory.path().join("rename-ddl-second.sqlite");
    let server = server();
    let first = MultiliteConnection::open_with(
        directory.path().join("rename-ddl-first.sqlite"),
        OpenOptions::new().server(router(Arc::clone(&server))),
    )
    .unwrap();
    assert!(server.create_space(SpaceId(first.database_id().to_bytes())));
    let second = MultiliteConnection::open_with(
        &second_path,
        OpenOptions::new()
            .invitation(first.replica_invitation())
            .server(router(Arc::clone(&server))),
    )
    .unwrap();

    first
        .update(|transaction| {
            transaction.execute("CREATE TABLE alpha (id INTEGER PRIMARY KEY)", ())?;
            transaction.execute("CREATE TABLE beta (id INTEGER PRIMARY KEY)", ())?;
            transaction.execute("INSERT INTO alpha VALUES (1)", ())?;
            transaction.execute("INSERT INTO beta VALUES (2)", ())?;
            Ok(())
        })
        .unwrap();
    assert_eq!(first.push().unwrap(), PushOutcome::Drained);
    second.pull().unwrap();
    second.rebase().unwrap();

    first
        .execute("ALTER TABLE alpha RENAME TO shared", ())
        .unwrap();
    second
        .execute("ALTER TABLE beta RENAME TO shared", ())
        .unwrap();
    assert_eq!(first.push().unwrap(), PushOutcome::Drained);
    let PushOutcome::Rejected(rejection) = second.push().unwrap() else {
        panic!("two tables acquired the same synchronized name")
    };
    drop(second);

    let second = MultiliteConnection::open_with(
        &second_path,
        OpenOptions::new().server(router(Arc::clone(&server))),
    )
    .unwrap();
    second.rollback(&rejection).unwrap();
    assert_eq!(
        second
            .query(
                "SELECT name FROM sqlite_schema
                 WHERE type = 'table' AND name IN ('alpha', 'beta', 'shared')
                 ORDER BY name",
                (),
                |row| row.get::<_, String>(0),
            )
            .unwrap(),
        ["alpha", "beta"],
        "rename rejection must restore the speculative source name after reopen"
    );
    assert_eq!(second.push().unwrap(), PushOutcome::Drained);
    first.pull().unwrap();
    second.pull().unwrap();
    first.rebase().unwrap();
    second.rebase().unwrap();

    first
        .execute("ALTER TABLE shared RENAME TO canonical", ())
        .unwrap();
    second
        .update(|transaction| {
            transaction.execute("ALTER TABLE shared RENAME TO alternate", ())?;
            transaction.execute("ALTER TABLE alternate RENAME TO detour", ())?;
            Ok(())
        })
        .unwrap();
    assert_eq!(first.push().unwrap(), PushOutcome::Drained);
    let PushOutcome::Rejected(rejection) = second.push().unwrap() else {
        panic!("one table admitted two concurrent names")
    };
    second.rollback(&rejection).unwrap();
    assert_eq!(
        second
            .query("SELECT id FROM shared", (), |row| row.get::<_, i64>(0))
            .unwrap(),
        [1]
    );
    assert_eq!(second.push().unwrap(), PushOutcome::Drained);
    first.pull().unwrap();
    second.pull().unwrap();
    first.rebase().unwrap();
    second.rebase().unwrap();

    for database in [&first, &second] {
        assert_eq!(
            database
                .query("SELECT id FROM canonical", (), |row| row.get::<_, i64>(0))
                .unwrap(),
            [1]
        );
        assert_eq!(
            database
                .query("SELECT id FROM beta", (), |row| row.get::<_, i64>(0))
                .unwrap(),
            [2]
        );
        assert!(
            database
                .query("SELECT id FROM shared", (), |row| row.get::<_, i64>(0))
                .is_err()
        );
        assert!(
            database
                .query("SELECT id FROM alternate", (), |row| row.get::<_, i64>(0))
                .is_err()
        );
        assert!(
            database
                .query("SELECT id FROM detour", (), |row| row.get::<_, i64>(0))
                .is_err()
        );
    }
}

#[test]
fn column_rename_and_stale_row_writes_converge_by_stable_column_identity() {
    let directory = tempfile::tempdir().unwrap();
    let server = server();
    let first = MultiliteConnection::open_with(
        directory.path().join("rename-column-first.sqlite"),
        OpenOptions::new().server(router(Arc::clone(&server))),
    )
    .unwrap();
    assert!(server.create_space(SpaceId(first.database_id().to_bytes())));
    let second = MultiliteConnection::open_with(
        directory.path().join("rename-column-second.sqlite"),
        OpenOptions::new()
            .invitation(first.replica_invitation())
            .server(router(Arc::clone(&server))),
    )
    .unwrap();

    first
        .execute(
            "CREATE TABLE notes (
                id INTEGER PRIMARY KEY,
                body TEXT NOT NULL UNIQUE
            )",
            (),
        )
        .unwrap();
    assert_eq!(first.push().unwrap(), PushOutcome::Drained);
    second.pull().unwrap();
    second.rebase().unwrap();

    first
        .execute("ALTER TABLE notes RENAME COLUMN body TO contents", ())
        .unwrap();
    second
        .execute("INSERT INTO notes (id, body) VALUES (2, 'stale')", ())
        .unwrap();
    assert_eq!(first.push().unwrap(), PushOutcome::Drained);
    assert_eq!(
        second.push().unwrap(),
        PushOutcome::Drained,
        "a name-only column change must not invalidate stale row writes"
    );

    first.pull().unwrap();
    second.pull().unwrap();
    first.rebase().unwrap();
    second.rebase().unwrap();
    for database in [&first, &second] {
        assert_eq!(
            database
                .query("SELECT id, contents FROM notes ORDER BY id", (), |row| Ok(
                    (row.get::<_, i64>(0)?, row.get::<_, String>(1)?)
                ),)
                .unwrap(),
            [(2, "stale".into())]
        );
    }
}

#[test]
fn renaming_distinct_columns_commutes_across_devices() {
    let directory = tempfile::tempdir().unwrap();
    let server = server();
    let first = MultiliteConnection::open_with(
        directory.path().join("rename-distinct-first.sqlite"),
        OpenOptions::new().server(router(Arc::clone(&server))),
    )
    .unwrap();
    assert!(server.create_space(SpaceId(first.database_id().to_bytes())));
    let second = MultiliteConnection::open_with(
        directory.path().join("rename-distinct-second.sqlite"),
        OpenOptions::new()
            .invitation(first.replica_invitation())
            .server(router(Arc::clone(&server))),
    )
    .unwrap();

    first
        .execute(
            "CREATE TABLE notes (
                id INTEGER PRIMARY KEY,
                body TEXT,
                title TEXT
            )",
            (),
        )
        .unwrap();
    assert_eq!(first.push().unwrap(), PushOutcome::Drained);
    second.pull().unwrap();
    second.rebase().unwrap();

    first
        .execute("ALTER TABLE notes RENAME COLUMN body TO contents", ())
        .unwrap();
    second
        .execute("ALTER TABLE notes RENAME COLUMN title TO headline", ())
        .unwrap();
    assert_eq!(first.push().unwrap(), PushOutcome::Drained);
    assert_eq!(
        second.push().unwrap(),
        PushOutcome::Drained,
        "separate column-name cells must admit independently"
    );

    first.pull().unwrap();
    second.pull().unwrap();
    first.rebase().unwrap();
    second.rebase().unwrap();
    for database in [&first, &second] {
        database
            .execute(
                "INSERT INTO notes (id, contents, headline)
                 VALUES (1, 'body', 'title')",
                (),
            )
            .unwrap();
        assert_eq!(
            database
                .query(
                    "SELECT contents, headline FROM notes WHERE id = 1",
                    (),
                    |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
                )
                .unwrap(),
            [("body".into(), "title".into())]
        );
        database
            .execute("DELETE FROM notes WHERE id = 1", ())
            .unwrap();
    }
}

#[test]
fn column_rename_fences_stale_name_bound_ddl_but_not_row_writes() {
    let directory = tempfile::tempdir().unwrap();
    let server = server();
    let first = MultiliteConnection::open_with(
        directory.path().join("rename-ddl-first.sqlite"),
        OpenOptions::new().server(router(Arc::clone(&server))),
    )
    .unwrap();
    assert!(server.create_space(SpaceId(first.database_id().to_bytes())));
    let second = MultiliteConnection::open_with(
        directory.path().join("rename-ddl-second.sqlite"),
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

    first
        .execute("ALTER TABLE notes RENAME COLUMN body TO contents", ())
        .unwrap();
    second
        .execute(
            "ALTER TABLE notes ADD COLUMN summary TEXT
             CHECK (body IS NULL OR length(body) > 0)",
            (),
        )
        .unwrap();
    assert_eq!(first.push().unwrap(), PushOutcome::Drained);
    let PushOutcome::Rejected(rejection) = second.push().unwrap() else {
        panic!("name-bound DDL compiled before a column rename unexpectedly drained")
    };
    second.rollback(&rejection).unwrap();
    assert_eq!(second.push().unwrap(), PushOutcome::Drained);
    second.pull().unwrap();
    second.rebase().unwrap();

    second
        .execute(
            "ALTER TABLE notes ADD COLUMN summary TEXT
             CHECK (contents IS NULL OR length(contents) > 0)",
            (),
        )
        .unwrap();
    assert_eq!(second.push().unwrap(), PushOutcome::Drained);
    first.pull().unwrap();
    first.rebase().unwrap();

    for database in [&first, &second] {
        database
            .execute(
                "INSERT INTO notes (id, contents, summary)
                 VALUES (1, 'renamed', 'valid')",
                (),
            )
            .unwrap();
        assert_eq!(
            database
                .query("SELECT contents, summary FROM notes", (), |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
                })
                .unwrap(),
            [("renamed".into(), "valid".into())]
        );
        database
            .execute("DELETE FROM notes WHERE id = 1", ())
            .unwrap();
    }
}

#[test]
fn unrelated_column_add_and_index_creation_commute_at_both_isolation_levels() {
    for isolation in [IsolationLevel::Snapshot, IsolationLevel::Serializable] {
        let directory = tempfile::tempdir().unwrap();
        let server = server();
        let first = MultiliteConnection::open_with(
            directory
                .path()
                .join(format!("add-index-{isolation:?}-first.sqlite")),
            OpenOptions::new()
                .isolation_level(isolation)
                .server(router(Arc::clone(&server))),
        )
        .unwrap();
        assert!(server.create_space(SpaceId(first.database_id().to_bytes())));
        let second = MultiliteConnection::open_with(
            directory
                .path()
                .join(format!("add-index-{isolation:?}-second.sqlite")),
            OpenOptions::new()
                .isolation_level(isolation)
                .invitation(first.replica_invitation())
                .server(router(Arc::clone(&server))),
        )
        .unwrap();

        first
            .execute(
                "CREATE TABLE notes (
                    id INTEGER PRIMARY KEY,
                    body TEXT NOT NULL
                )",
                (),
            )
            .unwrap();
        first
            .execute("INSERT INTO notes VALUES (1, 'one')", ())
            .unwrap();
        assert_eq!(first.push().unwrap(), PushOutcome::Drained);
        second.pull().unwrap();
        second.rebase().unwrap();

        first
            .execute(
                "ALTER TABLE notes ADD COLUMN summary TEXT DEFAULT 'summary'",
                (),
            )
            .unwrap();
        second
            .execute("CREATE INDEX notes_body ON notes (body)", ())
            .unwrap();

        // Admit the index first so applying the later column addition must
        // preserve a physical object absent from its original schema snapshot.
        assert_eq!(second.push().unwrap(), PushOutcome::Drained);
        assert_eq!(
            first.push().unwrap(),
            PushOutcome::Drained,
            "index DDL must depend on its referenced columns, not the whole table schema"
        );
        first.pull().unwrap();
        second.pull().unwrap();
        first.rebase().unwrap();
        second.rebase().unwrap();

        for database in [&first, &second] {
            assert_eq!(
                database
                    .query("SELECT id, body, summary FROM notes", (), |row| {
                        Ok((
                            row.get::<_, i64>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, String>(2)?,
                        ))
                    })
                    .unwrap(),
                [(1, "one".into(), "summary".into())]
            );
            assert_eq!(
                database
                    .query(
                        "SELECT name FROM sqlite_schema
                         WHERE type = 'index' AND name = 'notes_body'",
                        (),
                        |row| row.get::<_, String>(0),
                    )
                    .unwrap(),
                ["notes_body"]
            );
        }
    }
}

#[test]
fn column_rename_rejects_a_stale_index_bound_to_the_old_name() {
    for isolation in [IsolationLevel::Snapshot, IsolationLevel::Serializable] {
        let directory = tempfile::tempdir().unwrap();
        let server = server();
        let first = MultiliteConnection::open_with(
            directory
                .path()
                .join(format!("rename-index-{isolation:?}-first.sqlite")),
            OpenOptions::new()
                .isolation_level(isolation)
                .server(router(Arc::clone(&server))),
        )
        .unwrap();
        assert!(server.create_space(SpaceId(first.database_id().to_bytes())));
        let second = MultiliteConnection::open_with(
            directory
                .path()
                .join(format!("rename-index-{isolation:?}-second.sqlite")),
            OpenOptions::new()
                .isolation_level(isolation)
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

        first
            .execute("ALTER TABLE notes RENAME COLUMN body TO contents", ())
            .unwrap();
        second
            .execute(
                "CREATE INDEX stale_notes_body ON notes (lower(body)) WHERE body IS NOT NULL",
                (),
            )
            .unwrap();
        assert_eq!(first.push().unwrap(), PushOutcome::Drained);
        let PushOutcome::Rejected(rejection) = second.push().unwrap() else {
            panic!("an index compiled against a renamed column unexpectedly admitted")
        };
        second.rollback(&rejection).unwrap();
        assert_eq!(second.push().unwrap(), PushOutcome::Drained);
        second.pull().unwrap();
        second.rebase().unwrap();

        assert!(
            second
                .query("SELECT body FROM notes", (), |row| row.get::<_, String>(0))
                .is_err()
        );
        second
            .execute(
                "CREATE INDEX notes_contents ON notes (lower(contents))
                 WHERE contents IS NOT NULL",
                (),
            )
            .unwrap();
        assert_eq!(second.push().unwrap(), PushOutcome::Drained);
        first.pull().unwrap();
        first.rebase().unwrap();
        for database in [&first, &second] {
            assert_eq!(
                database
                    .query(
                        "SELECT name FROM sqlite_schema
                         WHERE type = 'index' AND name = 'notes_contents'",
                        (),
                        |row| row.get::<_, String>(0),
                    )
                    .unwrap(),
                ["notes_contents"]
            );
        }
    }
}

#[test]
fn index_creation_and_referenced_column_drop_conflict_in_either_order() {
    for (isolation, index_wins) in [
        (IsolationLevel::Snapshot, false),
        (IsolationLevel::Snapshot, true),
        (IsolationLevel::Serializable, false),
        (IsolationLevel::Serializable, true),
    ] {
        let directory = tempfile::tempdir().unwrap();
        let server = server();
        let first = MultiliteConnection::open_with(
            directory.path().join(format!(
                "index-drop-{isolation:?}-{index_wins}-first.sqlite"
            )),
            OpenOptions::new()
                .isolation_level(isolation)
                .server(router(Arc::clone(&server))),
        )
        .unwrap();
        assert!(server.create_space(SpaceId(first.database_id().to_bytes())));
        let second = MultiliteConnection::open_with(
            directory.path().join(format!(
                "index-drop-{isolation:?}-{index_wins}-second.sqlite"
            )),
            OpenOptions::new()
                .isolation_level(isolation)
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

        first
            .execute(
                "CREATE INDEX notes_body ON notes (lower(body)) WHERE body IS NOT NULL",
                (),
            )
            .unwrap();
        second
            .execute("ALTER TABLE notes DROP COLUMN body", ())
            .unwrap();

        let (winner, loser) = if index_wins {
            (&first, &second)
        } else {
            (&second, &first)
        };
        assert_eq!(winner.push().unwrap(), PushOutcome::Drained);
        let PushOutcome::Rejected(rejection) = loser.push().unwrap() else {
            panic!("an index and removal of its referenced column both admitted")
        };
        loser.rollback(&rejection).unwrap();
        assert_eq!(loser.push().unwrap(), PushOutcome::Drained);
        first.pull().unwrap();
        second.pull().unwrap();
        first.rebase().unwrap();
        second.rebase().unwrap();

        for database in [&first, &second] {
            if index_wins {
                database
                    .execute("INSERT INTO notes VALUES (1, 'indexed')", ())
                    .unwrap();
                assert_eq!(
                    database
                        .query("SELECT body FROM notes", (), |row| row.get::<_, String>(0))
                        .unwrap(),
                    ["indexed"]
                );
                database.execute("DELETE FROM notes", ()).unwrap();
            } else {
                assert!(
                    database
                        .query("SELECT body FROM notes", (), |row| row.get::<_, String>(0))
                        .is_err()
                );
                assert!(
                    database
                        .query(
                            "SELECT name FROM sqlite_schema
                             WHERE type = 'index' AND name = 'notes_body'",
                            (),
                            |row| row.get::<_, String>(0),
                        )
                        .unwrap()
                        .is_empty()
                );
            }
        }
    }
}

#[test]
fn rejected_drop_column_restores_local_sidecar_after_restart() {
    for isolation in [IsolationLevel::Snapshot, IsolationLevel::Serializable] {
        let directory = tempfile::tempdir().unwrap();
        let server = server();
        let first_path = directory
            .path()
            .join(format!("drop-sidecar-{isolation:?}-first.sqlite"));
        let second_path = directory
            .path()
            .join(format!("drop-sidecar-{isolation:?}-second.sqlite"));
        let first = MultiliteConnection::open_with(
            &first_path,
            OpenOptions::new()
                .isolation_level(isolation)
                .server(router(Arc::clone(&server))),
        )
        .unwrap();
        assert!(server.create_space(SpaceId(first.database_id().to_bytes())));
        let invitation = first.replica_invitation();
        let second = MultiliteConnection::open_with(
            &second_path,
            OpenOptions::new()
                .isolation_level(isolation)
                .invitation(invitation.clone())
                .server(router(Arc::clone(&server))),
        )
        .unwrap();

        first
            .execute(
                "CREATE TABLE notes (
                    id INTEGER PRIMARY KEY,
                    body TEXT,
                    detail BLOB
                )",
                (),
            )
            .unwrap();
        first
            .execute(
                "INSERT INTO notes VALUES (1, 'one', x'0001ff'), (2, 'two', NULL)",
                (),
            )
            .unwrap();
        assert_eq!(first.push().unwrap(), PushOutcome::Drained);
        second.pull().unwrap();
        second.rebase().unwrap();

        first
            .execute("CREATE INDEX notes_detail ON notes (detail)", ())
            .unwrap();
        second
            .execute("ALTER TABLE notes DROP COLUMN detail", ())
            .unwrap();
        drop(second);

        let second = MultiliteConnection::open_with(
            &second_path,
            OpenOptions::new()
                .isolation_level(isolation)
                .invitation(invitation.clone())
                .server(router(Arc::clone(&server))),
        )
        .unwrap();
        assert_eq!(first.push().unwrap(), PushOutcome::Drained);
        let PushOutcome::Rejected(rejection) = second.push().unwrap() else {
            panic!("a stale DROP COLUMN ignored the admitted index dependency")
        };
        drop(second);

        let second = MultiliteConnection::open_with(
            &second_path,
            OpenOptions::new()
                .isolation_level(isolation)
                .invitation(invitation.clone())
                .server(router(Arc::clone(&server))),
        )
        .unwrap();
        second.rollback(&rejection).unwrap();
        assert_eq!(
            second
                .query("SELECT id, hex(detail) FROM notes ORDER BY id", (), |row| {
                    Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
                })
                .unwrap(),
            [(1, "0001FF".into()), (2, "".into())]
        );
        assert_eq!(second.push().unwrap(), PushOutcome::Drained);

        first.pull().unwrap();
        second.pull().unwrap();
        first.rebase().unwrap();
        second.rebase().unwrap();
        for database in [&first, &second] {
            assert_eq!(
                database
                    .query(
                        "SELECT id, body, hex(detail) FROM notes ORDER BY id",
                        (),
                        |row| {
                            Ok((
                                row.get::<_, i64>(0)?,
                                row.get::<_, String>(1)?,
                                row.get::<_, String>(2)?,
                            ))
                        },
                    )
                    .unwrap(),
                [
                    (1, "one".into(), "0001FF".into()),
                    (2, "two".into(), "".into()),
                ]
            );
            assert_eq!(index_names(database), ["notes_detail"]);
        }
    }
}

#[test]
fn admitted_check_dependency_rejects_a_stale_referenced_column_drop() {
    for isolation in [IsolationLevel::Snapshot, IsolationLevel::Serializable] {
        let directory = tempfile::tempdir().unwrap();
        let server = server();
        let first = MultiliteConnection::open_with(
            directory
                .path()
                .join(format!("check-drop-{isolation:?}-first.sqlite")),
            OpenOptions::new()
                .isolation_level(isolation)
                .server(router(Arc::clone(&server))),
        )
        .unwrap();
        assert!(server.create_space(SpaceId(first.database_id().to_bytes())));
        let second = MultiliteConnection::open_with(
            directory
                .path()
                .join(format!("check-drop-{isolation:?}-second.sqlite")),
            OpenOptions::new()
                .isolation_level(isolation)
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

        first
            .execute(
                "ALTER TABLE notes ADD COLUMN summary TEXT
                 CHECK (body IS NULL OR length(body) > 0)",
                (),
            )
            .unwrap();
        second
            .execute("ALTER TABLE notes DROP COLUMN body", ())
            .unwrap();
        assert_eq!(first.push().unwrap(), PushOutcome::Drained);
        let PushOutcome::Rejected(rejection) = second.push().unwrap() else {
            panic!("a stale drop ignored an admitted CHECK dependency")
        };
        second.rollback(&rejection).unwrap();
        assert_eq!(second.push().unwrap(), PushOutcome::Drained);
        first.pull().unwrap();
        second.pull().unwrap();
        first.rebase().unwrap();
        second.rebase().unwrap();

        for database in [&first, &second] {
            database
                .execute("INSERT INTO notes VALUES (1, 'valid', 'summary')", ())
                .unwrap();
            assert!(
                database
                    .execute("INSERT INTO notes VALUES (2, '', 'invalid')", ())
                    .is_err()
            );
            database.execute("DELETE FROM notes", ()).unwrap();
        }
    }
}

#[test]
fn validated_add_column_checks_conflict_with_intervening_rows_in_either_order() {
    for (isolation, check_wins) in [
        (IsolationLevel::Snapshot, false),
        (IsolationLevel::Snapshot, true),
        (IsolationLevel::Serializable, false),
        (IsolationLevel::Serializable, true),
    ] {
        let directory = tempfile::tempdir().unwrap();
        let server = server();
        let first = MultiliteConnection::open_with(
            directory.path().join(format!(
                "add-check-row-{isolation:?}-{check_wins}-first.sqlite"
            )),
            OpenOptions::new()
                .isolation_level(isolation)
                .server(router(Arc::clone(&server))),
        )
        .unwrap();
        assert!(server.create_space(SpaceId(first.database_id().to_bytes())));
        let second = MultiliteConnection::open_with(
            directory.path().join(format!(
                "add-check-row-{isolation:?}-{check_wins}-second.sqlite"
            )),
            OpenOptions::new()
                .isolation_level(isolation)
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

        first
            .execute(
                "ALTER TABLE notes ADD COLUMN summary TEXT
                 CHECK (body IS NULL OR body <> 'invalid')",
                (),
            )
            .unwrap();
        second
            .execute("INSERT INTO notes (id, body) VALUES (1, 'invalid')", ())
            .unwrap();

        if check_wins {
            assert_eq!(first.push().unwrap(), PushOutcome::Drained);
            let PushOutcome::Rejected(rejection) = second.push().unwrap() else {
                panic!("a stale row bypassed an admitted CHECK contract")
            };
            second.rollback(&rejection).unwrap();
            assert_eq!(second.push().unwrap(), PushOutcome::Drained);
        } else {
            assert_eq!(second.push().unwrap(), PushOutcome::Drained);
            let PushOutcome::Rejected(rejection) = first.push().unwrap() else {
                panic!("CHECK DDL ignored a row admitted after its validated snapshot")
            };
            first.rollback(&rejection).unwrap();
            assert_eq!(first.push().unwrap(), PushOutcome::Drained);
        }

        first.pull().unwrap();
        second.pull().unwrap();
        first.rebase().unwrap();
        second.rebase().unwrap();

        for database in [&first, &second] {
            if check_wins {
                assert!(
                    database
                        .query("SELECT id, body, summary FROM notes", (), |row| Ok((
                            row.get::<_, i64>(0)?,
                            row.get::<_, Option<String>>(1)?,
                            row.get::<_, Option<String>>(2)?,
                        )),)
                        .unwrap()
                        .is_empty()
                );
            } else {
                assert_eq!(
                    database
                        .query("SELECT id, body FROM notes", (), |row| {
                            Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
                        })
                        .unwrap(),
                    [(1, "invalid".into())]
                );
                assert!(
                    database
                        .query("SELECT summary FROM notes", (), |row| {
                            row.get::<_, Option<String>>(0)
                        })
                        .is_err()
                );
            }
        }
    }
}

#[test]
fn disjoint_add_and_drop_columns_commute_at_both_isolation_levels() {
    for isolation in [IsolationLevel::Snapshot, IsolationLevel::Serializable] {
        let directory = tempfile::tempdir().unwrap();
        let server = server();
        let first = MultiliteConnection::open_with(
            directory
                .path()
                .join(format!("alter-columns-{isolation:?}-first.sqlite")),
            OpenOptions::new()
                .isolation_level(isolation)
                .server(router(Arc::clone(&server))),
        )
        .unwrap();
        assert!(server.create_space(SpaceId(first.database_id().to_bytes())));
        let second = MultiliteConnection::open_with(
            directory
                .path()
                .join(format!("alter-columns-{isolation:?}-second.sqlite")),
            OpenOptions::new()
                .isolation_level(isolation)
                .invitation(first.replica_invitation())
                .server(router(Arc::clone(&server))),
        )
        .unwrap();

        first
            .execute(
                "CREATE TABLE notes (
                    id INTEGER PRIMARY KEY,
                    body TEXT NOT NULL,
                    detail TEXT
                )",
                (),
            )
            .unwrap();
        first
            .execute(
                "INSERT INTO notes VALUES (1, 'body', 'must survive rollback')",
                (),
            )
            .unwrap();
        assert_eq!(first.push().unwrap(), PushOutcome::Drained);
        second.pull().unwrap();
        second.rebase().unwrap();

        first
            .execute(
                "ALTER TABLE notes ADD COLUMN remote_value TEXT DEFAULT 'remote'",
                (),
            )
            .unwrap();
        second
            .execute("ALTER TABLE notes DROP COLUMN detail", ())
            .unwrap();
        assert_eq!(first.push().unwrap(), PushOutcome::Drained);
        assert_eq!(
            second.push().unwrap(),
            PushOutcome::Drained,
            "independent schema components must admit together"
        );
        first.pull().unwrap();
        second.pull().unwrap();
        first.rebase().unwrap();
        second.rebase().unwrap();
        assert_eq!(
            second
                .query("SELECT body, remote_value FROM notes", (), |row| Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?
                )),)
                .unwrap(),
            [("body".into(), "remote".into())]
        );
        assert!(
            second
                .query("SELECT detail FROM notes", (), |row| row
                    .get::<_, String>(0))
                .is_err()
        );

        first
            .execute("ALTER TABLE notes RENAME COLUMN body TO contents", ())
            .unwrap();
        second
            .execute(
                "ALTER TABLE notes ADD COLUMN local_tag TEXT DEFAULT 'tag'",
                (),
            )
            .unwrap();
        assert_eq!(first.push().unwrap(), PushOutcome::Drained);
        assert_eq!(
            second.push().unwrap(),
            PushOutcome::Drained,
            "an unrelated rename and addition must commute"
        );
        first.pull().unwrap();
        second.pull().unwrap();
        first.rebase().unwrap();
        second.rebase().unwrap();

        first
            .execute("ALTER TABLE notes DROP COLUMN remote_value", ())
            .unwrap();
        assert_eq!(first.push().unwrap(), PushOutcome::Drained);
        second.pull().unwrap();
        second.rebase().unwrap();

        for database in [&first, &second] {
            assert_eq!(
                database
                    .query("SELECT id, contents, local_tag FROM notes", (), |row| {
                        Ok((
                            row.get::<_, i64>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, String>(2)?,
                        ))
                    })
                    .unwrap(),
                [(1, "body".into(), "tag".into())]
            );
            assert!(
                database
                    .query("SELECT remote_value FROM notes", (), |row| {
                        row.get::<_, String>(0)
                    })
                    .is_err()
            );
        }
    }
}

#[test]
fn concurrent_add_columns_converge_to_one_physical_order() {
    for isolation in [IsolationLevel::Snapshot, IsolationLevel::Serializable] {
        let directory = tempfile::tempdir().unwrap();
        let server = server();
        let first_path = directory
            .path()
            .join(format!("two-adds-{isolation:?}-first.sqlite"));
        let second_path = directory
            .path()
            .join(format!("two-adds-{isolation:?}-second.sqlite"));
        let first = MultiliteConnection::open_with(
            &first_path,
            OpenOptions::new()
                .isolation_level(isolation)
                .server(router(Arc::clone(&server))),
        )
        .unwrap();
        assert!(server.create_space(SpaceId(first.database_id().to_bytes())));
        let second = MultiliteConnection::open_with(
            &second_path,
            OpenOptions::new()
                .isolation_level(isolation)
                .invitation(first.replica_invitation())
                .server(router(Arc::clone(&server))),
        )
        .unwrap();

        first
            .execute(
                "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT NOT NULL)",
                (),
            )
            .unwrap();
        first
            .execute("INSERT INTO notes VALUES (1, 'original')", ())
            .unwrap();
        assert_eq!(first.push().unwrap(), PushOutcome::Drained);
        second.pull().unwrap();
        second.rebase().unwrap();

        first
            .execute(
                "ALTER TABLE notes ADD COLUMN alpha TEXT DEFAULT 'alpha'",
                (),
            )
            .unwrap();
        second
            .execute("ALTER TABLE notes ADD COLUMN beta TEXT DEFAULT 'beta'", ())
            .unwrap();
        assert_eq!(first.push().unwrap(), PushOutcome::Drained);
        assert_eq!(second.push().unwrap(), PushOutcome::Drained);
        first.pull().unwrap();
        second.pull().unwrap();
        first.rebase().unwrap();
        second.rebase().unwrap();

        let physical_row = |database: &MultiliteConnection<_>| {
            database
                .query("SELECT * FROM notes", (), |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                    ))
                })
                .unwrap()
        };
        let first_physical = physical_row(&first);
        assert_eq!(physical_row(&second), first_physical);
        assert_eq!(first_physical[0].0, 1);
        assert_eq!(first_physical[0].1, "original");
        assert!(["alpha", "beta"].contains(&first_physical[0].2.as_str()));
        assert!(["alpha", "beta"].contains(&first_physical[0].3.as_str()));
        assert_ne!(first_physical[0].2, first_physical[0].3);
        for database in [&first, &second] {
            assert_eq!(
                database
                    .query("SELECT id, body, alpha, beta FROM notes", (), |row| {
                        Ok((
                            row.get::<_, i64>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, String>(2)?,
                            row.get::<_, String>(3)?,
                        ))
                    })
                    .unwrap(),
                [(1, "original".into(), "alpha".into(), "beta".into())]
            );
        }

        drop(first);
        drop(second);
        let first = MultiliteConnection::open_with(
            &first_path,
            OpenOptions::new()
                .isolation_level(isolation)
                .server(router(Arc::clone(&server))),
        )
        .unwrap();
        let second = MultiliteConnection::open_with(
            &second_path,
            OpenOptions::new()
                .isolation_level(isolation)
                .server(router(Arc::clone(&server))),
        )
        .unwrap();
        assert_eq!(physical_row(&first), physical_row(&second));
    }
}

#[test]
fn rejected_general_drop_restores_schema_rows_and_incoming_foreign_keys() {
    let directory = tempfile::tempdir().unwrap();
    let server = server();
    let first = MultiliteConnection::open_with(
        directory.path().join("drop-repair-first.sqlite"),
        OpenOptions::new().server(router(Arc::clone(&server))),
    )
    .unwrap();
    assert!(server.create_space(SpaceId(first.database_id().to_bytes())));
    let second = MultiliteConnection::open_with(
        directory.path().join("drop-repair-second.sqlite"),
        OpenOptions::new()
            .invitation(first.replica_invitation())
            .server(router(Arc::clone(&server))),
    )
    .unwrap();

    first
        .update(|transaction| {
            transaction.execute(
                "CREATE TABLE parents (
                    id INTEGER PRIMARY KEY,
                    middle TEXT CONSTRAINT middle_nn NOT NULL DEFAULT 'seed'
                        CONSTRAINT middle_nonempty CHECK (length(middle) > 0),
                    tail TEXT
                )",
                (),
            )?;
            transaction.execute(
                "CREATE TABLE children (
                    id INTEGER PRIMARY KEY,
                    parent_id INTEGER REFERENCES parents(id)
                )",
                (),
            )?;
            transaction.execute("INSERT INTO parents VALUES (1, 'saved', 'tail')", ())?;
            transaction.execute("INSERT INTO children VALUES (7, 1)", ())?;
            Ok(())
        })
        .unwrap();
    assert_eq!(first.push().unwrap(), PushOutcome::Drained);
    second.pull().unwrap();
    second.rebase().unwrap();

    first
        .execute("ALTER TABLE parents RENAME COLUMN middle TO canonical", ())
        .unwrap();
    second
        .execute("ALTER TABLE parents DROP COLUMN middle", ())
        .unwrap();
    assert_eq!(first.push().unwrap(), PushOutcome::Drained);
    let PushOutcome::Rejected(rejection) = second.push().unwrap() else {
        panic!("DROP and RENAME of the same stable column both admitted")
    };
    second.rollback(&rejection).unwrap();
    assert_eq!(
        second
            .query(
                "SELECT parents.middle, children.parent_id
                 FROM parents JOIN children ON children.parent_id = parents.id",
                (),
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
            )
            .unwrap(),
        [("saved".into(), 1)]
    );
    assert_eq!(second.push().unwrap(), PushOutcome::Drained);
    second.pull().unwrap();
    second.rebase().unwrap();

    for database in [&first, &second] {
        assert_eq!(
            database
                .query(
                    "SELECT parents.canonical, children.parent_id
                     FROM parents JOIN children ON children.parent_id = parents.id",
                    (),
                    |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
                )
                .unwrap(),
            [("saved".into(), 1)]
        );
    }
    first
        .execute("INSERT INTO parents (id, tail) VALUES (2, 'defaulted')", ())
        .unwrap();
    assert_eq!(
        first
            .query("SELECT canonical FROM parents WHERE id = 2", (), |row| {
                row.get::<_, String>(0)
            })
            .unwrap(),
        ["seed"]
    );
    assert!(
        first
            .execute("INSERT INTO parents VALUES (3, '', 'invalid')", ())
            .is_err()
    );
}

#[test]
fn compatible_column_evolution_commutes_with_stale_dml_at_both_isolation_levels() {
    for isolation in [IsolationLevel::Snapshot, IsolationLevel::Serializable] {
        let directory = tempfile::tempdir().unwrap();
        let server = server();
        let first = MultiliteConnection::open_with(
            directory
                .path()
                .join(format!("compatible-column-{isolation:?}-first.sqlite")),
            OpenOptions::new()
                .isolation_level(isolation)
                .server(router(Arc::clone(&server))),
        )
        .unwrap();
        assert!(server.create_space(SpaceId(first.database_id().to_bytes())));
        let second = MultiliteConnection::open_with(
            directory
                .path()
                .join(format!("compatible-column-{isolation:?}-second.sqlite")),
            OpenOptions::new()
                .isolation_level(isolation)
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

        first
            .execute(
                "ALTER TABLE notes
                 ADD COLUMN summary TEXT DEFAULT 'new'",
                (),
            )
            .unwrap();
        second
            .execute(
                "INSERT INTO notes (id, body) VALUES (1, 'stale insert')",
                (),
            )
            .unwrap();
        assert_eq!(first.push().unwrap(), PushOutcome::Drained);
        assert_eq!(
            second.push().unwrap(),
            PushOutcome::Drained,
            "a defaulted column must project a stale insert"
        );
        first.pull().unwrap();
        second.pull().unwrap();
        first.rebase().unwrap();
        second.rebase().unwrap();

        first
            .execute("UPDATE notes SET body = 'stale update' WHERE id = 1", ())
            .unwrap();
        second
            .execute("ALTER TABLE notes DROP COLUMN summary", ())
            .unwrap();
        assert_eq!(first.push().unwrap(), PushOutcome::Drained);
        assert_eq!(
            second.push().unwrap(),
            PushOutcome::Drained,
            "dropping a non-key column must commute with a stale row update"
        );
        first.pull().unwrap();
        second.pull().unwrap();
        first.rebase().unwrap();
        second.rebase().unwrap();

        for database in [&first, &second] {
            assert_eq!(
                database
                    .query("SELECT id, body FROM notes", (), |row| {
                        Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
                    })
                    .unwrap(),
                [(1, "stale update".into())]
            );
            assert!(
                database
                    .query("SELECT summary FROM notes", (), |row| {
                        row.get::<_, String>(0)
                    })
                    .is_err()
            );
        }
    }
}

#[test]
fn table_rename_and_stale_foreign_key_writes_converge_across_replicas() {
    let directory = tempfile::tempdir().unwrap();
    let server = server();
    let first = MultiliteConnection::open_with(
        directory.path().join("rename-fk-first.sqlite"),
        OpenOptions::new().server(router(Arc::clone(&server))),
    )
    .unwrap();
    assert!(server.create_space(SpaceId(first.database_id().to_bytes())));
    let second = MultiliteConnection::open_with(
        directory.path().join("rename-fk-second.sqlite"),
        OpenOptions::new()
            .invitation(first.replica_invitation())
            .server(router(Arc::clone(&server))),
    )
    .unwrap();

    first
        .update(|transaction| {
            transaction.execute(
                "CREATE TABLE parents (
                    id INTEGER PRIMARY KEY,
                    code TEXT UNIQUE
                )",
                (),
            )?;
            transaction.execute(
                "CREATE INDEX parents_code_lookup
                 ON parents (lower(code), code COLLATE NOCASE DESC)",
                (),
            )?;
            transaction.execute(
                "CREATE TABLE children (
                    id INTEGER PRIMARY KEY,
                    parent_id INTEGER REFERENCES parents(id)
                )",
                (),
            )?;
            transaction.execute("INSERT INTO parents VALUES (1, 'one')", ())?;
            transaction.execute("INSERT INTO children VALUES (1, 1)", ())?;
            Ok(())
        })
        .unwrap();
    assert_eq!(first.push().unwrap(), PushOutcome::Drained);
    second.pull().unwrap();
    second.rebase().unwrap();

    first
        .execute("ALTER TABLE parents RENAME TO accounts", ())
        .unwrap();
    assert_eq!(first.push().unwrap(), PushOutcome::Drained);
    second
        .update(|transaction| {
            transaction.execute("INSERT INTO parents VALUES (2, 'two')", ())?;
            transaction.execute("INSERT INTO children VALUES (2, 2)", ())?;
            transaction.execute("CREATE INDEX stale_code_lookup ON parents (code)", ())?;
            Ok(())
        })
        .unwrap();
    assert_eq!(
        second.push().unwrap(),
        PushOutcome::Drained,
        "parent rename must not invalidate stale parent or child row writes"
    );

    first.pull().unwrap();
    second.pull().unwrap();
    first.rebase().unwrap();
    second.rebase().unwrap();
    for database in [&first, &second] {
        assert_eq!(
            database
                .query("SELECT id, code FROM accounts ORDER BY id", (), |row| {
                    Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
                })
                .unwrap(),
            [(1, "one".into()), (2, "two".into())]
        );
        assert_eq!(
            database
                .query(
                    "SELECT id, parent_id FROM children ORDER BY id",
                    (),
                    |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
                )
                .unwrap(),
            [(1, 1), (2, 2)]
        );
        assert_eq!(
            database
                .query(
                    "SELECT name, tbl_name FROM sqlite_schema
                     WHERE type = 'index'
                       AND name IN ('parents_code_lookup', 'stale_code_lookup')
                     ORDER BY name",
                    (),
                    |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
                )
                .unwrap(),
            [
                ("parents_code_lookup".into(), "accounts".into()),
                ("stale_code_lookup".into(), "accounts".into()),
            ]
        );
    }
}

#[test]
fn stale_row_operations_follow_identity_after_the_old_name_is_reused() {
    let directory = tempfile::tempdir().unwrap();
    let server = server();
    let first = MultiliteConnection::open_with(
        directory.path().join("rename-reuse-first.sqlite"),
        OpenOptions::new().server(router(Arc::clone(&server))),
    )
    .unwrap();
    assert!(server.create_space(SpaceId(first.database_id().to_bytes())));
    let second = MultiliteConnection::open_with(
        directory.path().join("rename-reuse-second.sqlite"),
        OpenOptions::new()
            .invitation(first.replica_invitation())
            .server(router(Arc::clone(&server))),
    )
    .unwrap();

    first
        .update(|transaction| {
            transaction.execute("CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)", ())?;
            transaction.execute("INSERT INTO notes VALUES (1, 'one'), (2, 'two')", ())?;
            Ok(())
        })
        .unwrap();
    assert_eq!(first.push().unwrap(), PushOutcome::Drained);
    second.pull().unwrap();
    second.rebase().unwrap();

    second
        .update(|transaction| {
            transaction.execute("UPDATE notes SET body = 'updated' WHERE id = 1", ())?;
            transaction.execute("DELETE FROM notes WHERE id = 2", ())?;
            transaction.execute("INSERT INTO notes VALUES (3, 'three')", ())?;
            Ok(())
        })
        .unwrap();
    first
        .execute("ALTER TABLE notes RENAME TO archived_notes", ())
        .unwrap();
    assert_eq!(first.push().unwrap(), PushOutcome::Drained);
    first
        .execute("CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)", ())
        .unwrap();
    assert_eq!(first.push().unwrap(), PushOutcome::Drained);
    assert_eq!(
        second.push().unwrap(),
        PushOutcome::Drained,
        "stale DML should remain addressed to the original table UUID"
    );

    first.pull().unwrap();
    second.pull().unwrap();
    first.rebase().unwrap();
    second.rebase().unwrap();
    for database in [&first, &second] {
        assert_eq!(
            database
                .query(
                    "SELECT id, body FROM archived_notes ORDER BY id",
                    (),
                    |row| Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?)),
                )
                .unwrap(),
            [(1, "updated".into()), (3, "three".into())]
        );
        assert!(
            database
                .query("SELECT id FROM notes", (), |row| row.get::<_, i64>(0))
                .unwrap()
                .is_empty(),
            "reusing the old name must not retarget stale DML"
        );
    }
}

#[test]
fn parent_rename_and_stale_incoming_foreign_key_converge_in_either_order() {
    let directory = tempfile::tempdir().unwrap();
    for rename_first in [true, false] {
        let label = if rename_first {
            "rename-first"
        } else {
            "relationship-first"
        };
        let server = server();
        let first = MultiliteConnection::open_with(
            directory.path().join(format!("{label}-first.sqlite")),
            OpenOptions::new().server(router(Arc::clone(&server))),
        )
        .unwrap();
        assert!(server.create_space(SpaceId(first.database_id().to_bytes())));
        let second = MultiliteConnection::open_with(
            directory.path().join(format!("{label}-second.sqlite")),
            OpenOptions::new()
                .invitation(first.replica_invitation())
                .server(router(Arc::clone(&server))),
        )
        .unwrap();
        first
            .execute("CREATE TABLE parents (id INTEGER PRIMARY KEY)", ())
            .unwrap();
        assert_eq!(first.push().unwrap(), PushOutcome::Drained);
        second.pull().unwrap();
        second.rebase().unwrap();

        first
            .execute("ALTER TABLE parents RENAME TO accounts", ())
            .unwrap();
        second
            .execute(
                "CREATE TABLE children (
                    id INTEGER PRIMARY KEY,
                    parent_id INTEGER REFERENCES parents(id)
                )",
                (),
            )
            .unwrap();

        if rename_first {
            assert_eq!(first.push().unwrap(), PushOutcome::Drained);
            assert_eq!(
                second.push().unwrap(),
                PushOutcome::Drained,
                "stable parent identity should survive an admitted rename"
            );
        } else {
            assert_eq!(second.push().unwrap(), PushOutcome::Drained);
            assert_eq!(
                first.push().unwrap(),
                PushOutcome::Drained,
                "a rename should not invalidate an incoming stable relationship"
            );
        }
        first.pull().unwrap();
        second.pull().unwrap();
        first.rebase().unwrap();
        second.rebase().unwrap();

        for database in [&first, &second] {
            let names = database
                .query(
                    "SELECT name FROM sqlite_schema
                     WHERE type = 'table' AND name NOT LIKE '__multilite__%'
                     ORDER BY name",
                    (),
                    |row| row.get::<_, String>(0),
                )
                .unwrap();
            assert_eq!(names, ["accounts", "children"], "{label}");
            assert!(
                database
                    .query(
                        "SELECT sql FROM sqlite_schema
                         WHERE type = 'table' AND name = 'children'",
                        (),
                        |row| row.get::<_, String>(0),
                    )
                    .unwrap()
                    .into_iter()
                    .all(|sql| sql.contains("accounts")),
                "{label}: physical foreign-key SQL did not follow the current parent name"
            );
        }
    }
}

#[test]
fn composite_without_rowid_rows_converge_and_repair_conflicts() {
    let directory = tempfile::tempdir().unwrap();
    let server = server();
    let first = MultiliteConnection::open_with(
        directory.path().join("composite-first.sqlite"),
        OpenOptions::new().server(router(Arc::clone(&server))),
    )
    .unwrap();
    assert!(server.create_space(SpaceId(first.database_id().to_bytes())));
    let second = MultiliteConnection::open_with(
        directory.path().join("composite-second.sqlite"),
        OpenOptions::new()
            .invitation(first.replica_invitation())
            .server(router(Arc::clone(&server))),
    )
    .unwrap();

    first
        .execute(
            "CREATE TABLE memberships (
                tenant TEXT,
                member INTEGER,
                body TEXT NOT NULL,
                PRIMARY KEY (member, tenant)
            ) WITHOUT ROWID",
            (),
        )
        .unwrap();
    assert_eq!(first.push().unwrap(), PushOutcome::Drained);
    second.pull().unwrap();
    second.rebase().unwrap();

    first
        .execute("INSERT INTO memberships VALUES ('north', 1, 'first')", ())
        .unwrap();
    second
        .execute("INSERT INTO memberships VALUES ('south', 1, 'second')", ())
        .unwrap();
    assert_eq!(first.push().unwrap(), PushOutcome::Drained);
    assert_eq!(second.push().unwrap(), PushOutcome::Drained);
    first.pull().unwrap();
    second.pull().unwrap();
    first.rebase().unwrap();
    second.rebase().unwrap();

    first
        .execute("INSERT INTO memberships VALUES ('shared', 7, 'winner')", ())
        .unwrap();
    second
        .execute("INSERT INTO memberships VALUES ('shared', 7, 'loser')", ())
        .unwrap();
    assert_eq!(first.push().unwrap(), PushOutcome::Drained);
    let PushOutcome::Rejected(rejection) = second.push().unwrap() else {
        panic!("same composite primary key was not rejected")
    };
    second.rollback(&rejection).unwrap();
    assert_eq!(second.push().unwrap(), PushOutcome::Drained);

    first.pull().unwrap();
    second.pull().unwrap();
    first.rebase().unwrap();
    second.rebase().unwrap();

    let expected = [
        ("north".into(), 1, "first".into()),
        ("south".into(), 1, "second".into()),
        ("shared".into(), 7, "winner".into()),
    ];
    assert_eq!(membership_rows(&first), expected);
    assert_eq!(membership_rows(&second), expected);
    assert_eq!(tables(&first), tables(&second));
}

#[test]
fn rejected_drop_table_restores_composite_rows_indexes_across_restart_and_converges() {
    let directory = tempfile::tempdir().unwrap();
    let server = server();
    let second_path = directory.path().join("drop-table-second.sqlite");
    let first = MultiliteConnection::open_with(
        directory.path().join("drop-table-first.sqlite"),
        OpenOptions::new().server(router(Arc::clone(&server))),
    )
    .unwrap();
    assert!(server.create_space(SpaceId(first.database_id().to_bytes())));
    let second = MultiliteConnection::open_with(
        &second_path,
        OpenOptions::new()
            .invitation(first.replica_invitation())
            .server(router(Arc::clone(&server))),
    )
    .unwrap();

    first
        .update(|transaction| {
            transaction.execute(
                "CREATE TABLE inventory (
                    tenant TEXT,
                    sku INTEGER,
                    body ANY,
                    PRIMARY KEY (tenant, sku)
                ) WITHOUT ROWID",
                (),
            )?;
            transaction.execute("CREATE INDEX inventory_body ON inventory(body)", ())?;
            transaction.execute(
                "INSERT INTO inventory VALUES
                    ('north', 1, X'CAFE'),
                    ('south', 2, NULL)",
                (),
            )?;
            Ok(())
        })
        .unwrap();
    assert_eq!(first.push().unwrap(), PushOutcome::Drained);
    second.pull().unwrap();
    second.rebase().unwrap();

    first
        .execute("INSERT INTO inventory VALUES ('west', 3, 7.5)", ())
        .unwrap();
    second.execute("DROP TABLE inventory", ()).unwrap();
    assert_eq!(first.push().unwrap(), PushOutcome::Drained);
    let PushOutcome::Rejected(rejection) = second.push().unwrap() else {
        panic!("stale DROP TABLE was not rejected")
    };
    drop(second);

    let second = MultiliteConnection::open_with(
        &second_path,
        OpenOptions::new().server(router(Arc::clone(&server))),
    )
    .unwrap();
    assert!(
        second
            .query("SELECT * FROM inventory", (), |_| Ok(()))
            .is_err()
    );
    second.rollback(&rejection).unwrap();
    assert_eq!(
        second
            .query(
                "SELECT tenant, sku, typeof(body), body IS NULL
                 FROM inventory ORDER BY tenant, sku",
                (),
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, bool>(3)?,
                    ))
                },
            )
            .unwrap(),
        [
            ("north".into(), 1, "blob".into(), false),
            ("south".into(), 2, "null".into(), true),
        ]
    );
    assert_eq!(index_names(&second), ["inventory_body"]);
    assert_eq!(second.push().unwrap(), PushOutcome::Drained);

    first.pull().unwrap();
    second.pull().unwrap();
    first.rebase().unwrap();
    second.rebase().unwrap();
    for database in [&first, &second] {
        assert_eq!(
            database
                .query(
                    "SELECT tenant, sku FROM inventory ORDER BY tenant, sku",
                    (),
                    |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?)),
                )
                .unwrap(),
            [("north".into(), 1), ("south".into(), 2), ("west".into(), 3),]
        );
        assert_eq!(index_names(database), ["inventory_body"]);
    }
}

#[test]
fn rejected_drop_table_restores_complete_schema_behavior_after_restart() {
    for isolation in [IsolationLevel::Snapshot, IsolationLevel::Serializable] {
        let directory = tempfile::tempdir().unwrap();
        let server = server();
        let second_path = directory
            .path()
            .join(format!("drop-table-schema-second-{isolation:?}.sqlite"));
        let first = MultiliteConnection::open_with(
            directory
                .path()
                .join(format!("drop-table-schema-first-{isolation:?}.sqlite")),
            OpenOptions::new()
                .server(router(Arc::clone(&server)))
                .isolation_level(isolation),
        )
        .unwrap();
        assert!(server.create_space(SpaceId(first.database_id().to_bytes())));
        let second = MultiliteConnection::open_with(
            &second_path,
            OpenOptions::new()
                .invitation(first.replica_invitation())
                .server(router(Arc::clone(&server)))
                .isolation_level(isolation),
        )
        .unwrap();

        first
            .update(|transaction| {
                transaction.execute(
                    "CREATE TABLE parents (
                        tenant TEXT,
                        id INTEGER,
                        PRIMARY KEY (tenant, id)
                    ) WITHOUT ROWID, STRICT",
                    (),
                )?;
                transaction.execute(
                    "CREATE TABLE inventory (
                        tenant TEXT,
                        sku INTEGER,
                        parent_tenant TEXT NOT NULL,
                        parent_id INTEGER NOT NULL,
                        code TEXT CONSTRAINT code_nn NOT NULL DEFAULT 'fallback',
                        quantity INTEGER NOT NULL DEFAULT 1
                            CONSTRAINT quantity_positive CHECK (quantity > 0),
                        note TEXT,
                        CONSTRAINT inventory_pk PRIMARY KEY (tenant, sku),
                        CONSTRAINT inventory_code UNIQUE (tenant, code),
                        CONSTRAINT inventory_parent FOREIGN KEY (parent_tenant, parent_id)
                            REFERENCES parents (tenant, id)
                    ) WITHOUT ROWID, STRICT",
                    (),
                )?;
                transaction.execute(
                    "CREATE UNIQUE INDEX inventory_note_unique ON inventory(note)",
                    (),
                )?;
                transaction.execute(
                    "CREATE INDEX inventory_code_lookup
                     ON inventory(lower(code) DESC) WHERE quantity > 0",
                    (),
                )?;
                transaction.execute("INSERT INTO parents VALUES ('north', 1), ('south', 2)", ())?;
                transaction.execute(
                    "INSERT INTO inventory VALUES
                        ('north', 1, 'north', 1, 'alpha', 2, 'north-note'),
                        ('south', 2, 'south', 2, 'beta', 3, 'south-note')",
                    (),
                )?;
                Ok(())
            })
            .unwrap();
        assert_eq!(first.push().unwrap(), PushOutcome::Drained);
        second.pull().unwrap();
        second.rebase().unwrap();

        first
            .execute(
                "INSERT INTO inventory VALUES
                    ('remote', 9, 'north', 1, 'remote', 4, 'remote-note')",
                (),
            )
            .unwrap();
        second.execute("DROP TABLE inventory", ()).unwrap();
        assert_eq!(first.push().unwrap(), PushOutcome::Drained);
        let PushOutcome::Rejected(rejection) = second.push().unwrap() else {
            panic!("stale DROP TABLE was not rejected")
        };
        drop(second);

        let second = MultiliteConnection::open_with(
            &second_path,
            OpenOptions::new()
                .server(router(Arc::clone(&server)))
                .isolation_level(isolation),
        )
        .unwrap();
        second.rollback(&rejection).unwrap();

        assert_eq!(
            index_names(&second),
            ["inventory_code_lookup", "inventory_note_unique"]
        );
        assert!(
            second
                .execute(
                    "INSERT INTO inventory VALUES
                        ('north', 7, 'north', 1, 'alpha', 2, 'another-note')",
                    (),
                )
                .is_err()
        );
        assert!(
            second
                .execute(
                    "INSERT INTO inventory VALUES
                        ('other', 7, 'north', 1, 'gamma', 2, 'north-note')",
                    (),
                )
                .is_err()
        );
        assert!(
            second
                .execute(
                    "INSERT INTO inventory VALUES
                        ('other', 8, 'north', 1, 'gamma', 0, 'zero-quantity')",
                    (),
                )
                .is_err()
        );
        assert!(
            second
                .execute(
                    "INSERT INTO inventory VALUES
                        ('other', 9, 'missing', 99, 'gamma', 2, 'missing-parent')",
                    (),
                )
                .is_err()
        );
        assert!(
            second
                .execute("DELETE FROM parents WHERE tenant = 'north' AND id = 1", ())
                .is_err()
        );
        assert_eq!(second.push().unwrap(), PushOutcome::Drained);
        second.pull().unwrap();
        second.rebase().unwrap();

        second
            .execute(
                "INSERT INTO inventory
                    (tenant, sku, parent_tenant, parent_id, note)
                 VALUES ('local', 3, 'south', 2, 'local-note')",
                (),
            )
            .unwrap();
        assert_eq!(
            second
                .query(
                    "SELECT code, quantity FROM inventory
                     WHERE tenant = 'local' AND sku = 3",
                    (),
                    |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?)),
                )
                .unwrap(),
            [("fallback".into(), 1)]
        );
        assert_eq!(second.push().unwrap(), PushOutcome::Drained);

        first.pull().unwrap();
        second.pull().unwrap();
        first.rebase().unwrap();
        second.rebase().unwrap();
        for database in [&first, &second] {
            assert_eq!(
                database
                    .query("SELECT count(*) FROM inventory", (), |row| {
                        row.get::<_, i64>(0)
                    })
                    .unwrap(),
                [4]
            );
            assert_eq!(
                index_names(database),
                ["inventory_code_lookup", "inventory_note_unique"]
            );
        }
    }
}

#[test]
fn admitted_child_drop_allows_later_parent_deletes_to_converge() {
    for isolation in [IsolationLevel::Snapshot, IsolationLevel::Serializable] {
        let directory = tempfile::tempdir().unwrap();
        let server = server();
        let first = MultiliteConnection::open_with(
            directory
                .path()
                .join(format!("drop-child-first-{isolation:?}.sqlite")),
            OpenOptions::new()
                .server(router(Arc::clone(&server)))
                .isolation_level(isolation),
        )
        .unwrap();
        assert!(server.create_space(SpaceId(first.database_id().to_bytes())));
        let second = MultiliteConnection::open_with(
            directory
                .path()
                .join(format!("drop-child-second-{isolation:?}.sqlite")),
            OpenOptions::new()
                .invitation(first.replica_invitation())
                .server(router(Arc::clone(&server)))
                .isolation_level(isolation),
        )
        .unwrap();

        first
            .update(|transaction| {
                transaction.execute(
                    "CREATE TABLE parents (
                        tenant TEXT,
                        id INTEGER,
                        PRIMARY KEY (tenant, id)
                    ) WITHOUT ROWID",
                    (),
                )?;
                transaction.execute(
                    "CREATE TABLE children (
                        id INTEGER PRIMARY KEY,
                        tenant TEXT,
                        parent_id INTEGER,
                        FOREIGN KEY (tenant, parent_id)
                            REFERENCES parents (tenant, id) ON DELETE CASCADE
                    )",
                    (),
                )?;
                transaction.execute("INSERT INTO parents VALUES ('north', 1)", ())?;
                transaction.execute("INSERT INTO children VALUES (7, 'north', 1)", ())?;
                Ok(())
            })
            .unwrap();
        assert_eq!(first.push().unwrap(), PushOutcome::Drained);
        second.pull().unwrap();
        second.rebase().unwrap();

        first.execute("DROP TABLE children", ()).unwrap();
        assert_eq!(first.push().unwrap(), PushOutcome::Drained);
        first
            .execute("DELETE FROM parents WHERE tenant = 'north' AND id = 1", ())
            .unwrap();
        assert_eq!(first.push().unwrap(), PushOutcome::Drained);

        second.pull().unwrap();
        second.rebase().unwrap();
        for database in [&first, &second] {
            assert!(
                database
                    .query("SELECT * FROM children", (), |_| Ok(()))
                    .is_err()
            );
            assert_eq!(
                database
                    .query("SELECT count(*) FROM parents", (), |row| row
                        .get::<_, i64>(0))
                    .unwrap(),
                [0]
            );
        }
    }
}

#[test]
fn composite_update_and_delete_repairs_survive_restart_and_converge() {
    let directory = tempfile::tempdir().unwrap();
    let server = server();
    let second_path = directory.path().join("composite-repair-second.sqlite");
    let first = MultiliteConnection::open_with(
        directory.path().join("composite-repair-first.sqlite"),
        OpenOptions::new().server(router(Arc::clone(&server))),
    )
    .unwrap();
    assert!(server.create_space(SpaceId(first.database_id().to_bytes())));
    let second = MultiliteConnection::open_with(
        &second_path,
        OpenOptions::new()
            .invitation(first.replica_invitation())
            .server(router(Arc::clone(&server))),
    )
    .unwrap();

    first
        .update(|transaction| {
            transaction.execute(
                "CREATE TABLE memberships (
                    tenant TEXT,
                    member INTEGER,
                    body TEXT NOT NULL,
                    PRIMARY KEY (tenant, member)
                ) WITHOUT ROWID",
                (),
            )?;
            transaction.execute(
                "INSERT INTO memberships VALUES
                    ('north', 1, 'north-original'),
                    ('south', 2, 'south-original'),
                    ('doomed', 9, 'delete-original')",
                (),
            )?;
            Ok(())
        })
        .unwrap();
    assert_eq!(first.push().unwrap(), PushOutcome::Drained);
    second.pull().unwrap();
    second.rebase().unwrap();

    first
        .execute(
            "UPDATE memberships
             SET tenant = 'shared', member = 7, body = 'winner'
             WHERE tenant = 'north' AND member = 1",
            (),
        )
        .unwrap();
    second
        .execute(
            "UPDATE memberships
             SET tenant = 'shared', member = 7, body = 'loser'
             WHERE tenant = 'south' AND member = 2",
            (),
        )
        .unwrap();
    assert_eq!(first.push().unwrap(), PushOutcome::Drained);
    let PushOutcome::Rejected(update_rejection) = second.push().unwrap() else {
        panic!("composite primary-key destination collision was not rejected")
    };
    drop(second);
    let second = MultiliteConnection::open_with(
        &second_path,
        OpenOptions::new().server(router(Arc::clone(&server))),
    )
    .unwrap();
    assert!(membership_rows(&second).contains(&("shared".into(), 7, "loser".into())));
    second.rollback(&update_rejection).unwrap();
    assert!(membership_rows(&second).contains(&("south".into(), 2, "south-original".into())));
    assert!(
        !membership_rows(&second)
            .iter()
            .any(|(tenant, member, _)| tenant == "shared" && *member == 7)
    );
    assert_eq!(second.push().unwrap(), PushOutcome::Drained);
    first.pull().unwrap();
    second.pull().unwrap();
    first.rebase().unwrap();
    second.rebase().unwrap();

    let after_update = [
        ("south".into(), 2, "south-original".into()),
        ("shared".into(), 7, "winner".into()),
        ("doomed".into(), 9, "delete-original".into()),
    ];
    assert_eq!(membership_rows(&first), after_update);
    assert_eq!(membership_rows(&second), after_update);

    first
        .execute(
            "DELETE FROM memberships WHERE tenant = 'doomed' AND member = 9",
            (),
        )
        .unwrap();
    second
        .execute(
            "DELETE FROM memberships WHERE tenant = 'doomed' AND member = 9",
            (),
        )
        .unwrap();
    assert_eq!(first.push().unwrap(), PushOutcome::Drained);
    let PushOutcome::Rejected(delete_rejection) = second.push().unwrap() else {
        panic!("same composite primary-key DELETE was not rejected")
    };
    drop(second);
    let second = MultiliteConnection::open_with(
        &second_path,
        OpenOptions::new().server(router(Arc::clone(&server))),
    )
    .unwrap();
    assert!(
        !membership_rows(&second)
            .iter()
            .any(|(tenant, member, _)| tenant == "doomed" && *member == 9)
    );
    second.rollback(&delete_rejection).unwrap();
    assert!(membership_rows(&second).contains(&("doomed".into(), 9, "delete-original".into())));
    assert_eq!(second.push().unwrap(), PushOutcome::Drained);
    first.pull().unwrap();
    second.pull().unwrap();
    first.rebase().unwrap();
    second.rebase().unwrap();

    let expected = [
        ("south".into(), 2, "south-original".into()),
        ("shared".into(), 7, "winner".into()),
    ];
    assert_eq!(membership_rows(&first), expected);
    assert_eq!(membership_rows(&second), expected);
}

#[test]
fn composite_unique_constraints_conflict_repair_and_converge() {
    let directory = tempfile::tempdir().unwrap();
    let server = server();
    let second_path = directory.path().join("unique-second.sqlite");
    let first = MultiliteConnection::open_with(
        directory.path().join("unique-first.sqlite"),
        OpenOptions::new().server(router(Arc::clone(&server))),
    )
    .unwrap();
    assert!(server.create_space(SpaceId(first.database_id().to_bytes())));
    let second = MultiliteConnection::open_with(
        &second_path,
        OpenOptions::new()
            .invitation(first.replica_invitation())
            .server(router(Arc::clone(&server))),
    )
    .unwrap();

    first
        .execute(
            "CREATE TABLE accounts (
                id INTEGER PRIMARY KEY,
                organization TEXT,
                email TEXT,
                CONSTRAINT account_email UNIQUE (organization, email)
            )",
            (),
        )
        .unwrap();
    assert_eq!(first.push().unwrap(), PushOutcome::Drained);
    second.pull().unwrap();
    second.rebase().unwrap();

    first
        .execute(
            "INSERT INTO accounts VALUES (1, 'acme', 'shared@example.com')",
            (),
        )
        .unwrap();
    second
        .execute(
            "INSERT INTO accounts VALUES (2, 'acme', 'shared@example.com')",
            (),
        )
        .unwrap();
    assert_eq!(first.push().unwrap(), PushOutcome::Drained);
    let PushOutcome::Rejected(rejection) = second.push().unwrap() else {
        panic!("composite UNIQUE collision was not rejected")
    };
    second.rollback(&rejection).unwrap();
    assert_eq!(second.push().unwrap(), PushOutcome::Drained);
    first.pull().unwrap();
    second.pull().unwrap();
    first.rebase().unwrap();
    second.rebase().unwrap();

    for database in [&first, &second] {
        assert_eq!(
            database
                .query(
                    "SELECT id, organization, email FROM accounts ORDER BY id",
                    (),
                    |row| {
                        Ok((
                            row.get::<_, i64>(0)?,
                            row.get::<_, Option<String>>(1)?,
                            row.get::<_, String>(2)?,
                        ))
                    },
                )
                .unwrap(),
            [(1, Some("acme".into()), "shared@example.com".into())]
        );
    }

    first
        .execute(
            "INSERT INTO accounts VALUES
                (10, 'alpha', 'a@example.com'),
                (11, 'beta', 'b@example.com')",
            (),
        )
        .unwrap();
    assert_eq!(first.push().unwrap(), PushOutcome::Drained);
    second.pull().unwrap();
    second.rebase().unwrap();

    first
        .execute(
            "UPDATE accounts
             SET organization = 'shared', email = 'updated@example.com'
             WHERE id = 10",
            (),
        )
        .unwrap();
    second
        .execute(
            "UPDATE accounts
             SET organization = 'shared', email = 'updated@example.com'
             WHERE id = 11",
            (),
        )
        .unwrap();
    assert_eq!(first.push().unwrap(), PushOutcome::Drained);
    let PushOutcome::Rejected(rejection) = second.push().unwrap() else {
        panic!("UPDATE into a composite UNIQUE collision was not rejected")
    };
    drop(second);
    let second = MultiliteConnection::open_with(
        &second_path,
        OpenOptions::new().server(router(Arc::clone(&server))),
    )
    .unwrap();
    second.rollback(&rejection).unwrap();
    assert_eq!(
        second
            .query(
                "SELECT organization, email FROM accounts WHERE id = 11",
                (),
                |row| { Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?,)) },
            )
            .unwrap(),
        [("beta".into(), "b@example.com".into())]
    );
    assert_eq!(second.push().unwrap(), PushOutcome::Drained);
    first.pull().unwrap();
    second.pull().unwrap();
    first.rebase().unwrap();
    second.rebase().unwrap();

    first
        .execute(
            "INSERT INTO accounts VALUES (20, NULL, 'nullable@example.com')",
            (),
        )
        .unwrap();
    second
        .execute(
            "INSERT INTO accounts VALUES (21, NULL, 'nullable@example.com')",
            (),
        )
        .unwrap();
    assert_eq!(first.push().unwrap(), PushOutcome::Drained);
    assert_eq!(second.push().unwrap(), PushOutcome::Drained);
    first.pull().unwrap();
    second.pull().unwrap();
    first.rebase().unwrap();
    second.rebase().unwrap();

    let expected = [
        (1, Some("acme".into()), "shared@example.com".into()),
        (10, Some("shared".into()), "updated@example.com".into()),
        (11, Some("beta".into()), "b@example.com".into()),
        (20, None, "nullable@example.com".into()),
        (21, None, "nullable@example.com".into()),
    ];
    for database in [&first, &second] {
        assert_eq!(
            database
                .query(
                    "SELECT id, organization, email FROM accounts ORDER BY id",
                    (),
                    |row| {
                        Ok((
                            row.get::<_, i64>(0)?,
                            row.get::<_, Option<String>>(1)?,
                            row.get::<_, String>(2)?,
                        ))
                    },
                )
                .unwrap(),
            expected
        );
    }

    first
        .execute("DELETE FROM accounts WHERE id = 1", ())
        .unwrap();
    assert_eq!(first.push().unwrap(), PushOutcome::Drained);
    second.pull().unwrap();
    second.rebase().unwrap();
    second
        .execute(
            "INSERT INTO accounts VALUES (30, 'acme', 'shared@example.com')",
            (),
        )
        .unwrap();
    assert_eq!(second.push().unwrap(), PushOutcome::Drained);
    first.pull().unwrap();
    second.pull().unwrap();
    first.rebase().unwrap();
    second.rebase().unwrap();
    for database in [&first, &second] {
        assert_eq!(
            database
                .query(
                    "SELECT id FROM accounts
                     WHERE organization = 'acme' AND email = 'shared@example.com'",
                    (),
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            [30]
        );
    }
}

#[test]
fn overlapping_unique_constraints_conflict_independently_and_together() {
    let directory = tempfile::tempdir().unwrap();
    let server = server();
    let second_path = directory.path().join("overlapping-unique-second.sqlite");
    let first = MultiliteConnection::open_with(
        directory.path().join("overlapping-unique-first.sqlite"),
        OpenOptions::new().server(router(Arc::clone(&server))),
    )
    .unwrap();
    assert!(server.create_space(SpaceId(first.database_id().to_bytes())));
    let second = MultiliteConnection::open_with(
        &second_path,
        OpenOptions::new()
            .invitation(first.replica_invitation())
            .server(router(Arc::clone(&server))),
    )
    .unwrap();

    first
        .execute(
            "CREATE TABLE profiles (
                id INTEGER PRIMARY KEY,
                tenant TEXT,
                email TEXT UNIQUE,
                username TEXT UNIQUE,
                UNIQUE (tenant, email),
                UNIQUE (tenant, username)
            )",
            (),
        )
        .unwrap();
    assert_eq!(first.push().unwrap(), PushOutcome::Drained);
    second.pull().unwrap();
    second.rebase().unwrap();

    // Different tenants avoid the composite key. Only UNIQUE(email) collides.
    first
        .execute(
            "INSERT INTO profiles VALUES
                (1, 'acme', 'shared@example.com', 'alpha')",
            (),
        )
        .unwrap();
    second
        .execute(
            "INSERT INTO profiles VALUES
                (2, 'other', 'shared@example.com', 'beta')",
            (),
        )
        .unwrap();
    assert_eq!(first.push().unwrap(), PushOutcome::Drained);
    let PushOutcome::Rejected(rejection) = second.push().unwrap() else {
        panic!("global UNIQUE collision was not rejected")
    };
    second.rollback(&rejection).unwrap();
    assert_eq!(second.push().unwrap(), PushOutcome::Drained);
    first.pull().unwrap();
    second.pull().unwrap();
    first.rebase().unwrap();
    second.rebase().unwrap();

    first
        .execute(
            "INSERT INTO profiles VALUES
                (10, 'acme', 'ten@example.com', 'ten'),
                (11, 'acme', 'eleven@example.com', 'eleven')",
            (),
        )
        .unwrap();
    assert_eq!(first.push().unwrap(), PushOutcome::Drained);
    second.pull().unwrap();
    second.rebase().unwrap();

    // This collides in both UNIQUE(username) and UNIQUE(tenant, username).
    first
        .execute(
            "UPDATE profiles
             SET email = 'winner@example.com', username = 'claimed'
             WHERE id = 10",
            (),
        )
        .unwrap();
    second
        .execute(
            "UPDATE profiles
             SET email = 'loser@example.com', username = 'claimed'
             WHERE id = 11",
            (),
        )
        .unwrap();
    assert_eq!(first.push().unwrap(), PushOutcome::Drained);
    let PushOutcome::Rejected(rejection) = second.push().unwrap() else {
        panic!("overlapping UNIQUE collision was not rejected")
    };
    drop(second);
    let second = MultiliteConnection::open_with(
        &second_path,
        OpenOptions::new().server(router(Arc::clone(&server))),
    )
    .unwrap();
    second.rollback(&rejection).unwrap();
    assert_eq!(second.push().unwrap(), PushOutcome::Drained);
    first.pull().unwrap();
    second.pull().unwrap();
    first.rebase().unwrap();
    second.rebase().unwrap();

    let expected = [
        (
            1,
            "acme".into(),
            "shared@example.com".into(),
            "alpha".into(),
        ),
        (
            10,
            "acme".into(),
            "winner@example.com".into(),
            "claimed".into(),
        ),
        (
            11,
            "acme".into(),
            "eleven@example.com".into(),
            "eleven".into(),
        ),
    ];
    assert_eq!(profiles(&first), expected);
    assert_eq!(profiles(&second), expected);
}

#[test]
fn unique_numeric_key_images_match_sqlite_storage_class_equality() {
    let directory = tempfile::tempdir().unwrap();
    let server = server();
    let first = MultiliteConnection::open_with(
        directory.path().join("numeric-unique-first.sqlite"),
        OpenOptions::new().server(router(Arc::clone(&server))),
    )
    .unwrap();
    assert!(server.create_space(SpaceId(first.database_id().to_bytes())));
    let second = MultiliteConnection::open_with(
        directory.path().join("numeric-unique-second.sqlite"),
        OpenOptions::new()
            .invitation(first.replica_invitation())
            .server(router(Arc::clone(&server))),
    )
    .unwrap();

    first
        .execute(
            "CREATE TABLE values_by_type (
                id INTEGER PRIMARY KEY,
                value BLOB UNIQUE
            )",
            (),
        )
        .unwrap();
    assert_eq!(first.push().unwrap(), PushOutcome::Drained);
    second.pull().unwrap();
    second.rebase().unwrap();

    first
        .execute("INSERT INTO values_by_type VALUES (1, 1)", ())
        .unwrap();
    second
        .execute("INSERT INTO values_by_type VALUES (2, 1.0)", ())
        .unwrap();
    assert_eq!(first.push().unwrap(), PushOutcome::Drained);
    let PushOutcome::Rejected(rejection) = second.push().unwrap() else {
        panic!("equal INTEGER and REAL unique values were both admitted")
    };
    second.rollback(&rejection).unwrap();
    assert_eq!(second.push().unwrap(), PushOutcome::Drained);
    first.pull().unwrap();
    second.pull().unwrap();
    first.rebase().unwrap();
    second.rebase().unwrap();

    first
        .execute("INSERT INTO values_by_type VALUES (3, 2)", ())
        .unwrap();
    second
        .execute("INSERT INTO values_by_type VALUES (4, '2')", ())
        .unwrap();
    assert_eq!(first.push().unwrap(), PushOutcome::Drained);
    assert_eq!(second.push().unwrap(), PushOutcome::Drained);
    first.pull().unwrap();
    second.pull().unwrap();
    first.rebase().unwrap();
    second.rebase().unwrap();

    for database in [&first, &second] {
        assert_eq!(
            database
                .query(
                    "SELECT id, typeof(value) FROM values_by_type ORDER BY id",
                    (),
                    |row| Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?)),
                )
                .unwrap(),
            [
                (1, "integer".into()),
                (3, "integer".into()),
                (4, "text".into()),
            ]
        );
    }
}

#[test]
fn disjoint_mixed_schema_and_row_transactions_converge_in_manifest_order() {
    let directory = tempfile::tempdir().unwrap();
    let server = server();
    let first = MultiliteConnection::open_with(
        directory.path().join("mixed-first.sqlite"),
        OpenOptions::new().server(router(Arc::clone(&server))),
    )
    .unwrap();
    assert!(server.create_space(SpaceId(first.database_id().to_bytes())));
    let second = MultiliteConnection::open_with(
        directory.path().join("mixed-second.sqlite"),
        OpenOptions::new()
            .invitation(first.replica_invitation())
            .server(router(Arc::clone(&server))),
    )
    .unwrap();

    first
        .update(|transaction| {
            transaction.execute(
                "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT NOT NULL)",
                (),
            )?;
            transaction.execute("INSERT INTO notes VALUES (1, 'first')", ())?;
            Ok(())
        })
        .unwrap();
    second
        .update(|transaction| {
            transaction.execute(
                "CREATE TABLE tasks (
                    id TEXT NOT NULL PRIMARY KEY,
                    body TEXT NOT NULL
                ) WITHOUT ROWID",
                (),
            )?;
            transaction.execute("INSERT INTO tasks VALUES ('a', 'second')", ())?;
            Ok(())
        })
        .unwrap();

    assert_eq!(first.push().unwrap(), PushOutcome::Drained);
    assert_eq!(second.push().unwrap(), PushOutcome::Drained);
    first.pull().unwrap();
    second.pull().unwrap();
    first.rebase().unwrap();
    second.rebase().unwrap();

    assert_eq!(tables(&first), tables(&second));
    for database in [&first, &second] {
        assert_eq!(
            database
                .query("SELECT id, body FROM notes", (), |row| {
                    Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
                })
                .unwrap(),
            [(1, "first".into())]
        );
        assert_eq!(
            database
                .query("SELECT id, body FROM tasks", (), |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
                })
                .unwrap(),
            [("a".into(), "second".into())]
        );
    }
}

#[test]
fn disjoint_updates_admit_and_same_row_updates_repair_then_converge() {
    let directory = tempfile::tempdir().unwrap();
    let server = server();
    let second_path = directory.path().join("update-second.sqlite");
    let first = MultiliteConnection::open_with(
        directory.path().join("update-first.sqlite"),
        OpenOptions::new().server(router(Arc::clone(&server))),
    )
    .unwrap();
    assert!(server.create_space(SpaceId(first.database_id().to_bytes())));
    let second = MultiliteConnection::open_with(
        &second_path,
        OpenOptions::new()
            .invitation(first.replica_invitation())
            .server(router(Arc::clone(&server))),
    )
    .unwrap();

    first
        .update(|transaction| {
            transaction.execute(
                "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT NOT NULL)",
                (),
            )?;
            transaction.execute(
                "INSERT INTO notes VALUES
                    (1, 'first'),
                    (2, 'second'),
                    (7, 'contended')",
                (),
            )?;
            Ok(())
        })
        .unwrap();
    assert_eq!(first.push().unwrap(), PushOutcome::Drained);
    second.pull().unwrap();
    second.rebase().unwrap();

    assert_eq!(
        first
            .execute("UPDATE notes SET body = 'first-updated' WHERE id = 1", ())
            .unwrap(),
        1
    );
    assert_eq!(
        second
            .execute("UPDATE notes SET body = 'second-updated' WHERE id = 2", ())
            .unwrap(),
        1
    );
    assert_eq!(first.push().unwrap(), PushOutcome::Drained);
    assert_eq!(second.push().unwrap(), PushOutcome::Drained);
    first.pull().unwrap();
    second.pull().unwrap();
    first.rebase().unwrap();
    second.rebase().unwrap();
    assert_eq!(
        rows(&first),
        [
            (1, "first-updated".into()),
            (2, "second-updated".into()),
            (7, "contended".into()),
        ]
    );
    assert_eq!(rows(&first), rows(&second));

    first
        .execute("UPDATE notes SET body = 'winner' WHERE id = 7", ())
        .unwrap();
    second
        .execute("UPDATE notes SET body = 'loser' WHERE id = 7", ())
        .unwrap();
    assert_eq!(first.push().unwrap(), PushOutcome::Drained);
    let PushOutcome::Rejected(rejection) = second.push().unwrap() else {
        panic!("same-row UPDATE was not rejected")
    };
    drop(second);
    let second = MultiliteConnection::open_with(
        &second_path,
        OpenOptions::new().server(router(Arc::clone(&server))),
    )
    .unwrap();
    assert_eq!(
        second
            .query("SELECT body FROM notes WHERE id = 7", (), |row| {
                row.get::<_, String>(0)
            })
            .unwrap(),
        ["loser"]
    );
    second.rollback(&rejection).unwrap();
    assert_eq!(
        second
            .query("SELECT body FROM notes WHERE id = 7", (), |row| {
                row.get::<_, String>(0)
            })
            .unwrap(),
        ["contended"]
    );
    assert_eq!(second.push().unwrap(), PushOutcome::Drained);

    first.pull().unwrap();
    second.pull().unwrap();
    first.rebase().unwrap();
    second.rebase().unwrap();
    assert_eq!(rows(&first), rows(&second));
    assert_eq!(
        first
            .query("SELECT body FROM notes WHERE id = 7", (), |row| {
                row.get::<_, String>(0)
            })
            .unwrap(),
        ["winner"]
    );
}

#[test]
fn disjoint_key_moves_admit_and_destination_collisions_repair_then_converge() {
    let directory = tempfile::tempdir().unwrap();
    let server = server();
    let first = MultiliteConnection::open_with(
        directory.path().join("move-first.sqlite"),
        OpenOptions::new().server(router(Arc::clone(&server))),
    )
    .unwrap();
    assert!(server.create_space(SpaceId(first.database_id().to_bytes())));
    let second = MultiliteConnection::open_with(
        directory.path().join("move-second.sqlite"),
        OpenOptions::new()
            .invitation(first.replica_invitation())
            .server(router(Arc::clone(&server))),
    )
    .unwrap();

    first
        .update(|transaction| {
            transaction.execute(
                "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT NOT NULL)",
                (),
            )?;
            transaction.execute(
                "INSERT INTO notes VALUES
                    (1, 'one'),
                    (2, 'two'),
                    (3, 'three'),
                    (4, 'four')",
                (),
            )?;
            Ok(())
        })
        .unwrap();
    assert_eq!(first.push().unwrap(), PushOutcome::Drained);
    second.pull().unwrap();
    second.rebase().unwrap();

    first
        .execute("UPDATE notes SET id = 11 WHERE id = 1", ())
        .unwrap();
    second
        .execute("UPDATE notes SET id = 12 WHERE id = 2", ())
        .unwrap();
    assert_eq!(first.push().unwrap(), PushOutcome::Drained);
    assert_eq!(second.push().unwrap(), PushOutcome::Drained);
    first.pull().unwrap();
    second.pull().unwrap();
    first.rebase().unwrap();
    second.rebase().unwrap();
    assert_eq!(rows(&first), rows(&second));
    assert_eq!(
        rows(&first),
        [
            (3, "three".into()),
            (4, "four".into()),
            (11, "one".into()),
            (12, "two".into()),
        ]
    );

    first
        .execute("UPDATE notes SET id = 30 WHERE id = 3", ())
        .unwrap();
    second
        .execute("UPDATE notes SET id = 30 WHERE id = 4", ())
        .unwrap();
    assert_eq!(first.push().unwrap(), PushOutcome::Drained);
    let PushOutcome::Rejected(rejection) = second.push().unwrap() else {
        panic!("colliding UPDATE destination was not rejected")
    };
    second.rollback(&rejection).unwrap();
    assert_eq!(
        second
            .query(
                "SELECT id, body FROM notes WHERE id IN (4, 30)",
                (),
                |row| { Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?)) }
            )
            .unwrap(),
        [(4, "four".into())]
    );
    assert_eq!(second.push().unwrap(), PushOutcome::Drained);

    first.pull().unwrap();
    second.pull().unwrap();
    first.rebase().unwrap();
    second.rebase().unwrap();
    assert_eq!(rows(&first), rows(&second));
    assert_eq!(
        rows(&first),
        [
            (4, "four".into()),
            (11, "one".into()),
            (12, "two".into()),
            (30, "three".into()),
        ]
    );
}

#[test]
fn disjoint_deletes_admit_and_same_row_deletes_repair_then_converge() {
    let directory = tempfile::tempdir().unwrap();
    let server = server();
    let second_path = directory.path().join("delete-second.sqlite");
    let first = MultiliteConnection::open_with(
        directory.path().join("delete-first.sqlite"),
        OpenOptions::new().server(router(Arc::clone(&server))),
    )
    .unwrap();
    assert!(server.create_space(SpaceId(first.database_id().to_bytes())));
    let second = MultiliteConnection::open_with(
        &second_path,
        OpenOptions::new()
            .invitation(first.replica_invitation())
            .server(router(Arc::clone(&server))),
    )
    .unwrap();

    first
        .update(|transaction| {
            transaction.execute(
                "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT NOT NULL)",
                (),
            )?;
            transaction.execute(
                "INSERT INTO notes VALUES
                    (1, 'first'),
                    (2, 'second'),
                    (7, 'contended')",
                (),
            )?;
            Ok(())
        })
        .unwrap();
    assert_eq!(first.push().unwrap(), PushOutcome::Drained);
    second.pull().unwrap();
    second.rebase().unwrap();

    assert_eq!(
        first
            .execute("DELETE FROM notes WHERE id = ?1", [1_i64])
            .unwrap(),
        1
    );
    assert_eq!(
        second
            .execute("DELETE FROM notes WHERE id = ?1", [2_i64])
            .unwrap(),
        1
    );
    assert_eq!(first.push().unwrap(), PushOutcome::Drained);
    assert_eq!(second.push().unwrap(), PushOutcome::Drained);
    first.pull().unwrap();
    second.pull().unwrap();
    first.rebase().unwrap();
    second.rebase().unwrap();
    assert_eq!(rows(&first), [(7, "contended".into())]);
    assert_eq!(rows(&second), [(7, "contended".into())]);

    first.execute("DELETE FROM notes WHERE id = 7", ()).unwrap();
    second
        .execute("DELETE FROM notes WHERE id = 7", ())
        .unwrap();
    assert!(rows(&second).is_empty());
    assert_eq!(first.push().unwrap(), PushOutcome::Drained);
    let PushOutcome::Rejected(rejection) = second.push().unwrap() else {
        panic!("same-row DELETE was not rejected")
    };
    drop(second);
    let second = MultiliteConnection::open_with(
        &second_path,
        OpenOptions::new().server(router(Arc::clone(&server))),
    )
    .unwrap();
    assert!(rows(&second).is_empty());
    second.rollback(&rejection).unwrap();
    assert_eq!(rows(&second), [(7, "contended".into())]);
    assert_eq!(second.push().unwrap(), PushOutcome::Drained);

    first.pull().unwrap();
    second.pull().unwrap();
    first.rebase().unwrap();
    second.rebase().unwrap();
    assert!(rows(&first).is_empty());
    assert!(rows(&second).is_empty());
}

#[test]
fn queued_insert_then_delete_admit_in_same_device_order() {
    let directory = tempfile::tempdir().unwrap();
    let server = server();
    let database = MultiliteConnection::open_with(
        directory.path().join("local-insert-delete.sqlite"),
        OpenOptions::new().server(router(Arc::clone(&server))),
    )
    .unwrap();
    assert!(server.create_space(SpaceId(database.database_id().to_bytes())));
    database
        .execute(
            "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT NOT NULL)",
            (),
        )
        .unwrap();
    assert_eq!(database.push().unwrap(), PushOutcome::Drained);

    database
        .execute("INSERT INTO notes VALUES (1, 'accepted prefix')", ())
        .unwrap();
    database
        .execute("DELETE FROM notes WHERE id = 1", ())
        .unwrap();
    assert!(rows(&database).is_empty());

    assert_eq!(database.push().unwrap(), PushOutcome::Drained);
    database.pull().unwrap();
    database.rebase().unwrap();
    assert!(rows(&database).is_empty());
}

#[test]
fn queued_insert_then_key_move_admit_in_same_device_order() {
    let directory = tempfile::tempdir().unwrap();
    let server = server();
    let database = MultiliteConnection::open_with(
        directory.path().join("local-insert-update.sqlite"),
        OpenOptions::new().server(router(Arc::clone(&server))),
    )
    .unwrap();
    assert!(server.create_space(SpaceId(database.database_id().to_bytes())));
    database
        .execute(
            "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT NOT NULL)",
            (),
        )
        .unwrap();
    assert_eq!(database.push().unwrap(), PushOutcome::Drained);

    database
        .execute("INSERT INTO notes VALUES (1, 'before')", ())
        .unwrap();
    database
        .execute("UPDATE notes SET id = 2, body = 'after' WHERE id = 1", ())
        .unwrap();
    assert_eq!(rows(&database), [(2, "after".into())]);

    assert_eq!(database.push().unwrap(), PushOutcome::Drained);
    database.pull().unwrap();
    database.rebase().unwrap();
    assert_eq!(rows(&database), [(2, "after".into())]);
}

#[test]
fn rejected_text_primary_key_delete_restores_the_complete_without_rowid_row() {
    let directory = tempfile::tempdir().unwrap();
    let server = server();
    let first = MultiliteConnection::open_with(
        directory.path().join("text-delete-first.sqlite"),
        OpenOptions::new().server(router(Arc::clone(&server))),
    )
    .unwrap();
    assert!(server.create_space(SpaceId(first.database_id().to_bytes())));
    let second = MultiliteConnection::open_with(
        directory.path().join("text-delete-second.sqlite"),
        OpenOptions::new()
            .invitation(first.replica_invitation())
            .server(router(Arc::clone(&server))),
    )
    .unwrap();

    first
        .update(|transaction| {
            transaction.execute(
                "CREATE TABLE documents (
                    id TEXT NOT NULL PRIMARY KEY,
                    body TEXT NOT NULL
                ) WITHOUT ROWID",
                (),
            )?;
            transaction.execute("INSERT INTO documents VALUES ('a', 'original')", ())?;
            Ok(())
        })
        .unwrap();
    assert_eq!(first.push().unwrap(), PushOutcome::Drained);
    second.pull().unwrap();
    second.rebase().unwrap();
    let original = document(&second);
    assert_eq!(document(&first), original);

    first
        .execute("DELETE FROM documents WHERE id = 'a'", ())
        .unwrap();
    second
        .execute("DELETE FROM documents WHERE id = 'a'", ())
        .unwrap();
    assert_eq!(first.push().unwrap(), PushOutcome::Drained);
    let PushOutcome::Rejected(rejection) = second.push().unwrap() else {
        panic!("same text-key DELETE was not rejected")
    };
    second.rollback(&rejection).unwrap();
    assert_eq!(document(&second), original);
    assert_eq!(second.push().unwrap(), PushOutcome::Drained);

    first.pull().unwrap();
    second.pull().unwrap();
    first.rebase().unwrap();
    second.rebase().unwrap();
    for database in [&first, &second] {
        assert!(
            database
                .query("SELECT id FROM documents", (), |row| row
                    .get::<_, String>(0))
                .unwrap()
                .is_empty()
        );
    }
}

#[test]
fn rejected_text_primary_key_move_restores_then_applies_the_winner() {
    let directory = tempfile::tempdir().unwrap();
    let server = server();
    let first = MultiliteConnection::open_with(
        directory.path().join("text-update-first.sqlite"),
        OpenOptions::new().server(router(Arc::clone(&server))),
    )
    .unwrap();
    assert!(server.create_space(SpaceId(first.database_id().to_bytes())));
    let second = MultiliteConnection::open_with(
        directory.path().join("text-update-second.sqlite"),
        OpenOptions::new()
            .invitation(first.replica_invitation())
            .server(router(Arc::clone(&server))),
    )
    .unwrap();

    first
        .update(|transaction| {
            transaction.execute(
                "CREATE TABLE documents (
                    id TEXT NOT NULL PRIMARY KEY,
                    body TEXT NOT NULL
                ) WITHOUT ROWID",
                (),
            )?;
            transaction.execute("INSERT INTO documents VALUES ('a', 'original')", ())?;
            Ok(())
        })
        .unwrap();
    assert_eq!(first.push().unwrap(), PushOutcome::Drained);
    second.pull().unwrap();
    second.rebase().unwrap();
    let original = document(&second);

    first
        .execute(
            "UPDATE documents SET id = 'winner', body = 'first' WHERE id = 'a'",
            (),
        )
        .unwrap();
    second
        .execute(
            "UPDATE documents SET id = 'loser', body = 'second' WHERE id = 'a'",
            (),
        )
        .unwrap();
    assert_eq!(first.push().unwrap(), PushOutcome::Drained);
    let PushOutcome::Rejected(rejection) = second.push().unwrap() else {
        panic!("same text-key UPDATE was not rejected")
    };
    second.rollback(&rejection).unwrap();
    assert_eq!(document(&second), original);
    assert_eq!(second.push().unwrap(), PushOutcome::Drained);

    first.pull().unwrap();
    second.pull().unwrap();
    first.rebase().unwrap();
    second.rebase().unwrap();
    for database in [&first, &second] {
        let updated = document(database);
        assert_eq!(
            (updated.0.as_str(), updated.1.as_str()),
            ("winner", "first")
        );
    }
}

#[test]
fn rejected_limited_delete_reopens_repairs_exact_row_then_converges() {
    for isolation in [IsolationLevel::Snapshot, IsolationLevel::Serializable] {
        let directory = tempfile::tempdir().unwrap();
        let server = server();
        let first_path = directory
            .path()
            .join(format!("limited-first-{isolation:?}.sqlite"));
        let second_path = directory
            .path()
            .join(format!("limited-second-{isolation:?}.sqlite"));
        let options = || {
            OpenOptions::new()
                .isolation_level(isolation)
                .server(router(Arc::clone(&server)))
        };
        let first = MultiliteConnection::open_with(&first_path, options()).unwrap();
        assert!(server.create_space(SpaceId(first.database_id().to_bytes())));
        let second = MultiliteConnection::open_with(
            &second_path,
            OpenOptions::new()
                .isolation_level(isolation)
                .invitation(first.replica_invitation())
                .server(router(Arc::clone(&server))),
        )
        .unwrap();

        first
            .update(|transaction| {
                transaction.execute(
                    "CREATE TABLE notes (
                        id INTEGER PRIMARY KEY,
                        score INTEGER NOT NULL,
                        body TEXT NOT NULL
                    )",
                    (),
                )?;
                transaction.execute(
                    "INSERT INTO notes VALUES
                        (1, 10, 'one'), (2, 20, 'two'),
                        (3, 30, 'three'), (4, 40, 'four')",
                    (),
                )?;
                Ok(())
            })
            .unwrap();
        assert_eq!(first.push().unwrap(), PushOutcome::Drained);
        second.pull().unwrap();
        second.rebase().unwrap();

        assert_eq!(
            first
                .execute(
                    "UPDATE notes SET body = 'winner'
                     ORDER BY score DESC, id LIMIT 2",
                    (),
                )
                .unwrap(),
            2
        );
        assert_eq!(
            second
                .execute("DELETE FROM notes ORDER BY score DESC, id LIMIT 1", (),)
                .unwrap(),
            1
        );
        assert_eq!(first.push().unwrap(), PushOutcome::Drained);
        let PushOutcome::Rejected(rejection) = second.push().unwrap() else {
            panic!("overlapping limited write was not rejected")
        };
        drop(second);

        let second = MultiliteConnection::open_with(&second_path, options()).unwrap();
        assert_eq!(
            second
                .query("SELECT id FROM notes ORDER BY id", (), |row| {
                    row.get::<_, i64>(0)
                })
                .unwrap(),
            [1, 2, 3]
        );
        second.rollback(&rejection).unwrap();
        assert_eq!(
            second
                .query("SELECT id, body FROM notes ORDER BY id", (), |row| {
                    Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
                })
                .unwrap(),
            [
                (1, "one".into()),
                (2, "two".into()),
                (3, "three".into()),
                (4, "four".into()),
            ]
        );
        assert_eq!(second.push().unwrap(), PushOutcome::Drained);

        first.pull().unwrap();
        second.pull().unwrap();
        first.rebase().unwrap();
        second.rebase().unwrap();
        let expected = vec![
            (1, "one".into()),
            (2, "two".into()),
            (3, "winner".into()),
            (4, "winner".into()),
        ];
        assert_eq!(rows(&first), expected);
        assert_eq!(rows(&second), expected);
    }
}

#[test]
fn schema_conflict_policies_repair_stale_replacements_and_converge() {
    for isolation in [IsolationLevel::Snapshot, IsolationLevel::Serializable] {
        let directory = tempfile::tempdir().unwrap();
        let authority = server();
        let first_path = directory
            .path()
            .join(format!("policy-first-{isolation:?}.sqlite"));
        let second_path = directory
            .path()
            .join(format!("policy-second-{isolation:?}.sqlite"));
        let first = MultiliteConnection::open_with(
            &first_path,
            OpenOptions::new()
                .isolation_level(isolation)
                .server(router(Arc::clone(&authority))),
        )
        .unwrap();
        assert!(authority.create_space(SpaceId(first.database_id().to_bytes())));
        let second_options = || {
            OpenOptions::new()
                .isolation_level(isolation)
                .invitation(first.replica_invitation())
                .server(router(Arc::clone(&authority)))
        };
        let second = MultiliteConnection::open_with(&second_path, second_options()).unwrap();

        first
            .execute(
                "CREATE TABLE accounts (
                    id INTEGER PRIMARY KEY ON CONFLICT REPLACE,
                    email TEXT UNIQUE ON CONFLICT REPLACE,
                    body TEXT NOT NULL ON CONFLICT REPLACE DEFAULT 'fallback'
                )",
                (),
            )
            .unwrap();
        assert_eq!(first.push().unwrap(), PushOutcome::Drained);
        second.pull().unwrap();
        second.rebase().unwrap();

        first
            .execute(
                "INSERT INTO accounts VALUES (1, 'same@example', 'winner')",
                (),
            )
            .unwrap();
        second
            .execute("INSERT INTO accounts VALUES (2, 'same@example', NULL)", ())
            .unwrap();
        assert_eq!(first.push().unwrap(), PushOutcome::Drained);
        let PushOutcome::Rejected(rejection) = second.push().unwrap() else {
            panic!("stale UNIQUE replacement was not rejected")
        };
        drop(second);

        let second = MultiliteConnection::open_with(&second_path, second_options()).unwrap();
        assert_eq!(
            second
                .query("SELECT id, body FROM accounts", (), |row| {
                    Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
                })
                .unwrap(),
            [(2, "fallback".into())]
        );
        second.rollback(&rejection).unwrap();
        assert!(
            second
                .query("SELECT id FROM accounts", (), |row| row.get::<_, i64>(0))
                .unwrap()
                .is_empty()
        );
        assert_eq!(second.push().unwrap(), PushOutcome::Drained);
        second.pull().unwrap();
        second.rebase().unwrap();

        second
            .execute("INSERT INTO accounts VALUES (2, 'same@example', NULL)", ())
            .unwrap();
        assert_eq!(second.push().unwrap(), PushOutcome::Drained);
        first.pull().unwrap();
        first.rebase().unwrap();
        second.pull().unwrap();
        second.rebase().unwrap();

        for database in [&first, &second] {
            assert_eq!(
                database
                    .query("SELECT id, email, body FROM accounts", (), |row| {
                        Ok((
                            row.get::<_, i64>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, String>(2)?,
                        ))
                    })
                    .unwrap(),
                [(2, "same@example".into(), "fallback".into())]
            );
        }
    }
}

fn document<H>(database: &MultiliteConnection<H>) -> (String, String)
where
    H: ServerHandle + Send + Sync + 'static,
{
    database
        .query("SELECT id, body FROM documents", (), |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })
        .unwrap()
        .into_iter()
        .next()
        .expect("document exists")
}

fn profiles<H>(database: &MultiliteConnection<H>) -> Vec<(i64, String, String, String)>
where
    H: ServerHandle + Send + Sync + 'static,
{
    database
        .query(
            "SELECT id, tenant, email, username FROM profiles ORDER BY id",
            (),
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                ))
            },
        )
        .unwrap()
}
