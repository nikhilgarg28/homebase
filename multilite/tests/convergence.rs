use std::sync::Arc;

use homebase_client::ServerHandle;
use homebase_core::space::SpaceId;
use multilite::{MultiliteConnection, OpenOptions, PushOutcome};

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
            "CREATE TABLE documents (id TEXT NOT NULL PRIMARY KEY, body TEXT NOT NULL)",
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
                "CREATE TABLE tasks (id TEXT NOT NULL PRIMARY KEY, body TEXT NOT NULL)",
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
fn rejected_text_primary_key_delete_restores_values_and_hidden_rowid() {
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
                )",
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

fn document<H>(database: &MultiliteConnection<H>) -> (i64, String, String)
where
    H: ServerHandle + Send + Sync + 'static,
{
    database
        .query("SELECT _rowid_, id, body FROM documents", (), |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
            ))
        })
        .unwrap()
        .into_iter()
        .next()
        .expect("document exists")
}
