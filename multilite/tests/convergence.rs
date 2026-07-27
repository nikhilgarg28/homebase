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
            "CREATE UNIQUE INDEX notes_identity ON notes (tenant, slug)",
            (),
        )
        .unwrap();
    second
        .execute(
            "CREATE UNIQUE INDEX notes_identity ON notes (slug, body)",
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

#[test]
fn rejected_text_primary_key_move_restores_rowid_then_applies_the_winner() {
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
        assert_eq!(updated.0, original.0);
        assert_eq!(
            (updated.1.as_str(), updated.2.as_str()),
            ("winner", "first")
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
