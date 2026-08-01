use std::collections::BTreeSet;
use std::sync::{Arc, Barrier};

use homebase_client::ServerHandle;
use homebase_core::space::SpaceId;
use multilite::{Error, MultiliteConnection, OpenOptions, PushOutcome, Result};

mod common;

use common::{router, server};

fn rows<H>(database: &MultiliteConnection<H>) -> Vec<(i64, String)>
where
    H: ServerHandle + Send + Sync + 'static,
{
    database
        .query("SELECT id, body FROM notes ORDER BY body", (), |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
        })
        .unwrap()
}

#[test]
fn generated_ipks_share_a_process_lease_and_preserve_explicit_values() {
    let directory = tempfile::tempdir().unwrap();
    let database = MultiliteConnection::open(directory.path().join("generated.sqlite")).unwrap();
    database
        .execute(
            "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT NOT NULL)",
            (),
        )
        .unwrap();
    database
        .execute("INSERT INTO notes (body) VALUES ('one'), ('two')", ())
        .unwrap();
    database
        .execute("INSERT INTO notes VALUES (42, 'explicit')", ())
        .unwrap();
    database
        .execute("INSERT INTO notes VALUES (NULL, 'three')", ())
        .unwrap();

    let generated = database
        .query(
            "SELECT id FROM notes WHERE body != 'explicit' ORDER BY id",
            (),
            |row| row.get::<_, i64>(0),
        )
        .unwrap();
    assert_eq!(generated.len(), 3);
    assert!(generated.iter().all(|id| *id >= 1_i64 << 47));
    assert_eq!(generated[1], generated[0] + 1);
    assert_eq!(generated[2], generated[1] + 1);
    assert_eq!(
        database
            .query("SELECT id FROM notes WHERE body = 'explicit'", (), |row| {
                row.get::<_, i64>(0)
            })
            .unwrap(),
        [42]
    );
}

#[test]
fn multi_row_insert_crosses_a_lease_boundary_without_gaps() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("lease-boundary.sqlite");
    let first;
    {
        let database = MultiliteConnection::open(&path).unwrap();
        database
            .execute(
                "CREATE TABLE notes (id INTEGER PRIMARY KEY, body INTEGER NOT NULL)",
                (),
            )
            .unwrap();
        database
            .execute(
                "WITH RECURSIVE generated(body) AS (
                    VALUES (0)
                    UNION ALL
                    SELECT body + 1 FROM generated WHERE body < 1024
                 )
                 INSERT INTO notes(body) SELECT body FROM generated",
                (),
            )
            .unwrap();
        let bounds = database
            .query("SELECT count(*), min(id), max(id) FROM notes", (), |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, i64>(2)?,
                ))
            })
            .unwrap()[0];
        assert_eq!(bounds.0, 1_025);
        assert_eq!(bounds.2 - bounds.1, 1_024);
        first = bounds.1;
    }

    let database = MultiliteConnection::open(&path).unwrap();
    database
        .execute("INSERT INTO notes(body) VALUES (1025)", ())
        .unwrap();
    let newest = database
        .query("SELECT max(id) FROM notes", (), |row| row.get::<_, i64>(0))
        .unwrap()[0];
    assert_eq!(newest, first + 2_048);
}

#[test]
fn concurrent_branches_share_one_allocator_without_duplicate_ids() {
    const WRITERS: usize = 24;

    let directory = tempfile::tempdir().unwrap();
    let database =
        Arc::new(MultiliteConnection::open(directory.path().join("concurrent.sqlite")).unwrap());
    database
        .execute(
            "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT NOT NULL UNIQUE)",
            (),
        )
        .unwrap();
    let barrier = Arc::new(Barrier::new(WRITERS));
    let writers = (0..WRITERS)
        .map(|index| {
            let database = Arc::clone(&database);
            let barrier = Arc::clone(&barrier);
            std::thread::spawn(move || {
                barrier.wait();
                database
                    .execute(
                        "INSERT INTO notes(body) VALUES (?1)",
                        [format!("writer-{index}")],
                    )
                    .unwrap();
            })
        })
        .collect::<Vec<_>>();
    for writer in writers {
        writer.join().unwrap();
    }

    let ids = database
        .query("SELECT id FROM notes", (), |row| row.get::<_, i64>(0))
        .unwrap();
    assert_eq!(ids.len(), WRITERS);
    assert_eq!(ids.iter().copied().collect::<BTreeSet<_>>().len(), WRITERS);
    assert!(ids.iter().all(|id| *id >= 1_i64 << 47));
}

#[test]
fn explicit_low_rowids_coexist_with_generated_device_ids() {
    let directory = tempfile::tempdir().unwrap();
    let database =
        MultiliteConnection::open(directory.path().join("reserved-namespace.sqlite")).unwrap();
    database
        .execute(
            "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT NOT NULL)",
            (),
        )
        .unwrap();
    database
        .execute(
            "INSERT INTO notes VALUES
                (1, 'one'),
                (42, 'forty-two'),
                (140737488355327, 'last-reserved')",
            (),
        )
        .unwrap();

    database
        .execute("INSERT INTO notes(body) VALUES ('generated')", ())
        .unwrap();
    let generated = database
        .query("SELECT id FROM notes WHERE body = 'generated'", (), |row| {
            row.get::<_, i64>(0)
        })
        .unwrap()[0];
    assert!(generated >= 1_i64 << 47);
    assert_eq!(
        database
            .query("SELECT count(*) FROM notes", (), |row| row.get::<_, i64>(0))
            .unwrap(),
        [4]
    );
}

#[test]
fn async_writes_allocate_rowids_without_blocking_the_async_api() {
    pollster::block_on(async {
        let directory = tempfile::tempdir().unwrap();
        let database = MultiliteConnection::open_async(directory.path().join("async-rowid.sqlite"))
            .await
            .unwrap();
        database
            .execute_async(
                "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT NOT NULL)",
                (),
            )
            .await
            .unwrap();
        database
            .execute_async("INSERT INTO notes(body) VALUES ('async')", ())
            .await
            .unwrap();
        let id = database
            .query_async("SELECT id FROM notes", (), |row| row.get::<_, i64>(0))
            .await
            .unwrap()[0];
        assert!(id >= 1_i64 << 47);
    });
}

#[test]
fn reopen_rejects_missing_or_malformed_allocator_metadata() {
    for (name, corrupt) in [
        ("missing-slot", "DELETE FROM __multilite__rowid_slots"),
        (
            "malformed-state",
            "UPDATE __multilite__rowid_state SET active_slot = 1",
        ),
        ("partial-namespace", "DROP TABLE __multilite__rowid_slots"),
    ] {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join(format!("{name}.sqlite"));
        let database = MultiliteConnection::open(&path).unwrap();
        drop(database);
        rusqlite::Connection::open(&path)
            .unwrap()
            .execute_batch(corrupt)
            .unwrap();

        assert!(matches!(
            MultiliteConnection::open(&path),
            Err(Error::InvalidDatabase(_))
        ));
    }
}

#[test]
fn rolled_back_ids_are_not_reused_and_restart_burns_only_the_lease_remainder() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("restart.sqlite");
    let first_id;
    {
        let database = MultiliteConnection::open(&path).unwrap();
        database
            .execute("CREATE TABLE notes (id INTEGER PRIMARY KEY)", ())
            .unwrap();
        database
            .execute("INSERT INTO notes DEFAULT VALUES", ())
            .unwrap();
        first_id = database
            .query("SELECT id FROM notes", (), |row| row.get::<_, i64>(0))
            .unwrap()[0];

        let result: Result<()> = database.update(|transaction| {
            transaction.execute("INSERT INTO notes DEFAULT VALUES", ())?;
            Err(Error::CaptureInvariant("injected rollback"))
        });
        assert!(result.is_err());
        database
            .execute("INSERT INTO notes DEFAULT VALUES", ())
            .unwrap();
        let ids = database
            .query("SELECT id FROM notes ORDER BY id", (), |row| {
                row.get::<_, i64>(0)
            })
            .unwrap();
        assert_eq!(ids, [first_id, first_id + 2]);
    }

    let database = MultiliteConnection::open(&path).unwrap();
    database
        .execute("INSERT INTO notes DEFAULT VALUES", ())
        .unwrap();
    let newest = database
        .query("SELECT max(id) FROM notes", (), |row| row.get::<_, i64>(0))
        .unwrap()[0];
    assert_eq!(newest, first_id + 1_024);
}

#[test]
fn two_devices_allocate_distinct_ipks_that_admit_and_converge() {
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
    assert_eq!(first.push().unwrap(), PushOutcome::Drained);
    second.pull().unwrap();
    second.rebase().unwrap();

    first
        .execute("INSERT INTO notes (body) VALUES ('first')", ())
        .unwrap();
    second
        .execute("INSERT INTO notes (body) VALUES ('second')", ())
        .unwrap();
    let first_id = first
        .query("SELECT id FROM notes", (), |row| row.get::<_, i64>(0))
        .unwrap()[0];
    let second_id = second
        .query("SELECT id FROM notes", (), |row| row.get::<_, i64>(0))
        .unwrap()[0];
    assert_ne!(first_id, second_id);
    assert!(first_id >= 1_i64 << 47);
    assert!(second_id >= 1_i64 << 47);

    assert_eq!(first.push().unwrap(), PushOutcome::Drained);
    assert_eq!(second.push().unwrap(), PushOutcome::Drained);
    first.pull().unwrap();
    second.pull().unwrap();
    first.rebase().unwrap();
    second.rebase().unwrap();

    assert_eq!(rows(&first), rows(&second));
    assert_eq!(
        rows(&first)
            .into_iter()
            .map(|(_, body)| body)
            .collect::<Vec<_>>(),
        ["first", "second"]
    );
}
