use std::panic::{AssertUnwindSafe, catch_unwind};

use multilite::{Error, MultiliteConnection, PushOutcome, Result, params};
use rusqlite::{Connection as SqliteConnection, ErrorCode};

#[test]
fn execute_and_query_roundtrip_across_reopen() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("roundtrip.sqlite");

    {
        let db = MultiliteConnection::open(&path).unwrap();
        db.execute(
            "CREATE TABLE values_v1 (id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
            (),
        )
        .unwrap();
        db.execute(
            "INSERT INTO values_v1 (id, name) VALUES (?1, ?2)",
            (7_i64, "seven"),
        )
        .unwrap();
    }

    let db = MultiliteConnection::open(&path).unwrap();
    let mut statement = db
        .prepare("SELECT name FROM values_v1 WHERE id = ?1")
        .unwrap();
    let names = statement
        .query_map([7_i64], |row| row.get::<_, String>(0))
        .unwrap();

    assert_eq!(names, ["seven"]);
}

#[test]
fn query_map_supports_normal_rusqlite_parameters_and_conversions() {
    let directory = tempfile::tempdir().unwrap();
    let db = MultiliteConnection::open(directory.path().join("params.sqlite")).unwrap();
    db.execute(
        "CREATE TABLE values_v1 (id INTEGER PRIMARY KEY, payload BLOB)",
        (),
    )
    .unwrap();
    db.execute(
        "INSERT INTO values_v1 (id, payload) VALUES (?1, ?2)",
        params![11_i64, b"payload"],
    )
    .unwrap();

    let mut statement = db
        .prepare("SELECT id, payload FROM values_v1 WHERE id = ?1")
        .unwrap();
    let rows = statement
        .query_map([11_i64], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, Vec<u8>>(1)?))
        })
        .unwrap();

    assert_eq!(rows, [(11, b"payload".to_vec())]);
}

#[test]
fn prepare_rejects_writes_without_mutating() {
    let directory = tempfile::tempdir().unwrap();
    let db = MultiliteConnection::open(directory.path().join("readonly.sqlite")).unwrap();
    db.execute(
        "CREATE TABLE values_v1 (id INTEGER PRIMARY KEY, value INTEGER)",
        (),
    )
    .unwrap();

    let error = match db.prepare("INSERT INTO values_v1 (value) VALUES (1)") {
        Ok(_) => panic!("write statement unexpectedly prepared"),
        Err(error) => error,
    };
    assert!(matches!(error, Error::PreparedWrite));

    let mut statement = db.prepare("SELECT count(*) FROM values_v1").unwrap();
    assert_eq!(
        statement.query_map((), |row| row.get::<_, i64>(0)).unwrap(),
        [0]
    );
}

#[test]
fn sqlite_constraint_error_retains_extended_detail() {
    let directory = tempfile::tempdir().unwrap();
    let db = MultiliteConnection::open(directory.path().join("constraint.sqlite")).unwrap();
    db.execute("CREATE TABLE values_v1 (id INTEGER PRIMARY KEY)", ())
        .unwrap();
    db.execute("INSERT INTO values_v1 (id) VALUES (1)", ())
        .unwrap();

    let error = db
        .execute("INSERT INTO values_v1 (id) VALUES (1)", ())
        .unwrap_err();
    let Error::Sqlite(rusqlite::Error::SqliteFailure(code, message)) = error else {
        panic!("unexpected error shape: {error:?}");
    };

    assert_eq!(code.code, ErrorCode::ConstraintViolation);
    assert!(code.extended_code > code.code as i32);
    assert!(message.is_some_and(|message| message.contains("UNIQUE constraint failed")));
}

#[test]
fn query_conversion_error_remains_a_sqlite_error() {
    let directory = tempfile::tempdir().unwrap();
    let db = MultiliteConnection::open(directory.path().join("conversion.sqlite")).unwrap();
    let mut statement = db.prepare("SELECT 'not an integer'").unwrap();

    let error = statement
        .query_map((), |row| row.get::<_, i64>(0))
        .unwrap_err();

    assert!(matches!(
        error,
        Error::Sqlite(rusqlite::Error::InvalidColumnType(..))
    ));
}

#[test]
fn empty_push_drains_without_contacting_an_offline_server() {
    let directory = tempfile::tempdir().unwrap();
    let db = MultiliteConnection::open(directory.path().join("empty-push.sqlite")).unwrap();

    assert_eq!(db.push().unwrap(), PushOutcome::Drained);
}

#[test]
fn managed_update_and_view_share_sqlite_shaped_query_methods() {
    let directory = tempfile::tempdir().unwrap();
    let db = MultiliteConnection::open(directory.path().join("managed.sqlite")).unwrap();

    let inserted = db
        .update(|tx| {
            tx.execute(
                "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT NOT NULL)",
                (),
            )?;
            let mut count = tx.prepare("SELECT count(*) FROM notes")?;
            assert_eq!(count.query_map((), |row| row.get::<_, i64>(0))?, [0]);
            tx.execute("INSERT INTO notes VALUES (1, 'one'), (2, 'two')", ())?;
            assert_eq!(count.query_map((), |row| row.get::<_, i64>(0))?, [2]);
            let visible = tx.query("SELECT id FROM notes ORDER BY id", (), |row| {
                row.get::<_, i64>(0)
            })?;
            assert_eq!(visible, [1, 2]);

            let mut statement = tx.prepare("SELECT body FROM notes WHERE id = ?1")?;
            assert_eq!(
                statement.query_map([2_i64], |row| row.get::<_, String>(0))?,
                ["two"]
            );
            Ok(visible.len())
        })
        .unwrap();
    assert_eq!(inserted, 2);

    let rows = db
        .view(|tx| {
            tx.query("SELECT id, body FROM notes ORDER BY id", (), |row| {
                Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
            })
        })
        .unwrap();
    assert_eq!(rows, [(1, "one".into()), (2, "two".into())]);
    assert_eq!(
        db.query("SELECT count(*) FROM notes", (), |row| row.get::<_, i64>(0))
            .unwrap(),
        [2]
    );
}

#[test]
fn managed_update_rolls_back_on_error_and_panic_and_remains_usable() {
    let directory = tempfile::tempdir().unwrap();
    let db = MultiliteConnection::open(directory.path().join("managed-rollback.sqlite")).unwrap();

    let error = db
        .update(|tx| {
            tx.execute("CREATE TABLE errored (id INTEGER PRIMARY KEY)", ())?;
            Err::<(), _>(Error::CaptureInvariant("injected closure error"))
        })
        .unwrap_err();
    assert!(matches!(error, Error::CaptureInvariant(_)));

    let panic = catch_unwind(AssertUnwindSafe(|| {
        let _ = db.update(|tx| -> Result<()> {
            tx.execute("CREATE TABLE panicked (id INTEGER PRIMARY KEY)", ())?;
            panic!("injected closure panic")
        });
    }));
    assert!(panic.is_err());

    for table in ["errored", "panicked"] {
        assert_eq!(
            db.query(
                "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE name = ?1)",
                [table],
                |row| row.get::<_, bool>(0),
            )
            .unwrap(),
            [false]
        );
    }
    db.update(|tx| {
        tx.execute("CREATE TABLE committed (id INTEGER PRIMARY KEY)", ())?;
        Ok(())
    })
    .unwrap();
}

#[test]
fn managed_transactions_own_transaction_control_and_keep_views_read_only() {
    let directory = tempfile::tempdir().unwrap();
    let db = MultiliteConnection::open(directory.path().join("managed-control.sqlite")).unwrap();

    for sql in [
        "BEGIN",
        "COMMIT",
        "ROLLBACK",
        "SAVEPOINT nested",
        "RELEASE nested",
    ] {
        assert!(matches!(
            db.update(|tx| tx.execute(sql, ())),
            Err(Error::UnsupportedSql(
                "transaction control is owned by the managed closure"
            ))
        ));
    }
    assert!(matches!(
        db.view(|tx| tx.prepare("BEGIN").map(|_| ())),
        Err(Error::UnsupportedSql(
            "transaction control is owned by the managed closure"
        ))
    ));

    db.execute("CREATE TABLE notes (id INTEGER PRIMARY KEY)", ())
        .unwrap();
    assert!(matches!(
        db.view(|tx| tx.prepare("INSERT INTO notes VALUES (1)").map(|_| ())),
        Err(Error::PreparedWrite)
    ));
    assert_eq!(
        db.query("SELECT count(*) FROM notes", (), |row| row.get::<_, i64>(0))
            .unwrap(),
        [0]
    );
}

#[test]
fn managed_transactions_pin_their_snapshot_before_the_closure_runs() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("pinned-snapshot.sqlite");
    let setup = SqliteConnection::open(&path).unwrap();
    assert_eq!(
        setup
            .query_row("PRAGMA journal_mode = WAL", (), |row| {
                row.get::<_, String>(0)
            })
            .unwrap()
            .to_ascii_lowercase(),
        "wal"
    );
    drop(setup);

    let db = MultiliteConnection::open(&path).unwrap();
    db.execute("CREATE TABLE notes (id INTEGER PRIMARY KEY)", ())
        .unwrap();
    let writer = SqliteConnection::open(&path).unwrap();

    let viewed = db
        .view(|tx| {
            writer.execute("INSERT INTO notes VALUES (1)", ())?;
            tx.query("SELECT count(*) FROM notes", (), |row| row.get::<_, i64>(0))
        })
        .unwrap();
    assert_eq!(viewed, [0]);

    let updated = db
        .update(|tx| {
            writer.execute("INSERT INTO notes VALUES (2)", ())?;
            tx.query("SELECT count(*) FROM notes", (), |row| row.get::<_, i64>(0))
        })
        .unwrap();
    assert_eq!(updated, [1]);
    assert_eq!(
        db.query("SELECT count(*) FROM notes", (), |row| row.get::<_, i64>(0))
            .unwrap(),
        [2]
    );
}
