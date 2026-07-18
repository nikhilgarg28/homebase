use multilite::{Error, MultiliteConnection};
use rusqlite::Connection;

#[test]
fn create_select_and_insert_work_for_arbitrary_user_tables() {
    let directory = tempfile::tempdir().unwrap();
    let db = MultiliteConnection::open(directory.path().join("surface.sqlite")).unwrap();

    db.execute(
        "CREATE TABLE notes (
            id INTEGER PRIMARY KEY,
            body TEXT NOT NULL UNIQUE
        )",
        (),
    )
    .unwrap();
    assert_eq!(
        db.execute(
            "INSERT INTO notes (body) VALUES ('one'), ('two'), ('three')",
            (),
        )
        .unwrap(),
        3
    );

    let mut statement = db
        .prepare("SELECT id, upper(body) FROM notes ORDER BY id")
        .unwrap();
    assert_eq!(
        statement
            .query_map((), |row| {
                Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
            })
            .unwrap(),
        [(1, "ONE".into()), (2, "TWO".into()), (3, "THREE".into())]
    );
}

#[test]
fn unsupported_verbs_transactions_and_multiple_statements_are_rejected() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("rejected.sqlite");
    let db = MultiliteConnection::open(&path).unwrap();
    db.execute("CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)", ())
        .unwrap();
    db.execute("INSERT INTO notes VALUES (1, 'original')", ())
        .unwrap();

    for sql in [
        "UPDATE notes SET body = 'updated' WHERE id = 1",
        "DELETE FROM notes WHERE id = 1",
        "ALTER TABLE notes ADD COLUMN extra TEXT",
        "DROP TABLE notes",
        "CREATE INDEX notes_body ON notes(body)",
        "CREATE VIEW note_view AS SELECT * FROM notes",
        "PRAGMA user_version = 9",
        "ATTACH DATABASE ':memory:' AS attached",
        "VACUUM",
        "ANALYZE",
        "REINDEX",
        "BEGIN",
        "COMMIT",
        "ROLLBACK",
        "SAVEPOINT caller_owned",
        "CREATE TABLE partial (value INTEGER); INSERT INTO partial VALUES (1)",
    ] {
        assert!(
            db.execute(sql, ()).is_err(),
            "statement was accepted: {sql}"
        );
    }

    assert_eq!(read_note(&db), "original");
    drop(db);

    let stock = Connection::open(path).unwrap();
    assert_eq!(
        stock
            .query_row("PRAGMA user_version", (), |row| row.get::<_, i64>(0))
            .unwrap(),
        0
    );
    assert_eq!(
        stock
            .query_row(
                "SELECT count(*) FROM sqlite_schema
                 WHERE name IN ('partial', 'note_view', 'notes_body')",
                (),
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        0
    );
}

#[test]
fn replace_and_every_insert_conflict_clause_are_rejected_without_mutation() {
    let directory = tempfile::tempdir().unwrap();
    let db = MultiliteConnection::open(directory.path().join("conflicts.sqlite")).unwrap();
    db.execute("CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)", ())
        .unwrap();
    db.execute("INSERT INTO notes VALUES (1, 'original')", ())
        .unwrap();

    for sql in [
        "REPLACE INTO notes VALUES (1, 'replaced')",
        "INSERT OR REPLACE INTO notes VALUES (1, 'replaced')",
        "INSERT OR IGNORE INTO notes VALUES (1, 'ignored')",
        "INSERT INTO notes VALUES (1, 'updated')
         ON CONFLICT(id) DO UPDATE SET body = excluded.body",
        "INSERT INTO notes VALUES (1, 'ignored') ON CONFLICT DO NOTHING",
    ] {
        assert!(matches!(db.execute(sql, ()), Err(Error::UnsupportedSql(_))));
        assert_eq!(read_note(&db), "original");
    }
}

#[test]
fn autoincrement_and_schema_conflict_policies_are_rejected_without_schema_changes() {
    let directory = tempfile::tempdir().unwrap();
    let db = MultiliteConnection::open(directory.path().join("schema-options.sqlite")).unwrap();

    for sql in [
        "CREATE TABLE auto_notes (id INTEGER PRIMARY KEY AUTOINCREMENT)",
        "CREATE TABLE replacing_notes (
            id INTEGER PRIMARY KEY,
            body TEXT UNIQUE ON CONFLICT REPLACE
        )",
        "CREATE TABLE ignoring_notes (
            id INTEGER PRIMARY KEY,
            body TEXT NOT NULL ON CONFLICT IGNORE
        )",
    ] {
        assert!(matches!(db.execute(sql, ()), Err(Error::UnsupportedSql(_))));
    }

    let mut statement = db
        .prepare(
            "SELECT count(*) FROM sqlite_schema
             WHERE name IN ('auto_notes', 'replacing_notes', 'ignoring_notes', 'sqlite_sequence')",
        )
        .unwrap();
    assert_eq!(
        statement.query_map((), |row| row.get::<_, i64>(0)).unwrap(),
        [0]
    );
}

#[test]
fn public_sql_cannot_access_or_create_reserved_tables() {
    let directory = tempfile::tempdir().unwrap();
    let db = MultiliteConnection::open(directory.path().join("reserved.sqlite")).unwrap();

    assert!(db.prepare("SELECT value FROM __multilite__meta").is_err());
    assert!(
        db.execute(
            "INSERT INTO __multilite__v1_schema (singleton, version) VALUES (1, 99)",
            (),
        )
        .is_err()
    );
    assert!(
        db.execute("CREATE TABLE __multilite__application (value BLOB)", (),)
            .is_err()
    );
    assert!(
        db.execute("CREATE TABLE \"__MULTILITE__application\" (value BLOB)", (),)
            .is_err()
    );
    assert!(
        db.execute(
            "CREATE TABLE IF NOT EXISTS __multilite__meta (key BLOB, value BLOB)",
            (),
        )
        .is_err()
    );

    let mut statement = db
        .prepare("SELECT count(*) FROM sqlite_schema WHERE name GLOB '__multilite__*'")
        .unwrap();
    assert_eq!(
        statement.query_map((), |row| row.get::<_, i64>(0)).unwrap(),
        [2]
    );
}

fn read_note(db: &MultiliteConnection) -> String {
    let mut statement = db.prepare("SELECT body FROM notes WHERE id = 1").unwrap();
    statement
        .query_map((), |row| row.get::<_, String>(0))
        .unwrap()
        .into_iter()
        .next()
        .unwrap()
}
