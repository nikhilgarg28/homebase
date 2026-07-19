use multilite::{Error, MultiliteConnection, params};
use rusqlite::ErrorCode;

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
