use rusqlite::{Connection, params};

fn ids(connection: &Connection) -> Vec<i64> {
    connection
        .prepare("SELECT id FROM items ORDER BY id")
        .unwrap()
        .query_map((), |row| row.get(0))
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap()
}

#[test]
fn bundled_sqlite_supports_limited_updates_and_deletes() {
    let connection = Connection::open_in_memory().unwrap();
    assert_eq!(
        connection
            .query_row(
                "SELECT sqlite_compileoption_used('ENABLE_UPDATE_DELETE_LIMIT')",
                (),
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        1
    );
    connection
        .execute_batch(
            "CREATE TABLE items (id INTEGER PRIMARY KEY, score INTEGER NOT NULL);
             INSERT INTO items VALUES (1, 10), (2, 40), (3, 30), (4, 20);",
        )
        .unwrap();

    assert_eq!(
        connection
            .execute(
                "UPDATE items SET score = score + 100
                 ORDER BY score DESC, id LIMIT ?1 OFFSET ?2",
                params![2, 1],
            )
            .unwrap(),
        2
    );
    assert_eq!(
        connection
            .prepare("SELECT id, score FROM items ORDER BY id")
            .unwrap()
            .query_map((), |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)))
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap(),
        [(1, 10), (2, 40), (3, 130), (4, 120)]
    );

    assert_eq!(
        connection
            .execute("DELETE FROM items ORDER BY score DESC LIMIT 1", ())
            .unwrap(),
        1
    );
    assert_eq!(ids(&connection), [1, 2, 4]);
}

#[test]
fn order_by_requires_limit_for_native_writes() {
    let connection = Connection::open_in_memory().unwrap();
    connection
        .execute_batch(
            "CREATE TABLE items (id INTEGER PRIMARY KEY);
             INSERT INTO items VALUES (1), (2);",
        )
        .unwrap();

    for sql in [
        "UPDATE items SET id = id ORDER BY id",
        "DELETE FROM items ORDER BY id",
    ] {
        assert!(connection.execute(sql, ()).is_err(), "{sql}");
    }
    assert_eq!(ids(&connection), [1, 2]);
}
