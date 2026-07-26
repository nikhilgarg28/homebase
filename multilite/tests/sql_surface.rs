use multilite::{Error, MultiliteConnection};
use rusqlite::Connection;

#[test]
fn create_select_and_insert_work_for_arbitrary_user_tables() {
    let directory = tempfile::tempdir().unwrap();
    let db = MultiliteConnection::open(directory.path().join("surface.sqlite")).unwrap();

    db.execute(
        "CREATE TABLE notes (
            id INTEGER PRIMARY KEY,
            body TEXT NOT NULL
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
fn composite_primary_keys_and_without_rowid_support_full_row_lifecycle() {
    let directory = tempfile::tempdir().unwrap();
    let db = MultiliteConnection::open(directory.path().join("composite-primary.sqlite")).unwrap();

    db.execute(
        "CREATE TABLE memberships (
            tenant TEXT,
            member INTEGER,
            body TEXT NOT NULL,
            PRIMARY KEY (member, tenant)
        ) WITHOUT ROWID",
        (),
    )
    .unwrap();
    db.execute(
        "INSERT INTO memberships VALUES
            ('north', 2, 'two'),
            ('north', 1, 'one'),
            ('south', 1, 'other')",
        (),
    )
    .unwrap();
    assert_eq!(
        db.query(
            "SELECT tenant, member, body FROM memberships ORDER BY member, tenant",
            (),
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, String>(2)?,
                ))
            },
        )
        .unwrap(),
        [
            ("north".into(), 1, "one".into()),
            ("south".into(), 1, "other".into()),
            ("north".into(), 2, "two".into()),
        ]
    );

    db.execute(
        "UPDATE memberships
         SET member = 3, body = 'moved'
         WHERE tenant = 'north' AND member = 2",
        (),
    )
    .unwrap();
    db.execute(
        "DELETE FROM memberships WHERE tenant = 'south' AND member = 1",
        (),
    )
    .unwrap();
    assert_eq!(
        db.query(
            "SELECT tenant, member, body FROM memberships ORDER BY member, tenant",
            (),
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, String>(2)?,
                ))
            },
        )
        .unwrap(),
        [
            ("north".into(), 1, "one".into()),
            ("north".into(), 3, "moved".into()),
        ]
    );
    assert!(
        db.execute(
            "INSERT INTO memberships VALUES ('north', 1, 'duplicate')",
            (),
        )
        .is_err()
    );
}

#[test]
fn ordinary_composite_primary_keys_preserve_hidden_rowids() {
    let directory = tempfile::tempdir().unwrap();
    let db = MultiliteConnection::open(directory.path().join("composite-rowid.sqlite")).unwrap();
    db.execute(
        "CREATE TABLE documents (
            tenant TEXT NOT NULL,
            document INTEGER NOT NULL,
            body TEXT NOT NULL,
            PRIMARY KEY (document, tenant)
        )",
        (),
    )
    .unwrap();
    db.execute("INSERT INTO documents VALUES ('north', 1, 'original')", ())
        .unwrap();
    let rowid = db
        .query("SELECT rowid FROM documents", (), |row| {
            row.get::<_, i64>(0)
        })
        .unwrap()[0];

    db.execute(
        "UPDATE documents
         SET document = 2, body = 'moved'
         WHERE tenant = 'north' AND document = 1",
        (),
    )
    .unwrap();
    assert_eq!(
        db.query(
            "SELECT rowid, tenant, document, body FROM documents",
            (),
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, String>(3)?,
                ))
            },
        )
        .unwrap(),
        [(rowid, "north".into(), 2, "moved".into())]
    );
}

#[test]
fn delete_uses_sqlite_predicates_and_zero_rows_are_a_noop() {
    let directory = tempfile::tempdir().unwrap();
    let db = MultiliteConnection::open(directory.path().join("delete.sqlite")).unwrap();
    db.execute(
        "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT NOT NULL)",
        (),
    )
    .unwrap();
    db.execute(
        "INSERT INTO notes VALUES
            (1, 'keep'),
            (2, 'delete'),
            (3, 'also delete')",
        (),
    )
    .unwrap();

    assert_eq!(
        db.execute(
            "DELETE FROM notes
             WHERE id IN (
                SELECT id FROM notes
                WHERE body LIKE '%delete%'
             )",
            (),
        )
        .unwrap(),
        2
    );
    assert_eq!(
        db.execute("DELETE FROM notes WHERE id = ?1", [99_i64])
            .unwrap(),
        0
    );
    assert_eq!(
        db.query("SELECT id, body FROM notes", (), |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
        })
        .unwrap(),
        [(1, "keep".into())]
    );
    assert_eq!(db.execute("DELETE FROM notes", ()).unwrap(), 1);
    assert!(
        db.query("SELECT id FROM notes", (), |row| row.get::<_, i64>(0))
            .unwrap()
            .is_empty()
    );
}

#[test]
fn update_uses_sqlite_expressions_subqueries_and_complete_row_capture() {
    let directory = tempfile::tempdir().unwrap();
    let db = MultiliteConnection::open(directory.path().join("update.sqlite")).unwrap();
    db.update(|transaction| {
        transaction.execute(
            "CREATE TABLE notes (
                id INTEGER PRIMARY KEY,
                body TEXT,
                score REAL NOT NULL,
                payload BLOB
            )",
            (),
        )?;
        transaction.execute("CREATE TABLE selected (id INTEGER PRIMARY KEY)", ())?;
        transaction.execute(
            "INSERT INTO notes VALUES
                (1, 'one', 1.5, x'01'),
                (2, NULL, 2.5, NULL),
                (3, 'three', 3.5, x'03')",
            (),
        )?;
        transaction.execute("INSERT INTO selected VALUES (1), (2)", ())?;
        Ok(())
    })
    .unwrap();

    assert_eq!(
        db.execute(
            "UPDATE notes
             SET body = coalesce(upper(body), ?1),
                 score = score + ?2,
                 payload = CASE id WHEN 1 THEN x'0a0b' ELSE x'' END
             WHERE id IN (SELECT id FROM selected)",
            ("EMPTY", 0.25_f64),
        )
        .unwrap(),
        2
    );
    assert_eq!(
        db.query(
            "SELECT id, body, score, payload FROM notes ORDER BY id",
            (),
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, Option<String>>(1)?,
                    row.get::<_, f64>(2)?,
                    row.get::<_, Option<Vec<u8>>>(3)?,
                ))
            },
        )
        .unwrap(),
        [
            (1, Some("ONE".into()), 1.75, Some(vec![10, 11])),
            (2, Some("EMPTY".into()), 2.75, Some(Vec::new())),
            (3, Some("three".into()), 3.5, Some(vec![3])),
        ]
    );

    assert_eq!(
        db.execute("UPDATE notes SET body = body WHERE id = 3", ())
            .unwrap(),
        1
    );
    assert_eq!(
        db.execute("UPDATE notes SET body = 'missing' WHERE id = 99", ())
            .unwrap(),
        0
    );
}

#[test]
fn update_moves_primary_keys_but_rejects_hidden_rowid_changes_atomically() {
    let directory = tempfile::tempdir().unwrap();
    let db = MultiliteConnection::open(directory.path().join("update-identity.sqlite")).unwrap();
    db.update(|transaction| {
        transaction.execute(
            "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT NOT NULL)",
            (),
        )?;
        transaction.execute(
            "CREATE TABLE documents (
                id TEXT NOT NULL PRIMARY KEY,
                body TEXT NOT NULL
            )",
            (),
        )?;
        transaction.execute("INSERT INTO notes VALUES (1, 'original')", ())?;
        transaction.execute("INSERT INTO documents VALUES ('a', 'document')", ())?;
        Ok(())
    })
    .unwrap();

    let original_document_rowid = db
        .query("SELECT _rowid_ FROM documents", (), |row| {
            row.get::<_, i64>(0)
        })
        .unwrap()[0];
    assert_eq!(
        db.execute("UPDATE notes SET id = 2, body = 'changed' WHERE id = 1", ())
            .unwrap(),
        1
    );
    assert_eq!(
        db.execute("UPDATE notes SET rowid = 4 WHERE id = 2", ())
            .unwrap(),
        1
    );
    assert_eq!(
        db.execute(
            "UPDATE documents SET id = 'b', body = 'moved' WHERE id = 'a'",
            (),
        )
        .unwrap(),
        1
    );
    assert!(matches!(
        db.execute("UPDATE documents SET rowid = rowid + 1 WHERE id = 'b'", ()),
        Err(Error::UnsupportedSql(
            "UPDATE of SQLite rowid is not supported"
        ))
    ));
    assert_eq!(
        db.query("SELECT id, body FROM notes", (), |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
        })
        .unwrap(),
        [(4, "changed".into())]
    );
    assert_eq!(
        db.query("SELECT _rowid_, id, body FROM documents", (), |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
            ))
        })
        .unwrap(),
        [(original_document_rowid, "b".into(), "moved".into())]
    );

    db.execute("INSERT INTO notes VALUES (5, 'occupied')", ())
        .unwrap();
    assert!(
        db.execute(
            "UPDATE notes SET id = 5, body = 'collision' WHERE id = 4",
            ()
        )
        .is_err()
    );
    assert_eq!(
        db.query("SELECT id, body FROM notes ORDER BY id", (), |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
        })
        .unwrap(),
        [(4, "changed".into()), (5, "occupied".into())]
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
fn update_extensions_and_reserved_targets_are_rejected_without_mutation() {
    let directory = tempfile::tempdir().unwrap();
    let db = MultiliteConnection::open(directory.path().join("update-shape.sqlite")).unwrap();
    db.execute("CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)", ())
        .unwrap();
    db.execute("INSERT INTO notes VALUES (1, 'original')", ())
        .unwrap();

    for sql in [
        "WITH old AS (SELECT 1) UPDATE notes SET body = 'x'",
        "UPDATE OR REPLACE notes SET body = 'x'",
        "UPDATE main.notes SET body = 'x'",
        "UPDATE notes AS old SET body = 'x'",
        "UPDATE notes INDEXED BY sqlite_autoindex_notes_1 SET body = 'x'",
        "UPDATE notes NOT INDEXED SET body = 'x'",
        "UPDATE notes SET (id, body) = (2, 'x')",
        "UPDATE notes SET body = source.body FROM notes AS source",
        "UPDATE notes SET body = 'x' RETURNING id",
        "UPDATE notes SET body = 'x' ORDER BY id LIMIT 1",
        "UPDATE __multilite__pending SET record = x''",
        "UPDATE sqlite_schema SET sql = NULL",
    ] {
        assert!(
            matches!(db.execute(sql, ()), Err(Error::UnsupportedSql(_))),
            "statement was accepted: {sql}"
        );
        assert_eq!(read_note(&db), "original");
    }
}

#[test]
fn delete_extensions_and_reserved_targets_are_rejected_without_mutation() {
    let directory = tempfile::tempdir().unwrap();
    let db = MultiliteConnection::open(directory.path().join("delete-shape.sqlite")).unwrap();
    db.execute("CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)", ())
        .unwrap();
    db.execute("INSERT INTO notes VALUES (1, 'original')", ())
        .unwrap();

    for sql in [
        "WITH old AS (SELECT 1) DELETE FROM notes WHERE id IN old",
        "DELETE FROM main.notes",
        "DELETE FROM notes AS old",
        "DELETE FROM notes INDEXED BY sqlite_autoindex_notes_1",
        "DELETE FROM notes NOT INDEXED",
        "DELETE FROM notes RETURNING id",
        "DELETE FROM notes ORDER BY id LIMIT 1",
        "DELETE FROM __multilite__pending",
        "DELETE FROM sqlite_sequence",
    ] {
        assert!(
            matches!(db.execute(sql, ()), Err(Error::UnsupportedSql(_))),
            "statement was accepted: {sql}"
        );
        assert_eq!(read_note(&db), "original");
    }
}

#[test]
fn delete_from_an_adopted_table_rolls_back_without_a_schema_identity() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("adopted-delete.sqlite");
    let stock = Connection::open(&path).unwrap();
    stock
        .execute_batch(
            "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT);
             INSERT INTO notes VALUES (1, 'original')",
        )
        .unwrap();
    drop(stock);

    let db = MultiliteConnection::open(&path).unwrap();
    assert!(matches!(
        db.execute("DELETE FROM notes WHERE id = 1", ()),
        Err(Error::UnsupportedSql(
            "DELETE target has no synchronized schema identity"
        ))
    ));
    assert_eq!(read_note(&db), "original");
}

#[test]
fn update_of_an_adopted_table_rolls_back_without_a_schema_identity() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("adopted-update.sqlite");
    Connection::open(&path)
        .unwrap()
        .execute_batch(
            "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT);
             INSERT INTO notes VALUES (1, 'original')",
        )
        .unwrap();

    let db = MultiliteConnection::open(&path).unwrap();
    assert!(matches!(
        db.execute("UPDATE notes SET body = 'changed' WHERE id = 1", ()),
        Err(Error::UnsupportedSql(
            "UPDATE target has no synchronized schema identity"
        ))
    ));
    assert_eq!(read_note(&db), "original");
}

#[test]
fn caught_adopted_table_write_errors_rollback_their_statement_savepoints() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("caught-adopted-write.sqlite");
    Connection::open(&path)
        .unwrap()
        .execute_batch(
            "CREATE TABLE adopted (id INTEGER PRIMARY KEY, body TEXT);
             INSERT INTO adopted VALUES (1, 'original')",
        )
        .unwrap();
    let db = MultiliteConnection::open(&path).unwrap();
    db.execute(
        "CREATE TABLE owned (id INTEGER PRIMARY KEY, body TEXT NOT NULL)",
        (),
    )
    .unwrap();

    db.update(|transaction| {
        assert!(matches!(
            transaction.execute("DELETE FROM adopted WHERE id = 1", ()),
            Err(Error::UnsupportedSql(
                "DELETE target has no synchronized schema identity"
            ))
        ));
        assert!(matches!(
            transaction.execute("INSERT INTO adopted VALUES (2, 'untracked')", ()),
            Err(Error::UnsupportedSql(
                "INSERT target has no synchronized schema identity"
            ))
        ));
        assert!(matches!(
            transaction.execute("UPDATE adopted SET body = 'changed' WHERE id = 1", ()),
            Err(Error::UnsupportedSql(
                "UPDATE target has no synchronized schema identity"
            ))
        ));
        transaction.execute("INSERT INTO owned VALUES (1, 'tracked')", ())?;
        Ok(())
    })
    .unwrap();

    assert_eq!(
        db.query("SELECT id, body FROM adopted", (), |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
        })
        .unwrap(),
        [(1, "original".into())]
    );
    assert_eq!(
        db.query("SELECT id, body FROM owned", (), |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
        })
        .unwrap(),
        [(1, "tracked".into())]
    );
}

#[test]
fn trigger_generated_delete_effects_abort_the_whole_statement() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("trigger-delete.sqlite");
    let db = MultiliteConnection::open(&path).unwrap();
    db.update(|transaction| {
        transaction.execute(
            "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT NOT NULL)",
            (),
        )?;
        transaction.execute(
            "CREATE TABLE audit (id INTEGER PRIMARY KEY, body TEXT NOT NULL)",
            (),
        )?;
        transaction.execute("INSERT INTO notes VALUES (1, 'note')", ())?;
        transaction.execute("INSERT INTO audit VALUES (1, 'audit')", ())?;
        Ok(())
    })
    .unwrap();
    drop(db);

    Connection::open(&path)
        .unwrap()
        .execute_batch(
            "CREATE TRIGGER delete_audit
             AFTER DELETE ON notes
             BEGIN
                 DELETE FROM audit WHERE id = old.id;
             END",
        )
        .unwrap();
    let db = MultiliteConnection::open(&path).unwrap();

    assert!(matches!(
        db.execute("DELETE FROM notes WHERE id = 1", ()),
        Err(Error::CaptureInvariant(
            "writes caused by triggers are not supported"
        ))
    ));
    for table in ["notes", "audit"] {
        assert_eq!(
            db.query(&format!("SELECT count(*) FROM {table}"), (), |row| row
                .get::<_, i64>(0),)
                .unwrap(),
            [1]
        );
    }
}

#[test]
fn trigger_generated_update_effects_abort_the_whole_statement() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("trigger-update.sqlite");
    let db = MultiliteConnection::open(&path).unwrap();
    db.update(|transaction| {
        transaction.execute(
            "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT NOT NULL)",
            (),
        )?;
        transaction.execute(
            "CREATE TABLE audit (id INTEGER PRIMARY KEY, body TEXT NOT NULL)",
            (),
        )?;
        transaction.execute("INSERT INTO notes VALUES (1, 'note')", ())?;
        transaction.execute("INSERT INTO audit VALUES (1, 'audit')", ())?;
        Ok(())
    })
    .unwrap();
    drop(db);

    Connection::open(&path)
        .unwrap()
        .execute_batch(
            "CREATE TRIGGER update_audit
             AFTER UPDATE ON notes
             BEGIN
                 UPDATE audit SET body = new.body WHERE id = new.id;
             END",
        )
        .unwrap();
    let db = MultiliteConnection::open(&path).unwrap();

    assert!(matches!(
        db.execute("UPDATE notes SET body = 'changed' WHERE id = 1", ()),
        Err(Error::CaptureInvariant(
            "writes caused by triggers are not supported"
        ))
    ));
    assert_eq!(read_note(&db), "note");
    assert_eq!(
        db.query("SELECT body FROM audit WHERE id = 1", (), |row| {
            row.get::<_, String>(0)
        })
        .unwrap(),
        ["audit"]
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
    let internal_tables = || {
        let mut statement = db
            .prepare("SELECT count(*) FROM sqlite_schema WHERE name GLOB '__multilite__*'")
            .unwrap();
        statement.query_map((), |row| row.get::<_, i64>(0)).unwrap()[0]
    };
    let initial_internal_tables = internal_tables();

    assert!(db.prepare("SELECT value FROM __multilite__meta").is_err());
    assert!(
        db.execute(
            "INSERT INTO __multilite__schema (schema_name, table_name) VALUES ('main', x'01')",
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

    assert_eq!(internal_tables(), initial_internal_tables);
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
