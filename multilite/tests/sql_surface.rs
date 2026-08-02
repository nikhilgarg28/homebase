use std::path::Path;

use multilite::{Error, MultiliteConnection};
use rusqlite::types::Value;
use rusqlite::{Connection, params};

fn local_write_state(path: &Path) -> (i64, String, Option<String>) {
    let raw = Connection::open(path).unwrap();
    (
        raw.query_row("SELECT count(*) FROM __multilite__pending", (), |row| {
            row.get::<_, i64>(0)
        })
        .unwrap(),
        raw.query_row(
            "SELECT hex(commit_seq) FROM __multilite__commit_state WHERE singleton = 1",
            (),
            |row| row.get::<_, String>(0),
        )
        .unwrap(),
        raw.query_row(
            "SELECT max(hex(device_seq)) FROM __multilite__pending",
            (),
            |row| row.get::<_, Option<String>>(0),
        )
        .unwrap(),
    )
}

#[test]
fn user_version_is_a_durable_write_and_schema_pragmas_are_explicit_reads() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("pragma.sqlite");
    let db = MultiliteConnection::open(&path).unwrap();
    db.execute("CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)", ())
        .unwrap();
    db.execute("CREATE INDEX notes_body ON notes(body)", ())
        .unwrap();
    db.execute("PRAGMA user_version = -17", ()).unwrap();
    assert_eq!(
        db.query("PRAGMA user_version", (), |row| row.get::<_, i32>(0))
            .unwrap(),
        [-17]
    );
    assert_eq!(
        db.query("PRAGMA table_info(notes)", (), |row| row
            .get::<_, String>(1))
            .unwrap(),
        ["id", "body"]
    );
    assert_eq!(
        db.query("PRAGMA index_list(notes)", (), |row| row
            .get::<_, String>(1))
            .unwrap(),
        ["notes_body"]
    );
    assert!(matches!(
        db.query("PRAGMA journal_mode", (), |_| Ok(())),
        Err(Error::UnsupportedSql(_))
    ));
    let pending = local_write_state(&path).0;
    db.execute("PRAGMA user_version = -17", ()).unwrap();
    assert_eq!(local_write_state(&path).0, pending);
    drop(db);

    let reopened = MultiliteConnection::open(&path).unwrap();
    assert_eq!(
        reopened
            .query("PRAGMA main.user_version", (), |row| row.get::<_, i32>(0))
            .unwrap(),
        [-17]
    );
}

#[test]
fn views_query_drop_repair_and_survive_reopen() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("views.sqlite");
    let db = MultiliteConnection::open(&path).unwrap();
    db.execute("CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)", ())
        .unwrap();
    db.execute("INSERT INTO notes VALUES (1, 'one'), (2, 'two')", ())
        .unwrap();
    db.execute(
        "CREATE VIEW upper_notes (id, body) AS
         SELECT id, upper(body) FROM notes WHERE id >= 2",
        (),
    )
    .unwrap();
    assert_eq!(
        db.query("SELECT id, body FROM upper_notes", (), |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
        })
        .unwrap(),
        [(2, "TWO".into())]
    );
    drop(db);

    let db = MultiliteConnection::open(&path).unwrap();
    assert_eq!(
        db.query("SELECT body FROM upper_notes", (), |row| row
            .get::<_, String>(0))
            .unwrap(),
        ["TWO"]
    );
    assert!(matches!(
        db.execute("DROP TABLE notes", ()),
        Err(Error::UnsupportedSql(
            "table is referenced by a synchronized view"
        ))
    ));
    assert!(matches!(
        db.execute("ALTER TABLE notes DROP COLUMN body", ()),
        Err(Error::UnsupportedSql(
            "table is referenced by a synchronized view"
        ))
    ));
    db.execute("DROP VIEW upper_notes", ()).unwrap();
    db.execute("DROP TABLE notes", ()).unwrap();
    assert!(
        db.query("SELECT * FROM upper_notes", (), |_| Ok(()))
            .is_err()
    );
}

#[test]
fn views_track_multiple_nested_sources_and_release_ddl_fences_on_drop() {
    let directory = tempfile::tempdir().unwrap();
    let database = MultiliteConnection::open(directory.path().join("nested-views.sqlite")).unwrap();
    database
        .update(|transaction| {
            transaction.execute("CREATE TABLE teams (id INTEGER PRIMARY KEY, name TEXT)", ())?;
            transaction.execute(
                "CREATE TABLE notes (
                    id INTEGER PRIMARY KEY,
                    team_id INTEGER,
                    body TEXT
                )",
                (),
            )?;
            transaction.execute("INSERT INTO teams VALUES (1, 'core')", ())?;
            transaction.execute("INSERT INTO notes VALUES (7, 1, 'ready')", ())?;
            transaction.execute(
                "CREATE VIEW team_notes AS
                 WITH selected AS (SELECT team_id, body FROM notes WHERE id = 7)
                 SELECT teams.name, selected.body
                 FROM teams JOIN selected ON selected.team_id = teams.id",
                (),
            )?;
            Ok(())
        })
        .unwrap();
    assert_eq!(
        database
            .query("SELECT name, body FROM team_notes", (), |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })
            .unwrap(),
        [("core".into(), "ready".into())]
    );
    for sql in [
        "DROP TABLE teams",
        "ALTER TABLE teams DROP COLUMN name",
        "DROP TABLE notes",
    ] {
        assert!(matches!(
            database.execute(sql, ()),
            Err(Error::UnsupportedSql(
                "table is referenced by a synchronized view"
            ))
        ));
    }
    database.execute("DROP VIEW team_notes", ()).unwrap();
    database
        .execute("ALTER TABLE teams DROP COLUMN name", ())
        .unwrap();
    database.execute("DROP TABLE notes", ()).unwrap();
}

#[test]
fn add_column_references_folds_relationships_and_enforces_actions_after_reopen() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("add-column-references.sqlite");
    let database = MultiliteConnection::open(&path).unwrap();
    database
        .update(|transaction| {
            transaction.execute(
                "CREATE TABLE parents (
                    id INTEGER PRIMARY KEY,
                    code TEXT NOT NULL UNIQUE
                )",
                (),
            )?;
            transaction.execute(
                "CREATE TABLE children (id INTEGER PRIMARY KEY, body TEXT)",
                (),
            )?;
            transaction.execute("INSERT INTO parents VALUES (1, 'one'), (2, 'two')", ())?;
            transaction.execute("INSERT INTO children VALUES (10, 'existing')", ())?;
            transaction.execute(
                "ALTER TABLE children ADD COLUMN parent_code TEXT
                 CONSTRAINT children_parent REFERENCES parents(code)
                 ON DELETE CASCADE ON UPDATE SET NULL",
                (),
            )?;
            Ok(())
        })
        .unwrap();
    assert_eq!(
        database
            .query(
                "SELECT id, parent_code FROM children ORDER BY id",
                (),
                |row| Ok((row.get::<_, i64>(0)?, row.get::<_, Option<String>>(1)?)),
            )
            .unwrap(),
        [(10, None)]
    );
    database
        .execute("INSERT INTO children VALUES (11, 'valid', 'one')", ())
        .unwrap();
    assert!(
        database
            .execute("INSERT INTO children VALUES (12, 'invalid', 'missing')", ())
            .is_err()
    );
    database
        .execute("UPDATE parents SET code = 'renamed' WHERE id = 1", ())
        .unwrap();
    assert_eq!(
        database
            .query(
                "SELECT parent_code FROM children WHERE id = 11",
                (),
                |row| { row.get::<_, Option<String>>(0) }
            )
            .unwrap(),
        [None]
    );
    database
        .execute("INSERT INTO children VALUES (13, 'cascade', 'two')", ())
        .unwrap();
    database
        .execute("DELETE FROM parents WHERE id = 2", ())
        .unwrap();
    assert_eq!(
        database
            .query("SELECT id FROM children ORDER BY id", (), |row| {
                row.get::<_, i64>(0)
            })
            .unwrap(),
        [10, 11]
    );
    drop(database);

    let reopened = MultiliteConnection::open(&path).unwrap();
    assert_eq!(
        reopened
            .query("PRAGMA foreign_key_list(children)", (), |row| {
                Ok((
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, String>(5)?,
                    row.get::<_, String>(6)?,
                ))
            })
            .unwrap(),
        [(
            "parents".into(),
            "parent_code".into(),
            "code".into(),
            "SET NULL".into(),
            "CASCADE".into()
        )]
    );
    assert!(
        reopened
            .execute("INSERT INTO children VALUES (14, 'invalid', 'missing')", ())
            .is_err()
    );

    for sql in [
        "ALTER TABLE children ADD COLUMN missing INTEGER REFERENCES absent(id)",
        "ALTER TABLE children ADD COLUMN wrong BLOB REFERENCES parents(id)",
        "ALTER TABLE children ADD COLUMN composite TEXT REFERENCES parents(id, code)",
        "ALTER TABLE children ADD COLUMN self_id INTEGER REFERENCES children(id)",
    ] {
        assert!(
            reopened.execute(sql, ()).is_err(),
            "accepted invalid FK: {sql}"
        );
    }
}

#[test]
fn add_column_references_supports_strict_without_rowid_children_and_is_atomic_on_refusal() {
    let directory = tempfile::tempdir().unwrap();
    let database =
        MultiliteConnection::open(directory.path().join("add-fk-strict.sqlite")).unwrap();
    database
        .update(|transaction| {
            transaction.execute(
                "CREATE TABLE parents (
                    id INTEGER PRIMARY KEY,
                    code TEXT NOT NULL UNIQUE
                 ) STRICT",
                (),
            )?;
            transaction.execute(
                "CREATE TABLE children (
                    tenant TEXT NOT NULL,
                    child INTEGER NOT NULL,
                    body TEXT,
                    PRIMARY KEY (tenant, child)
                 ) WITHOUT ROWID, STRICT",
                (),
            )?;
            transaction.execute("INSERT INTO parents VALUES (1, 'one')", ())?;
            transaction.execute("INSERT INTO children VALUES ('north', 7, 'existing')", ())?;
            transaction.execute(
                "ALTER TABLE children ADD COLUMN parent_code TEXT
                 REFERENCES parents(code) ON DELETE SET NULL",
                (),
            )?;
            Ok(())
        })
        .unwrap();
    assert_eq!(
        database
            .query(
                "SELECT tenant, child, parent_code FROM children",
                (),
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, Option<String>>(2)?,
                    ))
                },
            )
            .unwrap(),
        [("north".into(), 7, None)]
    );
    database
        .execute(
            "INSERT INTO children VALUES ('south', 8, 'valid', 'one')",
            (),
        )
        .unwrap();
    assert!(
        database
            .execute(
                "INSERT INTO children VALUES ('west', 9, 'invalid', 'missing')",
                (),
            )
            .is_err()
    );

    database
        .execute(
            "CREATE TABLE refusing (id INTEGER PRIMARY KEY, body TEXT)",
            (),
        )
        .unwrap();
    database
        .execute("INSERT INTO refusing VALUES (1, 'kept')", ())
        .unwrap();
    assert!(
        database
            .execute(
                "ALTER TABLE refusing ADD COLUMN parent_id INTEGER NOT NULL DEFAULT 1
                 REFERENCES parents(id)",
                (),
            )
            .is_err()
    );
    assert_eq!(
        database
            .query("PRAGMA table_info(refusing)", (), |row| {
                row.get::<_, String>(1)
            })
            .unwrap(),
        ["id", "body"]
    );
    assert_eq!(
        database
            .query("SELECT id, body FROM refusing", (), |row| {
                Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
            })
            .unwrap(),
        [(1, "kept".into())]
    );
}

#[test]
fn drop_table_streams_composite_rows_to_local_repair_and_survives_reopen() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("drop-table.sqlite");
    let db = MultiliteConnection::open(&path).unwrap();
    db.execute(
        "CREATE TABLE inventory (
            tenant TEXT,
            sku INTEGER,
            body ANY,
            PRIMARY KEY (tenant, sku)
        ) WITHOUT ROWID",
        (),
    )
    .unwrap();
    db.execute("CREATE INDEX inventory_body ON inventory(body)", ())
        .unwrap();
    db.execute(
        "INSERT INTO inventory VALUES
            ('a', 1, X'00FF'),
            ('a', 2, NULL),
            ('b', 1, 3.5)",
        (),
    )
    .unwrap();

    db.execute("DROP TABLE inventory", ()).unwrap();
    assert!(db.query("SELECT * FROM inventory", (), |_| Ok(())).is_err());
    drop(db);

    let reopened = MultiliteConnection::open(&path).unwrap();
    assert!(
        reopened
            .query("SELECT * FROM inventory", (), |_| Ok(()))
            .is_err()
    );
    drop(reopened);

    let raw = Connection::open(&path).unwrap();
    assert_eq!(
        raw.query_row("SELECT kind FROM __multilite__repair", (), |row| {
            row.get::<_, i64>(0)
        })
        .unwrap(),
        2
    );
    assert_eq!(
        raw.query_row(
            "SELECT key_parts, value_parts, row_count
             FROM __multilite__repair",
            (),
            |row| Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, i64>(2)?
            )),
        )
        .unwrap(),
        (2, 3, 3)
    );
}

#[test]
fn drop_table_rejects_referenced_parents_but_allows_child_then_parent() {
    let directory = tempfile::tempdir().unwrap();
    let db = MultiliteConnection::open(directory.path().join("drop-foreign-key.sqlite")).unwrap();
    db.execute(
        "CREATE TABLE parents (tenant TEXT, id INTEGER, PRIMARY KEY (tenant, id)) WITHOUT ROWID",
        (),
    )
    .unwrap();
    db.execute(
        "CREATE TABLE children (
            id INTEGER PRIMARY KEY,
            tenant TEXT,
            parent_id INTEGER,
            FOREIGN KEY (tenant, parent_id) REFERENCES parents (tenant, id)
        )",
        (),
    )
    .unwrap();
    db.execute("INSERT INTO parents VALUES ('north', 1)", ())
        .unwrap();
    db.execute("INSERT INTO children VALUES (1, 'north', 1)", ())
        .unwrap();

    assert!(matches!(
        db.execute("DROP TABLE parents", ()),
        Err(Error::UnsupportedSql(
            "DROP TABLE of a referenced parent table is not supported"
        ))
    ));
    assert_eq!(
        db.query("SELECT count(*) FROM parents", (), |row| row
            .get::<_, i64>(0))
            .unwrap(),
        [1]
    );
    assert_eq!(
        db.query("SELECT count(*) FROM children", (), |row| row
            .get::<_, i64>(0))
            .unwrap(),
        [1]
    );

    db.execute("DROP TABLE children", ()).unwrap();
    db.execute("DROP TABLE parents", ()).unwrap();
    assert!(db.query("SELECT * FROM parents", (), |_| Ok(())).is_err());
    assert!(db.query("SELECT * FROM children", (), |_| Ok(())).is_err());
}

#[test]
fn idempotent_ddl_noops_create_no_pending_operations_and_survive_reopen() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("idempotent-ddl.sqlite");
    let db = MultiliteConnection::open(&path).unwrap();
    let local_state = || {
        let raw = Connection::open(&path).unwrap();
        (
            raw.query_row("SELECT count(*) FROM __multilite__pending", (), |row| {
                row.get::<_, i64>(0)
            })
            .unwrap(),
            raw.query_row(
                "SELECT hex(commit_seq) FROM __multilite__commit_state WHERE singleton = 1",
                (),
                |row| row.get::<_, String>(0),
            )
            .unwrap(),
        )
    };

    db.execute(
        "CREATE TABLE IF NOT EXISTS notes (id INTEGER PRIMARY KEY, body TEXT)",
        (),
    )
    .unwrap();
    let after_create = local_state();
    assert_eq!(after_create.0, 1);
    db.execute(
        "CREATE TABLE IF NOT EXISTS NOTES (id INTEGER PRIMARY KEY, ignored BLOB)",
        (),
    )
    .unwrap();
    assert_eq!(local_state(), after_create);

    db.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS notes_body ON notes(body)",
        (),
    )
    .unwrap();
    let after_index = local_state();
    assert_eq!(after_index.0, 2);
    db.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS NOTES_BODY ON notes(missing)",
        (),
    )
    .unwrap();
    assert_eq!(local_state(), after_index);

    db.execute("DROP INDEX IF EXISTS absent_index", ()).unwrap();
    db.execute("DROP INDEX IF EXISTS notes", ()).unwrap();
    assert_eq!(local_state(), after_index);
    assert!(
        db.execute(
            "CREATE TABLE IF NOT EXISTS notes_body (id INTEGER PRIMARY KEY)",
            (),
        )
        .is_err()
    );
    assert!(
        db.execute("CREATE INDEX IF NOT EXISTS notes ON notes(body)", ())
            .is_err()
    );
    assert_eq!(local_state(), after_index);

    db.execute("DROP INDEX IF EXISTS notes_body", ()).unwrap();
    let after_drop = local_state();
    assert_eq!(after_drop.0, 3);
    db.execute("DROP INDEX IF EXISTS NOTES_BODY", ()).unwrap();
    assert_eq!(local_state(), after_drop);
    drop(db);

    let reopened = MultiliteConnection::open(&path).unwrap();
    assert_eq!(
        reopened
            .query(
                "SELECT name FROM sqlite_schema WHERE type = 'table' AND name = 'notes'",
                (),
                |row| row.get::<_, String>(0),
            )
            .unwrap(),
        ["notes"]
    );
    assert!(
        reopened
            .query(
                "SELECT name FROM sqlite_schema WHERE type = 'index' AND name = 'notes_body'",
                (),
                |row| row.get::<_, String>(0),
            )
            .unwrap()
            .is_empty()
    );

    reopened
        .update(|transaction| {
            transaction.execute(
                "CREATE TABLE IF NOT EXISTS batched (id INTEGER PRIMARY KEY, body TEXT)",
                (),
            )?;
            transaction.execute(
                "CREATE TABLE IF NOT EXISTS BATCHED (id INTEGER PRIMARY KEY, ignored BLOB)",
                (),
            )?;
            transaction.execute(
                "CREATE INDEX IF NOT EXISTS batched_body ON batched(body)",
                (),
            )?;
            transaction.execute(
                "CREATE INDEX IF NOT EXISTS BATCHED_BODY ON batched(missing)",
                (),
            )?;
            transaction.execute("DROP INDEX IF EXISTS never_created", ())?;
            Ok(())
        })
        .unwrap();
    assert_eq!(local_state().0, 4);
    assert_eq!(
        reopened
            .query(
                "SELECT name FROM sqlite_schema WHERE name IN ('batched', 'batched_body') ORDER BY name",
                (),
                |row| row.get::<_, String>(0),
            )
            .unwrap(),
        ["batched", "batched_body"]
    );

    let before_conditional_drop = local_state();
    reopened
        .execute("DROP TABLE IF EXISTS table_that_never_existed", ())
        .unwrap();
    reopened
        .execute("DROP TABLE IF EXISTS batched_body", ())
        .unwrap();
    assert_eq!(local_state(), before_conditional_drop);

    reopened
        .execute("DROP TABLE IF EXISTS BATCHED", ())
        .unwrap();
    let after_conditional_drop = local_state();
    assert_eq!(after_conditional_drop.0, before_conditional_drop.0 + 1);
    reopened
        .execute("DROP TABLE IF EXISTS batched", ())
        .unwrap();
    assert_eq!(local_state(), after_conditional_drop);
    drop(reopened);

    let reopened = MultiliteConnection::open(&path).unwrap();
    assert!(
        reopened
            .query("SELECT * FROM batched", (), |_| Ok(()))
            .is_err()
    );
    let raw = Connection::open(&path).unwrap();
    assert_eq!(
        raw.query_row(
            "SELECT count(*) FROM __multilite__repair WHERE kind = 2",
            (),
            |row| row.get::<_, i64>(0),
        )
        .unwrap(),
        1
    );
}

#[test]
fn defaults_checks_and_named_constraints_follow_sqlite_atomically() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("defaults-and-checks.sqlite");
    let db = MultiliteConnection::open(&path).unwrap();

    db.execute(
        "CREATE TABLE inventory (
            id INTEGER,
            label TEXT CONSTRAINT label_required NOT NULL
                CONSTRAINT label_default DEFAULT ('unlabeled')
                CONSTRAINT label_nonempty CHECK (length(label) > 0),
            quantity NUMERIC DEFAULT ('3.0'),
            note TEXT,
            CONSTRAINT inventory_pk PRIMARY KEY (id),
            CONSTRAINT quantity_nonnegative
                CHECK (quantity IS NULL OR quantity >= 0),
            CHECK (quantity != 0 OR note IS NOT NULL)
        )",
        (),
    )
    .unwrap();

    db.execute("INSERT INTO inventory (id) VALUES (1)", ())
        .unwrap();
    assert_eq!(
        db.query(
            "SELECT id, label, quantity, typeof(quantity), note
             FROM inventory",
            (),
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, Option<String>>(4)?,
                ))
            },
        )
        .unwrap(),
        [(1, "unlabeled".into(), 3, "integer".into(), None)]
    );

    assert!(matches!(
        db.execute(
            "INSERT INTO inventory (id, label, quantity) VALUES
                (2, 'valid', 1),
                (3, '', 1)",
            (),
        ),
        Err(Error::Sqlite(_))
    ));
    assert_eq!(
        db.query("SELECT id FROM inventory ORDER BY id", (), |row| {
            row.get::<_, i64>(0)
        })
        .unwrap(),
        [1]
    );

    db.update(|transaction| {
        assert!(matches!(
            transaction.execute("INSERT INTO inventory (id, quantity) VALUES (4, -1)", (),),
            Err(Error::Sqlite(_))
        ));
        transaction.execute(
            "INSERT INTO inventory (id, quantity, note) VALUES (5, 0, 'allowed')",
            (),
        )?;
        Ok(())
    })
    .unwrap();
    assert!(matches!(
        db.execute("UPDATE inventory SET label = '' WHERE id = 1", ()),
        Err(Error::Sqlite(_))
    ));
    assert_eq!(
        db.query(
            "SELECT id, label, quantity, note FROM inventory ORDER BY id",
            (),
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, Option<String>>(3)?,
                ))
            },
        )
        .unwrap(),
        [
            (1, "unlabeled".into(), 3, None),
            (5, "unlabeled".into(), 0, Some("allowed".into())),
        ]
    );

    drop(db);
    let reopened = MultiliteConnection::open(&path).unwrap();
    reopened
        .execute("INSERT INTO inventory (id) VALUES (6)", ())
        .unwrap();
    assert_eq!(
        reopened
            .query(
                "SELECT label, quantity FROM inventory WHERE id = 6",
                (),
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?)),
            )
            .unwrap(),
        [("unlabeled".into(), 3)]
    );
}

#[test]
fn strict_defaults_preserve_storage_class_and_failing_defaults_are_atomic() {
    let directory = tempfile::tempdir().unwrap();
    let db = MultiliteConnection::open(directory.path().join("strict-defaults.sqlite")).unwrap();

    db.execute(
        "CREATE TABLE strict_values (
            id INTEGER PRIMARY KEY,
            value ANY DEFAULT ('003')
                CHECK (typeof(value) = 'text'),
            count INTEGER DEFAULT 2 CHECK (count > 0),
            payload BLOB DEFAULT X'CAFE' CHECK (length(payload) = 2),
            nullable INTEGER DEFAULT NULL CHECK (nullable > 0),
            ratio REAL DEFAULT (1 + 0.5)
        ) STRICT",
        (),
    )
    .unwrap();
    db.execute("INSERT INTO strict_values (id) VALUES (1)", ())
        .unwrap();
    assert_eq!(
        db.query(
            "SELECT value, typeof(value), count, payload, nullable,
                    ratio, typeof(ratio)
             FROM strict_values",
            (),
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, Vec<u8>>(3)?,
                    row.get::<_, Option<i64>>(4)?,
                    row.get::<_, f64>(5)?,
                    row.get::<_, String>(6)?,
                ))
            },
        )
        .unwrap(),
        [(
            "003".into(),
            "text".into(),
            2,
            vec![0xca, 0xfe],
            None,
            1.5,
            "real".into(),
        )]
    );

    db.execute(
        "CREATE TABLE invalid_default (
            id INTEGER PRIMARY KEY,
            value INTEGER DEFAULT -1 CHECK (value >= 0)
        )",
        (),
    )
    .unwrap();
    assert!(matches!(
        db.execute("INSERT INTO invalid_default (id) VALUES (1)", ()),
        Err(Error::Sqlite(_))
    ));
    assert_eq!(
        db.query("SELECT count(*) FROM invalid_default", (), |row| {
            row.get::<_, i64>(0)
        })
        .unwrap(),
        [0]
    );
}

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
            "INSERT INTO notes (id, body) VALUES
                (1, 'one'),
                (2, 'two'),
                (3, 'three')",
            (),
        )
        .unwrap(),
        3
    );

    let mut statement = db
        .prepare("SELECT id, upper(body) FROM notes ORDER BY id")
        .unwrap();
    let rows = statement
        .query_map((), |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
        })
        .unwrap();
    assert_eq!(
        rows,
        [(1, "ONE".into()), (2, "TWO".into()), (3, "THREE".into())]
    );
}

#[test]
fn unique_index_ddl_is_atomic_and_names_can_be_reused_after_drop() {
    let directory = tempfile::tempdir().unwrap();
    let db = MultiliteConnection::open(directory.path().join("unique-index-ddl.sqlite")).unwrap();

    db.execute(
        "CREATE TABLE notes (
            id INTEGER PRIMARY KEY,
            tenant TEXT,
            slug TEXT,
            body TEXT
        )",
        (),
    )
    .unwrap();
    db.execute(
        "INSERT INTO notes VALUES
            (1, 'one', 'same', 'first'),
            (2, 'two', 'same', 'second')",
        (),
    )
    .unwrap();

    assert!(matches!(
        db.execute("CREATE UNIQUE INDEX duplicate_slug ON notes (slug)", ()),
        Err(Error::Sqlite(_))
    ));
    assert_eq!(
        db.query(
            "SELECT count(*) FROM sqlite_schema WHERE name = 'duplicate_slug'",
            (),
            |row| row.get::<_, i64>(0),
        )
        .unwrap(),
        [0]
    );

    db.execute(
        "CREATE UNIQUE INDEX notes_identity ON notes (tenant, slug)",
        (),
    )
    .unwrap();
    assert!(matches!(
        db.execute(
            "INSERT INTO notes VALUES (3, 'one', 'same', 'duplicate')",
            (),
        ),
        Err(Error::Sqlite(_))
    ));
    db.execute("DROP INDEX notes_identity", ()).unwrap();
    db.execute(
        "CREATE UNIQUE INDEX notes_identity ON notes (tenant, body)",
        (),
    )
    .unwrap();
    assert!(matches!(
        db.execute(
            "INSERT INTO notes VALUES (3, 'one', 'different', 'first')",
            (),
        ),
        Err(Error::Sqlite(_))
    ));
    assert_eq!(
        db.query(
            "SELECT count(*) FROM sqlite_schema
             WHERE type = 'index'
               AND name = 'notes_identity'
               AND tbl_name = 'notes'",
            (),
            |row| row.get::<_, i64>(0),
        )
        .unwrap(),
        [1]
    );
}

#[test]
fn secondary_indexes_cover_duplicate_null_and_row_lifecycle_sql() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("secondary-index-ddl.sqlite");
    let db = MultiliteConnection::open(&path).unwrap();

    db.execute(
        "CREATE TABLE notes (
            id INTEGER PRIMARY KEY,
            tenant TEXT,
            category TEXT,
            body TEXT
        )",
        (),
    )
    .unwrap();
    db.execute(
        "INSERT INTO notes VALUES
            (1, 'north', 'shared', 'first'),
            (2, 'south', 'shared', 'second'),
            (3, NULL, NULL, 'third')",
        (),
    )
    .unwrap();
    db.execute("CREATE INDEX notes_category ON notes (category)", ())
        .unwrap();
    db.execute(
        "CREATE INDEX notes_tenant_category ON notes (
            tenant COLLATE NOCASE DESC,
            lower(category) ASC,
            tenant
        ) WHERE tenant IS NOT NULL",
        (),
    )
    .unwrap();
    db.execute("INSERT INTO notes VALUES (4, NULL, 'shared', 'fourth')", ())
        .unwrap();
    db.execute(
        "UPDATE notes SET tenant = 'east', category = NULL WHERE id = 2",
        (),
    )
    .unwrap();
    db.execute("DELETE FROM notes WHERE id = 1", ()).unwrap();

    assert_eq!(
        db.query(
            "SELECT id, tenant, category FROM notes ORDER BY id",
            (),
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, Option<String>>(1)?,
                    row.get::<_, Option<String>>(2)?,
                ))
            },
        )
        .unwrap(),
        [
            (2, Some("east".into()), None),
            (3, None, None),
            (4, None, Some("shared".into())),
        ]
    );
    assert_eq!(
        db.query(
            "SELECT name FROM sqlite_schema
             WHERE type = 'index' AND name LIKE 'notes_%' ORDER BY name",
            (),
            |row| row.get::<_, String>(0),
        )
        .unwrap(),
        ["notes_category", "notes_tenant_category"]
    );

    db.execute("DROP INDEX notes_category", ()).unwrap();
    db.execute(
        "CREATE INDEX notes_category ON notes (
            substr(body, 1, 2) DESC,
            category COLLATE RTRIM
        ) WHERE category IS NOT NULL",
        (),
    )
    .unwrap();

    drop(db);
    let reopened = MultiliteConnection::open(&path).unwrap();
    assert_eq!(
        reopened
            .query(
                "SELECT name FROM sqlite_schema
                 WHERE type = 'index' AND name LIKE 'notes_%' ORDER BY name",
                (),
                |row| row.get::<_, String>(0),
            )
            .unwrap(),
        ["notes_category", "notes_tenant_category"]
    );
    reopened
        .execute(
            "INSERT INTO notes VALUES (5, 'west', 'open', 'reopened')",
            (),
        )
        .unwrap();
}

#[test]
fn invalid_secondary_index_expressions_leave_no_schema_or_catalog_state() {
    let directory = tempfile::tempdir().unwrap();
    let db =
        MultiliteConnection::open(directory.path().join("invalid-secondary-index.sqlite")).unwrap();
    db.execute("CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)", ())
        .unwrap();
    db.execute("INSERT INTO notes VALUES (1, 'one')", ())
        .unwrap();

    for sql in [
        "CREATE INDEX notes_random_term ON notes (random())",
        "CREATE INDEX notes_random_predicate ON notes (body) WHERE random() > 0",
        "CREATE INDEX notes_missing_column ON notes (missing)",
    ] {
        assert!(matches!(db.execute(sql, ()), Err(Error::Sqlite(_))));
    }
    assert!(
        db.query(
            "SELECT name FROM sqlite_schema
             WHERE type = 'index' AND name LIKE 'notes_%'",
            (),
            |row| row.get::<_, String>(0),
        )
        .unwrap()
        .is_empty()
    );

    db.execute(
        "CREATE INDEX notes_valid ON notes (lower(body)) WHERE body IS NOT NULL",
        (),
    )
    .unwrap();
}

#[test]
fn immediate_composite_foreign_keys_follow_sqlite_match_simple_semantics() {
    let directory = tempfile::tempdir().unwrap();
    let db = MultiliteConnection::open(directory.path().join("foreign-key.sqlite")).unwrap();

    db.execute(
        "CREATE TABLE parents (
            tenant TEXT NOT NULL,
            parent_id INTEGER NOT NULL,
            body TEXT,
            PRIMARY KEY (tenant, parent_id)
        ) WITHOUT ROWID",
        (),
    )
    .unwrap();
    db.execute(
        "CREATE TABLE children (
            child_id INTEGER PRIMARY KEY,
            tenant TEXT,
            parent_id INTEGER,
            body TEXT,
            CONSTRAINT parent_fk
                FOREIGN KEY (tenant, parent_id)
                REFERENCES PARENTS (TENANT, PARENT_ID)
        )",
        (),
    )
    .unwrap();
    db.execute("INSERT INTO parents VALUES ('north', 1, 'parent')", ())
        .unwrap();
    db.execute(
        "INSERT INTO children VALUES
            (1, 'north', 1, 'valid'),
            (2, NULL, 999, 'partial null'),
            (3, 'missing', NULL, 'other partial null')",
        (),
    )
    .unwrap();

    assert!(matches!(
        db.execute(
            "INSERT INTO children VALUES (4, 'north', 999, 'orphan')",
            (),
        ),
        Err(Error::Sqlite(_))
    ));
    assert!(matches!(
        db.execute("UPDATE children SET parent_id = 999 WHERE child_id = 1", (),),
        Err(Error::Sqlite(_))
    ));
    assert!(matches!(
        db.execute(
            "DELETE FROM parents WHERE tenant = 'north' AND parent_id = 1",
            (),
        ),
        Err(Error::Sqlite(_))
    ));

    db.execute("DELETE FROM children WHERE child_id = 1", ())
        .unwrap();
    assert_eq!(
        db.execute(
            "DELETE FROM parents WHERE tenant = 'north' AND parent_id = 1",
            (),
        )
        .unwrap(),
        1
    );
    assert_eq!(
        db.query(
            "SELECT child_id FROM children ORDER BY child_id",
            (),
            |row| { row.get::<_, i64>(0) }
        )
        .unwrap(),
        [2, 3]
    );
}

#[test]
fn delete_actions_capture_complete_multi_table_transitions() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("foreign-actions.sqlite");
    let db = MultiliteConnection::open(&path).unwrap();

    db.update(|transaction| {
        transaction.execute(
            "CREATE TABLE parents (
                tenant TEXT NOT NULL,
                parent_id INTEGER NOT NULL,
                body TEXT,
                PRIMARY KEY (tenant, parent_id)
            ) WITHOUT ROWID",
            (),
        )?;
        transaction.execute(
            "CREATE TABLE children (
                child_id INTEGER PRIMARY KEY,
                tenant TEXT,
                parent_id INTEGER,
                body TEXT,
                FOREIGN KEY (tenant, parent_id)
                    REFERENCES parents (tenant, parent_id)
                    ON DELETE CASCADE
            )",
            (),
        )?;
        transaction.execute(
            "CREATE TABLE grandchildren (
                id INTEGER PRIMARY KEY,
                child_id INTEGER REFERENCES children(child_id) ON DELETE CASCADE,
                body TEXT
            ) STRICT",
            (),
        )?;
        transaction.execute(
            "CREATE TABLE labels (
                tenant TEXT,
                label_id INTEGER,
                parent_tenant TEXT,
                parent_id INTEGER,
                body TEXT,
                PRIMARY KEY (tenant, label_id),
                FOREIGN KEY (parent_tenant, parent_id)
                    REFERENCES parents (tenant, parent_id)
                    ON DELETE SET NULL
            ) WITHOUT ROWID",
            (),
        )?;
        transaction.execute(
            "INSERT INTO parents VALUES
                ('north', 1, 'remove'),
                ('south', 2, 'keep')",
            (),
        )?;
        transaction.execute(
            "INSERT INTO children VALUES
                (10, 'north', 1, 'remove'),
                (20, 'south', 2, 'keep')",
            (),
        )?;
        transaction.execute(
            "INSERT INTO grandchildren VALUES
                (100, 10, 'remove'),
                (200, 20, 'keep')",
            (),
        )?;
        transaction.execute(
            "INSERT INTO labels VALUES
                ('labels', 1, 'north', 1, 'detach'),
                ('labels', 2, 'south', 2, 'keep')",
            (),
        )?;
        Ok(())
    })
    .unwrap();
    db.execute("ALTER TABLE children RENAME COLUMN body TO payload", ())
        .unwrap();
    db.execute(
        "ALTER TABLE labels ADD COLUMN extra TEXT DEFAULT 'preserved'",
        (),
    )
    .unwrap();

    assert_eq!(
        db.execute(
            "DELETE FROM parents WHERE tenant = 'north' AND parent_id = 1",
            (),
        )
        .unwrap(),
        1
    );
    assert_eq!(
        db.query(
            "SELECT tenant, parent_id FROM parents ORDER BY tenant",
            (),
            |row| { Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?)) }
        )
        .unwrap(),
        [("south".into(), 2)]
    );
    assert_eq!(
        db.query("SELECT child_id FROM children", (), |row| row
            .get::<_, i64>(0))
            .unwrap(),
        [20]
    );
    assert_eq!(
        db.query("SELECT id FROM grandchildren", (), |row| row
            .get::<_, i64>(0))
            .unwrap(),
        [200]
    );
    assert_eq!(
        db.query(
            "SELECT label_id, parent_tenant, parent_id FROM labels ORDER BY label_id",
            (),
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, Option<String>>(1)?,
                    row.get::<_, Option<i64>>(2)?,
                ))
            },
        )
        .unwrap(),
        [(1, None, None), (2, Some("south".into()), Some(2)),]
    );
    drop(db);

    let reopened = MultiliteConnection::open(&path).unwrap();
    assert_eq!(
        reopened
            .query("SELECT extra FROM labels ORDER BY label_id", (), |row| {
                row.get::<_, String>(0)
            },)
            .unwrap(),
        ["preserved", "preserved"]
    );
    drop(reopened);

    let physical = Connection::open(&path).unwrap();
    assert_eq!(
        physical
            .query_row(
                "SELECT \"table\", on_delete
                 FROM pragma_foreign_key_list('children')",
                (),
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )
            .unwrap(),
        ("parents".into(), "CASCADE".into())
    );
    assert_eq!(
        physical
            .query_row(
                "SELECT DISTINCT \"table\", on_delete
                 FROM pragma_foreign_key_list('labels')",
                (),
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )
            .unwrap(),
        ("parents".into(), "SET NULL".into())
    );
}

#[test]
fn set_default_captures_composite_strict_and_without_rowid_children() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("set-default.sqlite");
    let db = MultiliteConnection::open(&path).unwrap();
    db.update(|transaction| {
        transaction.execute(
            "CREATE TABLE parents (
                tenant TEXT NOT NULL,
                parent_id INTEGER NOT NULL,
                PRIMARY KEY (tenant, parent_id)
            ) WITHOUT ROWID",
            (),
        )?;
        transaction.execute(
            "CREATE TABLE strict_children (
                id INTEGER PRIMARY KEY,
                tenant TEXT NOT NULL DEFAULT 'fallback',
                parent_id INTEGER NOT NULL DEFAULT 0,
                body TEXT,
                FOREIGN KEY (tenant, parent_id)
                    REFERENCES parents (tenant, parent_id)
                    ON DELETE SET DEFAULT
            ) STRICT",
            (),
        )?;
        transaction.execute(
            "CREATE TABLE composite_children (
                group_name TEXT NOT NULL,
                child_id INTEGER NOT NULL,
                tenant TEXT DEFAULT 'fallback',
                parent_id INTEGER DEFAULT 0,
                body BLOB,
                PRIMARY KEY (group_name, child_id),
                FOREIGN KEY (tenant, parent_id)
                    REFERENCES parents (tenant, parent_id)
                    ON DELETE SET DEFAULT
            ) WITHOUT ROWID",
            (),
        )?;
        transaction.execute(
            "INSERT INTO parents VALUES ('fallback', 0), ('north', 1), ('south', 2)",
            (),
        )?;
        transaction.execute(
            "INSERT INTO strict_children VALUES
                (10, 'north', 1, 'change'),
                (20, 'south', 2, 'keep')",
            (),
        )?;
        transaction.execute(
            "INSERT INTO composite_children VALUES
                ('g', 10, 'north', 1, x'01'),
                ('g', 20, 'south', 2, x'02')",
            (),
        )?;
        Ok(())
    })
    .unwrap();

    assert_eq!(
        db.execute(
            "DELETE FROM parents WHERE tenant = 'north' AND parent_id = 1",
            (),
        )
        .unwrap(),
        1
    );
    assert_eq!(
        db.query(
            "SELECT id, tenant, parent_id FROM strict_children ORDER BY id",
            (),
            |row| Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, i64>(2)?
            )),
        )
        .unwrap(),
        [(10, "fallback".into(), 0), (20, "south".into(), 2)]
    );
    assert_eq!(
        db.query(
            "SELECT child_id, tenant, parent_id FROM composite_children ORDER BY child_id",
            (),
            |row| Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, i64>(2)?
            )),
        )
        .unwrap(),
        [(10, "fallback".into(), 0), (20, "south".into(), 2)]
    );
    drop(db);

    let reopened = MultiliteConnection::open(&path).unwrap();
    assert_eq!(
        reopened
            .query(
                "SELECT tenant, parent_id FROM composite_children WHERE child_id = 10",
                (),
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?)),
            )
            .unwrap(),
        [("fallback".into(), 0)]
    );
    drop(reopened);

    let physical = Connection::open(&path).unwrap();
    assert_eq!(
        physical
            .query_row(
                "SELECT DISTINCT on_delete FROM pragma_foreign_key_list('strict_children')",
                (),
                |row| row.get::<_, String>(0),
            )
            .unwrap(),
        "SET DEFAULT"
    );
}

#[test]
fn set_default_and_restrict_failures_rollback_every_indirect_effect() {
    let directory = tempfile::tempdir().unwrap();
    let db = MultiliteConnection::open(directory.path().join("blocking-actions.sqlite")).unwrap();
    db.update(|transaction| {
        transaction.execute("CREATE TABLE parents (id INTEGER PRIMARY KEY)", ())?;
        transaction.execute(
            "CREATE TABLE cascaded (
                id INTEGER PRIMARY KEY,
                parent INTEGER REFERENCES parents(id) ON DELETE CASCADE
            )",
            (),
        )?;
        transaction.execute(
            "CREATE TABLE defaulted (
                id INTEGER PRIMARY KEY,
                parent INTEGER NOT NULL DEFAULT 0
                    REFERENCES parents(id) ON DELETE SET DEFAULT
            ) STRICT",
            (),
        )?;
        transaction.execute(
            "CREATE TABLE restricted (
                id INTEGER PRIMARY KEY,
                parent INTEGER REFERENCES parents(id) ON DELETE RESTRICT
            )",
            (),
        )?;
        transaction.execute("INSERT INTO parents VALUES (1), (2), (3)", ())?;
        transaction.execute("INSERT INTO cascaded VALUES (10, 1), (20, 2)", ())?;
        transaction.execute("INSERT INTO defaulted VALUES (10, 1)", ())?;
        transaction.execute("INSERT INTO restricted VALUES (30, 2)", ())?;
        Ok(())
    })
    .unwrap();

    // Parent zero is absent, so SET DEFAULT makes the final FK check fail. The
    // preceding cascade must be rolled back with the outer statement.
    assert!(matches!(
        db.execute("DELETE FROM parents WHERE id = 1", ()),
        Err(Error::Sqlite(_))
    ));
    // RESTRICT is an immediate blocker and likewise leaves sibling cascades
    // untouched.
    assert!(matches!(
        db.execute("DELETE FROM parents WHERE id = 2", ()),
        Err(Error::Sqlite(_))
    ));
    assert_eq!(
        db.query("SELECT id FROM parents ORDER BY id", (), |row| row
            .get::<_, i64>(0))
            .unwrap(),
        [1, 2, 3]
    );
    assert_eq!(
        db.query("SELECT id, parent FROM cascaded ORDER BY id", (), |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?))
        })
        .unwrap(),
        [(10, 1), (20, 2)]
    );
    assert_eq!(
        db.query("SELECT parent FROM defaulted", (), |row| row
            .get::<_, i64>(0))
            .unwrap(),
        [1]
    );

    // RESTRICT is not a blanket fence: an unreferenced parent is deletable.
    assert_eq!(
        db.execute("DELETE FROM parents WHERE id = 3", ()).unwrap(),
        1
    );
}

#[test]
fn update_actions_capture_composite_multilevel_and_identity_changes() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("update-actions.sqlite");
    let db = MultiliteConnection::open(&path).unwrap();
    db.update(|transaction| {
        transaction.execute(
            "CREATE TABLE parents (
                tenant TEXT NOT NULL,
                parent_id INTEGER NOT NULL,
                body TEXT,
                PRIMARY KEY (tenant, parent_id)
            ) WITHOUT ROWID",
            (),
        )?;
        transaction.execute(
            "CREATE TABLE cascade_children (
                tenant TEXT NOT NULL,
                parent_id INTEGER NOT NULL,
                child_id INTEGER NOT NULL,
                body TEXT,
                PRIMARY KEY (tenant, parent_id, child_id),
                FOREIGN KEY (tenant, parent_id)
                    REFERENCES parents (tenant, parent_id)
                    ON UPDATE CASCADE
            ) WITHOUT ROWID, STRICT",
            (),
        )?;
        transaction.execute(
            "CREATE TABLE grandchildren (
                id INTEGER PRIMARY KEY,
                tenant TEXT,
                parent_id INTEGER,
                child_id INTEGER,
                FOREIGN KEY (tenant, parent_id, child_id)
                    REFERENCES cascade_children (tenant, parent_id, child_id)
                    ON UPDATE CASCADE
            ) STRICT",
            (),
        )?;
        transaction.execute(
            "CREATE TABLE null_children (
                id INTEGER PRIMARY KEY,
                tenant TEXT,
                parent_id INTEGER,
                FOREIGN KEY (tenant, parent_id)
                    REFERENCES parents (tenant, parent_id)
                    ON UPDATE SET NULL
            )",
            (),
        )?;
        transaction.execute(
            "CREATE TABLE default_children (
                id INTEGER PRIMARY KEY,
                tenant TEXT NOT NULL DEFAULT 'fallback',
                parent_id INTEGER NOT NULL DEFAULT 0,
                FOREIGN KEY (tenant, parent_id)
                    REFERENCES parents (tenant, parent_id)
                    ON UPDATE SET DEFAULT
            ) STRICT",
            (),
        )?;
        transaction.execute(
            "INSERT INTO parents VALUES
                ('fallback', 0, 'fallback'),
                ('north', 1, 'move')",
            (),
        )?;
        transaction.execute(
            "INSERT INTO cascade_children VALUES ('north', 1, 10, 'child')",
            (),
        )?;
        transaction.execute("INSERT INTO grandchildren VALUES (100, 'north', 1, 10)", ())?;
        transaction.execute("INSERT INTO null_children VALUES (200, 'north', 1)", ())?;
        transaction.execute("INSERT INTO default_children VALUES (300, 'north', 1)", ())?;
        Ok(())
    })
    .unwrap();

    assert_eq!(
        db.execute(
            "UPDATE parents
             SET tenant = 'west', parent_id = 9
             WHERE tenant = 'north' AND parent_id = 1",
            (),
        )
        .unwrap(),
        1
    );
    assert_eq!(
        db.query(
            "SELECT tenant, parent_id, child_id FROM cascade_children",
            (),
            |row| Ok((
                row.get::<_, String>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, i64>(2)?
            )),
        )
        .unwrap(),
        [("west".into(), 9, 10)]
    );
    assert_eq!(
        db.query(
            "SELECT tenant, parent_id, child_id FROM grandchildren",
            (),
            |row| Ok((
                row.get::<_, String>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, i64>(2)?
            )),
        )
        .unwrap(),
        [("west".into(), 9, 10)]
    );
    assert_eq!(
        db.query("SELECT tenant, parent_id FROM null_children", (), |row| {
            Ok((
                row.get::<_, Option<String>>(0)?,
                row.get::<_, Option<i64>>(1)?,
            ))
        })
        .unwrap(),
        [(None, None)]
    );
    assert_eq!(
        db.query(
            "SELECT tenant, parent_id FROM default_children",
            (),
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?)),
        )
        .unwrap(),
        [("fallback".into(), 0)]
    );
    drop(db);

    let reopened = MultiliteConnection::open(&path).unwrap();
    assert_eq!(
        reopened
            .query(
                "SELECT tenant, parent_id FROM cascade_children",
                (),
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?)),
            )
            .unwrap(),
        [("west".into(), 9)]
    );
    drop(reopened);

    let physical = Connection::open(&path).unwrap();
    for (table, expected) in [
        ("cascade_children", "CASCADE"),
        ("grandchildren", "CASCADE"),
        ("null_children", "SET NULL"),
        ("default_children", "SET DEFAULT"),
    ] {
        assert_eq!(
            physical
                .query_row(
                    "SELECT DISTINCT on_update FROM pragma_foreign_key_list(?1)",
                    [table],
                    |row| row.get::<_, String>(0),
                )
                .unwrap(),
            expected
        );
    }
}

#[test]
fn update_restrict_and_invalid_action_results_are_atomic() {
    let directory = tempfile::tempdir().unwrap();
    let db = MultiliteConnection::open(directory.path().join("update-blockers.sqlite")).unwrap();
    db.update(|transaction| {
        transaction.execute("CREATE TABLE parents (id INTEGER PRIMARY KEY)", ())?;
        transaction.execute(
            "CREATE TABLE cascaded (
                id INTEGER PRIMARY KEY,
                parent INTEGER REFERENCES parents(id) ON UPDATE CASCADE
            )",
            (),
        )?;
        transaction.execute(
            "CREATE TABLE defaulted (
                id INTEGER PRIMARY KEY,
                parent INTEGER NOT NULL DEFAULT 0
                    REFERENCES parents(id) ON UPDATE SET DEFAULT
            ) STRICT",
            (),
        )?;
        transaction.execute(
            "CREATE TABLE restricted (
                id INTEGER PRIMARY KEY,
                parent INTEGER REFERENCES parents(id) ON UPDATE RESTRICT
            )",
            (),
        )?;
        transaction.execute("INSERT INTO parents VALUES (1), (2), (3)", ())?;
        transaction.execute("INSERT INTO cascaded VALUES (10, 1), (20, 2)", ())?;
        transaction.execute("INSERT INTO defaulted VALUES (10, 1)", ())?;
        transaction.execute("INSERT INTO restricted VALUES (20, 2)", ())?;
        Ok(())
    })
    .unwrap();

    // SET DEFAULT targets missing parent zero. SQLite may have run earlier
    // cascades, but the statement savepoint must erase every indirect event.
    assert!(matches!(
        db.execute("UPDATE parents SET id = 9 WHERE id = 1", ()),
        Err(Error::Sqlite(_))
    ));
    assert!(matches!(
        db.execute("UPDATE parents SET id = 9 WHERE id = 2", ()),
        Err(Error::Sqlite(_))
    ));
    assert_eq!(
        db.query("SELECT id FROM parents ORDER BY id", (), |row| row
            .get::<_, i64>(0))
            .unwrap(),
        [1, 2, 3]
    );
    assert_eq!(
        db.query("SELECT id, parent FROM cascaded ORDER BY id", (), |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?))
        })
        .unwrap(),
        [(10, 1), (20, 2)]
    );
    assert_eq!(
        db.query("SELECT parent FROM defaulted", (), |row| row
            .get::<_, i64>(0))
            .unwrap(),
        [1]
    );

    assert_eq!(
        db.execute("UPDATE parents SET id = 9 WHERE id = 3", ())
            .unwrap(),
        1
    );
}

#[test]
fn multi_row_parent_key_shifts_preserve_reused_reference_prefixes() {
    let directory = tempfile::tempdir().unwrap();
    let db = MultiliteConnection::open(directory.path().join("multi-parent-shift.sqlite")).unwrap();
    db.update(|transaction| {
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

    assert_eq!(
        db.execute("UPDATE parents SET id = id - 1 WHERE id IN (2, 3)", ())
            .unwrap(),
        2
    );
    assert_eq!(
        db.query("SELECT id FROM parents ORDER BY id", (), |row| row
            .get::<_, i64>(0))
            .unwrap(),
        [1, 2]
    );
    assert_eq!(
        db.query("SELECT id, parent FROM children ORDER BY id", (), |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?))
        })
        .unwrap(),
        [(20, 1), (30, 2)]
    );
}

#[test]
fn mixed_foreign_key_paths_fold_repeated_child_events_to_one_final_state() {
    let directory = tempfile::tempdir().unwrap();
    let db = MultiliteConnection::open(directory.path().join("mixed-action-paths.sqlite")).unwrap();
    db.update(|transaction| {
        transaction.execute("CREATE TABLE parents (id INTEGER PRIMARY KEY)", ())?;
        transaction.execute(
            "CREATE TABLE children (
                id INTEGER PRIMARY KEY,
                cascade_parent INTEGER REFERENCES parents(id)
                    ON DELETE CASCADE ON UPDATE CASCADE,
                nullable_parent INTEGER REFERENCES parents(id)
                    ON DELETE SET NULL ON UPDATE SET NULL
            )",
            (),
        )?;
        transaction.execute("INSERT INTO parents VALUES (1), (2)", ())?;
        transaction.execute("INSERT INTO children VALUES (10, 1, 1), (20, 2, 2)", ())?;
        Ok(())
    })
    .unwrap();

    assert_eq!(
        db.execute("DELETE FROM parents WHERE id = 1", ()).unwrap(),
        1
    );
    assert_eq!(
        db.query("SELECT id FROM children", (), |row| row.get::<_, i64>(0))
            .unwrap(),
        [20]
    );

    assert_eq!(
        db.execute("UPDATE parents SET id = 3 WHERE id = 2", ())
            .unwrap(),
        1
    );
    assert_eq!(
        db.query(
            "SELECT id, cascade_parent, nullable_parent FROM children",
            (),
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, Option<i64>>(2)?,
                ))
            },
        )
        .unwrap(),
        [(20, 3, None)]
    );
}

#[test]
fn unique_parent_actions_and_set_default_null_semantics_are_captured() {
    let directory = tempfile::tempdir().unwrap();
    let db =
        MultiliteConnection::open(directory.path().join("unique-parent-actions.sqlite")).unwrap();
    db.update(|transaction| {
        transaction.execute(
            "CREATE TABLE accounts (
                id INTEGER PRIMARY KEY,
                tenant TEXT NOT NULL,
                email TEXT NOT NULL,
                UNIQUE (tenant, email)
            )",
            (),
        )?;
        transaction.execute(
            "CREATE TABLE default_messages (
                id INTEGER PRIMARY KEY,
                tenant TEXT DEFAULT 'fallback',
                email TEXT DEFAULT 'fallback@example.com',
                FOREIGN KEY (tenant, email) REFERENCES accounts (tenant, email)
                    ON DELETE SET DEFAULT ON UPDATE CASCADE
            )",
            (),
        )?;
        transaction.execute(
            "CREATE TABLE nullable_messages (
                id INTEGER PRIMARY KEY,
                tenant TEXT,
                email TEXT,
                FOREIGN KEY (tenant, email) REFERENCES accounts (tenant, email)
                    ON DELETE SET DEFAULT ON UPDATE CASCADE
            )",
            (),
        )?;
        transaction.execute(
            "INSERT INTO accounts VALUES
                (0, 'fallback', 'fallback@example.com'),
                (1, 'north', 'one@example.com')",
            (),
        )?;
        transaction.execute(
            "INSERT INTO default_messages VALUES (10, 'north', 'one@example.com')",
            (),
        )?;
        transaction.execute(
            "INSERT INTO nullable_messages VALUES (20, 'north', 'one@example.com')",
            (),
        )?;
        Ok(())
    })
    .unwrap();

    assert_eq!(
        db.execute(
            "UPDATE accounts
             SET tenant = 'west', email = 'moved@example.com'
             WHERE id = 1",
            (),
        )
        .unwrap(),
        1
    );
    for table in ["default_messages", "nullable_messages"] {
        assert_eq!(
            db.query(&format!("SELECT tenant, email FROM {table}"), (), |row| Ok(
                (row.get::<_, String>(0)?, row.get::<_, String>(1)?)
            ),)
                .unwrap(),
            [("west".into(), "moved@example.com".into())]
        );
    }

    assert_eq!(
        db.execute("DELETE FROM accounts WHERE id = 1", ()).unwrap(),
        1
    );
    assert_eq!(
        db.query("SELECT tenant, email FROM default_messages", (), |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })
        .unwrap(),
        [("fallback".into(), "fallback@example.com".into())]
    );
    assert_eq!(
        db.query("SELECT tenant, email FROM nullable_messages", (), |row| {
            Ok((
                row.get::<_, Option<String>>(0)?,
                row.get::<_, Option<String>>(1)?,
            ))
        })
        .unwrap(),
        [(None, None)]
    );
}

#[test]
fn referential_actions_survive_schema_rebuilds_and_reopen() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("action-rebuilds.sqlite");
    let db = MultiliteConnection::open(&path).unwrap();
    db.update(|transaction| {
        transaction.execute(
            "CREATE TABLE parents (
                id INTEGER PRIMARY KEY,
                spare TEXT,
                body TEXT NOT NULL
            )",
            (),
        )?;
        transaction.execute(
            "CREATE TABLE children (
                id INTEGER PRIMARY KEY,
                parent INTEGER DEFAULT 0 REFERENCES parents(id)
                    ON DELETE SET DEFAULT ON UPDATE CASCADE,
                spare TEXT
            )",
            (),
        )?;
        transaction.execute(
            "INSERT INTO parents VALUES (0, 'x', 'fallback'), (1, 'x', 'target')",
            (),
        )?;
        transaction.execute("INSERT INTO children VALUES (10, 1, 'x')", ())?;
        transaction.execute("ALTER TABLE parents RENAME TO accounts", ())?;
        transaction.execute("ALTER TABLE accounts RENAME COLUMN id TO account_id", ())?;
        transaction.execute(
            "ALTER TABLE children RENAME COLUMN parent TO account_id",
            (),
        )?;
        transaction.execute(
            "ALTER TABLE accounts ADD COLUMN added TEXT DEFAULT 'account'",
            (),
        )?;
        transaction.execute(
            "ALTER TABLE children ADD COLUMN added TEXT DEFAULT 'child'",
            (),
        )?;
        transaction.execute("ALTER TABLE accounts DROP COLUMN spare", ())?;
        transaction.execute("ALTER TABLE children DROP COLUMN spare", ())?;
        Ok(())
    })
    .unwrap();
    drop(db);

    let reopened = MultiliteConnection::open(&path).unwrap();
    assert_eq!(
        reopened
            .execute(
                "UPDATE accounts SET account_id = 2 WHERE account_id = 1",
                ()
            )
            .unwrap(),
        1
    );
    assert_eq!(
        reopened
            .query("SELECT account_id, added FROM children", (), |row| {
                Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
            })
            .unwrap(),
        [(2, "child".into())]
    );
    assert_eq!(
        reopened
            .execute("DELETE FROM accounts WHERE account_id = 2", ())
            .unwrap(),
        1
    );
    assert_eq!(
        reopened
            .query("SELECT account_id FROM children", (), |row| row
                .get::<_, i64>(0))
            .unwrap(),
        [0]
    );
}

#[test]
fn blocking_foreign_key_rolls_back_mixed_delete_actions_and_hook_events() {
    let directory = tempfile::tempdir().unwrap();
    let db = MultiliteConnection::open(directory.path().join("mixed-actions.sqlite")).unwrap();
    db.update(|transaction| {
        transaction.execute("CREATE TABLE parents (id INTEGER PRIMARY KEY)", ())?;
        transaction.execute(
            "CREATE TABLE cascaded (
                id INTEGER PRIMARY KEY,
                parent INTEGER REFERENCES parents(id) ON DELETE CASCADE
            )",
            (),
        )?;
        transaction.execute(
            "CREATE TABLE detached (
                id INTEGER PRIMARY KEY,
                parent INTEGER REFERENCES parents(id) ON DELETE SET NULL
            )",
            (),
        )?;
        transaction.execute(
            "CREATE TABLE blockers (
                id INTEGER PRIMARY KEY,
                parent INTEGER REFERENCES parents(id)
            )",
            (),
        )?;
        transaction.execute(
            "CREATE TABLE audit (id INTEGER PRIMARY KEY, body TEXT NOT NULL)",
            (),
        )?;
        transaction.execute("INSERT INTO parents VALUES (1)", ())?;
        transaction.execute("INSERT INTO cascaded VALUES (10, 1)", ())?;
        transaction.execute("INSERT INTO detached VALUES (20, 1)", ())?;
        transaction.execute("INSERT INTO blockers VALUES (30, 1)", ())?;
        transaction.execute("INSERT INTO audit VALUES (1, 'before')", ())?;
        Ok(())
    })
    .unwrap();

    db.update(|transaction| {
        assert!(matches!(
            transaction.execute("DELETE FROM parents WHERE id = 1", ()),
            Err(Error::Sqlite(_))
        ));
        transaction.execute("UPDATE audit SET body = 'after' WHERE id = 1", ())?;
        Ok(())
    })
    .unwrap();

    for table in ["parents", "cascaded", "detached", "blockers"] {
        assert_eq!(
            db.query(&format!("SELECT count(*) FROM {table}"), (), |row| {
                row.get::<_, i64>(0)
            })
            .unwrap(),
            [1],
            "failed parent delete changed {table}"
        );
    }
    assert_eq!(
        db.query("SELECT body FROM audit", (), |row| row.get::<_, String>(0))
            .unwrap(),
        ["after"]
    );

    db.update(|transaction| {
        transaction.execute("DELETE FROM blockers WHERE id = 30", ())?;
        transaction.execute("DELETE FROM parents WHERE id = 1", ())?;
        Ok(())
    })
    .unwrap();
    assert!(
        db.query("SELECT id FROM cascaded", (), |row| row.get::<_, i64>(0))
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        db.query("SELECT id, parent FROM detached", (), |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, Option<i64>>(1)?))
        })
        .unwrap(),
        [(20, None)]
    );
}

#[test]
fn set_null_respects_child_not_null_constraints_atomically() {
    let directory = tempfile::tempdir().unwrap();
    let db = MultiliteConnection::open(directory.path().join("set-null-not-null.sqlite")).unwrap();
    db.execute("CREATE TABLE parents (id INTEGER PRIMARY KEY)", ())
        .unwrap();
    db.execute(
        "CREATE TABLE children (
            id INTEGER PRIMARY KEY,
            parent INTEGER NOT NULL REFERENCES parents(id) ON DELETE SET NULL
        )",
        (),
    )
    .unwrap();
    db.execute("INSERT INTO parents VALUES (1)", ()).unwrap();
    db.execute("INSERT INTO children VALUES (10, 1)", ())
        .unwrap();

    assert!(matches!(
        db.execute("DELETE FROM parents WHERE id = 1", ()),
        Err(Error::Sqlite(_))
    ));
    assert_eq!(
        db.query("SELECT id FROM parents", (), |row| row.get::<_, i64>(0))
            .unwrap(),
        [1]
    );
    assert_eq!(
        db.query("SELECT id, parent FROM children", (), |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?))
        })
        .unwrap(),
        [(10, 1)]
    );
}

#[test]
fn foreign_keys_require_an_existing_matching_parent_key_contract() {
    let directory = tempfile::tempdir().unwrap();
    let db = MultiliteConnection::open(directory.path().join("foreign-key-shape.sqlite")).unwrap();

    assert!(matches!(
        db.execute(
            "CREATE TABLE child (
                id INTEGER PRIMARY KEY,
                parent INTEGER REFERENCES missing(id)
            )",
            (),
        ),
        Err(Error::UnsupportedSql(
            "foreign-key parent must already be a synchronized table"
        ))
    ));
    db.execute(
        "CREATE TABLE parents (
            tenant TEXT NOT NULL,
            id INTEGER NOT NULL,
            slug TEXT UNIQUE,
            PRIMARY KEY (tenant, id)
        ) WITHOUT ROWID",
        (),
    )
    .unwrap();

    for sql in [
        "CREATE TABLE wrong_order (
            id INTEGER PRIMARY KEY,
            tenant TEXT NOT NULL,
            parent INTEGER NOT NULL,
            FOREIGN KEY (parent, tenant) REFERENCES parents (id, tenant)
        )",
        "CREATE TABLE non_unique_parent (
            id INTEGER PRIMARY KEY,
            tenant TEXT REFERENCES parents (tenant)
        )",
        "CREATE TABLE affinity_mismatch (
            id INTEGER PRIMARY KEY,
            tenant BLOB NOT NULL,
            parent INTEGER NOT NULL,
            FOREIGN KEY (tenant, parent) REFERENCES parents (tenant, id)
        )",
        "CREATE TABLE recursive (
            id INTEGER PRIMARY KEY,
            parent INTEGER REFERENCES recursive (id)
        )",
    ] {
        assert!(matches!(db.execute(sql, ()), Err(Error::UnsupportedSql(_))));
    }
}

#[test]
fn composite_unique_foreign_keys_cover_constraints_and_explicit_indexes() {
    let directory = tempfile::tempdir().unwrap();
    let db = MultiliteConnection::open(directory.path().join("unique-foreign-key.sqlite")).unwrap();

    db.execute(
        "CREATE TABLE accounts (
            id INTEGER PRIMARY KEY,
            tenant TEXT,
            email TEXT,
            UNIQUE (tenant, email)
        )",
        (),
    )
    .unwrap();
    db.execute(
        "CREATE TABLE messages (
            id INTEGER PRIMARY KEY,
            tenant TEXT,
            recipient TEXT,
            FOREIGN KEY (tenant, recipient) REFERENCES accounts (tenant, email)
        )",
        (),
    )
    .unwrap();
    db.execute(
        "INSERT INTO accounts VALUES
            (1, 'north', 'one@example.com'),
            (2, NULL, 'nullable@example.com')",
        (),
    )
    .unwrap();
    db.execute(
        "INSERT INTO messages VALUES
            (10, 'north', 'one@example.com'),
            (11, NULL, 'missing@example.com')",
        (),
    )
    .unwrap();
    assert_eq!(
        db.execute("DELETE FROM accounts WHERE id = 2", ()).unwrap(),
        1
    );
    assert!(matches!(
        db.execute(
            "INSERT INTO messages VALUES (12, 'north', 'missing@example.com')",
            (),
        ),
        Err(Error::Sqlite(_))
    ));
    assert!(matches!(
        db.execute(
            "UPDATE accounts SET email = 'moved@example.com' WHERE id = 1",
            ()
        ),
        Err(Error::Sqlite(_))
    ));
    assert!(matches!(
        db.execute("DELETE FROM accounts WHERE id = 1", ()),
        Err(Error::Sqlite(_))
    ));
    db.execute("DELETE FROM messages WHERE id = 10", ())
        .unwrap();
    db.execute(
        "UPDATE accounts SET email = 'moved@example.com' WHERE id = 1",
        (),
    )
    .unwrap();
    db.execute(
        "INSERT INTO messages VALUES (13, 'north', 'moved@example.com')",
        (),
    )
    .unwrap();

    db.update(|transaction| {
        transaction.execute(
            "CREATE TABLE handles (
                id INTEGER PRIMARY KEY,
                region TEXT NOT NULL,
                handle TEXT NOT NULL
            )",
            (),
        )?;
        transaction.execute(
            "CREATE UNIQUE INDEX handles_identity ON handles (region, handle)",
            (),
        )?;
        transaction.execute(
            "CREATE TABLE mentions (
                id INTEGER PRIMARY KEY,
                region TEXT,
                handle TEXT,
                FOREIGN KEY (region, handle) REFERENCES handles (region, handle)
            )",
            (),
        )?;
        Ok(())
    })
    .unwrap();
    db.execute("INSERT INTO handles VALUES (1, 'west', 'nikhil')", ())
        .unwrap();
    db.execute("INSERT INTO mentions VALUES (1, 'west', 'nikhil')", ())
        .unwrap();
    assert!(matches!(
        db.execute("DROP INDEX handles_identity", ()),
        Err(Error::UnsupportedSql(
            "cannot drop a UNIQUE index referenced by a foreign key"
        ))
    ));
    assert_eq!(
        db.query(
            "SELECT count(*) FROM sqlite_schema WHERE name = 'handles_identity'",
            (),
            |row| row.get::<_, i64>(0),
        )
        .unwrap(),
        [1]
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
fn without_rowid_composite_primary_keys_move_as_one_logical_identity() {
    let directory = tempfile::tempdir().unwrap();
    let db = MultiliteConnection::open(directory.path().join("composite-rowid.sqlite")).unwrap();
    db.execute(
        "CREATE TABLE documents (
            tenant TEXT NOT NULL,
            document INTEGER NOT NULL,
            body TEXT NOT NULL,
            PRIMARY KEY (document, tenant)
        ) WITHOUT ROWID",
        (),
    )
    .unwrap();
    db.execute("INSERT INTO documents VALUES ('north', 1, 'original')", ())
        .unwrap();
    db.execute(
        "UPDATE documents
         SET document = 2, body = 'moved'
         WHERE tenant = 'north' AND document = 1",
        (),
    )
    .unwrap();
    assert_eq!(
        db.query("SELECT tenant, document, body FROM documents", (), |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, String>(2)?,
            ))
        },)
            .unwrap(),
        [("north".into(), 2, "moved".into())]
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
fn captured_dml_supports_ctes_update_from_tuples_and_index_hints() {
    let directory = tempfile::tempdir().unwrap();
    let db = MultiliteConnection::open(directory.path().join("richer-dml.sqlite")).unwrap();
    db.update(|transaction| {
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
            "INSERT INTO notes VALUES
                (1, 'one', 10), (2, 'two', 20), (3, 'three', 30)",
            (),
        )?;
        transaction.execute(
            "INSERT INTO replacements VALUES
                (1, 'ONE', 11), (2, 'TWO', 22)",
            (),
        )?;
        Ok(())
    })
    .unwrap();

    assert_eq!(
        db.execute(
            "WITH selected AS (
                SELECT id, body, score FROM replacements WHERE score > 10
             )
             UPDATE notes INDEXED BY notes_by_body
             SET (body, score) = (selected.body, selected.score)
             FROM selected
             WHERE selected.id = notes.id",
            (),
        )
        .unwrap(),
        2
    );
    assert_eq!(
        db.execute(
            "WITH doomed AS (SELECT id FROM replacements WHERE score >= 20)
             DELETE FROM notes NOT INDEXED
             WHERE id IN (SELECT id FROM doomed)",
            (),
        )
        .unwrap(),
        1
    );
    assert_eq!(
        db.query("SELECT id, body, score FROM notes ORDER BY id", (), |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, i64>(2)?,
            ))
        })
        .unwrap(),
        [(1, "ONE".into(), 11), (3, "three".into(), 30)]
    );
}

#[test]
fn dml_target_aliases_cover_upsert_update_from_delete_and_returning() {
    let directory = tempfile::tempdir().unwrap();
    let db = MultiliteConnection::open(directory.path().join("dml-aliases.sqlite")).unwrap();
    db.update(|transaction| {
        transaction.execute(
            "CREATE TABLE notes (
                id INTEGER PRIMARY KEY,
                score INTEGER NOT NULL,
                body TEXT NOT NULL
            )",
            (),
        )?;
        transaction.execute(
            "CREATE TABLE modifiers (
                id INTEGER PRIMARY KEY,
                suffix TEXT NOT NULL
            )",
            (),
        )?;
        transaction.execute(
            "INSERT INTO notes VALUES (1, 10, 'one'), (2, 20, 'two')",
            (),
        )?;
        transaction.execute("INSERT INTO modifiers VALUES (1, '-a'), (2, '-b')", ())?;
        Ok(())
    })
    .unwrap();

    assert_eq!(
        db.query(
            "INSERT INTO notes AS target VALUES (1, 30, 'next')
             ON CONFLICT(id) DO UPDATE
             SET score = excluded.score,
                 body = target.body || ':' || excluded.body
             RETURNING id, score, body",
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
        [(1, 30, "one:next".into())]
    );
    assert_eq!(
        db.query(
            "UPDATE notes AS target
             SET body = target.body || source.suffix
             FROM modifiers AS source
             WHERE target.id = source.id
             RETURNING id, body",
            (),
            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?)),
        )
        .unwrap(),
        [(1, "one:next-a".into()), (2, "two-b".into())]
    );
    assert_eq!(
        db.query(
            "DELETE FROM notes AS target WHERE target.id = 2 RETURNING id, body",
            (),
            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?)),
        )
        .unwrap(),
        [(2, "two-b".into())]
    );
    assert_eq!(
        db.query("SELECT id, score, body FROM notes", (), |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, String>(2)?,
            ))
        })
        .unwrap(),
        [(1, 30, "one:next-a".into())]
    );
}

#[test]
fn limited_writes_use_native_order_limit_and_offset_selection() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("limited-writes.sqlite");
    let db = MultiliteConnection::open(&path).unwrap();
    db.update(|transaction| {
        transaction.execute(
            "CREATE TABLE notes (
                id INTEGER PRIMARY KEY,
                score INTEGER NOT NULL,
                body TEXT NOT NULL
            )",
            (),
        )?;
        transaction.execute("CREATE INDEX notes_score ON notes(score)", ())?;
        transaction.execute(
            "INSERT INTO notes VALUES
                (1, 10, 'one'), (2, 50, 'two'), (3, 40, 'three'),
                (4, 40, 'four'), (5, 20, 'five'), (6, 30, 'six')",
            (),
        )?;
        Ok(())
    })
    .unwrap();

    assert_eq!(
        db.execute(
            "UPDATE notes INDEXED BY notes_score SET body = 'picked'
             ORDER BY score DESC, id ASC LIMIT ?1 OFFSET ?2",
            params![2, 1],
        )
        .unwrap(),
        2
    );
    assert_eq!(
        db.query(
            "SELECT id FROM notes WHERE body = 'picked' ORDER BY id",
            (),
            |row| row.get::<_, i64>(0),
        )
        .unwrap(),
        [3, 4]
    );

    assert_eq!(
        db.execute(
            "DELETE FROM notes NOT INDEXED WHERE body <> 'picked'
             ORDER BY score ASC, id DESC LIMIT ?1, ?2",
            params![1, 2],
        )
        .unwrap(),
        2
    );
    assert_eq!(
        db.query("SELECT id FROM notes ORDER BY id", (), |row| {
            row.get::<_, i64>(0)
        })
        .unwrap(),
        [1, 2, 3, 4]
    );

    let before_noop = local_write_state(&path);
    assert_eq!(
        db.execute("UPDATE notes SET body = 'never' ORDER BY id LIMIT 0", (),)
            .unwrap(),
        0
    );
    let after_noop = local_write_state(&path);
    assert_eq!(after_noop, before_noop);

    assert_eq!(
        db.execute(
            "UPDATE notes SET body = 'tail' ORDER BY id LIMIT -1 OFFSET 2",
            (),
        )
        .unwrap(),
        2
    );
    assert_eq!(
        db.query(
            "SELECT id FROM notes WHERE body = 'tail' ORDER BY id",
            (),
            |row| { row.get::<_, i64>(0) }
        )
        .unwrap(),
        [3, 4]
    );
    assert_eq!(
        db.execute(
            "DELETE FROM notes WHERE id < 0 ORDER BY id LIMIT 9223372036854775807",
            (),
        )
        .unwrap(),
        0
    );
}

#[test]
fn invalid_limited_write_bounds_roll_back_without_advancing_local_state() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("invalid-limited-bounds.sqlite");
    let db = MultiliteConnection::open(&path).unwrap();
    db.execute(
        "CREATE TABLE notes (
            id INTEGER PRIMARY KEY,
            body TEXT NOT NULL
        )",
        (),
    )
    .unwrap();
    db.execute("INSERT INTO notes VALUES (1, 'one'), (2, 'two')", ())
        .unwrap();

    let expected_rows = [(1, String::from("one")), (2, String::from("two"))];
    let state = local_write_state(&path);
    for sql in [
        "UPDATE notes SET body = 'bad' ORDER BY id LIMIT NULL",
        "DELETE FROM notes ORDER BY id LIMIT 'not-an-integer'",
        "UPDATE notes SET body = 'bad' ORDER BY id LIMIT 1.5",
        "DELETE FROM notes ORDER BY id LIMIT 9223372036854775808",
        "UPDATE notes SET body = 'bad' ORDER BY id LIMIT 1 OFFSET NULL",
        "DELETE FROM notes ORDER BY id LIMIT 1 OFFSET 'not-an-integer'",
    ] {
        assert!(
            matches!(db.execute(sql, ()), Err(Error::Sqlite(_))),
            "{sql}"
        );
        assert_eq!(
            db.query("SELECT id, body FROM notes ORDER BY id", (), |row| {
                Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
            })
            .unwrap(),
            expected_rows,
            "{sql}"
        );
        assert_eq!(local_write_state(&path), state, "{sql}");
    }

    for value in [
        Value::Null,
        Value::Text("not-an-integer".into()),
        Value::Real(1.5),
    ] {
        assert!(matches!(
            db.execute(
                "UPDATE notes SET body = 'bad' ORDER BY id LIMIT ?1",
                params![value],
            ),
            Err(Error::Sqlite(_))
        ));
        assert_eq!(local_write_state(&path), state);
    }
    for value in [
        Value::Null,
        Value::Text("not-an-integer".into()),
        Value::Real(1.5),
    ] {
        assert!(matches!(
            db.execute(
                "DELETE FROM notes ORDER BY id LIMIT 1 OFFSET ?1",
                params![value],
            ),
            Err(Error::Sqlite(_))
        ));
        assert_eq!(local_write_state(&path), state);
    }
    assert_eq!(
        db.query("SELECT id, body FROM notes ORDER BY id", (), |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
        })
        .unwrap(),
        expected_rows
    );

    assert_eq!(
        db.execute(
            "UPDATE notes SET body = 'first' ORDER BY id LIMIT 1 OFFSET -99",
            (),
        )
        .unwrap(),
        1
    );
    assert_eq!(
        db.query("SELECT id, body FROM notes ORDER BY id", (), |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
        })
        .unwrap(),
        [(1, "first".into()), (2, "two".into())]
    );
}

#[test]
fn limited_writes_cover_strict_composite_keys_cascades_and_replace_victims() {
    let directory = tempfile::tempdir().unwrap();
    let db = MultiliteConnection::open(directory.path().join("limited-shapes.sqlite")).unwrap();
    db.update(|transaction| {
        transaction.execute(
            "CREATE TABLE inventory (
                tenant TEXT,
                sku INTEGER,
                priority INTEGER NOT NULL,
                body ANY,
                PRIMARY KEY (tenant, sku)
            ) WITHOUT ROWID, STRICT",
            (),
        )?;
        transaction.execute(
            "INSERT INTO inventory VALUES
                ('a', 1, 20, X'01'), ('a', 2, 40, 2.5),
                ('b', 1, 40, NULL), ('b', 2, 10, 'tail')",
            (),
        )?;
        transaction.execute(
            "CREATE TABLE parents (
                id INTEGER PRIMARY KEY,
                priority INTEGER NOT NULL
            )",
            (),
        )?;
        transaction.execute(
            "CREATE TABLE children (
                id INTEGER PRIMARY KEY,
                parent_id INTEGER NOT NULL,
                FOREIGN KEY (parent_id) REFERENCES parents(id)
                    ON DELETE CASCADE ON UPDATE CASCADE
            )",
            (),
        )?;
        transaction.execute("INSERT INTO parents VALUES (1, 10), (2, 20), (3, 30)", ())?;
        transaction.execute(
            "INSERT INTO children VALUES (11, 1), (21, 2), (22, 2), (31, 3)",
            (),
        )?;
        transaction.execute(
            "CREATE TABLE slugs (
                id INTEGER PRIMARY KEY,
                slug TEXT NOT NULL UNIQUE,
                priority INTEGER NOT NULL
            )",
            (),
        )?;
        transaction.execute(
            "INSERT INTO slugs VALUES (1, 'one', 10), (2, 'two', 20)",
            (),
        )?;
        Ok(())
    })
    .unwrap();

    assert_eq!(
        db.execute(
            "UPDATE inventory SET body = 'selected'
             ORDER BY priority DESC, tenant, sku LIMIT 2",
            (),
        )
        .unwrap(),
        2
    );
    assert_eq!(
        db.query(
            "SELECT tenant, sku FROM inventory WHERE body = 'selected'
             ORDER BY tenant, sku",
            (),
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?)),
        )
        .unwrap(),
        [("a".into(), 2), ("b".into(), 1)]
    );
    assert_eq!(
        db.execute(
            "DELETE FROM inventory ORDER BY priority, tenant, sku LIMIT 1 OFFSET 1",
            (),
        )
        .unwrap(),
        1
    );
    assert_eq!(
        db.query(
            "SELECT tenant, sku FROM inventory ORDER BY tenant, sku",
            (),
            |row| { Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?)) }
        )
        .unwrap(),
        [("a".into(), 2), ("b".into(), 1), ("b".into(), 2)]
    );

    assert_eq!(
        db.execute("DELETE FROM parents ORDER BY priority DESC, id LIMIT 1", (),)
            .unwrap(),
        1
    );
    assert_eq!(
        db.query("SELECT id FROM children ORDER BY id", (), |row| {
            row.get::<_, i64>(0)
        })
        .unwrap(),
        [11, 21, 22]
    );
    assert_eq!(
        db.execute(
            "UPDATE parents SET id = id + 100 ORDER BY priority DESC, id LIMIT 1",
            (),
        )
        .unwrap(),
        1
    );
    assert_eq!(
        db.query(
            "SELECT id, parent_id FROM children ORDER BY id",
            (),
            |row| { Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)) }
        )
        .unwrap(),
        [(11, 1), (21, 102), (22, 102)]
    );

    assert_eq!(
        db.execute(
            "UPDATE OR REPLACE slugs SET slug = 'one'
             ORDER BY priority DESC, id LIMIT 1",
            (),
        )
        .unwrap(),
        1
    );
    assert_eq!(
        db.query("SELECT id, slug FROM slugs", (), |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
        })
        .unwrap(),
        [(2, "one".into())]
    );
}

#[test]
fn update_moves_declared_primary_keys_atomically() {
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
            ) WITHOUT ROWID",
            (),
        )?;
        transaction.execute("INSERT INTO notes VALUES (1, 'original')", ())?;
        transaction.execute("INSERT INTO documents VALUES ('a', 'document')", ())?;
        Ok(())
    })
    .unwrap();

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
    assert_eq!(
        db.query("SELECT id, body FROM notes", (), |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
        })
        .unwrap(),
        [(4, "changed".into())]
    );
    assert_eq!(
        db.query("SELECT id, body FROM documents", (), |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })
        .unwrap(),
        [("b".into(), "moved".into())]
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
fn table_rename_preserves_rows_indexes_foreign_keys_and_reopens() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("rename.sqlite");
    let db = MultiliteConnection::open(&path).unwrap();

    db.update(|transaction| {
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
        transaction.execute("ALTER TABLE parents RENAME TO \"renamed parents\"", ())?;
        transaction.execute(
            "CREATE INDEX renamed_parent_id ON \"renamed parents\" (id)",
            (),
        )?;
        transaction.execute("INSERT INTO \"renamed parents\" VALUES (2, 'two')", ())?;
        transaction.execute("INSERT INTO children VALUES (2, 2)", ())?;
        Ok(())
    })
    .unwrap();

    assert!(
        db.query("SELECT id FROM parents", (), |row| row.get::<_, i64>(0))
            .is_err()
    );
    assert_eq!(
        db.query(
            "SELECT id, code FROM \"renamed parents\" ORDER BY id",
            (),
            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?)),
        )
        .unwrap(),
        [(1, "one".into()), (2, "two".into())]
    );
    assert_eq!(
        db.query(
            "SELECT id, parent_id FROM children ORDER BY id",
            (),
            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
        )
        .unwrap(),
        [(1, 1), (2, 2)]
    );
    assert_eq!(
        db.query(
            "SELECT tbl_name FROM sqlite_schema
             WHERE type = 'index' AND name = 'parents_code_lookup'",
            (),
            |row| row.get::<_, String>(0),
        )
        .unwrap(),
        ["renamed parents"]
    );
    drop(db);

    let db = MultiliteConnection::open(&path).unwrap();
    db.execute("INSERT INTO \"renamed parents\" VALUES (3, 'three')", ())
        .unwrap();
    db.execute("INSERT INTO children VALUES (3, 3)", ())
        .unwrap();
    drop(db);

    let stock = Connection::open(&path).unwrap();
    stock.execute_batch("PRAGMA foreign_keys = ON").unwrap();
    assert_eq!(
        stock
            .query_row("SELECT count(*) FROM \"renamed parents\"", (), |row| {
                row.get::<_, i64>(0)
            })
            .unwrap(),
        3
    );
    assert_eq!(
        stock
            .prepare("PRAGMA foreign_key_check")
            .unwrap()
            .query_map((), |_| Ok(()))
            .unwrap()
            .count(),
        0
    );
    assert_eq!(
        stock
            .query_row("PRAGMA integrity_check", (), |row| row.get::<_, String>(0))
            .unwrap(),
        "ok"
    );
}

#[test]
fn column_rename_preserves_stable_row_identity_and_managed_reads() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("rename-column.sqlite");
    let db = MultiliteConnection::open(&path).unwrap();
    db.execute(
        "CREATE TABLE notes (
            tenant TEXT NOT NULL,
            note_id INTEGER NOT NULL,
            body TEXT,
            PRIMARY KEY (tenant, note_id),
            UNIQUE (tenant, body)
        ) WITHOUT ROWID",
        (),
    )
    .unwrap();
    db.execute("INSERT INTO notes VALUES ('acme', 1, 'first')", ())
        .unwrap();

    db.execute("ALTER TABLE notes RENAME COLUMN body TO contents", ())
        .unwrap();
    db.execute(
        "ALTER TABLE notes ADD COLUMN summary TEXT
         DEFAULT 'none'",
        (),
    )
    .unwrap();
    db.execute(
        "UPDATE notes SET contents = 'updated', summary = 'changed'
         WHERE tenant = 'acme' AND note_id = 1",
        (),
    )
    .unwrap();
    db.execute(
        "INSERT INTO notes (tenant, note_id, contents, summary)
         VALUES ('acme', 2, 'second', 'added')",
        (),
    )
    .unwrap();
    db.execute("CREATE INDEX notes_contents_lookup ON notes (contents)", ())
        .unwrap();
    db.execute(
        "CREATE TABLE note_links (
            id INTEGER PRIMARY KEY,
            tenant TEXT NOT NULL,
            contents TEXT NOT NULL,
            FOREIGN KEY (tenant, contents)
                REFERENCES notes (tenant, contents)
        )",
        (),
    )
    .unwrap();
    db.execute("INSERT INTO note_links VALUES (1, 'acme', 'second')", ())
        .unwrap();
    assert_eq!(
        db.update(|transaction| {
            transaction.query(
                "SELECT contents || ':' || summary FROM notes
                 WHERE tenant = 'acme' AND note_id = 2",
                (),
                |row| row.get::<_, String>(0),
            )
        })
        .unwrap(),
        ["second:added"]
    );
    db.execute(
        "DELETE FROM notes WHERE tenant = 'acme' AND note_id = 1",
        (),
    )
    .unwrap();
    db.execute("ALTER TABLE notes DROP COLUMN summary", ())
        .unwrap();

    drop(db);
    let reopened = MultiliteConnection::open(&path).unwrap();
    assert_eq!(
        reopened
            .query(
                "SELECT tenant, note_id, contents
                 FROM notes ORDER BY note_id",
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
        [("acme".into(), 2, "second".into())]
    );
    assert_eq!(
        reopened
            .query("SELECT tenant, contents FROM note_links", (), |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })
            .unwrap(),
        [("acme".into(), "second".into())]
    );
    drop(reopened);

    let stock = Connection::open(&path).unwrap();
    stock.execute_batch("PRAGMA foreign_keys = ON").unwrap();
    assert_eq!(
        stock
            .query_row("PRAGMA integrity_check", (), |row| row.get::<_, String>(0))
            .unwrap(),
        "ok"
    );
    assert_eq!(
        stock
            .prepare("PRAGMA foreign_key_check")
            .unwrap()
            .query_map((), |_| Ok(()))
            .unwrap()
            .count(),
        0
    );
}

#[test]
fn table_rename_preserves_strict_and_without_rowid_storage_modes() {
    let directory = tempfile::tempdir().unwrap();
    let database =
        MultiliteConnection::open(directory.path().join("rename-storage-modes.sqlite")).unwrap();

    database
        .update(|transaction| {
            transaction.execute(
                "CREATE TABLE composite (
                    tenant TEXT,
                    id INTEGER,
                    body BLOB,
                    PRIMARY KEY (tenant, id)
                ) WITHOUT ROWID",
                (),
            )?;
            transaction.execute(
                "CREATE TABLE strict_notes (
                    id INTEGER PRIMARY KEY,
                    body ANY
                ) STRICT",
                (),
            )?;
            transaction.execute("ALTER TABLE composite RENAME TO archived_composite", ())?;
            transaction.execute("ALTER TABLE strict_notes RENAME TO archived_strict", ())?;
            transaction.execute(
                "INSERT INTO archived_composite VALUES ('one', 1, x'0102')",
                (),
            )?;
            transaction.execute("INSERT INTO archived_strict VALUES (1, '0007')", ())?;
            Ok(())
        })
        .unwrap();

    assert_eq!(
        database
            .query(
                "SELECT tenant, id, hex(body) FROM archived_composite",
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
        [("one".into(), 1, "0102".into())]
    );
    assert_eq!(
        database
            .query(
                "SELECT id, body, typeof(body) FROM archived_strict",
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
        [(1, "0007".into(), "text".into())]
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
        "UPDATE OR FAIL notes SET body = 'x'",
        "UPDATE OR ROLLBACK notes SET body = 'x'",
        "UPDATE main.notes SET body = 'x'",
        "UPDATE notes INDEXED BY sqlite_autoindex_notes_1 SET body = 'x'",
        "UPDATE notes SET body = 'x' ORDER BY id",
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
        "DELETE FROM main.notes",
        "DELETE FROM notes INDEXED BY sqlite_autoindex_notes_1",
        "DELETE FROM notes ORDER BY id",
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
fn rename_of_an_adopted_table_is_rejected_without_mutation() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("adopted-rename.sqlite");
    Connection::open(&path)
        .unwrap()
        .execute_batch(
            "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT);
             INSERT INTO notes VALUES (1, 'original')",
        )
        .unwrap();

    let db = MultiliteConnection::open(&path).unwrap();
    assert!(matches!(
        db.execute("ALTER TABLE notes RENAME TO archived_notes", ()),
        Err(Error::UnsupportedSql(
            "ALTER TABLE target has no synchronized schema identity"
        ))
    ));
    assert_eq!(read_note(&db), "original");
    assert!(
        db.query(
            "SELECT name FROM sqlite_schema WHERE name = 'archived_notes'",
            (),
            |row| row.get::<_, String>(0),
        )
        .unwrap()
        .is_empty()
    );
}

#[test]
fn failed_rename_target_collision_rolls_back_its_statement_savepoint() {
    let directory = tempfile::tempdir().unwrap();
    let db = MultiliteConnection::open(directory.path().join("rename-collision.sqlite")).unwrap();
    db.execute("CREATE TABLE notes (id INTEGER PRIMARY KEY)", ())
        .unwrap();
    db.execute("CREATE TABLE tasks (id INTEGER PRIMARY KEY)", ())
        .unwrap();

    db.update(|transaction| {
        assert!(
            transaction
                .execute("ALTER TABLE notes RENAME TO tasks", ())
                .is_err()
        );
        transaction.execute("INSERT INTO notes VALUES (1)", ())?;
        Ok(())
    })
    .unwrap();

    assert_eq!(
        db.query("SELECT id FROM notes", (), |row| row.get::<_, i64>(0))
            .unwrap(),
        [1]
    );
    assert!(
        db.query("SELECT id FROM tasks", (), |row| row.get::<_, i64>(0))
            .unwrap()
            .is_empty()
    );
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
fn trigger_generated_delete_effects_join_the_same_atomic_transition() {
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

    assert_eq!(db.execute("DELETE FROM notes WHERE id = 1", ()).unwrap(), 1);
    for table in ["notes", "audit"] {
        assert_eq!(
            db.query(&format!("SELECT count(*) FROM {table}"), (), |row| row
                .get::<_, i64>(0),)
                .unwrap(),
            [0]
        );
    }
}

#[test]
fn trigger_writes_to_unsynchronized_tables_rollback_the_outer_statement() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("trigger-untracked.sqlite");
    Connection::open(&path)
        .unwrap()
        .execute_batch("CREATE TABLE adopted (id INTEGER PRIMARY KEY, body TEXT NOT NULL)")
        .unwrap();
    let db = MultiliteConnection::open(&path).unwrap();
    db.execute(
        "CREATE TABLE owned (id INTEGER PRIMARY KEY, body TEXT NOT NULL)",
        (),
    )
    .unwrap();
    db.execute("INSERT INTO owned VALUES (1, 'kept')", ())
        .unwrap();
    drop(db);

    Connection::open(&path)
        .unwrap()
        .execute_batch(
            "CREATE TRIGGER leak_delete
             AFTER DELETE ON owned
             BEGIN
                 INSERT INTO adopted VALUES (old.id, old.body);
             END",
        )
        .unwrap();
    let db = MultiliteConnection::open(&path).unwrap();

    assert!(matches!(
        db.execute("DELETE FROM owned WHERE id = 1", ()),
        Err(Error::UnsupportedSql(
            "INSERT target has no synchronized schema identity"
        ))
    ));
    assert_eq!(
        db.query("SELECT id, body FROM owned", (), |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
        })
        .unwrap(),
        [(1, "kept".into())]
    );
    assert!(
        db.query("SELECT id FROM adopted", (), |row| row.get::<_, i64>(0))
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        db.execute("UPDATE owned SET body = 'still-usable' WHERE id = 1", ())
            .unwrap(),
        1
    );
}

#[test]
fn trigger_generated_update_effects_replay_exactly_once_for_richer_dml() {
    for (index, sql) in [
        "UPDATE notes SET body = 'changed' WHERE id = 1",
        "UPDATE OR REPLACE notes SET body = 'changed' WHERE id = 1",
        "REPLACE INTO notes VALUES (1, 'changed')",
        "INSERT OR REPLACE INTO notes VALUES (1, 'changed')",
        "INSERT INTO notes VALUES (1, 'changed')
         ON CONFLICT(id) DO UPDATE SET body = excluded.body",
    ]
    .into_iter()
    .enumerate()
    {
        let directory = tempfile::tempdir().unwrap();
        let path = directory
            .path()
            .join(format!("trigger-update-{index}.sqlite"));
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
                     UPDATE audit SET body = new.body || '-trigger' WHERE id = new.id;
                 END;
                 CREATE TRIGGER insert_audit
                 AFTER INSERT ON notes
                 BEGIN
                     UPDATE audit SET body = new.body || '-trigger' WHERE id = new.id;
                 END",
            )
            .unwrap();
        let db = MultiliteConnection::open(&path).unwrap();

        assert_eq!(db.execute(sql, ()).unwrap(), 1);
        assert_eq!(read_note(&db), "changed");
        assert_eq!(
            db.query("SELECT body FROM audit WHERE id = 1", (), |row| {
                row.get::<_, String>(0)
            })
            .unwrap(),
            ["changed-trigger"]
        );
    }
}

#[test]
fn atomic_row_conflict_modes_follow_sqlite_and_partial_modes_are_rejected() {
    let directory = tempfile::tempdir().unwrap();
    let db = MultiliteConnection::open(directory.path().join("conflicts.sqlite")).unwrap();
    db.execute("CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)", ())
        .unwrap();
    db.execute("INSERT INTO notes VALUES (1, 'original')", ())
        .unwrap();

    for sql in [
        "INSERT OR FAIL INTO notes VALUES (1, 'failed')",
        "INSERT OR ROLLBACK INTO notes VALUES (1, 'rolled-back')",
    ] {
        assert!(matches!(db.execute(sql, ()), Err(Error::UnsupportedSql(_))));
        assert_eq!(read_note(&db), "original");
    }

    assert_eq!(
        db.execute("INSERT OR IGNORE INTO notes VALUES (1, 'ignored')", ())
            .unwrap(),
        0
    );
    assert_eq!(
        db.execute(
            "INSERT INTO notes VALUES (1, 'ignored') ON CONFLICT DO NOTHING",
            (),
        )
        .unwrap(),
        0
    );
    assert!(matches!(
        db.execute("INSERT OR ABORT INTO notes VALUES (1, 'aborted')", ()),
        Err(Error::Sqlite(_))
    ));
    assert_eq!(read_note(&db), "original");

    db.execute("INSERT INTO notes VALUES (2, 'second')", ())
        .unwrap();
    assert_eq!(
        db.execute("UPDATE OR IGNORE notes SET id = 1 WHERE id = 2", ())
            .unwrap(),
        0
    );
    assert!(matches!(
        db.execute("UPDATE OR ABORT notes SET id = 1 WHERE id = 2", ()),
        Err(Error::Sqlite(_))
    ));
    assert_eq!(
        db.query("SELECT id, body FROM notes ORDER BY id", (), |row| Ok((
            row.get::<_, i64>(0)?,
            row.get::<_, String>(1)?,
        )),)
            .unwrap(),
        [(1, "original".into()), (2, "second".into())]
    );
}

#[test]
fn replacement_conflict_modes_use_complete_net_row_effects() {
    let directory = tempfile::tempdir().unwrap();
    let db = MultiliteConnection::open(directory.path().join("replace.sqlite")).unwrap();
    db.execute(
        "CREATE TABLE profiles (
            id INTEGER PRIMARY KEY,
            tenant TEXT NOT NULL,
            email TEXT NOT NULL UNIQUE,
            username TEXT NOT NULL UNIQUE,
            body TEXT NOT NULL CHECK (length(body) > 0),
            UNIQUE (tenant, username)
        )",
        (),
    )
    .unwrap();
    db.execute(
        "INSERT INTO profiles VALUES
            (1, 'acme', 'a@example.com', 'alpha', 'first'),
            (2, 'other', 'b@example.com', 'beta', 'second'),
            (5, 'keep', 'keep@example.com', 'keep', 'keep')",
        (),
    )
    .unwrap();
    let rows = || {
        db.query(
            "SELECT id, tenant, email, username, body FROM profiles ORDER BY id",
            (),
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                ))
            },
        )
        .unwrap()
    };

    assert_eq!(
        db.execute(
            "REPLACE INTO profiles VALUES
                (3, 'acme', 'a@example.com', 'beta', 'replacement')",
            (),
        )
        .unwrap(),
        1
    );
    assert_eq!(
        rows(),
        [
            (
                3,
                "acme".into(),
                "a@example.com".into(),
                "beta".into(),
                "replacement".into(),
            ),
            (
                5,
                "keep".into(),
                "keep@example.com".into(),
                "keep".into(),
                "keep".into(),
            ),
        ]
    );

    db.execute(
        "INSERT OR REPLACE INTO profiles VALUES
            (3, 'acme', 'c@example.com', 'gamma', 'same-pk')",
        (),
    )
    .unwrap();
    db.execute(
        "WITH incoming(id, tenant, email, username, body) AS (
            VALUES (6, 'keep', 'keep@example.com', 'six', 'from-cte')
         )
         INSERT OR REPLACE INTO profiles
         SELECT id, tenant, email, username, body FROM incoming",
        (),
    )
    .unwrap();
    db.execute(
        "UPDATE OR REPLACE profiles
         SET email = 'keep@example.com', username = 'merged', body = 'updated'
         WHERE id = 3",
        (),
    )
    .unwrap();
    assert_eq!(
        rows(),
        [(
            3,
            "acme".into(),
            "keep@example.com".into(),
            "merged".into(),
            "updated".into(),
        )]
    );

    assert_eq!(
        db.execute(
            "INSERT OR REPLACE INTO profiles VALUES
                (7, 'transient', 'transient@example.com', 'transient', 'first'),
                (8, 'transient', 'transient@example.com', 'final', 'second')",
            (),
        )
        .unwrap(),
        2
    );
    assert_eq!(
        rows().into_iter().map(|row| row.0).collect::<Vec<_>>(),
        [3, 8]
    );

    db.execute(
        "INSERT OR REPLACE INTO profiles VALUES
            (9, 'unused', 'keep@example.com', 'unused', 'upserted')
         ON CONFLICT(email) DO UPDATE SET body = excluded.body",
        (),
    )
    .unwrap();
    assert_eq!(
        db.query("SELECT id, body FROM profiles WHERE id = 3", (), |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
        })
        .unwrap(),
        [(3, "upserted".into())]
    );

    let before_failure = rows();
    for sql in [
        "INSERT OR REPLACE INTO profiles VALUES
            (10, 'broken', 'keep@example.com', 'broken', '')",
        "INSERT OR REPLACE INTO profiles VALUES
            (11, 'unused', 'keep@example.com', 'final', 'must-abort')
         ON CONFLICT(email) DO UPDATE SET username = excluded.username",
    ] {
        assert!(matches!(db.execute(sql, ()), Err(Error::Sqlite(_))));
        assert_eq!(rows(), before_failure);
    }
}

#[test]
fn replacement_covers_composite_strict_and_foreign_key_tables() {
    let directory = tempfile::tempdir().unwrap();
    let db = MultiliteConnection::open(directory.path().join("replace-shapes.sqlite")).unwrap();
    db.update(|transaction| {
        transaction.execute(
            "CREATE TABLE memberships (
                tenant TEXT NOT NULL,
                member INTEGER NOT NULL,
                handle TEXT NOT NULL UNIQUE,
                body TEXT NOT NULL,
                PRIMARY KEY (tenant, member)
            ) WITHOUT ROWID",
            (),
        )?;
        transaction.execute(
            "INSERT INTO memberships VALUES
                ('a', 1, 'first', 'first'),
                ('b', 2, 'second', 'second')",
            (),
        )?;
        transaction.execute(
            "CREATE TABLE strict_values (
                id INTEGER PRIMARY KEY,
                token TEXT NOT NULL UNIQUE,
                amount INTEGER NOT NULL
            ) STRICT",
            (),
        )?;
        transaction.execute("INSERT INTO strict_values VALUES (1, 'token', 1)", ())?;
        transaction.execute(
            "CREATE TABLE parents (
                id INTEGER PRIMARY KEY,
                code TEXT NOT NULL UNIQUE
            )",
            (),
        )?;
        transaction.execute(
            "CREATE TABLE children (
                id INTEGER PRIMARY KEY,
                parent_code TEXT NOT NULL REFERENCES parents(code),
                body TEXT NOT NULL
            )",
            (),
        )?;
        transaction.execute("INSERT INTO parents VALUES (1, 'p1'), (2, 'p2')", ())?;
        transaction.execute("INSERT INTO children VALUES (10, 'p1', 'child')", ())?;
        Ok(())
    })
    .unwrap();

    db.execute(
        "REPLACE INTO memberships VALUES ('c', 3, 'first', 'replacement')",
        (),
    )
    .unwrap();
    db.execute(
        "UPDATE OR REPLACE memberships
         SET handle = 'second', body = 'merged'
         WHERE tenant = 'c' AND member = 3",
        (),
    )
    .unwrap();
    assert_eq!(
        db.query(
            "SELECT tenant, member, handle, body FROM memberships",
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
        [("c".into(), 3, "second".into(), "merged".into())]
    );

    db.execute(
        "INSERT OR REPLACE INTO strict_values VALUES (2, 'token', 2)",
        (),
    )
    .unwrap();
    assert!(matches!(
        db.execute(
            "INSERT OR REPLACE INTO strict_values VALUES (3, 'token', X'00')",
            (),
        ),
        Err(Error::Sqlite(_))
    ));
    assert_eq!(
        db.query("SELECT id, token, amount FROM strict_values", (), |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, i64>(2)?,
            ))
        })
        .unwrap(),
        [(2, "token".into(), 2)]
    );

    db.execute("REPLACE INTO parents VALUES (3, 'p1')", ())
        .unwrap();
    db.execute(
        "INSERT OR REPLACE INTO children VALUES (10, 'p2', 'retargeted')",
        (),
    )
    .unwrap();
    db.execute("UPDATE OR REPLACE parents SET code = 'p2' WHERE id = 3", ())
        .unwrap();
    assert_eq!(
        db.query("SELECT id, code FROM parents", (), |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
        })
        .unwrap(),
        [(3, "p2".into())]
    );
    assert_eq!(
        db.query("SELECT id, parent_code, body FROM children", (), |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
            ))
        })
        .unwrap(),
        [(10, "p2".into(), "retargeted".into())]
    );
    for sql in [
        "REPLACE INTO parents VALUES (3, 'p3')",
        "REPLACE INTO children VALUES (10, 'missing', 'invalid')",
    ] {
        assert!(matches!(db.execute(sql, ()), Err(Error::Sqlite(_))));
    }
    assert_eq!(
        db.query("SELECT code FROM parents", (), |row| row
            .get::<_, String>(0))
            .unwrap(),
        ["p2"]
    );
    assert_eq!(
        db.query("SELECT parent_code FROM children", (), |row| {
            row.get::<_, String>(0)
        })
        .unwrap(),
        ["p2"]
    );
}

#[test]
fn replacement_of_an_adopted_table_rolls_back_without_a_schema_identity() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("adopted-replace.sqlite");
    Connection::open(&path)
        .unwrap()
        .execute_batch(
            "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT UNIQUE);
             INSERT INTO notes VALUES (1, 'original')",
        )
        .unwrap();
    let db = MultiliteConnection::open(&path).unwrap();

    for sql in [
        "REPLACE INTO notes VALUES (1, 'replaced')",
        "INSERT OR REPLACE INTO notes VALUES (2, 'original')",
        "UPDATE OR REPLACE notes SET body = 'replaced' WHERE id = 1",
    ] {
        assert!(matches!(db.execute(sql, ()), Err(Error::UnsupportedSql(_))));
        assert_eq!(read_note(&db), "original");
    }
}

#[test]
fn upsert_do_update_uses_sqlites_net_row_effects() {
    let directory = tempfile::tempdir().unwrap();
    let db = MultiliteConnection::open(directory.path().join("upsert.sqlite")).unwrap();
    db.execute(
        "CREATE TABLE accounts (
            id INTEGER PRIMARY KEY,
            email TEXT NOT NULL UNIQUE,
            body TEXT NOT NULL,
            revisions INTEGER NOT NULL DEFAULT 0
        )",
        (),
    )
    .unwrap();
    db.execute(
        "INSERT INTO accounts VALUES
            (1, 'one', 'start', 0),
            (5, 'five', 'keep', 0)",
        (),
    )
    .unwrap();

    assert_eq!(
        db.execute(
            "INSERT INTO accounts VALUES
                (2, 'two', 'inserted', 0),
                (3, 'two', 'second-touch', 7),
                (9, 'one', 'updated-existing', 8),
                (10, 'one', 'where-false', 9)
             ON CONFLICT(email) DO UPDATE SET
                body = excluded.body,
                revisions = accounts.revisions + 1
             WHERE excluded.id <> 10",
            (),
        )
        .unwrap(),
        3
    );
    assert_eq!(
        db.query(
            "SELECT id, email, body, revisions FROM accounts ORDER BY id",
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
            (1, "one".into(), "updated-existing".into(), 1),
            (2, "two".into(), "second-touch".into(), 1),
            (5, "five".into(), "keep".into(), 0),
        ]
    );

    assert_eq!(
        db.execute(
            "INSERT INTO accounts VALUES
                (1, 'unused', 'id-conflict', 0),
                (12, 'five', 'email-conflict', 0),
                (13, 'new', 'new-row', 0)
             ON CONFLICT(id) DO NOTHING
             ON CONFLICT(email) DO UPDATE SET
                body = excluded.body,
                revisions = accounts.revisions + 1
             WHERE accounts.body <> excluded.body
             ON CONFLICT DO NOTHING",
            (),
        )
        .unwrap(),
        2
    );
    assert_eq!(
        db.execute(
            "INSERT INTO accounts VALUES (99, 'one', 'ignored', 0)
             ON CONFLICT(email) DO UPDATE SET body = excluded.body WHERE 0",
            (),
        )
        .unwrap(),
        0
    );

    assert_eq!(
        db.execute(
            "INSERT INTO accounts VALUES (10, 'one', 'moved', 0)
             ON CONFLICT(email) DO UPDATE SET
                id = excluded.id,
                body = excluded.body",
            (),
        )
        .unwrap(),
        1
    );
    assert_eq!(
        db.execute(
            "INSERT INTO accounts VALUES
                (11, 'one', 'moved-again', 0),
                (10, 'replacement', 'reused-key', 0)
             ON CONFLICT(email) DO UPDATE SET
                id = excluded.id,
                body = excluded.body",
            (),
        )
        .unwrap(),
        2
    );
    let before_abort = db
        .query(
            "SELECT id, email, body, revisions FROM accounts ORDER BY id",
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
        .unwrap();
    assert!(matches!(
        db.execute(
            "INSERT INTO accounts VALUES
                (20, 'transient', 'must-roll-back', 0),
                (21, 'one', 'violates-update', 0)
             ON CONFLICT(email) DO UPDATE SET email = 'two'",
            (),
        ),
        Err(Error::Sqlite(_))
    ));
    assert_eq!(
        db.query(
            "SELECT id, email, body, revisions FROM accounts ORDER BY id",
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
        before_abort
    );
}

#[test]
fn upsert_do_update_supports_composite_unique_targets_and_null_distinctness() {
    let directory = tempfile::tempdir().unwrap();
    let db = MultiliteConnection::open(directory.path().join("composite-upsert.sqlite")).unwrap();
    db.execute(
        "CREATE TABLE entries (
            id INTEGER PRIMARY KEY,
            tenant TEXT,
            slug TEXT,
            body TEXT NOT NULL,
            UNIQUE (tenant, slug)
        )",
        (),
    )
    .unwrap();
    db.execute(
        "INSERT INTO entries VALUES
            (1, 'acme', 'home', 'old'),
            (2, NULL, 'home', 'nullable-old')",
        (),
    )
    .unwrap();

    assert_eq!(
        db.execute(
            "WITH incoming(id, tenant, slug, body) AS (
                VALUES
                    (9, 'acme', 'home', 'updated'),
                    (3, NULL, 'home', 'nullable-new')
             )
             INSERT INTO entries
             SELECT id, tenant, slug, body FROM incoming WHERE true
             ON CONFLICT(tenant, slug) DO UPDATE SET
                body = (SELECT upper(excluded.body))",
            (),
        )
        .unwrap(),
        2
    );
    assert_eq!(
        db.query(
            "SELECT id, tenant, slug, body FROM entries ORDER BY id",
            (),
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, Option<String>>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                ))
            },
        )
        .unwrap(),
        [
            (1, Some("acme".into()), "home".into(), "UPDATED".into()),
            (2, None, "home".into(), "nullable-old".into()),
            (3, None, "home".into(), "nullable-new".into()),
        ]
    );
}

#[test]
fn upsert_do_update_moves_foreign_keys_and_rejects_missing_parents_atomically() {
    let directory = tempfile::tempdir().unwrap();
    let db = MultiliteConnection::open(directory.path().join("foreign-upsert.sqlite")).unwrap();
    db.update(|transaction| {
        transaction.execute(
            "CREATE TABLE parents (
                id INTEGER PRIMARY KEY,
                code TEXT NOT NULL UNIQUE
            )",
            (),
        )?;
        transaction.execute(
            "CREATE TABLE children (
                id INTEGER PRIMARY KEY,
                parent_code TEXT NOT NULL REFERENCES parents(code),
                body TEXT NOT NULL
            )",
            (),
        )?;
        transaction.execute("INSERT INTO parents VALUES (1, 'p1'), (2, 'p2')", ())?;
        transaction.execute("INSERT INTO children VALUES (10, 'p1', 'original')", ())?;
        Ok(())
    })
    .unwrap();

    assert_eq!(
        db.execute(
            "INSERT INTO children VALUES (10, 'p2', 'retargeted')
             ON CONFLICT(id) DO UPDATE SET
                parent_code = excluded.parent_code,
                body = excluded.body",
            (),
        )
        .unwrap(),
        1
    );
    assert!(matches!(
        db.execute(
            "INSERT INTO children VALUES (10, 'missing', 'invalid')
             ON CONFLICT(id) DO UPDATE SET
                parent_code = excluded.parent_code,
                body = excluded.body",
            (),
        ),
        Err(Error::Sqlite(_))
    ));
    assert_eq!(
        db.query("SELECT id, parent_code, body FROM children", (), |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
            ))
        },)
            .unwrap(),
        [(10, "p2".into(), "retargeted".into())]
    );
}

#[test]
fn upsert_do_update_secondary_constraint_failures_abort_outer_ignore_statements() {
    let directory = tempfile::tempdir().unwrap();
    let db = MultiliteConnection::open(directory.path().join("upsert-abort.sqlite")).unwrap();
    db.execute(
        "CREATE TABLE profiles (
            id INTEGER PRIMARY KEY,
            tenant TEXT NOT NULL,
            email TEXT NOT NULL UNIQUE,
            username TEXT NOT NULL UNIQUE,
            UNIQUE (tenant, username)
        )",
        (),
    )
    .unwrap();
    db.execute(
        "INSERT INTO profiles VALUES
            (1, 'acme', 'a@example.com', 'alpha'),
            (2, 'acme', 'b@example.com', 'beta')",
        (),
    )
    .unwrap();
    let rows = || {
        db.query(
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
    };
    let before = rows();

    assert!(matches!(
        db.execute(
            "INSERT OR IGNORE INTO profiles VALUES
                (3, 'other', 'c@example.com', 'gamma'),
                (9, 'acme', 'a@example.com', 'beta')
             ON CONFLICT(email) DO UPDATE SET username = excluded.username",
            (),
        ),
        Err(Error::Sqlite(_))
    ));
    assert_eq!(rows(), before);

    assert_eq!(
        db.execute(
            "INSERT INTO profiles VALUES
                (9, 'acme', 'a@example.com', 'gamma')
             ON CONFLICT(email) DO UPDATE SET username = excluded.username",
            (),
        )
        .unwrap(),
        1
    );
    assert_eq!(
        rows(),
        [
            (1, "acme".into(), "a@example.com".into(), "gamma".into()),
            (2, "acme".into(), "b@example.com".into(), "beta".into()),
        ]
    );
    let before_mismatch = rows();
    assert!(matches!(
        db.execute(
            "INSERT INTO profiles VALUES
                (10, 'acme', 'new@example.com', 'new')
             ON CONFLICT(tenant) DO UPDATE SET username = excluded.username",
            (),
        ),
        Err(Error::Sqlite(_))
    ));
    assert_eq!(rows(), before_mismatch);
}

#[test]
fn upsert_do_update_covers_strict_composite_and_affinity_key_shapes() {
    let directory = tempfile::tempdir().unwrap();
    let db = MultiliteConnection::open(directory.path().join("upsert-key-shapes.sqlite")).unwrap();
    db.update(|transaction| {
        transaction.execute(
            "CREATE TABLE strict_values (
                id INTEGER PRIMARY KEY,
                body TEXT NOT NULL,
                revision INTEGER NOT NULL
            ) STRICT",
            (),
        )?;
        transaction.execute(
            "CREATE TABLE memberships (
                organization TEXT NOT NULL,
                username TEXT NOT NULL,
                next_username TEXT NOT NULL,
                body TEXT NOT NULL,
                PRIMARY KEY (organization, username)
            ) WITHOUT ROWID",
            (),
        )?;
        transaction.execute(
            "CREATE TABLE numeric_keys (
                id INTEGER PRIMARY KEY,
                code NUMERIC NOT NULL UNIQUE,
                body TEXT NOT NULL
            )",
            (),
        )?;
        transaction.execute("INSERT INTO strict_values VALUES (1, 'old', 0)", ())?;
        transaction.execute(
            "INSERT INTO memberships VALUES ('acme', 'alice', 'alice', 'old')",
            (),
        )?;
        transaction.execute("INSERT INTO numeric_keys VALUES (1, 1, 'old')", ())?;
        Ok(())
    })
    .unwrap();

    assert_eq!(
        db.execute(
            "INSERT INTO strict_values VALUES (1, 'updated', 9)
             ON CONFLICT(id) DO UPDATE SET
                body = excluded.body,
                revision = strict_values.revision + 1",
            (),
        )
        .unwrap(),
        1
    );
    assert!(matches!(
        db.execute(
            "INSERT INTO strict_values VALUES (1, x'01', 9)
             ON CONFLICT(id) DO UPDATE SET body = excluded.body",
            (),
        ),
        Err(Error::Sqlite(_))
    ));
    assert_eq!(
        db.query("SELECT id, body, revision FROM strict_values", (), |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, i64>(2)?,
            ))
        },)
            .unwrap(),
        [(1, "updated".into(), 1)]
    );

    assert_eq!(
        db.execute(
            "INSERT INTO memberships VALUES ('acme', 'alice', 'bob', 'moved')
             ON CONFLICT(organization, username) DO UPDATE SET
                username = excluded.next_username,
                next_username = excluded.next_username,
                body = excluded.body",
            (),
        )
        .unwrap(),
        1
    );
    assert_eq!(
        db.query(
            "SELECT organization, username, next_username, body FROM memberships",
            (),
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                ))
            },
        )
        .unwrap(),
        [("acme".into(), "bob".into(), "bob".into(), "moved".into())]
    );

    assert_eq!(
        db.execute(
            "INSERT INTO numeric_keys VALUES (9, 1.0, 'numeric-equivalent')
             ON CONFLICT(code) DO UPDATE SET body = excluded.body",
            (),
        )
        .unwrap(),
        1
    );
    assert_eq!(
        db.query(
            "SELECT id, code, typeof(code), body FROM numeric_keys",
            (),
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                ))
            },
        )
        .unwrap(),
        [(1, 1, "integer".into(), "numeric-equivalent".into())]
    );
}

#[test]
fn schema_conflict_policies_match_sqlite_and_survive_schema_rebuilds() {
    let directory = tempfile::tempdir().unwrap();
    let db = MultiliteConnection::open(directory.path().join("schema-options.sqlite")).unwrap();

    db.execute(
        "CREATE TABLE policy_rows (
            id INTEGER PRIMARY KEY ON CONFLICT REPLACE,
            email TEXT UNIQUE ON CONFLICT IGNORE,
            body TEXT NOT NULL ON CONFLICT REPLACE DEFAULT 'fallback',
            tenant TEXT,
            handle TEXT,
            UNIQUE (tenant, handle) ON CONFLICT REPLACE
        )",
        (),
    )
    .unwrap();
    db.execute(
        "INSERT INTO policy_rows VALUES
            (1, 'one@example', 'first', 'acme', 'one'),
            (2, 'two@example', NULL, 'acme', 'two')",
        (),
    )
    .unwrap();
    assert_eq!(
        db.execute(
            "INSERT INTO policy_rows VALUES
                (3, 'one@example', 'ignored', 'other', 'three')",
            (),
        )
        .unwrap(),
        0
    );
    db.execute(
        "INSERT INTO policy_rows VALUES
            (1, 'replacement@example', 'pk replacement', 'other', 'one')",
        (),
    )
    .unwrap();
    db.execute(
        "INSERT INTO policy_rows VALUES
            (4, 'composite@example', 'composite replacement', 'acme', 'two')",
        (),
    )
    .unwrap();

    assert_eq!(
        db.query(
            "SELECT id, email, body, tenant, handle FROM policy_rows ORDER BY id",
            (),
            |row| Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, String>(4)?,
            )),
        )
        .unwrap(),
        [
            (
                1,
                "replacement@example".into(),
                "pk replacement".into(),
                "other".into(),
                "one".into(),
            ),
            (
                4,
                "composite@example".into(),
                "composite replacement".into(),
                "acme".into(),
                "two".into(),
            ),
        ]
    );

    assert_eq!(
        db.execute(
            "INSERT OR IGNORE INTO policy_rows VALUES
                (1, 'ignored-by-statement@example', 'ignored', 'x', 'x')",
            (),
        )
        .unwrap(),
        0
    );
    assert!(
        db.execute(
            "INSERT OR ABORT INTO policy_rows VALUES
                (5, 'five@example', 'aborted', 'acme', 'two')",
            (),
        )
        .is_err()
    );
    db.execute(
        "INSERT OR REPLACE INTO policy_rows VALUES
            (6, 'replacement@example', 'statement replacement', 'six', 'six')",
        (),
    )
    .unwrap();

    db.execute(
        "ALTER TABLE policy_rows ADD COLUMN state TEXT
            NOT NULL ON CONFLICT REPLACE DEFAULT 'new'",
        (),
    )
    .unwrap();
    db.execute("ALTER TABLE policy_rows RENAME COLUMN state TO status", ())
        .unwrap();
    db.execute(
        "INSERT INTO policy_rows
            (id, email, body, tenant, handle, status)
         VALUES (7, 'seven@example', 'seven', 'seven', 'seven', NULL)",
        (),
    )
    .unwrap();
    assert_eq!(
        db.query(
            "SELECT id, body, status FROM policy_rows ORDER BY id",
            (),
            |row| Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
            )),
        )
        .unwrap(),
        [
            (4, "composite replacement".into(), "new".into()),
            (6, "statement replacement".into(), "new".into()),
            (7, "seven".into(), "new".into()),
        ]
    );

    for sql in [
        "CREATE TABLE auto_notes (id INTEGER PRIMARY KEY AUTOINCREMENT)",
        "CREATE TABLE failing_notes (
            id INTEGER PRIMARY KEY,
            body TEXT UNIQUE ON CONFLICT FAIL
        )",
        "CREATE TABLE rolling_back_notes (
            id INTEGER PRIMARY KEY ON CONFLICT ROLLBACK
        )",
    ] {
        assert!(matches!(db.execute(sql, ()), Err(Error::UnsupportedSql(_))));
    }

    assert_eq!(
        db.query(
            "SELECT count(*) FROM sqlite_schema
             WHERE name IN (
                'auto_notes', 'failing_notes', 'rolling_back_notes', 'sqlite_sequence'
             )",
            (),
            |row| row.get::<_, i64>(0),
        )
        .unwrap(),
        [0],
    );
}

#[test]
fn schema_conflict_policies_cover_strict_composite_without_rowid_tables() {
    let directory = tempfile::tempdir().unwrap();
    let db = MultiliteConnection::open(directory.path().join("strict-policies.sqlite")).unwrap();
    db.execute(
        "CREATE TABLE memberships (
            tenant TEXT,
            member INTEGER,
            body TEXT NOT NULL ON CONFLICT REPLACE DEFAULT 'fallback',
            PRIMARY KEY (tenant, member) ON CONFLICT REPLACE,
            UNIQUE (tenant, body) ON CONFLICT IGNORE
        ) WITHOUT ROWID, STRICT",
        (),
    )
    .unwrap();
    db.execute("INSERT INTO memberships VALUES ('a', 1, NULL)", ())
        .unwrap();
    db.execute("INSERT INTO memberships VALUES ('a', 1, 'replacement')", ())
        .unwrap();
    assert_eq!(
        db.execute("INSERT INTO memberships VALUES ('a', 2, 'replacement')", ())
            .unwrap(),
        0
    );
    db.execute(
        "INSERT OR REPLACE INTO memberships VALUES ('a', 3, 'replacement')",
        (),
    )
    .unwrap();

    assert_eq!(
        db.query(
            "SELECT tenant, member, body FROM memberships",
            (),
            |row| Ok((
                row.get::<_, String>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, String>(2)?,
            )),
        )
        .unwrap(),
        [("a".into(), 3, "replacement".into())]
    );
}

#[test]
fn alter_column_rejects_identity_unsafe_shapes_and_accepts_rebuildable_drops() {
    let directory = tempfile::tempdir().unwrap();
    let db = MultiliteConnection::open(directory.path().join("alter-safety.sqlite")).unwrap();

    db.execute(
        "CREATE TABLE strict_notes (id INTEGER PRIMARY KEY) STRICT",
        (),
    )
    .unwrap();
    db.execute("ALTER TABLE strict_notes ADD COLUMN payload ANY", ())
        .unwrap();
    assert!(matches!(
        db.execute(
            "ALTER TABLE strict_notes ADD COLUMN unsupported VARCHAR",
            ()
        ),
        Err(Error::UnsupportedSql(_))
    ));

    db.execute(
        "CREATE TABLE keyed (
            id INTEGER PRIMARY KEY,
            value TEXT UNIQUE
        )",
        (),
    )
    .unwrap();
    assert!(matches!(
        db.execute("ALTER TABLE keyed DROP COLUMN value", ()),
        Err(Error::UnsupportedSql(_))
    ));
    db.execute(
        "CREATE TABLE checked (
            id INTEGER PRIMARY KEY,
            value TEXT CHECK (length(value) > 0)
        )",
        (),
    )
    .unwrap();
    db.execute("INSERT INTO checked VALUES (1, 'valid')", ())
        .unwrap();
    db.execute("ALTER TABLE checked DROP COLUMN value", ())
        .unwrap();
    db.execute(
        "ALTER TABLE checked ADD COLUMN value TEXT DEFAULT 'replacement'",
        (),
    )
    .unwrap();
    db.execute(
        "CREATE TABLE required (
            id INTEGER PRIMARY KEY,
            value TEXT NOT NULL,
            tail TEXT
        )",
        (),
    )
    .unwrap();
    db.execute("INSERT INTO required VALUES (1, 'required', 'tail')", ())
        .unwrap();
    db.execute("ALTER TABLE required DROP COLUMN value", ())
        .unwrap();

    assert_eq!(
        db.execute(
            "INSERT INTO strict_notes (id, payload) VALUES (1, x'01')",
            ()
        )
        .unwrap(),
        1
    );
    assert_eq!(
        db.query("SELECT typeof(payload) FROM strict_notes", (), |row| {
            row.get::<_, String>(0)
        })
        .unwrap(),
        ["blob"]
    );
    assert_eq!(
        db.query("SELECT id, tail FROM required", (), |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
        })
        .unwrap(),
        [(1, "tail".into())]
    );
    assert_eq!(
        db.query("SELECT id, value FROM checked", (), |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
        })
        .unwrap(),
        [(1, "replacement".into())]
    );
}

#[test]
fn column_rename_keeps_checks_and_expression_indexes_valid_across_rebuild() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("rename-expression-rebuild.sqlite");
    let db = MultiliteConnection::open(&path).unwrap();

    db.update(|transaction| {
        transaction.execute(
            "CREATE TABLE notes (
                id INTEGER PRIMARY KEY,
                body TEXT CONSTRAINT body_nonempty CHECK (length(body) > 0),
                spare TEXT
            )",
            (),
        )?;
        transaction.execute(
            "CREATE INDEX notes_body_search
             ON notes (lower(body), body COLLATE NOCASE DESC)
             WHERE body IS NOT NULL",
            (),
        )?;
        transaction.execute("INSERT INTO notes VALUES (1, 'one', 'spare')", ())?;
        Ok(())
    })
    .unwrap();
    db.execute("ALTER TABLE notes RENAME COLUMN body TO contents", ())
        .unwrap();
    db.execute("ALTER TABLE notes DROP COLUMN spare", ())
        .unwrap();

    assert_eq!(
        db.query("SELECT id, contents FROM notes", (), |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
        })
        .unwrap(),
        [(1, "one".into())]
    );
    assert!(
        db.execute("INSERT INTO notes VALUES (2, '')", ()).is_err(),
        "the renamed CHECK constraint must survive a later table rebuild"
    );
    db.execute("INSERT INTO notes VALUES (2, 'two')", ())
        .unwrap();
    drop(db);

    let stock = Connection::open(&path).unwrap();
    let table_sql = stock
        .query_row(
            "SELECT sql FROM sqlite_schema WHERE type = 'table' AND name = 'notes'",
            (),
            |row| row.get::<_, String>(0),
        )
        .unwrap();
    let index_sql = stock
        .query_row(
            "SELECT sql FROM sqlite_schema
             WHERE type = 'index' AND name = 'notes_body_search'",
            (),
            |row| row.get::<_, String>(0),
        )
        .unwrap();
    assert!(
        table_sql.contains("CHECK (length (\"contents\") > 0)"),
        "{table_sql}"
    );
    assert!(index_sql.contains("lower (\"contents\")"), "{index_sql}");
    assert!(
        index_sql.contains("WHERE \"contents\" IS NOT NULL"),
        "{index_sql}"
    );
    assert_eq!(
        stock
            .query_row("PRAGMA integrity_check", (), |row| row.get::<_, String>(0))
            .unwrap(),
        "ok"
    );
}

#[test]
fn managed_update_adds_multiple_columns_in_statement_order() {
    let directory = tempfile::tempdir().unwrap();
    let db = MultiliteConnection::open(directory.path().join("multi-add.sqlite")).unwrap();
    db.execute("CREATE TABLE notes (id INTEGER PRIMARY KEY)", ())
        .unwrap();
    db.execute("INSERT INTO notes VALUES (1)", ()).unwrap();

    db.update(|transaction| {
        transaction.execute(
            "ALTER TABLE notes ADD COLUMN first TEXT DEFAULT 'first'",
            (),
        )?;
        transaction.execute(
            "ALTER TABLE notes ADD COLUMN second TEXT DEFAULT 'second'",
            (),
        )?;
        Ok(())
    })
    .unwrap();

    assert_eq!(
        db.query("SELECT * FROM notes", (), |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
            ))
        })
        .unwrap(),
        [(1, "first".into(), "second".into())]
    );
}

#[test]
fn drop_constraint_projects_named_unique_check_and_foreign_keys() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("drop-constraint.sqlite");
    let db = MultiliteConnection::open(&path).unwrap();
    db.update(|transaction| {
        transaction.execute(
            "CREATE TABLE parents (
                tenant TEXT NOT NULL,
                id INTEGER NOT NULL,
                PRIMARY KEY (tenant, id)
            ) WITHOUT ROWID, STRICT",
            (),
        )?;
        transaction.execute(
            "CREATE TABLE children (
                row_id INTEGER PRIMARY KEY,
                tenant TEXT,
                parent_id INTEGER,
                email TEXT,
                score INTEGER,
                CONSTRAINT uq_email UNIQUE (tenant, email),
                CONSTRAINT ck_score CHECK (score >= 0),
                CONSTRAINT fk_parent FOREIGN KEY (tenant, parent_id)
                    REFERENCES parents (tenant, id)
            ) STRICT",
            (),
        )?;
        transaction.execute("INSERT INTO parents VALUES ('acme', 1)", ())?;
        transaction.execute(
            "INSERT INTO children VALUES (1, 'acme', 1, 'one@example', 1)",
            (),
        )?;
        Ok(())
    })
    .unwrap();

    assert!(
        db.execute(
            "INSERT INTO children VALUES (2, 'acme', 1, 'one@example', 2)",
            (),
        )
        .is_err()
    );
    db.execute("ALTER TABLE children DROP CONSTRAINT uq_email", ())
        .unwrap();
    db.execute(
        "INSERT INTO children VALUES (2, 'acme', 1, 'one@example', 2)",
        (),
    )
    .unwrap();

    assert!(
        db.execute(
            "INSERT INTO children VALUES (3, 'acme', 1, 'three@example', -1)",
            (),
        )
        .is_err()
    );
    db.execute("ALTER TABLE children DROP CONSTRAINT ck_score", ())
        .unwrap();
    db.execute(
        "INSERT INTO children VALUES (3, 'acme', 1, 'three@example', -1)",
        (),
    )
    .unwrap();

    assert!(
        db.execute(
            "INSERT INTO children VALUES (4, 'missing', 9, 'four@example', 4)",
            (),
        )
        .is_err()
    );
    db.execute("ALTER TABLE children DROP CONSTRAINT fk_parent", ())
        .unwrap();
    db.execute(
        "INSERT INTO children VALUES (4, 'missing', 9, 'four@example', 4)",
        (),
    )
    .unwrap();
    db.execute("DELETE FROM parents WHERE tenant = 'acme' AND id = 1", ())
        .unwrap();
    drop(db);

    let reopened = MultiliteConnection::open(&path).unwrap();
    assert_eq!(
        reopened
            .query("SELECT count(*) FROM children", (), |row| row
                .get::<_, i64>(0))
            .unwrap(),
        [4]
    );
    reopened
        .execute(
            "INSERT INTO children VALUES (5, 'missing', 9, 'one@example', -2)",
            (),
        )
        .unwrap();
}

#[test]
fn drop_constraint_is_atomic_inside_managed_updates() {
    let directory = tempfile::tempdir().unwrap();
    let db =
        MultiliteConnection::open(directory.path().join("drop-constraint-atomic.sqlite")).unwrap();
    db.execute(
        "CREATE TABLE notes (
            id INTEGER PRIMARY KEY,
            body TEXT,
            CONSTRAINT uq_body UNIQUE (body)
        )",
        (),
    )
    .unwrap();
    db.execute("INSERT INTO notes VALUES (1, 'one')", ())
        .unwrap();

    let result: multilite::Result<()> = db.update(|transaction| {
        transaction.execute("ALTER TABLE notes DROP CONSTRAINT uq_body", ())?;
        transaction.execute("INSERT INTO notes VALUES (2, 'one')", ())?;
        Err(Error::UnsupportedSql("injected rollback"))
    });
    assert!(result.is_err());
    assert_eq!(
        db.query("SELECT count(*) FROM notes", (), |row| row.get::<_, i64>(0))
            .unwrap(),
        [1]
    );
    assert!(
        db.execute("INSERT INTO notes VALUES (3, 'one')", ())
            .is_err()
    );
}

#[test]
fn drop_constraint_handles_names_parameters_and_foreign_key_dependencies() {
    let directory = tempfile::tempdir().unwrap();
    let db =
        MultiliteConnection::open(directory.path().join("drop-constraint-edges.sqlite")).unwrap();
    db.update(|transaction| {
        transaction.execute(
            "CREATE TABLE parents (
                id INTEGER PRIMARY KEY,
                code TEXT,
                CONSTRAINT \"Odd Unique\" UNIQUE (code)
            )",
            (),
        )?;
        transaction.execute(
            "CREATE TABLE children (
                id INTEGER PRIMARY KEY,
                code TEXT,
                CONSTRAINT fk_code FOREIGN KEY (code) REFERENCES parents (code)
            )",
            (),
        )?;
        Ok(())
    })
    .unwrap();

    assert!(matches!(
        db.execute("ALTER TABLE parents DROP CONSTRAINT \"odd unique\"", ()),
        Err(Error::UnsupportedSql(_))
    ));
    db.execute("ALTER TABLE children DROP CONSTRAINT fk_code", ())
        .unwrap();
    assert!(
        db.execute(
            "ALTER TABLE parents DROP CONSTRAINT \"Odd Unique\"",
            [1_i64],
        )
        .is_err(),
        "custom DDL must reject unused bound parameters"
    );
    db.execute("ALTER TABLE parents DROP CONSTRAINT \"Odd Unique\"", ())
        .unwrap();
    let reused_name = db.execute(
        "ALTER TABLE parents ADD COLUMN extra TEXT
         CONSTRAINT \"odd unique\" CHECK (extra IS NULL)",
        (),
    );
    assert!(
        matches!(reused_name, Err(Error::UnsupportedSql(_))),
        "retired constraint name was unexpectedly reusable: {reused_name:?}"
    );

    assert!(matches!(
        db.execute("ALTER TABLE parents DROP CONSTRAINT missing", ()),
        Err(Error::UnsupportedSql(_))
    ));
    assert!(matches!(
        db.execute(
            "CREATE TABLE duplicate_names (
                id INTEGER CONSTRAINT same PRIMARY KEY,
                value TEXT,
                CONSTRAINT SAME CHECK (value <> '')
            )",
            (),
        ),
        Err(Error::InvalidMultiliteOp(_))
    ));
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

    db.execute("CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)", ())
        .unwrap();
    db.execute("INSERT INTO notes VALUES (1, 'original')", ())
        .unwrap();

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
    for sql in [
        "INSERT INTO notes VALUES (1, 'updated')
         ON CONFLICT(id) DO UPDATE
         SET body = (SELECT value FROM __multilite__meta)",
        "INSERT INTO notes VALUES (1, 'updated')
         ON CONFLICT(id) DO UPDATE SET body = excluded.body
         WHERE EXISTS (SELECT 1 FROM __multilite__pending)",
    ] {
        assert!(matches!(db.execute(sql, ()), Err(Error::UnsupportedSql(_))));
    }

    assert_eq!(internal_tables(), initial_internal_tables);
    assert_eq!(read_note(&db), "original");
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
