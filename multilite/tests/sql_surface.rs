use multilite::{Error, MultiliteConnection};
use rusqlite::Connection;

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
    assert_eq!(
        db.query(
            "SELECT sql FROM sqlite_schema WHERE name = 'notes_identity'",
            (),
            |row| row.get::<_, String>(0),
        )
        .unwrap(),
        ["CREATE UNIQUE INDEX notes_identity ON notes (tenant, body)"]
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
        "DROP TABLE notes",
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
fn alter_column_rejects_shapes_that_cannot_be_replayed_safely() {
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
    assert!(matches!(
        db.execute("ALTER TABLE checked DROP COLUMN value", ()),
        Err(Error::UnsupportedSql(_))
    ));
    db.execute(
        "CREATE TABLE required (
            id INTEGER PRIMARY KEY,
            value TEXT NOT NULL
        )",
        (),
    )
    .unwrap();
    assert!(matches!(
        db.execute("ALTER TABLE required DROP COLUMN value", ()),
        Err(Error::UnsupportedSql(_))
    ));

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
