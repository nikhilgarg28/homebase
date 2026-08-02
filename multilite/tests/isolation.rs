use std::sync::Arc;

use homebase_client::ServerHandle;
use homebase_core::messages::KernelError;
use homebase_core::space::SpaceId;
use multilite::{
    Error, IsolationLevel, MultiliteConnection, OpenOptions, PushOutcome, PushRejection,
    UpdateOptions,
};

mod common;

use common::{router, server};

const CREATE_BOOKINGS: &str = "CREATE TABLE bookings (id INTEGER PRIMARY KEY, day TEXT NOT NULL)";

#[test]
fn snapshot_conditional_inserts_both_admit_and_converge() {
    let directory = tempfile::tempdir().unwrap();
    let authority = server();
    let first = MultiliteConnection::open_with(
        directory.path().join("snapshot-first.sqlite"),
        OpenOptions::new().server(router(Arc::clone(&authority))),
    )
    .unwrap();
    assert!(authority.create_space(SpaceId(first.database_id().to_bytes())));
    let second = MultiliteConnection::open_with(
        directory.path().join("snapshot-second.sqlite"),
        OpenOptions::new()
            .invitation(first.replica_invitation())
            .server(router(Arc::clone(&authority))),
    )
    .unwrap();
    synchronize_schema(&first, &second);

    assert!(conditionally_book(&first, IsolationLevel::Snapshot, 1));
    assert!(conditionally_book(&second, IsolationLevel::Snapshot, 2));

    assert_eq!(first.push().unwrap(), PushOutcome::Drained);
    assert_eq!(second.push().unwrap(), PushOutcome::Drained);
    converge(&first, &second);

    let expected = vec![(1, String::from("mon")), (2, String::from("mon"))];
    assert_eq!(bookings(&first), expected);
    assert_eq!(bookings(&second), expected);
}

#[test]
fn serializable_read_conflict_repairs_after_reopen_and_converges() {
    let directory = tempfile::tempdir().unwrap();
    let second_path = directory.path().join("serializable-second.sqlite");
    let authority = server();
    let first = MultiliteConnection::open_with(
        directory.path().join("serializable-first.sqlite"),
        OpenOptions::new().server(router(Arc::clone(&authority))),
    )
    .unwrap();
    assert!(authority.create_space(SpaceId(first.database_id().to_bytes())));
    let second = MultiliteConnection::open_with(
        &second_path,
        OpenOptions::new()
            .invitation(first.replica_invitation())
            .server(router(Arc::clone(&authority))),
    )
    .unwrap();
    synchronize_schema(&first, &second);

    assert!(conditionally_book(&first, IsolationLevel::Serializable, 1));
    assert!(conditionally_book(&second, IsolationLevel::Serializable, 2));
    assert_eq!(bookings(&second), [(2, String::from("mon"))]);

    assert_eq!(first.push().unwrap(), PushOutcome::Drained);
    second.pull().unwrap();
    assert!(matches!(
        second.rebase(),
        Err(Error::RebasePendingSubmissions)
    ));

    let rejection = rejected(second.push().unwrap());
    assert_range_assertion_failed(&rejection);
    drop(second);

    let second = MultiliteConnection::open_with(
        &second_path,
        OpenOptions::new().server(router(Arc::clone(&authority))),
    )
    .unwrap();
    assert_eq!(bookings(&second), [(2, String::from("mon"))]);

    second.rollback(&rejection).unwrap();
    assert!(bookings(&second).is_empty());
    assert!(matches!(
        second.rebase(),
        Err(Error::RebasePendingSubmissions)
    ));

    assert_eq!(second.push().unwrap(), PushOutcome::Drained);
    converge(&first, &second);

    let expected = vec![(1, String::from("mon"))];
    assert_eq!(bookings(&first), expected);
    assert_eq!(bookings(&second), expected);
}

#[test]
fn conditional_schema_noops_track_serializable_name_reads() {
    for isolation in [IsolationLevel::Snapshot, IsolationLevel::Serializable] {
        for drop_first in [false, true] {
            let directory = tempfile::tempdir().unwrap();
            let authority = server();
            let first = MultiliteConnection::open_with(
                directory.path().join(format!(
                    "conditional-first-{isolation:?}-{drop_first}.sqlite"
                )),
                OpenOptions::new().server(router(Arc::clone(&authority))),
            )
            .unwrap();
            assert!(authority.create_space(SpaceId(first.database_id().to_bytes())));
            let second = MultiliteConnection::open_with(
                directory.path().join(format!(
                    "conditional-second-{isolation:?}-{drop_first}.sqlite"
                )),
                OpenOptions::new()
                    .invitation(first.replica_invitation())
                    .server(router(Arc::clone(&authority))),
            )
            .unwrap();

            first
                .update(|transaction| {
                    transaction.execute(
                        "CREATE TABLE observed (id INTEGER PRIMARY KEY, body TEXT)",
                        (),
                    )?;
                    transaction
                        .execute("CREATE TABLE audit (id INTEGER PRIMARY KEY, body TEXT)", ())?;
                    Ok(())
                })
                .unwrap();
            assert_eq!(first.push().unwrap(), PushOutcome::Drained);
            second.pull().unwrap();
            second.rebase().unwrap();

            first
                .update_with(UpdateOptions::new(isolation), |transaction| {
                    transaction.execute(
                        "CREATE TABLE IF NOT EXISTS observed (
                            id INTEGER PRIMARY KEY,
                            ignored BLOB
                        )",
                        (),
                    )?;
                    transaction.execute("INSERT INTO audit VALUES (1, 'saw-observed')", ())?;
                    Ok(())
                })
                .unwrap();
            second.execute("DROP TABLE observed", ()).unwrap();

            let expect_observer_rejection = drop_first && isolation == IsolationLevel::Serializable;
            if drop_first {
                assert_eq!(second.push().unwrap(), PushOutcome::Drained);
                if expect_observer_rejection {
                    let rejection = rejected(first.push().unwrap());
                    assert_range_assertion_failed(&rejection);
                    first.rollback(&rejection).unwrap();
                    assert_eq!(first.push().unwrap(), PushOutcome::Drained);
                } else {
                    assert_eq!(first.push().unwrap(), PushOutcome::Drained);
                }
            } else {
                assert_eq!(first.push().unwrap(), PushOutcome::Drained);
                assert_eq!(second.push().unwrap(), PushOutcome::Drained);
            }

            converge(&first, &second);
            for database in [&first, &second] {
                assert!(
                    database
                        .query("SELECT * FROM observed", (), |_| Ok(()))
                        .is_err()
                );
                assert_eq!(
                    database
                        .query("SELECT count(*) FROM audit", (), |row| row.get::<_, i64>(0))
                        .unwrap(),
                    [i64::from(!expect_observer_rejection)]
                );
            }
        }
    }
}

#[test]
fn primary_key_collisions_are_mandatory_at_both_isolation_levels() {
    let directory = tempfile::tempdir().unwrap();
    for (label, isolation) in [
        ("snapshot", IsolationLevel::Snapshot),
        ("serializable", IsolationLevel::Serializable),
    ] {
        let authority = server();
        let first = MultiliteConnection::open_with(
            directory.path().join(format!("{label}-pk-first.sqlite")),
            OpenOptions::new().server(router(Arc::clone(&authority))),
        )
        .unwrap();
        assert!(authority.create_space(SpaceId(first.database_id().to_bytes())));
        let second = MultiliteConnection::open_with(
            directory.path().join(format!("{label}-pk-second.sqlite")),
            OpenOptions::new()
                .invitation(first.replica_invitation())
                .server(router(Arc::clone(&authority))),
        )
        .unwrap();
        synchronize_schema(&first, &second);

        insert_without_read(&first, isolation, 7, "winner");
        insert_without_read(&second, isolation, 7, "loser");
        assert_eq!(first.push().unwrap(), PushOutcome::Drained);
        let rejection = rejected(second.push().unwrap());
        assert_range_assertion_failed(&rejection);
        second.rollback(&rejection).unwrap();
        assert_eq!(second.push().unwrap(), PushOutcome::Drained);
        converge(&first, &second);

        let expected = vec![(7, String::from("winner"))];
        assert_eq!(bookings(&first), expected, "{label}");
        assert_eq!(bookings(&second), expected, "{label}");
    }
}

#[test]
fn composite_unique_collisions_are_mandatory_at_both_isolation_levels() {
    let directory = tempfile::tempdir().unwrap();
    for (label, isolation) in [
        ("snapshot", IsolationLevel::Snapshot),
        ("serializable", IsolationLevel::Serializable),
    ] {
        let authority = server();
        let first = MultiliteConnection::open_with(
            directory
                .path()
                .join(format!("{label}-unique-first.sqlite")),
            OpenOptions::new().server(router(Arc::clone(&authority))),
        )
        .unwrap();
        assert!(authority.create_space(SpaceId(first.database_id().to_bytes())));
        let second = MultiliteConnection::open_with(
            directory
                .path()
                .join(format!("{label}-unique-second.sqlite")),
            OpenOptions::new()
                .invitation(first.replica_invitation())
                .server(router(Arc::clone(&authority))),
        )
        .unwrap();
        first
            .execute(
                "CREATE TABLE accounts (
                    id INTEGER PRIMARY KEY,
                    organization TEXT,
                    email TEXT,
                    UNIQUE (organization, email)
                )",
                (),
            )
            .unwrap();
        assert_eq!(first.push().unwrap(), PushOutcome::Drained);
        second.pull().unwrap();
        second.rebase().unwrap();

        for (database, id) in [(&first, 1_i64), (&second, 2_i64)] {
            database
                .update_with(UpdateOptions::new(isolation), |transaction| {
                    transaction.execute(
                        "INSERT INTO accounts VALUES (?1, 'acme', 'shared@example.com')",
                        [id],
                    )?;
                    Ok(())
                })
                .unwrap();
        }
        assert_eq!(first.push().unwrap(), PushOutcome::Drained);
        let rejection = rejected(second.push().unwrap());
        assert_range_assertion_failed(&rejection);
        second.rollback(&rejection).unwrap();
        assert_eq!(second.push().unwrap(), PushOutcome::Drained);
        converge(&first, &second);

        for database in [&first, &second] {
            assert_eq!(
                database
                    .query("SELECT id FROM accounts", (), |row| row.get::<_, i64>(0))
                    .unwrap(),
                [1],
                "{label}"
            );
        }
    }
}

#[test]
fn secondary_index_ddl_does_not_invalidate_unsynced_row_writes_at_either_isolation() {
    let directory = tempfile::tempdir().unwrap();
    for (label, isolation) in [
        ("snapshot", IsolationLevel::Snapshot),
        ("serializable", IsolationLevel::Serializable),
    ] {
        let authority = server();
        let first = MultiliteConnection::open_with(
            directory
                .path()
                .join(format!("{label}-secondary-first.sqlite")),
            OpenOptions::new().server(router(Arc::clone(&authority))),
        )
        .unwrap();
        assert!(authority.create_space(SpaceId(first.database_id().to_bytes())));
        let second = MultiliteConnection::open_with(
            directory
                .path()
                .join(format!("{label}-secondary-second.sqlite")),
            OpenOptions::new()
                .invitation(first.replica_invitation())
                .server(router(Arc::clone(&authority))),
        )
        .unwrap();
        synchronize_schema(&first, &second);

        first
            .execute(
                "CREATE INDEX bookings_day_search ON bookings (
                    day COLLATE NOCASE DESC,
                    lower(trim(day)) ASC,
                    day
                ) WHERE day IS NOT NULL",
                (),
            )
            .unwrap();
        assert_eq!(first.push().unwrap(), PushOutcome::Drained, "{label}");

        // The second replica deliberately does not pull the admitted index.
        insert_without_read(&second, isolation, 7, "MiXeD ");
        assert_eq!(
            second.push().unwrap(),
            PushOutcome::Drained,
            "{label}: access-path DDL invalidated a stale row write"
        );
        converge(&first, &second);

        for database in [&first, &second] {
            assert_eq!(
                database
                    .query(
                        "SELECT name FROM sqlite_schema
                         WHERE type = 'index' AND name = 'bookings_day_search'",
                        (),
                        |row| row.get::<_, String>(0),
                    )
                    .unwrap(),
                ["bookings_day_search"],
                "{label}"
            );
            assert_eq!(bookings(database), [(7, String::from("MiXeD "))], "{label}");
        }
    }
}

#[test]
fn table_rename_does_not_invalidate_unsynced_row_writes_at_either_isolation() {
    let directory = tempfile::tempdir().unwrap();
    for (label, isolation) in [
        ("snapshot", IsolationLevel::Snapshot),
        ("serializable", IsolationLevel::Serializable),
    ] {
        let authority = server();
        let first = MultiliteConnection::open_with(
            directory
                .path()
                .join(format!("{label}-rename-first.sqlite")),
            OpenOptions::new().server(router(Arc::clone(&authority))),
        )
        .unwrap();
        assert!(authority.create_space(SpaceId(first.database_id().to_bytes())));
        let second = MultiliteConnection::open_with(
            directory
                .path()
                .join(format!("{label}-rename-second.sqlite")),
            OpenOptions::new()
                .invitation(first.replica_invitation())
                .server(router(Arc::clone(&authority))),
        )
        .unwrap();
        synchronize_schema(&first, &second);

        first
            .execute("ALTER TABLE bookings RENAME TO archived_bookings", ())
            .unwrap();
        assert_eq!(first.push().unwrap(), PushOutcome::Drained, "{label}");

        // The second replica deliberately writes through the old local name.
        insert_without_read(&second, isolation, 7, "mon");
        assert_eq!(
            second.push().unwrap(),
            PushOutcome::Drained,
            "{label}: identity-preserving rename invalidated a stale row write"
        );
        converge(&first, &second);

        for database in [&first, &second] {
            assert!(
                database
                    .query("SELECT id FROM bookings", (), |row| row.get::<_, i64>(0))
                    .is_err(),
                "{label}"
            );
            assert_eq!(
                database
                    .query(
                        "SELECT id, day FROM archived_bookings ORDER BY id",
                        (),
                        |row| Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?)),
                    )
                    .unwrap(),
                [(7, String::from("mon"))],
                "{label}"
            );
        }
    }
}

#[test]
fn serializable_read_tracing_keeps_stable_identity_across_an_in_transaction_rename() {
    let directory = tempfile::tempdir().unwrap();
    let database =
        MultiliteConnection::open(directory.path().join("rename-read-trace.sqlite")).unwrap();
    database.execute(CREATE_BOOKINGS, ()).unwrap();
    database
        .execute("INSERT INTO bookings VALUES (1, 'mon')", ())
        .unwrap();

    database
        .update_with(
            UpdateOptions::new(IsolationLevel::Serializable),
            |transaction| {
                assert_eq!(
                    transaction.query("SELECT id FROM bookings", (), |row| {
                        row.get::<_, i64>(0)
                    })?,
                    [1]
                );
                transaction.execute("ALTER TABLE bookings RENAME TO archived_bookings", ())?;
                assert_eq!(
                    transaction.query("SELECT id FROM archived_bookings", (), |row| {
                        row.get::<_, i64>(0)
                    })?,
                    [1]
                );
                Ok(())
            },
        )
        .unwrap();

    assert_eq!(
        database
            .query("SELECT id FROM archived_bookings", (), |row| {
                row.get::<_, i64>(0)
            })
            .unwrap(),
        [1]
    );
}

#[test]
fn coarse_serializable_reads_ignore_secondary_indexes_and_conflict_across_disjoint_predicates() {
    let directory = tempfile::tempdir().unwrap();
    let authority = server();
    let first = MultiliteConnection::open_with(
        directory.path().join("serializable-point-first.sqlite"),
        OpenOptions::new().server(router(Arc::clone(&authority))),
    )
    .unwrap();
    assert!(authority.create_space(SpaceId(first.database_id().to_bytes())));
    let second = MultiliteConnection::open_with(
        directory.path().join("serializable-point-second.sqlite"),
        OpenOptions::new()
            .invitation(first.replica_invitation())
            .server(router(Arc::clone(&authority))),
    )
    .unwrap();
    synchronize_schema(&first, &second);
    first
        .execute("CREATE INDEX bookings_day ON bookings (day)", ())
        .unwrap();
    assert_eq!(first.push().unwrap(), PushOutcome::Drained);
    second.pull().unwrap();
    second.rebase().unwrap();

    assert!(conditionally_book_after_point_read(
        &first,
        IsolationLevel::Serializable,
        1,
        "mon"
    ));
    assert!(conditionally_book_after_point_read(
        &second,
        IsolationLevel::Serializable,
        2,
        "tue"
    ));

    assert_eq!(first.push().unwrap(), PushOutcome::Drained);
    let rejection = rejected(second.push().unwrap());
    assert_range_assertion_failed(&rejection);
    second.rollback(&rejection).unwrap();
    assert_eq!(second.push().unwrap(), PushOutcome::Drained);
    converge(&first, &second);

    let expected = vec![(1, String::from("mon"))];
    assert_eq!(bookings(&first), expected);
    assert_eq!(bookings(&second), expected);
}

#[test]
fn serializable_delete_predicates_conflict_across_disjoint_rows() {
    let directory = tempfile::tempdir().unwrap();
    let authority = server();
    let first = MultiliteConnection::open_with(
        directory.path().join("serializable-delete-first.sqlite"),
        OpenOptions::new().server(router(Arc::clone(&authority))),
    )
    .unwrap();
    assert!(authority.create_space(SpaceId(first.database_id().to_bytes())));
    let second = MultiliteConnection::open_with(
        directory.path().join("serializable-delete-second.sqlite"),
        OpenOptions::new()
            .invitation(first.replica_invitation())
            .server(router(Arc::clone(&authority))),
    )
    .unwrap();
    synchronize_schema(&first, &second);
    first
        .execute("INSERT INTO bookings VALUES (1, 'mon'), (2, 'tue')", ())
        .unwrap();
    assert_eq!(first.push().unwrap(), PushOutcome::Drained);
    second.pull().unwrap();
    second.rebase().unwrap();

    for (database, id) in [(&first, 1_i64), (&second, 2_i64)] {
        database
            .update_with(
                UpdateOptions::new(IsolationLevel::Serializable),
                |transaction| {
                    transaction.execute("DELETE FROM bookings WHERE id = ?1", [id])?;
                    Ok(())
                },
            )
            .unwrap();
    }

    assert_eq!(first.push().unwrap(), PushOutcome::Drained);
    let rejection = rejected(second.push().unwrap());
    assert_range_assertion_failed(&rejection);
    second.rollback(&rejection).unwrap();
    assert_eq!(second.push().unwrap(), PushOutcome::Drained);
    converge(&first, &second);

    let expected = vec![(2, String::from("tue"))];
    assert_eq!(bookings(&first), expected);
    assert_eq!(bookings(&second), expected);
}

#[test]
fn serializable_update_predicates_conflict_across_disjoint_rows() {
    let directory = tempfile::tempdir().unwrap();
    let authority = server();
    let first = MultiliteConnection::open_with(
        directory.path().join("serializable-update-first.sqlite"),
        OpenOptions::new().server(router(Arc::clone(&authority))),
    )
    .unwrap();
    assert!(authority.create_space(SpaceId(first.database_id().to_bytes())));
    let second = MultiliteConnection::open_with(
        directory.path().join("serializable-update-second.sqlite"),
        OpenOptions::new()
            .invitation(first.replica_invitation())
            .server(router(Arc::clone(&authority))),
    )
    .unwrap();
    synchronize_schema(&first, &second);
    first
        .execute("INSERT INTO bookings VALUES (1, 'mon'), (2, 'tue')", ())
        .unwrap();
    assert_eq!(first.push().unwrap(), PushOutcome::Drained);
    second.pull().unwrap();
    second.rebase().unwrap();

    for (database, id, day) in [(&first, 1_i64, "monday"), (&second, 2_i64, "tuesday")] {
        database
            .update_with(
                UpdateOptions::new(IsolationLevel::Serializable),
                |transaction| {
                    transaction.execute("UPDATE bookings SET day = ?1 WHERE id = ?2", (day, id))?;
                    Ok(())
                },
            )
            .unwrap();
    }

    assert_eq!(first.push().unwrap(), PushOutcome::Drained);
    let rejection = rejected(second.push().unwrap());
    assert_range_assertion_failed(&rejection);
    second.rollback(&rejection).unwrap();
    assert_eq!(second.push().unwrap(), PushOutcome::Drained);
    converge(&first, &second);

    let expected = vec![(1, String::from("monday")), (2, String::from("tue"))];
    assert_eq!(bookings(&first), expected);
    assert_eq!(bookings(&second), expected);
}

#[test]
fn update_from_sources_are_reads_only_under_serializable_isolation() {
    let directory = tempfile::tempdir().unwrap();
    for (label, isolation, expect_rejection) in [
        ("snapshot", IsolationLevel::Snapshot, false),
        ("serializable", IsolationLevel::Serializable, true),
    ] {
        let authority = server();
        let first = MultiliteConnection::open_with(
            directory.path().join(format!("{label}-from-first.sqlite")),
            OpenOptions::new().server(router(Arc::clone(&authority))),
        )
        .unwrap();
        assert!(authority.create_space(SpaceId(first.database_id().to_bytes())));
        let second = MultiliteConnection::open_with(
            directory.path().join(format!("{label}-from-second.sqlite")),
            OpenOptions::new()
                .invitation(first.replica_invitation())
                .server(router(Arc::clone(&authority))),
        )
        .unwrap();

        first.execute(CREATE_BOOKINGS, ()).unwrap();
        first
            .execute(
                "CREATE TABLE replacements (id INTEGER PRIMARY KEY, day TEXT NOT NULL)",
                (),
            )
            .unwrap();
        first
            .execute("INSERT INTO bookings VALUES (1, 'old')", ())
            .unwrap();
        first
            .execute("INSERT INTO replacements VALUES (1, 'mon')", ())
            .unwrap();
        assert_eq!(first.push().unwrap(), PushOutcome::Drained);
        second.pull().unwrap();
        second.rebase().unwrap();

        first
            .update_with(UpdateOptions::new(isolation), |transaction| {
                transaction.execute(
                    "WITH source AS (SELECT id, day FROM replacements)
                     UPDATE bookings SET day = source.day
                     FROM source WHERE bookings.id = source.id",
                    (),
                )?;
                Ok(())
            })
            .unwrap();
        second
            .update_with(
                UpdateOptions::new(IsolationLevel::Snapshot),
                |transaction| {
                    transaction.execute("UPDATE replacements SET day = 'tue' WHERE id = 1", ())?;
                    Ok(())
                },
            )
            .unwrap();

        assert_eq!(second.push().unwrap(), PushOutcome::Drained);
        if expect_rejection {
            let rejection = rejected(first.push().unwrap());
            assert_range_assertion_failed(&rejection);
            first.rollback(&rejection).unwrap();
            assert_eq!(first.push().unwrap(), PushOutcome::Drained);
        } else {
            assert_eq!(first.push().unwrap(), PushOutcome::Drained);
        }
        converge(&first, &second);

        let expected_day = if expect_rejection { "old" } else { "mon" };
        for database in [&first, &second] {
            assert_eq!(
                database
                    .query("SELECT day FROM bookings", (), |row| row
                        .get::<_, String>(0))
                    .unwrap(),
                [expected_day],
                "{label}"
            );
            assert_eq!(
                database
                    .query("SELECT day FROM replacements", (), |row| {
                        row.get::<_, String>(0)
                    })
                    .unwrap(),
                ["tue"],
                "{label}"
            );
        }
    }
}

#[test]
fn direct_returning_projection_is_conservatively_serializable_at_table_scope() {
    let directory = tempfile::tempdir().unwrap();
    for (label, isolation, expect_rejection) in [
        ("snapshot", IsolationLevel::Snapshot, false),
        ("serializable", IsolationLevel::Serializable, true),
    ] {
        let authority = server();
        let first = MultiliteConnection::open_with(
            directory
                .path()
                .join(format!("{label}-projection-first.sqlite")),
            OpenOptions::new().server(router(Arc::clone(&authority))),
        )
        .unwrap();
        assert!(authority.create_space(SpaceId(first.database_id().to_bytes())));
        let second = MultiliteConnection::open_with(
            directory
                .path()
                .join(format!("{label}-projection-second.sqlite")),
            OpenOptions::new()
                .invitation(first.replica_invitation())
                .server(router(Arc::clone(&authority))),
        )
        .unwrap();
        synchronize_schema(&first, &second);

        for (database, id, day) in [(&first, 1_i64, "mon"), (&second, 2_i64, "tue")] {
            database
                .update_with(UpdateOptions::new(isolation), |transaction| {
                    assert_eq!(
                        transaction.query(
                            "INSERT INTO bookings VALUES (?1, ?2) RETURNING id",
                            (id, day),
                            |row| row.get::<_, i64>(0),
                        )?,
                        [id]
                    );
                    Ok(())
                })
                .unwrap();
        }

        assert_eq!(first.push().unwrap(), PushOutcome::Drained);
        if expect_rejection {
            let rejection = rejected(second.push().unwrap());
            assert_range_assertion_failed(&rejection);
            second.rollback(&rejection).unwrap();
            assert_eq!(second.push().unwrap(), PushOutcome::Drained);
        } else {
            assert_eq!(second.push().unwrap(), PushOutcome::Drained);
        }
        converge(&first, &second);

        let expected = if expect_rejection {
            vec![(1, String::from("mon"))]
        } else {
            vec![(1, String::from("mon")), (2, String::from("tue"))]
        };
        for database in [&first, &second] {
            assert_eq!(bookings(database), expected, "{label}");
        }
    }
}

#[test]
fn returning_subqueries_are_reads_only_under_serializable_isolation() {
    let directory = tempfile::tempdir().unwrap();
    for (label, isolation, expect_rejection) in [
        ("snapshot", IsolationLevel::Snapshot, false),
        ("serializable", IsolationLevel::Serializable, true),
    ] {
        let authority = server();
        let first = MultiliteConnection::open_with(
            directory
                .path()
                .join(format!("{label}-returning-first.sqlite")),
            OpenOptions::new().server(router(Arc::clone(&authority))),
        )
        .unwrap();
        assert!(authority.create_space(SpaceId(first.database_id().to_bytes())));
        let second = MultiliteConnection::open_with(
            directory
                .path()
                .join(format!("{label}-returning-second.sqlite")),
            OpenOptions::new()
                .invitation(first.replica_invitation())
                .server(router(Arc::clone(&authority))),
        )
        .unwrap();

        first.execute(CREATE_BOOKINGS, ()).unwrap();
        first
            .execute(
                "CREATE TABLE calendar (id INTEGER PRIMARY KEY, label TEXT NOT NULL)",
                (),
            )
            .unwrap();
        first
            .execute("INSERT INTO bookings VALUES (1, 'old')", ())
            .unwrap();
        first
            .execute("INSERT INTO calendar VALUES (1, 'mon')", ())
            .unwrap();
        assert_eq!(first.push().unwrap(), PushOutcome::Drained);
        second.pull().unwrap();
        second.rebase().unwrap();

        first
            .update_with(UpdateOptions::new(isolation), |transaction| {
                assert_eq!(
                    transaction.query(
                        "UPDATE bookings SET day = 'changed' WHERE id = 1
                         RETURNING (SELECT label FROM calendar WHERE id = 1)",
                        (),
                        |row| row.get::<_, String>(0),
                    )?,
                    ["mon"]
                );
                Ok(())
            })
            .unwrap();
        second
            .execute("UPDATE calendar SET label = 'tue' WHERE id = 1", ())
            .unwrap();

        assert_eq!(second.push().unwrap(), PushOutcome::Drained);
        if expect_rejection {
            let rejection = rejected(first.push().unwrap());
            assert_range_assertion_failed(&rejection);
            first.rollback(&rejection).unwrap();
            assert_eq!(first.push().unwrap(), PushOutcome::Drained);
        } else {
            assert_eq!(first.push().unwrap(), PushOutcome::Drained);
        }
        converge(&first, &second);

        let expected_day = if expect_rejection { "old" } else { "changed" };
        for database in [&first, &second] {
            assert_eq!(
                database
                    .query("SELECT day FROM bookings", (), |row| row
                        .get::<_, String>(0))
                    .unwrap(),
                [expected_day],
                "{label}"
            );
            assert_eq!(
                database
                    .query("SELECT label FROM calendar", (), |row| {
                        row.get::<_, String>(0)
                    })
                    .unwrap(),
                ["tue"],
                "{label}"
            );
        }
    }
}

fn synchronize_schema<H1, H2>(source: &MultiliteConnection<H1>, replica: &MultiliteConnection<H2>)
where
    H1: ServerHandle + Send + Sync + 'static,
    H2: ServerHandle + Send + Sync + 'static,
{
    source.execute(CREATE_BOOKINGS, ()).unwrap();
    assert_eq!(source.push().unwrap(), PushOutcome::Drained);
    replica.pull().unwrap();
    replica.rebase().unwrap();
}

fn conditionally_book<H>(
    database: &MultiliteConnection<H>,
    isolation: IsolationLevel,
    id: i64,
) -> bool
where
    H: ServerHandle + Send + Sync + 'static,
{
    database
        .update_with(UpdateOptions::new(isolation), |transaction| {
            let count = transaction.query(
                "SELECT count(*) FROM bookings WHERE day = ?1",
                ["mon"],
                |row| row.get::<_, i64>(0),
            )?[0];
            if count == 0 {
                transaction.execute("INSERT INTO bookings VALUES (?1, ?2)", (id, "mon"))?;
                Ok(true)
            } else {
                Ok(false)
            }
        })
        .unwrap()
}

fn insert_without_read<H>(
    database: &MultiliteConnection<H>,
    isolation: IsolationLevel,
    id: i64,
    day: &str,
) where
    H: ServerHandle + Send + Sync + 'static,
{
    database
        .update_with(UpdateOptions::new(isolation), |transaction| {
            transaction.execute("INSERT INTO bookings VALUES (?1, ?2)", (id, day))?;
            Ok(())
        })
        .unwrap();
}

fn conditionally_book_after_point_read<H>(
    database: &MultiliteConnection<H>,
    isolation: IsolationLevel,
    id: i64,
    day: &str,
) -> bool
where
    H: ServerHandle + Send + Sync + 'static,
{
    database
        .update_with(UpdateOptions::new(isolation), |transaction| {
            let count =
                transaction.query("SELECT count(*) FROM bookings WHERE id = ?1", [id], |row| {
                    row.get::<_, i64>(0)
                })?[0];
            if count == 0 {
                transaction.execute("INSERT INTO bookings VALUES (?1, ?2)", (id, day))?;
                Ok(true)
            } else {
                Ok(false)
            }
        })
        .unwrap()
}

fn converge<H1, H2>(first: &MultiliteConnection<H1>, second: &MultiliteConnection<H2>)
where
    H1: ServerHandle + Send + Sync + 'static,
    H2: ServerHandle + Send + Sync + 'static,
{
    first.pull().unwrap();
    second.pull().unwrap();
    first.rebase().unwrap();
    second.rebase().unwrap();
}

fn bookings<H>(database: &MultiliteConnection<H>) -> Vec<(i64, String)>
where
    H: ServerHandle + Send + Sync + 'static,
{
    database
        .query("SELECT id, day FROM bookings ORDER BY id", (), |row| {
            Ok((row.get(0)?, row.get(1)?))
        })
        .unwrap()
}

fn rejected(outcome: PushOutcome) -> PushRejection {
    let PushOutcome::Rejected(rejection) = outcome else {
        panic!("submission unexpectedly admitted")
    };
    rejection
}

fn assert_range_assertion_failed(rejection: &PushRejection) {
    assert!(matches!(
        rejection.error(),
        KernelError::RangeAssertFailed { failures } if !failures.is_empty()
    ));
}
