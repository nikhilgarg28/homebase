use std::sync::Arc;

use homebase_core::space::SpaceId;
use multilite::{Connection, OpenOptions, PushOutcome, SyncPolicy, Value};

mod common;

use common::{router, server};

fn require_send<T: Send>(value: T) -> T {
    value
}

#[test]
fn public_async_api_runs_local_work_off_the_caller_thread() {
    pollster::block_on(async {
        let directory = tempfile::tempdir().unwrap();
        let caller = std::thread::current().id();
        let database = require_send(Connection::open_async(
            directory.path().join("async-local.sqlite"),
        ))
        .await
        .unwrap();

        let update_worker = require_send(database.update_async(move |transaction| {
            let worker = std::thread::current().id();
            transaction.execute(
                "CREATE TABLE notes (
                        id INTEGER PRIMARY KEY,
                        body TEXT NOT NULL
                    )",
                (),
            )?;
            transaction.execute(
                "INSERT INTO notes VALUES (?1, ?2), (?3, ?4)",
                (
                    Value::Integer(1),
                    Value::Text("one".into()),
                    Value::Integer(2),
                    Value::Text("two".into()),
                ),
            )?;
            Ok(worker)
        }))
        .await
        .unwrap();
        assert_ne!(update_worker, caller);

        assert_eq!(
            require_send(database.query_async(
                "SELECT id, body FROM notes ORDER BY id",
                (),
                |row| { Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?)) },
            ))
            .await
            .unwrap(),
            [(1, "one".into()), (2, "two".into())]
        );

        let view_worker = database
            .view_async(move |view| {
                assert_eq!(
                    view.query("SELECT count(*) FROM notes", (), |row| {
                        row.get::<_, i64>(0)
                    })?,
                    [2]
                );
                Ok(std::thread::current().id())
            })
            .await
            .unwrap();
        assert_ne!(view_worker, caller);

        database
            .execute_async(
                "INSERT INTO notes VALUES (?1, ?2)",
                (Value::Integer(3), Value::Text("three".into())),
            )
            .await
            .unwrap();
        let insert = database
            .prepare_async("INSERT INTO notes VALUES (?1, ?2)")
            .await
            .unwrap();
        assert!(!insert.readonly());
        assert_eq!(
            insert
                .execute_async((Value::Integer(4), Value::Text("four".into())))
                .await
                .unwrap(),
            1
        );
        assert_eq!(
            database
                .execute_async(
                    "UPDATE notes SET body = upper(body) WHERE id = ?1",
                    [Value::Integer(1)],
                )
                .await
                .unwrap(),
            1
        );
        assert_eq!(
            database
                .execute_async("DELETE FROM notes WHERE id = ?1", [Value::Integer(2)])
                .await
                .unwrap(),
            1
        );
        let statement = database
            .prepare_async("SELECT body FROM notes WHERE id >= ?1 ORDER BY id")
            .await
            .unwrap();
        assert_eq!(
            statement
                .query_map_async([Value::Integer(2)], |row| row.get::<_, String>(0))
                .await
                .unwrap(),
            ["three", "four"]
        );
        assert_eq!(
            database
                .query_async("SELECT body FROM notes WHERE id = 1", (), |row| {
                    row.get::<_, String>(0)
                })
                .await
                .unwrap(),
            ["ONE"]
        );
    });
}

#[test]
fn public_async_authority_pipeline_converges_replicas() {
    pollster::block_on(async {
        let directory = tempfile::tempdir().unwrap();
        let server = server();
        let first = Connection::open_with_async(
            directory.path().join("async-first.sqlite"),
            OpenOptions::new().server(router(Arc::clone(&server))),
        )
        .await
        .unwrap();
        assert!(server.create_space(SpaceId(first.database_id().to_bytes())));
        let second = Connection::open_with_async(
            directory.path().join("async-second.sqlite"),
            OpenOptions::new()
                .invitation(first.replica_invitation())
                .server(router(Arc::clone(&server))),
        )
        .await
        .unwrap();

        first
            .update_async(|transaction| {
                transaction.execute(
                    "CREATE TABLE notes (
                        id INTEGER PRIMARY KEY,
                        body TEXT NOT NULL
                    )",
                    (),
                )?;
                transaction.execute("INSERT INTO notes VALUES (1, 'first')", ())?;
                Ok(())
            })
            .await
            .unwrap();
        assert_eq!(first.push_async().await.unwrap(), PushOutcome::Drained);
        second.pull_async().await.unwrap();
        second.rebase_async().await.unwrap();

        second
            .execute_async(
                "INSERT INTO notes VALUES (?1, ?2)",
                (Value::Integer(2), Value::Text("second".into())),
            )
            .await
            .unwrap();
        assert_eq!(second.push_async().await.unwrap(), PushOutcome::Drained);
        first.pull_async().await.unwrap();
        first.rebase_async().await.unwrap();

        let expected = vec![(1, String::from("first")), (2, String::from("second"))];
        for database in [&first, &second] {
            assert_eq!(
                database
                    .query_async("SELECT id, body FROM notes ORDER BY id", (), |row| {
                        Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
                    })
                    .await
                    .unwrap(),
                expected
            );
        }
    });
}

#[test]
fn async_remote_policy_refreshes_reads_and_admits_exact_writes() {
    pollster::block_on(async {
        let directory = tempfile::tempdir().unwrap();
        let server = server();
        let first = Connection::open_with_async(
            directory.path().join("async-remote-first.sqlite"),
            OpenOptions::new()
                .sync_policy(SyncPolicy::Remote)
                .server(router(Arc::clone(&server))),
        )
        .await
        .unwrap();
        assert!(server.create_space(SpaceId(first.database_id().to_bytes())));

        first
            .update_async(|transaction| {
                transaction.execute(
                    "CREATE TABLE notes (
                        id INTEGER PRIMARY KEY,
                        body TEXT NOT NULL
                    )",
                    (),
                )?;
                transaction.execute("INSERT INTO notes VALUES (1, 'first')", ())?;
                Ok(())
            })
            .await
            .unwrap();

        let second = Connection::open_with_async(
            directory.path().join("async-remote-second.sqlite"),
            OpenOptions::new()
                .invitation(first.replica_invitation())
                .sync_policy(SyncPolicy::Remote)
                .server(router(Arc::clone(&server))),
        )
        .await
        .unwrap();
        let readers = 8;
        let start = Arc::new(std::sync::Barrier::new(readers));
        let observed = std::thread::scope(|scope| {
            let mut reads = Vec::new();
            for _ in 0..readers {
                let database = &second;
                let start = Arc::clone(&start);
                reads.push(scope.spawn(move || {
                    start.wait();
                    pollster::block_on(database.query_async(
                        "SELECT id, body FROM notes",
                        (),
                        |row| Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?)),
                    ))
                }));
            }
            reads
                .into_iter()
                .map(|read| read.join().unwrap().unwrap())
                .collect::<Vec<_>>()
        });
        assert!(
            observed
                .iter()
                .all(|rows| rows == &[(1, String::from("first"))])
        );

        second
            .execute_async(
                "INSERT INTO notes VALUES (?1, ?2)",
                (Value::Integer(2), Value::Text("second".into())),
            )
            .await
            .unwrap();
        assert_eq!(
            first
                .query_async("SELECT id, body FROM notes ORDER BY id", (), |row| {
                    Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
                })
                .await
                .unwrap(),
            [(1, "first".into()), (2, "second".into())]
        );
    });
}

#[test]
fn async_rollback_repairs_a_rejected_speculative_write() {
    pollster::block_on(async {
        let directory = tempfile::tempdir().unwrap();
        let server = server();
        let first = Connection::open_with_async(
            directory.path().join("async-winner.sqlite"),
            OpenOptions::new().server(router(Arc::clone(&server))),
        )
        .await
        .unwrap();
        assert!(server.create_space(SpaceId(first.database_id().to_bytes())));
        let second = Connection::open_with_async(
            directory.path().join("async-loser.sqlite"),
            OpenOptions::new()
                .invitation(first.replica_invitation())
                .server(router(Arc::clone(&server))),
        )
        .await
        .unwrap();

        for database in [&first, &second] {
            database
                .execute_async(
                    "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT NOT NULL)",
                    (),
                )
                .await
                .unwrap();
        }
        assert_eq!(first.push_async().await.unwrap(), PushOutcome::Drained);
        let PushOutcome::Rejected(rejection) = second.push_async().await.unwrap() else {
            panic!("same table name did not conflict");
        };

        second.rollback_async(&rejection).await.unwrap();
        assert_eq!(second.push_async().await.unwrap(), PushOutcome::Drained);
        second.pull_async().await.unwrap();
        second.rebase_async().await.unwrap();

        assert_eq!(
            second
                .query_async(
                    "SELECT name FROM sqlite_schema
                     WHERE type = 'table' AND name = 'notes'",
                    (),
                    |row| row.get::<_, String>(0),
                )
                .await
                .unwrap(),
            ["notes"]
        );
    });
}
