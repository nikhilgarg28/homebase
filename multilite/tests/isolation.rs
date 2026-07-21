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
