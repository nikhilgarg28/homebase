use std::time::Duration;

use homebase_client::server::UnreachableSpace;
use homebase_core::space::SpaceId;
use multilite::{Error, MultiliteConnection, OpenOptions, ReplicaInvitation, SyncPolicy};
use rusqlite::Connection;

#[test]
fn open_reopen_and_invitation_preserve_identity_rules() {
    let directory = tempfile::tempdir().unwrap();
    let primary_path = directory.path().join("primary.sqlite");
    let replica_path = directory.path().join("replica.sqlite");

    let primary = MultiliteConnection::open(&primary_path).unwrap();
    let database_id = primary.database_id();
    let invitation = primary.replica_invitation();
    let primary_device = primary.device_id();
    drop(primary);

    let reopened = MultiliteConnection::open(&primary_path).unwrap();
    assert_eq!(reopened.database_id(), database_id);
    assert_eq!(reopened.device_id(), primary_device);

    let replica =
        MultiliteConnection::open_with(&replica_path, OpenOptions::new().invitation(invitation))
            .unwrap();
    assert_eq!(replica.database_id(), database_id);
    assert_ne!(replica.device_id(), primary_device);
}

#[test]
fn open_initializes_missing_and_empty_files() {
    let directory = tempfile::tempdir().unwrap();
    let missing_path = directory.path().join("missing.sqlite");
    let missing = MultiliteConnection::open(&missing_path).unwrap();
    assert!(missing_path.exists());
    assert_ne!(missing.database_id().to_bytes(), [0; 16]);

    let empty_path = directory.path().join("empty.sqlite");
    Connection::open(&empty_path).unwrap();
    let empty = MultiliteConnection::open(&empty_path).unwrap();
    assert_ne!(empty.database_id().to_bytes(), [0; 16]);
    assert_ne!(empty.database_id(), missing.database_id());
}

#[test]
fn invitation_roundtrips_and_conflicting_identity_is_rejected() {
    let directory = tempfile::tempdir().unwrap();
    let first_path = directory.path().join("first.sqlite");
    let second_path = directory.path().join("second.sqlite");

    let first = MultiliteConnection::open(&first_path).unwrap();
    let encoded = first.replica_invitation().to_bytes();
    let invitation = ReplicaInvitation::from_bytes(&encoded).unwrap();
    assert_eq!(invitation.database_id(), first.database_id());

    let second = MultiliteConnection::open(&second_path).unwrap();
    let conflicting = second.replica_invitation();
    drop(first);
    drop(second);

    assert!(matches!(
        MultiliteConnection::open_with(&first_path, OpenOptions::new().invitation(conflicting),),
        Err(Error::DatabaseIdMismatch { .. })
    ));

    for malformed in [&[][..], &[2][..], &[1, 0][..], &[1; 18][..]] {
        assert!(matches!(
            ReplicaInvitation::from_bytes(malformed),
            Err(Error::InvalidReplicaInvitation)
        ));
    }
}

#[test]
fn open_rejects_lookalike_internal_schema() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("lookalike.sqlite");
    Connection::open(&path)
        .unwrap()
        .execute_batch(
            "CREATE TABLE items (
                collection TEXT NOT NULL,
                id BLOB NOT NULL,
                payload BLOB NOT NULL,
                PRIMARY KEY (collection, id)
            );
            CREATE TABLE __multilite__meta (
                key TEXT PRIMARY KEY NOT NULL,
                value BLOB NOT NULL
            );",
        )
        .unwrap();

    assert!(matches!(
        MultiliteConnection::open(&path),
        Err(Error::InvalidDatabase(_))
    ));
}

#[test]
fn general_open_adopts_a_preexisting_user_schema() {
    let directory = tempfile::tempdir().unwrap();
    let items_only = directory.path().join("items-only.sqlite");
    Connection::open(&items_only)
        .unwrap()
        .execute_batch(
            "CREATE TABLE items (
                collection TEXT NOT NULL,
                id BLOB NOT NULL,
                payload BLOB NOT NULL,
                PRIMARY KEY (collection, id)
            )",
        )
        .unwrap();

    let database = MultiliteConnection::open(&items_only).unwrap();
    let mut statement = database.prepare("SELECT count(*) FROM items").unwrap();
    assert_eq!(
        statement.query_map((), |row| row.get::<_, i64>(0)).unwrap(),
        [0]
    );
    drop(statement);
    drop(database);

    let stock = Connection::open(&items_only).unwrap();
    assert!(
        stock
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE name = '__multilite__meta')",
                (),
                |row| row.get::<_, bool>(0),
            )
            .unwrap()
    );
    assert!(
        !stock
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM sqlite_schema
                 WHERE name = '__multilite__v1_schema')",
                (),
                |row| row.get::<_, bool>(0),
            )
            .unwrap()
    );
}

#[test]
fn general_schema_reopens_without_changes_and_is_stock_sqlite_readable() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("database.sqlite");
    {
        let database = MultiliteConnection::open(&path).unwrap();
        database
            .execute(
                "CREATE TABLE items (
                    collection TEXT NOT NULL,
                    id BLOB PRIMARY KEY NOT NULL,
                    payload BLOB NOT NULL
                )",
                (),
            )
            .unwrap();
        database
            .execute(
                "INSERT INTO items (collection, id, payload) VALUES (?1, ?2, ?3)",
                ("notes", b"one".as_slice(), b"hello".as_slice()),
            )
            .unwrap();
    }

    let stock = Connection::open(&path).unwrap();
    let schema_version_before: i64 = stock
        .query_row("PRAGMA schema_version", (), |row| row.get(0))
        .unwrap();
    let row: (String, Vec<u8>, Vec<u8>) = stock
        .query_row("SELECT collection, id, payload FROM items", (), |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?))
        })
        .unwrap();
    assert_eq!(row, ("notes".into(), b"one".to_vec(), b"hello".to_vec()));
    drop(stock);

    drop(MultiliteConnection::open(&path).unwrap());
    let stock = Connection::open(&path).unwrap();
    let schema_version_after: i64 = stock
        .query_row("PRAGMA schema_version", (), |row| row.get(0))
        .unwrap();
    let integrity: String = stock
        .query_row("PRAGMA integrity_check", (), |row| row.get(0))
        .unwrap();
    assert_eq!(schema_version_after, schema_version_before);
    assert_eq!(integrity, "ok");
}

#[test]
fn lifecycle_accepts_an_explicit_server_handle() {
    let directory = tempfile::tempdir().unwrap();
    let server = |_: &SpaceId| None::<UnreachableSpace>;
    let database = MultiliteConnection::open_with(
        directory.path().join("database.sqlite"),
        OpenOptions::new().server(server),
    )
    .unwrap();

    assert_ne!(database.database_id().to_bytes(), [0; 16]);
}

#[test]
fn open_time_sync_policy_is_public_and_validated() {
    let directory = tempfile::tempdir().unwrap();
    let local = MultiliteConnection::open(directory.path().join("local.sqlite")).unwrap();
    assert_eq!(local.sync_policy(), SyncPolicy::LocalOnly);

    let local_first = MultiliteConnection::open_with(
        directory.path().join("local-first.sqlite"),
        OpenOptions::new()
            .sync_policy(SyncPolicy::LocalFirst {
                write_delay: Duration::from_secs(30),
                read_staleness: Duration::from_secs(10),
            })
            .server(|_: &SpaceId| None::<UnreachableSpace>),
    )
    .unwrap();
    assert_eq!(
        local_first.sync_policy(),
        SyncPolicy::LocalFirst {
            write_delay: Duration::from_secs(30),
            read_staleness: Duration::from_secs(10),
        }
    );

    assert!(matches!(
        MultiliteConnection::open_with(
            directory.path().join("local-first-offline.sqlite"),
            OpenOptions::new().sync_policy(SyncPolicy::LocalFirst {
                write_delay: Duration::ZERO,
                read_staleness: Duration::ZERO,
            }),
        ),
        Err(Error::AuthorityRequired("local-first policy"))
    ));
    assert!(matches!(
        MultiliteConnection::open_with(
            directory.path().join("remote.sqlite"),
            OpenOptions::new().sync_policy(SyncPolicy::Remote),
        ),
        Err(Error::AuthorityRequired("remote policy"))
    ));
}
