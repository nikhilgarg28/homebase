//! Database orchestration tests.

use std::collections::BTreeSet;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

use homebase::Server;
use homebase::actor::{SpaceHandle, Spawner};
use homebase::storage::MemoryStore;
use homebase_client::meta::{AdmitCursors, ClientState, DeviceOp, OrderedMetaStore, SubmitMode};
use homebase_client::server::offline_router;
use homebase_core::clock::{ManualClock, Timestamp};
use homebase_core::key::{Key, MAX_COMPONENT_LEN};
use homebase_core::messages::{
    AcquireRequest, AcquireResponse, AdmissionRequest, AdmissionResponse, GetRequest, GetResponse,
    ListLeasesRequest, ListLeasesResponse, ListRequest, ListResponse, PullRequest, PullResponse,
    ReadAtRequest, ReadAtResponse, ReleaseRequest, ReleaseResponse, RenewRequest, RenewResponse,
};
use homebase_core::space::{Space, SpaceError};
use homebase_core::tag::{DeviceChecksum, DeviceSeq, Mutation};
use rusqlite::OptionalExtension;
use rusqlite::hooks::{AuthAction, AuthContext, Authorization};

use super::operation::MultiliteOp;
use super::transaction::MultiliteTransaction;
use super::*;
use crate::commit::history;

struct ThreadSpawner;

impl Spawner for ThreadSpawner {
    fn spawn(&self, task: Pin<Box<dyn Future<Output = ()> + Send + 'static>>) {
        std::thread::spawn(move || pollster::block_on(task));
    }
}

type TestServer = Server<MemoryStore, ManualClock, ThreadSpawner>;

fn server() -> Arc<TestServer> {
    Arc::new(Server::new(
        Arc::new(MemoryStore::new()),
        Arc::new(ManualClock::new(Timestamp(0))),
        ThreadSpawner,
    ))
}

fn router(server: Arc<TestServer>) -> impl Fn(&SpaceId) -> Option<SpaceHandle> + Sync {
    move |space| server.space(space)
}

struct GatedAdmitSpace {
    inner: SpaceHandle,
    gate_once: Arc<AtomicBool>,
    entered: Arc<Barrier>,
    release: Arc<Barrier>,
}

impl Space for GatedAdmitSpace {
    async fn acquire(
        &self,
        request: AcquireRequest,
    ) -> std::result::Result<AcquireResponse, SpaceError> {
        self.inner.acquire(request).await
    }

    async fn renew(&self, request: RenewRequest) -> std::result::Result<RenewResponse, SpaceError> {
        self.inner.renew(request).await
    }

    async fn release(
        &self,
        request: ReleaseRequest,
    ) -> std::result::Result<ReleaseResponse, SpaceError> {
        self.inner.release(request).await
    }

    async fn list_leases(
        &self,
        request: ListLeasesRequest,
    ) -> std::result::Result<ListLeasesResponse, SpaceError> {
        self.inner.list_leases(request).await
    }

    async fn admit(
        &self,
        request: AdmissionRequest,
    ) -> std::result::Result<AdmissionResponse, SpaceError> {
        if self.gate_once.swap(false, Ordering::SeqCst) {
            self.entered.wait();
            self.release.wait();
        }
        self.inner.admit(request).await
    }

    async fn pull(&self, request: PullRequest) -> std::result::Result<PullResponse, SpaceError> {
        self.inner.pull(request).await
    }

    async fn get(&self, request: GetRequest) -> std::result::Result<GetResponse, SpaceError> {
        self.inner.get(request).await
    }

    async fn list(&self, request: ListRequest) -> std::result::Result<ListResponse, SpaceError> {
        self.inner.list(request).await
    }

    async fn read_at(
        &self,
        request: ReadAtRequest,
    ) -> std::result::Result<ReadAtResponse, SpaceError> {
        self.inner.read_at(request).await
    }
}

fn gated_router(
    server: Arc<TestServer>,
    entered: Arc<Barrier>,
    release: Arc<Barrier>,
) -> impl Fn(&SpaceId) -> Option<GatedAdmitSpace> + Sync {
    let gate_once = Arc::new(AtomicBool::new(true));
    move |space| {
        server.space(space).map(|inner| GatedAdmitSpace {
            inner,
            gate_once: Arc::clone(&gate_once),
            entered: Arc::clone(&entered),
            release: Arc::clone(&release),
        })
    }
}

fn client_state<H: ServerHandle + Send + Sync + 'static>(database: &Database<H>) -> ClientState {
    let store = DatabaseMetaStore::new(database.owner.clone());
    block_on(store.load()).unwrap()
}

fn table_exists<H: ServerHandle + Send + Sync + 'static>(
    database: &Database<H>,
    table: &str,
) -> bool {
    database.with_connection(|connection| {
        connection
            .query_row(
                "SELECT EXISTS(
                         SELECT 1 FROM sqlite_schema
                         WHERE type = 'table' AND name = ?1 COLLATE NOCASE
                     )",
                [table],
                |row| row.get(0),
            )
            .unwrap()
    })
}

fn pending_ops<H: ServerHandle + Send + Sync + 'static>(
    database: &Database<H>,
) -> Vec<pending::PendingTransaction> {
    database.with_connection(pending::load).unwrap()
}

fn backend_for<H: ServerHandle + Send + Sync + 'static>(
    database: &Database<H>,
    path: &Path,
) -> DatabaseCommitBackend<H> {
    DatabaseCommitBackend {
        owner: database.owner.clone(),
        path: path.to_owned(),
        wal_path: wal_path_for(path),
        database_id: database.database_id,
        client: Arc::clone(&database.client),
        commit_history: CommitHistory::default(),
        snapshot_cache: parking_lot::Mutex::new(crate::branch::snapshot::SnapshotCache::new()),
        checkpoint: parking_lot::Mutex::new(crate::commit::checkpoint::CheckpointPolicy::default()),
    }
}

fn table_sql<H: ServerHandle + Send + Sync + 'static>(
    database: &Database<H>,
    table: &str,
) -> Option<String> {
    database.with_connection(|connection| {
        connection
            .query_row(
                "SELECT sql FROM sqlite_schema
                     WHERE type = 'table' AND name = ?1 COLLATE NOCASE",
                [table],
                |row| row.get(0),
            )
            .optional()
            .unwrap()
    })
}

fn row_ids<H: ServerHandle + Send + Sync + 'static>(database: &Database<H>) -> Vec<i64> {
    database.with_connection(|connection| {
        let mut statement = connection
            .prepare("SELECT id FROM notes ORDER BY id")
            .unwrap();
        statement
            .query_map((), |row| row.get(0))
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap()
    })
}

fn stock_user_schema(path: &Path) -> Vec<(String, String)> {
    let connection = SqliteConnection::open(path).unwrap();
    let mut statement = connection
        .prepare(
            "SELECT name, sql FROM sqlite_schema
                 WHERE type = 'table' ORDER BY name COLLATE NOCASE",
        )
        .unwrap();
    statement
        .query_map((), |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap()
        .into_iter()
        .filter(|(name, _)| {
            let name = name.to_ascii_lowercase();
            !name.starts_with("__multilite__") && !name.starts_with("sqlite_")
        })
        .collect()
}

fn create_operation(name: &str) -> MultiliteOp {
    MultiliteOp::create_table(
        &format!("CREATE TABLE {name} (id INTEGER PRIMARY KEY)"),
        schema::CreateTableSpec {
            name: schema::SqlName::new(name.into()),
            mode: Default::default(),
            storage: crate::database::schema::TableStorage::Rowid,
            columns: vec![schema::CreateColumn {
                name: schema::SqlName::new("id".into()),
                declared_type: schema::TypeDeclaration::integer(),
                not_null: false,
                primary_key: Some(0),
            }],
            unique_constraints: Vec::new(),
            foreign_keys: Vec::new(),
        },
    )
}

fn create_proposal(snapshot: SnapshotDescriptor, name: &str) -> CommitProposal {
    let transaction = MultiliteTransaction::new(vec![create_operation(name)]).unwrap();
    let (_, footprint) = transaction.to_homebase().unwrap().into_parts();
    CommitProposal::from_transaction(snapshot, IsolationLevel::Snapshot, transaction, footprint)
        .unwrap()
}

fn pending_apply_proposal<H: ServerHandle + Send + Sync + 'static>(
    database: &Database<H>,
) -> CommitProposal {
    let space_id = database.database_id.space_id();
    let store = DatabaseMetaStore::new(database.owner.clone());
    let (submit, admits, batches) = block_on(async {
        let submit = store.oplog_cursors(space_id).await?;
        let admits = store.admit_cursors(space_id).await?;
        let space = database.client.space(space_id).await?;
        let batches = space
            .admits()
            .iter(admits.neck..admits.tail)
            .await
            .map_err(ClientError::from)?;
        Ok::<_, Error>((submit, admits, batches))
    })
    .unwrap();
    let transactions = batches
        .into_iter()
        .filter(|batch| !batch.entries.is_empty())
        .map(|batch| {
            Ok(crate::commit::proposal::AdmittedTransaction {
                device: batch.device,
                transaction: MultiliteTransaction::from_homebase(&batch)?,
            })
        })
        .collect::<Result<Vec<_>>>()
        .unwrap();
    CommitProposal::apply_admissions(
        submit,
        admits,
        admits.tail,
        database.client.device(),
        transactions,
    )
    .unwrap()
}

fn submit_direct<H: ServerHandle + Send + Sync + 'static>(
    database: &Database<H>,
    operation: &MultiliteOp,
) {
    let (mutations, assertions) = MultiliteTransaction::new(vec![operation.clone()])
        .unwrap()
        .to_homebase()
        .unwrap()
        .plan(IsolationLevel::Serializable, AdmissionSeq(0));
    block_on(async {
        database
            .client
            .space(database.database_id().space_id())
            .await
            .unwrap()
            .submit_unchecked(mutations, assertions)
            .await
            .unwrap();
    });
}

fn wait_until(mut predicate: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(2);
    while !predicate() {
        assert!(Instant::now() < deadline, "condition did not become true");
        std::thread::sleep(Duration::from_millis(10));
    }
}

#[test]
fn policy_defaults_are_local_and_authority_requirements_fail_before_open() {
    let directory = tempfile::tempdir().unwrap();
    let local_path = directory.path().join("local.sqlite");
    let local = Database::open(&local_path).unwrap();
    assert_eq!(local.sync_policy(), SyncPolicy::LocalOnly);

    let remote_path = directory.path().join("remote.sqlite");
    assert!(matches!(
        Database::open_with(
            &remote_path,
            OpenOptions::new().sync_policy(SyncPolicy::Remote),
        ),
        Err(Error::AuthorityRequired("remote policy"))
    ));
    assert!(!remote_path.exists());

    let local_first_path = directory.path().join("local-first.sqlite");
    assert!(matches!(
        Database::open_with(
            &local_first_path,
            OpenOptions::new().sync_policy(SyncPolicy::LocalFirst {
                write_delay: Duration::ZERO,
                read_staleness: Duration::from_secs(1),
            }),
        ),
        Err(Error::AuthorityRequired("local-first policy"))
    ));
    assert!(!local_first_path.exists());
}

#[test]
fn database_workers_release_the_client_after_the_last_handle_drops() {
    let directory = tempfile::tempdir().unwrap();
    let database = Database::open(directory.path().join("worker-lifetime.sqlite")).unwrap();
    let client = Arc::downgrade(&database.client);

    drop(database);

    wait_until(|| client.upgrade().is_none());
}

#[test]
fn local_proposal_does_not_wait_for_an_inflight_authority_permit() {
    let directory = tempfile::tempdir().unwrap();
    let server = server();
    let entered = Arc::new(Barrier::new(2));
    let release = Arc::new(Barrier::new(2));
    let database = Database::open_with(
        directory.path().join("authority-permit.sqlite"),
        OpenOptions::new().server(gated_router(
            Arc::clone(&server),
            Arc::clone(&entered),
            Arc::clone(&release),
        )),
    )
    .unwrap();
    assert!(server.create_space(database.database_id().space_id()));
    let runtime = Arc::new(database.runtime().unwrap());
    database
        .execute(&runtime, "CREATE TABLE first (id INTEGER PRIMARY KEY)", ())
        .unwrap();

    let (push_reply, push_result) = std::sync::mpsc::sync_channel(1);
    let pushing = Arc::clone(&database);
    std::thread::spawn(move || {
        let _ = push_reply.send(pushing.push());
    });
    entered.wait();

    let (write_reply, write_result) = std::sync::mpsc::sync_channel(1);
    let writing = Arc::clone(&database);
    let write_runtime = Arc::clone(&runtime);
    std::thread::spawn(move || {
        let result = writing.execute(
            &write_runtime,
            "CREATE TABLE second (id INTEGER PRIMARY KEY)",
            (),
        );
        let _ = write_reply.send(result);
    });

    let write_result = write_result.recv_timeout(Duration::from_secs(2));
    release.wait();
    let push_result = push_result.recv_timeout(Duration::from_secs(2));

    assert_eq!(write_result.unwrap().unwrap(), 0);
    assert_eq!(push_result.unwrap().unwrap(), PushOutcome::Drained);
    assert!(table_exists(&database, "first"));
    assert!(table_exists(&database, "second"));
}

#[test]
fn local_first_zero_schedules_push_without_waiting_in_execute() {
    let directory = tempfile::tempdir().unwrap();
    let server = server();
    let database = Database::open_with(
        directory.path().join("local-first.sqlite"),
        OpenOptions::new()
            .sync_policy(SyncPolicy::LocalFirst {
                write_delay: Duration::ZERO,
                read_staleness: Duration::from_secs(60),
            })
            .server(router(Arc::clone(&server))),
    )
    .unwrap();
    assert!(server.create_space(database.database_id().space_id()));
    let runtime = database.runtime().unwrap();
    database.start_background_push().unwrap();

    database
        .execute(&runtime, "CREATE TABLE notes (id INTEGER PRIMARY KEY)", ())
        .unwrap();
    wait_until(|| pending_ops(&database).is_empty());

    let state = client_state(&database);
    let cursors = state.spaces[&database.database_id().space_id()].cursors;
    assert_eq!(cursors.neck, cursors.tail);
    assert!(table_exists(&database, "notes"));
}

#[test]
fn local_first_delete_pushes_in_the_background_and_rebases_on_a_replica() {
    let directory = tempfile::tempdir().unwrap();
    let server = server();
    let database = Database::open_with(
        directory.path().join("local-first-delete.sqlite"),
        OpenOptions::new()
            .sync_policy(SyncPolicy::LocalFirst {
                write_delay: Duration::ZERO,
                read_staleness: Duration::from_secs(60),
            })
            .server(router(Arc::clone(&server))),
    )
    .unwrap();
    assert!(server.create_space(database.database_id().space_id()));
    let runtime = database.runtime().unwrap();
    database.start_background_push().unwrap();
    database
        .update(&runtime, |update| {
            update.execute(
                "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT NOT NULL)",
                (),
            )?;
            update.execute("INSERT INTO notes VALUES (1, 'temporary')", ())?;
            Ok(())
        })
        .unwrap();
    wait_until(|| pending_ops(&database).is_empty());

    assert_eq!(
        database
            .execute(&runtime, "DELETE FROM notes WHERE id = 1", ())
            .unwrap(),
        1
    );
    assert!(row_ids(&database).is_empty());
    wait_until(|| pending_ops(&database).is_empty());

    let replica = Database::open_with(
        directory.path().join("local-first-delete-replica.sqlite"),
        OpenOptions::new()
            .invitation(database.replica_invitation())
            .server(router(Arc::clone(&server))),
    )
    .unwrap();
    let replica_runtime = replica.runtime().unwrap();
    replica.pull().unwrap();
    replica.rebase(&replica_runtime).unwrap();
    assert!(row_ids(&replica).is_empty());
}

#[test]
fn remote_write_returns_only_after_admission_and_pending_cleanup() {
    let directory = tempfile::tempdir().unwrap();
    let server = server();
    let database = Database::open_with(
        directory.path().join("remote.sqlite"),
        OpenOptions::new()
            .sync_policy(SyncPolicy::Remote)
            .server(router(Arc::clone(&server))),
    )
    .unwrap();
    assert!(server.create_space(database.database_id().space_id()));
    let runtime = database.runtime().unwrap();

    database
        .execute(&runtime, "CREATE TABLE notes (id INTEGER PRIMARY KEY)", ())
        .unwrap();

    assert!(pending_ops(&database).is_empty());
    let state = client_state(&database);
    let cursors = state.spaces[&database.database_id().space_id()].cursors;
    assert_eq!(cursors.neck, cursors.tail);
    assert!(table_exists(&database, "notes"));
}

#[test]
fn remote_rejection_undoes_sqlite_before_returning_the_error() {
    let directory = tempfile::tempdir().unwrap();
    let server = server();
    let first = Database::open_with(
        directory.path().join("winner.sqlite"),
        OpenOptions::new().server(router(Arc::clone(&server))),
    )
    .unwrap();
    assert!(server.create_space(first.database_id().space_id()));
    let first_runtime = first.runtime().unwrap();
    let second = Database::open_with(
        directory.path().join("loser.sqlite"),
        OpenOptions::new()
            .invitation(first.replica_invitation())
            .sync_policy(SyncPolicy::Remote)
            .server(router(Arc::clone(&server))),
    )
    .unwrap();
    let second_runtime = second.runtime().unwrap();
    let error = second
        .update_with(
            &second_runtime,
            UpdateOptions::new(IsolationLevel::Serializable),
            |update| {
                update.execute(
                    "CREATE TABLE NOTES (id INTEGER PRIMARY KEY, payload BLOB)",
                    (),
                )?;
                first.execute(
                    &first_runtime,
                    "CREATE TABLE notes (id INTEGER PRIMARY KEY)",
                    (),
                )?;
                assert_eq!(first.push()?, PushOutcome::Drained);
                Ok(())
            },
        )
        .unwrap_err();

    assert!(matches!(
        error,
        Error::AuthorityRejected(KernelError::RangeAssertFailed { .. })
    ));
    assert!(!table_exists(&second, "notes"));
    assert!(pending_ops(&second).is_empty());
    let state = client_state(&second);
    let cursors = state.spaces[&second.database_id().space_id()].cursors;
    assert_eq!(cursors.neck, cursors.tail);
}

#[test]
fn remote_delete_rejection_restores_the_complete_row_before_returning() {
    let directory = tempfile::tempdir().unwrap();
    let server = server();
    let first = Database::open_with(
        directory.path().join("delete-winner.sqlite"),
        OpenOptions::new().server(router(Arc::clone(&server))),
    )
    .unwrap();
    assert!(server.create_space(first.database_id().space_id()));
    let first_runtime = first.runtime().unwrap();
    first
        .update(&first_runtime, |update| {
            update.execute(
                "CREATE TABLE notes (
                    id INTEGER PRIMARY KEY,
                    body TEXT NOT NULL,
                    payload BLOB
                )",
                (),
            )?;
            update.execute("INSERT INTO notes VALUES (1, 'original', x'0102')", ())?;
            Ok(())
        })
        .unwrap();
    assert_eq!(first.push().unwrap(), PushOutcome::Drained);

    let second = Database::open_with(
        directory.path().join("delete-loser.sqlite"),
        OpenOptions::new()
            .invitation(first.replica_invitation())
            .sync_policy(SyncPolicy::Remote)
            .server(router(Arc::clone(&server))),
    )
    .unwrap();
    let second_runtime = second.runtime().unwrap();
    let error = second
        .update(&second_runtime, |update| {
            assert_eq!(update.execute("DELETE FROM notes WHERE id = 1", ())?, 1);
            assert_eq!(
                update.execute("INSERT INTO notes VALUES (1, 'replacement', x'99')", ())?,
                1
            );
            assert_eq!(
                update.execute("INSERT INTO notes VALUES (2, 'temporary', NULL)", ())?,
                1
            );
            assert_eq!(update.execute("DELETE FROM notes WHERE id = 2", ())?, 1);
            assert_eq!(
                first.execute(&first_runtime, "DELETE FROM notes WHERE id = 1", ())?,
                1
            );
            assert_eq!(first.push()?, PushOutcome::Drained);
            Ok(())
        })
        .unwrap_err();

    assert!(matches!(
        error,
        Error::AuthorityRejected(KernelError::RangeAssertFailed { .. })
    ));
    assert!(pending_ops(&second).is_empty());
    second.with_connection(|connection| {
        assert_eq!(
            connection
                .query_row("SELECT id, body, payload FROM notes", (), |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, Vec<u8>>(2)?,
                    ))
                },)
                .unwrap(),
            (1, "original".into(), vec![1, 2])
        );
    });
}

#[test]
fn remote_update_rejection_restores_the_before_image_before_returning() {
    let directory = tempfile::tempdir().unwrap();
    let server = server();
    let first = Database::open_with(
        directory.path().join("update-winner.sqlite"),
        OpenOptions::new().server(router(Arc::clone(&server))),
    )
    .unwrap();
    assert!(server.create_space(first.database_id().space_id()));
    let first_runtime = first.runtime().unwrap();
    first
        .update(&first_runtime, |update| {
            update.execute(
                "CREATE TABLE notes (
                    id INTEGER PRIMARY KEY,
                    body TEXT NOT NULL,
                    payload BLOB
                )",
                (),
            )?;
            update.execute("INSERT INTO notes VALUES (1, 'original', x'0102')", ())?;
            Ok(())
        })
        .unwrap();
    assert_eq!(first.push().unwrap(), PushOutcome::Drained);

    let second = Database::open_with(
        directory.path().join("update-loser.sqlite"),
        OpenOptions::new()
            .invitation(first.replica_invitation())
            .sync_policy(SyncPolicy::Remote)
            .server(router(Arc::clone(&server))),
    )
    .unwrap();
    let second_runtime = second.runtime().unwrap();
    let error = second
        .update(&second_runtime, |update| {
            assert_eq!(
                update.execute(
                    "UPDATE notes SET body = 'loser', payload = x'99' WHERE id = 1",
                    (),
                )?,
                1
            );
            assert_eq!(
                first.execute(
                    &first_runtime,
                    "UPDATE notes SET body = 'winner' WHERE id = 1",
                    (),
                )?,
                1
            );
            assert_eq!(first.push()?, PushOutcome::Drained);
            Ok(())
        })
        .unwrap_err();

    assert!(matches!(
        error,
        Error::AuthorityRejected(KernelError::RangeAssertFailed { .. })
    ));
    assert!(pending_ops(&second).is_empty());
    second.with_connection(|connection| {
        assert_eq!(
            connection
                .query_row("SELECT id, body, payload FROM notes", (), |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, Vec<u8>>(2)?,
                    ))
                })
                .unwrap(),
            (1, "original".into(), vec![1, 2])
        );
    });
}

#[test]
fn remote_writes_wait_for_their_own_submission_disposition() {
    let directory = tempfile::tempdir().unwrap();
    let server = server();
    let winner = Database::open_with(
        directory.path().join("exact-winner.sqlite"),
        OpenOptions::new().server(router(Arc::clone(&server))),
    )
    .unwrap();
    assert!(server.create_space(winner.database_id().space_id()));
    let winner_runtime = winner.runtime().unwrap();

    let entered = Arc::new(Barrier::new(2));
    let release = Arc::new(Barrier::new(2));
    let remote = Database::open_with(
        directory.path().join("exact-remote.sqlite"),
        OpenOptions::new()
            .invitation(winner.replica_invitation())
            .sync_policy(SyncPolicy::Remote)
            .server(gated_router(
                Arc::clone(&server),
                Arc::clone(&entered),
                Arc::clone(&release),
            )),
    )
    .unwrap();
    let runtime = Arc::new(remote.runtime().unwrap());

    let (first_ready, first_started) = std::sync::mpsc::sync_channel(1);
    let (first_release, first_continue) = std::sync::mpsc::sync_channel(1);
    let (first_reply, first_result) = std::sync::mpsc::sync_channel(1);
    let first = Arc::clone(&remote);
    let first_runtime = Arc::clone(&runtime);
    std::thread::spawn(move || {
        let result = first.update(&first_runtime, |update| {
            first_ready.send(()).unwrap();
            first_continue.recv().unwrap();
            update.execute("CREATE TABLE admitted (id INTEGER PRIMARY KEY)", ())?;
            Ok(())
        });
        let _ = first_reply.send(result);
    });

    let (second_ready, second_started) = std::sync::mpsc::sync_channel(1);
    let (second_release, second_continue) = std::sync::mpsc::sync_channel(1);
    let (second_reply, second_result) = std::sync::mpsc::sync_channel(1);
    let second = Arc::clone(&remote);
    let second_runtime = Arc::clone(&runtime);
    std::thread::spawn(move || {
        let result = second.update(&second_runtime, |update| {
            second_ready.send(()).unwrap();
            second_continue.recv().unwrap();
            update.execute(
                "CREATE TABLE CONFLICT (id INTEGER PRIMARY KEY, payload BLOB)",
                (),
            )?;
            Ok(())
        });
        let _ = second_reply.send(result);
    });

    first_started.recv().unwrap();
    second_started.recv().unwrap();
    winner
        .execute(
            &winner_runtime,
            "CREATE TABLE conflict (id INTEGER PRIMARY KEY)",
            (),
        )
        .unwrap();
    assert_eq!(winner.push().unwrap(), PushOutcome::Drained);

    first_release.send(()).unwrap();
    entered.wait();
    second_release.send(()).unwrap();
    wait_until(|| pending_ops(&remote).len() == 2);
    release.wait();

    assert_eq!(
        first_result
            .recv_timeout(Duration::from_secs(2))
            .unwrap()
            .unwrap(),
        ()
    );
    assert!(matches!(
        second_result.recv_timeout(Duration::from_secs(2)).unwrap(),
        Err(Error::AuthorityRejected(
            KernelError::RangeAssertFailed { .. }
        ))
    ));
    assert!(table_exists(&remote, "admitted"));
    assert!(!table_exists(&remote, "conflict"));
    assert!(pending_ops(&remote).is_empty());
}

#[test]
fn remote_write_first_drains_history_buffered_under_local_only() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("policy-change.sqlite");
    let server = server();
    let database = Database::open_with(
        &path,
        OpenOptions::new().server(router(Arc::clone(&server))),
    )
    .unwrap();
    assert!(server.create_space(database.database_id().space_id()));
    let runtime = database.runtime().unwrap();
    database
        .execute(
            &runtime,
            "CREATE TABLE buffered (id INTEGER PRIMARY KEY)",
            (),
        )
        .unwrap();
    assert_eq!(pending_ops(&database).len(), 1);
    drop(runtime);
    drop(database);

    let database = Database::open_with(
        &path,
        OpenOptions::new()
            .sync_policy(SyncPolicy::Remote)
            .server(router(Arc::clone(&server))),
    )
    .unwrap();
    let runtime = database.runtime().unwrap();
    database
        .execute(
            &runtime,
            "CREATE TABLE admitted (id INTEGER PRIMARY KEY)",
            (),
        )
        .unwrap();

    assert!(pending_ops(&database).is_empty());
    let state = client_state(&database);
    let cursors = state.spaces[&database.database_id().space_id()].cursors;
    assert_eq!(cursors.neck, cursors.tail);
}

#[test]
fn remote_read_pulls_and_rebases_before_running_a_prepared_query() {
    let directory = tempfile::tempdir().unwrap();
    let server = server();
    let source = Database::open_with(
        directory.path().join("read-source.sqlite"),
        OpenOptions::new()
            .sync_policy(SyncPolicy::Remote)
            .server(router(Arc::clone(&server))),
    )
    .unwrap();
    assert!(server.create_space(source.database_id().space_id()));
    let source_runtime = source.runtime().unwrap();
    source
        .execute(
            &source_runtime,
            "CREATE TABLE notes (id INTEGER PRIMARY KEY)",
            (),
        )
        .unwrap();

    let replica = Database::open_with(
        directory.path().join("read-replica.sqlite"),
        OpenOptions::new()
            .invitation(source.replica_invitation())
            .sync_policy(SyncPolicy::Remote)
            .server(router(Arc::clone(&server))),
    )
    .unwrap();
    let runtime = Arc::new(replica.runtime().unwrap());
    let mut statement = replica
        .prepare(
            &runtime,
            "SELECT name FROM sqlite_schema WHERE type = 'table' AND name = 'notes'",
        )
        .unwrap();

    assert_eq!(
        statement
            .query_map((), |row| row.get::<_, String>(0))
            .unwrap(),
        ["notes"]
    );
    assert!(table_exists(&replica, "notes"));
}

#[test]
fn managed_view_and_update_refresh_once_then_keep_one_snapshot() {
    let directory = tempfile::tempdir().unwrap();
    let server = server();
    let source = Database::open_with(
        directory.path().join("snapshot-source.sqlite"),
        OpenOptions::new()
            .sync_policy(SyncPolicy::Remote)
            .server(router(Arc::clone(&server))),
    )
    .unwrap();
    assert!(server.create_space(source.database_id().space_id()));
    let source_runtime = source.runtime().unwrap();
    source
        .execute(
            &source_runtime,
            "CREATE TABLE notes (id INTEGER PRIMARY KEY)",
            (),
        )
        .unwrap();
    source
        .execute(&source_runtime, "INSERT INTO notes VALUES (1)", ())
        .unwrap();

    let replica = Database::open_with(
        directory.path().join("snapshot-replica.sqlite"),
        OpenOptions::new()
            .invitation(source.replica_invitation())
            .sync_policy(SyncPolicy::Remote)
            .server(router(Arc::clone(&server))),
    )
    .unwrap();
    let replica_runtime = replica.runtime().unwrap();
    let count = |rows: Vec<i64>| rows.into_iter().next().unwrap();

    let observed = replica
        .view(&replica_runtime, |view| {
            let before =
                count(view.query("SELECT count(*) FROM notes", (), |row| row.get::<_, i64>(0))?);
            source.execute(&source_runtime, "INSERT INTO notes VALUES (2)", ())?;
            let after =
                count(view.query("SELECT count(*) FROM notes", (), |row| row.get::<_, i64>(0))?);
            Ok((before, after))
        })
        .unwrap();
    assert_eq!(observed, (1, 1));
    assert_eq!(
        replica
            .view(&replica_runtime, |view| {
                view.query("SELECT count(*) FROM notes", (), |row| row.get::<_, i64>(0))
            })
            .unwrap(),
        [2]
    );

    let observed = replica
        .update_with(
            &replica_runtime,
            UpdateOptions::new(IsolationLevel::Snapshot),
            |update| {
                let before = count(
                    update.query("SELECT count(*) FROM notes", (), |row| row.get::<_, i64>(0))?,
                );
                source.execute(&source_runtime, "INSERT INTO notes VALUES (3)", ())?;
                let after_remote = count(update.query(
                    "SELECT count(*) FROM notes",
                    (),
                    |row| row.get::<_, i64>(0),
                )?);
                update.execute("INSERT INTO notes VALUES (4)", ())?;
                let after_local = count(
                    update.query("SELECT count(*) FROM notes", (), |row| row.get::<_, i64>(0))?,
                );
                Ok((before, after_remote, after_local))
            },
        )
        .unwrap();
    assert_eq!(observed, (2, 2, 3));
    assert_eq!(
        replica
            .view(&replica_runtime, |view| {
                view.query("SELECT id FROM notes ORDER BY id", (), |row| {
                    row.get::<_, i64>(0)
                })
            })
            .unwrap(),
        [1, 2, 3, 4]
    );
}

#[test]
fn local_first_read_honors_its_staleness_window() {
    let directory = tempfile::tempdir().unwrap();
    let server = server();
    let source = Database::open_with(
        directory.path().join("stale-source.sqlite"),
        OpenOptions::new()
            .sync_policy(SyncPolicy::Remote)
            .server(router(Arc::clone(&server))),
    )
    .unwrap();
    assert!(server.create_space(source.database_id().space_id()));
    let source_runtime = source.runtime().unwrap();
    source
        .execute(
            &source_runtime,
            "CREATE TABLE first_table (id INTEGER PRIMARY KEY)",
            (),
        )
        .unwrap();

    let replica = Database::open_with(
        directory.path().join("stale-replica.sqlite"),
        OpenOptions::new()
            .invitation(source.replica_invitation())
            .sync_policy(SyncPolicy::LocalFirst {
                write_delay: Duration::from_secs(60),
                read_staleness: Duration::from_millis(100),
            })
            .server(router(Arc::clone(&server))),
    )
    .unwrap();
    let replica_runtime = Arc::new(replica.runtime().unwrap());
    let mut tables = replica
        .prepare(
            &replica_runtime,
            "SELECT name FROM sqlite_schema
                 WHERE type = 'table' AND name LIKE '%_table' ORDER BY name",
        )
        .unwrap();
    assert_eq!(
        tables.query_map((), |row| row.get::<_, String>(0)).unwrap(),
        ["first_table"]
    );

    source
        .execute(
            &source_runtime,
            "CREATE TABLE second_table (id INTEGER PRIMARY KEY)",
            (),
        )
        .unwrap();
    assert_eq!(
        tables.query_map((), |row| row.get::<_, String>(0)).unwrap(),
        ["first_table"],
        "fresh local state should not contact authority"
    );

    std::thread::sleep(Duration::from_millis(120));
    assert_eq!(
        tables.query_map((), |row| row.get::<_, String>(0)).unwrap(),
        ["first_table", "second_table"]
    );
}

#[test]
fn policies_and_isolation_levels_converge_across_two_devices_and_restart() {
    for (policy_name, policy) in [
        ("local", SyncPolicy::LocalOnly),
        (
            "local-first",
            SyncPolicy::LocalFirst {
                write_delay: Duration::from_secs(3600),
                read_staleness: Duration::from_secs(3600),
            },
        ),
        ("remote", SyncPolicy::Remote),
    ] {
        for (isolation_name, isolation) in [
            ("snapshot", IsolationLevel::Snapshot),
            ("serializable", IsolationLevel::Serializable),
        ] {
            let directory = tempfile::tempdir().unwrap();
            let first_path = directory
                .path()
                .join(format!("{policy_name}-{isolation_name}-first.sqlite"));
            let second_path = directory
                .path()
                .join(format!("{policy_name}-{isolation_name}-second.sqlite"));
            let server = server();
            let first = Database::open_with(
                &first_path,
                OpenOptions::new()
                    .sync_policy(policy)
                    .isolation_level(isolation)
                    .server(router(Arc::clone(&server))),
            )
            .unwrap();
            assert!(server.create_space(first.database_id().space_id()));
            let first_runtime = first.runtime().unwrap();
            first
                .execute(
                    &first_runtime,
                    "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)",
                    (),
                )
                .unwrap();
            assert_eq!(first.push().unwrap(), PushOutcome::Drained);

            let second = Database::open_with(
                &second_path,
                OpenOptions::new()
                    .invitation(first.replica_invitation())
                    .sync_policy(policy)
                    .isolation_level(isolation)
                    .server(router(Arc::clone(&server))),
            )
            .unwrap();
            let second_runtime = second.runtime().unwrap();
            second.pull().unwrap();
            second.rebase(&second_runtime).unwrap();

            first
                .update_with(&first_runtime, UpdateOptions::new(isolation), |update| {
                    update.execute("INSERT INTO notes VALUES (1, 'first')", ())?;
                    Ok(())
                })
                .unwrap();
            second
                .update_with(&second_runtime, UpdateOptions::new(isolation), |update| {
                    update.execute("INSERT INTO notes VALUES (2, 'second')", ())?;
                    Ok(())
                })
                .unwrap();
            assert_eq!(first.push().unwrap(), PushOutcome::Drained);
            assert_eq!(second.push().unwrap(), PushOutcome::Drained);

            first.pull().unwrap();
            first.rebase(&first_runtime).unwrap();
            second.pull().unwrap();
            second.rebase(&second_runtime).unwrap();
            assert_eq!(row_ids(&first), [1, 2]);
            assert_eq!(row_ids(&second), [1, 2]);

            drop(first_runtime);
            drop(second_runtime);
            drop(first);
            drop(second);

            let reopened_first = Database::open_with(
                &first_path,
                OpenOptions::new()
                    .sync_policy(policy)
                    .isolation_level(isolation)
                    .server(router(Arc::clone(&server))),
            )
            .unwrap();
            let reopened_second = Database::open_with(
                &second_path,
                OpenOptions::new()
                    .sync_policy(policy)
                    .isolation_level(isolation)
                    .server(router(Arc::clone(&server))),
            )
            .unwrap();
            assert_eq!(row_ids(&reopened_first), [1, 2]);
            assert_eq!(row_ids(&reopened_second), [1, 2]);
        }
    }
}

#[test]
fn authority_read_pushes_pending_local_submissions_before_rebase() {
    let directory = tempfile::tempdir().unwrap();
    let server = server();
    let path = directory.path().join("pending-read.sqlite");
    let database = Database::open_with(
        &path,
        OpenOptions::new().server(router(Arc::clone(&server))),
    )
    .unwrap();
    assert!(server.create_space(database.database_id().space_id()));
    let runtime = database.runtime().unwrap();
    database
        .execute(
            &runtime,
            "CREATE TABLE pending (id INTEGER PRIMARY KEY)",
            (),
        )
        .unwrap();
    assert_eq!(pending_ops(&database).len(), 1);
    drop(runtime);
    drop(database);

    let database = Database::open_with(
        &path,
        OpenOptions::new()
            .sync_policy(SyncPolicy::Remote)
            .server(router(Arc::clone(&server))),
    )
    .unwrap();
    let runtime = Arc::new(database.runtime().unwrap());
    let mut statement = database.prepare(&runtime, "SELECT 1").unwrap();

    assert_eq!(
        statement.query_map((), |row| row.get::<_, i64>(0)).unwrap(),
        [1]
    );
    assert!(table_exists(&database, "pending"));
    assert!(pending_ops(&database).is_empty());
    let state = client_state(&database);
    let space = &state.spaces[&database.database_id().space_id()];
    assert_eq!(space.cursors.neck, space.cursors.tail);
    assert_eq!(space.admit_cursors.neck, space.admit_cursors.tail);
}

#[test]
fn authority_read_surfaces_push_rejection_without_implicit_rollback() {
    let directory = tempfile::tempdir().unwrap();
    let server = server();
    let first = Database::open_with(
        directory.path().join("read-winner.sqlite"),
        OpenOptions::new().server(router(Arc::clone(&server))),
    )
    .unwrap();
    assert!(server.create_space(first.database_id().space_id()));
    let first_runtime = first.runtime().unwrap();
    first
        .execute(
            &first_runtime,
            "CREATE TABLE notes (id INTEGER PRIMARY KEY)",
            (),
        )
        .unwrap();
    assert_eq!(first.push().unwrap(), PushOutcome::Drained);

    let second_path = directory.path().join("read-loser.sqlite");
    let second = Database::open_with(
        &second_path,
        OpenOptions::new()
            .invitation(first.replica_invitation())
            .server(router(Arc::clone(&server))),
    )
    .unwrap();
    let second_runtime = second.runtime().unwrap();
    second
        .execute(
            &second_runtime,
            "CREATE TABLE NOTES (id INTEGER PRIMARY KEY, payload BLOB)",
            (),
        )
        .unwrap();
    assert_eq!(pending_ops(&second).len(), 1);
    drop(second_runtime);
    drop(second);

    let second = Database::open_with(
        &second_path,
        OpenOptions::new()
            .sync_policy(SyncPolicy::Remote)
            .server(router(Arc::clone(&server))),
    )
    .unwrap();
    let second_runtime = Arc::new(second.runtime().unwrap());
    let mut statement = second.prepare(&second_runtime, "SELECT 1").unwrap();

    let error = statement
        .query_map((), |row| row.get::<_, i64>(0))
        .unwrap_err();
    let Error::RefreshPushRejected(rejection) = error else {
        panic!("remote read did not surface its push rejection")
    };
    assert!(matches!(
        rejection.error(),
        KernelError::RangeAssertFailed { .. }
    ));
    assert!(table_exists(&second, "notes"));
    assert_eq!(pending_ops(&second).len(), 1);

    second.rollback(&rejection).unwrap();
    assert!(!table_exists(&second, "notes"));
    assert!(pending_ops(&second).is_empty());
}

#[test]
fn create_table_and_homebase_submission_commit_atomically_and_survive_reopen() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("captured-schema.sqlite");
    let database = Database::open(&path).unwrap();
    let database_id = database.database_id();
    let runtime = Arc::new(database.runtime().unwrap());

    database
        .execute(
            &runtime,
            "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT NOT NULL)",
            (),
        )
        .unwrap();
    assert!(table_exists(&database, "notes"));

    let state = client_state(&database);
    let space = state.spaces.get(&database_id.space_id()).unwrap();
    assert_eq!(space.cursors.tail, DeviceSeq(2));
    let DeviceOp::Commit {
        entries,
        range_asserts,
        submit_mode,
        ..
    } = space.oplog.get(&DeviceSeq(1)).unwrap()
    else {
        panic!("captured schema operation was not a commit")
    };
    assert_eq!(entries.len(), 8);
    assert_eq!(range_asserts.len(), 2);
    assert_eq!(*submit_mode, SubmitMode::Unchecked);
    assert!(
        range_asserts
            .iter()
            .all(|assertion| assertion.upto == AdmissionSeq(0))
    );
    assert_eq!(range_asserts[0].prefix, *entries[2].key());
    assert_eq!(range_asserts[1].prefix, *entries[7].key());
    let pending = pending_ops(&database);
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].seq, DeviceSeq(1));
    assert!(pending[0].on_accept.is_empty());
    assert!(matches!(
        pending[0].on_reject.as_slice(),
        [pending::Effect::DropTable { created }] if created.table_name() == "notes"
    ));

    drop(runtime);
    drop(database);

    let reopened = Database::open(&path).unwrap();
    assert!(table_exists(&reopened, "notes"));
    let state = client_state(&reopened);
    let space = state.spaces.get(&database_id.space_id()).unwrap();
    assert_eq!(space.cursors.tail, DeviceSeq(2));
    assert!(matches!(
        space.oplog.get(&DeviceSeq(1)),
        Some(DeviceOp::Commit { .. })
    ));
    assert_eq!(pending_ops(&reopened), pending);
}

#[test]
fn failed_schema_submission_rolls_back_the_created_table_and_oplog() {
    let directory = tempfile::tempdir().unwrap();
    let database = Database::open(directory.path().join("atomic-schema.sqlite")).unwrap();
    let database_id = database.database_id();
    let runtime = database.runtime().unwrap();
    database.with_connection(|connection| {
        connection
            .execute_batch(
                "CREATE TRIGGER reject_schema_submission
                     BEFORE INSERT ON __multilite__meta
                     BEGIN SELECT RAISE(ABORT, 'injected metadata failure'); END",
            )
            .unwrap();
    });

    assert!(
        database
            .execute(
                &runtime,
                "CREATE TABLE rolled_back (id INTEGER PRIMARY KEY)",
                (),
            )
            .is_err()
    );
    assert!(!table_exists(&database, "rolled_back"));

    let state = client_state(&database);
    let space = state.spaces.get(&database_id.space_id()).unwrap();
    assert_eq!(
        space.cursors,
        homebase_client::meta::OplogCursors::default()
    );
    assert!(space.oplog.is_empty());
    assert!(pending_ops(&database).is_empty());
}

#[test]
fn failed_pending_insert_rolls_back_the_table_and_homebase_submission() {
    let directory = tempfile::tempdir().unwrap();
    let database = Database::open(directory.path().join("atomic-pending.sqlite")).unwrap();
    let database_id = database.database_id();
    let runtime = database.runtime().unwrap();
    database.with_connection(|connection| {
        connection
            .execute_batch(
                "CREATE TRIGGER reject_pending_insert
                     BEFORE INSERT ON __multilite__pending
                     BEGIN SELECT RAISE(ABORT, 'injected pending failure'); END",
            )
            .unwrap();
    });

    assert!(
        database
            .execute(
                &runtime,
                "CREATE TABLE rolled_back_pending (id INTEGER PRIMARY KEY)",
                (),
            )
            .is_err()
    );
    assert!(!table_exists(&database, "rolled_back_pending"));
    assert!(pending_ops(&database).is_empty());

    let state = client_state(&database);
    let space = state.spaces.get(&database_id.space_id()).unwrap();
    assert_eq!(
        space.cursors,
        homebase_client::meta::OplogCursors::default()
    );
    assert!(space.oplog.is_empty());
}

#[test]
fn serializable_branch_update_is_one_submission_pending_record_and_admission() {
    let directory = tempfile::tempdir().unwrap();
    let server = server();
    let database = Database::open_with(
        directory.path().join("serialized-update.sqlite"),
        OpenOptions::new().server(router(Arc::clone(&server))),
    )
    .unwrap();
    assert!(server.create_space(database.database_id.space_id()));
    let replica = Database::open_with(
        directory.path().join("serialized-update-replica.sqlite"),
        OpenOptions::new()
            .invitation(database.replica_invitation())
            .server(router(Arc::clone(&server))),
    )
    .unwrap();
    let runtime = database.runtime().unwrap();
    let replica_runtime = replica.runtime().unwrap();

    let changed = database
        .update_with(
            &runtime,
            UpdateOptions::new(IsolationLevel::Serializable),
            |update| {
                Ok([
                    update.execute(
                        "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT NOT NULL)",
                        (),
                    )?,
                    update.execute("INSERT INTO notes VALUES (1, 'one')", ())?,
                    update.execute("INSERT INTO notes VALUES (2, 'two')", ())?,
                ])
            },
        )
        .unwrap();
    assert_eq!(changed, [0, 1, 1]);

    let state = client_state(&database);
    let space = &state.spaces[&database.database_id.space_id()];
    assert_eq!(space.cursors.tail, DeviceSeq(2));
    assert_eq!(space.oplog.len(), 1);
    let DeviceOp::Commit {
        entries,
        range_asserts,
        ..
    } = &space.oplog[&DeviceSeq(1)]
    else {
        panic!("serialized update was not one Homebase commit")
    };
    assert!(entries.len() > 3);
    assert!(!range_asserts.is_empty());
    assert!(
        range_asserts
            .iter()
            .all(|assertion| assertion.upto == AdmissionSeq(0))
    );

    let pending = pending_ops(&database);
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].seq, DeviceSeq(1));
    assert!(matches!(
        pending[0].transaction.operations(),
        [
            MultiliteOp::CreateTable(_),
            MultiliteOp::InsertRows(_),
            MultiliteOp::InsertRows(_)
        ]
    ));

    assert_eq!(database.push().unwrap(), PushOutcome::Drained);
    assert!(pending_ops(&database).is_empty());
    assert_eq!(database.pull().unwrap().captured_through(), 1);
    let state = client_state(&database);
    let space = &state.spaces[&database.database_id.space_id()];
    assert_eq!(space.admits.len(), 1);
    let admitted = block_on(async {
        database
            .client
            .space(database.database_id.space_id())
            .await
            .unwrap()
            .admits()
            .iter(AdmissionSeq(1)..AdmissionSeq(2))
            .await
            .unwrap()
    });
    assert_eq!(admitted.len(), 1);
    let decoded = MultiliteTransaction::from_homebase(&admitted[0]).unwrap();
    assert_eq!(decoded.operations().len(), 3);

    database.rebase(&runtime).unwrap();
    assert_eq!(replica.pull().unwrap().captured_through(), 1);
    replica.rebase(&replica_runtime).unwrap();
    let rows = |database: &Database<_>| {
        database.with_connection(|connection| {
            connection
                .prepare("SELECT id, body FROM notes ORDER BY id")
                .unwrap()
                .query_map((), |row| {
                    Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
                })
                .unwrap()
                .collect::<rusqlite::Result<Vec<_>>>()
                .unwrap()
        })
    };
    let expected = vec![(1, "one".into()), (2, "two".into())];
    assert_eq!(rows(&database), expected);
    assert_eq!(rows(&replica), expected);
}

#[test]
fn managed_update_reads_are_asserted_only_for_serializable_isolation() {
    let directory = tempfile::tempdir().unwrap();
    let assertions_for = |isolation, filename: &str, table: &str| {
        let database = Database::open(directory.path().join(filename)).unwrap();
        let runtime = database.runtime().unwrap();
        database
            .execute(
                &runtime,
                &format!(
                    "CREATE TABLE {table} (
                        id INTEGER PRIMARY KEY,
                        day TEXT NOT NULL
                     )"
                ),
                (),
            )
            .unwrap();
        database
            .update_with(&runtime, UpdateOptions::new(isolation), |update| {
                assert_eq!(
                    update.query(
                        &format!("SELECT count(*) FROM {table} WHERE day = ?1"),
                        ["mon"],
                        |row| row.get::<_, i64>(0),
                    )?,
                    [0]
                );
                update.execute(&format!("INSERT INTO {table} VALUES (1, 'mon')"), ())?;
                Ok(())
            })
            .unwrap();

        let traced = database.with_connection(|connection| {
            let created = catalog::by_name(connection, table).unwrap().unwrap();
            schema::table_prefix(created.table_id())
        });
        let state = client_state(&database);
        let space = &state.spaces[&database.database_id.space_id()];
        (space.oplog[&DeviceSeq(2)].range_asserts().to_vec(), traced)
    };

    let (snapshot, snapshot_read) =
        assertions_for(IsolationLevel::Snapshot, "snapshot.sqlite", "notes");
    assert!(
        !snapshot
            .iter()
            .any(|assertion| assertion.prefix == snapshot_read)
    );

    let (serializable, serializable_read) =
        assertions_for(IsolationLevel::Serializable, "serializable.sqlite", "tasks");
    assert!(
        serializable
            .iter()
            .any(|assertion| assertion.prefix == serializable_read)
    );
    assert!(
        serializable
            .iter()
            .all(|assertion| assertion.upto == AdmissionSeq(0))
    );
}

#[test]
fn serializable_branch_traces_joins_and_insert_select_sources() {
    let directory = tempfile::tempdir().unwrap();
    let database = Database::open(directory.path().join("coarse-reads.sqlite")).unwrap();
    let runtime = database.runtime().unwrap();
    database
        .update(&runtime, |update| {
            update.execute(
                "CREATE TABLE source (id INTEGER PRIMARY KEY, body TEXT NOT NULL)",
                (),
            )?;
            update.execute(
                "CREATE TABLE lookup (id INTEGER PRIMARY KEY, enabled INTEGER NOT NULL)",
                (),
            )?;
            update.execute(
                "CREATE TABLE archive (id INTEGER PRIMARY KEY, body TEXT NOT NULL)",
                (),
            )?;
            Ok(())
        })
        .unwrap();
    database
        .update(&runtime, |update| {
            update.execute("INSERT INTO source VALUES (1, 'one')", ())?;
            update.execute("INSERT INTO lookup VALUES (1, 1)", ())?;
            Ok(())
        })
        .unwrap();

    database
        .update_with(
            &runtime,
            UpdateOptions::new(IsolationLevel::Serializable),
            |update| {
                assert_eq!(
                    update.query(
                        "SELECT count(*)
                         FROM source JOIN lookup ON lookup.id = source.id
                         WHERE lookup.enabled = 1",
                        (),
                        |row| row.get::<_, i64>(0),
                    )?,
                    [1]
                );
                update.execute(
                    "INSERT INTO archive SELECT id, body FROM source WHERE id = 1",
                    (),
                )?;
                Ok(())
            },
        )
        .unwrap();

    let expected = database.with_connection(|connection| {
        ["source", "lookup"]
            .into_iter()
            .map(|table| {
                let created = catalog::by_name(connection, table).unwrap().unwrap();
                schema::table_prefix(created.table_id())
            })
            .collect::<BTreeSet<_>>()
    });
    let state = client_state(&database);
    let space = &state.spaces[&database.database_id.space_id()];
    let last = DeviceSeq(space.cursors.tail.0 - 1);
    let DeviceOp::Commit { range_asserts, .. } = &space.oplog[&last] else {
        panic!("serializable branch did not submit one commit")
    };
    let asserted = range_asserts
        .iter()
        .map(|assertion| assertion.prefix.clone())
        .collect::<BTreeSet<_>>();
    assert!(expected.is_subset(&asserted));
}

#[test]
fn statement_failure_rolls_back_the_complete_serializable_update() {
    let directory = tempfile::tempdir().unwrap();
    let database = Database::open(directory.path().join("statement-failure.sqlite")).unwrap();
    let runtime = database.runtime().unwrap();

    let error = database
        .update_with(
            &runtime,
            UpdateOptions::new(IsolationLevel::Serializable),
            |update| {
                update.execute("CREATE TABLE notes (id INTEGER PRIMARY KEY)", ())?;
                update.execute("INSERT INTO notes VALUES (1)", ())?;
                update.execute("INSERT INTO notes VALUES (1)", ())?;
                Ok(())
            },
        )
        .unwrap_err();
    assert!(matches!(error, Error::Sqlite(_)));
    assert!(!table_exists(&database, "notes"));
    assert!(
        database
            .with_connection(|connection| catalog::by_name(connection, "notes"))
            .unwrap()
            .is_none()
    );
    assert!(pending_ops(&database).is_empty());
    let state = client_state(&database);
    let space = &state.spaces[&database.database_id.space_id()];
    assert_eq!(space.cursors, OplogCursors::default());
    assert!(space.oplog.is_empty());
}

#[test]
fn final_submission_failure_rolls_back_all_serialized_update_effects() {
    let directory = tempfile::tempdir().unwrap();
    let database = Database::open(directory.path().join("submit-failure.sqlite")).unwrap();
    let runtime = database.runtime().unwrap();
    database.with_connection(|connection| {
        connection
            .execute_batch(
                "CREATE TRIGGER reject_serialized_submission
                     BEFORE INSERT ON __multilite__meta
                     BEGIN SELECT RAISE(ABORT, 'injected metadata failure'); END",
            )
            .unwrap();
    });

    assert!(
        database
            .update(&runtime, |update| {
                update.execute("CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)", ())?;
                update.execute("INSERT INTO notes VALUES (1, 'not committed')", ())?;
                Ok(())
            })
            .is_err()
    );
    assert!(!table_exists(&database, "notes"));
    assert!(pending_ops(&database).is_empty());
    let state = client_state(&database);
    let space = &state.spaces[&database.database_id.space_id()];
    assert_eq!(space.cursors, OplogCursors::default());
    assert!(space.oplog.is_empty());
}

#[test]
fn push_drains_and_retires_accepted_pending_operations() {
    let directory = tempfile::tempdir().unwrap();
    let server = server();
    let database = Database::open_with(
        directory.path().join("pushed.sqlite"),
        OpenOptions::new().server(router(Arc::clone(&server))),
    )
    .unwrap();
    assert!(server.create_space(database.database_id().space_id()));
    let runtime = database.runtime().unwrap();
    database
        .execute(&runtime, "CREATE TABLE notes (id INTEGER PRIMARY KEY)", ())
        .unwrap();

    assert_eq!(database.push().unwrap(), PushOutcome::Drained);
    assert!(pending_ops(&database).is_empty());
    assert!(table_exists(&database, "notes"));
    let state = client_state(&database);
    let space = state
        .spaces
        .get(&database.database_id().space_id())
        .unwrap();
    assert_eq!(space.cursors.neck, DeviceSeq(2));
    assert_eq!(space.cursors.tail, DeviceSeq(2));
}

#[test]
fn pull_fetches_admissions_without_applying_them_and_survives_reopen() {
    let directory = tempfile::tempdir().unwrap();
    let source_path = directory.path().join("source.sqlite");
    let replica_path = directory.path().join("replica.sqlite");
    let server = server();
    let source = Database::open_with(
        &source_path,
        OpenOptions::new().server(router(Arc::clone(&server))),
    )
    .unwrap();
    assert!(server.create_space(source.database_id().space_id()));
    let replica = Database::open_with(
        &replica_path,
        OpenOptions::new()
            .invitation(source.replica_invitation())
            .server(router(Arc::clone(&server))),
    )
    .unwrap();
    let source_runtime = source.runtime().unwrap();

    source
        .execute(
            &source_runtime,
            "CREATE TABLE notes (id INTEGER PRIMARY KEY)",
            (),
        )
        .unwrap();
    assert_eq!(source.push().unwrap(), PushOutcome::Drained);
    assert!(!table_exists(&replica, "notes"));

    let outcome = replica.pull().unwrap();
    assert_eq!(outcome.captured_through(), 1);
    let after_first_pull = client_state(&replica);
    let space = after_first_pull
        .spaces
        .get(&replica.database_id().space_id())
        .unwrap();
    assert_eq!(
        space.admit_cursors,
        AdmitCursors {
            head: AdmissionSeq(1),
            neck: AdmissionSeq(1),
            tail: AdmissionSeq(2),
        }
    );
    assert_eq!(space.admits.len(), 1);
    assert_eq!(space.admits[&AdmissionSeq(1)].entries.len(), 8);
    assert!(!table_exists(&replica, "notes"));

    assert_eq!(replica.pull().unwrap(), outcome);
    assert_eq!(client_state(&replica), after_first_pull);
    assert!(!table_exists(&replica, "notes"));

    drop(replica);
    let reopened = Database::open_with(
        &replica_path,
        OpenOptions::new().server(router(Arc::clone(&server))),
    )
    .unwrap();
    let reopened_state = client_state(&reopened);
    let reopened_space = reopened_state
        .spaces
        .get(&reopened.database_id().space_id())
        .unwrap();
    assert_eq!(reopened_space.admit_cursors, space.admit_cursors);
    assert_eq!(reopened_space.admits, space.admits);
    assert!(!table_exists(&reopened, "notes"));
}

#[test]
fn unavailable_pull_preserves_the_admit_log() {
    let directory = tempfile::tempdir().unwrap();
    let database = Database::open(directory.path().join("offline-pull.sqlite")).unwrap();
    let before = client_state(&database);

    assert!(database.pull().is_err());
    assert_eq!(client_state(&database), before);
}

#[test]
fn empty_rebase_is_an_idempotent_local_noop() {
    let directory = tempfile::tempdir().unwrap();
    let database = Database::open(directory.path().join("empty-rebase.sqlite")).unwrap();
    let runtime = database.runtime().unwrap();
    let before = client_state(&database);

    database.rebase(&runtime).unwrap();
    assert_eq!(client_state(&database), before);
}

#[test]
fn rebase_rejects_pending_submission_even_without_fetched_admissions() {
    let directory = tempfile::tempdir().unwrap();
    let database = Database::open(directory.path().join("pending-rebase.sqlite")).unwrap();
    let runtime = database.runtime().unwrap();
    database
        .execute(&runtime, "CREATE TABLE notes (id INTEGER PRIMARY KEY)", ())
        .unwrap();
    let before = client_state(&database);

    assert!(matches!(
        database.rebase(&runtime),
        Err(Error::RebasePendingSubmissions)
    ));
    assert_eq!(client_state(&database), before);
    assert!(table_exists(&database, "notes"));
    assert_eq!(pending_ops(&database).len(), 1);
}

#[test]
fn rebase_rejects_cursor_changes_between_snapshot_and_apply() {
    let directory = tempfile::tempdir().unwrap();
    let server = server();
    let source = Database::open_with(
        directory.path().join("moving-source.sqlite"),
        OpenOptions::new().server(router(Arc::clone(&server))),
    )
    .unwrap();
    assert!(server.create_space(source.database_id().space_id()));
    let replica = Database::open_with(
        directory.path().join("moving-replica.sqlite"),
        OpenOptions::new()
            .invitation(source.replica_invitation())
            .server(router(Arc::clone(&server))),
    )
    .unwrap();
    let source_runtime = source.runtime().unwrap();
    let replica_runtime = replica.runtime().unwrap();
    source
        .execute(
            &source_runtime,
            "CREATE TABLE first_remote (id INTEGER PRIMARY KEY)",
            (),
        )
        .unwrap();
    assert_eq!(source.push().unwrap(), PushOutcome::Drained);
    replica.pull().unwrap();

    let error = replica
        .rebase_after_snapshot(&replica_runtime, || {
            source.execute(
                &source_runtime,
                "CREATE TABLE second_remote (id INTEGER PRIMARY KEY)",
                (),
            )?;
            assert_eq!(source.push()?, PushOutcome::Drained);
            replica.pull()?;
            Ok(())
        })
        .unwrap_err();

    assert!(matches!(error, Error::RebaseStateChanged));
    assert!(!table_exists(&replica, "first_remote"));
    assert!(!table_exists(&replica, "second_remote"));
    let state = client_state(&replica);
    let space = &state.spaces[&replica.database_id().space_id()];
    assert_eq!(space.admit_cursors.neck, AdmissionSeq(1));
    assert_eq!(space.admit_cursors.tail, AdmissionSeq(3));
    assert_eq!(space.admits.len(), 2);
}

#[test]
fn rebase_applies_foreign_tables_and_preserves_own_tables_on_both_replicas() {
    let directory = tempfile::tempdir().unwrap();
    let first_path = directory.path().join("first-rebase.sqlite");
    let second_path = directory.path().join("second-rebase.sqlite");
    let server = server();
    let first = Database::open_with(
        &first_path,
        OpenOptions::new().server(router(Arc::clone(&server))),
    )
    .unwrap();
    assert!(server.create_space(first.database_id().space_id()));
    let second = Database::open_with(
        &second_path,
        OpenOptions::new()
            .invitation(first.replica_invitation())
            .server(router(Arc::clone(&server))),
    )
    .unwrap();
    let first_runtime = first.runtime().unwrap();
    let second_runtime = second.runtime().unwrap();

    first
        .execute(
            &first_runtime,
            "CREATE TABLE notes (id INTEGER PRIMARY KEY)",
            (),
        )
        .unwrap();
    second
        .execute(
            &second_runtime,
            "CREATE TABLE tasks (id INTEGER PRIMARY KEY)",
            (),
        )
        .unwrap();
    assert_eq!(first.push().unwrap(), PushOutcome::Drained);
    assert_eq!(second.push().unwrap(), PushOutcome::Drained);
    assert_eq!(first.pull().unwrap().captured_through(), 2);
    assert_eq!(second.pull().unwrap().captured_through(), 2);

    first.rebase(&first_runtime).unwrap();
    second.rebase(&second_runtime).unwrap();
    for database in [&first, &second] {
        assert!(table_exists(database, "notes"));
        assert!(table_exists(database, "tasks"));
        let state = client_state(database);
        let space = &state.spaces[&database.database_id().space_id()];
        assert_eq!(space.admit_cursors.neck, AdmissionSeq(3));
        assert_eq!(space.admit_cursors.tail, AdmissionSeq(3));
    }

    drop(first_runtime);
    drop(first);
    let reopened = Database::open_with(
        &first_path,
        OpenOptions::new().server(router(Arc::clone(&server))),
    )
    .unwrap();
    assert!(table_exists(&reopened, "notes"));
    assert!(table_exists(&reopened, "tasks"));
    let state = client_state(&reopened);
    assert_eq!(
        state.spaces[&reopened.database_id().space_id()]
            .admit_cursors
            .neck,
        AdmissionSeq(3)
    );
}

#[test]
fn pull_before_push_conflict_recovers_across_restarts_and_converges() {
    let directory = tempfile::tempdir().unwrap();
    let server = server();
    let first_path = directory.path().join("winning-schema.sqlite");
    let second_path = directory.path().join("conflicting-schema.sqlite");
    let first = Database::open_with(
        &first_path,
        OpenOptions::new().server(router(Arc::clone(&server))),
    )
    .unwrap();
    assert!(server.create_space(first.database_id().space_id()));
    let invitation = first.replica_invitation();
    let second = Database::open_with(
        &second_path,
        OpenOptions::new()
            .invitation(invitation)
            .server(router(Arc::clone(&server))),
    )
    .unwrap();
    let first_runtime = first.runtime().unwrap();
    let second_runtime = second.runtime().unwrap();

    first
        .execute(
            &first_runtime,
            "CREATE TABLE notes (id INTEGER PRIMARY KEY)",
            (),
        )
        .unwrap();
    assert_eq!(first.push().unwrap(), PushOutcome::Drained);
    second
        .execute(
            &second_runtime,
            "CREATE TABLE NOTES (id INTEGER PRIMARY KEY, payload BLOB)",
            (),
        )
        .unwrap();
    second.pull().unwrap();
    let before = client_state(&second);
    let before_sql = table_sql(&second, "notes").unwrap();

    assert!(matches!(
        second.rebase(&second_runtime),
        Err(Error::RebasePendingSubmissions)
    ));
    assert_eq!(client_state(&second), before);
    assert_eq!(table_sql(&second, "notes").unwrap(), before_sql);
    assert_eq!(pending_ops(&second).len(), 1);

    let PushOutcome::Rejected(before_restart) = second.push().unwrap() else {
        panic!("same-name schema submission unexpectedly drained")
    };
    drop(second_runtime);
    drop(second);

    let second = Database::open_with(
        &second_path,
        OpenOptions::new().server(router(Arc::clone(&server))),
    )
    .unwrap();
    let second_runtime = second.runtime().unwrap();
    assert!(table_exists(&second, "NOTES"));
    assert_eq!(pending_ops(&second).len(), 1);
    let PushOutcome::Rejected(after_restart) = second.push().unwrap() else {
        panic!("re-probed schema submission unexpectedly drained")
    };
    assert_eq!(after_restart, before_restart);

    second.rollback(&after_restart).unwrap();
    assert!(pending_ops(&second).is_empty());
    assert!(!table_exists(&second, "notes"));
    drop(second_runtime);
    drop(second);

    let second = Database::open_with(
        &second_path,
        OpenOptions::new().server(router(Arc::clone(&server))),
    )
    .unwrap();
    let second_runtime = second.runtime().unwrap();
    let state = client_state(&second);
    let space = &state.spaces[&second.database_id().space_id()];
    assert_eq!(space.cursors.neck, DeviceSeq(2));
    assert_eq!(space.cursors.tail, DeviceSeq(3));
    assert_eq!(
        space.oplog[&DeviceSeq(2)],
        DeviceOp::Rollback {
            marker: DeviceSeq(1)
        }
    );
    assert!(matches!(
        second.rebase(&second_runtime),
        Err(Error::RebasePendingSubmissions)
    ));
    assert_eq!(second.push().unwrap(), PushOutcome::Drained);
    drop(second_runtime);
    drop(second);

    let second = Database::open_with(
        &second_path,
        OpenOptions::new().server(router(Arc::clone(&server))),
    )
    .unwrap();
    let second_runtime = second.runtime().unwrap();
    second.pull().unwrap();
    second.rebase(&second_runtime).unwrap();
    assert!(table_exists(&second, "notes"));

    first.pull().unwrap();
    first.rebase(&first_runtime).unwrap();
    assert_eq!(table_sql(&first, "notes"), table_sql(&second, "notes"));
    let first_state = client_state(&first);
    let second_state = client_state(&second);
    assert_eq!(
        first_state.spaces[&first.database_id().space_id()].admit_cursors,
        second_state.spaces[&second.database_id().space_id()].admit_cursors
    );

    drop(first_runtime);
    drop(second_runtime);
    drop(first);
    drop(second);
    assert_eq!(
        stock_user_schema(&first_path),
        stock_user_schema(&second_path)
    );
    assert_eq!(
        stock_user_schema(&first_path),
        [(
            String::from("notes"),
            String::from("CREATE TABLE notes (id INTEGER PRIMARY KEY)")
        )]
    );
}

#[test]
fn malformed_admitted_transaction_fails_rebase_without_advancing_neck() {
    let directory = tempfile::tempdir().unwrap();
    let server = server();
    let source = Database::open_with(
        directory.path().join("malformed-source.sqlite"),
        OpenOptions::new().server(router(Arc::clone(&server))),
    )
    .unwrap();
    assert!(server.create_space(source.database_id().space_id()));
    let replica = Database::open_with(
        directory.path().join("malformed-replica.sqlite"),
        OpenOptions::new()
            .invitation(source.replica_invitation())
            .server(router(Arc::clone(&server))),
    )
    .unwrap();
    let replica_runtime = replica.runtime().unwrap();
    let malformed_key = Key::from_bytes([b"malformed".as_slice()]).unwrap();
    block_on(async {
        source
            .client
            .space(source.database_id().space_id())
            .await
            .unwrap()
            .submit_unchecked(
                vec![Mutation::Set {
                    key: malformed_key,
                    value: vec![1, 2, 3],
                }],
                vec![],
            )
            .await
            .unwrap();
    });
    assert_eq!(source.push().unwrap(), PushOutcome::Drained);
    replica.pull().unwrap();
    let before = client_state(&replica);

    assert!(matches!(
        replica.rebase(&replica_runtime),
        Err(Error::InvalidMultiliteTransaction(_))
    ));
    assert_eq!(client_state(&replica), before);
}

#[test]
fn failed_remote_ddl_rolls_back_prior_tables_and_admit_neck() {
    let directory = tempfile::tempdir().unwrap();
    let server = server();
    let source = Database::open_with(
        directory.path().join("atomic-source.sqlite"),
        OpenOptions::new().server(router(Arc::clone(&server))),
    )
    .unwrap();
    assert!(server.create_space(source.database_id().space_id()));
    let replica = Database::open_with(
        directory.path().join("atomic-replica.sqlite"),
        OpenOptions::new()
            .invitation(source.replica_invitation())
            .server(router(Arc::clone(&server))),
    )
    .unwrap();
    let replica_runtime = replica.runtime().unwrap();
    submit_direct(&source, &create_operation("first_remote"));
    submit_direct(&source, &create_operation("occupied"));
    assert_eq!(source.push().unwrap(), PushOutcome::Drained);
    replica.pull().unwrap();
    replica.with_connection(|connection| {
        connection
            .execute_batch("CREATE TABLE occupied (id INTEGER PRIMARY KEY, local BLOB)")
            .unwrap();
    });
    let before = client_state(&replica);

    assert!(matches!(
        replica.rebase(&replica_runtime),
        Err(Error::Sqlite(_))
    ));
    assert!(!table_exists(&replica, "first_remote"));
    assert!(table_exists(&replica, "occupied"));
    assert_eq!(client_state(&replica), before);
}

#[test]
fn rollback_preserves_an_accepted_prefix_and_retires_only_the_rejected_suffix() {
    let directory = tempfile::tempdir().unwrap();
    let server = server();
    let first = Database::open_with(
        directory.path().join("first.sqlite"),
        OpenOptions::new().server(router(Arc::clone(&server))),
    )
    .unwrap();
    assert!(server.create_space(first.database_id().space_id()));
    let second = Database::open_with(
        directory.path().join("second.sqlite"),
        OpenOptions::new()
            .invitation(first.replica_invitation())
            .server(router(Arc::clone(&server))),
    )
    .unwrap();
    let first_runtime = first.runtime().unwrap();
    let second_runtime = second.runtime().unwrap();

    first
        .execute(
            &first_runtime,
            "CREATE TABLE notes (id INTEGER PRIMARY KEY)",
            (),
        )
        .unwrap();
    assert_eq!(first.push().unwrap(), PushOutcome::Drained);

    second
        .execute(
            &second_runtime,
            "CREATE TABLE tasks (id INTEGER PRIMARY KEY)",
            (),
        )
        .unwrap();
    second
        .execute(
            &second_runtime,
            "CREATE TABLE \"NOTES\" (id INTEGER PRIMARY KEY)",
            (),
        )
        .unwrap();

    let PushOutcome::Rejected(rejection) = second.push().unwrap() else {
        panic!("same-name schema submission unexpectedly drained")
    };
    assert_eq!(rejection.database_id, second.database_id());
    assert_eq!(rejection.device_id, second.client.device());
    assert_eq!(rejection.failed_sequence(), 2);
    assert_eq!(
        rejection.submit_cursors,
        OplogCursors {
            head: DeviceSeq(2),
            neck: DeviceSeq(2),
            tail: DeviceSeq(3),
        }
    );
    assert!(matches!(
        rejection.error(),
        KernelError::RangeAssertFailed { failures } if failures.len() == 1
    ));
    let pending = pending_ops(&second);
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].seq, DeviceSeq(2));
    assert!(table_exists(&second, "tasks"));
    assert!(table_exists(&second, "NOTES"));

    let before_rollback = second.with_connection(history::current).unwrap();
    second.rollback(&rejection).unwrap();
    assert_eq!(
        second.with_connection(history::current).unwrap(),
        crate::commit::snapshot::CommitSeq(before_rollback.0 + 1)
    );
    assert!(pending_ops(&second).is_empty());
    assert!(table_exists(&second, "tasks"));
    assert!(!table_exists(&second, "NOTES"));
    let after_rollback = client_state(&second);
    let space = &after_rollback.spaces[&second.database_id().space_id()];
    assert_eq!(
        space.cursors,
        OplogCursors {
            head: DeviceSeq(2),
            neck: DeviceSeq(3),
            tail: DeviceSeq(4),
        }
    );
    assert_eq!(
        space.oplog[&DeviceSeq(3)],
        DeviceOp::Rollback {
            marker: DeviceSeq(2)
        }
    );

    second.rollback(&rejection).unwrap();
    assert_eq!(client_state(&second), after_rollback);
    assert!(matches!(
        second.rebase(&second_runtime),
        Err(Error::RebasePendingSubmissions)
    ));

    assert_eq!(second.push().unwrap(), PushOutcome::Drained);
    second.pull().unwrap();
    second.rebase(&second_runtime).unwrap();
    assert!(table_exists(&second, "tasks"));
    assert!(table_exists(&second, "notes"));
}

#[test]
fn rollback_rejects_foreign_or_stale_push_rejections_without_mutation() {
    let directory = tempfile::tempdir().unwrap();
    let server = server();
    let first = Database::open_with(
        directory.path().join("stale-first.sqlite"),
        OpenOptions::new().server(router(Arc::clone(&server))),
    )
    .unwrap();
    assert!(server.create_space(first.database_id().space_id()));
    let second = Database::open_with(
        directory.path().join("stale-second.sqlite"),
        OpenOptions::new()
            .invitation(first.replica_invitation())
            .server(router(Arc::clone(&server))),
    )
    .unwrap();
    let first_runtime = first.runtime().unwrap();
    let second_runtime = second.runtime().unwrap();

    first
        .execute(
            &first_runtime,
            "CREATE TABLE notes (id INTEGER PRIMARY KEY)",
            (),
        )
        .unwrap();
    assert_eq!(first.push().unwrap(), PushOutcome::Drained);
    second
        .execute(
            &second_runtime,
            "CREATE TABLE NOTES (id INTEGER PRIMARY KEY)",
            (),
        )
        .unwrap();
    let PushOutcome::Rejected(rejection) = second.push().unwrap() else {
        panic!("same-name schema submission unexpectedly drained")
    };

    let first_before = client_state(&first);
    assert!(matches!(
        first.rollback(&rejection),
        Err(Error::StalePushRejection)
    ));
    assert_eq!(client_state(&first), first_before);
    assert!(table_exists(&first, "notes"));

    second
        .execute(
            &second_runtime,
            "CREATE TABLE tasks (id INTEGER PRIMARY KEY)",
            (),
        )
        .unwrap();
    let second_before = client_state(&second);
    let pending_before = pending_ops(&second);
    assert!(matches!(
        second.rollback(&rejection),
        Err(Error::StalePushRejection)
    ));
    assert_eq!(client_state(&second), second_before);
    assert_eq!(pending_ops(&second), pending_before);
    assert!(table_exists(&second, "NOTES"));
    assert!(table_exists(&second, "tasks"));
}

#[test]
fn rollback_failure_restores_sqlite_pending_and_homebase_state_before_retry() {
    let directory = tempfile::tempdir().unwrap();
    let server = server();
    let first = Database::open_with(
        directory.path().join("atomic-rollback-first.sqlite"),
        OpenOptions::new().server(router(Arc::clone(&server))),
    )
    .unwrap();
    assert!(server.create_space(first.database_id().space_id()));
    let second = Database::open_with(
        directory.path().join("atomic-rollback-second.sqlite"),
        OpenOptions::new()
            .invitation(first.replica_invitation())
            .server(router(Arc::clone(&server))),
    )
    .unwrap();
    let first_runtime = first.runtime().unwrap();
    let second_runtime = second.runtime().unwrap();

    first
        .execute(
            &first_runtime,
            "CREATE TABLE notes (id INTEGER PRIMARY KEY)",
            (),
        )
        .unwrap();
    assert_eq!(first.push().unwrap(), PushOutcome::Drained);
    second
        .execute(
            &second_runtime,
            "CREATE TABLE NOTES (id INTEGER PRIMARY KEY)",
            (),
        )
        .unwrap();
    let PushOutcome::Rejected(rejection) = second.push().unwrap() else {
        panic!("same-name schema submission unexpectedly drained")
    };
    second.with_connection(|connection| {
        connection
            .execute_batch(
                "CREATE TRIGGER reject_pending_rollback
                     BEFORE DELETE ON __multilite__pending
                     BEGIN SELECT RAISE(ABORT, 'injected pending rollback failure'); END",
            )
            .unwrap();
    });
    let state_before = client_state(&second);
    let pending_before = pending_ops(&second);

    assert!(second.rollback(&rejection).is_err());
    assert_eq!(client_state(&second), state_before);
    assert_eq!(pending_ops(&second), pending_before);
    assert!(table_exists(&second, "NOTES"));

    second.with_connection(|connection| {
        connection
            .execute_batch("DROP TRIGGER reject_pending_rollback")
            .unwrap();
    });
    second.rollback(&rejection).unwrap();
    assert!(pending_ops(&second).is_empty());
    assert!(!table_exists(&second, "NOTES"));
    let state = client_state(&second);
    let space = &state.spaces[&second.database_id().space_id()];
    assert_eq!(space.cursors.neck, DeviceSeq(2));
    assert_eq!(space.cursors.tail, DeviceSeq(3));
    assert_eq!(
        space.oplog[&DeviceSeq(2)],
        DeviceOp::Rollback {
            marker: DeviceSeq(1)
        }
    );
}

#[test]
fn unavailable_push_preserves_the_active_pending_window() {
    let directory = tempfile::tempdir().unwrap();
    let database = Database::open(directory.path().join("offline.sqlite")).unwrap();
    let runtime = database.runtime().unwrap();
    database
        .execute(&runtime, "CREATE TABLE notes (id INTEGER PRIMARY KEY)", ())
        .unwrap();

    assert!(database.push().is_err());
    assert_eq!(pending_ops(&database).len(), 1);
    let state = client_state(&database);
    let space = state
        .spaces
        .get(&database.database_id().space_id())
        .unwrap();
    assert_eq!(space.cursors.neck, DeviceSeq(1));
    assert_eq!(space.cursors.tail, DeviceSeq(2));
}

#[test]
fn accepted_push_with_failed_local_trim_recovers_after_reopen() {
    let directory = tempfile::tempdir().unwrap();
    let server = server();
    let path = directory.path().join("atomic-accept.sqlite");
    let database = Database::open_with(
        &path,
        OpenOptions::new().server(router(Arc::clone(&server))),
    )
    .unwrap();
    assert!(server.create_space(database.database_id().space_id()));
    let runtime = database.runtime().unwrap();
    database
        .execute(&runtime, "CREATE TABLE notes (id INTEGER PRIMARY KEY)", ())
        .unwrap();
    database.with_connection(|connection| {
        connection
            .execute_batch(
                "CREATE TRIGGER reject_pending_cleanup
                     BEFORE DELETE ON __multilite__pending
                     BEGIN SELECT RAISE(ABORT, 'injected pending cleanup failure'); END",
            )
            .unwrap();
    });

    assert!(database.push().is_err());
    assert_eq!(pending_ops(&database).len(), 1);
    assert_eq!(
        client_state(&database)
            .spaces
            .get(&database.database_id().space_id())
            .unwrap()
            .cursors
            .neck,
        DeviceSeq(1)
    );
    drop(runtime);
    drop(database);

    SqliteConnection::open(&path)
        .unwrap()
        .execute_batch("DROP TRIGGER reject_pending_cleanup")
        .unwrap();
    let database = Database::open_with(
        &path,
        OpenOptions::new().server(router(Arc::clone(&server))),
    )
    .unwrap();

    assert_eq!(database.push().unwrap(), PushOutcome::Drained);
    assert!(pending_ops(&database).is_empty());
    assert_eq!(
        client_state(&database)
            .spaces
            .get(&database.database_id().space_id())
            .unwrap()
            .cursors
            .neck,
        DeviceSeq(2)
    );
    assert!(table_exists(&database, "notes"));
    assert_eq!(database.pull().unwrap().captured_through(), 1);
    let state = client_state(&database);
    let space = &state.spaces[&database.database_id().space_id()];
    assert_eq!(space.admit_cursors.tail, AdmissionSeq(2));
    assert_eq!(space.admits.len(), 1, "the retry must not admit twice");
}

#[test]
fn database_owns_the_public_sql_surface_independent_of_format_hooks() {
    let directory = tempfile::tempdir().unwrap();
    let database = Database::open(directory.path().join("sql-surface.sqlite")).unwrap();
    let runtime = Arc::new(database.runtime().unwrap());

    assert!(matches!(
        database.execute(
            &runtime,
            "CREATE TABLE rejected (id INTEGER PRIMARY KEY AUTOINCREMENT)",
            (),
        ),
        Err(Error::UnsupportedSql("AUTOINCREMENT is not supported"))
    ));
    assert!(
        database
            .execute(
                &runtime,
                "CREATE TABLE extra_params (id INTEGER PRIMARY KEY)",
                [1],
            )
            .is_err()
    );
    assert!(!table_exists(&database, "extra_params"));
    assert!(pending_ops(&database).is_empty());
    assert_eq!(
        database.with_connection(history::current).unwrap(),
        crate::commit::snapshot::CommitSeq(0)
    );
    database
        .execute(
            &runtime,
            "CREATE TABLE accepted (id INTEGER PRIMARY KEY, value TEXT)",
            (),
        )
        .unwrap();
    assert!(
        database
            .execute(
                &runtime,
                "CREATE TABLE __multilite__rejected (value TEXT)",
                (),
            )
            .is_err()
    );
    assert!(
        database
            .prepare(&runtime, "SELECT value FROM __multilite__meta")
            .is_err()
    );
    database
        .execute(&runtime, "INSERT INTO accepted VALUES (1, 'one')", ())
        .unwrap();
    assert_eq!(
        database
            .execute(
                &runtime,
                "UPDATE accepted SET value = 'two' WHERE id = 1",
                (),
            )
            .unwrap(),
        1
    );
    assert_eq!(
        database
            .execute(&runtime, "DELETE FROM accepted WHERE id = 1", ())
            .unwrap(),
        1
    );
}

#[test]
fn identity_invitation_and_device_rules_survive_reopen() {
    let directory = tempfile::tempdir().unwrap();
    let first_path = directory.path().join("first.sqlite");
    let replica_path = directory.path().join("replica.sqlite");

    let first = Database::open(&first_path).unwrap();
    let database_id = first.database_id();
    let device_id = first.device_id();
    let invitation = first.replica_invitation();
    drop(first);

    let reopened = Database::open(&first_path).unwrap();
    assert_eq!(reopened.database_id(), database_id);
    assert_eq!(reopened.device_id(), device_id);

    let replica =
        Database::open_with(&replica_path, OpenOptions::new().invitation(invitation)).unwrap();
    assert_eq!(replica.database_id(), database_id);
    assert_ne!(replica.device_id(), device_id);
}

#[test]
fn invitation_roundtrips_and_conflicting_identity_is_rejected() {
    let directory = tempfile::tempdir().unwrap();
    let first_path = directory.path().join("first.sqlite");
    let second_path = directory.path().join("second.sqlite");
    let first = Database::open(&first_path).unwrap();
    let encoded = first.replica_invitation().to_bytes();
    let invitation = ReplicaInvitation::from_bytes(&encoded).unwrap();
    assert_eq!(invitation.database_id(), first.database_id());
    let conflicting = Database::open(&second_path).unwrap().replica_invitation();
    drop(first);

    assert!(matches!(
        Database::open_with(&first_path, OpenOptions::new().invitation(conflicting)),
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
fn general_open_adopts_an_existing_sqlite_schema() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("existing.sqlite");
    SqliteConnection::open(&path)
        .unwrap()
        .execute_batch("CREATE TABLE application_data (id INTEGER PRIMARY KEY)")
        .unwrap();

    let database = Database::open(&path).unwrap();
    assert_ne!(database.database_id().to_bytes(), [0; 16]);
    database.with_connection(|connection| {
        assert!(
            connection
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM sqlite_schema
                         WHERE name = '__multilite__meta')",
                    (),
                    |row| row.get::<_, bool>(0),
                )
                .unwrap()
        );
        assert!(
            connection
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE name = 'application_data')",
                    (),
                    |row| row.get::<_, bool>(0),
                )
                .unwrap()
        );
    });
}

#[test]
fn general_open_rejects_unrecognized_metadata_namespace_tables() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("reserved.sqlite");
    SqliteConnection::open(&path)
        .unwrap()
        .execute_batch("CREATE TABLE __multilite__meta_future (value BLOB NOT NULL)")
        .unwrap();

    assert!(matches!(
        Database::open(&path),
        Err(Error::InvalidDatabase(
            "metadata table namespace contains unexpected tables"
        ))
    ));
}

#[test]
fn multi_row_insert_is_one_durable_pending_operation() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("rows.sqlite");
    let database = Database::open(&path).unwrap();
    let runtime = database.runtime().unwrap();
    database
        .execute(
            &runtime,
            "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT, payload BLOB)",
            (),
        )
        .unwrap();
    database
        .execute(
            &runtime,
            "WITH input(body, payload) AS (
                    VALUES ('one', x'01'), ('two', NULL), ('three', x'0304')
                 )
                 INSERT INTO notes (body, payload)
                 SELECT body, payload FROM input ORDER BY body DESC",
            (),
        )
        .unwrap();

    let state = client_state(&database);
    let space = state.spaces.get(&database.database_id.space_id()).unwrap();
    assert_eq!(space.cursors.tail, DeviceSeq(3));
    let DeviceOp::Commit {
        entries,
        range_asserts,
        ..
    } = space.oplog.get(&DeviceSeq(2)).unwrap()
    else {
        panic!("captured INSERT was not one commit")
    };
    assert_eq!(entries.len(), 4);
    assert_eq!(range_asserts.len(), 5);
    assert!(entries[1..].iter().all(|entry| {
        let components = entry.key().components();
        components.len() == 6 && components[3].as_bytes() == b"rows"
    }));

    let pending = pending_ops(&database);
    assert_eq!(pending.len(), 2);
    assert!(matches!(
        pending[1].transaction.operations(),
        [MultiliteOp::InsertRows(_)]
    ));
    assert!(matches!(
        pending[1].on_reject.as_slice(),
        [pending::Effect::DeleteRows { .. }]
    ));
    database.with_connection(|connection| {
        assert_eq!(
            connection
                .query_row("SELECT count(*) FROM notes", (), |row| row.get::<_, i64>(0))
                .unwrap(),
            3
        );
        let mut statement = connection
            .prepare("SELECT id, body FROM notes ORDER BY id")
            .unwrap();
        let rows = statement
            .query_map((), |row| {
                Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
            })
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap();
        assert_eq!(
            rows,
            [(1, "two".into()), (2, "three".into()), (3, "one".into())]
        );
    });

    drop(runtime);
    drop(database);
    let reopened = Database::open(&path).unwrap();
    assert_eq!(pending_ops(&reopened).len(), 2);
}

#[test]
fn zero_row_delete_does_not_advance_local_or_homebase_history() {
    let directory = tempfile::tempdir().unwrap();
    let database = Database::open(directory.path().join("empty-delete.sqlite")).unwrap();
    let runtime = database.runtime().unwrap();
    database
        .update(&runtime, |update| {
            update.execute(
                "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT NOT NULL)",
                (),
            )?;
            update.execute("INSERT INTO notes VALUES (1, 'kept')", ())?;
            Ok(())
        })
        .unwrap();
    let state_before = client_state(&database);
    let pending_before = pending_ops(&database);
    let commit_before = database.with_connection(history::current).unwrap();

    assert_eq!(
        database
            .execute(&runtime, "DELETE FROM notes WHERE id = 99", ())
            .unwrap(),
        0
    );

    assert_eq!(client_state(&database), state_before);
    assert_eq!(pending_ops(&database), pending_before);
    assert_eq!(
        database.with_connection(history::current).unwrap(),
        commit_before
    );
    assert_eq!(row_ids(&database), [1]);
}

#[test]
fn long_primary_key_succeeds_and_oversized_key_rolls_back_before_submission() {
    let directory = tempfile::tempdir().unwrap();
    let database = Database::open(directory.path().join("large-key.sqlite")).unwrap();
    let runtime = database.runtime().unwrap();
    database
        .execute(
            &runtime,
            "CREATE TABLE notes (id TEXT NOT NULL PRIMARY KEY)",
            (),
        )
        .unwrap();
    let longest = "x".repeat(MAX_COMPONENT_LEN - 1);
    assert_eq!(
        database
            .execute(
                &runtime,
                "INSERT INTO notes VALUES (?1)",
                rusqlite::params![longest],
            )
            .unwrap(),
        1
    );
    let pending_before = pending_ops(&database);
    let state_before = client_state(&database);

    assert!(matches!(
        database.execute(
            &runtime,
            "INSERT INTO notes VALUES (?1)",
            rusqlite::params!["y".repeat(MAX_COMPONENT_LEN)],
        ),
        Err(Error::InvalidMultiliteOp(_))
    ));
    assert_eq!(pending_ops(&database), pending_before);
    assert_eq!(client_state(&database), state_before);
    database.with_connection(|connection| {
        assert_eq!(
            connection
                .query_row("SELECT length(id) FROM notes", (), |row| row
                    .get::<_, i64>(0))
                .unwrap(),
            i64::try_from(MAX_COMPONENT_LEN - 1).unwrap()
        );
    });
}

#[test]
fn two_replicas_converge_rows_and_reject_only_a_conflicting_insert() {
    let directory = tempfile::tempdir().unwrap();
    let server = server();
    let first = Database::open_with(
        directory.path().join("first.sqlite"),
        OpenOptions::new().server(router(Arc::clone(&server))),
    )
    .unwrap();
    assert!(server.create_space(first.database_id.space_id()));
    let second = Database::open_with(
        directory.path().join("second.sqlite"),
        OpenOptions::new()
            .invitation(first.replica_invitation())
            .server(router(Arc::clone(&server))),
    )
    .unwrap();
    let first_runtime = first.runtime().unwrap();
    let second_runtime = second.runtime().unwrap();

    first
        .execute(
            &first_runtime,
            "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT NOT NULL)",
            (),
        )
        .unwrap();
    assert_eq!(first.push().unwrap(), PushOutcome::Drained);
    second.pull().unwrap();
    second.rebase(&second_runtime).unwrap();
    assert_eq!(
        second.with_connection(history::current).unwrap(),
        crate::commit::snapshot::CommitSeq(2)
    );

    first
        .execute(&first_runtime, "INSERT INTO notes VALUES (1, 'first')", ())
        .unwrap();
    second
        .execute(
            &second_runtime,
            "INSERT INTO notes VALUES (2, 'second')",
            (),
        )
        .unwrap();
    assert_eq!(first.push().unwrap(), PushOutcome::Drained);
    assert_eq!(second.push().unwrap(), PushOutcome::Drained);
    first.pull().unwrap();
    second.pull().unwrap();
    first.rebase(&first_runtime).unwrap();
    second.rebase(&second_runtime).unwrap();

    first
        .execute(&first_runtime, "INSERT INTO notes VALUES (7, 'winner')", ())
        .unwrap();
    second
        .execute(&second_runtime, "INSERT INTO notes VALUES (7, 'loser')", ())
        .unwrap();
    assert_eq!(first.push().unwrap(), PushOutcome::Drained);
    let PushOutcome::Rejected(rejection) = second.push().unwrap() else {
        panic!("same primary key was not rejected")
    };
    assert!(matches!(
        rejection.error(),
        KernelError::RangeAssertFailed { .. }
    ));
    second.rollback(&rejection).unwrap();
    assert!(pending_ops(&second).is_empty());
    assert_eq!(second.push().unwrap(), PushOutcome::Drained);

    first.pull().unwrap();
    second.pull().unwrap();
    first.rebase(&first_runtime).unwrap();
    second.rebase(&second_runtime).unwrap();

    let rows = |database: &Database<_>| {
        database.with_connection(|connection| {
            let mut statement = connection
                .prepare("SELECT id, body FROM notes ORDER BY id")
                .unwrap();
            statement
                .query_map((), |row| {
                    Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
                })
                .unwrap()
                .collect::<rusqlite::Result<Vec<_>>>()
                .unwrap()
        })
    };
    let expected = vec![
        (1, String::from("first")),
        (2, String::from("second")),
        (7, String::from("winner")),
    ];
    assert_eq!(rows(&first), expected);
    assert_eq!(rows(&second), expected);
}

#[test]
fn ordinary_affinities_converge_and_numeric_unique_images_conflict() {
    let directory = tempfile::tempdir().unwrap();
    let server = server();
    let first = Database::open_with(
        directory.path().join("affinity-first.sqlite"),
        OpenOptions::new().server(router(Arc::clone(&server))),
    )
    .unwrap();
    assert!(server.create_space(first.database_id.space_id()));
    let second = Database::open_with(
        directory.path().join("affinity-second.sqlite"),
        OpenOptions::new()
            .invitation(first.replica_invitation())
            .server(router(Arc::clone(&server))),
    )
    .unwrap();
    let first_runtime = first.runtime().unwrap();
    let second_runtime = second.runtime().unwrap();

    first
        .execute(
            &first_runtime,
            "CREATE TABLE typed_values (
                id INTEGER PRIMARY KEY,
                amount DECIMAL(10, 2) UNIQUE,
                label VARCHAR(40),
                payload BLOB,
                ratio DOUBLE PRECISION,
                anything ANY
            )",
            (),
        )
        .unwrap();
    assert_eq!(first.push().unwrap(), PushOutcome::Drained);
    second.pull().unwrap();
    second.rebase(&second_runtime).unwrap();

    first
        .execute(
            &first_runtime,
            "INSERT INTO typed_values VALUES (1, '1', 8, 9, '10.5', '12')",
            (),
        )
        .unwrap();
    second
        .execute(
            &second_runtime,
            "INSERT INTO typed_values VALUES (2, 1.0, 'other', x'00', 3, '13')",
            (),
        )
        .unwrap();

    assert_eq!(first.push().unwrap(), PushOutcome::Drained);
    let PushOutcome::Rejected(rejection) = second.push().unwrap() else {
        panic!("numeric UNIQUE collision was not rejected")
    };
    assert!(matches!(
        rejection.error(),
        KernelError::RangeAssertFailed { .. }
    ));
    second.rollback(&rejection).unwrap();
    assert_eq!(second.push().unwrap(), PushOutcome::Drained);

    first.pull().unwrap();
    second.pull().unwrap();
    first.rebase(&first_runtime).unwrap();
    second.rebase(&second_runtime).unwrap();

    let storage_classes = |database: &Database<_>| {
        database.with_connection(|connection| {
            connection
                .query_row(
                    "SELECT id, amount, label, payload, ratio, anything,
                            typeof(amount), typeof(label), typeof(payload),
                            typeof(ratio), typeof(anything)
                     FROM typed_values",
                    (),
                    |row| {
                        Ok((
                            row.get::<_, i64>(0)?,
                            row.get::<_, i64>(1)?,
                            row.get::<_, String>(2)?,
                            row.get::<_, i64>(3)?,
                            row.get::<_, f64>(4)?,
                            row.get::<_, i64>(5)?,
                            row.get::<_, String>(6)?,
                            row.get::<_, String>(7)?,
                            row.get::<_, String>(8)?,
                            row.get::<_, String>(9)?,
                            row.get::<_, String>(10)?,
                        ))
                    },
                )
                .unwrap()
        })
    };
    let expected = (
        1,
        1,
        "8".into(),
        9,
        10.5,
        12,
        "integer".into(),
        "text".into(),
        "integer".into(),
        "real".into(),
        "integer".into(),
    );
    assert_eq!(storage_classes(&first), expected);
    assert_eq!(storage_classes(&second), expected);
}

#[test]
fn strict_table_row_operations_preserve_storage_classes_across_replicas() {
    let directory = tempfile::tempdir().unwrap();
    let server = server();
    let first = Database::open_with(
        directory.path().join("strict-first.sqlite"),
        OpenOptions::new().server(router(Arc::clone(&server))),
    )
    .unwrap();
    assert!(server.create_space(first.database_id.space_id()));
    let second = Database::open_with(
        directory.path().join("strict-second.sqlite"),
        OpenOptions::new()
            .invitation(first.replica_invitation())
            .server(router(Arc::clone(&server))),
    )
    .unwrap();
    let first_runtime = first.runtime().unwrap();
    let second_runtime = second.runtime().unwrap();

    first
        .execute(
            &first_runtime,
            "CREATE TABLE strict_values (
                id INTEGER PRIMARY KEY,
                count INT,
                ratio REAL,
                label TEXT,
                payload BLOB,
                anything ANY UNIQUE
            ) STRICT",
            (),
        )
        .unwrap();
    assert_eq!(first.push().unwrap(), PushOutcome::Drained);
    second.pull().unwrap();
    second.rebase(&second_runtime).unwrap();

    assert!(
        first
            .execute(
                &first_runtime,
                "INSERT INTO strict_values
                 VALUES (99, 'not-an-integer', 1, 'bad', x'00', 'bad')",
                (),
            )
            .is_err()
    );
    first
        .execute(
            &first_runtime,
            "INSERT INTO strict_values VALUES (1, '7', 2, 3, x'04', '000123')",
            (),
        )
        .unwrap();
    second
        .execute(
            &second_runtime,
            "INSERT INTO strict_values VALUES (2, 8, 2.5, '4', x'05', 123)",
            (),
        )
        .unwrap();

    assert_eq!(first.push().unwrap(), PushOutcome::Drained);
    assert_eq!(second.push().unwrap(), PushOutcome::Drained);
    first.pull().unwrap();
    second.pull().unwrap();
    first.rebase(&first_runtime).unwrap();
    second.rebase(&second_runtime).unwrap();

    let rows = |database: &Database<_>| {
        database.with_connection(|connection| {
            let mut statement = connection
                .prepare(
                    "SELECT id, typeof(count), count, typeof(ratio), ratio,
                            typeof(label), label, typeof(payload), hex(payload),
                            typeof(anything), quote(anything)
                     FROM strict_values ORDER BY id",
                )
                .unwrap();
            statement
                .query_map((), |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, i64>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, f64>(4)?,
                        row.get::<_, String>(5)?,
                        row.get::<_, String>(6)?,
                        row.get::<_, String>(7)?,
                        row.get::<_, String>(8)?,
                        row.get::<_, String>(9)?,
                        row.get::<_, String>(10)?,
                    ))
                })
                .unwrap()
                .collect::<rusqlite::Result<Vec<_>>>()
                .unwrap()
        })
    };
    let expected = vec![
        (
            1,
            "integer".into(),
            7,
            "real".into(),
            2.0,
            "text".into(),
            "3".into(),
            "blob".into(),
            "04".into(),
            "text".into(),
            "'000123'".into(),
        ),
        (
            2,
            "integer".into(),
            8,
            "real".into(),
            2.5,
            "text".into(),
            "4".into(),
            "blob".into(),
            "05".into(),
            "integer".into(),
            "123".into(),
        ),
    ];
    assert_eq!(rows(&first), expected);
    assert_eq!(rows(&second), expected);

    first
        .execute(
            &first_runtime,
            "INSERT INTO strict_values VALUES
                (3, 9, 3, 'winner', x'06', 'collision')",
            (),
        )
        .unwrap();
    second
        .execute(
            &second_runtime,
            "INSERT INTO strict_values VALUES
                (4, 10, 4, 'rejected', x'07', 'collision')",
            (),
        )
        .unwrap();
    assert_eq!(first.push().unwrap(), PushOutcome::Drained);
    let PushOutcome::Rejected(rejection) = second.push().unwrap() else {
        panic!("strict UNIQUE collision was not rejected")
    };
    assert!(matches!(
        rejection.error(),
        KernelError::RangeAssertFailed { .. }
    ));
    second.rollback(&rejection).unwrap();
    assert_eq!(second.push().unwrap(), PushOutcome::Drained);
    first.pull().unwrap();
    second.pull().unwrap();
    first.rebase(&first_runtime).unwrap();
    second.rebase(&second_runtime).unwrap();
    for database in [&first, &second] {
        assert_eq!(
            database.with_connection(|connection| {
                let mut statement = connection
                    .prepare("SELECT id FROM strict_values ORDER BY id")
                    .unwrap();
                statement
                    .query_map((), |row| row.get::<_, i64>(0))
                    .unwrap()
                    .collect::<rusqlite::Result<Vec<_>>>()
                    .unwrap()
            }),
            [1, 2, 3]
        );
    }

    assert!(
        first
            .execute(
                &first_runtime,
                "UPDATE strict_values SET count = 'bad' WHERE id = 1",
                (),
            )
            .is_err()
    );
    first
        .execute(
            &first_runtime,
            "UPDATE strict_values SET label = 5 WHERE id = 1",
            (),
        )
        .unwrap();
    assert_eq!(first.push().unwrap(), PushOutcome::Drained);
    second.pull().unwrap();
    second.rebase(&second_runtime).unwrap();
    second
        .execute(
            &second_runtime,
            "DELETE FROM strict_values WHERE id IN (2, 3)",
            (),
        )
        .unwrap();
    assert_eq!(second.push().unwrap(), PushOutcome::Drained);
    first.pull().unwrap();
    first.rebase(&first_runtime).unwrap();

    for database in [&first, &second] {
        assert_eq!(
            database.with_connection(|connection| {
                connection
                    .query_row(
                        "SELECT count, typeof(ratio), label, typeof(label),
                                quote(anything), typeof(anything)
                         FROM strict_values",
                        (),
                        |row| {
                            Ok((
                                row.get::<_, i64>(0)?,
                                row.get::<_, String>(1)?,
                                row.get::<_, String>(2)?,
                                row.get::<_, String>(3)?,
                                row.get::<_, String>(4)?,
                                row.get::<_, String>(5)?,
                            ))
                        },
                    )
                    .unwrap()
            }),
            (
                7,
                "real".into(),
                "5".into(),
                "text".into(),
                "'000123'".into(),
                "text".into(),
            )
        );
    }
}

#[test]
fn snapshot_update_bodies_overlap_and_disjoint_proposals_both_commit() {
    let directory = tempfile::tempdir().unwrap();
    let database = Database::open(directory.path().join("concurrent-disjoint.sqlite")).unwrap();
    let runtime = database.runtime().unwrap();
    database
        .execute(
            &runtime,
            "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT NOT NULL)",
            (),
        )
        .unwrap();

    let rendezvous = Arc::new(Barrier::new(3));
    let handles = [(1_i64, "one"), (2_i64, "two")]
        .into_iter()
        .map(|(id, body)| {
            let database = Arc::clone(&database);
            let rendezvous = Arc::clone(&rendezvous);
            std::thread::spawn(move || {
                let runtime = database.runtime().unwrap();
                database.update(&runtime, |update| {
                    rendezvous.wait();
                    update.execute("INSERT INTO notes VALUES (?1, ?2)", (id, body))
                })
            })
        })
        .collect::<Vec<_>>();
    rendezvous.wait();
    for handle in handles {
        assert_eq!(handle.join().unwrap().unwrap(), 1);
    }

    let rows = database.with_connection(|connection| {
        connection
            .prepare("SELECT id, body FROM notes ORDER BY id")
            .unwrap()
            .query_map((), |row| {
                Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
            })
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap()
    });
    assert_eq!(rows, [(1, "one".into()), (2, "two".into())]);
    let concurrent_commit_seq = database.with_connection(history::current).unwrap();
    assert!(
        (2..=3).contains(&concurrent_commit_seq.0),
        "the concurrent proposals may share one group or occupy adjacent groups"
    );
    assert_eq!(pending_ops(&database).len(), 3);
    let retained = database
        .with_connection(|connection| {
            history::history_after(connection, crate::commit::snapshot::CommitSeq(0))
        })
        .unwrap();
    assert_eq!(retained.first().unwrap().commit_seq.0, 2);
    assert_eq!(retained.last().unwrap().commit_seq, concurrent_commit_seq);

    database
        .update(&runtime, |update| {
            update.execute("INSERT INTO notes VALUES (3, 'three')", ())
        })
        .unwrap();
    let final_commit_seq = crate::commit::snapshot::CommitSeq(concurrent_commit_seq.0 + 1);
    assert_eq!(
        database
            .with_connection(|connection| {
                history::history_after(connection, crate::commit::snapshot::CommitSeq(0))
            })
            .unwrap()
            .into_iter()
            .map(|committed| committed.commit_seq)
            .collect::<Vec<_>>(),
        [final_commit_seq]
    );
}

#[test]
fn commit_group_shares_one_sequence_and_skips_only_its_conflicting_member() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("commit-group.sqlite");
    let database = Database::open(&path).unwrap();
    let backend = backend_for(&database, &path);
    let snapshot = backend.capture_snapshot(true).unwrap();
    let first = create_proposal(snapshot.logical, "notes");
    let conflict = create_proposal(snapshot.logical, "NOTES");
    let last = create_proposal(snapshot.logical, "tasks");
    let expected_mutations = [&first, &last]
        .into_iter()
        .flat_map(|proposal| proposal.to_homebase().unwrap().0)
        .collect::<Vec<_>>();
    let expected_writes = history::writes_from_mutations(&expected_mutations);

    let results = backend
        .commit_group(&[&first, &first, &conflict, &last])
        .unwrap();
    assert_eq!(results.len(), 4);
    let first_receipt = results[0].as_ref().unwrap();
    assert_eq!(first_receipt.disposition, CommitDisposition::Applied);
    assert_eq!(first_receipt.submitted, Some(DeviceSeq(1)));
    assert_eq!(
        results[1].as_ref().unwrap(),
        &CommitReceipt {
            commit_seq: first_receipt.commit_seq,
            disposition: CommitDisposition::AlreadyCommitted,
            submitted: first_receipt.submitted,
        }
    );
    assert!(matches!(results[2], Err(Error::CommitConflict(_))));
    let last_receipt = results[3].as_ref().unwrap();
    assert_eq!(last_receipt.disposition, CommitDisposition::Applied);
    assert_eq!(last_receipt.submitted, Some(DeviceSeq(2)));
    assert_eq!(first_receipt.commit_seq, last_receipt.commit_seq);
    assert_eq!(
        first_receipt.commit_seq,
        crate::commit::snapshot::CommitSeq(1)
    );

    assert!(table_exists(&database, "notes"));
    assert!(table_exists(&database, "tasks"));
    assert_eq!(pending_ops(&database).len(), 2);
    let committed = database
        .with_connection(|connection| {
            history::history_after(connection, crate::commit::snapshot::CommitSeq(0))
        })
        .unwrap();
    assert_eq!(committed.len(), 2);
    assert!(
        committed
            .iter()
            .all(|record| record.commit_seq == crate::commit::snapshot::CommitSeq(1))
    );
    assert_eq!(
        committed
            .into_iter()
            .flat_map(|record| record.writes)
            .collect::<BTreeSet<_>>(),
        expected_writes.into_iter().collect()
    );
    assert_eq!(
        database.commit_proposal(first.clone()).unwrap(),
        CommitReceipt {
            commit_seq: crate::commit::snapshot::CommitSeq(1),
            disposition: CommitDisposition::AlreadyCommitted,
            submitted: first_receipt.submitted,
        }
    );
}

#[test]
fn remote_apply_and_disjoint_local_transaction_share_one_commit_sequence() {
    let directory = tempfile::tempdir().unwrap();
    let server = server();
    let source = Database::open_with(
        directory.path().join("source.sqlite"),
        OpenOptions::new().server(router(Arc::clone(&server))),
    )
    .unwrap();
    assert!(server.create_space(source.database_id.space_id()));
    let replica_path = directory.path().join("replica.sqlite");
    let replica = Database::open_with(
        &replica_path,
        OpenOptions::new()
            .invitation(source.replica_invitation())
            .server(router(Arc::clone(&server))),
    )
    .unwrap();
    let source_runtime = source.runtime().unwrap();
    source
        .execute(
            &source_runtime,
            "CREATE TABLE notes (id INTEGER PRIMARY KEY)",
            (),
        )
        .unwrap();
    assert_eq!(source.push().unwrap(), PushOutcome::Drained);
    replica.pull().unwrap();
    assert_eq!(
        replica.with_connection(history::current).unwrap(),
        crate::commit::snapshot::CommitSeq(1)
    );
    assert_eq!(
        replica
            .with_connection(|connection| {
                history::history_after(connection, crate::commit::snapshot::CommitSeq(0))
            })
            .unwrap(),
        [history::CommitRecord {
            commit_seq: crate::commit::snapshot::CommitSeq(1),
            writes: Vec::new(),
        }]
    );

    let apply = pending_apply_proposal(&replica);
    let backend = backend_for(&replica, &replica_path);
    let snapshot = backend.capture_snapshot(true).unwrap();
    let local = create_proposal(snapshot.logical, "tasks");
    let results = backend.commit_group(&[&apply, &local]).unwrap();

    let apply_receipt = results[0].as_ref().unwrap();
    let local_receipt = results[1].as_ref().unwrap();
    assert_eq!(apply_receipt.commit_seq, local_receipt.commit_seq);
    assert_eq!(
        apply_receipt.commit_seq,
        crate::commit::snapshot::CommitSeq(2)
    );
    assert!(table_exists(&replica, "notes"));
    assert!(table_exists(&replica, "tasks"));
    assert_eq!(pending_ops(&replica).len(), 1);
}

#[test]
fn remote_apply_rejects_only_an_overlapping_local_group_member() {
    let directory = tempfile::tempdir().unwrap();
    let server = server();
    let source = Database::open_with(
        directory.path().join("source.sqlite"),
        OpenOptions::new().server(router(Arc::clone(&server))),
    )
    .unwrap();
    assert!(server.create_space(source.database_id.space_id()));
    let replica_path = directory.path().join("replica.sqlite");
    let replica = Database::open_with(
        &replica_path,
        OpenOptions::new()
            .invitation(source.replica_invitation())
            .server(router(Arc::clone(&server))),
    )
    .unwrap();
    let source_runtime = source.runtime().unwrap();
    source
        .execute(
            &source_runtime,
            "CREATE TABLE notes (id INTEGER PRIMARY KEY)",
            (),
        )
        .unwrap();
    assert_eq!(source.push().unwrap(), PushOutcome::Drained);
    replica.pull().unwrap();

    let apply = pending_apply_proposal(&replica);
    let backend = backend_for(&replica, &replica_path);
    let snapshot = backend.capture_snapshot(true).unwrap();
    let collision = create_proposal(snapshot.logical, "NOTES");
    let disjoint = create_proposal(snapshot.logical, "tasks");
    let results = backend
        .commit_group(&[&apply, &collision, &disjoint])
        .unwrap();

    let shared = results[0].as_ref().unwrap().commit_seq;
    assert!(matches!(results[1], Err(Error::CommitConflict(_))));
    assert_eq!(results[2].as_ref().unwrap().commit_seq, shared);
    assert!(table_exists(&replica, "notes"));
    assert!(table_exists(&replica, "tasks"));
    assert_eq!(pending_ops(&replica).len(), 1);
}

#[test]
fn stale_metadata_proposal_does_not_poison_a_later_group_member() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("stale-metadata.sqlite");
    let database = Database::open(&path).unwrap();
    let backend = backend_for(&database, &path);
    let stale = CommitProposal::append_admissions(
        AdmitCursors {
            head: AdmissionSeq(1),
            neck: AdmissionSeq(1),
            tail: AdmissionSeq(2),
        },
        homebase_core::messages::PullResponse {
            after: AdmissionSeq(1),
            through: AdmissionSeq(1),
            batches: Vec::new(),
        },
    )
    .unwrap();
    let snapshot = backend.capture_snapshot(true).unwrap();
    let local = create_proposal(snapshot.logical, "notes");

    let results = backend.commit_group(&[&stale, &local]).unwrap();
    assert!(matches!(results[0], Err(Error::CommitConflict(_))));
    assert_eq!(
        results[1].as_ref().unwrap().commit_seq,
        crate::commit::snapshot::CommitSeq(1)
    );
    assert!(table_exists(&database, "notes"));
}

#[test]
fn branch_logical_coordinates_come_from_the_pinned_sqlite_snapshot() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("atomic-snapshot.sqlite");
    let database = Database::open(&path).unwrap();
    let backend = backend_for(&database, &path);
    let space = database.database_id.space_id();
    let device = database.client.device();

    let snapshot = backend
        .capture_snapshot_inner(true, || {
            database
                .owner
                .with_savepoint("__multilite__snapshot_race", |connection| {
                    let mut proposal_id = [5; 16];
                    proposal_id[6] = (proposal_id[6] & 0x0f) | 0x40;
                    proposal_id[8] = (proposal_id[8] & 0x3f) | 0x80;
                    backend.commit_history.record_group(
                        connection,
                        vec![history::PreparedRecord {
                            proposal_id,
                            proposal_hash: [5; 32],
                            submitted: None,
                            writes: vec![history::WriteRegion::Point(
                                Key::from_bytes([b"test".as_slice(), b"write".as_slice()]).unwrap(),
                            )],
                        }],
                    )?;
                    let metadata =
                        OrderedMetaStore::new(SqliteOrderedStore::new(database.owner.clone()));
                    block_on(async {
                        let reserved = metadata
                            .reserve_commit(space, 0, Vec::new(), SubmitMode::Unchecked)
                            .await?;
                        metadata.commit(space, reserved, Vec::new()).await?;
                        metadata
                            .append_admits(
                                space,
                                &homebase_core::messages::PullResponse {
                                    after: AdmissionSeq(0),
                                    through: AdmissionSeq(1),
                                    batches: vec![homebase_core::messages::AdmittedBatch {
                                        admission_seq: AdmissionSeq(1),
                                        device,
                                        device_seq: DeviceSeq(1),
                                        checksum: DeviceChecksum::EMPTY,
                                        entries: Vec::new(),
                                    }],
                                },
                            )
                            .await?;
                        metadata.mark_admits_applied(space, AdmissionSeq(2)).await?;
                        Ok::<_, Error>(())
                    })
                })
        })
        .unwrap();

    assert_eq!(
        snapshot.logical.commit_seq,
        crate::commit::snapshot::CommitSeq(0)
    );
    assert_eq!(snapshot.logical.submit_cursors, OplogCursors::default());
    assert_eq!(snapshot.logical.authority_applied_through, AdmissionSeq(0));

    let current = backend.capture_snapshot(false).unwrap();
    assert_eq!(
        current.logical.commit_seq,
        crate::commit::snapshot::CommitSeq(1)
    );
    assert_eq!(current.logical.submit_cursors.tail, DeviceSeq(2));
    assert_eq!(current.logical.authority_applied_through, AdmissionSeq(1));
}

#[test]
fn typed_committer_accepts_concurrent_owned_proposals() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("queued-group.sqlite");
    let database = Database::open(&path).unwrap();
    let backend = backend_for(&database, &path);
    let snapshot = backend.capture_snapshot(true).unwrap();
    let proposals = [
        create_proposal(snapshot.logical, "notes"),
        create_proposal(snapshot.logical, "tasks"),
    ];
    let handles = proposals
        .into_iter()
        .map(|proposal| {
            let database = Arc::clone(&database);
            std::thread::spawn(move || database.commit_proposal(proposal))
        })
        .collect::<Vec<_>>();

    let receipts = handles
        .into_iter()
        .map(|handle| handle.join().unwrap().unwrap())
        .collect::<Vec<_>>();
    assert!(receipts.iter().all(|receipt| receipt.commit_seq.0 >= 1));
    assert!(receipts.iter().all(|receipt| receipt.commit_seq.0 <= 2));
    assert!(table_exists(&database, "notes"));
    assert!(table_exists(&database, "tasks"));
    assert_eq!(pending_ops(&database).len(), 2);
}

#[test]
fn commit_group_finalization_failure_rolls_back_every_successful_member() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("commit-group-failure.sqlite");
    let database = Database::open(&path).unwrap();
    let backend = backend_for(&database, &path);
    let snapshot = backend.capture_snapshot(true).unwrap();
    let first = create_proposal(snapshot.logical, "notes");
    let second = create_proposal(snapshot.logical, "tasks");
    let state_before = client_state(&database);
    database.with_connection(|connection| {
        connection
            .execute_batch(
                "CREATE TRIGGER fail_group_receipt
                 BEFORE INSERT ON __multilite__commits
                 BEGIN SELECT RAISE(ABORT, 'injected group receipt failure'); END",
            )
            .unwrap();
    });

    assert!(matches!(
        backend.commit_group(&[&first, &second]),
        Err(Error::Sqlite(_))
    ));
    assert!(!table_exists(&database, "notes"));
    assert!(!table_exists(&database, "tasks"));
    assert!(pending_ops(&database).is_empty());
    assert_eq!(
        database.with_connection(history::current).unwrap(),
        crate::commit::snapshot::CommitSeq(0)
    );
    assert_eq!(client_state(&database), state_before);
}

#[test]
fn snapshot_insert_commits_across_unrelated_owned_ddl() {
    let directory = tempfile::tempdir().unwrap();
    let database = Database::open(directory.path().join("concurrent-ddl.sqlite")).unwrap();
    let runtime = database.runtime().unwrap();
    database
        .execute(
            &runtime,
            "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT NOT NULL)",
            (),
        )
        .unwrap();

    let branch_ready = Arc::new(Barrier::new(2));
    let ddl_finished = Arc::new(Barrier::new(2));
    let worker = {
        let database = Arc::clone(&database);
        let branch_ready = Arc::clone(&branch_ready);
        let ddl_finished = Arc::clone(&ddl_finished);
        std::thread::spawn(move || {
            let runtime = database.runtime().unwrap();
            database.update(&runtime, |update| {
                branch_ready.wait();
                ddl_finished.wait();
                update.execute("INSERT INTO notes VALUES (1, 'branch')", ())
            })
        })
    };

    branch_ready.wait();
    database
        .execute(
            &runtime,
            "CREATE TABLE tasks (id INTEGER PRIMARY KEY, done INTEGER NOT NULL)",
            (),
        )
        .unwrap();
    ddl_finished.wait();
    assert_eq!(worker.join().unwrap().unwrap(), 1);

    assert!(table_exists(&database, "tasks"));
    assert_eq!(
        database.with_connection(|connection| {
            connection
                .query_row("SELECT body FROM notes WHERE id = 1", (), |row| {
                    row.get::<_, String>(0)
                })
                .unwrap()
        }),
        "branch"
    );
    assert_eq!(
        database.with_connection(history::current).unwrap(),
        crate::commit::snapshot::CommitSeq(3)
    );
}

#[test]
fn concurrent_snapshot_updates_reject_one_primary_key_collision() {
    let directory = tempfile::tempdir().unwrap();
    let database = Database::open(directory.path().join("concurrent-collision.sqlite")).unwrap();
    let runtime = database.runtime().unwrap();
    database
        .execute(
            &runtime,
            "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT NOT NULL)",
            (),
        )
        .unwrap();

    let rendezvous = Arc::new(Barrier::new(3));
    let handles = ["first", "second"]
        .into_iter()
        .map(|body| {
            let database = Arc::clone(&database);
            let rendezvous = Arc::clone(&rendezvous);
            std::thread::spawn(move || {
                let runtime = database.runtime().unwrap();
                database.update(&runtime, |update| {
                    rendezvous.wait();
                    update.execute("INSERT INTO notes VALUES (7, ?1)", [body])
                })
            })
        })
        .collect::<Vec<_>>();
    rendezvous.wait();
    let results = handles
        .into_iter()
        .map(|handle| handle.join().unwrap())
        .collect::<Vec<_>>();

    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
    assert_eq!(
        results
            .iter()
            .filter(|result| matches!(result, Err(Error::CommitConflict(_))))
            .count(),
        1
    );
    assert_eq!(
        database.with_connection(|connection| {
            connection
                .query_row("SELECT count(*) FROM notes", (), |row| row.get::<_, i64>(0))
                .unwrap()
        }),
        1
    );
    assert_eq!(
        database.with_connection(history::current).unwrap(),
        crate::commit::snapshot::CommitSeq(2)
    );
    assert_eq!(pending_ops(&database).len(), 2);
}

#[test]
fn concurrent_snapshot_updates_reject_one_overlapping_unique_collision() {
    let directory = tempfile::tempdir().unwrap();
    let database =
        Database::open(directory.path().join("concurrent-unique-collision.sqlite")).unwrap();
    let runtime = database.runtime().unwrap();
    database
        .execute(
            &runtime,
            "CREATE TABLE profiles (
                id INTEGER PRIMARY KEY,
                tenant TEXT,
                email TEXT UNIQUE,
                UNIQUE (tenant, email)
            )",
            (),
        )
        .unwrap();

    let rendezvous = Arc::new(Barrier::new(3));
    let handles = [(1_i64, "acme"), (2_i64, "other")]
        .into_iter()
        .map(|(id, tenant)| {
            let database = Arc::clone(&database);
            let rendezvous = Arc::clone(&rendezvous);
            std::thread::spawn(move || {
                let runtime = database.runtime().unwrap();
                database.update(&runtime, |update| {
                    rendezvous.wait();
                    update.execute(
                        "INSERT INTO profiles VALUES (?1, ?2, 'shared@example.com')",
                        (id, tenant),
                    )
                })
            })
        })
        .collect::<Vec<_>>();
    rendezvous.wait();
    let results = handles
        .into_iter()
        .map(|handle| handle.join().unwrap())
        .collect::<Vec<_>>();

    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
    assert_eq!(
        results
            .iter()
            .filter(|result| matches!(result, Err(Error::CommitConflict(_))))
            .count(),
        1
    );
    assert_eq!(
        database.with_connection(|connection| {
            connection
                .query_row("SELECT count(*) FROM profiles", (), |row| {
                    row.get::<_, i64>(0)
                })
                .unwrap()
        }),
        1
    );
    assert_eq!(
        database.with_connection(history::current).unwrap(),
        crate::commit::snapshot::CommitSeq(2)
    );
    assert_eq!(pending_ops(&database).len(), 2);
}

#[test]
fn failed_general_bootstrap_rolls_back_all_metadata() {
    let owner = ConnectionOwner::open_in_memory().unwrap();
    let metadata_inserts = Arc::new(AtomicUsize::new(0));
    let counted = Arc::clone(&metadata_inserts);
    owner.with_connection(|connection| {
        connection
            .authorizer(Some(move |context: AuthContext<'_>| match context.action {
                AuthAction::Insert {
                    table_name: "__multilite__meta",
                } if counted.fetch_add(1, Ordering::Relaxed) == 1 => Authorization::Deny,
                _ => Authorization::Allow,
            }))
            .unwrap();
    });

    let error = match open_on(
        owner.clone(),
        PathBuf::from(":memory:"),
        OpenOptions::new().server(offline_router()),
    ) {
        Ok(_) => panic!("bootstrap unexpectedly succeeded"),
        Err(error) => error,
    };
    assert!(matches!(
        error,
        Error::Client(homebase_client::ClientError::Store(_))
    ));
    assert_eq!(metadata_inserts.load(Ordering::Relaxed), 2);
    owner.with_connection(|connection| {
        let tables: i64 = connection
            .query_row(
                "SELECT count(*) FROM sqlite_schema
                     WHERE type = 'table' AND name NOT LIKE 'sqlite_%'",
                (),
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(tables, 0);
    });
}
