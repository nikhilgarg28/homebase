//! Seeded end-to-end integrity checks over two synchronized replicas.

use std::collections::BTreeMap;
use std::sync::Arc;

use homebase_core::key::Key;
use homebase_core::messages::RangeCut;
use homebase_core::range::Range;
use homebase_core::tag::{AdmissionSeq, Mutation};

use super::*;
use crate::logical::codes;
use crate::logical::guard::TargetFamily;

const SEEDS: &[u64] = &[0x5eed_0001, 0x5eed_0002, 0x5eed_0003];
const ROUNDS: usize = 12;

#[derive(Clone, Copy, Debug)]
struct DeterministicRng(u64);

impl DeterministicRng {
    fn new(seed: u64) -> Self {
        Self(seed)
    }

    fn next(&mut self) -> u64 {
        let mut value = self.0;
        value ^= value << 13;
        value ^= value >> 7;
        value ^= value << 17;
        self.0 = value;
        value
    }

    fn choose(&mut self, choices: usize) -> usize {
        usize::try_from(self.next() % choices as u64).unwrap()
    }

    fn coin(&mut self) -> bool {
        self.next() & 1 == 1
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct MaterializedState {
    schema: Vec<(String, String, String, String)>,
    parents: Vec<(i64, String, String)>,
    children: Vec<(i64, String, String)>,
    materialized_cells: BTreeMap<Key, Vec<u8>>,
}

#[derive(Clone, Debug, Default)]
struct Coverage {
    scenarios: [bool; 10],
    push_orders: [bool; 2],
    reopened_before_push: bool,
    reopened_before_repair: bool,
    insert_or_ignore: bool,
    update_or_ignore: bool,
    replacement: bool,
    upsert_do_nothing: bool,
    upsert_do_update: bool,
}

impl Coverage {
    fn merge(&mut self, other: Self) {
        for (covered, observed) in self.scenarios.iter_mut().zip(other.scenarios) {
            *covered |= observed;
        }
        for (covered, observed) in self.push_orders.iter_mut().zip(other.push_orders) {
            *covered |= observed;
        }
        self.reopened_before_push |= other.reopened_before_push;
        self.reopened_before_repair |= other.reopened_before_repair;
        self.insert_or_ignore |= other.insert_or_ignore;
        self.update_or_ignore |= other.update_or_ignore;
        self.replacement |= other.replacement;
        self.upsert_do_nothing |= other.upsert_do_nothing;
        self.upsert_do_update |= other.upsert_do_update;
    }
}

#[test]
fn seeded_two_replica_workloads_preserve_logical_and_sqlite_integrity() {
    let mut coverage = Coverage::default();
    for isolation in [IsolationLevel::Snapshot, IsolationLevel::Serializable] {
        for &seed in SEEDS {
            coverage.merge(run_workload(isolation, seed));
        }
    }
    assert!(
        coverage.scenarios.into_iter().all(|covered| covered),
        "seed set missed a scenario: {coverage:?}"
    );
    assert!(
        coverage.push_orders.into_iter().all(|covered| covered),
        "seed set missed a push order: {coverage:?}"
    );
    assert!(coverage.reopened_before_push);
    assert!(coverage.reopened_before_repair);
    assert!(coverage.insert_or_ignore);
    assert!(coverage.update_or_ignore);
    assert!(coverage.replacement);
    assert!(coverage.upsert_do_nothing);
    assert!(coverage.upsert_do_update);
}

fn run_workload(isolation: IsolationLevel, seed: u64) -> Coverage {
    let directory = tempfile::tempdir().unwrap();
    let authority = server();
    let first_path = directory.path().join("first.sqlite");
    let second_path = directory.path().join("second.sqlite");
    let mut first = Database::open_with(
        &first_path,
        OpenOptions::new()
            .isolation_level(isolation)
            .server(router(Arc::clone(&authority))),
    )
    .unwrap();
    assert!(authority.create_space(first.database_id.space_id()));
    let invitation = first.replica_invitation();
    let mut second = Database::open_with(
        &second_path,
        OpenOptions::new()
            .isolation_level(isolation)
            .invitation(invitation)
            .server(router(Arc::clone(&authority))),
    )
    .unwrap();
    let mut first_runtime = first.runtime().unwrap();
    let mut second_runtime = second.runtime().unwrap();

    first
        .update(&first_runtime, |transaction| {
            transaction.execute(
                "CREATE TABLE parents (
                    id INTEGER PRIMARY KEY,
                    code TEXT NOT NULL UNIQUE,
                    body TEXT NOT NULL
                )",
                (),
            )?;
            transaction.execute(
                "CREATE TABLE children (
                    id INTEGER PRIMARY KEY,
                    parent_code TEXT REFERENCES parents(code),
                    body TEXT NOT NULL
                )",
                (),
            )?;
            transaction.execute(
                "CREATE INDEX children_by_parent
                 ON children (parent_code, body DESC)
                 WHERE parent_code IS NOT NULL",
                (),
            )?;
            Ok(())
        })
        .unwrap();
    assert_eq!(first.push().unwrap(), PushOutcome::Drained);
    synchronize(&first, &first_runtime, &second, &second_runtime);

    let mut random = DeterministicRng::new(seed);
    let mut coverage = Coverage::default();
    for round in 0..ROUNDS {
        let base = i64::try_from(round).unwrap() * 1_000;
        let parent_a = base + 1;
        let parent_b = base + 2;
        let child = base + 10;
        let added_child = base + 11;
        let code_a = format!("p{round}-a");
        let code_b = format!("p{round}-b");

        first
            .update(&first_runtime, |transaction| {
                transaction.execute(
                    "INSERT INTO parents (id, code, body) VALUES
                        (?1, ?2, 'a'), (?3, ?4, 'b')",
                    rusqlite::params![parent_a, code_a, parent_b, code_b],
                )?;
                transaction.execute(
                    "INSERT INTO children (id, parent_code, body)
                     VALUES (?1, ?2, 'base')",
                    rusqlite::params![child, code_a],
                )?;
                Ok(())
            })
            .unwrap();
        assert_eq!(first.push().unwrap(), PushOutcome::Drained);
        synchronize(&first, &first_runtime, &second, &second_runtime);

        let scenario = random.choose(10);
        coverage.scenarios[scenario] = true;
        let expects_conflict = match scenario {
            0 => {
                first
                    .execute(
                        &first_runtime,
                        "DELETE FROM parents WHERE id = ?1",
                        [parent_b],
                    )
                    .unwrap();
                second
                    .execute(
                        &second_runtime,
                        "INSERT INTO children VALUES (?1, ?2, 'late child')",
                        rusqlite::params![added_child, code_b],
                    )
                    .unwrap();
                true
            }
            1 => {
                first
                    .execute(
                        &first_runtime,
                        "UPDATE children SET parent_code = ?1 WHERE id = ?2",
                        rusqlite::params![code_b, child],
                    )
                    .unwrap();
                second
                    .execute(
                        &second_runtime,
                        "DELETE FROM parents WHERE id = ?1",
                        [parent_b],
                    )
                    .unwrap();
                true
            }
            2 => {
                coverage.update_or_ignore = true;
                coverage.upsert_do_update = true;
                first
                    .execute(
                        &first_runtime,
                        "UPDATE OR IGNORE children SET body = 'first' WHERE id = ?1",
                        [child],
                    )
                    .unwrap();
                second
                    .execute(
                        &second_runtime,
                        "INSERT INTO children VALUES (?1, ?2, 'second')
                         ON CONFLICT(id) DO UPDATE SET body = excluded.body",
                        rusqlite::params![child, code_a],
                    )
                    .unwrap();
                true
            }
            3 => {
                coverage.replacement = true;
                first
                    .execute(
                        &first_runtime,
                        "DELETE FROM children WHERE id = ?1",
                        [child],
                    )
                    .unwrap();
                second
                    .execute(
                        &second_runtime,
                        "INSERT OR REPLACE INTO children
                         VALUES (?1, ?2, 'replacement')",
                        rusqlite::params![child, code_a],
                    )
                    .unwrap();
                true
            }
            4 => {
                first
                    .execute(
                        &first_runtime,
                        "UPDATE parents SET body = 'updated' WHERE id = ?1",
                        [parent_a],
                    )
                    .unwrap();
                second
                    .execute(
                        &second_runtime,
                        "INSERT INTO children VALUES (?1, ?2, 'independent')",
                        rusqlite::params![added_child, code_a],
                    )
                    .unwrap();
                false
            }
            5 => {
                first
                    .execute(
                        &first_runtime,
                        "UPDATE parents SET code = ?1 WHERE id = ?2",
                        rusqlite::params![format!("{code_b}-moved"), parent_b],
                    )
                    .unwrap();
                second
                    .execute(
                        &second_runtime,
                        "INSERT INTO children VALUES (?1, ?2, 'old target')",
                        rusqlite::params![added_child, code_b],
                    )
                    .unwrap();
                true
            }
            6 => {
                first
                    .execute(
                        &first_runtime,
                        "UPDATE children SET id = ?1 WHERE id = ?2",
                        [base + 110, child],
                    )
                    .unwrap();
                second
                    .execute(
                        &second_runtime,
                        "UPDATE children SET id = ?1 WHERE id = ?2",
                        [base + 210, child],
                    )
                    .unwrap();
                true
            }
            7 => {
                coverage.insert_or_ignore = true;
                first
                    .update(&first_runtime, |transaction| {
                        transaction.execute(
                            "UPDATE parents SET body = 'first mixed' WHERE id = ?1",
                            [parent_a],
                        )?;
                        transaction.execute(
                            "UPDATE children SET body = 'first mixed' WHERE id = ?1",
                            [child],
                        )?;
                        Ok(())
                    })
                    .unwrap();
                second
                    .update(&second_runtime, |transaction| {
                        transaction.execute(
                            "INSERT OR IGNORE INTO parents (id, code, body)
                             VALUES (?1, ?2, 'new parent')",
                            rusqlite::params![base + 3, format!("p{round}-c")],
                        )?;
                        transaction.execute(
                            "INSERT OR IGNORE INTO children VALUES (?1, ?2, 'new child')",
                            rusqlite::params![base + 12, format!("p{round}-c")],
                        )?;
                        Ok(())
                    })
                    .unwrap();
                false
            }
            8 => {
                coverage.insert_or_ignore = true;
                coverage.upsert_do_nothing = true;
                let shared = format!("p{round}-shared");
                first
                    .execute(
                        &first_runtime,
                        "INSERT OR IGNORE INTO parents (id, code, body)
                         VALUES (?1, ?2, 'first owner')",
                        rusqlite::params![base + 3, shared],
                    )
                    .unwrap();
                second
                    .execute(
                        &second_runtime,
                        "INSERT INTO parents (id, code, body)
                         VALUES (?1, ?2, 'second owner')
                         ON CONFLICT(code) DO NOTHING",
                        rusqlite::params![base + 4, shared],
                    )
                    .unwrap();
                true
            }
            9 => {
                let column = format!("tag_{round}");
                first
                    .update(&first_runtime, |transaction| {
                        transaction.execute(
                            &format!("ALTER TABLE parents ADD COLUMN {column} TEXT DEFAULT 'seed'"),
                            (),
                        )?;
                        transaction.execute(
                            &format!("UPDATE parents SET {column} = 'local' WHERE id = ?1"),
                            [parent_a],
                        )?;
                        Ok(())
                    })
                    .unwrap();
                second
                    .execute(
                        &second_runtime,
                        "UPDATE parents SET body = 'stale writer' WHERE id = ?1",
                        [parent_b],
                    )
                    .unwrap();
                false
            }
            _ => unreachable!(),
        };

        if random.coin() {
            coverage.reopened_before_push = true;
            drop(first_runtime);
            drop(first);
            first = Database::open_with(
                &first_path,
                OpenOptions::new()
                    .isolation_level(isolation)
                    .server(router(Arc::clone(&authority))),
            )
            .unwrap();
            first_runtime = first.runtime().unwrap();
        } else if random.coin() {
            coverage.reopened_before_push = true;
            drop(second_runtime);
            drop(second);
            second = Database::open_with(
                &second_path,
                OpenOptions::new()
                    .isolation_level(isolation)
                    .server(router(Arc::clone(&authority))),
            )
            .unwrap();
            second_runtime = second.runtime().unwrap();
        }

        let first_pushes_first = random.coin();
        coverage.push_orders[usize::from(first_pushes_first)] = true;
        let (first_outcome, second_outcome) = if first_pushes_first {
            (first.push().unwrap(), second.push().unwrap())
        } else {
            let second_outcome = second.push().unwrap();
            (first.push().unwrap(), second_outcome)
        };
        let first_rejection = rejection(first_outcome);
        let second_rejection = rejection(second_outcome);
        let rejection_count =
            usize::from(first_rejection.is_some()) + usize::from(second_rejection.is_some());
        let context = format!(
            "isolation={isolation:?} seed={seed:#x} round={round} scenario={scenario} first_pushes_first={first_pushes_first}"
        );
        if expects_conflict {
            assert_eq!(
                rejection_count, 1,
                "mandatory conflict was not isolated to one loser: {context}"
            );
        } else if isolation == IsolationLevel::Snapshot {
            assert_eq!(
                rejection_count, 0,
                "snapshot-compatible operations did not commute: {context}"
            );
        } else {
            assert!(
                rejection_count <= 1,
                "coarse serializable tracing rejected both writers: {context}"
            );
        }

        if let Some(rejection) = first_rejection {
            if random.coin() {
                coverage.reopened_before_repair = true;
                drop(first_runtime);
                drop(first);
                first = Database::open_with(
                    &first_path,
                    OpenOptions::new()
                        .isolation_level(isolation)
                        .server(router(Arc::clone(&authority))),
                )
                .unwrap();
                first_runtime = first.runtime().unwrap();
            }
            first.rollback(&rejection).unwrap();
            assert_eq!(first.push().unwrap(), PushOutcome::Drained);
        }
        if let Some(rejection) = second_rejection {
            if random.coin() {
                coverage.reopened_before_repair = true;
                drop(second_runtime);
                drop(second);
                second = Database::open_with(
                    &second_path,
                    OpenOptions::new()
                        .isolation_level(isolation)
                        .server(router(Arc::clone(&authority))),
                )
                .unwrap();
                second_runtime = second.runtime().unwrap();
            }
            second.rollback(&rejection).unwrap();
            assert_eq!(second.push().unwrap(), PushOutcome::Drained);
        }

        synchronize(&first, &first_runtime, &second, &second_runtime);
        let first_state = audit(&first);
        let second_state = audit(&second);
        assert_eq!(first_state, second_state, "replicas diverged: {context}");
        crate::logical::row::validate_materialized_cells(
            &first_state.materialized_cells,
            &authority_materialized_cells(&first),
        )
        .unwrap_or_else(|error| {
            panic!("authority cells diverged from SQLite rows: {context}: {error}")
        });
    }
    coverage
}

fn synchronize<H1, H2>(
    first: &Arc<Database<H1>>,
    first_runtime: &DatabaseRuntime,
    second: &Arc<Database<H2>>,
    second_runtime: &DatabaseRuntime,
) where
    H1: ServerHandle + Send + Sync + 'static,
    H2: ServerHandle + Send + Sync + 'static,
{
    first.pull().unwrap();
    second.pull().unwrap();
    first.rebase(first_runtime).unwrap();
    second.rebase(second_runtime).unwrap();
}

fn rejection(outcome: PushOutcome) -> Option<PushRejection> {
    match outcome {
        PushOutcome::Drained => None,
        PushOutcome::Rejected(rejection) => Some(rejection),
    }
}

fn audit<H>(database: &Database<H>) -> MaterializedState
where
    H: ServerHandle + Send + Sync + 'static,
{
    database.with_connection(|connection| {
        catalog::validate(connection).unwrap();
        pending::validate(connection).unwrap();
        assert!(
            pending::load(connection).unwrap().is_empty(),
            "synchronized database retained pending operations"
        );
        let integrity = connection
            .query_row("PRAGMA integrity_check", (), |row| row.get::<_, String>(0))
            .unwrap();
        assert_eq!(integrity, "ok");
        let violations = connection
            .prepare("PRAGMA foreign_key_check")
            .unwrap()
            .query_map((), |_| Ok(()))
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap();
        assert!(
            violations.is_empty(),
            "foreign-key violations: {violations:?}"
        );

        let schema = connection
            .prepare(
                "SELECT type, name, tbl_name, sql
                 FROM sqlite_schema
                 WHERE name NOT LIKE '__multilite__%'
                   AND name NOT LIKE 'sqlite_autoindex_%'
                 ORDER BY type, name",
            )
            .unwrap()
            .query_map((), |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                ))
            })
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap();
        let parents = connection
            .prepare("SELECT id, code, body FROM parents ORDER BY id")
            .unwrap()
            .query_map((), |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap();
        let children = connection
            .prepare("SELECT id, parent_code, body FROM children ORDER BY id")
            .unwrap()
            .query_map((), |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap();
        let materialized_cells =
            crate::logical::row::expected_materialized_cells(connection).unwrap();
        MaterializedState {
            schema,
            parents,
            children,
            materialized_cells,
        }
    })
}

fn authority_materialized_cells<H>(database: &Database<H>) -> BTreeMap<Key, Vec<u8>>
where
    H: ServerHandle + Send + Sync + 'static,
{
    let prefix = Key::from_bytes([codes::ROOT, codes::TABLES]).unwrap();
    let fetched = block_on(async {
        database
            .client
            .space(database.database_id.space_id())
            .await
            .unwrap()
            .fetch(Range::Prefix(prefix), AdmissionSeq(0))
            .await
            .unwrap()
    });
    let entries = match fetched.cut {
        RangeCut::Delta(entries) | RangeCut::Snapshot(entries) => entries,
    };
    let mut state = BTreeMap::new();
    for entry in entries {
        match entry.device_entry.mutation {
            Mutation::Set { key, value } => {
                state.insert(key, value);
            }
            Mutation::Delete { key } => {
                state.remove(&key);
            }
            Mutation::DeleteRange { range } => {
                state.retain(|key, _| !range.covers_key(key));
            }
        }
    }
    state.retain(|key, _| {
        matches!(
            TargetFamily::classify(key),
            Some(TargetFamily::Row | TargetFamily::UniqueOwner | TargetFamily::ForeignReference)
        )
    });
    state
}
