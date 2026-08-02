use std::collections::BTreeSet;
use std::sync::Arc;

use homebase_client::ServerHandle;
use homebase_core::messages::KernelError;
use homebase_core::space::SpaceId;
use multilite::{
    IsolationLevel, MultiliteConnection, OpenOptions, PushOutcome, SyncPolicy, ValueRef,
};
use rusqlite::Connection;

mod common;

use common::{router, server};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Disposition {
    BothAdmit,
    SecondRejects,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ExpectedPair {
    left_then_right: Disposition,
    right_then_left: Disposition,
}

impl ExpectedPair {
    const COMMUTE: Self = Self {
        left_then_right: Disposition::BothAdmit,
        right_then_left: Disposition::BothAdmit,
    };

    const CONFLICT: Self = Self {
        left_then_right: Disposition::SecondRejects,
        right_then_left: Disposition::SecondRejects,
    };

    const fn directional(left_then_right: Disposition, right_then_left: Disposition) -> Self {
        Self {
            left_then_right,
            right_then_left,
        }
    }

    const fn for_order(self, order: AdmissionOrder) -> Disposition {
        match order {
            AdmissionOrder::LeftThenRight => self.left_then_right,
            AdmissionOrder::RightThenLeft => self.right_then_left,
        }
    }

    const fn commutes(self) -> bool {
        matches!(self.left_then_right, Disposition::BothAdmit)
            && matches!(self.right_then_left, Disposition::BothAdmit)
    }
}

#[derive(Clone, Copy, Debug)]
struct RegisteredOperation {
    shape: SqlShape,
    sql: &'static str,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum SqlShape {
    CreateTable,
    DropTable,
    Insert,
    Upsert,
    Replace,
    Delete,
    Update,
    CreateIndex,
    DropIndex,
    RenameTable,
    RenameColumn,
    AddColumn,
    DropColumn,
}

const SQL_SHAPES: [SqlShape; 13] = [
    SqlShape::CreateTable,
    SqlShape::DropTable,
    SqlShape::Insert,
    SqlShape::Upsert,
    SqlShape::Replace,
    SqlShape::Delete,
    SqlShape::Update,
    SqlShape::CreateIndex,
    SqlShape::DropIndex,
    SqlShape::RenameTable,
    SqlShape::RenameColumn,
    SqlShape::AddColumn,
    SqlShape::DropColumn,
];

macro_rules! operation {
    ($shape:ident, $sql:literal) => {
        RegisteredOperation {
            shape: SqlShape::$shape,
            sql: $sql,
        }
    };
}

#[derive(Clone, Copy, Debug)]
struct PairCase {
    name: &'static str,
    relationship: &'static str,
    left: RegisteredOperation,
    right: RegisteredOperation,
    snapshot: ExpectedPair,
    serializable: ExpectedPair,
}

impl PairCase {
    const fn expected(self, isolation: IsolationLevel) -> ExpectedPair {
        match isolation {
            IsolationLevel::Snapshot => self.snapshot,
            IsolationLevel::Serializable => self.serializable,
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum AdmissionOrder {
    LeftThenRight,
    RightThenLeft,
}

const PAIR_CASES: &[PairCase] = &[
    PairCase {
        name: "insert_disjoint_rows",
        relationship: "different primary and unique keys",
        left: operation!(
            Insert,
            "INSERT INTO notes VALUES (3, 'three', 'left', 'd3')"
        ),
        right: operation!(
            Insert,
            "INSERT INTO notes VALUES (4, 'four', 'right', 'd4')"
        ),
        snapshot: ExpectedPair::COMMUTE,
        serializable: ExpectedPair::COMMUTE,
    },
    PairCase {
        name: "insert_same_primary_key",
        relationship: "same row identity",
        left: operation!(
            Insert,
            "INSERT INTO notes VALUES (3, 'three-left', 'left', 'd3')"
        ),
        right: operation!(
            Insert,
            "INSERT INTO notes VALUES (3, 'three-right', 'right', 'd4')"
        ),
        snapshot: ExpectedPair::CONFLICT,
        serializable: ExpectedPair::CONFLICT,
    },
    PairCase {
        name: "insert_same_unique_key",
        relationship: "different rows claiming one unique value",
        left: operation!(
            Insert,
            "INSERT INTO notes VALUES (3, 'shared', 'left', 'd3')"
        ),
        right: operation!(
            Insert,
            "INSERT INTO notes VALUES (4, 'shared', 'right', 'd4')"
        ),
        snapshot: ExpectedPair::CONFLICT,
        serializable: ExpectedPair::CONFLICT,
    },
    PairCase {
        name: "insert_or_ignore_disjoint_rows",
        relationship: "conflict-mode insert with disjoint surviving effects",
        left: operation!(
            Insert,
            "INSERT OR IGNORE INTO notes VALUES (3, 'three', 'left', 'd3')"
        ),
        right: operation!(
            Insert,
            "INSERT OR IGNORE INTO notes VALUES (4, 'four', 'right', 'd4')"
        ),
        snapshot: ExpectedPair::COMMUTE,
        serializable: ExpectedPair::COMMUTE,
    },
    PairCase {
        name: "insert_or_ignore_same_primary_key",
        relationship: "conflict-mode inserts whose surviving effects claim one row",
        left: operation!(
            Insert,
            "INSERT OR IGNORE INTO notes VALUES (3, 'three-left', 'left', 'd3')"
        ),
        right: operation!(
            Insert,
            "INSERT OR IGNORE INTO notes VALUES (3, 'three-right', 'right', 'd4')"
        ),
        snapshot: ExpectedPair::CONFLICT,
        serializable: ExpectedPair::CONFLICT,
    },
    PairCase {
        name: "upsert_do_nothing_same_unique_key",
        relationship: "UPSERT survivors claiming one unique owner",
        left: operation!(
            Upsert,
            "INSERT INTO notes VALUES (3, 'shared', 'left', 'd3') ON CONFLICT DO NOTHING"
        ),
        right: operation!(
            Upsert,
            "INSERT INTO notes VALUES (4, 'shared', 'right', 'd4') ON CONFLICT DO NOTHING"
        ),
        snapshot: ExpectedPair::CONFLICT,
        serializable: ExpectedPair::CONFLICT,
    },
    PairCase {
        name: "upsert_do_update_disjoint_rows",
        relationship: "different target rows with a shared procedural read table",
        left: operation!(
            Upsert,
            "INSERT INTO notes VALUES (1, 'one', 'unused', 'd1')
             ON CONFLICT(id) DO UPDATE SET body = notes.body || '-left'"
        ),
        right: operation!(
            Upsert,
            "INSERT INTO notes VALUES (2, 'two', 'unused', 'd2')
             ON CONFLICT(id) DO UPDATE SET body = notes.body || '-right'"
        ),
        snapshot: ExpectedPair::COMMUTE,
        serializable: ExpectedPair::CONFLICT,
    },
    PairCase {
        name: "upsert_do_update_same_row",
        relationship: "two conflict updates to one row identity",
        left: operation!(
            Upsert,
            "INSERT INTO notes VALUES (1, 'one', 'left', 'd1')
             ON CONFLICT(id) DO UPDATE SET body = excluded.body"
        ),
        right: operation!(
            Upsert,
            "INSERT INTO notes VALUES (1, 'one', 'right', 'd1')
             ON CONFLICT(id) DO UPDATE SET body = excluded.body"
        ),
        snapshot: ExpectedPair::CONFLICT,
        serializable: ExpectedPair::CONFLICT,
    },
    PairCase {
        name: "upsert_do_update_insert_path_same_unique_key",
        relationship: "two UPSERT insert paths claiming one unique owner",
        left: operation!(
            Upsert,
            "INSERT INTO notes VALUES (3, 'shared', 'left', 'd3')
             ON CONFLICT(slug) DO UPDATE SET body = excluded.body"
        ),
        right: operation!(
            Upsert,
            "INSERT INTO notes VALUES (4, 'shared', 'right', 'd4')
             ON CONFLICT(slug) DO UPDATE SET body = excluded.body"
        ),
        snapshot: ExpectedPair::CONFLICT,
        serializable: ExpectedPair::CONFLICT,
    },
    PairCase {
        name: "upsert_primary_key_move_and_destination_insert",
        relationship: "UPSERT key movement and another write to its destination",
        left: operation!(
            Upsert,
            "INSERT INTO notes VALUES (9, 'one', 'moved', 'd9')
             ON CONFLICT(slug) DO UPDATE SET
                id = excluded.id,
                body = excluded.body,
                detail = excluded.detail"
        ),
        right: operation!(
            Insert,
            "INSERT INTO notes VALUES (9, 'nine', 'destination', 'd9')"
        ),
        snapshot: ExpectedPair::CONFLICT,
        serializable: ExpectedPair::CONFLICT,
    },
    PairCase {
        name: "upsert_child_retarget_and_parent_delete",
        relationship: "UPSERT foreign-key movement and deletion of its new parent",
        left: operation!(
            Upsert,
            "INSERT INTO children VALUES (100, 'p20', 'retargeted')
             ON CONFLICT(id) DO UPDATE SET
                parent_code = excluded.parent_code,
                body = excluded.body"
        ),
        right: operation!(Delete, "DELETE FROM parents WHERE id = 20"),
        snapshot: ExpectedPair::CONFLICT,
        serializable: ExpectedPair::CONFLICT,
    },
    PairCase {
        name: "replace_disjoint_unique_victims",
        relationship: "replacement statements retiring different row and UNIQUE owners",
        left: operation!(
            Replace,
            "REPLACE INTO notes VALUES (3, 'one', 'left', 'd3')"
        ),
        right: operation!(
            Replace,
            "INSERT OR REPLACE INTO notes VALUES (4, 'two', 'right', 'd4')"
        ),
        snapshot: ExpectedPair::COMMUTE,
        serializable: ExpectedPair::COMMUTE,
    },
    PairCase {
        name: "replace_same_unique_victim",
        relationship: "replacement statements retiring one shared row and UNIQUE owner",
        left: operation!(
            Replace,
            "REPLACE INTO notes VALUES (3, 'one', 'left', 'd3')"
        ),
        right: operation!(
            Replace,
            "INSERT OR REPLACE INTO notes VALUES (4, 'one', 'right', 'd4')"
        ),
        snapshot: ExpectedPair::CONFLICT,
        serializable: ExpectedPair::CONFLICT,
    },
    PairCase {
        name: "update_or_replace_and_victim_update",
        relationship: "replacement victim concurrently changed by another transaction",
        left: operation!(
            Replace,
            "UPDATE OR REPLACE notes SET slug = 'two', body = 'left' WHERE id = 1"
        ),
        right: operation!(Update, "UPDATE notes SET body = 'right' WHERE id = 2"),
        snapshot: ExpectedPair::CONFLICT,
        serializable: ExpectedPair::CONFLICT,
    },
    PairCase {
        name: "update_disjoint_rows",
        relationship: "different row writes with a shared predicate-read table",
        left: operation!(Update, "UPDATE notes SET body = 'left' WHERE id = 1"),
        right: operation!(Update, "UPDATE notes SET body = 'right' WHERE id = 2"),
        snapshot: ExpectedPair::COMMUTE,
        serializable: ExpectedPair::CONFLICT,
    },
    PairCase {
        name: "update_same_row",
        relationship: "same row identity",
        left: operation!(Update, "UPDATE notes SET body = 'left' WHERE id = 1"),
        right: operation!(Update, "UPDATE notes SET body = 'right' WHERE id = 1"),
        snapshot: ExpectedPair::CONFLICT,
        serializable: ExpectedPair::CONFLICT,
    },
    PairCase {
        name: "update_or_ignore_disjoint_rows",
        relationship: "conflict-mode writes to different rows with one predicate-read table",
        left: operation!(
            Update,
            "UPDATE OR IGNORE notes SET body = 'left' WHERE id = 1"
        ),
        right: operation!(
            Update,
            "UPDATE OR IGNORE notes SET body = 'right' WHERE id = 2"
        ),
        snapshot: ExpectedPair::COMMUTE,
        serializable: ExpectedPair::CONFLICT,
    },
    PairCase {
        name: "update_or_ignore_same_unique_key",
        relationship: "conflict-mode updates whose surviving effects claim one unique owner",
        left: operation!(
            Update,
            "UPDATE OR IGNORE notes SET slug = 'shared' WHERE id = 1"
        ),
        right: operation!(
            Update,
            "UPDATE OR IGNORE notes SET slug = 'shared' WHERE id = 2"
        ),
        snapshot: ExpectedPair::CONFLICT,
        serializable: ExpectedPair::CONFLICT,
    },
    PairCase {
        name: "delete_disjoint_rows",
        relationship: "different row deletes with a shared predicate-read table",
        left: operation!(Delete, "DELETE FROM notes WHERE id = 1"),
        right: operation!(Delete, "DELETE FROM notes WHERE id = 2"),
        snapshot: ExpectedPair::COMMUTE,
        serializable: ExpectedPair::CONFLICT,
    },
    PairCase {
        name: "update_and_delete_same_row",
        relationship: "same row identity across SQL shapes",
        left: operation!(Update, "UPDATE notes SET body = 'left' WHERE id = 1"),
        right: operation!(Delete, "DELETE FROM notes WHERE id = 1"),
        snapshot: ExpectedPair::CONFLICT,
        serializable: ExpectedPair::CONFLICT,
    },
    PairCase {
        name: "sibling_foreign_key_inserts",
        relationship: "different children referencing one live parent",
        left: operation!(Insert, "INSERT INTO children VALUES (101, 'p10', 'left')"),
        right: operation!(Insert, "INSERT INTO children VALUES (102, 'p10', 'right')"),
        snapshot: ExpectedPair::COMMUTE,
        serializable: ExpectedPair::CONFLICT,
    },
    PairCase {
        name: "parent_delete_and_child_insert",
        relationship: "parent key and its incoming-reference range",
        left: operation!(Delete, "DELETE FROM parents WHERE id = 20"),
        right: operation!(Insert, "INSERT INTO children VALUES (101, 'p20', 'late')"),
        snapshot: ExpectedPair::CONFLICT,
        serializable: ExpectedPair::CONFLICT,
    },
    PairCase {
        name: "create_distinct_tables",
        relationship: "different schema-object names and table identities",
        left: operation!(CreateTable, "CREATE TABLE alpha (id INTEGER PRIMARY KEY)"),
        right: operation!(CreateTable, "CREATE TABLE beta (id INTEGER PRIMARY KEY)"),
        snapshot: ExpectedPair::COMMUTE,
        serializable: ExpectedPair::COMMUTE,
    },
    PairCase {
        name: "create_same_table_name",
        relationship: "same SQLite schema-object name",
        left: operation!(
            CreateTable,
            "CREATE TABLE collision (id INTEGER PRIMARY KEY)"
        ),
        right: operation!(
            CreateTable,
            "CREATE TABLE collision (id INTEGER PRIMARY KEY, body TEXT)"
        ),
        snapshot: ExpectedPair::CONFLICT,
        serializable: ExpectedPair::CONFLICT,
    },
    PairCase {
        name: "drop_table_and_stale_insert",
        relationship: "table-root destruction and a stale row write",
        left: operation!(DropTable, "DROP TABLE notes"),
        right: operation!(Insert, "INSERT INTO notes VALUES (3, 'three', 'row', 'd3')"),
        snapshot: ExpectedPair::CONFLICT,
        serializable: ExpectedPair::CONFLICT,
    },
    PairCase {
        name: "drop_table_and_disjoint_parent_insert",
        relationship: "different table-owned roots",
        left: operation!(DropTable, "DROP TABLE notes"),
        right: operation!(
            Insert,
            "INSERT INTO parents VALUES (30, 'p30', 'parent-thirty')"
        ),
        snapshot: ExpectedPair::COMMUTE,
        serializable: ExpectedPair::COMMUTE,
    },
    PairCase {
        name: "table_and_index_name_collision",
        relationship: "table and index sharing SQLite's schema namespace",
        left: operation!(
            CreateTable,
            "CREATE TABLE collision (id INTEGER PRIMARY KEY)"
        ),
        right: operation!(CreateIndex, "CREATE INDEX collision ON notes (body)"),
        snapshot: ExpectedPair::CONFLICT,
        serializable: ExpectedPair::CONFLICT,
    },
    PairCase {
        name: "rename_distinct_columns",
        relationship: "different stable column-name bindings",
        left: operation!(
            RenameColumn,
            "ALTER TABLE notes RENAME COLUMN body TO contents"
        ),
        right: operation!(
            RenameColumn,
            "ALTER TABLE notes RENAME COLUMN detail TO annotation"
        ),
        snapshot: ExpectedPair::COMMUTE,
        serializable: ExpectedPair::COMMUTE,
    },
    PairCase {
        name: "rename_columns_to_same_name",
        relationship: "different columns claiming one name binding",
        left: operation!(
            RenameColumn,
            "ALTER TABLE notes RENAME COLUMN body TO contents"
        ),
        right: operation!(
            RenameColumn,
            "ALTER TABLE notes RENAME COLUMN detail TO contents"
        ),
        snapshot: ExpectedPair::CONFLICT,
        serializable: ExpectedPair::CONFLICT,
    },
    PairCase {
        name: "add_distinct_columns",
        relationship: "different column names with compatible defaults",
        left: operation!(
            AddColumn,
            "ALTER TABLE notes ADD COLUMN alpha TEXT DEFAULT 'alpha'"
        ),
        right: operation!(
            AddColumn,
            "ALTER TABLE notes ADD COLUMN beta TEXT DEFAULT 'beta'"
        ),
        snapshot: ExpectedPair::COMMUTE,
        serializable: ExpectedPair::COMMUTE,
    },
    PairCase {
        name: "add_and_drop_disjoint_columns",
        relationship: "new column and unrelated existing column",
        left: operation!(
            AddColumn,
            "ALTER TABLE notes ADD COLUMN summary TEXT DEFAULT 'summary'"
        ),
        right: operation!(DropColumn, "ALTER TABLE notes DROP COLUMN detail"),
        snapshot: ExpectedPair::COMMUTE,
        serializable: ExpectedPair::COMMUTE,
    },
    PairCase {
        name: "add_column_and_create_unrelated_index",
        relationship: "new column and index over an existing stable column",
        left: operation!(
            AddColumn,
            "ALTER TABLE notes ADD COLUMN summary TEXT DEFAULT 'summary'"
        ),
        right: operation!(
            CreateIndex,
            "CREATE INDEX notes_slug_lookup ON notes (slug)"
        ),
        snapshot: ExpectedPair::COMMUTE,
        serializable: ExpectedPair::COMMUTE,
    },
    PairCase {
        name: "rename_column_and_stale_index",
        relationship: "name-bound index compiled before a column rename",
        left: operation!(
            RenameColumn,
            "ALTER TABLE notes RENAME COLUMN body TO contents"
        ),
        right: operation!(CreateIndex, "CREATE INDEX notes_body_stale ON notes (body)"),
        snapshot: ExpectedPair::directional(Disposition::SecondRejects, Disposition::BothAdmit),
        serializable: ExpectedPair::directional(Disposition::SecondRejects, Disposition::BothAdmit),
    },
    PairCase {
        name: "create_distinct_secondary_indexes",
        relationship: "different indexes sharing the current schema head",
        left: operation!(
            CreateIndex,
            "CREATE INDEX notes_slug_second ON notes (slug)"
        ),
        right: operation!(
            CreateIndex,
            "CREATE INDEX notes_detail_second ON notes (detail)"
        ),
        snapshot: ExpectedPair::CONFLICT,
        serializable: ExpectedPair::CONFLICT,
    },
    PairCase {
        name: "secondary_index_and_stale_insert",
        relationship: "access-path DDL and a row write",
        left: operation!(
            CreateIndex,
            "CREATE INDEX notes_detail_lookup ON notes (detail)"
        ),
        right: operation!(Insert, "INSERT INTO notes VALUES (3, 'three', 'row', 'd3')"),
        snapshot: ExpectedPair::COMMUTE,
        serializable: ExpectedPair::COMMUTE,
    },
    PairCase {
        name: "secondary_index_and_stale_upsert",
        relationship: "access-path DDL and an UPSERT reading the same table",
        left: operation!(
            CreateIndex,
            "CREATE INDEX notes_detail_upsert_lookup ON notes (detail)"
        ),
        right: operation!(
            Upsert,
            "INSERT INTO notes VALUES (1, 'one', 'unused', 'd1')
             ON CONFLICT(id) DO UPDATE SET body = notes.body || '-upsert'"
        ),
        snapshot: ExpectedPair::COMMUTE,
        serializable: ExpectedPair::directional(Disposition::SecondRejects, Disposition::BothAdmit),
    },
    PairCase {
        name: "secondary_index_and_stale_replace",
        relationship: "access-path DDL and replacement of an existing row",
        left: operation!(
            CreateIndex,
            "CREATE INDEX notes_detail_replace_lookup ON notes (detail)"
        ),
        right: operation!(
            Replace,
            "REPLACE INTO notes VALUES (3, 'one', 'replacement', 'd3')"
        ),
        snapshot: ExpectedPair::COMMUTE,
        serializable: ExpectedPair::COMMUTE,
    },
    PairCase {
        name: "unique_index_and_stale_insert",
        relationship: "write-contract DDL and a row compiled against its predecessor",
        left: operation!(
            CreateIndex,
            "CREATE UNIQUE INDEX notes_detail_unique ON notes (detail)"
        ),
        right: operation!(Insert, "INSERT INTO notes VALUES (3, 'three', 'row', 'd3')"),
        snapshot: ExpectedPair::CONFLICT,
        serializable: ExpectedPair::CONFLICT,
    },
    PairCase {
        name: "unique_index_and_stale_upsert",
        relationship: "write-contract DDL and an UPSERT compiled against its predecessor",
        left: operation!(
            CreateIndex,
            "CREATE UNIQUE INDEX notes_detail_upsert_unique ON notes (detail)"
        ),
        right: operation!(
            Upsert,
            "INSERT INTO notes VALUES (1, 'one', 'unused', 'd1')
             ON CONFLICT(id) DO UPDATE SET body = excluded.body"
        ),
        snapshot: ExpectedPair::CONFLICT,
        serializable: ExpectedPair::CONFLICT,
    },
    PairCase {
        name: "unique_index_and_stale_replace",
        relationship: "write-contract DDL and replacement compiled against its predecessor",
        left: operation!(
            CreateIndex,
            "CREATE UNIQUE INDEX notes_detail_replace_unique ON notes (detail)"
        ),
        right: operation!(
            Replace,
            "REPLACE INTO notes VALUES (3, 'one', 'replacement', 'd3')"
        ),
        snapshot: ExpectedPair::CONFLICT,
        serializable: ExpectedPair::CONFLICT,
    },
    PairCase {
        name: "drop_secondary_index_and_stale_insert",
        relationship: "access-path retirement and a row write",
        left: operation!(DropIndex, "DROP INDEX notes_by_body"),
        right: operation!(Insert, "INSERT INTO notes VALUES (3, 'three', 'row', 'd3')"),
        snapshot: ExpectedPair::COMMUTE,
        serializable: ExpectedPair::COMMUTE,
    },
    PairCase {
        name: "table_rename_and_stale_insert",
        relationship: "stable table identity across a stale name binding",
        left: operation!(RenameTable, "ALTER TABLE notes RENAME TO archived_notes"),
        right: operation!(Insert, "INSERT INTO notes VALUES (3, 'three', 'row', 'd3')"),
        snapshot: ExpectedPair::COMMUTE,
        serializable: ExpectedPair::COMMUTE,
    },
    PairCase {
        name: "table_rename_and_stale_upsert",
        relationship: "stable table identity across a stale UPSERT name binding",
        left: operation!(RenameTable, "ALTER TABLE notes RENAME TO archived_notes"),
        right: operation!(
            Upsert,
            "INSERT INTO notes VALUES (1, 'one', 'unused', 'd1')
             ON CONFLICT(id) DO UPDATE SET body = notes.body || '-upsert'"
        ),
        snapshot: ExpectedPair::COMMUTE,
        serializable: ExpectedPair::COMMUTE,
    },
    PairCase {
        name: "table_rename_and_stale_replace",
        relationship: "stable table identity across a stale replacement name binding",
        left: operation!(RenameTable, "ALTER TABLE notes RENAME TO archived_notes"),
        right: operation!(
            Replace,
            "REPLACE INTO notes VALUES (3, 'one', 'replacement', 'd3')"
        ),
        snapshot: ExpectedPair::COMMUTE,
        serializable: ExpectedPair::COMMUTE,
    },
    PairCase {
        name: "column_rename_and_stale_upsert",
        relationship: "stable column identity across a stale UPSERT name binding",
        left: operation!(
            RenameColumn,
            "ALTER TABLE notes RENAME COLUMN body TO contents"
        ),
        right: operation!(
            Upsert,
            "INSERT INTO notes VALUES (1, 'one', 'unused', 'd1')
             ON CONFLICT(id) DO UPDATE SET body = notes.body || '-upsert'"
        ),
        snapshot: ExpectedPair::COMMUTE,
        serializable: ExpectedPair::directional(Disposition::SecondRejects, Disposition::BothAdmit),
    },
    PairCase {
        name: "add_column_and_stale_upsert",
        relationship: "compatible column evolution and an UPSERT reading the old schema",
        left: operation!(
            AddColumn,
            "ALTER TABLE notes ADD COLUMN summary TEXT DEFAULT 'summary'"
        ),
        right: operation!(
            Upsert,
            "INSERT INTO notes VALUES (1, 'one', 'unused', 'd1')
             ON CONFLICT(id) DO UPDATE SET body = notes.body || '-upsert'"
        ),
        snapshot: ExpectedPair::COMMUTE,
        serializable: ExpectedPair::directional(Disposition::SecondRejects, Disposition::BothAdmit),
    },
];

#[test]
fn registered_operation_pairs_obey_admission_and_convergence_contracts() {
    for &case in PAIR_CASES {
        for isolation in [IsolationLevel::Snapshot, IsolationLevel::Serializable] {
            let left_then_right = run_case(case, isolation, AdmissionOrder::LeftThenRight);
            let right_then_left = run_case(case, isolation, AdmissionOrder::RightThenLeft);
            if case.expected(isolation).commutes() {
                assert_eq!(
                    left_then_right, right_then_left,
                    "commuting pair produced order-dependent state: case={} relationship={} isolation={isolation:?}",
                    case.name, case.relationship
                );
            }
        }
    }
}

#[test]
fn pair_registry_covers_each_supported_sql_shape_and_relationship_class() {
    let names = PAIR_CASES
        .iter()
        .map(|case| case.name)
        .collect::<BTreeSet<_>>();
    assert_eq!(
        names.len(),
        PAIR_CASES.len(),
        "operation-pair names must be unique"
    );
    assert!(PAIR_CASES.iter().all(|case| !case.relationship.is_empty()));

    let shapes = PAIR_CASES
        .iter()
        .flat_map(|case| [case.left.shape, case.right.shape])
        .collect::<BTreeSet<_>>();
    assert_eq!(shapes, BTreeSet::from(SQL_SHAPES));

    assert!(PAIR_CASES.iter().any(|case| case.snapshot.commutes()));
    assert!(PAIR_CASES.iter().any(|case| {
        case.snapshot == ExpectedPair::CONFLICT && case.serializable == ExpectedPair::CONFLICT
    }));
    assert!(
        PAIR_CASES
            .iter()
            .any(|case| { case.snapshot.commutes() && !case.serializable.commutes() })
    );
    assert!(PAIR_CASES.iter().any(|case| {
        case.snapshot.left_then_right != case.snapshot.right_then_left
            || case.serializable.left_then_right != case.serializable.right_then_left
    }));
}

fn run_case(case: PairCase, isolation: IsolationLevel, order: AdmissionOrder) -> DatabaseState {
    let directory = tempfile::tempdir().unwrap();
    let authority = server();
    let left_path = directory.path().join("left.sqlite");
    let right_path = directory.path().join("right.sqlite");
    let left = MultiliteConnection::open_with(
        &left_path,
        OpenOptions::new()
            .isolation_level(isolation)
            .sync_policy(SyncPolicy::LocalOnly)
            .server(router(Arc::clone(&authority))),
    )
    .unwrap();
    assert!(authority.create_space(SpaceId(left.database_id().to_bytes())));
    let right = MultiliteConnection::open_with(
        &right_path,
        OpenOptions::new()
            .isolation_level(isolation)
            .sync_policy(SyncPolicy::LocalOnly)
            .invitation(left.replica_invitation())
            .server(router(Arc::clone(&authority))),
    )
    .unwrap();

    initialize(&left);
    assert!(matches!(left.push().unwrap(), PushOutcome::Drained));
    right.pull().unwrap();
    right.rebase().unwrap();

    left.execute(case.left.sql, ()).unwrap_or_else(|error| {
        panic!(
            "left operation failed locally: case={} isolation={isolation:?}: {error}",
            case.name
        )
    });
    right.execute(case.right.sql, ()).unwrap_or_else(|error| {
        panic!(
            "right operation failed locally: case={} isolation={isolation:?}: {error}",
            case.name
        )
    });

    let expected = case.expected(isolation).for_order(order);
    let (first, second) = match order {
        AdmissionOrder::LeftThenRight => (&left, &right),
        AdmissionOrder::RightThenLeft => (&right, &left),
    };
    assert!(
        matches!(first.push().unwrap(), PushOutcome::Drained),
        "first submission did not admit: case={} relationship={} isolation={isolation:?} order={order:?}",
        case.name,
        case.relationship
    );
    match (expected, second.push().unwrap()) {
        (Disposition::BothAdmit, PushOutcome::Drained) => {}
        (Disposition::SecondRejects, PushOutcome::Rejected(rejection)) => {
            assert!(
                matches!(
                    rejection.error(),
                    KernelError::RangeAssertFailed { failures } if !failures.is_empty()
                ),
                "pair rejection was not a range-assert conflict: case={} relationship={} isolation={isolation:?} order={order:?}: {:?}",
                case.name,
                case.relationship,
                rejection.error()
            );
            second.rollback(&rejection).unwrap();
            assert!(matches!(second.push().unwrap(), PushOutcome::Drained));
        }
        (Disposition::BothAdmit, PushOutcome::Rejected(rejection)) => panic!(
            "pair unexpectedly conflicted: case={} relationship={} isolation={isolation:?} order={order:?}: {:?}",
            case.name,
            case.relationship,
            rejection.error()
        ),
        (Disposition::SecondRejects, PushOutcome::Drained) => panic!(
            "pair unexpectedly commuted: case={} relationship={} isolation={isolation:?} order={order:?}",
            case.name, case.relationship
        ),
    }

    synchronize(&left, &right);
    drop(left);
    drop(right);

    let left = Connection::open(&left_path).unwrap();
    let right = Connection::open(&right_path).unwrap();
    assert_integrity(&left, case, isolation, order);
    assert_integrity(&right, case, isolation, order);
    let left_state = observe(&left);
    let right_state = observe(&right);
    assert_eq!(
        left_state, right_state,
        "replicas diverged: case={} relationship={} isolation={isolation:?} order={order:?}",
        case.name, case.relationship
    );
    left_state
}

fn initialize<H>(database: &MultiliteConnection<H>)
where
    H: ServerHandle + Send + Sync + 'static,
{
    database
        .update(|transaction| {
            transaction.execute(
                "CREATE TABLE notes (
                    id INTEGER PRIMARY KEY,
                    slug TEXT NOT NULL UNIQUE,
                    body TEXT NOT NULL,
                    detail TEXT
                )",
                (),
            )?;
            transaction.execute("CREATE INDEX notes_by_body ON notes (body)", ())?;
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
                "INSERT INTO notes VALUES
                    (1, 'one', 'body-one', 'd1'),
                    (2, 'two', 'body-two', 'd2')",
                (),
            )?;
            transaction.execute(
                "INSERT INTO parents VALUES
                    (10, 'p10', 'parent-ten'),
                    (20, 'p20', 'parent-twenty')",
                (),
            )?;
            transaction.execute("INSERT INTO children VALUES (100, 'p10', 'base')", ())?;
            Ok(())
        })
        .unwrap();
}

fn synchronize<H1, H2>(left: &MultiliteConnection<H1>, right: &MultiliteConnection<H2>)
where
    H1: ServerHandle + Send + Sync + 'static,
    H2: ServerHandle + Send + Sync + 'static,
{
    left.pull().unwrap();
    right.pull().unwrap();
    left.rebase().unwrap();
    right.rebase().unwrap();
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct DatabaseState {
    tables: Vec<TableState>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct TableState {
    name: String,
    columns: Vec<(String, String, bool, Option<String>, i64, i64)>,
    indexes: Vec<IndexState>,
    foreign_keys: Vec<ForeignKeyState>,
    rows: Vec<Vec<String>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct IndexState {
    name: String,
    unique: bool,
    origin: String,
    partial: bool,
    columns: Vec<IndexColumnState>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct IndexColumnState {
    sequence: i64,
    column_id: i64,
    name: Option<String>,
    descending: bool,
    collation: Option<String>,
    is_key: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ForeignKeyState {
    id: i64,
    sequence: i64,
    parent_table: String,
    child_column: String,
    parent_column: String,
    on_update: String,
    on_delete: String,
    match_kind: String,
}

fn observe(connection: &Connection) -> DatabaseState {
    let tables = connection
        .prepare(
            "SELECT name FROM sqlite_schema
             WHERE type = 'table'
               AND name NOT LIKE '__multilite__%'
               AND name NOT LIKE 'sqlite_%'
             ORDER BY lower(name), name",
        )
        .unwrap()
        .query_map((), |row| row.get::<_, String>(0))
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap()
        .into_iter()
        .map(|name| observe_table(connection, name))
        .collect();
    DatabaseState { tables }
}

fn observe_table(connection: &Connection, name: String) -> TableState {
    let columns = connection
        .prepare(
            "SELECT name, type, \"notnull\", dflt_value, pk, hidden
             FROM pragma_table_xinfo(?1)
             ORDER BY lower(name), name",
        )
        .unwrap()
        .query_map([&name], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, bool>(2)?,
                row.get::<_, Option<String>>(3)?,
                row.get::<_, i64>(4)?,
                row.get::<_, i64>(5)?,
            ))
        })
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap();
    let indexes = connection
        .prepare(
            "SELECT name, \"unique\", origin, partial
             FROM pragma_index_list(?1) ORDER BY name",
        )
        .unwrap()
        .query_map([&name], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, bool>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, bool>(3)?,
            ))
        })
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap()
        .into_iter()
        .map(|(name, unique, origin, partial)| {
            let columns = connection
                .prepare(
                    "SELECT seqno, cid, name, \"desc\", coll, \"key\"
                     FROM pragma_index_xinfo(?1) ORDER BY seqno",
                )
                .unwrap()
                .query_map([&name], |row| {
                    Ok(IndexColumnState {
                        sequence: row.get(0)?,
                        column_id: row.get(1)?,
                        name: row.get(2)?,
                        descending: row.get(3)?,
                        collation: row.get(4)?,
                        is_key: row.get(5)?,
                    })
                })
                .unwrap()
                .collect::<rusqlite::Result<Vec<_>>>()
                .unwrap();
            IndexState {
                name,
                unique,
                origin,
                partial,
                columns,
            }
        })
        .collect();
    let foreign_keys = connection
        .prepare(
            "SELECT id, seq, \"table\", \"from\", \"to\",
                    on_update, on_delete, \"match\"
             FROM pragma_foreign_key_list(?1) ORDER BY id, seq",
        )
        .unwrap()
        .query_map([&name], |row| {
            Ok(ForeignKeyState {
                id: row.get(0)?,
                sequence: row.get(1)?,
                parent_table: row.get(2)?,
                child_column: row.get(3)?,
                parent_column: row.get(4)?,
                on_update: row.get(5)?,
                on_delete: row.get(6)?,
                match_kind: row.get(7)?,
            })
        })
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap();
    let visible_columns = columns
        .iter()
        .filter(|column| column.5 == 0)
        .map(|column| quote_identifier(&column.0))
        .collect::<Vec<_>>();
    let projection = visible_columns.join(", ");
    let rows = connection
        .prepare(&format!(
            "SELECT {projection} FROM {} ORDER BY {projection}",
            quote_identifier(&name)
        ))
        .unwrap()
        .query_map((), |row| {
            (0..visible_columns.len())
                .map(|index| row.get_ref(index).map(encode_value))
                .collect::<rusqlite::Result<Vec<_>>>()
        })
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap();
    TableState {
        name,
        columns,
        indexes,
        foreign_keys,
        rows,
    }
}

fn assert_integrity(
    connection: &Connection,
    case: PairCase,
    isolation: IsolationLevel,
    order: AdmissionOrder,
) {
    let integrity = connection
        .query_row("PRAGMA integrity_check", (), |row| row.get::<_, String>(0))
        .unwrap();
    assert_eq!(
        integrity, "ok",
        "SQLite integrity failed: case={} isolation={isolation:?} order={order:?}",
        case.name
    );
    let foreign_key_violations = connection
        .prepare("PRAGMA foreign_key_check")
        .unwrap()
        .query_map((), |_| Ok(()))
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap();
    assert!(
        foreign_key_violations.is_empty(),
        "foreign-key integrity failed: case={} isolation={isolation:?} order={order:?}",
        case.name
    );
    let pending = connection
        .query_row("SELECT count(*) FROM __multilite__pending", (), |row| {
            row.get::<_, i64>(0)
        })
        .unwrap();
    assert_eq!(
        pending, 0,
        "pending journal was not retired: case={} isolation={isolation:?} order={order:?}",
        case.name
    );
}

fn encode_value(value: ValueRef<'_>) -> String {
    match value {
        ValueRef::Null => "null".into(),
        ValueRef::Integer(value) => format!("integer:{value}"),
        ValueRef::Real(value) => format!("real:{:016x}", value.to_bits()),
        ValueRef::Text(value) => format!("text:{}", hex(value)),
        ValueRef::Blob(value) => format!("blob:{}", hex(value)),
    }
}

fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for &byte in bytes {
        encoded.push(char::from(DIGITS[usize::from(byte >> 4)]));
        encoded.push(char::from(DIGITS[usize::from(byte & 0x0f)]));
    }
    encoded
}

fn quote_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}
