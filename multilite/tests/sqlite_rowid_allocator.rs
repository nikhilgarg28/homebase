use std::collections::VecDeque;
use std::ffi::{CStr, c_char, c_int, c_void};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use rusqlite::{Connection, Error, ffi};

#[derive(Default)]
struct Observed {
    calls: Mutex<Vec<(String, String)>>,
    destroyed: AtomicUsize,
}

struct Allocator {
    candidates: Mutex<VecDeque<std::result::Result<i64, c_int>>>,
    observed: Arc<Observed>,
    delegate_table: Option<String>,
}

unsafe extern "C" fn allocate(
    context: *mut c_void,
    _db: *mut ffi::sqlite3,
    schema: *const c_char,
    table: *const c_char,
    rowid: *mut ffi::sqlite3_int64,
) -> c_int {
    // SAFETY: `install` gives SQLite ownership of this box until `destroy`.
    let allocator = unsafe { &*(context.cast::<Allocator>()) };
    // SAFETY: SQLite supplies stable, NUL-terminated schema and table names.
    let schema = unsafe { CStr::from_ptr(schema) }
        .to_string_lossy()
        .into_owned();
    // SAFETY: Same contract as `schema`.
    let table = unsafe { CStr::from_ptr(table) }
        .to_string_lossy()
        .into_owned();
    allocator
        .observed
        .calls
        .lock()
        .unwrap()
        .push((schema, table.clone()));
    if allocator.delegate_table.as_deref() == Some(&table) {
        return ffi::SQLITE_NOTFOUND;
    }
    match allocator.candidates.lock().unwrap().pop_front() {
        Some(Ok(candidate)) => {
            // SAFETY: SQLite provides a valid output pointer for this call.
            unsafe { *rowid = candidate };
            ffi::SQLITE_OK
        }
        Some(Err(error)) => error,
        None => ffi::SQLITE_FULL,
    }
}

unsafe extern "C" fn destroy(context: *mut c_void) {
    // SAFETY: SQLite invokes the destructor exactly once for the owned box.
    let allocator = unsafe { Box::from_raw(context.cast::<Allocator>()) };
    allocator.observed.destroyed.fetch_add(1, Ordering::SeqCst);
}

fn install(
    connection: &Connection,
    candidates: impl IntoIterator<Item = std::result::Result<i64, c_int>>,
    delegate_table: Option<&str>,
) -> Arc<Observed> {
    let observed = Arc::new(Observed::default());
    let allocator = Box::new(Allocator {
        candidates: Mutex::new(candidates.into_iter().collect()),
        observed: Arc::clone(&observed),
        delegate_table: delegate_table.map(str::to_owned),
    });
    // SAFETY: The callback and destructor uphold the extension API contract.
    let result = unsafe {
        ffi::sqlite3_multilite_set_rowid_allocator(
            connection.handle(),
            Some(allocate),
            Box::into_raw(allocator).cast(),
            Some(destroy),
        )
    };
    assert_eq!(result, ffi::SQLITE_OK);
    observed
}

#[test]
fn stock_allocator_is_unchanged_without_a_hook() {
    let connection = Connection::open_in_memory().unwrap();
    connection
        .execute_batch(
            "CREATE TABLE items (id INTEGER PRIMARY KEY, body TEXT);
             INSERT INTO items (body) VALUES ('one'), ('two');",
        )
        .unwrap();
    assert_eq!(
        connection
            .prepare("SELECT id FROM items ORDER BY id")
            .unwrap()
            .query_map((), |row| row.get::<_, i64>(0))
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap(),
        [1, 2]
    );
}

#[test]
fn omitted_and_null_ipks_use_the_hook_but_explicit_ipks_do_not() {
    let connection = Connection::open_in_memory().unwrap();
    connection
        .execute_batch("CREATE TABLE items (id INTEGER PRIMARY KEY, body TEXT)")
        .unwrap();
    let observed = install(&connection, [Ok(401), Ok(402)], None);

    let first = connection
        .query_row(
            "INSERT INTO items (body) VALUES ('one') RETURNING id",
            (),
            |row| row.get::<_, i64>(0),
        )
        .unwrap();
    connection
        .execute("INSERT INTO items VALUES (900, 'explicit')", ())
        .unwrap();
    connection
        .execute("INSERT INTO items VALUES (NULL, 'null')", ())
        .unwrap();

    assert_eq!(first, 401);
    assert_eq!(connection.last_insert_rowid(), 402);
    assert_eq!(
        observed.calls.lock().unwrap().as_slice(),
        [
            ("main".into(), "items".into()),
            ("main".into(), "items".into())
        ]
    );
}

#[test]
fn collisions_retry_and_invalid_candidates_fail() {
    let connection = Connection::open_in_memory().unwrap();
    connection
        .execute_batch(
            "CREATE TABLE items (id INTEGER PRIMARY KEY);
             INSERT INTO items VALUES (700);",
        )
        .unwrap();
    let observed = install(&connection, [Ok(700), Ok(701), Ok(0)], None);

    connection
        .execute("INSERT INTO items DEFAULT VALUES", ())
        .unwrap();
    let error = connection
        .execute("INSERT INTO items DEFAULT VALUES", ())
        .unwrap_err();
    assert_eq!(
        error.sqlite_error().map(|error| error.extended_code),
        Some(ffi::SQLITE_MISMATCH)
    );
    assert_eq!(observed.calls.lock().unwrap().len(), 3);
    assert_eq!(
        connection
            .prepare("SELECT id FROM items ORDER BY id")
            .unwrap()
            .query_map((), |row| row.get::<_, i64>(0))
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap(),
        [700, 701]
    );
}

#[test]
fn collision_retry_budget_is_bounded() {
    let connection = Connection::open_in_memory().unwrap();
    connection
        .execute_batch(
            "CREATE TABLE items (id INTEGER PRIMARY KEY);
             INSERT INTO items VALUES (700);",
        )
        .unwrap();
    let observed = install(&connection, std::iter::repeat_n(Ok(700), 100), None);

    let error = connection
        .execute("INSERT INTO items DEFAULT VALUES", ())
        .unwrap_err();
    assert_eq!(
        error.sqlite_error().map(|error| error.extended_code),
        Some(ffi::SQLITE_FULL)
    );
    assert_eq!(observed.calls.lock().unwrap().len(), 100);
    assert_eq!(
        connection
            .query_row("SELECT count(*) FROM items", (), |row| row.get::<_, i64>(0))
            .unwrap(),
        1
    );
}

#[test]
fn callback_failure_rolls_back_the_whole_statement() {
    let connection = Connection::open_in_memory().unwrap();
    connection
        .execute_batch("CREATE TABLE items (id INTEGER PRIMARY KEY)")
        .unwrap();
    install(
        &connection,
        [Ok(801), Err(ffi::SQLITE_IOERR), Ok(802)],
        None,
    );

    let error = connection
        .execute("INSERT INTO items VALUES (NULL), (NULL)", ())
        .unwrap_err();
    assert!(matches!(error, Error::SqliteFailure(_, _)));
    assert_eq!(
        error.sqlite_error().map(|error| error.extended_code),
        Some(ffi::SQLITE_IOERR)
    );
    assert_eq!(
        connection
            .query_row("SELECT count(*) FROM items", (), |row| row.get::<_, i64>(0))
            .unwrap(),
        0
    );
}

#[test]
fn trigger_inserts_receive_ids_from_the_same_connection_allocator() {
    let connection = Connection::open_in_memory().unwrap();
    connection
        .execute_batch(
            "CREATE TABLE items (id INTEGER PRIMARY KEY, body TEXT);
             CREATE TABLE audit (id INTEGER PRIMARY KEY, item_id INTEGER NOT NULL);
             CREATE TRIGGER audit_item AFTER INSERT ON items BEGIN
                INSERT INTO audit(item_id) VALUES (NEW.id);
             END;",
        )
        .unwrap();
    let observed = install(&connection, [Ok(901), Ok(902)], None);

    connection
        .execute("INSERT INTO items(body) VALUES ('triggered')", ())
        .unwrap();
    assert_eq!(
        connection
            .query_row("SELECT id FROM items", (), |row| row.get::<_, i64>(0))
            .unwrap(),
        901
    );
    assert_eq!(
        connection
            .query_row("SELECT id, item_id FROM audit", (), |row| {
                Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?))
            })
            .unwrap(),
        (902, 901)
    );
    assert_eq!(
        observed.calls.lock().unwrap().as_slice(),
        [
            ("main".into(), "items".into()),
            ("main".into(), "audit".into())
        ]
    );
}

#[test]
fn unrelated_vm_rowids_and_ineligible_tables_bypass_the_hook() {
    let connection = Connection::open_in_memory().unwrap();
    connection
        .execute_batch(
            "CREATE TABLE ordinary (id INTEGER PRIMARY KEY, body TEXT);
             CREATE TABLE delegated (id INTEGER PRIMARY KEY);
             CREATE TABLE composite (a TEXT, b TEXT, PRIMARY KEY (a, b)) WITHOUT ROWID;
             CREATE TABLE automatic (id INTEGER PRIMARY KEY AUTOINCREMENT);",
        )
        .unwrap();
    let observed = install(&connection, [Ok(501)], Some("delegated"));

    connection
        .execute("INSERT INTO ordinary (body) VALUES ('z')", ())
        .unwrap();
    connection
        .execute("INSERT INTO delegated DEFAULT VALUES", ())
        .unwrap();
    connection
        .execute("INSERT INTO composite VALUES ('a', 'b')", ())
        .unwrap();
    connection
        .execute("INSERT INTO automatic DEFAULT VALUES", ())
        .unwrap();
    let sorted = connection
        .prepare("SELECT body FROM ordinary ORDER BY body")
        .unwrap()
        .query_map((), |row| row.get::<_, String>(0))
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap();

    assert_eq!(sorted, ["z"]);
    assert_eq!(
        observed.calls.lock().unwrap().as_slice(),
        [
            ("main".into(), "ordinary".into()),
            ("main".into(), "delegated".into())
        ]
    );
    assert_eq!(
        connection
            .query_row("SELECT id FROM delegated", (), |row| row.get::<_, i64>(0))
            .unwrap(),
        1
    );
    assert_eq!(
        connection
            .query_row("SELECT id FROM automatic", (), |row| row.get::<_, i64>(0))
            .unwrap(),
        1
    );
}

#[test]
fn replacing_and_closing_connections_destroy_owned_state_once() {
    let first = Arc::new(Observed::default());
    let second;
    {
        let connection = Connection::open_in_memory().unwrap();
        let allocator = Box::new(Allocator {
            candidates: Mutex::new([Ok(1)].into()),
            observed: Arc::clone(&first),
            delegate_table: None,
        });
        // SAFETY: The callback and destructor uphold the extension API contract.
        assert_eq!(
            unsafe {
                ffi::sqlite3_multilite_set_rowid_allocator(
                    connection.handle(),
                    Some(allocate),
                    Box::into_raw(allocator).cast(),
                    Some(destroy),
                )
            },
            ffi::SQLITE_OK
        );
        second = install(&connection, [Ok(2)], None);
        assert_eq!(first.destroyed.load(Ordering::SeqCst), 1);
        assert_eq!(second.destroyed.load(Ordering::SeqCst), 0);
    }
    assert_eq!(first.destroyed.load(Ordering::SeqCst), 1);
    assert_eq!(second.destroyed.load(Ordering::SeqCst), 1);
}
