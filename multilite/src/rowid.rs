//! Durable, device-scoped allocation of positive SQLite rowids.

use std::ffi::{CStr, c_char, c_int, c_void};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;

use homebase_core::tag::DeviceId;
use parking_lot::Mutex;
use rusqlite::ffi;
use rusqlite::{Connection, OptionalExtension, params};
use sha2::{Digest, Sha256};

use crate::commit::committer::Committer;
use crate::{Error, Result};

const STATE_TABLE: &str = "__multilite__rowid_state";
const SLOTS_TABLE: &str = "__multilite__rowid_slots";
const NAMESPACE: &str = "__multilite__rowid";
const DEVICE_TAG_DOMAIN: &[u8] = b"multilite:rowid-device-tag:v1\0";

const DEVICE_TAG_BITS: u32 = 16;
const RANDOM_SLOT_BITS: u32 = 27;
const OFFSET_BITS: u32 = 20;
const SLOT_CAPACITY: u32 = 1 << OFFSET_BITS;
const LEASE_SIZE: u32 = 1 << 10;
const RANDOM_SLOT_MASK: u32 = (1 << RANDOM_SLOT_BITS) - 1;
const MAX_SLOT: u64 = (1_u64 << (DEVICE_TAG_BITS + RANDOM_SLOT_BITS)) - 1;
const MAX_SLOT_ATTEMPTS: usize = 1_024;

/// One durable, half-open range of rowids reserved for a branch connection.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct RowidLease {
    slot: u64,
    next: u32,
    end: u32,
}

impl RowidLease {
    pub(crate) fn next_rowid(&mut self) -> Option<i64> {
        if self.next == self.end {
            return None;
        }
        let offset = self.next;
        self.next += 1;
        Some(compose(self.slot, offset))
    }

    #[cfg(test)]
    fn offsets(self) -> std::ops::Range<u32> {
        self.next..self.end
    }

    #[cfg(test)]
    fn slot(self) -> u64 {
        self.slot
    }

    #[cfg(test)]
    pub(crate) fn for_test(slot: u64, next: u32, end: u32) -> Self {
        Self { slot, next, end }
    }
}

struct LeaseCursor {
    committer: Committer,
    lease: Option<RowidLease>,
}

/// Process-local lease cursor shared by every writable branch of one database.
#[derive(Clone)]
pub(crate) struct RowidAllocator {
    cursor: Arc<Mutex<LeaseCursor>>,
}

impl RowidAllocator {
    pub(crate) fn new(committer: Committer) -> Self {
        Self {
            cursor: Arc::new(Mutex::new(LeaseCursor {
                committer,
                lease: None,
            })),
        }
    }

    fn next(&mut self) -> Result<i64> {
        let mut cursor = self.cursor.lock();
        loop {
            if let Some(rowid) = cursor.lease.as_mut().and_then(RowidLease::next_rowid) {
                return Ok(rowid);
            }
            cursor.lease = Some(cursor.committer.lease_rowids_blocking()?);
        }
    }
}

/// Install durable allocation on one private writable branch connection.
pub(crate) fn install(connection: &Connection, allocator: RowidAllocator) -> Result<()> {
    let allocator = Box::new(allocator);
    // SAFETY: SQLite owns the box after this call. The callback does not
    // re-enter this connection, and the destructor reclaims the box once.
    let result = unsafe {
        ffi::sqlite3_multilite_set_rowid_allocator(
            connection.handle(),
            Some(allocate),
            Box::into_raw(allocator).cast(),
            Some(destroy_allocator),
        )
    };
    if result != ffi::SQLITE_OK {
        return Err(rusqlite::Error::SqliteFailure(
            ffi::Error::new(result),
            Some("could not install the Multilite rowid allocator".into()),
        )
        .into());
    }
    Ok(())
}

unsafe extern "C" fn allocate(
    context: *mut c_void,
    _connection: *mut ffi::sqlite3,
    schema: *const c_char,
    table: *const c_char,
    rowid: *mut ffi::sqlite3_int64,
) -> c_int {
    // SAFETY: SQLite retains the box passed by `install` until its destructor.
    let allocator = unsafe { &mut *context.cast::<RowidAllocator>() };
    // SAFETY: SQLite supplies stable NUL-terminated names for this callback.
    let schema = unsafe { CStr::from_ptr(schema) }.to_bytes();
    // SAFETY: Same contract as `schema`.
    let table = unsafe { CStr::from_ptr(table) }.to_bytes();
    if !schema.eq_ignore_ascii_case(b"main") || has_internal_prefix(table) {
        return ffi::SQLITE_NOTFOUND;
    }

    match catch_unwind(AssertUnwindSafe(|| allocator.next())) {
        Ok(Ok(candidate)) => {
            // SAFETY: SQLite supplies a valid output pointer.
            unsafe { *rowid = candidate };
            ffi::SQLITE_OK
        }
        Ok(Err(_)) => ffi::SQLITE_IOERR,
        Err(_) => ffi::SQLITE_ABORT,
    }
}

unsafe extern "C" fn destroy_allocator(context: *mut c_void) {
    // SAFETY: SQLite invokes this exactly once for the box passed by `install`.
    drop(unsafe { Box::from_raw(context.cast::<RowidAllocator>()) });
}

fn has_internal_prefix(table: &[u8]) -> bool {
    table
        .get(.."__multilite__".len())
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case(b"__multilite__"))
}

/// Create allocator metadata for one newly adopted or initialized replica.
pub(crate) fn initialize(connection: &Connection, device: DeviceId) -> Result<()> {
    connection.execute_batch(&format!(
        "CREATE TABLE {STATE_TABLE} (
            singleton INTEGER PRIMARY KEY NOT NULL CHECK(singleton = 1),
            device_id BLOB NOT NULL CHECK(length(device_id) = 16),
            active_slot INTEGER NOT NULL CHECK(active_slot > 0 AND active_slot <= {MAX_SLOT}),
            leased_through INTEGER NOT NULL
                CHECK(leased_through >= 0 AND leased_through <= {SLOT_CAPACITY})
        ) WITHOUT ROWID;
        CREATE TABLE {SLOTS_TABLE} (
            slot INTEGER PRIMARY KEY NOT NULL CHECK(slot > 0 AND slot <= {MAX_SLOT})
        ) WITHOUT ROWID"
    ))?;
    let tag = device_tag(device);
    let slot = insert_unused_slot(connection, tag, random_suffix)?;
    connection.execute(
        &format!(
            "INSERT INTO {STATE_TABLE}
                (singleton, device_id, active_slot, leased_through)
             VALUES (1, ?1, ?2, 0)"
        ),
        params![
            device.0.as_slice(),
            i64::try_from(slot).expect("rowid slot fits in i64")
        ],
    )?;
    Ok(())
}

/// Whether the complete allocator namespace exists.
pub(crate) fn is_initialized(connection: &Connection) -> Result<bool> {
    let mut statement = connection.prepare(
        "SELECT name FROM sqlite_schema
         WHERE type = 'table'
           AND substr(name, 1, length(?1)) = ?1 COLLATE NOCASE
         ORDER BY name",
    )?;
    let tables = statement
        .query_map([NAMESPACE], |row| row.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    match tables.as_slice() {
        [] => Ok(false),
        [slots, state] if slots == SLOTS_TABLE && state == STATE_TABLE => Ok(true),
        _ => Err(Error::InvalidDatabase(
            "rowid allocator namespace contains unexpected tables",
        )),
    }
}

/// Validate allocator table shape and all persisted slot state.
pub(crate) fn validate(connection: &Connection) -> Result<()> {
    if !is_initialized(connection)? {
        return Err(Error::InvalidDatabase("rowid allocator tables are missing"));
    }
    validate_columns(
        connection,
        STATE_TABLE,
        &[
            ("singleton", "INTEGER", true, 1),
            ("device_id", "BLOB", true, 0),
            ("active_slot", "INTEGER", true, 0),
            ("leased_through", "INTEGER", true, 0),
        ],
        "rowid allocator state table schema is invalid",
    )?;
    validate_columns(
        connection,
        SLOTS_TABLE,
        &[("slot", "INTEGER", true, 1)],
        "rowid allocator slot table schema is invalid",
    )?;
    validate_without_rowid(connection, STATE_TABLE)?;
    validate_without_rowid(connection, SLOTS_TABLE)?;

    let state = load_state(connection)?;
    let expected_tag = device_tag(state.device);
    if slot_tag(state.active_slot) != expected_tag || state.leased_through > SLOT_CAPACITY {
        return Err(Error::InvalidDatabase("rowid allocator state is malformed"));
    }
    let slots = used_slots(connection)?;
    if slots.is_empty()
        || !slots.contains(&state.active_slot)
        || slots
            .iter()
            .any(|slot| *slot == 0 || *slot > MAX_SLOT || slot_tag(*slot) != expected_tag)
    {
        return Err(Error::InvalidDatabase(
            "rowid allocator slots are malformed",
        ));
    }
    Ok(())
}

/// Ensure allocator metadata belongs to this replica's durable Homebase ID.
pub(crate) fn validate_device(connection: &Connection, device: DeviceId) -> Result<()> {
    if load_state(connection)?.device != device {
        return Err(Error::InvalidDatabase(
            "rowid allocator belongs to another device",
        ));
    }
    Ok(())
}

/// Persist and return the next branch lease.
pub(crate) fn lease(connection: &Connection) -> Result<RowidLease> {
    lease_with(connection, random_suffix)
}

fn lease_with(
    connection: &Connection,
    mut suffix: impl FnMut() -> Result<u32>,
) -> Result<RowidLease> {
    let mut state = load_state(connection)?;
    let expected_slot = state.active_slot;
    let expected_through = state.leased_through;
    if state.leased_through == SLOT_CAPACITY {
        state.active_slot = insert_unused_slot(connection, device_tag(state.device), &mut suffix)?;
        state.leased_through = 0;
    }
    let start = state.leased_through;
    let end = start.saturating_add(LEASE_SIZE).min(SLOT_CAPACITY);
    let changed = connection.execute(
        &format!(
            "UPDATE {STATE_TABLE}
             SET active_slot = ?1, leased_through = ?2
             WHERE singleton = 1
               AND active_slot = ?3
               AND leased_through = ?4"
        ),
        params![
            i64::try_from(state.active_slot).expect("rowid slot fits in i64"),
            end,
            i64::try_from(expected_slot).expect("rowid slot fits in i64"),
            expected_through
        ],
    )?;
    if changed != 1 {
        return Err(Error::InvalidDatabase(
            "rowid allocator state changed while leasing",
        ));
    }
    Ok(RowidLease {
        slot: state.active_slot,
        next: start,
        end,
    })
}

#[derive(Clone, Copy)]
struct DurableState {
    device: DeviceId,
    active_slot: u64,
    leased_through: u32,
}

fn load_state(connection: &Connection) -> Result<DurableState> {
    let row = connection
        .query_row(
            &format!(
                "SELECT device_id, active_slot, leased_through
                 FROM {STATE_TABLE} WHERE singleton = 1"
            ),
            (),
            |row| {
                Ok((
                    row.get::<_, Vec<u8>>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, i64>(2)?,
                ))
            },
        )
        .optional()?
        .ok_or(Error::InvalidDatabase(
            "rowid allocator state row is missing",
        ))?;
    let device = DeviceId(
        row.0
            .try_into()
            .map_err(|_| Error::InvalidDatabase("rowid allocator device id is malformed"))?,
    );
    let active_slot = u64::try_from(row.1)
        .map_err(|_| Error::InvalidDatabase("rowid allocator active slot is malformed"))?;
    let leased_through = u32::try_from(row.2)
        .map_err(|_| Error::InvalidDatabase("rowid allocator lease is malformed"))?;
    Ok(DurableState {
        device,
        active_slot,
        leased_through,
    })
}

fn used_slots(connection: &Connection) -> Result<Vec<u64>> {
    let mut statement =
        connection.prepare(&format!("SELECT slot FROM {SLOTS_TABLE} ORDER BY slot"))?;
    statement
        .query_map((), |row| row.get::<_, i64>(0))?
        .map(|slot| {
            u64::try_from(slot?)
                .map_err(|_| Error::InvalidDatabase("rowid allocator slot is malformed"))
        })
        .collect()
}

fn insert_unused_slot(
    connection: &Connection,
    tag: u16,
    mut suffix: impl FnMut() -> Result<u32>,
) -> Result<u64> {
    for _ in 0..MAX_SLOT_ATTEMPTS {
        let slot = (u64::from(tag) << RANDOM_SLOT_BITS) | u64::from(suffix()? & RANDOM_SLOT_MASK);
        if connection.execute(
            &format!("INSERT OR IGNORE INTO {SLOTS_TABLE} (slot) VALUES (?1)"),
            [i64::try_from(slot).expect("rowid slot fits in i64")],
        )? == 1
        {
            return Ok(slot);
        }
    }
    Err(Error::Entropy(
        "could not allocate an unused rowid slot".into(),
    ))
}

fn random_suffix() -> Result<u32> {
    let mut bytes = [0; 4];
    getrandom::fill(&mut bytes).map_err(|error| Error::Entropy(error.to_string()))?;
    Ok(u32::from_be_bytes(bytes) & RANDOM_SLOT_MASK)
}

fn device_tag(device: DeviceId) -> u16 {
    for counter in 0_u32.. {
        let mut hash = Sha256::new();
        hash.update(DEVICE_TAG_DOMAIN);
        hash.update(device.0);
        hash.update(counter.to_be_bytes());
        let digest = hash.finalize();
        let tag = u16::from_be_bytes([digest[0], digest[1]]);
        if tag != 0 {
            return tag;
        }
    }
    unreachable!("a SHA-256 prefix eventually differs from zero")
}

fn slot_tag(slot: u64) -> u16 {
    (slot >> RANDOM_SLOT_BITS) as u16
}

fn compose(slot: u64, offset: u32) -> i64 {
    debug_assert!(slot > 0 && slot <= MAX_SLOT);
    debug_assert!(offset < SLOT_CAPACITY);
    i64::try_from((slot << OFFSET_BITS) | u64::from(offset))
        .expect("43-bit slot plus 20-bit offset is a positive i64")
}

fn validate_columns(
    connection: &Connection,
    table: &str,
    expected: &[(&str, &str, bool, u32)],
    message: &'static str,
) -> Result<()> {
    let mut statement = connection.prepare(&format!("PRAGMA table_info({table})"))?;
    let columns = statement
        .query_map((), |row| {
            Ok((
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, bool>(3)?,
                row.get::<_, u32>(5)?,
            ))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let expected = expected
        .iter()
        .map(|(name, kind, not_null, primary)| {
            ((*name).to_owned(), (*kind).to_owned(), *not_null, *primary)
        })
        .collect::<Vec<_>>();
    if columns != expected {
        return Err(Error::InvalidDatabase(message));
    }
    Ok(())
}

fn validate_without_rowid(connection: &Connection, table: &str) -> Result<()> {
    let sql: String = connection.query_row(
        "SELECT sql FROM sqlite_schema WHERE type = 'table' AND name = ?1",
        [table],
        |row| row.get(0),
    )?;
    if !sql.to_ascii_uppercase().contains("WITHOUT ROWID") {
        return Err(Error::InvalidDatabase(
            "rowid allocator tables must use WITHOUT ROWID",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn device(byte: u8) -> DeviceId {
        DeviceId([byte; 16])
    }

    #[test]
    fn leases_are_durable_disjoint_and_in_the_non_adoption_namespace() {
        let connection = Connection::open_in_memory().unwrap();
        initialize(&connection, device(7)).unwrap();

        let first = lease(&connection).unwrap();
        let second = lease(&connection).unwrap();
        assert_eq!(first.offsets(), 0..LEASE_SIZE);
        assert_eq!(second.offsets(), LEASE_SIZE..LEASE_SIZE * 2);
        assert_eq!(first.slot(), second.slot());
        assert!(first.clone().next_rowid().unwrap() >= (1_i64 << 47));
        validate(&connection).unwrap();
        validate_device(&connection, device(7)).unwrap();
    }

    #[test]
    fn restart_burns_an_exposed_lease_instead_of_reusing_it() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("leases.sqlite");
        let first = {
            let connection = Connection::open(&path).unwrap();
            initialize(&connection, device(3)).unwrap();
            lease(&connection).unwrap()
        };
        let connection = Connection::open(&path).unwrap();
        let second = lease(&connection).unwrap();

        assert_eq!(first.slot(), second.slot());
        assert_eq!(first.offsets().end, second.offsets().start);
    }

    #[test]
    fn exhausted_slots_rotate_and_used_slots_are_never_reselected() {
        let connection = Connection::open_in_memory().unwrap();
        initialize(&connection, device(9)).unwrap();
        let state = load_state(&connection).unwrap();
        connection
            .execute(
                &format!("UPDATE {STATE_TABLE} SET leased_through = ?1"),
                [SLOT_CAPACITY],
            )
            .unwrap();
        let old_suffix = (state.active_slot as u32) & RANDOM_SLOT_MASK;
        let new_suffix = old_suffix.wrapping_add(1) & RANDOM_SLOT_MASK;
        let mut candidates = [old_suffix, new_suffix].into_iter();
        let lease = lease_with(&connection, || Ok(candidates.next().unwrap())).unwrap();

        assert_ne!(lease.slot(), state.active_slot);
        assert_eq!(lease.offsets(), 0..LEASE_SIZE);
        assert_eq!(used_slots(&connection).unwrap().len(), 2);
    }

    #[test]
    fn validation_rejects_device_mismatch_and_corrupt_state() {
        let connection = Connection::open_in_memory().unwrap();
        initialize(&connection, device(1)).unwrap();
        assert!(matches!(
            validate_device(&connection, device(2)),
            Err(Error::InvalidDatabase(
                "rowid allocator belongs to another device"
            ))
        ));

        connection
            .execute(&format!("DELETE FROM {SLOTS_TABLE}"), ())
            .unwrap();
        assert!(matches!(
            validate(&connection),
            Err(Error::InvalidDatabase(
                "rowid allocator slots are malformed"
            ))
        ));
    }

    #[test]
    fn device_tags_are_stable_nonzero_and_slots_fit_positive_rowids() {
        assert_eq!(device_tag(device(4)), device_tag(device(4)));
        assert_ne!(device_tag(device(4)), 0);
        assert_ne!(device_tag(device(4)), device_tag(device(5)));
        assert_eq!(compose(MAX_SLOT, SLOT_CAPACITY - 1), i64::MAX);
    }
}
