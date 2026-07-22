//! Private SQLite connections backed by immutable WAL snapshots.

#![cfg_attr(
    not(test),
    allow(dead_code, reason = "wired into managed transactions in a later batch")
)]

use std::ffi::OsStr;
use std::ffi::{CStr, CString, c_char, c_int, c_void};
use std::fmt;
use std::fs::File;
use std::mem::{align_of, size_of};
use std::os::unix::ffi::OsStrExt as _;
use std::os::unix::fs::FileExt as _;
use std::path::{Path, PathBuf};
use std::ptr;
use std::sync::Arc;

use rusqlite::ffi;
use rusqlite::{Connection, OpenFlags};
use uuid::Uuid;

use super::wal::SnapshotDescriptor;

/// A read-only SQLite connection fixed at one committed WAL snapshot.
pub struct ReadBranch {
    connection: Option<Connection>,
    vfs: BranchVfs,
}

impl ReadBranch {
    pub fn open(
        database_path: impl AsRef<Path>,
        wal_path: impl AsRef<Path>,
        snapshot: SnapshotDescriptor,
    ) -> Result<Self, BranchError> {
        let database_path = database_path.as_ref();
        let reader = SnapshotReader::open(database_path, wal_path.as_ref(), snapshot)?;
        let vfs = BranchVfs::register(reader)?;
        let connection = Connection::open_with_flags_and_vfs(
            database_path,
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
            vfs.name(),
        )?;
        connection.execute_batch("PRAGMA query_only = ON; PRAGMA mmap_size = 0")?;
        Ok(Self {
            connection: Some(connection),
            vfs,
        })
    }

    pub fn connection(&self) -> &Connection {
        self.connection
            .as_ref()
            .expect("branch connection remains open until drop")
    }

    pub fn snapshot(&self) -> &SnapshotDescriptor {
        &self.vfs.context.reader.snapshot
    }
}

impl Drop for ReadBranch {
    fn drop(&mut self) {
        drop(self.connection.take());
    }
}

struct SnapshotReader {
    database_path: PathBuf,
    base: File,
    wal: File,
    snapshot: SnapshotDescriptor,
}

impl SnapshotReader {
    fn open(
        database_path: &Path,
        wal_path: &Path,
        snapshot: SnapshotDescriptor,
    ) -> std::io::Result<Self> {
        Ok(Self {
            database_path: database_path.to_owned(),
            base: File::open(database_path)?,
            wal: File::open(wal_path)?,
            snapshot,
        })
    }

    fn logical_size(&self) -> u64 {
        u64::from(self.snapshot.page_count) * u64::from(self.snapshot.page_size)
    }

    /// Read an arbitrary byte range and report whether it was entirely present.
    fn read_at(&self, offset: u64, destination: &mut [u8]) -> std::io::Result<bool> {
        destination.fill(0);
        let logical_size = self.logical_size();
        let available = logical_size.saturating_sub(offset);
        let requested = destination.len() as u64;
        let readable = available.min(requested) as usize;
        let page_size = u64::from(self.snapshot.page_size);
        let mut copied = 0_usize;

        while copied < readable {
            let position = offset + copied as u64;
            let page = u32::try_from(position / page_size + 1).map_err(|_| {
                std::io::Error::new(std::io::ErrorKind::InvalidInput, "page number overflow")
            })?;
            let within_page = position % page_size;
            let amount = (page_size - within_page).min((readable - copied) as u64) as usize;
            let (source, source_offset) = match self.snapshot.page_map.get(&page) {
                Some(frame) => (&self.wal, frame.data_offset() + within_page),
                None => (&self.base, u64::from(page - 1) * page_size + within_page),
            };
            if !read_exact_at(
                source,
                source_offset,
                &mut destination[copied..copied + amount],
            )? {
                patch_rollback_header(offset, destination);
                return Ok(false);
            }
            copied += amount;
        }

        // The branch presents its private image as a rollback-mode database so
        // SQLite never follows the canonical file's live WAL or wal-index.
        patch_rollback_header(offset, destination);
        Ok(readable == destination.len())
    }
}

fn read_exact_at(
    file: &File,
    mut offset: u64,
    mut destination: &mut [u8],
) -> std::io::Result<bool> {
    while !destination.is_empty() {
        let read = file.read_at(destination, offset)?;
        if read == 0 {
            return Ok(false);
        }
        offset += read as u64;
        destination = &mut destination[read..];
    }
    Ok(true)
}

fn patch_rollback_header(offset: u64, bytes: &mut [u8]) {
    for header_offset in [18_u64, 19] {
        if let Some(index) = header_offset.checked_sub(offset)
            && let Ok(index) = usize::try_from(index)
            && let Some(byte) = bytes.get_mut(index)
        {
            *byte = 1;
        }
    }
}

struct VfsContext {
    base_vfs: *mut ffi::sqlite3_vfs,
    reader: Arc<SnapshotReader>,
}

// SQLite's default VFS has process lifetime, while the reader is immutable and
// uses positional file reads. The raw pointer is never dereferenced after the
// registered wrapper has been unregistered.
unsafe impl Send for VfsContext {}
unsafe impl Sync for VfsContext {}

struct BranchVfs {
    context: Box<VfsContext>,
    vfs: Box<ffi::sqlite3_vfs>,
    name: CString,
}

// Boxed callback state remains at stable addresses when this owner moves.
unsafe impl Send for BranchVfs {}

impl BranchVfs {
    fn register(reader: SnapshotReader) -> Result<Self, BranchError> {
        let initialize = unsafe { ffi::sqlite3_initialize() };
        if initialize != ffi::SQLITE_OK {
            return Err(BranchError::SqliteVfs(initialize));
        }
        let base_vfs = unsafe { ffi::sqlite3_vfs_find(ptr::null()) };
        if base_vfs.is_null() {
            return Err(BranchError::MissingDefaultVfs);
        }
        let name = CString::new(format!("multilite-branch-{}", Uuid::new_v4()))
            .expect("generated VFS names contain no NUL bytes");
        let mut context = Box::new(VfsContext {
            base_vfs,
            reader: Arc::new(reader),
        });
        let os_file_size = branch_file_size(unsafe { (*base_vfs).szOsFile })?;
        let mut vfs = Box::new(ffi::sqlite3_vfs {
            iVersion: unsafe { (*base_vfs).iVersion },
            szOsFile: os_file_size,
            mxPathname: unsafe { (*base_vfs).mxPathname },
            pNext: ptr::null_mut(),
            zName: name.as_ptr(),
            pAppData: (&mut *context as *mut VfsContext).cast(),
            xOpen: Some(vfs_open),
            xDelete: Some(vfs_delete),
            xAccess: Some(vfs_access),
            xFullPathname: Some(vfs_full_pathname),
            xDlOpen: Some(vfs_dl_open),
            xDlError: Some(vfs_dl_error),
            xDlSym: Some(vfs_dl_sym),
            xDlClose: Some(vfs_dl_close),
            xRandomness: Some(vfs_randomness),
            xSleep: Some(vfs_sleep),
            xCurrentTime: Some(vfs_current_time),
            xGetLastError: Some(vfs_get_last_error),
            xCurrentTimeInt64: Some(vfs_current_time_i64),
            xSetSystemCall: Some(vfs_set_system_call),
            xGetSystemCall: Some(vfs_get_system_call),
            xNextSystemCall: Some(vfs_next_system_call),
        });
        let registered = unsafe { ffi::sqlite3_vfs_register(&mut *vfs, 0) };
        if registered != ffi::SQLITE_OK {
            return Err(BranchError::SqliteVfs(registered));
        }
        Ok(Self { context, vfs, name })
    }

    fn name(&self) -> &CStr {
        &self.name
    }
}

impl Drop for BranchVfs {
    fn drop(&mut self) {
        let _ = unsafe { ffi::sqlite3_vfs_unregister(&mut *self.vfs) };
    }
}

fn branch_file_size(base_size: c_int) -> Result<c_int, BranchError> {
    let offset = underlying_offset();
    let base_size = usize::try_from(base_size).map_err(|_| BranchError::VfsFileTooLarge)?;
    c_int::try_from(offset + base_size).map_err(|_| BranchError::VfsFileTooLarge)
}

const fn underlying_offset() -> usize {
    let alignment = align_of::<ffi::sqlite3_file>();
    (size_of::<BranchFile>() + alignment - 1) & !(alignment - 1)
}

#[repr(C)]
struct BranchFile {
    base: ffi::sqlite3_file,
    underlying: *mut ffi::sqlite3_file,
    context: *const VfsContext,
    main_database: bool,
}

unsafe fn branch_file<'a>(file: *mut ffi::sqlite3_file) -> &'a mut BranchFile {
    unsafe { &mut *file.cast::<BranchFile>() }
}

unsafe fn original_file(file: *mut ffi::sqlite3_file) -> *mut ffi::sqlite3_file {
    unsafe { branch_file(file).underlying }
}

unsafe fn original_methods<'a>(file: *mut ffi::sqlite3_file) -> &'a ffi::sqlite3_io_methods {
    let original = unsafe { original_file(file) };
    unsafe { &*(*original).pMethods }
}

unsafe fn vfs_context<'a>(vfs: *mut ffi::sqlite3_vfs) -> &'a VfsContext {
    unsafe { &*(*vfs).pAppData.cast::<VfsContext>() }
}

unsafe extern "C" fn vfs_open(
    vfs: *mut ffi::sqlite3_vfs,
    name: ffi::sqlite3_filename,
    file: *mut ffi::sqlite3_file,
    flags: c_int,
    output_flags: *mut c_int,
) -> c_int {
    let context = unsafe { vfs_context(vfs) };
    if flags & ffi::SQLITE_OPEN_WAL != 0 && is_canonical_wal_name(context, name) {
        return ffi::SQLITE_CANTOPEN;
    }
    let underlying = unsafe { file.cast::<u8>().add(underlying_offset()).cast() };
    unsafe {
        ptr::write_bytes(file.cast::<u8>(), 0, (*vfs).szOsFile as usize);
        ptr::write(
            file.cast::<BranchFile>(),
            BranchFile {
                base: ffi::sqlite3_file {
                    pMethods: ptr::null(),
                },
                underlying,
                context,
                main_database: flags & ffi::SQLITE_OPEN_MAIN_DB != 0,
            },
        );
    }
    let Some(open) = (unsafe { (*context.base_vfs).xOpen }) else {
        return ffi::SQLITE_CANTOPEN;
    };
    let result = unsafe { open(context.base_vfs, name, underlying, flags, output_flags) };
    if result == ffi::SQLITE_OK {
        unsafe { (*file).pMethods = &BRANCH_IO_METHODS };
    }
    result
}

macro_rules! delegate_vfs {
    ($function:ident, $field:ident, ($($name:ident: $type:ty),*) -> $return:ty, $fallback:expr) => {
        unsafe extern "C" fn $function(vfs: *mut ffi::sqlite3_vfs, $($name: $type),*) -> $return {
            let context = unsafe { vfs_context(vfs) };
            let Some(callback) = (unsafe { (*context.base_vfs).$field }) else {
                return $fallback;
            };
            unsafe { callback(context.base_vfs, $($name),*) }
        }
    };
}

delegate_vfs!(vfs_delete, xDelete, (name: *const c_char, sync_dir: c_int) -> c_int, ffi::SQLITE_IOERR);
delegate_vfs!(vfs_full_pathname, xFullPathname, (name: *const c_char, output_size: c_int, output: *mut c_char) -> c_int, ffi::SQLITE_IOERR);
delegate_vfs!(vfs_dl_open, xDlOpen, (name: *const c_char) -> *mut c_void, ptr::null_mut());
delegate_vfs!(vfs_dl_error, xDlError, (size: c_int, message: *mut c_char) -> (), ());
delegate_vfs!(vfs_dl_close, xDlClose, (handle: *mut c_void) -> (), ());
delegate_vfs!(vfs_randomness, xRandomness, (size: c_int, output: *mut c_char) -> c_int, 0);
delegate_vfs!(vfs_sleep, xSleep, (microseconds: c_int) -> c_int, 0);
delegate_vfs!(vfs_current_time, xCurrentTime, (time: *mut f64) -> c_int, ffi::SQLITE_IOERR);
delegate_vfs!(vfs_get_last_error, xGetLastError, (size: c_int, message: *mut c_char) -> c_int, 0);
delegate_vfs!(vfs_current_time_i64, xCurrentTimeInt64, (time: *mut ffi::sqlite3_int64) -> c_int, ffi::SQLITE_IOERR);
delegate_vfs!(vfs_set_system_call, xSetSystemCall, (name: *const c_char, call: ffi::sqlite3_syscall_ptr) -> c_int, ffi::SQLITE_NOTFOUND);
delegate_vfs!(vfs_get_system_call, xGetSystemCall, (name: *const c_char) -> ffi::sqlite3_syscall_ptr, None);
delegate_vfs!(vfs_next_system_call, xNextSystemCall, (name: *const c_char) -> *const c_char, ptr::null());

unsafe extern "C" fn vfs_access(
    vfs: *mut ffi::sqlite3_vfs,
    name: *const c_char,
    flags: c_int,
    result: *mut c_int,
) -> c_int {
    let context = unsafe { vfs_context(vfs) };
    if is_canonical_wal_name(context, name) || is_canonical_shm_name(context, name) {
        unsafe { *result = 0 };
        return ffi::SQLITE_OK;
    }
    let Some(access) = (unsafe { (*context.base_vfs).xAccess }) else {
        return ffi::SQLITE_IOERR;
    };
    unsafe { access(context.base_vfs, name, flags, result) }
}

fn is_canonical_wal_name(context: &VfsContext, name: *const c_char) -> bool {
    is_canonical_aux_name(context, name, b"-wal")
}

fn is_canonical_shm_name(context: &VfsContext, name: *const c_char) -> bool {
    is_canonical_aux_name(context, name, b"-shm")
}

fn is_canonical_aux_name(context: &VfsContext, name: *const c_char, suffix: &[u8]) -> bool {
    if name.is_null() {
        return false;
    }
    let mut expected = context.reader.database_path.as_os_str().as_bytes().to_vec();
    expected.extend_from_slice(suffix);
    let actual = unsafe { CStr::from_ptr(name) }.to_bytes();
    OsStr::from_bytes(actual) == OsStr::from_bytes(&expected)
}

unsafe extern "C" fn vfs_dl_sym(
    vfs: *mut ffi::sqlite3_vfs,
    handle: *mut c_void,
    symbol: *const c_char,
) -> Option<unsafe extern "C" fn(*mut ffi::sqlite3_vfs, *mut c_void, *const c_char)> {
    let context = unsafe { vfs_context(vfs) };
    let Some(callback) = (unsafe { (*context.base_vfs).xDlSym }) else {
        return None;
    };
    unsafe { callback(context.base_vfs, handle, symbol) }
}

unsafe extern "C" fn io_close(file: *mut ffi::sqlite3_file) -> c_int {
    let original = unsafe { original_file(file) };
    let result = match unsafe { (*original).pMethods.as_ref() }.and_then(|methods| methods.xClose) {
        Some(close) => unsafe { close(original) },
        None => ffi::SQLITE_OK,
    };
    unsafe { (*file).pMethods = ptr::null() };
    result
}

unsafe extern "C" fn io_read(
    file: *mut ffi::sqlite3_file,
    destination: *mut c_void,
    amount: c_int,
    offset: ffi::sqlite3_int64,
) -> c_int {
    let branch = unsafe { branch_file(file) };
    if !branch.main_database {
        let original = branch.underlying;
        let Some(read) = (unsafe { &*(*original).pMethods }).xRead else {
            return ffi::SQLITE_IOERR_READ;
        };
        return unsafe { read(original, destination, amount, offset) };
    }
    if amount < 0 || offset < 0 {
        return ffi::SQLITE_IOERR_READ;
    }
    let destination =
        unsafe { std::slice::from_raw_parts_mut(destination.cast(), amount as usize) };
    let context = unsafe { &*branch.context };
    match context.reader.read_at(offset as u64, destination) {
        Ok(true) => ffi::SQLITE_OK,
        Ok(false) => ffi::SQLITE_IOERR_SHORT_READ,
        Err(_) => ffi::SQLITE_IOERR_READ,
    }
}

unsafe extern "C" fn io_write(
    file: *mut ffi::sqlite3_file,
    source: *const c_void,
    amount: c_int,
    offset: ffi::sqlite3_int64,
) -> c_int {
    let branch = unsafe { branch_file(file) };
    if branch.main_database {
        return ffi::SQLITE_READONLY;
    }
    let original = branch.underlying;
    let Some(write) = (unsafe { &*(*original).pMethods }).xWrite else {
        return ffi::SQLITE_IOERR_WRITE;
    };
    unsafe { write(original, source, amount, offset) }
}

unsafe extern "C" fn io_truncate(file: *mut ffi::sqlite3_file, size: ffi::sqlite3_int64) -> c_int {
    let branch = unsafe { branch_file(file) };
    if branch.main_database {
        return ffi::SQLITE_READONLY;
    }
    let original = branch.underlying;
    let Some(truncate) = (unsafe { &*(*original).pMethods }).xTruncate else {
        return ffi::SQLITE_IOERR_TRUNCATE;
    };
    unsafe { truncate(original, size) }
}

unsafe extern "C" fn io_sync(file: *mut ffi::sqlite3_file, flags: c_int) -> c_int {
    let original = unsafe { original_file(file) };
    let Some(sync) = (unsafe { &*(*original).pMethods }).xSync else {
        return ffi::SQLITE_OK;
    };
    unsafe { sync(original, flags) }
}

unsafe extern "C" fn io_file_size(
    file: *mut ffi::sqlite3_file,
    output: *mut ffi::sqlite3_int64,
) -> c_int {
    let branch = unsafe { branch_file(file) };
    if branch.main_database {
        let context = unsafe { &*branch.context };
        let Ok(size) = i64::try_from(context.reader.logical_size()) else {
            return ffi::SQLITE_IOERR_FSTAT;
        };
        unsafe { *output = size };
        return ffi::SQLITE_OK;
    }
    let original = branch.underlying;
    let Some(file_size) = (unsafe { &*(*original).pMethods }).xFileSize else {
        return ffi::SQLITE_IOERR_FSTAT;
    };
    unsafe { file_size(original, output) }
}

macro_rules! delegate_io {
    ($function:ident, $field:ident, ($($name:ident: $type:ty),*) -> $return:ty, $fallback:expr) => {
        unsafe extern "C" fn $function(file: *mut ffi::sqlite3_file, $($name: $type),*) -> $return {
            let original = unsafe { original_file(file) };
            let Some(callback) = (unsafe { original_methods(file) }).$field else {
                return $fallback;
            };
            unsafe { callback(original, $($name),*) }
        }
    };
}

delegate_io!(io_lock, xLock, (level: c_int) -> c_int, ffi::SQLITE_IOERR_LOCK);
delegate_io!(io_unlock, xUnlock, (level: c_int) -> c_int, ffi::SQLITE_IOERR_UNLOCK);
delegate_io!(io_check_reserved, xCheckReservedLock, (result: *mut c_int) -> c_int, ffi::SQLITE_IOERR_CHECKRESERVEDLOCK);
delegate_io!(io_file_control, xFileControl, (operation: c_int, argument: *mut c_void) -> c_int, ffi::SQLITE_NOTFOUND);
delegate_io!(io_sector_size, xSectorSize, () -> c_int, 0);

unsafe extern "C" fn io_device_characteristics(file: *mut ffi::sqlite3_file) -> c_int {
    let branch = unsafe { branch_file(file) };
    let original = branch.underlying;
    let characteristics = (unsafe { original_methods(file) })
        .xDeviceCharacteristics
        .map_or(0, |callback| unsafe { callback(original) });
    if branch.main_database {
        characteristics | ffi::SQLITE_IOCAP_IMMUTABLE
    } else {
        characteristics
    }
}

static BRANCH_IO_METHODS: ffi::sqlite3_io_methods = ffi::sqlite3_io_methods {
    iVersion: 1,
    xClose: Some(io_close),
    xRead: Some(io_read),
    xWrite: Some(io_write),
    xTruncate: Some(io_truncate),
    xSync: Some(io_sync),
    xFileSize: Some(io_file_size),
    xLock: Some(io_lock),
    xUnlock: Some(io_unlock),
    xCheckReservedLock: Some(io_check_reserved),
    xFileControl: Some(io_file_control),
    xSectorSize: Some(io_sector_size),
    xDeviceCharacteristics: Some(io_device_characteristics),
    xShmMap: None,
    xShmLock: None,
    xShmBarrier: None,
    xShmUnmap: None,
    xFetch: None,
    xUnfetch: None,
};

#[derive(Debug)]
pub enum BranchError {
    Io(std::io::Error),
    Sqlite(rusqlite::Error),
    SqliteVfs(c_int),
    MissingDefaultVfs,
    VfsFileTooLarge,
}

impl fmt::Display for BranchError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(f, "branch file: {error}"),
            Self::Sqlite(error) => write!(f, "branch SQLite connection: {error}"),
            Self::SqliteVfs(code) => write!(f, "branch VFS returned SQLite code {code}"),
            Self::MissingDefaultVfs => f.write_str("SQLite default VFS is unavailable"),
            Self::VfsFileTooLarge => f.write_str("SQLite default VFS file handle is too large"),
        }
    }
}

impl std::error::Error for BranchError {}

impl From<std::io::Error> for BranchError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<rusqlite::Error> for BranchError {
    fn from(error: rusqlite::Error) -> Self {
        Self::Sqlite(error)
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use homebase_core::tag::AdmissionSeq;

    use super::*;
    use crate::database::wal::WalParser;

    struct Fixture {
        directory: tempfile::TempDir,
        writer: Connection,
        generation: u64,
    }

    impl Fixture {
        fn new() -> Self {
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("branch.sqlite");
            let writer = Connection::open(path).unwrap();
            writer.pragma_update(None, "journal_mode", "WAL").unwrap();
            writer.pragma_update(None, "wal_autocheckpoint", 0).unwrap();
            writer
                .execute_batch(
                    "CREATE TABLE records (
                        id INTEGER PRIMARY KEY,
                        value TEXT NOT NULL,
                        payload BLOB NOT NULL
                    )",
                )
                .unwrap();
            Self {
                directory,
                writer,
                generation: 1,
            }
        }

        fn path(&self) -> PathBuf {
            self.directory.path().join("branch.sqlite")
        }

        fn wal_path(&self) -> PathBuf {
            self.directory.path().join("branch.sqlite-wal")
        }

        fn snapshot(&self) -> SnapshotDescriptor {
            let parsed = WalParser::parse(&fs::read(self.wal_path()).unwrap()).unwrap();
            SnapshotDescriptor::from_wal(
                self.generation,
                AdmissionSeq(self.generation),
                &parsed.snapshot.unwrap(),
            )
        }

        fn advance(&mut self, sql: &str) {
            self.writer.execute_batch(sql).unwrap();
            self.generation += 1;
        }
    }

    #[test]
    fn read_only_branch_retains_its_snapshot_while_the_writer_advances() {
        let mut fixture = Fixture::new();
        fixture.advance(
            "INSERT INTO records VALUES (1, 'old', zeroblob(6000));
             INSERT INTO records VALUES (2, 'stable', zeroblob(3000));",
        );
        let first =
            ReadBranch::open(fixture.path(), fixture.wal_path(), fixture.snapshot()).unwrap();

        fixture.advance(
            "UPDATE records SET value = 'new', payload = randomblob(7000) WHERE id = 1;
             DELETE FROM records WHERE id = 2;
             INSERT INTO records VALUES (3, 'later', randomblob(9000));",
        );
        let second =
            ReadBranch::open(fixture.path(), fixture.wal_path(), fixture.snapshot()).unwrap();

        assert_eq!(
            first
                .connection()
                .query_row(
                    "SELECT group_concat(id || ':' || value, ',') FROM records",
                    (),
                    |row| row.get::<_, String>(0)
                )
                .unwrap(),
            "1:old,2:stable"
        );
        assert_eq!(
            second
                .connection()
                .query_row(
                    "SELECT group_concat(id || ':' || value, ',') FROM records",
                    (),
                    |row| row.get::<_, String>(0)
                )
                .unwrap(),
            "1:new,3:later"
        );
        assert!(first.snapshot().max_frame < second.snapshot().max_frame);
    }

    #[test]
    fn many_connections_keep_independent_immutable_views() {
        let mut fixture = Fixture::new();
        fixture.advance("INSERT INTO records VALUES (1, 'one', randomblob(5000))");
        let old_snapshot = fixture.snapshot();
        let branches = (0..12)
            .map(|_| {
                ReadBranch::open(fixture.path(), fixture.wal_path(), old_snapshot.clone()).unwrap()
            })
            .collect::<Vec<_>>();

        fixture.advance(
            "INSERT INTO records VALUES (2, 'two', randomblob(5000));
             INSERT INTO records VALUES (3, 'three', randomblob(5000));",
        );
        for branch in branches {
            assert_eq!(
                branch
                    .connection()
                    .query_row("SELECT count(*) FROM records", (), |row| row
                        .get::<_, i64>(0))
                    .unwrap(),
                1
            );
        }
    }

    #[test]
    fn arbitrary_reads_cross_page_sources_and_respect_logical_size() {
        let mut fixture = Fixture::new();
        fixture.advance("INSERT INTO records VALUES (1, 'one', randomblob(12000))");
        let snapshot = fixture.snapshot();
        let reader =
            SnapshotReader::open(&fixture.path(), &fixture.wal_path(), snapshot.clone()).unwrap();
        let page_size = snapshot.page_size as u64;
        let offset = page_size - 13;
        let mut crossed = vec![0; 64];
        assert!(reader.read_at(offset, &mut crossed).unwrap());

        let expected = (0..crossed.len())
            .map(|index| {
                let position = offset + index as u64;
                let page = (position / page_size + 1) as u32;
                let within = position % page_size;
                let (file, source_offset) = match snapshot.page_map.get(&page) {
                    Some(frame) => (&reader.wal, frame.data_offset() + within),
                    None => (&reader.base, u64::from(page - 1) * page_size + within),
                };
                let mut byte = [0];
                assert!(read_exact_at(file, source_offset, &mut byte).unwrap());
                byte[0]
            })
            .collect::<Vec<_>>();
        assert_eq!(crossed, expected);

        let mut end = vec![7; 16];
        assert!(!reader.read_at(reader.logical_size() - 4, &mut end).unwrap());
        assert_eq!(&end[4..], &[0; 12]);
    }

    #[test]
    fn branch_is_query_only_and_reports_its_snapshot_page_count() {
        let mut fixture = Fixture::new();
        fixture.advance("INSERT INTO records VALUES (1, 'one', randomblob(9000))");
        let snapshot = fixture.snapshot();
        let branch =
            ReadBranch::open(fixture.path(), fixture.wal_path(), snapshot.clone()).unwrap();

        assert_eq!(
            branch
                .connection()
                .query_row("PRAGMA page_count", (), |row| row.get::<_, u32>(0))
                .unwrap(),
            snapshot.page_count
        );
        assert_eq!(
            branch
                .connection()
                .query_row("PRAGMA mmap_size", (), |row| row.get::<_, i64>(0))
                .unwrap(),
            0
        );
        assert!(
            branch
                .connection()
                .execute("INSERT INTO records VALUES (2, 'blocked', x'')", ())
                .is_err()
        );
    }

    #[test]
    fn a_reader_gap_allows_checkpoint_rotation_before_the_next_branch() {
        let mut fixture = Fixture::new();
        fixture.advance("INSERT INTO records VALUES (1, 'before', randomblob(9000))");
        let old_epoch = {
            let branch =
                ReadBranch::open(fixture.path(), fixture.wal_path(), fixture.snapshot()).unwrap();
            assert_eq!(
                branch
                    .connection()
                    .query_row("SELECT value FROM records", (), |row| row
                        .get::<_, String>(0))
                    .unwrap(),
                "before"
            );
            branch.snapshot().wal_epoch
        };

        fixture
            .writer
            .query_row("PRAGMA wal_checkpoint(TRUNCATE)", (), |_| Ok(()))
            .unwrap();
        fixture.advance("UPDATE records SET value = 'after'");
        let branch =
            ReadBranch::open(fixture.path(), fixture.wal_path(), fixture.snapshot()).unwrap();
        assert_ne!(branch.snapshot().wal_epoch, old_epoch);
        assert_eq!(
            branch
                .connection()
                .query_row("SELECT value FROM records", (), |row| row
                    .get::<_, String>(0))
                .unwrap(),
            "after"
        );
    }
}
