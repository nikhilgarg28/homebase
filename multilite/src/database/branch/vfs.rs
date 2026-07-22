//! Raw SQLite VFS adapter for private branch images.

use std::ffi::{CStr, CString, c_char, c_int, c_void};
use std::mem::{align_of, size_of};
use std::ptr;

use rusqlite::ffi;
use uuid::Uuid;

use super::{BranchError, BranchImage};

struct VfsContext {
    base_vfs: *mut ffi::sqlite3_vfs,
    image: BranchImage,
}

// SQLite's default VFS has process lifetime, while the reader is immutable and
// uses positional file reads. The raw pointer is never dereferenced after the
// registered wrapper has been unregistered.
unsafe impl Send for VfsContext {}
unsafe impl Sync for VfsContext {}

pub(super) struct BranchVfs {
    context: Box<VfsContext>,
    vfs: Box<ffi::sqlite3_vfs>,
    name: CString,
}

// Boxed callback state remains at stable addresses when this owner moves.
unsafe impl Send for BranchVfs {}

impl BranchVfs {
    pub(super) fn register(image: BranchImage) -> Result<Self, BranchError> {
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
        let mut context = Box::new(VfsContext { base_vfs, image });
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

    pub(super) fn name(&self) -> &CStr {
        &self.name
    }

    pub(super) fn image(&self) -> &BranchImage {
        &self.context.image
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
    if is_hidden_aux_name(name) {
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
        #[allow(clippy::unused_unit, reason = "some SQLite VFS callbacks return C void")]
        unsafe extern "C" fn $function(vfs: *mut ffi::sqlite3_vfs, $($name: $type),*) -> $return {
            let context = unsafe { vfs_context(vfs) };
            let Some(callback) = (unsafe { (*context.base_vfs).$field }) else {
                return $fallback;
            };
            unsafe { callback(context.base_vfs, $($name),*) }
        }
    };
}

delegate_vfs!(vfs_full_pathname, xFullPathname, (name: *const c_char, output_size: c_int, output: *mut c_char) -> c_int, ffi::SQLITE_IOERR);
delegate_vfs!(vfs_dl_open, xDlOpen, (name: *const c_char) -> *mut c_void, ptr::null_mut());
delegate_vfs!(vfs_dl_error, xDlError, (size: c_int, message: *mut c_char) -> (), {});
delegate_vfs!(vfs_dl_close, xDlClose, (handle: *mut c_void) -> (), {});
delegate_vfs!(vfs_randomness, xRandomness, (size: c_int, output: *mut c_char) -> c_int, 0);
delegate_vfs!(vfs_sleep, xSleep, (microseconds: c_int) -> c_int, 0);
delegate_vfs!(vfs_current_time, xCurrentTime, (time: *mut f64) -> c_int, ffi::SQLITE_IOERR);
delegate_vfs!(vfs_get_last_error, xGetLastError, (size: c_int, message: *mut c_char) -> c_int, 0);
delegate_vfs!(vfs_current_time_i64, xCurrentTimeInt64, (time: *mut ffi::sqlite3_int64) -> c_int, ffi::SQLITE_IOERR);
delegate_vfs!(vfs_set_system_call, xSetSystemCall, (name: *const c_char, call: ffi::sqlite3_syscall_ptr) -> c_int, ffi::SQLITE_NOTFOUND);
delegate_vfs!(vfs_next_system_call, xNextSystemCall, (name: *const c_char) -> *const c_char, ptr::null());

unsafe extern "C" fn vfs_get_system_call(
    vfs: *mut ffi::sqlite3_vfs,
    name: *const c_char,
) -> ffi::sqlite3_syscall_ptr {
    let context = unsafe { vfs_context(vfs) };
    let callback = (unsafe { (*context.base_vfs).xGetSystemCall })?;
    unsafe { callback(context.base_vfs, name) }
}

unsafe extern "C" fn vfs_access(
    vfs: *mut ffi::sqlite3_vfs,
    name: *const c_char,
    flags: c_int,
    result: *mut c_int,
) -> c_int {
    let context = unsafe { vfs_context(vfs) };
    if is_hidden_aux_name(name) {
        unsafe { *result = 0 };
        return ffi::SQLITE_OK;
    }
    let Some(access) = (unsafe { (*context.base_vfs).xAccess }) else {
        return ffi::SQLITE_IOERR;
    };
    unsafe { access(context.base_vfs, name, flags, result) }
}

unsafe extern "C" fn vfs_delete(
    vfs: *mut ffi::sqlite3_vfs,
    name: *const c_char,
    sync_dir: c_int,
) -> c_int {
    if is_hidden_aux_name(name) {
        return ffi::SQLITE_OK;
    }
    let context = unsafe { vfs_context(vfs) };
    let Some(delete) = (unsafe { (*context.base_vfs).xDelete }) else {
        return ffi::SQLITE_IOERR;
    };
    unsafe { delete(context.base_vfs, name, sync_dir) }
}

fn is_hidden_aux_name(name: *const c_char) -> bool {
    [b"-wal".as_slice(), b"-shm", b"-journal"]
        .into_iter()
        .any(|suffix| has_name_suffix(name, suffix))
}

fn has_name_suffix(name: *const c_char, suffix: &[u8]) -> bool {
    if name.is_null() {
        return false;
    }
    unsafe { CStr::from_ptr(name) }.to_bytes().ends_with(suffix)
}

unsafe extern "C" fn vfs_dl_sym(
    vfs: *mut ffi::sqlite3_vfs,
    handle: *mut c_void,
    symbol: *const c_char,
) -> Option<unsafe extern "C" fn(*mut ffi::sqlite3_vfs, *mut c_void, *const c_char)> {
    let context = unsafe { vfs_context(vfs) };
    let callback = (unsafe { (*context.base_vfs).xDlSym })?;
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
    match context.image.read_at(offset as u64, destination) {
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
        if amount < 0 || offset < 0 {
            return ffi::SQLITE_IOERR_WRITE;
        }
        let source = unsafe { std::slice::from_raw_parts(source.cast(), amount as usize) };
        let context = unsafe { &*branch.context };
        return match context.image.write_at(offset as u64, source) {
            Ok(()) => ffi::SQLITE_OK,
            Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => {
                ffi::SQLITE_READONLY
            }
            Err(_) => ffi::SQLITE_IOERR_WRITE,
        };
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
        if size < 0 {
            return ffi::SQLITE_IOERR_TRUNCATE;
        }
        let context = unsafe { &*branch.context };
        return match context.image.truncate(size as u64) {
            Ok(()) => ffi::SQLITE_OK,
            Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => {
                ffi::SQLITE_READONLY
            }
            Err(_) => ffi::SQLITE_IOERR_TRUNCATE,
        };
    }
    let original = branch.underlying;
    let Some(truncate) = (unsafe { &*(*original).pMethods }).xTruncate else {
        return ffi::SQLITE_IOERR_TRUNCATE;
    };
    unsafe { truncate(original, size) }
}

unsafe extern "C" fn io_sync(file: *mut ffi::sqlite3_file, flags: c_int) -> c_int {
    if unsafe { branch_file(file) }.main_database {
        return ffi::SQLITE_OK;
    }
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
        let Ok(size) = i64::try_from(context.image.logical_size()) else {
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

unsafe extern "C" fn io_lock(file: *mut ffi::sqlite3_file, level: c_int) -> c_int {
    if unsafe { branch_file(file) }.main_database {
        return ffi::SQLITE_OK;
    }
    unsafe { delegate_lock(file, level) }
}

unsafe fn delegate_lock(file: *mut ffi::sqlite3_file, level: c_int) -> c_int {
    let original = unsafe { original_file(file) };
    let Some(lock) = (unsafe { original_methods(file) }).xLock else {
        return ffi::SQLITE_IOERR_LOCK;
    };
    unsafe { lock(original, level) }
}

unsafe extern "C" fn io_unlock(file: *mut ffi::sqlite3_file, level: c_int) -> c_int {
    if unsafe { branch_file(file) }.main_database {
        return ffi::SQLITE_OK;
    }
    let original = unsafe { original_file(file) };
    let Some(unlock) = (unsafe { original_methods(file) }).xUnlock else {
        return ffi::SQLITE_IOERR_UNLOCK;
    };
    unsafe { unlock(original, level) }
}

unsafe extern "C" fn io_check_reserved(file: *mut ffi::sqlite3_file, result: *mut c_int) -> c_int {
    if unsafe { branch_file(file) }.main_database {
        unsafe { *result = 0 };
        return ffi::SQLITE_OK;
    }
    let original = unsafe { original_file(file) };
    let Some(check) = (unsafe { original_methods(file) }).xCheckReservedLock else {
        return ffi::SQLITE_IOERR_CHECKRESERVEDLOCK;
    };
    unsafe { check(original, result) }
}

unsafe extern "C" fn io_file_control(
    file: *mut ffi::sqlite3_file,
    operation: c_int,
    argument: *mut c_void,
) -> c_int {
    if unsafe { branch_file(file) }.main_database {
        return ffi::SQLITE_NOTFOUND;
    }
    let original = unsafe { original_file(file) };
    let Some(control) = (unsafe { original_methods(file) }).xFileControl else {
        return ffi::SQLITE_NOTFOUND;
    };
    unsafe { control(original, operation, argument) }
}

unsafe extern "C" fn io_sector_size(file: *mut ffi::sqlite3_file) -> c_int {
    if unsafe { branch_file(file) }.main_database {
        return 4096;
    }
    let original = unsafe { original_file(file) };
    (unsafe { original_methods(file) })
        .xSectorSize
        .map_or(0, |sector_size| unsafe { sector_size(original) })
}

unsafe extern "C" fn io_device_characteristics(file: *mut ffi::sqlite3_file) -> c_int {
    let branch = unsafe { branch_file(file) };
    let original = branch.underlying;
    let characteristics = (unsafe { original_methods(file) })
        .xDeviceCharacteristics
        .map_or(0, |callback| unsafe { callback(original) });
    if branch.main_database && !unsafe { &*branch.context }.image.is_writable() {
        characteristics | ffi::SQLITE_IOCAP_IMMUTABLE
    } else if branch.main_database {
        0
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
