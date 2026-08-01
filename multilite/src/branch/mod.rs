//! Private SQLite connections backed by immutable WAL snapshots.

#![cfg_attr(
    not(test),
    allow(dead_code, reason = "wired into managed transactions in a later batch")
)]

mod vfs;
mod wal;

pub mod changeset;
pub mod snapshot;

use std::collections::BTreeMap;
use std::fmt;
use std::fs::File;
use std::os::unix::fs::FileExt;

use parking_lot::Mutex;
use rusqlite::{Connection, OpenFlags};

use self::snapshot::{PinnedSnapshot, SnapshotPin, SqliteSnapshot};
use self::vfs::BranchVfs;

/// A read-only SQLite connection fixed at one committed WAL snapshot.
pub struct ReadBranch {
    connection: Option<Connection>,
    vfs: BranchVfs,
}

impl ReadBranch {
    pub fn open(snapshot: PinnedSnapshot) -> Result<Self, BranchError> {
        let database_path = snapshot.database_path().to_owned();
        let reader = SnapshotReader::open(snapshot)?;
        let vfs = BranchVfs::register(BranchImage::read_only(reader))?;
        let connection = Connection::open_with_flags_and_vfs(
            &database_path,
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

    pub fn snapshot(&self) -> &SqliteSnapshot {
        &self.vfs.image().reader.snapshot
    }
}

/// Memory budget for dirty pages before a writable branch spills to disk.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OverlayOptions {
    pub memory_limit: usize,
}

impl Default for OverlayOptions {
    fn default() -> Self {
        Self {
            memory_limit: 8 * 1024 * 1024,
        }
    }
}

/// A private writable SQLite image rooted at one committed WAL snapshot.
pub struct WritableBranch {
    connection: Option<Connection>,
    vfs: BranchVfs,
}

impl WritableBranch {
    pub fn open(snapshot: PinnedSnapshot, options: OverlayOptions) -> Result<Self, BranchError> {
        let database_path = snapshot.database_path().to_owned();
        let reader = SnapshotReader::open(snapshot)?;
        let vfs = BranchVfs::register(BranchImage::writable(reader, options))?;
        let connection = Connection::open_with_flags_and_vfs(
            &database_path,
            OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_NO_MUTEX,
            vfs.name(),
        )?;
        connection.execute_batch(
            "PRAGMA journal_mode = MEMORY;
             PRAGMA synchronous = OFF;
             PRAGMA temp_store = MEMORY;
             PRAGMA locking_mode = EXCLUSIVE;
             PRAGMA foreign_keys = ON;
             PRAGMA mmap_size = 0",
        )?;
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

    pub fn snapshot(&self) -> &SqliteSnapshot {
        &self.vfs.image().reader.snapshot
    }

    pub fn overlay_stats(&self) -> OverlayStats {
        self.vfs.image().overlay_stats()
    }
}

impl Drop for WritableBranch {
    fn drop(&mut self) {
        drop(self.connection.take());
    }
}

impl Drop for ReadBranch {
    fn drop(&mut self) {
        drop(self.connection.take());
    }
}

struct SnapshotReader {
    base: Box<dyn PageSource>,
    wal: Option<Box<dyn PageSource>>,
    snapshot: SqliteSnapshot,
    _pin: SnapshotPin,
}

trait PageSource: Send + Sync {
    fn read_at(&self, destination: &mut [u8], offset: u64) -> std::io::Result<usize>;
}

trait SpillStore: PageSource {
    fn write_at(&self, source: &[u8], offset: u64) -> std::io::Result<usize>;
}

impl PageSource for File {
    fn read_at(&self, destination: &mut [u8], offset: u64) -> std::io::Result<usize> {
        FileExt::read_at(self, destination, offset)
    }
}

impl SpillStore for File {
    fn write_at(&self, source: &[u8], offset: u64) -> std::io::Result<usize> {
        FileExt::write_at(self, source, offset)
    }
}

type SpillFactory = fn() -> std::io::Result<Box<dyn SpillStore>>;

fn create_spill_store() -> std::io::Result<Box<dyn SpillStore>> {
    tempfile::tempfile().map(|file| Box::new(file) as Box<dyn SpillStore>)
}

impl SnapshotReader {
    fn open(snapshot: PinnedSnapshot) -> std::io::Result<Self> {
        let database_path = snapshot.database_path().to_owned();
        let wal_path = snapshot.wal_path().to_owned();
        let (snapshot, pin) = snapshot.into_snapshot_and_pin();
        Self::open_parts(&database_path, &wal_path, snapshot, pin)
    }

    fn open_parts(
        database_path: &std::path::Path,
        wal_path: &std::path::Path,
        snapshot: SqliteSnapshot,
        pin: SnapshotPin,
    ) -> std::io::Result<Self> {
        let base = File::open(database_path)?;
        let wal = snapshot
            .wal()
            .map(|_| File::open(wal_path).map(|file| Box::new(file) as Box<dyn PageSource>))
            .transpose()?;
        Ok(Self {
            base: Box::new(base),
            wal,
            snapshot,
            _pin: pin,
        })
    }

    #[cfg(test)]
    fn open_with_sources(
        snapshot: PinnedSnapshot,
        base: Box<dyn PageSource>,
        wal: Option<Box<dyn PageSource>>,
    ) -> Self {
        let (snapshot, pin) = snapshot.into_snapshot_and_pin();
        Self {
            base,
            wal,
            snapshot,
            _pin: pin,
        }
    }

    fn logical_size(&self) -> u64 {
        u64::from(self.snapshot.page_count()) * u64::from(self.snapshot.page_size())
    }

    /// Read an arbitrary byte range and report whether it was entirely present.
    fn read_at(&self, offset: u64, destination: &mut [u8]) -> std::io::Result<bool> {
        destination.fill(0);
        let logical_size = self.logical_size();
        let available = logical_size.saturating_sub(offset);
        let requested = destination.len() as u64;
        let readable = available.min(requested) as usize;
        let page_size = u64::from(self.snapshot.page_size());
        let mut copied = 0_usize;

        while copied < readable {
            let position = offset + copied as u64;
            let page = u32::try_from(position / page_size + 1).map_err(|_| {
                std::io::Error::new(std::io::ErrorKind::InvalidInput, "page number overflow")
            })?;
            let within_page = position % page_size;
            let amount = (page_size - within_page).min((readable - copied) as u64) as usize;
            let (source, source_offset) = match self.snapshot.frame_for(page) {
                Some(frame) => (
                    self.wal
                        .as_ref()
                        .expect("WAL-backed snapshot retains its WAL source"),
                    frame.data_offset() + within_page,
                ),
                None => (&self.base, u64::from(page - 1) * page_size + within_page),
            };
            if !read_exact_at(
                source.as_ref(),
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

struct BranchImage {
    reader: SnapshotReader,
    overlay: Option<Mutex<PageOverlay>>,
}

impl BranchImage {
    fn read_only(reader: SnapshotReader) -> Self {
        Self {
            reader,
            overlay: None,
        }
    }

    fn writable(reader: SnapshotReader, options: OverlayOptions) -> Self {
        let logical_size = reader.logical_size();
        let page_size = reader.snapshot.page_size() as usize;
        Self {
            reader,
            overlay: Some(Mutex::new(PageOverlay::new(
                page_size,
                logical_size,
                options.memory_limit,
            ))),
        }
    }

    fn writable_overlay(&self) -> std::io::Result<&Mutex<PageOverlay>> {
        self.overlay.as_ref().ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::PermissionDenied, "branch is read-only")
        })
    }

    fn is_writable(&self) -> bool {
        self.overlay.is_some()
    }

    fn logical_size(&self) -> u64 {
        self.overlay.as_ref().map_or_else(
            || self.reader.logical_size(),
            |overlay| overlay.lock().logical_size,
        )
    }

    fn read_at(&self, offset: u64, destination: &mut [u8]) -> std::io::Result<bool> {
        let Some(overlay) = &self.overlay else {
            return self.reader.read_at(offset, destination);
        };
        overlay.lock().read_at(&self.reader, offset, destination)
    }

    fn write_at(&self, offset: u64, source: &[u8]) -> std::io::Result<()> {
        self.writable_overlay()?
            .lock()
            .write_at(&self.reader, offset, source)
    }

    fn truncate(&self, size: u64) -> std::io::Result<()> {
        self.writable_overlay()?.lock().truncate(size)
    }

    fn overlay_stats(&self) -> OverlayStats {
        self.overlay.as_ref().map_or(
            OverlayStats {
                dirty_pages: 0,
                memory_bytes: 0,
                spilled_pages: 0,
                logical_size: self.reader.logical_size(),
            },
            |overlay| overlay.lock().stats(),
        )
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OverlayStats {
    pub dirty_pages: usize,
    pub memory_bytes: usize,
    pub spilled_pages: usize,
    pub logical_size: u64,
}

struct PageOverlay {
    page_size: usize,
    logical_size: u64,
    snapshot_fallback_size: u64,
    memory_limit: usize,
    memory_bytes: usize,
    pages: BTreeMap<u32, OverlayPage>,
    spill: Option<SpillFile>,
    spill_factory: SpillFactory,
}

enum OverlayPage {
    Memory(Vec<u8>),
    Spilled { offset: u64 },
}

struct SpillFile {
    file: Box<dyn SpillStore>,
    next_offset: u64,
}

impl PageOverlay {
    fn new(page_size: usize, logical_size: u64, memory_limit: usize) -> Self {
        Self::new_with_spill_factory(page_size, logical_size, memory_limit, create_spill_store)
    }

    fn new_with_spill_factory(
        page_size: usize,
        logical_size: u64,
        memory_limit: usize,
        spill_factory: SpillFactory,
    ) -> Self {
        Self {
            page_size,
            logical_size,
            snapshot_fallback_size: logical_size,
            memory_limit,
            memory_bytes: 0,
            pages: BTreeMap::new(),
            spill: None,
            spill_factory,
        }
    }

    fn read_at(
        &self,
        reader: &SnapshotReader,
        offset: u64,
        destination: &mut [u8],
    ) -> std::io::Result<bool> {
        destination.fill(0);
        let available = self.logical_size.saturating_sub(offset);
        let readable = available.min(destination.len() as u64) as usize;
        let mut copied = 0_usize;
        while copied < readable {
            let position = offset + copied as u64;
            let page = self.page_number(position)?;
            let within = position % self.page_size as u64;
            let amount = (self.page_size as u64 - within).min((readable - copied) as u64) as usize;
            match self.pages.get(&page) {
                Some(OverlayPage::Memory(bytes)) => {
                    destination[copied..copied + amount]
                        .copy_from_slice(&bytes[within as usize..within as usize + amount]);
                }
                Some(OverlayPage::Spilled { offset }) => {
                    let spill = self.spill.as_ref().expect("spilled page owns a spill file");
                    if !read_exact_at(
                        spill.file.as_ref(),
                        *offset + within,
                        &mut destination[copied..copied + amount],
                    )? {
                        return Ok(false);
                    }
                }
                None => {
                    let source_offset = u64::from(page - 1) * self.page_size as u64 + within;
                    if source_offset >= self.snapshot_fallback_size
                        || !reader
                            .read_at(source_offset, &mut destination[copied..copied + amount])?
                    {
                        // New or previously truncated pages start as zeroes.
                        destination[copied..copied + amount].fill(0);
                    }
                }
            }
            copied += amount;
        }
        patch_rollback_header(offset, destination);
        Ok(readable == destination.len())
    }

    fn write_at(
        &mut self,
        reader: &SnapshotReader,
        offset: u64,
        source: &[u8],
    ) -> std::io::Result<()> {
        let end = offset.checked_add(source.len() as u64).ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::InvalidInput, "branch write overflows")
        })?;
        let mut copied = 0_usize;
        while copied < source.len() {
            let position = offset + copied as u64;
            let page = self.page_number(position)?;
            let within = position % self.page_size as u64;
            let amount =
                (self.page_size as u64 - within).min((source.len() - copied) as u64) as usize;
            self.write_page_slice(
                reader,
                page,
                within as usize,
                &source[copied..copied + amount],
            )?;
            copied += amount;
        }
        self.logical_size = self.logical_size.max(end);
        self.spill_if_needed()?;
        Ok(())
    }

    fn write_page_slice(
        &mut self,
        reader: &SnapshotReader,
        page: u32,
        offset: usize,
        source: &[u8],
    ) -> std::io::Result<()> {
        if !self.pages.contains_key(&page) {
            let mut bytes = vec![0; self.page_size];
            let page_offset = u64::from(page - 1) * self.page_size as u64;
            if page_offset < self.snapshot_fallback_size {
                let _ = reader.read_at(page_offset, &mut bytes)?;
            }
            self.memory_bytes += bytes.len();
            self.pages.insert(page, OverlayPage::Memory(bytes));
        }
        match self.pages.get_mut(&page).expect("page inserted above") {
            OverlayPage::Memory(bytes) => {
                bytes[offset..offset + source.len()].copy_from_slice(source);
            }
            OverlayPage::Spilled {
                offset: spill_offset,
            } => {
                let spill = self.spill.as_ref().expect("spilled page owns a spill file");
                write_all_at(spill.file.as_ref(), *spill_offset + offset as u64, source)?;
            }
        }
        Ok(())
    }

    fn truncate(&mut self, size: u64) -> std::io::Result<()> {
        if !size.is_multiple_of(self.page_size as u64) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "SQLite branch truncation must end at a page boundary",
            ));
        }
        let pages = size / self.page_size as u64;
        let first_removed = u32::try_from(pages.saturating_add(1)).map_err(|_| {
            std::io::Error::new(std::io::ErrorKind::InvalidInput, "page count overflow")
        })?;
        let removed = self.pages.split_off(&first_removed);
        self.memory_bytes = self.memory_bytes.saturating_sub(
            removed
                .values()
                .filter(|page| matches!(page, OverlayPage::Memory(_)))
                .count()
                * self.page_size,
        );
        self.logical_size = size;
        self.snapshot_fallback_size = self.snapshot_fallback_size.min(size);
        Ok(())
    }

    fn spill_if_needed(&mut self) -> std::io::Result<()> {
        if self.memory_bytes <= self.memory_limit {
            return Ok(());
        }
        if self.spill.is_none() {
            self.spill = Some(SpillFile {
                file: (self.spill_factory)()?,
                next_offset: 0,
            });
        }
        let mut replacements = Vec::new();
        let final_offset = {
            let spill = self.spill.as_ref().expect("created above");
            let mut next_offset = spill.next_offset;
            for (page_number, page) in &self.pages {
                let OverlayPage::Memory(bytes) = page else {
                    continue;
                };
                write_all_at(spill.file.as_ref(), next_offset, bytes)?;
                replacements.push((*page_number, next_offset, bytes.len()));
                next_offset += bytes.len() as u64;
            }
            next_offset
        };
        let spilled_bytes = replacements
            .iter()
            .map(|(_, _, length)| *length)
            .sum::<usize>();
        for (page_number, offset, _) in replacements {
            self.pages
                .insert(page_number, OverlayPage::Spilled { offset });
        }
        self.spill.as_mut().expect("created above").next_offset = final_offset;
        self.memory_bytes = self.memory_bytes.saturating_sub(spilled_bytes);
        Ok(())
    }

    fn page_number(&self, position: u64) -> std::io::Result<u32> {
        u32::try_from(position / self.page_size as u64 + 1).map_err(|_| {
            std::io::Error::new(std::io::ErrorKind::InvalidInput, "page number overflow")
        })
    }

    fn stats(&self) -> OverlayStats {
        OverlayStats {
            dirty_pages: self.pages.len(),
            memory_bytes: self.memory_bytes,
            spilled_pages: self
                .pages
                .values()
                .filter(|page| matches!(page, OverlayPage::Spilled { .. }))
                .count(),
            logical_size: self.logical_size,
        }
    }
}

fn write_all_at(file: &dyn SpillStore, mut offset: u64, mut source: &[u8]) -> std::io::Result<()> {
    while !source.is_empty() {
        let written = file.write_at(source, offset)?;
        if written == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::WriteZero,
                "failed to spill branch page",
            ));
        }
        offset += written as u64;
        source = &source[written..];
    }
    Ok(())
}

fn read_exact_at(
    source: &dyn PageSource,
    mut offset: u64,
    mut destination: &mut [u8],
) -> std::io::Result<bool> {
    while !destination.is_empty() {
        let read = source.read_at(destination, offset)?;
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

#[derive(Debug)]
pub enum BranchError {
    Io(std::io::Error),
    Sqlite(rusqlite::Error),
    SqliteVfs(i32),
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
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use rusqlite::OptionalExtension as _;

    use super::*;
    use crate::branch::snapshot::PinnedSnapshot;

    struct Fixture {
        directory: tempfile::TempDir,
        writer: Connection,
        generation: u64,
    }

    struct FailingPageSource;

    struct PartiallyFailingSpillStore {
        writes: AtomicUsize,
    }

    impl PageSource for FailingPageSource {
        fn read_at(&self, _destination: &mut [u8], _offset: u64) -> std::io::Result<usize> {
            Err(std::io::Error::other("injected branch page read failure"))
        }
    }

    impl PageSource for PartiallyFailingSpillStore {
        fn read_at(&self, _destination: &mut [u8], _offset: u64) -> std::io::Result<usize> {
            Ok(0)
        }
    }

    impl SpillStore for PartiallyFailingSpillStore {
        fn write_at(&self, source: &[u8], _offset: u64) -> std::io::Result<usize> {
            if self.writes.fetch_add(1, Ordering::Relaxed) == 0 {
                Ok(source.len())
            } else {
                Err(std::io::Error::other("injected spill write failure"))
            }
        }
    }

    fn partially_failing_spill_store() -> std::io::Result<Box<dyn SpillStore>> {
        Ok(Box::new(PartiallyFailingSpillStore {
            writes: AtomicUsize::new(0),
        }))
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

        fn snapshot(&self) -> PinnedSnapshot {
            PinnedSnapshot::capture(self.path(), self.wal_path()).unwrap()
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
        let first = ReadBranch::open(fixture.snapshot()).unwrap();

        fixture.advance(
            "UPDATE records SET value = 'new', payload = randomblob(7000) WHERE id = 1;
             DELETE FROM records WHERE id = 2;
             INSERT INTO records VALUES (3, 'later', randomblob(9000));",
        );
        let second = ReadBranch::open(fixture.snapshot()).unwrap();

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
        assert!(
            first.snapshot().wal().unwrap().max_frame()
                < second.snapshot().wal().unwrap().max_frame()
        );
    }

    #[test]
    fn many_connections_keep_independent_immutable_views() {
        let mut fixture = Fixture::new();
        fixture.advance("INSERT INTO records VALUES (1, 'one', randomblob(5000))");
        let branches = (0..12)
            .map(|_| ReadBranch::open(fixture.snapshot()).unwrap())
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
        let image = snapshot.snapshot().wal().unwrap().clone();
        let reader = SnapshotReader::open(snapshot).unwrap();
        let page_size = image.page_size() as u64;
        let offset = page_size - 13;
        let mut crossed = vec![0; 64];
        assert!(reader.read_at(offset, &mut crossed).unwrap());

        let expected = (0..crossed.len())
            .map(|index| {
                let position = offset + index as u64;
                let page = (position / page_size + 1) as u32;
                let within = position % page_size;
                let (source, source_offset) = match image.page_map().get(&page) {
                    Some(frame) => (reader.wal.as_ref().unwrap(), frame.data_offset() + within),
                    None => (&reader.base, u64::from(page - 1) * page_size + within),
                };
                let mut byte = [0];
                assert!(read_exact_at(source.as_ref(), source_offset, &mut byte).unwrap());
                byte[0]
            })
            .collect::<Vec<_>>();
        assert_eq!(crossed, expected);

        let mut end = vec![7; 16];
        assert!(!reader.read_at(reader.logical_size() - 4, &mut end).unwrap());
        assert_eq!(&end[4..], &[0; 12]);
    }

    #[test]
    fn partial_spill_failure_keeps_every_dirty_page_in_memory() {
        let mut fixture = Fixture::new();
        fixture.advance("INSERT INTO records VALUES (1, 'one', randomblob(8000))");
        let reader = SnapshotReader::open(fixture.snapshot()).unwrap();
        let page_size = reader.snapshot.page_size() as usize;
        let mut overlay = PageOverlay::new_with_spill_factory(
            page_size,
            reader.logical_size(),
            0,
            partially_failing_spill_store,
        );
        let mut source = vec![0x41; page_size * 2];
        source[page_size..].fill(0x42);

        let error = overlay.write_at(&reader, 0, &source).unwrap_err();
        assert_eq!(error.to_string(), "injected spill write failure");
        assert_eq!(
            overlay.stats(),
            OverlayStats {
                dirty_pages: 2,
                memory_bytes: page_size * 2,
                spilled_pages: 0,
                logical_size: reader.logical_size(),
            }
        );

        let mut recovered = vec![0; page_size * 2];
        assert!(overlay.read_at(&reader, 0, &mut recovered).unwrap());
        assert_eq!(recovered[100], 0x41);
        assert_eq!(recovered[page_size + 100], 0x42);
    }

    #[test]
    fn page_source_failures_surface_as_sqlite_io_errors() {
        let mut fixture = Fixture::new();
        fixture.advance("INSERT INTO records VALUES (1, 'one', randomblob(12000))");
        let snapshot = fixture.snapshot();
        let reader = SnapshotReader::open_with_sources(
            snapshot,
            Box::new(File::open(fixture.path()).unwrap()),
            Some(Box::new(FailingPageSource)),
        );
        let vfs = BranchVfs::register(BranchImage::read_only(reader)).unwrap();
        let result = Connection::open_with_flags_and_vfs(
            fixture.path(),
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
            vfs.name(),
        )
        .and_then(|connection| {
            connection.query_row("SELECT count(*) FROM records", (), |row| {
                row.get::<_, i64>(0)
            })
        });

        let error = result.unwrap_err();
        assert_eq!(
            error.sqlite_error_code(),
            Some(rusqlite::ErrorCode::SystemIoFailure)
        );
    }

    #[test]
    fn truncate_then_regrow_never_resurrects_snapshot_pages() {
        let mut fixture = Fixture::new();
        fixture.advance("INSERT INTO records VALUES (1, 'one', randomblob(20000))");
        let snapshot = fixture.snapshot();
        let image = snapshot.snapshot().wal().unwrap().clone();
        let reader = SnapshotReader::open(snapshot).unwrap();
        let page_size = image.page_size() as usize;
        assert!(reader.logical_size() >= (page_size * 3) as u64);
        let mut overlay = PageOverlay::new(page_size, reader.logical_size(), usize::MAX);

        overlay.truncate(page_size as u64).unwrap();
        overlay
            .write_at(&reader, (page_size * 2 + 17) as u64, &[0x5a])
            .unwrap();

        let mut removed_page = vec![0xff; page_size];
        assert!(
            overlay
                .read_at(&reader, page_size as u64, &mut removed_page)
                .unwrap()
        );
        assert_eq!(removed_page, vec![0; page_size]);

        let mut new_page = vec![0xff; 18];
        assert!(
            overlay
                .read_at(&reader, (page_size * 2) as u64, &mut new_page)
                .unwrap()
        );
        assert_eq!(&new_page[..17], &[0; 17]);
        assert_eq!(new_page[17], 0x5a);
    }

    #[test]
    fn branch_is_query_only_and_reports_its_snapshot_page_count() {
        let mut fixture = Fixture::new();
        fixture.advance("INSERT INTO records VALUES (1, 'one', randomblob(9000))");
        let snapshot = fixture.snapshot();
        let page_count = snapshot.snapshot().page_count();
        let branch = ReadBranch::open(snapshot).unwrap();

        assert_eq!(
            branch
                .connection()
                .query_row("PRAGMA page_count", (), |row| row.get::<_, u32>(0))
                .unwrap(),
            page_count
        );
        let mmap_size = branch
            .connection()
            .query_row("PRAGMA mmap_size", (), |row| row.get::<_, i64>(0))
            .optional()
            .unwrap();
        assert!(mmap_size.is_none_or(|size| size == 0));
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
            let branch = ReadBranch::open(fixture.snapshot()).unwrap();
            assert_eq!(
                branch
                    .connection()
                    .query_row("SELECT value FROM records", (), |row| row
                        .get::<_, String>(0))
                    .unwrap(),
                "before"
            );
            branch.snapshot().wal().unwrap().epoch()
        };

        fixture
            .writer
            .query_row("PRAGMA wal_checkpoint(TRUNCATE)", (), |_| Ok(()))
            .unwrap();
        fixture.advance("UPDATE records SET value = 'after'");
        let branch = ReadBranch::open(fixture.snapshot()).unwrap();
        assert_ne!(branch.snapshot().wal().unwrap().epoch(), old_epoch);
        assert_eq!(
            branch
                .connection()
                .query_row("SELECT value FROM records", (), |row| row
                    .get::<_, String>(0))
                .unwrap(),
            "after"
        );
    }

    #[test]
    fn companion_pin_preserves_live_branches_across_every_checkpoint_mode() {
        for mode in ["PASSIVE", "FULL", "RESTART", "TRUNCATE"] {
            let mut fixture = Fixture::new();
            fixture.advance(
                "INSERT INTO records VALUES (1, 'old', randomblob(200000));
                 INSERT INTO records VALUES (2, 'stable', randomblob(200000))",
            );
            let branch = ReadBranch::open(fixture.snapshot()).unwrap();
            fixture.advance(
                "UPDATE records SET value = 'new', payload = randomblob(200000) WHERE id = 1",
            );
            fixture.writer.busy_timeout(Duration::ZERO).unwrap();

            let checkpoint = format!("PRAGMA wal_checkpoint({mode})");
            let (busy, _, _) = fixture
                .writer
                .query_row(&checkpoint, (), |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, i64>(2)?,
                    ))
                })
                .unwrap();
            if mode != "PASSIVE" {
                assert_eq!(busy, 1, "{mode} reset should be held behind the reader");
            }
            assert_eq!(
                branch
                    .connection()
                    .query_row("SELECT value FROM records WHERE id = 1", (), |row| {
                        row.get::<_, String>(0)
                    })
                    .unwrap(),
                "old"
            );

            drop(branch);
            let (busy, _, _) = fixture
                .writer
                .query_row("PRAGMA wal_checkpoint(TRUNCATE)", (), |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, i64>(2)?,
                    ))
                })
                .unwrap();
            assert_eq!(busy, 0);
            assert_eq!(fs::metadata(fixture.wal_path()).unwrap().len(), 0);
        }
    }

    #[test]
    fn writable_branch_runs_native_sql_without_touching_canonical_files() {
        let mut fixture = Fixture::new();
        fixture.advance(
            "CREATE TABLE native_rows (
                id INTEGER PRIMARY KEY,
                value TEXT NOT NULL DEFAULT 'default',
                payload BLOB NOT NULL DEFAULT x'CAFE',
                value_len INTEGER GENERATED ALWAYS AS (length(value)) STORED
             );
             CREATE TABLE audit (event TEXT NOT NULL);
             CREATE TRIGGER native_rows_insert AFTER INSERT ON native_rows BEGIN
                INSERT INTO audit VALUES (NEW.id || ':' || NEW.value);
             END;",
        );
        let database_before = fs::read(fixture.path()).unwrap();
        let wal_before = fs::read(fixture.wal_path()).unwrap();
        let branch = WritableBranch::open(fixture.snapshot(), OverlayOptions::default()).unwrap();
        assert_eq!(
            branch.snapshot().page_count(),
            branch
                .connection()
                .query_row("PRAGMA page_count", (), |row| row.get::<_, u32>(0))
                .unwrap()
        );

        branch.connection().execute_batch("BEGIN").unwrap();
        let inserted = branch
            .connection()
            .query_row(
                "INSERT INTO native_rows(id) VALUES (1)
                 RETURNING value, hex(payload), value_len",
                (),
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, i64>(2)?,
                    ))
                },
            )
            .unwrap();
        assert_eq!(inserted, ("default".into(), "CAFE".into(), 7));
        let updated = branch
            .connection()
            .query_row(
                "INSERT INTO native_rows(id, value) VALUES (1, 'updated')
                 ON CONFLICT(id) DO UPDATE SET value = excluded.value
                 RETURNING value_len",
                (),
                |row| row.get::<_, i64>(0),
            )
            .unwrap();
        assert_eq!(updated, 7);
        branch
            .connection()
            .execute_batch(
                "SAVEPOINT discard_me;
                 INSERT INTO native_rows(id, value) VALUES (2, 'discarded');
                 ROLLBACK TO discard_me;
                 RELEASE discard_me;
                 COMMIT;",
            )
            .unwrap();

        assert_eq!(
            branch
                .connection()
                .query_row("SELECT id || ':' || value FROM native_rows", (), |row| {
                    row.get::<_, String>(0)
                })
                .unwrap(),
            "1:updated"
        );
        assert_eq!(
            branch
                .connection()
                .query_row("SELECT group_concat(event, ',') FROM audit", (), |row| {
                    row.get::<_, String>(0)
                })
                .unwrap(),
            "1:default"
        );
        assert!(branch.overlay_stats().dirty_pages > 0);
        assert_eq!(fs::read(fixture.path()).unwrap(), database_before);
        assert_eq!(fs::read(fixture.wal_path()).unwrap(), wal_before);
        assert!(!fixture.path().with_extension("sqlite-journal").exists());
        assert_eq!(
            fixture
                .writer
                .query_row("SELECT count(*) FROM native_rows", (), |row| row
                    .get::<_, i64>(0))
                .unwrap(),
            0
        );
    }

    #[test]
    fn dirty_pages_spill_and_remain_queryable() {
        let mut fixture = Fixture::new();
        fixture.advance("CREATE TABLE large_rows (id INTEGER PRIMARY KEY, payload BLOB)");
        let branch =
            WritableBranch::open(fixture.snapshot(), OverlayOptions { memory_limit: 1 }).unwrap();
        branch
            .connection()
            .execute("INSERT INTO large_rows VALUES (1, randomblob(200000))", ())
            .unwrap();

        let stats = branch.overlay_stats();
        assert!(stats.dirty_pages > 10);
        assert_eq!(stats.memory_bytes, 0);
        assert_eq!(stats.spilled_pages, stats.dirty_pages);
        assert_eq!(
            branch
                .connection()
                .query_row("SELECT length(payload) FROM large_rows", (), |row| {
                    row.get::<_, i64>(0)
                })
                .unwrap(),
            200_000
        );
        assert_eq!(
            fixture
                .writer
                .query_row("SELECT count(*) FROM large_rows", (), |row| row
                    .get::<_, i64>(0))
                .unwrap(),
            0
        );
    }

    #[test]
    fn transaction_rollback_restores_the_private_snapshot() {
        let mut fixture = Fixture::new();
        fixture.advance("INSERT INTO records VALUES (1, 'original', x'01')");
        let branch = WritableBranch::open(fixture.snapshot(), OverlayOptions::default()).unwrap();
        branch
            .connection()
            .execute_batch(
                "BEGIN;
                 UPDATE records SET value = 'changed' WHERE id = 1;
                 INSERT INTO records VALUES (2, 'new', x'02');
                 ROLLBACK;",
            )
            .unwrap();

        assert_eq!(
            branch
                .connection()
                .query_row(
                    "SELECT group_concat(id || ':' || value) FROM records",
                    (),
                    |row| { row.get::<_, String>(0) }
                )
                .unwrap(),
            "1:original"
        );
    }

    #[test]
    fn writable_branches_do_not_lock_or_observe_each_other() {
        let mut fixture = Fixture::new();
        fixture.advance("CREATE TABLE proposals (id INTEGER PRIMARY KEY, value TEXT)");
        let first = WritableBranch::open(fixture.snapshot(), OverlayOptions::default()).unwrap();
        let second = WritableBranch::open(fixture.snapshot(), OverlayOptions::default()).unwrap();

        first
            .connection()
            .execute("INSERT INTO proposals VALUES (1, 'first')", ())
            .unwrap();
        second
            .connection()
            .execute("INSERT INTO proposals VALUES (1, 'second')", ())
            .unwrap();
        assert_eq!(
            first
                .connection()
                .query_row("SELECT value FROM proposals", (), |row| row
                    .get::<_, String>(0))
                .unwrap(),
            "first"
        );
        assert_eq!(
            second
                .connection()
                .query_row("SELECT value FROM proposals", (), |row| row
                    .get::<_, String>(0))
                .unwrap(),
            "second"
        );
        assert_eq!(
            fixture
                .writer
                .query_row("SELECT count(*) FROM proposals", (), |row| row
                    .get::<_, i64>(0))
                .unwrap(),
            0
        );
    }
}
