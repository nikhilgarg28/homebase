//! Recoverable SQLite WAL parsing and immutable committed page maps.

#![cfg_attr(
    not(test),
    allow(dead_code, reason = "consumed by the Branch VFS in the next batch")
)]

use std::collections::BTreeMap;
use std::fmt;
use std::fs::File;
use std::io;
use std::os::unix::fs::FileExt;
use std::sync::Arc;

const WAL_HEADER_SIZE: usize = 32;
const FRAME_HEADER_SIZE: usize = 24;
const WAL_MAGIC: u32 = 0x377f_0682;
const WAL_FORMAT_VERSION: u32 = 3_007_000;

/// Positional source used to observe one fixed-length WAL prefix.
pub trait WalSource {
    fn len(&self) -> io::Result<u64>;
    fn read_at(&self, offset: u64, destination: &mut [u8]) -> io::Result<usize>;
}

impl WalSource for File {
    fn len(&self) -> io::Result<u64> {
        Ok(self.metadata()?.len())
    }

    fn read_at(&self, offset: u64, destination: &mut [u8]) -> io::Result<usize> {
        FileExt::read_at(self, destination, offset)
    }
}

impl WalSource for &[u8] {
    fn len(&self) -> io::Result<u64> {
        Ok((**self).len() as u64)
    }

    fn read_at(&self, offset: u64, destination: &mut [u8]) -> io::Result<usize> {
        let offset = usize::try_from(offset)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "WAL offset is too large"))?;
        let Some(source) = (**self).get(offset..) else {
            return Ok(0);
        };
        let amount = source.len().min(destination.len());
        destination[..amount].copy_from_slice(&source[..amount]);
        Ok(amount)
    }
}

/// One WAL incarnation. A checkpoint reset rotates these salts.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct WalEpoch {
    salts: [u32; 2],
}

impl WalEpoch {
    pub fn salts(self) -> [u32; 2] {
        self.salts
    }
}

/// Location of the latest committed image of one database page.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WalFrame {
    frame: u32,
    data_offset: u64,
}

impl WalFrame {
    pub fn frame(self) -> u32 {
        self.frame
    }

    pub fn data_offset(self) -> u64 {
        self.data_offset
    }
}

/// Latest complete commit recovered from a WAL prefix.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WalSnapshot {
    epoch: WalEpoch,
    max_frame: u32,
    page_count: u32,
    page_size: u32,
    page_map: PageMap,
}

impl WalSnapshot {
    pub fn epoch(&self) -> WalEpoch {
        self.epoch
    }

    pub fn max_frame(&self) -> u32 {
        self.max_frame
    }

    pub fn page_count(&self) -> u32 {
        self.page_count
    }

    pub fn page_size(&self) -> u32 {
        self.page_size
    }

    pub fn page_map(&self) -> &PageMap {
        &self.page_map
    }
}

const PAGE_CHUNK_BITS: u32 = 8;
const PAGE_CHUNK_LEN: usize = 1 << PAGE_CHUNK_BITS;

/// Structurally shared latest-frame map grouped into 256-page chunks.
///
/// Publishing a committed snapshot clones only the root `Arc`. The first
/// later write copies the much smaller chunk directory and each touched chunk
/// at most once, while untouched chunks remain shared with older snapshots.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PageMap {
    chunks: Arc<BTreeMap<u32, Arc<PageChunk>>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct PageChunk {
    frames: Box<[Option<WalFrame>; PAGE_CHUNK_LEN]>,
}

impl Default for PageChunk {
    fn default() -> Self {
        Self {
            frames: Box::new([None; PAGE_CHUNK_LEN]),
        }
    }
}

impl PageMap {
    pub fn get(&self, page: &u32) -> Option<&WalFrame> {
        let (chunk, offset) = page_location(*page);
        self.chunks
            .get(&chunk)
            .and_then(|chunk| chunk.frames[offset].as_ref())
    }

    pub fn keys(&self) -> impl Iterator<Item = u32> + '_ {
        self.iter().map(|(page, _)| page)
    }

    pub fn values(&self) -> impl Iterator<Item = &WalFrame> + '_ {
        self.iter().map(|(_, frame)| frame)
    }

    fn insert(&mut self, page: u32, frame: WalFrame) {
        let (chunk, offset) = page_location(page);
        let chunks = Arc::make_mut(&mut self.chunks);
        let chunk = chunks.entry(chunk).or_default();
        Arc::make_mut(chunk).frames[offset] = Some(frame);
    }

    fn truncate(&mut self, page_count: u32) {
        let (last_chunk, last_offset) = page_location(page_count);
        let chunks = Arc::make_mut(&mut self.chunks);
        chunks.retain(|chunk, _| *chunk <= last_chunk);
        if let Some(chunk) = chunks.get_mut(&last_chunk) {
            Arc::make_mut(chunk).frames[last_offset + 1..].fill(None);
        }
    }

    fn iter(&self) -> impl Iterator<Item = (u32, &WalFrame)> + '_ {
        self.chunks.iter().flat_map(|(chunk_index, chunk)| {
            chunk
                .frames
                .iter()
                .enumerate()
                .filter_map(move |(offset, frame)| {
                    frame
                        .as_ref()
                        .map(|frame| ((*chunk_index << PAGE_CHUNK_BITS) | offset as u32, frame))
                })
        })
    }
}

fn page_location(page: u32) -> (u32, usize) {
    (
        page >> PAGE_CHUNK_BITS,
        (page & (PAGE_CHUNK_LEN as u32 - 1)) as usize,
    )
}

/// Condition of bytes after the last checksum-valid frame.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WalTail {
    Complete,
    Incomplete { bytes: usize },
    InvalidSalt { frame: u32 },
    InvalidChecksum { frame: u32 },
    InvalidPageNumber { frame: u32 },
}

/// Result of parsing one observed WAL prefix.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WalParse {
    pub snapshot: Option<WalSnapshot>,
    pub tail: WalTail,
    pub valid_bytes: usize,
}

/// Stateful parser used for both cold rebuilds and incremental extension.
#[derive(Default)]
pub struct WalParser {
    state: Option<ParserState>,
}

struct ParserState {
    header: WalHeader,
    next_frame: u32,
    next_offset: usize,
    checksum: [u32; 2],
    working_map: PageMap,
    committed: Option<WalSnapshot>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct WalHeader {
    epoch: WalEpoch,
    page_size: u32,
    checksum_order: ChecksumOrder,
    checksum: [u32; 2],
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ChecksumOrder {
    Little,
    Big,
}

impl WalParser {
    pub fn new() -> Self {
        Self::default()
    }

    /// Rebuild from scratch using the same state machine as incremental use.
    pub fn parse(bytes: &[u8]) -> Result<WalParse, WalError> {
        Self::new().refresh_source(&bytes)
    }

    /// Extend or reset this parser to match the complete observed WAL prefix.
    pub fn refresh(&mut self, bytes: &[u8]) -> Result<WalParse, WalError> {
        self.refresh_source(&bytes)
    }

    /// Extend or reset this parser by streaming one fixed-length WAL observation.
    pub fn refresh_source(&mut self, source: &impl WalSource) -> Result<WalParse, WalError> {
        let length = usize::try_from(source.len()?).map_err(|_| {
            WalError::from(io::Error::new(
                io::ErrorKind::FileTooLarge,
                "WAL is too large",
            ))
        })?;
        let mut header_bytes = [0; WAL_HEADER_SIZE];
        read_exact_at(source, 0, &mut header_bytes[..length.min(WAL_HEADER_SIZE)])?;
        if length < WAL_HEADER_SIZE {
            return Err(WalError::IncompleteHeader(length));
        }
        let header = parse_header(&header_bytes)?;
        let reset = self
            .state
            .as_ref()
            .is_none_or(|state| state.header != header || length < state.next_offset);
        if reset {
            self.state = Some(ParserState {
                header,
                next_frame: 1,
                next_offset: WAL_HEADER_SIZE,
                checksum: header.checksum,
                working_map: PageMap::default(),
                committed: None,
            });
        }

        let state = self.state.as_mut().expect("parser state initialized above");
        let frame_size = FRAME_HEADER_SIZE
            .checked_add(state.header.page_size as usize)
            .ok_or(WalError::InvalidPageSize(state.header.page_size))?;
        let mut frame = vec![0; frame_size];
        let mut tail = WalTail::Complete;

        while state.next_offset < length {
            let remaining = length - state.next_offset;
            if remaining < frame_size {
                tail = WalTail::Incomplete { bytes: remaining };
                break;
            }
            read_exact_at(source, state.next_offset as u64, &mut frame)?;
            let frame_number = state.next_frame;
            let page_number = be_u32(&frame[0..4]);
            let page_count = be_u32(&frame[4..8]);
            let salts = [be_u32(&frame[8..12]), be_u32(&frame[12..16])];
            if salts != state.header.epoch.salts {
                tail = WalTail::InvalidSalt {
                    frame: frame_number,
                };
                break;
            }
            if page_number == 0 {
                tail = WalTail::InvalidPageNumber {
                    frame: frame_number,
                };
                break;
            }

            let mut checksum = state.checksum;
            checksum_bytes(state.header.checksum_order, &frame[..8], &mut checksum);
            checksum_bytes(
                state.header.checksum_order,
                &frame[FRAME_HEADER_SIZE..],
                &mut checksum,
            );
            let stored = [be_u32(&frame[16..20]), be_u32(&frame[20..24])];
            if checksum != stored {
                tail = WalTail::InvalidChecksum {
                    frame: frame_number,
                };
                break;
            }

            let data_offset = (state.next_offset + FRAME_HEADER_SIZE) as u64;
            state.working_map.insert(
                page_number,
                WalFrame {
                    frame: frame_number,
                    data_offset,
                },
            );
            state.checksum = checksum;
            state.next_frame += 1;
            state.next_offset += frame_size;

            if page_count != 0 {
                state.working_map.truncate(page_count);
                state.committed = Some(WalSnapshot {
                    epoch: state.header.epoch,
                    max_frame: frame_number,
                    page_count,
                    page_size: state.header.page_size,
                    page_map: state.working_map.clone(),
                });
            }
        }

        Ok(WalParse {
            snapshot: state.committed.clone(),
            tail,
            valid_bytes: state.next_offset,
        })
    }

    pub fn clear(&mut self) {
        self.state = None;
    }
}

fn read_exact_at(
    source: &impl WalSource,
    mut offset: u64,
    mut destination: &mut [u8],
) -> Result<(), WalError> {
    while !destination.is_empty() {
        let read = source.read_at(offset, destination)?;
        if read == 0 {
            return Err(WalError::from(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "WAL shrank during observation",
            )));
        }
        offset += read as u64;
        destination = &mut destination[read..];
    }
    Ok(())
}

fn parse_header(bytes: &[u8]) -> Result<WalHeader, WalError> {
    if bytes.len() < WAL_HEADER_SIZE {
        return Err(WalError::IncompleteHeader(bytes.len()));
    }
    let magic = be_u32(&bytes[0..4]);
    let checksum_order = match magic {
        WAL_MAGIC => ChecksumOrder::Little,
        value if value == WAL_MAGIC | 1 => ChecksumOrder::Big,
        value => return Err(WalError::InvalidMagic(value)),
    };
    let version = be_u32(&bytes[4..8]);
    if version != WAL_FORMAT_VERSION {
        return Err(WalError::UnsupportedVersion(version));
    }
    let page_size = be_u32(&bytes[8..12]);
    if !(512..=65_536).contains(&page_size) || !page_size.is_power_of_two() {
        return Err(WalError::InvalidPageSize(page_size));
    }
    let epoch = WalEpoch {
        salts: [be_u32(&bytes[16..20]), be_u32(&bytes[20..24])],
    };
    let mut checksum = [0, 0];
    checksum_bytes(checksum_order, &bytes[..24], &mut checksum);
    let stored = [be_u32(&bytes[24..28]), be_u32(&bytes[28..32])];
    if checksum != stored {
        return Err(WalError::InvalidHeaderChecksum);
    }
    Ok(WalHeader {
        epoch,
        page_size,
        checksum_order,
        checksum,
    })
}

fn checksum_bytes(order: ChecksumOrder, bytes: &[u8], checksum: &mut [u32; 2]) {
    debug_assert!(bytes.len() >= 8 && bytes.len().is_multiple_of(8));
    for pair in bytes.chunks_exact(8) {
        let first = checksum_u32(order, &pair[..4]);
        let second = checksum_u32(order, &pair[4..]);
        checksum[0] = checksum[0].wrapping_add(first).wrapping_add(checksum[1]);
        checksum[1] = checksum[1].wrapping_add(second).wrapping_add(checksum[0]);
    }
}

fn checksum_u32(order: ChecksumOrder, bytes: &[u8]) -> u32 {
    let bytes: [u8; 4] = bytes.try_into().expect("checksum words are four bytes");
    match order {
        ChecksumOrder::Little => u32::from_le_bytes(bytes),
        ChecksumOrder::Big => u32::from_be_bytes(bytes),
    }
}

fn be_u32(bytes: &[u8]) -> u32 {
    u32::from_be_bytes(bytes.try_into().expect("WAL field is four bytes"))
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WalError {
    Io {
        kind: io::ErrorKind,
        message: String,
    },
    IncompleteHeader(usize),
    InvalidMagic(u32),
    UnsupportedVersion(u32),
    InvalidPageSize(u32),
    InvalidHeaderChecksum,
}

impl fmt::Display for WalError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io { message, .. } => write!(f, "WAL I/O: {message}"),
            Self::IncompleteHeader(bytes) => {
                write!(f, "incomplete SQLite WAL header ({bytes} bytes)")
            }
            Self::InvalidMagic(magic) => write!(f, "invalid SQLite WAL magic {magic:#x}"),
            Self::UnsupportedVersion(version) => {
                write!(f, "unsupported SQLite WAL format version {version}")
            }
            Self::InvalidPageSize(size) => write!(f, "invalid SQLite WAL page size {size}"),
            Self::InvalidHeaderChecksum => f.write_str("invalid SQLite WAL header checksum"),
        }
    }
}

impl std::error::Error for WalError {}

impl From<io::Error> for WalError {
    fn from(error: io::Error) -> Self {
        Self::Io {
            kind: error.kind(),
            message: error.to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::fs;

    use rusqlite::Connection;

    use super::*;

    fn test_wal(order: ChecksumOrder, frames: &[(u32, u32)]) -> Vec<u8> {
        let page_size = 512_u32;
        let salts = [0x1122_3344_u32, 0x5566_7788_u32];
        let mut bytes = vec![0_u8; WAL_HEADER_SIZE];
        bytes[0..4].copy_from_slice(
            &(WAL_MAGIC | if order == ChecksumOrder::Big { 1 } else { 0 }).to_be_bytes(),
        );
        bytes[4..8].copy_from_slice(&WAL_FORMAT_VERSION.to_be_bytes());
        bytes[8..12].copy_from_slice(&page_size.to_be_bytes());
        bytes[16..20].copy_from_slice(&salts[0].to_be_bytes());
        bytes[20..24].copy_from_slice(&salts[1].to_be_bytes());
        let mut checksum = [0, 0];
        checksum_bytes(order, &bytes[..24], &mut checksum);
        bytes[24..28].copy_from_slice(&checksum[0].to_be_bytes());
        bytes[28..32].copy_from_slice(&checksum[1].to_be_bytes());

        for (frame_index, &(page, page_count)) in frames.iter().enumerate() {
            let mut frame = vec![0_u8; FRAME_HEADER_SIZE + page_size as usize];
            frame[0..4].copy_from_slice(&page.to_be_bytes());
            frame[4..8].copy_from_slice(&page_count.to_be_bytes());
            frame[8..12].copy_from_slice(&salts[0].to_be_bytes());
            frame[12..16].copy_from_slice(&salts[1].to_be_bytes());
            frame[FRAME_HEADER_SIZE..].fill(frame_index as u8 + 1);
            checksum_bytes(order, &frame[..8], &mut checksum);
            checksum_bytes(order, &frame[FRAME_HEADER_SIZE..], &mut checksum);
            frame[16..20].copy_from_slice(&checksum[0].to_be_bytes());
            frame[20..24].copy_from_slice(&checksum[1].to_be_bytes());
            bytes.extend_from_slice(&frame);
        }
        bytes
    }

    struct FaultSource {
        bytes: Vec<u8>,
        reported_len: usize,
        max_chunk: usize,
        fail_on_read: Option<usize>,
        reads: Cell<usize>,
    }

    impl WalSource for FaultSource {
        fn len(&self) -> io::Result<u64> {
            Ok(self.reported_len as u64)
        }

        fn read_at(&self, offset: u64, destination: &mut [u8]) -> io::Result<usize> {
            let read_number = self.reads.get();
            self.reads.set(read_number + 1);
            if self.fail_on_read == Some(read_number) {
                return Err(io::Error::other("injected WAL read"));
            }
            let offset = offset as usize;
            if offset >= self.bytes.len() {
                return Ok(0);
            }
            let amount = destination
                .len()
                .min(self.max_chunk)
                .min(self.bytes.len() - offset);
            destination[..amount].copy_from_slice(&self.bytes[offset..offset + amount]);
            Ok(amount)
        }
    }

    #[test]
    fn malformed_headers_and_frame_guards_are_classified() {
        let valid = test_wal(ChecksumOrder::Little, &[(1, 1), (2, 2)]);
        for length in 0..WAL_HEADER_SIZE {
            assert_eq!(
                WalParser::parse(&valid[..length]),
                Err(WalError::IncompleteHeader(length))
            );
        }

        let mut bytes = valid.clone();
        bytes[0..4].copy_from_slice(&0xdead_beef_u32.to_be_bytes());
        assert_eq!(
            WalParser::parse(&bytes),
            Err(WalError::InvalidMagic(0xdead_beef))
        );

        let mut bytes = valid.clone();
        bytes[4..8].copy_from_slice(&1_u32.to_be_bytes());
        assert_eq!(
            WalParser::parse(&bytes),
            Err(WalError::UnsupportedVersion(1))
        );

        let mut bytes = valid.clone();
        bytes[8..12].copy_from_slice(&513_u32.to_be_bytes());
        assert_eq!(
            WalParser::parse(&bytes),
            Err(WalError::InvalidPageSize(513))
        );

        let mut bytes = valid.clone();
        bytes[24] ^= 1;
        assert_eq!(
            WalParser::parse(&bytes),
            Err(WalError::InvalidHeaderChecksum)
        );

        let frame_size = FRAME_HEADER_SIZE + 512;
        let second = WAL_HEADER_SIZE + frame_size;
        let mut bytes = valid.clone();
        bytes[second + 8] ^= 1;
        let parsed = WalParser::parse(&bytes).unwrap();
        assert_eq!(parsed.snapshot.unwrap().max_frame(), 1);
        assert_eq!(parsed.tail, WalTail::InvalidSalt { frame: 2 });

        let mut bytes = valid;
        bytes[second..second + 4].fill(0);
        let parsed = WalParser::parse(&bytes).unwrap();
        assert_eq!(parsed.snapshot.unwrap().max_frame(), 1);
        assert_eq!(parsed.tail, WalTail::InvalidPageNumber { frame: 2 });
    }

    #[test]
    fn streaming_parser_handles_short_reads_and_io_failure() {
        let bytes = test_wal(ChecksumOrder::Little, &[(1, 1), (2, 2)]);
        let short = FaultSource {
            bytes: bytes.clone(),
            reported_len: bytes.len(),
            max_chunk: 7,
            fail_on_read: None,
            reads: Cell::new(0),
        };
        assert_eq!(
            WalParser::new().refresh_source(&short).unwrap(),
            WalParser::parse(&bytes).unwrap()
        );
        assert!(short.reads.get() > 2);

        let growing = FaultSource {
            bytes: [bytes.clone(), vec![9; 100]].concat(),
            reported_len: bytes.len(),
            max_chunk: usize::MAX,
            fail_on_read: None,
            reads: Cell::new(0),
        };
        assert_eq!(
            WalParser::new().refresh_source(&growing).unwrap(),
            WalParser::parse(&bytes).unwrap()
        );

        let shrinking = FaultSource {
            bytes: bytes[..bytes.len() - 1].to_vec(),
            reported_len: bytes.len(),
            max_chunk: 31,
            fail_on_read: None,
            reads: Cell::new(0),
        };
        assert!(matches!(
            WalParser::new().refresh_source(&shrinking),
            Err(WalError::Io {
                kind: io::ErrorKind::UnexpectedEof,
                ..
            })
        ));

        let failing = FaultSource {
            bytes,
            reported_len: short.reported_len,
            max_chunk: 17,
            fail_on_read: Some(2),
            reads: Cell::new(0),
        };
        assert!(matches!(
            WalParser::new().refresh_source(&failing),
            Err(WalError::Io { message, .. }) if message == "injected WAL read"
        ));
    }

    #[test]
    fn randomized_incremental_observations_equal_cold_parses() {
        let mut seed = 0x6d75_6c74_696c_6974_u64;
        for order in [ChecksumOrder::Little, ChecksumOrder::Big] {
            let mut frames = Vec::new();
            for index in 0..80_u32 {
                seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
                let page = ((seed >> 32) as u32 % 16) + 1;
                seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
                let commit = index % 7 == 6 || seed.is_multiple_of(5);
                frames.push((page, if commit { 16 } else { 0 }));
            }
            frames.last_mut().unwrap().1 = 16;
            let bytes = test_wal(order, &frames);
            let mut incremental = WalParser::new();
            for _ in 0..500 {
                seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
                let length =
                    WAL_HEADER_SIZE + (seed as usize % (bytes.len() - WAL_HEADER_SIZE + 1));
                assert_eq!(
                    incremental.refresh(&bytes[..length]),
                    WalParser::parse(&bytes[..length])
                );
            }
            assert_eq!(incremental.refresh(&bytes), WalParser::parse(&bytes));
        }
    }

    #[test]
    fn parses_real_sqlite_wal_and_incremental_matches_cold_rebuild() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("incremental.sqlite");
        let wal_path = directory.path().join("incremental.sqlite-wal");
        let connection = Connection::open(&path).unwrap();
        connection
            .pragma_update(None, "journal_mode", "WAL")
            .unwrap();
        connection
            .pragma_update(None, "wal_autocheckpoint", 0)
            .unwrap();
        connection
            .execute_batch(
                "CREATE TABLE values_by_id (id INTEGER PRIMARY KEY, value BLOB NOT NULL)",
            )
            .unwrap();

        let mut incremental = WalParser::new();
        let mut previous_frame = 0;
        for value in 0_u8..32 {
            connection
                .execute(
                    "INSERT INTO values_by_id VALUES (?1, ?2)",
                    rusqlite::params![value, vec![value; 300]],
                )
                .unwrap();
            let bytes = fs::read(&wal_path).unwrap();
            let extended = incremental.refresh(&bytes).unwrap();
            let cold = WalParser::parse(&bytes).unwrap();
            assert_eq!(extended, cold);
            assert_eq!(extended.tail, WalTail::Complete);
            let snapshot = extended.snapshot.unwrap();
            assert!(snapshot.max_frame() > previous_frame);
            assert!(snapshot.page_map().keys().all(|page| page > 0));
            assert!(snapshot.page_map().values().all(|frame| {
                frame.frame() <= snapshot.max_frame()
                    && frame.data_offset() >= (WAL_HEADER_SIZE + FRAME_HEADER_SIZE) as u64
            }));
            previous_frame = snapshot.max_frame();
        }
    }

    #[test]
    fn publishes_only_complete_commits_across_every_crash_tail() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("tails.sqlite");
        let wal_path = directory.path().join("tails.sqlite-wal");
        let connection = Connection::open(&path).unwrap();
        connection
            .pragma_update(None, "journal_mode", "WAL")
            .unwrap();
        connection
            .pragma_update(None, "wal_autocheckpoint", 0)
            .unwrap();
        connection
            .execute_batch("CREATE TABLE records (id INTEGER PRIMARY KEY, value BLOB)")
            .unwrap();
        let before = fs::read(&wal_path).unwrap();
        let committed_before = WalParser::parse(&before).unwrap().snapshot.unwrap();

        connection
            .execute("INSERT INTO records VALUES (1, ?1)", [vec![7_u8; 8_000]])
            .unwrap();
        let after = fs::read(&wal_path).unwrap();
        let committed_after = WalParser::parse(&after).unwrap().snapshot.unwrap();
        assert!(committed_after.max_frame() > committed_before.max_frame());

        let frame_size = FRAME_HEADER_SIZE + committed_after.page_size() as usize;
        let final_transaction_start =
            WAL_HEADER_SIZE + committed_before.max_frame() as usize * frame_size;
        for end in final_transaction_start..after.len() {
            let parsed = WalParser::parse(&after[..end]).unwrap();
            assert_eq!(parsed.snapshot.as_ref(), Some(&committed_before));
            let ends_at_frame_boundary = (end - WAL_HEADER_SIZE).is_multiple_of(frame_size);
            assert_eq!(
                parsed.tail,
                if ends_at_frame_boundary {
                    WalTail::Complete
                } else {
                    WalTail::Incomplete {
                        bytes: (end - WAL_HEADER_SIZE) % frame_size,
                    }
                }
            );
        }
        assert_eq!(
            WalParser::parse(&after).unwrap().snapshot,
            Some(committed_after)
        );
    }

    #[test]
    fn checksum_failure_stops_at_the_last_valid_commit() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("checksum.sqlite");
        let wal_path = directory.path().join("checksum.sqlite-wal");
        let connection = Connection::open(&path).unwrap();
        connection
            .pragma_update(None, "journal_mode", "WAL")
            .unwrap();
        connection
            .pragma_update(None, "wal_autocheckpoint", 0)
            .unwrap();
        connection
            .execute_batch("CREATE TABLE records (id)")
            .unwrap();
        let before = fs::read(&wal_path).unwrap();
        let committed_before = WalParser::parse(&before).unwrap().snapshot.unwrap();
        connection
            .execute("INSERT INTO records VALUES (1)", ())
            .unwrap();
        let mut bytes = fs::read(&wal_path).unwrap();
        let frame_size = FRAME_HEADER_SIZE + committed_before.page_size() as usize;
        let next_frame_offset =
            WAL_HEADER_SIZE + committed_before.max_frame() as usize * frame_size;
        bytes[next_frame_offset + FRAME_HEADER_SIZE] ^= 0x80;

        let parsed = WalParser::parse(&bytes).unwrap();
        assert_eq!(parsed.snapshot, Some(committed_before));
        assert_eq!(
            parsed.tail,
            WalTail::InvalidChecksum {
                frame: parsed.snapshot.unwrap().max_frame() + 1
            }
        );
    }

    #[test]
    fn checkpoint_rotation_resets_incremental_state() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("rotation.sqlite");
        let wal_path = directory.path().join("rotation.sqlite-wal");
        let connection = Connection::open(&path).unwrap();
        connection
            .pragma_update(None, "journal_mode", "WAL")
            .unwrap();
        connection
            .pragma_update(None, "wal_autocheckpoint", 0)
            .unwrap();
        connection
            .execute_batch("CREATE TABLE records (id)")
            .unwrap();
        let mut parser = WalParser::new();
        let first = parser.refresh(&fs::read(&wal_path).unwrap()).unwrap();
        let first_epoch = first.snapshot.unwrap().epoch();

        connection
            .pragma_update(None, "wal_checkpoint", "TRUNCATE")
            .unwrap();
        connection
            .execute("INSERT INTO records VALUES (1)", ())
            .unwrap();
        let bytes = fs::read(&wal_path).unwrap();
        let refreshed = parser.refresh(&bytes).unwrap();
        assert_eq!(refreshed, WalParser::parse(&bytes).unwrap());
        assert_ne!(refreshed.snapshot.unwrap().epoch(), first_epoch);
    }

    #[test]
    fn commit_truncation_removes_stale_pages_for_both_checksum_orders() {
        for order in [ChecksumOrder::Little, ChecksumOrder::Big] {
            let bytes = test_wal(order, &[(5, 5), (4, 0), (1, 2)]);
            let parsed = WalParser::parse(&bytes).unwrap();
            let snapshot = parsed.snapshot.unwrap();
            assert_eq!(snapshot.max_frame(), 3);
            assert_eq!(snapshot.page_count(), 2);
            assert_eq!(snapshot.page_map().keys().collect::<Vec<_>>(), [1]);
        }
    }

    #[test]
    fn page_map_snapshots_share_untouched_chunks() {
        let mut map = PageMap::default();
        map.insert(
            1,
            WalFrame {
                frame: 1,
                data_offset: 56,
            },
        );
        map.insert(
            300,
            WalFrame {
                frame: 2,
                data_offset: 96,
            },
        );
        let published = map.clone();
        let published_second = Arc::clone(published.chunks.get(&1).unwrap());

        map.insert(
            1,
            WalFrame {
                frame: 3,
                data_offset: 136,
            },
        );

        assert_eq!(published.get(&1).unwrap().frame(), 1);
        assert_eq!(map.get(&1).unwrap().frame(), 3);
        assert!(Arc::ptr_eq(&published_second, map.chunks.get(&1).unwrap()));
    }
}
