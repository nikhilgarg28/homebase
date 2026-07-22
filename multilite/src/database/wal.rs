//! Recoverable SQLite WAL parsing and immutable committed page maps.

#![cfg_attr(
    not(test),
    allow(dead_code, reason = "consumed by the Branch VFS in the next batch")
)]

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use homebase_core::tag::AdmissionSeq;

const WAL_HEADER_SIZE: usize = 32;
const FRAME_HEADER_SIZE: usize = 24;
const WAL_MAGIC: u32 = 0x377f_0682;
const WAL_FORMAT_VERSION: u32 = 3_007_000;

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

/// Immutable coordinates for one complete local SQLite commit.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SnapshotDescriptor {
    pub local_generation: u64,
    pub authority_frontier: AdmissionSeq,
    pub wal_epoch: WalEpoch,
    pub max_frame: u32,
    pub page_count: u32,
    pub page_size: u32,
    pub page_map: Arc<BTreeMap<u32, WalFrame>>,
}

impl SnapshotDescriptor {
    pub fn from_wal(
        local_generation: u64,
        authority_frontier: AdmissionSeq,
        snapshot: &WalSnapshot,
    ) -> Self {
        Self {
            local_generation,
            authority_frontier,
            wal_epoch: snapshot.epoch,
            max_frame: snapshot.max_frame,
            page_count: snapshot.page_count,
            page_size: snapshot.page_size,
            page_map: Arc::clone(&snapshot.page_map),
        }
    }
}

/// Latest complete commit recovered from a WAL prefix.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WalSnapshot {
    epoch: WalEpoch,
    max_frame: u32,
    page_count: u32,
    page_size: u32,
    page_map: Arc<BTreeMap<u32, WalFrame>>,
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

    pub fn page_map(&self) -> &BTreeMap<u32, WalFrame> {
        &self.page_map
    }
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
    working_map: BTreeMap<u32, WalFrame>,
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
        Self::new().refresh(bytes)
    }

    /// Extend or reset this parser to match the complete observed WAL prefix.
    pub fn refresh(&mut self, bytes: &[u8]) -> Result<WalParse, WalError> {
        let header = parse_header(bytes)?;
        let reset = self
            .state
            .as_ref()
            .is_none_or(|state| state.header != header || bytes.len() < state.next_offset);
        if reset {
            self.state = Some(ParserState {
                header,
                next_frame: 1,
                next_offset: WAL_HEADER_SIZE,
                checksum: header.checksum,
                working_map: BTreeMap::new(),
                committed: None,
            });
        }

        let state = self.state.as_mut().expect("parser state initialized above");
        let frame_size = FRAME_HEADER_SIZE
            .checked_add(state.header.page_size as usize)
            .ok_or(WalError::InvalidPageSize(state.header.page_size))?;
        let mut tail = WalTail::Complete;

        while state.next_offset < bytes.len() {
            let remaining = bytes.len() - state.next_offset;
            if remaining < frame_size {
                tail = WalTail::Incomplete { bytes: remaining };
                break;
            }
            let frame_number = state.next_frame;
            let frame = &bytes[state.next_offset..state.next_offset + frame_size];
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
                state.working_map.retain(|page, _| *page <= page_count);
                state.committed = Some(WalSnapshot {
                    epoch: state.header.epoch,
                    max_frame: frame_number,
                    page_count,
                    page_size: state.header.page_size,
                    page_map: Arc::new(state.working_map.clone()),
                });
            }
        }

        Ok(WalParse {
            snapshot: state.committed.clone(),
            tail,
            valid_bytes: state.next_offset,
        })
    }
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
    debug_assert!(bytes.len() >= 8 && bytes.len() % 8 == 0);
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
    IncompleteHeader(usize),
    InvalidMagic(u32),
    UnsupportedVersion(u32),
    InvalidPageSize(u32),
    InvalidHeaderChecksum,
}

impl fmt::Display for WalError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
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

#[cfg(test)]
mod tests {
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
            assert!(snapshot.page_map().keys().all(|page| *page > 0));
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
            let ends_at_frame_boundary = (end - WAL_HEADER_SIZE) % frame_size == 0;
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
    fn descriptor_carries_both_local_and_authority_coordinates() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("descriptor.sqlite");
        let wal_path = directory.path().join("descriptor.sqlite-wal");
        let connection = Connection::open(&path).unwrap();
        connection
            .pragma_update(None, "journal_mode", "WAL")
            .unwrap();
        connection
            .execute_batch("CREATE TABLE records (id)")
            .unwrap();
        let parsed = WalParser::parse(&fs::read(&wal_path).unwrap()).unwrap();
        let snapshot = parsed.snapshot.unwrap();
        let descriptor = SnapshotDescriptor::from_wal(41, AdmissionSeq(17), &snapshot);

        assert_eq!(descriptor.local_generation, 41);
        assert_eq!(descriptor.authority_frontier, AdmissionSeq(17));
        assert_eq!(descriptor.wal_epoch, snapshot.epoch());
        assert_eq!(descriptor.max_frame, snapshot.max_frame());
        assert_eq!(descriptor.page_count, snapshot.page_count());
        assert_eq!(descriptor.wal_epoch.salts(), snapshot.epoch().salts());
        assert_eq!(descriptor.page_map.as_ref(), snapshot.page_map());
    }

    #[test]
    fn commit_truncation_removes_stale_pages_for_both_checksum_orders() {
        for order in [ChecksumOrder::Little, ChecksumOrder::Big] {
            let bytes = test_wal(order, &[(5, 5), (4, 0), (1, 2)]);
            let parsed = WalParser::parse(&bytes).unwrap();
            let snapshot = parsed.snapshot.unwrap();
            assert_eq!(snapshot.max_frame(), 3);
            assert_eq!(snapshot.page_count(), 2);
            assert_eq!(snapshot.page_map().keys().copied().collect::<Vec<_>>(), [1]);
        }
    }
}
