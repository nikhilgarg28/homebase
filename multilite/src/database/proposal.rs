//! Owned branch commit proposals and deterministic logical lowering.

use std::collections::BTreeMap;
use std::fmt;

use homebase_core::key::Key;
use homebase_core::messages::RangeAssert;
use homebase_core::reader::Reader;
use homebase_core::tag::Mutation;
use homebase_core::writer::Writer;
use rusqlite::Connection;
use uuid::{Uuid, Variant, Version};

use super::isolation::{ConflictFootprint, IsolationLevel};
use super::operation::MultiliteOp;
use super::row::{CapturedRow, InsertRows};
use super::transaction::MultiliteTransaction;
use crate::branch::changeset::CapturedChangeset;
use crate::snapshot::SnapshotDescriptor;
use crate::{Error, Result};

const PROPOSAL_FRAME_VERSION: u8 = 1;
const TAG_PROPOSAL_ID: u8 = 1;
const TAG_SNAPSHOT: u8 = 2;
const TAG_ISOLATION: u8 = 3;
const TAG_CHANGESET: u8 = 4;
const TAG_TRANSACTION: u8 = 5;
const TAG_WRITE: u8 = 10;
const TAG_CONSTRAINT: u8 = 11;
const TAG_READ: u8 = 12;

/// Stable identity used to deduplicate an uncertain canonical commit reply.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ProposalId([u8; 16]);

impl ProposalId {
    pub fn new() -> Self {
        Self(Uuid::new_v4().into_bytes())
    }

    pub const fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }

    pub const fn to_bytes(self) -> [u8; 16] {
        self.0
    }
}

/// Self-contained result of executing one managed update on a private branch.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommitProposal {
    id: ProposalId,
    snapshot: SnapshotDescriptor,
    isolation: IsolationLevel,
    changeset: CapturedChangeset,
    transaction: MultiliteTransaction,
    footprint: ConflictFootprint,
}

impl CommitProposal {
    /// Lower one insert-only captured branch into an owned commit proposal.
    pub fn from_captured(
        snapshot: SnapshotDescriptor,
        isolation: IsolationLevel,
        changeset: CapturedChangeset,
        connection: &Connection,
        reads: impl IntoIterator<Item = Key>,
    ) -> Result<Option<Self>> {
        if changeset.is_empty() {
            return Ok(None);
        }
        changeset
            .validate_schema(connection)
            .map_err(invalid_changeset)?;
        let operations = lower_insert_operations(&changeset, connection)?;
        let transaction = MultiliteTransaction::new(operations)?;
        let (_, mut footprint) = transaction.to_homebase()?.into_parts();
        for read in reads {
            footprint.add_read(read);
        }
        Ok(Some(Self {
            id: ProposalId::new(),
            snapshot,
            isolation,
            changeset,
            transaction,
            footprint,
        }))
    }

    pub fn id(&self) -> ProposalId {
        self.id
    }

    pub fn snapshot(&self) -> SnapshotDescriptor {
        self.snapshot
    }

    pub fn isolation(&self) -> IsolationLevel {
        self.isolation
    }

    pub fn changeset(&self) -> &CapturedChangeset {
        &self.changeset
    }

    pub fn transaction(&self) -> &MultiliteTransaction {
        &self.transaction
    }

    pub fn footprint(&self) -> &ConflictFootprint {
        &self.footprint
    }

    /// Produce the exact Homebase commit represented by this proposal.
    pub fn to_homebase(&self) -> Result<(Vec<Mutation>, Vec<RangeAssert>)> {
        let (mutations, mandatory) = self.transaction.to_homebase()?.into_parts();
        self.validate_mandatory_footprint(&mandatory)?;
        let assertions = self
            .footprint
            .clone()
            .plan(self.isolation, self.snapshot.authority_applied_through);
        Ok((mutations, assertions))
    }

    /// Cross-check captured SQLite rows against the logical operation envelope.
    pub fn validate_against(&self, connection: &Connection) -> Result<()> {
        self.changeset
            .validate_schema(connection)
            .map_err(invalid_changeset)?;
        let operations = lower_insert_operations(&self.changeset, connection)?;
        if operations != self.transaction.operations() {
            return Err(Error::InvalidCommitProposal(
                "captured SQLite changes contradict the logical transaction".into(),
            ));
        }
        let (_, mandatory) = self.transaction.to_homebase()?.into_parts();
        self.validate_mandatory_footprint(&mandatory)
    }

    /// Encode every input needed for validation, replay, and remote submission.
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut writer = Writer::new();
        writer.u8(PROPOSAL_FRAME_VERSION);
        put_field(&mut writer, TAG_PROPOSAL_ID, &self.id.0)?;
        put_field(&mut writer, TAG_SNAPSHOT, &self.snapshot.encode())?;
        put_field(
            &mut writer,
            TAG_ISOLATION,
            &[encode_isolation(self.isolation)],
        )?;
        put_field(
            &mut writer,
            TAG_CHANGESET,
            &self.changeset.encode().map_err(invalid_changeset)?,
        )?;
        put_field(&mut writer, TAG_TRANSACTION, &self.transaction.encode())?;
        for key in self.footprint.writes() {
            put_field(&mut writer, TAG_WRITE, &key.encode())?;
        }
        for key in self.footprint.constraints() {
            put_field(&mut writer, TAG_CONSTRAINT, &key.encode())?;
        }
        for key in self.footprint.reads() {
            put_field(&mut writer, TAG_READ, &key.encode())?;
        }
        Ok(writer.finish())
    }

    /// Decode one proposal and reject contradictory logical footprints.
    pub fn decode(frame: &[u8]) -> std::result::Result<Self, ProposalCodecError> {
        let mut reader = Reader::new(frame);
        if reader.u8() != Some(PROPOSAL_FRAME_VERSION) {
            return Err(ProposalCodecError::UnknownVersion);
        }
        let mut id = None;
        let mut snapshot = None;
        let mut isolation = None;
        let mut changeset = None;
        let mut transaction = None;
        let mut writes = Vec::new();
        let mut constraints = Vec::new();
        let mut reads = Vec::new();
        while let Some((tag, value)) = next_field(&mut reader)? {
            match tag {
                TAG_PROPOSAL_ID => set_once(&mut id, ProposalId(uuid_bytes(value)?), tag)?,
                TAG_SNAPSHOT => set_once(
                    &mut snapshot,
                    SnapshotDescriptor::decode(value)
                        .map_err(|error| ProposalCodecError::InvalidSnapshot(error.to_string()))?,
                    tag,
                )?,
                TAG_ISOLATION => set_once(&mut isolation, decode_isolation(value)?, tag)?,
                TAG_CHANGESET => set_once(
                    &mut changeset,
                    CapturedChangeset::decode(value)
                        .map_err(|error| ProposalCodecError::InvalidChangeset(error.to_string()))?,
                    tag,
                )?,
                TAG_TRANSACTION => set_once(
                    &mut transaction,
                    MultiliteTransaction::decode(value).map_err(|error| {
                        ProposalCodecError::InvalidTransaction(error.to_string())
                    })?,
                    tag,
                )?,
                TAG_WRITE => writes.push(decode_key(value)?),
                TAG_CONSTRAINT => constraints.push(decode_key(value)?),
                TAG_READ => reads.push(decode_key(value)?),
                _ => {}
            }
        }
        if !canonical_keys(&writes) || !canonical_keys(&constraints) || !canonical_keys(&reads) {
            return Err(ProposalCodecError::InvalidFootprint);
        }
        let proposal = Self {
            id: id.ok_or(ProposalCodecError::MissingField(TAG_PROPOSAL_ID))?,
            snapshot: snapshot.ok_or(ProposalCodecError::MissingField(TAG_SNAPSHOT))?,
            isolation: isolation.ok_or(ProposalCodecError::MissingField(TAG_ISOLATION))?,
            changeset: changeset.ok_or(ProposalCodecError::MissingField(TAG_CHANGESET))?,
            transaction: transaction.ok_or(ProposalCodecError::MissingField(TAG_TRANSACTION))?,
            footprint: ConflictFootprint::from_parts(writes, constraints, reads),
        };
        let (_, mandatory) = proposal
            .transaction
            .to_homebase()
            .map_err(|error| ProposalCodecError::InvalidTransaction(error.to_string()))?
            .into_parts();
        if proposal.footprint.writes() != mandatory.writes()
            || proposal.footprint.constraints() != mandatory.constraints()
        {
            return Err(ProposalCodecError::InvalidFootprint);
        }
        Ok(proposal)
    }

    fn validate_mandatory_footprint(&self, mandatory: &ConflictFootprint) -> Result<()> {
        if self.footprint.writes() != mandatory.writes()
            || self.footprint.constraints() != mandatory.constraints()
        {
            return Err(Error::InvalidCommitProposal(
                "typed footprint contradicts the logical transaction".into(),
            ));
        }
        Ok(())
    }

    #[cfg(test)]
    fn replace_id(&mut self, id: ProposalId) {
        self.id = id;
    }
}

fn lower_insert_operations(
    changeset: &CapturedChangeset,
    connection: &Connection,
) -> Result<Vec<MultiliteOp>> {
    let mut tables = BTreeMap::<Vec<u8>, Vec<CapturedRow>>::new();
    for inserted in changeset.inserted_rows().map_err(invalid_changeset)? {
        let mut canonical = inserted.table.as_bytes().to_vec();
        canonical.make_ascii_lowercase();
        tables.entry(canonical).or_default().push(CapturedRow {
            table: inserted.table,
            values: inserted.values,
        });
    }
    let mut operations = Vec::with_capacity(tables.len());
    for rows in tables.into_values() {
        let inserted = InsertRows::from_captured(connection, &rows)?.ok_or_else(|| {
            Error::InvalidCommitProposal(
                "captured INSERT target has no synchronized schema identity".into(),
            )
        })?;
        operations.push(MultiliteOp::InsertRows(inserted));
    }
    if operations.is_empty() {
        return Err(Error::InvalidCommitProposal(
            "non-empty SQLite changeset has no inserted rows".into(),
        ));
    }
    Ok(operations)
}

fn invalid_changeset(error: impl fmt::Display) -> Error {
    Error::InvalidCommitProposal(error.to_string())
}

fn encode_isolation(isolation: IsolationLevel) -> u8 {
    match isolation {
        IsolationLevel::Snapshot => 0,
        IsolationLevel::Serializable => 1,
    }
}

fn decode_isolation(frame: &[u8]) -> std::result::Result<IsolationLevel, ProposalCodecError> {
    match frame {
        [0] => Ok(IsolationLevel::Snapshot),
        [1] => Ok(IsolationLevel::Serializable),
        _ => Err(ProposalCodecError::InvalidIsolation),
    }
}

fn put_field(writer: &mut Writer, tag: u8, value: &[u8]) -> Result<()> {
    let length = u32::try_from(value.len())
        .map_err(|_| Error::InvalidCommitProposal("proposal field is too large".into()))?;
    writer.u8(tag);
    writer.u32(length);
    writer.bytes(value);
    Ok(())
}

fn next_field<'a>(
    reader: &mut Reader<'a>,
) -> std::result::Result<Option<(u8, &'a [u8])>, ProposalCodecError> {
    if reader.end().is_some() {
        return Ok(None);
    }
    let tag = reader.u8().ok_or(ProposalCodecError::Truncated)?;
    let length = reader.u32().ok_or(ProposalCodecError::Truncated)?;
    let length = usize::try_from(length).map_err(|_| ProposalCodecError::InvalidLength)?;
    let value = reader.take(length).ok_or(ProposalCodecError::Truncated)?;
    Ok(Some((tag, value)))
}

fn set_once<T>(
    slot: &mut Option<T>,
    value: T,
    tag: u8,
) -> std::result::Result<(), ProposalCodecError> {
    if slot.replace(value).is_some() {
        Err(ProposalCodecError::DuplicateField(tag))
    } else {
        Ok(())
    }
}

fn uuid_bytes(value: &[u8]) -> std::result::Result<[u8; 16], ProposalCodecError> {
    let bytes = value
        .try_into()
        .map_err(|_| ProposalCodecError::InvalidLength)?;
    let uuid = Uuid::from_bytes(bytes);
    if uuid.get_version() != Some(Version::Random) || uuid.get_variant() != Variant::RFC4122 {
        return Err(ProposalCodecError::InvalidUuid);
    }
    Ok(bytes)
}

fn decode_key(value: &[u8]) -> std::result::Result<Key, ProposalCodecError> {
    Key::decode(value).map_err(|error| ProposalCodecError::InvalidKey(error.to_string()))
}

fn canonical_keys(keys: &[Key]) -> bool {
    keys.windows(2)
        .all(|pair| pair[0] < pair[1] && !pair[1].starts_with(&pair[0]))
}

/// Failure to decode one durable commit proposal.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ProposalCodecError {
    UnknownVersion,
    Truncated,
    DuplicateField(u8),
    MissingField(u8),
    InvalidLength,
    InvalidUuid,
    InvalidIsolation,
    InvalidSnapshot(String),
    InvalidChangeset(String),
    InvalidTransaction(String),
    InvalidKey(String),
    InvalidFootprint,
}

impl fmt::Display for ProposalCodecError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownVersion => formatter.write_str("unknown commit proposal version"),
            Self::Truncated => formatter.write_str("commit proposal is truncated"),
            Self::DuplicateField(tag) => write!(formatter, "duplicate proposal field {tag}"),
            Self::MissingField(tag) => write!(formatter, "missing proposal field {tag}"),
            Self::InvalidLength => formatter.write_str("proposal field has an invalid length"),
            Self::InvalidUuid => formatter.write_str("proposal id is not a UUID v4"),
            Self::InvalidIsolation => formatter.write_str("proposal isolation level is invalid"),
            Self::InvalidSnapshot(error) => write!(formatter, "invalid snapshot: {error}"),
            Self::InvalidChangeset(error) => write!(formatter, "invalid changeset: {error}"),
            Self::InvalidTransaction(error) => write!(formatter, "invalid transaction: {error}"),
            Self::InvalidKey(error) => write!(formatter, "invalid footprint key: {error}"),
            Self::InvalidFootprint => {
                formatter.write_str("proposal footprint is non-canonical or contradictory")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use homebase_client::meta::OplogCursors;
    use homebase_core::tag::AdmissionSeq;

    use super::*;
    use crate::branch::changeset::ChangesetCapture;
    use crate::branch::snapshot::PinnedSnapshot;
    use crate::branch::{OverlayOptions, WritableBranch};
    use crate::database::catalog;
    use crate::database::schema::{
        CreateColumn, CreateTable, CreateTableSpec, DeclaredType, SqlName,
    };
    use crate::snapshot::LocalGeneration;

    struct Fixture {
        directory: tempfile::TempDir,
        writer: Connection,
    }

    impl Fixture {
        fn new() -> Self {
            let directory = tempfile::tempdir().unwrap();
            let writer = Connection::open(directory.path().join("proposal.sqlite")).unwrap();
            writer.pragma_update(None, "journal_mode", "WAL").unwrap();
            writer.pragma_update(None, "wal_autocheckpoint", 0).unwrap();
            catalog::initialize(&writer).unwrap();
            let created = CreateTable::new(
                "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT NOT NULL)",
                CreateTableSpec {
                    name: SqlName::new("notes".into()),
                    columns: vec![
                        CreateColumn {
                            name: SqlName::new("id".into()),
                            declared_type: DeclaredType::Integer,
                            not_null: false,
                            primary_key: true,
                        },
                        CreateColumn {
                            name: SqlName::new("body".into()),
                            declared_type: DeclaredType::Text,
                            not_null: true,
                            primary_key: false,
                        },
                    ],
                },
            );
            writer.execute(created.sql(), ()).unwrap();
            catalog::insert(&writer, &created).unwrap();
            Self { directory, writer }
        }

        fn path(&self) -> PathBuf {
            self.directory.path().join("proposal.sqlite")
        }

        fn snapshot(&self) -> PinnedSnapshot {
            PinnedSnapshot::capture(self.path(), self.path().with_extension("sqlite-wal")).unwrap()
        }

        fn branch(&self) -> WritableBranch {
            WritableBranch::open(self.snapshot(), OverlayOptions::default()).unwrap()
        }
    }

    fn descriptor() -> SnapshotDescriptor {
        SnapshotDescriptor {
            local_generation: LocalGeneration(0),
            authority_applied_through: AdmissionSeq(7),
            submit_cursors: OplogCursors::default(),
        }
    }

    fn insert_proposal(
        fixture: &Fixture,
        isolation: IsolationLevel,
        read: Option<Key>,
    ) -> CommitProposal {
        let branch = fixture.branch();
        let capture = ChangesetCapture::start(&branch, &["notes"]).unwrap();
        branch
            .connection()
            .execute("INSERT INTO notes(body) VALUES ('generated')", ())
            .unwrap();
        let changeset = capture.finish().unwrap();
        CommitProposal::from_captured(
            descriptor(),
            isolation,
            changeset,
            branch.connection(),
            read,
        )
        .unwrap()
        .unwrap()
    }

    #[test]
    fn proposal_roundtrips_and_lowers_deterministically() {
        let fixture = Fixture::new();
        let read = Key::from_bytes([b"multilite".as_slice(), b"observed".as_slice()]).unwrap();
        let proposal = insert_proposal(&fixture, IsolationLevel::Serializable, Some(read.clone()));

        let encoded = proposal.encode().unwrap();
        let decoded = CommitProposal::decode(&encoded).unwrap();
        assert_eq!(decoded, proposal);
        assert_eq!(
            decoded.to_homebase().unwrap(),
            proposal.to_homebase().unwrap()
        );

        let (mutations, assertions) = proposal.to_homebase().unwrap();
        assert_eq!(mutations.len(), 2);
        assert_eq!(assertions.len(), 4);
        assert!(assertions.iter().any(|assertion| assertion.prefix == read));
        assert!(
            assertions
                .iter()
                .all(|assertion| assertion.upto == AdmissionSeq(7))
        );
    }

    #[test]
    fn snapshot_proposals_retain_but_do_not_assert_ordinary_reads() {
        let fixture = Fixture::new();
        let read = Key::from_bytes([b"multilite".as_slice(), b"observed".as_slice()]).unwrap();
        let proposal = insert_proposal(&fixture, IsolationLevel::Snapshot, Some(read.clone()));

        assert!(proposal.footprint().reads().contains(&read));
        assert!(
            proposal
                .to_homebase()
                .unwrap()
                .1
                .iter()
                .all(|assertion| assertion.prefix != read)
        );
    }

    #[test]
    fn proposal_rejects_noncanonical_footprint_frames() {
        let fixture = Fixture::new();
        let read = Key::from_bytes([b"multilite".as_slice(), b"observed".as_slice()]).unwrap();
        let proposal = insert_proposal(&fixture, IsolationLevel::Serializable, Some(read.clone()));
        let mut encoded = proposal.encode().unwrap();
        encoded.push(TAG_READ);
        encoded.extend_from_slice(&(read.encode().len() as u32).to_be_bytes());
        encoded.extend_from_slice(&read.encode());

        assert_eq!(
            CommitProposal::decode(&encoded),
            Err(ProposalCodecError::InvalidFootprint)
        );
    }

    #[test]
    fn proposal_rejects_updates_and_schema_changes_in_branch_path() {
        let fixture = Fixture::new();
        fixture
            .writer
            .execute("INSERT INTO notes VALUES (1, 'before')", ())
            .unwrap();

        let branch = fixture.branch();
        let capture = ChangesetCapture::start(&branch, &["notes"]).unwrap();
        branch
            .connection()
            .execute("UPDATE notes SET body = 'after' WHERE id = 1", ())
            .unwrap();
        let changeset = capture.finish().unwrap();
        assert!(matches!(
            CommitProposal::from_captured(
                descriptor(),
                IsolationLevel::Snapshot,
                changeset,
                branch.connection(),
                [],
            ),
            Err(Error::InvalidCommitProposal(message)) if message.contains("unsupported UPDATE")
        ));

        let branch = fixture.branch();
        let capture = ChangesetCapture::start(&branch, &["notes"]).unwrap();
        branch
            .connection()
            .execute("INSERT INTO notes VALUES (2, 'changed')", ())
            .unwrap();
        branch
            .connection()
            .execute_batch("ALTER TABLE notes ADD COLUMN extra TEXT")
            .unwrap();
        let changeset = capture.finish().unwrap();
        assert!(matches!(
            CommitProposal::from_captured(
                descriptor(),
                IsolationLevel::Snapshot,
                changeset,
                branch.connection(),
                [],
            ),
            Err(Error::InvalidCommitProposal(message)) if message.contains("schema")
        ));
    }
}
