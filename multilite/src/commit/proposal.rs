//! Owned branch commit proposals and deterministic logical lowering.

#![cfg_attr(
    not(test),
    allow(
        dead_code,
        reason = "proposal decoding is reserved for durable queued proposals"
    )
)]

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use homebase_core::key::Key;
use homebase_core::messages::RangeAssert;
use homebase_core::reader::Reader;
use homebase_core::tag::Mutation;
use homebase_core::writer::Writer;
use rusqlite::{Connection, OptionalExtension, params};
use sha2::{Digest, Sha256};
use uuid::{Uuid, Variant, Version};

use crate::branch::changeset::CapturedChangeset;
use crate::commit::committer::CommitHistory;
use crate::commit::footprint::ConflictFootprint;
use crate::commit::history::{self, WriteRegion};
use crate::commit::snapshot::SnapshotDescriptor;
use crate::database::isolation::IsolationLevel;
use crate::database::operation::MultiliteOp;
use crate::database::row::{CapturedRow, InsertRows};
use crate::database::transaction::MultiliteTransaction;
use crate::{Error, Result};

const PROPOSAL_FRAME_VERSION: u8 = 3;
const TAG_PROPOSAL_ID: u8 = 1;
const TAG_SNAPSHOT: u8 = 2;
const TAG_ISOLATION: u8 = 3;
const TAG_TRANSACTION: u8 = 5;
const TAG_WRITE: u8 = 10;
const TAG_CONSTRAINT: u8 = 11;
const TAG_READ: u8 = 12;

const RECEIPT_TABLE: &str = "__multilite__commits";

/// Stable identity used to deduplicate an uncertain canonical commit reply.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ProposalId([u8; 16]);

impl ProposalId {
    pub fn new() -> Self {
        Self(Uuid::new_v4().into_bytes())
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
    transaction: MultiliteTransaction,
    footprint: ConflictFootprint,
}

/// Whether this call applied a proposal or found its durable receipt.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CommitDisposition {
    Applied,
    AlreadyCommitted,
}

/// Stable result of canonically committing one proposal.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CommitReceipt {
    pub commit_seq: crate::commit::snapshot::CommitSeq,
    pub disposition: CommitDisposition,
}

/// One successfully replayed proposal awaiting its group's durable receipt.
pub struct PreparedCommit {
    id: ProposalId,
    hash: [u8; 32],
    writes: Vec<WriteRegion>,
}

impl PreparedCommit {
    pub fn writes(&self) -> &[WriteRegion] {
        &self.writes
    }
}

/// Result of checking one proposal inside a canonical commit group.
pub enum PrepareOutcome {
    Prepared(PreparedCommit),
    AlreadyCommitted(CommitReceipt),
}

impl CommitProposal {
    /// Lower one insert-only captured branch into an owned logical proposal.
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
            .validate_tables(connection)
            .map_err(invalid_changeset)?;
        let operations = lower_insert_operations(&changeset, connection)?;
        let transaction = MultiliteTransaction::new(operations)?;
        Self::from_transaction(snapshot, isolation, transaction, reads).map(Some)
    }

    /// Build one proposal from a complete ordered logical transaction.
    pub fn from_transaction(
        snapshot: SnapshotDescriptor,
        isolation: IsolationLevel,
        transaction: MultiliteTransaction,
        reads: impl IntoIterator<Item = Key>,
    ) -> Result<Self> {
        let (_, mut footprint) = transaction.to_homebase()?.into_parts();
        for read in reads {
            footprint.add_read(read);
        }
        Ok(Self {
            id: ProposalId::new(),
            snapshot,
            isolation,
            transaction,
            footprint,
        })
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

    pub fn footprint(&self) -> &ConflictFootprint {
        &self.footprint
    }

    pub(crate) fn transaction(&self) -> &MultiliteTransaction {
        &self.transaction
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

    /// Cross-check the typed footprint against the immutable transaction.
    pub fn validate(&self) -> Result<()> {
        let (_, mandatory) = self.transaction.to_homebase()?.into_parts();
        self.validate_mandatory_footprint(&mandatory)
    }

    /// Encode every input needed for validation, materialization, and submission.
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

/// Create the durable proposal-receipt table as part of database bootstrap.
pub fn initialize(connection: &Connection) -> Result<()> {
    connection.execute_batch(&format!(
        "CREATE TABLE {RECEIPT_TABLE} (
            proposal_id BLOB PRIMARY KEY NOT NULL CHECK(length(proposal_id) = 16),
            commit_seq BLOB NOT NULL CHECK(length(commit_seq) = 8),
            proposal_hash BLOB NOT NULL CHECK(length(proposal_hash) = 32)
        ) WITHOUT ROWID"
    ))?;
    Ok(())
}

/// Check whether the proposal-receipt namespace is present.
pub fn is_initialized(connection: &Connection) -> Result<bool> {
    table_initialized(connection, RECEIPT_TABLE)
}

fn table_initialized(connection: &Connection, table: &'static str) -> Result<bool> {
    let mut statement = connection.prepare(
        "SELECT name FROM sqlite_schema
         WHERE type = 'table'
           AND substr(name, 1, length(?1)) = ?1 COLLATE NOCASE
         ORDER BY name",
    )?;
    let tables = statement
        .query_map([table], |row| row.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    match tables.as_slice() {
        [] => Ok(false),
        [found] if found == table => Ok(true),
        _ => Err(Error::InvalidDatabase(
            "commit receipt namespace contains unexpected tables",
        )),
    }
}

/// Validate the receipt schema and every retained proposal identity.
pub fn validate(connection: &Connection) -> Result<()> {
    if !is_initialized(connection)? {
        return Err(Error::InvalidDatabase("commit receipt table is missing"));
    }
    let mut statement = connection.prepare(&format!("PRAGMA table_info({RECEIPT_TABLE})"))?;
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
    let expected = vec![
        (String::from("proposal_id"), String::from("BLOB"), true, 1),
        (String::from("commit_seq"), String::from("BLOB"), true, 0),
        (String::from("proposal_hash"), String::from("BLOB"), true, 0),
    ];
    if columns != expected {
        return Err(Error::InvalidDatabase(
            "commit receipt table schema is invalid",
        ));
    }
    validate_without_rowid(
        connection,
        RECEIPT_TABLE,
        "commit receipt table must use WITHOUT ROWID",
    )?;
    let current = history::current(connection)?;
    let mut statement = connection.prepare(&format!(
        "SELECT proposal_id, commit_seq, proposal_hash FROM {RECEIPT_TABLE} ORDER BY commit_seq"
    ))?;
    let receipts = statement
        .query_map((), |row| {
            Ok((
                row.get::<_, Vec<u8>>(0)?,
                row.get::<_, Vec<u8>>(1)?,
                row.get::<_, Vec<u8>>(2)?,
            ))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    for (id, commit_seq, hash) in receipts {
        if uuid_bytes(&id).is_err() {
            return Err(Error::InvalidDatabase(
                "commit receipt proposal id is malformed",
            ));
        }
        let commit_seq = history::decode_commit_seq(&commit_seq, false)?;
        if commit_seq > current || hash.len() != 32 {
            return Err(Error::InvalidDatabase("commit receipt is malformed"));
        }
    }
    Ok(())
}

fn validate_without_rowid(
    connection: &Connection,
    table: &str,
    message: &'static str,
) -> Result<()> {
    let schema_sql: String = connection.query_row(
        "SELECT sql FROM sqlite_schema WHERE type = 'table' AND name = ?1",
        [table],
        |row| row.get(0),
    )?;
    if !schema_sql.to_ascii_uppercase().contains("WITHOUT ROWID") {
        return Err(Error::InvalidDatabase(message));
    }
    Ok(())
}

/// Validate, replay, and receipt one proposal in a single SQLite savepoint.
pub fn apply(connection: &Connection, proposal: &CommitProposal) -> Result<CommitReceipt> {
    connection.execute_batch("SAVEPOINT __multilite__commit_proposal")?;
    let result = apply_inner(connection, proposal);
    match result {
        Ok(receipt) => {
            connection.execute_batch("RELEASE __multilite__commit_proposal")?;
            Ok(receipt)
        }
        Err(error) => {
            let rollback = connection.execute_batch(
                "ROLLBACK TO __multilite__commit_proposal;
                 RELEASE __multilite__commit_proposal",
            );
            match rollback {
                Ok(()) => Err(error),
                Err(rollback) => Err(rollback.into()),
            }
        }
    }
}

fn apply_inner(connection: &Connection, proposal: &CommitProposal) -> Result<CommitReceipt> {
    match prepare(connection, proposal, &BTreeSet::new())? {
        PrepareOutcome::AlreadyCommitted(receipt) => Ok(receipt),
        PrepareOutcome::Prepared(prepared) => {
            let commit_seq = finalize_group(
                connection,
                &CommitHistory::default(),
                std::slice::from_ref(&prepared),
            )?;
            Ok(CommitReceipt {
                commit_seq,
                disposition: CommitDisposition::Applied,
            })
        }
    }
}

/// Validate and replay one proposal without advancing the group's commit sequence.
///
/// The caller must surround this operation with a proposal-local savepoint and
/// call [`finalize_group`] in the same outer transaction for every prepared
/// proposal it retains.
pub fn prepare(
    connection: &Connection,
    proposal: &CommitProposal,
    accepted_writes: &BTreeSet<WriteRegion>,
) -> Result<PrepareOutcome> {
    if let Some(receipt) = committed_receipt(connection, proposal)? {
        return Ok(PrepareOutcome::AlreadyCommitted(receipt));
    }

    let current = history::current(connection)?;
    if proposal.snapshot().commit_seq > current {
        return Err(Error::CommitConflict(
            "proposal snapshot is newer than canonical SQLite".into(),
        ));
    }
    proposal.validate()?;
    for committed in history::history_after(connection, proposal.snapshot().commit_seq)? {
        if proposal
            .footprint()
            .conflicts_with_writes(proposal.isolation(), &committed.writes)
        {
            return Err(Error::CommitConflict(format!(
                "proposal conflicts with local commit sequence {}",
                committed.commit_seq.0
            )));
        }
    }
    if proposal
        .footprint()
        .conflicts_with_writes(proposal.isolation(), accepted_writes)
    {
        return Err(Error::CommitConflict(
            "proposal conflicts with an earlier proposal in its commit group".into(),
        ));
    }
    let lowered = proposal.transaction().to_homebase()?;
    let writes = history::writes_from_mutations(&lowered.mutations);
    let encoded = proposal.encode()?;
    let hash = proposal_hash(&encoded);
    apply_canonical(connection, proposal)?;
    Ok(PrepareOutcome::Prepared(PreparedCommit {
        id: proposal.id(),
        hash,
        writes,
    }))
}

/// Publish one canonical visibility transition and all proposal receipts.
pub fn finalize_group(
    connection: &Connection,
    history: &CommitHistory,
    prepared: &[PreparedCommit],
) -> Result<crate::commit::snapshot::CommitSeq> {
    if prepared.is_empty() {
        return Err(Error::CaptureInvariant(
            "cannot finalize an empty commit group",
        ));
    }
    let mut ids = BTreeSet::new();
    let mut writes = BTreeSet::new();
    for commit in prepared {
        if !ids.insert(commit.id) {
            return Err(Error::InvalidCommitProposal(
                "commit group contains a duplicate proposal id".into(),
            ));
        }
        writes.extend(commit.writes.iter().cloned());
    }
    let commit_seq = history.record(connection, writes.into_iter().collect())?;
    for commit in prepared {
        connection.execute(
            &format!(
                "INSERT INTO {RECEIPT_TABLE} (proposal_id, commit_seq, proposal_hash)
                 VALUES (?1, ?2, ?3)"
            ),
            params![
                commit.id.to_bytes().as_slice(),
                commit_seq.0.to_be_bytes().as_slice(),
                commit.hash.as_slice(),
            ],
        )?;
    }
    Ok(commit_seq)
}

fn apply_canonical(connection: &Connection, proposal: &CommitProposal) -> Result<()> {
    proposal
        .transaction
        .apply(connection)
        .map_err(|error| match error {
            Error::Sqlite(error) => Error::CommitConflict(error.to_string()),
            error => error,
        })
}

fn committed_receipt(
    connection: &Connection,
    proposal: &CommitProposal,
) -> Result<Option<CommitReceipt>> {
    let row = connection
        .query_row(
            &format!(
                "SELECT commit_seq, proposal_hash FROM {RECEIPT_TABLE} WHERE proposal_id = ?1"
            ),
            [proposal.id().to_bytes().as_slice()],
            |row| Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, Vec<u8>>(1)?)),
        )
        .optional()?;
    row.map(|(commit_seq, expected_hash)| {
        let encoded = proposal.encode()?;
        if expected_hash != proposal_hash(&encoded) {
            return Err(Error::InvalidCommitProposal(
                "proposal id is already committed with another payload".into(),
            ));
        }
        Ok(CommitReceipt {
            commit_seq: history::decode_commit_seq(&commit_seq, false)?,
            disposition: CommitDisposition::AlreadyCommitted,
        })
    })
    .transpose()
}

fn proposal_hash(encoded: &[u8]) -> [u8; 32] {
    Sha256::digest(encoded).into()
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
            rowid: inserted.rowid,
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
    use crate::commit::history;
    use crate::commit::snapshot::CommitSeq;
    use crate::database::catalog;
    use crate::database::schema::{
        CreateColumn, CreateTable, CreateTableSpec, DeclaredType, SqlName,
    };

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
            initialize(&writer).unwrap();
            history::initialize(&writer).unwrap();
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
            commit_seq: CommitSeq(0),
            authority_applied_through: AdmissionSeq(7),
            submit_cursors: OplogCursors::default(),
        }
    }

    fn insert_proposal(
        fixture: &Fixture,
        isolation: IsolationLevel,
        read: Option<Key>,
    ) -> CommitProposal {
        proposal_for_sql(
            fixture,
            "INSERT INTO notes(body) VALUES ('generated')",
            isolation,
            descriptor(),
            read,
        )
    }

    fn proposal_for_sql(
        fixture: &Fixture,
        sql: &str,
        isolation: IsolationLevel,
        snapshot: SnapshotDescriptor,
        reads: impl IntoIterator<Item = Key>,
    ) -> CommitProposal {
        let branch = fixture.branch();
        let capture = ChangesetCapture::start(&branch, &["notes"]).unwrap();
        branch.connection().execute(sql, ()).unwrap();
        let changeset = capture.finish().unwrap();
        CommitProposal::from_captured(snapshot, isolation, changeset, branch.connection(), reads)
            .unwrap()
            .unwrap()
    }

    fn create_proposal(name: &str, snapshot: SnapshotDescriptor) -> CommitProposal {
        let transaction =
            MultiliteTransaction::new(vec![MultiliteOp::CreateTable(CreateTable::new(
                &format!("CREATE TABLE {name} (id INTEGER PRIMARY KEY)"),
                CreateTableSpec {
                    name: SqlName::new(name.into()),
                    columns: vec![CreateColumn {
                        name: SqlName::new("id".into()),
                        declared_type: DeclaredType::Integer,
                        not_null: false,
                        primary_key: true,
                    }],
                },
            ))])
            .unwrap();
        CommitProposal::from_transaction(
            snapshot,
            IsolationLevel::Snapshot,
            transaction,
            std::iter::empty(),
        )
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
    fn create_table_proposal_roundtrips_materializes_and_deduplicates() {
        let fixture = Fixture::new();
        let proposal = create_proposal("tasks", descriptor());
        let decoded = CommitProposal::decode(&proposal.encode().unwrap()).unwrap();
        assert_eq!(decoded, proposal);
        let lowered = proposal.transaction().to_homebase().unwrap();
        let expected_writes = history::writes_from_mutations(&lowered.mutations);

        assert_eq!(
            apply(&fixture.writer, &proposal).unwrap(),
            CommitReceipt {
                commit_seq: CommitSeq(1),
                disposition: CommitDisposition::Applied,
            }
        );
        assert_eq!(
            apply(&fixture.writer, &proposal).unwrap(),
            CommitReceipt {
                commit_seq: CommitSeq(1),
                disposition: CommitDisposition::AlreadyCommitted,
            }
        );
        assert!(
            catalog::by_name(&fixture.writer, "tasks")
                .unwrap()
                .is_some()
        );
        assert_eq!(
            history::history_after(&fixture.writer, CommitSeq(0)).unwrap(),
            [history::CommitRecord {
                commit_seq: CommitSeq(1),
                writes: expected_writes,
            }]
        );
    }

    #[test]
    fn create_table_proposals_use_name_keys_for_local_occ() {
        let fixture = Fixture::new();
        let first = create_proposal("tasks", descriptor());
        let collision = create_proposal("TASKS", descriptor());
        let disjoint = create_proposal("projects", descriptor());

        assert_eq!(
            apply(&fixture.writer, &first).unwrap().commit_seq,
            CommitSeq(1)
        );
        assert!(matches!(
            apply(&fixture.writer, &collision),
            Err(Error::CommitConflict(message)) if message.contains("commit sequence 1")
        ));
        assert_eq!(
            apply(&fixture.writer, &disjoint).unwrap().commit_seq,
            CommitSeq(2)
        );
        assert!(
            catalog::by_name(&fixture.writer, "projects")
                .unwrap()
                .is_some()
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

    #[test]
    fn canonical_apply_is_idempotent_and_persists_exact_write_history() {
        let fixture = Fixture::new();
        let proposal = proposal_for_sql(
            &fixture,
            "INSERT INTO notes VALUES (7, 'once')",
            IsolationLevel::Snapshot,
            descriptor(),
            [],
        );
        let lowered = proposal.transaction().to_homebase().unwrap();
        let expected_writes = history::writes_from_mutations(&lowered.mutations);

        assert_eq!(
            apply(&fixture.writer, &proposal).unwrap(),
            CommitReceipt {
                commit_seq: CommitSeq(1),
                disposition: CommitDisposition::Applied,
            }
        );
        assert_eq!(
            apply(&fixture.writer, &proposal).unwrap(),
            CommitReceipt {
                commit_seq: CommitSeq(1),
                disposition: CommitDisposition::AlreadyCommitted,
            }
        );
        assert_eq!(history::current(&fixture.writer).unwrap(), CommitSeq(1));
        assert_eq!(
            history::history_after(&fixture.writer, CommitSeq(0)).unwrap(),
            vec![history::CommitRecord {
                commit_seq: CommitSeq(1),
                writes: expected_writes,
            }]
        );
        assert_eq!(
            fixture
                .writer
                .query_row("SELECT count(*) FROM notes", (), |row| row.get::<_, i64>(0))
                .unwrap(),
            1
        );
        validate(&fixture.writer).unwrap();
        history::validate(&fixture.writer).unwrap();
    }

    #[test]
    fn pruning_occ_history_retains_idempotent_receipts() {
        let fixture = Fixture::new();
        let proposal = proposal_for_sql(
            &fixture,
            "INSERT INTO notes VALUES (7, 'once')",
            IsolationLevel::Snapshot,
            descriptor(),
            [],
        );
        assert_eq!(
            apply(&fixture.writer, &proposal).unwrap().disposition,
            CommitDisposition::Applied
        );
        assert_eq!(history::prune(&fixture.writer, CommitSeq(1)).unwrap(), 1);
        assert!(
            history::history_after(&fixture.writer, CommitSeq(0))
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            apply(&fixture.writer, &proposal).unwrap(),
            CommitReceipt {
                commit_seq: CommitSeq(1),
                disposition: CommitDisposition::AlreadyCommitted,
            }
        );
        assert_eq!(
            fixture
                .writer
                .query_row("SELECT count(*) FROM notes", (), |row| row.get::<_, i64>(0))
                .unwrap(),
            1
        );
        validate(&fixture.writer).unwrap();
    }

    #[test]
    fn validation_requires_without_rowid_for_receipts() {
        let connection = Connection::open_in_memory().unwrap();
        history::initialize(&connection).unwrap();
        initialize(&connection).unwrap();
        connection
            .execute_batch(&format!(
                "DROP TABLE {RECEIPT_TABLE};
                 CREATE TABLE {RECEIPT_TABLE} (
                    proposal_id BLOB PRIMARY KEY NOT NULL,
                    commit_seq BLOB NOT NULL,
                    proposal_hash BLOB NOT NULL
                 )"
            ))
            .unwrap();
        assert!(matches!(
            validate(&connection),
            Err(Error::InvalidDatabase(
                "commit receipt table must use WITHOUT ROWID"
            ))
        ));
    }

    #[test]
    fn validation_rejects_receipts_newer_than_canonical_state() {
        let fixture = Fixture::new();
        let proposal = proposal_for_sql(
            &fixture,
            "INSERT INTO notes VALUES (7, 'once')",
            IsolationLevel::Snapshot,
            descriptor(),
            [],
        );
        apply(&fixture.writer, &proposal).unwrap();
        fixture
            .writer
            .execute(
                &format!("UPDATE {RECEIPT_TABLE} SET commit_seq = x'0000000000000002'"),
                (),
            )
            .unwrap();

        assert!(matches!(
            validate(&fixture.writer),
            Err(Error::InvalidDatabase("commit receipt is malformed"))
        ));
    }

    #[test]
    fn snapshot_occ_accepts_disjoint_rows_and_rejects_the_same_primary_key() {
        let fixture = Fixture::new();
        let first = proposal_for_sql(
            &fixture,
            "INSERT INTO notes VALUES (1, 'first')",
            IsolationLevel::Snapshot,
            descriptor(),
            [],
        );
        let disjoint = proposal_for_sql(
            &fixture,
            "INSERT INTO notes VALUES (2, 'disjoint')",
            IsolationLevel::Snapshot,
            descriptor(),
            [],
        );
        let collision = proposal_for_sql(
            &fixture,
            "INSERT INTO notes VALUES (1, 'collision')",
            IsolationLevel::Snapshot,
            descriptor(),
            [],
        );

        assert_eq!(
            apply(&fixture.writer, &first).unwrap().commit_seq,
            CommitSeq(1)
        );
        assert_eq!(
            apply(&fixture.writer, &disjoint).unwrap().commit_seq,
            CommitSeq(2)
        );
        assert!(matches!(
            apply(&fixture.writer, &collision),
            Err(Error::CommitConflict(message)) if message.contains("commit sequence 1")
        ));
        assert_eq!(history::current(&fixture.writer).unwrap(), CommitSeq(2));
        assert_eq!(
            fixture
                .writer
                .prepare("SELECT id, body FROM notes ORDER BY id")
                .unwrap()
                .query_map((), |row| Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, String>(1)?
                )))
                .unwrap()
                .collect::<rusqlite::Result<Vec<_>>>()
                .unwrap(),
            vec![(1, "first".into()), (2, "disjoint".into())]
        );
    }

    #[test]
    fn serializable_reads_conflict_with_new_writes_but_snapshot_reads_do_not() {
        let fixture = Fixture::new();
        let first = proposal_for_sql(
            &fixture,
            "INSERT INTO notes VALUES (1, 'first')",
            IsolationLevel::Snapshot,
            descriptor(),
            [],
        );
        let first_row = first
            .footprint()
            .writes()
            .first()
            .expect("insert footprint has one row")
            .clone();
        let serializable = proposal_for_sql(
            &fixture,
            "INSERT INTO notes VALUES (2, 'serial')",
            IsolationLevel::Serializable,
            descriptor(),
            [first_row.clone()],
        );
        let snapshot = proposal_for_sql(
            &fixture,
            "INSERT INTO notes VALUES (3, 'snapshot')",
            IsolationLevel::Snapshot,
            descriptor(),
            [first_row],
        );

        apply(&fixture.writer, &first).unwrap();
        assert!(matches!(
            apply(&fixture.writer, &serializable),
            Err(Error::CommitConflict(_))
        ));
        assert_eq!(
            apply(&fixture.writer, &snapshot).unwrap().commit_seq,
            CommitSeq(2)
        );
    }

    #[test]
    fn receipt_failure_rolls_back_replay_and_retry_can_commit() {
        let fixture = Fixture::new();
        fixture
            .writer
            .execute_batch(&format!(
                "CREATE TABLE failure_switch (enabled INTEGER NOT NULL);
                 INSERT INTO failure_switch VALUES (1);
                 CREATE TRIGGER fail_commit_receipt
                 BEFORE INSERT ON {RECEIPT_TABLE}
                 WHEN (SELECT enabled FROM failure_switch) = 1
                 BEGIN SELECT RAISE(ABORT, 'injected receipt failure'); END"
            ))
            .unwrap();
        let proposal = proposal_for_sql(
            &fixture,
            "INSERT INTO notes VALUES (9, 'atomic')",
            IsolationLevel::Snapshot,
            descriptor(),
            [],
        );

        assert!(matches!(
            apply(&fixture.writer, &proposal),
            Err(Error::Sqlite(_))
        ));
        assert_eq!(history::current(&fixture.writer).unwrap(), CommitSeq(0));
        assert_eq!(
            fixture
                .writer
                .query_row("SELECT count(*) FROM notes", (), |row| row.get::<_, i64>(0))
                .unwrap(),
            0
        );

        fixture
            .writer
            .execute("UPDATE failure_switch SET enabled = 0", ())
            .unwrap();
        assert_eq!(
            apply(&fixture.writer, &proposal).unwrap().disposition,
            CommitDisposition::Applied
        );
        assert_eq!(
            fixture
                .writer
                .query_row("SELECT count(*) FROM notes", (), |row| row.get::<_, i64>(0))
                .unwrap(),
            1
        );
    }

    #[test]
    fn one_proposal_id_cannot_name_two_payloads() {
        let fixture = Fixture::new();
        let first = proposal_for_sql(
            &fixture,
            "INSERT INTO notes VALUES (1, 'first')",
            IsolationLevel::Snapshot,
            descriptor(),
            [],
        );
        let mut impostor = proposal_for_sql(
            &fixture,
            "INSERT INTO notes VALUES (2, 'impostor')",
            IsolationLevel::Snapshot,
            descriptor(),
            [],
        );
        impostor.replace_id(first.id());

        apply(&fixture.writer, &first).unwrap();
        assert!(matches!(
            apply(&fixture.writer, &impostor),
            Err(Error::InvalidCommitProposal(message)) if message.contains("another payload")
        ));
    }

    #[test]
    fn commit_namespace_validation_rejects_lookalike_tables() {
        let fixture = Fixture::new();
        assert!(is_initialized(&fixture.writer).unwrap());
        validate(&fixture.writer).unwrap();
        fixture
            .writer
            .execute_batch("CREATE TABLE __multilite__commits_future (value BLOB NOT NULL)")
            .unwrap();
        assert!(matches!(
            is_initialized(&fixture.writer),
            Err(Error::InvalidDatabase(
                "commit receipt namespace contains unexpected tables"
            ))
        ));
    }
}
