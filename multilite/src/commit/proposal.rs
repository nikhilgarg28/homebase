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

use homebase_client::meta::{AdmitCursors, OplogCursors};
use homebase_core::key::Key;
use homebase_core::messages::{AdmittedBatch, PullResponse, RangeAssert};
use homebase_core::range::Range;
use homebase_core::reader::Reader;
use homebase_core::seal::Seal;
use homebase_core::tag::{
    AdmissionSeq, AdmissionTag, AdmittedEntry, CipherEpoch, DeviceChecksum, DeviceEntry, DeviceId,
    DeviceSeq, DeviceTag, Mutation, OpaqueValue, Ver,
};
use homebase_core::writer::Writer;
use rusqlite::Connection;
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

const PROPOSAL_FRAME_VERSION: u8 = 4;
const TAG_PROPOSAL_ID: u8 = 1;
const TAG_SNAPSHOT: u8 = 2;
const TAG_ISOLATION: u8 = 3;
const TAG_KIND: u8 = 4;
const TAG_TRANSACTION: u8 = 5;
const TAG_WRITE: u8 = 10;
const TAG_CONSTRAINT: u8 = 11;
const TAG_READ: u8 = 12;
const TAG_EXPECTED_SUBMIT: u8 = 20;
const TAG_EXPECTED_ADMITS: u8 = 21;
const TAG_APPLY_THROUGH: u8 = 22;
const TAG_LOCAL_DEVICE: u8 = 23;
const TAG_ADMITTED_TRANSACTION: u8 = 24;
const TAG_REJECTED_AT: u8 = 25;
const TAG_ACK_THROUGH: u8 = 26;
const TAG_ACK_CHECKSUM: u8 = 27;
const TAG_PULL_RESPONSE: u8 = 28;

const TRANSACTION_PROPOSAL: u8 = 1;
const APPLY_ADMISSIONS_PROPOSAL: u8 = 2;
const REJECT_SUBMISSIONS_PROPOSAL: u8 = 3;
const ACCEPT_SUBMISSIONS_PROPOSAL: u8 = 4;
const APPEND_ADMISSIONS_PROPOSAL: u8 = 5;

/// Stable identity used to deduplicate an uncertain canonical commit reply.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ProposalId([u8; 16]);

impl ProposalId {
    pub fn new() -> Self {
        Self(Uuid::new_v4().into_bytes())
    }

    fn for_transition(domain: &[u8], transition: &[u8]) -> Self {
        let mut hash = Sha256::new();
        hash.update(domain);
        hash.update(transition);
        let mut bytes: [u8; 32] = hash.finalize().into();
        bytes[6] = (bytes[6] & 0x0f) | 0x40;
        bytes[8] = (bytes[8] & 0x3f) | 0x80;
        Self(
            bytes[..16]
                .try_into()
                .expect("digest prefix is sixteen bytes"),
        )
    }

    pub const fn to_bytes(self) -> [u8; 16] {
        self.0
    }
}

/// One admitted logical transaction and the device that originated it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AdmittedTransaction {
    pub device: DeviceId,
    pub transaction: MultiliteTransaction,
}

/// Snapshot-relative validation and effects for one managed local update.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TransactionProposal {
    snapshot: SnapshotDescriptor,
    isolation: IsolationLevel,
    transaction: MultiliteTransaction,
    footprint: ConflictFootprint,
}

/// Application of one exact captured admission interval.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ApplyAdmissionsProposal {
    expected_submit: OplogCursors,
    expected_admits: AdmitCursors,
    through: AdmissionSeq,
    local_device: DeviceId,
    transactions: Vec<AdmittedTransaction>,
}

/// Repair of one exact definitively rejected submit window.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RejectSubmissionsProposal {
    failed_at: DeviceSeq,
    expected_submit: OplogCursors,
}

/// Retirement of one server-acknowledged submit prefix.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AcceptSubmissionsProposal {
    expected_submit: OplogCursors,
    through: DeviceSeq,
    checksum: DeviceChecksum,
}

/// Durable capture of one authenticated server admission page.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AppendAdmissionsProposal {
    expected_admits: AdmitCursors,
    response: PullResponse,
}

/// The canonical transition requested by one proposal.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ProposalBody {
    Transaction(TransactionProposal),
    ApplyAdmissions(ApplyAdmissionsProposal),
    RejectSubmissions(RejectSubmissionsProposal),
    AcceptSubmissions(AcceptSubmissionsProposal),
    AppendAdmissions(AppendAdmissionsProposal),
}

/// One owned, idempotent proposal for changing canonical local state.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommitProposal {
    id: ProposalId,
    body: ProposalBody,
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
    /// Homebase identity allocated for a locally submitted transaction.
    ///
    /// Authority-only metadata transitions do not create a submission.
    pub submitted: Option<DeviceSeq>,
}

/// One successfully replayed proposal awaiting its group's durable receipt.
pub struct PreparedCommit {
    id: ProposalId,
    hash: [u8; 32],
    writes: Vec<WriteRegion>,
    submitted: Option<DeviceSeq>,
}

impl PreparedCommit {
    pub fn writes(&self) -> &[WriteRegion] {
        &self.writes
    }

    pub fn submitted(&self) -> Option<DeviceSeq> {
        self.submitted
    }

    pub fn with_submission(mut self, submitted: DeviceSeq) -> Self {
        self.submitted = Some(submitted);
        self
    }
}

/// Result of checking one proposal inside a canonical commit group.
pub enum PrepareOutcome {
    Prepared(PreparedCommit),
    AlreadyCommitted(CommitReceipt),
}

impl CommitProposal {
    /// Lower one legacy captured branch into an owned logical proposal.
    #[cfg(test)]
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
        let (_, mut footprint) = transaction.to_homebase()?.into_parts();
        for read in reads {
            footprint.add_read(read);
        }
        Self::from_transaction(snapshot, isolation, transaction, footprint).map(Some)
    }

    /// Build one proposal from a complete ordered logical transaction.
    pub fn from_transaction(
        snapshot: SnapshotDescriptor,
        isolation: IsolationLevel,
        transaction: MultiliteTransaction,
        footprint: ConflictFootprint,
    ) -> Result<Self> {
        let (_, mandatory) = transaction.to_homebase()?.into_parts();
        let body = TransactionProposal {
            snapshot,
            isolation,
            transaction,
            footprint,
        };
        body.validate_mandatory_footprint(&mandatory)?;
        Ok(Self {
            id: ProposalId::new(),
            body: ProposalBody::Transaction(body),
        })
    }

    /// Build one proposal that applies an exact captured admission interval.
    pub fn apply_admissions(
        expected_submit: OplogCursors,
        expected_admits: AdmitCursors,
        through: AdmissionSeq,
        local_device: DeviceId,
        transactions: Vec<AdmittedTransaction>,
    ) -> Result<Self> {
        if through < expected_admits.neck || through > expected_admits.tail {
            return Err(Error::InvalidCommitProposal(
                "admission apply frontier is outside the captured window".into(),
            ));
        }
        let body = ApplyAdmissionsProposal {
            expected_submit,
            expected_admits,
            through,
            local_device,
            transactions,
        };
        let identity = encode_apply_identity(&body)?;
        Ok(Self {
            id: ProposalId::for_transition(b"multilite:apply-admissions:v1\0", &identity),
            body: ProposalBody::ApplyAdmissions(body),
        })
    }

    /// Build one proposal that repairs an exact rejected submit window.
    pub fn reject_submissions(failed_at: DeviceSeq, expected_submit: OplogCursors) -> Result<Self> {
        if failed_at != expected_submit.neck || failed_at >= expected_submit.tail {
            return Err(Error::InvalidCommitProposal(
                "rejected sequence does not identify the active submit head".into(),
            ));
        }
        let body = RejectSubmissionsProposal {
            failed_at,
            expected_submit,
        };
        let identity = encode_rejection_identity(body);
        Ok(Self {
            id: ProposalId::for_transition(b"multilite:reject-submissions:v1\0", &identity),
            body: ProposalBody::RejectSubmissions(body),
        })
    }

    /// Build one proposal that retires an acknowledged submit prefix.
    pub fn accept_submissions(
        expected_submit: OplogCursors,
        through: DeviceSeq,
        checksum: DeviceChecksum,
    ) -> Result<Self> {
        if through < expected_submit.neck || through >= expected_submit.tail {
            return Err(Error::InvalidCommitProposal(
                "acknowledged sequence is outside the active submit window".into(),
            ));
        }
        let body = AcceptSubmissionsProposal {
            expected_submit,
            through,
            checksum,
        };
        let identity = encode_accept_identity(body);
        Ok(Self {
            id: ProposalId::for_transition(b"multilite:accept-submissions:v1\0", &identity),
            body: ProposalBody::AcceptSubmissions(body),
        })
    }

    /// Build one proposal that appends an authenticated pull page.
    pub fn append_admissions(
        expected_admits: AdmitCursors,
        response: PullResponse,
    ) -> Result<Self> {
        response
            .validate_dense()
            .map_err(|error| Error::InvalidCommitProposal(error.to_string()))?;
        let expected_after = AdmissionSeq(
            expected_admits
                .tail
                .0
                .checked_sub(1)
                .ok_or(Error::InvalidDatabase("admit tail cannot be zero"))?,
        );
        if response.after != expected_after {
            return Err(Error::InvalidCommitProposal(
                "pull response does not begin at the expected admit tail".into(),
            ));
        }
        let body = AppendAdmissionsProposal {
            expected_admits,
            response,
        };
        let identity = encode_append_identity(&body)?;
        Ok(Self {
            id: ProposalId::for_transition(b"multilite:append-admissions:v1\0", &identity),
            body: ProposalBody::AppendAdmissions(body),
        })
    }

    pub fn id(&self) -> ProposalId {
        self.id
    }

    pub fn body(&self) -> &ProposalBody {
        &self.body
    }

    pub fn transaction_proposal(&self) -> Option<&TransactionProposal> {
        match &self.body {
            ProposalBody::Transaction(proposal) => Some(proposal),
            ProposalBody::ApplyAdmissions(_)
            | ProposalBody::RejectSubmissions(_)
            | ProposalBody::AcceptSubmissions(_)
            | ProposalBody::AppendAdmissions(_) => None,
        }
    }

    pub fn footprint(&self) -> &ConflictFootprint {
        self.transaction_proposal()
            .expect("only transaction proposals have OCC footprints")
            .footprint()
    }

    pub(crate) fn transaction(&self) -> &MultiliteTransaction {
        self.transaction_proposal()
            .expect("only transaction proposals contain one local transaction")
            .transaction()
    }

    /// Produce the exact Homebase commit represented by this proposal.
    pub fn to_homebase(&self) -> Result<(Vec<Mutation>, Vec<RangeAssert>)> {
        let proposal = self.transaction_proposal().ok_or_else(|| {
            Error::InvalidCommitProposal(
                "only transaction proposals lower to Homebase submissions".into(),
            )
        })?;
        let (mutations, mandatory) = proposal.transaction.to_homebase()?.into_parts();
        proposal.validate_mandatory_footprint(&mandatory)?;
        let assertions = proposal.footprint.clone().plan(
            proposal.isolation,
            proposal.snapshot.authority_applied_through,
        );
        Ok((mutations, assertions))
    }

    /// Cross-check every body-specific invariant that is independent of state.
    pub fn validate(&self) -> Result<()> {
        match &self.body {
            ProposalBody::Transaction(proposal) => {
                let (_, mandatory) = proposal.transaction.to_homebase()?.into_parts();
                proposal.validate_mandatory_footprint(&mandatory)
            }
            ProposalBody::ApplyAdmissions(proposal) => {
                if proposal.through < proposal.expected_admits.neck
                    || proposal.through > proposal.expected_admits.tail
                {
                    return Err(Error::InvalidCommitProposal(
                        "admission apply frontier is outside the captured window".into(),
                    ));
                }
                Ok(())
            }
            ProposalBody::RejectSubmissions(proposal) => {
                if proposal.failed_at != proposal.expected_submit.neck
                    || proposal.failed_at >= proposal.expected_submit.tail
                {
                    return Err(Error::InvalidCommitProposal(
                        "rejected sequence does not identify the active submit head".into(),
                    ));
                }
                Ok(())
            }
            ProposalBody::AcceptSubmissions(proposal) => {
                if proposal.through < proposal.expected_submit.neck
                    || proposal.through >= proposal.expected_submit.tail
                {
                    return Err(Error::InvalidCommitProposal(
                        "acknowledged sequence is outside the active submit window".into(),
                    ));
                }
                Ok(())
            }
            ProposalBody::AppendAdmissions(proposal) => {
                proposal
                    .response
                    .validate_dense()
                    .map_err(|error| Error::InvalidCommitProposal(error.to_string()))?;
                let expected_after = AdmissionSeq(
                    proposal
                        .expected_admits
                        .tail
                        .0
                        .checked_sub(1)
                        .ok_or(Error::InvalidDatabase("admit tail cannot be zero"))?,
                );
                if proposal.response.after != expected_after {
                    return Err(Error::InvalidCommitProposal(
                        "pull response does not begin at the expected admit tail".into(),
                    ));
                }
                Ok(())
            }
        }
    }

    /// Encode every input needed for validation, materialization, and submission.
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut writer = Writer::new();
        writer.u8(PROPOSAL_FRAME_VERSION);
        {
            let mut field = |tag, value: &[u8]| {
                writer
                    .field(tag, value)
                    .map_err(|_| Error::InvalidCommitProposal("proposal field is too large".into()))
            };
            field(TAG_PROPOSAL_ID, &self.id.0)?;
            match &self.body {
                ProposalBody::Transaction(proposal) => {
                    field(TAG_KIND, &[TRANSACTION_PROPOSAL])?;
                    field(TAG_SNAPSHOT, &proposal.snapshot.encode())?;
                    field(TAG_ISOLATION, &[encode_isolation(proposal.isolation)])?;
                    field(TAG_TRANSACTION, &proposal.transaction.encode())?;
                    for key in proposal.footprint.writes() {
                        field(TAG_WRITE, &key.encode())?;
                    }
                    for key in proposal.footprint.constraints() {
                        field(TAG_CONSTRAINT, &key.encode())?;
                    }
                    for key in proposal.footprint.reads() {
                        field(TAG_READ, &key.encode())?;
                    }
                }
                ProposalBody::ApplyAdmissions(proposal) => {
                    field(TAG_KIND, &[APPLY_ADMISSIONS_PROPOSAL])?;
                    field(
                        TAG_EXPECTED_SUBMIT,
                        &encode_oplog_cursors(proposal.expected_submit),
                    )?;
                    field(
                        TAG_EXPECTED_ADMITS,
                        &encode_admit_cursors(proposal.expected_admits),
                    )?;
                    field(TAG_APPLY_THROUGH, &proposal.through.0.to_be_bytes())?;
                    field(TAG_LOCAL_DEVICE, &proposal.local_device.0)?;
                    for transaction in &proposal.transactions {
                        field(
                            TAG_ADMITTED_TRANSACTION,
                            &encode_admitted_transaction(transaction)?,
                        )?;
                    }
                }
                ProposalBody::RejectSubmissions(proposal) => {
                    field(TAG_KIND, &[REJECT_SUBMISSIONS_PROPOSAL])?;
                    field(
                        TAG_EXPECTED_SUBMIT,
                        &encode_oplog_cursors(proposal.expected_submit),
                    )?;
                    field(TAG_REJECTED_AT, &proposal.failed_at.0.to_be_bytes())?;
                }
                ProposalBody::AcceptSubmissions(proposal) => {
                    field(TAG_KIND, &[ACCEPT_SUBMISSIONS_PROPOSAL])?;
                    field(
                        TAG_EXPECTED_SUBMIT,
                        &encode_oplog_cursors(proposal.expected_submit),
                    )?;
                    field(TAG_ACK_THROUGH, &proposal.through.0.to_be_bytes())?;
                    field(TAG_ACK_CHECKSUM, &proposal.checksum.0)?;
                }
                ProposalBody::AppendAdmissions(proposal) => {
                    field(TAG_KIND, &[APPEND_ADMISSIONS_PROPOSAL])?;
                    field(
                        TAG_EXPECTED_ADMITS,
                        &encode_admit_cursors(proposal.expected_admits),
                    )?;
                    field(
                        TAG_PULL_RESPONSE,
                        &encode_pull_response(&proposal.response)?,
                    )?;
                }
            }
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
        let mut kind = None;
        let mut snapshot = None;
        let mut isolation = None;
        let mut transaction = None;
        let mut writes = Vec::new();
        let mut constraints = Vec::new();
        let mut reads = Vec::new();
        let mut expected_submit = None;
        let mut expected_admits = None;
        let mut apply_through = None;
        let mut local_device = None;
        let mut admitted_transactions = Vec::new();
        let mut rejected_at = None;
        let mut ack_through = None;
        let mut ack_checksum = None;
        let mut pull_response = None;
        while let Some((tag, value)) = reader.field().map_err(|_| ProposalCodecError::Truncated)? {
            match tag {
                TAG_PROPOSAL_ID => set_once(&mut id, ProposalId(uuid_bytes(value)?), tag)?,
                TAG_KIND => {
                    let [value] = value else {
                        return Err(ProposalCodecError::InvalidLength);
                    };
                    set_once(&mut kind, *value, tag)?;
                }
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
                TAG_EXPECTED_SUBMIT => {
                    set_once(&mut expected_submit, decode_oplog_cursors(value)?, tag)?
                }
                TAG_EXPECTED_ADMITS => {
                    set_once(&mut expected_admits, decode_admit_cursors(value)?, tag)?
                }
                TAG_APPLY_THROUGH => {
                    set_once(&mut apply_through, AdmissionSeq(decode_u64(value)?), tag)?
                }
                TAG_LOCAL_DEVICE => set_once(&mut local_device, DeviceId(bytes16(value)?), tag)?,
                TAG_ADMITTED_TRANSACTION => {
                    admitted_transactions.push(decode_admitted_transaction(value)?)
                }
                TAG_REJECTED_AT => set_once(&mut rejected_at, DeviceSeq(decode_u64(value)?), tag)?,
                TAG_ACK_THROUGH => set_once(&mut ack_through, DeviceSeq(decode_u64(value)?), tag)?,
                TAG_ACK_CHECKSUM => {
                    set_once(&mut ack_checksum, DeviceChecksum(bytes32(value)?), tag)?
                }
                TAG_PULL_RESPONSE => {
                    set_once(&mut pull_response, decode_pull_response(value)?, tag)?
                }
                _ => {}
            }
        }
        let kind = kind.ok_or(ProposalCodecError::MissingField(TAG_KIND))?;
        let body = match kind {
            TRANSACTION_PROPOSAL => {
                if !canonical_keys(&writes)
                    || !canonical_keys(&constraints)
                    || !canonical_keys(&reads)
                {
                    return Err(ProposalCodecError::InvalidFootprint);
                }
                ProposalBody::Transaction(TransactionProposal {
                    snapshot: snapshot.ok_or(ProposalCodecError::MissingField(TAG_SNAPSHOT))?,
                    isolation: isolation.ok_or(ProposalCodecError::MissingField(TAG_ISOLATION))?,
                    transaction: transaction
                        .ok_or(ProposalCodecError::MissingField(TAG_TRANSACTION))?,
                    footprint: ConflictFootprint::from_parts(writes, constraints, reads),
                })
            }
            APPLY_ADMISSIONS_PROPOSAL => ProposalBody::ApplyAdmissions(ApplyAdmissionsProposal {
                expected_submit: expected_submit
                    .ok_or(ProposalCodecError::MissingField(TAG_EXPECTED_SUBMIT))?,
                expected_admits: expected_admits
                    .ok_or(ProposalCodecError::MissingField(TAG_EXPECTED_ADMITS))?,
                through: apply_through
                    .ok_or(ProposalCodecError::MissingField(TAG_APPLY_THROUGH))?,
                local_device: local_device
                    .ok_or(ProposalCodecError::MissingField(TAG_LOCAL_DEVICE))?,
                transactions: admitted_transactions,
            }),
            REJECT_SUBMISSIONS_PROPOSAL => {
                ProposalBody::RejectSubmissions(RejectSubmissionsProposal {
                    failed_at: rejected_at
                        .ok_or(ProposalCodecError::MissingField(TAG_REJECTED_AT))?,
                    expected_submit: expected_submit
                        .ok_or(ProposalCodecError::MissingField(TAG_EXPECTED_SUBMIT))?,
                })
            }
            ACCEPT_SUBMISSIONS_PROPOSAL => {
                ProposalBody::AcceptSubmissions(AcceptSubmissionsProposal {
                    expected_submit: expected_submit
                        .ok_or(ProposalCodecError::MissingField(TAG_EXPECTED_SUBMIT))?,
                    through: ack_through
                        .ok_or(ProposalCodecError::MissingField(TAG_ACK_THROUGH))?,
                    checksum: ack_checksum
                        .ok_or(ProposalCodecError::MissingField(TAG_ACK_CHECKSUM))?,
                })
            }
            APPEND_ADMISSIONS_PROPOSAL => {
                ProposalBody::AppendAdmissions(AppendAdmissionsProposal {
                    expected_admits: expected_admits
                        .ok_or(ProposalCodecError::MissingField(TAG_EXPECTED_ADMITS))?,
                    response: pull_response
                        .ok_or(ProposalCodecError::MissingField(TAG_PULL_RESPONSE))?,
                })
            }
            kind => return Err(ProposalCodecError::UnknownKind(kind)),
        };
        let proposal = Self {
            id: id.ok_or(ProposalCodecError::MissingField(TAG_PROPOSAL_ID))?,
            body,
        };
        if let ProposalBody::Transaction(transaction) = &proposal.body {
            let (_, mandatory) = transaction
                .transaction
                .to_homebase()
                .map_err(|error| ProposalCodecError::InvalidTransaction(error.to_string()))?
                .into_parts();
            if transaction.footprint.writes() != mandatory.writes()
                || transaction.footprint.constraints() != mandatory.constraints()
            {
                return Err(ProposalCodecError::InvalidFootprint);
            }
        }
        proposal
            .validate()
            .map_err(|error| ProposalCodecError::InvalidProposal(error.to_string()))?;
        Ok(proposal)
    }

    pub fn prepare_receipt(&self, writes: Vec<WriteRegion>) -> Result<PreparedCommit> {
        Ok(PreparedCommit {
            id: self.id,
            hash: proposal_hash(&self.encode()?),
            writes,
            submitted: None,
        })
    }

    pub fn committed_receipt(&self, connection: &Connection) -> Result<Option<CommitReceipt>> {
        committed_receipt(connection, self)
    }

    #[cfg(test)]
    fn replace_id(&mut self, id: ProposalId) {
        self.id = id;
    }
}

impl TransactionProposal {
    pub fn snapshot(&self) -> SnapshotDescriptor {
        self.snapshot
    }

    pub fn isolation(&self) -> IsolationLevel {
        self.isolation
    }

    pub fn footprint(&self) -> &ConflictFootprint {
        &self.footprint
    }

    pub fn transaction(&self) -> &MultiliteTransaction {
        &self.transaction
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
}

impl ApplyAdmissionsProposal {
    pub fn expected_submit(&self) -> OplogCursors {
        self.expected_submit
    }

    pub fn expected_admits(&self) -> AdmitCursors {
        self.expected_admits
    }

    pub fn through(&self) -> AdmissionSeq {
        self.through
    }

    pub fn local_device(&self) -> DeviceId {
        self.local_device
    }

    pub fn transactions(&self) -> &[AdmittedTransaction] {
        &self.transactions
    }
}

impl RejectSubmissionsProposal {
    pub fn failed_at(&self) -> DeviceSeq {
        self.failed_at
    }

    pub fn expected_submit(&self) -> OplogCursors {
        self.expected_submit
    }
}

impl AcceptSubmissionsProposal {
    pub fn expected_submit(&self) -> OplogCursors {
        self.expected_submit
    }

    pub fn through(&self) -> DeviceSeq {
        self.through
    }

    pub fn checksum(&self) -> DeviceChecksum {
        self.checksum
    }
}

impl AppendAdmissionsProposal {
    pub fn expected_admits(&self) -> AdmitCursors {
        self.expected_admits
    }

    pub fn response(&self) -> &PullResponse {
        &self.response
    }
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
                submitted: prepared.submitted,
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

    let transaction = proposal.transaction_proposal().ok_or_else(|| {
        Error::InvalidCommitProposal(
            "body-specific preparation is required for this proposal".into(),
        )
    })?;
    let current = history::current(connection)?;
    if transaction.snapshot().commit_seq > current {
        return Err(Error::CommitConflict(
            "proposal snapshot is newer than canonical SQLite".into(),
        ));
    }
    proposal.validate()?;
    for committed in history::history_after(connection, transaction.snapshot().commit_seq)? {
        if transaction
            .footprint()
            .conflicts_with_writes(transaction.isolation(), &committed.writes)
        {
            return Err(Error::CommitConflict(format!(
                "proposal conflicts with local commit sequence {}",
                committed.commit_seq.0
            )));
        }
    }
    if transaction
        .footprint()
        .conflicts_with_writes(transaction.isolation(), accepted_writes)
    {
        return Err(Error::CommitConflict(
            "proposal conflicts with an earlier proposal in its commit group".into(),
        ));
    }
    let lowered = transaction.transaction().to_homebase()?;
    let writes = history::writes_from_mutations(&lowered.mutations);
    transaction
        .transaction()
        .apply(connection)
        .map_err(|error| match error {
            Error::Sqlite(error) => Error::CommitConflict(error.to_string()),
            error => error,
        })?;
    Ok(PrepareOutcome::Prepared(proposal.prepare_receipt(writes)?))
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
    history.record_group(
        connection,
        prepared
            .iter()
            .map(|commit| history::PreparedRecord {
                proposal_id: commit.id.to_bytes(),
                proposal_hash: commit.hash,
                submitted: commit.submitted,
                writes: commit.writes.clone(),
            })
            .collect(),
    )
}

fn committed_receipt(
    connection: &Connection,
    proposal: &CommitProposal,
) -> Result<Option<CommitReceipt>> {
    history::committed(connection, proposal.id().to_bytes())?
        .map(|stored| {
            let encoded = proposal.encode()?;
            if stored.proposal_hash != proposal_hash(&encoded) {
                return Err(Error::InvalidCommitProposal(
                    "proposal id is already committed with another payload".into(),
                ));
            }
            Ok(CommitReceipt {
                commit_seq: stored.commit_seq,
                disposition: CommitDisposition::AlreadyCommitted,
                submitted: stored.submitted,
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

fn encode_oplog_cursors(cursors: OplogCursors) -> [u8; 24] {
    encode_cursors(cursors.head.0, cursors.neck.0, cursors.tail.0)
}

fn decode_oplog_cursors(frame: &[u8]) -> std::result::Result<OplogCursors, ProposalCodecError> {
    let [head, neck, tail] = decode_cursors(frame)?;
    Ok(OplogCursors {
        head: DeviceSeq(head),
        neck: DeviceSeq(neck),
        tail: DeviceSeq(tail),
    })
}

fn encode_admit_cursors(cursors: AdmitCursors) -> [u8; 24] {
    encode_cursors(cursors.head.0, cursors.neck.0, cursors.tail.0)
}

fn decode_admit_cursors(frame: &[u8]) -> std::result::Result<AdmitCursors, ProposalCodecError> {
    let [head, neck, tail] = decode_cursors(frame)?;
    Ok(AdmitCursors {
        head: AdmissionSeq(head),
        neck: AdmissionSeq(neck),
        tail: AdmissionSeq(tail),
    })
}

fn encode_cursors(head: u64, neck: u64, tail: u64) -> [u8; 24] {
    let mut frame = [0; 24];
    frame[..8].copy_from_slice(&head.to_be_bytes());
    frame[8..16].copy_from_slice(&neck.to_be_bytes());
    frame[16..].copy_from_slice(&tail.to_be_bytes());
    frame
}

fn decode_cursors(frame: &[u8]) -> std::result::Result<[u64; 3], ProposalCodecError> {
    let frame: [u8; 24] = frame
        .try_into()
        .map_err(|_| ProposalCodecError::InvalidLength)?;
    Ok([
        u64::from_be_bytes(frame[..8].try_into().expect("cursor slice is eight bytes")),
        u64::from_be_bytes(
            frame[8..16]
                .try_into()
                .expect("cursor slice is eight bytes"),
        ),
        u64::from_be_bytes(frame[16..].try_into().expect("cursor slice is eight bytes")),
    ])
}

fn encode_admitted_transaction(transaction: &AdmittedTransaction) -> Result<Vec<u8>> {
    let encoded = transaction.transaction.encode();
    let mut frame = Vec::with_capacity(16 + encoded.len());
    frame.extend_from_slice(&transaction.device.0);
    frame.extend_from_slice(&encoded);
    Ok(frame)
}

fn encode_apply_identity(proposal: &ApplyAdmissionsProposal) -> Result<Vec<u8>> {
    let mut writer = Writer::new();
    writer.bytes(&encode_oplog_cursors(proposal.expected_submit));
    writer.bytes(&encode_admit_cursors(proposal.expected_admits));
    writer.u64(proposal.through.0);
    writer.bytes(&proposal.local_device.0);
    writer.u32(
        u32::try_from(proposal.transactions.len())
            .map_err(|_| Error::InvalidCommitProposal("too many admitted transactions".into()))?,
    );
    for admitted in &proposal.transactions {
        let encoded = encode_admitted_transaction(admitted)?;
        writer.u32(u32::try_from(encoded.len()).map_err(|_| {
            Error::InvalidCommitProposal("admitted transaction is too large".into())
        })?);
        writer.bytes(&encoded);
    }
    Ok(writer.finish())
}

fn encode_rejection_identity(proposal: RejectSubmissionsProposal) -> [u8; 32] {
    let mut identity = [0; 32];
    identity[..24].copy_from_slice(&encode_oplog_cursors(proposal.expected_submit));
    identity[24..].copy_from_slice(&proposal.failed_at.0.to_be_bytes());
    identity
}

fn encode_accept_identity(proposal: AcceptSubmissionsProposal) -> [u8; 64] {
    let mut identity = [0; 64];
    identity[..24].copy_from_slice(&encode_oplog_cursors(proposal.expected_submit));
    identity[24..32].copy_from_slice(&proposal.through.0.to_be_bytes());
    identity[32..].copy_from_slice(&proposal.checksum.0);
    identity
}

fn encode_append_identity(proposal: &AppendAdmissionsProposal) -> Result<Vec<u8>> {
    let response = encode_pull_response(&proposal.response)?;
    let mut identity = Vec::with_capacity(24 + response.len());
    identity.extend_from_slice(&encode_admit_cursors(proposal.expected_admits));
    identity.extend_from_slice(&response);
    Ok(identity)
}

const PULL_FRAME_VERSION: u8 = 1;
const SET_MUTATION: u8 = 1;
const DELETE_MUTATION: u8 = 2;
const DELETE_RANGE_MUTATION: u8 = 3;

fn encode_pull_response(response: &PullResponse) -> Result<Vec<u8>> {
    let mut writer = Writer::new();
    writer.u8(PULL_FRAME_VERSION);
    writer.u64(response.after.0);
    writer.u64(response.through.0);
    put_count(
        &mut writer,
        response.batches.len(),
        "too many admitted batches",
    )?;
    for batch in &response.batches {
        put_blob(
            &mut writer,
            &encode_admitted_batch(batch)?,
            "admitted batch is too large",
        )?;
    }
    Ok(writer.finish())
}

fn decode_pull_response(frame: &[u8]) -> std::result::Result<PullResponse, ProposalCodecError> {
    let mut reader = Reader::new(frame);
    if reader.u8() != Some(PULL_FRAME_VERSION) {
        return Err(ProposalCodecError::InvalidPullResponse);
    }
    let after = AdmissionSeq(reader.u64().ok_or(ProposalCodecError::Truncated)?);
    let through = AdmissionSeq(reader.u64().ok_or(ProposalCodecError::Truncated)?);
    let count = reader.u32().ok_or(ProposalCodecError::Truncated)?;
    let mut batches = Vec::with_capacity(count as usize);
    for _ in 0..count {
        batches.push(decode_admitted_batch(take_blob(&mut reader)?)?);
    }
    if reader.end().is_none() {
        return Err(ProposalCodecError::TrailingBytes);
    }
    let response = PullResponse {
        after,
        through,
        batches,
    };
    response
        .validate_dense()
        .map_err(|_| ProposalCodecError::InvalidPullResponse)?;
    Ok(response)
}

fn encode_admitted_batch(batch: &AdmittedBatch) -> Result<Vec<u8>> {
    let mut writer = Writer::new();
    writer.u64(batch.admission_seq.0);
    writer.bytes(&batch.device.0);
    writer.u64(batch.device_seq.0);
    writer.bytes(&batch.checksum.0);
    put_count(
        &mut writer,
        batch.entries.len(),
        "too many admitted entries",
    )?;
    for entry in &batch.entries {
        put_blob(
            &mut writer,
            &encode_admitted_entry(entry)?,
            "admitted entry is too large",
        )?;
    }
    Ok(writer.finish())
}

fn decode_admitted_batch(frame: &[u8]) -> std::result::Result<AdmittedBatch, ProposalCodecError> {
    let mut reader = Reader::new(frame);
    let admission_seq = AdmissionSeq(reader.u64().ok_or(ProposalCodecError::Truncated)?);
    let device = DeviceId(take_array::<16>(&mut reader)?);
    let device_seq = DeviceSeq(reader.u64().ok_or(ProposalCodecError::Truncated)?);
    let checksum = DeviceChecksum(take_array::<32>(&mut reader)?);
    let count = reader.u32().ok_or(ProposalCodecError::Truncated)?;
    let mut entries = Vec::with_capacity(count as usize);
    for _ in 0..count {
        entries.push(decode_admitted_entry(take_blob(&mut reader)?)?);
    }
    if reader.end().is_none() {
        return Err(ProposalCodecError::TrailingBytes);
    }
    let batch = AdmittedBatch {
        admission_seq,
        device,
        device_seq,
        checksum,
        entries,
    };
    batch
        .validate()
        .map_err(|_| ProposalCodecError::InvalidPullResponse)?;
    Ok(batch)
}

fn encode_admitted_entry(entry: &AdmittedEntry) -> Result<Vec<u8>> {
    let mut writer = Writer::new();
    put_blob(
        &mut writer,
        &encode_opaque_mutation(&entry.device_entry.mutation)?,
        "admitted mutation is too large",
    )?;
    writer.bytes(&entry.device_entry.tag.device.0);
    writer.u64(entry.device_entry.tag.device_seq.0);
    writer.u64(entry.device_entry.tag.ver.0);
    writer.u64(entry.device_entry.tag.cipher_epoch.0);
    put_blob(
        &mut writer,
        &entry.device_entry.seal.encode(),
        "admitted seal is too large",
    )?;
    writer.u64(entry.admission.admission_seq.0);
    writer.u32(entry.admission.op_index);
    Ok(writer.finish())
}

fn decode_admitted_entry(frame: &[u8]) -> std::result::Result<AdmittedEntry, ProposalCodecError> {
    let mut reader = Reader::new(frame);
    let mutation = decode_opaque_mutation(take_blob(&mut reader)?)?;
    let tag = DeviceTag {
        device: DeviceId(take_array::<16>(&mut reader)?),
        device_seq: DeviceSeq(reader.u64().ok_or(ProposalCodecError::Truncated)?),
        ver: Ver(reader.u64().ok_or(ProposalCodecError::Truncated)?),
        cipher_epoch: CipherEpoch(reader.u64().ok_or(ProposalCodecError::Truncated)?),
    };
    let seal = Seal::decode(take_blob(&mut reader)?)
        .map_err(|_| ProposalCodecError::InvalidPullResponse)?;
    let admission = AdmissionTag {
        admission_seq: AdmissionSeq(reader.u64().ok_or(ProposalCodecError::Truncated)?),
        op_index: reader.u32().ok_or(ProposalCodecError::Truncated)?,
    };
    if reader.end().is_none() {
        return Err(ProposalCodecError::TrailingBytes);
    }
    Ok(AdmittedEntry {
        device_entry: DeviceEntry {
            mutation,
            tag,
            seal,
        },
        admission,
    })
}

fn encode_opaque_mutation(mutation: &Mutation<OpaqueValue>) -> Result<Vec<u8>> {
    let mut writer = Writer::new();
    match mutation {
        Mutation::Set { key, value } => {
            writer.u8(SET_MUTATION);
            put_blob(&mut writer, &key.encode(), "admitted key is too large")?;
            put_blob(&mut writer, &value.0, "admitted ciphertext is too large")?;
        }
        Mutation::Delete { key } => {
            writer.u8(DELETE_MUTATION);
            put_blob(&mut writer, &key.encode(), "admitted key is too large")?;
        }
        Mutation::DeleteRange { range } => {
            writer.u8(DELETE_RANGE_MUTATION);
            put_blob(&mut writer, &range.encode(), "admitted range is too large")?;
        }
    }
    Ok(writer.finish())
}

fn decode_opaque_mutation(
    frame: &[u8],
) -> std::result::Result<Mutation<OpaqueValue>, ProposalCodecError> {
    let mut reader = Reader::new(frame);
    let mutation = match reader.u8().ok_or(ProposalCodecError::Truncated)? {
        SET_MUTATION => Mutation::Set {
            key: Key::decode(take_blob(&mut reader)?)
                .map_err(|_| ProposalCodecError::InvalidPullResponse)?,
            value: OpaqueValue(take_blob(&mut reader)?.to_vec()),
        },
        DELETE_MUTATION => Mutation::Delete {
            key: Key::decode(take_blob(&mut reader)?)
                .map_err(|_| ProposalCodecError::InvalidPullResponse)?,
        },
        DELETE_RANGE_MUTATION => Mutation::DeleteRange {
            range: Range::decode(take_blob(&mut reader)?)
                .ok_or(ProposalCodecError::InvalidPullResponse)?,
        },
        _ => return Err(ProposalCodecError::InvalidPullResponse),
    };
    if reader.end().is_none() {
        return Err(ProposalCodecError::TrailingBytes);
    }
    Ok(mutation)
}

fn put_count(writer: &mut Writer, count: usize, message: &'static str) -> Result<()> {
    writer.u32(u32::try_from(count).map_err(|_| Error::InvalidCommitProposal(message.into()))?);
    Ok(())
}

fn put_blob(writer: &mut Writer, bytes: &[u8], message: &'static str) -> Result<()> {
    writer
        .u32(u32::try_from(bytes.len()).map_err(|_| Error::InvalidCommitProposal(message.into()))?);
    writer.bytes(bytes);
    Ok(())
}

fn take_blob<'a>(reader: &mut Reader<'a>) -> std::result::Result<&'a [u8], ProposalCodecError> {
    let length = reader.u32().ok_or(ProposalCodecError::Truncated)?;
    let length = usize::try_from(length).map_err(|_| ProposalCodecError::InvalidLength)?;
    reader.take(length).ok_or(ProposalCodecError::Truncated)
}

fn take_array<const N: usize>(
    reader: &mut Reader<'_>,
) -> std::result::Result<[u8; N], ProposalCodecError> {
    reader
        .take(N)
        .ok_or(ProposalCodecError::Truncated)?
        .try_into()
        .map_err(|_| ProposalCodecError::InvalidLength)
}

fn decode_admitted_transaction(
    frame: &[u8],
) -> std::result::Result<AdmittedTransaction, ProposalCodecError> {
    if frame.len() <= 16 {
        return Err(ProposalCodecError::InvalidLength);
    }
    Ok(AdmittedTransaction {
        device: DeviceId(bytes16(&frame[..16])?),
        transaction: MultiliteTransaction::decode(&frame[16..])
            .map_err(|error| ProposalCodecError::InvalidTransaction(error.to_string()))?,
    })
}

fn decode_u64(frame: &[u8]) -> std::result::Result<u64, ProposalCodecError> {
    let frame: [u8; 8] = frame
        .try_into()
        .map_err(|_| ProposalCodecError::InvalidLength)?;
    Ok(u64::from_be_bytes(frame))
}

fn bytes16(value: &[u8]) -> std::result::Result<[u8; 16], ProposalCodecError> {
    value
        .try_into()
        .map_err(|_| ProposalCodecError::InvalidLength)
}

fn bytes32(value: &[u8]) -> std::result::Result<[u8; 32], ProposalCodecError> {
    value
        .try_into()
        .map_err(|_| ProposalCodecError::InvalidLength)
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
    UnknownKind(u8),
    Truncated,
    DuplicateField(u8),
    MissingField(u8),
    InvalidLength,
    InvalidUuid,
    InvalidIsolation,
    InvalidSnapshot(String),
    InvalidTransaction(String),
    InvalidProposal(String),
    InvalidPullResponse,
    TrailingBytes,
    InvalidKey(String),
    InvalidFootprint,
}

impl fmt::Display for ProposalCodecError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownVersion => formatter.write_str("unknown commit proposal version"),
            Self::UnknownKind(kind) => write!(formatter, "unknown commit proposal kind {kind}"),
            Self::Truncated => formatter.write_str("commit proposal is truncated"),
            Self::DuplicateField(tag) => write!(formatter, "duplicate proposal field {tag}"),
            Self::MissingField(tag) => write!(formatter, "missing proposal field {tag}"),
            Self::InvalidLength => formatter.write_str("proposal field has an invalid length"),
            Self::InvalidUuid => formatter.write_str("proposal id is not a UUID v4"),
            Self::InvalidIsolation => formatter.write_str("proposal isolation level is invalid"),
            Self::InvalidSnapshot(error) => write!(formatter, "invalid snapshot: {error}"),
            Self::InvalidTransaction(error) => write!(formatter, "invalid transaction: {error}"),
            Self::InvalidProposal(error) => write!(formatter, "invalid proposal: {error}"),
            Self::InvalidPullResponse => formatter.write_str("invalid pull response"),
            Self::TrailingBytes => formatter.write_str("proposal frame has trailing bytes"),
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
        CreateColumn, CreateTable, CreateTableSpec, SqlName, TypeDeclaration,
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
            history::initialize(&writer).unwrap();
            let created = CreateTable::new(
                "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT NOT NULL)",
                CreateTableSpec {
                    name: SqlName::new("notes".into()),
                    mode: Default::default(),
                    storage: crate::database::schema::TableStorage::Rowid,
                    columns: vec![
                        CreateColumn {
                            name: SqlName::new("id".into()),
                            declared_type: TypeDeclaration::integer(),
                            not_null: false,
                            not_null_name: None,
                            default: None,
                            primary_key: Some(0),
                        },
                        CreateColumn {
                            name: SqlName::new("body".into()),
                            declared_type: TypeDeclaration::text(),
                            not_null: true,
                            not_null_name: None,
                            default: None,
                            primary_key: None,
                        },
                    ],
                    unique_constraints: Vec::new(),
                    foreign_keys: Vec::new(),
                    primary_key_name: None,
                    checks: Vec::new(),
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
            WritableBranch::open_for_changeset_capture(self.snapshot(), OverlayOptions::default())
                .unwrap()
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
                    mode: Default::default(),
                    storage: crate::database::schema::TableStorage::Rowid,
                    columns: vec![CreateColumn {
                        name: SqlName::new("id".into()),
                        declared_type: TypeDeclaration::integer(),
                        not_null: false,
                        not_null_name: None,
                        default: None,
                        primary_key: Some(0),
                    }],
                    unique_constraints: Vec::new(),
                    foreign_keys: Vec::new(),
                    primary_key_name: None,
                    checks: Vec::new(),
                },
            ))])
            .unwrap();
        let (_, footprint) = transaction.to_homebase().unwrap().into_parts();
        CommitProposal::from_transaction(snapshot, IsolationLevel::Snapshot, transaction, footprint)
            .unwrap()
    }

    fn pull_response() -> PullResponse {
        let device = DeviceId([9; 16]);
        let device_seq = DeviceSeq(4);
        let admission_seq = AdmissionSeq(1);
        PullResponse {
            after: AdmissionSeq(0),
            through: admission_seq,
            batches: vec![AdmittedBatch {
                admission_seq,
                device,
                device_seq,
                checksum: DeviceChecksum([3; 32]),
                entries: vec![AdmittedEntry {
                    device_entry: DeviceEntry {
                        mutation: Mutation::Set {
                            key: Key::from_bytes([b"opaque".as_slice(), b"key".as_slice()])
                                .unwrap(),
                            value: OpaqueValue(vec![1, 2, 3]),
                        },
                        tag: DeviceTag {
                            device,
                            device_seq,
                            ver: Ver(7),
                            cipher_epoch: CipherEpoch(2),
                        },
                        seal: Seal::empty_aead_v1(),
                    },
                    admission: AdmissionTag {
                        admission_seq,
                        op_index: 0,
                    },
                }],
            }],
        }
    }

    #[test]
    fn every_authority_proposal_roundtrips_with_stable_identity() {
        let transaction = create_proposal("remote", descriptor())
            .transaction()
            .clone();
        let submit = OplogCursors {
            head: DeviceSeq(1),
            neck: DeviceSeq(1),
            tail: DeviceSeq(3),
        };
        let admits = AdmitCursors {
            head: AdmissionSeq(1),
            neck: AdmissionSeq(1),
            tail: AdmissionSeq(2),
        };
        let proposals = [
            CommitProposal::apply_admissions(
                OplogCursors::default(),
                admits,
                AdmissionSeq(2),
                DeviceId([1; 16]),
                vec![AdmittedTransaction {
                    device: DeviceId([2; 16]),
                    transaction,
                }],
            )
            .unwrap(),
            CommitProposal::reject_submissions(DeviceSeq(1), submit).unwrap(),
            CommitProposal::accept_submissions(submit, DeviceSeq(2), DeviceChecksum([5; 32]))
                .unwrap(),
            CommitProposal::append_admissions(AdmitCursors::default(), pull_response()).unwrap(),
        ];

        for proposal in proposals {
            let decoded = CommitProposal::decode(&proposal.encode().unwrap()).unwrap();
            assert_eq!(decoded, proposal);
            assert_eq!(decoded.id(), proposal.id());
        }
    }

    #[test]
    fn pull_response_codec_rejects_truncation_and_trailing_bytes() {
        let mut encoded = encode_pull_response(&pull_response()).unwrap();
        assert_eq!(
            decode_pull_response(&encoded[..encoded.len() - 1]),
            Err(ProposalCodecError::Truncated)
        );
        encoded.push(0);
        assert_eq!(
            decode_pull_response(&encoded),
            Err(ProposalCodecError::TrailingBytes)
        );
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
                submitted: None,
            }
        );
        assert_eq!(
            apply(&fixture.writer, &proposal).unwrap(),
            CommitReceipt {
                commit_seq: CommitSeq(1),
                disposition: CommitDisposition::AlreadyCommitted,
                submitted: None,
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
                submitted: None,
            }
        );
        assert_eq!(
            apply(&fixture.writer, &proposal).unwrap(),
            CommitReceipt {
                commit_seq: CommitSeq(1),
                disposition: CommitDisposition::AlreadyCommitted,
                submitted: None,
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
        history::validate(&fixture.writer).unwrap();
    }

    #[test]
    fn pruning_commit_log_retires_receipt_and_occ_evidence_together() {
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
        assert!(
            committed_receipt(&fixture.writer, &proposal)
                .unwrap()
                .is_none()
        );
        assert_eq!(
            fixture
                .writer
                .query_row("SELECT count(*) FROM notes", (), |row| row.get::<_, i64>(0))
                .unwrap(),
            1
        );
        history::validate(&fixture.writer).unwrap();
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
            .execute_batch(
                "CREATE TABLE failure_switch (enabled INTEGER NOT NULL);
                 INSERT INTO failure_switch VALUES (1);
                 CREATE TRIGGER fail_commit_receipt
                 BEFORE INSERT ON __multilite__commits
                 WHEN (SELECT enabled FROM failure_switch) = 1
                 BEGIN SELECT RAISE(ABORT, 'injected receipt failure'); END",
            )
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
}
