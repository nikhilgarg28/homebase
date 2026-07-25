//! General Multilite database identity and Homebase lifecycle.

mod async_api;
mod authority;
pub(crate) mod catalog;
mod codes;
mod connection;
pub(crate) mod isolation;
pub(crate) mod operation;
mod pending;
mod policy;
mod rebase;
pub(crate) mod row;
pub(crate) mod schema;
mod sql;
mod store;
pub(crate) mod transaction;
mod update;
mod view;
#[cfg(test)]
mod vtab;

use std::collections::{BTreeMap, BTreeSet};
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use homebase_client::cipher::{SpaceEnvelope, SystemNonceSource};
use homebase_client::meta::{MetaStore, OplogCursors, OrderedMetaStore};
use homebase_client::server::UnreachableSpace;
use homebase_client::{Client, ClientError, ServerHandle};
use homebase_core::clock::{Lineage, SystemHybridClock};
use homebase_core::messages::KernelError;
use homebase_core::space::SpaceId;
use homebase_core::tag::{AdmissionSeq, DeviceId, DeviceSeq};
use pollster::block_on;
use rusqlite::hooks::{AuthAction, AuthContext, Authorization, PreUpdateCase};
use rusqlite::{Connection as SqliteConnection, Row};

use crate::commit::committer::{
    CommitBackend, CommitHistory, CommitSnapshot, Committer, WeakCommitter,
};
use crate::commit::history;
use crate::commit::proposal::{
    self, CommitDisposition, CommitProposal, CommitReceipt, PrepareOutcome, ProposalBody,
};
use crate::commit::snapshot::SnapshotDescriptor;
use crate::connection::ConnectionOwner;
use crate::metastore::{SqliteOrderedStore, SqliteSnapshotStore};
use crate::runtime::{ExecutionMode, HookPolicy, RuntimeConnection};
use crate::{Error, Params, Result};

use self::authority::Authority;
use self::policy::{PolicyState, PushScheduler};
use self::row::{CapturedChange, CapturedRow, StoredValue};
use self::store::{CanonicalMetaSink, CanonicalRouter, DatabaseMetaStore};

pub use self::connection::Connection;
pub use self::isolation::{IsolationLevel, UpdateOptions};
pub use self::policy::SyncPolicy;
pub use self::update::UpdateTransaction;
pub use self::view::{TransactionStatement, ViewTransaction};

const REPLICA_INVITATION_VERSION: u8 = 1;

/// Result of fetching this database's available server admissions.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PullOutcome {
    through: AdmissionSeq,
}

impl PullOutcome {
    /// Last server admission sequence durably captured by this database.
    ///
    /// Capturing an admission does not imply that it has been rebased or
    /// applied to SQLite.
    pub fn captured_through(&self) -> u64 {
        self.through.0
    }
}

/// Result of pushing this database's active local submission window.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PushOutcome {
    /// Every currently active submission was admitted.
    Drained,
    /// Admission stopped at a kernel rejection.
    Rejected(PushRejection),
}

/// Opaque record of a rejection against one observed local submission window.
///
/// A later rollback will validate this identity and window before changing
/// local state. Merely receiving the handle never performs repair.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PushRejection {
    database_id: DatabaseId,
    device_id: DeviceId,
    failed_at: DeviceSeq,
    submit_cursors: OplogCursors,
    error: KernelError,
}

impl PushRejection {
    /// Homebase sequence of the first rejected local submission.
    pub fn failed_sequence(&self) -> u64 {
        self.failed_at.0
    }

    /// Kernel invariant that rejected the submission.
    pub fn error(&self) -> &KernelError {
        &self.error
    }
}

/// Public identity shared by every replica of a Multilite database.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct DatabaseId {
    space_id: [u8; 16],
}

impl DatabaseId {
    /// Reconstruct an id from its complete plaintext representation.
    pub const fn from_bytes(space_id: [u8; 16]) -> Self {
        Self { space_id }
    }

    /// Return the complete plaintext representation.
    pub const fn to_bytes(self) -> [u8; 16] {
        self.space_id
    }

    fn space_id(self) -> SpaceId {
        SpaceId(self.space_id)
    }
}

/// Opaque, versioned material used to initialize another local replica.
///
/// The current format carries only the public database identity. A future
/// encrypted format can carry or unlock the space envelope without changing
/// the open API.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReplicaInvitation {
    database_id: DatabaseId,
}

impl ReplicaInvitation {
    /// Public identity named by this invitation.
    pub const fn database_id(&self) -> DatabaseId {
        self.database_id
    }

    /// Encode the invitation for transport to another replica.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(17);
        bytes.push(REPLICA_INVITATION_VERSION);
        bytes.extend_from_slice(&self.database_id.to_bytes());
        bytes
    }

    /// Decode one complete invitation, rejecting unknown or malformed forms.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        let [version, id @ ..] = bytes else {
            return Err(Error::InvalidReplicaInvitation);
        };
        if *version != REPLICA_INVITATION_VERSION || id.len() != 16 {
            return Err(Error::InvalidReplicaInvitation);
        }
        let space_id = id.try_into().map_err(|_| Error::InvalidReplicaInvitation)?;
        Ok(Self {
            database_id: DatabaseId::from_bytes(space_id),
        })
    }

    fn new(database_id: DatabaseId) -> Self {
        Self { database_id }
    }
}

/// Default endpoint type for a database opened without a server handle.
pub type OfflineServer = fn(&SpaceId) -> Option<UnreachableSpace>;

/// Optional identity and server configuration for opening a database.
pub struct OpenOptions<H = OfflineServer>
where
    H: ServerHandle,
{
    invitation: Option<ReplicaInvitation>,
    server: H,
    authority: bool,
    sync_policy: SyncPolicy,
    isolation_level: IsolationLevel,
}

impl Default for OpenOptions<OfflineServer> {
    fn default() -> Self {
        Self::new()
    }
}

impl OpenOptions<OfflineServer> {
    /// Default to a locally initialized database and an offline server route.
    pub fn new() -> Self {
        Self {
            invitation: None,
            server: offline_server,
            authority: false,
            sync_policy: SyncPolicy::default(),
            isolation_level: IsolationLevel::default(),
        }
    }
}

impl<H: ServerHandle> OpenOptions<H> {
    /// Initialize from, or verify against, a replica invitation.
    pub fn invitation(mut self, invitation: ReplicaInvitation) -> Self {
        self.invitation = Some(invitation);
        self
    }

    /// Select how local reads and writes interact with authority.
    pub fn sync_policy(mut self, policy: SyncPolicy) -> Self {
        self.sync_policy = policy;
        self
    }

    /// Select the default isolation level for managed updates.
    pub fn isolation_level(mut self, isolation_level: IsolationLevel) -> Self {
        self.isolation_level = isolation_level;
        self
    }

    /// Replace the server route while retaining all other options.
    pub fn server<S: ServerHandle>(self, server: S) -> OpenOptions<S> {
        OpenOptions {
            invitation: self.invitation,
            server,
            authority: true,
            sync_policy: self.sync_policy,
            isolation_level: self.isolation_level,
        }
    }

    fn validate(&self) -> Result<()> {
        if !self.authority {
            match self.sync_policy {
                SyncPolicy::LocalOnly => {}
                SyncPolicy::LocalFirst { .. } => {
                    return Err(Error::AuthorityRequired("local-first policy"));
                }
                SyncPolicy::Remote => return Err(Error::AuthorityRequired("remote policy")),
            }
        }
        Ok(())
    }
}

pub(crate) type DatabaseClient<H> =
    Client<DatabaseMetaStore, H, SystemHybridClock, SystemNonceSource>;

pub(crate) struct DatabaseRuntime {
    inner: RuntimeConnection<DatabaseHooks>,
}

impl DatabaseRuntime {
    fn new(owner: ConnectionOwner) -> Result<Self> {
        Ok(Self {
            inner: RuntimeConnection::new(owner, DatabaseHooks)?,
        })
    }
}

impl Deref for DatabaseRuntime {
    type Target = RuntimeConnection<DatabaseHooks>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

pub(crate) struct DatabaseHooks;

impl HookPolicy for DatabaseHooks {
    type Event = CapturedChange;

    fn authorize(&mut self, mode: ExecutionMode, context: AuthContext<'_>) -> Authorization {
        authorize_database(mode, &context)
    }

    fn preupdate(
        &mut self,
        mode: ExecutionMode,
        database: &str,
        table: &str,
        update: &PreUpdateCase,
    ) -> Result<Option<Self::Event>> {
        capture_change(mode, database, table, update)
    }
}

fn capture_change(
    mode: ExecutionMode,
    database: &str,
    table: &str,
    update: &PreUpdateCase,
) -> Result<Option<CapturedChange>> {
    if mode != ExecutionMode::Public
        || database != "main"
        || is_schema_table(table)
        || has_multilite_prefix(table)
    {
        return Ok(None);
    }
    let (depth, rowid, column_count) = match update {
        PreUpdateCase::Insert(values) => (
            values.get_query_depth(),
            values.get_new_row_id(),
            values.get_column_count(),
        ),
        PreUpdateCase::Delete(values) => (
            values.get_query_depth(),
            values.get_old_row_id(),
            values.get_column_count(),
        ),
        PreUpdateCase::Update { .. } | PreUpdateCase::Unknown => {
            return Err(Error::CaptureInvariant(
                "public table mutation was not an insert or delete",
            ));
        }
    };
    if depth != 0 {
        return Err(Error::CaptureInvariant(
            "writes caused by triggers are not supported",
        ));
    }
    let values = (0..column_count)
        .map(|index| {
            match update {
                PreUpdateCase::Insert(values) => values.get_new_column_value(index),
                PreUpdateCase::Delete(values) => values.get_old_column_value(index),
                PreUpdateCase::Update { .. } | PreUpdateCase::Unknown => unreachable!(),
            }
            .map(StoredValue::capture)
            .map_err(Error::from)
        })
        .collect::<Result<Vec<_>>>()?;
    let row = CapturedRow {
        table: table.to_owned(),
        rowid,
        values,
    };
    Ok(Some(match update {
        PreUpdateCase::Insert(_) => CapturedChange::Insert(row),
        PreUpdateCase::Delete(_) => CapturedChange::Delete(row),
        PreUpdateCase::Update { .. } | PreUpdateCase::Unknown => unreachable!(),
    }))
}

/// An opened general Multilite database.
pub(crate) struct Database<H: ServerHandle> {
    owner: ConnectionOwner,
    database_id: DatabaseId,
    client: Arc<DatabaseClient<H>>,
    policy: PolicyState,
    isolation_level: IsolationLevel,
    committer: Committer,
    authority: Authority,
    scheduler: PushScheduler,
}

enum GroupSlot {
    Prepared { submitted: Option<DeviceSeq> },
    DuplicatePrepared { submitted: Option<DeviceSeq> },
    Complete(Result<CommitReceipt>),
}

struct SeenGroupProposal {
    encoded: Vec<u8>,
    receipt: Option<CommitReceipt>,
    submitted: Option<DeviceSeq>,
}

impl Database<OfflineServer> {
    pub(crate) fn open(path: impl AsRef<Path>) -> Result<Arc<Self>> {
        Self::open_with(path, OpenOptions::new())
    }
}

impl<H: ServerHandle + Send + Sync + 'static> Database<H> {
    pub(crate) fn open_with(path: impl AsRef<Path>, options: OpenOptions<H>) -> Result<Arc<Self>> {
        options.validate()?;
        let path = path.as_ref().to_owned();
        let owner = ConnectionOwner::open(&path)?;
        owner.with_connection(|connection| {
            connection.pragma_update(None, "journal_mode", "WAL")?;
            connection.pragma_update(None, "wal_autocheckpoint", 0)?;
            Ok::<_, rusqlite::Error>(())
        })?;
        let database = open_on(owner, path, options)?;
        Ok(Arc::new(database))
    }

    pub(crate) fn start_background_push(self: &Arc<Self>) -> Result<()> {
        if self.policy.write_delay().is_some() {
            self.scheduler.start(Arc::downgrade(self))?;
            let cursors = self.submit_cursors()?;
            if cursors.neck < cursors.tail {
                self.scheduler.schedule(std::time::Duration::ZERO);
            }
        }
        Ok(())
    }

    pub(crate) fn sync_policy(&self) -> SyncPolicy {
        self.policy.policy()
    }

    pub(crate) fn isolation_level(&self) -> IsolationLevel {
        self.isolation_level
    }

    pub(crate) fn database_id(&self) -> DatabaseId {
        self.database_id
    }

    pub(crate) fn replica_invitation(&self) -> ReplicaInvitation {
        ReplicaInvitation::new(self.database_id)
    }

    pub(crate) fn device_id(&self) -> [u8; 16] {
        self.client.device().0
    }

    pub(crate) fn runtime(&self) -> Result<DatabaseRuntime> {
        DatabaseRuntime::new(self.owner.clone())
    }

    pub(crate) fn execute<Q: Params>(
        self: &Arc<Self>,
        runtime: &DatabaseRuntime,
        sql: &str,
        params: Q,
    ) -> Result<usize> {
        let validated = sql::validate_execute(sql)?;
        self.update(runtime, |update| {
            update.execute_validated(sql, params, validated)
        })
    }

    pub(crate) fn push(self: &Arc<Self>) -> Result<PushOutcome> {
        block_on(self.push_async())
    }

    /// Undo the speculative SQLite effects covered by one definitive push
    /// rejection and retire that exact active submit window.
    pub(crate) fn rollback(self: &Arc<Self>, rejection: &PushRejection) -> Result<()> {
        block_on(self.rollback_async(rejection.clone()))
    }

    pub(crate) fn pull(self: &Arc<Self>) -> Result<PullOutcome> {
        block_on(self.pull_async())
    }

    pub(crate) fn prepare(
        self: &Arc<Self>,
        runtime: &Arc<DatabaseRuntime>,
        sql: &str,
    ) -> Result<Statement<H>> {
        sql::validate_managed_statement(sql)?;
        runtime.run(ExecutionMode::Public, |connection| {
            let statement = connection.prepare(sql)?;
            if statement.readonly() {
                Ok(())
            } else {
                Err(Error::PreparedWrite)
            }
        })?;
        Ok(Statement {
            database: Arc::clone(self),
            runtime: Arc::clone(runtime),
            sql: sql.to_owned(),
        })
    }

    #[cfg(test)]
    pub(crate) fn with_connection<T>(&self, operation: impl FnOnce(&SqliteConnection) -> T) -> T {
        self.owner.with_connection(operation)
    }

    fn submit_cursors(&self) -> Result<OplogCursors> {
        let store = DatabaseMetaStore::read_only(self.owner.clone());
        Ok(block_on(store.oplog_cursors(self.database_id.space_id()))?)
    }

    fn issue_branch_snapshot(self: &Arc<Self>, track_for_commit: bool) -> Result<CommitSnapshot> {
        self.committer.capture_snapshot_blocking(track_for_commit)
    }

    fn commit_proposal(self: &Arc<Self>, proposal: CommitProposal) -> Result<CommitReceipt> {
        self.committer.propose_blocking(proposal)
    }

    fn refresh_read(self: &Arc<Self>, runtime: &DatabaseRuntime) -> Result<()> {
        let _ = runtime;
        block_on(self.refresh_read_async())
    }
}

impl<H: ServerHandle + Send + Sync + 'static> DatabaseCommitBackend<H> {
    fn capture_snapshot_inner(
        &self,
        track_for_commit: bool,
        after_physical: impl FnOnce() -> Result<()>,
    ) -> Result<CommitSnapshot> {
        let physical = crate::branch::snapshot::PinnedSnapshot::capture(&self.path, &self.wal_path)
            .map_err(|error| Error::Branch(error.to_string()))?;
        after_physical()?;
        let (commit_seq, cursors) = physical.with_reader(|connection| {
            let commit_seq = self.commit_history.current(connection)?;
            let metadata = OrderedMetaStore::new(SqliteSnapshotStore::new(connection));
            let cursors = block_on(metadata.cursor_snapshot(self.database_id.space_id()))?;
            Ok::<_, Error>((commit_seq, cursors))
        })?;
        let authority_applied_through = AdmissionSeq(
            cursors
                .admits
                .neck
                .0
                .checked_sub(1)
                .ok_or(Error::InvalidDatabase("admit neck cannot be zero"))?,
        );
        Ok(CommitSnapshot {
            physical,
            logical: SnapshotDescriptor {
                commit_seq,
                authority_applied_through,
                submit_cursors: cursors.oplog,
            },
            history_pin: track_for_commit.then(|| self.commit_history.pin(commit_seq)),
        })
    }

    fn commit_proposal_group(
        &self,
        proposals: &[&CommitProposal],
    ) -> Result<Vec<Result<CommitReceipt>>> {
        self.owner
            .with_savepoint("__multilite__commit_group", |connection| {
                let mut accepted_writes = BTreeSet::new();
                let mut prepared = Vec::new();
                let mut slots = Vec::with_capacity(proposals.len());
                let mut seen: BTreeMap<_, SeenGroupProposal> = BTreeMap::new();

                for proposal in proposals {
                    let encoded = match proposal.encode() {
                        Ok(encoded) => encoded,
                        Err(error) => {
                            slots.push(GroupSlot::Complete(Err(error)));
                            continue;
                        }
                    };
                    if let Some(previous) = seen.get(&proposal.id()) {
                        if previous.encoded != encoded {
                            slots.push(GroupSlot::Complete(Err(Error::InvalidCommitProposal(
                                "one proposal id names different commit payloads".into(),
                            ))));
                        } else {
                            slots.push(match previous.receipt {
                                Some(receipt) => GroupSlot::Complete(Ok(receipt)),
                                None => GroupSlot::DuplicatePrepared {
                                    submitted: previous.submitted,
                                },
                            });
                        }
                        continue;
                    }

                    let outcome =
                        self.owner
                            .with_savepoint("__multilite__commit_member", |connection| {
                                self.prepare_commit_proposal(connection, proposal, &accepted_writes)
                            });

                    match outcome {
                        Ok(PrepareOutcome::Prepared(commit)) => {
                            accepted_writes.extend(commit.writes().iter().cloned());
                            let submitted = commit.submitted();
                            seen.insert(
                                proposal.id(),
                                SeenGroupProposal {
                                    encoded,
                                    receipt: None,
                                    submitted,
                                },
                            );
                            slots.push(GroupSlot::Prepared { submitted });
                            prepared.push(commit);
                        }
                        Ok(PrepareOutcome::AlreadyCommitted(receipt)) => {
                            seen.insert(
                                proposal.id(),
                                SeenGroupProposal {
                                    encoded,
                                    receipt: Some(receipt),
                                    submitted: receipt.submitted,
                                },
                            );
                            slots.push(GroupSlot::Complete(Ok(receipt)));
                        }
                        Err(error) => slots.push(GroupSlot::Complete(Err(error))),
                    }
                }

                let commit_seq = if prepared.is_empty() {
                    None
                } else {
                    Some(proposal::finalize_group(
                        connection,
                        &self.commit_history,
                        &prepared,
                    )?)
                };
                let results = slots
                    .into_iter()
                    .map(|slot| match slot {
                        GroupSlot::Prepared { submitted } => Ok(CommitReceipt {
                            commit_seq: commit_seq.expect("prepared group has a commit sequence"),
                            disposition: CommitDisposition::Applied,
                            submitted,
                        }),
                        GroupSlot::DuplicatePrepared { submitted } => Ok(CommitReceipt {
                            commit_seq: commit_seq.expect("prepared group has a commit sequence"),
                            disposition: CommitDisposition::AlreadyCommitted,
                            submitted,
                        }),
                        GroupSlot::Complete(result) => result,
                    })
                    .collect();

                self.commit_history.prune(connection)?;
                Ok(results)
            })
    }

    fn prepare_commit_proposal(
        &self,
        connection: &SqliteConnection,
        proposal: &CommitProposal,
        accepted_writes: &BTreeSet<history::WriteRegion>,
    ) -> Result<PrepareOutcome> {
        match proposal.body() {
            ProposalBody::Transaction(transaction) => {
                let outcome = proposal::prepare(connection, proposal, accepted_writes)?;
                let PrepareOutcome::Prepared(commit) = outcome else {
                    return Ok(outcome);
                };
                let (mutations, assertions) = proposal.to_homebase()?;
                let sequence = block_on(async {
                    let space = self.client.space(self.database_id.space_id()).await?;
                    let prepared = space
                        .prepare_unchecked(mutations, assertions)
                        .await
                        .map_err(ClientError::from)?;
                    space
                        .commit_prepared(prepared)
                        .await
                        .map_err(ClientError::from)
                        .map_err(Error::from)
                })?;
                pending::insert(connection, sequence, transaction.transaction())?;
                Ok(PrepareOutcome::Prepared(commit.with_submission(sequence)))
            }
            ProposalBody::ApplyAdmissions(apply) => {
                if let Some(receipt) = proposal.committed_receipt(connection)? {
                    return Ok(PrepareOutcome::AlreadyCommitted(receipt));
                }
                proposal.validate()?;
                let store = OrderedMetaStore::new(SqliteOrderedStore::new(self.owner.clone()));
                let space = self.database_id.space_id();
                let current_submit = block_on(store.oplog_cursors(space))?;
                let current_admits = block_on(store.admit_cursors(space))?;
                if current_submit != apply.expected_submit()
                    || current_admits != apply.expected_admits()
                {
                    return Err(Error::RebaseStateChanged);
                }
                if current_submit.neck != current_submit.tail {
                    return Err(Error::RebasePendingSubmissions);
                }

                let mut writes = BTreeSet::new();
                for admitted in apply.transactions() {
                    if admitted.device == apply.local_device() {
                        continue;
                    }
                    admitted.transaction.apply(connection)?;
                    let lowered = admitted.transaction.to_homebase()?;
                    writes.extend(history::writes_from_mutations(&lowered.mutations));
                }
                block_on(store.mark_admits_applied(space, apply.through()))?;
                Ok(PrepareOutcome::Prepared(
                    proposal.prepare_receipt(writes.into_iter().collect())?,
                ))
            }
            ProposalBody::RejectSubmissions(reject) => {
                if let Some(receipt) = proposal.committed_receipt(connection)? {
                    return Ok(PrepareOutcome::AlreadyCommitted(receipt));
                }
                proposal.validate()?;
                let store = OrderedMetaStore::new(SqliteOrderedStore::new(self.owner.clone()));
                let space = self.database_id.space_id();
                let current = block_on(store.oplog_cursors(space))?;
                if current != reject.expected_submit() {
                    return Err(Error::StalePushRejection);
                }
                let through = DeviceSeq(
                    current
                        .tail
                        .0
                        .checked_sub(1)
                        .ok_or(Error::InvalidDatabase("submit tail cannot be zero"))?,
                );
                let active = block_on(store.oplog(space, current.neck, through))?;
                let repair = pending::prepare_rejection(connection, &active)?
                    .ok_or(Error::StalePushRejection)?;
                let writes = repair.writes().to_vec();
                repair.apply(connection)?;
                block_on(store.rollback_if_unchanged(
                    space,
                    reject.failed_at(),
                    reject.expected_submit(),
                ))?;
                Ok(PrepareOutcome::Prepared(proposal.prepare_receipt(writes)?))
            }
            ProposalBody::AcceptSubmissions(accept) => {
                if let Some(receipt) = proposal.committed_receipt(connection)? {
                    return Ok(PrepareOutcome::AlreadyCommitted(receipt));
                }
                proposal.validate()?;
                let store = OrderedMetaStore::new(SqliteOrderedStore::new(self.owner.clone()));
                let space = self.database_id.space_id();
                let current = block_on(store.oplog_cursors(space))?;
                let expected = accept.expected_submit();
                if current.head != expected.head
                    || current.neck != expected.neck
                    || current.tail < expected.tail
                {
                    return Err(Error::CommitConflict(
                        "submit window changed before acknowledgement".into(),
                    ));
                }
                block_on(store.trim_oplog(space, accept.through(), accept.checksum()))?;
                pending::accept_through(connection, accept.through())?;
                Ok(PrepareOutcome::Prepared(
                    proposal.prepare_receipt(Vec::new())?,
                ))
            }
            ProposalBody::AppendAdmissions(append) => {
                if let Some(receipt) = proposal.committed_receipt(connection)? {
                    return Ok(PrepareOutcome::AlreadyCommitted(receipt));
                }
                proposal.validate()?;
                let store = OrderedMetaStore::new(SqliteOrderedStore::new(self.owner.clone()));
                let space = self.database_id.space_id();
                if block_on(store.admit_cursors(space))? != append.expected_admits() {
                    return Err(Error::CommitConflict(
                        "admit window changed before pull capture".into(),
                    ));
                }
                block_on(store.append_admits(space, append.response()))?;
                Ok(PrepareOutcome::Prepared(
                    proposal.prepare_receipt(Vec::new())?,
                ))
            }
        }
    }
}

impl<H: ServerHandle + Send + Sync + 'static> CommitBackend for DatabaseCommitBackend<H> {
    fn commit_group(&self, proposals: &[&CommitProposal]) -> Result<Vec<Result<CommitReceipt>>> {
        self.commit_proposal_group(proposals)
    }

    fn capture_snapshot(&self, writable: bool) -> Result<CommitSnapshot> {
        self.capture_snapshot_inner(writable, || Ok(()))
    }
}

fn authorize_database(mode: ExecutionMode, context: &AuthContext<'_>) -> Authorization {
    if mode != ExecutionMode::Public {
        return Authorization::Allow;
    }

    match context.action {
        AuthAction::Select | AuthAction::Function { .. } | AuthAction::Recursive => {
            Authorization::Allow
        }
        AuthAction::Read { table_name, .. } => authorize_read(context.database_name, table_name),
        AuthAction::CreateTable { table_name } => {
            authorize_user_table(context.database_name, table_name)
        }
        AuthAction::CreateIndex {
            index_name,
            table_name,
        } if index_name.starts_with("sqlite_autoindex_") => {
            authorize_user_table(context.database_name, table_name)
        }
        AuthAction::Insert { table_name } if is_schema_table(table_name) => {
            authorize_main(context.database_name)
        }
        AuthAction::Update { table_name, .. } if is_schema_table(table_name) => {
            authorize_main(context.database_name)
        }
        AuthAction::Insert { table_name } => {
            authorize_user_table(context.database_name, table_name)
        }
        AuthAction::Delete { table_name } => {
            authorize_user_table(context.database_name, table_name)
        }
        _ => Authorization::Deny,
    }
}

fn authorize_read(database: Option<&str>, table: &str) -> Authorization {
    if is_schema_table(table) {
        authorize_main(database)
    } else {
        authorize_user_table(database, table)
    }
}

fn authorize_user_table(database: Option<&str>, table: &str) -> Authorization {
    if is_main(database) && !has_multilite_prefix(table) && !is_sqlite_internal_table(table) {
        Authorization::Allow
    } else {
        Authorization::Deny
    }
}

fn authorize_main(database: Option<&str>) -> Authorization {
    if is_main(database) {
        Authorization::Allow
    } else {
        Authorization::Deny
    }
}

fn is_main(database: Option<&str>) -> bool {
    matches!(database, None | Some("main"))
}

fn is_schema_table(table: &str) -> bool {
    table.eq_ignore_ascii_case("sqlite_master") || table.eq_ignore_ascii_case("sqlite_schema")
}

fn is_sqlite_internal_table(table: &str) -> bool {
    table
        .get(.."sqlite_".len())
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("sqlite_"))
}

fn has_multilite_prefix(table: &str) -> bool {
    table
        .get(.."__multilite__".len())
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("__multilite__"))
}

/// A read-only prepared statement owned by a Multilite database.
pub struct Statement<H = OfflineServer>
where
    H: ServerHandle + Send + Sync + 'static,
{
    database: Arc<Database<H>>,
    runtime: Arc<DatabaseRuntime>,
    sql: String,
}

impl<H: ServerHandle + Send + Sync + 'static> Statement<H> {
    /// Execute the query and eagerly map every row.
    pub fn query_map<T, P, F>(&mut self, params: P, map: F) -> Result<Vec<T>>
    where
        P: Params,
        F: FnMut(&Row<'_>) -> rusqlite::Result<T>,
    {
        self.database
            .view(&self.runtime, |view| view.query(&self.sql, params, map))
    }

    /// Asynchronously execute this statement and return owned mapped values.
    pub async fn query_map_async<T, P, F>(&self, params: P, map: F) -> Result<Vec<T>>
    where
        T: Send + 'static,
        P: Params + Send + 'static,
        F: Send + 'static + for<'a> FnMut(&Row<'a>) -> rusqlite::Result<T>,
    {
        let sql = self.sql.clone();
        self.database
            .view_async(move |view| view.query(&sql, params, map))
            .await
    }

    /// Async alias matching the connection's direct-query vocabulary.
    pub async fn query_async<T, P, F>(&self, params: P, map: F) -> Result<Vec<T>>
    where
        T: Send + 'static,
        P: Params + Send + 'static,
        F: Send + 'static + for<'a> FnMut(&Row<'a>) -> rusqlite::Result<T>,
    {
        self.query_map_async(params, map).await
    }
}

fn open_on<H: ServerHandle + Send + Sync + 'static>(
    owner: ConnectionOwner,
    path: PathBuf,
    options: OpenOptions<H>,
) -> Result<Database<H>> {
    let OpenOptions {
        invitation,
        server,
        authority: _,
        sync_policy,
        isolation_level,
    } = options;
    let lineage = Lineage(mint_id()?);
    let commit_history = CommitHistory::default();
    let canonical = CanonicalRouter::default();
    let wal_path = wal_path_for(&path);
    let (database_id, client) =
        owner.with_savepoint("__multilite__database_open", |connection| {
            match classify(connection)? {
                DatabaseState::Fresh => {
                    initialize(&owner, invitation, server, lineage, canonical.clone())
                }
                DatabaseState::Initialized => reopen(
                    &owner,
                    invitation.as_ref(),
                    server,
                    lineage,
                    commit_history.clone(),
                    canonical.clone(),
                ),
            }
        })?;
    let client = Arc::new(client);
    let commit_backend = Arc::new(DatabaseCommitBackend {
        owner: owner.clone(),
        path,
        wal_path,
        database_id,
        client: Arc::clone(&client),
        commit_history: commit_history.clone(),
    });
    let committer = Committer::new(Arc::clone(&commit_backend)).map_err(committer_error)?;
    canonical.install(Arc::new(CommitterMetaSink {
        committer: committer.downgrade(),
    }))?;
    let authority = Authority::new(Arc::clone(&client), database_id.space_id())
        .map_err(|error| Error::BackgroundWorker(error.to_string()))?;
    Ok(Database {
        owner,
        database_id,
        client,
        policy: PolicyState::new(sync_policy),
        isolation_level,
        committer,
        authority,
        scheduler: PushScheduler::new(),
    })
}

struct DatabaseCommitBackend<H: ServerHandle> {
    owner: ConnectionOwner,
    path: PathBuf,
    wal_path: PathBuf,
    database_id: DatabaseId,
    client: Arc<DatabaseClient<H>>,
    commit_history: CommitHistory,
}

struct CommitterMetaSink {
    committer: WeakCommitter,
}

impl CanonicalMetaSink for CommitterMetaSink {
    fn propose(&self, proposal: CommitProposal) -> Result<()> {
        self.committer.propose_blocking(proposal)?;
        Ok(())
    }
}

fn wal_path_for(path: &Path) -> PathBuf {
    let mut wal = path.as_os_str().to_owned();
    wal.push("-wal");
    wal.into()
}

fn initialize<H: ServerHandle>(
    owner: &ConnectionOwner,
    invitation: Option<ReplicaInvitation>,
    server: H,
    lineage: Lineage,
    canonical: CanonicalRouter,
) -> Result<(DatabaseId, DatabaseClient<H>)> {
    let database_id = match invitation {
        Some(invitation) => invitation.database_id,
        None => DatabaseId::from_bytes(mint_id()?),
    };
    SqliteOrderedStore::initialize(owner)?;
    owner.with_connection(pending::initialize)?;
    owner.with_connection(catalog::initialize)?;
    owner.with_connection(history::initialize)?;
    let store = DatabaseMetaStore::with_database(owner.clone(), canonical);
    let client = block_on(Client::open(
        store,
        server,
        SystemHybridClock::new(lineage),
        DeviceId(mint_id()?),
        SystemNonceSource,
    ))?;
    block_on(client.attach(&SpaceEnvelope::plaintext(database_id.space_id())))?;
    Ok((database_id, client))
}

fn reopen<H: ServerHandle>(
    owner: &ConnectionOwner,
    invitation: Option<&ReplicaInvitation>,
    server: H,
    lineage: Lineage,
    commit_history: CommitHistory,
    canonical: CanonicalRouter,
) -> Result<(DatabaseId, DatabaseClient<H>)> {
    let store = DatabaseMetaStore::with_database(owner.clone(), canonical);
    let state = block_on(store.load())?;
    if state.device.is_none() {
        return Err(Error::InvalidDatabase("device identity is missing"));
    }
    if state.spaces.len() != 1 {
        return Err(Error::InvalidDatabase(
            "file must contain exactly one Homebase space",
        ));
    }
    let (space_id, space) = state
        .spaces
        .first_key_value()
        .expect("length checked above");
    let codec = space
        .codec
        .as_ref()
        .ok_or(Error::InvalidDatabase("space envelope is missing"))?;
    let envelope =
        SpaceEnvelope::decode(&codec.sealed).map_err(homebase_client::ClientError::from)?;
    if envelope != SpaceEnvelope::plaintext(*space_id) {
        return Err(Error::InvalidDatabase(
            "database requires a plaintext envelope matching its stored space",
        ));
    }
    let database_id = DatabaseId::from_bytes(space_id.0);
    if let Some(invitation) = invitation
        && invitation.database_id != database_id
    {
        return Err(Error::DatabaseIdMismatch {
            expected: invitation.database_id.to_bytes(),
            actual: database_id.to_bytes(),
        });
    }

    owner.with_connection(|connection| {
        catalog::validate(connection)?;
        history::validate(connection)?;
        pending::validate_active_from(connection, space.cursors.neck)?;
        commit_history.prune(connection)?;
        Ok::<_, Error>(())
    })?;

    let client = block_on(Client::open(
        store,
        server,
        SystemHybridClock::new(lineage),
        DeviceId(mint_id()?),
        SystemNonceSource,
    ))?;
    block_on(client.attach(&envelope))?;
    Ok((database_id, client))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DatabaseState {
    Fresh,
    Initialized,
}

fn classify(connection: &SqliteConnection) -> Result<DatabaseState> {
    let metadata = SqliteOrderedStore::is_initialized(connection)?;
    let pending = pending::is_initialized(connection)?;
    let catalog = catalog::is_initialized(connection)?;
    let history = history::is_initialized(connection)?;
    match (metadata, pending, catalog, history) {
        (false, false, false, false) => Ok(DatabaseState::Fresh),
        (true, true, true, true) => {
            SqliteOrderedStore::validate(connection)?;
            pending::validate(connection)?;
            catalog::validate(connection)?;
            Ok(DatabaseState::Initialized)
        }
        _ => Err(Error::InvalidDatabase(
            "general metadata tables are only partially initialized",
        )),
    }
}

fn mint_id() -> Result<[u8; 16]> {
    let mut id = [0; 16];
    getrandom::fill(&mut id).map_err(|error| Error::Entropy(error.to_string()))?;
    Ok(id)
}

fn offline_server(_: &SpaceId) -> Option<UnreachableSpace> {
    None
}

fn committer_error(error: crate::commit::committer::CommitterError) -> Error {
    Error::Committer(error.to_string())
}

#[cfg(test)]
mod tests;
