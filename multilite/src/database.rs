//! General Multilite database identity and Homebase lifecycle.

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
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use homebase_client::cipher::{SpaceEnvelope, SystemNonceSource};
use homebase_client::meta::{MetaStore, OplogCursors, OrderedMetaStore};
use homebase_client::server::UnreachableSpace;
use homebase_client::{Client, ClientError, PushOutcome as HomebasePushOutcome, ServerHandle};
use homebase_core::clock::{Lineage, SystemHybridClock};
use homebase_core::messages::KernelError;
use homebase_core::space::SpaceId;
use homebase_core::tag::{AdmissionSeq, DeviceId, DeviceSeq};
use pollster::block_on;
use rusqlite::hooks::{AuthAction, AuthContext, Authorization, PreUpdateCase};
use rusqlite::{Connection as SqliteConnection, Row};

use crate::commit::batch::{CommitQueue, QueuedCommit};
use crate::commit::committer::{CommitHistory, CommitPermit, Committer, HistoryPin};
use crate::commit::history;
use crate::commit::proposal::{
    self, CommitDisposition, CommitProposal, CommitReceipt, PrepareOutcome,
};
use crate::commit::snapshot::SnapshotDescriptor;
use crate::connection::ConnectionOwner;
use crate::metastore::{SqliteOrderedStore, SqliteSnapshotStore};
use crate::runtime::{ExecutionMode, HookPolicy, RuntimeConnection};
use crate::{Error, Params, Result};

use self::policy::{PolicyState, PushScheduler};
use self::row::{CapturedRow, StoredValue};
use self::store::DatabaseMetaStore;

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
    type Event = CapturedRow;

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
        capture_insert(mode, database, table, update)
    }
}

fn capture_insert(
    mode: ExecutionMode,
    database: &str,
    table: &str,
    update: &PreUpdateCase,
) -> Result<Option<CapturedRow>> {
    if mode != ExecutionMode::Public
        || database != "main"
        || is_schema_table(table)
        || has_multilite_prefix(table)
    {
        return Ok(None);
    }
    let PreUpdateCase::Insert(values) = update else {
        return Err(Error::CaptureInvariant(
            "public table mutation was not an insert",
        ));
    };
    if values.get_query_depth() != 0 {
        return Err(Error::CaptureInvariant(
            "writes caused by triggers are not supported",
        ));
    }
    let rowid = values.get_new_row_id();
    let values = (0..values.get_column_count())
        .map(|index| {
            values
                .get_new_column_value(index)
                .map(StoredValue::capture)
                .map_err(Error::from)
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Some(CapturedRow {
        table: table.to_owned(),
        rowid,
        values,
    }))
}

/// An opened general Multilite database.
pub(crate) struct Database<H: ServerHandle> {
    owner: ConnectionOwner,
    path: PathBuf,
    wal_path: PathBuf,
    database_id: DatabaseId,
    client: DatabaseClient<H>,
    policy: PolicyState,
    isolation_level: IsolationLevel,
    committer: Committer,
    commit_queue: CommitQueue,
    commit_history: CommitHistory,
    scheduler: PushScheduler,
}

enum GroupSlot {
    Prepared,
    DuplicatePrepared,
    Complete(Result<CommitReceipt>),
}

struct SeenGroupProposal {
    encoded: Vec<u8>,
    receipt: Option<CommitReceipt>,
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
        let database = Arc::clone(self);
        self.committer
            .call_blocking(move || database.push_serial())
            .map_err(committer_error)?
    }

    fn push_serial(&self) -> Result<PushOutcome> {
        let pushed = block_on(async {
            self.client
                .space(self.database_id.space_id())
                .await?
                .push()
                .await
        })?;
        match pushed {
            HomebasePushOutcome::Drained { .. } => Ok(PushOutcome::Drained),
            HomebasePushOutcome::Stalled { at, error, .. } => {
                let cursors = self.submit_cursors()?;
                Ok(PushOutcome::Rejected(PushRejection {
                    database_id: self.database_id,
                    device_id: self.client.device(),
                    failed_at: at,
                    submit_cursors: cursors,
                    error,
                }))
            }
        }
    }

    /// Undo the speculative SQLite effects covered by one definitive push
    /// rejection and retire that exact active submit window.
    pub(crate) fn rollback(self: &Arc<Self>, rejection: &PushRejection) -> Result<()> {
        let database = Arc::clone(self);
        let rejection = rejection.clone();
        self.committer
            .call_blocking(move || database.rollback_serial(&rejection))
            .map_err(committer_error)?
    }

    fn rollback_serial(&self, rejection: &PushRejection) -> Result<()> {
        if rejection.database_id != self.database_id
            || rejection.device_id != self.client.device()
            || rejection.failed_at != rejection.submit_cursors.neck
        {
            return Err(Error::StalePushRejection);
        }

        match block_on(self.client.rollback_if_unchanged(
            self.database_id.space_id(),
            rejection.failed_at,
            rejection.submit_cursors,
        )) {
            Ok(()) => self.prune_commit_history(),
            Err(ClientError::RollbackWindowChanged) => Err(Error::StalePushRejection),
            Err(error) => Err(error.into()),
        }
    }

    pub(crate) fn pull(self: &Arc<Self>) -> Result<PullOutcome> {
        let database = Arc::clone(self);
        self.committer
            .call_blocking(move || database.pull_serial())
            .map_err(committer_error)?
    }

    fn pull_serial(&self) -> Result<PullOutcome> {
        let through = block_on(async {
            let space = self.client.space(self.database_id.space_id()).await?;
            space.pull().await.map_err(ClientError::from)
        })?;
        self.policy.mark_pulled();
        Ok(PullOutcome { through })
    }

    pub(crate) fn prepare(
        self: &Arc<Self>,
        runtime: &Arc<DatabaseRuntime>,
        sql: &str,
    ) -> Result<Statement<H>> {
        sql::validate_managed_statement(sql)?;
        let _operation = self.enter_operation()?;
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
        let store =
            DatabaseMetaStore::with_history(self.owner.clone(), self.commit_history.clone());
        Ok(block_on(store.oplog_cursors(self.database_id.space_id()))?)
    }

    fn capture_branch_snapshot(&self, track_for_commit: bool) -> Result<BranchSnapshot> {
        self.capture_branch_snapshot_inner(track_for_commit, || Ok(()))
    }

    fn capture_branch_snapshot_inner(
        &self,
        track_for_commit: bool,
        after_physical: impl FnOnce() -> Result<()>,
    ) -> Result<BranchSnapshot> {
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
        Ok(BranchSnapshot {
            physical,
            logical: SnapshotDescriptor {
                commit_seq,
                authority_applied_through,
                submit_cursors: cursors.oplog,
            },
            history_pin: track_for_commit.then(|| self.commit_history.pin(commit_seq)),
        })
    }

    #[cfg(test)]
    fn capture_branch_snapshot_after_physical(
        &self,
        track_for_commit: bool,
        after_physical: impl FnOnce() -> Result<()>,
    ) -> Result<BranchSnapshot> {
        self.capture_branch_snapshot_inner(track_for_commit, after_physical)
    }

    fn issue_branch_snapshot(self: &Arc<Self>, track_for_commit: bool) -> Result<BranchSnapshot> {
        let database = Arc::clone(self);
        self.committer
            .call_blocking(move || database.capture_branch_snapshot(track_for_commit))
            .map_err(committer_error)?
    }

    fn commit_proposal(self: &Arc<Self>, proposal: CommitProposal) -> Result<CommitReceipt> {
        let ticket = self.commit_queue.enqueue(proposal)?;
        if ticket.should_schedule() {
            let database = Arc::clone(self);
            if let Err(error) = self
                .committer
                .dispatch_blocking(move || database.drain_commit_queue())
            {
                self.commit_queue.fail_all(error.to_string());
            }
        }
        ticket.wait()
    }

    fn drain_commit_queue(&self) {
        while let Some(group) = self.commit_queue.take_group() {
            let proposals = group.iter().map(QueuedCommit::proposal).collect::<Vec<_>>();
            let outcome = catch_unwind(AssertUnwindSafe(|| {
                self.commit_proposal_group_serial(&proposals)
            }));
            match outcome {
                Ok(Ok(results)) => {
                    debug_assert_eq!(group.len(), results.len());
                    for (queued, result) in group.into_iter().zip(results) {
                        queued.reply(result);
                    }
                }
                Ok(Err(error)) => {
                    let message = format!("commit group aborted: {error}");
                    for queued in group {
                        queued.reply(Err(Error::Committer(message.clone())));
                    }
                }
                Err(_) => {
                    let message = "commit group panicked".to_owned();
                    for queued in group {
                        queued.reply(Err(Error::Committer(message.clone())));
                    }
                    self.commit_queue.fail_all(message);
                    return;
                }
            }
        }
    }

    fn commit_proposal_group_serial(
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
                                None => GroupSlot::DuplicatePrepared,
                            });
                        }
                        continue;
                    }

                    let outcome =
                        self.owner
                            .with_savepoint("__multilite__commit_member", |connection| {
                                let outcome =
                                    proposal::prepare(connection, proposal, &accepted_writes)?;
                                if matches!(outcome, PrepareOutcome::Prepared(_)) {
                                    let (mutations, assertions) = proposal.to_homebase()?;
                                    let sequence = block_on(async {
                                        let space =
                                            self.client.space(self.database_id.space_id()).await?;
                                        let submission = space
                                            .submit_unchecked(mutations, assertions)
                                            .await
                                            .map_err(ClientError::from)?;
                                        Ok::<_, Error>(submission.seq)
                                    })?;
                                    pending::insert(connection, sequence, proposal.transaction())?;
                                }
                                Ok(outcome)
                            });

                    match outcome {
                        Ok(PrepareOutcome::Prepared(commit)) => {
                            accepted_writes.extend(commit.writes().iter().cloned());
                            seen.insert(
                                proposal.id(),
                                SeenGroupProposal {
                                    encoded,
                                    receipt: None,
                                },
                            );
                            slots.push(GroupSlot::Prepared);
                            prepared.push(commit);
                        }
                        Ok(PrepareOutcome::AlreadyCommitted(receipt)) => {
                            seen.insert(
                                proposal.id(),
                                SeenGroupProposal {
                                    encoded,
                                    receipt: Some(receipt),
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
                        GroupSlot::Prepared => Ok(CommitReceipt {
                            commit_seq: commit_seq.expect("prepared group has a commit sequence"),
                            disposition: CommitDisposition::Applied,
                        }),
                        GroupSlot::DuplicatePrepared => Ok(CommitReceipt {
                            commit_seq: commit_seq.expect("prepared group has a commit sequence"),
                            disposition: CommitDisposition::AlreadyCommitted,
                        }),
                        GroupSlot::Complete(result) => result,
                    })
                    .collect();

                self.commit_history.prune(connection)?;
                Ok(results)
            })
    }

    pub(super) fn prune_commit_history(&self) -> Result<()> {
        self.owner
            .with_savepoint("__multilite__commit_history_gc", |connection| {
                self.commit_history.prune(connection)?;
                Ok(())
            })
    }

    fn refresh_read_serial(&self, runtime: &DatabaseRuntime) -> Result<()> {
        if !self.policy.read_requires_refresh() {
            return Ok(());
        }
        let submit = self.submit_cursors()?;
        if submit.neck < submit.tail {
            match self.push_serial()? {
                PushOutcome::Drained => {}
                PushOutcome::Rejected(rejection) => {
                    return Err(Error::RefreshPushRejected(rejection));
                }
            }
        }
        self.pull_serial()?;
        self.rebase_serial(runtime)?;
        self.policy.mark_rebased();
        Ok(())
    }

    fn enter_operation(&self) -> Result<CommitPermit> {
        self.committer.enter_blocking().map_err(committer_error)
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
    if is_main(database) && !has_multilite_prefix(table) {
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
    let wal_path = wal_path_for(&path);
    let (database_id, client) =
        owner.with_savepoint("__multilite__database_open", |connection| {
            match classify(connection)? {
                DatabaseState::Fresh => initialize(
                    &owner,
                    invitation,
                    server,
                    lineage,
                    commit_history.clone(),
                    path.clone(),
                    wal_path.clone(),
                ),
                DatabaseState::Initialized => reopen(
                    &owner,
                    invitation.as_ref(),
                    server,
                    lineage,
                    commit_history.clone(),
                    path.clone(),
                    wal_path.clone(),
                ),
            }
        })?;
    Ok(Database {
        owner,
        wal_path,
        path,
        database_id,
        client,
        policy: PolicyState::new(sync_policy),
        isolation_level,
        committer: Committer::new().map_err(committer_error)?,
        commit_queue: CommitQueue::new(),
        commit_history,
        scheduler: PushScheduler::new(),
    })
}

struct BranchSnapshot {
    physical: crate::branch::snapshot::PinnedSnapshot,
    logical: SnapshotDescriptor,
    history_pin: Option<HistoryPin>,
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
    commit_history: CommitHistory,
    database_path: PathBuf,
    wal_path: PathBuf,
) -> Result<(DatabaseId, DatabaseClient<H>)> {
    let database_id = match invitation {
        Some(invitation) => invitation.database_id,
        None => DatabaseId::from_bytes(mint_id()?),
    };
    SqliteOrderedStore::initialize(owner)?;
    owner.with_connection(pending::initialize)?;
    owner.with_connection(catalog::initialize)?;
    owner.with_connection(proposal::initialize)?;
    owner.with_connection(history::initialize)?;
    let store =
        DatabaseMetaStore::with_database(owner.clone(), commit_history, database_path, wal_path);
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
    database_path: PathBuf,
    wal_path: PathBuf,
) -> Result<(DatabaseId, DatabaseClient<H>)> {
    let store = DatabaseMetaStore::with_database(
        owner.clone(),
        commit_history.clone(),
        database_path,
        wal_path,
    );
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
        proposal::validate(connection)?;
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
    let receipts = proposal::is_initialized(connection)?;
    let history = history::is_initialized(connection)?;
    match (metadata, pending, catalog, receipts, history) {
        (false, false, false, false, false) => Ok(DatabaseState::Fresh),
        (true, true, true, true, true) => {
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
