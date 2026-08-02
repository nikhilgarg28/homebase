//! Async-first orchestration over owned committer, authority, and branch work.

use std::sync::Arc;
use std::time::Instant;

use homebase_client::meta::{MetaStore, OplogCursors};
use homebase_client::{ClientError, PushOutcome as HomebasePushOutcome, ServerHandle};
use homebase_core::tag::DeviceSeq;
use rusqlite::Row;

use super::policy::RefreshTransition;
use super::store::DatabaseMetaStore;
use super::update::{BranchUpdate, run_branch_update};
use super::view::{ViewTransaction, run_branch_view};
use super::{
    Database, DatabaseRuntime, PullOutcome, PushOutcome, PushRejection, Statement, SyncPolicy,
    UpdateOptions, UpdateTransaction, sql,
};
use crate::commit::proposal::{AdmittedTransaction, CommitProposal, CommitReceipt};
use crate::logical::transaction::MultiliteTransaction;
use crate::{Error, Params, Result, blocking};

impl<H: ServerHandle + Send + Sync + 'static> Database<H> {
    pub(crate) async fn push_async(self: &Arc<Self>) -> Result<PushOutcome> {
        let workflow = self.policy.enter_workflow().await?;
        let result = self.push_via_authority_async(None).await;
        workflow.complete(RefreshTransition::Unchanged);
        result
    }

    async fn push_via_authority_async(
        self: &Arc<Self>,
        through: Option<DeviceSeq>,
    ) -> Result<PushOutcome> {
        let pushed = match through {
            Some(sequence) => self.authority.push_until(sequence).await?,
            None => self.authority.push().await?,
        };
        match pushed {
            HomebasePushOutcome::Drained { .. } => Ok(PushOutcome::Drained),
            HomebasePushOutcome::Stalled { at, error, .. } => {
                let cursors = self.submit_cursors_async().await?;
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

    /// Run one delayed LocalFirst push while the policy actor owns the workflow.
    pub(super) fn run_scheduled_push(self: &Arc<Self>) -> Result<()> {
        pollster::block_on(async {
            match self.push_via_authority_async(None).await? {
                PushOutcome::Drained => Ok(()),
                PushOutcome::Rejected(rejection) => {
                    self.rollback_locked_async(rejection).await?;
                    match self.push_via_authority_async(None).await? {
                        PushOutcome::Drained => Ok(()),
                        PushOutcome::Rejected(rejection) => {
                            Err(Error::AuthorityRejected(rejection.error))
                        }
                    }
                }
            }
        })
    }

    pub(crate) async fn rollback_async(self: &Arc<Self>, rejection: PushRejection) -> Result<()> {
        let workflow = self.policy.enter_workflow().await?;
        let result = self.rollback_locked_async(rejection).await;
        workflow.complete(RefreshTransition::Unchanged);
        result
    }

    async fn rollback_locked_async(self: &Arc<Self>, rejection: PushRejection) -> Result<()> {
        if rejection.database_id != self.database_id
            || rejection.device_id != self.client.device()
            || rejection.failed_at != rejection.submit_cursors.neck
        {
            return Err(Error::StalePushRejection);
        }
        let proposal =
            CommitProposal::reject_submissions(rejection.failed_at, rejection.submit_cursors)?;
        self.committer.propose(proposal).await?;
        Ok(())
    }

    pub(crate) async fn pull_async(self: &Arc<Self>) -> Result<PullOutcome> {
        let workflow = self.policy.enter_workflow().await?;
        match self.pull_locked_async().await {
            Ok(outcome) => {
                workflow.complete(RefreshTransition::Pulled {
                    pulled_at: Instant::now(),
                });
                Ok(outcome)
            }
            Err(error) => {
                workflow.complete(RefreshTransition::Unchanged);
                Err(error)
            }
        }
    }

    async fn pull_locked_async(self: &Arc<Self>) -> Result<PullOutcome> {
        let through = self.authority.pull().await?;
        Ok(PullOutcome { through })
    }

    pub(crate) async fn rebase_async(self: &Arc<Self>) -> Result<()> {
        let workflow = self.policy.enter_workflow().await?;
        let result = self.rebase_locked_async().await;
        workflow.complete(if result.is_ok() {
            RefreshTransition::Rebased { pulled_at: None }
        } else {
            RefreshTransition::Unchanged
        });
        result
    }

    async fn rebase_locked_async(self: &Arc<Self>) -> Result<()> {
        let space_id = self.database_id.space_id();
        let owner = self.owner.clone();
        let (initial_submit, initial_admits) = blocking::run(move || {
            let store = DatabaseMetaStore::read_only(owner);
            pollster::block_on(async {
                let submit = store.oplog_cursors(space_id).await?;
                let admits = store.admit_cursors(space_id).await?;
                Ok::<_, homebase_core::storage::StorageError>((submit, admits))
            })
            .map_err(Error::from)
        })
        .await?;
        if initial_submit.neck != initial_submit.tail {
            return Err(Error::RebasePendingSubmissions);
        }

        let admit_range = initial_admits.neck..initial_admits.tail;
        if admit_range.is_empty() {
            return Ok(());
        }
        let client = Arc::clone(&self.client);
        let batches = blocking::run(move || {
            pollster::block_on(async {
                let space = client.space(space_id).await?;
                space
                    .admits()
                    .iter(admit_range)
                    .await
                    .map_err(ClientError::from)
            })
            .map_err(Error::from)
        })
        .await?;
        let transactions = batches
            .into_iter()
            .map(|batch| {
                if batch.entries.is_empty() {
                    return Ok(None);
                }
                let transaction = MultiliteTransaction::from_homebase(&batch)?;
                Ok(Some(AdmittedTransaction {
                    device: batch.device,
                    transaction,
                }))
            })
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        let proposal = CommitProposal::apply_admissions(
            initial_submit,
            initial_admits,
            initial_admits.tail,
            self.client.device(),
            transactions,
        )?;
        self.committer.propose(proposal).await?;
        Ok(())
    }

    pub(crate) async fn execute_async<Q>(self: &Arc<Self>, sql: String, params: Q) -> Result<usize>
    where
        Q: Params + Send + 'static,
    {
        let validated = sql::validate_execute(&sql)?;
        self.execute_validated_async(sql, params, validated).await
    }

    pub(super) async fn execute_validated_async<Q>(
        self: &Arc<Self>,
        sql: String,
        params: Q,
        validated: sql::ValidatedExecute,
    ) -> Result<usize>
    where
        Q: Params + Send + 'static,
    {
        self.update_async(move |update| update.execute_validated(&sql, params, validated))
            .await
    }

    pub(crate) async fn query_async<T, P, F>(
        self: &Arc<Self>,
        sql: String,
        params: P,
        map: F,
    ) -> Result<Vec<T>>
    where
        T: Send + 'static,
        P: Params + Send + 'static,
        F: Send + 'static + for<'a> FnMut(&Row<'a>) -> rusqlite::Result<T>,
    {
        let validated = sql::validate_statement(&sql)?;
        if validated.output() != sql::StatementOutput::Rows {
            return Err(Error::StatementModeMismatch);
        }
        match validated {
            sql::ValidatedStatement::Read(_) => {
                self.view_async(move |view| view.query_prevalidated(&sql, params, map))
                    .await
            }
            sql::ValidatedStatement::Write(validated) => {
                self.query_write_validated_async(sql, params, map, *validated)
                    .await
            }
        }
    }

    pub(super) async fn query_write_validated_async<T, P, F>(
        self: &Arc<Self>,
        sql: String,
        params: P,
        map: F,
        validated: sql::ValidatedExecute,
    ) -> Result<Vec<T>>
    where
        T: Send + 'static,
        P: Params + Send + 'static,
        F: Send + 'static + for<'a> FnMut(&Row<'a>) -> rusqlite::Result<T>,
    {
        self.update_async(move |update| update.query_validated(&sql, params, map, validated))
            .await
    }

    pub(crate) async fn view_async<T, F>(self: &Arc<Self>, operation: F) -> Result<T>
    where
        T: Send + 'static,
        F: Send + 'static + for<'a> FnOnce(&ViewTransaction<'a>) -> Result<T>,
    {
        self.refresh_read_async().await?;
        let snapshot = self.committer.capture_view().await?;
        blocking::run(move || run_branch_view(snapshot, operation)).await
    }

    pub(crate) async fn update_async<T, F>(self: &Arc<Self>, operation: F) -> Result<T>
    where
        T: Send + 'static,
        F: Send + 'static + for<'a> FnOnce(&mut UpdateTransaction<'a, H>) -> Result<T>,
    {
        self.update_with_async(UpdateOptions::new(self.isolation_level), operation)
            .await
    }

    pub(crate) async fn update_with_async<T, F>(
        self: &Arc<Self>,
        options: UpdateOptions,
        operation: F,
    ) -> Result<T>
    where
        T: Send + 'static,
        F: Send + 'static + for<'a> FnOnce(&mut UpdateTransaction<'a, H>) -> Result<T>,
    {
        self.refresh_read_async().await?;
        let snapshot = self.committer.capture_snapshot(true).await?;
        let rowid_allocator = self.rowid_allocator.clone();
        let isolation = options.isolation_level();
        let BranchUpdate {
            value,
            proposal,
            history_pin: _history_pin,
        } = blocking::run(move || {
            run_branch_update(snapshot, rowid_allocator, isolation, operation)
        })
        .await?;
        if let Some(proposal) = proposal {
            let receipt = self.committer.propose(proposal).await?;
            self.finish_branch_write_async(receipt).await?;
        }
        Ok(value)
    }

    pub(crate) async fn prepare_async(
        self: &Arc<Self>,
        runtime: Arc<DatabaseRuntime>,
        sql: String,
    ) -> Result<Statement<H>> {
        let database = Arc::clone(self);
        blocking::run(move || database.prepare(&runtime, &sql)).await
    }

    pub(super) async fn refresh_read_async(self: &Arc<Self>) -> Result<()> {
        let requested_at = Instant::now();
        let Some(workflow) = self.policy.refresh_if_needed(requested_at).await? else {
            return Ok(());
        };
        let mut transition = RefreshTransition::Unchanged;
        let result = async {
            let submit = self.submit_cursors_async().await?;
            if submit.neck < submit.tail {
                match self.push_via_authority_async(None).await? {
                    PushOutcome::Drained => {}
                    PushOutcome::Rejected(rejection) => {
                        return Err(Error::RefreshPushRejected(rejection));
                    }
                }
            }
            self.pull_locked_async().await?;
            let pulled_at = Instant::now();
            transition = RefreshTransition::Pulled { pulled_at };
            self.rebase_locked_async().await?;
            transition = RefreshTransition::Rebased {
                pulled_at: Some(pulled_at),
            };
            Ok(())
        }
        .await;
        workflow.complete(transition);
        result
    }

    async fn submit_cursors_async(self: &Arc<Self>) -> Result<OplogCursors> {
        let owner = self.owner.clone();
        let space = self.database_id.space_id();
        blocking::run(move || {
            let store = DatabaseMetaStore::read_only(owner);
            pollster::block_on(store.oplog_cursors(space)).map_err(Error::from)
        })
        .await
    }

    pub(super) async fn finish_branch_write_async(
        self: &Arc<Self>,
        receipt: CommitReceipt,
    ) -> Result<()> {
        let sequence = receipt.submitted.ok_or(Error::CaptureInvariant(
            "transaction commit receipt has no Homebase sequence",
        ))?;
        match self.policy.policy() {
            SyncPolicy::LocalOnly => Ok(()),
            SyncPolicy::LocalFirst { write_delay, .. } => {
                self.policy.schedule_group(receipt.commit_seq, write_delay);
                Ok(())
            }
            SyncPolicy::Remote => {
                let workflow = self.policy.enter_workflow().await?;
                let result = async {
                    match self.push_via_authority_async(Some(sequence)).await? {
                        PushOutcome::Drained => Ok(()),
                        PushOutcome::Rejected(rejection) => {
                            let error = rejection.error.clone();
                            self.rollback_locked_async(rejection).await?;
                            let _ = self.push_via_authority_async(None).await;
                            Err(Error::AuthorityRejected(error))
                        }
                    }
                }
                .await;
                workflow.complete(RefreshTransition::Unchanged);
                result
            }
        }
    }
}
