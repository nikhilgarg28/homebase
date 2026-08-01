//! Async-first orchestration over owned committer, authority, and branch work.

use std::sync::Arc;
use std::time::Instant;

use homebase_client::meta::{MetaStore, OplogCursors};
use homebase_client::{ClientError, PushOutcome as HomebasePushOutcome, ServerHandle};
use homebase_core::tag::DeviceSeq;

use super::store::DatabaseMetaStore;
use super::transaction::MultiliteTransaction;
use super::update::{BranchUpdate, run_branch_update};
use super::view::{ViewTransaction, run_branch_view};
use super::{
    Database, DatabaseRuntime, PullOutcome, PushOutcome, PushRejection, Statement, SyncPolicy,
    UpdateOptions, UpdateTransaction, sql,
};
use crate::commit::proposal::{AdmittedTransaction, CommitProposal, CommitReceipt};
use crate::{Error, Params, Result, blocking};

impl<H: ServerHandle + Send + Sync + 'static> Database<H> {
    pub(crate) async fn push_async(self: &Arc<Self>) -> Result<PushOutcome> {
        self.push_via_authority_async(None).await
    }

    async fn push_submission_async(self: &Arc<Self>, sequence: DeviceSeq) -> Result<PushOutcome> {
        self.push_via_authority_async(Some(sequence)).await
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

    pub(crate) async fn rollback_async(self: &Arc<Self>, rejection: PushRejection) -> Result<()> {
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
        let _refresh = self.refresh.enter().await?;
        self.pull_locked_async().await
    }

    async fn pull_locked_async(self: &Arc<Self>) -> Result<PullOutcome> {
        let through = self.authority.pull().await?;
        self.policy.mark_pulled();
        Ok(PullOutcome { through })
    }

    pub(crate) async fn rebase_async(self: &Arc<Self>) -> Result<()> {
        let _refresh = self.refresh.enter().await?;
        self.rebase_locked_async().await
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
        if !self.policy.read_requires_refresh_since(requested_at) {
            return Ok(());
        }
        let _refresh = self.refresh.enter().await?;
        if !self.policy.read_requires_refresh_since(requested_at) {
            return Ok(());
        }
        let submit = self.submit_cursors_async().await?;
        if submit.neck < submit.tail {
            match self.push_async().await? {
                PushOutcome::Drained => {}
                PushOutcome::Rejected(rejection) => {
                    return Err(Error::RefreshPushRejected(rejection));
                }
            }
        }
        self.pull_locked_async().await?;
        let pulled_at = Instant::now();
        self.rebase_locked_async().await?;
        self.policy.mark_rebased(pulled_at);
        Ok(())
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
                self.scheduler
                    .schedule_group(receipt.commit_seq, write_delay);
                Ok(())
            }
            SyncPolicy::Remote => match self.push_submission_async(sequence).await? {
                PushOutcome::Drained => Ok(()),
                PushOutcome::Rejected(rejection) => {
                    let error = rejection.error.clone();
                    self.rollback_async(rejection).await?;
                    let _ = self.push_async().await;
                    Err(Error::AuthorityRejected(error))
                }
            },
        }
    }
}
