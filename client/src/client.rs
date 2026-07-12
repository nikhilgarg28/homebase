//! The client: one device's local-first view of many spaces.
//!
//! [`Client`] is the device-scoped coordinator — one [`MetaStore`], one
//! device identity, and independent persisted per-space oplogs — over many attached
//! spaces. Open a client over any [`MetaStore`] implementation, then
//! [`attach`](Client::attach) an envelope and [`space`](Client::space) to
//! work in it.
//!
//! # Coordination model
//!
//! One small event loop owns session state and grants per-space workflows
//! in FIFO order. It performs no storage, crypto, network, timer, or other
//! slow work. Public futures do network work in their executor task; bulk
//! crypto runs on the client's blocking pool; MetaStore adapters are
//! responsible for moving blocking SQLite work onto that same kind of
//! worker boundary. Results re-enter through channels and only the granted
//! workflow applies coordination-state transitions. Different spaces may
//! progress concurrently, so the loop is the correctness chokepoint rather
//! than a global performance chokepoint.

use crate::cipher::{
    CipherError, NonceSource, SpaceCipher, SpaceEnvelope, SystemNonceSource, V1_CIPHER_EPOCH,
    ValueNonce,
};
use crate::coordination::{BlockingPool, CoordinationError, Coordinator, SpacePermit};
use crate::meta::{CodecRecord, MetaStore, certify};
use crate::server::{ServerHandle, offline_router};
use crate::space::{DEFAULT_PUSH_CAP, PushOutcome, Space, SpaceDriverError, live_write_leases};
use homebase_core::clock::HybridClock;
use homebase_core::messages::{AdmissionBatch, AdmissionRequest, AdmissionResult, KernelError};
use homebase_core::space::{SpaceError, SpaceId};
use homebase_core::storage::StorageError;
use homebase_core::tag::{DeviceId, DeviceSeq};
use std::collections::BTreeMap;
use std::fmt;

/// Fast session state owned exclusively by the coordination loop. Durable
/// oplog, cursor, and lease truth remains in MetaStore.
pub(crate) struct ClientSession<N> {
    pub(crate) device: DeviceId,
    pub(crate) push_cap: usize,
    nonce_source: N,
    attached: BTreeMap<SpaceId, SpaceCipher>,
}

/// One device across many spaces.
pub struct Client<M, H, C, N = SystemNonceSource> {
    store: M,
    server: H,
    clock: C,
    coordinator: Coordinator<ClientSession<N>>,
    workers: BlockingPool,
}

impl<M: MetaStore, H: ServerHandle, C: HybridClock, N: NonceSource + Send + 'static>
    Client<M, H, C, N>
{
    /// Open a client over durable truth, a server endpoint, and a clock.
    ///
    /// `fresh` is used only when the store has no device record yet.
    /// Panics if loaded state fails [`certify`].
    pub async fn open(
        store: M,
        server: H,
        clock: C,
        fresh: DeviceId,
        nonce_source: N,
    ) -> Result<Self, ClientError> {
        let state = store.load().await?;
        certify(&state);
        let device = match state.device {
            Some(id) => id,
            None => {
                store.record_device(fresh).await?;
                fresh
            }
        };

        let now = clock.stamp();
        if state.clock_high.is_some_and(|high| now.wall < high) {
            for (space, space_state) in &state.spaces {
                let dead: Vec<_> = space_state
                    .leases
                    .values()
                    .map(|held| crate::meta::HeldLease {
                        lease: held.lease.clone(),
                        deadline: homebase_core::clock::HybridTimestamp::ZERO,
                        barrier: held.barrier,
                        forgotten: held.forgotten,
                    })
                    .collect();
                if !dead.is_empty() {
                    store.record_leases(*space, &dead).await?;
                }
            }
        }
        store.record_clock(now.wall).await?;

        let coordinator = Coordinator::new(ClientSession {
            device,
            push_cap: DEFAULT_PUSH_CAP,
            nonce_source,
            attached: BTreeMap::new(),
        })?;
        let workers = BlockingPool::new(2)?;
        Ok(Self {
            store,
            server,
            clock,
            coordinator,
            workers,
        })
    }

    pub fn device(&self) -> DeviceId {
        self.coordinator.call(|session| session.device)
    }

    /// Replace the grouping cap (entries per wire batch).
    pub fn with_push_cap(&self, cap: usize) -> &Self {
        assert!(cap > 0, "a zero cap would ship nothing");
        self.coordinator.call(move |session| session.push_cap = cap);
        self
    }

    /// Whether this space's cipher is attached in this client session.
    pub fn is_attached(&self, id: SpaceId) -> bool {
        self.coordinator
            .call(move |session| session.attached.contains_key(&id))
    }

    /// Attach a space for this session. Persists the envelope to the codec
    /// cache when absent; verifies it matches when present. Idempotent when
    /// already attached.
    pub async fn attach(&self, envelope: &SpaceEnvelope) -> Result<(), ClientError> {
        let cipher = envelope.open()?;
        let id = cipher.space_id();
        let _permit = self.enter_space(id).await?;

        let state = self.store.load().await?;
        match state.spaces.get(&id).and_then(|s| s.codec.as_ref()) {
            None => {
                self.store
                    .record_codec(
                        id,
                        &CodecRecord {
                            cipher_epoch: V1_CIPHER_EPOCH,
                            sealed: envelope.encode(),
                        },
                    )
                    .await?;
            }
            Some(record) => {
                let stored = SpaceEnvelope::decode(&record.sealed)?;
                if stored != *envelope {
                    return Err(ClientError::CodecMismatch { id });
                }
            }
        }

        if self.is_attached(id) {
            return Ok(());
        }

        self.coordinator.call(move |session| {
            session.attached.insert(id, cipher);
        });
        Ok(())
    }

    /// A handle to a space. Loads from the codec cache when not yet attached.
    pub async fn space(&self, id: SpaceId) -> Result<Space<'_, M, H, C, N>, ClientError> {
        if !self.is_attached(id) {
            self.attach_from_codec(id).await?;
        }
        Ok(Space::new(self, id))
    }

    /// Space ids attached in this session, in order.
    pub fn attached(&self) -> Vec<SpaceId> {
        self.coordinator
            .call(|session| session.attached.keys().copied().collect())
    }

    async fn attach_from_codec(&self, id: SpaceId) -> Result<(), ClientError> {
        let _permit = self.enter_space(id).await?;
        if self.is_attached(id) {
            return Ok(());
        }
        let state = self.store.load().await?;
        let Some(record) = state.spaces.get(&id).and_then(|s| s.codec.as_ref()) else {
            return Err(ClientError::MissingCodec(id));
        };
        let envelope = SpaceEnvelope::decode(&record.sealed)?;
        let cipher = envelope.open_expected(id)?;
        self.coordinator.call(move |session| {
            session.attached.insert(id, cipher);
        });
        Ok(())
    }

    pub(crate) fn store(&self) -> &M {
        &self.store
    }

    pub(crate) fn server(&self) -> &H {
        &self.server
    }

    pub(crate) fn clock(&self) -> &C {
        &self.clock
    }

    pub(crate) fn cipher(&self, id: SpaceId) -> SpaceCipher {
        self.coordinator.call(move |session| {
            session
                .attached
                .get(&id)
                .expect("space must be attached")
                .clone()
        })
    }

    pub(crate) fn next_nonce(&self) -> Result<ValueNonce, String> {
        self.coordinator
            .call(|session| session.nonce_source.next_nonce())
    }

    pub(crate) async fn enter_space(
        &self,
        space: SpaceId,
    ) -> Result<SpacePermit<ClientSession<N>>, CoordinationError> {
        self.coordinator.enter(space).await
    }

    pub(crate) async fn run_blocking<F, R>(&self, work: F) -> Result<R, CoordinationError>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        self.workers.run(work).await
    }

    /// Drain every persisted per-space oplog. This compatibility entry point
    /// becomes space-scoped when the push API batch lands.
    pub async fn push(&self) -> Result<PushOutcome, ClientError> {
        let state = self.store.load().await?;
        let mut acked = None;
        for (space, space_state) in state.spaces {
            if space_state.cursors.neck >= space_state.cursors.tail {
                continue;
            }
            match self.push_space(space).await? {
                PushOutcome::Drained {
                    acked_through: space_acked,
                } => {
                    if space_acked.is_some() {
                        acked = space_acked;
                    }
                }
                stalled @ PushOutcome::Stalled { .. } => return Ok(stalled),
            }
        }
        Ok(PushOutcome::Drained {
            acked_through: acked,
        })
    }

    async fn push_space(&self, space: SpaceId) -> Result<PushOutcome, ClientError> {
        let _permit = self.enter_space(space).await?;
        let mut acked = None;
        let mut probe = false;
        loop {
            let state = self.store.load().await?;
            let Some(space_state) = state.spaces.get(&space) else {
                return Ok(PushOutcome::Drained {
                    acked_through: acked,
                });
            };
            let cursors = space_state.cursors;
            if cursors.neck >= cursors.tail {
                return Ok(PushOutcome::Drained {
                    acked_through: acked,
                });
            }
            let push_cap = self.coordinator.call(|session| session.push_cap);
            let until = DeviceSeq(
                cursors
                    .neck
                    .0
                    .saturating_add(push_cap as u64 - 1)
                    .min(cursors.tail.0 - 1),
            );
            let device = self.device();

            let window = self.store.oplog(space, cursors.neck, until).await?;
            let Some((head, head_record)) = window.first() else {
                self.store.trim_oplog(space, until).await?;
                continue;
            };
            let head = *head;
            let mut last = head;
            let mut batches = vec![AdmissionBatch {
                device_seq: head,
                range_asserts: head_record.range_asserts().to_vec(),
                entries: head_record.entries().to_vec(),
            }];
            if !probe {
                for (seq, record) in &window[1..] {
                    if seq.0 != last.0 + 1
                        || batches
                            .iter()
                            .map(|batch| batch.entries.len())
                            .sum::<usize>()
                            + record.entries().len()
                            > push_cap
                    {
                        break;
                    }
                    batches.push(AdmissionBatch {
                        device_seq: *seq,
                        range_asserts: record.range_asserts().to_vec(),
                        entries: record.entries().to_vec(),
                    });
                    last = *seq;
                }
            }
            let keys: Vec<_> = batches
                .iter()
                .flat_map(|batch| batch.entries.iter().map(|entry| entry.key().clone()))
                .collect();
            let batch_count = batches.len();
            let request = AdmissionRequest {
                device,
                evidence: live_write_leases(self.store(), self.clock(), space, &keys).await?,
                batches,
            };
            match self.server.admit(&space, request).await {
                Ok(response) => {
                    if response.results.len() != batch_count {
                        return Err(ClientError::Space(SpaceDriverError::Unavailable {
                            reason: format!(
                                "malformed admit response: {} results for {} batches",
                                response.results.len(),
                                batch_count
                            ),
                        }));
                    }
                    if let Some((failed, error)) =
                        response
                            .results
                            .iter()
                            .enumerate()
                            .find_map(|(i, result)| match result {
                                AdmissionResult::Applied { .. } => None,
                                AdmissionResult::Failed { error } => Some((i, error.clone())),
                            })
                    {
                        let at = DeviceSeq(head.0 + failed as u64);
                        if last > head {
                            probe = true;
                            continue;
                        }
                        return Ok(PushOutcome::Stalled {
                            at,
                            error,
                            acked_through: acked,
                        });
                    }
                    self.store.trim_oplog(space, last).await?;
                    acked = Some(last);
                    probe = false;
                }
                Err(SpaceError::Kernel(KernelError::DeviceSeqRegression { current, .. })) => {
                    let ours = current < cursors.tail
                        && !self.store.oplog(space, current, current).await?.is_empty();
                    if !ours {
                        return Err(ClientError::Space(SpaceDriverError::Fork {
                            admitted: current,
                        }));
                    }
                    self.store.trim_oplog(space, current).await?;
                    acked = Some(current);
                    probe = false;
                }
                Err(SpaceError::Kernel(error)) => {
                    if last > head {
                        probe = true;
                        continue;
                    }
                    return Ok(PushOutcome::Stalled {
                        at: head,
                        error,
                        acked_through: acked,
                    });
                }
                Err(SpaceError::Unavailable { reason }) => {
                    return Err(ClientError::Space(SpaceDriverError::Unavailable { reason }));
                }
            }
        }
    }

    /// Retire a space's active oplog window after a definitive rejection.
    /// Retry an ambiguous push before calling this method.
    pub async fn rollback(&self, space: SpaceId, to: DeviceSeq) -> Result<(), ClientError> {
        let _permit = self.enter_space(space).await?;
        self.store.rollback(space, to).await?;
        Ok(())
    }
}

/// Open a client with no server endpoint.
pub async fn open_offline<M, C, N>(
    store: M,
    clock: C,
    fresh: DeviceId,
    nonce_source: N,
) -> Result<
    Client<M, impl Fn(&SpaceId) -> Option<crate::server::UnreachableSpace> + Sync + Copy, C, N>,
    ClientError,
>
where
    M: MetaStore,
    C: HybridClock,
    N: NonceSource + Send + 'static,
{
    Client::open(store, offline_router(), clock, fresh, nonce_source).await
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ClientError {
    Store(StorageError),
    Space(SpaceDriverError),
    Cipher(CipherError),
    MissingCodec(SpaceId),
    CodecMismatch { id: SpaceId },
    Coordination { reason: String },
}

impl From<StorageError> for ClientError {
    fn from(err: StorageError) -> Self {
        Self::Store(err)
    }
}

impl From<SpaceDriverError> for ClientError {
    fn from(err: SpaceDriverError) -> Self {
        Self::Space(err)
    }
}

impl From<CipherError> for ClientError {
    fn from(err: CipherError) -> Self {
        Self::Cipher(err)
    }
}

impl From<CoordinationError> for ClientError {
    fn from(err: CoordinationError) -> Self {
        Self::Coordination {
            reason: err.to_string(),
        }
    }
}

impl fmt::Display for ClientError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Store(err) => write!(f, "{err}"),
            Self::Space(err) => write!(f, "{err}"),
            Self::Cipher(err) => write!(f, "{err}"),
            Self::MissingCodec(id) => write!(f, "no codec record for space {id:?}"),
            Self::CodecMismatch { id } => {
                write!(
                    f,
                    "envelope does not match persisted codec for space {id:?}"
                )
            }
            Self::Coordination { reason } => write!(f, "{reason}"),
        }
    }
}

impl std::error::Error for ClientError {}
