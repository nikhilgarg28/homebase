//! The client: one device's local-first view of many spaces.
//!
//! [`Client`] is the device-scoped coordinator — one [`MetaStore`], one
//! device identity, and independent persisted per-space oplogs — over many attached
//! spaces. Open a client over any [`MetaStore`] implementation, then
//! [`attach`](Client::attach) an envelope and [`space`](Client::space) to
//! work in it.

use crate::cipher::{
    CipherError, NonceSource, SpaceCipher, SpaceEnvelope, SystemNonceSource, V1_KEY_EPOCH,
    ValueNonce,
};
use crate::meta::{CodecRecord, MetaStore, certify};
use crate::server::{ServerHandle, offline_router};
use crate::space::{DEFAULT_PUSH_CAP, PushOutcome, Space, SpaceDriverError, live_write_leases};
use homebase_core::clock::HybridClock;
use homebase_core::key::Key;
use homebase_core::messages::{BatchOp, KernelError, PutBatch, PutBatchRequest, PutBatchResult};
use homebase_core::space::{SpaceError, SpaceId};
use homebase_core::storage::StorageError;
use homebase_core::tag::{DeviceId, DeviceSeq};
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::fmt;

/// Client-global scalars. Oplog cursors live only in the MetaStore.
#[derive(Clone, Debug)]
pub(crate) struct ClientGlobals {
    pub(crate) device: DeviceId,
    pub(crate) push_cap: usize,
}

/// One device across many spaces.
pub struct Client<M, H, C, N = SystemNonceSource> {
    store: M,
    server: H,
    clock: C,
    nonce_source: RefCell<N>,
    pub(crate) globals: RefCell<ClientGlobals>,
    attached: RefCell<BTreeMap<SpaceId, SpaceCipher>>,
}

impl<M: MetaStore, H: ServerHandle, C: HybridClock, N: NonceSource> Client<M, H, C, N> {
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
                        retiring: held.retiring,
                    })
                    .collect();
                if !dead.is_empty() {
                    store.record_leases(*space, &dead).await?;
                }
            }
        }
        store.record_clock(now.wall).await?;

        Ok(Self {
            store,
            server,
            clock,
            nonce_source: RefCell::new(nonce_source),
            globals: RefCell::new(ClientGlobals {
                device,
                push_cap: DEFAULT_PUSH_CAP,
            }),
            attached: RefCell::new(BTreeMap::new()),
        })
    }

    pub fn device(&self) -> DeviceId {
        self.globals.borrow().device
    }

    /// Replace the grouping cap (entries per wire batch).
    pub fn with_push_cap(&self, cap: usize) -> &Self {
        assert!(cap > 0, "a zero cap would ship nothing");
        self.globals.borrow_mut().push_cap = cap;
        self
    }

    /// Whether this space's cipher is attached in this client session.
    pub fn is_attached(&self, id: SpaceId) -> bool {
        self.attached.borrow().contains_key(&id)
    }

    /// Attach a space for this session. Persists the envelope to the codec
    /// cache when absent; verifies it matches when present. Idempotent when
    /// already attached.
    pub async fn attach(&self, envelope: &SpaceEnvelope) -> Result<(), ClientError> {
        let cipher = envelope.open()?;
        let id = cipher.space_id();

        let state = self.store.load().await?;
        match state.spaces.get(&id).and_then(|s| s.codec.as_ref()) {
            None => {
                self.store
                    .record_codec(
                        id,
                        &CodecRecord {
                            space_key_epoch: V1_KEY_EPOCH,
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

        self.attached.borrow_mut().insert(id, cipher);
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
        self.attached.borrow().keys().copied().collect()
    }

    async fn attach_from_codec(&self, id: SpaceId) -> Result<(), ClientError> {
        let state = self.store.load().await?;
        let Some(record) = state.spaces.get(&id).and_then(|s| s.codec.as_ref()) else {
            return Err(ClientError::MissingCodec(id));
        };
        let envelope = SpaceEnvelope::decode(&record.sealed)?;
        let cipher = envelope.open_expected(id)?;
        self.attached.borrow_mut().insert(id, cipher);
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
        self.attached
            .borrow()
            .get(&id)
            .expect("space must be attached")
            .clone()
    }

    pub(crate) fn next_nonce(&self) -> Result<ValueNonce, String> {
        self.nonce_source.borrow_mut().next_nonce()
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
            let push_cap = self.globals.borrow().push_cap;
            let until = DeviceSeq(
                cursors
                    .neck
                    .0
                    .saturating_add(push_cap as u64 - 1)
                    .min(cursors.tail.0 - 1),
            );
            let device = self.globals.borrow().device;

            let window = self.store.oplog(space, cursors.neck, until).await?;
            let Some((head, head_record)) = window.first() else {
                self.store.trim_oplog(space, until).await?;
                continue;
            };
            let head = *head;
            let mut last = head;
            let mut batches = vec![PutBatch {
                device_seq: head,
                range_asserts: head_record.range_asserts().to_vec(),
                ops: head_record
                    .entries()
                    .iter()
                    .cloned()
                    .map(Into::into)
                    .collect(),
            }];
            if !probe {
                for (seq, record) in &window[1..] {
                    if seq.0 != last.0 + 1
                        || batches.iter().map(|batch| batch.ops.len()).sum::<usize>()
                            + record.entries().len()
                            > push_cap
                    {
                        break;
                    }
                    batches.push(PutBatch {
                        device_seq: *seq,
                        range_asserts: record.range_asserts().to_vec(),
                        ops: record.entries().iter().cloned().map(Into::into).collect(),
                    });
                    last = *seq;
                }
            }
            let keys: Vec<_> = batches
                .iter()
                .flat_map(|batch| batch.ops.iter().filter_map(op_key).cloned())
                .collect();
            let batch_count = batches.len();
            let request = PutBatchRequest {
                device,
                evidence: live_write_leases(self.store(), self.clock(), space, &keys).await?,
                batches,
            };
            match self.server.put_batch(&space, request).await {
                Ok(response) => {
                    if response.results.len() != batch_count {
                        return Err(ClientError::Space(SpaceDriverError::Unavailable {
                            reason: format!(
                                "malformed put_batch response: {} results for {} batches",
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
                                PutBatchResult::Applied { .. } => None,
                                PutBatchResult::Failed { error } => Some((i, error.clone())),
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

    /// Temporary pre-rollback API: drop one space's queued suffix.
    pub async fn discard_from(&self, space: SpaceId, from: DeviceSeq) -> Result<(), ClientError> {
        self.store.discard_from(space, from).await?;
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
    N: NonceSource,
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
        }
    }
}

fn op_key(op: &BatchOp) -> Option<&Key> {
    match op {
        BatchOp::Set { key, .. } | BatchOp::Delete { key, .. } => Some(key),
        BatchOp::NoOp => None,
    }
}

impl std::error::Error for ClientError {}
