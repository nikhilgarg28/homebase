//! Spaces: the unit of isolation and the client-facing verb contract.
//!
//! A space is one ordered map + one lease table + one admission sequence.
//! Every request executes within exactly one space. A server hosts many
//! spaces and routes to them (`SpaceId` → space, token → `SpaceId`), which
//! is why request bodies never carry a `SpaceId`.

use crate::messages::{
    AcquireRequest, AcquireResponse, GetRequest, GetResponse, KernelError, ListLeasesRequest,
    ListLeasesResponse, ListRequest, ListResponse, PutBatchRequest, PutBatchResponse,
    ReadAtRequest, ReadAtResponse, ReleaseRequest, ReleaseResponse, RenewRequest, RenewResponse,
};
use std::fmt;
use std::future::Future;

/// Identifies a space: 16 opaque bytes, UUID-shaped.
///
/// The kernel never generates or interprets these; they come from the
/// platform (token claims, provisioning).
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SpaceId(pub [u8; 16]);

impl fmt::Debug for SpaceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "space:")?;
        for b in self.0 {
            write!(f, "{b:02x}")?;
        }
        Ok(())
    }
}

/// Why a verb call failed.
///
/// Two very different failure planes share this type so the [`Space`] trait
/// can be honest about both:
///
/// - [`Kernel`](SpaceError::Kernel): a semantic rejection — the space is
///   healthy and *decided* no (contended, fenced, regression…). Meaningful
///   to the caller; retry per that error's own rules.
/// - [`Unavailable`](SpaceError::Unavailable): the space could not serve
///   the request at all — storage fault, shutdown mid-request, dead
///   mailbox. Says nothing about the request's validity. Reads may be
///   retried blindly; a retried `put_batch` that was actually admitted
///   before the failure is caught by the `device_seq` replay fence, so
///   clients treat that rejection as "already applied".
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SpaceError {
    Kernel(KernelError),
    Unavailable { reason: String },
}

impl SpaceError {
    pub fn unavailable(reason: impl Into<String>) -> Self {
        Self::Unavailable {
            reason: reason.into(),
        }
    }
}

impl From<KernelError> for SpaceError {
    fn from(err: KernelError) -> Self {
        Self::Kernel(err)
    }
}

impl fmt::Display for SpaceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Kernel(err) => write!(f, "{err}"),
            Self::Unavailable { reason } => write!(f, "space unavailable: {reason}"),
        }
    }
}

impl std::error::Error for SpaceError {}

/// The seven verbs — the contract between the server's implementation, the
/// in-process client used by tests and the torture sim, and (later, behind
/// the wire) the remote client.
///
/// Async because implementations sit on disk and network IO. Methods take
/// `&self`: handles are shared across tasks, and admission serialization is
/// an implementation obligation (the server wraps its deterministic state
/// machine in a mutex/actor), not a signature property. The state machine
/// itself — synchronous, explicit `now` — lives in the server crate.
///
/// Methods are written in desugared form so the returned futures are
/// guaranteed `Send` (required under multi-threaded executors). The cost is
/// dyn-compatibility: consumers stay generic over `S: Space`.
pub trait Space {
    fn acquire(
        &self,
        req: AcquireRequest,
    ) -> impl Future<Output = Result<AcquireResponse, SpaceError>> + Send;

    fn renew(
        &self,
        req: RenewRequest,
    ) -> impl Future<Output = Result<RenewResponse, SpaceError>> + Send;

    fn release(
        &self,
        req: ReleaseRequest,
    ) -> impl Future<Output = Result<ReleaseResponse, SpaceError>> + Send;

    fn list_leases(
        &self,
        req: ListLeasesRequest,
    ) -> impl Future<Output = Result<ListLeasesResponse, SpaceError>> + Send;

    fn put_batch(
        &self,
        req: PutBatchRequest,
    ) -> impl Future<Output = Result<PutBatchResponse, SpaceError>> + Send;

    fn get(&self, req: GetRequest) -> impl Future<Output = Result<GetResponse, SpaceError>> + Send;

    fn list(
        &self,
        req: ListRequest,
    ) -> impl Future<Output = Result<ListResponse, SpaceError>> + Send;

    fn read_at(
        &self,
        req: ReadAtRequest,
    ) -> impl Future<Output = Result<ReadAtResponse, SpaceError>> + Send;
}
