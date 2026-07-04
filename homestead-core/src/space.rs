//! Spaces: the unit of isolation and the client-facing verb contract.
//!
//! A space is one ordered map + one lease table + one admission sequence.
//! Every request executes within exactly one space. A server hosts many
//! spaces and routes to them (`SpaceId` → space, token → `SpaceId`), which
//! is why request bodies never carry a `SpaceId`.

use crate::messages::{
    AcquireRequest, AcquireResponse, GetRequest, GetResponse, KernelError, ListRequest,
    ListResponse, PutBatchRequest, PutBatchResponse, ReadAtRequest, ReadAtResponse,
    ReleaseRequest, ReleaseResponse, RenewRequest, RenewResponse,
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
    ) -> impl Future<Output = Result<AcquireResponse, KernelError>> + Send;

    fn renew(
        &self,
        req: RenewRequest,
    ) -> impl Future<Output = Result<RenewResponse, KernelError>> + Send;

    fn release(
        &self,
        req: ReleaseRequest,
    ) -> impl Future<Output = Result<ReleaseResponse, KernelError>> + Send;

    fn put_batch(
        &self,
        req: PutBatchRequest,
    ) -> impl Future<Output = Result<PutBatchResponse, KernelError>> + Send;

    fn get(&self, req: GetRequest) -> impl Future<Output = Result<GetResponse, KernelError>> + Send;

    fn list(
        &self,
        req: ListRequest,
    ) -> impl Future<Output = Result<ListResponse, KernelError>> + Send;

    fn read_at(
        &self,
        req: ReadAtRequest,
    ) -> impl Future<Output = Result<ReadAtResponse, KernelError>> + Send;
}
