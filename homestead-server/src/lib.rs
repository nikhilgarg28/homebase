//! The homestead kernel server.
//!
//! Layering, outermost in:
//!
//! - [`Server`] — hosts many spaces; routes `SpaceId` → space. Token
//!   verification (token → `SpaceId` + prefix scope) sits at the wire layer
//!   above this.
//! - [`space::Space`] — one space's complete verb state machine (lease
//!   table + data plane), deterministic: explicit `now: Timestamp`, verbs
//!   executed one at a time, proptested and torture-simmed directly. It
//!   will grow into the async facade implementing the core `Space` trait
//!   once it owns a store, a clock, and request serialization.
//! - [`storage::OrderedStore`] — the async ordered map underneath (slatedb
//!   in prod, [`storage::MemoryStore`] in tests); determinism holds because
//!   verbs never interleave and the test store resolves futures immediately.

pub mod error;
pub mod schema;
pub mod space;
pub mod storage;

use homestead_core::space::{Space, SpaceId};
use std::collections::HashMap;
use std::sync::Arc;

/// Hosts many spaces behind one endpoint.
///
/// Spaces are fully isolated: no verb spans two spaces, so the server layer
/// is pure routing plus space lifecycle. Lifecycle is deliberately *not* one
/// of the seven data-plane verbs — creating a space is a control-plane
/// action (provisioning, quotas, tokens) and will be exposed via an admin
/// surface, not the space wire protocol.
///
/// Handles are `Arc`s because `Space` methods take `&self`; concurrent
/// requests to one space serialize inside its implementation, not here.
pub struct Server<S> {
    spaces: HashMap<SpaceId, Arc<S>>,
}

impl<S: Space> Server<S> {
    pub fn new() -> Self {
        Self {
            spaces: HashMap::new(),
        }
    }

    /// Registers a pre-built space (e.g. rehydrated from persistence).
    /// Returns `false` (and leaves the existing space untouched) if the id
    /// is already taken.
    pub fn insert_space(&mut self, id: SpaceId, space: S) -> bool {
        match self.spaces.entry(id) {
            std::collections::hash_map::Entry::Occupied(_) => false,
            std::collections::hash_map::Entry::Vacant(v) => {
                v.insert(Arc::new(space));
                true
            }
        }
    }

    /// Looks up a space handle for request routing.
    pub fn space(&self, id: &SpaceId) -> Option<Arc<S>> {
        self.spaces.get(id).cloned()
    }
}

impl<S: Space + Default> Server<S> {
    /// Creates a fresh, empty space under `id`. Returns `false` (and leaves
    /// the existing space untouched) if the id is already taken.
    pub fn create_space(&mut self, id: SpaceId) -> bool {
        self.insert_space(id, S::default())
    }
}

impl<S: Space> Default for Server<S> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use homestead_core::messages::*;
    use std::future::Future;

    /// Minimal `Space` impl: proves the trait is implementable and keeps the
    /// server generics honest. Every verb answers "empty".
    #[derive(Default)]
    struct NullSpace;

    impl Space for NullSpace {
        fn acquire(
            &self,
            req: AcquireRequest,
        ) -> impl Future<Output = Result<AcquireResponse, KernelError>> + Send {
            async move {
                Err(KernelError::Contended {
                    prefix: req.specs[0].prefix.clone(),
                    retry_after: None,
                })
            }
        }

        fn renew(
            &self,
            req: RenewRequest,
        ) -> impl Future<Output = Result<RenewResponse, KernelError>> + Send {
            async move {
                Ok(RenewResponse {
                    granted: Vec::new(),
                    invalid: req.leases,
                })
            }
        }

        fn release(
            &self,
            _req: ReleaseRequest,
        ) -> impl Future<Output = Result<ReleaseResponse, KernelError>> + Send {
            async move { Ok(ReleaseResponse {}) }
        }

        fn put_batch(
            &self,
            req: PutBatchRequest,
        ) -> impl Future<Output = Result<PutBatchResponse, KernelError>> + Send {
            async move {
                Err(KernelError::NotCovered {
                    key: req.entries[0].key.clone(),
                })
            }
        }

        fn get(
            &self,
            req: GetRequest,
        ) -> impl Future<Output = Result<GetResponse, KernelError>> + Send {
            async move {
                Ok(GetResponse {
                    entries: req.keys.iter().map(|_| None).collect(),
                })
            }
        }

        fn list(
            &self,
            _req: ListRequest,
        ) -> impl Future<Output = Result<ListResponse, KernelError>> + Send {
            async move {
                Ok(ListResponse {
                    entries: Vec::new(),
                    truncated: false,
                })
            }
        }

        fn read_at(
            &self,
            req: ReadAtRequest,
        ) -> impl Future<Output = Result<ReadAtResponse, KernelError>> + Send {
            async move {
                Ok(ReadAtResponse {
                    at: homestead_core::AdmissionSeq(0),
                    ranges: req.ranges.iter().map(|_| RangeCut::Snapshot(Vec::new())).collect(),
                })
            }
        }
    }

    #[test]
    fn routes_by_space_id() {
        let a = SpaceId([1; 16]);
        let b = SpaceId([2; 16]);

        let mut server = Server::new();
        assert!(server.insert_space(a, NullSpace));
        assert!(!server.insert_space(a, NullSpace), "duplicate id must be rejected");

        assert!(server.space(&a).is_some());
        assert!(server.space(&b).is_none());
    }

    #[test]
    fn create_space_builds_empty_spaces() {
        let a = SpaceId([1; 16]);

        let mut server = Server::<NullSpace>::new();
        assert!(server.create_space(a));
        assert!(!server.create_space(a), "duplicate id must be rejected");
        assert!(server.space(&a).is_some());
    }
}
