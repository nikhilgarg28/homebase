//! The client's view of the server: the seven verbs, space-qualified.
//!
//! [`ServerHandle`] is deliberately *not* a "transport" or "connector"
//! abstraction, and it hands out no per-space objects: it exposes the
//! verbs themselves — exactly the shape of the wire service — with every
//! call naming its space, because one server endpoint multiplexes all of
//! a device's spaces. A per-space view is the *derived* form (the engine
//! curries the space id internally); the primitive matches what every
//! implementation can honestly deliver: remotely there is one channel and
//! stateless verbs, never a per-space object. It is a *handle* in the same
//! sense as the server's own `SpaceHandle` — the cheap, shareable reach
//! into a thing that lives elsewhere.
//!
//! Implementations:
//! - **in-process**: any closure `|id| server.space(id)` — the blanket
//!   impl below routes each verb through it, and
//!   `homebase_server::Server::space` already has exactly the right shape;
//! - **none**: [`Offline`], the uninhabited server for clients with no
//!   server at all — it cannot even be constructed, so the compiler knows
//!   a serverless client never issues a verb;
//! - **remote**: the gRPC channel adapter lands with the transport-adapters
//!   batch, gated on [`conformance`]: observationally identical to
//!   in-process, or it doesn't merge.
//!
//! A space the endpoint doesn't serve surfaces on the first verb as
//! `Unavailable` — transport-plane, retryable, saying nothing about the
//! request. There is deliberately no "does this space exist?" query:
//! in-process it would be cheap, remotely it would be a round trip or a
//! lie, and the contract only promises what every implementation can keep.

use homebase_core::messages::{
    AcquireRequest, AcquireResponse, GetRequest, GetResponse, ListRequest, ListResponse,
    PutBatchRequest, PutBatchResponse, ReadAtRequest, ReadAtResponse, ReleaseRequest,
    ReleaseResponse, RenewRequest, RenewResponse,
};
use homebase_core::space::{Space, SpaceError, SpaceId};
use std::future::Future;

/// The seven verbs, space-qualified — the client's whole vocabulary for
/// talking to a server. Mirrors [`Space`] exactly, plus the space id per
/// call; methods are desugared so the futures are `Send`, same as the
/// core trait.
pub trait ServerHandle {
    fn acquire(
        &self,
        space: &SpaceId,
        req: AcquireRequest,
    ) -> impl Future<Output = Result<AcquireResponse, SpaceError>> + Send;

    fn renew(
        &self,
        space: &SpaceId,
        req: RenewRequest,
    ) -> impl Future<Output = Result<RenewResponse, SpaceError>> + Send;

    fn release(
        &self,
        space: &SpaceId,
        req: ReleaseRequest,
    ) -> impl Future<Output = Result<ReleaseResponse, SpaceError>> + Send;

    fn put_batch(
        &self,
        space: &SpaceId,
        req: PutBatchRequest,
    ) -> impl Future<Output = Result<PutBatchResponse, SpaceError>> + Send;

    fn get(
        &self,
        space: &SpaceId,
        req: GetRequest,
    ) -> impl Future<Output = Result<GetResponse, SpaceError>> + Send;

    fn list(
        &self,
        space: &SpaceId,
        req: ListRequest,
    ) -> impl Future<Output = Result<ListResponse, SpaceError>> + Send;

    fn read_at(
        &self,
        space: &SpaceId,
        req: ReadAtRequest,
    ) -> impl Future<Output = Result<ReadAtResponse, SpaceError>> + Send;
}

/// Closures over an in-process server are servers:
/// `|id| server.space(id)` is the whole local story. A space the closure
/// doesn't route (`None`) surfaces as `Unavailable`.
impl<S, F> ServerHandle for F
where
    S: Space + Send,
    F: Fn(&SpaceId) -> Option<S> + Sync,
{
    async fn acquire(
        &self,
        space: &SpaceId,
        req: AcquireRequest,
    ) -> Result<AcquireResponse, SpaceError> {
        match self(space) {
            Some(s) => s.acquire(req).await,
            None => Err(SpaceError::unavailable("space not served by this endpoint")),
        }
    }

    async fn renew(&self, space: &SpaceId, req: RenewRequest) -> Result<RenewResponse, SpaceError> {
        match self(space) {
            Some(s) => s.renew(req).await,
            None => Err(SpaceError::unavailable("space not served by this endpoint")),
        }
    }

    async fn release(
        &self,
        space: &SpaceId,
        req: ReleaseRequest,
    ) -> Result<ReleaseResponse, SpaceError> {
        match self(space) {
            Some(s) => s.release(req).await,
            None => Err(SpaceError::unavailable("space not served by this endpoint")),
        }
    }

    async fn put_batch(
        &self,
        space: &SpaceId,
        req: PutBatchRequest,
    ) -> Result<PutBatchResponse, SpaceError> {
        match self(space) {
            Some(s) => s.put_batch(req).await,
            None => Err(SpaceError::unavailable("space not served by this endpoint")),
        }
    }

    async fn get(&self, space: &SpaceId, req: GetRequest) -> Result<GetResponse, SpaceError> {
        match self(space) {
            Some(s) => s.get(req).await,
            None => Err(SpaceError::unavailable("space not served by this endpoint")),
        }
    }

    async fn list(&self, space: &SpaceId, req: ListRequest) -> Result<ListResponse, SpaceError> {
        match self(space) {
            Some(s) => s.list(req).await,
            None => Err(SpaceError::unavailable("space not served by this endpoint")),
        }
    }

    async fn read_at(
        &self,
        space: &SpaceId,
        req: ReadAtRequest,
    ) -> Result<ReadAtResponse, SpaceError> {
        match self(space) {
            Some(s) => s.read_at(req).await,
            None => Err(SpaceError::unavailable("space not served by this endpoint")),
        }
    }
}

/// The server type for clients that have no server. Uninhabited: a value
/// can never exist, so `None::<Offline>` is the only way to use it and no
/// verb is ever reachable — the compiler proves it.
pub enum Offline {}

impl ServerHandle for Offline {
    async fn acquire(
        &self,
        _space: &SpaceId,
        _req: AcquireRequest,
    ) -> Result<AcquireResponse, SpaceError> {
        match *self {}
    }

    async fn renew(
        &self,
        _space: &SpaceId,
        _req: RenewRequest,
    ) -> Result<RenewResponse, SpaceError> {
        match *self {}
    }

    async fn release(
        &self,
        _space: &SpaceId,
        _req: ReleaseRequest,
    ) -> Result<ReleaseResponse, SpaceError> {
        match *self {}
    }

    async fn put_batch(
        &self,
        _space: &SpaceId,
        _req: PutBatchRequest,
    ) -> Result<PutBatchResponse, SpaceError> {
        match *self {}
    }

    async fn get(&self, _space: &SpaceId, _req: GetRequest) -> Result<GetResponse, SpaceError> {
        match *self {}
    }

    async fn list(&self, _space: &SpaceId, _req: ListRequest) -> Result<ListResponse, SpaceError> {
        match *self {}
    }

    async fn read_at(
        &self,
        _space: &SpaceId,
        _req: ReadAtRequest,
    ) -> Result<ReadAtResponse, SpaceError> {
        match *self {}
    }
}

pub mod conformance {
    //! Reusable [`ServerHandle`] conformance: any implementation — the
    //! in-process closure today, the gRPC adapter later — must pass
    //! [`run_all`] against a fresh kernel. This is the transport-adapter
    //! batch's merge gate: remote must be observationally identical to
    //! in-process, and this suite is the definition of "observationally".

    use super::ServerHandle;
    use homebase_core::key::Key;
    use homebase_core::lease::{LeaseMode, LeaseRef};
    use homebase_core::messages::{
        AcquireRequest, GetRequest, KernelError, LeaseSpec, ListRequest, PrefixCursor,
        PutBatchRequest, PutEntry, RangeCut, ReadAtRequest, ReleaseRequest, RenewRequest,
    };
    use homebase_core::space::{SpaceError, SpaceId};
    use homebase_core::tag::{AdmissionSeq, DeviceId, DeviceSeq, Value, Ver};
    use std::time::Duration;

    fn dev(n: u8) -> DeviceId {
        DeviceId([n; 16])
    }

    fn key(components: &[&[u8]]) -> Key {
        Key::from_bytes(components.iter().copied()).unwrap()
    }

    fn wspec(prefix: &Key) -> LeaseSpec {
        LeaseSpec {
            prefix: prefix.clone(),
            mode: LeaseMode::Write,
            ttl: Duration::from_secs(60),
            stealable: false,
        }
    }

    /// Drives the whole suite. `a` and `b` must be **fresh** (never
    /// written) spaces the handle serves; `unknown` must not be served.
    /// Panics on any contract violation.
    pub async fn run_all<T: ServerHandle>(handle: &T, a: SpaceId, b: SpaceId, unknown: SpaceId) {
        assert_ne!(a, b, "conformance needs two distinct served spaces");
        verbs_roundtrip(handle, a, b"from-a").await;
        verbs_roundtrip(handle, b, b"from-b").await;
        cross_space_isolation(handle, a, b).await;
        kernel_rejections_stay_on_the_kernel_plane(handle, a).await;
        unserved_space_is_a_transport_error(handle, unknown).await;
    }

    /// All seven verbs, end to end, in one space: acquire → put → get /
    /// list / read_at → renew → release → prefix free again. Also pins
    /// per-space admission sequencing: a fresh space's first batch is
    /// admission 1 behind a barrier of 0.
    pub async fn verbs_roundtrip<T: ServerHandle>(handle: &T, space: SpaceId, marker: &[u8]) {
        let db = key(&[b"db"]);
        let k = key(&[b"db", b"k"]);

        let granted = handle
            .acquire(
                &space,
                AcquireRequest {
                    device: dev(1),
                    specs: vec![wspec(&db)],
                    steal: false,
                },
            )
            .await
            .expect("acquire on a served space");
        assert_eq!(granted.leases.len(), 1);
        assert_eq!(
            granted.barrier,
            AdmissionSeq(0),
            "fresh space: nothing admitted yet"
        );
        let lease = LeaseRef {
            id: granted.leases[0].id,
            epoch: granted.leases[0].epoch,
        };

        let put = handle
            .put_batch(
                &space,
                PutBatchRequest {
                    device: dev(1),
                    device_seq: DeviceSeq(1),
                    leases: vec![lease],
                    entries: vec![PutEntry {
                        key: k.clone(),
                        value: Value::Present(marker.to_vec()),
                        ver: Ver(1),
                    }],
                },
            )
            .await
            .expect("covered put");
        assert_eq!(
            put.admission_seq,
            AdmissionSeq(1),
            "fresh space: first admission"
        );

        let got = handle
            .get(
                &space,
                GetRequest {
                    keys: vec![k.clone()],
                },
            )
            .await
            .unwrap();
        assert_eq!(
            got.entries[0].as_ref().map(|e| &e.value),
            Some(&Value::Present(marker.to_vec()))
        );

        let listed = handle
            .list(
                &space,
                ListRequest {
                    prefix: db.clone(),
                    start_after: None,
                    limit: None,
                },
            )
            .await
            .unwrap();
        assert_eq!(listed.entries.len(), 1);
        assert!(!listed.truncated);

        let cut = handle
            .read_at(
                &space,
                ReadAtRequest {
                    ranges: vec![PrefixCursor {
                        prefix: db.clone(),
                        since: None,
                    }],
                },
            )
            .await
            .unwrap();
        assert_eq!(cut.at, AdmissionSeq(1));
        assert!(matches!(&cut.ranges[0], RangeCut::Snapshot(entries) if entries.len() == 1));

        let renewed = handle
            .renew(
                &space,
                RenewRequest {
                    device: dev(1),
                    leases: vec![lease.id],
                },
            )
            .await
            .unwrap();
        assert_eq!(renewed.granted.len(), 1);
        assert!(renewed.invalid.is_empty());
        assert!(!renewed.granted[0].contended, "nobody is waiting");

        handle
            .release(
                &space,
                ReleaseRequest {
                    device: dev(1),
                    leases: vec![lease.id],
                },
            )
            .await
            .unwrap();
        handle
            .acquire(
                &space,
                AcquireRequest {
                    device: dev(2),
                    specs: vec![wspec(&db)],
                    steal: false,
                },
            )
            .await
            .expect("released prefix must be acquirable by another device");
    }

    /// The same key in two spaces holds two independent values, and the
    /// two spaces' admission sequences never couple.
    pub async fn cross_space_isolation<T: ServerHandle>(handle: &T, a: SpaceId, b: SpaceId) {
        let k = key(&[b"db", b"k"]);
        for (space, marker) in [(a, &b"from-a"[..]), (b, &b"from-b"[..])] {
            let got = handle
                .get(
                    &space,
                    GetRequest {
                        keys: vec![k.clone()],
                    },
                )
                .await
                .unwrap();
            assert_eq!(
                got.entries[0].as_ref().map(|e| &e.value),
                Some(&Value::Present(marker.to_vec())),
                "space {space:?} must see only its own write"
            );
        }
    }

    /// Kernel rejections must arrive on the kernel plane (`Kernel(..)`),
    /// never smeared into transport errors — the split the client retry
    /// contract is built on.
    pub async fn kernel_rejections_stay_on_the_kernel_plane<T: ServerHandle>(
        handle: &T,
        space: SpaceId,
    ) {
        let x = key(&[b"x"]);
        handle
            .acquire(
                &space,
                AcquireRequest {
                    device: dev(1),
                    specs: vec![wspec(&x)],
                    steal: false,
                },
            )
            .await
            .unwrap();

        let contended = handle
            .acquire(
                &space,
                AcquireRequest {
                    device: dev(2),
                    specs: vec![wspec(&x)],
                    steal: false,
                },
            )
            .await
            .unwrap_err();
        assert!(
            matches!(contended, SpaceError::Kernel(KernelError::Contended { .. })),
            "contention is a kernel decision, got {contended:?}"
        );

        let uncovered = handle
            .put_batch(
                &space,
                PutBatchRequest {
                    device: dev(2),
                    device_seq: DeviceSeq(1),
                    leases: vec![],
                    entries: vec![PutEntry {
                        key: key(&[b"x", b"k"]),
                        value: Value::Present(b"v".to_vec()),
                        ver: Ver(1),
                    }],
                },
            )
            .await
            .unwrap_err();
        assert!(
            matches!(
                uncovered,
                SpaceError::Kernel(KernelError::NotCovered { .. })
            ),
            "coverage refusal is a kernel decision, got {uncovered:?}"
        );
    }

    /// Every verb against an unserved space fails on the transport plane
    /// (`Unavailable`, or `Unauthorized` once tokens gate the wire) —
    /// never a kernel-semantic answer about a space that isn't there.
    pub async fn unserved_space_is_a_transport_error<T: ServerHandle>(
        handle: &T,
        unknown: SpaceId,
    ) {
        fn is_transport(err: &SpaceError) -> bool {
            matches!(
                err,
                SpaceError::Unavailable { .. }
                    | SpaceError::Kernel(KernelError::Unauthorized { .. })
            )
        }
        let db = key(&[b"db"]);

        let err = handle
            .acquire(
                &unknown,
                AcquireRequest {
                    device: dev(1),
                    specs: vec![wspec(&db)],
                    steal: false,
                },
            )
            .await
            .unwrap_err();
        assert!(is_transport(&err), "acquire: {err:?}");
        let err = handle
            .renew(
                &unknown,
                RenewRequest {
                    device: dev(1),
                    leases: vec![],
                },
            )
            .await
            .unwrap_err();
        assert!(is_transport(&err), "renew: {err:?}");
        let err = handle
            .release(
                &unknown,
                ReleaseRequest {
                    device: dev(1),
                    leases: vec![],
                },
            )
            .await
            .unwrap_err();
        assert!(is_transport(&err), "release: {err:?}");
        let err = handle
            .put_batch(
                &unknown,
                PutBatchRequest {
                    device: dev(1),
                    device_seq: DeviceSeq(1),
                    leases: vec![],
                    entries: vec![],
                },
            )
            .await
            .unwrap_err();
        assert!(is_transport(&err), "put_batch: {err:?}");
        let err = handle
            .get(&unknown, GetRequest { keys: vec![] })
            .await
            .unwrap_err();
        assert!(is_transport(&err), "get: {err:?}");
        let err = handle
            .list(
                &unknown,
                ListRequest {
                    prefix: db.clone(),
                    start_after: None,
                    limit: None,
                },
            )
            .await
            .unwrap_err();
        assert!(is_transport(&err), "list: {err:?}");
        let err = handle
            .read_at(&unknown, ReadAtRequest { ranges: vec![] })
            .await
            .unwrap_err();
        assert!(is_transport(&err), "read_at: {err:?}");
    }
}
