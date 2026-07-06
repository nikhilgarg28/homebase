//! The ordered-map storage abstraction both sides of the wire build on.
//!
//! The server's kernel reads and writes one ordered byte-keyed map per
//! shard through [`OrderedStore`] (slatedb in production — behind the
//! server crate's `slatedb` feature — local-NVMe object store for dev, S3
//! for prod). The client persists its own durable state — device identity,
//! per-space seq/oplog/watermarks/leases — through the *same* trait, so one
//! storage vocabulary serves both halves and the sim's fault-injecting
//! store can torture either side. The trait is async and fallible because
//! production backends do real IO that can fail. [`MemoryStore`] backs
//! tests and the deterministic sim — its futures resolve immediately and
//! never fail, so determinism is preserved: operations execute serially,
//! and no await point can interleave with another.
//!
//! Scans stream: [`OrderedStore::scan_prefix`] hands back a [`ScanIter`],
//! a pull-based async iterator, so a large range never has to materialize.
//!
//! Every logical operation applies exactly one atomic [`WriteBatch`].
//!
//! All methods take `&self`: one store is shared by every space actor on a
//! shard, and by every replica in a client (slatedb is naturally `&self`;
//! [`MemoryStore`] locks internally). Atomicity is the [`WriteBatch`]
//! contract, not `&mut` exclusivity — per-space serialization comes from
//! the space actor, and spaces never share keys.

use std::collections::BTreeMap;
use std::fmt;
use std::future::Future;
use std::ops::Bound;
use std::sync::RwLock;

/// A storage-backend failure (object store IO, corruption, …). Distinct from
/// [`KernelError`](crate::messages::KernelError): kernel errors are
/// semantic rejections, storage errors are infrastructure faults.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StorageError(pub String);

impl fmt::Display for StorageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "storage error: {}", self.0)
    }
}

impl std::error::Error for StorageError {}

/// A single mutation within a batch.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Op {
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
}

/// An atomic set of mutations. Applied in order; all-or-nothing.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct WriteBatch {
    pub ops: Vec<Op>,
}

impl WriteBatch {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn put(&mut self, key: Vec<u8>, value: Vec<u8>) {
        self.ops.push(Op::Put { key, value });
    }

    pub fn delete(&mut self, key: Vec<u8>) {
        self.ops.push(Op::Delete { key });
    }

    pub fn is_empty(&self) -> bool {
        self.ops.is_empty()
    }
}

/// A pull-based async iterator over scan results, in key order — the same
/// shape as slatedb's `DbIterator`, where each step may touch the object
/// store. Backends whose scan *setup* is async or fallible defer it to the
/// first `next` call; that is why [`OrderedStore::scan_prefix`] itself is
/// synchronous and infallible.
pub trait ScanIter: Send {
    /// The next entry, or `None` once the scan is exhausted.
    fn next(
        &mut self,
    ) -> impl Future<Output = Result<Option<(Vec<u8>, Vec<u8>)>, StorageError>> + Send;
}

/// Drains a scan into a `Vec`. For tests and scans known to be small.
pub async fn collect_scan<I: ScanIter>(
    mut iter: I,
) -> Result<Vec<(Vec<u8>, Vec<u8>)>, StorageError> {
    let mut out = Vec::new();
    while let Some(entry) = iter.next().await? {
        out.push(entry);
    }
    Ok(out)
}

/// An ordered byte-keyed map with range scans and atomic batches.
pub trait OrderedStore {
    fn get(&self, key: &[u8])
    -> impl Future<Output = Result<Option<Vec<u8>>, StorageError>> + Send;

    /// Streams entries with keys in `[start, end)` — `end: None` means
    /// unbounded — in ascending key order. The scan observes the store as of
    /// its creation; whether batches applied *after* creation are visible is
    /// backend-defined (callers must not rely on it — the kernel never
    /// interleaves a scan with its own writes).
    ///
    /// `start > end` is caller error; implementations may panic.
    fn scan(&self, start: Vec<u8>, end: Option<Vec<u8>>) -> impl ScanIter;

    /// Streams all entries whose key starts with `prefix`, in key order.
    fn scan_prefix(&self, prefix: &[u8]) -> impl ScanIter {
        self.scan(prefix.to_vec(), prefix_successor(prefix))
    }

    /// Applies a batch atomically.
    fn apply(&self, batch: WriteBatch) -> impl Future<Output = Result<(), StorageError>> + Send;
}

/// A shared reference to a store is a store — every method already takes
/// `&self`. This is what lets one store back several views at once (a
/// crash/resume test running two incarnations over one `MemoryStore`).
impl<S: OrderedStore> OrderedStore for &S {
    fn get(
        &self,
        key: &[u8],
    ) -> impl Future<Output = Result<Option<Vec<u8>>, StorageError>> + Send {
        (**self).get(key)
    }

    fn scan(&self, start: Vec<u8>, end: Option<Vec<u8>>) -> impl ScanIter {
        (**self).scan(start, end)
    }

    fn apply(&self, batch: WriteBatch) -> impl Future<Output = Result<(), StorageError>> + Send {
        (**self).apply(batch)
    }
}

/// The smallest byte string strictly greater than every string starting with
/// `prefix`, or `None` when no such bound exists (all-0xFF prefix).
pub fn prefix_successor(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut out = prefix.to_vec();
    while let Some(last) = out.last_mut() {
        if *last < 0xff {
            *last += 1;
            return Some(out);
        }
        out.pop();
    }
    None
}

/// In-memory store: the reference implementation for tests and the sim.
///
/// Interior locking makes it shareable (`&self` everywhere) like the
/// production store. Scans snapshot their range at creation — the cheapest
/// way to satisfy the "observes the store as of creation" contract through
/// a lock, and fine at test scale.
#[derive(Debug, Default)]
pub struct MemoryStore {
    map: RwLock<BTreeMap<Vec<u8>, Vec<u8>>>,
}

impl MemoryStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn len(&self) -> usize {
        self.map.read().unwrap().len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.read().unwrap().is_empty()
    }
}

/// Scan over a [`MemoryStore`]: a range snapshot wrapped in ready futures.
struct MemoryScan {
    inner: std::vec::IntoIter<(Vec<u8>, Vec<u8>)>,
}

impl ScanIter for MemoryScan {
    fn next(
        &mut self,
    ) -> impl Future<Output = Result<Option<(Vec<u8>, Vec<u8>)>, StorageError>> + Send {
        std::future::ready(Ok(self.inner.next()))
    }
}

impl OrderedStore for MemoryStore {
    fn get(
        &self,
        key: &[u8],
    ) -> impl Future<Output = Result<Option<Vec<u8>>, StorageError>> + Send {
        std::future::ready(Ok(self.map.read().unwrap().get(key).cloned()))
    }

    fn scan(&self, start: Vec<u8>, end: Option<Vec<u8>>) -> impl ScanIter {
        let end = match end {
            Some(end) => Bound::Excluded(end),
            None => Bound::Unbounded,
        };
        let snapshot: Vec<(Vec<u8>, Vec<u8>)> = self
            .map
            .read()
            .unwrap()
            .range((Bound::Included(start), end))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        MemoryScan {
            inner: snapshot.into_iter(),
        }
    }

    fn apply(&self, batch: WriteBatch) -> impl Future<Output = Result<(), StorageError>> + Send {
        let mut map = self.map.write().unwrap();
        for op in batch.ops {
            match op {
                Op::Put { key, value } => {
                    map.insert(key, value);
                }
                Op::Delete { key } => {
                    map.remove(&key);
                }
            }
        }
        std::future::ready(Ok(()))
    }
}

/// A reusable conformance suite for [`OrderedStore`] implementations.
///
/// Every check is a generic async fn that takes a **fresh, empty** store and
/// panics on contract violation; [`run_all`](conformance::run_all) drives the
/// whole suite from a store factory. New backends get their coverage in one
/// line under whatever executor they need:
///
/// ```ignore
/// block_on(conformance::run_all(MemoryStore::new));          // tests below
/// runtime.block_on(conformance::run_all(|| slate_store()));  // future slatedb impl
/// ```
pub mod conformance {
    use super::*;

    /// Runs every conformance check, each against a fresh store.
    pub async fn run_all<S: OrderedStore>(mut fresh: impl FnMut() -> S) {
        empty_store_reads_nothing(fresh()).await;
        put_then_get(fresh()).await;
        overwrite_replaces(fresh()).await;
        delete_removes_and_tolerates_missing(fresh()).await;
        batch_ops_apply_in_order(fresh()).await;
        empty_batch_is_a_noop(fresh()).await;
        scan_filters_and_orders(fresh()).await;
        scan_range_bounds_are_half_open(fresh()).await;
        scan_boundaries_are_exact(fresh()).await;
        scan_handles_high_prefixes(fresh()).await;
        scan_observes_all_prior_batches(fresh()).await;
    }

    /// Like [`run_all`], but the factory is async — for backends (slatedb)
    /// that need a tokio runtime even to open.
    pub async fn run_all_async<S: OrderedStore, F, Fut>(mut fresh: F)
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = S>,
    {
        empty_store_reads_nothing(fresh().await).await;
        put_then_get(fresh().await).await;
        overwrite_replaces(fresh().await).await;
        delete_removes_and_tolerates_missing(fresh().await).await;
        batch_ops_apply_in_order(fresh().await).await;
        empty_batch_is_a_noop(fresh().await).await;
        scan_filters_and_orders(fresh().await).await;
        scan_range_bounds_are_half_open(fresh().await).await;
        scan_boundaries_are_exact(fresh().await).await;
        scan_handles_high_prefixes(fresh().await).await;
        scan_observes_all_prior_batches(fresh().await).await;
    }

    async fn scan_all<S: OrderedStore>(store: &S, prefix: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)> {
        collect_scan(store.scan_prefix(prefix)).await.unwrap()
    }

    async fn put_all<S: OrderedStore>(store: &S, entries: &[(&[u8], &[u8])]) {
        let mut batch = WriteBatch::new();
        for (k, v) in entries {
            batch.put(k.to_vec(), v.to_vec());
        }
        store.apply(batch).await.unwrap();
    }

    pub async fn empty_store_reads_nothing<S: OrderedStore>(store: S) {
        assert_eq!(store.get(b"anything").await.unwrap(), None);
        assert!(scan_all(&store, &[]).await.is_empty());
        assert!(scan_all(&store, b"pfx").await.is_empty());
    }

    pub async fn put_then_get<S: OrderedStore>(store: S) {
        put_all(&store, &[(b"k1", b"v1"), (b"k2", b"v2")]).await;
        assert_eq!(store.get(b"k1").await.unwrap(), Some(b"v1".to_vec()));
        assert_eq!(store.get(b"k2").await.unwrap(), Some(b"v2".to_vec()));
        assert_eq!(store.get(b"k3").await.unwrap(), None);
    }

    pub async fn overwrite_replaces<S: OrderedStore>(store: S) {
        put_all(&store, &[(b"k", b"old")]).await;
        put_all(&store, &[(b"k", b"new")]).await;
        assert_eq!(store.get(b"k").await.unwrap(), Some(b"new".to_vec()));
        assert_eq!(
            scan_all(&store, b"k").await.len(),
            1,
            "overwrite must not duplicate"
        );
    }

    pub async fn delete_removes_and_tolerates_missing<S: OrderedStore>(store: S) {
        put_all(&store, &[(b"k", b"v")]).await;

        let mut batch = WriteBatch::new();
        batch.delete(b"k".to_vec());
        batch.delete(b"never-existed".to_vec());
        store.apply(batch).await.unwrap();

        assert_eq!(store.get(b"k").await.unwrap(), None);
        assert!(scan_all(&store, &[]).await.is_empty());
    }

    pub async fn batch_ops_apply_in_order<S: OrderedStore>(store: S) {
        // Last op on a key wins: put-delete-put …
        let mut batch = WriteBatch::new();
        batch.put(b"a".to_vec(), b"v1".to_vec());
        batch.delete(b"a".to_vec());
        batch.put(b"a".to_vec(), b"v2".to_vec());
        // … and put-then-delete.
        batch.put(b"b".to_vec(), b"v".to_vec());
        batch.delete(b"b".to_vec());
        store.apply(batch).await.unwrap();

        assert_eq!(store.get(b"a").await.unwrap(), Some(b"v2".to_vec()));
        assert_eq!(store.get(b"b").await.unwrap(), None);
    }

    pub async fn empty_batch_is_a_noop<S: OrderedStore>(store: S) {
        put_all(&store, &[(b"k", b"v")]).await;
        store.apply(WriteBatch::new()).await.unwrap();
        assert_eq!(store.get(b"k").await.unwrap(), Some(b"v".to_vec()));
    }

    pub async fn scan_filters_and_orders<S: OrderedStore>(store: S) {
        // Inserted deliberately out of order, across two batches.
        put_all(&store, &[(b"b/2", b"v3"), (b"a", b"v0"), (b"c", b"v5")]).await;
        put_all(&store, &[(b"b/1", b"v2"), (b"b", b"v1"), (b"b/3", b"v4")]).await;

        let hits = scan_all(&store, b"b").await;
        let expected: Vec<(Vec<u8>, Vec<u8>)> = [
            (b"b" as &[u8], b"v1" as &[u8]),
            (b"b/1", b"v2"),
            (b"b/2", b"v3"),
            (b"b/3", b"v4"),
        ]
        .iter()
        .map(|(k, v)| (k.to_vec(), v.to_vec()))
        .collect();
        assert_eq!(hits, expected, "prefix filter + ascending key order");

        let all = scan_all(&store, &[]).await;
        assert_eq!(all.len(), 6);
        assert!(
            all.windows(2).all(|w| w[0].0 < w[1].0),
            "empty prefix scans everything in order"
        );
    }

    pub async fn scan_range_bounds_are_half_open<S: OrderedStore>(store: S) {
        put_all(
            &store,
            &[(b"a", b"1"), (b"b", b"2"), (b"c", b"3"), (b"d", b"4")],
        )
        .await;

        let collect = |iter| async { collect_scan(iter).await.unwrap() };
        let keys = |entries: Vec<(Vec<u8>, Vec<u8>)>| {
            entries.into_iter().map(|(k, _)| k).collect::<Vec<_>>()
        };

        // [b, d): start inclusive, end exclusive.
        let hits = keys(collect(store.scan(b"b".to_vec(), Some(b"d".to_vec()))).await);
        assert_eq!(hits, vec![b"b".to_vec(), b"c".to_vec()]);

        // Unbounded end runs to the last key.
        let hits = keys(collect(store.scan(b"c".to_vec(), None)).await);
        assert_eq!(hits, vec![b"c".to_vec(), b"d".to_vec()]);

        // Start needn't be an existing key; empty ranges are empty.
        let hits = keys(collect(store.scan(b"aa".to_vec(), Some(b"b".to_vec()))).await);
        assert!(hits.is_empty());
        let hits = keys(collect(store.scan(b"b".to_vec(), Some(b"b".to_vec()))).await);
        assert!(hits.is_empty());
    }

    pub async fn scan_boundaries_are_exact<S: OrderedStore>(store: S) {
        put_all(
            &store,
            &[
                (&[0x00u8] as &[u8], b"below" as &[u8]),
                (&[0x01], b"exact"),
                (&[0x01, 0x00], b"low-child"),
                (&[0x01, 0xff], b"high-child"),
                (&[0x01, 0xff, 0xff], b"high-grandchild"),
                (&[0x02], b"above"),
            ],
        )
        .await;

        let hits: Vec<Vec<u8>> = scan_all(&store, &[0x01])
            .await
            .into_iter()
            .map(|(k, _)| k)
            .collect();
        assert_eq!(
            hits,
            vec![
                vec![0x01],
                vec![0x01, 0x00],
                vec![0x01, 0xff],
                vec![0x01, 0xff, 0xff],
            ],
            "neighbors on both sides excluded, 0xff children included"
        );
    }

    pub async fn scan_handles_high_prefixes<S: OrderedStore>(store: S) {
        // An all-0xff prefix has no successor: the scan must run unbounded.
        put_all(
            &store,
            &[
                (&[0xfeu8, 0xff] as &[u8], b"below" as &[u8]),
                (&[0xff], b"a"),
                (&[0xff, 0x00], b"b"),
                (&[0xff, 0xff], b"c"),
                (&[0xff, 0xff, 0x01], b"d"),
            ],
        )
        .await;

        assert_eq!(scan_all(&store, &[0xff]).await.len(), 4);
        assert_eq!(scan_all(&store, &[0xff, 0xff]).await.len(), 2);
    }

    pub async fn scan_observes_all_prior_batches<S: OrderedStore>(store: S) {
        put_all(&store, &[(b"k1", b"v1"), (b"k2", b"v2")]).await;

        let mut batch = WriteBatch::new();
        batch.delete(b"k1".to_vec());
        batch.put(b"k3".to_vec(), b"v3".to_vec());
        store.apply(batch).await.unwrap();

        let keys: Vec<Vec<u8>> = scan_all(&store, b"k")
            .await
            .into_iter()
            .map(|(k, _)| k)
            .collect();
        assert_eq!(keys, vec![b"k2".to_vec(), b"k3".to_vec()]);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pollster::block_on;

    #[test]
    fn prefix_successor_handles_carries() {
        assert_eq!(prefix_successor(b"ab"), Some(b"ac".to_vec()));
        assert_eq!(prefix_successor(&[0x01, 0xff]), Some(vec![0x02]));
        assert_eq!(prefix_successor(&[0xff, 0xff]), None);
        assert_eq!(prefix_successor(&[]), None);
    }

    #[test]
    fn memory_store_passes_conformance() {
        block_on(conformance::run_all(MemoryStore::new));
    }
}
