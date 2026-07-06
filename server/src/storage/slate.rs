//! SlateDB-backed [`OrderedStore`]: always object-store mode (`Db::builder`).
//!
//! Production uses S3 (or similar); dev/self-host and the sim pass a
//! [`LocalFileSystem`](slatedb::object_store::local::LocalFileSystem) or a
//! fault-injecting wrapper via [`local_object_store`] / custom impls.
//!
//! One `Db` per shard; spaces share it through disjoint key prefixes (the
//! space id is the first tuple component).

use super::{Op, OrderedStore, ScanIter, StorageError, WriteBatch};
use slatedb::Db;
use slatedb::config::{ObjectStoreCacheOptions, Settings, WriteOptions};
use slatedb::object_store::ObjectStore;
use slatedb::object_store::local::LocalFileSystem;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

/// Optional open-time tuning for [`SlateStore::open`].
#[derive(Clone, Debug, Default)]
pub struct SlateOpenOptions {
    /// Local disk cache in front of object-store reads. `None` leaves the
    /// cache disabled (slatedb default).
    pub object_store_cache_dir: Option<PathBuf>,
}

impl SlateOpenOptions {
    pub fn object_store_cache_dir(mut self, path: impl Into<PathBuf>) -> Self {
        self.object_store_cache_dir = Some(path.into());
        self
    }
}

/// Local NVMe / dev stand-in: an object store rooted at `root`.
pub fn local_object_store(root: impl AsRef<Path>) -> Result<Arc<LocalFileSystem>, StorageError> {
    let root = root.as_ref();
    std::fs::create_dir_all(root).map_err(|e| StorageError(e.to_string()))?;
    LocalFileSystem::new_with_prefix(root)
        .map(Arc::new)
        .map_err(|e| StorageError(e.to_string()))
}

/// A SlateDB shard store.
#[derive(Clone)]
pub struct SlateStore {
    db: Arc<Db>,
}

impl SlateStore {
    /// Opens (or creates) a database on `object_store`.
    ///
    /// `db_name` is the slate path prefix (shard identity). Pass
    /// [`local_object_store`] for dev, or any `Arc<dyn ObjectStore>` for prod.
    pub async fn open(
        db_name: impl AsRef<str>,
        object_store: Arc<dyn ObjectStore>,
        options: SlateOpenOptions,
    ) -> Result<Self, StorageError> {
        let mut object_store_cache_options = ObjectStoreCacheOptions::default();
        object_store_cache_options.root_folder = options.object_store_cache_dir;

        let settings = Settings {
            flush_interval: None,
            object_store_cache_options,
            ..Default::default()
        };
        let db = Db::builder(db_name.as_ref(), object_store)
            .with_settings(settings)
            .build()
            .await
            .map_err(|e| StorageError(e.to_string()))?;
        Ok(Self { db: Arc::new(db) })
    }

    /// Persists memtable + WAL to the object store (phase boundary / clean
    /// shutdown in torture tests).
    pub async fn flush(&self) -> Result<(), StorageError> {
        self.db
            .flush()
            .await
            .map_err(|e| StorageError(e.to_string()))
    }

    /// Closes the database cleanly. Fails if other clones still exist.
    pub async fn close(self) -> Result<(), StorageError> {
        Arc::try_unwrap(self.db)
            .map_err(|_| StorageError("outstanding SlateStore clones".into()))?
            .close()
            .await
            .map_err(|e| StorageError(e.to_string()))
    }
}

fn to_slate_batch(batch: WriteBatch) -> slatedb::WriteBatch {
    let mut out = slatedb::WriteBatch::new();
    for op in batch.ops {
        match op {
            Op::Put { key, value } => out.put(key, value),
            Op::Delete { key } => out.delete(key),
        }
    }
    out
}

async fn collect_range(
    db: &Db,
    start: Vec<u8>,
    end: Option<Vec<u8>>,
) -> Result<Vec<(Vec<u8>, Vec<u8>)>, StorageError> {
    if let Some(ref end) = end {
        if start >= *end {
            return Ok(Vec::new());
        }
    }
    let mut iter = match end {
        Some(end) => db
            .scan(start..end)
            .await
            .map_err(|e| StorageError(e.to_string()))?,
        None => db
            .scan(start..)
            .await
            .map_err(|e| StorageError(e.to_string()))?,
    };
    let mut out = Vec::new();
    while let Some(kv) = iter.next().await.map_err(|e| StorageError(e.to_string()))? {
        out.push((kv.key.to_vec(), kv.value.to_vec()));
    }
    Ok(out)
}

struct SlateScanInner {
    entries: Option<Vec<(Vec<u8>, Vec<u8>)>>,
    init:
        Option<Pin<Box<dyn Future<Output = Result<Vec<(Vec<u8>, Vec<u8>)>, StorageError>> + Send>>>,
    position: usize,
}

/// Scan iterator: materializes the range on first `next` (trait contract).
pub struct SlateScan {
    inner: Arc<Mutex<SlateScanInner>>,
}

impl SlateScan {
    fn new(db: Arc<Db>, start: Vec<u8>, end: Option<Vec<u8>>) -> Self {
        let init = Box::pin(async move { collect_range(&db, start, end).await });
        Self {
            inner: Arc::new(Mutex::new(SlateScanInner {
                entries: None,
                init: Some(init),
                position: 0,
            })),
        }
    }
}

struct SlateScanStep {
    inner: Arc<Mutex<SlateScanInner>>,
}

impl Future for SlateScanStep {
    type Output = Result<Option<(Vec<u8>, Vec<u8>)>, StorageError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut guard = self.inner.lock().unwrap();

        if guard.entries.is_none() {
            let init = guard
                .init
                .as_mut()
                .expect("scan init future missing on first poll");
            match init.as_mut().poll(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Ok(entries)) => {
                    guard.entries = Some(entries);
                    guard.init = None;
                }
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
            }
        }

        let entries = guard.entries.as_ref().unwrap();
        if guard.position >= entries.len() {
            return Poll::Ready(Ok(None));
        }
        let entry = entries[guard.position].clone();
        guard.position += 1;
        Poll::Ready(Ok(Some(entry)))
    }
}

impl ScanIter for SlateScan {
    fn next(
        &mut self,
    ) -> impl Future<Output = Result<Option<(Vec<u8>, Vec<u8>)>, StorageError>> + Send {
        SlateScanStep {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl OrderedStore for SlateStore {
    fn get(
        &self,
        key: &[u8],
    ) -> impl Future<Output = Result<Option<Vec<u8>>, StorageError>> + Send {
        let db = Arc::clone(&self.db);
        let key = key.to_vec();
        async move {
            match db
                .get(&key)
                .await
                .map_err(|e| StorageError(e.to_string()))?
            {
                Some(bytes) => Ok(Some(bytes.to_vec())),
                None => Ok(None),
            }
        }
    }

    fn scan(&self, start: Vec<u8>, end: Option<Vec<u8>>) -> impl ScanIter {
        SlateScan::new(Arc::clone(&self.db), start, end)
    }

    fn apply(&self, batch: WriteBatch) -> impl Future<Output = Result<(), StorageError>> + Send {
        let db = Arc::clone(&self.db);
        async move {
            if batch.is_empty() {
                return Ok(());
            }
            db.write_with_options(
                to_slate_batch(batch),
                &WriteOptions {
                    // WAL-visible is enough for the kernel's linearization
                    // point; [`flush`](Self::flush) / close persist to the
                    // object store (phase boundaries, group commit).
                    await_durable: false,
                    ..WriteOptions::default()
                },
            )
            .await
            .map_err(|e| StorageError(e.to_string()))?;
            Ok(())
        }
    }
}

#[cfg(all(test, feature = "slatedb"))]
mod tests {
    use super::*;
    use crate::storage::conformance;
    use tempfile::tempdir;

    #[tokio::test]
    async fn smoke_put_get() {
        let dir = tempdir().unwrap();
        let object_store = local_object_store(dir.path()).unwrap();
        let store = SlateStore::open("smoke", object_store, SlateOpenOptions::default())
            .await
            .unwrap();
        let mut batch = WriteBatch::new();
        batch.put(b"k".to_vec(), b"v".to_vec());
        store.apply(batch).await.unwrap();
        assert_eq!(store.get(b"k").await.unwrap(), Some(b"v".to_vec()));
    }

    #[tokio::test]
    async fn slate_store_passes_conformance() {
        let dir = tempdir().unwrap();
        let root = dir.path().to_path_buf();
        let mut n = 0u64;
        conformance::run_all_async(move || {
            n += 1;
            let name = format!("c{n}");
            let root = root.clone();
            async move {
                let object_store = local_object_store(&root).unwrap();
                SlateStore::open(name, object_store, SlateOpenOptions::default())
                    .await
                    .unwrap()
            }
        })
        .await;
    }
}
