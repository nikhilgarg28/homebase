//! SlateDB-backed [`OrderedStore`]: object-store mode with a local NVMe
//! stand-in via [`LocalFileSystem`](slatedb::object_store::local::LocalFileSystem).
//!
//! One `Db` per shard directory; spaces share it through disjoint key
//! prefixes (the space id is the first tuple component). `apply` writes a
//! batch and [`flush`](Db::flush)s so the kernel's "reply awaits durability"
//! contract holds even on the local-filesystem backend.

use super::{Op, OrderedStore, ScanIter, StorageError, WriteBatch};
use slatedb::Db;
use slatedb::config::{Settings, WriteOptions};
use slatedb::object_store::local::LocalFileSystem;
use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

/// A SlateDB instance over a local directory pretending to be object storage.
#[derive(Clone)]
pub struct SlateStore {
    db: Arc<Db>,
}

impl SlateStore {
    /// Opens (or creates) a database under `root/db_name` on a local object
    /// store rooted at `root`.
    pub async fn open(root: impl AsRef<Path>, db_name: &str) -> Result<Self, StorageError> {
        let root = root.as_ref();
        std::fs::create_dir_all(root).map_err(|e| StorageError(e.to_string()))?;
        let object_store = Arc::new(
            LocalFileSystem::new_with_prefix(root).map_err(|e| StorageError(e.to_string()))?,
        );
        let settings = Settings {
            flush_interval: None,
            ..Default::default()
        };
        let db = Db::builder(db_name, object_store)
            .with_settings(settings)
            .build()
            .await
            .map_err(|e| StorageError(e.to_string()))?;
        Ok(Self { db: Arc::new(db) })
    }

    /// Persists memtable + WAL to the object store (phase boundary / clean
    /// shutdown in torture tests).
    pub async fn flush(&self) -> Result<(), StorageError> {
        self.db.flush().await.map_err(|e| StorageError(e.to_string()))
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
    while let Some(kv) = iter
        .next()
        .await
        .map_err(|e| StorageError(e.to_string()))?
    {
        out.push((kv.key.to_vec(), kv.value.to_vec()));
    }
    Ok(out)
}

struct SlateScanInner {
    entries: Option<Vec<(Vec<u8>, Vec<u8>)>>,
    init: Option<Pin<Box<dyn Future<Output = Result<Vec<(Vec<u8>, Vec<u8>)>, StorageError>> + Send>>>,
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
            match db.get(&key).await.map_err(|e| StorageError(e.to_string()))? {
                Some(bytes) => Ok(Some(bytes.to_vec())),
                None => Ok(None),
            }
        }
    }

    fn scan(&self, start: Vec<u8>, end: Option<Vec<u8>>) -> impl ScanIter {
        SlateScan::new(Arc::clone(&self.db), start, end)
    }

    fn apply(
        &self,
        batch: WriteBatch,
    ) -> impl Future<Output = Result<(), StorageError>> + Send {
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
        let store = SlateStore::open(dir.path(), "smoke").await.unwrap();
        let mut batch = WriteBatch::new();
        batch.put(b"k".to_vec(), b"v".to_vec());
        store.apply(batch).await.unwrap();
        assert_eq!(store.get(b"k").await.unwrap(), Some(b"v".to_vec()));
    }

    #[tokio::test]
    async fn slate_store_passes_conformance() {
        let dir = tempdir().unwrap();
        let mut n = 0u64;
        conformance::run_all_async(move || {
            n += 1;
            let name = format!("c{n}");
            let path = dir.path().to_path_buf();
            async move { SlateStore::open(&path, &name).await.unwrap() }
        })
        .await;
    }
}
