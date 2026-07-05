//! Seeded fault injection at the object-store boundary (Layer 3).
//!
//! Wraps [`LocalFileSystem`](slatedb::object_store::local::LocalFileSystem):
//! operations can fail or yield before touching the inner store. A
//! [`checkpoint`](FaultObjectStore::checkpoint) / [`simulate_power_loss`](FaultObjectStore::simulate_power_loss)
//! pair models the durability boundary — power loss restores the object
//! store to the last checkpoint (taken after slatedb `flush`).

#[cfg(feature = "slatedb")]
mod imp {
    use async_trait::async_trait;
    use crate::store::FaultConfig;
    use futures_util::stream::BoxStream;
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};
    use slatedb::bytes::Bytes;
    use slatedb::object_store::{
        CopyOptions, Error, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
        ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions, Result,
        local::LocalFileSystem,
        path::Path,
    };
    use std::fmt;
    use std::ops::Range;
    use std::path::{Path as StdPath, PathBuf};
    use std::sync::{Arc, Mutex};

    #[derive(Debug)]
    struct State {
        config: FaultConfig,
        rng: StdRng,
    }

    /// Local object store with seeded IO faults and checkpointed crash recovery.
    #[derive(Debug)]
    pub struct FaultObjectStore {
        inner: Arc<LocalFileSystem>,
        root: PathBuf,
        checkpoint: PathBuf,
        state: Mutex<State>,
    }

    impl FaultObjectStore {
        pub fn new(
            root: impl AsRef<StdPath>,
            seed: u64,
            config: FaultConfig,
        ) -> std::io::Result<Self> {
            let root = root.as_ref().to_path_buf();
            std::fs::create_dir_all(&root)?;
            let checkpoint = root.with_extension("checkpoint");
            let inner = LocalFileSystem::new_with_prefix(&root)
                .map_err(|e| std::io::Error::other(e.to_string()))?;
            Ok(Self {
                inner: Arc::new(inner),
                root,
                checkpoint,
                state: Mutex::new(State {
                    config,
                    rng: StdRng::seed_from_u64(seed),
                }),
            })
        }

        pub fn set_config(&self, config: FaultConfig) {
            self.state.lock().unwrap().config = config;
        }

        /// Snapshot durable object-store bytes (post-`SlateStore::flush`).
        pub fn checkpoint(&self) -> std::io::Result<()> {
            if self.checkpoint.exists() {
                remove_dir_all(&self.checkpoint)?;
            }
            copy_dir_all(&self.root, &self.checkpoint)
        }

        /// Power loss: roll the object store back to the last checkpoint.
        pub fn simulate_power_loss(&self) -> std::io::Result<()> {
            if !self.checkpoint.exists() {
                return Ok(());
            }
            remove_dir_all(&self.root)?;
            copy_dir_all(&self.checkpoint, &self.root)
        }

        pub fn as_arc(self: &Arc<Self>) -> Arc<dyn ObjectStore> {
            Arc::clone(self) as Arc<dyn ObjectStore>
        }

        fn draw_error(&self) -> bool {
            let mut s = self.state.lock().unwrap();
            let rate = s.config.error_rate;
            rate > 0.0 && s.rng.random_bool(rate)
        }

        async fn inject_delay(&self) {
            let yields = {
                let mut s = self.state.lock().unwrap();
                let max = s.config.max_latency_yields;
                if max == 0 {
                    0
                } else {
                    s.rng.random_range(0..=max)
                }
            };
            for _ in 0..yields {
                tokio::task::yield_now().await;
            }
        }

        fn io_err(op: &str) -> Error {
            Error::Generic {
                store: "FaultObjectStore",
                source: op.into(),
            }
        }
    }

    impl fmt::Display for FaultObjectStore {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "FaultObjectStore({})", self.inner)
        }
    }

    #[async_trait]
    impl ObjectStore for FaultObjectStore {
        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            opts: PutOptions,
        ) -> Result<PutResult> {
            self.inject_delay().await;
            if self.draw_error() {
                return Err(Self::io_err("put"));
            }
            self.inner.put_opts(location, payload, opts).await
        }

        async fn put_multipart_opts(
            &self,
            location: &Path,
            opts: PutMultipartOptions,
        ) -> Result<Box<dyn MultipartUpload>> {
            self.inject_delay().await;
            if self.draw_error() {
                return Err(Self::io_err("put_multipart"));
            }
            self.inner.put_multipart_opts(location, opts).await
        }

        async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
            self.inject_delay().await;
            if self.draw_error() {
                return Err(Self::io_err("get"));
            }
            self.inner.get_opts(location, options).await
        }

        async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> Result<Vec<Bytes>> {
            self.inject_delay().await;
            if self.draw_error() {
                return Err(Self::io_err("get_ranges"));
            }
            self.inner.get_ranges(location, ranges).await
        }

        fn delete_stream(
            &self,
            locations: BoxStream<'static, Result<Path>>,
        ) -> BoxStream<'static, Result<Path>> {
            self.inner.delete_stream(locations)
        }

        fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
            self.inner.list(prefix)
        }

        fn list_with_offset(
            &self,
            prefix: Option<&Path>,
            offset: &Path,
        ) -> BoxStream<'static, Result<ObjectMeta>> {
            self.inner.list_with_offset(prefix, offset)
        }

        async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
            self.inject_delay().await;
            if self.draw_error() {
                return Err(Self::io_err("list"));
            }
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
            self.inject_delay().await;
            if self.draw_error() {
                return Err(Self::io_err("copy"));
            }
            self.inner.copy_opts(from, to, options).await
        }

        async fn rename_opts(&self, from: &Path, to: &Path, options: RenameOptions) -> Result<()> {
            self.inject_delay().await;
            if self.draw_error() {
                return Err(Self::io_err("rename"));
            }
            self.inner.rename_opts(from, to, options).await
        }
    }

    fn copy_dir_all(src: &StdPath, dst: &StdPath) -> std::io::Result<()> {
        std::fs::create_dir_all(dst)?;
        for entry in std::fs::read_dir(src)? {
            let entry = entry?;
            let ty = entry.file_type()?;
            let dest = dst.join(entry.file_name());
            if ty.is_dir() {
                copy_dir_all(&entry.path(), &dest)?;
            } else {
                std::fs::copy(entry.path(), dest)?;
            }
        }
        Ok(())
    }

    fn remove_dir_all(path: &StdPath) -> std::io::Result<()> {
        if path.exists() {
            std::fs::remove_dir_all(path)?;
        }
        Ok(())
    }
}

#[cfg(feature = "slatedb")]
pub use imp::FaultObjectStore;
