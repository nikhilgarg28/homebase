//! Layer 3 shard lifecycle: fault object store + slatedb open / power-loss reopen.

#[cfg(feature = "slatedb")]
mod imp {
    use crate::fault_object_store::FaultObjectStore;
    use crate::fault_slate::FaultSlateStore;
    use crate::store::FaultConfig;
    use homestead_server::storage::{SlateOpenOptions, SlateStore};
    use std::sync::Arc;
    use tempfile::TempDir;

    const DB_NAME: &str = "shard";

    /// One temp shard directory with fault injection and crash/reopen support.
    pub struct SlateShard {
        _dir: TempDir,
        fault_store: Arc<FaultObjectStore>,
        store: Option<Arc<FaultSlateStore>>,
        store_seed: u64,
        faults: FaultConfig,
    }

    impl SlateShard {
        pub async fn new(seed: u64, faults: FaultConfig) -> Self {
            let dir = tempfile::tempdir().expect("temp shard dir");
            let fault_store = Arc::new(
                FaultObjectStore::new(dir.path(), seed, faults).expect("fault object store"),
            );
            let store = Some(
                Self::open_store(
                    Arc::clone(&fault_store),
                    seed.wrapping_add(1),
                )
                .await,
            );
            Self {
                _dir: dir,
                fault_store,
                store,
                store_seed: seed.wrapping_add(1),
                faults,
            }
        }

        pub fn store(&self) -> Arc<FaultSlateStore> {
            Arc::clone(self.store.as_ref().expect("slate shard closed"))
        }

        pub fn set_faults(&mut self, faults: FaultConfig) {
            self.faults = faults;
            use crate::crash::{SLATE_OS_FAULTS, SLATE_STORE_FAULTS};
            self.fault_store.set_config(SLATE_OS_FAULTS);
            if let Some(store) = &self.store {
                store.set_config(SLATE_STORE_FAULTS);
            }
        }

        pub fn disable_faults(&mut self) {
            self.set_faults(FaultConfig::NONE);
        }

        /// Simulated power loss: close db, roll object store back, reopen.
        pub async fn power_loss(&mut self) {
            if let Some(store) = self.store.take() {
                let _ = FaultSlateStore::shutdown(store).await;
            }
            self.fault_store
                .simulate_power_loss()
                .expect("power loss restore");
            self.store = Some(
                Self::open_store(Arc::clone(&self.fault_store), self.store_seed).await,
            );
        }

        async fn open_store(
            fault_store: Arc<FaultObjectStore>,
            seed: u64,
        ) -> Arc<FaultSlateStore> {
            use crate::crash::{SLATE_OS_FAULTS, SLATE_STORE_FAULTS};
            fault_store.set_config(SLATE_OS_FAULTS);
            let object_store = fault_store.as_arc();
            let on_durable = {
                let fs = Arc::clone(&fault_store);
                Arc::new(move || fs.checkpoint().expect("object store checkpoint"))
            };
            let slate = Arc::new(
                SlateStore::open(DB_NAME, object_store, SlateOpenOptions::default())
                    .await
                    .expect("open slatedb"),
            );
            Arc::new(FaultSlateStore::new(
                slate,
                seed,
                SLATE_STORE_FAULTS,
                on_durable,
            ))
        }
    }
}

#[cfg(feature = "slatedb")]
pub use imp::SlateShard;
