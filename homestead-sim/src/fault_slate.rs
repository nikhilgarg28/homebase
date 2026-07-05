//! [`OrderedStore`] wrapper around [`SlateStore`] with SimStore-style yields,
//! apply-time error injection, and probabilistic flush (durability boundary).

#[cfg(feature = "slatedb")]
mod imp {
    use crate::store::FaultConfig;
    use homestead_server::storage::{OrderedStore, ScanIter, SlateStore, StorageError, WriteBatch};
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};
    use std::future::Future;
    use std::sync::{Arc, Mutex};

    struct State {
        config: FaultConfig,
        rng: StdRng,
    }

    /// SlateDB store with seeded interleaving points and flush-rate durability.
    #[derive(Clone)]
    pub struct FaultSlateStore {
        inner: Arc<SlateStore>,
        on_durable: Arc<dyn Fn() + Send + Sync>,
        state: Arc<Mutex<State>>,
    }

    impl FaultSlateStore {
        pub fn new(
            inner: Arc<SlateStore>,
            seed: u64,
            config: FaultConfig,
            on_durable: Arc<dyn Fn() + Send + Sync>,
        ) -> Self {
            Self {
                inner,
                on_durable,
                state: Arc::new(Mutex::new(State {
                    config,
                    rng: StdRng::seed_from_u64(seed),
                })),
            }
        }

        pub fn set_config(&self, config: FaultConfig) {
            self.state.lock().unwrap().config = config;
        }

        pub fn inner(&self) -> &Arc<SlateStore> {
            &self.inner
        }

        /// Close the inner db; caller must hold the only [`Arc`] to `self`.
        pub async fn shutdown(self: Arc<Self>) -> Result<(), StorageError> {
            if Arc::strong_count(&self) != 1 {
                return Ok(());
            }
            let this = Arc::try_unwrap(self)
                .map_err(|_| StorageError("outstanding FaultSlateStore clones".into()))?;
            match Arc::try_unwrap(this.inner) {
                Ok(slate) => slate.close().await,
                Err(_) => Ok(()),
            }
        }

        async fn delay_and_maybe_fail(&self, op: &str) -> Result<(), StorageError> {
            let yields = {
                let mut s = self.state.lock().unwrap();
                let max = s.config.max_latency_yields;
                if max == 0 {
                    0
                } else {
                    s.rng.random_range(0..=max)
                }
            };
            let fail = {
                let mut s = self.state.lock().unwrap();
                let rate = s.config.error_rate;
                rate > 0.0 && s.rng.random_bool(rate)
            };
            for _ in 0..yields {
                tokio::task::yield_now().await;
            }
            if fail {
                Err(StorageError(format!("injected {op} fault")))
            } else {
                Ok(())
            }
        }

        fn should_flush(&self) -> bool {
            let mut s = self.state.lock().unwrap();
            let rate = s.config.flush_rate;
            rate > 0.0 && s.rng.random_bool(rate)
        }
    }

    impl OrderedStore for FaultSlateStore {
        fn get(
            &self,
            key: &[u8],
        ) -> impl Future<Output = Result<Option<Vec<u8>>, StorageError>> + Send {
            let this = self.clone();
            let key = key.to_vec();
            async move {
                this.delay_and_maybe_fail("get").await?;
                this.inner.get(&key).await
            }
        }

        fn scan(&self, start: Vec<u8>, end: Option<Vec<u8>>) -> impl ScanIter {
            self.inner.scan(start, end)
        }

        fn apply(
            &self,
            batch: WriteBatch,
        ) -> impl Future<Output = Result<(), StorageError>> + Send {
            let this = self.clone();
            async move {
                this.delay_and_maybe_fail("apply").await?;
                this.inner.apply(batch).await?;
                if this.should_flush() {
                    this.inner.flush().await?;
                    (this.on_durable)();
                }
                Ok(())
            }
        }
    }
}

#[cfg(feature = "slatedb")]
pub use imp::FaultSlateStore;
