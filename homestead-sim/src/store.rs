//! The fault-injecting store.
//!
//! Two maps model the durability boundary: `current` is what the running
//! process sees; `durable` is what survives a crash. Every applied batch
//! lands in `current`, and a seeded coin decides when `current` is flushed
//! to `durable` — so the state lost at a crash is always the *suffix* of
//! batches applied since the last flush, never a torn batch and never a
//! gap. This is the WAL contract, minus the WAL.
//!
//! Faults are injected strictly *before* any mutation, so an errored
//! `apply` leaves no trace — the atomicity contract is real, not
//! simulated. Every operation also yields a seeded number of times before
//! completing, giving the executor interleaving points where a real store
//! would have IO latency.

use homestead_server::storage::{Op, OrderedStore, ScanIter, StorageError, WriteBatch};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::collections::BTreeMap;
use std::future::Future;
use std::ops::Bound;
use std::sync::{Arc, Mutex};
use std::task::Poll;

/// Fault dials. Probabilities per operation, all driven by the store's
/// seeded RNG.
#[derive(Clone, Copy, Debug)]
pub struct FaultConfig {
    /// Chance an operation returns an injected [`StorageError`].
    pub error_rate: f64,
    /// Chance an applied batch is immediately flushed to durable state.
    pub flush_rate: f64,
    /// Operations yield `0..=max_latency_yields` times before completing.
    pub max_latency_yields: u32,
}

impl FaultConfig {
    /// No faults, instant flushes: behaves like `MemoryStore`.
    pub const NONE: Self = Self {
        error_rate: 0.0,
        flush_rate: 1.0,
        max_latency_yields: 0,
    };
}

struct Inner {
    current: BTreeMap<Vec<u8>, Vec<u8>>,
    durable: BTreeMap<Vec<u8>, Vec<u8>>,
    config: FaultConfig,
    rng: StdRng,
}

/// Cheap-to-clone handle; all clones see one store (it is the shard).
#[derive(Clone)]
pub struct SimStore {
    inner: Arc<Mutex<Inner>>,
}

impl SimStore {
    pub fn new(seed: u64, config: FaultConfig) -> Self {
        Self {
            inner: Arc::new(Mutex::new(Inner {
                current: BTreeMap::new(),
                durable: BTreeMap::new(),
                config,
                rng: StdRng::seed_from_u64(seed),
            })),
        }
    }

    /// Simulated power loss: volatile state is gone; the store rolls back
    /// to the last flush. The caller then restarts actors over this same
    /// store — recovery is just "read what survived".
    pub fn crash(&self) {
        let mut inner = self.inner.lock().unwrap();
        inner.current = inner.durable.clone();
    }

    /// Force-flushes current state to durable (a clean shutdown).
    pub fn flush(&self) {
        let mut inner = self.inner.lock().unwrap();
        inner.durable = inner.current.clone();
    }

    /// Swaps the fault dials (e.g. disable faults while verifying).
    pub fn set_config(&self, config: FaultConfig) {
        self.inner.lock().unwrap().config = config;
    }

    /// Number of live keys the running process sees.
    pub fn len(&self) -> usize {
        self.inner.lock().unwrap().current.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn draw_yields(&self) -> u32 {
        let mut inner = self.inner.lock().unwrap();
        let max = inner.config.max_latency_yields;
        if max == 0 { 0 } else { inner.rng.random_range(0..=max) }
    }

    fn draw_error(&self) -> bool {
        let mut inner = self.inner.lock().unwrap();
        let rate = inner.config.error_rate;
        rate > 0.0 && inner.rng.random_bool(rate)
    }
}

/// Yields `remaining` times (self-waking each time), then runs `op` and
/// completes — the mutation and `Ready` happen in the *same* poll, so a
/// cancelled future either did everything or nothing.
struct DelayedOp<F, T>
where
    F: FnMut() -> T + Unpin,
{
    remaining: u32,
    op: F,
}

impl<F, T> Future for DelayedOp<F, T>
where
    F: FnMut() -> T + Unpin,
{
    type Output = T;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Self::Output> {
        if self.remaining > 0 {
            self.remaining -= 1;
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }
        Poll::Ready((self.op)())
    }
}

/// Scan over a crash-consistent snapshot taken at creation.
pub struct SimScan {
    entries: std::vec::IntoIter<(Vec<u8>, Vec<u8>)>,
}

impl ScanIter for SimScan {
    fn next(
        &mut self,
    ) -> impl Future<Output = Result<Option<(Vec<u8>, Vec<u8>)>, StorageError>> + Send {
        std::future::ready(Ok(self.entries.next()))
    }
}

impl OrderedStore for SimStore {
    fn get(
        &self,
        key: &[u8],
    ) -> impl Future<Output = Result<Option<Vec<u8>>, StorageError>> + Send {
        let this = self.clone();
        let key = key.to_vec();
        let fail = self.draw_error();
        DelayedOp {
            remaining: self.draw_yields(),
            op: move || {
                if fail {
                    return Err(StorageError("injected get fault".into()));
                }
                Ok(this.inner.lock().unwrap().current.get(&key).cloned())
            },
        }
    }

    fn scan(&self, start: Vec<u8>, end: Option<Vec<u8>>) -> impl ScanIter {
        // Setup is synchronous and infallible by trait contract; fault
        // injection for reads is carried by `get` and the per-item yields
        // of the surrounding verb instead.
        let end = match end {
            Some(end) => Bound::Excluded(end),
            None => Bound::Unbounded,
        };
        let entries: Vec<(Vec<u8>, Vec<u8>)> = self
            .inner
            .lock()
            .unwrap()
            .current
            .range((Bound::Included(start), end))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        SimScan {
            entries: entries.into_iter(),
        }
    }

    fn apply(
        &self,
        batch: WriteBatch,
    ) -> impl Future<Output = Result<(), StorageError>> + Send {
        let this = self.clone();
        let fail = self.draw_error();
        let mut batch = Some(batch);
        DelayedOp {
            remaining: self.draw_yields(),
            op: move || {
                if fail {
                    // Before any mutation: an errored apply leaves no trace.
                    return Err(StorageError("injected apply fault".into()));
                }
                let mut inner = this.inner.lock().unwrap();
                for op in batch.take().expect("apply future polled after Ready").ops {
                    match op {
                        Op::Put { key, value } => {
                            inner.current.insert(key, value);
                        }
                        Op::Delete { key } => {
                            inner.current.remove(&key);
                        }
                    }
                }
                let flush_rate = inner.config.flush_rate;
                if flush_rate > 0.0 && inner.rng.random_bool(flush_rate) {
                    inner.durable = inner.current.clone();
                }
                Ok(())
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use homestead_server::storage::conformance;
    use pollster::block_on;

    #[test]
    fn fault_free_simstore_passes_conformance() {
        let mut n = 0u64;
        block_on(conformance::run_all(move || {
            n += 1;
            SimStore::new(n, FaultConfig::NONE)
        }));
    }

    #[test]
    fn crash_rolls_back_to_the_last_flush_prefix() {
        let store = SimStore::new(7, FaultConfig {
            error_rate: 0.0,
            flush_rate: 0.0, // manual flushes only
            max_latency_yields: 0,
        });
        let put = |k: &[u8], v: &[u8]| {
            let mut batch = WriteBatch::new();
            batch.put(k.to_vec(), v.to_vec());
            block_on(store.apply(batch)).unwrap();
        };

        put(b"a", b"1");
        put(b"b", b"2");
        store.flush();
        put(b"c", b"3");
        put(b"d", b"4");

        store.crash();
        assert_eq!(block_on(store.get(b"a")).unwrap(), Some(b"1".to_vec()));
        assert_eq!(block_on(store.get(b"b")).unwrap(), Some(b"2".to_vec()));
        assert_eq!(block_on(store.get(b"c")).unwrap(), None, "unflushed suffix lost");
        assert_eq!(block_on(store.get(b"d")).unwrap(), None);
    }

    #[test]
    fn injected_apply_fault_leaves_no_trace() {
        let store = SimStore::new(1, FaultConfig {
            error_rate: 1.0,
            flush_rate: 1.0,
            max_latency_yields: 0,
        });
        let mut batch = WriteBatch::new();
        batch.put(b"k".to_vec(), b"v".to_vec());
        assert!(block_on(store.apply(batch)).is_err());

        store.set_config(FaultConfig::NONE);
        assert_eq!(block_on(store.get(b"k")).unwrap(), None);
    }
}
