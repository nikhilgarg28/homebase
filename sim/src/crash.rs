//! Parameterized crash-restart torture: Layer 1 ([`sim`]) and Layer 3 ([`slate`]).
//!
//! Same clients, oracles, and recovery paths; backends differ only in store
//! factory and crash primitive. The first three phases inject faults and
//! crash; the final fault-free phase proves recovery can make forward progress.

use crate::check;
use crate::exec::SimExecutor;
use crate::store::{FaultConfig, SimStore};
use homebase_core::clock::{HybridTimestamp, ManualClock, Timestamp};
use homebase_core::key::Key;
use homebase_core::lease::{LeaseId, LeaseMode};
use homebase_core::messages::{
    AcquireRequest, AdmissionBatch, AdmissionRequest, KernelError, LeaseSpec,
};
use homebase_core::seal::Seal;
use homebase_core::space::{Space as _, SpaceError, SpaceId};
use homebase_core::tag::{
    CipherEpoch, DeviceChecksum, DeviceEntry, DeviceId, DeviceSeq, DeviceTag, Mutation,
    OpaqueValue, Ver,
};
use homebase_server::actor::{SpaceActor, SpaceHandle};
use homebase_server::storage::OrderedStore;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

pub const SPACE: SpaceId = SpaceId([3; 16]);
pub const DEVICES: u8 = 2;
pub const PHASES: usize = 4;
pub const PUTS_PER_PHASE: u64 = 8;

pub const FAULTS: FaultConfig = FaultConfig {
    error_rate: 0.02,
    flush_rate: 0.25,
    max_latency_yields: 3,
};

/// Object-store-level faults only; [`FaultSlateStore`] handles yields + flush.
#[cfg(feature = "slatedb")]
pub const SLATE_OS_FAULTS: FaultConfig = FAULTS;

#[cfg(feature = "slatedb")]
pub const SLATE_STORE_FAULTS: FaultConfig = FaultConfig {
    error_rate: 0.0,
    flush_rate: FAULTS.flush_rate,
    max_latency_yields: FAULTS.max_latency_yields,
};

pub fn dev(d: u8) -> DeviceId {
    DeviceId([d + 1; 16])
}

pub fn prefix(d: u8) -> Key {
    Key::from_bytes([format!("d{d}").into_bytes()]).unwrap()
}

pub fn user_key(d: u8, seq: u64) -> Key {
    Key::from_bytes([
        format!("d{d}").into_bytes(),
        format!("k{seq:06}").into_bytes(),
    ])
    .unwrap()
}

pub fn value(d: u8, seq: u64) -> Vec<u8> {
    format!("v{d}-{seq}").into_bytes()
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Ack {
    pub device: u8,
    pub device_seq: u64,
    pub admission_seq: u64,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct Coverage {
    pub lease_invalid: u32,
    pub seq_regression: u32,
    pub acked_writes_lost: u32,
    pub unavailable: u32,
}

#[derive(Clone)]
pub struct DeviceState {
    pub lease: Arc<Mutex<Option<LeaseId>>>,
    pub next_seq: Arc<AtomicU64>,
    pub checksum: Arc<Mutex<DeviceChecksum>>,
}

pub async fn client(
    handle: SpaceHandle,
    d: u8,
    state: DeviceState,
    acks: Arc<Mutex<Vec<Ack>>>,
    coverage: Arc<Mutex<Coverage>>,
) {
    let mut completed = 0u64;
    let mut attempts = 0u32;
    while completed < PUTS_PER_PHASE && attempts < 10 * PUTS_PER_PHASE as u32 {
        attempts += 1;

        if state.lease.lock().unwrap().is_none() {
            let req = AcquireRequest {
                device: dev(d),
                requested_at: HybridTimestamp::ZERO,
                specs: vec![LeaseSpec {
                    prefix: prefix(d),
                    mode: LeaseMode::Write,
                    ttl: Duration::from_secs(60),
                }],
            };
            match handle.acquire(req).await {
                Ok(resp) => {
                    *state.lease.lock().unwrap() = Some(resp.leases[0].id);
                }
                Err(SpaceError::Unavailable { .. }) => {
                    coverage.lock().unwrap().unavailable += 1;
                    return;
                }
                Err(SpaceError::Kernel(err)) => {
                    panic!("per-device prefix should not contend: {err:?}")
                }
            }
            continue;
        }

        let seq = state.next_seq.load(Ordering::SeqCst);
        let lease = state.lease.lock().unwrap().unwrap();
        let confirmed_checksum = *state.checksum.lock().unwrap();
        let req = AdmissionRequest {
            device: dev(d),
            expected_checksum: confirmed_checksum,
            evidence: vec![lease],
            batches: vec![AdmissionBatch {
                device_seq: DeviceSeq(seq),
                range_asserts: vec![],
                entries: vec![DeviceEntry {
                    mutation: Mutation::Set {
                        key: user_key(d, seq),
                        value: OpaqueValue(value(d, seq)),
                    },
                    tag: DeviceTag {
                        device: dev(d),
                        device_seq: DeviceSeq(seq),
                        ver: Ver(1),
                        cipher_epoch: CipherEpoch(0),
                    },
                    seal: Seal::empty_aead_v1(),
                }],
            }],
        };
        let sent_checksum = req.batches[0].checksum(confirmed_checksum, SPACE, dev(d));
        match handle.admit(req).await {
            Ok(resp) => {
                acks.lock().unwrap().push(Ack {
                    device: d,
                    device_seq: seq,
                    admission_seq: resp.applied_admission_seq(0).unwrap().0,
                });
                state.next_seq.store(seq + 1, Ordering::SeqCst);
                *state.checksum.lock().unwrap() = resp.checksum;
                completed += 1;
            }
            Err(SpaceError::Kernel(KernelError::LeaseInvalid { .. })) => {
                coverage.lock().unwrap().lease_invalid += 1;
                *state.lease.lock().unwrap() = None;
            }
            Err(SpaceError::Kernel(KernelError::DeviceSeqRegression { .. })) => {
                coverage.lock().unwrap().seq_regression += 1;
                // This direct-kernel torture client has no re-mint/resync
                // layer; end this attempt and let the next phase recover.
                return;
            }
            Err(SpaceError::Kernel(KernelError::DeviceChecksumMismatch {
                current_seq,
                current,
            })) => {
                coverage.lock().unwrap().seq_regression += 1;
                if current_seq != DeviceSeq(seq) || current != sent_checksum {
                    // The harness records the rollback/fork detection, then
                    // rebases so later phases can continue exercising the
                    // kernel. The real client returns a fatal Fork instead.
                    state.next_seq.store(current_seq.0 + 1, Ordering::SeqCst);
                    *state.checksum.lock().unwrap() = current;
                    continue;
                }
                state.next_seq.store(seq + 1, Ordering::SeqCst);
                *state.checksum.lock().unwrap() = sent_checksum;
                completed += 1;
            }
            Err(SpaceError::Unavailable { .. }) => {
                coverage.lock().unwrap().unavailable += 1;
                return;
            }
            Err(SpaceError::Kernel(err)) => panic!("unexpected kernel rejection: {err:?}"),
        }
    }
}

pub fn phase_oracle<S: OrderedStore>(
    store: &S,
    acks: &Arc<Mutex<Vec<Ack>>>,
    coverage: &Arc<Mutex<Coverage>>,
    seed: u64,
) {
    let audit = check::audit(SPACE, store);
    let high = audit.max_admission_seq;
    acks.lock().unwrap().retain(|ack| {
        let record = audit.data.get(&user_key(ack.device, ack.device_seq));
        if ack.admission_seq <= high {
            let record = record.unwrap_or_else(|| {
                panic!("acked batch below high water lost: {ack:?} (seed {seed})")
            });
            assert_eq!(
                record.entry.device_entry.mutation,
                Mutation::Set {
                    key: user_key(ack.device, ack.device_seq),
                    value: OpaqueValue(value(ack.device, ack.device_seq)),
                },
                "acked value corrupted: {ack:?} (seed {seed})"
            );
            true
        } else {
            assert!(
                record.is_none(),
                "batch above high water partially survived: {ack:?} (seed {seed})"
            );
            coverage.lock().unwrap().acked_writes_lost += 1;
            false
        }
    });
}

fn device_states(_master: &mut StdRng) -> Vec<DeviceState> {
    (0..DEVICES)
        .map(|_| DeviceState {
            lease: Arc::new(Mutex::new(None)),
            next_seq: Arc::new(AtomicU64::new(1)),
            checksum: Arc::new(Mutex::new(DeviceChecksum::EMPTY)),
        })
        .collect()
}

/// Layer 1: in-memory [`SimStore`] + synchronous [`SimExecutor`].
pub mod sim {
    use super::*;

    pub fn run_seed(seed: u64) -> (Vec<Ack>, Coverage) {
        let mut master = StdRng::seed_from_u64(seed);
        let store = SimStore::new(master.random(), FAULTS);
        let clock = Arc::new(ManualClock::new(Timestamp(0)));
        let acks = Arc::new(Mutex::new(Vec::new()));
        let coverage = Arc::new(Mutex::new(Coverage::default()));
        let devices = device_states(&mut master);

        for phase in 0..PHASES {
            store.set_config(if phase == PHASES - 1 {
                FaultConfig::NONE
            } else {
                FAULTS
            });
            let mut exec = SimExecutor::new(master.random());
            let (actor, handle) =
                SpaceActor::new(SPACE, Arc::new(store.clone()), Arc::clone(&clock));
            let actor_task = exec.spawn(actor.run());
            let client_tasks: Vec<_> = (0..DEVICES)
                .map(|d| {
                    exec.spawn(client(
                        handle.clone(),
                        d,
                        devices[d as usize].clone(),
                        Arc::clone(&acks),
                        Arc::clone(&coverage),
                    ))
                })
                .collect();
            drop(handle);

            if phase != PHASES - 1 {
                let steps = master.random_range(30..400);
                for _ in 0..steps {
                    if !exec.step() {
                        break;
                    }
                    if master.random_bool(0.05) {
                        clock.advance(Duration::from_millis(master.random_range(1..10)));
                    }
                }
                exec.cancel(actor_task);
                for task in client_tasks {
                    exec.cancel(task);
                }
                store.crash();
                exec.run_until_stalled();
            } else {
                exec.run_until_stalled();
            }

            store.set_config(FaultConfig::NONE);
            phase_oracle(&store, &acks, &coverage, seed);
        }

        let trace = acks.lock().unwrap().clone();
        assert!(
            !trace.is_empty(),
            "seed {seed} made no progress at all — faults drowned the workload"
        );
        (trace, *coverage.lock().unwrap())
    }
}

/// Layer 3: real [`SlateStore`] over a fault-injecting object store.
#[cfg(feature = "slatedb")]
pub mod slate {
    use super::*;

    pub async fn run_seed(seed: u64) -> (Vec<Ack>, Coverage) {
        let mut master = StdRng::seed_from_u64(seed);
        let mut shard = crate::slate_shard::SlateShard::new(master.random(), FAULTS).await;
        let clock = Arc::new(ManualClock::new(Timestamp(0)));
        let acks = Arc::new(Mutex::new(Vec::new()));
        let coverage = Arc::new(Mutex::new(Coverage::default()));
        let devices = device_states(&mut master);

        for phase in 0..PHASES {
            if phase == PHASES - 1 {
                shard.disable_faults();
            } else {
                shard.set_faults(FAULTS);
            }
            let store = shard.store();
            let (actor, handle) = SpaceActor::new(SPACE, store, Arc::clone(&clock));
            let actor_task = tokio::spawn(actor.run());
            let client_tasks: Vec<_> = (0..DEVICES)
                .map(|d| {
                    tokio::spawn(client(
                        handle.clone(),
                        d,
                        devices[d as usize].clone(),
                        Arc::clone(&acks),
                        Arc::clone(&coverage),
                    ))
                })
                .collect();
            drop(handle);

            if phase != PHASES - 1 {
                let steps = master.random_range(30..400);
                for _ in 0..steps {
                    tokio::task::yield_now().await;
                    if master.random_bool(0.05) {
                        clock.advance(Duration::from_millis(master.random_range(1..10)));
                    }
                }
                actor_task.abort();
                for task in client_tasks {
                    task.abort();
                }
                shard.power_loss().await;
            } else {
                for task in client_tasks {
                    let _ = task.await;
                }
                actor_task.abort();
            }

            shard.disable_faults();
            phase_oracle(shard.store().as_ref(), &acks, &coverage, seed);
        }

        let trace = acks.lock().unwrap().clone();
        assert!(
            !trace.is_empty(),
            "seed {seed} made no progress at all — faults drowned the workload"
        );
        (trace, *coverage.lock().unwrap())
    }
}
