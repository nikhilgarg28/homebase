//! SlateDB torture: real persistence on a local object store, restart via
//! flush + reopen (no [`SimStore::crash`] — slatedb replays its WAL).
//!
//! Run: `cargo test -p homebase-sim slate_torture`
//! Skip (Layer 1 only): `cargo test -p homebase-sim --no-default-features`

#![cfg(feature = "slatedb")]

use homebase_core::clock::{ManualClock, Timestamp};
use homebase_core::key::Key;
use homebase_core::lease::{LeaseMode, LeaseRef};
use homebase_core::messages::{
    AcquireRequest, GetRequest, LeaseSpec, PutBatch, PutBatchRequest, PutEntry,
};
use homebase_core::space::{Space as _, SpaceId};
use homebase_core::tag::{DeviceId, DeviceSeq, Value, Ver};
use homebase_server::actor::{SpaceActor, SpaceHandle};
use homebase_server::storage::{SlateOpenOptions, SlateStore, local_object_store};
use homebase_sim::check;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::sync::Arc;
use std::time::Duration;
use tempfile::tempdir;

const SPACE: SpaceId = SpaceId([5; 16]);

fn dev() -> DeviceId {
    DeviceId([1; 16])
}

fn key(parts: &[&[u8]]) -> Key {
    Key::from_bytes(parts.iter().copied()).unwrap()
}

async fn write_marker(
    handle: &SpaceHandle,
    device_seq: u64,
    stealable: bool,
    steal: bool,
    marker: &[u8],
) {
    let granted = handle
        .acquire(AcquireRequest {
            device: dev(),
            steal,
            specs: vec![LeaseSpec {
                prefix: key(&[b"db"]),
                mode: LeaseMode::Write,
                ttl: Duration::from_secs(60),
                stealable,
            }],
        })
        .await
        .unwrap();
    let lease = LeaseRef {
        id: granted.leases[0].id,
        epoch: granted.leases[0].epoch,
    };
    handle
        .put_batch(PutBatchRequest {
            device: dev(),
            leases: vec![lease],
            batches: vec![PutBatch {
                device_seq: DeviceSeq(device_seq),
                entries: vec![PutEntry {
                    key: key(&[b"db", b"marker"]),
                    value: Value::Present(marker.to_vec()),
                    ver: Ver(device_seq),
                }],
            }],
        })
        .await
        .unwrap();
}

async fn open_shard(root: &std::path::Path) -> Arc<SlateStore> {
    let object_store = local_object_store(root).unwrap();
    Arc::new(
        SlateStore::open("shard", object_store, SlateOpenOptions::default())
            .await
            .unwrap(),
    )
}

#[tokio::test]
async fn slate_survives_flush_and_reopen() {
    let dir = tempdir().unwrap();
    let root = dir.path();
    let clock = Arc::new(ManualClock::new(Timestamp(0)));

    let store = open_shard(root).await;
    let (actor, handle) = SpaceActor::new(SPACE, Arc::clone(&store), Arc::clone(&clock));
    let task = tokio::spawn(actor.run());
    write_marker(&handle, 1, true, false, b"before-crash").await;
    store.flush().await.unwrap();
    drop(handle);
    task.abort();
    let _ = task.await;

    // "Crash": reopen; stale lease record survived — steal it back.
    let store2 = open_shard(root).await;
    let (actor2, handle2) = SpaceActor::new(SPACE, Arc::clone(&store2), clock);
    let task2 = tokio::spawn(actor2.run());
    write_marker(&handle2, 2, true, true, b"after-reopen").await;
    store2.flush().await.unwrap();

    let got = handle2
        .get(GetRequest {
            keys: vec![key(&[b"db", b"marker"])],
        })
        .await
        .unwrap();
    let entry = got.entries[0].as_ref().unwrap();
    assert_eq!(entry.value, Value::Present(b"after-reopen".to_vec()));

    check::audit(SPACE, store2.as_ref());
    drop(handle2);
    task2.abort();
}

#[tokio::test]
async fn slate_seeded_writes_audit_clean() {
    let mut rng = StdRng::seed_from_u64(99);
    let dir = tempdir().unwrap();
    let store = open_shard(dir.path()).await;
    let clock = Arc::new(ManualClock::new(Timestamp(0)));
    let (actor, handle) = SpaceActor::new(SPACE, Arc::clone(&store), clock);
    let task = tokio::spawn(actor.run());

    let granted = handle
        .acquire(AcquireRequest {
            device: dev(),
            steal: false,
            specs: vec![LeaseSpec {
                prefix: key(&[b"load"]),
                mode: LeaseMode::Write,
                ttl: Duration::from_secs(60),
                stealable: false,
            }],
        })
        .await
        .unwrap();
    let lease = LeaseRef {
        id: granted.leases[0].id,
        epoch: granted.leases[0].epoch,
    };

    for seq in 1..=rng.random_range(5..15) {
        let k = key(&[b"load", format!("k{seq}").as_bytes()]);
        handle
            .put_batch(PutBatchRequest {
                device: dev(),
                leases: vec![lease],
                batches: vec![PutBatch {
                    device_seq: DeviceSeq(seq),
                    entries: vec![PutEntry {
                        key: k,
                        value: Value::Present(format!("v{seq}").into_bytes()),
                        ver: Ver(1),
                    }],
                }],
            })
            .await
            .unwrap();
    }
    store.flush().await.unwrap();
    check::audit(SPACE, store.as_ref());
    drop(handle);
    task.abort();
}
