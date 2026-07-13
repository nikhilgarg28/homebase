//! Seeded property workload comparing the public space engine with the
//! append-only reference model after every command.

use super::reference::{ReferenceModel, conflicts_with_lease};
use super::{key, plaintext_entry};
use crate::error::Error;
use crate::space::{Space, data};
use crate::storage::MemoryStore;
use homebase_core::clock::{HybridTimestamp, Timestamp};
use homebase_core::key::Key;
use homebase_core::lease::{LeaseId, LeaseMode};
use homebase_core::messages::{
    AcquireRequest, AdmissionBatch, AdmissionRequest, AdmissionResult, GetRequest, KernelError,
    LeaseSpec, PullRequest, Range, RangeAssert, RangeCursor, RangeCut, ReadAtRequest,
    ReleaseRequest,
};
use homebase_core::seal::Seal;
use homebase_core::space::SpaceId;
use homebase_core::tag::{
    AdmissionSeq, CipherEpoch, DeviceChecksum, DeviceEntry, DeviceId, DeviceSeq, DeviceTag,
    Mutation, OpaqueValue, Ver,
};
use pollster::block_on;
use proptest::prelude::*;
use std::time::Duration;

const SPACE: SpaceId = SpaceId([13; 16]);
const DEVICES: [DeviceId; 2] = [DeviceId([1; 16]), DeviceId([2; 16])];

#[derive(Clone, Debug)]
enum Cmd {
    Write {
        device: usize,
        kinds: Vec<u8>,
    },
    Assert {
        device: usize,
        range: u8,
        stale: bool,
    },
    Acquire {
        device: usize,
        prefix: u8,
    },
    Release {
        device: usize,
        slot: usize,
    },
}

fn arb_cmd() -> impl Strategy<Value = Cmd> {
    prop_oneof![
        7 => (0..DEVICES.len(), prop::collection::vec(0u8..8, 1..=3))
            .prop_map(|(device, kinds)| Cmd::Write { device, kinds }),
        2 => (0..DEVICES.len(), 0u8..3, any::<bool>())
            .prop_map(|(device, range, stale)| Cmd::Assert { device, range, stale }),
        2 => (0..DEVICES.len(), 0u8..5)
            .prop_map(|(device, prefix)| Cmd::Acquire { device, prefix }),
        1 => (0..DEVICES.len(), 0usize..8)
            .prop_map(|(device, slot)| Cmd::Release { device, slot }),
    ]
}

fn keys() -> Vec<Key> {
    vec![
        key(&[b"db", b"a"]),
        key(&[b"db", b"a", b"child"]),
        key(&[b"db", b"b"]),
        key(&[b"other", b"c"]),
    ]
}

fn query_ranges() -> Vec<Range> {
    vec![
        Range::Full,
        Range::Prefix(key(&[b"db"])),
        Range::Prefix(key(&[b"db", b"a"])),
        Range::Prefix(key(&[b"db", b"b"])),
        Range::Prefix(key(&[b"other"])),
    ]
}

fn assert_prefix(which: u8) -> Key {
    match which % 3 {
        0 => key(&[b"db"]),
        1 => key(&[b"db", b"a"]),
        _ => key(&[b"other"]),
    }
}

fn lease_prefix(which: u8) -> Key {
    match which % 5 {
        0 => key(&[b"db"]),
        1 => key(&[b"db", b"a"]),
        2 => key(&[b"db", b"a", b"child"]),
        3 => key(&[b"db", b"b"]),
        _ => key(&[b"other"]),
    }
}

fn mutation(kind: u8, value: u64) -> Mutation<Vec<u8>> {
    let keys = keys();
    match kind % 8 {
        0 => Mutation::Set {
            key: keys[0].clone(),
            value: value.to_be_bytes().to_vec(),
        },
        1 => Mutation::Set {
            key: keys[1].clone(),
            value: value.to_be_bytes().to_vec(),
        },
        2 => Mutation::Set {
            key: keys[2].clone(),
            value: value.to_be_bytes().to_vec(),
        },
        3 => Mutation::Delete {
            key: keys[(value as usize) % keys.len()].clone(),
        },
        4 => Mutation::DeleteRange {
            range: Range::Prefix(key(&[b"db"])),
        },
        5 => Mutation::DeleteRange {
            range: Range::Prefix(key(&[b"db", b"a"])),
        },
        6 => Mutation::DeleteRange { range: Range::Full },
        _ => Mutation::Set {
            key: keys[3].clone(),
            value: value.to_be_bytes().to_vec(),
        },
    }
}

fn opaque(mutation: Mutation<Vec<u8>>) -> Mutation<OpaqueValue> {
    match mutation {
        Mutation::Set { key, value } => Mutation::Set {
            key,
            value: OpaqueValue(value),
        },
        Mutation::Delete { key } => Mutation::Delete { key },
        Mutation::DeleteRange { range } => Mutation::DeleteRange { range },
    }
}

#[derive(Clone)]
struct Held {
    id: LeaseId,
    device: usize,
    prefix: Key,
}

struct Harness {
    space: Space,
    store: MemoryStore,
    checksums: [DeviceChecksum; 2],
    device_seqs: [u64; 2],
    next_ver: u64,
    leases: Vec<Held>,
    model: ReferenceModel,
}

impl Harness {
    fn new() -> Self {
        Self {
            space: Space::new(SPACE),
            store: MemoryStore::new(),
            checksums: [DeviceChecksum::EMPTY; 2],
            device_seqs: [0; 2],
            next_ver: 1,
            leases: Vec::new(),
            model: ReferenceModel::default(),
        }
    }

    fn write(&mut self, device: usize, kinds: Vec<u8>) -> Result<(), TestCaseError> {
        let seq = self.device_seqs[device] + 1;
        let mut plaintext = Vec::new();
        let mut entries = Vec::new();
        for kind in kinds {
            let ver = Ver(self.next_ver);
            self.next_ver += 1;
            let mutation = mutation(kind, ver.0);
            plaintext.push((mutation.clone(), ver));
            entries.push(DeviceEntry {
                mutation: opaque(mutation),
                tag: DeviceTag {
                    device: DEVICES[device],
                    device_seq: DeviceSeq(seq),
                    ver,
                    cipher_epoch: CipherEpoch(0),
                },
                seal: Seal::empty_aead_v1(),
            });
        }
        let conflict = plaintext.iter().any(|(mutation, _)| {
            self.leases.iter().any(|lease| {
                lease.device != device && conflicts_with_lease(mutation, &lease.prefix)
            })
        });
        let response = block_on(
            self.space.admit(
                &self.store,
                Timestamp(self.next_ver),
                &AdmissionRequest {
                    device: DEVICES[device],
                    expected_checksum: self.checksums[device],
                    evidence: self
                        .leases
                        .iter()
                        .filter(|lease| lease.device == device)
                        .map(|lease| lease.id)
                        .collect(),
                    batches: vec![AdmissionBatch {
                        device_seq: DeviceSeq(seq),
                        range_asserts: vec![],
                        entries,
                    }],
                },
            ),
        );
        if conflict {
            prop_assert!(
                matches!(
                    &response,
                    Err(Error::Kernel(
                        KernelError::Contended { .. } | KernelError::RangeContended { .. }
                    ))
                ),
                "foreign lease conflict was not enforced: {:?}",
                response
            );
        } else {
            let response = response
                .map_err(|error| TestCaseError::fail(format!("valid write rejected: {error:?}")))?;
            prop_assert_eq!(
                response.applied_admission_seq(0),
                Some(AdmissionSeq(self.model.high_water().0 + 1))
            );
            self.checksums[device] = response.checksum;
            self.device_seqs[device] = seq;
            self.model
                .append_batch(DEVICES[device], DeviceSeq(seq), plaintext);
        }
        Ok(())
    }

    fn assert_range(&mut self, device: usize, which: u8, stale: bool) -> Result<(), TestCaseError> {
        let prefix = assert_prefix(which);
        let range = Range::Prefix(prefix.clone());
        let actual = self
            .model
            .max_admission_excluding(&range, DEVICES[device], self.model.high_water())
            .unwrap_or(AdmissionSeq(0));
        let should_fail = stale && actual.0 > 0;
        let upto = if should_fail {
            AdmissionSeq(actual.0 - 1)
        } else {
            actual
        };
        let seq = self.device_seqs[device] + 1;
        let response = block_on(self.space.admit(
            &self.store,
            Timestamp(self.next_ver),
            &AdmissionRequest {
                device: DEVICES[device],
                expected_checksum: self.checksums[device],
                evidence: vec![],
                batches: vec![AdmissionBatch {
                    device_seq: DeviceSeq(seq),
                    range_asserts: vec![RangeAssert { prefix, upto }],
                    entries: vec![],
                }],
            },
        ))
        .map_err(|error| TestCaseError::fail(format!("assert verb errored: {error:?}")))?;
        if should_fail {
            prop_assert!(
                matches!(
                    &response.results[0],
                    AdmissionResult::Failed {
                        error: KernelError::RangeAssertFailed { .. }
                    }
                ),
                "stale assertion unexpectedly passed: {:?}",
                response
            );
            prop_assert_eq!(response.checksum, self.checksums[device]);
        } else {
            prop_assert!(
                matches!(&response.results[0], AdmissionResult::Applied { .. }),
                "valid assertion unexpectedly failed: {:?}",
                response
            );
            self.checksums[device] = response.checksum;
            self.device_seqs[device] = seq;
            self.model
                .append_batch(DEVICES[device], DeviceSeq(seq), vec![]);
        }
        Ok(())
    }

    fn acquire(&mut self, device: usize, which: u8) -> Result<(), TestCaseError> {
        let prefix = lease_prefix(which);
        let conflict = self
            .leases
            .iter()
            .any(|lease| prefix.starts_with(&lease.prefix) || lease.prefix.starts_with(&prefix));
        let response = block_on(self.space.acquire(
            &self.store,
            Timestamp(1),
            &AcquireRequest {
                device: DEVICES[device],
                requested_at: HybridTimestamp::ZERO,
                specs: vec![LeaseSpec {
                    prefix: prefix.clone(),
                    mode: LeaseMode::Write,
                    ttl: Duration::from_secs(1 << 30),
                }],
            },
        ));
        if conflict {
            prop_assert!(
                matches!(&response, Err(Error::Kernel(KernelError::Contended { .. }))),
                "overlapping lease acquisition was not rejected: {:?}",
                response
            );
        } else {
            let lease = response
                .map_err(|error| TestCaseError::fail(format!("lease rejected: {error:?}")))?
                .leases
                .remove(0);
            prop_assert_eq!(lease.barrier, self.model.high_water());
            self.leases.push(Held {
                id: lease.id,
                device,
                prefix,
            });
        }
        Ok(())
    }

    fn release(&mut self, device: usize, slot: usize) -> Result<(), TestCaseError> {
        let owned: Vec<usize> = self
            .leases
            .iter()
            .enumerate()
            .filter(|(_, lease)| lease.device == device)
            .map(|(index, _)| index)
            .collect();
        let Some(index) = owned.get(slot % owned.len().max(1)).copied() else {
            return Ok(());
        };
        let id = self.leases[index].id;
        block_on(self.space.release(
            &self.store,
            Timestamp(1),
            &ReleaseRequest {
                device: DEVICES[device],
                leases: vec![id],
            },
        ))
        .map_err(|error| TestCaseError::fail(format!("release failed: {error:?}")))?;
        self.leases.remove(index);
        Ok(())
    }

    fn check(&self) -> Result<(), TestCaseError> {
        let high = self.model.high_water();
        let keys = keys();
        let got = block_on(
            self.space
                .get(&self.store, &GetRequest { keys: keys.clone() }),
        )
        .unwrap();
        for (key, actual) in keys.iter().zip(got.entries.iter()) {
            let expected = self.model.get_at(key, high);
            prop_assert_eq!(
                actual.as_ref().map(plaintext_entry),
                expected,
                "get mismatch for {:?}",
                key
            );
        }

        let pulled = block_on(self.space.pull(
            &self.store,
            &PullRequest {
                after: AdmissionSeq(0),
                max_batches: None,
            },
        ))
        .unwrap();
        prop_assert_eq!(pulled.through, high);
        let expected_batches = self.model.replay(AdmissionSeq(0), high);
        prop_assert_eq!(pulled.batches.len(), expected_batches.len());
        for (actual, expected) in pulled.batches.iter().zip(expected_batches.iter()) {
            prop_assert_eq!(actual.admission_seq, expected.admission_seq);
            prop_assert_eq!(actual.device, expected.device);
            prop_assert_eq!(actual.device_seq, expected.device_seq);
            let actual_entries = actual
                .entries
                .iter()
                .map(plaintext_entry)
                .collect::<Vec<_>>();
            prop_assert_eq!(&actual_entries, &expected.entries);
        }

        for range in query_ranges() {
            let response = block_on(self.space.read_at(
                &self.store,
                &ReadAtRequest {
                    ranges: vec![RangeCursor {
                        range: range.clone(),
                        since: Some(AdmissionSeq(0)),
                    }],
                },
            ))
            .unwrap();
            let RangeCut::Delta(actual) = &response.ranges[0] else {
                unreachable!()
            };
            let expected = self.model.read(&range, Some(AdmissionSeq(0)));
            let RangeCut::Delta(expected) = expected.cut else {
                unreachable!()
            };
            prop_assert_eq!(response.at, high);
            prop_assert_eq!(
                actual.iter().map(plaintext_entry).collect::<Vec<_>>(),
                expected
            );
            prop_assert_eq!(
                block_on(data::effective_live_count(SPACE, &self.store, &range)).unwrap(),
                self.model.live_count(&range, high)
            );
            let expected_ver = self.model.max_ver(&range, high);
            let actual_history =
                block_on(data::effective_history(SPACE, &self.store, &range)).unwrap();
            match expected_ver {
                Some(ver) => prop_assert_eq!(actual_history.max_ver, ver),
                None => {
                    prop_assert_eq!(actual_history.max_ver, Ver(0));
                    prop_assert_eq!(actual_history.history.max_admission_seq(), AdmissionSeq(0));
                }
            }
        }
        Ok(())
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    #[test]
    fn public_engine_refines_reference_after_every_command(
        commands in prop::collection::vec(arb_cmd(), 1..=50)
    ) {
        let mut harness = Harness::new();
        for command in commands {
            match command {
                Cmd::Write { device, kinds } => harness.write(device, kinds)?,
                Cmd::Assert { device, range, stale } => {
                    harness.assert_range(device, range, stale)?
                }
                Cmd::Acquire { device, prefix } => harness.acquire(device, prefix)?,
                Cmd::Release { device, slot } => harness.release(device, slot)?,
            }
            harness.check()?;
        }
    }
}
