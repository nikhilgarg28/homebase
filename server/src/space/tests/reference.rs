//! Append-only plaintext oracle for range-delete semantics.
//!
//! This module is test-only by design. It stores exact admitted batches and
//! derives every answer by replay or scan: it has no materialized point map,
//! prefix aggregate, tombstone index, or lazy count state. Production
//! optimizations are correct only when they refine these deliberately simple
//! answers.

use homebase_core::key::Key;
use homebase_core::messages::RangeCut;
use homebase_core::range::Range;
use homebase_core::seal::Seal;
use homebase_core::tag::{
    AdmissionSeq, AdmissionTag, AdmittedEntry, CipherEpoch, DeviceEntry, DeviceId, DeviceSeq,
    DeviceTag, Mutation, Ver,
};
use std::collections::BTreeMap;

/// One exact admitted batch, including accepted empty batches.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ModelBatch {
    pub admission_seq: AdmissionSeq,
    pub device: DeviceId,
    pub device_seq: DeviceSeq,
    pub entries: Vec<AdmittedEntry<Vec<u8>>>,
}

/// One stateless range observation at an atomic model cut.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ModelRead {
    pub at: AdmissionSeq,
    pub cut: RangeCut<Vec<u8>>,
}

/// Exact history from which all reference answers are derived.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct ReferenceModel {
    batches: Vec<ModelBatch>,
}

impl ReferenceModel {
    pub fn high_water(&self) -> AdmissionSeq {
        AdmissionSeq(self.batches.len() as u64)
    }

    /// Append one already-admitted batch. This does not enforce production
    /// admission policy; callers use the derived maxima and conflict queries
    /// to model acceptance separately.
    pub fn append_batch(
        &mut self,
        device: DeviceId,
        device_seq: DeviceSeq,
        mutations: Vec<(Mutation<Vec<u8>>, Ver)>,
    ) -> AdmissionSeq {
        let admission_seq = AdmissionSeq(self.high_water().0 + 1);
        let entries = mutations
            .into_iter()
            .enumerate()
            .map(|(op_index, (mutation, ver))| AdmittedEntry {
                device_entry: DeviceEntry {
                    mutation,
                    tag: DeviceTag {
                        device,
                        device_seq,
                        ver,
                        cipher_epoch: CipherEpoch(0),
                    },
                    seal: Seal::empty_aead_v1(),
                },
                admission: AdmissionTag {
                    admission_seq,
                    op_index: u32::try_from(op_index)
                        .expect("model batch operation count must fit in u32"),
                },
            })
            .collect();
        self.batches.push(ModelBatch {
            admission_seq,
            device,
            device_seq,
            entries,
        });
        admission_seq
    }

    /// Dense exact batch replay over `(after, through]`.
    pub fn replay(&self, after: AdmissionSeq, through: AdmissionSeq) -> Vec<ModelBatch> {
        self.assert_cut(after);
        self.assert_cut(through);
        assert!(after <= through, "replay start must not exceed its cut");
        self.batches
            .iter()
            .filter(|batch| batch.admission_seq > after && batch.admission_seq <= through)
            .cloned()
            .collect()
    }

    /// Exact source operations relevant to a range, in `AdmissionOrder`.
    pub fn history(&self, range: &Range, through: AdmissionSeq) -> Vec<AdmittedEntry<Vec<u8>>> {
        self.assert_cut(through);
        self.entries_through(through)
            .filter(|entry| relevant(range, &entry.device_entry.mutation))
            .cloned()
            .collect()
    }

    /// Current visible Set for one key at a cut.
    pub fn get_at(&self, key: &Key, through: AdmissionSeq) -> Option<AdmittedEntry<Vec<u8>>> {
        self.assert_cut(through);
        let mut point = None;
        let mut covering_delete = None;
        for entry in self.entries_through(through) {
            match &entry.device_entry.mutation {
                Mutation::Set { key: candidate, .. } | Mutation::Delete { key: candidate }
                    if candidate == key =>
                {
                    point = Some(entry);
                }
                Mutation::DeleteRange { range } if range.covers_key(key) => {
                    covering_delete = Some(entry.admission.order());
                }
                _ => {}
            }
        }
        let point = point?;
        if covering_delete.is_some_and(|delete| point.admission.order() <= delete) {
            return None;
        }
        match point.device_entry.mutation {
            Mutation::Set { .. } => Some(point.clone()),
            Mutation::Delete { .. } | Mutation::DeleteRange { .. } => None,
        }
    }

    /// Visible Sets in key order under a range at a cut.
    pub fn list_at(&self, range: &Range, through: AdmissionSeq) -> Vec<AdmittedEntry<Vec<u8>>> {
        self.assert_cut(through);
        let mut keys = BTreeMap::<Key, ()>::new();
        for entry in self.entries_through(through) {
            if let Some(key) = entry.point_key()
                && range.covers_key(key)
            {
                keys.insert(key.clone(), ());
            }
        }
        keys.into_keys()
            .filter_map(|key| self.get_at(&key, through))
            .collect()
    }

    pub fn live_count(&self, range: &Range, through: AdmissionSeq) -> u64 {
        self.list_at(range, through).len() as u64
    }

    /// Greatest version of every point event below `range` and every range
    /// tombstone that either covers or lies below it.
    pub fn max_ver(&self, range: &Range, through: AdmissionSeq) -> Option<Ver> {
        self.history(range, through)
            .into_iter()
            .map(|entry| entry.ver())
            .max()
    }

    /// Point-version floor: exact point history plus covering range deletes.
    pub fn max_ver_for_key(&self, key: &Key, through: AdmissionSeq) -> Option<Ver> {
        self.assert_cut(through);
        self.entries_through(through)
            .filter(|entry| match &entry.device_entry.mutation {
                Mutation::Set { key: candidate, .. } | Mutation::Delete { key: candidate } => {
                    candidate == key
                }
                Mutation::DeleteRange { range } => range.covers_key(key),
            })
            .map(AdmittedEntry::ver)
            .max()
    }

    pub fn max_admission_excluding(
        &self,
        range: &Range,
        excluded: DeviceId,
        through: AdmissionSeq,
    ) -> Option<AdmissionSeq> {
        self.history(range, through)
            .into_iter()
            .filter(|entry| entry.device_entry.tag.device != excluded)
            .map(|entry| entry.admission.admission_seq)
            .max()
    }

    /// Stateless snapshot/delta observation at an explicit atomic cut.
    pub fn read_at_cut(
        &self,
        range: &Range,
        since: Option<AdmissionSeq>,
        through: AdmissionSeq,
    ) -> ModelRead {
        self.assert_cut(through);
        let cut = match since {
            None => RangeCut::Snapshot(self.list_at(range, through)),
            Some(since) => {
                self.assert_cut(since);
                assert!(since <= through, "range cursor must not exceed its cut");
                RangeCut::Delta(
                    self.history(range, through)
                        .into_iter()
                        .filter(|entry| entry.admission.admission_seq > since)
                        .collect(),
                )
            }
        };
        ModelRead { at: through, cut }
    }

    pub fn read(&self, range: &Range, since: Option<AdmissionSeq>) -> ModelRead {
        self.read_at_cut(range, since, self.high_water())
    }

    fn entries_through(
        &self,
        through: AdmissionSeq,
    ) -> impl Iterator<Item = &AdmittedEntry<Vec<u8>>> {
        self.batches
            .iter()
            .take(through.0 as usize)
            .flat_map(|batch| &batch.entries)
    }

    fn assert_cut(&self, cut: AdmissionSeq) {
        assert!(cut <= self.high_water(), "model cut exceeds high water");
    }
}

/// Whether a write overlaps a prefix lease reservation.
pub(crate) fn conflicts_with_lease<T>(mutation: &Mutation<T>, lease_prefix: &Key) -> bool {
    match mutation {
        Mutation::Set { key, .. } | Mutation::Delete { key } => key.starts_with(lease_prefix),
        Mutation::DeleteRange { range } => range.overlaps(&Range::Prefix(lease_prefix.clone())),
    }
}

fn relevant<T>(query: &Range, mutation: &Mutation<T>) -> bool {
    match mutation {
        Mutation::Set { key, .. } | Mutation::Delete { key } => query.covers_key(key),
        Mutation::DeleteRange { range } => query.overlaps(range),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    const DEVICES: [DeviceId; 2] = [DeviceId([1; 16]), DeviceId([2; 16])];

    fn key(parts: &[&[u8]]) -> Key {
        Key::from_bytes(parts.iter().copied()).unwrap()
    }

    fn operation(kind: u8, value: u8) -> Mutation<Vec<u8>> {
        match kind % 6 {
            0 => Mutation::Set {
                key: key(&[b"db", b"a"]),
                value: vec![value],
            },
            1 => Mutation::Set {
                key: key(&[b"db", b"b", b"x"]),
                value: vec![value],
            },
            2 => Mutation::Delete {
                key: key(&[b"db", b"a"]),
            },
            3 => Mutation::DeleteRange {
                range: Range::Prefix(key(&[b"db"])),
            },
            4 => Mutation::DeleteRange {
                range: Range::Prefix(key(&[b"db", b"b"])),
            },
            _ => Mutation::DeleteRange { range: Range::Full },
        }
    }

    fn apply_eager(state: &mut BTreeMap<Key, Vec<u8>>, mutation: &Mutation<Vec<u8>>) {
        match mutation {
            Mutation::Set { key, value } => {
                state.insert(key.clone(), value.clone());
            }
            Mutation::Delete { key } => {
                state.remove(key);
            }
            Mutation::DeleteRange { range } => {
                state.retain(|key, _| !range.covers_key(key));
            }
        }
    }

    fn visible_map(model: &ReferenceModel, range: &Range) -> BTreeMap<Key, Vec<u8>> {
        model
            .list_at(range, model.high_water())
            .into_iter()
            .map(|entry| match entry.device_entry.mutation {
                Mutation::Set { key, value } => (key, value),
                Mutation::Delete { .. } | Mutation::DeleteRange { .. } => unreachable!(),
            })
            .collect()
    }

    #[test]
    fn same_batch_order_controls_visibility() {
        let db = key(&[b"db"]);
        let row = key(&[b"db", b"a"]);
        let delete = Mutation::DeleteRange {
            range: Range::Prefix(db),
        };
        let set = Mutation::Set {
            key: row.clone(),
            value: b"live".to_vec(),
        };

        let mut delete_then_set = ReferenceModel::default();
        delete_then_set.append_batch(
            DEVICES[0],
            DeviceSeq(1),
            vec![(delete.clone(), Ver(1)), (set.clone(), Ver(2))],
        );
        assert!(delete_then_set.get_at(&row, AdmissionSeq(1)).is_some());

        let mut set_then_delete = ReferenceModel::default();
        set_then_delete.append_batch(
            DEVICES[0],
            DeviceSeq(1),
            vec![(set, Ver(1)), (delete, Ver(2))],
        );
        assert!(set_then_delete.get_at(&row, AdmissionSeq(1)).is_none());
    }

    #[test]
    fn history_versions_counts_and_foreign_heads_include_covering_ranges() {
        let db = key(&[b"db"]);
        let row = key(&[b"db", b"a"]);
        let query = Range::Prefix(row.clone());
        let mut model = ReferenceModel::default();
        model.append_batch(
            DEVICES[0],
            DeviceSeq(1),
            vec![(
                Mutation::Set {
                    key: row.clone(),
                    value: b"old".to_vec(),
                },
                Ver(100),
            )],
        );
        model.append_batch(
            DEVICES[1],
            DeviceSeq(1),
            vec![(
                Mutation::DeleteRange {
                    range: Range::Prefix(db),
                },
                Ver(101),
            )],
        );

        assert!(model.get_at(&row, AdmissionSeq(2)).is_none());
        assert_eq!(model.live_count(&query, AdmissionSeq(2)), 0);
        assert_eq!(model.max_ver(&query, AdmissionSeq(2)), Some(Ver(101)));
        assert_eq!(model.max_ver_for_key(&row, AdmissionSeq(2)), Some(Ver(101)));
        assert_eq!(
            model.max_admission_excluding(&query, DEVICES[0], AdmissionSeq(2)),
            Some(AdmissionSeq(2))
        );

        model.append_batch(
            DEVICES[0],
            DeviceSeq(2),
            vec![(
                Mutation::DeleteRange {
                    range: Range::Prefix(key(&[b"db", b"a", b"nested"])),
                },
                Ver(102),
            )],
        );
        assert_eq!(
            model.max_ver(&Range::Prefix(key(&[b"db"])), AdmissionSeq(3)),
            Some(Ver(102))
        );
        assert_eq!(
            model.max_admission_excluding(&query, DEVICES[0], AdmissionSeq(3)),
            Some(AdmissionSeq(2)),
            "own descendant history must not replace the foreign head"
        );
    }

    #[test]
    fn replay_and_stateless_reads_preserve_exact_sources_and_empty_batches() {
        let db = Range::Prefix(key(&[b"db"]));
        let child = Range::Prefix(key(&[b"db", b"a"]));
        let sibling = key(&[b"other", b"x"]);
        let mut model = ReferenceModel::default();
        model.append_batch(DEVICES[0], DeviceSeq(1), vec![]);
        model.append_batch(
            DEVICES[0],
            DeviceSeq(2),
            vec![(
                Mutation::Set {
                    key: sibling,
                    value: vec![1],
                },
                Ver(1),
            )],
        );
        model.append_batch(
            DEVICES[1],
            DeviceSeq(1),
            vec![(Mutation::DeleteRange { range: db }, Ver(2))],
        );

        let replay = model.replay(AdmissionSeq(0), AdmissionSeq(3));
        assert_eq!(replay.len(), 3);
        assert!(replay[0].entries.is_empty());
        let ModelRead {
            at,
            cut: RangeCut::Delta(delta),
        } = model.read(&child, Some(AdmissionSeq(1)))
        else {
            panic!("expected delta")
        };
        assert_eq!(at, AdmissionSeq(3));
        assert_eq!(delta.len(), 1);
        assert!(matches!(
            delta[0].device_entry.mutation,
            Mutation::DeleteRange { .. }
        ));
    }

    #[test]
    fn adjacent_range_deltas_compose_to_the_direct_delta() {
        let query = Range::Prefix(key(&[b"db"]));
        let mut model = ReferenceModel::default();
        for (index, mutation) in [
            Mutation::Set {
                key: key(&[b"db", b"a"]),
                value: vec![1],
            },
            Mutation::Set {
                key: key(&[b"other", b"ignored"]),
                value: vec![2],
            },
            Mutation::DeleteRange {
                range: Range::Prefix(key(&[b"db", b"a"])),
            },
            Mutation::Set {
                key: key(&[b"db", b"b"]),
                value: vec![4],
            },
        ]
        .into_iter()
        .enumerate()
        {
            model.append_batch(
                DEVICES[index % 2],
                DeviceSeq(index as u64 + 1),
                vec![(mutation, Ver(index as u64 + 1))],
            );
        }

        let delta = |after, through| {
            let RangeCut::Delta(entries) = model
                .read_at_cut(&query, Some(AdmissionSeq(after)), AdmissionSeq(through))
                .cut
            else {
                unreachable!()
            };
            entries
        };
        let direct = delta(0, 4);
        let mut composed = delta(0, 2);
        composed.extend(delta(2, 4));
        assert_eq!(composed, direct);
        assert_eq!(
            direct
                .iter()
                .map(|entry| entry.admission.order())
                .collect::<Vec<_>>(),
            vec![
                AdmissionTag {
                    admission_seq: AdmissionSeq(1),
                    op_index: 0
                }
                .order(),
                AdmissionTag {
                    admission_seq: AdmissionSeq(3),
                    op_index: 0
                }
                .order(),
                AdmissionTag {
                    admission_seq: AdmissionSeq(4),
                    op_index: 0
                }
                .order(),
            ]
        );
    }

    #[test]
    fn point_and_range_lease_conflicts_use_different_geometry() {
        let db = key(&[b"db"]);
        let row = key(&[b"db", b"a"]);
        let child_lease = key(&[b"db", b"a", b"child"]);
        assert!(conflicts_with_lease(
            &Mutation::Delete::<Vec<u8>> { key: row.clone() },
            &db
        ));
        assert!(!conflicts_with_lease(
            &Mutation::Delete::<Vec<u8>> { key: row },
            &child_lease
        ));
        assert!(conflicts_with_lease(
            &Mutation::DeleteRange::<Vec<u8>> {
                range: Range::Prefix(db)
            },
            &child_lease
        ));
        assert!(conflicts_with_lease(
            &Mutation::DeleteRange::<Vec<u8>> { range: Range::Full },
            &key(&[b"other"])
        ));
    }

    #[test]
    fn exhaustive_short_histories_match_eager_visibility() {
        const COMMANDS: u64 = 12;
        const STEPS: usize = 4;
        for encoded in 0..COMMANDS.pow(STEPS as u32) {
            let mut cursor = encoded;
            let mut model = ReferenceModel::default();
            let mut eager = BTreeMap::new();
            for step in 0..STEPS {
                let command = (cursor % COMMANDS) as u8;
                cursor /= COMMANDS;
                let mutation = operation(command % 6, step as u8 + 1);
                apply_eager(&mut eager, &mutation);
                model.append_batch(
                    DEVICES[(command / 6) as usize],
                    DeviceSeq(step as u64 + 1),
                    vec![(mutation, Ver(step as u64 + 1))],
                );

                assert_eq!(visible_map(&model, &Range::Full), eager);
                for range in [
                    Range::Prefix(key(&[b"db"])),
                    Range::Prefix(key(&[b"db", b"a"])),
                    Range::Prefix(key(&[b"db", b"b"])),
                ] {
                    let expected = eager
                        .iter()
                        .filter(|(key, _)| range.covers_key(key))
                        .map(|(key, value)| (key.clone(), value.clone()))
                        .collect::<BTreeMap<_, _>>();
                    assert_eq!(visible_map(&model, &range), expected);
                    assert_eq!(
                        model.live_count(&range, model.high_water()),
                        expected.len() as u64
                    );
                }
            }
        }
    }

    proptest! {
        #[test]
        fn randomized_commands_match_eager_replay(
            commands in prop::collection::vec((0u8..6, any::<bool>()), 0..80)
        ) {
            let mut model = ReferenceModel::default();
            let mut eager = BTreeMap::new();
            for (step, (kind, second_device)) in commands.into_iter().enumerate() {
                let mutation = operation(kind, step as u8);
                apply_eager(&mut eager, &mutation);
                model.append_batch(
                    DEVICES[second_device as usize],
                    DeviceSeq(step as u64 + 1),
                    vec![(mutation, Ver(step as u64 + 1))],
                );
                prop_assert_eq!(visible_map(&model, &Range::Full), eager.clone());
                prop_assert_eq!(
                    model.live_count(&Range::Full, model.high_water()),
                    eager.len() as u64
                );
            }
        }
    }
}
