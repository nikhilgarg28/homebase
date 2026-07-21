//! Isolation policy and transaction conflict-footprint planning.

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex, MutexGuard};

use homebase_core::key::Key;
use homebase_core::messages::RangeAssert;
use homebase_core::tag::AdmissionSeq;

/// Conflict guarantees requested for admitted Multilite transactions.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum IsolationLevel {
    /// Validate writes and mandatory constraints, but not ordinary reads.
    Snapshot,
    /// Additionally validate every logical read range observed by the update.
    #[default]
    Serializable,
}

/// Options that override one managed update's open-time defaults.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct UpdateOptions {
    isolation: IsolationLevel,
}

impl UpdateOptions {
    /// Configure one managed update with an explicit isolation level.
    pub const fn new(isolation: IsolationLevel) -> Self {
        Self { isolation }
    }

    /// Isolation level used to plan this update's conflict assertions.
    pub const fn isolation_level(self) -> IsolationLevel {
        self.isolation
    }
}

/// Logical conflicts accumulated by one Multilite transaction.
///
/// Writes and constraints are mandatory at every isolation level. Ordinary
/// reads become assertions only under serializable isolation.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ConflictFootprint {
    writes: PrefixSet,
    constraints: PrefixSet,
    reads: PrefixSet,
}

/// Shared read-prefix sink for every statement in one managed update.
///
/// The vtable layer will clone this handle into prepared statements and record
/// logical ranges as SQLite executes them. Keeping this read-only by type
/// prevents statement execution from contributing mandatory write guards.
#[derive(Clone, Debug, Default)]
pub struct ReadTrace {
    footprint: Arc<Mutex<ConflictFootprint>>,
}

impl ReadTrace {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record(&self, key: Key) {
        lock(&self.footprint).add_read(key);
    }

    pub fn footprint(&self) -> ConflictFootprint {
        lock(&self.footprint).clone()
    }
}

impl ConflictFootprint {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_write(&mut self, key: Key) {
        self.writes.insert(key);
    }

    pub fn add_constraint(&mut self, key: Key) {
        self.constraints.insert(key);
    }

    pub fn add_read(&mut self, key: Key) {
        self.reads.insert(key);
    }

    pub fn extend(&mut self, other: Self) {
        self.writes.extend(other.writes);
        self.constraints.extend(other.constraints);
        self.reads.extend(other.reads);
    }

    #[cfg(test)]
    pub fn writes(&self) -> &BTreeSet<Key> {
        self.writes.as_set()
    }

    #[cfg(test)]
    pub fn constraints(&self) -> &BTreeSet<Key> {
        self.constraints.as_set()
    }

    #[cfg(test)]
    pub fn reads(&self) -> &BTreeSet<Key> {
        self.reads.as_set()
    }

    /// Merge typed antichains and bind them to one authority frontier.
    pub fn plan(self, isolation: IsolationLevel, upto: AdmissionSeq) -> Vec<RangeAssert> {
        let mut selected = self.writes;
        selected.extend(self.constraints);
        if isolation == IsolationLevel::Serializable {
            selected.extend(self.reads);
        }
        selected
            .into_keys()
            .into_iter()
            .map(|prefix| RangeAssert { prefix, upto })
            .collect()
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Sorted component-prefix antichain maintained as keys arrive.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct PrefixSet {
    keys: BTreeSet<Key>,
}

impl PrefixSet {
    fn insert(&mut self, key: Key) {
        if self
            .keys
            .range(..=&key)
            .next_back()
            .is_some_and(|prefix| key.starts_with(prefix))
        {
            return;
        }

        let descendants = self
            .keys
            .range(key.clone()..)
            .take_while(|candidate| candidate.starts_with(&key))
            .cloned()
            .collect::<Vec<_>>();
        for descendant in descendants {
            self.keys.remove(&descendant);
        }
        self.keys.insert(key);
    }

    fn into_keys(self) -> Vec<Key> {
        self.keys.into_iter().collect()
    }

    fn extend(&mut self, other: Self) {
        for key in other.keys {
            self.insert(key);
        }
    }

    #[cfg(test)]
    fn as_set(&self) -> &BTreeSet<Key> {
        &self.keys
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(parts: &[&[u8]]) -> Key {
        Key::from_bytes(parts.iter().copied()).unwrap()
    }

    #[test]
    fn mandatory_guards_apply_at_every_isolation_level() {
        let write = key(&[b"tables", b"one", b"rows", b"7"]);
        let constraint = key(&[b"tables", b"one", b"write-revision"]);
        let read = key(&[b"tables", b"one", b"rows"]);
        let mut footprint = ConflictFootprint::new();
        footprint.add_write(write.clone());
        footprint.add_constraint(constraint.clone());
        footprint.add_read(read.clone());

        let snapshot = footprint
            .clone()
            .plan(IsolationLevel::Snapshot, AdmissionSeq(17));
        assert_eq!(
            snapshot,
            vec![
                RangeAssert {
                    prefix: write.clone(),
                    upto: AdmissionSeq(17),
                },
                RangeAssert {
                    prefix: constraint.clone(),
                    upto: AdmissionSeq(17),
                },
            ]
        );

        let serializable = footprint.plan(IsolationLevel::Serializable, AdmissionSeq(17));
        assert_eq!(serializable.len(), 2);
        assert_eq!(serializable[0].prefix, read);
        for mandatory in [&write, &constraint] {
            assert!(
                serializable
                    .iter()
                    .any(|assertion| mandatory.starts_with(&assertion.prefix))
            );
        }
        assert!(
            serializable
                .iter()
                .all(|assertion| assertion.upto == AdmissionSeq(17))
        );
    }

    #[test]
    fn planning_merges_prepruned_mandatory_categories() {
        let table = key(&[b"tables", b"one"]);
        let row = key(&[b"tables", b"one", b"rows", b"7"]);
        let other = key(&[b"tables", b"two"]);
        let mut footprint = ConflictFootprint::new();
        footprint.add_write(row);
        footprint.add_write(other.clone());
        footprint.add_constraint(table.clone());
        footprint.add_constraint(other.clone());

        assert_eq!(footprint.writes().len(), 2);
        assert_eq!(
            footprint.constraints(),
            &BTreeSet::from([table.clone(), other.clone()])
        );

        assert_eq!(
            footprint.plan(IsolationLevel::Snapshot, AdmissionSeq(9)),
            vec![
                RangeAssert {
                    prefix: table,
                    upto: AdmissionSeq(9),
                },
                RangeAssert {
                    prefix: other,
                    upto: AdmissionSeq(9),
                },
            ]
        );
    }

    #[test]
    fn each_typed_prefix_set_is_pruned_as_contributions_arrive() {
        let table = key(&[b"tables", b"one"]);
        let first = key(&[b"tables", b"one", b"rows", b"7"]);
        let second = key(&[b"tables", b"one", b"rows", b"9"]);
        let mut footprint = ConflictFootprint::new();
        footprint.add_write(first);
        footprint.add_write(second);
        footprint.add_write(table.clone());
        footprint.add_write(table.clone());

        assert_eq!(footprint.writes(), &BTreeSet::from([table]));
    }

    #[test]
    fn cloned_read_traces_share_one_eagerly_pruned_antichain() {
        let trace = ReadTrace::new();
        let statement_trace = trace.clone();
        let table = key(&[b"tables", b"one", b"rows"]);
        statement_trace.record(key(&[b"tables", b"one", b"rows", b"7"]));
        statement_trace.record(key(&[b"tables", b"one", b"rows", b"9"]));
        statement_trace.record(table.clone());

        assert_eq!(trace.footprint().reads(), &BTreeSet::from([table]));
    }
}
