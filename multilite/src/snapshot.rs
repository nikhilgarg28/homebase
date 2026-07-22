//! Logical transaction coordinates independent of the physical SQLite image.

#![cfg_attr(
    not(test),
    allow(
        dead_code,
        reason = "consumed by owned commit proposals in the next batch"
    )
)]

use homebase_client::meta::OplogCursors;
use homebase_core::tag::AdmissionSeq;

/// Monotone canonical SQLite commit coordinate.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct LocalGeneration(pub u64);

/// Complete logical transaction-start cut across SQLite and Homebase state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SnapshotDescriptor {
    pub local_generation: LocalGeneration,
    pub authority_applied_through: AdmissionSeq,
    pub submit_cursors: OplogCursors,
}

#[cfg(test)]
mod tests {
    use homebase_core::tag::DeviceSeq;

    use super::*;

    #[test]
    fn descriptor_separates_local_submit_and_authority_coordinates() {
        let cursors = OplogCursors {
            head: DeviceSeq(3),
            neck: DeviceSeq(5),
            tail: DeviceSeq(8),
        };
        let descriptor = SnapshotDescriptor {
            local_generation: LocalGeneration(41),
            authority_applied_through: AdmissionSeq(17),
            submit_cursors: cursors,
        };

        assert_eq!(descriptor.local_generation, LocalGeneration(41));
        assert_eq!(descriptor.authority_applied_through, AdmissionSeq(17));
        assert_eq!(descriptor.submit_cursors, cursors);
    }
}
