//! Logical transaction coordinates independent of the physical SQLite image.

use homebase_client::meta::OplogCursors;
use homebase_core::reader::Reader;
use homebase_core::tag::AdmissionSeq;
use homebase_core::writer::Writer;

const SNAPSHOT_FRAME_VERSION: u8 = 1;

/// Monotone coordinate for every canonical SQLite materialization transition.
///
/// This is deliberately not a Homebase `DeviceSeq`: remote applies and local
/// rejection repairs can change canonical SQLite without creating a local
/// submission.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CommitSeq(pub u64);

/// Complete logical transaction-start cut across SQLite and Homebase state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SnapshotDescriptor {
    /// Canonical SQLite image observed when the branch began.
    pub commit_seq: CommitSeq,
    pub authority_applied_through: AdmissionSeq,
    pub submit_cursors: OplogCursors,
}

impl SnapshotDescriptor {
    /// Encode one complete logical transaction-start frontier.
    #[allow(
        dead_code,
        reason = "persisted by owned proposals before actor integration"
    )]
    pub fn encode(self) -> Vec<u8> {
        let mut writer = Writer::with_capacity(42);
        writer.u8(SNAPSHOT_FRAME_VERSION);
        writer.u64(self.commit_seq.0);
        writer.u64(self.authority_applied_through.0);
        writer.bytes(&self.submit_cursors.encode());
        writer.finish()
    }

    /// Decode and validate one complete logical transaction-start frontier.
    pub fn decode(frame: &[u8]) -> Result<Self, SnapshotCodecError> {
        let mut reader = Reader::new(frame);
        if reader.u8() != Some(SNAPSHOT_FRAME_VERSION) {
            return Err(SnapshotCodecError::UnknownVersion);
        }
        let commit_seq = CommitSeq(reader.u64().ok_or(SnapshotCodecError::Truncated)?);
        let authority_applied_through =
            AdmissionSeq(reader.u64().ok_or(SnapshotCodecError::Truncated)?);
        let submit_cursors =
            OplogCursors::decode(reader.rest()).ok_or(SnapshotCodecError::InvalidSubmitCursors)?;
        if submit_cursors.head.0 == 0
            || submit_cursors.head > submit_cursors.neck
            || submit_cursors.neck > submit_cursors.tail
        {
            return Err(SnapshotCodecError::InvalidSubmitCursors);
        }
        Ok(Self {
            commit_seq,
            authority_applied_through,
            submit_cursors,
        })
    }
}

/// Failure to decode a logical snapshot descriptor.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SnapshotCodecError {
    UnknownVersion,
    Truncated,
    InvalidSubmitCursors,
}

impl std::fmt::Display for SnapshotCodecError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnknownVersion => formatter.write_str("unknown snapshot descriptor version"),
            Self::Truncated => formatter.write_str("snapshot descriptor is truncated"),
            Self::InvalidSubmitCursors => {
                formatter.write_str("snapshot descriptor has invalid submit cursors")
            }
        }
    }
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
            commit_seq: CommitSeq(41),
            authority_applied_through: AdmissionSeq(17),
            submit_cursors: cursors,
        };

        assert_eq!(descriptor.commit_seq, CommitSeq(41));
        assert_eq!(descriptor.authority_applied_through, AdmissionSeq(17));
        assert_eq!(descriptor.submit_cursors, cursors);
        assert_eq!(
            SnapshotDescriptor::decode(&descriptor.encode()),
            Ok(descriptor)
        );
    }

    #[test]
    fn descriptor_rejects_malformed_and_reversed_submit_frontiers() {
        assert_eq!(
            SnapshotDescriptor::decode(&[]),
            Err(SnapshotCodecError::UnknownVersion)
        );

        let descriptor = SnapshotDescriptor {
            commit_seq: CommitSeq(1),
            authority_applied_through: AdmissionSeq(2),
            submit_cursors: OplogCursors {
                head: DeviceSeq(4),
                neck: DeviceSeq(3),
                tail: DeviceSeq(5),
            },
        };
        assert_eq!(
            SnapshotDescriptor::decode(&descriptor.encode()),
            Err(SnapshotCodecError::InvalidSubmitCursors)
        );
    }
}
