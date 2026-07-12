//! Device-authored and admitted entries.
//!
//! A [`DeviceEntry`] is the complete client-authenticated object: its seal
//! binds the mutation to every field in [`DeviceTag`]. An [`AdmittedEntry`]
//! wraps that object without changing it and adds only server-assigned
//! admission metadata.

use crate::key::Key;
use crate::range::Range;
use crate::seal::Seal;
use sha2::{Digest, Sha256};
use std::fmt;

/// Identifies a writing device: 16 opaque bytes, UUID-shaped.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DeviceId(pub [u8; 16]);

impl fmt::Debug for DeviceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "device:")?;
        for b in self.0 {
            write!(f, "{b:02x}")?;
        }
        Ok(())
    }
}

/// Client-assigned admission-batch sequence number, strictly increasing per
/// device and space.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DeviceSeq(pub u64);

/// Server-assigned total-order position of one admitted batch in a space.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct AdmissionSeq(pub u64);

/// Stable order of one operation within a space's admission history.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct AdmissionOrder {
    pub admission_seq: AdmissionSeq,
    pub op_index: u32,
}

/// Cumulative cryptographic checksum of one device's admitted batch stream
/// within a space. The all-zero value is the empty stream.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct DeviceChecksum(pub [u8; 32]);

impl DeviceChecksum {
    pub const EMPTY: Self = Self([0; 32]);

    pub(crate) fn hasher(&self) -> Sha256 {
        let mut hash = Sha256::new();
        hash.update(b"homebase.device-checksum.v1\0");
        hash.update(self.0);
        hash
    }
}

impl fmt::Debug for DeviceChecksum {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "checksum:")?;
        for byte in self.0 {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}

/// Client-computed per-key version.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Ver(pub u64);

/// Selects the value-encryption key used by a sealing scheme.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CipherEpoch(pub u64);

/// Opaque bytes carried by an admitted Set mutation. Clients normally place
/// ciphertext here; the kernel does not interpret or classify the bytes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OpaqueValue(pub Vec<u8>);

impl AsRef<[u8]> for OpaqueValue {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

/// A point or range mutation. Bare client mutations use plaintext `Vec<u8>`;
/// device/server entries use [`OpaqueValue`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Mutation<T = Vec<u8>> {
    Set { key: Key, value: T },
    Delete { key: Key },
    DeleteRange { range: Range },
}

impl<T> Mutation<T> {
    /// Returns the point key, or `None` for a range mutation.
    pub fn point_key(&self) -> Option<&Key> {
        match self {
            Self::Set { key, .. } | Self::Delete { key } => Some(key),
            Self::DeleteRange { .. } => None,
        }
    }

    /// Point-key convenience for code paths that have already rejected
    /// range mutations.
    pub fn key(&self) -> &Key {
        self.point_key().expect("range mutation has no point key")
    }

    pub fn range(&self) -> Option<&Range> {
        match self {
            Self::DeleteRange { range } => Some(range),
            Self::Set { .. } | Self::Delete { .. } => None,
        }
    }

    pub fn is_set(&self) -> bool {
        matches!(self, Self::Set { .. })
    }

    pub fn is_delete(&self) -> bool {
        matches!(self, Self::Delete { .. })
    }

    pub fn is_delete_range(&self) -> bool {
        matches!(self, Self::DeleteRange { .. })
    }
}

/// Every client-minted field authenticated as AEAD associated data.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DeviceTag {
    pub device: DeviceId,
    pub device_seq: DeviceSeq,
    pub ver: Ver,
    pub cipher_epoch: CipherEpoch,
}

/// One complete device-authenticated data mutation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeviceEntry<T = OpaqueValue> {
    pub mutation: Mutation<T>,
    pub tag: DeviceTag,
    pub seal: Seal,
}

impl<T> DeviceEntry<T> {
    pub fn point_key(&self) -> Option<&Key> {
        self.mutation.point_key()
    }

    pub fn key(&self) -> &Key {
        self.point_key().expect("range entry has no point key")
    }

    pub fn ver(&self) -> Ver {
        self.tag.ver
    }
}

/// Server-minted metadata deliberately excluded from value AAD.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AdmissionTag {
    pub admission_seq: AdmissionSeq,
    pub op_index: u32,
}

impl AdmissionTag {
    pub const fn order(self) -> AdmissionOrder {
        AdmissionOrder {
            admission_seq: self.admission_seq,
            op_index: self.op_index,
        }
    }
}

/// An unchanged device-authenticated entry plus authority metadata.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AdmittedEntry<T = OpaqueValue> {
    pub device_entry: DeviceEntry<T>,
    pub admission: AdmissionTag,
}

impl<T> AdmittedEntry<T> {
    pub fn point_key(&self) -> Option<&Key> {
        self.device_entry.point_key()
    }

    pub fn key(&self) -> &Key {
        self.point_key().expect("range entry has no point key")
    }

    pub fn ver(&self) -> Ver {
        self.device_entry.ver()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn admission_order_is_lexicographic() {
        let first = AdmissionOrder {
            admission_seq: AdmissionSeq(7),
            op_index: 0,
        };
        let second = AdmissionOrder {
            admission_seq: AdmissionSeq(7),
            op_index: 1,
        };
        let next_batch = AdmissionOrder {
            admission_seq: AdmissionSeq(8),
            op_index: 0,
        };

        assert!(first < second);
        assert!(second < next_batch);
    }
}
