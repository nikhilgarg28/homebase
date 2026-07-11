//! Device-authored and admitted entries.
//!
//! A [`DeviceEntry`] is the complete client-authenticated object: its seal
//! binds the mutation to every field in [`DeviceTag`]. An [`AdmittedEntry`]
//! wraps that object without changing it and adds only server-assigned
//! admission metadata.

use crate::key::Key;
use crate::seal::Seal;
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

/// Client-computed per-key version.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Ver(pub u64);

/// Selects the value-encryption key used by a sealing scheme.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CipherEpoch(pub u64);

/// Opaque encrypted bytes carried by a Set mutation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Ciphertext(pub Vec<u8>);

impl AsRef<[u8]> for Ciphertext {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

/// A Set or Delete shape. Bare client mutations use plaintext `Vec<u8>`;
/// device/server entries use [`Ciphertext`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Mutation<T = Vec<u8>> {
    Set { key: Key, value: T },
    Delete { key: Key },
}

impl<T> Mutation<T> {
    pub fn key(&self) -> &Key {
        match self {
            Self::Set { key, .. } | Self::Delete { key } => key,
        }
    }

    pub fn is_set(&self) -> bool {
        matches!(self, Self::Set { .. })
    }

    pub fn is_delete(&self) -> bool {
        matches!(self, Self::Delete { .. })
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
pub struct DeviceEntry<T = Ciphertext> {
    pub mutation: Mutation<T>,
    pub tag: DeviceTag,
    pub seal: Seal,
}

impl<T> DeviceEntry<T> {
    pub fn key(&self) -> &Key {
        self.mutation.key()
    }

    pub fn ver(&self) -> Ver {
        self.tag.ver
    }
}

/// Server-minted metadata deliberately excluded from value AAD.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AdmissionTag {
    pub admission_seq: AdmissionSeq,
}

/// An unchanged device-authenticated entry plus authority metadata.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AdmittedEntry<T = Ciphertext> {
    pub device_entry: DeviceEntry<T>,
    pub admission: AdmissionTag,
}

impl<T> AdmittedEntry<T> {
    pub fn key(&self) -> &Key {
        self.device_entry.key()
    }

    pub fn ver(&self) -> Ver {
        self.device_entry.ver()
    }
}
