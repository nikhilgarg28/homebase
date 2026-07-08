//! Client-computed AEAD seal metadata.
//!
//! A [`Seal`] carries the scheme byte, nonce, and authentication tag for a
//! client-side value encryption operation. The kernel stores and transports
//! seals opaquely; clients bind them to operation kind, key, version, and
//! device context in AEAD associated data.
//!
//! Because the AEAD tag is stored separately from the ciphertext, clients
//! should not encrypt raw Set payloads directly. The value cipher should wrap
//! every Set in a non-empty plaintext frame before encryption so an empty
//! logical value does not produce an empty ciphertext.

use std::fmt;

/// Number of bytes in the AEAD nonce stored in a [`Seal`].
pub const SEAL_NONCE_LEN: usize = 24;

/// Number of bytes in the AEAD authentication tag stored in a [`Seal`].
pub const SEAL_AEAD_TAG_LEN: usize = 16;

/// Number of fixed bytes in the seal wire header before any scheme payload.
pub const SEAL_FIXED_HEADER_LEN: usize = 1 + SEAL_NONCE_LEN + SEAL_AEAD_TAG_LEN;

/// Sealing/encryption scheme for a value operation.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum SealScheme {
    /// Initial XChaCha20-Poly1305 value sealing scheme.
    AeadV1 = 0,
}

impl SealScheme {
    /// Converts the scheme to its stable wire byte.
    pub const fn to_u8(self) -> u8 {
        match self {
            Self::AeadV1 => 0,
        }
    }

    /// Converts a stable wire byte to a known scheme.
    pub const fn from_u8(value: u8) -> Result<Self, UnknownSealScheme> {
        match value {
            0 => Ok(Self::AeadV1),
            other => Err(UnknownSealScheme(other)),
        }
    }
}

impl From<SealScheme> for u8 {
    fn from(value: SealScheme) -> Self {
        value.to_u8()
    }
}

impl TryFrom<u8> for SealScheme {
    type Error = UnknownSealScheme;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        Self::from_u8(value)
    }
}

/// Unknown value sealing scheme byte.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct UnknownSealScheme(pub u8);

impl fmt::Display for UnknownSealScheme {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "unknown seal scheme {}", self.0)
    }
}

impl std::error::Error for UnknownSealScheme {}

/// Metadata for a client-side AEAD value operation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Seal {
    /// Sealing/encryption scheme. Encodes to one byte on the wire.
    pub scheme: SealScheme,
    /// AEAD nonce.
    pub nonce: [u8; SEAL_NONCE_LEN],
    /// AEAD authentication tag.
    pub aead: [u8; SEAL_AEAD_TAG_LEN],
    /// Scheme-specific opaque extension bytes. Empty for [`SealScheme::AeadV1`].
    pub payload: Vec<u8>,
}

impl Seal {
    /// Empty scheme-0 seal. This is useful for tests and for the legacy
    /// PutEntry-to-BatchOp bridge until the value cipher emits real seals.
    pub fn empty_aead_v1() -> Self {
        Self {
            scheme: SealScheme::AeadV1,
            nonce: [0; SEAL_NONCE_LEN],
            aead: [0; SEAL_AEAD_TAG_LEN],
            payload: Vec::new(),
        }
    }

    /// Validates that the opaque payload is allowed for this scheme.
    pub fn validate_payload(&self) -> Result<(), SealPayloadError> {
        match self.scheme {
            SealScheme::AeadV1 if self.payload.is_empty() => Ok(()),
            SealScheme::AeadV1 => Err(SealPayloadError::UnexpectedPayload {
                scheme: self.scheme,
                payload_len: self.payload.len(),
            }),
        }
    }
}

/// Invalid scheme-specific seal payload.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SealPayloadError {
    UnexpectedPayload {
        scheme: SealScheme,
        payload_len: usize,
    },
}

impl fmt::Display for SealPayloadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnexpectedPayload {
                scheme,
                payload_len,
            } => write!(
                f,
                "seal scheme {scheme:?} does not allow payload of {payload_len} bytes"
            ),
        }
    }
}

impl std::error::Error for SealPayloadError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scheme_roundtrips_wire_byte() {
        assert_eq!(SealScheme::AeadV1.to_u8(), 0);
        assert_eq!(u8::from(SealScheme::AeadV1), 0);
        assert_eq!(SealScheme::from_u8(0).unwrap(), SealScheme::AeadV1);
        assert_eq!(SealScheme::try_from(0).unwrap(), SealScheme::AeadV1);
    }

    #[test]
    fn unknown_scheme_rejects() {
        assert_eq!(SealScheme::from_u8(1), Err(UnknownSealScheme(1)));
        assert_eq!(SealScheme::try_from(255), Err(UnknownSealScheme(255)));
    }

    #[test]
    fn seal_has_fixed_wire_header_size() {
        let seal = Seal {
            scheme: SealScheme::AeadV1,
            nonce: [7; SEAL_NONCE_LEN],
            aead: [9; SEAL_AEAD_TAG_LEN],
            payload: Vec::new(),
        };

        assert_eq!(SEAL_FIXED_HEADER_LEN, 41);
        assert_eq!(
            SEAL_FIXED_HEADER_LEN,
            1 + seal.nonce.len() + seal.aead.len()
        );
    }

    #[test]
    fn aead_v1_requires_empty_payload() {
        let seal = Seal {
            scheme: SealScheme::AeadV1,
            nonce: [7; SEAL_NONCE_LEN],
            aead: [9; SEAL_AEAD_TAG_LEN],
            payload: Vec::new(),
        };
        assert_eq!(seal.validate_payload(), Ok(()));

        let with_payload = Seal {
            payload: vec![1],
            ..seal
        };
        assert_eq!(
            with_payload.validate_payload(),
            Err(SealPayloadError::UnexpectedPayload {
                scheme: SealScheme::AeadV1,
                payload_len: 1
            })
        );
    }
}
