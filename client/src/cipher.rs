//! Client-side key/value cipher.
//!
//! This is the crypto boundary below identity/link bootstrap and above the
//! kernel verbs. It intentionally takes all entropy as input: tests and the
//! deterministic simulator can drive it without ambient RNG.
//!
//! Encrypted Set values use a non-empty versioned plaintext frame, then store
//! the XChaCha20-Poly1305 ciphertext separately from [`Seal`]'s nonce and
//! detached tag. Delete authenticates empty plaintext and carries no
//! ciphertext. Both bind the scheme, operation kind, cipher epoch, anonymized
//! key, device, device sequence, and value version as AEAD associated data.
//! Admission sequence is omitted because the server assigns it after sealing.

use chacha20poly1305::aead::{AeadInOut, KeyInit};
use chacha20poly1305::{Key as AeadKey, Tag as AeadTag, XChaCha20Poly1305, XNonce};
use hkdf::Hkdf;
use hmac::{Hmac, Mac};
use homebase_core::key::{Key, KeyComponent, KeyError};
use homebase_core::messages::Range;
use homebase_core::seal::{Seal, SealScheme};
use homebase_core::space::SpaceId;
use homebase_core::tag::{AdmittedEntry, Ciphertext, DeviceEntry, DeviceTag, Mutation};
use sha2::Sha256;
use std::collections::BTreeMap;
use std::fmt;

type HmacSha256 = Hmac<Sha256>;

pub const KEY_LEN: usize = 32;
pub const VALUE_NONCE_LEN: usize = 24;
pub const V1_CIPHER_EPOCH: u64 = 0;

const SPACE_ID_INFO: &[u8] = b"homebase:space-id:v1";
const CHILD_KEY_INFO: &[u8] = b"homebase:name-child:v1";
const COMPONENT_INFO: &[u8] = b"homebase:name-component:v1";
const SEAL_AAD_PREFIX: &[u8] = b"homebase:seal-aad:v1";
const SET_FRAME_V1: u8 = 1;
const SET_OP_KIND: u8 = 1;
const DELETE_OP_KIND: u8 = 2;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NameKey(pub [u8; KEY_LEN]);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SpaceKey(pub [u8; KEY_LEN]);

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SpaceEnvelope {
    /// Encrypted spaces commit to their id through the name key and carry
    /// value keys by epoch. V1 uses epoch 0 only.
    Encrypted {
        name_key: NameKey,
        space_keys: BTreeMap<u64, SpaceKey>,
    },
    /// Plaintext spaces do no key or value transformation. The asserted id's
    /// integrity belongs to the envelope deliverer.
    Plaintext { space_id: SpaceId },
}

#[derive(Clone, Debug)]
pub struct SpaceCipher {
    space_id: SpaceId,
    mode: CipherMode,
}

#[derive(Clone, Debug)]
enum CipherMode {
    Plaintext,
    Encrypted {
        name_key: NameKey,
        space_keys: BTreeMap<u64, SpaceKey>,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ValueNonce(pub [u8; VALUE_NONCE_LEN]);

pub trait NonceSource {
    fn next_nonce(&mut self) -> Result<ValueNonce, String>;
}

#[derive(Clone, Copy, Debug, Default)]
pub struct SystemNonceSource;

impl NonceSource for SystemNonceSource {
    fn next_nonce(&mut self) -> Result<ValueNonce, String> {
        let mut bytes = [0u8; VALUE_NONCE_LEN];
        getrandom::fill(&mut bytes).map_err(|err| err.to_string())?;
        Ok(ValueNonce(bytes))
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CipherError {
    MissingSpaceKey { epoch: u64 },
    SpaceIdMismatch { expected: SpaceId, derived: SpaceId },
    InvalidKey(KeyError),
    MalformedSealedValue,
    MalformedEnvelopeRecord,
    DecryptFailed,
}

impl fmt::Display for CipherError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingSpaceKey { epoch } => write!(f, "missing space key for epoch {epoch}"),
            Self::SpaceIdMismatch { expected, derived } => {
                write!(
                    f,
                    "space id mismatch: expected {expected:?}, derived {derived:?}"
                )
            }
            Self::InvalidKey(err) => write!(f, "{err}"),
            Self::MalformedSealedValue => write!(f, "malformed sealed value"),
            Self::MalformedEnvelopeRecord => write!(f, "malformed envelope record"),
            Self::DecryptFailed => write!(f, "value decryption failed"),
        }
    }
}

impl std::error::Error for CipherError {}

impl From<KeyError> for CipherError {
    fn from(err: KeyError) -> Self {
        Self::InvalidKey(err)
    }
}

const ENVELOPE_RECORD_VERSION: u8 = 1;
const ENVELOPE_KIND_ENCRYPTED: u8 = 0;
const ENVELOPE_KIND_PLAINTEXT: u8 = 1;

impl SpaceEnvelope {
    /// Mint a fresh encrypted envelope from caller-supplied random key material.
    pub fn mint(name_key: NameKey, space_key: SpaceKey) -> Self {
        Self::encrypted(name_key, space_key)
    }

    pub fn encrypted(name_key: NameKey, space_key: SpaceKey) -> Self {
        Self::encrypted_with_epochs(name_key, [(V1_CIPHER_EPOCH, space_key)])
    }

    pub fn encrypted_with_epochs(
        name_key: NameKey,
        space_keys: impl IntoIterator<Item = (u64, SpaceKey)>,
    ) -> Self {
        Self::Encrypted {
            name_key,
            space_keys: space_keys.into_iter().collect(),
        }
    }

    pub fn plaintext(space_id: SpaceId) -> Self {
        Self::Plaintext { space_id }
    }

    pub fn space_id(&self) -> SpaceId {
        match self {
            Self::Encrypted { name_key, .. } => derive_space_id(*name_key),
            Self::Plaintext { space_id } => *space_id,
        }
    }

    pub fn open(&self) -> Result<SpaceCipher, CipherError> {
        self.open_expected(self.space_id())
    }

    pub fn open_expected(&self, expected: SpaceId) -> Result<SpaceCipher, CipherError> {
        let derived = self.space_id();
        if derived != expected {
            return Err(CipherError::SpaceIdMismatch { expected, derived });
        }
        match self {
            Self::Encrypted {
                name_key,
                space_keys,
            } => {
                if !space_keys.contains_key(&V1_CIPHER_EPOCH) {
                    return Err(CipherError::MissingSpaceKey {
                        epoch: V1_CIPHER_EPOCH,
                    });
                }
                Ok(SpaceCipher {
                    space_id: derived,
                    mode: CipherMode::Encrypted {
                        name_key: *name_key,
                        space_keys: space_keys.clone(),
                    },
                })
            }
            Self::Plaintext { .. } => Ok(SpaceCipher {
                space_id: derived,
                mode: CipherMode::Plaintext,
            }),
        }
    }

    /// Persisted form for [`crate::meta::CodecRecord::sealed`].
    pub fn encode(&self) -> Vec<u8> {
        match self {
            Self::Encrypted {
                name_key,
                space_keys,
            } => {
                let count = space_keys.len().min(u16::MAX as usize) as u16;
                let mut out =
                    Vec::with_capacity(1 + 1 + KEY_LEN + 2 + space_keys.len() * (8 + KEY_LEN));
                out.push(ENVELOPE_RECORD_VERSION);
                out.push(ENVELOPE_KIND_ENCRYPTED);
                out.extend_from_slice(&name_key.0);
                out.extend_from_slice(&count.to_be_bytes());
                for (epoch, key) in space_keys {
                    out.extend_from_slice(&epoch.to_be_bytes());
                    out.extend_from_slice(&key.0);
                }
                out
            }
            Self::Plaintext { space_id } => {
                let mut out = Vec::with_capacity(1 + 1 + 16);
                out.push(ENVELOPE_RECORD_VERSION);
                out.push(ENVELOPE_KIND_PLAINTEXT);
                out.extend_from_slice(&space_id.0);
                out
            }
        }
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, CipherError> {
        let Some((&[version, kind], rest)) = bytes.split_first_chunk() else {
            return Err(CipherError::MalformedEnvelopeRecord);
        };
        if version != ENVELOPE_RECORD_VERSION {
            return Err(CipherError::MalformedEnvelopeRecord);
        }
        match kind {
            ENVELOPE_KIND_ENCRYPTED => {
                if rest.len() < KEY_LEN + 2 {
                    return Err(CipherError::MalformedEnvelopeRecord);
                }
                let mut name_bytes = [0u8; KEY_LEN];
                name_bytes.copy_from_slice(&rest[..KEY_LEN]);
                let count =
                    u16::from_be_bytes(rest[KEY_LEN..KEY_LEN + 2].try_into().unwrap()) as usize;
                let mut offset = KEY_LEN + 2;
                let mut space_keys = BTreeMap::new();
                for _ in 0..count {
                    if rest.len() < offset + 8 + KEY_LEN {
                        return Err(CipherError::MalformedEnvelopeRecord);
                    }
                    let epoch = u64::from_be_bytes(rest[offset..offset + 8].try_into().unwrap());
                    offset += 8;
                    let mut key_bytes = [0u8; KEY_LEN];
                    key_bytes.copy_from_slice(&rest[offset..offset + KEY_LEN]);
                    offset += KEY_LEN;
                    space_keys.insert(epoch, SpaceKey(key_bytes));
                }
                if offset != rest.len() {
                    return Err(CipherError::MalformedEnvelopeRecord);
                }
                Ok(Self::Encrypted {
                    name_key: NameKey(name_bytes),
                    space_keys,
                })
            }
            ENVELOPE_KIND_PLAINTEXT => {
                if rest.len() != 16 {
                    return Err(CipherError::MalformedEnvelopeRecord);
                }
                let mut id = [0u8; 16];
                id.copy_from_slice(rest);
                Ok(Self::Plaintext {
                    space_id: SpaceId(id),
                })
            }
            _ => Err(CipherError::MalformedEnvelopeRecord),
        }
    }
}

impl SpaceCipher {
    pub fn space_id(&self) -> SpaceId {
        self.space_id
    }

    pub fn is_plaintext(&self) -> bool {
        matches!(self.mode, CipherMode::Plaintext)
    }

    pub fn encode_key(&self, key: &Key) -> Result<Key, CipherError> {
        match &self.mode {
            CipherMode::Plaintext => Ok(key.clone()),
            CipherMode::Encrypted { name_key, .. } => encode_name_key(*name_key, key),
        }
    }

    pub fn encode_range(&self, range: &Range) -> Result<Range, CipherError> {
        match range {
            Range::Full => Ok(Range::Full),
            Range::Prefix(prefix) => Ok(Range::Prefix(self.encode_key(prefix)?)),
        }
    }

    /// Seal one stamped mutation whose key is already anonymized.
    ///
    /// Plaintext spaces retain the same Set/Delete wire shapes but pass Set
    /// bytes through without the encrypted-value frame.
    pub fn encode_device_entry(
        &self,
        mutation: Mutation,
        tag: DeviceTag,
        nonce: ValueNonce,
    ) -> Result<DeviceEntry, CipherError> {
        let (mutation, seal) = match mutation {
            Mutation::Set { key, value } => {
                let framed = match self.mode {
                    CipherMode::Plaintext => value,
                    CipherMode::Encrypted { .. } => {
                        let mut framed = Vec::with_capacity(value.len() + 1);
                        framed.push(SET_FRAME_V1);
                        framed.extend_from_slice(&value);
                        framed
                    }
                };
                let (seal, ciphertext) =
                    self.seal_detached(&key, SET_OP_KIND, framed, tag, nonce)?;
                (
                    Mutation::Set {
                        key,
                        value: Ciphertext(ciphertext),
                    },
                    seal,
                )
            }
            Mutation::Delete { key } => {
                let (seal, ciphertext) =
                    self.seal_detached(&key, DELETE_OP_KIND, Vec::new(), tag, nonce)?;
                debug_assert!(ciphertext.is_empty());
                (Mutation::Delete { key }, seal)
            }
        };
        Ok(DeviceEntry {
            mutation,
            tag,
            seal,
        })
    }

    /// Authenticate and open one server-admitted entry.
    pub fn open_admitted_entry(
        &self,
        entry: &AdmittedEntry,
    ) -> Result<AdmittedEntry<Vec<u8>>, CipherError> {
        let device = &entry.device_entry;
        device
            .seal
            .validate_payload()
            .map_err(|_| CipherError::MalformedSealedValue)?;
        let mutation = match (&self.mode, &device.mutation) {
            (CipherMode::Plaintext, Mutation::Set { key, value }) => Mutation::Set {
                key: key.clone(),
                value: value.0.clone(),
            },
            (CipherMode::Plaintext, Mutation::Delete { key }) => {
                Mutation::Delete { key: key.clone() }
            }
            (CipherMode::Encrypted { .. }, Mutation::Set { key, value }) => {
                let plaintext = self.open_detached(
                    key,
                    SET_OP_KIND,
                    value.0.clone(),
                    &device.seal,
                    device.tag,
                )?;
                let Some((&SET_FRAME_V1, value)) = plaintext.split_first() else {
                    return Err(CipherError::MalformedSealedValue);
                };
                Mutation::Set {
                    key: key.clone(),
                    value: value.to_vec(),
                }
            }
            (CipherMode::Encrypted { .. }, Mutation::Delete { key }) => {
                let plaintext =
                    self.open_detached(key, DELETE_OP_KIND, Vec::new(), &device.seal, device.tag)?;
                if !plaintext.is_empty() {
                    return Err(CipherError::MalformedSealedValue);
                }
                Mutation::Delete { key: key.clone() }
            }
        };
        Ok(AdmittedEntry {
            device_entry: DeviceEntry {
                mutation,
                tag: device.tag,
                seal: device.seal.clone(),
            },
            admission: entry.admission,
        })
    }

    fn seal_detached(
        &self,
        encoded_key: &Key,
        op_kind: u8,
        mut plaintext: Vec<u8>,
        context: DeviceTag,
        nonce: ValueNonce,
    ) -> Result<(Seal, Vec<u8>), CipherError> {
        match &self.mode {
            CipherMode::Plaintext => Ok((Seal::empty_aead_v1(), plaintext)),
            CipherMode::Encrypted { space_keys, .. } => {
                let key = space_keys.get(&context.cipher_epoch.0).ok_or(
                    CipherError::MissingSpaceKey {
                        epoch: context.cipher_epoch.0,
                    },
                )?;
                let scheme = SealScheme::AeadV1;
                let aad = seal_aad(encoded_key, scheme, op_kind, context);
                let cipher = XChaCha20Poly1305::new(
                    &AeadKey::try_from(&key.0[..]).expect("fixed-length value key"),
                );
                let tag = cipher
                    .encrypt_inout_detached(
                        &XNonce::try_from(&nonce.0[..]).expect("fixed-length value nonce"),
                        &aad,
                        plaintext.as_mut_slice().into(),
                    )
                    .map_err(|_| CipherError::DecryptFailed)?;
                Ok((
                    Seal {
                        scheme,
                        nonce: nonce.0,
                        aead: tag.into(),
                        payload: Vec::new(),
                    },
                    plaintext,
                ))
            }
        }
    }

    fn open_detached(
        &self,
        encoded_key: &Key,
        op_kind: u8,
        mut ciphertext: Vec<u8>,
        seal: &Seal,
        context: DeviceTag,
    ) -> Result<Vec<u8>, CipherError> {
        seal.validate_payload()
            .map_err(|_| CipherError::MalformedSealedValue)?;
        match &self.mode {
            CipherMode::Plaintext => Ok(ciphertext),
            CipherMode::Encrypted { space_keys, .. } => {
                let key = space_keys.get(&context.cipher_epoch.0).ok_or(
                    CipherError::MissingSpaceKey {
                        epoch: context.cipher_epoch.0,
                    },
                )?;
                let aad = seal_aad(encoded_key, seal.scheme, op_kind, context);
                let cipher = XChaCha20Poly1305::new(
                    &AeadKey::try_from(&key.0[..]).expect("fixed-length value key"),
                );
                let tag: &AeadTag = (&seal.aead[..])
                    .try_into()
                    .expect("fixed-length authentication tag");
                cipher
                    .decrypt_inout_detached(
                        &XNonce::try_from(&seal.nonce[..]).expect("fixed-length value nonce"),
                        &aad,
                        ciphertext.as_mut_slice().into(),
                        tag,
                    )
                    .map_err(|_| CipherError::DecryptFailed)?;
                Ok(ciphertext)
            }
        }
    }
}

pub fn derive_space_id(name_key: NameKey) -> SpaceId {
    let hkdf = Hkdf::<Sha256>::new(None, &name_key.0);
    let mut out = [0u8; 16];
    hkdf.expand(SPACE_ID_INFO, &mut out)
        .expect("fixed-length HKDF expand");
    SpaceId(out)
}

fn encode_name_key(name_key: NameKey, key: &Key) -> Result<Key, CipherError> {
    let mut path_key = name_key.0;
    let mut out = Vec::with_capacity(key.components().len());
    for component in key.components() {
        let pseudonym = component_pseudonym(&path_key, component.as_bytes());
        out.push(KeyComponent::new(pseudonym.to_vec())?);
        path_key = child_name_key(&path_key, component.as_bytes());
    }
    Ok(Key::new(out)?)
}

fn component_pseudonym(path_key: &[u8; KEY_LEN], component: &[u8]) -> [u8; KEY_LEN] {
    let mut mac =
        HmacSha256::new_from_slice(path_key).expect("HMAC accepts fixed-length path keys");
    mac.update(COMPONENT_INFO);
    mac.update(&(component.len() as u32).to_be_bytes());
    mac.update(component);
    mac.finalize().into_bytes().into()
}

fn child_name_key(path_key: &[u8; KEY_LEN], component: &[u8]) -> [u8; KEY_LEN] {
    let hkdf = Hkdf::<Sha256>::new(Some(path_key), component);
    let mut out = [0u8; KEY_LEN];
    hkdf.expand(CHILD_KEY_INFO, &mut out)
        .expect("fixed-length HKDF expand");
    out
}

fn seal_aad(encoded_key: &Key, scheme: SealScheme, op_kind: u8, context: DeviceTag) -> Vec<u8> {
    let key = encoded_key.encode();
    let mut out = Vec::with_capacity(SEAL_AAD_PREFIX.len() + 2 + 8 + 16 + 8 + 8 + 4 + key.len());
    out.extend_from_slice(SEAL_AAD_PREFIX);
    out.push(scheme.to_u8());
    out.push(op_kind);
    out.extend_from_slice(&context.cipher_epoch.0.to_be_bytes());
    out.extend_from_slice(&context.device.0);
    out.extend_from_slice(&context.device_seq.0.to_be_bytes());
    out.extend_from_slice(&context.ver.0.to_be_bytes());
    out.extend_from_slice(&(key.len() as u32).to_be_bytes());
    out.extend_from_slice(&key);
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use homebase_core::tag::{AdmissionSeq, AdmissionTag, CipherEpoch, DeviceId, DeviceSeq, Ver};

    fn key(parts: &[&[u8]]) -> Key {
        Key::from_bytes(parts.iter().copied()).unwrap()
    }

    fn name_key(n: u8) -> NameKey {
        NameKey([n; KEY_LEN])
    }

    fn space_key(n: u8) -> SpaceKey {
        SpaceKey([n; KEY_LEN])
    }

    fn nonce(n: u8) -> ValueNonce {
        ValueNonce([n; VALUE_NONCE_LEN])
    }

    fn context(ver: u64) -> DeviceTag {
        DeviceTag {
            device: DeviceId([7; 16]),
            device_seq: DeviceSeq(8),
            ver: Ver(ver),
            cipher_epoch: CipherEpoch(0),
        }
    }

    fn admitted(device_entry: DeviceEntry) -> AdmittedEntry {
        AdmittedEntry {
            device_entry,
            admission: AdmissionTag {
                admission_seq: AdmissionSeq(0),
                op_index: 0,
            },
        }
    }

    fn set(key: Key, value: &[u8]) -> Mutation {
        Mutation::Set {
            key,
            value: value.to_vec(),
        }
    }

    #[test]
    fn encrypted_envelope_commits_to_space_id() {
        let envelope = SpaceEnvelope::encrypted(name_key(1), space_key(2));
        let id = derive_space_id(name_key(1));
        let codec = envelope.open_expected(id).unwrap();
        assert_eq!(codec.space_id(), id);
        assert_eq!(
            envelope.open_expected(SpaceId([9; 16])).unwrap_err(),
            CipherError::SpaceIdMismatch {
                expected: SpaceId([9; 16]),
                derived: id
            }
        );
    }

    #[test]
    fn plaintext_envelope_passes_through() {
        let id = SpaceId([7; 16]);
        let codec = SpaceEnvelope::plaintext(id).open().unwrap();
        let raw = key(&[b"db", b"k"]);
        assert_eq!(codec.space_id(), id);
        assert_eq!(codec.encode_key(&raw).unwrap(), raw);
        let entry = codec
            .encode_device_entry(set(raw, b"v"), context(1), nonce(3))
            .unwrap();
        let Mutation::Set { value, .. } = &entry.mutation else {
            panic!("set changed shape")
        };
        assert_eq!(value.0, b"v");
        assert_eq!(
            codec
                .open_admitted_entry(&admitted(entry))
                .unwrap()
                .device_entry
                .mutation,
            set(key(&[b"db", b"k"]), b"v")
        );
    }

    #[test]
    fn name_cipher_preserves_prefix_correspondence_and_hides_components() {
        let codec = SpaceEnvelope::encrypted(name_key(1), space_key(2))
            .open()
            .unwrap();
        let db = key(&[b"db"]);
        let row = key(&[b"db", b"row"]);
        let other = key(&[b"other", b"row"]);

        let encoded_db = codec.encode_key(&db).unwrap();
        let encoded_row = codec.encode_key(&row).unwrap();
        let encoded_other = codec.encode_key(&other).unwrap();

        assert!(encoded_row.starts_with(&encoded_db));
        assert_ne!(encoded_db, db);
        assert_ne!(encoded_row.components()[1], encoded_other.components()[1]);
        assert_eq!(codec.encode_key(&row).unwrap(), encoded_row);
    }

    #[test]
    fn ranges_encode_under_the_same_prefix_rules() {
        let codec = SpaceEnvelope::encrypted(name_key(1), space_key(2))
            .open()
            .unwrap();
        assert_eq!(codec.encode_range(&Range::Full).unwrap(), Range::Full);
        let encoded = codec.encode_range(&Range::Prefix(key(&[b"db"]))).unwrap();
        let Range::Prefix(prefix) = encoded else {
            panic!("prefix range changed shape")
        };
        assert_eq!(prefix.components().len(), 1);
        assert_ne!(prefix, key(&[b"db"]));
    }

    #[test]
    fn encrypted_set_roundtrips_and_empty_value_has_ciphertext() {
        let codec = SpaceEnvelope::encrypted(name_key(1), space_key(2))
            .open()
            .unwrap();
        let encoded_key = codec.encode_key(&key(&[b"db", b"k"])).unwrap();
        let entry = codec
            .encode_device_entry(set(encoded_key.clone(), b""), context(9), nonce(3))
            .unwrap();
        let Mutation::Set { value, .. } = &entry.mutation else {
            panic!("set changed shape")
        };
        assert_eq!(value.0.len(), 1);
        assert!(entry.seal.payload.is_empty());
        assert_eq!(
            codec
                .open_admitted_entry(&admitted(entry))
                .unwrap()
                .device_entry
                .mutation,
            set(encoded_key, b"")
        );
    }

    #[test]
    fn encrypted_delete_authenticates_empty_plaintext() {
        let codec = SpaceEnvelope::encrypted(name_key(1), space_key(2))
            .open()
            .unwrap();
        let encoded_key = codec.encode_key(&key(&[b"db", b"k"])).unwrap();
        let entry = codec
            .encode_device_entry(
                Mutation::Delete {
                    key: encoded_key.clone(),
                },
                context(9),
                nonce(5),
            )
            .unwrap();
        let Mutation::Delete { .. } = &entry.mutation else {
            panic!("delete changed shape")
        };
        assert_ne!(entry.seal.aead, [0; 16]);
        assert_eq!(
            codec
                .open_admitted_entry(&admitted(entry))
                .unwrap()
                .device_entry
                .mutation,
            Mutation::Delete { key: encoded_key }
        );
    }

    #[test]
    fn seal_binds_key_operation_and_full_write_context() {
        let codec = SpaceEnvelope::encrypted(name_key(1), space_key(2))
            .open()
            .unwrap();
        let encoded_key = codec.encode_key(&key(&[b"db", b"k"])).unwrap();
        let entry = codec
            .encode_device_entry(set(encoded_key, b"secret"), context(9), nonce(5))
            .unwrap();

        let mut wrong_key = admitted(entry.clone());
        match &mut wrong_key.device_entry.mutation {
            Mutation::Set { key: entry_key, .. } => {
                *entry_key = codec.encode_key(&key(&[b"db", b"other"])).unwrap()
            }
            Mutation::Delete { .. } => unreachable!(),
        }
        assert_eq!(
            codec.open_admitted_entry(&wrong_key),
            Err(CipherError::DecryptFailed)
        );

        let mut wrong_kind = admitted(entry.clone());
        wrong_kind.device_entry.mutation = Mutation::Delete {
            key: wrong_kind.key().clone(),
        };
        assert_eq!(
            codec.open_admitted_entry(&wrong_kind),
            Err(CipherError::DecryptFailed)
        );

        for tag in [
            DeviceTag {
                device: DeviceId([8; 16]),
                ..context(9)
            },
            DeviceTag {
                device_seq: DeviceSeq(9),
                ..context(9)
            },
            DeviceTag {
                ver: Ver(10),
                ..context(9)
            },
        ] {
            let mut entry = admitted(entry.clone());
            entry.device_entry.tag = tag;
            assert_eq!(
                codec.open_admitted_entry(&entry),
                Err(CipherError::DecryptFailed)
            );
        }
    }

    #[test]
    fn seal_rejects_ciphertext_nonce_tag_and_payload_tampering() {
        let codec = SpaceEnvelope::encrypted(name_key(1), space_key(2))
            .open()
            .unwrap();
        let encoded_key = codec.encode_key(&key(&[b"db", b"k"])).unwrap();
        let entry = codec
            .encode_device_entry(set(encoded_key, b"secret"), context(9), nonce(5))
            .unwrap();

        let mut ciphertext = admitted(entry.clone());
        let Mutation::Set { value, .. } = &mut ciphertext.device_entry.mutation else {
            unreachable!()
        };
        value.0[0] ^= 1;
        assert_eq!(
            codec.open_admitted_entry(&ciphertext),
            Err(CipherError::DecryptFailed)
        );

        let mut nonce = admitted(entry.clone());
        nonce.device_entry.seal.nonce[0] ^= 1;
        assert_eq!(
            codec.open_admitted_entry(&nonce),
            Err(CipherError::DecryptFailed)
        );

        let mut tag = admitted(entry.clone());
        tag.device_entry.seal.aead[0] ^= 1;
        assert_eq!(
            codec.open_admitted_entry(&tag),
            Err(CipherError::DecryptFailed)
        );

        let mut payload = admitted(entry);
        payload.device_entry.seal.payload.push(1);
        assert_eq!(
            codec.open_admitted_entry(&payload),
            Err(CipherError::MalformedSealedValue)
        );
    }

    #[test]
    fn different_nonce_changes_set_ciphertext() {
        let codec = SpaceEnvelope::encrypted(name_key(1), space_key(2))
            .open()
            .unwrap();
        let mutation = set(codec.encode_key(&key(&[b"db", b"k"])).unwrap(), b"secret");
        let first = codec
            .encode_device_entry(mutation.clone(), context(9), nonce(5))
            .unwrap();
        let second = codec
            .encode_device_entry(mutation, context(9), nonce(6))
            .unwrap();
        let (Mutation::Set { value: first, .. }, Mutation::Set { value: second, .. }) =
            (&first.mutation, &second.mutation)
        else {
            panic!("sets changed shape")
        };
        assert_ne!(first, second);
    }
}
