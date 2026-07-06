//! Client-side key/value cipher.
//!
//! This is the crypto boundary below identity/link bootstrap and above the
//! kernel verbs. It intentionally takes all entropy as input: tests and the
//! deterministic simulator can drive it without ambient RNG.

use chacha20poly1305::aead::{Aead, KeyInit, Payload};
use chacha20poly1305::{Key as AeadKey, XChaCha20Poly1305, XNonce};
use hkdf::Hkdf;
use hmac::{Hmac, Mac};
use homebase_core::key::{Key, KeyComponent, KeyError};
use homebase_core::messages::{PutEntry, Range};
use homebase_core::space::SpaceId;
use homebase_core::tag::Value;
use sha2::Sha256;
use std::collections::BTreeMap;
use std::fmt;

type HmacSha256 = Hmac<Sha256>;

pub const KEY_LEN: usize = 32;
pub const VALUE_NONCE_LEN: usize = 24;
pub const V1_KEY_EPOCH: u64 = 0;

const SPACE_ID_INFO: &[u8] = b"homebase:space-id:v1";
const CHILD_KEY_INFO: &[u8] = b"homebase:name-child:v1";
const COMPONENT_INFO: &[u8] = b"homebase:name-component:v1";
const VALUE_AAD_PREFIX: &[u8] = b"homebase:value-aad:v1";
const VALUE_ENVELOPE_VERSION: u8 = 1;

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
    MalformedValueEnvelope,
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
            Self::MalformedValueEnvelope => write!(f, "malformed value envelope"),
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

impl SpaceEnvelope {
    pub fn encrypted(name_key: NameKey, space_key: SpaceKey) -> Self {
        Self::encrypted_with_epochs(name_key, [(V1_KEY_EPOCH, space_key)])
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
                if !space_keys.contains_key(&V1_KEY_EPOCH) {
                    return Err(CipherError::MissingSpaceKey {
                        epoch: V1_KEY_EPOCH,
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

    pub fn encode_put_entry(
        &self,
        entry: &PutEntry,
        nonce: ValueNonce,
    ) -> Result<PutEntry, CipherError> {
        let key = self.encode_key(&entry.key)?;
        let value = self.encode_value(&key, &entry.value, nonce)?;
        Ok(PutEntry {
            key,
            value,
            ver: entry.ver,
        })
    }

    pub fn encode_value(
        &self,
        encoded_key: &Key,
        value: &Value,
        nonce: ValueNonce,
    ) -> Result<Value, CipherError> {
        let Value::Present(plaintext) = value else {
            return Ok(Value::Absent);
        };
        match &self.mode {
            CipherMode::Plaintext => Ok(Value::Present(plaintext.clone())),
            CipherMode::Encrypted { space_keys, .. } => {
                let key = space_keys
                    .get(&V1_KEY_EPOCH)
                    .ok_or(CipherError::MissingSpaceKey {
                        epoch: V1_KEY_EPOCH,
                    })?;
                let aad = value_aad(encoded_key, V1_KEY_EPOCH);
                let cipher = XChaCha20Poly1305::new(
                    &AeadKey::try_from(&key.0[..]).expect("fixed-length value key"),
                );
                let ciphertext = cipher
                    .encrypt(
                        &XNonce::try_from(&nonce.0[..]).expect("fixed-length value nonce"),
                        Payload {
                            msg: plaintext,
                            aad: &aad,
                        },
                    )
                    .map_err(|_| CipherError::DecryptFailed)?;
                Ok(Value::Present(encode_value_envelope(
                    V1_KEY_EPOCH,
                    nonce,
                    &ciphertext,
                )))
            }
        }
    }

    pub fn decode_value(&self, encoded_key: &Key, value: &Value) -> Result<Value, CipherError> {
        let Value::Present(bytes) = value else {
            return Ok(Value::Absent);
        };
        match &self.mode {
            CipherMode::Plaintext => Ok(Value::Present(bytes.clone())),
            CipherMode::Encrypted { space_keys, .. } => {
                let (epoch, nonce, ciphertext) = decode_value_envelope(bytes)?;
                let key = space_keys
                    .get(&epoch)
                    .ok_or(CipherError::MissingSpaceKey { epoch })?;
                let aad = value_aad(encoded_key, epoch);
                let cipher = XChaCha20Poly1305::new(
                    &AeadKey::try_from(&key.0[..]).expect("fixed-length value key"),
                );
                let plaintext = cipher
                    .decrypt(
                        &XNonce::try_from(&nonce.0[..]).expect("fixed-length value nonce"),
                        Payload {
                            msg: ciphertext,
                            aad: &aad,
                        },
                    )
                    .map_err(|_| CipherError::DecryptFailed)?;
                Ok(Value::Present(plaintext))
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

fn value_aad(encoded_key: &Key, epoch: u64) -> Vec<u8> {
    let key = encoded_key.encode();
    let mut out = Vec::with_capacity(VALUE_AAD_PREFIX.len() + 8 + key.len());
    out.extend_from_slice(VALUE_AAD_PREFIX);
    out.extend_from_slice(&epoch.to_be_bytes());
    out.extend_from_slice(&key);
    out
}

fn encode_value_envelope(epoch: u64, nonce: ValueNonce, ciphertext: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(1 + 8 + VALUE_NONCE_LEN + ciphertext.len());
    out.push(VALUE_ENVELOPE_VERSION);
    out.extend_from_slice(&epoch.to_be_bytes());
    out.extend_from_slice(&nonce.0);
    out.extend_from_slice(ciphertext);
    out
}

fn decode_value_envelope(bytes: &[u8]) -> Result<(u64, ValueNonce, &[u8]), CipherError> {
    if bytes.len() < 1 + 8 + VALUE_NONCE_LEN || bytes[0] != VALUE_ENVELOPE_VERSION {
        return Err(CipherError::MalformedValueEnvelope);
    }
    let epoch = u64::from_be_bytes(
        bytes[1..9]
            .try_into()
            .map_err(|_| CipherError::MalformedValueEnvelope)?,
    );
    let nonce = ValueNonce(
        bytes[9..9 + VALUE_NONCE_LEN]
            .try_into()
            .map_err(|_| CipherError::MalformedValueEnvelope)?,
    );
    Ok((epoch, nonce, &bytes[9 + VALUE_NONCE_LEN..]))
}

#[cfg(test)]
mod tests {
    use super::*;
    use homebase_core::tag::Ver;

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
        assert_eq!(
            codec
                .encode_value(&raw, &Value::Present(b"v".to_vec()), nonce(3))
                .unwrap(),
            Value::Present(b"v".to_vec())
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
    fn value_encryption_roundtrips_and_binds_context() {
        let codec = SpaceEnvelope::encrypted(name_key(1), space_key(2))
            .open()
            .unwrap();
        let encoded_key = codec.encode_key(&key(&[b"db", b"k"])).unwrap();
        let value = Value::Present(b"secret".to_vec());
        let sealed = codec.encode_value(&encoded_key, &value, nonce(3)).unwrap();
        assert_ne!(sealed, value);
        assert_eq!(codec.decode_value(&encoded_key, &sealed).unwrap(), value);
        let other_key = codec.encode_key(&key(&[b"db", b"other"])).unwrap();
        assert_eq!(
            codec.decode_value(&other_key, &sealed).unwrap_err(),
            CipherError::DecryptFailed
        );

        let mut tampered = match sealed {
            Value::Present(bytes) => bytes,
            Value::Absent => unreachable!(),
        };
        *tampered.last_mut().unwrap() ^= 0x01;
        assert_eq!(
            codec
                .decode_value(&encoded_key, &Value::Present(tampered))
                .unwrap_err(),
            CipherError::DecryptFailed
        );
    }

    #[test]
    fn put_entries_encode_key_and_value_together() {
        let codec = SpaceEnvelope::encrypted(name_key(1), space_key(2))
            .open()
            .unwrap();
        let entry = PutEntry {
            key: key(&[b"db", b"k"]),
            value: Value::Present(b"secret".to_vec()),
            ver: Ver(9),
        };
        let encoded = codec.encode_put_entry(&entry, nonce(5)).unwrap();
        assert_ne!(encoded.key, entry.key);
        assert_ne!(encoded.value, entry.value);
        assert_eq!(
            codec.decode_value(&encoded.key, &encoded.value).unwrap(),
            entry.value
        );

        let tombstone = PutEntry {
            value: Value::Absent,
            ..entry
        };
        assert_eq!(
            codec.encode_put_entry(&tombstone, nonce(6)).unwrap().value,
            Value::Absent
        );
    }
}
