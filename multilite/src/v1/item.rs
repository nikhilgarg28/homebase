//! Canonical identity and value frames for V1 append-only items.
//!
//! `ItemKey` frames are encoded as a version byte followed by the collection
//! and id, each prefixed by a big-endian `u64` byte length. `ItemInsert`
//! frames similarly contain a length-delimited `ItemKey` frame and payload.
//! Decoders accept exactly one complete frame and reject trailing bytes.

#![cfg_attr(
    not(test),
    allow(dead_code, reason = "consumed by later Multilite V1 batches")
)]

use std::fmt;

use homebase_core::key::Key;
use homebase_core::reader::Reader;
use sha2::{Digest, Sha256};

use crate::value::{require_blob, require_text};
use crate::{Result, Value};

const ITEM_KEY_FRAME_VERSION: u8 = 1;
const ITEM_INSERT_FRAME_VERSION: u8 = 1;
const ITEM_KEY_HASH_DOMAIN: &[u8] = b"multilite:item-key:v1\0";
const ITEM_KEY_NAMESPACE: [&[u8]; 2] = [b"multilite", b"items.v1"];

/// The logical primary key of one V1 item.
#[derive(Clone, Debug, PartialEq, Eq)]
struct ItemKey {
    collection: String,
    id: Vec<u8>,
}

impl ItemKey {
    fn new(collection: impl Into<String>, id: impl Into<Vec<u8>>) -> Self {
        Self {
            collection: collection.into(),
            id: id.into(),
        }
    }

    fn encode(&self) -> Vec<u8> {
        let collection = self.collection.as_bytes();
        let mut frame =
            Vec::with_capacity(1 + size_of::<u64>() * 2 + collection.len() + self.id.len());
        frame.push(ITEM_KEY_FRAME_VERSION);
        put_bytes(&mut frame, collection);
        put_bytes(&mut frame, &self.id);
        frame
    }

    fn decode(frame: &[u8]) -> std::result::Result<Self, ItemCodecError> {
        let mut reader = Reader::new(frame);
        let version = reader.u8().ok_or(ItemCodecError::Truncated)?;
        if version != ITEM_KEY_FRAME_VERSION {
            return Err(ItemCodecError::UnknownVersion {
                frame: FrameKind::ItemKey,
                version,
            });
        }
        let collection = read_bytes(&mut reader)?;
        let id = read_bytes(&mut reader)?.to_vec();
        require_end(&reader)?;
        let collection = std::str::from_utf8(collection)
            .map_err(ItemCodecError::InvalidCollectionUtf8)?
            .to_owned();
        Ok(Self { collection, id })
    }

    /// Derive the fixed-size, terminal Homebase key for this logical item.
    fn homebase_key(&self) -> Key {
        let mut hash = Sha256::new();
        hash.update(ITEM_KEY_HASH_DOMAIN);
        hash.update(self.encode());
        let digest: [u8; 32] = hash.finalize().into();

        Key::from_bytes([
            ITEM_KEY_NAMESPACE[0],
            ITEM_KEY_NAMESPACE[1],
            digest.as_slice(),
        ])
        .expect("fixed V1 namespace and SHA-256 digest must form a valid Homebase key")
    }
}

/// The complete plaintext carried by a V1 Homebase `Set`.
#[derive(Clone, Debug, PartialEq, Eq)]
struct ItemInsert {
    key: ItemKey,
    payload: Vec<u8>,
}

impl ItemInsert {
    fn new(key: ItemKey, payload: impl Into<Vec<u8>>) -> Self {
        Self {
            key,
            payload: payload.into(),
        }
    }

    fn from_values(collection: &Value, id: &Value, payload: &Value) -> Result<Self> {
        Ok(Self {
            key: ItemKey::new(require_text(collection)?, require_blob(id)?),
            payload: require_blob(payload)?.to_vec(),
        })
    }

    fn encode(&self) -> Vec<u8> {
        let key = self.key.encode();
        let mut frame =
            Vec::with_capacity(1 + size_of::<u64>() * 2 + key.len() + self.payload.len());
        frame.push(ITEM_INSERT_FRAME_VERSION);
        put_bytes(&mut frame, &key);
        put_bytes(&mut frame, &self.payload);
        frame
    }

    fn decode(frame: &[u8]) -> std::result::Result<Self, ItemCodecError> {
        let mut reader = Reader::new(frame);
        let version = reader.u8().ok_or(ItemCodecError::Truncated)?;
        if version != ITEM_INSERT_FRAME_VERSION {
            return Err(ItemCodecError::UnknownVersion {
                frame: FrameKind::ItemInsert,
                version,
            });
        }
        let key = ItemKey::decode(read_bytes(&mut reader)?)?;
        let payload = read_bytes(&mut reader)?.to_vec();
        require_end(&reader)?;
        Ok(Self { key, payload })
    }
}

fn put_bytes(frame: &mut Vec<u8>, bytes: &[u8]) {
    let len = u64::try_from(bytes.len()).expect("in-memory frame length must fit in u64");
    frame.extend_from_slice(&len.to_be_bytes());
    frame.extend_from_slice(bytes);
}

fn read_bytes<'a>(reader: &mut Reader<'a>) -> std::result::Result<&'a [u8], ItemCodecError> {
    let len = reader.u64().ok_or(ItemCodecError::Truncated)?;
    let len = usize::try_from(len).map_err(|_| ItemCodecError::LengthOverflow { len })?;
    reader.take(len).ok_or(ItemCodecError::Truncated)
}

fn require_end(reader: &Reader<'_>) -> std::result::Result<(), ItemCodecError> {
    reader.end().ok_or(ItemCodecError::TrailingBytes {
        len: reader.rest().len(),
    })
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FrameKind {
    ItemKey,
    ItemInsert,
}

impl fmt::Display for FrameKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ItemKey => f.write_str("ItemKey"),
            Self::ItemInsert => f.write_str("ItemInsert"),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum ItemCodecError {
    Truncated,
    LengthOverflow { len: u64 },
    UnknownVersion { frame: FrameKind, version: u8 },
    InvalidCollectionUtf8(std::str::Utf8Error),
    TrailingBytes { len: usize },
}

impl fmt::Display for ItemCodecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Truncated => f.write_str("item frame is truncated"),
            Self::LengthOverflow { len } => {
                write!(f, "item frame length {len} does not fit in memory")
            }
            Self::UnknownVersion { frame, version } => {
                write!(f, "unknown {frame} frame version {version}")
            }
            Self::InvalidCollectionUtf8(error) => {
                write!(f, "item collection is not valid UTF-8: {error}")
            }
            Self::TrailingBytes { len } => {
                write!(f, "item frame has {len} trailing bytes")
            }
        }
    }
}

impl std::error::Error for ItemCodecError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::InvalidCollectionUtf8(error) => Some(error),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Error, Type};

    fn digest(key: &Key) -> &[u8] {
        key.components().last().unwrap().as_bytes()
    }

    #[test]
    fn values_extract_an_item_insert_with_exact_storage_classes() {
        let insert = ItemInsert::from_values(
            &Value::Text(String::from("notes")),
            &Value::Blob(vec![0, 1]),
            &Value::Blob(b"hello".to_vec()),
        )
        .unwrap();

        assert_eq!(
            insert,
            ItemInsert::new(ItemKey::new("notes", vec![0, 1]), b"hello")
        );
    }

    #[test]
    fn value_extraction_rejects_the_wrong_class_for_each_column() {
        let text = Value::Text(String::from("notes"));
        let blob = Value::Blob(Vec::new());

        assert!(matches!(
            ItemInsert::from_values(&blob, &blob, &blob),
            Err(Error::UnexpectedValueType {
                expected: Type::Text,
                actual: Type::Blob,
            })
        ));
        assert!(matches!(
            ItemInsert::from_values(&text, &text, &blob),
            Err(Error::UnexpectedValueType {
                expected: Type::Blob,
                actual: Type::Text,
            })
        ));
        assert!(matches!(
            ItemInsert::from_values(&text, &blob, &Value::Null),
            Err(Error::UnexpectedValueType {
                expected: Type::Blob,
                actual: Type::Null,
            })
        ));
    }

    #[test]
    fn empty_and_large_identities_derive_valid_fixed_homebase_keys() {
        for logical in [
            ItemKey::new("", Vec::new()),
            ItemKey::new("c".repeat(16 * 1024), vec![0xa5; 16 * 1024]),
        ] {
            let key = logical.homebase_key();
            assert_eq!(key.components().len(), 3);
            assert_eq!(key.components()[0].as_bytes(), b"multilite");
            assert_eq!(key.components()[1].as_bytes(), b"items.v1");
            assert_eq!(key.components()[2].as_bytes().len(), 32);
        }
    }

    #[test]
    fn key_frames_are_unambiguous_and_digest_vectors_are_pinned() {
        let left = ItemKey::new("a", b"bc".to_vec());
        let right = ItemKey::new("ab", b"c".to_vec());
        assert_ne!(left.encode(), right.encode());
        assert_ne!(left.homebase_key(), right.homebase_key());

        assert_eq!(
            digest(&ItemKey::new("", Vec::new()).homebase_key()),
            [
                0xa6, 0xb5, 0xa8, 0x83, 0x0c, 0x53, 0x30, 0x02, 0xdb, 0xbf, 0xd0, 0xe0, 0x95, 0x57,
                0xbd, 0xe6, 0xf7, 0xcd, 0xec, 0xc1, 0xc2, 0x01, 0x5e, 0xbf, 0x30, 0x03, 0x60, 0xe0,
                0xeb, 0x43, 0x14, 0xdd,
            ]
        );
        assert_eq!(
            digest(&ItemKey::new("users", vec![0, 1, 0xff]).homebase_key()),
            [
                0x3f, 0x41, 0x8e, 0x77, 0xfa, 0xab, 0x2c, 0xcf, 0x38, 0xf4, 0x61, 0xee, 0xd5, 0x75,
                0xb4, 0x30, 0x02, 0x29, 0xcd, 0xf5, 0xf0, 0xdb, 0xaf, 0x86, 0xce, 0xef, 0x14, 0xa3,
                0xd9, 0xce, 0xef, 0x71,
            ]
        );
    }

    #[test]
    fn item_key_frame_is_canonical_and_roundtrips() {
        let key = ItemKey::new("users", vec![0, 1, 0xff]);
        assert_eq!(
            key.encode(),
            [
                1, 0, 0, 0, 0, 0, 0, 0, 5, b'u', b's', b'e', b'r', b's', 0, 0, 0, 0, 0, 0, 0, 3, 0,
                1, 0xff,
            ]
        );
        assert_eq!(ItemKey::decode(&key.encode()).unwrap(), key);
    }

    #[test]
    fn item_insert_roundtrips_empty_and_nonempty_values() {
        for insert in [
            ItemInsert::new(ItemKey::new("", Vec::new()), Vec::new()),
            ItemInsert::new(ItemKey::new("notes", vec![7]), b"payload"),
        ] {
            assert_eq!(ItemInsert::decode(&insert.encode()).unwrap(), insert);
        }
    }

    #[test]
    fn decoders_reject_unknown_versions_truncation_utf8_and_trailing_bytes() {
        assert_eq!(ItemKey::decode(&[]), Err(ItemCodecError::Truncated));
        assert_eq!(
            ItemKey::decode(&[2]),
            Err(ItemCodecError::UnknownVersion {
                frame: FrameKind::ItemKey,
                version: 2,
            })
        );

        let mut invalid_utf8 = ItemKey::new("x", Vec::new()).encode();
        invalid_utf8[8] = 1;
        invalid_utf8[9] = 0xff;
        assert!(matches!(
            ItemKey::decode(&invalid_utf8),
            Err(ItemCodecError::InvalidCollectionUtf8(_))
        ));

        let mut key_with_trailing = ItemKey::new("x", Vec::new()).encode();
        key_with_trailing.push(0);
        assert_eq!(
            ItemKey::decode(&key_with_trailing),
            Err(ItemCodecError::TrailingBytes { len: 1 })
        );

        assert_eq!(
            ItemInsert::decode(&[2]),
            Err(ItemCodecError::UnknownVersion {
                frame: FrameKind::ItemInsert,
                version: 2,
            })
        );
        let insert = ItemInsert::new(ItemKey::new("x", Vec::new()), Vec::new()).encode();
        assert_eq!(
            ItemInsert::decode(&insert[..insert.len() - 1]),
            Err(ItemCodecError::Truncated)
        );
        let mut insert_with_trailing = insert;
        insert_with_trailing.push(0);
        assert_eq!(
            ItemInsert::decode(&insert_with_trailing),
            Err(ItemCodecError::TrailingBytes { len: 1 })
        );
    }
}
