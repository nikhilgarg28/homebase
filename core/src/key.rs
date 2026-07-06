//! Tuple keys and their order-preserving flat encoding.
//!
//! A kernel key is a tuple of byte strings (max [`MAX_COMPONENTS`] components
//! of [`MAX_COMPONENT_LEN`] bytes each). Prefix means *component-wise* prefix:
//! `["db","pay"]` never covers `["db","payroll"]`.
//!
//! # Encoding
//!
//! The server stores keys as flat byte strings. Each component is encoded by
//! escaping `0x00` as `0x00 0x01` and appending the terminator `0x00 0x00`;
//! the key is the concatenation of its encoded components. This gives three
//! properties (each certified by a property test in `tests/key_props.rs`):
//!
//! 1. **Roundtrip** — decoding an encoded key returns the original.
//! 2. **Order preservation** — `encode(a) < encode(b) ⟺ a < b`, where keys
//!    compare component-wise lexicographically.
//! 3. **Prefix correspondence** — `a` starts with tuple-prefix `b` if and
//!    only if `encode(a)` starts with byte-prefix `encode(b)`. Prefix scans
//!    on the flat map are therefore plain byte-range scans.
//!
//! Note the terminator is two bytes, not one: with single-byte terminators a
//! pure-bytes encoding is ambiguous (`["a\x00"]` and `["a","\xFF"]` would
//! collide) and byte-prefixes would leak across component boundaries.

use std::fmt;

/// Maximum number of components in a key.
pub const MAX_COMPONENTS: usize = 16;

/// Maximum byte length of a single key component.
pub const MAX_COMPONENT_LEN: usize = 256;

/// A single validated key component. Empty components are legal.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct KeyComponent(Vec<u8>);

impl KeyComponent {
    /// Creates a component from raw bytes, enforcing [`MAX_COMPONENT_LEN`].
    pub fn new(bytes: impl Into<Vec<u8>>) -> Result<Self, KeyError> {
        let bytes = bytes.into();
        if bytes.len() > MAX_COMPONENT_LEN {
            return Err(KeyError::ComponentTooLong { len: bytes.len() });
        }
        Ok(Self(bytes))
    }

    /// The raw bytes of this component.
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    /// Consumes the component, returning its raw bytes.
    pub fn into_bytes(self) -> Vec<u8> {
        self.0
    }
}

impl fmt::Debug for KeyComponent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "b\"{}\"", self.0.escape_ascii())
    }
}

/// A validated tuple key: 1..=[`MAX_COMPONENTS`] components.
///
/// `Ord` is component-wise lexicographic — the canonical tuple order that the
/// encoding preserves.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct Key(Vec<KeyComponent>);

impl Key {
    /// Creates a key from validated components.
    pub fn new(components: impl Into<Vec<KeyComponent>>) -> Result<Self, KeyError> {
        let components = components.into();
        if components.is_empty() {
            return Err(KeyError::Empty);
        }
        if components.len() > MAX_COMPONENTS {
            return Err(KeyError::TooManyComponents {
                len: components.len(),
            });
        }
        Ok(Self(components))
    }

    /// Creates a key from raw byte components.
    pub fn from_bytes<T>(components: impl IntoIterator<Item = T>) -> Result<Self, KeyError>
    where
        T: Into<Vec<u8>>,
    {
        let components = components
            .into_iter()
            .map(KeyComponent::new)
            .collect::<Result<Vec<_>, _>>()?;
        Self::new(components)
    }

    /// The components of this key.
    pub fn components(&self) -> &[KeyComponent] {
        &self.0
    }

    /// True when `prefix` is a component-wise prefix of this key.
    pub fn starts_with(&self, prefix: &Key) -> bool {
        self.0.starts_with(&prefix.0)
    }

    /// Encodes this key into its order-preserving flat form.
    pub fn encode(&self) -> Vec<u8> {
        encode_components(&self.0)
    }

    /// Decodes a flat-encoded key. Rejects malformed escapes, truncated
    /// input, and keys violating the component/count limits.
    pub fn decode(bytes: &[u8]) -> Result<Self, DecodeError> {
        Self::new(decode_components(bytes)?).map_err(DecodeError::InvalidKey)
    }
}

/// Decodes a flat-encoded component sequence *without* enforcing the
/// [`MAX_COMPONENTS`] count limit — the inverse of [`encode_components`].
///
/// The storage layer decodes tuples longer than any user key (space id ⊕
/// record kind ⊕ user components ⊕ suffixes); per-component length limits
/// still apply. Empty input decodes to an empty sequence.
pub fn decode_components(bytes: &[u8]) -> Result<Vec<KeyComponent>, DecodeError> {
    let mut components = Vec::new();
    let mut current = Vec::new();
    let mut at_boundary = true;
    let mut i = 0;
    while i < bytes.len() {
        at_boundary = false;
        match bytes[i] {
            0x00 => match bytes.get(i + 1) {
                Some(0x00) => {
                    let component = KeyComponent::new(std::mem::take(&mut current))
                        .map_err(DecodeError::InvalidKey)?;
                    components.push(component);
                    at_boundary = true;
                    i += 2;
                }
                Some(0x01) => {
                    current.push(0x00);
                    i += 2;
                }
                Some(&byte) => return Err(DecodeError::InvalidEscape { offset: i, byte }),
                None => return Err(DecodeError::Truncated),
            },
            b => {
                current.push(b);
                i += 1;
            }
        }
    }
    if !at_boundary && !bytes.is_empty() {
        return Err(DecodeError::Truncated);
    }
    Ok(components)
}

/// Encodes a component sequence into the order-preserving flat form
/// *without* enforcing the [`MAX_COMPONENTS`] count limit.
///
/// [`Key`] enforces the limit for user-facing keys; the server's storage
/// layer composes longer tuples (space id ⊕ record kind ⊕ user components ⊕
/// suffixes) and encodes them through this function. All encoding properties
/// (order preservation, prefix correspondence) hold regardless of count.
pub fn encode_components(components: &[KeyComponent]) -> Vec<u8> {
    let cap: usize = components.iter().map(|c| c.0.len() + 2).sum();
    let mut out = Vec::with_capacity(cap);
    for component in components {
        for &b in &component.0 {
            if b == 0x00 {
                out.extend_from_slice(&[0x00, 0x01]);
            } else {
                out.push(b);
            }
        }
        out.extend_from_slice(&[0x00, 0x00]);
    }
    out
}

/// Key validation errors.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum KeyError {
    /// Keys must have at least one component.
    Empty,
    /// More than [`MAX_COMPONENTS`] components.
    TooManyComponents { len: usize },
    /// A component longer than [`MAX_COMPONENT_LEN`] bytes.
    ComponentTooLong { len: usize },
}

impl fmt::Display for KeyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => write!(f, "key must have at least one component"),
            Self::TooManyComponents { len } => {
                write!(f, "key has {len} components (max {MAX_COMPONENTS})")
            }
            Self::ComponentTooLong { len } => {
                write!(f, "key component is {len} bytes (max {MAX_COMPONENT_LEN})")
            }
        }
    }
}

impl std::error::Error for KeyError {}

/// Errors decoding a flat-encoded key.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DecodeError {
    /// Input ended mid-component or mid-escape.
    Truncated,
    /// A `0x00` was followed by a byte that is neither a terminator nor a
    /// valid escape continuation.
    InvalidEscape { offset: usize, byte: u8 },
    /// The decoded key violates the key limits.
    InvalidKey(KeyError),
}

impl fmt::Display for DecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Truncated => write!(f, "encoded key is truncated"),
            Self::InvalidEscape { offset, byte } => {
                write!(f, "invalid escape 0x00 0x{byte:02x} at offset {offset}")
            }
            Self::InvalidKey(err) => write!(f, "decoded key is invalid: {err}"),
        }
    }
}

impl std::error::Error for DecodeError {}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(components: &[&[u8]]) -> Key {
        Key::from_bytes(components.iter().copied()).unwrap()
    }

    #[test]
    fn enforces_limits() {
        assert_eq!(
            KeyComponent::new(vec![0; MAX_COMPONENT_LEN + 1]).unwrap_err(),
            KeyError::ComponentTooLong {
                len: MAX_COMPONENT_LEN + 1
            }
        );
        let too_many = vec![&b"x"[..]; MAX_COMPONENTS + 1];
        assert_eq!(
            Key::from_bytes(too_many).unwrap_err(),
            KeyError::TooManyComponents {
                len: MAX_COMPONENTS + 1
            }
        );
        assert_eq!(
            Key::from_bytes(Vec::<Vec<u8>>::new()).unwrap_err(),
            KeyError::Empty
        );
    }

    #[test]
    fn prefix_is_component_wise() {
        let payroll = key(&[b"db", b"payroll"]);
        assert!(payroll.starts_with(&key(&[b"db"])));
        assert!(payroll.starts_with(&payroll));
        assert!(!payroll.starts_with(&key(&[b"db", b"pay"])));
    }

    #[test]
    fn encode_golden() {
        assert_eq!(
            key(&[b"a\x00b"]).encode(),
            [0x61, 0x00, 0x01, 0x62, 0x00, 0x00]
        );
        assert_eq!(key(&[b"a", b""]).encode(), [0x61, 0x00, 0x00, 0x00, 0x00]);
    }

    #[test]
    fn ambiguity_pair_encodes_distinctly() {
        // The pair that collides under single-byte-terminator schemes.
        let a = key(&[b"a\x00"]);
        let b = key(&[b"a", b"\xff"]);
        assert_ne!(a.encode(), b.encode());
        assert_eq!(a.encode().cmp(&b.encode()), a.cmp(&b));
    }

    #[test]
    fn pinned_orderings() {
        // Tuple order and encoded order agree on tricky neighbors.
        let cases = [
            (key(&[b"a"]), key(&[b"a\x00"])),
            (key(&[b"a", b"z"]), key(&[b"a\x00"])),
            (key(&[b"a", b""]), key(&[b"a\x00"])),
            (key(&[b"a\x00"]), key(&[b"a\x01"])),
            (key(&[b"db", b"pay"]), key(&[b"db", b"payroll"])),
        ];
        for (lo, hi) in cases {
            assert!(lo < hi, "{lo:?} should sort below {hi:?}");
            assert!(lo.encode() < hi.encode(), "encodings of {lo:?}/{hi:?}");
        }
    }

    #[test]
    fn decode_rejects_malformed_input() {
        assert_eq!(
            Key::decode(&[]).unwrap_err(),
            DecodeError::InvalidKey(KeyError::Empty)
        );
        assert_eq!(Key::decode(&[0x61]).unwrap_err(), DecodeError::Truncated);
        assert_eq!(Key::decode(&[0x00]).unwrap_err(), DecodeError::Truncated);
        assert_eq!(
            Key::decode(&[0x61, 0x00, 0x00, 0x61]).unwrap_err(),
            DecodeError::Truncated
        );
        assert_eq!(
            Key::decode(&[0x00, 0x02, 0x00, 0x00]).unwrap_err(),
            DecodeError::InvalidEscape {
                offset: 0,
                byte: 0x02
            }
        );
    }
}
