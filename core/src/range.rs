//! Component-wise key ranges shared by reads, assertions, and mutations.

use crate::key::Key;

/// A full space or one tuple-prefix subtree.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Range {
    Full,
    Prefix(Key),
}

impl Range {
    pub fn covers_key(&self, key: &Key) -> bool {
        match self {
            Self::Full => true,
            Self::Prefix(prefix) => key.starts_with(prefix),
        }
    }

    pub fn covers_range(&self, other: &Range) -> bool {
        match (self, other) {
            (Self::Full, _) => true,
            (Self::Prefix(_), Self::Full) => false,
            (Self::Prefix(a), Self::Prefix(b)) => b.starts_with(a),
        }
    }

    pub fn overlaps(&self, other: &Range) -> bool {
        self.covers_range(other) || other.covers_range(self)
    }

    /// The shared subtree of two overlapping hierarchical ranges. Because
    /// ranges are only Full or tuple prefixes, an intersection is always the
    /// narrower operand rather than a newly synthesized shape.
    pub fn intersection(&self, other: &Range) -> Option<Range> {
        if self.covers_range(other) {
            Some(other.clone())
        } else if other.covers_range(self) {
            Some(self.clone())
        } else {
            None
        }
    }

    /// Stable encoding used by checksums, local storage, and AEAD AAD.
    pub fn encode(&self) -> Vec<u8> {
        match self {
            Self::Full => vec![0],
            Self::Prefix(prefix) => {
                let key = prefix.encode();
                let mut out = Vec::with_capacity(1 + 4 + key.len());
                out.push(1);
                out.extend_from_slice(&(key.len() as u32).to_be_bytes());
                out.extend_from_slice(&key);
                out
            }
        }
    }

    pub fn decode(bytes: &[u8]) -> Option<Self> {
        match bytes {
            [0] => Some(Self::Full),
            [1, len @ ..] if len.len() >= 4 => {
                let key_len = u32::from_be_bytes(len[..4].try_into().ok()?) as usize;
                let key = len.get(4..)?;
                (key.len() == key_len)
                    .then(|| Key::decode(key).ok().map(Self::Prefix))
                    .flatten()
            }
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(parts: &[&[u8]]) -> Key {
        Key::from_bytes(parts.iter().copied()).unwrap()
    }

    #[test]
    fn range_roundtrips_and_overlap_is_bidirectional() {
        let parent = Range::Prefix(key(&[b"db"]));
        let child = Range::Prefix(key(&[b"db", b"row"]));
        let sibling = Range::Prefix(key(&[b"other"]));

        for range in [Range::Full, parent.clone(), child.clone()] {
            assert_eq!(Range::decode(&range.encode()), Some(range));
        }
        assert!(parent.overlaps(&child));
        assert!(child.overlaps(&parent));
        assert!(!parent.overlaps(&sibling));
    }

    #[test]
    fn intersection_returns_the_narrower_overlapping_range() {
        let parent = Range::Prefix(key(&[b"db"]));
        let child = Range::Prefix(key(&[b"db", b"child"]));
        let sibling = Range::Prefix(key(&[b"other"]));

        assert_eq!(Range::Full.intersection(&child), Some(child.clone()));
        assert_eq!(parent.intersection(&child), Some(child.clone()));
        assert_eq!(child.intersection(&parent), Some(child));
        assert_eq!(parent.intersection(&sibling), None);
    }

    #[test]
    fn range_decode_rejects_non_canonical_or_malformed_bytes() {
        assert_eq!(Range::decode(&[]), None);
        assert_eq!(Range::decode(&[0, 0]), None);
        assert_eq!(Range::decode(&[1, 0, 0, 0]), None);
        assert_eq!(Range::decode(&[1, 0, 0, 0, 2, 0]), None);
        assert_eq!(Range::decode(&[2]), None);
    }
}
