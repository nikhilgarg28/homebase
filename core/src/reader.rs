//! Bounds-checked reads over encoded byte records.

/// A cursor over an immutable byte slice.
///
/// Failed reads return `None` and leave the cursor unchanged. Record codecs
/// remain responsible for mapping that failure into their own corruption
/// errors and for deciding whether trailing bytes are permitted.
pub struct Reader<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> Reader<'a> {
    /// Start reading at the beginning of `bytes`.
    pub fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }

    /// Read one byte.
    pub fn u8(&mut self) -> Option<u8> {
        let value = *self.bytes.get(self.position)?;
        self.position += 1;
        Some(value)
    }

    /// Read a big-endian `u32`.
    pub fn u32(&mut self) -> Option<u32> {
        Some(u32::from_be_bytes(self.take(4)?.try_into().unwrap()))
    }

    /// Read a big-endian `u64`.
    pub fn u64(&mut self) -> Option<u64> {
        Some(u64::from_be_bytes(self.take(8)?.try_into().unwrap()))
    }

    /// Read exactly 16 bytes.
    pub fn bytes16(&mut self) -> Option<[u8; 16]> {
        Some(self.take(16)?.try_into().unwrap())
    }

    /// Read exactly `len` bytes.
    pub fn take(&mut self, len: usize) -> Option<&'a [u8]> {
        let end = self.position.checked_add(len)?;
        let value = self.bytes.get(self.position..end)?;
        self.position = end;
        Some(value)
    }

    /// Return all unread bytes without advancing.
    pub fn rest(&self) -> &'a [u8] {
        &self.bytes[self.position..]
    }

    /// Succeed only when the complete input has been consumed.
    pub fn end(&self) -> Option<()> {
        (self.position == self.bytes.len()).then_some(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_primitives_and_tracks_the_remainder() {
        let mut bytes = Vec::new();
        bytes.push(7);
        bytes.extend_from_slice(&42_u32.to_be_bytes());
        bytes.extend_from_slice(&99_u64.to_be_bytes());
        bytes.extend_from_slice(&[5; 16]);
        bytes.extend_from_slice(b"tail");

        let mut reader = Reader::new(&bytes);
        assert_eq!(reader.u8(), Some(7));
        assert_eq!(reader.u32(), Some(42));
        assert_eq!(reader.u64(), Some(99));
        assert_eq!(reader.bytes16(), Some([5; 16]));
        assert_eq!(reader.rest(), b"tail");
        assert_eq!(reader.end(), None);
        assert_eq!(reader.take(4), Some(b"tail".as_slice()));
        assert_eq!(reader.end(), Some(()));
    }

    #[test]
    fn failed_reads_do_not_advance_or_overflow() {
        let mut reader = Reader::new(b"abc");
        assert_eq!(reader.u64(), None);
        assert_eq!(reader.rest(), b"abc");
        assert_eq!(reader.take(usize::MAX), None);
        assert_eq!(reader.rest(), b"abc");
        assert_eq!(reader.take(3), Some(b"abc".as_slice()));
    }
}
