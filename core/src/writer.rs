//! Construction of encoded byte records.

/// A growable byte sink symmetric with [`crate::reader::Reader`].
///
/// Record codecs remain responsible for field ordering, length conversion,
/// and their own framing rules. This type centralizes primitive byte order and
/// removes direct `Vec<u8>` manipulation from those codecs.
#[derive(Default)]
pub struct Writer {
    bytes: Vec<u8>,
}

impl Writer {
    /// Start writing an empty record.
    pub fn new() -> Self {
        Self::default()
    }

    /// Start writing with space reserved for at least `capacity` bytes.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            bytes: Vec::with_capacity(capacity),
        }
    }

    /// Write one byte.
    pub fn u8(&mut self, value: u8) {
        self.bytes.push(value);
    }

    /// Write a big-endian `u32`.
    pub fn u32(&mut self, value: u32) {
        self.bytes.extend_from_slice(&value.to_be_bytes());
    }

    /// Write a big-endian `u64`.
    pub fn u64(&mut self, value: u64) {
        self.bytes.extend_from_slice(&value.to_be_bytes());
    }

    /// Write exactly 16 bytes.
    pub fn bytes16(&mut self, value: &[u8; 16]) {
        self.bytes.extend_from_slice(value);
    }

    /// Write `value` without adding a length or terminator.
    pub fn bytes(&mut self, value: &[u8]) {
        self.bytes.extend_from_slice(value);
    }

    /// Finish the record and return its bytes.
    pub fn finish(self) -> Vec<u8> {
        self.bytes
    }
}

#[cfg(test)]
mod tests {
    use crate::reader::Reader;

    use super::*;

    #[test]
    fn writes_primitives_in_reader_order() {
        let mut writer = Writer::new();
        writer.u8(7);
        writer.u32(42);
        writer.u64(99);
        writer.bytes16(&[5; 16]);
        writer.bytes(b"tail");

        let bytes = writer.finish();
        let mut reader = Reader::new(&bytes);
        assert_eq!(reader.u8(), Some(7));
        assert_eq!(reader.u32(), Some(42));
        assert_eq!(reader.u64(), Some(99));
        assert_eq!(reader.bytes16(), Some([5; 16]));
        assert_eq!(reader.take(4), Some(b"tail".as_slice()));
        assert_eq!(reader.end(), Some(()));
    }

    #[test]
    fn capacity_does_not_change_the_encoded_record() {
        let mut writer = Writer::with_capacity(128);
        writer.bytes(b"record");
        assert_eq!(writer.finish(), b"record");
    }
}
