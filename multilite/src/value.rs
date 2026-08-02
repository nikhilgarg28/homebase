//! Lossless values shared by SQLite capture, repair, and logical lowering.

use std::fmt;

use homebase_core::reader::Reader;
use homebase_core::writer::Writer;
use rusqlite::ToSql;
use rusqlite::types::{ToSqlOutput, ValueRef};

/// One SQLite storage-class value, preserving REAL bits and raw text bytes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum StoredValue {
    Null,
    Integer(i64),
    Real(u64),
    Text(Vec<u8>),
    Blob(Vec<u8>),
}

impl StoredValue {
    pub(crate) fn capture(value: ValueRef<'_>) -> Self {
        match value {
            ValueRef::Null => Self::Null,
            ValueRef::Integer(value) => Self::Integer(value),
            ValueRef::Real(value) => Self::Real(value.to_bits()),
            ValueRef::Text(value) => Self::Text(value.to_vec()),
            ValueRef::Blob(value) => Self::Blob(value.to_vec()),
        }
    }

    pub(crate) fn encoded_len(&self) -> usize {
        match self {
            Self::Null => 1,
            Self::Integer(_) | Self::Real(_) => 9,
            Self::Text(value) | Self::Blob(value) => 1usize.saturating_add(value.len()),
        }
    }

    pub(crate) fn encode(&self) -> Vec<u8> {
        let mut writer = Writer::new();
        match self {
            Self::Null => writer.u8(0),
            Self::Integer(value) => {
                writer.u8(1);
                writer.u64(u64::from_be_bytes(value.to_be_bytes()));
            }
            Self::Real(bits) => {
                writer.u8(2);
                writer.u64(*bits);
            }
            Self::Text(value) => {
                writer.u8(3);
                writer.bytes(value);
            }
            Self::Blob(value) => {
                writer.u8(4);
                writer.bytes(value);
            }
        }
        writer.finish()
    }

    pub(crate) fn decode(frame: &[u8]) -> std::result::Result<Self, StoredValueCodecError> {
        let mut reader = Reader::new(frame);
        let kind = reader.u8().ok_or(StoredValueCodecError::Truncated)?;
        let value = match kind {
            0 => Self::Null,
            1 => {
                let bits = reader.u64().ok_or(StoredValueCodecError::InvalidLength)?;
                Self::Integer(i64::from_be_bytes(bits.to_be_bytes()))
            }
            2 => Self::Real(reader.u64().ok_or(StoredValueCodecError::InvalidLength)?),
            3 | 4 => {
                let remaining = reader.rest().len();
                let bytes = reader
                    .take(remaining)
                    .expect("remaining byte count came from this reader")
                    .to_vec();
                if kind == 3 {
                    Self::Text(bytes)
                } else {
                    Self::Blob(bytes)
                }
            }
            _ => return Err(StoredValueCodecError::InvalidKind(kind)),
        };
        if reader.end().is_none() {
            return Err(StoredValueCodecError::InvalidLength);
        }
        Ok(value)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum StoredValueCodecError {
    Truncated,
    InvalidLength,
    InvalidKind(u8),
}

impl fmt::Display for StoredValueCodecError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Truncated => formatter.write_str("stored SQLite value is truncated"),
            Self::InvalidLength => formatter.write_str("stored SQLite value has an invalid length"),
            Self::InvalidKind(kind) => {
                write!(formatter, "stored SQLite value has unknown kind {kind}")
            }
        }
    }
}

impl ToSql for StoredValue {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        Ok(ToSqlOutput::Borrowed(match self {
            Self::Null => ValueRef::Null,
            Self::Integer(value) => ValueRef::Integer(*value),
            Self::Real(bits) => ValueRef::Real(f64::from_bits(*bits)),
            Self::Text(value) => ValueRef::Text(value),
            Self::Blob(value) => ValueRef::Blob(value),
        }))
    }
}
