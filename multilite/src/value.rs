//! Lossless values shared by SQLite capture and logical row lowering.

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
