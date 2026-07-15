use crate::{Error, Result, Type, Value, ValueRef};

/// Copy a borrowed SQLite value into its owned representation.
#[cfg_attr(
    not(test),
    allow(dead_code, reason = "consumed by Batch 4 V1 item extraction")
)]
pub(crate) fn owned_value(value: ValueRef<'_>) -> Result<Value> {
    Value::try_from(value).map_err(Error::ValueConversion)
}

/// Return a value's text, requiring SQLite's TEXT storage class.
#[cfg_attr(
    not(test),
    allow(dead_code, reason = "consumed by Batch 4 V1 item extraction")
)]
pub(crate) fn require_text(value: &Value) -> Result<&str> {
    match value {
        Value::Text(text) => Ok(text),
        other => Err(unexpected_type(Type::Text, other)),
    }
}

/// Return a value's bytes, requiring SQLite's BLOB storage class.
#[cfg_attr(
    not(test),
    allow(dead_code, reason = "consumed by Batch 4 V1 item extraction")
)]
pub(crate) fn require_blob(value: &Value) -> Result<&[u8]> {
    match value {
        Value::Blob(bytes) => Ok(bytes),
        other => Err(unexpected_type(Type::Blob, other)),
    }
}

#[cfg_attr(
    not(test),
    allow(dead_code, reason = "consumed by Batch 4 V1 item extraction")
)]
fn unexpected_type(expected: Type, actual: &Value) -> Error {
    Error::UnexpectedValueType {
        expected,
        actual: actual.data_type(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::types::FromSqlError;

    #[test]
    fn owned_value_copies_every_sqlite_storage_class() {
        let text = b"hello";
        let blob = b"\x00\x01\xff";
        let cases = [
            (ValueRef::Null, Value::Null),
            (ValueRef::Integer(-17), Value::Integer(-17)),
            (ValueRef::Real(2.5), Value::Real(2.5)),
            (ValueRef::Text(text), Value::Text(String::from("hello"))),
            (ValueRef::Blob(blob), Value::Blob(blob.to_vec())),
        ];

        for (borrowed, expected) in cases {
            assert_eq!(owned_value(borrowed).unwrap(), expected);
        }
    }

    #[test]
    fn owned_text_rejects_invalid_utf8_without_losing_the_cause() {
        let error = owned_value(ValueRef::Text(&[0xff])).unwrap_err();

        assert!(matches!(
            &error,
            Error::ValueConversion(FromSqlError::Utf8Error(_))
        ));
        assert!(std::error::Error::source(&error).is_some());
    }

    #[test]
    fn text_and_blob_helpers_accept_only_their_storage_class() {
        let text = Value::Text(String::from("collection"));
        let blob = Value::Blob(vec![0, 1, 2]);

        assert_eq!(require_text(&text).unwrap(), "collection");
        assert_eq!(require_blob(&blob).unwrap(), [0, 1, 2]);

        assert!(matches!(
            require_text(&blob),
            Err(Error::UnexpectedValueType {
                expected: Type::Text,
                actual: Type::Blob,
            })
        ));
        assert!(matches!(
            require_blob(&text),
            Err(Error::UnexpectedValueType {
                expected: Type::Blob,
                actual: Type::Text,
            })
        ));
    }

    #[test]
    fn storage_class_errors_report_every_actual_type() {
        let non_text_values = [
            Value::Null,
            Value::Integer(1),
            Value::Real(1.0),
            Value::Blob(Vec::new()),
        ];

        for value in non_text_values {
            let actual = value.data_type();
            let error = require_text(&value).unwrap_err();
            assert!(matches!(
                error,
                Error::UnexpectedValueType {
                    expected: Type::Text,
                    actual: error_actual,
                } if error_actual == actual
            ));
        }

        let non_blob_values = [
            Value::Null,
            Value::Integer(1),
            Value::Real(1.0),
            Value::Text(String::new()),
        ];

        for value in non_blob_values {
            let actual = value.data_type();
            let error = require_blob(&value).unwrap_err();
            assert!(matches!(
                error,
                Error::UnexpectedValueType {
                    expected: Type::Blob,
                    actual: error_actual,
                } if error_actual == actual
            ));
        }
    }
}
