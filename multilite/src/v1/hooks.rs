//! V1 authorization and preupdate capture policy.

#![cfg_attr(
    not(test),
    allow(
        dead_code,
        reason = "integrated into MultiliteConnection by later batches"
    )
)]

use rusqlite::hooks::{AuthAction, AuthContext, Authorization, PreUpdateCase};

use super::item::ItemInsert;
use crate::runtime::{ExecutionMode, HookPolicy};
use crate::value::owned_value;
use crate::{Error, Result};

pub(super) struct V1Hooks;

impl HookPolicy for V1Hooks {
    type Event = ItemInsert;

    fn authorize(&mut self, mode: ExecutionMode, context: AuthContext<'_>) -> Authorization {
        if mode != ExecutionMode::Public {
            return Authorization::Allow;
        }

        match context.action {
            AuthAction::CreateTable { .. }
            | AuthAction::Delete {
                table_name: "items",
            }
            | AuthAction::Update {
                table_name: "items",
                ..
            } => Authorization::Deny,
            _ => Authorization::Allow,
        }
    }

    fn preupdate(
        &mut self,
        mode: ExecutionMode,
        database: &str,
        table: &str,
        update: &PreUpdateCase,
    ) -> Result<Option<Self::Event>> {
        if mode != ExecutionMode::Public || database != "main" || table != "items" {
            return Ok(None);
        }

        let PreUpdateCase::Insert(values) = update else {
            return Err(Error::CaptureInvariant(
                "public items mutation was not an insert",
            ));
        };
        if values.get_column_count() != 3 {
            return Err(Error::CaptureInvariant(
                "V1 items row did not contain exactly three columns",
            ));
        }

        let collection = owned_value(values.get_new_column_value(0)?)?;
        let id = owned_value(values.get_new_column_value(1)?)?;
        let payload = owned_value(values.get_new_column_value(2)?)?;
        Ok(Some(ItemInsert::from_values(&collection, &id, &payload)?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::RuntimeConnection;
    use std::panic::{AssertUnwindSafe, catch_unwind};

    fn runtime() -> RuntimeConnection<V1Hooks> {
        let runtime = RuntimeConnection::open_in_memory(V1Hooks).unwrap();
        runtime
            .run(ExecutionMode::InternalMetadata, |connection| {
                connection.execute_batch(
                    "CREATE TABLE items (
                        collection TEXT NOT NULL,
                        id BLOB NOT NULL,
                        payload BLOB NOT NULL,
                        PRIMARY KEY (collection, id)
                    );
                    CREATE TABLE _mt_meta_probe (value BLOB NOT NULL);",
                )?;
                Ok(())
            })
            .unwrap();
        runtime
    }

    fn item_count(runtime: &RuntimeConnection<V1Hooks>) -> i64 {
        runtime
            .run(ExecutionMode::InternalMetadata, |connection| {
                Ok(connection.query_row("SELECT count(*) FROM items", (), |row| row.get(0))?)
            })
            .unwrap()
            .0
    }

    #[test]
    fn public_authorizer_denies_unsupported_mutation() {
        let runtime = runtime();
        let error = runtime
            .run(ExecutionMode::Public, |connection| {
                connection.execute("DELETE FROM items", ())?;
                Ok(())
            })
            .unwrap_err();

        assert!(matches!(error, Error::Sqlite(_)));
        assert_eq!(item_count(&runtime), 0);
    }

    #[test]
    fn preupdate_captures_owned_insert_values() {
        let runtime = runtime();
        let (_, captured) = runtime
            .run(ExecutionMode::Public, |connection| {
                connection.execute(
                    "INSERT INTO items VALUES (?1, ?2, ?3)",
                    ("notes", b"one".as_slice(), b"hello".as_slice()),
                )?;
                Ok(())
            })
            .unwrap();

        assert_eq!(captured.len(), 1);
        assert_eq!(
            captured[0],
            ItemInsert::from_values(
                &crate::Value::Text(String::from("notes")),
                &crate::Value::Blob(b"one".to_vec()),
                &crate::Value::Blob(b"hello".to_vec()),
            )
            .unwrap()
        );
        assert_eq!(item_count(&runtime), 1);
    }

    #[test]
    fn operation_failure_rolls_back_row_and_capture() {
        let runtime = runtime();
        let error = runtime
            .run(ExecutionMode::Public, |connection| {
                connection.execute("INSERT INTO items VALUES ('notes', x'01', x'02')", ())?;
                Err::<(), _>(Error::CaptureInvariant("injected after insert"))
            })
            .unwrap_err();

        assert!(matches!(
            error,
            Error::CaptureInvariant("injected after insert")
        ));
        assert_eq!(item_count(&runtime), 0);

        let (_, captured) = runtime
            .run(ExecutionMode::Public, |connection| {
                connection.execute("INSERT INTO items VALUES ('notes', x'03', x'04')", ())?;
                Ok(())
            })
            .unwrap();
        assert_eq!(captured.len(), 1);
    }

    #[test]
    fn callback_failure_rolls_back_the_sqlite_change() {
        let runtime = RuntimeConnection::open_in_memory(V1Hooks).unwrap();
        runtime
            .run(ExecutionMode::InternalMetadata, |connection| {
                connection.execute_batch("CREATE TABLE items (collection TEXT, id BLOB)")?;
                Ok(())
            })
            .unwrap();

        let error = runtime
            .run(ExecutionMode::Public, |connection| {
                connection.execute("INSERT INTO items VALUES ('notes', x'01')", ())?;
                Ok(())
            })
            .unwrap_err();
        assert!(matches!(error, Error::CaptureInvariant(_)));
        assert_eq!(item_count(&runtime), 0);
    }

    #[test]
    fn captured_storage_class_failure_rolls_back_the_sqlite_change() {
        let runtime = runtime();
        let error = runtime
            .run(ExecutionMode::Public, |connection| {
                connection.execute("INSERT INTO items VALUES (x'ff', x'01', x'02')", ())?;
                Ok(())
            })
            .unwrap_err();

        assert!(matches!(
            error,
            Error::UnexpectedValueType {
                expected: crate::Type::Text,
                actual: crate::Type::Blob,
            }
        ));
        assert_eq!(item_count(&runtime), 0);
    }

    #[test]
    fn panic_rolls_back_and_leaves_the_runtime_reusable() {
        let runtime = runtime();
        let panic = catch_unwind(AssertUnwindSafe(|| {
            let _ = runtime.run::<()>(ExecutionMode::Public, |connection| {
                connection.execute("INSERT INTO items VALUES ('notes', x'01', x'02')", ())?;
                panic!("injected after insert")
            });
        }));
        assert!(panic.is_err());
        assert_eq!(item_count(&runtime), 0);

        let (_, captured) = runtime
            .run(ExecutionMode::Public, |connection| {
                connection.execute("INSERT INTO items VALUES ('notes', x'03', x'04')", ())?;
                Ok(())
            })
            .unwrap();
        assert_eq!(captured.len(), 1);
        assert_eq!(item_count(&runtime), 1);
    }

    #[test]
    fn internal_apply_and_repair_modes_bypass_public_policy_and_capture() {
        let runtime = runtime();

        let (_, internal) = runtime
            .run(ExecutionMode::InternalMetadata, |connection| {
                connection.execute("INSERT INTO _mt_meta_probe VALUES (x'01')", ())?;
                Ok(())
            })
            .unwrap();
        assert!(internal.is_empty());

        let (_, applied) = runtime
            .run(ExecutionMode::RemoteApply, |connection| {
                connection.execute("INSERT INTO items VALUES ('notes', x'01', x'02')", ())?;
                Ok(())
            })
            .unwrap();
        assert!(applied.is_empty());
        assert_eq!(item_count(&runtime), 1);

        let (_, repaired) = runtime
            .run(ExecutionMode::Repair, |connection| {
                connection.execute("DELETE FROM items", ())?;
                Ok(())
            })
            .unwrap();
        assert!(repaired.is_empty());
        assert_eq!(item_count(&runtime), 0);
    }
}
