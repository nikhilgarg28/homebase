//! Replicated application-defined SQLite `user_version` metadata.

use std::fmt;

use homebase_core::reader::Reader;
use homebase_core::tag::Mutation;
use homebase_core::writer::Writer;
use rusqlite::Connection;

use super::guard::{GuardPlan, GuardReason, LogicalTarget, OperationFamily};
use crate::commit::footprint::ConflictFootprint;
use crate::{Error, Result};

const FRAME_VERSION: u8 = 1;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SetUserVersion {
    before: i32,
    after: i32,
}

pub struct UserVersionHomebaseOp {
    pub mutations: Vec<Mutation>,
    pub footprint: ConflictFootprint,
    pub guards: GuardPlan,
}

impl SetUserVersion {
    pub fn prepare(connection: &Connection, after: i32) -> Result<Option<Self>> {
        let before = read(connection)?;
        Ok((before != after).then_some(Self { before, after }))
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut writer = Writer::with_capacity(9);
        writer.u8(FRAME_VERSION);
        writer.u32(self.before as u32);
        writer.u32(self.after as u32);
        writer.finish()
    }

    pub fn decode(frame: &[u8]) -> std::result::Result<Self, UserVersionCodecError> {
        let mut reader = Reader::new(frame);
        if reader.u8() != Some(FRAME_VERSION) {
            return Err(UserVersionCodecError::UnknownVersion);
        }
        let before = reader.u32().ok_or(UserVersionCodecError::Truncated)? as i32;
        let after = reader.u32().ok_or(UserVersionCodecError::Truncated)? as i32;
        if reader.end().is_none() || before == after {
            return Err(UserVersionCodecError::InvalidFrame);
        }
        Ok(Self { before, after })
    }

    pub fn to_homebase(&self) -> Result<UserVersionHomebaseOp> {
        let key = key();
        let mutations = vec![Mutation::Set {
            key: key.clone(),
            value: self.after.to_be_bytes().to_vec(),
        }];
        let mut guards = GuardPlan::for_operation(OperationFamily::SetUserVersion);
        guards.write(key, GuardReason::UserVersion)?;
        let footprint = guards.footprint();
        Ok(UserVersionHomebaseOp {
            mutations,
            footprint,
            guards,
        })
    }

    pub fn apply(&self, connection: &Connection) -> Result<()> {
        set(connection, self.after)
    }

    pub fn rollback(&self, connection: &Connection) -> Result<()> {
        if read(connection)? != self.after {
            return Err(Error::InvalidDatabase(
                "pending user_version no longer matches SQLite state",
            ));
        }
        set(connection, self.before)
    }

    #[cfg(debug_assertions)]
    pub fn verify_materialized(&self, connection: &Connection) -> Result<()> {
        if read(connection)? == self.after {
            Ok(())
        } else {
            Err(Error::InvalidDatabase(
                "replicated user_version does not match SQLite state",
            ))
        }
    }
}

pub fn key() -> homebase_core::key::Key {
    LogicalTarget::UserVersion
        .render()
        .expect("user-version key is bounded")
}

pub fn read(connection: &Connection) -> Result<i32> {
    Ok(connection.query_row("PRAGMA main.user_version", (), |row| row.get(0))?)
}

fn set(connection: &Connection, value: i32) -> Result<()> {
    connection.pragma_update(Some("main"), "user_version", value)?;
    Ok(())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UserVersionCodecError {
    UnknownVersion,
    Truncated,
    InvalidFrame,
}

impl fmt::Display for UserVersionCodecError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{self:?}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logical::operation::MultiliteOp;

    #[test]
    fn codec_lowering_apply_and_rollback_are_exact() {
        let connection = Connection::open_in_memory().unwrap();
        let operation = SetUserVersion::prepare(&connection, -7).unwrap().unwrap();
        assert_eq!(
            SetUserVersion::decode(&operation.encode()).unwrap(),
            operation
        );
        let lowered = operation.to_homebase().unwrap();
        assert_eq!(lowered.mutations.len(), 1);
        assert_eq!(lowered.footprint.writes().len(), 1);
        operation.apply(&connection).unwrap();
        assert_eq!(read(&connection).unwrap(), -7);
        operation.rollback(&connection).unwrap();
        assert_eq!(read(&connection).unwrap(), 0);
        let logical = MultiliteOp::SetUserVersion(operation);
        assert_eq!(MultiliteOp::decode(&logical.encode()).unwrap(), logical);
    }

    #[test]
    fn codec_rejects_noops_truncation_and_trailing_bytes() {
        assert_eq!(
            SetUserVersion::decode(&[]),
            Err(UserVersionCodecError::UnknownVersion)
        );
        assert_eq!(
            SetUserVersion::decode(&[1]),
            Err(UserVersionCodecError::Truncated)
        );
        assert_eq!(
            SetUserVersion::decode(&[1, 0, 0, 0, 1, 0, 0, 0, 1]),
            Err(UserVersionCodecError::InvalidFrame)
        );
        assert!(SetUserVersion::decode(&[1, 0, 0, 0, 1, 0, 0, 0, 2, 9]).is_err());
    }
}
