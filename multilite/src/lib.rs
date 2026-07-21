//! Multi-writer SQLite with E2EE sync, built on homebase.
//!
//! Not published for use yet.

mod connection;
mod database;
mod error;
mod metastore;
mod runtime;

pub use database::{Connection as MultiliteConnection, Statement as MultiliteStatement};
pub use database::{
    Connection, Statement, TransactionStatement, UpdateTransaction, ViewTransaction,
};
pub use database::{
    DatabaseId, IsolationLevel, OfflineServer, OpenOptions, PullOutcome, PushOutcome,
    PushRejection, ReplicaInvitation, SyncPolicy, UpdateOptions,
};
pub use error::{Error, Result};
pub use rusqlite::types::{FromSql, Type, Value, ValueRef};
pub use rusqlite::{Params, ToSql, params};
