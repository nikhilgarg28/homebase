//! Multi-writer SQLite with E2EE sync, built on homebase.
//!
//! Not published for use yet.

mod connection;
mod error;

pub use connection::{MultiliteConnection, MultiliteStatement};
pub use error::{Error, Result};
