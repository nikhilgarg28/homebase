//! Durable Multilite operations, schema IR, guards, and transaction compilation.
//!
//! This layer owns the semantic objects shared by local capture, canonical
//! application, authority replay, and rejection repair. Connection lifecycle,
//! transport, policy, and pending persistence remain in `database`; SQLite AST
//! parsing and the folded catalog are crate-level compiler adapters.

pub(crate) mod alter;
pub(crate) mod codes;
pub(crate) mod drop_table;
pub(crate) mod guard;
pub(crate) mod index;
pub(crate) mod isolation;
pub(crate) mod operation;
pub(crate) mod row;
pub(crate) mod schema;
pub(crate) mod transaction;
