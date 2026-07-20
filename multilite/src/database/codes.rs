//! Stable byte labels used by Multilite's durable Homebase key layout.

pub const ROOT: &[u8] = b"multilite";
pub const SCHEMA: &[u8] = b"schema";
pub const LOG: &[u8] = b"log";
pub const NAMES: &[u8] = b"names";
pub const TABLES: &[u8] = b"tables";
pub const MAIN: &[u8] = b"main";
pub const ACTIVE_ROW_KEYSPACE: &[u8] = b"active-row-keyspace";
pub const ROW_KEYSPACES: &[u8] = b"row-keyspaces";
pub const ROWS: &[u8] = b"rows";
pub const WRITE_REVISION: &[u8] = b"write-revision";
pub const TRANSACTIONS: &[u8] = b"transactions";
