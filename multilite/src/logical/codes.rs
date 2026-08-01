//! Stable byte labels used by Multilite's durable Homebase key layout.

pub const ROOT: &[u8] = b"multilite";
pub const SCHEMA: &[u8] = b"schema";
pub const LOG: &[u8] = b"log";
pub const NAMES: &[u8] = b"names";
pub const TABLES: &[u8] = b"tables";
pub const COLUMNS: &[u8] = b"columns";
pub const COLUMN_DEPENDENCIES: &[u8] = b"column-dependencies";
pub const MAIN: &[u8] = b"main";
pub const INDEXES: &[u8] = b"indexes";
pub const ACTIVE_SCHEMA_REVISION: &[u8] = b"active-schema-revision";
pub const ACTIVE_PRIMARY_INDEX: &[u8] = b"active-primary-index";
pub const INDEX_DEFINITIONS: &[u8] = b"index-definitions";
pub const ROWS: &[u8] = b"rows";
pub const UNIQUE: &[u8] = b"unique";
pub const FOREIGN_REFERENCES: &[u8] = b"foreign-references";
pub const WRITE_REVISION: &[u8] = b"write-revision";
pub const TRANSACTIONS: &[u8] = b"transactions";

/// Components before the value images in row and UNIQUE keys.
pub const VALUE_KEY_PREFIX_COMPONENTS: usize = 5;

/// Components outside the parent and child value images in a foreign-reference key.
pub const FOREIGN_REFERENCE_KEY_FIXED_COMPONENTS: usize = 7;
