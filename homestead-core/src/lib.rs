//! Shared vocabulary for the homestead kernel.
//!
//! This crate holds the types both the server and the client speak: tuple
//! keys and their order-preserving encoding, and (in later batches) leases,
//! tags, cursors, and the verb request/response types.

pub mod key;
