//! Shared vocabulary for the homebase kernel.
//!
//! This crate holds the types both the server and the client speak:
//!
//! - [`key`] — tuple keys and their order-preserving flat encoding
//! - [`tag`] — value tags `(device, device_seq, epoch, ver, admission_seq)`
//!   and stored entries
//! - [`lease`] — read/write leases and TTLs
//! - [`clock`] — [`Timestamp`] and the [`Clock`] abstraction: real time for
//!   servers and clients, hand-cranked time for tests and the sim
//! - [`messages`] — transport-neutral request/response messages for the
//!   seven verbs, and [`KernelError`]
//! - [`seal`] — AEAD seal metadata attached to encrypted value operations
//! - [`space`] — [`SpaceId`] and the [`Space`] trait, the async verb
//!   contract every request executes against
//! - [`storage`] — [`OrderedStore`](storage::OrderedStore), the ordered
//!   byte-keyed map with atomic batches that the server's shards and the
//!   client's durable state both sit on (plus the reference
//!   [`MemoryStore`](storage::MemoryStore))
//!
//! No kernel or client logic lives here: the server crate hosts many spaces
//! (each a deterministic state machine behind the async trait); the client
//! crate speaks the same trait over the wire.

pub mod clock;
pub mod key;
pub mod lease;
pub mod messages;
pub mod seal;
pub mod space;
pub mod storage;
pub mod tag;

pub use clock::{Clock, ManualClock, MonotonicClock, Timestamp};
pub use key::{Key, KeyComponent};
pub use lease::{Lease, LeaseId, LeaseMode, LeaseRef};
pub use messages::KernelError;
pub use seal::{Seal, SealPayloadError, SealScheme, UnknownSealScheme};
pub use space::{Space, SpaceId};
pub use tag::{AdmissionSeq, DeviceId, DeviceSeq, Entry, Epoch, Tag, Value, Ver};
