//! Client durable truth — contract 2 of 3, expressed as the transitions
//! themselves: [`MetaStore`], the trait the engine writes through.
//!
//! The engine never wants raw key/value access; it wants exactly this
//! vocabulary — genesis, commit, trim, advance, lease churn — plus one
//! [`load`](MetaStore::load) at open. So the *trait* is that
//! vocabulary: every method is one **atomic, durable transition**, and
//! the storage representation is entirely the implementation's business:
//!
//! - [`OrderedMetaStore`] — the reference implementation over any
//!   [`OrderedStore`]: memory for tests, the sim's fault store for crash
//!   torture, a k/v file for standalone consumers;
//! - multilite implements the trait natively as legible SQLite system
//!   tables (`hb_oplog(commit_seq, space, entries)`, `hb_leases(…)`, …),
//!   running each transition inside the transaction that is already
//!   writing the user's rows — one fsync domain, literally;
//! - a fault/crash decorator can wrap *any* implementation generically.
//!
//! Transitions are individually atomic; multi-transition flows (genesis,
//! push) must be idempotent and resumable — the saga rule. The engine
//! loads once at open and writes through: durable truth lives here, the
//! in-memory view is never a second owner. One engine drives one store —
//! transitions are serialized by their caller, never raced against each
//! other (the space-actor discipline, client-side).
//!
//! # The state doctrine
//!
//! **A client serves any number of spaces** — with client-level singletons
//! where one suffices: one device identity, one shared `DeviceSeq`
//! stream, one oplog, one ver counter. (Multilite's policy of exactly one
//! data space plus a companion link space is layered above.) The server
//! requires only *strictly increasing per (space, device), gaps legal*,
//! so every space sees a strictly increasing subsequence of the one
//! stream; likewise the single queue drains in order, each space
//! receiving its commits in order.
//!
//! **The queue is keyed by the wire seq, assigned by reservation.**
//! [`reserve_commit`](MetaStore::reserve_commit) stamps each batch with
//! the next `DeviceSeq`, and [`commit`](MetaStore::commit) persists that
//! reserved assignment atomically with the entry — write-ahead *by
//! construction*: a successor can never reuse a seq a dead incarnation
//! may have sent, because the send and the committed reservation are the
//! same record. The contract this rests on: **a store-backed client
//! writes the server exclusively through its queue** (mixing direct puts
//! with queued commits on one device id would interleave the stream);
//! storeless engine-tier consumers are separate devices.
//!
//! **Two cursors, two directions — they never meet.** Range watermarks are
//! the *pull* cursors: per space and exact range, in the server's
//! `AdmissionSeq` domain — "this range has synced down through here."
//! Effective watermark lookup walks ancestor ranges and takes the max.
//! Trim is *push* acknowledgment:
//! client-level, in the device's own `DeviceSeq` domain — "the server
//! admitted my queue through here, drop the prefix." Different sequence
//! spaces, never compared: a write-only client trims forever without a
//! range watermark; a read-only one advances range watermarks without ever trimming.
//!
//! **Vers are assigned by the store: one Lamport high-water, no per-key
//! table.** The protocol's per-key ver chains stay (the untrusted-server
//! rollback tripwire, the exclusion auditor, what makes fork-recovery
//! requeues safe) — but per-key monotonicity does not require per-key
//! state: [`reserve_commit`](MetaStore::reserve_commit) stamps entries
//! with consecutive vers above the high-water (`+1, +2, …` in entry
//! order — so duplicate keys in one batch behave like a sequence,
//! mirroring the kernel's own within-batch rule), and pulls raise the
//! high-water to the maximum ver observed
//! ([`advance_watermark`](MetaStore::advance_watermark)). By the
//! acquire-barrier rule a writer has pulled everything under its lease
//! before writing, so the counter dominates the stored ver of every key
//! it may touch. (Multilite may additionally keep a per-row shadow tag
//! table for rollback detection and provenance — a layer above; ver
//! *assignment* stays here either way.)
//!
//! **Device identity: a random id, minted unless supplied, living in the
//! store.** Stable across moves and renames — and copied by file copies,
//! the accepted hazard (environment-derived ids were rejected: ambient,
//! untestable, unbindable into token claims). Safety is downstream: an
//! unexpected `DeviceSeqRegression` is proof of a fork, and the engine
//! re-mints, resyncs, and requeues. Device ids are disposable by design.
//!
//! **Lease deadlines are hybrid stamps, and the clock high-water is
//! their tripwire.** A deadline carries both rulers (wall + monotonic)
//! and the lineage of the monotonic one: the stamping incarnation
//! judges it precisely, any successor falls back to the wall reading
//! with a margin — that is what lets a restarted client keep its
//! offline authority without a round trip. The wall's one lie is the
//! backward step, so the store keeps a [`ClockRecord`]: the highest
//! wall send stamp ever recorded. An open that reads a wall clock
//! *behind* the high-water knows the timeline regressed while it was
//! dead — every stored deadline is suspect, and the engine zero-stamps
//! them (a renewal re-stamps on the new timeline, which is
//! automatically conservative). [`certify`] holds wall stamps to the
//! high-water.
//!
//! # The oracle
//!
//! [`certify`] is the recomputation audit — the client twin of the
//! server-side `check` — over the [`ClientState`] any implementation
//! loads; [`audit`] is load-then-certify. Implementation-specific
//! integrity (key shapes, record decoding) is each implementation's
//! `load` obligation; [`conformance`] drives any implementation through
//! the full transition lifecycle and certifies at every step.

use homebase_core::clock::{HybridTimestamp, Lineage, Timestamp};
use homebase_core::key::{Key, KeyComponent, decode_components, encode_components};
use homebase_core::lease::{Lease, LeaseId, LeaseMode};
use homebase_core::messages::{PutEntry, Range};
use homebase_core::space::SpaceId;
use homebase_core::storage::{OrderedStore, ScanIter, StorageError, WriteBatch, collect_scan};
use homebase_core::tag::{AdmissionSeq, DeviceId, DeviceSeq, Value, Ver};
use std::collections::BTreeMap;
use std::future::Future;
use std::time::Duration;

// ---------------------------------------------------------------------------
// records — the values transitions carry, storage-representation-neutral

/// The store's identity: minted at first open — and *disposable*: re-mint
/// (and resync) whenever file-copy doubt breaks continuity.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DeviceRecord {
    pub id: DeviceId,
}

/// The next `DeviceSeq` a commit reservation will be stamped with — one
/// stream shared by every space, advanced atomically inside
/// [`MetaStore::commit`] (the reserved assignment and the queue entry are
/// one record: write-ahead by construction).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SeqRecord {
    pub next: DeviceSeq,
}

/// The ver high-water: every commit stamps its entries with consecutive
/// vers above it and advances it past them; every pull raises it to the
/// maximum ver observed.
/// One counter serves every key of every space (see the module docs for
/// why per-key monotonicity needs no per-key state).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct VerHighRecord {
    pub high: Ver,
}

/// One exact range watermark: the admission seq this replica has synced
/// through for that range.
/// Absence is meaningful — no pull has completed, so the next one is a
/// snapshot.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WatermarkRecord {
    pub at: AdmissionSeq,
}

/// The wall-clock high-water: the highest send stamp this store has
/// recorded. The backward-step tripwire — a fresh reading below it means
/// the wall clock regressed while the client was down, and every lease
/// stamp written before the step is suspect. Recorded (not maxed) so the
/// engine can re-anchor after handling a poisoned open.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ClockRecord {
    pub high_water: Timestamp,
}

/// Cache of a space's sealed key bundle: ciphertext plus the space-key
/// epoch that sealed it. Opaque here — the codec and identity batches own
/// the bytes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CodecRecord {
    pub space_key_epoch: u64,
    pub sealed: Vec<u8>,
}

/// A held grant with its deadline: request-send + granted TTL, a
/// [`HybridTimestamp`] — wall time, monotonic time, and the lineage of
/// the monotonic ruler. The incarnation that stamped it judges it by
/// the precise monotonic ruler (wall alongside, for suspend); any later
/// incarnation falls back to the wall reading with a safety margin —
/// which is what keeps offline authority across restarts. The clock
/// high-water is the tripwire for wall regression: a poisoned open
/// zeroes the stamp (structurally dead) until a renewal re-stamps it.
/// Epochs remain the correctness backstop for writes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HeldLease {
    pub lease: Lease,
    /// Send time + granted TTL, stamped by the incarnation that heard
    /// the grant.
    pub deadline: HybridTimestamp,
    /// Fresh acquires are not local authority until the effective watermark
    /// for this lease prefix has reached the grant's barrier. Renewals preserve this.
    pub barrier: Option<AdmissionSeq>,
    /// A release intent has been durably recorded. Retiring leases are
    /// never local authority, but keep the id so the server release can
    /// be retried after a crash.
    pub retiring: bool,
}

/// One committed, unshipped batch in the single queue, keyed by the
/// `DeviceSeq` it ships under and carrying the space it ships to. Entries
/// are stored exactly as they will ship (`PutEntry`, vers stamped by the
/// store at commit) — after the codec batch that means pseudonymized keys
/// and sealed values; this layer is codec-agnostic.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommitRecord {
    pub space: SpaceId,
    pub entries: Vec<PutEntry>,
}

// ---------------------------------------------------------------------------
// loaded state + the oracle

/// One space's slice of the loaded state.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct SpaceState {
    pub watermarks: BTreeMap<Range, AdmissionSeq>,
    pub codec: Option<CodecRecord>,
    pub leases: BTreeMap<LeaseId, HeldLease>,
}

/// Everything a [`MetaStore`] remembers — what [`MetaStore::load`] hands
/// the engine at open, and what [`certify`] holds to the invariants.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct ClientState {
    pub device: Option<DeviceId>,
    /// The seq the next commit will be stamped with.
    pub next_seq: Option<DeviceSeq>,
    /// The ver the next commit's entries will exceed.
    pub ver_high: Option<Ver>,
    /// The wall-clock high-water (see [`ClockRecord`]).
    pub clock_high: Option<Timestamp>,
    /// The one queue, keyed by wire seq; each record names its space.
    pub oplog: BTreeMap<DeviceSeq, CommitRecord>,
    pub spaces: BTreeMap<SpaceId, SpaceState>,
}

/// The recomputation oracle — the client twin of the server-side `check`.
/// Panics with context on any violation:
///
/// 1. vers are **strictly increasing per (space, key)** across the queue
///    in commit order — a regression would bounce off the server as
///    `VerRegression`;
/// 2. the counters cover the queue: `next_seq` exceeds every queued seq
///    and `ver_high` is at least every queued ver — a lagging counter
///    means a torn commit (the assignment and the entry are one atomic
///    transition).
///
/// 3. when a clock high-water is recorded, every lease's send stamp
///    (`deadline − ttl`) lies at or under it — a stamp past the
///    high-water is a torn transition or a tampered timeline.
///
/// Queue seqs carry no density invariant: trims take a prefix and
/// discards take a suffix, but the seq counter never rewinds, so a
/// discard followed by new commits legally leaves a gap.
///
/// Implementation-level integrity (key shapes, record decoding, index
/// agreement) is each implementation's `load` obligation.
pub fn certify(state: &ClientState) {
    if let Some(high) = state.clock_high {
        for (space, space_state) in &state.spaces {
            for held in space_state.leases.values() {
                let ttl = held.lease.ttl.as_millis().min(u64::MAX as u128) as u64;
                let send = held.deadline.wall.0.saturating_sub(ttl);
                assert!(
                    send <= high.0,
                    "lease {:?} in {space:?} stamped past the clock high-water: \
                     send {send}, high {}",
                    held.lease.id,
                    high.0
                );
            }
        }
    }

    let mut last_ver: BTreeMap<(SpaceId, &Key), Ver> = BTreeMap::new();
    for record in state.oplog.values() {
        for entry in &record.entries {
            if let Some(previous) = last_ver.get(&(record.space, &entry.key)) {
                assert!(
                    entry.ver > *previous,
                    "oplog vers regress in {:?} at {:?}: {previous:?} then {:?}",
                    record.space,
                    entry.key,
                    entry.ver
                );
            }
            last_ver.insert((record.space, &entry.key), entry.ver);
        }
    }

    if let Some((max_seq, _)) = state.oplog.last_key_value() {
        let next = state.next_seq.expect("queued commits require a seq record");
        assert!(
            next > *max_seq,
            "next_seq lags the oplog: next {next:?}, queued through {max_seq:?}"
        );
        let high = state.ver_high.expect("queued commits require a ver record");
        let queued_high = state
            .oplog
            .values()
            .flat_map(|r| r.entries.iter().map(|e| e.ver))
            .max();
        if let Some(queued_high) = queued_high {
            assert!(
                high >= queued_high,
                "ver high lags the oplog: high {high:?}, queued {queued_high:?}"
            );
        }
    }
}

// ---------------------------------------------------------------------------
// the contract

/// Client durable truth as a transition vocabulary. Every method is one
/// **atomic, durable transition** (all-or-nothing against a crash);
/// multi-transition flows must be idempotent and resumable. Methods take
/// `&self` — implementations manage their own interior atomicity — and
/// return `Send` futures (desugared, like every trait in this codebase).
pub trait MetaStore {
    /// Everything remembered from the last incarnation. The engine calls
    /// this once at open — to certify and to adopt the constant-shape
    /// facts (identity, counters) — and afterward holds **no mirror** of
    /// the collections: leases and the queue are consulted through the
    /// point reads below, on demand. Corruption is a panic with context
    /// (the audit posture), IO failure an `Err`.
    fn load(&self) -> impl Future<Output = Result<ClientState, StorageError>> + Send;

    // -- reads: on-demand lookups against durable truth. Local-disk
    // cheap; implementations are free to buffer for performance — the
    // single-driver discipline means no one else invalidates them.

    /// The queued commits with `from ≤ seq ≤ through`, ascending — the
    /// one queue read. The pusher walks the queue in bounded seq windows
    /// (an empty answer inside a window is a legal gap, not the end);
    /// `oplog(s, s)` is the point lookup (the fork check: a seq the
    /// server claims we sent must still be queued).
    fn oplog(
        &self,
        from: DeviceSeq,
        through: DeviceSeq,
    ) -> impl Future<Output = Result<Vec<(DeviceSeq, CommitRecord)>, StorageError>> + Send;

    /// The held leases whose prefixes **cover** any of `prefixes`
    /// (component-wise ancestors, the query itself included) — the only
    /// lease question the engine ever asks: "what authority do I hold
    /// over these keys?" Never the whole space.
    fn leases_covering(
        &self,
        space: SpaceId,
        prefixes: &[Key],
    ) -> impl Future<Output = Result<Vec<HeldLease>, StorageError>> + Send;

    /// Effective pull cursor for `range`: exact cursor or the max cursor
    /// from any stored ancestor range (Full covers everything, prefix
    /// ancestors cover descendants).
    fn watermark(
        &self,
        space: SpaceId,
        range: &Range,
    ) -> impl Future<Output = Result<Option<AdmissionSeq>, StorageError>> + Send;

    // -- transitions: every method one atomic, durable step.

    /// Identity minted at first open (or re-minted after a suspected fork).
    fn record_device(&self, id: DeviceId) -> impl Future<Output = Result<(), StorageError>> + Send;

    /// Reserve a commit with the next
    /// `DeviceSeq` and its entries with consecutive vers above the
    /// high-water (in entry order — duplicate keys are legal and behave
    /// like a sequence, the kernel's own within-batch rule), but does not
    /// advance counters or append to the queue. The replica uses this
    /// window to transform values with stamped tags in AEAD associated
    /// data, then persists through [`commit`](Self::commit).
    ///
    /// Crash recovery ignores reservations because they are never durable.
    fn reserve_commit(
        &self,
        space: SpaceId,
        entries: Vec<(Key, Value)>,
    ) -> impl Future<Output = Result<ReservedCommit, StorageError>> + Send;

    /// Commit a reservation: advances the counters and appends the
    /// caller-transformed entries to the queue — **one atomic transition**.
    /// Returns what was assigned.
    fn commit(
        &self,
        reserved: ReservedCommit,
    ) -> impl Future<Output = Result<Committed, StorageError>> + Send;

    /// Acknowledged commits leave the queue: deletes every queued entry
    /// with seq ≤ `through`. Prefix-only **by construction** — pushes are
    /// FIFO, so a later seq cannot be acknowledged before an earlier one
    /// and a gap is unrepresentable. Idempotent: re-acknowledging is a
    /// no-op.
    ///
    /// No staged-group record exists on purpose: the admitted set is
    /// always a prefix of the queue, so a pusher recovers any grouping —
    /// a seq collision reveals the admitted extent to trim, and a ver
    /// regression on a *solo* head commit convicts a genuinely faulty
    /// one. Grouping is a wire-time choice, never durable state.
    fn trim_oplog(
        &self,
        through: DeviceSeq,
    ) -> impl Future<Output = Result<(), StorageError>> + Send;

    /// Rollback: deletes every queued entry with seq ≥ `from` — the
    /// suffix mirror of [`trim_oplog`](Self::trim_oplog), the resolution
    /// for a rejected push the caller chooses not to repair. The
    /// single-driver discipline keeps it from racing a push.
    ///
    /// The seq counter never rewinds (a discarded seq may have been
    /// *sent* and rejected; re-minting it would confuse the replay
    /// fence), so a discard followed by new commits leaves a **gap** in
    /// the queue — legal, and [`certify`] treats it so.
    fn discard_from(
        &self,
        from: DeviceSeq,
    ) -> impl Future<Output = Result<(), StorageError>> + Send;

    /// A pulled range cut records that exact range's sync point — and
    /// raises the ver high-water to the maximum ver the cut carried,
    /// atomically with it. Ancestor max is computed at read time, not
    /// fanned out here.
    fn advance_watermark(
        &self,
        space: SpaceId,
        range: &Range,
        at: AdmissionSeq,
        ver_seen: Ver,
    ) -> impl Future<Output = Result<(), StorageError>> + Send;

    /// The wall-clock high-water updates — written at open (re-anchor)
    /// and at every lease stamp (advance). A plain overwrite on purpose:
    /// recovering from a poisoned open must be able to *lower* it onto
    /// the new timeline.
    fn record_clock(
        &self,
        high: Timestamp,
    ) -> impl Future<Output = Result<(), StorageError>> + Send;

    /// Grants (or renewals) become durable — one atomic transition for
    /// the whole batch, because a batch acquire is all-or-nothing at the
    /// server and must not be half-remembered here. Records are
    /// identified by **(space, prefix)**: a re-grant of the same prefix
    /// replaces the superseded record (the server holds at most one live
    /// lease per prefix per device). Resumable, but unconfirmed until
    /// the next renewal succeeds (the stored deadline is never trusted
    /// across incarnations).
    fn record_leases(
        &self,
        space: SpaceId,
        leases: &[HeldLease],
    ) -> impl Future<Output = Result<(), StorageError>> + Send;

    /// Release intent: mark held leases unusable locally while retaining
    /// enough information to retry the server release after a crash.
    fn retire_leases(
        &self,
        space: SpaceId,
        ids: &[LeaseId],
    ) -> impl Future<Output = Result<(), StorageError>> + Send;

    /// Released or refused leases are forgotten, atomically as a batch.
    fn drop_leases(
        &self,
        space: SpaceId,
        ids: &[LeaseId],
    ) -> impl Future<Output = Result<(), StorageError>> + Send;

    /// The sealed-bundle cache updates (genesis, refresh after a pull).
    fn record_codec(
        &self,
        space: SpaceId,
        record: &CodecRecord,
    ) -> impl Future<Output = Result<(), StorageError>> + Send;
}

/// What [`MetaStore::commit`] assigned: the wire seq the batch will ship
/// under, and the new ver high-water — the entries were stamped
/// `(previous high, ver_high]` in order.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Committed {
    pub seq: DeviceSeq,
    pub ver_high: Ver,
}

/// A non-durable reserved commit. The entries may still carry caller-plain
/// values; only [`MetaStore::commit`] makes the record durable.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReservedCommit {
    pub seq: DeviceSeq,
    pub ver_high: Ver,
    pub record: CommitRecord,
}

/// Load-then-certify: the audit entry point for any implementation.
pub async fn audit<M: MetaStore>(store: &M) -> ClientState {
    let state = store.load().await.expect("audit loads must not fault");
    certify(&state);
    state
}

// ---------------------------------------------------------------------------
// reference implementation over any OrderedStore

/// First key component the reference implementation writes: the brand.
/// Cohabitants sharing the same `OrderedStore` (an embedded shard,
/// consumer data) keep to other brands; `Data` is reserved for them and
/// never scanned here.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum StoreNamespace {
    /// Client metadata — [`OrderedMetaStore`]'s slice.
    Meta = 0,
    /// Reserved for cohabitants; never written or scanned here.
    Data = 1,
}

/// The reference [`MetaStore`]: records encoded onto an ordered byte map
/// under the [`StoreNamespace::Meta`] brand. Memory for tests, the sim's
/// fault store for crash torture, a k/v file for standalone consumers.
///
/// ```text
/// (Meta, Client, Device)               → DeviceRecord
/// (Meta, Client, Seq)                  → SeqRecord
/// (Meta, Client, Ver)                  → VerHighRecord
/// (Meta, Client, Clock)                → ClockRecord
/// (Meta, Client, Oplog, seq_be)        → CommitRecord
/// (Meta, Space, id, Watermark, range)  → WatermarkRecord (exact range cursor)
/// (Meta, Space, id, Codec)             → CodecRecord
/// (Meta, Space, id, Lease, prefix)      → LeaseRecord
/// ```
pub struct OrderedMetaStore<S> {
    store: S,
}

impl<S: OrderedStore> OrderedMetaStore<S> {
    pub fn new(store: S) -> Self {
        Self { store }
    }
}

/// Root of every metadata key: client-global vs space-scoped.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
enum Root {
    Client = 0,
    Space = 1,
}

/// Record kind under `(Meta, Client, …)`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
enum ClientKind {
    Device = 0,
    Seq = 1,
    Ver = 2,
    Oplog = 3,
    Clock = 4,
}

/// Record kind under `(Meta, Space, id, …)`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
enum SpaceKind {
    Watermark = 0,
    Codec = 1,
    Lease = 2,
}

fn byte_component(b: u8) -> KeyComponent {
    KeyComponent::new(vec![b]).expect("single byte component")
}

fn space_component(space: SpaceId) -> KeyComponent {
    KeyComponent::new(space.0.to_vec()).expect("16-byte component")
}

fn u64_component(v: u64) -> KeyComponent {
    KeyComponent::new(v.to_be_bytes().to_vec()).expect("8-byte component")
}

fn client_kind(kind: ClientKind) -> Vec<KeyComponent> {
    vec![
        byte_component(StoreNamespace::Meta as u8),
        byte_component(Root::Client as u8),
        byte_component(kind as u8),
    ]
}

fn space_kind(space: SpaceId, kind: SpaceKind) -> Vec<KeyComponent> {
    vec![
        byte_component(StoreNamespace::Meta as u8),
        byte_component(Root::Space as u8),
        space_component(space),
        byte_component(kind as u8),
    ]
}

/// Byte prefix of everything the reference implementation owns.
pub fn meta_scan_all() -> Vec<u8> {
    encode_components(&[byte_component(StoreNamespace::Meta as u8)])
}

fn device_key() -> Vec<u8> {
    encode_components(&client_kind(ClientKind::Device))
}

fn seq_key() -> Vec<u8> {
    encode_components(&client_kind(ClientKind::Seq))
}

fn ver_key() -> Vec<u8> {
    encode_components(&client_kind(ClientKind::Ver))
}

fn clock_key() -> Vec<u8> {
    encode_components(&client_kind(ClientKind::Clock))
}

fn oplog_scan() -> Vec<u8> {
    encode_components(&client_kind(ClientKind::Oplog))
}

fn oplog_key(seq: DeviceSeq) -> Vec<u8> {
    let mut components = client_kind(ClientKind::Oplog);
    components.push(u64_component(seq.0));
    encode_components(&components)
}

fn watermark_key(space: SpaceId, range: &Range) -> Vec<u8> {
    let mut components = space_kind(space, SpaceKind::Watermark);
    match range {
        Range::Full => components.push(byte_component(0)),
        Range::Prefix(prefix) => {
            components.push(byte_component(1));
            components.push(KeyComponent::new(prefix.encode()).expect("nonempty encoded prefix"));
        }
    }
    encode_components(&components)
}

fn range_from_watermark_key(components: &[KeyComponent]) -> Range {
    // Compatibility with the earlier scalar watermark: no range suffix
    // meant the full-range cursor.
    if components.len() == 4 {
        return Range::Full;
    }
    match single_byte(components, 4, "watermark range kind") {
        0 => {
            assert_eq!(components.len(), 5, "full watermark has no suffix");
            Range::Full
        }
        1 => {
            assert_eq!(
                components.len(),
                6,
                "prefix watermark stores one encoded prefix"
            );
            Range::Prefix(
                Key::decode(
                    components
                        .get(5)
                        .expect("prefix watermark missing encoded prefix")
                        .as_bytes(),
                )
                .expect("undecodable watermark prefix"),
            )
        }
        other => panic!("unknown watermark range kind {other}"),
    }
}

fn watermark_ancestors(range: &Range) -> Vec<Range> {
    let mut out = vec![Range::Full];
    if let Range::Prefix(prefix) = range {
        let components = prefix.components();
        for depth in 1..=components.len() {
            out.push(Range::Prefix(
                Key::new(components[..depth].to_vec())
                    .expect("a prefix of a valid key is a valid key"),
            ));
        }
    }
    out
}

fn codec_key(space: SpaceId) -> Vec<u8> {
    encode_components(&space_kind(space, SpaceKind::Codec))
}

/// Leases are keyed by the prefix they cover — the question the engine
/// asks is "who covers this key?", answered by point reads on the
/// query's ancestors. One live lease per (space, prefix): a re-grant of
/// the same prefix overwrites.
fn lease_key(space: SpaceId, prefix: &Key) -> Vec<u8> {
    let mut components = space_kind(space, SpaceKind::Lease);
    components.push(KeyComponent::new(prefix.encode()).expect("nonempty encoded prefix"));
    encode_components(&components)
}

impl<S: OrderedStore + Sync> MetaStore for OrderedMetaStore<S> {
    async fn load(&self) -> Result<ClientState, StorageError> {
        let all = collect_scan(self.store.scan_prefix(&meta_scan_all())).await?;

        let mut out = ClientState::default();
        for (storage_key, bytes) in all {
            let components = decode_components(&storage_key).expect("undecodable storage key");
            let namespace = single_byte(&components, 0, "store namespace");
            assert_eq!(
                namespace,
                StoreNamespace::Meta as u8,
                "load scanned outside its brand"
            );
            let root = single_byte(&components, 1, "root");
            match root {
                r if r == Root::Client as u8 => {
                    let kind = single_byte(&components, 2, "client kind");
                    match kind {
                        k if k == ClientKind::Device as u8 => {
                            assert_eq!(components.len(), 3, "device key has no suffix");
                            let record =
                                DeviceRecord::decode(&bytes).expect("undecodable device record");
                            out.device = Some(record.id);
                        }
                        k if k == ClientKind::Seq as u8 => {
                            assert_eq!(components.len(), 3, "seq key has no suffix");
                            let record = SeqRecord::decode(&bytes).expect("undecodable seq record");
                            out.next_seq = Some(record.next);
                        }
                        k if k == ClientKind::Ver as u8 => {
                            assert_eq!(components.len(), 3, "ver key has no suffix");
                            let record =
                                VerHighRecord::decode(&bytes).expect("undecodable ver record");
                            out.ver_high = Some(record.high);
                        }
                        k if k == ClientKind::Clock as u8 => {
                            assert_eq!(components.len(), 3, "clock key has no suffix");
                            let record =
                                ClockRecord::decode(&bytes).expect("undecodable clock record");
                            out.clock_high = Some(record.high_water);
                        }
                        k if k == ClientKind::Oplog as u8 => {
                            let seq = DeviceSeq(u64_at(&components, 3, "commit seq"));
                            let record =
                                CommitRecord::decode(&bytes).expect("undecodable commit record");
                            out.oplog.insert(seq, record);
                        }
                        other => panic!("unknown client record kind {other}"),
                    }
                }
                r if r == Root::Space as u8 => {
                    let id: [u8; 16] = components
                        .get(2)
                        .expect("space key missing id")
                        .as_bytes()
                        .try_into()
                        .expect("space id must be 16 bytes");
                    let space = out.spaces.entry(SpaceId(id)).or_default();
                    let kind = single_byte(&components, 3, "space kind");
                    match kind {
                        k if k == SpaceKind::Watermark as u8 => {
                            let record =
                                WatermarkRecord::decode(&bytes).expect("undecodable watermark");
                            space
                                .watermarks
                                .insert(range_from_watermark_key(&components), record.at);
                        }
                        k if k == SpaceKind::Codec as u8 => {
                            let record = CodecRecord::decode(&bytes).expect("undecodable codec");
                            space.codec = Some(record);
                        }
                        k if k == SpaceKind::Lease as u8 => {
                            let record = HeldLease::decode(&bytes).expect("undecodable lease");
                            assert_eq!(
                                components
                                    .get(4)
                                    .expect("lease key missing prefix")
                                    .as_bytes(),
                                record.lease.prefix.encode(),
                                "lease record prefix diverges from its storage key"
                            );
                            space.leases.insert(record.lease.id, record);
                        }
                        other => panic!("unknown space record kind {other}"),
                    }
                }
                other => panic!("unknown root component {other}"),
            }
        }
        Ok(out)
    }

    async fn oplog(
        &self,
        from: DeviceSeq,
        through: DeviceSeq,
    ) -> Result<Vec<(DeviceSeq, CommitRecord)>, StorageError> {
        let mut out = Vec::new();
        if from > through {
            return Ok(out);
        }
        let end = homebase_core::storage::prefix_successor(&oplog_scan());
        let mut scan = self.store.scan(oplog_key(from), end);
        while let Some((storage_key, bytes)) = scan.next().await? {
            let components = decode_components(&storage_key).expect("undecodable storage key");
            let seq = DeviceSeq(u64_at(&components, 3, "commit seq"));
            if seq > through {
                break;
            }
            let record = CommitRecord::decode(&bytes).expect("undecodable commit record");
            out.push((seq, record));
        }
        Ok(out)
    }

    async fn leases_covering(
        &self,
        space: SpaceId,
        prefixes: &[Key],
    ) -> Result<Vec<HeldLease>, StorageError> {
        // Every component-wise ancestor of every query (the query
        // itself included) is one point read; dedup across queries.
        let mut candidates = std::collections::BTreeSet::new();
        for prefix in prefixes {
            let components = prefix.components();
            for depth in 1..=components.len() {
                let ancestor = Key::new(components[..depth].to_vec())
                    .expect("a prefix of a valid key is a valid key");
                candidates.insert(lease_key(space, &ancestor));
            }
        }
        let mut out = Vec::new();
        for candidate in candidates {
            if let Some(bytes) = self.store.get(&candidate).await? {
                out.push(HeldLease::decode(&bytes).expect("undecodable lease"));
            }
        }
        Ok(out)
    }

    async fn watermark(
        &self,
        space: SpaceId,
        range: &Range,
    ) -> Result<Option<AdmissionSeq>, StorageError> {
        let mut out = None;
        for ancestor in watermark_ancestors(range) {
            if let Some(bytes) = self.store.get(&watermark_key(space, &ancestor)).await? {
                let at = WatermarkRecord::decode(&bytes)
                    .expect("undecodable watermark")
                    .at;
                out = Some(out.map_or(at, |current: AdmissionSeq| current.max(at)));
            }
        }
        Ok(out)
    }

    async fn record_device(&self, id: DeviceId) -> Result<(), StorageError> {
        let mut batch = WriteBatch::new();
        batch.put(device_key(), DeviceRecord { id }.encode());
        self.store.apply(batch).await
    }

    async fn reserve_commit(
        &self,
        space: SpaceId,
        entries: Vec<(Key, Value)>,
    ) -> Result<ReservedCommit, StorageError> {
        let seq = match self.store.get(&seq_key()).await? {
            Some(bytes) => {
                SeqRecord::decode(&bytes)
                    .expect("undecodable seq record")
                    .next
            }
            None => DeviceSeq(1),
        };
        let high = match self.store.get(&ver_key()).await? {
            Some(bytes) => {
                VerHighRecord::decode(&bytes)
                    .expect("undecodable ver record")
                    .high
            }
            None => Ver(0),
        };
        let record = CommitRecord {
            space,
            entries: entries
                .into_iter()
                .enumerate()
                .map(|(i, (key, value))| PutEntry {
                    key,
                    value,
                    ver: Ver(high.0 + 1 + i as u64),
                })
                .collect(),
        };
        let ver_high = Ver(high.0 + record.entries.len() as u64);
        Ok(ReservedCommit {
            seq,
            ver_high,
            record,
        })
    }

    async fn commit(&self, reserved: ReservedCommit) -> Result<Committed, StorageError> {
        let entries_len = reserved.record.entries.len() as u64;
        let expected_high = Ver(reserved
            .ver_high
            .0
            .checked_sub(entries_len)
            .ok_or_else(|| {
                StorageError("malformed commit reservation: ver high below entry count".into())
            })?);
        let current_seq = match self.store.get(&seq_key()).await? {
            Some(bytes) => {
                SeqRecord::decode(&bytes)
                    .expect("undecodable seq record")
                    .next
            }
            None => DeviceSeq(1),
        };
        let current_high = match self.store.get(&ver_key()).await? {
            Some(bytes) => {
                VerHighRecord::decode(&bytes)
                    .expect("undecodable ver record")
                    .high
            }
            None => Ver(0),
        };
        if current_seq != reserved.seq || current_high != expected_high {
            return Err(StorageError(
                "stale commit reservation: counters advanced before commit".into(),
            ));
        }

        let mut batch = WriteBatch::new();
        batch.put(oplog_key(reserved.seq), reserved.record.encode());
        batch.put(
            seq_key(),
            SeqRecord {
                next: DeviceSeq(reserved.seq.0 + 1),
            }
            .encode(),
        );
        batch.put(
            ver_key(),
            VerHighRecord {
                high: reserved.ver_high,
            }
            .encode(),
        );
        self.store.apply(batch).await?;
        Ok(Committed {
            seq: reserved.seq,
            ver_high: reserved.ver_high,
        })
    }

    async fn trim_oplog(&self, through: DeviceSeq) -> Result<(), StorageError> {
        let queued = collect_scan(self.store.scan_prefix(&oplog_scan())).await?;
        let mut batch = WriteBatch::new();
        for (storage_key, _) in queued {
            let components = decode_components(&storage_key).expect("undecodable storage key");
            let seq = DeviceSeq(u64_at(&components, 3, "commit seq"));
            if seq > through {
                break; // ordered scan: everything after is newer
            }
            batch.delete(storage_key);
        }
        if !batch.is_empty() {
            self.store.apply(batch).await?;
        }
        Ok(())
    }

    async fn discard_from(&self, from: DeviceSeq) -> Result<(), StorageError> {
        let queued = collect_scan(self.store.scan_prefix(&oplog_scan())).await?;
        let mut batch = WriteBatch::new();
        for (storage_key, _) in queued {
            let components = decode_components(&storage_key).expect("undecodable storage key");
            let seq = DeviceSeq(u64_at(&components, 3, "commit seq"));
            if seq >= from {
                batch.delete(storage_key);
            }
        }
        if !batch.is_empty() {
            self.store.apply(batch).await?;
        }
        Ok(())
    }

    async fn advance_watermark(
        &self,
        space: SpaceId,
        range: &Range,
        at: AdmissionSeq,
        ver_seen: Ver,
    ) -> Result<(), StorageError> {
        let high = match self.store.get(&ver_key()).await? {
            Some(bytes) => {
                VerHighRecord::decode(&bytes)
                    .expect("undecodable ver record")
                    .high
            }
            None => Ver(0),
        };
        let mut batch = WriteBatch::new();
        batch.put(watermark_key(space, range), WatermarkRecord { at }.encode());
        batch.put(
            ver_key(),
            VerHighRecord {
                high: high.max(ver_seen),
            }
            .encode(),
        );
        self.store.apply(batch).await
    }

    async fn record_clock(&self, high: Timestamp) -> Result<(), StorageError> {
        let mut batch = WriteBatch::new();
        batch.put(clock_key(), ClockRecord { high_water: high }.encode());
        self.store.apply(batch).await
    }

    async fn record_leases(
        &self,
        space: SpaceId,
        leases: &[HeldLease],
    ) -> Result<(), StorageError> {
        let mut batch = WriteBatch::new();
        for held in leases {
            batch.put(lease_key(space, &held.lease.prefix), held.encode());
        }
        self.store.apply(batch).await
    }

    async fn retire_leases(&self, space: SpaceId, ids: &[LeaseId]) -> Result<(), StorageError> {
        // Records key by prefix, server speaks ids. Same short scan as
        // drop_leases, but rewrite matching records with `retiring = true`.
        let scan = encode_components(&space_kind(space, SpaceKind::Lease));
        let mut batch = WriteBatch::new();
        for (storage_key, bytes) in collect_scan(self.store.scan_prefix(&scan)).await? {
            let mut record = HeldLease::decode(&bytes).expect("undecodable lease");
            if ids.contains(&record.lease.id) && !record.retiring {
                record.retiring = true;
                batch.put(storage_key, record.encode());
            }
        }
        if !batch.is_empty() {
            self.store.apply(batch).await?;
        }
        Ok(())
    }

    async fn drop_leases(&self, space: SpaceId, ids: &[LeaseId]) -> Result<(), StorageError> {
        // The server speaks ids; records key by prefix. A space holds
        // few leases, so the resolution is one short scan.
        let scan = encode_components(&space_kind(space, SpaceKind::Lease));
        let mut batch = WriteBatch::new();
        for (storage_key, bytes) in collect_scan(self.store.scan_prefix(&scan)).await? {
            let record = HeldLease::decode(&bytes).expect("undecodable lease");
            if ids.contains(&record.lease.id) {
                batch.delete(storage_key);
            }
        }
        if !batch.is_empty() {
            self.store.apply(batch).await?;
        }
        Ok(())
    }

    async fn record_codec(&self, space: SpaceId, record: &CodecRecord) -> Result<(), StorageError> {
        let mut batch = WriteBatch::new();
        batch.put(codec_key(space), record.encode());
        self.store.apply(batch).await
    }
}

// ---------------------------------------------------------------------------
// record encodings (used by the reference implementation; other
// implementations are free to shred records into columns)

const DEVICE_RECORD_VERSION: u8 = 1;
const SEQ_RECORD_VERSION: u8 = 1;
const VER_HIGH_RECORD_VERSION: u8 = 1;
const WATERMARK_RECORD_VERSION: u8 = 1;
const CLOCK_RECORD_VERSION: u8 = 1;
const CODEC_RECORD_VERSION: u8 = 1;
const LEASE_RECORD_VERSION: u8 = 2;
const COMMIT_RECORD_VERSION: u8 = 1;

impl DeviceRecord {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(1 + 16);
        out.push(DEVICE_RECORD_VERSION);
        out.extend_from_slice(&self.id.0);
        out
    }

    pub fn decode(bytes: &[u8]) -> Option<Self> {
        let mut r = Reader::new(bytes);
        if r.u8()? != DEVICE_RECORD_VERSION {
            return None;
        }
        let id = DeviceId(r.bytes16()?);
        r.end()?;
        Some(Self { id })
    }
}

impl SeqRecord {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(1 + 8);
        out.push(SEQ_RECORD_VERSION);
        out.extend_from_slice(&self.next.0.to_be_bytes());
        out
    }

    pub fn decode(bytes: &[u8]) -> Option<Self> {
        let mut r = Reader::new(bytes);
        if r.u8()? != SEQ_RECORD_VERSION {
            return None;
        }
        let next = DeviceSeq(r.u64()?);
        r.end()?;
        Some(Self { next })
    }
}

impl VerHighRecord {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(1 + 8);
        out.push(VER_HIGH_RECORD_VERSION);
        out.extend_from_slice(&self.high.0.to_be_bytes());
        out
    }

    pub fn decode(bytes: &[u8]) -> Option<Self> {
        let mut r = Reader::new(bytes);
        if r.u8()? != VER_HIGH_RECORD_VERSION {
            return None;
        }
        let high = Ver(r.u64()?);
        r.end()?;
        Some(Self { high })
    }
}

impl ClockRecord {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(1 + 8);
        out.push(CLOCK_RECORD_VERSION);
        out.extend_from_slice(&self.high_water.0.to_be_bytes());
        out
    }

    pub fn decode(bytes: &[u8]) -> Option<Self> {
        let mut r = Reader::new(bytes);
        if r.u8()? != CLOCK_RECORD_VERSION {
            return None;
        }
        let high_water = Timestamp(r.u64()?);
        r.end()?;
        Some(Self { high_water })
    }
}

impl WatermarkRecord {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(1 + 8);
        out.push(WATERMARK_RECORD_VERSION);
        out.extend_from_slice(&self.at.0.to_be_bytes());
        out
    }

    pub fn decode(bytes: &[u8]) -> Option<Self> {
        let mut r = Reader::new(bytes);
        if r.u8()? != WATERMARK_RECORD_VERSION {
            return None;
        }
        let at = AdmissionSeq(r.u64()?);
        r.end()?;
        Some(Self { at })
    }
}

impl CodecRecord {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(1 + 8 + self.sealed.len());
        out.push(CODEC_RECORD_VERSION);
        out.extend_from_slice(&self.space_key_epoch.to_be_bytes());
        out.extend_from_slice(&self.sealed);
        out
    }

    pub fn decode(bytes: &[u8]) -> Option<Self> {
        let mut r = Reader::new(bytes);
        if r.u8()? != CODEC_RECORD_VERSION {
            return None;
        }
        Some(Self {
            space_key_epoch: r.u64()?,
            sealed: r.rest().to_vec(),
        })
    }
}

impl HeldLease {
    pub fn encode(&self) -> Vec<u8> {
        let l = &self.lease;
        let mut out = Vec::with_capacity(1 + 8 + 1 + 1 + 8 + 8 + 32 + 32 + 10);
        out.push(LEASE_RECORD_VERSION);
        out.extend_from_slice(&l.id.0.to_be_bytes());
        out.push(match l.mode {
            LeaseMode::Read => 0,
            LeaseMode::Write => 1,
        });
        out.push(l.stealable as u8);
        out.extend_from_slice(&l.epoch.0.to_be_bytes());
        let ttl_ms = l.ttl.as_millis().min(u64::MAX as u128) as u64;
        out.extend_from_slice(&ttl_ms.to_be_bytes());
        out.extend_from_slice(&self.deadline.wall.0.to_be_bytes());
        out.extend_from_slice(&self.deadline.mono.0.to_be_bytes());
        out.extend_from_slice(&self.deadline.lineage.0);
        out.push(self.retiring as u8);
        match self.barrier {
            Some(barrier) => {
                out.push(1);
                out.extend_from_slice(&barrier.0.to_be_bytes());
            }
            None => out.push(0),
        }
        out.extend_from_slice(&l.prefix.encode());
        out
    }

    pub fn decode(bytes: &[u8]) -> Option<Self> {
        let mut r = Reader::new(bytes);
        let version = r.u8()?;
        if !matches!(version, 1 | LEASE_RECORD_VERSION) {
            return None;
        }
        let id = LeaseId(r.u64()?);
        let mode = match r.u8()? {
            0 => LeaseMode::Read,
            1 => LeaseMode::Write,
            _ => return None,
        };
        let stealable = match r.u8()? {
            0 => false,
            1 => true,
            _ => return None,
        };
        let epoch = homebase_core::tag::Epoch(r.u64()?);
        let ttl = Duration::from_millis(r.u64()?);
        let deadline = HybridTimestamp {
            wall: Timestamp(r.u64()?),
            mono: Timestamp(r.u64()?),
            lineage: Lineage(r.bytes16()?),
        };
        let (retiring, barrier) = if version == 1 {
            (false, None)
        } else {
            let retiring = match r.u8()? {
                0 => false,
                1 => true,
                _ => return None,
            };
            let barrier = match r.u8()? {
                0 => None,
                1 => Some(AdmissionSeq(r.u64()?)),
                _ => return None,
            };
            (retiring, barrier)
        };
        let prefix = Key::decode(r.rest()).ok()?;
        Some(Self {
            lease: Lease {
                id,
                prefix,
                mode,
                epoch,
                ttl,
                stealable,
            },
            deadline,
            barrier,
            retiring,
        })
    }
}

impl CommitRecord {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = vec![COMMIT_RECORD_VERSION];
        out.extend_from_slice(&self.space.0);
        out.extend_from_slice(&(self.entries.len() as u32).to_be_bytes());
        for entry in &self.entries {
            let key = entry.key.encode();
            out.extend_from_slice(&(key.len() as u32).to_be_bytes());
            out.extend_from_slice(&key);
            out.extend_from_slice(&entry.ver.0.to_be_bytes());
            match &entry.value {
                Value::Absent => out.push(0),
                Value::Present(bytes) => {
                    out.push(1);
                    out.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
                    out.extend_from_slice(bytes);
                }
            }
        }
        out
    }

    pub fn decode(bytes: &[u8]) -> Option<Self> {
        let mut r = Reader::new(bytes);
        if r.u8()? != COMMIT_RECORD_VERSION {
            return None;
        }
        let space = SpaceId(r.bytes16()?);
        let count = r.u32()? as usize;
        let mut entries = Vec::with_capacity(count.min(1024));
        for _ in 0..count {
            let key_len = r.u32()? as usize;
            let key = Key::decode(r.take(key_len)?).ok()?;
            let ver = Ver(r.u64()?);
            let value = match r.u8()? {
                0 => Value::Absent,
                1 => {
                    let len = r.u32()? as usize;
                    Value::Present(r.take(len)?.to_vec())
                }
                _ => return None,
            };
            entries.push(PutEntry { key, value, ver });
        }
        r.end()?;
        Some(Self { space, entries })
    }
}

fn single_byte(components: &[KeyComponent], index: usize, what: &str) -> u8 {
    let bytes = components
        .get(index)
        .unwrap_or_else(|| panic!("storage key missing {what}"))
        .as_bytes();
    assert_eq!(bytes.len(), 1, "{what} must be one byte");
    bytes[0]
}

fn u64_at(components: &[KeyComponent], index: usize, what: &str) -> u64 {
    let bytes = components
        .get(index)
        .unwrap_or_else(|| panic!("storage key missing {what}"))
        .as_bytes();
    u64::from_be_bytes(
        bytes
            .try_into()
            .unwrap_or_else(|_| panic!("{what} must be 8 bytes")),
    )
}

struct Reader<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, pos: 0 }
    }

    fn u8(&mut self) -> Option<u8> {
        let b = *self.bytes.get(self.pos)?;
        self.pos += 1;
        Some(b)
    }

    fn u32(&mut self) -> Option<u32> {
        let slice = self.bytes.get(self.pos..self.pos + 4)?;
        self.pos += 4;
        Some(u32::from_be_bytes(slice.try_into().unwrap()))
    }

    fn u64(&mut self) -> Option<u64> {
        let slice = self.bytes.get(self.pos..self.pos + 8)?;
        self.pos += 8;
        Some(u64::from_be_bytes(slice.try_into().unwrap()))
    }

    fn bytes16(&mut self) -> Option<[u8; 16]> {
        let slice = self.bytes.get(self.pos..self.pos + 16)?;
        self.pos += 16;
        Some(slice.try_into().unwrap())
    }

    fn take(&mut self, len: usize) -> Option<&'a [u8]> {
        let slice = self.bytes.get(self.pos..self.pos.checked_add(len)?)?;
        self.pos += len;
        Some(slice)
    }

    fn rest(&self) -> &'a [u8] {
        &self.bytes[self.pos..]
    }

    /// For fixed-shape records: trailing garbage is corruption, not slack.
    fn end(&self) -> Option<()> {
        (self.pos == self.bytes.len()).then_some(())
    }
}

// ---------------------------------------------------------------------------
// conformance

pub mod conformance {
    //! Reusable [`MetaStore`] conformance: any implementation — the
    //! ordered reference, multilite's SQLite tables — must pass
    //! [`run_all`] against a fresh store. Drives the full transition
    //! lifecycle and certifies at every step; this is the merge gate for
    //! new backends.

    use super::*;

    fn key(components: &[&[u8]]) -> Key {
        Key::from_bytes(components.iter().copied()).unwrap()
    }

    /// A hybrid stamp with both rulers at `ms`, on a test lineage.
    fn stamp(ms: u64) -> HybridTimestamp {
        HybridTimestamp {
            wall: Timestamp(ms),
            mono: Timestamp(ms),
            lineage: Lineage([9; 16]),
        }
    }

    fn sample_lease() -> HeldLease {
        HeldLease {
            lease: Lease {
                id: LeaseId(42),
                prefix: key(&[b"db", b"pay"]),
                mode: LeaseMode::Write,
                epoch: homebase_core::tag::Epoch(9),
                ttl: Duration::from_secs(300),
                stealable: true,
            },
            deadline: stamp(1_300),
            barrier: None,
            retiring: false,
        }
    }

    async fn commit_entries<M: MetaStore>(
        store: &M,
        space: SpaceId,
        entries: Vec<(Key, Value)>,
    ) -> Committed {
        let reserved = store.reserve_commit(space, entries).await.unwrap();
        store.commit(reserved).await.unwrap()
    }

    /// Drives the whole lifecycle against a **fresh** store. Panics on any
    /// contract violation.
    pub async fn run_all<M: MetaStore>(store: &M) {
        let space = SpaceId([7; 16]);
        let link = SpaceId([8; 16]);

        // Fresh: empty, certifiable.
        let state = audit(store).await;
        assert_eq!(
            state,
            ClientState::default(),
            "a fresh store remembers nothing"
        );

        // Genesis.
        let device = DeviceId([1; 16]);
        store.record_device(device).await.unwrap();
        assert_eq!(audit(store).await.device, Some(device));

        // Three commits through the one queue, two spaces interleaved.
        // The store assigns both counters: seqs dense from 1, vers
        // climbing from the high-water.
        let first = commit_entries(
            store,
            space,
            vec![
                (key(&[b"db", b"a"]), Value::Present(b"ciphertext".to_vec())),
                (key(&[b"db", b"gone"]), Value::Absent),
            ],
        )
        .await;
        assert_eq!(
            first,
            Committed {
                seq: DeviceSeq(1),
                ver_high: Ver(2)
            }
        );
        let second = commit_entries(
            store,
            link,
            vec![(key(&[b"dir", b"entry"]), Value::Present(b"sealed".to_vec()))],
        )
        .await;
        assert_eq!(
            second,
            Committed {
                seq: DeviceSeq(2),
                ver_high: Ver(3)
            }
        );
        let third = commit_entries(
            store,
            space,
            vec![(key(&[b"db", b"a"]), Value::Present(b"newer".to_vec()))],
        )
        .await;
        assert_eq!(
            third,
            Committed {
                seq: DeviceSeq(3),
                ver_high: Ver(4)
            }
        );

        let state = audit(store).await;
        assert_eq!(state.next_seq, Some(DeviceSeq(4)));
        assert_eq!(state.ver_high, Some(Ver(4)));
        assert_eq!(state.oplog.len(), 3);
        assert_eq!(state.oplog[&DeviceSeq(1)].space, space);
        assert_eq!(state.oplog[&DeviceSeq(1)].entries[0].ver, Ver(1));
        assert_eq!(
            state.oplog[&DeviceSeq(1)].entries[1].ver,
            Ver(2),
            "consecutive in order"
        );
        assert_eq!(state.oplog[&DeviceSeq(2)].space, link);
        assert_eq!(
            state.oplog[&DeviceSeq(3)].entries[0].ver,
            Ver(4),
            "chain continued"
        );

        // The queue read sees exactly what load sees: a seq range,
        // ascending; a point lookup is the one-seq range.
        let window = store.oplog(DeviceSeq(1), DeviceSeq(2)).await.unwrap();
        assert_eq!(
            window.iter().map(|(seq, _)| *seq).collect::<Vec<_>>(),
            vec![DeviceSeq(1), DeviceSeq(2)],
            "a range read is inclusive on both ends"
        );
        assert_eq!(window[0].1, state.oplog[&DeviceSeq(1)]);
        assert_eq!(
            store
                .oplog(DeviceSeq(1), DeviceSeq(u64::MAX))
                .await
                .unwrap()
                .len(),
            3,
            "an open-ended range reads the whole queue"
        );
        assert_eq!(
            store.oplog(DeviceSeq(2), DeviceSeq(2)).await.unwrap(),
            vec![(DeviceSeq(2), state.oplog[&DeviceSeq(2)].clone())]
        );
        assert!(
            store
                .oplog(DeviceSeq(9), DeviceSeq(9))
                .await
                .unwrap()
                .is_empty(),
            "never minted"
        );

        // A push ack trims the acknowledged prefix — nothing else to
        // clear; grouping is a wire-time choice, never durable state.
        store.trim_oplog(DeviceSeq(1)).await.unwrap();
        let state = audit(store).await;
        assert_eq!(
            state.oplog.keys().copied().collect::<Vec<_>>(),
            vec![DeviceSeq(2), DeviceSeq(3)],
            "trim takes a prefix"
        );
        assert_eq!(state.next_seq, Some(DeviceSeq(4)));
        store.trim_oplog(DeviceSeq(1)).await.unwrap();
        assert_eq!(audit(store).await.oplog.len(), 2, "re-ack is a no-op");
        assert!(
            store
                .oplog(DeviceSeq(1), DeviceSeq(1))
                .await
                .unwrap()
                .is_empty(),
            "a trimmed seq is gone from range reads too"
        );

        // A pull moves the sync point and raises the ver high-water to
        // what the cut carried; the next commit climbs past it.
        let db_range = Range::Prefix(key(&[b"db"]));
        store
            .advance_watermark(space, &db_range, AdmissionSeq(40), Ver(10))
            .await
            .unwrap();
        let state = audit(store).await;
        assert_eq!(state.spaces[&space].watermarks[&db_range], AdmissionSeq(40));
        assert_eq!(state.ver_high, Some(Ver(10)));
        assert_eq!(
            store.watermark(space, &db_range).await.unwrap(),
            Some(AdmissionSeq(40))
        );
        assert_eq!(
            store.watermark(link, &db_range).await.unwrap(),
            None,
            "no pull yet"
        );
        let fourth = commit_entries(
            store,
            space,
            vec![(key(&[b"db", b"b"]), Value::Present(b"post-pull".to_vec()))],
        )
        .await;
        assert_eq!(
            fourth,
            Committed {
                seq: DeviceSeq(4),
                ver_high: Ver(11)
            }
        );

        // A pull carrying older vers never regresses the high-water.
        store
            .advance_watermark(space, &db_range, AdmissionSeq(41), Ver(5))
            .await
            .unwrap();
        assert_eq!(audit(store).await.ver_high, Some(Ver(11)));
        store
            .advance_watermark(space, &Range::Full, AdmissionSeq(50), Ver(5))
            .await
            .unwrap();
        let state = audit(store).await;
        assert_eq!(
            state.spaces[&space].watermarks[&db_range],
            AdmissionSeq(41),
            "exact prefix cursor is not rewritten by ancestor progress"
        );
        assert_eq!(
            store.watermark(space, &db_range).await.unwrap(),
            Some(AdmissionSeq(50)),
            "effective prefix cursor is ancestor max"
        );

        // Duplicate keys in one commit behave like a sequence — the
        // kernel's own within-batch rule: later occurrences carry
        // strictly greater vers, and the state still certifies.
        let dup = commit_entries(
            store,
            space,
            vec![
                (key(&[b"db", b"c"]), Value::Present(b"twice".to_vec())),
                (
                    key(&[b"db", b"c"]),
                    Value::Present(b"the second wins".to_vec()),
                ),
            ],
        )
        .await;
        assert_eq!(
            dup,
            Committed {
                seq: DeviceSeq(5),
                ver_high: Ver(13)
            }
        );
        let state = audit(store).await;
        let entries = &state.oplog[&DeviceSeq(5)].entries;
        assert_eq!(entries[0].ver, Ver(12));
        assert_eq!(entries[1].ver, Ver(13));

        // A merged wire batch acks under its last seq; the group ack
        // drops everything through it and nothing after — prefix-only by
        // construction, no gap is even expressible.
        store.trim_oplog(DeviceSeq(4)).await.unwrap();
        let state = audit(store).await;
        assert_eq!(
            state.oplog.keys().copied().collect::<Vec<_>>(),
            vec![DeviceSeq(5)],
            "later commits survive a deep trim"
        );

        // Rollback: discard drops the suffix — the resolution for a
        // rejected push the caller chooses not to repair.
        let doomed = commit_entries(
            store,
            space,
            vec![(key(&[b"db", b"doomed"]), Value::Present(b"x".to_vec()))],
        )
        .await;
        store.discard_from(doomed.seq).await.unwrap();
        let state = audit(store).await;
        assert_eq!(
            state.oplog.keys().copied().collect::<Vec<_>>(),
            vec![DeviceSeq(5)],
            "discard takes a suffix"
        );
        assert_eq!(state.next_seq, Some(DeviceSeq(7)), "counters never rewind");

        // A commit after a discard leaves a gap — legal, because the seq
        // counter never rewinds. The state still certifies and the head
        // window shows the hole.
        let after_gap = commit_entries(
            store,
            space,
            vec![(key(&[b"db", b"post"]), Value::Present(b"gap".to_vec()))],
        )
        .await;
        assert_eq!(after_gap.seq, DeviceSeq(7));
        let state = audit(store).await;
        assert_eq!(
            state.oplog.keys().copied().collect::<Vec<_>>(),
            vec![DeviceSeq(5), DeviceSeq(7)],
            "a discarded seq is never re-minted"
        );
        assert!(
            store
                .oplog(DeviceSeq(6), DeviceSeq(6))
                .await
                .unwrap()
                .is_empty(),
            "the hole is real"
        );
        assert_eq!(
            store
                .oplog(DeviceSeq(1), DeviceSeq(u64::MAX))
                .await
                .unwrap()
                .iter()
                .map(|(seq, _)| *seq)
                .collect::<Vec<_>>(),
            vec![DeviceSeq(5), DeviceSeq(7)],
            "the range read walks straight over the gap"
        );

        // The clock high-water is a plain overwrite — advanced at every
        // stamp, and lowerable on purpose (re-anchoring after a
        // poisoned open lands on an earlier timeline).
        store.record_clock(Timestamp(1_000)).await.unwrap();
        assert_eq!(audit(store).await.clock_high, Some(Timestamp(1_000)));
        store.record_clock(Timestamp(500)).await.unwrap();
        assert_eq!(audit(store).await.clock_high, Some(Timestamp(500)));
        store.record_clock(Timestamp(2_000)).await.unwrap();

        // Lease churn + codec cache, across both spaces.
        // A batch grant is recorded atomically as a batch.
        let second_lease = HeldLease {
            lease: Lease {
                id: LeaseId(43),
                prefix: key(&[b"db", b"hr"]),
                mode: LeaseMode::Read,
                epoch: homebase_core::tag::Epoch(10),
                ttl: Duration::from_secs(300),
                stealable: false,
            },
            deadline: stamp(2_000),
            barrier: Some(AdmissionSeq(40)),
            retiring: false,
        };
        store
            .record_leases(space, &[sample_lease(), second_lease.clone()])
            .await
            .unwrap();
        let link_lease = HeldLease {
            lease: Lease {
                id: LeaseId(7),
                prefix: key(&[b"dir"]),
                mode: LeaseMode::Write,
                epoch: homebase_core::tag::Epoch(1),
                ttl: Duration::from_secs(60),
                stealable: false,
            },
            deadline: stamp(900),
            barrier: None,
            retiring: false,
        };
        store
            .record_leases(link, std::slice::from_ref(&link_lease))
            .await
            .unwrap();
        store
            .record_codec(
                space,
                &CodecRecord {
                    space_key_epoch: 0,
                    sealed: b"sealed".to_vec(),
                },
            )
            .await
            .unwrap();

        let state = audit(store).await;
        assert_eq!(state.spaces[&space].leases[&LeaseId(42)], sample_lease());
        assert_eq!(state.spaces[&space].leases[&LeaseId(43)], second_lease);
        assert_eq!(state.spaces[&link].leases[&LeaseId(7)], link_lease);
        // The lease read answers "who covers these keys?" — ancestors
        // only, never the whole space.
        let covering = store
            .leases_covering(space, &[key(&[b"db", b"pay", b"row1"])])
            .await
            .unwrap();
        assert_eq!(covering, vec![sample_lease()], "an ancestor prefix covers");
        let covering = store
            .leases_covering(space, &[key(&[b"db", b"pay"]), key(&[b"db", b"hr", b"x"])])
            .await
            .unwrap();
        assert_eq!(
            covering,
            vec![second_lease.clone(), sample_lease()],
            "multiple queries dedup into one answer, in prefix order"
        );
        assert!(
            store
                .leases_covering(space, &[key(&[b"db"])])
                .await
                .unwrap()
                .is_empty(),
            "a descendant lease does not cover its ancestor"
        );
        assert_eq!(
            store
                .leases_covering(link, &[key(&[b"dir", b"entry"])])
                .await
                .unwrap(),
            vec![link_lease.clone()]
        );
        assert_eq!(
            state.spaces[&space].codec,
            Some(CodecRecord {
                space_key_epoch: 0,
                sealed: b"sealed".to_vec()
            })
        );

        // Renewal overwrites in place; release forgets.
        // A renewal overwrites in place with a fresh deadline stamp.
        let renewed = HeldLease {
            lease: Lease {
                ttl: Duration::from_secs(600),
                ..sample_lease().lease
            },
            deadline: stamp(5_000),
            barrier: None,
            retiring: false,
        };
        store
            .record_leases(space, std::slice::from_ref(&renewed))
            .await
            .unwrap();
        assert_eq!(
            audit(store).await.spaces[&space].leases[&LeaseId(42)],
            renewed
        );
        // A re-grant of the same prefix replaces the old record: one
        // live lease per (space, prefix).
        let regrant = HeldLease {
            lease: Lease {
                id: LeaseId(99),
                ..sample_lease().lease
            },
            deadline: stamp(9_000),
            barrier: None,
            retiring: false,
        };
        store
            .record_leases(space, std::slice::from_ref(&regrant))
            .await
            .unwrap();
        let state = audit(store).await;
        assert!(
            !state.spaces[&space].leases.contains_key(&LeaseId(42)),
            "the superseded grant is gone"
        );
        assert_eq!(state.spaces[&space].leases[&LeaseId(99)], regrant);

        store
            .drop_leases(space, &[LeaseId(99), LeaseId(43)])
            .await
            .unwrap();
        assert!(audit(store).await.spaces[&space].leases.is_empty());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use homebase_core::storage::MemoryStore;
    use homebase_core::tag::Epoch;
    use pollster::block_on;

    const SPACE: SpaceId = SpaceId([7; 16]);
    const LINK: SpaceId = SpaceId([8; 16]);

    fn key(components: &[&[u8]]) -> Key {
        Key::from_bytes(components.iter().copied()).unwrap()
    }

    fn stamp(ms: u64) -> HybridTimestamp {
        HybridTimestamp {
            wall: Timestamp(ms),
            mono: Timestamp(ms),
            lineage: Lineage([9; 16]),
        }
    }

    fn sample_commit() -> CommitRecord {
        CommitRecord {
            space: SPACE,
            entries: vec![
                PutEntry {
                    key: key(&[b"db", b"a"]),
                    value: Value::Present(b"ciphertext".to_vec()),
                    ver: Ver(3),
                },
                PutEntry {
                    key: key(&[b"db", b"gone"]),
                    value: Value::Absent,
                    ver: Ver(3),
                },
                PutEntry {
                    key: key(&[b"db", b"empty"]),
                    value: Value::Present(vec![]),
                    ver: Ver(3),
                },
            ],
        }
    }

    /// A hand-built state whose counters cover its queue, for corrupting.
    fn covered_state() -> ClientState {
        let mut state = ClientState::default();
        state.oplog.insert(DeviceSeq(1), sample_commit());
        state.next_seq = Some(DeviceSeq(2));
        state.ver_high = Some(Ver(3));
        state
    }

    #[test]
    fn records_roundtrip() {
        let device = DeviceRecord {
            id: DeviceId([3; 16]),
        };
        assert_eq!(DeviceRecord::decode(&device.encode()), Some(device));

        let seq = SeqRecord {
            next: DeviceSeq(17),
        };
        assert_eq!(SeqRecord::decode(&seq.encode()), Some(seq));

        let ver = VerHighRecord { high: Ver(9) };
        assert_eq!(VerHighRecord::decode(&ver.encode()), Some(ver));

        let watermark = WatermarkRecord {
            at: AdmissionSeq(99),
        };
        assert_eq!(
            WatermarkRecord::decode(&watermark.encode()),
            Some(watermark)
        );

        let clock = ClockRecord {
            high_water: Timestamp(123_456),
        };
        assert_eq!(ClockRecord::decode(&clock.encode()), Some(clock));

        let codec = CodecRecord {
            space_key_epoch: 0,
            sealed: b"sealed-bundle".to_vec(),
        };
        assert_eq!(CodecRecord::decode(&codec.encode()), Some(codec));

        let lease = HeldLease {
            lease: Lease {
                id: LeaseId(42),
                prefix: key(&[b"db", b"pay"]),
                mode: LeaseMode::Write,
                epoch: Epoch(9),
                ttl: Duration::from_secs(300),
                stealable: true,
            },
            deadline: stamp(1_234),
            barrier: Some(AdmissionSeq(17)),
            retiring: true,
        };
        assert_eq!(HeldLease::decode(&lease.encode()), Some(lease));

        let commit = sample_commit();
        assert_eq!(CommitRecord::decode(&commit.encode()), Some(commit));
        let empty = CommitRecord {
            space: LINK,
            entries: vec![],
        };
        assert_eq!(CommitRecord::decode(&empty.encode()), Some(empty));
    }

    #[test]
    fn fixed_records_reject_trailing_garbage() {
        let mut bytes = SeqRecord { next: DeviceSeq(1) }.encode();
        bytes.push(0);
        assert_eq!(SeqRecord::decode(&bytes), None);

        let mut bytes = sample_commit().encode();
        bytes.push(0);
        assert_eq!(CommitRecord::decode(&bytes), None);
    }

    #[test]
    fn reference_keys_stay_inside_the_meta_brand() {
        let brand = meta_scan_all();
        for storage_key in [
            device_key(),
            seq_key(),
            ver_key(),
            clock_key(),
            oplog_key(DeviceSeq(1)),
            watermark_key(SPACE, &Range::Full),
            watermark_key(SPACE, &Range::Prefix(key(&[b"db"]))),
            codec_key(SPACE),
            lease_key(SPACE, &key(&[b"db", b"pay"])),
        ] {
            assert!(
                storage_key.starts_with(&brand),
                "every reference key wears the brand"
            );
        }
        assert!(
            oplog_key(DeviceSeq(1)) < oplog_key(DeviceSeq(2)),
            "queue drains in order"
        );

        // A cohabitant under the Data brand is invisible to meta scans.
        let foreign = encode_components(&[
            KeyComponent::new(vec![StoreNamespace::Data as u8]).unwrap(),
            KeyComponent::new(b"anything".to_vec()).unwrap(),
        ]);
        assert!(!foreign.starts_with(&brand));
    }

    #[test]
    fn reference_impl_passes_conformance() {
        block_on(async {
            let store = OrderedMetaStore::new(MemoryStore::new());
            conformance::run_all(&store).await;
        });
    }

    #[test]
    fn reference_load_ignores_cohabitants() {
        block_on(async {
            let inner = MemoryStore::new();
            let mut batch = WriteBatch::new();
            batch.put(
                encode_components(&[
                    KeyComponent::new(vec![StoreNamespace::Data as u8]).unwrap(),
                    KeyComponent::new(b"user-row".to_vec()).unwrap(),
                ]),
                b"not ours".to_vec(),
            );
            inner.apply(batch).await.unwrap();

            let store = OrderedMetaStore::new(inner);
            let state = store.load().await.unwrap();
            assert_eq!(
                state,
                ClientState::default(),
                "the Data brand is none of our business"
            );
        });
    }

    #[test]
    fn certify_allows_queue_gaps() {
        // A discard followed by new commits leaves a hole (the counter
        // never rewinds) — a legal history, not corruption. Distinct
        // keys keep the ver chains clean.
        let mut state = covered_state();
        state.oplog.insert(
            DeviceSeq(3),
            CommitRecord {
                space: SPACE,
                entries: vec![PutEntry {
                    key: key(&[b"db", b"later"]),
                    value: Value::Present(b"x".to_vec()),
                    ver: Ver(4),
                }],
            },
        );
        state.next_seq = Some(DeviceSeq(4));
        state.ver_high = Some(Ver(4));
        certify(&state);
    }

    #[test]
    #[should_panic(expected = "stamped past the clock high-water")]
    fn certify_catches_stamps_from_the_future() {
        let mut state = ClientState {
            clock_high: Some(Timestamp(1_000)),
            ..ClientState::default()
        };
        let space = state.spaces.entry(SPACE).or_default();
        space.leases.insert(
            LeaseId(1),
            HeldLease {
                lease: Lease {
                    id: LeaseId(1),
                    prefix: key(&[b"db"]),
                    mode: LeaseMode::Write,
                    epoch: Epoch(1),
                    ttl: Duration::from_secs(1),
                    stealable: false,
                },
                // wall send = 100_000 − 1_000 = 99_000, far past the
                // high-water: a stamp the recorded timeline never saw.
                deadline: stamp(100_000),
                barrier: None,
                retiring: false,
            },
        );
        certify(&state);
    }

    #[test]
    #[should_panic(expected = "oplog vers regress")]
    fn certify_catches_broken_chains() {
        let mut state = covered_state();
        // A stale ver for a key seq 1 already advanced — the regression
        // the server would bounce.
        state.oplog.insert(
            DeviceSeq(2),
            CommitRecord {
                space: SPACE,
                entries: vec![PutEntry {
                    key: key(&[b"db", b"a"]),
                    value: Value::Present(b"stale".to_vec()),
                    ver: Ver(2),
                }],
            },
        );
        state.next_seq = Some(DeviceSeq(3));
        certify(&state);
    }

    #[test]
    #[should_panic(expected = "next_seq lags the oplog")]
    fn certify_catches_lagging_seq() {
        let mut state = covered_state();
        // A queue entry the seq counter never covered: a torn commit.
        state.next_seq = Some(DeviceSeq(1));
        certify(&state);
    }

    #[test]
    #[should_panic(expected = "ver high lags the oplog")]
    fn certify_catches_lagging_ver_high() {
        let mut state = covered_state();
        state.ver_high = Some(Ver(1));
        certify(&state);
    }
}
