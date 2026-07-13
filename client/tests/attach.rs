//! [`Client::attach`] edge cases and session bookkeeping.

use homebase_client::cipher::{NameKey, SpaceEnvelope, SpaceKey, SystemNonceSource};
use homebase_client::meta::OrderedMetaStore;
use homebase_client::{Client, ClientError};
use homebase_core::clock::{ManualClock, Timestamp};
use homebase_core::space::SpaceId;
use homebase_core::storage::MemoryStore;
use homebase_core::tag::DeviceId;
use pollster::block_on;

fn dev(n: u8) -> DeviceId {
    DeviceId([n; 16])
}

#[test]
fn is_attached_tracks_session_only() {
    block_on(async {
        let envelope = SpaceEnvelope::mint(NameKey([1; 32]), SpaceKey([2; 32]));
        let id = envelope.space_id();
        let mem = MemoryStore::new();
        let clock = ManualClock::new(Timestamp(0));
        let handle = |_: &SpaceId| None::<homebase::actor::SpaceHandle>;

        let client = Client::open(
            OrderedMetaStore::new(&mem),
            &handle,
            &clock,
            dev(1),
            SystemNonceSource,
        )
        .await
        .unwrap();

        assert!(!client.is_attached(id));
        client.attach(&envelope).await.unwrap();
        assert!(client.is_attached(id));
        assert_eq!(client.attached(), vec![id]);
    });
}

#[test]
fn attach_is_idempotent() {
    block_on(async {
        let envelope = SpaceEnvelope::plaintext(SpaceId([9; 16]));
        let id = envelope.space_id();
        let mem = MemoryStore::new();
        let clock = ManualClock::new(Timestamp(0));
        let handle = |_: &SpaceId| None::<homebase::actor::SpaceHandle>;

        let client = Client::open(
            OrderedMetaStore::new(&mem),
            &handle,
            &clock,
            dev(1),
            SystemNonceSource,
        )
        .await
        .unwrap();

        client.attach(&envelope).await.unwrap();
        client.attach(&envelope).await.unwrap();
        assert!(client.is_attached(id));
    });
}

#[test]
fn attach_rejects_mismatched_envelope_when_codec_present() {
    block_on(async {
        let genesis = SpaceEnvelope::mint(NameKey([3; 32]), SpaceKey([4; 32]));
        let id = genesis.space_id();
        let wrong_key = SpaceEnvelope::mint(NameKey([3; 32]), SpaceKey([5; 32]));
        assert_eq!(wrong_key.space_id(), id);

        let mem = MemoryStore::new();
        let clock = ManualClock::new(Timestamp(0));
        let handle = |_: &SpaceId| None::<homebase::actor::SpaceHandle>;

        let client = Client::open(
            OrderedMetaStore::new(&mem),
            &handle,
            &clock,
            dev(1),
            SystemNonceSource,
        )
        .await
        .unwrap();

        client.attach(&genesis).await.unwrap();
        assert_eq!(
            client.attach(&wrong_key).await.unwrap_err(),
            ClientError::CodecMismatch { id }
        );
    });
}

#[test]
fn space_without_codec_errors_missing_codec() {
    block_on(async {
        let id = SpaceId([7; 16]);
        let mem = MemoryStore::new();
        let clock = ManualClock::new(Timestamp(0));
        let handle = |_: &SpaceId| None::<homebase::actor::SpaceHandle>;

        let client = Client::open(
            OrderedMetaStore::new(&mem),
            &handle,
            &clock,
            dev(1),
            SystemNonceSource,
        )
        .await
        .unwrap();

        match client.space(id).await {
            Err(ClientError::MissingCodec(got)) => assert_eq!(got, id),
            Ok(_) => panic!("expected MissingCodec"),
            Err(_) => panic!("expected MissingCodec"),
        }
    });
}

#[test]
fn space_lazy_loads_from_codec_without_attach() {
    block_on(async {
        let envelope = SpaceEnvelope::mint(NameKey([8; 32]), SpaceKey([9; 32]));
        let id = envelope.space_id();
        let mem = MemoryStore::new();
        let clock = ManualClock::new(Timestamp(0));
        let handle = |_: &SpaceId| None::<homebase::actor::SpaceHandle>;

        {
            let client = Client::open(
                OrderedMetaStore::new(&mem),
                &handle,
                &clock,
                dev(1),
                SystemNonceSource,
            )
            .await
            .unwrap();
            client.attach(&envelope).await.unwrap();
        }

        let client = Client::open(
            OrderedMetaStore::new(&mem),
            &handle,
            &clock,
            dev(1),
            SystemNonceSource,
        )
        .await
        .unwrap();
        assert!(!client.is_attached(id));
        assert!(client.space(id).await.is_ok());
        assert!(client.is_attached(id));
    });
}
