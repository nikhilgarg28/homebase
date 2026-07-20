//! Property tests for tuple keys: the exit criteria for this module.

use homebase_core::key::{Key, MAX_COMPONENT_LEN, MAX_COMPONENTS};
use proptest::prelude::*;

/// Random keys covering both maximum depth and substantial component sizes.
/// Exact size and total-budget boundaries live in `key`'s unit tests so one
/// proptest case cannot allocate hundreds of megabytes.
fn arb_key() -> impl Strategy<Value = Key> {
    prop_oneof![
        3 => prop::collection::vec(
            prop::collection::vec(any::<u8>(), 1..=8),
            1..=MAX_COMPONENTS,
        ),
        1 => prop::collection::vec(
            prop::collection::vec(any::<u8>(), 1..=1024.min(MAX_COMPONENT_LEN)),
            1..=16,
        ),
    ]
    .prop_map(|components| Key::from_bytes(components).unwrap())
}

/// Keys drawn from a tiny alphabet of encoding-critical bytes with short
/// components, so random pairs frequently collide, share prefixes, and land
/// exactly on escape/terminator edge cases.
fn arb_clustered_key() -> impl Strategy<Value = Key> {
    let byte = prop::sample::select(vec![0x00u8, 0x01, 0x02, 0xfe, 0xff, b'a']);
    prop::collection::vec(prop::collection::vec(byte, 1..4), 1..=4)
        .prop_map(|components| Key::from_bytes(components).unwrap())
}

proptest! {
    /// decode(encode(k)) == k.
    #[test]
    fn roundtrip(k in arb_key()) {
        prop_assert_eq!(Key::decode(&k.encode()).unwrap(), k);
    }

    /// Encoding is canonical: anything that decodes re-encodes to the same bytes.
    #[test]
    fn decode_is_canonical(bytes in prop::collection::vec(any::<u8>(), 0..512)) {
        if let Ok(k) = Key::decode(&bytes) {
            prop_assert_eq!(k.encode(), bytes);
        }
    }

    /// encode(a) < encode(b) ⟺ a < b, over the full key space.
    #[test]
    fn order_preserved(a in arb_key(), b in arb_key()) {
        prop_assert_eq!(a.encode().cmp(&b.encode()), a.cmp(&b));
    }

    /// Same property, concentrated on escape/terminator edge cases.
    #[test]
    fn order_preserved_clustered(a in arb_clustered_key(), b in arb_clustered_key()) {
        prop_assert_eq!(a.encode().cmp(&b.encode()), a.cmp(&b));
    }

    /// Tuple-prefix ⟺ encoded-byte-prefix (kills separator-injection bugs;
    /// makes prefix scans plain byte-range scans).
    #[test]
    fn prefix_correspondence(a in arb_clustered_key(), b in arb_clustered_key()) {
        prop_assert_eq!(a.starts_with(&b), a.encode().starts_with(&b.encode()));
    }

    /// Every truncation of a key to its first n components is a tuple-prefix
    /// and a byte-prefix of the whole.
    #[test]
    fn truncation_is_prefix(k in arb_key(), n in 1usize..=MAX_COMPONENTS) {
        let n = n.min(k.components().len());
        let p = Key::new(k.components()[..n].to_vec()).unwrap();
        prop_assert!(k.starts_with(&p));
        prop_assert!(k.encode().starts_with(&p.encode()));
    }

    /// Decoding arbitrary bytes returns Ok or Err, never panics.
    #[test]
    fn decode_total(bytes in prop::collection::vec(any::<u8>(), 0..512)) {
        let _ = Key::decode(&bytes);
    }
}
