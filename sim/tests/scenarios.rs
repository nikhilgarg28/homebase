//! Advanced torture scenarios (SimStore + deterministic executor).

use homebase_sim::seeds;
use homebase_sim::torture;

#[test]
fn contention_races_between_devices() {
    for seed in seeds::scenario_seeds() {
        torture::run_contention_race(seed);
    }
}

#[test]
fn contended_handoff() {
    for seed in seeds::scenario_seeds() {
        torture::run_contended_handoff(seed);
    }
}

#[test]
fn expired_evidence_write_after_expiry() {
    for seed in seeds::scenario_seeds() {
        torture::run_expired_evidence_write(seed);
    }
}

#[test]
fn replica_tracks_read_at() {
    for seed in seeds::scenario_seeds() {
        torture::run_replica_sync(seed);
    }
}
