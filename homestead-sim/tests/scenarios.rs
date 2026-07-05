//! Advanced torture scenarios (SimStore + deterministic executor).

use homestead_sim::seeds;
use homestead_sim::torture;

#[test]
fn steal_races_between_devices() {
    for seed in seeds::scenario_seeds() {
        torture::run_steal_race(seed);
    }
}

#[test]
fn contended_handoff() {
    for seed in seeds::scenario_seeds() {
        torture::run_contended_handoff(seed);
    }
}

#[test]
fn zombie_writer_after_expiry() {
    for seed in seeds::scenario_seeds() {
        torture::run_zombie_writer(seed);
    }
}

#[test]
fn replica_tracks_read_at() {
    for seed in seeds::scenario_seeds() {
        torture::run_replica_sync(seed);
    }
}
