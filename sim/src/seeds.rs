//! Seed counts for torture suites. Override via environment:
//!
//! - `HOMEBASE_TORTURE_SEEDS` — crash / lease contention / replica (default 1000)
//! - `HOMEBASE_SCENARIO_SEEDS` — scenario harness (default 100)

const DEFAULT_TORTURE: u64 = 1000;
const DEFAULT_SCENARIO: u64 = 100;
const DEFAULT_PERSIST: u64 = 100;

fn parse_env(name: &str) -> Option<u64> {
    std::env::var(name).ok()?.parse().ok()
}

/// Number of seeds for the main torture suites (Layer 1).
pub fn torture_seed_count() -> u64 {
    parse_env("HOMEBASE_TORTURE_SEEDS").unwrap_or(DEFAULT_TORTURE)
}

/// Layer 3 (slatedb persist) seeds — separate knob; slatedb is much slower.
pub fn persist_torture_seed_count() -> u64 {
    parse_env("HOMEBASE_PERSIST_TORTURE_SEEDS").unwrap_or(DEFAULT_PERSIST)
}

/// Number of seeds per scenario in `tests/scenarios.rs`.
pub fn scenario_seed_count() -> u64 {
    parse_env("HOMEBASE_SCENARIO_SEEDS").unwrap_or(DEFAULT_SCENARIO)
}

/// Inclusive-exclusive range `0..torture_seed_count()`.
pub fn torture_seeds() -> impl Iterator<Item = u64> {
    0..torture_seed_count()
}

/// Inclusive-exclusive range `0..scenario_seed_count()`.
pub fn scenario_seeds() -> impl Iterator<Item = u64> {
    0..scenario_seed_count()
}

/// Inclusive-exclusive range `0..persist_torture_seed_count()`.
pub fn persist_torture_seeds() -> impl Iterator<Item = u64> {
    0..persist_torture_seed_count()
}

/// Recovery-path coverage assertions need enough seeds to be meaningful.
pub fn torture_coverage_enforced() -> bool {
    torture_seed_count() >= 50
}

pub fn persist_coverage_enforced() -> bool {
    persist_torture_seed_count() >= 50
}
