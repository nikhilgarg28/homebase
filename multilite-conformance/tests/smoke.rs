use std::path::PathBuf;

use multilite_conformance::{RunOptions, collect_test_files, run_file, run_paths};

#[test]
fn sqlite_driver_runs_a_basic_sqllogictest_file() {
    let report = run_file("tests/slt/basic.slt", &RunOptions::sqlite());

    assert_eq!(report.record_count(), 3);
    assert_eq!(report.failed_count(), 0);
}

#[test]
fn multilite_driver_runs_a_basic_sqllogictest_file() {
    let report = run_file("tests/slt/basic.slt", &RunOptions::multilite());

    assert_eq!(report.record_count(), 3);
    assert_eq!(report.failed_count(), 0);
}

#[test]
fn both_mode_runs_reference_and_candidate() {
    let report = run_file(
        "tests/slt/basic.slt",
        &RunOptions {
            engine: multilite_conformance::Engine::Both,
            max_records: None,
        },
    );

    assert_eq!(report.record_count(), 3);
    assert_eq!(report.failed_count(), 0);
}

#[test]
fn record_limit_stops_after_complete_records() {
    let report = run_file(
        "tests/slt/basic.slt",
        &RunOptions {
            engine: multilite_conformance::Engine::Both,
            max_records: Some(2),
        },
    );

    assert_eq!(report.record_count(), 2);
    assert_eq!(report.failed_count(), 0);
}

#[test]
fn affinity_fixture_matches_sqlite() {
    let report = run_file(
        "tests/slt/affinity.slt",
        &RunOptions {
            engine: multilite_conformance::Engine::Both,
            max_records: None,
        },
    );

    assert_eq!(report.record_count(), 11);
    assert_eq!(report.failed_count(), 0);
}

#[test]
fn defaults_and_checks_fixture_matches_sqlite() {
    let report = run_file(
        "tests/slt/defaults-and-checks.slt",
        &RunOptions {
            engine: multilite_conformance::Engine::Both,
            max_records: None,
        },
    );

    assert_eq!(report.record_count(), 8);
    assert_eq!(report.failed_count(), 0);
}

#[test]
fn corpus_walker_discovers_sqllogictest_files() {
    let paths = vec![PathBuf::from("tests/slt")];
    let files = collect_test_files(&paths);

    assert!(files.iter().any(|file| file.ends_with("basic.slt")));
    assert!(files.iter().any(|file| file.ends_with("affinity.slt")));
    assert!(
        files
            .iter()
            .any(|file| file.ends_with("defaults-and-checks.slt"))
    );

    let report = run_paths(&paths, &RunOptions::sqlite());
    assert!(report.record_count() >= 10);
    assert_eq!(report.failed_count(), 0);
}
