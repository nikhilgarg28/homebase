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
fn limited_writes_fixture_matches_sqlite() {
    let report = run_file(
        "tests/slt/limited-writes.slt",
        &RunOptions {
            engine: multilite_conformance::Engine::Both,
            max_records: None,
        },
    );

    assert_eq!(report.record_count(), 14);
    assert_eq!(report.failed_count(), 0);
}

#[test]
fn dml_aliases_fixture_matches_sqlite() {
    let report = run_file(
        "tests/slt/dml-aliases.slt",
        &RunOptions {
            engine: multilite_conformance::Engine::Both,
            max_records: None,
        },
    );

    assert_eq!(report.record_count(), 8);
    assert_eq!(report.failed_count(), 0);
}

#[test]
fn schema_conflict_policies_fixture_matches_sqlite() {
    let report = run_file(
        "tests/slt/schema-conflict-policies.slt",
        &RunOptions {
            engine: multilite_conformance::Engine::Both,
            max_records: None,
        },
    );

    assert_eq!(report.record_count(), 10);
    assert_eq!(report.failed_count(), 0);
}

#[test]
fn both_mode_classifies_unsupported_grammar_as_coverage_not_divergence() {
    let report = run_file(
        "tests/slt/unsupported.slt",
        &RunOptions {
            engine: multilite_conformance::Engine::Both,
            max_records: None,
        },
    );

    assert_eq!(report.record_count(), 2);
    assert_eq!(report.unsupported_count(), 1);
    assert_eq!(report.failed_count(), 0);
    let shapes = multilite_conformance::ConformanceReport {
        files: vec![report],
    }
    .statement_shapes();
    assert_eq!(shapes["create_table"].passed, 1);
    assert_eq!(shapes["create_view"].unsupported, 1);
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
    assert!(
        files
            .iter()
            .any(|file| file.ends_with("limited-writes.slt"))
    );
    assert!(files.iter().any(|file| file.ends_with("dml-aliases.slt")));
    assert!(
        files
            .iter()
            .any(|file| file.ends_with("schema-conflict-policies.slt"))
    );
    assert!(files.iter().any(|file| file.ends_with("unsupported.slt")));

    let report = run_paths(&paths, &RunOptions::sqlite());
    assert!(report.record_count() >= 10);
    assert_eq!(report.failed_count(), 0);
}
