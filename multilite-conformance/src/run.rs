use std::path::{Path, PathBuf};

use sqllogictest::{Control, DefaultColumnType, Record, ResultMode, Runner};

use crate::drivers::{MultiliteDriver, SqliteDriver};
use crate::report::{ConformanceReport, FileReport, RecordReport, RecordStatus};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Engine {
    Sqlite,
    Multilite,
    Both,
}

#[derive(Clone, Debug)]
pub struct RunOptions {
    pub engine: Engine,
}

impl RunOptions {
    pub fn sqlite() -> Self {
        Self {
            engine: Engine::Sqlite,
        }
    }

    pub fn multilite() -> Self {
        Self {
            engine: Engine::Multilite,
        }
    }
}

pub fn run_file(path: impl AsRef<Path>, options: &RunOptions) -> FileReport {
    match options.engine {
        Engine::Sqlite => {
            let directory = tempfile::tempdir().expect("temporary sqlite db directory");
            let database_path = directory.path().join("reference.sqlite");
            run_with(path.as_ref(), "sqlite", move || {
                let database_path = database_path.clone();
                async move { SqliteDriver::open(database_path) }
            })
        }
        Engine::Multilite => {
            let directory = tempfile::tempdir().expect("temporary multilite db directory");
            let database_path = directory.path().join("candidate.sqlite");
            run_with(path.as_ref(), "multilite", move || {
                let database_path = database_path.clone();
                async move { MultiliteDriver::open(database_path) }
            })
        }
        Engine::Both => run_both(path.as_ref()),
    }
}

pub fn run_paths(paths: &[PathBuf], options: &RunOptions) -> ConformanceReport {
    let mut report = ConformanceReport::default();
    for file in collect_test_files(paths) {
        report.files.push(run_file(file, options));
    }
    report
}

pub fn collect_test_files(paths: &[PathBuf]) -> Vec<PathBuf> {
    let mut files = Vec::new();
    for path in paths {
        collect_test_files_inner(path, &mut files);
    }
    files.sort();
    files
}

fn collect_test_files_inner(path: &Path, files: &mut Vec<PathBuf>) {
    if path.is_file() {
        if is_test_file(path) {
            files.push(path.to_owned());
        }
        return;
    }
    if !path.is_dir() {
        return;
    }
    let Ok(entries) = std::fs::read_dir(path) else {
        return;
    };
    for entry in entries.flatten() {
        let child = entry.path();
        if child
            .file_name()
            .is_some_and(|name| name == ".git" || name == ".fslckout")
        {
            continue;
        }
        collect_test_files_inner(&child, files);
    }
}

fn is_test_file(path: &Path) -> bool {
    path.extension()
        .and_then(|extension| extension.to_str())
        .is_some_and(|extension| matches!(extension, "slt" | "test"))
}

fn run_both(path: &Path) -> FileReport {
    let sqlite = run_file(path, &RunOptions::sqlite());
    let multilite = run_file(path, &RunOptions::multilite());
    let mut report = FileReport::new(path);
    let max_records = sqlite.records.len().max(multilite.records.len());
    for index in 0..max_records {
        let reference = sqlite.records.get(index);
        let candidate = multilite.records.get(index);
        match (reference, candidate) {
            (Some(reference), Some(candidate))
                if reference.status == RecordStatus::Passed
                    && candidate.status == RecordStatus::Passed =>
            {
                report.records.push(RecordReport::passed(index));
            }
            (Some(reference), _) if reference.status != RecordStatus::Passed => {
                report.records.push(RecordReport::reference_failed(
                    index,
                    reference.detail.clone(),
                ));
            }
            (Some(_), Some(candidate)) if candidate.status != RecordStatus::Passed => {
                report.records.push(RecordReport::candidate_failed(
                    index,
                    candidate.detail.clone(),
                ));
            }
            (Some(reference), Some(candidate)) => report.records.push(RecordReport::diverged(
                index,
                format!(
                    "reference status {} ({}) differed from candidate status {} ({})",
                    reference.status, reference.detail, candidate.status, candidate.detail
                ),
            )),
            (Some(_), None) => report.records.push(RecordReport::diverged(
                index,
                "candidate did not produce a record report",
            )),
            (None, Some(_)) => report.records.push(RecordReport::diverged(
                index,
                "reference did not produce a record report",
            )),
            (None, None) => {}
        }
    }
    report
}

fn run_with<D, F, Fut>(path: &Path, engine_name: &'static str, connect: F) -> FileReport
where
    D: sqllogictest::DB<ColumnType = DefaultColumnType> + Send + 'static,
    F: Fn() -> Fut + Clone,
    Fut: std::future::Future<Output = Result<D, <D as sqllogictest::DB>::Error>>,
{
    let mut report = FileReport::new(path);
    let mut runner = Runner::new(connect);
    runner.with_hash_threshold(8);
    let _ = runner.run(Record::Control(Control::ResultMode(ResultMode::ValueWise)));
    match parse_compat_file(path, engine_name) {
        Ok(records) => {
            for (index, record) in records.into_iter().enumerate() {
                if matches!(record, Record::Halt { .. }) {
                    break;
                }
                match runner.run(record) {
                    Ok(_) => report.records.push(RecordReport::passed(index)),
                    Err(error) => report
                        .records
                        .push(RecordReport::failed(index, error.to_string())),
                }
            }
        }
        Err(error) => report
            .records
            .push(RecordReport::parse_error(format!("parse error: {error}"))),
    }
    report
}

fn parse_compat_file(
    path: &Path,
    engine_name: &str,
) -> Result<Vec<Record<DefaultColumnType>>, String> {
    let script =
        std::fs::read_to_string(path).map_err(|error| format!("failed to read file: {error}"))?;
    let script = strip_legacy_directive_comments(&script);
    let script = rewrite_conditional_halts(&script, engine_name);
    sqllogictest::parse(&script).map_err(|error| error.to_string())
}

fn strip_legacy_directive_comments(script: &str) -> String {
    let mut rewritten = String::with_capacity(script.len());
    for line in script.lines() {
        let trimmed = line.trim_start();
        if is_directive_line(trimmed) {
            if let Some((prefix, _comment)) = line.split_once(" #") {
                rewritten.push_str(prefix.trim_end());
                rewritten.push('\n');
                continue;
            }
        }
        rewritten.push_str(line);
        rewritten.push('\n');
    }
    rewritten
}

fn is_directive_line(trimmed: &str) -> bool {
    matches!(
        trimmed.split_whitespace().next(),
        Some("skipif" | "onlyif" | "statement" | "query" | "hash-threshold")
    )
}

fn rewrite_conditional_halts(script: &str, engine_name: &str) -> String {
    let lines = script.lines().collect::<Vec<_>>();
    let mut rewritten = String::with_capacity(script.len());
    let mut index = 0;
    while index < lines.len() {
        let trimmed = lines[index].trim();
        let words = trimmed.split_whitespace().collect::<Vec<_>>();
        if matches!(words.as_slice(), ["skipif", _] | ["onlyif", _])
            && lines
                .get(index + 1)
                .is_some_and(|next| next.trim() == "halt")
        {
            let condition_matches = words[1] == engine_name;
            let should_skip_halt = match words[0] {
                "skipif" => condition_matches,
                "onlyif" => !condition_matches,
                _ => false,
            };
            if !should_skip_halt {
                rewritten.push_str("halt\n");
            }
            index += 2;
            continue;
        }
        rewritten.push_str(lines[index]);
        rewritten.push('\n');
        index += 1;
    }
    rewritten
}
