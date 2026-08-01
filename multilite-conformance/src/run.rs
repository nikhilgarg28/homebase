use std::path::{Path, PathBuf};

use fallible_iterator::FallibleIterator as _;
use sqlite3_parser::ast::{Cmd, Stmt};
use sqlite3_parser::lexer::sql::Parser;
use sqllogictest::{Control, DefaultColumnType, Record, ResultMode, Runner, TestErrorKind};

use crate::drivers::{DriverError, MultiliteDriver, SqliteDriver};
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
    pub max_records: Option<usize>,
}

impl RunOptions {
    pub fn sqlite() -> Self {
        Self {
            engine: Engine::Sqlite,
            max_records: None,
        }
    }

    pub fn multilite() -> Self {
        Self {
            engine: Engine::Multilite,
            max_records: None,
        }
    }
}

pub fn run_file(path: impl AsRef<Path>, options: &RunOptions) -> FileReport {
    match options.engine {
        Engine::Sqlite => {
            let directory = tempfile::tempdir().expect("temporary sqlite db directory");
            let database_path = directory.path().join("reference.sqlite");
            run_with(path.as_ref(), "sqlite", options.max_records, move || {
                let database_path = database_path.clone();
                async move { SqliteDriver::open(database_path) }
            })
        }
        Engine::Multilite => {
            let directory = tempfile::tempdir().expect("temporary multilite db directory");
            let database_path = directory.path().join("candidate.sqlite");
            run_with(path.as_ref(), "multilite", options.max_records, move || {
                let database_path = database_path.clone();
                async move { MultiliteDriver::open(database_path) }
            })
        }
        Engine::Both => run_both(path.as_ref(), options.max_records),
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

fn run_both(path: &Path, max_records: Option<usize>) -> FileReport {
    let sqlite = run_file(
        path,
        &RunOptions {
            engine: Engine::Sqlite,
            max_records,
        },
    );
    let multilite = run_file(
        path,
        &RunOptions {
            engine: Engine::Multilite,
            max_records,
        },
    );
    let mut report = FileReport::new(path);
    let max_records = sqlite.records.len().max(multilite.records.len());
    for index in 0..max_records {
        let reference = sqlite.records.get(index);
        let candidate = multilite.records.get(index);
        let shape = candidate
            .and_then(|record| record.shape.clone())
            .or_else(|| reference.and_then(|record| record.shape.clone()));
        let combined = match (reference, candidate) {
            (Some(reference), Some(candidate))
                if reference.status == RecordStatus::Passed
                    && candidate.status == RecordStatus::Passed =>
            {
                RecordReport::passed(index)
            }
            (Some(reference), _) if reference.status != RecordStatus::Passed => {
                RecordReport::reference_failed(index, reference.detail.clone())
            }
            (Some(_), Some(candidate)) if candidate.status == RecordStatus::Unsupported => {
                RecordReport::unsupported(index, candidate.detail.clone())
            }
            (Some(_), Some(candidate)) if candidate.status != RecordStatus::Passed => {
                RecordReport::candidate_failed(index, candidate.detail.clone())
            }
            (Some(reference), Some(candidate)) => RecordReport::diverged(
                index,
                format!(
                    "reference status {} ({}) differed from candidate status {} ({})",
                    reference.status, reference.detail, candidate.status, candidate.detail
                ),
            ),
            (Some(_), None) => {
                RecordReport::diverged(index, "candidate did not produce a record report")
            }
            (None, Some(_)) => {
                RecordReport::diverged(index, "reference did not produce a record report")
            }
            (None, None) => continue,
        };
        report.records.push(combined.with_shape(shape));
    }
    report
}

fn run_with<D, F, Fut>(
    path: &Path,
    engine_name: &'static str,
    max_records: Option<usize>,
    connect: F,
) -> FileReport
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
            for (index, record) in records
                .into_iter()
                .take(max_records.unwrap_or(usize::MAX))
                .enumerate()
            {
                if matches!(record, Record::Halt { .. }) {
                    break;
                }
                let shape = record_shape(&record);
                match runner.run(record) {
                    Ok(_) => report
                        .records
                        .push(RecordReport::passed(index).with_shape(shape)),
                    Err(error) if is_unsupported(&error.kind()) => report.records.push(
                        RecordReport::unsupported(index, error.to_string()).with_shape(shape),
                    ),
                    Err(error) => report
                        .records
                        .push(RecordReport::failed(index, error.to_string()).with_shape(shape)),
                }
            }
        }
        Err(error) => report
            .records
            .push(RecordReport::parse_error(format!("parse error: {error}"))),
    }
    report
}

fn is_unsupported(error: &TestErrorKind) -> bool {
    let driver_error = match error {
        TestErrorKind::Fail { err, .. } | TestErrorKind::ErrorMismatch { err, .. } => {
            err.downcast_ref::<DriverError>()
        }
        _ => None,
    };
    driver_error.is_some_and(DriverError::is_unsupported)
}

fn record_shape(record: &Record<DefaultColumnType>) -> Option<String> {
    let sql = match record {
        Record::Statement { sql, .. } | Record::Query { sql, .. } | Record::Let { sql, .. } => sql,
        _ => return None,
    };
    let mut parser = Parser::new(sql.as_bytes());
    let command = parser.next().ok().flatten()?;
    let statement = match command {
        Cmd::Stmt(statement) | Cmd::Explain(statement) | Cmd::ExplainQueryPlan(statement) => {
            statement
        }
    };
    Some(
        match statement {
            Stmt::AlterTable(..) => "alter_table",
            Stmt::Analyze(..) => "analyze",
            Stmt::Attach { .. } => "attach",
            Stmt::Begin(..) => "begin",
            Stmt::Commit(..) => "commit",
            Stmt::CreateIndex { unique: true, .. } => "create_unique_index",
            Stmt::CreateIndex { .. } => "create_index",
            Stmt::CreateTable { .. } => "create_table",
            Stmt::CreateTrigger { .. } => "create_trigger",
            Stmt::CreateView { .. } => "create_view",
            Stmt::CreateVirtualTable { .. } => "create_virtual_table",
            Stmt::Delete { .. } => "delete",
            Stmt::Detach(..) => "detach",
            Stmt::DropIndex { .. } => "drop_index",
            Stmt::DropTable { .. } => "drop_table",
            Stmt::DropTrigger { .. } => "drop_trigger",
            Stmt::DropView { .. } => "drop_view",
            Stmt::Insert { .. } => "insert",
            Stmt::Pragma(..) => "pragma",
            Stmt::Reindex { .. } => "reindex",
            Stmt::Release(..) => "release",
            Stmt::Rollback { .. } => "rollback",
            Stmt::Savepoint(..) => "savepoint",
            Stmt::Select(..) => "select",
            Stmt::Update { .. } => "update",
            Stmt::Vacuum(..) => "vacuum",
        }
        .to_owned(),
    )
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
        if is_directive_line(trimmed)
            && let Some((prefix, _comment)) = line.split_once(" #")
        {
            rewritten.push_str(prefix.trim_end());
            rewritten.push('\n');
            continue;
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
