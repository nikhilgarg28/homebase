use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::path::{Path, PathBuf};

/// Summary for one conformance run.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ConformanceReport {
    pub files: Vec<FileReport>,
}

impl ConformanceReport {
    pub fn record_count(&self) -> usize {
        self.files.iter().map(|file| file.records.len()).sum()
    }

    pub fn passed_count(&self) -> usize {
        self.files
            .iter()
            .flat_map(|file| &file.records)
            .filter(|record| record.status == RecordStatus::Passed)
            .count()
    }

    pub fn parse_error_count(&self) -> usize {
        self.files
            .iter()
            .flat_map(|file| &file.records)
            .filter(|record| record.status == RecordStatus::ParseError)
            .count()
    }

    pub fn unsupported_count(&self) -> usize {
        self.files
            .iter()
            .flat_map(|file| &file.records)
            .filter(|record| record.status == RecordStatus::Unsupported)
            .count()
    }

    pub fn failed_count(&self) -> usize {
        self.files
            .iter()
            .flat_map(|file| &file.records)
            .filter(|record| record.status.is_failure())
            .count()
    }

    pub fn statement_shapes(&self) -> BTreeMap<String, ShapeStats> {
        let mut shapes = BTreeMap::new();
        for record in self.files.iter().flat_map(|file| &file.records) {
            if let Some(shape) = &record.shape {
                shapes
                    .entry(shape.clone())
                    .or_insert_with(ShapeStats::default)
                    .observe(record.status);
            }
        }
        shapes
    }

    pub fn to_json(&self) -> String {
        let mut json = String::new();
        json.push_str("{\n");
        write!(
            json,
            "  \"files\": {},\n  \"records\": {},\n  \"passed\": {},\n  \"unsupported\": {},\n  \"failed\": {},\n",
            self.files.len(),
            self.record_count(),
            self.passed_count(),
            self.unsupported_count(),
            self.failed_count()
        )
        .expect("writing JSON to a string cannot fail");
        json.push_str("  \"statement_shapes\": {\n");
        for (index, (shape, stats)) in self.statement_shapes().iter().enumerate() {
            if index > 0 {
                json.push_str(",\n");
            }
            write!(
                json,
                "    \"{}\": {{ \"records\": {}, \"passed\": {}, \"unsupported\": {}, \"failed\": {} }}",
                escape_json(shape),
                stats.records,
                stats.passed,
                stats.unsupported,
                stats.failed
            )
            .expect("writing JSON to a string cannot fail");
        }
        json.push_str("\n  },\n");
        json.push_str("  \"file_reports\": [\n");
        for (index, file) in self.files.iter().enumerate() {
            if index > 0 {
                json.push_str(",\n");
            }
            file.write_json(&mut json, 4);
        }
        json.push_str("\n  ]\n}\n");
        json
    }
}

/// Summary for one SQL Logic Test file.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FileReport {
    pub path: PathBuf,
    pub records: Vec<RecordReport>,
}

impl FileReport {
    pub fn new(path: impl AsRef<Path>) -> Self {
        Self {
            path: path.as_ref().to_owned(),
            records: Vec::new(),
        }
    }

    pub fn record_count(&self) -> usize {
        self.records.len()
    }

    pub fn passed_count(&self) -> usize {
        self.records
            .iter()
            .filter(|record| record.status == RecordStatus::Passed)
            .count()
    }

    pub fn failed_count(&self) -> usize {
        self.records
            .iter()
            .filter(|record| record.status.is_failure())
            .count()
    }

    pub fn unsupported_count(&self) -> usize {
        self.records
            .iter()
            .filter(|record| record.status == RecordStatus::Unsupported)
            .count()
    }

    fn write_json(&self, json: &mut String, indent: usize) {
        let pad = " ".repeat(indent);
        writeln!(json, "{pad}{{").expect("writing JSON to a string cannot fail");
        writeln!(
            json,
            "{pad}  \"path\": \"{}\",",
            escape_json(&self.path.display().to_string())
        )
        .expect("writing JSON to a string cannot fail");
        writeln!(json, "{pad}  \"records\": [").expect("writing JSON to a string cannot fail");
        for (index, record) in self.records.iter().enumerate() {
            if index > 0 {
                json.push_str(",\n");
            }
            record.write_json(json, indent + 4);
        }
        writeln!(json, "\n{pad}  ]").expect("writing JSON to a string cannot fail");
        write!(json, "{pad}}}").expect("writing JSON to a string cannot fail");
    }
}

/// Summary for one record in an SLT file.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecordReport {
    pub index: usize,
    pub status: RecordStatus,
    pub shape: Option<String>,
    pub detail: String,
}

impl RecordReport {
    pub fn passed(index: usize) -> Self {
        Self {
            index,
            status: RecordStatus::Passed,
            shape: None,
            detail: String::new(),
        }
    }

    pub fn failed(index: usize, detail: impl Into<String>) -> Self {
        Self {
            index,
            status: RecordStatus::Failed,
            shape: None,
            detail: detail.into(),
        }
    }

    pub fn parse_error(detail: impl Into<String>) -> Self {
        Self {
            index: 0,
            status: RecordStatus::ParseError,
            shape: None,
            detail: detail.into(),
        }
    }

    pub fn reference_failed(index: usize, detail: impl Into<String>) -> Self {
        Self {
            index,
            status: RecordStatus::ReferenceFailed,
            shape: None,
            detail: detail.into(),
        }
    }

    pub fn candidate_failed(index: usize, detail: impl Into<String>) -> Self {
        Self {
            index,
            status: RecordStatus::CandidateFailed,
            shape: None,
            detail: detail.into(),
        }
    }

    pub fn diverged(index: usize, detail: impl Into<String>) -> Self {
        Self {
            index,
            status: RecordStatus::Diverged,
            shape: None,
            detail: detail.into(),
        }
    }

    pub fn unsupported(index: usize, detail: impl Into<String>) -> Self {
        Self {
            index,
            status: RecordStatus::Unsupported,
            shape: None,
            detail: detail.into(),
        }
    }

    pub fn with_shape(mut self, shape: Option<String>) -> Self {
        self.shape = shape;
        self
    }

    fn write_json(&self, json: &mut String, indent: usize) {
        let pad = " ".repeat(indent);
        write!(
            json,
            "{pad}{{ \"index\": {}, \"status\": \"{}\", \"shape\": ",
            self.index, self.status
        )
        .expect("writing JSON to a string cannot fail");
        match &self.shape {
            Some(shape) => write!(json, "\"{}\"", escape_json(shape)),
            None => json.write_str("null"),
        }
        .expect("writing JSON to a string cannot fail");
        write!(json, ", \"detail\": \"{}\" }}", escape_json(&self.detail))
            .expect("writing JSON to a string cannot fail");
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ShapeStats {
    pub records: usize,
    pub passed: usize,
    pub unsupported: usize,
    pub failed: usize,
}

impl ShapeStats {
    fn observe(&mut self, status: RecordStatus) {
        self.records += 1;
        match status {
            RecordStatus::Passed => self.passed += 1,
            RecordStatus::Unsupported => self.unsupported += 1,
            _ => self.failed += 1,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RecordStatus {
    Passed,
    Unsupported,
    Failed,
    ParseError,
    ReferenceFailed,
    CandidateFailed,
    Diverged,
}

impl RecordStatus {
    fn is_failure(self) -> bool {
        !matches!(self, Self::Passed | Self::Unsupported)
    }
}

impl std::fmt::Display for RecordStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Passed => f.write_str("passed"),
            Self::Unsupported => f.write_str("unsupported"),
            Self::Failed => f.write_str("failed"),
            Self::ParseError => f.write_str("parse_error"),
            Self::ReferenceFailed => f.write_str("reference_failed"),
            Self::CandidateFailed => f.write_str("candidate_failed"),
            Self::Diverged => f.write_str("diverged"),
        }
    }
}

fn escape_json(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for character in value.chars() {
        match character {
            '"' => escaped.push_str("\\\""),
            '\\' => escaped.push_str("\\\\"),
            '\n' => escaped.push_str("\\n"),
            '\r' => escaped.push_str("\\r"),
            '\t' => escaped.push_str("\\t"),
            character if character.is_control() => {
                write!(escaped, "\\u{:04x}", character as u32)
                    .expect("writing JSON escape to a string cannot fail");
            }
            character => escaped.push(character),
        }
    }
    escaped
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn report_renders_basic_json_without_extra_dependencies() {
        let mut file = FileReport::new("smoke.slt");
        file.records
            .push(RecordReport::passed(0).with_shape(Some("select".into())));
        file.records.push(
            RecordReport::failed(1, "expected \"x\"\nactual y").with_shape(Some("select".into())),
        );
        file.records.push(
            RecordReport::unsupported(2, "unsupported SQL").with_shape(Some("drop_table".into())),
        );
        let report = ConformanceReport { files: vec![file] };

        let json = report.to_json();
        assert!(json.contains("\"files\": 1"));
        assert!(json.contains("\"passed\": 1"));
        assert!(json.contains("\"unsupported\": 1"));
        assert!(json.contains("\"select\": { \"records\": 2"));
        assert!(json.contains("\"drop_table\": { \"records\": 1"));
        assert!(json.contains("expected \\\"x\\\"\\nactual y"));
    }
}
