//! Testable core for the Multilite LocalOnly shell.

use std::fmt;
use std::io::{self, BufRead, Write};

use multilite::{Error, MultiliteConnection, QueryTable, Value};

/// Errors from running shell SQL or writing shell output.
#[derive(Debug)]
pub enum ShellError {
    /// Multilite rejected or failed the statement.
    Sql(Error),
    /// The output sink failed.
    Io(io::Error),
}

impl fmt::Display for ShellError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Sql(error) => write!(formatter, "{error}"),
            Self::Io(error) => write!(formatter, "{error}"),
        }
    }
}

impl std::error::Error for ShellError {}

impl From<Error> for ShellError {
    fn from(error: Error) -> Self {
        Self::Sql(error)
    }
}

impl From<io::Error> for ShellError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

/// Result of handling a leading dot command.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DotCommand {
    /// Keep reading input.
    Continue,
    /// Exit the shell successfully.
    Quit,
}

/// Owned result of one successful statement.
#[derive(Clone, Debug, PartialEq)]
pub enum StatementResult {
    /// Row-producing statement.
    Table(QueryTable),
    /// Rowless mutating statement.
    Changed(usize),
}

/// Whether `buffer` contains a complete SQL statement terminated by `;`
/// outside of quotes.
pub fn statement_complete(buffer: &str) -> bool {
    let mut in_single = false;
    let mut in_double = false;
    let mut chars = buffer.chars().peekable();
    while let Some(ch) = chars.next() {
        match ch {
            '\'' if !in_double => {
                if in_single && chars.peek() == Some(&'\'') {
                    chars.next();
                } else {
                    in_single = !in_single;
                }
            }
            '"' if !in_single => {
                if in_double && chars.peek() == Some(&'"') {
                    chars.next();
                } else {
                    in_double = !in_double;
                }
            }
            ';' if !in_single && !in_double => return true,
            _ => {}
        }
    }
    false
}

/// Strip a completed statement buffer down to Multilite SQL (no trailing `;`).
pub fn take_statement(buffer: &str) -> String {
    buffer.trim().trim_end_matches(';').trim().to_owned()
}

/// Handle a leading `.` command. Writes help/unknown messages to `out`.
pub fn handle_dot(command: &str, out: &mut impl Write) -> io::Result<DotCommand> {
    match command {
        ".quit" | ".exit" | ".q" => Ok(DotCommand::Quit),
        ".help" => {
            writeln!(
                out,
                "\
Dot commands:
  .help                Show this message
  .quit / .exit / .q   Leave the shell

SQL:
  One Multilite statement at a time, terminated by ';'.
  Opens with SyncPolicy::LocalOnly (no push/pull).
  Raw BEGIN/COMMIT and multi-statement batches are rejected by Multilite."
            )?;
            Ok(DotCommand::Continue)
        }
        _ => {
            writeln!(out, "unknown dot command: {command} (try .help)")?;
            Ok(DotCommand::Continue)
        }
    }
}

/// Evaluate one Multilite statement without printing.
pub fn run_statement(
    connection: &MultiliteConnection,
    sql: &str,
) -> Result<StatementResult, Error> {
    match connection.query_table(sql, ()) {
        Ok(table) => Ok(StatementResult::Table(table)),
        Err(Error::StatementModeMismatch) => {
            Ok(StatementResult::Changed(connection.execute(sql, ())?))
        }
        Err(error) => Err(error),
    }
}

/// Write a [`StatementResult`] in shell format.
pub fn write_statement_result(
    out: &mut impl Write,
    result: &StatementResult,
) -> io::Result<()> {
    match result {
        StatementResult::Table(table) => write!(out, "{}", format_table(table)),
        StatementResult::Changed(changed) => writeln!(out, "rows changed: {changed}"),
    }
}

/// Run one Multilite statement, writing a table or change count to `out`.
pub fn execute_sql(
    connection: &MultiliteConnection,
    sql: &str,
    out: &mut impl Write,
) -> Result<(), ShellError> {
    let result = run_statement(connection, sql)?;
    write_statement_result(out, &result)?;
    Ok(())
}

/// Drive a non-interactive shell from line-oriented input.
///
/// Dot commands and `;`-terminated SQL are accepted. SQL errors are written to
/// `err` and do not abort the script.
pub fn run_script(
    connection: &MultiliteConnection,
    input: impl BufRead,
    out: &mut impl Write,
    err: &mut impl Write,
) -> io::Result<()> {
    let mut buffer = String::new();
    for line in input.lines() {
        let line = line?;
        let trimmed = line.trim();
        if buffer.is_empty() && trimmed.starts_with('.') {
            match handle_dot(trimmed, out)? {
                DotCommand::Continue => continue,
                DotCommand::Quit => return Ok(()),
            }
        }

        if !buffer.is_empty() {
            buffer.push('\n');
        }
        buffer.push_str(&line);
        if !statement_complete(&buffer) {
            continue;
        }

        let sql = take_statement(&buffer);
        buffer.clear();
        if sql.is_empty() {
            continue;
        }
        if let Err(error) = execute_sql(connection, &sql, out) {
            writeln!(err, "Error: {error}")?;
        }
    }
    Ok(())
}

/// Format a query result as an aligned ASCII table.
pub fn format_table(table: &QueryTable) -> String {
    let mut out = String::new();
    if table.columns.is_empty() {
        out.push_str("(no columns)\n");
        return out;
    }

    let widths = column_widths(table);
    push_row(&mut out, &table.columns, &widths);
    push_separator(&mut out, &widths);
    for row in &table.rows {
        let cells = row.iter().map(format_value).collect::<Vec<_>>();
        push_row(&mut out, &cells, &widths);
    }
    out.push_str(&format!(
        "{} row{}\n",
        table.rows.len(),
        if table.rows.len() == 1 { "" } else { "s" }
    ));
    out
}

/// Render one SQLite value for shell display.
pub fn format_value(value: &Value) -> String {
    match value {
        Value::Null => "NULL".to_owned(),
        Value::Integer(value) => value.to_string(),
        Value::Real(value) => value.to_string(),
        Value::Text(value) => value.clone(),
        Value::Blob(value) => format!("X'{}'", encode_hex(value)),
    }
}

fn column_widths(table: &QueryTable) -> Vec<usize> {
    let mut widths = table
        .columns
        .iter()
        .map(|column| column.chars().count())
        .collect::<Vec<_>>();
    for row in &table.rows {
        for (index, value) in row.iter().enumerate() {
            widths[index] = widths[index].max(format_value(value).chars().count());
        }
    }
    widths
}

fn push_row(out: &mut String, cells: &[String], widths: &[usize]) {
    for (index, (cell, width)) in cells.iter().zip(widths).enumerate() {
        if index > 0 {
            out.push_str(" | ");
        }
        out.push_str(cell);
        for _ in cell.chars().count()..*width {
            out.push(' ');
        }
    }
    out.push('\n');
}

fn push_separator(out: &mut String, widths: &[usize]) {
    for (index, width) in widths.iter().enumerate() {
        if index > 0 {
            out.push_str("-+-");
        }
        for _ in 0..*width {
            out.push('-');
        }
    }
    out.push('\n');
}

fn encode_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0xf) as usize] as char);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use multilite::Value;

    #[test]
    fn statement_complete_respects_quotes_and_escapes() {
        assert!(!statement_complete("SELECT 1"));
        assert!(statement_complete("SELECT 1;"));
        assert!(!statement_complete(r"SELECT ';'"));
        assert!(statement_complete(r"SELECT ';';"));
        assert!(!statement_complete(r#"SELECT "a;b""#));
        assert!(statement_complete(r#"SELECT "a;b";"#));
        assert!(statement_complete(r"SELECT '''';"));
        assert!(statement_complete(
            "CREATE TABLE notes (\n  id INTEGER PRIMARY KEY\n);"
        ));
    }

    #[test]
    fn take_statement_strips_terminator_and_whitespace() {
        assert_eq!(take_statement("  SELECT 1;  "), "SELECT 1");
        assert_eq!(take_statement("SELECT 1;;"), "SELECT 1");
    }

    #[test]
    fn format_value_covers_storage_classes() {
        assert_eq!(format_value(&Value::Null), "NULL");
        assert_eq!(format_value(&Value::Integer(7)), "7");
        assert_eq!(format_value(&Value::Real(1.5)), "1.5");
        assert_eq!(format_value(&Value::Text("hi".into())), "hi");
        assert_eq!(format_value(&Value::Blob(vec![0xab, 0xcd])), "X'abcd'");
    }

    #[test]
    fn format_table_aligns_headers_and_rows() {
        let table = QueryTable {
            columns: vec!["id".into(), "body".into()],
            rows: vec![vec![Value::Integer(1), Value::Text("hello".into())]],
        };
        assert_eq!(
            format_table(&table),
            "\
id | body 
---+------
1  | hello
1 row
"
        );
    }

    #[test]
    fn handle_dot_quit_and_help() {
        let mut out = Vec::new();
        assert_eq!(handle_dot(".quit", &mut out).unwrap(), DotCommand::Quit);
        assert_eq!(handle_dot(".help", &mut out).unwrap(), DotCommand::Continue);
        let help = String::from_utf8(out).unwrap();
        assert!(help.contains(".quit"));
        assert!(help.contains("LocalOnly"));
    }

    #[test]
    fn execute_sql_and_run_script_cover_ddl_dml_and_select() {
        let directory = tempfile::tempdir().unwrap();
        let connection =
            MultiliteConnection::open(directory.path().join("shell.sqlite")).unwrap();

        let mut out = Vec::new();
        execute_sql(
            &connection,
            "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT NOT NULL)",
            &mut out,
        )
        .unwrap();
        assert_eq!(String::from_utf8_lossy(&out), "rows changed: 0\n");

        out.clear();
        let script = "\
INSERT INTO notes VALUES (1, 'hello');
SELECT id, body FROM notes;
.quit
SELECT id FROM notes;
";
        let mut err = Vec::new();
        run_script(&connection, script.as_bytes(), &mut out, &mut err).unwrap();
        let printed = String::from_utf8(out).unwrap();
        assert!(printed.contains("rows changed: 1"));
        assert!(printed.contains("id | body"));
        assert!(printed.contains("1  | hello"));
        assert!(err.is_empty(), "{err:?}");
        // `.quit` stops the script before the trailing SELECT.
        let count: i64 = connection
            .query("SELECT count(*) FROM notes", (), |row| row.get(0))
            .unwrap()[0];
        assert_eq!(count, 1);
    }

    #[test]
    fn run_script_reports_sql_errors_without_aborting() {
        let directory = tempfile::tempdir().unwrap();
        let connection =
            MultiliteConnection::open(directory.path().join("errors.sqlite")).unwrap();
        let mut out = Vec::new();
        let mut err = Vec::new();
        run_script(
            &connection,
            &b"SELECT * FROM missing;\nSELECT 1 AS one;\n"[..],
            &mut out,
            &mut err,
        )
        .unwrap();
        let err = String::from_utf8(err).unwrap();
        assert!(err.contains("Error:"), "{err}");
        let out = String::from_utf8(out).unwrap();
        assert!(out.contains("one"));
        assert!(out.contains('1'));
    }
}
