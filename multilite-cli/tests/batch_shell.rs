//! Binary smoke tests for non-interactive `multilite` scripts.

use std::process::{Command, Stdio};

#[test]
fn piped_script_creates_queries_and_quits() {
    let directory = tempfile::tempdir().unwrap();
    let db = directory.path().join("batch.sqlite");
    let bin = env!("CARGO_BIN_EXE_multilite");

    let mut child = Command::new(bin)
        .arg(&db)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn multilite");

    {
        use std::io::Write;
        let stdin = child.stdin.as_mut().expect("stdin");
        write!(
            stdin,
            "\
CREATE TABLE notes (
  id INTEGER PRIMARY KEY,
  body TEXT NOT NULL
);
INSERT INTO notes VALUES (1, 'hello');
SELECT id, body FROM notes;
.quit
"
        )
        .unwrap();
    }

    let output = child.wait_with_output().expect("wait");
    assert!(
        output.status.success(),
        "stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("rows changed: 0"), "{stdout}");
    assert!(stdout.contains("rows changed: 1"), "{stdout}");
    assert!(stdout.contains("id | body"), "{stdout}");
    assert!(stdout.contains("1  | hello"), "{stdout}");
    assert!(db.exists());
}

#[test]
fn missing_database_argument_exits_with_usage() {
    let bin = env!("CARGO_BIN_EXE_multilite");
    let output = Command::new(bin)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .expect("run multilite");
    assert_eq!(output.status.code(), Some(2));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("usage: multilite <database>"), "{stderr}");
}
