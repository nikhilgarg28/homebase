//! sqlite3-style LocalOnly REPL over [`multilite::MultiliteConnection`].

use std::env;
use std::io::{self, IsTerminal, Write};
use std::path::PathBuf;
use std::process::ExitCode;

use multilite::MultiliteConnection;
use multilite_cli::{
    DotCommand, execute_sql, handle_dot, run_script, statement_complete, take_statement,
};
use rustyline::DefaultEditor;
use rustyline::error::ReadlineError;

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(code) => code,
    }
}

fn run() -> Result<(), ExitCode> {
    let mut args = env::args().skip(1);
    let Some(path) = args.next() else {
        eprintln!("usage: multilite <database>");
        return Err(ExitCode::from(2));
    };
    if args.next().is_some() {
        eprintln!("usage: multilite <database>");
        return Err(ExitCode::from(2));
    }

    let path = PathBuf::from(path);
    let connection = MultiliteConnection::open(&path).map_err(|error| {
        eprintln!("open {}: {error}", path.display());
        ExitCode::FAILURE
    })?;

    if io::stdin().is_terminal() {
        run_interactive(&connection)
    } else {
        let stdin = io::stdin();
        let mut stdout = io::stdout();
        let mut stderr = io::stderr();
        run_script(&connection, stdin.lock(), &mut stdout, &mut stderr).map_err(|error| {
            eprintln!("{error}");
            ExitCode::FAILURE
        })
    }
}

fn run_interactive(connection: &MultiliteConnection) -> Result<(), ExitCode> {
    println!("Multilite LocalOnly shell. Enter SQL ending with ';'. Dot commands: .help .quit");
    let mut editor = DefaultEditor::new().map_err(|error| {
        eprintln!("readline: {error}");
        ExitCode::FAILURE
    })?;
    let mut buffer = String::new();
    let mut stdout = io::stdout();
    let mut stderr = io::stderr();

    loop {
        let prompt = if buffer.is_empty() {
            "mlite> "
        } else {
            "   ...> "
        };
        let line = match editor.readline(prompt) {
            Ok(line) => line,
            Err(ReadlineError::Interrupted) => {
                buffer.clear();
                println!("^C");
                continue;
            }
            Err(ReadlineError::Eof) => break,
            Err(error) => {
                eprintln!("readline: {error}");
                return Err(ExitCode::FAILURE);
            }
        };

        let trimmed = line.trim();
        if buffer.is_empty() && trimmed.starts_with('.') {
            match handle_dot(trimmed, &mut stdout).map_err(|error| {
                eprintln!("{error}");
                ExitCode::FAILURE
            })? {
                DotCommand::Continue => {
                    let _ = editor.add_history_entry(trimmed);
                    let _ = stdout.flush();
                    continue;
                }
                DotCommand::Quit => break,
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
        let _ = editor.add_history_entry(format!("{sql};"));
        if let Err(error) = execute_sql(connection, &sql, &mut stdout) {
            let _ = writeln!(stderr, "Error: {error}");
        }
        let _ = stdout.flush();
        let _ = stderr.flush();
    }

    Ok(())
}
