use std::env;
use std::fs;
use std::path::PathBuf;

use multilite_conformance::{Engine, RunOptions, run_paths};

fn main() {
    let args = Args::parse();
    let report = run_paths(&args.paths, &args.options);

    let json = report.to_json();
    if let Some(output) = args.output {
        if let Some(parent) = output.parent() {
            fs::create_dir_all(parent).unwrap_or_else(|error| {
                panic!(
                    "failed to create report directory {}: {error}",
                    parent.display()
                )
            });
        }
        fs::write(&output, json)
            .unwrap_or_else(|error| panic!("failed to write report {}: {error}", output.display()));
    } else {
        print!("{json}");
    }

    if report.failed_count() > 0 {
        std::process::exit(1);
    }
}

struct Args {
    options: RunOptions,
    output: Option<PathBuf>,
    paths: Vec<PathBuf>,
}

impl Args {
    fn parse() -> Self {
        let mut engine = Engine::Multilite;
        let mut output = None;
        let mut paths = Vec::new();
        let mut args = env::args().skip(1);
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--engine" => {
                    let value = args.next().expect("--engine requires sqlite or multilite");
                    engine = match value.as_str() {
                        "sqlite" => Engine::Sqlite,
                        "multilite" => Engine::Multilite,
                        "both" => Engine::Both,
                        _ => panic!(
                            "unsupported engine {value}; expected sqlite, multilite, or both"
                        ),
                    };
                }
                "--corpus" => {
                    paths.push(PathBuf::from(
                        args.next().expect("--corpus requires a path"),
                    ));
                }
                "--output" => {
                    output = Some(PathBuf::from(
                        args.next().expect("--output requires a path"),
                    ));
                }
                "--help" | "-h" => {
                    print_help();
                    std::process::exit(0);
                }
                _ if arg.starts_with('-') => panic!("unknown option {arg}"),
                _ => paths.push(PathBuf::from(arg)),
            }
        }
        if paths.is_empty() {
            print_help();
            panic!("at least one .slt/.test file or --corpus directory is required");
        }
        Self {
            options: RunOptions { engine },
            output,
            paths,
        }
    }
}

fn print_help() {
    eprintln!(
        "Usage: multilite-conformance [--engine sqlite|multilite|both] [--output report.json] [--corpus dir] <file-or-dir>..."
    );
}
