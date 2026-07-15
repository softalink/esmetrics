//! `esmalert-tool` binary entry point (mirrors `esmalert`'s bin/lib split):
//! parse argv, dispatch to a subcommand, map its result to a process exit
//! code. The rest of the crate lives in `lib.rs` so integration tests can
//! call `esmalert_tool::run_unittest` in-process.
//!
//! Reference: `app/vmalert-tool/main.go:1-80` (subcommand dispatch, exit
//! codes via the `cli` package's `App.Run` + `Action` error convention).

use std::process::exit;

fn main() {
    let argv: Vec<String> = std::env::args().skip(1).collect();
    exit(run(&argv));
}

/// Parses `argv` (without the program name) and dispatches to the matching
/// subcommand, returning the process exit code:
/// - `0`: `unittest` ran and every test in every file passed.
/// - `1`: `unittest` ran but at least one assertion failed, or a file-level
///   error occurred while running it (bad YAML, missing rule file, unreadable
///   test file, etc.) — see [`esmalert_tool::run_unittest`]'s doc comment for
///   the `Ok(false)`-vs-`Err` split; both map to this exit code.
/// - `2`: a usage error — no subcommand given, an unknown subcommand, or
///   `unittest` given no files. Mirrors Go's `flag.ExitOnError`/`cli`
///   convention `esmalert::main` already follows for its own usage errors.
fn run(argv: &[String]) -> i32 {
    match argv.split_first() {
        None => usage_error("no subcommand given"),
        Some((cmd, rest)) if cmd == "unittest" => run_unittest_subcommand(rest),
        Some((cmd, _)) => usage_error(&format!("unknown subcommand: {cmd:?}")),
    }
}

fn run_unittest_subcommand(files: &[String]) -> i32 {
    if files.is_empty() {
        return usage_error("\"unittest\" requires at least one test file");
    }
    match esmalert_tool::run_unittest(files) {
        Ok(true) => 0,
        Ok(false) => 1,
        Err(e) => {
            eprintln!("esmalert-tool: {e}");
            1
        }
    }
}

fn usage_error(msg: &str) -> i32 {
    eprintln!("esmalert-tool: {msg}\n\n{}", usage());
    2
}

fn usage() -> String {
    "esmalert-tool - a Rust port of the upstream VictoriaMetrics vmalert-tool.\n\n\
     Usage:\n\
     \x20 esmalert-tool unittest <file>...   Run unit tests defined in each file\n"
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::run;

    fn args(a: &[&str]) -> Vec<String> {
        a.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn no_subcommand_is_a_usage_error() {
        assert_eq!(run(&args(&[])), 2);
    }

    #[test]
    fn unknown_subcommand_is_a_usage_error() {
        assert_eq!(run(&args(&["bogus"])), 2);
    }

    #[test]
    fn unittest_with_no_files_is_a_usage_error() {
        assert_eq!(run(&args(&["unittest"])), 2);
    }

    #[test]
    fn unittest_with_a_nonexistent_file_exits_1() {
        assert_eq!(run(&args(&["unittest", "/no/such/file.yml"])), 1);
    }
}
