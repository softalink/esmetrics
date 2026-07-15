//! esmalert binary entry point (mirrors `main()` in upstream
//! `app/vmalert/main.go`, and `esmauth`'s binary entry point in structure:
//! parse flags -> `-dryRun`/`-help`/`-version` short-circuits -> `app::run`).
//!
//! The rest of the crate lives in `lib.rs` (mirrors `esmauth`'s
//! `main.rs`/`lib.rs` split, done so integration tests can drive the engine
//! in-process); this file stays a thin shell.

// Windows's default process heap serializes concurrent allocations; mimalloc
// restores per-thread allocation, matching the esmetrics/esmauth binaries.
#[cfg(windows)]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

use std::process::exit;

use esm_common::{errorf, infof};
use esmalert::flags::{self, FlagError, VERSION_STRING};
use esmalert::{app, signal};

fn main() {
    let argv: Vec<String> = std::env::args().skip(1).collect();
    let parsed = match flags::parse_flags(&argv) {
        Ok(f) => f,
        Err(FlagError::Help) => {
            print!("{}", flags::usage());
            return;
        }
        Err(FlagError::Version) => {
            println!("{VERSION_STRING}");
            return;
        }
        Err(FlagError::Invalid(msg)) => {
            eprintln!("{msg}");
            exit(2); // Go's flag.ExitOnError status
        }
    };

    if parsed.dry_run {
        match app::run_dry(&parsed) {
            Ok(()) => {
                println!("esmalert: dryRun OK, rule config is valid");
                return;
            }
            Err(e) => {
                eprintln!("esmalert: dryRun failed: {e}");
                exit(1);
            }
        }
    }

    esm_common::logger::init();
    // Install signal handlers before starting so an early SIGINT/SIGTERM is
    // captured rather than hitting the default disposition.
    signal::install();

    infof!("starting esmalert at {:?}...", parsed.http_listen_addr);
    if let Err(e) = app::run(parsed) {
        // `app::run`'s error is secret-free: every fallible step maps its
        // error via `.to_string()` on types that never format auth
        // credentials (see datasource::DsError / notifier::NotifyError /
        // remotewrite::RwError's doc comments).
        errorf!("FATAL: cannot start esmalert: {e}");
        exit(1);
    }
}
