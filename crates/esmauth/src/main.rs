//! esmauth binary entry point (mirrors `main()` in upstream
//! `app/vmauth/main.go`).

use std::process::exit;

// Windows's default process heap serializes concurrent allocations; mimalloc
// restores per-thread allocation, matching the esmetrics binary.
#[cfg(windows)]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;
use std::time::Instant;

use esm_common::{errorf, infof};
use esmauth::flags::{self, ParseOutcome, VERSION_STRING};
use esmauth::signal;

fn main() {
    let flags = match flags::parse(std::env::args().skip(1)) {
        Ok(ParseOutcome::Flags(flags)) => flags,
        Ok(ParseOutcome::Help) => {
            print!("{}", flags::usage());
            return;
        }
        Ok(ParseOutcome::Version) => {
            println!("{VERSION_STRING}");
            return;
        }
        Err(msg) => {
            eprintln!("{msg}");
            exit(2); // Go's flag.ExitOnError status
        }
    };

    esm_common::logger::init();

    // Install signal handlers before starting so an early SIGHUP/SIGTERM is
    // captured rather than hitting the default disposition.
    signal::install();

    infof!("starting esmauth at {:?}...", flags.http_listen_addr);
    infof!("-auth.config={:?}", flags.auth_config);
    let start_time = Instant::now();
    let app = match esmauth::run(&flags) {
        Ok(app) => app,
        Err(err) => {
            // The error from `run` is secret-free (config-load errors are
            // sanitized before surfacing).
            errorf!(
                "FATAL: cannot start esmauth at {:?}: {err}",
                flags.http_listen_addr
            );
            exit(1);
        }
    };
    infof!(
        "started esmauth in {:.3} seconds",
        start_time.elapsed().as_secs_f64()
    );

    let sig = signal::wait_for_shutdown_signal();
    infof!("received signal {sig}");

    infof!(
        "gracefully shutting down webservice at {:?}",
        flags.http_listen_addr
    );
    let start_time = Instant::now();
    app.stop();
    infof!(
        "successfully shut down the webservice in {:.3} seconds",
        start_time.elapsed().as_secs_f64()
    );
}
