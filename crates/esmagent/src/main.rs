//! esmagent binary entry point (mirrors `esmalert`'s `main.rs`/`lib.rs`
//! split): parse flags -> `-help`/`-version`/`-promscrape.config.dryRun`/
//! `-dryRun` short-circuits -> `esmagent::run` -> loop on "shutdown OR
//! SIGHUP OR `-promscrape.configCheckInterval` elapsed" -> on shutdown,
//! graceful stop; on reload, re-read `-promscrape.config` and keep serving.

// Windows's default process heap serializes concurrent allocations; mimalloc
// restores per-thread allocation, matching the esmetrics/esmalert binaries.
#[cfg(windows)]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

use std::process::exit;

use esm_common::{errorf, infof};
use esmagent::flags::{self, FlagError, VERSION_STRING};
use esmagent::signal;

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

    if parsed.promscrape_config_dry_run {
        match esmagent::run_scrape_config_dry(&parsed) {
            Ok(()) => {
                println!("esmagent: promscrape config dryRun OK, config is valid");
                return;
            }
            Err(e) => {
                eprintln!("esmagent: promscrape config dryRun failed: {e}");
                exit(1);
            }
        }
    }

    if parsed.dry_run {
        match esmagent::run_dry(&parsed) {
            Ok(()) => {
                println!("esmagent: dryRun OK, config is valid");
                return;
            }
            Err(e) => {
                eprintln!("esmagent: dryRun failed: {e}");
                exit(1);
            }
        }
    }

    esm_common::logger::init();
    // Install signal handlers before starting so an early SIGINT/SIGTERM/
    // SIGHUP is captured rather than hitting the default disposition.
    signal::install();

    infof!("starting esmagent at {:?}...", parsed.http_listen_addr);
    let mut app = match esmagent::run(&parsed) {
        Ok(app) => app,
        Err(e) => {
            // `esmagent::run`'s error is secret-free: every fallible step
            // maps its error via `.to_string()`/explicit formatting that
            // never includes an auth credential value (see
            // `resolve_auth`/`resolve_secret`'s doc comments in lib.rs).
            errorf!("FATAL: cannot start esmagent: {e}");
            exit(1);
        }
    };
    infof!("esmagent: serving on {}", app.local_addr());

    // `0`/unset means "no interval polling" — the scrape config is still
    // reloadable via SIGHUP either way (`signal::wait_for_event` observes
    // SIGHUP regardless of `timeout`).
    let check_interval = (!parsed.promscrape_config_check_interval.is_zero())
        .then_some(parsed.promscrape_config_check_interval);

    loop {
        match signal::wait_for_event(check_interval) {
            signal::Event::Shutdown(sig) => {
                infof!("esmagent: received signal {sig}, shutting down");
                break;
            }
            signal::Event::Reload => {
                infof!("esmagent: SIGHUP received, reloading -promscrape.config");
                reload_scrape_config(&mut app, &parsed);
            }
            signal::Event::Timeout => {
                reload_scrape_config(&mut app, &parsed);
            }
        }
    }
    app.stop();
}

/// Re-reads `-promscrape.config` and reloads the scrape manager. A failure
/// (unreadable file, bad YAML, failed validation) is logged and otherwise
/// ignored — the scrape manager keeps its previous config running (see
/// `esmagent::App::reload_scrape_config`'s doc); this must never crash the
/// process, since it runs on every SIGHUP and every
/// `-promscrape.configCheckInterval` tick for the life of the process.
fn reload_scrape_config(app: &mut esmagent::App, flags: &esmagent::flags::Flags) {
    if let Err(e) = app.reload_scrape_config(flags) {
        errorf!("esmagent: promscrape config reload failed (keeping previous config): {e}");
    }
}
