//! esmetrics binary entry point
//! (mirrors `main()` in upstream `app/victoria-metrics/main.go`).

use std::process::exit;

// Windows's default process heap serializes concurrent allocations, which
// cripples the per-series buffers of the parallel query path (Go ships its
// own allocator and is unaffected). mimalloc restores per-thread allocation.
#[cfg(windows)]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;
use std::time::Instant;

use esm_common::{errorf, infof};
use esmetrics::flags::{self, ParseOutcome, VERSION_STRING};
use esmetrics::signal;

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

    esm_common::logger::init_with_level(flags.level_filter());
    esm_common::memory::init(flags.memory_allowed_percent, flags.memory_allowed_bytes);

    infof!("starting esmetrics at {:?}...", flags.http_listen_addr);
    infof!(
        "-storageDataPath={:?}, -retentionPeriod={} (storage wiring comes in a later stage)",
        flags.storage_data_path,
        flags.retention_period
    );
    let start_time = Instant::now();
    let server = match esmetrics::run(&flags) {
        Ok(server) => server,
        Err(err) => {
            errorf!(
                "FATAL: cannot start esmetrics at {:?}: {err}",
                flags.http_listen_addr
            );
            exit(1);
        }
    };
    infof!(
        "started esmetrics in {:.3} seconds",
        start_time.elapsed().as_secs_f64()
    );

    let sig = signal::wait_for_shutdown_signal();
    infof!("received signal {sig}");

    infof!(
        "gracefully shutting down webservice at {:?}",
        flags.http_listen_addr
    );
    let start_time = Instant::now();
    server.stop();
    infof!(
        "successfully shut down the webservice in {:.3} seconds",
        start_time.elapsed().as_secs_f64()
    );
}
