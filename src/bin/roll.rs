//! Roll the playground: run the pingpong test continuously.
//!
//! `cargo run` ticks the Schedule on an interval and redraws the live matrix
//! each round — the pingpong test as ADR-0028 means it, over time and never
//! stopping. Every implemented transport by every content contract, over real
//! loopback connections, validated against a real contract on arrival.
//!
//! Pass a number to run that many rounds and stop (a bounded look); omit it to
//! roll until interrupted. When stdout is a terminal the board is redrawn in
//! place; when it is piped, one summary line per round is appended instead.
//!
//! After every tick it writes the snapshot to a JSON file the monitoring GUI
//! reads, so an operator watches the same matrix in the web or desktop UI over
//! time. The path is `XMIP_PLAYGROUND_SNAPSHOT` if set, else a well-known temp
//! file the GUI defaults to as well — the two agree with no configuration.

use std::io::IsTerminal;
use std::path::PathBuf;
use std::time::Duration;

use xmip_observe::{Health, History, Snapshot};
use xmip_test_playground::{CONTRACTS, Schedule, history_toml, to_toml, write_atomic};

fn main() {
    let node = "xmip:///playground";
    let file_dir = std::env::temp_dir().join("xmip-playground");
    std::fs::remove_dir_all(&file_dir).ok();
    let mut schedule = Schedule::new(node, &file_dir);

    // An hour of history at one point a second: enough to watch a shift, bounded
    // so a week-long run does not grow. ADR-0029.
    let mut history = History::with_capacity(3600);

    let snapshot_path = snapshot_path();
    let history_path = history_path();
    let limit: Option<u64> = std::env::args().nth(1).and_then(|arg| arg.parse().ok());
    let live = std::io::stdout().is_terminal();
    let interval = Duration::from_millis(1000);

    if !live {
        println!("publishing snapshots to {}", snapshot_path.display());
        println!("publishing history to   {}", history_path.display());
    }

    let mut round: u64 = 0;
    loop {
        round += 1;
        let snapshot = schedule.tick();
        history.record(&snapshot);

        if let Err(error) = write_atomic(&snapshot_path, &to_toml(node, &snapshot)) {
            eprintln!(
                "could not write the snapshot to {}: {error}",
                snapshot_path.display()
            );
        }

        if let Err(error) = write_atomic(&history_path, &history_toml(node, &history)) {
            eprintln!(
                "could not write the history to {}: {error}",
                history_path.display()
            );
        }

        if live {
            redraw(node, round, &snapshot);
            println!("  publishing to {}", snapshot_path.display());
        } else {
            summarise(node, round, &snapshot);
        }

        if limit.is_some_and(|limit| round >= limit) {
            break;
        }
        std::thread::sleep(interval);
    }

    std::fs::remove_dir_all(&file_dir).ok();
}

/// Where the snapshot is written for the GUI to read: the environment override,
/// or the well-known temp file the GUI defaults to as well.
fn snapshot_path() -> PathBuf {
    std::env::var_os("XMIP_PLAYGROUND_SNAPSHOT").map_or_else(
        || std::env::temp_dir().join("xmip-playground-snapshot.toml"),
        PathBuf::from,
    )
}

/// Where the throughput history is written for the CLI and UI to read.
fn history_path() -> PathBuf {
    std::env::var_os("XMIP_PLAYGROUND_HISTORY").map_or_else(
        || std::env::temp_dir().join("xmip-playground-history.toml"),
        PathBuf::from,
    )
}

/// The full board, cleared and reprinted in place — a live terminal view.
fn redraw(node: &str, round: u64, snapshot: &Snapshot) {
    print!("\x1b[2J\x1b[H");
    println!("Xmip Playground — rolling the pingpong test   (round {round})");
    println!("{:-<74}", "");

    for record in pairs(node, snapshot) {
        let pair = record
            .scope
            .rsplit("/exercise/")
            .next()
            .unwrap_or(&record.scope);
        println!(
            "  {:<22} {:<7} sev {:>3}   {}",
            pair,
            word(record.health),
            record.severity,
            record.evidence
        );
    }

    let count = pairs(node, snapshot).len();
    println!("{:-<74}", "");
    println!("  {}", rollup(node, snapshot, count));
    println!("\n  ctrl-c to stop");
}

/// One line per round, for a piped run: the rollup, and the worst pair when it
/// is not green.
fn summarise(node: &str, round: u64, snapshot: &Snapshot) {
    let worst = snapshot.worst(node).map_or("NONE", word);
    let count = pairs(node, snapshot).len();

    let trouble = pairs(node, snapshot)
        .into_iter()
        .find(|record| record.health != Health::Green)
        .map_or_else(String::new, |record| {
            format!("  — worst {}: {}", record.scope, record.evidence)
        });

    println!("round {round:>4}: {worst}  ({count} pairs){trouble}");
}

fn pairs(node: &str, snapshot: &Snapshot) -> Vec<xmip_observe::HealthRecord> {
    let mut records = snapshot.health(&format!("{node}/exercise"));
    records.sort_by(|left, right| left.scope.cmp(&right.scope));
    records
}

fn rollup(node: &str, snapshot: &Snapshot, pairs: usize) -> String {
    let worst = snapshot.worst(node).map_or("NONE", word);
    let contracts = CONTRACTS.len();
    let transports = if contracts == 0 { 0 } else { pairs / contracts };
    format!(
        "rollup at {node}: {worst}   ({transports} transports × {contracts} contracts = {pairs} pairs)"
    )
}

fn word(health: Health) -> &'static str {
    match health {
        Health::Green => "GREEN",
        Health::Yellow => "YELLOW",
        Health::Red => "RED",
    }
}
