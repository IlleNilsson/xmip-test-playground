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

use std::io::IsTerminal;
use std::time::Duration;

use xmip_observe::{Health, Snapshot};
use xmip_test_playground::{CONTRACTS, Schedule};

fn main() {
    let node = "xmip:///playground";
    let file_dir = std::env::temp_dir().join("xmip-playground");
    std::fs::remove_dir_all(&file_dir).ok();
    let mut schedule = Schedule::new(node, &file_dir);

    let limit: Option<u64> = std::env::args().nth(1).and_then(|arg| arg.parse().ok());
    let live = std::io::stdout().is_terminal();
    let interval = Duration::from_millis(1000);

    let mut round: u64 = 0;
    loop {
        round += 1;
        let snapshot = schedule.tick();

        if live {
            redraw(node, round, &snapshot);
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
