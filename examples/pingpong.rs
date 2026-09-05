//! Run the pingpong test and print its verdicts.
//!
//! `cargo run --example pingpong` builds a Schedule over every wired transport
//! and every content contract, ticks it a few rounds on real loopback sockets,
//! and prints the health per pair and the rollup. This is ADR-0028's pingpong
//! test doing what it says — on this machine, right now — rather than a unit
//! test asserting one round in isolation.

use std::time::Instant;

use xmip_observe::Health;
use xmip_test_playground::Schedule;

fn main() {
    let node = "xmip:///playground";
    let file_dir = std::env::temp_dir().join("xmip-pingpong-example");
    std::fs::remove_dir_all(&file_dir).ok();

    let mut schedule = Schedule::new(node, &file_dir);

    let rounds = 5;
    let started = Instant::now();
    let mut latest = schedule.tick();
    for _ in 1..rounds {
        latest = schedule.tick();
    }
    let elapsed = started.elapsed();

    let exercise = format!("{node}/exercise");
    let mut records = latest.health(&exercise);
    records.sort_by(|left, right| left.scope.cmp(&right.scope));

    println!();
    println!("pingpong test — {rounds} rounds over every transport × contract");
    println!("{:-<74}", "");
    for record in &records {
        let pair = record
            .scope
            .strip_prefix(&format!("{exercise}/"))
            .unwrap_or(&record.scope);
        println!(
            "  {:<22} {:<7} sev {:>3}   {}",
            pair,
            word(record.health),
            record.severity,
            record.evidence
        );
    }
    println!("{:-<74}", "");

    let rollup = latest.worst(node).map_or("NONE", word);
    println!(
        "  rollup at {node}: {rollup}   ({} pairs, {rounds} rounds in {elapsed:.1?})",
        records.len()
    );
    println!();

    std::fs::remove_dir_all(&file_dir).ok();
}

fn word(health: Health) -> &'static str {
    match health {
        Health::Green => "GREEN",
        Health::Yellow => "YELLOW",
        Health::Red => "RED",
    }
}
