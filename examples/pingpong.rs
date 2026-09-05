//! Run the pingpong test and print its verdicts.
//!
//! `cargo run --example pingpong` builds a Schedule over every wired transport
//! and every content contract, ticks it a few rounds on real loopback sockets,
//! and prints the health per pair and the rollup. This is ADR-0028's pingpong
//! test doing what it says — on this machine, right now — rather than a unit
//! test asserting one round in isolation.

use std::time::Instant;

use observe::Health;
use stream::Stream;
use xcore::StreamId;
use xmip_test_playground::{Contract, Schedule};

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

    let mut records = latest.health(node);
    records.sort_by(|left, right| left.scope.cmp(&right.scope));

    println!();
    println!("pingpong test — {rounds} rounds over every stage, transport and contract");
    println!("{:-<74}", "");
    for record in &records {
        let pair = record
            .scope
            .strip_prefix(&format!("{node}/"))
            .unwrap_or(&record.scope);
        println!(
            "  {:<28} {:<7} sev {:>3}   {}",
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

    demonstrate_contracts_bite();

    std::fs::remove_dir_all(&file_dir).ok();
}

/// The point of validating a Stream rather than comparing bytes: a malformed
/// Stream is a contract violation, not a pass. This shows each real contract
/// holding a good Stream and rejecting a bad one.
fn demonstrate_contracts_bite() {
    println!("contract validation — a malformed Stream must not pass");
    println!("{:-<74}", "");
    let cases = [
        (Contract::Json, &br#"{"ok":true}"#[..], &b"{not json"[..]),
        (Contract::Xml, &b"<a><b/></a>"[..], &b"<a><b></a>"[..]),
        (
            Contract::Text,
            "xmip \u{2713}".as_bytes(),
            &[0xff, 0xfe][..],
        ),
    ];
    for (contract, good, bad) in cases {
        show(contract, "well-formed", good);
        show(contract, "malformed  ", bad);
    }
    println!();
}

fn show(contract: Contract, label: &str, bytes: &[u8]) {
    let stream = Stream::new(
        StreamId::new(1),
        bytes.to_vec(),
        Some(contract.shape().representation().to_string()),
    );
    match contract.validate(&stream) {
        Ok(()) => println!("  {:<5} {label}   HELD", contract.name()),
        Err(why) => println!("  {:<5} {label}   VIOLATED — {why}", contract.name()),
    }
}

fn word(health: Health) -> &'static str {
    match health {
        Health::Green => "GREEN",
        Health::Yellow => "YELLOW",
        Health::Red => "RED",
    }
}
