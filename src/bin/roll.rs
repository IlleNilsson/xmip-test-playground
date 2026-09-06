//! Roll the playground: run every scenario continuously.
//!
//! `cargo run` ticks all four scenarios on an interval and redraws the combined
//! board each round — the tests as ADR-0028 means them, over time and never
//! stopping:
//!
//!   - **pingpong** — every transport by every contract round-trips and holds
//!     its contract; the message-path stages, with injected faults.
//!   - **furious** — the same pairs, timed against a latency budget (p50/p99).
//!   - **load** — a megabyte per pair; does it arrive whole and still validate.
//!   - **secretary** — retention and archiving: keep, archive, purge, by age.
//!
//! Each publishes under its own subtree of `xmip:///playground`, merged into one
//! snapshot so the rollup covers all four and an operator drills scenario →
//! detail → the failing leaf.
//!
//! Pass a number to run that many rounds and stop; omit it to roll until
//! interrupted. When stdout is a terminal the board is redrawn in place; when it
//! is piped, one summary line per round is appended. After every tick the
//! snapshot, history and activity are written to the TOML files the monitoring
//! GUI reads, overridable with `XMIP_PLAYGROUND_SNAPSHOT`, `_HISTORY`, `_ACTIVITY`.

use std::io::IsTerminal;
use std::path::{Path, PathBuf};
use std::time::Duration;

use observe::{Health, History, Snapshot};
use xmip_test_playground::{
    FaultPlan, Furious, Load, Schedule, Secretary, activity_toml, history_toml, to_toml,
    write_atomic,
};

fn main() {
    let root = "xmip:///playground";
    let base = std::env::temp_dir().join("playground");
    std::fs::remove_dir_all(&base).ok();

    // Each scenario under its own subtree, each with faults or pressure on, so
    // the board is realistic rather than uniformly green. `file` stays clean in
    // every one.
    let mut pingpong = Schedule::new(format!("{root}/pingpong"), base.join("pingpong"))
        .with_faults(FaultPlan::realistic());
    let mut furious =
        Furious::new(format!("{root}/furious"), base.join("furious")).under_pressure();
    let mut load = Load::new(format!("{root}/load"), base.join("load")).under_pressure();
    let mut secretary = Secretary::new(format!("{root}/secretary")).under_pressure();

    // An hour of history at one point a second: enough to watch a shift, bounded
    // so a week-long run does not grow. ADR-0029.
    let mut history = History::with_capacity(3600);

    let snapshot_path = env_path("XMIP_PLAYGROUND_SNAPSHOT", "playground-snapshot.toml");
    let history_path = env_path("XMIP_PLAYGROUND_HISTORY", "playground-history.toml");
    let activity_path = env_path("XMIP_PLAYGROUND_ACTIVITY", "playground-activity.toml");
    let limit: Option<u64> = std::env::args().nth(1).and_then(|arg| arg.parse().ok());
    let live = std::io::stdout().is_terminal();
    let interval = Duration::from_millis(1000);

    if !live {
        println!("publishing snapshots to {}", snapshot_path.display());
    }

    let mut round: u64 = 0;
    loop {
        round += 1;

        let mut snapshot = Snapshot::new();
        merge(&mut snapshot, &pingpong.tick());
        merge(&mut snapshot, &furious.tick());
        merge(&mut snapshot, &load.tick());
        merge(&mut snapshot, &secretary.tick());

        history.record(&snapshot);

        write(&snapshot_path, &to_toml(root, &snapshot), "snapshot");
        write(&history_path, &history_toml(root, &history), "history");
        write(
            &activity_path,
            &activity_toml(root, pingpong.activity()),
            "activity",
        );

        if live {
            redraw(root, round, &snapshot);
            println!("  publishing to {}", snapshot_path.display());
        } else {
            summarise(root, round, &snapshot);
        }

        if limit.is_some_and(|limit| round >= limit) {
            break;
        }
        std::thread::sleep(interval);
    }

    std::fs::remove_dir_all(&base).ok();
}

/// Copy every health record and count from one scenario's snapshot into the
/// combined one. Scopes are disjoint per scenario, so nothing collides.
fn merge(into: &mut Snapshot, from: &Snapshot) {
    for record in from.health_records() {
        into.record_health(record.clone());
    }
    for count in from.all_counts() {
        into.record_count(count.clone());
    }
}

/// A publish path: the environment override, or the well-known temp file the GUI
/// defaults to as well. The variable is external, so it keeps the prefix.
fn env_path(variable: &str, default: &str) -> PathBuf {
    std::env::var_os(variable).map_or_else(|| std::env::temp_dir().join(default), PathBuf::from)
}

fn write(path: &Path, contents: &str, what: &str) {
    if let Err(error) = write_atomic(path, contents) {
        eprintln!("could not write the {what} to {}: {error}", path.display());
    }
}

/// The full board, cleared and reprinted in place — a live terminal view.
fn redraw(node: &str, round: u64, snapshot: &Snapshot) {
    print!("\x1b[2J\x1b[H");
    println!("Xmip Playground — rolling every scenario   (round {round})");
    println!("{:-<86}", "");

    for record in pairs(node, snapshot) {
        let leaf = record
            .scope
            .strip_prefix(&format!("{node}/"))
            .unwrap_or(&record.scope);
        println!(
            "  {:<44} {:<7} sev {:>3}   {}",
            leaf,
            word(record.health),
            record.severity,
            record.evidence
        );
    }

    println!("{:-<86}", "");
    println!(
        "  rollup at {node}: {}",
        word(snapshot.worst(node).unwrap_or(Health::Green))
    );
    println!("\n  ctrl-c to stop");
}

/// One line per round, for a piped run: the rollup, and the worst leaf when it is
/// not green.
fn summarise(node: &str, round: u64, snapshot: &Snapshot) {
    let worst = snapshot.worst(node).map_or("NONE", word);
    let count = pairs(node, snapshot).len();

    let trouble = pairs(node, snapshot)
        .into_iter()
        .find(|record| record.health != Health::Green)
        .map_or_else(String::new, |record| {
            format!("  — worst {}: {}", record.scope, record.evidence)
        });

    println!("round {round:>4}: {worst}  ({count} leaves){trouble}");
}

fn pairs(node: &str, snapshot: &Snapshot) -> Vec<observe::HealthRecord> {
    let mut records = snapshot.health(node);
    records.sort_by(|left, right| left.scope.cmp(&right.scope));
    records
}

fn word(health: Health) -> &'static str {
    match health {
        Health::Green => "GREEN",
        Health::Yellow => "YELLOW",
        Health::Red => "RED",
    }
}
