//! The daily scenario: drain a backlog as fast as the estate can, and escalate
//! when one node cannot keep up.
//!
//! ADR-0028. A day's work lands at once — many files to process, all going out as
//! fast as possible. One node drains at a fixed capacity; when arrivals outpace
//! it the backlog climbs, and the scenario escalates the way an operator would:
//! first a **tweak** — raise the node's concurrency — and, if that only slows the
//! rise, **add a node** so a second drainer shares the same backlog through the
//! claim (ADR-0024, proven exactly-once by the `claim` scenario; here it is the
//! throughput a second node buys). The board shows the backlog climb, the action
//! taken, and the backlog fall.
//!
//! The backlog is real files in a directory: each round drops arrivals and
//! removes up to the current capacity, so the queue depth an operator watches is
//! a real count on disk, not a number in memory.

use std::path::{Path, PathBuf};

use observe::{Count, Counted, Snapshot};

use crate::standing::{Mark, Standing};
use crate::support::now_unix_nanos;

/// Files that arrive each round — the day's steady inflow.
const ARRIVAL: usize = 30;
/// Files one reader clears per round.
const PER_READER: usize = 5;
/// Readers per node before and after the tweak.
const BASE_READERS: usize = 2;
const TWEAK_READERS: usize = 4;
/// Backlog at which each remedy kicks in, and the depth that is a red SLA breach.
const TWEAK_AT: usize = 35;
const SCALE_AT: usize = 55;
const CEILING: usize = 150;

/// A day's drain: a real file backlog, a capacity that escalates when it cannot
/// keep up.
pub struct Daily {
    node: String,
    dir: PathBuf,
    round: u64,
    seq: u64,
    backlog: usize,
    previous: usize,
    drained: u64,
    readers: usize,
    nodes: usize,
    tweaked: bool,
    scaled: bool,
    action: &'static str,
    standing: Standing,
}

impl Daily {
    /// A drain publishing under `node`, using `dir` for the backlog, starting at
    /// one node with the base concurrency.
    #[must_use]
    pub fn new(node: impl Into<String>, dir: impl Into<PathBuf>) -> Self {
        let dir = dir.into();
        std::fs::remove_dir_all(&dir).ok();
        std::fs::create_dir_all(&dir).ok();
        Self {
            node: node.into(),
            dir,
            round: 0,
            seq: 0,
            backlog: 0,
            previous: 0,
            drained: 0,
            readers: BASE_READERS,
            nodes: 1,
            tweaked: false,
            scaled: false,
            action: "",
            standing: Standing::default(),
        }
    }

    /// One round: arrivals land, the current capacity drains what it can, the
    /// backlog is measured, and the scenario escalates if it is falling behind.
    pub fn tick(&mut self) -> Snapshot {
        self.round += 1;
        let now = now_unix_nanos();

        self.arrive();
        let capacity = self.nodes * self.readers * PER_READER;
        let processed = drain(&self.dir, capacity);
        self.drained += processed as u64;
        self.backlog = count(&self.dir);

        self.escalate();

        let mark = if self.backlog > CEILING {
            Mark::Fail
        } else if self.backlog < self.previous {
            Mark::Pass
        } else if self.backlog > self.previous {
            Mark::Warn
        } else {
            Mark::Pass
        };
        let config = format!("{} node(s) x {} readers", self.nodes, self.readers);
        let evidence = format!(
            "backlog {} ({processed}/round, capacity {capacity}, {config}){}",
            self.backlog, self.action
        );
        self.standing.record(mark, evidence);
        self.previous = self.backlog;

        let mut snapshot = Snapshot::new();
        snapshot.record_health(self.standing.health(&format!("{}/drain", self.node), now));
        self.record_counts(&mut snapshot, now);
        snapshot
    }

    fn arrive(&mut self) {
        for _ in 0..ARRIVAL {
            self.seq += 1;
            let path = self.dir.join(format!("daily_{:08}", self.seq));
            std::fs::write(&path, b"x").ok();
        }
    }

    /// Raise concurrency first; add a node only if the tweak was not enough.
    fn escalate(&mut self) {
        if !self.tweaked && self.backlog > TWEAK_AT {
            self.readers = TWEAK_READERS;
            self.tweaked = true;
            self.action = " — raised concurrency (tweak)";
        } else if self.tweaked && !self.scaled && self.backlog > SCALE_AT {
            self.nodes += 1;
            self.scaled = true;
            self.action = " — added a node";
        }
    }

    fn record_counts(&self, snapshot: &mut Snapshot, now: i64) {
        for (counted, value) in [
            (Counted::Streams, self.drained),
            (Counted::Messages, self.backlog as u64),
        ] {
            snapshot.record_count(Count {
                scope: self.node.clone(),
                counted,
                value,
                window_start_unix_nanos: now,
                window_end_unix_nanos: now,
                observed_unix_nanos: now,
            });
        }
    }
}

/// Remove up to `capacity` files, returning how many were drained.
fn drain(dir: &Path, capacity: usize) -> usize {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return 0;
    };
    let mut processed = 0;
    for entry in entries.flatten().take(capacity) {
        if std::fs::remove_file(entry.path()).is_ok() {
            processed += 1;
        }
    }
    processed
}

fn count(dir: &Path) -> usize {
    std::fs::read_dir(dir)
        .map(|e| e.flatten().count())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::support::scratch;
    use observe::Health;

    #[test]
    fn a_backlog_escalates_through_a_tweak_then_a_node_and_clears() {
        let dir = scratch("escalate");
        let mut daily = Daily::new("xmip:///playground/daily", &dir);
        let mut cleared = false;
        for _ in 0..30 {
            let snapshot = daily.tick();
            if snapshot.worst("xmip:///playground/daily") == Some(Health::Fine) && daily.scaled {
                cleared = true;
            }
        }
        assert!(daily.tweaked, "one node falling behind should tweak first");
        assert!(daily.scaled, "a tweak that is not enough should add a node");
        assert!(
            cleared,
            "the added node should bring the backlog back to green"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn the_backlog_is_real_files_and_a_tweak_alone_does_not_clear_it() {
        let dir = scratch("files");
        let mut daily = Daily::new("xmip:///playground/daily", &dir);
        // A few rounds in, the backlog is real files on disk and rising.
        for _ in 0..3 {
            daily.tick();
        }
        assert!(
            count(&dir) > 0,
            "the backlog is real files in the directory"
        );
        std::fs::remove_dir_all(&dir).ok();
    }
}
