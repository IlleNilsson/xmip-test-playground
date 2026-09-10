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
//!
//! **Across processes, 2026-09-09.** A [`Daily::shared`] drain works a directory
//! other node processes drop into and drain from at the same time. Its own
//! arrivals carry its process id, so the directory tells how many nodes are
//! feeding it, and the node judges its *share* of the backlog — the depth
//! divided by the feeders — so ten nodes over one directory escalate the way one
//! node over its own does. A file another node removed first is a lost race,
//! and the drain takes the next one (ADR-0024 clause 7).

use std::collections::BTreeSet;
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
/// Backlog share at which each remedy kicks in, and the depth that is a red SLA
/// breach.
const TWEAK_AT: usize = 35;
const SCALE_AT: usize = 55;
const CEILING: usize = 150;

/// A day's drain: a real file backlog, a capacity that escalates when it cannot
/// keep up.
pub struct Daily {
    node: String,
    dir: PathBuf,
    tag: String,
    round: u64,
    seq: u64,
    backlog: usize,
    feeders: usize,
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
    /// one node with the base concurrency. The directory is this drain's own
    /// and starts empty.
    #[must_use]
    pub fn new(node: impl Into<String>, dir: impl Into<PathBuf>) -> Self {
        let dir = dir.into();
        std::fs::remove_dir_all(&dir).ok();
        Self::shared(node, dir)
    }

    /// The same drain over a directory other node processes feed and drain
    /// too. Nothing already in it is touched at start; it is theirs.
    #[must_use]
    pub fn shared(node: impl Into<String>, dir: impl Into<PathBuf>) -> Self {
        let dir = dir.into();
        std::fs::create_dir_all(&dir).ok();
        Self {
            node: node.into(),
            dir,
            tag: std::process::id().to_string(),
            round: 0,
            seq: 0,
            backlog: 0,
            feeders: 1,
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
        (self.backlog, self.feeders) = measure(&self.dir);
        let share = self.backlog / self.feeders.max(1);

        self.escalate(share);

        let mark = if share > CEILING {
            Mark::Fail
        } else if share > self.previous {
            Mark::Warn
        } else {
            Mark::Pass
        };
        let config = format!("{} node(s) x {} readers", self.nodes, self.readers);
        let feeders = if self.feeders > 1 {
            format!(" over {} feeders, {share} each", self.feeders)
        } else {
            String::new()
        };
        let evidence = format!(
            "backlog {}{feeders} ({processed}/round, capacity {capacity}, {config}){}",
            self.backlog, self.action
        );
        self.standing.record(mark, evidence);
        self.previous = share;

        let mut snapshot = Snapshot::new();
        snapshot.record_health(self.standing.health(&format!("{}/drain", self.node), now));
        self.record_counts(&mut snapshot, now);
        snapshot
    }

    /// Drop the round's arrivals, each named for the process that fed it.
    fn arrive(&mut self) {
        for _ in 0..ARRIVAL {
            self.seq += 1;
            let path = self.dir.join(format!("daily_{}_{:08}", self.tag, self.seq));
            std::fs::write(&path, b"x").ok();
        }
    }

    /// Raise concurrency first; add a node only if the tweak was not enough.
    fn escalate(&mut self, share: usize) {
        if !self.tweaked && share > TWEAK_AT {
            self.readers = TWEAK_READERS;
            self.tweaked = true;
            self.action = " — raised concurrency (tweak)";
        } else if self.tweaked && !self.scaled && share > SCALE_AT {
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

/// Remove up to `capacity` files, returning how many were drained. A file
/// another process removed first does not count; the next one is taken.
fn drain(dir: &Path, capacity: usize) -> usize {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return 0;
    };
    let mut processed = 0;
    for entry in entries.flatten() {
        if processed >= capacity {
            break;
        }
        if std::fs::remove_file(entry.path()).is_ok() {
            processed += 1;
        }
    }
    processed
}

/// The backlog: how many files wait, and how many processes fed them.
fn measure(dir: &Path) -> (usize, usize) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return (0, 0);
    };
    let mut backlog = 0;
    let mut feeders = BTreeSet::new();
    for entry in entries.flatten() {
        backlog += 1;
        let name = entry.file_name();
        let name = name.to_str().unwrap_or_default();
        if let Some(tag) = name
            .strip_prefix("daily_")
            .and_then(|rest| rest.split('_').next())
        {
            feeders.insert(tag.to_string());
        }
    }
    (backlog, feeders.len())
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
            measure(&dir).0 > 0,
            "the backlog is real files in the directory"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn a_shared_drain_judges_its_share_and_leaves_others_files_alone_at_start() {
        let dir = scratch("shared-daily");
        std::fs::create_dir_all(&dir).expect("dir");
        for n in 0..40 {
            std::fs::write(dir.join(format!("daily_other_{n:08}")), b"x").expect("a file");
        }
        let mut daily = Daily::shared("xmip:///playground/daily", &dir);
        assert_eq!(
            measure(&dir),
            (40, 1),
            "the other feeder's files survive start"
        );

        let snapshot = daily.tick();
        let record = &snapshot.health("xmip:///playground/daily")[0];
        assert_eq!(daily.feeders, 2, "two feeders are seen");
        assert!(
            record.evidence.contains("over 2 feeders"),
            "the evidence names the feeders: {}",
            record.evidence
        );
        std::fs::remove_dir_all(&dir).ok();
    }
}
