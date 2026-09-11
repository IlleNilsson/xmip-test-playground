//! The storm scenario: every transport by every contract, at a stress level,
//! all workers at once, harsh faults, the level's payload cycling by round.
//!
//! ADR-0028. Pingpong asks whether a pair delivered; the storm asks whether
//! the playground itself keeps its shape while everything is thrown at it at
//! once. It publishes a leaf per pair under `<node>/<transport>/<contract>`
//! the way pingpong does, but its subject is four invariants under stress:
//!
//!   - a tick finishes within `pairs * TIMEOUT * 3 / workers` — a round is
//!     judged, never waited on, and the workers really run at once;
//!   - every verdict that is not delivered carries a reason;
//!   - health rolls up consistently — a red leaf makes the root not Fine, and
//!     a root that is Fine has nothing but Fine beneath it;
//!   - nothing panics — a pair that does is caught, counted and published red
//!     with the panic's message, never allowed to take the tick down.
//!
//! The tick's own verdict lives at `<node>/tick`: green with how long the tick
//! took against its budget, red naming the invariant that broke. Nothing in
//! `Counted` measures a duration, so the time is the evidence line; the pairs
//! delivered are counted as Streams at the node and the bytes moved as Bytes.

use std::collections::BTreeMap;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::time::{Duration, Instant};

use observe::{Count, Counted, Health, HealthRecord, Snapshot};

use crate::fault::FaultPlan;
use crate::pingpong::ping_pong_with;
use crate::roundtrip::{RoundTrip, TIMEOUT, all_transports};
use crate::schedule::{CONTRACTS, drive_pairs};
use crate::standing::{Mark, Standing};
use crate::stress::{self, Stress};
use crate::support::now_unix_nanos;
use crate::verdict::{Contract, Outcome, Stage};

/// One pair judged this round.
struct Line {
    scope: String,
    outcome: Outcome,
    bytes: u64,
    panicked: bool,
}

/// The storm: the whole matrix at a level, driven from the level's workers,
/// judged on whether the playground kept its invariants under it.
pub struct Storm {
    node: String,
    transports: Vec<Box<dyn RoundTrip>>,
    stress: Stress,
    faults: FaultPlan,
    round: u64,
    standings: BTreeMap<String, Standing>,
    delivered: u64,
    moved_bytes: u64,
    panics: u64,
    last_tick: Duration,
    broken: Vec<String>,
}

impl Storm {
    /// A storm publishing under `node` over every wired transport, at
    /// `Realistic` until [`Storm::at`] says otherwise. `file_dir` is where
    /// the file transport ping-pongs.
    #[must_use]
    pub fn new(node: impl Into<String>, file_dir: impl Into<std::path::PathBuf>) -> Self {
        Self {
            node: node.into(),
            transports: all_transports(file_dir),
            stress: Stress::Realistic,
            faults: FaultPlan::realistic().at(Stress::Realistic),
            round: 0,
            standings: BTreeMap::new(),
            delivered: 0,
            moved_bytes: 0,
            panics: 0,
            last_tick: Duration::ZERO,
            broken: Vec::new(),
        }
    }

    /// The same storm at a level: its faults, its payload sizes, its workers.
    #[must_use]
    pub fn at(mut self, stress: Stress) -> Self {
        self.stress = stress;
        self.faults = if stress == Stress::Calm {
            FaultPlan::none()
        } else {
            FaultPlan::realistic().at(stress)
        };
        self
    }

    /// Drive these transports rather than every one.
    #[must_use]
    pub fn over(mut self, transports: Vec<Box<dyn RoundTrip>>) -> Self {
        self.transports = transports;
        self
    }

    /// How long a tick may take: every pair waited on to its timeout, three
    /// times over, shared across the workers. Longer means a round hung past
    /// its adapter's timeout or the workers did not run at once.
    #[must_use]
    pub fn budget(&self) -> Duration {
        let pairs = u32::try_from(self.transports.len() * CONTRACTS.len()).unwrap_or(u32::MAX);
        let workers = u32::try_from(self.stress.workers().max(1)).unwrap_or(u32::MAX);
        TIMEOUT * 3 * pairs / workers
    }

    /// How long the last tick took.
    #[must_use]
    pub const fn last_tick(&self) -> Duration {
        self.last_tick
    }

    /// How many pairs have panicked so far, over every tick.
    #[must_use]
    pub const fn panics(&self) -> u64 {
        self.panics
    }

    /// The invariants the last tick broke, one line each; empty when it
    /// kept them all.
    #[must_use]
    pub fn broken(&self) -> &[String] {
        &self.broken
    }

    /// One tick: every pair at once from the level's workers with the
    /// level's payload for the round, each judged and folded into its
    /// standing; then the invariants over what was published, and the
    /// tick's own verdict.
    pub fn tick(&mut self) -> Snapshot {
        self.round += 1;
        let now = now_unix_nanos();
        let size = self.stress.size_for(self.round);

        let started = Instant::now();
        let lines = drive_pairs(
            &self.transports,
            self.stress.workers(),
            |transport, contract| self.judge(transport, contract, size),
        );
        self.last_tick = started.elapsed();

        let mut snapshot = Snapshot::new();
        let mut broken = Vec::new();
        let mut panicked = 0u64;
        for line in lines {
            panicked += u64::from(line.panicked);
            let (mark, evidence) = match &line.outcome {
                Outcome::Delivered => {
                    self.delivered += 1;
                    self.moved_bytes += line.bytes;
                    (Mark::Pass, format!("{} bytes out and back", line.bytes))
                }
                Outcome::OneSided(why) => (Mark::Warn, why.clone()),
                Outcome::Failed(why) => (Mark::Fail, why.clone()),
            };
            if mark != Mark::Pass && evidence.trim().is_empty() {
                broken.push(format!(
                    "{} was not delivered and gave no reason",
                    line.scope
                ));
            }
            let standing = self.standings.entry(line.scope.clone()).or_default();
            standing.record(mark, evidence);
            snapshot.record_health(standing.health(&line.scope, now));
        }
        self.panics += panicked;

        broken.extend(violations(&snapshot, &self.node));
        if self.last_tick > self.budget() {
            broken.push(format!(
                "the tick took {:?}, over its budget of {:?}",
                self.last_tick,
                self.budget()
            ));
        }
        if panicked > 0 {
            broken.push(format!("{panicked} pair(s) panicked this round"));
        }

        snapshot.record_health(self.tick_record(&broken, now));
        self.record_counts(&mut snapshot, now);
        self.broken = broken;
        snapshot
    }

    /// One pair this round: the real exchange with the level's payload,
    /// caught if it panics, then the round's fault over it if one fires on
    /// any stage — the storm has no stages, so a fault anywhere fails the
    /// pair with the fault's own line.
    fn judge(&self, transport: &dyn RoundTrip, contract: Contract, size: usize) -> Line {
        let name = transport.transport();
        let scope = format!("{}/{name}/{}", self.node, contract.name());
        let payload = stress::payload(contract, size);

        let (outcome, bytes, panicked) = match catch_unwind(AssertUnwindSafe(|| {
            ping_pong_with(transport, contract, &payload)
        })) {
            Ok((outcome, bytes)) => (outcome, bytes, false),
            Err(panic) => (
                Outcome::Failed(format!("panicked: {}", panic_message(panic.as_ref()))),
                0,
                true,
            ),
        };

        let faulted = Stage::ALL
            .iter()
            .find_map(|&stage| self.faults.fault_for(stage, name, contract, self.round));
        match faulted {
            Some(fault) if !panicked => Line {
                scope,
                outcome: Outcome::Failed(fault.evidence()),
                bytes: 0,
                panicked,
            },
            _ => Line {
                scope,
                outcome,
                bytes,
                panicked,
            },
        }
    }

    /// The tick's own leaf: green with the timing, red naming what broke.
    fn tick_record(&self, broken: &[String], now: i64) -> HealthRecord {
        let (pairs, workers) = (
            self.transports.len() * CONTRACTS.len(),
            self.stress.workers(),
        );
        let timing = format!(
            "{pairs} pairs from {workers} workers in {:?} (budget {:?}) at {}",
            self.last_tick,
            self.budget(),
            self.stress.name()
        );
        let (health, severity, evidence) = if broken.is_empty() {
            (Health::Fine, 0, timing)
        } else {
            (Health::Done, 90, format!("{timing}: {}", broken.join("; ")))
        };
        HealthRecord {
            scope: format!("{}/tick", self.node),
            health,
            severity,
            evidence,
            observed_unix_nanos: now,
        }
    }

    /// The pairs delivered so far as Streams, and the bytes that moved.
    fn record_counts(&self, snapshot: &mut Snapshot, now: i64) {
        for (counted, value) in [
            (Counted::Streams, self.delivered),
            (Counted::Bytes, self.moved_bytes),
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

/// The invariants any snapshot under stress must keep, as the lines that
/// broke them: every record beneath `node` that is not Fine carries a
/// reason, and the rollup at `node` is Fine exactly when everything beneath
/// it is. Shared by every scenario's stress test, so a rollup that lies is
/// caught wherever it lies.
#[must_use]
pub fn violations(snapshot: &Snapshot, node: &str) -> Vec<String> {
    let records = snapshot.health(node);
    let mut found: Vec<String> = records
        .iter()
        .filter(|record| record.health != Health::Fine && record.evidence.trim().is_empty())
        .map(|record| format!("{} is {:?} with no reason", record.scope, record.health))
        .collect();

    let trouble = records.iter().any(|record| record.health != Health::Fine);
    match snapshot.worst(node) {
        Some(Health::Fine) if trouble => {
            found.push(format!("{node} rolls up Fine over a leaf that is not"));
        }
        Some(health) if health != Health::Fine && !trouble => {
            found.push(format!("{node} rolls up {health:?} over nothing but Fine"));
        }
        None if !records.is_empty() => {
            found.push(format!(
                "{node} rolls up to nothing over {} records",
                records.len()
            ));
        }
        _ => {}
    }
    found
}

/// The message a panic carried, when it was a string.
fn panic_message(panic: &(dyn std::any::Any + Send)) -> String {
    panic
        .downcast_ref::<String>()
        .cloned()
        .or_else(|| panic.downcast_ref::<&str>().map(ToString::to_string))
        .unwrap_or_else(|| "no message".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::roundtrip::{Exchange, FileRoundTrip, TcpRoundTrip, UdpRoundTrip};
    use crate::support::scratch;

    const NODE: &str = "xmip:///playground/storm";

    fn sample(dir: &std::path::Path) -> Vec<Box<dyn RoundTrip>> {
        vec![
            Box::new(FileRoundTrip::new(dir)),
            Box::new(TcpRoundTrip),
            Box::new(UdpRoundTrip),
        ]
    }

    /// Drive `storm` for `rounds`, asserting every invariant every round, and
    /// return the last snapshot.
    fn weather(storm: &mut Storm, rounds: u64) -> Snapshot {
        let mut snapshot = Snapshot::new();
        for round in 1..=rounds {
            snapshot = storm.tick();
            assert!(
                storm.broken().is_empty(),
                "round {round} broke: {}",
                storm.broken().join("; ")
            );
            assert!(storm.last_tick() <= storm.budget());
            assert_eq!(storm.panics(), 0, "nothing panics");
            let lying = violations(&snapshot, NODE);
            assert!(lying.is_empty(), "round {round}: {}", lying.join("; "));
            assert_eq!(
                snapshot.worst(&format!("{NODE}/tick")),
                Some(Health::Fine),
                "the tick leaf is green when every invariant held"
            );
        }
        snapshot
    }

    #[test]
    fn a_calm_storm_over_file_is_green_and_times_its_tick() {
        let dir = scratch("storm-calm");
        let mut storm = Storm::new(NODE, &dir)
            .at(Stress::Calm)
            .over(vec![Box::new(FileRoundTrip::new(&dir))]);
        let snapshot = weather(&mut storm, 2);
        assert_eq!(snapshot.worst(NODE), Some(Health::Fine));
        let tick = snapshot.health(&format!("{NODE}/tick"));
        assert!(tick[0].evidence.contains("budget"), "{}", tick[0].evidence);
        assert_eq!(
            snapshot.measure(NODE, Counted::Streams).map(|c| c.value),
            Some(2 * CONTRACTS.len() as u64),
            "every pair delivered, both rounds"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn the_budget_is_pairs_by_three_timeouts_over_the_workers() {
        let dir = scratch("storm-budget");
        let storm = Storm::new(NODE, &dir).at(Stress::Harsh).over(sample(&dir));
        let pairs = u32::try_from(3 * CONTRACTS.len()).expect("small");
        let workers = u32::try_from(Stress::Harsh.workers()).expect("small");
        assert_eq!(storm.budget(), TIMEOUT * 3 * pairs / workers);
    }

    /// An adapter that panics on every exchange: the fourth invariant's
    /// proof that a pair which panics is caught and named, not fatal.
    struct Explodes;

    impl RoundTrip for Explodes {
        fn transport(&self) -> &'static str {
            "explodes"
        }
        fn exchange(&self, _: &[u8]) -> Exchange {
            panic!("the adapter blew up")
        }
    }

    #[test]
    fn a_panicking_pair_is_caught_counted_and_published_red() {
        let dir = scratch("storm-panic");
        let mut storm = Storm::new(NODE, &dir)
            .at(Stress::Calm)
            .over(vec![Box::new(FileRoundTrip::new(&dir)), Box::new(Explodes)]);
        let snapshot = storm.tick();
        assert_eq!(storm.panics(), CONTRACTS.len() as u64);
        assert!(
            storm.broken().iter().any(|line| line.contains("panicked")),
            "{:?}",
            storm.broken()
        );
        let leaf = snapshot.health(&format!("{NODE}/explodes/json"));
        assert_eq!(leaf[0].health, Health::Done);
        assert!(leaf[0].evidence.contains("the adapter blew up"));
        assert_eq!(snapshot.worst(NODE), Some(Health::Holding));
        assert!(
            violations(&snapshot, NODE).is_empty(),
            "the rollup still tells the truth"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn harsh_storm_over_three_transports_keeps_every_invariant() {
        let dir = scratch("storm-harsh");
        let mut storm = Storm::new(NODE, &dir).at(Stress::Harsh).over(sample(&dir));
        let snapshot = weather(&mut storm, Stress::Harsh.rounds());
        // Harsh faults and a datagram refused above its ceiling both surface.
        assert_eq!(snapshot.worst(NODE), Some(Health::Holding));
        let udp = snapshot.health(&format!("{NODE}/udp"));
        assert!(
            udp.iter()
                .any(|r| r.health == Health::Done || r.health == Health::Stressed),
            "udp refuses sixteen bits plus one"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    #[ignore = "brutal: the whole matrix at every core, for the runner"]
    fn brutal_storm_over_every_transport() {
        let dir = scratch("storm-brutal");
        let mut storm = Storm::new(NODE, &dir).at(Stress::Brutal);
        weather(&mut storm, Stress::Brutal.rounds());
        std::fs::remove_dir_all(&dir).ok();
    }
}
