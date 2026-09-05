//! The Schedule: what drives the pingpong test, and how it accumulates.
//!
//! The owner's shape, 2026-09-05: the pingpong test is an **integration test
//! over time** — Schedule, Receive and Send. A Schedule ticks; each tick runs
//! one round over every transport, for every contract, and folds the result
//! into a running tally per pair. What it publishes is not the last round but
//! the record over time: how many rounds passed, and whether the pair is
//! failing now. One failure among thousands of passes is the signal, and it
//! stays visible until a round passes again.
//!
//! The running thread is the caller's — the crate gives the tick, so a test
//! drives many rounds without waiting on a clock.

use std::collections::BTreeMap;
use std::time::{SystemTime, UNIX_EPOCH};

use xmip_observe::{Health, HealthRecord, Snapshot};

use crate::pingpong::ping_pong;
use crate::roundtrip::{FileRoundTrip, HttpRoundTrip, RoundTrip, SmtpRoundTrip, TcpRoundTrip};
use crate::verdict::{Contract, Outcome, Verdict};

/// One pair's record over time: how many rounds it has run, how many failed,
/// and the last round's outcome. This is what "over time" means — a pair is
/// judged by its history, not its latest tick.
#[derive(Clone, Debug, Default)]
pub struct Tally {
    pub rounds: u64,
    pub failures: u64,
    pub last: Option<Outcome>,
}

impl Tally {
    fn fold(&mut self, outcome: &Outcome) {
        self.rounds += 1;
        if matches!(outcome, Outcome::Failed(_)) {
            self.failures += 1;
        }
        self.last = Some(outcome.clone());
    }
}

/// Every contract the playground exercises today. Grows as the content modules
/// land; ADR-0028's matrix is every transport by every one of these.
pub const CONTRACTS: [Contract; 2] = [Contract::Bytes, Contract::Text];

/// A scheduled exercise of the estate's transports. Holds one [`RoundTrip`]
/// adapter per transport and, on each tick, runs the scenario over every
/// adapter by every contract and publishes what it found.
///
/// File, tcp, http and smtp are wired today: file ping-pongs over one directory
/// with no port to coordinate, the sockets over a real loopback connection. udp
/// joins once the transport can report the address it bound to (it binds a fresh
/// socket per receive today, so the sender cannot learn where to aim). A
/// transport not yet wired is simply absent from the tick rather than reported
/// as failing.
pub struct Schedule {
    node: String,
    transports: Vec<Box<dyn RoundTrip>>,
    tallies: BTreeMap<String, Tally>,
}

impl Schedule {
    /// A schedule publishing under `node`, running the pingpong test over every
    /// wired transport. `file_dir` is where the file transport ping-pongs.
    #[must_use]
    pub fn new(node: impl Into<String>, file_dir: impl Into<std::path::PathBuf>) -> Self {
        let transports: Vec<Box<dyn RoundTrip>> = vec![
            Box::new(FileRoundTrip::new(file_dir)),
            Box::new(TcpRoundTrip::new()),
            Box::new(HttpRoundTrip::new()),
            Box::new(SmtpRoundTrip::new()),
        ];

        Self {
            node: node.into(),
            transports,
            tallies: BTreeMap::new(),
        }
    }

    /// Run every pair once, fold each into its tally, and return the snapshot
    /// to publish. One tick; call it on a schedule.
    pub fn tick(&mut self) -> Snapshot {
        let now = now_unix_nanos();
        let mut snapshot = Snapshot::new();

        for verdict in self.run_once(now) {
            let scope = verdict.scope(&self.node);
            let tally = self.tallies.entry(scope.clone()).or_default();
            tally.fold(&verdict.outcome);
            snapshot.record_health(over_time(&scope, tally, now));
        }

        snapshot
    }

    /// The verdicts of one round — every wired transport by every contract —
    /// before they fold into the tallies. Separated so a test can read them
    /// directly.
    #[must_use]
    pub fn run_once(&self, now: i64) -> Vec<Verdict> {
        self.transports
            .iter()
            .flat_map(|transport| {
                CONTRACTS
                    .iter()
                    .map(move |&contract| ping_pong(transport.as_ref(), contract, now))
            })
            .collect()
    }

    /// The tally for one pair's scope, for a caller that wants the numbers
    /// rather than the health.
    #[must_use]
    pub fn tally(&self, scope: &str) -> Option<&Tally> {
        self.tallies.get(scope)
    }
}

/// A pair's health from its record over time. Green while the last round
/// passed, its severity rising with the failure rate so a pair that fails one
/// round in ten reads worse than one that failed once an hour ago. Red the
/// moment the last round failed, with the fault as evidence.
fn over_time(scope: &str, tally: &Tally, now: i64) -> HealthRecord {
    let passed = tally.rounds - tally.failures;

    let (health, severity, evidence) = match &tally.last {
        Some(Outcome::Delivered) if tally.failures == 0 => (
            Health::Green,
            0,
            format!("{passed}/{} rounds passed", tally.rounds),
        ),
        Some(Outcome::Delivered) => (
            // Passing now, but it has failed before — a yellow that says so,
            // deepening with how often it has failed.
            Health::Yellow,
            rate_severity(tally),
            format!(
                "{passed}/{} rounds passed, {} failed",
                tally.rounds, tally.failures
            ),
        ),
        Some(Outcome::OneSided(why)) => (Health::Yellow, 40, why.clone()),
        Some(Outcome::Failed(why)) => (
            Health::Red,
            90,
            format!(
                "{why} — {} of {} rounds have failed",
                tally.failures, tally.rounds
            ),
        ),
        None => (Health::Yellow, 40, "not yet run".to_string()),
    };

    HealthRecord {
        scope: scope.to_string(),
        health,
        severity,
        evidence,
        observed_unix_nanos: now,
    }
}

/// Severity from the failure rate, 1..=80, for a pair that is passing now but
/// has failed before. Never 0 (that is unblemished green) and never red's 90.
fn rate_severity(tally: &Tally) -> u8 {
    if tally.rounds == 0 {
        return 40;
    }

    let rate = (tally.failures * 80) / tally.rounds;
    rate.clamp(1, 80) as u8
}

fn now_unix_nanos() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |since| {
            i64::try_from(since.as_nanos()).unwrap_or(i64::MAX)
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use xmip_observe::Health;

    fn scratch(name: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("xmip-schedule-{name}"));
        std::fs::remove_dir_all(&dir).ok();
        dir
    }

    #[test]
    fn a_tick_runs_every_contract_and_reports_each() {
        let dir = scratch("tick");
        let mut schedule = Schedule::new("xmip:///playground", &dir);

        let snapshot = schedule.tick();

        let records = snapshot.health("xmip:///playground/exercise/file");
        assert_eq!(records.len(), CONTRACTS.len());
        assert!(records.iter().all(|r| r.health == Health::Green));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn the_tally_accumulates_over_ticks() {
        // Over time is the point: each tick adds to the record, and green
        // evidence counts the rounds rather than reporting only the last.
        let dir = scratch("tally");
        let mut schedule = Schedule::new("xmip:///playground", &dir);

        for _ in 0..5 {
            let _ = schedule.tick();
        }

        let tally = schedule
            .tally("xmip:///playground/exercise/file/text")
            .expect("the pair has run");
        assert_eq!(tally.rounds, 5);
        assert_eq!(tally.failures, 0);

        let snapshot = schedule.tick();
        let record = &snapshot.health("xmip:///playground/exercise/file/text")[0];
        assert!(record.evidence.contains("6/6 rounds passed"));
    }

    #[test]
    fn a_working_exercise_rolls_up_to_green() {
        let dir = scratch("rollup");
        let mut schedule = Schedule::new("xmip:///playground", &dir);

        let snapshot = schedule.tick();
        assert_eq!(snapshot.worst("xmip:///playground"), Some(Health::Green));
        std::fs::remove_dir_all(&dir).ok();
    }
}
