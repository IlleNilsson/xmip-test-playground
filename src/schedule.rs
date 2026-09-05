//! The Schedule: what drives the pingpong test, and how it accumulates.
//!
//! The owner's shape, 2026-09-05: the pingpong test is an **integration test
//! over time** across the message path — Receive, Process, Send. A Schedule
//! ticks; each tick runs one round over every transport, for every contract, and
//! expands it into a verdict per stage. Loopback itself never fails, so the
//! Schedule injects the faults a real integration suffers — transport,
//! addressing, authentication and contract, on all three stages — from a
//! [`FaultPlan`]. What it publishes is not the last round but the record over
//! time: how many rounds passed, and whether the pair is failing now.
//!
//! The running thread is the caller's — the crate gives the tick, so a test
//! drives many rounds without waiting on a clock.

use std::collections::BTreeMap;
use std::time::{SystemTime, UNIX_EPOCH};

use xmip_observe::{Activity, Count, Counted, Health, HealthRecord, Item, ItemKind, Snapshot};

use crate::fault::FaultPlan;
use crate::pingpong::ping_pong;
use crate::roundtrip::{
    FileRoundTrip, HttpRoundTrip, RoundTrip, SmtpRoundTrip, TcpRoundTrip, UdpRoundTrip,
    WebSocketRoundTrip,
};
use crate::verdict::{Contract, Outcome, Stage, Verdict};

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
pub const CONTRACTS: [Contract; 5] = [
    Contract::Bytes,
    Contract::Text,
    Contract::Json,
    Contract::Xml,
    Contract::Html,
];

/// A scheduled exercise of the estate's transports over the message path. Holds
/// one [`RoundTrip`] adapter per transport and a [`FaultPlan`], and on each tick
/// runs the scenario over every adapter by every contract, expands it across
/// Receive, Process and Send, and publishes what it found.
pub struct Schedule {
    node: String,
    transports: Vec<Box<dyn RoundTrip>>,
    faults: FaultPlan,
    round: u64,
    tallies: BTreeMap<String, Tally>,
    activity: Activity,
    item_seq: u64,
    streams: u64,
    messages: u64,
    journeys: u64,
    moved_bytes: u64,
}

impl Schedule {
    /// A schedule publishing under `node`, running the pingpong test over every
    /// wired transport with no injected faults. `file_dir` is where the file
    /// transport ping-pongs.
    #[must_use]
    pub fn new(node: impl Into<String>, file_dir: impl Into<std::path::PathBuf>) -> Self {
        let transports: Vec<Box<dyn RoundTrip>> = vec![
            Box::new(FileRoundTrip::new(file_dir)),
            Box::new(TcpRoundTrip::new()),
            Box::new(HttpRoundTrip::new()),
            Box::new(SmtpRoundTrip::new()),
            Box::new(UdpRoundTrip::new()),
            Box::new(WebSocketRoundTrip::new()),
        ];

        Self {
            node: node.into(),
            transports,
            faults: FaultPlan::none(),
            round: 0,
            tallies: BTreeMap::new(),
            activity: Activity::with_capacity(2048),
            item_seq: 0,
            streams: 0,
            messages: 0,
            journeys: 0,
            moved_bytes: 0,
        }
    }

    /// The same schedule, injecting `faults`. The runner uses
    /// [`FaultPlan::realistic`]; the tests use the fault-free default.
    #[must_use]
    pub fn with_faults(mut self, faults: FaultPlan) -> Self {
        self.faults = faults;
        self
    }

    /// Run every pair once, expand across the stages, fold each into its tally,
    /// and return the snapshot to publish. One tick; call it on a schedule.
    pub fn tick(&mut self) -> Snapshot {
        self.round += 1;
        let now = now_unix_nanos();
        let mut snapshot = Snapshot::new();

        for verdict in self.run_once(now) {
            if matches!(verdict.outcome, Outcome::Delivered) {
                match verdict.stage {
                    Stage::Receive => self.streams += 1,
                    Stage::Process => self.journeys += 1,
                    Stage::Send => {
                        self.messages += 1;
                        self.moved_bytes += verdict.bytes;
                    }
                }
            }

            let scope = verdict.scope(&self.node);
            let tally = self.tallies.entry(scope.clone()).or_default();
            tally.fold(&verdict.outcome);
            snapshot.record_health(over_time(&scope, tally, now));

            self.item_seq += 1;
            self.activity.record(Item {
                kind: item_kind(verdict.stage),
                scope,
                id: format!("{:08}", self.item_seq),
                bytes: verdict.bytes,
                detail: detail(&verdict.outcome),
                observed_unix_nanos: now,
            });
        }

        self.record_throughput(&mut snapshot, now);

        snapshot
    }

    /// The recent individual items — the Streams, Messages and Journeys of the
    /// last rounds — for the surface that lists what actually flowed. ADR-0032.
    #[must_use]
    pub fn activity(&self) -> &Activity {
        &self.activity
    }

    /// Publish the cumulative throughput at the node scope: Streams in at
    /// Receive, Journeys through Process, Messages out at Send, and the Bytes
    /// that moved. These are what the operator's stage cards count.
    fn record_throughput(&self, snapshot: &mut Snapshot, now: i64) {
        for (counted, value) in [
            (Counted::Streams, self.streams),
            (Counted::Journeys, self.journeys),
            (Counted::Messages, self.messages),
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

    /// The verdicts of one round: every transport by every contract, each
    /// expanded across Receive, Process and Send with its faults injected.
    #[must_use]
    pub fn run_once(&self, now: i64) -> Vec<Verdict> {
        let mut verdicts = Vec::new();

        for transport in &self.transports {
            let name = transport.transport();
            for &contract in &CONTRACTS {
                let (base, bytes) = ping_pong(transport.as_ref(), contract);

                for stage in Stage::ALL {
                    let (outcome, stage_bytes) =
                        match self.faults.fault_for(stage, name, contract, self.round) {
                            Some(fault) => (Outcome::Failed(fault.evidence()), 0),
                            None => stage_outcome(stage, &base, bytes),
                        };

                    verdicts.push(Verdict {
                        stage,
                        transport: name.to_string(),
                        contract,
                        outcome,
                        bytes: stage_bytes,
                        observed_unix_nanos: now,
                    });
                }
            }
        }

        verdicts
    }

    /// The tally for one pair's scope, for a caller that wants the numbers
    /// rather than the health.
    #[must_use]
    pub fn tally(&self, scope: &str) -> Option<&Tally> {
        self.tallies.get(scope)
    }
}

/// Which kind of item a stage produces: Receive a Stream in, Process a Journey
/// through, Send a Message out.
const fn item_kind(stage: Stage) -> ItemKind {
    match stage {
        Stage::Receive => ItemKind::Stream,
        Stage::Process => ItemKind::Journey,
        Stage::Send => ItemKind::Message,
    }
}

/// The item's detail line: what became of it.
fn detail(outcome: &Outcome) -> String {
    match outcome {
        Outcome::Delivered => "delivered".to_string(),
        Outcome::OneSided(why) | Outcome::Failed(why) => why.clone(),
    }
}

/// One stage's outcome from the base result of the real exchange. Loopback
/// almost always delivers, so this is mostly Delivered; a genuine failure is
/// attributed to the stage it belongs to — a contract failure to Process, a
/// transport failure to Receive. Bytes are counted once, at Send.
fn stage_outcome(stage: Stage, base: &Outcome, bytes: u64) -> (Outcome, u64) {
    match base {
        Outcome::Delivered => match stage {
            Stage::Send => (Outcome::Delivered, bytes),
            _ => (Outcome::Delivered, 0),
        },
        Outcome::OneSided(why) => (Outcome::OneSided(why.clone()), 0),
        Outcome::Failed(why) => {
            let owns = if why.starts_with("contract not held") {
                stage == Stage::Process
            } else {
                stage == Stage::Receive
            };
            if owns {
                (Outcome::Failed(why.clone()), 0)
            } else if stage == Stage::Send {
                (Outcome::Delivered, bytes)
            } else {
                (Outcome::Delivered, 0)
            }
        }
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
    fn a_tick_reports_every_pair_across_the_three_stages() {
        let dir = scratch("tick");
        let mut schedule = Schedule::new("xmip:///playground", &dir);

        let snapshot = schedule.tick();

        // file carries no fault, so every stage of every file pair is green.
        for stage in ["receive", "process", "send"] {
            let records = snapshot.health(&format!("xmip:///playground/{stage}/file"));
            assert_eq!(
                records.len(),
                CONTRACTS.len(),
                "one per contract at {stage}"
            );
            assert!(records.iter().all(|r| r.health == Health::Green));
        }
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn a_fault_free_schedule_rolls_up_to_green() {
        let dir = scratch("rollup");
        let mut schedule = Schedule::new("xmip:///playground", &dir);

        let snapshot = schedule.tick();
        assert_eq!(snapshot.worst("xmip:///playground"), Some(Health::Green));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn injected_faults_turn_pairs_red_but_leave_file_green() {
        let dir = scratch("faults");
        let mut schedule =
            Schedule::new("xmip:///playground", &dir).with_faults(FaultPlan::realistic());

        let mut snapshot = schedule.tick();
        for _ in 0..40 {
            snapshot = schedule.tick();
        }

        assert_eq!(
            snapshot.worst("xmip:///playground"),
            Some(Health::Red),
            "faults should surface"
        );
        assert_eq!(
            snapshot.worst("xmip:///playground/receive/file"),
            Some(Health::Green),
            "file is left alone"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn throughput_counts_per_stage() {
        let dir = scratch("throughput");
        let mut schedule = Schedule::new("xmip:///playground", &dir);

        let snapshot = schedule.tick();
        let pairs = (CONTRACTS.len() * 6) as u64; // six transports

        assert_eq!(
            snapshot
                .measure("xmip:///playground", Counted::Streams)
                .map(|c| c.value),
            Some(pairs),
            "one Stream in per pair"
        );
        assert_eq!(
            snapshot
                .measure("xmip:///playground", Counted::Journeys)
                .map(|c| c.value),
            Some(pairs),
            "one Journey per pair"
        );
        std::fs::remove_dir_all(&dir).ok();
    }
}
