//! The Schedule: what drives the pingpong test, and how it accumulates.
//!
//! The owner's shape, 2026-09-05: the pingpong test is an **integration test
//! over time** across the message path — Receive, Process, Send. A Schedule
//! ticks; each tick runs one round over every transport, for every contract, and
//! expands it into a verdict per stage. Loopback itself never fails, so the
//! Schedule injects transport and content faults — transport, addressing and
//! contract — on all three stages from a [`FaultPlan`], while the identity
//! pipeline faults its own steps. What it publishes is not the last round but
//! the record over time: how many rounds passed, and whether the pair fails now.
//!
//! The running thread is the caller's — the crate gives the tick, so a test
//! drives many rounds without waiting on a clock.

use std::collections::BTreeMap;

use observe::{Activity, Count, Counted, Health, HealthRecord, Item, ItemKind, Snapshot};

use crate::fault::FaultPlan;
use crate::identity::{self, IdentityFaults};
use crate::pingpong::ping_pong;
use crate::roundtrip::{RoundTrip, all_transports};
use crate::support::now_unix_nanos;
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
    identity_faults: IdentityFaults,
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
        let transports = all_transports(file_dir);

        Self {
            node: node.into(),
            transports,
            faults: FaultPlan::none(),
            identity_faults: IdentityFaults::none(),
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
        // One switch turns on both: an empty plan runs the identity pipeline
        // clean, a realistic plan faults it too, so the runner's single
        // `with_faults(FaultPlan::realistic())` gets transport and identity
        // faults together.
        self.identity_faults = if faults.is_empty() {
            IdentityFaults::none()
        } else {
            IdentityFaults::realistic()
        };
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
            // Throughput and the activity feed count the transport verdict, not
            // the identity children: a Stream is received once, not once per
            // identity step. The identity points still fold into health, so an
            // operator drills to the step that failed.
            let is_transport = verdict.point.is_none();

            if is_transport && matches!(verdict.outcome, Outcome::Delivered) {
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

            if is_transport {
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
                        point: None,
                        observed_unix_nanos: now,
                    });
                }

                self.push_identity(&mut verdicts, name, contract, now);
            }
        }

        verdicts
    }

    /// Append the identity verdicts for one (transport, contract): the three
    /// Receive steps — Identification, Authentication, Authorization — and the
    /// Send presentation, each a child scope under its stage. ADR-0019, ADR-0033.
    fn push_identity(&self, verdicts: &mut Vec<Verdict>, name: &str, contract: Contract, now: i64) {
        for (step, outcome) in identity::receive(&self.identity_faults, name, contract, self.round)
        {
            verdicts.push(Verdict {
                stage: Stage::Receive,
                transport: name.to_string(),
                contract,
                outcome,
                bytes: 0,
                point: Some(step.name()),
                observed_unix_nanos: now,
            });
        }

        verdicts.push(Verdict {
            stage: Stage::Send,
            transport: name.to_string(),
            contract,
            outcome: identity::send(&self.identity_faults, name, contract, self.round),
            bytes: 0,
            point: Some("identity"),
            observed_unix_nanos: now,
        });
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
            Health::Fine,
            0,
            format!("{passed}/{} rounds passed", tally.rounds),
        ),
        Some(Outcome::Delivered) => (
            // Passing now, but it has failed before — a yellow that says so,
            // deepening with how often it has failed.
            Health::Stressed,
            rate_severity(tally),
            format!(
                "{passed}/{} rounds passed, {} failed",
                tally.rounds, tally.failures
            ),
        ),
        Some(Outcome::OneSided(why)) => (Health::Stressed, 40, why.clone()),
        Some(Outcome::Failed(why)) => (
            Health::Done,
            90,
            format!(
                "{why} — {} of {} rounds have failed",
                tally.failures, tally.rounds
            ),
        ),
        None => (Health::Stressed, 40, "not yet run".to_string()),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::support::scratch;
    use observe::Health;

    #[test]
    fn a_tick_reports_every_pair_across_the_three_stages() {
        let dir = scratch("tick");
        let mut schedule = Schedule::new("xmip:///playground", &dir);

        let snapshot = schedule.tick();

        // file carries no fault — transport or identity — so every record under
        // every stage of every file pair is green. Receive now carries the
        // transport verdict plus three identity steps per contract; Send carries
        // the transport verdict plus the identity presentation.
        for (stage, per_contract) in [("receive", 4), ("process", 1), ("send", 2)] {
            let records = snapshot.health(&format!("xmip:///playground/{stage}/file"));
            assert_eq!(
                records.len(),
                CONTRACTS.len() * per_contract,
                "{per_contract} record(s) per contract at {stage}"
            );
            assert!(records.iter().all(|r| r.health == Health::Fine));
        }
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn a_fault_free_schedule_rolls_up_to_green() {
        let dir = scratch("rollup");
        let mut schedule = Schedule::new("xmip:///playground", &dir);

        let snapshot = schedule.tick();
        assert_eq!(snapshot.worst("xmip:///playground"), Some(Health::Fine));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn injected_faults_turn_pairs_done_but_leave_file_fine() {
        let dir = scratch("faults");
        let mut schedule =
            Schedule::new("xmip:///playground", &dir).with_faults(FaultPlan::realistic());

        let mut snapshot = schedule.tick();
        for _ in 0..40 {
            snapshot = schedule.tick();
        }

        assert_eq!(
            snapshot.worst("xmip:///playground"),
            Some(Health::Holding),
            "faults should surface — a Done leaf rolls up to Holding (ADR-0041)"
        );
        assert_eq!(
            snapshot.worst("xmip:///playground/receive/file"),
            Some(Health::Fine),
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
