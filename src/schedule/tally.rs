//! A pair's record over the rounds it has run, and the health it publishes.
//!
//! The pingpong test is judged over time (ADR-0028 clause 3): not the last
//! round but the record — how many rounds passed, whether the pair fails now.
//! This is that record and the one way it becomes a health record, kept
//! beside the schedule that folds into it. The severity scales with the
//! failure rate, which is why pingpong keeps this rather than the shared
//! [`Standing`](crate::standing::Standing).

use observe::{Health, HealthRecord};

use crate::verdict::Outcome;

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
    pub(crate) fn fold(&mut self, outcome: &Outcome) {
        self.rounds += 1;
        if matches!(outcome, Outcome::Failed(_)) {
            self.failures += 1;
        }
        self.last = Some(outcome.clone());
    }
}

/// A pair's health from its record over time. Green while the last round
/// passed, its severity rising with the failure rate so a pair that fails one
/// round in ten reads worse than one that failed once an hour ago. Red the
/// moment the last round failed, with the fault as evidence.
pub(crate) fn over_time(scope: &str, tally: &Tally, now: i64) -> HealthRecord {
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
