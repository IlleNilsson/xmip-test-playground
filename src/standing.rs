//! A scope's standing over the rounds it has run, and the health it publishes.
//!
//! Three scenarios — load, secretary, claim — judge a scope the same way: each
//! round passes, warns or fails; the scope reads green while it is passing with
//! a clean history, yellow while it passes now but has failed before (or warned
//! this round), red the round it fails. This is that one judgement, so each
//! scenario keeps only what is unique to it. Pingpong keeps its own, richer,
//! Outcome-based tally in `schedule.rs` (it scales severity with the failure
//! rate); the rest share this.

use observe::{Health, HealthRecord};

/// How one round went for a scope.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Mark {
    /// Green this round.
    Pass,
    /// Yellow this round — degraded, not broken (a one-sided transport, say).
    Warn,
    /// Red this round.
    Fail,
}

/// A scope's standing over time: how many rounds failed, how the last one went,
/// and the line an operator reads.
#[derive(Clone, Debug, Default)]
pub struct Standing {
    failures: u64,
    last_pass: bool,
    last_warn: bool,
    line: String,
}

impl Standing {
    /// Fold one round's mark and evidence in.
    pub fn record(&mut self, mark: Mark, line: impl Into<String>) {
        self.last_pass = mark == Mark::Pass;
        self.last_warn = mark == Mark::Warn;
        if mark == Mark::Fail {
            self.failures += 1;
        }
        self.line = line.into();
    }

    /// The health record for this scope, judged over time.
    #[must_use]
    pub fn health(&self, scope: &str, now: i64) -> HealthRecord {
        let (health, severity) = if self.last_pass && self.failures == 0 {
            (Health::Fine, 0)
        } else if self.last_pass {
            (Health::Average, 45)
        } else if self.last_warn {
            (Health::Average, 40)
        } else {
            (Health::Done, 90)
        };

        HealthRecord {
            scope: scope.to_string(),
            health,
            severity,
            evidence: self.line.clone(),
            observed_unix_nanos: now,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_clean_pass_is_green() {
        let mut standing = Standing::default();
        standing.record(Mark::Pass, "all good");
        assert_eq!(standing.health("s", 1).health, Health::Fine);
    }

    #[test]
    fn a_pass_after_a_failure_is_yellow() {
        let mut standing = Standing::default();
        standing.record(Mark::Fail, "broke");
        standing.record(Mark::Pass, "recovered");
        assert_eq!(standing.health("s", 1).health, Health::Average);
    }

    #[test]
    fn a_failing_round_is_red_and_a_warn_is_yellow() {
        let mut standing = Standing::default();
        standing.record(Mark::Fail, "broke");
        assert_eq!(standing.health("s", 1).health, Health::Done);
        standing.record(Mark::Warn, "one-sided");
        assert_eq!(standing.health("s", 1).health, Health::Average);
    }
}
