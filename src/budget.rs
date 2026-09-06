//! Two time limits every roll of the playground honours (ADR-0028): a **maximum
//! time** to run, and a **factor on time**.
//!
//! The maximum is a wall-clock ceiling: when it is reached the roll stops,
//! whatever the round count. The factor is against real time and governs a
//! **simulated clock**: factor `1.0` mimics real time — one simulated second per
//! real second — and *retracting* it below one runs simulated time faster than
//! real, so a long horizon plays out in a short run. Fifteen real minutes over
//! three simulated years is `max = 15min`, `factor = 15min / 3yr ≈ 9.5e-6`.
//!
//! Round cadence is separate and stays real: the roll ticks on its own interval
//! regardless of the factor. The factor stretches *simulated* time, not the wait
//! between rounds.

use std::time::{Duration, Instant};

/// The two limits of a roll: an optional maximum wall-clock time, and a factor
/// on time (`1.0` is real time; smaller runs simulated time faster).
pub struct Budget {
    max: Option<Duration>,
    factor: f64,
    started: Instant,
}

impl Budget {
    /// A budget with `max` as the wall-clock ceiling (`None` runs until stopped
    /// otherwise) and `factor` on time. A factor of zero or less is meaningless
    /// and falls back to real time.
    #[must_use]
    pub fn new(max: Option<Duration>, factor: f64) -> Self {
        Self {
            max,
            factor: if factor > 0.0 { factor } else { 1.0 },
            started: Instant::now(),
        }
    }

    /// Simulated time for a given real elapsed: real divided by the factor, so
    /// `1.0` leaves it untouched and a smaller factor stretches it. Saturates at
    /// `Duration::MAX` rather than panicking on an extreme factor.
    #[must_use]
    pub fn simulated(&self, real: Duration) -> Duration {
        let seconds = real.as_secs_f64() / self.factor;
        Duration::try_from_secs_f64(seconds).unwrap_or(Duration::MAX)
    }

    /// Simulated time elapsed since the budget began.
    #[must_use]
    pub fn simulated_elapsed(&self) -> Duration {
        self.simulated(self.started.elapsed())
    }

    /// Whether the maximum wall-clock time has been reached — the signal to stop
    /// the roll.
    #[must_use]
    pub fn expired(&self) -> bool {
        self.max.is_some_and(|max| self.started.elapsed() >= max)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn real_time_leaves_simulated_untouched() {
        let budget = Budget::new(None, 1.0);
        assert_eq!(
            budget.simulated(Duration::from_secs(10)),
            Duration::from_secs(10)
        );
    }

    #[test]
    fn a_retracted_factor_runs_simulated_time_faster() {
        let budget = Budget::new(None, 0.5);
        assert_eq!(
            budget.simulated(Duration::from_secs(10)),
            Duration::from_secs(20)
        );
    }

    #[test]
    fn three_years_fit_in_fifteen_minutes_at_the_matching_factor() {
        let three_years: u64 = 3 * 365 * 24 * 60 * 60;
        let factor = 900.0 / 94_608_000.0_f64; // 900 real seconds / three years
        let budget = Budget::new(None, factor);
        let simulated = budget.simulated(Duration::from_secs(900)).as_secs();
        // Within a day of three years, allowing for f64 rounding.
        assert!(simulated.abs_diff(three_years) < 86_400);
    }

    #[test]
    fn a_zero_or_negative_factor_falls_back_to_real_time() {
        let budget = Budget::new(None, 0.0);
        assert_eq!(
            budget.simulated(Duration::from_secs(8)),
            Duration::from_secs(8)
        );
    }

    #[test]
    fn no_maximum_never_expires() {
        assert!(!Budget::new(None, 1.0).expired());
    }

    #[test]
    fn a_zero_maximum_is_already_expired() {
        assert!(Budget::new(Some(Duration::ZERO), 1.0).expired());
    }
}
