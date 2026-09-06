//! Two time limits every roll of the playground honours (ADR-0028): a **maximum
//! time** to run, and a **factor on time**.
//!
//! The factor is against real time. Factor `1.0` mimics real time — one round
//! per real second, the rate an operator would watch. Retract the factor below
//! one and the same rounds play out in less wall-clock time; raise it above one
//! and they play out slower. The maximum is a wall-clock ceiling: when it is
//! reached the roll stops, whatever the round count.
//!
//! These are framework limits, not one scenario's: any test rolled through the
//! playground is bounded the same way.

use std::time::{Duration, Instant};

/// The two limits of a roll: an optional maximum wall-clock time, and a factor
/// on time (`1.0` is real time).
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

    /// The wait between rounds: the real-time interval scaled by the factor, so
    /// `1.0` leaves it untouched and a smaller factor shortens it.
    #[must_use]
    pub fn interval(&self, real: Duration) -> Duration {
        real.mul_f64(self.factor)
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
    fn real_time_leaves_the_interval_untouched() {
        let budget = Budget::new(None, 1.0);
        assert_eq!(
            budget.interval(Duration::from_millis(1000)),
            Duration::from_millis(1000)
        );
    }

    #[test]
    fn a_retracted_factor_shortens_the_interval() {
        let budget = Budget::new(None, 0.25);
        assert_eq!(
            budget.interval(Duration::from_millis(1000)),
            Duration::from_millis(250)
        );
    }

    #[test]
    fn a_zero_or_negative_factor_falls_back_to_real_time() {
        let budget = Budget::new(None, 0.0);
        assert_eq!(
            budget.interval(Duration::from_millis(800)),
            Duration::from_millis(800)
        );
    }

    #[test]
    fn no_maximum_never_expires() {
        let budget = Budget::new(None, 1.0);
        assert!(!budget.expired());
    }

    #[test]
    fn a_zero_maximum_is_already_expired() {
        let budget = Budget::new(Some(Duration::ZERO), 1.0);
        assert!(budget.expired());
    }
}
