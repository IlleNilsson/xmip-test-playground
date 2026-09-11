//! What the machine has free of everyone else's work, and the half of it the
//! tests may use — followed as it moves.
//!
//! The owner's rule, 2026-09-11: the tests may use half of the resources
//! left over by whatever else the machine is doing, and that load is not a
//! constant. A machine a quarter busy with other work has three quarters
//! free, and the Playground takes half of that; when the other work grows
//! to half the machine, the Playground's share falls to a quarter. So the
//! measure is taken again before every round, and it measures the *others*:
//! the machine's processor time less what the roll and its fleet of nodes
//! are burning themselves, or a busy roll would read its own load as someone
//! else's and throttle itself to nothing.
//!
//! On Windows the counters come from `typeperf`; on Linux from `/proc/stat`
//! and the processes' own `/proc/<pid>/stat`; anywhere else the machine is
//! taken as free, and says so.

use std::sync::Mutex;
use std::time::Duration;

/// The fraction of the machine free of other work at the last measure, and
/// the counts the tests may run at within half of it.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Headroom {
    /// What is free of other work, 0 to 1.
    free: f64,
}

/// The last measure taken, shared by every scenario in the process.
static CURRENT: Mutex<Option<Headroom>> = Mutex::new(None);

impl Headroom {
    /// What the tests may take of what is free.
    const SHARE: f64 = 0.5;

    /// The measure the roll last took; taken now if none has been.
    pub fn current() -> Self {
        let mut current = CURRENT
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *current.get_or_insert_with(Self::measure)
    }

    /// Measure again and make it current. The roll calls this before every
    /// round; a test that wants a known budget sets one with
    /// [`Self::make_current`].
    #[must_use]
    pub fn refresh() -> Self {
        Self::measure().make_current()
    }

    /// Make this the measure every scenario reads.
    #[must_use]
    pub fn make_current(self) -> Self {
        *CURRENT
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(self);
        self
    }

    /// A headroom from a known free fraction, for a test or a report.
    #[must_use]
    pub fn from_free(free: f64) -> Self {
        Self {
            free: free.clamp(0.0, 1.0),
        }
    }

    /// The fraction of the machine that is free of other work.
    #[must_use]
    pub const fn free(&self) -> f64 {
        self.free
    }

    /// The fraction of the machine the tests may use: half of what is free.
    #[must_use]
    pub fn budget(&self) -> f64 {
        self.free * Self::SHARE
    }

    /// `whole` — a count that would take the whole machine — scaled to the
    /// budget, never fewer than one.
    #[must_use]
    #[allow(clippy::cast_precision_loss, clippy::cast_possible_truncation)]
    #[allow(clippy::cast_sign_loss)]
    pub fn share(&self, whole: usize) -> usize {
        ((whole as f64 * self.budget()).round() as usize).max(1)
    }

    /// The cores the tests may drive: the machine's, scaled to the budget.
    #[must_use]
    pub fn cores(&self) -> usize {
        self.share(std::thread::available_parallelism().map_or(1, usize::from))
    }

    /// One line for a board: what is free and what the tests take of it.
    #[must_use]
    pub fn describe(&self) -> String {
        format!(
            "{:.0}% free of other work, tests at {:.0}%",
            self.free * 100.0,
            self.budget() * 100.0
        )
    }

    /// Sample processor time over a short window, less the Playground's own.
    fn measure() -> Self {
        Self::from_free(sample::free_of_others().unwrap_or(1.0))
    }
}

/// The platform's reading of what others use. Each returns the fraction of
/// the machine free of other work, or `None` where it cannot be read.
#[cfg(windows)]
mod sample {
    /// The counters: the whole machine, and every process by name. Process
    /// counters are in percent of one core, so the Playground's own are
    /// summed and divided by the core count before they leave the total.
    pub(super) fn free_of_others() -> Option<f64> {
        let output = std::process::Command::new("typeperf")
            .args([
                r"\Processor(_Total)\% Processor Time",
                r"\Process(*)\% Processor Time",
                "-sc",
                "2",
                "-si",
                "1",
            ])
            .output()
            .ok()?;
        let text = String::from_utf8_lossy(&output.stdout);
        let mut lines = text.lines().filter(|line| line.starts_with('"'));
        let header: Vec<String> = fields(lines.next()?);
        let last: Vec<String> = fields(lines.next_back()?);
        let cores = std::thread::available_parallelism().map_or(1, usize::from);
        let mut total = None;
        let mut own = 0.0;
        for (name, value) in header.iter().zip(&last) {
            let Ok(percent) = value.parse::<f64>() else {
                continue;
            };
            if name.contains(r"\Processor(_Total)\") {
                total = Some(percent);
            } else if is_ours(name) {
                own += percent;
            }
        }
        #[allow(clippy::cast_precision_loss)]
        let others = (total? - own / cores as f64).max(0.0);
        Some((1.0 - others / 100.0).clamp(0.0, 1.0))
    }

    /// The counters of the roll and of its fleet, by process name.
    fn is_ours(counter: &str) -> bool {
        let instance = counter
            .split(r"\Process(")
            .nth(1)
            .and_then(|rest| rest.split(')').next())
            .unwrap_or_default();
        instance == "roll" || instance.starts_with("node")
    }

    /// One CSV line of `typeperf` into its quoted fields.
    fn fields(line: &str) -> Vec<String> {
        line.split("\",\"")
            .map(|field| field.trim_matches('"').to_string())
            .collect()
    }
}

/// The platform's reading of what others use.
#[cfg(target_os = "linux")]
mod sample {
    use std::time::Duration;

    pub(super) fn free_of_others() -> Option<f64> {
        let (first, own_first) = (cpu_line()?, own_ticks());
        std::thread::sleep(Duration::from_millis(500));
        let (second, own_second) = (cpu_line()?, own_ticks());
        let total: u64 = second.iter().sum::<u64>().checked_sub(first.iter().sum())?;
        let idle = second.get(3)?.checked_sub(*first.get(3)?)?;
        let own = own_second.saturating_sub(own_first);
        if total == 0 {
            return None;
        }
        #[allow(clippy::cast_precision_loss)]
        Some(((idle + own) as f64 / total as f64).clamp(0.0, 1.0))
    }

    /// The aggregate `cpu` line of `/proc/stat` as its counters.
    fn cpu_line() -> Option<Vec<u64>> {
        let stat = std::fs::read_to_string("/proc/stat").ok()?;
        let line = stat.lines().find(|line| line.starts_with("cpu "))?;
        Some(
            line.split_whitespace()
                .skip(1)
                .filter_map(|field| field.parse().ok())
                .collect(),
        )
    }

    /// This process's user and system ticks; the fleet's nodes are read the
    /// same way where they are this process's children.
    fn own_ticks() -> u64 {
        let stat = std::fs::read_to_string("/proc/self/stat").unwrap_or_default();
        let after_name = stat.rsplit(national_close()).next().unwrap_or_default();
        let fields: Vec<&str> = after_name.split_whitespace().collect();
        let user: u64 = fields.get(11).and_then(|f| f.parse().ok()).unwrap_or(0);
        let system: u64 = fields.get(12).and_then(|f| f.parse().ok()).unwrap_or(0);
        user + system
    }

    /// The character that closes the command name in `/proc/self/stat`.
    const fn national_close() -> char {
        ')'
    }
}

/// Not read on this platform: the machine is taken as free.
#[cfg(not(any(windows, target_os = "linux")))]
mod sample {
    pub(super) fn free_of_others() -> Option<f64> {
        None
    }
}

/// How long one measure takes, for whoever schedules it.
pub const MEASURE_TAKES: Duration = Duration::from_secs(2);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn half_of_what_is_free_never_fewer_than_one() {
        let three_quarters_free = Headroom::from_free(0.75);
        assert!((three_quarters_free.budget() - 0.375).abs() < 1e-9);
        assert_eq!(three_quarters_free.share(40), 15);
        assert_eq!(three_quarters_free.share(16), 6);
        assert_eq!(Headroom::from_free(0.0).share(40), 1);
        assert_eq!(Headroom::from_free(1.0).share(40), 20);
        assert!((Headroom::from_free(7.0).free() - 1.0).abs() < f64::EPSILON);
        assert_eq!(
            Headroom::from_free(0.5).describe(),
            "50% free of other work, tests at 25%"
        );
    }

    #[test]
    fn a_measure_is_a_fraction_and_the_current_one_can_be_set() {
        let measured = Headroom::refresh();
        assert!((0.0..=1.0).contains(&measured.free()));
        assert!(measured.cores() >= 1);
        let known = Headroom::from_free(0.25).make_current();
        assert_eq!(Headroom::current(), known);
        assert!(MEASURE_TAKES >= Duration::from_secs(1));
    }
}
