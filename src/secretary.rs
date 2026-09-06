//! The secretary scenario: retention and archiving, done methodically over a
//! **simulated clock** so a long records horizon plays out in a short run.
//!
//! ADR-0028; ADR-0013 and the observability model own retention; ADR-0040 sets
//! the boundary. Where pingpong watches a message cross the wire, the secretary
//! watches it age: an item is **retained** while it is young, and **archived**
//! once it passes its retention window. There is no third act — **Xmip retains
//! and archives, it does not delete** (ADR-0040). Once archived, what becomes of
//! the archive is the archive owner's decision, not Xmip's.
//!
//! Time is simulated. Each `tick` is handed how much simulated time has elapsed
//! (the roll's `Budget` factor, ADR-0028: factor 1.0 is real time, retracted it
//! runs faster), so fifteen real minutes can carry three simulated years. Items
//! are created at a steady **one per content class per simulated day**, and aged
//! against a realistic window — retained 90 days, then archived — so an operator
//! watches records born, live out their retention, and cross into the archive.
//!
//! Two stages, per content class: **retain** and **archive**. Green while every
//! item is in the bucket its age dictates; **red** on a leak — an item past its
//! retention window still live (retain), or an archive the store refused
//! (archive). Under pressure the secretary misses a sweep now and then,
//! deterministically, so the leaks it is meant to catch actually occur.

use std::collections::BTreeMap;
use std::time::Duration;

use archive::{ArchiveError, ArchiveItem, ArchiveReceipt, ArchiveStore};
use observe::{HealthRecord, Snapshot};
use retain::{RetentionAction, RetentionPolicy};

use crate::fault::fires_keyed;
use crate::schedule::CONTRACTS;
use crate::standing::{Mark, Standing};
use crate::support::now_unix_nanos;
use crate::verdict::Contract;

/// Seconds in a day — the unit the simulated clock is quantised to for creation.
const SECONDS_PER_DAY: u64 = 86_400;

/// How long an item is retained before it is archived, in **days** of simulated
/// time. A realistic records lifecycle: live for a quarter, then archived.
const KEEP_DAYS: u64 = 90;

/// A ceiling on how many simulated days one tick may create at once, so a large
/// jump in simulated time cannot burst the working set in a single round.
const MAX_DAYS_PER_TICK: u64 = 60;

/// The retention window as a real [`RetentionPolicy`]: keep while young, then
/// archive. Never delete (ADR-0040). The one the secretary consults and the
/// verdict checks.
struct Windows;

impl RetentionPolicy for Windows {
    fn action_for(&self, _data_type: &str, age: Duration) -> RetentionAction {
        if age.as_secs() / SECONDS_PER_DAY < KEEP_DAYS {
            RetentionAction::Keep
        } else {
            RetentionAction::Archive
        }
    }
}

/// An in-memory archive store: it keeps what it is given, and refuses an item
/// whose identifier is marked poisoned — how the scenario exercises the store's
/// error path without a real backend failing.
#[derive(Default)]
struct MemoryArchive {
    held: std::sync::Mutex<Vec<ArchiveItem>>,
}

impl ArchiveStore for MemoryArchive {
    fn archive(&self, item: ArchiveItem) -> Result<ArchiveReceipt, ArchiveError> {
        if item.identifier.starts_with("poison-") {
            return Err(ArchiveError {
                message: format!("the archive store rejected {}", item.identifier),
            });
        }
        let location = format!("mem://{}/{}", item.data_type, item.identifier);
        if let Ok(mut held) = self.held.lock() {
            held.push(item);
        }
        Ok(ArchiveReceipt {
            location,
            checksum: None,
        })
    }

    fn restore(&self, receipt: &ArchiveReceipt) -> Result<ArchiveItem, ArchiveError> {
        Err(ArchiveError {
            message: format!("restore is not exercised: {}", receipt.location),
        })
    }
}

/// One item the secretary is looking after, stamped with the simulated second it
/// was created so its age is simulated, not real.
struct Item {
    contract: Contract,
    created_secs: u64,
    id: u64,
}

/// A stage of the retention lifecycle. Two, and only two — Xmip does not delete
/// (ADR-0040).
#[derive(Clone, Copy)]
enum Sweep {
    Retain,
    Archive,
}

impl Sweep {
    const ALL: [Sweep; 2] = [Sweep::Retain, Sweep::Archive];
    fn name(self) -> &'static str {
        match self {
            Sweep::Retain => "retain",
            Sweep::Archive => "archive",
        }
    }
}

/// The secretary: it creates an item per class each simulated day, ages the lot
/// against the window, and sweeps them from retained to archived, driving the
/// real policy and store. Archived items accumulate — Xmip hands them off and
/// never deletes them.
pub struct Secretary {
    node: String,
    policy: Windows,
    store: MemoryArchive,
    live: Vec<Item>,
    archived: Vec<Item>,
    round: u64,
    emitted_days: u64,
    next_id: u64,
    under_pressure: bool,
    standings: BTreeMap<String, Standing>,
}

impl Secretary {
    /// A secretary publishing under `node`, missing no sweeps.
    #[must_use]
    pub fn new(node: impl Into<String>) -> Self {
        Self {
            node: node.into(),
            policy: Windows,
            store: MemoryArchive::default(),
            live: Vec::new(),
            archived: Vec::new(),
            round: 0,
            emitted_days: 0,
            next_id: 0,
            under_pressure: false,
            standings: BTreeMap::new(),
        }
    }

    /// The same secretary, occasionally missing a sweep so leaks occur.
    #[must_use]
    pub fn under_pressure(mut self) -> Self {
        self.under_pressure = true;
        self
    }

    /// One round at simulated time `simulated`: create an item per class for each
    /// simulated day newly reached, age everything, sweep it, and publish a
    /// verdict per (stage, class).
    pub fn tick(&mut self, simulated: Duration) -> Snapshot {
        self.round += 1;
        let now = now_unix_nanos();
        let now_secs = simulated.as_secs();

        self.create_through(now_secs);

        let mut leaks = Leaks::default();
        self.sweep_live(now_secs, &mut leaks);

        let mut snapshot = Snapshot::new();
        for sweep in Sweep::ALL {
            for &contract in &CONTRACTS {
                let scope = format!("{}/{}/{}", self.node, sweep.name(), contract.name());
                let record = self.verdict(&scope, sweep, contract, &leaks, now);
                snapshot.record_health(record);
            }
        }
        snapshot
    }

    /// Create one item per class for every simulated day newly reached, bounded
    /// so a large jump cannot burst the working set in one round.
    fn create_through(&mut self, now_secs: u64) {
        let today = now_secs / SECONDS_PER_DAY;
        let target = today.min(self.emitted_days + MAX_DAYS_PER_TICK);
        while self.emitted_days < target {
            self.emitted_days += 1;
            let created_secs = self.emitted_days * SECONDS_PER_DAY;
            for &contract in &CONTRACTS {
                self.live.push(Item {
                    contract,
                    created_secs,
                    id: self.next_id,
                });
                self.next_id += 1;
            }
        }
    }

    fn age_of(now_secs: u64, item: &Item) -> Duration {
        Duration::from_secs(now_secs.saturating_sub(item.created_secs))
    }

    /// Sweep live items: retain the young, archive the aged. Nothing is deleted.
    fn sweep_live(&mut self, now_secs: u64, leaks: &mut Leaks) {
        let mut kept = Vec::new();
        for item in std::mem::take(&mut self.live) {
            let age = Self::age_of(now_secs, &item);
            match self.policy.action_for(item.contract.name(), age) {
                RetentionAction::Keep => kept.push(item),
                RetentionAction::Archive => self.try_archive(item, leaks, &mut kept),
            }
        }
        self.live = kept;
    }

    /// Archive one aged item, or record the leak: a missed sweep leaves it live
    /// (retain leak); a store refusal leaves it live (archive fault).
    fn try_archive(&mut self, item: Item, leaks: &mut Leaks, kept: &mut Vec<Item>) {
        let name = item.contract.name();
        if self.miss("archive-skip", item.contract) {
            leaks.retain.insert(name);
            kept.push(item);
            return;
        }

        let poisoned = self.miss("archive-fault", item.contract);
        let identifier = if poisoned {
            format!("poison-{name}#{}", item.id)
        } else {
            format!("{name}#{}", item.id)
        };
        let archive_item = ArchiveItem {
            data_type: name.to_string(),
            identifier,
            bytes: Vec::new(),
            metadata: Vec::new(),
        };

        if self.store.archive(archive_item).is_err() {
            leaks.archive.insert(name);
            kept.push(item);
        } else {
            self.archived.push(item);
        }
    }

    /// Whether a sweep of the given kind is missed for this class this round.
    fn miss(&self, kind: &str, contract: Contract) -> bool {
        if !self.under_pressure {
            return false;
        }
        fires_keyed(5, &format!("{kind}/{}", contract.name()), self.round)
    }

    /// The verdict for one (stage, class) this round, folded over time.
    fn verdict(
        &mut self,
        scope: &str,
        sweep: Sweep,
        contract: Contract,
        leaks: &Leaks,
        now: i64,
    ) -> HealthRecord {
        let name = contract.name();
        let (leaked, line) = match sweep {
            Sweep::Retain => (
                leaks.retain.contains(name),
                self.retain_line(contract, leaks),
            ),
            Sweep::Archive => (
                leaks.archive.contains(name),
                self.archive_line(contract, leaks),
            ),
        };

        let mark = if leaked { Mark::Fail } else { Mark::Pass };
        let standing = self.standings.entry(scope.to_string()).or_default();
        standing.record(mark, line);
        standing.health(scope, now)
    }

    fn count_live(&self, contract: Contract) -> usize {
        self.live.iter().filter(|i| i.contract == contract).count()
    }

    fn count_archived(&self, contract: Contract) -> usize {
        self.archived
            .iter()
            .filter(|i| i.contract == contract)
            .count()
    }

    fn retain_line(&self, contract: Contract, leaks: &Leaks) -> String {
        if leaks.retain.contains(contract.name()) {
            "retention leak: an item past its window is still live".to_string()
        } else {
            format!("{} retained within the window", self.count_live(contract))
        }
    }

    fn archive_line(&self, contract: Contract, leaks: &Leaks) -> String {
        if leaks.archive.contains(contract.name()) {
            "the archive store refused an item".to_string()
        } else {
            format!("{} archived", self.count_archived(contract))
        }
    }
}

/// The leaks found this round, by class token.
#[derive(Default)]
struct Leaks {
    retain: std::collections::BTreeSet<&'static str>,
    archive: std::collections::BTreeSet<&'static str>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use observe::Health;

    /// Drive the secretary across `ticks` rounds, advancing simulated time by
    /// `days_per_tick` each round.
    fn run(secretary: &mut Secretary, days_per_tick: u64, ticks: u64) -> Snapshot {
        let mut snapshot = Snapshot::new();
        for round in 1..=ticks {
            let simulated = Duration::from_secs(round * days_per_tick * SECONDS_PER_DAY);
            snapshot = secretary.tick(simulated);
        }
        snapshot
    }

    #[test]
    fn a_methodical_run_retains_then_archives_without_a_leak() {
        let mut secretary = Secretary::new("xmip:///playground/secretary");
        // Three simulated years at ten days a tick — long past the retention window.
        let snapshot = run(&mut secretary, 10, 120);
        assert_eq!(
            snapshot.worst("xmip:///playground/secretary"),
            Some(Health::Green),
            "a clean secretary never leaks"
        );
        assert!(
            !secretary.archived.is_empty(),
            "items past the window are archived, not deleted"
        );
    }

    #[test]
    fn the_windows_policy_keeps_then_archives_and_never_deletes() {
        let policy = Windows;
        let at_days = |days: u64| Duration::from_secs(days * SECONDS_PER_DAY);
        assert_eq!(
            policy.action_for("json", Duration::ZERO),
            RetentionAction::Keep
        );
        assert_eq!(
            policy.action_for("json", at_days(KEEP_DAYS)),
            RetentionAction::Archive
        );
        // Far past the window it is still Archive — there is no Delete to reach.
        assert_eq!(
            policy.action_for("json", at_days(KEEP_DAYS * 100)),
            RetentionAction::Archive
        );
    }

    #[test]
    fn items_age_on_the_simulated_clock_not_the_round_count() {
        // Many rounds, but simulated time barely moves: nothing ages out of the
        // window, so nothing is archived. Round count alone would have archived them.
        let mut secretary = Secretary::new("xmip:///playground/secretary");
        for _ in 0..50 {
            secretary.tick(Duration::from_secs(SECONDS_PER_DAY)); // one simulated day, held
        }
        assert!(
            secretary.archived.is_empty(),
            "at one simulated day, nothing has passed the 90-day retention window"
        );
    }

    #[test]
    fn the_live_set_stays_bounded_and_the_archive_only_grows() {
        let mut secretary = Secretary::new("xmip:///playground/secretary");
        run(&mut secretary, 5, 200);
        let archived_first = secretary.archived.len();
        // Live is items younger than KEEP_DAYS — bounded by the window regardless
        // of how long it runs.
        assert!(secretary.live.len() as u64 <= (KEEP_DAYS + 1) * CONTRACTS.len() as u64);
        // The archive only ever grows; Xmip never deletes from it.
        run(&mut secretary, 5, 100);
        assert!(
            secretary.archived.len() >= archived_first,
            "the archive is never purged"
        );
    }

    #[test]
    fn under_pressure_a_leak_surfaces() {
        let mut secretary = Secretary::new("xmip:///playground/secretary").under_pressure();
        // A leak is red on the round it happens and fades to yellow after, so the
        // proof is that some round went red, not the state of the last one.
        let mut ever_red = false;
        for round in 1..=130 {
            let simulated = Duration::from_secs(round * 10 * SECONDS_PER_DAY);
            let snapshot = secretary.tick(simulated);
            if snapshot.worst("xmip:///playground/secretary") == Some(Health::Red) {
                ever_red = true;
            }
        }
        assert!(ever_red, "missed sweeps must surface as a leak");
    }
}
