//! The secretary scenario: retention and archiving, done methodically over time.
//!
//! ADR-0028; ADR-0013 and the observability model own retention. Where pingpong
//! watches a message cross the wire, the secretary watches it age: an item is
//! **kept** while it is young, **archived** when it passes its retention window,
//! and **purged** when it passes its archive window. The scenario drives the
//! estate's real [`RetentionPolicy`] (keep, archive, delete by age) and real
//! [`ArchiveStore`] over a logical clock — one round is one second — so the whole
//! lifecycle runs, not a mock of it.
//!
//! Three stages, per content class: **retain**, **archive**, **purge**. Green
//! while every item is in the bucket its age dictates; **red** on a leak — an
//! item past its window still live (retain), an archive the store refused
//! (archive), an item past retention still stored (purge). Under pressure the
//! secretary misses a sweep now and then, deterministically, so the leaks it is
//! meant to catch actually occur.

use std::collections::BTreeMap;
use std::time::Duration;

use archive::{ArchiveError, ArchiveItem, ArchiveReceipt, ArchiveStore};
use observe::{Health, HealthRecord, Snapshot};
use retain::{RetentionAction, RetentionPolicy};

use crate::fault::fires_keyed;
use crate::schedule::{CONTRACTS, now_unix_nanos};
use crate::verdict::Contract;

/// How long an item is kept before it is archived, and how long it is archived
/// before it is purged, in rounds. Small, so the lifecycle turns over quickly and
/// the live and archived sets stay tiny.
const KEEP_ROUNDS: u64 = 3;
const ARCHIVE_ROUNDS: u64 = 3;

/// The retention windows as a real [`RetentionPolicy`]: keep, then archive, then
/// delete, by age. The one the secretary consults and the verdict checks against.
struct Windows;

impl RetentionPolicy for Windows {
    fn action_for(&self, _data_type: &str, age: Duration) -> RetentionAction {
        let seconds = age.as_secs();
        if seconds < KEEP_ROUNDS {
            RetentionAction::Keep
        } else if seconds < KEEP_ROUNDS + ARCHIVE_ROUNDS {
            RetentionAction::Archive
        } else {
            RetentionAction::Delete
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

/// One item the secretary is looking after.
struct Item {
    contract: Contract,
    created: u64,
    id: u64,
}

/// One (stage, class) record over time.
#[derive(Clone, Debug, Default)]
struct Tally {
    rounds: u64,
    failures: u64,
    last_ok: bool,
    last_line: String,
}

/// A stage of the retention lifecycle.
#[derive(Clone, Copy)]
enum Sweep {
    Retain,
    Archive,
    Purge,
}

impl Sweep {
    const ALL: [Sweep; 3] = [Sweep::Retain, Sweep::Archive, Sweep::Purge];
    fn name(self) -> &'static str {
        match self {
            Sweep::Retain => "retain",
            Sweep::Archive => "archive",
            Sweep::Purge => "purge",
        }
    }
}

/// The secretary: it creates an item per class each round, ages the lot, and
/// sweeps them through keep, archive and purge, driving the real policy and store.
pub struct Secretary {
    node: String,
    policy: Windows,
    store: MemoryArchive,
    live: Vec<Item>,
    archived: Vec<Item>,
    round: u64,
    next_id: u64,
    under_pressure: bool,
    tallies: BTreeMap<String, Tally>,
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
            next_id: 0,
            under_pressure: false,
            tallies: BTreeMap::new(),
        }
    }

    /// The same secretary, occasionally missing a sweep so leaks occur.
    #[must_use]
    pub fn under_pressure(mut self) -> Self {
        self.under_pressure = true;
        self
    }

    /// One round: create an item per class, age everything, sweep it, and
    /// publish a verdict per (stage, class).
    pub fn tick(&mut self) -> Snapshot {
        self.round += 1;
        let now = now_unix_nanos();

        for &contract in &CONTRACTS {
            self.live.push(Item {
                contract,
                created: self.round,
                id: self.next_id,
            });
            self.next_id += 1;
        }

        let mut leaks = Leaks::default();
        self.purge_archived(&mut leaks);
        self.sweep_live(&mut leaks);

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

    /// Purge archived items whose age says delete — unless a purge is missed.
    fn purge_archived(&mut self, leaks: &mut Leaks) {
        let round = self.round;
        let mut kept = Vec::new();
        for item in std::mem::take(&mut self.archived) {
            let age = Duration::from_secs(round - item.created);
            if self.policy.action_for(item.contract.name(), age) == RetentionAction::Delete {
                if self.miss("purge", item.contract) {
                    leaks.purge.insert(item.contract.name());
                    kept.push(item);
                }
                // else: dropped — purged.
            } else {
                kept.push(item);
            }
        }
        self.archived = kept;
    }

    /// Sweep live items: keep the young, archive the aged, and catch anything
    /// that slipped past its windows.
    fn sweep_live(&mut self, leaks: &mut Leaks) {
        let round = self.round;
        let mut kept = Vec::new();
        for item in std::mem::take(&mut self.live) {
            let age = Duration::from_secs(round - item.created);
            match self.policy.action_for(item.contract.name(), age) {
                RetentionAction::Keep => kept.push(item),
                RetentionAction::Archive => self.try_archive(item, leaks, &mut kept),
                RetentionAction::Delete => {
                    // It should have been archived already; that it is still live
                    // is a retain leak. Purge it now unless that is missed too.
                    leaks.retain.insert(item.contract.name());
                    if self.miss("purge", item.contract) {
                        leaks.purge.insert(item.contract.name());
                        kept.push(item);
                    }
                }
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
            Sweep::Purge => (leaks.purge.contains(name), purge_line(contract, leaks)),
        };

        let tally = self.tallies.entry(scope.to_string()).or_default();
        tally.rounds += 1;
        tally.last_ok = !leaked;
        tally.last_line = line;
        if leaked {
            tally.failures += 1;
        }

        health(scope, tally, now)
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
            "retention leak: an item past its keep window is still live".to_string()
        } else {
            format!("{} kept within the window", self.count_live(contract))
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

fn purge_line(contract: Contract, leaks: &Leaks) -> String {
    if leaks.purge.contains(contract.name()) {
        "purge overdue: an item past its retention is still stored".to_string()
    } else {
        "none overdue".to_string()
    }
}

/// The leaks found this round, by class token.
#[derive(Default)]
struct Leaks {
    retain: std::collections::BTreeSet<&'static str>,
    archive: std::collections::BTreeSet<&'static str>,
    purge: std::collections::BTreeSet<&'static str>,
}

fn health(scope: &str, tally: &Tally, now: i64) -> HealthRecord {
    let (health, severity) = if tally.last_ok && tally.failures == 0 {
        (Health::Green, 0)
    } else if tally.last_ok {
        (Health::Yellow, 45)
    } else {
        (Health::Red, 90)
    };

    HealthRecord {
        scope: scope.to_string(),
        health,
        severity,
        evidence: tally.last_line.clone(),
        observed_unix_nanos: now,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_methodical_run_keeps_archives_and_purges_without_a_leak() {
        let mut secretary = Secretary::new("xmip:///playground/secretary");
        let mut snapshot = secretary.tick();
        for _ in 0..40 {
            snapshot = secretary.tick();
        }
        assert_eq!(
            snapshot.worst("xmip:///playground/secretary"),
            Some(Health::Green),
            "a clean secretary never leaks"
        );
    }

    #[test]
    fn the_live_and_archived_sets_stay_bounded() {
        let mut secretary = Secretary::new("xmip:///playground/secretary");
        for _ in 0..200 {
            secretary.tick();
        }
        // Keep window is KEEP_ROUNDS ages (0..KEEP) per class, archived is
        // ARCHIVE_ROUNDS ages per class. Bounded regardless of how long it runs.
        assert!(
            u64::try_from(secretary.live.len()).expect("len fits u64")
                <= (KEEP_ROUNDS + 1) * CONTRACTS.len() as u64
        );
        assert!(
            u64::try_from(secretary.archived.len()).expect("len fits u64")
                <= (ARCHIVE_ROUNDS + 1) * CONTRACTS.len() as u64
        );
    }

    #[test]
    fn under_pressure_a_leak_surfaces() {
        let mut secretary = Secretary::new("xmip:///playground/secretary").under_pressure();
        let mut snapshot = secretary.tick();
        for _ in 0..120 {
            snapshot = secretary.tick();
        }
        assert_eq!(
            snapshot.worst("xmip:///playground/secretary"),
            Some(Health::Red),
            "missed sweeps must surface as a leak"
        );
    }

    #[test]
    fn the_windows_policy_keeps_then_archives_then_deletes() {
        let policy = Windows;
        assert_eq!(
            policy.action_for("json", Duration::from_secs(0)),
            RetentionAction::Keep
        );
        assert_eq!(
            policy.action_for("json", Duration::from_secs(KEEP_ROUNDS)),
            RetentionAction::Archive
        );
        assert_eq!(
            policy.action_for("json", Duration::from_secs(KEEP_ROUNDS + ARCHIVE_ROUNDS)),
            RetentionAction::Delete
        );
    }
}
