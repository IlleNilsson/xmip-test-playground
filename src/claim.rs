//! The claim scenario: exclusive pickup, one holder at a time.
//!
//! ADR-0028; ADR-0024 owns the claim. Some resources must be read by exactly one
//! party — a thread, in a process, on a node — and no one else may touch the
//! resource until it is processed. Two readers grabbing the same file is a
//! duplicate payment, a duplicate order: the classic file-pickup bug. This
//! scenario proves the claim holds under real contention.
//!
//! The leaf axis is **execution style** (runtime-model.md), how work runs once
//! claimed — Sequential, Parallel, Concurrent. The claim (one holder per item)
//! must hold in all three; **Sequential** additionally keeps order per key,
//! which a claim enables but does not itself provide (runtime-model.md, *A claim
//! is not ordering*).
//!
//! It runs over the file substrate: a reader claims an item by **atomically
//! creating its lock** (`create_new`, `O_EXCL`), which lets exactly one creator
//! win even under real contention — a rename to a per-reader name does not, as
//! two readers can each move a source they both still see. Competing reader
//! threads race for a shared directory; under pressure the atomic claim is
//! removed, so a second reader grabs the same item and the breach shows. The
//! claim is transport-agnostic — any other pollable transport gets this exercise
//! by adding a `RoundTrip` adapter, no change here, so no protocol is named in
//! this code.
//!
//! **Across processes, 2026-09-09.** A [`Claim::shared`] exercise lays its items
//! in a *lane* — `<dir>/<style>/<pid>-<round>/` — beside every other process's
//! lanes, and its readers scan them all, so the contention is between real
//! System Processes (ADR-0028 clause 2), which is the property `O_EXCL` exists
//! to prove. Every holder writes a *done* record beside the item it processed,
//! honest or not, and the process that dropped the items judges from those
//! records once its lane is drained by whoever got there first. A lost race —
//! an item gone before it could be read, a lane torn down under a straggler —
//! is a normal outcome, never an error.

mod pickup;

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicU64;
use std::time::{Duration, Instant};

use observe::{HealthRecord, Snapshot};

use crate::fault::fires_keyed;
use crate::standing::{Mark, Standing};
use crate::stress::{Stress, scaled_rate};
use crate::support::now_unix_nanos;
use pickup::{Processed, Reader, ledger, list, remove_lane, stage_lane};

/// Competing reader threads per round.
const READERS: usize = 4;
/// Percent of rounds a pressured run drops the atomic claim, so the board mostly
/// holds and a breach surfaces now and then rather than every round.
const BREACH_RATE: u8 = 12;
/// How long a shared judge waits for the fleet to drain its lane before it
/// calls what remains missed.
const DRAIN_WAIT: Duration = Duration::from_secs(2);

/// The transport substrate the claim runs over: the file directory, the one
/// pollable transport with an adapter. Named because it is implemented; no
/// unimplemented protocol is named here.
const SUBSTRATE: &str = "file";

/// How work runs once claimed. runtime-model.md.
#[derive(Clone, Copy)]
enum Style {
    Sequential,
    Parallel,
    Concurrent,
}

impl Style {
    const ALL: [Style; 3] = [Style::Sequential, Style::Parallel, Style::Concurrent];

    fn name(self) -> &'static str {
        match self {
            Style::Sequential => "sequential",
            Style::Parallel => "parallel",
            Style::Concurrent => "concurrent",
        }
    }

    /// Sequential is the one that keeps order per key.
    fn ordered(self) -> bool {
        matches!(self, Style::Sequential)
    }
}

/// What a round concluded for one style.
enum Verdict {
    Held(String),
    Contended(String),
    Missed(String),
}

/// The claim exercise: each round drops keyed items into a lane and races
/// reader threads — and, when shared, every other process's readers — for
/// them, one style at a time.
pub struct Claim {
    node: String,
    dir: PathBuf,
    tag: String,
    shared: bool,
    round: u64,
    rate: u8,
    standings: BTreeMap<String, Standing>,
}

impl Claim {
    /// A claim exercise publishing under `node`, using `dir` for the pickup
    /// directory, with the atomic claim intact and no other process in it.
    #[must_use]
    pub fn new(node: impl Into<String>, dir: impl Into<PathBuf>) -> Self {
        Self {
            node: node.into(),
            dir: dir.into(),
            tag: std::process::id().to_string(),
            shared: false,
            round: 0,
            rate: 0,
            standings: BTreeMap::new(),
        }
    }

    /// The same exercise over a directory other processes share: this
    /// process's readers scan every lane, and its lane is drained by whichever
    /// process gets there first.
    #[must_use]
    pub fn shared(node: impl Into<String>, dir: impl Into<PathBuf>) -> Self {
        let mut claim = Self::new(node, dir);
        claim.shared = true;
        claim
    }

    /// The same exercise with the atomic claim removed now and then, so a
    /// breach occurs.
    #[must_use]
    pub fn under_pressure(mut self) -> Self {
        self.rate = BREACH_RATE;
        self
    }

    /// The breach rate scaled to a stress level: none at `Calm`, the realistic
    /// rate at `Realistic`, more above.
    #[must_use]
    pub fn at(mut self, stress: Stress) -> Self {
        self.rate = scaled_rate(BREACH_RATE, stress);
        self
    }

    /// One round: run the exclusive pickup over the file substrate for each style.
    pub fn tick(&mut self) -> Snapshot {
        self.round += 1;
        let now = now_unix_nanos();
        let mut snapshot = Snapshot::new();

        for style in Style::ALL {
            let verdict = self.exercise(style);
            snapshot.record_health(self.fold(style, verdict, now));
        }

        snapshot
    }

    /// Run the real pickup for one style over the file substrate, and judge it.
    fn exercise(&self, style: Style) -> Verdict {
        let style_dir = self.dir.join(style.name());
        let lane = style_dir.join(format!("{}-{}", self.tag, self.round));
        let Some(dropped) = stage_lane(&lane) else {
            return Verdict::Missed("could not create the pickup lane".to_string());
        };

        let broken = fires_keyed(self.rate, &format!("breach/{}", style.name()), self.round);
        self.race(&style_dir, &lane, style, broken);
        let processed = self.settle(&lane);
        let verdict = judge(style, dropped, &processed);
        remove_lane(&lane);
        verdict
    }

    /// Race `READERS` threads for the items in every lane this process scans.
    fn race(&self, style_dir: &Path, own: &Path, style: Style, broken: bool) {
        let clock = AtomicU64::new(0);
        std::thread::scope(|scope| {
            for index in 0..READERS {
                let reader = Reader {
                    holder: format!("{}-{index}", self.tag),
                    clock: &clock,
                    shared: self.shared,
                    style_dir,
                    own,
                };
                scope.spawn(move || reader.run(style.ordered(), broken));
            }
        });
    }

    /// The ledger for this process's lane once it is drained — by these
    /// readers, or by any other process's — or once a breach is already
    /// visible, or once the wait runs out.
    fn settle(&self, lane: &Path) -> Vec<Processed> {
        let started = Instant::now();
        loop {
            let processed = ledger(lane);
            let drained = list(lane, "item_").is_empty();
            if !self.shared || drained || duplicated(&processed) || started.elapsed() > DRAIN_WAIT {
                return processed;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    fn fold(&mut self, style: Style, verdict: Verdict, now: i64) -> HealthRecord {
        let scope = format!("{}/{}/{}", self.node, SUBSTRATE, style.name());
        let (ok, line) = match verdict {
            Verdict::Held(line) => (true, line),
            Verdict::Contended(line) | Verdict::Missed(line) => (false, line),
        };

        let mark = if ok { Mark::Pass } else { Mark::Fail };
        let standing = self.standings.entry(scope.clone()).or_default();
        standing.record(mark, line);
        standing.health(&scope, now)
    }
}

/// Item → the holders that processed it.
fn holders(processed: &[Processed]) -> BTreeMap<&str, BTreeSet<&str>> {
    let mut holders: BTreeMap<&str, BTreeSet<&str>> = BTreeMap::new();
    for record in processed {
        holders
            .entry(&record.item)
            .or_default()
            .insert(&record.holder);
    }
    holders
}

fn duplicated(processed: &[Processed]) -> bool {
    holders(processed).values().any(|who| who.len() > 1)
}

/// Judge a round: every item claimed by exactly one holder (the claim), none
/// missed, and — for Sequential — each key processed in order. A breach the
/// playground injected says so, so an operator can tell it from a real one.
fn judge(style: Style, dropped: usize, processed: &[Processed]) -> Verdict {
    let holders = holders(processed);

    if let Some((item, who)) = holders.iter().find(|(_, who)| who.len() > 1) {
        let names: Vec<&str> = who.iter().copied().collect();
        let injected = processed.iter().any(|record| record.injected);
        let note = if injected { " (injected)" } else { "" };
        return Verdict::Contended(format!(
            "{item} was claimed by {} holders at once: {}{note}",
            who.len(),
            names.join(", ")
        ));
    }

    if holders.len() < dropped {
        return Verdict::Missed(format!(
            "{} of {dropped} items were never claimed",
            dropped - holders.len()
        ));
    }

    if style.ordered() && !ordered_per_key(processed) {
        return Verdict::Contended("the sequence was reordered under contention".to_string());
    }

    let owners: BTreeSet<&str> = processed
        .iter()
        .filter_map(|record| record.holder.split_once('-'))
        .map(|(process, _)| process)
        .collect();
    let note = if style.ordered() {
        ", in order per key"
    } else {
        ""
    };
    Verdict::Held(format!(
        "{dropped} items, one holder each{note}, {} process(es)",
        owners.len()
    ))
}

/// Whether each key's items were processed in non-decreasing sequence.
fn ordered_per_key(processed: &[Processed]) -> bool {
    let mut order: Vec<&Processed> = processed.iter().collect();
    order.sort_by_key(|record| record.order);

    let mut last: BTreeMap<usize, usize> = BTreeMap::new();
    for record in order {
        let previous = last.insert(record.key, record.seq);
        if let Some(previous) = previous
            && record.seq < previous
        {
            return false;
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::report::from_toml;
    use crate::support::scratch;
    use observe::Health;

    #[test]
    fn the_atomic_claim_gives_every_item_one_holder() {
        let dir = scratch("held");
        let mut claim = Claim::new("xmip:///playground/claim", &dir);
        let mut snapshot = claim.tick();
        for _ in 0..10 {
            snapshot = claim.tick();
        }
        assert_eq!(
            snapshot.worst("xmip:///playground/claim/file"),
            Some(Health::Fine),
            "the atomic rename claim holds under contention"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn without_the_claim_a_breach_shows() {
        let dir = scratch("breach");
        let mut claim = Claim::new("xmip:///playground/claim", &dir).under_pressure();
        let mut saw_red = false;
        for _ in 0..80 {
            let snapshot = claim.tick();
            // A Done leaf rolls up to Holding at the aggregate (ADR-0041).
            if snapshot.worst("xmip:///playground/claim/file") == Some(Health::Holding) {
                saw_red = true;
                break;
            }
        }
        assert!(
            saw_red,
            "a dropped claim must surface as a breach within 80 rounds"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn every_style_holds_the_claim_when_healthy() {
        let dir = scratch("styles");
        let mut claim = Claim::new("xmip:///playground/claim", &dir);
        let mut snapshot = claim.tick();
        for _ in 0..5 {
            snapshot = claim.tick();
        }
        for style in ["sequential", "parallel", "concurrent"] {
            assert_eq!(
                snapshot.worst(&format!("xmip:///playground/claim/file/{style}")),
                Some(Health::Fine),
                "{style} holds the claim"
            );
        }
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn calm_leaves_the_claim_intact_and_stress_scales_the_breach() {
        assert_eq!(Claim::new("n", "d").at(Stress::Calm).rate, 0);
        assert_eq!(Claim::new("n", "d").at(Stress::Realistic).rate, BREACH_RATE);
        assert!(Claim::new("n", "d").at(Stress::Harsh).rate > BREACH_RATE);
    }

    /// Two real System Processes over one shared directory: the property
    /// ADR-0024's claim exists to prove, `O_EXCL` across processes. The test
    /// stages a lane of a thousand items both nodes find on their first scan,
    /// so their readers contend for the same items, and judges the ledger
    /// itself; both nodes' own verdicts must hold as well.
    #[test]
    fn two_processes_never_both_claim_the_same_item() {
        let dir = scratch("two-processes");
        let shared = dir.join("shared");
        let contended = shared.join("claim/parallel/contended-0");
        let staging = contended.with_file_name(".contended-0");
        std::fs::create_dir_all(&staging).expect("staging dir");
        for n in 0..1_000 {
            std::fs::write(staging.join(format!("item_0_{n}")), b"x").expect("an item");
        }
        std::fs::rename(&staging, &contended).expect("the lane into place");
        let node = crate::fleet::built_node_binary();

        let children: Vec<std::process::Child> = ["left", "right"]
            .iter()
            .map(|name| {
                std::process::Command::new(&node)
                    .args(["--name", name, "--stress", "calm", "--rounds", "4"])
                    .args(["--interval-ms", "0"])
                    .arg("--shared")
                    .arg(&shared)
                    .arg("--snapshot")
                    .arg(dir.join(format!("{name}.toml")))
                    .spawn()
                    .expect("spawn the node binary")
            })
            .collect();
        for mut child in children {
            let status = child.wait().expect("wait for the node");
            assert!(status.success(), "a node exits cleanly: {status}");
        }

        let processed = ledger(&contended);
        assert!(list(&contended, "item_").is_empty(), "every item was taken");
        assert!(!duplicated(&processed), "no item has two holders");
        assert_eq!(
            holders(&processed).len(),
            1_000,
            "every item has one holder"
        );
        let owners: BTreeSet<&str> = processed
            .iter()
            .filter_map(|record| record.holder.split_once('-'))
            .map(|(process, _)| process)
            .collect();
        assert_eq!(owners.len(), 2, "both processes took items from the lane");

        for name in ["left", "right"] {
            let text = std::fs::read_to_string(dir.join(format!("{name}.toml"))).expect("snapshot");
            let snapshot = from_toml(&text).expect("a node's snapshot parses");
            let scope = format!("xmip:///playground/node/{name}/claim/file");
            for record in snapshot.health(&scope) {
                assert_eq!(
                    record.health,
                    Health::Fine,
                    "{}: {}",
                    record.scope,
                    record.evidence
                );
            }
        }
        std::fs::remove_dir_all(&dir).ok();
    }
}
