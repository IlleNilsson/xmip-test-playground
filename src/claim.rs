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
//! win even under real thread contention — a rename to a per-reader name does
//! not, as two readers can each move a source they both still see. Competing
//! reader threads race for a shared directory; under pressure the atomic claim is
//! removed, so a second reader grabs the same item and the breach shows. The
//! claim is transport-agnostic — any other pollable transport gets this exercise
//! by adding a `RoundTrip` adapter, no change here, so no protocol is named in
//! this code.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use observe::{HealthRecord, Snapshot};

use crate::fault::fires_keyed;
use crate::standing::{Mark, Standing};
use crate::support::now_unix_nanos;

/// Competing reader threads per round.
const READERS: usize = 4;
/// Order keys, and items per key. Small, so a round is quick.
const KEYS: usize = 2;
const PER_KEY: usize = 3;
/// Percent of rounds a pressured run drops the atomic claim, so the board mostly
/// holds and a breach surfaces now and then rather than every round.
const BREACH_RATE: u8 = 12;

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

/// One item processed by one reader, in the global order it happened.
#[derive(Clone)]
struct Processed {
    item: String,
    key: usize,
    seq: usize,
    reader: usize,
    order: u64,
}

/// What a round concluded for one style.
enum Verdict {
    Held(String),
    Contended(String),
    Missed(String),
}

/// The claim exercise: each round drops keyed items into a directory and races
/// reader threads for them, one style at a time.
pub struct Claim {
    node: String,
    dir: PathBuf,
    round: u64,
    under_pressure: bool,
    standings: BTreeMap<String, Standing>,
}

impl Claim {
    /// A claim exercise publishing under `node`, using `dir` for the shared
    /// pickup directory, with the atomic claim intact.
    #[must_use]
    pub fn new(node: impl Into<String>, dir: impl Into<PathBuf>) -> Self {
        Self {
            node: node.into(),
            dir: dir.into(),
            round: 0,
            under_pressure: false,
            standings: BTreeMap::new(),
        }
    }

    /// The same exercise with the atomic claim removed, so a breach occurs.
    #[must_use]
    pub fn under_pressure(mut self) -> Self {
        self.under_pressure = true;
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

    /// Run the real pickup for one style over FILE, and judge it.
    fn exercise(&self, style: Style) -> Verdict {
        let round_dir = self.dir.join(format!("{}-{}", style.name(), self.round));
        std::fs::remove_dir_all(&round_dir).ok();
        if std::fs::create_dir_all(&round_dir).is_err() {
            return Verdict::Missed("could not create the pickup directory".to_string());
        }

        let dropped = drop_items(&round_dir);
        let broken = self.under_pressure
            && fires_keyed(BREACH_RATE, &format!("breach/{}", style.name()), self.round);
        let processed = Self::race(&round_dir, style, broken);
        std::fs::remove_dir_all(&round_dir).ok();

        judge(style, dropped, &processed)
    }

    /// Race `READERS` threads for the items, and return what each processed.
    fn race(dir: &Path, style: Style, broken: bool) -> Vec<Processed> {
        let log = Arc::new(Mutex::new(Vec::<Processed>::new()));
        let clock = Arc::new(AtomicU64::new(0));

        std::thread::scope(|scope| {
            for reader in 0..READERS {
                let log = Arc::clone(&log);
                let clock = Arc::clone(&clock);
                scope.spawn(move || {
                    if broken {
                        read_without_claiming(dir, reader, &log, &clock);
                    } else if style.ordered() {
                        claim_per_key(dir, reader, &log, &clock);
                    } else {
                        claim_per_item(dir, reader, &log, &clock);
                    }
                });
            }
        });

        Arc::try_unwrap(log)
            .map(|mutex| mutex.into_inner().unwrap_or_default())
            .unwrap_or_default()
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

/// Drop `KEYS` × `PER_KEY` items, named `item_<key>_<seq>`, each carrying nothing
/// but its name — the pickup, not the content, is the subject.
fn drop_items(dir: &Path) -> usize {
    let mut count = 0;
    for key in 0..KEYS {
        for seq in 0..PER_KEY {
            let path = dir.join(format!("item_{key}_{seq}"));
            if std::fs::write(&path, b"x").is_ok() {
                count += 1;
            }
        }
    }
    count
}

/// The honest per-item claim: win the item by atomically creating its lock —
/// `create_new` is `O_EXCL`, so exactly one creator wins even under real thread
/// contention (a rename to a per-reader name is not; two can both move a source
/// they each still see). The winner records it and removes it. Parallel and
/// Concurrent.
fn claim_per_item(dir: &Path, reader: usize, log: &Mutex<Vec<Processed>>, clock: &AtomicU64) {
    // Bounded so a held-but-not-yet-removed item can never spin forever; the
    // items are few, so this is far more headroom than needed.
    for _ in 0..64 {
        let items = list(dir, "item_");
        if items.is_empty() {
            break;
        }
        for path in items {
            if claimed(&path.with_extension("lock")) {
                record(&path, reader, log, clock);
                std::fs::remove_file(&path).ok();
            }
        }
    }
}

/// The honest per-key claim: win the key by atomically creating its lock, then
/// drain the key's items in sequence. One holder per key keeps the order.
/// Sequential.
fn claim_per_key(dir: &Path, reader: usize, log: &Mutex<Vec<Processed>>, clock: &AtomicU64) {
    for key in 0..KEYS {
        if claimed(&dir.join(format!("key_{key}.lock"))) {
            for seq in 0..PER_KEY {
                let path = dir.join(format!("item_{key}_{seq}"));
                record(&path, reader, log, clock);
                std::fs::remove_file(&path).ok();
            }
        }
    }
}

/// Atomically take a lock: `true` for the one creator, `false` for everyone else.
fn claimed(lock: &Path) -> bool {
    std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(lock)
        .is_ok()
}

/// The broken claim under pressure: read and process without any atomic step, so
/// every reader takes every item. The breach the scenario exists to catch.
fn read_without_claiming(
    dir: &Path,
    reader: usize,
    log: &Mutex<Vec<Processed>>,
    clock: &AtomicU64,
) {
    for path in list(dir, "item_") {
        record(&path, reader, log, clock);
    }
}

fn list(dir: &Path, prefix: &str) -> Vec<PathBuf> {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return Vec::new();
    };
    entries
        .flatten()
        .map(|entry| entry.path())
        .filter(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with(prefix) && !name.contains('.'))
        })
        .collect()
}

fn record(path: &Path, reader: usize, log: &Mutex<Vec<Processed>>, clock: &AtomicU64) {
    let name = path
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or_default()
        .to_string();
    let (key, seq) = parse(&name);
    let order = clock.fetch_add(1, Ordering::SeqCst);
    if let Ok(mut log) = log.lock() {
        log.push(Processed {
            item: name,
            key,
            seq,
            reader,
            order,
        });
    }
}

/// `item_<key>_<seq>` → (key, seq); zeros if it does not parse.
fn parse(name: &str) -> (usize, usize) {
    let mut parts = name.trim_start_matches("item_").split('_');
    let key = parts.next().and_then(|p| p.parse().ok()).unwrap_or(0);
    let seq = parts.next().and_then(|p| p.parse().ok()).unwrap_or(0);
    (key, seq)
}

/// Judge a round: every item claimed by exactly one reader (the claim), none
/// missed, and — for Sequential — each key processed in order.
fn judge(style: Style, dropped: usize, processed: &[Processed]) -> Verdict {
    use std::collections::BTreeMap as Map;

    let mut holders: Map<&str, std::collections::BTreeSet<usize>> = Map::new();
    for record in processed {
        holders
            .entry(&record.item)
            .or_default()
            .insert(record.reader);
    }

    if let Some((item, who)) = holders.iter().find(|(_, who)| who.len() > 1) {
        return Verdict::Contended(format!(
            "{item} was claimed by {} readers at once",
            who.len()
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

    let note = if style.ordered() {
        ", in order per key"
    } else {
        ""
    };
    Verdict::Held(format!("{dropped} items, one holder each{note}"))
}

/// Whether each key's items were processed in non-decreasing sequence.
fn ordered_per_key(processed: &[Processed]) -> bool {
    let mut order = processed.to_vec();
    order.sort_by_key(|record| record.order);

    let mut last: std::collections::BTreeMap<usize, usize> = std::collections::BTreeMap::new();
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
            Some(Health::Green),
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
            if snapshot.worst("xmip:///playground/claim/file") == Some(Health::Red) {
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
                Some(Health::Green),
                "{style} holds the claim"
            );
        }
        std::fs::remove_dir_all(&dir).ok();
    }
}
