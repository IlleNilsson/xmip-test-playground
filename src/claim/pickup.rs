//! The pickup the claim scenario runs over: a lane of items on disk, the locks
//! a reader takes, the ledger every holder signs, and the reader itself.
//!
//! Split from `claim.rs` on 2026-09-09 when sharing across processes made the
//! scenario file long; the scenario, the race and the judge stay there, and
//! this file is what touches the filesystem. Nothing here is a verdict.

use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// Order keys, and items per key. Small, so a round is quick.
pub(super) const KEYS: usize = 2;
pub(super) const PER_KEY: usize = 3;
/// How many times a reader rescans before giving up on a lane that will not
/// drain — bounded so a held-but-never-removed item cannot spin it forever.
const SCANS: usize = 64;

/// One item processed by one holder, read back from its done record.
pub(super) struct Processed {
    pub(super) item: String,
    pub(super) key: usize,
    pub(super) seq: usize,
    pub(super) holder: String,
    pub(super) order: u64,
    pub(super) injected: bool,
}

/// One reader thread: which lanes it scans and how it signs what it did.
pub(super) struct Reader<'a> {
    pub(super) holder: String,
    pub(super) clock: &'a AtomicU64,
    pub(super) shared: bool,
    pub(super) style_dir: &'a Path,
    pub(super) own: &'a Path,
}

impl Reader<'_> {
    /// Rescan until this process's own lane is drained. A broken reader makes
    /// one pass: it consumes nothing, so a rescan would only repeat it.
    pub(super) fn run(&self, ordered: bool, broken: bool) {
        let passes = if broken { 1 } else { SCANS };
        for _ in 0..passes {
            for lane in self.lanes() {
                if broken {
                    self.without_claiming(&lane);
                } else if ordered {
                    self.per_key(&lane);
                } else {
                    self.per_item(&lane);
                }
            }
            if list(self.own, "item_").is_empty() {
                break;
            }
        }
    }

    /// The lanes to scan: every process's when shared, else only this one's.
    /// A lane still being staged is hidden behind a dot and skipped.
    fn lanes(&self) -> Vec<PathBuf> {
        if !self.shared {
            return vec![self.own.to_path_buf()];
        }
        let Ok(entries) = std::fs::read_dir(self.style_dir) else {
            return Vec::new();
        };
        entries
            .flatten()
            .map(|entry| entry.path())
            .filter(|path| path.is_dir() && !name_of(path).starts_with('.'))
            .collect()
    }

    /// The honest per-item claim: win the item by atomically creating its
    /// lock, then read it. A lock won on an item already gone is a lost race,
    /// not a pickup. Parallel and Concurrent.
    fn per_item(&self, lane: &Path) {
        for path in list(lane, "item_") {
            if claimed(&path.with_extension("lock")) {
                self.process(&path, false);
            }
        }
    }

    /// The honest per-key claim: win the key by atomically creating its lock,
    /// then drain the key's items in sequence. One holder per key keeps the
    /// order. Sequential.
    fn per_key(&self, lane: &Path) {
        for key in 0..KEYS {
            if claimed(&lane.join(format!("key_{key}.lock"))) {
                for seq in 0..PER_KEY {
                    self.process(&lane.join(format!("item_{key}_{seq}")), false);
                }
            }
        }
    }

    /// The broken claim under pressure: read and process without any atomic
    /// step, so every reader takes every item. The breach the scenario exists
    /// to catch.
    fn without_claiming(&self, lane: &Path) {
        for path in list(lane, "item_") {
            self.process(&path, true);
        }
    }

    /// Process one item: read it, sign the done record, and consume it — unless
    /// this is the injected breach, which leaves the item for the next reader.
    fn process(&self, item: &Path, injected: bool) {
        if std::fs::read(item).is_err() {
            return;
        }
        let order = self.clock.fetch_add(1, Ordering::SeqCst);
        let done = item.with_extension(format!("done.{}", self.holder));
        let note = if injected { " injected" } else { "" };
        if let Ok(mut file) = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(done)
        {
            file.write_all(format!("{order}{note}").as_bytes()).ok();
        }
        if !injected {
            std::fs::remove_file(item).ok();
        }
    }
}

/// Lay `KEYS` × `PER_KEY` items, named `item_<key>_<seq>`, in the lane — staged
/// behind a dot and renamed into place, so no reader sees a half-filled lane.
/// Each carries nothing but its name; the pickup, not the content, is the
/// subject. `None` when the lane could not be made.
pub(super) fn stage_lane(lane: &Path) -> Option<usize> {
    let staging = lane.with_file_name(format!(".{}", name_of(lane)));
    std::fs::remove_dir_all(&staging).ok();
    std::fs::create_dir_all(&staging).ok()?;

    let mut count = 0;
    for key in 0..KEYS {
        for seq in 0..PER_KEY {
            if std::fs::write(staging.join(format!("item_{key}_{seq}")), b"x").is_ok() {
                count += 1;
            }
        }
    }

    std::fs::rename(&staging, lane).ok()?;
    Some(count)
}

/// Tear a judged lane down. A straggler from another process may be inside it;
/// a few tries cover that, and a lane left behind is litter, not a defect.
pub(super) fn remove_lane(lane: &Path) {
    for _ in 0..3 {
        if std::fs::remove_dir_all(lane).is_ok() {
            return;
        }
        std::thread::sleep(Duration::from_millis(10));
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

fn name_of(path: &Path) -> &str {
    path.file_name()
        .and_then(|n| n.to_str())
        .unwrap_or_default()
}

/// The items in a lane: names with `prefix` and no extension.
pub(super) fn list(dir: &Path, prefix: &str) -> Vec<PathBuf> {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return Vec::new();
    };
    entries
        .flatten()
        .map(|entry| entry.path())
        .filter(|path| {
            let name = name_of(path);
            name.starts_with(prefix) && !name.contains('.')
        })
        .collect()
}

/// Every done record in a lane: `item_<key>_<seq>.done.<holder>` holding the
/// holder's order and whether it was the injected breach.
pub(super) fn ledger(lane: &Path) -> Vec<Processed> {
    let Ok(entries) = std::fs::read_dir(lane) else {
        return Vec::new();
    };
    entries
        .flatten()
        .filter_map(|entry| {
            let name = entry.file_name().to_str()?.to_string();
            let (item, holder) = name.split_once(".done.")?;
            let content = std::fs::read_to_string(entry.path()).unwrap_or_default();
            let (key, seq) = parse(item);
            Some(Processed {
                item: item.to_string(),
                key,
                seq,
                holder: holder.to_string(),
                order: content
                    .split_whitespace()
                    .next()
                    .and_then(|o| o.parse().ok())
                    .unwrap_or(0),
                injected: content.contains("injected"),
            })
        })
        .collect()
}

/// `item_<key>_<seq>` → (key, seq); zeros if it does not parse.
fn parse(name: &str) -> (usize, usize) {
    let mut parts = name.trim_start_matches("item_").split('_');
    let key = parts.next().and_then(|p| p.parse().ok()).unwrap_or(0);
    let seq = parts.next().and_then(|p| p.parse().ok()).unwrap_or(0);
    (key, seq)
}
