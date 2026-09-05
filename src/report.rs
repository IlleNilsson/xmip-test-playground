//! Writing a snapshot and its history where a monitoring surface can read them.
//!
//! The playground and the readers (the GUI, the CLI) are separate processes;
//! the bridge between them is a file. It is written **atomically** — a temp file
//! renamed over the target — so a reader never catches a half-written file even
//! after a week of ticks.
//!
//! **TOML, not JSON.** On disk the estate is TOML — the owner's rule, the same
//! reason `architecture.json` was deleted for `architecture.toml`; JSON is
//! reserved for what lives in memory or on the wire. These files persist, so
//! they are TOML. (Content that happens to be JSON, like a probe's payload, is
//! a different thing entirely — that is data being carried, not a file the
//! estate configures itself from.)

use std::io;
use std::path::Path;

use serde::Serialize;
use xmip_observe::{Counted, Health, History, Snapshot};

#[derive(Serialize)]
struct SnapshotReport {
    source: String,
    node: String,
    records: Vec<RecordReport>,
    counts: Vec<CountReport>,
}

#[derive(Serialize)]
struct RecordReport {
    scope: String,
    state: String,
    severity: u8,
    evidence: String,
    observed_unix_nanos: i64,
}

#[derive(Serialize)]
struct CountReport {
    counted: String,
    value: u64,
}

#[derive(Serialize)]
struct HistoryReport {
    node: String,
    points: Vec<PointReport>,
}

#[derive(Serialize)]
struct PointReport {
    counted: String,
    observed_unix_nanos: i64,
    value: u64,
}

/// The snapshot beneath `node` as the TOML the GUI's file surface reads: health
/// records and the node's throughput counts.
#[must_use]
pub fn to_toml(node: &str, snapshot: &Snapshot) -> String {
    let records = snapshot
        .health(node)
        .into_iter()
        .map(|record| RecordReport {
            scope: record.scope,
            state: state(record.health).to_string(),
            severity: record.severity,
            evidence: record.evidence,
            observed_unix_nanos: record.observed_unix_nanos,
        })
        .collect();

    let counts = [
        Counted::Streams,
        Counted::Messages,
        Counted::Journeys,
        Counted::Bytes,
    ]
    .into_iter()
    .filter_map(|counted| {
        snapshot.measure(node, counted).map(|count| CountReport {
            counted: counted_name(counted).to_string(),
            value: count.value,
        })
    })
    .collect();

    let report = SnapshotReport {
        source: format!("playground — {node}"),
        node: node.to_string(),
        records,
        counts,
    };

    toml::to_string(&report).unwrap_or_default()
}

/// The node's throughput over time as the TOML the history cmdlet and UI read:
/// one point per counted kind per tick, oldest first. ADR-0029.
#[must_use]
pub fn history_toml(node: &str, history: &History) -> String {
    let mut points = Vec::new();

    for counted in [Counted::Streams, Counted::Messages, Counted::Bytes] {
        for point in history.count_series(node, counted) {
            points.push(PointReport {
                counted: counted_name(counted).to_string(),
                observed_unix_nanos: point.observed_unix_nanos,
                value: point.value,
            });
        }
    }

    let report = HistoryReport {
        node: node.to_string(),
        points,
    };

    toml::to_string(&report).unwrap_or_default()
}

/// Write `contents` to `path` atomically: a sibling temp file, then a rename
/// over the target. A reader either sees the previous file or this one, never a
/// torn write.
///
/// # Errors
///
/// Where the parent could not be created, or the file could not be written or
/// renamed.
pub fn write_atomic(path: &Path, contents: &str) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    let temp = path.with_extension("toml.writing");
    std::fs::write(&temp, contents)?;
    std::fs::rename(&temp, path)
}

const fn state(health: Health) -> &'static str {
    match health {
        Health::Green => "green",
        Health::Yellow => "yellow",
        Health::Red => "red",
    }
}

const fn counted_name(counted: Counted) -> &'static str {
    match counted {
        Counted::Streams => "streams",
        Counted::Messages => "messages",
        Counted::Journeys => "journeys",
        Counted::Bytes => "bytes",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Schedule;

    #[test]
    fn a_ticked_schedule_serialises_to_toml_with_records_and_counts() {
        let dir = std::env::temp_dir().join("xmip-report-test");
        std::fs::remove_dir_all(&dir).ok();
        let mut schedule = Schedule::new("xmip:///playground", &dir);
        let snapshot = schedule.tick();

        let text = to_toml("xmip:///playground", &snapshot);
        let parsed: toml::Value = text.parse().expect("valid TOML");

        assert_eq!(parsed["node"].as_str(), Some("xmip:///playground"));
        assert!(!parsed["records"].as_array().expect("records").is_empty());
        assert!(!parsed["counts"].as_array().expect("counts").is_empty());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn history_serialises_to_toml_points() {
        let dir = std::env::temp_dir().join("xmip-report-history-test");
        std::fs::remove_dir_all(&dir).ok();
        let mut schedule = Schedule::new("xmip:///playground", &dir);
        let mut history = History::default();
        history.record(&schedule.tick());
        history.record(&schedule.tick());

        let text = history_toml("xmip:///playground", &history);
        let parsed: toml::Value = text.parse().expect("valid TOML");

        let points = parsed["points"].as_array().expect("points");
        assert!(!points.is_empty());
        assert!(
            points
                .iter()
                .any(|p| p["counted"].as_str() == Some("bytes"))
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn an_atomic_write_lands_the_contents() {
        let path = std::env::temp_dir().join("xmip-report-atomic/snapshot.toml");
        std::fs::remove_dir_all(path.parent().expect("parent")).ok();

        write_atomic(&path, "node = \"x\"\n").expect("write");

        assert_eq!(
            std::fs::read_to_string(&path).expect("read"),
            "node = \"x\"\n"
        );
        std::fs::remove_dir_all(path.parent().expect("parent")).ok();
    }
}
