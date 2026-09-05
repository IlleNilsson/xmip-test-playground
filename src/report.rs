//! Writing a snapshot where a monitoring UI can read it.
//!
//! The playground and the GUI are two long-running processes in different
//! languages. The simplest durable bridge between them is a file: the playground
//! writes its current snapshot after every tick, the GUI reads it whenever it
//! renders. The write is atomic — a temp file renamed over the target — so the
//! reader never catches a half-written snapshot even after a week of ticks.
//!
//! JSON, not TOML: this is transport, not configuration. Xmip never configures
//! in JSON, and it always moves data in it.

use std::io;
use std::path::Path;

use serde_json::{Value, json};
use xmip_observe::{Counted, Health, Snapshot};

/// The snapshot beneath `node`, as the JSON the GUI's file surface reads:
/// health records and the node's throughput counts.
#[must_use]
pub fn to_json(node: &str, snapshot: &Snapshot) -> String {
    let records: Vec<Value> = snapshot
        .health(node)
        .into_iter()
        .map(|record| {
            json!({
                "scope": record.scope,
                "state": state(record.health),
                "severity": record.severity,
                "evidence": record.evidence,
                "observedUnixNanos": record.observed_unix_nanos,
            })
        })
        .collect();

    let counts: Vec<Value> = [
        Counted::Streams,
        Counted::Messages,
        Counted::Journeys,
        Counted::Bytes,
    ]
    .into_iter()
    .filter_map(|counted| {
        snapshot
            .measure(node, counted)
            .map(|count| json!({ "counted": counted_name(counted), "value": count.value }))
    })
    .collect();

    let document = json!({
        "source": format!("playground — {node}"),
        "node": node,
        "records": records,
        "counts": counts,
    });

    serde_json::to_string_pretty(&document).unwrap_or_else(|_| "{}".to_string())
}

/// Write `contents` to `path` atomically: a sibling temp file, then a rename
/// over the target. A reader either sees the previous snapshot or this one,
/// never a torn write.
///
/// # Errors
///
/// Where the parent could not be created, or the file could not be written or
/// renamed.
pub fn write_atomic(path: &Path, contents: &str) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    let temp = path.with_extension("json.writing");
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
    fn a_ticked_schedule_serialises_to_json_with_records_and_counts() {
        let dir = std::env::temp_dir().join("xmip-report-test");
        std::fs::remove_dir_all(&dir).ok();
        let mut schedule = Schedule::new("xmip:///playground", &dir);
        let snapshot = schedule.tick();

        let json = to_json("xmip:///playground", &snapshot);
        let parsed: Value = serde_json::from_str(&json).expect("valid JSON");

        assert!(!parsed["records"].as_array().expect("records").is_empty());
        assert!(!parsed["counts"].as_array().expect("counts").is_empty());
        assert_eq!(parsed["node"], "xmip:///playground");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn an_atomic_write_lands_the_contents() {
        let path = std::env::temp_dir().join("xmip-report-atomic/snapshot.json");
        std::fs::remove_dir_all(path.parent().expect("parent")).ok();

        write_atomic(&path, "{\"ok\":true}").expect("write");

        assert_eq!(
            std::fs::read_to_string(&path).expect("read"),
            "{\"ok\":true}"
        );
        std::fs::remove_dir_all(path.parent().expect("parent")).ok();
    }
}
