//! Small helpers shared across the scenarios: the wall clock every scenario
//! stamps its records with, and — for tests — a scratch directory.
//!
//! `now_unix_nanos` lived in `schedule.rs` and five other scenarios reached into
//! it; it belongs in a neutral place, not in one scenario's module.

use std::time::{SystemTime, UNIX_EPOCH};

/// Now, in unix nanoseconds, saturating rather than failing before the epoch or
/// past `i64`.
#[must_use]
pub(crate) fn now_unix_nanos() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |since| {
            i64::try_from(since.as_nanos()).unwrap_or(i64::MAX)
        })
}

/// A fresh, empty scratch directory for a test, unique per name and run so
/// parallel tests never collide. The test removes it when done.
#[cfg(test)]
pub(crate) fn scratch(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("xmip-play-{name}-{}", now_unix_nanos()));
    std::fs::remove_dir_all(&dir).ok();
    dir
}
