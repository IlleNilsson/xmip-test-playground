//! The filing scenario: every archive technology, by every contract, files a
//! probe item and gets it back whole.
//!
//! ADR-0028; ADR-0040 sets the boundary. The secretary watches an item age
//! and cross into the archive; the filing watches what the archive does with
//! it. Each tick files one probe item per (technology, contract) through the
//! real store — a Parquet file, a `SQLite` row, a `PostgreSQL` row over the
//! wire, an object in a bucket — restores it from the receipt, and judges the
//! round: what came back equal to what was filed is a pass; anything else, or
//! a store that refused, is a fail with the reason. One health record per
//! `<node>/<technology>/<contract>`, judged over time through a
//! [`Standing`], so a cabinet that lost an item once stays yellow until an
//! operator has seen it.
//!
//! Under pressure the filing skips a cabinet now and then, deterministically,
//! and reports the skip as a fail — an archive fault an operator watches
//! surface, and watches fade.

use std::collections::BTreeMap;
use std::path::PathBuf;

use archive::ArchiveItem;
use observe::{Count, Counted, Snapshot};

use crate::cabinet::{Cabinet, Filed, all_cabinets};
use crate::fault::fires_keyed;
use crate::schedule::CONTRACTS;
use crate::standing::{Mark, Standing};
use crate::support::now_unix_nanos;
use crate::verdict::Contract;

/// How often, in percent of rounds, a pressured filing skips one
/// (technology, contract).
const SKIP_RATE: u8 = 5;

/// One (technology, contract) judged this round: the scope, how it went, the
/// line an operator reads, and the bytes that moved.
struct Judged {
    scope: String,
    mark: Mark,
    line: String,
    filed: bool,
    bytes: u64,
}

/// The filing: one probe item per (technology, contract) a tick, through the
/// real archive stores, judged whole-or-not and folded over time.
pub struct Filing {
    node: String,
    cabinets: Vec<Box<dyn Cabinet>>,
    round: u64,
    under_pressure: bool,
    standings: BTreeMap<String, Standing>,
    filed: u64,
    moved_bytes: u64,
}

impl Filing {
    /// A filing publishing under `node`, over every cabinet, skipping none.
    /// `dir` is where the directory-rooted cabinets keep their files.
    #[must_use]
    pub fn new(node: impl Into<String>, dir: impl Into<PathBuf>) -> Self {
        Self {
            node: node.into(),
            cabinets: all_cabinets(dir),
            round: 0,
            under_pressure: false,
            standings: BTreeMap::new(),
            filed: 0,
            moved_bytes: 0,
        }
    }

    /// The same filing, occasionally skipping a cabinet so faults occur.
    #[must_use]
    pub fn under_pressure(mut self) -> Self {
        self.under_pressure = true;
        self
    }

    /// Drive these cabinets rather than every one. A test that judges the
    /// rollup over many rounds wants the cheap ones; the runner drives all.
    #[must_use]
    pub fn over(mut self, cabinets: Vec<Box<dyn Cabinet>>) -> Self {
        self.cabinets = cabinets;
        self
    }

    /// How many items have been handed to a cabinet so far.
    #[must_use]
    pub const fn filed(&self) -> u64 {
        self.filed
    }

    /// One round: file a probe per (technology, contract), judge each, fold
    /// it into its standing, and publish a health record per scope and the
    /// bytes moved at the node.
    pub fn tick(&mut self) -> Snapshot {
        self.round += 1;
        let now = now_unix_nanos();
        let mut snapshot = Snapshot::new();

        for judged in self.file_round() {
            if judged.filed {
                self.filed += 1;
            }
            self.moved_bytes += judged.bytes;
            let standing = self.standings.entry(judged.scope.clone()).or_default();
            standing.record(judged.mark, judged.line);
            snapshot.record_health(standing.health(&judged.scope, now));
        }

        snapshot.record_count(Count {
            scope: self.node.clone(),
            counted: Counted::Bytes,
            value: self.moved_bytes,
            window_start_unix_nanos: now,
            window_end_unix_nanos: now,
            observed_unix_nanos: now,
        });
        snapshot
    }

    /// Every cabinet by every contract, filed and judged.
    fn file_round(&self) -> Vec<Judged> {
        let mut judged = Vec::with_capacity(self.cabinets.len() * CONTRACTS.len());
        for cabinet in &self.cabinets {
            for &contract in &CONTRACTS {
                judged.push(self.judge(cabinet.as_ref(), contract));
            }
        }
        judged
    }

    /// File one probe through one cabinet and judge it: returned equal to
    /// what was filed is a pass; a skip, a refusal or a difference is a fail
    /// with the reason.
    fn judge(&self, cabinet: &dyn Cabinet, contract: Contract) -> Judged {
        let technology = cabinet.technology();
        let scope = format!("{}/{technology}/{}", self.node, contract.name());
        if self.skipped(technology, contract) {
            return Judged {
                scope,
                mark: Mark::Fail,
                line: "the cabinet was skipped this round: the item was not filed".to_string(),
                filed: false,
                bytes: 0,
            };
        }

        let item = self.probe(contract);
        let size = item.bytes.len() as u64;
        let (mark, line, bytes) = match cabinet.file(item.clone()) {
            Filed::Returned(returned) if returned == item => (
                Mark::Pass,
                format!("{size} bytes filed and returned whole"),
                // In to the archive and back out again.
                size * 2,
            ),
            Filed::Returned(returned) => (
                Mark::Fail,
                format!("returned, but {}", difference(&item, &returned)),
                size,
            ),
            Filed::Failed(why) => (Mark::Fail, why, 0),
        };
        Judged {
            scope,
            mark,
            line,
            filed: true,
            bytes,
        }
    }

    /// The probe item for this round and contract: the contract's own
    /// payload, tagged with where it came from and which round filed it.
    fn probe(&self, contract: Contract) -> ArchiveItem {
        ArchiveItem {
            data_type: contract.name().to_string(),
            identifier: format!("{}-{}", self.round, contract.name()),
            bytes: contract.payload(),
            metadata: vec![
                ("source".to_string(), "playground".to_string()),
                ("round".to_string(), self.round.to_string()),
            ],
        }
    }

    /// Whether this (technology, contract) is skipped this round.
    fn skipped(&self, technology: &str, contract: Contract) -> bool {
        if !self.under_pressure {
            return false;
        }
        fires_keyed(
            SKIP_RATE,
            &format!("skip/{technology}/{}", contract.name()),
            self.round,
        )
    }
}

/// The first field that differs between what was filed and what came back.
fn difference(filed: &ArchiveItem, returned: &ArchiveItem) -> &'static str {
    if filed.bytes != returned.bytes {
        "the bytes differ"
    } else if filed.metadata != returned.metadata {
        "the metadata differs"
    } else if filed.identifier != returned.identifier {
        "the identifier differs"
    } else {
        "the data type differs"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cabinet::{FileCabinet, ParquetCabinet};
    use crate::support::scratch;
    use observe::Health;

    const NODE: &str = "xmip:///playground/filing";

    #[test]
    fn one_tick_files_every_contract_through_every_cabinet_and_rolls_up_green() {
        let dir = scratch("filing");
        let mut filing = Filing::new(NODE, &dir);

        let snapshot = filing.tick();

        let pairs = (all_cabinets(&dir).len() * CONTRACTS.len()) as u64;
        let records = snapshot.health(NODE);
        assert_eq!(records.len() as u64, pairs, "one record per pair");
        assert!(
            records.iter().all(|r| r.health == Health::Fine),
            "every cabinet returns every contract whole: {:?}",
            records.iter().find(|r| r.health != Health::Fine)
        );
        assert_eq!(snapshot.worst(NODE), Some(Health::Fine));
        assert_eq!(
            snapshot.health(&format!("{NODE}/sqlite/json")).len(),
            1,
            "the scope is <node>/<technology>/<contract>"
        );
        assert_eq!(filing.filed(), pairs);
        assert!(
            snapshot
                .measure(NODE, Counted::Bytes)
                .is_some_and(|count| count.value > 0),
            "the bytes moved are published at the node"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn under_pressure_a_skipped_filing_surfaces_as_a_fault() {
        let dir = scratch("filing-pressure");
        let mut filing = Filing::new(NODE, &dir).under_pressure().over(vec![
            Box::new(FileCabinet::new(dir.join("file"))),
            Box::new(ParquetCabinet::new(dir.join("parquet"))),
        ]);

        // A skip is red the round it happens and fades to yellow after, so the
        // proof is that some round went red, not the state of the last one.
        let mut ever_red = false;
        for _ in 0..60 {
            let snapshot = filing.tick();
            // A Done leaf rolls up to Holding at the node (ADR-0041).
            if snapshot.worst(NODE) == Some(Health::Holding) {
                ever_red = true;
                break;
            }
        }
        assert!(ever_red, "a skipped filing must surface as a fault");
        std::fs::remove_dir_all(&dir).ok();
    }
}
