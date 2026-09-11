//! The load scenario: a large payload round-trips whole, and the contract
//! still holds at size.
//!
//! ADR-0028. Pingpong sends a handful of bytes; load sends a large payload — a
//! megabyte by default, gigabytes on demand ([`Load::with_bytes`]) — and asks
//! whether it comes back byte-for-byte and still validates. **Green** when it
//! does, with the throughput; **red** when it is truncated, corrupted, or the
//! transport cannot carry it — a UDP datagram cannot hold a megabyte, and that
//! real ceiling shows as red without any injection. Under pressure the scenario
//! also drops a transfer mid-flight now and then, deterministically. `file` is
//! left clean. At size a byte pattern is sent and integrity is checked without
//! parsing; below the ceiling a valid document is sent and the contract is run.
//!
//! Judged over time like the rest: a pair that carried the load last round but
//! dropped it before reads yellow, not green.
//!
//! At a [`Stress`] level the drop rate scales with it and the payload follows
//! it: the load's own size leads each cycle — the megabyte, or what
//! [`Load::with_bytes`] set — and the level's edge sizes follow, so a hard
//! round proves the transport at the sizes protocols break on as well as at
//! scale. Pairs still run one at a time: peak memory is twice the payload,
//! and at gigabytes that is not a thing to multiply by the cores.

pub mod payload;

use std::collections::BTreeMap;
use std::time::Instant;

use observe::{Count, Counted, Snapshot};

use crate::fault::fires_keyed;
use crate::roundtrip::{Exchange, RoundTrip, all_transports};
use crate::schedule::CONTRACTS;
use crate::standing::{Mark, Standing};
use crate::stress::{self, Stress, scaled_rate};
use crate::support::now_unix_nanos;
use crate::verdict::Contract;

pub use payload::as_stream;
pub(crate) use payload::{filler, large_payload};

/// How often, in percent of rounds at `Realistic`, a pressured transfer is
/// dropped mid-flight.
const DROP_RATE: u8 = 4;

/// The default size of one load, in bytes. A megabyte: large enough that a UDP
/// datagram cannot carry it and a real transfer is measurable, small enough that
/// a loopback round trip stays quick. The runner raises it — gigabytes, on a box
/// with the memory for it — with [`Load::with_bytes`].
const TARGET_BYTES: usize = 1024 * 1024;

/// Above this size the structural contract is not parsed. Proving a JSON or XML
/// contract *holds at size* is worth doing at a few megabytes; parsing a
/// multi-gigabyte document allocates a second copy the size of the payload and
/// proves nothing more. Above the ceiling the claim is byte integrity at scale —
/// it arrived whole — checked without a parse.
const VALIDATE_CEILING: usize = 16 * 1024 * 1024;

/// A scheduled size exercise: every transport by every contract, a payload each
/// round (a megabyte by default, up to gigabytes), judged on whether it survived
/// whole and how fast it moved.
pub struct Load {
    node: String,
    transports: Vec<Box<dyn RoundTrip>>,
    bytes: usize,
    /// The level, when one was set: its drop rate and its sizes. `None` is
    /// the load as it ran before the axis existed — no drops, one size.
    stress: Option<Stress>,
    round: u64,
    standings: BTreeMap<String, Standing>,
    moved_bytes: u64,
    /// How many pairs one round carries, when bounded; the rest wait their
    /// turn. `None` carries every pair every round, as a test wants.
    per_round: Option<usize>,
    /// Where the next round starts in the matrix.
    cursor: usize,
}

impl Load {
    /// A size exercise publishing under `node`, with no injected drops.
    #[must_use]
    pub fn new(node: impl Into<String>, file_dir: impl Into<std::path::PathBuf>) -> Self {
        let transports = all_transports(file_dir);

        Self {
            node: node.into(),
            transports,
            bytes: TARGET_BYTES,
            stress: None,
            round: 0,
            standings: BTreeMap::new(),
            moved_bytes: 0,
            per_round: None,
            cursor: 0,
        }
    }

    /// Carry at most `pairs` pairs a round, the matrix rotating under it, so
    /// a round finishes while an operator watches: a mebibyte over each of
    /// sixteen hundred pairs is a roll's afternoon, not a round (2026-09-11).
    /// Every pair's standing stays on the board between its turns.
    #[must_use]
    pub fn pairs_per_round(mut self, pairs: usize) -> Self {
        self.per_round = Some(pairs.max(1));
        self
    }

    /// The same exercise, dropping the occasional transfer mid-flight:
    /// [`Load::at`] `Realistic`.
    #[must_use]
    pub fn under_pressure(self) -> Self {
        self.at(Stress::Realistic)
    }

    /// The same exercise at a level: drops at the level's rate, and the
    /// payload cycling through the load's own size then the level's.
    #[must_use]
    pub fn at(mut self, stress: Stress) -> Self {
        self.stress = Some(stress);
        self
    }

    /// The payload size this round: the load's own size without a level;
    /// with one, its own size leading a cycle of the level's non-empty sizes.
    fn size_this_round(&self) -> usize {
        let Some(stress) = self.stress else {
            return self.bytes;
        };
        let edges: Vec<usize> = stress
            .sizes()
            .iter()
            .copied()
            .filter(|&size| size > 0)
            .collect();
        let cycle = edges.len() as u64 + 1;
        let at = self.round.saturating_sub(1) % cycle;
        match usize::try_from(at) {
            Ok(0) | Err(_) => self.bytes,
            Ok(at) => edges[at - 1],
        }
    }

    /// Drive these transports rather than every one. The tests judge three;
    /// the runner drives all, and a matrix of hundreds of pairs at size is
    /// the runner's to take its time over, not a test's.
    #[must_use]
    pub fn over(mut self, transports: Vec<Box<dyn RoundTrip>>) -> Self {
        self.transports = transports;
        self
    }

    /// The same exercise at a chosen payload size — the knob that reaches
    /// gigabytes. Peak memory is roughly twice this per pair (the payload and the
    /// copy that comes back), and pairs run one at a time, so the machine needs
    /// about that much free. Above [`VALIDATE_CEILING`] integrity is checked
    /// without parsing the structural contract.
    #[must_use]
    pub fn with_bytes(mut self, bytes: usize) -> Self {
        self.bytes = bytes.max(1);
        self
    }

    /// Run this round's pairs — every one, or the bounded slice — with a
    /// large payload and return the snapshot, every pair's standing on it.
    pub fn tick(&mut self) -> Snapshot {
        self.round += 1;
        let now = now_unix_nanos();
        let mut snapshot = Snapshot::new();

        let matrix = self.transports.len() * CONTRACTS.len();
        let carried = self.per_round.map_or(matrix, |pairs| pairs.min(matrix));
        for step in 0..carried {
            let at = (self.cursor + step) % matrix.max(1);
            let transport = &self.transports[at / CONTRACTS.len()];
            let contract = CONTRACTS[at % CONTRACTS.len()];
            let name = transport.transport();
            let scope = format!("{}/{}/{}", self.node, name, contract.name());
            let line = self.carry(transport.as_ref(), contract);

            let mark = if line.delivered {
                Mark::Pass
            } else if line.one_sided {
                Mark::Warn
            } else {
                Mark::Fail
            };
            if line.delivered {
                self.moved_bytes += line.bytes;
            }
            self.standings
                .entry(scope)
                .or_default()
                .record(mark, line.evidence);
        }
        self.cursor = (self.cursor + carried) % matrix.max(1);

        for (scope, standing) in &self.standings {
            snapshot.record_health(standing.health(scope, now));
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

    /// Carry one load over one transport, and judge it.
    #[allow(clippy::cast_precision_loss)] // display throughput, not arithmetic that must be exact
    fn carry(&self, transport: &dyn RoundTrip, contract: Contract) -> Line {
        let name = transport.transport();
        let size = self.size_this_round();
        if let Some(limit) = transport.ceiling()
            && size > limit
        {
            // Judged, not waited on: the transport says it cannot carry this.
            return Line::failed(format!(
                "{} exceeds the transport's ceiling of {}",
                human(size as f64),
                human(limit as f64)
            ));
        }
        let structural = size <= VALIDATE_CEILING;
        // Below the ceiling: a valid document of the contract's shape, so the
        // contract can be run at size. Above it: a byte pattern, since parsing a
        // gigabyte proves nothing the byte check does not.
        let payload = if structural {
            stress::payload(contract, size)
        } else {
            filler(size)
        };

        let started = Instant::now();
        let exchange = transport.exchange(&payload);
        let elapsed = started.elapsed();

        match exchange {
            Exchange::Returned(back) if back == payload => {
                if self.dropped(name, contract) {
                    return Line::failed(format!(
                        "connection dropped at {} of {}",
                        human((size / 2) as f64),
                        human(size as f64)
                    ));
                }
                // It came back whole. Below the ceiling the contract must still
                // hold at size; above it the byte check is the whole claim.
                if structural && let Err(why) = contract.validate(&as_stream(contract, back)) {
                    return Line::failed(format!("contract not held at size: {why}"));
                }
                let secs = elapsed.as_secs_f64().max(0.000_001);
                let rate = size as f64 / secs;
                let note = if structural { "" } else { " (integrity only)" };
                Line::delivered(
                    size as u64,
                    format!(
                        "{} in {:.1}ms ({}/s){note}",
                        human(size as f64),
                        elapsed.as_secs_f64() * 1000.0,
                        human(rate)
                    ),
                )
            }
            Exchange::Returned(_) => {
                Line::failed("returned bytes did not match — corrupted in transit".to_string())
            }
            Exchange::OneSided(why) => Line::one_sided(why),
            Exchange::Failed(why) => Line::failed(why),
        }
    }

    /// Whether an injected drop hits this pair this round. `file` and `udp` never
    /// get one — file is the clean transport and udp already fails on size.
    fn dropped(&self, transport: &str, contract: Contract) -> bool {
        let Some(stress) = self.stress else {
            return false;
        };
        if transport == "file" || transport == "udp" {
            return false;
        }
        let key = format!("drop/{transport}/{}", contract.name());
        fires_keyed(scaled_rate(DROP_RATE, stress), &key, self.round)
    }
}

/// One round's result for one pair.
struct Line {
    delivered: bool,
    one_sided: bool,
    bytes: u64,
    evidence: String,
}

impl Line {
    fn delivered(bytes: u64, evidence: String) -> Self {
        Self {
            delivered: true,
            one_sided: false,
            bytes,
            evidence,
        }
    }
    fn failed(evidence: String) -> Self {
        Self {
            delivered: false,
            one_sided: false,
            bytes: 0,
            evidence,
        }
    }
    fn one_sided(evidence: String) -> Self {
        Self {
            delivered: false,
            one_sided: true,
            bytes: 0,
            evidence,
        }
    }
}

/// A byte count for display, scaled to B, KB, MB or GB — so a gigabyte load
/// reads as "1.0 GB", not a seven-digit byte count.
fn human(bytes: f64) -> String {
    const KB: f64 = 1024.0;
    const MB: f64 = KB * 1024.0;
    const GB: f64 = MB * 1024.0;
    if bytes >= GB {
        format!("{:.1} GB", bytes / GB)
    } else if bytes >= MB {
        format!("{:.1} MB", bytes / MB)
    } else if bytes >= KB {
        format!("{:.1} KB", bytes / KB)
    } else {
        format!("{bytes:.0} B")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::roundtrip::{FileRoundTrip, TcpRoundTrip, UdpRoundTrip};

    /// The three transports the tests judge: enough to prove a megabyte
    /// carries and that a datagram cannot. The runner drives every one.
    fn sample(dir: &std::path::Path) -> Vec<Box<dyn RoundTrip>> {
        vec![
            Box::new(FileRoundTrip::new(dir)),
            Box::new(TcpRoundTrip),
            Box::new(UdpRoundTrip),
        ]
    }
    use crate::storm::violations;
    use crate::support::scratch;
    use observe::Health;

    #[test]
    fn a_large_payload_round_trips_over_file_and_tcp() {
        let dir = scratch("carry");
        let mut hl = Load::new("xmip:///playground/load", &dir).over(sample(&dir));
        let snapshot = hl.tick();
        assert_eq!(
            snapshot.worst("xmip:///playground/load/file"),
            Some(Health::Fine),
            "file carries a megabyte"
        );
        assert_eq!(
            snapshot.worst("xmip:///playground/load/tcp"),
            Some(Health::Fine),
            "tcp carries a megabyte"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn udp_cannot_carry_a_megabyte_and_says_so() {
        let dir = scratch("udp");
        let mut hl = Load::new("xmip:///playground/load", &dir).over(sample(&dir));
        let snapshot = hl.tick();
        assert_ne!(
            snapshot.worst("xmip:///playground/load/udp"),
            Some(Health::Fine),
            "a datagram cannot hold a megabyte"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn human_scales_from_bytes_to_gigabytes() {
        assert_eq!(human(512.0), "512 B");
        assert_eq!(human(1024.0 * 1024.0), "1.0 MB");
        assert_eq!(human(2.0 * 1024.0 * 1024.0 * 1024.0), "2.0 GB");
    }

    #[test]
    fn filler_is_the_requested_size() {
        assert_eq!(filler(4096).len(), 4096);
    }

    #[test]
    fn above_the_ceiling_the_parse_is_skipped_but_integrity_holds() {
        let dir = scratch("ceiling");
        // Just over the ceiling: a byte pattern, checked whole, no structural
        // parse. Over file only would be ideal, but a tick runs all transports;
        // the size is kept just past the ceiling so the test stays quick.
        let mut hl = Load::new("xmip:///playground/load", &dir)
            .over(sample(&dir))
            .with_bytes(VALIDATE_CEILING + 1);
        let snapshot = hl.tick();
        let file = snapshot
            .health("xmip:///playground/load/file")
            .into_iter()
            .next()
            .expect("a file record");
        assert_eq!(file.health, Health::Fine);
        assert!(
            file.evidence.contains("integrity only"),
            "no parse above the ceiling"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn the_large_payloads_hold_their_contracts() {
        for contract in CONTRACTS {
            let bytes = large_payload(contract, 64 * 1024);
            assert!(bytes.len() >= 64 * 1024);
            let stream = as_stream(contract, bytes);
            assert!(
                contract.validate(&stream).is_ok(),
                "{} must still validate at size",
                contract.name()
            );
        }
    }

    #[test]
    fn bytes_moved_accumulates() {
        let dir = scratch("bytes");
        let mut hl = Load::new("xmip:///playground/load", &dir).over(sample(&dir));
        hl.tick();
        let snapshot = hl.tick();
        let moved = snapshot
            .measure("xmip:///playground/load", Counted::Bytes)
            .map_or(0, |c| c.value);
        assert!(
            moved > 1024 * 1024,
            "several megabytes moved across two ticks"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn at_a_level_the_load_leads_a_cycle_of_the_edge_sizes() {
        let dir = scratch("sizes");
        let mut hl = Load::new("xmip:///playground/load", &dir)
            .at(Stress::Harsh)
            .over(Vec::new());
        // Harsh's zero — the probe — is not a load and is left out.
        let expected = [
            TARGET_BYTES,
            1,
            1_471,
            1_472,
            1_473,
            8_192,
            65_507,
            65_537,
            TARGET_BYTES,
        ];
        for size in expected {
            hl.tick();
            assert_eq!(hl.size_this_round(), size, "round {}", hl.round);
        }
    }

    /// Drive `hl` for `rounds` at a level: every round within the storm's
    /// budget, every record a whole delivery or a reason, the rollup honest,
    /// and `file` — no drops, no ceiling — whole every round.
    fn stress_rounds(hl: &mut Load, rounds: u64) -> Snapshot {
        let mut snapshot = Snapshot::new();
        for round in 1..=rounds {
            let started = Instant::now();
            snapshot = hl.tick();
            let took = started.elapsed();
            let budget = crate::roundtrip::TIMEOUT * 3 * 20 * 3;
            assert!(took <= budget, "round {round} took {took:?}");
            let lying = violations(&snapshot, "xmip:///playground/load");
            assert!(lying.is_empty(), "round {round}: {}", lying.join("; "));
            for record in snapshot.health("xmip:///playground/load/file") {
                assert_eq!(record.health, Health::Fine, "round {round}: {record:?}");
            }
        }
        snapshot
    }

    /// The three transports at Harsh, in two tests rather than one: udp
    /// declares no ceiling yet, so every round above a datagram waits twenty
    /// timeouts, and one full cycle of the level's sizes over all three would
    /// cost the suite two minutes on its own. file and tcp take the whole
    /// cycle; udp takes the rounds that prove the refusal and the carry.
    #[test]
    fn harsh_sizes_arrive_whole_over_file_and_tcp() {
        let dir = scratch("load-harsh");
        let mut hl = Load::new("xmip:///playground/load", &dir)
            .at(Stress::Harsh)
            .over(vec![
                Box::new(FileRoundTrip::new(&dir)),
                Box::new(TcpRoundTrip),
            ]);
        let cycle = 1 + Stress::Harsh.sizes().iter().filter(|&&s| s > 0).count() as u64;
        let snapshot = stress_rounds(&mut hl, cycle);
        // tcp is dropped now and then at three times the rate; every red
        // says so, and nothing is truncated or corrupted.
        for record in snapshot.health("xmip:///playground/load/tcp") {
            assert!(
                matches!(record.health, Health::Fine | Health::Stressed)
                    || record.evidence.contains("dropped"),
                "{record:?}"
            );
        }
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn harsh_udp_refuses_above_a_datagram_with_a_reason_and_carries_below() {
        let dir = scratch("load-harsh-udp");
        let mut hl = Load::new("xmip:///playground/load", &dir)
            .at(Stress::Harsh)
            .over(vec![Box::new(UdpRoundTrip)]);
        // The megabyte, then one byte and the MTU minus one.
        let snapshot = stress_rounds(&mut hl, 3);
        let udp = snapshot.health("xmip:///playground/load/udp");
        assert!(udp.iter().all(|r| !r.evidence.is_empty()));
        assert!(
            udp.iter().all(|r| r.health == Health::Stressed),
            "refused the megabyte, carried the rest: {udp:?}"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    #[ignore = "brutal: every transport at every size, for the runner"]
    fn brutal_sizes_over_every_transport() {
        let dir = scratch("load-brutal");
        let mut hl = Load::new("xmip:///playground/load", &dir).at(Stress::Brutal);
        stress_rounds(&mut hl, Stress::Brutal.rounds());
        std::fs::remove_dir_all(&dir).ok();
    }
}
