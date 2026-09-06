//! The load scenario: a large payload round-trips whole, and the contract
//! still holds at size.
//!
//! ADR-0028. Pingpong sends a handful of bytes; load sends a megabyte and
//! asks whether it comes back byte-for-byte and still validates. **Green** when
//! it does, with the throughput; **red** when it is truncated, corrupted, or the
//! transport cannot carry it — a UDP datagram cannot hold a megabyte, and that
//! real ceiling shows as red without any injection. Under pressure the scenario
//! also drops a transfer mid-flight now and then, deterministically. `file` is
//! left clean.
//!
//! Judged over time like the rest: a pair that carried the load last round but
//! dropped it before reads yellow, not green.

use std::collections::BTreeMap;
use std::time::Instant;

use observe::{Count, Counted, Health, HealthRecord, Snapshot};
use stream::Stream;
use xcore::StreamId;

use crate::fault::fires_keyed;
use crate::roundtrip::{
    Exchange, FileRoundTrip, HttpRoundTrip, RoundTrip, SmtpRoundTrip, TcpRoundTrip, UdpRoundTrip,
    WebSocketRoundTrip,
};
use crate::schedule::{CONTRACTS, now_unix_nanos};
use crate::verdict::Contract;

/// The size of one heavy load, in bytes. A megabyte: large enough that a UDP
/// datagram cannot carry it and a real transfer is measurable, small enough that
/// a loopback round trip stays quick.
const TARGET_BYTES: usize = 1024 * 1024;

/// One pair's record over time.
#[derive(Clone, Debug, Default)]
struct Tally {
    rounds: u64,
    failures: u64,
    last_delivered: bool,
    last_line: String,
}

/// A scheduled size exercise: every transport by every contract, a megabyte each
/// round, judged on whether it survived whole and how fast it moved.
pub struct Load {
    node: String,
    transports: Vec<Box<dyn RoundTrip>>,
    under_pressure: bool,
    round: u64,
    tallies: BTreeMap<String, Tally>,
    moved_bytes: u64,
}

impl Load {
    /// A size exercise publishing under `node`, with no injected drops.
    #[must_use]
    pub fn new(node: impl Into<String>, file_dir: impl Into<std::path::PathBuf>) -> Self {
        let transports: Vec<Box<dyn RoundTrip>> = vec![
            Box::new(FileRoundTrip::new(file_dir)),
            Box::new(TcpRoundTrip::new()),
            Box::new(HttpRoundTrip::new()),
            Box::new(SmtpRoundTrip::new()),
            Box::new(UdpRoundTrip::new()),
            Box::new(WebSocketRoundTrip::new()),
        ];

        Self {
            node: node.into(),
            transports,
            under_pressure: false,
            round: 0,
            tallies: BTreeMap::new(),
            moved_bytes: 0,
        }
    }

    /// The same exercise, dropping the occasional transfer mid-flight.
    #[must_use]
    pub fn under_pressure(mut self) -> Self {
        self.under_pressure = true;
        self
    }

    /// Run every pair once with a large payload and return the snapshot.
    pub fn tick(&mut self) -> Snapshot {
        self.round += 1;
        let now = now_unix_nanos();
        let mut snapshot = Snapshot::new();

        for transport in &self.transports {
            let name = transport.transport();
            for &contract in &CONTRACTS {
                let scope = format!("{}/{}/{}", self.node, name, contract.name());
                let line = self.carry(transport.as_ref(), contract);

                let tally = self.tallies.entry(scope.clone()).or_default();
                tally.rounds += 1;
                tally.last_delivered = line.delivered;
                tally.last_line.clone_from(&line.evidence);
                if !line.delivered && !line.one_sided {
                    tally.failures += 1;
                }
                if line.delivered {
                    self.moved_bytes += line.bytes;
                }

                snapshot.record_health(health(&scope, tally, now));
            }
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

    /// Carry one large payload over one transport, and judge it.
    #[allow(clippy::cast_precision_loss)] // display throughput, not arithmetic that must be exact
    fn carry(&self, transport: &dyn RoundTrip, contract: Contract) -> Line {
        let name = transport.transport();
        let payload = large_payload(contract, TARGET_BYTES);
        let size = payload.len();

        let started = Instant::now();
        let exchange = transport.exchange(&payload);
        let elapsed = started.elapsed();

        match exchange {
            Exchange::Returned(back) if back == payload => {
                if self.dropped(name, contract) {
                    return Line::failed(format!(
                        "connection dropped at {} of {}",
                        megabytes(size / 2),
                        megabytes(size)
                    ));
                }
                // It came back whole; the contract must still hold at size.
                if let Err(why) = contract.validate(&as_stream(contract, back)) {
                    return Line::failed(format!("contract not held at size: {why}"));
                }
                let secs = elapsed.as_secs_f64().max(0.000_001);
                let rate = (size as f64 / secs) / (1024.0 * 1024.0);
                Line::delivered(
                    size as u64,
                    format!(
                        "{} in {:.1}ms ({rate:.0} MB/s)",
                        megabytes(size),
                        elapsed.as_secs_f64() * 1000.0
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
        if !self.under_pressure || transport == "file" || transport == "udp" {
            return false;
        }
        let key = format!("drop/{transport}/{}", contract.name());
        fires_keyed(4, &key, self.round)
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

fn health(scope: &str, tally: &Tally, now: i64) -> HealthRecord {
    let (health, severity) = if tally.last_delivered && tally.failures == 0 {
        (Health::Green, 0)
    } else if tally.last_delivered {
        (Health::Yellow, 45)
    } else if tally.last_line.contains("only") || tally.last_line.contains("side") {
        (Health::Yellow, 40)
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

#[allow(clippy::cast_precision_loss)] // a size for display
fn megabytes(bytes: usize) -> String {
    format!("{:.1} MB", bytes as f64 / (1024.0 * 1024.0))
}

/// A large, valid payload of `contract`'s shape, at least `target` bytes. Each
/// shape is built so its real contract still holds at size — valid JSON, XML and
/// HTML, not just bytes of the right length.
fn large_payload(contract: Contract, target: usize) -> Vec<u8> {
    match contract {
        Contract::Bytes => (0..target)
            .map(|i| u8::try_from(i % 256).unwrap_or(0))
            .collect(),
        Contract::Text => repeat_to("xmip load ", target).into_bytes(),
        Contract::Json => {
            let mut body = String::from("{\"probe\":\"heavy\",\"n\":[0");
            let mut i = 1u64;
            while body.len() < target {
                body.push(',');
                body.push_str(&i.to_string());
                i += 1;
            }
            body.push_str("]}");
            body.into_bytes()
        }
        Contract::Xml => wrap_to("<probe>", "<i>x</i>", "</probe>", target),
        Contract::Html => wrap_to(
            "<!doctype html><title>xmip</title>",
            "<p>heavy</p>",
            "",
            target,
        ),
    }
}

fn repeat_to(unit: &str, target: usize) -> String {
    let mut out = String::with_capacity(target + unit.len());
    while out.len() < target {
        out.push_str(unit);
    }
    out
}

fn wrap_to(open: &str, unit: &str, close: &str, target: usize) -> Vec<u8> {
    let mut out = String::from(open);
    while out.len() + close.len() < target {
        out.push_str(unit);
    }
    out.push_str(close);
    out.into_bytes()
}

/// Rebuild an arrived large payload into a Stream, for the contract check the
/// caller runs. Kept here so the scenario owns the Stream shape it validates.
#[must_use]
pub fn as_stream(contract: Contract, bytes: Vec<u8>) -> Stream {
    Stream::new(
        StreamId::new(1),
        bytes,
        Some(contract.shape().representation().to_string()),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scratch(name: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("xmip-load-{name}"));
        std::fs::remove_dir_all(&dir).ok();
        dir
    }

    #[test]
    fn a_large_payload_round_trips_over_file_and_tcp() {
        let dir = scratch("carry");
        let mut hl = Load::new("xmip:///playground/load", &dir);
        let snapshot = hl.tick();
        assert_eq!(
            snapshot.worst("xmip:///playground/load/file"),
            Some(Health::Green),
            "file carries a megabyte"
        );
        assert_eq!(
            snapshot.worst("xmip:///playground/load/tcp"),
            Some(Health::Green),
            "tcp carries a megabyte"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn udp_cannot_carry_a_megabyte_and_says_so() {
        let dir = scratch("udp");
        let mut hl = Load::new("xmip:///playground/load", &dir);
        let snapshot = hl.tick();
        assert_ne!(
            snapshot.worst("xmip:///playground/load/udp"),
            Some(Health::Green),
            "a datagram cannot hold a megabyte"
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
        let mut hl = Load::new("xmip:///playground/load", &dir);
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
}
