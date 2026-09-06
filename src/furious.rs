//! The furious scenario: round-trip latency under a budget.
//!
//! ADR-0028. Pingpong asks *did it arrive whole*; furious asks *did it
//! arrive in time*. One small message per (transport, contract) each round,
//! timed end to end, kept in a bounded ring so the health is a percentile over
//! recent rounds, not one lucky tick: **green** while p99 is within the
//! transport's budget, **yellow** while it creeps over, **red** when it blows it
//! or the round trip fails.
//!
//! Loopback is fast, so nothing breaches a budget on its own and the board would
//! prove nothing. Under pressure the scenario injects latency spikes — a GC
//! pause, a re-established connection, a TLS renegotiation — deterministically
//! per (transport, contract, round), so p99 rises and an operator sees it. `file`
//! is left clean, the one transport that stays fast.

use std::collections::BTreeMap;
use std::time::Instant;

use observe::{Health, HealthRecord, Snapshot};

use crate::fault::fires_keyed;
use crate::roundtrip::{Exchange, RoundTrip, all_transports};
use crate::schedule::CONTRACTS;
use crate::support::now_unix_nanos;
use crate::verdict::Contract;

/// How many recent latencies each pair keeps for its percentiles.
const RING: usize = 256;

/// Rounds skipped before latencies are recorded. The first round pays cold-start
/// costs — sockets bound, a TLS session set up — that are not steady-state
/// latency and would pin a percentile high for the life of the ring.
const WARMUP: u64 = 3;

/// A bounded ring of latencies in microseconds, newest overwriting oldest.
#[derive(Clone, Debug, Default)]
struct Ring {
    samples: Vec<u64>,
    next: usize,
}

impl Ring {
    fn record(&mut self, micros: u64) {
        if self.samples.len() < RING {
            self.samples.push(micros);
        } else {
            self.samples[self.next] = micros;
            self.next = (self.next + 1) % RING;
        }
    }

    /// The `p`-th percentile (0..=100) of what is held, in microseconds.
    fn percentile(&self, p: u64) -> u64 {
        if self.samples.is_empty() {
            return 0;
        }
        let mut sorted = self.samples.clone();
        sorted.sort_unstable();
        let rank = ((p * (sorted.len() as u64 - 1)) / 100) as usize;
        sorted[rank]
    }

    fn len(&self) -> usize {
        self.samples.len()
    }
}

/// The latency each transport is expected to stay under, in microseconds. Real
/// loopback beats these by an order of magnitude; they are the line an injected
/// spike crosses.
fn budget_micros(transport: &str) -> u64 {
    // Generous next to real loopback (well under a millisecond), so ordinary
    // jitter — disk contention on file, scheduler hiccups — stays green and only
    // an injected spike, three times the budget, crosses the line.
    match transport {
        "file" | "udp" | "tcp" => 25_000,
        "websocket" | "http" => 30_000,
        _ => 40_000,
    }
}

/// A scheduled latency exercise: every transport by every contract, timed each
/// round, judged on the percentile of recent rounds.
pub struct Furious {
    node: String,
    transports: Vec<Box<dyn RoundTrip>>,
    under_pressure: bool,
    round: u64,
    rings: BTreeMap<String, Ring>,
}

impl Furious {
    /// A latency exercise publishing under `node`, with no injected spikes.
    #[must_use]
    pub fn new(node: impl Into<String>, file_dir: impl Into<std::path::PathBuf>) -> Self {
        let transports = all_transports(file_dir);

        Self {
            node: node.into(),
            transports,
            under_pressure: false,
            round: 0,
            rings: BTreeMap::new(),
        }
    }

    /// The same exercise, injecting realistic latency spikes. The runner uses
    /// it; the tests use the spike-free default.
    #[must_use]
    pub fn under_pressure(mut self) -> Self {
        self.under_pressure = true;
        self
    }

    /// Run every pair once, time it, fold it into the pair's ring, and return the
    /// snapshot to publish.
    pub fn tick(&mut self) -> Snapshot {
        self.round += 1;
        let now = now_unix_nanos();
        let mut snapshot = Snapshot::new();

        for transport in &self.transports {
            let name = transport.transport();
            for &contract in &CONTRACTS {
                let scope = format!("{}/{}/{}", self.node, name, contract.name());
                let payload = contract.stream().bytes().to_vec();

                let started = Instant::now();
                let exchange = transport.exchange(&payload);
                let measured = u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX);

                let record = match exchange {
                    Exchange::Returned(back) if back == payload => {
                        if self.round > WARMUP {
                            let sample = self.sample(name, contract, measured);
                            self.rings.entry(scope.clone()).or_default().record(sample);
                        }
                        self.health(&scope, name, now)
                    }
                    Exchange::OneSided(why) => yellow(&scope, why, now),
                    Exchange::Returned(_) => {
                        red(&scope, "what came back did not match".to_string(), now)
                    }
                    Exchange::Failed(why) => red(&scope, why, now),
                };
                snapshot.record_health(record);
            }
        }

        snapshot
    }

    /// The latency to record: the real measurement, or an injected spike three
    /// times the budget when this round is under pressure. `file` never spikes.
    fn sample(&self, transport: &str, contract: Contract, measured: u64) -> u64 {
        if self.under_pressure && transport != "file" {
            let key = format!("spike/{transport}/{}", contract.name());
            if fires_keyed(4, &key, self.round) {
                return budget_micros(transport) * 3;
            }
        }
        measured
    }

    /// The pair's health from the percentiles of its ring against the budget.
    fn health(&self, scope: &str, transport: &str, now: i64) -> HealthRecord {
        let ring = self.rings.get(scope);
        let (p50, p99, n) = ring.map_or((0, 0, 0), |ring| {
            (ring.percentile(50), ring.percentile(99), ring.len())
        });
        let budget = budget_micros(transport);

        if n == 0 {
            return HealthRecord {
                scope: scope.to_string(),
                health: Health::Fine,
                severity: 0,
                evidence: "warming up".to_string(),
                observed_unix_nanos: now,
            };
        }

        let (health, severity) = if p99 <= budget {
            (Health::Fine, 0)
        } else if p99 <= budget * 2 {
            (Health::Average, 45)
        } else {
            (Health::Done, 90)
        };

        HealthRecord {
            scope: scope.to_string(),
            health,
            severity,
            evidence: format!(
                "p50 {} p99 {} (budget {}) over {n}",
                millis(p50),
                millis(p99),
                millis(budget)
            ),
            observed_unix_nanos: now,
        }
    }
}

#[allow(clippy::cast_precision_loss)] // a latency for display
fn millis(micros: u64) -> String {
    format!("{:.1}ms", micros as f64 / 1000.0)
}

fn yellow(scope: &str, why: String, now: i64) -> HealthRecord {
    HealthRecord {
        scope: scope.to_string(),
        health: Health::Average,
        severity: 40,
        evidence: why,
        observed_unix_nanos: now,
    }
}

fn red(scope: &str, why: String, now: i64) -> HealthRecord {
    HealthRecord {
        scope: scope.to_string(),
        health: Health::Done,
        severity: 90,
        evidence: why,
        observed_unix_nanos: now,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::support::scratch;

    #[test]
    fn a_clean_run_stays_within_budget_and_is_green() {
        let dir = scratch("clean");
        let mut ff = Furious::new("xmip:///playground/furious", &dir);
        let mut snapshot = ff.tick();
        for _ in 0..15 {
            snapshot = ff.tick();
        }
        assert_eq!(
            snapshot.worst("xmip:///playground/furious"),
            Some(Health::Fine),
            "loopback beats every budget with no spikes"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn spikes_push_a_pair_over_its_budget() {
        let dir = scratch("spikes");
        let mut ff = Furious::new("xmip:///playground/furious", &dir).under_pressure();
        let mut snapshot = ff.tick();
        for _ in 0..80 {
            snapshot = ff.tick();
        }
        assert_eq!(
            snapshot.worst("xmip:///playground/furious"),
            Some(Health::Holding),
            "injected spikes should drive p99 past a budget — a Done rolls up to Holding (ADR-0041)"
        );
        // file never spikes: it stays green.
        assert_eq!(
            snapshot.worst("xmip:///playground/furious/file"),
            Some(Health::Fine),
            "file is left fast"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn the_ring_reports_percentiles_and_stays_bounded() {
        let mut ring = Ring::default();
        for value in 0..1000 {
            ring.record(value);
        }
        assert_eq!(ring.len(), RING, "the ring is bounded");
        // The last RING values are 744..=999; the median of those is ~871.
        assert!(ring.percentile(50) >= 744);
        assert!(ring.percentile(99) >= ring.percentile(50));
    }
}
