//! Injected faults: the world does not run green. ADR-0028.
//!
//! Loopback never fails, so a Playground with nothing but loopback proves the
//! transports work and proves nothing about the monitoring — an operator never
//! sees a red, never drills to it, never watches it recover. So the Playground
//! injects faults across the message path — **Receive, Process and Send** — of
//! the kinds real integrations actually suffer: **transport** errors (a reset, a
//! timeout, a port in use, a lost datagram), **addressing** errors (an
//! unresolved host, no route, a rejected recipient) and **contract** errors
//! (content that fails its schema). A rule fires for a fraction of rounds, so a
//! pair flickers — yellow when it has failed before and passes now, red the
//! round it fails.
//!
//! Identity faults — a rejected certificate, an expired Let's Encrypt cert, a
//! party not permitted — are not here. They belong to the identity pipeline
//! (`identity.rs`), which faults its own Identification, Authentication and
//! Authorization steps on Receive and its presentation on Send (ADR-0019).
//!
//! Firing is deterministic per (stage, pair, round), not random: the same round
//! faults the same way, so a test can assert it and a run reproduces, while
//! across rounds it looks varied.

use crate::verdict::{Contract, Stage};

/// The kind of transport-and-content fault an operator triages by. Identity
/// faults are not here: they belong to the identity pipeline (`identity.rs`),
/// which surfaces Identification, Authentication and Authorization as their own
/// steps on Receive and Send (ADR-0019, ADR-0033).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FaultKind {
    /// The connection itself: reset, timeout, refused, port in use, lost.
    Transport,
    /// Where it was going: an unresolved host, no route, a bad target.
    Addressing,
    /// The content: it did not hold its contract.
    Contract,
}

impl FaultKind {
    /// The token as it appears in the evidence line.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            FaultKind::Transport => "transport",
            FaultKind::Addressing => "addressing",
            FaultKind::Contract => "contract",
        }
    }
}

/// An injected fault: its kind and the line an operator reads.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Fault {
    pub kind: FaultKind,
    pub reason: &'static str,
}

impl Fault {
    /// The evidence line: the kind, then what happened.
    #[must_use]
    pub fn evidence(self) -> String {
        format!("{} fault — {}", self.kind.name(), self.reason)
    }
}

/// One rule: on a stage, for a transport (or any) and a contract (or any), a
/// fault of a kind fires for `rate` percent of rounds.
pub struct FaultRule {
    stage: Stage,
    transport: Option<&'static str>,
    contract: Option<Contract>,
    rate: u8,
    fault: Fault,
}

impl FaultRule {
    #[must_use]
    const fn new(
        stage: Stage,
        transport: Option<&'static str>,
        contract: Option<Contract>,
        rate: u8,
        kind: FaultKind,
        reason: &'static str,
    ) -> Self {
        Self {
            stage,
            transport,
            contract,
            rate,
            fault: Fault { kind, reason },
        }
    }
}

/// The faults the Playground injects. Empty by default — a bare `Schedule` runs
/// clean, which the tests rely on — and set to a realistic mix by the runner.
pub struct FaultPlan {
    rules: Vec<FaultRule>,
}

impl FaultPlan {
    /// No injected faults.
    #[must_use]
    pub const fn none() -> Self {
        Self { rules: Vec::new() }
    }

    /// Whether this plan injects nothing. The Schedule reads it to decide whether
    /// to run the identity pipeline clean or with faults, so one `with_faults`
    /// call turns on both transport and identity faults together.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.rules.is_empty()
    }

    /// A realistic spread of transport, addressing, authentication and contract
    /// faults over Receive, Process and Send — the four kinds a real integration
    /// suffers, including Let's Encrypt certificate lifecycle faults, which are a
    /// standing source of authentication trouble (a cert expires, a renewal
    /// fails, an ACME challenge cannot reach the host). Rates are low, so the
    /// board is mostly green with faults surfacing over time. `file`'s transport
    /// path carries no rule, so one transport's Receive and Send stay green.
    ///
    /// The length is a data table, not logic — one row per fault — so the
    /// function-length lint is off for it.
    #[must_use]
    #[allow(clippy::too_many_lines)]
    pub fn realistic() -> Self {
        use Contract::{Bytes, Json, Xml};
        use FaultKind::{Addressing, Contract as Held, Transport};
        use Stage::{Process, Receive, Send};

        type Spec = (
            Stage,
            Option<&'static str>,
            Option<Contract>,
            u8,
            FaultKind,
            &'static str,
        );

        let specs: &[Spec] = &[
            (
                Receive,
                Some("udp"),
                None,
                9,
                Transport,
                "the datagram never arrived",
            ),
            (
                Receive,
                Some("tcp"),
                Some(Bytes),
                5,
                Transport,
                "bind failed: the receive port is already in use",
            ),
            (
                Receive,
                Some("smtp"),
                None,
                4,
                Transport,
                "connection reset by peer",
            ),
            (
                Process,
                None,
                Some(Xml),
                6,
                Held,
                "schema validation failed: unexpected element",
            ),
            (
                Process,
                None,
                Some(Json),
                4,
                Held,
                "malformed content: unexpected token",
            ),
            (
                Send,
                Some("http"),
                None,
                5,
                Transport,
                "the send target refused the connection",
            ),
            (
                Send,
                Some("websocket"),
                None,
                3,
                Transport,
                "send timed out",
            ),
            (
                Send,
                Some("http"),
                None,
                5,
                Addressing,
                "the target host did not resolve",
            ),
            (
                Send,
                Some("tcp"),
                None,
                3,
                Addressing,
                "no route to the target host",
            ),
            (
                Send,
                Some("smtp"),
                None,
                3,
                Addressing,
                "recipient address rejected (550 no such user)",
            ),
        ];

        Self {
            rules: specs
                .iter()
                .map(|&(stage, transport, contract, rate, kind, reason)| {
                    FaultRule::new(stage, transport, contract, rate, kind, reason)
                })
                .collect(),
        }
    }

    /// The fault this (stage, transport, contract) suffers this round, if any.
    /// The first matching rule that fires wins.
    #[must_use]
    pub fn fault_for(
        &self,
        stage: Stage,
        transport: &str,
        contract: Contract,
        round: u64,
    ) -> Option<Fault> {
        self.rules
            .iter()
            .filter(|rule| rule.stage == stage)
            .filter(|rule| rule.transport.is_none_or(|only| only == transport))
            .filter(|rule| rule.contract.is_none_or(|only| only == contract))
            .find(|rule| fires(rule.rate, stage, transport, contract, round))
            .map(|rule| rule.fault)
    }
}

impl Default for FaultPlan {
    fn default() -> Self {
        Self::none()
    }
}

/// Whether a rule of the given rate fires for this (stage, pair) this round.
/// Deterministic: a hash of the composed key, taken modulo 100, under the rate.
fn fires(rate: u8, stage: Stage, transport: &str, contract: Contract, round: u64) -> bool {
    let key = format!("{}{transport}{}", stage.name(), contract.name());
    fires_keyed(rate, &key, round)
}

/// Whether a rule of the given rate fires for an arbitrary key this round.
/// Deterministic: a hash of the key's bytes and the round, modulo 100, under the
/// rate. Shared with `identity.rs` so the identity pipeline faults the same way —
/// reproducible, varied across rounds, assertable in a test.
pub(crate) fn fires_keyed(rate: u8, key: &str, round: u64) -> bool {
    if rate == 0 {
        return false;
    }

    let mut hash = round.wrapping_mul(0x9E37_79B9_7F4A_7C15);
    for byte in key.bytes() {
        hash = (hash ^ u64::from(byte)).wrapping_mul(0x0100_0000_01B3);
    }

    (hash % 100) < u64::from(rate)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_faults_never_fire() {
        let plan = FaultPlan::none();
        for round in 0..1000 {
            assert!(
                plan.fault_for(Stage::Receive, "udp", Contract::Bytes, round)
                    .is_none()
            );
        }
    }

    #[test]
    fn faults_land_on_all_three_stages_and_all_three_kinds() {
        let plan = FaultPlan::realistic();
        let mut stages = std::collections::BTreeSet::new();
        let mut kinds = std::collections::BTreeSet::new();

        let transports = ["udp", "tcp", "smtp", "http", "websocket"];
        for round in 0..2000u64 {
            for stage in Stage::ALL {
                for transport in transports {
                    for contract in [Contract::Bytes, Contract::Json, Contract::Xml] {
                        if let Some(fault) = plan.fault_for(stage, transport, contract, round) {
                            stages.insert(stage.name());
                            kinds.insert(fault.kind.name());
                        }
                    }
                }
            }
        }

        assert_eq!(stages.len(), 3, "faults on receive, process and send");
        assert_eq!(kinds.len(), 3, "transport, addressing and contract");
    }

    #[test]
    fn firing_is_deterministic() {
        let plan = FaultPlan::realistic();
        for round in 0..500 {
            let first = plan.fault_for(Stage::Send, "smtp", Contract::Json, round);
            let again = plan.fault_for(Stage::Send, "smtp", Contract::Json, round);
            assert_eq!(first, again);
        }
    }

    #[test]
    fn file_has_no_transport_faults() {
        // file's transport path — Receive and Send — never faults; it is the
        // reliable local transport. Process (content) faults are transport-
        // agnostic and can still hit it, which is realistic: bad content is bad
        // over any transport.
        let plan = FaultPlan::realistic();
        for round in 0..2000 {
            for stage in [Stage::Receive, Stage::Send] {
                for contract in [Contract::Bytes, Contract::Text, Contract::Json] {
                    assert!(plan.fault_for(stage, "file", contract, round).is_none());
                }
            }
        }
    }

    #[test]
    fn the_evidence_names_the_kind() {
        let fault = Fault {
            kind: FaultKind::Addressing,
            reason: "the target host did not resolve",
        };
        assert_eq!(
            fault.evidence(),
            "addressing fault — the target host did not resolve"
        );
    }
}
