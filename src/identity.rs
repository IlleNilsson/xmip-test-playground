//! The identity pipeline a Receive Location runs, and what a Send Location
//! presents.
//!
//! ADR-0019: **Identification → Authentication → Authorization**, invariant
//! across security technologies. ADR-0033: certificates on Receive and Send. A
//! Receive Location must identify who is claimed, authenticate the claim, and
//! authorize what it may do; a Send Location must present an identity to the far
//! end.
//!
//! The playground drives the estate's **real** gates — `identify_transport`,
//! `authenticate`, `authorize` — with stand-in implementors, so a fault is a
//! genuine `Refusal` or `Decision::Denied`, not a fabricated string. The
//! mechanism is mutual-TLS throughout (ADR-0033's first mechanism). A fault
//! feeds the gate the input that makes it truly refuse: an arrival with no
//! certificate, an authenticator that will not prove, a policy that denies.

use authenticate::{
    Acceptance, AuthenticateError, Authenticator, PartyRegistry, Refusal, authenticate,
};
use authorize::{Action, Attempt, Authorizer, Decision, authorize};
use context::{Alignment, AuthenticatedIdentity, IdentityFacts, OnMisalignment, Verified};
use identify::{IdentifyError, Presented, StreamArrival, TransportIdentifier, identify_transport};
use xcore::{Arriving, Layer, Mechanism, PartyId, Purpose, mechanism};

use crate::fault::fires_keyed;
use crate::verdict::{Contract, Outcome};

/// The subject a well-formed arrival claims, and the Party it resolves to.
const SUBJECT: &str = "CN=partner-x.example";
fn partner() -> PartyId {
    PartyId::new(1)
}

/// The steps a Receive Location runs, in order. Each is its own scope so an
/// operator drills to the one that failed. ADR-0019.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Step {
    Identification,
    Authentication,
    Authorization,
}

impl Step {
    /// The three, in message-path order.
    pub const RECEIVE: [Step; 3] = [
        Step::Identification,
        Step::Authentication,
        Step::Authorization,
    ];

    /// The token as it appears in the scope: `receive/<t>/<c>/<step>`.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Step::Identification => "identification",
            Step::Authentication => "authentication",
            Step::Authorization => "authorization",
        }
    }
}

/// Reads a mutual-TLS subject out of the arrival. A well-formed arrival carries
/// a `peer.subject`; an identification fault carries none, and `None` is the
/// honest answer — nothing was presented to identify.
struct SubjectIdentifier;

impl TransportIdentifier for SubjectIdentifier {
    fn mechanism(&self) -> Mechanism {
        mechanism::mutual_tls()
    }

    fn identify(&self, arrival: &StreamArrival<'_>) -> Result<Option<Presented>, IdentifyError> {
        Ok(arrival
            .property("peer.subject")
            .map(|subject| Presented::passed(mechanism::mutual_tls(), subject)))
    }
}

/// Proves a mutual-TLS subject. `honest` proves it; otherwise it refuses, which
/// is what a rejected or expired certificate does at the handshake.
struct TlsAuthenticator {
    honest: bool,
}

impl Authenticator for TlsAuthenticator {
    fn mechanism(&self) -> Mechanism {
        mechanism::mutual_tls()
    }

    fn verify(&self, _presented: &Presented) -> Result<Verified, AuthenticateError> {
        Ok(if self.honest {
            Verified::Proven
        } else {
            Verified::Refused
        })
    }
}

/// Resolves the subject to the partner Party. The Receive Location knows this
/// partner.
struct KnownPartner;

impl PartyRegistry for KnownPartner {
    fn resolve(&self, _mechanism: &str, _purpose: Purpose, _value: &str) -> Option<PartyId> {
        Some(partner())
    }
}

/// Allows the action on this location. `permits` allows; otherwise it denies —
/// the Party is proven but not permitted here.
struct LocationPolicy {
    permits: bool,
}

impl Authorizer for LocationPolicy {
    // The trait fixes the signature to `-> &str`; the name is a literal, and an
    // impl cannot widen the lifetime the trait elided.
    #[allow(clippy::unnecessary_literal_bound)]
    fn name(&self) -> &str {
        "playground-location"
    }

    fn layer(&self) -> Layer {
        Layer::Transport
    }

    fn decide(&self, _identity: &IdentityFacts, attempt: &Attempt) -> Option<Decision> {
        Some(if self.permits {
            Decision::Allowed
        } else {
            Decision::denied(
                "playground-location",
                format!("the party is not permitted to {} here", attempt.action),
            )
        })
    }
}

/// The identity faults the playground injects, or none. Mirrors `FaultPlan` for
/// the identity pipeline: certificate-lifecycle and policy faults on the three
/// Receive steps and on Send presentation. `file` is exempt, so one transport's
/// identity stays green, exactly as its transport does.
pub struct IdentityFaults {
    enabled: bool,
}

impl IdentityFaults {
    /// No injected identity faults — every step proves and is allowed.
    #[must_use]
    pub const fn none() -> Self {
        Self { enabled: false }
    }

    /// A realistic spread of identity faults across the pipeline.
    #[must_use]
    pub const fn realistic() -> Self {
        Self { enabled: true }
    }

    /// The Receive step this (transport, contract) fails this round, if any, and
    /// the operator-facing reason. The first rule that fires wins.
    fn receive_fault(
        &self,
        transport: &str,
        contract: Contract,
        round: u64,
    ) -> Option<(Step, &'static str)> {
        const RULES: &[(Step, &str, u8, &str)] = &[
            (
                Step::Identification,
                "http",
                3,
                "no client certificate presented",
            ),
            (
                Step::Authentication,
                "http",
                4,
                "client certificate rejected",
            ),
            (
                Step::Authentication,
                "http",
                3,
                "Let's Encrypt certificate expired; renewal pending",
            ),
            (
                Step::Authentication,
                "smtp",
                3,
                "STARTTLS certificate not trusted",
            ),
            (
                Step::Authorization,
                "tcp",
                3,
                "the party is not permitted on this Receive Location",
            ),
        ];

        if !self.enabled || transport == "file" {
            return None;
        }

        for &(step, only, rate, reason) in RULES {
            if only == transport && fires_keyed(rate, &key(step.name(), transport, contract), round)
            {
                return Some((step, reason));
            }
        }
        None
    }

    /// The reason Send cannot present an identity this round, if any.
    fn send_fault(&self, transport: &str, contract: Contract, round: u64) -> Option<&'static str> {
        const RULES: &[(&str, u8, &str)] = &[
            ("http", 4, "no certificate to present"),
            ("smtp", 3, "the presented certificate has expired"),
            (
                "websocket",
                3,
                "ACME renewal pending; cannot present a certificate",
            ),
        ];

        if !self.enabled || transport == "file" {
            return None;
        }

        for &(only, rate, reason) in RULES {
            if only == transport
                && fires_keyed(rate, &key("send-identity", transport, contract), round)
            {
                return Some(reason);
            }
        }
        None
    }
}

fn key(step: &str, transport: &str, contract: Contract) -> String {
    format!("{step}/{transport}/{}", contract.name())
}

/// Run the Receive Location's identity pipeline for one (transport, contract)
/// this round. Returns an outcome per step in order. The pipeline stops at the
/// first failure: earlier steps ran for real and passed, the failing step
/// carries the reason, and the rest report "not reached".
#[must_use]
pub fn receive(
    faults: &IdentityFaults,
    transport: &str,
    contract: Contract,
    round: u64,
) -> Vec<(Step, Outcome)> {
    let faulted = faults.receive_fault(transport, contract, round);
    let mut out = Vec::with_capacity(3);

    // Identification.
    let subject_present = faulted.is_none_or(|(step, _)| step != Step::Identification);
    let claims = run_identification(subject_present);
    let Some(presented) = claims.first() else {
        let reason = faulted.map_or("no identity was presented", |(_, reason)| reason);
        out.push((Step::Identification, Outcome::Failed(reason.to_string())));
        out.push((Step::Authentication, not_reached(Step::Identification)));
        out.push((Step::Authorization, not_reached(Step::Identification)));
        return out;
    };
    out.push((Step::Identification, Outcome::Delivered));

    // Authentication.
    let honest = faulted.is_none_or(|(step, _)| step != Step::Authentication);
    let identity = match run_authentication(presented, honest) {
        Ok(identity) => {
            out.push((Step::Authentication, Outcome::Delivered));
            identity
        }
        Err(refusal) => {
            out.push((
                Step::Authentication,
                Outcome::Failed(reason_or(faulted, Step::Authentication, &refusal)),
            ));
            out.push((Step::Authorization, not_reached(Step::Authentication)));
            return out;
        }
    };

    // Authorization.
    let permits = faulted.is_none_or(|(step, _)| step != Step::Authorization);
    let decision = run_authorization(identity, Action::Receive, permits);
    if decision.allowed() {
        out.push((Step::Authorization, Outcome::Delivered));
    } else {
        out.push((
            Step::Authorization,
            Outcome::Failed(reason_or(faulted, Step::Authorization, &decision)),
        ));
    }
    out
}

/// The Send Location presents an identity to the far end (ADR-0033: a Send
/// Location presents a client certificate). Delivered when Xmip holds a
/// credential and is authorized to present it here; Failed when it has none, the
/// one it has is unusable, or presenting it is not permitted.
#[must_use]
pub fn send(faults: &IdentityFaults, transport: &str, contract: Contract, round: u64) -> Outcome {
    if let Some(reason) = faults.send_fault(transport, contract, round) {
        return Outcome::Failed(reason.to_string());
    }

    let presented = Presented::passed(mechanism::mutual_tls(), SUBJECT);
    match run_authentication(&presented, true) {
        Ok(identity) => {
            let decision = run_authorization(identity, Action::Send, true);
            if decision.allowed() {
                Outcome::Delivered
            } else {
                Outcome::Failed(decision.to_string())
            }
        }
        Err(refusal) => Outcome::Failed(refusal.to_string()),
    }
}

/// The scenario reason when this step is the faulted one, else what the real
/// gate said. A fault names the operator-facing cause; the gate still genuinely
/// refused, which is what proves the pipeline is wired.
fn reason_or(
    faulted: Option<(Step, &'static str)>,
    step: Step,
    gate: &dyn std::fmt::Display,
) -> String {
    faulted
        .filter(|(faulted_step, _)| *faulted_step == step)
        .map_or_else(|| gate.to_string(), |(_, reason)| reason.to_string())
}

fn not_reached(after: Step) -> Outcome {
    Outcome::OneSided(format!("not reached — {} did not pass", after.name()))
}

fn run_identification(subject_present: bool) -> Vec<Presented> {
    let stream = Contract::Text.stream();
    let properties: Vec<(String, String)> = if subject_present {
        vec![("peer.subject".to_string(), SUBJECT.to_string())]
    } else {
        Vec::new()
    };
    let arrival = StreamArrival::new(&stream, Arriving::Pushed, "xmip:///playground", &properties);
    let identifier = SubjectIdentifier;
    let identifiers: [&dyn TransportIdentifier; 1] = [&identifier];
    identify_transport(&identifiers, &arrival).unwrap_or_default()
}

fn run_authentication(
    presented: &Presented,
    honest: bool,
) -> Result<AuthenticatedIdentity, Refusal> {
    let acceptance = Acceptance::closed()
        .accepting(&mechanism::mutual_tls())
        .from_party(partner());
    let authenticator = TlsAuthenticator { honest };
    let authenticators: [&dyn Authenticator; 1] = [&authenticator];
    let registry = KnownPartner;
    authenticate(&acceptance, &authenticators, &registry, presented)
}

fn run_authorization(identity: AuthenticatedIdentity, action: Action, permits: bool) -> Decision {
    let facts = IdentityFacts::evaluate(Alignment::None, identity, None);
    let policy = LocationPolicy { permits };
    let policies: [&dyn Authorizer; 1] = [&policy];
    let attempt = Attempt::new(action, "partner-x");
    authorize(&policies, &facts, &attempt, OnMisalignment::Accept)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_clean_receive_passes_all_three_steps() {
        let steps = receive(&IdentityFaults::none(), "http", Contract::Json, 1);
        assert_eq!(steps.len(), 3);
        assert!(
            steps
                .iter()
                .all(|(_, outcome)| *outcome == Outcome::Delivered),
            "every step delivered on the happy path"
        );
    }

    #[test]
    fn a_clean_send_presents_identity() {
        assert_eq!(
            send(&IdentityFaults::none(), "http", Contract::Json, 1),
            Outcome::Delivered
        );
    }

    #[test]
    fn a_faulted_step_fails_and_the_rest_are_not_reached() {
        // Find a round where http suffers an identity fault, then assert the
        // pipeline stops at it.
        let faults = IdentityFaults::realistic();
        let mut seen_stop = false;
        for round in 0..500 {
            let steps = receive(&faults, "http", Contract::Json, round);
            if let Some(index) = steps
                .iter()
                .position(|(_, o)| matches!(o, Outcome::Failed(_)))
            {
                seen_stop = true;
                for (_, outcome) in &steps[index + 1..] {
                    assert!(
                        matches!(outcome, Outcome::OneSided(_)),
                        "steps after a failure are not reached"
                    );
                }
            }
        }
        assert!(
            seen_stop,
            "http should suffer an identity fault within 500 rounds"
        );
    }

    #[test]
    fn file_never_faults_its_identity() {
        let faults = IdentityFaults::realistic();
        for round in 0..2000 {
            for contract in [Contract::Bytes, Contract::Json, Contract::Xml] {
                let steps = receive(&faults, "file", contract, round);
                assert!(steps.iter().all(|(_, o)| *o == Outcome::Delivered));
                assert_eq!(send(&faults, "file", contract, round), Outcome::Delivered);
            }
        }
    }

    #[test]
    fn identity_faults_are_deterministic() {
        let faults = IdentityFaults::realistic();
        for round in 0..500 {
            assert_eq!(
                receive(&faults, "smtp", Contract::Json, round),
                receive(&faults, "smtp", Contract::Json, round)
            );
        }
    }
}
