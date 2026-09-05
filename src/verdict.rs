//! What one round of ping-pong concluded, and how it becomes health.
//!
//! ADR-0028 clause 4: a verdict per (transport, contract) pair, published as
//! health on `xmip:///<node>/exercise/<transport>/<contract>`. Green when the
//! payload went out, came back and matched; red when it did not, with the
//! reason as evidence.

use xmip_observe::{Health, HealthRecord};

/// The content a probe sends and expects back. The matrix's second axis;
/// ADR-0028 exercises every transport by every contract. It starts with the
/// contracts that need no module — raw bytes and UTF-8 text — and grows as the
/// content modules land.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Contract {
    /// Arbitrary bytes, returned unchanged.
    Bytes,
    /// UTF-8 text, returned unchanged.
    Text,
}

impl Contract {
    /// The token as it appears in a scope and a repository name.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Contract::Bytes => "bytes",
            Contract::Text => "text",
        }
    }

    /// The payload this contract sends. What comes back must equal it.
    #[must_use]
    pub fn payload(self) -> Vec<u8> {
        match self {
            Contract::Bytes => vec![0x00, 0x01, 0x02, 0xfd, 0xfe, 0xff],
            Contract::Text => b"xmip ping-pong".to_vec(),
        }
    }
}

/// One round's outcome for one pair.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Verdict {
    pub transport: String,
    pub contract: Contract,
    pub outcome: Outcome,
    /// How many bytes made the round trip. Zero on failure.
    pub bytes: u64,
    pub observed_unix_nanos: i64,
}

/// What happened, and the one line an operator reads first.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Outcome {
    /// Out, back and matched.
    Delivered,
    /// The transport does not support the direction the round trip needs — one
    /// side only. ADR-0028 clause 5: exercised as far as the side allows, and
    /// the verdict says so. Yellow, not red: nothing is broken.
    OneSided(String),
    /// Sent, received and what came back was wrong, or the round trip failed.
    Failed(String),
}

impl Verdict {
    /// The scope this verdict is published under.
    #[must_use]
    pub fn scope(&self, node: &str) -> String {
        format!(
            "{node}/exercise/{}/{}",
            self.transport,
            self.contract.name()
        )
    }

    /// The verdict as a health record for the snapshot.
    #[must_use]
    pub fn health(&self, node: &str) -> HealthRecord {
        let (health, severity, evidence) = match &self.outcome {
            Outcome::Delivered => (
                Health::Green,
                0,
                format!("{} bytes out and back", self.bytes),
            ),
            Outcome::OneSided(why) => (Health::Yellow, 40, why.clone()),
            Outcome::Failed(why) => (Health::Red, 90, why.clone()),
        };

        HealthRecord {
            scope: self.scope(node),
            health,
            severity,
            evidence,
            observed_unix_nanos: self.observed_unix_nanos,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_delivered_verdict_is_green_and_says_how_much_moved() {
        let verdict = Verdict {
            transport: "file".to_string(),
            contract: Contract::Text,
            outcome: Outcome::Delivered,
            bytes: 14,
            observed_unix_nanos: 1,
        };

        let record = verdict.health("xmip:///playground");

        assert_eq!(record.scope, "xmip:///playground/exercise/file/text");
        assert_eq!(record.health, Health::Green);
        assert!(record.evidence.contains("14 bytes"));
    }

    #[test]
    fn a_one_sided_transport_is_yellow_not_red() {
        let verdict = Verdict {
            transport: "mdns".to_string(),
            contract: Contract::Bytes,
            outcome: Outcome::OneSided("receive only".to_string()),
            bytes: 0,
            observed_unix_nanos: 1,
        };

        assert_eq!(verdict.health("xmip:///p").health, Health::Yellow);
    }

    #[test]
    fn a_failure_is_red_and_carries_the_reason() {
        let verdict = Verdict {
            transport: "tcp".to_string(),
            contract: Contract::Bytes,
            outcome: Outcome::Failed("connection refused".to_string()),
            bytes: 0,
            observed_unix_nanos: 1,
        };

        let record = verdict.health("xmip:///p");
        assert_eq!(record.health, Health::Red);
        assert_eq!(record.evidence, "connection refused");
    }
}
