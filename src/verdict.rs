//! What one round of ping-pong concluded, and how it becomes health.
//!
//! ADR-0028 clause 4: a verdict per (transport, contract) pair, published as
//! health on `xmip:///<node>/exercise/<transport>/<contract>`. Green when the
//! payload went out, came back and matched; red when it did not, with the
//! reason as evidence.

use contract::Contract as ContractTrait;
use observe::{Health, HealthRecord};
use stream::Stream;
use xcore::StreamId;

use crate::contracts::{ContentContract, Shape};

/// The content a probe sends and expects back. The matrix's second axis;
/// ADR-0028 exercises every transport by every contract. Each carries an actual
/// [`Stream`] and a real [`ContentContract`] that must hold on arrival — bytes
/// with no structural claim, UTF-8 text, well-formed JSON and XML, and HTML
/// markup. It grows as the estate's own content modules land. A representation is
/// not a transport (ADR-0010): each rides the contract axis and is exercised
/// over every transport at once.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Contract {
    /// Arbitrary bytes, returned unchanged. No structural claim.
    Bytes,
    /// UTF-8 text.
    Text,
    /// Well-formed JSON.
    Json,
    /// Well-formed XML.
    Xml,
    /// HTML markup.
    Html,
    /// CSV rows, the `csv` contract technology.
    Csv,
    /// Fixed-width records, the `fixed-width` contract technology.
    FixedWidth,
    /// A UN/EDIFACT interchange, the `edi-edifact` contract technology.
    Edifact,
    /// Text held by a pattern, the `regex` contract technology.
    Regex,
    /// XML held by rules, the `schematron` contract technology.
    Schematron,
    /// An HL7 v2 message, the `hl7-v2` contract technology.
    Hl7v2,
    /// A FHIR resource, the `fhir` contract technology.
    Fhir,
}

/// One ADT^A01 admission, HL7 v2.5: carriage returns between segments, as HL7
/// writes them, and none at the end. A bare CR is not a line ending to mail, so
/// SMTP returns it byte for byte.
pub const HL7_PROBE: &[u8] = concat!(
    "MSH|^~\\&|LAB|HOSP|EMR|CLINIC|20260908103000||ADT^A01|PROBE1|P|2.5\r",
    "PID|1||123456^^^HOSP^MR||DOE^JOHN||19800101|M"
)
.as_bytes();

/// One sound ORDERS interchange, D96A: the EDIFACT probe.
pub const EDIFACT_PROBE: &[u8] = b"UNA:+.? 'UNB+UNOC:3+SENDER+RECEIVER+260907:1345+REF001'\
UNH+1+ORDERS:D:96A:UN'BGM+220+PO4711'DTM+137:20260907:102'LIN+1++X001:SA'QTY+21:2'\
UNT+6+1'UNZ+1+REF001'";

impl Contract {
    /// The token as it appears in a scope and a repository name.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Contract::Bytes => "bytes",
            Contract::Text => "text",
            Contract::Json => "json",
            Contract::Xml => "xml",
            Contract::Html => "html",
            Contract::Csv => "csv",
            Contract::FixedWidth => "fixed-width",
            Contract::Edifact => "edi-edifact",
            Contract::Regex => "regex",
            Contract::Schematron => "schematron",
            Contract::Hl7v2 => "hl7-v2",
            Contract::Fhir => "fhir",
        }
    }

    /// The content shape this contract holds its Stream to.
    #[must_use]
    pub const fn shape(self) -> Shape {
        match self {
            Contract::Bytes => Shape::Bytes,
            Contract::Text => Shape::Text,
            Contract::Json => Shape::Json,
            Contract::Xml => Shape::Xml,
            Contract::Html => Shape::Html,
            Contract::Csv => Shape::Csv,
            Contract::FixedWidth => Shape::FixedWidth,
            Contract::Edifact => Shape::Edifact,
            Contract::Regex => Shape::Regex,
            Contract::Schematron => Shape::Schematron,
            Contract::Hl7v2 => Shape::Hl7v2,
            Contract::Fhir => Shape::Fhir,
        }
    }

    /// The payload this contract sends. What comes back must equal it, and the
    /// contract must hold over it.
    #[must_use]
    pub fn payload(self) -> Vec<u8> {
        match self {
            Contract::Bytes => vec![0x00, 0x01, 0x02, 0xfd, 0xfe, 0xff],
            Contract::Text => b"xmip ping-pong".to_vec(),
            Contract::Json => br#"{"probe":"ping-pong","n":1}"#.to_vec(),
            Contract::Xml => b"<probe><n>1</n>ping-pong</probe>".to_vec(),
            Contract::Html => b"<!doctype html><title>xmip</title><p>ping-pong".to_vec(),
            // CRLF and no trailing break: mail carries lines, and a bare LF or
            // a final newline would not survive SMTP byte for byte.
            Contract::Csv => b"probe,n\r\n\"ping,pong\",1".to_vec(),
            Contract::FixedWidth => b"A00001ACME      02\r\nA00002BOLT      01".to_vec(),
            Contract::Edifact => EDIFACT_PROBE.to_vec(),
            Contract::Regex => b"PROBE-4711 ping-pong".to_vec(),
            Contract::Schematron => b"<probe xmlns=\"urn:xmip:probe\"><n>1</n></probe>".to_vec(),
            Contract::Hl7v2 => HL7_PROBE.to_vec(),
            Contract::Fhir => br#"{"resourceType":"Patient","id":"probe-1"}"#.to_vec(),
        }
    }

    /// The actual Stream a probe sends: the payload, tagged with the media type
    /// the contract declares.
    #[must_use]
    pub fn stream(self) -> Stream {
        Stream::new(
            StreamId::new(1),
            self.payload(),
            Some(self.shape().representation().to_string()),
        )
    }

    /// Run the real contract over an arrived Stream. `Ok` when it held; `Err`
    /// with the first issue when it did not.
    ///
    /// # Errors
    ///
    /// When the Stream is not identified as this contract's, or fails validation.
    pub fn validate(self, arrived: &Stream) -> Result<(), String> {
        let contract = ContentContract::new(self.name(), self.shape());

        match contract.identify(arrived) {
            Ok(true) => {}
            Ok(false) => return Err("the arrived Stream is not this contract's".to_string()),
            Err(error) => return Err(format!("identify failed: {error}")),
        }

        match contract.validate(arrived) {
            Ok(result) if result.valid => Ok(()),
            Ok(result) => Err(result
                .issues
                .first()
                .map_or_else(|| "invalid".to_string(), |issue| issue.message.clone())),
            Err(error) => Err(format!("validation failed: {error}")),
        }
    }
}

/// A stage of the message path, the axis an operator drills first. A pingpong
/// round drives all three: Receive takes the Stream in, Process holds the
/// contract over it, Send delivers it back out.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Stage {
    Receive,
    Process,
    Send,
}

impl Stage {
    /// Every stage, in message-path order.
    pub const ALL: [Stage; 3] = [Stage::Receive, Stage::Process, Stage::Send];

    /// The token as it appears in a scope and on the landing page.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Stage::Receive => "receive",
            Stage::Process => "process",
            Stage::Send => "send",
        }
    }
}

/// One round's outcome for one (stage, transport, contract). ADR-0028: a verdict
/// per pair, published on `xmip:///<node>/<stage>/<transport>/<contract>`, so
/// the message-path stages an operator watches are the first axis of the tree.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Verdict {
    pub stage: Stage,
    pub transport: String,
    pub contract: Contract,
    pub outcome: Outcome,
    /// How many bytes made the round trip. Zero on failure.
    pub bytes: u64,
    /// A point below the (stage, transport, contract) node, or `None` for the
    /// transport verdict itself. The identity pipeline sets this to its step —
    /// `identification`, `authentication`, `authorization` on Receive, `identity`
    /// on Send — so it publishes as a child an operator drills into. ADR-0019.
    pub point: Option<&'static str>,
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
    /// The scope this verdict is published under:
    /// `<node>/<stage>/<transport>/<contract>`.
    #[must_use]
    pub fn scope(&self, node: &str) -> String {
        let base = format!(
            "{node}/{}/{}/{}",
            self.stage.name(),
            self.transport,
            self.contract.name()
        );
        match self.point {
            Some(point) => format!("{base}/{point}"),
            None => base,
        }
    }

    /// The verdict as a health record for the snapshot.
    #[must_use]
    pub fn health(&self, node: &str) -> HealthRecord {
        let (health, severity, evidence) = match &self.outcome {
            Outcome::Delivered => (
                Health::Fine,
                0,
                format!("{} bytes out and back", self.bytes),
            ),
            Outcome::OneSided(why) => (Health::Stressed, 40, why.clone()),
            Outcome::Failed(why) => (Health::Done, 90, why.clone()),
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
    fn a_delivered_verdict_is_green_and_scopes_by_stage() {
        let verdict = Verdict {
            stage: Stage::Receive,
            transport: "file".to_string(),
            contract: Contract::Text,
            outcome: Outcome::Delivered,
            bytes: 14,
            point: None,
            observed_unix_nanos: 1,
        };

        let record = verdict.health("xmip:///playground");

        assert_eq!(record.scope, "xmip:///playground/receive/file/text");
        assert_eq!(record.health, Health::Fine);
        assert!(record.evidence.contains("14 bytes"));
    }

    #[test]
    fn the_stage_is_the_first_segment_under_the_node() {
        for stage in Stage::ALL {
            let verdict = Verdict {
                stage,
                transport: "tcp".to_string(),
                contract: Contract::Json,
                outcome: Outcome::Delivered,
                bytes: 1,
                point: None,
                observed_unix_nanos: 1,
            };
            assert_eq!(
                verdict.scope("xmip:///n"),
                format!("xmip:///n/{}/tcp/json", stage.name())
            );
        }
    }

    #[test]
    fn a_one_sided_transport_is_stressed_not_done() {
        let verdict = Verdict {
            stage: Stage::Send,
            transport: "mdns".to_string(),
            contract: Contract::Bytes,
            outcome: Outcome::OneSided("receive only".to_string()),
            bytes: 0,
            point: None,
            observed_unix_nanos: 1,
        };

        assert_eq!(verdict.health("xmip:///p").health, Health::Stressed);
    }

    #[test]
    fn a_failure_is_done_and_carries_the_reason() {
        let verdict = Verdict {
            stage: Stage::Process,
            transport: "tcp".to_string(),
            contract: Contract::Bytes,
            outcome: Outcome::Failed("connection refused".to_string()),
            bytes: 0,
            point: None,
            observed_unix_nanos: 1,
        };

        let record = verdict.health("xmip:///p");
        assert_eq!(record.health, Health::Done);
        assert_eq!(record.evidence, "connection refused");
    }
}
