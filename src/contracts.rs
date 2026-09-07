//! The content contracts the pingpong test validates, over actual Streams.
//!
//! ADR-0028's contract axis, made real: a probe does not just send bytes and
//! compare them back, it sends an actual [`Stream`] and, on arrival, a real
//! [`contract::Contract`] validates it. A pair is delivered only if the
//! bytes round-tripped *and* the contract held — which is the difference between
//! testing a transport and testing an integration. JSON well-formedness leans on
//! a real parser; XML on a small well-formedness scan; text and html on lighter
//! structural claims; bytes makes no claim at all.
//!
//! JSON and XML validate through the estate's own contract technologies,
//! `xmip-core-contract-json-schema` and `xmip-core-contract-xml-schema`, since
//! 2026-09-07 — the swap the shape here was kept for. Text, html and bytes stay
//! local: no contract technology claims them.

use contract::{
    Contract, ContractDescriptor, ContractError, ContractId, ValidationIssue, ValidationResult,
};
use contract_csv::Csv;
use contract_edi_edifact::Edifact;
use contract_fixed_width::FixedWidth;
use contract_json_schema::JsonSchema;
use contract_regex::RegexContract;
use contract_schematron::Schematron;
use contract_xml_schema::XmlSchema;
use stream::Stream;

/// The content shape a contract holds a Stream to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Shape {
    /// No structural claim. Any bytes hold.
    Bytes,
    /// Valid UTF-8.
    Text,
    /// Well-formed JSON.
    Json,
    /// Well-formed XML — tags balanced and nested.
    Xml,
    /// Carries HTML markup.
    Html,
    /// Rows with a consistent field count and CSV quoting.
    Csv,
    /// Text records; a bound copybook would lay them out.
    FixedWidth,
    /// A sound UN/EDIFACT interchange.
    Edifact,
    /// Text; a bound pattern would hold it.
    Regex,
    /// Well-formed XML; bound rules would hold it.
    Schematron,
}

impl Shape {
    /// The media type a Stream of this shape declares.
    #[must_use]
    pub const fn representation(self) -> &'static str {
        match self {
            Shape::Bytes => "application/octet-stream",
            Shape::Text | Shape::FixedWidth | Shape::Regex => "text/plain",
            Shape::Json => "application/json",
            Shape::Xml | Shape::Schematron => "application/xml",
            Shape::Html => "text/html",
            Shape::Csv => "text/csv",
            Shape::Edifact => "application/EDIFACT",
        }
    }
}

/// A content contract the playground exercises. Implements the estate's
/// [`Contract`] trait, so the pingpong validates an arrived Stream exactly as a
/// Journey would.
pub struct ContentContract {
    descriptor: ContractDescriptor,
    shape: Shape,
}

impl ContentContract {
    #[must_use]
    pub fn new(name: &str, shape: Shape) -> Self {
        Self {
            descriptor: ContractDescriptor {
                id: ContractId(format!("pingpong-{name}")),
                version: "1".to_string(),
                representation: shape.representation().to_string(),
            },
            shape,
        }
    }
}

impl Contract for ContentContract {
    fn descriptor(&self) -> &ContractDescriptor {
        &self.descriptor
    }

    fn identify(&self, stream: &Stream) -> Result<bool, ContractError> {
        // A probe's Stream always carries the contract's own media type, so
        // identify is the media-type match.
        Ok(stream.media_type() == Some(self.descriptor.representation.as_str()))
    }

    fn validate(&self, stream: &Stream) -> Result<ValidationResult, ContractError> {
        match self.shape {
            Shape::Json => return JsonSchema::new().validate(stream),
            Shape::Xml => return XmlSchema::new().validate(stream),
            Shape::Csv => return Csv::new().validate(stream),
            Shape::FixedWidth => return FixedWidth::new().validate(stream),
            Shape::Edifact => return Edifact::new().validate(stream),
            Shape::Regex => return RegexContract::new().validate(stream),
            Shape::Schematron => return Schematron::new().validate(stream),
            Shape::Bytes | Shape::Text | Shape::Html => {}
        }
        let issues = check(self.shape, stream.bytes());

        Ok(ValidationResult {
            valid: issues.is_empty(),
            issues,
        })
    }
}

fn issue(message: impl Into<String>) -> ValidationIssue {
    ValidationIssue {
        code: "malformed".to_string(),
        message: message.into(),
        path: None,
    }
}

/// The one structural check per local shape. Empty means it held. Every shape
/// with a contract technology never reaches here: `validate` hands it over.
fn check(shape: Shape, bytes: &[u8]) -> Vec<ValidationIssue> {
    match shape {
        Shape::Bytes
        | Shape::Json
        | Shape::Xml
        | Shape::Csv
        | Shape::FixedWidth
        | Shape::Edifact
        | Shape::Regex
        | Shape::Schematron => Vec::new(),
        Shape::Text => match std::str::from_utf8(bytes) {
            Ok(_) => Vec::new(),
            Err(error) => vec![issue(format!("not valid UTF-8: {error}"))],
        },
        Shape::Html => has_markup(bytes),
    }
}

/// HTML is not XML — a lighter claim: valid UTF-8 that carries markup.
fn has_markup(bytes: &[u8]) -> Vec<ValidationIssue> {
    match std::str::from_utf8(bytes) {
        Ok(text) if text.contains('<') && text.contains('>') => Vec::new(),
        Ok(_) => vec![issue("no HTML markup found")],
        Err(error) => vec![issue(format!("not valid UTF-8: {error}"))],
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use xcore::StreamId;

    fn stream(shape: Shape, bytes: &[u8]) -> Stream {
        Stream::new(
            StreamId::new(1),
            bytes.to_vec(),
            Some(shape.representation().to_string()),
        )
    }

    fn holds(shape: Shape, bytes: &[u8]) -> bool {
        ContentContract::new("t", shape)
            .validate(&stream(shape, bytes))
            .expect("validation runs")
            .valid
    }

    #[test]
    fn valid_json_holds_and_broken_json_does_not() {
        assert!(holds(Shape::Json, br#"{"probe":"ping-pong","n":1}"#));
        assert!(!holds(Shape::Json, b"{not json"));
    }

    #[test]
    fn well_formed_xml_holds_and_a_dangling_tag_does_not() {
        assert!(holds(Shape::Xml, b"<probe><n>1</n>ping-pong</probe>"));
        assert!(!holds(Shape::Xml, b"<probe><n>1</probe>"));
        assert!(!holds(Shape::Xml, b"<probe>never closed"));
    }

    #[test]
    fn text_rejects_invalid_utf8_but_bytes_never_complains() {
        assert!(holds(Shape::Text, "xmip ✓".as_bytes()));
        assert!(!holds(Shape::Text, &[0xff, 0xfe]));
        assert!(holds(Shape::Bytes, &[0xff, 0xfe]));
    }

    #[test]
    fn identify_matches_the_declared_media_type() {
        let contract = ContentContract::new("json", Shape::Json);
        assert!(
            contract
                .identify(&stream(Shape::Json, b"{}"))
                .expect("identify runs")
        );
        assert!(
            !contract
                .identify(&stream(Shape::Text, b"{}"))
                .expect("identify runs")
        );
    }
}
