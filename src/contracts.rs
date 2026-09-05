//! The content contracts the pingpong test validates, over actual Streams.
//!
//! ADR-0028's contract axis, made real: a probe does not just send bytes and
//! compare them back, it sends an actual [`Stream`] and, on arrival, a real
//! [`xmip_contract::Contract`] validates it. A pair is delivered only if the
//! bytes round-tripped *and* the contract held — which is the difference between
//! testing a transport and testing an integration. JSON well-formedness leans on
//! a real parser; XML on a small well-formedness scan; text and html on lighter
//! structural claims; bytes makes no claim at all.
//!
//! These live in the playground until the estate's own `xmip-core-message-*` and
//! `xmip-core-contract-*` modules land, at which point the probe validates
//! against those instead — the shape here is deliberately the estate's Contract
//! trait so that swap is a move, not a rewrite.

use serde_json::Value;
use xmip_contract::{
    Contract, ContractDescriptor, ContractError, ContractId, ValidationIssue, ValidationResult,
};
use xmip_stream::Stream;

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
}

impl Shape {
    /// The media type a Stream of this shape declares.
    #[must_use]
    pub const fn representation(self) -> &'static str {
        match self {
            Shape::Bytes => "application/octet-stream",
            Shape::Text => "text/plain",
            Shape::Json => "application/json",
            Shape::Xml => "application/xml",
            Shape::Html => "text/html",
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

/// The one structural check per shape. Empty means it held.
fn check(shape: Shape, bytes: &[u8]) -> Vec<ValidationIssue> {
    match shape {
        Shape::Bytes => Vec::new(),
        Shape::Text => match std::str::from_utf8(bytes) {
            Ok(_) => Vec::new(),
            Err(error) => vec![issue(format!("not valid UTF-8: {error}"))],
        },
        Shape::Json => match serde_json::from_slice::<Value>(bytes) {
            Ok(_) => Vec::new(),
            Err(error) => vec![issue(format!("not valid JSON: {error}"))],
        },
        Shape::Xml => well_formed_xml(bytes),
        Shape::Html => has_markup(bytes),
    }
}

/// A small well-formedness scan: tags open and close in order, self-closing and
/// declarations aside. Not a schema — the difference ADR-0010 draws between a
/// representation being well-formed and a contract being satisfied.
fn well_formed_xml(bytes: &[u8]) -> Vec<ValidationIssue> {
    let text = match std::str::from_utf8(bytes) {
        Ok(text) => text,
        Err(error) => return vec![issue(format!("not valid UTF-8: {error}"))],
    };

    let mut open: Vec<&str> = Vec::new();
    let mut rest = text;
    while let Some(start) = rest.find('<') {
        rest = &rest[start + 1..];
        let Some(end) = rest.find('>') else {
            return vec![issue("a '<' with no matching '>'")];
        };
        let tag = rest[..end].trim();
        rest = &rest[end + 1..];

        if tag.starts_with('?') || tag.starts_with('!') {
            // A declaration, doctype or comment — not an element, so skip it.
        } else if let Some(name) = tag.strip_prefix('/') {
            match open.pop() {
                Some(opened) if opened == name.trim() => {}
                Some(opened) => {
                    return vec![issue(format!("</{}> closes <{opened}>", name.trim()))];
                }
                None => return vec![issue(format!("</{}> with nothing open", name.trim()))],
            }
        } else if !tag.ends_with('/') {
            let name = tag.split_whitespace().next().unwrap_or("");
            if name.is_empty() {
                return vec![issue("an empty tag")];
            }
            open.push(name);
        }
    }

    match open.last() {
        Some(opened) => vec![issue(format!("<{opened}> was never closed"))],
        None => Vec::new(),
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
    use xmip_core::StreamId;

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
