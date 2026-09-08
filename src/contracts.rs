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
    Contract as ContractTrait, ContractDescriptor, ContractError, ContractId, ValidationIssue,
    ValidationResult,
};
use contract_asyncapi::AsyncApi;
use contract_avro::Avro;
use contract_csv::Csv;
use contract_edi_edifact::Edifact;
use contract_edi_x12::X12;
use contract_fhir::Fhir;
use contract_fixed_width::FixedWidth;
use contract_graphql_schema::GraphqlSchema;
use contract_hl7_v2::Hl7v2;
use contract_json_schema::JsonSchema;
use contract_openapi::OpenApi;
use contract_protobuf::Protobuf;
use contract_regex::RegexContract;
use contract_schematron::Schematron;
use contract_wsdl::Wsdl;
use contract_xml_schema::XmlSchema;
use stream::Stream;

use crate::verdict::Contract;

impl Contract {
    /// The media type a Stream of this contract declares.
    #[must_use]
    pub const fn representation(self) -> &'static str {
        match self {
            Contract::Bytes => "application/octet-stream",
            Contract::Text | Contract::FixedWidth | Contract::Regex => "text/plain",
            Contract::Json => "application/json",
            Contract::Xml | Contract::Schematron => "application/xml",
            Contract::Html => "text/html",
            Contract::Csv => "text/csv",
            Contract::Edifact => "application/EDIFACT",
            Contract::Hl7v2 => "x-application/hl7-v2+er7",
            Contract::Fhir => "application/fhir+json",
            Contract::X12 => "application/EDI-X12",
            Contract::Avro => "application/avro",
            Contract::GraphqlSchema => "application/graphql",
            Contract::Protobuf => "application/protobuf",
            Contract::Wsdl => "application/wsdl+xml",
            Contract::OpenApi => "application/vnd.oai.openapi+json",
            Contract::AsyncApi => "application/vnd.aai.asyncapi+json",
        }
    }
}

/// A content contract the playground exercises. Implements the estate's
/// [`ContractTrait`], so the pingpong validates an arrived Stream exactly as a
/// Journey would.
pub struct ContentContract {
    descriptor: ContractDescriptor,
    contract: Contract,
}

impl ContentContract {
    #[must_use]
    pub fn new(contract: Contract) -> Self {
        Self {
            descriptor: ContractDescriptor {
                id: ContractId(format!("pingpong-{}", contract.name())),
                version: "1".to_string(),
                representation: contract.representation().to_string(),
            },
            contract,
        }
    }
}

impl ContractTrait for ContentContract {
    fn descriptor(&self) -> &ContractDescriptor {
        &self.descriptor
    }

    fn identify(&self, stream: &Stream) -> Result<bool, ContractError> {
        // A probe's Stream always carries the contract's own media type, so
        // identify is the media-type match.
        Ok(stream.media_type() == Some(self.descriptor.representation.as_str()))
    }

    fn validate(&self, stream: &Stream) -> Result<ValidationResult, ContractError> {
        match self.contract {
            Contract::Json => return JsonSchema::new().validate(stream),
            Contract::Xml => return XmlSchema::new().validate(stream),
            Contract::Csv => return Csv::new().validate(stream),
            Contract::FixedWidth => return FixedWidth::new().validate(stream),
            Contract::Edifact => return Edifact::new().validate(stream),
            Contract::Regex => return RegexContract::new().validate(stream),
            Contract::Schematron => return Schematron::new().validate(stream),
            Contract::Hl7v2 => return Hl7v2::new().validate(stream),
            Contract::Fhir => return Fhir::new().validate(stream),
            Contract::X12 => return X12::new().validate(stream),
            Contract::Avro => return Avro::new().validate(stream),
            Contract::GraphqlSchema => return GraphqlSchema::new().validate(stream),
            Contract::Protobuf => return Protobuf::new().validate(stream),
            Contract::Wsdl => return Wsdl::new().validate(stream),
            Contract::OpenApi => return OpenApi::new().validate(stream),
            Contract::AsyncApi => return AsyncApi::new().validate(stream),
            Contract::Bytes | Contract::Text | Contract::Html => {}
        }
        let issues = check(self.contract, stream.bytes());

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

/// The one structural check per local contract. Empty means it held. Every contract
/// with a contract technology never reaches here: `validate` hands it over.
fn check(contract: Contract, bytes: &[u8]) -> Vec<ValidationIssue> {
    match contract {
        Contract::Bytes
        | Contract::Json
        | Contract::Xml
        | Contract::Csv
        | Contract::FixedWidth
        | Contract::Edifact
        | Contract::Regex
        | Contract::Schematron
        | Contract::Hl7v2
        | Contract::Fhir
        | Contract::X12
        | Contract::Avro
        | Contract::GraphqlSchema
        | Contract::Protobuf
        | Contract::Wsdl
        | Contract::OpenApi
        | Contract::AsyncApi => Vec::new(),
        Contract::Text => match std::str::from_utf8(bytes) {
            Ok(_) => Vec::new(),
            Err(error) => vec![issue(format!("not valid UTF-8: {error}"))],
        },
        Contract::Html => has_markup(bytes),
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

    fn stream(contract: Contract, bytes: &[u8]) -> Stream {
        Stream::new(
            StreamId::new(1),
            bytes.to_vec(),
            Some(contract.representation().to_string()),
        )
    }

    fn holds(contract: Contract, bytes: &[u8]) -> bool {
        ContentContract::new(contract)
            .validate(&stream(contract, bytes))
            .expect("validation runs")
            .valid
    }

    #[test]
    fn valid_json_holds_and_broken_json_does_not() {
        assert!(holds(Contract::Json, br#"{"probe":"ping-pong","n":1}"#));
        assert!(!holds(Contract::Json, b"{not json"));
    }

    #[test]
    fn well_formed_xml_holds_and_a_dangling_tag_does_not() {
        assert!(holds(Contract::Xml, b"<probe><n>1</n>ping-pong</probe>"));
        assert!(!holds(Contract::Xml, b"<probe><n>1</probe>"));
        assert!(!holds(Contract::Xml, b"<probe>never closed"));
    }

    #[test]
    fn text_rejects_invalid_utf8_but_bytes_never_complains() {
        assert!(holds(Contract::Text, "xmip ✓".as_bytes()));
        assert!(!holds(Contract::Text, &[0xff, 0xfe]));
        assert!(holds(Contract::Bytes, &[0xff, 0xfe]));
    }

    #[test]
    fn identify_matches_the_declared_media_type() {
        let contract = ContentContract::new(Contract::Json);
        assert!(
            contract
                .identify(&stream(Contract::Json, b"{}"))
                .expect("identify runs")
        );
        assert!(
            !contract
                .identify(&stream(Contract::Text, b"{}"))
                .expect("identify runs")
        );
    }
}
