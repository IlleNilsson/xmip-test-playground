//! The load's payloads: a large, valid document of each contract's shape, a
//! plain byte pattern for sizes past the parse ceiling, and the Stream an
//! arrived payload is rebuilt into for the contract check.
//!
//! Each builder pads its contract's probe to a target size while keeping the
//! contract sound — an X12 `SE` count kept true, an EDIFACT `UNT` count kept
//! true, HL7 segments separated by CR and no break at the end — so a load
//! proves the contract at size and not only the bytes. The stress axis reads
//! [`large_payload`] for its own sizes.

use stream::Stream;
use xcore::StreamId;

use crate::verdict::Contract;

/// A plain byte pattern of `size` bytes, for loads too large to bother giving a
/// structural shape. Cheap to build and to check.
pub(crate) fn filler(size: usize) -> Vec<u8> {
    (0..size)
        .map(|i| u8::try_from(i % 256).unwrap_or(0))
        .collect()
}

/// A large, valid payload of `contract`'s shape, at least `target` bytes. Each
/// shape is built so its real contract still holds at size — valid JSON, XML and
/// HTML, not just bytes of the right length.
pub(crate) fn large_payload(contract: Contract, target: usize) -> Vec<u8> {
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
        Contract::Csv => lines_to("id,customer,total", "A1,\"ACME, Inc\",15.00", target),
        Contract::FixedWidth => lines_to("A00001ACME      02", "A00002BOLT      01", target),
        Contract::Edifact => large_edifact(target),
        Contract::Regex => repeat_to("PROBE-4711 heavy ", target).into_bytes(),
        Contract::Schematron => wrap_to(
            "<probe xmlns=\"urn:xmip:probe\">",
            "<n>1</n>",
            "</probe>",
            target,
        ),
        Contract::Hl7v2 => large_hl7(target),
        Contract::Fhir => wrap_to(
            concat!(
                r#"{"resourceType":"Bundle","type":"collection","entry":"#,
                r#"[{"resource":{"resourceType":"Patient","id":"p0"}}"#
            ),
            r#",{"resource":{"resourceType":"Observation","id":"o"}}"#,
            "]}",
            target,
        ),
        Contract::X12 => large_x12(target),
        // Each record is about a dozen bytes; the container is judged by
        // every one of them decoding against the schema it carries.
        Contract::Avro => crate::verdict::avro_container(target / 12 + 1, "heavy record"),
        Contract::GraphqlSchema => wrap_to("query Heavy { ", "probe { id ping } ", "}", target),
        // Each field-3 entry is two bytes of tag and length and the text.
        Contract::Protobuf => crate::verdict::protobuf_message(target / 12 + 1, "heavy rec."),
        Contract::Wsdl => large_wsdl(target),
        Contract::OpenApi => large_openapi(target),
        Contract::AsyncApi => large_asyncapi(target),
        Contract::Sql => wrap_to(
            "BEGIN;
",
            "INSERT INTO probe (n, ping) VALUES (1, 'heavy row');
",
            "COMMIT;
",
            target,
        ),
    }
}

/// One 850 padded with `MSG` segments to `target`, its `SE` count kept true
/// so the interchange stays sound at any size.
fn large_x12(target: usize) -> Vec<u8> {
    use std::fmt::Write as _;
    let probe = String::from_utf8_lossy(crate::verdict::X12_PROBE).into_owned();
    let (head, _) = probe
        .split_once("SE*4*0001~")
        .expect("the probe closes its set");
    let mut body = head.to_string();
    let mut segments = 3; // ST, BEG and PO1
    while body.len() < target {
        body.push_str("MSG*heavy~");
        segments += 1;
    }
    segments += 1; // SE itself
    let _ = write!(body, "SE*{segments}*0001~GE*1*1~IEA*1*000000001~");
    body.into_bytes()
}

/// One ADT message padded with `NTE` segments to `target`, CR between segments
/// and none at the end.
fn large_hl7(target: usize) -> Vec<u8> {
    let mut body = String::from_utf8_lossy(crate::verdict::HL7_PROBE).into_owned();
    while body.len() < target {
        body.push_str("\rNTE|1||heavy");
    }
    body.into_bytes()
}

/// `first` then `unit` lines to `target`, CRLF between and none at the end, so
/// the payload survives a line-carrying transport byte for byte.
fn lines_to(first: &str, unit: &str, target: usize) -> Vec<u8> {
    let mut out = String::from(first);
    while out.len() < target {
        out.push_str("\r\n");
        out.push_str(unit);
    }
    out.into_bytes()
}

/// One message padded with `FTX` segments to `target`, its `UNT` count kept
/// true so the interchange stays sound at any size.
fn large_edifact(target: usize) -> Vec<u8> {
    use std::fmt::Write as _;
    let mut body = String::from(
        "UNA:+.? 'UNB+UNOC:3+SENDER+RECEIVER+260907:1345+REF001'\
         UNH+1+ORDERS:D:96A:UN'BGM+220+PO4711'",
    );
    let mut segments = 2; // UNH and BGM
    while body.len() < target {
        body.push_str("FTX+AAI+++heavy'");
        segments += 1;
    }
    segments += 1; // UNT itself
    let _ = write!(body, "UNT+{segments}+1'UNZ+1+REF001'");
    body.into_bytes()
}

fn repeat_to(unit: &str, target: usize) -> String {
    let mut out = String::with_capacity(target + unit.len());
    while out.len() < target {
        out.push_str(unit);
    }
    out
}

/// `open`, then `unit` until `close` would reach `target`, then `close` —
/// and always at least one `unit`: a GraphQL selection set or a FHIR bundle
/// with nothing in it is not the contract, whatever the target.
fn wrap_to(open: &str, unit: &str, close: &str, target: usize) -> Vec<u8> {
    let mut out = String::from(open);
    out.push_str(unit);
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
        Some(contract.representation().to_string()),
    )
}

/// One service description padded with messages to `target`, every
/// reference still landing.
fn large_wsdl(target: usize) -> Vec<u8> {
    use std::fmt::Write as _;
    let probe = String::from_utf8_lossy(crate::verdict::WSDL_PROBE).into_owned();
    let (head, tail) = probe
        .split_once("<portType")
        .expect("the probe has a port type");
    let mut body = head.to_string();
    let mut n = 0;
    while body.len() + tail.len() < target {
        let _ = write!(body, "<message name=\"M{n}\"/>");
        n += 1;
    }
    body.push_str("<portType");
    body.push_str(tail);
    body.into_bytes()
}

/// One description padded with paths to `target`, each with its responses.
fn large_openapi(target: usize) -> Vec<u8> {
    use std::fmt::Write as _;
    let mut body =
        String::from(r#"{"openapi":"3.0.3","info":{"title":"Heavy","version":"1"},"paths":{"#);
    let mut n = 0;
    while body.len() < target {
        if n > 0 {
            body.push(',');
        }
        let _ = write!(
            body,
            r#""/p{n}":{{"get":{{"responses":{{"200":{{"description":"ok"}}}}}}}}"#
        );
        n += 1;
    }
    body.push_str("}}");
    body.into_bytes()
}

/// One description padded with channels to `target`.
fn large_asyncapi(target: usize) -> Vec<u8> {
    use std::fmt::Write as _;
    let mut body =
        String::from(r#"{"asyncapi":"2.6.0","info":{"title":"Heavy","version":"1"},"channels":{"#);
    let mut n = 0;
    while body.len() < target {
        if n > 0 {
            body.push(',');
        }
        let _ = write!(
            body,
            r#""c/{n}":{{"subscribe":{{"message":{{"name":"m"}}}}}}"#
        );
        n += 1;
    }
    body.push_str("}}");
    body.into_bytes()
}
