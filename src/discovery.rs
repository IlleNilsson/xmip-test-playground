//! The discovery round trips: ssdp, mdns, dhcp and snmp, each behind the
//! same [`RoundTrip`] the pingpong scenario drives.
//!
//! These four say what a device is rather than what it sends, and a Stream
//! arrives as what they say it in: HTTPU headers, TXT strings, option lines,
//! variable bindings. Each has a place for bytes it does not read — a vendor
//! header, a TXT string, a site-specific option, an OCTET STRING — and the
//! adapter carries the Stream there, in hex where the place is text and in
//! as many messages as it takes, every one asked for or acknowledged: a
//! search answered per chunk, a query answered per chunk, an inform
//! acknowledged per option, one SET whose response is waited for. That is
//! what a device does with an opaque blob — it answers what it is asked —
//! and the adapter does it here so the scenario stays one thing over all of
//! them. Every far end is unicast on loopback; no build box needs a
//! multicast route. Announcing the chunks unasked was how this began, and
//! it lost the tail of a mebibyte on loopback (2026-09-09): neither SSDP nor
//! mDNS acknowledges a notification, so the far end's socket buffer was the
//! only flow control there was.

use std::fmt::Write;
use std::net::UdpSocket;

use transport::error::{classify, protocol_error};
use transport_mdns::MdnsTransport;
use transport_ssdp::SsdpTransport;

use crate::roundtrip::{Exchange, RoundTrip, TIMEOUT};

/// The datagram shape the four share: the device runs on its own thread,
/// the node asking it on this one, and the round is judged when both are
/// done. A far end that hears nothing times out; a send that cannot frame
/// the payload fails first and is what the verdict names.
pub(crate) fn datagram_exchange<S, R>(send: S, receive: R) -> Exchange
where
    S: FnOnce() -> transport::Result<()> + Send + 'static,
    R: FnOnce() -> transport::Result<Vec<u8>>,
{
    let sender = std::thread::spawn(send);
    let caught = receive();
    match (caught, sender.join()) {
        (Ok(bytes), Ok(Ok(()))) => Exchange::Returned(bytes),
        (_, Ok(Err(error))) => Exchange::Failed(format!("send failed: {error}")),
        (Err(error), _) => Exchange::Failed(format!("receive failed: {error}")),
        (_, Err(_)) => Exchange::Failed("the sending thread panicked".to_string()),
    }
}

/// A socket for the device's side, timing out as the node's does.
pub(crate) fn device_socket() -> transport::Result<(UdpSocket, String)> {
    transport::socket::bind_udp("127.0.0.1:0", Some(TIMEOUT))
}

/// `bytes` as lower-case hex pairs, the one form every text place takes.
pub(crate) fn hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        let _ = write!(out, "{byte:02x}");
    }
    out
}

/// The bytes `digits` spell, refused where they do not.
pub(crate) fn unhex(digits: &str) -> transport::Result<Vec<u8>> {
    if !digits.len().is_multiple_of(2) {
        return Err(protocol_error(format!(
            "an odd number of hex digits: {digits:?}"
        )));
    }
    (0..digits.len())
        .step_by(2)
        .map(|at| {
            digits
                .get(at..at + 2)
                .and_then(|pair| u8::from_str_radix(pair, 16).ok())
                .ok_or_else(|| protocol_error(format!("not hex: {digits:?}")))
        })
        .collect()
}

/// The chunk `name` asks for as `{prefix}N`, where it asks for one.
pub(crate) fn chunk_asked(name: &str, prefix: &str) -> transport::Result<usize> {
    name.strip_prefix(prefix)
        .and_then(|rest| rest.split('.').next()?.parse().ok())
        .ok_or_else(|| protocol_error(format!("a request for something else: {name:?}")))
}

/// The vendor header an SSDP response carries the Stream in.
const SSDP_HEADER: &str = "X-STREAM";
/// What one response carries: twice this in hex and the headers stay well
/// inside the datagram the transport takes.
const SSDP_CHUNK: usize = 3000;
/// The search type chunk `N` answers to, `{SSDP_STREAM}N`.
const SSDP_STREAM: &str = "urn:xmip:stream:";
const SSDP_USN: &str = "uuid:xmip-probe::urn:xmip:device:Probe:1";

/// SSDP: bind a node and a device, the device holding the payload; the node
/// searches for the Stream chunk by chunk, an `M-SEARCH` whose `ST` names
/// the chunk, and the device answers each with one response carrying
/// [`SSDP_CHUNK`] bytes in hex under one vendor header; a search past the
/// end is answered with no such header, and that closes it. A message goes
/// through the transport as it is, so what the wire carries is exactly the
/// HTTPU text composed here.
pub struct SsdpRoundTrip;

/// The response to a search for chunk `n` of `payload`.
fn ssdp_response(payload: &[u8], n: usize) -> Vec<u8> {
    let response = transport_ssdp::Message::response(
        &format!("{SSDP_STREAM}{n}"),
        SSDP_USN,
        "http://127.0.0.1/probe.xml",
        "xmip/0.1 UPnP/1.1",
    );
    let response = match payload.chunks(SSDP_CHUNK).nth(n) {
        Some(chunk) => response.with(SSDP_HEADER, &hex(chunk)),
        None => response,
    };
    transport_ssdp::message::format(&response)
}

impl RoundTrip for SsdpRoundTrip {
    fn transport(&self) -> &'static str {
        "ssdp"
    }

    fn exchange(&self, payload: &[u8]) -> Exchange {
        let far_end = SsdpTransport::new("127.0.0.1:0").timing_out_after(TIMEOUT);
        let (socket, _) = match far_end.bind_udp() {
            Ok(bound) => bound,
            Err(error) => return Exchange::Failed(format!("bind failed: {error}")),
        };
        let (device, device_address) = match device_socket() {
            Ok(bound) => bound,
            Err(error) => return Exchange::Failed(format!("bind failed: {error}")),
        };
        let payload = payload.to_vec();
        datagram_exchange(
            move || {
                let chunks = payload.chunks(SSDP_CHUNK).count();
                let mut buffer = vec![0u8; transport_ssdp::MAX_DATAGRAM];
                loop {
                    let (read, peer) = device
                        .recv_from(&mut buffer)
                        .map_err(|e| classify("awaiting a search", &e))?;
                    let search = transport_ssdp::message::parse(&buffer[..read])?;
                    let n = chunk_asked(search.notification_type(), SSDP_STREAM)?;
                    device
                        .send_to(&ssdp_response(&payload, n), peer)
                        .map_err(|e| classify("answering a search", &e))?;
                    if n >= chunks {
                        return Ok(());
                    }
                }
            },
            || {
                let mut bytes = Vec::new();
                let mut n = 0;
                loop {
                    let search = transport_ssdp::Message::search(&format!("{SSDP_STREAM}{n}"), 1);
                    socket
                        .send_to(&transport_ssdp::message::format(&search), &device_address)
                        .map_err(|e| classify("searching", &e))?;
                    let arrived = far_end.receive_datagram(&socket)?;
                    let response = transport_ssdp::message::parse(&arrived.bytes)?;
                    let Some(chunk) = response.header(SSDP_HEADER) else {
                        return Ok(bytes);
                    };
                    bytes.extend(unhex(chunk)?);
                    n += 1;
                }
            },
        )
    }
}

/// What one TXT string carries: twice this in hex fits the 255 bytes a
/// string holds.
const MDNS_STRING: usize = 125;
/// What one response carries: this many strings and the three records
/// stay inside the message mDNS sends.
const MDNS_CHUNK: usize = MDNS_STRING * 32;
/// The service type every chunk is an instance of, instance `chunk-N`.
const MDNS_KIND: &str = "_xmip._udp";

/// mDNS: bind a node and a responder, the responder holding the payload;
/// the node queries for the Stream chunk by chunk, each the instance
/// `chunk-N` of one service type, and the responder answers each query with
/// that instance's records, its TXT strings [`MDNS_STRING`] bytes in hex
/// each; a query past the end is answered with no strings, and that closes
/// it.
pub struct MdnsRoundTrip;

impl RoundTrip for MdnsRoundTrip {
    fn transport(&self) -> &'static str {
        "mdns"
    }

    fn exchange(&self, payload: &[u8]) -> Exchange {
        let far_end = MdnsTransport::new("127.0.0.1:0").timing_out_after(TIMEOUT);
        let (socket, _) = match far_end.bind_udp() {
            Ok(bound) => bound,
            Err(error) => return Exchange::Failed(format!("bind failed: {error}")),
        };
        let (device, device_address) = match device_socket() {
            Ok(bound) => bound,
            Err(error) => return Exchange::Failed(format!("bind failed: {error}")),
        };
        let payload = payload.to_vec();
        datagram_exchange(
            move || {
                let responder = MdnsTransport::new("127.0.0.1:0");
                let chunks = payload.chunks(MDNS_CHUNK).count();
                let mut buffer = vec![0u8; transport_mdns::MAX_MESSAGE];
                loop {
                    let (read, peer) = device
                        .recv_from(&mut buffer)
                        .map_err(|e| classify("awaiting a query", &e))?;
                    let query = transport_mdns::message::decode(&buffer[..read])?;
                    let asked = query.questions.first().map_or("", |q| q.name.as_str());
                    let n = chunk_asked(asked, "chunk-")?;
                    let target = format!("mdns://{peer}/chunk-{n}.{MDNS_KIND}.local?port=1");
                    let (_, mut service) = responder.service_at(&target)?;
                    service.txt = payload
                        .chunks(MDNS_CHUNK)
                        .nth(n)
                        .map(|chunk| chunk.chunks(MDNS_STRING).map(hex).collect())
                        .unwrap_or_default();
                    let records = service.records(transport_mdns::TTL);
                    let response = transport_mdns::Message::response(query.id, records);
                    device
                        .send_to(&transport_mdns::message::encode(&response)?, peer)
                        .map_err(|e| classify("answering a query", &e))?;
                    if n >= chunks {
                        return Ok(());
                    }
                }
            },
            || {
                let mut bytes = Vec::new();
                let mut n = 0;
                loop {
                    let name = format!("chunk-{n}.{MDNS_KIND}.local.");
                    let any = transport_mdns::message::TYPE_ANY;
                    let query = transport_mdns::Message::query(&name, any);
                    socket
                        .send_to(&transport_mdns::message::encode(&query)?, &device_address)
                        .map_err(|e| classify("querying", &e))?;
                    for arrived in far_end.receive_datagram(&socket)? {
                        if arrived.bytes.is_empty() {
                            return Ok(bytes);
                        }
                        let text = std::str::from_utf8(&arrived.bytes)
                            .map_err(|_| protocol_error("TXT strings that are not text"))?;
                        for line in text.lines() {
                            bytes.extend(unhex(line)?);
                        }
                    }
                    n += 1;
                }
            },
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn returned(rt: &dyn RoundTrip, payload: &[u8]) -> Vec<u8> {
        match rt.exchange(payload) {
            Exchange::Returned(bytes) => bytes,
            Exchange::OneSided(why) | Exchange::Failed(why) => {
                panic!("{} did not return: {why}", rt.transport())
            }
        }
    }

    /// Bytes no text place takes as they are: a NUL, a line break, a byte
    /// that is not UTF-8. What comes back must be these, not a view of them.
    const OPAQUE: &[u8] = b"\x00line\r\nbreak \xff";

    #[test]
    fn ssdp_carries_a_stream_as_notifications() {
        let long = vec![0x2a; 5000];
        assert_eq!(returned(&SsdpRoundTrip, OPAQUE), OPAQUE);
        assert_eq!(returned(&SsdpRoundTrip, &long), long);
        assert_eq!(returned(&SsdpRoundTrip, b""), b"");
    }

    #[test]
    fn mdns_carries_a_stream_as_announcements() {
        let long = vec![0x2a; 5000];
        assert_eq!(returned(&MdnsRoundTrip, OPAQUE), OPAQUE);
        assert_eq!(returned(&MdnsRoundTrip, &long), long);
        assert_eq!(returned(&MdnsRoundTrip, b""), b"");
    }

    #[test]
    fn ssdp_carries_the_edges() {
        crate::support::carries_the_edges(&SsdpRoundTrip);
    }

    #[test]
    fn mdns_carries_the_edges() {
        crate::support::carries_the_edges(&MdnsRoundTrip);
    }

    #[test]
    fn hex_reads_back_and_refuses_what_is_not_hex() {
        assert_eq!(hex(&[0, 0x7f, 0xff]), "007fff");
        assert_eq!(unhex("007fff").expect("hex"), [0, 0x7f, 0xff]);
        assert!(unhex("").expect("nothing").is_empty());
        assert!(unhex("abc").is_err(), "odd");
        assert!(unhex("zz").is_err(), "not hex");
        assert_eq!(
            chunk_asked("chunk-12._xmip._udp.local.", "chunk-").expect("asked"),
            12
        );
        assert_eq!(
            chunk_asked("urn:xmip:stream:0", "urn:xmip:stream:").expect("asked"),
            0
        );
        assert!(chunk_asked("ssdp:all", "urn:xmip:stream:").is_err());
    }
}
