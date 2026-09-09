//! The discovery round trips: ssdp, mdns, dhcp and snmp, each behind the
//! same [`RoundTrip`] the pingpong scenario drives.
//!
//! These four say what a device is rather than what it sends, and a Stream
//! arrives as what they say it in: HTTPU headers, TXT strings, option lines,
//! variable bindings. Each has a place for bytes it does not read — a vendor
//! header, a TXT string, a site-specific option, an OCTET STRING — and the
//! adapter carries the Stream there, in hex where the place is text and in
//! as many messages as it takes: notifications until a byebye, announcements
//! until an empty one, informs until one with no option, one SET whose
//! response is waited for. That is what a device does with an opaque blob,
//! and the adapter does it here so the scenario stays one thing over all of
//! them. Every far end is unicast on loopback; no build box needs a
//! multicast route.

use std::fmt::Write;

use transport::Transport;
use transport::error::{classify, protocol_error};
use transport_dhcp::{BOOTREQUEST, DhcpTransport};
use transport_mdns::MdnsTransport;
use transport_snmp::{Envelope, MAX_DATAGRAM, PduType, SnmpTransport};
use transport_ssdp::{BYEBYE, SsdpTransport};

use crate::roundtrip::{Exchange, RoundTrip, TIMEOUT};

/// The datagram shape the four share: the send runs on its own thread, the
/// receive on this one, and the round is judged when both are done. A far
/// end that hears nothing times out; a send that cannot frame the payload
/// fails first and is what the verdict names.
fn datagram_exchange<S, R>(send: S, receive: R) -> Exchange
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

/// `bytes` as lower-case hex pairs, the one form every text place takes.
fn hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        let _ = write!(out, "{byte:02x}");
    }
    out
}

/// The bytes `digits` spell, refused where they do not.
fn unhex(digits: &str) -> transport::Result<Vec<u8>> {
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

/// The vendor header an SSDP notification carries the Stream in.
const SSDP_HEADER: &str = "X-STREAM";
/// What one notification carries: twice this in hex and the headers stay
/// well inside the datagram the transport takes.
const SSDP_CHUNK: usize = 3000;
const SSDP_NT: &str = "urn:xmip:device:Probe:1";
const SSDP_USN: &str = "uuid:xmip-probe::urn:xmip:device:Probe:1";

/// SSDP: bind a node, send the payload from another thread as `ssdp:alive`
/// notifications of at most [`SSDP_CHUNK`] bytes each, in hex under one
/// vendor header, then `ssdp:byebye` to close; the node takes them in turn
/// until the byebye. A message goes through the transport as it is, so what
/// the wire carries is exactly the HTTPU text composed here.
pub struct SsdpRoundTrip;

fn ssdp_alive(chunk: &[u8]) -> Vec<u8> {
    let alive = transport_ssdp::Message::alive(
        SSDP_NT,
        SSDP_USN,
        "http://127.0.0.1/probe.xml",
        "xmip/0.1 UPnP/1.1",
    )
    .with(SSDP_HEADER, &hex(chunk));
    transport_ssdp::message::format(&alive)
}

impl RoundTrip for SsdpRoundTrip {
    fn transport(&self) -> &'static str {
        "ssdp"
    }

    fn exchange(&self, payload: &[u8]) -> Exchange {
        let far_end = SsdpTransport::new("127.0.0.1:0").timing_out_after(TIMEOUT);
        let (socket, address) = match far_end.bind_udp() {
            Ok(bound) => bound,
            Err(error) => return Exchange::Failed(format!("bind failed: {error}")),
        };
        let payload = payload.to_vec();
        datagram_exchange(
            move || {
                let near_end = SsdpTransport::new("127.0.0.1:0");
                let target = format!("ssdp://{address}");
                for chunk in payload.chunks(SSDP_CHUNK) {
                    near_end.send(&target, &ssdp_alive(chunk))?;
                }
                let byebye = transport_ssdp::Message::byebye(SSDP_NT, SSDP_USN);
                near_end.send(&target, &transport_ssdp::message::format(&byebye))
            },
            || {
                let mut bytes = Vec::new();
                loop {
                    let arrived = far_end.receive_datagram(&socket)?;
                    let message = transport_ssdp::message::parse(&arrived.bytes)?;
                    if message.header("NTS") == Some(BYEBYE) {
                        return Ok(bytes);
                    }
                    let chunk = message.header(SSDP_HEADER).ok_or_else(|| {
                        protocol_error(format!("a notification without {SSDP_HEADER}"))
                    })?;
                    bytes.extend(unhex(chunk)?);
                }
            },
        )
    }
}

/// What one TXT string carries: twice this in hex fits the 255 bytes a
/// string holds.
const MDNS_STRING: usize = 125;
/// What one announcement carries: this many strings and the three records
/// stay inside the message mDNS sends.
const MDNS_CHUNK: usize = MDNS_STRING * 32;

/// mDNS: bind a node, announce a service from another thread with the
/// payload as its TXT strings — [`MDNS_STRING`] bytes in hex each, as many
/// announcements as it takes — then announce it with no strings to close;
/// the node takes the announcements in turn until the empty one.
pub struct MdnsRoundTrip;

impl RoundTrip for MdnsRoundTrip {
    fn transport(&self) -> &'static str {
        "mdns"
    }

    fn exchange(&self, payload: &[u8]) -> Exchange {
        let far_end = MdnsTransport::new("127.0.0.1:0").timing_out_after(TIMEOUT);
        let (socket, address) = match far_end.bind_udp() {
            Ok(bound) => bound,
            Err(error) => return Exchange::Failed(format!("bind failed: {error}")),
        };
        let payload = payload.to_vec();
        datagram_exchange(
            move || {
                let near_end = MdnsTransport::new("127.0.0.1:0");
                let target = format!("mdns://{address}/Probe._xmip._udp.local?port=1");
                for chunk in payload.chunks(MDNS_CHUNK) {
                    let strings: Vec<String> = chunk.chunks(MDNS_STRING).map(hex).collect();
                    near_end.send(&target, strings.join("\n").as_bytes())?;
                }
                near_end.send(&target, &[])
            },
            || {
                let mut bytes = Vec::new();
                loop {
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
                }
            },
        )
    }
}

/// What one option holds, and so what one inform carries.
const DHCP_OPTION: usize = 255;
/// The site-specific option the Stream rides in, as a line names it.
const DHCP_LINE: &str = "option-224=0x";
/// A locally administered hardware address, so no vendor's is borrowed.
const DHCP_MAC: [u8; 6] = [0x02, 0x00, 0x00, 0x78, 0x6d, 0x69];

/// DHCP: bind a server, send the payload from a client socket as informs of
/// one site-specific option each — [`DHCP_OPTION`] bytes, what an option
/// holds — then an inform with none to close; the server takes them in turn
/// and hands each up as its option lines, which read back to the bytes. The
/// client is a socket and the transport's own message, because what the
/// transport sends is a reply and what its server takes is a request.
pub struct DhcpRoundTrip;

fn dhcp_inform(lines: &str) -> transport::Result<Vec<u8>> {
    let inform = transport_dhcp::Message::new(BOOTREQUEST, 0x786d_6970, &DHCP_MAC)
        .with_lines(lines.as_bytes())?;
    transport_dhcp::message::encode(&inform)
}

impl RoundTrip for DhcpRoundTrip {
    fn transport(&self) -> &'static str {
        "dhcp"
    }

    fn exchange(&self, payload: &[u8]) -> Exchange {
        let far_end = DhcpTransport::new("127.0.0.1:0").timing_out_after(TIMEOUT);
        let (socket, address) = match far_end.bind_udp() {
            Ok(bound) => bound,
            Err(error) => return Exchange::Failed(format!("bind failed: {error}")),
        };
        let payload = payload.to_vec();
        datagram_exchange(
            move || {
                let (client, _) = transport::socket::bind_udp("127.0.0.1:0", None)?;
                let mut informs: Vec<String> = payload
                    .chunks(DHCP_OPTION)
                    .map(|chunk| format!("message-type=inform\n{DHCP_LINE}{}\n", hex(chunk)))
                    .collect();
                informs.push("message-type=inform\n".to_string());
                for lines in informs {
                    client
                        .send_to(&dhcp_inform(&lines)?, &address)
                        .map_err(|e| classify("sending an inform", &e))?;
                }
                Ok(())
            },
            || {
                let mut bytes = Vec::new();
                loop {
                    let arrived = far_end.receive_datagram(&socket)?;
                    let text = std::str::from_utf8(&arrived.bytes)
                        .map_err(|_| protocol_error("option lines that are not text"))?;
                    let mut carried = false;
                    for digits in text.lines().filter_map(|line| line.strip_prefix(DHCP_LINE)) {
                        bytes.extend(unhex(digits)?);
                        carried = true;
                    }
                    if !carried {
                        return Ok(bytes);
                    }
                }
            },
        )
    }
}

/// SNMP: bind an agent, SET one object to the payload as an OCTET STRING
/// from another thread — the transport waits for the response — and take
/// the octets the request bound. The manager the transport binds hands
/// bindings up as `oid=value` lines, and a line is a view of the bytes, not
/// the bytes; the agent is the far end a SET has, and the transport's own
/// codec reads the request and writes the response.
pub struct SnmpRoundTrip;

impl RoundTrip for SnmpRoundTrip {
    fn transport(&self) -> &'static str {
        "snmp"
    }

    fn exchange(&self, payload: &[u8]) -> Exchange {
        let (socket, address) = match transport::socket::bind_udp("127.0.0.1:0", Some(TIMEOUT)) {
            Ok(bound) => bound,
            Err(error) => return Exchange::Failed(format!("bind failed: {error}")),
        };
        let payload = payload.to_vec();
        datagram_exchange(
            move || {
                SnmpTransport::new("127.0.0.1:0", "private")
                    .timing_out_after(TIMEOUT)
                    .send(&format!("snmp+set://{address}/1.3.6.1.4.1.0.1.0"), &payload)
            },
            || {
                let mut buffer = vec![0u8; MAX_DATAGRAM];
                let (read, peer) = socket
                    .recv_from(&mut buffer)
                    .map_err(|e| classify("receiving the request", &e))?;
                let request = Envelope::decode(&buffer[..read])?;
                let pdu = request.pdu();
                if pdu.kind != PduType::SetRequest {
                    return Err(protocol_error(format!(
                        "a {} where a set was due",
                        pdu.kind.name()
                    )));
                }
                let bytes = pdu
                    .bindings
                    .first()
                    .ok_or_else(|| protocol_error("a set binding nothing"))?
                    .value
                    .octets()?
                    .to_vec();
                socket
                    .send_to(&request.answering(pdu.response(0)).encode(), peer)
                    .map_err(|e| classify("answering the set", &e))?;
                Ok(bytes)
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
    fn dhcp_carries_a_stream_as_informs() {
        let long = vec![0x2a; 3000];
        assert_eq!(returned(&DhcpRoundTrip, OPAQUE), OPAQUE);
        assert_eq!(returned(&DhcpRoundTrip, &long), long);
        assert_eq!(returned(&DhcpRoundTrip, b""), b"");
    }

    #[test]
    fn snmp_carries_a_stream_as_one_set() {
        let long = vec![0x2a; 5000];
        assert_eq!(returned(&SnmpRoundTrip, OPAQUE), OPAQUE);
        assert_eq!(returned(&SnmpRoundTrip, &long), long);
        assert_eq!(returned(&SnmpRoundTrip, b""), b"");
    }

    #[test]
    fn hex_reads_back_and_refuses_what_is_not_hex() {
        assert_eq!(hex(&[0, 0x7f, 0xff]), "007fff");
        assert_eq!(unhex("007fff").expect("hex"), [0, 0x7f, 0xff]);
        assert!(unhex("").expect("nothing").is_empty());
        assert!(unhex("abc").is_err(), "odd");
        assert!(unhex("zz").is_err(), "not hex");
    }
}
