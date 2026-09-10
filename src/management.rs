//! The management round trips: dhcp and snmp, each behind the same
//! [`RoundTrip`] the pingpong scenario drives, on the datagram shape
//! `discovery.rs` gives ssdp and mdns.
//!
//! A DHCP inform carries the Stream as a site-specific option, an option at
//! a time, each acknowledged; SNMP carries it as one OCTET STRING in a SET
//! whose response is waited for. They lived in `discovery.rs` until
//! 2026-09-10, when the edge-payload tests pushed that file past the gate.

use std::net::UdpSocket;
use std::sync::OnceLock;

use transport::Transport;
use transport::error::{classify, protocol_error};
use transport_dhcp::{BOOTREPLY, BOOTREQUEST, DhcpTransport, MessageType};
use transport_snmp::{Binding, Envelope, MAX_DATAGRAM, Pdu, PduType, SnmpTransport, Value};

use crate::discovery::{datagram_exchange, device_socket, hex, unhex};
use crate::roundtrip::{Exchange, RoundTrip, TIMEOUT};

/// What one option holds, and so what one inform carries.
const DHCP_OPTION: usize = 255;
/// The site-specific option the Stream rides in, as a line names it.
const DHCP_LINE: &str = "option-224=0x";
/// A locally administered hardware address, so no vendor's is borrowed.
const DHCP_MAC: [u8; 6] = [0x02, 0x00, 0x00, 0x78, 0x6d, 0x69];

/// DHCP: bind a server, send the payload from a client socket as informs of
/// one site-specific option each — [`DHCP_OPTION`] bytes, what an option
/// holds — each acknowledged before the next goes, as RFC 2131 answers a
/// DHCPINFORM with a DHCPACK, then an inform with none to close; the server
/// takes them in turn and hands each up as its option lines, which read
/// back to the bytes. The client is a socket and the transport's own
/// message, because what the transport sends is a reply and what its
/// server takes is a request.
pub struct DhcpRoundTrip;

fn dhcp_inform(lines: &str) -> transport::Result<Vec<u8>> {
    let inform = transport_dhcp::Message::new(BOOTREQUEST, 0x786d_6970, &DHCP_MAC)
        .with_lines(lines.as_bytes())?;
    transport_dhcp::message::encode(&inform)
}

/// The DHCPACK answering the inform `origin` names, back to its client.
fn dhcp_acknowledge(socket: &UdpSocket, origin: &str) -> transport::Result<()> {
    let target = origin.replace("type=inform", "type=ack");
    let (client, ack) = DhcpTransport::reply_for(&target, &[])?;
    socket
        .send_to(&transport_dhcp::message::encode(&ack)?, client)
        .map_err(|e| classify("acknowledging an inform", &e))?;
    Ok(())
}

/// The DHCPACK a client waits for after each inform.
fn dhcp_acknowledged(client: &UdpSocket) -> transport::Result<()> {
    let mut buffer = vec![0u8; transport_dhcp::MAX_MESSAGE * 2];
    let (read, _) = client
        .recv_from(&mut buffer)
        .map_err(|e| classify("awaiting the acknowledgement", &e))?;
    let reply = transport_dhcp::message::decode(&buffer[..read])?;
    if reply.op == BOOTREPLY && reply.message_type() == Some(MessageType::Ack) {
        Ok(())
    } else {
        Err(protocol_error("an answer that is not a DHCPACK"))
    }
}

/// The most a DHCP message can ever be: option 57, Maximum DHCP Message
/// Size, is sixteen bits (RFC 2132 section 9.10), less the fixed header and
/// the magic cookie. A Stream larger than the largest possible message is
/// not a DHCP conversation, however many informs it were cut into.
const DHCP_CEILING: usize = u16::MAX as usize - 240 - 4;

impl RoundTrip for DhcpRoundTrip {
    fn transport(&self) -> &'static str {
        "dhcp"
    }

    fn ceiling(&self) -> Option<usize> {
        Some(DHCP_CEILING)
    }

    fn exchange(&self, payload: &[u8]) -> Exchange {
        if payload.len() > DHCP_CEILING {
            return Exchange::Failed(format!(
                "{} bytes is over the {DHCP_CEILING} the largest DHCP message holds",
                payload.len()
            ));
        }
        let far_end = DhcpTransport::new("127.0.0.1:0").timing_out_after(TIMEOUT);
        let (socket, address) = match far_end.bind_udp() {
            Ok(bound) => bound,
            Err(error) => return Exchange::Failed(format!("bind failed: {error}")),
        };
        let payload = payload.to_vec();
        datagram_exchange(
            move || {
                let (client, _) = device_socket()?;
                let mut informs: Vec<String> = payload
                    .chunks(DHCP_OPTION)
                    .map(|chunk| format!("message-type=inform\n{DHCP_LINE}{}\n", hex(chunk)))
                    .collect();
                informs.push("message-type=inform\n".to_string());
                for lines in informs {
                    client
                        .send_to(&dhcp_inform(&lines)?, &address)
                        .map_err(|e| classify("sending an inform", &e))?;
                    dhcp_acknowledged(&client)?;
                }
                Ok(())
            },
            || {
                let mut bytes = Vec::new();
                loop {
                    let arrived = far_end.receive_datagram(&socket)?;
                    dhcp_acknowledge(&socket, &arrived.origin_uri)?;
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

/// The object one SET binds.
const SNMP_OID: [u32; 9] = [1, 3, 6, 1, 4, 1, 0, 1, 0];

/// The most one SET carries: the datagram less what the v2c envelope, the
/// PDU and the binding take around the OCTET STRING, measured through the
/// transport's own codec once at a size where every length is already in
/// its long form, as it is near the ceiling; the request id is the 1 a
/// fresh transport starts at.
fn snmp_ceiling() -> usize {
    static CEILING: OnceLock<usize> = OnceLock::new();
    *CEILING.get_or_init(|| {
        let probe = 300;
        let binding = Binding::new(&SNMP_OID, Value::OctetString(vec![0; probe]));
        let set = Pdu::new(PduType::SetRequest, 1, vec![binding]);
        let wire = Envelope::V2c(transport_snmp::Message::v2c("private", set)).encode();
        MAX_DATAGRAM - (wire.len() - probe)
    })
}

/// SNMP: bind an agent, SET one object to the payload as an OCTET STRING
/// from another thread — the transport waits for the response — and take
/// the octets the request bound. The manager the transport binds hands
/// bindings up as `oid=value` lines, and a line is a view of the bytes, not
/// the bytes; the agent is the far end a SET has, and the transport's own
/// codec reads the request and writes the response. A payload over the
/// ceiling is refused before anything waits on it.
pub struct SnmpRoundTrip;

impl RoundTrip for SnmpRoundTrip {
    fn transport(&self) -> &'static str {
        "snmp"
    }

    fn ceiling(&self) -> Option<usize> {
        Some(snmp_ceiling())
    }

    fn exchange(&self, payload: &[u8]) -> Exchange {
        if payload.len() > snmp_ceiling() {
            return Exchange::Failed(format!(
                "{} bytes is over the {} one SET carries in a datagram",
                payload.len(),
                snmp_ceiling()
            ));
        }
        let (socket, address) = match device_socket() {
            Ok(bound) => bound,
            Err(error) => return Exchange::Failed(format!("bind failed: {error}")),
        };
        let oid: Vec<String> = SNMP_OID.iter().map(ToString::to_string).collect();
        let target = format!("snmp+set://{address}/{}", oid.join("."));
        let payload = payload.to_vec();
        datagram_exchange(
            move || {
                SnmpTransport::new("127.0.0.1:0", "private")
                    .timing_out_after(TIMEOUT)
                    .send(&target, &payload)
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
    use crate::support::carries_the_edges;

    /// Bytes no text place takes as they are: a NUL, a line break, a byte
    /// that is not UTF-8. What comes back must be these, not a view of them.
    const OPAQUE: &[u8] = b"\x00line\r\nbreak \xff";

    fn returned(rt: &dyn RoundTrip, payload: &[u8]) -> Vec<u8> {
        match rt.exchange(payload) {
            Exchange::Returned(bytes) => bytes,
            Exchange::OneSided(why) | Exchange::Failed(why) => {
                panic!("{} did not return: {why}", rt.transport())
            }
        }
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
    fn dhcp_carries_the_edges() {
        carries_the_edges(&DhcpRoundTrip);
    }

    #[test]
    fn snmp_carries_the_edges() {
        carries_the_edges(&SnmpRoundTrip);
        assert!(
            (65_000..MAX_DATAGRAM).contains(&snmp_ceiling()),
            "{}",
            snmp_ceiling()
        );
        let brim = crate::stress::patterned(snmp_ceiling());
        assert_eq!(returned(&SnmpRoundTrip, &brim), brim);
        let over = vec![0u8; snmp_ceiling() + 1];
        assert!(matches!(SnmpRoundTrip.exchange(&over), Exchange::Failed(_)));
    }
}
