//! The telemetry round trips: syslog and coap, each behind the same
//! [`RoundTrip`] the pingpong scenario drives.
//!
//! Syslog rides TCP with octet counting here, the carrier that holds a
//! megabyte; the datagram carrier is the one the transport binds by default
//! and it is exercised in the transport's own tests. CoAP carries at most a
//! kilobyte per message, so a Stream travels as confirmable POSTs in turn,
//! each acknowledged, and an empty one to close — what block-wise transfer
//! will do above it, done here so the scenario stays one thing.

use std::time::Duration;

use transport::Transport;
use transport_coap::{CoapTransport, MAX_PAYLOAD, message};
use transport_syslog::{Carrier, SyslogTransport};

use crate::roundtrip::{Exchange, RoundTrip, TIMEOUT, listen_exchange};

/// Syslog over TCP: bind a collector, send the payload as one octet-counted
/// message, and take its MSG back.
pub struct SyslogRoundTrip;

impl RoundTrip for SyslogRoundTrip {
    fn transport(&self) -> &'static str {
        "syslog"
    }

    fn exchange(&self, payload: &[u8]) -> Exchange {
        let far_end = SyslogTransport::new("127.0.0.1:0", "playground", "collector")
            .over(Carrier::Tcp)
            .timing_out_after(TIMEOUT);
        let (listener, address) = match far_end.bind_tcp() {
            Ok(bound) => bound,
            Err(error) => return Exchange::Failed(format!("bind failed: {error}")),
        };
        listen_exchange(
            listener,
            &address,
            move |listener| {
                let mut connection = far_end.accept_one(listener)?;
                connection.next_message()?.ok_or_else(|| {
                    transport::error::protocol_error("the sender closed without a message")
                })
            },
            |address| {
                SyslogTransport::new("127.0.0.1:0", "playground", "probe")
                    .send(&format!("syslog+tcp://{address}"), payload)
            },
        )
    }
}

/// CoAP: bind a server, POST the payload as confirmable messages of at most
/// a kilobyte in turn, each acknowledged before the next goes, then an empty
/// POST to close; the server takes them in order, a retransmitted one only
/// once. One message in flight is block-wise transfer with a window of one,
/// and it is the flow control that keeps a burst from overrunning the far
/// end's socket: sent non-confirmable, a mebibyte lost its tail on loopback
/// (2026-09-09), which was UDP being honest about a sender without one.
pub struct CoapRoundTrip;

impl RoundTrip for CoapRoundTrip {
    fn transport(&self) -> &'static str {
        "coap"
    }

    fn exchange(&self, payload: &[u8]) -> Exchange {
        let far_end = CoapTransport::new("127.0.0.1:0").timing_out_after(TIMEOUT);
        let (socket, address) = match far_end.bind() {
            Ok(bound) => bound,
            Err(error) => return Exchange::Failed(format!("bind failed: {error}")),
        };
        let payload = payload.to_vec();
        let sender = std::thread::spawn(move || {
            // Loopback acknowledges within a millisecond; a hundred, doubled
            // on every retransmission, keeps a far end that went away inside
            // the bound a round is judged by.
            let near_end =
                CoapTransport::new("127.0.0.1:0").acknowledged_within(Duration::from_millis(100));
            let target = format!("coap://{address}/probe");
            for block in payload.chunks(MAX_PAYLOAD) {
                near_end.send(&target, block)?;
            }
            near_end.send(&target, &[])
        });
        let mut bytes = Vec::new();
        let mut last_seen: Option<(String, u16)> = None;
        let caught = loop {
            let request = match far_end.receive_one(&socket) {
                Ok(request) => request,
                Err(error) => break Err(error),
            };
            if let Err(error) = far_end.respond(&socket, &request, message::CHANGED, &[]) {
                break Err(error);
            }
            let seen = (request.peer.clone(), request.message.id);
            if last_seen.as_ref() == Some(&seen) {
                continue;
            }
            last_seen = Some(seen);
            if request.message.payload.is_empty() {
                break Ok(());
            }
            bytes.extend_from_slice(&request.message.payload);
        };
        match (caught, sender.join()) {
            (Ok(()), Ok(Ok(()))) => Exchange::Returned(bytes),
            (Err(error), _) => Exchange::Failed(format!("receive failed: {error}")),
            (_, Ok(Err(error))) => Exchange::Failed(format!("send failed: {error}")),
            (_, Err(_)) => Exchange::Failed("the sending thread panicked".to_string()),
        }
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

    #[test]
    fn syslog_carries_a_stream_as_one_message() {
        let long = vec![0x2a; 100_000];
        assert_eq!(
            returned(&SyslogRoundTrip, b"line\r\nbreak"),
            b"line\r\nbreak"
        );
        assert_eq!(returned(&SyslogRoundTrip, &long), long);
        assert_eq!(returned(&SyslogRoundTrip, b""), b"");
        assert_eq!(returned(&SyslogRoundTrip, b"<probe/>"), b"<probe/>");
    }

    #[test]
    fn coap_carries_a_stream_as_posts_in_turn() {
        let long = vec![0x2a; 5000];
        assert_eq!(returned(&CoapRoundTrip, b"post"), b"post");
        assert_eq!(returned(&CoapRoundTrip, &long), long);
        assert_eq!(returned(&CoapRoundTrip, b""), b"");
    }

    #[test]
    fn syslog_carries_the_edges() {
        crate::support::carries_the_edges(&SyslogRoundTrip);
    }

    #[test]
    fn coap_carries_the_edges() {
        crate::support::carries_the_edges(&CoapRoundTrip);
    }
}
