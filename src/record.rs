//! The record round trips: redis-streams and dns, each behind the same
//! [`RoundTrip`] the pingpong scenario drives.
//!
//! Both carry a Stream as a record something else keeps: an entry appended
//! to a Redis stream, a TXT record added to a zone. The far end is the
//! transport's own one-client session or server, the near end appends or
//! updates, and what the far end took is what came back. DNS carries at most
//! a few kilobytes over UDP and says so; that ceiling shows red at load
//! without any injection, as UDP's does.

use transport::Transport;
use transport::error::protocol_error;
use transport_dns::DnsTransport;
use transport_redis_streams::RedisStreamsTransport;

use crate::roundtrip::{Exchange, RoundTrip, TIMEOUT, listen_exchange};

/// Redis Streams: bind a session, connect a client, XADD the payload as
/// one entry, and take it as the session's one add.
pub struct RedisStreamsRoundTrip;

impl RoundTrip for RedisStreamsRoundTrip {
    fn transport(&self) -> &'static str {
        "redis-streams"
    }

    fn exchange(&self, payload: &[u8]) -> Exchange {
        let far_end = RedisStreamsTransport::new("127.0.0.1:0", "probe").timing_out_after(TIMEOUT);
        let (listener, address) = match far_end.bind() {
            Ok(bound) => bound,
            Err(error) => return Exchange::Failed(format!("bind failed: {error}")),
        };
        let timeout = TIMEOUT;
        listen_exchange(
            listener,
            &address,
            move |listener| {
                let mut session = far_end.accept_one(listener)?;
                session
                    .next_add()?
                    .ok_or_else(|| protocol_error("the client closed without appending"))
            },
            |address| {
                RedisStreamsTransport::new(address, "probe")
                    .timing_out_after(timeout)
                    .send("probe", payload)
            },
        )
    }
}

/// DNS: bind a server for one zone, send an update adding a TXT record that
/// carries the payload, and take the update's payload as what came back.
pub struct DnsRoundTrip;

impl RoundTrip for DnsRoundTrip {
    fn transport(&self) -> &'static str {
        "dns"
    }

    fn exchange(&self, payload: &[u8]) -> Exchange {
        let far_end =
            DnsTransport::new("127.0.0.1:0", "xmip.playground.", "probe.xmip.playground.")
                .timing_out_after(TIMEOUT);
        let (socket, address) = match far_end.bind_udp() {
            Ok(bound) => bound,
            Err(error) => return Exchange::Failed(format!("bind failed: {error}")),
        };
        // The datagram ceiling, judged before anything waits on it: a
        // payload the update cannot frame never reaches the wire.
        let update = transport_dns::Message::update_adding_txt(0, "xmip.playground.", "p", payload)
            .with_edns();
        match transport_dns::message::encode(&update) {
            Ok(bytes) if bytes.len() <= transport_dns::UDP_EDNS => {}
            Ok(bytes) => {
                return Exchange::Failed(format!(
                    "an update of {} bytes is over the {} a datagram carries",
                    bytes.len(),
                    transport_dns::UDP_EDNS
                ));
            }
            Err(error) => return Exchange::Failed(format!("update failed: {error}")),
        }
        let payload = payload.to_vec();
        let timeout = TIMEOUT;
        let sender = std::thread::spawn(move || {
            DnsTransport::new("127.0.0.1:0", "xmip.playground.", "probe.xmip.playground.")
                .timing_out_after(timeout)
                .send(&address, &payload)
        });
        let received = far_end.receive_datagram(&socket);
        match (received, sender.join()) {
            (Ok(arrived), Ok(Ok(()))) => Exchange::Returned(arrived.bytes),
            (_, Ok(Err(error))) => Exchange::Failed(format!("update failed: {error}")),
            (Err(error), _) => Exchange::Failed(format!("receive failed: {error}")),
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
    fn redis_streams_appends_and_dns_updates() {
        let long = vec![0x2a; 100_000];
        assert_eq!(returned(&RedisStreamsRoundTrip, b"entry"), b"entry");
        assert_eq!(returned(&RedisStreamsRoundTrip, &long), long);
        assert_eq!(returned(&RedisStreamsRoundTrip, b""), b"");
        assert_eq!(returned(&DnsRoundTrip, b"txt"), b"txt");
        assert_eq!(returned(&DnsRoundTrip, &long[..3000]), long[..3000]);
        assert_eq!(returned(&DnsRoundTrip, b""), b"");
        assert!(matches!(DnsRoundTrip.exchange(&long), Exchange::Failed(_)));
    }
}
