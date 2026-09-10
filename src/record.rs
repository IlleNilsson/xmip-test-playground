//! The record round trips: redis-streams and dns, each behind the same
//! [`RoundTrip`] the pingpong scenario drives.
//!
//! Both carry a Stream as a record something else keeps: an entry appended
//! to a Redis stream, a TXT record added to a zone. The far end is the
//! transport's own one-client session or server, the near end appends or
//! updates, and what the far end took is what came back. DNS carries at most
//! a few kilobytes over UDP and says so; that ceiling shows red at load
//! without any injection, as UDP's does.

use std::sync::OnceLock;

use transport::Transport;
use transport::error::protocol_error;
use transport_dns::{Carrier, MAX_MESSAGE};
use transport_dns::{DnsTransport, Message, UDP_EDNS, message};
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

/// The zone the update adds to, and the name it adds under.
const ZONE: &str = "xmip.playground.";
const NAME: &str = "probe.xmip.playground.";

/// The most one update carries: the largest payload whose update — the zone
/// question, one TXT record of it in strings of 255, the OPT record asking
/// for [`UDP_EDNS`] — encodes within the datagram, found through the
/// transport's own encoder once and remembered.
fn dns_ceiling() -> usize {
    static CEILING: OnceLock<usize> = OnceLock::new();
    *CEILING.get_or_init(|| {
        let fits = |bytes: usize| {
            let update = Message::update_adding_txt(0, ZONE, NAME, &vec![0; bytes]).with_edns();
            message::encode(&update).is_ok_and(|wire| wire.len() <= UDP_EDNS)
        };
        (0..=UDP_EDNS).rev().find(|&bytes| fits(bytes)).unwrap_or(0)
    })
}

/// DNS: bind a server for one zone, send an update adding a TXT record that
/// carries the payload, and take the update's payload as what came back.
/// Under the datagram ceiling the update travels as UDP with EDNS; above it,
/// as TCP with a length prefix, which is what a resolver does too (RFC 7766).
/// The ceiling is then the message's own sixteen-bit length.
pub struct DnsRoundTrip;

impl RoundTrip for DnsRoundTrip {
    fn transport(&self) -> &'static str {
        "dns"
    }

    fn ceiling(&self) -> Option<usize> {
        Some(tcp_ceiling())
    }

    fn exchange(&self, payload: &[u8]) -> Exchange {
        if payload.len() > tcp_ceiling() {
            return Exchange::Failed(format!(
                "{} bytes is over the {} one update carries in a message",
                payload.len(),
                tcp_ceiling()
            ));
        }
        if payload.len() <= dns_ceiling() {
            return over_udp(payload);
        }
        over_tcp(payload)
    }
}

fn over_udp(payload: &[u8]) -> Exchange {
    let far_end = DnsTransport::new("127.0.0.1:0", ZONE, NAME).timing_out_after(TIMEOUT);
    let (socket, address) = match far_end.bind_udp() {
        Ok(bound) => bound,
        Err(error) => return Exchange::Failed(format!("bind failed: {error}")),
    };
    let payload = payload.to_vec();
    let timeout = TIMEOUT;
    let sender = std::thread::spawn(move || {
        DnsTransport::new("127.0.0.1:0", ZONE, NAME)
            .timing_out_after(timeout)
            .send(&address, &payload)
    });
    judge(far_end.receive_datagram(&socket), sender.join())
}

fn over_tcp(payload: &[u8]) -> Exchange {
    let far_end = DnsTransport::new("127.0.0.1:0", ZONE, NAME).timing_out_after(TIMEOUT);
    let (listener, address) = match far_end.bind_tcp() {
        Ok(bound) => bound,
        Err(error) => return Exchange::Failed(format!("bind failed: {error}")),
    };
    let payload = payload.to_vec();
    let timeout = TIMEOUT;
    let sender = std::thread::spawn(move || {
        DnsTransport::new("127.0.0.1:0", ZONE, NAME)
            .over(Carrier::Tcp)
            .timing_out_after(timeout)
            .send(&address, &payload)
    });
    judge(far_end.receive_connection(&listener), sender.join())
}

fn judge(
    received: transport::Result<transport::Arrived>,
    sent: std::thread::Result<transport::Result<()>>,
) -> Exchange {
    match (received, sent) {
        (Ok(arrived), Ok(Ok(()))) => Exchange::Returned(arrived.bytes),
        (_, Ok(Err(error))) => Exchange::Failed(format!("update failed: {error}")),
        (Err(error), _) => Exchange::Failed(format!("receive failed: {error}")),
        (_, Err(_)) => Exchange::Failed("the sending thread panicked".to_string()),
    }
}

/// The most one update carries as a TCP message: the largest message whose
/// length still fits the sixteen-bit prefix.
fn tcp_ceiling() -> usize {
    static CEILING: OnceLock<usize> = OnceLock::new();
    *CEILING.get_or_init(|| {
        let fits = |bytes: usize| {
            let update = Message::update_adding_txt(0, ZONE, NAME, &vec![0; bytes]);
            message::encode(&update).is_ok_and(|wire| wire.len() <= MAX_MESSAGE)
        };
        (0..=MAX_MESSAGE)
            .rev()
            .find(|&bytes| fits(bytes))
            .unwrap_or(0)
    })
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

    #[test]
    fn redis_streams_carries_the_edges() {
        crate::support::carries_the_edges(&RedisStreamsRoundTrip);
    }

    #[test]
    fn dns_carries_the_edges() {
        crate::support::carries_the_edges(&DnsRoundTrip);
        assert!(
            (3_000..UDP_EDNS).contains(&dns_ceiling()),
            "{}",
            dns_ceiling()
        );
        // The datagram's brim goes as UDP, one byte more as TCP, and past the
        // message's own length nothing goes.
        let brim = crate::stress::patterned(dns_ceiling());
        assert_eq!(returned(&DnsRoundTrip, &brim), brim);
        let over = crate::stress::patterned(dns_ceiling() + 1);
        assert_eq!(returned(&DnsRoundTrip, &over), over);
        let past = vec![0u8; tcp_ceiling() + 1];
        assert!(matches!(DnsRoundTrip.exchange(&past), Exchange::Failed(_)));
    }
}
