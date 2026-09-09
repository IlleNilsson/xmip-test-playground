//! The factory floor round trips: cotp, s7comm and secs-gem, each behind the
//! same [`RoundTrip`] the pingpong scenario drives.
//!
//! COTP is ISO transport on TCP (RFC 1006): a connect handshake between two
//! TSAPs, then one message segmented to the negotiated TPDU size, the last
//! segment marked, and DR to close — the carrier Siemens S7 and IEC 61850
//! MMS ride on. S7comm is what a Siemens CPU speaks over it, and a delivery
//! is a write: bytes at an address, in as many jobs as the PDU length takes.
//! SECS/GEM is the semiconductor pair — HSMS carrying SECS-II messages
//! addressed by stream and function, S6F11 for an event report. None of
//! them has a device on the build box, so each adapter binds the
//! transport's own far end — a COTP listener, one session's worth of CPU
//! holding a data block, the passive HSMS side — and a client connects,
//! delivers the payload once and says goodbye, and what the far end took is
//! what came back: the message reassembled, the data block after the write,
//! the body of the one event.

use transport::Transport;
use transport::error::protocol_error;
use transport_cotp::CotpTransport;
use transport_s7comm::{Area, S7Transport};
use transport_secs_gem::SecsGemTransport;

use crate::roundtrip::{Exchange, RoundTrip, TIMEOUT, listen_exchange};

/// COTP: bind a listener, connect a caller between the default TSAPs, send
/// the payload as one message in as many DT segments as the TPDU size takes,
/// and take it reassembled up to the segment with EOT set.
pub struct CotpRoundTrip;

impl RoundTrip for CotpRoundTrip {
    fn transport(&self) -> &'static str {
        "cotp"
    }

    fn exchange(&self, payload: &[u8]) -> Exchange {
        let far_end = CotpTransport::new("127.0.0.1:0").timing_out_after(TIMEOUT);
        let (listener, address) = match far_end.bind() {
            Ok(bound) => bound,
            Err(error) => return Exchange::Failed(format!("bind failed: {error}")),
        };
        let timeout = TIMEOUT;
        listen_exchange(
            listener,
            &address,
            move |listener| {
                let mut connection = far_end.accept_one(listener)?;
                let message = connection
                    .next_data()?
                    .ok_or_else(|| protocol_error("the caller disconnected without a message"))?;
                // See the DR that follows, so the goodbye is read rather than
                // met with a closed socket.
                connection.next_data()?;
                Ok(transport::Arrived::new(connection.origin(), message))
            },
            |address| {
                CotpTransport::new("127.0.0.1:0")
                    .timing_out_after(timeout)
                    .send(address, payload)
            },
        )
    }
}

/// The address a delivery is written at: data block 1 from byte 0, the
/// payload's length as the range.
const BLOCK: &str = "DB1.DBB0";

/// S7comm: bind a session holding data block 1 sized to the payload, connect
/// a client, write the payload there in as many jobs as the PDU length takes,
/// and take the block as it is once the client disconnects. An empty payload
/// is a write of no jobs and an empty block: Setup Communication, then DR.
pub struct S7RoundTrip;

impl RoundTrip for S7RoundTrip {
    fn transport(&self) -> &'static str {
        "s7comm"
    }

    fn exchange(&self, payload: &[u8]) -> Exchange {
        let far_end = S7Transport::new("127.0.0.1:0", BLOCK).timing_out_after(TIMEOUT);
        let (listener, address) = match far_end.bind() {
            Ok(bound) => bound,
            Err(error) => return Exchange::Failed(format!("bind failed: {error}")),
        };
        let timeout = TIMEOUT;
        let length = payload.len();
        listen_exchange(
            listener,
            &address,
            move |listener| {
                let mut session =
                    far_end
                        .accept_one(listener)?
                        .with_area(Area::DataBlock, 1, vec![0u8; length]);
                session.serve()?;
                let block = session
                    .area(Area::DataBlock, 1)
                    .ok_or_else(|| protocol_error("the data block went missing"))?
                    .to_vec();
                Ok(transport::Arrived::new(session.origin(), block))
            },
            |address| {
                S7Transport::new(address, BLOCK)
                    .timing_out_after(timeout)
                    .send(address, payload)
            },
        )
    }
}

/// SECS/GEM: bind the passive side, connect the active one and select, send
/// the payload as the body of one S6F11 without waiting for a reply, and
/// take that one data message's body.
pub struct SecsGemRoundTrip;

impl RoundTrip for SecsGemRoundTrip {
    fn transport(&self) -> &'static str {
        "secs-gem"
    }

    fn exchange(&self, payload: &[u8]) -> Exchange {
        let far_end = SecsGemTransport::new("127.0.0.1:0").timing_out_after(TIMEOUT);
        let (listener, address) = match far_end.bind() {
            Ok(bound) => bound,
            Err(error) => return Exchange::Failed(format!("bind failed: {error}")),
        };
        let timeout = TIMEOUT;
        listen_exchange(
            listener,
            &address,
            move |listener| {
                let mut connection = far_end.accept_one(listener)?;
                let message = connection
                    .next_data()?
                    .ok_or_else(|| protocol_error("the host separated without a message"))?;
                // See the Separate.req that follows, so the goodbye is read.
                connection.next_data()?;
                let origin = connection.origin(&message.header);
                Ok(transport::Arrived::new(origin, message.body))
            },
            |address| {
                SecsGemTransport::new("127.0.0.1:0")
                    .timing_out_after(timeout)
                    .send(address, payload)
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

    /// Three thousand bytes that are not all alike, so a segment out of
    /// order or a job written at the wrong offset would show.
    fn long() -> Vec<u8> {
        (0..=255u8).cycle().take(3000).collect()
    }

    #[test]
    fn cotp_carries_a_message_as_segments() {
        let long = long();
        assert_eq!(returned(&CotpRoundTrip, b"one tpdu"), b"one tpdu");
        assert_eq!(returned(&CotpRoundTrip, &long), long);
        assert_eq!(returned(&CotpRoundTrip, b""), b"");
    }

    #[test]
    fn s7comm_writes_a_stream_into_a_data_block() {
        let long = long();
        assert_eq!(returned(&S7RoundTrip, b"write var"), b"write var");
        assert_eq!(returned(&S7RoundTrip, &long), long);
        assert_eq!(returned(&S7RoundTrip, b""), b"");
    }

    #[test]
    fn secs_gem_carries_a_stream_as_one_event_report() {
        let long = long();
        assert_eq!(returned(&SecsGemRoundTrip, b"S6F11"), b"S6F11");
        assert_eq!(returned(&SecsGemRoundTrip, &long), long);
        assert_eq!(returned(&SecsGemRoundTrip, b""), b"");
    }
}
