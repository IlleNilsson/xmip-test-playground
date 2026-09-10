//! The reply round trips: http, smtp, websocket and mllp — the transports
//! whose far end answers on the connection the sender holds open, each
//! behind the same [`RoundTrip`] the pingpong scenario drives.
//!
//! All four are the listen-and-accept shape `listen_exchange` keeps: a
//! listener bound on loopback, the send from another thread, the accept
//! reading what arrived and answering it — a status line, a 250, a close
//! frame, an acknowledgement — so the sender's wait for the reply is real.
//! They lived in `roundtrip.rs` beside the trait until 2026-09-10, when the
//! edge-payload tests pushed that file past the gate.

use transport::Transport;
use transport_http::HttpTransport;
use transport_mllp::MllpTransport;
use transport_smtp::SmtpTransport;
use transport_websocket::WebSocketTransport;

use crate::roundtrip::{Exchange, RoundTrip, TIMEOUT, listen_exchange};

/// Why a mail path cannot carry `payload` as it is: DATA is lines ending in
/// CRLF with a leading period stuffed, so a bare LF becomes CRLF on the way
/// (a lone CR inside a line survives, which is why HL7 v2 travels). Text
/// mail survives; bytes with bare LFs do not, and the adapter says so rather
/// than calling a changed payload delivered.
pub(crate) fn mail_refusal(payload: &[u8]) -> Option<String> {
    let mut at = 0;
    while at < payload.len() {
        match payload[at] {
            b'\r' if payload.get(at + 1) == Some(&b'\n') => at += 2,
            b'\n' => {
                return Some("a bare LF is canonicalised to CRLF by DATA".to_string());
            }
            _ => at += 1,
        }
    }
    None
}

/// MLLP: bind a listener, send one framed message from another thread, accept
/// it, acknowledge on the same connection, and read the message back. The tcp
/// shape with HL7's framing on top and the acknowledgement the sender waits for.
pub struct MllpRoundTrip;

impl RoundTrip for MllpRoundTrip {
    fn transport(&self) -> &'static str {
        "mllp"
    }

    fn exchange(&self, payload: &[u8]) -> Exchange {
        let far_end = MllpTransport::new("127.0.0.1:0").timing_out_after(TIMEOUT);
        let (listener, address) = match far_end.bind() {
            Ok(bound) => bound,
            Err(error) => return Exchange::Failed(format!("bind failed: {error}")),
        };
        let timeout = TIMEOUT;
        listen_exchange(
            listener,
            &address,
            move |listener| {
                let (arrived, mut connection) = far_end.accept_one(listener)?;
                // The acknowledgement is HL7's to compose; the probe answers with
                // the bytes it got, which proves the reply channel and nothing more.
                transport_mllp::acknowledge(&mut connection, &arrived.bytes)?;
                Ok(arrived)
            },
            |address| transport_mllp::send_and_receive(address, payload, Some(timeout)).map(|_| ()),
        )
    }
}

/// HTTP: bind a listener, send the payload as a request body from another
/// thread, accept the one request and read the body back. The tcp shape with
/// HTTP framing on top — the server writes a response, so the sender's `send`
/// completes rather than blocking on a reply.
pub struct HttpRoundTrip;

impl HttpRoundTrip {
    #[must_use]
    pub const fn new() -> Self {
        Self
    }
}

impl Default for HttpRoundTrip {
    fn default() -> Self {
        Self::new()
    }
}

impl RoundTrip for HttpRoundTrip {
    fn transport(&self) -> &'static str {
        "http"
    }

    fn exchange(&self, payload: &[u8]) -> Exchange {
        let far_end = HttpTransport::new("127.0.0.1:0");

        let (listener, address) = match far_end.bind() {
            Ok(bound) => bound,
            Err(error) => return Exchange::Failed(format!("bind failed: {error}")),
        };

        listen_exchange(
            listener,
            &address,
            move |listener| far_end.accept_one(listener),
            |address| {
                HttpTransport::new("127.0.0.1:0")
                    .send(&format!("http://{address}/pingpong"), payload)
            },
        )
    }
}

/// SMTP: bind a receiver, relay the payload as one message from another thread,
/// accept the one session and read the message body back.
pub struct SmtpRoundTrip;

impl SmtpRoundTrip {
    #[must_use]
    pub const fn new() -> Self {
        Self
    }
}

impl Default for SmtpRoundTrip {
    fn default() -> Self {
        Self::new()
    }
}

impl RoundTrip for SmtpRoundTrip {
    fn transport(&self) -> &'static str {
        "smtp"
    }

    /// The mail path's line rule, and one more of SMTP's own: the CRLF that
    /// ends the last line belongs to the `<CRLF>.<CRLF>` terminator, so a
    /// body ending in CRLF comes back one line ending short.
    fn refuses(&self, payload: &[u8]) -> Option<String> {
        mail_refusal(payload).or_else(|| {
            payload
                .ends_with(b"\r\n")
                .then(|| "a trailing CRLF is absorbed by the DATA terminator".to_string())
        })
    }

    fn exchange(&self, payload: &[u8]) -> Exchange {
        let far_end = SmtpTransport::receiving("127.0.0.1:0");

        let (listener, address) = match far_end.bind() {
            Ok(bound) => bound,
            Err(error) => return Exchange::Failed(format!("bind failed: {error}")),
        };

        listen_exchange(
            listener,
            &address,
            move |listener| far_end.accept_one(listener),
            |address| {
                SmtpTransport::sending(address.to_string(), "xmip@example.com")
                    .send("mailto:pingpong@example.com", payload)
            },
        )
    }
}

/// WebSocket: the http upgrade shape. Bind, then from another thread connect,
/// complete the opening handshake and send one frame; accept the connection,
/// finish the handshake, read the frame.
pub struct WebSocketRoundTrip;

impl WebSocketRoundTrip {
    #[must_use]
    pub const fn new() -> Self {
        Self
    }
}

impl Default for WebSocketRoundTrip {
    fn default() -> Self {
        Self::new()
    }
}

impl RoundTrip for WebSocketRoundTrip {
    fn transport(&self) -> &'static str {
        "websocket"
    }

    fn exchange(&self, payload: &[u8]) -> Exchange {
        let far_end = WebSocketTransport::new("127.0.0.1:0");

        let (listener, address) = match far_end.bind() {
            Ok(bound) => bound,
            Err(error) => return Exchange::Failed(format!("bind failed: {error}")),
        };

        listen_exchange(
            listener,
            &address,
            move |listener| far_end.accept_one(listener),
            |address| {
                WebSocketTransport::new("127.0.0.1:0")
                    .send(&format!("ws://{address}/pingpong"), payload)
            },
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::support::carries_the_edges;

    fn label(exchange: &Exchange) -> String {
        match exchange {
            Exchange::Returned(_) => "Returned".to_string(),
            Exchange::OneSided(why) => format!("OneSided({why})"),
            Exchange::Failed(why) => format!("Failed({why})"),
        }
    }

    #[test]
    fn http_round_trips_a_body() {
        let rt = HttpRoundTrip;

        match rt.exchange(b"<order/>") {
            Exchange::Returned(bytes) => assert_eq!(bytes, b"<order/>"),
            other => panic!("expected Returned, got {}", label(&other)),
        }
    }

    #[test]
    fn smtp_round_trips_a_message() {
        let rt = SmtpRoundTrip;

        match rt.exchange(b"Subject: ping\r\n\r\npong") {
            Exchange::Returned(bytes) => assert_eq!(bytes, b"Subject: ping\r\n\r\npong"),
            other => panic!("expected Returned, got {}", label(&other)),
        }
    }

    #[test]
    fn websocket_round_trips_a_frame() {
        let rt = WebSocketRoundTrip;
        let payload = [0x00u8, 0x01, 0x02, 0xfd, 0xfe, 0xff];

        match rt.exchange(&payload) {
            Exchange::Returned(bytes) => assert_eq!(bytes, payload),
            other => panic!("expected Returned, got {}", label(&other)),
        }
    }

    #[test]
    fn http_carries_the_edges() {
        carries_the_edges(&HttpRoundTrip);
    }

    #[test]
    fn smtp_carries_the_edges() {
        carries_the_edges(&SmtpRoundTrip);
    }

    #[test]
    fn websocket_carries_the_edges() {
        carries_the_edges(&WebSocketRoundTrip);
    }

    #[test]
    fn mllp_carries_the_edges() {
        carries_the_edges(&MllpRoundTrip);
    }
}
