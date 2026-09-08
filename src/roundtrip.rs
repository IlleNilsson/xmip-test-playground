//! One shape the pingpong test drives, and an adapter per transport.
//!
//! The transports do not share a round-trip shape: file sends into a directory
//! and reads it back from the same place; tcp, http and smtp bind a listener,
//! accept one connection and read it while a sender connects; udp is
//! datagrams. The `Transport` trait in `xmip-core-transport` is send-and-take,
//! which fits file and not a listen/accept socket.
//!
//! So the scenario drives this smaller thing instead: [`RoundTrip::exchange`]
//! — hand it a payload, get back what returned, or why it could not. Each
//! transport gets an adapter that does its own dance behind that one method,
//! and the scenario stays one thing over all of them. Keeping every protocol
//! in mind is exactly this: a new transport is a new adapter, not a new
//! scenario.

use std::net::{TcpListener, TcpStream};
use std::time::Duration;

use transport::Transport;
use transport_file::FileTransport;
use transport_http::HttpTransport;
use transport_mllp::MllpTransport;
use transport_smtp::SmtpTransport;
use transport_tcp::TcpTransport;
use transport_udp::UdpTransport;
use transport_websocket::WebSocketTransport;

/// What one round returned.
pub enum Exchange {
    /// It came back. Compare to what was sent.
    Returned(Vec<u8>),
    /// The transport cannot round-trip on its own — one side only, or a shape
    /// the playground does not drive yet. Yellow, with the reason.
    OneSided(String),
    /// The round trip failed. Red, with the reason.
    Failed(String),
}

/// How long any adapter waits on its far end before the round is judged
/// rather than waited on: a lost datagram, a peer that never connects, a
/// broker gone quiet. Two seconds is long enough for loopback and short
/// enough that a matrix of hundreds of pairs stays a test.
pub const TIMEOUT: Duration = Duration::from_secs(2);

/// A transport the pingpong scenario can drive, behind one method.
pub trait RoundTrip {
    /// The transport token, as it appears in a scope and a repository name.
    fn transport(&self) -> &'static str;

    /// Send `payload` and return what came back. The adapter does whatever its
    /// transport needs — a directory read-back, a listen-and-accept, a
    /// datagram — behind this one call.
    fn exchange(&self, payload: &[u8]) -> Exchange;
}

/// Every implemented transport, each behind its adapter — the one list the
/// scenarios share, so a new transport is wired in a single place rather than in
/// each scenario. `file_dir` is where the file transport ping-pongs.
#[must_use]
pub fn all_transports(file_dir: impl Into<std::path::PathBuf>) -> Vec<Box<dyn RoundTrip>> {
    vec![
        Box::new(FileRoundTrip::new(file_dir)),
        Box::new(TcpRoundTrip),
        Box::new(HttpRoundTrip),
        Box::new(SmtpRoundTrip),
        Box::new(UdpRoundTrip),
        Box::new(WebSocketRoundTrip),
        Box::new(MllpRoundTrip),
        Box::new(crate::industrial::ModbusRoundTrip),
        Box::new(crate::industrial::BacnetRoundTrip),
        Box::new(crate::industrial::SerialRoundTrip),
        Box::new(crate::industrial::CanRoundTrip),
        Box::new(crate::messaging::MqttRoundTrip),
        Box::new(crate::messaging::NatsRoundTrip),
        Box::new(crate::telemetry::SyslogRoundTrip),
        Box::new(crate::telemetry::CoapRoundTrip),
        Box::new(crate::collect::FtpRoundTrip),
        Box::new(crate::collect::Pop3RoundTrip),
        Box::new(crate::record::RedisStreamsRoundTrip),
        Box::new(crate::record::DnsRoundTrip),
        Box::new(crate::collect::ImapRoundTrip),
        Box::new(crate::messaging::AmqpRoundTrip),
        Box::new(crate::industrial::Iec104RoundTrip),
        Box::new(crate::industrial::Dnp3RoundTrip),
        Box::new(crate::messaging::KafkaRoundTrip),
    ]
}

/// The listen-and-accept shape tcp, http, smtp, websocket and mllp share, with
/// the one rule that keeps a round from hanging the schedule: the accept runs
/// on its own thread and the send on this one, and when the send fails before
/// it connected — an ephemeral port exhausted, a refused connect under load —
/// the listener is poked with a throwaway connect so the accept returns and is
/// judged rather than waited on forever. Found 2026-09-08 when the matrix grew
/// to twelve contracts over seven transports and one round out of thousands
/// blocked a whole `cargo test` in `accept`.
pub(crate) fn listen_exchange<A, S>(
    listener: TcpListener,
    address: &str,
    accept: A,
    send: S,
) -> Exchange
where
    A: FnOnce(&TcpListener) -> transport::Result<transport::Arrived> + Send + 'static,
    S: FnOnce(&str) -> transport::Result<()>,
{
    let receiver = std::thread::spawn(move || accept(&listener));
    let outcome = send(address);
    if outcome.is_err() {
        let _ = TcpStream::connect(address);
    }
    match (receiver.join(), outcome) {
        (Ok(Ok(arrived)), Ok(())) => Exchange::Returned(arrived.bytes),
        (_, Err(error)) => Exchange::Failed(format!("send failed: {error}")),
        (Ok(Err(error)), Ok(())) => Exchange::Failed(format!("accept failed: {error}")),
        (Err(_), Ok(())) => Exchange::Failed("the receiving thread panicked".to_string()),
    }
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

/// File: send into a directory, read it back from the same directory. The
/// self-contained case, and the reason file was first.
pub struct FileRoundTrip {
    dir: std::path::PathBuf,
}

impl FileRoundTrip {
    #[must_use]
    pub fn new(dir: impl Into<std::path::PathBuf>) -> Self {
        Self { dir: dir.into() }
    }
}

impl RoundTrip for FileRoundTrip {
    fn transport(&self) -> &'static str {
        "file"
    }

    fn exchange(&self, payload: &[u8]) -> Exchange {
        let transport = FileTransport::new(&self.dir);

        if let Err(error) = transport.send("pingpong", payload) {
            return Exchange::Failed(format!("send failed: {error}"));
        }

        match transport.receive() {
            Ok(arrived) => match arrived.into_iter().find(|a| a.bytes == payload) {
                Some(a) => Exchange::Returned(a.bytes),
                None => Exchange::Failed("sent, but it did not come back".to_string()),
            },
            Err(error) => Exchange::Failed(format!("receive failed: {error}")),
        }
    }
}

/// TCP: bind a listener, connect and send from another thread, accept the one
/// connection and read it. The listen/accept shape http and smtp also take.
pub struct TcpRoundTrip;

impl RoundTrip for TcpRoundTrip {
    fn transport(&self) -> &'static str {
        "tcp"
    }

    fn exchange(&self, payload: &[u8]) -> Exchange {
        // Bind on an ephemeral port; the OS hands back the real address.
        let far_end = TcpTransport::new("127.0.0.1:0").timing_out_after(TIMEOUT);

        let (listener, address) = match far_end.bind() {
            Ok(bound) => bound,
            Err(error) => return Exchange::Failed(format!("bind failed: {error}")),
        };

        listen_exchange(
            listener,
            &address,
            move |listener| far_end.accept_one(listener),
            |address| TcpTransport::new("127.0.0.1:0").send(address, payload),
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

/// UDP: bind the receiving socket first (a datagram fired before the receiver
/// is bound is dropped silently), learn its address, fire one datagram from
/// another thread, receive it. A read timeout keeps a lost datagram from
/// hanging the round — UDP has no delivery guarantee.
pub struct UdpRoundTrip;

impl RoundTrip for UdpRoundTrip {
    fn transport(&self) -> &'static str {
        "udp"
    }

    fn exchange(&self, payload: &[u8]) -> Exchange {
        let far_end = UdpTransport::new("127.0.0.1:0").timing_out_after(TIMEOUT);

        // Bind before the sender fires, or the datagram is gone.
        let (socket, address) = match far_end.bind() {
            Ok(bound) => bound,
            Err(error) => return Exchange::Failed(format!("bind failed: {error}")),
        };

        // UDP cannot hang: the receive has a timeout, and a datagram that never
        // arrives is a timeout, which is what UDP is.
        let payload = payload.to_vec();
        let sender =
            std::thread::spawn(move || UdpTransport::new("127.0.0.1:0").send(&address, &payload));

        let caught = far_end.receive_one(&socket);

        match (caught, sender.join()) {
            (Ok(arrived), Ok(Ok(()))) => Exchange::Returned(arrived.bytes),
            (Err(error), _) => Exchange::Failed(format!("receive failed: {error}")),
            (_, Ok(Err(error))) => Exchange::Failed(format!("send failed: {error}")),
            (_, Err(_)) => Exchange::Failed("the sending thread panicked".to_string()),
        }
    }
}

/// The verdict every listen/accept transport reaches the same way: the payload
/// came back iff both the receive and the send half succeeded.
#[cfg(test)]
mod tests {
    use super::*;
    use crate::support::scratch;

    #[test]
    fn file_round_trips_a_payload() {
        let dir = scratch("file");
        let rt = FileRoundTrip::new(&dir);

        match rt.exchange(b"over file") {
            Exchange::Returned(bytes) => assert_eq!(bytes, b"over file"),
            other => panic!("expected Returned, got {}", label(&other)),
        }
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn tcp_round_trips_a_payload_over_a_real_socket() {
        let rt = TcpRoundTrip;

        match rt.exchange(b"over tcp") {
            Exchange::Returned(bytes) => assert_eq!(bytes, b"over tcp"),
            other => panic!("expected Returned, got {}", label(&other)),
        }
    }

    #[test]
    fn tcp_carries_binary_unharmed() {
        let rt = TcpRoundTrip;
        let payload = [0x00u8, 0x01, 0xfe, 0xff];

        match rt.exchange(&payload) {
            Exchange::Returned(bytes) => assert_eq!(bytes, payload),
            other => panic!("expected Returned, got {}", label(&other)),
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
    fn udp_round_trips_a_datagram() {
        let rt = UdpRoundTrip;
        let payload = [0x00u8, 0x01, 0x02, 0xfd, 0xfe, 0xff];

        match rt.exchange(&payload) {
            Exchange::Returned(bytes) => assert_eq!(bytes, payload),
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

    fn label(exchange: &Exchange) -> String {
        match exchange {
            Exchange::Returned(_) => "Returned".to_string(),
            Exchange::OneSided(why) => format!("OneSided({why})"),
            Exchange::Failed(why) => format!("Failed({why})"),
        }
    }
}
