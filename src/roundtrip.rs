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
use transport_tcp::TcpTransport;
use transport_udp::UdpTransport;

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

/// A transport the pingpong scenario can drive, behind one method. `Send`
/// and `Sync` so a schedule can drive pairs from several threads at once —
/// an adapter holds a directory or nothing, never a live socket.
pub trait RoundTrip: Send + Sync {
    /// The transport token, as it appears in a scope and a repository name.
    fn transport(&self) -> &'static str;

    /// The largest payload this transport carries whole in one round, or
    /// `None` when it carries any. A datagram protocol has one — 65 507
    /// bytes of UDP, less what its own header takes — and above it the
    /// adapter answers a refusal with a reason, never a hang. The stress
    /// tests drive every edge payload under the ceiling and expect it back,
    /// and every one above it and expect the refusal.
    fn ceiling(&self) -> Option<usize> {
        None
    }

    /// Why this transport cannot carry `payload` as it is, or `None` when it
    /// can. A ceiling is about size; this is about content — a mail path
    /// canonicalises line endings, an MLLP block cannot hold its own
    /// terminator. A refusal is judged one-sided, yellow with the reason:
    /// the transport declares the shape it does not carry rather than
    /// changing the bytes and calling that delivered.
    fn refuses(&self, payload: &[u8]) -> Option<String> {
        let _ = payload;
        None
    }

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
        Box::new(crate::reply::HttpRoundTrip),
        Box::new(crate::reply::SmtpRoundTrip),
        Box::new(UdpRoundTrip),
        Box::new(crate::reply::WebSocketRoundTrip),
        Box::new(crate::reply::MllpRoundTrip),
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
        Box::new(crate::factory::CotpRoundTrip),
        Box::new(crate::factory::S7RoundTrip),
        Box::new(crate::factory::SecsGemRoundTrip),
        Box::new(crate::storage::S3RoundTrip),
        Box::new(crate::storage::AzureBlobRoundTrip),
        Box::new(crate::storage::GcsRoundTrip),
        Box::new(crate::storage::WebDavRoundTrip),
        Box::new(crate::broker::ActiveMqRoundTrip),
        Box::new(crate::broker::RabbitMqRoundTrip),
        Box::new(crate::broker::JetStreamRoundTrip),
        Box::new(crate::broker::RedpandaRoundTrip),
        Box::new(crate::broker::PostgresqlRoundTrip),
        Box::new(crate::discovery::SsdpRoundTrip),
        Box::new(crate::discovery::MdnsRoundTrip),
        Box::new(crate::management::DhcpRoundTrip),
        Box::new(crate::management::SnmpRoundTrip),
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
        // One directory per thread: pairs driven at once from several
        // threads would otherwise pick up each other's file and report it as
        // "sent, but it did not come back" (found at Harsh, 2026-09-10). Per
        // thread rather than per exchange so the directory is made once and
        // the round stays as fast as the file transport is.
        let dir = self.dir.join(format!("t{:?}", std::thread::current().id()));
        if let Err(error) = std::fs::create_dir_all(&dir) {
            return Exchange::Failed(format!("creating the exchange directory: {error}"));
        }
        let transport = FileTransport::new(&dir);

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

/// The most one IPv4 datagram carries: 65 535 less the twenty bytes of the
/// IP header and the eight of the UDP header (RFC 791, RFC 768).
const UDP_DATAGRAM: usize = 65_507;

/// UDP: bind the receiving socket first (a datagram fired before the receiver
/// is bound is dropped silently), learn its address, fire one datagram from
/// another thread, receive it. A read timeout keeps a lost datagram from
/// hanging the round — UDP has no delivery guarantee. A payload over
/// [`UDP_DATAGRAM`] is refused before anything waits on it.
pub struct UdpRoundTrip;

impl RoundTrip for UdpRoundTrip {
    fn transport(&self) -> &'static str {
        "udp"
    }

    fn ceiling(&self) -> Option<usize> {
        Some(UDP_DATAGRAM)
    }

    fn exchange(&self, payload: &[u8]) -> Exchange {
        if payload.len() > UDP_DATAGRAM {
            return Exchange::Failed(format!(
                "{} bytes is over the {UDP_DATAGRAM} one datagram carries",
                payload.len()
            ));
        }
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
    fn udp_round_trips_a_datagram() {
        let rt = UdpRoundTrip;
        let payload = [0x00u8, 0x01, 0x02, 0xfd, 0xfe, 0xff];

        match rt.exchange(&payload) {
            Exchange::Returned(bytes) => assert_eq!(bytes, payload),
            other => panic!("expected Returned, got {}", label(&other)),
        }
    }

    #[test]
    fn file_carries_the_edges() {
        let dir = scratch("file-edges");
        crate::support::carries_the_edges(&FileRoundTrip::new(&dir));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn tcp_carries_the_edges() {
        crate::support::carries_the_edges(&TcpRoundTrip);
    }

    #[test]
    fn udp_carries_the_edges() {
        crate::support::carries_the_edges(&UdpRoundTrip);
        let brim = crate::stress::patterned(UDP_DATAGRAM);
        assert!(matches!(UdpRoundTrip.exchange(&brim), Exchange::Returned(back) if back == brim));
        let over = vec![0u8; UDP_DATAGRAM + 1];
        assert!(matches!(UdpRoundTrip.exchange(&over), Exchange::Failed(_)));
    }

    fn label(exchange: &Exchange) -> String {
        match exchange {
            Exchange::Returned(_) => "Returned".to_string(),
            Exchange::OneSided(why) => format!("OneSided({why})"),
            Exchange::Failed(why) => format!("Failed({why})"),
        }
    }
}
