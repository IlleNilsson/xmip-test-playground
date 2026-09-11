//! One shape the pingpong test drives, and the transport's own far end behind it.
//!
//! The transports do not share a round-trip shape: file sends into a directory
//! and reads it back from the same place; tcp, http and smtp bind a listener,
//! accept one connection and read it while a sender connects; udp is
//! datagrams. The `Transport` trait in `xmip-core-transport` is send-and-take,
//! which fits file and not a listen/accept socket.
//!
//! So the scenario drives this smaller thing instead: [`RoundTrip::exchange`]
//! — hand it a payload, get back what returned, or why it could not. Until
//! 2026-09-11 each transport got an adapter here that did its own dance
//! behind that one method, forty-two of them. The dance belongs with the
//! protocol (ADR-0051): a technology implements [`Loopback`] in its own crate
//! — the far end, the near end, the ceiling, the refusals — and [`Looped`]
//! is the one adapter over all of them. A new transport is a `loopback()`
//! constructor in its crate and one line in [`all_transports`]. The adapters
//! still written here are the ones not yet moved.

use std::net::{TcpListener, TcpStream};
use std::time::Duration;

use transport::{LOOPBACK_TIMEOUT, Loopback};
use transport_file::FileTransport;
use transport_http::HttpTransport;
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
/// rather than waited on. The capability's number, so a technology's far end
/// and the playground agree.
pub const TIMEOUT: Duration = LOOPBACK_TIMEOUT;

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

/// The one adapter over every transport that is its own far end: the
/// protocol's [`Loopback`] does the dance, this reports the outcome.
pub struct Looped<L: Loopback>(pub L);

impl<L: Loopback> RoundTrip for Looped<L> {
    fn transport(&self) -> &'static str {
        self.0.name()
    }

    fn ceiling(&self) -> Option<usize> {
        self.0.ceiling()
    }

    fn refuses(&self, payload: &[u8]) -> Option<String> {
        self.0.refuses(payload)
    }

    fn exchange(&self, payload: &[u8]) -> Exchange {
        match self.0.round(payload) {
            Ok(arrived) => Exchange::Returned(arrived.bytes),
            Err(error) => Exchange::Failed(error.message),
        }
    }
}

/// Every implemented transport, each behind its adapter — the one list the
/// scenarios share, so a new transport is wired in a single place rather than in
/// each scenario. `file_dir` is where the file transport ping-pongs.
#[must_use]
pub fn all_transports(file_dir: impl Into<std::path::PathBuf>) -> Vec<Box<dyn RoundTrip>> {
    vec![
        Box::new(Looped(FileTransport::loopback(file_dir))),
        Box::new(Looped(TcpTransport::loopback())),
        Box::new(Looped(HttpTransport::loopback())),
        Box::new(crate::reply::SmtpRoundTrip),
        Box::new(Looped(UdpTransport::loopback())),
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

/// The listen-and-accept shape the adapters not yet moved into their crates
/// still use, with the one rule that keeps a round from hanging the schedule:
/// the accept runs on its own thread and the send on this one, and when the
/// send fails before it connected — an ephemeral port exhausted, a refused
/// connect under load — the listener is poked with a throwaway connect so the
/// accept returns and is judged rather than waited on forever. Found
/// 2026-09-08 when the matrix grew to twelve contracts over seven transports
/// and one round out of thousands blocked a whole `cargo test` in `accept`.
/// `Loopback::round` in the capability is this same rule; this goes when the
/// last adapter moves.
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

/// File over a directory: the file technology's own loopback, named here
/// because the scenarios' tests build it by directory.
pub struct FileRoundTrip(Looped<FileTransport>);

impl FileRoundTrip {
    #[must_use]
    pub fn new(dir: impl Into<std::path::PathBuf>) -> Self {
        Self(Looped(FileTransport::loopback(dir)))
    }
}

impl RoundTrip for FileRoundTrip {
    fn transport(&self) -> &'static str {
        self.0.transport()
    }

    fn exchange(&self, payload: &[u8]) -> Exchange {
        self.0.exchange(payload)
    }
}

/// TCP: the tcp technology's own loopback, named here for the tests that
/// drive it by name.
pub struct TcpRoundTrip;

impl RoundTrip for TcpRoundTrip {
    fn transport(&self) -> &'static str {
        "tcp"
    }

    fn exchange(&self, payload: &[u8]) -> Exchange {
        Looped(TcpTransport::loopback()).exchange(payload)
    }
}

/// UDP: the udp technology's own loopback, with the datagram ceiling it
/// declares.
pub struct UdpRoundTrip;

impl RoundTrip for UdpRoundTrip {
    fn transport(&self) -> &'static str {
        "udp"
    }

    fn ceiling(&self) -> Option<usize> {
        UdpTransport::loopback().ceiling()
    }

    fn exchange(&self, payload: &[u8]) -> Exchange {
        Looped(UdpTransport::loopback()).exchange(payload)
    }
}

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
    fn a_looped_transport_reports_its_own_name_and_ceiling() {
        let looped = Looped(UdpTransport::loopback());
        assert_eq!(looped.transport(), "udp");
        assert_eq!(looped.ceiling(), Some(transport_udp::MAX_DATAGRAM));
        assert_eq!(Looped(HttpTransport::loopback()).transport(), "http");
        assert!(Looped(TcpTransport::loopback()).ceiling().is_none());
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
        let ceiling = transport_udp::MAX_DATAGRAM;
        let brim = crate::stress::patterned(ceiling);
        assert!(matches!(UdpRoundTrip.exchange(&brim), Exchange::Returned(back) if back == brim));
        let over = vec![0u8; ceiling + 1];
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
