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
//! constructor in its crate and one line in [`all_transports`].

use std::time::Duration;

use transport::{LOOPBACK_TIMEOUT, Loopback};
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

    /// A payload over the ceiling is refused before anything is sent: the
    /// transport declared the size it carries, and a probe past it is
    /// one-sided with the reason, never a red for a fact about the protocol.
    fn refuses(&self, payload: &[u8]) -> Option<String> {
        if let Some(limit) = self.0.ceiling()
            && payload.len() > limit
        {
            return Some(format!(
                "{} bytes is over the {limit} {} carries in one round",
                payload.len(),
                self.0.name()
            ));
        }
        self.0.refuses(payload)
    }

    fn exchange(&self, payload: &[u8]) -> Exchange {
        if let Some(why) = self.0.unavailable() {
            return Exchange::OneSided(why);
        }
        match self.0.round(payload) {
            Ok(arrived) => Exchange::Returned(arrived.bytes),
            Err(error) => Exchange::Failed(error.message),
        }
    }
}

/// Every transport that is its own far end, each behind the one adapter —
/// the one list the scenarios share, so a new transport is wired in a single
/// place rather than in each scenario, under the family it belongs to.
/// `file_dir` is where the file transport ping-pongs and where sqlite keeps
/// its queue.
#[must_use]
pub fn all_transports(file_dir: impl Into<std::path::PathBuf>) -> Vec<Box<dyn RoundTrip>> {
    let file_dir = file_dir.into();
    let mut all = local(&file_dir);
    all.extend(internet());
    all.extend(messaging());
    all.extend(cloud());
    all.extend(databases());
    all.extend(field());
    all
}

/// Where both ends are this machine's own objects: a directory, a database file, an OS pipe or socket, a serial line.
fn local(file_dir: &std::path::Path) -> Vec<Box<dyn RoundTrip>> {
    vec![
        Box::new(Looped(transport_file::FileTransport::loopback(file_dir))),
        Box::new(Looped(transport_sqlite::SqliteTransport::loopback(
            file_dir.join("sqlite"),
        ))),
        Box::new(Looped(transport_named_pipe::NamedPipeTransport::loopback())),
        Box::new(Looped(
            transport_unix_socket::UnixSocketTransport::loopback(),
        )),
        Box::new(Looped(transport_serial::SerialTransport::loopback())),
    ]
}

/// The internet protocols: a socket, a listener, a request and its answer.
fn internet() -> Vec<Box<dyn RoundTrip>> {
    vec![
        Box::new(Looped(transport_tcp::TcpTransport::loopback())),
        Box::new(Looped(transport_udp::UdpTransport::loopback())),
        Box::new(Looped(transport_http::HttpTransport::loopback())),
        Box::new(Looped(transport_smtp::SmtpTransport::loopback())),
        Box::new(Looped(transport_websocket::WebSocketTransport::loopback())),
        Box::new(Looped(transport_mllp::MllpTransport::loopback())),
        Box::new(Looped(transport_ftp::FtpTransport::loopback())),
        Box::new(Looped(transport_pop3::Pop3Transport::loopback())),
        Box::new(Looped(transport_imap::ImapTransport::loopback())),
        Box::new(Looped(transport_syslog::SyslogTransport::loopback())),
        Box::new(Looped(transport_coap::CoapTransport::loopback())),
        Box::new(Looped(transport_dns::DnsTransport::loopback())),
        Box::new(Looped(transport_ssdp::SsdpTransport::loopback())),
        Box::new(Looped(transport_mdns::MdnsTransport::loopback())),
        Box::new(Looped(transport_dhcp::DhcpTransport::loopback())),
        Box::new(Looped(transport_snmp::SnmpTransport::loopback())),
        Box::new(Looped(transport_webdav::WebDavTransport::loopback())),
        Box::new(Looped(transport_nfs::NfsTransport::loopback())),
        Box::new(Looped(transport_smb::SmbTransport::loopback())),
        Box::new(Looped(transport_as2::As2Transport::loopback())),
        Box::new(Looped(transport_as4::As4Transport::loopback())),
        Box::new(Looped(transport_dicom::DicomTransport::loopback())),
    ]
}

/// The brokers and queues, each stood up as its own one-client session.
fn messaging() -> Vec<Box<dyn RoundTrip>> {
    vec![
        Box::new(Looped(transport_mqtt::MqttTransport::loopback())),
        Box::new(Looped(transport_nats::NatsTransport::loopback())),
        Box::new(Looped(transport_amqp::AmqpTransport::loopback())),
        Box::new(Looped(transport_kafka::KafkaTransport::loopback())),
        Box::new(Looped(transport_activemq::ActiveMqTransport::loopback())),
        Box::new(Looped(transport_rabbitmq::RabbitMqTransport::loopback())),
        Box::new(Looped(
            transport_nats_jetstream::JetStreamTransport::loopback(),
        )),
        Box::new(Looped(transport_redpanda::RedpandaTransport::loopback())),
        Box::new(Looped(
            transport_redis_streams::RedisStreamsTransport::loopback(),
        )),
        Box::new(Looped(transport_ibm_mq::IbmMqTransport::loopback())),
        Box::new(Looped(transport_msmq::MsmqTransport::loopback())),
    ]
}

/// The cloud APIs over http, each against an in-process far end.
fn cloud() -> Vec<Box<dyn RoundTrip>> {
    vec![
        Box::new(Looped(transport_s3::S3Transport::loopback())),
        Box::new(Looped(transport_azure_blob::AzureBlobTransport::loopback())),
        Box::new(Looped(
            transport_google_cloud_storage::GcsTransport::loopback(),
        )),
        Box::new(Looped(transport_aws_sqs::SqsTransport::loopback())),
        Box::new(Looped(transport_aws_kinesis::KinesisTransport::loopback())),
        Box::new(Looped(transport_aws_sns::SnsTransport::loopback())),
        Box::new(Looped(
            transport_azure_service_bus::ServiceBusTransport::loopback(),
        )),
        Box::new(Looped(
            transport_azure_event_hubs::EventHubsTransport::loopback(),
        )),
        Box::new(Looped(
            transport_azure_event_grid::EventGridTransport::loopback(),
        )),
        Box::new(Looped(transport_google_pub_sub::PubSubTransport::loopback())),
    ]
}

/// The database wires, each against an in-process listener.
fn databases() -> Vec<Box<dyn RoundTrip>> {
    vec![
        Box::new(Looped(transport_postgresql::PostgresTransport::loopback())),
        Box::new(Looped(transport_mssql::MssqlTransport::loopback())),
        Box::new(Looped(transport_mysql::MysqlTransport::loopback())),
        Box::new(Looped(transport_oracle::OracleTransport::loopback())),
    ]
}

/// The field, vehicle, meter, wireless and building protocols, over loopback buses, links and radios.
fn field() -> Vec<Box<dyn RoundTrip>> {
    vec![
        Box::new(Looped(transport_modbus::ModbusTransport::loopback())),
        Box::new(Looped(transport_bacnet::BacnetTransport::loopback())),
        Box::new(Looped(transport_can_bus::CanTransport::loopback())),
        Box::new(Looped(
            transport_iec_60870_5_104::Iec104Transport::loopback(),
        )),
        Box::new(Looped(transport_dnp3::Dnp3Transport::loopback())),
        Box::new(Looped(transport_cotp::CotpTransport::loopback())),
        Box::new(Looped(transport_s7comm::S7Transport::loopback())),
        Box::new(Looped(transport_secs_gem::SecsGemTransport::loopback())),
        Box::new(Looped(transport_opc_ua::OpcUaTransport::loopback())),
        Box::new(Looped(transport_hart::HartTransport::loopback())),
        Box::new(Looped(transport_iso_tp::IsoTpTransport::loopback())),
        Box::new(Looped(transport_ethernet::EthernetTransport::loopback())),
        Box::new(Looped(transport_canopen::CanOpenTransport::loopback())),
        Box::new(Looped(transport_ethercat::EtherCatTransport::loopback())),
        Box::new(Looped(
            transport_ethernet_ip::EtherNetIpTransport::loopback(),
        )),
        Box::new(Looped(transport_profinet::ProfinetTransport::loopback())),
        Box::new(Looped(transport_io_link::IoLinkTransport::loopback())),
        Box::new(Looped(transport_iec_61850::Iec61850Transport::loopback())),
        Box::new(Looped(transport_j1939::J1939Transport::loopback())),
        Box::new(Looped(transport_obd_ii::ObdTransport::loopback())),
        Box::new(Looped(transport_uds::UdsTransport::loopback())),
        Box::new(Looped(transport_m_bus::MBusTransport::loopback())),
        Box::new(Looped(
            transport_wireless_m_bus::WirelessMBusTransport::loopback(),
        )),
        Box::new(Looped(transport_bluetooth::BluetoothTransport::loopback())),
        Box::new(Looped(transport_lorawan::LorawanTransport::loopback())),
        Box::new(Looped(transport_zigbee::ZigbeeTransport::loopback())),
        Box::new(Looped(transport_thread::ThreadTransport::loopback())),
        Box::new(Looped(transport_knx::KnxTransport::loopback())),
        Box::new(Looped(
            transport_wireless_hart::WirelessHartTransport::loopback(),
        )),
        Box::new(Looped(transport_dds::DdsTransport::loopback())),
    ]
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
        assert_eq!(
            Looped(transport_http::HttpTransport::loopback()).transport(),
            "http"
        );
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
