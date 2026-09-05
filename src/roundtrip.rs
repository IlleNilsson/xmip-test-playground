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

use std::net::TcpStream;
use std::time::Duration;

use xmip_transport::{FileTransport, TcpTransport, Transport};

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

/// A transport the pingpong scenario can drive, behind one method.
pub trait RoundTrip {
    /// The transport token, as it appears in a scope and a repository name.
    fn transport(&self) -> &'static str;

    /// Send `payload` and return what came back. The adapter does whatever its
    /// transport needs — a directory read-back, a listen-and-accept, a
    /// datagram — behind this one call.
    fn exchange(&self, payload: &[u8]) -> Exchange;
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
pub struct TcpRoundTrip {
    accept_timeout: Duration,
}

impl TcpRoundTrip {
    #[must_use]
    pub fn new() -> Self {
        // A short timeout so a round that cannot connect fails the test rather
        // than hanging the schedule — the point of exercising over time is that
        // no one round can stop the next.
        Self {
            accept_timeout: Duration::from_secs(2),
        }
    }
}

impl Default for TcpRoundTrip {
    fn default() -> Self {
        Self::new()
    }
}

impl RoundTrip for TcpRoundTrip {
    fn transport(&self) -> &'static str {
        "tcp"
    }

    fn exchange(&self, payload: &[u8]) -> Exchange {
        // Bind on an ephemeral port; the OS hands back the real address.
        let far_end = TcpTransport::new("127.0.0.1:0").timing_out_after(self.accept_timeout);

        let (listener, address) = match far_end.bind() {
            Ok(bound) => bound,
            Err(error) => return Exchange::Failed(format!("bind failed: {error}")),
        };

        // The sender is another thread: connect to the bound address and send.
        // TcpStream carries the send directly — TcpTransport::send would work
        // too, but the raw stream keeps the sender half small.
        let payload = payload.to_vec();
        let sender = std::thread::spawn(move || {
            use std::io::Write;
            let mut stream = TcpStream::connect(&address)?;
            stream.write_all(&payload)?;
            stream.flush()
        });

        let caught = far_end.accept_one(&listener);

        let sent = sender.join();

        match (caught, sent) {
            (Ok(arrived), Ok(Ok(()))) => Exchange::Returned(arrived.bytes),
            (Err(error), _) => Exchange::Failed(format!("accept failed: {error}")),
            (_, Ok(Err(error))) => Exchange::Failed(format!("send failed: {error}")),
            (_, Err(_)) => Exchange::Failed("the sending thread panicked".to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scratch(name: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("xmip-rt-{name}"));
        std::fs::remove_dir_all(&dir).ok();
        dir
    }

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
        let rt = TcpRoundTrip::new();

        match rt.exchange(b"over tcp") {
            Exchange::Returned(bytes) => assert_eq!(bytes, b"over tcp"),
            other => panic!("expected Returned, got {}", label(&other)),
        }
    }

    #[test]
    fn tcp_carries_binary_unharmed() {
        let rt = TcpRoundTrip::new();
        let payload = [0x00u8, 0x01, 0xfe, 0xff];

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
