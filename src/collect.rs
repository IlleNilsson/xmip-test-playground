//! The collecting round trips: ftp, pop3 and imap, each behind the same
//! [`RoundTrip`] the pingpong scenario drives.
//!
//! Both are the partner's drop box: a far end that holds artefacts and a
//! near end that collects them. FTP goes both ways, so the near end stores
//! the payload into the far end's session and what the session took is what
//! came back. POP3 only collects, so the far end serves the payload as the
//! one message in a maildrop and the near end collects it — the send half of
//! that pair is SMTP, exercised in its own adapter.

use transport::Transport;
use transport::error::protocol_error;
use transport_ftp::FtpTransport;
use transport_imap::ImapTransport;
use transport_pop3::{Login, Pop3Transport};

use crate::roundtrip::{Exchange, RoundTrip, TIMEOUT, listen_exchange};

/// FTP: bind a session, log a client in, store the payload as one file, and
/// take it as the session's one store.
pub struct FtpRoundTrip;

impl RoundTrip for FtpRoundTrip {
    fn transport(&self) -> &'static str {
        "ftp"
    }

    fn exchange(&self, payload: &[u8]) -> Exchange {
        let far_end = FtpTransport::new("127.0.0.1:0").timing_out_after(TIMEOUT);
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
                let arrived = session
                    .next_store()?
                    .ok_or_else(|| protocol_error("the client quit without storing"))?;
                // Serve the QUIT that follows, so the client's goodbye is
                // answered rather than met by a closed socket.
                session.next_store()?;
                Ok(arrived)
            },
            |address| {
                FtpTransport::new(address)
                    .timing_out_after(timeout)
                    .send("probe.bin", payload)
            },
        )
    }
}

/// POP3: bind a maildrop holding the payload as its one message, collect it
/// with a client, and what was collected is what came back.
pub struct Pop3RoundTrip;

fn login() -> Login {
    Login {
        user: "probe".to_string(),
        password: "probe".to_string(),
    }
}

impl RoundTrip for Pop3RoundTrip {
    fn transport(&self) -> &'static str {
        "pop3"
    }

    fn exchange(&self, payload: &[u8]) -> Exchange {
        let far_end = Pop3Transport::new("127.0.0.1:0", login()).timing_out_after(TIMEOUT);
        let (listener, address) = match far_end.bind() {
            Ok(bound) => bound,
            Err(error) => return Exchange::Failed(format!("bind failed: {error}")),
        };
        let message = payload.to_vec();
        let server = std::thread::spawn(move || {
            far_end
                .accept_one(&listener, vec![message])
                .and_then(transport_pop3::Session::serve)
        });
        let collected = Pop3Transport::new(address, login())
            .timing_out_after(TIMEOUT)
            .receive();
        match (collected, server.join()) {
            (Ok(mut arrived), Ok(Ok(_))) if arrived.len() == 1 => {
                Exchange::Returned(arrived.remove(0).bytes)
            }
            (Ok(arrived), Ok(Ok(_))) => {
                Exchange::Failed(format!("collected {} messages, not one", arrived.len()))
            }
            (Err(error), _) => Exchange::Failed(format!("collect failed: {error}")),
            (_, Ok(Err(error))) => Exchange::Failed(format!("serve failed: {error}")),
            (_, Err(_)) => Exchange::Failed("the serving thread panicked".to_string()),
        }
    }
}

/// IMAP: bind a mailbox holding the payload as its one message, collect it
/// with a client, and what was collected is what came back.
pub struct ImapRoundTrip;

impl RoundTrip for ImapRoundTrip {
    fn transport(&self) -> &'static str {
        "imap"
    }

    fn exchange(&self, payload: &[u8]) -> Exchange {
        let login = transport_imap::Login {
            user: "probe".to_string(),
            password: "probe".to_string(),
        };
        let far_end =
            ImapTransport::new("127.0.0.1:0", "INBOX", login.clone()).timing_out_after(TIMEOUT);
        let (listener, address) = match far_end.bind() {
            Ok(bound) => bound,
            Err(error) => return Exchange::Failed(format!("bind failed: {error}")),
        };
        let message = payload.to_vec();
        let server = std::thread::spawn(move || {
            far_end
                .accept_one(&listener, vec![message])
                .and_then(transport_imap::Session::serve)
        });
        let collected = ImapTransport::new(address, "INBOX", login)
            .timing_out_after(TIMEOUT)
            .receive();
        match (collected, server.join()) {
            (Ok(mut arrived), Ok(Ok(_))) if arrived.len() == 1 => {
                Exchange::Returned(arrived.remove(0).bytes)
            }
            (Ok(arrived), Ok(Ok(_))) => {
                Exchange::Failed(format!("collected {} messages, not one", arrived.len()))
            }
            (Err(error), _) => Exchange::Failed(format!("collect failed: {error}")),
            (_, Ok(Err(error))) => Exchange::Failed(format!("serve failed: {error}")),
            (_, Err(_)) => Exchange::Failed("the serving thread panicked".to_string()),
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
    fn ftp_stores_and_pop3_collects() {
        let long = vec![0x2a; 100_000];
        assert_eq!(returned(&FtpRoundTrip, b"a file"), b"a file");
        assert_eq!(returned(&FtpRoundTrip, &long), long);
        assert_eq!(returned(&FtpRoundTrip, b""), b"");
        assert_eq!(
            returned(&Pop3RoundTrip, b"Subject: x\r\n\r\n.dot"),
            b"Subject: x\r\n\r\n.dot"
        );
        assert_eq!(returned(&Pop3RoundTrip, &long), long);
        assert_eq!(returned(&Pop3RoundTrip, b""), b"");
        assert_eq!(
            returned(
                &ImapRoundTrip,
                b"Subject: y

{3}"
            ),
            b"Subject: y

{3}"
        );
        assert_eq!(returned(&ImapRoundTrip, &long), long);
        assert_eq!(returned(&ImapRoundTrip, b""), b"");
    }
}
