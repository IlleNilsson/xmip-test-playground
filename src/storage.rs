//! The storing round trips: s3, azure-blob, google-cloud-storage and webdav,
//! each behind the same [`RoundTrip`] the pingpong scenario drives.
//!
//! All four are the partner's drop box behind an HTTP API — a bucket, a
//! container, a collection — and all four ride the http technology over plain
//! TCP. The build box has no bucket and no web server, so each adapter binds
//! the transport's own one-client session as the far end — one credential,
//! one store in memory, answers shaped as the real service shapes them — and
//! the near end puts the payload as one object. What the session took is what
//! came back. The three cloud stores present a credential on every request
//! and open a connection per call, so their far end serves one request at a
//! time until one stored; `WebDAV` keeps its connection, so its far end reads
//! it until the PUT.

use transport::error::protocol_error;
use transport::{Transport, socket};
use transport_azure_blob::AzureBlobTransport;
use transport_google_cloud_storage::GcsTransport;
use transport_s3::S3Transport;
use transport_webdav::WebDavTransport;

use crate::roundtrip::{Exchange, RoundTrip, TIMEOUT, listen_exchange};

/// The bucket, container or collection every adapter puts into, and the one
/// object it puts there.
const STORE: &str = "probe";
const OBJECT: &str = "probe.bin";

/// The credentials the near end presents and the far end expects: an S3
/// access key in a region, an Azure account with its key in base64 as the
/// portal shows it, a Cloud Storage bearer token.
const REGION: &str = "eu-north-1";
const ACCESS_KEY: &str = "AKIDPROBE";
const SECRET_KEY: &str = "probe";
const ACCOUNT_KEY: &str = "cHJvYmU=";
const TOKEN: &str = "ya29.probe";

/// S3: bind a session holding the access key, put the payload as one object
/// from a client signing as that key, and take it as the session's one store.
pub struct S3RoundTrip;

impl RoundTrip for S3RoundTrip {
    fn transport(&self) -> &'static str {
        "s3"
    }

    fn exchange(&self, payload: &[u8]) -> Exchange {
        let (listener, address) = match socket::bind_tcp("127.0.0.1:0") {
            Ok(bound) => bound,
            Err(error) => return Exchange::Failed(format!("bind failed: {error}")),
        };
        listen_exchange(
            listener,
            &address,
            move |listener| {
                let mut session = transport_s3::Session::new(REGION, ACCESS_KEY, SECRET_KEY)
                    .timing_out_after(TIMEOUT);
                loop {
                    match session.serve_one(listener)? {
                        transport_s3::Event::Stored(arrived) => return Ok(arrived),
                        transport_s3::Event::Refused(code) => {
                            return Err(protocol_error(format!("the session refused: {code}")));
                        }
                        _ => {}
                    }
                }
            },
            |address| {
                S3Transport::new(format!("http://{address}"), REGION, STORE)
                    .with_credentials(ACCESS_KEY, SECRET_KEY)
                    .timing_out_after(TIMEOUT)
                    .send(OBJECT, payload)
            },
        )
    }
}

/// Azure Blob Storage: bind a session holding the account key, put the payload
/// as one block blob from a client signing with that key, and take it as the
/// session's one store.
pub struct AzureBlobRoundTrip;

impl RoundTrip for AzureBlobRoundTrip {
    fn transport(&self) -> &'static str {
        "azure-blob"
    }

    fn exchange(&self, payload: &[u8]) -> Exchange {
        let (listener, address) = match socket::bind_tcp("127.0.0.1:0") {
            Ok(bound) => bound,
            Err(error) => return Exchange::Failed(format!("bind failed: {error}")),
        };
        listen_exchange(
            listener,
            &address,
            move |listener| {
                let mut session = transport_azure_blob::Session::new(STORE, ACCOUNT_KEY)?
                    .timing_out_after(TIMEOUT);
                loop {
                    match session.serve_one(listener)? {
                        transport_azure_blob::Event::Stored(arrived) => return Ok(arrived),
                        transport_azure_blob::Event::Refused(code) => {
                            return Err(protocol_error(format!("the session refused: {code}")));
                        }
                        _ => {}
                    }
                }
            },
            |address| {
                AzureBlobTransport::new(format!("http://{address}"), STORE, STORE)
                    .with_key(ACCOUNT_KEY)
                    .timing_out_after(TIMEOUT)
                    .send(OBJECT, payload)
            },
        )
    }
}

/// Cloud Storage: bind a session expecting the bearer token, upload the
/// payload as one object from a client presenting that token, and take it as
/// the session's one store.
pub struct GcsRoundTrip;

impl RoundTrip for GcsRoundTrip {
    fn transport(&self) -> &'static str {
        "google-cloud-storage"
    }

    fn exchange(&self, payload: &[u8]) -> Exchange {
        let (listener, address) = match socket::bind_tcp("127.0.0.1:0") {
            Ok(bound) => bound,
            Err(error) => return Exchange::Failed(format!("bind failed: {error}")),
        };
        listen_exchange(
            listener,
            &address,
            move |listener| {
                let mut session =
                    transport_google_cloud_storage::Session::new(TOKEN).timing_out_after(TIMEOUT);
                loop {
                    match session.serve_one(listener)? {
                        transport_google_cloud_storage::Event::Stored(arrived) => {
                            return Ok(arrived);
                        }
                        transport_google_cloud_storage::Event::Refused(reason) => {
                            return Err(protocol_error(format!("the session refused: {reason}")));
                        }
                        _ => {}
                    }
                }
            },
            |address| {
                GcsTransport::new(format!("http://{address}"), STORE)
                    .with_token(TOKEN)
                    .timing_out_after(TIMEOUT)
                    .send(OBJECT, payload)
            },
        )
    }
}

/// `WebDAV`: bind a session over an empty store, PUT the payload as one member
/// of the root collection from a client on one connection, and take it as
/// the session's one put.
pub struct WebDavRoundTrip;

impl RoundTrip for WebDavRoundTrip {
    fn transport(&self) -> &'static str {
        "webdav"
    }

    fn exchange(&self, payload: &[u8]) -> Exchange {
        let far_end = WebDavTransport::new("webdav://127.0.0.1:0").timing_out_after(TIMEOUT);
        let (listener, address) = match far_end.bind() {
            Ok(bound) => bound,
            Err(error) => return Exchange::Failed(format!("bind failed: {error}")),
        };
        listen_exchange(
            listener,
            &address,
            move |listener| {
                let mut session = far_end.accept_one(listener)?;
                session
                    .next_put()?
                    .ok_or_else(|| protocol_error("the client closed without storing"))
            },
            |address| {
                WebDavTransport::new(format!("webdav://{address}"))
                    .timing_out_after(TIMEOUT)
                    .send(OBJECT, payload)
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

    #[test]
    fn s3_stores_one_object_through_a_session() {
        let long = vec![0x2a; 3_000];
        assert_eq!(returned(&S3RoundTrip, b"an object"), b"an object");
        assert_eq!(returned(&S3RoundTrip, &long), long);
        assert_eq!(returned(&S3RoundTrip, b""), b"");
    }

    #[test]
    fn azure_blob_stores_one_blob_through_a_session() {
        let long = vec![0x2a; 3_000];
        assert_eq!(returned(&AzureBlobRoundTrip, b"a blob"), b"a blob");
        assert_eq!(returned(&AzureBlobRoundTrip, &long), long);
        assert_eq!(returned(&AzureBlobRoundTrip, b""), b"");
    }

    #[test]
    fn google_cloud_storage_uploads_one_object_through_a_session() {
        let long = vec![0x2a; 3_000];
        assert_eq!(returned(&GcsRoundTrip, b"an upload"), b"an upload");
        assert_eq!(returned(&GcsRoundTrip, &long), long);
        assert_eq!(returned(&GcsRoundTrip, b""), b"");
    }

    #[test]
    fn webdav_puts_one_member_through_a_session() {
        let long = vec![0x2a; 3_000];
        assert_eq!(returned(&WebDavRoundTrip, b"a member"), b"a member");
        assert_eq!(returned(&WebDavRoundTrip, &long), long);
        assert_eq!(returned(&WebDavRoundTrip, b""), b"");
    }
}
