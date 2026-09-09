//! The remote cabinets: postgresql, s3, azure-blob and google-cloud-storage,
//! each behind the same [`Cabinet`] the filing scenario drives.
//!
//! Every one of these has a server at the far end in production — a
//! database, a bucket, a container. The build box has none, so each adapter
//! binds the sibling transport's own one-client `Session` on loopback for
//! the length of one filing — a server's worth of protocol, one credential,
//! one store in memory — serves exactly what an archive and a restore need,
//! and joins it. The session runs on its own thread and the filing on this
//! one, so a far end that hangs is judged within [`TIMEOUT`] rather than
//! waited on: the same rule `listen_exchange` keeps for the transports.

use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};

use archive::ArchiveItem;
use archive_azure_blob::AzureBlobArchive;
use archive_gcs::GcsArchive;
use archive_postgresql::PostgresqlArchive;
use archive_s3::S3Archive;
use transport::error::protocol_error;
use transport::socket;
use transport_postgresql::{Answer, Session};

use crate::cabinet::{Cabinet, Filed, file_through};
use crate::roundtrip::TIMEOUT;

/// The bucket or container every object cabinet files into, and the prefix
/// its items are laid out under.
const STORE: &str = "probe";
const PREFIX: &str = "filing";

/// The credentials the near end presents and the far end expects: an S3
/// access key in a region, an Azure account with its key in base64 as the
/// portal shows it, a Cloud Storage bearer token.
const REGION: &str = "eu-north-1";
const ACCESS_KEY: &str = "AKIDPROBE";
const SECRET_KEY: &str = "probe";
const ACCOUNT_KEY: &str = "cHJvYmU=";
const TOKEN: &str = "ya29.probe";

/// An archive is two requests — the bytes, then the metadata beside them —
/// and a restore is the same two back: what an object store's far end
/// serves per filing, one connection each.
const OBJECT_REQUESTS: usize = 4;

/// The row id the `PostgreSQL` far end answers every `INSERT` with.
const ROW_ID: u64 = 1;

/// The serve-and-file shape the four share: the far end serves on its own
/// thread, the filing runs on this one, and when the filing fails before the
/// far end has served its last request the listener is poked with a
/// throwaway connect so the accept returns and the round is judged rather
/// than waited on forever.
pub(crate) fn serve_filing<S, F>(listener: TcpListener, address: &str, serve: S, file: F) -> Filed
where
    S: FnOnce(&TcpListener) -> transport::Result<()> + Send + 'static,
    F: FnOnce(&str) -> Filed,
{
    let far_end = std::thread::spawn(move || serve(&listener));
    let filed = file(address);
    if matches!(filed, Filed::Failed(_)) {
        let _ = TcpStream::connect(address);
    }
    match (far_end.join(), filed) {
        (_, Filed::Failed(why)) => Filed::Failed(why),
        (Ok(Ok(())), Filed::Returned(item)) => Filed::Returned(item),
        (Ok(Err(error)), Filed::Returned(_)) => {
            Filed::Failed(format!("the far end failed: {error}"))
        }
        (Err(_), Filed::Returned(_)) => Filed::Failed("the far end thread panicked".to_string()),
    }
}

/// Every single-quoted literal in `sql`, in order, quotes undoubled. The
/// archive's `INSERT` carries five — `data_type`, `identifier`, the bytes in
/// bytea hex, the metadata, `archived_at` — and the far end keeps the first
/// four as the row it answers the `SELECT` with, so what comes back is what
/// went over the wire, not a canned copy.
pub(crate) fn literals(sql: &str) -> Vec<String> {
    let mut found = Vec::new();
    let mut chars = sql.chars().peekable();
    while let Some(c) = chars.next() {
        if c != '\'' {
            continue;
        }
        let mut literal = String::new();
        loop {
            match chars.next() {
                Some('\'') if chars.peek() == Some(&'\'') => {
                    chars.next();
                    literal.push('\'');
                }
                Some('\'') | None => break,
                Some(other) => literal.push(other),
            }
        }
        found.push(literal);
    }
    found
}

/// `PostgreSQL`: bind a session, take the archive's one connection — the
/// `INSERT`, answered with a row id, its literals kept as the row — then the
/// restore's — the `SELECT`, answered with that row — and read each
/// Terminate so the goodbye is taken rather than written into a closed
/// socket.
pub struct PostgresqlCabinet;

impl PostgresqlCabinet {
    fn serve(listener: &TcpListener) -> transport::Result<()> {
        let held: Arc<Mutex<Vec<Option<String>>>> = Arc::default();
        let keep = Arc::clone(&held);
        let mut inserting = Session::accept(listener, None, Some(TIMEOUT))?.answering(move |sql| {
            if !sql.starts_with("INSERT") {
                return None;
            }
            if let Ok(mut row) = keep.lock() {
                *row = literals(sql).into_iter().take(4).map(Some).collect();
            }
            Some(Answer::Rows {
                columns: vec!["id".to_string()],
                rows: vec![vec![Some(ROW_ID.to_string())]],
            })
        });
        while inserting.next_event()?.is_some() {}

        let row = held
            .lock()
            .map_err(|_| protocol_error("the held row was poisoned"))?
            .clone();
        let cells: Vec<Option<&str>> = row.iter().map(Option::as_deref).collect();
        let rows: [&[Option<&str>]; 1] = [&cells];
        let mut selecting = Session::accept(listener, None, Some(TIMEOUT))?
            .with_table(&["data_type", "identifier", "bytes", "metadata"], &rows);
        while selecting.next_event()?.is_some() {}
        Ok(())
    }
}

impl Cabinet for PostgresqlCabinet {
    fn technology(&self) -> &'static str {
        "postgresql"
    }

    fn file(&self, item: ArchiveItem) -> Filed {
        let (listener, address) = match socket::bind_tcp("127.0.0.1:0") {
            Ok(bound) => bound,
            Err(error) => return Filed::Failed(format!("bind failed: {error}")),
        };
        serve_filing(listener, &address, Self::serve, |address| {
            let store =
                PostgresqlArchive::new(address, "playground", "xmip").timing_out_after(TIMEOUT);
            file_through(&store, item)
        })
    }
}

/// S3: bind a session holding the access key, serve the two puts and the
/// two gets of one filing from a client signing as that key.
pub struct S3Cabinet;

impl S3Cabinet {
    fn serve(listener: &TcpListener) -> transport::Result<()> {
        let mut session =
            transport_s3::Session::new(REGION, ACCESS_KEY, SECRET_KEY).timing_out_after(TIMEOUT);
        for _ in 0..OBJECT_REQUESTS {
            if let transport_s3::Event::Refused(code) = session.serve_one(listener)? {
                return Err(protocol_error(format!("the session refused: {code}")));
            }
        }
        Ok(())
    }
}

impl Cabinet for S3Cabinet {
    fn technology(&self) -> &'static str {
        "s3"
    }

    fn file(&self, item: ArchiveItem) -> Filed {
        let (listener, address) = match socket::bind_tcp("127.0.0.1:0") {
            Ok(bound) => bound,
            Err(error) => return Filed::Failed(format!("bind failed: {error}")),
        };
        serve_filing(listener, &address, Self::serve, |address| {
            let store = S3Archive::new(format!("http://{address}"), REGION, STORE)
                .with_credentials(ACCESS_KEY, SECRET_KEY)
                .with_prefix(PREFIX)
                .timing_out_after(TIMEOUT);
            file_through(&store, item)
        })
    }
}

/// Azure Blob Storage: bind a session holding the account key, serve the two
/// puts and the two gets of one filing from a client signing with that key.
pub struct AzureBlobCabinet;

impl AzureBlobCabinet {
    fn serve(listener: &TcpListener) -> transport::Result<()> {
        let mut session =
            transport_azure_blob::Session::new(STORE, ACCOUNT_KEY)?.timing_out_after(TIMEOUT);
        for _ in 0..OBJECT_REQUESTS {
            if let transport_azure_blob::Event::Refused(code) = session.serve_one(listener)? {
                return Err(protocol_error(format!("the session refused: {code}")));
            }
        }
        Ok(())
    }
}

impl Cabinet for AzureBlobCabinet {
    fn technology(&self) -> &'static str {
        "azure-blob"
    }

    fn file(&self, item: ArchiveItem) -> Filed {
        let (listener, address) = match socket::bind_tcp("127.0.0.1:0") {
            Ok(bound) => bound,
            Err(error) => return Filed::Failed(format!("bind failed: {error}")),
        };
        serve_filing(listener, &address, Self::serve, |address| {
            let store =
                AzureBlobArchive::new(format!("http://{address}"), STORE, ACCOUNT_KEY, STORE)
                    .with_prefix(PREFIX)
                    .timing_out_after(TIMEOUT);
            file_through(&store, item)
        })
    }
}

/// Cloud Storage: bind a session expecting the bearer token, serve the two
/// uploads and the two gets of one filing from a client presenting it.
pub struct GcsCabinet;

impl GcsCabinet {
    fn serve(listener: &TcpListener) -> transport::Result<()> {
        let mut session =
            transport_google_cloud_storage::Session::new(TOKEN).timing_out_after(TIMEOUT);
        for _ in 0..OBJECT_REQUESTS {
            if let transport_google_cloud_storage::Event::Refused(reason) =
                session.serve_one(listener)?
            {
                return Err(protocol_error(format!("the session refused: {reason}")));
            }
        }
        Ok(())
    }
}

impl Cabinet for GcsCabinet {
    fn technology(&self) -> &'static str {
        "google-cloud-storage"
    }

    fn file(&self, item: ArchiveItem) -> Filed {
        let (listener, address) = match socket::bind_tcp("127.0.0.1:0") {
            Ok(bound) => bound,
            Err(error) => return Filed::Failed(format!("bind failed: {error}")),
        };
        serve_filing(listener, &address, Self::serve, |address| {
            let store = GcsArchive::new(format!("http://{address}"), TOKEN, STORE)
                .with_prefix(PREFIX)
                .timing_out_after(TIMEOUT);
            file_through(&store, item)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::support::files_whole;

    #[test]
    fn postgresql_files_an_item_whole_through_a_session() {
        files_whole(&PostgresqlCabinet);
    }

    #[test]
    fn s3_files_an_item_whole_through_a_session() {
        files_whole(&S3Cabinet);
    }

    #[test]
    fn azure_blob_files_an_item_whole_through_a_session() {
        files_whole(&AzureBlobCabinet);
    }

    #[test]
    fn google_cloud_storage_files_an_item_whole_through_a_session() {
        files_whole(&GcsCabinet);
    }

    #[test]
    fn the_literals_of_an_insert_come_out_in_order_with_quotes_undoubled() {
        let sql = "INSERT INTO \"archive\" (a, b, c) VALUES ('json', 'it''s #1', '') RETURNING id";
        assert_eq!(literals(sql), ["json", "it's #1", ""]);
        assert!(literals("SELECT 1").is_empty());
    }
}
