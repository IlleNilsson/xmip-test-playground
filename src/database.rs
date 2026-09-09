//! The database cabinets: mssql and mysql, each behind the same [`Cabinet`]
//! the filing scenario drives, on the shape `remote.rs` gives `PostgreSQL`.
//!
//! Both put a database server at the far end in production; the build box
//! has none, so each adapter binds the sibling transport's own one-client
//! `Session` on loopback for one filing: the archive's connection, whose
//! `INSERT` is answered with a row id and whose literals are kept as the
//! row; then the restore's, whose `SELECT` is answered with that row. What
//! comes back is what went over the wire, not a canned copy. Each dialect
//! carries the bytes differently — SQL Server as an unquoted `0x…`, `MySQL` as
//! `X'…'` — and the far end keeps that literal in the form the archive
//! decodes on the way back.

use std::net::TcpListener;
use std::sync::{Arc, Mutex};

use archive::ArchiveItem;
use archive_mssql::MssqlArchive;
use archive_mysql::MysqlArchive;
use transport::error::protocol_error;
use transport::socket;

use crate::cabinet::{Cabinet, Filed, file_through};
use crate::remote::{literals, serve_filing};
use crate::roundtrip::TIMEOUT;

/// The row id both far ends answer an `INSERT` with.
const ROW_ID: &str = "1";

/// The columns a restore selects, in the order the archives read them.
const COLUMNS: [&str; 4] = ["data_type", "identifier", "bytes", "metadata"];

/// The unquoted `0x…` token in a SQL Server `INSERT`: the bytes, which the
/// single-quoted literals around it do not carry.
fn hex_token(sql: &str) -> Option<String> {
    let start = sql.find(", 0x")? + 2;
    let digits = sql[start + 2..]
        .chars()
        .take_while(char::is_ascii_hexdigit)
        .count();
    Some(sql[start..start + 2 + digits].to_string())
}

/// The held row shared between the serving thread's two connections.
type Held = Arc<Mutex<Vec<Option<String>>>>;

fn held_row(held: &Held) -> transport::Result<Vec<Option<String>>> {
    held.lock()
        .map(|row| row.clone())
        .map_err(|_| protocol_error("the held row was poisoned"))
}

/// SQL Server: the `INSERT … OUTPUT INSERTED.id` is answered with a row, its
/// three text literals and the `0x…` bytes kept; the `SELECT` gets them back.
pub struct MssqlCabinet;

impl MssqlCabinet {
    fn serve(listener: &TcpListener) -> transport::Result<()> {
        use transport_mssql::{Answer, Session};
        let held: Held = Arc::default();
        let keep = Arc::clone(&held);
        let mut inserting = Session::accept(listener, None, Some(TIMEOUT))?.answering(move |sql| {
            if !sql.starts_with("INSERT") {
                return None;
            }
            if let Ok(mut row) = keep.lock() {
                let text = literals(sql);
                *row = vec![
                    text.first().cloned(),
                    text.get(1).cloned(),
                    hex_token(sql),
                    text.get(2).cloned(),
                ];
            }
            Some(Answer::Rows {
                columns: vec!["id".to_string()],
                rows: vec![vec![Some(ROW_ID.to_string())]],
            })
        });
        while inserting.next_event()?.is_some() {}

        let row = held_row(&held)?;
        let cells: Vec<Option<&str>> = row.iter().map(Option::as_deref).collect();
        let rows: [&[Option<&str>]; 1] = [&cells];
        let mut selecting =
            Session::accept(listener, None, Some(TIMEOUT))?.with_table(&COLUMNS, &rows);
        while selecting.next_event()?.is_some() {}
        Ok(())
    }
}

impl Cabinet for MssqlCabinet {
    fn technology(&self) -> &'static str {
        "mssql"
    }

    fn file(&self, item: ArchiveItem) -> Filed {
        let (listener, address) = match socket::bind_tcp("127.0.0.1:0") {
            Ok(bound) => bound,
            Err(error) => return Filed::Failed(format!("bind failed: {error}")),
        };
        serve_filing(listener, &address, Self::serve, |address| {
            let store = MssqlArchive::new(address, "playground", "xmip").timing_out_after(TIMEOUT);
            file_through(&store, item)
        })
    }
}

/// `MySQL`: the `INSERT` completes, `SELECT LAST_INSERT_ID()` names the row,
/// and the `SELECT` on the next connection answers with the kept literals —
/// the bytes in the `0x…` form the archive's `HEX()` select would produce.
pub struct MysqlCabinet;

impl MysqlCabinet {
    fn serve(listener: &TcpListener) -> transport::Result<()> {
        use transport_mysql::{Answer, Login, Session};
        let login = Login::new("xmip", "");
        let held: Held = Arc::default();
        let keep = Arc::clone(&held);
        let mut inserting =
            Session::accept(listener, &login, Some(TIMEOUT))?.answering(move |sql| {
                if sql.starts_with("INSERT") {
                    if let Ok(mut row) = keep.lock() {
                        let text = literals(sql);
                        *row = vec![
                            text.first().cloned(),
                            text.get(1).cloned(),
                            text.get(2).map(|hex| format!("0x{hex}")),
                            text.get(3).cloned(),
                        ];
                    }
                    Some(Answer::Complete(1))
                } else if sql.contains("LAST_INSERT_ID") {
                    Some(Answer::Rows {
                        columns: vec!["LAST_INSERT_ID()".to_string()],
                        rows: vec![vec![Some(ROW_ID.to_string())]],
                    })
                } else {
                    None
                }
            });
        while inserting.next_event()?.is_some() {}

        let row = held_row(&held)?;
        let cells: Vec<Option<&str>> = row.iter().map(Option::as_deref).collect();
        let rows: [&[Option<&str>]; 1] = [&cells];
        let mut selecting =
            Session::accept(listener, &login, Some(TIMEOUT))?.with_table(&COLUMNS, &rows);
        while selecting.next_event()?.is_some() {}
        Ok(())
    }
}

impl Cabinet for MysqlCabinet {
    fn technology(&self) -> &'static str {
        "mysql"
    }

    fn file(&self, item: ArchiveItem) -> Filed {
        let (listener, address) = match socket::bind_tcp("127.0.0.1:0") {
            Ok(bound) => bound,
            Err(error) => return Filed::Failed(format!("bind failed: {error}")),
        };
        serve_filing(listener, &address, Self::serve, |address| {
            let store = MysqlArchive::new(address, "playground", "xmip").timing_out_after(TIMEOUT);
            file_through(&store, item)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::support::files_whole;

    #[test]
    fn the_hex_token_is_the_unquoted_bytes() {
        assert_eq!(
            hex_token("INSERT INTO [a] (x) OUTPUT INSERTED.id VALUES (N'j', N'i', 0x7b22, N'm')")
                .as_deref(),
            Some("0x7b22")
        );
        assert_eq!(hex_token("VALUES (N'j', 0x, N'm')").as_deref(), Some("0x"));
        assert_eq!(hex_token("VALUES (N'j', N'm')"), None);
    }

    #[test]
    fn mssql_files_and_returns_whole() {
        files_whole(&MssqlCabinet);
    }

    #[test]
    fn mysql_files_and_returns_whole() {
        files_whole(&MysqlCabinet);
    }
}
