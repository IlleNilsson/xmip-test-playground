//! One shape the filing scenario drives, and an adapter per archive
//! technology.
//!
//! The archive technologies share the [`ArchiveStore`] trait, so unlike the
//! transports they do not need a dance each; what they differ in is how they
//! are stood up. Four are rooted in a directory — a Parquet file, a `SQLite`
//! row, a plain file with a sidecar, an `INSERT` in a script — and are simply
//! constructed. Four talk to a server — `PostgreSQL`, S3, Azure Blob, Cloud
//! Storage — and the build box has none, so each binds the sibling
//! transport's own one-client `Session` as the far end on loopback, per call
//! (`remote.rs`).
//!
//! So the scenario drives this one thing: [`Cabinet::file`] — hand it an
//! item, it archives the item, restores it from the receipt, and hands back
//! what came back, or why it could not. A new archive technology is a new
//! adapter, not a new scenario, exactly as a transport is a new `RoundTrip`.

use std::path::PathBuf;

use archive::{ArchiveItem, ArchiveStore};
use archive_file::FileArchive;
use archive_parquet::ParquetArchive;
use archive_sql::SqlScriptArchive;
use archive_sqlite::SqliteArchive;

/// What one filing returned.
#[derive(Debug, PartialEq, Eq)]
pub enum Filed {
    /// It came back from the archive. Compare to what was filed.
    Returned(ArchiveItem),
    /// The archive or the restore failed. Red, with the reason.
    Failed(String),
}

/// An archive technology the filing scenario can drive, behind one method.
pub trait Cabinet {
    /// The technology token, as it appears in a scope and a repository name.
    fn technology(&self) -> &'static str;

    /// Archive `item`, restore it from the receipt, and return what came back.
    /// The adapter does whatever its technology needs — a directory, a far
    /// end on a thread — behind this one call.
    fn file(&self, item: ArchiveItem) -> Filed;
}

/// Every implemented archive technology, each behind its adapter — the one
/// list the scenarios share, so a new technology is wired in a single place.
/// `dir` is where the directory-rooted cabinets keep their files, one
/// subdirectory each.
#[must_use]
pub fn all_cabinets(dir: impl Into<PathBuf>) -> Vec<Box<dyn Cabinet>> {
    let dir = dir.into();
    vec![
        Box::new(ParquetCabinet::new(dir.join("parquet"))),
        Box::new(SqliteCabinet::new(dir.join("sqlite"))),
        Box::new(FileCabinet::new(dir.join("file"))),
        Box::new(SqlScriptCabinet::new(dir.join("sql"))),
        Box::new(crate::remote::PostgresqlCabinet),
        Box::new(crate::database::MssqlCabinet),
        Box::new(crate::database::MysqlCabinet),
        Box::new(crate::remote::S3Cabinet),
        Box::new(crate::remote::AzureBlobCabinet),
        Box::new(crate::remote::GcsCabinet),
    ]
}

/// The one round every cabinet runs: archive, then restore from the receipt.
/// Each half's failure says which half it was.
pub(crate) fn file_through(store: &dyn ArchiveStore, item: ArchiveItem) -> Filed {
    let receipt = match store.archive(item) {
        Ok(receipt) => receipt,
        Err(error) => return Filed::Failed(format!("archive failed: {error}")),
    };
    match store.restore(&receipt) {
        Ok(returned) => Filed::Returned(returned),
        Err(error) => Filed::Failed(format!("restore failed: {error}")),
    }
}

/// Parquet: one item is one Parquet file under the directory, read back with
/// a real Parquet reader.
pub struct ParquetCabinet {
    store: ParquetArchive,
}

impl ParquetCabinet {
    #[must_use]
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        Self {
            store: ParquetArchive::new(dir),
        }
    }
}

impl Cabinet for ParquetCabinet {
    fn technology(&self) -> &'static str {
        "parquet"
    }

    fn file(&self, item: ArchiveItem) -> Filed {
        file_through(&self.store, item)
    }
}

/// `SQLite`: one item is one row of one database file under the directory,
/// restored by row id.
pub struct SqliteCabinet {
    store: SqliteArchive,
}

impl SqliteCabinet {
    #[must_use]
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        Self {
            store: SqliteArchive::new(dir.into().join("archive.sqlite")),
        }
    }
}

impl Cabinet for SqliteCabinet {
    fn technology(&self) -> &'static str {
        "sqlite"
    }

    fn file(&self, item: ArchiveItem) -> Filed {
        file_through(&self.store, item)
    }
}

/// File: one item is one file under the directory with its metadata in a
/// sidecar beside it, and the receipt's checksum is checked on the way back.
pub struct FileCabinet {
    store: FileArchive,
}

impl FileCabinet {
    #[must_use]
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        Self {
            store: FileArchive::new(dir),
        }
    }
}

impl Cabinet for FileCabinet {
    fn technology(&self) -> &'static str {
        "file"
    }

    fn file(&self, item: ArchiveItem) -> Filed {
        file_through(&self.store, item)
    }
}

/// SQL script: one item is one `INSERT` appended to a script per data type
/// under the directory, restored by reading the script back.
pub struct SqlScriptCabinet {
    store: SqlScriptArchive,
}

impl SqlScriptCabinet {
    #[must_use]
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        Self {
            store: SqlScriptArchive::new(dir),
        }
    }
}

impl Cabinet for SqlScriptCabinet {
    fn technology(&self) -> &'static str {
        "sql"
    }

    fn file(&self, item: ArchiveItem) -> Filed {
        file_through(&self.store, item)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::support::{files_whole, scratch};

    #[test]
    fn parquet_files_an_item_whole() {
        let dir = scratch("cabinet-parquet");
        files_whole(&ParquetCabinet::new(&dir));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn sqlite_files_an_item_whole() {
        let dir = scratch("cabinet-sqlite");
        files_whole(&SqliteCabinet::new(&dir));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn file_files_an_item_whole() {
        let dir = scratch("cabinet-file");
        files_whole(&FileCabinet::new(&dir));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn sql_script_files_an_item_whole() {
        let dir = scratch("cabinet-sql");
        files_whole(&SqlScriptCabinet::new(&dir));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn every_cabinet_is_listed_once() {
        let dir = scratch("cabinet-all");
        let mut names: Vec<_> = all_cabinets(&dir)
            .iter()
            .map(|cabinet| cabinet.technology())
            .collect();
        let listed = names.len();
        names.sort_unstable();
        names.dedup();
        assert_eq!(names.len(), listed, "no technology is listed twice");
        assert_eq!(listed, 10, "every archive technology on main");
    }
}
