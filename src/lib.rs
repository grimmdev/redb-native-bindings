use napi::bindgen_prelude::*;
use napi_derive::napi;
use redb::{
    Database, Durability, ReadOnlyDatabase, ReadableTable, ReadableTableMetadata, TableDefinition,
    TableError,
};
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

const DEFAULT_CACHE_BYTES: usize = 32 * 1024 * 1024;

fn map_err<E: std::fmt::Display>(err: E) -> Error {
    Error::from_reason(err.to_string())
}

fn parse_durability(value: Option<&str>) -> Result<Durability> {
    match value {
        None | Some("immediate") => Ok(Durability::Immediate),
        Some("none") => Ok(Durability::None),
        Some(other) => Err(Error::from_reason(format!(
            "Invalid durability '{}': expected 'immediate' or 'none'",
            other
        ))),
    }
}

fn prefix_upper_bound(prefix: &str) -> Option<String> {
    let mut bytes = prefix.as_bytes().to_vec();
    while let Some(last) = bytes.pop() {
        if last < u8::MAX {
            bytes.push(last + 1);
            if let Ok(s) = String::from_utf8(bytes.clone()) {
                return Some(s);
            }
        }
    }
    None
}

#[napi(object)]
#[derive(Debug, Clone)]
pub struct OpenOptions {
    pub cache_size_bytes: Option<i64>,
}

#[napi(object)]
#[derive(Debug, Clone)]
pub struct WriteOptions {
    /// "immediate" (default, fsync on commit) or "none" (skip fsync for bulk/ephemeral writes).
    pub durability: Option<String>,
}

#[napi(object)]
#[derive(Debug, Clone)]
pub struct ScanOptions {
    pub limit: Option<u32>,
    pub reverse: Option<bool>,
}

#[napi(object)]
#[derive(Debug, Clone)]
pub struct KvEntry {
    pub key: String,
    pub value: String,
}

#[napi(object)]
#[derive(Debug, Clone)]
pub struct KvBufferEntry {
    pub key: String,
    pub value: Buffer,
}

/// Single operation inside an atomic cross-table transaction.
/// Supported `op`: "set", "setBuffer", "delete", "assertEq", "assertExists",
/// "assertMissing", "clearTable", "deleteTable".
#[napi(object)]
#[derive(Clone)]
pub struct BatchOp {
    pub op: String,
    pub table: String,
    pub key: Option<String>,
    pub value: Option<String>,
    pub buffer_value: Option<Buffer>,
}

#[napi(object)]
#[derive(Debug, Clone)]
pub struct TableStat {
    pub name: String,
    pub entries: i64,
}

#[napi(object)]
#[derive(Debug, Clone)]
pub struct DbStats {
    pub file_size_bytes: i64,
    pub tables: Vec<TableStat>,
}

// Internal synchronous core shared by both sync and async N-API methods.
// Uses RwLock<Database>: concurrent reads and MVCC writes take a shared read lock
// (redb serializes writes internally), while compact/checkIntegrity take an exclusive lock.
fn do_open_table(db: &Arc<RwLock<Database>>, table_name: &str) -> Result<()> {
    let guard = db.read().map_err(map_err)?;
    let write_txn = guard.begin_write().map_err(map_err)?;
    {
        let def: TableDefinition<&str, &[u8]> = TableDefinition::new(table_name);
        write_txn.open_table(def).map_err(map_err)?;
    }
    write_txn.commit().map_err(map_err)?;
    Ok(())
}

fn do_delete_table(db: &Arc<RwLock<Database>>, table_name: &str) -> Result<bool> {
    let guard = db.read().map_err(map_err)?;
    let write_txn = guard.begin_write().map_err(map_err)?;
    let def: TableDefinition<&str, &[u8]> = TableDefinition::new(table_name);
    let deleted = write_txn.delete_table(def).map_err(map_err)?;
    write_txn.commit().map_err(map_err)?;
    Ok(deleted)
}

fn do_clear_table(db: &Arc<RwLock<Database>>, table_name: &str) -> Result<()> {
    let guard = db.read().map_err(map_err)?;
    let write_txn = guard.begin_write().map_err(map_err)?;
    let def: TableDefinition<&str, &[u8]> = TableDefinition::new(table_name);
    let _ = write_txn.delete_table(def).map_err(map_err)?;
    {
        let def2: TableDefinition<&str, &[u8]> = TableDefinition::new(table_name);
        write_txn.open_table(def2).map_err(map_err)?;
    }
    write_txn.commit().map_err(map_err)?;
    Ok(())
}

fn do_list_tables(db: &Arc<RwLock<Database>>) -> Result<Vec<String>> {
    let guard = db.read().map_err(map_err)?;
    let read_txn = guard.begin_read().map_err(map_err)?;
    let mut names = Vec::new();
    for handle in read_txn.list_tables().map_err(map_err)? {
        names.push(handle.name().to_string());
    }
    Ok(names)
}

fn do_count(db: &Arc<RwLock<Database>>, table_name: &str) -> Result<i64> {
    let guard = db.read().map_err(map_err)?;
    let read_txn = guard.begin_read().map_err(map_err)?;
    let def: TableDefinition<&str, &[u8]> = TableDefinition::new(table_name);
    let table = match read_txn.open_table(def) {
        Ok(t) => t,
        Err(TableError::TableDoesNotExist(_)) => return Ok(0),
        Err(e) => return Err(map_err(e)),
    };
    Ok(table.len().map_err(map_err)? as i64)
}

fn do_get_bytes(db: &Arc<RwLock<Database>>, table_name: &str, key: &str) -> Result<Option<Vec<u8>>> {
    let guard = db.read().map_err(map_err)?;
    let read_txn = guard.begin_read().map_err(map_err)?;
    let def: TableDefinition<&str, &[u8]> = TableDefinition::new(table_name);
    let table = match read_txn.open_table(def) {
        Ok(t) => t,
        Err(TableError::TableDoesNotExist(_)) => return Ok(None),
        Err(e) => return Err(map_err(e)),
    };
    Ok(table.get(key).map_err(map_err)?.map(|v| v.value().to_vec()))
}

fn do_get_many_strings(
    db: &Arc<RwLock<Database>>,
    table_name: &str,
    keys: &[String],
) -> Result<Vec<Option<String>>> {
    let guard = db.read().map_err(map_err)?;
    let read_txn = guard.begin_read().map_err(map_err)?;
    let def: TableDefinition<&str, &[u8]> = TableDefinition::new(table_name);
    let table = match read_txn.open_table(def) {
        Ok(t) => t,
        Err(TableError::TableDoesNotExist(_)) => return Ok(vec![None; keys.len()]),
        Err(e) => return Err(map_err(e)),
    };
    let mut results = Vec::with_capacity(keys.len());
    for key in keys {
        match table.get(key.as_str()).map_err(map_err)? {
            Some(v) => {
                let s = String::from_utf8(v.value().to_vec()).map_err(map_err)?;
                results.push(Some(s));
            }
            None => results.push(None),
        }
    }
    Ok(results)
}

fn do_has(db: &Arc<RwLock<Database>>, table_name: &str, key: &str) -> Result<bool> {
    let guard = db.read().map_err(map_err)?;
    let read_txn = guard.begin_read().map_err(map_err)?;
    let def: TableDefinition<&str, &[u8]> = TableDefinition::new(table_name);
    let table = match read_txn.open_table(def) {
        Ok(t) => t,
        Err(TableError::TableDoesNotExist(_)) => return Ok(false),
        Err(e) => return Err(map_err(e)),
    };
    Ok(table.get(key).map_err(map_err)?.is_some())
}

fn do_set_bytes(
    db: &Arc<RwLock<Database>>,
    table_name: &str,
    key: &str,
    bytes: &[u8],
    durability: Durability,
) -> Result<()> {
    let guard = db.read().map_err(map_err)?;
    let mut write_txn = guard.begin_write().map_err(map_err)?;
    write_txn.set_durability(durability);
    {
        let def: TableDefinition<&str, &[u8]> = TableDefinition::new(table_name);
        let mut table = write_txn.open_table(def).map_err(map_err)?;
        table.insert(key, bytes).map_err(map_err)?;
    }
    write_txn.commit().map_err(map_err)?;
    Ok(())
}

fn do_delete(
    db: &Arc<RwLock<Database>>,
    table_name: &str,
    key: &str,
    durability: Durability,
) -> Result<bool> {
    let guard = db.read().map_err(map_err)?;
    let mut write_txn = guard.begin_write().map_err(map_err)?;
    write_txn.set_durability(durability);
    let removed = {
        let def: TableDefinition<&str, &[u8]> = TableDefinition::new(table_name);
        let mut table = write_txn.open_table(def).map_err(map_err)?;
        table.remove(key).map_err(map_err)?.is_some()
    };
    write_txn.commit().map_err(map_err)?;
    Ok(removed)
}

fn do_scan_bytes(
    db: &Arc<RwLock<Database>>,
    table_name: &str,
    start: &str,
    end: Option<&str>,
    limit: usize,
    reverse: bool,
) -> Result<Vec<(String, Vec<u8>)>> {
    let guard = db.read().map_err(map_err)?;
    let read_txn = guard.begin_read().map_err(map_err)?;
    let def: TableDefinition<&str, &[u8]> = TableDefinition::new(table_name);
    let table = match read_txn.open_table(def) {
        Ok(t) => t,
        Err(TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
        Err(e) => return Err(map_err(e)),
    };

    let range = match end {
        Some(e) => table.range(start..e).map_err(map_err)?,
        None => table.range(start..).map_err(map_err)?,
    };

    let mut out = Vec::new();
    if reverse {
        for item in range.rev().take(limit) {
            let (k, v) = item.map_err(map_err)?;
            out.push((k.value().to_string(), v.value().to_vec()));
        }
    } else {
        for item in range.take(limit) {
            let (k, v) = item.map_err(map_err)?;
            out.push((k.value().to_string(), v.value().to_vec()));
        }
    }
    Ok(out)
}

fn do_transaction(
    db: &Arc<RwLock<Database>>,
    ops: Vec<BatchOp>,
    durability: Durability,
) -> Result<()> {
    let guard = db.read().map_err(map_err)?;
    let mut write_txn = guard.begin_write().map_err(map_err)?;
    write_txn.set_durability(durability);

    for step in ops {
        match step.op.as_str() {
            "deleteTable" => {
                let def: TableDefinition<&str, &[u8]> = TableDefinition::new(&step.table);
                let _ = write_txn.delete_table(def).map_err(map_err)?;
                continue;
            }
            "clearTable" => {
                let def: TableDefinition<&str, &[u8]> = TableDefinition::new(&step.table);
                let _ = write_txn.delete_table(def).map_err(map_err)?;
                let def2: TableDefinition<&str, &[u8]> = TableDefinition::new(&step.table);
                write_txn.open_table(def2).map_err(map_err)?;
                continue;
            }
            _ => {}
        }

        let key = step
            .key
            .as_deref()
            .ok_or_else(|| Error::from_reason(format!("Op '{}' requires a 'key'", step.op)))?;
        let def: TableDefinition<&str, &[u8]> = TableDefinition::new(&step.table);
        let mut table = write_txn.open_table(def).map_err(map_err)?;

        match step.op.as_str() {
            "assertMissing" => {
                if table.get(key).map_err(map_err)?.is_some() {
                    return Err(Error::from_reason(format!(
                        "CAS failed: key '{}' already exists in table '{}'",
                        key, step.table
                    )));
                }
            }
            "assertExists" => {
                if table.get(key).map_err(map_err)?.is_none() {
                    return Err(Error::from_reason(format!(
                        "CAS failed: key '{}' missing in table '{}'",
                        key, step.table
                    )));
                }
            }
            "assertEq" => {
                let expected: Vec<u8> = if let Some(b) = step.buffer_value {
                    b.to_vec()
                } else if let Some(s) = step.value {
                    s.into_bytes()
                } else {
                    return Err(Error::from_reason(
                        "assertEq requires 'value' or 'bufferValue'",
                    ));
                };
                let actual = table.get(key).map_err(map_err)?.map(|g| g.value().to_vec());
                if actual.as_deref() != Some(expected.as_slice()) {
                    return Err(Error::from_reason(format!(
                        "CAS failed: value mismatch for key '{}' in table '{}'",
                        key, step.table
                    )));
                }
            }
            "set" => {
                let val = step
                    .value
                    .ok_or_else(|| Error::from_reason("set requires 'value'"))?;
                table.insert(key, val.as_bytes()).map_err(map_err)?;
            }
            "setBuffer" => {
                let buf = step
                    .buffer_value
                    .ok_or_else(|| Error::from_reason("setBuffer requires 'bufferValue'"))?;
                table.insert(key, buf.as_ref()).map_err(map_err)?;
            }
            "delete" => {
                table.remove(key).map_err(map_err)?;
            }
            other => {
                return Err(Error::from_reason(format!(
                    "Unsupported transaction op '{}'",
                    other
                )));
            }
        }
    }

    write_txn.commit().map_err(map_err)?;
    Ok(())
}

fn do_stats(db: &Arc<RwLock<Database>>, path: &PathBuf) -> Result<DbStats> {
    let file_size_bytes = std::fs::metadata(path).map(|m| m.len() as i64).unwrap_or(0);
    let guard = db.read().map_err(map_err)?;
    let read_txn = guard.begin_read().map_err(map_err)?;
    let mut tables = Vec::new();
    for handle in read_txn.list_tables().map_err(map_err)? {
        let name = handle.name().to_string();
        let def: TableDefinition<&str, &[u8]> = TableDefinition::new(&name);
        if let Ok(t) = read_txn.open_table(def) {
            let entries = t.len().map_err(map_err)? as i64;
            tables.push(TableStat { name, entries });
        }
    }
    Ok(DbStats {
        file_size_bytes,
        tables,
    })
}

fn to_string_entries(raw: Vec<(String, Vec<u8>)>) -> Result<Vec<KvEntry>> {
    raw.into_iter()
        .map(|(key, bytes)| {
            let value = String::from_utf8(bytes).map_err(map_err)?;
            Ok(KvEntry { key, value })
        })
        .collect()
}

fn to_buffer_entries(raw: Vec<(String, Vec<u8>)>) -> Vec<KvBufferEntry> {
    raw.into_iter()
        .map(|(key, bytes)| KvBufferEntry {
            key,
            value: Buffer::from(bytes),
        })
        .collect()
}

#[napi]
pub struct RedbDatabase {
    db: Arc<RwLock<Database>>,
    path: PathBuf,
}

#[napi]
impl RedbDatabase {
    /// Opens or creates a disk-backed redb database with a bounded RAM cache.
    #[napi(constructor)]
    pub fn new(db_path: String, options: Option<OpenOptions>) -> Result<Self> {
        let cache_bytes = options
            .and_then(|o| o.cache_size_bytes)
            .filter(|&v| v > 0)
            .map(|v| v as usize)
            .unwrap_or(DEFAULT_CACHE_BYTES);

        let db = Database::builder()
            .set_cache_size(cache_bytes)
            .create(&db_path)
            .map_err(map_err)?;

        Ok(Self {
            db: Arc::new(RwLock::new(db)),
            path: PathBuf::from(db_path),
        })
    }

    /// Returns a lightweight cloned handle backed by the same underlying redb instance.
    #[napi]
    pub fn clone_handle(&self) -> Self {
        Self {
            db: Arc::clone(&self.db),
            path: self.path.clone(),
        }
    }

    // --- Table management (async & sync) ---

    #[napi]
    pub async fn open_table(&self, table_name: String) -> Result<()> {
        let db = Arc::clone(&self.db);
        napi::tokio::task::spawn_blocking(move || do_open_table(&db, &table_name))
            .await
            .map_err(map_err)?
    }

    #[napi]
    pub fn open_table_sync(&self, table_name: String) -> Result<()> {
        do_open_table(&self.db, &table_name)
    }

    #[napi]
    pub async fn delete_table(&self, table_name: String) -> Result<bool> {
        let db = Arc::clone(&self.db);
        napi::tokio::task::spawn_blocking(move || do_delete_table(&db, &table_name))
            .await
            .map_err(map_err)?
    }

    #[napi]
    pub fn delete_table_sync(&self, table_name: String) -> Result<bool> {
        do_delete_table(&self.db, &table_name)
    }

    #[napi]
    pub async fn clear_table(&self, table_name: String) -> Result<()> {
        let db = Arc::clone(&self.db);
        napi::tokio::task::spawn_blocking(move || do_clear_table(&db, &table_name))
            .await
            .map_err(map_err)?
    }

    #[napi]
    pub fn clear_table_sync(&self, table_name: String) -> Result<()> {
        do_clear_table(&self.db, &table_name)
    }

    #[napi]
    pub async fn list_tables(&self) -> Result<Vec<String>> {
        let db = Arc::clone(&self.db);
        napi::tokio::task::spawn_blocking(move || do_list_tables(&db))
            .await
            .map_err(map_err)?
    }

    #[napi]
    pub fn list_tables_sync(&self) -> Result<Vec<String>> {
        do_list_tables(&self.db)
    }

    #[napi]
    pub async fn count(&self, table_name: String) -> Result<i64> {
        let db = Arc::clone(&self.db);
        napi::tokio::task::spawn_blocking(move || do_count(&db, &table_name))
            .await
            .map_err(map_err)?
    }

    #[napi]
    pub fn count_sync(&self, table_name: String) -> Result<i64> {
        do_count(&self.db, &table_name)
    }

    // --- Single-key & multi-key reads (async & sync) ---

    #[napi]
    pub async fn get(&self, table_name: String, key: String) -> Result<Option<String>> {
        let db = Arc::clone(&self.db);
        napi::tokio::task::spawn_blocking(move || match do_get_bytes(&db, &table_name, &key)? {
            Some(bytes) => Ok(Some(String::from_utf8(bytes).map_err(map_err)?)),
            None => Ok(None),
        })
        .await
        .map_err(map_err)?
    }

    #[napi]
    pub fn get_sync(&self, table_name: String, key: String) -> Result<Option<String>> {
        match do_get_bytes(&self.db, &table_name, &key)? {
            Some(bytes) => Ok(Some(String::from_utf8(bytes).map_err(map_err)?)),
            None => Ok(None),
        }
    }

    #[napi]
    pub async fn get_buffer(&self, table_name: String, key: String) -> Result<Option<Buffer>> {
        let db = Arc::clone(&self.db);
        napi::tokio::task::spawn_blocking(move || {
            Ok(do_get_bytes(&db, &table_name, &key)?.map(Buffer::from))
        })
        .await
        .map_err(map_err)?
    }

    #[napi]
    pub fn get_buffer_sync(&self, table_name: String, key: String) -> Result<Option<Buffer>> {
        Ok(do_get_bytes(&self.db, &table_name, &key)?.map(Buffer::from))
    }

    #[napi]
    pub async fn get_many(
        &self,
        table_name: String,
        keys: Vec<String>,
    ) -> Result<Vec<Option<String>>> {
        let db = Arc::clone(&self.db);
        napi::tokio::task::spawn_blocking(move || do_get_many_strings(&db, &table_name, &keys))
            .await
            .map_err(map_err)?
    }

    #[napi]
    pub fn get_many_sync(
        &self,
        table_name: String,
        keys: Vec<String>,
    ) -> Result<Vec<Option<String>>> {
        do_get_many_strings(&self.db, &table_name, &keys)
    }

    #[napi]
    pub async fn has(&self, table_name: String, key: String) -> Result<bool> {
        let db = Arc::clone(&self.db);
        napi::tokio::task::spawn_blocking(move || do_has(&db, &table_name, &key))
            .await
            .map_err(map_err)?
    }

    #[napi]
    pub fn has_sync(&self, table_name: String, key: String) -> Result<bool> {
        do_has(&self.db, &table_name, &key)
    }

    // --- Single-key writes (async & sync) ---

    #[napi]
    pub async fn set(
        &self,
        table_name: String,
        key: String,
        value: String,
        options: Option<WriteOptions>,
    ) -> Result<()> {
        let durability = parse_durability(options.as_ref().and_then(|o| o.durability.as_deref()))?;
        let db = Arc::clone(&self.db);
        napi::tokio::task::spawn_blocking(move || {
            do_set_bytes(&db, &table_name, &key, value.as_bytes(), durability)
        })
        .await
        .map_err(map_err)?
    }

    #[napi]
    pub fn set_sync(
        &self,
        table_name: String,
        key: String,
        value: String,
        options: Option<WriteOptions>,
    ) -> Result<()> {
        let durability = parse_durability(options.as_ref().and_then(|o| o.durability.as_deref()))?;
        do_set_bytes(&self.db, &table_name, &key, value.as_bytes(), durability)
    }

    #[napi]
    pub async fn set_buffer(
        &self,
        table_name: String,
        key: String,
        value: Buffer,
        options: Option<WriteOptions>,
    ) -> Result<()> {
        let durability = parse_durability(options.as_ref().and_then(|o| o.durability.as_deref()))?;
        let db = Arc::clone(&self.db);
        let bytes = value.to_vec();
        napi::tokio::task::spawn_blocking(move || {
            do_set_bytes(&db, &table_name, &key, &bytes, durability)
        })
        .await
        .map_err(map_err)?
    }

    #[napi]
    pub fn set_buffer_sync(
        &self,
        table_name: String,
        key: String,
        value: Buffer,
        options: Option<WriteOptions>,
    ) -> Result<()> {
        let durability = parse_durability(options.as_ref().and_then(|o| o.durability.as_deref()))?;
        do_set_bytes(&self.db, &table_name, &key, value.as_ref(), durability)
    }

    #[napi]
    pub async fn delete(
        &self,
        table_name: String,
        key: String,
        options: Option<WriteOptions>,
    ) -> Result<bool> {
        let durability = parse_durability(options.as_ref().and_then(|o| o.durability.as_deref()))?;
        let db = Arc::clone(&self.db);
        napi::tokio::task::spawn_blocking(move || do_delete(&db, &table_name, &key, durability))
            .await
            .map_err(map_err)?
    }

    #[napi]
    pub fn delete_sync(
        &self,
        table_name: String,
        key: String,
        options: Option<WriteOptions>,
    ) -> Result<bool> {
        let durability = parse_durability(options.as_ref().and_then(|o| o.durability.as_deref()))?;
        do_delete(&self.db, &table_name, &key, durability)
    }

    // --- Prefix & range scans (string and binary, async & sync) ---

    #[napi]
    pub async fn scan_prefix(
        &self,
        table_name: String,
        prefix: String,
        options: Option<ScanOptions>,
    ) -> Result<Vec<KvEntry>> {
        let limit = options.as_ref().and_then(|o| o.limit).unwrap_or(1000) as usize;
        let reverse = options.as_ref().and_then(|o| o.reverse).unwrap_or(false);
        let db = Arc::clone(&self.db);
        napi::tokio::task::spawn_blocking(move || {
            let upper = prefix_upper_bound(&prefix);
            let raw = do_scan_bytes(&db, &table_name, &prefix, upper.as_deref(), limit, reverse)?;
            to_string_entries(raw)
        })
        .await
        .map_err(map_err)?
    }

    #[napi]
    pub fn scan_prefix_sync(
        &self,
        table_name: String,
        prefix: String,
        options: Option<ScanOptions>,
    ) -> Result<Vec<KvEntry>> {
        let limit = options.as_ref().and_then(|o| o.limit).unwrap_or(1000) as usize;
        let reverse = options.as_ref().and_then(|o| o.reverse).unwrap_or(false);
        let upper = prefix_upper_bound(&prefix);
        let raw = do_scan_bytes(
            &self.db,
            &table_name,
            &prefix,
            upper.as_deref(),
            limit,
            reverse,
        )?;
        to_string_entries(raw)
    }

    #[napi]
    pub async fn scan_prefix_buffer(
        &self,
        table_name: String,
        prefix: String,
        options: Option<ScanOptions>,
    ) -> Result<Vec<KvBufferEntry>> {
        let limit = options.as_ref().and_then(|o| o.limit).unwrap_or(1000) as usize;
        let reverse = options.as_ref().and_then(|o| o.reverse).unwrap_or(false);
        let db = Arc::clone(&self.db);
        napi::tokio::task::spawn_blocking(move || {
            let upper = prefix_upper_bound(&prefix);
            let raw = do_scan_bytes(&db, &table_name, &prefix, upper.as_deref(), limit, reverse)?;
            Ok(to_buffer_entries(raw))
        })
        .await
        .map_err(map_err)?
    }

    #[napi]
    pub fn scan_prefix_buffer_sync(
        &self,
        table_name: String,
        prefix: String,
        options: Option<ScanOptions>,
    ) -> Result<Vec<KvBufferEntry>> {
        let limit = options.as_ref().and_then(|o| o.limit).unwrap_or(1000) as usize;
        let reverse = options.as_ref().and_then(|o| o.reverse).unwrap_or(false);
        let upper = prefix_upper_bound(&prefix);
        let raw = do_scan_bytes(
            &self.db,
            &table_name,
            &prefix,
            upper.as_deref(),
            limit,
            reverse,
        )?;
        Ok(to_buffer_entries(raw))
    }

    #[napi]
    pub async fn scan_range(
        &self,
        table_name: String,
        start_key: String,
        end_key: String,
        options: Option<ScanOptions>,
    ) -> Result<Vec<KvEntry>> {
        let limit = options.as_ref().and_then(|o| o.limit).unwrap_or(1000) as usize;
        let reverse = options.as_ref().and_then(|o| o.reverse).unwrap_or(false);
        let db = Arc::clone(&self.db);
        napi::tokio::task::spawn_blocking(move || {
            let raw = do_scan_bytes(
                &db,
                &table_name,
                &start_key,
                Some(&end_key),
                limit,
                reverse,
            )?;
            to_string_entries(raw)
        })
        .await
        .map_err(map_err)?
    }

    #[napi]
    pub fn scan_range_sync(
        &self,
        table_name: String,
        start_key: String,
        end_key: String,
        options: Option<ScanOptions>,
    ) -> Result<Vec<KvEntry>> {
        let limit = options.as_ref().and_then(|o| o.limit).unwrap_or(1000) as usize;
        let reverse = options.as_ref().and_then(|o| o.reverse).unwrap_or(false);
        let raw = do_scan_bytes(
            &self.db,
            &table_name,
            &start_key,
            Some(&end_key),
            limit,
            reverse,
        )?;
        to_string_entries(raw)
    }

    #[napi]
    pub async fn scan_range_buffer(
        &self,
        table_name: String,
        start_key: String,
        end_key: String,
        options: Option<ScanOptions>,
    ) -> Result<Vec<KvBufferEntry>> {
        let limit = options.as_ref().and_then(|o| o.limit).unwrap_or(1000) as usize;
        let reverse = options.as_ref().and_then(|o| o.reverse).unwrap_or(false);
        let db = Arc::clone(&self.db);
        napi::tokio::task::spawn_blocking(move || {
            let raw = do_scan_bytes(
                &db,
                &table_name,
                &start_key,
                Some(&end_key),
                limit,
                reverse,
            )?;
            Ok(to_buffer_entries(raw))
        })
        .await
        .map_err(map_err)?
    }

    #[napi]
    pub fn scan_range_buffer_sync(
        &self,
        table_name: String,
        start_key: String,
        end_key: String,
        options: Option<ScanOptions>,
    ) -> Result<Vec<KvBufferEntry>> {
        let limit = options.as_ref().and_then(|o| o.limit).unwrap_or(1000) as usize;
        let reverse = options.as_ref().and_then(|o| o.reverse).unwrap_or(false);
        let raw = do_scan_bytes(
            &self.db,
            &table_name,
            &start_key,
            Some(&end_key),
            limit,
            reverse,
        )?;
        Ok(to_buffer_entries(raw))
    }

    // --- Atomic cross-table CAS transactions (async & sync) ---

    #[napi]
    pub async fn transaction(
        &self,
        ops: Vec<BatchOp>,
        options: Option<WriteOptions>,
    ) -> Result<()> {
        let durability = parse_durability(options.as_ref().and_then(|o| o.durability.as_deref()))?;
        let db = Arc::clone(&self.db);
        napi::tokio::task::spawn_blocking(move || do_transaction(&db, ops, durability))
            .await
            .map_err(map_err)?
    }

    #[napi]
    pub fn transaction_sync(
        &self,
        ops: Vec<BatchOp>,
        options: Option<WriteOptions>,
    ) -> Result<()> {
        let durability = parse_durability(options.as_ref().and_then(|o| o.durability.as_deref()))?;
        do_transaction(&self.db, ops, durability)
    }

    // --- Maintenance & diagnostics (async & sync) ---

    #[napi]
    pub async fn compact(&self) -> Result<bool> {
        let db = Arc::clone(&self.db);
        napi::tokio::task::spawn_blocking(move || {
            let mut guard = db.write().map_err(map_err)?;
            guard.compact().map_err(map_err)
        })
        .await
        .map_err(map_err)?
    }

    #[napi]
    pub fn compact_sync(&self) -> Result<bool> {
        let mut guard = self.db.write().map_err(map_err)?;
        guard.compact().map_err(map_err)
    }

    #[napi]
    pub async fn check_integrity(&self) -> Result<bool> {
        let db = Arc::clone(&self.db);
        napi::tokio::task::spawn_blocking(move || {
            let mut guard = db.write().map_err(map_err)?;
            guard.check_integrity().map_err(map_err)
        })
        .await
        .map_err(map_err)?
    }

    #[napi]
    pub fn check_integrity_sync(&self) -> Result<bool> {
        let mut guard = self.db.write().map_err(map_err)?;
        guard.check_integrity().map_err(map_err)
    }

    #[napi]
    pub async fn stats(&self) -> Result<DbStats> {
        let db = Arc::clone(&self.db);
        let path = self.path.clone();
        napi::tokio::task::spawn_blocking(move || do_stats(&db, &path))
            .await
            .map_err(map_err)?
    }

    #[napi]
    pub fn stats_sync(&self) -> Result<DbStats> {
        do_stats(&self.db, &self.path)
    }
}

/// Dedicated read-only handle using redb::ReadOnlyDatabase for safe concurrent inspection.
#[napi]
pub struct ReadOnlyRedbDatabase {
    db: Arc<ReadOnlyDatabase>,
}

#[napi]
impl ReadOnlyRedbDatabase {
    #[napi(constructor)]
    pub fn new(db_path: String, options: Option<OpenOptions>) -> Result<Self> {
        let cache_bytes = options
            .and_then(|o| o.cache_size_bytes)
            .filter(|&v| v > 0)
            .map(|v| v as usize)
            .unwrap_or(DEFAULT_CACHE_BYTES);

        let db = ReadOnlyDatabase::builder()
            .set_cache_size(cache_bytes)
            .open(&db_path)
            .map_err(map_err)?;

        Ok(Self { db: Arc::new(db) })
    }

    #[napi]
    pub async fn get(&self, table_name: String, key: String) -> Result<Option<String>> {
        let db = Arc::clone(&self.db);
        napi::tokio::task::spawn_blocking(move || {
            let read_txn = db.begin_read().map_err(map_err)?;
            let def: TableDefinition<&str, &[u8]> = TableDefinition::new(&table_name);
            let table = match read_txn.open_table(def) {
                Ok(t) => t,
                Err(TableError::TableDoesNotExist(_)) => return Ok(None),
                Err(e) => return Err(map_err(e)),
            };
            match table.get(key.as_str()).map_err(map_err)? {
                Some(v) => Ok(Some(String::from_utf8(v.value().to_vec()).map_err(map_err)?)),
                None => Ok(None),
            }
        })
        .await
        .map_err(map_err)?
    }

    #[napi]
    pub fn get_sync(&self, table_name: String, key: String) -> Result<Option<String>> {
        let read_txn = self.db.begin_read().map_err(map_err)?;
        let def: TableDefinition<&str, &[u8]> = TableDefinition::new(&table_name);
        let table = match read_txn.open_table(def) {
            Ok(t) => t,
            Err(TableError::TableDoesNotExist(_)) => return Ok(None),
            Err(e) => return Err(map_err(e)),
        };
        match table.get(key.as_str()).map_err(map_err)? {
            Some(v) => Ok(Some(String::from_utf8(v.value().to_vec()).map_err(map_err)?)),
            None => Ok(None),
        }
    }

    #[napi]
    pub fn list_tables_sync(&self) -> Result<Vec<String>> {
        let read_txn = self.db.begin_read().map_err(map_err)?;
        let mut names = Vec::new();
        for handle in read_txn.list_tables().map_err(map_err)? {
            names.push(handle.name().to_string());
        }
        Ok(names)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[test]
    fn sync_async_concurrent_reads_and_cas_transactions() {
        let rt = napi::tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let tmp = NamedTempFile::new().unwrap();
            let path = tmp.path().to_string_lossy().to_string();
            let db = RedbDatabase::new(
                path,
                Some(OpenOptions {
                    cache_size_bytes: Some(8 * 1024 * 1024),
                }),
            )
            .unwrap();

            // Sync and async writes + multi-key reads.
            db.set_sync("users".into(), "u:1".into(), "Ada".into(), None)
                .unwrap();
            db.set("users".into(), "u:2".into(), "Linus".into(), None)
                .await
                .unwrap();

            let batch = db
                .get_many("users".into(), vec!["u:1".into(), "u:2".into(), "u:9".into()])
                .await
                .unwrap();
            assert_eq!(batch, vec![Some("Ada".into()), Some("Linus".into()), None]);

            // Concurrent multi-threaded reads via cloned handles.
            let h1 = db.clone_handle();
            let h2 = db.clone_handle();
            let (r1, r2) = napi::tokio::join!(
                h1.get("users".into(), "u:1".into()),
                h2.get("users".into(), "u:2".into())
            );
            assert_eq!(r1.unwrap().as_deref(), Some("Ada"));
            assert_eq!(r2.unwrap().as_deref(), Some("Linus"));

            // Atomic cross-table CAS transaction with rollback check.
            db.transaction_sync(
                vec![
                    BatchOp {
                        op: "assertEq".into(),
                        table: "users".into(),
                        key: Some("u:1".into()),
                        value: Some("Ada".into()),
                        buffer_value: None,
                    },
                    BatchOp {
                        op: "set".into(),
                        table: "users".into(),
                        key: Some("u:1".into()),
                        value: Some("Ada Lovelace".into()),
                        buffer_value: None,
                    },
                    BatchOp {
                        op: "set".into(),
                        table: "ledger".into(),
                        key: Some("tx:1".into()),
                        value: Some("ok".into()),
                        buffer_value: None,
                    },
                ],
                None,
            )
            .unwrap();
            assert_eq!(
                db.get_sync("users".into(), "u:1".into()).unwrap().as_deref(),
                Some("Ada Lovelace")
            );

            assert!(db.check_integrity().await.unwrap());
            let _ = db.compact().await.unwrap();
            assert_eq!(db.count_sync("users".into()).unwrap(), 2);
            db.clear_table_sync("users".into()).unwrap();
            assert_eq!(db.count_sync("users".into()).unwrap(), 0);
            assert!(db.delete_table("users".into()).await.unwrap());
        });
    }
}
