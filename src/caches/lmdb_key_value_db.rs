//! LMDB-based key-value database implementation.
//!
//! Uses the heed crate to provide a persistent key-value store backed by LMDB.

use std::ops::Bound;
use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use heed::types::Bytes;
use heed::{Database, Env, EnvOpenOptions};

/// Number of entries to fetch in each batch during list_entries iteration.
const LIST_ENTRIES_BATCH_SIZE: usize = 64;

use super::key_value_db::{
    KeyValueDb, KeyValueDbError, KeyValueDbTransaction, KeyValueDbWrites, KeyValueEntries,
    KeyValueEntry, Result, WriteOp,
};

// =============================================================================
// LmdbKeyValueDb
// =============================================================================

/// An LMDB-backed key-value database.
///
/// This is a simple implementation without caching - both `write()` and
/// `transaction()` create immediate transactions.
pub struct LmdbKeyValueDb {
    env: Arc<Env>,
    db: Database<Bytes, Bytes>,
}

impl LmdbKeyValueDb {
    /// Create a new LMDB database at the given path.
    ///
    /// Creates the directory if it doesn't exist.
    pub fn new(path: &Path) -> Result<Self> {
        // Ensure directory exists
        std::fs::create_dir_all(path)?;

        // Open LMDB environment
        // TODO: Make map_size configurable. Also consider failure scenarios:
        // - What happens when the database reaches max size? (writes fail)
        // - Should we monitor usage and warn before hitting the limit?
        // - Should we implement cache eviction at the LMDB level?
        // - How do we recover if the database becomes full?
        let env = unsafe {
            EnvOpenOptions::new()
                .map_size(1024 * 1024 * 1024) // 1GB max size
                .max_dbs(1)
                .open(path)
                .map_err(|e| KeyValueDbError::Database(e.to_string()))?
        };

        // Open or create the default database
        let mut wtxn = env
            .write_txn()
            .map_err(|e| KeyValueDbError::Database(e.to_string()))?;
        let db: Database<Bytes, Bytes> = env
            .create_database(&mut wtxn, None)
            .map_err(|e| KeyValueDbError::Database(e.to_string()))?;
        wtxn.commit()
            .map_err(|e| KeyValueDbError::Database(e.to_string()))?;

        Ok(Self {
            env: Arc::new(env),
            db,
        })
    }
}

#[async_trait]
impl KeyValueDb for LmdbKeyValueDb {
    async fn exists(&self, key: &[u8]) -> Result<bool> {
        let env = self.env.clone();
        let db = self.db;
        let key = key.to_vec();

        tokio::task::spawn_blocking(move || {
            let rtxn = env
                .read_txn()
                .map_err(|e| KeyValueDbError::Database(e.to_string()))?;
            let exists = db
                .get(&rtxn, &key)
                .map_err(|e| KeyValueDbError::Database(e.to_string()))?
                .is_some();
            Ok(exists)
        })
        .await
        .map_err(|e| KeyValueDbError::Database(e.to_string()))?
    }

    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let env = self.env.clone();
        let db = self.db;
        let key = key.to_vec();

        tokio::task::spawn_blocking(move || {
            let rtxn = env
                .read_txn()
                .map_err(|e| KeyValueDbError::Database(e.to_string()))?;
            let value = db
                .get(&rtxn, &key)
                .map_err(|e| KeyValueDbError::Database(e.to_string()))?
                .map(|v| v.to_vec());
            Ok(value)
        })
        .await
        .map_err(|e| KeyValueDbError::Database(e.to_string()))?
    }

    async fn list_entries(&self, prefix: &[u8]) -> Result<Box<dyn KeyValueEntries + Send>> {
        Ok(Box::new(LmdbKeyValueEntries::new(
            self.env.clone(),
            self.db,
            prefix.to_vec(),
        )))
    }

    async fn transaction(&self) -> Result<Box<dyn KeyValueDbTransaction + Send>> {
        Ok(Box::new(LmdbTransaction {
            env: self.env.clone(),
            db: self.db,
            pending: Vec::new(),
        }))
    }

    async fn write(&self) -> Result<Box<dyn KeyValueDbWrites + Send>> {
        // For LMDB without caching, write() behaves the same as transaction()
        Ok(Box::new(LmdbWrites {
            env: self.env.clone(),
            db: self.db,
            pending: Vec::new(),
        }))
    }
}

// =============================================================================
// LmdbTransaction
// =============================================================================

struct LmdbTransaction {
    env: Arc<Env>,
    db: Database<Bytes, Bytes>,
    pending: Vec<WriteOp>,
}

#[async_trait]
impl KeyValueDbTransaction for LmdbTransaction {
    async fn exists(&self, key: &[u8]) -> Result<bool> {
        // Check pending writes first
        for op in self.pending.iter().rev() {
            if op.key() == key {
                return Ok(matches!(op, WriteOp::Set { .. }));
            }
        }

        let env = self.env.clone();
        let db = self.db;
        let key = key.to_vec();

        tokio::task::spawn_blocking(move || {
            let rtxn = env
                .read_txn()
                .map_err(|e| KeyValueDbError::Database(e.to_string()))?;
            let exists = db
                .get(&rtxn, &key)
                .map_err(|e| KeyValueDbError::Database(e.to_string()))?
                .is_some();
            Ok(exists)
        })
        .await
        .map_err(|e| KeyValueDbError::Database(e.to_string()))?
    }

    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        // Check pending writes first
        for op in self.pending.iter().rev() {
            if op.key() == key {
                return match op {
                    WriteOp::Set { value, .. } => Ok(Some(value.clone())),
                    WriteOp::Del { .. } => Ok(None),
                };
            }
        }

        let env = self.env.clone();
        let db = self.db;
        let key = key.to_vec();

        tokio::task::spawn_blocking(move || {
            let rtxn = env
                .read_txn()
                .map_err(|e| KeyValueDbError::Database(e.to_string()))?;
            let value = db
                .get(&rtxn, &key)
                .map_err(|e| KeyValueDbError::Database(e.to_string()))?
                .map(|v| v.to_vec());
            Ok(value)
        })
        .await
        .map_err(|e| KeyValueDbError::Database(e.to_string()))?
    }

    async fn set(&mut self, key: Vec<u8>, val: Vec<u8>) {
        self.pending.push(WriteOp::Set { key, value: val });
    }

    async fn del(&mut self, key: Vec<u8>) {
        self.pending.push(WriteOp::Del { key });
    }

    async fn commit(self: Box<Self>) -> Result<()> {
        if self.pending.is_empty() {
            return Ok(());
        }

        let env = self.env;
        let db = self.db;
        let pending = self.pending;

        tokio::task::spawn_blocking(move || {
            let mut wtxn = env
                .write_txn()
                .map_err(|e| KeyValueDbError::Database(e.to_string()))?;

            for op in pending {
                match op {
                    WriteOp::Set { key, value } => {
                        db.put(&mut wtxn, &key, &value)
                            .map_err(|e| KeyValueDbError::Database(e.to_string()))?;
                    }
                    WriteOp::Del { key } => {
                        db.delete(&mut wtxn, &key)
                            .map_err(|e| KeyValueDbError::Database(e.to_string()))?;
                    }
                }
            }

            wtxn.commit()
                .map_err(|e| KeyValueDbError::Database(e.to_string()))?;
            Ok(())
        })
        .await
        .map_err(|e| KeyValueDbError::Database(e.to_string()))?
    }
}

// =============================================================================
// LmdbWrites
// =============================================================================

struct LmdbWrites {
    env: Arc<Env>,
    db: Database<Bytes, Bytes>,
    pending: Vec<WriteOp>,
}

#[async_trait]
impl KeyValueDbWrites for LmdbWrites {
    async fn set(&mut self, key: Vec<u8>, val: Vec<u8>) {
        self.pending.push(WriteOp::Set { key, value: val });
    }

    async fn del(&mut self, key: Vec<u8>) {
        self.pending.push(WriteOp::Del { key });
    }

    async fn flush(self: Box<Self>) -> Result<()> {
        if self.pending.is_empty() {
            return Ok(());
        }

        let env = self.env;
        let db = self.db;
        let pending = self.pending;

        tokio::task::spawn_blocking(move || {
            let mut wtxn = env
                .write_txn()
                .map_err(|e| KeyValueDbError::Database(e.to_string()))?;

            for op in pending {
                match op {
                    WriteOp::Set { key, value } => {
                        db.put(&mut wtxn, &key, &value)
                            .map_err(|e| KeyValueDbError::Database(e.to_string()))?;
                    }
                    WriteOp::Del { key } => {
                        db.delete(&mut wtxn, &key)
                            .map_err(|e| KeyValueDbError::Database(e.to_string()))?;
                    }
                }
            }

            wtxn.commit()
                .map_err(|e| KeyValueDbError::Database(e.to_string()))?;
            Ok(())
        })
        .await
        .map_err(|e| KeyValueDbError::Database(e.to_string()))?
    }
}

// =============================================================================
// LmdbKeyValueEntries
// =============================================================================

/// Iterator over LMDB entries matching a prefix, fetched in batches.
struct LmdbKeyValueEntries {
    env: Arc<Env>,
    db: Database<Bytes, Bytes>,
    prefix: Vec<u8>,
    /// Current batch of entries.
    batch: Vec<KeyValueEntry>,
    /// Index into the current batch.
    batch_index: usize,
    /// The last key we saw, used to resume iteration for the next batch.
    /// None means we haven't started or we're done.
    last_key: Option<Vec<u8>>,
    /// Whether we've exhausted all entries.
    exhausted: bool,
}

impl LmdbKeyValueEntries {
    fn new(env: Arc<Env>, db: Database<Bytes, Bytes>, prefix: Vec<u8>) -> Self {
        Self {
            env,
            db,
            prefix,
            batch: Vec::new(),
            batch_index: 0,
            last_key: None,
            exhausted: false,
        }
    }

    /// Fetch the next batch of entries.
    async fn fetch_batch(&mut self) -> Result<()> {
        let env = self.env.clone();
        let db = self.db;
        let prefix = self.prefix.clone();
        let last_key = self.last_key.clone();

        let batch = tokio::task::spawn_blocking(move || -> Result<Vec<KeyValueEntry>> {
            let rtxn = env
                .read_txn()
                .map_err(|e| KeyValueDbError::Database(e.to_string()))?;

            let mut entries = Vec::with_capacity(LIST_ENTRIES_BATCH_SIZE);

            if prefix.is_empty() {
                // Empty prefix: iterate all entries
                match &last_key {
                    None => {
                        let iter = db
                            .iter(&rtxn)
                            .map_err(|e| KeyValueDbError::Database(e.to_string()))?;

                        for result in iter.take(LIST_ENTRIES_BATCH_SIZE) {
                            let (key, value) =
                                result.map_err(|e| KeyValueDbError::Database(e.to_string()))?;
                            entries.push(KeyValueEntry::new(key.to_vec(), value.to_vec()));
                        }
                    }
                    Some(last) => {
                        let iter = db
                            .range(
                                &rtxn,
                                &(Bound::Excluded(last.as_slice()), Bound::Unbounded),
                            )
                            .map_err(|e| KeyValueDbError::Database(e.to_string()))?;

                        for result in iter.take(LIST_ENTRIES_BATCH_SIZE) {
                            let (key, value) =
                                result.map_err(|e| KeyValueDbError::Database(e.to_string()))?;
                            entries.push(KeyValueEntry::new(key.to_vec(), value.to_vec()));
                        }
                    }
                }
            } else {
                // With prefix: use prefix iteration
                match &last_key {
                    None => {
                        // First batch: start from the prefix
                        let iter = db
                            .prefix_iter(&rtxn, &prefix)
                            .map_err(|e| KeyValueDbError::Database(e.to_string()))?;

                        for result in iter.take(LIST_ENTRIES_BATCH_SIZE) {
                            let (key, value) =
                                result.map_err(|e| KeyValueDbError::Database(e.to_string()))?;
                            entries.push(KeyValueEntry::new(key.to_vec(), value.to_vec()));
                        }
                    }
                    Some(last) => {
                        // Subsequent batches: use range starting after last key
                        let iter = db
                            .range(
                                &rtxn,
                                &(Bound::Excluded(last.as_slice()), Bound::Unbounded),
                            )
                            .map_err(|e| KeyValueDbError::Database(e.to_string()))?;

                        for result in iter.take(LIST_ENTRIES_BATCH_SIZE) {
                            let (key, value) =
                                result.map_err(|e| KeyValueDbError::Database(e.to_string()))?;
                            // Check if still within prefix
                            if !key.starts_with(&prefix) {
                                break;
                            }
                            entries.push(KeyValueEntry::new(key.to_vec(), value.to_vec()));
                        }
                    }
                }
            }

            Ok(entries)
        })
        .await
        .map_err(|e| KeyValueDbError::Database(e.to_string()))??;

        self.exhausted = batch.len() < LIST_ENTRIES_BATCH_SIZE;
        self.batch = batch;
        self.batch_index = 0;

        Ok(())
    }
}

#[async_trait]
impl KeyValueEntries for LmdbKeyValueEntries {
    async fn next(&mut self) -> Result<Option<KeyValueEntry>> {
        // If we need a new batch, fetch one
        if self.batch_index >= self.batch.len() {
            if self.exhausted {
                return Ok(None);
            }
            self.fetch_batch().await?;
            if self.batch.is_empty() {
                return Ok(None);
            }
        }

        let entry = self.batch[self.batch_index].clone();
        self.batch_index += 1;
        self.last_key = Some(entry.key.clone());
        Ok(Some(entry))
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_basic_operations() {
        let temp_dir = TempDir::new().unwrap();
        let db = LmdbKeyValueDb::new(temp_dir.path()).unwrap();

        // Initially empty
        assert!(!db.exists(b"key1").await.unwrap());
        assert!(db.get(b"key1").await.unwrap().is_none());

        // Write a value
        let mut writes = db.write().await.unwrap();
        writes.set(b"key1".to_vec(), b"value1".to_vec()).await;
        writes.flush().await.unwrap();

        // Now it exists
        assert!(db.exists(b"key1").await.unwrap());
        assert_eq!(db.get(b"key1").await.unwrap(), Some(b"value1".to_vec()));
    }

    #[tokio::test]
    async fn test_transaction() {
        let temp_dir = TempDir::new().unwrap();
        let db = LmdbKeyValueDb::new(temp_dir.path()).unwrap();

        // Use a transaction
        let mut txn = db.transaction().await.unwrap();
        txn.set(b"key1".to_vec(), b"value1".to_vec()).await;
        txn.set(b"key2".to_vec(), b"value2".to_vec()).await;

        // Within transaction, we can see pending writes
        assert_eq!(txn.get(b"key1").await.unwrap(), Some(b"value1".to_vec()));

        txn.commit().await.unwrap();

        // After commit, values are persisted
        assert_eq!(db.get(b"key1").await.unwrap(), Some(b"value1".to_vec()));
        assert_eq!(db.get(b"key2").await.unwrap(), Some(b"value2".to_vec()));
    }

    #[tokio::test]
    async fn test_delete() {
        let temp_dir = TempDir::new().unwrap();
        let db = LmdbKeyValueDb::new(temp_dir.path()).unwrap();

        // Write a value
        let mut writes = db.write().await.unwrap();
        writes.set(b"key1".to_vec(), b"value1".to_vec()).await;
        writes.flush().await.unwrap();

        assert!(db.exists(b"key1").await.unwrap());

        // Delete it
        let mut writes = db.write().await.unwrap();
        writes.del(b"key1".to_vec()).await;
        writes.flush().await.unwrap();

        assert!(!db.exists(b"key1").await.unwrap());
    }

    #[tokio::test]
    async fn test_list_entries() {
        let temp_dir = TempDir::new().unwrap();
        let db = LmdbKeyValueDb::new(temp_dir.path()).unwrap();

        // Write some values with prefixes
        let mut writes = db.write().await.unwrap();
        writes.set(b"prefix/a".to_vec(), b"value_a".to_vec()).await;
        writes.set(b"prefix/b".to_vec(), b"value_b".to_vec()).await;
        writes.set(b"prefix/c".to_vec(), b"value_c".to_vec()).await;
        writes.set(b"other/x".to_vec(), b"value_x".to_vec()).await;
        writes.flush().await.unwrap();

        // List entries with prefix
        let mut entries = db.list_entries(b"prefix/").await.unwrap();

        let e1 = entries.next().await.unwrap().unwrap();
        assert_eq!(e1.key, b"prefix/a");
        assert_eq!(e1.value, b"value_a");

        let e2 = entries.next().await.unwrap().unwrap();
        assert_eq!(e2.key, b"prefix/b");
        assert_eq!(e2.value, b"value_b");

        let e3 = entries.next().await.unwrap().unwrap();
        assert_eq!(e3.key, b"prefix/c");
        assert_eq!(e3.value, b"value_c");

        // No more entries
        assert!(entries.next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_list_entries_empty_prefix() {
        let temp_dir = TempDir::new().unwrap();
        let db = LmdbKeyValueDb::new(temp_dir.path()).unwrap();

        // Write some values
        let mut writes = db.write().await.unwrap();
        writes.set(b"a".to_vec(), b"1".to_vec()).await;
        writes.set(b"b".to_vec(), b"2".to_vec()).await;
        writes.flush().await.unwrap();

        // List all entries (empty prefix)
        let mut entries = db.list_entries(b"").await.unwrap();

        let e1 = entries.next().await.unwrap().unwrap();
        assert_eq!(e1.key, b"a");

        let e2 = entries.next().await.unwrap().unwrap();
        assert_eq!(e2.key, b"b");

        assert!(entries.next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_list_entries_no_match() {
        let temp_dir = TempDir::new().unwrap();
        let db = LmdbKeyValueDb::new(temp_dir.path()).unwrap();

        // Write some values
        let mut writes = db.write().await.unwrap();
        writes.set(b"foo/a".to_vec(), b"1".to_vec()).await;
        writes.flush().await.unwrap();

        // List with non-matching prefix
        let mut entries = db.list_entries(b"bar/").await.unwrap();
        assert!(entries.next().await.unwrap().is_none());
    }
}
