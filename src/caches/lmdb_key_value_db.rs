//! LMDB-based key-value database implementation.
//!
//! Uses the heed crate to provide a persistent key-value store backed by LMDB.

use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use heed::types::Bytes;
use heed::{Database, Env, EnvOpenOptions};

use super::key_value_db::{KeyValueDb, KeyValueDbError, KeyValueDbTransaction, KeyValueDbWrites, Result, WriteOp};

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

    fn set(&mut self, key: Vec<u8>, val: Vec<u8>) {
        self.pending.push(WriteOp::Set { key, value: val });
    }

    fn del(&mut self, key: Vec<u8>) {
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
    fn set(&mut self, key: Vec<u8>, val: Vec<u8>) {
        self.pending.push(WriteOp::Set { key, value: val });
    }

    fn del(&mut self, key: Vec<u8>) {
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
        writes.set(b"key1".to_vec(), b"value1".to_vec());
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
        txn.set(b"key1".to_vec(), b"value1".to_vec());
        txn.set(b"key2".to_vec(), b"value2".to_vec());

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
        writes.set(b"key1".to_vec(), b"value1".to_vec());
        writes.flush().await.unwrap();

        assert!(db.exists(b"key1").await.unwrap());

        // Delete it
        let mut writes = db.write().await.unwrap();
        writes.del(b"key1".to_vec());
        writes.flush().await.unwrap();

        assert!(!db.exists(b"key1").await.unwrap());
    }
}
