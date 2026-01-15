//! Caching layer for key-value databases.
//!
//! Provides write-back caching with LRU eviction on top of any KeyValueDb.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use lru::LruCache;
use tokio::sync::Mutex;

use super::key_value_db::{
    KeyValueDb, KeyValueDbTransaction, KeyValueDbWrites, Result, WriteOp,
};

// =============================================================================
// Configuration
// =============================================================================

/// Configuration for the caching layer.
#[derive(Debug, Clone)]
pub struct CachingConfig {
    /// How often to flush pending writes (milliseconds).
    pub flush_period_ms: u64,
    /// Maximum number of pending writes before forcing a flush.
    pub max_pending_count: usize,
    /// Maximum total size of pending writes before forcing a flush.
    pub max_pending_size: usize,
    /// Maximum memory for the read cache (bytes).
    pub max_cache_size: usize,
}

impl Default for CachingConfig {
    fn default() -> Self {
        Self {
            flush_period_ms: 500,
            max_pending_count: 10_000,
            max_pending_size: 10 * 1024 * 1024, // 10MB
            max_cache_size: 100 * 1024 * 1024,  // 100MB
        }
    }
}

// =============================================================================
// CacheEntry
// =============================================================================

/// A cached value with its size.
#[derive(Clone)]
enum CacheEntry {
    /// Value exists with this data.
    Present(Vec<u8>),
    /// Value is known to not exist.
    Absent,
}

impl CacheEntry {
    fn size(&self) -> usize {
        match self {
            CacheEntry::Present(v) => v.len(),
            CacheEntry::Absent => 0,
        }
    }
}

// =============================================================================
// CachingKeyValueDb
// =============================================================================

/// A caching wrapper around a KeyValueDb.
///
/// Provides:
/// - LRU cache for reads
/// - Write-back buffering with periodic flushes
/// - Direct transaction support for operations requiring atomicity
pub struct CachingKeyValueDb {
    inner: Arc<dyn KeyValueDb>,
    state: Arc<Mutex<CacheState>>,
    config: CachingConfig,
}

struct CacheState {
    /// LRU cache for read values.
    cache: LruCache<Vec<u8>, CacheEntry>,
    /// Current size of cached data.
    cache_size: usize,
    /// Pending writes to be flushed.
    pending: HashMap<Vec<u8>, WriteOp>,
    /// Total size of pending writes.
    pending_size: usize,
    /// Last flush time.
    last_flush: Instant,
}

impl CachingKeyValueDb {
    /// Create a new caching wrapper around the given database.
    pub fn new(inner: Arc<dyn KeyValueDb>, config: CachingConfig) -> Self {
        // Use a large capacity since we manage size ourselves
        let cache = LruCache::unbounded();

        Self {
            inner,
            state: Arc::new(Mutex::new(CacheState {
                cache,
                cache_size: 0,
                pending: HashMap::new(),
                pending_size: 0,
                last_flush: Instant::now(),
            })),
            config,
        }
    }

    /// Flush pending writes if conditions are met.
    async fn maybe_flush(&self) -> Result<()> {
        let should_flush = {
            let state = self.state.lock().await;
            let elapsed = state.last_flush.elapsed();

            state.pending.len() >= self.config.max_pending_count
                || state.pending_size >= self.config.max_pending_size
                || elapsed >= Duration::from_millis(self.config.flush_period_ms)
        };

        if should_flush {
            self.flush().await?;
        }

        Ok(())
    }

    /// Force flush all pending writes.
    pub async fn flush(&self) -> Result<()> {
        let pending = {
            let mut state = self.state.lock().await;
            if state.pending.is_empty() {
                return Ok(());
            }
            state.last_flush = Instant::now();
            state.pending_size = 0;
            std::mem::take(&mut state.pending)
        };

        // Write all pending operations
        let mut txn = self.inner.transaction().await?;
        for (_, op) in pending {
            match op {
                WriteOp::Set { key, value } => txn.set(key, value),
                WriteOp::Del { key } => txn.del(key),
            }
        }
        txn.commit().await?;

        Ok(())
    }

    /// Add an entry to the cache, evicting old entries if needed.
    fn add_to_cache(state: &mut CacheState, key: Vec<u8>, entry: CacheEntry, max_size: usize) {
        let entry_size = key.len() + entry.size();

        // Remove old entry if it exists
        if let Some((_, old_entry)) = state.cache.pop_entry(&key) {
            state.cache_size = state.cache_size.saturating_sub(key.len() + old_entry.size());
        }

        // Evict entries until we have room
        while state.cache_size + entry_size > max_size && !state.cache.is_empty() {
            if let Some((old_key, old_entry)) = state.cache.pop_lru() {
                // Don't evict pending writes
                if state.pending.contains_key(&old_key) {
                    // Put it back and try another
                    state.cache.put(old_key, old_entry);
                    break;
                }
                state.cache_size = state
                    .cache_size
                    .saturating_sub(old_key.len() + old_entry.size());
            }
        }

        state.cache.put(key, entry);
        state.cache_size += entry_size;
    }
}

#[async_trait]
impl KeyValueDb for CachingKeyValueDb {
    async fn exists(&self, key: &[u8]) -> Result<bool> {
        // Check cache first
        {
            let mut state = self.state.lock().await;

            // Check pending writes
            if let Some(op) = state.pending.get(key) {
                return Ok(matches!(op, WriteOp::Set { .. }));
            }

            // Check read cache
            if let Some(entry) = state.cache.get(key) {
                return Ok(matches!(entry, CacheEntry::Present(_)));
            }
        }

        // Query underlying database
        let exists = self.inner.exists(key).await?;

        // Cache the result
        {
            let mut state = self.state.lock().await;
            let entry = if exists {
                // We don't have the value, just mark as present
                // A subsequent get() will fetch the actual value
                CacheEntry::Present(Vec::new())
            } else {
                CacheEntry::Absent
            };
            Self::add_to_cache(&mut state, key.to_vec(), entry, self.config.max_cache_size);
        }

        Ok(exists)
    }

    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        // Check cache first
        {
            let mut state = self.state.lock().await;

            // Check pending writes
            if let Some(op) = state.pending.get(key) {
                return match op {
                    WriteOp::Set { value, .. } => Ok(Some(value.clone())),
                    WriteOp::Del { .. } => Ok(None),
                };
            }

            // Check read cache
            if let Some(entry) = state.cache.get(key) {
                match entry {
                    CacheEntry::Present(v) if !v.is_empty() => return Ok(Some(v.clone())),
                    CacheEntry::Present(_) => {
                        // We know it exists but don't have the value cached
                        // Fall through to fetch it
                    }
                    CacheEntry::Absent => return Ok(None),
                }
            }
        }

        // Query underlying database
        let value = self.inner.get(key).await?;

        // Cache the result
        {
            let mut state = self.state.lock().await;
            let entry = match &value {
                Some(v) => CacheEntry::Present(v.clone()),
                None => CacheEntry::Absent,
            };
            Self::add_to_cache(&mut state, key.to_vec(), entry, self.config.max_cache_size);
        }

        Ok(value)
    }

    async fn transaction(&self) -> Result<Box<dyn KeyValueDbTransaction + Send>> {
        // Transactions go directly to the underlying database
        // but we need to include any pending writes in reads
        Ok(Box::new(CachingTransaction {
            inner: Mutex::new(self.inner.transaction().await?),
            state: self.state.clone(),
            local_pending: Mutex::new(Vec::new()),
        }))
    }

    async fn write(&self) -> Result<Box<dyn KeyValueDbWrites + Send>> {
        // Maybe flush before returning a new write handle
        self.maybe_flush().await?;

        Ok(Box::new(CachingWrites {
            state: self.state.clone(),
            config: self.config.clone(),
            local_pending: Vec::new(),
        }))
    }
}

// =============================================================================
// CachingTransaction
// =============================================================================

struct CachingTransaction {
    inner: Mutex<Box<dyn KeyValueDbTransaction + Send>>,
    state: Arc<Mutex<CacheState>>,
    local_pending: Mutex<Vec<WriteOp>>,
}

#[async_trait]
impl KeyValueDbTransaction for CachingTransaction {
    async fn exists(&self, key: &[u8]) -> Result<bool> {
        // Check local pending first
        {
            let local_pending = self.local_pending.lock().await;
            for op in local_pending.iter().rev() {
                if op.key() == key {
                    return Ok(matches!(op, WriteOp::Set { .. }));
                }
            }
        }

        // Check shared pending writes
        {
            let state = self.state.lock().await;
            if let Some(op) = state.pending.get(key) {
                return Ok(matches!(op, WriteOp::Set { .. }));
            }
        }

        self.inner.lock().await.exists(key).await
    }

    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        // Check local pending first
        {
            let local_pending = self.local_pending.lock().await;
            for op in local_pending.iter().rev() {
                if op.key() == key {
                    return match op {
                        WriteOp::Set { value, .. } => Ok(Some(value.clone())),
                        WriteOp::Del { .. } => Ok(None),
                    };
                }
            }
        }

        // Check shared pending writes
        {
            let state = self.state.lock().await;
            if let Some(op) = state.pending.get(key) {
                return match op {
                    WriteOp::Set { value, .. } => Ok(Some(value.clone())),
                    WriteOp::Del { .. } => Ok(None),
                };
            }
        }

        self.inner.lock().await.get(key).await
    }

    fn set(&mut self, key: Vec<u8>, val: Vec<u8>) {
        // This is called with &mut self, so we can use blocking lock
        self.local_pending
            .blocking_lock()
            .push(WriteOp::Set { key, value: val });
    }

    fn del(&mut self, key: Vec<u8>) {
        self.local_pending
            .blocking_lock()
            .push(WriteOp::Del { key });
    }

    async fn commit(self: Box<Self>) -> Result<()> {
        // Extract inner transaction and local pending
        let mut inner = self.inner.into_inner();
        let local_pending = self.local_pending.into_inner();

        // Apply local pending to inner transaction
        for op in local_pending {
            match op {
                WriteOp::Set { key, value } => inner.set(key, value),
                WriteOp::Del { key } => inner.del(key),
            }
        }
        inner.commit().await
    }
}

// =============================================================================
// CachingWrites
// =============================================================================

struct CachingWrites {
    state: Arc<Mutex<CacheState>>,
    config: CachingConfig,
    local_pending: Vec<WriteOp>,
}

#[async_trait]
impl KeyValueDbWrites for CachingWrites {
    fn set(&mut self, key: Vec<u8>, val: Vec<u8>) {
        self.local_pending
            .push(WriteOp::Set { key, value: val });
    }

    fn del(&mut self, key: Vec<u8>) {
        self.local_pending.push(WriteOp::Del { key });
    }

    async fn flush(self: Box<Self>) -> Result<()> {
        if self.local_pending.is_empty() {
            return Ok(());
        }

        // Add to shared pending state
        let mut state = self.state.lock().await;
        for op in self.local_pending {
            let size = op.size();
            let key = op.key().to_vec();

            // Remove old pending if it exists
            if let Some(old_op) = state.pending.remove(&key) {
                state.pending_size = state.pending_size.saturating_sub(old_op.size());
            }

            // Update cache with new value
            let entry = match &op {
                WriteOp::Set { value, .. } => CacheEntry::Present(value.clone()),
                WriteOp::Del { .. } => CacheEntry::Absent,
            };
            CachingKeyValueDb::add_to_cache(
                &mut state,
                key.clone(),
                entry,
                self.config.max_cache_size,
            );

            state.pending.insert(key, op);
            state.pending_size += size;
        }

        Ok(())
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::super::lmdb_key_value_db::LmdbKeyValueDb;
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_caching_basic() {
        let temp_dir = TempDir::new().unwrap();
        let lmdb = Arc::new(LmdbKeyValueDb::new(temp_dir.path()).unwrap());
        let db = CachingKeyValueDb::new(lmdb, CachingConfig::default());

        // Write through cache
        let mut writes = db.write().await.unwrap();
        writes.set(b"key1".to_vec(), b"value1".to_vec());
        writes.flush().await.unwrap();

        // Read from cache
        assert_eq!(db.get(b"key1").await.unwrap(), Some(b"value1".to_vec()));
        assert!(db.exists(b"key1").await.unwrap());
    }

    #[tokio::test]
    async fn test_caching_pending_visible() {
        let temp_dir = TempDir::new().unwrap();
        let lmdb = Arc::new(LmdbKeyValueDb::new(temp_dir.path()).unwrap());
        let db = CachingKeyValueDb::new(lmdb, CachingConfig::default());

        // Write to pending
        let mut writes = db.write().await.unwrap();
        writes.set(b"key1".to_vec(), b"value1".to_vec());
        writes.flush().await.unwrap();

        // Should be visible even before flush to underlying db
        assert_eq!(db.get(b"key1").await.unwrap(), Some(b"value1".to_vec()));

        // Force flush to underlying
        db.flush().await.unwrap();

        // Still visible
        assert_eq!(db.get(b"key1").await.unwrap(), Some(b"value1".to_vec()));
    }

    #[tokio::test]
    async fn test_transaction_sees_pending() {
        let temp_dir = TempDir::new().unwrap();
        let lmdb = Arc::new(LmdbKeyValueDb::new(temp_dir.path()).unwrap());
        let db = CachingKeyValueDb::new(lmdb, CachingConfig::default());

        // Write to pending (not yet flushed to underlying)
        let mut writes = db.write().await.unwrap();
        writes.set(b"key1".to_vec(), b"value1".to_vec());
        writes.flush().await.unwrap();

        // Transaction should see pending writes
        let txn = db.transaction().await.unwrap();
        assert_eq!(txn.get(b"key1").await.unwrap(), Some(b"value1".to_vec()));
    }
}
