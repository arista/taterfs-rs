//! Caching layer for key-value databases.
//!
//! Provides write-back caching with LRU eviction on top of any KeyValueDb.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use lru::LruCache;
use tokio::sync::Mutex;

use super::key_value_db::{
    KeyValueDb, KeyValueDbTransaction, KeyValueDbWrites, KeyValueEntries, KeyValueEntry, Result,
    WriteOp,
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

/// A cached value with its size (including key size for memory accounting).
#[derive(Clone)]
struct CacheEntry {
    /// The cached value, or None if known to not exist.
    value: Option<Vec<u8>>,
    /// Size of the key (for accurate memory accounting).
    key_len: usize,
}

impl CacheEntry {
    /// Create an entry for a present value.
    fn present(key_len: usize, value: Vec<u8>) -> Self {
        Self {
            value: Some(value),
            key_len,
        }
    }

    /// Create an entry for an absent (non-existent) key.
    fn absent(key_len: usize) -> Self {
        Self {
            value: None,
            key_len,
        }
    }

    /// Total memory size of this entry (key + value).
    fn size(&self) -> usize {
        self.key_len + self.value.as_ref().map_or(0, |v| v.len())
    }

    /// Whether this entry represents an existing value.
    fn is_present(&self) -> bool {
        self.value.is_some()
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

        // Write all pending operations to underlying database
        let mut txn = self.inner.transaction().await?;
        for op in pending.values() {
            match op {
                WriteOp::Set { key, value } => txn.set(key.clone(), value.clone()).await,
                WriteOp::Del { key } => txn.del(key.clone()).await,
            }
        }
        txn.commit().await?;

        // Now that writes are committed, add them to the cache
        {
            let mut state = self.state.lock().await;
            for (key, op) in pending {
                let entry = match op {
                    WriteOp::Set { value, .. } => CacheEntry::present(key.len(), value),
                    WriteOp::Del { .. } => CacheEntry::absent(key.len()),
                };
                Self::add_to_cache(&mut state, key, entry, self.config.max_cache_size);
            }
        }

        Ok(())
    }

    /// Add an entry to the cache, evicting old entries if needed.
    ///
    /// Note: Pending writes are NOT stored in the cache, so eviction is safe.
    fn add_to_cache(state: &mut CacheState, key: Vec<u8>, entry: CacheEntry, max_size: usize) {
        let entry_size = entry.size();

        // Remove old entry if it exists
        if let Some((_, old_entry)) = state.cache.pop_entry(&key) {
            state.cache_size = state.cache_size.saturating_sub(old_entry.size());
        }

        // Evict entries until we have room
        while state.cache_size + entry_size > max_size && !state.cache.is_empty() {
            if let Some((_, old_entry)) = state.cache.pop_lru() {
                state.cache_size = state.cache_size.saturating_sub(old_entry.size());
            }
        }

        state.cache_size += entry_size;
        state.cache.put(key, entry);
    }
}

#[async_trait]
impl KeyValueDb for CachingKeyValueDb {
    async fn exists(&self, key: &[u8]) -> Result<bool> {
        // Check cache first
        {
            let mut state = self.state.lock().await;

            // Check pending writes first (they are not in the cache)
            if let Some(op) = state.pending.get(key) {
                return Ok(matches!(op, WriteOp::Set { .. }));
            }

            // Check read cache
            if let Some(entry) = state.cache.get(key) {
                return Ok(entry.is_present());
            }
        }

        // Query underlying database
        let exists = self.inner.exists(key).await?;

        // Cache the result (but not if there's a pending write for this key)
        {
            let mut state = self.state.lock().await;
            if !state.pending.contains_key(key) {
                let entry = if exists {
                    // We don't have the value, just mark as present with empty value
                    // A subsequent get() will fetch the actual value
                    CacheEntry::present(key.len(), Vec::new())
                } else {
                    CacheEntry::absent(key.len())
                };
                Self::add_to_cache(&mut state, key.to_vec(), entry, self.config.max_cache_size);
            }
        }

        Ok(exists)
    }

    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        // Check cache first
        {
            let mut state = self.state.lock().await;

            // Check pending writes first (they are not in the cache)
            if let Some(op) = state.pending.get(key) {
                return match op {
                    WriteOp::Set { value, .. } => Ok(Some(value.clone())),
                    WriteOp::Del { .. } => Ok(None),
                };
            }

            // Check read cache
            if let Some(entry) = state.cache.get(key) {
                match &entry.value {
                    Some(v) if !v.is_empty() => return Ok(Some(v.clone())),
                    Some(_) => {
                        // We know it exists but don't have the value cached
                        // Fall through to fetch it
                    }
                    None => return Ok(None),
                }
            }
        }

        // Query underlying database
        let value = self.inner.get(key).await?;

        // Cache the result (but not if there's a pending write for this key)
        {
            let mut state = self.state.lock().await;
            if !state.pending.contains_key(key) {
                let entry = match &value {
                    Some(v) => CacheEntry::present(key.len(), v.clone()),
                    None => CacheEntry::absent(key.len()),
                };
                Self::add_to_cache(&mut state, key.to_vec(), entry, self.config.max_cache_size);
            }
        }

        Ok(value)
    }

    async fn list_entries(&self, prefix: &[u8]) -> Result<Box<dyn KeyValueEntries + Send>> {
        // Get entries from underlying database
        let mut underlying_entries = self.inner.list_entries(prefix).await?;

        // Collect underlying entries into a BTreeMap for sorted iteration
        let mut merged: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
        while let Some(entry) = underlying_entries.next().await? {
            merged.insert(entry.key, entry.value);
        }

        // Overlay pending writes
        {
            let state = self.state.lock().await;
            for (key, op) in &state.pending {
                if key.starts_with(prefix) {
                    match op {
                        WriteOp::Set { value, .. } => {
                            merged.insert(key.clone(), value.clone());
                        }
                        WriteOp::Del { .. } => {
                            merged.remove(key);
                        }
                    }
                }
            }
        }

        // Convert to entry list
        let entries: Vec<KeyValueEntry> = merged
            .into_iter()
            .map(|(key, value)| KeyValueEntry::new(key, value))
            .collect();

        Ok(Box::new(CachingKeyValueEntries { entries, index: 0 }))
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

    async fn set(&mut self, key: Vec<u8>, val: Vec<u8>) {
        self.local_pending
            .lock()
            .await
            .push(WriteOp::Set { key, value: val });
    }

    async fn del(&mut self, key: Vec<u8>) {
        self.local_pending
            .lock()
            .await
            .push(WriteOp::Del { key });
    }

    async fn commit(self: Box<Self>) -> Result<()> {
        // Extract inner transaction and local pending
        let mut inner = self.inner.into_inner();
        let local_pending = self.local_pending.into_inner();

        // Apply local pending to inner transaction
        for op in local_pending {
            match op {
                WriteOp::Set { key, value } => inner.set(key, value).await,
                WriteOp::Del { key } => inner.del(key).await,
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
    local_pending: Vec<WriteOp>,
}

#[async_trait]
impl KeyValueDbWrites for CachingWrites {
    async fn set(&mut self, key: Vec<u8>, val: Vec<u8>) {
        self.local_pending
            .push(WriteOp::Set { key, value: val });
    }

    async fn del(&mut self, key: Vec<u8>) {
        self.local_pending.push(WriteOp::Del { key });
    }

    async fn flush(self: Box<Self>) -> Result<()> {
        if self.local_pending.is_empty() {
            return Ok(());
        }

        // Add to shared pending state (NOT to the cache - pending entries are separate)
        let mut state = self.state.lock().await;
        for op in self.local_pending {
            let size = op.size();
            let key = op.key().to_vec();

            // Remove old pending if it exists
            if let Some(old_op) = state.pending.remove(&key) {
                state.pending_size = state.pending_size.saturating_sub(old_op.size());
            }

            // Remove from cache if present (pending entries are not cached)
            if let Some((_, old_entry)) = state.cache.pop_entry(&key) {
                state.cache_size = state.cache_size.saturating_sub(old_entry.size());
            }

            state.pending.insert(key, op);
            state.pending_size += size;
        }

        Ok(())
    }
}

// =============================================================================
// CachingKeyValueEntries
// =============================================================================

struct CachingKeyValueEntries {
    entries: Vec<KeyValueEntry>,
    index: usize,
}

#[async_trait]
impl KeyValueEntries for CachingKeyValueEntries {
    async fn next(&mut self) -> Result<Option<KeyValueEntry>> {
        if self.index >= self.entries.len() {
            return Ok(None);
        }
        let entry = self.entries[self.index].clone();
        self.index += 1;
        Ok(Some(entry))
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
        writes.set(b"key1".to_vec(), b"value1".to_vec()).await;
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
        writes.set(b"key1".to_vec(), b"value1".to_vec()).await;
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
        writes.set(b"key1".to_vec(), b"value1".to_vec()).await;
        writes.flush().await.unwrap();

        // Transaction should see pending writes
        let txn = db.transaction().await.unwrap();
        assert_eq!(txn.get(b"key1").await.unwrap(), Some(b"value1".to_vec()));
    }

    #[tokio::test]
    async fn test_list_entries_from_underlying() {
        let temp_dir = TempDir::new().unwrap();
        let lmdb = Arc::new(LmdbKeyValueDb::new(temp_dir.path()).unwrap());

        // Write directly to underlying LMDB
        {
            let mut writes = lmdb.write().await.unwrap();
            writes.set(b"prefix/a".to_vec(), b"value_a".to_vec()).await;
            writes.set(b"prefix/b".to_vec(), b"value_b".to_vec()).await;
            writes.flush().await.unwrap();
        }

        let db = CachingKeyValueDb::new(lmdb, CachingConfig::default());

        // List entries should see underlying data
        let mut entries = db.list_entries(b"prefix/").await.unwrap();

        let e1 = entries.next().await.unwrap().unwrap();
        assert_eq!(e1.key, b"prefix/a");

        let e2 = entries.next().await.unwrap().unwrap();
        assert_eq!(e2.key, b"prefix/b");

        assert!(entries.next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_list_entries_sees_pending_writes() {
        let temp_dir = TempDir::new().unwrap();
        let lmdb = Arc::new(LmdbKeyValueDb::new(temp_dir.path()).unwrap());
        let db = CachingKeyValueDb::new(lmdb, CachingConfig::default());

        // Add pending writes (not flushed to underlying)
        let mut writes = db.write().await.unwrap();
        writes.set(b"prefix/a".to_vec(), b"value_a".to_vec()).await;
        writes.set(b"prefix/b".to_vec(), b"value_b".to_vec()).await;
        writes.flush().await.unwrap();

        // list_entries should see pending writes
        let mut entries = db.list_entries(b"prefix/").await.unwrap();

        let e1 = entries.next().await.unwrap().unwrap();
        assert_eq!(e1.key, b"prefix/a");
        assert_eq!(e1.value, b"value_a");

        let e2 = entries.next().await.unwrap().unwrap();
        assert_eq!(e2.key, b"prefix/b");
        assert_eq!(e2.value, b"value_b");

        assert!(entries.next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_list_entries_pending_overrides_underlying() {
        let temp_dir = TempDir::new().unwrap();
        let lmdb = Arc::new(LmdbKeyValueDb::new(temp_dir.path()).unwrap());

        // Write to underlying LMDB
        {
            let mut writes = lmdb.write().await.unwrap();
            writes.set(b"prefix/a".to_vec(), b"old_value".to_vec()).await;
            writes.flush().await.unwrap();
        }

        let db = CachingKeyValueDb::new(lmdb, CachingConfig::default());

        // Override with pending write
        let mut writes = db.write().await.unwrap();
        writes.set(b"prefix/a".to_vec(), b"new_value".to_vec()).await;
        writes.flush().await.unwrap();

        // list_entries should return the pending (new) value
        let mut entries = db.list_entries(b"prefix/").await.unwrap();

        let e1 = entries.next().await.unwrap().unwrap();
        assert_eq!(e1.key, b"prefix/a");
        assert_eq!(e1.value, b"new_value");

        assert!(entries.next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_list_entries_pending_delete_hides_underlying() {
        let temp_dir = TempDir::new().unwrap();
        let lmdb = Arc::new(LmdbKeyValueDb::new(temp_dir.path()).unwrap());

        // Write to underlying LMDB
        {
            let mut writes = lmdb.write().await.unwrap();
            writes.set(b"prefix/a".to_vec(), b"value_a".to_vec()).await;
            writes.set(b"prefix/b".to_vec(), b"value_b".to_vec()).await;
            writes.flush().await.unwrap();
        }

        let db = CachingKeyValueDb::new(lmdb, CachingConfig::default());

        // Delete one key via pending write
        let mut writes = db.write().await.unwrap();
        writes.del(b"prefix/a".to_vec()).await;
        writes.flush().await.unwrap();

        // list_entries should NOT include the deleted key
        let mut entries = db.list_entries(b"prefix/").await.unwrap();

        let e1 = entries.next().await.unwrap().unwrap();
        assert_eq!(e1.key, b"prefix/b");
        assert_eq!(e1.value, b"value_b");

        // Only one entry (the deleted one is hidden)
        assert!(entries.next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_list_entries_merged_sorted() {
        let temp_dir = TempDir::new().unwrap();
        let lmdb = Arc::new(LmdbKeyValueDb::new(temp_dir.path()).unwrap());

        // Write to underlying LMDB
        {
            let mut writes = lmdb.write().await.unwrap();
            writes.set(b"prefix/b".to_vec(), b"value_b".to_vec()).await;
            writes.set(b"prefix/d".to_vec(), b"value_d".to_vec()).await;
            writes.flush().await.unwrap();
        }

        let db = CachingKeyValueDb::new(lmdb, CachingConfig::default());

        // Add interleaved pending writes
        let mut writes = db.write().await.unwrap();
        writes.set(b"prefix/a".to_vec(), b"value_a".to_vec()).await;
        writes.set(b"prefix/c".to_vec(), b"value_c".to_vec()).await;
        writes.flush().await.unwrap();

        // list_entries should return all entries in sorted order
        let mut entries = db.list_entries(b"prefix/").await.unwrap();

        let e1 = entries.next().await.unwrap().unwrap();
        assert_eq!(e1.key, b"prefix/a");

        let e2 = entries.next().await.unwrap().unwrap();
        assert_eq!(e2.key, b"prefix/b");

        let e3 = entries.next().await.unwrap().unwrap();
        assert_eq!(e3.key, b"prefix/c");

        let e4 = entries.next().await.unwrap().unwrap();
        assert_eq!(e4.key, b"prefix/d");

        assert!(entries.next().await.unwrap().is_none());
    }
}
