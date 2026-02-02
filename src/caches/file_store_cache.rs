//! File store cache trait for caching file fingerprints and object IDs.

use std::sync::Arc;

use async_trait::async_trait;

use super::cache_db::CacheDb;
use super::key_value_db::KeyValueDbError;

// =============================================================================
// Error Types
// =============================================================================

/// Error type for file store cache operations.
#[derive(Debug)]
pub enum FileStoreCacheError {
    /// An I/O error occurred.
    Io(std::io::Error),
    /// A custom error message.
    Other(String),
}

impl std::fmt::Display for FileStoreCacheError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FileStoreCacheError::Io(e) => write!(f, "I/O error: {}", e),
            FileStoreCacheError::Other(msg) => write!(f, "{}", msg),
        }
    }
}

impl std::error::Error for FileStoreCacheError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            FileStoreCacheError::Io(e) => Some(e),
            FileStoreCacheError::Other(_) => None,
        }
    }
}

impl From<std::io::Error> for FileStoreCacheError {
    fn from(e: std::io::Error) -> Self {
        FileStoreCacheError::Io(e)
    }
}

impl From<KeyValueDbError> for FileStoreCacheError {
    fn from(e: KeyValueDbError) -> Self {
        FileStoreCacheError::Other(e.to_string())
    }
}

/// Result type for file store cache operations.
pub type Result<T> = std::result::Result<T, FileStoreCacheError>;

// Re-export types for convenience
pub use super::cache_db::{DbId, FingerprintedFileInfo};

// =============================================================================
// FileStoreCache Trait
// =============================================================================

/// A cache for file store fingerprint information.
///
/// Maps file paths to their fingerprints and corresponding object IDs,
/// enabling quick change detection without reading file contents.
#[async_trait]
pub trait FileStoreCache: Send + Sync {
    /// Get or create a path ID for a full path string.
    ///
    /// Creates intermediate path entries as needed.
    /// Returns `None` for empty/root paths (e.g., "" or "/").
    async fn get_path_id(&self, path: &str) -> Result<Option<DbId>>;

    /// Get or create a path ID for a path entry (parent + name).
    ///
    /// If `parent` is `None`, this is a root-level entry.
    async fn get_path_entry_id(&self, parent: Option<DbId>, name: &str) -> Result<DbId>;

    /// Get cached fingerprint info for a path ID.
    ///
    /// Returns `None` if the path ID is not in the cache.
    async fn get_fingerprinted_file_info(
        &self,
        path_id: DbId,
    ) -> Result<Option<FingerprintedFileInfo>>;

    /// Cache fingerprint info for a path ID.
    async fn set_fingerprinted_file_info(
        &self,
        path_id: DbId,
        info: &FingerprintedFileInfo,
    ) -> Result<()>;
}

/// A provider of file store caches, keyed by file store URL.
///
/// Implementations manage a collection of caches, typically one per file store.
#[async_trait]
pub trait FileStoreCaches: Send + Sync {
    /// Get or create a cache for the file store with the given URL.
    async fn get_cache(&self, url: &str) -> std::result::Result<Arc<dyn FileStoreCache>, String>;
}

// =============================================================================
// NoopFileStoreCache
// =============================================================================

/// A no-op file store cache that never caches anything.
///
/// This implementation returns dummy path IDs and never stores fingerprint info.
pub struct NoopFileStoreCache;

#[async_trait]
impl FileStoreCache for NoopFileStoreCache {
    async fn get_path_id(&self, _path: &str) -> Result<Option<DbId>> {
        // Return a dummy ID - this cache doesn't actually store anything
        Ok(Some(0))
    }

    async fn get_path_entry_id(&self, _parent: Option<DbId>, _name: &str) -> Result<DbId> {
        // Return a dummy ID - this cache doesn't actually store anything
        Ok(0)
    }

    async fn get_fingerprinted_file_info(
        &self,
        _path_id: DbId,
    ) -> Result<Option<FingerprintedFileInfo>> {
        Ok(None)
    }

    async fn set_fingerprinted_file_info(
        &self,
        _path_id: DbId,
        _info: &FingerprintedFileInfo,
    ) -> Result<()> {
        Ok(())
    }
}

// =============================================================================
// NoopFileStoreCaches
// =============================================================================

/// A no-op implementation that always returns [`NoopFileStoreCache`].
pub struct NoopFileStoreCaches;

#[async_trait]
impl FileStoreCaches for NoopFileStoreCaches {
    async fn get_cache(&self, _url: &str) -> std::result::Result<Arc<dyn FileStoreCache>, String> {
        Ok(Arc::new(NoopFileStoreCache))
    }
}

// =============================================================================
// DbFileStoreCache
// =============================================================================

/// A file store cache backed by [`CacheDb`].
pub struct DbFileStoreCache {
    cache_db: Arc<CacheDb>,
    filestore_id: DbId,
}

impl DbFileStoreCache {
    /// Create a new database-backed file store cache.
    pub fn new(cache_db: Arc<CacheDb>, filestore_id: DbId) -> Self {
        Self {
            cache_db,
            filestore_id,
        }
    }
}

#[async_trait]
impl FileStoreCache for DbFileStoreCache {
    async fn get_path_id(&self, path: &str) -> Result<Option<DbId>> {
        // CacheDb.get_path_id internally calls get_or_create_* methods
        Ok(self.cache_db.get_path_id(self.filestore_id, path).await?)
    }

    async fn get_path_entry_id(&self, parent: Option<DbId>, name: &str) -> Result<DbId> {
        // First get or create the name ID
        let name_id = self
            .cache_db
            .get_or_create_name_id(self.filestore_id, name)
            .await?;
        // Then get or create the path entry ID
        Ok(self
            .cache_db
            .get_or_create_path_entry_id(self.filestore_id, parent, name_id)
            .await?)
    }

    async fn get_fingerprinted_file_info(
        &self,
        path_id: DbId,
    ) -> Result<Option<FingerprintedFileInfo>> {
        Ok(self
            .cache_db
            .get_fingerprinted_file_info(self.filestore_id, path_id)
            .await?)
    }

    async fn set_fingerprinted_file_info(
        &self,
        path_id: DbId,
        info: &FingerprintedFileInfo,
    ) -> Result<()> {
        self.cache_db
            .set_fingerprinted_file_info(self.filestore_id, path_id, info)
            .await?;
        Ok(())
    }
}

// =============================================================================
// DbFileStoreCaches
// =============================================================================

/// A provider of database-backed file store caches.
pub struct DbFileStoreCaches {
    cache_db: Arc<CacheDb>,
}

impl DbFileStoreCaches {
    /// Create a new provider backed by the given cache database.
    pub fn new(cache_db: Arc<CacheDb>) -> Self {
        Self { cache_db }
    }
}

#[async_trait]
impl FileStoreCaches for DbFileStoreCaches {
    async fn get_cache(&self, url: &str) -> std::result::Result<Arc<dyn FileStoreCache>, String> {
        let filestore_id = self
            .cache_db
            .get_or_create_filestore_id(url)
            .await
            .map_err(|e| e.to_string())?;

        Ok(Arc::new(DbFileStoreCache::new(
            self.cache_db.clone(),
            filestore_id,
        )))
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::super::lmdb_key_value_db::LmdbKeyValueDb;
    use super::super::object_cache_db::NoopObjectCacheDb;
    use super::*;
    use tempfile::TempDir;

    fn create_test_caches() -> (TempDir, DbFileStoreCaches) {
        let temp_dir = TempDir::new().unwrap();
        let lmdb = Arc::new(LmdbKeyValueDb::new(temp_dir.path()).unwrap());
        let object_cache = Arc::new(NoopObjectCacheDb);
        let cache_db = Arc::new(CacheDb::new(lmdb, object_cache));
        let caches = DbFileStoreCaches::new(cache_db);
        (temp_dir, caches)
    }

    #[tokio::test]
    async fn test_file_store_cache() {
        let (_temp, caches) = create_test_caches();

        let cache = caches.get_cache("file:///test/store").await.unwrap();

        // Get path ID (creates it)
        let path_id = cache.get_path_id("foo/bar.txt").await.unwrap().unwrap();
        assert!(path_id > 0);

        // Same path should return same ID
        let path_id2 = cache.get_path_id("foo/bar.txt").await.unwrap().unwrap();
        assert_eq!(path_id, path_id2);

        // Initially not cached
        assert!(
            cache
                .get_fingerprinted_file_info(path_id)
                .await
                .unwrap()
                .is_none()
        );

        // Set info
        let info = FingerprintedFileInfo {
            fingerprint: "12345:100:-".to_string(),
            object_id: "abc123".to_string(),
        };
        cache
            .set_fingerprinted_file_info(path_id, &info)
            .await
            .unwrap();

        // Get it back
        let retrieved = cache
            .get_fingerprinted_file_info(path_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(retrieved, info);
    }

    #[tokio::test]
    async fn test_different_filestores() {
        let (_temp, caches) = create_test_caches();

        let cache1 = caches.get_cache("file:///store1").await.unwrap();
        let cache2 = caches.get_cache("file:///store2").await.unwrap();

        let info1 = FingerprintedFileInfo {
            fingerprint: "111".to_string(),
            object_id: "aaa".to_string(),
        };
        let info2 = FingerprintedFileInfo {
            fingerprint: "222".to_string(),
            object_id: "bbb".to_string(),
        };

        // Same path, different stores - get path IDs first
        let path_id1 = cache1.get_path_id("test.txt").await.unwrap().unwrap();
        let path_id2 = cache2.get_path_id("test.txt").await.unwrap().unwrap();

        cache1
            .set_fingerprinted_file_info(path_id1, &info1)
            .await
            .unwrap();
        cache2
            .set_fingerprinted_file_info(path_id2, &info2)
            .await
            .unwrap();

        // Each should have its own data
        assert_eq!(
            cache1
                .get_fingerprinted_file_info(path_id1)
                .await
                .unwrap()
                .unwrap(),
            info1
        );
        assert_eq!(
            cache2
                .get_fingerprinted_file_info(path_id2)
                .await
                .unwrap()
                .unwrap(),
            info2
        );
    }

    #[tokio::test]
    async fn test_get_path_entry_id() {
        let (_temp, caches) = create_test_caches();

        let cache = caches.get_cache("file:///test/store").await.unwrap();

        // Create path entries incrementally
        let foo_id = cache.get_path_entry_id(None, "foo").await.unwrap();
        let bar_id = cache.get_path_entry_id(Some(foo_id), "bar").await.unwrap();
        let baz_id = cache
            .get_path_entry_id(Some(bar_id), "baz.txt")
            .await
            .unwrap();

        // Should get same IDs on repeated calls
        let foo_id2 = cache.get_path_entry_id(None, "foo").await.unwrap();
        let bar_id2 = cache.get_path_entry_id(Some(foo_id), "bar").await.unwrap();

        assert_eq!(foo_id, foo_id2);
        assert_eq!(bar_id, bar_id2);

        // The full path should give us the same final ID
        let full_path_id = cache.get_path_id("foo/bar/baz.txt").await.unwrap().unwrap();
        assert_eq!(baz_id, full_path_id);
    }
}
