//! File store cache trait for caching file fingerprints and object IDs.

use std::sync::Arc;

use async_trait::async_trait;

use super::cache_db::{CacheDb, DbId};
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

// =============================================================================
// FingerprintedFileInfo
// =============================================================================

/// Information about a file identified by its fingerprint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FingerprintedFileInfo {
    /// The fingerprint that identifies this version of the file.
    pub fingerprint: String,
    /// The object ID of the file in the repository.
    pub object_id: String,
}

// =============================================================================
// FileStoreCache Trait
// =============================================================================

/// A cache for file store fingerprint information.
///
/// Maps file paths to their fingerprints and corresponding object IDs,
/// enabling quick change detection without reading file contents.
#[async_trait]
pub trait FileStoreCache: Send + Sync {
    /// Get cached fingerprint info for a file path.
    ///
    /// Returns `None` if the path is not in the cache.
    async fn get_fingerprinted_file_info(&self, path: &str) -> Result<Option<FingerprintedFileInfo>>;

    /// Cache fingerprint info for a file path.
    async fn set_fingerprinted_file_info(
        &self,
        path: &str,
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
pub struct NoopFileStoreCache;

#[async_trait]
impl FileStoreCache for NoopFileStoreCache {
    async fn get_fingerprinted_file_info(
        &self,
        _path: &str,
    ) -> Result<Option<FingerprintedFileInfo>> {
        Ok(None)
    }

    async fn set_fingerprinted_file_info(
        &self,
        _path: &str,
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
    async fn get_fingerprinted_file_info(
        &self,
        path: &str,
    ) -> Result<Option<FingerprintedFileInfo>> {
        // Get the path ID (this creates intermediate entries if needed)
        let path_id = match self.cache_db.get_path_id(self.filestore_id, path).await {
            Ok(id) => id,
            Err(_) => return Ok(None), // Path not in cache
        };

        // Get the cached info
        match self
            .cache_db
            .get_fingerprinted_file_info(self.filestore_id, path_id)
            .await?
        {
            Some((fingerprint, object_id)) => Ok(Some(FingerprintedFileInfo {
                fingerprint,
                object_id,
            })),
            None => Ok(None),
        }
    }

    async fn set_fingerprinted_file_info(
        &self,
        path: &str,
        info: &FingerprintedFileInfo,
    ) -> Result<()> {
        // Get or create the path ID
        let path_id = self.cache_db.get_path_id(self.filestore_id, path).await?;

        // Set the cached info
        self.cache_db
            .set_fingerprinted_file_info(
                self.filestore_id,
                path_id,
                &info.fingerprint,
                &info.object_id,
            )
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
    use super::*;
    use tempfile::TempDir;

    fn create_test_caches() -> (TempDir, DbFileStoreCaches) {
        let temp_dir = TempDir::new().unwrap();
        let lmdb = Arc::new(LmdbKeyValueDb::new(temp_dir.path()).unwrap());
        let cache_db = Arc::new(CacheDb::new(lmdb));
        let caches = DbFileStoreCaches::new(cache_db);
        (temp_dir, caches)
    }

    #[tokio::test]
    async fn test_file_store_cache() {
        let (_temp, caches) = create_test_caches();

        let cache = caches.get_cache("file:///test/store").await.unwrap();

        // Initially not cached
        assert!(cache
            .get_fingerprinted_file_info("foo/bar.txt")
            .await
            .unwrap()
            .is_none());

        // Set info
        let info = FingerprintedFileInfo {
            fingerprint: "12345:100:-".to_string(),
            object_id: "abc123".to_string(),
        };
        cache
            .set_fingerprinted_file_info("foo/bar.txt", &info)
            .await
            .unwrap();

        // Get it back
        let retrieved = cache
            .get_fingerprinted_file_info("foo/bar.txt")
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

        // Same path, different stores
        cache1
            .set_fingerprinted_file_info("test.txt", &info1)
            .await
            .unwrap();
        cache2
            .set_fingerprinted_file_info("test.txt", &info2)
            .await
            .unwrap();

        // Each should have its own data
        assert_eq!(
            cache1
                .get_fingerprinted_file_info("test.txt")
                .await
                .unwrap()
                .unwrap(),
            info1
        );
        assert_eq!(
            cache2
                .get_fingerprinted_file_info("test.txt")
                .await
                .unwrap()
                .unwrap(),
            info2
        );
    }
}
