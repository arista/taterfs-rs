//! Local chunks cache trait for caching chunk locations on the local filesystem.

use std::sync::Arc;

use async_trait::async_trait;
use sha2::{Digest, Sha256};
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt, SeekFrom};

use super::cache_db::{CacheDb, DbId, LocalChunk, PossibleLocalChunk};
use super::key_value_db::KeyValueDbError;

// =============================================================================
// Error Types
// =============================================================================

/// Error type for local chunks cache operations.
#[derive(Debug)]
pub enum LocalChunksCacheError {
    /// An I/O error occurred.
    Io(std::io::Error),
    /// A custom error message.
    Other(String),
}

impl std::fmt::Display for LocalChunksCacheError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LocalChunksCacheError::Io(e) => write!(f, "I/O error: {}", e),
            LocalChunksCacheError::Other(msg) => write!(f, "{}", msg),
        }
    }
}

impl std::error::Error for LocalChunksCacheError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            LocalChunksCacheError::Io(e) => Some(e),
            LocalChunksCacheError::Other(_) => None,
        }
    }
}

impl From<std::io::Error> for LocalChunksCacheError {
    fn from(e: std::io::Error) -> Self {
        LocalChunksCacheError::Io(e)
    }
}

impl From<KeyValueDbError> for LocalChunksCacheError {
    fn from(e: KeyValueDbError) -> Self {
        LocalChunksCacheError::Other(e.to_string())
    }
}

/// Result type for local chunks cache operations.
pub type Result<T> = std::result::Result<T, LocalChunksCacheError>;

// =============================================================================
// LocalChunksCache Trait
// =============================================================================

/// A cache for tracking where chunks can be found on the local filesystem.
///
/// This cache spans the entire filesystem (not scoped to any filestore) and
/// is used to accelerate downloads by finding chunks that already exist locally.
///
/// Entries are hints, not guarantees - consumers should verify chunks exist
/// and remove stale entries.
#[async_trait]
pub trait LocalChunksCache: Send + Sync {
    /// Get or create a path ID for a full path string.
    ///
    /// Creates intermediate path entries as needed.
    /// Returns `None` for empty/root paths (e.g., "" or "/").
    async fn get_path_id(&self, path: &str) -> Result<Option<DbId>>;

    /// Get or create a path ID for a path entry (parent + name).
    ///
    /// If `parent` is `None`, this is a root-level entry.
    async fn get_path_entry_id(&self, parent: Option<DbId>, name: &str) -> Result<DbId>;

    /// Record that a chunk can be found at a specific location in a file.
    async fn set_local_chunk(&self, path_id: DbId, chunk: &LocalChunk) -> Result<()>;

    /// List possible locations where a chunk might be found locally.
    ///
    /// Returns up to 256 entries.
    async fn list_possible_local_chunks(&self, chunk_id: &str) -> Result<Vec<PossibleLocalChunk>>;

    /// Invalidate all local chunk entries for a path.
    ///
    /// Calls both file and directory invalidation.
    async fn invalidate_local_chunks(&self, path_id: DbId) -> Result<()>;

    /// Invalidate local chunk entries for a file.
    async fn invalidate_local_chunk_file(&self, path_id: DbId) -> Result<()>;

    /// Invalidate local chunk entries for a directory and all descendants.
    async fn invalidate_local_chunk_directory(&self, path_id: DbId) -> Result<()>;

    /// Attempt to retrieve a chunk from local disk.
    ///
    /// Searches through possible local chunk locations, reads the data,
    /// verifies the hash matches the chunk_id, and returns the data if found.
    /// Invalidates stale entries that no longer match.
    async fn get_chunk(&self, chunk_id: &str) -> Result<Option<Vec<u8>>>;
}

// =============================================================================
// NoopLocalChunksCache
// =============================================================================

/// A no-op local chunks cache that never caches anything.
pub struct NoopLocalChunksCache;

#[async_trait]
impl LocalChunksCache for NoopLocalChunksCache {
    async fn get_path_id(&self, _path: &str) -> Result<Option<DbId>> {
        Ok(Some(0))
    }

    async fn get_path_entry_id(&self, _parent: Option<DbId>, _name: &str) -> Result<DbId> {
        Ok(0)
    }

    async fn set_local_chunk(&self, _path_id: DbId, _chunk: &LocalChunk) -> Result<()> {
        Ok(())
    }

    async fn list_possible_local_chunks(&self, _chunk_id: &str) -> Result<Vec<PossibleLocalChunk>> {
        Ok(Vec::new())
    }

    async fn invalidate_local_chunks(&self, _path_id: DbId) -> Result<()> {
        Ok(())
    }

    async fn invalidate_local_chunk_file(&self, _path_id: DbId) -> Result<()> {
        Ok(())
    }

    async fn invalidate_local_chunk_directory(&self, _path_id: DbId) -> Result<()> {
        Ok(())
    }

    async fn get_chunk(&self, _chunk_id: &str) -> Result<Option<Vec<u8>>> {
        Ok(None)
    }
}

// =============================================================================
// DbLocalChunksCache
// =============================================================================

/// A local chunks cache backed by [`CacheDb`].
pub struct DbLocalChunksCache {
    cache_db: Arc<CacheDb>,
}

impl DbLocalChunksCache {
    /// Create a new database-backed local chunks cache.
    pub fn new(cache_db: Arc<CacheDb>) -> Self {
        Self { cache_db }
    }

    /// Read a chunk from a file and verify its hash.
    ///
    /// Returns `Ok(Some(data))` if the chunk was read and verified,
    /// `Ok(None)` if the hash didn't match, or `Err` on I/O error.
    async fn read_and_verify_chunk(
        &self,
        path: &std::path::Path,
        offset: u64,
        length: u64,
        expected_chunk_id: &str,
    ) -> std::result::Result<Option<Vec<u8>>, std::io::Error> {
        let mut file = File::open(path).await?;
        file.seek(SeekFrom::Start(offset)).await?;

        let mut buffer = vec![0u8; length as usize];
        file.read_exact(&mut buffer).await?;

        // Compute hash and verify
        let mut hasher = Sha256::new();
        hasher.update(&buffer);
        let result = hasher.finalize();
        let computed_id = hex::encode(result);

        if computed_id == expected_chunk_id {
            Ok(Some(buffer))
        } else {
            Ok(None)
        }
    }

    /// Invalidate a chunk entry (helper to avoid verbose error handling).
    async fn invalidate_chunk_entry(
        &self,
        path_id: DbId,
        chunk_id: &str,
        possible: &PossibleLocalChunk,
    ) {
        let chunk = LocalChunk {
            chunk_id: chunk_id.to_string(),
            offset: possible.offset,
            length: possible.length,
        };
        // Ignore errors during invalidation - best effort
        let _ = self.cache_db.invalidate_local_chunk(path_id, &chunk).await;
    }
}

#[async_trait]
impl LocalChunksCache for DbLocalChunksCache {
    async fn get_path_id(&self, path: &str) -> Result<Option<DbId>> {
        Ok(self.cache_db.get_path_id(path).await?)
    }

    async fn get_path_entry_id(&self, parent: Option<DbId>, name: &str) -> Result<DbId> {
        Ok(self
            .cache_db
            .get_or_create_path_entry_id_by_name(parent, name)
            .await?)
    }

    async fn set_local_chunk(&self, path_id: DbId, chunk: &LocalChunk) -> Result<()> {
        Ok(self.cache_db.set_local_chunk(path_id, chunk).await?)
    }

    async fn list_possible_local_chunks(&self, chunk_id: &str) -> Result<Vec<PossibleLocalChunk>> {
        Ok(self.cache_db.list_possible_local_chunks(chunk_id).await?)
    }

    async fn invalidate_local_chunks(&self, path_id: DbId) -> Result<()> {
        Ok(self.cache_db.invalidate_local_chunks(path_id).await?)
    }

    async fn invalidate_local_chunk_file(&self, path_id: DbId) -> Result<()> {
        Ok(self.cache_db.invalidate_local_chunk_file(path_id).await?)
    }

    async fn invalidate_local_chunk_directory(&self, path_id: DbId) -> Result<()> {
        Ok(self
            .cache_db
            .invalidate_local_chunk_directory(path_id)
            .await?)
    }

    async fn get_chunk(&self, chunk_id: &str) -> Result<Option<Vec<u8>>> {
        let possible_chunks = self.list_possible_local_chunks(chunk_id).await?;

        for possible in possible_chunks {
            // Get the file path from path_id
            let path = match self.cache_db.get_path(possible.path_id).await? {
                Some(p) => p,
                None => {
                    // Path no longer exists in cache, invalidate
                    self.invalidate_chunk_entry(possible.path_id, chunk_id, &possible)
                        .await;
                    continue;
                }
            };

            // Try to read and verify the chunk
            match self
                .read_and_verify_chunk(&path, possible.offset, possible.length, chunk_id)
                .await
            {
                Ok(Some(data)) => return Ok(Some(data)),
                Ok(None) => {
                    // Hash didn't match, invalidate
                    self.invalidate_chunk_entry(possible.path_id, chunk_id, &possible)
                        .await;
                }
                Err(_) => {
                    // I/O error (file not found, etc.), invalidate
                    self.invalidate_chunk_entry(possible.path_id, chunk_id, &possible)
                        .await;
                }
            }
        }

        Ok(None)
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
    use std::io::Write;
    use tempfile::TempDir;

    fn create_test_cache() -> (TempDir, DbLocalChunksCache) {
        let temp_dir = TempDir::new().unwrap();
        let lmdb = Arc::new(LmdbKeyValueDb::new(temp_dir.path()).unwrap());
        let object_cache = Arc::new(NoopObjectCacheDb);
        let cache_db = Arc::new(CacheDb::new(lmdb, object_cache));
        let cache = DbLocalChunksCache::new(cache_db);
        (temp_dir, cache)
    }

    #[tokio::test]
    async fn test_local_chunks_cache_basic() {
        let (_temp, cache) = create_test_cache();

        // Get path ID
        let path_id = cache
            .get_path_id("/data/files/test.bin")
            .await
            .unwrap()
            .unwrap();
        assert!(path_id > 0);

        // Same path should return same ID
        let path_id2 = cache
            .get_path_id("/data/files/test.bin")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(path_id, path_id2);
    }

    #[tokio::test]
    async fn test_local_chunks_cache_set_and_list() {
        let (_temp, cache) = create_test_cache();

        let path_id = cache
            .get_path_id("/data/file.bin")
            .await
            .unwrap()
            .unwrap();

        // Initially no chunks
        let chunks = cache.list_possible_local_chunks("abc123").await.unwrap();
        assert!(chunks.is_empty());

        // Add a chunk
        let chunk = LocalChunk {
            chunk_id: "abc123".to_string(),
            offset: 0,
            length: 4096,
        };
        cache.set_local_chunk(path_id, &chunk).await.unwrap();

        // Should find it now
        let chunks = cache.list_possible_local_chunks("abc123").await.unwrap();
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].path_id, path_id);
        assert_eq!(chunks[0].offset, 0);
        assert_eq!(chunks[0].length, 4096);
    }

    #[tokio::test]
    async fn test_local_chunks_cache_invalidate() {
        let (_temp, cache) = create_test_cache();

        let path_id = cache
            .get_path_id("/data/file.bin")
            .await
            .unwrap()
            .unwrap();

        // Add chunks
        let chunk = LocalChunk {
            chunk_id: "chunk1".to_string(),
            offset: 0,
            length: 1024,
        };
        cache.set_local_chunk(path_id, &chunk).await.unwrap();

        // Verify it exists
        assert_eq!(
            cache
                .list_possible_local_chunks("chunk1")
                .await
                .unwrap()
                .len(),
            1
        );

        // Invalidate
        cache.invalidate_local_chunk_file(path_id).await.unwrap();

        // Should be gone
        assert!(
            cache
                .list_possible_local_chunks("chunk1")
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn test_path_entry_id() {
        let (_temp, cache) = create_test_cache();

        // Build path incrementally
        let data_id = cache.get_path_entry_id(None, "data").await.unwrap();
        let files_id = cache.get_path_entry_id(Some(data_id), "files").await.unwrap();
        let test_id = cache
            .get_path_entry_id(Some(files_id), "test.bin")
            .await
            .unwrap();

        // Full path should give same ID
        let full_path_id = cache
            .get_path_id("data/files/test.bin")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(test_id, full_path_id);
    }

    #[tokio::test]
    async fn test_get_chunk_found() {
        let (temp, cache) = create_test_cache();

        // Create a test file with known content
        let test_content = b"Hello, this is test content for the chunk!";
        let file_path = temp.path().join("test_file.bin");
        {
            let mut file = std::fs::File::create(&file_path).unwrap();
            file.write_all(test_content).unwrap();
        }

        // Compute the expected chunk ID (SHA-256 hash)
        let mut hasher = Sha256::new();
        hasher.update(test_content);
        let result = hasher.finalize();
        let expected_chunk_id = hex::encode(result);

        // Register the chunk in the cache
        let path_str = file_path.to_str().unwrap();
        let path_id = cache.get_path_id(path_str).await.unwrap().unwrap();

        let chunk = LocalChunk {
            chunk_id: expected_chunk_id.clone(),
            offset: 0,
            length: test_content.len() as u64,
        };
        cache.set_local_chunk(path_id, &chunk).await.unwrap();

        // Retrieve the chunk
        let retrieved = cache.get_chunk(&expected_chunk_id).await.unwrap();
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap(), test_content);
    }

    #[tokio::test]
    async fn test_get_chunk_not_found() {
        let (_temp, cache) = create_test_cache();

        // Try to get a chunk that doesn't exist
        let result = cache.get_chunk("nonexistent_chunk_id").await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_get_chunk_stale_entry_invalidated() {
        let (temp, cache) = create_test_cache();

        // Create a test file with initial content
        let initial_content = b"Initial content";
        let file_path = temp.path().join("test_file2.bin");
        {
            let mut file = std::fs::File::create(&file_path).unwrap();
            file.write_all(initial_content).unwrap();
        }

        // Compute the chunk ID for initial content
        let mut hasher = Sha256::new();
        hasher.update(initial_content);
        let result = hasher.finalize();
        let initial_chunk_id = hex::encode(result);

        // Register the chunk
        let path_str = file_path.to_str().unwrap();
        let path_id = cache.get_path_id(path_str).await.unwrap().unwrap();

        let chunk = LocalChunk {
            chunk_id: initial_chunk_id.clone(),
            offset: 0,
            length: initial_content.len() as u64,
        };
        cache.set_local_chunk(path_id, &chunk).await.unwrap();

        // Verify chunk is registered
        let chunks = cache
            .list_possible_local_chunks(&initial_chunk_id)
            .await
            .unwrap();
        assert_eq!(chunks.len(), 1);

        // Modify the file (content no longer matches hash)
        {
            let mut file = std::fs::File::create(&file_path).unwrap();
            file.write_all(b"Modified content").unwrap();
        }

        // Try to get the chunk - should fail and invalidate the entry
        let result = cache.get_chunk(&initial_chunk_id).await.unwrap();
        assert!(result.is_none());

        // Entry should be invalidated
        let chunks = cache
            .list_possible_local_chunks(&initial_chunk_id)
            .await
            .unwrap();
        assert!(chunks.is_empty());
    }

    #[tokio::test]
    async fn test_get_chunk_file_deleted_invalidated() {
        let (temp, cache) = create_test_cache();

        // Create a test file
        let test_content = b"Test content";
        let file_path = temp.path().join("test_file3.bin");
        {
            let mut file = std::fs::File::create(&file_path).unwrap();
            file.write_all(test_content).unwrap();
        }

        // Compute the chunk ID
        let mut hasher = Sha256::new();
        hasher.update(test_content);
        let result = hasher.finalize();
        let chunk_id = hex::encode(result);

        // Register the chunk
        let path_str = file_path.to_str().unwrap();
        let path_id = cache.get_path_id(path_str).await.unwrap().unwrap();

        let chunk = LocalChunk {
            chunk_id: chunk_id.clone(),
            offset: 0,
            length: test_content.len() as u64,
        };
        cache.set_local_chunk(path_id, &chunk).await.unwrap();

        // Delete the file
        std::fs::remove_file(&file_path).unwrap();

        // Try to get the chunk - should fail and invalidate
        let result = cache.get_chunk(&chunk_id).await.unwrap();
        assert!(result.is_none());

        // Entry should be invalidated
        let chunks = cache.list_possible_local_chunks(&chunk_id).await.unwrap();
        assert!(chunks.is_empty());
    }
}
