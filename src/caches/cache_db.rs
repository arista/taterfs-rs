//! Cache database built on top of KeyValueDb and ObjectCacheDb.
//!
//! Provides higher-level cache operations for repositories and file stores.

use std::sync::Arc;

use percent_encoding::{NON_ALPHANUMERIC, percent_encode};
use tokio::sync::Mutex;

use crate::repository::RepoObject;

use super::key_value_db::{KeyValueDb, KeyValueDbError, Result};
use super::object_cache_db::ObjectCacheDb;

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

/// Database ID type (u64).
pub type DbId = u64;

/// Block size for ID allocation.
const ID_BLOCK_SIZE: u64 = 100;

// =============================================================================
// Key Prefixes
// =============================================================================

const KEY_NEXT_ID: &[u8] = b"next-id";
const KEY_REPO_ID_PREFIX: &[u8] = b"repository-id-by-uuid/";
const KEY_EXISTS_PREFIX: &[u8] = b"ex/";
const KEY_FULLY_STORED_PREFIX: &[u8] = b"fs/";
const KEY_FILESTORE_ID_PREFIX: &[u8] = b"filestore-id-by-url/";
const KEY_NAME_ID_PREFIX: &[u8] = b"na/";
const KEY_PATH_ENTRY_PREFIX: &[u8] = b"pa/";
const KEY_FILE_INFO_PREFIX: &[u8] = b"fi/";

// =============================================================================
// CacheDb
// =============================================================================

/// A cache database providing repository and file store caching.
///
/// Built on top of a KeyValueDb and ObjectCacheDb, provides:
/// - Repository existence and fully-stored tracking
/// - Repository object caching (via ObjectCacheDb)
/// - File store fingerprint caching
/// - ID allocation with block caching
pub struct CacheDb {
    db: Arc<dyn KeyValueDb>,
    object_cache: Arc<dyn ObjectCacheDb>,
    id_allocator: Mutex<IdAllocator>,
}

struct IdAllocator {
    /// Next ID to return from current block.
    next: u64,
    /// Upper bound of current block (exclusive).
    block_end: u64,
}

impl CacheDb {
    /// Create a new cache database.
    pub fn new(db: Arc<dyn KeyValueDb>, object_cache: Arc<dyn ObjectCacheDb>) -> Self {
        Self {
            db,
            object_cache,
            id_allocator: Mutex::new(IdAllocator {
                next: 0,
                block_end: 0,
            }),
        }
    }

    // =========================================================================
    // ID Generation
    // =========================================================================

    /// Generate a new unique database ID.
    ///
    /// Uses block allocation to minimize transactions.
    pub async fn generate_next_id(&self) -> Result<DbId> {
        let mut allocator = self.id_allocator.lock().await;

        // If we have IDs in the current block, return one
        if allocator.next < allocator.block_end {
            let id = allocator.next;
            allocator.next += 1;
            return Ok(id);
        }

        // Need to allocate a new block
        let mut txn = self.db.transaction().await?;

        // Get current next_id
        let current = match txn.get(KEY_NEXT_ID).await? {
            Some(bytes) => parse_u64(&bytes)?,
            None => 1, // Start at 1 if not set
        };

        // Reserve a block
        let new_next = current + ID_BLOCK_SIZE;
        txn.set(KEY_NEXT_ID.to_vec(), new_next.to_string().into_bytes())
            .await;
        txn.commit().await?;

        // Update local allocator
        allocator.next = current + 1;
        allocator.block_end = new_next;

        Ok(current)
    }

    // =========================================================================
    // Repository ID
    // =========================================================================

    /// Get the database ID for a repository UUID.
    pub async fn get_repository_id(&self, uuid: &str) -> Result<Option<DbId>> {
        let key = repo_id_key(uuid);
        match self.db.get(&key).await? {
            Some(bytes) => Ok(Some(parse_u64(&bytes)?)),
            None => Ok(None),
        }
    }

    /// Set the database ID for a repository UUID.
    pub async fn set_repository_id(&self, uuid: &str, id: DbId) -> Result<()> {
        let key = repo_id_key(uuid);
        let mut writes = self.db.write().await?;
        writes.set(key, id.to_string().into_bytes()).await;
        writes.flush().await
    }

    /// Get or create a database ID for a repository UUID.
    pub async fn get_or_create_repository_id(&self, uuid: &str) -> Result<DbId> {
        // Try to get existing
        if let Some(id) = self.get_repository_id(uuid).await? {
            return Ok(id);
        }

        // Create new - use transaction for atomicity
        let key = repo_id_key(uuid);
        let mut txn = self.db.transaction().await?;

        // Double-check it wasn't created while we were waiting
        if let Some(bytes) = txn.get(&key).await? {
            return parse_u64(&bytes);
        }

        let id = self.generate_next_id().await?;
        txn.set(key, id.to_string().into_bytes()).await;
        txn.commit().await?;

        Ok(id)
    }

    // =========================================================================
    // Object Existence
    // =========================================================================

    /// Check if an object exists in a repository.
    pub async fn get_exists(&self, repo_id: DbId, object_id: &str) -> Result<bool> {
        let key = exists_key(repo_id, object_id);
        self.db.exists(&key).await
    }

    /// Mark an object as existing in a repository.
    pub async fn set_exists(&self, repo_id: DbId, object_id: &str) -> Result<()> {
        let key = exists_key(repo_id, object_id);
        let mut writes = self.db.write().await?;
        writes.set(key, Vec::new()).await;
        writes.flush().await
    }

    // =========================================================================
    // Fully Stored
    // =========================================================================

    /// Check if an object is fully stored (it and all reachable objects exist).
    pub async fn get_fully_stored(&self, repo_id: DbId, object_id: &str) -> Result<bool> {
        let key = fully_stored_key(repo_id, object_id);
        self.db.exists(&key).await
    }

    /// Mark an object as fully stored.
    pub async fn set_fully_stored(&self, repo_id: DbId, object_id: &str) -> Result<()> {
        let key = fully_stored_key(repo_id, object_id);
        let mut writes = self.db.write().await?;
        writes.set(key, Vec::new()).await;
        writes.flush().await
    }

    // =========================================================================
    // Repository Objects
    // =========================================================================

    /// Get a cached repository object.
    pub async fn get_object(&self, object_id: &str) -> Result<Option<RepoObject>> {
        self.object_cache
            .get_object(object_id)
            .await
            .map_err(|e| KeyValueDbError::Encoding(e.to_string()))
    }

    /// Cache a repository object.
    pub async fn set_object(&self, object_id: &str, obj: &RepoObject) -> Result<()> {
        self.object_cache
            .set_object(object_id, obj)
            .await
            .map_err(|e| KeyValueDbError::Encoding(e.to_string()))
    }

    // =========================================================================
    // Filestore ID
    // =========================================================================

    /// Get the database ID for a filestore URL.
    pub async fn get_filestore_id(&self, url: &str) -> Result<Option<DbId>> {
        let key = filestore_id_key(url);
        match self.db.get(&key).await? {
            Some(bytes) => Ok(Some(parse_u64(&bytes)?)),
            None => Ok(None),
        }
    }

    /// Set the database ID for a filestore URL.
    pub async fn set_filestore_id(&self, url: &str, id: DbId) -> Result<()> {
        let key = filestore_id_key(url);
        let mut writes = self.db.write().await?;
        writes.set(key, id.to_string().into_bytes()).await;
        writes.flush().await
    }

    /// Get or create a database ID for a filestore URL.
    pub async fn get_or_create_filestore_id(&self, url: &str) -> Result<DbId> {
        // Try to get existing
        if let Some(id) = self.get_filestore_id(url).await? {
            return Ok(id);
        }

        // Create new
        let key = filestore_id_key(url);
        let mut txn = self.db.transaction().await?;

        if let Some(bytes) = txn.get(&key).await? {
            return parse_u64(&bytes);
        }

        let id = self.generate_next_id().await?;
        txn.set(key, id.to_string().into_bytes()).await;
        txn.commit().await?;

        Ok(id)
    }

    // =========================================================================
    // Name ID (for path component interning)
    // =========================================================================

    /// Get the ID for a name within a filestore.
    pub async fn get_name_id(&self, filestore_id: DbId, name: &str) -> Result<Option<DbId>> {
        let key = name_id_key(filestore_id, name);
        match self.db.get(&key).await? {
            Some(bytes) => Ok(Some(parse_u64(&bytes)?)),
            None => Ok(None),
        }
    }

    /// Set the ID for a name within a filestore.
    pub async fn set_name_id(&self, filestore_id: DbId, name: &str, id: DbId) -> Result<()> {
        let key = name_id_key(filestore_id, name);
        let mut writes = self.db.write().await?;
        writes.set(key, id.to_string().into_bytes()).await;
        writes.flush().await
    }

    /// Get or create an ID for a name within a filestore.
    pub async fn get_or_create_name_id(&self, filestore_id: DbId, name: &str) -> Result<DbId> {
        if let Some(id) = self.get_name_id(filestore_id, name).await? {
            return Ok(id);
        }

        let key = name_id_key(filestore_id, name);
        let mut txn = self.db.transaction().await?;

        if let Some(bytes) = txn.get(&key).await? {
            return parse_u64(&bytes);
        }

        let id = self.generate_next_id().await?;
        txn.set(key, id.to_string().into_bytes()).await;
        txn.commit().await?;

        Ok(id)
    }

    // =========================================================================
    // Path Entry ID
    // =========================================================================

    /// Get the path ID for a path entry.
    pub async fn get_path_entry_id(
        &self,
        filestore_id: DbId,
        parent: Option<DbId>,
        name_id: DbId,
    ) -> Result<Option<DbId>> {
        let key = path_entry_key(filestore_id, parent, name_id);
        match self.db.get(&key).await? {
            Some(bytes) => Ok(Some(parse_u64(&bytes)?)),
            None => Ok(None),
        }
    }

    /// Set the path ID for a path entry.
    pub async fn set_path_entry_id(
        &self,
        filestore_id: DbId,
        parent: Option<DbId>,
        name_id: DbId,
        path_id: DbId,
    ) -> Result<()> {
        let key = path_entry_key(filestore_id, parent, name_id);
        let mut writes = self.db.write().await?;
        writes.set(key, path_id.to_string().into_bytes()).await;
        writes.flush().await
    }

    /// Get or create a path ID for a path entry.
    pub async fn get_or_create_path_entry_id(
        &self,
        filestore_id: DbId,
        parent: Option<DbId>,
        name_id: DbId,
    ) -> Result<DbId> {
        if let Some(id) = self
            .get_path_entry_id(filestore_id, parent, name_id)
            .await?
        {
            return Ok(id);
        }

        let key = path_entry_key(filestore_id, parent, name_id);
        let mut txn = self.db.transaction().await?;

        if let Some(bytes) = txn.get(&key).await? {
            return parse_u64(&bytes);
        }

        let id = self.generate_next_id().await?;
        txn.set(key, id.to_string().into_bytes()).await;
        txn.commit().await?;

        Ok(id)
    }

    /// Get a path ID for a full path string, creating intermediate entries as needed.
    ///
    /// Returns `None` for empty/root paths (e.g., "" or "/").
    pub async fn get_path_id(&self, filestore_id: DbId, path: &str) -> Result<Option<DbId>> {
        let components: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();

        let mut parent: Option<DbId> = None;

        for component in components {
            let name_id = self.get_or_create_name_id(filestore_id, component).await?;
            parent = Some(
                self.get_or_create_path_entry_id(filestore_id, parent, name_id)
                    .await?,
            );
        }

        // For empty/root path, return None
        Ok(parent)
    }

    // =========================================================================
    // Fingerprinted File Info
    // =========================================================================

    /// Get fingerprinted file info.
    pub async fn get_fingerprinted_file_info(
        &self,
        filestore_id: DbId,
        path_id: DbId,
    ) -> Result<Option<FingerprintedFileInfo>> {
        let key = file_info_key(filestore_id, path_id);
        match self.db.get(&key).await? {
            Some(bytes) => {
                let s = String::from_utf8(bytes)
                    .map_err(|e| KeyValueDbError::Encoding(e.to_string()))?;
                let parts: Vec<&str> = s.splitn(2, '|').collect();
                if parts.len() == 2 {
                    Ok(Some(FingerprintedFileInfo {
                        fingerprint: parts[0].to_string(),
                        object_id: parts[1].to_string(),
                    }))
                } else {
                    Ok(None)
                }
            }
            None => Ok(None),
        }
    }

    /// Set fingerprinted file info.
    pub async fn set_fingerprinted_file_info(
        &self,
        filestore_id: DbId,
        path_id: DbId,
        info: &FingerprintedFileInfo,
    ) -> Result<()> {
        let key = file_info_key(filestore_id, path_id);
        let value = format!("{}|{}", info.fingerprint, info.object_id);
        let mut writes = self.db.write().await?;
        writes.set(key, value.into_bytes()).await;
        writes.flush().await
    }

    /// Flush any pending writes.
    pub async fn flush(&self) -> Result<()> {
        // The underlying CachingKeyValueDb handles periodic flushing
        // This is for explicit flush requests
        Ok(())
    }
}

// =============================================================================
// Key Construction Helpers
// =============================================================================

fn encode_uri_component(s: &str) -> String {
    percent_encode(s.as_bytes(), NON_ALPHANUMERIC).to_string()
}

fn repo_id_key(uuid: &str) -> Vec<u8> {
    let mut key = KEY_REPO_ID_PREFIX.to_vec();
    key.extend(encode_uri_component(uuid).as_bytes());
    key
}

fn exists_key(repo_id: DbId, object_id: &str) -> Vec<u8> {
    let mut key = KEY_EXISTS_PREFIX.to_vec();
    key.extend(repo_id.to_string().as_bytes());
    key.push(b'/');
    key.extend(object_id.as_bytes());
    key
}

fn fully_stored_key(repo_id: DbId, object_id: &str) -> Vec<u8> {
    let mut key = KEY_FULLY_STORED_PREFIX.to_vec();
    key.extend(repo_id.to_string().as_bytes());
    key.push(b'/');
    key.extend(object_id.as_bytes());
    key
}

fn filestore_id_key(url: &str) -> Vec<u8> {
    let mut key = KEY_FILESTORE_ID_PREFIX.to_vec();
    key.extend(encode_uri_component(url).as_bytes());
    key
}

fn name_id_key(filestore_id: DbId, name: &str) -> Vec<u8> {
    let mut key = KEY_NAME_ID_PREFIX.to_vec();
    key.extend(filestore_id.to_string().as_bytes());
    key.push(b'/');
    key.extend(encode_uri_component(name).as_bytes());
    key
}

fn path_entry_key(filestore_id: DbId, parent: Option<DbId>, name_id: DbId) -> Vec<u8> {
    let mut key = KEY_PATH_ENTRY_PREFIX.to_vec();
    key.extend(filestore_id.to_string().as_bytes());
    key.push(b'/');
    match parent {
        Some(id) => key.extend(id.to_string().as_bytes()),
        None => key.extend(b"root"),
    }
    key.push(b'/');
    key.extend(name_id.to_string().as_bytes());
    key
}

fn file_info_key(filestore_id: DbId, path_id: DbId) -> Vec<u8> {
    let mut key = KEY_FILE_INFO_PREFIX.to_vec();
    key.extend(filestore_id.to_string().as_bytes());
    key.push(b'/');
    key.extend(path_id.to_string().as_bytes());
    key
}

fn parse_u64(bytes: &[u8]) -> Result<u64> {
    let s = std::str::from_utf8(bytes).map_err(|e| KeyValueDbError::Encoding(e.to_string()))?;
    s.parse()
        .map_err(|e: std::num::ParseIntError| KeyValueDbError::Encoding(e.to_string()))
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

    fn create_test_db() -> (TempDir, CacheDb) {
        let temp_dir = TempDir::new().unwrap();
        let lmdb = Arc::new(LmdbKeyValueDb::new(temp_dir.path()).unwrap());
        let object_cache = Arc::new(NoopObjectCacheDb);
        let cache_db = CacheDb::new(lmdb, object_cache);
        (temp_dir, cache_db)
    }

    #[tokio::test]
    async fn test_id_generation() {
        let (_temp, db) = create_test_db();

        let id1 = db.generate_next_id().await.unwrap();
        let id2 = db.generate_next_id().await.unwrap();
        let id3 = db.generate_next_id().await.unwrap();

        assert_eq!(id1, 1);
        assert_eq!(id2, 2);
        assert_eq!(id3, 3);
    }

    #[tokio::test]
    async fn test_repository_id() {
        let (_temp, db) = create_test_db();

        let uuid = "test-uuid-12345";

        // Initially not set
        assert!(db.get_repository_id(uuid).await.unwrap().is_none());

        // Get or create
        let id1 = db.get_or_create_repository_id(uuid).await.unwrap();
        assert!(id1 > 0);

        // Should return same ID
        let id2 = db.get_or_create_repository_id(uuid).await.unwrap();
        assert_eq!(id1, id2);
    }

    #[tokio::test]
    async fn test_exists_and_fully_stored() {
        let (_temp, db) = create_test_db();

        let repo_id = db.get_or_create_repository_id("test-repo").await.unwrap();
        let object_id = "abc123def456";

        // Initially not set
        assert!(!db.get_exists(repo_id, object_id).await.unwrap());
        assert!(!db.get_fully_stored(repo_id, object_id).await.unwrap());

        // Set exists
        db.set_exists(repo_id, object_id).await.unwrap();
        assert!(db.get_exists(repo_id, object_id).await.unwrap());
        assert!(!db.get_fully_stored(repo_id, object_id).await.unwrap());

        // Set fully stored
        db.set_fully_stored(repo_id, object_id).await.unwrap();
        assert!(db.get_fully_stored(repo_id, object_id).await.unwrap());
    }

    #[tokio::test]
    async fn test_path_id() {
        let (_temp, db) = create_test_db();

        let fs_id = db.get_or_create_filestore_id("file:///test").await.unwrap();

        let path1 = db
            .get_path_id(fs_id, "foo/bar/baz.txt")
            .await
            .unwrap()
            .unwrap();
        let path2 = db
            .get_path_id(fs_id, "foo/bar/baz.txt")
            .await
            .unwrap()
            .unwrap();

        // Same path should get same ID
        assert_eq!(path1, path2);

        // Different path should get different ID
        let path3 = db
            .get_path_id(fs_id, "foo/bar/other.txt")
            .await
            .unwrap()
            .unwrap();
        assert_ne!(path1, path3);

        // Empty/root paths should return None
        assert!(db.get_path_id(fs_id, "").await.unwrap().is_none());
        assert!(db.get_path_id(fs_id, "/").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_fingerprinted_file_info() {
        let (_temp, db) = create_test_db();

        let fs_id = db.get_or_create_filestore_id("file:///test").await.unwrap();
        let path_id = db.get_path_id(fs_id, "test.txt").await.unwrap().unwrap();

        // Initially not set
        assert!(
            db.get_fingerprinted_file_info(fs_id, path_id)
                .await
                .unwrap()
                .is_none()
        );

        // Set it
        let info = FingerprintedFileInfo {
            fingerprint: "12345:100:-".to_string(),
            object_id: "objectid123".to_string(),
        };
        db.set_fingerprinted_file_info(fs_id, path_id, &info)
            .await
            .unwrap();

        // Get it back
        let retrieved = db
            .get_fingerprinted_file_info(fs_id, path_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(retrieved, info);
    }
}
