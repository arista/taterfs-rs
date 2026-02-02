//! Object cache database for caching repository objects.
//!
//! Provides a separate caching mechanism for repository objects, stored as
//! JSON files on disk with an in-memory LRU cache layer.

use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use lru::LruCache;
use thiserror::Error;
use tokio::sync::Mutex;

use crate::repository::{RepoObject, to_canonical_json};

// =============================================================================
// Error Types
// =============================================================================

/// Errors that can occur during object cache operations.
#[derive(Debug, Error)]
pub enum ObjectCacheError {
    /// I/O error.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// JSON serialization/deserialization error.
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    /// Repository JSON error.
    #[error("Repository JSON error: {0}")]
    RepoJson(#[from] crate::repository::JsonError),
}

/// Result type for object cache operations.
pub type Result<T> = std::result::Result<T, ObjectCacheError>;

// =============================================================================
// ObjectCacheDb Trait
// =============================================================================

/// Interface for caching repository objects.
#[async_trait]
pub trait ObjectCacheDb: Send + Sync {
    /// Get a cached repository object.
    async fn get_object(&self, object_id: &str) -> Result<Option<RepoObject>>;

    /// Cache a repository object.
    async fn set_object(&self, object_id: &str, obj: &RepoObject) -> Result<()>;
}

// =============================================================================
// FsObjectCacheDb
// =============================================================================

/// Filesystem-based object cache that stores objects as JSON files.
///
/// Objects are stored at:
/// `{cache_dir}/cache_objects/{id[0..2]}/{id[2..4]}/{id[4..6]}/{id}.json`
///
/// Writes go to a temporary directory first, then are moved atomically.
pub struct FsObjectCacheDb {
    /// Base directory for the cache.
    cache_dir: PathBuf,
}

impl FsObjectCacheDb {
    /// Create a new filesystem object cache.
    pub fn new(cache_dir: impl Into<PathBuf>) -> Self {
        Self {
            cache_dir: cache_dir.into(),
        }
    }

    /// Get the path for an object.
    fn object_path(&self, object_id: &str) -> PathBuf {
        // Ensure object_id is long enough for the path structure
        let id = if object_id.len() >= 6 {
            object_id
        } else {
            // Pad short IDs (shouldn't happen in practice)
            return self
                .cache_dir
                .join("cache_objects")
                .join("00")
                .join("00")
                .join("00")
                .join(format!("{}.json", object_id));
        };

        self.cache_dir
            .join("cache_objects")
            .join(&id[0..2])
            .join(&id[2..4])
            .join(&id[4..6])
            .join(format!("{}.json", object_id))
    }

    /// Get the temporary directory for writes.
    fn temp_dir(&self) -> PathBuf {
        self.cache_dir.join(".cache_objects_tmp")
    }
}

#[async_trait]
impl ObjectCacheDb for FsObjectCacheDb {
    async fn get_object(&self, object_id: &str) -> Result<Option<RepoObject>> {
        let path = self.object_path(object_id);

        match tokio::fs::read_to_string(&path).await {
            Ok(json) => {
                let obj: RepoObject = serde_json::from_str(&json)?;
                Ok(Some(obj))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    async fn set_object(&self, object_id: &str, obj: &RepoObject) -> Result<()> {
        let final_path = self.object_path(object_id);
        let temp_dir = self.temp_dir();

        // Ensure directories exist
        if let Some(parent) = final_path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        tokio::fs::create_dir_all(&temp_dir).await?;

        // Serialize to canonical JSON
        let json = to_canonical_json(obj)?;

        // Write to temp file
        let temp_path = temp_dir.join(format!("{}.json.tmp", object_id));
        tokio::fs::write(&temp_path, &json).await?;

        // Move to final location (atomic on same filesystem)
        tokio::fs::rename(&temp_path, &final_path).await?;

        Ok(())
    }
}

// =============================================================================
// CachingObjectCacheDb
// =============================================================================

/// In-memory LRU cache wrapper for ObjectCacheDb.
///
/// Caches objects in memory with a maximum size limit. Size is computed as
/// the serialized JSON length plus the object ID length.
pub struct CachingObjectCacheDb {
    inner: Arc<dyn ObjectCacheDb>,
    cache: Mutex<LruCache<String, CachedObject>>,
    max_size: usize,
    current_size: Mutex<usize>,
}

struct CachedObject {
    obj: RepoObject,
    size: usize,
}

impl CachingObjectCacheDb {
    /// Create a new caching wrapper.
    ///
    /// `max_size` is the maximum total memory size for cached objects.
    pub fn new(inner: Arc<dyn ObjectCacheDb>, max_size: usize) -> Self {
        // Use a large capacity since we manage size ourselves
        let cache = LruCache::unbounded();
        Self {
            inner,
            cache: Mutex::new(cache),
            max_size,
            current_size: Mutex::new(0),
        }
    }

    /// Compute the memory size of a cached object.
    fn compute_size(object_id: &str, obj: &RepoObject) -> usize {
        // Size = serialized length + object ID length
        let json_size = serde_json::to_string(obj).map(|s| s.len()).unwrap_or(0);
        json_size + object_id.len()
    }

    /// Evict entries until we're under the max size.
    async fn evict_if_needed(&self, new_entry_size: usize) {
        let mut cache = self.cache.lock().await;
        let mut current_size = self.current_size.lock().await;

        // Evict LRU entries until we have room
        while *current_size + new_entry_size > self.max_size && !cache.is_empty() {
            if let Some((_, evicted)) = cache.pop_lru() {
                *current_size = current_size.saturating_sub(evicted.size);
            }
        }
    }
}

#[async_trait]
impl ObjectCacheDb for CachingObjectCacheDb {
    async fn get_object(&self, object_id: &str) -> Result<Option<RepoObject>> {
        // Check cache first
        {
            let mut cache = self.cache.lock().await;
            if let Some(cached) = cache.get(object_id) {
                return Ok(Some(cached.obj.clone()));
            }
        }

        // Not in cache, try underlying storage
        let obj = self.inner.get_object(object_id).await?;

        // Cache the result if found
        if let Some(ref obj) = obj {
            let size = Self::compute_size(object_id, obj);

            // Only cache if it fits in our size limit
            if size <= self.max_size {
                self.evict_if_needed(size).await;

                let mut cache = self.cache.lock().await;
                let mut current_size = self.current_size.lock().await;

                cache.put(
                    object_id.to_string(),
                    CachedObject {
                        obj: obj.clone(),
                        size,
                    },
                );
                *current_size += size;
            }
        }

        Ok(obj)
    }

    async fn set_object(&self, object_id: &str, obj: &RepoObject) -> Result<()> {
        // Write to underlying storage first
        self.inner.set_object(object_id, obj).await?;

        // Then update cache
        let size = Self::compute_size(object_id, obj);

        if size <= self.max_size {
            self.evict_if_needed(size).await;

            let mut cache = self.cache.lock().await;
            let mut current_size = self.current_size.lock().await;

            // Remove old entry if exists
            if let Some(old) = cache.pop(object_id) {
                *current_size = current_size.saturating_sub(old.size);
            }

            cache.put(
                object_id.to_string(),
                CachedObject {
                    obj: obj.clone(),
                    size,
                },
            );
            *current_size += size;
        }

        Ok(())
    }
}

// =============================================================================
// NoopObjectCacheDb
// =============================================================================

/// A no-op object cache that doesn't cache anything.
pub struct NoopObjectCacheDb;

#[async_trait]
impl ObjectCacheDb for NoopObjectCacheDb {
    async fn get_object(&self, _object_id: &str) -> Result<Option<RepoObject>> {
        Ok(None)
    }

    async fn set_object(&self, _object_id: &str, _obj: &RepoObject) -> Result<()> {
        Ok(())
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::repository::{Commit, CommitMetadata, CommitType};
    use tempfile::TempDir;

    fn test_commit() -> RepoObject {
        RepoObject::Commit(Commit {
            type_tag: CommitType::Commit,
            metadata: Some(CommitMetadata {
                message: Some("test commit".to_string()),
                author: Some("test".to_string()),
                committer: None,
                timestamp: Some("2024-01-01T00:00:00Z".to_string()),
            }),
            directory: "abc123def456".to_string(),
            parents: vec![],
        })
    }

    #[tokio::test]
    async fn test_fs_object_cache_roundtrip() {
        let temp_dir = TempDir::new().unwrap();
        let cache = FsObjectCacheDb::new(temp_dir.path());

        let object_id = "abc123def456789012345678901234567890123456789012345678901234";
        let obj = test_commit();

        // Initially not found
        assert!(cache.get_object(object_id).await.unwrap().is_none());

        // Set and get
        cache.set_object(object_id, &obj).await.unwrap();
        let retrieved = cache.get_object(object_id).await.unwrap().unwrap();

        // Compare by re-serializing (RepoObject doesn't derive PartialEq)
        assert_eq!(
            serde_json::to_string(&retrieved).unwrap(),
            serde_json::to_string(&obj).unwrap()
        );
    }

    #[tokio::test]
    async fn test_caching_object_cache() {
        let temp_dir = TempDir::new().unwrap();
        let fs_cache = Arc::new(FsObjectCacheDb::new(temp_dir.path()));
        let cache = CachingObjectCacheDb::new(fs_cache, 1024 * 1024); // 1MB

        let object_id = "abc123def456789012345678901234567890123456789012345678901234";
        let obj = test_commit();

        // Set object
        cache.set_object(object_id, &obj).await.unwrap();

        // Get should hit cache
        let retrieved = cache.get_object(object_id).await.unwrap().unwrap();
        assert_eq!(
            serde_json::to_string(&retrieved).unwrap(),
            serde_json::to_string(&obj).unwrap()
        );
    }

    #[tokio::test]
    async fn test_object_path_structure() {
        let cache = FsObjectCacheDb::new("/tmp/cache");
        let path = cache.object_path("abc123def456");

        assert!(path.to_string_lossy().contains("cache_objects"));
        assert!(path.to_string_lossy().contains("ab"));
        assert!(path.to_string_lossy().contains("c1"));
        assert!(path.to_string_lossy().contains("23"));
        assert!(path.to_string_lossy().ends_with("abc123def456.json"));
    }
}
