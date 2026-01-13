//! Repository interface providing caching, deduplication, and flow control.
//!
//! The [`Repo`] struct wraps a [`RepoBackend`] and adds:
//! - Caching via [`RepoCache`]
//! - Request deduplication for concurrent identical requests
//! - Flow control via [`CapacityManager`] instances

use bytes::Bytes;
use sha2::{Digest, Sha256};
use std::sync::Arc;

use crate::backend::{BackendError, RepoBackend};
use crate::caches::RepoCache;
use crate::repository::{
    from_json, to_canonical_json, Branches, Commit, Directory, File, JsonError, ObjectId,
    RepoObject, Root,
};
use crate::util::{CapacityManager, Dedup, UsedCapacity};

// =============================================================================
// Error Types
// =============================================================================

/// Error type for repository operations.
#[derive(Debug, Clone)]
pub enum RepoError {
    /// The object was not found.
    NotFound,
    /// An I/O error occurred.
    Io(String),
    /// JSON serialization/deserialization error.
    Json(String),
    /// Object type mismatch.
    TypeMismatch {
        expected: &'static str,
        actual: &'static str,
    },
    /// A custom error message.
    Other(String),
}

impl std::fmt::Display for RepoError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RepoError::NotFound => write!(f, "not found"),
            RepoError::Io(msg) => write!(f, "I/O error: {}", msg),
            RepoError::Json(msg) => write!(f, "JSON error: {}", msg),
            RepoError::TypeMismatch { expected, actual } => {
                write!(f, "type mismatch: expected {}, got {}", expected, actual)
            }
            RepoError::Other(msg) => write!(f, "{}", msg),
        }
    }
}

impl std::error::Error for RepoError {}

impl From<BackendError> for RepoError {
    fn from(e: BackendError) -> Self {
        match e {
            BackendError::NotFound => RepoError::NotFound,
            BackendError::Io(io_err) => RepoError::Io(io_err.to_string()),
            BackendError::Other(msg) => RepoError::Other(msg),
        }
    }
}

impl From<JsonError> for RepoError {
    fn from(e: JsonError) -> Self {
        RepoError::Json(e.to_string())
    }
}

/// Result type for repository operations.
pub type Result<T> = std::result::Result<T, RepoError>;

// =============================================================================
// Flow Control Configuration
// =============================================================================

/// Flow control configuration for a repository.
#[derive(Clone, Default)]
pub struct FlowControl {
    /// Limits the rate of requests (requests per time period).
    pub request_rate_limiter: Option<CapacityManager>,
    /// Limits concurrent in-flight requests.
    pub concurrent_request_limiter: Option<CapacityManager>,
    /// Limits read throughput (bytes per time period).
    pub read_throughput_limiter: Option<CapacityManager>,
    /// Limits write throughput (bytes per time period).
    pub write_throughput_limiter: Option<CapacityManager>,
    /// Limits total throughput (bytes per time period).
    pub total_throughput_limiter: Option<CapacityManager>,
}

// =============================================================================
// Repo
// =============================================================================

/// A repository providing caching, deduplication, and flow control over a backend.
///
/// `Repo` wraps a [`RepoBackend`] and [`RepoCache`] to provide a higher-level
/// interface with:
/// - **Caching**: Consults the cache before hitting the backend
/// - **Deduplication**: Combines concurrent identical requests
/// - **Flow control**: Respects capacity limits for requests and throughput
pub struct Repo<B, C> {
    backend: Arc<B>,
    cache: Arc<C>,
    flow_control: FlowControl,

    // Deduplication for various operations
    dedup_object_exists: Dedup<ObjectId, bool, RepoError>,
    dedup_read: Dedup<ObjectId, Bytes, RepoError>,
}

impl<B, C> Repo<B, C>
where
    B: RepoBackend + 'static,
    C: RepoCache + 'static,
{
    /// Create a new repository with the given backend and cache.
    pub fn new(backend: B, cache: C) -> Self {
        Self {
            backend: Arc::new(backend),
            cache: Arc::new(cache),
            flow_control: FlowControl::default(),
            dedup_object_exists: Dedup::new(),
            dedup_read: Dedup::new(),
        }
    }

    /// Create a new repository with flow control configuration.
    pub fn with_flow_control(backend: B, cache: C, flow_control: FlowControl) -> Self {
        Self {
            backend: Arc::new(backend),
            cache: Arc::new(cache),
            flow_control,
            dedup_object_exists: Dedup::new(),
            dedup_read: Dedup::new(),
        }
    }

    // =========================================================================
    // Flow Control Helpers
    // =========================================================================

    /// Acquire capacity for a request (rate + concurrency).
    async fn acquire_request_capacity(&self) -> (Option<UsedCapacity>, Option<UsedCapacity>) {
        let rate = if let Some(ref limiter) = self.flow_control.request_rate_limiter {
            Some(limiter.use_capacity(1).await)
        } else {
            None
        };

        let concurrent = if let Some(ref limiter) = self.flow_control.concurrent_request_limiter {
            Some(limiter.use_capacity(1).await)
        } else {
            None
        };

        (rate, concurrent)
    }

    /// Acquire capacity for read throughput.
    #[allow(dead_code)]
    async fn acquire_read_throughput(&self, size: u64) -> (Option<UsedCapacity>, Option<UsedCapacity>) {
        let read = if let Some(ref limiter) = self.flow_control.read_throughput_limiter {
            Some(limiter.use_capacity(size).await)
        } else {
            None
        };

        let total = if let Some(ref limiter) = self.flow_control.total_throughput_limiter {
            Some(limiter.use_capacity(size).await)
        } else {
            None
        };

        (read, total)
    }

    /// Acquire capacity for write throughput.
    async fn acquire_write_throughput(&self, size: u64) -> (Option<UsedCapacity>, Option<UsedCapacity>) {
        let write = if let Some(ref limiter) = self.flow_control.write_throughput_limiter {
            Some(limiter.use_capacity(size).await)
        } else {
            None
        };

        let total = if let Some(ref limiter) = self.flow_control.total_throughput_limiter {
            Some(limiter.use_capacity(size).await)
        } else {
            None
        };

        (write, total)
    }

    // =========================================================================
    // Current Root Operations
    // =========================================================================

    /// Check if a current root exists.
    pub async fn current_root_exists(&self) -> Result<bool> {
        let (_rate, _concurrent) = self.acquire_request_capacity().await;
        let result = self.backend.read_current_root().await?;
        Ok(result.is_some())
    }

    /// Read the current root object ID.
    ///
    /// Returns an error if no root exists.
    pub async fn read_current_root(&self) -> Result<ObjectId> {
        let (_rate, _concurrent) = self.acquire_request_capacity().await;
        self.backend
            .read_current_root()
            .await?
            .ok_or(RepoError::NotFound)
    }

    /// Write a new current root object ID.
    pub async fn write_current_root(&self, root_id: &ObjectId) -> Result<()> {
        let (_rate, _concurrent) = self.acquire_request_capacity().await;
        self.backend.write_current_root(root_id).await?;
        Ok(())
    }

    // =========================================================================
    // Object Existence
    // =========================================================================

    /// Check if an object exists in the repository.
    ///
    /// This operation is deduplicated and cached.
    pub async fn object_exists(&self, id: &ObjectId) -> Result<bool> {
        // Check cache first
        if let Ok(true) = self.cache.object_exists(id).await {
            return Ok(true);
        }

        // Deduplicated backend call
        let backend = Arc::clone(&self.backend);
        let cache = Arc::clone(&self.cache);
        let flow_control = self.flow_control.clone();
        let id_owned = id.clone();

        self.dedup_object_exists
            .call(id.clone(), || async move {
                let (_rate, _concurrent) = {
                    let rate = if let Some(ref limiter) = flow_control.request_rate_limiter {
                        Some(limiter.use_capacity(1).await)
                    } else {
                        None
                    };
                    let concurrent =
                        if let Some(ref limiter) = flow_control.concurrent_request_limiter {
                            Some(limiter.use_capacity(1).await)
                        } else {
                            None
                        };
                    (rate, concurrent)
                };

                let exists = backend.object_exists(&id_owned).await?;

                // Update cache on success
                if exists {
                    let _ = cache.set_object_exists(&id_owned).await;
                }

                Ok(exists)
            })
            .await
    }

    // =========================================================================
    // Raw Read/Write
    // =========================================================================

    /// Write raw bytes to the repository.
    ///
    /// The object ID is the SHA-256 hash of the data.
    pub async fn write(&self, id: &ObjectId, data: Bytes) -> Result<()> {
        let size = data.len() as u64;

        let (_rate, _concurrent) = self.acquire_request_capacity().await;
        let (_write, _total) = self.acquire_write_throughput(size).await;

        self.backend.write_object(id, &data).await?;

        // Mark as existing in cache
        let _ = self.cache.set_object_exists(id).await;

        Ok(())
    }

    /// Read raw bytes from the repository.
    ///
    /// If `expected_size` is provided, throughput limiting happens before the read.
    /// Otherwise, it happens after the read completes.
    ///
    /// This operation is deduplicated.
    pub async fn read(&self, id: &ObjectId, expected_size: Option<u64>) -> Result<Bytes> {
        let backend = Arc::clone(&self.backend);
        let flow_control = self.flow_control.clone();
        let id_owned = id.clone();

        self.dedup_read
            .call(id.clone(), || async move {
                // Acquire request capacity
                let (_rate, _concurrent) = {
                    let rate = if let Some(ref limiter) = flow_control.request_rate_limiter {
                        Some(limiter.use_capacity(1).await)
                    } else {
                        None
                    };
                    let concurrent =
                        if let Some(ref limiter) = flow_control.concurrent_request_limiter {
                            Some(limiter.use_capacity(1).await)
                        } else {
                            None
                        };
                    (rate, concurrent)
                };

                // Acquire read throughput if size is known
                let (_pre_read, _pre_total) = if let Some(size) = expected_size {
                    let read = if let Some(ref limiter) = flow_control.read_throughput_limiter {
                        Some(limiter.use_capacity(size).await)
                    } else {
                        None
                    };
                    let total = if let Some(ref limiter) = flow_control.total_throughput_limiter {
                        Some(limiter.use_capacity(size).await)
                    } else {
                        None
                    };
                    (read, total)
                } else {
                    (None, None)
                };

                let data = backend.read_object(&id_owned).await?;
                let bytes = Bytes::from(data);

                // Acquire read throughput after if size was unknown
                if expected_size.is_none() {
                    let size = bytes.len() as u64;
                    if let Some(ref limiter) = flow_control.read_throughput_limiter {
                        let _ = limiter.use_capacity(size).await;
                    }
                    if let Some(ref limiter) = flow_control.total_throughput_limiter {
                        let _ = limiter.use_capacity(size).await;
                    }
                }

                Ok(bytes)
            })
            .await
    }

    // =========================================================================
    // Object Read/Write
    // =========================================================================

    /// Write a repository object and return its ID.
    ///
    /// The object is serialized to canonical JSON and its SHA-256 hash becomes the ID.
    pub async fn write_object(&self, obj: &RepoObject) -> Result<ObjectId> {
        let json = to_canonical_json(obj)?;
        let id = compute_object_id(&json);

        self.write(&id, Bytes::from(json)).await?;

        // Cache the object
        let _ = self.cache.set_object(&id, obj).await;

        Ok(id)
    }

    /// Read and parse a repository object.
    ///
    /// This operation checks the cache first.
    pub async fn read_object(&self, id: &ObjectId) -> Result<RepoObject> {
        // Check cache first
        if let Ok(Some(obj)) = self.cache.get_object(id).await {
            return Ok(obj);
        }

        // Read from backend
        let bytes = self.read(id, None).await?;
        let obj: RepoObject = from_json(&bytes)?;

        // Cache the result
        let _ = self.cache.set_object(id, &obj).await;

        Ok(obj)
    }

    // =========================================================================
    // Typed Object Readers
    // =========================================================================

    /// Read and parse a Root object.
    pub async fn read_root(&self, id: &ObjectId) -> Result<Root> {
        let obj = self.read_object(id).await?;
        match obj {
            RepoObject::Root(root) => Ok(root),
            other => Err(RepoError::TypeMismatch {
                expected: "Root",
                actual: other.type_name(),
            }),
        }
    }

    /// Read and parse a Branches object.
    pub async fn read_branches(&self, id: &ObjectId) -> Result<Branches> {
        let obj = self.read_object(id).await?;
        match obj {
            RepoObject::Branches(branches) => Ok(branches),
            other => Err(RepoError::TypeMismatch {
                expected: "Branches",
                actual: other.type_name(),
            }),
        }
    }

    /// Read and parse a Commit object.
    pub async fn read_commit(&self, id: &ObjectId) -> Result<Commit> {
        let obj = self.read_object(id).await?;
        match obj {
            RepoObject::Commit(commit) => Ok(commit),
            other => Err(RepoError::TypeMismatch {
                expected: "Commit",
                actual: other.type_name(),
            }),
        }
    }

    /// Read and parse a Directory object.
    pub async fn read_directory(&self, id: &ObjectId) -> Result<Directory> {
        let obj = self.read_object(id).await?;
        match obj {
            RepoObject::Directory(dir) => Ok(dir),
            other => Err(RepoError::TypeMismatch {
                expected: "Directory",
                actual: other.type_name(),
            }),
        }
    }

    /// Read and parse a File object.
    pub async fn read_file(&self, id: &ObjectId) -> Result<File> {
        let obj = self.read_object(id).await?;
        match obj {
            RepoObject::File(file) => Ok(file),
            other => Err(RepoError::TypeMismatch {
                expected: "File",
                actual: other.type_name(),
            }),
        }
    }
}

// =============================================================================
// Helper Functions
// =============================================================================

/// Compute the object ID (SHA-256 hash) for the given data.
fn compute_object_id(data: &[u8]) -> ObjectId {
    let mut hasher = Sha256::new();
    hasher.update(data);
    let result = hasher.finalize();
    hex::encode(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::MemoryBackend;
    use crate::caches::CacheError;
    use std::collections::HashMap;
    use std::sync::Mutex;

    /// A simple in-memory cache for testing.
    struct TestCache {
        exists: Mutex<HashMap<ObjectId, bool>>,
        objects: Mutex<HashMap<ObjectId, RepoObject>>,
    }

    impl TestCache {
        fn new() -> Self {
            Self {
                exists: Mutex::new(HashMap::new()),
                objects: Mutex::new(HashMap::new()),
            }
        }
    }

    impl RepoCache for TestCache {
        async fn object_exists(&self, id: &ObjectId) -> std::result::Result<bool, CacheError> {
            Ok(*self.exists.lock().unwrap().get(id).unwrap_or(&false))
        }

        async fn set_object_exists(&self, id: &ObjectId) -> std::result::Result<(), CacheError> {
            self.exists.lock().unwrap().insert(id.clone(), true);
            Ok(())
        }

        async fn object_fully_stored(
            &self,
            _id: &ObjectId,
        ) -> std::result::Result<bool, CacheError> {
            Ok(false)
        }

        async fn set_object_fully_stored(
            &self,
            _id: &ObjectId,
        ) -> std::result::Result<(), CacheError> {
            Ok(())
        }

        async fn get_object(
            &self,
            id: &ObjectId,
        ) -> std::result::Result<Option<RepoObject>, CacheError> {
            Ok(self.objects.lock().unwrap().get(id).cloned())
        }

        async fn set_object(
            &self,
            id: &ObjectId,
            obj: &RepoObject,
        ) -> std::result::Result<(), CacheError> {
            self.objects.lock().unwrap().insert(id.clone(), obj.clone());
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_write_and_read_object() {
        let backend = MemoryBackend::new();
        let cache = TestCache::new();
        let repo = Repo::new(backend, cache);

        let commit = crate::repository::Commit {
            type_tag: crate::repository::CommitType::Commit,
            directory: "abc123".to_string(),
            parents: vec![],
            metadata: None,
        };

        let obj = RepoObject::Commit(commit.clone());
        let id = repo.write_object(&obj).await.unwrap();

        // Read it back
        let read_obj = repo.read_object(&id).await.unwrap();
        assert_eq!(read_obj, obj);

        // Read as typed
        let read_commit = repo.read_commit(&id).await.unwrap();
        assert_eq!(read_commit, commit);
    }

    #[tokio::test]
    async fn test_object_exists() {
        let backend = MemoryBackend::new();
        let cache = TestCache::new();
        let repo = Repo::new(backend, cache);

        let commit = RepoObject::Commit(crate::repository::Commit {
            type_tag: crate::repository::CommitType::Commit,
            directory: "abc123".to_string(),
            parents: vec![],
            metadata: None,
        });

        // Object doesn't exist yet
        let id = "nonexistent".to_string();
        assert!(!repo.object_exists(&id).await.unwrap());

        // Write and check again
        let id = repo.write_object(&commit).await.unwrap();
        assert!(repo.object_exists(&id).await.unwrap());
    }

    #[tokio::test]
    async fn test_type_mismatch_error() {
        let backend = MemoryBackend::new();
        let cache = TestCache::new();
        let repo = Repo::new(backend, cache);

        let commit = RepoObject::Commit(crate::repository::Commit {
            type_tag: crate::repository::CommitType::Commit,
            directory: "abc123".to_string(),
            parents: vec![],
            metadata: None,
        });

        let id = repo.write_object(&commit).await.unwrap();

        // Try to read as wrong type
        let result = repo.read_root(&id).await;
        assert!(matches!(
            result,
            Err(RepoError::TypeMismatch {
                expected: "Root",
                actual: "Commit"
            })
        ));
    }

    #[tokio::test]
    async fn test_current_root_operations() {
        let backend = MemoryBackend::new();
        let cache = TestCache::new();
        let repo = Repo::new(backend, cache);

        // No root initially
        assert!(!repo.current_root_exists().await.unwrap());
        assert!(repo.read_current_root().await.is_err());

        // Write a root
        let root_id = "root123".to_string();
        repo.write_current_root(&root_id).await.unwrap();

        // Now it exists
        assert!(repo.current_root_exists().await.unwrap());
        assert_eq!(repo.read_current_root().await.unwrap(), root_id);
    }
}
