//! Repository interface providing caching, deduplication, and flow control.
//!
//! The [`Repo`] struct wraps a [`RepoBackend`] and adds:
//! - Caching via [`RepoCache`]
//! - Request deduplication for concurrent identical requests
//! - Flow control via [`CapacityManager`] instances

use sha2::{Digest, Sha256};
use std::sync::Arc;

use chrono::Utc;

use crate::backend::{BackendError, RepoBackend, RepositoryInfo};
use crate::caches::RepoCache;
use crate::repository::{
    Branch, BranchListEntry, Branches, BranchesType, Commit, CommitMetadata, CommitType,
    Directory, DirectoryType, File, JsonError, ObjectId, RepoObject, Root, RootType, from_json,
    to_canonical_json,
};
use crate::util::{
    CapacityManager, Complete, Completes, Dedup, ManagedBuffer, ManagedBuffers, NotifyComplete,
    UsedCapacity, WithComplete,
};

// =============================================================================
// Error Types
// =============================================================================

/// Error type for repository operations.
#[derive(Debug, Clone)]
pub enum RepoError {
    /// The object was not found.
    NotFound,
    /// The repository is already initialized.
    AlreadyInitialized,
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
            RepoError::AlreadyInitialized => write!(f, "repository already initialized"),
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
            BackendError::AlreadyInitialized => RepoError::AlreadyInitialized,
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
// Constants
// =============================================================================

/// For writes larger than this threshold, check if the object already exists
/// in the backend before writing. This avoids redundant writes for large objects.
pub const MAX_WRITE_WITHOUT_EXISTENCE_CHECK: u64 = 1024 * 1024; // 1 MB

// =============================================================================
// Initialization Configuration
// =============================================================================

/// Configuration for initializing a new repository.
pub struct RepoInitialize {
    /// Optional UUID for the repository. If not provided, one will be generated.
    pub uuid: Option<String>,
    /// Name of the default branch to create.
    pub default_branch_name: String,
}

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
    /// Manages memory buffers for read/write operations.
    pub managed_buffers: Option<ManagedBuffers>,
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
pub struct Repo {
    backend: Arc<dyn RepoBackend>,
    cache: Arc<dyn RepoCache>,
    flow_control: FlowControl,

    // Deduplication for various operations
    dedup_current_root: Dedup<(), Option<ObjectId>, RepoError>,
    dedup_write_current_root: Dedup<ObjectId, (), RepoError>,
    dedup_object_exists: Dedup<ObjectId, bool, RepoError>,
    dedup_write: Dedup<ObjectId, (), RepoError>,
    dedup_read: Dedup<ObjectId, Arc<ManagedBuffer>, RepoError>,
}

impl Repo {
    /// Create a new repository with the given backend and cache.
    pub fn new<B, C>(backend: B, cache: C) -> Self
    where
        B: RepoBackend + 'static,
        C: RepoCache + 'static,
    {
        Self {
            backend: Arc::new(backend),
            cache: Arc::new(cache),
            flow_control: FlowControl::default(),
            dedup_current_root: Dedup::new(),
            dedup_write_current_root: Dedup::new(),
            dedup_object_exists: Dedup::new(),
            dedup_write: Dedup::new(),
            dedup_read: Dedup::new(),
        }
    }

    /// Create a new repository with flow control configuration.
    pub fn with_flow_control<B, C>(backend: B, cache: C, flow_control: FlowControl) -> Self
    where
        B: RepoBackend + 'static,
        C: RepoCache + 'static,
    {
        Self {
            backend: Arc::new(backend),
            cache: Arc::new(cache),
            flow_control,
            dedup_current_root: Dedup::new(),
            dedup_write_current_root: Dedup::new(),
            dedup_object_exists: Dedup::new(),
            dedup_write: Dedup::new(),
            dedup_read: Dedup::new(),
        }
    }

    /// Create a new repository from pre-wrapped trait objects.
    pub fn from_dyn(
        backend: Arc<dyn RepoBackend>,
        cache: Arc<dyn RepoCache>,
        flow_control: FlowControl,
    ) -> Self {
        Self {
            backend,
            cache,
            flow_control,
            dedup_current_root: Dedup::new(),
            dedup_write_current_root: Dedup::new(),
            dedup_object_exists: Dedup::new(),
            dedup_write: Dedup::new(),
            dedup_read: Dedup::new(),
        }
    }

    // =========================================================================
    // Accessors
    // =========================================================================

    /// Get a reference to the repository's cache.
    pub fn cache(&self) -> &Arc<dyn RepoCache> {
        &self.cache
    }

    // =========================================================================
    // Initialization
    // =========================================================================

    /// Check if the repository is initialized.
    ///
    /// A repository is considered initialized if it has repository info set.
    pub async fn is_initialized(&self) -> Result<bool> {
        Ok(self.backend.has_repository_info().await?)
    }

    /// Initialize a new repository with the given configuration.
    ///
    /// This method creates the initial repository structure:
    /// - An empty Directory
    /// - A Commit pointing to that directory with "Repo initialization" message
    /// - A Branch with the specified default branch name pointing to the commit
    /// - An empty Branches object for other branches
    /// - A Root pointing to all of the above
    ///
    /// Returns an error if the repository is already initialized.
    pub async fn initialize(&self, init: RepoInitialize) -> Result<()> {
        // Check if already initialized
        if self.backend.has_repository_info().await? {
            return Err(RepoError::AlreadyInitialized);
        }

        // Generate UUID if not provided
        let uuid = init.uuid.unwrap_or_else(generate_uuid);
        let timestamp = Utc::now().to_rfc3339();

        // Track all write completions
        let completes = Completes::new();

        // 1. Create and write an empty Directory
        let empty_directory = Directory {
            type_tag: DirectoryType::Directory,
            entries: vec![],
        };
        let directory_write = self
            .write_object(&RepoObject::Directory(empty_directory))
            .await?;
        let directory_id = directory_write.result.clone();
        completes.add(directory_write.complete).unwrap();

        // 2. Create and write a Commit
        let commit = Commit {
            type_tag: CommitType::Commit,
            directory: directory_id,
            parents: vec![],
            metadata: Some(CommitMetadata {
                timestamp: Some(timestamp.clone()),
                author: None,
                committer: None,
                message: Some("Repo initialization".to_string()),
            }),
        };
        let commit_write = self.write_object(&RepoObject::Commit(commit)).await?;
        let commit_id = commit_write.result.clone();
        completes.add(commit_write.complete).unwrap();

        // 3. Create the Branch (note: Branch is embedded in BranchListEntry, not written separately)
        let default_branch = Branch {
            name: init.default_branch_name.clone(),
            commit: commit_id,
        };

        // 4. Create and write an empty Branches object for other branches
        let empty_branches = Branches {
            type_tag: BranchesType::Branches,
            branches: vec![],
        };
        let other_branches_write = self
            .write_object(&RepoObject::Branches(empty_branches))
            .await?;
        let other_branches_id = other_branches_write.result.clone();
        completes.add(other_branches_write.complete).unwrap();

        // 5. Create the default branch as a Branches object containing just the default branch
        let default_branch_branches = Branches {
            type_tag: BranchesType::Branches,
            branches: vec![BranchListEntry::Branch(default_branch)],
        };
        let default_branch_write = self
            .write_object(&RepoObject::Branches(default_branch_branches))
            .await?;
        let default_branch_id = default_branch_write.result.clone();
        completes.add(default_branch_write.complete).unwrap();

        // 6. Create and write a Root
        let root = Root {
            type_tag: RootType::Root,
            timestamp: timestamp.clone(),
            default_branch_name: init.default_branch_name,
            default_branch: default_branch_id,
            other_branches: other_branches_id,
            previous_root: None,
        };
        let root_write = self.write_object(&RepoObject::Root(root)).await?;
        let root_id = root_write.result.clone();
        completes.add(root_write.complete).unwrap();

        // 7. Wait for all writes to complete
        completes.done();
        completes.complete().await.map_err(|e| {
            RepoError::Other(format!("failed to write initialization objects: {}", e))
        })?;

        // 8. Set the current root
        self.backend.write_current_root(&root_id).await?;

        // 9. Write the repository info
        let repo_info = RepositoryInfo { uuid };
        self.backend.set_repository_info(&repo_info).await?;

        Ok(())
    }

    /// Get repository information.
    ///
    /// Returns an error if the repository is not initialized.
    pub async fn get_repository_info(&self) -> Result<RepositoryInfo> {
        Ok(self.backend.get_repository_info().await?)
    }

    // =========================================================================
    // Current Root Operations
    // =========================================================================

    /// Internal: deduplicated read of current root.
    async fn read_current_root_internal(&self) -> Result<Option<ObjectId>> {
        let backend = Arc::clone(&self.backend);
        let flow_control = self.flow_control.clone();

        self.dedup_current_root
            .call((), || async move {
                let (_rate, _concurrent) = acquire_request_capacity(&flow_control).await;
                let result = backend.read_current_root().await?;
                Ok(result)
            })
            .await
    }

    /// Check if a current root exists.
    ///
    /// This operation is deduplicated with `read_current_root`.
    pub async fn current_root_exists(&self) -> Result<bool> {
        let result = self.read_current_root_internal().await?;
        Ok(result.is_some())
    }

    /// Read the current root object ID.
    ///
    /// Returns an error if no root exists.
    /// This operation is deduplicated with `current_root_exists`.
    pub async fn read_current_root(&self) -> Result<ObjectId> {
        self.read_current_root_internal()
            .await?
            .ok_or(RepoError::NotFound)
    }

    /// Write a new current root object ID.
    ///
    /// This operation is deduplicated by root ID.
    pub async fn write_current_root(&self, root_id: &ObjectId) -> Result<()> {
        let backend = Arc::clone(&self.backend);
        let flow_control = self.flow_control.clone();
        let root_id_owned = root_id.clone();

        self.dedup_write_current_root
            .call(root_id.clone(), || async move {
                let (_rate, _concurrent) = acquire_request_capacity(&flow_control).await;
                backend.write_current_root(&root_id_owned).await?;
                Ok(())
            })
            .await
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
                let (_rate, _concurrent) = acquire_request_capacity(&flow_control).await;
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
    /// This operation is deduplicated by object ID.
    ///
    /// Returns immediately with a [`WithComplete`] containing a completion handle.
    /// The actual write (including flow control) happens in the background.
    /// On completion, the reference to the ManagedBuffer is dropped, returning
    /// its capacity to the ManagedBuffers.
    ///
    /// Skips the write if the object already exists (checked via cache, or
    /// via backend for objects larger than [`MAX_WRITE_WITHOUT_EXISTENCE_CHECK`]).
    pub async fn write(&self, id: &ObjectId, data: Arc<ManagedBuffer>) -> Result<WithComplete<()>> {
        // Check cache first - skip write if already exists
        if let Ok(true) = self.cache.object_exists(id).await {
            let complete = Arc::new(NotifyComplete::new());
            complete.notify_complete();
            return Ok(WithComplete::new((), complete));
        }

        // For large objects, do an actual existence check before writing
        let size = data.size();
        if size > MAX_WRITE_WITHOUT_EXISTENCE_CHECK && self.object_exists(id).await? {
            let complete = Arc::new(NotifyComplete::new());
            complete.notify_complete();
            return Ok(WithComplete::new((), complete));
        }

        // Create completion handle
        let complete = Arc::new(NotifyComplete::new());
        let complete_for_task = Arc::clone(&complete);

        let backend = Arc::clone(&self.backend);
        let cache = Arc::clone(&self.cache);
        let flow_control = self.flow_control.clone();
        let id_owned = id.clone();
        let dedup = self.dedup_write.clone();

        // Spawn the write operation in the background
        tokio::spawn(async move {
            let result = dedup
                .call(id_owned.clone(), || {
                    let backend = Arc::clone(&backend);
                    let cache = Arc::clone(&cache);
                    let flow_control = flow_control.clone();
                    let id_owned = id_owned.clone();
                    let data = Arc::clone(&data);

                    async move {
                        let size = data.size();

                        let (_rate, _concurrent) = acquire_request_capacity(&flow_control).await;
                        let (_write, _total) = acquire_write_throughput(&flow_control, size).await;

                        backend.write_object(&id_owned, data.as_ref()).await?;

                        // Mark as existing in cache
                        let _ = cache.set_object_exists(&id_owned).await;

                        Ok(())
                    }
                })
                .await;

            // Signal completion with success or error
            // The ManagedBuffer (data) is dropped here, returning capacity
            match result {
                Ok(()) => complete_for_task.notify_complete(),
                Err(e) => complete_for_task.notify_error(e.to_string()),
            }
        });

        Ok(WithComplete::new((), complete as Arc<dyn Complete>))
    }

    /// Read raw bytes from the repository.
    ///
    /// If `expected_size` is provided, throughput limiting happens before the read.
    /// Otherwise, it happens after the read completes.
    ///
    /// Returns data wrapped in a [`ManagedBuffer`]. The ManagedBuffer is acquired
    /// after other flow control handles are dropped to avoid deadlock.
    ///
    /// This operation is deduplicated.
    pub async fn read(
        &self,
        id: &ObjectId,
        expected_size: Option<u64>,
    ) -> Result<Arc<ManagedBuffer>> {
        let backend = Arc::clone(&self.backend);
        let flow_control = self.flow_control.clone();
        let id_owned = id.clone();

        self.dedup_read
            .call(id.clone(), || async move {
                let (rate, concurrent) = acquire_request_capacity(&flow_control).await;

                // Acquire read throughput if size is known
                let (pre_read, pre_total) = if let Some(size) = expected_size {
                    acquire_read_throughput(&flow_control, size).await
                } else {
                    (None, None)
                };

                let data = backend.read_object(&id_owned).await?;
                let size = data.len() as u64;

                // Acquire read throughput after if size was unknown.
                if expected_size.is_none() {
                    let _ = acquire_read_throughput(&flow_control, size).await;
                }

                // Drop all flow control handles BEFORE acquiring ManagedBuffer
                // to avoid potential deadlock
                drop(rate);
                drop(concurrent);
                drop(pre_read);
                drop(pre_total);

                // Wrap data in ManagedBuffer
                let managed_buffer = if let Some(ref mb) = flow_control.managed_buffers {
                    mb.get_buffer_with_data(data).await
                } else {
                    // No capacity management - create unmanaged buffer
                    ManagedBuffers::new().get_buffer_with_data(data).await
                };

                Ok(Arc::new(managed_buffer))
            })
            .await
    }

    // =========================================================================
    // Object Read/Write
    // =========================================================================

    /// Write a repository object and return its ID.
    ///
    /// The object is serialized to canonical JSON and its SHA-256 hash becomes the ID.
    ///
    /// Returns immediately with a [`WithComplete`] containing the object ID and
    /// a completion handle. The actual write (including flow control) happens
    /// in the background.
    pub async fn write_object(&self, obj: &RepoObject) -> Result<WithComplete<ObjectId>> {
        let json = to_canonical_json(obj)?;
        let id = compute_object_id(&json);

        // Wrap JSON in a ManagedBuffer
        let managed_buffer = if let Some(ref mb) = self.flow_control.managed_buffers {
            mb.get_buffer_with_data(json).await
        } else {
            ManagedBuffers::new().get_buffer_with_data(json).await
        };

        let write_result = self.write(&id, Arc::new(managed_buffer)).await?;

        // Cache the object immediately (this is fast, in-memory)
        let _ = self.cache.set_object(&id, obj).await;

        Ok(WithComplete::new(id, write_result.complete))
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
        let buffer = self.read(id, None).await?;
        let obj: RepoObject = from_json(buffer.as_ref())?;

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

/// Acquire capacity for a request (rate + concurrency).
async fn acquire_request_capacity(
    flow_control: &FlowControl,
) -> (Option<UsedCapacity>, Option<UsedCapacity>) {
    let rate = if let Some(ref limiter) = flow_control.request_rate_limiter {
        Some(limiter.use_capacity(1).await)
    } else {
        None
    };

    let concurrent = if let Some(ref limiter) = flow_control.concurrent_request_limiter {
        Some(limiter.use_capacity(1).await)
    } else {
        None
    };

    (rate, concurrent)
}

/// Acquire capacity for read throughput.
async fn acquire_read_throughput(
    flow_control: &FlowControl,
    size: u64,
) -> (Option<UsedCapacity>, Option<UsedCapacity>) {
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
}

/// Acquire capacity for write throughput.
async fn acquire_write_throughput(
    flow_control: &FlowControl,
    size: u64,
) -> (Option<UsedCapacity>, Option<UsedCapacity>) {
    let write = if let Some(ref limiter) = flow_control.write_throughput_limiter {
        Some(limiter.use_capacity(size).await)
    } else {
        None
    };

    let total = if let Some(ref limiter) = flow_control.total_throughput_limiter {
        Some(limiter.use_capacity(size).await)
    } else {
        None
    };

    (write, total)
}

/// Compute the object ID (SHA-256 hash) for the given data.
fn compute_object_id(data: &[u8]) -> ObjectId {
    let mut hasher = Sha256::new();
    hasher.update(data);
    let result = hasher.finalize();
    hex::encode(result)
}

/// Generate a UUID for repository initialization.
fn generate_uuid() -> String {
    uuid::Uuid::new_v4().to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::MemoryBackend;
    use crate::caches::CacheError;
    use async_trait::async_trait;
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

    #[async_trait]
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
        let write_result = repo.write_object(&obj).await.unwrap();
        let id = write_result.result;

        // Wait for write to complete
        write_result.complete.complete().await.unwrap();

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
        let write_result = repo.write_object(&commit).await.unwrap();
        write_result.complete.complete().await.unwrap();
        assert!(repo.object_exists(&write_result.result).await.unwrap());
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

        let write_result = repo.write_object(&commit).await.unwrap();
        write_result.complete.complete().await.unwrap();
        let id = write_result.result;

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

    #[tokio::test]
    async fn test_is_initialized_uninitialized() {
        let backend = MemoryBackend::new();
        let cache = TestCache::new();
        let repo = Repo::new(backend, cache);

        // Uninitialized repo should return false
        assert!(!repo.is_initialized().await.unwrap());
    }

    #[tokio::test]
    async fn test_is_initialized_initialized() {
        let backend = MemoryBackend::new_initialized();
        let cache = TestCache::new();
        let repo = Repo::new(backend, cache);

        // Pre-initialized backend should return true
        assert!(repo.is_initialized().await.unwrap());
    }

    #[tokio::test]
    async fn test_initialize_creates_repo_structure() {
        let backend = MemoryBackend::new();
        let cache = TestCache::new();
        let repo = Repo::new(backend, cache);

        // Initialize with default branch name
        let init = RepoInitialize {
            uuid: Some("test-uuid-12345".to_string()),
            default_branch_name: "main".to_string(),
        };
        repo.initialize(init).await.unwrap();

        // Now it should be initialized
        assert!(repo.is_initialized().await.unwrap());

        // Should have a current root
        assert!(repo.current_root_exists().await.unwrap());
        let root_id = repo.read_current_root().await.unwrap();

        // Read and verify the root
        let root = repo.read_root(&root_id).await.unwrap();
        assert_eq!(root.default_branch_name, "main");

        // Get repository info
        let info = repo.get_repository_info().await.unwrap();
        assert_eq!(info.uuid, "test-uuid-12345");
    }

    #[tokio::test]
    async fn test_initialize_already_initialized_error() {
        let backend = MemoryBackend::new_initialized();
        let cache = TestCache::new();
        let repo = Repo::new(backend, cache);

        // Try to initialize an already initialized repo
        let init = RepoInitialize {
            uuid: None,
            default_branch_name: "main".to_string(),
        };
        let result = repo.initialize(init).await;

        assert!(matches!(result, Err(RepoError::AlreadyInitialized)));
    }

    #[tokio::test]
    async fn test_initialize_generates_uuid_if_not_provided() {
        let backend = MemoryBackend::new();
        let cache = TestCache::new();
        let repo = Repo::new(backend, cache);

        let init = RepoInitialize {
            uuid: None,
            default_branch_name: "main".to_string(),
        };
        repo.initialize(init).await.unwrap();

        // Should have generated a UUID
        let info = repo.get_repository_info().await.unwrap();
        assert!(!info.uuid.is_empty());
    }
}
