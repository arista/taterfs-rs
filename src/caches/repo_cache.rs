//! Repository cache trait for caching repository object metadata and content.

use std::sync::Arc;

use async_trait::async_trait;

use crate::repository::{ObjectId, RepoObject};

/// Error type for cache operations.
#[derive(Debug)]
pub enum CacheError {
    /// An I/O error occurred.
    Io(std::io::Error),
    /// A custom error message.
    Other(String),
}

impl std::fmt::Display for CacheError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CacheError::Io(e) => write!(f, "I/O error: {}", e),
            CacheError::Other(msg) => write!(f, "{}", msg),
        }
    }
}

impl std::error::Error for CacheError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            CacheError::Io(e) => Some(e),
            CacheError::Other(_) => None,
        }
    }
}

impl From<std::io::Error> for CacheError {
    fn from(e: std::io::Error) -> Self {
        CacheError::Io(e)
    }
}

/// Result type for cache operations.
pub type Result<T> = std::result::Result<T, CacheError>;

/// A cache for repository object metadata and content.
///
/// This trait provides caching for:
/// - Object existence: whether an object exists in the repository
/// - Object fully stored: whether an object and all objects reachable from it exist
/// - Object content: the deserialized repository object itself
#[async_trait]
pub trait RepoCache: Send + Sync {
    /// Check if an object exists in the repository.
    async fn object_exists(&self, id: &ObjectId) -> Result<bool>;

    /// Mark an object as existing in the repository.
    async fn set_object_exists(&self, id: &ObjectId) -> Result<()>;

    /// Check if an object and all objects reachable from it exist in the repository.
    async fn object_fully_stored(&self, id: &ObjectId) -> Result<bool>;

    /// Mark an object (and implicitly all objects reachable from it) as fully stored.
    async fn set_object_fully_stored(&self, id: &ObjectId) -> Result<()>;

    /// Retrieve a cached repository object.
    ///
    /// Returns `None` if the object is not in the cache.
    async fn get_object(&self, id: &ObjectId) -> Result<Option<RepoObject>>;

    /// Store a repository object in the cache.
    async fn set_object(&self, id: &ObjectId, obj: &RepoObject) -> Result<()>;
}

/// A provider of repository caches, keyed by repository UUID.
///
/// Implementations manage a collection of caches, typically one per repository.
#[async_trait]
pub trait RepoCaches: Send + Sync {
    /// Get or create a cache for the repository with the given UUID.
    async fn get_cache(&self, uuid: &str) -> std::result::Result<Arc<dyn RepoCache>, String>;
}

// =============================================================================
// NoopCache
// =============================================================================

/// A no-op cache implementation that never caches anything.
///
/// All reads return cache misses, all writes silently succeed without storing.
/// Use this when caching is disabled via configuration.
pub struct NoopCache;

#[async_trait]
impl RepoCache for NoopCache {
    async fn object_exists(&self, _id: &ObjectId) -> Result<bool> {
        Ok(false)
    }

    async fn set_object_exists(&self, _id: &ObjectId) -> Result<()> {
        Ok(())
    }

    async fn object_fully_stored(&self, _id: &ObjectId) -> Result<bool> {
        Ok(false)
    }

    async fn set_object_fully_stored(&self, _id: &ObjectId) -> Result<()> {
        Ok(())
    }

    async fn get_object(&self, _id: &ObjectId) -> Result<Option<RepoObject>> {
        Ok(None)
    }

    async fn set_object(&self, _id: &ObjectId, _obj: &RepoObject) -> Result<()> {
        Ok(())
    }
}

// =============================================================================
// NoopCaches
// =============================================================================

/// A no-op implementation of [`RepoCaches`] that always returns [`NoopCache`].
///
/// Use this as a placeholder when a real cache implementation is not available.
pub struct NoopCaches;

#[async_trait]
impl RepoCaches for NoopCaches {
    async fn get_cache(&self, _uuid: &str) -> std::result::Result<Arc<dyn RepoCache>, String> {
        Ok(Arc::new(NoopCache))
    }
}
