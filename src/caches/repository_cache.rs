//! Repository cache trait for caching repository object metadata and content.

use std::future::Future;

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
pub trait RepositoryCache: Send + Sync {
    /// Check if an object exists in the repository.
    fn object_exists(&self, id: &ObjectId) -> impl Future<Output = Result<bool>> + Send;

    /// Mark an object as existing in the repository.
    fn set_object_exists(&self, id: &ObjectId) -> impl Future<Output = Result<()>> + Send;

    /// Check if an object and all objects reachable from it exist in the repository.
    fn object_fully_stored(&self, id: &ObjectId) -> impl Future<Output = Result<bool>> + Send;

    /// Mark an object (and implicitly all objects reachable from it) as fully stored.
    fn set_object_fully_stored(&self, id: &ObjectId) -> impl Future<Output = Result<()>> + Send;

    /// Retrieve a cached repository object.
    ///
    /// Returns `None` if the object is not in the cache.
    fn get_object(&self, id: &ObjectId) -> impl Future<Output = Result<Option<RepoObject>>> + Send;

    /// Store a repository object in the cache.
    fn set_object(
        &self,
        id: &ObjectId,
        obj: &RepoObject,
    ) -> impl Future<Output = Result<()>> + Send;
}
