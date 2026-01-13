use std::future::Future;

use serde::{Deserialize, Serialize};

/// Object ID is a sha-256 hash represented as a lowercase hexadecimal string.
pub type ObjectId = String;

/// Information about a repository.
///
/// Contains metadata about the repository, including its unique identifier.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RepositoryInfo {
    /// The unique identifier for this repository, assigned at creation.
    pub uuid: String,
}

/// Error type for backend operations.
#[derive(Debug)]
pub enum BackendError {
    /// The object was not found.
    NotFound,
    /// An I/O error occurred.
    Io(std::io::Error),
    /// A custom error message.
    Other(String),
}

impl std::fmt::Display for BackendError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BackendError::NotFound => write!(f, "not found"),
            BackendError::Io(e) => write!(f, "I/O error: {}", e),
            BackendError::Other(msg) => write!(f, "{}", msg),
        }
    }
}

impl std::error::Error for BackendError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            BackendError::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<std::io::Error> for BackendError {
    fn from(e: std::io::Error) -> Self {
        BackendError::Io(e)
    }
}

pub type Result<T> = std::result::Result<T, BackendError>;

/// The result of a compare-and-swap operation on the current root.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SwapResult {
    /// The swap succeeded.
    Success,
    /// The swap failed because the current root did not match the expected value.
    /// Contains the actual current root (if any).
    Mismatch(Option<ObjectId>),
}

/// The primary backend interface for taterfs repositories.
///
/// All operations are asynchronous. Implementations store objects (identified by
/// their content hash) and track the current root object ID.
pub trait RepoBackend: Send + Sync {
    /// Get information about the repository.
    fn get_repository_info(&self) -> impl Future<Output = Result<RepositoryInfo>> + Send;

    /// Read the current root object ID, if one exists.
    fn read_current_root(&self) -> impl Future<Output = Result<Option<ObjectId>>> + Send;

    /// Write a new current root object ID.
    fn write_current_root(&self, root_id: &ObjectId) -> impl Future<Output = Result<()>> + Send;

    /// Atomically swap the current root if it matches the expected value.
    ///
    /// If the current root matches `expected` (where `None` means no root exists),
    /// update it to `new_root` and return `SwapResult::Success`.
    ///
    /// If the current root does not match, return `SwapResult::Mismatch` with
    /// the actual current root.
    ///
    /// Backends are not required to implement this transactionally, but are
    /// encouraged to if they can.
    fn swap_current_root(
        &self,
        expected: Option<&ObjectId>,
        new_root: &ObjectId,
    ) -> impl Future<Output = Result<SwapResult>> + Send;

    /// Check if an object with the given ID exists.
    fn object_exists(&self, id: &ObjectId) -> impl Future<Output = Result<bool>> + Send;

    /// Read an object's contents by ID.
    ///
    /// Returns `BackendError::NotFound` if the object does not exist.
    fn read_object(&self, id: &ObjectId) -> impl Future<Output = Result<Vec<u8>>> + Send;

    /// Write an object with the given ID and contents.
    ///
    /// It is optional for implementations to verify that the ID matches the
    /// sha-256 hash of the contents.
    fn write_object(&self, id: &ObjectId, data: &[u8]) -> impl Future<Output = Result<()>> + Send;
}
