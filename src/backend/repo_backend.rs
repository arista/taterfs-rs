use async_trait::async_trait;
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
    /// The repository is already initialized.
    AlreadyInitialized,
    /// An I/O error occurred.
    Io(std::io::Error),
    /// A custom error message.
    Other(String),
}

impl std::fmt::Display for BackendError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BackendError::NotFound => write!(f, "not found"),
            BackendError::AlreadyInitialized => write!(f, "repository already initialized"),
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
#[async_trait]
pub trait RepoBackend: Send + Sync {
    /// Check if repository info exists (i.e., if the repository is initialized).
    async fn has_repository_info(&self) -> Result<bool>;

    /// Set repository info during initialization.
    ///
    /// This should only be called once when the repository is first initialized.
    /// Returns `BackendError::AlreadyInitialized` if called on an already-initialized repository.
    async fn set_repository_info(&self, info: &RepositoryInfo) -> Result<()>;

    /// Get information about the repository.
    ///
    /// Returns `BackendError::NotFound` if the repository is not initialized.
    async fn get_repository_info(&self) -> Result<RepositoryInfo>;

    /// Read the current root object ID, if one exists.
    async fn read_current_root(&self) -> Result<Option<ObjectId>>;

    /// Write a new current root object ID.
    async fn write_current_root(&self, root_id: &ObjectId) -> Result<()>;

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
    async fn swap_current_root(
        &self,
        expected: Option<&ObjectId>,
        new_root: &ObjectId,
    ) -> Result<SwapResult>;

    /// Check if an object with the given ID exists.
    async fn object_exists(&self, id: &ObjectId) -> Result<bool>;

    /// Read an object's contents by ID.
    ///
    /// Returns `BackendError::NotFound` if the object does not exist.
    async fn read_object(&self, id: &ObjectId) -> Result<Vec<u8>>;

    /// Write an object with the given ID and contents.
    ///
    /// It is optional for implementations to verify that the ID matches the
    /// sha-256 hash of the contents.
    async fn write_object(&self, id: &ObjectId, data: &[u8]) -> Result<()>;
}
