//! Error types for merge operations.

use crate::app::UploadError;
use crate::repo::RepoError;

// =============================================================================
// Error Types
// =============================================================================

/// Error type for merge operations.
#[derive(Debug)]
pub enum MergeError {
    /// Repository error during merge.
    Repo(RepoError),
    /// Both base entries were None, which is invalid state for to_dir_change.
    InvalidBaseState { name: String },
    /// Upload error during file merge.
    Upload(UploadError),
}

impl std::fmt::Display for MergeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MergeError::Repo(e) => write!(f, "repository error: {}", e),
            MergeError::InvalidBaseState { name } => {
                write!(
                    f,
                    "invalid base state: both entries are None for '{}'",
                    name
                )
            }
            MergeError::Upload(e) => write!(f, "upload error: {}", e),
        }
    }
}

impl std::error::Error for MergeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            MergeError::Repo(e) => Some(e),
            MergeError::InvalidBaseState { .. } => None,
            MergeError::Upload(e) => Some(e),
        }
    }
}

impl From<RepoError> for MergeError {
    fn from(e: RepoError) -> Self {
        MergeError::Repo(e)
    }
}

impl From<UploadError> for MergeError {
    fn from(e: UploadError) -> Self {
        MergeError::Upload(e)
    }
}

/// Result type for merge operations.
pub type Result<T> = std::result::Result<T, MergeError>;
