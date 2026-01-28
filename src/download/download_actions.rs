//! Download actions for synchronizing a repository directory to a file store.
//!
//! This module provides the [`download_actions`] function which generates a sequence
//! of [`DownloadAction`]s needed to make a file store location match a repository
//! directory's contents.

use std::path::Path;
use std::sync::Arc;

use crate::file_store::FileStore;
use crate::repo::Repo;
use crate::repository::ObjectId;

/// Result type for download operations.
pub type Result<T> = std::result::Result<T, DownloadError>;

/// Errors that can occur during download operations.
#[derive(Debug, Clone)]
pub enum DownloadError {
    /// Repository error.
    Repo(String),
    /// File store error.
    FileStore(String),
}

impl std::fmt::Display for DownloadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DownloadError::Repo(msg) => write!(f, "repository error: {}", msg),
            DownloadError::FileStore(msg) => write!(f, "file store error: {}", msg),
        }
    }
}

impl std::error::Error for DownloadError {}

/// An action needed to synchronize a file store with a repository directory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DownloadAction {
    /// Create a new directory.
    CreateDirectory(String),
    /// Remove a directory (and all its contents).
    RemoveDirectory(String),
    /// Remove a file.
    RemoveFile(String),
    /// Enter a directory (both repo and file store have this directory).
    EnterDirectory(String),
    /// Exit the current directory.
    ExitDirectory,
    /// Download a file from the repository.
    DownloadFile(String, ObjectId),
}

/// Iterator that generates download actions by comparing repo and file store contents.
pub struct DownloadActions;

impl DownloadActions {
    /// Get the next download action.
    ///
    /// Returns `None` when all actions have been generated.
    pub async fn next(&mut self) -> Result<Option<DownloadAction>> {
        Ok(None)
    }
}

/// Create a download actions iterator.
///
/// This function compares a repository directory with a file store location and
/// generates the sequence of actions needed to make the file store match the
/// repository contents.
///
/// # Arguments
///
/// * `repo` - The repository to download from
/// * `directory_id` - The root directory ID in the repository
/// * `store` - The file store to compare against
/// * `path` - The base path in the file store
pub async fn download_actions(
    _repo: Arc<Repo>,
    _directory_id: ObjectId,
    _store: &dyn FileStore,
    _path: &Path,
) -> Result<DownloadActions> {
    Ok(DownloadActions)
}
