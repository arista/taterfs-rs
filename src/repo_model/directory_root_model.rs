//! Directory root model and path resolution types.

use std::sync::Arc;

use crate::app::search_directory;
use crate::repo::{DirectoryEntry, Repo, RepoError};
use crate::repository::ObjectId;

use super::{DirectoryEntryModel, DirectoryModel, FileEntryModel};

/// Result of resolving a path in the repository.
pub enum ResolvePathResult {
    /// Path component not found.
    None,
    /// Path resolved to the root (empty path or "/").
    Root,
    /// Path resolved to a directory.
    Directory(DirectoryEntryModel),
    /// Path resolved to a file.
    File(FileEntryModel),
}

/// Model for the root directory of a commit.
///
/// Provides methods for navigating the directory tree starting from the root.
pub struct DirectoryRootModel {
    repo: Arc<Repo>,
    directory_id: ObjectId,
}

impl DirectoryRootModel {
    /// Create a new DirectoryRootModel with the given root directory ID.
    pub fn new(repo: Arc<Repo>, directory_id: ObjectId) -> Self {
        Self { repo, directory_id }
    }

    /// Get the root directory model.
    pub fn directory(&self) -> DirectoryModel {
        DirectoryModel::new(Arc::clone(&self.repo), self.directory_id.clone())
    }

    /// Resolve a path starting from the root directory.
    ///
    /// Uses "/" as a path separator. Leading and trailing slashes are ignored.
    /// An empty path or "/" resolves to `ResolvePathResult::Root`.
    pub async fn resolve_path(&self, path: &str) -> Result<ResolvePathResult, RepoError> {
        let components: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();

        if components.is_empty() {
            return Ok(ResolvePathResult::Root);
        }

        let mut current_dir_id = self.directory_id.clone();

        for (i, component) in components.iter().enumerate() {
            let is_last = i == components.len() - 1;

            let entry =
                search_directory(Arc::clone(&self.repo), &current_dir_id, component).await?;

            match entry {
                None => return Ok(ResolvePathResult::None),
                Some(DirectoryEntry::File(f)) => {
                    if is_last {
                        return Ok(ResolvePathResult::File(FileEntryModel::new(
                            Arc::clone(&self.repo),
                            f.name,
                            f.size,
                            f.executable,
                            f.file,
                        )));
                    } else {
                        // Can't traverse into a file
                        return Ok(ResolvePathResult::None);
                    }
                }
                Some(DirectoryEntry::Directory(d)) => {
                    if is_last {
                        return Ok(ResolvePathResult::Directory(DirectoryEntryModel::new(
                            Arc::clone(&self.repo),
                            d.name,
                            d.directory.clone(),
                        )));
                    } else {
                        current_dir_id = d.directory;
                    }
                }
            }
        }

        // Should not reach here, but just in case
        Ok(ResolvePathResult::Root)
    }

    /// Resolve a path to a directory.
    ///
    /// Returns an error if the path does not resolve to a directory or the root.
    pub async fn resolve_path_to_directory(
        &self,
        path: &str,
    ) -> Result<DirectoryModel, ResolvePathError> {
        match self.resolve_path(path).await? {
            ResolvePathResult::Root => Ok(self.directory()),
            ResolvePathResult::Directory(d) => Ok(d.directory()),
            ResolvePathResult::File(_) => Err(ResolvePathError::NotADirectory(path.to_string())),
            ResolvePathResult::None => Err(ResolvePathError::NotFound(path.to_string())),
        }
    }
}

/// Error type for path resolution failures.
#[derive(Debug, thiserror::Error)]
pub enum ResolvePathError {
    /// Path was not found.
    #[error("path not found: {0}")]
    NotFound(String),
    /// Path resolved to a file, not a directory.
    #[error("path is not a directory: {0}")]
    NotADirectory(String),
    /// Repository error during resolution.
    #[error("repository error: {0}")]
    Repo(#[from] RepoError),
}
