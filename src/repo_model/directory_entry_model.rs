//! Directory entry model type.

use std::sync::Arc;

use crate::repo::Repo;
use crate::repository::ObjectId;

use super::DirectoryModel;

/// Model wrapper for a directory entry (subdirectory) in a directory.
///
/// Contains directory metadata and provides access to the directory model.
pub struct DirectoryEntryModel {
    repo: Arc<Repo>,
    /// Name of the directory.
    pub name: String,
    directory_id: ObjectId,
}

impl DirectoryEntryModel {
    /// Create a new DirectoryEntryModel.
    pub fn new(repo: Arc<Repo>, name: String, directory_id: ObjectId) -> Self {
        Self {
            repo,
            name,
            directory_id,
        }
    }

    /// Get the DirectoryModel for this directory entry.
    pub fn directory(&self) -> DirectoryModel {
        DirectoryModel::new(Arc::clone(&self.repo), self.directory_id.clone())
    }
}
