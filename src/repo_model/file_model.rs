//! File model types.

use std::sync::Arc;

use crate::repo::Repo;
use crate::repository::ObjectId;

/// Model wrapper for a file in the repository.
///
/// Provides access to file content through the repository.
pub struct FileModel {
    #[allow(dead_code)]
    repo: Arc<Repo>,
    /// The object ID of the file.
    pub id: ObjectId,
}

impl FileModel {
    /// Create a new FileModel with the given file ID.
    pub fn new(repo: Arc<Repo>, id: ObjectId) -> Self {
        Self { repo, id }
    }
}

/// Model wrapper for a file entry in a directory.
///
/// Contains file metadata and provides access to the file model.
pub struct FileEntryModel {
    repo: Arc<Repo>,
    /// Name of the file.
    pub name: String,
    /// Size of the file in bytes.
    pub size: u64,
    /// Whether the file is executable.
    pub executable: bool,
    file_id: ObjectId,
}

impl FileEntryModel {
    /// Create a new FileEntryModel.
    pub fn new(
        repo: Arc<Repo>,
        name: String,
        size: u64,
        executable: bool,
        file_id: ObjectId,
    ) -> Self {
        Self {
            repo,
            name,
            size,
            executable,
            file_id,
        }
    }

    /// Get the FileModel for this file entry.
    pub fn file(&self) -> FileModel {
        FileModel::new(Arc::clone(&self.repo), self.file_id.clone())
    }
}
