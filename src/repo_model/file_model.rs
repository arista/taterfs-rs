//! File model types.

use std::sync::Arc;

use crate::repo::Repo;
use crate::repository::{FileEntry, ObjectId};

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
/// Thin wrapper around a FileEntry that provides access to the file model.
pub struct FileEntryModel {
    repo: Arc<Repo>,
    entry: FileEntry,
}

impl FileEntryModel {
    /// Create a new FileEntryModel wrapping a FileEntry.
    pub fn new(repo: Arc<Repo>, entry: FileEntry) -> Self {
        Self { repo, entry }
    }

    /// Get the name of the file.
    pub fn name(&self) -> &str {
        &self.entry.name
    }

    /// Get the size of the file in bytes.
    pub fn size(&self) -> u64 {
        self.entry.size
    }

    /// Get whether the file is executable.
    pub fn executable(&self) -> bool {
        self.entry.executable
    }

    /// Get the FileModel for this file entry.
    pub fn file(&self) -> FileModel {
        FileModel::new(Arc::clone(&self.repo), self.entry.file.clone())
    }
}
