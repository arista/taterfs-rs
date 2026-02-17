//! Directory model and entry iteration types.

use std::sync::Arc;

use crate::app::search_directory;
use crate::repo::{DirectoryEntry, Repo, RepoError};
use crate::repository::ObjectId;

use super::{DirectoryEntryModel, FileEntryModel};

/// Model wrapper for a directory in the repository.
pub struct DirectoryModel {
    repo: Arc<Repo>,
    /// The object ID of the directory.
    pub id: ObjectId,
}

impl DirectoryModel {
    /// Create a new DirectoryModel with the given directory ID.
    pub fn new(repo: Arc<Repo>, id: ObjectId) -> Self {
        Self { repo, id }
    }

    /// Get an iterator over all entries in this directory.
    pub async fn entries(&self) -> Result<EntryModelList, RepoError> {
        EntryModelList::new(Arc::clone(&self.repo), &self.id).await
    }

    /// Find an entry by name using binary search.
    pub async fn find_entry(&self, name: &str) -> Result<Option<EntryModel>, RepoError> {
        let result = search_directory(Arc::clone(&self.repo), &self.id, name).await?;
        Ok(result.map(|entry| entry_model_from_directory_entry(Arc::clone(&self.repo), entry)))
    }
}

/// An entry in a directory - either a file or a subdirectory.
pub enum EntryModel {
    /// A file entry.
    File(FileEntryModel),
    /// A subdirectory entry.
    Directory(DirectoryEntryModel),
}

impl EntryModel {
    /// Get the name of this entry.
    pub fn name(&self) -> &str {
        match self {
            EntryModel::File(f) => &f.name,
            EntryModel::Directory(d) => &d.name,
        }
    }
}

/// Convert a DirectoryEntry to an EntryModel.
fn entry_model_from_directory_entry(repo: Arc<Repo>, entry: DirectoryEntry) -> EntryModel {
    match entry {
        DirectoryEntry::File(f) => EntryModel::File(FileEntryModel::new(
            repo,
            f.name,
            f.size,
            f.executable,
            f.file,
        )),
        DirectoryEntry::Directory(d) => {
            EntryModel::Directory(DirectoryEntryModel::new(repo, d.name, d.directory))
        }
    }
}

/// Async iterator over directory entries.
pub struct EntryModelList {
    repo: Arc<Repo>,
    inner: crate::repo::DirectoryEntryList,
}

impl EntryModelList {
    /// Create a new entry model list for a directory.
    async fn new(repo: Arc<Repo>, directory_id: &ObjectId) -> Result<Self, RepoError> {
        let inner = repo.list_directory_entries(directory_id).await?;
        Ok(Self { repo, inner })
    }

    /// Get the next entry in the directory.
    ///
    /// Returns `None` when all entries have been yielded.
    pub async fn next(&mut self) -> Result<Option<EntryModel>, RepoError> {
        match self.inner.next().await? {
            None => Ok(None),
            Some(entry) => Ok(Some(entry_model_from_directory_entry(
                Arc::clone(&self.repo),
                entry,
            ))),
        }
    }
}
