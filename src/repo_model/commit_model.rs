//! Commit model type.

use std::sync::Arc;

use tokio::sync::OnceCell;

use crate::repo::{Repo, RepoError};
use crate::repository::{Commit, ObjectId};

use super::DirectoryRootModel;

/// Model wrapper for a commit in the repository.
///
/// Lazily fetches and caches the backend Commit object as needed.
pub struct CommitModel {
    repo: Arc<Repo>,
    /// The object ID of the commit.
    pub id: ObjectId,
    commit: OnceCell<Commit>,
}

impl CommitModel {
    /// Create a new CommitModel with the given commit ID.
    pub fn new(repo: Arc<Repo>, id: ObjectId) -> Self {
        Self {
            repo,
            id,
            commit: OnceCell::new(),
        }
    }

    /// Get or fetch the underlying Commit object.
    async fn get_commit(&self) -> Result<&Commit, RepoError> {
        self.commit
            .get_or_try_init(|| async { self.repo.read_commit(&self.id).await })
            .await
    }

    /// Get the root directory model for this commit.
    pub async fn root(&self) -> Result<DirectoryRootModel, RepoError> {
        let commit = self.get_commit().await?;
        Ok(DirectoryRootModel::new(
            Arc::clone(&self.repo),
            commit.directory.clone(),
        ))
    }
}
