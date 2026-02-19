//! Repository model type.

use std::sync::Arc;

use tokio::sync::OnceCell;

use crate::app::search_branches;
use crate::repo::{Repo, RepoError};
use crate::repository::Root;

use super::BranchModel;

/// High-level model for interacting with a repository.
///
/// Lazily fetches and caches the current Root object as needed.
pub struct RepoModel {
    repo: Arc<Repo>,
    root: OnceCell<Root>,
}

impl RepoModel {
    /// Create a new RepoModel wrapping the given repository.
    pub fn new(repo: Arc<Repo>) -> Self {
        Self {
            repo,
            root: OnceCell::new(),
        }
    }

    /// Get or fetch the current Root object.
    async fn get_root(&self) -> Result<&Root, RepoError> {
        self.root
            .get_or_try_init(|| async {
                let root_id = self.repo.read_current_root().await?;
                self.repo.read_root(&root_id).await
            })
            .await
    }

    /// Get the default branch model.
    pub async fn default_branch(&self) -> Result<BranchModel, RepoError> {
        let root = self.get_root().await?;
        let branch =
            search_branches(Arc::clone(&self.repo), &root.branches, &root.default_branch_name)
                .await?;
        match branch {
            Some(b) => Ok(BranchModel::new(Arc::clone(&self.repo), b)),
            None => Err(RepoError::Other(format!(
                "default branch '{}' not found",
                root.default_branch_name
            ))),
        }
    }

    /// Get a branch by name.
    ///
    /// Performs a binary search through the branches list.
    /// Returns `None` if the branch is not found.
    pub async fn get_branch(&self, name: &str) -> Result<Option<BranchModel>, RepoError> {
        let root = self.get_root().await?;
        let branch = search_branches(Arc::clone(&self.repo), &root.branches, name).await?;
        Ok(branch.map(|b| BranchModel::new(Arc::clone(&self.repo), b)))
    }
}
