//! Repository model type.

use std::future::Future;
use std::sync::Arc;

use chrono::Utc;
use tokio::sync::OnceCell;

use crate::app::{
    modify_branches, search_branches, BranchListModifications, ListModifications,
};
use crate::repo::{Repo, RepoError, SwapResult};
use crate::repository::{Branch, ObjectId, RepoObject, Root, RootType};
use crate::util::WithComplete;

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

    /// Get a clone of the underlying Repo.
    pub fn repo(&self) -> Arc<Repo> {
        Arc::clone(&self.repo)
    }

    /// Update the repository with a new root using optimistic concurrency control.
    ///
    /// Calls `f` to generate a new root, then attempts to atomically update the
    /// current root. If the root changed while `f` was running, retries up to
    /// `max_attempts` times.
    ///
    /// Returns a new `RepoModel` instance on success (with fresh cache).
    pub async fn update<F, Fut>(&self, f: F, max_attempts: usize) -> Result<RepoModel, RepoError>
    where
        F: Fn() -> Fut,
        Fut: Future<Output = Result<WithComplete<ObjectId>, RepoError>>,
    {
        let mut starting_root_id = self.repo.read_current_root().await?;

        for _ in 0..max_attempts {
            // Call f() to generate a new root
            let result = f().await?;
            let new_root_id = result.result;

            // Wait for the write to complete
            result
                .complete
                .complete()
                .await
                .map_err(|e| RepoError::Other(format!("write completion failed: {}", e)))?;

            // Try to atomically swap the root
            match self
                .repo
                .swap_current_root(Some(&starting_root_id), &new_root_id)
                .await?
            {
                SwapResult::Success => {
                    // Return a new RepoModel with fresh cache
                    return Ok(RepoModel::new(Arc::clone(&self.repo)));
                }
                SwapResult::Mismatch(actual) => {
                    // Root changed, retry with the new value
                    starting_root_id = actual.ok_or_else(|| {
                        RepoError::Other("root was deleted during update".to_string())
                    })?;
                }
            }
        }

        Err(RepoError::Other(format!(
            "update failed after {} attempts due to concurrent modifications",
            max_attempts
        )))
    }

    /// Create a new Root object inheriting from the current root.
    ///
    /// If `branches_id` is None, inherits from the current root's branches.
    /// If `default_branch_name` is None, inherits from the current root's default branch name.
    pub async fn create_next_root(
        &self,
        branches_id: Option<ObjectId>,
        default_branch_name: Option<String>,
    ) -> Result<WithComplete<ObjectId>, RepoError> {
        let current_root = self.get_root().await?;
        let current_root_id = self.repo.read_current_root().await?;

        let new_root = Root {
            type_tag: RootType::Root,
            timestamp: Utc::now().to_rfc3339(),
            default_branch_name: default_branch_name
                .unwrap_or_else(|| current_root.default_branch_name.clone()),
            branches: branches_id.unwrap_or_else(|| current_root.branches.clone()),
            previous_root: Some(current_root_id),
        };

        self.repo.write_object(&RepoObject::Root(new_root)).await
    }

    /// Create a new Branches object with a branch added, updated, or removed.
    ///
    /// If `commit_id` is Some, the branch is added or updated with that commit.
    /// If `commit_id` is None, the branch is removed.
    pub async fn create_next_branches(
        &self,
        branch_name: &str,
        commit_id: Option<ObjectId>,
    ) -> Result<WithComplete<ObjectId>, RepoError> {
        let root = self.get_root().await?;

        let mut modifications = BranchListModifications::new();
        let branch = commit_id.map(|id| Branch {
            name: branch_name.to_string(),
            commit: id,
        });
        modifications
            .add(branch_name.to_string(), branch)
            .map_err(|e| RepoError::Other(format!("modification error: {}", e)))?;

        modify_branches(Arc::clone(&self.repo), &root.branches, modifications).await
    }
}
