//! Branch model type.

use std::sync::Arc;

use crate::repo::Repo;
use crate::repository::Branch;

use super::CommitModel;

/// Model wrapper for a branch in the repository.
///
/// Contains the branch data and provides access to the commit model.
pub struct BranchModel {
    repo: Arc<Repo>,
    /// Name of the branch.
    pub branch_name: String,
    /// The underlying Branch object.
    pub branch: Branch,
}

impl BranchModel {
    /// Create a new BranchModel.
    pub fn new(repo: Arc<Repo>, branch: Branch) -> Self {
        Self {
            repo,
            branch_name: branch.name.clone(),
            branch,
        }
    }

    /// Get the commit model for this branch's current commit.
    pub fn commit(&self) -> CommitModel {
        CommitModel::new(Arc::clone(&self.repo), self.branch.commit.clone())
    }
}
