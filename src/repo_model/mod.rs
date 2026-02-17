//! High-level object model for repository navigation.
//!
//! This module provides model objects that wrap repository backend storage objects,
//! offering a higher-level API for navigating repository structure. Model objects
//! lazily fetch and cache backend objects as needed.

mod branch_model;
mod commit_model;
mod directory_entry_model;
mod directory_model;
mod directory_root_model;
mod file_model;
#[allow(clippy::module_inception)]
mod repo_model;

pub use branch_model::BranchModel;
pub use commit_model::CommitModel;
pub use directory_entry_model::DirectoryEntryModel;
pub use directory_model::{DirectoryModel, EntryModel, EntryModelList};
pub use directory_root_model::{DirectoryRootModel, ResolvePathError, ResolvePathResult};
pub use file_model::{FileEntryModel, FileModel};
pub use repo_model::RepoModel;
