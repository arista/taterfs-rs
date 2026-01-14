//! Application-level utilities.

#[allow(clippy::module_inception)]
mod app;
mod capacity_managers;
mod list_builder;

pub use app::{
    App, AppContext, AppCreateFileStoreContext, AppCreateRepoContext, AppError, Result,
};
pub use capacity_managers::CapacityManagers;
pub use list_builder::{
    BranchListBuilder, BranchesConfig, DirectoryConfig, DirectoryLeaf, DirectoryListBuilder,
    FileConfig, FileListBuilder, ListBuilder, ListBuilderConfig, ListResult,
};
