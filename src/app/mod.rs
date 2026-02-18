//! Application-level utilities.

#[allow(clippy::module_inception)]
mod app;
mod capacity_managers;
mod list_builder;
mod list_modifier;
mod list_search;
mod mod_dir_tree;
mod upload;

pub use app::{App, AppContext, AppCreateFileStoreContext, AppCreateRepoContext, AppError, Result};
pub use capacity_managers::CapacityManagers;
pub use list_builder::{
    BranchListBuilder, BranchesConfig, DirectoryConfig, DirectoryLeaf, DirectoryListBuilder,
    FileConfig, FileListBuilder, ListBuilder, ListBuilderConfig, ListResult,
};
pub use list_modifier::{
    BranchListElems, BranchListModifications, DirectoryEntryListElems,
    DirectoryEntryListModifications, FileChunkListElems, FileChunkListModifications, ListElem,
    ListElems, ListModification, ListModificationError, ListModifications, ListModificationsVec,
    modify_branches, modify_directory, modify_file, modify_list,
};
pub use list_search::{search_branches, search_directory, search_file};
pub use mod_dir_tree::{
    mod_dir_tree, DirTreeModSpec, DirTreeModSpecEntry, DirTreeModSpecEntryIter,
    DirTreeModSpecError,
};
pub use upload::{
    UploadDirectoryResult, UploadError, UploadFileResult, upload_directory, upload_file,
};
