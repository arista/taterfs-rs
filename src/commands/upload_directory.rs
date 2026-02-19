//! Upload directory command.
//!
//! Uploads a directory from a filestore to a repository, creating a new commit
//! with the uploaded content.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use thiserror::Error;

use crate::app::{
    mod_dir_tree, upload_directory as app_upload_directory, App, AppCreateFileStoreContext,
    AppCreateRepoContext, DirTreeModSpec, DirTreeModSpecError, UploadError,
};
use crate::cli::command_context::CommandContext;
use crate::repo::{DirectoryEntry, RepoError};
use crate::repo_model::RepoModel;
use crate::repository::{CommitMetadata, DirEntry, ObjectId};
use crate::util::{Complete, Completes, WithComplete};

// =============================================================================
// Error Types
// =============================================================================

/// Errors that can occur during the upload_directory command.
#[derive(Debug, Error)]
pub enum UploadDirectoryError {
    /// App error.
    #[error("app error: {0}")]
    App(#[from] crate::app::AppError),

    /// Upload error.
    #[error("upload error: {0}")]
    Upload(#[from] UploadError),

    /// Repository error.
    #[error("repository error: {0}")]
    Repo(#[from] RepoError),

    /// Dir tree modification error.
    #[error("dir tree modification error: {0}")]
    DirTreeMod(#[from] DirTreeModSpecError),

    /// Missing required context.
    #[error("missing required context: {0}")]
    MissingContext(String),

    /// Completion error.
    #[error("completion error: {0}")]
    Completion(String),

    /// Cache error.
    #[error("cache error: {0}")]
    Cache(String),
}

/// Result type for upload_directory command.
pub type Result<T> = std::result::Result<T, UploadDirectoryError>;

// =============================================================================
// Command Arguments
// =============================================================================

/// Arguments for the upload_directory command.
pub struct UploadDirectoryArgs {
    /// Path within the filestore to upload.
    pub filestore_path: PathBuf,
    /// Path within the repository to upload to.
    pub repo_path: PathBuf,
}

// =============================================================================
// Command Implementation
// =============================================================================

/// Upload a directory from a filestore to a repository.
///
/// This command:
/// 1. Uploads the directory from the filestore to get a directory_id
/// 2. Creates a RepoModel and calls update() with a function that:
///    - Gets the current commit of the branch and its directory_id
///    - Uses mod_dir_tree to insert the uploaded directory at repo_path
///    - Creates a new commit with the commit_metadata
///    - Creates new branches pointing to the new commit
///    - Creates a new root with the updated branches
///    - Returns the new root ObjectId
pub async fn upload_directory(
    args: UploadDirectoryArgs,
    context: &CommandContext,
    app: &App,
) -> Result<ObjectId> {
    // Validate required context
    let repository_spec = context
        .repository_spec
        .as_ref()
        .ok_or_else(|| UploadDirectoryError::MissingContext("repository_spec".to_string()))?;
    let file_store_spec = context
        .file_store_spec
        .as_ref()
        .ok_or_else(|| UploadDirectoryError::MissingContext("file_store_spec".to_string()))?;
    let branch_name = context
        .branch
        .as_ref()
        .ok_or_else(|| UploadDirectoryError::MissingContext("branch".to_string()))?;
    let commit_metadata = context.commit_metadata.clone();

    // Create repo and filestore
    let repo = app
        .create_repo(AppCreateRepoContext::new(repository_spec))
        .await?;
    let file_store = app
        .create_file_store(AppCreateFileStoreContext::new(file_store_spec))
        .await?;

    // Get filestore cache
    let cache = app
        .file_store_caches()
        .get_cache(file_store_spec)
        .await
        .map_err(UploadDirectoryError::Cache)?;

    // Upload the directory
    let upload_result = app_upload_directory(
        file_store.as_ref(),
        Arc::clone(&repo),
        cache,
        Some(args.filestore_path.as_path()),
    )
    .await?;

    let uploaded_dir_id = upload_result.result.hash.clone();
    let upload_complete = upload_result.complete;

    // Create RepoModel and update
    let repo_model = RepoModel::new(Arc::clone(&repo));
    let max_attempts = context.config.config().repositories_config.max_root_swap_attempts;

    let new_repo_model = repo_model
        .update(
            || {
                let repo = Arc::clone(&repo);
                let branch_name = branch_name.clone();
                let repo_path = args.repo_path.clone();
                let uploaded_dir_id = uploaded_dir_id.clone();
                let commit_metadata = commit_metadata.clone();
                let upload_complete = Arc::clone(&upload_complete);

                async move {
                    update_repo_with_upload(
                        &repo,
                        &branch_name,
                        &repo_path,
                        &uploaded_dir_id,
                        commit_metadata,
                        upload_complete,
                    )
                    .await
                }
            },
            max_attempts,
        )
        .await?;

    // Return the new root id
    Ok(new_repo_model.repo().read_current_root().await?)
}

/// Helper function to perform the repository update.
async fn update_repo_with_upload(
    repo: &Arc<crate::repo::Repo>,
    branch_name: &str,
    repo_path: &Path,
    uploaded_dir_id: &ObjectId,
    commit_metadata: Option<CommitMetadata>,
    upload_complete: Arc<dyn Complete>,
) -> std::result::Result<WithComplete<ObjectId>, RepoError> {
    // Create a temporary RepoModel to access methods
    let repo_model = RepoModel::new(Arc::clone(repo));

    // Get the branch's current commit
    let branch_model = repo_model
        .get_branch(branch_name)
        .await?
        .ok_or_else(|| RepoError::Other(format!("branch '{}' not found", branch_name)))?;

    let commit_model = branch_model.commit();
    let root_dir_model = commit_model.root().await?;

    // Get the actual Directory object for mod_dir_tree
    let dir_model = root_dir_model.directory();
    let current_directory = repo.read_directory(&dir_model.id).await?;

    // Build DirTreeModSpec to insert uploaded directory at repo_path
    // We need to extract the last component of repo_path as the name
    let dir_name = repo_path
        .file_name()
        .map(|n| n.to_string_lossy().to_string())
        .unwrap_or_default();

    let dir_entry = DirEntry {
        name: dir_name,
        directory: uploaded_dir_id.clone(),
    };

    let mut spec = DirTreeModSpec::new();
    spec.add(
        repo_path,
        DirectoryEntry::Directory(dir_entry),
        Arc::clone(&upload_complete) as Arc<dyn Complete>,
    )
    .map_err(|e| RepoError::Other(format!("dir tree mod error: {}", e)))?;

    // Modify the directory tree
    let mod_result = mod_dir_tree(repo, current_directory, spec).await?;
    let new_dir_id = mod_result.result;
    let mod_complete = mod_result.complete;

    // Create next commit
    let commit_result = commit_model
        .create_next_commit(new_dir_id, commit_metadata, None)
        .await?;
    let new_commit_id = commit_result.result;
    let commit_complete = commit_result.complete;

    // Create next branches
    let branches_result = repo_model
        .create_next_branches(branch_name, Some(new_commit_id))
        .await?;
    let new_branches_id = branches_result.result;
    let branches_complete = branches_result.complete;

    // Create next root
    let root_result = repo_model
        .create_next_root(Some(new_branches_id), None)
        .await?;
    let new_root_id = root_result.result;
    let root_complete = root_result.complete;

    // Combine all completes
    let completes = Completes::new();
    completes
        .add(upload_complete)
        .map_err(|e| RepoError::Other(e.to_string()))?;
    completes
        .add(mod_complete)
        .map_err(|e| RepoError::Other(e.to_string()))?;
    completes
        .add(commit_complete)
        .map_err(|e| RepoError::Other(e.to_string()))?;
    completes
        .add(branches_complete)
        .map_err(|e| RepoError::Other(e.to_string()))?;
    completes
        .add(root_complete)
        .map_err(|e| RepoError::Other(e.to_string()))?;
    completes.done();

    Ok(WithComplete::new(new_root_id, Arc::new(completes)))
}
