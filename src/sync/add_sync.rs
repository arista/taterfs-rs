//! Add sync functionality.
//!
//! This module implements the `add_sync` function that connects a file store
//! to a repository directory.

use std::path::Path;
use std::sync::Arc;

use chrono::Utc;

use crate::app::{DirectoryListBuilder, upload_directory};
use crate::download::download_directory;
use crate::file_store::{FileStore, SyncState};
use crate::repo::Repo;
use crate::repo_model::{DirectoryModel, RepoModel, ResolvePathResult};
use crate::repository::CommitMetadata;
use crate::sync::error::{Result, SyncError};
use crate::util::NoopComplete;

/// Options for the add_sync operation.
#[derive(Debug, Clone, Default)]
pub struct AddSyncOptions {
    /// Custom commit message (default: "Add sync to /{directory}").
    pub commit_message: Option<String>,
}

/// Add a sync relationship between a file store and a repository directory.
///
/// This connects the file store to the specified repo, branch, and directory,
/// ensuring both have the same content before completing.
///
/// # Scenarios
///
/// 1. File store already has sync state -> Error
/// 2. Both empty -> Create empty repo directory with commit
/// 3. File store empty, repo has content -> Download repo to file store
/// 4. Repo empty, file store has content -> Upload file store to repo
/// 5. Both have content -> Error
///
/// # Arguments
///
/// * `file_store` - The file store to sync
/// * `repo` - The repository
/// * `repo_model` - High-level repo operations
/// * `repo_url` - URL/spec of the repository (for sync state)
/// * `branch` - Branch name to sync with
/// * `repo_directory` - Directory within repo to sync with
/// * `options` - Additional options
pub async fn add_sync(
    file_store: Arc<dyn FileStore>,
    repo: Arc<Repo>,
    repo_model: &RepoModel,
    repo_url: &str,
    branch: &str,
    repo_directory: &str,
    options: AddSyncOptions,
) -> Result<()> {
    // Check if file store supports sync state
    let sync_state_mgr = file_store
        .get_sync_state_manager()
        .ok_or(SyncError::NoSyncStateSupport)?;

    // Check if already has sync state
    if sync_state_mgr.get_sync_state().await?.is_some() {
        return Err(SyncError::AlreadyHasSyncState);
    }
    if sync_state_mgr.get_next_sync_state().await?.is_some() {
        return Err(SyncError::PendingSyncState);
    }

    // Check if file store is empty
    let file_store_empty = is_file_store_empty(&*file_store).await?;

    // Check if repo directory is empty
    let (repo_dir_empty, branch_commit_id) =
        is_repo_directory_empty(repo_model, branch, repo_directory).await?;

    // Handle scenarios
    let commit_message = options
        .commit_message
        .unwrap_or_else(|| format!("Add sync to /{}", repo_directory));

    match (file_store_empty, repo_dir_empty) {
        // Both empty - create empty repo directory
        (true, true) => {
            let base_commit_id = create_empty_directory_commit_with_update(
                &repo,
                repo_model,
                branch,
                repo_directory,
                &commit_message,
                branch_commit_id.as_ref(),
            )
            .await?;

            // Create and commit sync state
            let sync_state = SyncState {
                created_at: Utc::now().to_rfc3339(),
                repository_url: repo_url.to_string(),
                repository_directory: repo_directory.to_string(),
                branch_name: branch.to_string(),
                base_commit: base_commit_id,
            };
            sync_state_mgr
                .set_next_sync_state(Some(&sync_state))
                .await?;
            sync_state_mgr.commit_next_sync_state().await?;
        }

        // File store empty, repo has content - download
        (true, false) => {
            let branch_commit = branch_commit_id.ok_or_else(|| {
                SyncError::Other("branch has no commit but repo directory is not empty".to_string())
            })?;

            // Get the directory to download
            let branch_model = repo_model
                .get_branch(branch)
                .await?
                .ok_or_else(|| SyncError::BranchNotFound(branch.to_string()))?;
            let commit = branch_model.commit();
            let root = commit.root().await?;

            // Resolve the repo directory path
            let dir_to_download = if repo_directory.is_empty() || repo_directory == "/" {
                root.directory().id.clone()
            } else {
                let resolved = root.resolve_path(repo_directory).await?;
                match resolved {
                    ResolvePathResult::Directory(d) => d.id().clone(),
                    ResolvePathResult::Root => root.directory().id.clone(),
                    ResolvePathResult::File(_) => {
                        return Err(SyncError::Other(format!(
                            "repo path {} is a file, not a directory",
                            repo_directory
                        )));
                    }
                    ResolvePathResult::None => {
                        return Err(SyncError::Other(format!(
                            "repo path {} not found",
                            repo_directory
                        )));
                    }
                }
            };

            // Create pending sync state before download
            let sync_state = SyncState {
                created_at: Utc::now().to_rfc3339(),
                repository_url: repo_url.to_string(),
                repository_directory: repo_directory.to_string(),
                branch_name: branch.to_string(),
                base_commit: branch_commit,
            };
            sync_state_mgr
                .set_next_sync_state(Some(&sync_state))
                .await?;

            // Download and wait for completion
            let download_result = download_directory(
                Arc::clone(&repo),
                &dir_to_download,
                &*file_store,
                std::path::PathBuf::new(),
                true, // with_stage
                false,
                None,
            )
            .await?;
            download_result
                .complete
                .complete()
                .await
                .map_err(|e| SyncError::Other(format!("download completion failed: {}", e)))?;

            // Commit sync state
            sync_state_mgr.commit_next_sync_state().await?;
        }

        // Repo empty, file store has content - upload
        (false, true) => {
            // Get file store cache
            let cache = file_store.get_cache();

            // Upload file store contents and wait for completion
            let upload_result =
                upload_directory(&*file_store, Arc::clone(&repo), cache, None).await?;
            upload_result
                .complete
                .complete()
                .await
                .map_err(|e| SyncError::Other(format!("upload completion failed: {}", e)))?;

            // Create commit with the uploaded directory using RepoModel.update
            let base_commit_id = create_directory_commit_with_update(
                &repo,
                repo_model,
                branch,
                repo_directory,
                upload_result.result.hash,
                &commit_message,
                branch_commit_id.as_ref(),
            )
            .await?;

            // Create and commit sync state
            let sync_state = SyncState {
                created_at: Utc::now().to_rfc3339(),
                repository_url: repo_url.to_string(),
                repository_directory: repo_directory.to_string(),
                branch_name: branch.to_string(),
                base_commit: base_commit_id,
            };
            sync_state_mgr
                .set_next_sync_state(Some(&sync_state))
                .await?;
            sync_state_mgr.commit_next_sync_state().await?;
        }

        // Both have content - error
        (false, false) => {
            return Err(SyncError::BothHaveContent);
        }
    }

    Ok(())
}

/// Check if a file store is empty (has no files).
async fn is_file_store_empty(file_store: &dyn FileStore) -> Result<bool> {
    let source = file_store.get_source().ok_or(SyncError::NoFileSource)?;
    let mut scan_events = source.scan(None).await?;

    // If we can get any event, it's not empty
    // We're looking for the first non-directory event, or any file
    loop {
        match scan_events.next().await {
            Some(Ok(event)) => {
                use crate::file_store::ScanEvent;
                match event {
                    ScanEvent::EnterDirectory { .. } => continue,
                    ScanEvent::ExitDirectory => continue,
                    ScanEvent::File { .. } => return Ok(false), // Has at least one file
                }
            }
            Some(Err(e)) => return Err(SyncError::FileStore(e)),
            None => return Ok(true), // No files found
        }
    }
}

/// Check if a repo directory is empty.
/// Returns (is_empty, current_branch_commit_id).
async fn is_repo_directory_empty(
    repo_model: &RepoModel,
    branch: &str,
    repo_directory: &str,
) -> Result<(bool, Option<String>)> {
    // Get the branch
    let branch_model = match repo_model.get_branch(branch).await? {
        Some(b) => b,
        None => {
            // Branch doesn't exist - directory is empty
            return Ok((true, None));
        }
    };

    // Get the commit
    let commit = branch_model.commit();
    let commit_id = commit.id.clone();

    // Get the root directory
    let root = commit.root().await?;

    // Resolve the path
    if repo_directory.is_empty() || repo_directory == "/" {
        // Check root directory
        let dir = root.directory();
        let is_empty = is_directory_empty(&dir).await?;
        Ok((is_empty, Some(commit_id)))
    } else {
        let resolved = root.resolve_path(repo_directory).await?;
        match resolved {
            ResolvePathResult::Directory(d) => {
                let dir = d.directory();
                let is_empty = is_directory_empty(&dir).await?;
                Ok((is_empty, Some(commit_id)))
            }
            ResolvePathResult::Root => {
                let dir = root.directory();
                let is_empty = is_directory_empty(&dir).await?;
                Ok((is_empty, Some(commit_id)))
            }
            ResolvePathResult::File(_) => {
                // Path is a file - not empty
                Ok((false, Some(commit_id)))
            }
            ResolvePathResult::None => {
                // Path doesn't exist - empty
                Ok((true, Some(commit_id)))
            }
        }
    }
}

/// Check if a directory model is empty.
async fn is_directory_empty(dir: &DirectoryModel) -> Result<bool> {
    let mut entries = dir.entries().await?;
    match entries.next().await {
        Ok(entry) => Ok(entry.is_none()),
        Err(e) => Err(SyncError::Repo(e)),
    }
}

/// Create an empty directory and commit it using RepoModel.update.
async fn create_empty_directory_commit_with_update(
    repo: &Arc<Repo>,
    repo_model: &RepoModel,
    branch: &str,
    repo_directory: &str,
    commit_message: &str,
    parent_commit: Option<&String>,
) -> Result<String> {
    // Create an empty directory and wait for completion
    let builder = DirectoryListBuilder::new(Arc::clone(repo));
    let empty_dir = builder.finish().await?;
    empty_dir
        .complete
        .complete()
        .await
        .map_err(|e| SyncError::Other(format!("empty directory completion failed: {}", e)))?;

    create_directory_commit_with_update(
        repo,
        repo_model,
        branch,
        repo_directory,
        empty_dir.hash,
        commit_message,
        parent_commit,
    )
    .await
}

/// Create a commit with the given directory at the specified repo path using RepoModel.update.
async fn create_directory_commit_with_update(
    repo: &Arc<Repo>,
    repo_model: &RepoModel,
    branch: &str,
    repo_directory: &str,
    directory_id: String,
    commit_message: &str,
    parent_commit: Option<&String>,
) -> Result<String> {
    use crate::app::{DirTreeModSpec, mod_dir_tree};
    use crate::repo::DirectoryEntry;
    use crate::repository::{Commit, CommitType, DirEntry, Directory, DirectoryType, RepoObject};

    // Get or create the root directory
    let root_dir = if let Some(parent_id) = parent_commit {
        let commit = repo.read_commit(parent_id).await?;
        repo.read_directory(&commit.directory).await?
    } else {
        Directory {
            type_tag: DirectoryType::Directory,
            entries: vec![],
        }
    };

    // If repo_directory is root, just use the directory directly
    let final_root_dir_id = if repo_directory.is_empty() || repo_directory == "/" {
        directory_id
    } else {
        // Use mod_dir_tree to place the directory at the right path
        let mut spec = DirTreeModSpec::new();
        let dir_entry = DirectoryEntry::Directory(DirEntry {
            name: Path::new(repo_directory)
                .file_name()
                .and_then(|s| s.to_str())
                .unwrap_or(repo_directory)
                .to_string(),
            directory: directory_id,
        });
        spec.add(Path::new(repo_directory), dir_entry, Arc::new(NoopComplete))
            .map_err(|e| SyncError::Other(e.to_string()))?;

        let result = mod_dir_tree(repo, root_dir, spec).await?;
        result
            .complete
            .complete()
            .await
            .map_err(|e| SyncError::Other(format!("mod_dir_tree completion failed: {}", e)))?;
        result.result
    };

    // Create the commit
    let commit = Commit {
        type_tag: CommitType::Commit,
        directory: final_root_dir_id.clone(),
        parents: parent_commit.into_iter().cloned().collect(),
        metadata: Some(CommitMetadata {
            timestamp: Some(Utc::now().to_rfc3339()),
            author: None,
            committer: None,
            message: Some(commit_message.to_string()),
        }),
    };

    let commit_result = repo.write_object(&RepoObject::Commit(commit)).await?;
    commit_result
        .complete
        .complete()
        .await
        .map_err(|e| SyncError::Other(format!("commit write completion failed: {}", e)))?;

    let commit_id = commit_result.result;

    // Use RepoModel.update to atomically update the repo with the new branch
    let branch_owned = branch.to_string();
    let commit_id_for_closure = commit_id.clone();
    let repo_for_closure = Arc::clone(repo);
    repo_model
        .update(
            || {
                let branch_name = branch_owned.clone();
                let cid = commit_id_for_closure.clone();
                let r = Arc::clone(&repo_for_closure);
                async move {
                    let rm = RepoModel::new(r);
                    rm.create_next_branches(&branch_name, Some(cid)).await
                }
            },
            3, // max_attempts
        )
        .await?;

    Ok(commit_id)
}
