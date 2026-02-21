//! Command context for CLI commands.
//!
//! This module provides automatic discovery of repositories and file stores
//! based on the current working directory and `.tfs/` configuration.

use std::path::{Path, PathBuf};

use chrono::Utc;
use thiserror::Error;

use crate::app::{App, AppCreateRepoContext};
use crate::config::ConfigHelper;
use crate::file_store::{FileStore, SyncState};
use crate::repo_model::RepoModel;
use crate::repository::{CommitMetadata, ObjectId};

// =============================================================================
// Error Types
// =============================================================================

/// Errors that can occur during command context creation.
#[derive(Debug, Error)]
pub enum CommandContextError {
    /// Repository is required but not specified and could not be discovered.
    #[error("repository is required but not specified and could not be discovered")]
    RepositoryRequired,

    /// File store is required but not specified and could not be discovered.
    #[error("file store is required but not specified and could not be discovered")]
    FileStoreRequired,

    /// Configuration error.
    #[error("configuration error: {0}")]
    Config(String),

    /// I/O error.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// File store error.
    #[error("file store error: {0}")]
    FileStore(#[from] crate::file_store::Error),

    /// App error.
    #[error("app error: {0}")]
    App(#[from] crate::app::AppError),
}

/// Result type for command context operations.
pub type Result<T> = std::result::Result<T, CommandContextError>;

// =============================================================================
// CommandContextInput
// =============================================================================

/// Input from CLI arguments for command context resolution.
#[derive(Debug, Clone, Default)]
pub struct CommandContextInput {
    /// Path to the main configuration file.
    pub config_file: Option<PathBuf>,
    /// Path to the configuration overrides file.
    pub config_file_overrides: Option<PathBuf>,
    /// Configuration overrides (name=value pairs).
    pub config: Vec<(String, String)>,
    /// Format output as JSON.
    pub json: bool,
    /// Disable caching.
    pub no_cache: bool,
    /// Repository specification (URL or name).
    pub repository_spec: Option<String>,
    /// File store specification (URL or name).
    pub file_store_spec: Option<String>,
    /// Explicit file store path from command argument.
    pub file_store_path: Option<String>,
    /// Explicit repository path from command argument.
    pub repository_path: Option<String>,
    /// Branch name to use.
    pub branch: Option<String>,
    /// Specific commit to use.
    pub commit: Option<String>,
    /// Commit timestamp in ISO 8601 format.
    pub commit_timestamp: Option<String>,
    /// Commit author.
    pub commit_author: Option<String>,
    /// Commit committer.
    pub commit_committer: Option<String>,
    /// Commit message.
    pub commit_message: Option<String>,
}

impl CommandContextInput {
    /// Set the file store path (builder pattern).
    pub fn with_file_store_path(mut self, path: Option<String>) -> Self {
        self.file_store_path = path;
        self
    }

    /// Set the repository path (builder pattern).
    pub fn with_repository_path(mut self, path: Option<String>) -> Self {
        self.repository_path = path;
        self
    }
}

// =============================================================================
// CommandContextRequirements
// =============================================================================

/// Requirements for a command's context.
#[derive(Debug, Clone, Default)]
pub struct CommandContextRequirements {
    /// Command requires a repository.
    pub require_repository: bool,
    /// Command requires a repository path (cwd-relative path within repo).
    pub require_repository_path: bool,
    /// Command requires a file store.
    pub require_file_store: bool,
    /// Command requires a file store path (cwd-relative path within filestore).
    pub require_file_store_path: bool,
    /// Command requires a branch.
    pub require_branch: bool,
    /// Command requires a commit (existing commit to read from).
    pub require_commit: bool,
    /// Command requires commit metadata.
    pub require_commit_metadata: bool,
}

impl CommandContextRequirements {
    /// Create a new requirements builder with no requirements.
    pub fn new() -> Self {
        Self::default()
    }

    /// Require a repository.
    pub fn with_repository(mut self) -> Self {
        self.require_repository = true;
        self
    }

    /// Require a repository path (implies requiring repository).
    pub fn with_repository_path(mut self) -> Self {
        self.require_repository = true;
        self.require_repository_path = true;
        self
    }

    /// Require a file store.
    pub fn with_file_store(mut self) -> Self {
        self.require_file_store = true;
        self
    }

    /// Require a file store path (implies requiring file store).
    pub fn with_file_store_path(mut self) -> Self {
        self.require_file_store = true;
        self.require_file_store_path = true;
        self
    }

    /// Require a branch (implies requiring repository).
    pub fn with_branch(mut self) -> Self {
        self.require_repository = true;
        self.require_branch = true;
        self
    }

    /// Require a commit (implies requiring repository and branch).
    pub fn with_commit(mut self) -> Self {
        self.require_repository = true;
        self.require_branch = true;
        self.require_commit = true;
        self
    }

    /// Require commit metadata.
    pub fn with_commit_metadata(mut self) -> Self {
        self.require_commit_metadata = true;
        self
    }
}

// =============================================================================
// CommandContext
// =============================================================================

/// Resolved command context with discovered/specified resources.
#[derive(Debug, Clone)]
pub struct CommandContext {
    /// Resolved configuration.
    pub config: ConfigHelper,
    /// Format output as JSON.
    pub json: bool,
    /// Disable caching.
    pub no_cache: bool,
    /// Repository specification (if available/required).
    pub repository_spec: Option<String>,
    /// Fully resolved path within the repository (if required).
    pub repository_path: Option<String>,
    /// File store specification (if available/required).
    pub file_store_spec: Option<String>,
    /// Fully resolved path within the file store (if required).
    pub file_store_path: Option<String>,
    /// Branch name (if required). Defaults to repository's default branch.
    pub branch: Option<String>,
    /// Commit ID (if required). Defaults to branch's current commit.
    pub commit: Option<ObjectId>,
    /// Commit metadata (if required).
    pub commit_metadata: Option<CommitMetadata>,
}

// =============================================================================
// Path Resolution Helpers
// =============================================================================

/// Resolve a path relative to a base path.
///
/// - If `explicit_path` is None, returns `base`.
/// - If `explicit_path` starts with "/", returns it as-is.
/// - Otherwise, resolves `explicit_path` relative to `base`.
pub fn resolve_path(base: &str, explicit_path: Option<&str>) -> String {
    match explicit_path {
        None => base.to_string(),
        Some(path) if path.starts_with('/') => path.to_string(),
        Some(path) => {
            if base == "/" {
                format!("/{}", path)
            } else {
                format!("{}/{}", base.trim_end_matches('/'), path)
            }
        }
    }
}

/// Compute the filestore path based on cwd relative to the filestore root.
///
/// - If cwd is within the filestore root, returns the relative path prefixed with "/".
/// - Otherwise, returns "/".
fn compute_filestore_base_path(filestore_root: &Path, cwd: &Path) -> String {
    if cwd.starts_with(filestore_root) {
        cwd.strip_prefix(filestore_root)
            .map(|p| {
                let s = p.to_string_lossy();
                if s.is_empty() {
                    "/".to_string()
                } else {
                    format!("/{}", s)
                }
            })
            .unwrap_or_else(|_| "/".to_string())
    } else {
        "/".to_string()
    }
}

/// Compute the repository path based on cwd, filestore root, and repository directory.
///
/// Takes the cwd-relative path within the filestore and resolves it against the
/// repository_directory from the sync state.
fn compute_repository_base_path(
    filestore_root: &Path,
    cwd: &Path,
    repository_directory: &str,
) -> String {
    let fs_relative = if cwd.starts_with(filestore_root) {
        cwd.strip_prefix(filestore_root)
            .map(|p| p.to_string_lossy().to_string())
            .unwrap_or_default()
    } else {
        String::new()
    };

    let repo_dir = repository_directory.trim_matches('/');

    if fs_relative.is_empty() {
        if repo_dir.is_empty() {
            "/".to_string()
        } else {
            format!("/{}", repo_dir)
        }
    } else if repo_dir.is_empty() {
        format!("/{}", fs_relative)
    } else {
        format!("/{}/{}", repo_dir, fs_relative)
    }
}

// =============================================================================
// FileStore Discovery
// =============================================================================

/// Information about a discovered filestore.
struct DiscoveredFileStore {
    /// The root path of the filestore.
    root: PathBuf,
    /// The generated spec for the filestore.
    spec: String,
}

/// Discover the filestore by searching upward for a `.tfs/` directory.
fn discover_filestore(cwd: &Path) -> Option<DiscoveredFileStore> {
    let mut current = cwd.to_path_buf();

    loop {
        let tfs_dir = current.join(".tfs");
        if tfs_dir.is_dir() {
            let spec = format!("file://{}", current.to_string_lossy());
            return Some(DiscoveredFileStore {
                root: current,
                spec,
            });
        }

        if !current.pop() {
            break;
        }
    }

    None
}

/// Discover the repository from a filestore's sync state.
async fn discover_repository_from_filestore(
    file_store: &dyn FileStore,
) -> Option<(String, String)> {
    let sync_manager = file_store.get_sync_state_manager()?;

    // Try current sync state first, then next sync state
    let sync_state: Option<SyncState> = match sync_manager.get_sync_state().await {
        Ok(Some(state)) => Some(state),
        Ok(None) => sync_manager.get_next_sync_state().await.ok().flatten(),
        Err(_) => sync_manager.get_next_sync_state().await.ok().flatten(),
    };

    sync_state.map(|s| (s.repository_url, s.repository_directory))
}

// =============================================================================
// Context Creation
// =============================================================================

/// Create a command context from input and requirements.
///
/// This function:
/// 1. Discovers or uses the explicit filestore specification
/// 2. Discovers or uses the explicit repository specification
/// 3. Resolves paths based on the current working directory
pub async fn create_command_context(
    input: CommandContextInput,
    requirements: CommandContextRequirements,
    app: &App,
) -> Result<CommandContext> {
    let cwd = std::env::current_dir()?;

    // Step 1: Determine filestore
    let (file_store_spec, filestore_root) = resolve_filestore(&input, &requirements, &cwd)?;

    // Step 2: Determine repository (may need to load filestore to read sync state)
    let (repository_spec, repository_directory) =
        resolve_repository(&input, &requirements, &file_store_spec, app).await?;

    // Step 3: Compute and resolve paths if required
    let file_store_path = if requirements.require_file_store_path {
        // Compute base path from cwd relative to filestore root
        let base = filestore_root
            .as_ref()
            .map(|root| compute_filestore_base_path(root, &cwd))
            .unwrap_or_else(|| "/".to_string());
        // Resolve explicit path (if any) relative to base
        Some(resolve_path(&base, input.file_store_path.as_deref()))
    } else {
        None
    };

    let repository_path = if requirements.require_repository_path {
        // Compute base path from cwd relative to repo directory
        let base = match (&filestore_root, &repository_directory) {
            (Some(root), Some(repo_dir)) => compute_repository_base_path(root, &cwd, repo_dir),
            _ => "/".to_string(),
        };
        // Resolve explicit path (if any) relative to base
        Some(resolve_path(&base, input.repository_path.as_deref()))
    } else {
        None
    };

    // Step 4: Resolve branch if required
    let branch = if requirements.require_branch || requirements.require_commit {
        if let Some(branch) = &input.branch {
            Some(branch.clone())
        } else if let Some(repo_spec) = &repository_spec {
            // Need to get default branch from repo's Root
            let repo = app
                .create_repo(AppCreateRepoContext::new(repo_spec))
                .await?;
            let repo_model = RepoModel::new(repo);
            let default_branch = repo_model.default_branch().await.map_err(|e| {
                CommandContextError::Config(format!("failed to get default branch: {}", e))
            })?;
            Some(default_branch.branch_name.clone())
        } else {
            None
        }
    } else {
        input.branch.clone()
    };

    // Step 5: Resolve commit if required
    let commit: Option<ObjectId> = if requirements.require_commit {
        if let Some(commit_str) = &input.commit {
            // Use explicitly provided commit
            Some(commit_str.clone())
        } else if let (Some(repo_spec), Some(branch_name)) = (&repository_spec, &branch) {
            // Resolve commit from branch's current commit
            let repo = app
                .create_repo(AppCreateRepoContext::new(repo_spec))
                .await?;
            let repo_model = RepoModel::new(repo);
            let branch_model = repo_model.get_branch(branch_name).await.map_err(|e| {
                CommandContextError::Config(format!(
                    "failed to get branch '{}': {}",
                    branch_name, e
                ))
            })?;
            match branch_model {
                Some(bm) => Some(bm.branch.commit.clone()),
                None => {
                    return Err(CommandContextError::Config(format!(
                        "branch '{}' not found",
                        branch_name
                    )));
                }
            }
        } else {
            None
        }
    } else {
        input.commit.clone()
    };

    // Step 6: Build commit metadata if required
    let commit_metadata = if requirements.require_commit_metadata {
        Some(CommitMetadata {
            timestamp: input
                .commit_timestamp
                .clone()
                .or_else(|| Some(Utc::now().to_rfc3339())),
            author: input.commit_author.clone(),
            committer: input.commit_committer.clone(),
            message: input.commit_message.clone(),
        })
    } else {
        None
    };

    Ok(CommandContext {
        config: app.config().clone(),
        json: input.json,
        no_cache: input.no_cache,
        repository_spec,
        repository_path,
        file_store_spec,
        file_store_path,
        branch,
        commit,
        commit_metadata,
    })
}

/// Resolve the filestore specification and root path.
fn resolve_filestore(
    input: &CommandContextInput,
    requirements: &CommandContextRequirements,
    cwd: &Path,
) -> Result<(Option<String>, Option<PathBuf>)> {
    if let Some(spec) = &input.file_store_spec {
        // Explicit filestore spec - try to determine root if it's filesystem-based
        let root = if spec.starts_with("file://") {
            Some(PathBuf::from(spec.strip_prefix("file://").unwrap()))
        } else {
            None
        };
        Ok((Some(spec.clone()), root))
    } else if requirements.require_file_store || requirements.require_repository {
        // Need to discover filestore
        if let Some(discovered) = discover_filestore(cwd) {
            Ok((Some(discovered.spec), Some(discovered.root)))
        } else if requirements.require_file_store {
            Err(CommandContextError::FileStoreRequired)
        } else {
            Ok((None, None))
        }
    } else {
        Ok((None, None))
    }
}

/// Resolve the repository specification and directory.
async fn resolve_repository(
    input: &CommandContextInput,
    requirements: &CommandContextRequirements,
    file_store_spec: &Option<String>,
    app: &App,
) -> Result<(Option<String>, Option<String>)> {
    if let Some(spec) = &input.repository_spec {
        // Explicit repository spec
        Ok((Some(spec.clone()), None))
    } else if requirements.require_repository {
        // Need to discover repository from filestore's sync state
        if let Some(fs_spec) = file_store_spec {
            let fs_ctx = crate::app::AppCreateFileStoreContext {
                spec: fs_spec.clone(),
            };
            let file_store = app.create_file_store(fs_ctx).await?;

            if let Some((repo_url, repo_dir)) =
                discover_repository_from_filestore(file_store.as_ref()).await
            {
                Ok((Some(repo_url), Some(repo_dir)))
            } else {
                Err(CommandContextError::RepositoryRequired)
            }
        } else {
            Err(CommandContextError::RepositoryRequired)
        }
    } else {
        Ok((None, None))
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_resolve_path_no_explicit() {
        assert_eq!(resolve_path("/foo/bar", None), "/foo/bar");
        assert_eq!(resolve_path("/", None), "/");
    }

    #[test]
    fn test_resolve_path_absolute() {
        assert_eq!(resolve_path("/foo/bar", Some("/other")), "/other");
        assert_eq!(resolve_path("/", Some("/other/path")), "/other/path");
    }

    #[test]
    fn test_resolve_path_relative() {
        assert_eq!(resolve_path("/foo", Some("bar")), "/foo/bar");
        assert_eq!(resolve_path("/foo/", Some("bar")), "/foo/bar");
        assert_eq!(resolve_path("/", Some("bar")), "/bar");
    }

    #[test]
    fn test_compute_filestore_base_path() {
        let root = PathBuf::from("/store");

        // cwd at root
        assert_eq!(
            compute_filestore_base_path(&root, &PathBuf::from("/store")),
            "/"
        );

        // cwd in subdirectory
        assert_eq!(
            compute_filestore_base_path(&root, &PathBuf::from("/store/foo/bar")),
            "/foo/bar"
        );

        // cwd outside filestore
        assert_eq!(
            compute_filestore_base_path(&root, &PathBuf::from("/other/path")),
            "/"
        );
    }

    #[test]
    fn test_compute_repository_base_path() {
        let root = PathBuf::from("/store");

        // cwd at root, no repo dir
        assert_eq!(
            compute_repository_base_path(&root, &PathBuf::from("/store"), ""),
            "/"
        );

        // cwd at root, with repo dir
        assert_eq!(
            compute_repository_base_path(&root, &PathBuf::from("/store"), "data"),
            "/data"
        );

        // cwd in subdir, with repo dir
        assert_eq!(
            compute_repository_base_path(&root, &PathBuf::from("/store/local/path"), "data"),
            "/data/local/path"
        );

        // cwd outside store
        assert_eq!(
            compute_repository_base_path(&root, &PathBuf::from("/other"), "data"),
            "/data"
        );
    }

    #[test]
    fn test_requirements_builder() {
        let req = CommandContextRequirements::new()
            .with_repository()
            .with_file_store_path();

        assert!(req.require_repository);
        assert!(!req.require_repository_path);
        assert!(req.require_file_store);
        assert!(req.require_file_store_path);
    }
}
