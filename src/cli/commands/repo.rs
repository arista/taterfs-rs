//! Repository subcommands.

use std::sync::Arc;

use clap::{Args, Subcommand};
use serde::Serialize;
use sha2::{Digest, Sha256};

use std::path::Path;

use crate::app::{upload_directory, upload_file, App};
use crate::cli::{CliError, FileStoreArgs, GlobalArgs, InputSource, OutputSink, RepoArgs, Result};
use crate::download::{download_actions, DownloadAction};
use crate::repo::RepoInitialize;
use crate::util::ManagedBuffers;

// =============================================================================
// Repo Subcommands
// =============================================================================

/// Repository subcommands.
#[derive(Subcommand, Debug)]
pub enum RepoCommand {
    /// Initialize a repository.
    Initialize(InitializeArgs),

    /// Get the current root object ID.
    #[command(name = "get-current-root")]
    GetCurrentRoot(GetCurrentRootArgs),

    /// Get repository information.
    #[command(name = "get-repository-info")]
    GetRepositoryInfo(GetRepositoryInfoArgs),

    /// Set the current root object ID.
    #[command(name = "set-current-root")]
    SetCurrentRoot(SetCurrentRootArgs),

    /// Check if an object exists.
    Exists(ExistsArgs),

    /// Read an object.
    Read(ReadArgs),

    /// Write an object.
    Write(WriteArgs),

    /// Upload a file to the repository.
    #[command(name = "upload-file")]
    UploadFile(UploadFileArgs),

    /// Upload a directory to the repository.
    #[command(name = "upload-directory")]
    UploadDirectory(UploadDirectoryArgs),

    /// Generate download actions for synchronizing a file store with a repository directory.
    #[command(name = "download-actions")]
    DownloadActions(DownloadActionsArgs),
}

impl RepoCommand {
    /// Run the repo subcommand.
    pub async fn run(self, app: &App, global: &GlobalArgs) -> Result<()> {
        match self {
            RepoCommand::Initialize(args) => args.run(app, global).await,
            RepoCommand::GetCurrentRoot(args) => args.run(app, global).await,
            RepoCommand::GetRepositoryInfo(args) => args.run(app, global).await,
            RepoCommand::SetCurrentRoot(args) => args.run(app, global).await,
            RepoCommand::Exists(args) => args.run(app, global).await,
            RepoCommand::Read(args) => args.run(app, global).await,
            RepoCommand::Write(args) => args.run(app, global).await,
            RepoCommand::UploadFile(args) => args.run(app, global).await,
            RepoCommand::UploadDirectory(args) => args.run(app, global).await,
            RepoCommand::DownloadActions(args) => args.run(app, global).await,
        }
    }
}

// =============================================================================
// Initialize
// =============================================================================

/// Arguments for the initialize command.
#[derive(Args, Debug)]
pub struct InitializeArgs {
    #[command(flatten)]
    pub repo: RepoArgs,

    /// Name of the default branch.
    #[arg(long, default_value = "main")]
    pub default_branch_name: String,

    /// Repository UUID (generated if not specified).
    #[arg(long)]
    pub uuid: Option<String>,

    #[command(flatten)]
    pub output: OutputSink,
}

impl InitializeArgs {
    pub async fn run(self, app: &App, global: &GlobalArgs) -> Result<()> {
        let repo_ctx = self.repo.to_create_repo_context(true);
        let repo = app.create_repo(repo_ctx).await?;

        let init = RepoInitialize {
            uuid: self.uuid,
            default_branch_name: self.default_branch_name,
        };

        repo.initialize(init).await?;

        // Get and output the repository info
        let info = repo.get_repository_info().await?;

        if global.json {
            self.output
                .write(&RepositoryInfoOutput { uuid: info.uuid.clone() }, true)
                .await?;
        } else {
            self.output.write_str(&info.uuid).await?;
        }

        Ok(())
    }
}

// =============================================================================
// Get Current Root
// =============================================================================

/// Arguments for the get-current-root command.
#[derive(Args, Debug)]
pub struct GetCurrentRootArgs {
    #[command(flatten)]
    pub repo: RepoArgs,

    #[command(flatten)]
    pub output: OutputSink,
}

#[derive(Serialize)]
struct CurrentRootOutput {
    root: Option<String>,
}

impl GetCurrentRootArgs {
    pub async fn run(self, app: &App, global: &GlobalArgs) -> Result<()> {
        let repo_ctx = self.repo.to_create_repo_context(false);
        let repo = app.create_repo(repo_ctx).await?;

        let root = if repo.current_root_exists().await? {
            Some(repo.read_current_root().await?)
        } else {
            None
        };

        if global.json {
            self.output
                .write(&CurrentRootOutput { root }, true)
                .await?;
        } else {
            match root {
                Some(r) => self.output.write_str(&r).await?,
                None => self.output.write_str("(no root)").await?,
            }
        }

        Ok(())
    }
}

// =============================================================================
// Get Repository Info
// =============================================================================

/// Arguments for the get-repository-info command.
#[derive(Args, Debug)]
pub struct GetRepositoryInfoArgs {
    #[command(flatten)]
    pub repo: RepoArgs,

    #[command(flatten)]
    pub output: OutputSink,
}

#[derive(Serialize)]
struct RepositoryInfoOutput {
    uuid: String,
}

impl GetRepositoryInfoArgs {
    pub async fn run(self, app: &App, global: &GlobalArgs) -> Result<()> {
        let repo_ctx = self.repo.to_create_repo_context(false);
        let repo = app.create_repo(repo_ctx).await?;

        let info = repo.get_repository_info().await?;

        if global.json {
            self.output
                .write(&RepositoryInfoOutput { uuid: info.uuid.clone() }, true)
                .await?;
        } else {
            self.output.write_str(&info.uuid).await?;
        }

        Ok(())
    }
}

// =============================================================================
// Set Current Root
// =============================================================================

/// Arguments for the set-current-root command.
#[derive(Args, Debug)]
pub struct SetCurrentRootArgs {
    #[command(flatten)]
    pub repo: RepoArgs,

    /// The new root object ID.
    pub root: Option<String>,

    #[command(flatten)]
    pub input: InputSource,
}

impl SetCurrentRootArgs {
    pub async fn run(self, app: &App, global: &GlobalArgs) -> Result<()> {
        let repo_ctx = self.repo.to_create_repo_context(false);
        let repo = app.create_repo(repo_ctx).await?;

        let root_id = self.input.read(self.root.as_deref()).await?;
        repo.write_current_root(&root_id).await?;

        if global.json {
            println!("{{\"status\": \"ok\"}}");
        }

        Ok(())
    }
}

// =============================================================================
// Exists
// =============================================================================

/// Arguments for the exists command.
#[derive(Args, Debug)]
pub struct ExistsArgs {
    #[command(flatten)]
    pub repo: RepoArgs,

    /// The object ID to check.
    pub object_id: String,

    #[command(flatten)]
    pub output: OutputSink,
}

#[derive(Serialize)]
struct ExistsOutput {
    exists: bool,
}

impl ExistsArgs {
    pub async fn run(self, app: &App, global: &GlobalArgs) -> Result<()> {
        let repo_ctx = self.repo.to_create_repo_context(false);
        let repo = app.create_repo(repo_ctx).await?;

        let exists = repo.object_exists(&self.object_id).await?;

        if global.json {
            self.output.write(&ExistsOutput { exists }, true).await?;
        } else {
            self.output.write_str(if exists { "true" } else { "false" }).await?;
        }

        Ok(())
    }
}

// =============================================================================
// Read
// =============================================================================

/// Arguments for the read command.
#[derive(Args, Debug)]
pub struct ReadArgs {
    #[command(flatten)]
    pub repo: RepoArgs,

    /// The object ID to read.
    pub object_id: Option<String>,

    #[command(flatten)]
    pub input: InputSource,

    #[command(flatten)]
    pub output: OutputSink,
}

impl ReadArgs {
    pub async fn run(self, app: &App, global: &GlobalArgs) -> Result<()> {
        let repo_ctx = self.repo.to_create_repo_context(false);
        let repo = app.create_repo(repo_ctx).await?;

        let object_id = self.input.read(self.object_id.as_deref()).await?;
        let buffer = repo.read(&object_id, None).await?;
        let data: &[u8] = &buffer;

        if global.json {
            // Try to parse as JSON and pretty-print
            match serde_json::from_slice::<serde_json::Value>(data) {
                Ok(value) => {
                    let pretty = serde_json::to_string_pretty(&value)
                        .map_err(|e| CliError::Other(e.to_string()))?;
                    self.output.write_str(&pretty).await?;
                }
                Err(_) => {
                    // Not valid JSON, output as-is
                    self.output.write_bytes(data).await?;
                }
            }
        } else {
            self.output.write_bytes(data).await?;
        }

        Ok(())
    }
}

// =============================================================================
// Write
// =============================================================================

/// Arguments for the write command.
#[derive(Args, Debug)]
pub struct WriteArgs {
    #[command(flatten)]
    pub repo: RepoArgs,

    /// The contents to write.
    pub contents: Option<String>,

    #[command(flatten)]
    pub input: InputSource,

    #[command(flatten)]
    pub output: OutputSink,
}

#[derive(Serialize)]
struct WriteOutput {
    id: String,
}

impl WriteArgs {
    pub async fn run(self, app: &App, global: &GlobalArgs) -> Result<()> {
        let repo_ctx = self.repo.to_create_repo_context(false);
        let repo = app.create_repo(repo_ctx).await?;

        let contents = self.input.read(self.contents.as_deref()).await?;
        let data = contents.as_bytes();

        // Compute the object ID (SHA-256 hash)
        let mut hasher = Sha256::new();
        hasher.update(data);
        let hash = hasher.finalize();
        let id = hex::encode(hash);

        // Create a ManagedBuffer for the data
        let managed_buffers = ManagedBuffers::new();
        let buffer = managed_buffers.get_buffer_with_data(data.to_vec()).await;

        // Write to the repository
        let result = repo.write(&id, Arc::new(buffer)).await?;

        // Wait for write to complete
        result
            .complete
            .complete()
            .await
            .map_err(|e| CliError::Other(format!("write failed: {}", e)))?;

        if global.json {
            self.output.write(&WriteOutput { id }, true).await?;
        } else {
            self.output.write_str(&id).await?;
        }

        Ok(())
    }
}

// =============================================================================
// Upload File
// =============================================================================

/// Arguments for the upload-file command.
#[derive(Args, Debug)]
pub struct UploadFileArgs {
    #[command(flatten)]
    pub repo: RepoArgs,

    #[command(flatten)]
    pub file_store: FileStoreArgs,

    /// Path to the file within the file store.
    pub path: String,

    #[command(flatten)]
    pub output: OutputSink,
}

#[derive(Serialize)]
struct UploadFileOutput {
    hash: String,
}

impl UploadFileArgs {
    pub async fn run(self, app: &App, global: &GlobalArgs) -> Result<()> {
        let repo_ctx = self.repo.to_create_repo_context(false);
        let repo = app.create_repo(repo_ctx).await?;

        let fs_ctx = self.file_store.to_create_file_store_context();
        let file_store = app.create_file_store(fs_ctx).await?;

        let path = Path::new(&self.path);
        let result = upload_file(file_store.as_ref(), repo, path)
            .await
            .map_err(|e| CliError::Other(e.to_string()))?;

        // Wait for upload to complete
        result
            .complete
            .complete()
            .await
            .map_err(|e| CliError::Other(format!("upload failed: {}", e)))?;

        let hash = result.result.hash;

        if global.json {
            self.output
                .write(&UploadFileOutput { hash }, true)
                .await?;
        } else {
            self.output.write_str(&hash).await?;
        }

        Ok(())
    }
}

// =============================================================================
// Upload Directory
// =============================================================================

/// Arguments for the upload-directory command.
#[derive(Args, Debug)]
pub struct UploadDirectoryArgs {
    #[command(flatten)]
    pub repo: RepoArgs,

    #[command(flatten)]
    pub file_store: FileStoreArgs,

    /// Path to the directory within the file store. If not specified, uploads the entire file store.
    pub path: Option<String>,

    #[command(flatten)]
    pub output: OutputSink,
}

#[derive(Serialize)]
struct UploadDirectoryOutput {
    hash: String,
}

impl UploadDirectoryArgs {
    pub async fn run(self, app: &App, global: &GlobalArgs) -> Result<()> {
        let repo_ctx = self.repo.to_create_repo_context(false);
        let repo = app.create_repo(repo_ctx).await?;

        let fs_ctx = self.file_store.to_create_file_store_context();
        let file_store = app.create_file_store(fs_ctx).await?;

        // Get the cache from the file store
        let cache = file_store.get_cache();

        let path = self.path.as_deref().map(Path::new);
        let result = upload_directory(file_store.as_ref(), repo, cache, path)
            .await
            .map_err(|e| CliError::Other(e.to_string()))?;

        // Wait for upload to complete
        result
            .complete
            .complete()
            .await
            .map_err(|e| CliError::Other(format!("upload failed: {}", e)))?;

        let hash = result.result.hash;

        if global.json {
            self.output
                .write(&UploadDirectoryOutput { hash }, true)
                .await?;
        } else {
            self.output.write_str(&hash).await?;
        }

        Ok(())
    }
}

// =============================================================================
// Download Actions
// =============================================================================

/// Arguments for the download-actions command.
#[derive(Args, Debug)]
pub struct DownloadActionsArgs {
    #[command(flatten)]
    pub repo: RepoArgs,

    /// The directory object ID in the repository.
    pub directory_id: String,

    #[command(flatten)]
    pub file_store: FileStoreArgs,

    /// Path in the file store to compare against. If not specified, compares against the entire file store.
    pub path: Option<String>,

    #[command(flatten)]
    pub output: OutputSink,
}

#[derive(Serialize)]
struct DownloadActionsOutput {
    actions: Vec<DownloadActionOutput>,
}

#[derive(Serialize)]
struct DownloadActionOutput {
    action: String,
    name: Option<String>,
    object_id: Option<String>,
}

impl From<&DownloadAction> for DownloadActionOutput {
    fn from(action: &DownloadAction) -> Self {
        match action {
            DownloadAction::CreateDirectory(name) => DownloadActionOutput {
                action: "mkdir".to_string(),
                name: Some(name.clone()),
                object_id: None,
            },
            DownloadAction::RemoveDirectory(name) => DownloadActionOutput {
                action: "rmdir".to_string(),
                name: Some(name.clone()),
                object_id: None,
            },
            DownloadAction::RemoveFile(name) => DownloadActionOutput {
                action: "rm".to_string(),
                name: Some(name.clone()),
                object_id: None,
            },
            DownloadAction::EnterDirectory(name) => DownloadActionOutput {
                action: "enter".to_string(),
                name: Some(name.clone()),
                object_id: None,
            },
            DownloadAction::ExitDirectory => DownloadActionOutput {
                action: "exit".to_string(),
                name: None,
                object_id: None,
            },
            DownloadAction::DownloadFile(name, object_id) => DownloadActionOutput {
                action: "download".to_string(),
                name: Some(name.clone()),
                object_id: Some(object_id.clone()),
            },
        }
    }
}

impl DownloadActionsArgs {
    pub async fn run(self, app: &App, global: &GlobalArgs) -> Result<()> {
        let repo_ctx = self.repo.to_create_repo_context(false);
        let repo = app.create_repo(repo_ctx).await?;

        let fs_ctx = self.file_store.to_create_file_store_context();
        let file_store = app.create_file_store(fs_ctx).await?;

        let path = self.path.as_deref().map(Path::new).unwrap_or(Path::new(""));

        let mut actions_iter = download_actions(repo, self.directory_id, file_store.as_ref(), path)
            .await
            .map_err(|e| CliError::Other(e.to_string()))?;

        if global.json {
            // Collect all actions for JSON output
            let mut actions = Vec::new();
            while let Some(action) = actions_iter
                .next()
                .await
                .map_err(|e| CliError::Other(e.to_string()))?
            {
                actions.push(DownloadActionOutput::from(&action));
            }
            self.output
                .write(&DownloadActionsOutput { actions }, true)
                .await?;
        } else {
            // Output actions in plain text format with indentation
            let mut indent = 0usize;
            let mut output = String::new();

            while let Some(action) = actions_iter
                .next()
                .await
                .map_err(|e| CliError::Other(e.to_string()))?
            {
                let indent_str = " ".repeat(indent);
                match &action {
                    DownloadAction::CreateDirectory(name) => {
                        output.push_str(&format!("{}mkdir({})\n", indent_str, name));
                    }
                    DownloadAction::RemoveDirectory(name) => {
                        output.push_str(&format!("{}rmdir({})\n", indent_str, name));
                    }
                    DownloadAction::RemoveFile(name) => {
                        output.push_str(&format!("{}rm({})\n", indent_str, name));
                    }
                    DownloadAction::EnterDirectory(name) => {
                        output.push_str(&format!("{}enter({})\n", indent_str, name));
                        indent += 4;
                    }
                    DownloadAction::ExitDirectory => {
                        indent = indent.saturating_sub(4);
                        let indent_str = " ".repeat(indent);
                        output.push_str(&format!("{}exit\n", indent_str));
                    }
                    DownloadAction::DownloadFile(name, object_id) => {
                        output.push_str(&format!("{}download({}, {})\n", indent_str, name, object_id));
                    }
                }
            }

            // Remove trailing newline if present
            if output.ends_with('\n') {
                output.pop();
            }

            self.output.write_str(&output).await?;
        }

        Ok(())
    }
}
