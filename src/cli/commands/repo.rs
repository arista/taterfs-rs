//! Repository subcommands.

use std::sync::Arc;

use clap::{Args, Subcommand};
use serde::Serialize;
use sha2::{Digest, Sha256};

use crate::app::App;
use crate::cli::{CliError, GlobalArgs, InputSource, OutputSink, RepoArgs, Result};
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
    pub default_branch: String,
}

impl InitializeArgs {
    pub async fn run(self, app: &App, global: &GlobalArgs) -> Result<()> {
        let repo_ctx = self.repo.to_create_repo_context(true);
        let repo = app.create_repo(repo_ctx).await?;

        let init = RepoInitialize {
            uuid: None,
            default_branch_name: self.default_branch,
        };

        repo.initialize(init).await?;

        if global.json {
            println!("{{\"status\": \"initialized\"}}");
        } else {
            println!("Repository initialized");
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
    pub object_id: String,

    #[command(flatten)]
    pub output: OutputSink,
}

impl ReadArgs {
    pub async fn run(self, app: &App, global: &GlobalArgs) -> Result<()> {
        let repo_ctx = self.repo.to_create_repo_context(false);
        let repo = app.create_repo(repo_ctx).await?;

        let buffer = repo.read(&self.object_id, None).await?;
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
