//! Repository subcommands.

use std::sync::Arc;

use clap::{Args, Subcommand};
use serde::Serialize;
use sha2::{Digest, Sha256};

use std::path::{Path, PathBuf};

use crate::app::{upload_directory, upload_file, App, AppCreateFileStoreContext, AppCreateRepoContext};
use crate::cli::{
    create_command_context, CliError, CommandContextRequirements, GlobalArgs, InputSource,
    OutputSink, Result,
};
use crate::download::ActionCallback;
use crate::repo::RepoInitialize;

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

    /// Download a directory from the repository to a file store.
    #[command(name = "download-directory")]
    DownloadDirectory(DownloadDirectoryArgs),

    /// Download a file from the repository to a file store.
    #[command(name = "download-file")]
    DownloadFile(DownloadFileArgs),
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
            RepoCommand::DownloadDirectory(args) => args.run(app, global).await,
            RepoCommand::DownloadFile(args) => args.run(app, global).await,
        }
    }
}

// =============================================================================
// Initialize
// =============================================================================

/// Arguments for the initialize command.
#[derive(Args, Debug)]
pub struct InitializeArgs {
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
        let ctx = create_command_context(
            global.to_command_context_input(),
            CommandContextRequirements::new().with_repository(),
            app,
        )
        .await?;

        let repo_spec = ctx
            .repository_spec
            .ok_or_else(|| CliError::Other("repository required".to_string()))?;

        let repo_ctx = AppCreateRepoContext {
            spec: repo_spec,
            allow_uninitialized: true,
        };
        let repo = app.create_repo(repo_ctx).await?;

        let init = RepoInitialize {
            uuid: self.uuid,
            default_branch_name: self.default_branch_name,
        };

        repo.initialize(init).await?;

        // Get and output the repository info
        let info = repo.get_repository_info().await?;

        if ctx.json {
            self.output
                .write(
                    &RepositoryInfoOutput {
                        uuid: info.uuid.clone(),
                    },
                    true,
                )
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
    pub output: OutputSink,
}

#[derive(Serialize)]
struct CurrentRootOutput {
    root: Option<String>,
}

impl GetCurrentRootArgs {
    pub async fn run(self, app: &App, global: &GlobalArgs) -> Result<()> {
        let ctx = create_command_context(
            global.to_command_context_input(),
            CommandContextRequirements::new().with_repository(),
            app,
        )
        .await?;

        let repo_spec = ctx
            .repository_spec
            .ok_or_else(|| CliError::Other("repository required".to_string()))?;

        let repo_ctx = AppCreateRepoContext {
            spec: repo_spec,
            allow_uninitialized: false,
        };
        let repo = app.create_repo(repo_ctx).await?;

        let root = if repo.current_root_exists().await? {
            Some(repo.read_current_root().await?)
        } else {
            None
        };

        if ctx.json {
            self.output.write(&CurrentRootOutput { root }, true).await?;
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
    pub output: OutputSink,
}

#[derive(Serialize)]
struct RepositoryInfoOutput {
    uuid: String,
}

impl GetRepositoryInfoArgs {
    pub async fn run(self, app: &App, global: &GlobalArgs) -> Result<()> {
        let ctx = create_command_context(
            global.to_command_context_input(),
            CommandContextRequirements::new().with_repository(),
            app,
        )
        .await?;

        let repo_spec = ctx
            .repository_spec
            .ok_or_else(|| CliError::Other("repository required".to_string()))?;

        let repo_ctx = AppCreateRepoContext {
            spec: repo_spec,
            allow_uninitialized: false,
        };
        let repo = app.create_repo(repo_ctx).await?;

        let info = repo.get_repository_info().await?;

        if ctx.json {
            self.output
                .write(
                    &RepositoryInfoOutput {
                        uuid: info.uuid.clone(),
                    },
                    true,
                )
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
    /// The new root object ID.
    pub root: Option<String>,

    #[command(flatten)]
    pub input: InputSource,
}

impl SetCurrentRootArgs {
    pub async fn run(self, app: &App, global: &GlobalArgs) -> Result<()> {
        let ctx = create_command_context(
            global.to_command_context_input(),
            CommandContextRequirements::new().with_repository(),
            app,
        )
        .await?;

        let repo_spec = ctx
            .repository_spec
            .ok_or_else(|| CliError::Other("repository required".to_string()))?;

        let repo_ctx = AppCreateRepoContext {
            spec: repo_spec,
            allow_uninitialized: false,
        };
        let repo = app.create_repo(repo_ctx).await?;

        let root_id = self.input.read(self.root.as_deref()).await?;
        repo.write_current_root(&root_id).await?;

        if ctx.json {
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
        let ctx = create_command_context(
            global.to_command_context_input(),
            CommandContextRequirements::new().with_repository(),
            app,
        )
        .await?;

        let repo_spec = ctx
            .repository_spec
            .ok_or_else(|| CliError::Other("repository required".to_string()))?;

        let repo_ctx = AppCreateRepoContext {
            spec: repo_spec,
            allow_uninitialized: false,
        };
        let repo = app.create_repo(repo_ctx).await?;

        let exists = repo.object_exists(&self.object_id).await?;

        if ctx.json {
            self.output.write(&ExistsOutput { exists }, true).await?;
        } else {
            self.output
                .write_str(if exists { "true" } else { "false" })
                .await?;
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
    /// The object ID to read.
    pub object_id: Option<String>,

    #[command(flatten)]
    pub input: InputSource,

    #[command(flatten)]
    pub output: OutputSink,
}

impl ReadArgs {
    pub async fn run(self, app: &App, global: &GlobalArgs) -> Result<()> {
        let ctx = create_command_context(
            global.to_command_context_input(),
            CommandContextRequirements::new().with_repository(),
            app,
        )
        .await?;

        let repo_spec = ctx
            .repository_spec
            .ok_or_else(|| CliError::Other("repository required".to_string()))?;

        let repo_ctx = AppCreateRepoContext {
            spec: repo_spec,
            allow_uninitialized: false,
        };
        let repo = app.create_repo(repo_ctx).await?;

        let object_id = self.input.read(self.object_id.as_deref()).await?;
        let buffer = repo.read(&object_id, None).await?;
        let data: &[u8] = &buffer;

        if ctx.json {
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
        let ctx = create_command_context(
            global.to_command_context_input(),
            CommandContextRequirements::new().with_repository(),
            app,
        )
        .await?;

        let repo_spec = ctx
            .repository_spec
            .ok_or_else(|| CliError::Other("repository required".to_string()))?;

        let repo_ctx = AppCreateRepoContext {
            spec: repo_spec,
            allow_uninitialized: false,
        };
        let repo = app.create_repo(repo_ctx).await?;

        let contents = self.input.read(self.contents.as_deref()).await?;
        let data = contents.as_bytes();

        // Compute the object ID (SHA-256 hash)
        let mut hasher = Sha256::new();
        hasher.update(data);
        let hash = hasher.finalize();
        let id = hex::encode(hash);

        // Create a ManagedBuffer for the data
        let buffer = app
            .managed_buffers()
            .get_buffer_with_data(data.to_vec())
            .await;

        // Write to the repository
        let result = repo.write(&id, Arc::new(buffer)).await?;

        // Wait for write to complete
        result
            .complete
            .complete()
            .await
            .map_err(|e| CliError::Other(format!("write failed: {}", e)))?;

        if ctx.json {
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
        let ctx = create_command_context(
            global
                .to_command_context_input()
                .with_file_store_path(Some(self.path.clone())),
            CommandContextRequirements::new()
                .with_repository()
                .with_file_store_path(),
            app,
        )
        .await?;

        let repo_spec = ctx
            .repository_spec
            .clone()
            .ok_or_else(|| CliError::Other("repository required".to_string()))?;
        let fs_spec = ctx
            .file_store_spec
            .clone()
            .ok_or_else(|| CliError::Other("file store required".to_string()))?;
        let resolved_path = ctx
            .file_store_path
            .clone()
            .ok_or_else(|| CliError::Other("file store path required".to_string()))?;

        let repo_ctx = AppCreateRepoContext {
            spec: repo_spec,
            allow_uninitialized: false,
        };
        let repo = app.create_repo(repo_ctx).await?;

        let fs_ctx = AppCreateFileStoreContext { spec: fs_spec };
        let file_store = app.create_file_store(fs_ctx).await?;

        let path = Path::new(&resolved_path);

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

        if ctx.json {
            self.output.write(&UploadFileOutput { hash }, true).await?;
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
    /// Path to the directory within the file store. If not specified, uses the current directory.
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
        let ctx = create_command_context(
            global
                .to_command_context_input()
                .with_file_store_path(self.path.clone()),
            CommandContextRequirements::new()
                .with_repository()
                .with_file_store_path(),
            app,
        )
        .await?;

        let repo_spec = ctx
            .repository_spec
            .clone()
            .ok_or_else(|| CliError::Other("repository required".to_string()))?;
        let fs_spec = ctx
            .file_store_spec
            .clone()
            .ok_or_else(|| CliError::Other("file store required".to_string()))?;
        let resolved_path = ctx
            .file_store_path
            .clone()
            .ok_or_else(|| CliError::Other("file store path required".to_string()))?;

        let repo_ctx = AppCreateRepoContext {
            spec: repo_spec,
            allow_uninitialized: false,
        };
        let repo = app.create_repo(repo_ctx).await?;

        let fs_ctx = AppCreateFileStoreContext { spec: fs_spec };
        let file_store = app.create_file_store(fs_ctx).await?;

        // Get the cache from the file store
        let cache = file_store.get_cache();

        let path = if resolved_path == "/" {
            None
        } else {
            Some(Path::new(&resolved_path))
        };

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

        if ctx.json {
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
// Download Directory
// =============================================================================

/// Arguments for the download-directory command.
#[derive(Args, Debug)]
pub struct DownloadDirectoryArgs {
    /// The directory object ID to download.
    pub directory_object_id: String,

    /// Path within the file store to download to.
    pub path: Option<String>,

    /// Perform a dry run without making changes.
    #[arg(long, short = 'n')]
    pub dry_run: bool,

    /// Print actions that would be taken.
    #[arg(long, short = 'v')]
    pub verbose: bool,

    /// Download without using a staging area.
    #[arg(long)]
    pub no_stage: bool,

    #[command(flatten)]
    pub output: OutputSink,
}

impl DownloadDirectoryArgs {
    pub async fn run(self, app: &App, global: &GlobalArgs) -> Result<()> {
        let ctx = create_command_context(
            global
                .to_command_context_input()
                .with_file_store_path(self.path.clone()),
            CommandContextRequirements::new()
                .with_repository()
                .with_file_store_path(),
            app,
        )
        .await?;

        let repo_spec = ctx
            .repository_spec
            .clone()
            .ok_or_else(|| CliError::Other("repository required".to_string()))?;
        let fs_spec = ctx
            .file_store_spec
            .clone()
            .ok_or_else(|| CliError::Other("file store required".to_string()))?;
        let resolved_path = ctx
            .file_store_path
            .clone()
            .ok_or_else(|| CliError::Other("file store path required".to_string()))?;

        let store_path = if resolved_path == "/" {
            PathBuf::new()
        } else {
            PathBuf::from(&resolved_path)
        };

        // Determine if we need verbose output
        let on_action = if self.verbose || self.dry_run || ctx.json {
            Some(self.create_action_callback(&ctx)?)
        } else {
            None
        };

        let repo_ctx = AppCreateRepoContext {
            spec: repo_spec,
            allow_uninitialized: false,
        };
        let repo = app.create_repo(repo_ctx).await?;

        let fs_ctx = AppCreateFileStoreContext { spec: fs_spec };
        let file_store = app.create_file_store(fs_ctx).await?;

        // Use download_directory for all cases - it handles dry_run and verbose internally
        let with_stage = !self.no_stage;
        let result = crate::download::download_directory(
            repo,
            &self.directory_object_id,
            file_store.as_ref(),
            store_path,
            with_stage,
            self.dry_run,
            on_action,
        )
        .await
        .map_err(|e| CliError::Other(e.to_string()))?;

        result
            .complete
            .complete()
            .await
            .map_err(|e| CliError::Other(format!("download failed: {e}")))?;

        Ok(())
    }

    fn create_action_callback(
        &self,
        ctx: &crate::cli::CommandContext,
    ) -> Result<ActionCallback> {
        use std::io::Write;
        use std::sync::Mutex;

        let writer: Arc<Mutex<Box<dyn Write + Send>>> = match &self.output.file {
            Some(path) => {
                let file = std::fs::File::create(path)
                    .map_err(|e| CliError::Other(format!("failed to create output file: {e}")))?;
                Arc::new(Mutex::new(Box::new(std::io::BufWriter::new(file))))
            }
            None => Arc::new(Mutex::new(Box::new(std::io::stdout()))),
        };

        let on_action: ActionCallback = if ctx.json {
            let w = writer.clone();
            Arc::new(move |s: &str| {
                if let Ok(encoded) = serde_json::to_string(s) {
                    let mut w = w.lock().unwrap();
                    let _ = writeln!(w, "{encoded}");
                }
            })
        } else {
            let w = writer.clone();
            Arc::new(move |s: &str| {
                let mut w = w.lock().unwrap();
                let _ = writeln!(w, "{s}");
            })
        };

        Ok(on_action)
    }
}

// =============================================================================
// download-file command
// =============================================================================

/// Arguments for the download-file command.
#[derive(Args, Debug)]
pub struct DownloadFileArgs {
    /// The file object ID to download.
    pub file_object_id: String,

    /// Path within the file store to download to.
    pub path: String,
}

impl DownloadFileArgs {
    pub async fn run(self, app: &App, global: &GlobalArgs) -> Result<()> {
        let ctx = create_command_context(
            global
                .to_command_context_input()
                .with_file_store_path(Some(self.path.clone())),
            CommandContextRequirements::new()
                .with_repository()
                .with_file_store_path(),
            app,
        )
        .await?;

        let repo_spec = ctx
            .repository_spec
            .clone()
            .ok_or_else(|| CliError::Other("repository required".to_string()))?;
        let fs_spec = ctx
            .file_store_spec
            .clone()
            .ok_or_else(|| CliError::Other("file store required".to_string()))?;
        let resolved_path = ctx
            .file_store_path
            .clone()
            .ok_or_else(|| CliError::Other("file store path required".to_string()))?;

        let path = PathBuf::from(&resolved_path);

        let repo_ctx = AppCreateRepoContext {
            spec: repo_spec,
            allow_uninitialized: false,
        };
        let repo = app.create_repo(repo_ctx).await?;

        let fs_ctx = AppCreateFileStoreContext { spec: fs_spec };
        let file_store = app.create_file_store(fs_ctx).await?;

        // Download the file (default to not executable - the file metadata will
        // be determined from the File object in the repo)
        let result = crate::download::download_file(
            repo,
            &self.file_object_id,
            file_store.as_ref(),
            &path,
            false, // executable bit - will be set based on file metadata if needed
        )
        .await
        .map_err(|e| CliError::Other(e.to_string()))?;

        // Wait for the download to complete
        result
            .complete
            .complete()
            .await
            .map_err(|e| CliError::Other(format!("download failed: {e}")))?;

        Ok(())
    }
}
