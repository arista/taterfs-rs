//! Command-line interface for taterfs.

pub mod args;
pub mod command_context;
mod commands;

use clap::{Parser, Subcommand};
use thiserror::Error;

use crate::app::{App, AppError};

pub use args::{GlobalArgs, InputSource, OutputSink};
pub use command_context::{
    create_command_context, CommandContext, CommandContextError, CommandContextInput,
    CommandContextRequirements,
};

// =============================================================================
// Error Types
// =============================================================================

/// Errors that can occur during CLI execution.
#[derive(Debug, Error)]
pub enum CliError {
    /// Argument processing error.
    #[error("{0}")]
    Args(#[from] args::ArgsError),

    /// App error.
    #[error("{0}")]
    App(#[from] AppError),

    /// Repository error.
    #[error("{0}")]
    Repo(#[from] crate::repo::RepoError),

    /// File store error.
    #[error("{0}")]
    FileStore(#[from] crate::file_store::Error),

    /// Command context error.
    #[error("{0}")]
    CommandContext(#[from] CommandContextError),

    /// I/O error.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// Other error.
    #[error("{0}")]
    Other(String),
}

/// Result type for CLI operations.
pub type Result<T> = std::result::Result<T, CliError>;

// =============================================================================
// CLI Definition
// =============================================================================

/// taterfs - A file storage utility.
#[derive(Parser, Debug)]
#[command(name = "tfs", version, about, long_about = None)]
pub struct Cli {
    #[command(flatten)]
    pub global: GlobalArgs,

    #[command(subcommand)]
    pub command: Command,
}

/// Top-level commands.
#[derive(Subcommand, Debug)]
pub enum Command {
    /// List directory contents.
    #[command(name = "ls")]
    Ls {
        /// Path within the repository to list.
        #[arg(default_value = "/")]
        path: String,

        /// Use long listing format.
        #[arg(short = 'l', long)]
        long: bool,

        /// Force single-column output.
        #[arg(short = '1')]
        single_column: bool,

        /// Sort entries horizontally in multi-column output.
        #[arg(short = 'x')]
        horizontal_sort: bool,

        /// Manually specify screen width.
        #[arg(short = 'w', long = "width")]
        width: Option<usize>,

        /// Append indicator (/ for directories, * for executables).
        #[arg(short = 'F', long = "classify")]
        classify: bool,

        /// Include object IDs in output.
        #[arg(long = "include-id")]
        include_id: bool,
    },

    /// Upload a directory from a filestore to a repository.
    #[command(name = "upload-directory")]
    UploadDirectory {
        /// Path within the filestore to upload.
        filestore_path: String,
        /// Path within the repository to upload to.
        repo_path: String,
    },

    /// Repository operations.
    Repo {
        #[command(subcommand)]
        command: commands::repo::RepoCommand,
    },

    /// File store operations.
    #[command(name = "file-store")]
    FileStore {
        #[command(subcommand)]
        command: commands::file_store::FileStoreCommand,
    },

    /// Key-value cache operations.
    #[command(name = "key-value-cache")]
    KeyValueCache {
        #[command(subcommand)]
        command: commands::key_value_cache::KeyValueCacheCommand,
    },
}

// =============================================================================
// CLI Execution
// =============================================================================

impl Cli {
    /// Parse command-line arguments and return the CLI instance.
    pub fn parse_args() -> Self {
        Cli::parse()
    }

    /// Run the CLI command.
    pub async fn run(self) -> Result<()> {
        let global = self.global;
        let command = self.command;

        App::with_app(global.to_app_context(), |app| {
            Box::pin(async move {
                match command {
                    Command::Ls {
                        path,
                        long,
                        single_column,
                        horizontal_sort,
                        width,
                        classify,
                        include_id,
                    } => {
                        run_ls(
                            app,
                            &global,
                            path,
                            long,
                            single_column,
                            horizontal_sort,
                            width,
                            classify,
                            include_id,
                        )
                        .await
                    }
                    Command::UploadDirectory {
                        filestore_path,
                        repo_path,
                    } => {
                        run_upload_directory(app, &global, filestore_path, repo_path).await
                    }
                    Command::Repo { command } => command.run(app, &global).await,
                    Command::FileStore { command } => command.run(app, &global).await,
                    Command::KeyValueCache { command } => command.run(app, &global).await,
                }
            })
        })
        .await
    }
}

/// Run the upload-directory command.
async fn run_upload_directory(
    app: &App,
    global: &GlobalArgs,
    filestore_path: String,
    repo_path: String,
) -> Result<()> {
    use std::path::PathBuf;

    // Create command context with requirements
    let requirements = CommandContextRequirements::new()
        .with_repository()
        .with_file_store()
        .with_branch()
        .with_commit_metadata();

    let input = global
        .to_command_context_input()
        .with_file_store_path(Some(filestore_path.clone()));

    let context = create_command_context(input, requirements, app).await?;

    // Run the command
    let args = crate::commands::UploadDirectoryArgs {
        filestore_path: PathBuf::from(&filestore_path),
        repo_path: PathBuf::from(&repo_path),
    };

    let result = crate::commands::upload_directory(args, &context, app)
        .await
        .map_err(|e| CliError::Other(e.to_string()))?;

    // Output the result
    if context.json {
        println!("{}", serde_json::to_string_pretty(&result).unwrap_or_default());
    } else {
        println!("{}", result);
    }

    Ok(())
}

/// Run the ls command.
#[allow(clippy::too_many_arguments)]
async fn run_ls(
    app: &App,
    global: &GlobalArgs,
    path: String,
    long: bool,
    single_column: bool,
    horizontal_sort: bool,
    width: Option<usize>,
    classify: bool,
    include_id: bool,
) -> Result<()> {
    // Create command context with requirements
    let requirements = CommandContextRequirements::new()
        .with_commit()
        .with_repository_path();

    let input = global
        .to_command_context_input()
        .with_repository_path(Some(path.clone()));

    let context = create_command_context(input, requirements, app).await?;

    // Determine format based on flags
    let format = if global.json {
        crate::commands::ListFormat::Json(crate::commands::ListJsonFormat { include_id })
    } else if long {
        crate::commands::ListFormat::Long(crate::commands::ListLongFormat { include_id })
    } else {
        let columns_spec = if single_column {
            crate::commands::ListColumnsSpec::Single
        } else if horizontal_sort {
            crate::commands::ListColumnsSpec::MultiHorizontalSort
        } else {
            crate::commands::ListColumnsSpec::MultiVerticalSort
        };
        crate::commands::ListFormat::Short(crate::commands::ListShortFormat {
            width,
            columns_spec,
        })
    };

    // Build command args
    let args = crate::commands::ListCommandArgs {
        path: context.repository_path.clone().unwrap_or_else(|| "/".to_string()),
        format,
        classify_format: classify,
    };

    // Run the command
    crate::commands::list(args, &context, app)
        .await
        .map_err(|e| CliError::Other(e.to_string()))?;

    Ok(())
}

/// Main entry point for the CLI.
pub async fn main() -> Result<()> {
    let cli = Cli::parse_args();
    cli.run().await
}
