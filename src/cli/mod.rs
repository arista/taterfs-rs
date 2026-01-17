//! Command-line interface for taterfs.

pub mod args;
mod commands;

use clap::{Parser, Subcommand};
use thiserror::Error;

use crate::app::{App, AppError};

pub use args::{GlobalArgs, RepoArgs, FileStoreArgs, InputSource, OutputSink};

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
                    Command::Repo { command } => command.run(app, &global).await,
                    Command::FileStore { command } => command.run(app, &global).await,
                    Command::KeyValueCache { command } => command.run(app, &global).await,
                }
            })
        })
        .await
    }
}

/// Main entry point for the CLI.
pub async fn main() -> Result<()> {
    let cli = Cli::parse_args();
    cli.run().await
}
