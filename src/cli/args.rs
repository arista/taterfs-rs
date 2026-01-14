//! Command-line argument definitions and helpers.

use std::path::PathBuf;

use clap::Args;
use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::app::{AppContext, AppCreateFileStoreContext, AppCreateRepoContext};
use crate::config::ConfigSource;

// =============================================================================
// Error Types
// =============================================================================

/// Errors that can occur during argument processing.
#[derive(Debug, Error)]
pub enum ArgsError {
    /// I/O error reading or writing.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// Invalid argument combination.
    #[error("{0}")]
    InvalidArgs(String),

    /// JSON serialization error.
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
}

/// Result type for argument operations.
pub type Result<T> = std::result::Result<T, ArgsError>;

// =============================================================================
// Global Arguments
// =============================================================================

/// Global arguments that apply to all commands.
#[derive(Args, Debug, Default)]
pub struct GlobalArgs {
    /// Path to the main configuration file.
    #[arg(long = "config-file", global = true)]
    pub config_file: Option<PathBuf>,

    /// Path to the configuration overrides file.
    #[arg(long = "config-file-overrides", global = true)]
    pub config_file_overrides: Option<PathBuf>,

    /// Configuration overrides in the form name=value.
    #[arg(long = "config", value_parser = parse_config_override, global = true)]
    pub config_overrides: Vec<(String, String)>,

    /// Format output as JSON.
    #[arg(long, global = true)]
    pub json: bool,

    /// Disable caching.
    #[arg(long = "no-cache", global = true)]
    pub no_cache: bool,
}

impl GlobalArgs {
    /// Convert to a ConfigSource for reading configuration.
    pub fn to_config_source(&self) -> ConfigSource {
        ConfigSource {
            config_file: self.config_file.clone(),
            override_file: self.config_file_overrides.clone(),
            overrides: self.config_overrides.clone(),
        }
    }

    /// Convert to an AppContext for creating an App.
    pub fn to_app_context(&self) -> AppContext {
        AppContext {
            config_source: self.to_config_source(),
        }
    }
}

/// Parse a config override from "name=value" format.
fn parse_config_override(s: &str) -> std::result::Result<(String, String), String> {
    let (name, value) = s
        .split_once('=')
        .ok_or_else(|| format!("invalid config override '{}': expected name=value", s))?;
    Ok((name.to_string(), value.to_string()))
}

// =============================================================================
// Repo Arguments
// =============================================================================

/// Arguments common to repository commands.
#[derive(Args, Debug)]
pub struct RepoArgs {
    /// Repository specification (URL or named repository).
    pub repo_spec: String,
}

impl RepoArgs {
    /// Convert to an AppCreateRepoContext.
    pub fn to_create_repo_context(&self, allow_uninitialized: bool) -> AppCreateRepoContext {
        AppCreateRepoContext {
            spec: self.repo_spec.clone(),
            allow_uninitialized,
        }
    }
}

// =============================================================================
// File Store Arguments
// =============================================================================

/// Arguments common to file store commands.
#[derive(Args, Debug)]
pub struct FileStoreArgs {
    /// File store specification (URL or named file store).
    pub file_store_spec: String,
}

impl FileStoreArgs {
    /// Convert to an AppCreateFileStoreContext.
    pub fn to_create_file_store_context(&self) -> AppCreateFileStoreContext {
        AppCreateFileStoreContext {
            spec: self.file_store_spec.clone(),
        }
    }
}

// =============================================================================
// Input/Output Helpers
// =============================================================================

/// Helper for commands that read input from an argument, file, or stdin.
#[derive(Args, Debug, Default)]
pub struct InputSource {
    /// Read input from this file instead of an argument.
    #[arg(id = "input_file", short = 'f', long = "input-file")]
    pub file: Option<PathBuf>,
}

impl InputSource {
    /// Read the input value.
    ///
    /// If `arg_value` is provided, returns it (error if file is also set).
    /// If `file` is set, reads from the file.
    /// Otherwise, reads from stdin.
    pub async fn read(&self, arg_value: Option<&str>) -> Result<String> {
        match (arg_value, &self.file) {
            (Some(_), Some(_)) => Err(ArgsError::InvalidArgs(
                "cannot specify both a value argument and --input-file".to_string(),
            )),
            (Some(value), None) => Ok(value.to_string()),
            (None, Some(path)) => {
                let contents = tokio::fs::read_to_string(path).await?;
                Ok(contents.trim().to_string())
            }
            (None, None) => {
                let mut contents = String::new();
                tokio::io::stdin().read_to_string(&mut contents).await?;
                Ok(contents.trim().to_string())
            }
        }
    }

    /// Read the input value, requiring either an argument or file (no stdin fallback).
    pub async fn read_required(&self, arg_value: Option<&str>) -> Result<String> {
        match (arg_value, &self.file) {
            (Some(_), Some(_)) => Err(ArgsError::InvalidArgs(
                "cannot specify both a value argument and --input-file".to_string(),
            )),
            (Some(value), None) => Ok(value.to_string()),
            (None, Some(path)) => {
                let contents = tokio::fs::read_to_string(path).await?;
                Ok(contents.trim().to_string())
            }
            (None, None) => Err(ArgsError::InvalidArgs(
                "must specify either a value argument or --input-file".to_string(),
            )),
        }
    }
}

/// Helper for commands that write output to a file or stdout.
#[derive(Args, Debug, Default)]
pub struct OutputSink {
    /// Write output to this file instead of stdout.
    #[arg(id = "output_file", short = 'o', long = "output-file")]
    pub file: Option<PathBuf>,
}

impl OutputSink {
    /// Write a string value to the output.
    pub async fn write_str(&self, value: &str) -> Result<()> {
        match &self.file {
            Some(path) => {
                tokio::fs::write(path, value).await?;
            }
            None => {
                tokio::io::stdout().write_all(value.as_bytes()).await?;
                tokio::io::stdout().write_all(b"\n").await?;
            }
        }
        Ok(())
    }

    /// Write a value to the output, optionally as JSON.
    pub async fn write<T: serde::Serialize>(&self, value: &T, json: bool) -> Result<()> {
        let output = if json {
            serde_json::to_string_pretty(value)?
        } else {
            // For non-JSON, try to convert to string sensibly
            serde_json::to_value(value)
                .map(|v| match v {
                    serde_json::Value::String(s) => s,
                    other => other.to_string(),
                })
                .unwrap_or_default()
        };
        self.write_str(&output).await
    }

    /// Write raw bytes to the output.
    pub async fn write_bytes(&self, data: &[u8]) -> Result<()> {
        match &self.file {
            Some(path) => {
                tokio::fs::write(path, data).await?;
            }
            None => {
                tokio::io::stdout().write_all(data).await?;
            }
        }
        Ok(())
    }
}
