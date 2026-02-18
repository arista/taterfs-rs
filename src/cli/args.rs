//! Command-line argument definitions and helpers.

use std::path::PathBuf;

use clap::Args;
use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::app::AppContext;
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

    /// Repository specification (URL or named repository).
    /// If not specified, auto-discovered from filestore's sync state.
    #[arg(long = "repository", global = true)]
    pub repository: Option<String>,

    /// File store specification (URL or named file store).
    /// If not specified, auto-discovered by searching for .tfs/ directory.
    #[arg(long = "filestore", global = true)]
    pub filestore: Option<String>,
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

    /// Convert to a CommandContextInput for command context creation.
    pub fn to_command_context_input(&self) -> crate::cli::command_context::CommandContextInput {
        crate::cli::command_context::CommandContextInput {
            config_file: self.config_file.clone(),
            config_file_overrides: self.config_file_overrides.clone(),
            config: self.config_overrides.clone(),
            json: self.json,
            no_cache: self.no_cache,
            repository_spec: self.repository.clone(),
            file_store_spec: self.filestore.clone(),
            file_store_path: None,
            repository_path: None,
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
