//! Key-value cache subcommands.

use clap::{Args, Subcommand};

use crate::app::App;
use crate::cli::{CliError, GlobalArgs, OutputSink, Result};

// =============================================================================
// KeyValueCache Subcommands
// =============================================================================

/// Key-value cache subcommands.
#[derive(Subcommand, Debug)]
pub enum KeyValueCacheCommand {
    /// List entries in the cache.
    #[command(name = "list-entries")]
    ListEntries(ListEntriesArgs),
}

impl KeyValueCacheCommand {
    /// Run the key-value-cache subcommand.
    pub async fn run(self, app: &App, global: &GlobalArgs) -> Result<()> {
        match self {
            KeyValueCacheCommand::ListEntries(args) => args.run(app, global).await,
        }
    }
}

// =============================================================================
// ListEntries
// =============================================================================

/// Arguments for the list-entries command.
#[derive(Args, Debug)]
pub struct ListEntriesArgs {
    /// Optional prefix to filter entries.
    pub prefix: Option<String>,

    #[command(flatten)]
    pub output: OutputSink,
}

impl ListEntriesArgs {
    pub async fn run(self, app: &App, _global: &GlobalArgs) -> Result<()> {
        let db = app
            .key_value_db()
            .ok_or_else(|| CliError::Other("caching is disabled (--no-cache)".to_string()))?;

        let prefix = self.prefix.as_deref().unwrap_or("");
        let mut entries = db
            .list_entries(prefix.as_bytes())
            .await
            .map_err(|e| CliError::Other(e.to_string()))?;

        let mut output = String::new();

        while let Some(entry) = entries
            .next()
            .await
            .map_err(|e| CliError::Other(e.to_string()))?
        {
            let key_str = entry.key_string().unwrap_or("<invalid utf-8>");
            let value_str = entry.value_string().unwrap_or("<invalid utf-8>");
            output.push_str(&format!("{} -> {}\n", key_str, value_str));
        }

        // Remove trailing newline for consistent output
        let output = output.trim_end();
        if !output.is_empty() {
            self.output.write_str(output).await?;
        }

        Ok(())
    }
}
