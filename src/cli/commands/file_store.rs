//! File store subcommands.

use std::path::Path;

use clap::{Args, Subcommand};

use crate::app::{App, AppCreateFileStoreContext};
use crate::cli::{
    create_command_context, CliError, CommandContextRequirements, GlobalArgs, InputSource,
    OutputSink, Result,
};
use crate::file_store::ScanEvent;

// =============================================================================
// FileStore Subcommands
// =============================================================================

/// File store subcommands.
#[derive(Subcommand, Debug)]
pub enum FileStoreCommand {
    /// Scan a file store and print its contents.
    Scan(ScanArgs),

    /// Get source chunks for a file.
    #[command(name = "source-chunks")]
    SourceChunks(SourceChunksArgs),
}

impl FileStoreCommand {
    /// Run the file-store subcommand.
    pub async fn run(self, app: &App, global: &GlobalArgs) -> Result<()> {
        match self {
            FileStoreCommand::Scan(args) => args.run(app, global).await,
            FileStoreCommand::SourceChunks(args) => args.run(app, global).await,
        }
    }
}

// =============================================================================
// Scan
// =============================================================================

/// Arguments for the scan command.
#[derive(Args, Debug)]
pub struct ScanArgs {
    /// Optional path to scan from within the file store.
    pub path: Option<String>,

    #[command(flatten)]
    pub output: OutputSink,
}

impl ScanArgs {
    pub async fn run(self, app: &App, global: &GlobalArgs) -> Result<()> {
        let ctx = create_command_context(
            global
                .to_command_context_input()
                .with_file_store_path(self.path.clone()),
            CommandContextRequirements::new().with_file_store_path(),
            app,
        )
        .await?;

        let fs_spec = ctx
            .file_store_spec
            .clone()
            .ok_or_else(|| CliError::Other("file store required".to_string()))?;
        let resolved_path = ctx
            .file_store_path
            .clone()
            .ok_or_else(|| CliError::Other("file store path required".to_string()))?;

        let fs_ctx = AppCreateFileStoreContext { spec: fs_spec };
        let file_store = app.create_file_store(fs_ctx).await?;

        let source = file_store
            .get_source()
            .ok_or_else(|| CliError::Other("file store does not support reading".to_string()))?;

        let scan_path = if resolved_path == "/" {
            None
        } else {
            Some(resolved_path)
        };

        let mut events = source
            .scan(scan_path.as_ref().map(|s| Path::new(s.as_str())))
            .await
            .map_err(|e| CliError::Other(e.to_string()))?;

        let mut output = String::new();
        let mut indent = 0usize;

        while let Some(event_result) = events.next().await {
            let event = event_result.map_err(|e| CliError::Other(e.to_string()))?;

            match event {
                ScanEvent::EnterDirectory(dir) => {
                    output.push_str(&" ".repeat(indent));
                    output.push_str(&dir.name);
                    output.push_str("/\n");
                    indent += 4;
                }
                ScanEvent::ExitDirectory => {
                    indent = indent.saturating_sub(4);
                }
                ScanEvent::File(file) => {
                    output.push_str(&" ".repeat(indent));
                    output.push_str(&file.name);
                    output.push_str(&format!(
                        " ({} bytes, {}, fingerprint: {})\n",
                        file.size,
                        if file.executable { "x" } else { "-" },
                        file.fingerprint.as_deref().unwrap_or("-")
                    ));
                }
            }
        }

        // Remove trailing newline for consistent output
        let output = output.trim_end();
        self.output.write_str(output).await?;

        Ok(())
    }
}

// =============================================================================
// SourceChunks
// =============================================================================

/// Arguments for the source_chunks command.
#[derive(Args, Debug)]
pub struct SourceChunksArgs {
    /// The path to the file.
    pub path: Option<String>,

    #[command(flatten)]
    pub input: InputSource,

    #[command(flatten)]
    pub output: OutputSink,
}

impl SourceChunksArgs {
    pub async fn run(self, app: &App, global: &GlobalArgs) -> Result<()> {
        // Get the path from argument or input first
        let path_str = self.input.read(self.path.as_deref()).await?;

        let ctx = create_command_context(
            global
                .to_command_context_input()
                .with_file_store_path(Some(path_str)),
            CommandContextRequirements::new().with_file_store_path(),
            app,
        )
        .await?;

        let fs_spec = ctx
            .file_store_spec
            .clone()
            .ok_or_else(|| CliError::Other("file store required".to_string()))?;
        let resolved_path = ctx
            .file_store_path
            .clone()
            .ok_or_else(|| CliError::Other("file store path required".to_string()))?;

        let fs_ctx = AppCreateFileStoreContext { spec: fs_spec };
        let file_store = app.create_file_store(fs_ctx).await?;

        let source = file_store
            .get_source()
            .ok_or_else(|| CliError::Other("file store does not support reading".to_string()))?;

        let path = Path::new(&resolved_path);

        let mut contents = source
            .get_source_chunks_with_content(path)
            .await
            .map_err(|e| CliError::Other(e.to_string()))?
            .ok_or_else(|| CliError::Other(format!("path not found: {}", resolved_path)))?;

        let mut output = String::new();
        let mut total_size = 0u64;

        while let Some(chunk_result) = contents.next().await {
            let chunk = chunk_result.map_err(|e| CliError::Other(e.to_string()))?;
            let content = chunk
                .content()
                .await
                .map_err(|e| CliError::Other(e.to_string()))?;
            output.push_str(&format!(
                "{}+{}, hash: {}\n",
                content.offset, content.size, content.hash
            ));
            total_size += content.size;
        }

        output.push_str(&format!("total: {}", total_size));

        self.output.write_str(&output).await?;

        Ok(())
    }
}
