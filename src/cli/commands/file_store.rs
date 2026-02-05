//! File store subcommands.

use std::path::Path;

use clap::{Args, Subcommand};

use crate::app::App;
use crate::cli::{CliError, FileStoreArgs, GlobalArgs, InputSource, OutputSink, Result};
use crate::file_store::ScanEvent;
use crate::util::ManagedBuffers;

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
    #[command(flatten)]
    pub file_store: FileStoreArgs,

    /// Optional path to scan from within the file store.
    pub path: Option<String>,

    #[command(flatten)]
    pub output: OutputSink,
}

impl ScanArgs {
    pub async fn run(self, app: &App, _global: &GlobalArgs) -> Result<()> {
        let fs_ctx = self.file_store.to_create_file_store_context();
        let file_store = app.create_file_store(fs_ctx).await?;

        let source = file_store
            .get_source()
            .ok_or_else(|| CliError::Other("file store does not support reading".to_string()))?;

        let scan_path = self.path.as_ref().map(Path::new);
        let mut events = source
            .scan(scan_path)
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
    #[command(flatten)]
    pub file_store: FileStoreArgs,

    /// The path to the file.
    pub path: Option<String>,

    #[command(flatten)]
    pub input: InputSource,

    #[command(flatten)]
    pub output: OutputSink,
}

impl SourceChunksArgs {
    pub async fn run(self, app: &App, _global: &GlobalArgs) -> Result<()> {
        let fs_ctx = self.file_store.to_create_file_store_context();
        let file_store = app.create_file_store(fs_ctx).await?;

        let source = file_store
            .get_source()
            .ok_or_else(|| CliError::Other("file store does not support reading".to_string()))?;

        let path_str = self.input.read(self.path.as_deref()).await?;
        let path = Path::new(&path_str);

        let chunks = source
            .get_source_chunks(path)
            .await
            .map_err(|e| CliError::Other(e.to_string()))?
            .ok_or_else(|| CliError::Other(format!("path not found: {}", path_str)))?;

        let mut contents = source
            .get_source_chunks_with_content(chunks, ManagedBuffers::new())
            .await
            .map_err(|e| CliError::Other(e.to_string()))?;

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
