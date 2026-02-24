//! Sync CLI commands.

use clap::Args;
use serde::Serialize;

use crate::app::{App, AppCreateFileStoreContext, AppCreateRepoContext};
use crate::cli::{
    CliError, CommandContextRequirements, GlobalArgs, OutputSink, Result, create_command_context,
};
use crate::repo_model::RepoModel;
use crate::sync::{AddSyncOptions, RunSyncsOptions, SyncSpec, add_sync, run_syncs};

// =============================================================================
// Add Sync
// =============================================================================

/// Arguments for the add-sync command.
#[derive(Args, Debug)]
pub struct AddSyncArgs {
    /// Path within the filestore to sync.
    #[arg(default_value = "/")]
    pub filestore_path: String,

    /// Path within the repository to sync.
    pub repo_path: String,

    #[command(flatten)]
    pub output: OutputSink,
}

#[derive(Serialize)]
struct AddSyncOutput {
    status: String,
    filestore_path: String,
    repo_path: String,
}

impl AddSyncArgs {
    pub async fn run(self, app: &App, global: &GlobalArgs) -> Result<()> {
        // Create command context - requires repository, file store, and branch
        let ctx = create_command_context(
            global
                .to_command_context_input()
                .with_file_store_path(Some(self.filestore_path.clone())),
            CommandContextRequirements::new()
                .with_repository()
                .with_file_store()
                .with_branch(),
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
        let branch = ctx
            .branch
            .clone()
            .ok_or_else(|| CliError::Other("branch required".to_string()))?;

        // Create repo and file store
        let repo_ctx = AppCreateRepoContext {
            spec: repo_spec.clone(),
            allow_uninitialized: false,
        };
        let repo = app.create_repo(repo_ctx).await?;

        let fs_ctx = AppCreateFileStoreContext {
            spec: fs_spec.clone(),
        };
        let file_store = app.create_file_store(fs_ctx).await?;

        // Create repo model for high-level operations
        let repo_model = RepoModel::new(repo.clone());

        // Run add_sync
        let options = AddSyncOptions {
            commit_message: None,
        };

        add_sync(
            file_store,
            repo,
            &repo_model,
            &repo_spec,
            &branch,
            &self.repo_path,
            options,
        )
        .await
        .map_err(|e| CliError::Other(e.to_string()))?;

        // Output result
        if ctx.json {
            self.output
                .write(
                    &AddSyncOutput {
                        status: "ok".to_string(),
                        filestore_path: self.filestore_path.clone(),
                        repo_path: self.repo_path.clone(),
                    },
                    true,
                )
                .await?;
        } else {
            self.output
                .write_str(&format!(
                    "Sync added: {} -> {}",
                    self.filestore_path, self.repo_path
                ))
                .await?;
        }

        Ok(())
    }
}

// =============================================================================
// Run Syncs
// =============================================================================

/// Arguments for the sync run command.
#[derive(Args, Debug)]
pub struct RunSyncsArgs {
    /// Filestore specs to sync. If empty, syncs all configured filestores.
    pub filestore_specs: Vec<String>,

    /// Force completion of pending downloads before proceeding.
    #[arg(long)]
    pub force_pending_downloads: bool,

    /// Download without using a staging area.
    #[arg(long)]
    pub no_stage: bool,

    #[command(flatten)]
    pub output: OutputSink,
}

#[derive(Serialize)]
struct RunSyncsOutput {
    total: usize,
    successful: usize,
    failed: usize,
    conflicts: usize,
}

impl RunSyncsArgs {
    pub async fn run(self, app: &App, global: &GlobalArgs) -> Result<()> {
        // Create command context - minimal requirements since we get info from sync state
        let ctx = create_command_context(
            global.to_command_context_input(),
            CommandContextRequirements::new(),
            app,
        )
        .await?;

        // Build SyncSpecs from filestore specs
        let mut sync_specs = Vec::new();

        for fs_spec_str in &self.filestore_specs {
            let fs_ctx = AppCreateFileStoreContext {
                spec: fs_spec_str.clone(),
            };
            let file_store = app.create_file_store(fs_ctx).await?;

            // Get sync state from file store
            let sync_state_mgr = file_store.get_sync_state_manager().ok_or_else(|| {
                CliError::Other(format!(
                    "file store '{}' does not support sync state",
                    fs_spec_str
                ))
            })?;

            let sync_state = sync_state_mgr
                .get_sync_state()
                .await
                .map_err(|e| CliError::Other(e.to_string()))?
                .ok_or_else(|| {
                    CliError::Other(format!(
                        "file store '{}' has no sync state - run 'add-sync' first",
                        fs_spec_str
                    ))
                })?;

            sync_specs.push(SyncSpec {
                file_store,
                repo_spec: sync_state.repository_url,
                branch_name: sync_state.branch_name,
                repo_directory: sync_state.repository_directory,
            });
        }

        if sync_specs.is_empty() {
            if ctx.json {
                self.output
                    .write(
                        &RunSyncsOutput {
                            total: 0,
                            successful: 0,
                            failed: 0,
                            conflicts: 0,
                        },
                        true,
                    )
                    .await?;
            } else {
                self.output
                    .write_str("No filestores specified to sync")
                    .await?;
            }
            return Ok(());
        }

        // Run syncs
        let options = RunSyncsOptions {
            force_pending_downloads: self.force_pending_downloads,
            with_stage: !self.no_stage,
            ..Default::default()
        };

        let result = run_syncs(sync_specs, app, options)
            .await
            .map_err(|e| CliError::Other(e.to_string()))?;

        // Wait for completion
        result
            .complete
            .complete()
            .await
            .map_err(|e| CliError::Other(format!("sync failed: {}", e)))?;

        // Summarize results
        let total = result.result.results.len();
        let successful = result.result.results.iter().filter(|r| r.success).count();
        let failed = total - successful;
        let conflicts: usize = result
            .result
            .results
            .iter()
            .map(|r| r.conflicts.len())
            .sum();

        // Output result
        if ctx.json {
            self.output
                .write(
                    &RunSyncsOutput {
                        total,
                        successful,
                        failed,
                        conflicts,
                    },
                    true,
                )
                .await?;
        } else {
            self.output
                .write_str(&format!(
                    "Sync complete: {}/{} successful, {} conflicts",
                    successful, total, conflicts
                ))
                .await?;

            // Report any errors
            for (i, r) in result.result.results.iter().enumerate() {
                if let Some(ref error) = r.error {
                    eprintln!("  Filestore {}: {}", i, error);
                }
            }
        }

        Ok(())
    }
}
