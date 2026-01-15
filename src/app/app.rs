//! Top-level application component.
//!
//! The [`App`] owns all global services and is the root for the application's functionality.

use std::sync::Arc;

use thiserror::Error;

use crate::app::CapacityManagers;
use crate::caches::NoopCaches;
use crate::config::{read_config, ConfigHelper, ConfigSource};
use crate::file_store::{
    create_file_store, CreateFileStoreContext, CreateFileStoreError, FileStore,
};
use crate::repo::{create_repo, CreateRepoContext, CreateRepoError, Repo};
use crate::util::ManagedBuffers;

// =============================================================================
// Error Types
// =============================================================================

/// Errors that can occur during App operations.
#[derive(Debug, Error)]
pub enum AppError {
    /// Configuration error.
    #[error("configuration error: {0}")]
    Config(String),

    /// Repository creation error.
    #[error("failed to create repository: {0}")]
    CreateRepo(#[from] CreateRepoError),

    /// File store creation error.
    #[error("failed to create file store: {0}")]
    CreateFileStore(#[from] CreateFileStoreError),
}

/// Result type for App operations.
pub type Result<T> = std::result::Result<T, AppError>;

// =============================================================================
// Context Types
// =============================================================================

/// Context for creating an App.
#[derive(Default)]
pub struct AppContext {
    /// Source for configuration files.
    pub config_source: ConfigSource,
}

/// Context for creating a repository via the App.
pub struct AppCreateRepoContext {
    /// Repository specification (URL or named repository).
    pub spec: String,
    /// Whether to allow creating a Repo for an uninitialized repository.
    pub allow_uninitialized: bool,
}

impl AppCreateRepoContext {
    /// Create a new context for the given spec.
    pub fn new(spec: impl Into<String>) -> Self {
        Self {
            spec: spec.into(),
            allow_uninitialized: false,
        }
    }

    /// Set whether to allow uninitialized repositories.
    pub fn with_allow_uninitialized(mut self, allow: bool) -> Self {
        self.allow_uninitialized = allow;
        self
    }
}

/// Context for creating a file store via the App.
pub struct AppCreateFileStoreContext {
    /// File store specification (URL or named file store).
    pub spec: String,
}

impl AppCreateFileStoreContext {
    /// Create a new context for the given spec.
    pub fn new(spec: impl Into<String>) -> Self {
        Self { spec: spec.into() }
    }
}

// =============================================================================
// App
// =============================================================================

/// The top-level application component.
///
/// Owns all global services including configuration, caches, and capacity managers.
/// Provides methods for creating repositories and file stores.
pub struct App {
    config: ConfigHelper,
    managed_buffers: ManagedBuffers,
    network_managers: CapacityManagers,
    s3_managers: CapacityManagers,
}

impl App {
    /// Create a new App with the given context.
    pub fn new(ctx: AppContext) -> Result<Self> {
        let config_result =
            read_config(&ctx.config_source).map_err(|e| AppError::Config(e.to_string()))?;

        let config = ConfigHelper::new(config_result.config);
        let managed_buffers = ManagedBuffers::new();

        // Create global capacity managers
        let network_managers =
            CapacityManagers::from_resolved_limits(&config.resolve_network_limits());
        let s3_managers = CapacityManagers::from_resolved_limits(&config.resolve_s3_limits());

        Ok(Self {
            config,
            managed_buffers,
            network_managers,
            s3_managers,
        })
    }

    /// Get the configuration helper.
    pub fn config(&self) -> &ConfigHelper {
        &self.config
    }

    /// Create a repository from a specification.
    pub async fn create_repo(&self, ctx: AppCreateRepoContext) -> Result<Arc<Repo>> {
        // TODO: Use a real cache implementation when available
        let repo_ctx = CreateRepoContext::new(
            self.config.clone(),
            NoopCaches,
            self.network_managers.clone(),
            self.s3_managers.clone(),
        )
        .with_allow_uninitialized(ctx.allow_uninitialized);

        let repo = create_repo(&ctx.spec, &repo_ctx).await?;
        Ok(Arc::new(repo))
    }

    /// Create a file store from a specification.
    pub async fn create_file_store(
        &self,
        ctx: AppCreateFileStoreContext,
    ) -> Result<Arc<dyn FileStore>> {
        let file_store_ctx =
            CreateFileStoreContext::new(self.config.clone(), self.managed_buffers.clone());

        let store = create_file_store(&ctx.spec, &file_store_ctx).await?;
        Ok(store)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_app_creation() {
        let ctx = AppContext::default();
        let app = App::new(ctx).unwrap();
        assert!(app.config().config().cache.no_cache == false);
    }

    #[test]
    fn test_app_create_repo_context() {
        let ctx = AppCreateRepoContext::new("file:///tmp/repo");
        assert_eq!(ctx.spec, "file:///tmp/repo");
        assert!(!ctx.allow_uninitialized);

        let ctx = ctx.with_allow_uninitialized(true);
        assert!(ctx.allow_uninitialized);
    }

    #[test]
    fn test_app_create_file_store_context() {
        let ctx = AppCreateFileStoreContext::new("file:///tmp/files");
        assert_eq!(ctx.spec, "file:///tmp/files");
    }
}
