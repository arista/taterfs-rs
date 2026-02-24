//! Error types for sync operations.

use crate::app::UploadError;
use crate::download::DownloadError;
use crate::merge::MergeError;
use crate::repo::RepoError;

/// Error type for sync operations.
#[derive(Debug, thiserror::Error)]
pub enum SyncError {
    /// File store already has a sync state configured.
    #[error("file store already has a sync state")]
    AlreadyHasSyncState,

    /// File store has a pending sync state from an interrupted sync.
    #[error("file store has pending sync state (interrupted sync)")]
    PendingSyncState,

    /// Both file store and repo directory have content - cannot add sync.
    #[error(
        "both file store and repo directory have content - clear or move aside file store content first"
    )]
    BothHaveContent,

    /// File store error.
    #[error("file store error: {0}")]
    FileStore(#[from] crate::file_store::Error),

    /// Upload error.
    #[error("upload error: {0}")]
    Upload(#[from] UploadError),

    /// Download error.
    #[error("download error: {0}")]
    Download(#[from] DownloadError),

    /// Repository error.
    #[error("repository error: {0}")]
    Repo(#[from] RepoError),

    /// Merge error.
    #[error("merge error: {0}")]
    Merge(#[from] MergeError),

    /// File store does not support sync state.
    #[error("file store does not support sync state")]
    NoSyncStateSupport,

    /// File store does not support reading.
    #[error("file store does not support reading")]
    NoFileSource,

    /// File store does not support writing.
    #[error("file store does not support writing")]
    NoFileDest,

    /// Branch not found.
    #[error("branch not found: {0}")]
    BranchNotFound(String),

    /// Force pending downloads required but not specified.
    #[error("pending download exists - use force_pending_downloads to resume")]
    PendingDownloadRequired,

    /// Other error.
    #[error("{0}")]
    Other(String),
}

/// Result type for sync operations.
pub type Result<T> = std::result::Result<T, SyncError>;
