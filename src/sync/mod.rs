//! Sync module for synchronizing file stores with repositories.
//!
//! This module provides functionality to:
//! - Add a sync relationship between a file store and a repository directory
//! - Run sync operations for one or more file stores
//!
//! # Overview
//!
//! A "sync" operation synchronizes a file store's contents with a repository
//! directory by uploading local changes, merging with remote changes, and
//! downloading the merged result.
//!
//! # Key Functions
//!
//! - [`add_sync`] - Connect a file store to a repository directory
//! - [`run_syncs`] - Execute sync for multiple file stores

mod add_sync;
mod error;
mod run_sync;

pub use add_sync::{AddSyncOptions, add_sync};
pub use error::{Result, SyncError};
pub use run_sync::{RunSyncsOptions, RunSyncsResult, SyncResult, SyncSpec, run_syncs};
