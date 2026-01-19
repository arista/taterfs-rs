//! Download functionality for synchronizing repository content to file stores.
//!
//! This module provides tools for downloading repository directories to file stores,
//! including generating the sequence of actions needed to synchronize content.

mod download_actions;

pub use download_actions::{download_actions, DownloadAction, DownloadActions, DownloadError, Result};
