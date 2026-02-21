//! 3-way directory merge for taterfs-rs.
//!
//! This module implements a 3-way merge algorithm for combining changes
//! from two directory trees that share a common ancestor.
//!
//! # Overview
//!
//! The merge algorithm works by "zippering" through the entries of three
//! directories (base, dir_1, dir_2) in sorted order:
//!
//! 1. For each entry name, compute the change from base to dir_1 and base to dir_2
//! 2. Determine the merge action based on those two changes
//! 3. Apply the merge action (take one side, merge recursively, or conflict)
//!
//! # Key Types
//!
//! - [`ChangingDirEntry`] - Represents an entry state (None, File, Directory)
//! - [`DirEntryChange`] - Represents a change between two states
//! - [`DirChangeMerge`] - The merge action to take
//! - [`ConflictContext`] - Information about a merge conflict
//!
//! # Key Functions
//!
//! - [`to_dir_change`] - Compute the change from base to derived
//! - [`changes_to_merge`] - Determine merge action from two changes
//! - [`merge_directories`] - Perform the 3-way directory merge
//!
//! # Example
//!
//! ```ignore
//! use taterfs_rs::merge::merge_directories;
//!
//! let result = merge_directories(
//!     repo,
//!     Some(&base_dir_id),
//!     &dir_1_id,
//!     &dir_2_id,
//! ).await?;
//!
//! if result.conflicts.is_empty() {
//!     println!("Merged successfully: {}", result.directory.result);
//! } else {
//!     println!("Merge had {} conflicts", result.conflicts.len());
//! }
//! ```

mod change_merge;
mod dir_change;
mod directory;
mod error;
mod types;

pub use change_merge::changes_to_merge;
pub use dir_change::to_dir_change;
pub use directory::{
    ConflictResolution, MergeDirectoryResult, MergeFileResult, handle_conflict, merge_directories,
    merge_files,
};
pub use error::{MergeError, Result};
pub use types::{ChangingDirEntry, ConflictContext, DirChangeMerge, DirEntryChange};
