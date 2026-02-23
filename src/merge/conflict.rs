//! Conflict directory representation for merge conflicts.
//!
//! When a merge conflict cannot be auto-resolved, this module creates a conflict
//! directory containing all relevant versions and resolution tools.
//!
//! The conflict directory structure:
//! ```text
//! {name}/
//!   ├── base          # Base version (if existed)
//!   ├── theirs        # Version from dir_1 (if exists)
//!   ├── ours          # Version from dir_2 (if exists)
//!   ├── merged        # Merged file with conflict markers (if applicable)
//!   ├── CONFLICT.txt  # Explanation of the conflict
//!   └── take          # Bash script for resolution
//! ```

use std::sync::Arc;

use crate::app::{DirectoryLeaf, DirectoryListBuilder, StreamingFileUploader};
use crate::merge::error::{MergeError, Result};
use crate::merge::types::{ChangingDirEntry, ConflictContext, DirEntryChange, MergeFileOutcome};
use crate::repo::RepoError;
use crate::repo::Repo;
use crate::repository::{DirEntry, FileEntry};
use crate::util::{Complete, Completes, ManagedBuffers, NoopComplete, WithComplete};

// =============================================================================
// Public API
// =============================================================================

/// Build a conflict directory for a merge conflict.
///
/// Creates a directory containing all versions involved in the conflict,
/// along with explanation and resolution tools.
///
/// # Arguments
/// * `repo` - Repository access
/// * `context` - The conflict context with all relevant information
///
/// # Returns
/// A `DirectoryLeaf` representing the conflict directory, with completion tracking.
pub async fn build_conflict_directory(
    repo: Arc<Repo>,
    context: ConflictContext,
) -> Result<WithComplete<DirectoryLeaf>> {
    let mut builder = DirectoryListBuilder::new(Arc::clone(&repo));
    let completes = Completes::new();

    // Add entries in sorted order (base, merged, ours, take, theirs, CONFLICT.txt)
    // Directory entries are added in lexicographic order by name

    // Add "CONFLICT.txt" (comes first alphabetically with uppercase)
    let conflict_txt = generate_conflict_txt(&repo, &context).await?;
    completes
        .add(conflict_txt.complete)
        .map_err(|e| MergeError::Repo(RepoError::Other(e.to_string())))?;
    builder
        .add(conflict_txt.result, Arc::new(NoopComplete) as Arc<dyn Complete>)
        .await?;

    // Add "base" entry if it exists
    if let Some(leaf) = changing_entry_to_leaf("base", &context.base) {
        builder
            .add(leaf, Arc::new(NoopComplete) as Arc<dyn Complete>)
            .await?;
    }

    // Add "merged" entry if a merge was attempted with conflict markers
    if let Some(MergeFileOutcome::MergeConflict { file_id, size }) = &context.merge_file_result {
        let merged_entry = FileEntry {
            name: "merged".to_string(),
            size: *size,
            executable: false,
            file: file_id.clone(),
        };
        builder
            .add(
                DirectoryLeaf::File(merged_entry),
                Arc::new(NoopComplete) as Arc<dyn Complete>,
            )
            .await?;
    }

    // Add "ours" entry (from entry_2) if it exists
    if let Some(leaf) = changing_entry_to_leaf("ours", &context.entry_2) {
        builder
            .add(leaf, Arc::new(NoopComplete) as Arc<dyn Complete>)
            .await?;
    }

    // Add "take" script
    let take_script = generate_take_script(&repo, &context).await?;
    completes
        .add(take_script.complete)
        .map_err(|e| MergeError::Repo(RepoError::Other(e.to_string())))?;
    builder
        .add(take_script.result, Arc::new(NoopComplete) as Arc<dyn Complete>)
        .await?;

    // Add "theirs" entry (from entry_1) if it exists
    if let Some(leaf) = changing_entry_to_leaf("theirs", &context.entry_1) {
        builder
            .add(leaf, Arc::new(NoopComplete) as Arc<dyn Complete>)
            .await?;
    }

    // Finish building the directory
    let result = builder.finish().await?;
    completes.done();

    // Create the final directory entry
    let dir_entry = DirEntry {
        name: context.name.clone(),
        directory: result.hash,
    };

    // Combine all completion handles
    let combined_complete = Arc::new(completes) as Arc<dyn Complete>;

    Ok(WithComplete::new(
        DirectoryLeaf::Dir(dir_entry),
        combined_complete,
    ))
}

// =============================================================================
// Helper Functions
// =============================================================================

/// Convert a ChangingDirEntry to a DirectoryLeaf with the given name.
fn changing_entry_to_leaf(name: &str, entry: &ChangingDirEntry) -> Option<DirectoryLeaf> {
    match entry {
        ChangingDirEntry::None => None,
        ChangingDirEntry::File(fe) => Some(DirectoryLeaf::File(FileEntry {
            name: name.to_string(),
            size: fe.size,
            executable: fe.executable,
            file: fe.file.clone(),
        })),
        ChangingDirEntry::Directory(d) => Some(DirectoryLeaf::Dir(DirEntry {
            name: name.to_string(),
            directory: d.clone(),
        })),
    }
}

/// Generate the CONFLICT.txt explanation file.
async fn generate_conflict_txt(
    repo: &Arc<Repo>,
    context: &ConflictContext,
) -> Result<WithComplete<DirectoryLeaf>> {
    let content = format_conflict_txt(context);
    let content_bytes = content.into_bytes();
    let size = content_bytes.len() as u64;

    let mut uploader = StreamingFileUploader::new(Arc::clone(repo));
    let managed_buffers = ManagedBuffers::new();
    let buffer = managed_buffers.get_buffer_with_data(content_bytes).await;
    uploader.add(buffer).await?;
    let result = uploader.finish().await?;

    let file_entry = FileEntry {
        name: "CONFLICT.txt".to_string(),
        size,
        executable: false,
        file: result.result.hash,
    };

    Ok(WithComplete::new(
        DirectoryLeaf::File(file_entry),
        result.complete,
    ))
}

/// Generate the "take" script for conflict resolution.
async fn generate_take_script(
    repo: &Arc<Repo>,
    context: &ConflictContext,
) -> Result<WithComplete<DirectoryLeaf>> {
    let content = format_take_script(context);
    let content_bytes = content.into_bytes();
    let size = content_bytes.len() as u64;

    let mut uploader = StreamingFileUploader::new(Arc::clone(repo));
    let managed_buffers = ManagedBuffers::new();
    let buffer = managed_buffers.get_buffer_with_data(content_bytes).await;
    uploader.add(buffer).await?;
    let result = uploader.finish().await?;

    let file_entry = FileEntry {
        name: "take".to_string(),
        size,
        executable: true, // Mark as executable
        file: result.result.hash,
    };

    Ok(WithComplete::new(
        DirectoryLeaf::File(file_entry),
        result.complete,
    ))
}

/// Format the CONFLICT.txt content.
fn format_conflict_txt(context: &ConflictContext) -> String {
    let mut content = String::new();

    content.push_str(&format!("CONFLICT: {}\n\n", context.name));
    content.push_str(
        "This directory represents a merge conflict that could not be automatically resolved.\n\n",
    );

    // Describe base
    content.push_str("BASE:\n");
    content.push_str(&format!("  {}\n\n", describe_entry(&context.base)));

    // Describe theirs (entry_1)
    content.push_str("THEIRS (from first branch):\n");
    content.push_str(&format!("  {}\n\n", describe_entry(&context.entry_1)));

    // Describe ours (entry_2)
    content.push_str("OURS (from second branch):\n");
    content.push_str(&format!("  {}\n\n", describe_entry(&context.entry_2)));

    // Describe changes
    content.push_str("CHANGES:\n");
    content.push_str(&format!(
        "  Base -> Theirs: {}\n",
        describe_change(&context.change_1)
    ));
    content.push_str(&format!(
        "  Base -> Ours:   {}\n\n",
        describe_change(&context.change_2)
    ));

    // Describe merge result if applicable
    if let Some(ref outcome) = context.merge_file_result {
        content.push_str("MERGE ATTEMPT:\n");
        match outcome {
            MergeFileOutcome::Merged { .. } => {
                content.push_str("  Successfully merged (should not see this in conflict)\n\n");
            }
            MergeFileOutcome::MergeConflict { .. } => {
                content.push_str("  A text merge was attempted. The \"merged\" file contains the result\n");
                content.push_str("  with conflict markers (<<<<<<< / ======= / >>>>>>>).\n\n");
            }
            MergeFileOutcome::Binary => {
                content.push_str("  Could not merge: one or more files are binary.\n\n");
            }
            MergeFileOutcome::TooLarge => {
                content.push_str("  Could not merge: combined file sizes exceed memory limit.\n\n");
            }
        }
    }

    // Resolution instructions
    content.push_str("RESOLUTION:\n");
    content.push_str("  To resolve this conflict:\n");
    content.push_str("  1. Examine the versions above\n");
    content.push_str("  2. Either:\n");
    content.push_str("     - Use the \"take\" script: ./take base|theirs|ours|merged\n");
    content.push_str(
        "     - Manually resolve: delete this directory and replace with desired content\n",
    );

    content
}

/// Describe a ChangingDirEntry for the CONFLICT.txt.
fn describe_entry(entry: &ChangingDirEntry) -> String {
    match entry {
        ChangingDirEntry::None => "Did not exist".to_string(),
        ChangingDirEntry::File(fe) => {
            let exec = if fe.executable { " (executable)" } else { "" };
            format!("File ({} bytes){}", fe.size, exec)
        }
        ChangingDirEntry::Directory(_) => "Directory".to_string(),
    }
}

/// Describe a DirEntryChange for the CONFLICT.txt.
fn describe_change(change: &DirEntryChange) -> String {
    match change {
        DirEntryChange::Unchanged => "No change".to_string(),
        DirEntryChange::FileCreated(_) => "Created file".to_string(),
        DirEntryChange::FileChanged {
            content_change,
            executable_change,
            ..
        } => {
            let content_desc = if content_change.is_some() {
                "content changed"
            } else {
                ""
            };
            let exec_desc = match executable_change {
                Some(true) => "made executable",
                Some(false) => "made non-executable",
                None => "",
            };
            match (content_change.is_some(), executable_change.is_some()) {
                (true, true) => format!("{}, {}", content_desc, exec_desc),
                (true, false) => content_desc.to_string(),
                (false, true) => exec_desc.to_string(),
                (false, false) => "No change".to_string(),
            }
        }
        DirEntryChange::FileChangedToDirectory { .. } => "Changed from file to directory".to_string(),
        DirEntryChange::FileRemoved => "Removed file".to_string(),
        DirEntryChange::DirectoryCreated(_) => "Created directory".to_string(),
        DirEntryChange::DirectoryChanged { .. } => "Directory contents changed".to_string(),
        DirEntryChange::DirectoryChangedToFile { .. } => "Changed from directory to file".to_string(),
        DirEntryChange::DirectoryRemoved => "Removed directory".to_string(),
    }
}

/// Format the "take" script content.
fn format_take_script(context: &ConflictContext) -> String {
    format!(
        r#"#!/bin/bash
# Conflict resolution script for: {}
# Usage: ./take <base|theirs|ours|merged>
#
# This script resolves the conflict by replacing this directory
# with the selected version.

taterfs conflict take "$@"
"#,
        context.name
    )
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::MemoryBackend;
    use crate::caches::NoopCache;
    use crate::merge::types::DirEntryChange;

    fn create_test_repo() -> Arc<Repo> {
        let backend = MemoryBackend::new();
        let cache = NoopCache;
        Arc::new(Repo::new(backend, cache))
    }

    #[tokio::test]
    async fn test_build_conflict_directory_file_vs_file() {
        let repo = create_test_repo();

        // Create test file entries
        let base_file = FileEntry {
            name: "test.txt".to_string(),
            size: 100,
            executable: false,
            file: "base_hash".to_string(),
        };
        let theirs_file = FileEntry {
            name: "test.txt".to_string(),
            size: 150,
            executable: false,
            file: "theirs_hash".to_string(),
        };
        let ours_file = FileEntry {
            name: "test.txt".to_string(),
            size: 200,
            executable: false,
            file: "ours_hash".to_string(),
        };

        let context = ConflictContext {
            name: "test.txt".to_string(),
            base: ChangingDirEntry::File(base_file.clone()),
            entry_1: ChangingDirEntry::File(theirs_file),
            entry_2: ChangingDirEntry::File(ours_file),
            change_1: DirEntryChange::FileChanged {
                base: base_file.clone(),
                content_change: Some(FileEntry {
                    name: "test.txt".to_string(),
                    size: 150,
                    executable: false,
                    file: "theirs_hash".to_string(),
                }),
                executable_change: None,
            },
            change_2: DirEntryChange::FileChanged {
                base: base_file,
                content_change: Some(FileEntry {
                    name: "test.txt".to_string(),
                    size: 200,
                    executable: false,
                    file: "ours_hash".to_string(),
                }),
                executable_change: None,
            },
            merge_file_result: Some(MergeFileOutcome::MergeConflict {
                file_id: "merged_hash".to_string(),
                size: 250,
            }),
        };

        let result = build_conflict_directory(repo, context).await;
        assert!(result.is_ok());

        let leaf = result.unwrap().result;
        match leaf {
            DirectoryLeaf::Dir(dir) => {
                assert_eq!(dir.name, "test.txt");
            }
            _ => panic!("Expected directory leaf"),
        }
    }

    #[tokio::test]
    async fn test_build_conflict_directory_file_vs_directory() {
        let repo = create_test_repo();

        let base_file = FileEntry {
            name: "item".to_string(),
            size: 100,
            executable: false,
            file: "base_hash".to_string(),
        };

        let context = ConflictContext {
            name: "item".to_string(),
            base: ChangingDirEntry::File(base_file.clone()),
            entry_1: ChangingDirEntry::File(FileEntry {
                name: "item".to_string(),
                size: 150,
                executable: false,
                file: "theirs_hash".to_string(),
            }),
            entry_2: ChangingDirEntry::Directory("ours_dir_hash".to_string()),
            change_1: DirEntryChange::FileChanged {
                base: base_file.clone(),
                content_change: Some(FileEntry {
                    name: "item".to_string(),
                    size: 150,
                    executable: false,
                    file: "theirs_hash".to_string(),
                }),
                executable_change: None,
            },
            change_2: DirEntryChange::FileChangedToDirectory {
                base: base_file,
                directory: "ours_dir_hash".to_string(),
            },
            merge_file_result: None,
        };

        let result = build_conflict_directory(repo, context).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_describe_entry() {
        assert_eq!(describe_entry(&ChangingDirEntry::None), "Did not exist");

        let file = FileEntry {
            name: "test".to_string(),
            size: 1024,
            executable: false,
            file: "hash".to_string(),
        };
        assert_eq!(
            describe_entry(&ChangingDirEntry::File(file)),
            "File (1024 bytes)"
        );

        let exec_file = FileEntry {
            name: "test".to_string(),
            size: 512,
            executable: true,
            file: "hash".to_string(),
        };
        assert_eq!(
            describe_entry(&ChangingDirEntry::File(exec_file)),
            "File (512 bytes) (executable)"
        );

        assert_eq!(
            describe_entry(&ChangingDirEntry::Directory("hash".to_string())),
            "Directory"
        );
    }

    #[tokio::test]
    async fn test_describe_change() {
        assert_eq!(describe_change(&DirEntryChange::Unchanged), "No change");
        assert_eq!(describe_change(&DirEntryChange::FileRemoved), "Removed file");
        assert_eq!(
            describe_change(&DirEntryChange::DirectoryRemoved),
            "Removed directory"
        );
    }

    #[test]
    fn test_format_take_script() {
        let context = ConflictContext {
            name: "myfile.txt".to_string(),
            base: ChangingDirEntry::None,
            entry_1: ChangingDirEntry::None,
            entry_2: ChangingDirEntry::None,
            change_1: DirEntryChange::Unchanged,
            change_2: DirEntryChange::Unchanged,
            merge_file_result: None,
        };

        let script = format_take_script(&context);
        assert!(script.starts_with("#!/bin/bash"));
        assert!(script.contains("myfile.txt"));
        assert!(script.contains("taterfs conflict take"));
    }

    #[test]
    fn test_format_conflict_txt() {
        let base_file = FileEntry {
            name: "test.txt".to_string(),
            size: 100,
            executable: false,
            file: "base_hash".to_string(),
        };

        let context = ConflictContext {
            name: "test.txt".to_string(),
            base: ChangingDirEntry::File(base_file.clone()),
            entry_1: ChangingDirEntry::File(FileEntry {
                name: "test.txt".to_string(),
                size: 150,
                executable: false,
                file: "theirs_hash".to_string(),
            }),
            entry_2: ChangingDirEntry::None,
            change_1: DirEntryChange::FileChanged {
                base: base_file.clone(),
                content_change: Some(FileEntry {
                    name: "test.txt".to_string(),
                    size: 150,
                    executable: false,
                    file: "theirs_hash".to_string(),
                }),
                executable_change: None,
            },
            change_2: DirEntryChange::FileRemoved,
            merge_file_result: None,
        };

        let txt = format_conflict_txt(&context);
        assert!(txt.contains("CONFLICT: test.txt"));
        assert!(txt.contains("BASE:"));
        assert!(txt.contains("THEIRS"));
        assert!(txt.contains("OURS"));
        assert!(txt.contains("CHANGES:"));
        assert!(txt.contains("RESOLUTION:"));
    }
}
