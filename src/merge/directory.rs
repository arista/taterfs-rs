//! Three-way directory merge implementation.
//!
//! This module implements the core `merge_directories` function that performs
//! a 3-way merge of directories by zippering through entries in sorted order.

use std::sync::Arc;

use crate::app::{DirectoryLeaf, DirectoryListBuilder};
use crate::merge::change_merge::changes_to_merge;
use crate::merge::conflict::build_conflict_directory;
use crate::merge::dir_change::to_dir_change;
use crate::merge::error::{MergeError, Result};
use crate::merge::file::merge_files;
use crate::merge::types::{ChangingDirEntry, ConflictContext, DirChangeMerge};
use crate::repo::{DirectoryEntry, Repo};
use crate::repository::{DirEntry, FileEntry, ObjectId};
use crate::util::{Complete, Completes, NoopComplete, WithComplete};

// =============================================================================
// MergeDirectoryResult
// =============================================================================

/// Result of a directory merge operation.
pub struct MergeDirectoryResult {
    /// The merged directory ObjectId with completion tracking.
    pub directory: WithComplete<ObjectId>,
    /// Any conflicts encountered during the merge.
    pub conflicts: Vec<ConflictContext>,
}

// =============================================================================
// MergeFileResult
// =============================================================================

/// Result of a file merge operation.
pub enum MergeFileResult {
    /// Successfully merged the files.
    Merged {
        /// The merged file entry.
        file: FileEntry,
        /// Completion handle for the merge.
        complete: Arc<dyn Complete>,
    },
    /// Could not merge - conflict.
    Conflict(Box<ConflictContext>),
}

// =============================================================================
// merge_directories
// =============================================================================

/// Perform a 3-way merge of directories.
///
/// This function implements the merge algorithm specified in docs/architecture/merge.md.
/// It zippers through the entries of base, dir_1, and dir_2 in sorted order,
/// computing changes and determining merge actions.
///
/// # Arguments
/// * `repo` - Repository access
/// * `base` - Base directory ObjectId (None for newly created directories)
/// * `dir_1` - First directory to merge
/// * `dir_2` - Second directory to merge
/// * `max_merge_memory` - Maximum combined file size for text merges
///
/// # Returns
/// The merged directory result with ObjectId and any conflicts encountered.
pub async fn merge_directories(
    repo: Arc<Repo>,
    base: Option<&ObjectId>,
    dir_1: &ObjectId,
    dir_2: &ObjectId,
    max_merge_memory: u64,
) -> Result<MergeDirectoryResult> {
    // Create entry lists for all directories
    let mut base_list = if let Some(base_id) = base {
        Some(repo.list_directory_entries(base_id).await?)
    } else {
        None
    };
    let mut list_1 = repo.list_directory_entries(dir_1).await?;
    let mut list_2 = repo.list_directory_entries(dir_2).await?;

    // Initialize builder and tracking
    let mut builder = DirectoryListBuilder::new(Arc::clone(&repo));
    let completes = Completes::new();
    let mut conflicts = Vec::new();

    // Get initial entries from all lists
    let mut current_base = if let Some(ref mut list) = base_list {
        list.next().await?
    } else {
        None
    };
    let mut current_1 = list_1.next().await?;
    let mut current_2 = list_2.next().await?;

    // Zipper through all entries
    loop {
        // Find the minimum name among current entries
        let min_name = min_entry_name(
            current_base.as_ref(),
            current_1.as_ref(),
            current_2.as_ref(),
        );

        let Some(name) = min_name else {
            // All lists exhausted
            break;
        };

        let name = name.to_string();

        // Collect entries matching the minimum name
        let (base_entry, advance_base) = collect_if_matches(&current_base, &name);
        let (entry_1, advance_1) = collect_if_matches(&current_1, &name);
        let (entry_2, advance_2) = collect_if_matches(&current_2, &name);

        // Convert to ChangingDirEntry
        let base_changing = to_changing_dir_entry(base_entry);
        let changing_1 = to_changing_dir_entry(entry_1);
        let changing_2 = to_changing_dir_entry(entry_2);

        // Compute changes
        let change_1 = to_dir_change(&base_changing, &changing_1, &name)?;
        let change_2 = to_dir_change(&base_changing, &changing_2, &name)?;

        // Determine merge action
        let merge_action = changes_to_merge(&change_1, &change_2, &base_changing);

        // Execute merge action
        match merge_action {
            DirChangeMerge::TakeEither | DirChangeMerge::Take1 => {
                if let Some(leaf) = changing_to_leaf(&name, &changing_1) {
                    let complete: Arc<dyn Complete> = Arc::new(NoopComplete);
                    builder.add(leaf, complete).await?;
                }
            }
            DirChangeMerge::Take2 => {
                if let Some(leaf) = changing_to_leaf(&name, &changing_2) {
                    let complete: Arc<dyn Complete> = Arc::new(NoopComplete);
                    builder.add(leaf, complete).await?;
                }
            }
            DirChangeMerge::Remove => {
                // Skip this entry - don't add to builder
            }
            DirChangeMerge::TakeFile(fe) => {
                let mut fe_with_name = fe;
                fe_with_name.name = name.clone();
                let complete: Arc<dyn Complete> = Arc::new(NoopComplete);
                builder
                    .add(DirectoryLeaf::File(fe_with_name), complete)
                    .await?;
            }
            DirChangeMerge::MergeDirectories { base, dir_1, dir_2 } => {
                // Recursively merge directories
                let merge_result = Box::pin(merge_directories(
                    Arc::clone(&repo),
                    base.as_ref(),
                    &dir_1,
                    &dir_2,
                    max_merge_memory,
                ))
                .await?;

                // Add the merged directory to builder
                let dir_entry = DirEntry {
                    name: name.clone(),
                    directory: merge_result.directory.result.clone(),
                };
                completes
                    .add(merge_result.directory.complete)
                    .map_err(|e| MergeError::Repo(crate::repo::RepoError::Other(e.to_string())))?;
                let complete: Arc<dyn Complete> = Arc::new(NoopComplete);
                builder.add(DirectoryLeaf::Dir(dir_entry), complete).await?;

                // Collect any conflicts from the recursive merge
                conflicts.extend(merge_result.conflicts);
            }
            DirChangeMerge::MergeFiles {
                base,
                fe1,
                fe2,
                executable,
            } => {
                let context = ConflictContext {
                    name: name.clone(),
                    base: base_changing.clone(),
                    entry_1: changing_1.clone(),
                    entry_2: changing_2.clone(),
                    change_1: change_1.clone(),
                    change_2: change_2.clone(),
                    merge_file_result: None,
                };

                let merge_result = merge_files(
                    Arc::clone(&repo),
                    &name,
                    base.as_ref(),
                    &fe1,
                    &fe2,
                    executable,
                    max_merge_memory,
                    context.clone(),
                )
                .await?;

                match merge_result {
                    MergeFileResult::Merged { file, complete } => {
                        builder.add(DirectoryLeaf::File(file), complete).await?;
                    }
                    MergeFileResult::Conflict(ctx) => {
                        // Build a conflict directory containing all versions
                        let conflict_result =
                            build_conflict_directory(Arc::clone(&repo), (*ctx).clone()).await?;

                        // Add the conflict directory to the builder
                        completes.add(conflict_result.complete).map_err(|e| {
                            MergeError::Repo(crate::repo::RepoError::Other(e.to_string()))
                        })?;
                        builder
                            .add(
                                conflict_result.result,
                                Arc::new(NoopComplete) as Arc<dyn Complete>,
                            )
                            .await?;

                        // Record the conflict for reporting
                        conflicts.push(*ctx);
                    }
                }
            }
            DirChangeMerge::Conflict => {
                let context = ConflictContext {
                    name: name.clone(),
                    base: base_changing.clone(),
                    entry_1: changing_1.clone(),
                    entry_2: changing_2.clone(),
                    change_1: change_1.clone(),
                    change_2: change_2.clone(),
                    merge_file_result: None,
                };

                // Build a conflict directory containing all versions
                let conflict_result =
                    build_conflict_directory(Arc::clone(&repo), context.clone()).await?;

                // Add the conflict directory to the builder
                completes
                    .add(conflict_result.complete)
                    .map_err(|e| MergeError::Repo(crate::repo::RepoError::Other(e.to_string())))?;
                builder
                    .add(
                        conflict_result.result,
                        Arc::new(NoopComplete) as Arc<dyn Complete>,
                    )
                    .await?;

                // Record the conflict for reporting
                conflicts.push(context);
            }
        }

        // Advance iterators that matched
        if advance_base && let Some(ref mut list) = base_list {
            current_base = list.next().await?;
        }
        if advance_1 {
            current_1 = list_1.next().await?;
        }
        if advance_2 {
            current_2 = list_2.next().await?;
        }
    }

    // Finish building
    let result = builder.finish().await?;
    completes.done();

    Ok(MergeDirectoryResult {
        directory: WithComplete {
            result: result.hash,
            complete: result.complete,
        },
        conflicts,
    })
}

// =============================================================================
// Helper Functions
// =============================================================================

/// Get the minimum name from up to 3 optional entries.
fn min_entry_name<'a>(
    e1: Option<&'a DirectoryEntry>,
    e2: Option<&'a DirectoryEntry>,
    e3: Option<&'a DirectoryEntry>,
) -> Option<&'a str> {
    let names = [
        e1.map(|e| e.name()),
        e2.map(|e| e.name()),
        e3.map(|e| e.name()),
    ];
    names.into_iter().flatten().min()
}

/// Check if the entry matches the given name, return the entry and whether to advance.
fn collect_if_matches<'a>(
    entry: &'a Option<DirectoryEntry>,
    name: &str,
) -> (Option<&'a DirectoryEntry>, bool) {
    match entry {
        Some(e) if e.name() == name => (Some(e), true),
        _ => (None, false),
    }
}

/// Convert a DirectoryEntry to ChangingDirEntry.
fn to_changing_dir_entry(entry: Option<&DirectoryEntry>) -> ChangingDirEntry {
    match entry {
        None => ChangingDirEntry::None,
        Some(DirectoryEntry::File(fe)) => ChangingDirEntry::File(fe.clone()),
        Some(DirectoryEntry::Directory(de)) => ChangingDirEntry::Directory(de.directory.clone()),
    }
}

/// Convert a ChangingDirEntry to DirectoryLeaf for the builder.
fn changing_to_leaf(name: &str, entry: &ChangingDirEntry) -> Option<DirectoryLeaf> {
    match entry {
        ChangingDirEntry::None => None,
        ChangingDirEntry::File(fe) => {
            let mut fe_with_name = fe.clone();
            fe_with_name.name = name.to_string();
            Some(DirectoryLeaf::File(fe_with_name))
        }
        ChangingDirEntry::Directory(d) => Some(DirectoryLeaf::Dir(DirEntry {
            name: name.to_string(),
            directory: d.clone(),
        })),
    }
}
