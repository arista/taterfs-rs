//! Determine merge actions from directory entry changes.
//!
//! This module implements the `changes_to_merge` function that determines
//! what merge action to take given two changes from a common base.

use crate::merge::types::{ChangingDirEntry, DirChangeMerge, DirEntryChange};
use crate::repository::FileEntry;

/// Determine the merge action for two changes from the same base.
///
/// # Arguments
/// * `c1` - Change from base to dir_1
/// * `c2` - Change from base to dir_2
/// * `base` - The base entry (needed for resolving executable when only executable changes)
///
/// # Returns
/// The `DirChangeMerge` describing what action to take.
pub fn changes_to_merge(
    c1: &DirEntryChange,
    c2: &DirEntryChange,
    _base: &ChangingDirEntry,
) -> DirChangeMerge {
    // If both changed the same way, take either
    if c1 == c2 {
        return DirChangeMerge::TakeEither;
    }

    // If only one is unchanged, take the other
    if matches!(c1, DirEntryChange::Unchanged) {
        return DirChangeMerge::Take2;
    }
    if matches!(c2, DirEntryChange::Unchanged) {
        return DirChangeMerge::Take1;
    }

    // If both removed, remove
    if c1.is_removed() && c2.is_removed() {
        return DirChangeMerge::Remove;
    }

    // If both directories changed, but not in the same way, merge them
    if let (
        DirEntryChange::DirectoryChanged {
            base: b,
            directory: d1,
        },
        DirEntryChange::DirectoryChanged { directory: d2, .. },
    ) = (c1, c2)
    {
        return DirChangeMerge::MergeDirectories {
            base: Some(b.clone()),
            dir_1: d1.clone(),
            dir_2: d2.clone(),
        };
    }

    // If both created a new directory, treat as a merge with no base
    if let (Some(d1), Some(d2)) = (c1.new_directory(), c2.new_directory()) {
        return DirChangeMerge::MergeDirectories {
            base: None,
            dir_1: d1.clone(),
            dir_2: d2.clone(),
        };
    }

    // If both files changed, merge them
    if let (
        DirEntryChange::FileChanged {
            base: base_fe,
            content_change: fc1,
            executable_change: ec1,
        },
        DirEntryChange::FileChanged {
            content_change: fc2,
            executable_change: ec2,
            ..
        },
    ) = (c1, c2)
    {
        // Resolve executable: first available from e1, e2, or base
        let executable = ec1.or(*ec2).unwrap_or(base_fe.executable);

        match (fc1, fc2) {
            // Both changed content -> need to merge files
            (Some(fe1), Some(fe2)) => {
                return DirChangeMerge::MergeFiles {
                    base: Some(base_fe.clone()),
                    fe1: with_executable(fe1, executable),
                    fe2: with_executable(fe2, executable),
                    executable,
                };
            }
            // Only c1 changed content -> take c1's file with resolved executable
            (Some(fe1), None) => {
                return DirChangeMerge::TakeFile(with_executable(fe1, executable));
            }
            // Only c2 changed content -> take c2's file with resolved executable
            (None, Some(fe2)) => {
                return DirChangeMerge::TakeFile(with_executable(fe2, executable));
            }
            // Neither changed content (both only changed executable) -> take base with resolved executable
            (None, None) => {
                return DirChangeMerge::TakeFile(with_executable(base_fe, executable));
            }
        }
    }

    // If both created a new file, treat as a merge with no base
    if let (Some(fe1), Some(fe2)) = (c1.new_file(), c2.new_file()) {
        if fe1.executable == fe2.executable {
            return DirChangeMerge::MergeFiles {
                base: None,
                fe1: fe1.clone(),
                fe2: fe2.clone(),
                executable: fe1.executable,
            };
        } else {
            // Conflict: different executable flags on new files
            return DirChangeMerge::Conflict;
        }
    }

    // All other cases are treated as conflicts
    DirChangeMerge::Conflict
}

/// Create a copy of a FileEntry with a different executable flag.
fn with_executable(fe: &FileEntry, executable: bool) -> FileEntry {
    FileEntry {
        name: fe.name.clone(),
        size: fe.size,
        executable,
        file: fe.file.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::repository::FileEntry;

    fn make_file_entry(file: &str, executable: bool) -> FileEntry {
        FileEntry {
            name: "test".to_string(),
            size: 100,
            executable,
            file: file.to_string(),
        }
    }

    #[test]
    fn test_both_unchanged_takes_either() {
        let result = changes_to_merge(
            &DirEntryChange::Unchanged,
            &DirEntryChange::Unchanged,
            &ChangingDirEntry::None,
        );
        assert!(matches!(result, DirChangeMerge::TakeEither));
    }

    #[test]
    fn test_one_unchanged_takes_other() {
        let fe = make_file_entry("abc", false);
        let result = changes_to_merge(
            &DirEntryChange::Unchanged,
            &DirEntryChange::FileCreated(fe),
            &ChangingDirEntry::None,
        );
        assert!(matches!(result, DirChangeMerge::Take2));

        let fe = make_file_entry("abc", false);
        let result = changes_to_merge(
            &DirEntryChange::FileCreated(fe),
            &DirEntryChange::Unchanged,
            &ChangingDirEntry::None,
        );
        assert!(matches!(result, DirChangeMerge::Take1));
    }

    #[test]
    fn test_both_removed() {
        // Identical removals return TakeEither (same change on both sides)
        let result = changes_to_merge(
            &DirEntryChange::FileRemoved,
            &DirEntryChange::FileRemoved,
            &ChangingDirEntry::None,
        );
        assert!(matches!(result, DirChangeMerge::TakeEither));

        let result = changes_to_merge(
            &DirEntryChange::DirectoryRemoved,
            &DirEntryChange::DirectoryRemoved,
            &ChangingDirEntry::None,
        );
        assert!(matches!(result, DirChangeMerge::TakeEither));

        // Different removal types (impossible in practice, but handled) return Remove
        let result = changes_to_merge(
            &DirEntryChange::FileRemoved,
            &DirEntryChange::DirectoryRemoved,
            &ChangingDirEntry::None,
        );
        assert!(matches!(result, DirChangeMerge::Remove));
    }

    #[test]
    fn test_both_directories_changed() {
        let result = changes_to_merge(
            &DirEntryChange::DirectoryChanged {
                base: "base".to_string(),
                directory: "d1".to_string(),
            },
            &DirEntryChange::DirectoryChanged {
                base: "base".to_string(),
                directory: "d2".to_string(),
            },
            &ChangingDirEntry::Directory("base".to_string()),
        );
        match result {
            DirChangeMerge::MergeDirectories { base, dir_1, dir_2 } => {
                assert_eq!(base, Some("base".to_string()));
                assert_eq!(dir_1, "d1");
                assert_eq!(dir_2, "d2");
            }
            _ => panic!("Expected MergeDirectories"),
        }
    }

    #[test]
    fn test_both_created_directories() {
        let result = changes_to_merge(
            &DirEntryChange::DirectoryCreated("d1".to_string()),
            &DirEntryChange::DirectoryCreated("d2".to_string()),
            &ChangingDirEntry::None,
        );
        match result {
            DirChangeMerge::MergeDirectories { base, dir_1, dir_2 } => {
                assert!(base.is_none());
                assert_eq!(dir_1, "d1");
                assert_eq!(dir_2, "d2");
            }
            _ => panic!("Expected MergeDirectories with no base"),
        }
    }

    #[test]
    fn test_both_files_content_changed() {
        let base_fe = make_file_entry("base", false);
        let fe1 = make_file_entry("f1", false);
        let fe2 = make_file_entry("f2", false);

        let result = changes_to_merge(
            &DirEntryChange::FileChanged {
                base: base_fe.clone(),
                content_change: Some(fe1.clone()),
                executable_change: None,
            },
            &DirEntryChange::FileChanged {
                base: base_fe.clone(),
                content_change: Some(fe2.clone()),
                executable_change: None,
            },
            &ChangingDirEntry::File(base_fe),
        );

        match result {
            DirChangeMerge::MergeFiles {
                base,
                fe1: result_fe1,
                fe2: result_fe2,
                executable,
            } => {
                assert!(base.is_some());
                assert_eq!(result_fe1.file, "f1");
                assert_eq!(result_fe2.file, "f2");
                assert!(!executable);
            }
            _ => panic!("Expected MergeFiles"),
        }
    }

    #[test]
    fn test_one_content_one_executable_change() {
        let base_fe = make_file_entry("base", false);
        let fe1 = make_file_entry("f1", false);

        let result = changes_to_merge(
            &DirEntryChange::FileChanged {
                base: base_fe.clone(),
                content_change: Some(fe1.clone()),
                executable_change: None,
            },
            &DirEntryChange::FileChanged {
                base: base_fe.clone(),
                content_change: None,
                executable_change: Some(true),
            },
            &ChangingDirEntry::File(base_fe),
        );

        match result {
            DirChangeMerge::TakeFile(fe) => {
                assert_eq!(fe.file, "f1");
                assert!(fe.executable); // Resolved from c2's executable change
            }
            _ => panic!("Expected TakeFile"),
        }
    }

    #[test]
    fn test_both_only_executable_changed() {
        let base_fe = make_file_entry("base", false);

        let result = changes_to_merge(
            &DirEntryChange::FileChanged {
                base: base_fe.clone(),
                content_change: None,
                executable_change: Some(true),
            },
            &DirEntryChange::FileChanged {
                base: base_fe.clone(),
                content_change: None,
                executable_change: Some(true),
            },
            &ChangingDirEntry::File(base_fe.clone()),
        );

        // Same change -> TakeEither
        assert!(matches!(result, DirChangeMerge::TakeEither));
    }

    #[test]
    fn test_both_created_files_same_executable() {
        let fe1 = make_file_entry("f1", false);
        let fe2 = make_file_entry("f2", false);

        let result = changes_to_merge(
            &DirEntryChange::FileCreated(fe1),
            &DirEntryChange::FileCreated(fe2),
            &ChangingDirEntry::None,
        );

        match result {
            DirChangeMerge::MergeFiles {
                base, executable, ..
            } => {
                assert!(base.is_none());
                assert!(!executable);
            }
            _ => panic!("Expected MergeFiles"),
        }
    }

    #[test]
    fn test_both_created_files_different_executable() {
        let fe1 = make_file_entry("f1", false);
        let fe2 = make_file_entry("f2", true);

        let result = changes_to_merge(
            &DirEntryChange::FileCreated(fe1),
            &DirEntryChange::FileCreated(fe2),
            &ChangingDirEntry::None,
        );

        assert!(matches!(result, DirChangeMerge::Conflict));
    }

    #[test]
    fn test_file_vs_directory_conflict() {
        let fe = make_file_entry("f1", false);

        let result = changes_to_merge(
            &DirEntryChange::FileCreated(fe),
            &DirEntryChange::DirectoryCreated("d1".to_string()),
            &ChangingDirEntry::None,
        );

        assert!(matches!(result, DirChangeMerge::Conflict));
    }

    #[test]
    fn test_remove_vs_modify_conflict() {
        let fe = make_file_entry("f1", false);

        let result = changes_to_merge(
            &DirEntryChange::FileRemoved,
            &DirEntryChange::FileCreated(fe),
            &ChangingDirEntry::None,
        );

        assert!(matches!(result, DirChangeMerge::Conflict));
    }
}
