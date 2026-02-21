//! Compute directory entry changes for 3-way merge.
//!
//! This module implements the `to_dir_change` function that computes
//! the change from a base entry to a derived entry.

use crate::merge::error::{MergeError, Result};
use crate::merge::types::{ChangingDirEntry, DirEntryChange};

/// Compute the change from a base entry to a derived entry.
///
/// # Arguments
/// * `base` - The entry in the base directory (or `ChangingDirEntry::None` if absent)
/// * `derived` - The entry in the derived directory (or `ChangingDirEntry::None` if absent)
/// * `name` - The name of the entry (for error messages)
///
/// # Returns
/// The `DirEntryChange` describing the transformation from base to derived.
///
/// # Errors
/// Returns `MergeError::InvalidBaseState` if both base and derived are `None`.
pub fn to_dir_change(
    base: &ChangingDirEntry,
    derived: &ChangingDirEntry,
    name: &str,
) -> Result<DirEntryChange> {
    match (base, derived) {
        // None -> None: Invalid state
        (ChangingDirEntry::None, ChangingDirEntry::None) => Err(MergeError::InvalidBaseState {
            name: name.to_string(),
        }),

        // None -> File: File created
        (ChangingDirEntry::None, ChangingDirEntry::File(fe)) => {
            Ok(DirEntryChange::FileCreated(fe.clone()))
        }

        // None -> Directory: Directory created
        (ChangingDirEntry::None, ChangingDirEntry::Directory(d)) => {
            Ok(DirEntryChange::DirectoryCreated(d.clone()))
        }

        // File -> None: File removed
        (ChangingDirEntry::File(_), ChangingDirEntry::None) => Ok(DirEntryChange::FileRemoved),

        // File -> File: Check for changes
        (ChangingDirEntry::File(fe1), ChangingDirEntry::File(fe2)) => {
            let content_same = fe1.file == fe2.file;
            let executable_same = fe1.executable == fe2.executable;

            if content_same && executable_same {
                Ok(DirEntryChange::Unchanged)
            } else if content_same {
                // Only executable changed
                Ok(DirEntryChange::FileChanged {
                    base: fe1.clone(),
                    content_change: None,
                    executable_change: Some(fe2.executable),
                })
            } else if executable_same {
                // Only content changed
                Ok(DirEntryChange::FileChanged {
                    base: fe1.clone(),
                    content_change: Some(fe2.clone()),
                    executable_change: None,
                })
            } else {
                // Both changed
                Ok(DirEntryChange::FileChanged {
                    base: fe1.clone(),
                    content_change: Some(fe2.clone()),
                    executable_change: Some(fe2.executable),
                })
            }
        }

        // File -> Directory: File changed to directory
        (ChangingDirEntry::File(fe), ChangingDirEntry::Directory(d)) => {
            Ok(DirEntryChange::FileChangedToDirectory {
                base: fe.clone(),
                directory: d.clone(),
            })
        }

        // Directory -> None: Directory removed
        (ChangingDirEntry::Directory(_), ChangingDirEntry::None) => {
            Ok(DirEntryChange::DirectoryRemoved)
        }

        // Directory -> File: Directory changed to file
        (ChangingDirEntry::Directory(d), ChangingDirEntry::File(fe)) => {
            Ok(DirEntryChange::DirectoryChangedToFile {
                base: d.clone(),
                file: fe.clone(),
            })
        }

        // Directory -> Directory: Check for changes
        (ChangingDirEntry::Directory(d1), ChangingDirEntry::Directory(d2)) => {
            if d1 == d2 {
                Ok(DirEntryChange::Unchanged)
            } else {
                Ok(DirEntryChange::DirectoryChanged {
                    base: d1.clone(),
                    directory: d2.clone(),
                })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::repository::FileEntry;

    fn make_file_entry(file: &str, size: u64, executable: bool) -> FileEntry {
        FileEntry {
            name: "test".to_string(),
            size,
            executable,
            file: file.to_string(),
        }
    }

    #[test]
    fn test_none_to_none_is_error() {
        let result = to_dir_change(&ChangingDirEntry::None, &ChangingDirEntry::None, "test");
        assert!(matches!(result, Err(MergeError::InvalidBaseState { .. })));
    }

    #[test]
    fn test_none_to_file_is_created() {
        let fe = make_file_entry("abc123", 100, false);
        let result = to_dir_change(
            &ChangingDirEntry::None,
            &ChangingDirEntry::File(fe.clone()),
            "test",
        );
        assert!(matches!(result, Ok(DirEntryChange::FileCreated(_))));
    }

    #[test]
    fn test_none_to_directory_is_created() {
        let result = to_dir_change(
            &ChangingDirEntry::None,
            &ChangingDirEntry::Directory("dir123".to_string()),
            "test",
        );
        assert!(matches!(result, Ok(DirEntryChange::DirectoryCreated(_))));
    }

    #[test]
    fn test_file_to_none_is_removed() {
        let fe = make_file_entry("abc123", 100, false);
        let result = to_dir_change(&ChangingDirEntry::File(fe), &ChangingDirEntry::None, "test");
        assert!(matches!(result, Ok(DirEntryChange::FileRemoved)));
    }

    #[test]
    fn test_file_unchanged() {
        let fe = make_file_entry("abc123", 100, false);
        let result = to_dir_change(
            &ChangingDirEntry::File(fe.clone()),
            &ChangingDirEntry::File(fe),
            "test",
        );
        assert!(matches!(result, Ok(DirEntryChange::Unchanged)));
    }

    #[test]
    fn test_file_content_changed() {
        let fe1 = make_file_entry("abc123", 100, false);
        let fe2 = make_file_entry("def456", 200, false);
        let result = to_dir_change(
            &ChangingDirEntry::File(fe1),
            &ChangingDirEntry::File(fe2),
            "test",
        );
        match result {
            Ok(DirEntryChange::FileChanged {
                content_change,
                executable_change,
                ..
            }) => {
                assert!(content_change.is_some());
                assert!(executable_change.is_none());
            }
            _ => panic!("Expected FileChanged with content_change"),
        }
    }

    #[test]
    fn test_file_executable_changed() {
        let fe1 = make_file_entry("abc123", 100, false);
        let fe2 = make_file_entry("abc123", 100, true);
        let result = to_dir_change(
            &ChangingDirEntry::File(fe1),
            &ChangingDirEntry::File(fe2),
            "test",
        );
        match result {
            Ok(DirEntryChange::FileChanged {
                content_change,
                executable_change,
                ..
            }) => {
                assert!(content_change.is_none());
                assert_eq!(executable_change, Some(true));
            }
            _ => panic!("Expected FileChanged with executable_change"),
        }
    }

    #[test]
    fn test_file_both_changed() {
        let fe1 = make_file_entry("abc123", 100, false);
        let fe2 = make_file_entry("def456", 200, true);
        let result = to_dir_change(
            &ChangingDirEntry::File(fe1),
            &ChangingDirEntry::File(fe2),
            "test",
        );
        match result {
            Ok(DirEntryChange::FileChanged {
                content_change,
                executable_change,
                ..
            }) => {
                assert!(content_change.is_some());
                assert_eq!(executable_change, Some(true));
            }
            _ => panic!("Expected FileChanged with both changes"),
        }
    }

    #[test]
    fn test_file_to_directory() {
        let fe = make_file_entry("abc123", 100, false);
        let result = to_dir_change(
            &ChangingDirEntry::File(fe),
            &ChangingDirEntry::Directory("dir123".to_string()),
            "test",
        );
        assert!(matches!(
            result,
            Ok(DirEntryChange::FileChangedToDirectory { .. })
        ));
    }

    #[test]
    fn test_directory_to_none_is_removed() {
        let result = to_dir_change(
            &ChangingDirEntry::Directory("dir123".to_string()),
            &ChangingDirEntry::None,
            "test",
        );
        assert!(matches!(result, Ok(DirEntryChange::DirectoryRemoved)));
    }

    #[test]
    fn test_directory_to_file() {
        let fe = make_file_entry("abc123", 100, false);
        let result = to_dir_change(
            &ChangingDirEntry::Directory("dir123".to_string()),
            &ChangingDirEntry::File(fe),
            "test",
        );
        assert!(matches!(
            result,
            Ok(DirEntryChange::DirectoryChangedToFile { .. })
        ));
    }

    #[test]
    fn test_directory_unchanged() {
        let result = to_dir_change(
            &ChangingDirEntry::Directory("dir123".to_string()),
            &ChangingDirEntry::Directory("dir123".to_string()),
            "test",
        );
        assert!(matches!(result, Ok(DirEntryChange::Unchanged)));
    }

    #[test]
    fn test_directory_changed() {
        let result = to_dir_change(
            &ChangingDirEntry::Directory("dir123".to_string()),
            &ChangingDirEntry::Directory("dir456".to_string()),
            "test",
        );
        assert!(matches!(
            result,
            Ok(DirEntryChange::DirectoryChanged { .. })
        ));
    }
}
