//! Type definitions for 3-way directory merge.
//!
//! These types represent the state changes during merge operations,
//! as specified in docs/architecture/merge.md.

use crate::repository::{FileEntry, ObjectId};

// =============================================================================
// ChangingDirEntry
// =============================================================================

/// Represents an entry in a directory during merge comparison.
/// This is the "before" or "after" state of an entry.
///
/// Note: `FileEntry` includes `name`, but during merge the name comes from
/// the zipper iteration context.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChangingDirEntry {
    /// No entry at this name.
    None,
    /// A file entry.
    File(FileEntry),
    /// A directory entry with its ObjectId.
    Directory(ObjectId),
}

impl ChangingDirEntry {
    /// Returns true if this is the None variant.
    pub fn is_none(&self) -> bool {
        matches!(self, ChangingDirEntry::None)
    }

    /// Returns the FileEntry if this is a File variant.
    pub fn as_file(&self) -> Option<&FileEntry> {
        match self {
            ChangingDirEntry::File(fe) => Some(fe),
            _ => None,
        }
    }

    /// Returns the directory ObjectId if this is a Directory variant.
    pub fn as_directory(&self) -> Option<&ObjectId> {
        match self {
            ChangingDirEntry::Directory(d) => Some(d),
            _ => None,
        }
    }
}

// =============================================================================
// DirEntryChange
// =============================================================================

/// Describes the change from a base entry to a derived entry.
///
/// The `FileChanged` variant uses Options to indicate which aspects changed:
/// - `content_change`: Some(FileEntry) if the file content (ObjectId) changed
/// - `executable_change`: Some(bool) if the executable flag changed
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DirEntryChange {
    /// No change - base and derived are identical.
    Unchanged,
    /// A new file was created (base was None).
    FileCreated(FileEntry),
    /// File was modified - content and/or executable changed.
    FileChanged {
        /// The base file entry.
        base: FileEntry,
        /// The new file entry if content (ObjectId) changed.
        content_change: Option<FileEntry>,
        /// The new executable value if it changed.
        executable_change: Option<bool>,
    },
    /// File was changed to a directory.
    FileChangedToDirectory {
        /// The base file that was replaced.
        base: FileEntry,
        /// The new directory ObjectId.
        directory: ObjectId,
    },
    /// File was removed.
    FileRemoved,
    /// A new directory was created (base was None).
    DirectoryCreated(ObjectId),
    /// Directory content changed (different ObjectId).
    DirectoryChanged {
        /// The base directory ObjectId.
        base: ObjectId,
        /// The new directory ObjectId.
        directory: ObjectId,
    },
    /// Directory was changed to a file.
    DirectoryChangedToFile {
        /// The base directory ObjectId.
        base: ObjectId,
        /// The new file entry.
        file: FileEntry,
    },
    /// Directory was removed.
    DirectoryRemoved,
}

impl DirEntryChange {
    /// Returns the new directory ObjectId if this change results in a directory.
    pub fn new_directory(&self) -> Option<&ObjectId> {
        match self {
            DirEntryChange::FileChangedToDirectory { directory, .. } => Some(directory),
            DirEntryChange::DirectoryCreated(d) => Some(d),
            _ => None,
        }
    }

    /// Returns the new FileEntry if this change creates a new file.
    pub fn new_file(&self) -> Option<&FileEntry> {
        match self {
            DirEntryChange::FileCreated(fe) => Some(fe),
            DirEntryChange::DirectoryChangedToFile { file, .. } => Some(file),
            _ => None,
        }
    }

    /// Returns true if this change removes the entry.
    pub fn is_removed(&self) -> bool {
        matches!(
            self,
            DirEntryChange::FileRemoved | DirEntryChange::DirectoryRemoved
        )
    }
}

// =============================================================================
// DirChangeMerge
// =============================================================================

/// The result of comparing two DirEntryChanges - what should happen in the merge.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DirChangeMerge {
    /// Both changes are identical - take either one.
    TakeEither,
    /// Take the first entry (from dir_1).
    Take1,
    /// Take the second entry (from dir_2).
    Take2,
    /// Remove the entry (both sides removed it).
    Remove,
    /// Recursively merge two directories.
    MergeDirectories {
        /// Base directory ObjectId (None if both sides created new directories).
        base: Option<ObjectId>,
        /// Directory ObjectId from dir_1.
        dir_1: ObjectId,
        /// Directory ObjectId from dir_2.
        dir_2: ObjectId,
    },
    /// Attempt to merge two files (text merge).
    MergeFiles {
        /// Base file entry (None if both sides created new files).
        base: Option<FileEntry>,
        /// File entry from dir_1.
        fe1: FileEntry,
        /// File entry from dir_2.
        fe2: FileEntry,
        /// Resolved executable flag for the merged file.
        executable: bool,
    },
    /// Take a file (possibly with merged executable from both sides).
    TakeFile(FileEntry),
    /// Conflict that cannot be auto-resolved.
    Conflict,
}

// =============================================================================
// ConflictContext
// =============================================================================

/// Context information for a merge conflict.
///
/// Provides all the information needed to understand and potentially
/// resolve a conflict.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConflictContext {
    /// Name of the entry that conflicted.
    pub name: String,
    /// Base entry state.
    pub base: ChangingDirEntry,
    /// Entry state in dir_1.
    pub entry_1: ChangingDirEntry,
    /// Entry state in dir_2.
    pub entry_2: ChangingDirEntry,
    /// Change from base to entry_1.
    pub change_1: DirEntryChange,
    /// Change from base to entry_2.
    pub change_2: DirEntryChange,
}
