//! Directory tree modification.
//!
//! This module provides `DirTreeModSpec` for specifying batch modifications to a directory
//! hierarchy, and `mod_dir_tree` for executing those modifications.
//!
//! The key benefit is efficiency: multiple directory modifications can be applied in a single
//! pass, avoiding wasteful intermediate directory objects that would never be referenced again.

use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::collections::btree_map;
use std::path::Path;
use std::sync::Arc;

use crate::app::list_builder::{DirectoryLeaf, DirectoryListBuilder};
use crate::repo::{DirectoryEntry, Repo, RepoError};
use crate::repository::{DirEntry, Directory, DirectoryType, ObjectId};
use crate::util::{Complete, NoopComplete, WithComplete};

// =============================================================================
// Error Types
// =============================================================================

/// Error type for DirTreeModSpec operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DirTreeModSpecError {
    /// Path is empty (root modification not allowed).
    EmptyPath,
    /// Path overlaps with an existing modification.
    OverlappingPath(String),
}

impl std::fmt::Display for DirTreeModSpecError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DirTreeModSpecError::EmptyPath => {
                write!(f, "path must have at least one component")
            }
            DirTreeModSpecError::OverlappingPath(path) => {
                write!(f, "path overlaps with existing modification: {}", path)
            }
        }
    }
}

impl std::error::Error for DirTreeModSpecError {}

// =============================================================================
// DirTreeModSpec
// =============================================================================

/// Specification for modifying a directory tree.
///
/// This acts as a "virtual directory hierarchy" that the application builds up
/// to specify multiple modifications to be applied at once.
///
/// # Example
///
/// ```ignore
/// let mut spec = DirTreeModSpec::new();
/// spec.add(Path::new("foo/bar.txt"), file_entry, complete)?;
/// spec.remove(Path::new("old/file.txt"))?;
/// let result = mod_dir_tree(&repo, directory, spec).await?;
/// ```
#[derive(Default)]
pub struct DirTreeModSpec {
    entries: BTreeMap<String, DirTreeModSpecEntry>,
}

/// An entry in a DirTreeModSpec.
pub enum DirTreeModSpecEntry {
    /// Remove the entry at this path.
    Remove,
    /// Set the entry to this value.
    Entry(DirectoryEntry, Arc<dyn Complete>),
    /// Recursively modify the subdirectory at this path.
    Directory(DirTreeModSpec),
}

impl DirTreeModSpec {
    /// Create a new empty DirTreeModSpec.
    pub fn new() -> Self {
        Self {
            entries: BTreeMap::new(),
        }
    }

    /// Add an entry at the specified path.
    ///
    /// The path must have at least one component (not the root).
    /// Returns an error if the path overlaps with an existing modification.
    pub fn add(
        &mut self,
        path: &Path,
        entry: DirectoryEntry,
        complete: Arc<dyn Complete>,
    ) -> Result<(), DirTreeModSpecError> {
        self.insert_at_path(path, DirTreeModSpecEntry::Entry(entry, complete))
    }

    /// Mark the specified path for removal.
    ///
    /// The path must have at least one component (not the root).
    /// Returns an error if the path overlaps with an existing modification.
    pub fn remove(&mut self, path: &Path) -> Result<(), DirTreeModSpecError> {
        self.insert_at_path(path, DirTreeModSpecEntry::Remove)
    }

    /// Get a sorted iterator over the entries.
    ///
    /// This consumes the spec and returns an iterator that yields entries
    /// in sorted order by name.
    pub fn into_entries(self) -> DirTreeModSpecEntryIter {
        DirTreeModSpecEntryIter {
            inner: self.entries.into_iter(),
        }
    }

    /// Check if this spec is empty (has no modifications).
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Internal method to insert an entry at a path.
    fn insert_at_path(
        &mut self,
        path: &Path,
        entry: DirTreeModSpecEntry,
    ) -> Result<(), DirTreeModSpecError> {
        let mut components: Vec<&str> = path
            .components()
            .filter_map(|c| {
                if let std::path::Component::Normal(s) = c {
                    s.to_str()
                } else {
                    None
                }
            })
            .collect();

        if components.is_empty() {
            return Err(DirTreeModSpecError::EmptyPath);
        }

        // Navigate to the final component
        let final_name = components.pop().unwrap().to_string();
        let mut current = self;

        for component in components {
            let component = component.to_string();

            // Check if there's already an entry at this path
            match current.entries.get(&component) {
                Some(DirTreeModSpecEntry::Directory(_)) => {
                    // Continue into the nested spec
                }
                Some(_) => {
                    // Overlap: trying to create subpath of an existing Entry or Remove
                    return Err(DirTreeModSpecError::OverlappingPath(component));
                }
                None => {
                    // Create a new nested spec
                    current.entries.insert(
                        component.clone(),
                        DirTreeModSpecEntry::Directory(DirTreeModSpec::new()),
                    );
                }
            }

            // Get mutable reference to the nested spec
            match current.entries.get_mut(&component) {
                Some(DirTreeModSpecEntry::Directory(nested)) => {
                    current = nested;
                }
                _ => unreachable!(),
            }
        }

        // Check for overlap at the final position
        if current.entries.contains_key(&final_name) {
            return Err(DirTreeModSpecError::OverlappingPath(final_name));
        }

        current.entries.insert(final_name, entry);
        Ok(())
    }
}

// =============================================================================
// DirTreeModSpecEntryIter
// =============================================================================

/// Iterator over DirTreeModSpec entries.
pub struct DirTreeModSpecEntryIter {
    inner: btree_map::IntoIter<String, DirTreeModSpecEntry>,
}

impl Iterator for DirTreeModSpecEntryIter {
    type Item = (String, DirTreeModSpecEntry);

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}

// =============================================================================
// mod_dir_tree function
// =============================================================================

/// Modify a directory tree according to the given specification.
///
/// This function applies the modifications specified in `spec` to the given
/// `directory`, using a "zipper" algorithm to efficiently merge the changes.
///
/// # Arguments
///
/// * `repo` - The repository context
/// * `directory` - The directory to modify
/// * `spec` - The specification of modifications to apply
///
/// # Returns
///
/// Returns the ObjectId of the new modified directory, along with a completion
/// handle that can be awaited to ensure all writes have completed.
pub async fn mod_dir_tree(
    repo: &Arc<Repo>,
    directory: Directory,
    spec: DirTreeModSpec,
) -> Result<WithComplete<ObjectId>, RepoError> {
    let mut builder = DirectoryListBuilder::new(Arc::clone(repo));

    // Get sorted iterators
    let mut dir_entries = repo.list_entries_of_directory(directory);
    let mut spec_entries = spec.into_entries().peekable();

    // Zipper merge
    let mut current_dir_entry = dir_entries.next().await?;

    loop {
        let current_spec_entry = spec_entries.peek();

        match (&current_dir_entry, current_spec_entry) {
            // Both exhausted - we're done
            (None, None) => break,

            // Only directory entries left - pass through remaining entries
            (Some(dir_entry), None) => {
                add_dir_entry_to_builder(&mut builder, dir_entry.clone()).await?;
                current_dir_entry = dir_entries.next().await?;
            }

            // Only spec entries left - add new entries (skip Remove since nothing to remove)
            (None, Some((name, _))) => {
                let name = name.clone();
                let (_, spec_entry) = spec_entries.next().unwrap();
                apply_spec_entry(repo, &mut builder, &name, spec_entry, None).await?;
            }

            // Both present - compare keys
            (Some(dir_entry), Some((name, _))) => {
                match dir_entry.name().cmp(name.as_str()) {
                    Ordering::Less => {
                        // Directory entry comes before spec entry - pass through
                        add_dir_entry_to_builder(&mut builder, dir_entry.clone()).await?;
                        current_dir_entry = dir_entries.next().await?;
                    }
                    Ordering::Greater => {
                        // Spec entry comes before directory entry - insert new entry
                        let name = name.clone();
                        let (_, spec_entry) = spec_entries.next().unwrap();
                        apply_spec_entry(repo, &mut builder, &name, spec_entry, None).await?;
                    }
                    Ordering::Equal => {
                        // Keys match - spec overrides existing
                        let name = name.clone();
                        let existing = current_dir_entry.take();
                        let (_, spec_entry) = spec_entries.next().unwrap();
                        apply_spec_entry(repo, &mut builder, &name, spec_entry, existing).await?;
                        current_dir_entry = dir_entries.next().await?;
                    }
                }
            }
        }
    }

    let result = builder.finish().await?;
    Ok(WithComplete::new(result.hash, result.complete))
}

/// Add a directory entry to the builder.
async fn add_dir_entry_to_builder(
    builder: &mut DirectoryListBuilder,
    entry: DirectoryEntry,
) -> Result<(), RepoError> {
    let leaf = dir_entry_to_leaf(entry);
    let complete: Arc<dyn Complete> = Arc::new(NoopComplete);
    builder.add(leaf, complete).await
}

/// Convert a DirectoryEntry to a DirectoryLeaf.
fn dir_entry_to_leaf(entry: DirectoryEntry) -> DirectoryLeaf {
    match entry {
        DirectoryEntry::File(f) => DirectoryLeaf::File(f),
        DirectoryEntry::Directory(d) => DirectoryLeaf::Dir(d),
    }
}

/// Apply a spec entry to the builder.
async fn apply_spec_entry(
    repo: &Arc<Repo>,
    builder: &mut DirectoryListBuilder,
    name: &str,
    spec_entry: DirTreeModSpecEntry,
    existing: Option<DirectoryEntry>,
) -> Result<(), RepoError> {
    match spec_entry {
        DirTreeModSpecEntry::Remove => {
            // Skip - don't add anything to the builder
        }
        DirTreeModSpecEntry::Entry(entry, complete) => {
            let leaf = dir_entry_to_leaf(entry);
            builder.add(leaf, complete).await?;
        }
        DirTreeModSpecEntry::Directory(subdir_spec) => {
            // Get or create the subdirectory to recurse into
            let subdir = if let Some(DirectoryEntry::Directory(dir_entry)) = existing {
                repo.read_directory(&dir_entry.directory).await?
            } else {
                // Create an empty directory
                Directory {
                    type_tag: DirectoryType::Directory,
                    entries: vec![],
                }
            };

            // Recurse
            let result = Box::pin(mod_dir_tree(repo, subdir, subdir_spec)).await?;

            // Add the resulting directory to the builder
            let dir_entry = DirEntry {
                name: name.to_string(),
                directory: result.result.clone(),
            };
            builder
                .add(DirectoryLeaf::Dir(dir_entry), result.complete)
                .await?;
        }
    }
    Ok(())
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::repository::FileEntry;
    use std::path::Path;

    #[test]
    fn test_spec_new() {
        let spec = DirTreeModSpec::new();
        assert!(spec.is_empty());
    }

    #[test]
    fn test_spec_add_single_component() {
        let mut spec = DirTreeModSpec::new();
        let entry = DirectoryEntry::File(FileEntry {
            name: "test.txt".to_string(),
            size: 100,
            executable: false,
            file: "abc123".to_string(),
        });
        let complete: Arc<dyn Complete> = Arc::new(NoopComplete);

        spec.add(Path::new("test.txt"), entry, complete).unwrap();
        assert!(!spec.is_empty());
    }

    #[test]
    fn test_spec_add_multi_component() {
        let mut spec = DirTreeModSpec::new();
        let entry = DirectoryEntry::File(FileEntry {
            name: "bar.txt".to_string(),
            size: 100,
            executable: false,
            file: "abc123".to_string(),
        });
        let complete: Arc<dyn Complete> = Arc::new(NoopComplete);

        spec.add(Path::new("foo/bar.txt"), entry, complete).unwrap();
        assert!(!spec.is_empty());

        // Check that intermediate Directory was created
        let mut iter = spec.into_entries();
        let (name, entry) = iter.next().unwrap();
        assert_eq!(name, "foo");
        assert!(matches!(entry, DirTreeModSpecEntry::Directory(_)));
    }

    #[test]
    fn test_spec_remove() {
        let mut spec = DirTreeModSpec::new();
        spec.remove(Path::new("old.txt")).unwrap();
        assert!(!spec.is_empty());
    }

    #[test]
    fn test_spec_empty_path_error() {
        let mut spec = DirTreeModSpec::new();
        let entry = DirectoryEntry::File(FileEntry {
            name: "test.txt".to_string(),
            size: 100,
            executable: false,
            file: "abc123".to_string(),
        });
        let complete: Arc<dyn Complete> = Arc::new(NoopComplete);

        let result = spec.add(Path::new(""), entry, complete);
        assert!(matches!(result, Err(DirTreeModSpecError::EmptyPath)));
    }

    #[test]
    fn test_spec_overlap_same_path() {
        let mut spec = DirTreeModSpec::new();
        let entry1 = DirectoryEntry::File(FileEntry {
            name: "test.txt".to_string(),
            size: 100,
            executable: false,
            file: "abc123".to_string(),
        });
        let entry2 = DirectoryEntry::File(FileEntry {
            name: "test.txt".to_string(),
            size: 200,
            executable: false,
            file: "def456".to_string(),
        });
        let complete1: Arc<dyn Complete> = Arc::new(NoopComplete);
        let complete2: Arc<dyn Complete> = Arc::new(NoopComplete);

        spec.add(Path::new("test.txt"), entry1, complete1).unwrap();
        let result = spec.add(Path::new("test.txt"), entry2, complete2);
        assert!(matches!(
            result,
            Err(DirTreeModSpecError::OverlappingPath(_))
        ));
    }

    #[test]
    fn test_spec_overlap_subpath() {
        let mut spec = DirTreeModSpec::new();
        let entry1 = DirectoryEntry::File(FileEntry {
            name: "foo".to_string(),
            size: 100,
            executable: false,
            file: "abc123".to_string(),
        });
        let entry2 = DirectoryEntry::File(FileEntry {
            name: "bar.txt".to_string(),
            size: 200,
            executable: false,
            file: "def456".to_string(),
        });
        let complete1: Arc<dyn Complete> = Arc::new(NoopComplete);
        let complete2: Arc<dyn Complete> = Arc::new(NoopComplete);

        // First add "foo" as a file
        spec.add(Path::new("foo"), entry1, complete1).unwrap();
        // Then try to add "foo/bar.txt" - should fail because "foo" is already a file
        let result = spec.add(Path::new("foo/bar.txt"), entry2, complete2);
        assert!(matches!(
            result,
            Err(DirTreeModSpecError::OverlappingPath(_))
        ));
    }

    #[test]
    fn test_entries_sorted() {
        let mut spec = DirTreeModSpec::new();
        let complete: Arc<dyn Complete> = Arc::new(NoopComplete);

        // Add in non-sorted order
        spec.remove(Path::new("zebra")).unwrap();
        spec.add(
            Path::new("alpha"),
            DirectoryEntry::File(FileEntry {
                name: "alpha".to_string(),
                size: 100,
                executable: false,
                file: "abc".to_string(),
            }),
            Arc::clone(&complete),
        )
        .unwrap();
        spec.remove(Path::new("middle")).unwrap();

        // Iterator should yield in sorted order
        let entries: Vec<_> = spec.into_entries().collect();
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].0, "alpha");
        assert_eq!(entries[1].0, "middle");
        assert_eq!(entries[2].0, "zebra");
    }
}
