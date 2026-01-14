//! Generic list builder for constructing tree-structured repository objects.
//!
//! This module provides a generic mechanism for building tree-structured lists
//! that can grow unbounded but are chunked into objects of a maximum size.
//! It is used for:
//! - `Branches` - list of branch entries
//! - `Directory` - list of directory entries (files and subdirectories)
//! - `File` - list of file parts (chunks)
//!
//! The list builder maintains a stack of partially-filled objects. When an object
//! fills up, it is written to the repository and a reference to it is added to
//! the next level up in the hierarchy.

use std::marker::PhantomData;
use std::sync::Arc;

use serde::Serialize;

use crate::repo::{Repo, RepoError};
use crate::repository::{
    Branch, BranchListEntry, Branches, BranchesEntry, BranchesType, ChunkFilePart, DirEntry,
    Directory, DirectoryPart, DirectoryType, File, FileEntry, FileFilePart, FilePart, FileType,
    ObjectId, PartialDirectory, RepoObject, MAX_BRANCH_LIST_ENTRIES, MAX_DIRECTORY_ENTRIES,
    MAX_FILE_PARTS,
};
use crate::util::{Complete, Completes};

// =============================================================================
// DirectoryLeaf
// =============================================================================

/// Leaf entry type for Directory list building.
///
/// A directory can contain either file entries or subdirectory entries.
/// This enum wraps both possibilities for use with the generic list builder.
#[derive(Debug, Clone)]
pub enum DirectoryLeaf {
    /// A file entry in the directory.
    File(FileEntry),
    /// A subdirectory entry.
    Dir(DirEntry),
}

impl DirectoryLeaf {
    /// Get the name of this directory leaf entry.
    pub fn name(&self) -> &str {
        match self {
            DirectoryLeaf::File(f) => &f.name,
            DirectoryLeaf::Dir(d) => &d.name,
        }
    }
}

impl From<FileEntry> for DirectoryLeaf {
    fn from(entry: FileEntry) -> Self {
        DirectoryLeaf::File(entry)
    }
}

impl From<DirEntry> for DirectoryLeaf {
    fn from(entry: DirEntry) -> Self {
        DirectoryLeaf::Dir(entry)
    }
}

// =============================================================================
// ListBuilderConfig Trait
// =============================================================================

/// Configuration trait for the generic list builder.
///
/// This trait defines the type mappings and conversion functions needed
/// to build a specific kind of list (Branches, Directory, or File).
pub trait ListBuilderConfig: Send + Sync {
    /// The object type being built (Branches, Directory, File).
    type Object: Serialize + Send + Sync + Clone;

    /// Entry type in the object (BranchListEntry, DirectoryPart, FilePart).
    type Item: Clone + Send + Sync;

    /// Leaf item type being added (Branch, DirectoryLeaf, ChunkFilePart).
    type Leaf: Send;

    /// Tree entry type for rollups (BranchesEntry, PartialDirectory, FileFilePart).
    type Tree: Send;

    /// Maximum number of items per node.
    const MAX_ITEMS: usize;

    /// Convert a leaf entry to an item.
    fn leaf_to_item(leaf: Self::Leaf) -> Self::Item;

    /// Convert a tree entry to an item.
    fn tree_to_item(tree: Self::Tree) -> Self::Item;

    /// Get the name from a leaf entry.
    ///
    /// Returns `None` for File entries which have no name concept.
    fn leaf_name(leaf: &Self::Leaf) -> Option<&str>;

    /// Get the first name from an item.
    ///
    /// For leaf items, this is the item's name.
    /// For tree items, this is the `first_name` field.
    /// Returns `None` for File items which have no name concept.
    fn item_first_name(item: &Self::Item) -> Option<&str>;

    /// Get the last name from an item.
    ///
    /// For leaf items, this is the item's name.
    /// For tree items, this is the `last_name` field.
    /// Returns `None` for File items which have no name concept.
    fn item_last_name(item: &Self::Item) -> Option<&str>;

    /// Create a tree entry from a hash and name bounds.
    ///
    /// For File, names will be `None` and ignored.
    fn make_tree(
        hash: ObjectId,
        first_name: Option<String>,
        last_name: Option<String>,
        obj: &Self::Object,
    ) -> Self::Tree;

    /// Create an object from a list of items.
    fn make_object(items: Vec<Self::Item>) -> Self::Object;

    /// Convert an object to a RepoObject for writing.
    fn to_repo_object(obj: Self::Object) -> RepoObject;
}

// =============================================================================
// BranchesConfig
// =============================================================================

/// Configuration for building a Branches list.
pub struct BranchesConfig;

impl ListBuilderConfig for BranchesConfig {
    type Object = Branches;
    type Item = BranchListEntry;
    type Leaf = Branch;
    type Tree = BranchesEntry;

    const MAX_ITEMS: usize = MAX_BRANCH_LIST_ENTRIES;

    fn leaf_to_item(leaf: Branch) -> BranchListEntry {
        BranchListEntry::Branch(leaf)
    }

    fn tree_to_item(tree: BranchesEntry) -> BranchListEntry {
        BranchListEntry::BranchesEntry(tree)
    }

    fn leaf_name(leaf: &Branch) -> Option<&str> {
        Some(&leaf.name)
    }

    fn item_first_name(item: &BranchListEntry) -> Option<&str> {
        match item {
            BranchListEntry::Branch(b) => Some(&b.name),
            BranchListEntry::BranchesEntry(e) => Some(&e.first_name),
        }
    }

    fn item_last_name(item: &BranchListEntry) -> Option<&str> {
        match item {
            BranchListEntry::Branch(b) => Some(&b.name),
            BranchListEntry::BranchesEntry(e) => Some(&e.last_name),
        }
    }

    fn make_tree(
        hash: ObjectId,
        first_name: Option<String>,
        last_name: Option<String>,
        _obj: &Branches,
    ) -> BranchesEntry {
        BranchesEntry {
            first_name: first_name.expect("BranchesEntry requires first_name"),
            last_name: last_name.expect("BranchesEntry requires last_name"),
            branches: hash,
        }
    }

    fn make_object(items: Vec<BranchListEntry>) -> Branches {
        Branches {
            type_tag: BranchesType::Branches,
            branches: items,
        }
    }

    fn to_repo_object(obj: Branches) -> RepoObject {
        RepoObject::Branches(obj)
    }
}

// =============================================================================
// DirectoryConfig
// =============================================================================

/// Configuration for building a Directory list.
pub struct DirectoryConfig;

impl ListBuilderConfig for DirectoryConfig {
    type Object = Directory;
    type Item = DirectoryPart;
    type Leaf = DirectoryLeaf;
    type Tree = PartialDirectory;

    const MAX_ITEMS: usize = MAX_DIRECTORY_ENTRIES;

    fn leaf_to_item(leaf: DirectoryLeaf) -> DirectoryPart {
        match leaf {
            DirectoryLeaf::File(f) => DirectoryPart::File(f),
            DirectoryLeaf::Dir(d) => DirectoryPart::Directory(d),
        }
    }

    fn tree_to_item(tree: PartialDirectory) -> DirectoryPart {
        DirectoryPart::Partial(tree)
    }

    fn leaf_name(leaf: &DirectoryLeaf) -> Option<&str> {
        Some(leaf.name())
    }

    fn item_first_name(item: &DirectoryPart) -> Option<&str> {
        match item {
            DirectoryPart::File(f) => Some(&f.name),
            DirectoryPart::Directory(d) => Some(&d.name),
            DirectoryPart::Partial(p) => Some(&p.first_name),
        }
    }

    fn item_last_name(item: &DirectoryPart) -> Option<&str> {
        match item {
            DirectoryPart::File(f) => Some(&f.name),
            DirectoryPart::Directory(d) => Some(&d.name),
            DirectoryPart::Partial(p) => Some(&p.last_name),
        }
    }

    fn make_tree(
        hash: ObjectId,
        first_name: Option<String>,
        last_name: Option<String>,
        _obj: &Directory,
    ) -> PartialDirectory {
        PartialDirectory {
            first_name: first_name.expect("PartialDirectory requires first_name"),
            last_name: last_name.expect("PartialDirectory requires last_name"),
            directory: hash,
        }
    }

    fn make_object(items: Vec<DirectoryPart>) -> Directory {
        Directory {
            type_tag: DirectoryType::Directory,
            entries: items,
        }
    }

    fn to_repo_object(obj: Directory) -> RepoObject {
        RepoObject::Directory(obj)
    }
}

// =============================================================================
// FileConfig
// =============================================================================

/// Configuration for building a File list.
pub struct FileConfig;

impl ListBuilderConfig for FileConfig {
    type Object = File;
    type Item = FilePart;
    type Leaf = ChunkFilePart;
    type Tree = FileFilePart;

    const MAX_ITEMS: usize = MAX_FILE_PARTS;

    fn leaf_to_item(leaf: ChunkFilePart) -> FilePart {
        FilePart::Chunk(leaf)
    }

    fn tree_to_item(tree: FileFilePart) -> FilePart {
        FilePart::File(tree)
    }

    fn leaf_name(_leaf: &ChunkFilePart) -> Option<&str> {
        None // File parts have no name concept
    }

    fn item_first_name(_item: &FilePart) -> Option<&str> {
        None // File parts have no name concept
    }

    fn item_last_name(_item: &FilePart) -> Option<&str> {
        None // File parts have no name concept
    }

    fn make_tree(
        hash: ObjectId,
        _first_name: Option<String>,
        _last_name: Option<String>,
        obj: &File,
    ) -> FileFilePart {
        // Calculate total size from all parts
        let size: u64 = obj.parts.iter().map(file_part_size).sum();
        FileFilePart { size, file: hash }
    }

    fn make_object(items: Vec<FilePart>) -> File {
        File {
            type_tag: FileType::File,
            parts: items,
        }
    }

    fn to_repo_object(obj: File) -> RepoObject {
        RepoObject::File(obj)
    }
}

/// Helper to get the size of a FilePart.
fn file_part_size(part: &FilePart) -> u64 {
    match part {
        FilePart::Chunk(c) => c.size,
        FilePart::File(f) => f.size,
    }
}

// =============================================================================
// ListBuilderStackItem
// =============================================================================

/// A single level in the list builder stack.
struct ListBuilderStackItem<I> {
    /// Items at this level.
    items: Vec<I>,
    /// Completion tracking for items at this level.
    completes: Completes,
}

impl<I> ListBuilderStackItem<I> {
    /// Create a new empty stack item.
    fn new() -> Self {
        Self {
            items: Vec::new(),
            completes: Completes::new(),
        }
    }
}

// =============================================================================
// ListResult
// =============================================================================

/// Result of finishing a list builder.
pub struct ListResult<O> {
    /// The final object at the root of the tree.
    pub object: O,
    /// The hash/object ID of the root object.
    pub hash: ObjectId,
    /// Completion handle for the entire tree.
    pub complete: Arc<dyn Complete>,
}

// =============================================================================
// ListBuilder
// =============================================================================

/// A generic builder for tree-structured repository objects.
///
/// The list builder maintains a stack of partially-filled objects. Items are
/// added at level 0 (the leaf level). When a level fills up to `MAX_ITEMS`,
/// it is written to the repository and a reference is added to the next level.
///
/// This pattern is used for Branches, Directory, and File objects, all of which
/// can grow unbounded but need to be chunked into manageable sizes.
pub struct ListBuilder<C: ListBuilderConfig> {
    repo: Arc<Repo>,
    stack: Vec<ListBuilderStackItem<C::Item>>,
    _marker: PhantomData<C>,
}

impl<C: ListBuilderConfig> ListBuilder<C> {
    /// Create a new list builder.
    pub fn new(repo: Arc<Repo>) -> Self {
        Self {
            repo,
            stack: vec![ListBuilderStackItem::new()],
            _marker: PhantomData,
        }
    }

    /// Add a leaf entry to the list.
    ///
    /// The entry is converted to an item and added at level 0.
    /// If level 0 is full, it will be rolled up before adding.
    pub async fn add(
        &mut self,
        leaf: C::Leaf,
        complete: Arc<dyn Complete>,
    ) -> Result<(), RepoError> {
        let item = C::leaf_to_item(leaf);
        self.add_item_at_level(item, 0, complete).await
    }

    /// Finish building and return the result.
    ///
    /// This rolls up all remaining items in the stack and returns
    /// the final root object with its hash and completion handle.
    pub async fn finish(mut self) -> Result<ListResult<C::Object>, RepoError> {
        // Roll up all levels except the top
        // Note: stack can grow during this process
        let mut ix = 0;
        while ix < self.stack.len() - 1 {
            if !self.stack_item_empty_at(ix) {
                self.rollup_stack_item_at(ix).await?;
            }
            ix += 1;
        }

        // Convert the top level to the final object
        let (obj, hash, completes) = self.stack_item_to_object(self.stack.len() - 1).await?;

        Ok(ListResult {
            object: obj,
            hash,
            complete: completes,
        })
    }

    // =========================================================================
    // Internal Methods
    // =========================================================================

    /// Add an item at the specified level.
    async fn add_item_at_level(
        &mut self,
        item: C::Item,
        ix: usize,
        complete: Arc<dyn Complete>,
    ) -> Result<(), RepoError> {
        self.ensure_space_at(ix).await?;

        self.stack[ix].items.push(item);
        self.stack[ix]
            .completes
            .add(complete)
            .map_err(|e| RepoError::Other(e.to_string()))?;

        Ok(())
    }

    /// Ensure there is space at the given level, rolling up if necessary.
    async fn ensure_space_at(&mut self, ix: usize) -> Result<(), RepoError> {
        if self.stack_item_full_at(ix) {
            self.ensure_parent_of(ix);
            self.rollup_stack_item_at(ix).await?;
            self.clear_stack_item_at(ix);
        }
        Ok(())
    }

    /// Check if the stack item at the given level is full.
    fn stack_item_full_at(&self, ix: usize) -> bool {
        self.stack[ix].items.len() >= C::MAX_ITEMS
    }

    /// Check if the stack item at the given level is empty.
    fn stack_item_empty_at(&self, ix: usize) -> bool {
        self.stack[ix].items.is_empty()
    }

    /// Ensure a parent level exists above the given index.
    fn ensure_parent_of(&mut self, ix: usize) {
        if ix == self.stack.len() - 1 {
            self.stack.push(ListBuilderStackItem::new());
        }
    }

    /// Clear the stack item at the given level.
    fn clear_stack_item_at(&mut self, ix: usize) {
        self.stack[ix] = ListBuilderStackItem::new();
    }

    /// Roll up the stack item at the given level.
    ///
    /// This writes the current items as an object to the repository,
    /// creates a tree entry referencing it, and adds that entry to
    /// the next level up.
    async fn rollup_stack_item_at(&mut self, ix: usize) -> Result<(), RepoError> {
        // Get first and last names from items before taking them
        let first_name = self.items_first_name(ix);
        let last_name = self.items_last_name(ix);

        let (obj, hash, completes) = self.stack_item_to_object(ix).await?;

        // Create tree entry
        let tree = C::make_tree(hash, first_name, last_name, &obj);
        let tree_item = C::tree_to_item(tree);

        // Add tree item to the next level
        // Box the recursive call to avoid infinite future size
        Box::pin(self.add_item_at_level(tree_item, ix + 1, completes)).await
    }

    /// Convert the stack item at the given level to an object.
    ///
    /// This writes the object to the repository and sets up cache flags
    /// on completion.
    async fn stack_item_to_object(
        &mut self,
        ix: usize,
    ) -> Result<(C::Object, ObjectId, Arc<dyn Complete>), RepoError> {
        // Take items from the stack item
        let items = std::mem::take(&mut self.stack[ix].items);
        let completes = std::mem::take(&mut self.stack[ix].completes);

        // Create the object
        let obj = C::make_object(items);
        let repo_obj = C::to_repo_object(obj.clone());

        // Write to repository
        let write_result = self.repo.write_object(&repo_obj).await?;
        let hash = write_result.result;
        let write_complete = write_result.complete;

        // Spawn task to set exists flag when write completes
        let write_complete_for_exists = Arc::clone(&write_complete);
        let cache_for_exists = Arc::clone(self.repo.cache());
        let hash_for_exists = hash.clone();
        tokio::spawn(async move {
            if write_complete_for_exists.complete().await.is_ok() {
                let _ = cache_for_exists.set_object_exists(&hash_for_exists).await;
            }
        });

        // Add write completion to completes
        completes
            .add(write_complete)
            .map_err(|e| RepoError::Other(e.to_string()))?;

        // Mark completes as done - no more items will be added
        completes.done();

        // Wrap completes in Arc for spawning and returning
        let completes = Arc::new(completes);

        // Spawn task to set fully_stored flag when all completes finish
        let completes_for_stored = Arc::clone(&completes);
        let cache_for_stored = Arc::clone(self.repo.cache());
        let hash_for_stored = hash.clone();
        tokio::spawn(async move {
            if completes_for_stored.complete().await.is_ok() {
                let _ = cache_for_stored.set_object_fully_stored(&hash_for_stored).await;
            }
        });

        Ok((obj, hash, completes))
    }

    /// Get the first name from items at the given level.
    fn items_first_name(&self, ix: usize) -> Option<String> {
        self.stack[ix]
            .items
            .first()
            .and_then(C::item_first_name)
            .map(String::from)
    }

    /// Get the last name from items at the given level.
    fn items_last_name(&self, ix: usize) -> Option<String> {
        self.stack[ix]
            .items
            .last()
            .and_then(C::item_last_name)
            .map(String::from)
    }
}

// =============================================================================
// Type Aliases
// =============================================================================

/// A list builder for Branches objects.
pub type BranchListBuilder = ListBuilder<BranchesConfig>;

/// A list builder for Directory objects.
pub type DirectoryListBuilder = ListBuilder<DirectoryConfig>;

/// A list builder for File objects.
pub type FileListBuilder = ListBuilder<FileConfig>;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::MemoryBackend;
    use crate::caches::NoopCache;
    use crate::util::NotifyComplete;

    fn create_test_repo() -> Arc<Repo> {
        let backend = MemoryBackend::new();
        let cache = NoopCache;
        Arc::new(Repo::new(backend, cache))
    }

    #[tokio::test]
    async fn test_branch_list_builder_single_item() {
        let repo = create_test_repo();

        let mut builder = BranchListBuilder::new(repo);

        let branch = Branch {
            name: "main".to_string(),
            commit: "abc123".to_string(),
        };
        let complete = Arc::new(NotifyComplete::new());
        complete.notify_complete();

        builder.add(branch, complete).await.unwrap();

        let result = builder.finish().await.unwrap();
        assert_eq!(result.object.branches.len(), 1);

        match &result.object.branches[0] {
            BranchListEntry::Branch(b) => {
                assert_eq!(b.name, "main");
                assert_eq!(b.commit, "abc123");
            }
            _ => panic!("Expected Branch, got BranchesEntry"),
        }
    }

    #[tokio::test]
    async fn test_directory_list_builder_mixed_entries() {
        let repo = create_test_repo();

        let mut builder = DirectoryListBuilder::new(repo);

        // Add a file
        let file_entry = FileEntry {
            name: "readme.txt".to_string(),
            size: 100,
            executable: false,
            file: "file123".to_string(),
        };
        let complete1 = Arc::new(NotifyComplete::new());
        complete1.notify_complete();
        builder
            .add(DirectoryLeaf::File(file_entry), complete1)
            .await
            .unwrap();

        // Add a directory
        let dir_entry = DirEntry {
            name: "src".to_string(),
            directory: "dir456".to_string(),
        };
        let complete2 = Arc::new(NotifyComplete::new());
        complete2.notify_complete();
        builder
            .add(DirectoryLeaf::Dir(dir_entry), complete2)
            .await
            .unwrap();

        let result = builder.finish().await.unwrap();
        assert_eq!(result.object.entries.len(), 2);
    }

    #[tokio::test]
    async fn test_file_list_builder_single_chunk() {
        let repo = create_test_repo();

        let mut builder = FileListBuilder::new(repo);

        let chunk = ChunkFilePart {
            size: 1024,
            content: "chunk123".to_string(),
        };
        let complete = Arc::new(NotifyComplete::new());
        complete.notify_complete();

        builder.add(chunk, complete).await.unwrap();

        let result = builder.finish().await.unwrap();
        assert_eq!(result.object.parts.len(), 1);

        match &result.object.parts[0] {
            FilePart::Chunk(c) => {
                assert_eq!(c.size, 1024);
                assert_eq!(c.content, "chunk123");
            }
            _ => panic!("Expected Chunk, got File"),
        }
    }

    #[tokio::test]
    async fn test_directory_leaf_from() {
        let file_entry = FileEntry {
            name: "test.txt".to_string(),
            size: 100,
            executable: false,
            file: "file123".to_string(),
        };
        let leaf: DirectoryLeaf = file_entry.into();
        assert_eq!(leaf.name(), "test.txt");

        let dir_entry = DirEntry {
            name: "subdir".to_string(),
            directory: "dir456".to_string(),
        };
        let leaf: DirectoryLeaf = dir_entry.into();
        assert_eq!(leaf.name(), "subdir");
    }
}
