//! Generic list search for finding entries in tree-structured repository objects.
//!
//! This module provides a binary search with recursive tree descent for finding
//! entries by key in tree-structured lists. It is used for:
//! - `Branches` - searching for a branch by name
//! - `Directory` - searching for a directory entry by name
//! - `File` - searching for a file chunk by byte position
//!
//! The search uses adapters to apply a single generic algorithm to different
//! kinds of lists (Branches, Directory, File).

use std::cmp::Ordering;
use std::sync::Arc;

use async_trait::async_trait;

use crate::repo::{DirectoryEntry, Repo, RepoError};
use crate::repository::{
    Branch, BranchListEntry, Branches, ChunkFilePart, DirectoryPart, File, FilePart, ObjectId,
};

// =============================================================================
// SearchableList Trait
// =============================================================================

/// A list that supports indexed access and binary search.
///
/// Implementations wrap a specific list type (Branches, Directory, File) and
/// provide the comparison and access operations needed for binary search with
/// recursive tree descent.
#[async_trait]
pub trait SearchableList: Send + Sized {
    /// The key type used for searching (String for names, u64 for byte positions).
    type Key: Ord + Send + Sync;

    /// The leaf entry type returned by a successful search.
    type Entry: Send;

    /// Returns the number of entries in this list.
    fn len(&self) -> usize;

    /// Compare a search key against the entry at the given index.
    ///
    /// For leaf entries: compares `key` against the leaf's key.
    /// For tree entries: compares `key` against the entry's key range.
    /// Returns `Less` if key < entry, `Greater` if key > entry, `Equal` if matching.
    fn compare(&self, ix: usize, key: &Self::Key) -> Ordering;

    /// Returns true if the entry at the given index is a leaf (not a tree reference).
    fn is_leaf(&self, ix: usize) -> bool;

    /// Returns the leaf entry at the given index.
    ///
    /// # Panics
    /// Panics if the entry at `ix` is not a leaf.
    fn leaf_entry(&self, ix: usize) -> Self::Entry;

    /// Loads and returns the sub-list referenced by the tree entry at the given index.
    ///
    /// # Panics
    /// Panics if the entry at `ix` is not a tree reference.
    async fn tree_sublist(&self, ix: usize) -> Result<Self, RepoError>;
}

// =============================================================================
// Generic Search Functions
// =============================================================================

/// Binary search over a searchable list for the given key.
///
/// Returns the index of the matching entry, or `None` if not found.
fn search_list_entries<L: SearchableList>(list: &L, key: &L::Key) -> Option<usize> {
    let mut lo = 0;
    let mut hi = list.len();

    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        match list.compare(mid, key) {
            Ordering::Less => hi = mid,
            Ordering::Greater => lo = mid + 1,
            Ordering::Equal => return Some(mid),
        }
    }

    None
}

/// Search a list for an entry matching the given key.
///
/// Performs a binary search and recursively descends into tree entries
/// until a leaf is found or the search fails.
pub async fn search_list<L: SearchableList>(
    list: &L,
    key: &L::Key,
) -> Result<Option<L::Entry>, RepoError> {
    match search_list_entries(list, key) {
        None => Ok(None),
        Some(ix) => {
            if list.is_leaf(ix) {
                Ok(Some(list.leaf_entry(ix)))
            } else {
                let sublist = list.tree_sublist(ix).await?;
                Box::pin(search_list(&sublist, key)).await
            }
        }
    }
}

// =============================================================================
// BranchesSearchList
// =============================================================================

/// Adapter for searching a Branches list by branch name.
pub struct BranchesSearchList {
    repo: Arc<Repo>,
    branches: Branches,
}

impl BranchesSearchList {
    /// Create a new searchable list wrapping the given Branches object.
    pub fn new(repo: Arc<Repo>, branches: Branches) -> Self {
        Self { repo, branches }
    }
}

#[async_trait]
impl SearchableList for BranchesSearchList {
    type Key = String;
    type Entry = Branch;

    fn len(&self) -> usize {
        self.branches.branches.len()
    }

    fn compare(&self, ix: usize, key: &String) -> Ordering {
        match &self.branches.branches[ix] {
            BranchListEntry::Branch(b) => key.as_str().cmp(b.name.as_str()),
            BranchListEntry::BranchesEntry(e) => {
                if key.as_str() < e.first_name.as_str() {
                    Ordering::Less
                } else if key.as_str() > e.last_name.as_str() {
                    Ordering::Greater
                } else {
                    Ordering::Equal
                }
            }
        }
    }

    fn is_leaf(&self, ix: usize) -> bool {
        matches!(&self.branches.branches[ix], BranchListEntry::Branch(_))
    }

    fn leaf_entry(&self, ix: usize) -> Branch {
        match &self.branches.branches[ix] {
            BranchListEntry::Branch(b) => b.clone(),
            _ => panic!("leaf_entry called on a tree entry"),
        }
    }

    async fn tree_sublist(&self, ix: usize) -> Result<Self, RepoError> {
        match &self.branches.branches[ix] {
            BranchListEntry::BranchesEntry(e) => {
                let branches = self.repo.read_branches(&e.branches).await?;
                Ok(Self::new(Arc::clone(&self.repo), branches))
            }
            _ => panic!("tree_sublist called on a leaf entry"),
        }
    }
}

// =============================================================================
// DirectorySearchList
// =============================================================================

/// Adapter for searching a Directory list by entry name.
pub struct DirectorySearchList {
    repo: Arc<Repo>,
    directory: crate::repository::Directory,
}

impl DirectorySearchList {
    /// Create a new searchable list wrapping the given Directory object.
    pub fn new(repo: Arc<Repo>, directory: crate::repository::Directory) -> Self {
        Self { repo, directory }
    }
}

#[async_trait]
impl SearchableList for DirectorySearchList {
    type Key = String;
    type Entry = DirectoryEntry;

    fn len(&self) -> usize {
        self.directory.entries.len()
    }

    fn compare(&self, ix: usize, key: &String) -> Ordering {
        match &self.directory.entries[ix] {
            DirectoryPart::File(f) => key.as_str().cmp(f.name.as_str()),
            DirectoryPart::Directory(d) => key.as_str().cmp(d.name.as_str()),
            DirectoryPart::Partial(p) => {
                if key.as_str() < p.first_name.as_str() {
                    Ordering::Less
                } else if key.as_str() > p.last_name.as_str() {
                    Ordering::Greater
                } else {
                    Ordering::Equal
                }
            }
        }
    }

    fn is_leaf(&self, ix: usize) -> bool {
        matches!(
            &self.directory.entries[ix],
            DirectoryPart::File(_) | DirectoryPart::Directory(_)
        )
    }

    fn leaf_entry(&self, ix: usize) -> DirectoryEntry {
        match &self.directory.entries[ix] {
            DirectoryPart::File(f) => DirectoryEntry::File(f.clone()),
            DirectoryPart::Directory(d) => DirectoryEntry::Directory(d.clone()),
            _ => panic!("leaf_entry called on a tree entry"),
        }
    }

    async fn tree_sublist(&self, ix: usize) -> Result<Self, RepoError> {
        match &self.directory.entries[ix] {
            DirectoryPart::Partial(p) => {
                let directory = self.repo.read_directory(&p.directory).await?;
                Ok(Self::new(Arc::clone(&self.repo), directory))
            }
            _ => panic!("tree_sublist called on a leaf entry"),
        }
    }
}

// =============================================================================
// FileSearchList
// =============================================================================

/// A file part with its precomputed byte position range.
struct FilePartWithPosition {
    start: u64,
    size: u64,
    part: FilePart,
}

/// Adapter for searching a File list by byte position.
pub struct FileSearchList {
    repo: Arc<Repo>,
    parts: Vec<FilePartWithPosition>,
}

impl FileSearchList {
    /// Create a new searchable list wrapping the given File object.
    ///
    /// Precomputes the start position for each file part as a running sum of sizes.
    pub fn new(repo: Arc<Repo>, file: File) -> Self {
        Self::with_offset(repo, file, 0)
    }

    /// Create a searchable list with positions offset by the given base.
    ///
    /// Used when descending into subtrees so that byte positions remain absolute.
    fn with_offset(repo: Arc<Repo>, file: File, offset: u64) -> Self {
        let mut position = offset;
        let parts = file
            .parts
            .into_iter()
            .map(|part| {
                let size = match &part {
                    FilePart::Chunk(c) => c.size,
                    FilePart::File(f) => f.size,
                };
                let entry = FilePartWithPosition {
                    start: position,
                    size,
                    part,
                };
                position += size;
                entry
            })
            .collect();
        Self { repo, parts }
    }
}

#[async_trait]
impl SearchableList for FileSearchList {
    type Key = u64;
    type Entry = ChunkFilePart;

    fn len(&self) -> usize {
        self.parts.len()
    }

    fn compare(&self, ix: usize, key: &u64) -> Ordering {
        let entry = &self.parts[ix];
        if *key < entry.start {
            Ordering::Less
        } else if *key >= entry.start + entry.size {
            Ordering::Greater
        } else {
            Ordering::Equal
        }
    }

    fn is_leaf(&self, ix: usize) -> bool {
        matches!(&self.parts[ix].part, FilePart::Chunk(_))
    }

    fn leaf_entry(&self, ix: usize) -> ChunkFilePart {
        match &self.parts[ix].part {
            FilePart::Chunk(c) => c.clone(),
            _ => panic!("leaf_entry called on a tree entry"),
        }
    }

    async fn tree_sublist(&self, ix: usize) -> Result<Self, RepoError> {
        match &self.parts[ix].part {
            FilePart::File(f) => {
                let file = self.repo.read_file(&f.file).await?;
                Ok(Self::with_offset(
                    Arc::clone(&self.repo),
                    file,
                    self.parts[ix].start,
                ))
            }
            _ => panic!("tree_sublist called on a leaf entry"),
        }
    }
}

// =============================================================================
// Convenience Functions
// =============================================================================

/// Search a branches list for a branch with the given name.
pub async fn search_branches(
    repo: Arc<Repo>,
    branches_id: &ObjectId,
    name: &str,
) -> Result<Option<Branch>, RepoError> {
    let branches = repo.read_branches(branches_id).await?;
    let list = BranchesSearchList::new(repo, branches);
    search_list(&list, &name.to_string()).await
}

/// Search a directory for an entry with the given name.
pub async fn search_directory(
    repo: Arc<Repo>,
    directory_id: &ObjectId,
    name: &str,
) -> Result<Option<DirectoryEntry>, RepoError> {
    let directory = repo.read_directory(directory_id).await?;
    let list = DirectorySearchList::new(repo, directory);
    search_list(&list, &name.to_string()).await
}

/// Search a file for the chunk containing the given byte position.
pub async fn search_file(
    repo: Arc<Repo>,
    file_id: &ObjectId,
    position: u64,
) -> Result<Option<ChunkFilePart>, RepoError> {
    let file = repo.read_file(file_id).await?;
    let list = FileSearchList::new(repo, file);
    search_list(&list, &position).await
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::{BranchListBuilder, DirectoryLeaf, DirectoryListBuilder, FileListBuilder};
    use crate::backend::MemoryBackend;
    use crate::caches::NoopCache;
    use crate::repository::{
        BranchesType, DirEntry, Directory, DirectoryType, FileEntry, FileType,
    };
    use crate::util::NotifyComplete;

    fn create_test_repo() -> Arc<Repo> {
        let backend = MemoryBackend::new();
        let cache = NoopCache;
        Arc::new(Repo::new(backend, cache))
    }

    fn make_complete() -> Arc<NotifyComplete> {
        let c = Arc::new(NotifyComplete::new());
        c.notify_complete();
        c
    }

    // =========================================================================
    // Branches tests
    // =========================================================================

    #[tokio::test]
    async fn test_search_branches_found() {
        let repo = create_test_repo();
        let branches = Branches {
            type_tag: BranchesType::Branches,
            branches: vec![
                BranchListEntry::Branch(Branch {
                    name: "alpha".to_string(),
                    commit: "commit-alpha".to_string(),
                }),
                BranchListEntry::Branch(Branch {
                    name: "beta".to_string(),
                    commit: "commit-beta".to_string(),
                }),
                BranchListEntry::Branch(Branch {
                    name: "gamma".to_string(),
                    commit: "commit-gamma".to_string(),
                }),
            ],
        };

        let list = BranchesSearchList::new(repo, branches);
        let found = search_list(&list, &"beta".to_string()).await.unwrap();
        assert!(found.is_some());
        let branch = found.unwrap();
        assert_eq!(branch.name, "beta");
        assert_eq!(branch.commit, "commit-beta");
    }

    #[tokio::test]
    async fn test_search_branches_not_found() {
        let repo = create_test_repo();
        let branches = Branches {
            type_tag: BranchesType::Branches,
            branches: vec![
                BranchListEntry::Branch(Branch {
                    name: "alpha".to_string(),
                    commit: "commit-alpha".to_string(),
                }),
                BranchListEntry::Branch(Branch {
                    name: "gamma".to_string(),
                    commit: "commit-gamma".to_string(),
                }),
            ],
        };

        let list = BranchesSearchList::new(repo, branches);
        let found = search_list(&list, &"delta".to_string()).await.unwrap();
        assert!(found.is_none());
    }

    #[tokio::test]
    async fn test_search_branches_empty() {
        let repo = create_test_repo();
        let branches = Branches {
            type_tag: BranchesType::Branches,
            branches: vec![],
        };

        let list = BranchesSearchList::new(repo, branches);
        let found = search_list(&list, &"main".to_string()).await.unwrap();
        assert!(found.is_none());
    }

    #[tokio::test]
    async fn test_search_branches_nested() {
        let repo = create_test_repo();

        // Build enough branches to force tree structure
        let mut builder = BranchListBuilder::new(Arc::clone(&repo));
        let count = 200;
        for i in 0..count {
            let branch = Branch {
                name: format!("branch-{i:04}"),
                commit: format!("commit-{i:04}"),
            };
            builder.add(branch, make_complete()).await.unwrap();
        }
        let result = builder.finish().await.unwrap();
        // Wait for all writes to complete
        result.complete.complete().await.unwrap();

        // Search for first, middle, and last
        let found = search_branches(Arc::clone(&repo), &result.hash, "branch-0000")
            .await
            .unwrap();
        assert_eq!(found.unwrap().name, "branch-0000");

        let found = search_branches(Arc::clone(&repo), &result.hash, "branch-0100")
            .await
            .unwrap();
        assert_eq!(found.unwrap().name, "branch-0100");

        let found = search_branches(Arc::clone(&repo), &result.hash, "branch-0199")
            .await
            .unwrap();
        assert_eq!(found.unwrap().name, "branch-0199");

        // Search for non-existent
        let found = search_branches(Arc::clone(&repo), &result.hash, "branch-9999")
            .await
            .unwrap();
        assert!(found.is_none());
    }

    // =========================================================================
    // Directory tests
    // =========================================================================

    #[tokio::test]
    async fn test_search_directory_file_found() {
        let repo = create_test_repo();
        let directory = Directory {
            type_tag: DirectoryType::Directory,
            entries: vec![
                DirectoryPart::File(FileEntry {
                    name: "readme.txt".to_string(),
                    size: 100,
                    executable: false,
                    file: "file123".to_string(),
                }),
                DirectoryPart::Directory(DirEntry {
                    name: "src".to_string(),
                    directory: "dir456".to_string(),
                }),
            ],
        };

        let list = DirectorySearchList::new(repo, directory);
        let found = search_list(&list, &"readme.txt".to_string()).await.unwrap();
        assert!(found.is_some());
        assert_eq!(found.unwrap().name(), "readme.txt");
    }

    #[tokio::test]
    async fn test_search_directory_dir_found() {
        let repo = create_test_repo();
        let directory = Directory {
            type_tag: DirectoryType::Directory,
            entries: vec![
                DirectoryPart::File(FileEntry {
                    name: "readme.txt".to_string(),
                    size: 100,
                    executable: false,
                    file: "file123".to_string(),
                }),
                DirectoryPart::Directory(DirEntry {
                    name: "src".to_string(),
                    directory: "dir456".to_string(),
                }),
            ],
        };

        let list = DirectorySearchList::new(repo, directory);
        let found = search_list(&list, &"src".to_string()).await.unwrap();
        assert!(found.is_some());
        assert_eq!(found.unwrap().name(), "src");
    }

    #[tokio::test]
    async fn test_search_directory_not_found() {
        let repo = create_test_repo();
        let directory = Directory {
            type_tag: DirectoryType::Directory,
            entries: vec![DirectoryPart::File(FileEntry {
                name: "readme.txt".to_string(),
                size: 100,
                executable: false,
                file: "file123".to_string(),
            })],
        };

        let list = DirectorySearchList::new(repo, directory);
        let found = search_list(&list, &"missing.txt".to_string())
            .await
            .unwrap();
        assert!(found.is_none());
    }

    #[tokio::test]
    async fn test_search_directory_nested() {
        let repo = create_test_repo();

        // Build enough entries to force tree structure
        let mut builder = DirectoryListBuilder::new(Arc::clone(&repo));
        let count = 300;
        for i in 0..count {
            let file = FileEntry {
                name: format!("file-{i:04}.txt"),
                size: 100,
                executable: false,
                file: format!("fileid-{i:04}"),
            };
            builder
                .add(DirectoryLeaf::File(file), make_complete())
                .await
                .unwrap();
        }
        let result = builder.finish().await.unwrap();
        result.complete.complete().await.unwrap();

        let found = search_directory(Arc::clone(&repo), &result.hash, "file-0000.txt")
            .await
            .unwrap();
        assert_eq!(found.unwrap().name(), "file-0000.txt");

        let found = search_directory(Arc::clone(&repo), &result.hash, "file-0150.txt")
            .await
            .unwrap();
        assert_eq!(found.unwrap().name(), "file-0150.txt");

        let found = search_directory(Arc::clone(&repo), &result.hash, "file-9999.txt")
            .await
            .unwrap();
        assert!(found.is_none());
    }

    // =========================================================================
    // File tests
    // =========================================================================

    #[tokio::test]
    async fn test_search_file_found() {
        let repo = create_test_repo();
        let file = File {
            type_tag: FileType::File,
            parts: vec![
                FilePart::Chunk(ChunkFilePart {
                    size: 100,
                    content: "chunk1".to_string(),
                }),
                FilePart::Chunk(ChunkFilePart {
                    size: 200,
                    content: "chunk2".to_string(),
                }),
                FilePart::Chunk(ChunkFilePart {
                    size: 150,
                    content: "chunk3".to_string(),
                }),
            ],
        };

        let list = FileSearchList::new(repo, file);

        // Position 0 -> first chunk
        let found = search_list(&list, &0u64).await.unwrap();
        assert_eq!(found.unwrap().content, "chunk1");

        // Position 50 -> first chunk
        let found = search_list(&list, &50u64).await.unwrap();
        assert_eq!(found.unwrap().content, "chunk1");

        // Position 99 -> first chunk (last byte)
        let found = search_list(&list, &99u64).await.unwrap();
        assert_eq!(found.unwrap().content, "chunk1");

        // Position 100 -> second chunk (starts at 100)
        let found = search_list(&list, &100u64).await.unwrap();
        assert_eq!(found.unwrap().content, "chunk2");

        // Position 299 -> second chunk (last byte)
        let found = search_list(&list, &299u64).await.unwrap();
        assert_eq!(found.unwrap().content, "chunk2");

        // Position 300 -> third chunk (starts at 300)
        let found = search_list(&list, &300u64).await.unwrap();
        assert_eq!(found.unwrap().content, "chunk3");

        // Position 449 -> third chunk (last byte)
        let found = search_list(&list, &449u64).await.unwrap();
        assert_eq!(found.unwrap().content, "chunk3");
    }

    #[tokio::test]
    async fn test_search_file_not_found() {
        let repo = create_test_repo();
        let file = File {
            type_tag: FileType::File,
            parts: vec![FilePart::Chunk(ChunkFilePart {
                size: 100,
                content: "chunk1".to_string(),
            })],
        };

        let list = FileSearchList::new(repo, file);

        // Position beyond end
        let found = search_list(&list, &100u64).await.unwrap();
        assert!(found.is_none());
    }

    #[tokio::test]
    async fn test_search_file_empty() {
        let repo = create_test_repo();
        let file = File {
            type_tag: FileType::File,
            parts: vec![],
        };

        let list = FileSearchList::new(repo, file);
        let found = search_list(&list, &0u64).await.unwrap();
        assert!(found.is_none());
    }

    #[tokio::test]
    async fn test_search_file_nested() {
        let repo = create_test_repo();

        // Build enough chunks to force tree structure
        let mut builder = FileListBuilder::new(Arc::clone(&repo));
        let count = 100;
        for i in 0..count {
            let chunk = ChunkFilePart {
                size: 1024,
                content: format!("chunk-{i:04}"),
            };
            builder.add(chunk, make_complete()).await.unwrap();
        }
        let result = builder.finish().await.unwrap();
        result.complete.complete().await.unwrap();

        // Position 0 -> first chunk
        let found = search_file(Arc::clone(&repo), &result.hash, 0)
            .await
            .unwrap();
        assert_eq!(found.unwrap().content, "chunk-0000");

        // Position in the middle
        let found = search_file(Arc::clone(&repo), &result.hash, 50 * 1024)
            .await
            .unwrap();
        assert_eq!(found.unwrap().content, "chunk-0050");

        // Last chunk
        let found = search_file(Arc::clone(&repo), &result.hash, 99 * 1024)
            .await
            .unwrap();
        assert_eq!(found.unwrap().content, "chunk-0099");

        // Beyond end
        let found = search_file(Arc::clone(&repo), &result.hash, 100 * 1024)
            .await
            .unwrap();
        assert!(found.is_none());
    }
}
