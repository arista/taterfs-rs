//! Generic list modifier for applying changes to tree-structured repository objects.
//!
//! This module provides a generic mechanism for modifying tree-structured lists
//! by applying a sorted sequence of modifications (add, update, delete).
//! It uses a "zipper" algorithm that merges the original list with the modifications.
//!
//! It is used for:
//! - `Branches` - modifying branch entries
//! - `Directory` - modifying directory entries (files and subdirectories)
//! - `File` - modifying file chunks

use std::cmp::Ordering;
use std::sync::Arc;

use async_trait::async_trait;

use crate::app::list_builder::{
    BranchListBuilder, DirectoryLeaf, DirectoryListBuilder, FileListBuilder, ListBuilder,
    ListBuilderConfig,
};
use crate::repo::{BranchList, DirectoryEntry, DirectoryEntryList, FileChunkList, Repo, RepoError};
use crate::repository::{Branch, ChunkFilePart, ObjectId};
use crate::util::{Complete, NoopComplete, WithComplete};

// =============================================================================
// Error Types
// =============================================================================

/// Error type for list modification operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ListModificationError {
    /// Attempted to add a modification after iteration has started.
    AddAfterIteration,
}

impl std::fmt::Display for ListModificationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ListModificationError::AddAfterIteration => {
                write!(f, "cannot add modifications after iteration has started")
            }
        }
    }
}

impl std::error::Error for ListModificationError {}

// =============================================================================
// ListElem
// =============================================================================

/// An element from a list, with its key for sorting/comparison.
pub struct ListElem<K, E> {
    /// The key used for sorting and matching modifications.
    pub key: K,
    /// The entry data.
    pub entry: E,
}

// =============================================================================
// ListModification
// =============================================================================

/// A single modification to apply to a list.
pub struct ListModification<K, E> {
    /// The key identifying where to apply the modification.
    pub key: K,
    /// The new entry, or None to delete the entry at this key.
    pub entry: Option<E>,
}

// =============================================================================
// ListElems Trait
// =============================================================================

/// Async iterator over list elements.
///
/// Implementations provide sorted elements from the original list.
#[async_trait]
pub trait ListElems<K, E>: Send {
    /// Get the next element from the list.
    ///
    /// Returns `None` when all elements have been yielded.
    /// Elements must be returned in sorted order by key.
    async fn next(&mut self) -> Result<Option<ListElem<K, E>>, RepoError>;
}

// =============================================================================
// ListModifications Trait
// =============================================================================

/// A collection of modifications to apply to a list.
///
/// Modifications must be yielded in sorted order by key.
/// Adding new modifications after iteration has started is an error.
pub trait ListModifications<K: Ord, E>: Send {
    /// Add a modification to the collection.
    ///
    /// Returns an error if called after `next()` has been called.
    fn add(&mut self, key: K, entry: Option<E>) -> Result<(), ListModificationError>;

    /// Get the next modification.
    ///
    /// Returns `None` when all modifications have been yielded.
    /// Modifications are returned in sorted order by key.
    fn next(&mut self) -> Option<ListModification<K, E>>;
}

// =============================================================================
// ListModifications Implementation
// =============================================================================

/// A vector-based implementation of ListModifications.
pub struct ListModificationsVec<K: Ord, E> {
    modifications: Option<Vec<ListModification<K, E>>>,
    drain: Option<std::vec::IntoIter<ListModification<K, E>>>,
}

impl<K: Ord, E> ListModificationsVec<K, E> {
    /// Create a new empty modifications collection.
    pub fn new() -> Self {
        Self {
            modifications: Some(Vec::new()),
            drain: None,
        }
    }
}

impl<K: Ord, E> Default for ListModificationsVec<K, E> {
    fn default() -> Self {
        Self::new()
    }
}

impl<K: Ord + Send, E: Send> ListModifications<K, E> for ListModificationsVec<K, E> {
    fn add(&mut self, key: K, entry: Option<E>) -> Result<(), ListModificationError> {
        let mods = self
            .modifications
            .as_mut()
            .ok_or(ListModificationError::AddAfterIteration)?;
        mods.push(ListModification { key, entry });
        Ok(())
    }

    fn next(&mut self) -> Option<ListModification<K, E>> {
        // On first call to next(), sort and create the drain iterator
        if self.drain.is_none()
            && let Some(mut mods) = self.modifications.take()
        {
            mods.sort_by(|a, b| a.key.cmp(&b.key));
            self.drain = Some(mods.into_iter());
        }

        self.drain.as_mut().and_then(|d| d.next())
    }
}

// =============================================================================
// modify_list
// =============================================================================

/// Apply modifications to a list using a zipper algorithm.
///
/// This function merges the original list elements with the modifications,
/// outputting the result through the provided builder.
///
/// The algorithm walks through both lists in sorted order:
/// - Elements without matching modifications are passed through unchanged
/// - Elements with matching modifications are replaced (or deleted if entry is None)
/// - New modifications (no matching element) are inserted
///
/// Returns the resulting object ID and completion handle.
pub async fn modify_list<K, E, L, C, M>(
    mut elems: L,
    mut modifications: M,
    mut builder: ListBuilder<C>,
    elem_to_leaf: impl Fn(E) -> C::Leaf,
    elem_complete: impl Fn(&E) -> Arc<dyn Complete>,
) -> Result<WithComplete<ObjectId>, RepoError>
where
    K: Ord,
    L: ListElems<K, E>,
    M: ListModifications<K, E>,
    C: ListBuilderConfig,
{
    let mut current_elem = elems.next().await?;
    let mut current_mod = modifications.next();

    loop {
        match (&current_elem, &current_mod) {
            // Both exhausted - we're done
            (None, None) => break,

            // Only elements left - pass through remaining elements
            (Some(_), None) => {
                let elem = current_elem.take().unwrap();
                let complete = elem_complete(&elem.entry);
                builder.add(elem_to_leaf(elem.entry), complete).await?;
                current_elem = elems.next().await?;
            }

            // Only modifications left - add new entries (skip deletes)
            (None, Some(_)) => {
                let m = current_mod.take().unwrap();
                if let Some(entry) = m.entry {
                    let complete: Arc<dyn Complete> = Arc::new(NoopComplete);
                    builder.add(elem_to_leaf(entry), complete).await?;
                }
                current_mod = modifications.next();
            }

            // Both present - compare keys
            (Some(elem), Some(m)) => {
                match elem.key.cmp(&m.key) {
                    Ordering::Less => {
                        // Element comes before modification - pass through
                        let elem = current_elem.take().unwrap();
                        let complete = elem_complete(&elem.entry);
                        builder.add(elem_to_leaf(elem.entry), complete).await?;
                        current_elem = elems.next().await?;
                    }
                    Ordering::Greater => {
                        // Modification comes before element - insert new entry
                        let m = current_mod.take().unwrap();
                        if let Some(entry) = m.entry {
                            let complete: Arc<dyn Complete> = Arc::new(NoopComplete);
                            builder.add(elem_to_leaf(entry), complete).await?;
                        }
                        current_mod = modifications.next();
                    }
                    Ordering::Equal => {
                        // Keys match - replace or delete
                        let _elem = current_elem.take().unwrap();
                        let m = current_mod.take().unwrap();
                        if let Some(entry) = m.entry {
                            // Replace with new entry
                            let complete: Arc<dyn Complete> = Arc::new(NoopComplete);
                            builder.add(elem_to_leaf(entry), complete).await?;
                        }
                        // If m.entry is None, we skip (delete) the element
                        current_elem = elems.next().await?;
                        current_mod = modifications.next();
                    }
                }
            }
        }
    }

    let result = builder.finish().await?;
    Ok(WithComplete {
        result: result.hash,
        complete: result.complete,
    })
}

// =============================================================================
// BranchListElems
// =============================================================================

/// ListElems implementation for branches.
pub struct BranchListElems {
    inner: BranchList,
}

impl BranchListElems {
    /// Create a new BranchListElems from a BranchList.
    pub fn new(branch_list: BranchList) -> Self {
        Self { inner: branch_list }
    }
}

#[async_trait]
impl ListElems<String, Branch> for BranchListElems {
    async fn next(&mut self) -> Result<Option<ListElem<String, Branch>>, RepoError> {
        match self.inner.next().await? {
            Some(branch) => Ok(Some(ListElem {
                key: branch.name.clone(),
                entry: branch,
            })),
            None => Ok(None),
        }
    }
}

/// Type alias for branch modifications.
pub type BranchListModifications = ListModificationsVec<String, Branch>;

// =============================================================================
// DirectoryEntryListElems
// =============================================================================

/// ListElems implementation for directory entries.
pub struct DirectoryEntryListElems {
    inner: DirectoryEntryList,
}

impl DirectoryEntryListElems {
    /// Create a new DirectoryEntryListElems from a DirectoryEntryList.
    pub fn new(entry_list: DirectoryEntryList) -> Self {
        Self { inner: entry_list }
    }
}

#[async_trait]
impl ListElems<String, DirectoryLeaf> for DirectoryEntryListElems {
    async fn next(&mut self) -> Result<Option<ListElem<String, DirectoryLeaf>>, RepoError> {
        match self.inner.next().await? {
            Some(entry) => {
                let key = entry.name().to_string();
                let leaf = match entry {
                    DirectoryEntry::File(f) => DirectoryLeaf::File(f),
                    DirectoryEntry::Directory(d) => DirectoryLeaf::Dir(d),
                };
                Ok(Some(ListElem { key, entry: leaf }))
            }
            None => Ok(None),
        }
    }
}

/// Type alias for directory entry modifications.
pub type DirectoryEntryListModifications = ListModificationsVec<String, DirectoryLeaf>;

// =============================================================================
// FileChunkListElems
// =============================================================================

/// ListElems implementation for file chunks.
///
/// The key is the byte position of the chunk within the file,
/// calculated as a running total of chunk sizes.
pub struct FileChunkListElems {
    inner: FileChunkList,
    position: u64,
}

impl FileChunkListElems {
    /// Create a new FileChunkListElems from a FileChunkList.
    pub fn new(chunk_list: FileChunkList) -> Self {
        Self {
            inner: chunk_list,
            position: 0,
        }
    }
}

#[async_trait]
impl ListElems<u64, ChunkFilePart> for FileChunkListElems {
    async fn next(&mut self) -> Result<Option<ListElem<u64, ChunkFilePart>>, RepoError> {
        match self.inner.next().await? {
            Some(chunk) => {
                let key = self.position;
                self.position += chunk.size;
                Ok(Some(ListElem { key, entry: chunk }))
            }
            None => Ok(None),
        }
    }
}

/// Type alias for file chunk modifications.
pub type FileChunkListModifications = ListModificationsVec<u64, ChunkFilePart>;

// =============================================================================
// Convenience Functions
// =============================================================================

/// Modify a branches list.
///
/// Applies the given modifications to the branches list and returns
/// the new branches object ID.
pub async fn modify_branches(
    repo: Arc<Repo>,
    branches_id: &ObjectId,
    modifications: BranchListModifications,
) -> Result<WithComplete<ObjectId>, RepoError> {
    let branch_list = repo.list_branches(branches_id).await?;
    let elems = BranchListElems::new(branch_list);
    let builder = BranchListBuilder::new(repo);

    modify_list(
        elems,
        modifications,
        builder,
        |branch| branch,
        |_| Arc::new(NoopComplete) as Arc<dyn Complete>,
    )
    .await
}

/// Modify a directory.
///
/// Applies the given modifications to the directory and returns
/// the new directory object ID.
pub async fn modify_directory(
    repo: Arc<Repo>,
    directory_id: &ObjectId,
    modifications: DirectoryEntryListModifications,
) -> Result<WithComplete<ObjectId>, RepoError> {
    let entry_list = repo.list_directory_entries(directory_id).await?;
    let elems = DirectoryEntryListElems::new(entry_list);
    let builder = DirectoryListBuilder::new(repo);

    modify_list(
        elems,
        modifications,
        builder,
        |leaf| leaf,
        |_| Arc::new(NoopComplete) as Arc<dyn Complete>,
    )
    .await
}

/// Modify a file.
///
/// Applies the given modifications to the file chunks and returns
/// the new file object ID.
pub async fn modify_file(
    repo: Arc<Repo>,
    file_id: &ObjectId,
    modifications: FileChunkListModifications,
) -> Result<WithComplete<ObjectId>, RepoError> {
    let chunk_list = repo.list_file_chunks(file_id).await?;
    let elems = FileChunkListElems::new(chunk_list);
    let builder = FileListBuilder::new(repo);

    modify_list(
        elems,
        modifications,
        builder,
        |chunk| chunk,
        |_| Arc::new(NoopComplete) as Arc<dyn Complete>,
    )
    .await
}
