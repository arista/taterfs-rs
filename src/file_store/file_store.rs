// Interfaces for a FileStore - a representation of a filesystem that will likely serve as

// Thanks ChatGPT

use async_trait::async_trait;
use bytes::Bytes;
use std::path::{Path};

//----------------------------------------
// FileStoreService - interface to a FileStore

#[async_trait]
pub trait FileStoreService: Send + Sync {
    async fn list_directory(&self, root: &Path) -> anyhow::Result<Box<dyn DirectoryLister>>;
    async fn get_file_chunks(
        &self,
        path: std::path::PathBuf,
    ) -> anyhow::Result<Box<dyn FileChunksIterator>>;
}

//----------------------------------------
// DirectoryLister - lists the contents of a directory, sorted by name.  Directory entries are yielded one at a time by calling "next()", until Ok(None) is reached.

#[async_trait]
pub trait DirectoryLister: Send + Sync {
    // Pull the next entry, or `Ok(None)` at end-of-directory.
    async fn next(&mut self) -> anyhow::Result<Option<DirEntry>>;
}

pub enum DirEntry {
    File(Box<dyn FileEntry>),
    Directory(Box<dyn DirectoryEntry>),
}

pub trait FileEntry {
    // Basename
    fn name(&self) -> &str;
    // Absolute path
    fn abs_path(&self) -> &Path;
    // Path relative to the listing root (uses OS separators)
    fn rel_path(&self) -> &Path;
    fn size(&self) -> u64;
    fn executable(&self) -> bool;
}

pub trait DirectoryEntry {
    // Basename
    fn name(&self) -> &str;
    fn abs_path(&self) -> &Path;
    // Path relative to the listing root (uses OS separators)
    fn rel_path(&self) -> &Path;
    // DirectoryLister that can be used to recursively list this directory - this method transfers ownership out of the DirectoryEntry
    fn lister(&mut self) -> Box<dyn DirectoryLister>;
}

//----------------------------------------
// FileChunksIterator - iterates through a file, yielding chunks in CHUNK_SIZES order

/// A handle for one file chunk.
#[async_trait]
pub trait FileChunkHandle: Send + Sync {
    /// Size of this chunk in bytes.
    fn size(&self) -> usize;

    /// Offset of this chunk in bytes.
    fn offset(&self) -> usize;

    /// Fetch the chunk's bytes.
    async fn get_chunk(&self) -> anyhow::Result<Bytes>;
}

/// An async iterator over file chunks. Call `next().await` until it returns `Ok(None)`.
#[async_trait]
pub trait FileChunksIterator: Send {
    async fn next(&mut self) -> anyhow::Result<Option<Box<dyn FileChunkHandle>>>;
}
