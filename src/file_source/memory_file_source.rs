use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;

use crate::file_source::directory_list::{DirectoryList, DirectoryListing};
use crate::file_source::error::{FileSourceError, Result};
use crate::file_source::file_chunks::{FileChunking, FileChunks};
use crate::file_source::file_source::FileSource;
use crate::file_source::types::{
    next_chunk_size, DirEntry, DirectoryListEntry, FileChunk, FileEntry, GetChildEntryFn,
    GetChunksFn, ListDirectoryFn,
};

/// An entry in the in-memory filesystem.
#[derive(Debug, Clone)]
pub enum MemoryFsEntry {
    /// A directory containing other entries.
    Directory(BTreeMap<String, MemoryFsEntry>),
    /// A file with explicit contents.
    File {
        contents: Vec<u8>,
        executable: bool,
    },
    /// A file filled with repeated content to a given size.
    RepeatedFile {
        pattern: Vec<u8>,
        size: u64,
        executable: bool,
    },
}

impl MemoryFsEntry {
    /// Create an empty directory.
    pub fn dir() -> Self {
        MemoryFsEntry::Directory(BTreeMap::new())
    }

    /// Create a file with the given contents.
    pub fn file(contents: impl Into<Vec<u8>>) -> Self {
        MemoryFsEntry::File {
            contents: contents.into(),
            executable: false,
        }
    }

    /// Create an executable file with the given contents.
    pub fn executable(contents: impl Into<Vec<u8>>) -> Self {
        MemoryFsEntry::File {
            contents: contents.into(),
            executable: true,
        }
    }

    /// Create a file filled with repeated content.
    pub fn repeated(pattern: impl Into<Vec<u8>>, size: u64) -> Self {
        MemoryFsEntry::RepeatedFile {
            pattern: pattern.into(),
            size,
            executable: false,
        }
    }

    /// Create an executable file filled with repeated content.
    pub fn repeated_executable(pattern: impl Into<Vec<u8>>, size: u64) -> Self {
        MemoryFsEntry::RepeatedFile {
            pattern: pattern.into(),
            size,
            executable: true,
        }
    }

    /// Get the size of this entry if it's a file.
    fn file_size(&self) -> Option<u64> {
        match self {
            MemoryFsEntry::Directory(_) => None,
            MemoryFsEntry::File { contents, .. } => Some(contents.len() as u64),
            MemoryFsEntry::RepeatedFile { size, .. } => Some(*size),
        }
    }

    /// Check if this entry is executable (for files).
    fn is_executable(&self) -> bool {
        match self {
            MemoryFsEntry::Directory(_) => false,
            MemoryFsEntry::File { executable, .. } => *executable,
            MemoryFsEntry::RepeatedFile { executable, .. } => *executable,
        }
    }

    /// Read bytes from this file entry.
    fn read_bytes(&self, offset: u64, len: u64) -> Option<Vec<u8>> {
        match self {
            MemoryFsEntry::Directory(_) => None,
            MemoryFsEntry::File { contents, .. } => {
                let start = offset as usize;
                let end = (offset + len) as usize;
                if end <= contents.len() {
                    Some(contents[start..end].to_vec())
                } else if start < contents.len() {
                    Some(contents[start..].to_vec())
                } else {
                    Some(Vec::new())
                }
            }
            MemoryFsEntry::RepeatedFile { pattern, size, .. } => {
                if offset >= *size {
                    return Some(Vec::new());
                }
                let actual_len = len.min(*size - offset) as usize;
                let mut result = Vec::with_capacity(actual_len);
                let pattern_len = pattern.len();
                if pattern_len == 0 {
                    return Some(vec![0u8; actual_len]);
                }
                let mut pos = offset as usize;
                while result.len() < actual_len {
                    let pattern_offset = pos % pattern_len;
                    let remaining = actual_len - result.len();
                    let chunk_len = remaining.min(pattern_len - pattern_offset);
                    result.extend_from_slice(&pattern[pattern_offset..pattern_offset + chunk_len]);
                    pos += chunk_len;
                }
                Some(result)
            }
        }
    }
}

/// Builder for constructing a MemoryFileSource.
pub struct MemoryFileSourceBuilder {
    root: BTreeMap<String, MemoryFsEntry>,
}

impl Default for MemoryFileSourceBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl MemoryFileSourceBuilder {
    /// Create a new builder.
    pub fn new() -> Self {
        Self {
            root: BTreeMap::new(),
        }
    }

    /// Add an entry at the given path.
    ///
    /// Path components are separated by '/'. Parent directories are created automatically.
    pub fn add(mut self, path: &str, entry: MemoryFsEntry) -> Self {
        let parts: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
        if parts.is_empty() {
            return self;
        }

        Self::add_at_path(&mut self.root, &parts, entry);
        self
    }

    fn add_at_path(
        current: &mut BTreeMap<String, MemoryFsEntry>,
        parts: &[&str],
        entry: MemoryFsEntry,
    ) {
        if parts.len() == 1 {
            current.insert(parts[0].to_string(), entry);
            return;
        }

        let dir_name = parts[0].to_string();
        let child = current
            .entry(dir_name)
            .or_insert_with(MemoryFsEntry::dir);

        if let MemoryFsEntry::Directory(children) = child {
            Self::add_at_path(children, &parts[1..], entry);
        }
    }

    /// Build the MemoryFileSource.
    pub fn build(self) -> MemoryFileSource {
        MemoryFileSource {
            inner: Arc::new(MemoryFileSourceInner {
                root: MemoryFsEntry::Directory(self.root),
            }),
        }
    }
}

/// Inner state for MemoryFileSource, wrapped in Arc for sharing with closures.
struct MemoryFileSourceInner {
    root: MemoryFsEntry,
}

/// An in-memory implementation of FileSource for testing.
#[derive(Clone)]
pub struct MemoryFileSource {
    inner: Arc<MemoryFileSourceInner>,
}

impl MemoryFileSource {
    /// Create a new builder for constructing a MemoryFileSource.
    pub fn builder() -> MemoryFileSourceBuilder {
        MemoryFileSourceBuilder::new()
    }

    /// Create an empty MemoryFileSource.
    pub fn empty() -> Self {
        Self {
            inner: Arc::new(MemoryFileSourceInner {
                root: MemoryFsEntry::Directory(BTreeMap::new()),
            }),
        }
    }

    /// Navigate to an entry at the given path.
    fn get_entry_at<'a>(inner: &'a MemoryFileSourceInner, path: &Path) -> Option<&'a MemoryFsEntry> {
        let mut current = &inner.root;

        for component in path.components() {
            let name = component.as_os_str().to_string_lossy();
            if name == "/" || name.is_empty() {
                continue;
            }

            match current {
                MemoryFsEntry::Directory(children) => {
                    current = children.get(name.as_ref())?;
                }
                _ => return None,
            }
        }

        Some(current)
    }

    /// Normalize a path by stripping leading "/" and converting to PathBuf.
    fn normalize_path(path: &Path) -> PathBuf {
        let path_str = path.to_string_lossy();
        let stripped = path_str.trim_start_matches('/');
        if stripped.is_empty() {
            PathBuf::new()
        } else {
            PathBuf::from(stripped)
        }
    }

    /// Create a closure that lists a directory.
    fn make_list_fn(inner: Arc<MemoryFileSourceInner>, path: PathBuf) -> ListDirectoryFn {
        Arc::new(move || {
            let inner = Arc::clone(&inner);
            let path = path.clone();
            Box::pin(async move { Self::list_directory_internal(inner, &path).await })
        })
    }

    /// Create a closure that gets a child entry by name.
    fn make_get_child_fn(inner: Arc<MemoryFileSourceInner>, path: PathBuf) -> GetChildEntryFn {
        Arc::new(move |name: String| {
            let inner = Arc::clone(&inner);
            let child_path = if path.as_os_str().is_empty() {
                PathBuf::from(&name)
            } else {
                path.join(&name)
            };
            Box::pin(async move { Self::get_entry_internal(inner, &child_path).await })
        })
    }

    /// Create a closure that gets file chunks.
    fn make_get_chunks_fn(inner: Arc<MemoryFileSourceInner>, path: PathBuf) -> GetChunksFn {
        Arc::new(move || {
            let inner = Arc::clone(&inner);
            let path = path.clone();
            Box::pin(async move { Self::get_file_chunks_internal(inner, &path).await })
        })
    }

    /// Internal implementation of list_directory.
    fn list_directory_internal(
        inner: Arc<MemoryFileSourceInner>,
        path: &Path,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<DirectoryList>> + Send>> {
        let path = path.to_path_buf();
        Box::pin(async move {
            let entry = Self::get_entry_at(&inner, &path).ok_or(FileSourceError::NotFound)?;

            let children = match entry {
                MemoryFsEntry::Directory(children) => children,
                _ => return Err(FileSourceError::NotFound),
            };

            let parent_path = Self::normalize_path(&path);

            // Collect entries sorted by name (BTreeMap already sorted)
            let entries: Vec<(String, MemoryFsEntry)> = children
                .iter()
                .map(|(name, entry)| (name.clone(), entry.clone()))
                .collect();

            Ok(DirectoryList::new(MemoryDirectoryList {
                inner: Arc::clone(&inner),
                parent_path,
                entries,
                index: 0,
            }))
        })
    }

    /// Internal implementation of get_entry.
    fn get_entry_internal(
        inner: Arc<MemoryFileSourceInner>,
        path: &Path,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<Option<DirectoryListEntry>>> + Send>> {
        let path = path.to_path_buf();
        Box::pin(async move {
            let entry = match Self::get_entry_at(&inner, &path) {
                Some(e) => e,
                None => return Ok(None),
            };

            let name = path
                .file_name()
                .map(|n| n.to_string_lossy().to_string())
                .unwrap_or_default();

            let entry_path = Self::normalize_path(&path);

            let result = match entry {
                &MemoryFsEntry::Directory(_) => {
                    let list_fn = Self::make_list_fn(Arc::clone(&inner), entry_path.clone());
                    let get_child_fn = Self::make_get_child_fn(Arc::clone(&inner), entry_path.clone());
                    DirectoryListEntry::Directory(DirEntry::new(name, entry_path, list_fn, get_child_fn))
                }
                &MemoryFsEntry::File { ref contents, executable } => {
                    let get_chunks_fn = Self::make_get_chunks_fn(Arc::clone(&inner), entry_path.clone());
                    DirectoryListEntry::File(FileEntry::new(
                        name,
                        entry_path,
                        contents.len() as u64,
                        executable,
                        get_chunks_fn,
                    ))
                }
                &MemoryFsEntry::RepeatedFile { size, executable, .. } => {
                    let get_chunks_fn = Self::make_get_chunks_fn(Arc::clone(&inner), entry_path.clone());
                    DirectoryListEntry::File(FileEntry::new(
                        name,
                        entry_path,
                        size,
                        executable,
                        get_chunks_fn,
                    ))
                }
            };

            Ok(Some(result))
        })
    }

    /// Internal implementation of get_file_chunks.
    fn get_file_chunks_internal(
        inner: Arc<MemoryFileSourceInner>,
        path: &Path,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<FileChunks>> + Send>> {
        let path = path.to_path_buf();
        Box::pin(async move {
            let entry = Self::get_entry_at(&inner, &path).ok_or(FileSourceError::NotFound)?;

            let size = entry.file_size().ok_or(FileSourceError::NotFound)?;

            Ok(FileChunks::new(MemoryFileChunks {
                entry: entry.clone(),
                size,
                offset: 0,
            }))
        })
    }
}

impl FileSource for MemoryFileSource {
    async fn list_directory(&self, path: &Path) -> Result<DirectoryList> {
        Self::list_directory_internal(Arc::clone(&self.inner), path).await
    }

    async fn get_file_chunks(&self, path: &Path) -> Result<FileChunks> {
        Self::get_file_chunks_internal(Arc::clone(&self.inner), path).await
    }

    async fn get_entry(&self, path: &Path) -> Result<Option<DirectoryListEntry>> {
        Self::get_entry_internal(Arc::clone(&self.inner), path).await
    }
}

/// Directory listing iterator for MemoryFileSource.
struct MemoryDirectoryList {
    inner: Arc<MemoryFileSourceInner>,
    parent_path: PathBuf,
    entries: Vec<(String, MemoryFsEntry)>,
    index: usize,
}

impl DirectoryListing for MemoryDirectoryList {
    async fn next(&mut self) -> Result<Option<DirectoryListEntry>> {
        if self.index >= self.entries.len() {
            return Ok(None);
        }

        let (name, mem_entry) = &self.entries[self.index];
        self.index += 1;

        let entry_path = if self.parent_path.as_os_str().is_empty() {
            PathBuf::from(name)
        } else {
            self.parent_path.join(name)
        };

        let entry = match mem_entry {
            MemoryFsEntry::Directory(_) => {
                let list_fn = MemoryFileSource::make_list_fn(Arc::clone(&self.inner), entry_path.clone());
                let get_child_fn =
                    MemoryFileSource::make_get_child_fn(Arc::clone(&self.inner), entry_path.clone());
                DirectoryListEntry::Directory(DirEntry::new(
                    name.clone(),
                    entry_path,
                    list_fn,
                    get_child_fn,
                ))
            }
            MemoryFsEntry::File { .. } | MemoryFsEntry::RepeatedFile { .. } => {
                let get_chunks_fn =
                    MemoryFileSource::make_get_chunks_fn(Arc::clone(&self.inner), entry_path.clone());
                DirectoryListEntry::File(FileEntry::new(
                    name.clone(),
                    entry_path,
                    mem_entry.file_size().unwrap_or(0),
                    mem_entry.is_executable(),
                    get_chunks_fn,
                ))
            }
        };

        Ok(Some(entry))
    }
}

/// File chunks iterator for MemoryFileSource.
struct MemoryFileChunks {
    entry: MemoryFsEntry,
    size: u64,
    offset: u64,
}

impl FileChunking for MemoryFileChunks {
    async fn next(&mut self) -> Result<Option<FileChunk>> {
        if self.offset >= self.size {
            return Ok(None);
        }

        let remaining = self.size - self.offset;
        let chunk_size = next_chunk_size(remaining);
        let data = self
            .entry
            .read_bytes(self.offset, chunk_size)
            .unwrap_or_default();

        let chunk = FileChunk::new(self.offset, data);
        self.offset += chunk_size;

        Ok(Some(chunk))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_empty_source() {
        let source = MemoryFileSource::empty();
        let mut list = source.list_directory(Path::new("/")).await.unwrap();
        assert!(list.next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_list_directory() {
        let source = MemoryFileSource::builder()
            .add("file1.txt", MemoryFsEntry::file("hello"))
            .add("file2.txt", MemoryFsEntry::file("world"))
            .add("subdir", MemoryFsEntry::dir())
            .build();

        let mut list = source.list_directory(Path::new("/")).await.unwrap();

        // Entries should be in lexical order
        let entry1 = list.next().await.unwrap().unwrap();
        assert_eq!(entry1.name(), "file1.txt");
        assert!(matches!(entry1, DirectoryListEntry::File(_)));

        let entry2 = list.next().await.unwrap().unwrap();
        assert_eq!(entry2.name(), "file2.txt");

        let entry3 = list.next().await.unwrap().unwrap();
        assert_eq!(entry3.name(), "subdir");
        assert!(matches!(entry3, DirectoryListEntry::Directory(_)));

        assert!(list.next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_nested_directory() {
        let source = MemoryFileSource::builder()
            .add("a/b/c.txt", MemoryFsEntry::file("nested"))
            .build();

        // List root
        let mut list = source.list_directory(Path::new("/")).await.unwrap();
        let entry = list.next().await.unwrap().unwrap();
        assert_eq!(entry.name(), "a");
        assert!(matches!(entry, DirectoryListEntry::Directory(_)));

        // List nested directory
        let mut list = source.list_directory(Path::new("/a/b")).await.unwrap();
        let entry = list.next().await.unwrap().unwrap();
        assert_eq!(entry.name(), "c.txt");
        assert_eq!(entry.path(), &PathBuf::from("a/b/c.txt"));
    }

    #[tokio::test]
    async fn test_file_chunks_small_file() {
        let source = MemoryFileSource::builder()
            .add("small.txt", MemoryFsEntry::file("hello world"))
            .build();

        let mut chunks = source
            .get_file_chunks(Path::new("/small.txt"))
            .await
            .unwrap();

        let chunk = chunks.next().await.unwrap().unwrap();
        assert_eq!(chunk.offset(), 0);
        assert_eq!(chunk.data(), b"hello world");

        assert!(chunks.next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_file_chunks_large_file() {
        // Create a file larger than 4MB to test chunking
        let size = 5 * 1024 * 1024; // 5MB
        let source = MemoryFileSource::builder()
            .add("large.bin", MemoryFsEntry::repeated(b"x".to_vec(), size))
            .build();

        let mut chunks = source
            .get_file_chunks(Path::new("/large.bin"))
            .await
            .unwrap();

        // First chunk should be 4MB
        let chunk1 = chunks.next().await.unwrap().unwrap();
        assert_eq!(chunk1.offset(), 0);
        assert_eq!(chunk1.size(), 4 * 1024 * 1024);

        // Second chunk should be 1MB
        let chunk2 = chunks.next().await.unwrap().unwrap();
        assert_eq!(chunk2.offset(), 4 * 1024 * 1024);
        assert_eq!(chunk2.size(), 1024 * 1024);

        assert!(chunks.next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_get_entry() {
        let source = MemoryFileSource::builder()
            .add("file.txt", MemoryFsEntry::file("content"))
            .add("dir", MemoryFsEntry::dir())
            .build();

        let file_entry = source
            .get_entry(Path::new("/file.txt"))
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(file_entry, DirectoryListEntry::File(_)));

        let dir_entry = source.get_entry(Path::new("/dir")).await.unwrap().unwrap();
        assert!(matches!(dir_entry, DirectoryListEntry::Directory(_)));

        let missing = source.get_entry(Path::new("/missing")).await.unwrap();
        assert!(missing.is_none());
    }

    #[tokio::test]
    async fn test_executable_file() {
        let source = MemoryFileSource::builder()
            .add("script.sh", MemoryFsEntry::executable("#!/bin/bash"))
            .build();

        let entry = source
            .get_entry(Path::new("/script.sh"))
            .await
            .unwrap()
            .unwrap();
        if let DirectoryListEntry::File(f) = entry {
            assert!(f.executable);
        } else {
            panic!("expected file");
        }
    }

    #[tokio::test]
    async fn test_not_found() {
        let source = MemoryFileSource::empty();

        let result = source.list_directory(Path::new("/missing")).await;
        assert!(matches!(result, Err(FileSourceError::NotFound)));

        let result = source.get_file_chunks(Path::new("/missing")).await;
        assert!(matches!(result, Err(FileSourceError::NotFound)));
    }

    #[tokio::test]
    async fn test_lexical_ordering() {
        let source = MemoryFileSource::builder()
            .add("z.txt", MemoryFsEntry::file(""))
            .add("a.txt", MemoryFsEntry::file(""))
            .add("m.txt", MemoryFsEntry::file(""))
            .build();

        let mut list = source.list_directory(Path::new("/")).await.unwrap();

        assert_eq!(list.next().await.unwrap().unwrap().name(), "a.txt");
        assert_eq!(list.next().await.unwrap().unwrap().name(), "m.txt");
        assert_eq!(list.next().await.unwrap().unwrap().name(), "z.txt");
    }

    #[tokio::test]
    async fn test_repeated_file_content() {
        let source = MemoryFileSource::builder()
            .add("repeated.txt", MemoryFsEntry::repeated(b"abc", 10))
            .build();

        let mut chunks = source
            .get_file_chunks(Path::new("/repeated.txt"))
            .await
            .unwrap();
        let chunk = chunks.next().await.unwrap().unwrap();

        assert_eq!(chunk.data(), b"abcabcabca");
    }

    // New tests for entry-based operations

    #[tokio::test]
    async fn test_dir_entry_list_directory() {
        let source = MemoryFileSource::builder()
            .add("subdir/file.txt", MemoryFsEntry::file("content"))
            .build();

        // Get the directory entry
        let entry = source.get_entry(Path::new("/subdir")).await.unwrap().unwrap();
        if let DirectoryListEntry::Directory(dir) = entry {
            // Use the entry's list_directory method
            let mut list = dir.list_directory().await.unwrap();
            let file = list.next().await.unwrap().unwrap();
            assert_eq!(file.name(), "file.txt");
        } else {
            panic!("expected directory");
        }
    }

    #[tokio::test]
    async fn test_dir_entry_get_entry() {
        let source = MemoryFileSource::builder()
            .add("subdir/file.txt", MemoryFsEntry::file("content"))
            .build();

        // Get the directory entry
        let entry = source.get_entry(Path::new("/subdir")).await.unwrap().unwrap();
        if let DirectoryListEntry::Directory(dir) = entry {
            // Use the entry's get_entry method
            let file = dir.get_entry("file.txt").await.unwrap().unwrap();
            assert_eq!(file.name(), "file.txt");
            assert!(matches!(file, DirectoryListEntry::File(_)));

            // Missing entry returns None
            let missing = dir.get_entry("missing.txt").await.unwrap();
            assert!(missing.is_none());
        } else {
            panic!("expected directory");
        }
    }

    #[tokio::test]
    async fn test_file_entry_get_chunks() {
        let source = MemoryFileSource::builder()
            .add("file.txt", MemoryFsEntry::file("hello world"))
            .build();

        // Get the file entry
        let entry = source.get_entry(Path::new("/file.txt")).await.unwrap().unwrap();
        if let DirectoryListEntry::File(file) = entry {
            // Use the entry's get_chunks method
            let mut chunks = file.get_chunks().await.unwrap();
            let chunk = chunks.next().await.unwrap().unwrap();
            assert_eq!(chunk.data(), b"hello world");
        } else {
            panic!("expected file");
        }
    }

    #[tokio::test]
    async fn test_traversal_from_listing() {
        let source = MemoryFileSource::builder()
            .add("dir/subdir/file.txt", MemoryFsEntry::file("deep content"))
            .build();

        // Start by listing root
        let mut root_list = source.list_directory(Path::new("/")).await.unwrap();
        let dir_entry = root_list.next().await.unwrap().unwrap();

        if let DirectoryListEntry::Directory(dir) = dir_entry {
            // List the directory
            let mut dir_list = dir.list_directory().await.unwrap();
            let subdir_entry = dir_list.next().await.unwrap().unwrap();

            if let DirectoryListEntry::Directory(subdir) = subdir_entry {
                // Get a specific child
                let file = subdir.get_entry("file.txt").await.unwrap().unwrap();
                if let DirectoryListEntry::File(f) = file {
                    let mut chunks = f.get_chunks().await.unwrap();
                    let chunk = chunks.next().await.unwrap().unwrap();
                    assert_eq!(chunk.data(), b"deep content");
                } else {
                    panic!("expected file");
                }
            } else {
                panic!("expected directory");
            }
        } else {
            panic!("expected directory");
        }
    }
}
