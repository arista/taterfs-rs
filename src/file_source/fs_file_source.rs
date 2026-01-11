use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;

use tokio::fs;
use tokio::io::AsyncReadExt;

use crate::file_source::directory_list::{DirectoryList, DirectoryListing};
use crate::file_source::error::{FileSourceError, Result};
use crate::file_source::file_chunks::{FileChunking, FileChunks};
use crate::file_source::file_source::FileSource;
use crate::file_source::types::{
    next_chunk_size, DirEntry, DirectoryListEntry, FileChunk, FileEntry, GetChildEntryFn,
    GetChunksFn, ListDirectoryFn,
};

/// Inner state for FsFileSource, wrapped in Arc for sharing with closures.
struct FsFileSourceInner {
    root: PathBuf,
}

/// A FileSource implementation backed by the local filesystem.
#[derive(Clone)]
pub struct FsFileSource {
    inner: Arc<FsFileSourceInner>,
}

impl FsFileSource {
    /// Create a new FsFileSource rooted at the given path.
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            inner: Arc::new(FsFileSourceInner { root: root.into() }),
        }
    }

    /// Get the root path of this file source.
    pub fn root(&self) -> &Path {
        &self.inner.root
    }

    /// Convert a relative path to an absolute path within this source.
    fn to_abs_path(inner: &FsFileSourceInner, path: &Path) -> PathBuf {
        if path.is_absolute() {
            // Strip leading / and treat as relative to root
            let stripped = path.strip_prefix("/").unwrap_or(path);
            inner.root.join(stripped)
        } else {
            inner.root.join(path)
        }
    }

    /// Get the path relative to the root.
    fn to_rel_path(inner: &FsFileSourceInner, abs_path: &Path) -> PathBuf {
        abs_path
            .strip_prefix(&inner.root)
            .unwrap_or(abs_path)
            .to_path_buf()
    }

    /// Create a closure that lists a directory.
    fn make_list_fn(inner: Arc<FsFileSourceInner>, path: PathBuf) -> ListDirectoryFn {
        Arc::new(move || {
            let inner = Arc::clone(&inner);
            let path = path.clone();
            Box::pin(async move { Self::list_directory_internal(inner, &path).await })
        })
    }

    /// Create a closure that gets a child entry by name.
    fn make_get_child_fn(inner: Arc<FsFileSourceInner>, path: PathBuf) -> GetChildEntryFn {
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
    fn make_get_chunks_fn(inner: Arc<FsFileSourceInner>, path: PathBuf) -> GetChunksFn {
        Arc::new(move || {
            let inner = Arc::clone(&inner);
            let path = path.clone();
            Box::pin(async move { Self::get_file_chunks_internal(inner, &path).await })
        })
    }

    /// Internal implementation of list_directory.
    fn list_directory_internal(
        inner: Arc<FsFileSourceInner>,
        path: &Path,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<DirectoryList>> + Send>> {
        let path = path.to_path_buf();
        Box::pin(async move {
            let abs_path = Self::to_abs_path(&inner, &path);

            let mut read_dir = fs::read_dir(&abs_path).await?;
            let mut entries = Vec::new();

            while let Some(entry) = read_dir.next_entry().await? {
                let file_name = entry.file_name().to_string_lossy().to_string();
                let file_type = entry.file_type().await?;
                let entry_abs_path = entry.path();
                let entry_path = Self::to_rel_path(&inner, &entry_abs_path);

                if file_type.is_dir() {
                    entries.push((file_name, entry_path, FsEntryType::Directory));
                } else if file_type.is_file() {
                    let metadata = entry.metadata().await?;
                    let executable = is_executable(&metadata);
                    let size = metadata.len();
                    entries.push((
                        file_name,
                        entry_path,
                        FsEntryType::File { size, executable },
                    ));
                }
                // Skip symlinks and other special files
            }

            // Sort by name for lexical ordering
            entries.sort_by(|a, b| a.0.cmp(&b.0));

            Ok(DirectoryList::new(FsDirectoryList {
                inner,
                entries,
                index: 0,
            }))
        })
    }

    /// Internal implementation of get_entry.
    fn get_entry_internal(
        inner: Arc<FsFileSourceInner>,
        path: &Path,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<Option<DirectoryListEntry>>> + Send>> {
        let path = path.to_path_buf();
        Box::pin(async move {
            let abs_path = Self::to_abs_path(&inner, &path);

            let metadata = match fs::metadata(&abs_path).await {
                Ok(m) => m,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
                Err(e) => return Err(e.into()),
            };

            let file_name = abs_path
                .file_name()
                .map(|n| n.to_string_lossy().to_string())
                .unwrap_or_default();

            let entry_path = Self::to_rel_path(&inner, &abs_path);

            let entry = if metadata.is_dir() {
                let list_fn = Self::make_list_fn(Arc::clone(&inner), entry_path.clone());
                let get_child_fn = Self::make_get_child_fn(Arc::clone(&inner), entry_path.clone());
                DirectoryListEntry::Directory(DirEntry::new(
                    file_name,
                    entry_path,
                    list_fn,
                    get_child_fn,
                ))
            } else if metadata.is_file() {
                let executable = is_executable(&metadata);
                let get_chunks_fn = Self::make_get_chunks_fn(Arc::clone(&inner), entry_path.clone());
                DirectoryListEntry::File(FileEntry::new(
                    file_name,
                    entry_path,
                    metadata.len(),
                    executable,
                    get_chunks_fn,
                ))
            } else {
                // Symlinks and other special files return None
                return Ok(None);
            };

            Ok(Some(entry))
        })
    }

    /// Internal implementation of get_file_chunks.
    fn get_file_chunks_internal(
        inner: Arc<FsFileSourceInner>,
        path: &Path,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<FileChunks>> + Send>> {
        let path = path.to_path_buf();
        Box::pin(async move {
            let abs_path = Self::to_abs_path(&inner, &path);

            let metadata = fs::metadata(&abs_path).await?;
            if !metadata.is_file() {
                return Err(FileSourceError::NotFound);
            }

            let file = fs::File::open(&abs_path).await?;
            let size = metadata.len();

            Ok(FileChunks::new(FsFileChunks {
                file,
                size,
                offset: 0,
            }))
        })
    }
}

impl FileSource for FsFileSource {
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

/// Check if a file is executable based on its metadata.
#[cfg(unix)]
fn is_executable(metadata: &std::fs::Metadata) -> bool {
    use std::os::unix::fs::PermissionsExt;
    metadata.permissions().mode() & 0o111 != 0
}

#[cfg(not(unix))]
fn is_executable(_metadata: &std::fs::Metadata) -> bool {
    false
}

/// Entry type for FsDirectoryList.
enum FsEntryType {
    Directory,
    File { size: u64, executable: bool },
}

/// Directory listing iterator for FsFileSource.
struct FsDirectoryList {
    inner: Arc<FsFileSourceInner>,
    entries: Vec<(String, PathBuf, FsEntryType)>,
    index: usize,
}

impl DirectoryListing for FsDirectoryList {
    async fn next(&mut self) -> Result<Option<DirectoryListEntry>> {
        if self.index >= self.entries.len() {
            return Ok(None);
        }

        let (name, entry_path, entry_type) = &self.entries[self.index];
        self.index += 1;

        let entry = match entry_type {
            FsEntryType::Directory => {
                let list_fn = FsFileSource::make_list_fn(Arc::clone(&self.inner), entry_path.clone());
                let get_child_fn =
                    FsFileSource::make_get_child_fn(Arc::clone(&self.inner), entry_path.clone());
                DirectoryListEntry::Directory(DirEntry::new(
                    name.clone(),
                    entry_path.clone(),
                    list_fn,
                    get_child_fn,
                ))
            }
            FsEntryType::File { size, executable } => {
                let get_chunks_fn =
                    FsFileSource::make_get_chunks_fn(Arc::clone(&self.inner), entry_path.clone());
                DirectoryListEntry::File(FileEntry::new(
                    name.clone(),
                    entry_path.clone(),
                    *size,
                    *executable,
                    get_chunks_fn,
                ))
            }
        };

        Ok(Some(entry))
    }
}

/// File chunks iterator for FsFileSource.
struct FsFileChunks {
    file: fs::File,
    size: u64,
    offset: u64,
}

impl FileChunking for FsFileChunks {
    async fn next(&mut self) -> Result<Option<FileChunk>> {
        if self.offset >= self.size {
            return Ok(None);
        }

        let remaining = self.size - self.offset;
        let chunk_size = next_chunk_size(remaining);

        let mut buffer = vec![0u8; chunk_size as usize];
        let mut bytes_read = 0;

        while bytes_read < chunk_size as usize {
            let n = self.file.read(&mut buffer[bytes_read..]).await?;
            if n == 0 {
                break;
            }
            bytes_read += n;
        }

        buffer.truncate(bytes_read);

        let chunk = FileChunk::new(self.offset, buffer);
        self.offset += bytes_read as u64;

        Ok(Some(chunk))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    use tokio::fs;
    use tokio::io::AsyncWriteExt;

    async fn create_test_dir() -> TempDir {
        tempfile::tempdir().unwrap()
    }

    #[tokio::test]
    async fn test_list_empty_directory() {
        let temp_dir = create_test_dir().await;
        let source = FsFileSource::new(temp_dir.path());

        let mut list = source.list_directory(Path::new("/")).await.unwrap();
        assert!(list.next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_list_directory_with_files() {
        let temp_dir = create_test_dir().await;

        // Create some files
        fs::write(temp_dir.path().join("a.txt"), "hello").await.unwrap();
        fs::write(temp_dir.path().join("b.txt"), "world").await.unwrap();
        fs::create_dir(temp_dir.path().join("subdir")).await.unwrap();

        let source = FsFileSource::new(temp_dir.path());
        let mut list = source.list_directory(Path::new("/")).await.unwrap();

        // Should be in lexical order
        let entry1 = list.next().await.unwrap().unwrap();
        assert_eq!(entry1.name(), "a.txt");
        assert!(matches!(entry1, DirectoryListEntry::File(_)));

        let entry2 = list.next().await.unwrap().unwrap();
        assert_eq!(entry2.name(), "b.txt");

        let entry3 = list.next().await.unwrap().unwrap();
        assert_eq!(entry3.name(), "subdir");
        assert!(matches!(entry3, DirectoryListEntry::Directory(_)));

        assert!(list.next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_file_chunks_small_file() {
        let temp_dir = create_test_dir().await;
        fs::write(temp_dir.path().join("small.txt"), "hello world")
            .await
            .unwrap();

        let source = FsFileSource::new(temp_dir.path());
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
    async fn test_file_chunks_larger_file() {
        let temp_dir = create_test_dir().await;

        // Create a file that spans multiple chunk sizes
        // 4MB + 1MB + some remainder = 5MB + 100KB
        let size = 5 * 1024 * 1024 + 100 * 1024;
        let data = vec![0x42u8; size];
        fs::write(temp_dir.path().join("large.bin"), &data)
            .await
            .unwrap();

        let source = FsFileSource::new(temp_dir.path());
        let mut chunks = source
            .get_file_chunks(Path::new("/large.bin"))
            .await
            .unwrap();

        // First chunk: 4MB
        let chunk1 = chunks.next().await.unwrap().unwrap();
        assert_eq!(chunk1.offset(), 0);
        assert_eq!(chunk1.size(), 4 * 1024 * 1024);

        // Second chunk: 1MB
        let chunk2 = chunks.next().await.unwrap().unwrap();
        assert_eq!(chunk2.offset(), 4 * 1024 * 1024);
        assert_eq!(chunk2.size(), 1024 * 1024);

        // Third chunk: 64KB
        let chunk3 = chunks.next().await.unwrap().unwrap();
        assert_eq!(chunk3.offset(), 5 * 1024 * 1024);
        assert_eq!(chunk3.size(), 64 * 1024);

        // Fourth chunk: 16KB
        let chunk4 = chunks.next().await.unwrap().unwrap();
        assert_eq!(chunk4.size(), 16 * 1024);

        // Fifth chunk: 16KB
        let chunk5 = chunks.next().await.unwrap().unwrap();
        assert_eq!(chunk5.size(), 16 * 1024);

        // Sixth chunk: remaining 4KB
        let chunk6 = chunks.next().await.unwrap().unwrap();
        assert_eq!(chunk6.size(), 4 * 1024);

        assert!(chunks.next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_get_entry_file() {
        let temp_dir = create_test_dir().await;
        fs::write(temp_dir.path().join("test.txt"), "content")
            .await
            .unwrap();

        let source = FsFileSource::new(temp_dir.path());
        let entry = source
            .get_entry(Path::new("/test.txt"))
            .await
            .unwrap()
            .unwrap();

        assert!(matches!(entry, DirectoryListEntry::File(_)));
        if let DirectoryListEntry::File(f) = entry {
            assert_eq!(f.size, 7);
        }
    }

    #[tokio::test]
    async fn test_get_entry_directory() {
        let temp_dir = create_test_dir().await;
        fs::create_dir(temp_dir.path().join("subdir")).await.unwrap();

        let source = FsFileSource::new(temp_dir.path());
        let entry = source
            .get_entry(Path::new("/subdir"))
            .await
            .unwrap()
            .unwrap();

        assert!(matches!(entry, DirectoryListEntry::Directory(_)));
    }

    #[tokio::test]
    async fn test_get_entry_not_found() {
        let temp_dir = create_test_dir().await;
        let source = FsFileSource::new(temp_dir.path());

        let entry = source.get_entry(Path::new("/missing")).await.unwrap();
        assert!(entry.is_none());
    }

    #[tokio::test]
    async fn test_list_directory_not_found() {
        let temp_dir = create_test_dir().await;
        let source = FsFileSource::new(temp_dir.path());

        let result = source.list_directory(Path::new("/missing")).await;
        assert!(matches!(result, Err(FileSourceError::NotFound)));
    }

    #[tokio::test]
    async fn test_nested_directory() {
        let temp_dir = create_test_dir().await;
        fs::create_dir_all(temp_dir.path().join("a/b")).await.unwrap();
        fs::write(temp_dir.path().join("a/b/file.txt"), "nested")
            .await
            .unwrap();

        let source = FsFileSource::new(temp_dir.path());

        // List nested directory
        let mut list = source.list_directory(Path::new("/a/b")).await.unwrap();
        let entry = list.next().await.unwrap().unwrap();
        assert_eq!(entry.name(), "file.txt");
        assert_eq!(entry.path(), &PathBuf::from("a/b/file.txt"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn test_executable_file() {
        use std::os::unix::fs::PermissionsExt;

        let temp_dir = create_test_dir().await;
        let script_path = temp_dir.path().join("script.sh");

        let mut file = fs::File::create(&script_path).await.unwrap();
        file.write_all(b"#!/bin/bash\necho hello").await.unwrap();
        drop(file);

        // Make executable
        let mut perms = std::fs::metadata(&script_path).unwrap().permissions();
        perms.set_mode(0o755);
        std::fs::set_permissions(&script_path, perms).unwrap();

        let source = FsFileSource::new(temp_dir.path());
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

    // New tests for entry-based operations

    #[tokio::test]
    async fn test_dir_entry_list_directory() {
        let temp_dir = create_test_dir().await;
        fs::create_dir(temp_dir.path().join("subdir")).await.unwrap();
        fs::write(temp_dir.path().join("subdir/file.txt"), "content")
            .await
            .unwrap();

        let source = FsFileSource::new(temp_dir.path());

        // Get the directory entry
        let entry = source
            .get_entry(Path::new("/subdir"))
            .await
            .unwrap()
            .unwrap();
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
        let temp_dir = create_test_dir().await;
        fs::create_dir(temp_dir.path().join("subdir")).await.unwrap();
        fs::write(temp_dir.path().join("subdir/file.txt"), "content")
            .await
            .unwrap();

        let source = FsFileSource::new(temp_dir.path());

        // Get the directory entry
        let entry = source
            .get_entry(Path::new("/subdir"))
            .await
            .unwrap()
            .unwrap();
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
        let temp_dir = create_test_dir().await;
        fs::write(temp_dir.path().join("file.txt"), "hello world")
            .await
            .unwrap();

        let source = FsFileSource::new(temp_dir.path());

        // Get the file entry
        let entry = source
            .get_entry(Path::new("/file.txt"))
            .await
            .unwrap()
            .unwrap();
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
        let temp_dir = create_test_dir().await;
        fs::create_dir_all(temp_dir.path().join("dir/subdir"))
            .await
            .unwrap();
        fs::write(temp_dir.path().join("dir/subdir/file.txt"), "deep content")
            .await
            .unwrap();

        let source = FsFileSource::new(temp_dir.path());

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
