use std::path::{Path, PathBuf};

use tokio::fs;
use tokio::io::AsyncReadExt;

use crate::file_source::directory_list::{DirectoryList, DirectoryListing};
use crate::file_source::error::{FileSourceError, Result};
use crate::file_source::file_chunks::{FileChunking, FileChunks};
use crate::file_source::file_source::FileSource;
use crate::file_source::types::{
    next_chunk_size, DirEntry, DirectoryListEntry, DirectoryListEntryName, FileChunk, FileEntry,
};

/// A FileSource implementation backed by the local filesystem.
#[derive(Clone)]
pub struct FsFileSource {
    root: PathBuf,
}

impl FsFileSource {
    /// Create a new FsFileSource rooted at the given path.
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    /// Get the root path of this file source.
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Convert a relative path to an absolute path within this source.
    fn to_abs_path(&self, path: &Path) -> PathBuf {
        if path.is_absolute() {
            // Strip leading / and treat as relative to root
            let stripped = path
                .strip_prefix("/")
                .unwrap_or(path);
            self.root.join(stripped)
        } else {
            self.root.join(path)
        }
    }
}

impl FileSource for FsFileSource {
    async fn list_directory(&self, path: &Path) -> Result<DirectoryList> {
        let abs_path = self.to_abs_path(path);

        let mut read_dir = fs::read_dir(&abs_path).await?;
        let mut entries = Vec::new();

        while let Some(entry) = read_dir.next_entry().await? {
            let file_name = entry.file_name().to_string_lossy().to_string();
            let file_type = entry.file_type().await?;
            let entry_abs_path = entry.path();
            let rel_path = entry_abs_path
                .strip_prefix(&self.root)
                .unwrap_or(&entry_abs_path)
                .to_path_buf();

            let entry_name = DirectoryListEntryName {
                name: file_name.clone(),
                abs_path: entry_abs_path.clone(),
                rel_path,
            };

            let dir_entry = if file_type.is_dir() {
                DirectoryListEntry::Directory(DirEntry { name: entry_name })
            } else if file_type.is_file() {
                let metadata = entry.metadata().await?;
                let executable = is_executable(&metadata);
                DirectoryListEntry::File(FileEntry {
                    name: entry_name,
                    size: metadata.len(),
                    executable,
                })
            } else {
                // Skip symlinks and other special files for now
                continue;
            };

            entries.push((file_name, dir_entry));
        }

        // Sort by name for lexical ordering
        entries.sort_by(|a, b| a.0.cmp(&b.0));

        Ok(DirectoryList::new(FsDirectoryList { entries, index: 0 }))
    }

    async fn get_file_chunks(&self, path: &Path) -> Result<FileChunks> {
        let abs_path = self.to_abs_path(path);

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
    }

    async fn get_entry(&self, path: &Path) -> Result<Option<DirectoryListEntry>> {
        let abs_path = self.to_abs_path(path);

        let metadata = match fs::metadata(&abs_path).await {
            Ok(m) => m,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e.into()),
        };

        let file_name = abs_path
            .file_name()
            .map(|n| n.to_string_lossy().to_string())
            .unwrap_or_default();

        let rel_path = abs_path
            .strip_prefix(&self.root)
            .unwrap_or(&abs_path)
            .to_path_buf();

        let entry_name = DirectoryListEntryName {
            name: file_name,
            abs_path: abs_path.clone(),
            rel_path,
        };

        let entry = if metadata.is_dir() {
            DirectoryListEntry::Directory(DirEntry { name: entry_name })
        } else if metadata.is_file() {
            let executable = is_executable(&metadata);
            DirectoryListEntry::File(FileEntry {
                name: entry_name,
                size: metadata.len(),
                executable,
            })
        } else {
            // Symlinks and other special files return None
            return Ok(None);
        };

        Ok(Some(entry))
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

/// Directory listing iterator for FsFileSource.
struct FsDirectoryList {
    entries: Vec<(String, DirectoryListEntry)>,
    index: usize,
}

impl DirectoryListing for FsDirectoryList {
    async fn next(&mut self) -> Result<Option<DirectoryListEntry>> {
        if self.index >= self.entries.len() {
            return Ok(None);
        }
        let (_, entry) = self.entries[self.index].clone();
        self.index += 1;
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
        assert_eq!(entry1.base_name(), "a.txt");
        assert!(matches!(entry1, DirectoryListEntry::File(_)));

        let entry2 = list.next().await.unwrap().unwrap();
        assert_eq!(entry2.base_name(), "b.txt");

        let entry3 = list.next().await.unwrap().unwrap();
        assert_eq!(entry3.base_name(), "subdir");
        assert!(matches!(entry3, DirectoryListEntry::Directory(_)));

        assert!(list.next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_file_chunks_small_file() {
        let temp_dir = create_test_dir().await;
        fs::write(temp_dir.path().join("small.txt"), "hello world").await.unwrap();

        let source = FsFileSource::new(temp_dir.path());
        let mut chunks = source.get_file_chunks(Path::new("/small.txt")).await.unwrap();

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
        fs::write(temp_dir.path().join("large.bin"), &data).await.unwrap();

        let source = FsFileSource::new(temp_dir.path());
        let mut chunks = source.get_file_chunks(Path::new("/large.bin")).await.unwrap();

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
        fs::write(temp_dir.path().join("test.txt"), "content").await.unwrap();

        let source = FsFileSource::new(temp_dir.path());
        let entry = source.get_entry(Path::new("/test.txt")).await.unwrap().unwrap();

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
        let entry = source.get_entry(Path::new("/subdir")).await.unwrap().unwrap();

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
        fs::write(temp_dir.path().join("a/b/file.txt"), "nested").await.unwrap();

        let source = FsFileSource::new(temp_dir.path());

        // List nested directory
        let mut list = source.list_directory(Path::new("/a/b")).await.unwrap();
        let entry = list.next().await.unwrap().unwrap();
        assert_eq!(entry.base_name(), "file.txt");
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
        let entry = source.get_entry(Path::new("/script.sh")).await.unwrap().unwrap();

        if let DirectoryListEntry::File(f) = entry {
            assert!(f.executable);
        } else {
            panic!("expected file");
        }
    }
}
