use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use tokio::fs;

use super::fs_like_repo_backend::FsLikeRepoBackend;
use super::repo_backend::{BackendError, Result};

/// Counter for generating unique temp file names.
static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

/// A filesystem-based implementation of `FsLikeRepoBackend`.
///
/// All paths are relative to a configured base directory.
/// Writes are atomic: data is written to a temp file in `.tfs/` then renamed.
pub struct FsBackend {
    base_path: PathBuf,
}

impl FsBackend {
    /// Create a new filesystem backend rooted at the given path.
    pub fn new(base_path: impl AsRef<Path>) -> Self {
        Self {
            base_path: base_path.as_ref().to_path_buf(),
        }
    }

    /// Resolve a relative path against the base path.
    fn full_path(&self, relative: &str) -> PathBuf {
        self.base_path.join(relative)
    }

    /// Get the path to the temp directory.
    fn temp_dir(&self) -> PathBuf {
        self.base_path.join(".tfs")
    }

    /// Generate a unique temp file path.
    fn temp_file_path(&self) -> PathBuf {
        let counter = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
        let pid = std::process::id();
        self.temp_dir().join(format!("tmp.{}.{}", pid, counter))
    }
}

impl FsLikeRepoBackend for FsBackend {
    async fn file_exists(&self, path: &str) -> Result<bool> {
        let full_path = self.full_path(path);
        Ok(full_path.exists())
    }

    async fn read_file(&self, path: &str) -> Result<Vec<u8>> {
        let full_path = self.full_path(path);
        fs::read(&full_path).await.map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                BackendError::NotFound
            } else {
                BackendError::Io(e)
            }
        })
    }

    async fn write_file(&self, path: &str, data: &[u8]) -> Result<()> {
        let full_path = self.full_path(path);
        let temp_path = self.temp_file_path();

        // Ensure temp directory exists
        fs::create_dir_all(self.temp_dir()).await?;

        // Write to temp file
        fs::write(&temp_path, data).await?;

        // Create parent directories of final path if needed
        if let Some(parent) = full_path.parent() {
            fs::create_dir_all(parent).await?;
        }

        // Atomically rename temp file to final location
        fs::rename(&temp_path, &full_path).await?;
        Ok(())
    }

    async fn first_file(&self, directory: &str) -> Result<Option<String>> {
        let full_path = self.full_path(directory);

        let mut entries = match fs::read_dir(&full_path).await {
            Ok(entries) => entries,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(BackendError::Io(e)),
        };

        let mut first: Option<String> = None;

        while let Some(entry) = entries.next_entry().await? {
            let name = entry.file_name().to_string_lossy().into_owned();
            match &first {
                None => first = Some(name),
                Some(current) if name < *current => first = Some(name),
                _ => {}
            }
        }

        Ok(first)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_file_roundtrip() {
        let temp_dir = TempDir::new().unwrap();
        let backend = FsBackend::new(temp_dir.path());

        let path = "test/nested/file.txt";
        let data = b"hello world";

        assert!(!backend.file_exists(path).await.unwrap());

        backend.write_file(path, data).await.unwrap();

        assert!(backend.file_exists(path).await.unwrap());
        assert_eq!(backend.read_file(path).await.unwrap(), data);
    }

    #[tokio::test]
    async fn test_read_nonexistent_file() {
        let temp_dir = TempDir::new().unwrap();
        let backend = FsBackend::new(temp_dir.path());

        let result = backend.read_file("nonexistent.txt").await;
        assert!(matches!(result, Err(BackendError::NotFound)));
    }

    #[tokio::test]
    async fn test_first_file_empty_dir() {
        let temp_dir = TempDir::new().unwrap();
        let backend = FsBackend::new(temp_dir.path());

        // Directory doesn't exist
        let result = backend.first_file("nonexistent").await.unwrap();
        assert_eq!(result, None);

        // Create empty directory
        fs::create_dir(temp_dir.path().join("empty")).await.unwrap();
        let result = backend.first_file("empty").await.unwrap();
        assert_eq!(result, None);
    }

    #[tokio::test]
    async fn test_first_file_returns_lexically_first() {
        let temp_dir = TempDir::new().unwrap();
        let backend = FsBackend::new(temp_dir.path());

        // Create files in non-lexical order
        backend.write_file("dir/charlie", b"").await.unwrap();
        backend.write_file("dir/alpha", b"").await.unwrap();
        backend.write_file("dir/bravo", b"").await.unwrap();

        let result = backend.first_file("dir").await.unwrap();
        assert_eq!(result, Some("alpha".to_string()));
    }

    #[tokio::test]
    async fn test_overwrite_file() {
        let temp_dir = TempDir::new().unwrap();
        let backend = FsBackend::new(temp_dir.path());

        let path = "file.txt";

        backend.write_file(path, b"first").await.unwrap();
        assert_eq!(backend.read_file(path).await.unwrap(), b"first");

        backend.write_file(path, b"second").await.unwrap();
        assert_eq!(backend.read_file(path).await.unwrap(), b"second");
    }
}
