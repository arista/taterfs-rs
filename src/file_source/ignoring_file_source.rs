use std::path::{Path, PathBuf};
use std::sync::Arc;

use ignore::gitignore::{Gitignore, GitignoreBuilder};

use crate::file_source::directory_list::{DirectoryList, DirectoryListing};
use crate::file_source::error::{FileSourceError, Result};
use crate::file_source::file_chunks::FileChunks;
use crate::file_source::file_source::FileSource;
use crate::file_source::types::DirectoryListEntry;

/// A FileSource wrapper that filters entries based on ignore rules.
///
/// Filters out:
/// - The `.tfs/` directory (always)
/// - Files/directories matching patterns in `.gitignore` files
/// - Files/directories matching patterns in `.tfsignore` files
///
/// When both `.gitignore` and `.tfsignore` are present, they are treated as
/// concatenated with `.gitignore` patterns first.
///
/// Ignore rules are inherited from parent directories, following git semantics.
pub struct IgnoringFileSource<S: FileSource> {
    inner: Arc<S>,
}

impl<S: FileSource> IgnoringFileSource<S> {
    /// Create a new IgnoringFileSource wrapping the given source.
    pub fn new(inner: S) -> Self {
        Self {
            inner: Arc::new(inner),
        }
    }

    /// Build a Gitignore matcher for the given directory path.
    ///
    /// This loads ignore files from the root down to the target path,
    /// accumulating rules as git does.
    async fn build_ignore_matcher(&self, path: &Path) -> Gitignore {
        let mut builder = GitignoreBuilder::new(Path::new(""));

        // Collect all path components from root to target
        let components: Vec<_> = path.components().collect();

        // Start from root and work down, loading ignore files at each level
        let mut current_path = PathBuf::new();

        // Load ignore files from root first
        self.load_ignore_files(&mut builder, Path::new("")).await;

        // Then load from each directory down to the target
        for component in &components {
            current_path.push(component);
            self.load_ignore_files(&mut builder, &current_path).await;
        }

        builder.build().unwrap_or_else(|_| Gitignore::empty())
    }

    /// Load .gitignore and .tfsignore files from a directory into the builder.
    async fn load_ignore_files(&self, builder: &mut GitignoreBuilder, dir_path: &Path) {
        // Load .gitignore first
        let gitignore_path = if dir_path.as_os_str().is_empty() {
            PathBuf::from(".gitignore")
        } else {
            dir_path.join(".gitignore")
        };
        self.load_ignore_file(builder, &gitignore_path).await;

        // Then load .tfsignore (patterns added after .gitignore)
        let tfsignore_path = if dir_path.as_os_str().is_empty() {
            PathBuf::from(".tfsignore")
        } else {
            dir_path.join(".tfsignore")
        };
        self.load_ignore_file(builder, &tfsignore_path).await;
    }

    /// Load patterns from a single ignore file into the builder.
    async fn load_ignore_file(&self, builder: &mut GitignoreBuilder, file_path: &Path) {
        // Try to read the file
        let chunks_result = self.inner.get_file_chunks(file_path).await;
        let mut chunks = match chunks_result {
            Ok(chunks) => chunks,
            Err(_) => return, // File doesn't exist or can't be read, skip it
        };

        // Collect all chunks into a single buffer
        let mut contents = Vec::new();
        loop {
            match chunks.next().await {
                Ok(Some(chunk)) => contents.extend_from_slice(chunk.data()),
                Ok(None) => break,
                Err(_) => return, // Error reading, skip this file
            }
        }

        // Parse as UTF-8 and add each line as a pattern
        if let Ok(text) = String::from_utf8(contents) {
            for line in text.lines() {
                // GitignoreBuilder::add_line handles comments and empty lines
                let _ = builder.add_line(Some(file_path.to_path_buf()), line);
            }
        }
    }

    /// Check if a path should be ignored.
    fn is_ignored(&self, matcher: &Gitignore, entry: &DirectoryListEntry) -> bool {
        let name = entry.name();
        let path = entry.path();
        let is_dir = matches!(entry, DirectoryListEntry::Directory(_));

        // Always ignore .tfs and .git directories
        if is_dir && (name == ".tfs" || name == ".git") {
            return true;
        }

        // Check against gitignore patterns
        matcher.matched(path, is_dir).is_ignore()
    }
}

impl<S: FileSource + 'static> FileSource for IgnoringFileSource<S> {
    async fn list_directory(&self, path: &Path) -> Result<DirectoryList> {
        // Build the ignore matcher for this directory path
        let matcher = self.build_ignore_matcher(path).await;

        // Get the inner directory listing
        let inner_list = self.inner.list_directory(path).await?;

        Ok(DirectoryList::new(IgnoringDirectoryList {
            inner: inner_list,
            matcher,
            inner_source: Arc::clone(&self.inner),
        }))
    }

    async fn get_file_chunks(&self, path: &Path) -> Result<FileChunks> {
        // Check if this file should be ignored
        // We need to check against ignore rules from parent directories
        if let Some(parent) = path.parent() {
            let matcher = self.build_ignore_matcher(parent).await;

            // Check if the file matches ignore patterns
            if matcher.matched(path, false).is_ignore() {
                return Err(FileSourceError::NotFound);
            }

            // Also check for .tfs or .git in path components
            for component in path.components() {
                if let std::path::Component::Normal(name) = component {
                    if name == ".tfs" || name == ".git" {
                        return Err(FileSourceError::NotFound);
                    }
                }
            }
        }

        self.inner.get_file_chunks(path).await
    }

    async fn get_entry(&self, path: &Path) -> Result<Option<DirectoryListEntry>> {
        // Check if this path should be ignored
        if let Some(parent) = path.parent() {
            let matcher = self.build_ignore_matcher(parent).await;

            // First get the entry to know if it's a file or directory
            let entry = self.inner.get_entry(path).await?;

            if let Some(ref entry) = entry {
                if self.is_ignored(&matcher, entry) {
                    return Ok(None);
                }
            }

            Ok(entry)
        } else {
            // Root path, just check for .tfs and .git
            let entry = self.inner.get_entry(path).await?;
            if let Some(ref e) = entry {
                let name = e.name();
                if (name == ".tfs" || name == ".git")
                    && matches!(e, DirectoryListEntry::Directory(_))
                {
                    return Ok(None);
                }
            }
            Ok(entry)
        }
    }
}

/// Filtering directory listing that skips ignored entries.
struct IgnoringDirectoryList<S: FileSource> {
    inner: DirectoryList,
    matcher: Gitignore,
    #[allow(dead_code)]
    inner_source: Arc<S>,
}

impl<S: FileSource> DirectoryListing for IgnoringDirectoryList<S> {
    async fn next(&mut self) -> Result<Option<DirectoryListEntry>> {
        loop {
            match self.inner.next().await? {
                Some(entry) => {
                    let name = entry.name();
                    let path = entry.path();
                    let is_dir = matches!(entry, DirectoryListEntry::Directory(_));

                    // Always ignore .tfs and .git directories
                    if is_dir && (name == ".tfs" || name == ".git") {
                        continue;
                    }

                    // Check against gitignore patterns
                    if self.matcher.matched(path, is_dir).is_ignore() {
                        continue;
                    }

                    return Ok(Some(entry));
                }
                None => return Ok(None),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::file_source::{MemoryFileSource, MemoryFsEntry};

    #[tokio::test]
    async fn test_ignores_tfs_and_git_directories() {
        let inner = MemoryFileSource::builder()
            .add("file.txt", MemoryFsEntry::file("content"))
            .add(".tfs/config", MemoryFsEntry::file("config"))
            .add(".git/HEAD", MemoryFsEntry::file("ref: refs/heads/main"))
            .add("other/file.txt", MemoryFsEntry::file("other"))
            .build();

        let source = IgnoringFileSource::new(inner);

        // List root directory
        let mut listing = source.list_directory(Path::new("")).await.unwrap();

        let mut entries = Vec::new();
        while let Some(entry) = listing.next().await.unwrap() {
            entries.push(entry.name().to_string());
        }

        // .tfs and .git should not appear
        assert!(!entries.contains(&".tfs".to_string()));
        assert!(!entries.contains(&".git".to_string()));
        assert!(entries.contains(&"file.txt".to_string()));
        assert!(entries.contains(&"other".to_string()));
    }

    #[tokio::test]
    async fn test_gitignore_patterns() {
        let inner = MemoryFileSource::builder()
            .add(".gitignore", MemoryFsEntry::file("*.log\nbuild/\n"))
            .add("app.log", MemoryFsEntry::file("log content"))
            .add("main.rs", MemoryFsEntry::file("fn main() {}"))
            .add("build/output", MemoryFsEntry::file("output"))
            .build();

        let source = IgnoringFileSource::new(inner);

        let mut listing = source.list_directory(Path::new("")).await.unwrap();

        let mut entries = Vec::new();
        while let Some(entry) = listing.next().await.unwrap() {
            entries.push(entry.name().to_string());
        }

        // *.log files should be ignored
        assert!(!entries.contains(&"app.log".to_string()));
        // build/ directory should be ignored
        assert!(!entries.contains(&"build".to_string()));
        // main.rs should be present
        assert!(entries.contains(&"main.rs".to_string()));
        // .gitignore itself should be present (not self-ignored)
        assert!(entries.contains(&".gitignore".to_string()));
    }

    #[tokio::test]
    async fn test_tfsignore_patterns() {
        let inner = MemoryFileSource::builder()
            .add(".tfsignore", MemoryFsEntry::file("secret.txt\n"))
            .add("secret.txt", MemoryFsEntry::file("secret"))
            .add("public.txt", MemoryFsEntry::file("public"))
            .build();

        let source = IgnoringFileSource::new(inner);

        let mut listing = source.list_directory(Path::new("")).await.unwrap();

        let mut entries = Vec::new();
        while let Some(entry) = listing.next().await.unwrap() {
            entries.push(entry.name().to_string());
        }

        assert!(!entries.contains(&"secret.txt".to_string()));
        assert!(entries.contains(&"public.txt".to_string()));
    }

    #[tokio::test]
    async fn test_combined_gitignore_and_tfsignore() {
        let inner = MemoryFileSource::builder()
            .add(".gitignore", MemoryFsEntry::file("*.log\n"))
            .add(".tfsignore", MemoryFsEntry::file("*.tmp\n"))
            .add("app.log", MemoryFsEntry::file("log"))
            .add("cache.tmp", MemoryFsEntry::file("tmp"))
            .add("main.rs", MemoryFsEntry::file("code"))
            .build();

        let source = IgnoringFileSource::new(inner);

        let mut listing = source.list_directory(Path::new("")).await.unwrap();

        let mut entries = Vec::new();
        while let Some(entry) = listing.next().await.unwrap() {
            entries.push(entry.name().to_string());
        }

        // Both .gitignore and .tfsignore patterns should be applied
        assert!(!entries.contains(&"app.log".to_string()));
        assert!(!entries.contains(&"cache.tmp".to_string()));
        assert!(entries.contains(&"main.rs".to_string()));
    }

    #[tokio::test]
    async fn test_inherited_ignore_rules() {
        let inner = MemoryFileSource::builder()
            .add(".gitignore", MemoryFsEntry::file("*.log\n"))
            .add("src/app.log", MemoryFsEntry::file("log"))
            .add("src/main.rs", MemoryFsEntry::file("code"))
            .build();

        let source = IgnoringFileSource::new(inner);

        // List the src/ subdirectory - should inherit root .gitignore
        let mut listing = source.list_directory(Path::new("src")).await.unwrap();

        let mut entries = Vec::new();
        while let Some(entry) = listing.next().await.unwrap() {
            entries.push(entry.name().to_string());
        }

        // *.log pattern from root .gitignore should apply to src/
        assert!(!entries.contains(&"app.log".to_string()));
        assert!(entries.contains(&"main.rs".to_string()));
    }

    #[tokio::test]
    async fn test_get_entry_respects_ignore() {
        let inner = MemoryFileSource::builder()
            .add(".gitignore", MemoryFsEntry::file("secret.txt\n"))
            .add("secret.txt", MemoryFsEntry::file("secret"))
            .add("public.txt", MemoryFsEntry::file("public"))
            .build();

        let source = IgnoringFileSource::new(inner);

        // Ignored file should return None
        let entry = source.get_entry(Path::new("secret.txt")).await.unwrap();
        assert!(entry.is_none());

        // Non-ignored file should return Some
        let entry = source.get_entry(Path::new("public.txt")).await.unwrap();
        assert!(entry.is_some());
    }

    #[tokio::test]
    async fn test_get_file_chunks_respects_ignore() {
        let inner = MemoryFileSource::builder()
            .add(".gitignore", MemoryFsEntry::file("secret.txt\n"))
            .add("secret.txt", MemoryFsEntry::file("secret"))
            .add("public.txt", MemoryFsEntry::file("public"))
            .build();

        let source = IgnoringFileSource::new(inner);

        // Ignored file should return NotFound
        let result = source.get_file_chunks(Path::new("secret.txt")).await;
        assert!(matches!(result, Err(FileSourceError::NotFound)));

        // Non-ignored file should work
        let result = source.get_file_chunks(Path::new("public.txt")).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_negation_patterns() {
        let inner = MemoryFileSource::builder()
            .add(".gitignore", MemoryFsEntry::file("*.log\n!important.log\n"))
            .add("app.log", MemoryFsEntry::file("log"))
            .add("important.log", MemoryFsEntry::file("important"))
            .build();

        let source = IgnoringFileSource::new(inner);

        let mut listing = source.list_directory(Path::new("")).await.unwrap();

        let mut entries = Vec::new();
        while let Some(entry) = listing.next().await.unwrap() {
            entries.push(entry.name().to_string());
        }

        // app.log should be ignored
        assert!(!entries.contains(&"app.log".to_string()));
        // important.log should NOT be ignored (negation pattern)
        assert!(entries.contains(&"important.log".to_string()));
    }

    #[tokio::test]
    async fn test_no_ignore_files() {
        let inner = MemoryFileSource::builder()
            .add("file1.txt", MemoryFsEntry::file("content1"))
            .add("file2.txt", MemoryFsEntry::file("content2"))
            .build();

        let source = IgnoringFileSource::new(inner);

        let mut listing = source.list_directory(Path::new("")).await.unwrap();

        let mut entries = Vec::new();
        while let Some(entry) = listing.next().await.unwrap() {
            entries.push(entry.name().to_string());
        }

        // All files should be present when no ignore files exist
        assert!(entries.contains(&"file1.txt".to_string()));
        assert!(entries.contains(&"file2.txt".to_string()));
    }
}
