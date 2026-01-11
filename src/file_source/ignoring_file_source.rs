use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;

use ignore::gitignore::{Gitignore, GitignoreBuilder};

use crate::file_source::directory_list::{DirectoryList, DirectoryListing};
use crate::file_source::error::{FileSourceError, Result};
use crate::file_source::file_chunks::FileChunks;
use crate::file_source::file_source::FileSource;
use crate::file_source::types::{
    DirEntry, DirectoryListEntry, FileEntry, GetChildEntryFn, GetChunksFn, ListDirectoryFn,
};

/// Context for ignore rules, passed down through directory traversal.
#[derive(Clone)]
struct IgnoreContext {
    /// Compiled gitignore matcher with accumulated rules from root to current path.
    matcher: Arc<Gitignore>,
}

impl IgnoreContext {
    /// Create a new empty context.
    fn empty() -> Self {
        Self {
            matcher: Arc::new(Gitignore::empty()),
        }
    }

    /// Check if an entry should be ignored.
    fn is_ignored(&self, name: &str, path: &Path, is_dir: bool) -> bool {
        // Always ignore .tfs and .git directories
        if is_dir && (name == ".tfs" || name == ".git") {
            return true;
        }

        // Check against gitignore patterns
        self.matcher.matched(path, is_dir).is_ignore()
    }
}

/// Inner state for IgnoringFileSource, wrapped in Arc for sharing with closures.
struct IgnoringFileSourceInner<S: FileSource> {
    inner: S,
}

/// A FileSource wrapper that filters entries based on ignore rules.
///
/// Filters out:
/// - The `.tfs/` and `.git/` directories (always)
/// - Files/directories matching patterns in `.gitignore` files
/// - Files/directories matching patterns in `.tfsignore` files
///
/// When both `.gitignore` and `.tfsignore` are present, they are treated as
/// concatenated with `.gitignore` patterns first.
///
/// Ignore rules are inherited from parent directories, following git semantics.
pub struct IgnoringFileSource<S: FileSource> {
    inner: Arc<IgnoringFileSourceInner<S>>,
}

impl<S: FileSource> IgnoringFileSource<S> {
    /// Create a new IgnoringFileSource wrapping the given source.
    pub fn new(inner: S) -> Self {
        Self {
            inner: Arc::new(IgnoringFileSourceInner { inner }),
        }
    }

    /// Build a Gitignore matcher for the given directory path.
    ///
    /// This loads ignore files from the root down to the target path,
    /// accumulating rules as git does.
    async fn build_ignore_matcher(inner: &IgnoringFileSourceInner<S>, path: &Path) -> Gitignore {
        let mut builder = GitignoreBuilder::new(Path::new(""));

        // Collect all path components from root to target
        let components: Vec<_> = path.components().collect();

        // Start from root and work down, loading ignore files at each level
        let mut current_path = PathBuf::new();

        // Load ignore files from root first
        Self::load_ignore_files(inner, &mut builder, Path::new("")).await;

        // Then load from each directory down to the target
        for component in &components {
            current_path.push(component);
            Self::load_ignore_files(inner, &mut builder, &current_path).await;
        }

        builder.build().unwrap_or_else(|_| Gitignore::empty())
    }

    /// Extend an existing matcher with ignore files from a specific directory.
    async fn extend_ignore_matcher(
        inner: &IgnoringFileSourceInner<S>,
        existing: &Gitignore,
        dir_path: &Path,
    ) -> Gitignore {
        let mut builder = GitignoreBuilder::new(Path::new(""));

        // Add existing patterns first
        // Note: Gitignore doesn't expose its patterns, so we rebuild
        // For a more efficient implementation, we'd cache the pattern strings

        // Load ignore files from just this directory
        Self::load_ignore_files(inner, &mut builder, dir_path).await;

        // If we loaded new patterns, combine with existing
        let new_matcher = builder.build().unwrap_or_else(|_| Gitignore::empty());

        // For simplicity, we rebuild from scratch including parent context
        // A production implementation might want to cache more efficiently
        Self::build_ignore_matcher(inner, dir_path).await
    }

    /// Load .gitignore and .tfsignore files from a directory into the builder.
    async fn load_ignore_files(
        inner: &IgnoringFileSourceInner<S>,
        builder: &mut GitignoreBuilder,
        dir_path: &Path,
    ) {
        // Load .gitignore first
        let gitignore_path = if dir_path.as_os_str().is_empty() {
            PathBuf::from(".gitignore")
        } else {
            dir_path.join(".gitignore")
        };
        Self::load_ignore_file(inner, builder, &gitignore_path).await;

        // Then load .tfsignore (patterns added after .gitignore)
        let tfsignore_path = if dir_path.as_os_str().is_empty() {
            PathBuf::from(".tfsignore")
        } else {
            dir_path.join(".tfsignore")
        };
        Self::load_ignore_file(inner, builder, &tfsignore_path).await;
    }

    /// Load patterns from a single ignore file into the builder.
    async fn load_ignore_file(
        inner: &IgnoringFileSourceInner<S>,
        builder: &mut GitignoreBuilder,
        file_path: &Path,
    ) {
        // Try to read the file
        let chunks_result = inner.inner.get_file_chunks(file_path).await;
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

    /// Create a closure that lists a directory with ignore context.
    fn make_list_fn(
        inner: Arc<IgnoringFileSourceInner<S>>,
        path: PathBuf,
        context: IgnoreContext,
    ) -> ListDirectoryFn
    where
        S: 'static,
    {
        Arc::new(move || {
            let inner = Arc::clone(&inner);
            let path = path.clone();
            let context = context.clone();
            Box::pin(async move { Self::list_directory_internal(inner, &path, context).await })
        })
    }

    /// Create a closure that gets a child entry by name with ignore context.
    fn make_get_child_fn(
        inner: Arc<IgnoringFileSourceInner<S>>,
        path: PathBuf,
        context: IgnoreContext,
    ) -> GetChildEntryFn
    where
        S: 'static,
    {
        Arc::new(move |name: String| {
            let inner = Arc::clone(&inner);
            let context = context.clone();
            let child_path = if path.as_os_str().is_empty() {
                PathBuf::from(&name)
            } else {
                path.join(&name)
            };
            Box::pin(async move { Self::get_entry_internal(inner, &child_path, context).await })
        })
    }

    /// Create a closure that gets file chunks (ignore context not needed for file reading).
    fn make_get_chunks_fn(
        inner: Arc<IgnoringFileSourceInner<S>>,
        path: PathBuf,
    ) -> GetChunksFn
    where
        S: 'static,
    {
        Arc::new(move || {
            let inner = Arc::clone(&inner);
            let path = path.clone();
            Box::pin(async move { inner.inner.get_file_chunks(&path).await })
        })
    }

    /// Internal implementation of list_directory.
    fn list_directory_internal(
        inner: Arc<IgnoringFileSourceInner<S>>,
        path: &Path,
        context: IgnoreContext,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<DirectoryList>> + Send>>
    where
        S: 'static,
    {
        let path = path.to_path_buf();
        Box::pin(async move {
            // Build matcher for this directory (includes patterns from parent dirs)
            let matcher = Self::build_ignore_matcher(&inner, &path).await;
            let context = IgnoreContext {
                matcher: Arc::new(matcher),
            };

            // Get the inner directory listing
            let inner_list = inner.inner.list_directory(&path).await?;

            Ok(DirectoryList::new(IgnoringDirectoryList {
                inner: inner_list,
                source_inner: Arc::clone(&inner),
                context,
            }))
        })
    }

    /// Internal implementation of get_entry.
    fn get_entry_internal(
        inner: Arc<IgnoringFileSourceInner<S>>,
        path: &Path,
        context: IgnoreContext,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<Option<DirectoryListEntry>>> + Send>>
    where
        S: 'static,
    {
        let path = path.to_path_buf();
        Box::pin(async move {
            // First get the entry from the inner source
            let entry = inner.inner.get_entry(&path).await?;

            let Some(entry) = entry else {
                return Ok(None);
            };

            let name = entry.name();
            let is_dir = matches!(entry, DirectoryListEntry::Directory(_));

            // Check if this entry should be ignored
            if context.is_ignored(name, &path, is_dir) {
                return Ok(None);
            }

            // Wrap the entry with our closures
            let wrapped = match entry {
                DirectoryListEntry::Directory(dir) => {
                    let list_fn =
                        Self::make_list_fn(Arc::clone(&inner), dir.path.clone(), context.clone());
                    let get_child_fn =
                        Self::make_get_child_fn(Arc::clone(&inner), dir.path.clone(), context);
                    DirectoryListEntry::Directory(DirEntry::new(
                        dir.name.clone(),
                        dir.path.clone(),
                        list_fn,
                        get_child_fn,
                    ))
                }
                DirectoryListEntry::File(file) => {
                    let get_chunks_fn =
                        Self::make_get_chunks_fn(Arc::clone(&inner), file.path.clone());
                    DirectoryListEntry::File(FileEntry::new(
                        file.name.clone(),
                        file.path.clone(),
                        file.size,
                        file.executable,
                        get_chunks_fn,
                    ))
                }
            };

            Ok(Some(wrapped))
        })
    }
}

impl<S: FileSource + 'static> FileSource for IgnoringFileSource<S> {
    async fn list_directory(&self, path: &Path) -> Result<DirectoryList> {
        Self::list_directory_internal(
            Arc::clone(&self.inner),
            path,
            IgnoreContext::empty(),
        )
        .await
    }

    async fn get_file_chunks(&self, path: &Path) -> Result<FileChunks> {
        // Check if this file should be ignored
        // We need to check against ignore rules from parent directories
        if let Some(parent) = path.parent() {
            let matcher = Self::build_ignore_matcher(&self.inner, parent).await;

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

        self.inner.inner.get_file_chunks(path).await
    }

    async fn get_entry(&self, path: &Path) -> Result<Option<DirectoryListEntry>> {
        // Build context from parent directories
        let context = if let Some(parent) = path.parent() {
            let matcher = Self::build_ignore_matcher(&self.inner, parent).await;
            IgnoreContext {
                matcher: Arc::new(matcher),
            }
        } else {
            IgnoreContext::empty()
        };

        Self::get_entry_internal(Arc::clone(&self.inner), path, context).await
    }
}

/// Filtering directory listing that skips ignored entries.
struct IgnoringDirectoryList<S: FileSource + 'static> {
    inner: DirectoryList,
    source_inner: Arc<IgnoringFileSourceInner<S>>,
    context: IgnoreContext,
}

impl<S: FileSource + 'static> DirectoryListing for IgnoringDirectoryList<S> {
    async fn next(&mut self) -> Result<Option<DirectoryListEntry>> {
        loop {
            match self.inner.next().await? {
                Some(entry) => {
                    let name = entry.name();
                    let path = entry.path();
                    let is_dir = matches!(entry, DirectoryListEntry::Directory(_));

                    // Check if this entry should be ignored
                    if self.context.is_ignored(name, path, is_dir) {
                        continue;
                    }

                    // Wrap the entry with our closures that carry the ignore context
                    let wrapped = match entry {
                        DirectoryListEntry::Directory(dir) => {
                            let list_fn = IgnoringFileSource::make_list_fn(
                                Arc::clone(&self.source_inner),
                                dir.path.clone(),
                                self.context.clone(),
                            );
                            let get_child_fn = IgnoringFileSource::make_get_child_fn(
                                Arc::clone(&self.source_inner),
                                dir.path.clone(),
                                self.context.clone(),
                            );
                            DirectoryListEntry::Directory(DirEntry::new(
                                dir.name.clone(),
                                dir.path.clone(),
                                list_fn,
                                get_child_fn,
                            ))
                        }
                        DirectoryListEntry::File(file) => {
                            let get_chunks_fn = IgnoringFileSource::make_get_chunks_fn(
                                Arc::clone(&self.source_inner),
                                file.path.clone(),
                            );
                            DirectoryListEntry::File(FileEntry::new(
                                file.name.clone(),
                                file.path.clone(),
                                file.size,
                                file.executable,
                                get_chunks_fn,
                            ))
                        }
                    };

                    return Ok(Some(wrapped));
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

    // New tests for entry-based operations with context propagation

    #[tokio::test]
    async fn test_dir_entry_list_directory_inherits_context() {
        let inner = MemoryFileSource::builder()
            .add(".gitignore", MemoryFsEntry::file("*.log\n"))
            .add("subdir/app.log", MemoryFsEntry::file("log"))
            .add("subdir/main.rs", MemoryFsEntry::file("code"))
            .build();

        let source = IgnoringFileSource::new(inner);

        // Get the subdir entry from root listing
        let mut listing = source.list_directory(Path::new("")).await.unwrap();
        let mut subdir_entry = None;
        while let Some(entry) = listing.next().await.unwrap() {
            if entry.name() == "subdir" {
                subdir_entry = Some(entry);
                break;
            }
        }

        let subdir = subdir_entry.expect("subdir should exist");
        if let DirectoryListEntry::Directory(dir) = subdir {
            // Use the entry's list_directory method - should inherit ignore context
            let mut sublist = dir.list_directory().await.unwrap();

            let mut entries = Vec::new();
            while let Some(entry) = sublist.next().await.unwrap() {
                entries.push(entry.name().to_string());
            }

            // *.log should still be ignored from inherited root .gitignore
            assert!(!entries.contains(&"app.log".to_string()));
            assert!(entries.contains(&"main.rs".to_string()));
        } else {
            panic!("expected directory");
        }
    }

    #[tokio::test]
    async fn test_dir_entry_get_entry_respects_ignore() {
        let inner = MemoryFileSource::builder()
            .add(".gitignore", MemoryFsEntry::file("secret.txt\n"))
            .add("subdir/secret.txt", MemoryFsEntry::file("secret"))
            .add("subdir/public.txt", MemoryFsEntry::file("public"))
            .build();

        let source = IgnoringFileSource::new(inner);

        // Get the subdir entry
        let entry = source.get_entry(Path::new("subdir")).await.unwrap().unwrap();
        if let DirectoryListEntry::Directory(dir) = entry {
            // Ignored file should return None
            let secret = dir.get_entry("secret.txt").await.unwrap();
            assert!(secret.is_none());

            // Non-ignored file should return Some
            let public = dir.get_entry("public.txt").await.unwrap();
            assert!(public.is_some());
        } else {
            panic!("expected directory");
        }
    }

    #[tokio::test]
    async fn test_file_entry_get_chunks() {
        let inner = MemoryFileSource::builder()
            .add("file.txt", MemoryFsEntry::file("hello world"))
            .build();

        let source = IgnoringFileSource::new(inner);

        // Get the file entry
        let entry = source.get_entry(Path::new("file.txt")).await.unwrap().unwrap();
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
    async fn test_traversal_with_ignore_context() {
        let inner = MemoryFileSource::builder()
            .add(".gitignore", MemoryFsEntry::file("*.secret\n"))
            .add("dir/subdir/file.txt", MemoryFsEntry::file("content"))
            .add("dir/subdir/data.secret", MemoryFsEntry::file("secret"))
            .build();

        let source = IgnoringFileSource::new(inner);

        // Start by listing root - find the "dir" entry (skip .gitignore)
        let mut root_list = source.list_directory(Path::new("")).await.unwrap();
        let mut dir_entry = None;
        while let Some(entry) = root_list.next().await.unwrap() {
            if entry.name() == "dir" {
                dir_entry = Some(entry);
                break;
            }
        }
        let dir_entry = dir_entry.expect("dir should exist");

        if let DirectoryListEntry::Directory(dir) = dir_entry {
            let mut dir_list = dir.list_directory().await.unwrap();
            let subdir_entry = dir_list.next().await.unwrap().unwrap();

            if let DirectoryListEntry::Directory(subdir) = subdir_entry {
                // List subdir - secret files should be filtered
                let mut subdir_list = subdir.list_directory().await.unwrap();

                let mut entries = Vec::new();
                while let Some(entry) = subdir_list.next().await.unwrap() {
                    entries.push(entry.name().to_string());
                }

                assert!(entries.contains(&"file.txt".to_string()));
                assert!(!entries.contains(&"data.secret".to_string()));

                // Get specific entry - secret should be filtered
                let secret = subdir.get_entry("data.secret").await.unwrap();
                assert!(secret.is_none());

                // Get non-secret file
                let file = subdir.get_entry("file.txt").await.unwrap().unwrap();
                if let DirectoryListEntry::File(f) = file {
                    let mut chunks = f.get_chunks().await.unwrap();
                    let chunk = chunks.next().await.unwrap().unwrap();
                    assert_eq!(chunk.data(), b"content");
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
