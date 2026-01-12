//! Helper for applying ignore rules during file scanning.
//!
//! The ScanIgnoreHelper follows along with a scan operation, loading
//! .gitignore and .tfsignore files as directories are entered, and
//! provides a method to check if entries should be ignored.

use crate::file_store::{DirectoryScanEvent, FileSource};
use ignore::gitignore::{Gitignore, GitignoreBuilder};
use std::path::Path;

/// Directories that are always ignored regardless of ignore files.
const ALWAYS_IGNORED_DIRS: &[&str] = &[".git", ".tfs"];

/// Helper for applying ignore rules during file scanning.
///
/// This helper maintains a stack of ignore matchers as directories are
/// entered and exited. It loads .gitignore and .tfsignore files from
/// each directory and applies their rules cumulatively.
pub struct ScanIgnoreHelper {
    /// Stack of ignore matchers, one per directory level.
    /// Each entry contains the gitignore matcher for that directory.
    stack: Vec<Option<Gitignore>>,
}

impl ScanIgnoreHelper {
    /// Create a new ScanIgnoreHelper.
    pub fn new() -> Self {
        Self { stack: Vec::new() }
    }

    /// Process a scan event, updating internal state.
    ///
    /// Call this for each EnterDirectory and ExitDirectory event during scanning.
    /// When entering a directory, this loads any .gitignore or .tfsignore files.
    pub async fn on_scan_event<S: FileSource + ?Sized>(
        &mut self,
        event: &DirectoryScanEvent,
        source: &S,
    ) {
        match event {
            DirectoryScanEvent::EnterDirectory(dir) => {
                let matcher = self.load_ignore_files(source, &dir.path).await;
                self.stack.push(matcher);
            }
            DirectoryScanEvent::ExitDirectory => {
                self.stack.pop();
            }
        }
    }

    /// Check if a name should be ignored.
    ///
    /// The `is_dir` parameter indicates whether the entry is a directory,
    /// which affects pattern matching (e.g., patterns ending with `/`).
    pub fn should_ignore(&self, name: &str, is_dir: bool) -> bool {
        // Always ignore .git and .tfs directories
        if is_dir && ALWAYS_IGNORED_DIRS.contains(&name) {
            return true;
        }

        // Check against all active ignore matchers (from root to current)
        for matcher in self.stack.iter().flatten() {
            let matched = matcher.matched(Path::new(name), is_dir);
            if matched.is_ignore() {
                return true;
            }
            if matched.is_whitelist() {
                return false;
            }
        }

        false
    }

    /// Load .gitignore and .tfsignore files from a directory.
    async fn load_ignore_files<S: FileSource + ?Sized>(
        &self,
        source: &S,
        dir_path: &str,
    ) -> Option<Gitignore> {
        let mut builder = GitignoreBuilder::new(dir_path);
        let mut has_patterns = false;

        // Load .gitignore first (lower priority)
        let gitignore_path = if dir_path.is_empty() {
            ".gitignore".to_string()
        } else {
            format!("{}/{}", dir_path, ".gitignore")
        };

        if let Ok(contents) = source.get_file(Path::new(&gitignore_path)).await
            && let Ok(text) = std::str::from_utf8(&contents)
        {
            for line in text.lines() {
                // GitignoreBuilder::add handles comments and blank lines
                let _ = builder.add_line(None, line);
            }
            has_patterns = true;
        }

        // Load .tfsignore second (higher priority, appended after .gitignore)
        let tfsignore_path = if dir_path.is_empty() {
            ".tfsignore".to_string()
        } else {
            format!("{}/{}", dir_path, ".tfsignore")
        };

        if let Ok(contents) = source.get_file(Path::new(&tfsignore_path)).await
            && let Ok(text) = std::str::from_utf8(&contents)
        {
            for line in text.lines() {
                let _ = builder.add_line(None, line);
            }
            has_patterns = true;
        }

        if has_patterns {
            builder.build().ok()
        } else {
            None
        }
    }
}

impl Default for ScanIgnoreHelper {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::file_store::{DirEntry, MemoryFileStore, MemoryFsEntry};

    #[tokio::test]
    async fn test_always_ignores_git_and_tfs() {
        let helper = ScanIgnoreHelper::new();

        assert!(helper.should_ignore(".git", true));
        assert!(helper.should_ignore(".tfs", true));

        // But not if they're files
        assert!(!helper.should_ignore(".git", false));
        assert!(!helper.should_ignore(".tfs", false));
    }

    #[tokio::test]
    async fn test_no_ignore_without_files() {
        let helper = ScanIgnoreHelper::new();

        assert!(!helper.should_ignore("file.txt", false));
        assert!(!helper.should_ignore("dir", true));
    }

    #[tokio::test]
    async fn test_gitignore_patterns() {
        let store = MemoryFileStore::builder()
            .add(".gitignore", MemoryFsEntry::file("*.log\nbuild/"))
            .add("app.log", MemoryFsEntry::file("log content"))
            .add("main.rs", MemoryFsEntry::file("fn main() {}"))
            .build();

        let mut helper = ScanIgnoreHelper::new();

        // Enter root directory
        helper
            .on_scan_event(
                &DirectoryScanEvent::EnterDirectory(DirEntry {
                    name: String::new(),
                    path: String::new(),
                }),
                &store,
            )
            .await;

        assert!(helper.should_ignore("app.log", false));
        assert!(helper.should_ignore("build", true));
        assert!(!helper.should_ignore("main.rs", false));
    }

    #[tokio::test]
    async fn test_tfsignore_patterns() {
        let store = MemoryFileStore::builder()
            .add(".tfsignore", MemoryFsEntry::file("*.tmp"))
            .add("data.tmp", MemoryFsEntry::file("temp"))
            .build();

        let mut helper = ScanIgnoreHelper::new();

        helper
            .on_scan_event(
                &DirectoryScanEvent::EnterDirectory(DirEntry {
                    name: String::new(),
                    path: String::new(),
                }),
                &store,
            )
            .await;

        assert!(helper.should_ignore("data.tmp", false));
    }

    #[tokio::test]
    async fn test_combined_gitignore_and_tfsignore() {
        let store = MemoryFileStore::builder()
            .add(".gitignore", MemoryFsEntry::file("*.log"))
            .add(".tfsignore", MemoryFsEntry::file("*.tmp"))
            .build();

        let mut helper = ScanIgnoreHelper::new();

        helper
            .on_scan_event(
                &DirectoryScanEvent::EnterDirectory(DirEntry {
                    name: String::new(),
                    path: String::new(),
                }),
                &store,
            )
            .await;

        // Both patterns should work
        assert!(helper.should_ignore("app.log", false));
        assert!(helper.should_ignore("data.tmp", false));
        assert!(!helper.should_ignore("main.rs", false));
    }

    #[tokio::test]
    async fn test_nested_directory_ignore() {
        let store = MemoryFileStore::builder()
            .add(".gitignore", MemoryFsEntry::file("*.log"))
            .add("src/.gitignore", MemoryFsEntry::file("*.bak"))
            .build();

        let mut helper = ScanIgnoreHelper::new();

        // Enter root
        helper
            .on_scan_event(
                &DirectoryScanEvent::EnterDirectory(DirEntry {
                    name: String::new(),
                    path: String::new(),
                }),
                &store,
            )
            .await;

        assert!(helper.should_ignore("app.log", false));
        assert!(!helper.should_ignore("file.bak", false));

        // Enter src/
        helper
            .on_scan_event(
                &DirectoryScanEvent::EnterDirectory(DirEntry {
                    name: "src".to_string(),
                    path: "src".to_string(),
                }),
                &store,
            )
            .await;

        // Both root and src patterns should apply
        assert!(helper.should_ignore("app.log", false));
        assert!(helper.should_ignore("file.bak", false));

        // Exit src/
        helper
            .on_scan_event(&DirectoryScanEvent::ExitDirectory, &store)
            .await;

        // Back to root - only root patterns
        assert!(helper.should_ignore("app.log", false));
        assert!(!helper.should_ignore("file.bak", false));
    }

    #[tokio::test]
    async fn test_negation_pattern() {
        let store = MemoryFileStore::builder()
            .add(".gitignore", MemoryFsEntry::file("*.log\n!important.log"))
            .build();

        let mut helper = ScanIgnoreHelper::new();

        helper
            .on_scan_event(
                &DirectoryScanEvent::EnterDirectory(DirEntry {
                    name: String::new(),
                    path: String::new(),
                }),
                &store,
            )
            .await;

        assert!(helper.should_ignore("debug.log", false));
        assert!(!helper.should_ignore("important.log", false));
    }
}
