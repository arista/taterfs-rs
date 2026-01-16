//! Helper for applying ignore rules during file scanning.
//!
//! The ScanIgnoreHelper follows along with a scan operation, loading
//! .gitignore and .tfsignore files as directories are entered, and
//! provides a method to check if entries should be ignored.

use crate::file_store::{DirectoryScanEvent, FileSource};
use ignore::gitignore::{Gitignore, GitignoreBuilder};
use std::path::Path;

/// Default global ignore patterns if none are configured.
const DEFAULT_GLOBAL_IGNORES: &[&str] = &[".git/", ".tfs/"];

/// Helper for applying ignore rules during file scanning.
///
/// This helper maintains a stack of ignore matchers as directories are
/// entered and exited. It loads .gitignore and .tfsignore files from
/// each directory and applies their rules cumulatively.
pub struct ScanIgnoreHelper {
    /// Matcher for global ignore patterns (from config).
    global_matcher: Option<Gitignore>,
    /// Stack of ignore matchers, one per directory level.
    /// Each entry contains the gitignore matcher for that directory.
    stack: Vec<Option<Gitignore>>,
}

impl ScanIgnoreHelper {
    /// Create a new ScanIgnoreHelper with default global ignores.
    pub fn new() -> Self {
        Self::with_global_ignores(
            DEFAULT_GLOBAL_IGNORES
                .iter()
                .map(|s| s.to_string())
                .collect(),
        )
    }

    /// Create a new ScanIgnoreHelper with the specified global ignore patterns.
    ///
    /// The patterns are interpreted as gitignore patterns. Common patterns:
    /// - `.git/` - ignore the .git directory
    /// - `.tfs/` - ignore the .tfs directory
    /// - `*.log` - ignore all .log files
    pub fn with_global_ignores(patterns: Vec<String>) -> Self {
        let global_matcher = if patterns.is_empty() {
            None
        } else {
            let mut builder = GitignoreBuilder::new("");
            for pattern in &patterns {
                let _ = builder.add_line(None, pattern);
            }
            builder.build().ok()
        };
        Self {
            global_matcher,
            stack: Vec::new(),
        }
    }

    /// Initialize the helper by walking from root to the given path.
    ///
    /// This loads ignore files from the root directory and each directory
    /// along the path to the target. Call this before scanning from a
    /// subdirectory to ensure parent ignore rules are applied.
    ///
    /// If `path` is `None` or empty, only the root directory's ignore files
    /// are loaded.
    pub async fn initialize_to_path<S: FileSource + ?Sized>(
        &mut self,
        path: Option<&Path>,
        source: &S,
    ) {
        use crate::file_store::DirEntry;

        // Always enter root first
        let root_entry = DirEntry {
            name: String::new(),
            path: String::new(),
        };
        self.on_scan_event(&DirectoryScanEvent::EnterDirectory(root_entry), source)
            .await;

        // If there's a path, walk through each component
        if let Some(p) = path {
            let mut accumulated = String::new();
            for component in p.components() {
                if let std::path::Component::Normal(name) = component {
                    let name_str = name.to_string_lossy().into_owned();
                    if accumulated.is_empty() {
                        accumulated = name_str.clone();
                    } else {
                        accumulated = format!("{}/{}", accumulated, name_str);
                    }
                    let dir_entry = DirEntry {
                        name: name_str,
                        path: accumulated.clone(),
                    };
                    self.on_scan_event(&DirectoryScanEvent::EnterDirectory(dir_entry), source)
                        .await;
                }
            }
        }
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
        // Check global ignore patterns first
        if let Some(ref matcher) = self.global_matcher {
            let matched = matcher.matched(Path::new(name), is_dir);
            if matched.is_ignore() {
                return true;
            }
            if matched.is_whitelist() {
                return false;
            }
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

    #[tokio::test]
    async fn test_custom_global_ignores() {
        // Custom ignores that don't include .git or .tfs
        let helper = ScanIgnoreHelper::with_global_ignores(vec![
            "node_modules/".to_string(),
            "target/".to_string(),
        ]);

        // These should be ignored with custom patterns
        assert!(helper.should_ignore("node_modules", true));
        assert!(helper.should_ignore("target", true));

        // .git and .tfs should NOT be ignored with custom patterns
        assert!(!helper.should_ignore(".git", true));
        assert!(!helper.should_ignore(".tfs", true));
    }

    #[tokio::test]
    async fn test_empty_global_ignores() {
        // Empty global ignores - nothing ignored by default
        let helper = ScanIgnoreHelper::with_global_ignores(vec![]);

        assert!(!helper.should_ignore(".git", true));
        assert!(!helper.should_ignore(".tfs", true));
        assert!(!helper.should_ignore("node_modules", true));
    }

    #[tokio::test]
    async fn test_global_ignores_with_file_patterns() {
        let helper =
            ScanIgnoreHelper::with_global_ignores(vec![".git/".to_string(), "*.bak".to_string()]);

        assert!(helper.should_ignore(".git", true));
        assert!(helper.should_ignore("file.bak", false));
        assert!(!helper.should_ignore("file.txt", false));
    }

    #[tokio::test]
    async fn test_initialize_to_path_loads_parent_ignores() {
        // Create a store with ignore files at root and in a subdirectory
        let store = MemoryFileStore::builder()
            .add(".gitignore", MemoryFsEntry::file("*.log"))
            .add("foo/.gitignore", MemoryFsEntry::file("*.tmp"))
            .add("foo/bar/.gitignore", MemoryFsEntry::file("*.bak"))
            .add("foo/bar/file.txt", MemoryFsEntry::file("content"))
            .build();

        let mut helper = ScanIgnoreHelper::new();

        // Initialize to foo/bar - should load ignores from root, foo, and foo/bar
        helper
            .initialize_to_path(Some(Path::new("foo/bar")), &store)
            .await;

        // All three ignore patterns should be active
        assert!(helper.should_ignore("debug.log", false)); // from root
        assert!(helper.should_ignore("cache.tmp", false)); // from foo
        assert!(helper.should_ignore("backup.bak", false)); // from foo/bar
        assert!(!helper.should_ignore("file.txt", false)); // not ignored
    }

    #[tokio::test]
    async fn test_initialize_to_path_none_loads_root_only() {
        let store = MemoryFileStore::builder()
            .add(".gitignore", MemoryFsEntry::file("*.log"))
            .add("foo/.gitignore", MemoryFsEntry::file("*.tmp"))
            .build();

        let mut helper = ScanIgnoreHelper::new();

        // Initialize with None - should only load root ignores
        helper.initialize_to_path(None, &store).await;

        assert!(helper.should_ignore("debug.log", false)); // from root
        assert!(!helper.should_ignore("cache.tmp", false)); // foo's ignore not loaded
    }

    #[tokio::test]
    async fn test_initialize_to_path_empty_loads_root_only() {
        let store = MemoryFileStore::builder()
            .add(".gitignore", MemoryFsEntry::file("*.log"))
            .add("foo/.gitignore", MemoryFsEntry::file("*.tmp"))
            .build();

        let mut helper = ScanIgnoreHelper::new();

        // Initialize with empty path - should only load root ignores
        helper.initialize_to_path(Some(Path::new("")), &store).await;

        assert!(helper.should_ignore("debug.log", false)); // from root
        assert!(!helper.should_ignore("cache.tmp", false)); // foo's ignore not loaded
    }
}
