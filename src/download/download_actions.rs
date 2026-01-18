//! Download actions for synchronizing a repository directory to a file store.
//!
//! This module provides the [`download_actions`] function which generates a sequence
//! of [`DownloadAction`]s needed to make a file store location match a repository
//! directory's contents.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use futures::StreamExt;

use crate::file_store::{FileSource, ScanEvent, ScanEvents};
use crate::repo::{DirectoryScan, Repo, RepoScanEvent};
use crate::repository::ObjectId;

/// Result type for download operations.
pub type Result<T> = std::result::Result<T, DownloadError>;

/// Errors that can occur during download operations.
#[derive(Debug, Clone)]
pub enum DownloadError {
    /// Repository error.
    Repo(String),
    /// File store error.
    FileStore(String),
}

impl std::fmt::Display for DownloadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DownloadError::Repo(msg) => write!(f, "repository error: {}", msg),
            DownloadError::FileStore(msg) => write!(f, "file store error: {}", msg),
        }
    }
}

impl std::error::Error for DownloadError {}

impl From<crate::repo::RepoError> for DownloadError {
    fn from(e: crate::repo::RepoError) -> Self {
        DownloadError::Repo(e.to_string())
    }
}

impl From<crate::file_store::Error> for DownloadError {
    fn from(e: crate::file_store::Error) -> Self {
        DownloadError::FileStore(e.to_string())
    }
}

/// An action needed to synchronize a file store with a repository directory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DownloadAction {
    /// Create a new directory.
    CreateDirectory(String),
    /// Remove a directory (and all its contents).
    RemoveDirectory(String),
    /// Remove a file.
    RemoveFile(String),
    /// Enter a directory (both repo and file store have this directory).
    EnterDirectory(String),
    /// Exit the current directory.
    ExitDirectory,
    /// Download a file from the repository.
    DownloadFile(String, ObjectId),
}

/// Pending entry from one of the scan streams.
#[derive(Debug, Clone)]
enum PendingEntry {
    /// A directory entry with its name.
    Directory(String, ObjectId),
    /// A file entry with its name and object ID.
    File(String, ObjectId),
}

/// Pending entry from the file store scan.
#[derive(Debug, Clone)]
enum FsPendingEntry {
    /// A directory entry with its name.
    Directory(String),
    /// A file entry with its name.
    File(String),
}

/// State machine for the download actions iterator.
enum State {
    /// Processing entries at the current directory level.
    Processing,
    /// Need to recurse into a directory that exists in both repo and file store.
    EnterBoth {
        name: String,
        repo_dir_id: ObjectId,
    },
    /// Need to recurse into a directory that only exists in repo.
    EnterRepoOnly {
        name: String,
        repo_dir_id: ObjectId,
    },
    /// Need to remove remaining file store entries then exit.
    DrainFileStore,
    /// Need to add remaining repo entries then exit.
    DrainRepo,
    /// Done with this level, emit ExitDirectory.
    ExitPending,
}

/// Frame in the directory stack for nested traversal.
struct StackFrame {
    /// Repo scan iterator for this directory level.
    repo_scan: DirectoryScan,
    /// File store scan iterator for this directory level.
    fs_scan: ScanEvents,
    /// Pending repo entry (already read but not yet processed).
    pending_repo: Option<PendingEntry>,
    /// Pending file store entry (already read but not yet processed).
    pending_fs: Option<FsPendingEntry>,
    /// Current state.
    state: State,
}

/// Iterator that generates download actions by comparing repo and file store contents.
///
/// This struct "zippers" together a repository directory scan and a file store scan,
/// generating the sequence of actions needed to make the file store match the repository.
pub struct DownloadActions {
    /// Reference to the repository.
    repo: Arc<Repo>,
    /// Reference to the file source (for scanning).
    source: Arc<dyn FileSource>,
    /// Base path in the file store.
    base_path: PathBuf,
    /// Stack of directory frames for nested traversal.
    stack: Vec<StackFrame>,
    /// Whether we've started processing (emitted initial EnterDirectory for root).
    started: bool,
    /// Root directory ID in the repository.
    root_dir_id: ObjectId,
}

impl DownloadActions {
    /// Create a new download actions iterator.
    fn new(
        repo: Arc<Repo>,
        source: Arc<dyn FileSource>,
        root_dir_id: ObjectId,
        base_path: PathBuf,
    ) -> Self {
        Self {
            repo,
            source,
            base_path,
            stack: Vec::new(),
            started: false,
            root_dir_id,
        }
    }

    /// Get the next download action.
    ///
    /// Returns `None` when all actions have been generated.
    pub async fn next(&mut self) -> Result<Option<DownloadAction>> {
        // Initialize on first call
        if !self.started {
            self.started = true;
            return self.initialize().await;
        }

        // Process from the current stack frame
        let Some(frame) = self.stack.last_mut() else {
            return Ok(None);
        };

        match &frame.state {
            State::Processing => self.process_current_level().await,
            State::EnterBoth { .. } => {
                // Extract values before modifying frame
                let (name, repo_dir_id) = {
                    let frame = self.stack.last().unwrap();
                    match &frame.state {
                        State::EnterBoth { name, repo_dir_id } => {
                            (name.clone(), repo_dir_id.clone())
                        }
                        _ => unreachable!(),
                    }
                };

                // Update state to Processing before recursing
                self.stack.last_mut().unwrap().state = State::Processing;

                // Start new scan for the subdirectory
                self.enter_directory_both(&name, &repo_dir_id).await
            }
            State::EnterRepoOnly { .. } => {
                // Extract values before modifying frame
                let (name, repo_dir_id) = {
                    let frame = self.stack.last().unwrap();
                    match &frame.state {
                        State::EnterRepoOnly { name, repo_dir_id } => {
                            (name.clone(), repo_dir_id.clone())
                        }
                        _ => unreachable!(),
                    }
                };

                // Update state to Processing before recursing
                self.stack.last_mut().unwrap().state = State::Processing;

                // Start new scan for repo-only directory
                self.enter_directory_repo_only(&name, &repo_dir_id).await
            }
            State::DrainFileStore => self.drain_file_store().await,
            State::DrainRepo => self.drain_repo().await,
            State::ExitPending => {
                self.stack.pop();
                Ok(Some(DownloadAction::ExitDirectory))
            }
        }
    }

    /// Initialize the iterator by setting up the root directory scans.
    async fn initialize(&mut self) -> Result<Option<DownloadAction>> {
        // Start repo scan
        let repo_scan = self.repo.scan_directory(&self.root_dir_id);

        // Start file store scan
        let fs_scan = self.source.scan(Some(&self.base_path)).await;

        // Handle case where file store path doesn't exist
        let fs_scan = match fs_scan {
            Ok(scan) => scan,
            Err(crate::file_store::Error::NotFound(_)) => {
                // Path doesn't exist - create empty scan
                Box::pin(futures::stream::empty())
            }
            Err(e) => return Err(e.into()),
        };

        self.stack.push(StackFrame {
            repo_scan,
            fs_scan,
            pending_repo: None,
            pending_fs: None,
            state: State::Processing,
        });

        // Skip the root EnterDirectory events from both scans
        self.skip_root_enter_events().await?;

        // Now process entries at the root level
        self.process_current_level().await
    }

    /// Skip the initial EnterDirectory events from both scans (for the root).
    async fn skip_root_enter_events(&mut self) -> Result<()> {
        // Skip repo's root EnterDirectory
        let pending_repo = {
            let frame = self.stack.last_mut().unwrap();
            match frame.repo_scan.next().await? {
                Some(RepoScanEvent::EnterDirectory(_)) => None,
                Some(other) => Self::repo_event_to_pending(other),
                None => None,
            }
        };
        if pending_repo.is_some() {
            self.stack.last_mut().unwrap().pending_repo = pending_repo;
        }

        // Skip file store's root EnterDirectory
        let pending_fs = {
            let frame = self.stack.last_mut().unwrap();
            match frame.fs_scan.next().await {
                Some(Ok(ScanEvent::EnterDirectory(_))) => None,
                Some(Ok(other)) => Self::fs_event_to_pending(other),
                Some(Err(e)) => return Err(e.into()),
                None => None,
            }
        };
        if pending_fs.is_some() {
            self.stack.last_mut().unwrap().pending_fs = pending_fs;
        }

        Ok(())
    }

    /// Convert a repo scan event to a pending entry.
    fn repo_event_to_pending(event: RepoScanEvent) -> Option<PendingEntry> {
        match event {
            RepoScanEvent::EnterDirectory(d) => Some(PendingEntry::Directory(d.name, d.directory)),
            RepoScanEvent::File(f) => Some(PendingEntry::File(f.name, f.file)),
            RepoScanEvent::ExitDirectory => None,
        }
    }

    /// Convert a file store scan event to a pending entry.
    fn fs_event_to_pending(event: ScanEvent) -> Option<FsPendingEntry> {
        match event {
            ScanEvent::EnterDirectory(d) => Some(FsPendingEntry::Directory(d.name)),
            ScanEvent::File(f) => Some(FsPendingEntry::File(f.name)),
            ScanEvent::ExitDirectory => None,
        }
    }

    /// Get the next repo entry (from pending or by reading).
    async fn next_repo_entry(&mut self) -> Result<Option<PendingEntry>> {
        let frame = self.stack.last_mut().unwrap();

        // Return pending entry if we have one
        if let Some(entry) = frame.pending_repo.take() {
            return Ok(Some(entry));
        }

        // Read next event from repo scan
        match frame.repo_scan.next().await? {
            Some(RepoScanEvent::EnterDirectory(d)) => {
                Ok(Some(PendingEntry::Directory(d.name, d.directory)))
            }
            Some(RepoScanEvent::File(f)) => Ok(Some(PendingEntry::File(f.name, f.file))),
            Some(RepoScanEvent::ExitDirectory) | None => Ok(None),
        }
    }

    /// Get the next file store entry (from pending or by reading).
    async fn next_fs_entry(&mut self) -> Result<Option<FsPendingEntry>> {
        let frame = self.stack.last_mut().unwrap();

        // Return pending entry if we have one
        if let Some(entry) = frame.pending_fs.take() {
            return Ok(Some(entry));
        }

        // Read next event from file store scan
        match frame.fs_scan.next().await {
            Some(Ok(ScanEvent::EnterDirectory(d))) => Ok(Some(FsPendingEntry::Directory(d.name))),
            Some(Ok(ScanEvent::File(f))) => Ok(Some(FsPendingEntry::File(f.name))),
            Some(Ok(ScanEvent::ExitDirectory)) | None => Ok(None),
            Some(Err(e)) => Err(e.into()),
        }
    }

    /// Process entries at the current directory level using the zipper algorithm.
    async fn process_current_level(&mut self) -> Result<Option<DownloadAction>> {
        // Get next entries from both streams
        let repo_entry = self.next_repo_entry().await?;
        let fs_entry = self.next_fs_entry().await?;

        match (repo_entry, fs_entry) {
            // Both exhausted - exit this directory
            (None, None) => {
                self.stack.last_mut().unwrap().state = State::ExitPending;
                Ok(Some(DownloadAction::ExitDirectory))
            }

            // Only repo has entries - add them
            (Some(entry), None) => {
                self.stack.last_mut().unwrap().pending_repo = Some(entry);
                self.stack.last_mut().unwrap().state = State::DrainRepo;
                self.drain_repo().await
            }

            // Only file store has entries - remove them
            (None, Some(entry)) => {
                self.stack.last_mut().unwrap().pending_fs = Some(entry);
                self.stack.last_mut().unwrap().state = State::DrainFileStore;
                self.drain_file_store().await
            }

            // Both have entries - compare by name
            (Some(repo_entry), Some(fs_entry)) => {
                let repo_name = match &repo_entry {
                    PendingEntry::Directory(name, _) | PendingEntry::File(name, _) => name.as_str(),
                };
                let fs_name = match &fs_entry {
                    FsPendingEntry::Directory(name) | FsPendingEntry::File(name) => name.as_str(),
                };

                match repo_name.cmp(fs_name) {
                    // Repo entry comes first - add it
                    std::cmp::Ordering::Less => {
                        // Put fs_entry back as pending
                        self.stack.last_mut().unwrap().pending_fs = Some(fs_entry);
                        self.add_repo_entry(repo_entry).await
                    }

                    // File store entry comes first - remove it
                    std::cmp::Ordering::Greater => {
                        // Put repo_entry back as pending
                        self.stack.last_mut().unwrap().pending_repo = Some(repo_entry);
                        self.remove_fs_entry(fs_entry).await
                    }

                    // Same name - handle based on types
                    std::cmp::Ordering::Equal => {
                        self.handle_matching_names(repo_entry, fs_entry).await
                    }
                }
            }
        }
    }

    /// Add a repo entry that doesn't exist in the file store.
    async fn add_repo_entry(&mut self, entry: PendingEntry) -> Result<Option<DownloadAction>> {
        match entry {
            PendingEntry::Directory(name, dir_id) => {
                // Create directory and schedule entering it
                self.stack.last_mut().unwrap().state = State::EnterRepoOnly {
                    name: name.clone(),
                    repo_dir_id: dir_id,
                };
                Ok(Some(DownloadAction::CreateDirectory(name)))
            }
            PendingEntry::File(name, file_id) => Ok(Some(DownloadAction::DownloadFile(name, file_id))),
        }
    }

    /// Remove a file store entry that doesn't exist in the repo.
    async fn remove_fs_entry(&mut self, entry: FsPendingEntry) -> Result<Option<DownloadAction>> {
        match entry {
            FsPendingEntry::Directory(name) => {
                // Need to skip the contents of this directory in the file store scan
                self.skip_fs_directory_contents().await?;
                Ok(Some(DownloadAction::RemoveDirectory(name)))
            }
            FsPendingEntry::File(name) => Ok(Some(DownloadAction::RemoveFile(name))),
        }
    }

    /// Skip all contents of a directory in the file store scan.
    async fn skip_fs_directory_contents(&mut self) -> Result<()> {
        let frame = self.stack.last_mut().unwrap();
        let mut depth = 1;

        while depth > 0 {
            match frame.fs_scan.next().await {
                Some(Ok(ScanEvent::EnterDirectory(_))) => {
                    depth += 1;
                }
                Some(Ok(ScanEvent::ExitDirectory)) => {
                    depth -= 1;
                }
                Some(Ok(ScanEvent::File(_))) => {
                    // Skip files
                }
                Some(Err(e)) => {
                    return Err(e.into());
                }
                None => {
                    // Unexpected end of stream
                    break;
                }
            }
        }

        Ok(())
    }

    /// Handle entries with matching names.
    async fn handle_matching_names(
        &mut self,
        repo_entry: PendingEntry,
        fs_entry: FsPendingEntry,
    ) -> Result<Option<DownloadAction>> {
        match (repo_entry, fs_entry) {
            // Both are directories - enter both
            (PendingEntry::Directory(name, dir_id), FsPendingEntry::Directory(_)) => {
                self.stack.last_mut().unwrap().state = State::EnterBoth {
                    name: name.clone(),
                    repo_dir_id: dir_id,
                };
                Ok(Some(DownloadAction::EnterDirectory(name)))
            }

            // Both are files - download (replace) the file
            (PendingEntry::File(name, file_id), FsPendingEntry::File(_)) => {
                // For now, always download. A smarter implementation could check
                // if the file already matches using fingerprints/hashes.
                Ok(Some(DownloadAction::DownloadFile(name, file_id)))
            }

            // Repo has directory, file store has file - remove file, create directory
            (PendingEntry::Directory(name, dir_id), FsPendingEntry::File(_)) => {
                // Remove the file first, then create directory
                // We need to emit RemoveFile, then CreateDirectory
                // For simplicity, emit RemoveFile now and handle CreateDirectory in next call
                self.stack.last_mut().unwrap().pending_repo =
                    Some(PendingEntry::Directory(name.clone(), dir_id));
                self.stack.last_mut().unwrap().state = State::Processing;
                Ok(Some(DownloadAction::RemoveFile(name)))
            }

            // Repo has file, file store has directory - remove directory, download file
            (PendingEntry::File(name, file_id), FsPendingEntry::Directory(_)) => {
                // Skip the directory contents first
                self.skip_fs_directory_contents().await?;
                // Remove the directory, then download file
                // Emit RemoveDirectory now, put the file back as pending
                self.stack.last_mut().unwrap().pending_repo =
                    Some(PendingEntry::File(name.clone(), file_id));
                Ok(Some(DownloadAction::RemoveDirectory(name)))
            }
        }
    }

    /// Drain remaining file store entries (remove them all).
    async fn drain_file_store(&mut self) -> Result<Option<DownloadAction>> {
        if let Some(entry) = self.next_fs_entry().await? {
            return self.remove_fs_entry(entry).await;
        }

        // No more file store entries - check if repo has more
        if let Some(entry) = self.next_repo_entry().await? {
            self.stack.last_mut().unwrap().pending_repo = Some(entry);
            self.stack.last_mut().unwrap().state = State::DrainRepo;
            return self.drain_repo().await;
        }

        // Both exhausted - exit
        self.stack.last_mut().unwrap().state = State::ExitPending;
        Ok(Some(DownloadAction::ExitDirectory))
    }

    /// Drain remaining repo entries (add them all).
    async fn drain_repo(&mut self) -> Result<Option<DownloadAction>> {
        if let Some(entry) = self.next_repo_entry().await? {
            return self.add_repo_entry(entry).await;
        }

        // No more repo entries - exit
        self.stack.last_mut().unwrap().state = State::ExitPending;
        Ok(Some(DownloadAction::ExitDirectory))
    }

    /// Enter a directory that exists in both repo and file store.
    async fn enter_directory_both(
        &mut self,
        _name: &str,
        repo_dir_id: &ObjectId,
    ) -> Result<Option<DownloadAction>> {
        // The EnterDirectory action was already emitted
        // Now we need to process the subdirectory contents

        // The repo scan already entered this directory (we got the EnterDirectory event)
        // But we need a new scan for the subdirectory
        let repo_scan = self.repo.scan_directory(repo_dir_id);

        // The file store scan also already entered this directory
        // We continue with the existing fs_scan, but we need to track that we're
        // at a new level

        // For the file store, we already consumed the EnterDirectory event
        // The subsequent events will be for this subdirectory until ExitDirectory

        // We need to split the current fs_scan - take events until ExitDirectory
        // This is tricky because we can't split a stream easily

        // Alternative approach: don't use nested DirectoryScan, instead track depth
        // and process events from the same streams

        // For now, let's use a simpler approach: create a new frame that shares
        // the fs_scan from the parent but has its own repo_scan

        let parent_frame = self.stack.last_mut().unwrap();
        let fs_scan = std::mem::replace(
            &mut parent_frame.fs_scan,
            Box::pin(futures::stream::empty()),
        );

        self.stack.push(StackFrame {
            repo_scan,
            fs_scan,
            pending_repo: None,
            pending_fs: None,
            state: State::Processing,
        });

        // Skip the root EnterDirectory from the new repo scan
        let frame = self.stack.last_mut().unwrap();
        if let Some(RepoScanEvent::EnterDirectory(_)) = frame.repo_scan.next().await? {
            // Expected - skip it
        }

        // Continue processing at the new level
        self.process_current_level().await
    }

    /// Enter a directory that only exists in the repo (not in file store).
    async fn enter_directory_repo_only(
        &mut self,
        _name: &str,
        repo_dir_id: &ObjectId,
    ) -> Result<Option<DownloadAction>> {
        // The CreateDirectory action was already emitted
        // Now we need to process the subdirectory contents (all additions)

        let repo_scan = self.repo.scan_directory(repo_dir_id);

        // No file store scan for this directory (it doesn't exist)
        let fs_scan: ScanEvents = Box::pin(futures::stream::empty());

        self.stack.push(StackFrame {
            repo_scan,
            fs_scan,
            pending_repo: None,
            pending_fs: None,
            state: State::Processing,
        });

        // Skip the root EnterDirectory from the new repo scan
        let frame = self.stack.last_mut().unwrap();
        if let Some(RepoScanEvent::EnterDirectory(_)) = frame.repo_scan.next().await? {
            // Expected - skip it
        }

        // Continue processing at the new level (will be all DrainRepo)
        self.process_current_level().await
    }
}

/// Create a download actions iterator.
///
/// This function compares a repository directory with a file store location and
/// generates the sequence of actions needed to make the file store match the
/// repository contents.
///
/// The actions are generated lazily by "zippering" together the repo's directory
/// scan and the file store's scan, comparing entries in lexicographic order.
///
/// # Arguments
///
/// * `repo` - The repository to download from
/// * `directory_id` - The root directory ID in the repository
/// * `source` - The file source to compare against
/// * `path` - The base path in the file store
pub fn download_actions(
    repo: Arc<Repo>,
    directory_id: ObjectId,
    source: Arc<dyn FileSource>,
    path: &Path,
) -> DownloadActions {
    DownloadActions::new(repo, source, directory_id, path.to_path_buf())
}

#[cfg(test)]
mod tests {
    use super::*;

    // Basic test to ensure the module compiles
    #[test]
    fn test_download_action_enum() {
        let action = DownloadAction::CreateDirectory("test".to_string());
        assert!(matches!(action, DownloadAction::CreateDirectory(_)));

        let action = DownloadAction::RemoveFile("file.txt".to_string());
        assert!(matches!(action, DownloadAction::RemoveFile(_)));

        let action = DownloadAction::DownloadFile("file.txt".to_string(), "abc123".to_string());
        assert!(matches!(action, DownloadAction::DownloadFile(_, _)));
    }
}
