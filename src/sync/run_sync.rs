//! Run sync functionality.
//!
//! This module implements the `run_syncs` function that executes sync operations
//! for one or more file stores.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use chrono::Utc;
use futures::future::join_all;

use crate::app::{mod_dir_tree, upload_directory, App, AppCreateRepoContext, DirTreeModSpec};
use crate::download::download_directory;
use crate::file_store::{FileStore, SyncState};
use crate::merge::{merge_commits, ConflictContext};
use crate::repo::Repo;
use crate::repo_model::{CommitModel, RepoModel};
use crate::repository::{Commit, CommitMetadata, CommitType, DirEntry, RepoObject};
use crate::sync::error::{Result, SyncError};
use crate::util::{Complete, Completes, NoopComplete, WithComplete};

/// Specification for a single sync operation.
#[derive(Clone)]
pub struct SyncSpec {
    /// The file store to sync.
    pub file_store: Arc<dyn FileStore>,
    /// Repository specification (URL or configured name).
    pub repo_spec: String,
    /// Branch name to sync with.
    pub branch_name: String,
    /// Directory within repo to sync with.
    pub repo_directory: String,
}

/// Options for running syncs.
#[derive(Debug, Clone)]
pub struct RunSyncsOptions {
    /// Maximum memory for text merge operations.
    pub max_merge_memory: u64,
    /// If true, complete pending downloads from interrupted syncs.
    pub force_pending_downloads: bool,
    /// Use staging for downloads if available.
    pub with_stage: bool,
}

impl Default for RunSyncsOptions {
    fn default() -> Self {
        Self {
            max_merge_memory: 100 * 1024 * 1024, // 100 MB
            force_pending_downloads: false,
            with_stage: true,
        }
    }
}

/// Result of running multiple syncs.
pub struct RunSyncsResult {
    /// Per-filestore results (in same order as input).
    pub results: Vec<SyncResult>,
}

/// Result for a single sync operation.
pub struct SyncResult {
    /// Whether sync succeeded.
    pub success: bool,
    /// Any conflicts from merge.
    pub conflicts: Vec<ConflictContext>,
    /// Error if sync failed.
    pub error: Option<SyncError>,
}

impl SyncResult {
    /// Create a successful result.
    pub fn success(conflicts: Vec<ConflictContext>) -> Self {
        Self {
            success: true,
            conflicts,
            error: None,
        }
    }

    /// Create a failed result.
    pub fn failure(error: SyncError) -> Self {
        Self {
            success: false,
            conflicts: Vec::new(),
            error: Some(error),
        }
    }
}

// =============================================================================
// Internal types for grouping
// =============================================================================

/// A single sync item with its state and results.
struct SyncItem {
    /// Index in the original input list (for ordering results).
    original_index: usize,
    /// The file store to sync.
    file_store: Arc<dyn FileStore>,
    /// Directory within repo to sync with.
    repo_directory: String,
    /// Current sync state from file store.
    sync_state: SyncState,
    /// Uploaded directory ObjectId (filled during upload phase).
    uploaded_dir_id: Option<String>,
    /// Result of this sync (filled at end).
    result: Option<SyncResult>,
}

/// A group of syncs targeting the same repo+branch.
struct BranchGroup {
    branch_name: String,
    items: Vec<SyncItem>,
}

/// A group of syncs targeting the same repo+branch+base commit.
struct BaseGroup {
    base_commit_id: String,
    items: Vec<SyncItem>,
}

/// A group of syncs targeting the same repo.
struct RepoGroup {
    repo_spec: String,
    repo: Arc<Repo>,
    branch_groups: Vec<BranchGroup>,
}

// =============================================================================
// run_syncs implementation
// =============================================================================

/// Run sync operations for multiple file stores.
///
/// Syncs are grouped by repo, then by branch, to minimize redundant
/// operations and create efficient batch commits.
///
/// # Algorithm
///
/// 1. Group syncs by repo spec (parallel across repos)
/// 2. Within each repo, group by branch (parallel across branches)
/// 3. Within each branch group:
///    - Upload all filestores concurrently
///    - Sort by path depth (desc), then alphabetically
///    - Use DirTreeModSpec with retry for overlapping paths
///    - Create merge commits iteratively
/// 4. Update all branches atomically for the repo
/// 5. Download merged results to all filestores
///
/// # Arguments
///
/// * `syncs` - List of sync specifications
/// * `app` - Application context (for creating repos)
/// * `options` - Sync options
pub async fn run_syncs(
    syncs: Vec<SyncSpec>,
    app: &App,
    options: RunSyncsOptions,
) -> Result<WithComplete<RunSyncsResult>> {
    if syncs.is_empty() {
        return Ok(WithComplete::new(
            RunSyncsResult { results: vec![] },
            Arc::new(NoopComplete) as Arc<dyn Complete>,
        ));
    }

    let completes = Completes::new();

    // Build sync items with their current sync state
    let mut items_by_index: HashMap<usize, SyncItem> = HashMap::new();
    let total_count = syncs.len();

    for (index, spec) in syncs.into_iter().enumerate() {
        match build_sync_item(index, spec, &options).await {
            Ok(item) => {
                items_by_index.insert(index, item);
            }
            Err(e) => {
                // Store failure result
                items_by_index.insert(
                    index,
                    SyncItem {
                        original_index: index,
                        file_store: Arc::new(crate::file_store::MemoryFileStore::builder().build()), // placeholder
                        repo_directory: String::new(),
                        sync_state: SyncState {
                            created_at: String::new(),
                            repository_url: String::new(),
                            repository_directory: String::new(),
                            branch_name: String::new(),
                            base_commit: String::new(),
                        },
                        uploaded_dir_id: None,
                        result: Some(SyncResult::failure(e)),
                    },
                );
            }
        }
    }

    // Group items by repo spec
    let repo_groups = group_by_repo(items_by_index, app).await?;

    // Process all repo groups in parallel
    let repo_futures: Vec<_> = repo_groups
        .into_iter()
        .map(|group| process_repo_group(group, &options, &completes))
        .collect();

    let repo_results = join_all(repo_futures).await;

    // Collect all items back and sort by original index
    let mut all_items: Vec<SyncItem> = repo_results
        .into_iter()
        .flatten()
        .collect();

    all_items.sort_by_key(|item| item.original_index);

    // Build final results
    let results: Vec<SyncResult> = all_items
        .into_iter()
        .map(|item| item.result.unwrap_or_else(|| SyncResult::failure(
            SyncError::Other("sync item missing result".to_string())
        )))
        .collect();

    // Ensure we have the right number of results
    assert_eq!(results.len(), total_count);

    completes.done();
    Ok(WithComplete::new(
        RunSyncsResult { results },
        Arc::new(completes) as Arc<dyn Complete>,
    ))
}

/// Build a SyncItem from a SyncSpec, validating and extracting sync state.
async fn build_sync_item(
    index: usize,
    spec: SyncSpec,
    options: &RunSyncsOptions,
) -> Result<SyncItem> {
    // Get sync state manager
    let sync_mgr = spec
        .file_store
        .get_sync_state_manager()
        .ok_or(SyncError::NoSyncStateSupport)?;

    // Check for pending sync state (interrupted download)
    if let Some(pending) = sync_mgr.get_next_sync_state().await? {
        if !options.force_pending_downloads {
            return Err(SyncError::PendingDownloadRequired);
        }
        // If force_pending_downloads, we'll use the pending state
        return Ok(SyncItem {
            original_index: index,
            file_store: spec.file_store,
            repo_directory: pending.repository_directory.clone(),
            sync_state: pending,
            uploaded_dir_id: None,
            result: None,
        });
    }

    // Get current sync state
    let sync_state = sync_mgr
        .get_sync_state()
        .await?
        .ok_or_else(|| SyncError::Other("file store has no sync state".to_string()))?;

    Ok(SyncItem {
        original_index: index,
        file_store: spec.file_store,
        repo_directory: sync_state.repository_directory.clone(),
        sync_state,
        uploaded_dir_id: None,
        result: None,
    })
}

/// Group sync items by repo spec.
async fn group_by_repo(
    mut items: HashMap<usize, SyncItem>,
    app: &App,
) -> Result<Vec<RepoGroup>> {
    // Separate items that already have results (failures) from those to process
    let mut failed_items: Vec<SyncItem> = vec![];
    let mut items_to_process: Vec<SyncItem> = vec![];

    for (_, item) in items.drain() {
        if item.result.is_some() {
            failed_items.push(item);
        } else {
            items_to_process.push(item);
        }
    }

    // Group by repo URL
    let mut repo_map: HashMap<String, Vec<SyncItem>> = HashMap::new();
    for item in items_to_process {
        repo_map
            .entry(item.sync_state.repository_url.clone())
            .or_default()
            .push(item);
    }

    // Create RepoGroups
    let mut repo_groups = Vec::new();
    for (repo_url, items) in repo_map {
        // Create repo from URL
        let repo = match app.create_repo(AppCreateRepoContext::new(&repo_url)).await {
            Ok(r) => r,
            Err(e) => {
                // Mark all items in this group as failed
                let error_msg = e.to_string();
                for mut item in items {
                    item.result = Some(SyncResult::failure(SyncError::Other(error_msg.clone())));
                    failed_items.push(item);
                }
                continue;
            }
        };

        // Group by branch within this repo
        let branch_groups = group_by_branch(items);

        repo_groups.push(RepoGroup {
            repo_spec: repo_url,
            repo,
            branch_groups,
        });
    }

    // Add failed items as a "dummy" repo group so they're included in results
    if !failed_items.is_empty() {
        // These items already have results set, they'll just pass through
        repo_groups.push(RepoGroup {
            repo_spec: String::new(),
            repo: Arc::new(Repo::new(
                crate::backend::MemoryBackend::new(),
                crate::caches::NoopCache,
            )),
            branch_groups: vec![BranchGroup {
                branch_name: String::new(),
                items: failed_items,
            }],
        });
    }

    Ok(repo_groups)
}

/// Group sync items by branch name.
fn group_by_branch(items: Vec<SyncItem>) -> Vec<BranchGroup> {
    let mut branch_map: HashMap<String, Vec<SyncItem>> = HashMap::new();
    for item in items {
        branch_map
            .entry(item.sync_state.branch_name.clone())
            .or_default()
            .push(item);
    }

    branch_map
        .into_iter()
        .map(|(branch_name, items)| BranchGroup { branch_name, items })
        .collect()
}

/// Group sync items by base commit, returning groups sorted by base commit id.
fn group_by_base_commit(items: Vec<SyncItem>) -> Vec<BaseGroup> {
    let mut base_map: HashMap<String, Vec<SyncItem>> = HashMap::new();
    for item in items {
        base_map
            .entry(item.sync_state.base_commit.clone())
            .or_default()
            .push(item);
    }

    let mut groups: Vec<BaseGroup> = base_map
        .into_iter()
        .map(|(base_commit_id, items)| BaseGroup {
            base_commit_id,
            items,
        })
        .collect();

    // Sort by base commit id for deterministic ordering
    groups.sort_by(|a, b| a.base_commit_id.cmp(&b.base_commit_id));
    groups
}

/// Process a single repo group.
async fn process_repo_group(
    group: RepoGroup,
    options: &RunSyncsOptions,
    _completes: &Completes,
) -> Vec<SyncItem> {
    // Skip if this is a "dummy" group of failed items
    if group.repo_spec.is_empty() {
        return group.branch_groups.into_iter().flat_map(|bg| bg.items).collect();
    }

    let repo_model = RepoModel::new(Arc::clone(&group.repo));

    // Process all branch groups in parallel
    let branch_futures: Vec<_> = group
        .branch_groups
        .into_iter()
        .map(|bg| process_branch_group(bg, Arc::clone(&group.repo), &repo_model, options))
        .collect();

    let branch_results = join_all(branch_futures).await;

    // Collect results and branch updates
    let mut all_items = Vec::new();
    let mut branch_updates: Vec<(String, String)> = Vec::new(); // (branch_name, new_commit_id)

    for (items, branch_update) in branch_results {
        all_items.extend(items);
        if let Some((branch, commit)) = branch_update {
            branch_updates.push((branch, commit));
        }
    }

    // Update all branches atomically
    for (branch_name, commit_id) in &branch_updates {
        if let Err(e) = repo_model
            .create_next_branches(branch_name, Some(commit_id.clone()))
            .await
        {
            // Mark all items for this branch as failed
            for item in &mut all_items {
                if item.sync_state.branch_name == *branch_name && item.result.is_none() {
                    item.result = Some(SyncResult::failure(SyncError::Repo(e.clone())));
                }
            }
        }
    }

    // Download phase for all items - run in parallel
    let download_futures: Vec<_> = all_items
        .iter()
        .enumerate()
        .filter(|(_, item)| item.result.is_none())
        .map(|(idx, item)| {
            // Find the new commit for this item's branch
            let new_commit_id = branch_updates
                .iter()
                .find(|(b, _)| *b == item.sync_state.branch_name)
                .map(|(_, c)| c.clone());

            let repo = Arc::clone(&group.repo);
            let file_store = Arc::clone(&item.file_store);
            let repo_directory = item.repo_directory.clone();
            let sync_state = item.sync_state.clone();
            let with_stage = options.with_stage;

            async move {
                let result = if let Some(commit_id) = new_commit_id {
                    download_and_finalize_item(
                        &repo,
                        &file_store,
                        &repo_directory,
                        &sync_state,
                        &commit_id,
                        with_stage,
                    )
                    .await
                } else {
                    Err(SyncError::Other("missing branch update".to_string()))
                };
                (idx, result)
            }
        })
        .collect();

    let download_results = join_all(download_futures).await;

    // Apply download results to items
    for (idx, result) in download_results {
        match result {
            Ok(conflicts) => {
                all_items[idx].result = Some(SyncResult::success(conflicts));
            }
            Err(e) => {
                all_items[idx].result = Some(SyncResult::failure(e));
            }
        }
    }

    all_items
}

/// Process a single branch group: upload, merge, prepare for download.
/// Returns the items and optionally the (branch_name, new_commit_id) to update.
async fn process_branch_group(
    mut group: BranchGroup,
    repo: Arc<Repo>,
    repo_model: &RepoModel,
    options: &RunSyncsOptions,
) -> (Vec<SyncItem>, Option<(String, String)>) {
    // Skip if empty or all items already have results
    if group.items.is_empty() || group.items.iter().all(|i| i.result.is_some()) {
        return (group.items, None);
    }

    let branch_name = group.branch_name.clone();

    // Upload all filestores concurrently
    let upload_futures: Vec<_> = group
        .items
        .iter()
        .enumerate()
        .filter(|(_, item)| item.result.is_none())
        .map(|(idx, item)| {
            let repo = Arc::clone(&repo);
            let file_store = Arc::clone(&item.file_store);
            async move {
                let cache = file_store.get_cache();
                let result = upload_directory(&*file_store, repo, cache, None).await;
                (idx, result)
            }
        })
        .collect();

    let upload_results = join_all(upload_futures).await;

    // Store upload results
    for (idx, result) in upload_results {
        match result {
            Ok(upload_result) => {
                // Wait for upload to complete
                if let Err(e) = upload_result.complete.complete().await {
                    group.items[idx].result = Some(SyncResult::failure(SyncError::Other(
                        format!("upload completion failed: {}", e),
                    )));
                } else {
                    group.items[idx].uploaded_dir_id = Some(upload_result.result.hash);
                }
            }
            Err(e) => {
                group.items[idx].result = Some(SyncResult::failure(SyncError::Upload(e)));
            }
        }
    }

    // Separate items into uploaded (success) and failed
    let mut uploaded_items: Vec<SyncItem> = Vec::new();
    let mut failed_items: Vec<SyncItem> = Vec::new();
    for item in group.items {
        if item.result.is_some() {
            failed_items.push(item);
        } else if item.uploaded_dir_id.is_some() {
            uploaded_items.push(item);
        } else {
            // No result and no upload - shouldn't happen, but mark as failed
            let mut item = item;
            item.result = Some(SyncResult::failure(SyncError::Other(
                "item has no upload result".to_string(),
            )));
            failed_items.push(item);
        }
    }

    if uploaded_items.is_empty() {
        return (failed_items, None);
    }

    // Get the current branch head
    let branch_model = match repo_model.get_branch(&branch_name).await {
        Ok(Some(b)) => b,
        Ok(None) => {
            for item in &mut uploaded_items {
                item.result = Some(SyncResult::failure(SyncError::BranchNotFound(
                    branch_name.clone(),
                )));
            }
            uploaded_items.extend(failed_items);
            return (uploaded_items, None);
        }
        Err(e) => {
            for item in &mut uploaded_items {
                item.result = Some(SyncResult::failure(SyncError::Repo(e.clone())));
            }
            uploaded_items.extend(failed_items);
            return (uploaded_items, None);
        }
    };

    let branch_head_id = branch_model.commit().id.clone();

    // Group uploaded items by base commit
    let base_groups = group_by_base_commit(uploaded_items);

    // Process each base group in sequence, chaining the commits
    let mut current_commit_1_id = branch_head_id;
    let mut all_processed_items: Vec<SyncItem> = Vec::new();

    for base_group in base_groups {
        let items_refs: Vec<&SyncItem> = base_group.items.iter().collect();

        // Merge uploaded directories with retry for overlapping paths
        let merge_result = merge_uploaded_directories(
            &repo,
            &items_refs,
            &base_group.base_commit_id,
            &current_commit_1_id,
            options,
        )
        .await;

        match merge_result {
            Ok((new_commit_id, _conflicts)) => {
                // Update commit_1 for the next base group
                current_commit_1_id = new_commit_id;
                // Items will get their results during download phase
                all_processed_items.extend(base_group.items);
            }
            Err(e) => {
                let error_msg = e.to_string();
                for mut item in base_group.items {
                    item.result = Some(SyncResult::failure(SyncError::Other(error_msg.clone())));
                    all_processed_items.push(item);
                }
            }
        }
    }

    // Combine with failed items
    all_processed_items.extend(failed_items);

    (all_processed_items, Some((branch_name, current_commit_1_id)))
}

/// Merge uploaded directories using DirTreeModSpec with retry for overlaps.
///
/// # Arguments
/// * `repo` - The repository
/// * `items` - Items to merge (must all have the same base commit)
/// * `base_commit_id` - The common base commit for all items
/// * `theirs_commit_id` - The "theirs" commit (branch head or previous merge result)
/// * `options` - Sync options
///
/// # Terminology
/// - "ours" = the uploaded filestore content (what the user has locally)
/// - "theirs" = the branch head (the remote state)
async fn merge_uploaded_directories(
    repo: &Arc<Repo>,
    items: &[&SyncItem],
    base_commit_id: &str,
    theirs_commit_id: &str,
    options: &RunSyncsOptions,
) -> Result<(String, Vec<ConflictContext>)> {
    // Sort items by path depth (descending), then alphabetically
    let mut sorted_items: Vec<(&SyncItem, usize)> = items
        .iter()
        .map(|item| {
            let depth = item.repo_directory.split('/').filter(|s| !s.is_empty()).count();
            (*item, depth)
        })
        .collect();
    sorted_items.sort_by(|a, b| {
        // Sort by depth descending, then by path ascending
        b.1.cmp(&a.1).then_with(|| a.0.repo_directory.cmp(&b.0.repo_directory))
    });

    let sorted_items: Vec<&SyncItem> = sorted_items.into_iter().map(|(item, _)| item).collect();

    // Get the base commit's directory
    let base_commit = repo.read_commit(&base_commit_id.to_string()).await?;
    let base_root_dir = repo.read_directory(&base_commit.directory).await?;

    // Track the current "theirs" commit for merging (starts as branch head)
    // After each merge, this becomes the merge result for the next iteration
    let mut current_theirs_commit_id = theirs_commit_id.to_string();
    let mut all_conflicts: Vec<ConflictContext> = Vec::new();
    let mut remaining_items: Vec<&SyncItem> = sorted_items;

    // Iterate until all items are processed
    while !remaining_items.is_empty() {
        let mut spec = DirTreeModSpec::new();
        let mut retry_list: Vec<&SyncItem> = Vec::new();
        let mut added_any = false;

        for item in &remaining_items {
            let uploaded_dir_id = item.uploaded_dir_id.as_ref().unwrap();
            let path = Path::new(&item.repo_directory);

            if spec.can_add(path) {
                // Create directory entry for this uploaded directory
                let dir_entry = crate::repo::DirectoryEntry::Directory(DirEntry {
                    name: path
                        .file_name()
                        .and_then(|s| s.to_str())
                        .unwrap_or(&item.repo_directory)
                        .to_string(),
                    directory: uploaded_dir_id.clone(),
                });

                if spec.add(path, dir_entry, Arc::new(NoopComplete)).is_err() {
                    retry_list.push(*item);
                } else {
                    added_any = true;
                }
            } else {
                retry_list.push(*item);
            }
        }

        if !added_any {
            // Safety: prevent infinite loop if can_add returns false for all
            return Err(SyncError::Other(
                "cannot add any directories - possible bug in overlap detection".to_string(),
            ));
        }

        // Apply modifications to create new directory tree
        let modified_dir = mod_dir_tree(repo, base_root_dir.clone(), spec).await?;
        modified_dir.complete.complete().await
            .map_err(|e| SyncError::Other(format!("mod_dir_tree completion failed: {}", e)))?;

        // Create a commit with this modified directory (the "ours" commit - uploaded content)
        let upload_commit = Commit {
            type_tag: CommitType::Commit,
            directory: modified_dir.result.clone(),
            parents: vec![base_commit_id.to_string()],
            metadata: Some(CommitMetadata {
                timestamp: Some(Utc::now().to_rfc3339()),
                author: None,
                committer: None,
                message: Some("Sync upload".to_string()),
            }),
        };
        let upload_commit_result = repo.write_object(&RepoObject::Commit(upload_commit)).await?;
        upload_commit_result.complete.complete().await
            .map_err(|e| SyncError::Other(format!("commit write completion failed: {}", e)))?;

        // Merge: base vs theirs (branch head) vs ours (uploaded)
        // commit_1 = theirs (branch head), commit_2 = ours (uploaded)
        let base_model = CommitModel::new(Arc::clone(repo), base_commit_id.to_string());
        let theirs_model = CommitModel::new(Arc::clone(repo), current_theirs_commit_id.clone());
        let ours_model = CommitModel::new(Arc::clone(repo), upload_commit_result.result.clone());

        let merge_result = merge_commits(
            Arc::clone(repo),
            &base_model,
            &theirs_model,
            &ours_model,
            Some(CommitMetadata {
                timestamp: Some(Utc::now().to_rfc3339()),
                author: None,
                committer: None,
                message: Some("Sync merge".to_string()),
            }),
            options.max_merge_memory,
        )
        .await?;

        merge_result.commit.complete.complete().await
            .map_err(|e| SyncError::Other(format!("merge completion failed: {}", e)))?;

        all_conflicts.extend(merge_result.conflicts);
        // Update "theirs" to the merge result for the next iteration
        current_theirs_commit_id = merge_result.commit.result;

        remaining_items = retry_list;
    }

    Ok((current_theirs_commit_id, all_conflicts))
}

/// Download merged content and finalize sync state for a single item.
///
/// This is a standalone version that takes individual parameters instead of
/// a SyncItem reference, allowing parallel execution.
async fn download_and_finalize_item(
    repo: &Arc<Repo>,
    file_store: &Arc<dyn FileStore>,
    repo_directory: &str,
    sync_state: &SyncState,
    new_commit_id: &str,
    with_stage: bool,
) -> Result<Vec<ConflictContext>> {
    let sync_mgr = file_store
        .get_sync_state_manager()
        .ok_or(SyncError::NoSyncStateSupport)?;

    // Get the directory to download
    let commit = repo.read_commit(&new_commit_id.to_string()).await?;

    // Resolve the repo directory path to get the specific subdirectory
    let dir_to_download = if repo_directory.is_empty() || repo_directory == "/" {
        commit.directory.clone()
    } else {
        // We need to resolve the path within the commit's directory
        let repo_model = RepoModel::new(Arc::clone(repo));
        let branch_model = repo_model
            .get_branch(&sync_state.branch_name)
            .await?
            .ok_or_else(|| SyncError::BranchNotFound(sync_state.branch_name.clone()))?;
        let commit_model = branch_model.commit();
        let root = commit_model.root().await?;

        let resolved = root.resolve_path(repo_directory).await?;
        match resolved {
            crate::repo_model::ResolvePathResult::Directory(d) => d.id().clone(),
            crate::repo_model::ResolvePathResult::Root => commit.directory.clone(),
            _ => {
                return Err(SyncError::Other(format!(
                    "repo path {} is not a directory after merge",
                    repo_directory
                )));
            }
        }
    };

    // Create pending sync state before download
    let pending_state = SyncState {
        created_at: Utc::now().to_rfc3339(),
        repository_url: sync_state.repository_url.clone(),
        repository_directory: repo_directory.to_string(),
        branch_name: sync_state.branch_name.clone(),
        base_commit: new_commit_id.to_string(),
    };
    sync_mgr.set_next_sync_state(Some(&pending_state)).await?;

    // Download
    let download_result = download_directory(
        Arc::clone(repo),
        &dir_to_download,
        &**file_store,
        std::path::PathBuf::new(),
        with_stage,
        false,
        None,
    )
    .await?;

    download_result.complete.complete().await
        .map_err(|e| SyncError::Other(format!("download completion failed: {}", e)))?;

    // Commit sync state
    sync_mgr.commit_next_sync_state().await?;

    // Return empty conflicts for now (conflicts were tracked at merge level)
    Ok(Vec::new())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_run_syncs_options_default() {
        let options = RunSyncsOptions::default();
        assert_eq!(options.max_merge_memory, 100 * 1024 * 1024);
        assert!(!options.force_pending_downloads);
        assert!(options.with_stage);
    }

    #[test]
    fn test_sync_result_success() {
        let result = SyncResult::success(vec![]);
        assert!(result.success);
        assert!(result.conflicts.is_empty());
        assert!(result.error.is_none());
    }

    #[test]
    fn test_sync_result_failure() {
        let result = SyncResult::failure(SyncError::NoSyncStateSupport);
        assert!(!result.success);
        assert!(result.conflicts.is_empty());
        assert!(result.error.is_some());
    }

    #[test]
    fn test_group_by_branch() {
        // Create test sync items
        let sync_state_branch_a = SyncState {
            created_at: String::new(),
            repository_url: "test://repo".to_string(),
            repository_directory: "/dir_a".to_string(),
            branch_name: "branch_a".to_string(),
            base_commit: "abc".to_string(),
        };
        let sync_state_branch_b = SyncState {
            created_at: String::new(),
            repository_url: "test://repo".to_string(),
            repository_directory: "/dir_b".to_string(),
            branch_name: "branch_b".to_string(),
            base_commit: "def".to_string(),
        };
        let sync_state_branch_a2 = SyncState {
            created_at: String::new(),
            repository_url: "test://repo".to_string(),
            repository_directory: "/dir_c".to_string(),
            branch_name: "branch_a".to_string(),
            base_commit: "abc".to_string(),
        };

        let items = vec![
            SyncItem {
                original_index: 0,
                file_store: Arc::new(crate::file_store::MemoryFileStore::builder().build()),
                repo_directory: "/dir_a".to_string(),
                sync_state: sync_state_branch_a,
                uploaded_dir_id: None,
                result: None,
            },
            SyncItem {
                original_index: 1,
                file_store: Arc::new(crate::file_store::MemoryFileStore::builder().build()),
                repo_directory: "/dir_b".to_string(),
                sync_state: sync_state_branch_b,
                uploaded_dir_id: None,
                result: None,
            },
            SyncItem {
                original_index: 2,
                file_store: Arc::new(crate::file_store::MemoryFileStore::builder().build()),
                repo_directory: "/dir_c".to_string(),
                sync_state: sync_state_branch_a2,
                uploaded_dir_id: None,
                result: None,
            },
        ];

        let groups = group_by_branch(items);

        // Should have 2 groups: branch_a with 2 items, branch_b with 1 item
        assert_eq!(groups.len(), 2);

        let branch_a_group = groups.iter().find(|g| g.branch_name == "branch_a");
        let branch_b_group = groups.iter().find(|g| g.branch_name == "branch_b");

        assert!(branch_a_group.is_some());
        assert!(branch_b_group.is_some());
        assert_eq!(branch_a_group.unwrap().items.len(), 2);
        assert_eq!(branch_b_group.unwrap().items.len(), 1);
    }

    #[tokio::test]
    async fn test_run_syncs_empty() {
        use crate::app::{App, AppContext, AppError};

        // Use with_app for proper App lifecycle
        let result: std::result::Result<usize, AppError> =
            App::with_app(AppContext::default(), |app| {
                Box::pin(async move {
                    let result = run_syncs(vec![], app, RunSyncsOptions::default())
                        .await
                        .map_err(|e| AppError::Config(e.to_string()))?;
                    Ok(result.result.results.len())
                })
            })
            .await;

        assert_eq!(result.unwrap(), 0);
    }

    #[test]
    fn test_directory_depth_sorting() {
        // Test the sorting logic: deeper paths should come first, then alphabetically
        let paths = vec![
            "/a".to_string(),
            "/a/b/c".to_string(),
            "/a/b".to_string(),
            "/z".to_string(),
            "/a/b/c/d".to_string(),
        ];

        let mut sorted: Vec<(&str, usize)> = paths
            .iter()
            .map(|p| {
                let depth = p.split('/').filter(|s| !s.is_empty()).count();
                (p.as_str(), depth)
            })
            .collect();

        sorted.sort_by(|a, b| {
            // Sort by depth descending, then by path ascending
            b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0))
        });

        let sorted_paths: Vec<&str> = sorted.iter().map(|(p, _)| *p).collect();

        // Expected order: deepest first, then alphabetically within same depth
        assert_eq!(sorted_paths[0], "/a/b/c/d"); // depth 4
        assert_eq!(sorted_paths[1], "/a/b/c"); // depth 3
        assert_eq!(sorted_paths[2], "/a/b"); // depth 2
        // depth 1: /a and /z, alphabetically /a < /z
        assert_eq!(sorted_paths[3], "/a"); // depth 1
        assert_eq!(sorted_paths[4], "/z"); // depth 1
    }

    #[test]
    fn test_group_by_base_commit() {
        // Create test sync items with different base commits
        let sync_state_base_a1 = SyncState {
            created_at: String::new(),
            repository_url: "test://repo".to_string(),
            repository_directory: "/dir_1".to_string(),
            branch_name: "main".to_string(),
            base_commit: "commit_aaa".to_string(),
        };
        let sync_state_base_b = SyncState {
            created_at: String::new(),
            repository_url: "test://repo".to_string(),
            repository_directory: "/dir_2".to_string(),
            branch_name: "main".to_string(),
            base_commit: "commit_bbb".to_string(),
        };
        let sync_state_base_a2 = SyncState {
            created_at: String::new(),
            repository_url: "test://repo".to_string(),
            repository_directory: "/dir_3".to_string(),
            branch_name: "main".to_string(),
            base_commit: "commit_aaa".to_string(),
        };

        let items = vec![
            SyncItem {
                original_index: 0,
                file_store: Arc::new(crate::file_store::MemoryFileStore::builder().build()),
                repo_directory: "/dir_1".to_string(),
                sync_state: sync_state_base_a1,
                uploaded_dir_id: None,
                result: None,
            },
            SyncItem {
                original_index: 1,
                file_store: Arc::new(crate::file_store::MemoryFileStore::builder().build()),
                repo_directory: "/dir_2".to_string(),
                sync_state: sync_state_base_b,
                uploaded_dir_id: None,
                result: None,
            },
            SyncItem {
                original_index: 2,
                file_store: Arc::new(crate::file_store::MemoryFileStore::builder().build()),
                repo_directory: "/dir_3".to_string(),
                sync_state: sync_state_base_a2,
                uploaded_dir_id: None,
                result: None,
            },
        ];

        let groups = group_by_base_commit(items);

        // Should have 2 groups: commit_aaa with 2 items, commit_bbb with 1 item
        assert_eq!(groups.len(), 2);

        // Groups should be sorted by base commit id
        assert_eq!(groups[0].base_commit_id, "commit_aaa");
        assert_eq!(groups[1].base_commit_id, "commit_bbb");

        assert_eq!(groups[0].items.len(), 2);
        assert_eq!(groups[1].items.len(), 1);
    }
}
