//! Commit merge implementation.
//!
//! This module implements merging of commits, which wraps directory merging
//! with fast-forward detection and commit creation.

use std::sync::Arc;

use crate::merge::directory::{merge_directories, MergeDirectoryResult};
use crate::merge::error::Result;
use crate::merge::types::ConflictContext;
use crate::repo::Repo;
use crate::repo_model::CommitModel;
use crate::repository::{CommitMetadata, ObjectId};
use crate::util::{NoopComplete, WithComplete};

// =============================================================================
// MergeCommitResult
// =============================================================================

/// Result of a commit merge operation.
pub struct MergeCommitResult {
    /// The resulting commit ObjectId with completion tracking.
    pub commit: WithComplete<ObjectId>,
    /// Any conflicts encountered during the merge.
    pub conflicts: Vec<ConflictContext>,
    /// Whether this was a fast-forward merge.
    pub fast_forward: bool,
}

// =============================================================================
// merge_commits
// =============================================================================

/// Merge two commits that share a common base.
///
/// This function implements the merge_commits algorithm from docs/architecture/merge.md.
/// It handles fast-forward cases where one commit is the same as the base, and
/// otherwise performs a full directory merge and creates a new merge commit.
///
/// # Arguments
/// * `repo` - Repository access
/// * `base` - The common ancestor commit
/// * `commit_1` - First commit to merge (will be the primary parent)
/// * `commit_2` - Second commit to merge (will be an additional parent)
/// * `commit_metadata` - Optional metadata for the new merge commit
/// * `max_merge_memory` - Maximum combined file size for text merges
///
/// # Returns
/// The merge result containing the resulting commit ID and any conflicts.
///
/// # Fast-Forward Cases
/// - If `base` and `commit_1` have the same ID, returns `commit_2` (fast-forward)
/// - If `base` and `commit_2` have the same ID, returns `commit_1` (fast-forward)
pub async fn merge_commits(
    repo: Arc<Repo>,
    base: &CommitModel,
    commit_1: &CommitModel,
    commit_2: &CommitModel,
    commit_metadata: Option<CommitMetadata>,
    max_merge_memory: u64,
) -> Result<MergeCommitResult> {
    // Fast-forward: if base and commit_1 are the same, return commit_2
    if base.id == commit_1.id {
        return Ok(MergeCommitResult {
            commit: WithComplete::new(commit_2.id.clone(), Arc::new(NoopComplete)),
            conflicts: Vec::new(),
            fast_forward: true,
        });
    }

    // Fast-forward: if base and commit_2 are the same, return commit_1
    if base.id == commit_2.id {
        return Ok(MergeCommitResult {
            commit: WithComplete::new(commit_1.id.clone(), Arc::new(NoopComplete)),
            conflicts: Vec::new(),
            fast_forward: true,
        });
    }

    // Get the directory IDs from each commit
    let base_dir = base.root().await?.directory();
    let commit_1_dir = commit_1.root().await?.directory();
    let commit_2_dir = commit_2.root().await?.directory();

    // Perform the directory merge
    let MergeDirectoryResult {
        directory: merged_dir,
        conflicts,
    } = merge_directories(
        Arc::clone(&repo),
        Some(&base_dir.id),
        &commit_1_dir.id,
        &commit_2_dir.id,
        max_merge_memory,
    )
    .await?;

    // Create a new commit with commit_1 as primary parent and commit_2 as additional parent
    let new_commit = commit_1
        .create_next_commit(
            merged_dir.result,
            commit_metadata,
            Some(vec![commit_2.id.clone()]),
        )
        .await?;

    Ok(MergeCommitResult {
        commit: new_commit,
        conflicts,
        fast_forward: false,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::DirectoryListBuilder;
    use crate::backend::MemoryBackend;
    use crate::caches::NoopCache;
    use crate::repository::{Commit, CommitType, FileEntry, RepoObject};
    use crate::util::{Complete, NotifyComplete};

    fn create_test_repo() -> Arc<Repo> {
        let backend = MemoryBackend::new();
        let cache = NoopCache;
        Arc::new(Repo::new(backend, cache))
    }

    fn make_complete() -> Arc<dyn Complete> {
        let c = Arc::new(NotifyComplete::new());
        c.notify_complete();
        c
    }

    /// Create an empty directory and return its ObjectId.
    async fn create_empty_directory(repo: &Arc<Repo>) -> ObjectId {
        let builder = DirectoryListBuilder::new(Arc::clone(repo));
        let result = builder.finish().await.unwrap();
        result.complete.complete().await.unwrap();
        result.hash
    }

    /// Create a directory with a single file and return its ObjectId.
    async fn create_directory_with_file(
        repo: &Arc<Repo>,
        filename: &str,
        file_id: &str,
    ) -> ObjectId {
        use crate::app::DirectoryLeaf;

        let mut builder = DirectoryListBuilder::new(Arc::clone(repo));
        let file_entry = FileEntry {
            name: filename.to_string(),
            size: 100,
            executable: false,
            file: file_id.to_string(),
        };
        builder
            .add(DirectoryLeaf::File(file_entry), make_complete())
            .await
            .unwrap();
        let result = builder.finish().await.unwrap();
        result.complete.complete().await.unwrap();
        result.hash
    }

    /// Create a commit with the given directory and parents.
    async fn create_commit(
        repo: &Arc<Repo>,
        directory_id: ObjectId,
        parents: Vec<ObjectId>,
    ) -> ObjectId {
        let commit = Commit {
            type_tag: CommitType::Commit,
            directory: directory_id,
            parents,
            metadata: None,
        };
        let result = repo.write_object(&RepoObject::Commit(commit)).await.unwrap();
        result.complete.complete().await.unwrap();
        result.result
    }

    #[tokio::test]
    async fn test_fast_forward_base_equals_commit_1() {
        let repo = create_test_repo();

        // Create directories
        let base_dir = create_empty_directory(&repo).await;
        let commit_2_dir = create_directory_with_file(&repo, "new_file.txt", "file123").await;

        // Create commits: base == commit_1, commit_2 has changes
        let base_commit_id = create_commit(&repo, base_dir.clone(), vec![]).await;
        let commit_2_id = create_commit(&repo, commit_2_dir, vec![base_commit_id.clone()]).await;

        // Create CommitModels
        let base = CommitModel::new(Arc::clone(&repo), base_commit_id.clone());
        let commit_1 = CommitModel::new(Arc::clone(&repo), base_commit_id); // Same as base
        let commit_2 = CommitModel::new(Arc::clone(&repo), commit_2_id.clone());

        // Merge
        let result = merge_commits(
            Arc::clone(&repo),
            &base,
            &commit_1,
            &commit_2,
            None,
            1024 * 1024 * 1024,
        )
        .await
        .unwrap();

        // Should fast-forward to commit_2
        assert!(result.fast_forward);
        assert_eq!(result.commit.result, commit_2_id);
        assert!(result.conflicts.is_empty());
    }

    #[tokio::test]
    async fn test_fast_forward_base_equals_commit_2() {
        let repo = create_test_repo();

        // Create directories
        let base_dir = create_empty_directory(&repo).await;
        let commit_1_dir = create_directory_with_file(&repo, "new_file.txt", "file123").await;

        // Create commits: base == commit_2, commit_1 has changes
        let base_commit_id = create_commit(&repo, base_dir.clone(), vec![]).await;
        let commit_1_id = create_commit(&repo, commit_1_dir, vec![base_commit_id.clone()]).await;

        // Create CommitModels
        let base = CommitModel::new(Arc::clone(&repo), base_commit_id.clone());
        let commit_1 = CommitModel::new(Arc::clone(&repo), commit_1_id.clone());
        let commit_2 = CommitModel::new(Arc::clone(&repo), base_commit_id); // Same as base

        // Merge
        let result = merge_commits(
            Arc::clone(&repo),
            &base,
            &commit_1,
            &commit_2,
            None,
            1024 * 1024 * 1024,
        )
        .await
        .unwrap();

        // Should fast-forward to commit_1
        assert!(result.fast_forward);
        assert_eq!(result.commit.result, commit_1_id);
        assert!(result.conflicts.is_empty());
    }

    /// Helper to create a directory with two files.
    async fn create_directory_with_two_files(
        repo: &Arc<Repo>,
        file1_name: &str,
        file1_id: &str,
        file2_name: &str,
        file2_id: &str,
    ) -> ObjectId {
        use crate::app::DirectoryLeaf;

        let mut builder = DirectoryListBuilder::new(Arc::clone(repo));

        // Add files in sorted order
        let mut files = vec![(file1_name, file1_id), (file2_name, file2_id)];
        files.sort_by_key(|(name, _)| *name);

        for (name, id) in files {
            builder
                .add(
                    DirectoryLeaf::File(FileEntry {
                        name: name.to_string(),
                        size: 100,
                        executable: false,
                        file: id.to_string(),
                    }),
                    make_complete(),
                )
                .await
                .unwrap();
        }

        let result = builder.finish().await.unwrap();
        result.complete.complete().await.unwrap();
        result.hash
    }

    #[tokio::test]
    async fn test_non_fast_forward_no_conflicts() {
        let repo = create_test_repo();

        // Create base directory with two files
        let base_dir =
            create_directory_with_two_files(&repo, "file_a.txt", "a_base", "file_b.txt", "b_base")
                .await;

        // commit_1: modifies file_a, keeps file_b unchanged
        let commit_1_dir = create_directory_with_two_files(
            &repo,
            "file_a.txt",
            "a_modified_1",
            "file_b.txt",
            "b_base",
        )
        .await;

        // commit_2: keeps file_a unchanged, modifies file_b
        let commit_2_dir = create_directory_with_two_files(
            &repo,
            "file_a.txt",
            "a_base",
            "file_b.txt",
            "b_modified_2",
        )
        .await;

        // Create commits
        let base_commit_id = create_commit(&repo, base_dir, vec![]).await;
        let commit_1_id = create_commit(&repo, commit_1_dir, vec![base_commit_id.clone()]).await;
        let commit_2_id = create_commit(&repo, commit_2_dir, vec![base_commit_id.clone()]).await;

        // Create CommitModels
        let base = CommitModel::new(Arc::clone(&repo), base_commit_id);
        let commit_1 = CommitModel::new(Arc::clone(&repo), commit_1_id.clone());
        let commit_2 = CommitModel::new(Arc::clone(&repo), commit_2_id.clone());

        // Merge
        let result = merge_commits(
            Arc::clone(&repo),
            &base,
            &commit_1,
            &commit_2,
            None,
            1024 * 1024 * 1024,
        )
        .await
        .unwrap();

        // Should NOT be fast-forward
        assert!(!result.fast_forward);
        assert!(result.conflicts.is_empty());

        // The result should be a new commit (not commit_1 or commit_2)
        assert_ne!(result.commit.result, commit_1_id);
        assert_ne!(result.commit.result, commit_2_id);

        // Verify the new commit has correct parents
        result.commit.complete.complete().await.unwrap();
        let new_commit = repo.read_commit(&result.commit.result).await.unwrap();
        assert_eq!(new_commit.parents.len(), 2);
        assert_eq!(new_commit.parents[0], commit_1_id);
        assert_eq!(new_commit.parents[1], commit_2_id);

        // Verify merged directory contains both files
        let mut entry_list = repo
            .list_directory_entries(&new_commit.directory)
            .await
            .unwrap();
        let mut names = Vec::new();
        while let Some(entry) = entry_list.next().await.unwrap() {
            names.push(entry.name().to_string());
        }
        assert_eq!(names.len(), 2);
        assert!(names.contains(&"file_a.txt".to_string()));
        assert!(names.contains(&"file_b.txt".to_string()));
    }

    #[tokio::test]
    async fn test_non_fast_forward_with_conflict() {
        use crate::app::{DirectoryLeaf, StreamingFileUploader};
        use crate::util::ManagedBuffers;

        let repo = create_test_repo();
        let managed_buffers = ManagedBuffers::new();

        // Helper to write actual file content and return the file ObjectId
        async fn write_file_content(
            repo: &Arc<Repo>,
            managed_buffers: &ManagedBuffers,
            content: &[u8],
        ) -> ObjectId {
            let mut uploader = StreamingFileUploader::new(Arc::clone(repo));
            let buf = managed_buffers.get_buffer_with_data(content.to_vec()).await;
            uploader.add(buf).await.unwrap();
            let result = uploader.finish().await.unwrap();
            result.complete.complete().await.unwrap();
            result.result.hash
        }

        // Create actual file content
        let base_file_id =
            write_file_content(&repo, &managed_buffers, b"line1\nline2\nline3\n").await;
        let commit_1_file_id =
            write_file_content(&repo, &managed_buffers, b"modified_1\nline2\nline3\n").await;
        let commit_2_file_id =
            write_file_content(&repo, &managed_buffers, b"modified_2\nline2\nline3\n").await;

        // Create directories with actual files
        let base_dir = {
            let mut builder = DirectoryListBuilder::new(Arc::clone(&repo));
            builder
                .add(
                    DirectoryLeaf::File(FileEntry {
                        name: "conflict.txt".to_string(),
                        size: 18,
                        executable: false,
                        file: base_file_id,
                    }),
                    make_complete(),
                )
                .await
                .unwrap();
            let result = builder.finish().await.unwrap();
            result.complete.complete().await.unwrap();
            result.hash
        };

        let commit_1_dir = {
            let mut builder = DirectoryListBuilder::new(Arc::clone(&repo));
            builder
                .add(
                    DirectoryLeaf::File(FileEntry {
                        name: "conflict.txt".to_string(),
                        size: 24,
                        executable: false,
                        file: commit_1_file_id,
                    }),
                    make_complete(),
                )
                .await
                .unwrap();
            let result = builder.finish().await.unwrap();
            result.complete.complete().await.unwrap();
            result.hash
        };

        let commit_2_dir = {
            let mut builder = DirectoryListBuilder::new(Arc::clone(&repo));
            builder
                .add(
                    DirectoryLeaf::File(FileEntry {
                        name: "conflict.txt".to_string(),
                        size: 24,
                        executable: false,
                        file: commit_2_file_id,
                    }),
                    make_complete(),
                )
                .await
                .unwrap();
            let result = builder.finish().await.unwrap();
            result.complete.complete().await.unwrap();
            result.hash
        };

        // Create commits
        let base_commit_id = create_commit(&repo, base_dir, vec![]).await;
        let commit_1_id = create_commit(&repo, commit_1_dir, vec![base_commit_id.clone()]).await;
        let commit_2_id = create_commit(&repo, commit_2_dir, vec![base_commit_id.clone()]).await;

        // Create CommitModels
        let base = CommitModel::new(Arc::clone(&repo), base_commit_id);
        let commit_1 = CommitModel::new(Arc::clone(&repo), commit_1_id.clone());
        let commit_2 = CommitModel::new(Arc::clone(&repo), commit_2_id.clone());

        // Merge
        let result = merge_commits(
            Arc::clone(&repo),
            &base,
            &commit_1,
            &commit_2,
            None,
            1024 * 1024 * 1024,
        )
        .await
        .unwrap();

        // Should NOT be fast-forward
        assert!(!result.fast_forward);

        // Should have a conflict (the file was modified differently on both sides - same line)
        assert!(!result.conflicts.is_empty());
        assert_eq!(result.conflicts[0].name, "conflict.txt");

        // The result should still be a new commit
        assert_ne!(result.commit.result, commit_1_id);
        assert_ne!(result.commit.result, commit_2_id);
    }

    #[tokio::test]
    async fn test_merge_with_metadata() {
        let repo = create_test_repo();

        // Create base directory with two files
        let base_dir =
            create_directory_with_two_files(&repo, "file_a.txt", "a_base", "file_b.txt", "b_base")
                .await;

        // commit_1: modifies file_a, keeps file_b unchanged
        let commit_1_dir = create_directory_with_two_files(
            &repo,
            "file_a.txt",
            "a_modified",
            "file_b.txt",
            "b_base",
        )
        .await;

        // commit_2: keeps file_a unchanged, modifies file_b
        let commit_2_dir = create_directory_with_two_files(
            &repo,
            "file_a.txt",
            "a_base",
            "file_b.txt",
            "b_modified",
        )
        .await;

        // Create commits
        let base_commit_id = create_commit(&repo, base_dir, vec![]).await;
        let commit_1_id = create_commit(&repo, commit_1_dir, vec![base_commit_id.clone()]).await;
        let commit_2_id = create_commit(&repo, commit_2_dir, vec![base_commit_id.clone()]).await;

        // Create CommitModels
        let base = CommitModel::new(Arc::clone(&repo), base_commit_id);
        let commit_1 = CommitModel::new(Arc::clone(&repo), commit_1_id);
        let commit_2 = CommitModel::new(Arc::clone(&repo), commit_2_id);

        // Merge with metadata
        let metadata = CommitMetadata {
            timestamp: Some("2024-01-01T00:00:00Z".to_string()),
            author: Some("test_author".to_string()),
            committer: Some("test_committer".to_string()),
            message: Some("Merge commit".to_string()),
        };

        let result = merge_commits(
            Arc::clone(&repo),
            &base,
            &commit_1,
            &commit_2,
            Some(metadata),
            1024 * 1024 * 1024,
        )
        .await
        .unwrap();

        assert!(!result.fast_forward);

        // Verify metadata was applied
        result.commit.complete.complete().await.unwrap();
        let new_commit = repo.read_commit(&result.commit.result).await.unwrap();
        let meta = new_commit.metadata.unwrap();
        assert_eq!(meta.message, Some("Merge commit".to_string()));
        assert_eq!(meta.author, Some("test_author".to_string()));
    }
}
