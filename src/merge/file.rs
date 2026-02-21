//! Three-way file merge implementation.
//!
//! This module implements the `merge_files` function that performs a 3-way
//! text merge of files using the `merge3` crate, with binary detection via
//! `content_inspector` and streaming output via `StreamingFileUploader`.

use std::sync::Arc;

use merge3::{Merge3, MergeGroup};

use crate::app::StreamingFileUploader;
use crate::merge::directory::MergeFileResult;
use crate::merge::error::Result;
use crate::merge::types::{ConflictContext, MergeFileOutcome};
use crate::repo::Repo;
use crate::repository::{FileEntry, ObjectId};
use crate::util::ManagedBuffers;

// =============================================================================
// merge_files
// =============================================================================

/// Attempt a 3-way text merge of two files against a common base.
///
/// # Algorithm
///
/// 1. Check combined file sizes against `max_merge_memory`
/// 2. Download the first chunk of each file concurrently to detect binary content
/// 3. Download remaining chunks into memory
/// 4. Run a 3-way merge via the `merge3` crate
/// 5. Stream the merged output (with conflict markers if needed) via `StreamingFileUploader`
///
/// # Returns
///
/// - `MergeFileResult::Merged` if the merge succeeded cleanly
/// - `MergeFileResult::Conflict` with an appropriate `MergeFileOutcome` if the
///   files are too large, binary, or have conflicting changes
#[allow(clippy::too_many_arguments)]
pub async fn merge_files(
    repo: Arc<Repo>,
    name: &str,
    base: Option<&FileEntry>,
    file_1: &FileEntry,
    file_2: &FileEntry,
    executable: bool,
    max_merge_memory: u64,
    context: ConflictContext,
) -> Result<MergeFileResult> {
    // 1. Size check
    let base_size = base.map_or(0, |b| b.size);
    let combined_size = base_size + file_1.size + file_2.size;
    if combined_size > max_merge_memory {
        let mut ctx = context;
        ctx.merge_file_result = Some(MergeFileOutcome::TooLarge);
        return Ok(MergeFileResult::Conflict(Box::new(ctx)));
    }

    // 2-3. Download files and check for binary content
    let (base_bytes, file_1_bytes, file_2_bytes) =
        download_and_check_binary(&repo, base, file_1, file_2).await?;

    // Check binary results
    if let DownloadResult::Binary = &base_bytes {
        let mut ctx = context;
        ctx.merge_file_result = Some(MergeFileOutcome::Binary);
        return Ok(MergeFileResult::Conflict(Box::new(ctx)));
    }
    if let DownloadResult::Binary = &file_1_bytes {
        let mut ctx = context;
        ctx.merge_file_result = Some(MergeFileOutcome::Binary);
        return Ok(MergeFileResult::Conflict(Box::new(ctx)));
    }
    if let DownloadResult::Binary = &file_2_bytes {
        let mut ctx = context;
        ctx.merge_file_result = Some(MergeFileOutcome::Binary);
        return Ok(MergeFileResult::Conflict(Box::new(ctx)));
    }

    let base_data = base_bytes.into_bytes();
    let file_1_data = file_1_bytes.into_bytes();
    let file_2_data = file_2_bytes.into_bytes();

    // 4. Parse as UTF-8 and split into lines
    let base_text = std::str::from_utf8(&base_data).unwrap_or("");
    let file_1_text = std::str::from_utf8(&file_1_data).unwrap_or("");
    let file_2_text = std::str::from_utf8(&file_2_data).unwrap_or("");

    let base_lines: Vec<&str> = split_lines(base_text);
    let file_1_lines: Vec<&str> = split_lines(file_1_text);
    let file_2_lines: Vec<&str> = split_lines(file_2_text);

    // 5. Run merge3
    let m3 = Merge3::new(&base_lines, &file_1_lines, &file_2_lines);
    let groups = m3.merge_groups();

    // 6. Stream output through StreamingFileUploader
    let mut uploader = StreamingFileUploader::new(Arc::clone(&repo));
    let managed_buffers = ManagedBuffers::new();
    let mut has_conflicts = false;
    let mut total_size: u64 = 0;

    for group in &groups {
        match group {
            MergeGroup::Unchanged(lines)
            | MergeGroup::Same(lines)
            | MergeGroup::A(lines)
            | MergeGroup::B(lines) => {
                let bytes = lines_to_bytes(lines);
                total_size += bytes.len() as u64;
                let buf = managed_buffers.get_buffer_with_data(bytes).await;
                uploader.add(buf).await?;
            }
            MergeGroup::Conflict(base_lines, a_lines, b_lines) => {
                has_conflicts = true;
                let bytes = format_conflict(base_lines, a_lines, b_lines, name);
                total_size += bytes.len() as u64;
                let buf = managed_buffers.get_buffer_with_data(bytes).await;
                uploader.add(buf).await?;
            }
        }
    }

    // 7. Finish upload
    let upload_result = uploader.finish().await?;
    let file_id: ObjectId = upload_result.result.hash.clone();

    // 8. Return result
    if has_conflicts {
        let mut ctx = context;
        ctx.merge_file_result = Some(MergeFileOutcome::MergeConflict {
            file_id,
            size: total_size,
        });
        Ok(MergeFileResult::Conflict(Box::new(ctx)))
    } else {
        let file_entry = FileEntry {
            name: name.to_string(),
            size: total_size,
            executable,
            file: file_id,
        };
        Ok(MergeFileResult::Merged {
            file: file_entry,
            complete: upload_result.complete,
        })
    }
}

// =============================================================================
// Download and Binary Detection
// =============================================================================

/// Result of downloading a file with binary check.
enum DownloadResult {
    /// File data (not binary).
    Data(Vec<u8>),
    /// File is binary.
    Binary,
    /// No file (base was None).
    Empty,
}

impl DownloadResult {
    fn into_bytes(self) -> Vec<u8> {
        match self {
            DownloadResult::Data(bytes) => bytes,
            DownloadResult::Binary => Vec::new(),
            DownloadResult::Empty => Vec::new(),
        }
    }
}

/// Download all files concurrently, checking the first chunk of each for binary content.
async fn download_and_check_binary(
    repo: &Arc<Repo>,
    base: Option<&FileEntry>,
    file_1: &FileEntry,
    file_2: &FileEntry,
) -> Result<(DownloadResult, DownloadResult, DownloadResult)> {
    if let Some(base_entry) = base {
        let (base_result, f1_result, f2_result) = tokio::try_join!(
            download_file_with_binary_check(repo, &base_entry.file),
            download_file_with_binary_check(repo, &file_1.file),
            download_file_with_binary_check(repo, &file_2.file),
        )?;
        Ok((base_result, f1_result, f2_result))
    } else {
        let (f1_result, f2_result) = tokio::try_join!(
            download_file_with_binary_check(repo, &file_1.file),
            download_file_with_binary_check(repo, &file_2.file),
        )?;
        Ok((DownloadResult::Empty, f1_result, f2_result))
    }
}

/// Download a file's chunks, checking the first chunk for binary content.
///
/// Returns `DownloadResult::Binary` if the first chunk is detected as binary,
/// otherwise returns all file bytes concatenated.
async fn download_file_with_binary_check(
    repo: &Arc<Repo>,
    file_id: &ObjectId,
) -> Result<DownloadResult> {
    let mut chunks = repo.read_file_chunks_with_content(file_id).await?;
    let mut bytes = Vec::new();

    // Get first chunk and check for binary
    if let Some(first_chunk) = chunks.next().await? {
        let content = first_chunk.content().await?;
        let first_bytes: &[u8] = content.as_ref();

        if content_inspector::inspect(first_bytes).is_binary() {
            return Ok(DownloadResult::Binary);
        }

        bytes.extend_from_slice(first_bytes);
    } else {
        // Empty file
        return Ok(DownloadResult::Data(Vec::new()));
    }

    // Download remaining chunks
    while let Some(chunk) = chunks.next().await? {
        let content = chunk.content().await?;
        bytes.extend_from_slice(content.as_ref());
    }

    Ok(DownloadResult::Data(bytes))
}

// =============================================================================
// Line Splitting and Formatting
// =============================================================================

/// Split text into lines, preserving line endings.
///
/// Each element includes its trailing newline (if present), which is important
/// for merge3 to produce correct output.
fn split_lines(text: &str) -> Vec<&str> {
    if text.is_empty() {
        return Vec::new();
    }

    let mut lines = Vec::new();
    let mut start = 0;

    for (i, c) in text.char_indices() {
        if c == '\n' {
            lines.push(&text[start..=i]);
            start = i + 1;
        }
    }

    // Handle last line without trailing newline
    if start < text.len() {
        lines.push(&text[start..]);
    }

    lines
}

/// Convert a slice of line references to bytes.
fn lines_to_bytes(lines: &[&str]) -> Vec<u8> {
    let mut bytes = Vec::new();
    for line in lines {
        bytes.extend_from_slice(line.as_bytes());
    }
    bytes
}

/// Format a conflict group with standard conflict markers.
fn format_conflict(
    base_lines: &Option<&[&str]>,
    a_lines: &[&str],
    b_lines: &[&str],
    name: &str,
) -> Vec<u8> {
    let mut bytes = Vec::new();

    // <<<<<<< file_1
    bytes.extend_from_slice(format!("<<<<<<< {}_1\n", name).as_bytes());
    for line in a_lines {
        bytes.extend_from_slice(line.as_bytes());
        if !line.ends_with('\n') {
            bytes.push(b'\n');
        }
    }

    // ||||||| base (if base exists)
    if let Some(base) = base_lines {
        bytes.extend_from_slice(b"||||||| base\n");
        for line in *base {
            bytes.extend_from_slice(line.as_bytes());
            if !line.ends_with('\n') {
                bytes.push(b'\n');
            }
        }
    }

    // =======
    bytes.extend_from_slice(b"=======\n");
    for line in b_lines {
        bytes.extend_from_slice(line.as_bytes());
        if !line.ends_with('\n') {
            bytes.push(b'\n');
        }
    }

    // >>>>>>> file_2
    bytes.extend_from_slice(format!(">>>>>>> {}_2\n", name).as_bytes());

    bytes
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::MemoryBackend;
    use crate::caches::NoopCache;
    use crate::util::ManagedBuffers;

    fn create_test_repo() -> Arc<Repo> {
        let backend = MemoryBackend::new();
        let cache = NoopCache;
        Arc::new(Repo::new(backend, cache))
    }

    async fn write_file(repo: &Arc<Repo>, content: &[u8]) -> (ObjectId, u64) {
        let managed_buffers = ManagedBuffers::new();
        let mut uploader = StreamingFileUploader::new(Arc::clone(repo));
        let buf = managed_buffers.get_buffer_with_data(content.to_vec()).await;
        uploader.add(buf).await.unwrap();
        let result = uploader.finish().await.unwrap();
        result.complete.complete().await.unwrap();
        let hash = result.result.hash;
        let size = content.len() as u64;
        (hash, size)
    }

    fn make_file_entry(name: &str, file_id: ObjectId, size: u64) -> FileEntry {
        FileEntry {
            name: name.to_string(),
            size,
            executable: false,
            file: file_id,
        }
    }

    fn make_context(name: &str) -> ConflictContext {
        use crate::merge::types::{ChangingDirEntry, DirEntryChange};
        ConflictContext {
            name: name.to_string(),
            base: ChangingDirEntry::None,
            entry_1: ChangingDirEntry::None,
            entry_2: ChangingDirEntry::None,
            change_1: DirEntryChange::Unchanged,
            change_2: DirEntryChange::Unchanged,
            merge_file_result: None,
        }
    }

    #[tokio::test]
    async fn test_clean_merge_different_sections() {
        let repo = create_test_repo();

        let base_content = b"line1\nline2\nline3\n";
        let file_1_content = b"line1_modified\nline2\nline3\n";
        let file_2_content = b"line1\nline2\nline3_modified\n";

        let (base_id, base_size) = write_file(&repo, base_content).await;
        let (f1_id, f1_size) = write_file(&repo, file_1_content).await;
        let (f2_id, f2_size) = write_file(&repo, file_2_content).await;

        let base_entry = make_file_entry("test.txt", base_id, base_size);
        let f1_entry = make_file_entry("test.txt", f1_id, f1_size);
        let f2_entry = make_file_entry("test.txt", f2_id, f2_size);

        let result = merge_files(
            repo,
            "test.txt",
            Some(&base_entry),
            &f1_entry,
            &f2_entry,
            false,
            1024 * 1024 * 1024,
            make_context("test.txt"),
        )
        .await
        .unwrap();

        match result {
            MergeFileResult::Merged { file, complete } => {
                complete.complete().await.unwrap();
                assert_eq!(file.name, "test.txt");
                assert!(!file.executable);
            }
            MergeFileResult::Conflict(_) => panic!("expected clean merge, got conflict"),
        }
    }

    #[tokio::test]
    async fn test_conflict_merge_same_line() {
        let repo = create_test_repo();

        let base_content = b"line1\nline2\nline3\n";
        let file_1_content = b"line1_a\nline2\nline3\n";
        let file_2_content = b"line1_b\nline2\nline3\n";

        let (base_id, base_size) = write_file(&repo, base_content).await;
        let (f1_id, f1_size) = write_file(&repo, file_1_content).await;
        let (f2_id, f2_size) = write_file(&repo, file_2_content).await;

        let base_entry = make_file_entry("test.txt", base_id, base_size);
        let f1_entry = make_file_entry("test.txt", f1_id, f1_size);
        let f2_entry = make_file_entry("test.txt", f2_id, f2_size);

        let result = merge_files(
            repo,
            "test.txt",
            Some(&base_entry),
            &f1_entry,
            &f2_entry,
            false,
            1024 * 1024 * 1024,
            make_context("test.txt"),
        )
        .await
        .unwrap();

        match result {
            MergeFileResult::Merged { .. } => panic!("expected conflict, got clean merge"),
            MergeFileResult::Conflict(ctx) => {
                assert!(matches!(
                    ctx.merge_file_result,
                    Some(MergeFileOutcome::MergeConflict { .. })
                ));
            }
        }
    }

    #[tokio::test]
    async fn test_binary_detection() {
        let repo = create_test_repo();

        let base_content = b"normal text\n";
        let file_1_content = b"\x00\x01\x02\x03binary content";
        let file_2_content = b"other text\n";

        let (base_id, base_size) = write_file(&repo, base_content).await;
        let (f1_id, f1_size) = write_file(&repo, file_1_content).await;
        let (f2_id, f2_size) = write_file(&repo, file_2_content).await;

        let base_entry = make_file_entry("test.bin", base_id, base_size);
        let f1_entry = make_file_entry("test.bin", f1_id, f1_size);
        let f2_entry = make_file_entry("test.bin", f2_id, f2_size);

        let result = merge_files(
            repo,
            "test.bin",
            Some(&base_entry),
            &f1_entry,
            &f2_entry,
            false,
            1024 * 1024 * 1024,
            make_context("test.bin"),
        )
        .await
        .unwrap();

        match result {
            MergeFileResult::Merged { .. } => panic!("expected binary conflict"),
            MergeFileResult::Conflict(ctx) => {
                assert_eq!(ctx.merge_file_result, Some(MergeFileOutcome::Binary));
            }
        }
    }

    #[tokio::test]
    async fn test_too_large() {
        let repo = create_test_repo();

        let base_content = b"base\n";
        let file_1_content = b"file1\n";
        let file_2_content = b"file2\n";

        let (base_id, base_size) = write_file(&repo, base_content).await;
        let (f1_id, f1_size) = write_file(&repo, file_1_content).await;
        let (f2_id, f2_size) = write_file(&repo, file_2_content).await;

        let base_entry = make_file_entry("test.txt", base_id, base_size);
        let f1_entry = make_file_entry("test.txt", f1_id, f1_size);
        let f2_entry = make_file_entry("test.txt", f2_id, f2_size);

        // Set max_merge_memory to 1 byte — everything is too large
        let result = merge_files(
            repo,
            "test.txt",
            Some(&base_entry),
            &f1_entry,
            &f2_entry,
            false,
            1,
            make_context("test.txt"),
        )
        .await
        .unwrap();

        match result {
            MergeFileResult::Merged { .. } => panic!("expected too large conflict"),
            MergeFileResult::Conflict(ctx) => {
                assert_eq!(ctx.merge_file_result, Some(MergeFileOutcome::TooLarge));
            }
        }
    }

    #[tokio::test]
    async fn test_no_base_clean_merge() {
        let repo = create_test_repo();

        // Both sides created the same file — should merge cleanly
        let file_1_content = b"same content\n";
        let file_2_content = b"same content\n";

        let (f1_id, f1_size) = write_file(&repo, file_1_content).await;
        let (f2_id, f2_size) = write_file(&repo, file_2_content).await;

        let f1_entry = make_file_entry("test.txt", f1_id, f1_size);
        let f2_entry = make_file_entry("test.txt", f2_id, f2_size);

        let result = merge_files(
            repo,
            "test.txt",
            None,
            &f1_entry,
            &f2_entry,
            false,
            1024 * 1024 * 1024,
            make_context("test.txt"),
        )
        .await
        .unwrap();

        match result {
            MergeFileResult::Merged { file, complete } => {
                complete.complete().await.unwrap();
                assert_eq!(file.name, "test.txt");
            }
            MergeFileResult::Conflict(_) => panic!("expected clean merge, got conflict"),
        }
    }

    #[tokio::test]
    async fn test_empty_files() {
        let repo = create_test_repo();

        let (base_id, base_size) = write_file(&repo, b"").await;
        let (f1_id, f1_size) = write_file(&repo, b"").await;
        let (f2_id, f2_size) = write_file(&repo, b"").await;

        let base_entry = make_file_entry("test.txt", base_id, base_size);
        let f1_entry = make_file_entry("test.txt", f1_id, f1_size);
        let f2_entry = make_file_entry("test.txt", f2_id, f2_size);

        let result = merge_files(
            repo,
            "test.txt",
            Some(&base_entry),
            &f1_entry,
            &f2_entry,
            false,
            1024 * 1024 * 1024,
            make_context("test.txt"),
        )
        .await
        .unwrap();

        match result {
            MergeFileResult::Merged { file, complete } => {
                complete.complete().await.unwrap();
                assert_eq!(file.size, 0);
            }
            MergeFileResult::Conflict(_) => panic!("expected clean merge for empty files"),
        }
    }

    #[test]
    fn test_split_lines() {
        assert_eq!(split_lines(""), Vec::<&str>::new());
        assert_eq!(split_lines("a\nb\n"), vec!["a\n", "b\n"]);
        assert_eq!(split_lines("a\nb"), vec!["a\n", "b"]);
        assert_eq!(split_lines("single"), vec!["single"]);
        assert_eq!(split_lines("\n"), vec!["\n"]);
    }

    #[test]
    fn test_format_conflict_with_base() {
        let base = vec!["base_line\n"];
        let a = vec!["a_line\n"];
        let b = vec!["b_line\n"];

        let bytes = format_conflict(&Some(base.as_slice()), &a, &b, "test.txt");
        let text = String::from_utf8(bytes).unwrap();

        assert!(text.contains("<<<<<<< test.txt_1"));
        assert!(text.contains("a_line"));
        assert!(text.contains("||||||| base"));
        assert!(text.contains("base_line"));
        assert!(text.contains("======="));
        assert!(text.contains("b_line"));
        assert!(text.contains(">>>>>>> test.txt_2"));
    }

    #[test]
    fn test_format_conflict_without_base() {
        let a = vec!["a_line\n"];
        let b = vec!["b_line\n"];

        let bytes = format_conflict(&None, &a, &b, "test.txt");
        let text = String::from_utf8(bytes).unwrap();

        assert!(text.contains("<<<<<<< test.txt_1"));
        assert!(text.contains("a_line"));
        assert!(!text.contains("||||||| base"));
        assert!(text.contains("======="));
        assert!(text.contains("b_line"));
        assert!(text.contains(">>>>>>> test.txt_2"));
    }
}
