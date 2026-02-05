//! S3-based FileSource implementation.
//!
//! S3FileSource provides read access to an S3 bucket as a FileSource,
//! treating object keys as paths with "/" as the directory separator.

use super::chunk_sizes::next_chunk_size;
use super::scan_ignore_helper::{
    ScanDirEntry, ScanDirectoryEvent, ScanFileSource, ScanIgnoreHelper,
};
use crate::file_store::{
    DirEntry, DirectoryEntry, Error, FileEntry, FileSource, Result, ScanEvent, ScanEvents,
    SourceChunk, SourceChunkContent, SourceChunkList, SourceChunkWithContent,
    SourceChunkWithContentList, SourceChunks, SourceChunksWithContent, VecScanEventList,
};
use crate::repo::FlowControl;
use crate::util::ManagedBuffers;
use async_trait::async_trait;
use aws_config::BehaviorVersion;
use aws_sdk_s3::Client;
use bytes::Bytes;
use sha2::{Digest, Sha256};
use std::path::Path;
use std::sync::Arc;

// =============================================================================
// Configuration
// =============================================================================

/// Configuration for S3FileSource.
#[derive(Debug, Clone)]
pub struct S3FileSourceConfig {
    /// S3 bucket name.
    pub bucket: String,
    /// Optional prefix within the bucket.
    pub prefix: Option<String>,
    /// Optional custom endpoint URL (for LocalStack, MinIO, etc.).
    pub endpoint_url: Option<String>,
    /// Optional region override.
    pub region: Option<String>,
}

impl S3FileSourceConfig {
    /// Create a new S3FileSourceConfig with the given bucket name.
    pub fn new(bucket: impl Into<String>) -> Self {
        Self {
            bucket: bucket.into(),
            prefix: None,
            endpoint_url: None,
            region: None,
        }
    }

    /// Set an optional prefix within the bucket.
    pub fn with_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.prefix = Some(prefix.into());
        self
    }

    /// Set a custom endpoint URL (for LocalStack, MinIO, etc.).
    pub fn with_endpoint_url(mut self, url: impl Into<String>) -> Self {
        self.endpoint_url = Some(url.into());
        self
    }

    /// Set a region override.
    pub fn with_region(mut self, region: impl Into<String>) -> Self {
        self.region = Some(region.into());
        self
    }

    /// Generate the cache URL for this configuration.
    pub fn cache_url(&self) -> String {
        match &self.prefix {
            Some(prefix) => format!("s3://{}/{}", self.bucket, prefix),
            None => format!("s3://{}", self.bucket),
        }
    }
}

// =============================================================================
// S3FileSource
// =============================================================================

/// A FileSource backed by an S3 bucket.
pub struct S3FileSource {
    client: Client,
    config: S3FileSourceConfig,
    /// Flow control for rate limiting and concurrency.
    flow_control: FlowControl,
    /// Buffer manager for chunk allocation.
    managed_buffers: ManagedBuffers,
}

impl S3FileSource {
    /// Create a new S3FileSource with the given configuration.
    pub async fn new(
        config: S3FileSourceConfig,
        flow_control: FlowControl,
        managed_buffers: ManagedBuffers,
    ) -> Self {
        let mut aws_config_loader = aws_config::defaults(BehaviorVersion::latest());

        if let Some(ref region) = config.region {
            aws_config_loader =
                aws_config_loader.region(aws_sdk_s3::config::Region::new(region.clone()));
        }

        let aws_config = aws_config_loader.load().await;

        let mut s3_config_builder = aws_sdk_s3::config::Builder::from(&aws_config);

        if let Some(ref endpoint) = config.endpoint_url {
            s3_config_builder = s3_config_builder
                .endpoint_url(endpoint)
                .force_path_style(true);
        }

        let client = Client::from_conf(s3_config_builder.build());

        Self {
            client,
            config,
            flow_control,
            managed_buffers,
        }
    }

    /// Get the cache URL for this file source.
    pub fn cache_url(&self) -> String {
        self.config.cache_url()
    }

    /// Convert a relative path to an S3 key.
    fn to_key(&self, relative: &Path) -> String {
        let path_str = relative.to_string_lossy();
        match &self.config.prefix {
            Some(prefix) if !path_str.is_empty() => format!("{}/{}", prefix, path_str),
            Some(prefix) => prefix.clone(),
            None => path_str.into_owned(),
        }
    }

    /// Convert an S3 key to a relative path.
    fn key_to_relative(&self, key: &str) -> String {
        match &self.config.prefix {
            Some(prefix) => {
                let stripped = key.strip_prefix(prefix).unwrap_or(key);
                stripped.strip_prefix('/').unwrap_or(stripped).to_string()
            }
            None => key.to_string(),
        }
    }

    /// Extract the name (last component) from a path or key.
    fn name_from_path(path: &str) -> String {
        path.trim_end_matches('/')
            .rsplit('/')
            .next()
            .unwrap_or(path)
            .to_string()
    }

    /// List objects at a given prefix, returning entries.
    async fn list_at_prefix(&self, prefix: &str) -> Result<Vec<DirectoryEntry>> {
        let mut entries = Vec::new();
        let mut continuation_token: Option<String> = None;

        loop {
            let mut request = self
                .client
                .list_objects_v2()
                .bucket(&self.config.bucket)
                .delimiter("/");

            if !prefix.is_empty() {
                request = request.prefix(prefix);
            }

            if let Some(token) = continuation_token.take() {
                request = request.continuation_token(token);
            }

            let response = request
                .send()
                .await
                .map_err(|e| Error::Other(e.to_string()))?;

            // Process common prefixes (directories)
            for cp in response.common_prefixes() {
                if let Some(p) = cp.prefix() {
                    let relative = self.key_to_relative(p);
                    let name = Self::name_from_path(&relative);
                    entries.push(DirectoryEntry::Dir(DirEntry {
                        name,
                        path: relative.trim_end_matches('/').to_string(),
                    }));
                }
            }

            // Process objects (files)
            for obj in response.contents() {
                if let Some(key) = obj.key() {
                    // Skip the prefix itself if it appears as an object
                    if key.ends_with('/') {
                        continue;
                    }
                    // Skip if this is exactly the prefix (happens when prefix is a "directory marker")
                    if key == prefix.trim_end_matches('/') {
                        continue;
                    }

                    let relative = self.key_to_relative(key);
                    let name = Self::name_from_path(&relative);
                    let size = obj.size().unwrap_or(0) as u64;
                    let fingerprint = obj.e_tag().and_then(|etag| {
                        let etag = etag.trim_matches('"');
                        if etag.len() <= 128 {
                            Some(etag.to_string())
                        } else {
                            None
                        }
                    });

                    entries.push(DirectoryEntry::File(FileEntry {
                        name,
                        path: relative,
                        size,
                        executable: false, // S3 doesn't track permissions
                        fingerprint,
                    }));
                }
            }

            if response.is_truncated() == Some(true) {
                continuation_token = response.next_continuation_token().map(|s| s.to_string());
            } else {
                break;
            }
        }

        // Sort entries by name for lexicographic order
        entries.sort_by(|a, b| {
            let name_a = match a {
                DirectoryEntry::Dir(d) => &d.name,
                DirectoryEntry::File(f) => &f.name,
            };
            let name_b = match b {
                DirectoryEntry::Dir(d) => &d.name,
                DirectoryEntry::File(f) => &f.name,
            };
            name_a.cmp(name_b)
        });

        Ok(entries)
    }

    /// Check if an S3 error indicates the object was not found.
    fn is_not_found_error(err: &aws_sdk_s3::operation::head_object::HeadObjectError) -> bool {
        matches!(
            err,
            aws_sdk_s3::operation::head_object::HeadObjectError::NotFound(_)
        )
    }
}

// =============================================================================
// Chunk List Implementation
// =============================================================================

/// SourceChunkList implementation for S3FileSource.
///
/// Holds the S3 client, bucket, key, and chunk metadata so that
/// get_source_chunks_with_content can use it.
pub struct S3SourceChunkList {
    client: Client,
    bucket: String,
    key: String,
    chunks: Vec<SourceChunk>,
    index: usize,
    flow_control: FlowControl,
}

impl S3SourceChunkList {
    fn new(
        client: Client,
        bucket: String,
        key: String,
        file_size: u64,
        flow_control: FlowControl,
    ) -> Self {
        // Compute all chunk metadata upfront
        let mut chunks = Vec::new();
        let mut offset = 0u64;
        while offset < file_size {
            let remaining = file_size - offset;
            let chunk_size = next_chunk_size(remaining);
            chunks.push(SourceChunk {
                offset,
                size: chunk_size,
            });
            offset += chunk_size;
        }

        Self {
            client,
            bucket,
            key,
            chunks,
            index: 0,
            flow_control,
        }
    }
}

#[async_trait]
impl SourceChunkList for S3SourceChunkList {
    async fn next(&mut self) -> Option<Result<SourceChunk>> {
        if self.index < self.chunks.len() {
            let chunk = self.chunks[self.index].clone();
            self.index += 1;
            Some(Ok(chunk))
        } else {
            None
        }
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

// =============================================================================
// Chunk Content Implementation (with deadlock-safe ordering)
// =============================================================================

/// Default concurrency limit for parallel downloads.
const DEFAULT_CONCURRENCY: usize = 10;

/// Downloaded chunk data (before being wrapped in a ManagedBuffer).
struct DownloadedChunk {
    offset: u64,
    size: u64,
    data: Vec<u8>,
    hash: String,
}

/// SourceChunkWithContentList for S3 with concurrent downloads and deadlock prevention.
///
/// This implementation:
/// 1. Downloads happen in the background WITHOUT acquiring ManagedBuffers
/// 2. When next() is called, we acquire a ManagedBuffer IN ORDER
/// 3. Then wait for the download to complete and wrap in the buffer
///
/// This ordering prevents deadlock because buffers are always acquired
/// in chunk order during next() calls, not in the download tasks.
struct S3SourceChunkWithContentList {
    client: Client,
    bucket: String,
    key: String,
    chunks: Vec<SourceChunk>,
    index: usize,
    managed_buffers: ManagedBuffers,
    flow_control: FlowControl,
    /// Background download tasks that have been spawned.
    /// Downloads happen WITHOUT buffer acquisition to avoid deadlock.
    pending_downloads: Vec<Option<tokio::task::JoinHandle<Result<DownloadedChunk>>>>,
}

impl S3SourceChunkWithContentList {
    fn new(
        client: Client,
        bucket: String,
        key: String,
        chunks: Vec<SourceChunk>,
        managed_buffers: ManagedBuffers,
        flow_control: FlowControl,
    ) -> Self {
        let len = chunks.len();
        Self {
            client,
            bucket,
            key,
            chunks,
            index: 0,
            managed_buffers,
            flow_control,
            pending_downloads: (0..len).map(|_| None).collect(),
        }
    }

    /// Spawn background downloads for chunks that haven't been started yet,
    /// up to the concurrency limit ahead of the current index.
    /// IMPORTANT: Downloads do NOT acquire ManagedBuffers - that happens in next().
    fn spawn_pending_downloads(&mut self) {
        let start = self.index;
        let end = (self.index + DEFAULT_CONCURRENCY).min(self.chunks.len());

        for i in start..end {
            if self.pending_downloads[i].is_none() {
                // Spawn a download task for this chunk
                let client = self.client.clone();
                let bucket = self.bucket.clone();
                let key = self.key.clone();
                let chunk = self.chunks[i].clone();
                let flow_control = self.flow_control.clone();

                let handle = tokio::spawn(async move {
                    // Apply flow control (but NOT buffer acquisition!)
                    let _rate = if let Some(ref limiter) = flow_control.request_rate_limiter {
                        Some(limiter.use_capacity(1).await)
                    } else {
                        None
                    };
                    let _concurrent =
                        if let Some(ref limiter) = flow_control.concurrent_request_limiter {
                            Some(limiter.use_capacity(1).await)
                        } else {
                            None
                        };
                    let _read = if let Some(ref limiter) = flow_control.read_throughput_limiter {
                        Some(limiter.use_capacity(chunk.size).await)
                    } else {
                        None
                    };
                    let _total = if let Some(ref limiter) = flow_control.total_throughput_limiter {
                        Some(limiter.use_capacity(chunk.size).await)
                    } else {
                        None
                    };

                    // Download the chunk (without buffer acquisition)
                    let range = format!("bytes={}-{}", chunk.offset, chunk.offset + chunk.size - 1);

                    let response = client
                        .get_object()
                        .bucket(&bucket)
                        .key(&key)
                        .range(range)
                        .send()
                        .await
                        .map_err(|e| Error::Other(e.to_string()))?;

                    let body = response.body.collect().await;
                    let aggregated = body.map_err(|e| Error::Other(e.to_string()))?;
                    let bytes = aggregated.into_bytes();

                    let hash = {
                        let mut hasher = Sha256::new();
                        hasher.update(&bytes);
                        format!("{:x}", hasher.finalize())
                    };

                    Ok(DownloadedChunk {
                        offset: chunk.offset,
                        size: chunk.size,
                        data: bytes.to_vec(),
                        hash,
                    })
                });

                self.pending_downloads[i] = Some(handle);
            }
        }
    }
}

#[async_trait]
impl SourceChunkWithContentList for S3SourceChunkWithContentList {
    async fn next(&mut self) -> Option<Result<SourceChunkWithContent>> {
        if self.index >= self.chunks.len() {
            return None;
        }

        let chunk_index = self.index;
        let chunk = self.chunks[chunk_index].clone();
        self.index += 1;

        // First, spawn any pending downloads up to concurrency limit
        // (downloads do NOT acquire buffers)
        self.spawn_pending_downloads();

        // KEY DEADLOCK FIX: Acquire the ManagedBuffer HERE, in order.
        // This is what prevents the deadlock - buffers are acquired
        // strictly in chunk order during next() calls, not in download tasks.

        // Wait for this chunk's download to complete
        let downloaded = if let Some(handle) = self.pending_downloads[chunk_index].take() {
            match handle.await {
                Ok(Ok(downloaded)) => downloaded,
                Ok(Err(e)) => return Some(Err(e)),
                Err(e) => return Some(Err(Error::Other(format!("Download task failed: {}", e)))),
            }
        } else {
            // Download wasn't started - this shouldn't happen
            return Some(Err(Error::Other("Download not started".to_string())));
        };

        // NOW acquire the buffer (in order!) and wrap the downloaded data
        let managed_buffer = self
            .managed_buffers
            .get_buffer_with_data(downloaded.data)
            .await;

        let content = SourceChunkContent {
            offset: downloaded.offset,
            size: downloaded.size,
            bytes: Arc::new(managed_buffer),
            hash: downloaded.hash,
        };

        Some(Ok(SourceChunkWithContent::new_immediate(
            chunk.offset,
            chunk.size,
            content,
        )))
    }
}

// =============================================================================
// FileSource Implementation
// =============================================================================

#[async_trait]
impl FileSource for S3FileSource {
    async fn scan(&self, path: Option<&Path>) -> Result<ScanEvents> {
        // Determine the starting prefix
        let start_prefix = match path {
            Some(p) => {
                let path_str = p.to_string_lossy();
                if path_str.is_empty() {
                    String::new()
                } else {
                    // Check if the path is a directory by listing with prefix
                    let check_prefix = match &self.config.prefix {
                        Some(pfx) => format!("{}/{}/", pfx, path_str),
                        None => format!("{}/", path_str),
                    };
                    let response = self
                        .client
                        .list_objects_v2()
                        .bucket(&self.config.bucket)
                        .prefix(&check_prefix)
                        .max_keys(1)
                        .send()
                        .await
                        .map_err(|e| Error::Other(e.to_string()))?;

                    let has_contents =
                        !response.contents().is_empty() || !response.common_prefixes().is_empty();

                    if !has_contents {
                        // Path doesn't exist as a directory in S3
                        return Err(Error::NotADirectory(path_str.into_owned()));
                    }
                    path_str.into_owned()
                }
            }
            None => String::new(),
        };

        let source = Arc::new(ScanSourceAdapter {
            client: self.client.clone(),
            config: self.config.clone(),
        });

        let mut events = Vec::new();
        let mut helper = ScanIgnoreHelper::new();

        // Initialize helper by walking from root to the target path,
        // loading ignore files along the way
        helper.initialize_to_path(path, source.as_ref()).await;

        // Start scanning from the specified prefix
        self.scan_directory(&start_prefix, &mut events, &mut helper, source.as_ref())
            .await?;

        Ok(Box::new(VecScanEventList::new(events)))
    }

    async fn get_source_chunks(&self, path: &Path) -> Result<Option<SourceChunks>> {
        let key = self.to_key(path);

        // Get object metadata to check if it exists and get size
        let head = self
            .client
            .head_object()
            .bucket(&self.config.bucket)
            .key(&key)
            .send()
            .await;

        let metadata = match head {
            Ok(m) => m,
            Err(e) => {
                let service_err = e.into_service_error();
                if Self::is_not_found_error(&service_err) {
                    return Ok(None);
                }
                return Err(Error::Other(service_err.to_string()));
            }
        };

        let file_size = metadata.content_length().unwrap_or(0) as u64;

        Ok(Some(Box::new(S3SourceChunkList::new(
            self.client.clone(),
            self.config.bucket.clone(),
            key,
            file_size,
            self.flow_control.clone(),
        ))))
    }

    async fn get_source_chunks_with_content(
        &self,
        chunks: SourceChunks,
        managed_buffers: ManagedBuffers,
    ) -> Result<SourceChunksWithContent> {
        // Try to downcast to S3SourceChunkList to get the S3 context
        if let Some(s3_chunks) = chunks.as_any().downcast_ref::<S3SourceChunkList>() {
            let remaining_chunks = s3_chunks.chunks[s3_chunks.index..].to_vec();

            Ok(Box::new(S3SourceChunkWithContentList::new(
                s3_chunks.client.clone(),
                s3_chunks.bucket.clone(),
                s3_chunks.key.clone(),
                remaining_chunks,
                managed_buffers,
                s3_chunks.flow_control.clone(),
            )))
        } else {
            Err(Error::Other(
                "S3FileSource::get_source_chunks_with_content: chunks must come from this store's get_source_chunks"
                    .to_string(),
            ))
        }
    }

    async fn get_entry(&self, path: &Path) -> Result<Option<DirectoryEntry>> {
        let key = self.to_key(path);

        // First try to get object metadata (file case)
        let head = self
            .client
            .head_object()
            .bucket(&self.config.bucket)
            .key(&key)
            .send()
            .await;

        if let Ok(metadata) = head {
            let path_str = path.to_string_lossy().into_owned();
            let name = Self::name_from_path(&path_str);
            let size = metadata.content_length().unwrap_or(0) as u64;
            let fingerprint = metadata.e_tag().and_then(|etag| {
                let etag = etag.trim_matches('"');
                if etag.len() <= 128 {
                    Some(etag.to_string())
                } else {
                    None
                }
            });

            return Ok(Some(DirectoryEntry::File(FileEntry {
                name,
                path: path_str,
                size,
                executable: false,
                fingerprint,
            })));
        }

        // Check if it's a directory by listing with prefix
        let prefix = if key.is_empty() {
            String::new()
        } else {
            format!("{}/", key)
        };

        let response = self
            .client
            .list_objects_v2()
            .bucket(&self.config.bucket)
            .prefix(&prefix)
            .max_keys(1)
            .send()
            .await
            .map_err(|e| Error::Other(e.to_string()))?;

        let has_contents =
            !response.contents().is_empty() || !response.common_prefixes().is_empty();

        if has_contents || path.as_os_str().is_empty() {
            let path_str = path.to_string_lossy().into_owned();
            let name = Self::name_from_path(&path_str);
            return Ok(Some(DirectoryEntry::Dir(DirEntry {
                name,
                path: path_str,
            })));
        }

        Ok(None)
    }

    async fn get_file(&self, path: &Path) -> Result<Bytes> {
        let key = self.to_key(path);

        let response = self
            .client
            .get_object()
            .bucket(&self.config.bucket)
            .key(&key)
            .send()
            .await
            .map_err(|e| Error::Other(e.to_string()))?;

        let body = response.body.collect().await;
        let aggregated = body.map_err(|e| Error::Other(e.to_string()))?;
        let bytes = aggregated.into_bytes();

        Ok(Bytes::from(bytes.to_vec()))
    }
}

impl S3FileSource {
    /// Recursively scan a directory in S3.
    async fn scan_directory(
        &self,
        prefix: &str,
        events: &mut Vec<ScanEvent>,
        helper: &mut ScanIgnoreHelper,
        source: &dyn ScanFileSource,
    ) -> Result<()> {
        let s3_prefix = match &self.config.prefix {
            Some(p) if !prefix.is_empty() => format!("{}/{}/", p, prefix),
            Some(p) => format!("{}/", p),
            None if !prefix.is_empty() => format!("{}/", prefix),
            None => String::new(),
        };

        let entries = self.list_at_prefix(&s3_prefix).await?;

        for entry in entries {
            match entry {
                DirectoryEntry::Dir(dir) => {
                    if helper.should_ignore(&dir.name, true) {
                        continue;
                    }

                    events.push(ScanEvent::EnterDirectory(dir.clone()));

                    helper
                        .on_scan_event(
                            &ScanDirectoryEvent::EnterDirectory(ScanDirEntry::from(&dir)),
                            source,
                        )
                        .await;

                    Box::pin(self.scan_directory(&dir.path, events, helper, source)).await?;

                    helper
                        .on_scan_event(&ScanDirectoryEvent::ExitDirectory, source)
                        .await;

                    events.push(ScanEvent::ExitDirectory);
                }
                DirectoryEntry::File(file) => {
                    if helper.should_ignore(&file.name, false) {
                        continue;
                    }

                    events.push(ScanEvent::File(file));
                }
            }
        }

        Ok(())
    }
}

// =============================================================================
// Helper adapter for ScanIgnoreHelper
// =============================================================================

/// Adapter to use S3FileSource with ScanIgnoreHelper.
///
/// Implements ScanFileSource directly, providing only the `get_file`
/// method needed by the helper to load ignore files.
struct ScanSourceAdapter {
    client: Client,
    config: S3FileSourceConfig,
}

#[async_trait]
impl ScanFileSource for ScanSourceAdapter {
    async fn get_file(
        &self,
        path: &Path,
    ) -> std::result::Result<Bytes, Box<dyn std::error::Error + Send + Sync>> {
        let key = {
            let path_str = path.to_string_lossy();
            match &self.config.prefix {
                Some(prefix) if !path_str.is_empty() => format!("{}/{}", prefix, path_str),
                Some(prefix) => prefix.clone(),
                None => path_str.into_owned(),
            }
        };

        let response = self
            .client
            .get_object()
            .bucket(&self.config.bucket)
            .key(&key)
            .send()
            .await
            .map_err(|e| {
                Box::new(Error::Other(e.to_string())) as Box<dyn std::error::Error + Send + Sync>
            })?;

        let body = response.body.collect().await;
        let aggregated = body.map_err(|e| {
            Box::new(Error::Other(e.to_string())) as Box<dyn std::error::Error + Send + Sync>
        })?;
        let bytes = aggregated.into_bytes();

        Ok(Bytes::from(bytes.to_vec()))
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_builder() {
        let config = S3FileSourceConfig::new("my-bucket")
            .with_prefix("data/root")
            .with_endpoint_url("http://localhost:4566")
            .with_region("us-west-2");

        assert_eq!(config.bucket, "my-bucket");
        assert_eq!(config.prefix, Some("data/root".to_string()));
        assert_eq!(
            config.endpoint_url,
            Some("http://localhost:4566".to_string())
        );
        assert_eq!(config.region, Some("us-west-2".to_string()));
    }

    #[test]
    fn test_name_from_path() {
        assert_eq!(S3FileSource::name_from_path("foo/bar/baz.txt"), "baz.txt");
        assert_eq!(S3FileSource::name_from_path("foo/bar/"), "bar");
        assert_eq!(S3FileSource::name_from_path("single"), "single");
        assert_eq!(S3FileSource::name_from_path(""), "");
    }
}
