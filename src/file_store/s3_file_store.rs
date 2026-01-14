//! S3-based FileSource implementation.
//!
//! S3FileSource provides read access to an S3 bucket as a FileSource,
//! treating object keys as paths with "/" as the directory separator.

use super::chunk_sizes::next_chunk_size;
use super::scan_ignore_helper::ScanIgnoreHelper;
use crate::file_store::{
    DirEntry, DirectoryEntry, DirectoryScanEvent, Error, FileEntry, FileSource, Result, ScanEvent,
    ScanEvents, SourceChunk, SourceChunkContent, SourceChunkContents, SourceChunks,
};
use crate::repo::FlowControl;
use crate::util::ManagedBuffers;
use async_trait::async_trait;
use aws_config::BehaviorVersion;
use aws_sdk_s3::Client;
use bytes::Bytes;
use futures::{StreamExt, stream};
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
// SourceChunk Implementation
// =============================================================================

/// A chunk from an S3 object.
struct S3SourceChunk {
    client: Client,
    bucket: String,
    key: String,
    offset: u64,
    size: u64,
    managed_buffers: ManagedBuffers,
}

#[async_trait]
impl SourceChunk for S3SourceChunk {
    fn offset(&self) -> u64 {
        self.offset
    }

    fn size(&self) -> u64 {
        self.size
    }

    async fn get(&self) -> Result<SourceChunkContent> {
        let range = format!("bytes={}-{}", self.offset, self.offset + self.size - 1);

        let response = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(&self.key)
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

        let managed_buffer = self
            .managed_buffers
            .get_buffer_with_data(bytes.to_vec())
            .await;

        Ok(SourceChunkContent {
            offset: self.offset,
            size: self.size,
            bytes: Arc::new(managed_buffer),
            hash,
        })
    }
}

// =============================================================================
// FileSource Implementation
// =============================================================================

#[async_trait]
impl FileSource for S3FileSource {
    async fn scan(&self) -> Result<ScanEvents> {
        let source = Arc::new(ScanSourceAdapter {
            client: self.client.clone(),
            config: self.config.clone(),
        });

        let mut events = Vec::new();
        let mut helper = ScanIgnoreHelper::new();

        // Process root to load top-level ignore files
        let root_entry = DirEntry {
            name: String::new(),
            path: String::new(),
        };
        helper
            .on_scan_event(
                &DirectoryScanEvent::EnterDirectory(root_entry),
                source.as_ref(),
            )
            .await;

        // Start scanning from root
        self.scan_directory("", &mut events, &mut helper, source.as_ref())
            .await?;

        Ok(Box::pin(stream::iter(events.into_iter().map(Ok))))
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

        // Create lazy stream for chunks
        let client = self.client.clone();
        let bucket = self.config.bucket.clone();
        let managed_buffers = self.managed_buffers.clone();

        struct ChunkIterState {
            client: Client,
            bucket: String,
            key: String,
            file_size: u64,
            offset: u64,
            managed_buffers: ManagedBuffers,
        }

        let chunks_stream = stream::unfold(
            ChunkIterState {
                client,
                bucket,
                key,
                file_size,
                offset: 0,
                managed_buffers,
            },
            |state| async move {
                if state.offset >= state.file_size {
                    return None;
                }

                let remaining = state.file_size - state.offset;
                let chunk_size = next_chunk_size(remaining);

                let chunk = S3SourceChunk {
                    client: state.client.clone(),
                    bucket: state.bucket.clone(),
                    key: state.key.clone(),
                    offset: state.offset,
                    size: chunk_size,
                    managed_buffers: state.managed_buffers.clone(),
                };

                let next_state = ChunkIterState {
                    client: state.client,
                    bucket: state.bucket,
                    key: state.key,
                    file_size: state.file_size,
                    offset: state.offset + chunk_size,
                    managed_buffers: state.managed_buffers,
                };

                Some((Ok(Box::new(chunk) as Box<dyn SourceChunk>), next_state))
            },
        );

        Ok(Some(Box::pin(chunks_stream)))
    }

    async fn get_source_chunk_contents(&self, chunks: SourceChunks) -> Result<SourceChunkContents> {
        // Concurrent implementation with flow control.
        // We use buffered() to fetch chunks concurrently but yield them in order.
        let flow_control = self.flow_control.clone();

        // Default concurrency limit if not specified by flow control
        const DEFAULT_CONCURRENCY: usize = 10;

        let contents_stream = chunks
            .map(move |chunk_result| {
                let flow_control = flow_control.clone();
                async move {
                    let chunk = chunk_result?;

                    // Acquire flow control capacity before fetching
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

                    let size = chunk.size();

                    // Acquire read throughput capacity
                    let _read = if let Some(ref limiter) = flow_control.read_throughput_limiter {
                        Some(limiter.use_capacity(size).await)
                    } else {
                        None
                    };
                    let _total = if let Some(ref limiter) = flow_control.total_throughput_limiter {
                        Some(limiter.use_capacity(size).await)
                    } else {
                        None
                    };

                    // Fetch the chunk content
                    chunk.get().await
                }
            })
            .buffered(DEFAULT_CONCURRENCY);

        Ok(Box::pin(contents_stream))
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
        source: &dyn FileSource,
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
                        .on_scan_event(&DirectoryScanEvent::EnterDirectory(dir.clone()), source)
                        .await;

                    Box::pin(self.scan_directory(&dir.path, events, helper, source)).await?;

                    helper
                        .on_scan_event(&DirectoryScanEvent::ExitDirectory, source)
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
struct ScanSourceAdapter {
    client: Client,
    config: S3FileSourceConfig,
}

#[async_trait]
impl FileSource for ScanSourceAdapter {
    async fn scan(&self) -> Result<ScanEvents> {
        // Not used by ScanIgnoreHelper
        Ok(Box::pin(stream::empty()))
    }

    async fn get_source_chunks(&self, _path: &Path) -> Result<Option<SourceChunks>> {
        // Not used by ScanIgnoreHelper
        Ok(None)
    }

    async fn get_source_chunk_contents(&self, chunks: SourceChunks) -> Result<SourceChunkContents> {
        // Not used by ScanIgnoreHelper - provide simple sequential implementation
        let contents_stream = chunks.then(|chunk_result| async move {
            let chunk = chunk_result?;
            chunk.get().await
        });
        Ok(Box::pin(contents_stream))
    }

    async fn get_entry(&self, path: &Path) -> Result<Option<DirectoryEntry>> {
        let key = {
            let path_str = path.to_string_lossy();
            match &self.config.prefix {
                Some(prefix) if !path_str.is_empty() => format!("{}/{}", prefix, path_str),
                Some(prefix) => prefix.clone(),
                None => path_str.into_owned(),
            }
        };

        // Try as file first
        let head = self
            .client
            .head_object()
            .bucket(&self.config.bucket)
            .key(&key)
            .send()
            .await;

        if let Ok(metadata) = head {
            let path_str = path.to_string_lossy().into_owned();
            let name = path
                .file_name()
                .map(|s| s.to_string_lossy().into_owned())
                .unwrap_or_default();
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

        Ok(None)
    }

    async fn get_file(&self, path: &Path) -> Result<Bytes> {
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
            .map_err(|e| Error::Other(e.to_string()))?;

        let body = response.body.collect().await;
        let aggregated = body.map_err(|e| Error::Other(e.to_string()))?;
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
