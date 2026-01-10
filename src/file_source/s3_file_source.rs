use std::path::{Path, PathBuf};

use aws_sdk_s3::Client;
use aws_sdk_s3::error::SdkError;

use crate::file_source::directory_list::{DirectoryList, DirectoryListing};
use crate::file_source::error::{FileSourceError, Result};
use crate::file_source::file_chunks::{FileChunking, FileChunks};
use crate::file_source::file_source::FileSource;
use crate::file_source::types::{
    next_chunk_size, DirEntry, DirectoryListEntry, FileChunk, FileEntry,
};

/// Configuration for S3FileSource.
pub struct S3FileSourceConfig {
    /// The S3 bucket name.
    pub bucket: String,
    /// Optional path prefix within the bucket.
    pub prefix: Option<String>,
    /// Optional custom endpoint URL (for LocalStack/MinIO testing).
    pub endpoint_url: Option<String>,
    /// Optional region override.
    pub region: Option<String>,
}

impl S3FileSourceConfig {
    /// Create a new config with just a bucket name.
    pub fn new(bucket: impl Into<String>) -> Self {
        Self {
            bucket: bucket.into(),
            prefix: None,
            endpoint_url: None,
            region: None,
        }
    }

    /// Set the path prefix.
    pub fn with_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.prefix = Some(prefix.into());
        self
    }

    /// Set a custom endpoint URL (for LocalStack/MinIO).
    pub fn with_endpoint_url(mut self, endpoint_url: impl Into<String>) -> Self {
        self.endpoint_url = Some(endpoint_url.into());
        self
    }

    /// Set the region.
    pub fn with_region(mut self, region: impl Into<String>) -> Self {
        self.region = Some(region.into());
        self
    }
}

/// An S3-based implementation of `FileSource`.
///
/// Treats S3 objects as files, using "/" as directory separators.
/// Directory listings and file reads are performed incrementally to handle
/// buckets and files of arbitrary size.
pub struct S3FileSource {
    client: Client,
    bucket: String,
    prefix: Option<String>,
}

impl S3FileSource {
    /// Create a new S3 file source with the given configuration.
    ///
    /// Uses the standard AWS credential chain (env vars, ~/.aws, IAM roles, etc.).
    pub async fn new(config: S3FileSourceConfig) -> Self {
        let mut aws_config_loader =
            aws_config::defaults(aws_config::BehaviorVersion::latest());

        if let Some(region) = &config.region {
            aws_config_loader =
                aws_config_loader.region(aws_config::Region::new(region.clone()));
        }

        if let Some(endpoint_url) = &config.endpoint_url {
            aws_config_loader = aws_config_loader.endpoint_url(endpoint_url);
        }

        let aws_config = aws_config_loader.load().await;
        let client = Client::new(&aws_config);

        Self {
            client,
            bucket: config.bucket,
            prefix: config.prefix,
        }
    }

    /// Build the full S3 key from a path.
    fn full_key(&self, path: &Path) -> String {
        let path_str = path_to_s3_key(path);
        match &self.prefix {
            Some(prefix) => {
                let prefix = prefix.trim_end_matches('/');
                if path_str.is_empty() {
                    prefix.to_string()
                } else {
                    format!("{}/{}", prefix, path_str)
                }
            }
            None => path_str,
        }
    }

    /// Build the S3 prefix for listing a directory.
    fn list_prefix(&self, path: &Path) -> String {
        let key = self.full_key(path);
        if key.is_empty() {
            String::new()
        } else {
            format!("{}/", key.trim_end_matches('/'))
        }
    }
}

/// Convert a Path to an S3 key (using "/" as separator).
fn path_to_s3_key(path: &Path) -> String {
    let path_str = path.to_string_lossy();
    let stripped = path_str.trim_start_matches('/').trim_start_matches('\\');
    // Convert OS separators to "/"
    stripped.replace('\\', "/")
}

fn is_not_found<E>(err: &SdkError<E>) -> bool {
    matches!(err, SdkError::ServiceError(e) if e.raw().status().as_u16() == 404)
}

fn map_sdk_error<E: std::fmt::Debug>(err: SdkError<E>) -> FileSourceError {
    FileSourceError::Other(format!("{:?}", err))
}

impl FileSource for S3FileSource {
    async fn list_directory(&self, path: &Path) -> Result<DirectoryList> {
        let prefix = self.list_prefix(path);
        let parent_path = path_to_s3_key(path);

        Ok(DirectoryList::new(S3DirectoryList {
            client: self.client.clone(),
            bucket: self.bucket.clone(),
            prefix,
            parent_path,
            buffer: Vec::new(),
            buffer_index: 0,
            continuation_token: None,
            initial_fetch_done: false,
            exhausted: false,
        }))
    }

    async fn get_file_chunks(&self, path: &Path) -> Result<FileChunks> {
        let key = self.full_key(path);

        // Get file size via HEAD request (doesn't download the file)
        let head_response = self
            .client
            .head_object()
            .bucket(&self.bucket)
            .key(&key)
            .send()
            .await
            .map_err(|err| {
                if is_not_found(&err) {
                    FileSourceError::NotFound
                } else {
                    map_sdk_error(err)
                }
            })?;

        let size = head_response.content_length().unwrap_or(0) as u64;

        Ok(FileChunks::new(S3FileChunks {
            client: self.client.clone(),
            bucket: self.bucket.clone(),
            key,
            size,
            offset: 0,
        }))
    }

    async fn get_entry(&self, path: &Path) -> Result<Option<DirectoryListEntry>> {
        let key = self.full_key(path);
        let path_str = path_to_s3_key(path);

        // First, try to get the object directly (it might be a file)
        match self
            .client
            .head_object()
            .bucket(&self.bucket)
            .key(&key)
            .send()
            .await
        {
            Ok(response) => {
                let name = path
                    .file_name()
                    .map(|n| n.to_string_lossy().to_string())
                    .unwrap_or_default();

                return Ok(Some(DirectoryListEntry::File(FileEntry {
                    name,
                    path: PathBuf::from(path_str.replace('/', std::path::MAIN_SEPARATOR_STR)),
                    size: response.content_length().unwrap_or(0) as u64,
                    executable: false,
                })));
            }
            Err(err) if is_not_found(&err) => {
                // Not a file, check if it's a directory (has objects with this prefix)
            }
            Err(err) => return Err(map_sdk_error(err)),
        }

        // Check if it's a directory by looking for objects with this prefix
        let prefix = if key.is_empty() {
            String::new()
        } else {
            format!("{}/", key)
        };

        let response = self
            .client
            .list_objects_v2()
            .bucket(&self.bucket)
            .prefix(&prefix)
            .max_keys(1)
            .send()
            .await
            .map_err(map_sdk_error)?;

        let has_contents = response.contents().first().is_some()
            || response.common_prefixes().first().is_some();

        if has_contents {
            let name = path
                .file_name()
                .map(|n| n.to_string_lossy().to_string())
                .unwrap_or_default();

            Ok(Some(DirectoryListEntry::Directory(DirEntry {
                name,
                path: PathBuf::from(path_str.replace('/', std::path::MAIN_SEPARATOR_STR)),
            })))
        } else {
            Ok(None)
        }
    }
}

/// Directory listing iterator for S3FileSource.
///
/// Fetches pages lazily - only makes S3 requests as entries are consumed.
/// Each page's Contents and CommonPrefixes are merged in lexical order.
struct S3DirectoryList {
    client: Client,
    bucket: String,
    /// The S3 prefix to list (includes trailing slash if non-empty).
    prefix: String,
    /// The parent path (without trailing slash) for building entry paths.
    parent_path: String,
    /// Buffer of entries from the current page, sorted by name.
    buffer: Vec<(String, DirectoryListEntry)>,
    /// Current index into the buffer.
    buffer_index: usize,
    /// Continuation token for fetching the next page.
    continuation_token: Option<String>,
    /// Whether we've done the initial fetch.
    initial_fetch_done: bool,
    /// Whether we've exhausted all pages.
    exhausted: bool,
}

impl S3DirectoryList {
    /// Fetch the next page of results from S3.
    async fn fetch_next_page(&mut self) -> Result<()> {
        let mut request = self
            .client
            .list_objects_v2()
            .bucket(&self.bucket)
            .delimiter("/");

        if !self.prefix.is_empty() {
            request = request.prefix(&self.prefix);
        }

        if let Some(token) = &self.continuation_token {
            request = request.continuation_token(token);
        }

        let response = request.send().await.map_err(map_sdk_error)?;

        self.buffer.clear();
        self.buffer_index = 0;

        // Process files (Contents)
        for obj in response.contents() {
            if let Some(key) = obj.key() {
                // Extract just the filename from the key
                if let Some(name) = key.strip_prefix(&self.prefix) {
                    if !name.is_empty() && !name.contains('/') {
                        let entry_path = if self.parent_path.is_empty() {
                            PathBuf::from(name)
                        } else {
                            PathBuf::from(
                                format!("{}/{}", self.parent_path, name)
                                    .replace('/', std::path::MAIN_SEPARATOR_STR),
                            )
                        };

                        self.buffer.push((
                            name.to_string(),
                            DirectoryListEntry::File(FileEntry {
                                name: name.to_string(),
                                path: entry_path,
                                size: obj.size().unwrap_or(0) as u64,
                                executable: false,
                            }),
                        ));
                    }
                }
            }
        }

        // Process directories (CommonPrefixes)
        for cp in response.common_prefixes() {
            if let Some(cp_prefix) = cp.prefix() {
                if let Some(name) = cp_prefix.strip_prefix(&self.prefix) {
                    let name = name.trim_end_matches('/');
                    if !name.is_empty() {
                        let entry_path = if self.parent_path.is_empty() {
                            PathBuf::from(name)
                        } else {
                            PathBuf::from(
                                format!("{}/{}", self.parent_path, name)
                                    .replace('/', std::path::MAIN_SEPARATOR_STR),
                            )
                        };

                        self.buffer.push((
                            name.to_string(),
                            DirectoryListEntry::Directory(DirEntry {
                                name: name.to_string(),
                                path: entry_path,
                            }),
                        ));
                    }
                }
            }
        }

        // Sort by name for lexical ordering within this page
        self.buffer.sort_by(|a, b| a.0.cmp(&b.0));

        // Update pagination state
        if response.is_truncated() == Some(true) {
            self.continuation_token = response.next_continuation_token().map(|s| s.to_string());
        } else {
            self.continuation_token = None;
            self.exhausted = true;
        }

        self.initial_fetch_done = true;

        Ok(())
    }
}

impl DirectoryListing for S3DirectoryList {
    async fn next(&mut self) -> Result<Option<DirectoryListEntry>> {
        // If we haven't fetched yet, or buffer is exhausted and more pages exist
        if !self.initial_fetch_done
            || (self.buffer_index >= self.buffer.len() && !self.exhausted)
        {
            self.fetch_next_page().await?;
        }

        // Return next entry from buffer if available
        if self.buffer_index < self.buffer.len() {
            let (_, entry) = self.buffer[self.buffer_index].clone();
            self.buffer_index += 1;
            Ok(Some(entry))
        } else {
            Ok(None)
        }
    }
}

/// File chunks iterator for S3FileSource.
///
/// Fetches chunks lazily using S3 range requests - only downloads each chunk
/// as it's requested.
struct S3FileChunks {
    client: Client,
    bucket: String,
    key: String,
    size: u64,
    offset: u64,
}

impl FileChunking for S3FileChunks {
    async fn next(&mut self) -> Result<Option<FileChunk>> {
        if self.offset >= self.size {
            return Ok(None);
        }

        let remaining = self.size - self.offset;
        let chunk_size = next_chunk_size(remaining);

        // Calculate range (inclusive end)
        let range_start = self.offset;
        let range_end = self.offset + chunk_size - 1;
        let range = format!("bytes={}-{}", range_start, range_end);

        // Fetch just this chunk using a range request
        let response = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(&self.key)
            .range(range)
            .send()
            .await
            .map_err(|err| {
                if is_not_found(&err) {
                    FileSourceError::NotFound
                } else {
                    map_sdk_error(err)
                }
            })?;

        let body = response
            .body
            .collect()
            .await
            .map_err(|e| FileSourceError::Other(e.to_string()))?;

        let chunk_data = body.to_vec();
        let chunk = FileChunk::new(self.offset, chunk_data);
        self.offset += chunk_size;

        Ok(Some(chunk))
    }
}
