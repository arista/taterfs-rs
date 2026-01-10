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

    /// Convert an S3 key back to a path relative to the source root.
    fn key_to_path(&self, key: &str) -> PathBuf {
        let relative = match &self.prefix {
            Some(prefix) => {
                let prefix = prefix.trim_end_matches('/');
                key.strip_prefix(prefix)
                    .and_then(|s| s.strip_prefix('/'))
                    .unwrap_or(key)
            }
            None => key,
        };
        // Convert "/" separators to OS path separators
        PathBuf::from(relative.replace('/', std::path::MAIN_SEPARATOR_STR))
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

        let mut entries = Vec::new();
        let mut continuation_token: Option<String> = None;

        loop {
            let mut request = self
                .client
                .list_objects_v2()
                .bucket(&self.bucket)
                .delimiter("/");

            if !prefix.is_empty() {
                request = request.prefix(&prefix);
            }

            if let Some(token) = &continuation_token {
                request = request.continuation_token(token);
            }

            let response = request.send().await.map_err(map_sdk_error)?;

            // Process files (Contents)
            for obj in response.contents() {
                if let Some(key) = obj.key() {
                    // Extract just the filename from the key
                    if let Some(name) = key.strip_prefix(&prefix) {
                        if !name.is_empty() && !name.contains('/') {
                            let entry_path = if parent_path.is_empty() {
                                PathBuf::from(name)
                            } else {
                                PathBuf::from(format!("{}/{}", parent_path, name)
                                    .replace('/', std::path::MAIN_SEPARATOR_STR))
                            };

                            entries.push((
                                name.to_string(),
                                DirectoryListEntry::File(FileEntry {
                                    name: name.to_string(),
                                    path: entry_path,
                                    size: obj.size().unwrap_or(0) as u64,
                                    executable: false, // S3 doesn't have executable flag
                                }),
                            ));
                        }
                    }
                }
            }

            // Process directories (CommonPrefixes)
            for cp in response.common_prefixes() {
                if let Some(cp_prefix) = cp.prefix() {
                    if let Some(name) = cp_prefix.strip_prefix(&prefix) {
                        let name = name.trim_end_matches('/');
                        if !name.is_empty() {
                            let entry_path = if parent_path.is_empty() {
                                PathBuf::from(name)
                            } else {
                                PathBuf::from(format!("{}/{}", parent_path, name)
                                    .replace('/', std::path::MAIN_SEPARATOR_STR))
                            };

                            entries.push((
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

            // Check if there are more results
            if response.is_truncated() == Some(true) {
                continuation_token = response.next_continuation_token().map(|s| s.to_string());
            } else {
                break;
            }
        }

        // Sort by name for lexical ordering
        entries.sort_by(|a, b| a.0.cmp(&b.0));

        Ok(DirectoryList::new(S3DirectoryList { entries, index: 0 }))
    }

    async fn get_file_chunks(&self, path: &Path) -> Result<FileChunks> {
        let key = self.full_key(path);

        // Get the object to read its contents
        let response = self
            .client
            .get_object()
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

        let size = response.content_length().unwrap_or(0) as u64;

        // Collect the body into memory
        let body = response
            .body
            .collect()
            .await
            .map_err(|e| FileSourceError::Other(e.to_string()))?;

        let data = body.to_vec();

        Ok(FileChunks::new(S3FileChunks {
            data,
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
struct S3DirectoryList {
    entries: Vec<(String, DirectoryListEntry)>,
    index: usize,
}

impl DirectoryListing for S3DirectoryList {
    async fn next(&mut self) -> Result<Option<DirectoryListEntry>> {
        if self.index >= self.entries.len() {
            return Ok(None);
        }
        let (_, entry) = self.entries[self.index].clone();
        self.index += 1;
        Ok(Some(entry))
    }
}

/// File chunks iterator for S3FileSource.
///
/// Since S3 doesn't support range requests in a streaming fashion easily,
/// we read the entire file into memory and chunk it.
struct S3FileChunks {
    data: Vec<u8>,
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

        let start = self.offset as usize;
        let end = (self.offset + chunk_size) as usize;
        let chunk_data = self.data[start..end.min(self.data.len())].to_vec();

        let chunk = FileChunk::new(self.offset, chunk_data);
        self.offset += chunk_size;

        Ok(Some(chunk))
    }
}
