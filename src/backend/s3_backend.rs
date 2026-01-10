use aws_sdk_s3::Client;
use aws_sdk_s3::error::SdkError;

use super::fs_like_repo_backend::FsLikeRepoBackend;
use super::repo_backend::{BackendError, Result};

/// Configuration for S3Backend.
pub struct S3BackendConfig {
    /// The S3 bucket name.
    pub bucket: String,
    /// Optional path prefix within the bucket.
    pub prefix: Option<String>,
    /// Optional custom endpoint URL (for LocalStack/MinIO testing).
    pub endpoint_url: Option<String>,
    /// Optional region override.
    pub region: Option<String>,
}

impl S3BackendConfig {
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

/// An S3-based implementation of `FsLikeRepoBackend`.
pub struct S3Backend {
    client: Client,
    bucket: String,
    prefix: Option<String>,
}

impl S3Backend {
    /// Create a new S3 backend with the given configuration.
    ///
    /// Uses the standard AWS credential chain (env vars, ~/.aws, IAM roles, etc.).
    pub async fn new(config: S3BackendConfig) -> Self {
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

    /// Build the full S3 key from a relative path.
    fn full_key(&self, path: &str) -> String {
        match &self.prefix {
            Some(prefix) => format!("{}/{}", prefix.trim_end_matches('/'), path),
            None => path.to_string(),
        }
    }
}

fn is_not_found<E>(err: &SdkError<E>) -> bool {
    matches!(err, SdkError::ServiceError(e) if e.raw().status().as_u16() == 404)
}

impl FsLikeRepoBackend for S3Backend {
    async fn file_exists(&self, path: &str) -> Result<bool> {
        let key = self.full_key(path);

        match self.client.head_object().bucket(&self.bucket).key(&key).send().await {
            Ok(_) => Ok(true),
            Err(err) => {
                if is_not_found(&err) {
                    Ok(false)
                } else {
                    Err(map_sdk_error(err))
                }
            }
        }
    }

    async fn read_file(&self, path: &str) -> Result<Vec<u8>> {
        let key = self.full_key(path);

        let response = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(&key)
            .send()
            .await
            .map_err(|err| {
                if is_not_found(&err) {
                    BackendError::NotFound
                } else {
                    map_sdk_error(err)
                }
            })?;

        let bytes = response
            .body
            .collect()
            .await
            .map_err(|e| BackendError::Other(e.to_string()))?;

        Ok(bytes.to_vec())
    }

    async fn write_file(&self, path: &str, data: &[u8]) -> Result<()> {
        let key = self.full_key(path);

        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(&key)
            .body(data.to_vec().into())
            .send()
            .await
            .map_err(map_sdk_error)?;

        Ok(())
    }

    async fn first_file(&self, directory: &str) -> Result<Option<String>> {
        let prefix = self.full_key(directory);
        let prefix = if prefix.ends_with('/') {
            prefix
        } else {
            format!("{}/", prefix)
        };

        let response = self
            .client
            .list_objects_v2()
            .bucket(&self.bucket)
            .prefix(&prefix)
            .delimiter("/")
            .max_keys(1)
            .send()
            .await
            .map_err(map_sdk_error)?;

        // Check for files (Contents) first, then directories (CommonPrefixes)
        // and return the lexically first one
        let mut first: Option<String> = None;

        for obj in response.contents() {
            if let Some(key) = obj.key() {
                // Extract just the filename from the key
                if let Some(name) = key.strip_prefix(&prefix)
                    && !name.is_empty()
                    && !name.contains('/')
                {
                    match &first {
                        None => first = Some(name.to_string()),
                        Some(current) if name < current.as_str() => {
                            first = Some(name.to_string())
                        }
                        _ => {}
                    }
                }
            }
        }

        for cp in response.common_prefixes() {
            if let Some(cp_prefix) = cp.prefix() {
                // Extract just the directory name
                if let Some(name) = cp_prefix.strip_prefix(&prefix)
                    && !name.trim_end_matches('/').is_empty()
                {
                    let name = name.trim_end_matches('/');
                    match &first {
                        None => first = Some(name.to_string()),
                        Some(current) if name < current.as_str() => {
                            first = Some(name.to_string())
                        }
                        _ => {}
                    }
                }
            }
        }

        Ok(first)
    }
}

fn map_sdk_error<E: std::fmt::Debug>(err: SdkError<E>) -> BackendError {
    BackendError::Other(format!("{:?}", err))
}
