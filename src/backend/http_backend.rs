use async_trait::async_trait;
use reqwest::{Client, StatusCode};

use super::repo_backend::{
    BackendError, ObjectId, RepoBackend, RepositoryInfo, Result, SwapResult,
};

/// An HTTP-based implementation of `RepoBackend`.
///
/// Operates against an HTTP server implementing the taterfs backend protocol.
pub struct HttpBackend {
    client: Client,
    base_url: String,
}

impl HttpBackend {
    /// Create a new HTTP backend pointing to the given base URL.
    pub fn new(base_url: impl Into<String>) -> Self {
        Self {
            client: Client::new(),
            base_url: base_url.into().trim_end_matches('/').to_string(),
        }
    }

    /// Create a new HTTP backend with a custom reqwest client.
    pub fn with_client(client: Client, base_url: impl Into<String>) -> Self {
        Self {
            client,
            base_url: base_url.into().trim_end_matches('/').to_string(),
        }
    }

    fn repository_info_url(&self) -> String {
        format!("{}/repository_info", self.base_url)
    }

    fn objects_url(&self, id: &ObjectId) -> String {
        format!("{}/objects/{}", self.base_url, id)
    }

    fn current_root_url(&self) -> String {
        format!("{}/current_root", self.base_url)
    }

    fn swap_root_url(&self, expected: &ObjectId) -> String {
        format!("{}/current_root/{}", self.base_url, expected)
    }
}

#[async_trait]
impl RepoBackend for HttpBackend {
    async fn has_repository_info(&self) -> Result<bool> {
        let response = self
            .client
            .head(self.repository_info_url())
            .send()
            .await
            .map_err(|e| BackendError::Other(e.to_string()))?;

        match response.status() {
            StatusCode::OK => Ok(true),
            StatusCode::NOT_FOUND => Ok(false),
            status => Err(BackendError::Other(format!(
                "unexpected status code: {}",
                status
            ))),
        }
    }

    async fn set_repository_info(&self, info: &RepositoryInfo) -> Result<()> {
        // Check if already initialized first
        if self.has_repository_info().await? {
            return Err(BackendError::AlreadyInitialized);
        }

        let response = self
            .client
            .put(self.repository_info_url())
            .json(info)
            .send()
            .await
            .map_err(|e| BackendError::Other(e.to_string()))?;

        if response.status().is_success() {
            Ok(())
        } else {
            Err(BackendError::Other(format!(
                "failed to set repository info: {}",
                response.status()
            )))
        }
    }

    async fn get_repository_info(&self) -> Result<RepositoryInfo> {
        let response = self
            .client
            .get(self.repository_info_url())
            .send()
            .await
            .map_err(|e| BackendError::Other(e.to_string()))?;

        match response.status() {
            StatusCode::OK => {
                let info: RepositoryInfo = response.json().await.map_err(|e| {
                    BackendError::Other(format!("failed to parse repository info: {}", e))
                })?;
                Ok(info)
            }
            StatusCode::NOT_FOUND => Err(BackendError::NotFound),
            status => Err(BackendError::Other(format!(
                "unexpected status code: {}",
                status
            ))),
        }
    }

    async fn read_current_root(&self) -> Result<Option<ObjectId>> {
        let response = self
            .client
            .get(self.current_root_url())
            .send()
            .await
            .map_err(|e| BackendError::Other(e.to_string()))?;

        match response.status() {
            StatusCode::OK => {
                let root_id = response
                    .text()
                    .await
                    .map_err(|e| BackendError::Other(e.to_string()))?;
                Ok(Some(root_id.trim().to_string()))
            }
            StatusCode::NOT_FOUND => Ok(None),
            status => Err(BackendError::Other(format!(
                "unexpected status code: {}",
                status
            ))),
        }
    }

    async fn write_current_root(&self, root_id: &ObjectId) -> Result<()> {
        let response = self
            .client
            .put(self.current_root_url())
            .body(root_id.clone())
            .send()
            .await
            .map_err(|e| BackendError::Other(e.to_string()))?;

        if response.status().is_success() {
            Ok(())
        } else {
            Err(BackendError::Other(format!(
                "failed to write root: {}",
                response.status()
            )))
        }
    }

    async fn swap_current_root(
        &self,
        expected: Option<&ObjectId>,
        new_root: &ObjectId,
    ) -> Result<SwapResult> {
        match expected {
            Some(expected_id) => {
                // Use the conditional PUT endpoint
                let response = self
                    .client
                    .put(self.swap_root_url(expected_id))
                    .body(new_root.clone())
                    .send()
                    .await
                    .map_err(|e| BackendError::Other(e.to_string()))?;

                match response.status() {
                    StatusCode::OK | StatusCode::NO_CONTENT => Ok(SwapResult::Success),
                    StatusCode::NOT_FOUND => {
                        // Current root didn't match - fetch actual current root
                        let current = self.read_current_root().await?;
                        Ok(SwapResult::Mismatch(current))
                    }
                    status => Err(BackendError::Other(format!(
                        "unexpected status code: {}",
                        status
                    ))),
                }
            }
            None => {
                // No expected value - check if root is actually empty first
                let current = self.read_current_root().await?;
                if current.is_some() {
                    return Ok(SwapResult::Mismatch(current));
                }
                self.write_current_root(new_root).await?;
                Ok(SwapResult::Success)
            }
        }
    }

    async fn object_exists(&self, id: &ObjectId) -> Result<bool> {
        let response = self
            .client
            .head(self.objects_url(id))
            .send()
            .await
            .map_err(|e| BackendError::Other(e.to_string()))?;

        match response.status() {
            StatusCode::OK => Ok(true),
            StatusCode::NOT_FOUND => Ok(false),
            status => Err(BackendError::Other(format!(
                "unexpected status code: {}",
                status
            ))),
        }
    }

    async fn read_object(&self, id: &ObjectId) -> Result<Vec<u8>> {
        let response = self
            .client
            .get(self.objects_url(id))
            .send()
            .await
            .map_err(|e| BackendError::Other(e.to_string()))?;

        match response.status() {
            StatusCode::OK => {
                let bytes = response
                    .bytes()
                    .await
                    .map_err(|e| BackendError::Other(e.to_string()))?;
                Ok(bytes.to_vec())
            }
            StatusCode::NOT_FOUND => Err(BackendError::NotFound),
            status => Err(BackendError::Other(format!(
                "unexpected status code: {}",
                status
            ))),
        }
    }

    async fn write_object(&self, id: &ObjectId, data: &[u8]) -> Result<()> {
        let response = self
            .client
            .put(self.objects_url(id))
            .body(data.to_vec())
            .send()
            .await
            .map_err(|e| BackendError::Other(e.to_string()))?;

        if response.status().is_success() {
            Ok(())
        } else {
            Err(BackendError::Other(format!(
                "failed to write object: {}",
                response.status()
            )))
        }
    }
}
