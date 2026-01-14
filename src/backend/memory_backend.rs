use std::collections::HashMap;
use std::sync::RwLock;

use async_trait::async_trait;

use super::repo_backend::{
    BackendError, ObjectId, RepoBackend, RepositoryInfo, Result, SwapResult,
};

/// An in-memory implementation of `RepoBackend`, intended primarily for testing.
pub struct MemoryBackend {
    repository_info: RwLock<Option<RepositoryInfo>>,
    objects: RwLock<HashMap<ObjectId, Vec<u8>>>,
    current_root: RwLock<Option<ObjectId>>,
}

impl MemoryBackend {
    /// Create a new empty in-memory backend (uninitialized).
    pub fn new() -> Self {
        Self {
            repository_info: RwLock::new(None),
            objects: RwLock::new(HashMap::new()),
            current_root: RwLock::new(None),
        }
    }

    /// Create a new in-memory backend that is already initialized with a random UUID.
    ///
    /// This is useful for tests that don't need to test initialization.
    pub fn new_initialized() -> Self {
        Self {
            repository_info: RwLock::new(Some(RepositoryInfo {
                uuid: generate_uuid(),
            })),
            objects: RwLock::new(HashMap::new()),
            current_root: RwLock::new(None),
        }
    }

    /// Create a new in-memory backend that is already initialized with the specified UUID.
    ///
    /// This is useful for tests that need a specific UUID.
    pub fn new_initialized_with_uuid(uuid: String) -> Self {
        Self {
            repository_info: RwLock::new(Some(RepositoryInfo { uuid })),
            objects: RwLock::new(HashMap::new()),
            current_root: RwLock::new(None),
        }
    }
}

/// Generate a simple UUID-like string for testing purposes.
fn generate_uuid() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("{:032x}", now)
}

impl Default for MemoryBackend {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl RepoBackend for MemoryBackend {
    async fn has_repository_info(&self) -> Result<bool> {
        let info = self.repository_info.read().unwrap();
        Ok(info.is_some())
    }

    async fn set_repository_info(&self, info: &RepositoryInfo) -> Result<()> {
        let mut repo_info = self.repository_info.write().unwrap();
        if repo_info.is_some() {
            return Err(BackendError::AlreadyInitialized);
        }
        *repo_info = Some(info.clone());
        Ok(())
    }

    async fn get_repository_info(&self) -> Result<RepositoryInfo> {
        let info = self.repository_info.read().unwrap();
        info.clone().ok_or(BackendError::NotFound)
    }

    async fn read_current_root(&self) -> Result<Option<ObjectId>> {
        let root = self.current_root.read().unwrap();
        Ok(root.clone())
    }

    async fn write_current_root(&self, root_id: &ObjectId) -> Result<()> {
        let mut root = self.current_root.write().unwrap();
        *root = Some(root_id.clone());
        Ok(())
    }

    async fn swap_current_root(
        &self,
        expected: Option<&ObjectId>,
        new_root: &ObjectId,
    ) -> Result<SwapResult> {
        let mut root = self.current_root.write().unwrap();

        let matches = match (&*root, expected) {
            (None, None) => true,
            (Some(current), Some(exp)) => current == exp,
            _ => false,
        };

        if matches {
            *root = Some(new_root.clone());
            Ok(SwapResult::Success)
        } else {
            Ok(SwapResult::Mismatch(root.clone()))
        }
    }

    async fn object_exists(&self, id: &ObjectId) -> Result<bool> {
        let objects = self.objects.read().unwrap();
        Ok(objects.contains_key(id))
    }

    async fn read_object(&self, id: &ObjectId) -> Result<Vec<u8>> {
        let objects = self.objects.read().unwrap();
        objects.get(id).cloned().ok_or(BackendError::NotFound)
    }

    async fn write_object(&self, id: &ObjectId, data: &[u8]) -> Result<()> {
        let mut objects = self.objects.write().unwrap();
        objects.insert(id.clone(), data.to_vec());
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_object_roundtrip() {
        let backend = MemoryBackend::new();
        let id = "abc123".to_string();
        let data = b"hello world";

        assert!(!backend.object_exists(&id).await.unwrap());

        backend.write_object(&id, data).await.unwrap();

        assert!(backend.object_exists(&id).await.unwrap());
        assert_eq!(backend.read_object(&id).await.unwrap(), data);
    }

    #[tokio::test]
    async fn test_read_nonexistent_object() {
        let backend = MemoryBackend::new();
        let id = "nonexistent".to_string();

        let result = backend.read_object(&id).await;
        assert!(matches!(result, Err(BackendError::NotFound)));
    }

    #[tokio::test]
    async fn test_root_operations() {
        let backend = MemoryBackend::new();

        assert_eq!(backend.read_current_root().await.unwrap(), None);

        let root1 = "root1".to_string();
        backend.write_current_root(&root1).await.unwrap();
        assert_eq!(
            backend.read_current_root().await.unwrap(),
            Some(root1.clone())
        );

        let root2 = "root2".to_string();
        backend.write_current_root(&root2).await.unwrap();
        assert_eq!(backend.read_current_root().await.unwrap(), Some(root2));
    }

    #[tokio::test]
    async fn test_swap_current_root_success() {
        let backend = MemoryBackend::new();

        // Swap from None to root1
        let root1 = "root1".to_string();
        let result = backend.swap_current_root(None, &root1).await.unwrap();
        assert_eq!(result, SwapResult::Success);
        assert_eq!(
            backend.read_current_root().await.unwrap(),
            Some(root1.clone())
        );

        // Swap from root1 to root2
        let root2 = "root2".to_string();
        let result = backend
            .swap_current_root(Some(&root1), &root2)
            .await
            .unwrap();
        assert_eq!(result, SwapResult::Success);
        assert_eq!(backend.read_current_root().await.unwrap(), Some(root2));
    }

    #[tokio::test]
    async fn test_swap_current_root_mismatch() {
        let backend = MemoryBackend::new();

        let root1 = "root1".to_string();
        backend.write_current_root(&root1).await.unwrap();

        // Try to swap with wrong expected value
        let wrong_expected = "wrong".to_string();
        let root2 = "root2".to_string();
        let result = backend
            .swap_current_root(Some(&wrong_expected), &root2)
            .await
            .unwrap();
        assert_eq!(result, SwapResult::Mismatch(Some(root1.clone())));

        // Root should be unchanged
        assert_eq!(backend.read_current_root().await.unwrap(), Some(root1));
    }

    #[tokio::test]
    async fn test_swap_from_none_when_root_exists() {
        let backend = MemoryBackend::new();

        let root1 = "root1".to_string();
        backend.write_current_root(&root1).await.unwrap();

        // Try to swap from None when root exists
        let root2 = "root2".to_string();
        let result = backend.swap_current_root(None, &root2).await.unwrap();
        assert_eq!(result, SwapResult::Mismatch(Some(root1.clone())));

        // Root should be unchanged
        assert_eq!(backend.read_current_root().await.unwrap(), Some(root1));
    }

    #[tokio::test]
    async fn test_repository_info_uninitialized() {
        let backend = MemoryBackend::new();

        // Uninitialized backend should not have repository info
        assert!(!backend.has_repository_info().await.unwrap());

        // Getting info should fail
        let result = backend.get_repository_info().await;
        assert!(matches!(result, Err(BackendError::NotFound)));
    }

    #[tokio::test]
    async fn test_repository_info_set_and_get() {
        let backend = MemoryBackend::new();

        let info = RepositoryInfo {
            uuid: "test-uuid-123".to_string(),
        };

        // Set repository info
        backend.set_repository_info(&info).await.unwrap();

        // Now it should be initialized
        assert!(backend.has_repository_info().await.unwrap());

        // Get should return the info
        let retrieved = backend.get_repository_info().await.unwrap();
        assert_eq!(retrieved.uuid, "test-uuid-123");
    }

    #[tokio::test]
    async fn test_repository_info_already_initialized() {
        let backend = MemoryBackend::new();

        let info1 = RepositoryInfo {
            uuid: "uuid-1".to_string(),
        };
        let info2 = RepositoryInfo {
            uuid: "uuid-2".to_string(),
        };

        // First set should succeed
        backend.set_repository_info(&info1).await.unwrap();

        // Second set should fail
        let result = backend.set_repository_info(&info2).await;
        assert!(matches!(result, Err(BackendError::AlreadyInitialized)));

        // Original info should be preserved
        let retrieved = backend.get_repository_info().await.unwrap();
        assert_eq!(retrieved.uuid, "uuid-1");
    }

    #[tokio::test]
    async fn test_new_initialized() {
        let backend = MemoryBackend::new_initialized();

        // Should already be initialized
        assert!(backend.has_repository_info().await.unwrap());

        // Get should succeed
        let info = backend.get_repository_info().await.unwrap();
        assert!(!info.uuid.is_empty());
    }

    #[tokio::test]
    async fn test_new_initialized_with_uuid() {
        let backend = MemoryBackend::new_initialized_with_uuid("custom-uuid".to_string());

        assert!(backend.has_repository_info().await.unwrap());

        let info = backend.get_repository_info().await.unwrap();
        assert_eq!(info.uuid, "custom-uuid");
    }
}
