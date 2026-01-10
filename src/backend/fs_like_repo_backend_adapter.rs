use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use super::fs_like_repo_backend::FsLikeRepoBackend;
use super::repo_backend::{ObjectId, RepoBackend, Result, SwapResult};

/// Adapter that implements `RepoBackend` using an underlying `FsLikeRepoBackend`.
///
/// This adapter maps object IDs to filesystem paths and manages the current root
/// using a reverse-timestamp directory structure.
pub struct FsLikeRepoBackendAdapter<B: FsLikeRepoBackend> {
    backend: Arc<B>,
}

impl<B: FsLikeRepoBackend> FsLikeRepoBackendAdapter<B> {
    /// Create a new adapter wrapping the given filesystem-like backend.
    pub fn new(backend: Arc<B>) -> Self {
        Self { backend }
    }

    /// Convert an object ID to its storage path.
    ///
    /// Path format: `objects/{id[0..2]}/{id[2..4]}/{id[4..6]}/{id}`
    fn object_path(id: &ObjectId) -> String {
        format!(
            "objects/{}/{}/{}/{}",
            &id[0..2],
            &id[2..4],
            &id[4..6],
            id
        )
    }

    /// Generate a reverse timestamp for root ordering.
    ///
    /// The reverse timestamp decreases lexically over time, computed by subtracting
    /// the current time in milliseconds from u64::MAX, then zero-padding to 20 digits.
    fn reverse_timestamp() -> String {
        let now_millis = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time before unix epoch")
            .as_millis() as u64;
        let reverse = u64::MAX - now_millis;
        format!("{:020}", reverse)
    }

    /// Convert a reverse timestamp to a directory path.
    ///
    /// Path format: `current_root/{ts[0..8]}/{ts[8..10]}/{ts[10..12]}/{ts[12..20]}`
    fn root_dir_path(reverse_ts: &str) -> String {
        format!(
            "current_root/{}/{}/{}/{}",
            &reverse_ts[0..8],
            &reverse_ts[8..10],
            &reverse_ts[10..12],
            &reverse_ts[12..20]
        )
    }

    /// Build the full path for a root entry.
    fn root_entry_path(reverse_ts: &str, root_id: &ObjectId) -> String {
        format!("{}/{}", Self::root_dir_path(reverse_ts), root_id)
    }

    /// Find the current root by looking for the lexically first file in the
    /// root directory hierarchy.
    async fn find_current_root(&self) -> Result<Option<ObjectId>> {
        // Navigate through the timestamp directory hierarchy to find the first entry
        let mut path = "current_root".to_string();

        // Navigate 4 levels deep: {ts[0..8]}/{ts[8..10]}/{ts[10..12]}/{ts[12..20]}
        for _ in 0..4 {
            match self.backend.first_file(&path).await? {
                Some(entry) => {
                    path = format!("{}/{}", path, entry);
                }
                None => return Ok(None),
            }
        }

        // The final entry should be the root object ID
        match self.backend.first_file(&path).await? {
            Some(root_id) => Ok(Some(root_id)),
            None => Ok(None),
        }
    }
}

impl<B: FsLikeRepoBackend> RepoBackend for FsLikeRepoBackendAdapter<B> {
    async fn read_current_root(&self) -> Result<Option<ObjectId>> {
        self.find_current_root().await
    }

    async fn write_current_root(&self, root_id: &ObjectId) -> Result<()> {
        let reverse_ts = Self::reverse_timestamp();
        let path = Self::root_entry_path(&reverse_ts, root_id);
        // Write a zero-length file
        self.backend.write_file(&path, &[]).await
    }

    async fn swap_current_root(
        &self,
        expected: Option<&ObjectId>,
        new_root: &ObjectId,
    ) -> Result<SwapResult> {
        // Best-effort implementation: read current, check, then write
        // This is not atomic and has race conditions
        let current = self.find_current_root().await?;

        let matches = match (&current, expected) {
            (None, None) => true,
            (Some(current_id), Some(expected_id)) => current_id == expected_id,
            _ => false,
        };

        if matches {
            self.write_current_root(new_root).await?;
            Ok(SwapResult::Success)
        } else {
            Ok(SwapResult::Mismatch(current))
        }
    }

    async fn object_exists(&self, id: &ObjectId) -> Result<bool> {
        let path = Self::object_path(id);
        self.backend.file_exists(&path).await
    }

    async fn read_object(&self, id: &ObjectId) -> Result<Vec<u8>> {
        let path = Self::object_path(id);
        self.backend.read_file(&path).await
    }

    async fn write_object(&self, id: &ObjectId, data: &[u8]) -> Result<()> {
        let path = Self::object_path(id);
        self.backend.write_file(&path, data).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_object_path() {
        let id = "abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890".to_string();
        let path = FsLikeRepoBackendAdapter::<DummyBackend>::object_path(&id);
        assert_eq!(
            path,
            "objects/ab/cd/ef/abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890"
        );
    }

    #[test]
    fn test_reverse_timestamp_format() {
        let ts = FsLikeRepoBackendAdapter::<DummyBackend>::reverse_timestamp();
        assert_eq!(ts.len(), 20);
        assert!(ts.chars().all(|c| c.is_ascii_digit()));
    }

    #[test]
    fn test_root_dir_path() {
        let ts = "12345678901234567890";
        let path = FsLikeRepoBackendAdapter::<DummyBackend>::root_dir_path(ts);
        assert_eq!(path, "current_root/12345678/90/12/34567890");
    }

    #[test]
    fn test_root_entry_path() {
        let ts = "12345678901234567890";
        let root_id = "abc123".to_string();
        let path = FsLikeRepoBackendAdapter::<DummyBackend>::root_entry_path(ts, &root_id);
        assert_eq!(path, "current_root/12345678/90/12/34567890/abc123");
    }

    // Dummy backend for type parameter in tests
    struct DummyBackend;

    impl FsLikeRepoBackend for DummyBackend {
        async fn file_exists(&self, _path: &str) -> Result<bool> {
            unimplemented!()
        }

        async fn read_file(&self, _path: &str) -> Result<Vec<u8>> {
            unimplemented!()
        }

        async fn write_file(&self, _path: &str, _data: &[u8]) -> Result<()> {
            unimplemented!()
        }

        async fn first_file(&self, _directory: &str) -> Result<Option<String>> {
            unimplemented!()
        }
    }
}
