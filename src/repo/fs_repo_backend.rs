// Thanks ChatGPT

// src/repo/fs_repo_backend.rs

use crate::repo::repo_backend::{BackendError, RepoBackend};
use crate::repo::repo_model::ObjectId;

use async_trait::async_trait;
use bytes::Bytes;
use rand::{Rng, distr::Alphanumeric};
use sha2::{Digest, Sha256};
use std::{
    path::{Path, PathBuf},
    str::FromStr,
};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use tokio::fs;
use tokio::io::{self, AsyncWriteExt};

/// Configuration for the filesystem backend.
#[derive(Clone, Debug)]
pub struct Context {
    /// Root directory for this backend (e.g., “…/repo”).
    pub root: PathBuf,
    /// If true, verify sha256(bytes) == ObjectId on write().
    pub validate_hashes_on_write: bool,
}

/// Filesystem implementation of RepoBackend.
#[derive(Clone, Debug)]
pub struct FsRepoBackend {
    ctx: Context,
}

impl FsRepoBackend {
    pub fn new(ctx: Context) -> Self {
        Self { ctx }
    }

    fn objects_dir(&self) -> PathBuf {
        self.ctx.root.join("objects")
    }
    fn current_root_dir(&self) -> PathBuf {
        self.ctx.root.join("current-root")
    }
    fn tmp_dir(&self) -> PathBuf {
        self.ctx.root.join("tmp")
    }

    fn object_path(&self, id_hex: &str) -> PathBuf {
        let (xx, yy, zz) = (&id_hex[0..2], &id_hex[2..4], &id_hex[4..6]);
        self.objects_dir().join(xx).join(yy).join(zz).join(id_hex)
    }

    fn current_root_entry_paths(&self) -> io::Result<(PathBuf, String)> {
        let now = OffsetDateTime::now_utc();
        let secs = now.unix_timestamp();
        let secs_u64 = u64::try_from(secs).unwrap_or(0);
        let inv = u64::MAX - secs_u64;
        let inv_str = format!("{:020}", inv);
        let iso = now
            .format(&Rfc3339)
            .unwrap_or_else(|_| "unknown-time".into());

        let shard = &inv_str[0..6];
        let filename = format!("current-root-{}-{}", inv_str, iso);
        Ok((
            self.current_root_dir().join(shard).join(&filename),
            filename,
        ))
    }

    async fn find_current_root_head_file(&self) -> io::Result<Option<PathBuf>> {
        let cr = self.current_root_dir();
        let mut rd = match fs::read_dir(&cr).await {
            Ok(rd) => rd,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e),
        };

        let mut shard_dirs: Vec<(String, PathBuf)> = Vec::new();
        while let Some(entry) = rd.next_entry().await? {
            let md = entry.metadata().await?;
            if md.is_dir() {
                let name = entry.file_name().to_string_lossy().to_string();
                shard_dirs.push((name, entry.path()));
            }
        }
        if shard_dirs.is_empty() {
            return Ok(None);
        }
        shard_dirs.sort_by(|a, b| a.0.cmp(&b.0));

        let mut files: Vec<(String, PathBuf)> = Vec::new();
        let mut rd2 = fs::read_dir(&shard_dirs[0].1).await?;
        while let Some(entry) = rd2.next_entry().await? {
            let md = entry.metadata().await?;
            if md.is_file() {
                let name = entry.file_name().to_string_lossy().to_string();
                files.push((name, entry.path()));
            }
        }
        if files.is_empty() {
            return Ok(None);
        }
        files.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(files.first().map(|(_, p)| p.clone()))
    }

    async fn ensure_parent_dir(p: &Path) -> io::Result<()> {
        if let Some(parent) = p.parent() {
            fs::create_dir_all(parent).await?;
        }
        Ok(())
    }

    async fn relaxed_write(&self, final_path: &Path, data: &[u8]) -> io::Result<()> {
        Self::ensure_parent_dir(final_path).await?;
        fs::create_dir_all(self.tmp_dir()).await?;

        let rand_tail: String = rand::rng()
            .sample_iter(Alphanumeric)
            .take(10)
            .map(char::from)
            .collect();

        let tmp_name = format!(
            "{}.{}.tmp",
            final_path
                .file_name()
                .and_then(|s| s.to_str())
                .unwrap_or("tmpfile"),
            rand_tail
        );
        let tmp_path = self.tmp_dir().join(tmp_name);

        let mut f = fs::File::create(&tmp_path).await?;
        f.write_all(data).await?;
        drop(f);

        if let Some(parent) = final_path.parent() {
            fs::create_dir_all(parent).await?;
        }
        fs::rename(&tmp_path, final_path).await?;
        Ok(())
    }

    async fn durable_publish(&self, final_path: &Path, data: &[u8]) -> io::Result<()> {
        Self::ensure_parent_dir(final_path).await?;
        fs::create_dir_all(self.tmp_dir()).await?;

        let rand_tail: String = rand::rng()
            .sample_iter(Alphanumeric)
            .take(10)
            .map(char::from)
            .collect();

        let tmp_name = format!(
            "{}.{}.tmp",
            final_path
                .file_name()
                .and_then(|s| s.to_str())
                .unwrap_or("tmpfile"),
            rand_tail
        );
        let tmp_path = self.tmp_dir().join(tmp_name);

        {
            let mut f = fs::File::create(&tmp_path).await?;
            f.write_all(data).await?;
            f.sync_data().await?;
        }

        if let Some(parent) = final_path.parent() {
            fs::create_dir_all(parent).await?;
        }
        fs::rename(&tmp_path, final_path).await?;

        if let Some(parent) = final_path.parent() {
            // Open the directory and fsync it. No OpenOptionsExt needed.
            if let Ok(dirf) = std::fs::File::open(parent) {
                // This is a blocking syscall; keep Tokio happy:
                let _ = tokio::task::block_in_place(|| dirf.sync_all());
            }
        }

        Ok(())
    }

    async fn read_current_root_from_file(path: &Path) -> Result<ObjectId, BackendError> {
        let bytes = fs::read(path).await?;
        let s = String::from_utf8_lossy(&bytes);
        let line = s
            .lines()
            .map(|l| l.trim())
            .find(|l| !l.is_empty())
            .ok_or(BackendError::RootMissing)?;
        ObjectId::from_str(line).map_err(|_| {
            BackendError::Other(format!(
                "invalid ObjectId in current-root file {}: {:?}",
                path.display(),
                line
            ))
        })
    }

    fn hex_sha256(bytes: &[u8]) -> String {
        let mut hasher = Sha256::new();
        hasher.update(bytes);
        let digest = hasher.finalize();
        digest.iter().map(|b| format!("{:02x}", b)).collect()
    }
}

#[async_trait]
impl RepoBackend for FsRepoBackend {
    async fn current_root_exists(&self) -> Result<bool, BackendError> {
        Ok(self.find_current_root_head_file().await?.is_some())
    }

    async fn read_current_root(&self) -> Result<ObjectId, BackendError> {
        let head = self
            .find_current_root_head_file()
            .await?
            .ok_or(BackendError::RootMissing)?;
        Self::read_current_root_from_file(&head).await
    }

    async fn write_current_root(&self, root: &ObjectId) -> Result<(), BackendError> {
        let (final_path, _fname) = self.current_root_entry_paths()?;
        let mut contents = root.to_string();
        contents.push('\n');
        self.durable_publish(&final_path, contents.as_bytes())
            .await?;
        Ok(())
    }

    async fn compare_and_swap_current_root(
        &self,
        expected: Option<&ObjectId>,
        new: &ObjectId,
    ) -> Result<(), BackendError> {
        if let Some(exp) = expected {
            let cur = self.read_current_root().await?;
            if &cur != exp {
                return Err(BackendError::Conflict);
            }
        }

        let (final_path, _fname) = self.current_root_entry_paths()?;
        let mut contents = new.to_string();
        contents.push('\n');
        self.durable_publish(&final_path, contents.as_bytes())
            .await?;

        let head_now = self.read_current_root().await?;
        if &head_now != new {
            return Err(BackendError::Conflict);
        }
        Ok(())
    }

    async fn exists(&self, id: &ObjectId) -> Result<bool, BackendError> {
        let id_str = id.to_string();
        Ok(fs::try_exists(self.object_path(&id_str)).await?)
    }

    async fn read(&self, id: &ObjectId) -> Result<Bytes, BackendError> {
        let id_str = id.to_string();
        let p = self.object_path(&id_str);
        match fs::read(&p).await {
            Ok(v) => Ok(Bytes::from(v)),
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                Err(BackendError::NotFound(id.clone()))
            }
            Err(e) => Err(e.into()),
        }
    }

    async fn write(&self, id: &ObjectId, bytes: Bytes) -> Result<(), BackendError> {
        let id_str = id.to_string();

        if self.ctx.validate_hashes_on_write {
            let computed = Self::hex_sha256(bytes.as_ref());
            if computed != id_str {
                return Err(BackendError::HashMismatch {
                    id: id.clone(),
                    expected: id_str,
                    computed,
                });
            }
        }

        let final_path = self.object_path(&id_str);
        if fs::try_exists(&final_path).await? {
            return Ok(());
        }
        self.relaxed_write(&final_path, bytes.as_ref()).await?;
        Ok(())
    }
}
