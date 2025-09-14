// A filesystem implementation of FileStore, with a couple mechanisms to ensure robustness:
//
// * files are first written to a temporary file then renamed to their final locations
// * a flush() implementation that performs an OS-level flush to disk of all files and directories created since the previous flush, thereby ensuring that all previous writes are written to durable storage

// Thanks ChatGPT

use anyhow::{Context as AnyhowContext, Result};
use async_trait::async_trait;
use bytes::Bytes;
use std::cell::{Cell, RefCell};
use std::collections::{HashSet, VecDeque};
use std::path::{Path, PathBuf};
use tokio::fs;
use tokio::io::AsyncWriteExt;

use super::FileStore;

pub struct FSFileStoreContext {
    /// Root of the logical store; all paths are relative to this.
    pub root_path: PathBuf,
    /// Shared temp directory (must be on the same filesystem as root_path), used to store files during writing before an atomic rename to their final locations
    pub tmp_dir: PathBuf,
    /// When (pending files + dirty dirs) reaches this, do an automatic flush.
    pub max_pending_flush: usize,
}

struct PendingFile {
    file: Option<fs::File>, // kept open until flush(); closed before rename
    tmp: PathBuf,           // tmp_dir/.<name>.tmp-<nonce>
    final_path: PathBuf,    // root/.../name
}

pub struct FSFileStore {
    ctx: FSFileStoreContext,                 // owned
    pending: RefCell<VecDeque<PendingFile>>, // single-threaded => RefCell
    dirty_dirs: RefCell<HashSet<PathBuf>>,   // dirs to fsync on flush
    nonce: Cell<u64>,                        // tmp suffix
}

impl FSFileStore {
    pub fn new(ctx: FSFileStoreContext) -> Result<Self> {
        assert!(ctx.max_pending_flush > 0, "max_pending_flush must be >= 1");

        // Make sure both dirs exist before canonicalize/stat (blocking is fine in a constructor).
        std::fs::create_dir_all(&ctx.root_path)
            .with_context(|| format!("create_dir_all {:?}", ctx.root_path))?;
        std::fs::create_dir_all(&ctx.tmp_dir)
            .with_context(|| format!("create_dir_all {:?}", ctx.tmp_dir))?;

        // Verify same filesystem to avoid EXDEV on rename(tmp -> final).
        verify_same_filesystem(&ctx.root_path, &ctx.tmp_dir)?;

        Ok(Self {
            ctx,
            pending: RefCell::new(VecDeque::new()),
            dirty_dirs: RefCell::new(HashSet::new()),
            nonce: Cell::new(1),
        })
    }

    fn abs(&self, rel: &str) -> PathBuf {
        let rel = rel.strip_prefix("/").unwrap_or(rel);
        self.ctx.root_path.join(rel)
    }

    fn next_nonce(&self) -> u64 {
        let n = self.nonce.get();
        self.nonce.set(n.wrapping_add(1));
        n
    }

    async fn ensure_dir_exists_mark_dirty(&self, abs_dir: &Path) -> Result<()> {
        if !fs::try_exists(abs_dir).await? {
            fs::create_dir_all(abs_dir)
                .await
                .with_context(|| format!("create_dir_all {:?}", abs_dir))?;
        }
        self.dirty_dirs.borrow_mut().insert(abs_dir.to_path_buf());
        Ok(())
    }

    #[cfg(unix)]
    async fn fsync_dir(path: &Path) -> Result<()> {
        use std::{fs::OpenOptions, os::unix::fs::OpenOptionsExt};
        let p = PathBuf::from(path);
        tokio::task::spawn_blocking(move || -> std::io::Result<()> {
            let f = OpenOptions::new()
                .read(true)
                .custom_flags(libc::O_DIRECTORY)
                .open(&p)?;
            f.sync_all()?;
            Ok(())
        })
        .await
        .expect("spawn_blocking panicked")?;
        Ok(())
    }

    #[cfg(not(unix))]
    async fn fsync_dir(_path: &str) -> Result<()> {
        // TODO: add Windows-specific directory fsync if you need strong durability there.
        Ok(())
    }
}

#[async_trait(?Send)]
impl FileStore for FSFileStore {
    async fn exists(&self, path: &str) -> Result<bool> {
        Ok(fs::try_exists(self.abs(path)).await?)
    }

    async fn read(&self, path: &str) -> Result<Bytes> {
        let p = self.abs(path);
        let v = fs::read(&p)
            .await
            .with_context(|| format!("read {:?}", p))?;
        Ok(Bytes::from(v))
    }

    /// Write to a temp in `tmp_dir`, queue it, and auto-flush if the queue is “full”.
    async fn write(&self, path: &str, buf: Bytes) -> Result<()> {
        let final_path = self.abs(path);
        let parent = final_path
            .parent()
            .ok_or_else(|| anyhow::anyhow!("no parent for {:?}", final_path))?
            .to_path_buf();

        // Ensure final parent exists and is marked dirty (will be fsync'd)
        self.ensure_dir_exists_mark_dirty(&parent).await?;

        // Ensure tmp_dir exists and is marked dirty too
        self.ensure_dir_exists_mark_dirty(&self.ctx.tmp_dir).await?;

        // Build tmp path in tmp_dir
        let base = final_path
            .file_name()
            .and_then(|s| s.to_str())
            .unwrap_or("unnamed");
        let tmp = self
            .ctx
            .tmp_dir
            .join(format!(".{}.tmp-{}", base, self.next_nonce()));

        // Create temp and write payload
        let mut f = fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp)
            .await
            .with_context(|| format!("open tmp {:?}", tmp))?;
        f.write_all(&buf)
            .await
            .with_context(|| format!("write tmp {:?}", tmp))?;

        // Queue for batched flush (drop the mutable borrow before we maybe await on flush)
        {
            let mut q = self.pending.borrow_mut();
            q.push_back(PendingFile {
                file: Some(f),
                tmp,
                final_path,
            });
        }

        // Auto-flush if queue is “full”
        let pending_len = self.pending.borrow().len();
        let dirty_len = self.dirty_dirs.borrow().len();
        if pending_len + dirty_len >= self.ctx.max_pending_flush {
            self.flush().await?; // durability barrier
        }

        Ok(())
    }

    /// Deterministic recursive search (iterative, lexicographic paths).
    async fn first_file(&self, path: &str) -> Result<Option<String>> {
        let base_abs = self.abs(path);
        if !fs::try_exists(&base_abs).await? {
            return Ok(None);
        }

        struct Frame {
            entries: Vec<PathBuf>,
            i: usize,
        }

        async fn list_sorted(dir: &Path) -> Result<Vec<PathBuf>> {
            let mut rd = match fs::read_dir(dir).await {
                Ok(rd) => rd,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(vec![]),
                Err(e) => return Err(e).with_context(|| format!("read_dir {:?}", dir)),
            };
            let mut v = Vec::new();
            while let Some(e) = rd.next_entry().await? {
                v.push(e.path());
            }
            v.sort_by_key(|p| p.file_name().map(|s| s.to_os_string()));
            Ok(v)
        }

        let mut dir_stack: Vec<PathBuf> = vec![base_abs.clone()];
        let mut frame_stack: Vec<Frame> = vec![Frame {
            entries: list_sorted(&base_abs).await?,
            i: 0,
        }];

        while let Some(frame) = frame_stack.last_mut() {
            if frame.i >= frame.entries.len() {
                frame_stack.pop();
                dir_stack.pop();
                continue;
            }

            let child = frame.entries[frame.i].clone();
            frame.i += 1;

            let meta = fs::metadata(&child).await?;
            if meta.is_file() {
                let rel = child.strip_prefix(&base_abs).unwrap().to_path_buf();
                if !rel.as_os_str().is_empty() {
                    return Ok(Some(rel.to_string_lossy().to_string()));
                }
            } else if meta.is_dir() {
                dir_stack.push(child.clone());
                frame_stack.push(Frame {
                    entries: list_sorted(&child).await?,
                    i: 0,
                });
            }
        }

        Ok(None)
    }

    /// Durability barrier for all queued writes:
    /// 1) fdatasync temps, 2) close, 3) rename, 4) fsync dirs (parents + tmp_dir)
    async fn flush(&self) -> Result<()> {
        // Take pending out so we don't keep a RefCell borrow across awaits.
        let mut pending: Vec<PendingFile> = self.pending.borrow_mut().drain(..).collect();

        // Persist file data (fdatasync) while handles are open.
        for p in &mut pending {
            if let Some(f) = p.file.as_mut() {
                f.sync_data()
                    .await
                    .with_context(|| format!("fdatasync {:?}", p.tmp))?;
            }
        }

        // Close handles (needed on Windows; harmless elsewhere).
        for p in &mut pending {
            if let Some(f) = p.file.take() {
                drop(f);
            }
        }

        // Rename temps → finals (must be same filesystem).
        for p in &pending {
            fs::rename(&p.tmp, &p.final_path)
                .await
                .with_context(|| format!("rename {:?} -> {:?}", p.tmp, p.final_path))?;
        }

        // Make sure final parents are marked dirty even if caller didn't write directly to them.
        {
            let mut dd = self.dirty_dirs.borrow_mut();
            dd.insert(self.ctx.tmp_dir.clone());
            for p in &pending {
                if let Some(parent) = p.final_path.parent() {
                    dd.insert(parent.to_path_buf());
                }
            }
        }

        // Fsync all touched directories once.
        let dirs: Vec<PathBuf> = self.dirty_dirs.borrow_mut().drain().collect();
        for d in dirs {
            Self::fsync_dir(&d).await?;
        }

        Ok(())
    }
}

/// Return Ok(()) if both paths live on the same filesystem; Err otherwise.
/// On Unix, compares st_dev; on non-Unix this is a no-op (always Ok).
#[cfg(unix)]
fn verify_same_filesystem(a: &Path, b: &Path) -> Result<()> {
    use std::os::unix::fs::MetadataExt;

    // Follow symlinks to the real mount points.
    let a_real = std::fs::canonicalize(a).with_context(|| format!("canonicalize {:?}", a))?;
    let b_real = std::fs::canonicalize(b).with_context(|| format!("canonicalize {:?}", b))?;

    let a_dev = std::fs::metadata(&a_real)
        .with_context(|| format!("stat {:?}", a_real))?
        .dev();
    let b_dev = std::fs::metadata(&b_real)
        .with_context(|| format!("stat {:?}", b_real))?
        .dev();

    if a_dev != b_dev {
        anyhow::bail!(
            "root_path ({:?}) and tmp_dir ({:?}) are on different filesystems \
             (st_dev {} != {}): cross-device rename would fail (EXDEV)",
            a_real,
            b_real,
            a_dev,
            b_dev
        );
    }
    Ok(())
}

#[cfg(not(unix))]
fn verify_same_filesystem(_a: &str, _b: &str) -> Result<()> {
    // TODO: implement a Windows check if you need it (compare volume/drive).
    Ok(())
}
