// Implementation of FileStore that uses the local filesystem.  Ignores any directories named ".git" or ".tfs".  Also respects the directives in ".gitignore" and ".tfsignore" (.tfsignore is the same format as .gitignore - if both are present, their entries are combined, .gitignore first then .tfsignore)

// Thanks ChatGPT

use bytes::Bytes;
use std::ffi::OsString;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result};
use async_trait::async_trait;
use ignore::gitignore::{Gitignore, GitignoreBuilder};
use tokio::fs;
use tokio::io::{AsyncReadExt, AsyncSeekExt, SeekFrom};

use crate::file_store::file_store::{
    DirEntry, DirectoryEntry, DirectoryLister, FileChunkHandle, FileChunksIterator, FileEntry,
    FileStoreService,
};
use crate::repo::repo_model::CHUNK_SIZES;

// Tokio-based local filesystem implementation.
#[derive(Debug, Default, Clone)]
pub struct FsFileStoreService;

#[async_trait]
impl FileStoreService for FsFileStoreService {
    async fn list_directory(&self, root: &Path) -> Result<Box<dyn DirectoryLister>> {
        LocalDirectoryLister::new_root(root)
            .await
            .map(|l| Box::new(l) as _)
    }

    async fn get_file_chunks(&self, path: PathBuf) -> Result<Box<dyn FileChunksIterator>> {
        let meta = fs::metadata(&path)
            .await
            .with_context(|| format!("stat failed for {}", path.display()))?;
        let len = meta.len();

        let iter = FsFileChunksIterator {
            path: Arc::new(path),
            file_len: len,
            cursor: 0,
        };
        Ok(Box::new(iter))
    }
}

// Handle .gitignore / .tfsignore.  Keep a "chain" of the ignore rules that gets passed down as the DirectoryLister runs recursively

#[derive(Clone)]
struct IgnoreChain {
    root_abs: Arc<PathBuf>,
    layers: Arc<Vec<Gitignore>>,
}

impl IgnoreChain {
    fn new(root_abs: PathBuf) -> Self {
        Self {
            root_abs: Arc::new(root_abs),
            layers: Arc::new(Vec::new()),
        }
    }

    // Extend with ignore rules found directly in `dir_abs` (.gitignore / .tfsignore).
    async fn extend_with_dir(&self, dir_abs: &Path) -> Result<Self> {
        let gi = load_gitignore_for_dir(dir_abs).await?;
        if let Some(gi) = gi {
            let mut v = (*self.layers).clone();
            v.push(gi);
            Ok(Self {
                root_abs: self.root_abs.clone(),
                layers: Arc::new(v),
            })
        } else {
            Ok(self.clone())
        }
    }

    // Return true if `abs_path` should be ignored given `is_dir`.
    // Only applies rules at/below the listing root.
    fn is_ignored(&self, abs_path: &Path, is_dir: bool) -> bool {
        if !abs_path.starts_with(&*self.root_abs) {
            // Out of scope—be conservative and don't ignore.
            return false;
        }
        // Last match wins from root → current.
        let mut decision: Option<bool> = None; // Some(true)=ignore, Some(false)=whitelist
        for gi in self.layers.iter() {
            let m = gi.matched(abs_path, is_dir);
            if m.is_whitelist() {
                decision = Some(false);
            } else if m.is_ignore() {
                decision = Some(true);
            }
        }
        decision.unwrap_or(false)
    }
}

// Read `.gitignore` and/or `.tfsignore` inside `dir_abs` and build a combined matcher.
// Uses async reads and `GitignoreBuilder::add_line` so we don’t block.
async fn load_gitignore_for_dir(dir_abs: &Path) -> Result<Option<Gitignore>> {
    let git = dir_abs.join(".gitignore");
    let tfs = dir_abs.join(".tfsignore");

    // Read files if present (async).
    let git_txt = match fs::read_to_string(&git).await {
        Ok(s) => Some(s),
        Err(e) if e.kind() == ErrorKind::NotFound => None,
        Err(e) => {
            return Err(e).with_context(|| format!("reading {}", git.display()));
        }
    };
    let tfs_txt = match fs::read_to_string(&tfs).await {
        Ok(s) => Some(s),
        Err(e) if e.kind() == ErrorKind::NotFound => None,
        Err(e) => {
            return Err(e).with_context(|| format!("reading {}", tfs.display()));
        }
    };

    if git_txt.is_none() && tfs_txt.is_none() {
        return Ok(None);
    }

    // Build matcher from lines.
    let mut b = GitignoreBuilder::new(dir_abs);
    if let Some(s) = git_txt {
        for line in s.lines() {
            // Treat both files with identical gitignore semantics.
            b.add_line(Some(dir_abs.to_path_buf()), line)
                .with_context(|| {
                    format!(
                        "invalid ignore pattern in {} (from .gitignore)",
                        dir_abs.display()
                    )
                })?;
        }
    }
    if let Some(s) = tfs_txt {
        for line in s.lines() {
            b.add_line(Some(dir_abs.to_path_buf()), line)
                .with_context(|| {
                    format!(
                        "invalid ignore pattern in {} (from .tfsignore)",
                        dir_abs.display()
                    )
                })?;
        }
    }
    let gi = b
        .build()
        .with_context(|| format!("building ignore matcher for {}", dir_abs.display()))?;
    Ok(Some(gi))
}

// --- Local DirectoryLister (Tokio, ignores, rel paths, skip dir symlinks) ---

pub struct LocalDirectoryLister {
    root_abs: Arc<PathBuf>,
    dir_abs: PathBuf,
    // Relative to the root ("" for the root itself)
    rel_dir: PathBuf,
    names_sorted: Vec<OsString>, // alphabetically sorted basenames
    idx: usize,
    ignores: IgnoreChain,
}

impl std::fmt::Debug for LocalDirectoryLister {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LocalDirectoryLister")
            .field("dir_abs", &self.dir_abs)
            .field("rel_dir", &self.rel_dir)
            .field("len", &self.names_sorted.len())
            .field("idx", &self.idx)
            .finish()
    }
}

impl LocalDirectoryLister {
    pub async fn new_root(root: &Path) -> Result<Self> {
        let root_abs = fs::canonicalize(root)
            .await
            .with_context(|| format!("canonicalize {}", root.display()))?;
        let rel_dir = PathBuf::new();

        let mut names = Vec::<OsString>::new();
        let mut rd = fs::read_dir(&root_abs)
            .await
            .with_context(|| format!("read_dir {}", root_abs.display()))?;
        while let Some(ent) = rd
            .next_entry()
            .await
            .with_context(|| format!("read_dir next_entry {}", root_abs.display()))?
        {
            names.push(ent.file_name());
        }
        names.sort_by(|a, b| a.to_string_lossy().cmp(&b.to_string_lossy()));

        let ignores = IgnoreChain::new(root_abs.clone())
            .extend_with_dir(&root_abs)
            .await?;

        Ok(Self {
            root_abs: Arc::new(root_abs.clone()),
            dir_abs: root_abs,
            rel_dir,
            names_sorted: names,
            idx: 0,
            ignores,
        })
    }

    async fn new_child(
        root_abs: Arc<PathBuf>,
        parent_ignores: &IgnoreChain,
        parent_rel_dir: &Path,
        child_name: &OsString,
    ) -> Result<Self> {
        let dir_abs = root_abs.join(parent_rel_dir).join(child_name);
        let rel_dir = parent_rel_dir.join(child_name);

        let mut names = Vec::<OsString>::new();
        let mut rd = fs::read_dir(&dir_abs)
            .await
            .with_context(|| format!("read_dir {}", dir_abs.display()))?;
        while let Some(ent) = rd
            .next_entry()
            .await
            .with_context(|| format!("read_dir next_entry {}", dir_abs.display()))?
        {
            names.push(ent.file_name());
        }
        names.sort_by(|a, b| a.to_string_lossy().cmp(&b.to_string_lossy()));

        let ignores = parent_ignores.extend_with_dir(&dir_abs).await?;

        Ok(Self {
            root_abs,
            dir_abs,
            rel_dir,
            names_sorted: names,
            idx: 0,
            ignores,
        })
    }
}

#[async_trait]
impl DirectoryLister for LocalDirectoryLister {
    async fn next(&mut self) -> Result<Option<DirEntry>> {
        loop {
            if self.idx >= self.names_sorted.len() {
                return Ok(None);
            }

            let name_os = self.names_sorted[self.idx].clone();
            self.idx += 1;

            let name = name_os.to_string_lossy().into_owned();
            let rel_path = self.rel_dir.join(&name_os);
            let abs_path = self.root_abs.join(&rel_path);

            // Use symlink_metadata first so we can detect symlinks without following.
            let lmeta = match fs::symlink_metadata(&abs_path).await {
                Ok(m) => m,
                Err(e) => {
                    // Can't stat; skip entry and continue.
                    // (Alternatively, return Err(e) to fail-fast.)
                    eprintln!("warn: failed to stat {}: {e}", abs_path.display());
                    continue;
                }
            };

            let ftype = lmeta.file_type();
            let is_symlink = ftype.is_symlink();

            // Decide file / dir. For symlinks, we only follow for files (optional).
            // We *skip symlinked directories entirely* as requested.
            if ftype.is_dir() {
                // Real directory (not a symlink dir).
                if self.ignores.is_ignored(&abs_path, true) {
                    continue;
                }
                // Ignore .git and .tfs directories
                if name == ".git" || name == ".tfs" {
                    continue;
                }
                let child = LocalDirectoryLister::new_child(
                    self.root_abs.clone(),
                    &self.ignores,
                    &self.rel_dir,
                    &name_os,
                )
                .await?;

                return Ok(Some(DirEntry::Directory(DirectoryEntry {
                    name,
                    rel_path,
                    abs_path,
                    lister: Box::new(child),
                })));
            }

            if is_symlink {
                // Follow target only to see if it's a file; skip if it is (or points to) a directory.
                match fs::metadata(&abs_path).await {
                    Ok(target_meta) => {
                        if target_meta.is_dir() {
                            // symlink -> dir: skip
                            continue;
                        }
                        if target_meta.is_file() {
                            if self.ignores.is_ignored(&abs_path, false) {
                                continue;
                            }
                            let size = target_meta.len();
                            let executable = is_executable(&target_meta, &abs_path);
                            return Ok(Some(DirEntry::File(FileEntry {
                                name,
                                rel_path,
                                abs_path,
                                size,
                                executable,
                            })));
                        }
                        // special type: skip
                        continue;
                    }
                    Err(_) => {
                        // Broken symlink: skip
                        continue;
                    }
                }
            }

            if lmeta.is_file() {
                if self.ignores.is_ignored(&abs_path, false) {
                    continue;
                }
                let size = lmeta.len();
                let executable = is_executable(&lmeta, &abs_path);
                return Ok(Some(DirEntry::File(FileEntry {
                    name,
                    rel_path,
                    abs_path,
                    size,
                    executable,
                })));
            }

            // Special file (fifo, socket, device): skip
            continue;
        }
    }
}

// Helpers

fn is_executable(meta: &std::fs::Metadata, _path: &Path) -> bool {
    use std::os::unix::fs::PermissionsExt;
    (meta.permissions().mode() & 0o111) != 0
}

/// Filesystem iterator over chunks of a file.
#[derive(Clone, Debug)]
pub struct FsFileChunksIterator {
    path: Arc<PathBuf>,
    file_len: u64,
    cursor: u64,
}

impl FsFileChunksIterator {
    #[inline]
    fn choose_chunk_size(remaining: u64) -> usize {
        // Pick the largest CHUNK_SIZES entry <= remaining; if none, use remaining as tail.
        let mut best: u64 = 0;
        for &s in CHUNK_SIZES.iter() {
            let sz = s as u64;
            if sz <= remaining && sz > best {
                best = sz;
            }
        }
        if best == 0 {
            remaining as usize
        } else {
            best as usize
        }
    }
}

#[async_trait]
impl FileChunksIterator for FsFileChunksIterator {
    async fn next(&mut self) -> Result<Option<Box<dyn FileChunkHandle>>> {
        if self.cursor >= self.file_len {
            return Ok(None);
        }

        let remaining = self.file_len - self.cursor;
        let this_size = Self::choose_chunk_size(remaining);
        let offset = self.cursor as usize;

        self.cursor = self
            .cursor
            .saturating_add(this_size as u64)
            .min(self.file_len);

        let handle = FsFileChunkHandle {
            path: Arc::clone(&self.path),
            offset,
            size: this_size,
        };

        Ok(Some(Box::new(handle)))
    }
}

/// Filesystem-backed chunk handle (lazy reader for a byte range).
#[derive(Clone, Debug)]
pub struct FsFileChunkHandle {
    path: Arc<PathBuf>,
    offset: usize,
    size: usize,
}

#[async_trait]
impl FileChunkHandle for FsFileChunkHandle {
    fn size(&self) -> usize {
        self.size
    }

    fn offset(&self) -> usize {
        self.offset
    }

    async fn get_chunk(&self) -> Result<Bytes> {
        let mut f = fs::File::open(&*self.path)
            .await
            .with_context(|| format!("open {}", self.path.display()))?;
        f.seek(SeekFrom::Start(self.offset as u64))
            .await
            .with_context(|| format!("seek to {} in {}", self.offset, self.path.display()))?;

        let mut buf = vec![0u8; self.size];
        f.read_exact(&mut buf).await.with_context(|| {
            format!(
                "read {} bytes at offset {} from {}",
                self.size,
                self.offset,
                self.path.display()
            )
        })?;
        Ok(Bytes::from(buf))
    }
}
