// Thanks ChatGPT

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::fmt;
use std::ops::Deref;
use std::str::FromStr;

// =============================
// Zero-cost strong typedefs
// =============================

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct TimestampISOString(String);

impl TimestampISOString {
    pub fn new<S: Into<String>>(s: S) -> Self {
        Self(s.into())
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
impl Deref for TimestampISOString {
    type Target = str;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl From<&str> for TimestampISOString {
    fn from(s: &str) -> Self {
        Self(s.to_string())
    }
}
impl From<String> for TimestampISOString {
    fn from(s: String) -> Self {
        Self(s)
    }
}
impl std::fmt::Display for TimestampISOString {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ObjectId(String);

impl ObjectId {
    pub fn new<S: Into<String>>(s: S) -> Self {
        Self(s.into())
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
impl Deref for ObjectId {
    type Target = str;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl From<&str> for ObjectId {
    fn from(s: &str) -> Self {
        Self(s.to_string())
    }
}
impl From<String> for ObjectId {
    fn from(s: String) -> Self {
        Self(s)
    }
}
impl std::fmt::Display for ObjectId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl fmt::Debug for ObjectId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Short debug to keep logs tidy
        write!(f, "ObjectId({}…)", &self.0.get(0..8).unwrap_or(""))
    }
}

impl FromStr for ObjectId {
    type Err = &'static str;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // 64 hex chars (sha-256 style). Loosen if you use different hash.
        if s.len() == 64 && s.chars().all(|c| c.is_ascii_hexdigit()) {
            Ok(ObjectId(s.to_ascii_lowercase()))
        } else {
            Err("invalid ObjectId: expected 64 hex chars")
        }
    }
}

// =============================
// Top-level RepoObject
// =============================

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "PascalCase")]
pub enum RepoObject {
    Root(Root),
    Branches(Branches),
    Commit(Commit),
    Directory(Directory),
    File(File),
}

// -----------------------------
// Root / Branches / Branch
// -----------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Root {
    pub timestamp: TimestampISOString,
    pub default_branch_name: String,
    pub default_branch: Branch,
    pub other_branches: ObjectId, // points to Branches
    pub previous_root: Option<ObjectId>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Branches {
    pub branches_by_name: BTreeMap<String, Branch>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Branch {
    pub commit: ObjectId,
}

// -----------------------------
// Commit
// -----------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Commit {
    /// Root directory of the commit
    pub directory: ObjectId,
    /// Parent commits (can be merge)
    pub parents: Vec<ObjectId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<CommitMetadata>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CommitMetadata {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timestamp: Option<TimestampISOString>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub author: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub committer: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

// -----------------------------
// Directory
// -----------------------------

pub const MAX_DIRECTORY_ENTRIES: usize = 256;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Directory {
    pub entries: Vec<DirectoryPart>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "PascalCase")]
pub enum DirectoryEntry {
    File(FileEntry),
    Directory(DirEntry),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "PascalCase")]
pub enum DirectoryPart {
    File(FileEntry),
    Directory(DirEntry),
    Partial(PartialDirectory),
}

impl DirectoryPart {
    pub fn from(entry: DirectoryEntry) -> DirectoryPart {
        match entry {
            DirectoryEntry::File(file_entry) => DirectoryPart::File(file_entry),
            DirectoryEntry::Directory(dir_entry) => DirectoryPart::Directory(dir_entry),
        }
    }

    pub fn first_name(&self) -> &String {
        match self {
            DirectoryPart::File(part) => &part.name,
            DirectoryPart::Directory(part) => &part.name,
            DirectoryPart::Partial(part) => &part.first_name,
        }
    }

    pub fn last_name(&self) -> &String {
        match self {
            DirectoryPart::File(part) => &part.name,
            DirectoryPart::Directory(part) => &part.name,
            DirectoryPart::Partial(part) => &part.last_name,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FileEntry {
    pub name: String,
    pub size: u64,
    pub executable: bool,
    /// Hash of a File object
    pub file: ObjectId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DirEntry {
    pub name: String,
    /// Hash of a Directory object
    pub directory: ObjectId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PartialDirectory {
    pub first_name: String,
    pub last_name: String,
    /// Hash of a Directory object
    pub directory: ObjectId,
}

// -----------------------------
// File
// -----------------------------

pub const MAX_FILE_PARTS: usize = 64;
pub const CHUNK_SIZES: &[u32] = &[
    4_194_304, // 4 MiB
    1_048_576, // 1 MiB
    262_144,   // 256 KiB
    65_536,    // 64 KiB
    16_384,    // 16 KiB
];

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct File {
    pub parts: Vec<FilePart>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "PascalCase")]
pub enum FilePart {
    Chunk(ChunkFilePart),
    File(FileFilePart),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ChunkFilePart {
    pub size: u64,
    /// Hash of raw bytes object
    pub content: ObjectId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FileFilePart {
    pub size: u64,
    /// Hash of a File object
    pub file: ObjectId,
}

// =============================
// Validation & helpers
// =============================

#[derive(thiserror::Error, Debug)]
pub enum ValidateError {
    #[error("directory entry count {0} exceeds MAX_DIRECTORY_ENTRIES={1}")]
    DirTooMany(usize, usize),
    #[error(
        "directory entries must be uniformly typed: leaf(File/Directory) XOR internal(Partial)"
    )]
    DirMixedLeafInternal,
    #[error("directory entries not sorted by UTF-16 name at index {0}")]
    DirOrder(usize),
    #[error("partial ranges must be non-empty and ordered; problem at index {0}")]
    DirPartialOrder(usize),
    #[error("partial [{0}..={1}] is invalid (first > last) at index {2}")]
    DirPartialRange(String, String, usize),
    #[error("file parts count {0} exceeds MAX_FILE_PARTS={1}")]
    FileTooMany(usize, usize),
    #[error("file parts must be leaf(Chunk only) XOR internal(File only)")]
    FileMixedLeafInternal,
    #[error("chunk size {0} is not one of CHUNK_SIZES and not a tail remainder")]
    ChunkSize(u64),
}

fn cmp_utf16(a: &str, b: &str) -> Ordering {
    let a16 = a.encode_utf16();
    let b16 = b.encode_utf16();
    a16.cmp(b16)
}

fn is_sorted_utf16(names: impl Iterator<Item = String>) -> bool {
    let mut prev: Option<String> = None;
    for n in names {
        if let Some(p) = &prev {
            if cmp_utf16(p, &n) == Ordering::Greater {
                return false;
            }
        }
        prev = Some(n);
    }
    true
}

impl Directory {
    pub fn validate(&self) -> Result<(), ValidateError> {
        if self.entries.len() > MAX_DIRECTORY_ENTRIES {
            return Err(ValidateError::DirTooMany(
                self.entries.len(),
                MAX_DIRECTORY_ENTRIES,
            ));
        }
        let has_partial = self
            .entries
            .iter()
            .any(|e| matches!(e, DirectoryPart::Partial(_)));
        let has_file_or_dir = self
            .entries
            .iter()
            .any(|e| matches!(e, DirectoryPart::File(_) | DirectoryPart::Directory(_)));
        if has_partial && has_file_or_dir {
            return Err(ValidateError::DirMixedLeafInternal);
        }
        if has_file_or_dir {
            let names = self.entries.iter().map(|e| match e {
                DirectoryPart::File(f) => f.name.clone(),
                DirectoryPart::Directory(d) => d.name.clone(),
                _ => unreachable!(),
            });
            if !is_sorted_utf16(names) {
                for i in 1..self.entries.len() {
                    let prev = match &self.entries[i - 1] {
                        DirectoryPart::File(f) => &f.name,
                        DirectoryPart::Directory(d) => &d.name,
                        _ => continue,
                    };
                    let cur = match &self.entries[i] {
                        DirectoryPart::File(f) => &f.name,
                        DirectoryPart::Directory(d) => &d.name,
                        _ => continue,
                    };
                    if cmp_utf16(prev, cur) == Ordering::Greater {
                        return Err(ValidateError::DirOrder(i));
                    }
                }
            }
        } else {
            for (i, win) in self.entries.windows(2).enumerate() {
                let (a, b) = match (&win[0], &win[1]) {
                    (DirectoryPart::Partial(a), DirectoryPart::Partial(b)) => (a, b),
                    _ => continue,
                };
                if cmp_utf16(&a.first_name, &a.last_name) == Ordering::Greater {
                    return Err(ValidateError::DirPartialRange(
                        a.first_name.clone(),
                        a.last_name.clone(),
                        i,
                    ));
                }
                if cmp_utf16(&a.last_name, &b.first_name) == Ordering::Greater {
                    return Err(ValidateError::DirPartialOrder(i));
                }
            }
            for (i, p) in self.entries.iter().enumerate() {
                if let DirectoryPart::Partial(p) = p {
                    if cmp_utf16(&p.first_name, &p.last_name) == Ordering::Greater {
                        return Err(ValidateError::DirPartialRange(
                            p.first_name.clone(),
                            p.last_name.clone(),
                            i,
                        ));
                    }
                }
            }
        }
        Ok(())
    }
}

impl File {
    pub fn validate(&self) -> Result<(), ValidateError> {
        if self.parts.len() > MAX_FILE_PARTS {
            return Err(ValidateError::FileTooMany(self.parts.len(), MAX_FILE_PARTS));
        }
        let has_chunk = self.parts.iter().any(|p| matches!(p, FilePart::Chunk(_)));
        let has_file = self.parts.iter().any(|p| matches!(p, FilePart::File(_)));
        if has_chunk && has_file {
            return Err(ValidateError::FileMixedLeafInternal);
        }
        if has_chunk {
            for p in &self.parts {
                if let FilePart::Chunk(c) = p {
                    let ok = CHUNK_SIZES.iter().any(|&s| s as u64 == c.size);
                    if !ok && c.size >= CHUNK_SIZES.last().copied().unwrap() as u64 {
                        return Err(ValidateError::ChunkSize(c.size));
                    }
                }
            }
        }
        Ok(())
    }

    pub fn total_len(&self) -> u64 {
        self.parts
            .iter()
            .map(|p| match p {
                FilePart::Chunk(c) => c.size,
                FilePart::File(f) => f.size,
            })
            .sum()
    }
}

// =============================
// Builders (still “toy” stubs)
// =============================

pub fn build_directory_tree(mut leaves: Vec<DirectoryPart>) -> Directory {
    // sort by UTF-16 per spec
    leaves.sort_by(|a, b| {
        let an = match a {
            DirectoryPart::File(f) => &f.name,
            DirectoryPart::Directory(d) => &d.name,
            DirectoryPart::Partial(_) => unreachable!("leaves should not contain Partial"),
        };
        let bn = match b {
            DirectoryPart::File(f) => &f.name,
            DirectoryPart::Directory(d) => &d.name,
            DirectoryPart::Partial(_) => unreachable!("leaves should not contain Partial"),
        };
        cmp_utf16(an, bn)
    });

    let mut level: Vec<(String, String, Directory)> = Vec::new();
    for chunk in leaves.chunks(MAX_DIRECTORY_ENTRIES) {
        let first = match &chunk[0] {
            DirectoryPart::File(f) => f.name.clone(),
            DirectoryPart::Directory(d) => d.name.clone(),
            _ => unreachable!(),
        };
        let last = match &chunk[chunk.len() - 1] {
            DirectoryPart::File(f) => f.name.clone(),
            DirectoryPart::Directory(d) => d.name.clone(),
            _ => unreachable!(),
        };
        level.push((
            first,
            last,
            Directory {
                entries: chunk.to_vec(),
            },
        ));
    }

    if level.len() == 1 {
        return level.pop().unwrap().2;
    }

    loop {
        let mut next: Vec<(String, String, Directory)> = Vec::new();
        for group in level.chunks(MAX_DIRECTORY_ENTRIES) {
            let mut parent_entries = Vec::with_capacity(group.len());
            for (first, last, _child_dir_placeholder) in group {
                parent_entries.push(DirectoryPart::Partial(PartialDirectory {
                    first_name: first.clone(),
                    last_name: last.clone(),
                    // placeholder; fill after persisting children
                    directory: ObjectId::from(""),
                }));
            }
            let first = group.first().unwrap().0.clone();
            let last = group.last().unwrap().1.clone();
            next.push((
                first,
                last,
                Directory {
                    entries: parent_entries,
                },
            ));
        }
        if next.len() == 1 {
            return next.pop().unwrap().2;
        }
        level = next;
    }
}

pub fn build_file_tree(chunks: Vec<(u64, ObjectId)>) -> File {
    let mut leaves: Vec<File> = Vec::new();
    for group in chunks.chunks(MAX_FILE_PARTS) {
        let parts = group
            .iter()
            .map(|(size, content)| {
                FilePart::Chunk(ChunkFilePart {
                    size: *size,
                    content: content.clone(),
                })
            })
            .collect::<Vec<_>>();
        leaves.push(File { parts });
    }
    if leaves.len() == 1 {
        return leaves.pop().unwrap();
    }

    let mut level = leaves;
    loop {
        let mut next: Vec<File> = Vec::new();
        for group in level.chunks(MAX_FILE_PARTS) {
            let mut parts = Vec::with_capacity(group.len());
            for child in group {
                let size = child.total_len();
                parts.push(FilePart::File(FileFilePart {
                    size,
                    // placeholder; fill after writing child
                    file: ObjectId::from(""),
                }));
            }
            next.push(File { parts });
        }
        if next.len() == 1 {
            return next.pop().unwrap();
        }
        level = next;
    }
}

// =============================
// Hashing / canonical JSON
// =============================

use sha2::{Digest, Sha256};

pub fn sha256_hex_id(bytes: &[u8]) -> ObjectId {
    let mut h = Sha256::new();
    h.update(bytes);
    let out = h.finalize();
    ObjectId::from(hex::encode(out))
}

pub fn to_canonical_json_bytes<T: Serialize>(value: &T) -> Bytes {
    let s = serde_jcs::to_string(value).expect("canonical JSON");
    s.into_bytes().into()
}

pub fn object_hash(value: &RepoObject) -> ObjectId {
    let bytes = to_canonical_json_bytes(value);
    sha256_hex_id(&bytes)
}

pub fn bytes_hash(bytes: &Bytes) -> ObjectId {
    sha256_hex_id(&bytes)
}
