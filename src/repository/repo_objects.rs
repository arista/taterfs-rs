//! Repository object types for taterfs backend storage.
//!
//! This module defines all JSON object types stored in a taterfs repository,
//! as specified in the backend storage model. All structural objects are
//! stored as canonical JSON (RFC 8785).

use serde::{Deserialize, Serialize};

/// Object ID is a sha-256 hash represented as a lowercase hexadecimal string.
pub type ObjectId = String;

// =============================================================================
// Conventional Limits
// =============================================================================

/// Maximum number of entries in a single Directory object before breaking into PartialDirectories.
pub const MAX_DIRECTORY_ENTRIES: usize = 256;

/// Maximum number of parts in a single File object before breaking into FileFileParts.
pub const MAX_FILE_PARTS: usize = 64;

/// Maximum number of entries in a Branches object before breaking into nested Branches.
pub const MAX_BRANCH_LIST_ENTRIES: usize = 64;

// =============================================================================
// Root
// =============================================================================

/// A repository's root object. A new Root is created with every change.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Root {
    /// Type discriminator, always "Root".
    #[serde(rename = "type")]
    pub type_tag: RootType,
    /// Creation time in ISO 8601 format.
    pub timestamp: String,
    /// Name of the default branch.
    pub default_branch_name: String,
    /// Object ID of the default Branch.
    pub default_branch: ObjectId,
    /// Object ID of the Branches object containing other branches.
    pub other_branches: ObjectId,
    /// Object ID of the previous Root, if any.
    pub previous_root: Option<ObjectId>,
}

/// Type tag for Root objects.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RootType {
    Root,
}

// =============================================================================
// Branches
// =============================================================================

/// A collection of branches. May contain inline Branch entries or references to nested Branches.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Branches {
    /// Type discriminator, always "Branches".
    #[serde(rename = "type")]
    pub type_tag: BranchesType,
    /// List of branch entries.
    pub branches: Vec<BranchListEntry>,
}

/// Type tag for Branches objects.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BranchesType {
    Branches,
}

/// An entry in a branch list - either a Branch or a reference to nested Branches.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum BranchListEntry {
    /// A branch definition.
    Branch(Branch),
    /// A reference to a nested Branches object for a range of branches.
    BranchesEntry(BranchesEntry),
}

/// A named branch pointing to a commit.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Branch {
    /// Name of the branch.
    pub name: String,
    /// Object ID of the branch's current Commit.
    pub commit: ObjectId,
}

/// A reference to a nested Branches object covering a range of branch names.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BranchesEntry {
    /// Name of the first entry in the referenced branches list.
    pub first_name: String,
    /// Name of the last entry in the referenced branches list.
    pub last_name: String,
    /// Object ID of the nested Branches object.
    pub branches: ObjectId,
}

// =============================================================================
// Commit
// =============================================================================

/// A commit representing a version of the repository.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Commit {
    /// Type discriminator, always "Commit".
    #[serde(rename = "type")]
    pub type_tag: CommitType,
    /// Object ID of the Directory at the root of this version.
    pub directory: ObjectId,
    /// Object IDs of parent commits (empty for initial commit, multiple for merges).
    pub parents: Vec<ObjectId>,
    /// Optional metadata about the commit.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<CommitMetadata>,
}

/// Type tag for Commit objects.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CommitType {
    Commit,
}

/// Optional metadata for a commit.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct CommitMetadata {
    /// Commit time in ISO 8601 format.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timestamp: Option<String>,
    /// Author of the changes.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub author: Option<String>,
    /// Person who created the commit.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub committer: Option<String>,
    /// Commit message.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

// =============================================================================
// Directory
// =============================================================================

/// A directory containing its entries inline.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Directory {
    /// Type discriminator, always "Directory".
    #[serde(rename = "type")]
    pub type_tag: DirectoryType,
    /// Directory entries (inlined).
    pub entries: Vec<DirectoryPart>,
}

/// Type tag for top-level Directory objects.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DirectoryType {
    Directory,
}

/// A part of a directory - either an entry (file or subdirectory) or a partial directory reference.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum DirectoryPart {
    /// A file entry in the directory.
    File(FileEntry),
    /// A subdirectory entry.
    Directory(DirEntry),
    /// A reference to a portion of the directory entries.
    Partial(PartialDirectory),
}

/// A file entry within a directory.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileEntry {
    /// Name of the file.
    pub name: String,
    /// Size of the file in bytes.
    pub size: u64,
    /// Whether the file is executable.
    pub executable: bool,
    /// Object ID of the File object.
    pub file: ObjectId,
}

/// A subdirectory entry within a directory.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DirEntry {
    /// Name of the subdirectory.
    pub name: String,
    /// Object ID of the Directory object.
    pub directory: ObjectId,
}

/// A reference to a portion of directory entries (for large directories).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PartialDirectory {
    /// Name of the first entry in the partial list.
    pub first_name: String,
    /// Name of the last entry in the partial list.
    pub last_name: String,
    /// Object ID of the Directory containing these entries.
    pub directory: ObjectId,
}

// =============================================================================
// File
// =============================================================================

/// A file containing its parts inline (chunks or nested file references).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct File {
    /// Type discriminator, always "File".
    #[serde(rename = "type")]
    pub type_tag: FileType,
    /// File parts (inlined).
    pub parts: Vec<FilePart>,
}

/// Type tag for top-level File objects.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FileType {
    File,
}

/// A part of a file - either a chunk of data or a reference to a nested File object.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum FilePart {
    /// A chunk of file data.
    Chunk(ChunkFilePart),
    /// A reference to a nested File object for very large files.
    File(FileFilePart),
}

/// A chunk of file data.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChunkFilePart {
    /// Size of the chunk in bytes.
    pub size: u64,
    /// Object ID of the raw chunk data.
    pub content: ObjectId,
}

/// A reference to a nested File object (for very large files).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileFilePart {
    /// Total size of this file part in bytes.
    pub size: u64,
    /// Object ID of the nested File object.
    pub file: ObjectId,
}

// =============================================================================
// RepoObject - Unified Enum
// =============================================================================

/// An enum encompassing all id'able repository objects.
///
/// This enum can represent any object that can be stored in the repository
/// and identified by an ObjectId.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum RepoObject {
    /// A repository root object.
    Root(Root),
    /// A collection of branches.
    Branches(Branches),
    /// A commit representing a version.
    Commit(Commit),
    /// A directory containing entries.
    Directory(Directory),
    /// A file containing parts.
    File(File),
}

impl RepoObject {
    /// Returns the type name of this object as it appears in JSON.
    pub fn type_name(&self) -> &'static str {
        match self {
            RepoObject::Root(_) => "Root",
            RepoObject::Branches(_) => "Branches",
            RepoObject::Commit(_) => "Commit",
            RepoObject::Directory(_) => "Directory",
            RepoObject::File(_) => "File",
        }
    }
}

// =============================================================================
// JSON Conversion Functions
// =============================================================================

/// Error type for JSON operations.
#[derive(Debug)]
pub enum JsonError {
    /// Serialization error.
    Serialize(serde_json::Error),
    /// Deserialization error.
    Deserialize(serde_json::Error),
}

impl std::fmt::Display for JsonError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JsonError::Serialize(e) => write!(f, "JSON serialization error: {}", e),
            JsonError::Deserialize(e) => write!(f, "JSON deserialization error: {}", e),
        }
    }
}

impl std::error::Error for JsonError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            JsonError::Serialize(e) | JsonError::Deserialize(e) => Some(e),
        }
    }
}

/// Result type for JSON operations.
pub type JsonResult<T> = std::result::Result<T, JsonError>;

/// Deserialize a repository object from JSON bytes.
pub fn from_json<T: for<'de> Deserialize<'de>>(json: &[u8]) -> JsonResult<T> {
    serde_json::from_slice(json).map_err(JsonError::Deserialize)
}

/// Deserialize a repository object from a JSON string.
pub fn from_json_str<T: for<'de> Deserialize<'de>>(json: &str) -> JsonResult<T> {
    serde_json::from_str(json).map_err(JsonError::Deserialize)
}

/// Serialize a repository object to canonical JSON (RFC 8785).
///
/// Canonical JSON has the following properties:
/// - No whitespace
/// - Object keys sorted lexicographically by UTF-16 code units
/// - Numbers in a specific format (no unnecessary precision)
pub fn to_canonical_json<T: Serialize>(value: &T) -> JsonResult<Vec<u8>> {
    serde_json_canonicalizer::to_vec(value).map_err(JsonError::Serialize)
}

/// Serialize a repository object to a canonical JSON string.
pub fn to_canonical_json_string<T: Serialize>(value: &T) -> JsonResult<String> {
    serde_json_canonicalizer::to_string(value).map_err(JsonError::Serialize)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_root_serialization() {
        let root = Root {
            type_tag: RootType::Root,
            timestamp: "2024-01-15T10:30:00Z".to_string(),
            default_branch_name: "main".to_string(),
            default_branch: "abc123".to_string(),
            other_branches: "def456".to_string(),
            previous_root: None,
        };

        let json = to_canonical_json_string(&root).unwrap();
        assert!(json.contains("\"type\":\"Root\""));
        assert!(json.contains("\"defaultBranchName\":\"main\""));

        let parsed: Root = from_json_str(&json).unwrap();
        assert_eq!(parsed, root);
    }

    #[test]
    fn test_branch_list_entry_serialization() {
        let branch = BranchListEntry::Branch(Branch {
            name: "feature".to_string(),
            commit: "abc123".to_string(),
        });

        let json = to_canonical_json_string(&branch).unwrap();
        assert!(json.contains("\"type\":\"Branch\""));

        let parsed: BranchListEntry = from_json_str(&json).unwrap();
        assert_eq!(parsed, branch);

        let branches_entry = BranchListEntry::BranchesEntry(BranchesEntry {
            first_name: "a".to_string(),
            last_name: "z".to_string(),
            branches: "def456".to_string(),
        });

        let json = to_canonical_json_string(&branches_entry).unwrap();
        assert!(json.contains("\"type\":\"BranchesEntry\""));
    }

    #[test]
    fn test_directory_part_serialization() {
        let file_entry = DirectoryPart::File(FileEntry {
            name: "test.txt".to_string(),
            size: 1024,
            executable: false,
            file: "abc123".to_string(),
        });

        let json = to_canonical_json_string(&file_entry).unwrap();
        assert!(json.contains("\"type\":\"File\""));
        assert!(json.contains("\"executable\":false"));

        let dir_entry = DirectoryPart::Directory(DirEntry {
            name: "subdir".to_string(),
            directory: "def456".to_string(),
        });

        let json = to_canonical_json_string(&dir_entry).unwrap();
        assert!(json.contains("\"type\":\"Directory\""));
    }

    #[test]
    fn test_file_part_serialization() {
        let chunk = FilePart::Chunk(ChunkFilePart {
            size: 4096,
            content: "abc123".to_string(),
        });

        let json = to_canonical_json_string(&chunk).unwrap();
        assert!(json.contains("\"type\":\"Chunk\""));

        let file_part = FilePart::File(FileFilePart {
            size: 1_000_000,
            file: "def456".to_string(),
        });

        let json = to_canonical_json_string(&file_part).unwrap();
        assert!(json.contains("\"type\":\"File\""));
    }

    #[test]
    fn test_canonical_json_key_ordering() {
        let commit = Commit {
            type_tag: CommitType::Commit,
            directory: "dir123".to_string(),
            parents: vec!["parent1".to_string()],
            metadata: None,
        };

        let json = to_canonical_json_string(&commit).unwrap();

        // Keys should be sorted: directory, parents, type
        let dir_pos = json.find("\"directory\"").unwrap();
        let parents_pos = json.find("\"parents\"").unwrap();
        let type_pos = json.find("\"type\"").unwrap();

        assert!(dir_pos < parents_pos);
        assert!(parents_pos < type_pos);
    }

    #[test]
    fn test_canonical_json_no_whitespace() {
        let root = Root {
            type_tag: RootType::Root,
            timestamp: "2024-01-15T10:30:00Z".to_string(),
            default_branch_name: "main".to_string(),
            default_branch: "abc123".to_string(),
            other_branches: "def456".to_string(),
            previous_root: Some("prev789".to_string()),
        };

        let json = to_canonical_json_string(&root).unwrap();

        // No spaces or newlines in canonical JSON
        assert!(!json.contains(" :"));
        assert!(!json.contains(": "));
        assert!(!json.contains(" ,"));
        assert!(!json.contains(", "));
        assert!(!json.contains('\n'));
    }

    #[test]
    fn test_repo_object_deserialization() {
        let root_json = r#"{"type":"Root","timestamp":"2024-01-15","defaultBranchName":"main","defaultBranch":"abc","otherBranches":"def","previousRoot":null}"#;
        let obj: RepoObject = from_json_str(root_json).unwrap();
        assert!(matches!(obj, RepoObject::Root(_)));

        let commit_json = r#"{"type":"Commit","directory":"abc","parents":["parent1"]}"#;
        let obj: RepoObject = from_json_str(commit_json).unwrap();
        assert!(matches!(obj, RepoObject::Commit(_)));
    }

    #[test]
    fn test_constants() {
        assert_eq!(MAX_DIRECTORY_ENTRIES, 256);
        assert_eq!(MAX_FILE_PARTS, 64);
        assert_eq!(MAX_BRANCH_LIST_ENTRIES, 64);
    }
}
