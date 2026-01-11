# File Sources

A FileSource is a source of directory and file data to be stored in a repository.  The FileSource interface abstracts multiple potential implementations.

## Interfaces

A FileSource's task boils down to listing the items in a directory, and retrieving the chunks of a file.

### FileSource Trait

```rust
trait FileSource: Send + Sync {
    // List directory entries, returned via async iterator
    async fn list_directory(&self, path: &Path) -> Result<DirectoryList>;
    // Get file chunks, returned via async iterator
    async fn get_file_chunks(&self, path: &Path) -> Result<FileChunks>;
    // Get a single entry (file or directory) at a path
    async fn get_entry(&self, path: &Path) -> Result<Option<DirectoryListEntry>>;
}
```

### Directory Entries

```rust
enum DirectoryListEntry {
    Directory(DirEntry),
    File(FileEntry),
}

struct DirEntry {
    pub name: String,     // Base name of the directory
    pub path: PathBuf,    // Path from the FileSource root (uses OS separators)
    // Methods:
    async fn list_directory(&self) -> Result<DirectoryList>;
    async fn get_entry(&self, name: &str) -> Result<Option<DirectoryListEntry>>;
}

struct FileEntry {
    pub name: String,     // Base name of the file
    pub path: PathBuf,    // Path from the FileSource root (uses OS separators)
    pub size: u64,        // Size of the file in bytes
    pub executable: bool, // Whether the file is executable
    // Methods:
    async fn get_chunks(&self) -> Result<FileChunks>;
}
```

Directory entries are listed in lexical order.

### Entry-Based Operations

The preferred way to traverse a file tree is using the entry-based API. Each `DirEntry` provides methods to list its contents or get specific children, and each `FileEntry` provides a method to get its chunks:

```rust
// Start from the root
let mut root_list = source.list_directory(Path::new("")).await?;

// Iterate through entries
while let Some(entry) = root_list.next().await? {
    match entry {
        DirectoryListEntry::Directory(dir) => {
            // List directory contents
            let mut contents = dir.list_directory().await?;

            // Or get a specific child by name
            if let Some(child) = dir.get_entry("specific_file.txt").await? {
                // ...
            }
        }
        DirectoryListEntry::File(file) => {
            // Get file chunks
            let mut chunks = file.get_chunks().await?;
            while let Some(chunk) = chunks.next().await? {
                // Process chunk data...
            }
        }
    }
}
```

This entry-based approach has several advantages:
- Natural tree traversal without needing to construct paths
- Context propagation (e.g., IgnoringFileSource passes ignore rules down the tree)
- Efficient lazy loading of directory contents

TODO: links are currently ignored - should they be included?

### File Chunks

File chunks contain the actual file data and are used to break large files into smaller pieces for storage.

```rust
struct FileChunk {
    offset: u64,     // Offset of this chunk within the file
    data: Vec<u8>,   // The chunk data

    fn size(&self) -> u64;    // Size of this chunk in bytes
    fn offset(&self) -> u64;  // Offset within the file
    fn data(&self) -> &[u8];  // Access the chunk data
}
```

#### Chunking Algorithm

Files are broken into chunks according to the algorithm defined in [backend_storage_model.md](./backend_storage_model.md). The chunk sizes are (in descending order):

- 4 MB (4,194,304 bytes)
- 1 MB (1,048,576 bytes)
- 256 KB (262,144 bytes)
- 64 KB (65,536 bytes)
- 16 KB (16,384 bytes)

When chunking a file, the process selects the largest chunk size from this list that is less than or equal to the remaining file size. This continues until no entry from the table fits, at which point the remainder becomes the final chunk.

This approach optimizes for files that grow from their end, minimizing changes between versions.

## Implementations

### MemoryFileSource

This implementation stores an in-memory representation of a filesystem and exposes a FileSource to access it.  This will most likely be used for testing.

The implementation provides a builder API for defining the file hierarchy in memory:

```rust
let source = MemoryFileSource::builder()
    .add("file.txt", MemoryFsEntry::file("contents"))
    .add("script.sh", MemoryFsEntry::executable("#!/bin/bash"))
    .add("large.bin", MemoryFsEntry::repeated(b"x", 10_000_000))
    .add("dir/nested.txt", MemoryFsEntry::file("nested"))
    .build();
```

Entry types:
- `MemoryFsEntry::file(contents)` - File with explicit contents
- `MemoryFsEntry::executable(contents)` - Executable file with explicit contents
- `MemoryFsEntry::repeated(pattern, size)` - File filled with repeated pattern to given size
- `MemoryFsEntry::repeated_executable(pattern, size)` - Executable file with repeated content
- `MemoryFsEntry::dir()` - Empty directory (usually created implicitly)

### FsFileSource

This implementation is pointed at a local filesystem path and will implement FileSource using that path as the root.

```rust
let source = FsFileSource::new("/path/to/root");
```

On Unix systems, the executable flag is determined by checking if any execute bit is set in the file permissions.

### S3FileSource

This implementation reads from an S3 bucket, treating S3 object keys as paths where "/" is used as the directory separator.

```rust
let config = S3FileSourceConfig::new("my-bucket")
    .with_prefix("data/root")           // Optional: prefix within the bucket
    .with_endpoint_url("http://localhost:4566")  // Optional: for LocalStack/MinIO
    .with_region("us-west-2");          // Optional: region override

let source = S3FileSource::new(config).await;
```

Configuration options:
- `bucket` - The S3 bucket name (required)
- `prefix` - Optional path prefix within the bucket
- `endpoint_url` - Custom endpoint URL for LocalStack, MinIO, or other S3-compatible services
- `region` - Region override (otherwise uses standard AWS credential chain)

Notes:
- Uses the standard AWS credential chain (environment variables, ~/.aws, IAM roles)
- Directory listings use `list_objects_v2` with "/" delimiter to simulate directories
- Directory pages are fetched lazily as entries are consumed (handles arbitrarily large directories)
- File chunks are fetched lazily using S3 range requests (handles arbitrarily large files)
- The `executable` flag is always `false` since S3 doesn't track file permissions

### HttpFileSource

This implementation is pointed at an HTTP url.

TODO: What HTTP API should it expect the server to implement?  Does WebDAV make sense (in which case, maybe it should be called WebDavFileSource)?  Should it try to work against a variety of HTTP api's?

### IgnoringFileSource

This is a FileSource that takes another FileSource as a parameter, and filters its output to ignore certain files.

```rust
let inner = FsFileSource::new("/path/to/root");
let source = IgnoringFileSource::new(inner);
```

The file ignoring rules are as follows:

* The directories ".tfs/" and ".git/" are always ignored
* If a ".gitignore" is present, then its directives are followed the same way that git works
* If a ".tfsignore" is present, it is treated the same as ".gitignore"
* If both ".gitignore" and ".tfsignore" are present, they are treated as concatenated with ".gitignore" patterns first

Ignore rules are inherited from parent directories, following git semantics. When listing `foo/bar/`, the ignore patterns from `/.gitignore`, `/foo/.gitignore`, and `/foo/bar/.gitignore` are all applied.

The ignore context is propagated through the entry-based API. When you traverse the tree using `dir.list_directory()` or `dir.get_entry(name)`, the returned entries automatically inherit the accumulated ignore rules. This makes traversal efficient - ignore files are loaded once per directory level rather than re-scanned for each operation.

The implementation uses the `ignore` crate for gitignore pattern matching, which provides full compatibility with git's ignore specification including:
- Glob patterns (`*.log`, `build/`)
- Negation patterns (`!important.log`)
- Directory-only patterns (`logs/`)
- Comments and blank lines
