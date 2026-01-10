# File Sources

A FileSource is a source of directory and file data to be stored in a repository.  The FileSource interface abstracts multiple potential implementations.

## Interfaces

A FileSource's task boils down to listing the items in a directory, and retrieving the chunks of a file.

```
trait FileSource {
  // List directory entries, returned via async iterator
  async list_directory(path: Path) -> DirectoryList
  // Get file chunks, returned via async iterator
  async get_file_chunks(path: Path) -> FileChunks
  // Get a single entry (file or directory) at a path
  async get_entry(path: Path) -> DirectoryListEntry | null
}

// Async iterator for directory entries
trait DirectoryListing {
  async next() -> DirectoryListEntry | null
}

// Async iterator for file chunks
trait FileChunking {
  async next() -> FileChunk | null
}
```

Directory entries are listed in lexical order.

```
enum DirectoryListEntry {
  Directory(DirEntry)
  File(FileEntry)
}

DirEntry {
    name: String,
    // Path from the FileSource root (uses OS separators)
    path: PathBuf,
}

FileEntry {
    name: String,
    // Path from the FileSource root (uses OS separators)
    path: PathBuf,
    size: u64,
    executable: bool,
}
```

To recursively list entries in a subdirectory, call `file_source.list_directory(&dir_entry.path)`.

TODO: links are currently ignored - should they be included?

### File Chunks

File chunks contain the actual file data and are used to break large files into smaller pieces for storage.

```
FileChunk {
    offset: u64,     // Offset of this chunk within the file
    data: Vec<u8>,   // The chunk data

    size() -> u64    // Size of this chunk in bytes
    offset() -> u64  // Offset within the file
    data() -> &[u8]  // Access the chunk data
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

The implementation uses the `ignore` crate for gitignore pattern matching, which provides full compatibility with git's ignore specification including:
- Glob patterns (`*.log`, `build/`)
- Negation patterns (`!important.log`)
- Directory-only patterns (`logs/`)
- Comments and blank lines
