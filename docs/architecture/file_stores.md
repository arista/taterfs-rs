# File Stores

A File Store comprises two optional interfaces:

* FileSource - allows a File Store to be a source of directory and file data to be stored in a repository
* FileDest - allows a File Store to store directory and file data from a repository

A File Store will use this interface:

```
interface FileStore {
  get_source() -> FileSource | null
  get_dest() -> FileDest | null
}
```

A FileStore can return null for either interface if that function is not supported.

## Interfaces

### FileSource

A FileSource's main purpose is to allow scanning through directories and files.  Its interface looks something like this:

```
interface FileSource {
  // Walks depth-first through a directory structure yielding directory and file events in lexicographic order.  The scan starts at the given path, ending immediately if the path is not a directory.  The resulting ScanEvents are yielded relative to the path.  If path is None, then scan through the entire FileStore
  scan(Option<path>) -> ScanEvents
  // Yields the contents of a file, broken down into chunks of CHUNK_SIZES according to the method specified in [backend_storage_model](./backend_storage_model.md)
  get_source_chunks(path: Path) -> SourceChunks | null
  // Similar to get_source_chunks, except that multiple can be retrieved simultaneously but will still be returned in order
  get_source_chunk_contents(chunks: SourceChunks) -> SourceChunkContents | null
  // Return information about one file or directory
  get_entry(path: Path) -> DirectoryEntry | null
  // Retrieve an entire file's contents, error if the path is not a File.  This should only be used when the file is expected to be relatively small
  get_file(path: Path) -> Bytes
}

interface ScanEvents {
  async next() -> Option<ScanEvent>
}

enum ScanEvent {
  EnterDirectory(DirEntry)
  ExitDirectory
  File(FileEntry)
}

enum DirectoryScanEvent {
  EnterDirectory(DirEntry)
  ExitDirectory
}

interface DirEntry {
  name: string
  path: string // relative to the FileStore's root
}

interface FileEntry {
  name: string
  path: string // relative to the FileStore's root
  size: u64
  executable: bool
  // A string that should change when a file changes
  fingerprint: Option<string>
}

interface SourceChunks {
  async next() -> Option<SourceChunk>
}

interface SourceChunk {
  offset: u64
  size: u64
  async get() -> SoureChunkContent
}

interface SourceChunkContent {
  offset: u64
  size: u64
  bytes: Arc<ManagedBuffer>
  hash: "*sha-256 hash of the content in lower-case hexadecimal*"
}

interface SourceChunkContents {
  async next() -> Option<SourceChunkContent>
}

enum DirectoryEntry {
  Dir(DirEntry)
  File(FileEntry)
}
```

The fingerprint is used to quickly determine if a file has changed without reading the entire file's contents.  Each FileStore will implement this differently - the FSFileStore might include the last modified time, the S3FileStore might use the ETag, etc.  The fingerprint must be less than 128 characters, and must change when a file's content, **or executable bit**, *may* have changed.  If a FileStore cannot meet these requirements, then it should just leave this null.

Note that get_source_chunks iterates over a set of SourceChunk items, but doesn't actually retrieve content until SourceChunk.get() is called.  An application may call SourceChunk.get() in any order, and may even make multiple SourceChunk.get() calls simultaneously.  If a FileSource requires any kind of flow control (e.g., limiting network throughput), that is the responsibility of the FileSource implementation.

get_source_chunk_contents is similar to get_source_chunks, except that it doesn't require the two-step retrieval process.  It can use get_source_chunks underneath, and may initiate multiple concurrent chunk retrievals, but it must still return the chunks in order regardless of when the retrievals complete.

### FileDest

A FileDest allows files and directories to be written to a FileStore, ultimately with the goal of having the FileStore's contents match some portion of a repository's directory structure.  Its interface looks something like this:

```
interface FileDest {
// Return information about one file or directory
  get_entry(path: Path) -> Option<DirectoryEntry>
  // List the contents of a directory, error if the path does not point to a directory
  async list_directory(path: Path) -> Option<DirectoryList>
  // Write a file whose contents are supplied asynchronously by the given FileChunks
  async write_file_from_chunks(path: Path, chunks: SourceChunks)
  // Remove the file or directory at the given Path, if it exists
  async rm(path: Path)
  // Create a new directory at the given Path if there isn't yet a directory there, error if there is already a file there
  async mkdir(path: Path)
  // Change the executable bit of a file, error if the path does not point to a file
  async set_executable(path: Path, executable: bool)
}

interface DirectoryList {
  async next() -> Option<DirectoryEntry>
}
```

A FileDest implementation should do its best to avoid leaving a partially-written file, even if it's interrupted during a write_file_from_chunks operation.  Some implementations will, for example, build the file in a temporary location, then move the file to its final location atomically.

## Implementation Helpers

### ScanIgnoreHelper

Mutliple file stores will be expected to implement the following "ignore" rules in scan():

* If a global_ignores is specified in the [filestores] section of the [configuration](./configuration.md), then those entries are treated as a top-level ignore file in gitignore format.
* If a ".gitignore" is present, then its directives are followed the same way that git works
* If a ".tfsignore" is present, it is treated the same as ".gitignore"
* If both ".gitignore" and ".tfsignore" are present, they are treated as concatenated with ".gitignore" patterns first

Ignore rules are inherited from parent directories, following git semantics. When listing `foo/bar/`, the ignore patterns from `/.gitignore`, `/foo/.gitignore`, and `/foo/bar/.gitignore` are all applied.

The implementation uses the `ignore` crate for gitignore pattern matching, which provides full compatibility with git's ignore specification including:
- Glob patterns (`*.log`, `build/`)
- Negation patterns (`!important.log`)
- Directory-only patterns (`logs/`)
- Comments and blank lines

To implement these rules, FileStores can take advantage of a ScanIgnoreHelper.  This component has the following API:

```
interface ScanIgnoreHelper {
  async on_scan_event(event: DirectoryScanEvent, file_source: FileSource)
  should_ignore(name: string) -> bool
}
```

The idea is that the helper "follows" along with the FileSource as it enters and leaves directories.  Upon entering a directory, the helper uses the file_source's get_entry and get_file methods to check for a ".tfsignore" or ".gitignore", and push their directives into its context, then pop those directives upon leaving the directory.

Note that the ignore rules only come into play as part of the FileSource's scan() operation.  The other FileSource operations do not follow these ignore rules.

## Implementations

### MemoryFileStore

This implementation stores an in-memory representation of a filesystem and exposes both FileSource and FileDest to it.  This will most likely be used for testing.

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

The FileSource implementation should use the ScanIgnoreHelper to implement the ignore rules.

The FileStore should always return a fingerprint of None.

get_source_chunk_contents() should not attempt any concurrency, but should just retrieve and return the results of get_source_chunks in order

### FsFileStore

This implementation is pointed at a local filesystem path and implements both FileSource and FileDest using that path as the root.

```rust
let source = FsFileSource::new("/path/to/root");
```

On Unix systems, the executable flag is determined by checking if any execute bit is set in the file permissions.

The FileSource implementation for FsFileStore is relatively simple.  Note that the get_source_chunks() function should not read the entire file into memory, but should instead return SourceChunk items that open the file and retrieve a file range in response to SourceChunk.get().

The FsFileSource implementation should use the ScanIgnoreHelper to implement the ignore rules.

The FsFileStore should use a file fingerprint in the following format:

get_source_chunk_contents() should not attempt any concurrency, but should just retrieve and return the results of get_source_chunks in order

```
{last modified time in millis since the epoch}:{file size}:{executable bit, either "x" or "-"}
```

TODO: The FileDest.write_file_from_chunks() implementation is more complicated, as it involves downloading multiple chunks simultaneously into a temporary location (possibly with some flow control), assembling those chunks into the final file, then moving that file into its final location.

### S3FileStore

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

The S3FileStore should also be configured with a set of FlowControl services, similar to what is provided to Repo.  Those services should be used by get_source_chunks().

Notes:
- Uses the standard AWS credential chain (environment variables, ~/.aws, IAM roles)
- Directory listings use `list_objects_v2` with "/" delimiter to simulate directories
- Directory pages are fetched lazily as entries are consumed (handles arbitrarily large directories)
- File chunks are fetched lazily using S3 range requests (handles arbitrarily large files)
- The `executable` flag is always `false` since S3 doesn't track file permissions

The S3FileSource implementation should use the ScanIgnoreHelper to implement the ignore rules.

The S3FileStore should use a file fingerprint that is the ETag that S3 makes available for the file.  If the ETag is greater than 128 bytes (I don't think that would ever be the case), then just set it to null.

get_source_chunk_contents() should retrieve chunks concurrently, but still return them in order

TODO: for now, the S3FileStore does not offer a FileDest implementation

### HttpFileSource

This implementation is pointed at an HTTP url.

TODO: What HTTP API should it expect the server to implement?  Does WebDAV make sense (in which case, maybe it should be called WebDavFileSource)?  Should it try to work against a variety of HTTP api's?

## Specifying and Creating FileStores

When the application runs, it will need a way to specify which FileStores to use.  Those specifications will need to have enough information to construct a specific FileStore implementation, connected to particular caches, and capacity managers.  Some of that can be provided through configuration, while also providing a mechanism to override settings.

The basic way to reference a file store is through a URL:


```
s3://{bucket}/{prefix}
file://{directory}
http://...
```

Each of those URL's would lead to the creation of an S3FileStore, FsFileStore, or HttpFileStore

Some file store types might require additional parmeters.  The S3FileStore, for example, can optionally take an endpoint_url and a region.  Those can be specified as query parameters to the URL:

```
s3://{bucket}/{prefix}?endpoint_url=...&region=...
```

Besides URL's, a file store can also be specifed by name, in which case its parameters are taken from the [filestore.{name}] section of the [config file](./configuration.md), with an error if that named file store is not found.  The config file can also specify defaults for parameters like endpoint_url and region, if they haven't been specified in the url.

The capacity managers also need to be obtained.  There are potentially multiple sets of capacity managers - the ones corresponding to the [network] section of the config file, the ones corresponding to the [s3] section of the config file, then ones that might need to be created specifically for the file store.  For an FsFileStore, no throughput or request rate capacity managers should be used.

Once all of those elements are obtained, then the file store can be created.

All of this should be encapsulated in:

```
async create_file_store(file_store_spec: string, ctx: CreateFileStoreContext) -> Arc<FileStore>

CreateRepoContext {
  network_capacity_managers() -> CapacityManagers
  s3_capacity_managers() -> CapacityManagers
}

CapacityManagers {
  concurrent_requests() -> CapacityManager
  request_rate() -> CapacityManager
  read_throughput() -> CapacityManager
  write_throughput() -> CapacityManager
  total_throughput() -> CapacityManager
}
```

