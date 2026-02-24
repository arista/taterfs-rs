# File Stores

A File Store comprises two optional interfaces:

* FileSource - allows a File Store to be a source of directory and file data to be stored in a repository
* FileDest - allows a File Store to store directory and file data from a repository

A File Store will use this interface:

```
interface FileStore {
  get_source() -> FileSource | null
  get_dest() -> FileDest | null
  get_sync_state() -> StoreSyncState | null
}
```

A FileStore can return null for either interface if that function is not supported.

## Interfaces

### FileSource

A FileSource's main purpose is to allow scanning through directories and files.  Its interface looks something like this:

```
interface FileSource {
  // Walks depth-first through a directory structure yielding directory and file events in lexicographic order.  The scan starts at the given path, which must be a directory.  The resulting ScanEvents are yielded relative to the path.  If path is None, then scan through the entire FileStore
  scan(Option<path>) -> ScanEventList
  // Yields the locations of a file's contents broken down into chunks of CHUNK_SIZES according to the method specified in [backend_storage_model](./backend_storage_model.md)
  get_source_chunks(path: Path) -> SourceChunkList | null
  // Yields the contents of a file, broken down into chunks of CHUNK_SIZES according to the method specified in [backend_storage_model](./backend_storage_model.md).
  get_source_chunks_with_content(path: Path) -> SourceChunkWithContentList | null
  // Return information about one file or directory
  get_entry(path: Path) -> DirectoryEntry | null
  // Retrieve an entire file's contents, error if the path is not a File.  This should only be used when the file is expected to be relatively small
  get_file(path: Path) -> Bytes
}

interface ScanEventList {
  async next() -> Option<ScanEvent>
}

enum ScanEvent {
  EnterDirectory(DirEntry)
  ExitDirectory
  File(FileEntry)
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

interface SourceChunkList {
  async next() -> Option<SourceChunk>
}

interface SourceChunk {
  offset: u64
  size: u64
}

interface SourceChunkWithContentList {
  async next() -> Option<SourceChunkWithContent>
}

interface SourceChunkWithContent {
  offset: u64
  size: u64
  async content() -> SourceChunkContent
}

interface SourceChunkContent {
  bytes: Arc<ManagedBuffer>
  hash: "*sha-256 hash of the content in lower-case hexadecimal*"
}

enum DirectoryEntry {
  Dir(DirEntry)
  File(FileEntry)
}

impl DirectoryEntry {
  name() -> string
  path() -> string
}
```

The fingerprint is used to quickly determine if a file has changed without reading the entire file's contents.  Each FileStore will implement this differently - the FSFileStore might include the last modified time, the S3FileStore might use the ETag, etc.  The fingerprint must be less than 128 characters, and must change when a file's content, **or executable bit**, *may* have changed.  If a FileStore cannot meet these requirements, then it should just leave this null.

get_source_chunks_with_content() is intended to return content chunks in order, but also start downloading those chunks in the background while adhering to the constraints of a ManagedBuffers.  When SourceChunkWithContentList.next() is called, it will block until a ManagedBuffer is acquired that will hold the SourceChunkWithContent's content.  Once that ManagedBuffer is acquired, the SourceChunkWithContent will be returned, but a download of that content into the ManagedBuffer will also be initiated in the background (subject to any flow control required by the FileStore implementation, keeping in mind that the ManagedBuffer has already been acquired).  When SourceChunkWithContent.content() is called later by the application, it will block (if necessary) until that background download completes, and then it will return the ManagedBuffer.  The application is free to call .content() as many times as it wishes - each call should return the same ManagedBuffer (or block until the buffer is downloaded).

### FileDest

A FileDest allows files and directories to be written to a FileStore, ultimately with the goal of having the FileStore's contents match some portion of a repository's directory structure.  Its interface looks something like this:

```
interface FileDest {
// Return information about one file or directory
  get_entry(path: Path) -> Option<DirectoryEntry>
  // List the contents of a directory, error if the path does not point to a directory
  async list_directory(path: Path) -> Option<DirectoryList>
  // Write a file whose contents are supplied asynchronously by the given FileChunks
  async write_file_from_chunks(path: Path, chunks: FileChunkWithContentList, executable: bool) -> WithComplete<()>
  // Remove the file or directory at the given Path, if it exists
  async rm(path: Path)
  // Create a new directory at the given Path if there isn't yet a directory there, error if there is already a file there
  async mkdir(path: Path)
  // Change the executable bit of a file, error if the path does not point to a file
  async set_executable(path: Path, executable: bool)
  
  async create_stage() -> Option<FileDestStage>
}

interface DirectoryList {
  async next() -> Option<DirectoryEntry>
  // List the contents of a directory that is an immediate child of the directory represented by this structure, error if the name does not point to a directory
  async list_directory(name: string) -> Option<DirectoryList>
}
```

A FileDest implementation should do its best to avoid leaving a partially-written file, even if it's interrupted during a write_file_from_chunks operation.  Some implementations will, for example, build the file in a temporary location, then move the file to its final location atomically.

The list_directory() function (and its recursive DirectoryList.list_directory function) should respect ignore directives the same way that FileSource.scan does.

A FileDest is also responsible for updating the [cache](./caches.md) associated with the FileStore.  When write_file_from_chunks or set_executable is called, the cache should be updated by calling set_fingerprinted_file_info with the information obtained by calling get_entry (if that returns a FileEntry).

The write_file_from_chunks function will download multiple chunks in parallel, then write them to the file in sequential order.  The file should be written to a temporary location, then moved into its final location in the store atomically, if possible.  The function will return once all chunks have started downloading (i.e., after chunks.next() has successfully been called, which subjects it to ManagedBuffers flow control), and will then complete the WithComplete once all the downloads have completed.  The algorithm for this looks like:

```
async write_file_from_chunks(path: Path, chunks: FileChunkWithContentList, executable: bool) -> WithComplete<()> {
  create a NotifyComplete
  create a queue with elements Option<FileChunkWithContent>
  open a temporary file
  set the temporary file's executable bit according to "executable"

  start a separate process that runs a loop {
    wait to take the next Option<chunk> from the queue
    if None {
        close the temporary file
        move the file to its final location
        mark the NotifyComplete as complete
    }
    else {
        wait for the chunk's content()
        write the content to the temporary file
    }
  }

  loop through the FileChunkWithContentList {
    await next() to get the Option<chunk>
    if None {
      add None to the queue
      return a WithComplete<()> holding the NotifyComplete
    }
    else {
      add the chunk to the queue
    }
  }
}
```

### FileDestStage

Some FileDests will implement a "stage" for downloading files.  This is a mechanism for temporarily caching downloaded file chunks, allowing all required content to be pulled from a repo before modifying any existing directories or files in the FileStore.  Once all chunks have been downloaded to the stage, the files can be assembled and moved to their final locations in quick succession, thereby minimizing the time that the FileStore is being disrupted.

```
FileDestStage {
  async write_chunk(id: ObjectId, chunk: ManagedBuffer) -> Result<()>
  async read_chunk(id: ObjectId) -> Result<Option<ManagedBuffer>>
  async has_chunk(id: ObjectId) -> Result<bool>
  async cleanup() -> Result<()>
}
```

The cleanup function removes the stage and all of its content.

### StoreSyncState

FileStores are often "sync"'ed with a repository, meaning that changes in the FileStore will be reflected in the repo, and vice versa.  To support this, additional "SyncState" needs to be stored with the FileStore:

* repository_url - the repo to sync with
* repository_directory - the directory within the repo synced with the FileStore
* branch_name - the name of the branch to sync with
* base_commit - the commit against which changes will be registered.  When syncing, changes to the FileStore and changes to the repository will be merged, using this base_commit as the merge base.

Performing a sync is a multi-stage process in which the file store's content is uploaded, the file store's changes are merged against the repo's changes, then the merged result is downloaded to the file store.  That last step is sensitive - if it is interrupted, then the FileStore will be left in an ambiguous state.  Some parts of the FileStore may have been modified, but it will be ambiguous as to whether those modifications happened because of local changes, or because it was in the middle of downloading.

To address this, the StoreSyncState supports the notion of a "next" SyncState.  This is recorded in the FileStore to indicate that a sync operation is in its final download phase, with the intention of matching the commit specified in that "next" SyncState.  Once that download phase has completed successfully, the "next" SyncState will be "committed", by overwriting the current SyncState.

If a sync operation sees that there is already a "next" SyncState, it can first complete that operation, or alert the user of the interrupted download.

The interfaces look like this (all async):

```
StoreSyncState {
  get_sync_state() -> Option<SyncState>
  get_next_sync_state() -> Option<SyncState>
  set_next_sync_state(state: Option<SyncState>)
  commit_next_sync_state()
}

SyncState {
  created_at: "*time this SyncState version was created, in ISO 8601 format*"
  repository_url: string
  repository_directory: string
  branch_name: string
  base_commit: ObjectId
}
```

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

To implement these rules, FileStores can take advantage of a ScanIgnoreHelper.  This component defines its own interfaces (`ScanDirectoryEvent`, `ScanFileSource`) so it can be used outside the file_store module.  Its API:

```
interface ScanIgnoreHelper {
  async on_scan_event(event: ScanDirectoryEvent, source: ScanFileSource)
  should_ignore(name: string) -> bool
}

enum ScanDirectoryEvent {
  EnterDirectory(ScanDirEntry)
  ExitDirectory
}

interface ScanFileSource {
  async get_file(path: Path) -> Bytes
}
```

The idea is that the helper "follows" along with a scan as it enters and leaves directories.  Upon entering a directory, the helper uses the source's get_file method to check for a ".tfsignore" or ".gitignore", and push their directives into its context, then pop those directives upon leaving the directory.

FileSource implements ScanFileSource via a blanket impl, so FileStore implementations can pass themselves directly.  Callers convert DirEntry to ScanDirEntry (via `From`) when constructing ScanDirectoryEvent values.

Note that the ignore rules only come into play as part of the FileSource's scan() operation.  The other FileSource operations do not follow these ignore rules.

## Implementations

### MemoryFileStore

This implementation stores an in-memory representation of a filesystem and exposes both FileSource and FileDest to it.  Its FileDest does not provide a FileDestStage.  This will most likely be used for testing.

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

```
{last modified time in millis since the epoch}:{file size}:{executable bit, either "x" or "-"}
```

The FileDest.write_file_from_chunks() implementation should follow the write_file_from_chunks algorithm described above, storing its temporary file to `{destination path}/.download.{filename}-{temp filename}`

The FileDest implementation should offer a FileDestStage implementation:

* Each create_stage() should create a directory "{file store root}/.tfs/tmp/stage-{temp filename}/"
* Chunks are stored in the stage under "chunks/{id[0..2]}/{id[2..4]}/{id[4..6]}/{id}"
* While chunks are being written, they should first be written to a temporary file "chunks/{id[0..2]}/{id[2..4]}/{id[4..6]}/.download.{id}-{temp filename}", then moved atomically to their intended stage destination
* The cleanup() function should remove the entire stage directory

The FsFileStore should update the [LocalChunksCache](./caches.md) in response to operations and findings.  In all these cases, note that the path should be an absolute filesystem path, as opposed to a local path within the file store (since the LocalChunksCache is global).

* FileDest.rm should call LocalChunksCache.invalidate_local_chunks
* FileDest.write_file_from_chunks should call set_local_chunk for the chunks as they are written
* FileSource.get_source_chunks_with_content should call set_local_chunk for the chunks as they are read

The StoreSyncState interface should be implemented, with the current sync state stored in "{file store root}/.tfs/sync_state.json", and the next sync state stored in "{file store root}/.tfs/next_sync_state.json".  It is recommended that the json files be pretty-printed, rather than minimized.

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

TODO: for now, the S3FileStore does not offer a FileDest implementation or a StoreSyncState implementation

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

An FsFileStore can also be specified in short form through an absolute path:

```
/{directory}
```

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

