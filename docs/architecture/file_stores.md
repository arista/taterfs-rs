# File Stores

A File Store comprises two optional interfaces:

* FileSource - allows a File Store to be a source of directory and file data to be stored in a repository
* FileDest - allows a File Store to retrieve and store directory and file data from a repository

A File Store will use this interface:

```
interface FileStore {
  get_source() -> FileSource | null
  get_dest() -> FileDest | null
}
```

## Interfaces

### FileSource

A FileSource's main purpose is to allow scanning through directories and files.  Its interface looks something like this:

```
interface FileSource {
  // Walks recursively through a directory structure yielding directory and file events in lexicographic order
  scan() -> ScanEvents
  // Yields the contents of a file, broken down into chunks of CHUNK_SIZES according to the method specified in (backend_storage_model)[./backend_storage_model.md]
  get_file_chunks(path: Path) -> FileChunks | null
}

interface ScanEvents {
  async next() -> ScanEvent
}

enum ScanEvent {
  EnterDirectory(DirEntry)
  ExitDirectory
  File(FileEntry)
}

interface DirEntry {
  name: string
  path: string // relative to the FileSource's root
}

interface FileEntry {
  name: string
  path: string // relative to the FileSource's root
  size: u64
  executable: bool
}

interface FileChunks {
  async next() -> Option<Bytes>
}

```
