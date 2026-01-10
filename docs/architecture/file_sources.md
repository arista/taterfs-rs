# File Sources

A FileSource is a source of directory and file data to be stored in a repository.  The FileSource interface abstracts multiple potential implementations.

## Interfaces

A FileSource's task boils down to listing the items in a directory, and retrieving the chunks of a file.

```
interface FileSource {
  list_directory(path: Path) -> Stream of DirectoryListEntry
  get_file_chunks(path: Path) -> Stream of FileChunks
  async get_entry(path: Path) -> DirectoryListEntry | null
}

```

Directory entries are listed in lexical order

```
enum DirectoryListEntry {
  Directory(DirEntry)
  File(FileEntry)
}

DirectoryListEntryName {
    name: String,
    abs_path: PathBuf,
    // Path relative to the listing root (uses OS separators)
    rel_path: PathBuf,
}

DirEntry {
    name: DirectoryListEntryName
    // Recursively list entries in this directory
    list_directory() -> Stream of DirectoryListEntry
}

FileEntry {
    name: DirectoryListEntryName
    size: u64,
    executable: bool,
}

```

TODO: links are currently ignored - should they be included?

```
FileChunk {
    size() -> u64
    // The offset of the chunk within the overall File
    offset() -> u64
    // Retrieve the contents of the chunk
    async get_chunk() -> bytes
}
```

## Implementations

### MemoryFileSource

This implementation stores an in-memory representation of a filesystem and exposes a FileSource to access it.  This will most likely be used for testing.

The implementation should provide a convenient mechanism for defining the file hierarchy in memory, including the executable flag and the contents of each file.  One should be able to define the entire contents of the file inline, or declare that the file should be of a certain length, filled with repeats of a given string.

### FsFileSource

This implementation is pointed at a local filesystem path and will implement FileSource using that path as the root.

### S3FileSource

This implementation is pointed at an S3 bucket and prefix, and will implement FileSource on the assumption that "/" should be used as directory separators.

### HttpFileSource

This implementation is pointed at an HTTP url.

TODO: What HTTP API should it expect the server to implement?  Does WebDAV make sense (in which case, maybe it should be called WebDavFileSource)?  Should it try to work against a variety of HTTP api's?

### IgnoringFileSource

This is a FileSource that takes another FileSource as a parameter, and filters its output to ignore certain files.  The file ignoring rules are as follows:

* The directory ".tfs/" should be ignored
* If a ".gitignore" is present, then its directives should be followed the same way that git works
* If a ".tfsignore" is present, it should be treated the same as ".gitignore"
* If both ".gitignore" and ".tfsignore" are present, it should behave as if they were concatenated into a single file, with ".gitignore" preceding ".tfsignore"

