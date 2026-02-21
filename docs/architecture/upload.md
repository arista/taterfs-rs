# Upload

Uploading files and directories to a repository is a core function of the system.

## Basic File Upload

A file is uploaded from a [file store](./file_stores.md) to a [repository](./repository_interface.md) using a [list builder](./list_builder.md) populated by [repository objects](./backend_storage_model.md).   The process is straightforward:

```
async upload_file(store: FileStore, repo: Repo, path: Path) -> WithComplete<File>
```

* Create a FileListBuilder
* Call get_source_chunk_contents() on the file_store and get a SourceChunkContents()
* For each SourceChunkContent:
    * Call Repo.write, passing in the object id and bytes
    * Create a ChunkFilePart with the chunk's size and id
    * Add the ChunkFilePart to the FileListBuilder
* Call finish() on the ListBuilder
* Return the File and complete from the finish() call

All the flow control mechanisms should be managed by the repo and file store, with memory being managed by an Arc<ManagedBuffer> being passed, so this function should not need to worry about any of that.

## Basic Directory Upload

A directory is uploaded from a [file store](./file_stores.md) to a [repository](./repository_interface.md) using a [list builder](./list_builder.md) populated by [repository objects](./backend_storage_model.md).

The process looks like this:

```
async upload_directory(store: FileStore, repo: Repo, path: Option<string>) -> WithComplete<Directory>
```

* Call scan(path) on the file_store and get a ScanEvents
* Pass it to the upload_directory_from_scan_events described below:

```
async upload_directory_from_scan_events(store: FileStore, repo: Repo, path: PathBuf, cache_path_id: Option<DbId>, scan_events: ScanEvents) -> WithComplete<Directory>
```

* Create a DirectoryListBuilder
* For each ScanEvent:
    * Exit directory, or None
        * call finish() on the DirectoryListBuilder
        * return its Directory and complete flag
    * Enter directory
        * Recursively call upload_directory_from_scan_events
            * pass in an updated path
            * pass in a corresponding DbId obtained from the FileStore's cache
            * pass in the current ScanEvents
        * Add its Directory/complete result to the DirectoryListBuilder
    * File
        * Call get_fingerprinted_file_info from the FileStore's cache
            * If:
                * The cached fingerprint matches the fingerprint, AND
                * The repo's cache says the corresponding hash has been fully stored
            * Then
                * Add a FileEntry with the corresponding hash to the DirectoryListBuilder, and a NoopComplete
            * Else:
                * Call upload_file
                * add a FileEntry with the corresponding File and complete
                * Call set_fingerprinted_file_info on the FileStore's cache

## Streamed File Upload

In some cases, the source of a file might be computed and streamed on the fly, such as when [merging files](./merge.md).  To assist with this, a helper struct should be defined:

```
StreamingFileUploader {
  new(repo: Repo)
  async add(ManagedBuffer)
  async finish() -> WithComplete<FileId>
}
```

An application would construct an instance of this, call add() to add buffers of various sizes, then call finish().  It is an error to call add() after calling finish().

Calls to add() and finish() should return quickly - they don't have to wait for all the content to be uploaded, since that's handled by the Complete flag returned by WithComplete.  Flow control is handled on the application side when it acquires ManagedBuffers.

The StreamingFileUploader should segment files into chunks of CHUNK_SIZES according to the same algorithm used by FileStore.get_source_chunks_with_content.  To do this, it should maintain a list of buffers that have been add()ed to it.  Once the list reaches the size of the largest CHUNK_SIZE, it should flush out a chunk of that size (possibly pieced together from multiple buffers), releasing any buffers that have been fully written.  When finish() is called, it should similarly write out the remaining content broken down by the remaining CHUNK_SIZES.
