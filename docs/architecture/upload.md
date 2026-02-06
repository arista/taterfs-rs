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
