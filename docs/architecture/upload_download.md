# Upload/Download

Uploading and downloading files and directories is a core function of the system.  There are basic algorithms for doing this, as well as more complex algorithms for downloading into an existing filesystem with minimal disruption.

## Basic Upload

### Basic File Upload

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

### Basic Directory Upload

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

### Download

A repo Directory is downloaded recursively from a [repository](./repository_interface.md) to a location in a [file store](./file_stores.md), such that the resulting file store location has the same contents as the repo directory.  There may already be files and directories in that file store, so as the download proceeds, files and directories may need to be removed or replaced.

The download process is built on the download_repo_to_store function, which handles the logic of determining what modifications need to be made to the store.  The actions are expressed as calls to the DownloadAction interface:

```
DownloadActions {
    rm(path: &Path)
    mkdir(path: &Path)
    download_file(path: &Path, file_id: ObjectId, executable: bool)
    set_executable(path: &Path, executable: bool)
}
```

The process operates on this structure, which represents the state needed to download a single directory.  As the process descends recursively through the directory structure, new instances of this structure will be instantiated and called recursively.

```
DownloadRepoToStore {
  repo: Repo
  actions: DownloadActions
  file_store_cache: FileStoreCache

  // The path within the store that is being downloaded
  store_path: Path
  // The path id within the file store cache
  cache_path_id: DbId
  // The repo Directory being downloaded
  repo_directory_id: ObjectId
  // The entries of the store's directory, None if the directory doesn't yet exist in the store
  store_entries: Option<DirectoryEntryList>
}
```

With this in place, the download_repo_to_store function looks like this:

```
async download_repo_to_store() {
  repo_entries = repo.list_directory_entries(repo_directory_id)
  repo_entry = repo_entries.next()
  store_entry = store_entries && store_entries.next()
  while true {
    if repo_entry != null {
      if store_entry != null {
        if repo_entry.name < store_entry.name {
          download_repo_entry(repo_entry)
          repo_entry = repo_entries.next()
        }
        else if repo_entry.name > store_entry.name {
          remove_store_entry(store_entry)
          store_entry = store_entries.next()
        }
        else {
          merge_entries(repo_entry, store_entry)
          repo_entry = repo_entries.next()
          store_entry = store_entries.next()
        }
      }
      else {
        download_repo_entry(repo_entry)
        repo_entry = repo_entries.next()
      }
    }
    else {
      if store_entry != null {
        remove_store_entry(store_entry)
        store_entry = file_store_entries.next()
      }
      else {
        break
      }
    }
  }
}


download_repo_entry(repo_entry) {
  case repo_entry {
    Directory -> {
      // descend recursively
      file_dest.mkdir(store_path)
      download_repo_to_store() recursive call with child repo_entry and no store_entries
    }
    File -> {
      file_dest.download_file(store_path, file id, file executable)
    }
  }
}

remove_store_entry(store_entry) {
  actions.rm(store_path)
}

merge_entries(path, repo_entry, store_entry, file_dest) {
  case repo_entry {
    Directory -> {
      case store_entry -> {
        Directory -> {
          // descend recursively
          download_repo_to_store() recursive call with child repo_entry and child store_entries
        }
        File -> {
          // replace store file with repo directory
          file_dest.rm(store_path)
          file_dest.mkdir(store_path)
          download_repo_to_store() recursive call with child repo_entry and no store_entries
        }
      }
    }
    File -> {
      case store_entry -> {
        Directory -> {
          // replace store directory with repo file
          file_dest.rm(store_path)
          file_dest.download_file(store_path, file id, file executable)
        }
        File -> {
          check file_store_cache for the file's FingerprintedFileInfo
          if it has that info and its object id matches the repo_entry {
            if file_store's executable bit doesn't match the repo entry's executable bit {
              file_dest.set_executable(store_path, file executable)
            }
          }
          else {
            // replace store file with repo file
            file_dest.rm(store_path)
            file_dest.download_file(store_path, file id, file executable)
          }
        }
      }
    }
  }
}

```
