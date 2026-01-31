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

### Download Actions

A repo Directory is downloaded recursively from a [repository](./repository_interface.md) to a location in a [file store](./file_stores.md), such that the resulting file store location has the same contents as the repo directory.  There may already be files and directories in that file store, so as the download proceeds, files and directories may need to be removed or replaced.

To better specify the behavior, we introduce the concept of a DownloadAction, which represents one action needed to bring a FileStore closer to duplicating a Repository.  By executing a full sequence of these DownloadActions, a FileStore will end up containing the same contents as a Repository Directory.

```
enum DownloadAction {
  CreateDirectory(name)
  RemoveDirectory(name)
  RemoveFile(name)
  EnterDirectory(name)
  ExitDirectory
  DownloadFile(name, object_id)
}
```

A function then generates the list of these DownloadActions by calling scan_directory on the Repo, and calling scan on the FileStore's FileSource, "zippering" the two together, so that the list of DownloadActions is generated lazily:

```
async download_actions(repo: Repo, directory_id: ObjectId, file_store: FileStore, path: Path) -> DownloadActions

interface DownloadActions {
  async next() -> Option<DownloadAction>
}
```

The algorithm for download_actions is as follows:

Maintain the following state:

```
DownloadActionsState {
  // The queue of actions to be returned before generating new ones
  actions_queue: Queue of DownloadAction
  // The stack of directories recursively being examined
  dir_stack: stack of DownloadActionsDir
}

DownloadActionDir {
  // The repo's entries from the directory, None if the directory doesn't exist in the repo
  repo_entries: Option<Repo ScanEvents>
  // The latest entry read within the directory, None if none remain
  repo_entry: Option<Repo DirectoryEntry>
  // The file store's entries from the directory, None if the directory doesn't exist in the file store
  store_entries: Option<FileSource ScanEvents>
  // The latest entry read within the directory, None if none remain
  store_entry: Option<FileSource DirectoryEntry>
}
```

download_actions():

* Create a DownloadActionsState
* Create a Repo DirectoryScan for the root
* Get its first scan event, or None if it's None or ExitDirectory
* Create a FileSource ScanEvents for the root
* Get its first scan event, or None if it's None or ExitDirectory
* Push a new DownloadActionDir with all that info
* Create and return a DownloadActions that closes over all the state


DownloadActions.next():

* If actions_queue is empty
    * call generate_download_actions()
* If actions_queue is still empty
    * return None
* else
    * return the next action from the queue

DownloadActions.generate_download_actions():


```
if repo_entries is None {
    // scanning through a store directory that's not in the repo, so just ignore (it will be removed when done scanning)}
    advance_store_entry()
else {
    if store_entries is None {
        // Add repo entry to a store directory that doesn't exist yet
        add_repo_entry()
    }
    else {
        if repo_entry is None {
            // Out of repo entries, remove any remaining store entries
            if store_entry is None {
                end_directory()
            }
            else {
                remove_store_entry()
            }
        }
        else {
            if store_entry is None {
                // Out of directory entries, so add any repo entries
                add_repo_entry()
            }
            else {
                if repo_entry.name < store_entry.name {
                    // Repo entry comes before store entry, so add it
                    add_repo_entry()
                }
                else if repo_entry.name > store_entry.name {
                    // Repo entry comes after store entry, so remove the store entry
                    remove_store_entry()
                }
                else {
                    // Repo and store entries have the same name, so "merge" them
                    merge_entries()
                }
            }
        }
    }
}
```

add_repo_entry():

```
if repo_entry is DirectoryEntry {
  add CreateDirectory action
  push a new DownloadActionDir with repo_entries set, and store_entries None
  advance_repo()
}
else {
  add DownloadFile action
  advance_repo()
}
```

remove_store_entry():

```
if store_entry is DirectoryEntry {
  // FIXME
}
else {
  add RemoveFile action
}
```

merge_entries():

```
```

advance_repo():

```
```

end_directory():

```
```


* if Some(repo_entries)
    * if Some(store_entries)
        * if Some(repo_entry)
            * if Some(store_entry)
                * if repo_entry.name < store_entry.name
                    * add_repo_entry()
                * else if repo_entry.name > store_entry.name
                    * remove_store_entry()
                * else
                    * merge_entries()
            * else
                * add_repo_entry()
        * else
            * if Some(store_entry)
                * remove_store_entry()
            * else
                * FIXME
    * else
        * if Some(repo_entry)
            * add_repo_entry()
* else - we are scanning through a store directory that's not in the repo, so just ignore (it will be removed when done scanning)
    * advance_store_entry()
```

add_repo_entry():

remove_store_entry():

merge_entries():

advance_store_entry():
  store_entry = store_entries.next()
  if store_entry is None


* if repo_scan_event != null
    * if store_scan_event != null
        * if repo_scan_event.name < store_scan_event.name
            * add_from_repo(repo_scan_event)
        * else if repo_scan_event.name > store_scan_event.name
            * remove_from_store(store_scan_event)
        * else
            * merge_repo_and_store(repo_scan_event, store_scan_event)
    * else
        * add_from_repo(repo_scan_event)
* else
    * if store_scan_event != null
        * do nothing
    * else
        * exit_directory()


* add_from_repo(repo_scan_event)
    * if EnterDirectory
        * add CreateDirectory
        * add EnterDirectory
        * advance_repo
    * if FileEntry
        * add DownloadFile
        * advance_repo

* remove_from_store(store_scan_event)
    * if EnterDirectory
        * add RemoveDirectory
    * if FileEntry
        * add RemoveFile

* resolve_from_repo_and_store_scan_events(repo_scan_event, store_scan_event)

* exit_directory()


RepoScanEvent

    /// Entering a directory. The root directory will have an empty name.
    EnterDirectory(DirEntry),
    /// Exiting the current directory.
    ExitDirectory,
    /// A file in the current directory.
    File(FileEntry),

pub enum ScanEvent {
    /// Entering a directory.
    EnterDirectory(DirEntry),
    /// Exiting a directory (returning to parent).
    ExitDirectory,
    /// A file was encountered.
    File(FileEntry),
}
