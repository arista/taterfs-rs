# Download

Downloading files and directories is a core function of the system.  Because downloading is a potentially destructive action on a filesystem, the system tries to minimize the sensitive time during which destructive operations are taking place.  It does this by "staging" files from a repo as they are being downloaded, then moving them into position as quickly as possible once all files are available.

## Download File

```
async download_file(repo: Repo, file_id: ObjectId, store: FileStore, path: Path) -> WithComplete<()>
```

Downloads a single file from a [repository](./repository_interface.md) to a location in a [file store](./file_stores.md).  If the file already exists in the FileStore, it will be overwritten.  The store must expose a FileDest.

The function will block until it has acquired all the resources needed to complete the operation (usually ManagedBuffers), then mark the WithComplete as complete once the download has finished.

The function will call read_file_chunks_with_content, then pass that to FileDest.write_file_from_chunks, returning its WithComplete.

## Download Directory

```
async download_directory(repo: Repo, directory_id: ObjectId, store: FileStore, path: Path, with_stage: bool) -> WithComplete<()>
```

Recursively downloads a directory from a [repository](./repository_interface.md) to a location in a [file store](./file_stores.md).  The store must expose a FileDest.  The resulting file store location should end up with the same contents as the repo directory (with the exception of ignored files and directories), which means that existing files and directories in the store may end up getting removed or replaced.

If with_stage is false, then the download will proceed in a single phase, with files being downloaded using the FileDest.write_file_from_chunks calls.  This minimizes the need for temporary storage, but potentially leaves file store in a disrupted state for longer as it waits for files to be retrieved from the store.

If with_stage is true, then the download will retrieve all of the required files into a temporary "stage" location.  Once all of the required files are available locally, they will moved quickly into their final locations, thereby minimizing the time that the file store is in a disrupted state.

### DownloadActions

The download_directory function uses the download_repo_to_store function, which handles the logic of determining what modifications need to be made to the store.  The actions are expressed as calls to the DownloadActions interface:

```
DownloadActions {
    async rm(path: &Path) -> WithComplete<()>
    async mkdir(path: &Path) -> WithComplete<()>
    async download_file(path: &Path, file_id: ObjectId, executable: bool) -> WithComplete<()>
    async set_executable(path: &Path, executable: bool) -> WithComplete<()>
}
```

(all paths are expressed relative to the root of the file store)

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
  store_entries: Option<file store DirectoryList>
  // The Completes representing the completion flags of any WithCompletes returned by calls to actions
  completes: Completes
}

impl DownloadRepoToStore {
  new(repo, repo_directory_id, file_store, store_path, actions) {
    constructs the DownloadRepoToStore, filling in the appropriate cache_path_id by calling the file store cache from the file_store, and creating store_entries based on what the store_path points to in the file store (file_store.get_dest.get_entry):
      a directory -> the DirectoryList for that directory (file_store.get_dest.list_directory)
      nothing -> call actions.mkdir, and None
      a file -> call actions.rm, actions.mkdir, and None
  }
  
  for_child(&self, name, repo_entry, parent_store_entries?) {
    construct a new DonwloadRepoToStore that inherits values from this one, but represents a child directory with the given name.
      The store_path will append the name
      The cache_path_id will retrieve the child's path id from the file_store's cache.
      The repo_directory_id will be obtained from the repo_entry
      The store_entries is obtained by calling list_directory on the parent_store_entries (if any).
      The child's completes is added to this completes
  }
}
```

With this in place, the download_repo_to_store function looks like this:

```
async download_repo_to_store() -> WithComplete<()> {
  repo_entries = repo.list_directory_entries(repo_directory_id)
  repo_entry = repo_entries.next()
  store_entry = store_entries && store_entries.next()
  while true {
    if repo_entry != null {
      if store_entry != null {
        if repo_entry.name < store_entry.name {
          // Add repo entries until we reach a store entry
          download_repo_entry(repo_entry)
          repo_entry = repo_entries.next()
        }
        else if repo_entry.name > store_entry.name {
          // Remove store entries until we reach a repo entry
          remove_store_entry()
          store_entry = store_entries.next()
        }
        else {
          // Both repo and store entry exist
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
        remove_store_entry()
        store_entry = store_entries.next()
      }
      else {
        break
      }
    }
  }
  return WithComplete::new((), completes)
}


download_repo_entry(repo_entry) {
  case repo_entry {
    Directory -> {
      // descend recursively
      actions.mkdir(store_path) - add result to completes
      for_child(name, repo_entry, None).download_repo_to_store
    }
    File -> {
      actions.download_file(store_path, file id, file executable) - add result to completes
    }
  }
}

remove_store_entry() {
  actions.rm(store_path) - add result to completes
}

merge_entries(path, repo_entry, store_entry, file_dest) {
  case repo_entry {
    Directory -> {
      case store_entry -> {
        Directory -> {
          // descend recursively through both
          for_child(name, repo_entry, store_entries).download_repo_to_store
        }
        File -> {
          // replace store file with repo directory
          actions.rm(store_path) - add result to completes
          actions.mkdir(store_path) - add result to completes
          for_child(name, repo_entry, None).download_repo_to_store
        }
      }
    }
    File -> {
      case store_entry -> {
        Directory -> {
          // replace store directory with repo file
          actions.rm(store_path) - add result to completes
          actions.download_file(store_path, file id, file executable) - add result to completes
        }
        File -> {
          check file_store_cache for the file's FingerprintedFileInfo
          if it has that info and it matches the File's current fingerprint and its object id matches the repo_entry {
            // The file already exists and matches what would have been downloaded.  Check to see if its executable bit has changed
            if file_store's executable bit doesn't match the repo entry's executable bit {
              actions.set_executable(store_path, file executable) - add result to completes
            }
          }
          else {
            // replace store file with repo file
            actions.rm(store_path) - add result to completes
            actions.download_file(store_path, file id, file executable) - add result to completes
          }
        }
      }
    }
  }
}

```

### Staged Download

If a FileDest offers a [FileDestStage](./file_stores.md), then the download will proceed in two phases.  In the first phase, it downloads all required chunks to the stage.  In the second pass, it makes the required modifications to the file store, assembling the required files from the chunks on the stage.

The first phase looks like this:

```
{
  stage = create DownloadStage
  downloading = Set of object id's (to dedup chunk downloads)
  call download_repo_to_store with a DownloadToStageActions
  wait for the resulting WithComplete to complete
}

DownloadToStageActions {
  rm -> noop
  mkdir -> noop
  set_executable -> noop

  // download the file's chunks to the stage, deduping, downloading concurrently, with back pressure applied through ManagedBuffers.  Returns when resources have been acquired to perform the downloads, and the WithComplete completes when the downloads complete.
  download_file(... file_id) {
    completes = Completes
    file_chunks = repo.list_file_chunks(file_id)

    // Add chunks to download to the queue
    for each file_chunks.next() {
      if None {
        completes.done()
        return a WithComplete<()> with the completes
      }
      else {
        if !downloading.has(file_chunk.content), and !stage.has_chunk(file_chunk.content) {
          // record that we're going to download, to avoid duplicates
          downloading.add(file_chunk.content)
          complete = NotifyComplete
          completes.add(complete)
          get an AcquiredCapacity
          handle = spawn task {
            repo.read chunk using the acquired capacity
            write the chunk to the stage
            // the stage will now report that the chunk has been downloaded which will be sufficient to dedup
            downloading.remove(file_chunk.content)
          }
        }
      }
    }
  }
}
```

At this point, the stage should have downloaded all of the chunks required to complete the download operation.  All that should be required at this point is a second pass through download_repo_to_store, this time assembling files from the downloaded chunks.  However, it's possible that the FileStore changed between the two phases, and some new chunks need to be downloaded.  So the second phase should read from the stage if possible, but still fall back to reading from the repo.

The second phase executes by calling FileDest.write_file_from_chunks, passing in a StagedFileChunkWithContentList, which implements FileChunkWithContentList, but looks in the stage 

So the second phase looks like this:

```
{
  call download_repo_to_store with a DownloadWithStageActions
  wait for the resulting WithComplete to complete
}

DownloadWithStageActions {
  rm -> {make the appropriate fs call}
  mkdir -> {make the appropriate fs call}
  set_executable -> {make the appropriate fs call}

  download_file(... file_id) {
    call write_file_from_chunks with the StagedFileChunkWithContentList
  }
}
```
