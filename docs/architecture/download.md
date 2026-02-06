# Download

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
  store_entries: Option<file store DirectoryList>
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
  }
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
}


download_repo_entry(repo_entry) {
  case repo_entry {
    Directory -> {
      // descend recursively
      file_dest.mkdir(store_path)
      for_child(name, repo_entry, None).download_repo_to_store
    }
    File -> {
      file_dest.download_file(store_path, file id, file executable)
    }
  }
}

remove_store_entry() {
  actions.rm(store_path)
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
          file_dest.rm(store_path)
          file_dest.mkdir(store_path)
          for_child(name, repo_entry, None).download_repo_to_store
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
          if it has that info and it matches the File's current fingerprint and its object id matches the repo_entry {
            // The file already exists and matches what would have been downloaded.  Check to see if its executable bit has changed
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
