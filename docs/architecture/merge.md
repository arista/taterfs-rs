# Merge

Performing a 3-way directory merge is a key element in combining changes from two streams of commits originating from a common commit.  Such a merge involves 3 directories: base, dir1, and dir2, and proceeds by "zippering" the entries from the three sources in sorted order.

## DirChange

A DirChange is a change from an entry in the base to the corresponding entry in dir (dir1 or dir2).  The entry and change are represented as:

```
enum ChangingDirEntry {
  None
  File(f: ObjectId, executable: bool)
  Directory(d: ObjectId)
}

enum DirEntryChange {
  Unchanged
  FileCreated(f, executable)
  FileChanged(base, Option<f>, Option<executable>) - Option indicates which, or both, changed
  FileChangedToDirectory(f, d)
  FileRemoved
  DirectoryCreated(d)
  DirectoryChanged(d1, d2)
  DirectoryChangedToFile(d, f, executable)
  DirectoryRemoved
}

impl DirEntryChange {
  new_directory() -> Option<directory ObjectId> {
    FileChangedToDirectory(d) => d
    DirectoryCreated(d) => d
    _ => None
  }

  new_file() -> Option<file ObjectId, executable> {
    FileCreated(f, executable) -> f, executable
    DirectoryChangedToFile(d, f, executable) -> f, executable
    _ => None
  }

  is_removed() -> bool {
    FileRemoved || DirectoryRemoved
  }
}
```

The algorithm looks like this:

```
to_dir_change(e1: ChangingDirEntry, e2: ChangingDirEntry) -> DirEntryChange {
  e1 {
    None -> e2 {
      None -> error
      File(f, executable) -> FileCreated(f, executable)
      Directory(d) -> DirectoryCreated(d)
    }
    File(f1, executable1) -> {
      None -> FileRemoved
      File(f2, executable2) -> {
        if f1 == f2 {
          if executable1 == executable2 {
            Unchanged
          }
          else {
            FileChanged(f1, None, executable2)
          }
        }
        else {
          if executable1 == executable2 {
            FileChanged(f1, f2, None)
          }
          else {
            FileChanged(f1, f2, executable2)
          }
        }
      }
      Directory(d) -> FileChangedToDirectory(d)
    }
    Directory(d1) -> {
      None -> DirectoryRemoved
      File(f, executable) -> DirectoryChangedToFile(f, executable)
      Directory(d2) -> {
        if d1 == d2 {
          Unchanged
        }
        else {
          DirectoryChanged(d1, d2)
        }
      }
    }
  }
}
```

## DirChangeMerge

As the merge proceeds through a directory, for each entry it will generate base_to_dir1 and base_to_dir2 DirEntryChanges.  A DirChangeMerge then describes what should happen to that entry based on those two changes.  The possible results are:

```
enum DirChangeMerge {
  TakeEither // take either 1 or 2, doesn't matter
  Take1 // take entry 1
  Take2 // take entry 2
  Remove // remove entry
  MergeDirectories(Option<base>, d1, d2) // recursively merge directories
  MergeFiles(Option<base>, f1, f2, executable) // attempt to merge files
  TakeFile(f, executable) // take a file changed from one and executable changed from the other
  Conflict // conflict that can't be auto-reconciled
}

ConflictContext {
  base: ChangingDirEntry
  entry_1: ChangingDirEntry
  entry_2: ChangingDirEntry
  change_1: DirEntryChange
  change_2: DirEntryChange
}
```

The algorithm for determining the merge:

```
changes_to_merge(c1: DirEntryChange, c2: DirEntryChange) -> DirChangeMerge {
  // If both changed the same way, take either
  if c1 == c2 {
    TakeEither
  }

  // If only one is unchanged, take the other (both unchanged is handled above)
  if c1 == Unchanged {
    Take2
  }
  if c2 == Unchanged {
    Take1
  }

  // If both removed, remove
  if c1.is_removed() && c2.is_removed {
    Remove
  }
  
  // If both directories changed, but not in the same way, merge them
  if c1 == DirectoryChanged(b1, d1) && c2 == DirectoryChanged(b2, d2) {
    MergeDirectories(b1, d1, d2)
  }

  // If both created a new directory, treat as a merge with no base
  if c1.new_directory()->d1 && c2.new_directory->d2 {
    MergeDirectories(None, d1, d2)
  }

  // If both files changed, merge them
  if c1 == FileChanged(b1, f1, e1) && c2 == FileChanged(b2, f2, e2) {
    executable = e1 || e2 || base executable
    if f1 {
      if f2 {
        MergeFiles(b1, f1, f2, executable)
      }
      else {
        TakeFile(f1, executable)
      }
    }
    else {
      if f2 {
        TakeFile(f2, executable)
      }
      else {
        TakeFile(base, executable)
      }
    }
  }

  // If both created a new file, treat as a merge with no base
  if c1.new_file()->d1,e1 && c2.new_file->d2,e2 {
    if e1 == e2 {
      MergeFiles(None, f1, f2, e1)
    }
    else {
      Conflict
    }
  }
  
  // All other cases are treated as conflicts
  Conflict
}
```

## merge_directories

With the above defined, the merge_directories function looks something like this:

```
async merge_directories(repo: Repo, base: <Option directory ObjectId>, dir_1: ObjectId, dir_2: ObjectId) -> WithComplete<new Directory ObjectId> {
  start a directory_builder
  create a completes
  "zipper" through the ChangingDirEntry entries of base, dir_1, and dir_2, treating base as an empty list if None - for each (name, base_entry, entry_1, entry_2) {
    change_1 = to_dir_change(base_entry, entry_1)
    change_2 = to_dir_change(base_entry, entry_2)
    merge_action = changes_to_merge(change_1, change2)
    merge_action {
      TakeEither => add entry_1 to directory_builder
      Take1 => add entry_1 to directory_builder
      Take2 => add entry_2 to directory_builder
      TakeFile => add file and executable to directory_builder
      Remove => skip this entry
      MergeDirectories(b, d1, d2) => recursively call merge_directories(repo, b, d1, d2), add to directory_builder and completes
      MergeFiles(b, f1, f2, e) => call merge_files, add to directory_builder and completes
      Conflict => call conflict
    }
  }
  finish the directory_builder, add to completes
  return directory_builder ObjectId and completes
}

async merge_files(repo: Repo, name: string, base: Option<file ObjectId> , file_1: ObjectId, file_2: ObjectId, executable: bool, conflict: ConflictContext) -> DirectoryEntry {
  // FIXME - to be defined later
}

async conflict(repo: Repo, name: string, conflict: ConflictContext) -> DirectoryEntry {
  // FIXME - to be defined later
}
```
