# Merge

Performing a 3-way directory merge is a key element in combining changes from two streams of commits originating from a common commit.  Such a merge involves 3 directories: base, dir1, and dir2, and proceeds by "zippering" the entries from the three sources in sorted order.

## DirChange

A DirChange is a change from an entry in the base to the corresponding entry in dir (dir1 or dir2).  The entry and change are represented as:

Note: `FileEntry` refers to the struct from repo_objects.rs (name, size, executable, file ObjectId). In these enums, `name` comes from the zipper iteration context.

```
enum ChangingDirEntry {
  None
  File(FileEntry)
  Directory(d: ObjectId)
}

enum DirEntryChange {
  Unchanged
  FileCreated(FileEntry)
  FileChanged(base: FileEntry, Option<FileEntry>, Option<executable>) - Options indicate content change and/or executable change
  FileChangedToDirectory(base: FileEntry, d: ObjectId)
  FileRemoved
  DirectoryCreated(d: ObjectId)
  DirectoryChanged(base: ObjectId, d: ObjectId)
  DirectoryChangedToFile(base: ObjectId, FileEntry)
  DirectoryRemoved
}

impl DirEntryChange {
  new_directory() -> Option<ObjectId> {
    FileChangedToDirectory(_, d) => d
    DirectoryCreated(d) => d
    _ => None
  }

  new_file() -> Option<FileEntry> {
    FileCreated(fe) -> fe
    DirectoryChangedToFile(_, fe) -> fe
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
      File(fe) -> FileCreated(fe)
      Directory(d) -> DirectoryCreated(d)
    }
    File(fe1) -> e2 {
      None -> FileRemoved
      File(fe2) -> {
        if fe1.file == fe2.file {
          if fe1.executable == fe2.executable {
            Unchanged
          }
          else {
            FileChanged(fe1, None, fe2.executable)
          }
        }
        else {
          if fe1.executable == fe2.executable {
            FileChanged(fe1, fe2, None)
          }
          else {
            FileChanged(fe1, fe2, fe2.executable)
          }
        }
      }
      Directory(d) -> FileChangedToDirectory(fe1, d)
    }
    Directory(d1) -> e2 {
      None -> DirectoryRemoved
      File(fe) -> DirectoryChangedToFile(d1, fe)
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
  MergeDirectories(Option<ObjectId>, d1: ObjectId, d2: ObjectId) // recursively merge directories
  MergeFiles(Option<FileEntry>, fe1: FileEntry, fe2: FileEntry, executable: bool) // attempt to merge files
  TakeFile(FileEntry) // take a file (possibly with merged executable from both sides)
  Conflict // conflict that can't be auto-reconciled
}

ConflictContext {
  name: String
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
  if c1 == DirectoryChanged(b, d1) && c2 == DirectoryChanged(_, d2) {
    MergeDirectories(b, d1, d2)
  }

  // If both created a new directory, treat as a merge with no base
  if c1.new_directory()->d1 && c2.new_directory()->d2 {
    MergeDirectories(None, d1, d2)
  }

  // If both files changed, merge them
  // c1 = FileChanged(base1, Option<fe1>, Option<e1>)
  // c2 = FileChanged(base2, Option<fe2>, Option<e2>)
  if c1 == FileChanged(base, fe1_opt, e1_opt) && c2 == FileChanged(_, fe2_opt, e2_opt) {
    executable = e1_opt || e2_opt || base.executable  // first available
    if fe1_opt {
      if fe2_opt {
        MergeFiles(base, fe1_opt.with_executable(executable), fe2_opt.with_executable(executable), executable)
      }
      else {
        TakeFile(fe1_opt.with_executable(executable))
      }
    }
    else {
      if fe2_opt {
        TakeFile(fe2_opt.with_executable(executable))
      }
      else {
        TakeFile(base.with_executable(executable))
      }
    }
  }

  // If both created a new file, treat as a merge with no base
  if c1.new_file()->fe1 && c2.new_file()->fe2 {
    if fe1.executable == fe2.executable {
      MergeFiles(None, fe1, fe2, fe1.executable)
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
      TakeFile(fe) => add fe (with name) to directory_builder
      Remove => skip this entry
      MergeDirectories(b, d1, d2) => recursively call merge_directories(repo, b, d1, d2), add to directory_builder and completes
      MergeFiles(b, fe1, fe2, e) => call merge_files(repo, name, b, fe1, fe2, e, conflict_context), add to directory_builder and completes
      Conflict => call conflict
    }
  }
  finish the directory_builder, add to completes
  return directory_builder ObjectId and completes
}

async merge_files(repo: Repo, name: String, base: Option<FileEntry>, fe1: FileEntry, fe2: FileEntry, executable: bool, conflict: ConflictContext) -> DirectoryEntry {
  // FIXME - to be defined later
}

async conflict(repo: Repo, name: string, conflict: ConflictContext) -> DirectoryEntry {
  // FIXME - to be defined later
}
```
