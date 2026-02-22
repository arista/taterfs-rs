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
  merge_file_result: Option<MergeFileResult>
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
        if successful {
          TakeFile of the merged file
        }
        else {
          Conflict (with the MergeFileResult)
        }
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
```

## merge_files

The above algorithm calls out a case when a file has been modified in both dir1 and dir2, indicating that an attempt should be made to merge the two files.  The file merge algorithm will make use of the [content_inspector](https://crates.io/crates/content-inspector) crate to determine if a file is a "binary" file or not.  If the files to be merged are both text files, then the [merge3](https://crates.io/crates/merge3) crate will be used to perform the actual merge.

The merge3 crate expects to hold all the files in memory (although the output can be streamed).  The files can be pulled into memory using Repo.read_file_chunks_with_content and organized into whatever form merge3 requires.  All 3 files (base, dir1, dir2) should be downloaded simultaneously.

The output of merge3 is streamed using an iterator, and written to the repo using a [StreamingFileUploader](./upload.md).

Because the files need to be loaded into memory, there is a limit to the size of files that will attempt merging.  The maximum size is specified in configuration [memory.max_merge_memory](./configuration.md).  If the base, file1, and file2 together exceed that size, then the merge will fail.

When the files are download into memory, the merge should do a quick check to make sure none of the files are binary files.  This means that when calling read_file_chunks_with_content, the merge should start by loading just the first chunk from all 3 (simultaneously), check for binary, and if all are clear, then continue with downloading.

The outcome of the merge can be one of the following:

```
enum MergeFileResult {
  Merged(file_id) - successful merge
  MergeConflict(file_id) - merged, but with conflict markers in the result
  Binary - either base, file1, or file2 was determined to be a binary file
  TooLarge - base, file1, and file2 together are too large to fit in memory
}
```

And the file merge looks like this:

```
async merge_files(repo: Repo, managed_buffers, base: Option<FileEntry>, fe1: FileEntry, fe2: FileEntry) -> WithComplete<MergeFileResult> {
  check the combined sizes of base, fe1, fe2, return TooLarge if too large
  download one chunk all 3 files (base, fe1, fe2) simultaneously
  if any chunk is binary, return Binary
  continue downloading the remainders of the files into memory (into whatever form merge3 requires, probably split into lines)
  create a StreamingFileUploader
  call merge3
  iterate through the merge3 results, adding them to StreamingFileUploader
  if there are any conflict markers (or however merge3 signals that), return MergeConflict with the uplaoder's complete
  otherwise return Merged with the uploader's complete
}
```

## conflict representation

If a directory entry with a given name (i.e., {directory}/{name}) cannot be resolved without a conflict, this means that two different values are vying for that name.  Traditionally those values are called "theirs" and "ours", with some heuristics for determining which should be called which.  There is also a "base" value, from which both values are derived.  Finally, if both values are text files and a merge was attempted between them, then there is a "merged" value, which should contain conflict markers.

The values need not be files.  For example, if one change added a file while another added a directory, then "theirs" might be the file and "ours" might be the directory.  Or if one changed a file while another deleted it, then "ours" might hold the changed file, and "theirs" might not exist at all.

Given all this, a conflict is represented like this:

* {directory}/{name} becomes a directory, regardless of what its original value was
* {directory}/{name} contains these entries (if they exist):
    * base
    * theirs
    * ours
    * merged
* {directory}/{name} also contains CONFLICT.txt, that explains why there's a conflict and possible resolutions
* {directory}/{name} also contains a script called "take", which takes argument "base|theirs|ours|merged", and replaces the entire conflict directory with the specified entry (if it exists).

This is what the "conflict()" function does, which is called when a merge results in a conflict:

```
async conflict(repo: Repo, name: string, conflict: ConflictContext) -> WithComplete<DirectoryEntry> {
  generate the conflict directory structure above, treating entry 1 as "theirs" and entry 2 as "ours"
  use the StreamingFileUploader to generate the "CONFLICT.txt" and "take" files, using a Completes to gather their complete flags
  use a DirectoryBuilder to create the actual directory, adding its complete flag to the Completes
  return the resulting DirectoryEntry, with complete flags
}
```

The CONFLICT.txt file should contain the following:

* An indication that this directory represents a conflict that occurred when trying to merge two commits derived from a base commit
* List the three commits involved, using the "theirs" and "ours" terminology
* Summarize the change from base to theirs, and the change from base to ours
* If a merge was attempted into "merged", indicate that as well
* Explain that the conflict should be resolved by deleting the directory and replacing it with the intended contents after being manually resolved.  Explain that calling the "take" script can make this easier

The "take" script should turn into a call to "tfs take {conflict directory} {arguments passed to the script}".  The take script doesn't need to do argument checking - that can be implemented by "tfs take".

FIXME - how do we know if the "tfs" executable is in the path, or if that's even what it's called

## conflict discussion

Every merge results in a new commit that's intended to be added to a particular branch.  That merge might contain conflicts, as represented above.  If so, what should happen?

* Discard the commit, and alert the user to the conflicts
* Add the commit to the intended branch, and alert the user that there are conflicts on the branch that should be resolved
* Place the commit on a new branch, alerting the user

Most version control systems effectively do the first, where they at least put up a good fight before allowing a conflict into the repo.

This system is a little different though, in that it's targeted to individual users trying to archive content shared across multiple machines.  The emphasis is on getting the content safely into the system, and less on putting roadblocks in the way of getting that content in, even if it isn't quite right.

So that would point to getting the conflict into the repo.  The simplest thing to do is to just put it on the branch, and warn the user very loudly that there are conflicts that should be resolved.  But that runs the risk of allowing work to continue, building on top of conflicts (which even raises the possibility of conflict directories that themselves contain conflicts, leading to nested conflict directories).

The "safer" choice is to put the commit on a separate branch and loudly alerting the user that those conflicts and that branch exist.  But this runs the risk of the user just missing that messaging and not realizing that changes aren't on the expected branch.  It also means that there should be clear tools and messaging for addressing the conflicts.
