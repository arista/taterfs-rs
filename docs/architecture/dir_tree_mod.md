# DirTreeMod

DirTreeMod allows the application to specify and execute multiple modifications to a directory hierarchy.  For example, an application can upload multiple directories, then insert those directories into various locations in the directory hieararchy in a single operation.

This is mainly used when "sync"'ing multiple local directories to various locations within a repo.  This operation begins by uploading all of those local directories, then modifying the repo's directory tree to reference those newly-uploaded directories.  A simplistic approach would be to upload each directory, then modify the overall directory structure for each uploaded directory.  However, this leads to a wasteful set of intermediate directory objects that will never be referenced again.  Instead, using DirTreeMod, all of those directory structures can be modified once.

## DirTreeModSpec

DirTreeMod is driven by a DirTreeModSpec.  This acts as a "virtual directory hierarchy" built up by the application, allowing new directory entries to be placed anywhere within the hierarchy.  Its interface looks something like this:

```
impl DirTreeModSpec {
  add(path, entry: DirectoryEntry, complete: Complete)
  remove(path)
}
```
(where DirectoryEntry is the same as what's specified in the [backend storage model](./backend_storage_model.md)

The add method indicates that the specified path should be set to the given entry, creating it if necessary or replacing what's already there.  The remove method indicates that the specified path should be removed.

The path specified for both add and remove may not be the root - it must have at least one named component.

When the DirTreeModSpec is applied to a repo directory, any entries not mentioned in the spec are left as-is.

Internally, the DirTreeModSpec is represented as its own hierarchy:

```
DirTreeModSpec {
  entries: Map<name, DirTreeModSpecEntry>
}

enum DirTreeModSpecEntry {
  Remove
  Entry(DirectoryEntry, Complete)
  Directory(DirTreeModSpec)
}
```

The idea being that when a Path is specified, it is broken down into components and Directory entries are placed to form the hierarchy until the final name component is reached, at which point either a Remove or an Entry is placed.

It is an error to "overlap" modifications.  For example, if an entry has already been placed or removed at "/a/b/c", then it is an error to try to make another modification at that path, or any subpath (e.g., "/a/b/c/d")

The list of entries can then be extracted, sorted by name:

```
DirTreeModSpec {
  entries() -> DirTreeModSpecEntryList
}

DirTreeModSpecEntryList {
  next() -> Option<DirTreeModSpecEntryWithName>
}

DirTreeModSpecEntryWithName {
  name: string
  entry: DirTreeModSpecEntry
}
```

## DirTreeMod execution

DirTreeMod uses a "zippering" approach similar to [list_modifier](./list_modifier.md), and uses a [list_builder](./list_builder.md).  The function looks like this:

```
async mod_dir_tree(repo: Repo, directory: repo Directory, spec: DirTreeModSpec) -> WithComplete<new directory ObjectId> {
  create a Directory ListBuilder

  // Retrieve sorted lists of entries from both the repo Directory and the mod spec
  dir_entries = repo.list_entries_of_directory(directory)
  mod_spec_entries = spec.entries()

  "Zipper" both sorted lists of entries into Option<mod_spec_entry>, Option<dir_entry>.  For each entry pair {
    if mod_spec_entry {
      Remove -> skip the entry, ignoring any dir_entry
      Entry -> add the entry to the ListBuilder with its complete, ignoring any dir_entry
      Directory(subdir spec) -> {
        subdir = if dir_entry is also a Directory { retrieve that directory} else { create a blank new Directory }
        recursively call mod_dir_tree on subdir and subdir spec
        add the resulting directory and its complete to the ListBuilder
      }
    }
    else if dir_entry {
      // retain any entries not found in spec
      add the dir_entry to the ListBuilder
    }
  }

  call finish() on the ListBuilder
  return a WithComplete with the result
}
```
