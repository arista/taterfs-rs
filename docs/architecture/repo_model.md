# RepoModel

A RepoModel is an object model around a [Repo](./repository_interface.md) that presents a higher-level API for interacting with that Repo.

## RepoModel interface

```
RepoModel {
  new(repo) -> RepoModel
  async default_branch() -> BranchModel
  async get_branch(name: string) -> Option<BranchModel>
}
```

## BranchModel interface

```
BranchModel {
  id: ObjectId
  async commit() -> CommitModel
}
```

## CommitModel interface

```
CommitModel {
  id: ObjectId
  async root() -> RootModel
}
```

## RootModel interface

```
RootModel {
  // Returns the root directory
  directory() -> DirectoryModel

  // Resolves the given path starting from the root, using "/" as a path separator.
  async resolve_path(path) -> ResolvePathResult

  // Resolves the given path, with an error if the result is not a DirectoryEntry or Root.  For the non-error cases, the resulting Directory is returned
  async resolve_path_to_directory(path) -> DirectoryModel
}
```

## DirectoryModel interface

```
DirectoryModel {
  id: ObjectId
  async entries() -> EntryModelList
  // Performs a binary search for the entry with the given name
  async find_entry(name: string) -> Option<EntryModel>
}

EntryModelList {
  async next() -> Option<EntryModel>
}

enum EntryModel {
  File(FileEntryModel)
  Directory(DirectoryEntryModel)
}

impl EntryModel {
  name() -> string
}

FileEntryModel {
  name: string
  size: number
  executable: bool
  file: FileModel
}

DirectoryEntryModel {
  name: string
  directory: DirectoryModel
}

enum ResolvePathResult {
  None
  Root
  Directory(DirectoryEntryModel)
  File(FileEntryModel)
}
```

## FileModel interface

```
FileModel {
  id: ObjectId
}
```
