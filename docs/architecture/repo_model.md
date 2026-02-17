# RepoModel

A RepoModel is an object model around a [Repo](./repository_interface.md) that presents a higher-level API for interacting with that Repo.

In most cases, the model object is a thin wrapper around a corresponding [backend storage object](./backend_storage_model.md).  The model object might be constructed with only an ObjectId (or branch name for a BranchModel), and a place to hold that backend storage object, fetched lazily when needed.

## RepoModel interface

```
RepoModel {
  new(repo) -> RepoModel
  async default_branch() -> BranchModel
  // performs a binary search for the given branch
  async get_branch(name: string) -> Option<BranchModel>
}
```
Lazily fetches and caches the current "Root" object, using it as the source of its info.

The functions that return a BranchModel will find the Branch in the branches list and construct with BranchModel with that Branch.

## BranchModel interface

```
BranchModel {
  branch_name: string
  branch: Branch
  async commit() -> CommitModel
}
```
## CommitModel interface

```
CommitModel {
  id: ObjectId
  async root() -> DirectoryRootModel
}
```
Constructed with the commit's id.  Lazily fetches and caches the backend "Commit" object, using it as the source of its info

## RootModel interface

```
DirectoryRootModel {
  // Returns the root directory
  directory() -> DirectoryModel

  // Resolves the given path starting from the root, using "/" as a path separator.
  async resolve_path(path) -> ResolvePathResult

  // Resolves the given path, with an error if the result is not a DirectoryEntry or Root.  For the non-error cases, the resulting Directory is returned
  async resolve_path_to_directory(path) -> DirectoryModel
}
```
Constructed with the id of the commit's directory.  Lazily fetches and caches the backend "Commit" object, using it as the source of its info

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

DirectoryModel is constructed with the directory's id.  Lazily fetches and caches the backend "Directory" object as needed, using it as the source of its info.

## FileModel interface

```
FileModel {
  id: ObjectId
}
```

FileModel is constructed with the directory's id.  Lazily fetches and caches the backend "Directory" object as needed, using it as the source of its info.

