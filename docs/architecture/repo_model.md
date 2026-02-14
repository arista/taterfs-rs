# RepoModel

A RepoModel is an object model around a [Repo](./repository_interface.md) that presents a higher-level API for interacting with that Repo.

## RepoModel interface

```
RepoModel {
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
  async root() -> DirectoryModel
}
```

## DirectoryModel interface

```
DirectoryModel {
  id: ObjectId
  async entries() -> EntryModelList
}

EntryModelList {
  async next() -> Option<EntryModel>
}

enum EntryModel {
  File(FileEntryModel)
  Directory(DirectoryEntryModel)
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
```

## FileModel interface

```
FileModel {
  id: ObjectId
}
```
