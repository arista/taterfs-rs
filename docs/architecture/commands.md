# Commands

Commands are top-level tfs operations, typically invoked by the CLI.


# list

```
ListCommandArgs {
    path: Path
    format: ListFormat
    classify_format: bool
}

enum ListFormat {
  Long(ListLongFormat)
  Short(ListShortFormat)
  Json(ListJsonFormat)
}

ListLongFormat {
  include_id: bool
}

ListShortFormat {
  width: Option<number>
  columns_spec: Option<ListColumnsSpec>
}

ListJsonFormat {
  include_id: bool
}

ListColumnsSpec {
  Single
  MultiVerticalSort
  MultiHorizontalSort
}

ListEntryJson {
  File(ListEntryFileJson)
  Directory(ListEntryDirectoryJson)
}

ListEntryFileJson {
  type: "File"
  size: number
  executable?: boolean
  name: string
  id?: string 
}

ListEntryDirectoryJson {
  type: "Directory"
  name: string
  id?: string 
}

list(args: ListCommandArgs, context: CommandContext)
```
CommandContext requires: repository spec, commit

List the repo file or contents of the repo directory resolved by the path in the given repo under the given commit.  Error if path does not exist.

The list of entries to be listed is a single entry (if a file), or the list of entries from the directory, in the order that the directory has stored them (which should be alphabetical)

For long and short format, directory names are printed in blue, executable file names in green, non-executable file names in green.  In addition, if classify_format is true, then names are appended (in white) with "/" for directories, or "*" for executable files.  Names are escaped using the "shell-escape" behavior used by modern "ls" implementations.

* Long format

Entries are listed one per line:

{format char} [{id}] {size} {name}

Each column is sized to its widest value (this really only applies to size).  The id column is only included if include_id is specified.  The size entry is blank for directories.  The format char is "d" for directories, "x" for executable files, "-" for non-executable files.

* Short format

Entry names are printed, possibly in multiple-columns.  The columns_spec determines if the entries should be forced to a single column, or if multiple columns should be allowed, with entries running in either vertical or horizontal order.  If multiple columns are allowed, the number of columns is determined by finding the largest-sized name entry, and dividing the available width by that amount (never going less than 1 column), allowing for two spaces between columns.

* Json format

Entries are printed using jsonl (one JSON object per line), each formatted as a ListDirectoryEntryJson

# upload_directory

```
UploadDirectoryCommandArgs {
    filestore_path: Path
    repo_path: Path
}

upload_directory(args: UploadDirectoryCommandArgs, context: CommandContext)
```
CommandContext requires: repository spec, filestore spec, branch, commit metadata

Modify the repository so that the given directory from the filestore has been uploaded to the given repo_path in the repository.  The algorithm is as follows:

* use [upload_directory](./upload.md) to obtain a new directory_id and complete
* create a RepoModel, call update with a function that does the following:
  * get the current commit of the branch, and its current directory_id
  * use [mod_dir_tree](./dir_tree_mod.md) to obtain a new root directory id, with the newly-uploaded directory, and get its complete
  * use [create_next_commit](./repo_model.md) to obtain a new Commit and its complete
  * use [create_next_branches](./repo_model.md) to obtain a new Branches and its complete
  * use [create_next_root](./repo_model.md) to obtain a new Root and its complete
  * combine all the completes
  * return the new root and the combined Completes
