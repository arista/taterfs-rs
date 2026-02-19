# Commands

Commands are top-level tfs operations, typically invoked by the CLI.


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
