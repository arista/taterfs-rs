# sync

"Sync" is expeccted to be the primary usage of the system.  A sync operation causes filestores and repos to end up with the same content, [merging](./merge.md) the changes from both against a common base commit.  A sync operation effectively uploads the contents of a filestore, merges against the repo's current commit, then downloads the result back to the filestore (along with any [conflicts](./merge.md)).

A sync operation depends on the [StoreSyncState](./file_stores.md) mechanism exposed by a FileStore.  This is what allows a sync operation to know which branch and base commit to use when performing the merge portion of the sync.

## adding a sync

Before a file store can by sync'ed to a repo, an "add sync" operation must be performed on the file store, connecting the file store to a particular repo, branch, and repo directory.  Adding a file store as a sync will create a StoreSyncState for the FileStore, and also set the FileStore and repo to have the same content, according to these possible scenarios:

* The file store already has an associated StoreSyncState (either current or pending) - error
* Both the file store and the repo directory are empty (or nonexistent) - create the empty repo directory, with commit message "Add sync to /{directory}"
* The file store is empty, but the repo directory contains content - [download](./download.md) the repo's content to the file store
* The repo directory is empty, but the file store contains content - [upload](./upload.md) the file store to the repo directory with commit message "Add sync to /{directory}"
* Both the file store and the repo directory contain content - error, tell the user to clear or move aside any content already in the file store

Use [RepoModel](./repo_model.md) to modify the repo or traverse repo directory paths.

In non-error cases, adding a sync should create a pending StoreSyncState, then when all operations are complete (uploads and downloads), it should commit that StoreSyncState.

```
async add_sync(file_store, repo_model, branch, repo_directory: Path)
```

## running a sync

Running a sync involves these steps:

* Check if there is a pending StoreSyncState - this is a difficult situation, indicating that a previous download was interrupted, leaving things in an indeterminate state.  See discussion later.
* Get the current SyncState
* Retrieve the branch and base commit from the SyncState
* Upload the file store's contents to the repo directory.  If its content is different from the base commit, then create a new commit with the new content, with commit message "Sync upload"
* Use merge_commit to create a merge commit.  The base is the base commit specified by the SyncState, commit 1 is the commit currently on the branch, and commit 2 is uploaded commit.
* Update the repo to have the branch point to the commit
* Create a pending StoreSyncState, with the base commit set to the new commit
* Download the new branch's head commit to the filestore
* Commit the StoreSyncState

That's the basic process for sync'ing a single filestore.  Where things get more interesting is sync'ing multiple filestores simultaneously, which is a likely use case for a local disk that is sync'ed to multiple repos, or multiple directories within a single repo, or a combination of the two.  In such a case, the system should try to update each repo once, rather than once for every sync.  For example, if sync 1 and sync 2 are simultaneously syncing to the repo at directory /dir1 and /dir2 on the same branch, then both uploads should happen in tandem, and a single new merge commit shoulld be created.

To do this, when a sync is initiated with multiple file stores, the syncs should be grouped first by repo spec.  Each "repo group" then proceeds simultaneously with the other "repo groups".

Each "repo group" is then broken down by branch, into "repo+branch groups", each proceeding simultaneously with the other "repo+branch groups".  Each "repo+branch" group is then grouped by common base commit, into separate "repo+branch+base groups", sorted by base id.

Each "repo+branch+base group" proceeds by uploading the contents of its file stores.  It then uses [mod_dir_tree](./dir_tree_mod.md) to gather all of those uploaded file stores into a single root directory tree, with each uploaded filestore positioned to the repo directory specified by its sync.  It then uses merge_commits to create a new commit as described above.

There's a bit of a catch here - there's no guarantee that the filestores will have non-overlapping directories, as is required by [DirTreeModSpec](./dir_tree_mod.md).  If there are overlapping directories, then multiple merges and commits will be required.

To handle this, the algorithm will sort the repo directories in each "repo+branch+base group" - first by number of path components, descending (i.e., longest paths first), then alphabetically.  The idea is to get an order that tries to group as many nonoverlapping paths as it can, in a deterministic way.  Then, when it uses mod_dir_tree, it goes through that list in order.  If any items fail DirTreeModSpec.can_add, then they will not be added, and will instead be placed on a retry list.

The merge_commits call then proceeds.  Once it completes, it checks to see if there are any items remaining on the retry list.  If so, then it goes back and repeats the process of using mod_dir_tree and merge_commits, this time using the commit that was created in the prevoius merge as commit 1.  Once the retry list is exhausted, we have the final commit needed.

If a "repo+branch group" has multiple "repo+branch+base groups", then this process repeats for each "repo+branch+base group", but with each previous operation using the previous commit as commit 1.

In other words, it proceeds as if the overlapping groups and the different repo+branch+base groups happened in succession against the same branch.

Once all the "repo+branch groups" are complete for a single "repo group", that repo can be updated.  The repo is updated to set all the branches to their new commits simultaneously.  The downloads can then proceed for the syncs associated with that repo.

The overall sync operation completes when all "repo groups" are complete.

```
async run_syncs(file_stores, app) -> WithComplete<()>
```

The app is required for its create_repo method.  FIXME - does it make sense to abstract create_repo out into its own trait and pass that in, rather than the whole App?

## interrupted sync download

If a sync is attempted for a filestore that has a pending StoreSyncState, this means that a previous sync operation was interrupted during the download phase, while the filestore was being actively modified.  This leaves things in a difficult situation.  It is possible that the FileStore has been modified since then, so simply continuing with the previously-aborted download could overwrite changes.  Similarly, it's not possible to tell for certain what portions of the download already completed.  In other words, there's nothing that the system can safely do to resolve the situattion.  It can only alert the user, and possibly have a "force_pending_downloads" option, which would complete the previous download before proceeding with the next sync.

Because of this, it is suggested that the download process use the FileStore's stage, if available, to download as much content as possible to the stage BEFORE setting the pending StoreSyncState.  Once the content is available on the stage and ready to assemble into the FileStore, the pending StoreSyncState can be created.  This cuts down on the time when a pending StoreSyncState exists, thereby reducing the chance of an interruption leaving the system in an ambiguous state.

