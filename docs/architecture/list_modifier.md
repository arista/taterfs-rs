# List Modifier

Similar to a [List Builder](./list_builder.md), a List Modifier applies a set of modifications to a list (Branches, File, Directory), writing the modified list to the repo, and returning the id of the result.  As with the List Builder, the List Modifier uses a common set of code, with generics used to apply that code to the various list types.

The interfaces involved look like this:

```
async modify_list(list_elems: ListElems, modifications: ListModifications, builder: ListBuilder) -> WithComplete<ObjectId>

ListElems<K, E> {
  async next() -> Option<ListElem<K, E>>
}

ListElem<K, E> {
  key: K
  entry: E
}

ListModifications<K, E> {
  next() -> Option<ListModification>
  add(key: K, entry: Option<E>)
}

ListModification<K, E> {
  key: K
  entry: Option<E>
}
```

The general idea is that the ListModifications is expected to hold a vector of ListModification objects that were added by calling add(), and are kept in sorted order sorting by key (which must be a sortable type).  Its next() method yields each modification in order.  If a modification's entry is None, that means the entry is to be removed.  Otherwise, the entry is to be added, or should replace an existing entry with the same key.  It is an error to call add() after next() has been called.

The modify_list function then "zippers" the list_elems (which should also be sorted by key) with the modifications, calling the given ListBuilder with each resulting entry.

As with list builder, there will be different implementations of ListElems for each of the list types (defined in repo_objects.rs)

* Branches

The entries are of type Branch, and the next() method should return those entries corresponding to BranchList, as returned by repo.list_branches.  The key type is String, corresponding to Branch.name

The key type is String, corresponding to Branch.name

* Directory

The entries are of type DirectoryEntry, and the next() method should return those entries corresponding to DirectoryEntryList, as returned by repo.list_directory_entries.  The key type is String, corresponding to DirectoryEntry.name (

* File

The entries are of type ChunkFilePart, and the next() method should return those entries corresponding to FileChunkList, as returned by repo.list_file_chunks.  The key type is a u64, corresponding to the position of the chunk.  The position is not contained in the ChunkFilePart, so the ListElems implementation must keep a running total of that position, adding each chunk's size  for the next chunk.

There should also be separate types for of ListModifications corresponding to the above list types.  And the ListBuilder should be a ListBuilder with the appropriate ListBuilderConfig type.


There should then be modify_branches, modify_directory, and modify_file specialized methods that call the generic modify_list.

