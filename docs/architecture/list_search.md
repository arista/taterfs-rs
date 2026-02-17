# List Search

Similar to concepts in [List Builder](./list_builder.md) and [List Modifier](./list_modifier.md), List Search searches a list (Branches, File, Directory) for an entry with a specified key.  It uses a binary search, and descends recursively into the "tree" structure used to represent lists.

The List Search is implemented with a single function and algorithm, and uses adpaters to apply that algorithm to different kinds of lists (Branches, File or Directory).

The interfaces look like this.  K is the key type, which must be comparable, E is the "leaf" entry type (Branch, DirectoryEntry, ChunkFilePart), T is the "tree" entry type (BranchesEntry, DirectoryEntry, FileFilePart).

```
SearchableList<K, E, T> {
  length() -> number
  // Error if ix is out of range
  get(ix: number) -> SearchableListEntry<K, E, T>
}

enum SearchableListEntry<K, E, T> {
  Leaf(SearchableListLeaf)
  Tree(SearchableListTree)
}

impl SearchableListEntry<K, E, T> {
  // Compare the key against the entry.  If a leaf, compare against the leaf's key.  If a tree, compare against the entry's key range (i.e., < if less than lower bound, > if greater than upper bound, = if between lower and upper bounds, inclusive)
  compare(key:K)
}

SearchableListLeaf<K, E, T> {
  key() -> K
  entry() -> E
}

SearchableListTree<K, E, T> {
  async list() -> SearchableList<K, E, T>
}
```

The algorithm can be divided into two functions:

```
search_list_entries(list: SearchableList, key: K) -> Option<SearchableListEntry> {
  perform a binary search on list, using the compare method on each entry
  if an entry matches, then return it, otherwise return None
}

seach_list(list: SearchableList, key: K) -> Option<E> {
  call search_list_entries
  if None, return None
  otherwise, if Leaf, then return the leaf.entry()
  otherwise, recursively call search_list_entries on leaf.list()
}
```

Each of the list types then has its own adapters to the traits specified above:

## Branches

* SearchableList wraps a Branches and returns BranchListEntry items from the branches property wrapped in SearchableListEntry
* SearchableListEntry wraps a BranchListEntry, compare() compares against Branch.name or BranchesEntry.firstName/lastName
* SearchableListLeaf wraps a Branch
* SearchableListTree wraps a BranchesEntry

## Directory

* SearchableList wraps a Directory and returns DirectoryPart items from the entries property wrapped in SearchableListEntry
* SearchableListEntry wraps a DirectoryPart, compare() compares against DirectoryEntry.name or PartialDirectory.firstName/lastName
* SearchableListLeaf wraps a DirectoryEntry
* SearchableListTree wraps a PartialDirectory

## File

* SearchableList wraps a File and returns FilePart items from the parts property wrapped in SearchableListEntry.  The SearchableList will have to compute the start and end positions of each FilePart, storing that in the SearchableListEntry.  That should be done up front when the SearchableList is constructed.
* SearchableListEntry wraps a FilePart, compare() compares against the start and end positions described above
* SearchableListLeaf wraps a ChunkFilePart
* SearchableListTree wraps a FileFilePart
