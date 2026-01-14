# List Builder

There are several repository structures that conceptually represent lists of indeterminate length, but are split up into a "tree" of objects that spreads the list into more manageable units.  These structures include Branches, File, and Directory.

The process for building up these structures is essentially the same.  Items are supplied sequentially, and are added to an instance of the object.  When an object "fills up", it is written to the repository, and an entry for it is added to the next higher object in the hierarchy.  If the object is already at the top of the hierarchy, then a new object is created "above" it to act as the new root of the hierarchy.

Once an object has been filled up and written to the repository, a new empty object is created in its place at the same level in the hierarchy.  This effectively keeps the tree balanced at the same depth throughout, with the "leaves" of the tree all at the same level of the hierarchy.

Meanwhile, as objects are being written, their Complete flags are being gathered at each level, such that an object's Complete flag effectively encompasses the object plus all the other objects it references.  As each Complete flag completes, the [cache's](./caches.md) "exists" and "fully_stored" flags are written for the object.

Finally, once all entries have been added, a final "cleanup" step will roll up any remaining objects in the hierarchy up each level until only the root is left.  The Complete flags also roll up as before, until only the root's Complete flag is left.  And that becomes the Complete flag for the whole list building operation.

## Detailed algorithm

Assume:

OBJ - object type being built (Branches, Directory, File)
ITEM - entry type in the OBJ (BranchListEntry, DirectoryPart, FilePart)
LEAF - leaf item type being added (Branch, FileEntry|DirEntry, ChunkFilePart)
TREE - entry type representing a "rollup" of entries lower in the hierarchy (BranchesEntry, PartialDirectory, FileFilePart)
MAX - maximum number of items in any node

```
ListBuilder {
  add(entry: LEAF, complete: Complete)
  finish() -> ListResult
}

ListResult {
  list: OBJ
  complete: Complete
}
```

Internally, the ListBuilder uses a couple structures and methods
```
ListBuilderStackItem {
  items: Array<ITEM>
  completes: Completes
}

stack: Array<ListBuilderStackItem>

```

The stack is arranged such that the leaf is at the beginning of the array, and the root is at the end of the array.  The following functions describe how an item is added:

```
add(leaf: LEAF, complete: Complete)
```
* Wrap the leaf in the appropriate ITEM
* add_item_at_level(item, 0, complete)


```
add_item_at_level(item: ITEM, ix: usize, complete: Complete)
```

* if stack_item_full_at(ix)
    * ensure_parent_of(ix)
    * rollup_stack_item_at(ix)
    * clear_stack_level(ix)
* force_add_item_at_level(item, ix, complete)


```
stack_item_full_at(ix: usize) -> bool
```
stack[ix].items.length >= MAX


```
stack_item_empty_at(ix: usize) -> bool
```
stack[ix].items.length == 0


```
ensure_parent_of(ix: usize)
```
* if ix is the top of the stack
    * create a new ListBuilderStackItem with empty items and new Completes
    * push it onto the top of the stack


```
clear_stack_level(ix: usize)
```
* Clear out stack[ix].items
* set stack[ix].completes to a new Completes


```
force_add_item_at_level(item: ITEM, ix: usize, complete: Complete)
```
* add item to stack[ix].items
* add complete to stack[ix].completes


```
rollup_stack_item_at(ix: usize)
```

* (obj, hash, complete) = stack_item_to_object(ix)
* Form the appropriate TREE from the hash (BranchesEntry, PartialDirectory, FileFilePart) - may involve consulting the obj to find firstName and lastName, for example
* Wrap that TREE in the appropriate ITEM (BranchListEntry, DirectoryPart, FilePart)
* recursively call add_item_at_level(ITEM, ix + 1, complete)


```
stack_item_to_object(ix: usize) -> (OBJ, hash, complete)
```
* Form the stack[ix].items into the appropriate nested OBJ (Branches, Directory, File)
* Write the OBJ to the repo, get its hash and complete
* Set the complete so that it sets the cache's exists flag for the hash when it completes
* Add the complete to stack[ix].completes
* Set stack[ix].completes so that it sets the cache's fully-stored flag for the hash when it completes
* Return the object, hash and stack[ix].completes


```
finish() -> (hash, complete)
```

* loop ix from 0 to the item before the top of the stack, keeping in mind that the stack could grow during this process
  * if !stack_item_empty_at(ix)
    * rollup_stack_item_at(ix)
* (obj, hash, complete) = stack_item_to_object(top of stack)
* return (hash, complete)
