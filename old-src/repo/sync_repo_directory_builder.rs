use crate::repo::repo_backend::RepoBackend;
use crate::repo::repo_directory_builder::RepoDirectoryBuilder;
use crate::repo::repo_model::{
    Directory, DirectoryEntry, DirectoryPart, MAX_DIRECTORY_ENTRIES, ObjectId, PartialDirectory,
    RepoObject, bytes_hash, to_canonical_json_bytes,
};
use async_trait::async_trait;
use std::sync::Arc;

#[derive(Clone)]
pub struct Context {
    pub backend: Arc<dyn RepoBackend + Send + Sync>,
}

#[derive(Default)]
pub struct PartsStackElem {
    parts: Vec<DirectoryPart>,
}

impl PartsStackElem {
    // Return the current list of parts, replacing it with a new empty list
    fn replace_parts(&mut self) -> Vec<DirectoryPart> {
        std::mem::take(&mut self.parts)
    }
}

pub struct SyncRepoDirectoryBuilder {
    cx: Context,

    // [0] of the stack represents the current "bottom" of the stack where new parts will go.  The last element of the stack represents the root
    stack: Vec<PartsStackElem>,
}

impl SyncRepoDirectoryBuilder {
    pub fn new(cx: Context) -> Self {
        Self {
            cx,
            stack: Vec::new(),
        }
    }

    // Test if the stack[ix] has enough space to hold a new part
    pub fn has_space(&self, ix: usize) -> bool {
        self.stack[ix].parts.len() < MAX_DIRECTORY_ENTRIES
    }

    // Add a new part to stack[0].  If stack[0] doesn't have enough space to hold it, then collapse that stack element into a new DirectoryPart, and add it to the next higher stack element, making sure that higher stack element has enough space, and so on.  If we run out of stack elements, then add a new item to the stack to act as the new root.
    pub async fn add_part_at(&mut self, part: DirectoryPart, ix: usize) -> anyhow::Result<()> {
        // The stack item we're trying to add to, and the part we want to add to that item
        let mut ix_to_add = ix;
        let mut part_for_ix = part;
        loop {
            // If we've gone all the way up the stack still looking for a entry with available space, then we need to add a new empty element onto the stack
            if ix_to_add >= self.stack.len() {
                self.stack.push(PartsStackElem::default());
            }

            // If this item has space for the item, then we can add it and be done
            if self.has_space(ix_to_add) {
                self.force_add_part_at(part_for_ix, ix_to_add);
                break;
            }
            // No space in this item - we'll have to make room, possibly going up the stack
            else {
                let new_part = self.turn_stack_elem_into_part(ix_to_add).await?;
                self.force_add_part_at(part_for_ix, ix_to_add);

                // Set up to add the new part to the parent stack item
                part_for_ix = new_part;
                ix_to_add += 1;
            }
        }
        Ok(())
    }

    pub fn force_add_part_at(&mut self, part: DirectoryPart, ix: usize) {
        self.stack[ix].parts.push(part);
    }

    // Get the list of parts from the given stack item, write it as a Directory object, return the Directory wrapped in a PartialDirectory, and clear out the stack item so it can receive a new part
    pub async fn turn_stack_elem_into_part(&mut self, ix: usize) -> anyhow::Result<DirectoryPart> {
        let parts = self.stack[ix].replace_parts();
        let first_name = parts.first().unwrap().first_name().clone();
        let last_name = parts.last().unwrap().last_name().clone();
        let directory = Directory { entries: parts };
        let directory_obj = RepoObject::Directory(directory);
        let directory_bytes = to_canonical_json_bytes(&directory_obj);
        let directory_id = bytes_hash(&directory_bytes);
        self.cx
            .backend
            .write(&directory_id, directory_bytes)
            .await?;
        Ok(DirectoryPart::Partial(PartialDirectory {
            first_name,
            last_name,
            directory: directory_id,
        }))
    }
}

#[async_trait(?Send)]
impl RepoDirectoryBuilder for SyncRepoDirectoryBuilder {
    async fn add_entry(&mut self, entry: DirectoryEntry) -> anyhow::Result<()> {
        let directory_part = DirectoryPart::from(entry);
        self.add_part_at(directory_part, 0).await?;

        Ok(())
    }

    async fn complete(&mut self) -> anyhow::Result<ObjectId> {
        // Go through and add each element of the stack to the item above
        let mut ix = 0;
        while ix + 1 < self.stack.len() {
            let part = self.turn_stack_elem_into_part(ix).await?;
            self.add_part_at(part, ix + 1).await?;
            ix += 1;
        }

        // Take the final element and turn it into a File
        let parts = if self.stack.is_empty() {
            Vec::new()
        } else {
            self.stack[ix].replace_parts()
        };
        let directory = Directory { entries: parts };
        let directory_obj = RepoObject::Directory(directory);
        let directory_bytes = to_canonical_json_bytes(&directory_obj);
        let directory_id = bytes_hash(&directory_bytes);
        self.cx
            .backend
            .write(&directory_id, directory_bytes)
            .await?;
        Ok(directory_id)
    }
}
