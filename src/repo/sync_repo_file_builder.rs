use async_trait::async_trait;
use std::sync::Arc;
use bytes::Bytes;
use crate::repo::repo_model::{RepoObject, File, FilePart, ChunkFilePart, FileFilePart, ObjectId, MAX_FILE_PARTS, to_canonical_json_bytes, bytes_hash};
use crate::repo::repo_file_builder::RepoFileBuilder;
use crate::repo::repo_backend::RepoBackend;

#[derive(Clone)]
pub struct Context {
    pub backend: Arc<dyn RepoBackend + Send + Sync>,
}

#[derive(Default)]
pub struct PartsStackElem {
    parts: Vec<FilePart>,
}

impl PartsStackElem {
    // Return the current list of parts, replacing it with a new empty list
    fn replace_parts(&mut self) -> Vec<FilePart> {
        std::mem::take(&mut self.parts)
    }
}

pub struct SyncRepoFileBuilder {
    cx: Context,

    // [0] of the stack represents the current "bottom" of the stack where new parts will go.  The last element of the stack represents the root
    stack: Vec<PartsStackElem>
}

impl SyncRepoFileBuilder {
    pub fn new(cx: Context) -> Self {
        Self {
            cx,
            stack: Vec::new(),
        }
    }

    // Test if the stack[ix] has enough space to hold a new part
    pub fn has_space(&self, ix: usize) -> bool {
        self.stack[ix].parts.len() < MAX_FILE_PARTS
    }

    // Add a new part to stack[0].  If stack[0] doesn't have enough space to hold it, then collapse that stack element into a new FilePart, and add it to the next higher stack element, making sure that higher stack element has enough space, and so on.  If we run out of stack elements, then add a new item to the stack to act as the new root.
    pub async fn add_part_at(&mut self, part: FilePart, ix: usize) -> anyhow::Result<()> {
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
                let new_file_part = self.turn_stack_elem_into_part(ix_to_add).await?;
                self.force_add_part_at(part_for_ix, ix_to_add);

                // Set up to add the new file part to the parent stack item
                part_for_ix = new_file_part;
                ix_to_add += 1;
            }
        }
        Ok(())
    }

    pub fn force_add_part_at(&mut self, part: FilePart, ix: usize) {
        self.stack[ix].parts.push(part);
    }

    // Get the list of parts from the given stack item, write it as a File object, return the File wrapped in a FilePart, and clear out the stack item so it can receive a new part
    pub async fn turn_stack_elem_into_part(&mut self, ix: usize) -> anyhow::Result<FilePart> {
        let parts = self.stack[ix].replace_parts();
        let file = File{parts};
        let file_size = file.total_len();
        let file_obj = RepoObject::File(file);
        let file_bytes = to_canonical_json_bytes(&file_obj);
        let file_id = bytes_hash(&file_bytes);
        self.cx.backend.write(&file_id, file_bytes).await?;
        Ok(FilePart::File(FileFilePart {size: file_size, file: file_id}))
    }
}

#[async_trait]
impl RepoFileBuilder for SyncRepoFileBuilder {
    async fn add_chunk(&mut self, buf: Bytes) -> anyhow::Result<()> {
        // Get the chunk's hash
        let chunk_id = bytes_hash(&buf);
        let chunk_size = buf.len() as u64;

        // Write the chunk
        self.cx.backend.write(&chunk_id, buf).await?;

        let file_part = FilePart::Chunk(ChunkFilePart {size: chunk_size, content: chunk_id});
        self.add_part_at(file_part, 0).await?;

        Ok(())
    }

    async fn complete(mut self: Box<Self>) -> anyhow::Result<ObjectId> {
        // Go through and add each element of the stack to the item above
        let mut ix = 0;
        while ix < self.stack.len() - 1 {
            let part = self.turn_stack_elem_into_part(ix).await?;
            self.add_part_at(part, ix + 1).await?;
            ix += 1;
        }

        // Take the final element and turn it into a File
        let parts = self.stack[ix].replace_parts();
        let file = File{parts};
        let file_obj = RepoObject::File(file);
        let file_bytes = to_canonical_json_bytes(&file_obj);
        let file_id = bytes_hash(&file_bytes);
        self.cx.backend.write(&file_id, file_bytes).await?;
        Ok(file_id)
    }
}
