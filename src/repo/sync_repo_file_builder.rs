use async_trait::async_trait;
use std::sync::Arc;
use bytes::Bytes;
use crate::repo::repo_model::{RepoObject, File, FilePart, ChunkFilePart, FileFilePart, ObjectId, MAX_FILE_PARTS, to_canonical_json_bytes, object_hash, bytes_hash};
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
    pub async fn add_part(&mut self, part: FilePart) -> anyhow::Result<()> {
        let mut ix = 0;
        let mut part_for_ix = part;
        loop {
            // If we've reached the end, add a new item
            if ix >= self.stack.len() {
                self.stack.push(PartsStackElem::default());
            }

            // If we have space for the item, then go ahead and add it
            if self.has_space(ix) {
                self.stack[ix].parts.push(part_for_ix);
                break;
            }
            // No space in this item - we'll have to make room, possibly going up the stack
            else {
                // Get the list of parts from this item, replace with an empty list, then add the new part to it
                let new_part = self.write_part(ix).await?;
                self.stack[ix].parts.push(part);
                part_for_ix = new_part;
            }

            ix += 1;
        }
        Ok(())
    }

    pub async fn write_part(&mut self, ix: usize) -> anyhow::Result<FilePart> {
        let parts = self.stack[ix].replace_parts();
        let file = File{parts};
        let file_obj = RepoObject::File(file);
        let file_bytes = to_canonical_json_bytes(&file_obj);
        let file_id = bytes_hash(&file_bytes);
        let file_size = file.total_len();
        self.cx.backend.write(file_id, file_bytes).await?;
        FilePart::File(FileFilePart {size: file_size, file: file_id})
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

        Ok(())
    }

    async fn complete(mut self: Box<Self>) -> anyhow::Result<ObjectId> {
        let parts = Vec::<FilePart>::new();
        let file = File { parts: parts};
        let file_obj = RepoObject::File(file);
        Ok(object_hash(&file_obj))
    }
}
