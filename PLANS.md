# Plans

This document tracks the roadmap and planned features for taterfs-rs.

## Current Focus



## Planned Features

## Completed

- Implemented `FileDest.write_file_from_chunks` for FsFileStore with queue-based algorithm:
  - Takes `FileChunkWithContentList` (from repo) instead of `SourceChunksWithContent`
  - Returns `WithComplete<()>` to signal when background writes complete
  - Uses mpsc channel to pass chunks from the main loop to a background writer task
  - Temp files named `.download.{filename}-{pid}`, then atomically renamed
- Implemented `FileDestStage` for FsFileStore:
  - Stage directory at `{root}/.tfs/tmp/stage-{pid}/`
  - Chunks stored at `chunks/{id[0..2]}/{id[2..4]}/{id[4..6]}/{id}`
  - Atomic writes via temp files named `.download.{id}-{pid}`
  - `cleanup()` removes entire stage directory
- Added `create_stage()` method to `FileDest` trait
- Updated `MemoryFileStore` to match new trait (returns `NoopComplete`, `create_stage()` returns `None`)

## Ideas / Backlog

- Set up LocalStack or MinIO for S3Backend integration tests
- Look into running expensive computations (hash/crypto) in separate threads

