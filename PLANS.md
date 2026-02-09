# Plans

This document tracks the roadmap and planned features for taterfs-rs.

## Current Focus



## Planned Features


## Completed

- Changed `read()` to take `Option<AcquiredCapacity>` instead of `Option<u64>` for expected_size
  - If `acquired` is provided, it's used to create the ManagedBuffer (caller controls backpressure)
  - If `acquired` is `None`, capacity is acquired internally after the read
  - Dedup now returns raw bytes (`Arc<Vec<u8>>`), and each caller wraps in their own ManagedBuffer
- Refactored `FileChunkWithContentList.next()` to use the new `read()` with pre-acquired capacity

## Ideas / Backlog

- Set up LocalStack or MinIO for S3Backend integration tests
- Look into running expensive computations (hash/crypto) in separate threads

