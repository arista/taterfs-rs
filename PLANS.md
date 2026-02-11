# Plans

This document tracks the roadmap and planned features for taterfs-rs.

## Current Focus

repository_interface.md and file_store.md describe how the LocalChunksCache is to be used and updated.  These changes should be implemented, keeping in mind the cache updates and consulting the cache should be bypassed if no-cache is specified in the configuration (or from the cli)

## Planned Features

## Completed

## Ideas / Backlog

- Set up LocalStack or MinIO for S3Backend integration tests
- Look into running expensive computations (hash/crypto) in separate threads

