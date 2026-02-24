# Plans

This document tracks the roadmap and planned features for taterfs-rs.

## Current Focus

Look at the changes on this branch outlining new sync related cli commands, and a slight ergonomic improvement to filestore specs.

## Planned Features

## Completed

## Ideas / Backlog

- Set up LocalStack or MinIO for S3Backend integration tests
- Look into running expensive computations (hash/crypto) in separate threads
- When running a sync, see if we can split up the times when files are downloaded to the stage vs when they're actually written to the filestore, and only have a pending StoreSyncState for that last portion (I think it's already described in sync.md)
