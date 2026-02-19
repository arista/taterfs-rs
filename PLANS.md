# Plans

This document tracks the roadmap and planned features for taterfs-rs.

## Current Focus

Look at the changes to [backend_storage_model.md](/docs/architecture/backend_storage_model.md), which involves removing defaultBranch as a separately-stored value, and instead mixes it into the full Branches structure.  This will likely be a pretty disruptive change.

## Planned Features

## Completed

## Ideas / Backlog

- Set up LocalStack or MinIO for S3Backend integration tests
- Look into running expensive computations (hash/crypto) in separate threads

