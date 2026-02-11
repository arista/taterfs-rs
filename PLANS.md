# Plans

This document tracks the roadmap and planned features for taterfs-rs.

## Current Focus

In download_directory, if a stage is used, it needs to have "cleanup()" called on it when the download operation is complete.  Note that this happens when the download_to_repo_store's WithComplete is actually complete.  But download_directory's WithComplete shouldn't complete until that cleanup() operation is also finished.

## Planned Features

## Completed

## Ideas / Backlog

- Set up LocalStack or MinIO for S3Backend integration tests
- Look into running expensive computations (hash/crypto) in separate threads

