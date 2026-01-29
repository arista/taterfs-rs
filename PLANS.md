# Plans

This document tracks the roadmap and planned features for taterfs-rs.

## Current Focus

Currently scan_ignore_helper.rs depends directly on interfaces from file_store: DirectoryScanEvent and FileSource.  I would like to separate out ScanIgnoreHelper so that it can be used in more contexts.  Therefore, please remove those direct references, and instead have it define its own ScanDirectoryEvent, and its own ScanFileSource interface, tailored to just what it needs.  The places that call ScanIgnoreHelper will then need to bridge to those structures.  Also, update file_stores.md to match the changes (please keep the changes to that .md file minimal, no need to copy in the full rust definitions).  Please do this all on branch "nsa-abstract-ignore"

## Planned Features


## Completed


## Ideas / Backlog

- Set up LocalStack or MinIO for S3Backend integration tests
