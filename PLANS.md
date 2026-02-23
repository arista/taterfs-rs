# Plans

This document tracks the roadmap and planned features for taterfs-rs.

## Current Focus

A couple notes:

add_sync:

* In cases where it's modifying the repo (create empty repo directory, or upload), it needs to modify the repo using RepoModel.update.
* Before it commits the next sync state, it needs to wait for all completes to complete.  In fact, returning WithComplete is an error on my part.  It shouldn't do that.
* In the case where it downloads content into the filestore, it's creating and committing sync state twice.  It shouldn't do that.

## Planned Features

## Completed

## Ideas / Backlog

- Set up LocalStack or MinIO for S3Backend integration tests
- Look into running expensive computations (hash/crypto) in separate threads

