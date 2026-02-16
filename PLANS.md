# Plans

This document tracks the roadmap and planned features for taterfs-rs.

## Current Focus

Look at the changes specfied in list_modifier.md.  There are some additional changes that will be required:

* A list_branches method needs to be added to repo.rs, similar to the way that list_directory_entries and list_file_chunks is implemented, that returns a BranchList.
* In repo.rs, DirectoryEntry needs an impl with a name() getter that returns the name from a FileEntry or DirEntry

## Planned Features

## Completed

## Ideas / Backlog

- Set up LocalStack or MinIO for S3Backend integration tests
- Look into running expensive computations (hash/crypto) in separate threads

