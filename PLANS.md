# Plans

This document tracks the roadmap and planned features for taterfs-rs.

## Current Focus

Implement the mod_dir_tree function in dir_tree_mod.md, with its associated structures.  This should go into app/mod_dir_tree.rs

This may require additional changes:

* in repo.rs, factor out a list_entries_of_directory that takes a Directory instead of a directory_id, and have list_directory_entries call it

## Planned Features

## Completed

## Ideas / Backlog

- Set up LocalStack or MinIO for S3Backend integration tests
- Look into running expensive computations (hash/crypto) in separate threads

