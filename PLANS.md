# Plans

This document tracks the roadmap and planned features for taterfs-rs.

## Current Focus

As specified in download.md, implement the download_file and download_directory functions.  There are some small changes to the download_repo_to_store function which incorporate the use of a WithComplete.

Perhaps a major change is the implied changes to FileChunkWithContent - namely the ability to look to a Stage for content.  It's worth thinking what the right approach is:

* convert FileChunkWithContent to an interface with multiple implementations
* allow an optional Stage to be passed to FileChunkWithContent (and subsequently to read_file_chunks_with_content).  Feels like an unnatural mixing of concerns though.
* similar to above, but a more abstract interface than Stage, something like an abstract ChunkContentSource.  But if we're going to do that, why wouldn't the "normal" downloading of content from a repo backend also be a ChunkContentSource?

## Planned Features

## Completed

## Ideas / Backlog

- Set up LocalStack or MinIO for S3Backend integration tests
- Look into running expensive computations (hash/crypto) in separate threads

