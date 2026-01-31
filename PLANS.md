# Plans

This document tracks the roadmap and planned features for taterfs-rs.

## Current Focus

Look at the changes in upload_download.md.  The original DownloadActions has already been removed in a previous commit.  The new download_repo_to_store call needs to be implemented in src/download/download.rs.  This should stay on branch nsa-download.

Note that there are also changes in file_stores.md and backend_storage_model.md that will be used by the above changes, so those need to be added as well to their appropriate source files.

## Planned Features


## Completed


## Ideas / Backlog

- Set up LocalStack or MinIO for S3Backend integration tests
