# Plans

This document tracks the roadmap and planned features for taterfs-rs.

## Current Focus

There is an issue with the repo cli command, download directory.  There are a couple orthogonal features being conflated:

  whether this is a "dry run" or not
  whether actions should be reported verbosely

It appears that the "real" download is bypassed by this line:

  if self.verbose || global.json
  
Because it calls download_repo_to_store instead of download_directory.  But unless it's a dry run, it should be calling download_directory.

I also notice that for a dry run, it calls "run_dry_run", with what appears to be some duplicate code for its own version of the DownloadActions.

I wonder if it makes more sense to move the notion of a "dry run" into the download_directory call itself, which should hopefully simplify how it's called.  If it's called as a dry run, then it shouldn't bother trying to create a stage, and it should just do a single phase of download_repo_to_store.  The cli can then call it with the same VerboseDownloadActions, change its inner to an Option<...>, and set it to None for dry run.

## Planned Features

## Completed

## Ideas / Backlog

- Set up LocalStack or MinIO for S3Backend integration tests
- Look into running expensive computations (hash/crypto) in separate threads

