# Plans

This document tracks the roadmap and planned features for taterfs-rs.

## Current Focus

Look at the changes to [cli.md](/docs/architecture/cli.md), particularly the new "tfs upload-directory" command.  This brings in some additional common cli args, plus changes to config, plus the introduction of a separate "commands" module that is called by the CLI, to avoid concentrating too much code directly in the CLI.

## Planned Features

## Completed

## Ideas / Backlog

- Set up LocalStack or MinIO for S3Backend integration tests
- Look into running expensive computations (hash/crypto) in separate threads

