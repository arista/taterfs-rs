# Architecture

This document describes the high-level architecture of taterfs-rs.

## Components

* [backend storage model](docs/architecture/backend_storage_model.md)
* [backend interfaces](docs/architecture/backend_interfaces.md)
* [file stores](docs/architecture/file_stores.md)
* [flow control](docs/architecture/flow_control.md)
* [caches](docs/architecture/caches.md)
* [repository interface](docs/architecture/repository_interface.md)
* [repository specification](docs/architecture/repository_specification.md)
* [configuration](docs/architecture/configuration.md)
* [completes](docs/architecture/completes.md)
* [list_builder](docs/architecture/list_builder.md)
* [list_modifier](docs/architecture/list_modifier.md)
* [list_search](docs/architecture/list_search.md)
* [upload](docs/architecture/upload.md)
* [download](docs/architecture/download.md)
* [dir_tree_mod](docs/architecture/dir_tree_mod.md)
* [merge](docs/architecture/merge.md)
* [sync](docs/architecture/sync.md)
* [repo_model](docs/architecture/repo_model.md)
* [app](docs/architecture/app.md)
* [commands](docs/architecture/commands.md)
* [cli](docs/architecture/cli.md)

### Entry Point

`src/main.rs` - Application entry point and CLI handling.

