# App

The App is the top-level component of the system, which owns all of the global services and is the root for the application's functionality.

The App is typically instantiated by a CLI function and provided with various options from the CLI.  The CLI will then typically create the repos and/or file stores needed to fulfill whatever command is running.

## Interfaces

```
App {
  new(ctx: AppContext)

  create_repo(ctx: AppCreateRepoContext) -> Arc<Repo>
  create_file_store(ctx: AppCreateFileStoreContext) -> Arc<FileStore>
}

AppContext {
  config_source: ConfigSource
}

AppCreateRepoContext {
  spec: string
  allow_uninitialized: bool
}

AppCreateFileStoreContext {
  spec: string
}
```

Upon creation, the AppContext will create the global owned structures that it needs: cache, capacity managers, etc:

* ConfigResult (from read_config.rs)
* LmdbKeyValueDb (see [caches](./caches.md))
* CachingKeyValueDb (see [caches](./caches.md))
* CacheDb (see [caches](./caches.md))
* ManagedBuffers
* Network CapacityManagers
* S3 CapacityManagers
