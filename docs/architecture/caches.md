# Caches

Caches are used in multiple places throughout the system.  Some caches are epehmeral in-memory caches, while others are are stored on the local disk.

## Interfaces

### RepositoryCache

```
interface RepoCache {
  // Indicates if the given object already exists in the repository
  async object_exists(object_id: ObjectId) -> bool
  async set_object_exists(object_id: ObjectId)

  // Indicates if the object, and every other object reachable from the object, already exist in the repository
  async object_fully_stored(object_id: ObjectId) -> bool
  async set_object_fully_stored(object_id: ObjectId)

  // Maintains a cache of objects
  async get_object(object_id: ObjectId) -> RepoObject | null
  async set_object(object_id: ObjectId, obj: RepoObject)
}

interface RepoCaches {
  // Returns the RepositorCache for the given uuid, creating it if not yet found
  async get_repository_cache(repository_uuid: string) -> RepoCache
}
```

## Implementations

### KeyValueCacheDB

TODO: define an interface for a generic key/value cache database, select a key/value cache implementation.  Also define an implementation for an in-memory implementation that preloads from the db, and buffers changes to the db

### CacheDBRepositoryCache

TODO: define the cache implementation, implemented on top of a KeyValueCacheDB
