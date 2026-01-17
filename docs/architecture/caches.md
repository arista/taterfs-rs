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

### FileStoreCache

```
interface FileStoreCache {
  async get_path_id(path: Path) -> DbId
  async get_path_entry_id(parent: Option<DbId>, name: string) -> DbId
  async get_fingerprinted_file_info(path_id: DbId) -> Option<FingerprintedFileInfo>
  async set_fingerprinted_file_info(path_id: DbId, FingerprintedFileInfo)
}

interface FileStoreCaches {
  // Returns the FileStoreCache for the given url, creating it if not yet found
  async get_filestore_cache(filestore_url: string) -> FileStoreCache
}

FingerprintedFileInfo {
  fingerprint: string
  object_id: string
}

```

### CacheDb

The above interfaces are built against an underlying CacheDb service (all methods async)

```
DbId = u64

CacheDb {
  get_next_id() -> Option<DbId>
  set_next_id(id: DbId)
  generate_next_id() -> DbId

  get_repository_id(uuid) -> Option<DbId>
  set_repository_id(uuid, id: DbId)
  get_or_create_repository_id(uuid) -> DbId
  get_exists(repo_id: DbId, object_id: ObjectId) -> bool
  set_exists(repo_id: DbId, object_id: ObjectId)
  get_fully_stored(repo_id: DbId, object_id: ObjectId) -> bool
  set_fully_stored(repo_id: DbId, object_id: ObjectId)

  get_object(object_id: ObjectId) -> RepoObject | null
  set_object(object_id: ObjectId, obj: RepoObject)

  get_filestore_id(filestore_url) -> Option<DbId>
  set_filestore_id(filestore_url, id: DbId)
  get_or_create_filestore_id(filestore_url) -> DbId
  get_fingerprinted_file_info(filetore_id: DbId, path_id: DbId) -> Option<FingerprintedFileInfo>
  set_fingerprinted_file_info(filetore_id: DbId, path_id: DbId, info: FingerprintedFileInfo)
  get_name_id(filestore_id: DbId, name: string) -> Option<DbId>
  set_name_id(filestore_id: DbId, name: string, id: DbId)
  get_or_create_name_id(filestore_id: DbId, name: string) -> DbId
  get_path_entry_id(filestore_id: DbId, parent: Option<DbId>, name: DbId) -> Option<DbId>
  set_path_entry_id(filestore_id: DbId, parent: Option<DbId>, name: DbId, path_id: DbId)
  get_or_create_path_entry_id(filestore_id: DbId, parent: Option<DbId>, name: string) -> DbId
  get_path_id(path: string) -> DbId
}
```

The RepositoryCache and FileStoreCache are thin wrappers around this service, which each store a DbId obtained from get_repository_id or get_filestore_id, and pass that id in as appropriate.

As will be described later, these calls map to a key/value database in a straightforward way.  The get/set calls map nearly directly.  Some of the functions are convenience methods built on top of that:

* generate_next_id() -> get_next_id(), set_next_id(incremented id), return the retrieved id
* get_or_create_repository_id does a get_repository_id/[next_id/set_repository_id] sequence
* get_or_create_filestore_id does a get_filestore_id/[next_id/set_filestore_id] sequence
* get_or_create_name_id does a get_name_id/[next_id/set_name_id] sequence
* get_or_create_path_entry_id does a get_path_entry_id/[next_id/set_path_entry_id] sequence

The path functions are all intended to allow paths to be used in keys (for get_fingerprinted_file_info, for example) while cutting down on key size.  Each component of a path is mapped to a name id, and the path hierarchy is represented by "path entry" mappings from a [parent path, name] combo to a new path id.  The get_path_id() function is a convenience function that goes through that logic.

The get_object() and set_object() calls do not use the Key/Value database.  Those are instead mapped to an ObjectCacheDb (described later)

### KeyValueDb

This is an even more fundamental interface to an underlying key/value database that will be used to implement CacheDb.

```
KeyValueDb {
  async exists(key: bytes) -> bool
  async get(key: bytes) -> Option<bytes>
  async list_entries(prefix: bytes) -> KeyValueEntries
  async transaction() -> KeyValueDbTransaction
  async write() -> KeyValueDbWrites
}

KeyValueDbTransaction {
  async exists(key: bytes) -> bool
  async get(key: bytes) -> Option<bytes>
  async set(key: bytes, val: bytes)
  async del(key: bytes)
}

KeyValueDbWrites {
  async set(key: bytes, val: bytes)
  async del(key: bytes)
}

KeyValueEntries {
  async get() -> Option<KeyValueEntry>
}

KeyValueEntry {
  key: bytes
  value: bytes
  
  key_string() -> string
  value_string() -> string
}
```

(there is some flexibility in this depending on the implementation of key/value database used)

The difference between write() and transaction() is just that transaction() indicates that the writes are intended to be written transactionally (committed when the KeyValueDbWrites is dropped), whereas write() just indicates that the writes may be written at any time.  Some implementations may just have both do the same thing.

## Implementations

### CacheDb on KeyValueDb

The CacheDb functions map to KeyValueDb using the following mappings

A Key/Value database is used to store the cache.  The database is organized as follows:

Some notes:

* **dbid** refers to a u64 id allocated in the database using next-id.
* "encoded" means that a string is encoded using the equivalent of JS encodeURIComponent


```
next-id -> {the next dbid to be generated}

repository-id-by-uuid/{encoded uuid} -> {repository dbid} - implements get/set_repository_id
ex/{repository dbid}/{object id} -> {empty value} - implements get/set_exists
fs/{repository dbid}/{object id} -> {empty value} - implements get/set_fully_stored

filestore-id-by-url/{filestore url, uri-component-encoded} -> implements get/set_filestore_id
na/{filestore dbid}/{encoded name} -> {name id} - implements get/set_name_id
pa/{filestore dbid}/{path dbid or "root"}/{name dbid} -> {path dbid} - implements get/set_path_entry_id
fi/{filestore dbid}/{path dbid} -> {file info, of the form "{fingerprint}|{file hash}"} - implements get/set_fingerprinted_file_info
```
#### Memory Caching and Write Backs

The system makes the following assumptions that affect the design:

* Multiple processes may be using the same cache files simultaneously
* Transactional writes will get expensive at high frequency
* Most writes to the database are not critical - it's ok if the system goes down without storing a set of edits (except for some cases)
* It is not critical (in most cases) for one process to "see" the writes from another process in a timely mannger
* "Preloading" many entries at once may improve lookup speed

The design addresses these in the following ways:

* Generating a DbId in its get/set cycle does need to be fully transactional.  To cut down on transactions, the system should retrieve a "block" of id's into memory (i.e., increment the next_id counter by say, 100), then hand out those id's until the block is exhausted and only then go back through the transactional get/set cycle again.
* The same is true for the get_or_create_repository_id and get_or_create_filestore_id calls.
* All other writes need to be buffered into a pending list that is periodically "flushed" in a single transaction.  The flushing logic is:
    * Every 500ms
    * When the number of items in the list exceeds 10k
    * At shutdown (best effort)
* All of the read methods must take into account any pending writes.

This implies that a full in-memory cache will be needed to handle the write-back behavior.  There are two ways this could happen:

* Implement the caching behavior at the KeyValueDb level.  Simpler implementation, but might be at some cost to performance, since the CacheDb still needs to convert rust values to and from key/value structures
* Implement the caching behavior at the CacheDb level.  This would be most efficient, since rust objects can be stored in memory, but is most complex since each type of cached object effectively needs its own cache.

To start with, go with caching at the KeyValueDb level (described below), and move to the other approach if the performance cost warrants it.

### LmdbKeyValueDb

Explore implementing KeyValueDb with LMDB, with the heed crate.  Determine if this is a low-cost mapping.

Implement this without any caching.  The write() and transaction() calls both do the same thing.

### CachingKeyValueDb

This takes a KeyValueDb and implements a write-back caching layer on top of it.  Its transaction() call allows for direct transactions on the underlying KeyValueDb (needed for generate_next_id).  Its write() call however, writes to a pending list.

The pending writes are stored in the memory cache, so that reads of pending writes are correct.  Pending writes are flushed to the underlying KeyValueDb (using transaction()) on these conditions (some specified by [configuration](./configuration.md).

* Every "pending_writes_flush_period_ms" (from [cache] config section)
* If the number of items in the pending_writes_is more than "pending_writes_max_count" (from [cache] config section)
* If the approximate size of items in the pending_writes_is more than "pending_writes_max_size" (from [cache] config section)
* At shutdown (best effort)

The cache should also limit the use of its non-pending entries to "max_memory_size" (from [cache] config section), evicting entries in using an LRU algorithm.

### ObjectCacheDb

This is the interface used to cache repo objects locally.  It is kept separate from the KeyValueDb because it will likely grow to a much larger scale and may have to be managed differently.  Its interface looks like this:

```
ObjectCacheDb {
  // Maintains a cache of objects
  async get_object(object_id: ObjectId) -> RepoObject | null
  async set_object(object_id: ObjectId, obj: RepoObject)
}
```

It has a file system implementation, FsObjectCacheDb, which is instantiated with a local directory.  It stores objects serialized as canonical JSON, with each object stored in a separate file under the form:

```
{cache directory}/cache_objects/{object_id[0..2]}/{object_id[2..4]}/{object_id[4..6]}/{object_id}.json
```

When writing, each object is first written to a temporary file, then moved to the appropriate location.  There is no batching of writes - set_object() will immediately write to the filesystem.  The temporary location is:

```
{cache directory}/.cache_objects_tmp
```

The FsObjectCacheDb will be located in the same directory as the LmdbKeyValueDb.

TODO: think about encrypting object contents at rest

There is also an in-memory caching implementation, CachingObjectCacheDb, which maintains an in-memory LRU cache.  This is the implementation that is used by the rest of the application (CacheDb).  The maximum size of the cache is set in [configuration](./configuration.md) under [cache].max_object_memory_size.  Memory size is computed by the serialized length of the value, plus the length of the object id.

