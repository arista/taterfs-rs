# Repository Interface

The **Repo** object manages an application's interaction with a stored repository, whose [backend](./backend_interfaces.md) will be passed to the Repo as a configuration parameter.

A Repo presents an interface that is slightly more ergonomic than the raw RepoBackend interface.  More importantly, a Repo adds important functionality around the calls in those interfaces:

* Caching - if configured with a RepoCache, then that cache will be consulted before handing requests to the backend.  If requests do get handed to the backend, then on success, the cache will be updated accordingly.
* Request deduplication - if a request is made and handed off to the backend, and then additional requests are made with the same parameters before the backend returns with the result, those additional requests are combined with the original request.  Once the backend returns with the answer, all of those requests will be fulfilled.
* Flow control - if so configured, the Repo will respect various CapacityManager flow control mechanisms "around" calls to the backend
    * concurrent request limiter
    * read throughput limiter
    * write throughput limiter
    * total throughput limiter
    * request rate limiter
* Logging - TODO: eventually define this

## Interface

The public Repo interface looks like this:

```
async current_root_exists() -> bool
async read_current_root() -> ObjectId
async write_current_root(root: Object_id)
async object_exists(id: ObjectId) -> bool

async write(id: ObjectId, bytes: Bytes)
async read(id: ObjectId, expected_size: Option<u64>) -> Bytes

async write_object(obj: RepoObject) -> ObjectId
async read_object(id: ObjectId) -> RepoObject
async read_root(id: ObjectId) -> Root
async read_branches(id: ObjectId) -> Branches
async read_commit(id: ObjectId) -> Commit
async read_directory(id: ObjectId) -> Directory
async read_file(id: ObjectId) -> File
```

The expected_size passed to read is used when interacting with the throughput limiters.  If no expected_size is passed, then once the object is read and the expected_size is known, the throughput limiters are called at that point.  For the write calls, the size is known up front, so the size can be passed to the capacity managers.

The read_{object type} functions are convenience functions that effectively call read_object and "cast" to the appropriate object, erroring if the actual object doesn't match the requested type.

The Repo is configured with:

```
backend: RepoBackend
cache: RepoCache
request_rate_limiter: Option<CapacityManager>
concurrent_request_limiter: Option<CapacityManager>
read_throughput_limiter: Option<CapacityManager>
write_throughput_limiter: Option<CapacityManager>
total_throughput_limiter: Option<CapacityManager>
```

It is expected that there will be helper functions for handling the limiters that can be reused by the appropriate calls.

## Specifying Repos

When the application runs, it will need a way to specify which Repositories to use.  Those specifications will need to have enough information to construct a Repo connected to particular backends, caches, and capacity managers.  Some of that can be provided through configuration, while also providing a mechanism to override settings.

The basic way to reference a repository is through a URL:


```
s3://{bucket}/{prefix}
file://{directory}
http://...
```

Each of those URL's would lead to the creation of a Repo connected to an FsBackend, S3Backend, or HttpBackend.

Some backend types might require additional parmeters.  The S3Backend, for example, can optionally take an endpoint_url and a region.  Those can be specified as query parameters to the URL:

```
s3://{bucket}/{prefix}?endpoint_url=...&region=...
```

