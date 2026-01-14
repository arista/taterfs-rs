# Backend Interfaces

Taterfs is designed to allow for a variety of backend implementations built to a simple set of interfaces.  Implementations can be built at various "levels" as appropriate.

## Interfaces

### repo_backend

The internal functions will assume that they operate against a basic repo_backend interface, which defines only a few functions:

```
has_repository_info() -> bool
set_repository_info(repository_info: RepositoryInfo)
get_repository_info() -> RepositoryInfo

read_current_root() -> root object id | null
write_current_root(root object id)
swap_current_root(expected current root object id, new root object id)

object_exists(id) -> bool
read_object(id) -> byte array
write_object(id, byte array)
```
(note - all these functions should assume they are async)

The `object_exists`, `read_object` and `write_object` functions simply read or write a set of bytes associated with an id, or test if an object with the id already exists in the repository.  It is optional for a backend to verify that the id matches the contents of the object's byte array.

The functions that deal with the current root simply read or write a single value.  The `swap_current_root` function will only write a new root if the current root matches what is supplied.  Backends are not required to implement this function transactionally, but are encouraged to if they can.

### repository info

Every repository contains basic information in a RepositoryInfo structure.  At a minimum, this contains the UUID of the repository, assigned when the repository was created:

```
RepositoryInfo {
  uuid: string
}
```

Eventually this might contain other global information about the repository, such as the version of the format used to store the repository's data.  It is not expected that this information will change often, if at all.

Repositories may choose to store this information as they choose.

The set_repository_info() function will return an error if the Repository already has RepositoryInfo - it should only be set once when the repository is first initialized.

### fs_like_repo_backend

Many backend implementations will use an underlying filesystem-like mechanism for storage, such as the FileSystemRepoBackend or S3RepoBackend.  These backends will likely implement the repo_backend interface in similar ways, mapping object ids to filenames and using a file to read/write the current root.  Those backends can implement a different interface, and use an adapter to bridge from the repo_backend interface to the fs_like_repo_backend interface.

The fs_like_repo_backend interface looks like this:

```
file_exists(filename) -> bool
read_file(filename) -> byte array
write_file(filename, byte array)
first_file(directory) -> filename | null
```
(note - all these functions should assume they are async)

The fs_like_repo_backend_adapter is a component that takes a fs_like_repo_backend interface as a property, and implements the repo_backend interface by making calls to that interface.

For the get_repository_info() function, the adapter assumes that the info is stored in:

```
repository_info.json
```

It will read, parse and return that info.

For the object_exists, read_object, and write_object methods, the adapter assumes that objects are stored in this structure:

```
objects/{id[0..2]}/{id[2..4]}/{id[4..6]}/{id}
```

For the current root, the adapter assumes that the roots are stored in this structure:

```
current_root/{reverse root timestamp}/{root object id} (0-length file)
```

The reverse root timestamp is a timestamp that decreases lexically over time, computed by subtracting the current time in millis from the maximum u64, then converting to a zero-padded 20 digit number.  This way, the lexically first entry (which is easy to find on most filesystems) will be the most recent root.

The reverse root timestamp is actually formed by splitting into multiple directories like this:

```
current_root/{ts[0..8]}/{ts[8..10]}/{ts[10..12]}/{ts[12..20]}/{root object id} (0-length file)
```

With the expectation of "{changes ~every 30 years}/{changes ~3x per year}/{changes ~every day}/{changes every ms}".

### http_repo_backend

The http_repo_backend is another implementation of repo_backend that operates against an HTTP server at a given base url.  An HTTP server implementing the expected requests will then be able to serve as a backend while running in a separate process or even a different hosting environment.

The http_repo_backend expects the HTTP server to respond to these requests:

```
GET {base url}/repository_info
  200 - return the repository info JSON (content type application/json)
  404 - object not found

GET {base url}/objects/{object id}
  200 - return the object's bytes (content type, etc. are ignored)
  404 - object not found

HEAD {base url}/objects/{object id}
  can also be used to determine the existence of an object

PUT {base url}/objects/{object id}
  payload is the object's contents

GET {base url}/current_root
  200 - return a string with the root id
  404 - no root

PUT {base url}/current_root
  payload is the new root id

PUT {base url}/current_root/{expected current root id}
  payload is the new root id
  404 if the current root is not the expected current root id
```

## Implementations

### memory_backend

This is a simple in-memory implementation of repo_backend, intended primarily for testing.

### fs_backend

This is an implementation of the fs_like_repo_backend interface that is configured to work against a filesystem directory.

### s3_backend

This is an implementation of the fs_like_repo_backend interface that is configured to work against an S3 bucket and path prefix.

## encryption

Some backend implementations may choose to store their data encrypted at rest.  When this feature is defined it is expected that the above interfaces will likely remain the same, but the implementations may need to be configured with additional information, such as keys and key id's.

It is also hoped that the fs_like_repo_backend_adapter will have an encrypted version that operates against the same fs_like_repo_backend interface.  This means that backends that are built to implement fs_like_repo_backend will automatically take advantage of encryption without needing any changes.

