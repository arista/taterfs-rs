# Backend Interfaces

Taterfs is designed to allow for a variety of backend implementations built to a simple set of interfaces.  Implementations can be built a various "levels" as appropriate.

## Base repo_backend

The basic repo_backend interface requires only a few functions:

```
read_current_root() -> root object id | null
write_current_root(root object id)
swap_current_root(expected current root object id, new root object id)

object_exists(id) -> bool
read_object(id) -> byte array
write_object(id, byte array)
```
The `object_exists`, `read_object` and `write_object` functions simply read or write a set of bytes associated with an id, or test if an object with the id already exists in the repository.  It is optional for a backend to verify that the id matches the contents of the object's byte array.

The functions that deal with the current root simply read or write a single value.  The `swap_current_root` function will only write a new root if the current root matches what is supplied.  Backends are not required to implement this function transactionally, but are encouraged to if they can.

